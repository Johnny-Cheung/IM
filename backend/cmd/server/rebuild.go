package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/infra"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

const (
	defaultRedisRebuildTimeout      = 30 * time.Minute
	defaultRedisRebuildBatch        = 1000
	defaultRedisRebuildQueueTimeout = 10 * time.Minute
	maxRedisRebuildBatch            = 10000
	releaseRedisRebuildLockScript   = releaseSeqBackfillLockScript
	// redisRebuildLockMargin outlives the command's own deadline so its lock is
	// never dropped while the rebuild is still working.
	redisRebuildLockMargin = 5 * time.Minute
	// defaultMaintenanceAddr is the rebuild deployment's backend port. It is
	// the port the stopped-backend check probes when -addr was not given.
	defaultMaintenanceAddr = "0.0.0.0:8198"
	// backendProbeTimeout bounds the stopped-backend check; a listening
	// loopback socket accepts immediately.
	backendProbeTimeout = 250 * time.Millisecond
)

// acquireMaintenanceLockScript takes one one-shot maintenance lock only when
// the other command's lock is absent. Redis runs a script atomically, so the
// rebuild and the seq backfill cannot both pass their checks. KEYS[1] is the
// other command's lock, KEYS[2] is ours; ARGV[1] is the token and ARGV[2] the
// TTL in milliseconds. It returns 1 when acquired, 0 when the other command
// holds its lock and -1 when our own lock is already held.
const acquireMaintenanceLockScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
local acquired = redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', ARGV[2])
if acquired then
  return 1
end
return -1
`

// maintenanceLock describes one side of the rebuild/backfill exclusion.
type maintenanceLock struct {
	OwnKey    string
	OwnName   string
	OtherKey  string
	OtherName string
	Token     string
	TTL       time.Duration
}

// acquireMaintenanceLock refuses with an explicit message when either
// maintenance lock is held.
func acquireMaintenanceLock(
	ctx context.Context,
	client redis.UniversalClient,
	lock maintenanceLock,
) error {
	if lock.TTL <= 0 {
		return fmt.Errorf("%s lock requires a positive TTL", lock.OwnName)
	}
	ttlMillis := lock.TTL.Milliseconds()
	if ttlMillis <= 0 {
		ttlMillis = 1
	}
	result, err := redis.NewScript(acquireMaintenanceLockScript).Run(
		ctx, client, []string{lock.OtherKey, lock.OwnKey}, lock.Token, ttlMillis,
	).Int()
	if err != nil {
		return fmt.Errorf("acquire %s lock: %w", lock.OwnName, err)
	}
	switch result {
	case 1:
		return nil
	case 0:
		return fmt.Errorf(
			"%s is running (lock %s); wait for it to finish before running the %s",
			lock.OtherName, lock.OtherKey, lock.OwnName,
		)
	case -1:
		return fmt.Errorf(
			"another %s is running (lock %s); wait for it to finish",
			lock.OwnName, lock.OwnKey,
		)
	default:
		return fmt.Errorf(
			"acquire %s lock: unexpected script result %d", lock.OwnName, result,
		)
	}
}

// maintenanceDial probes the backend's HTTP port. Tests replace it.
var maintenanceDial = func(address string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", address, timeout)
}

// maintenanceProbeTarget returns the loopback address to probe. The host in
// -addr is ignored: the supported deployment runs the maintenance command on
// the same host as the backend, whose listener is reachable over loopback. A
// remote backend is out of scope for this check.
func maintenanceProbeTarget(addr string) (string, error) {
	if addr == "" {
		addr = defaultMaintenanceAddr
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("parse -addr %q: %w", addr, err)
	}
	if port == "" {
		return "", fmt.Errorf("parse -addr %q: missing port", addr)
	}
	return net.JoinHostPort("127.0.0.1", port), nil
}

// ensureBackendStopped refuses a destructive one-shot command while a backend
// accepts connections on its HTTP port. imctl performs the same check; this is
// the in-process backstop for `im-server -rebuild-redis` run directly.
func ensureBackendStopped(addr, command string) error {
	target, err := maintenanceProbeTarget(addr)
	if err != nil {
		return err
	}
	connection, err := maintenanceDial(target, backendProbeTimeout)
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return nil
		}
		return fmt.Errorf(
			"cannot verify that no backend is serving on %s before the %s: %w; "+
				"stop the backend first",
			target, command, err,
		)
	}
	_ = connection.Close()
	return fmt.Errorf(
		"a backend is serving on %s; stop the backend (`imctl backend-stop`) "+
			"before the %s, or run `imctl %s` which repeats these checks",
		target, command, strings.TrimPrefix(command, "-"),
	)
}

// maintenanceAddr is the address the stopped-backend check probes. Without an
// explicit -addr the one-shot commands default to the rebuild deployment's
// port rather than the HTTP server's development default.
func (o serverOptions) maintenanceAddr() string {
	if !o.addrSet || o.addr == "" {
		return defaultMaintenanceAddr
	}
	return o.addr
}

// rebuildSettings validates the rebuild flags so a bad batch size or timeout
// is rejected before any connection is opened. The queue timeout must fit
// inside the command timeout, which also sizes the rebuild lock.
func rebuildSettings(options serverOptions) (int, time.Duration, time.Duration, error) {
	batch := options.rebuildBatch
	if batch < 1 || batch > maxRedisRebuildBatch {
		return 0, 0, 0, fmt.Errorf(
			"-rebuild-batch must be between 1 and %d", maxRedisRebuildBatch,
		)
	}
	queueTimeout := options.rebuildQueueTimeout
	if queueTimeout <= 0 {
		return 0, 0, 0, fmt.Errorf("-rebuild-queue-timeout must be positive")
	}
	timeout := options.rebuildTimeout
	if timeout <= 0 {
		return 0, 0, 0, fmt.Errorf("-rebuild-timeout must be positive")
	}
	if queueTimeout >= timeout {
		return 0, 0, 0, fmt.Errorf(
			"-rebuild-queue-timeout (%s) must be shorter than -rebuild-timeout (%s)",
			queueTimeout, timeout,
		)
	}
	return batch, queueTimeout, timeout, nil
}

// runRedisRebuild drains the persist queues and regenerates the whole Redis
// messaging namespace from MySQL. It is invoked with -rebuild-redis while the
// backend is stopped and writes seq:ready only after verification.
func runRedisRebuild(options serverOptions) error {
	batch, queueTimeout, timeout, err := rebuildSettings(options)
	if err != nil {
		return err
	}
	// Nothing may be drained or cleared while a backend is serving it.
	if err := ensureBackendStopped(options.maintenanceAddr(), "-rebuild-redis"); err != nil {
		return err
	}

	dsn, err := config.MySQLDSN()
	if err != nil {
		return fmt.Errorf("load MySQL configuration: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	dataStore, err := store.Open(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect MySQL: %w", err)
	}
	defer dataStore.Close()

	if err := dataStore.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate MySQL: %w", err)
	}

	infraConfig, err := config.LoadInfra()
	if err != nil {
		return fmt.Errorf("load infrastructure configuration: %w", err)
	}
	infraManager, err := infra.New(infraConfig)
	if err != nil {
		return fmt.Errorf("configure infrastructure: %w", err)
	}
	if infraManager == nil || !infraManager.RedisEnabled() {
		return fmt.Errorf("Redis is required for the Redis rebuild")
	}
	if !infraConfig.RabbitMQ.Enabled() {
		return fmt.Errorf("RabbitMQ is required for the Redis rebuild")
	}
	defer func() {
		if err := infraManager.Close(); err != nil {
			log.Printf("close infrastructure: %v", err)
		}
	}()

	namespace, err := messaging.NewNamespace(infraConfig.MessagingNamespace)
	if err != nil {
		return fmt.Errorf("configure messaging namespace: %w", err)
	}
	keyspace := messaging.NewKeyspace(namespace)
	redisClient := infraManager.Redis()
	lockKey := messaging.RebuildLockKey(keyspace)
	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	// The lock outlives the command deadline so it cannot expire mid-rebuild,
	// and it is taken only when the seq backfill lock is absent.
	if err := acquireMaintenanceLock(ctx, redisClient, maintenanceLock{
		OwnKey:    lockKey,
		OwnName:   "Redis rebuild",
		OtherKey:  messaging.BackfillLockKey(keyspace),
		OtherName: "seq backfill",
		Token:     token,
		TTL:       timeout + redisRebuildLockMargin,
	}); err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelRelease()
		if _, err := redisClient.Eval(
			releaseCtx, releaseRedisRebuildLockScript, []string{lockKey}, token,
		).Result(); err != nil {
			log.Printf("release Redis rebuild lock: %v", err)
		}
	}()

	// Drain both persist chains: the consumer path does not need the group
	// feature flag, and clearing the namespace with pending group deliveries
	// would lose them. The drain refuses if the group queue holds messages and
	// no persister is configured.
	groupPersister := messaging.GroupMessagePersister(dataStore)

	rebuilder := messaging.NewRedisRebuilder(
		redisClient,
		dataStore,
		messaging.RedisRebuildOptions{
			Keyspace:  keyspace,
			Namespace: namespace,
			Batch:     batch,
			Logf:      log.Printf,
			DrainQueues: func(ctx context.Context) (messaging.QueueDrainResult, error) {
				return messaging.DrainPersistQueues(ctx, messaging.QueueDrainOptions{
					Namespace:       namespace,
					Rabbit:          infraConfig.RabbitMQ,
					Dialer:          infra.DialRabbit,
					DirectPersister: dataStore,
					GroupPersister:  groupPersister,
					Timeout:         queueTimeout,
					Logf:            log.Printf,
				})
			},
		},
	)

	result, err := rebuilder.Run(ctx)
	if err != nil {
		return fmt.Errorf("rebuild Redis: %w", err)
	}

	log.Printf(
		"Redis rebuild complete namespace=%s cleared=%d drained_direct=%d "+
			"drained_group=%d dead_lettered=%d direct_messages=%d group_messages=%d "+
			"conversations=%d groups=%d dedup=%d watermarks=%d duration=%s",
		namespace.Base(),
		result.Cleared,
		result.Drained.Direct,
		result.Drained.Group,
		result.Drained.DeadLettered,
		result.DirectMessages,
		result.GroupMessages,
		result.Conversations,
		result.Groups,
		result.DedupRecords,
		result.Watermarks,
		result.Duration,
	)
	return nil
}
