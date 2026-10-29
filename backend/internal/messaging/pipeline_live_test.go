package messaging

// TestV182PipelineLive exercises the v1.8.2 delivery, recovery and
// reconciliation paths against real infrastructure. It is skipped unless
// IM_TEST_PIPELINE_LIVE=true and is intended to run on the isolated host only.
//
// Required environment:
//
//	IM_TEST_PIPELINE_LIVE=true
//	IM_REDIS_URL_FILE / IM_REDIS_PASSWORD_FILE            (Redis DB 1 is forced)
//	IM_RABBITMQ_URL_FILE / IM_RABBITMQ_PASSWORD_FILE      (vhost must be im_v13_verify)
//	IM_TEST_MYSQL_DSN                                     (no password in the DSN)
//	IM_TEST_MYSQL_PASSWORD_FILE
//
// MySQL uses the dedicated schema im_v182_pipeline_test. The schema is created
// explicitly and the test fails if it already exists; only a schema created by
// this test is dropped. Redis uses only im:v182-drill:* in DB 1. RabbitMQ uses
// only the im.v182-drill topology in the verify vhost. Cleanup deletes exactly
// those keys/queues/exchanges; it never flushes Redis or deletes unknown
// queues.
//
// The lock-wait step prefers a real, scoped InnoDB gap lock on the dedicated
// messages/group_messages tables. If that lock cannot be proven, it falls back
// to a clearly labelled simulated persister that returns driver.ErrBadConn for
// longer than a minute; the fallback is reported in the test log.
//
// Run (on the isolated host):
//
//	IM_TEST_PIPELINE_LIVE=true go test -run TestV182PipelineLive -count=1 \
//	  -timeout 6m ./internal/messaging

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/infra"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

const (
	liveNamespaceBase = "im:v182-drill"
	liveSchemaName    = "im_v182_pipeline_test"
	liveRabbitVHost   = "im_v13_verify"

	liveTestTimeout    = 5 * time.Minute
	livePollInterval   = 200 * time.Millisecond
	liveLockHold       = 62 * time.Second
	liveSimulatedHold  = 65 * time.Second
	liveLockDirectLow  = uint64(3)
	liveLockDirectHigh = uint64(4)
	liveLockGroupID    = uint64(2)
	liveLockSeq        = uint64(2)
)

type liveFixture struct {
	pendingDirect store.Message
	pendingGroup  store.GroupMessage
	reconcile     liveReconcileFixture
}

type liveReconcileFixture struct {
	oldDirect     store.Message
	oldGroup      store.GroupMessage
	missingEntity store.Message
	missingIndex  store.Message
	young         store.Message
}

type liveTestEnv struct {
	namespace Namespace
	keyspace  Keyspace
	redis     *redis.Client
	rabbit    config.RabbitMQConfig
	store     *store.Store
	db        *sql.DB

	adminConn *amqp091.Connection
	adminCh   *amqp091.Channel
	publisher *livePublisher
	dlq       *AMQPDeadLetterChannel

	directDLQ DeadLetterTopology
	groupDLQ  DeadLetterTopology

	persister *livePersister

	directRelayTracker    *liveDialTracker
	groupRelayTracker     *liveDialTracker
	directConsumerTracker *liveDialTracker
	groupConsumerTracker  *liveDialTracker
}

type liveWorkers struct {
	directRelay    *StreamRelay
	directConsumer *RabbitMessageConsumer
	groupRelay     *GroupStreamRelay
	groupConsumer  *GroupMessageConsumer

	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   chan struct{}
}

// liveDialTracker records every connection the wrapped dialer creates. Only
// the relay trackers are ever closed by the tests.
type liveDialTracker struct {
	mu          sync.Mutex
	dials       int
	connections []*amqp091.Connection
}

func (d *liveDialTracker) Dial(cfg config.RabbitMQConfig) (*amqp091.Connection, error) {
	connection, err := infra.DialRabbit(cfg)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.dials++
	d.connections = append(d.connections, connection)
	d.mu.Unlock()
	return connection, nil
}

func (d *liveDialTracker) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dials
}

// CloseAll closes every connection captured so far. It is only called on relay
// trackers, so consumers keep their existing broker connections.
func (d *liveDialTracker) CloseAll() int {
	d.mu.Lock()
	connections := append([]*amqp091.Connection(nil), d.connections...)
	d.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
	return len(connections)
}

// livePersister delegates to the real Store. Its only non-store behaviour is
// the explicitly labelled fallback fault window, used when a scoped MySQL lock
// cannot be proven on this server.
type livePersister struct {
	store *store.Store

	mu            sync.Mutex
	simulateUntil time.Time
}

func (p *livePersister) PersistMessage(ctx context.Context, message store.Message) (bool, error) {
	if p.simulating() {
		return false, driver.ErrBadConn
	}
	return p.store.PersistMessage(ctx, message)
}

func (p *livePersister) PersistGroupMessage(ctx context.Context, message store.GroupMessage) (bool, error) {
	if p.simulating() {
		return false, driver.ErrBadConn
	}
	return p.store.PersistGroupMessage(ctx, message)
}

func (p *livePersister) simulateFor(duration time.Duration) {
	p.mu.Lock()
	p.simulateUntil = time.Now().Add(duration)
	p.mu.Unlock()
}

func (p *livePersister) simulating() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return time.Now().Before(p.simulateUntil)
}

type livePublisher struct {
	channel  *amqp091.Channel
	confirms chan amqp091.Confirmation
	returns  chan amqp091.Return
}

func newLivePublisher(channel *amqp091.Channel) (*livePublisher, error) {
	if channel == nil {
		return nil, fmt.Errorf("live publisher channel is nil")
	}
	if err := channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable live publisher confirms: %w", err)
	}
	return &livePublisher{
		channel:  channel,
		confirms: channel.NotifyPublish(make(chan amqp091.Confirmation, 1)),
		returns:  channel.NotifyReturn(make(chan amqp091.Return, 1)),
	}, nil
}

func (p *livePublisher) Publish(
	ctx context.Context,
	exchange, routingKey, messageID string,
	payload []byte,
) error {
	if p == nil || p.channel == nil || p.confirms == nil {
		return fmt.Errorf("live publisher is not ready")
	}
	sequence := p.channel.GetNextPublishSeqNo()
	if err := p.channel.PublishWithContext(ctx, exchange, routingKey, true, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    messageID,
		Body:         payload,
	}); err != nil {
		return fmt.Errorf("publish live message: %w", err)
	}
	for {
		select {
		case confirmation, ok := <-p.confirms:
			if !ok {
				return fmt.Errorf("live publisher confirm channel closed")
			}
			if confirmation.DeliveryTag < sequence {
				continue
			}
			if confirmation.DeliveryTag != sequence {
				return fmt.Errorf("unexpected live publisher confirmation tag %d", confirmation.DeliveryTag)
			}
			if !confirmation.Ack {
				return fmt.Errorf("RabbitMQ rejected live message %s", messageID)
			}
			for {
				select {
				case returned, ok := <-p.returns:
					if !ok {
						return nil
					}
					if returned.MessageId == messageID {
						return fmt.Errorf("live message %s was returned", messageID)
					}
				default:
					return nil
				}
			}
		case returned, ok := <-p.returns:
			if !ok {
				p.returns = nil
				continue
			}
			if returned.MessageId == messageID {
				return fmt.Errorf("live message %s was unroutable", messageID)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type liveGapLock struct {
	conn *sql.Conn
	tx   *sql.Tx
	once sync.Once
}

func (l *liveGapLock) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		if l.tx != nil {
			_ = l.tx.Rollback()
		}
		if l.conn != nil {
			_ = l.conn.Close()
		}
	})
}

func TestV182PipelineLive(t *testing.T) {
	if strings.TrimSpace(os.Getenv("IM_TEST_PIPELINE_LIVE")) != "true" {
		t.Skip("IM_TEST_PIPELINE_LIVE is not true; skipping live pipeline test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), liveTestTimeout)
	defer cancel()

	env := liveOpenEnv(t, ctx)
	fixture := livePrepareState(t, ctx, env)
	workers := liveStartWorkers(t, ctx, env)

	steps := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "recovers real pending entries",
			run: func(t *testing.T) {
				liveCheckPendingRecovery(t, ctx, env, workers, fixture)
			},
		},
		{
			name: "relay reconnects after captured connections close",
			run: func(t *testing.T) {
				liveCheckRelayReconnect(t, ctx, env, workers)
			},
		},
		{
			name: "reconciler repairs old messages and is idempotent",
			run: func(t *testing.T) {
				liveCheckReconcile(t, ctx, env, fixture)
			},
		},
		{
			name: "poison envelopes dead-letter then replay",
			run: func(t *testing.T) {
				liveCheckPoisonAndReplay(t, ctx, env)
			},
		},
		{
			name: "removed dlq binding is redeclared after mandatory return",
			run: func(t *testing.T) {
				liveCheckDeadLetterBinding(t, ctx, env, workers)
			},
		},
		{
			name: "scoped insert lock drives transient retries",
			run: func(t *testing.T) {
				liveCheckScopedLock(t, ctx, env, workers)
			},
		},
	}
	for _, step := range steps {
		if !t.Run(step.name, step.run) {
			t.Logf("step=%q failed; skipping remaining live steps", step.name)
			return
		}
	}
}

func liveOpenEnv(t *testing.T, ctx context.Context) *liveTestEnv {
	t.Helper()

	infraConfig, err := config.LoadInfra()
	if err != nil {
		t.Fatalf("load infrastructure configuration: %v", err)
	}
	if !infraConfig.Redis.Enabled() {
		t.Fatal("IM_REDIS_URL or IM_REDIS_URL_FILE is required")
	}
	if !infraConfig.RabbitMQ.Enabled() {
		t.Fatal("IM_RABBITMQ_URL or IM_RABBITMQ_URL_FILE is required")
	}

	rabbitURL, err := url.Parse(infraConfig.RabbitMQ.URL)
	if err != nil {
		t.Fatalf("parse RabbitMQ URL: %v", err)
	}
	if rabbitURL.Scheme != "amqp" && rabbitURL.Scheme != "amqps" {
		t.Fatalf("RabbitMQ URL must use amqp or amqps")
	}
	if rabbitURL.Host == "" {
		t.Fatal("RabbitMQ URL has no host")
	}
	if strings.Trim(rabbitURL.Path, "/") != liveRabbitVHost {
		t.Fatalf("RabbitMQ URL must target vhost %q; refusing to use another vhost", liveRabbitVHost)
	}

	namespace, err := NewNamespace(liveNamespaceBase)
	if err != nil {
		t.Fatalf("configure live namespace: %v", err)
	}
	keyspace := NewKeyspace(namespace)

	redisOptions, err := redis.ParseURL(infraConfig.Redis.URL)
	if err != nil {
		t.Fatalf("parse Redis URL: %v", err)
	}
	if infraConfig.Redis.Password != "" {
		redisOptions.Password = infraConfig.Redis.Password
	}
	// DB 1 is mandatory for this drill even if the environment names another
	// database.
	redisOptions.DB = 1
	redisOptions.DialTimeout = infraConfig.Redis.DialTimeout
	redisOptions.ReadTimeout = infraConfig.Redis.ReadTimeout
	redisOptions.WriteTimeout = infraConfig.Redis.WriteTimeout
	redisClient := redis.NewClient(redisOptions)

	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	defer cancelPing()
	if err := redisClient.Ping(pingCtx).Err(); err != nil {
		_ = redisClient.Close()
		t.Fatalf("ping Redis: %v", err)
	}
	clientInfo, err := redisClient.Do(pingCtx, "CLIENT", "INFO").Text()
	if err != nil {
		_ = redisClient.Close()
		t.Fatalf("read Redis client info: %v", err)
	}
	if !strings.Contains(clientInfo, "db=1") {
		_ = redisClient.Close()
		t.Fatal("Redis connection is not on DB 1")
	}

	// Refuse to share a namespace: any pre-existing key means a previous drill
	// did not clean up. The guard key is created only after this check.
	cursor := uint64(0)
	for {
		keys, next, err := redisClient.Scan(ctx, cursor, keyspace.prefixOrDefault()+"*", 200).Result()
		if err != nil {
			_ = redisClient.Close()
			t.Fatalf("scan live namespace: %v", err)
		}
		if len(keys) > 0 {
			_ = redisClient.Close()
			t.Fatalf("live namespace %q already contains %d keys; refusing to reuse it", liveNamespaceBase, len(keys))
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	guardKey := keyspace.prefixOrDefault() + "drill:guard"
	created, err := redisClient.SetNX(ctx, guardKey, "1", 0).Result()
	if err != nil {
		_ = redisClient.Close()
		t.Fatalf("create live namespace guard: %v", err)
	}
	if !created {
		_ = redisClient.Close()
		t.Fatal("live namespace guard already exists")
	}
	t.Cleanup(func() {
		liveDeleteRedisNamespace(t, redisClient, keyspace)
		_ = redisClient.Close()
	})

	dataStore, rawDB := liveOpenMySQL(t, ctx)

	adminConn, adminCh, publisher, dlqChannel := liveOpenRabbit(t, infraConfig.RabbitMQ, namespace)

	env := &liveTestEnv{
		namespace: namespace,
		keyspace:  keyspace,
		redis:     redisClient,
		rabbit:    infraConfig.RabbitMQ,
		store:     dataStore,
		db:        rawDB,
		adminConn: adminConn,
		adminCh:   adminCh,
		publisher: publisher,
		dlq:       dlqChannel,
		directDLQ: DeadLetterTopologyFor(namespace, DeadLetterDirect),
		groupDLQ:  DeadLetterTopologyFor(namespace, DeadLetterGroup),
		persister: &livePersister{store: dataStore},

		directRelayTracker:    &liveDialTracker{},
		groupRelayTracker:     &liveDialTracker{},
		directConsumerTracker: &liveDialTracker{},
		groupConsumerTracker:  &liveDialTracker{},
	}
	return env
}

func liveOpenMySQL(t *testing.T, ctx context.Context) (*store.Store, *sql.DB) {
	t.Helper()

	dsn := strings.TrimSpace(os.Getenv("IM_TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Fatal("IM_TEST_MYSQL_DSN is required for the live pipeline test")
	}
	base, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse IM_TEST_MYSQL_DSN: %v", err)
	}
	if base.Passwd != "" {
		t.Fatal("IM_TEST_MYSQL_DSN must not contain a password; use IM_TEST_MYSQL_PASSWORD_FILE")
	}
	if path := strings.TrimSpace(os.Getenv("IM_TEST_MYSQL_PASSWORD_FILE")); path != "" {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read IM_TEST_MYSQL_PASSWORD_FILE: %v", err)
		}
		base.Passwd = strings.TrimRight(string(contents), "\r\n")
	}

	cfg := *base
	cfg.DBName = ""
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.Timeout = 10 * time.Second
	cfg.ReadTimeout = 30 * time.Second
	cfg.WriteTimeout = 30 * time.Second
	cfg.ClientFoundRows = false
	cfg.MultiStatements = false
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	// Scope the lock wait to one second so a blocked consumer sees a real 1205
	// promptly. REPEATABLE READ is what makes the explicit gap lock meaningful.
	cfg.Params["innodb_lock_wait_timeout"] = "1"
	cfg.Params["transaction_isolation"] = "'REPEATABLE-READ'"
	cfg.Params["sql_mode"] = "'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES'"

	adminDB, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("open MySQL admin connection: %v", err)
	}
	t.Cleanup(func() { _ = adminDB.Close() })
	adminDB.SetMaxOpenConns(4)
	adminDB.SetMaxIdleConns(2)

	setupCtx, cancelSetup := context.WithTimeout(ctx, 20*time.Second)
	defer cancelSetup()
	if err := adminDB.PingContext(setupCtx); err != nil {
		t.Fatalf("ping MySQL: %v", err)
	}

	// CREATE DATABASE (not IF NOT EXISTS): an existing schema is an error and
	// is never dropped by this test.
	if _, err := adminDB.ExecContext(setupCtx,
		"CREATE DATABASE `"+liveSchemaName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci",
	); err != nil {
		t.Fatalf("create dedicated schema %s: %v; an existing schema is never reused or dropped", liveSchemaName, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := adminDB.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+liveSchemaName+"`"); err != nil {
			t.Logf("dedicated schema %s left behind: %v", liveSchemaName, err)
		}
	})

	storeCfg := cfg
	storeCfg.DBName = liveSchemaName
	dataStore, err := store.Open(ctx, storeCfg.FormatDSN())
	if err != nil {
		t.Fatalf("open dedicated schema %s: %v", liveSchemaName, err)
	}
	t.Cleanup(func() { _ = dataStore.Close() })
	if err := dataStore.Migrate(ctx); err != nil {
		t.Fatalf("migrate dedicated schema %s: %v", liveSchemaName, err)
	}

	rawDB, err := sql.Open("mysql", storeCfg.FormatDSN())
	if err != nil {
		t.Fatalf("open dedicated fixture connection: %v", err)
	}
	rawDB.SetMaxOpenConns(8)
	rawDB.SetMaxIdleConns(4)
	t.Cleanup(func() { _ = rawDB.Close() })
	return dataStore, rawDB
}

func liveOpenRabbit(
	t *testing.T,
	rabbitConfig config.RabbitMQConfig,
	namespace Namespace,
) (*amqp091.Connection, *amqp091.Channel, *livePublisher, *AMQPDeadLetterChannel) {
	t.Helper()

	connection, err := infra.DialRabbit(rabbitConfig)
	if err != nil {
		t.Fatalf("connect RabbitMQ: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	// Register topology cleanup before declaring anything so a partial
	// declaration is still removed.
	t.Cleanup(func() { liveDeleteRabbitTopology(t, rabbitConfig, namespace) })

	channel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open RabbitMQ admin channel: %v", err)
	}
	if err := declareDirectTopology(channel, namespace); err != nil {
		t.Fatalf("declare direct topology: %v", err)
	}
	if err := declareGroupTopology(channel, namespace); err != nil {
		t.Fatalf("declare group topology: %v", err)
	}
	if err := DeclareDeadLetterTopology(channel, DeadLetterTopologyFor(namespace, DeadLetterDirect)); err != nil {
		t.Fatalf("declare direct dead letter topology: %v", err)
	}
	if err := DeclareDeadLetterTopology(channel, DeadLetterTopologyFor(namespace, DeadLetterGroup)); err != nil {
		t.Fatalf("declare group dead letter topology: %v", err)
	}

	publishChannel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open RabbitMQ publish channel: %v", err)
	}
	publisher, err := newLivePublisher(publishChannel)
	if err != nil {
		t.Fatalf("prepare RabbitMQ publisher: %v", err)
	}
	dlqChannel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open RabbitMQ dead letter admin channel: %v", err)
	}
	dlq, err := NewAMQPDeadLetterChannel(dlqChannel)
	if err != nil {
		t.Fatalf("prepare RabbitMQ dead letter admin channel: %v", err)
	}
	return connection, channel, publisher, dlq
}

func liveDeleteRabbitTopology(t *testing.T, rabbitConfig config.RabbitMQConfig, namespace Namespace) {
	t.Helper()
	connection, err := infra.DialRabbit(rabbitConfig)
	if err != nil {
		t.Logf("cleanup RabbitMQ topology: dial failed: %v", err)
		return
	}
	defer func() { _ = connection.Close() }()
	channel, err := connection.Channel()
	if err != nil {
		t.Logf("cleanup RabbitMQ topology: channel failed: %v", err)
		return
	}
	directDLQ := DeadLetterTopologyFor(namespace, DeadLetterDirect)
	groupDLQ := DeadLetterTopologyFor(namespace, DeadLetterGroup)
	for _, queue := range []string{
		directQueueName(namespace),
		directDLQ.Queue,
		groupQueueName(namespace),
		groupDLQ.Queue,
	} {
		if _, err := channel.QueueDelete(queue, false, false, false); err != nil {
			t.Logf("cleanup RabbitMQ queue %s: %v", queue, err)
		}
	}
	for _, exchange := range []string{
		directExchangeName(namespace),
		directDLQ.Exchange,
		groupExchangeName(namespace),
		groupDLQ.Exchange,
	} {
		if err := channel.ExchangeDelete(exchange, false, false); err != nil {
			t.Logf("cleanup RabbitMQ exchange %s: %v", exchange, err)
		}
	}
}

func liveDeleteRedisNamespace(t *testing.T, client *redis.Client, keyspace Keyspace) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cursor := uint64(0)
	for {
		keys, next, err := client.Scan(ctx, cursor, keyspace.prefixOrDefault()+"*", 200).Result()
		if err != nil {
			t.Logf("cleanup live Redis namespace: scan failed: %v", err)
			return
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Logf("cleanup live Redis namespace: delete failed: %v", err)
				return
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

func livePrepareState(t *testing.T, ctx context.Context, env *liveTestEnv) *liveFixture {
	t.Helper()

	liveSeedSQL(t, ctx, env.db)
	liveSeedRedis(t, ctx, env.redis, env.keyspace)

	pendingDirect := liveAcceptDirect(t, ctx, env, 1, 2, "live-pending-direct", time.Now().UTC())
	pendingGroup := liveAcceptGroup(t, ctx, env, 1, 1, "live-pending-group", time.Now().UTC())

	liveCreateAndReadPending(
		t, ctx, env.redis,
		env.keyspace.messageStreamKey(),
		directRelayGroup(env.namespace),
		directConsumerName(env.namespace),
	)
	liveCreateAndReadPending(
		t, ctx, env.redis,
		env.keyspace.groupMessageStreamKey(),
		groupRelayName(env.namespace),
		groupConsumerName(env.namespace),
	)

	old := time.Now().UTC().Add(-10 * time.Minute)
	reconcile := liveReconcileFixture{
		oldDirect:     liveAcceptDirect(t, ctx, env, 7, 8, "live-reconcile-old", old),
		oldGroup:      liveAcceptGroup(t, ctx, env, 1, 4, "live-reconcile-group", old),
		missingEntity: liveAcceptDirect(t, ctx, env, 9, 10, "live-reconcile-missing-entity", old),
		missingIndex:  liveAcceptDirect(t, ctx, env, 11, 12, "live-reconcile-missing-index", old),
		young:         liveAcceptDirect(t, ctx, env, 13, 14, "live-reconcile-young", time.Now().UTC()),
	}

	// Simulate an accepted message whose stream entry never reached the broker
	// while keeping the Redis entity/counter/index exactly as acceptance wrote
	// them.
	liveDeleteStreamEntry(t, ctx, env.redis, env.keyspace.messageStreamKey(), reconcile.oldDirect.ID)
	liveDeleteStreamEntry(t, ctx, env.redis, env.keyspace.groupMessageStreamKey(), reconcile.oldGroup.ID)
	liveDeleteStreamEntry(t, ctx, env.redis, env.keyspace.messageStreamKey(), reconcile.missingEntity.ID)
	liveDeleteStreamEntry(t, ctx, env.redis, env.keyspace.messageStreamKey(), reconcile.missingIndex.ID)
	liveDeleteStreamEntry(t, ctx, env.redis, env.keyspace.messageStreamKey(), reconcile.young.ID)

	// Remove only the entity of one message and only the conversation index
	// member of another.
	if err := env.redis.Del(ctx, env.keyspace.messageKey(reconcile.missingEntity.ID)).Err(); err != nil {
		t.Fatalf("remove missing-entity message hash: %v", err)
	}
	if err := env.redis.ZRem(
		ctx,
		env.keyspace.directPairKey(reconcile.missingIndex.SenderID, reconcile.missingIndex.RecipientID),
		strconv.FormatUint(reconcile.missingIndex.ID, 10),
	).Err(); err != nil {
		t.Fatalf("remove missing-index conversation member: %v", err)
	}

	return &liveFixture{
		pendingDirect: pendingDirect,
		pendingGroup:  pendingGroup,
		reconcile:     reconcile,
	}
}

func liveSeedSQL(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	createdAt := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Millisecond)

	for id := uint64(1); id <= 16; id++ {
		username := fmt.Sprintf("live%02d", id)
		liveExec(t, ctx, db, "seed users",
			`INSERT INTO users (id, username, username_key, nickname, password_hash) VALUES (?, ?, ?, ?, ?)`,
			id, username, username, username, "test-hash")
	}
	for _, pair := range [][2]uint64{
		{1, 2}, {3, 4}, {5, 6}, {7, 8},
		{9, 10}, {11, 12}, {13, 14}, {15, 16},
	} {
		liveExec(t, ctx, db, "seed friendships",
			`INSERT INTO friendships (user_id, friend_user_id, created_at) VALUES (?, ?, ?)`,
			pair[0], pair[1], createdAt)
	}
	for _, groupID := range []uint64{1, 2, 3, 4} {
		liveExec(t, ctx, db, "seed group",
			`INSERT INTO chat_groups (id, name, owner_id, created_at) VALUES (?, ?, 1, ?)`,
			groupID, fmt.Sprintf("live group %d", groupID), createdAt)
	}
	for _, member := range [][2]uint64{{1, 1}, {1, 2}, {2, 3}, {3, 1}, {4, 1}} {
		liveExec(t, ctx, db, "seed group member",
			`INSERT INTO group_members (group_id, user_id, joined_at, last_read_message_id, last_read_seq)
			 VALUES (?, ?, ?, 0, 0)`,
			member[0], member[1], createdAt)
	}

	// Existing row at seq 1 gives the gap lock below a bounded next-key gap at
	// seq 2 for both chains.
	liveExec(t, ctx, db, "seed direct lock row",
		`INSERT INTO messages
		   (server_message_id, conversation_low_id, conversation_high_id, seq,
		    sender_id, recipient_id, client_message_id, body, created_at)
		 VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?)`,
		800001, liveLockDirectLow, liveLockDirectHigh, liveLockDirectLow, liveLockDirectHigh,
		"live-seed-lock-direct", "live seed direct", createdAt)
	liveExec(t, ctx, db, "seed group lock row",
		`INSERT INTO group_messages
		   (server_message_id, seq, group_id, sender_id, client_message_id, body, created_at)
		 VALUES (?, 1, ?, ?, ?, ?, ?)`,
		800002, liveLockGroupID, liveLockDirectLow, "live-seed-lock-group", "live seed group", createdAt)
}

func liveSeedRedis(t *testing.T, ctx context.Context, client *redis.Client, keyspace Keyspace) {
	t.Helper()
	if err := client.Set(ctx, keyspace.sequenceReadyKey(), "1", 0).Err(); err != nil {
		t.Fatalf("seed sequence-ready marker: %v", err)
	}
	for _, pair := range [][2]uint64{
		{1, 2}, {3, 4}, {5, 6}, {7, 8},
		{9, 10}, {11, 12}, {13, 14}, {15, 16},
	} {
		if err := client.SAdd(ctx, keyspace.friendshipKey(pair[0]), pair[1]).Err(); err != nil {
			t.Fatalf("seed friendship projection: %v", err)
		}
	}
	for _, member := range [][2]uint64{{1, 1}, {1, 2}, {2, 3}, {3, 1}, {4, 1}} {
		if err := client.SAdd(ctx, keyspace.groupMembersKey(member[0]), member[1]).Err(); err != nil {
			t.Fatalf("seed group projection: %v", err)
		}
	}
	for _, groupID := range []uint64{1, 2, 3, 4} {
		if err := client.Del(
			ctx,
			keyspace.groupDissolvedKey(groupID),
			keyspace.groupSyncRequiredKey(groupID),
		).Err(); err != nil {
			t.Fatalf("clear group lifecycle projection: %v", err)
		}
	}
	// The accepted lock messages must be seq 2, behind the seeded MySQL rows.
	if err := client.Set(ctx, keyspace.directSequenceKey(liveLockDirectLow, liveLockDirectHigh), "1", 0).Err(); err != nil {
		t.Fatalf("seed direct lock counter: %v", err)
	}
	if err := client.Set(ctx, keyspace.groupSequenceKey(liveLockGroupID), "1", 0).Err(); err != nil {
		t.Fatalf("seed group lock counter: %v", err)
	}
}

func liveAcceptDirect(
	t *testing.T,
	ctx context.Context,
	env *liveTestEnv,
	senderID, recipientID uint64,
	clientMessageID string,
	createdAt time.Time,
) store.Message {
	t.Helper()
	acceptor := NewRedisAcceptor(env.redis, RedisAcceptorOptions{Keyspace: env.keyspace})
	result, err := acceptor.Accept(ctx, DirectMessageRequest{
		SenderID:        senderID,
		RecipientID:     recipientID,
		ClientMessageID: clientMessageID,
		Body:            "live " + clientMessageID,
		CreatedAt:       createdAt,
	})
	if err != nil {
		t.Fatalf("accept direct message: %v", err)
	}
	if !result.Created {
		t.Fatal("accept direct message unexpectedly deduplicated")
	}
	return result.Message
}

func liveAcceptGroup(
	t *testing.T,
	ctx context.Context,
	env *liveTestEnv,
	senderID, groupID uint64,
	clientMessageID string,
	createdAt time.Time,
) store.GroupMessage {
	t.Helper()
	acceptor := NewGroupRedisAcceptor(env.redis, env.keyspace, RedisAcceptorOptions{Keyspace: env.keyspace})
	name := fmt.Sprintf("live%02d", senderID)
	result, err := acceptor.Accept(ctx, GroupMessageRequest{
		SenderID:        senderID,
		GroupID:         groupID,
		ClientMessageID: clientMessageID,
		Body:            "live " + clientMessageID,
		CreatedAt:       createdAt,
		SenderUsername:  name,
		SenderNickname:  name,
	})
	if err != nil {
		t.Fatalf("accept group message: %v", err)
	}
	if !result.Created {
		t.Fatal("accept group message unexpectedly deduplicated")
	}
	return result.Message
}

func liveCreateAndReadPending(
	t *testing.T,
	ctx context.Context,
	client *redis.Client,
	stream, group, consumer string,
) {
	t.Helper()
	if err := client.XGroupCreateMkStream(ctx, stream, group, "0-0").Err(); err != nil &&
		!strings.Contains(err.Error(), "BUSYGROUP") {
		t.Fatalf("create relay consumer group: %v", err)
	}
	streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    10,
	}).Result()
	if err != nil {
		t.Fatalf("read pending stream entry: %v", err)
	}
	if relayStreamCount(streams) != 1 {
		t.Fatalf("pending stream entries = %d, want 1", relayStreamCount(streams))
	}
	pending, err := client.XPending(ctx, stream, group).Result()
	if err != nil {
		t.Fatalf("read pending count: %v", err)
	}
	if pending.Count != 1 {
		t.Fatalf("pending count = %d, want 1", pending.Count)
	}
}

func liveDeleteStreamEntry(t *testing.T, ctx context.Context, client *redis.Client, stream string, messageID uint64) {
	t.Helper()
	want := strconv.FormatUint(messageID, 10)
	entries, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("read stream for entry removal: %v", err)
	}
	deleted := int64(0)
	for _, entry := range entries {
		raw, ok := entry.Values["id"]
		if !ok || fmt.Sprint(raw) != want {
			continue
		}
		count, err := client.XDel(ctx, stream, entry.ID).Result()
		if err != nil {
			t.Fatalf("delete stream entry: %v", err)
		}
		deleted += count
	}
	if deleted != 1 {
		t.Fatalf("deleted stream entries = %d, want 1", deleted)
	}
}

func liveStartWorkers(t *testing.T, ctx context.Context, env *liveTestEnv) *liveWorkers {
	t.Helper()

	workerCtx, cancel := context.WithCancel(ctx)
	workers := &liveWorkers{
		directRelay: NewStreamRelayWithNamespace(
			env.redis, env.rabbit, env.directRelayTracker.Dial, env.keyspace, env.namespace,
		),
		directConsumer: NewRabbitMessageConsumerWithNamespace(
			env.rabbit, env.directConsumerTracker.Dial, env.persister, env.namespace,
		),
		groupRelay: NewGroupStreamRelay(
			env.redis, env.rabbit, env.groupRelayTracker.Dial, env.keyspace, env.namespace,
		),
		groupConsumer: NewGroupMessageConsumer(
			env.rabbit, env.groupConsumerTracker.Dial, env.persister, env.namespace,
		),
		cancel: cancel,
		done:   make(chan struct{}),
	}

	workers.wg.Add(4)
	go func() {
		defer workers.wg.Done()
		if err := workers.directRelay.Run(workerCtx); err != nil && workerCtx.Err() == nil {
			t.Logf("live direct relay stopped: %v", err)
		}
	}()
	go func() {
		defer workers.wg.Done()
		if err := workers.directConsumer.Run(workerCtx); err != nil && workerCtx.Err() == nil {
			t.Logf("live direct consumer stopped: %v", err)
		}
	}()
	go func() {
		defer workers.wg.Done()
		if err := workers.groupRelay.Run(workerCtx); err != nil && workerCtx.Err() == nil {
			t.Logf("live group relay stopped: %v", err)
		}
	}()
	go func() {
		defer workers.wg.Done()
		if err := workers.groupConsumer.Run(workerCtx); err != nil && workerCtx.Err() == nil {
			t.Logf("live group consumer stopped: %v", err)
		}
	}()
	go func() {
		workers.wg.Wait()
		close(workers.done)
	}()
	t.Cleanup(func() { workers.stop(t) })

	liveEventually(t, ctx, 20*time.Second, "direct relay connects", func(context.Context) (bool, error) {
		return workers.directRelay.Status().Connected, nil
	})
	liveEventually(t, ctx, 20*time.Second, "group relay connects", func(context.Context) (bool, error) {
		return workers.groupRelay.Status().Connected, nil
	})
	liveEventually(t, ctx, 20*time.Second, "direct consumer connects", func(context.Context) (bool, error) {
		return env.directConsumerTracker.Count() > 0, nil
	})
	liveEventually(t, ctx, 20*time.Second, "group consumer connects", func(context.Context) (bool, error) {
		return env.groupConsumerTracker.Count() > 0, nil
	})
	return workers
}

func (w *liveWorkers) stop(t *testing.T) {
	t.Helper()
	if w == nil {
		return
	}
	w.cancel()
	select {
	case <-w.done:
	case <-time.After(30 * time.Second):
		t.Log("live worker shutdown timed out")
	}
}

func liveCheckPendingRecovery(
	t *testing.T,
	ctx context.Context,
	env *liveTestEnv,
	workers *liveWorkers,
	fixture *liveFixture,
) {
	t.Helper()

	liveEventually(t, ctx, 30*time.Second, "pending direct message persists", func(checkCtx context.Context) (bool, error) {
		return liveDirectRowExists(checkCtx, env.db, fixture.pendingDirect.ID)
	})
	liveEventually(t, ctx, 30*time.Second, "pending group message persists", func(checkCtx context.Context) (bool, error) {
		return liveGroupRowExists(checkCtx, env.db, fixture.pendingGroup.ID)
	})
	liveEventually(t, ctx, 30*time.Second, "direct stream drains", func(checkCtx context.Context) (bool, error) {
		return liveStreamEmpty(checkCtx, env.redis, env.keyspace.messageStreamKey())
	})
	liveEventually(t, ctx, 30*time.Second, "group stream drains", func(checkCtx context.Context) (bool, error) {
		return liveStreamEmpty(checkCtx, env.redis, env.keyspace.groupMessageStreamKey())
	})
	liveEventually(t, ctx, 30*time.Second, "direct pending drains", func(checkCtx context.Context) (bool, error) {
		return livePendingEmpty(checkCtx, env.redis, env.keyspace.messageStreamKey(), directRelayGroup(env.namespace))
	})
	liveEventually(t, ctx, 30*time.Second, "group pending drains", func(checkCtx context.Context) (bool, error) {
		return livePendingEmpty(checkCtx, env.redis, env.keyspace.groupMessageStreamKey(), groupRelayName(env.namespace))
	})

	directRecovered := workers.directRelay.Status().Recovered
	groupRecovered := workers.groupRelay.Status().Recovered
	if directRecovered < 1 || groupRecovered < 1 {
		t.Fatalf("recovered direct=%d group=%d, want both at least 1", directRecovered, groupRecovered)
	}
	t.Logf("step=pending-recovery direct_rows=1 group_rows=1 direct_recovered=%d group_recovered=%d",
		directRecovered, groupRecovered)
}

func liveCheckRelayReconnect(t *testing.T, ctx context.Context, env *liveTestEnv, workers *liveWorkers) {
	t.Helper()

	directBefore := env.directRelayTracker.Count()
	groupBefore := env.groupRelayTracker.Count()
	closedDirect := env.directRelayTracker.CloseAll()
	closedGroup := env.groupRelayTracker.CloseAll()
	if closedDirect < 1 || closedGroup < 1 {
		t.Fatalf("captured relay connections direct=%d group=%d, want both at least 1", closedDirect, closedGroup)
	}

	directMessage := liveAcceptDirect(t, ctx, env, 1, 2, "live-relay-reconnect-direct", time.Now().UTC())
	groupMessage := liveAcceptGroup(t, ctx, env, 1, 1, "live-relay-reconnect-group", time.Now().UTC())

	liveEventually(t, ctx, 30*time.Second, "direct relay redials", func(context.Context) (bool, error) {
		return env.directRelayTracker.Count() > directBefore, nil
	})
	liveEventually(t, ctx, 30*time.Second, "group relay redials", func(context.Context) (bool, error) {
		return env.groupRelayTracker.Count() > groupBefore, nil
	})
	liveEventually(t, ctx, 30*time.Second, "reconnected direct message persists", func(checkCtx context.Context) (bool, error) {
		return liveDirectRowExists(checkCtx, env.db, directMessage.ID)
	})
	liveEventually(t, ctx, 30*time.Second, "reconnected group message persists", func(checkCtx context.Context) (bool, error) {
		return liveGroupRowExists(checkCtx, env.db, groupMessage.ID)
	})
	liveEventually(t, ctx, 30*time.Second, "direct stream drains after reconnect", func(checkCtx context.Context) (bool, error) {
		return liveStreamEmpty(checkCtx, env.redis, env.keyspace.messageStreamKey())
	})
	liveEventually(t, ctx, 30*time.Second, "group stream drains after reconnect", func(checkCtx context.Context) (bool, error) {
		return liveStreamEmpty(checkCtx, env.redis, env.keyspace.groupMessageStreamKey())
	})

	t.Logf("step=relay-reconnect closed_direct=%d closed_group=%d direct_dials=%d group_dials=%d",
		closedDirect, closedGroup, env.directRelayTracker.Count(), env.groupRelayTracker.Count())
}

func liveCheckReconcile(t *testing.T, ctx context.Context, env *liveTestEnv, fixture *liveFixture) {
	t.Helper()
	reconcile := fixture.reconcile

	for _, pair := range [][2]uint64{
		{reconcile.oldDirect.SenderID, reconcile.oldDirect.RecipientID},
		{reconcile.missingEntity.SenderID, reconcile.missingEntity.RecipientID},
		{reconcile.missingIndex.SenderID, reconcile.missingIndex.RecipientID},
		{reconcile.young.SenderID, reconcile.young.RecipientID},
	} {
		count, err := liveConversationRowCount(ctx, env.db, pair[0], pair[1])
		if err != nil {
			t.Fatalf("count pre-reconcile rows: %v", err)
		}
		if count != 0 {
			t.Fatalf("pre-reconcile conversation rows = %d, want 0", count)
		}
	}
	if count, err := liveGroupRowCount(ctx, env.db, reconcile.oldGroup.GroupID); err != nil {
		t.Fatalf("count pre-reconcile group rows: %v", err)
	} else if count != 0 {
		t.Fatalf("pre-reconcile group rows = %d, want 0", count)
	}

	reconciler := NewReconciler(env.redis, env.store, ReconcileOptions{Keyspace: env.keyspace})
	first, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("first Reconcile RunOnce: %v", err)
	}
	if first.Repaired < 2 {
		t.Fatalf("first reconcile repaired = %d, want at least 2", first.Repaired)
	}
	if first.Unresolved < 2 {
		t.Fatalf("first reconcile unresolved = %d, want at least 2", first.Unresolved)
	}
	if first.InFlight < 1 {
		t.Fatalf("first reconcile in_flight = %d, want at least 1", first.InFlight)
	}

	directSeq, directClient, directBody, found, err := liveReadDirectRow(ctx, env.db, reconcile.oldDirect.ID)
	if err != nil {
		t.Fatalf("read repaired direct row: %v", err)
	}
	if !found || directSeq != reconcile.oldDirect.Seq || directClient != reconcile.oldDirect.ClientMessageID ||
		directBody != reconcile.oldDirect.Body {
		t.Fatal("repaired direct row does not match the Redis entity")
	}
	groupSeq, groupClient, groupBody, found, err := liveReadGroupRow(ctx, env.db, reconcile.oldGroup.ID)
	if err != nil {
		t.Fatalf("read repaired group row: %v", err)
	}
	if !found || groupSeq != reconcile.oldGroup.Seq || groupClient != reconcile.oldGroup.ClientMessageID ||
		groupBody != reconcile.oldGroup.Body {
		t.Fatal("repaired group row does not match the Redis entity")
	}

	for _, message := range []store.Message{reconcile.missingEntity, reconcile.missingIndex, reconcile.young} {
		count, err := liveConversationRowCount(ctx, env.db, message.SenderID, message.RecipientID)
		if err != nil {
			t.Fatalf("count unresolved conversation rows: %v", err)
		}
		if count != 0 {
			t.Fatalf("unresolved/in-flight conversation rows = %d, want 0", count)
		}
	}
	blankDirect, err := liveBlankRowCount(ctx, env.db, "messages")
	if err != nil {
		t.Fatalf("count blank direct rows: %v", err)
	}
	blankGroup, err := liveBlankRowCount(ctx, env.db, "group_messages")
	if err != nil {
		t.Fatalf("count blank group rows: %v", err)
	}
	if blankDirect != 0 || blankGroup != 0 {
		t.Fatalf("blank rows direct=%d group=%d, want 0", blankDirect, blankGroup)
	}

	second, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("second Reconcile RunOnce: %v", err)
	}
	if second.Repaired != 0 {
		t.Fatalf("second reconcile repaired = %d, want 0", second.Repaired)
	}
	count, err := liveConversationRowCount(ctx, env.db, reconcile.oldDirect.SenderID, reconcile.oldDirect.RecipientID)
	if err != nil || count != 1 {
		t.Fatalf("repaired direct rows after second run = %d (err %v), want 1", count, err)
	}
	count, err = liveGroupRowCount(ctx, env.db, reconcile.oldGroup.GroupID)
	if err != nil || count != 1 {
		t.Fatalf("repaired group rows after second run = %d (err %v), want 1", count, err)
	}

	t.Logf("step=reconcile first_repaired=%d first_unresolved=%d first_in_flight=%d second_repaired=%d",
		first.Repaired, first.Unresolved, first.InFlight, second.Repaired)
}

func liveCheckPoisonAndReplay(t *testing.T, ctx context.Context, env *liveTestEnv) {
	t.Helper()
	createdAt := time.Now().UTC()

	directPoison := store.Message{
		ID:              920001,
		Seq:             1,
		SenderID:        9001,
		RecipientID:     5,
		ClientMessageID: "live-poison-direct",
		Body:            "live poison direct",
		CreatedAt:       createdAt,
	}
	groupPoison := store.GroupMessage{
		ID:              920002,
		Seq:             1,
		GroupID:         9001,
		SenderID:        1,
		Sender:          store.PublicUser{Username: "live01", Nickname: "live01"},
		ClientMessageID: "live-poison-group",
		Body:            "live poison group",
		CreatedAt:       createdAt,
	}
	directValid := store.Message{
		ID:              920003,
		Seq:             1,
		SenderID:        5,
		RecipientID:     6,
		ClientMessageID: "live-poison-followup-direct",
		Body:            "live valid direct",
		CreatedAt:       createdAt,
	}
	groupValid := store.GroupMessage{
		ID:              920004,
		Seq:             1,
		GroupID:         3,
		SenderID:        1,
		Sender:          store.PublicUser{Username: "live01", Nickname: "live01"},
		ClientMessageID: "live-poison-followup-group",
		Body:            "live valid group",
		CreatedAt:       createdAt,
	}

	directPoisonPayload, err := encodeDirectMessage(directPoison)
	if err != nil {
		t.Fatalf("encode direct poison: %v", err)
	}
	groupPoisonPayload, err := encodeGroupMessage(groupPoison)
	if err != nil {
		t.Fatalf("encode group poison: %v", err)
	}
	directValidPayload, err := encodeDirectMessage(directValid)
	if err != nil {
		t.Fatalf("encode direct follow-up: %v", err)
	}
	groupValidPayload, err := encodeGroupMessage(groupValid)
	if err != nil {
		t.Fatalf("encode group follow-up: %v", err)
	}

	start := time.Now()
	livePublishDirect(t, ctx, env, directPoison.ID, directPoisonPayload)
	livePublishGroup(t, ctx, env, groupPoison.ID, groupPoisonPayload)
	livePublishDirect(t, ctx, env, directValid.ID, directValidPayload)
	livePublishGroup(t, ctx, env, groupValid.ID, groupValidPayload)

	directEntry := liveWaitDeadLetter(t, ctx, env, env.directDLQ, 60*time.Second, func(entry DeadLetterEntry) bool {
		return entry.ID == directPoison.ID && entry.Attempts == 6
	})
	groupEntry := liveWaitDeadLetter(t, ctx, env, env.groupDLQ, 60*time.Second, func(entry DeadLetterEntry) bool {
		return entry.ID == groupPoison.ID && entry.Attempts == 6
	})
	if directEntry.Reason != dlqReasonRetriesExhausted || groupEntry.Reason != dlqReasonRetriesExhausted {
		t.Fatalf("poison reasons direct=%q group=%q, want %q",
			directEntry.Reason, groupEntry.Reason, dlqReasonRetriesExhausted)
	}
	elapsed := time.Since(start)
	if elapsed < 30*time.Second {
		t.Fatalf("poison dead-lettered after %s, want at least 30s of bounded retries", elapsed)
	}

	liveEventually(t, ctx, 45*time.Second, "valid direct follow-up persists", func(checkCtx context.Context) (bool, error) {
		return liveDirectRowExists(checkCtx, env.db, directValid.ID)
	})
	liveEventually(t, ctx, 45*time.Second, "valid group follow-up persists", func(checkCtx context.Context) (bool, error) {
		return liveGroupRowExists(checkCtx, env.db, groupValid.ID)
	})

	// Repair the foreign keys the poison envelopes intentionally violated.
	liveExec(t, ctx, env.db, "repair poison direct user",
		`INSERT INTO users (id, username, username_key, nickname, password_hash) VALUES (?, ?, ?, ?, ?)`,
		9001, "livepoison", "livepoison", "livepoison", "test-hash")
	liveExec(t, ctx, env.db, "repair poison group",
		`INSERT INTO chat_groups (id, name, owner_id, created_at) VALUES (?, ?, 1, ?)`,
		9001, "live recovered group", time.Now().UTC())

	directReplay, err := ReplayDeadLetters(ctx, env.dlq, env.directDLQ, directPoison.ID, 100)
	if err != nil {
		t.Fatalf("replay direct poison: %v", err)
	}
	groupReplay, err := ReplayDeadLetters(ctx, env.dlq, env.groupDLQ, groupPoison.ID, 100)
	if err != nil {
		t.Fatalf("replay group poison: %v", err)
	}
	if directReplay.Replayed != 1 || groupReplay.Replayed != 1 {
		t.Fatalf("replayed direct=%d group=%d, want both 1", directReplay.Replayed, groupReplay.Replayed)
	}
	liveEventually(t, ctx, 45*time.Second, "replayed direct poison persists", func(checkCtx context.Context) (bool, error) {
		return liveDirectRowExists(checkCtx, env.db, directPoison.ID)
	})
	liveEventually(t, ctx, 45*time.Second, "replayed group poison persists", func(checkCtx context.Context) (bool, error) {
		return liveGroupRowExists(checkCtx, env.db, groupPoison.ID)
	})

	repeatDirect, err := ReplayDeadLetters(ctx, env.dlq, env.directDLQ, directPoison.ID, 100)
	if err != nil {
		t.Fatalf("repeat direct replay: %v", err)
	}
	repeatGroup, err := ReplayDeadLetters(ctx, env.dlq, env.groupDLQ, groupPoison.ID, 100)
	if err != nil {
		t.Fatalf("repeat group replay: %v", err)
	}
	if repeatDirect.Replayed != 0 || repeatGroup.Replayed != 0 {
		t.Fatalf("repeat replay direct=%d group=%d, want both 0", repeatDirect.Replayed, repeatGroup.Replayed)
	}

	decodedDirect, err := decodeDirectMessage(directPoisonPayload)
	if err != nil {
		t.Fatalf("decode direct poison: %v", err)
	}
	created, err := env.store.PersistMessage(ctx, decodedDirect)
	if err != nil || created {
		t.Fatalf("duplicate direct persist created=%t err=%v, want false nil", created, err)
	}
	decodedGroup, err := decodeGroupMessage(groupPoisonPayload)
	if err != nil {
		t.Fatalf("decode group poison: %v", err)
	}
	created, err = env.store.PersistGroupMessage(ctx, decodedGroup)
	if err != nil || created {
		t.Fatalf("duplicate group persist created=%t err=%v, want false nil", created, err)
	}

	malformedID := "930001"
	if err := env.publisher.Publish(ctx, directExchangeName(env.namespace), directRoutingKey, malformedID, []byte(`{"server_message_id":`)); err != nil {
		t.Fatalf("publish malformed direct envelope: %v", err)
	}
	malformed := liveWaitDeadLetter(t, ctx, env, env.directDLQ, 30*time.Second, func(entry DeadLetterEntry) bool {
		return entry.ID == 930001
	})
	if malformed.Reason != dlqReasonMalformed || malformed.Attempts != 1 {
		t.Fatalf("malformed reason=%q attempts=%d, want %q and 1",
			malformed.Reason, malformed.Attempts, dlqReasonMalformed)
	}
	discarded, err := DiscardDeadLetters(ctx, env.dlq, env.directDLQ, 930001, 100)
	if err != nil {
		t.Fatalf("discard malformed direct envelope: %v", err)
	}
	if discarded != 1 {
		t.Fatalf("discarded malformed envelopes = %d, want 1", discarded)
	}
	liveEventually(t, ctx, 15*time.Second, "malformed entry leaves the dlq", func(checkCtx context.Context) (bool, error) {
		entries, err := ListDeadLetters(checkCtx, env.dlq, env.directDLQ, 100)
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			if entry.ID == 930001 {
				return false, nil
			}
		}
		return true, nil
	})

	t.Logf("step=poison-replay direct_attempts=%d group_attempts=%d elapsed_s=%d malformed_discarded=%d",
		directEntry.Attempts, groupEntry.Attempts, int(elapsed/time.Second), discarded)
}

func liveCheckDeadLetterBinding(t *testing.T, ctx context.Context, env *liveTestEnv, workers *liveWorkers) {
	t.Helper()

	baseline := env.directConsumerTracker.Count()
	if err := env.adminCh.QueueUnbind(
		env.directDLQ.Queue,
		env.directDLQ.RoutingKey,
		env.directDLQ.Exchange,
		nil,
	); err != nil {
		t.Fatalf("remove drill direct dead-letter binding: %v", err)
	}

	// The body is deliberately invalid so the consumer immediately attempts a
	// dead letter. With the binding gone the mandatory publish returns, the
	// consumer reconnects, redeclares topology and the broker redelivers the
	// still-unacked original.
	if err := env.publisher.Publish(ctx, directExchangeName(env.namespace), directRoutingKey, "940001", []byte(`{"server_message_id":`)); err != nil {
		t.Fatalf("publish malformed envelope after unbind: %v", err)
	}

	liveEventually(t, ctx, 30*time.Second, "direct consumer reconnects after mandatory return", func(context.Context) (bool, error) {
		if env.directConsumerTracker.Count() <= baseline {
			return false, nil
		}
		// The consumer only reconnects for this fault when the mandatory
		// dead-letter publish is returned by the broker.
		return strings.Contains(workers.directConsumer.Status().LastError, "unroutable"), nil
	})
	original := liveWaitDeadLetter(t, ctx, env, env.directDLQ, 30*time.Second, func(entry DeadLetterEntry) bool {
		return entry.ID == 940001
	})
	if original.Reason != dlqReasonMalformed {
		t.Fatalf("redeclared dlq reason = %q, want %q", original.Reason, dlqReasonMalformed)
	}

	valid := store.Message{
		ID:              940002,
		Seq:             1,
		SenderID:        15,
		RecipientID:     16,
		ClientMessageID: "live-dlq-followup-direct",
		Body:            "live dlq follow-up direct",
		CreatedAt:       time.Now().UTC(),
	}
	payload, err := encodeDirectMessage(valid)
	if err != nil {
		t.Fatalf("encode dlq follow-up: %v", err)
	}
	livePublishDirect(t, ctx, env, valid.ID, payload)
	liveEventually(t, ctx, 30*time.Second, "follow-up after dlq redeclare persists", func(checkCtx context.Context) (bool, error) {
		return liveDirectRowExists(checkCtx, env.db, valid.ID)
	})

	// Keep the direct DLQ empty for the later DLQ=0 assertion; the original was
	// already proven present before this discard.
	discarded, err := DiscardDeadLetters(ctx, env.dlq, env.directDLQ, 940001, 100)
	if err != nil {
		t.Fatalf("discard redeclared dlq proof: %v", err)
	}
	if discarded != 1 {
		t.Fatalf("discarded redeclared dlq entries = %d, want 1", discarded)
	}

	t.Logf("step=dlq-redeclare consumer_dials_before=%d consumer_dials_after=%d original_not_lost=%d",
		baseline, env.directConsumerTracker.Count(), discarded)
}

func liveCheckScopedLock(t *testing.T, ctx context.Context, env *liveTestEnv, workers *liveWorkers) {
	t.Helper()

	// Earlier fault steps deliberately dead-lettered poison envelopes, so keep
	// their counters as the baseline and prove this step adds no new ones.
	directDeadLetteredBefore := workers.directConsumer.Status().DeadLettered
	groupDeadLetteredBefore := workers.groupConsumer.Status().DeadLettered
	directTransientBefore := workers.directConsumer.Status().TransientRetries
	groupTransientBefore := workers.groupConsumer.Status().TransientRetries

	lock, proven, err := liveAcquireGapLock(ctx, env.db)
	if err != nil {
		t.Fatalf("acquire scoped InnoDB gap lock: %v", err)
	}

	holdDone := make(chan struct{})
	if proven {
		t.Cleanup(func() { lock.Release() })
		go func() {
			defer close(holdDone)
			timer := time.NewTimer(liveLockHold)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
			}
			lock.Release()
		}()
	} else {
		lock.Release()
		env.persister.simulateFor(liveSimulatedHold)
		t.Logf("step=scoped-lock fallback simulated=true reason=scoped-gap-lock-not-proven hold_s=%d",
			int(liveSimulatedHold/time.Second))
	}

	directMessage := liveAcceptDirect(
		t, ctx, env, liveLockDirectLow, liveLockDirectHigh, "live-lock-direct", time.Now().UTC(),
	)
	groupMessage := liveAcceptGroup(
		t, ctx, env, liveLockDirectLow, liveLockGroupID, "live-lock-group", time.Now().UTC(),
	)

	liveEventually(t, ctx, 30*time.Second, "direct consumer records transient retries", func(context.Context) (bool, error) {
		return workers.directConsumer.Status().TransientRetries > directTransientBefore, nil
	})
	liveEventually(t, ctx, 30*time.Second, "group consumer records transient retries", func(context.Context) (bool, error) {
		return workers.groupConsumer.Status().TransientRetries > groupTransientBefore, nil
	})

	if proven {
		liveEventually(t, ctx, 20*time.Second, "direct consumer sees lock wait timeout", func(context.Context) (bool, error) {
			return strings.Contains(workers.directConsumer.Status().LastError, "1205"), nil
		})
		liveEventually(t, ctx, 20*time.Second, "group consumer sees lock wait timeout", func(context.Context) (bool, error) {
			return strings.Contains(workers.groupConsumer.Status().LastError, "1205"), nil
		})
		select {
		case <-holdDone:
		case <-time.After(liveLockHold + 20*time.Second):
			t.Fatal("scoped gap lock was not released in time")
		}
	} else {
		liveEventually(t, ctx, liveSimulatedHold+15*time.Second, "simulated fault window ends", func(context.Context) (bool, error) {
			return !env.persister.simulating(), nil
		})
	}

	liveEventually(t, ctx, 75*time.Second, "direct message persists after lock release", func(checkCtx context.Context) (bool, error) {
		return liveDirectRowExists(checkCtx, env.db, directMessage.ID)
	})
	liveEventually(t, ctx, 75*time.Second, "group message persists after lock release", func(checkCtx context.Context) (bool, error) {
		return liveGroupRowExists(checkCtx, env.db, groupMessage.ID)
	})

	directStatus := workers.directConsumer.Status()
	groupStatus := workers.groupConsumer.Status()
	if directStatus.DeadLettered != directDeadLetteredBefore || groupStatus.DeadLettered != groupDeadLetteredBefore {
		t.Fatalf("dead-lettered direct=%d->%d group=%d->%d, want no increase",
			directDeadLetteredBefore, directStatus.DeadLettered,
			groupDeadLetteredBefore, groupStatus.DeadLettered)
	}
	if directStatus.TransientRetries <= directTransientBefore || groupStatus.TransientRetries <= groupTransientBefore {
		t.Fatalf("transient retries direct=%d->%d group=%d->%d, want both to increase",
			directTransientBefore, directStatus.TransientRetries,
			groupTransientBefore, groupStatus.TransientRetries)
	}
	directDLQCount, err := DeadLetterCount(ctx, env.dlq, env.directDLQ)
	if err != nil {
		t.Fatalf("count direct dlq: %v", err)
	}
	groupDLQCount, err := DeadLetterCount(ctx, env.dlq, env.groupDLQ)
	if err != nil {
		t.Fatalf("count group dlq: %v", err)
	}
	if directDLQCount != 0 || groupDLQCount != 0 {
		t.Fatalf("dlq counts direct=%d group=%d, want both 0", directDLQCount, groupDLQCount)
	}

	t.Logf("step=scoped-lock simulated=%t direct_transient=%d group_transient=%d direct_dlq=%d group_dlq=%d",
		!proven, directStatus.TransientRetries, groupStatus.TransientRetries, directDLQCount, groupDLQCount)
}

func liveAcquireGapLock(ctx context.Context, db *sql.DB) (*liveGapLock, bool, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	lock := &liveGapLock{conn: conn, tx: tx}

	var id uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT id FROM messages
		 WHERE conversation_low_id = ? AND conversation_high_id = ? AND seq = ?
		 FOR UPDATE`,
		liveLockDirectLow, liveLockDirectHigh, liveLockSeq,
	).Scan(&id); err != nil && !errors.Is(err, sql.ErrNoRows) {
		lock.Release()
		return nil, false, fmt.Errorf("lock direct conversation gap: %w", err)
	}
	if err := tx.QueryRowContext(ctx,
		`SELECT id FROM group_messages WHERE group_id = ? AND seq = ? FOR UPDATE`,
		liveLockGroupID, liveLockSeq,
	).Scan(&id); err != nil && !errors.Is(err, sql.ErrNoRows) {
		lock.Release()
		return nil, false, fmt.Errorf("lock group conversation gap: %w", err)
	}

	directBlocked, err := liveProbeBlockedInsert(ctx, db,
		`INSERT INTO messages
		   (server_message_id, conversation_low_id, conversation_high_id, seq,
		    sender_id, recipient_id, client_message_id, body, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		890001, liveLockDirectLow, liveLockDirectHigh, liveLockSeq,
		liveLockDirectLow, liveLockDirectHigh, "live-probe-direct", "live probe", time.Now().UTC(),
	)
	if err != nil {
		lock.Release()
		return nil, false, err
	}
	groupBlocked, err := liveProbeBlockedInsert(ctx, db,
		`INSERT INTO group_messages
		   (server_message_id, seq, group_id, sender_id, client_message_id, body, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		890002, liveLockSeq, liveLockGroupID, liveLockDirectLow,
		"live-probe-group", "live probe", time.Now().UTC(),
	)
	if err != nil {
		lock.Release()
		return nil, false, err
	}
	if !directBlocked || !groupBlocked {
		lock.Release()
		return nil, false, nil
	}
	// Prove the lock is scoped to the two target gaps: an insert for an
	// unrelated conversation in the same dedicated schema must still succeed.
	unrelated, err := liveProbeAllowedInsert(ctx, db,
		`INSERT INTO messages
		   (server_message_id, conversation_low_id, conversation_high_id, seq,
		    sender_id, recipient_id, client_message_id, body, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		890003, 5, 6, 999, 5, 6, "live-probe-unrelated", "live probe", time.Now().UTC(),
	)
	if err != nil {
		lock.Release()
		return nil, false, fmt.Errorf("probe unrelated insert: %w", err)
	}
	if !unrelated {
		lock.Release()
		return nil, false, errors.New("scoped gap lock blocked an unrelated conversation insert")
	}
	return lock, true, nil
}

func liveProbeAllowedInsert(ctx context.Context, db *sql.DB, query string, args ...any) (bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	_, execErr := tx.ExecContext(ctx, query, args...)
	_ = tx.Rollback()
	if execErr != nil {
		return false, execErr
	}
	return true, nil
}

func liveProbeBlockedInsert(ctx context.Context, db *sql.DB, query string, args ...any) (bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	_, execErr := tx.ExecContext(ctx, query, args...)
	_ = tx.Rollback()
	if execErr == nil {
		return false, nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(execErr, &mysqlErr) {
		switch mysqlErr.Number {
		case 1205, 1213:
			return true, nil
		}
		return false, nil
	}
	if errors.Is(execErr, driver.ErrBadConn) {
		return false, execErr
	}
	return false, nil
}

func livePublishDirect(t *testing.T, ctx context.Context, env *liveTestEnv, messageID uint64, payload []byte) {
	t.Helper()
	if err := env.publisher.Publish(
		ctx, directExchangeName(env.namespace), directRoutingKey,
		strconv.FormatUint(messageID, 10), payload,
	); err != nil {
		t.Fatalf("publish direct message %d: %v", messageID, err)
	}
}

func livePublishGroup(t *testing.T, ctx context.Context, env *liveTestEnv, messageID uint64, payload []byte) {
	t.Helper()
	if err := env.publisher.Publish(
		ctx, groupExchangeName(env.namespace), groupRoutingKey(),
		strconv.FormatUint(messageID, 10), payload,
	); err != nil {
		t.Fatalf("publish group message %d: %v", messageID, err)
	}
}

func liveWaitDeadLetter(
	t *testing.T,
	ctx context.Context,
	env *liveTestEnv,
	topology DeadLetterTopology,
	timeout time.Duration,
	match func(DeadLetterEntry) bool,
) DeadLetterEntry {
	t.Helper()
	var found DeadLetterEntry
	liveEventually(t, ctx, timeout, "dead letter appears", func(checkCtx context.Context) (bool, error) {
		entries, err := ListDeadLetters(checkCtx, env.dlq, topology, 100)
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			if match(entry) {
				found = entry
				return true, nil
			}
		}
		return false, nil
	})
	return found
}

func liveEventually(
	t *testing.T,
	parent context.Context,
	timeout time.Duration,
	description string,
	check func(context.Context) (bool, error),
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	var lastErr error
	for {
		ok, err := check(ctx)
		if err == nil && ok {
			return
		}
		if err != nil {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				t.Fatalf("%s: timed out: %v", description, lastErr)
			}
			t.Fatalf("%s: timed out", description)
		case <-time.After(livePollInterval):
		}
	}
}

func liveDirectRowExists(ctx context.Context, db *sql.DB, messageID uint64) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE server_message_id = ?`, messageID,
	).Scan(&count); err != nil {
		return false, err
	}
	return count == 1, nil
}

func liveGroupRowExists(ctx context.Context, db *sql.DB, messageID uint64) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM group_messages WHERE server_message_id = ?`, messageID,
	).Scan(&count); err != nil {
		return false, err
	}
	return count == 1, nil
}

func liveConversationRowCount(ctx context.Context, db *sql.DB, lowID, highID uint64) (int, error) {
	var count int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE conversation_low_id = ? AND conversation_high_id = ?`,
		lowID, highID,
	).Scan(&count)
	return count, err
}

func liveGroupRowCount(ctx context.Context, db *sql.DB, groupID uint64) (int, error) {
	var count int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM group_messages WHERE group_id = ?`, groupID,
	).Scan(&count)
	return count, err
}

func liveReadDirectRow(
	ctx context.Context,
	db *sql.DB,
	messageID uint64,
) (seq uint64, clientMessageID, body string, found bool, err error) {
	err = db.QueryRowContext(ctx,
		`SELECT seq, client_message_id, body FROM messages WHERE server_message_id = ?`,
		messageID,
	).Scan(&seq, &clientMessageID, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", "", false, nil
	}
	if err != nil {
		return 0, "", "", false, err
	}
	return seq, clientMessageID, body, true, nil
}

func liveReadGroupRow(
	ctx context.Context,
	db *sql.DB,
	messageID uint64,
) (seq uint64, clientMessageID, body string, found bool, err error) {
	err = db.QueryRowContext(ctx,
		`SELECT seq, client_message_id, body FROM group_messages WHERE server_message_id = ?`,
		messageID,
	).Scan(&seq, &clientMessageID, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", "", false, nil
	}
	if err != nil {
		return 0, "", "", false, err
	}
	return seq, clientMessageID, body, true, nil
}

func liveBlankRowCount(ctx context.Context, db *sql.DB, table string) (int, error) {
	// table is one of the two constants used below, never user input.
	query := `SELECT COUNT(*) FROM messages WHERE TRIM(body) = ''`
	if table == "group_messages" {
		query = `SELECT COUNT(*) FROM group_messages WHERE TRIM(body) = ''`
	}
	var count int
	err := db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

func liveStreamEmpty(ctx context.Context, client *redis.Client, stream string) (bool, error) {
	length, err := client.XLen(ctx, stream).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}
	return length == 0, nil
}

func livePendingEmpty(ctx context.Context, client *redis.Client, stream, group string) (bool, error) {
	pending, err := client.XPending(ctx, stream, group).Result()
	if errors.Is(err, redis.Nil) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return pending.Count == 0, nil
}

func liveExec(t *testing.T, ctx context.Context, db *sql.DB, description, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("%s: %v", description, err)
	}
}
