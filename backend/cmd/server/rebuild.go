package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/infra"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

const (
	redisRebuildTimeout             = 30 * time.Minute
	defaultRedisRebuildBatch        = 1000
	defaultRedisRebuildQueueTimeout = 10 * time.Minute
	maxRedisRebuildBatch            = 10000
	releaseRedisRebuildLockScript   = releaseSeqBackfillLockScript
)

// rebuildSettings validates the rebuild flags so a bad batch size or timeout
// is rejected before any connection is opened.
func rebuildSettings(options serverOptions) (int, time.Duration, error) {
	batch := options.rebuildBatch
	if batch < 1 || batch > maxRedisRebuildBatch {
		return 0, 0, fmt.Errorf(
			"-rebuild-batch must be between 1 and %d", maxRedisRebuildBatch,
		)
	}
	queueTimeout := options.rebuildQueueTimeout
	if queueTimeout <= 0 {
		return 0, 0, fmt.Errorf("-rebuild-queue-timeout must be positive")
	}
	return batch, queueTimeout, nil
}

// runRedisRebuild drains the persist queues and regenerates the whole Redis
// messaging namespace from MySQL. It is invoked with -rebuild-redis while the
// backend is stopped and writes seq:ready only after verification.
func runRedisRebuild(options serverOptions) error {
	batch, queueTimeout, err := rebuildSettings(options)
	if err != nil {
		return err
	}

	dsn, err := config.MySQLDSN()
	if err != nil {
		return fmt.Errorf("load MySQL configuration: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisRebuildTimeout)
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
	acquired, err := redisClient.SetNX(
		ctx, lockKey, token, messaging.RebuildLockTTL,
	).Result()
	if err != nil {
		return fmt.Errorf("acquire Redis rebuild lock: %w", err)
	}
	if !acquired {
		return fmt.Errorf(
			"another Redis rebuild is running (lock %s); wait for it to finish", lockKey,
		)
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

	// A nil group persister leaves the group queue alone, matching a deployment
	// with group messaging disabled.
	var groupPersister messaging.GroupMessagePersister
	if infraConfig.GroupMessagingEnabled {
		groupPersister = dataStore
	}

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
