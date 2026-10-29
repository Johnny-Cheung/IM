package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/infra"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

const releaseSeqBackfillLockScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// refuseIncompleteRebuild stops a seq backfill on a namespace whose rebuild did
// not finish. seq:ready written there would certify exactly the incomplete
// projection the rebuild still has to regenerate.
func refuseIncompleteRebuild(
	ctx context.Context,
	client redis.UniversalClient,
	keyspace messaging.Keyspace,
) error {
	pendingKey := messaging.RebuildPendingKey(keyspace)
	pending, err := client.Exists(ctx, pendingKey).Result()
	if err != nil {
		return fmt.Errorf("check rebuild pending marker: %w", err)
	}
	if pending > 0 {
		return fmt.Errorf(
			"a Redis rebuild did not finish (marker %s); run `imctl rebuild-redis` "+
				"before -backfill-seq",
			pendingKey,
		)
	}
	return nil
}

// runSeqBackfill numbers direct and group history, rebuilds the seq read model
// and writes seq:ready. It is invoked with -backfill-seq while the server is
// stopped and can run against an empty database.
func runSeqBackfill(options serverOptions) error {
	// Nothing may be numbered or marked ready while a backend is serving it.
	if err := ensureBackendStopped(options.maintenanceAddr(), "-backfill-seq"); err != nil {
		return err
	}

	dsn, err := config.MySQLDSN()
	if err != nil {
		return fmt.Errorf("load MySQL configuration: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
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
		return fmt.Errorf("Redis is required for the seq backfill")
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
	// A failed rebuild leaves its pending marker behind. A backfill would
	// certify that half-built namespace, so refuse until the rebuild is rerun.
	if err := refuseIncompleteRebuild(ctx, redisClient, keyspace); err != nil {
		return err
	}
	lockKey := messaging.BackfillLockKey(keyspace)
	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	// The lock is taken only when the rebuild lock is absent; the reverse is
	// enforced by the rebuild's copy of the same atomic script.
	if err := acquireMaintenanceLock(ctx, redisClient, maintenanceLock{
		OwnKey:    lockKey,
		OwnName:   "seq backfill",
		OtherKey:  messaging.RebuildLockKey(keyspace),
		OtherName: "Redis rebuild",
		Token:     token,
		TTL:       messaging.SeqBackfillLockTTL,
	}); err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := redisClient.Eval(
			releaseCtx, releaseSeqBackfillLockScript, []string{lockKey}, token,
		).Result(); err != nil {
			log.Printf("release seq backfill lock: %v", err)
		}
	}()

	backfill := messaging.NewSeqBackfill(
		infraManager.Redis(),
		keyspace,
		dataStore,
		options.backfillBatch,
	)
	result, err := backfill.Run(ctx)
	if err != nil {
		return fmt.Errorf("backfill conversation seq: %w", err)
	}

	log.Printf(
		"seq backfill complete namespace=%s prechecked_pairs=%d prechecked_groups=%d "+
			"pairs_filled=%d messages_numbered=%d group_messages_numbered=%d "+
			"pair_entries=%d group_inbox_entries=%d entities_patched=%d counters_set=%d "+
			"read_positions_converted=%d read_positions_verified=%d "+
			"conversations_verified=%d groups_verified=%d memory_before=%s memory_after=%s",
		namespace.Base(),
		result.PrecheckedPairs,
		result.PrecheckedGroups,
		result.PairsFilled,
		result.MessagesNumbered,
		result.GroupMessagesNumbered,
		result.PairEntriesIndexed,
		result.GroupInboxEntriesIndexed,
		result.EntitiesPatched,
		result.CountersSet,
		result.ReadPositionsConverted,
		result.ReadPositionsVerified,
		result.ConversationsVerified,
		result.GroupsVerified,
		result.MemoryBefore,
		result.MemoryAfter,
	)
	return nil
}
