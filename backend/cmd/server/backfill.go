package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/infra"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// runSeqBackfill numbers direct and group history, rebuilds the seq read model
// and writes seq:ready. It is invoked with -backfill-seq while the server is
// stopped and can run against an empty database.
func runSeqBackfill(options serverOptions) error {
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
