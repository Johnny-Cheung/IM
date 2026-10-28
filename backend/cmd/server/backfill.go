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

// runBackfill rebuilds the Redis direct read model and exits. It is invoked
// with -backfill-read before starting a version that requires the read model.
func runBackfill(options serverOptions) error {
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

	infraConfig, err := config.LoadInfra()
	if err != nil {
		return fmt.Errorf("load infrastructure configuration: %w", err)
	}
	infraManager, err := infra.New(infraConfig)
	if err != nil {
		return fmt.Errorf("configure infrastructure: %w", err)
	}
	if infraManager == nil || !infraManager.RedisEnabled() {
		return fmt.Errorf("Redis is required for direct read backfill")
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
	backfill := messaging.NewDirectReadBackfill(
		infraManager.Redis(),
		keyspace,
		dataStore,
		options.backfillBatch,
	)
	result, err := backfill.Run(ctx)
	if err != nil {
		return fmt.Errorf("backfill direct read model: %w", err)
	}

	log.Printf(
		"direct read backfill complete namespace=%s messages=%d read_positions=%d "+
			"pairs_verified=%d read_positions_verified=%d memory_before=%s memory_after=%s",
		namespace.Base(),
		result.MessagesIndexed,
		result.ReadPositionsIndexed,
		result.PairsVerified,
		result.ReadPositionsVerified,
		result.MemoryBefore,
		result.MemoryAfter,
	)
	return nil
}

// runGroupBackfill rebuilds the Redis group read model and exits. It is
// invoked with -backfill-group-read while the server is stopped.
func runGroupBackfill(options serverOptions) error {
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

	infraConfig, err := config.LoadInfra()
	if err != nil {
		return fmt.Errorf("load infrastructure configuration: %w", err)
	}
	infraManager, err := infra.New(infraConfig)
	if err != nil {
		return fmt.Errorf("configure infrastructure: %w", err)
	}
	if infraManager == nil || !infraManager.RedisEnabled() {
		return fmt.Errorf("Redis is required for group read backfill")
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
	backfill := messaging.NewGroupReadBackfill(
		infraManager.Redis(),
		keyspace,
		dataStore,
		options.backfillBatch,
	)
	result, err := backfill.Run(ctx)
	if err != nil {
		return fmt.Errorf("backfill group read model: %w", err)
	}

	log.Printf(
		"group read backfill complete namespace=%s messages=%d groups_rebuilt=%d "+
			"read_positions=%d written_back=%d groups_verified=%d "+
			"read_positions_verified=%d memory_before=%s memory_after=%s",
		namespace.Base(),
		result.MessagesIndexed,
		result.GroupsRebuilt,
		result.ReadPositionsIndexed,
		result.ReadPositionsWrittenBack,
		result.GroupsVerified,
		result.ReadPositionsVerified,
		result.MemoryBefore,
		result.MemoryAfter,
	)
	return nil
}
