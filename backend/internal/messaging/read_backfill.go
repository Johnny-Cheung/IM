package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// DirectReadBackfillStore reads the durable data needed to rebuild the Redis
// direct read model.
type DirectReadBackfillStore interface {
	ListDirectMessageIndex(ctx context.Context, afterID uint64, limit int) ([]store.MessageIndex, error)
	ListConversationReadsAfter(
		ctx context.Context,
		afterUserID, afterPeerUserID uint64,
		limit int,
	) ([]store.ConversationRead, error)
	CountDirectMessagesByPair(ctx context.Context) ([]store.DirectPairCount, error)
}

// DirectReadBackfillResult reports what one backfill run indexed and verified.
type DirectReadBackfillResult struct {
	MessagesIndexed       int
	ReadPositionsIndexed  int
	PairsVerified         int
	ReadPositionsVerified int
	MemoryBefore          string
	MemoryAfter           string
}

// DirectReadBackfill rebuilds dm:conv and dm:read from MySQL. It is
// idempotent and can run while the service is accepting new messages.
type DirectReadBackfill struct {
	client    redis.UniversalClient
	keyspace  Keyspace
	store     DirectReadBackfillStore
	batchSize int
}

func NewDirectReadBackfill(
	client redis.UniversalClient,
	keyspace Keyspace,
	dataStore DirectReadBackfillStore,
	batchSize int,
) *DirectReadBackfill {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &DirectReadBackfill{
		client:    client,
		keyspace:  keyspace,
		store:     dataStore,
		batchSize: batchSize,
	}
}

// Run indexes historical messages and read positions, verifies the result,
// and finally marks the read model ready.
func (b *DirectReadBackfill) Run(ctx context.Context) (DirectReadBackfillResult, error) {
	if b == nil || b.client == nil || b.store == nil {
		return DirectReadBackfillResult{}, ErrMessageServiceUnavailable
	}
	result := DirectReadBackfillResult{
		MemoryBefore: redisUsedMemory(ctx, b.client),
	}

	afterID := uint64(0)
	for {
		rows, err := b.store.ListDirectMessageIndex(ctx, afterID, b.batchSize)
		if err != nil {
			return result, fmt.Errorf("list direct messages: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		pipe := b.client.Pipeline()
		for _, row := range rows {
			if row.RecipientID != 0 && row.SenderID != 0 && row.RecipientID != row.SenderID {
				pipe.ZAdd(
					ctx,
					b.keyspace.directConversationKey(row.RecipientID, row.SenderID),
					redis.Z{
						Score:  float64(row.ID),
						Member: strconv.FormatUint(row.ID, 10),
					},
				)
				result.MessagesIndexed++
			}
			afterID = row.ID
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return result, fmt.Errorf("index direct messages: %w", err)
		}
	}

	afterUserID, afterPeerID := uint64(0), uint64(0)
	for {
		rows, err := b.store.ListConversationReadsAfter(
			ctx, afterUserID, afterPeerID, b.batchSize,
		)
		if err != nil {
			return result, fmt.Errorf("list conversation reads: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		pipe := b.client.Pipeline()
		for _, row := range rows {
			if row.UserID != 0 && row.PeerUserID != 0 &&
				row.UserID != row.PeerUserID && row.LastReadMessageID != 0 {
				pipe.HSet(
					ctx,
					b.keyspace.directReadKey(row.UserID),
					strconv.FormatUint(row.PeerUserID, 10),
					strconv.FormatUint(row.LastReadMessageID, 10),
				)
				result.ReadPositionsIndexed++
			}
			afterUserID = row.UserID
			afterPeerID = row.PeerUserID
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return result, fmt.Errorf("index conversation reads: %w", err)
		}
	}

	pairs, err := b.store.CountDirectMessagesByPair(ctx)
	if err != nil {
		return result, fmt.Errorf("count persisted messages: %w", err)
	}
	if len(pairs) > 0 {
		pipe := b.client.Pipeline()
		commands := make([]*redis.IntCmd, len(pairs))
		for index, pair := range pairs {
			commands[index] = pipe.ZCard(
				ctx,
				b.keyspace.directConversationKey(pair.RecipientID, pair.SenderID),
			)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return result, fmt.Errorf("count indexed messages: %w", err)
		}
		for index, pair := range pairs {
			indexed, err := commands[index].Result()
			if err != nil {
				return result, fmt.Errorf("count indexed messages: %w", err)
			}
			if uint64(indexed) < pair.Count {
				return result, fmt.Errorf(
					"read model missing messages for pair %d->%d: redis=%d mysql=%d",
					pair.SenderID, pair.RecipientID, indexed, pair.Count,
				)
			}
			result.PairsVerified++
		}
	}

	if err := b.verifyReadPositions(ctx, &result); err != nil {
		return result, err
	}

	tracker := NewDirectReadTracker(
		b.client,
		DirectReadTrackerOptions{Keyspace: b.keyspace},
	)
	if err := tracker.MarkReadModelReady(ctx); err != nil {
		return result, err
	}
	result.MemoryAfter = redisUsedMemory(ctx, b.client)
	return result, nil
}

func (b *DirectReadBackfill) verifyReadPositions(
	ctx context.Context,
	result *DirectReadBackfillResult,
) error {
	afterUserID, afterPeerID := uint64(0), uint64(0)
	for {
		rows, err := b.store.ListConversationReadsAfter(
			ctx, afterUserID, afterPeerID, b.batchSize,
		)
		if err != nil {
			return fmt.Errorf("list read positions for verification: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}

		pipe := b.client.Pipeline()
		commands := make([]*redis.StringCmd, len(rows))
		for index, row := range rows {
			commands[index] = pipe.HGet(
				ctx,
				b.keyspace.directReadKey(row.UserID),
				strconv.FormatUint(row.PeerUserID, 10),
			)
			afterUserID = row.UserID
			afterPeerID = row.PeerUserID
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("read indexed positions: %w", err)
		}

		for index, row := range rows {
			if row.UserID == 0 || row.PeerUserID == 0 ||
				row.UserID == row.PeerUserID || row.LastReadMessageID == 0 {
				continue
			}
			raw, err := commands[index].Result()
			if errors.Is(err, redis.Nil) {
				return fmt.Errorf(
					"read model missing position for %d->%d: mysql=%d",
					row.UserID, row.PeerUserID, row.LastReadMessageID,
				)
			}
			if err != nil {
				return fmt.Errorf("read indexed position: %w", err)
			}
			indexed, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				return fmt.Errorf("parse indexed position: %w", err)
			}
			if indexed < row.LastReadMessageID {
				return fmt.Errorf(
					"read model position behind for %d->%d: redis=%d mysql=%d",
					row.UserID, row.PeerUserID, indexed, row.LastReadMessageID,
				)
			}
			result.ReadPositionsVerified++
		}
	}
}

func redisUsedMemory(ctx context.Context, client redis.UniversalClient) string {
	info, err := client.Info(ctx, "memory").Result()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "used_memory_human:") {
			return strings.TrimPrefix(line, "used_memory_human:")
		}
	}
	return ""
}
