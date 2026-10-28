package messaging

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// SeqCalibrationStore is the durable source of per-conversation maximum seq.
type SeqCalibrationStore interface {
	ListConversationSeqMax(
		ctx context.Context,
		afterLowID, afterHighID uint64,
		limit int,
	) ([]store.ConversationSeqMax, error)
	ListGroupSeqMax(
		ctx context.Context,
		afterGroupID uint64,
		limit int,
	) ([]store.GroupSeqMax, error)
}

const defaultCalibrationBatchSize = 500

// raiseSequenceScriptSource raises a counter without ever lowering it, and
// creates it when missing.
const raiseSequenceScriptSource = `
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local target = tonumber(ARGV[1])
if target > current then
  redis.call('SET', KEYS[1], ARGV[1])
end
return 1
`

// SeqCalibrator raises Redis conversation counters to at least the maximum seq
// persisted in MySQL. It is idempotent and safe to rerun.
type SeqCalibrator struct {
	client    redis.UniversalClient
	keyspace  Keyspace
	store     SeqCalibrationStore
	batchSize int
}

func NewSeqCalibrator(
	client redis.UniversalClient,
	keyspace Keyspace,
	dataStore SeqCalibrationStore,
	batchSize int,
) *SeqCalibrator {
	if batchSize <= 0 {
		batchSize = defaultCalibrationBatchSize
	}
	return &SeqCalibrator{
		client:    client,
		keyspace:  keyspace,
		store:     dataStore,
		batchSize: batchSize,
	}
}

// Calibrate raises every counter and returns the number of conversations
// processed.
func (c *SeqCalibrator) Calibrate(ctx context.Context) (int, error) {
	if c == nil || c.client == nil || c.store == nil {
		return 0, ErrMessageServiceUnavailable
	}
	processed := 0

	afterLowID, afterHighID := uint64(0), uint64(0)
	for {
		rows, err := c.store.ListConversationSeqMax(
			ctx, afterLowID, afterHighID, c.batchSize,
		)
		if err != nil {
			return processed, fmt.Errorf("list conversation seq max: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		pipe := c.client.Pipeline()
		for _, row := range rows {
			afterLowID, afterHighID = row.LowID, row.HighID
			if row.LowID == 0 || row.HighID == 0 || row.LowID == row.HighID || row.MaxSeq == 0 {
				continue
			}
			pipe.Eval(
				ctx,
				raiseSequenceScriptSource,
				[]string{c.keyspace.directSequenceKey(row.LowID, row.HighID)},
				row.MaxSeq,
			)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return processed, fmt.Errorf("raise conversation seq: %w", err)
		}
		processed += len(rows)
	}

	afterGroupID := uint64(0)
	for {
		rows, err := c.store.ListGroupSeqMax(ctx, afterGroupID, c.batchSize)
		if err != nil {
			return processed, fmt.Errorf("list group seq max: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		pipe := c.client.Pipeline()
		for _, row := range rows {
			afterGroupID = row.GroupID
			if row.GroupID == 0 || row.MaxSeq == 0 {
				continue
			}
			pipe.Eval(
				ctx,
				raiseSequenceScriptSource,
				[]string{c.keyspace.groupSequenceKey(row.GroupID)},
				row.MaxSeq,
			)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return processed, fmt.Errorf("raise group seq: %w", err)
		}
		processed += len(rows)
	}
	return processed, nil
}
