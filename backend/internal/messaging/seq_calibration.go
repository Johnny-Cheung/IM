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

// calibrationRaise carries one counter to raise and the index command whose
// top score is the Redis floor.
type calibrationRaise struct {
	counterKey string
	mysqlMax   uint64
	topScore   *redis.ZSliceCmd
}

// SeqCalibrator raises Redis conversation counters to at least the maximum seq
// persisted in MySQL and the highest score surviving in the conversation
// index. The index floor matters when an accepted-but-unpersisted tail is
// still in Redis: MySQL alone would reopen a seq that is already live. It is
// idempotent, only ever raises, and safe to rerun.
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
		// Read every conversation's top index score in one pipeline per MySQL
		// page, then raise the counters in a second pipeline. No full index
		// scan: the last score is one ZRANGE per conversation.
		pipe := c.client.Pipeline()
		raises := make([]calibrationRaise, 0, len(rows))
		for _, row := range rows {
			afterLowID, afterHighID = row.LowID, row.HighID
			if row.LowID == 0 || row.HighID == 0 || row.LowID == row.HighID {
				continue
			}
			raises = append(raises, calibrationRaise{
				counterKey: c.keyspace.directSequenceKey(row.LowID, row.HighID),
				mysqlMax:   row.MaxSeq,
				topScore: pipe.ZRangeWithScores(
					ctx, c.keyspace.directPairKey(row.LowID, row.HighID), -1, -1,
				),
			})
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return processed, fmt.Errorf("read conversation index top: %w", err)
		}
		if err := c.raiseCalibratedCounters(ctx, raises); err != nil {
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
		raises := make([]calibrationRaise, 0, len(rows))
		for _, row := range rows {
			afterGroupID = row.GroupID
			if row.GroupID == 0 {
				continue
			}
			raises = append(raises, calibrationRaise{
				counterKey: c.keyspace.groupSequenceKey(row.GroupID),
				mysqlMax:   row.MaxSeq,
				topScore: pipe.ZRangeWithScores(
					ctx, c.keyspace.groupInboxKey(row.GroupID), -1, -1,
				),
			})
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return processed, fmt.Errorf("read group index top: %w", err)
		}
		if err := c.raiseCalibratedCounters(ctx, raises); err != nil {
			return processed, fmt.Errorf("raise group seq: %w", err)
		}
		processed += len(rows)
	}
	return processed, nil
}

// raiseCalibratedCounters raises each counter to max(MySQL max, top surviving
// index score), never lowering it. The top scores were read in the page's
// pipeline, so the raise is one more pipeline per page.
func (c *SeqCalibrator) raiseCalibratedCounters(
	ctx context.Context,
	raises []calibrationRaise,
) error {
	pipe := c.client.Pipeline()
	queued := 0
	for _, raise := range raises {
		target := raise.mysqlMax
		if scores := raise.topScore.Val(); len(scores) > 0 {
			if top := uint64(scores[0].Score); top > target {
				target = top
			}
		}
		if target == 0 {
			continue
		}
		pipe.Eval(ctx, raiseSequenceScriptSource, []string{raise.counterKey}, target)
		queued++
	}
	if queued == 0 {
		return nil
	}
	_, err := pipe.Exec(ctx)
	return err
}
