package messaging

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

//go:embed lua/conv_last_advance.lua
var convLastAdvanceScriptSource string

var (
	// projectionRaiseHashFieldScriptSource raises a hash field without lowering
	// it; it returns 1 when the field moved.
	projectionRaiseHashFieldScriptSource = `
local current = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
local target = tonumber(ARGV[2])
if target > current then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  return 1
end
return 0
`
	// projectionRaiseZSetScoreScriptSource mirrors it for ZSET scores.
	projectionRaiseZSetScoreScriptSource = `
local current = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[1]) or '0')
local target = tonumber(ARGV[2])
if target > current then
  redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
  return 1
end
return 0
`
)

// ProjectionWriteOptions configures a ProjectionWriter. Zero values select the
// defaults.
type ProjectionWriteOptions struct {
	DedupTTL time.Duration
	Now      func() time.Time
}

// ProjectionWriter writes the Redis read model from durable rows. It is shared
// by the full rebuild and the audit repair path, and is idempotent: entities
// are overwritten, indexes are scored by seq, previews and counters only move
// forward.
type ProjectionWriter struct {
	client   redis.UniversalClient
	keyspace Keyspace
	dedupTTL time.Duration
	now      func() time.Time
}

func NewProjectionWriter(
	client redis.UniversalClient,
	keyspace Keyspace,
	options ProjectionWriteOptions,
) *ProjectionWriter {
	if options.DedupTTL <= 0 {
		options.DedupTTL = defaultDedupTTL
	}
	if options.Now == nil {
		options.Now = func() time.Time { return time.Now().UTC() }
	}
	return &ProjectionWriter{
		client:   client,
		keyspace: keyspace,
		dedupTTL: options.DedupTTL,
		now:      options.Now,
	}
}

// messageDedupTTL returns how long the idempotency record should live, or false
// when the message is already older than the dedup window or has no client ID.
func messageDedupTTL(
	clientMessageID string,
	createdAt, now time.Time,
	ttl time.Duration,
) (time.Duration, bool) {
	if clientMessageID == "" {
		return 0, false
	}
	remaining := ttl - now.Sub(createdAt)
	if remaining <= 0 {
		return 0, false
	}
	if remaining > ttl {
		remaining = ttl
	}
	if remaining < time.Second {
		remaining = time.Second
	}
	return remaining, true
}

// WriteDirectBatch writes direct message entities, the conversation index, both
// feeds, both previews, the conversation counter and the dedup records. It
// returns the number of messages written.
func (w *ProjectionWriter) WriteDirectBatch(
	ctx context.Context,
	messages []store.Message,
) (int, error) {
	if w == nil || w.client == nil {
		return 0, ErrMessageServiceUnavailable
	}
	if len(messages) == 0 {
		return 0, nil
	}
	now := w.now().UTC()
	pipe := w.client.Pipeline()
	maxSeqs := make(map[[2]uint64]uint64, len(messages))
	written := 0

	for _, message := range messages {
		if message.ID == 0 || message.Seq == 0 {
			return 0, fmt.Errorf("rebuild direct message %d has no seq", message.ID)
		}
		if message.SenderID == 0 || message.RecipientID == 0 ||
			message.SenderID == message.RecipientID {
			return 0, fmt.Errorf("rebuild direct message %d has invalid participants", message.ID)
		}
		idValue := strconv.FormatUint(message.ID, 10)
		senderValue := strconv.FormatUint(message.SenderID, 10)
		recipientValue := strconv.FormatUint(message.RecipientID, 10)
		createdAt := message.CreatedAt.UTC().Truncate(time.Millisecond)
		createdAtMS := strconv.FormatInt(createdAt.UnixMilli(), 10)
		bodyHash := hashMessageBody(message.Body)

		pipe.HSet(ctx, w.keyspace.messageKey(message.ID),
			"id", idValue,
			"sender_id", senderValue,
			"recipient_id", recipientValue,
			"client_message_id", message.ClientMessageID,
			"body", message.Body,
			"created_at_ms", createdAtMS,
			"body_hash", bodyHash,
			"seq", strconv.FormatUint(message.Seq, 10),
		)
		if ttl, ok := messageDedupTTL(
			message.ClientMessageID, message.CreatedAt, now, w.dedupTTL,
		); ok {
			dedupKey := w.keyspace.dedupKey(
				message.SenderID, message.RecipientID, message.ClientMessageID,
			)
			pipe.HSet(ctx, dedupKey,
				"id", idValue,
				"created_at_ms", createdAtMS,
				"body_hash", bodyHash,
			)
			pipe.Expire(ctx, dedupKey, ttl)
		}

		pipe.ZAdd(ctx, w.keyspace.directPairKey(message.SenderID, message.RecipientID),
			redis.Z{Score: float64(message.Seq), Member: idValue})
		pipe.ZAdd(ctx, w.keyspace.directFeedKey(message.SenderID),
			redis.Z{Score: float64(message.ID), Member: idValue})
		pipe.ZAdd(ctx, w.keyspace.directFeedKey(message.RecipientID),
			redis.Z{Score: float64(message.ID), Member: idValue})
		pipe.Eval(ctx, convLastAdvanceScriptSource,
			[]string{w.keyspace.directConversationLastKey(message.SenderID)},
			recipientValue, idValue,
		)
		pipe.Eval(ctx, convLastAdvanceScriptSource,
			[]string{w.keyspace.directConversationLastKey(message.RecipientID)},
			senderValue, idValue,
		)

		pair := [2]uint64{}
		pair[0], pair[1] = sortedPair(message.SenderID, message.RecipientID)
		if message.Seq > maxSeqs[pair] {
			maxSeqs[pair] = message.Seq
		}
		written++
	}

	for pair, seq := range maxSeqs {
		pipe.Eval(ctx, raiseSequenceScriptSource,
			[]string{w.keyspace.directSequenceKey(pair[0], pair[1])},
			strconv.FormatUint(seq, 10),
		)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("write direct projection batch: %w", err)
	}
	return written, nil
}

// WriteGroupBatch writes group message entities, the group inbox, the group
// counter and the dedup records. A missing sender snapshot is written as empty
// strings rather than rejected.
func (w *ProjectionWriter) WriteGroupBatch(
	ctx context.Context,
	messages []store.GroupMessage,
) (int, error) {
	if w == nil || w.client == nil {
		return 0, ErrMessageServiceUnavailable
	}
	if len(messages) == 0 {
		return 0, nil
	}
	now := w.now().UTC()
	pipe := w.client.Pipeline()
	maxSeqs := make(map[uint64]uint64, len(messages))
	written := 0

	for _, message := range messages {
		if message.ID == 0 || message.Seq == 0 {
			return 0, fmt.Errorf("rebuild group message %d has no seq", message.ID)
		}
		if message.GroupID == 0 || message.SenderID == 0 {
			return 0, fmt.Errorf("rebuild group message %d has invalid participants", message.ID)
		}
		idValue := strconv.FormatUint(message.ID, 10)
		createdAt := message.CreatedAt.UTC().Truncate(time.Millisecond)
		createdAtMS := strconv.FormatInt(createdAt.UnixMilli(), 10)
		bodyHash := hashMessageBody(message.Body)

		pipe.HSet(ctx, w.keyspace.groupMessageKey(message.ID),
			"id", idValue,
			"group_id", strconv.FormatUint(message.GroupID, 10),
			"sender_id", strconv.FormatUint(message.SenderID, 10),
			"client_message_id", message.ClientMessageID,
			"body", message.Body,
			"created_at_ms", createdAtMS,
			"sender_username", message.Sender.Username,
			"sender_nickname", message.Sender.Nickname,
			"body_hash", bodyHash,
			"seq", strconv.FormatUint(message.Seq, 10),
		)
		if ttl, ok := messageDedupTTL(
			message.ClientMessageID, message.CreatedAt, now, w.dedupTTL,
		); ok {
			dedupKey := w.keyspace.groupDedupKey(
				message.GroupID, message.SenderID, message.ClientMessageID,
			)
			pipe.HSet(ctx, dedupKey,
				"id", idValue,
				"created_at_ms", createdAtMS,
				"body_hash", bodyHash,
			)
			pipe.Expire(ctx, dedupKey, ttl)
		}

		pipe.ZAdd(ctx, w.keyspace.groupInboxKey(message.GroupID),
			redis.Z{Score: float64(message.Seq), Member: idValue})
		if message.Seq > maxSeqs[message.GroupID] {
			maxSeqs[message.GroupID] = message.Seq
		}
		written++
	}

	for groupID, seq := range maxSeqs {
		pipe.Eval(ctx, raiseSequenceScriptSource,
			[]string{w.keyspace.groupSequenceKey(groupID)},
			strconv.FormatUint(seq, 10),
		)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("write group projection batch: %w", err)
	}
	return written, nil
}

// RaiseDirectReadWatermarks raises the private read watermarks of one batch and
// returns how many positions actually moved.
func (w *ProjectionWriter) RaiseDirectReadWatermarks(
	ctx context.Context,
	states []store.ConversationReadState,
) (int, error) {
	if w == nil || w.client == nil {
		return 0, ErrMessageServiceUnavailable
	}
	pipe := w.client.Pipeline()
	commands := make([]*redis.Cmd, 0, len(states))
	for _, state := range states {
		if state.UserID == 0 || state.PeerID == 0 ||
			state.UserID == state.PeerID || state.LastReadSeq == 0 {
			continue
		}
		commands = append(commands, pipe.Eval(ctx, projectionRaiseHashFieldScriptSource,
			[]string{w.keyspace.directReadKey(state.UserID)},
			strconv.FormatUint(state.PeerID, 10),
			strconv.FormatUint(state.LastReadSeq, 10),
		))
	}
	if len(commands) == 0 {
		return 0, nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("raise direct read watermarks: %w", err)
	}
	return countRaised(commands), nil
}

// RaiseGroupReadWatermarks raises group member watermarks of one batch and
// returns how many positions actually moved.
func (w *ProjectionWriter) RaiseGroupReadWatermarks(
	ctx context.Context,
	states []store.GroupMemberReadState,
) (int, error) {
	if w == nil || w.client == nil {
		return 0, ErrMessageServiceUnavailable
	}
	pipe := w.client.Pipeline()
	commands := make([]*redis.Cmd, 0, len(states))
	for _, state := range states {
		if state.GroupID == 0 || state.UserID == 0 || state.LastReadSeq == 0 {
			continue
		}
		commands = append(commands, pipe.Eval(ctx, projectionRaiseZSetScoreScriptSource,
			[]string{w.keyspace.groupReadKey(state.GroupID)},
			strconv.FormatUint(state.UserID, 10),
			strconv.FormatUint(state.LastReadSeq, 10),
		))
	}
	if len(commands) == 0 {
		return 0, nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("raise group read watermarks: %w", err)
	}
	return countRaised(commands), nil
}

// RaiseGroupReadPositions raises every member watermark of one group and
// returns how many positions actually moved.
func (w *ProjectionWriter) RaiseGroupReadPositions(
	ctx context.Context,
	groupID uint64,
	positions map[uint64]uint64,
) (int, error) {
	if w == nil || w.client == nil {
		return 0, ErrMessageServiceUnavailable
	}
	if groupID == 0 || len(positions) == 0 {
		return 0, nil
	}
	pipe := w.client.Pipeline()
	commands := make([]*redis.Cmd, 0, len(positions))
	for userID, position := range positions {
		if userID == 0 || position == 0 {
			continue
		}
		commands = append(commands, pipe.Eval(ctx, projectionRaiseZSetScoreScriptSource,
			[]string{w.keyspace.groupReadKey(groupID)},
			strconv.FormatUint(userID, 10),
			strconv.FormatUint(position, 10),
		))
	}
	if len(commands) == 0 {
		return 0, nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("raise group read positions: %w", err)
	}
	return countRaised(commands), nil
}

// RaiseDirectCounter raises one private conversation counter; it never lowers
// it.
func (w *ProjectionWriter) RaiseDirectCounter(
	ctx context.Context,
	lowID, highID, seq uint64,
) error {
	if w == nil || w.client == nil {
		return ErrMessageServiceUnavailable
	}
	if seq == 0 {
		return nil
	}
	return w.client.Eval(
		ctx,
		raiseSequenceScriptSource,
		[]string{w.keyspace.directSequenceKey(lowID, highID)},
		strconv.FormatUint(seq, 10),
	).Err()
}

// RaiseGroupCounter raises one group counter; it never lowers it.
func (w *ProjectionWriter) RaiseGroupCounter(
	ctx context.Context,
	groupID, seq uint64,
) error {
	if w == nil || w.client == nil {
		return ErrMessageServiceUnavailable
	}
	if groupID == 0 || seq == 0 {
		return nil
	}
	return w.client.Eval(
		ctx,
		raiseSequenceScriptSource,
		[]string{w.keyspace.groupSequenceKey(groupID)},
		strconv.FormatUint(seq, 10),
	).Err()
}

func countRaised(commands []*redis.Cmd) int {
	raised := 0
	for _, command := range commands {
		if value, err := command.Int64(); err == nil && value == 1 {
			raised++
		}
	}
	return raised
}
