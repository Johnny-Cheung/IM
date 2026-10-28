package messaging

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/read_advance.lua
var readAdvanceScriptSource string

var readAdvanceScript = redis.NewScript(readAdvanceScriptSource)

const defaultReadStreamMaxLen = int64(10000)

// DirectReadTracker owns direct-chat read watermarks in Redis.
//
// Only the last read seq is stored. Unread counts are derived from the
// conversation counter minus that watermark; no message index is queried.
type DirectReadTracker struct {
	client       redis.UniversalClient
	keyspace     Keyspace
	streamMaxLen int64
	gate         *SeqGate
}

// DirectReadTrackerOptions configures the tracker.
type DirectReadTrackerOptions struct {
	Keyspace     Keyspace
	StreamMaxLen int64
	Gate         *SeqGate
}

// DirectReadAdvance reports the outcome of one read-position advance.
type DirectReadAdvance struct {
	LastReadSeq uint64
	Candidate   uint64
	Advanced    bool
	StreamError bool
}

// DirectConversationState is one private conversation's seq snapshot.
type DirectConversationState struct {
	LastSeq     uint64
	ReadSeq     uint64
	PeerReadSeq uint64
	Unread      uint64
}

func NewDirectReadTracker(
	client redis.UniversalClient,
	options DirectReadTrackerOptions,
) *DirectReadTracker {
	if options.StreamMaxLen <= 0 {
		options.StreamMaxLen = defaultReadStreamMaxLen
	}
	return &DirectReadTracker{
		client:       client,
		keyspace:     options.Keyspace,
		streamMaxLen: options.StreamMaxLen,
		gate:         options.Gate,
	}
}

// AdvanceRead moves the user's read position for peer forward to
// min(throughSeq, conversation counter), or to the counter when throughSeq is
// zero.
func (t *DirectReadTracker) AdvanceRead(
	ctx context.Context,
	userID, peerID, throughSeq uint64,
	now time.Time,
) (DirectReadAdvance, error) {
	if t == nil || t.client == nil {
		return DirectReadAdvance{}, ErrMessageServiceUnavailable
	}
	if err := t.gate.Ready(); err != nil {
		return DirectReadAdvance{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	keys := []string{
		t.keyspace.directSequenceKey(userID, peerID),
		t.keyspace.directReadKey(userID),
		t.keyspace.directReadStreamKey(),
	}
	args := []interface{}{
		peerID,
		throughSeq,
		now.UTC().UnixMilli(),
		t.streamMaxLen,
		userID,
	}

	values, err := readAdvanceScript.Run(ctx, t.client, keys, args...).Slice()
	if err != nil {
		return DirectReadAdvance{}, err
	}
	return parseReadAdvance(values)
}

// ReadPositions returns every stored peer read position for the user.
func (t *DirectReadTracker) ReadPositions(
	ctx context.Context,
	userID uint64,
) (map[uint64]uint64, error) {
	if t == nil || t.client == nil {
		return nil, ErrMessageServiceUnavailable
	}

	raw, err := t.client.HGetAll(ctx, t.keyspace.directReadKey(userID)).Result()
	if err != nil {
		return nil, err
	}
	positions := make(map[uint64]uint64, len(raw))
	for field, value := range raw {
		peerID, err := strconv.ParseUint(field, 10, 64)
		if err != nil || peerID == 0 {
			continue
		}
		position, err := strconv.ParseUint(value, 10, 64)
		if err != nil || position == 0 {
			continue
		}
		positions[peerID] = position
	}
	return positions, nil
}

// PeerReadPositions returns how far each peer has read messages sent by the
// user, in seq units. Missing or zero positions are omitted.
func (t *DirectReadTracker) PeerReadPositions(
	ctx context.Context,
	userID uint64,
	peerIDs []uint64,
) (map[uint64]uint64, error) {
	if t == nil || t.client == nil {
		return nil, ErrMessageServiceUnavailable
	}
	result := make(map[uint64]uint64, len(peerIDs))
	if len(peerIDs) == 0 {
		return result, nil
	}

	pipe := t.client.Pipeline()
	commands := make([]*redis.StringCmd, len(peerIDs))
	field := strconv.FormatUint(userID, 10)
	for index, peerID := range peerIDs {
		commands[index] = pipe.HGet(ctx, t.keyspace.directReadKey(peerID), field)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	for index, command := range commands {
		value, err := command.Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, err
		}
		position, err := strconv.ParseUint(value, 10, 64)
		if err != nil || position == 0 {
			continue
		}
		result[peerIDs[index]] = position
	}
	return result, nil
}

// States reads the counter, the user's watermark and the peer's watermark of
// several conversations in one pipeline, so they come from a single snapshot.
func (t *DirectReadTracker) States(
	ctx context.Context,
	userID uint64,
	peerIDs []uint64,
) (map[uint64]DirectConversationState, error) {
	if t == nil || t.client == nil {
		return nil, ErrMessageServiceUnavailable
	}
	if err := t.gate.Ready(); err != nil {
		return nil, err
	}
	states := make(map[uint64]DirectConversationState, len(peerIDs))
	if len(peerIDs) == 0 {
		return states, nil
	}

	pipe := t.client.Pipeline()
	lastCommands := make([]*redis.StringCmd, len(peerIDs))
	readCommands := make([]*redis.StringCmd, len(peerIDs))
	peerCommands := make([]*redis.StringCmd, len(peerIDs))
	userField := strconv.FormatUint(userID, 10)
	for index, peerID := range peerIDs {
		lastCommands[index] = pipe.Get(ctx, t.keyspace.directSequenceKey(userID, peerID))
		readCommands[index] = pipe.HGet(ctx, t.keyspace.directReadKey(userID), strconv.FormatUint(peerID, 10))
		peerCommands[index] = pipe.HGet(ctx, t.keyspace.directReadKey(peerID), userField)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	for index, peerID := range peerIDs {
		lastSeq, err := readCounterResult(lastCommands[index])
		if err != nil {
			return nil, err
		}
		readSeq, err := readPositionResult(readCommands[index])
		if err != nil {
			return nil, err
		}
		peerReadSeq, err := readPositionResult(peerCommands[index])
		if err != nil {
			return nil, err
		}
		states[peerID] = DirectConversationState{
			LastSeq:     lastSeq,
			ReadSeq:     readSeq,
			PeerReadSeq: peerReadSeq,
			Unread:      unreadBetween(lastSeq, readSeq),
		}
	}
	return states, nil
}

// UnreadCounts returns max(0, counter - position) for every requested peer.
// Every requested peer is present in the result, including zero counts.
func (t *DirectReadTracker) UnreadCounts(
	ctx context.Context,
	userID uint64,
	peerIDs []uint64,
	positions map[uint64]uint64,
) (map[uint64]int, error) {
	if t == nil || t.client == nil {
		return nil, ErrMessageServiceUnavailable
	}
	result := make(map[uint64]int, len(peerIDs))
	if len(peerIDs) == 0 {
		return result, nil
	}

	pipe := t.client.Pipeline()
	commands := make([]*redis.StringCmd, len(peerIDs))
	for index, peerID := range peerIDs {
		commands[index] = pipe.Get(ctx, t.keyspace.directSequenceKey(userID, peerID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	for index, command := range commands {
		lastSeq, err := readCounterResult(command)
		if err != nil {
			return nil, err
		}
		result[peerIDs[index]] = int(unreadBetween(lastSeq, positions[peerIDs[index]]))
	}
	return result, nil
}

// ReadModelReady reports whether the gate is open and seq:ready exists.
func (t *DirectReadTracker) ReadModelReady(ctx context.Context) (bool, error) {
	if t == nil || t.client == nil {
		return false, ErrMessageServiceUnavailable
	}
	if t.gate.Ready() != nil {
		return false, nil
	}
	exists, err := t.client.Exists(ctx, t.keyspace.sequenceReadyKey()).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

// MarkReadModelReady records that history and counters are complete.
func (t *DirectReadTracker) MarkReadModelReady(ctx context.Context) error {
	if t == nil || t.client == nil {
		return ErrMessageServiceUnavailable
	}
	if err := t.client.Set(ctx, t.keyspace.sequenceReadyKey(), "1", 0).Err(); err != nil {
		return fmt.Errorf("mark sequence ready: %w", err)
	}
	return nil
}

func unreadBetween(lastSeq, readSeq uint64) uint64 {
	if lastSeq <= readSeq {
		return 0
	}
	return lastSeq - readSeq
}

func readCounterResult(command *redis.StringCmd) (uint64, error) {
	return readOptionalUint(command)
}

func readPositionResult(command *redis.StringCmd) (uint64, error) {
	return readOptionalUint(command)
}

func readOptionalUint(command *redis.StringCmd) (uint64, error) {
	raw, err := command.Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(raw, 10, 64)
}

func parseReadAdvance(values []interface{}) (DirectReadAdvance, error) {
	if len(values) < 3 {
		return DirectReadAdvance{}, ErrMessageServiceUnavailable
	}
	lastReadSeq, err := parseRedisUint(values[0])
	if err != nil {
		return DirectReadAdvance{}, err
	}
	candidate, err := parseRedisUint(values[1])
	if err != nil {
		return DirectReadAdvance{}, err
	}
	advanced, err := parseReadAdvanced(values[2])
	if err != nil {
		return DirectReadAdvance{}, err
	}
	streamError := false
	if len(values) >= 4 {
		streamError = fmt.Sprint(values[3]) == "stream_error"
	}
	return DirectReadAdvance{
		LastReadSeq: lastReadSeq,
		Candidate:   candidate,
		Advanced:    advanced,
		StreamError: streamError,
	}, nil
}

func parseReadAdvanced(value interface{}) (bool, error) {
	switch typed := value.(type) {
	case string:
		return typed == "1", nil
	case int64:
		return typed == 1, nil
	case uint64:
		return typed == 1, nil
	default:
		return false, fmt.Errorf("parse read advance flag: unexpected type %T", value)
	}
}
