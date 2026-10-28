package messaging

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// ConversationPage is one direct-message page served from the Redis window.
//
// Complete reports whether Redis can prove the whole requested range: every
// seq of the window resolved from a Redis entity, the window is non-empty, and
// the counter covers the durable tail (a counter behind MySQL's MAX(seq) makes
// the window unprovable). When it is false the caller must consult MySQL
// before answering.
type ConversationPage struct {
	Page     store.MessagePage
	Complete bool
}

// RedisMessageReader reads one private conversation from dm:pair and the
// bidirectional per-user feed from dm:feed.
type RedisMessageReader struct {
	client    redis.UniversalClient
	keyspace  Keyspace
	feedEpoch uint64
}

func NewRedisMessageReader(client redis.UniversalClient) *RedisMessageReader {
	return NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
}

// NewRedisMessageReaderWithKeyspace builds a reader. feedEpoch is the
// global-ID floor of dm:feed only; conversation ranges never use it.
func NewRedisMessageReaderWithKeyspace(
	client redis.UniversalClient,
	keyspace Keyspace,
	feedEpoch uint64,
) *RedisMessageReader {
	return &RedisMessageReader{client: client, keyspace: keyspace, feedEpoch: feedEpoch}
}

// ReadConversation returns the newest messages before beforeSeq, or the newest
// messages of the conversation when beforeSeq is zero. The page is ordered by
// seq ascending, matching the store contract.
func (r *RedisMessageReader) ReadConversation(
	ctx context.Context,
	userID, otherID, beforeSeq uint64,
	limit int,
) (ConversationPage, error) {
	return r.read(ctx, userID, otherID, false, beforeSeq, 0, limit)
}

// ReadConversationAfter returns messages with a seq greater than afterSeq,
// oldest first. afterSeq zero starts at seq 1.
func (r *RedisMessageReader) ReadConversationAfter(
	ctx context.Context,
	userID, otherID, afterSeq uint64,
	limit int,
) (ConversationPage, error) {
	return r.read(ctx, userID, otherID, true, 0, afterSeq, limit)
}

// FeedPage is one page of the user's direct-message feed.
//
// Complete reports whether the feed covers every message after the cursor.
// When it is false the caller must resynchronise from conversation history.
type FeedPage struct {
	Page     store.MessagePage
	Complete bool
}

// ReadFeed returns the messages the user exchanged after afterID, oldest
// first, across all direct conversations. The cursor stays a global message
// ID; returned messages carry their conversation seq.
func (r *RedisMessageReader) ReadFeed(
	ctx context.Context,
	userID, afterID uint64,
	limit int,
) (FeedPage, error) {
	if r == nil || r.client == nil {
		return FeedPage{}, ErrMessageServiceUnavailable
	}
	if limit < 1 {
		return FeedPage{}, fmt.Errorf("invalid feed limit")
	}

	lower := r.feedEpoch
	if afterID > lower {
		lower = afterID
	}
	values, err := r.client.ZRangeByScore(ctx, r.keyspace.directFeedKey(userID), &redis.ZRangeBy{
		Min:    exclusiveMin(lower),
		Max:    "+inf",
		Offset: 0,
		Count:  int64(limit + 1),
	}).Result()
	if err != nil {
		return FeedPage{}, err
	}

	ids := make([]uint64, 0, len(values))
	for _, value := range values {
		id, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr != nil || id == 0 {
			continue
		}
		ids = append(ids, id)
	}

	messages, missing, err := r.loadMessages(ctx, ids)
	if err != nil {
		return FeedPage{}, err
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID < messages[j].ID })

	hasMore := len(messages) > limit
	if hasMore {
		messages = messages[:limit]
	}
	return FeedPage{
		Page:     store.MessagePage{Messages: messages, HasMore: hasMore},
		Complete: !missing && afterID >= r.feedEpoch,
	}, nil
}

// LastConversationMessages returns the newest message exchanged with each peer,
// keyed by peer user ID.
func (r *RedisMessageReader) LastConversationMessages(
	ctx context.Context,
	userID uint64,
) (map[uint64]store.Message, error) {
	if r == nil || r.client == nil {
		return nil, ErrMessageServiceUnavailable
	}
	values, err := r.client.HGetAll(ctx, r.keyspace.directConversationLastKey(userID)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return map[uint64]store.Message{}, nil
	}

	peerByMessage := make(map[uint64]uint64, len(values))
	ids := make([]uint64, 0, len(values))
	for peerRaw, messageRaw := range values {
		peerID, parseErr := strconv.ParseUint(peerRaw, 10, 64)
		if parseErr != nil {
			continue
		}
		messageID, parseErr := strconv.ParseUint(messageRaw, 10, 64)
		if parseErr != nil {
			continue
		}
		peerByMessage[messageID] = peerID
		ids = append(ids, messageID)
	}

	messages, _, err := r.loadMessages(ctx, ids)
	if err != nil {
		return nil, err
	}
	result := make(map[uint64]store.Message, len(messages))
	for _, message := range messages {
		if peerID, ok := peerByMessage[message.ID]; ok {
			result[peerID] = message
		}
	}
	return result, nil
}

// ReadDirectSequenceCounters reads the current seq counter of several private
// conversations in one pipeline. Missing counters are reported as zero.
func ReadDirectSequenceCounters(
	ctx context.Context,
	client redis.UniversalClient,
	keyspace Keyspace,
	userID uint64,
	peerIDs []uint64,
) (map[uint64]uint64, error) {
	if client == nil {
		return nil, ErrMessageServiceUnavailable
	}
	result := make(map[uint64]uint64, len(peerIDs))
	if len(peerIDs) == 0 {
		return result, nil
	}
	pipe := client.Pipeline()
	commands := make([]*redis.StringCmd, len(peerIDs))
	for index, peerID := range peerIDs {
		commands[index] = pipe.Get(ctx, keyspace.directSequenceKey(userID, peerID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	for index, command := range commands {
		raw, err := command.Result()
		if errors.Is(err, redis.Nil) {
			result[peerIDs[index]] = 0
			continue
		}
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse conversation seq: %w", err)
		}
		result[peerIDs[index]] = value
	}
	return result, nil
}

// ReadGroupSequenceCounters reads the current seq counter of several groups in
// one pipeline. Missing counters are reported as zero.
func ReadGroupSequenceCounters(
	ctx context.Context,
	client redis.UniversalClient,
	keyspace Keyspace,
	groupIDs []uint64,
) (map[uint64]uint64, error) {
	if client == nil {
		return nil, ErrMessageServiceUnavailable
	}
	result := make(map[uint64]uint64, len(groupIDs))
	if len(groupIDs) == 0 {
		return result, nil
	}
	pipe := client.Pipeline()
	commands := make([]*redis.StringCmd, len(groupIDs))
	for index, groupID := range groupIDs {
		commands[index] = pipe.Get(ctx, keyspace.groupSequenceKey(groupID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	for index, command := range commands {
		raw, err := command.Result()
		if errors.Is(err, redis.Nil) {
			result[groupIDs[index]] = 0
			continue
		}
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse group seq: %w", err)
		}
		result[groupIDs[index]] = value
	}
	return result, nil
}

func (r *RedisMessageReader) read(
	ctx context.Context,
	userID, otherID uint64,
	afterMode bool,
	beforeSeq, afterSeq uint64,
	limit int,
) (ConversationPage, error) {
	if r == nil || r.client == nil {
		return ConversationPage{}, ErrMessageServiceUnavailable
	}
	if limit < 1 {
		return ConversationPage{}, fmt.Errorf("invalid hot message limit")
	}

	counter, present, err := r.readSequenceCounter(
		ctx, r.keyspace.directSequenceKey(userID, otherID),
	)
	if err != nil {
		return ConversationPage{}, err
	}
	if !present {
		// The counter is the only proof of the seq range. Without it the hot
		// window cannot be trusted, so the caller must read MySQL.
		return ConversationPage{
			Page:     store.MessagePage{},
			Complete: false,
		}, nil
	}

	var low, high uint64
	var hasMore, counterBounded bool
	switch {
	case afterMode:
		low = afterSeq
		high = counter
		if afterSeq+uint64(limit) < high {
			high = afterSeq + uint64(limit)
		} else {
			counterBounded = true
		}
		hasMore = high < counter
	default:
		high = counter
		counterBounded = beforeSeq == 0
		if beforeSeq > 0 && beforeSeq-1 < high {
			// before_seq is exclusive: the page ends just below it.
			high = beforeSeq - 1
		} else if beforeSeq > 0 {
			counterBounded = true
		}
	}
	staleCounter := false
	if counterBounded {
		// A lowered counter with an intact index means the tail extends past
		// the counter. Keep the window bounded and hand the page to MySQL
		// instead of loading an unbounded index range.
		top, err := r.client.ZRangeWithScores(
			ctx,
			r.keyspace.directPairKey(userID, otherID),
			-1,
			-1,
		).Result()
		if err != nil {
			return ConversationPage{}, err
		}
		if len(top) > 0 && uint64(top[0].Score) > high {
			staleCounter = true
		}
	}
	if !afterMode {
		if beforeSeq > 0 && beforeSeq-1 < high {
			high = beforeSeq - 1
		}
		if high > uint64(limit) {
			low = high - uint64(limit)
		}
		hasMore = low > 0
	}
	if low >= high {
		// A counter that is behind MySQL's MAX(seq) (lost AOF tail, lowered
		// counter, in-flight calibration) makes an empty window unprovable.
		// Never claim completeness; let the caller fill from MySQL.
		return ConversationPage{
			Page:     store.MessagePage{},
			Complete: false,
		}, nil
	}

	entries, err := r.client.ZRangeByScoreWithScores(
		ctx,
		r.keyspace.directPairKey(userID, otherID),
		&redis.ZRangeBy{
			Min: exclusiveMin(low),
			Max: strconv.FormatUint(high, 10),
		},
	).Result()
	if err != nil {
		return ConversationPage{}, err
	}

	ids := make([]uint64, 0, len(entries))
	seqByID := make(map[uint64]uint64, len(entries))
	for _, entry := range entries {
		member, ok := entry.Member.(string)
		if !ok {
			continue
		}
		messageID, parseErr := strconv.ParseUint(member, 10, 64)
		if parseErr != nil || messageID == 0 {
			continue
		}
		ids = append(ids, messageID)
		seqByID[messageID] = uint64(entry.Score)
	}

	loaded, _, err := r.loadMessages(ctx, ids)
	if err != nil {
		return ConversationPage{}, err
	}
	messages := make([]store.Message, 0, len(loaded))
	resolved := make(map[uint64]struct{}, len(loaded))
	for _, message := range loaded {
		seq, ok := seqByID[message.ID]
		if !ok || seq == 0 || message.Seq != seq {
			continue
		}
		messages = append(messages, message)
		resolved[seq] = struct{}{}
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].Seq < messages[j].Seq })
	if len(messages) > limit {
		if afterMode {
			messages = messages[:limit]
			// The window was extended past a stale counter, so the cut-off
			// tail still has more messages.
			hasMore = true
		} else {
			messages = messages[len(messages)-limit:]
		}
	}

	return ConversationPage{
		Page:     store.MessagePage{Messages: messages, HasMore: hasMore},
		Complete: !staleCounter && completeSeqRange(resolved, low, high),
	}, nil
}

func (r *RedisMessageReader) readSequenceCounter(
	ctx context.Context,
	key string,
) (uint64, bool, error) {
	raw, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read conversation seq: %w", err)
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse conversation seq: %w", err)
	}
	return value, true, nil
}

// completeSeqRange reports whether every seq in (low, high] was resolved.
func completeSeqRange(resolved map[uint64]struct{}, low, high uint64) bool {
	for seq := low + 1; seq <= high; seq++ {
		if _, ok := resolved[seq]; !ok {
			return false
		}
	}
	return true
}

func (r *RedisMessageReader) loadMessages(
	ctx context.Context,
	ids []uint64,
) ([]store.Message, bool, error) {
	if len(ids) == 0 {
		return nil, false, nil
	}

	pipe := r.client.Pipeline()
	commands := make([]*redis.SliceCmd, len(ids))
	for index, id := range ids {
		commands[index] = pipe.HMGet(ctx, r.keyspace.messageKey(id),
			"id", "seq", "sender_id", "recipient_id", "client_message_id", "body", "created_at_ms",
		)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, false, err
	}

	messages := make([]store.Message, 0, len(ids))
	missing := false
	for _, command := range commands {
		values, err := command.Result()
		if err != nil {
			return nil, false, err
		}
		message, ok, err := parseMessageValues(values)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			missing = true
			continue
		}
		messages = append(messages, message)
	}
	return messages, missing, nil
}

func parseMessageValues(values []interface{}) (store.Message, bool, error) {
	if len(values) != 7 || values[0] == nil || values[1] == nil {
		return store.Message{}, false, nil
	}
	messageID, err := redisValueUint(values[0])
	if err != nil {
		return store.Message{}, false, err
	}
	seq, err := redisValueUint(values[1])
	if err != nil || seq == 0 {
		return store.Message{}, false, nil
	}
	senderID, err := redisValueUint(values[2])
	if err != nil {
		return store.Message{}, false, err
	}
	recipientID, err := redisValueUint(values[3])
	if err != nil {
		return store.Message{}, false, err
	}
	clientMessageID, err := redisValueString(values[4])
	if err != nil {
		return store.Message{}, false, err
	}
	body, err := redisValueString(values[5])
	if err != nil {
		return store.Message{}, false, err
	}
	createdAtMS, err := redisValueInt64(values[6])
	if err != nil {
		return store.Message{}, false, err
	}
	return store.Message{
		ID:              messageID,
		Seq:             seq,
		SenderID:        senderID,
		RecipientID:     recipientID,
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       time.UnixMilli(createdAtMS).UTC(),
	}, true, nil
}

func exclusiveMin(id uint64) string {
	if id == 0 {
		return "-inf"
	}
	return fmt.Sprintf("(%d", id)
}

func redisValueString(value interface{}) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	default:
		return "", fmt.Errorf("unexpected Redis value type %T", value)
	}
}

func redisValueUint(value interface{}) (uint64, error) {
	raw, err := redisValueString(value)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(raw, 10, 64)
}

func redisValueInt64(value interface{}) (int64, error) {
	raw, err := redisValueString(value)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(raw, 10, 64)
}
