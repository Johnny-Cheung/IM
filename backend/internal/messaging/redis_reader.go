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
// Complete reports whether the window fully covers the requested range. When
// it is false the caller must consult MySQL before answering.
type ConversationPage struct {
	Page     store.MessagePage
	Complete bool
}

// RedisMessageReader reads one conversation from the directional indexes.
type RedisMessageReader struct {
	client   redis.UniversalClient
	keyspace Keyspace
	epoch    uint64
}

func NewRedisMessageReader(client redis.UniversalClient) *RedisMessageReader {
	return NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
}

func NewRedisMessageReaderWithKeyspace(
	client redis.UniversalClient,
	keyspace Keyspace,
	epoch uint64,
) *RedisMessageReader {
	return &RedisMessageReader{client: client, keyspace: keyspace, epoch: epoch}
}

// ReadConversation returns the newest messages before beforeID, or the newest
// messages of the conversation when beforeID is zero. The page is timestamp
// ordered ascending, matching the store contract.
func (r *RedisMessageReader) ReadConversation(
	ctx context.Context,
	userID, otherID, beforeID uint64,
	limit int,
) (ConversationPage, error) {
	return r.read(ctx, userID, otherID, beforeID, 0, limit)
}

// ReadConversationAfter returns messages newer than afterID, oldest first.
func (r *RedisMessageReader) ReadConversationAfter(
	ctx context.Context,
	userID, otherID, afterID uint64,
	limit int,
) (ConversationPage, error) {
	return r.read(ctx, userID, otherID, 0, afterID, limit)
}

func (r *RedisMessageReader) read(
	ctx context.Context,
	userID, otherID, beforeID, afterID uint64,
	limit int,
) (ConversationPage, error) {
	if r == nil || r.client == nil {
		return ConversationPage{}, ErrMessageServiceUnavailable
	}
	if limit < 1 {
		return ConversationPage{}, fmt.Errorf("invalid hot message limit")
	}

	lower := r.epoch
	if afterID > lower {
		lower = afterID
	}

	keys := []string{
		r.keyspace.directConversationKey(userID, otherID),
		r.keyspace.directConversationKey(otherID, userID),
	}
	ids := make(map[uint64]struct{}, (limit+1)*2)
	for _, key := range keys {
		var values []string
		var err error
		switch {
		case beforeID > 0:
			values, err = r.client.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
				Max:    exclusiveMax(beforeID),
				Min:    exclusiveMin(lower),
				Offset: 0,
				Count:  int64(limit + 1),
			}).Result()
		case afterID > 0:
			values, err = r.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
				Min:    exclusiveMin(lower),
				Max:    "+inf",
				Offset: 0,
				Count:  int64(limit + 1),
			}).Result()
		default:
			values, err = r.client.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
				Max:    "+inf",
				Min:    exclusiveMin(lower),
				Offset: 0,
				Count:  int64(limit + 1),
			}).Result()
		}
		if err != nil {
			return ConversationPage{}, err
		}
		for _, value := range values {
			id, parseErr := strconv.ParseUint(value, 10, 64)
			if parseErr != nil || id == 0 {
				continue
			}
			ids[id] = struct{}{}
		}
	}

	ordered := make([]uint64, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	if afterID > 0 {
		sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	} else {
		sort.Slice(ordered, func(i, j int) bool { return ordered[i] > ordered[j] })
	}
	if len(ordered) > limit+1 {
		ordered = ordered[:limit+1]
	}

	messages, missing, err := r.loadMessages(ctx, ordered)
	if err != nil {
		return ConversationPage{}, err
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID < messages[j].ID })

	hasMore := len(messages) > limit
	if hasMore {
		if afterID > 0 {
			messages = messages[:limit]
		} else {
			messages = messages[len(messages)-limit:]
		}
	}

	complete := false
	switch {
	case missing:
		complete = false
	case afterID > 0:
		complete = afterID >= r.epoch
	case r.epoch == 0:
		complete = true
	default:
		complete = len(ordered) >= limit+1
	}

	return ConversationPage{
		Page:     store.MessagePage{Messages: messages, HasMore: hasMore},
		Complete: complete,
	}, nil
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
			"id", "sender_id", "recipient_id", "client_message_id", "body", "created_at_ms",
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
	if len(values) != 6 || values[0] == nil {
		return store.Message{}, false, nil
	}
	messageID, err := redisValueUint(values[0])
	if err != nil {
		return store.Message{}, false, err
	}
	senderID, err := redisValueUint(values[1])
	if err != nil {
		return store.Message{}, false, err
	}
	recipientID, err := redisValueUint(values[2])
	if err != nil {
		return store.Message{}, false, err
	}
	clientMessageID, err := redisValueString(values[3])
	if err != nil {
		return store.Message{}, false, err
	}
	body, err := redisValueString(values[4])
	if err != nil {
		return store.Message{}, false, err
	}
	createdAtMS, err := redisValueInt64(values[5])
	if err != nil {
		return store.Message{}, false, err
	}
	return store.Message{
		ID:              messageID,
		SenderID:        senderID,
		RecipientID:     recipientID,
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       time.UnixMilli(createdAtMS).UTC(),
	}, true, nil
}

func exclusiveMax(id uint64) string {
	return fmt.Sprintf("(%d", id)
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
