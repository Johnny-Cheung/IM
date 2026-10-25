package messaging

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/redis/go-redis/v9"
)

// RedisMessageReader reads the bounded hot-message window for one conversation.
type RedisMessageReader struct {
	client redis.UniversalClient
}

func NewRedisMessageReader(client redis.UniversalClient) *RedisMessageReader {
	return &RedisMessageReader{client: client}
}

func (r *RedisMessageReader) ListMessages(
	ctx context.Context,
	userID, otherID, beforeID uint64,
	limit int,
) (store.MessagePage, error) {
	return r.list(ctx, userID, otherID, beforeID, 0, limit)
}

func (r *RedisMessageReader) ListMessagesAfter(
	ctx context.Context,
	userID, otherID, afterID uint64,
	limit int,
) (store.MessagePage, error) {
	return r.list(ctx, userID, otherID, 0, afterID, limit)
}

func (r *RedisMessageReader) list(
	ctx context.Context,
	userID, otherID, beforeID, afterID uint64,
	limit int,
) (store.MessagePage, error) {
	if r == nil || r.client == nil {
		return store.MessagePage{}, ErrMessageServiceUnavailable
	}
	if limit < 1 {
		return store.MessagePage{}, fmt.Errorf("invalid hot message limit")
	}

	ids := make(map[uint64]struct{}, (limit+1)*2)
	for _, key := range []string{inboxKey(userID), outboxKey(userID)} {
		var values []string
		var err error
		if beforeID > 0 {
			values, err = r.client.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
				Max:    exclusiveMax(beforeID),
				Min:    "-inf",
				Offset: 0,
				Count:  int64(limit + 1),
			}).Result()
		} else {
			values, err = r.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
				Min:    exclusiveMin(afterID),
				Max:    "+inf",
				Offset: 0,
				Count:  int64(limit + 1),
			}).Result()
		}
		if err != nil {
			return store.MessagePage{}, err
		}
		for _, value := range values {
			id, err := strconv.ParseUint(value, 10, 64)
			if err != nil || id == 0 {
				continue
			}
			ids[id] = struct{}{}
		}
	}

	messages := make([]store.Message, 0, len(ids))
	for id := range ids {
		message, ok, err := r.loadMessage(ctx, id)
		if err != nil {
			return store.MessagePage{}, err
		}
		if !ok || !sameConversation(message, userID, otherID) {
			continue
		}
		if beforeID > 0 && message.ID >= beforeID {
			continue
		}
		if afterID > 0 && message.ID <= afterID {
			continue
		}
		messages = append(messages, message)
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID < messages[j].ID })

	hasMore := len(messages) > limit
	if hasMore {
		if beforeID > 0 {
			messages = messages[len(messages)-limit:]
		} else {
			messages = messages[:limit]
		}
	}
	return store.MessagePage{Messages: messages, HasMore: hasMore}, nil
}

func (r *RedisMessageReader) loadMessage(ctx context.Context, id uint64) (store.Message, bool, error) {
	values, err := r.client.HMGet(ctx, messageKey(id),
		"id", "sender_id", "recipient_id", "client_message_id", "body", "created_at_ms",
	).Result()
	if err != nil {
		return store.Message{}, false, err
	}
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

func sameConversation(message store.Message, userID, otherID uint64) bool {
	return (message.SenderID == userID && message.RecipientID == otherID) ||
		(message.SenderID == otherID && message.RecipientID == userID)
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
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("invalid Redis message ID")
	}
	return parsed, nil
}

func redisValueInt64(value interface{}) (int64, error) {
	raw, err := redisValueString(value)
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid Redis timestamp")
	}
	return parsed, nil
}
