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

// GroupMessageReader reads the bounded hot-message window for one group.
type GroupMessageReader struct {
	client   redis.UniversalClient
	keyspace Keyspace
}

func NewGroupMessageReader(client redis.UniversalClient, keyspace Keyspace) *GroupMessageReader {
	return &GroupMessageReader{client: client, keyspace: keyspace}
}

func (r *GroupMessageReader) ListGroupMessages(
	ctx context.Context,
	_ uint64,
	groupID, beforeID uint64,
	limit int,
) (store.GroupMessagePage, error) {
	return r.list(ctx, groupID, beforeID, 0, limit)
}

func (r *GroupMessageReader) ListGroupMessagesAfter(
	ctx context.Context,
	_ uint64,
	groupID, afterID uint64,
	limit int,
) (store.GroupMessagePage, error) {
	return r.list(ctx, groupID, 0, afterID, limit)
}

func (r *GroupMessageReader) list(
	ctx context.Context,
	groupID, beforeID, afterID uint64,
	limit int,
) (store.GroupMessagePage, error) {
	if r == nil || r.client == nil {
		return store.GroupMessagePage{}, ErrMessageServiceUnavailable
	}
	if limit < 1 {
		return store.GroupMessagePage{}, fmt.Errorf("invalid group hot message limit")
	}

	var values []string
	var err error
	switch {
	case beforeID > 0:
		values, err = r.client.ZRevRangeByScore(ctx, r.keyspace.groupInboxKey(groupID), &redis.ZRangeBy{
			Max:    exclusiveMax(beforeID),
			Min:    "-inf",
			Offset: 0,
			Count:  int64(limit + 1),
		}).Result()
	case afterID > 0:
		values, err = r.client.ZRangeByScore(ctx, r.keyspace.groupInboxKey(groupID), &redis.ZRangeBy{
			Min:    exclusiveMin(afterID),
			Max:    "+inf",
			Offset: 0,
			Count:  int64(limit + 1),
		}).Result()
	default:
		values, err = r.client.ZRevRangeByScore(ctx, r.keyspace.groupInboxKey(groupID), &redis.ZRangeBy{
			Max:    "+inf",
			Min:    "-inf",
			Offset: 0,
			Count:  int64(limit + 1),
		}).Result()
	}
	if err != nil {
		return store.GroupMessagePage{}, err
	}

	messages := make([]store.GroupMessage, 0, len(values))
	for _, value := range values {
		messageID, err := strconv.ParseUint(value, 10, 64)
		if err != nil || messageID == 0 {
			continue
		}
		message, ok, err := r.loadMessage(ctx, messageID)
		if err != nil {
			return store.GroupMessagePage{}, err
		}
		if !ok || message.GroupID != groupID {
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
		if beforeID > 0 || afterID == 0 {
			messages = messages[len(messages)-limit:]
		} else {
			messages = messages[:limit]
		}
	}
	return store.GroupMessagePage{Messages: messages, HasMore: hasMore}, nil
}

func (r *GroupMessageReader) loadMessage(
	ctx context.Context,
	messageID uint64,
) (store.GroupMessage, bool, error) {
	values, err := r.client.HMGet(ctx, r.keyspace.groupMessageKey(messageID),
		"id", "group_id", "sender_id", "client_message_id", "body", "created_at_ms", "sender_username", "sender_nickname",
	).Result()
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	if len(values) != 8 || values[0] == nil {
		return store.GroupMessage{}, false, nil
	}
	id, err := redisValueUint(values[0])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	groupID, err := redisValueUint(values[1])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	senderID, err := redisValueUint(values[2])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	clientMessageID, err := redisValueString(values[3])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	body, err := redisValueString(values[4])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	createdAtMS, err := redisValueInt64(values[5])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	username, err := redisValueString(values[6])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	nickname, err := redisValueString(values[7])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	return store.GroupMessage{
		ID:              id,
		GroupID:         groupID,
		SenderID:        senderID,
		Sender:          store.PublicUser{Username: username, Nickname: nickname},
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       time.UnixMilli(createdAtMS).UTC(),
	}, true, nil
}
