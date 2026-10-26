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

// GroupConversationPage is one group-message page served from the Redis window.
//
// Complete reports whether the window fully covers the requested range. When
// it is false the caller must consult MySQL before answering.
type GroupConversationPage struct {
	Page     store.GroupMessagePage
	Complete bool
}

// GroupMessageReader reads one group from the Redis group inbox.
type GroupMessageReader struct {
	client   redis.UniversalClient
	keyspace Keyspace
	epoch    uint64
}

func NewGroupMessageReader(
	client redis.UniversalClient,
	keyspace Keyspace,
	epoch uint64,
) *GroupMessageReader {
	return &GroupMessageReader{client: client, keyspace: keyspace, epoch: epoch}
}

// ReadGroup returns the newest group messages before beforeID, or the newest
// messages of the group when beforeID is zero.
func (r *GroupMessageReader) ReadGroup(
	ctx context.Context,
	_ uint64,
	groupID, beforeID uint64,
	limit int,
) (GroupConversationPage, error) {
	return r.read(ctx, groupID, beforeID, 0, limit)
}

// ReadGroupAfter returns group messages newer than afterID, oldest first.
func (r *GroupMessageReader) ReadGroupAfter(
	ctx context.Context,
	_ uint64,
	groupID, afterID uint64,
	limit int,
) (GroupConversationPage, error) {
	return r.read(ctx, groupID, 0, afterID, limit)
}

func (r *GroupMessageReader) read(
	ctx context.Context,
	groupID, beforeID, afterID uint64,
	limit int,
) (GroupConversationPage, error) {
	if r == nil || r.client == nil {
		return GroupConversationPage{}, ErrMessageServiceUnavailable
	}
	if limit < 1 {
		return GroupConversationPage{}, fmt.Errorf("invalid group hot message limit")
	}

	lower := r.epoch
	if afterID > lower {
		lower = afterID
	}
	key := r.keyspace.groupInboxKey(groupID)

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
		return GroupConversationPage{}, err
	}

	ids := make([]uint64, 0, len(values))
	for _, value := range values {
		messageID, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr != nil || messageID == 0 {
			continue
		}
		ids = append(ids, messageID)
	}

	messages, missing, err := r.loadMessages(ctx, ids, groupID)
	if err != nil {
		return GroupConversationPage{}, err
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
		complete = len(ids) >= limit+1
	}

	return GroupConversationPage{
		Page:     store.GroupMessagePage{Messages: messages, HasMore: hasMore},
		Complete: complete,
	}, nil
}

func (r *GroupMessageReader) loadMessages(
	ctx context.Context,
	ids []uint64,
	groupID uint64,
) ([]store.GroupMessage, bool, error) {
	if len(ids) == 0 {
		return nil, false, nil
	}

	pipe := r.client.Pipeline()
	commands := make([]*redis.SliceCmd, len(ids))
	for index, id := range ids {
		commands[index] = pipe.HMGet(ctx, r.keyspace.groupMessageKey(id),
			"id", "group_id", "sender_id", "client_message_id", "body", "created_at_ms",
			"sender_username", "sender_nickname",
		)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, false, err
	}

	messages := make([]store.GroupMessage, 0, len(ids))
	missing := false
	for _, command := range commands {
		values, err := command.Result()
		if err != nil {
			return nil, false, err
		}
		message, ok, err := parseGroupMessageValues(values)
		if err != nil {
			return nil, false, err
		}
		if !ok || message.GroupID != groupID {
			missing = true
			continue
		}
		messages = append(messages, message)
	}
	return messages, missing, nil
}

func parseGroupMessageValues(values []interface{}) (store.GroupMessage, bool, error) {
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
