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
// Complete reports whether every seq of the requested range was resolved from
// a Redis entity. When it is false the caller must consult MySQL before
// answering.
type GroupConversationPage struct {
	Page     store.GroupMessagePage
	Complete bool
}

// GroupMessageReader reads one group from the Redis group inbox.
type GroupMessageReader struct {
	client   redis.UniversalClient
	keyspace Keyspace
}

func NewGroupMessageReader(
	client redis.UniversalClient,
	keyspace Keyspace,
) *GroupMessageReader {
	return &GroupMessageReader{client: client, keyspace: keyspace}
}

// ReadGroup returns the newest group messages before beforeSeq, or the newest
// messages of the group when beforeSeq is zero.
func (r *GroupMessageReader) ReadGroup(
	ctx context.Context,
	_ uint64,
	groupID, beforeSeq uint64,
	limit int,
) (GroupConversationPage, error) {
	return r.read(ctx, groupID, false, beforeSeq, 0, limit)
}

// ReadGroupAfter returns group messages with a seq greater than afterSeq,
// oldest first. afterSeq zero starts at seq 1.
func (r *GroupMessageReader) ReadGroupAfter(
	ctx context.Context,
	_ uint64,
	groupID, afterSeq uint64,
	limit int,
) (GroupConversationPage, error) {
	return r.read(ctx, groupID, true, 0, afterSeq, limit)
}

func (r *GroupMessageReader) read(
	ctx context.Context,
	groupID uint64,
	afterMode bool,
	beforeSeq, afterSeq uint64,
	limit int,
) (GroupConversationPage, error) {
	if r == nil || r.client == nil {
		return GroupConversationPage{}, ErrMessageServiceUnavailable
	}
	if limit < 1 {
		return GroupConversationPage{}, fmt.Errorf("invalid group hot message limit")
	}

	counter, present, err := r.readSequenceCounter(
		ctx, r.keyspace.groupSequenceKey(groupID),
	)
	if err != nil {
		return GroupConversationPage{}, err
	}
	if !present {
		// The counter is the only proof of the seq range. Without it the hot
		// window cannot be trusted, so the caller must read MySQL.
		return GroupConversationPage{
			Page:     store.GroupMessagePage{},
			Complete: false,
		}, nil
	}

	var low, high uint64
	var hasMore bool
	switch {
	case afterMode:
		low = afterSeq
		high = counter
		if afterSeq+uint64(limit) < high {
			high = afterSeq + uint64(limit)
		}
		hasMore = high < counter
	default:
		high = counter
		if beforeSeq > 0 {
			// before_seq is exclusive: the page ends just below it.
			if upper := beforeSeq - 1; upper < high {
				high = upper
			}
		}
		if high > uint64(limit) {
			low = high - uint64(limit)
		}
		hasMore = low > 0
	}
	if low >= high {
		return GroupConversationPage{
			Page:     store.GroupMessagePage{},
			Complete: true,
		}, nil
	}

	entries, err := r.client.ZRangeByScoreWithScores(
		ctx,
		r.keyspace.groupInboxKey(groupID),
		&redis.ZRangeBy{
			Min: exclusiveMin(low),
			Max: strconv.FormatUint(high, 10),
		},
	).Result()
	if err != nil {
		return GroupConversationPage{}, err
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

	loaded, _, err := r.loadMessages(ctx, ids, groupID)
	if err != nil {
		return GroupConversationPage{}, err
	}
	messages := make([]store.GroupMessage, 0, len(loaded))
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
		} else {
			messages = messages[len(messages)-limit:]
		}
	}

	return GroupConversationPage{
		Page:     store.GroupMessagePage{Messages: messages, HasMore: hasMore},
		Complete: completeSeqRange(resolved, low, high),
	}, nil
}

func (r *GroupMessageReader) readSequenceCounter(
	ctx context.Context,
	key string,
) (uint64, bool, error) {
	raw, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read group seq: %w", err)
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse group seq: %w", err)
	}
	return value, true, nil
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
			"id", "seq", "group_id", "sender_id", "client_message_id", "body", "created_at_ms",
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
	if len(values) != 9 || values[0] == nil || values[1] == nil {
		return store.GroupMessage{}, false, nil
	}
	id, err := redisValueUint(values[0])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	seq, err := redisValueUint(values[1])
	if err != nil || seq == 0 {
		return store.GroupMessage{}, false, nil
	}
	groupID, err := redisValueUint(values[2])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	senderID, err := redisValueUint(values[3])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	clientMessageID, err := redisValueString(values[4])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	body, err := redisValueString(values[5])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	createdAtMS, err := redisValueInt64(values[6])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	username, err := redisValueString(values[7])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	nickname, err := redisValueString(values[8])
	if err != nil {
		return store.GroupMessage{}, false, err
	}
	return store.GroupMessage{
		ID:              id,
		Seq:             seq,
		GroupID:         groupID,
		SenderID:        senderID,
		Sender:          store.PublicUser{Username: username, Nickname: nickname},
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       time.UnixMilli(createdAtMS).UTC(),
	}, true, nil
}
