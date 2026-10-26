package messaging

import (
	"context"
	"embed"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/redis/go-redis/v9"
)

//go:embed lua/group_accept.lua
var groupAcceptScriptSource string

var groupAcceptScript = redis.NewScript(groupAcceptScriptSource)

// GroupMessageRequest is the validated input to the Redis group acceptance path.
type GroupMessageRequest struct {
	SenderID        uint64
	GroupID         uint64
	ClientMessageID string
	Body            string
	CreatedAt       time.Time
	SenderUsername  string
	SenderNickname  string
}

// GroupMessageResult is the message returned by the group acceptance path.
type GroupMessageResult struct {
	Message store.GroupMessage
	Created bool
}

// GroupRedisAcceptor accepts group messages atomically in Redis.
type GroupRedisAcceptor struct {
	client   redis.Scripter
	keyspace Keyspace
	options  RedisAcceptorOptions
}

func NewGroupRedisAcceptor(
	client redis.Scripter,
	keyspace Keyspace,
	options RedisAcceptorOptions,
) *GroupRedisAcceptor {
	if options.DedupTTL <= 0 {
		options.DedupTTL = defaultDedupTTL
	}
	if options.StreamMaxLen <= 0 {
		options.StreamMaxLen = defaultStreamMaxLen
	}
	return &GroupRedisAcceptor{client: client, keyspace: keyspace, options: options}
}

func (a *GroupRedisAcceptor) Accept(
	ctx context.Context,
	request GroupMessageRequest,
) (GroupMessageResult, error) {
	if a == nil || a.client == nil {
		return GroupMessageResult{}, ErrMessageServiceUnavailable
	}
	if request.CreatedAt.IsZero() {
		request.CreatedAt = time.Now().UTC()
	}
	createdAt := request.CreatedAt.UTC().Truncate(time.Millisecond)
	keys := []string{
		a.keyspace.groupMembersKey(request.GroupID),
		a.keyspace.groupDissolvedKey(request.GroupID),
		a.keyspace.groupSyncRequiredKey(request.GroupID),
		a.keyspace.groupDedupKey(request.GroupID, request.SenderID, request.ClientMessageID),
		a.keyspace.counterKey(),
		a.keyspace.groupInboxKey(request.GroupID),
		a.keyspace.groupLastAcceptedKey(request.GroupID),
		a.keyspace.groupMessageStreamKey(),
	}
	args := []interface{}{
		request.SenderID,
		request.GroupID,
		request.ClientMessageID,
		request.Body,
		createdAt.UnixMilli(),
		hashMessageBody(request.Body),
		a.options.SeedFloor,
		store.MaxSafeMessageID,
		int64(a.options.DedupTTL / time.Second),
		a.options.StreamMaxLen,
		a.keyspace.prefixOrDefault() + "group-messages:",
		request.SenderUsername,
		request.SenderNickname,
	}
	values, err := groupAcceptScript.Run(ctx, a.client, keys, args...).Slice()
	if err != nil {
		return GroupMessageResult{}, err
	}
	if len(values) == 0 {
		return GroupMessageResult{}, ErrMessageServiceUnavailable
	}
	status, ok := values[0].(string)
	if !ok {
		return GroupMessageResult{}, ErrMessageServiceUnavailable
	}
	switch status {
	case "accepted", "duplicate":
		if len(values) < 3 {
			return GroupMessageResult{}, ErrMessageServiceUnavailable
		}
		messageID, err := parseRedisUint(values[1])
		if err != nil {
			return GroupMessageResult{}, err
		}
		createdAtMS, err := parseRedisInt64(values[2])
		if err != nil {
			return GroupMessageResult{}, err
		}
		return GroupMessageResult{
			Message: store.GroupMessage{
				ID:              messageID,
				GroupID:         request.GroupID,
				SenderID:        request.SenderID,
				Sender:          store.PublicUser{Username: request.SenderUsername, Nickname: request.SenderNickname},
				ClientMessageID: request.ClientMessageID,
				Body:            request.Body,
				CreatedAt:       time.UnixMilli(createdAtMS).UTC(),
			},
			Created: status == "accepted",
		}, nil
	case "dissolved", "not_member":
		return GroupMessageResult{}, store.ErrGroupNotFound
	case "projection_missing":
		return GroupMessageResult{}, ErrMessageServiceUnavailable
	case "conflict":
		return GroupMessageResult{}, store.ErrMessageConflict
	case "id_overflow":
		return GroupMessageResult{}, store.ErrMessageIDExhausted
	default:
		return GroupMessageResult{}, ErrMessageServiceUnavailable
	}
}

var _ embed.FS
