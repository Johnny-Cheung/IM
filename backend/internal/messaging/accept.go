package messaging

import (
	"context"
	"errors"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/redis/go-redis/v9"
)

var ErrMessageServiceUnavailable = errors.New("message service unavailable")

// DirectMessageRequest is the validated input to the Redis acceptance path.
type DirectMessageRequest struct {
	SenderID        uint64
	RecipientID     uint64
	ClientMessageID string
	Body            string
	CreatedAt       time.Time
}

// DirectMessageResult is the message returned by the acceptance path.
type DirectMessageResult struct {
	Message store.Message
	Created bool
}

// RedisAcceptor accepts direct messages atomically in Redis.
type RedisAcceptor struct {
	client  redis.Scripter
	options RedisAcceptorOptions
}

// RedisAcceptorOptions controls Redis message storage.
//
// Message entities and the inbox/outbox indexes are append-only: they carry no
// TTL and are never trimmed, so every index entry always resolves to a stored
// entity. Bounding the hot window is deferred until the read path owns an
// explicit window marker.
type RedisAcceptorOptions struct {
	SeedFloor    uint64
	Keyspace     Keyspace
	DedupTTL     time.Duration
	StreamMaxLen int64
}

const (
	defaultDedupTTL     = 7 * 24 * time.Hour
	defaultStreamMaxLen = int64(10000)
)

func NewRedisAcceptor(client redis.Scripter, options RedisAcceptorOptions) *RedisAcceptor {
	if options.DedupTTL <= 0 {
		options.DedupTTL = defaultDedupTTL
	}
	if options.StreamMaxLen <= 0 {
		options.StreamMaxLen = defaultStreamMaxLen
	}
	return &RedisAcceptor{client: client, options: options}
}

func (a *RedisAcceptor) Accept(ctx context.Context, request DirectMessageRequest) (DirectMessageResult, error) {
	if a == nil || a.client == nil {
		return DirectMessageResult{}, ErrMessageServiceUnavailable
	}
	if request.CreatedAt.IsZero() {
		request.CreatedAt = time.Now().UTC()
	}
	createdAt := request.CreatedAt.UTC().Truncate(time.Millisecond)
	bodyHash := hashMessageBody(request.Body)
	keyspace := a.options.Keyspace
	keys := []string{
		keyspace.counterKey(),
		keyspace.friendshipKey(request.SenderID),
		keyspace.dedupKey(request.SenderID, request.RecipientID, request.ClientMessageID),
		keyspace.directFeedKey(request.SenderID),
		keyspace.directFeedKey(request.RecipientID),
		keyspace.directConversationKey(request.RecipientID, request.SenderID),
		keyspace.directConversationLastKey(request.SenderID),
		keyspace.directConversationLastKey(request.RecipientID),
		keyspace.messageStreamKey(),
	}
	args := []interface{}{
		request.SenderID,
		request.RecipientID,
		request.ClientMessageID,
		request.Body,
		createdAt.UnixMilli(),
		bodyHash,
		a.options.SeedFloor,
		store.MaxSafeMessageID,
		int64(a.options.DedupTTL / time.Second),
		a.options.StreamMaxLen,
		keyspace.prefixOrDefault() + "messages:",
	}
	values, err := acceptScript.Run(ctx, a.client, keys, args...).Slice()
	if err != nil {
		return DirectMessageResult{}, err
	}
	if len(values) == 0 {
		return DirectMessageResult{}, ErrMessageServiceUnavailable
	}
	status, ok := values[0].(string)
	if !ok {
		return DirectMessageResult{}, ErrMessageServiceUnavailable
	}
	switch status {
	case "accepted", "duplicate":
		if len(values) < 3 {
			return DirectMessageResult{}, ErrMessageServiceUnavailable
		}
		messageID, err := parseRedisUint(values[1])
		if err != nil {
			return DirectMessageResult{}, err
		}
		createdAtMS, err := parseRedisInt64(values[2])
		if err != nil {
			return DirectMessageResult{}, err
		}
		return DirectMessageResult{
			Message: store.Message{
				ID:              messageID,
				SenderID:        request.SenderID,
				RecipientID:     request.RecipientID,
				ClientMessageID: request.ClientMessageID,
				Body:            request.Body,
				CreatedAt:       time.UnixMilli(createdAtMS).UTC(),
			},
			Created: status == "accepted",
		}, nil
	case "cannot_message_self":
		return DirectMessageResult{}, store.ErrCannotMessageSelf
	case "not_friends":
		return DirectMessageResult{}, store.ErrNotFriends
	case "conflict":
		return DirectMessageResult{}, store.ErrMessageConflict
	case "id_overflow":
		return DirectMessageResult{}, store.ErrMessageIDExhausted
	default:
		return DirectMessageResult{}, ErrMessageServiceUnavailable
	}
}
