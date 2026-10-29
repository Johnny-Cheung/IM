package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/redis/go-redis/v9"
)

var ErrMessageServiceUnavailable = errors.New("message service unavailable")

// ErrSeqNotReady reports that the conversation sequence is missing, the
// readiness marker is absent, an allocated message ID or sequence is already
// occupied, or calibration has not finished. It wraps
// ErrMessageServiceUnavailable so existing 503 mapping keeps working.
var ErrSeqNotReady = fmt.Errorf("%w: conversation sequence not ready", ErrMessageServiceUnavailable)

// ErrSeqOverflow reports that a conversation reached MaxSafeSeq.
var ErrSeqOverflow = fmt.Errorf("%w: conversation sequence exhausted", ErrMessageServiceUnavailable)

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
	SeedFloor uint64
	Keyspace  Keyspace
	DedupTTL  time.Duration
	// Gate blocks acceptance while Redis is being calibrated. Nil means ready.
	Gate *SeqGate
}

const defaultDedupTTL = 7 * 24 * time.Hour

func NewRedisAcceptor(client redis.Scripter, options RedisAcceptorOptions) *RedisAcceptor {
	if options.DedupTTL <= 0 {
		options.DedupTTL = defaultDedupTTL
	}
	return &RedisAcceptor{client: client, options: options}
}

func (a *RedisAcceptor) Accept(ctx context.Context, request DirectMessageRequest) (DirectMessageResult, error) {
	if a == nil || a.client == nil {
		return DirectMessageResult{}, ErrMessageServiceUnavailable
	}
	if err := a.options.Gate.Ready(); err != nil {
		return DirectMessageResult{}, err
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
		keyspace.directPairKey(request.SenderID, request.RecipientID),
		keyspace.directSequenceKey(request.SenderID, request.RecipientID),
		keyspace.directConversationLastKey(request.SenderID),
		keyspace.directConversationLastKey(request.RecipientID),
		keyspace.messageStreamKey(),
		keyspace.sequenceReadyKey(),
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
		store.MaxSafeSeq,
		int64(a.options.DedupTTL / time.Second),
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
		if len(values) < 4 {
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
		seq, err := parseRedisUint(values[3])
		if err != nil || seq == 0 {
			return DirectMessageResult{}, ErrSeqNotReady
		}
		return DirectMessageResult{
			Message: store.Message{
				ID:              messageID,
				Seq:             seq,
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
	case "seq_not_ready", "seq_unavailable", "id_unavailable":
		return DirectMessageResult{}, ErrSeqNotReady
	case "seq_overflow":
		return DirectMessageResult{}, ErrSeqOverflow
	default:
		return DirectMessageResult{}, ErrMessageServiceUnavailable
	}
}
