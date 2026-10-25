package messaging

import (
	"context"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/redis/go-redis/v9"
)

var ensureFriendshipScript = redis.NewScript(`
redis.call('SADD', KEYS[1], ARGV[2])
redis.call('SADD', KEYS[2], ARGV[1])
return 1
`)

var removeFriendshipScript = redis.NewScript(`
redis.call('SREM', KEYS[1], ARGV[2])
redis.call('SREM', KEYS[2], ARGV[1])
return 1
`)

// FriendshipProjection mirrors the directed friendship rows needed by Lua.
type FriendshipProjection struct {
	client redis.UniversalClient
}

func NewFriendshipProjection(client redis.UniversalClient) *FriendshipProjection {
	return &FriendshipProjection{client: client}
}

// EnsurePair adds both directions of a friendship atomically in Redis.
func (p *FriendshipProjection) EnsurePair(ctx context.Context, userID, friendUserID uint64) error {
	if p == nil || p.client == nil {
		return ErrMessageServiceUnavailable
	}
	return ensureFriendshipScript.Run(
		ctx,
		p.client,
		[]string{friendshipKey(userID), friendshipKey(friendUserID)},
		userID,
		friendUserID,
	).Err()
}

// RemovePair removes both directions of a friendship atomically in Redis.
func (p *FriendshipProjection) RemovePair(ctx context.Context, userID, friendUserID uint64) error {
	if p == nil || p.client == nil {
		return ErrMessageServiceUnavailable
	}
	return removeFriendshipScript.Run(
		ctx,
		p.client,
		[]string{friendshipKey(userID), friendshipKey(friendUserID)},
		userID,
		friendUserID,
	).Err()
}

// IsFriend checks the projected directed relationship.
func (p *FriendshipProjection) IsFriend(ctx context.Context, userID, friendUserID uint64) (bool, error) {
	if p == nil || p.client == nil {
		return false, ErrMessageServiceUnavailable
	}
	return p.client.SIsMember(ctx, friendshipKey(userID), friendUserID).Result()
}

// Rebuild replaces all v1.2 friendship projection keys from MySQL rows.
func (p *FriendshipProjection) Rebuild(ctx context.Context, pairs []store.FriendshipPair) error {
	if p == nil || p.client == nil {
		return ErrMessageServiceUnavailable
	}

	var cursor uint64
	for {
		keys, next, err := p.client.Scan(ctx, cursor, redisPrefix+"friends:*", 0).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := p.client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	pipe := p.client.Pipeline()
	for _, pair := range pairs {
		pipe.SAdd(ctx, friendshipKey(pair.UserID), pair.FriendUserID)
	}
	_, err := pipe.Exec(ctx)
	return err
}
