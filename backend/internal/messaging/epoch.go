package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// EnsureIndexEpoch returns the highest message ID that the Redis read indexes
// are not guaranteed to contain. Only IDs above the returned value may be
// served from Redis without consulting MySQL.
//
// The value is seeded from the durable maximum the first time an index is
// built, and kept across restarts so already indexed history stays readable.
// If the Redis message counter is behind the durable maximum, some messages
// were written without the index (for example through the MySQL fallback), so
// the epoch is raised to the durable maximum.
func EnsureIndexEpoch(
	ctx context.Context,
	client redis.UniversalClient,
	keyspace Keyspace,
	durableMax uint64,
) (uint64, error) {
	if client == nil {
		return 0, ErrMessageServiceUnavailable
	}
	key := keyspace.indexEpochKey()

	raw, err := client.Get(ctx, key).Result()
	switch {
	case errors.Is(err, redis.Nil):
		counter, err := readMessageCounter(ctx, client, keyspace)
		if err != nil {
			return 0, err
		}
		// Messages allocated by an older version, or accepted but not yet
		// persisted, are above the durable maximum and were never written to
		// the new indexes. Seed above them so the first reads fall back.
		seed := durableMax
		if counter > seed {
			seed = counter
		}
		if err := client.Set(ctx, key, seed, 0).Err(); err != nil {
			return 0, fmt.Errorf("seed index epoch: %w", err)
		}
		return seed, nil
	case err != nil:
		return 0, fmt.Errorf("read index epoch: %w", err)
	}

	epoch, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse index epoch: %w", err)
	}

	counter, err := readMessageCounter(ctx, client, keyspace)
	if err != nil {
		return 0, err
	}
	if counter < durableMax && durableMax > epoch {
		if err := client.Set(ctx, key, durableMax, 0).Err(); err != nil {
			return 0, fmt.Errorf("raise index epoch: %w", err)
		}
		return durableMax, nil
	}
	return epoch, nil
}

func readMessageCounter(
	ctx context.Context,
	client redis.UniversalClient,
	keyspace Keyspace,
) (uint64, error) {
	raw, err := client.Get(ctx, keyspace.counterKey()).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read message counter: %w", err)
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse message counter: %w", err)
	}
	return value, nil
}
