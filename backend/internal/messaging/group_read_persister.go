package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// GroupReadStore persists group read positions to durable storage.
type GroupReadStore interface {
	UpsertGroupRead(
		ctx context.Context,
		groupID, userID, lastReadSeq uint64,
	) (bool, error)
}

// GroupReadPersister consumes group-read:stream and writes read positions to
// MySQL. Redis remains authoritative for the online read path.
type GroupReadPersister struct {
	client   redis.UniversalClient
	keyspace Keyspace
	store    GroupReadStore
	group    string
	consumer string
}

type groupReadStreamEvent struct {
	GroupID     uint64
	UserID      uint64
	LastReadSeq uint64
}

type groupReadEventKey struct {
	groupID uint64
	userID  uint64
}

type groupReadEventBatch struct {
	key      groupReadEventKey
	position uint64
	entries  []string
}

func NewGroupReadPersister(
	client redis.UniversalClient,
	keyspace Keyspace,
	namespace Namespace,
	dataStore GroupReadStore,
) *GroupReadPersister {
	return &GroupReadPersister{
		client:   client,
		keyspace: keyspace,
		store:    dataStore,
		group:    groupReadPersistGroupName(namespace),
		consumer: groupReadPersistWorkerName(namespace),
	}
}

func groupReadPersistGroupName(namespace Namespace) string {
	return namespace.ConsumerPrefix() + "-group-read-persist"
}

func groupReadPersistWorkerName(namespace Namespace) string {
	return namespace.ConsumerPrefix() + "-group-read-worker"
}

// Run consumes group read-position events until the context is cancelled.
func (p *GroupReadPersister) Run(ctx context.Context) error {
	if p == nil || p.client == nil || p.store == nil {
		return ErrMessageServiceUnavailable
	}
	if err := p.ensureGroup(ctx); err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.drainPending(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("persist pending group reads: %v", err)
			if !waitContext(ctx, time.Second) {
				return ctx.Err()
			}
			continue
		}

		streams, err := p.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    p.group,
			Consumer: p.consumer,
			Streams:  []string{p.keyspace.groupReadStreamKey(), ">"},
			Count:    100,
			Block:    2 * time.Second,
		}).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("read group read stream: %v", err)
			if !waitContext(ctx, time.Second) {
				return ctx.Err()
			}
			continue
		}
		if err := p.processStreams(ctx, streams); err != nil {
			log.Printf("persist group read: %v", err)
			if !waitContext(ctx, time.Second) {
				return ctx.Err()
			}
		}
	}
}

func (p *GroupReadPersister) ensureGroup(ctx context.Context) error {
	err := p.client.XGroupCreateMkStream(
		ctx,
		p.keyspace.groupReadStreamKey(),
		p.group,
		"0-0",
	).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create group read group: %w", err)
	}
	return nil
}

// drainPending retries entries already delivered to this consumer but not
// acknowledged, for example after a MySQL outage or a process restart.
func (p *GroupReadPersister) drainPending(ctx context.Context) error {
	streams, err := p.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    p.group,
		Consumer: p.consumer,
		Streams:  []string{p.keyspace.groupReadStreamKey(), "0"},
		Count:    100,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		return err
	}
	return p.processStreams(ctx, streams)
}

// processStreams coalesces a batch by (group, user) and writes each key once.
// Entries of a key with a transient store failure stay unacknowledged; all
// other keys in the batch still make progress.
func (p *GroupReadPersister) processStreams(
	ctx context.Context,
	streams []redis.XStream,
) error {
	batches := make(map[groupReadEventKey]*groupReadEventBatch)
	order := make([]groupReadEventKey, 0)
	malformed := make([]string, 0)
	for _, stream := range streams {
		for _, entry := range stream.Messages {
			event, err := parseGroupReadStreamEvent(entry.Values)
			if err != nil {
				log.Printf("drop malformed group read event %s: %v", entry.ID, err)
				malformed = append(malformed, entry.ID)
				continue
			}
			key := groupReadEventKey{groupID: event.GroupID, userID: event.UserID}
			batch, ok := batches[key]
			if !ok {
				batch = &groupReadEventBatch{key: key}
				batches[key] = batch
				order = append(order, key)
			}
			if event.LastReadSeq > batch.position {
				batch.position = event.LastReadSeq
			}
			batch.entries = append(batch.entries, entry.ID)
		}
	}

	var firstErr error
	for _, entryID := range malformed {
		if err := p.ackAndDelete(ctx, entryID); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, key := range order {
		batch := batches[key]
		// A removed member has no row to update, so the bool needs no action.
		if _, err := p.store.UpsertGroupRead(
			ctx, key.groupID, key.userID, batch.position,
		); err != nil {
			if isPermanentReadPersistError(err) {
				log.Printf(
					"drop permanent group read event group=%d user=%d: %v",
					key.groupID, key.userID, err,
				)
				for _, entryID := range batch.entries {
					if ackErr := p.ackAndDelete(ctx, entryID); ackErr != nil && firstErr == nil {
						firstErr = ackErr
					}
				}
				continue
			}
			if firstErr == nil {
				firstErr = fmt.Errorf(
					"persist group read group=%d user=%d: %w",
					key.groupID, key.userID, err,
				)
			}
			continue
		}
		for _, entryID := range batch.entries {
			if err := p.ackAndDelete(ctx, entryID); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				break
			}
		}
	}
	return firstErr
}

func (p *GroupReadPersister) ackAndDelete(ctx context.Context, entryID string) error {
	if err := p.client.XAck(
		ctx,
		p.keyspace.groupReadStreamKey(),
		p.group,
		entryID,
	).Err(); err != nil {
		return fmt.Errorf("ack group read event %s: %w", entryID, err)
	}
	if err := p.client.XDel(
		ctx,
		p.keyspace.groupReadStreamKey(),
		entryID,
	).Err(); err != nil {
		log.Printf("delete group read event %s: %v", entryID, err)
	}
	return nil
}

func parseGroupReadStreamEvent(values map[string]interface{}) (groupReadStreamEvent, error) {
	groupID, err := parseRedisUint(values["group_id"])
	if err != nil {
		return groupReadStreamEvent{}, fmt.Errorf("group_id: %w", err)
	}
	userID, err := parseRedisUint(values["user_id"])
	if err != nil {
		return groupReadStreamEvent{}, fmt.Errorf("user_id: %w", err)
	}
	lastReadSeq, err := parseRedisUint(values["last_read_seq"])
	if err != nil {
		return groupReadStreamEvent{}, fmt.Errorf("last_read_seq: %w", err)
	}
	if _, err := parseRedisInt64(values["updated_at_ms"]); err != nil {
		return groupReadStreamEvent{}, fmt.Errorf("updated_at_ms: %w", err)
	}
	if groupID == 0 || userID == 0 || lastReadSeq == 0 {
		return groupReadStreamEvent{}, fmt.Errorf("invalid group read event identity")
	}
	return groupReadStreamEvent{
		GroupID:     groupID,
		UserID:      userID,
		LastReadSeq: lastReadSeq,
	}, nil
}
