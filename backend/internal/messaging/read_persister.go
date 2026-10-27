package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/go-sql-driver/mysql"
)

// DirectReadPersistenceStore persists read positions to durable storage.
type DirectReadPersistenceStore interface {
	UpsertConversationRead(
		ctx context.Context,
		userID, peerID, lastReadMessageID uint64,
		updatedAt time.Time,
	) error
}

// DirectReadPersister consumes dm:read:stream and writes read positions to
// MySQL. Redis remains authoritative for the online read path.
type DirectReadPersister struct {
	client   redis.UniversalClient
	keyspace Keyspace
	store    DirectReadPersistenceStore
	group    string
	consumer string
}

type readStreamEvent struct {
	UserID            uint64
	PeerID            uint64
	LastReadMessageID uint64
	UpdatedAt         time.Time
}

func NewDirectReadPersister(
	client redis.UniversalClient,
	keyspace Keyspace,
	namespace Namespace,
	dataStore DirectReadPersistenceStore,
) *DirectReadPersister {
	return &DirectReadPersister{
		client:   client,
		keyspace: keyspace,
		store:    dataStore,
		group:    directReadGroupName(namespace),
		consumer: directReadWorkerName(namespace),
	}
}

func directReadGroupName(namespace Namespace) string {
	return namespace.ConsumerPrefix() + "-read-persist"
}

func directReadWorkerName(namespace Namespace) string {
	return namespace.ConsumerPrefix() + "-read-worker"
}

// Run consumes read-position events until the context is cancelled.
func (p *DirectReadPersister) Run(ctx context.Context) error {
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
			log.Printf("persist pending direct reads: %v", err)
			if !waitContext(ctx, time.Second) {
				return ctx.Err()
			}
			continue
		}

		streams, err := p.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    p.group,
			Consumer: p.consumer,
			Streams:  []string{p.keyspace.directReadStreamKey(), ">"},
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
			log.Printf("read direct read stream: %v", err)
			if !waitContext(ctx, time.Second) {
				return ctx.Err()
			}
			continue
		}
		if err := p.processStreams(ctx, streams); err != nil {
			log.Printf("persist direct read: %v", err)
			if !waitContext(ctx, time.Second) {
				return ctx.Err()
			}
		}
	}
}

func (p *DirectReadPersister) ensureGroup(ctx context.Context) error {
	err := p.client.XGroupCreateMkStream(
		ctx,
		p.keyspace.directReadStreamKey(),
		p.group,
		"0-0",
	).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create direct read group: %w", err)
	}
	return nil
}

// drainPending retries messages already delivered to this consumer but not
// acknowledged, for example after a MySQL outage or a process restart.
func (p *DirectReadPersister) drainPending(ctx context.Context) error {
	streams, err := p.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    p.group,
		Consumer: p.consumer,
		Streams:  []string{p.keyspace.directReadStreamKey(), "0"},
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

func (p *DirectReadPersister) processStreams(
	ctx context.Context,
	streams []redis.XStream,
) error {
	for _, stream := range streams {
		for _, entry := range stream.Messages {
			if err := p.processEntry(ctx, entry); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *DirectReadPersister) processEntry(ctx context.Context, entry redis.XMessage) error {
	event, err := parseReadStreamEvent(entry.Values)
	if err != nil {
		log.Printf("drop malformed direct read event %s: %v", entry.ID, err)
		return p.ackAndDelete(ctx, entry.ID)
	}
	if err := p.store.UpsertConversationRead(
		ctx,
		event.UserID,
		event.PeerID,
		event.LastReadMessageID,
		event.UpdatedAt,
	); err != nil {
		if isPermanentReadPersistError(err) {
			log.Printf("drop permanent direct read event %s: %v", entry.ID, err)
			return p.ackAndDelete(ctx, entry.ID)
		}
		return fmt.Errorf("persist direct read %s: %w", entry.ID, err)
	}
	return p.ackAndDelete(ctx, entry.ID)
}

// A read event can reference a user that no longer exists after test cleanup
// or account removal. Retrying cannot succeed, so acknowledge and drop it.
func isPermanentReadPersistError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	case 1452, 3819:
		return true
	default:
		return false
	}
}

func (p *DirectReadPersister) ackAndDelete(ctx context.Context, entryID string) error {
	if err := p.client.XAck(
		ctx,
		p.keyspace.directReadStreamKey(),
		p.group,
		entryID,
	).Err(); err != nil {
		return fmt.Errorf("ack direct read event %s: %w", entryID, err)
	}
	if err := p.client.XDel(
		ctx,
		p.keyspace.directReadStreamKey(),
		entryID,
	).Err(); err != nil {
		log.Printf("delete direct read event %s: %v", entryID, err)
	}
	return nil
}

func parseReadStreamEvent(values map[string]interface{}) (readStreamEvent, error) {
	userID, err := parseRedisUint(values["user_id"])
	if err != nil {
		return readStreamEvent{}, fmt.Errorf("user_id: %w", err)
	}
	peerID, err := parseRedisUint(values["peer_id"])
	if err != nil {
		return readStreamEvent{}, fmt.Errorf("peer_id: %w", err)
	}
	lastReadMessageID, err := parseRedisUint(values["last_read_message_id"])
	if err != nil {
		return readStreamEvent{}, fmt.Errorf("last_read_message_id: %w", err)
	}
	updatedAtMS, err := parseRedisInt64(values["updated_at_ms"])
	if err != nil {
		return readStreamEvent{}, fmt.Errorf("updated_at_ms: %w", err)
	}
	if userID == 0 || peerID == 0 || userID == peerID || lastReadMessageID == 0 {
		return readStreamEvent{}, fmt.Errorf("invalid read event identity")
	}
	return readStreamEvent{
		UserID:            userID,
		PeerID:            peerID,
		LastReadMessageID: lastReadMessageID,
		UpdatedAt:         time.UnixMilli(updatedAtMS).UTC(),
	}, nil
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
