package messaging

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeReadPersistenceStore struct {
	mu       sync.Mutex
	calls    []readStreamEvent
	failures int
	err      error
}

func (f *fakeReadPersistenceStore) UpsertConversationRead(
	_ context.Context,
	userID, peerID, lastReadMessageID uint64,
	updatedAt time.Time,
) error {
	f.mu.Lock()
	f.calls = append(f.calls, readStreamEvent{
		UserID:            userID,
		PeerID:            peerID,
		LastReadMessageID: lastReadMessageID,
		UpdatedAt:         updatedAt,
	})
	fail := f.failures > 0
	if fail {
		f.failures--
	}
	err := f.err
	f.mu.Unlock()
	if fail {
		return errors.New("mysql unavailable")
	}
	return err
}

func (f *fakeReadPersistenceStore) snapshot() []readStreamEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]readStreamEvent, len(f.calls))
	copy(result, f.calls)
	return result
}

func newReadPersisterTest(
	t *testing.T,
	store DirectReadPersistenceStore,
) (*DirectReadPersister, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	persister := NewDirectReadPersister(
		client,
		defaultKeyspace,
		DefaultNamespace(),
		store,
	)
	return persister, client
}

func addReadEvent(
	t *testing.T,
	client *redis.Client,
	values map[string]interface{},
) string {
	t.Helper()
	id, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: directReadStreamKey(),
		Values: values,
	}).Result()
	if err != nil {
		t.Fatalf("add read event: %v", err)
	}
	return id
}

func readNewEvents(
	t *testing.T,
	client *redis.Client,
	persister *DirectReadPersister,
) []redis.XStream {
	t.Helper()
	streams, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    persister.group,
		Consumer: persister.consumer,
		Streams:  []string{directReadStreamKey(), ">"},
		Count:    10,
	}).Result()
	if err != nil {
		t.Fatalf("read group: %v", err)
	}
	return streams
}

func TestDirectReadPersisterProcessesAndAcknowledges(t *testing.T) {
	store := &fakeReadPersistenceStore{}
	persister, client := newReadPersisterTest(t, store)
	ctx := context.Background()
	if err := persister.ensureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	addReadEvent(t, client, map[string]interface{}{
		"user_id":              "2",
		"peer_id":              "1",
		"last_read_message_id": "42",
		"updated_at_ms":        "1700000000123",
	})
	if err := persister.processStreams(ctx, readNewEvents(t, client, persister)); err != nil {
		t.Fatalf("process streams: %v", err)
	}

	calls := store.snapshot()
	if len(calls) != 1 {
		t.Fatalf("store calls = %d, want 1", len(calls))
	}
	if calls[0].UserID != 2 || calls[0].PeerID != 1 || calls[0].LastReadMessageID != 42 {
		t.Fatalf("store call = %#v", calls[0])
	}
	if got := calls[0].UpdatedAt; !got.Equal(time.UnixMilli(1700000000123).UTC()) {
		t.Fatalf("updated at = %v", got)
	}
	if got := client.XLen(ctx, directReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
}

func TestDirectReadPersisterRetriesPendingEvent(t *testing.T) {
	store := &fakeReadPersistenceStore{failures: 1}
	persister, client := newReadPersisterTest(t, store)
	ctx := context.Background()
	if err := persister.ensureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	addReadEvent(t, client, map[string]interface{}{
		"user_id":              "2",
		"peer_id":              "1",
		"last_read_message_id": "42",
		"updated_at_ms":        "1700000000123",
	})
	if err := persister.processStreams(ctx, readNewEvents(t, client, persister)); err == nil {
		t.Fatalf("first process error = nil, want failure")
	}
	if got := client.XLen(ctx, directReadStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after failure = %d, want 1", got)
	}

	if err := persister.drainPending(ctx); err != nil {
		t.Fatalf("drain pending: %v", err)
	}
	if calls := store.snapshot(); len(calls) != 2 {
		t.Fatalf("store calls = %d, want 2", len(calls))
	}
	if got := client.XLen(ctx, directReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length after retry = %d, want 0", got)
	}
}

func TestDirectReadPersisterDropsMalformedEvent(t *testing.T) {
	store := &fakeReadPersistenceStore{}
	persister, client := newReadPersisterTest(t, store)
	ctx := context.Background()
	if err := persister.ensureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	addReadEvent(t, client, map[string]interface{}{
		"user_id": "2",
		"peer_id": "1",
	})
	if err := persister.processStreams(ctx, readNewEvents(t, client, persister)); err != nil {
		t.Fatalf("process malformed event: %v", err)
	}
	if calls := store.snapshot(); len(calls) != 0 {
		t.Fatalf("store calls = %d, want 0", len(calls))
	}
	if got := client.XLen(ctx, directReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
}

func TestParseReadStreamEvent(t *testing.T) {
	event, err := parseReadStreamEvent(map[string]interface{}{
		"user_id":              int64(2),
		"peer_id":              "1",
		"last_read_message_id": "42",
		"updated_at_ms":        "1700000000123",
	})
	if err != nil {
		t.Fatalf("parse event: %v", err)
	}
	if event.UserID != 2 || event.PeerID != 1 ||
		event.LastReadMessageID != 42 ||
		!event.UpdatedAt.Equal(time.UnixMilli(1700000000123).UTC()) {
		t.Fatalf("event = %#v", event)
	}

	for name, values := range map[string]map[string]interface{}{
		"missing user": {
			"peer_id":              "1",
			"last_read_message_id": "42",
			"updated_at_ms":        "1700000000123",
		},
		"self": {
			"user_id":              "1",
			"peer_id":              "1",
			"last_read_message_id": "42",
			"updated_at_ms":        "1700000000123",
		},
		"zero position": {
			"user_id":              "2",
			"peer_id":              "1",
			"last_read_message_id": "0",
			"updated_at_ms":        "1700000000123",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseReadStreamEvent(values); err == nil {
				t.Fatalf("parse error = nil, want failure")
			}
		})
	}
}

func TestDirectReadPersisterRequiresDependencies(t *testing.T) {
	persister := NewDirectReadPersister(nil, defaultKeyspace, DefaultNamespace(), nil)
	if err := persister.Run(context.Background()); err == nil {
		t.Fatalf("Run() error = nil, want service unavailable")
	}
}
