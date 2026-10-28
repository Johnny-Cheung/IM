package messaging

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

type groupReadStoreCall struct {
	GroupID           uint64
	UserID            uint64
	LastReadMessageID uint64
}

type fakeGroupReadStore struct {
	mu        sync.Mutex
	calls     []groupReadStoreCall
	failures  map[uint64]int
	permanent error
	gone      map[uint64]bool
}

func (f *fakeGroupReadStore) UpsertGroupRead(
	_ context.Context,
	groupID, userID, lastReadMessageID uint64,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, groupReadStoreCall{
		GroupID:           groupID,
		UserID:            userID,
		LastReadMessageID: lastReadMessageID,
	})
	if f.failures[groupID] > 0 {
		f.failures[groupID]--
		return false, errors.New("mysql unavailable")
	}
	if f.permanent != nil {
		return false, f.permanent
	}
	return !f.gone[groupID], nil
}

func (f *fakeGroupReadStore) snapshot() []groupReadStoreCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]groupReadStoreCall, len(f.calls))
	copy(result, f.calls)
	return result
}

func newGroupReadPersisterTest(
	t *testing.T,
	store GroupReadStore,
) (*GroupReadPersister, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	persister := NewGroupReadPersister(
		client,
		defaultKeyspace,
		DefaultNamespace(),
		store,
	)
	return persister, client
}

func addGroupReadEvent(
	t *testing.T,
	client *redis.Client,
	values map[string]interface{},
) string {
	t.Helper()
	id, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: defaultKeyspace.groupReadStreamKey(),
		Values: values,
	}).Result()
	if err != nil {
		t.Fatalf("add group read event: %v", err)
	}
	return id
}

func readNewGroupReadEvents(
	t *testing.T,
	client *redis.Client,
	persister *GroupReadPersister,
) []redis.XStream {
	t.Helper()
	streams, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    persister.group,
		Consumer: persister.consumer,
		Streams:  []string{defaultKeyspace.groupReadStreamKey(), ">"},
		Count:    100,
	}).Result()
	if err != nil {
		t.Fatalf("read group read stream: %v", err)
	}
	return streams
}

func groupReadEventValues(groupID, userID, messageID string) map[string]interface{} {
	return map[string]interface{}{
		"group_id":             groupID,
		"user_id":              userID,
		"last_read_message_id": messageID,
		"updated_at_ms":        "1700000000123",
	}
}

func TestGroupReadPersisterProcessesAndAcknowledges(t *testing.T) {
	store := &fakeGroupReadStore{}
	persister, client := newGroupReadPersisterTest(t, store)
	ctx := context.Background()
	if err := persister.ensureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	addGroupReadEvent(t, client, groupReadEventValues("9", "2", "42"))
	if err := persister.processStreams(ctx, readNewGroupReadEvents(t, client, persister)); err != nil {
		t.Fatalf("process streams: %v", err)
	}

	calls := store.snapshot()
	if len(calls) != 1 || calls[0] != (groupReadStoreCall{GroupID: 9, UserID: 2, LastReadMessageID: 42}) {
		t.Fatalf("store calls = %#v", calls)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
}

func TestGroupReadPersisterCoalescesByKey(t *testing.T) {
	store := &fakeGroupReadStore{}
	persister, client := newGroupReadPersisterTest(t, store)
	ctx := context.Background()
	if err := persister.ensureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	addGroupReadEvent(t, client, groupReadEventValues("9", "2", "10"))
	addGroupReadEvent(t, client, groupReadEventValues("9", "2", "30"))
	addGroupReadEvent(t, client, groupReadEventValues("9", "2", "20"))
	if err := persister.processStreams(ctx, readNewGroupReadEvents(t, client, persister)); err != nil {
		t.Fatalf("process streams: %v", err)
	}

	calls := store.snapshot()
	if len(calls) != 1 {
		t.Fatalf("store calls = %#v, want one coalesced call", calls)
	}
	if calls[0].LastReadMessageID != 30 {
		t.Fatalf("coalesced position = %d, want 30", calls[0].LastReadMessageID)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
}

func TestGroupReadPersisterRetriesPendingEvent(t *testing.T) {
	store := &fakeGroupReadStore{failures: map[uint64]int{9: 1}}
	persister, client := newGroupReadPersisterTest(t, store)
	ctx := context.Background()
	if err := persister.ensureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	addGroupReadEvent(t, client, groupReadEventValues("9", "2", "42"))
	if err := persister.processStreams(ctx, readNewGroupReadEvents(t, client, persister)); err == nil {
		t.Fatalf("first process error = nil, want failure")
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after failure = %d, want 1", got)
	}

	if err := persister.drainPending(ctx); err != nil {
		t.Fatalf("drain pending: %v", err)
	}
	if calls := store.snapshot(); len(calls) != 2 {
		t.Fatalf("store calls = %#v, want 2 attempts", calls)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length after retry = %d, want 0", got)
	}
}

func TestGroupReadPersisterMixedSuccessAndFailure(t *testing.T) {
	store := &fakeGroupReadStore{failures: map[uint64]int{9: 1}}
	persister, client := newGroupReadPersisterTest(t, store)
	ctx := context.Background()
	if err := persister.ensureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	addGroupReadEvent(t, client, groupReadEventValues("9", "2", "42"))
	addGroupReadEvent(t, client, groupReadEventValues("10", "3", "43"))
	if err := persister.processStreams(ctx, readNewGroupReadEvents(t, client, persister)); err == nil {
		t.Fatalf("process error = nil, want failure")
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length = %d, want only the failed key pending", got)
	}

	if err := persister.drainPending(ctx); err != nil {
		t.Fatalf("drain pending: %v", err)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length after retry = %d, want 0", got)
	}
	if calls := store.snapshot(); len(calls) != 3 {
		t.Fatalf("store calls = %#v, want 3 attempts", calls)
	}
}

func TestGroupReadPersisterDropsMalformedEvent(t *testing.T) {
	store := &fakeGroupReadStore{}
	persister, client := newGroupReadPersisterTest(t, store)
	ctx := context.Background()
	if err := persister.ensureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	addGroupReadEvent(t, client, map[string]interface{}{
		"group_id": "9",
		"user_id":  "2",
	})
	if err := persister.processStreams(ctx, readNewGroupReadEvents(t, client, persister)); err != nil {
		t.Fatalf("process malformed event: %v", err)
	}
	if calls := store.snapshot(); len(calls) != 0 {
		t.Fatalf("store calls = %#v, want none", calls)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
}

func TestGroupReadPersisterDropsPermanentFailure(t *testing.T) {
	store := &fakeGroupReadStore{
		permanent: &mysql.MySQLError{Number: 1452, Message: "foreign key"},
	}
	persister, client := newGroupReadPersisterTest(t, store)
	ctx := context.Background()
	if err := persister.ensureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	addGroupReadEvent(t, client, groupReadEventValues("9", "2", "42"))
	if err := persister.processStreams(ctx, readNewGroupReadEvents(t, client, persister)); err != nil {
		t.Fatalf("process permanent event: %v", err)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
}

func TestGroupReadPersisterAcknowledgesRemovedMember(t *testing.T) {
	store := &fakeGroupReadStore{gone: map[uint64]bool{9: true}}
	persister, client := newGroupReadPersisterTest(t, store)
	ctx := context.Background()
	if err := persister.ensureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	addGroupReadEvent(t, client, groupReadEventValues("9", "2", "42"))
	if err := persister.processStreams(ctx, readNewGroupReadEvents(t, client, persister)); err != nil {
		t.Fatalf("process removed member event: %v", err)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
}

func TestParseGroupReadStreamEvent(t *testing.T) {
	event, err := parseGroupReadStreamEvent(map[string]interface{}{
		"group_id":             int64(9),
		"user_id":              "2",
		"last_read_message_id": "42",
		"updated_at_ms":        "1700000000123",
	})
	if err != nil {
		t.Fatalf("parse event: %v", err)
	}
	if event.GroupID != 9 || event.UserID != 2 || event.LastReadMessageID != 42 {
		t.Fatalf("event = %#v", event)
	}

	for name, values := range map[string]map[string]interface{}{
		"missing group": {
			"user_id":              "2",
			"last_read_message_id": "42",
			"updated_at_ms":        "1700000000123",
		},
		"zero user": {
			"group_id":             "9",
			"user_id":              "0",
			"last_read_message_id": "42",
			"updated_at_ms":        "1700000000123",
		},
		"zero position": {
			"group_id":             "9",
			"user_id":              "2",
			"last_read_message_id": "0",
			"updated_at_ms":        "1700000000123",
		},
		"missing timestamp": {
			"group_id":             "9",
			"user_id":              "2",
			"last_read_message_id": "42",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseGroupReadStreamEvent(values); err == nil {
				t.Fatalf("parse error = nil, want failure")
			}
		})
	}
}

func TestGroupReadPersisterRequiresDependencies(t *testing.T) {
	persister := NewGroupReadPersister(nil, defaultKeyspace, DefaultNamespace(), nil)
	if err := persister.Run(context.Background()); err == nil {
		t.Fatalf("Run() error = nil, want service unavailable")
	}
}
