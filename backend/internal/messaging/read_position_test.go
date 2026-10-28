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

func newDirectReadTestClient(t *testing.T) *redis.Client {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newReadTracker(t *testing.T, client *redis.Client) *DirectReadTracker {
	t.Helper()
	return NewDirectReadTracker(client, DirectReadTrackerOptions{StreamMaxLen: 100})
}

func TestDirectReadTrackerAdvancesMonotonically(t *testing.T) {
	client := newDirectReadTestClient(t)
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()

	setDirectSequence(t, client, 2, 1, 30)
	tracker := newReadTracker(t, client)

	first, err := tracker.AdvanceRead(ctx, 2, 1, 20, now)
	if err != nil {
		t.Fatalf("first advance: %v", err)
	}
	if !first.Advanced || first.LastReadSeq != 20 || first.Candidate != 20 {
		t.Fatalf("first advance = %#v, want position 20 advanced", first)
	}
	if got := client.HGet(ctx, directReadKey(2), "1").Val(); got != "20" {
		t.Fatalf("stored position = %q, want 20", got)
	}
	if got := client.XLen(ctx, directReadStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length = %d, want 1", got)
	}

	backwards, err := tracker.AdvanceRead(ctx, 2, 1, 15, now.Add(time.Second))
	if err != nil {
		t.Fatalf("backwards advance: %v", err)
	}
	if backwards.Advanced || backwards.LastReadSeq != 20 || backwards.Candidate != 15 {
		t.Fatalf("backwards advance = %#v, want unchanged position 20", backwards)
	}
	if got := client.XLen(ctx, directReadStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after no-op = %d, want 1", got)
	}

	forward, err := tracker.AdvanceRead(ctx, 2, 1, 999, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("forward advance: %v", err)
	}
	if !forward.Advanced || forward.LastReadSeq != 30 || forward.Candidate != 30 {
		t.Fatalf("forward advance = %#v, want position 30 clamped to the counter", forward)
	}

	unbounded, err := tracker.AdvanceRead(ctx, 2, 1, 0, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("unbounded advance: %v", err)
	}
	if unbounded.Advanced || unbounded.LastReadSeq != 30 || unbounded.Candidate != 30 {
		t.Fatalf("unbounded advance = %#v, want unchanged position 30", unbounded)
	}

	positions, err := tracker.ReadPositions(ctx, 2)
	if err != nil {
		t.Fatalf("read positions: %v", err)
	}
	if positions[1] != 30 {
		t.Fatalf("read positions = %#v, want peer 1 at 30", positions)
	}

	events, err := client.XRange(ctx, directReadStreamKey(), "-", "+").Result()
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("stream events = %d, want 2", len(events))
	}
	values := events[0].Values
	if values["user_id"] != "2" || values["peer_id"] != "1" ||
		values["last_read_seq"] != "20" || values["updated_at_ms"] != "1700000000000" {
		t.Fatalf("stream event = %#v", values)
	}
	if _, ok := values["last_read_message_id"]; ok {
		t.Fatalf("stream event still uses last_read_message_id: %#v", values)
	}
}

func TestDirectReadTrackerWithoutCounterDoesNotAdvance(t *testing.T) {
	client := newDirectReadTestClient(t)
	ctx := context.Background()
	tracker := newReadTracker(t, client)

	result, err := tracker.AdvanceRead(ctx, 2, 1, 50, time.Now())
	if err != nil {
		t.Fatalf("advance without a counter: %v", err)
	}
	if result.Advanced || result.LastReadSeq != 0 || result.Candidate != 0 {
		t.Fatalf("advance = %#v, want no change", result)
	}
	if got := client.Exists(ctx, directReadKey(2)).Val(); got != 0 {
		t.Fatalf("read key exists = %d, want 0", got)
	}
	if got := client.XLen(ctx, directReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
}

func TestDirectReadTrackerClampsToCounter(t *testing.T) {
	client := newDirectReadTestClient(t)
	ctx := context.Background()

	setDirectSequence(t, client, 2, 1, 10)
	tracker := newReadTracker(t, client)

	result, err := tracker.AdvanceRead(ctx, 2, 1, 999, time.Now())
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.LastReadSeq != 10 || !result.Advanced || result.Candidate != 10 {
		t.Fatalf("advance = %#v, want position 10", result)
	}

	below, err := tracker.AdvanceRead(ctx, 2, 1, 5, time.Now())
	if err != nil {
		t.Fatalf("advance below: %v", err)
	}
	if below.Advanced || below.LastReadSeq != 10 || below.Candidate != 5 {
		t.Fatalf("advance below = %#v, want unchanged position 10", below)
	}
}

func TestDirectReadTrackerDerivesUnreadAndPeerPositions(t *testing.T) {
	client := newDirectReadTestClient(t)
	ctx := context.Background()

	setDirectSequence(t, client, 2, 1, 40)
	tracker := newReadTracker(t, client)
	if _, err := tracker.AdvanceRead(ctx, 2, 1, 25, time.Now()); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := client.HSet(ctx, directReadKey(1), "2", "35").Err(); err != nil {
		t.Fatalf("seed peer read: %v", err)
	}

	positions, err := tracker.ReadPositions(ctx, 2)
	if err != nil {
		t.Fatalf("read positions: %v", err)
	}
	unread, err := tracker.UnreadCounts(ctx, 2, []uint64{1, 3}, positions)
	if err != nil {
		t.Fatalf("unread counts: %v", err)
	}
	if unread[1] != 15 {
		t.Fatalf("unread for peer 1 = %d, want 40-25=15", unread[1])
	}
	if unread[3] != 0 {
		t.Fatalf("unread for peer 3 = %d, want 0", unread[3])
	}

	clamped, err := tracker.UnreadCounts(ctx, 2, []uint64{1}, map[uint64]uint64{1: 100})
	if err != nil {
		t.Fatalf("clamped unread: %v", err)
	}
	if clamped[1] != 0 {
		t.Fatalf("clamped unread = %d, want 0", clamped[1])
	}

	peerPositions, err := tracker.PeerReadPositions(ctx, 2, []uint64{1, 3})
	if err != nil {
		t.Fatalf("peer read positions: %v", err)
	}
	if peerPositions[1] != 35 {
		t.Fatalf("peer 1 position = %d, want 35", peerPositions[1])
	}
	if _, ok := peerPositions[3]; ok {
		t.Fatalf("peer 3 position = %#v, want absent", peerPositions)
	}
}

func TestDirectReadTrackerStates(t *testing.T) {
	client := newDirectReadTestClient(t)
	ctx := context.Background()

	setDirectSequence(t, client, 2, 1, 40)
	setDirectSequence(t, client, 2, 3, 8)
	if err := client.HSet(ctx, directReadKey(2), "1", "25").Err(); err != nil {
		t.Fatalf("seed read position: %v", err)
	}
	if err := client.HSet(ctx, directReadKey(1), "2", "35").Err(); err != nil {
		t.Fatalf("seed peer read position: %v", err)
	}
	tracker := newReadTracker(t, client)

	states, err := tracker.States(ctx, 2, []uint64{1, 3, 4})
	if err != nil {
		t.Fatalf("States() error = %v", err)
	}
	peer := states[1]
	if peer.LastSeq != 40 || peer.ReadSeq != 25 || peer.PeerReadSeq != 35 || peer.Unread != 15 {
		t.Fatalf("peer state = %#v", peer)
	}
	other := states[3]
	if other.LastSeq != 8 || other.ReadSeq != 0 || other.Unread != 8 {
		t.Fatalf("other state = %#v", other)
	}
	missing := states[4]
	if missing.LastSeq != 0 || missing.Unread != 0 {
		t.Fatalf("missing state = %#v", missing)
	}
}

func TestDirectReadTrackerConcurrentAdvancesStayMonotonic(t *testing.T) {
	client := newDirectReadTestClient(t)
	ctx := context.Background()

	setDirectSequence(t, client, 2, 1, 100)
	tracker := newReadTracker(t, client)

	var wait sync.WaitGroup
	failures := make(chan error, 10)
	for index := uint64(1); index <= 10; index++ {
		wait.Add(1)
		go func(through uint64) {
			defer wait.Done()
			if _, err := tracker.AdvanceRead(ctx, 2, 1, through*10, time.Now()); err != nil {
				failures <- err
			}
		}(index)
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Fatalf("concurrent advance: %v", err)
	}

	final, err := tracker.AdvanceRead(ctx, 2, 1, 0, time.Now())
	if err != nil {
		t.Fatalf("final advance: %v", err)
	}
	if final.LastReadSeq != 100 || final.Advanced {
		t.Fatalf("final advance = %#v, want position 100 already applied", final)
	}
	streamLength := client.XLen(ctx, directReadStreamKey()).Val()
	if streamLength < 1 || streamLength > 10 {
		t.Fatalf("stream length = %d, want between 1 and 10", streamLength)
	}
}

func TestDirectReadTrackerReadModelReady(t *testing.T) {
	client := newDirectReadTestClient(t)
	ctx := context.Background()
	gate := NewSeqGate()
	tracker := NewDirectReadTracker(client, DirectReadTrackerOptions{Gate: gate})

	ready, err := tracker.ReadModelReady(ctx)
	if err != nil {
		t.Fatalf("initial ready: %v", err)
	}
	if ready {
		t.Fatal("initial ready = true, want false without seq:ready")
	}
	if err := tracker.MarkReadModelReady(ctx); err != nil {
		t.Fatalf("mark ready: %v", err)
	}
	ready, err = tracker.ReadModelReady(ctx)
	if err != nil || !ready {
		t.Fatalf("ready after mark = %v, %v; want true", ready, err)
	}

	gate.Close()
	ready, err = tracker.ReadModelReady(ctx)
	if err != nil || ready {
		t.Fatalf("ready with closed gate = %v, %v; want false", ready, err)
	}
}

func TestDirectReadTrackerGate(t *testing.T) {
	client := newDirectReadTestClient(t)
	ctx := context.Background()
	gate := NewSeqGate()
	tracker := NewDirectReadTracker(client, DirectReadTrackerOptions{Gate: gate})
	gate.Close()

	if _, err := tracker.AdvanceRead(ctx, 2, 1, 0, time.Now()); !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("AdvanceRead() error = %v, want ErrSeqNotReady", err)
	}
	if _, err := tracker.States(ctx, 2, []uint64{1}); !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("States() error = %v, want ErrSeqNotReady", err)
	}
}

func TestDirectReadTrackerRequiresRedis(t *testing.T) {
	tracker := NewDirectReadTracker(nil, DirectReadTrackerOptions{})
	ctx := context.Background()
	if _, err := tracker.AdvanceRead(ctx, 1, 2, 10, time.Now()); err == nil {
		t.Fatalf("AdvanceRead() error = nil, want service unavailable")
	}
	if _, err := tracker.ReadPositions(ctx, 1); err == nil {
		t.Fatalf("ReadPositions() error = nil, want service unavailable")
	}
	if _, err := tracker.UnreadCounts(ctx, 1, []uint64{2}, nil); err == nil {
		t.Fatalf("UnreadCounts() error = nil, want service unavailable")
	}
	if _, err := tracker.States(ctx, 1, []uint64{2}); err == nil {
		t.Fatalf("States() error = nil, want service unavailable")
	}
}
