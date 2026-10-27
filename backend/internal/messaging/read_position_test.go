package messaging

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func seedConversationIndex(
	t *testing.T,
	client *redis.Client,
	ownerID, peerID uint64,
	messageIDs ...uint64,
) {
	t.Helper()
	ctx := context.Background()
	for _, messageID := range messageIDs {
		if err := client.ZAdd(
			ctx,
			directConversationKey(ownerID, peerID),
			redis.Z{Score: float64(messageID), Member: strconv.FormatUint(messageID, 10)},
		).Err(); err != nil {
			t.Fatalf("seed conversation message %d: %v", messageID, err)
		}
	}
}

func newReadTracker(t *testing.T, client *redis.Client) *DirectReadTracker {
	t.Helper()
	return NewDirectReadTracker(client, DirectReadTrackerOptions{StreamMaxLen: 100})
}

func TestDirectReadTrackerAdvancesMonotonically(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()

	seedConversationIndex(t, client, 2, 1, 10, 20, 30)
	tracker := newReadTracker(t, client)

	first, err := tracker.AdvanceRead(ctx, 2, 1, 20, now)
	if err != nil {
		t.Fatalf("first advance: %v", err)
	}
	if !first.Advanced || first.LastReadMessageID != 20 || first.Candidate != 20 {
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
	if backwards.Advanced || backwards.LastReadMessageID != 20 || backwards.Candidate != 10 {
		t.Fatalf("backwards advance = %#v, want unchanged position 20", backwards)
	}
	if got := client.XLen(ctx, directReadStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after no-op = %d, want 1", got)
	}

	forward, err := tracker.AdvanceRead(ctx, 2, 1, 999, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("forward advance: %v", err)
	}
	if !forward.Advanced || forward.LastReadMessageID != 30 {
		t.Fatalf("forward advance = %#v, want position 30 advanced", forward)
	}

	unbounded, err := tracker.AdvanceRead(ctx, 2, 1, 0, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("unbounded advance: %v", err)
	}
	if unbounded.Advanced || unbounded.LastReadMessageID != 30 || unbounded.Candidate != 30 {
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
		values["last_read_message_id"] != "20" || values["updated_at_ms"] != "1700000000000" {
		t.Fatalf("stream event = %#v", values)
	}
}

func TestDirectReadTrackerNoIncomingMessage(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	tracker := newReadTracker(t, client)

	result, err := tracker.AdvanceRead(ctx, 2, 1, 50, time.Now())
	if err != nil {
		t.Fatalf("advance without messages: %v", err)
	}
	if result.Advanced || result.LastReadMessageID != 0 || result.Candidate != 0 {
		t.Fatalf("advance = %#v, want no change", result)
	}
	if got := client.Exists(ctx, directReadKey(2)).Val(); got != 0 {
		t.Fatalf("read key exists = %d, want 0", got)
	}
	if got := client.XLen(ctx, directReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
}

func TestDirectReadTrackerClampsToIncomingDirection(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	// Peer 2 sent 25 to user 1; user 2 only has peer 1's message 10.
	seedConversationIndex(t, client, 1, 2, 25)
	seedConversationIndex(t, client, 2, 1, 10)
	tracker := newReadTracker(t, client)

	result, err := tracker.AdvanceRead(ctx, 2, 1, 999, time.Now())
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.LastReadMessageID != 10 || !result.Advanced {
		t.Fatalf("advance = %#v, want position 10", result)
	}

	below, err := tracker.AdvanceRead(ctx, 2, 1, 5, time.Now())
	if err != nil {
		t.Fatalf("advance below all messages: %v", err)
	}
	if below.Advanced || below.LastReadMessageID != 10 || below.Candidate != 0 {
		t.Fatalf("advance below = %#v, want unchanged position 10", below)
	}
}

func TestDirectReadTrackerDerivesUnreadAndPeerPositions(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	seedConversationIndex(t, client, 2, 1, 10, 20, 30, 40)
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
	if unread[1] != 2 {
		t.Fatalf("unread for peer 1 = %d, want 2", unread[1])
	}
	if unread[3] != 0 {
		t.Fatalf("unread for peer 3 = %d, want 0", unread[3])
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

func TestDirectReadTrackerConcurrentAdvancesStayMonotonic(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	ids := make([]uint64, 0, 10)
	for index := uint64(1); index <= 10; index++ {
		ids = append(ids, index*10)
	}
	seedConversationIndex(t, client, 2, 1, ids...)
	tracker := newReadTracker(t, client)

	var wg sync.WaitGroup
	errors := make(chan error, len(ids))
	for _, throughID := range ids {
		wg.Add(1)
		go func(throughID uint64) {
			defer wg.Done()
			if _, err := tracker.AdvanceRead(ctx, 2, 1, throughID, time.Now()); err != nil {
				errors <- err
			}
		}(throughID)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Fatalf("concurrent advance: %v", err)
	}

	final, err := tracker.AdvanceRead(ctx, 2, 1, 0, time.Now())
	if err != nil {
		t.Fatalf("final advance: %v", err)
	}
	if final.LastReadMessageID != 100 || final.Advanced {
		t.Fatalf("final advance = %#v, want position 100 already applied", final)
	}
	streamLength := client.XLen(ctx, directReadStreamKey()).Val()
	if streamLength < 1 || streamLength > 10 {
		t.Fatalf("stream length = %d, want between 1 and 10", streamLength)
	}
}

func TestDirectReadTrackerReadModelReady(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	tracker := newReadTracker(t, client)

	ready, err := tracker.ReadModelReady(ctx)
	if err != nil {
		t.Fatalf("initial ready: %v", err)
	}
	if ready {
		t.Fatalf("initial ready = true, want false")
	}
	if err := tracker.MarkReadModelReady(ctx); err != nil {
		t.Fatalf("mark ready: %v", err)
	}
	ready, err = tracker.ReadModelReady(ctx)
	if err != nil {
		t.Fatalf("ready after mark: %v", err)
	}
	if !ready {
		t.Fatalf("ready after mark = false, want true")
	}
}

func TestDirectReadTrackerRequiresRedis(t *testing.T) {
	tracker := NewDirectReadTracker(nil, DirectReadTrackerOptions{})
	if _, err := tracker.AdvanceRead(context.Background(), 1, 2, 10, time.Now()); err == nil {
		t.Fatalf("AdvanceRead() error = nil, want service unavailable")
	}
	if _, err := tracker.ReadPositions(context.Background(), 1); err == nil {
		t.Fatalf("ReadPositions() error = nil, want service unavailable")
	}
	if _, err := tracker.UnreadCounts(context.Background(), 1, []uint64{2}, nil); err == nil {
		t.Fatalf("UnreadCounts() error = nil, want service unavailable")
	}
}
