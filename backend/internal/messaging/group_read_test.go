package messaging

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newGroupReadTestClient(t *testing.T) *redis.Client {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newGroupReadTracker(t *testing.T, client *redis.Client) *GroupReadTracker {
	t.Helper()
	return NewGroupReadTracker(client, GroupReadTrackerOptions{StreamMaxLen: 100})
}

func seedGroupReadState(
	t *testing.T,
	client *redis.Client,
	groupID uint64,
	memberIDs []uint64,
	messageIDs []uint64,
	positions map[uint64]uint64,
) {
	t.Helper()
	ctx := context.Background()
	if len(memberIDs) > 0 {
		members := make([]interface{}, 0, len(memberIDs))
		for _, memberID := range memberIDs {
			members = append(members, memberID)
		}
		if err := client.SAdd(ctx, defaultKeyspace.groupMembersKey(groupID), members...).Err(); err != nil {
			t.Fatalf("seed members: %v", err)
		}
	}
	for _, messageID := range messageIDs {
		if err := client.ZAdd(ctx, defaultKeyspace.groupInboxKey(groupID), redis.Z{
			Score:  float64(messageID),
			Member: strconv.FormatUint(messageID, 10),
		}).Err(); err != nil {
			t.Fatalf("seed inbox: %v", err)
		}
	}
	for userID, position := range positions {
		if err := client.ZAdd(ctx, defaultKeyspace.groupReadKey(groupID), redis.Z{
			Score:  float64(position),
			Member: strconv.FormatUint(userID, 10),
		}).Err(); err != nil {
			t.Fatalf("seed read position: %v", err)
		}
	}
}

func TestGroupReadTrackerAdvancesMonotonically(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	seedGroupReadState(t, client, 9, []uint64{1, 2, 3}, []uint64{10, 20, 30}, nil)
	tracker := newGroupReadTracker(t, client)

	first, err := tracker.AdvanceRead(ctx, 9, 2, 20, now)
	if err != nil {
		t.Fatalf("first advance: %v", err)
	}
	if !first.Advanced || first.LastReadMessageID != 20 || first.UserID != 2 {
		t.Fatalf("first advance = %#v, want user 2 at 20", first)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "2").Val(); got != 20 {
		t.Fatalf("stored position = %v, want 20", got)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length = %d, want 1", got)
	}

	backwards, err := tracker.AdvanceRead(ctx, 9, 2, 15, now.Add(time.Second))
	if err != nil {
		t.Fatalf("backwards advance: %v", err)
	}
	if backwards.Advanced || backwards.LastReadMessageID != 20 {
		t.Fatalf("backwards advance = %#v, want unchanged position 20", backwards)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after no-op = %d, want 1", got)
	}

	forward, err := tracker.AdvanceRead(ctx, 9, 2, 999, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("forward advance: %v", err)
	}
	if !forward.Advanced || forward.LastReadMessageID != 30 {
		t.Fatalf("forward advance = %#v, want position 30", forward)
	}

	unbounded, err := tracker.AdvanceRead(ctx, 9, 2, 0, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("unbounded advance: %v", err)
	}
	if unbounded.Advanced || unbounded.LastReadMessageID != 30 {
		t.Fatalf("unbounded advance = %#v, want unchanged position 30", unbounded)
	}

	events, err := client.XRange(ctx, defaultKeyspace.groupReadStreamKey(), "-", "+").Result()
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("stream events = %d, want 2", len(events))
	}
	values := events[0].Values
	if values["group_id"] != "9" || values["user_id"] != "2" ||
		values["last_read_message_id"] != "20" || values["updated_at_ms"] != "1700000000000" {
		t.Fatalf("stream event = %#v", values)
	}
}

func TestGroupReadTrackerClampsToInbox(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1}, []uint64{10, 20, 30}, nil)
	tracker := newGroupReadTracker(t, client)

	clamped, err := tracker.AdvanceRead(ctx, 9, 1, 25, time.Now())
	if err != nil {
		t.Fatalf("clamped advance: %v", err)
	}
	if !clamped.Advanced || clamped.LastReadMessageID != 20 {
		t.Fatalf("clamped advance = %#v, want position 20", clamped)
	}

	below, err := tracker.AdvanceRead(ctx, 9, 1, 5, time.Now())
	if err != nil {
		t.Fatalf("below advance: %v", err)
	}
	if below.Advanced || below.LastReadMessageID != 20 {
		t.Fatalf("below advance = %#v, want unchanged position 20", below)
	}
}

func TestGroupReadTrackerNoMessages(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1}, nil, nil)
	tracker := newGroupReadTracker(t, client)

	result, err := tracker.AdvanceRead(ctx, 9, 1, 100, time.Now())
	if err != nil {
		t.Fatalf("advance without messages: %v", err)
	}
	if result.Advanced || result.LastReadMessageID != 0 {
		t.Fatalf("advance = %#v, want no change", result)
	}
	if got := client.Exists(ctx, defaultKeyspace.groupReadKey(9)).Val(); got != 0 {
		t.Fatalf("read key exists = %d, want 0", got)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
}

func TestGroupReadTrackerFailsClosed(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	t.Run("members missing", func(t *testing.T) {
		client := newGroupReadTestClient(t)
		tracker := newGroupReadTracker(t, client)
		if _, err := tracker.AdvanceRead(ctx, 9, 1, 0, now); err != ErrMessageServiceUnavailable {
			t.Fatalf("AdvanceRead() error = %v, want service unavailable", err)
		}
	})
	t.Run("sync required", func(t *testing.T) {
		client := newGroupReadTestClient(t)
		seedGroupReadState(t, client, 9, []uint64{1}, []uint64{10}, nil)
		if err := client.Set(ctx, defaultKeyspace.groupSyncRequiredKey(9), "1", 0).Err(); err != nil {
			t.Fatalf("set sync required: %v", err)
		}
		tracker := newGroupReadTracker(t, client)
		if _, err := tracker.AdvanceRead(ctx, 9, 1, 0, now); err != ErrMessageServiceUnavailable {
			t.Fatalf("AdvanceRead() error = %v, want service unavailable", err)
		}
	})
	t.Run("dissolved", func(t *testing.T) {
		client := newGroupReadTestClient(t)
		seedGroupReadState(t, client, 9, []uint64{1}, []uint64{10}, nil)
		if err := client.Set(ctx, defaultKeyspace.groupDissolvedKey(9), "1", 0).Err(); err != nil {
			t.Fatalf("set dissolved: %v", err)
		}
		tracker := newGroupReadTracker(t, client)
		if _, err := tracker.AdvanceRead(ctx, 9, 1, 0, now); err != ErrGroupDissolved {
			t.Fatalf("AdvanceRead() error = %v, want group dissolved", err)
		}
		if _, err := tracker.ReadStats(ctx, 9, 1, 10); err != ErrGroupDissolved {
			t.Fatalf("ReadStats() error = %v, want group dissolved", err)
		}
	})
	t.Run("not a member", func(t *testing.T) {
		client := newGroupReadTestClient(t)
		seedGroupReadState(t, client, 9, []uint64{1}, []uint64{10}, nil)
		tracker := newGroupReadTracker(t, client)
		if _, err := tracker.AdvanceRead(ctx, 9, 2, 0, now); err != ErrGroupNotMember {
			t.Fatalf("AdvanceRead() error = %v, want not member", err)
		}
		if _, err := tracker.ReadStats(ctx, 9, 2, 10); err != ErrGroupNotMember {
			t.Fatalf("ReadStats() error = %v, want not member", err)
		}
	})
}

func TestGroupReadTrackerStreamFailureKeepsRead(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1}, []uint64{10, 20}, nil)
	if err := client.Set(ctx, defaultKeyspace.groupReadStreamKey(), "not-a-stream", 0).Err(); err != nil {
		t.Fatalf("corrupt stream key: %v", err)
	}
	tracker := newGroupReadTracker(t, client)

	result, err := tracker.AdvanceRead(ctx, 9, 1, 20, time.Now())
	if err != nil {
		t.Fatalf("advance with stream failure: %v", err)
	}
	if !result.Advanced || result.LastReadMessageID != 20 || !result.StreamError {
		t.Fatalf("advance = %#v, want advanced position 20 with stream error", result)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "1").Val(); got != 20 {
		t.Fatalf("stored position = %v, want 20", got)
	}
}

func TestGroupReadTrackerConcurrentAdvancesStayMonotonic(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	ids := make([]uint64, 0, 10)
	for index := uint64(1); index <= 10; index++ {
		ids = append(ids, index*10)
	}
	seedGroupReadState(t, client, 9, []uint64{1}, ids, nil)
	tracker := newGroupReadTracker(t, client)

	var wait sync.WaitGroup
	failures := make(chan error, len(ids))
	for _, throughID := range ids {
		wait.Add(1)
		go func(throughID uint64) {
			defer wait.Done()
			if _, err := tracker.AdvanceRead(ctx, 9, 1, throughID, time.Now()); err != nil {
				failures <- err
			}
		}(throughID)
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Fatalf("concurrent advance: %v", err)
	}

	final, err := tracker.AdvanceRead(ctx, 9, 1, 0, time.Now())
	if err != nil {
		t.Fatalf("final advance: %v", err)
	}
	if final.LastReadMessageID != 100 || final.Advanced {
		t.Fatalf("final advance = %#v, want position 100 already applied", final)
	}
	if length := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); length < 1 || length > 10 {
		t.Fatalf("stream length = %d, want between 1 and 10", length)
	}
}

func TestGroupReadTrackerAdvanceMany(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1, 2, 3}, []uint64{10, 20, 30}, nil)
	tracker := newGroupReadTracker(t, client)

	advances, err := tracker.AdvanceReadMany(
		ctx, 9, []uint64{2, 3, 2, 0, 99}, 25, time.Now(),
	)
	if err != nil {
		t.Fatalf("advance many: %v", err)
	}
	if len(advances) != 2 {
		t.Fatalf("advances = %#v, want users 2 and 3", advances)
	}
	for _, advance := range advances {
		if !advance.Advanced || advance.LastReadMessageID != 20 {
			t.Fatalf("advance = %#v, want position 20", advance)
		}
	}
	if advances[0].UserID != 2 || advances[1].UserID != 3 {
		t.Fatalf("advance order = %#v", advances)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 2 {
		t.Fatalf("stream length = %d, want 2", got)
	}

	again, err := tracker.AdvanceReadMany(ctx, 9, []uint64{2, 3}, 25, time.Now())
	if err != nil {
		t.Fatalf("second advance many: %v", err)
	}
	for _, advance := range again {
		if advance.Advanced || advance.LastReadMessageID != 20 {
			t.Fatalf("second advance = %#v, want unchanged position 20", advance)
		}
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 2 {
		t.Fatalf("stream length after no-op = %d, want 2", got)
	}

	empty, err := tracker.AdvanceReadMany(ctx, 9, nil, 0, time.Now())
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty advance many = %#v, %v", empty, err)
	}
}

func TestGroupReadTrackerAdvanceManyFailsClosed(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	tracker := newGroupReadTracker(t, client)
	if _, err := tracker.AdvanceReadMany(ctx, 9, []uint64{1}, 0, time.Now()); err != ErrMessageServiceUnavailable {
		t.Fatalf("missing members error = %v, want service unavailable", err)
	}

	seedGroupReadState(t, client, 9, []uint64{1}, []uint64{10}, nil)
	if err := client.Set(ctx, defaultKeyspace.groupDissolvedKey(9), "1", 0).Err(); err != nil {
		t.Fatalf("set dissolved: %v", err)
	}
	if _, err := tracker.AdvanceReadMany(ctx, 9, []uint64{1}, 0, time.Now()); err != ErrGroupDissolved {
		t.Fatalf("dissolved error = %v, want group dissolved", err)
	}
}

func TestGroupReadTrackerUnreadCounts(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1, 2}, []uint64{10, 20, 30}, map[uint64]uint64{1: 20})
	seedGroupReadState(t, client, 10, []uint64{1}, []uint64{40, 50}, nil)
	seedGroupReadState(t, client, 11, []uint64{1}, nil, map[uint64]uint64{1: 50})
	tracker := newGroupReadTracker(t, client)

	counts, err := tracker.UnreadCounts(ctx, 1, []uint64{9, 10, 11})
	if err != nil {
		t.Fatalf("unread counts: %v", err)
	}
	if counts[9] != 1 || counts[10] != 2 || counts[11] != 0 {
		t.Fatalf("unread counts = %#v, want 9:1 10:2 11:0", counts)
	}
}

func TestGroupReadTrackerUnreadCountsFailsClosed(t *testing.T) {
	ctx := context.Background()
	for name, prepare := range map[string]func(*redis.Client){
		"members missing": func(*redis.Client) {},
		"not a member": func(client *redis.Client) {
			seedGroupReadState(t, client, 9, []uint64{2}, []uint64{10}, nil)
		},
		"sync required": func(client *redis.Client) {
			seedGroupReadState(t, client, 9, []uint64{1}, []uint64{10}, nil)
			if err := client.Set(ctx, defaultKeyspace.groupSyncRequiredKey(9), "1", 0).Err(); err != nil {
				t.Fatalf("set sync required: %v", err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			client := newGroupReadTestClient(t)
			prepare(client)
			tracker := newGroupReadTracker(t, client)
			if _, err := tracker.UnreadCounts(ctx, 1, []uint64{9}); err != ErrMessageServiceUnavailable {
				t.Fatalf("UnreadCounts() error = %v, want service unavailable", err)
			}
		})
	}
}

func TestGroupReadTrackerReadStats(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(
		t, client, 9, []uint64{1, 2, 3}, []uint64{10, 20, 30},
		map[uint64]uint64{1: 25, 2: 30, 3: 40},
	)
	tracker := newGroupReadTracker(t, client)

	notCaughtUp, err := tracker.ReadStats(ctx, 9, 1, 30)
	if err != nil {
		t.Fatalf("read stats: %v", err)
	}
	if notCaughtUp.ReadCount != 2 || notCaughtUp.MemberCount != 2 || !notCaughtUp.AllRead {
		t.Fatalf("read stats = %#v, want 2/2 all read", notCaughtUp)
	}

	if err := client.ZAdd(ctx, defaultKeyspace.groupReadKey(9), redis.Z{
		Score: 30, Member: "1",
	}).Err(); err != nil {
		t.Fatalf("catch sender up: %v", err)
	}
	caughtUp, err := tracker.ReadStats(ctx, 9, 1, 30)
	if err != nil {
		t.Fatalf("read stats after catch-up: %v", err)
	}
	if caughtUp.ReadCount != 2 || caughtUp.MemberCount != 2 || !caughtUp.AllRead {
		t.Fatalf("read stats = %#v, want sender excluded from both counts", caughtUp)
	}

	if err := client.ZRem(ctx, defaultKeyspace.groupReadKey(9), "3").Err(); err != nil {
		t.Fatalf("remove reader: %v", err)
	}
	partial, err := tracker.ReadStats(ctx, 9, 1, 30)
	if err != nil {
		t.Fatalf("partial read stats: %v", err)
	}
	if partial.ReadCount != 1 || partial.MemberCount != 2 || partial.AllRead {
		t.Fatalf("partial read stats = %#v, want 1/2", partial)
	}

	if err := tracker.RemoveMember(ctx, 9, 3); err != nil {
		t.Fatalf("remove member: %v", err)
	}
	if err := client.SRem(ctx, defaultKeyspace.groupMembersKey(9), 3).Err(); err != nil {
		t.Fatalf("remove member from set: %v", err)
	}
	shrunk, err := tracker.ReadStats(ctx, 9, 1, 30)
	if err != nil {
		t.Fatalf("shrunk read stats: %v", err)
	}
	if shrunk.ReadCount != 1 || shrunk.MemberCount != 1 || !shrunk.AllRead {
		t.Fatalf("shrunk read stats = %#v, want 1/1 all read", shrunk)
	}
}

func TestGroupReadTrackerReadStatsMissingPositionsCountAsUnread(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1, 2, 3}, []uint64{30}, map[uint64]uint64{2: 30})
	tracker := newGroupReadTracker(t, client)

	stats, err := tracker.ReadStats(ctx, 9, 1, 30)
	if err != nil {
		t.Fatalf("read stats: %v", err)
	}
	if stats.ReadCount != 1 || stats.MemberCount != 2 || stats.AllRead {
		t.Fatalf("read stats = %#v, want 1/2", stats)
	}
}

func TestGroupReadTrackerReadStatsAloneInGroup(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1}, []uint64{5}, map[uint64]uint64{1: 5})
	tracker := newGroupReadTracker(t, client)

	stats, err := tracker.ReadStats(ctx, 9, 1, 5)
	if err != nil {
		t.Fatalf("read stats: %v", err)
	}
	if stats.ReadCount != 0 || stats.MemberCount != 0 || stats.AllRead {
		t.Fatalf("read stats = %#v, want empty denominator", stats)
	}
}

func TestGroupReadTrackerMessageSender(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	if err := client.HSet(ctx, defaultKeyspace.groupMessageKey(50), map[string]interface{}{
		"group_id": "9", "sender_id": "7",
	}).Err(); err != nil {
		t.Fatalf("seed message 50: %v", err)
	}
	if err := client.HSet(ctx, defaultKeyspace.groupMessageKey(51), map[string]interface{}{
		"group_id": "8", "sender_id": "7",
	}).Err(); err != nil {
		t.Fatalf("seed message 51: %v", err)
	}
	if err := client.HSet(ctx, defaultKeyspace.groupMessageKey(52), map[string]interface{}{
		"sender_id": "7",
	}).Err(); err != nil {
		t.Fatalf("seed message 52: %v", err)
	}
	tracker := newGroupReadTracker(t, client)

	senderID, found, err := tracker.MessageSender(ctx, 9, 50)
	if err != nil || !found || senderID != 7 {
		t.Fatalf("MessageSender(50) = %d, %t, %v", senderID, found, err)
	}
	senderID, found, err = tracker.MessageSender(ctx, 9, 51)
	if err != nil || !found || senderID != 0 {
		t.Fatalf("MessageSender(51) = %d, %t, %v, want other group", senderID, found, err)
	}
	_, found, err = tracker.MessageSender(ctx, 9, 53)
	if err != nil || found {
		t.Fatalf("MessageSender(53) = %t, %v, want missing", found, err)
	}
	_, found, err = tracker.MessageSender(ctx, 9, 0)
	if err != nil || found {
		t.Fatalf("MessageSender(0) = %t, %v, want missing", found, err)
	}
	senderID, found, err = tracker.MessageSender(ctx, 9, 52)
	if err != nil || !found || senderID != 0 {
		t.Fatalf("MessageSender(52) = %d, %t, %v, want unknown sender", senderID, found, err)
	}
}

func TestGroupReadTrackerSeedRemoveAndReconcile(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1, 2}, nil, map[uint64]uint64{2: 20, 3: 30})
	tracker := newGroupReadTracker(t, client)

	if err := tracker.SeedMember(ctx, 9, 1, 5); err != nil {
		t.Fatalf("seed member: %v", err)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "1").Val(); got != 5 {
		t.Fatalf("seeded position = %v, want 5", got)
	}
	if err := tracker.SeedMember(ctx, 9, 1, 15); err != nil {
		t.Fatalf("seed existing member: %v", err)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "1").Val(); got != 5 {
		t.Fatalf("existing position = %v, want unchanged 5", got)
	}

	if err := tracker.RemoveMember(ctx, 9, 1); err != nil {
		t.Fatalf("remove member: %v", err)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "1").Err(); !errors.Is(got, redis.Nil) {
		t.Fatalf("removed position error = %v, want redis.Nil", got)
	}

	if err := tracker.ReconcileGroup(ctx, 9, map[uint64]uint64{2: 10, 4: 7}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "2").Val(); got != 20 {
		t.Fatalf("reconciled position 2 = %v, want unchanged 20", got)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "4").Val(); got != 7 {
		t.Fatalf("reconciled position 4 = %v, want 7", got)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "3").Err(); !errors.Is(got, redis.Nil) {
		t.Fatalf("removed member 3 error = %v, want redis.Nil", got)
	}

	if err := tracker.ReconcileGroup(ctx, 9, nil); err != nil {
		t.Fatalf("reconcile empty: %v", err)
	}
	if got := client.ZCard(ctx, defaultKeyspace.groupReadKey(9)).Val(); got != 0 {
		t.Fatalf("reconciled member count = %d, want 0", got)
	}
}

func TestGroupReadTrackerReadModelReady(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	tracker := newGroupReadTracker(t, client)

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

func TestGroupReadTrackerRequiresRedis(t *testing.T) {
	tracker := NewGroupReadTracker(nil, GroupReadTrackerOptions{})
	ctx := context.Background()
	if _, err := tracker.AdvanceRead(ctx, 1, 2, 10, time.Now()); err == nil {
		t.Fatalf("AdvanceRead() error = nil, want service unavailable")
	}
	if _, err := tracker.AdvanceReadMany(ctx, 1, []uint64{2}, 10, time.Now()); err == nil {
		t.Fatalf("AdvanceReadMany() error = nil, want service unavailable")
	}
	if _, err := tracker.UnreadCounts(ctx, 2, []uint64{1}); err == nil {
		t.Fatalf("UnreadCounts() error = nil, want service unavailable")
	}
	if _, err := tracker.ReadStats(ctx, 1, 2, 10); err == nil {
		t.Fatalf("ReadStats() error = nil, want service unavailable")
	}
}
