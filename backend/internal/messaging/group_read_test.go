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
	seqs []uint64,
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
	for _, seq := range seqs {
		if err := client.ZAdd(ctx, defaultKeyspace.groupInboxKey(groupID), redis.Z{
			Score:  float64(seq),
			Member: strconvUint(seq),
		}).Err(); err != nil {
			t.Fatalf("seed inbox: %v", err)
		}
	}
	for userID, position := range positions {
		if err := client.ZAdd(ctx, defaultKeyspace.groupReadKey(groupID), redis.Z{
			Score:  float64(position),
			Member: strconvUint(userID),
		}).Err(); err != nil {
			t.Fatalf("seed read position: %v", err)
		}
	}
}

func TestGroupReadTrackerAdvancesMonotonically(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	seedGroupReadState(t, client, 9, []uint64{1, 2, 3}, []uint64{1, 2, 3}, nil)
	setGroupSequence(t, client, 9, 3)
	tracker := newGroupReadTracker(t, client)

	first, err := tracker.AdvanceRead(ctx, 9, 2, 2, now)
	if err != nil {
		t.Fatalf("first advance: %v", err)
	}
	if !first.Advanced || first.LastReadSeq != 2 || first.UserID != 2 {
		t.Fatalf("first advance = %#v, want user 2 at 2", first)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "2").Val(); got != 2 {
		t.Fatalf("stored position = %v, want 2", got)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length = %d, want 1", got)
	}

	backwards, err := tracker.AdvanceRead(ctx, 9, 2, 1, now.Add(time.Second))
	if err != nil {
		t.Fatalf("backwards advance: %v", err)
	}
	if backwards.Advanced || backwards.LastReadSeq != 2 {
		t.Fatalf("backwards advance = %#v, want unchanged position 2", backwards)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupReadStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after no-op = %d, want 1", got)
	}

	forward, err := tracker.AdvanceRead(ctx, 9, 2, 999, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("forward advance: %v", err)
	}
	if !forward.Advanced || forward.LastReadSeq != 3 {
		t.Fatalf("forward advance = %#v, want position 3 clamped to the counter", forward)
	}

	unbounded, err := tracker.AdvanceRead(ctx, 9, 2, 0, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("unbounded advance: %v", err)
	}
	if unbounded.Advanced || unbounded.LastReadSeq != 3 {
		t.Fatalf("unbounded advance = %#v, want unchanged position 3", unbounded)
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
		values["last_read_seq"] != "2" || values["updated_at_ms"] != "1700000000000" {
		t.Fatalf("stream event = %#v", values)
	}
	if _, ok := values["last_read_message_id"]; ok {
		t.Fatalf("stream event still uses last_read_message_id: %#v", values)
	}
}

func TestGroupReadTrackerClampsToCounter(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1}, []uint64{1, 2, 3}, nil)
	setGroupSequence(t, client, 9, 3)
	tracker := newGroupReadTracker(t, client)

	clamped, err := tracker.AdvanceRead(ctx, 9, 1, 2, time.Now())
	if err != nil {
		t.Fatalf("clamped advance: %v", err)
	}
	if !clamped.Advanced || clamped.LastReadSeq != 2 {
		t.Fatalf("clamped advance = %#v, want position 2", clamped)
	}

	below, err := tracker.AdvanceRead(ctx, 9, 1, 1, time.Now())
	if err != nil {
		t.Fatalf("below advance: %v", err)
	}
	if below.Advanced || below.LastReadSeq != 2 {
		t.Fatalf("below advance = %#v, want unchanged position 2", below)
	}
}

func TestGroupReadTrackerNoCounter(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1}, []uint64{1}, nil)
	tracker := newGroupReadTracker(t, client)

	result, err := tracker.AdvanceRead(ctx, 9, 1, 100, time.Now())
	if err != nil {
		t.Fatalf("advance without a counter: %v", err)
	}
	if result.Advanced || result.LastReadSeq != 0 {
		t.Fatalf("advance = %#v, want no change", result)
	}
	if got := client.Exists(ctx, defaultKeyspace.groupReadKey(9)).Val(); got != 0 {
		t.Fatalf("read key exists = %d, want 0", got)
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
		seedGroupReadState(t, client, 9, []uint64{1}, []uint64{1}, nil)
		setGroupSequence(t, client, 9, 1)
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
		seedGroupReadState(t, client, 9, []uint64{1}, []uint64{1}, nil)
		if err := client.Set(ctx, defaultKeyspace.groupDissolvedKey(9), "1", 0).Err(); err != nil {
			t.Fatalf("set dissolved: %v", err)
		}
		tracker := newGroupReadTracker(t, client)
		if _, err := tracker.AdvanceRead(ctx, 9, 1, 0, now); err != ErrGroupDissolved {
			t.Fatalf("AdvanceRead() error = %v, want group dissolved", err)
		}
		if _, err := tracker.ReadStats(ctx, 9, 1, 1); err != ErrGroupDissolved {
			t.Fatalf("ReadStats() error = %v, want group dissolved", err)
		}
	})
	t.Run("not a member", func(t *testing.T) {
		client := newGroupReadTestClient(t)
		seedGroupReadState(t, client, 9, []uint64{1}, []uint64{1}, nil)
		tracker := newGroupReadTracker(t, client)
		if _, err := tracker.AdvanceRead(ctx, 9, 2, 0, now); err != ErrGroupNotMember {
			t.Fatalf("AdvanceRead() error = %v, want not member", err)
		}
		if _, err := tracker.ReadStats(ctx, 9, 2, 1); err != ErrGroupNotMember {
			t.Fatalf("ReadStats() error = %v, want not member", err)
		}
	})
}

func TestGroupReadTrackerStreamFailureKeepsRead(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1}, []uint64{1, 2}, nil)
	setGroupSequence(t, client, 9, 2)
	if err := client.Set(ctx, defaultKeyspace.groupReadStreamKey(), "not-a-stream", 0).Err(); err != nil {
		t.Fatalf("corrupt stream key: %v", err)
	}
	tracker := newGroupReadTracker(t, client)

	result, err := tracker.AdvanceRead(ctx, 9, 1, 2, time.Now())
	if err != nil {
		t.Fatalf("advance with stream failure: %v", err)
	}
	if !result.Advanced || result.LastReadSeq != 2 || !result.StreamError {
		t.Fatalf("advance = %#v, want advanced position 2 with stream error", result)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "1").Val(); got != 2 {
		t.Fatalf("stored position = %v, want 2", got)
	}
}

func TestGroupReadTrackerConcurrentAdvancesStayMonotonic(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	setGroupSequence(t, client, 9, 100)
	seedGroupReadState(t, client, 9, []uint64{1}, nil, nil)
	tracker := newGroupReadTracker(t, client)

	var wait sync.WaitGroup
	failures := make(chan error, 10)
	for index := uint64(1); index <= 10; index++ {
		wait.Add(1)
		go func(through uint64) {
			defer wait.Done()
			if _, err := tracker.AdvanceRead(ctx, 9, 1, through*10, time.Now()); err != nil {
				failures <- err
			}
		}(index)
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
	if final.LastReadSeq != 100 || final.Advanced {
		t.Fatalf("final advance = %#v, want position 100 already applied", final)
	}
}

func TestGroupReadTrackerAdvanceMany(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1, 2, 3}, []uint64{1, 2, 3}, nil)
	setGroupSequence(t, client, 9, 3)
	tracker := newGroupReadTracker(t, client)

	advances, err := tracker.AdvanceReadMany(
		ctx, 9, []uint64{2, 3, 2, 0, 99}, 2, time.Now(),
	)
	if err != nil {
		t.Fatalf("advance many: %v", err)
	}
	if len(advances) != 2 {
		t.Fatalf("advances = %#v, want users 2 and 3", advances)
	}
	for _, advance := range advances {
		if !advance.Advanced || advance.LastReadSeq != 2 {
			t.Fatalf("advance = %#v, want position 2", advance)
		}
	}
	if advances[0].UserID != 2 || advances[1].UserID != 3 {
		t.Fatalf("advance order = %#v", advances)
	}

	again, err := tracker.AdvanceReadMany(ctx, 9, []uint64{2, 3}, 2, time.Now())
	if err != nil {
		t.Fatalf("second advance many: %v", err)
	}
	for _, advance := range again {
		if advance.Advanced || advance.LastReadSeq != 2 {
			t.Fatalf("second advance = %#v, want unchanged position 2", advance)
		}
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

	seedGroupReadState(t, client, 9, []uint64{1}, nil, nil)
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
	seedGroupReadState(t, client, 9, []uint64{1, 2}, nil, map[uint64]uint64{1: 2})
	setGroupSequence(t, client, 9, 3)
	seedGroupReadState(t, client, 10, []uint64{1}, nil, nil)
	setGroupSequence(t, client, 10, 2)
	seedGroupReadState(t, client, 11, []uint64{1}, nil, map[uint64]uint64{1: 5})
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
			seedGroupReadState(t, client, 9, []uint64{2}, nil, nil)
		},
		"sync required": func(client *redis.Client) {
			seedGroupReadState(t, client, 9, []uint64{1}, nil, nil)
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

func TestGroupReadTrackerStates(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1}, nil, map[uint64]uint64{1: 2})
	setGroupSequence(t, client, 9, 5)
	seedGroupReadState(t, client, 10, []uint64{1}, nil, nil)
	setGroupSequence(t, client, 10, 4)
	seedGroupReadState(t, client, 11, []uint64{1}, nil, nil)
	tracker := newGroupReadTracker(t, client)

	states, err := tracker.States(ctx, 1, []uint64{9, 10, 11})
	if err != nil {
		t.Fatalf("States() error = %v", err)
	}
	if states[9].LastSeq != 5 || states[9].ReadSeq != 2 || states[9].Unread != 3 {
		t.Fatalf("group 9 state = %#v", states[9])
	}
	if states[10].LastSeq != 4 || states[10].Unread != 4 {
		t.Fatalf("group 10 state = %#v", states[10])
	}
	if states[11].LastSeq != 0 || states[11].Unread != 0 {
		t.Fatalf("group 11 state = %#v", states[11])
	}
}

func TestGroupReadTrackerStatesFailsClosed(t *testing.T) {
	ctx := context.Background()
	for name, prepare := range map[string]func(*redis.Client){
		"members missing": func(*redis.Client) {},
		"not a member": func(client *redis.Client) {
			seedGroupReadState(t, client, 9, []uint64{2}, nil, nil)
		},
		"sync required": func(client *redis.Client) {
			seedGroupReadState(t, client, 9, []uint64{1}, nil, nil)
			if err := client.Set(ctx, defaultKeyspace.groupSyncRequiredKey(9), "1", 0).Err(); err != nil {
				t.Fatalf("set sync required: %v", err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			client := newGroupReadTestClient(t)
			prepare(client)
			tracker := newGroupReadTracker(t, client)
			if _, err := tracker.States(ctx, 1, []uint64{9}); err != ErrMessageServiceUnavailable {
				t.Fatalf("States() error = %v, want service unavailable", err)
			}
		})
	}
}

func TestGroupReadTrackerReadStats(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(
		t, client, 9, []uint64{1, 2, 3}, nil,
		map[uint64]uint64{1: 2, 2: 3, 3: 4},
	)
	tracker := newGroupReadTracker(t, client)

	notCaughtUp, err := tracker.ReadStats(ctx, 9, 1, 3)
	if err != nil {
		t.Fatalf("read stats: %v", err)
	}
	if notCaughtUp.ReadCount != 2 || notCaughtUp.MemberCount != 2 || !notCaughtUp.AllRead {
		t.Fatalf("read stats = %#v, want 2/2 all read", notCaughtUp)
	}

	if err := client.ZAdd(ctx, defaultKeyspace.groupReadKey(9), redis.Z{
		Score: 3, Member: "1",
	}).Err(); err != nil {
		t.Fatalf("catch sender up: %v", err)
	}
	caughtUp, err := tracker.ReadStats(ctx, 9, 1, 3)
	if err != nil {
		t.Fatalf("read stats after catch-up: %v", err)
	}
	if caughtUp.ReadCount != 2 || caughtUp.MemberCount != 2 || !caughtUp.AllRead {
		t.Fatalf("read stats = %#v, want sender excluded from both counts", caughtUp)
	}

	if err := client.ZRem(ctx, defaultKeyspace.groupReadKey(9), "3").Err(); err != nil {
		t.Fatalf("remove reader: %v", err)
	}
	partial, err := tracker.ReadStats(ctx, 9, 1, 3)
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
	shrunk, err := tracker.ReadStats(ctx, 9, 1, 3)
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
	seedGroupReadState(t, client, 9, []uint64{1, 2, 3}, nil, map[uint64]uint64{2: 3})
	tracker := newGroupReadTracker(t, client)

	stats, err := tracker.ReadStats(ctx, 9, 1, 3)
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
	seedGroupReadState(t, client, 9, []uint64{1}, nil, map[uint64]uint64{1: 1})
	tracker := newGroupReadTracker(t, client)

	stats, err := tracker.ReadStats(ctx, 9, 1, 1)
	if err != nil {
		t.Fatalf("read stats: %v", err)
	}
	if stats.ReadCount != 0 || stats.MemberCount != 0 || stats.AllRead {
		t.Fatalf("read stats = %#v, want empty denominator", stats)
	}
}

func TestGroupReadTrackerMessageBySeq(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupMessages(t, client, 9, map[uint64]uint64{50: 5, 52: 8})
	seedGroupMessages(t, client, 8, map[uint64]uint64{51: 6})
	if err := client.HDel(ctx, defaultKeyspace.groupMessageKey(52), "sender_id").Err(); err != nil {
		t.Fatalf("remove sender: %v", err)
	}
	tracker := newGroupReadTracker(t, client)

	ref, found, err := tracker.MessageBySeq(ctx, 9, 5)
	if err != nil || !found || ref.ID != 50 || ref.SenderID != 1 {
		t.Fatalf("MessageBySeq(9,5) = %#v, %t, %v", ref, found, err)
	}
	_, found, err = tracker.MessageBySeq(ctx, 9, 6)
	if err != nil || found {
		t.Fatalf("MessageBySeq(9,6) = %t, %v, want missing (other group)", found, err)
	}
	_, found, err = tracker.MessageBySeq(ctx, 9, 7)
	if err != nil || found {
		t.Fatalf("MessageBySeq(9,7) = %t, %v, want missing", found, err)
	}
	_, found, err = tracker.MessageBySeq(ctx, 9, 0)
	if err != nil || found {
		t.Fatalf("MessageBySeq(9,0) = %t, %v, want missing", found, err)
	}
	ref, found, err = tracker.MessageBySeq(ctx, 9, 8)
	if err != nil || !found || ref.ID != 52 || ref.SenderID != 0 {
		t.Fatalf("MessageBySeq(9,8) = %#v, %t, %v, want entity without sender", ref, found, err)
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

func TestGroupReadTrackerSetMemberBaselineRaisesOnly(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1, 2}, nil, map[uint64]uint64{1: 100})
	tracker := newGroupReadTracker(t, client)

	if err := tracker.SetMemberBaseline(ctx, 9, 1, 150); err != nil {
		t.Fatalf("raise baseline: %v", err)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "1").Val(); got != 150 {
		t.Fatalf("raised position = %v, want 150", got)
	}
	if err := tracker.SetMemberBaseline(ctx, 9, 1, 90); err != nil {
		t.Fatalf("lower baseline: %v", err)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "1").Val(); got != 150 {
		t.Fatalf("lowered position = %v, want unchanged 150", got)
	}
	if err := tracker.SetMemberBaseline(ctx, 9, 2, 60); err != nil {
		t.Fatalf("seed missing member: %v", err)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "2").Val(); got != 60 {
		t.Fatalf("new member position = %v, want 60", got)
	}
}

func TestGroupReadTrackerMemberIDs(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	seedGroupReadState(t, client, 9, []uint64{1, 2, 3}, nil, nil)
	tracker := newGroupReadTracker(t, client)

	got, err := tracker.MemberIDs(ctx, 9)
	if err != nil {
		t.Fatalf("member ids: %v", err)
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("member ids = %#v", got)
	}
}

func TestGroupReadTrackerReadModelReady(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	gate := NewSeqGate()
	tracker := NewGroupReadTracker(client, GroupReadTrackerOptions{Gate: gate})

	ready, err := tracker.ReadModelReady(ctx)
	if err != nil {
		t.Fatalf("initial ready: %v", err)
	}
	if ready {
		t.Fatal("initial ready = true, want false")
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

func TestGroupReadTrackerGate(t *testing.T) {
	client := newGroupReadTestClient(t)
	ctx := context.Background()
	gate := NewSeqGate()
	tracker := NewGroupReadTracker(client, GroupReadTrackerOptions{Gate: gate})
	gate.Close()

	if _, err := tracker.AdvanceReadMany(ctx, 9, []uint64{1}, 0, time.Now()); !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("AdvanceReadMany() error = %v, want ErrSeqNotReady", err)
	}
	if _, err := tracker.States(ctx, 1, []uint64{9}); !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("States() error = %v, want ErrSeqNotReady", err)
	}
	if _, _, err := tracker.MessageBySeq(ctx, 9, 1); !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("MessageBySeq() error = %v, want ErrSeqNotReady", err)
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
	if _, err := tracker.States(ctx, 2, []uint64{1}); err == nil {
		t.Fatalf("States() error = nil, want service unavailable")
	}
	if _, _, err := tracker.MessageBySeq(ctx, 1, 10); err == nil {
		t.Fatalf("MessageBySeq() error = nil, want service unavailable")
	}
}
