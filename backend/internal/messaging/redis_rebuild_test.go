package messaging

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

type fakeRebuildStore struct {
	direct             []store.Message
	groups             []store.GroupMessage
	friends            []store.FriendshipPair
	convReads          []store.ConversationReadState
	groupProjections   []store.GroupMembershipProjection
	maxID              uint64
	convStatsOverride  []store.ConversationSeqStat
	groupStatsOverride []store.GroupSeqStat
	// beforeConvStats runs once, on the first verify read, so a test can damage
	// the namespace between the write passes and their verification.
	beforeConvStats func()
}

func newFakeRebuildStore(now time.Time) *fakeRebuildStore {
	return &fakeRebuildStore{
		direct: []store.Message{
			{ID: 1, Seq: 1, SenderID: 1, RecipientID: 2, ClientMessageID: "c1", Body: "a", CreatedAt: now.Add(-2 * time.Hour)},
			{ID: 2, Seq: 2, SenderID: 2, RecipientID: 1, ClientMessageID: "c2", Body: "b", CreatedAt: now.Add(-90 * time.Minute)},
			{ID: 3, Seq: 3, SenderID: 1, RecipientID: 2, ClientMessageID: "c3", Body: "c", CreatedAt: now.Add(-30 * time.Minute)},
			{ID: 4, Seq: 1, SenderID: 2, RecipientID: 3, ClientMessageID: "c4", Body: "d", CreatedAt: now.Add(-75 * time.Minute)},
			{ID: 5, Seq: 2, SenderID: 3, RecipientID: 2, ClientMessageID: "c5", Body: "e", CreatedAt: now.Add(-15 * time.Minute)},
		},
		groups: []store.GroupMessage{
			{ID: 6, Seq: 1, GroupID: 7, SenderID: 1,
				Sender:          store.PublicUser{Username: "u1", Nickname: "n1"},
				ClientMessageID: "g1", Body: "x", CreatedAt: now.Add(-time.Hour)},
			{ID: 7, Seq: 2, GroupID: 7, SenderID: 2,
				Sender:          store.PublicUser{Username: "u2", Nickname: "n2"},
				ClientMessageID: "g2", Body: "y", CreatedAt: now.Add(-time.Hour)},
			{ID: 8, Seq: 1, GroupID: 8, SenderID: 1,
				ClientMessageID: "g3", Body: "z", CreatedAt: now.Add(-time.Hour)},
		},
		friends: []store.FriendshipPair{
			{UserID: 1, FriendUserID: 2},
			{UserID: 2, FriendUserID: 1},
			{UserID: 2, FriendUserID: 3},
			{UserID: 3, FriendUserID: 2},
		},
		convReads: []store.ConversationReadState{
			{UserID: 1, PeerID: 2, LastReadSeq: 2},
			{UserID: 2, PeerID: 1, LastReadSeq: 1},
			{UserID: 3, PeerID: 2, LastReadSeq: 1},
		},
		groupProjections: []store.GroupMembershipProjection{
			{GroupID: 7, MemberIDs: []uint64{1, 2}, Positions: map[uint64]uint64{1: 1, 2: 0}},
			{GroupID: 8, MemberIDs: []uint64{1}, Positions: map[uint64]uint64{1: 1}},
		},
		maxID: 8,
	}
}

func (f *fakeRebuildStore) sortedDirect() []store.Message {
	rows := append([]store.Message(nil), f.direct...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

func (f *fakeRebuildStore) sortedGroups() []store.GroupMessage {
	rows := append([]store.GroupMessage(nil), f.groups...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

func (f *fakeRebuildStore) ListMessagesForRebuild(
	_ context.Context,
	afterID uint64,
	limit int,
) ([]store.Message, error) {
	result := make([]store.Message, 0, limit)
	for _, message := range f.sortedDirect() {
		if message.ID <= afterID {
			continue
		}
		if len(result) == limit {
			break
		}
		result = append(result, message)
	}
	return result, nil
}

func (f *fakeRebuildStore) ListGroupMessagesForRebuild(
	_ context.Context,
	afterID uint64,
	limit int,
) ([]store.GroupMessage, error) {
	result := make([]store.GroupMessage, 0, limit)
	for _, message := range f.sortedGroups() {
		if message.ID <= afterID {
			continue
		}
		if len(result) == limit {
			break
		}
		result = append(result, message)
	}
	return result, nil
}

func (f *fakeRebuildStore) ListMessagesBySeqRange(
	_ context.Context,
	lowID, highID, afterSeq uint64,
	limit int,
) ([]store.Message, error) {
	rows := make([]store.Message, 0)
	for _, message := range f.sortedDirect() {
		pair := [2]uint64{}
		pair[0], pair[1] = sortedPair(message.SenderID, message.RecipientID)
		if pair[0] != lowID || pair[1] != highID || message.Seq <= afterSeq {
			continue
		}
		rows = append(rows, message)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Seq < rows[j].Seq })
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (f *fakeRebuildStore) ListGroupMessagesBySeqRange(
	_ context.Context,
	groupID, afterSeq uint64,
	limit int,
) ([]store.GroupMessage, error) {
	rows := make([]store.GroupMessage, 0)
	for _, message := range f.groups {
		if message.GroupID != groupID || message.Seq <= afterSeq {
			continue
		}
		rows = append(rows, message)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Seq < rows[j].Seq })
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (f *fakeRebuildStore) MaxServerMessageID(context.Context) (uint64, error) {
	return f.maxID, nil
}

func (f *fakeRebuildStore) ListConversationReadStates(
	_ context.Context,
	afterUserID, afterPeerID uint64,
	limit int,
) ([]store.ConversationReadState, error) {
	rows := append([]store.ConversationReadState(nil), f.convReads...)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].UserID != rows[j].UserID {
			return rows[i].UserID < rows[j].UserID
		}
		return rows[i].PeerID < rows[j].PeerID
	})
	result := make([]store.ConversationReadState, 0, limit)
	for _, row := range rows {
		if row.UserID < afterUserID ||
			(row.UserID == afterUserID && row.PeerID <= afterPeerID) {
			continue
		}
		if len(result) == limit {
			break
		}
		result = append(result, row)
	}
	return result, nil
}

func (f *fakeRebuildStore) ListConversationSeqStats(
	_ context.Context,
	afterLowID, afterHighID uint64,
	limit int,
) ([]store.ConversationSeqStat, error) {
	if f.beforeConvStats != nil {
		before := f.beforeConvStats
		f.beforeConvStats = nil
		before()
	}
	rows := f.convStatsOverride
	if rows == nil {
		type stats struct {
			count, maxSeq uint64
		}
		byPair := make(map[[2]uint64]*stats)
		for _, message := range f.direct {
			pair := [2]uint64{}
			pair[0], pair[1] = sortedPair(message.SenderID, message.RecipientID)
			entry, ok := byPair[pair]
			if !ok {
				entry = &stats{}
				byPair[pair] = entry
			}
			entry.count++
			if message.Seq > entry.maxSeq {
				entry.maxSeq = message.Seq
			}
		}
		for pair, entry := range byPair {
			rows = append(rows, store.ConversationSeqStat{
				LowID: pair[0], HighID: pair[1], Count: entry.count, MaxSeq: entry.maxSeq,
			})
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].LowID != rows[j].LowID {
				return rows[i].LowID < rows[j].LowID
			}
			return rows[i].HighID < rows[j].HighID
		})
	}
	result := make([]store.ConversationSeqStat, 0, limit)
	for _, row := range rows {
		if row.LowID < afterLowID ||
			(row.LowID == afterLowID && row.HighID <= afterHighID) {
			continue
		}
		if len(result) == limit {
			break
		}
		result = append(result, row)
	}
	return result, nil
}

func (f *fakeRebuildStore) ListGroupSeqStats(
	_ context.Context,
	afterGroupID uint64,
	limit int,
) ([]store.GroupSeqStat, error) {
	rows := f.groupStatsOverride
	if rows == nil {
		type stats struct {
			count, maxSeq uint64
		}
		byGroup := make(map[uint64]*stats)
		for _, message := range f.groups {
			entry, ok := byGroup[message.GroupID]
			if !ok {
				entry = &stats{}
				byGroup[message.GroupID] = entry
			}
			entry.count++
			if message.Seq > entry.maxSeq {
				entry.maxSeq = message.Seq
			}
		}
		for groupID, entry := range byGroup {
			rows = append(rows, store.GroupSeqStat{
				GroupID: groupID, Count: entry.count, MaxSeq: entry.maxSeq,
			})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].GroupID < rows[j].GroupID })
	}
	result := make([]store.GroupSeqStat, 0, limit)
	for _, row := range rows {
		if row.GroupID <= afterGroupID {
			continue
		}
		if len(result) == limit {
			break
		}
		result = append(result, row)
	}
	return result, nil
}

func (f *fakeRebuildStore) ListGroupMembershipProjections(
	context.Context,
) ([]store.GroupMembershipProjection, error) {
	return f.groupProjections, nil
}

func (f *fakeRebuildStore) ListFriendshipPairs(
	context.Context,
) ([]store.FriendshipPair, error) {
	return f.friends, nil
}

func (f *fakeRebuildStore) CountUnnumberedMessages(
	context.Context,
) (uint64, uint64, error) {
	direct, group := uint64(0), uint64(0)
	for _, message := range f.direct {
		if message.Seq == 0 {
			direct++
		}
	}
	for _, message := range f.groups {
		if message.Seq == 0 {
			group++
		}
	}
	return direct, group, nil
}

type drainRecorder struct {
	calls  int
	result QueueDrainResult
	err    error
	onCall func(context.Context)
}

func (d *drainRecorder) Drain(ctx context.Context) (QueueDrainResult, error) {
	d.calls++
	if d.onCall != nil {
		d.onCall(ctx)
	}
	return d.result, d.err
}

func newRebuildTest(
	t *testing.T,
	now time.Time,
) (*redis.Client, *fakeRebuildStore, *drainRecorder) {
	t.Helper()
	client, _ := newProjectionTest(t)
	fakeStore := newFakeRebuildStore(now)
	drain := &drainRecorder{result: QueueDrainResult{Direct: 2, Group: 1, DeadLettered: 1}}
	return client, fakeStore, drain
}

func runRebuild(
	t *testing.T,
	client *redis.Client,
	fakeStore RebuildStore,
	drain *drainRecorder,
	now time.Time,
	batch int,
) (RedisRebuildResult, error) {
	t.Helper()
	rebuilder := NewRedisRebuilder(client, fakeStore, RedisRebuildOptions{
		Keyspace:    defaultKeyspace,
		Batch:       batch,
		DedupTTL:    7 * 24 * time.Hour,
		Now:         func() time.Time { return now },
		Logf:        func(string, ...interface{}) {},
		DrainQueues: drain.Drain,
	})
	return rebuilder.Run(context.Background())
}

func TestRedisRebuildHappyPath(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	staleKey := messageKey(999)
	if err := client.Set(ctx, staleKey, "stale", 0).Err(); err != nil {
		t.Fatalf("seed stale entity: %v", err)
	}
	if err := client.Set(ctx, directSequenceKey(9, 10), "5", 0).Err(); err != nil {
		t.Fatalf("seed stale counter: %v", err)
	}
	if err := client.Set(ctx, sequenceReadyKey(), "1", 0).Err(); err != nil {
		t.Fatalf("seed stale ready marker: %v", err)
	}
	drain.onCall = func(ctx context.Context) {
		if exists := client.Exists(ctx, staleKey).Val(); exists != 1 {
			t.Fatalf("drain ran after the namespace was cleared")
		}
	}

	result, err := runRebuild(t, client, fakeStore, drain, now, 2)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if drain.calls != 1 {
		t.Fatalf("drain calls = %d, want 1", drain.calls)
	}
	if result.Drained != drain.result {
		t.Fatalf("drained = %#v, want %#v", result.Drained, drain.result)
	}
	if result.Cleared != 3 {
		t.Fatalf("cleared = %d, want 3", result.Cleared)
	}
	if result.DirectMessages != 5 || result.GroupMessages != 3 {
		t.Fatalf("messages = %d/%d, want 5/3", result.DirectMessages, result.GroupMessages)
	}
	if result.DedupRecords != 8 {
		t.Fatalf("dedup records = %d, want 8", result.DedupRecords)
	}
	if result.Conversations != 2 || result.Groups != 2 {
		t.Fatalf(
			"conversations/groups = %d/%d, want 2/2",
			result.Conversations, result.Groups,
		)
	}
	if result.Watermarks != 5 {
		t.Fatalf("watermarks = %d, want 5", result.Watermarks)
	}
	if result.Duration <= 0 {
		t.Fatalf("duration = %v, want positive", result.Duration)
	}
	if exists := client.Exists(ctx, staleKey).Val(); exists != 0 {
		t.Fatalf("stale entity survived the rebuild")
	}
	if exists := client.Exists(ctx, directSequenceKey(9, 10)).Val(); exists != 0 {
		t.Fatalf("stale counter survived the rebuild")
	}

	expectString(t, client, directSequenceKey(1, 2), "3")
	expectString(t, client, directSequenceKey(2, 3), "2")
	expectString(t, client, defaultKeyspace.groupSequenceKey(7), "2")
	expectString(t, client, defaultKeyspace.groupSequenceKey(8), "1")
	expectZCard(t, client, directPairKey(1, 2), 3)
	expectZCard(t, client, directPairKey(2, 3), 2)
	expectZCard(t, client, defaultKeyspace.groupInboxKey(7), 2)
	expectZCard(t, client, defaultKeyspace.groupInboxKey(8), 1)
	expectZCard(t, client, directFeedKey(1), 3)
	expectZCard(t, client, directFeedKey(2), 5)
	expectZCard(t, client, directFeedKey(3), 2)
	expectString(t, client, defaultKeyspace.counterKey(), "8")
	expectString(t, client, defaultKeyspace.indexEpochKey(), "0")
	expectString(t, client, sequenceReadyKey(), "1")

	for _, pair := range [][2]uint64{{1, 2}, {2, 1}, {2, 3}, {3, 2}} {
		friend, err := client.SIsMember(ctx, friendshipKey(pair[0]), pair[1]).Result()
		if err != nil || !friend {
			t.Fatalf("friendship %d->%d = %v, %v", pair[0], pair[1], friend, err)
		}
	}

	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "2" {
		t.Fatalf("dm:read:1 peer 2 = %q, want 2", got)
	}
	if got := client.HGet(ctx, directReadKey(2), "1").Val(); got != "1" {
		t.Fatalf("dm:read:2 peer 1 = %q, want 1", got)
	}
	if got := client.HGet(ctx, directReadKey(3), "2").Val(); got != "1" {
		t.Fatalf("dm:read:3 peer 2 = %q, want 1", got)
	}
	if score := client.ZScore(ctx, defaultKeyspace.groupReadKey(7), "1").Val(); score != 1 {
		t.Fatalf("group 7 member 1 watermark = %v, want 1", score)
	}
	if exists := client.ZScore(ctx, defaultKeyspace.groupReadKey(7), "2").Err(); !errors.Is(exists, redis.Nil) {
		t.Fatalf("group 7 member 2 has a stored watermark")
	}
	if score := client.ZScore(ctx, defaultKeyspace.groupReadKey(8), "1").Val(); score != 1 {
		t.Fatalf("group 8 member 1 watermark = %v, want 1", score)
	}

	if got := client.HGet(ctx, messageKey(3), "body").Val(); got != "c" {
		t.Fatalf("message 3 body = %q, want c", got)
	}
	if got := client.HGet(ctx, defaultKeyspace.groupMessageKey(7), "sender_nickname").Val(); got != "n2" {
		t.Fatalf("group message 7 nickname = %q, want n2", got)
	}
	groupFields, err := client.HGetAll(ctx, defaultKeyspace.groupMessageKey(8)).Result()
	if err != nil {
		t.Fatalf("read group message 8: %v", err)
	}
	if groupFields["sender_username"] != "" || groupFields["sender_nickname"] != "" {
		t.Fatalf("group message 8 sender snapshot = %#v, want empty", groupFields)
	}
	if exists := client.Exists(ctx, dedupKey(1, 2, "c1")).Val(); exists != 1 {
		t.Fatalf("direct dedup record missing")
	}
	if exists := client.Exists(ctx, defaultKeyspace.groupDedupKey(7, 1, "g1")).Val(); exists != 1 {
		t.Fatalf("group dedup record missing")
	}
}

func TestRedisRebuildRerunIsIdentical(t *testing.T) {
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)

	first, err := runRebuild(t, client, fakeStore, drain, now, 2)
	if err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	firstCounters := readCounters(t, client)
	second, err := runRebuild(t, client, fakeStore, drain, now, 2)
	if err != nil {
		t.Fatalf("second Run() error = %v", err)
	}

	if first.DirectMessages != second.DirectMessages ||
		first.GroupMessages != second.GroupMessages ||
		first.DedupRecords != second.DedupRecords ||
		first.Watermarks != second.Watermarks ||
		first.Conversations != second.Conversations ||
		first.Groups != second.Groups {
		t.Fatalf("rerun counts differ: %#v vs %#v", first, second)
	}
	secondCounters := readCounters(t, client)
	if len(firstCounters) != len(secondCounters) {
		t.Fatalf("rerun counters differ: %#v vs %#v", firstCounters, secondCounters)
	}
	for key, value := range firstCounters {
		if secondCounters[key] != value {
			t.Fatalf("counter %s = %q, want %q", key, secondCounters[key], value)
		}
	}
	if drain.calls != 2 {
		t.Fatalf("drain calls = %d, want 2", drain.calls)
	}
}

func TestRedisRebuildPagesWithBatchOne(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)

	result, err := runRebuild(t, client, fakeStore, drain, now, 1)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Cleared != 0 {
		t.Fatalf("cleared = %d, want 0 on an empty namespace", result.Cleared)
	}
	if result.DirectMessages != 5 || result.GroupMessages != 3 {
		t.Fatalf("messages = %d/%d, want 5/3", result.DirectMessages, result.GroupMessages)
	}
	if result.Conversations != 2 || result.Groups != 2 {
		t.Fatalf(
			"conversations/groups = %d/%d, want 2/2",
			result.Conversations, result.Groups,
		)
	}
	if result.DedupRecords != 8 || result.Watermarks != 5 {
		t.Fatalf(
			"dedup/watermarks = %d/%d, want 8/5",
			result.DedupRecords, result.Watermarks,
		)
	}
	expectZCard(t, client, directPairKey(1, 2), 3)
	expectZCard(t, client, defaultKeyspace.groupInboxKey(7), 2)
	expectZCard(t, client, directFeedKey(2), 5)
	expectString(t, client, directSequenceKey(1, 2), "3")
	expectString(t, client, defaultKeyspace.counterKey(), "8")
	expectString(t, client, defaultKeyspace.indexEpochKey(), "0")
	expectString(t, client, sequenceReadyKey(), "1")
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "2" {
		t.Fatalf("dm:read:1 peer 2 = %q, want 2", got)
	}
	friend, err := client.SIsMember(ctx, friendshipKey(1), 2).Result()
	if err != nil || !friend {
		t.Fatalf("friendship 1->2 = %v, %v", friend, err)
	}
}

func TestRedisRebuildRefusesMessagesWithoutSeq(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	fakeStore.direct[0].Seq = 0
	keepKey := messageKey(999)
	if err := client.Set(ctx, keepKey, "keep", 0).Err(); err != nil {
		t.Fatalf("seed stale entity: %v", err)
	}

	_, err := runRebuild(t, client, fakeStore, drain, now, 2)
	if !errors.Is(err, ErrRebuildMissingSeq) {
		t.Fatalf("Run() error = %v, want ErrRebuildMissingSeq", err)
	}
	if !strings.Contains(err.Error(), "backfill-seq") {
		t.Fatalf("Run() error = %v, want a backfill-seq hint", err)
	}
	if drain.calls != 0 {
		t.Fatalf("drain calls = %d, want 0", drain.calls)
	}
	if exists := client.Exists(ctx, keepKey).Val(); exists != 1 {
		t.Fatalf("namespace was cleared before the NULL-seq refusal")
	}
	if exists := client.Exists(ctx, sequenceReadyKey()).Val(); exists != 0 {
		t.Fatalf("seq:ready was written after the refusal")
	}
}

type unnumberedRebuildStore struct {
	*fakeRebuildStore
}

func (u *unnumberedRebuildStore) CountUnnumberedMessages(
	context.Context,
) (uint64, uint64, error) {
	return 2, 1, nil
}

func TestRedisRebuildRefusesBeforeClearingWhenStoreReportsUnnumbered(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	keepKey := messageKey(999)
	if err := client.Set(ctx, keepKey, "keep", 0).Err(); err != nil {
		t.Fatalf("seed stale entity: %v", err)
	}

	_, err := runRebuild(t, client, &unnumberedRebuildStore{fakeRebuildStore: fakeStore}, drain, now, 2)
	if !errors.Is(err, ErrRebuildMissingSeq) {
		t.Fatalf("Run() error = %v, want ErrRebuildMissingSeq", err)
	}
	if drain.calls != 0 {
		t.Fatalf("drain calls = %d, want 0", drain.calls)
	}
	if exists := client.Exists(ctx, keepKey).Val(); exists != 1 {
		t.Fatalf("namespace was cleared before the refusal")
	}
}

func TestRedisRebuildRefusesWhileBackfilling(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	if err := client.Set(ctx, BackfillLockKey(defaultKeyspace), "token", 0).Err(); err != nil {
		t.Fatalf("seed backfill lock: %v", err)
	}

	_, err := runRebuild(t, client, fakeStore, drain, now, 2)
	if err == nil || !strings.Contains(err.Error(), "backfill") {
		t.Fatalf("Run() error = %v, want a backfill lock error", err)
	}
	if drain.calls != 0 {
		t.Fatalf("drain calls = %d, want 0", drain.calls)
	}
	if exists := client.Exists(ctx, sequenceReadyKey()).Val(); exists != 0 {
		t.Fatalf("seq:ready was written while backfill lock exists")
	}
}

func TestRedisRebuildAbortsWhenDrainFails(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	keepKey := messageKey(999)
	if err := client.Set(ctx, keepKey, "keep", 0).Err(); err != nil {
		t.Fatalf("seed stale entity: %v", err)
	}
	drainErr := errors.New("queue drain timed out")
	drain.err = drainErr

	_, err := runRebuild(t, client, fakeStore, drain, now, 2)
	if !errors.Is(err, drainErr) {
		t.Fatalf("Run() error = %v, want the drain error", err)
	}
	if exists := client.Exists(ctx, keepKey).Val(); exists != 1 {
		t.Fatalf("namespace was cleared although the drain failed")
	}
	if exists := client.Exists(ctx, sequenceReadyKey()).Val(); exists != 0 {
		t.Fatalf("seq:ready was written although the drain failed")
	}
}

func TestRedisRebuildVerifyFailureDoesNotWriteReady(t *testing.T) {
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	fakeStore.convStatsOverride = []store.ConversationSeqStat{
		{LowID: 1, HighID: 2, Count: 4, MaxSeq: 4},
		{LowID: 2, HighID: 3, Count: 2, MaxSeq: 2},
	}

	_, err := runRebuild(t, client, fakeStore, drain, now, 2)
	if err == nil {
		t.Fatalf("Run() succeeded with a mismatched conversation")
	}
	if !strings.Contains(err.Error(), "1:2") {
		t.Fatalf("Run() error = %v, want the offending conversation", err)
	}
	if exists := client.Exists(context.Background(), sequenceReadyKey()).Val(); exists != 0 {
		t.Fatalf("seq:ready was written after a failed verification")
	}
	if exists := client.Exists(context.Background(), defaultKeyspace.indexEpochKey()).Val(); exists != 0 {
		t.Fatalf("index:epoch was written before verification passed")
	}
	if exists := client.Exists(context.Background(), RebuildPendingKey(defaultKeyspace)).Val(); exists != 1 {
		t.Fatalf("rebuild:pending was cleared although the rebuild failed")
	}
}

func TestRedisRebuildHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	drain.onCall = func(context.Context) { cancel() }

	rebuilder := NewRedisRebuilder(client, fakeStore, RedisRebuildOptions{
		Keyspace:    defaultKeyspace,
		Batch:       2,
		DedupTTL:    7 * 24 * time.Hour,
		Now:         func() time.Time { return now },
		Logf:        func(string, ...interface{}) {},
		DrainQueues: drain.Drain,
	})
	_, err := rebuilder.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if exists := client.Exists(context.Background(), sequenceReadyKey()).Val(); exists != 0 {
		t.Fatalf("seq:ready was written after cancellation")
	}
}

func TestClearNamespaceKeepsLocksAndUnknownKeys(t *testing.T) {
	ctx := context.Background()
	client, _ := newProjectionTest(t)
	known := []string{
		messageKey(1),
		defaultKeyspace.groupMessageKey(2),
		directFeedKey(3),
		directSequenceKey(1, 2),
		defaultKeyspace.groupInboxKey(7),
		friendshipKey(1),
		dedupKey(1, 2, "x"),
		defaultKeyspace.groupDedupKey(7, 1, "y"),
		defaultKeyspace.counterKey(),
		defaultKeyspace.messageStreamKey(),
		defaultKeyspace.groupMessageStreamKey(),
		directReadStreamKey(),
		defaultKeyspace.groupReadStreamKey(),
		defaultKeyspace.indexEpochKey(),
		sequenceReadyKey(),
	}
	for _, key := range known {
		if err := client.Set(ctx, key, "1", 0).Err(); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	protected := []string{
		RebuildLockKey(defaultKeyspace),
		RebuildPendingKey(defaultKeyspace),
		BackfillLockKey(defaultKeyspace),
		"im:v12:unknown:key",
		"other:messages:1",
	}
	for _, key := range protected {
		if err := client.Set(ctx, key, "1", 0).Err(); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	deleted, err := ClearNamespace(ctx, client, defaultKeyspace)
	if err != nil {
		t.Fatalf("ClearNamespace() error = %v", err)
	}
	if deleted != len(known) {
		t.Fatalf("deleted = %d, want %d", deleted, len(known))
	}
	for _, key := range known {
		if exists := client.Exists(ctx, key).Val(); exists != 0 {
			t.Fatalf("known key %s survived", key)
		}
	}
	for _, key := range protected {
		if exists := client.Exists(ctx, key).Val(); exists != 1 {
			t.Fatalf("protected key %s was deleted", key)
		}
	}
}

func TestClearNamespaceUsesItsOwnPrefix(t *testing.T) {
	ctx := context.Background()
	client, _ := newProjectionTest(t)
	keyspace := NewKeyspace(mustNamespace(t, "im:v99-test"))
	other := "other:messages:1"
	if err := client.Set(ctx, keyspace.messageKey(1), "1", 0).Err(); err != nil {
		t.Fatalf("seed own key: %v", err)
	}
	if err := client.Set(ctx, other, "1", 0).Err(); err != nil {
		t.Fatalf("seed other key: %v", err)
	}

	deleted, err := ClearNamespace(ctx, client, keyspace)
	if err != nil {
		t.Fatalf("ClearNamespace() error = %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	if exists := client.Exists(ctx, other).Val(); exists != 1 {
		t.Fatalf("key outside the keyspace was deleted")
	}
}

func TestRedisRebuildToleratesSeqHoles(t *testing.T) {
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	// A dead-lettered persist leaves seq 2 of 1:2 and seq 1 of group 7 unused.
	keepDirect := make([]store.Message, 0, len(fakeStore.direct))
	for _, message := range fakeStore.direct {
		if message.ID == 2 {
			continue
		}
		keepDirect = append(keepDirect, message)
	}
	fakeStore.direct = keepDirect
	keepGroups := make([]store.GroupMessage, 0, len(fakeStore.groups))
	for _, message := range fakeStore.groups {
		if message.ID == 6 {
			continue
		}
		keepGroups = append(keepGroups, message)
	}
	fakeStore.groups = keepGroups

	result, err := runRebuild(t, client, fakeStore, drain, now, 2)
	if err != nil {
		t.Fatalf("Run() error = %v, want holes to be tolerated", err)
	}
	if result.Holes != 2 {
		t.Fatalf("holes = %d, want 2", result.Holes)
	}
	expectZCard(t, client, directPairKey(1, 2), 2)
	expectString(t, client, directSequenceKey(1, 2), "3")
	expectZCard(t, client, defaultKeyspace.groupInboxKey(7), 1)
	expectString(t, client, defaultKeyspace.groupSequenceKey(7), "2")
	expectString(t, client, sequenceReadyKey(), "1")
}

func TestRedisRebuildClampsWatermarksPastLostTail(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	// Read receipts can outlive a lost tail: durable seq 10 over a max seq of 3.
	fakeStore.convReads[0] = store.ConversationReadState{UserID: 1, PeerID: 2, LastReadSeq: 10}
	fakeStore.groupProjections[0].Positions[1] = 9

	if _, err := runRebuild(t, client, fakeStore, drain, now, 2); err != nil {
		t.Fatalf("Run() error = %v, want the watermarks clamped", err)
	}
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "3" {
		t.Fatalf("dm:read:1 peer 2 = %q, want 3", got)
	}
	if score := client.ZScore(ctx, defaultKeyspace.groupReadKey(7), "1").Val(); score != 2 {
		t.Fatalf("group 7 member 1 watermark = %v, want 2", score)
	}
	expectString(t, client, sequenceReadyKey(), "1")
}

// delFailClient fails one DEL so a test can interrupt a destructive pass.
type delFailClient struct {
	redis.UniversalClient
	failOn func(keys []string) bool
}

func (c *delFailClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	if c.failOn(keys) {
		return redis.NewIntResult(0, errors.New("injected del failure"))
	}
	return c.UniversalClient.Del(ctx, keys...)
}

// setFailClient fails one SET so a test can interrupt the final writes.
type setFailClient struct {
	redis.UniversalClient
	failOn func(key string) bool
}

func (c *setFailClient) Set(
	ctx context.Context,
	key string,
	value interface{},
	expiration time.Duration,
) *redis.StatusCmd {
	if c.failOn(key) {
		return redis.NewStatusResult("", errors.New("injected set failure"))
	}
	return c.UniversalClient.Set(ctx, key, value, expiration)
}

func TestClearNamespaceInvalidatesMarkersBeforeFailing(t *testing.T) {
	ctx := context.Background()
	client, _ := newProjectionTest(t)
	if err := client.Set(ctx, sequenceReadyKey(), "1", 0).Err(); err != nil {
		t.Fatalf("seed seq:ready: %v", err)
	}
	if err := client.Set(ctx, defaultKeyspace.indexEpochKey(), "0", 0).Err(); err != nil {
		t.Fatalf("seed index:epoch: %v", err)
	}
	if err := client.Set(ctx, dedupKey(1, 2, "x"), "1", 0).Err(); err != nil {
		t.Fatalf("seed dedup: %v", err)
	}
	failing := &delFailClient{
		UniversalClient: client,
		failOn: func(keys []string) bool {
			for _, key := range keys {
				if strings.Contains(key, "dedup:") {
					return true
				}
			}
			return false
		},
	}

	if _, err := ClearNamespace(ctx, failing, defaultKeyspace); err == nil {
		t.Fatalf("ClearNamespace() succeeded despite the injected DEL failure")
	}
	if exists := client.Exists(ctx, sequenceReadyKey()).Val(); exists != 0 {
		t.Fatalf("seq:ready survived a failed clear")
	}
	if exists := client.Exists(ctx, defaultKeyspace.indexEpochKey()).Val(); exists != 0 {
		t.Fatalf("index:epoch survived a failed clear")
	}
}

func TestRedisRebuildVerifyCatchesMissingDedup(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	fakeStore.beforeConvStats = func() {
		if err := client.Del(ctx, dedupKey(1, 2, "c1")).Err(); err != nil {
			t.Fatalf("delete dedup record: %v", err)
		}
	}

	_, err := runRebuild(t, client, fakeStore, drain, now, 2)
	if err == nil || !strings.Contains(err.Error(), "dedup") {
		t.Fatalf("Run() error = %v, want a dedup mismatch", err)
	}
	if exists := client.Exists(ctx, sequenceReadyKey()).Val(); exists != 0 {
		t.Fatalf("seq:ready was written without its dedup records")
	}
}

func TestRedisRebuildVerifyCatchesStaleConversationPreview(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	fakeStore.beforeConvStats = func() {
		if err := client.HDel(ctx, directConversationLastKey(1), "2").Err(); err != nil {
			t.Fatalf("delete conversation preview: %v", err)
		}
	}

	_, err := runRebuild(t, client, fakeStore, drain, now, 2)
	if err == nil || !strings.Contains(err.Error(), "conversation preview") {
		t.Fatalf("Run() error = %v, want a conversation preview mismatch", err)
	}
	if exists := client.Exists(ctx, sequenceReadyKey()).Val(); exists != 0 {
		t.Fatalf("seq:ready was written with a stale conversation preview")
	}
}

func TestRedisRebuildPendingMarkerLifetime(t *testing.T) {
	now := time.UnixMilli(1700000000000).UTC()

	t.Run("cleared after success", func(t *testing.T) {
		ctx := context.Background()
		client, fakeStore, drain := newRebuildTest(t, now)
		if _, err := runRebuild(t, client, fakeStore, drain, now, 2); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		if exists := client.Exists(ctx, RebuildPendingKey(defaultKeyspace)).Val(); exists != 0 {
			t.Fatalf("rebuild:pending survived a successful rebuild")
		}
	})

	t.Run("kept when the clear fails", func(t *testing.T) {
		ctx := context.Background()
		client, fakeStore, drain := newRebuildTest(t, now)
		if err := client.Set(ctx, sequenceReadyKey(), "1", 0).Err(); err != nil {
			t.Fatalf("seed stale seq:ready: %v", err)
		}
		if err := client.Set(ctx, dedupKey(1, 2, "stale"), "1", 0).Err(); err != nil {
			t.Fatalf("seed stale dedup: %v", err)
		}
		failing := &delFailClient{
			UniversalClient: client,
			failOn: func(keys []string) bool {
				for _, key := range keys {
					if strings.Contains(key, "dedup:") {
						return true
					}
				}
				return false
			},
		}
		rebuilder := NewRedisRebuilder(failing, fakeStore, RedisRebuildOptions{
			Keyspace:    defaultKeyspace,
			Batch:       2,
			DedupTTL:    7 * 24 * time.Hour,
			Now:         func() time.Time { return now },
			Logf:        func(string, ...interface{}) {},
			DrainQueues: drain.Drain,
		})

		if _, err := rebuilder.Run(ctx); err == nil {
			t.Fatalf("Run() succeeded despite the injected DEL failure")
		}
		if exists := client.Exists(ctx, RebuildPendingKey(defaultKeyspace)).Val(); exists != 1 {
			t.Fatalf("rebuild:pending was removed by a failed rebuild")
		}
		if exists := client.Exists(ctx, sequenceReadyKey()).Val(); exists != 0 {
			t.Fatalf("seq:ready survived a failed clear")
		}
	})
}

func TestRedisRebuildClearsEpochWhenReadyWriteFails(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	client, fakeStore, drain := newRebuildTest(t, now)
	failing := &setFailClient{
		UniversalClient: client,
		failOn: func(key string) bool {
			return key == sequenceReadyKey()
		},
	}
	rebuilder := NewRedisRebuilder(failing, fakeStore, RedisRebuildOptions{
		Keyspace:    defaultKeyspace,
		Batch:       2,
		DedupTTL:    7 * 24 * time.Hour,
		Now:         func() time.Time { return now },
		Logf:        func(string, ...interface{}) {},
		DrainQueues: drain.Drain,
	})

	if _, err := rebuilder.Run(ctx); err == nil {
		t.Fatalf("Run() succeeded despite the injected SET failure")
	}
	if exists := client.Exists(ctx, defaultKeyspace.indexEpochKey()).Val(); exists != 0 {
		t.Fatalf("index:epoch survived a failed seq:ready write")
	}
	if exists := client.Exists(ctx, sequenceReadyKey()).Val(); exists != 0 {
		t.Fatalf("seq:ready exists after the injected SET failure")
	}
}

func readCounters(t *testing.T, client *redis.Client) map[string]string {
	t.Helper()
	ctx := context.Background()
	keys := map[string]string{
		"direct:1:2": directSequenceKey(1, 2),
		"direct:2:3": directSequenceKey(2, 3),
		"group:7":    defaultKeyspace.groupSequenceKey(7),
		"group:8":    defaultKeyspace.groupSequenceKey(8),
		"next_id":    defaultKeyspace.counterKey(),
	}
	result := make(map[string]string, len(keys))
	for name, key := range keys {
		result[name] = client.Get(ctx, key).Val()
	}
	return result
}

func expectString(t *testing.T, client *redis.Client, key, want string) {
	t.Helper()
	if got := client.Get(context.Background(), key).Val(); got != want {
		t.Fatalf("%s = %q, want %q", key, got, want)
	}
}

func expectZCard(t *testing.T, client *redis.Client, key string, want int64) {
	t.Helper()
	if got := client.ZCard(context.Background(), key).Val(); got != want {
		t.Fatalf("%s zcard = %d, want %d", key, got, want)
	}
}

func mustNamespace(t *testing.T, base string) Namespace {
	t.Helper()
	namespace, err := NewNamespace(base)
	if err != nil {
		t.Fatalf("NewNamespace(%q) error = %v", base, err)
	}
	return namespace
}
