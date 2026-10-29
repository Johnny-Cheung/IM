package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// recordingRepairer wraps the real projection writer so a test can assert the
// exact batches and their order while Redis still receives the real shapes.
type recordingRepairer struct {
	inner ProjectionRepairer

	mu      sync.Mutex
	direct  [][]store.Message
	group   [][]store.GroupMessage
	directW [][]store.ConversationReadState
	groupW  [][]store.GroupMemberReadState
	directC [][3]uint64
	groupC  [][2]uint64
}

func (r *recordingRepairer) WriteDirectBatch(ctx context.Context, messages []store.Message) (int, error) {
	r.mu.Lock()
	r.direct = append(r.direct, append([]store.Message(nil), messages...))
	r.mu.Unlock()
	return r.inner.WriteDirectBatch(ctx, messages)
}

func (r *recordingRepairer) WriteGroupBatch(ctx context.Context, messages []store.GroupMessage) (int, error) {
	r.mu.Lock()
	r.group = append(r.group, append([]store.GroupMessage(nil), messages...))
	r.mu.Unlock()
	return r.inner.WriteGroupBatch(ctx, messages)
}

func (r *recordingRepairer) RaiseDirectReadWatermarks(ctx context.Context, states []store.ConversationReadState) (int, error) {
	r.mu.Lock()
	r.directW = append(r.directW, append([]store.ConversationReadState(nil), states...))
	r.mu.Unlock()
	return r.inner.RaiseDirectReadWatermarks(ctx, states)
}

func (r *recordingRepairer) RaiseGroupReadWatermarks(ctx context.Context, states []store.GroupMemberReadState) (int, error) {
	r.mu.Lock()
	r.groupW = append(r.groupW, append([]store.GroupMemberReadState(nil), states...))
	r.mu.Unlock()
	return r.inner.RaiseGroupReadWatermarks(ctx, states)
}

func (r *recordingRepairer) RaiseDirectCounter(ctx context.Context, lowID, highID, seq uint64) error {
	r.mu.Lock()
	r.directC = append(r.directC, [3]uint64{lowID, highID, seq})
	r.mu.Unlock()
	return r.inner.RaiseDirectCounter(ctx, lowID, highID, seq)
}

func (r *recordingRepairer) RaiseGroupCounter(ctx context.Context, groupID, seq uint64) error {
	r.mu.Lock()
	r.groupC = append(r.groupC, [2]uint64{groupID, seq})
	r.mu.Unlock()
	return r.inner.RaiseGroupCounter(ctx, groupID, seq)
}

func (r *recordingRepairer) directSeqs() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var seqs []uint64
	for _, batch := range r.direct {
		for _, message := range batch {
			seqs = append(seqs, message.Seq)
		}
	}
	return seqs
}

func newAuditWriter(client redis.UniversalClient, now time.Time) *ProjectionWriter {
	return NewProjectionWriter(client, defaultKeyspace, ProjectionWriteOptions{
		Now: func() time.Time { return now },
	})
}

func auditDirectMessages(count int, createdAt time.Time) []store.Message {
	messages := make([]store.Message, 0, count)
	for seq := 1; seq <= count; seq++ {
		messages = append(messages, store.Message{
			ID:              uint64(100 + seq),
			Seq:             uint64(seq),
			SenderID:        1,
			RecipientID:     2,
			ClientMessageID: fmt.Sprintf("cm-%d", seq),
			Body:            fmt.Sprintf("body-%d", seq),
			CreatedAt:       createdAt,
		})
	}
	return messages
}

func auditGroupMessages(count int, createdAt time.Time) []store.GroupMessage {
	messages := make([]store.GroupMessage, 0, count)
	for seq := 1; seq <= count; seq++ {
		messages = append(messages, store.GroupMessage{
			ID:              uint64(200 + seq),
			Seq:             uint64(seq),
			GroupID:         7,
			SenderID:        42,
			Sender:          store.PublicUser{Username: "alice", Nickname: "Alice"},
			ClientMessageID: fmt.Sprintf("gcm-%d", seq),
			Body:            fmt.Sprintf("gbody-%d", seq),
			CreatedAt:       createdAt,
		})
	}
	return messages
}

func TestReconcilerRaisesRedisCounter(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	setDirectSequence(t, client, 1, 2, 2)
	for seq := uint64(1); seq <= 3; seq++ {
		seedReconcileDirectIndex(t, client, 1, 2, 100+seq, seq)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 3, MaxSeq: 3}}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
	})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.CountersRaised != 1 || result.RedisMissing != 1 || result.RedisRepaired != 1 {
		t.Fatalf("result = %+v, want one counter raised", result)
	}
	if result.Mismatched != 0 || result.Repaired != 0 {
		t.Fatalf("result = %+v, want the forward direction untouched", result)
	}
	counter, err := client.Get(context.Background(), defaultKeyspace.directSequenceKey(1, 2)).Uint64()
	if err != nil {
		t.Fatalf("read counter: %v", err)
	}
	if counter != 3 {
		t.Fatalf("counter = %d, want 3", counter)
	}
	if status := reconciler.Status(); status.RedisRepaired != 1 || status.WatermarksRaised != 0 {
		t.Fatalf("status = %+v", status)
	}
}

func TestReconcilerRepairsMissingRedisIndex(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	createdAt := now.Add(-30 * time.Minute)
	messages := auditDirectMessages(5, createdAt)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 5, MaxSeq: 5}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages

	writer := newAuditWriter(client, now)
	ctx := context.Background()
	if _, err := writer.WriteDirectBatch(ctx, []store.Message{messages[0], messages[2]}); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}
	setDirectSequence(t, client, 1, 2, 5)

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{Repair: writer})
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.RedisMissing != 3 || result.RedisRepaired != 3 {
		t.Fatalf("result = %+v, want 3 missing and 3 repaired", result)
	}
	if result.Mismatched != 0 || result.Repaired != 0 {
		t.Fatalf("result = %+v, want the forward direction untouched", result)
	}
	if result.NeedsRebuild != 0 {
		t.Fatalf("result = %+v, want no rebuild flag", result)
	}
	card, err := client.ZCard(ctx, defaultKeyspace.directPairKey(1, 2)).Result()
	if err != nil {
		t.Fatalf("read pair card: %v", err)
	}
	if card != 5 {
		t.Fatalf("pair card = %d, want 5", card)
	}
	// Entity fields must come from the MySQL row, not from the index.
	fields, err := client.HGetAll(ctx, defaultKeyspace.messageKey(102)).Result()
	if err != nil {
		t.Fatalf("read repaired entity: %v", err)
	}
	if fields["body"] != "body-2" || fields["client_message_id"] != "cm-2" ||
		fields["sender_id"] != "1" || fields["recipient_id"] != "2" ||
		fields["seq"] != "2" ||
		fields["created_at_ms"] != fmt.Sprint(createdAt.UnixMilli()) {
		t.Fatalf("repaired entity = %v", fields)
	}
}

func TestReconcilerNeedsRebuildForLargeIndexGap(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	setDirectSequence(t, client, 1, 2, 300)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 300, MaxSeq: 300}}
	logs := captureReconcileLog(t)

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
	})
	for run := 0; run < 2; run++ {
		result, err := reconciler.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("RunOnce() run %d error = %v", run, err)
		}
		if result.NeedsRebuild != 1 || result.RedisRepaired != 0 {
			t.Fatalf("run %d result = %+v, want one rebuild flag and no repair", run, result)
		}
		if result.RedisMissing != 300 {
			t.Fatalf("run %d redis_missing = %d, want 300", run, result.RedisMissing)
		}
	}
	if fakeStore.directRangeCalls() != 0 {
		t.Fatalf("seq range calls = %d, want no scan past the cap", fakeStore.directRangeCalls())
	}
	marker := "reconcile needs-rebuild direct:1:2:index"
	if got := strings.Count(logs.String(), marker); got != 1 {
		t.Fatalf("needs-rebuild log lines = %d, want 1: %s", got, logs.String())
	}
}

func TestReconcilerRepairLimitDefersToNextRun(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	messages := auditDirectMessages(5, now.Add(-30*time.Minute))

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 5, MaxSeq: 5}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages
	writer := newAuditWriter(client, now)
	ctx := context.Background()
	if _, err := writer.WriteDirectBatch(ctx, []store.Message{messages[0], messages[2]}); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}
	setDirectSequence(t, client, 1, 2, 5)

	repair := &recordingRepairer{inner: writer}
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:      repair,
		RepairLimit: 2,
	})
	first, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("first RunOnce() error = %v", err)
	}
	if first.RedisRepaired != 2 {
		t.Fatalf("first result = %+v, want 2 repaired", first)
	}
	if got := repair.directSeqs(); len(got) != 2 || got[0] != 2 || got[1] != 4 {
		t.Fatalf("first repaired seqs = %v, want [2 4]", got)
	}

	second, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if second.RedisRepaired != 1 {
		t.Fatalf("second result = %+v, want the deferred message repaired", second)
	}
	if got := repair.directSeqs(); len(got) != 3 || got[2] != 5 {
		t.Fatalf("repaired seqs = %v, want [2 4 5]", got)
	}
}

func TestReconcilerRaisesReadWatermarks(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()

	if err := client.HSet(ctx, defaultKeyspace.directReadKey(1), "2", 3).Err(); err != nil {
		t.Fatalf("seed direct watermark: %v", err)
	}
	if err := client.HSet(ctx, defaultKeyspace.directReadKey(3), "4", 7).Err(); err != nil {
		t.Fatalf("seed newer direct watermark: %v", err)
	}
	if err := client.ZAdd(ctx, defaultKeyspace.groupReadKey(7), redis.Z{
		Score: 2, Member: "9",
	}).Err(); err != nil {
		t.Fatalf("seed group watermark: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversationReads = []store.ConversationReadState{
		{UserID: 1, PeerID: 2, LastReadSeq: 5},
		{UserID: 3, PeerID: 4, LastReadSeq: 2},
	}
	fakeStore.groupReads = []store.GroupMemberReadState{
		{GroupID: 7, UserID: 9, LastReadSeq: 4},
	}
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
	})
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.WatermarksRaised != 2 || result.RedisMissing != 2 {
		t.Fatalf("result = %+v, want two watermarks raised", result)
	}
	if got := client.HGet(ctx, defaultKeyspace.directReadKey(1), "2").Val(); got != "5" {
		t.Fatalf("direct watermark = %q, want 5", got)
	}
	if got := client.HGet(ctx, defaultKeyspace.directReadKey(3), "4").Val(); got != "7" {
		t.Fatalf("newer direct watermark = %q, want it untouched at 7", got)
	}
	if score := client.ZScore(ctx, defaultKeyspace.groupReadKey(7), "9").Val(); score != 4 {
		t.Fatalf("group watermark = %v, want 4", score)
	}
}

func TestReconcilerDeepAuditRepairsMissingEntity(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditDirectMessages(5, now.Add(-time.Hour))

	writer := newAuditWriter(client, now)
	if _, err := writer.WriteDirectBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}
	if err := client.Del(ctx, defaultKeyspace.messageKey(101)).Err(); err != nil {
		t.Fatalf("delete entity: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 5, MaxSeq: 5}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
		Now:    func() time.Time { return current },
	})
	current = now.Add(2 * time.Hour)
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.RedisRepaired != 1 || result.RedisMissing < 1 {
		t.Fatalf("result = %+v, want the sampled entity repaired", result)
	}
	fields, err := client.HGetAll(ctx, defaultKeyspace.messageKey(101)).Result()
	if err != nil {
		t.Fatalf("read entity: %v", err)
	}
	if fields["body"] != "body-1" || fields["seq"] != "1" || fields["id"] != "101" {
		t.Fatalf("repaired entity = %v", fields)
	}
	status := reconciler.Status()
	if !status.LastDeepAuditAt.Equal(current) || status.DeepAuditError != "" {
		t.Fatalf("status = %+v, want the deep audit recorded", status)
	}
}

func TestReconcilerDeepAuditRepairsSmallFeedGap(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditDirectMessages(3, now.Add(-time.Hour))

	fakeStore := newFakeReconcileStore()
	fakeStore.directMessages[[2]uint64{1, 2}] = messages
	fakeStore.userIDs = []uint64{1}
	fakeStore.userPeers[1] = []uint64{2}
	if err := client.ZAdd(ctx, defaultKeyspace.directFeedKey(1), redis.Z{
		Score: 101, Member: "101",
	}).Err(); err != nil {
		t.Fatalf("seed feed: %v", err)
	}

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:         newAuditWriter(client, now),
		DeepAuditUsers: 1,
		Now:            func() time.Time { return current },
	})
	current = now.Add(2 * time.Hour)
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.FeedRepairs != 2 || result.RedisMissing != 2 {
		t.Fatalf("result = %+v, want two feed entries repaired", result)
	}
	card, err := client.ZCard(ctx, defaultKeyspace.directFeedKey(1)).Result()
	if err != nil {
		t.Fatalf("read feed card: %v", err)
	}
	if card != 3 {
		t.Fatalf("feed card = %d, want 3", card)
	}
}

func TestReconcilerDeepAuditLargeFeedGapNeedsRebuild(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	messages := auditDirectMessages(51, now.Add(-time.Hour))

	fakeStore := newFakeReconcileStore()
	fakeStore.directMessages[[2]uint64{1, 2}] = messages
	fakeStore.userIDs = []uint64{1}
	fakeStore.userPeers[1] = []uint64{2}
	logs := captureReconcileLog(t)

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:         newAuditWriter(client, now),
		DeepAuditUsers: 1,
		Now:            func() time.Time { return current },
	})
	current = now.Add(2 * time.Hour)
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.NeedsRebuild != 1 || result.FeedRepairs != 0 {
		t.Fatalf("result = %+v, want needs_rebuild without repair", result)
	}
	if result.RedisMissing != 51 {
		t.Fatalf("redis_missing = %d, want 51", result.RedisMissing)
	}
	if card := client.ZCard(context.Background(), defaultKeyspace.directFeedKey(1)).Val(); card != 0 {
		t.Fatalf("feed card = %d, want no repair", card)
	}
	if !strings.Contains(logs.String(), "needs-rebuild user:1:feed") {
		t.Fatalf("log = %s, want a needs-rebuild line", logs.String())
	}
}

func TestReconcilerDeepAuditRotatesUsers(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()

	fakeStore := newFakeReconcileStore()
	fakeStore.userIDs = []uint64{1, 2}

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:         newAuditWriter(client, now),
		DeepAuditUsers: 1,
		Now:            func() time.Time { return current },
	})
	current = now.Add(2 * time.Hour)
	if _, err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("first RunOnce() error = %v", err)
	}
	if got := fakeStore.userCountCallsSnapshot(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("first audit users = %v, want [1]", got)
	}

	current = now.Add(3 * time.Hour)
	if _, err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if got := fakeStore.userCountCallsSnapshot(); len(got) != 2 || got[1] != 2 {
		t.Fatalf("audited users = %v, want the next user on the next run", got)
	}
}

func TestReconcilerNilRepairReportsWithoutWriting(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditDirectMessages(3, now.Add(-time.Hour))

	setDirectSequence(t, client, 1, 2, 3)
	seedReconcileDirectIndex(t, client, 1, 2, 101, 1)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 3, MaxSeq: 3}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages
	fakeStore.conversationReads = []store.ConversationReadState{{UserID: 1, PeerID: 2, LastReadSeq: 3}}
	fakeStore.userIDs = []uint64{1}
	fakeStore.userPeers[1] = []uint64{2}

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		DeepAuditUsers: 1,
		Now:            func() time.Time { return current },
	})
	current = now.Add(2 * time.Hour)
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.RedisMissing == 0 {
		t.Fatalf("result = %+v, want the defects reported", result)
	}
	if result.RedisRepaired != 0 || result.WatermarksRaised != 0 || result.FeedRepairs != 0 {
		t.Fatalf("result = %+v, want no writes with a nil repairer", result)
	}
	if card := client.ZCard(ctx, defaultKeyspace.directPairKey(1, 2)).Val(); card != 1 {
		t.Fatalf("pair card = %d, want 1", card)
	}
	if card := client.ZCard(ctx, defaultKeyspace.directFeedKey(1)).Val(); card != 0 {
		t.Fatalf("feed card = %d, want 0", card)
	}
	if got := client.HGet(ctx, defaultKeyspace.directReadKey(1), "2").Val(); got != "" {
		t.Fatalf("direct watermark = %q, want it untouched", got)
	}
}

func TestReconcilerNeedsRebuildRatio(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	setDirectSequence(t, client, 1, 2, 100)
	for seq := uint64(1); seq <= 75; seq++ {
		seedReconcileDirectIndex(t, client, 1, 2, 1000+seq, seq)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 100, MaxSeq: 100}}
	logs := captureReconcileLog(t)

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
	})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	// 25 of 100 is above the 20% ratio and above the 20-message floor.
	if result.NeedsRebuild != 1 || result.RedisRepaired != 0 || result.RedisMissing != 25 {
		t.Fatalf("result = %+v, want a ratio-based needs_rebuild", result)
	}
	if fakeStore.directRangeCalls() != 0 {
		t.Fatalf("seq range calls = %d, want no repair scan", fakeStore.directRangeCalls())
	}
	if !strings.Contains(logs.String(), "needs-rebuild direct:1:2:index") {
		t.Fatalf("log = %s, want a needs-rebuild line", logs.String())
	}
}

func TestReconcilerDeepAuditDoesNotOverwriteDivergedEntity(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditDirectMessages(3, now.Add(-time.Hour))

	writer := newAuditWriter(client, now)
	if _, err := writer.WriteDirectBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}
	if err := client.HSet(ctx, defaultKeyspace.messageKey(101), "body", "tampered").Err(); err != nil {
		t.Fatalf("tamper entity: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 3, MaxSeq: 3}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
		Now:    func() time.Time { return current },
	})
	current = now.Add(2 * time.Hour)
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.NeedsRebuild != 1 || result.RedisRepaired != 0 {
		t.Fatalf("result = %+v, want needs_rebuild without overwriting", result)
	}
	if got := client.HGet(ctx, defaultKeyspace.messageKey(101), "body").Val(); got != "tampered" {
		t.Fatalf("entity body = %q, want the existing value untouched", got)
	}
}

func TestReconcilerDeepAuditErrorIsRecorded(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	messages := auditDirectMessages(2, now.Add(-time.Hour))
	setDirectSequence(t, client, 1, 2, 2)
	seedReconcileDirectIndex(t, client, 1, 2, 101, 1)
	seedReconcileDirectIndex(t, client, 1, 2, 102, 2)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 2, MaxSeq: 2}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages
	fakeStore.rangeErr = fmt.Errorf("store unavailable")

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
		Now:    func() time.Time { return current },
	})
	current = now.Add(2 * time.Hour)
	_, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v, want a recorded audit error", err)
	}
	status := reconciler.Status()
	if !strings.Contains(status.DeepAuditError, "store unavailable") {
		t.Fatalf("status = %+v, want the audit error recorded", status)
	}
	if !status.LastDeepAuditAt.Equal(current) {
		t.Fatalf("LastDeepAuditAt = %v, want %v", status.LastDeepAuditAt, current)
	}
}

func TestReconcilerRepairsMissingGroupInbox(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditGroupMessages(3, now.Add(-time.Hour))

	fakeStore := newFakeReconcileStore()
	fakeStore.groups = []store.GroupSeqStat{{GroupID: 7, Count: 3, MaxSeq: 3}}
	fakeStore.groupMessages[7] = messages

	writer := newAuditWriter(client, now)
	if _, err := writer.WriteGroupBatch(ctx, []store.GroupMessage{messages[0]}); err != nil {
		t.Fatalf("seed group inbox: %v", err)
	}
	setGroupSequence(t, client, 7, 2)

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{Repair: writer})
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.CountersRaised != 1 || result.RedisRepaired != 3 || result.RedisMissing != 3 {
		t.Fatalf("result = %+v, want the counter and two messages repaired", result)
	}
	if counter := client.Get(ctx, defaultKeyspace.groupSequenceKey(7)).Val(); counter != "3" {
		t.Fatalf("group counter = %q, want 3", counter)
	}
	if card := client.ZCard(ctx, defaultKeyspace.groupInboxKey(7)).Val(); card != 3 {
		t.Fatalf("group inbox card = %d, want 3", card)
	}
	fields, err := client.HGetAll(ctx, defaultKeyspace.groupMessageKey(202)).Result()
	if err != nil {
		t.Fatalf("read group entity: %v", err)
	}
	if fields["body"] != "gbody-2" || fields["group_id"] != "7" ||
		fields["sender_id"] != "42" || fields["sender_nickname"] != "Alice" {
		t.Fatalf("repaired group entity = %v", fields)
	}
}

func TestReconcileStatusReverseFieldsJSON(t *testing.T) {
	status := ReconcileStatus{
		RedisMissing:     1,
		RedisRepaired:    2,
		WatermarksRaised: 3,
		FeedRepairs:      4,
		NeedsRebuild:     5,
		LastDeepAuditAt:  reconcileTestNow(),
		DeepAuditError:   "boom",
	}
	raw, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	for _, key := range []string{
		"redis_missing",
		"redis_repaired",
		"watermarks_raised",
		"feed_repairs",
		"needs_rebuild",
		"last_deep_audit_at",
		"deep_audit_error",
	} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("status JSON %s misses %q", raw, key)
		}
	}
}
