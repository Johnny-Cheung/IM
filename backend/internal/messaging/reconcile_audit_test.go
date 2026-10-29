package messaging

import (
	"context"
	"encoding/json"
	"errors"
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
	global  []uint64
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

func (r *recordingRepairer) RaiseGlobalMessageID(ctx context.Context, messageID uint64) (bool, error) {
	r.mu.Lock()
	r.global = append(r.global, messageID)
	r.mu.Unlock()
	return r.inner.RaiseGlobalMessageID(ctx, messageID)
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

// controlledRepairer wraps a ProjectionRepairer and injects failures on demand.
// It deliberately does not forward the optional index-only methods, so callers
// that use it exercise the plain ProjectionRepairer path.
type controlledRepairer struct {
	ProjectionRepairer

	mu             sync.Mutex
	failWatermarks bool
}

func (r *controlledRepairer) setFailWatermarks(fail bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failWatermarks = fail
}

func (r *controlledRepairer) RaiseDirectReadWatermarks(
	ctx context.Context,
	states []store.ConversationReadState,
) (int, error) {
	r.mu.Lock()
	fail := r.failWatermarks
	r.mu.Unlock()
	if fail {
		return 0, fmt.Errorf("redis unavailable")
	}
	return r.ProjectionRepairer.RaiseDirectReadWatermarks(ctx, states)
}

func (r *controlledRepairer) RaiseGroupReadWatermarks(
	ctx context.Context,
	states []store.GroupMemberReadState,
) (int, error) {
	r.mu.Lock()
	fail := r.failWatermarks
	r.mu.Unlock()
	if fail {
		return 0, fmt.Errorf("redis unavailable")
	}
	return r.ProjectionRepairer.RaiseGroupReadWatermarks(ctx, states)
}

// A conversation whose counter key alone was lost must have it raised from
// MySQL even though the index is intact; otherwise every send fails with
// seq_unavailable.
func TestReconcilerRaisesMissingDirectCounter(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	for seq := uint64(1); seq <= 3; seq++ {
		seedReconcileDirectIndex(t, client, 1, 2, 100+seq, seq)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 3, MaxSeq: 3}}
	repair := &recordingRepairer{inner: newAuditWriter(client, now)}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{Repair: repair})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.CountersRaised != 1 || result.RedisRepaired != 1 || result.NeedsRebuild != 0 {
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
}

// Losing both the counter and the index must restore the counter and small
// amount of message data in the same pass.
func TestReconcilerRepairsMissingCounterAndEmptyIndex(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	messages := auditDirectMessages(3, now.Add(-time.Hour))

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 3, MaxSeq: 3}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
	})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.CountersRaised != 1 || result.RedisRepaired != 4 || result.NeedsRebuild != 0 {
		t.Fatalf("result = %+v, want the counter and three messages repaired", result)
	}
	if counter := client.Get(context.Background(), defaultKeyspace.directSequenceKey(1, 2)).Val(); counter != "3" {
		t.Fatalf("counter = %q, want 3", counter)
	}
	if card := client.ZCard(context.Background(), defaultKeyspace.directPairKey(1, 2)).Val(); card != 3 {
		t.Fatalf("pair card = %d, want 3", card)
	}
	if body := client.HGet(context.Background(), defaultKeyspace.messageKey(101), "body").Val(); body != "body-1" {
		t.Fatalf("entity body = %q, want body-1", body)
	}
}

// A large loss is reported for a rebuild but the missing counter is still
// raised, so a rebuild decision is not confused with a broken send path.
func TestReconcilerMissingCounterLargeEmptyIndexNeedsRebuild(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 300, MaxSeq: 300}}
	logs := captureReconcileLog(t)

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
	})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.CountersRaised != 1 || result.NeedsRebuild != 1 || result.RedisRepaired != 1 {
		t.Fatalf("result = %+v, want only the counter raised and a rebuild flag", result)
	}
	if counter := client.Get(context.Background(), defaultKeyspace.directSequenceKey(1, 2)).Val(); counter != "300" {
		t.Fatalf("counter = %q, want 300", counter)
	}
	if fakeStore.directRangeCalls() != 0 {
		t.Fatalf("seq range calls = %d, want no repair scan above the cap", fakeStore.directRangeCalls())
	}
	if !strings.Contains(logs.String(), "needs-rebuild direct:1:2:index") {
		t.Fatalf("log = %s, want the rebuild warning", logs.String())
	}
}

func TestReconcilerRaisesMissingGroupCounter(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	for seq := uint64(1); seq <= 2; seq++ {
		seedReconcileGroupIndex(t, client, 7, 200+seq, seq)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.groups = []store.GroupSeqStat{{GroupID: 7, Count: 2, MaxSeq: 2}}
	repair := &recordingRepairer{inner: newAuditWriter(client, now)}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{Repair: repair})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.CountersRaised != 1 || result.RedisRepaired != 1 {
		t.Fatalf("result = %+v, want the group counter raised", result)
	}
	if counter := client.Get(context.Background(), defaultKeyspace.groupSequenceKey(7)).Val(); counter != "2" {
		t.Fatalf("group counter = %q, want 2", counter)
	}
}

// The hourly sample must rotate through the middle of a conversation, not only
// its oldest and newest messages.
func TestReconcilerDeepAuditRepairsRotatingMiddleEntity(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditDirectMessages(5, now.Add(-time.Hour))

	writer := newAuditWriter(client, now)
	if _, err := writer.WriteDirectBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}
	// seq 2 is neither the oldest nor the newest message with a boundary of 1.
	if err := client.Del(ctx, defaultKeyspace.messageKey(102)).Err(); err != nil {
		t.Fatalf("delete middle entity: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 5, MaxSeq: 5}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:            newAuditWriter(client, now),
		DeepAuditBoundary: 1,
		Now:               func() time.Time { return current },
	})
	current = now.Add(2 * time.Hour)
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.RedisRepaired != 1 {
		t.Fatalf("result = %+v, want the middle entity repaired", result)
	}
	if body := client.HGet(ctx, defaultKeyspace.messageKey(102), "body").Val(); body != "body-2" {
		t.Fatalf("middle entity body = %q, want body-2", body)
	}
}

func TestReconcilerDeepAuditRepairsRotatingMiddleGroupEntity(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditGroupMessages(5, now.Add(-time.Hour))

	writer := newAuditWriter(client, now)
	if _, err := writer.WriteGroupBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}
	if err := client.Del(ctx, defaultKeyspace.groupMessageKey(202)).Err(); err != nil {
		t.Fatalf("delete middle group entity: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.groups = []store.GroupSeqStat{{GroupID: 7, Count: 5, MaxSeq: 5}}
	fakeStore.groupMessages[7] = messages

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:            newAuditWriter(client, now),
		DeepAuditBoundary: 1,
		Now:               func() time.Time { return current },
	})
	current = now.Add(2 * time.Hour)
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.RedisRepaired != 1 {
		t.Fatalf("result = %+v, want the middle group entity repaired", result)
	}
	if body := client.HGet(ctx, defaultKeyspace.groupMessageKey(202), "body").Val(); body != "gbody-2" {
		t.Fatalf("middle group entity body = %q, want gbody-2", body)
	}
}

// The sampled user's conversation previews are compared with MySQL and
// restored when the newest entity is present.
func TestReconcilerDeepAuditRepairsConversationPreview(t *testing.T) {
	cases := []struct {
		name         string
		breakPreview func(t *testing.T, ctx context.Context, client redis.UniversalClient)
	}{
		{
			name: "absent preview",
			breakPreview: func(t *testing.T, ctx context.Context, client redis.UniversalClient) {
				t.Helper()
				if err := client.HDel(ctx, defaultKeyspace.directConversationLastKey(1), "2").Err(); err != nil {
					t.Fatalf("delete preview: %v", err)
				}
			},
		},
		{
			name: "stale preview",
			breakPreview: func(t *testing.T, ctx context.Context, client redis.UniversalClient) {
				t.Helper()
				if err := client.HSet(ctx, defaultKeyspace.directConversationLastKey(1), "2", 101).Err(); err != nil {
					t.Fatalf("stale preview: %v", err)
				}
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			client, _ := newReconcileTestClient(t)
			now := reconcileTestNow()
			ctx := context.Background()
			messages := auditDirectMessages(3, now.Add(-time.Hour))
			writer := newAuditWriter(client, now)
			if _, err := writer.WriteDirectBatch(ctx, messages); err != nil {
				t.Fatalf("seed Redis: %v", err)
			}
			testCase.breakPreview(t, ctx, client)

			fakeStore := newFakeReconcileStore()
			fakeStore.directMessages[[2]uint64{1, 2}] = messages
			fakeStore.userIDs = []uint64{1}
			fakeStore.userConversations = map[uint64][]store.Conversation{
				1: {{UserID: 2, LastMessage: &messages[2]}},
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
			if result.RedisMissing < 1 || result.RedisRepaired < 1 {
				t.Fatalf("result = %+v, want the preview detected and repaired", result)
			}
			if preview := client.HGet(ctx, defaultKeyspace.directConversationLastKey(1), "2").Val(); preview != "103" {
				t.Fatalf("preview = %q, want the newest message 103", preview)
			}
		})
	}
}

// A preview whose newest message entity is gone cannot be restored; it must be
// reported as needing a rebuild rather than pointing at a missing entity.
func TestReconcilerDeepAuditConversationPreviewEntityMissingNeedsRebuild(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditDirectMessages(3, now.Add(-time.Hour))
	writer := newAuditWriter(client, now)
	if _, err := writer.WriteDirectBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}
	if err := client.Del(ctx, defaultKeyspace.messageKey(103)).Err(); err != nil {
		t.Fatalf("delete newest entity: %v", err)
	}
	if err := client.HDel(ctx, defaultKeyspace.directConversationLastKey(1), "2").Err(); err != nil {
		t.Fatalf("delete preview: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{
		LowID: 1, HighID: 2, Count: 3, MaxSeq: 3,
	}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1, 2, 3}
	fakeStore.userIDs = []uint64{1}
	fakeStore.userConversations = map[uint64][]store.Conversation{
		1: {
			{UserID: 2, LastMessage: &messages[2]},
		},
	}

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
	if result.NeedsRebuild != 1 {
		t.Fatalf("result = %+v, want the missing preview entity flagged", result)
	}
	if preview := client.HGet(ctx, defaultKeyspace.directConversationLastKey(1), "2").Val(); preview != "" {
		t.Fatalf("preview = %q, want it left absent", preview)
	}
}

// A sampled repair deferred by the budget must keep the deep-audit cursor at the
// previous conversation so the next hourly audit retries it instead of waiting a
// full rotation.
func TestReconcilerDeepAuditRetriesDeferredConversation(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	writer := newAuditWriter(client, now)
	fakeStore := newFakeReconcileStore()

	const (
		conversations    = 60
		repairFirst      = 2
		missingEntitySeq = 1
	)
	for index := 1; index <= conversations; index++ {
		low := uint64(2*index - 1)
		high := low + 1
		pair := [2]uint64{low, high}
		switch {
		case index <= repairFirst:
			messages := make([]store.Message, 0, 3)
			for seq := uint64(1); seq <= 3; seq++ {
				messages = append(messages, store.Message{
					ID:              uint64(100000*index) + seq,
					Seq:             seq,
					SenderID:        low,
					RecipientID:     high,
					ClientMessageID: fmt.Sprintf("c-%d-%d", index, seq),
					Body:            fmt.Sprintf("body-%d-%d", index, seq),
					CreatedAt:       now.Add(-time.Hour),
				})
			}
			if _, err := writer.WriteDirectBatch(ctx, messages); err != nil {
				t.Fatalf("seed conversation %d: %v", index, err)
			}
			fakeStore.directMessages[pair] = messages
			fakeStore.conversations = append(fakeStore.conversations, store.ConversationSeqStat{
				LowID: low, HighID: high, Count: 3, MaxSeq: 3,
			})
			if err := client.Del(ctx, defaultKeyspace.messageKey(messages[missingEntitySeq-1].ID)).Err(); err != nil {
				t.Fatalf("delete sampled entity: %v", err)
			}
		case index == conversations:
			messages := make([]store.Message, 0, 2)
			for seq := uint64(1); seq <= 2; seq++ {
				messages = append(messages, store.Message{
					ID:              uint64(900000) + seq,
					Seq:             seq,
					SenderID:        low,
					RecipientID:     high,
					ClientMessageID: fmt.Sprintf("tail-%d", seq),
					Body:            fmt.Sprintf("tail-%d", seq),
					CreatedAt:       now.Add(-time.Hour),
				})
			}
			fakeStore.directMessages[pair] = messages
			setDirectSequence(t, client, low, high, 2)
			fakeStore.conversations = append(fakeStore.conversations, store.ConversationSeqStat{
				LowID: low, HighID: high, Count: 2, MaxSeq: 2,
			})
		default:
			setDirectSequence(t, client, low, high, 1)
			seedReconcileDirectIndex(t, client, low, high, uint64(200000)+uint64(index), 1)
			fakeStore.conversations = append(fakeStore.conversations, store.ConversationSeqStat{
				LowID: low, HighID: high, Count: 1, MaxSeq: 1,
			})
		}
	}

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:      writer,
		RepairLimit: 2,
		Now:         func() time.Time { return current },
	})
	current = now.Add(2 * time.Hour)
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("first RunOnce() error = %v", err)
	}
	// One repair is spent by the 5-minute phase on the tail conversation, one
	// by the deep audit on the first sampled conversation.
	if result.RedisRepaired != 2 {
		t.Fatalf("first result = %+v, want quick and deep repairs to share the budget", result)
	}
	deferredID := uint64(200000) + uint64(missingEntitySeq)
	if body := client.HGet(ctx, defaultKeyspace.messageKey(deferredID), "body").Val(); body != "" {
		t.Fatalf("second conversation was repaired despite the spent budget: %q", body)
	}
	if low, high := reconciler.directDeepCursorState(); low != 1 || high != 2 {
		t.Fatalf("deep cursor = (%d,%d), want it kept before the deferred conversation", low, high)
	}

	current = now.Add(3 * time.Hour)
	if _, err := reconciler.RunOnce(ctx); err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if body := client.HGet(ctx, defaultKeyspace.messageKey(deferredID), "body").Val(); body != "body-2-1" {
		t.Fatalf("deferred conversation entity body = %q, want it retried and repaired", body)
	}
}

// A failed watermark raise must leave the read cursor before the failed row so
// the next run retries it instead of skipping a full rotation.
func TestReconcilerWatermarkCursorRetriesFailedRaise(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	fakeStore := newFakeReconcileStore()
	fakeStore.conversationReads = []store.ConversationReadState{
		{UserID: 1, PeerID: 2, LastReadSeq: 5},
	}
	repair := &controlledRepairer{
		ProjectionRepairer: newAuditWriter(client, now),
		failWatermarks:     true,
	}
	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: repair,
		Batch:  1,
		Now:    func() time.Time { return current },
	})
	reconciler.setDirectReadCursor(1, 1)

	if _, err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("first RunOnce() error = %v", err)
	}
	if got := client.HGet(context.Background(), defaultKeyspace.directReadKey(1), "2").Val(); got != "" {
		t.Fatalf("watermark = %q, want the failed raise to write nothing", got)
	}
	if userID, peerID := reconciler.directReadCursorState(); userID != 1 || peerID != 1 {
		t.Fatalf("read cursor = (%d,%d), want the pre-scan position kept", userID, peerID)
	}

	repair.setFailWatermarks(false)
	if _, err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if got := client.HGet(context.Background(), defaultKeyspace.directReadKey(1), "2").Val(); got != "5" {
		t.Fatalf("watermark = %q, want 5 after the retry", got)
	}
	if userID, peerID := reconciler.directReadCursorState(); userID != 0 || peerID != 0 {
		t.Fatalf("read cursor = (%d,%d), want it advanced after the successful raise", userID, peerID)
	}
}

// The reverse index repair must not rewrite an entity that already exists and
// diverges from MySQL: that is a rebuild signal, matching the deep audit.
func TestReconcilerIndexGapDoesNotOverwriteDivergedEntity(t *testing.T) {
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
	if err := client.ZRem(ctx, defaultKeyspace.directPairKey(1, 2), "101").Err(); err != nil {
		t.Fatalf("remove index member: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 3, MaxSeq: 3}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{Repair: writer})
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.NeedsRebuild != 1 || result.RedisRepaired != 0 {
		t.Fatalf("result = %+v, want a rebuild flag and no rewrite", result)
	}
	if body := client.HGet(ctx, defaultKeyspace.messageKey(101), "body").Val(); body != "tampered" {
		t.Fatalf("entity body = %q, want the divergence left untouched", body)
	}
}

// A present entity that matches MySQL is repaired without rewriting the hash.
func TestReconcilerIndexGapRepairsExistingEntityIndexOnly(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditDirectMessages(3, now.Add(-time.Hour))

	writer := newAuditWriter(client, now)
	if _, err := writer.WriteDirectBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}
	// body_hash is not part of the equality check, so an index-only repair
	// leaves it alone while a full rewrite would recompute it.
	if err := client.HSet(ctx, defaultKeyspace.messageKey(101), "body_hash", "sentinel").Err(); err != nil {
		t.Fatalf("mark entity: %v", err)
	}
	if err := client.ZRem(ctx, defaultKeyspace.directPairKey(1, 2), "101").Err(); err != nil {
		t.Fatalf("remove index member: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 3, MaxSeq: 3}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{Repair: writer})
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.NeedsRebuild != 0 || result.RedisRepaired != 1 {
		t.Fatalf("result = %+v, want the index repaired", result)
	}
	if card := client.ZCard(ctx, defaultKeyspace.directPairKey(1, 2)).Val(); card != 3 {
		t.Fatalf("pair card = %d, want 3", card)
	}
	if hash := client.HGet(ctx, defaultKeyspace.messageKey(101), "body_hash").Val(); hash != "sentinel" {
		t.Fatalf("entity body_hash = %q, want it untouched by the index repair", hash)
	}
}

func TestReconcilerGroupIndexGapDoesNotOverwriteDivergedEntity(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditGroupMessages(3, now.Add(-time.Hour))

	writer := newAuditWriter(client, now)
	if _, err := writer.WriteGroupBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}
	if err := client.HSet(ctx, defaultKeyspace.groupMessageKey(201), "body", "tampered").Err(); err != nil {
		t.Fatalf("tamper group entity: %v", err)
	}
	if err := client.ZRem(ctx, defaultKeyspace.groupInboxKey(7), "201").Err(); err != nil {
		t.Fatalf("remove inbox member: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.groups = []store.GroupSeqStat{{GroupID: 7, Count: 3, MaxSeq: 3}}
	fakeStore.groupMessages[7] = messages

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{Repair: writer})
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.NeedsRebuild != 1 || result.RedisRepaired != 0 {
		t.Fatalf("result = %+v, want a rebuild flag and no rewrite", result)
	}
	if body := client.HGet(ctx, defaultKeyspace.groupMessageKey(201), "body").Val(); body != "tampered" {
		t.Fatalf("group entity body = %q, want the divergence left untouched", body)
	}
}

// A lost dm:seq must be recovered above the surviving Redis-only tail, not just
// to MySQL MAX(seq): otherwise the next accept reuses a seq that already has a
// pair member and the persist loses one of the two messages.
func TestReconcilerRaisesLostDirectCounterAboveSurvivingTail(t *testing.T) {
	cases := []struct {
		name    string
		counter uint64
		set     bool
	}{
		{name: "missing counter", set: false},
		{name: "counter behind MySQL", counter: 2, set: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			client, _ := newReconcileTestClient(t)
			now := reconcileTestNow()
			old := now.Add(-30 * time.Minute)
			// Redis knows seqs 1..5; MySQL only persisted 1..3. Seqs 4 and 5
			// are the accepted-but-unpersisted tail.
			for seq := uint64(1); seq <= 5; seq++ {
				seedReconcileDirectMessage(t, client, 1, 2, 100+seq, seq, old)
			}
			if testCase.set {
				setDirectSequence(t, client, 1, 2, testCase.counter)
			}

			fakeStore := newFakeReconcileStore()
			fakeStore.conversations = []store.ConversationSeqStat{{
				LowID: 1, HighID: 2, Count: 3, MaxSeq: 3,
			}}
			fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1, 2, 3}

			reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
				Repair: newAuditWriter(client, now),
			})
			result, err := reconciler.RunOnce(context.Background())
			if err != nil {
				t.Fatalf("RunOnce() error = %v", err)
			}
			if counter := client.Get(context.Background(), defaultKeyspace.directSequenceKey(1, 2)).Val(); counter != "5" {
				t.Fatalf("counter = %q, want it raised to the surviving tail score 5", counter)
			}
			if result.Repaired != 2 || result.Unresolved != 0 {
				t.Fatalf("result = %+v, want both tail seqs persisted", result)
			}
			persisted, _, _, _, _ := fakeStore.snapshot()
			if len(persisted) != 2 || persisted[0].Seq != 4 || persisted[1].Seq != 5 {
				t.Fatalf("persisted = %+v, want seqs 4 and 5", persisted)
			}
		})
	}
}

func TestReconcilerRaisesLostGroupCounterAboveSurvivingTail(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	old := now.Add(-30 * time.Minute)
	for seq := uint64(1); seq <= 5; seq++ {
		seedReconcileGroupMessage(t, client, 7, 200+seq, seq, old)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.groups = []store.GroupSeqStat{{GroupID: 7, Count: 3, MaxSeq: 3}}
	fakeStore.groupSeqs[7] = []uint64{1, 2, 3}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
	})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if counter := client.Get(context.Background(), defaultKeyspace.groupSequenceKey(7)).Val(); counter != "5" {
		t.Fatalf("group counter = %q, want it raised to the surviving tail score 5", counter)
	}
	if result.Repaired != 2 || result.Unresolved != 0 {
		t.Fatalf("result = %+v, want both tail seqs persisted", result)
	}
	_, persistedGroups, _, _, _ := fakeStore.snapshot()
	if len(persistedGroups) != 2 ||
		persistedGroups[0].Seq != 4 || persistedGroups[1].Seq != 5 {
		t.Fatalf("persisted groups = %+v, want seqs 4 and 5", persistedGroups)
	}
}

// messages:next_id is the only guard against reusing a message ID. A lost or
// stale key must be raised from the durable and Redis high-water marks and must
// never move backwards.
func TestReconcilerAuditsGlobalMessageIDCounter(t *testing.T) {
	ctx := context.Background()
	t.Run("missing counter is raised", func(t *testing.T) {
		client, _ := newReconcileTestClient(t)
		if err := client.Del(ctx, defaultKeyspace.counterKey()).Err(); err != nil {
			t.Fatalf("delete counter: %v", err)
		}
		fakeStore := newFakeReconcileStore()
		fakeStore.maxServerMessageID = 42
		reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
			Repair: newAuditWriter(client, reconcileTestNow()),
		})
		result, err := reconciler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("RunOnce() error = %v", err)
		}
		if counter := client.Get(ctx, defaultKeyspace.counterKey()).Val(); counter != "42" {
			t.Fatalf("counter = %q, want 42", counter)
		}
		if result.CountersRaised != 1 || result.RedisRepaired != 1 || result.RedisMissing != 1 {
			t.Fatalf("result = %+v, want one counter raised", result)
		}
	})
	t.Run("counter below durable max is raised", func(t *testing.T) {
		client, _ := newReconcileTestClient(t)
		if err := client.Set(ctx, defaultKeyspace.counterKey(), 10, 0).Err(); err != nil {
			t.Fatalf("seed counter: %v", err)
		}
		fakeStore := newFakeReconcileStore()
		fakeStore.maxServerMessageID = 42
		reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
			Repair: newAuditWriter(client, reconcileTestNow()),
		})
		result, err := reconciler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("RunOnce() error = %v", err)
		}
		if counter := client.Get(ctx, defaultKeyspace.counterKey()).Val(); counter != "42" {
			t.Fatalf("counter = %q, want 42", counter)
		}
		if result.CountersRaised != 1 {
			t.Fatalf("result = %+v, want one counter raised", result)
		}
	})
	t.Run("lost counter is raised above surviving entities", func(t *testing.T) {
		client, _ := newReconcileTestClient(t)
		if err := client.Del(ctx, defaultKeyspace.counterKey()).Err(); err != nil {
			t.Fatalf("delete counter: %v", err)
		}
		for _, key := range []string{
			defaultKeyspace.messageKey(101),
			defaultKeyspace.messageKey(103),
			defaultKeyspace.groupMessageKey(207),
		} {
			if err := client.HSet(ctx, key, "id", key).Err(); err != nil {
				t.Fatalf("seed entity %s: %v", key, err)
			}
		}
		fakeStore := newFakeReconcileStore()
		fakeStore.maxServerMessageID = 5
		reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
			Repair: newAuditWriter(client, reconcileTestNow()),
		})
		if _, err := reconciler.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce() error = %v", err)
		}
		if counter := client.Get(ctx, defaultKeyspace.counterKey()).Val(); counter != "207" {
			t.Fatalf("counter = %q, want 207 (above the surviving entity)", counter)
		}
	})
	t.Run("counter is never lowered", func(t *testing.T) {
		client, _ := newReconcileTestClient(t)
		if err := client.Set(ctx, defaultKeyspace.counterKey(), 100, 0).Err(); err != nil {
			t.Fatalf("seed counter: %v", err)
		}
		fakeStore := newFakeReconcileStore()
		fakeStore.maxServerMessageID = 10
		reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
			Repair: newAuditWriter(client, reconcileTestNow()),
		})
		result, err := reconciler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("RunOnce() error = %v", err)
		}
		if counter := client.Get(ctx, defaultKeyspace.counterKey()).Val(); counter != "100" {
			t.Fatalf("counter = %q, want it left at 100", counter)
		}
		if result.CountersRaised != 0 || result.RedisRepaired != 0 {
			t.Fatalf("result = %+v, want no write", result)
		}
	})
}

// A present feed entity that differs from MySQL must be reported, not
// overwritten, exactly like the index-gap and deep-entity repairs.
func TestReconcilerDeepAuditFeedDoesNotOverwriteDivergedEntity(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditDirectMessages(3, now.Add(-time.Hour))
	if _, err := newAuditWriter(client, now).WriteDirectBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}
	if err := client.HSet(ctx, defaultKeyspace.messageKey(102), "body", "tampered").Err(); err != nil {
		t.Fatalf("tamper entity: %v", err)
	}
	if err := client.ZRem(ctx, defaultKeyspace.directFeedKey(1), "102").Err(); err != nil {
		t.Fatalf("remove feed member: %v", err)
	}

	logs := captureReconcileLog(t)
	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{
		LowID: 1, HighID: 2, Count: 3, MaxSeq: 3,
	}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1, 2, 3}
	fakeStore.userIDs = []uint64{1}
	fakeStore.userPeers[1] = []uint64{2}

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
	if result.NeedsRebuild == 0 || result.FeedRepairs != 0 {
		t.Fatalf("result = %+v, want the divergence flagged without a feed repair", result)
	}
	if !strings.Contains(logs.String(), "needs-rebuild user:1:feed:entity") {
		t.Fatalf("log = %s, want the feed divergence reported", logs.String())
	}
	if body := client.HGet(ctx, defaultKeyspace.messageKey(102), "body").Val(); body != "tampered" {
		t.Fatalf("entity body = %q, want the divergence left untouched", body)
	}
	if err := client.ZScore(ctx, defaultKeyspace.directFeedKey(1), "102").Err(); !errors.Is(err, redis.Nil) {
		t.Fatalf("feed member 102 error = %v, want the feed entry still absent", err)
	}
}

// A diverged entity behind a stale conversation preview must not be rewritten
// just to repair the preview.
func TestReconcilerDeepAuditPreviewDoesNotOverwriteDivergedEntity(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	messages := auditDirectMessages(3, now.Add(-time.Hour))
	if _, err := newAuditWriter(client, now).WriteDirectBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}
	if err := client.HSet(ctx, defaultKeyspace.messageKey(103), "body", "tampered").Err(); err != nil {
		t.Fatalf("tamper entity: %v", err)
	}
	if err := client.HSet(
		ctx, defaultKeyspace.directConversationLastKey(1), "2", 101,
	).Err(); err != nil {
		t.Fatalf("stale preview: %v", err)
	}

	logs := captureReconcileLog(t)
	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{
		LowID: 1, HighID: 2, Count: 3, MaxSeq: 3,
	}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1, 2, 3}
	fakeStore.userIDs = []uint64{1}
	fakeStore.userConversations = map[uint64][]store.Conversation{
		1: {{UserID: 2, LastMessage: &messages[2]}},
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
	if result.NeedsRebuild == 0 || result.RedisRepaired != 0 {
		t.Fatalf("result = %+v, want the divergence flagged without a rewrite", result)
	}
	if !strings.Contains(logs.String(), "needs-rebuild user:1:conv-last:entity") {
		t.Fatalf("log = %s, want the preview divergence reported", logs.String())
	}
	if body := client.HGet(ctx, defaultKeyspace.messageKey(103), "body").Val(); body != "tampered" {
		t.Fatalf("entity body = %q, want the divergence left untouched", body)
	}
	if preview := client.HGet(ctx, defaultKeyspace.directConversationLastKey(1), "2").Val(); preview != "101" {
		t.Fatalf("preview = %q, want the stale value left in place", preview)
	}
}

// The feed walk must not report success when it stopped at the 2000-check cap
// before finding the whole gap; the user must be retried on the next audit.
func TestReconcilerFeedWalkRetriesGapAfterCheckCap(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	const (
		firstPeerMessages = 2000
		missingID         = uint64(5000)
	)
	writer := newAuditWriter(client, now)
	messages := make([]store.Message, 0, firstPeerMessages+1)
	for seq := uint64(1); seq <= firstPeerMessages; seq++ {
		messages = append(messages, store.Message{
			ID:              uint64(1000) + seq,
			Seq:             seq,
			SenderID:        1,
			RecipientID:     2,
			ClientMessageID: fmt.Sprintf("cap-a-%d", seq),
			Body:            fmt.Sprintf("cap-a-%d", seq),
			CreatedAt:       now.Add(-time.Hour),
		})
	}
	missing := store.Message{
		ID:              missingID,
		Seq:             1,
		SenderID:        1,
		RecipientID:     3,
		ClientMessageID: "cap-b-1",
		Body:            "cap-b-1",
		CreatedAt:       now.Add(-time.Hour),
	}
	messages = append(messages, missing)
	if _, err := writer.WriteDirectBatch(ctx, messages[:firstPeerMessages]); err != nil {
		t.Fatalf("seed first conversation: %v", err)
	}
	if _, err := writer.WriteDirectBatch(ctx, []store.Message{missing}); err != nil {
		t.Fatalf("seed second conversation: %v", err)
	}
	// Only the sampled user's feed loses the second conversation's message; the
	// pair index and the entity stay healthy.
	if err := client.ZRem(ctx, defaultKeyspace.directFeedKey(1), strconvUint(missingID)).Err(); err != nil {
		t.Fatalf("remove feed member: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.directMessages[[2]uint64{1, 2}] = messages[:firstPeerMessages]
	fakeStore.directMessages[[2]uint64{1, 3}] = []store.Message{missing}
	fakeStore.directSeqs[[2]uint64{1, 2}] = sequenceRange(1, firstPeerMessages)
	fakeStore.directSeqs[[2]uint64{1, 3}] = []uint64{1}
	fakeStore.conversations = []store.ConversationSeqStat{
		{LowID: 1, HighID: 2, Count: firstPeerMessages, MaxSeq: firstPeerMessages},
		{LowID: 1, HighID: 3, Count: 1, MaxSeq: 1},
	}
	fakeStore.userIDs = []uint64{1}
	fakeStore.userPeers[1] = []uint64{2, 3}

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:         writer,
		DeepAuditUsers: 1,
		Now:            func() time.Time { return current },
	})
	current = now.Add(2 * time.Hour)
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("first RunOnce() error = %v", err)
	}
	if result.FeedRepairs != 0 {
		t.Fatalf("first result = %+v, want the capped walk to repair nothing", result)
	}
	if userID := reconciler.userDeepCursorState(); userID != 0 {
		t.Fatalf("user cursor = %d, want the user retried next audit", userID)
	}

	current = now.Add(3 * time.Hour)
	result, err = reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if result.FeedRepairs != 1 {
		t.Fatalf("second result = %+v, want the deferred gap repaired", result)
	}
	if err := client.ZScore(
		ctx, defaultKeyspace.directFeedKey(1), strconvUint(missingID),
	).Err(); err != nil {
		t.Fatalf("feed member error = %v, want the entry restored", err)
	}
}

func sequenceRange(from, to uint64) []uint64 {
	seqs := make([]uint64, 0, to-from+1)
	for seq := from; seq <= to; seq++ {
		seqs = append(seqs, seq)
	}
	return seqs
}

// failingWatermarkRepairer fails the direct watermark raise for one user, so a
// test can prove the retry budget and the cursor advance past a dead chunk.
type failingWatermarkRepairer struct {
	ProjectionRepairer

	mu         sync.Mutex
	failUserID uint64
	failures   int
}

func (r *failingWatermarkRepairer) RaiseDirectReadWatermarks(
	ctx context.Context,
	states []store.ConversationReadState,
) (int, error) {
	r.mu.Lock()
	for _, state := range states {
		if state.UserID == r.failUserID {
			r.failures++
			r.mu.Unlock()
			return 0, fmt.Errorf("watermark key is permanently broken")
		}
	}
	r.mu.Unlock()
	return r.ProjectionRepairer.RaiseDirectReadWatermarks(ctx, states)
}

func (r *failingWatermarkRepairer) failureCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failures
}

// A permanently failing watermark chunk must be marked and skipped after three
// attempts instead of pinning every later watermark forever.
func TestReconcilerWatermarkRetryLimitAdvancesPastFailingChunk(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()

	fakeStore := newFakeReconcileStore()
	fakeStore.conversationReads = []store.ConversationReadState{
		{UserID: 1, PeerID: 2, LastReadSeq: 5},
		{UserID: 2, PeerID: 4, LastReadSeq: 5},
		{UserID: 3, PeerID: 6, LastReadSeq: 5},
	}
	repair := &failingWatermarkRepairer{
		ProjectionRepairer: newAuditWriter(client, now),
		failUserID:         2,
	}
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: repair,
		Batch:  1,
		Now:    func() time.Time { return now },
	})

	var last ReconcileResult
	for run := 0; run < reconcileWatermarkRetryLimit; run++ {
		result, err := reconciler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("RunOnce() run %d error = %v", run, err)
		}
		last = result
	}
	if last.NeedsRebuild != 1 {
		t.Fatalf("last result = %+v, want the dead chunk marked for rebuild", last)
	}
	if repair.failureCount() != reconcileWatermarkRetryLimit {
		t.Fatalf("failures = %d, want %d", repair.failureCount(), reconcileWatermarkRetryLimit)
	}
	if got := client.HGet(ctx, defaultKeyspace.directReadKey(2), "4").Val(); got != "" {
		t.Fatalf("failed watermark = %q, want it never written", got)
	}
	if got := client.HGet(ctx, defaultKeyspace.directReadKey(1), "2").Val(); got != "5" {
		t.Fatalf("first watermark = %q, want 5", got)
	}
	if got := client.HGet(ctx, defaultKeyspace.directReadKey(3), "6").Val(); got != "5" {
		t.Fatalf("later watermark = %q, want 5 after the cursor moved past the dead chunk", got)
	}
	if userID, peerID := reconciler.directReadCursorState(); userID != 0 || peerID != 0 {
		t.Fatalf("read cursor = (%d,%d), want it advanced past the scan", userID, peerID)
	}
}

// A wrong-type watermark key is one unrecoverable key, not a failed audit. The
// batch must keep comparing and raising the remaining direct and group keys,
// report the malformed keys for a rebuild, never overwrite them, and still
// advance the read cursors.
func TestReconcilerToleratesWrongTypeWatermarkKeys(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()

	if err := client.Set(ctx, defaultKeyspace.directReadKey(2), "not-a-hash", 0).Err(); err != nil {
		t.Fatalf("seed wrong-type direct watermark: %v", err)
	}
	if err := client.Set(ctx, defaultKeyspace.groupReadKey(8), "not-a-zset", 0).Err(); err != nil {
		t.Fatalf("seed wrong-type group watermark: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversationReads = []store.ConversationReadState{
		{UserID: 1, PeerID: 2, LastReadSeq: 5},
		{UserID: 2, PeerID: 3, LastReadSeq: 6},
		{UserID: 3, PeerID: 4, LastReadSeq: 7},
	}
	fakeStore.groupReads = []store.GroupMemberReadState{
		{GroupID: 7, UserID: 9, LastReadSeq: 4},
		{GroupID: 8, UserID: 9, LastReadSeq: 4},
	}
	logs := captureReconcileLog(t)

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
		Now:    func() time.Time { return current },
	})
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v, want a per-key watermark error tolerated", err)
	}
	if result.WatermarksRaised != 3 || result.NeedsRebuild != 2 {
		t.Fatalf("result = %+v, want three watermarks raised and two rebuild flags", result)
	}
	if got := client.HGet(ctx, defaultKeyspace.directReadKey(1), "2").Val(); got != "5" {
		t.Fatalf("direct watermark before the bad key = %q, want 5", got)
	}
	if got := client.HGet(ctx, defaultKeyspace.directReadKey(3), "4").Val(); got != "7" {
		t.Fatalf("direct watermark after the bad key = %q, want 7", got)
	}
	if score := client.ZScore(ctx, defaultKeyspace.groupReadKey(7), "9").Val(); score != 4 {
		t.Fatalf("group watermark = %v, want 4", score)
	}
	if got := client.Get(ctx, defaultKeyspace.directReadKey(2)).Val(); got != "not-a-hash" {
		t.Fatalf("wrong-type direct key = %q, want it left untouched", got)
	}
	if got := client.Get(ctx, defaultKeyspace.groupReadKey(8)).Val(); got != "not-a-zset" {
		t.Fatalf("wrong-type group key = %q, want it left untouched", got)
	}
	if userID, peerID := reconciler.directReadCursorState(); userID != 0 || peerID != 0 {
		t.Fatalf("read cursor = (%d,%d), want it advanced past the scan", userID, peerID)
	}
	if groupID, userID := reconciler.groupReadCursorState(); groupID != 0 || userID != 0 {
		t.Fatalf("group read cursor = (%d,%d), want it advanced past the scan", groupID, userID)
	}
	for _, want := range []string{"needs-rebuild direct:read:2", "needs-rebuild group:read:8"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log = %s, want %q", logs.String(), want)
		}
	}
	if got := strings.Count(logs.String(), "direct:read:2"); got != 1 {
		t.Fatalf("bad direct key logged %d times, want one throttled line: %s", got, logs.String())
	}

	// The bad keys stay malformed, so the second audit reports them again but
	// must still not stop the rotation or abort the run.
	current = now.Add(time.Hour)
	second, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if second.WatermarksRaised != 0 || second.NeedsRebuild != 2 {
		t.Fatalf("second result = %+v, want no new raises and the same rebuild flags", second)
	}
}

// A malformed group read key must not abort the group watermark rotation on its
// own: later groups are still raised, and the bad key is reported, not written.
func TestReconcilerToleratesWrongTypeGroupWatermarkKey(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()

	if err := client.Set(ctx, defaultKeyspace.groupReadKey(8), "not-a-zset", 0).Err(); err != nil {
		t.Fatalf("seed wrong-type group watermark: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.groupReads = []store.GroupMemberReadState{
		{GroupID: 7, UserID: 9, LastReadSeq: 4},
		{GroupID: 8, UserID: 9, LastReadSeq: 4},
	}
	logs := captureReconcileLog(t)
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair: newAuditWriter(client, now),
	})
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce() error = %v, want the per-key group error tolerated", err)
	}
	if result.WatermarksRaised != 1 || result.NeedsRebuild != 1 {
		t.Fatalf("result = %+v, want one watermark raised and one rebuild flag", result)
	}
	if score := client.ZScore(ctx, defaultKeyspace.groupReadKey(7), "9").Val(); score != 4 {
		t.Fatalf("healthy group watermark = %v, want 4", score)
	}
	if got := client.Get(ctx, defaultKeyspace.groupReadKey(8)).Val(); got != "not-a-zset" {
		t.Fatalf("wrong-type group key = %q, want it left untouched", got)
	}
	if groupID, userID := reconciler.groupReadCursorState(); groupID != 0 || userID != 0 {
		t.Fatalf("group read cursor = (%d,%d), want it advanced past the scan", groupID, userID)
	}
	if !strings.Contains(logs.String(), "needs-rebuild group:read:8") {
		t.Fatalf("log = %s, want the bad group key reported", logs.String())
	}
}

// The middle-entity sample must rotate across audits, wrap back to the start of
// the window, and restart from the beginning when the sample page is empty.
func TestReconcilerDeepAuditRotatesMiddleSample(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	const messageCount = 7
	messages := auditDirectMessages(messageCount, now.Add(-time.Hour))
	writer := newAuditWriter(client, now)
	if _, err := writer.WriteDirectBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{
		LowID: 1, HighID: 2, Count: messageCount, MaxSeq: messageCount,
	}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:            writer,
		DeepAuditBoundary: 1,
		Now:               func() time.Time { return current },
	})
	key := reconcileSeqCursorKey{first: 1, second: 2}
	// With boundary 1 the middle window is 2..6 and each audit samples exactly
	// one seq. Deleting that entity before the audit proves the cursor moved.
	for seq := uint64(2); seq <= 6; seq++ {
		if err := client.Del(ctx, defaultKeyspace.messageKey(100+seq)).Err(); err != nil {
			t.Fatalf("delete entity %d: %v", seq, err)
		}
		current = current.Add(time.Hour)
		result, err := reconciler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("RunOnce() seq %d error = %v", seq, err)
		}
		if result.RedisRepaired != 1 {
			t.Fatalf("seq %d result = %+v, want only that middle entity repaired", seq, result)
		}
		if body := client.HGet(ctx, defaultKeyspace.messageKey(100+seq), "body").Val(); body == "" {
			t.Fatalf("seq %d entity was not repaired", seq)
		}
	}
	if start := reconciler.deepSeqStart(key, 2, 6); start != 2 {
		t.Fatalf("middle cursor start = %d, want the wrapped window start 2", start)
	}

	// The wrapped cursor must sample the first middle entity again.
	if err := client.Del(ctx, defaultKeyspace.messageKey(102)).Err(); err != nil {
		t.Fatalf("delete wrapped entity: %v", err)
	}
	current = current.Add(time.Hour)
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("wrapped RunOnce() error = %v", err)
	}
	if result.RedisRepaired != 1 {
		t.Fatalf("wrapped result = %+v, want the first middle entity sampled again", result)
	}
	if body := client.HGet(ctx, defaultKeyspace.messageKey(102), "body").Val(); body != "body-2" {
		t.Fatalf("wrapped entity body = %q, want body-2", body)
	}
}

func TestReconcilerDeepAuditMiddleSampleResetsOnEmptyPage(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	// MySQL claims a long conversation but only holds seqs 1 and 2, so the middle
	// page after seq 3 is empty.
	messages := auditDirectMessages(2, now.Add(-time.Hour))
	if _, err := newAuditWriter(client, now).WriteDirectBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{
		LowID: 1, HighID: 2, Count: 2, MaxSeq: 7,
	}}
	fakeStore.directMessages[[2]uint64{1, 2}] = messages

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:            newAuditWriter(client, now),
		DeepAuditBoundary: 1,
		Now:               func() time.Time { return current },
	})
	key := reconcileSeqCursorKey{first: 1, second: 2}
	reconciler.setDeepSeqCursor(key, 4, false)
	if start := reconciler.deepSeqStart(key, 2, 6); start != 4 {
		t.Fatalf("middle cursor start = %d, want the seeded 4", start)
	}

	current = now.Add(2 * time.Hour)
	if _, err := reconciler.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if start := reconciler.deepSeqStart(key, 2, 6); start != 2 {
		t.Fatalf("middle cursor start = %d, want the empty page to reset it", start)
	}
}

// The group middle-entity sample must rotate across audits, wrap back to the
// start of the window, and restart from the beginning when the page is empty.
func TestReconcilerDeepAuditRotatesMiddleGroupSample(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	const messageCount = 7
	messages := auditGroupMessages(messageCount, now.Add(-time.Hour))
	writer := newAuditWriter(client, now)
	if _, err := writer.WriteGroupBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.groups = []store.GroupSeqStat{{
		GroupID: 7, Count: messageCount, MaxSeq: messageCount,
	}}
	fakeStore.groupMessages[7] = messages

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:            writer,
		DeepAuditBoundary: 1,
		Now:               func() time.Time { return current },
	})
	key := reconcileSeqCursorKey{group: true, first: 7}
	// With boundary 1 the middle window is 2..6 and each audit samples exactly
	// one seq. Deleting that entity before the audit proves the cursor moved.
	for seq := uint64(2); seq <= 6; seq++ {
		if err := client.Del(ctx, defaultKeyspace.groupMessageKey(200+seq)).Err(); err != nil {
			t.Fatalf("delete entity %d: %v", seq, err)
		}
		current = current.Add(time.Hour)
		result, err := reconciler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("RunOnce() seq %d error = %v", seq, err)
		}
		if result.RedisRepaired != 1 {
			t.Fatalf("seq %d result = %+v, want only that middle group entity repaired", seq, result)
		}
		if body := client.HGet(ctx, defaultKeyspace.groupMessageKey(200+seq), "body").Val(); body == "" {
			t.Fatalf("seq %d group entity was not repaired", seq)
		}
	}
	if start := reconciler.deepSeqStart(key, 2, 6); start != 2 {
		t.Fatalf("middle cursor start = %d, want the wrapped window start 2", start)
	}

	// The wrapped cursor must sample the first middle entity again.
	if err := client.Del(ctx, defaultKeyspace.groupMessageKey(202)).Err(); err != nil {
		t.Fatalf("delete wrapped entity: %v", err)
	}
	current = current.Add(time.Hour)
	result, err := reconciler.RunOnce(ctx)
	if err != nil {
		t.Fatalf("wrapped RunOnce() error = %v", err)
	}
	if result.RedisRepaired != 1 {
		t.Fatalf("wrapped result = %+v, want the first middle entity sampled again", result)
	}
	if body := client.HGet(ctx, defaultKeyspace.groupMessageKey(202), "body").Val(); body != "gbody-2" {
		t.Fatalf("wrapped entity body = %q, want gbody-2", body)
	}
}

func TestReconcilerDeepAuditMiddleGroupSampleResetsOnEmptyPage(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	ctx := context.Background()
	// MySQL claims a long group but only holds seqs 1 and 2, so the middle page
	// after seq 3 is empty.
	messages := auditGroupMessages(2, now.Add(-time.Hour))
	if _, err := newAuditWriter(client, now).WriteGroupBatch(ctx, messages); err != nil {
		t.Fatalf("seed Redis: %v", err)
	}

	fakeStore := newFakeReconcileStore()
	fakeStore.groups = []store.GroupSeqStat{{
		GroupID: 7, Count: 2, MaxSeq: 7,
	}}
	fakeStore.groupMessages[7] = messages

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Repair:            newAuditWriter(client, now),
		DeepAuditBoundary: 1,
		Now:               func() time.Time { return current },
	})
	key := reconcileSeqCursorKey{group: true, first: 7}
	reconciler.setDeepSeqCursor(key, 4, false)
	if start := reconciler.deepSeqStart(key, 2, 6); start != 4 {
		t.Fatalf("middle cursor start = %d, want the seeded 4", start)
	}

	current = now.Add(2 * time.Hour)
	if _, err := reconciler.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if start := reconciler.deepSeqStart(key, 2, 6); start != 2 {
		t.Fatalf("middle cursor start = %d, want the empty page to reset it", start)
	}
}
