package messaging

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// fakeReconcileStore serves stats and seqs from memory and records repairs. It
// mirrors the store's paging so batch behaviour is exercised.
type fakeReconcileStore struct {
	mu sync.Mutex

	conversations []store.ConversationSeqStat
	groups        []store.GroupSeqStat
	directSeqs    map[[2]uint64][]uint64
	groupSeqs     map[uint64][]uint64
	persistErrs   map[uint64]error
	statsErr      error
	seqsErr       error

	persisted       []store.Message
	persistedGroups []store.GroupMessage
	attempts        int
	statsCalls      int
	detailCalls     int

	firstCall  chan struct{}
	signalOnce sync.Once
}

func newFakeReconcileStore() *fakeReconcileStore {
	return &fakeReconcileStore{
		directSeqs:  make(map[[2]uint64][]uint64),
		groupSeqs:   make(map[uint64][]uint64),
		persistErrs: make(map[uint64]error),
		firstCall:   make(chan struct{}),
	}
}

func (f *fakeReconcileStore) signalLocked() {
	f.signalOnce.Do(func() { close(f.firstCall) })
}

func (f *fakeReconcileStore) ListConversationSeqStats(
	_ context.Context,
	afterLowID, afterHighID uint64,
	limit int,
) ([]store.ConversationSeqStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statsCalls++
	f.signalLocked()
	if f.statsErr != nil {
		return nil, f.statsErr
	}
	rows := append([]store.ConversationSeqStat(nil), f.conversations...)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].LowID != rows[j].LowID {
			return rows[i].LowID < rows[j].LowID
		}
		return rows[i].HighID < rows[j].HighID
	})
	result := make([]store.ConversationSeqStat, 0, limit)
	for _, row := range rows {
		if row.LowID < afterLowID || (row.LowID == afterLowID && row.HighID <= afterHighID) {
			continue
		}
		result = append(result, row)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeReconcileStore) ListGroupSeqStats(
	_ context.Context,
	afterGroupID uint64,
	limit int,
) ([]store.GroupSeqStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statsCalls++
	f.signalLocked()
	if f.statsErr != nil {
		return nil, f.statsErr
	}
	rows := append([]store.GroupSeqStat(nil), f.groups...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].GroupID < rows[j].GroupID })
	result := make([]store.GroupSeqStat, 0, limit)
	for _, row := range rows {
		if row.GroupID <= afterGroupID {
			continue
		}
		result = append(result, row)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeReconcileStore) ListConversationSeqs(
	_ context.Context,
	lowID, highID, upTo uint64,
) ([]uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.detailCalls++
	if f.seqsErr != nil {
		return nil, f.seqsErr
	}
	return filterSeqs(f.directSeqs[[2]uint64{lowID, highID}], upTo), nil
}

func (f *fakeReconcileStore) ListGroupSeqs(
	_ context.Context,
	groupID, upTo uint64,
) ([]uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.detailCalls++
	if f.seqsErr != nil {
		return nil, f.seqsErr
	}
	return filterSeqs(f.groupSeqs[groupID], upTo), nil
}

func filterSeqs(stored []uint64, upTo uint64) []uint64 {
	seqs := append([]uint64(nil), stored...)
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	result := make([]uint64, 0, len(seqs))
	for _, seq := range seqs {
		if seq <= upTo {
			result = append(result, seq)
		}
	}
	return result
}

func (f *fakeReconcileStore) PersistMessage(
	_ context.Context,
	message store.Message,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if err, ok := f.persistErrs[message.ID]; ok {
		return false, err
	}
	f.persisted = append(f.persisted, message)
	lowID, highID := message.SenderID, message.RecipientID
	if lowID > highID {
		lowID, highID = highID, lowID
	}
	pair := [2]uint64{lowID, highID}
	f.directSeqs[pair] = append(f.directSeqs[pair], message.Seq)
	f.addConversationLocked(lowID, highID, message.Seq)
	return true, nil
}

func (f *fakeReconcileStore) PersistGroupMessage(
	_ context.Context,
	message store.GroupMessage,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if err, ok := f.persistErrs[message.ID]; ok {
		return false, err
	}
	f.persistedGroups = append(f.persistedGroups, message)
	f.groupSeqs[message.GroupID] = append(f.groupSeqs[message.GroupID], message.Seq)
	f.addGroupLocked(message.GroupID, message.Seq)
	return true, nil
}

func (f *fakeReconcileStore) addConversationLocked(lowID, highID, seq uint64) {
	for index := range f.conversations {
		row := &f.conversations[index]
		if row.LowID != lowID || row.HighID != highID {
			continue
		}
		row.Count++
		if seq > row.MaxSeq {
			row.MaxSeq = seq
		}
		return
	}
	f.conversations = append(f.conversations, store.ConversationSeqStat{
		LowID: lowID, HighID: highID, Count: 1, MaxSeq: seq,
	})
}

func (f *fakeReconcileStore) addGroupLocked(groupID, seq uint64) {
	for index := range f.groups {
		row := &f.groups[index]
		if row.GroupID != groupID {
			continue
		}
		row.Count++
		if seq > row.MaxSeq {
			row.MaxSeq = seq
		}
		return
	}
	f.groups = append(f.groups, store.GroupSeqStat{GroupID: groupID, Count: 1, MaxSeq: seq})
}

func (f *fakeReconcileStore) snapshot() ([]store.Message, []store.GroupMessage, int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.Message(nil), f.persisted...),
		append([]store.GroupMessage(nil), f.persistedGroups...),
		f.attempts, f.statsCalls, f.detailCalls
}

func newReconcileTestClient(t *testing.T) (redis.UniversalClient, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client, server
}

func reconcileTestNow() time.Time {
	return time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
}

func seedReconcileDirectMessage(
	t *testing.T,
	client redis.UniversalClient,
	lowID, highID, messageID, seq uint64,
	createdAt time.Time,
) {
	t.Helper()
	ctx := context.Background()
	if err := client.HSet(ctx, defaultKeyspace.messageKey(messageID), map[string]interface{}{
		"id":                messageID,
		"seq":               seq,
		"sender_id":         lowID,
		"recipient_id":      highID,
		"client_message_id": "c-" + strconvUint(messageID),
		"body":              "body-" + strconvUint(messageID),
		"created_at_ms":     createdAt.UnixMilli(),
	}).Err(); err != nil {
		t.Fatalf("seed direct message %d: %v", messageID, err)
	}
	if err := client.ZAdd(ctx, defaultKeyspace.directPairKey(lowID, highID), redis.Z{
		Score:  float64(seq),
		Member: strconvUint(messageID),
	}).Err(); err != nil {
		t.Fatalf("seed direct index %d: %v", messageID, err)
	}
}

func seedReconcileDirectIndex(t *testing.T, client redis.UniversalClient, lowID, highID, messageID, seq uint64) {
	t.Helper()
	if err := client.ZAdd(context.Background(), defaultKeyspace.directPairKey(lowID, highID), redis.Z{
		Score:  float64(seq),
		Member: strconvUint(messageID),
	}).Err(); err != nil {
		t.Fatalf("seed direct index %d: %v", messageID, err)
	}
}

func seedReconcileGroupMessage(
	t *testing.T,
	client redis.UniversalClient,
	groupID, messageID, seq uint64,
	createdAt time.Time,
) {
	t.Helper()
	ctx := context.Background()
	if err := client.HSet(ctx, defaultKeyspace.groupMessageKey(messageID), map[string]interface{}{
		"id":                messageID,
		"seq":               seq,
		"group_id":          groupID,
		"sender_id":         42,
		"client_message_id": "c-" + strconvUint(messageID),
		"body":              "body-" + strconvUint(messageID),
		"created_at_ms":     createdAt.UnixMilli(),
		"sender_username":   "alice",
		"sender_nickname":   "Alice",
	}).Err(); err != nil {
		t.Fatalf("seed group message %d: %v", messageID, err)
	}
	if err := client.ZAdd(ctx, defaultKeyspace.groupInboxKey(groupID), redis.Z{
		Score:  float64(seq),
		Member: strconvUint(messageID),
	}).Err(); err != nil {
		t.Fatalf("seed group inbox %d: %v", messageID, err)
	}
}

func seedReconcileGroupIndex(t *testing.T, client redis.UniversalClient, groupID, messageID, seq uint64) {
	t.Helper()
	if err := client.ZAdd(context.Background(), defaultKeyspace.groupInboxKey(groupID), redis.Z{
		Score:  float64(seq),
		Member: strconvUint(messageID),
	}).Err(); err != nil {
		t.Fatalf("seed group inbox %d: %v", messageID, err)
	}
}

func captureReconcileLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&buffer)
	t.Cleanup(func() { log.SetOutput(previous) })
	return &buffer
}

func newTestReconciler(
	client redis.UniversalClient,
	fakeStore *fakeReconcileStore,
	options ReconcileOptions,
) *Reconciler {
	options.Keyspace = defaultKeyspace
	if options.Now == nil {
		options.Now = reconcileTestNow
	}
	return NewReconciler(client, fakeStore, options)
}

func TestReconcilerHealthyData(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	setDirectSequence(t, client, 1, 2, 3)
	setGroupSequence(t, client, 7, 2)
	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 3, MaxSeq: 3}}
	fakeStore.groups = []store.GroupSeqStat{{GroupID: 7, Count: 2, MaxSeq: 2}}
	logs := captureReconcileLog(t)

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Scanned != 2 || result.Mismatched != 0 || result.Repaired != 0 || result.Unresolved != 0 {
		t.Fatalf("result = %+v, want scanned 2 and nothing else", result)
	}
	persisted, persistedGroups, attempts, _, detailCalls := fakeStore.snapshot()
	if len(persisted) != 0 || len(persistedGroups) != 0 || attempts != 0 {
		t.Fatalf("healthy data was repaired: %v %v", persisted, persistedGroups)
	}
	if detailCalls != 0 {
		t.Fatalf("detail queries = %d, want 0", detailCalls)
	}
	if strings.Contains(logs.String(), "reconcile") {
		t.Fatalf("healthy run logged: %s", logs.String())
	}
	status := reconciler.Status()
	if status.TotalRepaired != 0 || status.Running || status.Scanned != 2 {
		t.Fatalf("status = %+v", status)
	}
}

func TestReconcilerRepairsMissingDirectTail(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	old := now.Add(-10 * time.Minute)
	setDirectSequence(t, client, 1, 2, 5)
	seedReconcileDirectMessage(t, client, 1, 2, 104, 4, old)
	seedReconcileDirectMessage(t, client, 1, 2, 105, 5, old)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 3, MaxSeq: 3}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1, 2, 3}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Scanned != 1 || result.Mismatched != 1 || result.Repaired != 2 || result.Unresolved != 0 {
		t.Fatalf("result = %+v, want scanned 1 mismatched 1 repaired 2", result)
	}
	persisted, _, _, _, _ := fakeStore.snapshot()
	if len(persisted) != 2 {
		t.Fatalf("persisted = %+v, want 2 messages", persisted)
	}
	if persisted[0].Seq != 4 || persisted[0].ID != 104 || persisted[1].Seq != 5 || persisted[1].ID != 105 {
		t.Fatalf("persisted order = %+v, want seq 4 then 5", persisted)
	}
	message := persisted[0]
	if message.SenderID != 1 || message.RecipientID != 2 ||
		message.ClientMessageID != "c-104" || message.Body != "body-104" {
		t.Fatalf("repaired message fields = %+v", message)
	}
	if !message.CreatedAt.Equal(old) {
		t.Fatalf("repaired createdAt = %v, want %v", message.CreatedAt, old)
	}
	if total := reconciler.Status().TotalRepaired; total != 2 {
		t.Fatalf("TotalRepaired = %d, want 2", total)
	}
}

func TestReconcilerRepairsMissingGroupTail(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	old := now.Add(-10 * time.Minute)
	setGroupSequence(t, client, 7, 4)
	seedReconcileGroupMessage(t, client, 7, 203, 3, old)
	seedReconcileGroupMessage(t, client, 7, 204, 4, old)

	fakeStore := newFakeReconcileStore()
	fakeStore.groups = []store.GroupSeqStat{{GroupID: 7, Count: 2, MaxSeq: 2}}
	fakeStore.groupSeqs[7] = []uint64{1, 2}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Repaired != 2 || result.Unresolved != 0 {
		t.Fatalf("result = %+v, want 2 repaired", result)
	}
	_, persistedGroups, _, _, _ := fakeStore.snapshot()
	if len(persistedGroups) != 2 {
		t.Fatalf("persisted groups = %+v, want 2", persistedGroups)
	}
	message := persistedGroups[0]
	if message.ID != 203 || message.Seq != 3 || message.GroupID != 7 || message.SenderID != 42 {
		t.Fatalf("repaired group message = %+v", message)
	}
	if message.Sender.Username != "alice" || message.Sender.Nickname != "Alice" {
		t.Fatalf("sender snapshot = %+v, want alice/Alice", message.Sender)
	}
	if !message.CreatedAt.Equal(old) {
		t.Fatalf("repaired createdAt = %v, want %v", message.CreatedAt, old)
	}
}

func TestReconcilerSkipsYoungEntity(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	setDirectSequence(t, client, 1, 2, 2)
	seedReconcileDirectMessage(t, client, 1, 2, 102, 2, now.Add(-time.Minute))

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 1, MaxSeq: 1}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Repaired != 0 || result.Unresolved != 0 || result.InFlight != 1 {
		t.Fatalf("result = %+v, want one in-flight and nothing else", result)
	}
	persisted, _, attempts, _, _ := fakeStore.snapshot()
	if len(persisted) != 0 || attempts != 0 {
		t.Fatalf("young entity was persisted: %+v", persisted)
	}
}

func TestReconcilerRepairsHoleInMiddle(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	old := now.Add(-10 * time.Minute)
	setDirectSequence(t, client, 1, 2, 5)
	seedReconcileDirectMessage(t, client, 1, 2, 103, 3, old)
	seedReconcileDirectMessage(t, client, 1, 2, 104, 4, old)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 3, MaxSeq: 5}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1, 2, 5}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Mismatched != 1 || result.Repaired != 2 {
		t.Fatalf("result = %+v, want 2 repaired", result)
	}
	persisted, _, _, _, _ := fakeStore.snapshot()
	if len(persisted) != 2 || persisted[0].Seq != 3 || persisted[1].Seq != 4 {
		t.Fatalf("persisted = %+v, want seqs 3 then 4", persisted)
	}
}

func TestReconcilerEntityMissingLoggedOnce(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	setDirectSequence(t, client, 1, 2, 2)
	seedReconcileDirectIndex(t, client, 1, 2, 102, 2)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 1, MaxSeq: 1}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1}
	logs := captureReconcileLog(t)

	current := now
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Now: func() time.Time { return current },
	})

	for run := 0; run < 2; run++ {
		result, err := reconciler.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("RunOnce() run %d error = %v", run, err)
		}
		if result.Unresolved != 1 || result.Repaired != 0 {
			t.Fatalf("run %d result = %+v, want one unresolved", run, result)
		}
	}
	marker := "reconcile unresolved direct:1:2:2"
	if got := strings.Count(logs.String(), marker); got != 1 {
		t.Fatalf("unresolved log lines = %d, want 1: %s", got, logs.String())
	}
	status := reconciler.Status()
	if status.Unresolved != 1 || status.LastError != "" {
		t.Fatalf("status = %+v, want unresolved 1 and no error", status)
	}

	current = now.Add(2 * time.Hour)
	if _, err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() third run error = %v", err)
	}
	if got := strings.Count(logs.String(), marker); got != 2 {
		t.Fatalf("unresolved log lines after LogEvery = %d, want 2", got)
	}
}

func TestReconcilerIndexEntryMissing(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	setDirectSequence(t, client, 1, 2, 2)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 1, MaxSeq: 1}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1}
	logs := captureReconcileLog(t)

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Unresolved != 1 || result.Repaired != 0 {
		t.Fatalf("result = %+v, want one unresolved", result)
	}
	if !strings.Contains(logs.String(), "index entry missing") {
		t.Fatalf("log = %s, want index entry missing", logs.String())
	}
}

func TestReconcilerPermanentPersistErrorIsUnresolved(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"message conflict", store.ErrMessageConflict},
		{"seq conflict", store.ErrSeqConflict},
		{"foreign key", &mysql.MySQLError{Number: 1452, Message: "fk"}},
		{"data too long", &mysql.MySQLError{Number: 1406, Message: "too long"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			client, _ := newReconcileTestClient(t)
			now := reconcileTestNow()
			setDirectSequence(t, client, 1, 2, 2)
			seedReconcileDirectMessage(t, client, 1, 2, 102, 2, now.Add(-10*time.Minute))

			fakeStore := newFakeReconcileStore()
			fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 1, MaxSeq: 1}}
			fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1}
			fakeStore.persistErrs[102] = testCase.err

			reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
			result, err := reconciler.RunOnce(context.Background())
			if err != nil {
				t.Fatalf("RunOnce() error = %v, want nil for a permanent error", err)
			}
			if result.Unresolved != 1 || result.Repaired != 0 {
				t.Fatalf("result = %+v, want one unresolved", result)
			}
			persisted, _, attempts, _, _ := fakeStore.snapshot()
			if len(persisted) != 0 || attempts != 1 {
				t.Fatalf("attempts = %d persistent = %d, want a single failed attempt", attempts, len(persisted))
			}
			if reconciler.Status().Unresolved != 1 {
				t.Fatalf("status = %+v, want unresolved 1", reconciler.Status())
			}
		})
	}
}

func TestReconcilerTransientPersistErrorAbortsRun(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	old := now.Add(-10 * time.Minute)
	setDirectSequence(t, client, 1, 2, 2)
	setDirectSequence(t, client, 3, 4, 2)
	seedReconcileDirectMessage(t, client, 1, 2, 102, 2, old)
	seedReconcileDirectMessage(t, client, 3, 4, 202, 2, old)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{
		{LowID: 1, HighID: 2, Count: 1, MaxSeq: 1},
		{LowID: 3, HighID: 4, Count: 1, MaxSeq: 1},
	}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1}
	fakeStore.directSeqs[[2]uint64{3, 4}] = []uint64{1}
	fakeStore.persistErrs[102] = driver.ErrBadConn

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err == nil {
		t.Fatal("RunOnce() error = nil, want the transient error")
	}
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("RunOnce() error = %v, want it to wrap driver.ErrBadConn", err)
	}
	if result.Repaired != 0 || result.Unresolved != 0 {
		t.Fatalf("result = %+v, want an aborted run", result)
	}
	persisted, _, attempts, _, _ := fakeStore.snapshot()
	if len(persisted) != 0 || attempts != 1 {
		t.Fatalf("persisted = %d attempts = %d, want the run aborted after the first failure", len(persisted), attempts)
	}
	status := reconciler.Status()
	if status.LastError == "" || status.Running {
		t.Fatalf("status = %+v, want LastError set and not running", status)
	}
}

func TestReconcilerRunWaitsForGate(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	setDirectSequence(t, client, 1, 2, 1)
	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 1, MaxSeq: 1}}

	gate := NewSeqGate()
	gate.Close()
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Gate:     gate,
		Interval: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		reconciler.Run(ctx)
		close(done)
	}()

	select {
	case <-fakeStore.firstCall:
		t.Fatal("Run called the store while the gate was closed")
	case <-time.After(50 * time.Millisecond):
	}

	gate.Open()
	select {
	case <-fakeStore.firstCall:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not reconcile after the gate opened")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestReconcilerPagesRowsAndLimitsDetail(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	old := now.Add(-10 * time.Minute)
	setDirectSequence(t, client, 1, 2, 3)
	setDirectSequence(t, client, 3, 4, 2)
	setDirectSequence(t, client, 5, 6, 2)
	seedReconcileDirectMessage(t, client, 1, 2, 103, 3, old)
	seedReconcileDirectMessage(t, client, 3, 4, 202, 2, old)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{
		{LowID: 1, HighID: 2, Count: 2, MaxSeq: 2},
		{LowID: 3, HighID: 4, Count: 1, MaxSeq: 1},
		{LowID: 5, HighID: 6, Count: 2, MaxSeq: 2},
	}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1, 2}
	fakeStore.directSeqs[[2]uint64{3, 4}] = []uint64{1}
	fakeStore.directSeqs[[2]uint64{5, 6}] = []uint64{1, 2}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{
		Batch:     1,
		MaxDetail: 1,
	})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Scanned != 3 || result.Mismatched != 2 || result.Repaired != 1 {
		t.Fatalf("result = %+v, want scanned 3 mismatched 2 repaired 1", result)
	}
	persisted, _, _, statsCalls, _ := fakeStore.snapshot()
	if len(persisted) != 1 || persisted[0].ID != 103 {
		t.Fatalf("persisted = %+v, want only the first suspect repaired", persisted)
	}
	if statsCalls < 4 {
		t.Fatalf("stats pages = %d, want every batch and the final empty page", statsCalls)
	}

	result, err = reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if result.Repaired != 1 || result.Mismatched != 1 {
		t.Fatalf("second result = %+v, want the deferred suspect repaired", result)
	}
	persisted, _, _, _, _ = fakeStore.snapshot()
	if len(persisted) != 2 || persisted[1].ID != 202 {
		t.Fatalf("persisted = %+v, want both suspects repaired across runs", persisted)
	}
	if total := reconciler.Status().TotalRepaired; total != 2 {
		t.Fatalf("TotalRepaired = %d, want 2", total)
	}
}

func TestReconcilerSkipsMissingCounter(t *testing.T) {
	client, _ := newReconcileTestClient(t)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 2, MaxSeq: 2}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1, 2}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Scanned != 0 || result.Mismatched != 0 || result.Repaired != 0 {
		t.Fatalf("result = %+v, want the conversation skipped", result)
	}
	_, _, attempts, _, detailCalls := fakeStore.snapshot()
	if attempts != 0 || detailCalls != 0 {
		t.Fatalf("attempts = %d detail calls = %d, want none", attempts, detailCalls)
	}
}

func TestReconcilerRepairsRedisOnlyConversations(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	old := now.Add(-10 * time.Minute)
	setDirectSequence(t, client, 1, 2, 2)
	setGroupSequence(t, client, 7, 1)
	seedReconcileDirectMessage(t, client, 1, 2, 101, 1, old)
	seedReconcileDirectMessage(t, client, 1, 2, 102, 2, old)
	seedReconcileGroupMessage(t, client, 7, 201, 1, old)

	fakeStore := newFakeReconcileStore()
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Scanned != 2 || result.Mismatched != 2 || result.Repaired != 3 {
		t.Fatalf("result = %+v, want 2 conversations and 3 repaired messages", result)
	}
	persisted, persistedGroups, _, _, _ := fakeStore.snapshot()
	if len(persisted) != 2 || persisted[0].ID != 101 || persisted[1].ID != 102 {
		t.Fatalf("persisted = %+v, want 101 then 102", persisted)
	}
	if len(persistedGroups) != 1 || persistedGroups[0].ID != 201 {
		t.Fatalf("persisted groups = %+v, want message 201", persistedGroups)
	}

	result, err = reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if result.Mismatched != 0 || result.Repaired != 0 {
		t.Fatalf("second result = %+v, want healthy after repair", result)
	}
}

func TestReconcileCounterKeyParsing(t *testing.T) {
	lowID, highID, ok := defaultKeyspace.reconcileParseDirectCounterKey(
		defaultKeyspace.directSequenceKey(5, 3),
	)
	if !ok || lowID != 3 || highID != 5 {
		t.Fatalf("direct key parsed as (%d, %d, %v), want (3, 5, true)", lowID, highID, ok)
	}
	if _, _, ok := defaultKeyspace.reconcileParseDirectCounterKey("im:v12:dm:seq:1"); ok {
		t.Fatal("malformed direct key parsed successfully")
	}
	if _, _, ok := defaultKeyspace.reconcileParseDirectCounterKey("im:v12:dm:pair:1:2"); ok {
		t.Fatal("unrelated key parsed as a direct counter")
	}

	groupID, ok := defaultKeyspace.reconcileParseGroupCounterKey(
		defaultKeyspace.groupSequenceKey(7),
	)
	if !ok || groupID != 7 {
		t.Fatalf("group key parsed as (%d, %v), want (7, true)", groupID, ok)
	}
	if _, ok := defaultKeyspace.reconcileParseGroupCounterKey("im:v12:group:7:read"); ok {
		t.Fatal("group read key parsed as a counter")
	}
	if _, ok := defaultKeyspace.reconcileParseGroupCounterKey("im:v12:group:x:seq"); ok {
		t.Fatal("non-numeric group key parsed successfully")
	}
}

func TestReconcileMissingSeqs(t *testing.T) {
	missing := reconcileMissingSeqs([]uint64{1, 2, 5}, 5)
	if len(missing) != 2 || missing[0] != 3 || missing[1] != 4 {
		t.Fatalf("missing = %v, want [3 4]", missing)
	}
	if got := reconcileMissingSeqs([]uint64{1, 2, 3}, 3); len(got) != 0 {
		t.Fatalf("missing = %v, want none", got)
	}
	if got := reconcileMissingSeqs(nil, 0); len(got) != 0 {
		t.Fatalf("missing = %v, want none", got)
	}
}
