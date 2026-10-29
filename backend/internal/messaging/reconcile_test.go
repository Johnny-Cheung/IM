package messaging

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
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
	rangeErr      error

	directMessages map[[2]uint64][]store.Message
	groupMessages  map[uint64][]store.GroupMessage
	userIDs        []uint64
	userPeers      map[uint64][]uint64
	// peerOrder overrides ListConversationBasics ordering to mirror the
	// production store, which returns conversations by latest message rather
	// than by peer.
	peerOrder map[uint64][]uint64
	// userConversations overrides ListConversationBasics with full rows,
	// including the newest message, as the preview audit needs.
	userConversations map[uint64][]store.Conversation
	conversationReads []store.ConversationReadState
	groupReads        []store.GroupMemberReadState

	persisted        []store.Message
	persistedGroups  []store.GroupMessage
	attempts         int
	statsCalls       int
	detailCalls      int
	rangeCalls       int
	directRangeCount int
	userCountCalls   []uint64
	directSeqCalls   []fakeSeqCall
	groupSeqCalls    []fakeSeqCall
	rangeQueries     []fakeRangeQuery

	firstCall  chan struct{}
	signalOnce sync.Once
}

func newFakeReconcileStore() *fakeReconcileStore {
	return &fakeReconcileStore{
		directSeqs:     make(map[[2]uint64][]uint64),
		groupSeqs:      make(map[uint64][]uint64),
		persistErrs:    make(map[uint64]error),
		directMessages: make(map[[2]uint64][]store.Message),
		groupMessages:  make(map[uint64][]store.GroupMessage),
		userPeers:      make(map[uint64][]uint64),
		firstCall:      make(chan struct{}),
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
	lowID, highID, upTo, afterSeq uint64,
	limit int,
) ([]uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.detailCalls++
	f.directSeqCalls = append(f.directSeqCalls, fakeSeqCall{
		lowID: lowID, highID: highID, upTo: upTo, afterSeq: afterSeq,
	})
	if f.seqsErr != nil {
		return nil, f.seqsErr
	}
	return filterSeqs(f.directSeqs[[2]uint64{lowID, highID}], upTo, afterSeq, limit), nil
}

func (f *fakeReconcileStore) ListGroupSeqs(
	_ context.Context,
	groupID, upTo, afterSeq uint64,
	limit int,
) ([]uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.detailCalls++
	f.groupSeqCalls = append(f.groupSeqCalls, fakeSeqCall{
		lowID: groupID, upTo: upTo, afterSeq: afterSeq,
	})
	if f.seqsErr != nil {
		return nil, f.seqsErr
	}
	return filterSeqs(f.groupSeqs[groupID], upTo, afterSeq, limit), nil
}

// fakeSeqCall records one paged seq query so a test can prove where a walk
// started and that it used cursors instead of rescanning from zero.
type fakeSeqCall struct {
	lowID, highID  uint64
	upTo, afterSeq uint64
}

// fakeRangeQuery records one ListMessagesBySeqRange call so a test can prove
// which conversation and offset an audit resumed from.
type fakeRangeQuery struct {
	lowID, highID uint64
	afterSeq      uint64
	limit         int
}

func (f *fakeReconcileStore) rangeQuerySnapshot() []fakeRangeQuery {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeRangeQuery(nil), f.rangeQueries...)
}

func (f *fakeReconcileStore) seqCalls() ([]fakeSeqCall, []fakeSeqCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeSeqCall(nil), f.directSeqCalls...),
		append([]fakeSeqCall(nil), f.groupSeqCalls...)
}

func filterSeqs(stored []uint64, upTo, afterSeq uint64, limit int) []uint64 {
	if limit <= 0 {
		return nil
	}
	seqs := stored
	if !sort.SliceIsSorted(seqs, func(i, j int) bool { return seqs[i] < seqs[j] }) {
		seqs = append([]uint64(nil), stored...)
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	}
	start := sort.Search(len(seqs), func(i int) bool { return seqs[i] > afterSeq })
	result := make([]uint64, 0, limit)
	for _, seq := range seqs[start:] {
		if seq > upTo {
			break
		}
		result = append(result, seq)
		if len(result) == limit {
			break
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

func (f *fakeReconcileStore) ListMessagesBySeqRange(
	_ context.Context,
	lowID, highID, afterSeq uint64,
	limit int,
) ([]store.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rangeCalls++
	f.directRangeCount++
	f.rangeQueries = append(f.rangeQueries, fakeRangeQuery{
		lowID: lowID, highID: highID, afterSeq: afterSeq, limit: limit,
	})
	if f.rangeErr != nil {
		return nil, f.rangeErr
	}
	if f.seqsErr != nil {
		return nil, f.seqsErr
	}
	if limit <= 0 {
		return nil, nil
	}
	messages := append([]store.Message(nil), f.directMessages[[2]uint64{lowID, highID}]...)
	sort.Slice(messages, func(i, j int) bool { return messages[i].Seq < messages[j].Seq })
	result := make([]store.Message, 0, limit)
	for _, message := range messages {
		if message.Seq <= afterSeq {
			continue
		}
		result = append(result, message)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeReconcileStore) ListGroupMessagesBySeqRange(
	_ context.Context,
	groupID, afterSeq uint64,
	limit int,
) ([]store.GroupMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rangeCalls++
	if f.rangeErr != nil {
		return nil, f.rangeErr
	}
	if f.seqsErr != nil {
		return nil, f.seqsErr
	}
	if limit <= 0 {
		return nil, nil
	}
	messages := append([]store.GroupMessage(nil), f.groupMessages[groupID]...)
	sort.Slice(messages, func(i, j int) bool { return messages[i].Seq < messages[j].Seq })
	result := make([]store.GroupMessage, 0, limit)
	for _, message := range messages {
		if message.Seq <= afterSeq {
			continue
		}
		result = append(result, message)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeReconcileStore) CountMessagesInvolvingUser(
	_ context.Context,
	userID uint64,
) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.userCountCalls = append(f.userCountCalls, userID)
	if f.seqsErr != nil {
		return 0, f.seqsErr
	}
	var count uint64
	for _, messages := range f.directMessages {
		for _, message := range messages {
			if message.SenderID == userID || message.RecipientID == userID {
				count++
			}
		}
	}
	return count, nil
}

func (f *fakeReconcileStore) ListUserIDs(
	_ context.Context,
	afterID uint64,
	limit int,
) ([]uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rangeCalls++
	if f.seqsErr != nil {
		return nil, f.seqsErr
	}
	if limit <= 0 {
		return nil, nil
	}
	ids := append([]uint64(nil), f.userIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	result := make([]uint64, 0, limit)
	for _, id := range ids {
		if id <= afterID {
			continue
		}
		result = append(result, id)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeReconcileStore) ListConversationReadStates(
	_ context.Context,
	afterUserID, afterPeerID uint64,
	limit int,
) ([]store.ConversationReadState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rangeCalls++
	if f.seqsErr != nil {
		return nil, f.seqsErr
	}
	if limit <= 0 {
		return nil, nil
	}
	rows := append([]store.ConversationReadState(nil), f.conversationReads...)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].UserID != rows[j].UserID {
			return rows[i].UserID < rows[j].UserID
		}
		return rows[i].PeerID < rows[j].PeerID
	})
	result := make([]store.ConversationReadState, 0, limit)
	for _, row := range rows {
		if row.UserID < afterUserID || (row.UserID == afterUserID && row.PeerID <= afterPeerID) {
			continue
		}
		result = append(result, row)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeReconcileStore) ListGroupMemberReadStates(
	_ context.Context,
	afterGroupID, afterUserID uint64,
	limit int,
) ([]store.GroupMemberReadState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rangeCalls++
	if f.seqsErr != nil {
		return nil, f.seqsErr
	}
	if limit <= 0 {
		return nil, nil
	}
	rows := append([]store.GroupMemberReadState(nil), f.groupReads...)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].GroupID != rows[j].GroupID {
			return rows[i].GroupID < rows[j].GroupID
		}
		return rows[i].UserID < rows[j].UserID
	})
	result := make([]store.GroupMemberReadState, 0, limit)
	for _, row := range rows {
		if row.GroupID < afterGroupID || (row.GroupID == afterGroupID && row.UserID <= afterUserID) {
			continue
		}
		result = append(result, row)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeReconcileStore) ListConversationBasics(
	_ context.Context,
	userID uint64,
) ([]store.Conversation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rangeCalls++
	if f.seqsErr != nil {
		return nil, f.seqsErr
	}
	if conversations, ok := f.userConversations[userID]; ok {
		return append([]store.Conversation(nil), conversations...), nil
	}
	peers := append([]uint64(nil), f.userPeers[userID]...)
	if order, ok := f.peerOrder[userID]; ok {
		// Mirror the production store's order (latest message first).
		peers = append([]uint64(nil), order...)
	} else {
		sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	}
	conversations := make([]store.Conversation, 0, len(peers))
	for _, peer := range peers {
		conversations = append(conversations, store.Conversation{UserID: peer})
	}
	return conversations, nil
}

func (f *fakeReconcileStore) snapshot() ([]store.Message, []store.GroupMessage, int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.Message(nil), f.persisted...),
		append([]store.GroupMessage(nil), f.persistedGroups...),
		f.attempts, f.statsCalls, f.detailCalls
}

func (f *fakeReconcileStore) directRangeCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.directRangeCount
}

func (f *fakeReconcileStore) userCountCallsSnapshot() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.userCountCalls...)
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
	seedReconcileDirectIndex(t, client, 1, 2, 101, 1)
	seedReconcileDirectIndex(t, client, 1, 2, 102, 2)
	seedReconcileDirectIndex(t, client, 1, 2, 103, 3)
	seedReconcileGroupIndex(t, client, 7, 201, 1)
	seedReconcileGroupIndex(t, client, 7, 202, 2)
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

// A missing counter used to be skipped by the reverse audit, which left every
// send returning seq_unavailable or dead-lettering on a seq conflict. The
// reverse check now runs first; see
// TestReconcilerRepairsMissingCounterAndEmptyIndex for the repair path.
func TestReconcilerMissingCounterStillCountsAsScanned(t *testing.T) {
	client, _ := newReconcileTestClient(t)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 2, MaxSeq: 2}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1, 2}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	// With no repairer the counter cannot be raised, but the conversation is
	// still examined and its counter plus whole index gap are reported.
	if result.Scanned != 1 || result.RedisMissing != 3 || result.CountersRaised != 0 {
		t.Fatalf("result = %+v, want the missing counter audited", result)
	}
	if result.Mismatched != 0 || result.Repaired != 0 {
		t.Fatalf("result = %+v, want the forward direction untouched", result)
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

func TestWalkMissingSeqs(t *testing.T) {
	stored := []uint64{1, 2, 5}
	fetch := func(_ context.Context, after uint64) ([]uint64, error) {
		return filterSeqs(stored, 5, after, 100), nil
	}
	var got []uint64
	next, truncated, err := walkMissingSeqs(
		context.Background(), 1, 5, 100, 100, fetch,
		func(seq uint64) error {
			got = append(got, seq)
			return nil
		},
	)
	if err != nil || truncated || next != 0 {
		t.Fatalf("walk = (next %d, truncated %v, err %v), want complete", next, truncated, err)
	}
	if len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("missing = %v, want [3 4]", got)
	}

	got = nil
	next, truncated, err = walkMissingSeqs(
		context.Background(), 1, 5, 100, 1, fetch,
		func(seq uint64) error {
			got = append(got, seq)
			return nil
		},
	)
	if err != nil || !truncated || next != 4 {
		t.Fatalf("capped walk = (next %d, truncated %v, err %v), want next 4", next, truncated, err)
	}
	if len(got) != 1 || got[0] != 3 {
		t.Fatalf("capped missing = %v, want [3]", got)
	}

	empty := func(_ context.Context, _ uint64) ([]uint64, error) { return nil, nil }
	next, truncated, err = walkMissingSeqs(
		context.Background(), 1, 3, 100, 100, empty,
		func(uint64) error { return nil },
	)
	if err != nil || truncated || next != 0 {
		t.Fatalf("empty walk = (next %d, truncated %v, err %v), want complete", next, truncated, err)
	}
}

func TestWalkMissingSeqsPagesWithBoundedCalls(t *testing.T) {
	stored := make([]uint64, 0, 9)
	for seq := uint64(1); seq <= 10; seq++ {
		if seq == 5 {
			continue
		}
		stored = append(stored, seq)
	}
	calls := 0
	fetch := func(_ context.Context, after uint64) ([]uint64, error) {
		calls++
		return filterSeqs(stored, 10, after, 3), nil
	}
	var got []uint64
	next, truncated, err := walkMissingSeqs(
		context.Background(), 1, 10, 3, 100, fetch,
		func(seq uint64) error {
			got = append(got, seq)
			return nil
		},
	)
	if err != nil || truncated || next != 0 {
		t.Fatalf("walk = (next %d, truncated %v, err %v), want complete", next, truncated, err)
	}
	if len(got) != 1 || got[0] != 5 {
		t.Fatalf("missing = %v, want [5]", got)
	}
	if calls != 3 {
		t.Fatalf("page calls = %d, want 3 (9 stored seqs in pages of 3)", calls)
	}
}

// seedReconcileEntity writes an entity hash field by field so a test can omit
// or blank a field.
func seedReconcileEntity(t *testing.T, client redis.UniversalClient, key string, fields map[string]interface{}) {
	t.Helper()
	if err := client.HSet(context.Background(), key, fields).Err(); err != nil {
		t.Fatalf("seed entity %s: %v", key, err)
	}
}

func TestReconcilerPhasesHaveIndependentBudgets(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	old := now.Add(-10 * time.Minute)
	logs := captureReconcileLog(t)

	fakeStore := newFakeReconcileStore()
	const suspects = 60
	for index := 0; index < suspects; index++ {
		lowID := uint64(2*index + 1)
		highID := lowID + 1
		setDirectSequence(t, client, lowID, highID, 2)
		pair := [2]uint64{lowID, highID}
		fakeStore.conversations = append(fakeStore.conversations, store.ConversationSeqStat{
			LowID: lowID, HighID: highID, Count: 1, MaxSeq: 1,
		})
		fakeStore.directSeqs[pair] = []uint64{1}
	}
	setGroupSequence(t, client, 7, 2)
	seedReconcileGroupMessage(t, client, 7, 202, 2, old)
	fakeStore.groups = []store.GroupSeqStat{{GroupID: 7, Count: 1, MaxSeq: 1}}
	fakeStore.groupSeqs[7] = []uint64{1}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Unresolved != 50 || result.Repaired != 1 {
		t.Fatalf("result = %+v, want 50 unresolved direct suspects and the group repaired", result)
	}
	_, persistedGroups, _, _, _ := fakeStore.snapshot()
	if len(persistedGroups) != 1 || persistedGroups[0].ID != 202 {
		t.Fatalf("persisted groups = %+v, want group message 202 repaired despite the spent direct budget", persistedGroups)
	}
	lateMarker := "reconcile unresolved direct:119:120:2"
	if strings.Contains(logs.String(), lateMarker) {
		t.Fatalf("60th direct suspect was attempted in run 1: %s", logs.String())
	}

	if _, err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if !strings.Contains(logs.String(), lateMarker) {
		t.Fatalf("60th direct suspect was not reached on the next run: %s", logs.String())
	}
}

func TestReconcilerRotatesToLaterSuspect(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	old := now.Add(-10 * time.Minute)
	setDirectSequence(t, client, 1, 2, 2)
	setDirectSequence(t, client, 3, 4, 2)
	setDirectSequence(t, client, 5, 6, 2)
	seedReconcileDirectMessage(t, client, 5, 6, 106, 2, old)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{
		{LowID: 1, HighID: 2, Count: 1, MaxSeq: 1},
		{LowID: 3, HighID: 4, Count: 1, MaxSeq: 1},
		{LowID: 5, HighID: 6, Count: 1, MaxSeq: 1},
	}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1}
	fakeStore.directSeqs[[2]uint64{3, 4}] = []uint64{1}
	fakeStore.directSeqs[[2]uint64{5, 6}] = []uint64{1}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{MaxDetail: 2})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Unresolved != 2 || result.Repaired != 0 {
		t.Fatalf("first result = %+v, want the two repair-less suspects attempted", result)
	}
	result, err = reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if result.Repaired != 1 {
		t.Fatalf("second result = %+v, want the later repairable conversation reached", result)
	}
	persisted, _, _, _, _ := fakeStore.snapshot()
	if len(persisted) != 1 || persisted[0].ID != 106 {
		t.Fatalf("persisted = %+v, want message 106", persisted)
	}
}

func TestReconcilerRejectsCounterAboveMaxSafeSeq(t *testing.T) {
	cases := []struct {
		name    string
		counter uint64
	}{
		{"above max safe seq", store.MaxSafeSeq + 1},
		{"max uint64", ^uint64(0)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			client, _ := newReconcileTestClient(t)
			setDirectSequence(t, client, 1, 2, testCase.counter)

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
				t.Fatalf("result = %+v, want one unresolved and no repair", result)
			}
			_, _, attempts, _, detailCalls := fakeStore.snapshot()
			if attempts != 0 || detailCalls != 0 {
				t.Fatalf("attempts = %d detail calls = %d, want the counter rejected before any lookup", attempts, detailCalls)
			}
			if !strings.Contains(logs.String(), "exceeds MaxSafeSeq") {
				t.Fatalf("log = %s, want the out-of-range counter reported", logs.String())
			}
		})
	}
}

func TestReconcilerPagesLargeConversationGapWalk(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	const gap = 100001
	const stored = 200000
	const upTo = 200001
	setDirectSequence(t, client, 1, 2, upTo)
	seedReconcileDirectMessage(t, client, 1, 2, 900001, gap, now.Add(-10*time.Minute))

	seqs := make([]uint64, 0, stored)
	for seq := uint64(1); seq <= upTo; seq++ {
		if seq == gap {
			continue
		}
		seqs = append(seqs, seq)
	}
	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: stored, MaxSeq: upTo}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = seqs

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{Batch: 1000})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Repaired != 1 || result.Unresolved != 0 {
		t.Fatalf("result = %+v, want the single gap repaired", result)
	}
	persisted, _, attempts, _, detailCalls := fakeStore.snapshot()
	if len(persisted) != 1 || persisted[0].Seq != gap || persisted[0].ID != 900001 {
		t.Fatalf("persisted = %+v, want only seq %d", persisted, gap)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if detailCalls < 2 || detailCalls > 202 {
		t.Fatalf("detail page calls = %d, want the conversation walked in pages of 1000", detailCalls)
	}
}

// COUNT(*) == MAX(seq) proves every seq in 1..MaxSeq is persisted, so a pure
// tail lag must be walked from MaxSeq+1, not by paging the whole conversation.
func TestReconcilerPureTailLagStartsAfterProvenPrefix(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	const maxSeq uint64 = 200000
	const counter = maxSeq + 1
	setDirectSequence(t, client, 1, 2, counter)
	seedReconcileDirectMessage(t, client, 1, 2, 900001, counter, now.Add(-10*time.Minute))

	seqs := make([]uint64, 0, maxSeq)
	for seq := uint64(1); seq <= maxSeq; seq++ {
		seqs = append(seqs, seq)
	}
	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{
		LowID: 1, HighID: 2, Count: maxSeq, MaxSeq: maxSeq,
	}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = seqs

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	// A stale cursor below the proven prefix must not force a rescan of it.
	reconciler.mu.Lock()
	reconciler.missingSeqs[reconcileSeqCursorKey{first: 1, second: 2}] = 100
	reconciler.mu.Unlock()

	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Repaired != 1 || result.Unresolved != 0 {
		t.Fatalf("result = %+v, want only the tail seq repaired", result)
	}
	persisted, _, _, _, _ := fakeStore.snapshot()
	if len(persisted) != 1 || persisted[0].Seq != counter || persisted[0].ID != 900001 {
		t.Fatalf("persisted = %+v, want only seq %d", persisted, counter)
	}
	directCalls, _ := fakeStore.seqCalls()
	if len(directCalls) != 1 {
		t.Fatalf("detail page calls = %d, want 1: %+v", len(directCalls), directCalls)
	}
	if directCalls[0].afterSeq != maxSeq {
		t.Fatalf("first page afterSeq = %d, want %d (must not scan the proven prefix)", directCalls[0].afterSeq, maxSeq)
	}
	if directCalls[0].upTo != counter {
		t.Fatalf("first page upTo = %d, want %d", directCalls[0].upTo, counter)
	}
}

func TestReconcilerPureTailLagStartsAfterProvenPrefixGroup(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	const maxSeq uint64 = 200000
	const counter = maxSeq + 1
	setGroupSequence(t, client, 7, counter)
	seedReconcileGroupMessage(t, client, 7, 900002, counter, now.Add(-10*time.Minute))

	seqs := make([]uint64, 0, maxSeq)
	for seq := uint64(1); seq <= maxSeq; seq++ {
		seqs = append(seqs, seq)
	}
	fakeStore := newFakeReconcileStore()
	fakeStore.groups = []store.GroupSeqStat{{GroupID: 7, Count: maxSeq, MaxSeq: maxSeq}}
	fakeStore.groupSeqs[7] = seqs

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Repaired != 1 || result.Unresolved != 0 {
		t.Fatalf("result = %+v, want only the tail seq repaired", result)
	}
	_, persistedGroups, _, _, _ := fakeStore.snapshot()
	if len(persistedGroups) != 1 || persistedGroups[0].Seq != counter || persistedGroups[0].ID != 900002 {
		t.Fatalf("persisted groups = %+v, want only seq %d", persistedGroups, counter)
	}
	_, groupCalls := fakeStore.seqCalls()
	if len(groupCalls) != 1 {
		t.Fatalf("detail page calls = %d, want 1: %+v", len(groupCalls), groupCalls)
	}
	if groupCalls[0].afterSeq != maxSeq || groupCalls[0].lowID != 7 {
		t.Fatalf("first group page = %+v, want group 7 after %d (no prefix scan)", groupCalls[0], maxSeq)
	}
}

// A real hole below MAX(seq) makes COUNT(*) != MAX(seq), which proves nothing
// about the prefix, so the walk must still start at seq 1 and page through it.
func TestReconcilerMidGapStillWalksFromStart(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	const maxSeq uint64 = 20000
	const gap uint64 = 10000
	setDirectSequence(t, client, 1, 2, maxSeq)
	seedReconcileDirectMessage(t, client, 1, 2, 900010, gap, now.Add(-10*time.Minute))

	seqs := make([]uint64, 0, maxSeq-1)
	for seq := uint64(1); seq <= maxSeq; seq++ {
		if seq == gap {
			continue
		}
		seqs = append(seqs, seq)
	}
	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{
		LowID: 1, HighID: 2, Count: maxSeq - 1, MaxSeq: maxSeq,
	}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = seqs

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Repaired != 1 || result.Unresolved != 0 {
		t.Fatalf("result = %+v, want the mid gap repaired", result)
	}
	persisted, _, _, _, _ := fakeStore.snapshot()
	if len(persisted) != 1 || persisted[0].Seq != gap || persisted[0].ID != 900010 {
		t.Fatalf("persisted = %+v, want seq %d", persisted, gap)
	}
	directCalls, _ := fakeStore.seqCalls()
	if len(directCalls) < 2 {
		t.Fatalf("detail page calls = %d, want the prefix paged", len(directCalls))
	}
	if directCalls[0].afterSeq != 0 {
		t.Fatalf("first page afterSeq = %d, want 0 when the prefix is unproven", directCalls[0].afterSeq)
	}
}

// A hole and a tail can be missing at once. The count/max stats prove nothing
// about the prefix, so the same walk must cover both ends.
func TestReconcilerHoleAndTailAreBothRepaired(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	const maxSeq uint64 = 20000
	const gap uint64 = 10000
	const counter = maxSeq + 3
	old := now.Add(-10 * time.Minute)
	setDirectSequence(t, client, 1, 2, counter)
	seedReconcileDirectMessage(t, client, 1, 2, 910000, gap, old)
	for seq := maxSeq + 1; seq <= counter; seq++ {
		seedReconcileDirectMessage(t, client, 1, 2, 900000+seq, seq, old)
	}

	seqs := make([]uint64, 0, maxSeq-1)
	for seq := uint64(1); seq <= maxSeq; seq++ {
		if seq == gap {
			continue
		}
		seqs = append(seqs, seq)
	}
	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{
		LowID: 1, HighID: 2, Count: maxSeq - 1, MaxSeq: maxSeq,
	}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = seqs

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Repaired != 4 || result.Unresolved != 0 {
		t.Fatalf("result = %+v, want the hole and all three tail seqs repaired", result)
	}
	persisted, _, _, _, _ := fakeStore.snapshot()
	gotSeqs := make([]uint64, 0, len(persisted))
	for _, message := range persisted {
		gotSeqs = append(gotSeqs, message.Seq)
	}
	sort.Slice(gotSeqs, func(i, j int) bool { return gotSeqs[i] < gotSeqs[j] })
	want := []uint64{gap, maxSeq + 1, maxSeq + 2, maxSeq + 3}
	if fmt.Sprint(gotSeqs) != fmt.Sprint(want) {
		t.Fatalf("repaired seqs = %v, want %v", gotSeqs, want)
	}
	directCalls, _ := fakeStore.seqCalls()
	if len(directCalls) == 0 || directCalls[0].afterSeq != 0 {
		t.Fatalf("first page = %+v, want a walk from 0 when the prefix is unproven", directCalls)
	}
}

func TestReconcileSeqLowerBound(t *testing.T) {
	cases := []struct {
		name                   string
		count, maxSeq, counter uint64
		want                   uint64
	}{
		{"pure tail", 5, 5, 6, 6},
		{"hole below max", 4, 5, 5, 1},
		{"hole plus tail", 4, 5, 6, 1},
		{"counter not ahead", 5, 5, 5, 1},
		{"no numbered rows", 0, 0, 1, 1},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := reconcileSeqLowerBound(testCase.count, testCase.maxSeq, testCase.counter); got != testCase.want {
				t.Fatalf("reconcileSeqLowerBound(%d, %d, %d) = %d, want %d",
					testCase.count, testCase.maxSeq, testCase.counter, got, testCase.want)
			}
		})
	}
}

func TestReconcilerCapsMissingSeqsPerConversationAndResumes(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	setDirectSequence(t, client, 1, 2, 1501)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 1, MaxSeq: 1}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Unresolved != reconcileMaxMissingPerConversation || result.Deferred != 1 {
		t.Fatalf("first result = %+v, want the per-conversation cap hit", result)
	}
	result, err = reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if result.Unresolved != 500 || result.Deferred != 0 {
		t.Fatalf("second result = %+v, want the remaining 500 seqs retried", result)
	}
	if result.Unresolved+reconcileMaxMissingPerConversation != 1500 {
		t.Fatalf("walked %d + %d seqs, want the whole 1500-seq range covered", reconcileMaxMissingPerConversation, result.Unresolved)
	}
}

func TestReconcilerLogThrottleKeepsFreshEntries(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	setDirectSequence(t, client, 1, 2, 2)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 1, MaxSeq: 1}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1}
	logs := captureReconcileLog(t)
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})

	reconciler.mu.Lock()
	for index := 0; index < reconcileLogKeyLimit; index++ {
		reconciler.logged[fmt.Sprintf("pre:%d", index)] = now
	}
	reconciler.mu.Unlock()

	if _, err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if strings.Contains(logs.String(), "reconcile unresolved direct:1:2:2") {
		t.Fatalf("a new item was logged while the map was full of fresh entries: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "suppressed=1") {
		t.Fatalf("summary = %s, want the suppressed item counted", logs.String())
	}
	reconciler.mu.Lock()
	size := len(reconciler.logged)
	_, kept := reconciler.logged["pre:0"]
	reconciler.mu.Unlock()
	if size != reconcileLogKeyLimit || !kept {
		t.Fatalf("logged map = %d entries (pre:0 kept %v), want fresh entries kept", size, kept)
	}
}

func TestReconcilerLogThrottleExpiresStaleEntries(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	now := reconcileTestNow()
	setDirectSequence(t, client, 1, 2, 2)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 1, MaxSeq: 1}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1}
	logs := captureReconcileLog(t)
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})

	reconciler.mu.Lock()
	for index := 0; index < reconcileLogKeyLimit; index++ {
		reconciler.logged[fmt.Sprintf("stale:%d", index)] = now.Add(-2 * time.Hour)
	}
	reconciler.mu.Unlock()

	if _, err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if !strings.Contains(logs.String(), "reconcile unresolved direct:1:2:2") {
		t.Fatalf("log = %s, want the expired map to make room", logs.String())
	}
	if !strings.Contains(logs.String(), "suppressed=0") {
		t.Fatalf("summary = %s, want nothing suppressed", logs.String())
	}
}

func TestReconcilerLogPerRunCap(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	setDirectSequence(t, client, 1, 2, 1501)

	fakeStore := newFakeReconcileStore()
	fakeStore.conversations = []store.ConversationSeqStat{{LowID: 1, HighID: 2, Count: 1, MaxSeq: 1}}
	fakeStore.directSeqs[[2]uint64{1, 2}] = []uint64{1}
	logs := captureReconcileLog(t)

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	if _, err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if got := strings.Count(logs.String(), "reconcile unresolved direct:1:2:"); got != reconcileMaxLogsPerRun {
		t.Fatalf("unresolved detail lines = %d, want the per-run cap %d", got, reconcileMaxLogsPerRun)
	}
	if !strings.Contains(logs.String(), "suppressed=500") {
		t.Fatalf("summary = %s, want 500 suppressed items reported", logs.String())
	}
}

func TestReconcilerIncompleteEntityNotPersisted(t *testing.T) {
	cases := []struct {
		name   string
		fields map[string]interface{}
	}{
		{
			name: "missing body",
			fields: map[string]interface{}{
				"id": 102, "seq": 2, "sender_id": 1, "recipient_id": 2,
				"client_message_id": "c-102", "created_at_ms": reconcileTestNow().Add(-10 * time.Minute).UnixMilli(),
			},
		},
		{
			name: "blank body",
			fields: map[string]interface{}{
				"id": 102, "seq": 2, "sender_id": 1, "recipient_id": 2,
				"client_message_id": "c-102", "body": "   ",
				"created_at_ms": reconcileTestNow().Add(-10 * time.Minute).UnixMilli(),
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			client, _ := newReconcileTestClient(t)
			setDirectSequence(t, client, 1, 2, 2)
			seedReconcileDirectIndex(t, client, 1, 2, 102, 2)
			seedReconcileEntity(t, client, defaultKeyspace.messageKey(102), testCase.fields)

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
				t.Fatalf("result = %+v, want one unresolved and no repair", result)
			}
			_, _, attempts, _, _ := fakeStore.snapshot()
			if attempts != 0 {
				t.Fatalf("attempts = %d, want the incomplete entity rejected before PersistMessage", attempts)
			}
			if !strings.Contains(logs.String(), "entity is incomplete") {
				t.Fatalf("log = %s, want the incomplete entity reported", logs.String())
			}
		})
	}
}

func TestReconcilerIncompleteGroupEntityNotPersisted(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	setGroupSequence(t, client, 7, 2)
	seedReconcileGroupIndex(t, client, 7, 201, 2)
	seedReconcileEntity(t, client, defaultKeyspace.groupMessageKey(201), map[string]interface{}{
		"id": 201, "seq": 2, "group_id": 7, "sender_id": 42,
		"client_message_id": "c-201", "body": "body-201",
		"created_at_ms":   reconcileTestNow().Add(-10 * time.Minute).UnixMilli(),
		"sender_username": "alice",
	})

	fakeStore := newFakeReconcileStore()
	fakeStore.groups = []store.GroupSeqStat{{GroupID: 7, Count: 1, MaxSeq: 1}}
	fakeStore.groupSeqs[7] = []uint64{1}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	result, err := reconciler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Unresolved != 1 || result.Repaired != 0 {
		t.Fatalf("result = %+v, want the missing sender snapshot reported", result)
	}
	_, persistedGroups, attempts, _, _ := fakeStore.snapshot()
	if attempts != 0 || len(persistedGroups) != 0 {
		t.Fatalf("attempts = %d groups = %+v, want the incomplete group entity rejected", attempts, persistedGroups)
	}
}

// The feed audit's resume cursor is peer-ordered, but the store returns
// conversations by latest message. Paging must sort by peer, otherwise the walk
// oscillates between the newest conversations and never reaches a quiet one.
func TestReconcilerFeedCursorFollowsPeerOrderNotRecency(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	ctx := context.Background()

	fakeStore := newFakeReconcileStore()
	// Recency order: peer 500 is first, peer 100 second.
	fakeStore.userPeers[1] = []uint64{500, 100}
	fakeStore.peerOrder = map[uint64][]uint64{1: {500, 100}}

	const (
		firstPeerMessages  = 2000
		secondPeerMessages = 3000
		missingSeq         = 2500
	)
	missingID := uint64(700000 + missingSeq)
	feedMembers := make([]redis.Z, 0, firstPeerMessages+secondPeerMessages-1)
	for seq := uint64(1); seq <= firstPeerMessages; seq++ {
		message := store.Message{
			ID: uint64(100000 + seq), Seq: seq, SenderID: 1, RecipientID: 500,
			ClientMessageID: fmt.Sprintf("a-%d", seq), Body: fmt.Sprintf("a-%d", seq),
		}
		fakeStore.directMessages[[2]uint64{1, 500}] = append(fakeStore.directMessages[[2]uint64{1, 500}], message)
		feedMembers = append(feedMembers, redis.Z{Score: float64(message.ID), Member: strconvUint(message.ID)})
	}
	for seq := uint64(1); seq <= secondPeerMessages; seq++ {
		message := store.Message{
			ID: uint64(700000 + seq), Seq: seq, SenderID: 1, RecipientID: 100,
			ClientMessageID: fmt.Sprintf("b-%d", seq), Body: fmt.Sprintf("b-%d", seq),
		}
		fakeStore.directMessages[[2]uint64{1, 100}] = append(fakeStore.directMessages[[2]uint64{1, 100}], message)
		if seq != missingSeq {
			feedMembers = append(feedMembers, redis.Z{Score: float64(message.ID), Member: strconvUint(message.ID)})
		}
	}
	if err := client.ZAdd(ctx, defaultKeyspace.directFeedKey(1), feedMembers...).Err(); err != nil {
		t.Fatalf("seed feed: %v", err)
	}

	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	missing, err := reconciler.missingFeedMessages(ctx, 1, 1)
	if err != nil {
		t.Fatalf("first missingFeedMessages() error = %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("first missing = %+v, want none in the first 2000 checks", missing)
	}
	if cursor := reconciler.feedCursorStart(1); cursor.peer != 100 || cursor.seq != firstPeerMessages {
		t.Fatalf("first cursor = %+v, want the peer-ordered second conversation at seq %d", cursor, firstPeerMessages)
	}

	missing, err = reconciler.missingFeedMessages(ctx, 1, 1)
	if err != nil {
		t.Fatalf("second missingFeedMessages() error = %v", err)
	}
	if len(missing) != 1 || missing[0].ID != missingID {
		t.Fatalf("second missing = %+v, want message %d found by resuming", missing, missingID)
	}
	if cursor := reconciler.feedCursorStart(1); cursor.peer != 0 {
		t.Fatalf("second cursor = %+v, want it cleared after the gap was covered", cursor)
	}
	for _, query := range fakeStore.rangeQuerySnapshot() {
		if query.lowID == 1 && query.highID == 500 && query.afterSeq == 0 {
			t.Fatalf("recency-first conversation was rescanned from seq 0: %+v", query)
		}
	}
}

// needs-rebuild and repair-error for the same conversation must both reach the
// log inside one throttling window.
func TestReconcilerLogThrottleSeparatesEventKinds(t *testing.T) {
	client, _ := newReconcileTestClient(t)
	fakeStore := newFakeReconcileStore()
	logs := captureReconcileLog(t)
	reconciler := newTestReconciler(client, fakeStore, ReconcileOptions{})
	now := reconcileTestNow()

	reconciler.logThrottled(now, "needs-rebuild", "direct:1:2:index", "redis index gap")
	reconciler.logThrottled(now, "repair-error", "direct:1:2:index", "write missing messages")

	if got := strings.Count(logs.String(), "direct:1:2:index"); got != 2 {
		t.Fatalf("log lines = %d, want both events: %s", got, logs.String())
	}
}
