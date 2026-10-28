package messaging

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

type fakeDirectMessage struct {
	id        uint64
	senderID  uint64
	recipient uint64
	lowID     uint64
	highID    uint64
	seq       uint64
}

type fakeGroupMessage struct {
	id      uint64
	groupID uint64
	seq     uint64
}

type fakeSeqBackfillStore struct {
	direct    map[uint64]*fakeDirectMessage
	groups    map[uint64]*fakeGroupMessage
	convReads map[[2]uint64]store.ConversationReadState
	groupRead map[[2]uint64]store.GroupMemberReadState
}

func newFakeSeqBackfillStore() *fakeSeqBackfillStore {
	return &fakeSeqBackfillStore{
		direct:    make(map[uint64]*fakeDirectMessage),
		groups:    make(map[uint64]*fakeGroupMessage),
		convReads: make(map[[2]uint64]store.ConversationReadState),
		groupRead: make(map[[2]uint64]store.GroupMemberReadState),
	}
}

func (f *fakeSeqBackfillStore) directSorted() []*fakeDirectMessage {
	rows := make([]*fakeDirectMessage, 0, len(f.direct))
	for _, row := range f.direct {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	return rows
}

func (f *fakeSeqBackfillStore) groupSorted() []*fakeGroupMessage {
	rows := make([]*fakeGroupMessage, 0, len(f.groups))
	for _, row := range f.groups {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	return rows
}

func (f *fakeSeqBackfillStore) ListMessageSeqIndex(
	_ context.Context,
	afterServerMessageID uint64,
	limit int,
) ([]store.MessageSeqIndex, error) {
	result := make([]store.MessageSeqIndex, 0, limit)
	for _, row := range f.directSorted() {
		if row.id <= afterServerMessageID {
			continue
		}
		result = append(result, store.MessageSeqIndex{
			ServerMessageID: row.id,
			Seq:             row.seq,
			LowID:           row.lowID,
			HighID:          row.highID,
			SenderID:        row.senderID,
			RecipientID:     row.recipient,
		})
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeSeqBackfillStore) ListGroupMessageSeqIndex(
	_ context.Context,
	afterServerMessageID uint64,
	limit int,
) ([]store.GroupMessageSeqIndex, error) {
	result := make([]store.GroupMessageSeqIndex, 0, limit)
	for _, row := range f.groupSorted() {
		if row.id <= afterServerMessageID {
			continue
		}
		result = append(result, store.GroupMessageSeqIndex{
			ServerMessageID: row.id,
			Seq:             row.seq,
			GroupID:         row.groupID,
		})
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeSeqBackfillStore) ListConversationSeqMax(
	_ context.Context,
	afterLowID, afterHighID uint64,
	limit int,
) ([]store.ConversationSeqMax, error) {
	counts := make(map[[2]uint64]*store.ConversationSeqMax)
	for _, row := range f.direct {
		if row.lowID == 0 || row.highID == 0 {
			continue
		}
		key := [2]uint64{row.lowID, row.highID}
		item, ok := counts[key]
		if !ok {
			item = &store.ConversationSeqMax{LowID: row.lowID, HighID: row.highID}
			counts[key] = item
		}
		item.Count++
		if row.seq > item.MaxSeq {
			item.MaxSeq = row.seq
		}
	}
	keys := make([][2]uint64, 0, len(counts))
	for key := range counts {
		if key[0] < afterLowID || (key[0] == afterLowID && key[1] <= afterHighID) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	result := make([]store.ConversationSeqMax, 0, limit)
	for _, key := range keys {
		result = append(result, *counts[key])
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeSeqBackfillStore) ListGroupSeqMax(
	_ context.Context,
	afterGroupID uint64,
	limit int,
) ([]store.GroupSeqMax, error) {
	counts := make(map[uint64]*store.GroupSeqMax)
	for _, row := range f.groups {
		item, ok := counts[row.groupID]
		if !ok {
			item = &store.GroupSeqMax{GroupID: row.groupID}
			counts[row.groupID] = item
		}
		item.Count++
		if row.seq > item.MaxSeq {
			item.MaxSeq = row.seq
		}
	}
	groupIDs := make([]uint64, 0, len(counts))
	for groupID := range counts {
		if groupID > afterGroupID {
			groupIDs = append(groupIDs, groupID)
		}
	}
	sort.Slice(groupIDs, func(i, j int) bool { return groupIDs[i] < groupIDs[j] })
	result := make([]store.GroupSeqMax, 0, limit)
	for _, groupID := range groupIDs {
		result = append(result, *counts[groupID])
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeSeqBackfillStore) ListConversationReadStates(
	_ context.Context,
	afterUserID, afterPeerID uint64,
	limit int,
) ([]store.ConversationReadState, error) {
	keys := make([][2]uint64, 0, len(f.convReads))
	for key := range f.convReads {
		if key[0] < afterUserID || (key[0] == afterUserID && key[1] <= afterPeerID) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	result := make([]store.ConversationReadState, 0, limit)
	for _, key := range keys {
		result = append(result, f.convReads[key])
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeSeqBackfillStore) ListGroupMemberReadStates(
	_ context.Context,
	afterGroupID, afterUserID uint64,
	limit int,
) ([]store.GroupMemberReadState, error) {
	keys := make([][2]uint64, 0, len(f.groupRead))
	for key := range f.groupRead {
		if key[0] < afterGroupID || (key[0] == afterGroupID && key[1] <= afterUserID) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	result := make([]store.GroupMemberReadState, 0, limit)
	for _, key := range keys {
		result = append(result, f.groupRead[key])
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeSeqBackfillStore) CountDirectMessagesByPair(
	context.Context,
) ([]store.DirectPairCount, error) {
	counts := make(map[[2]uint64]uint64)
	for _, row := range f.direct {
		key := [2]uint64{row.recipient, row.senderID}
		counts[key]++
	}
	result := make([]store.DirectPairCount, 0, len(counts))
	for key, count := range counts {
		result = append(result, store.DirectPairCount{
			RecipientID: key[0], SenderID: key[1], Count: count,
		})
	}
	return result, nil
}

func (f *fakeSeqBackfillStore) CountGroupMessagesByGroup(
	context.Context,
) (map[uint64]uint64, error) {
	counts := make(map[uint64]uint64)
	for _, row := range f.groups {
		counts[row.groupID]++
	}
	return counts, nil
}

func (f *fakeSeqBackfillStore) CountUnnumberedMessages(
	context.Context,
) (uint64, uint64, error) {
	var direct, group uint64
	for _, row := range f.direct {
		if row.seq == 0 {
			direct++
		}
	}
	for _, row := range f.groups {
		if row.seq == 0 {
			group++
		}
	}
	return direct, group, nil
}

func (f *fakeSeqBackfillStore) FillMessageConversationPairs(
	_ context.Context,
	limit int,
) (int, error) {
	filled := 0
	for _, row := range f.directSorted() {
		if row.lowID != 0 && row.highID != 0 {
			continue
		}
		if row.senderID < row.recipient {
			row.lowID, row.highID = row.senderID, row.recipient
		} else {
			row.lowID, row.highID = row.recipient, row.senderID
		}
		filled++
		if filled == limit {
			break
		}
	}
	return filled, nil
}

func (f *fakeSeqBackfillStore) maxDirectSeq(lowID, highID uint64) uint64 {
	var maxSeq uint64
	for _, row := range f.direct {
		if row.lowID == lowID && row.highID == highID && row.seq > maxSeq {
			maxSeq = row.seq
		}
	}
	return maxSeq
}

func (f *fakeSeqBackfillStore) AssignMessageSeqs(
	_ context.Context,
	limit int,
) (int, error) {
	next := make(map[[2]uint64]uint64)
	numbered := 0
	for _, row := range f.directSorted() {
		if row.seq != 0 || row.lowID == 0 {
			continue
		}
		key := [2]uint64{row.lowID, row.highID}
		seq, ok := next[key]
		if !ok {
			seq = f.maxDirectSeq(row.lowID, row.highID)
		}
		seq++
		next[key] = seq
		row.seq = seq
		numbered++
		if numbered == limit {
			break
		}
	}
	return numbered, nil
}

func (f *fakeSeqBackfillStore) maxGroupSeq(groupID uint64) uint64 {
	var maxSeq uint64
	for _, row := range f.groups {
		if row.groupID == groupID && row.seq > maxSeq {
			maxSeq = row.seq
		}
	}
	return maxSeq
}

func (f *fakeSeqBackfillStore) AssignGroupMessageSeqs(
	_ context.Context,
	limit int,
) (int, error) {
	next := make(map[uint64]uint64)
	numbered := 0
	for _, row := range f.groupSorted() {
		if row.seq != 0 {
			continue
		}
		seq, ok := next[row.groupID]
		if !ok {
			seq = f.maxGroupSeq(row.groupID)
		}
		seq++
		next[row.groupID] = seq
		row.seq = seq
		numbered++
		if numbered == limit {
			break
		}
	}
	return numbered, nil
}

func (f *fakeSeqBackfillStore) DirectSeqAtOrBefore(
	_ context.Context,
	lowID, highID, serverMessageID uint64,
) (uint64, error) {
	var seq uint64
	for _, row := range f.directSorted() {
		if row.lowID != lowID || row.highID != highID || row.id > serverMessageID {
			continue
		}
		seq = row.seq
	}
	return seq, nil
}

func (f *fakeSeqBackfillStore) GroupSeqAtOrBefore(
	_ context.Context,
	groupID, serverMessageID uint64,
) (uint64, error) {
	var seq uint64
	for _, row := range f.groupSorted() {
		if row.groupID != groupID || row.id > serverMessageID {
			continue
		}
		seq = row.seq
	}
	return seq, nil
}

func (f *fakeSeqBackfillStore) UpsertConversationRead(
	_ context.Context,
	userID, peerID, lastReadSeq uint64,
	_ time.Time,
) error {
	key := [2]uint64{userID, peerID}
	state := f.convReads[key]
	state.UserID, state.PeerID = userID, peerID
	if lastReadSeq > state.LastReadSeq {
		state.LastReadSeq = lastReadSeq
	}
	f.convReads[key] = state
	return nil
}

func (f *fakeSeqBackfillStore) UpsertGroupRead(
	_ context.Context,
	groupID, userID, lastReadSeq uint64,
) (bool, error) {
	key := [2]uint64{groupID, userID}
	state := f.groupRead[key]
	state.GroupID, state.UserID = groupID, userID
	if lastReadSeq > state.LastReadSeq {
		state.LastReadSeq = lastReadSeq
	}
	f.groupRead[key] = state
	return true, nil
}

func newSeqBackfillFixture(t *testing.T) (*redis.Client, *fakeSeqBackfillStore) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	dataStore := newFakeSeqBackfillStore()
	dataStore.direct[10] = &fakeDirectMessage{id: 10, senderID: 1, recipient: 2}
	dataStore.direct[20] = &fakeDirectMessage{id: 20, senderID: 2, recipient: 1}
	dataStore.direct[30] = &fakeDirectMessage{id: 30, senderID: 1, recipient: 2}
	dataStore.groups[40] = &fakeGroupMessage{id: 40, groupID: 9}
	dataStore.groups[50] = &fakeGroupMessage{id: 50, groupID: 9}
	dataStore.convReads[[2]uint64{1, 2}] = store.ConversationReadState{
		UserID: 1, PeerID: 2, LastReadMessageID: 30,
	}
	dataStore.convReads[[2]uint64{2, 1}] = store.ConversationReadState{
		UserID: 2, PeerID: 1, LastReadMessageID: 20,
	}
	dataStore.groupRead[[2]uint64{9, 2}] = store.GroupMemberReadState{
		GroupID: 9, UserID: 2, LastReadMessageID: 45,
	}
	return client, dataStore
}

func TestSeqBackfillRun(t *testing.T) {
	client, fakeStore := newSeqBackfillFixture(t)
	ctx := context.Background()

	setDirectSequence(t, client, 1, 2, 100)
	if err := client.HSet(ctx, messageKey(10), "id", 10).Err(); err != nil {
		t.Fatalf("seed entity: %v", err)
	}
	legacyIndex := legacyDirectConversationKey(
		defaultKeyspace.prefixOrDefault(), 2, 1,
	)
	if err := client.ZAdd(ctx, legacyIndex, redis.Z{Score: 10, Member: "10"}).Err(); err != nil {
		t.Fatalf("seed legacy index: %v", err)
	}
	if err := client.Set(
		ctx, legacyGroupLastAcceptedKey(defaultKeyspace.prefixOrDefault(), 9), "40", 0,
	).Err(); err != nil {
		t.Fatalf("seed legacy group marker: %v", err)
	}
	if err := client.Set(ctx, directReadStreamKey(), "old", 0).Err(); err != nil {
		t.Fatalf("seed legacy read stream: %v", err)
	}
	if err := client.HSet(ctx, directReadKey(1), "2", "20").Err(); err != nil {
		t.Fatalf("seed direct watermark: %v", err)
	}
	if err := client.HSet(ctx, directReadKey(2), "1", "15").Err(); err != nil {
		t.Fatalf("seed reverse watermark: %v", err)
	}
	if err := client.ZAdd(ctx, defaultKeyspace.groupReadKey(9), redis.Z{
		Score: 40, Member: "2",
	}).Err(); err != nil {
		t.Fatalf("seed group watermark: %v", err)
	}

	backfill := NewSeqBackfill(client, defaultKeyspace, fakeStore, 2)
	result, err := backfill.Run(ctx)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if fakeStore.direct[10].seq != 1 || fakeStore.direct[20].seq != 2 || fakeStore.direct[30].seq != 3 {
		t.Fatalf("direct seqs = %d,%d,%d", fakeStore.direct[10].seq, fakeStore.direct[20].seq, fakeStore.direct[30].seq)
	}
	if fakeStore.groups[40].seq != 1 || fakeStore.groups[50].seq != 2 {
		t.Fatalf("group seqs = %d,%d", fakeStore.groups[40].seq, fakeStore.groups[50].seq)
	}
	if result.MessagesNumbered != 3 || result.GroupMessagesNumbered != 2 {
		t.Fatalf("numbered = %#v", result)
	}

	for messageID, seq := range map[uint64]float64{10: 1, 20: 2, 30: 3} {
		if got := client.ZScore(ctx, directPairKey(1, 2), strconvUint(messageID)).Val(); got != seq {
			t.Fatalf("dm:pair[%d] score = %v, want %v", messageID, got, seq)
		}
	}
	if got := client.ZCard(ctx, directPairKey(1, 2)).Val(); got != 3 {
		t.Fatalf("dm:pair size = %d, want 3", got)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupInboxKey(9), "50").Val(); got != 2 {
		t.Fatalf("inbox score = %v, want 2", got)
	}
	if got := client.Get(ctx, directSequenceKey(1, 2)).Val(); got != "100" {
		t.Fatalf("dm:seq lowered to %q, want 100", got)
	}
	if got := client.Get(ctx, defaultKeyspace.groupSequenceKey(9)).Val(); got != "2" {
		t.Fatalf("group:seq = %q, want 2", got)
	}

	if got := client.HGet(ctx, messageKey(10), "seq").Val(); got != "1" {
		t.Fatalf("existing entity seq = %q, want 1", got)
	}
	if got := client.Exists(ctx, messageKey(20)).Val(); got != 0 {
		t.Fatalf("backfill created a missing entity: %d", got)
	}

	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "3" {
		t.Fatalf("sender watermark = %q, want 3 (includes own message 30)", got)
	}
	if got := client.HGet(ctx, directReadKey(2), "1").Val(); got != "2" {
		t.Fatalf("peer watermark = %q, want 2", got)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "2").Val(); got != 1 {
		t.Fatalf("group watermark = %v, want 1", got)
	}
	if got := fakeStore.convReads[[2]uint64{1, 2}].LastReadSeq; got != 3 {
		t.Fatalf("persisted sender read seq = %d, want 3", got)
	}
	if got := fakeStore.groupRead[[2]uint64{9, 2}].LastReadSeq; got != 1 {
		t.Fatalf("persisted group read seq = %d, want 1", got)
	}

	for _, key := range []string{
		legacyIndex,
		legacyGroupLastAcceptedKey(defaultKeyspace.prefixOrDefault(), 9),
		directReadStreamKey(),
		defaultKeyspace.groupReadStreamKey(),
	} {
		if got := client.Exists(ctx, key).Val(); got != 0 {
			t.Fatalf("legacy key %q still exists", key)
		}
	}
	if got := client.Get(ctx, sequenceReadyKey()).Val(); got != "1" {
		t.Fatalf("seq:ready = %q, want 1", got)
	}
	if result.ConversationsVerified != 1 || result.GroupsVerified != 1 {
		t.Fatalf("verified = %#v", result)
	}
	if result.ReadPositionsConverted != 3 || result.ReadPositionsVerified != 3 {
		t.Fatalf("watermarks = %#v", result)
	}
	if result.EntitiesPatched != 1 {
		t.Fatalf("entities patched = %d, want 1", result.EntitiesPatched)
	}
}

func TestSeqBackfillRerunIsIdempotent(t *testing.T) {
	client, fakeStore := newSeqBackfillFixture(t)
	ctx := context.Background()
	setDirectSequence(t, client, 1, 2, 100)
	backfill := NewSeqBackfill(client, defaultKeyspace, fakeStore, 2)

	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	second, err := backfill.Run(ctx)
	if err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	if second.MessagesNumbered != 0 || second.GroupMessagesNumbered != 0 {
		t.Fatalf("rerun numbered messages again: %#v", second)
	}
	if got := client.Get(ctx, directSequenceKey(1, 2)).Val(); got != "100" {
		t.Fatalf("rerun lowered dm:seq to %q", got)
	}
	if got := client.ZCard(ctx, directPairKey(1, 2)).Val(); got != 3 {
		t.Fatalf("rerun pair size = %d, want 3", got)
	}
	if got := client.Get(ctx, sequenceReadyKey()).Val(); got != "1" {
		t.Fatalf("seq:ready = %q, want 1", got)
	}
}

func TestSeqBackfillAbortsOnAcceptedButUnpersistedMessages(t *testing.T) {
	client, fakeStore := newSeqBackfillFixture(t)
	ctx := context.Background()
	legacyIndex := legacyDirectConversationKey(
		defaultKeyspace.prefixOrDefault(), 2, 1,
	)
	if err := client.ZAdd(ctx, legacyIndex,
		redis.Z{Score: 10, Member: "10"},
		redis.Z{Score: 20, Member: "20"},
		redis.Z{Score: 30, Member: "30"},
	).Err(); err != nil {
		t.Fatalf("seed legacy index: %v", err)
	}

	backfill := NewSeqBackfill(client, defaultKeyspace, fakeStore, 2)
	if _, err := backfill.Run(ctx); err == nil {
		t.Fatal("Run() error = nil, want precheck failure")
	}
	if got := client.Exists(ctx, sequenceReadyKey()).Val(); got != 0 {
		t.Fatalf("seq:ready written after a failed precheck")
	}
	if fakeStore.direct[10].seq != 0 {
		t.Fatalf("messages numbered despite precheck failure")
	}
}

func TestSeqBackfillEmptyDatabaseWritesReady(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	backfill := NewSeqBackfill(
		client, defaultKeyspace, newFakeSeqBackfillStore(), 10,
	)
	if _, err := backfill.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := client.Get(context.Background(), sequenceReadyKey()).Val(); got != "1" {
		t.Fatalf("seq:ready = %q, want 1", got)
	}
}

func TestSeqBackfillRaisesLowCounter(t *testing.T) {
	client, fakeStore := newSeqBackfillFixture(t)
	ctx := context.Background()
	setDirectSequence(t, client, 1, 2, 1)

	backfill := NewSeqBackfill(client, defaultKeyspace, fakeStore, 10)
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := client.Get(ctx, directSequenceKey(1, 2)).Val(); got != "3" {
		t.Fatalf("dm:seq = %q, want 3", got)
	}
}

func TestSeqBackfillPrecheckCatchesRedisOnlyIndexes(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	backfill := NewSeqBackfill(client, defaultKeyspace, newFakeSeqBackfillStore(), 10)
	legacyIndex := legacyDirectConversationKey(
		defaultKeyspace.prefixOrDefault(), 2, 1,
	)
	if err := client.ZAdd(ctx, legacyIndex, redis.Z{Score: 1, Member: "1"}).Err(); err != nil {
		t.Fatalf("seed legacy index: %v", err)
	}
	if _, err := backfill.Run(ctx); err == nil {
		t.Fatal("Run() error = nil, want precheck failure for a Redis-only conversation")
	}
	if got := client.Exists(ctx, sequenceReadyKey()).Val(); got != 0 {
		t.Fatal("seq:ready written after a failed precheck")
	}

	if err := client.Del(ctx, legacyIndex).Err(); err != nil {
		t.Fatalf("delete legacy index: %v", err)
	}
	if err := client.ZAdd(ctx, defaultKeyspace.groupInboxKey(9), redis.Z{
		Score: 1, Member: "40",
	}).Err(); err != nil {
		t.Fatalf("seed inbox: %v", err)
	}
	if _, err := backfill.Run(ctx); err == nil {
		t.Fatal("Run() error = nil, want precheck failure for a Redis-only group")
	}
}

func newEmptySeqBackfill(
	t *testing.T,
) (*redis.Client, *miniredis.Miniredis, *fakeSeqBackfillStore) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client, server, newFakeSeqBackfillStore()
}

func seedBackfillMessages(
	dataStore *fakeSeqBackfillStore,
	senderID, recipientID, firstMessageID uint64,
	count int,
) {
	low, high := sortedPair(senderID, recipientID)
	for index := 0; index < count; index++ {
		messageID := firstMessageID + uint64(index)
		dataStore.direct[messageID] = &fakeDirectMessage{
			id:        messageID,
			senderID:  senderID,
			recipient: recipientID,
			lowID:     low,
			highID:    high,
			seq:       uint64(index + 1),
		}
	}
}

func seedBackfillGroupMessages(
	dataStore *fakeSeqBackfillStore,
	groupID, firstMessageID uint64,
	count int,
) {
	for index := 0; index < count; index++ {
		messageID := firstMessageID + uint64(index)
		dataStore.groups[messageID] = &fakeGroupMessage{
			id:      messageID,
			groupID: groupID,
			seq:     uint64(index + 1),
		}
	}
}

func TestSeqBackfillRerunKeepsSeqWatermark(t *testing.T) {
	client, _, dataStore := newEmptySeqBackfill(t)
	ctx := context.Background()
	seedBackfillMessages(dataStore, 1, 2, 1001, 10)
	dataStore.convReads[[2]uint64{1, 2}] = store.ConversationReadState{
		UserID: 1, PeerID: 2, LastReadMessageID: 900, LastReadSeq: 10,
	}
	if err := client.HSet(ctx, directReadKey(1), "2", "10").Err(); err != nil {
		t.Fatalf("seed watermark: %v", err)
	}

	backfill := NewSeqBackfill(client, defaultKeyspace, dataStore, 10)
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "10" {
		t.Fatalf("watermark = %q, want 10", got)
	}
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "10" {
		t.Fatalf("rerun lowered watermark to %q, want 10", got)
	}
}

func TestSeqBackfillRestoresSeqWatermarkAfterRedisLoss(t *testing.T) {
	client, server, dataStore := newEmptySeqBackfill(t)
	ctx := context.Background()
	seedBackfillMessages(dataStore, 1, 2, 1001, 10)
	dataStore.convReads[[2]uint64{1, 2}] = store.ConversationReadState{
		UserID: 1, PeerID: 2, LastReadMessageID: 1007,
	}
	if err := client.HSet(ctx, directReadKey(1), "2", "1007").Err(); err != nil {
		t.Fatalf("seed watermark: %v", err)
	}

	backfill := NewSeqBackfill(client, defaultKeyspace, dataStore, 10)
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "7" {
		t.Fatalf("converted watermark = %q, want 7", got)
	}

	server.FlushAll()
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("recovery Run() error = %v", err)
	}
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "7" {
		t.Fatalf("recovered watermark = %q, want 7", got)
	}
}

func TestSeqBackfillKeepsDurableSeqAboveLegacyGlobalID(t *testing.T) {
	client, _, dataStore := newEmptySeqBackfill(t)
	ctx := context.Background()
	// Global 900 maps to seq 120; the other seqs use lower and higher IDs.
	low, high := sortedPair(1, 2)
	for seq := uint64(1); seq <= 500; seq++ {
		messageID := 1000 + seq
		if seq <= 119 {
			messageID = 100 + seq
		}
		if seq == 120 {
			messageID = 900
		}
		dataStore.direct[messageID] = &fakeDirectMessage{
			id: messageID, senderID: 1, recipient: 2, lowID: low, highID: high, seq: seq,
		}
	}
	dataStore.convReads[[2]uint64{1, 2}] = store.ConversationReadState{
		UserID: 1, PeerID: 2, LastReadMessageID: 900, LastReadSeq: 500,
	}
	if err := client.HSet(ctx, directReadKey(1), "2", "500").Err(); err != nil {
		t.Fatalf("seed watermark: %v", err)
	}

	backfill := NewSeqBackfill(client, defaultKeyspace, dataStore, 100)
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "500" {
		t.Fatalf("watermark = %q, want 500 (durable seq must win)", got)
	}
}

func TestSeqBackfillConvertsRedisOnlyWatermarks(t *testing.T) {
	client, _, dataStore := newEmptySeqBackfill(t)
	ctx := context.Background()
	seedBackfillMessages(dataStore, 1, 2, 1001, 10)
	seedBackfillGroupMessages(dataStore, 9, 2001, 5)
	if err := client.HSet(ctx, directReadKey(1), "2", "1007").Err(); err != nil {
		t.Fatalf("seed direct watermark: %v", err)
	}
	if err := client.ZAdd(ctx, defaultKeyspace.groupReadKey(9), redis.Z{
		Score: 2003, Member: "1",
	}).Err(); err != nil {
		t.Fatalf("seed group watermark: %v", err)
	}

	backfill := NewSeqBackfill(client, defaultKeyspace, dataStore, 10)
	result, err := backfill.Run(ctx)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "7" {
		t.Fatalf("direct watermark = %q, want 7", got)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "1").Val(); got != 3 {
		t.Fatalf("group watermark = %v, want 3", got)
	}
	if dataStore.convReads[[2]uint64{1, 2}].LastReadSeq != 7 {
		t.Fatalf("persisted direct watermark = %#v", dataStore.convReads[[2]uint64{1, 2}])
	}
	if dataStore.groupRead[[2]uint64{9, 1}].LastReadSeq != 3 {
		t.Fatalf("persisted group watermark = %#v", dataStore.groupRead[[2]uint64{9, 1}])
	}
	if result.ReadPositionsConverted != 2 {
		t.Fatalf("converted = %d, want 2", result.ReadPositionsConverted)
	}
}

func TestSeqBackfillGroupRerunKeepsSeqWatermark(t *testing.T) {
	client, _, dataStore := newEmptySeqBackfill(t)
	ctx := context.Background()
	seedBackfillGroupMessages(dataStore, 9, 2001, 5)
	dataStore.groupRead[[2]uint64{9, 1}] = store.GroupMemberReadState{
		GroupID: 9, UserID: 1, LastReadMessageID: 2003, LastReadSeq: 5,
	}
	if err := client.ZAdd(ctx, defaultKeyspace.groupReadKey(9), redis.Z{
		Score: 5, Member: "1",
	}).Err(); err != nil {
		t.Fatalf("seed watermark: %v", err)
	}

	backfill := NewSeqBackfill(client, defaultKeyspace, dataStore, 10)
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "1").Val(); got != 5 {
		t.Fatalf("watermark = %v, want 5", got)
	}
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "1").Val(); got != 5 {
		t.Fatalf("rerun lowered watermark to %v, want 5", got)
	}
}

func TestSeqBackfillConvertsSmallRedisOnlyGlobalID(t *testing.T) {
	client, _, dataStore := newEmptySeqBackfill(t)
	ctx := context.Background()
	// Global IDs 7..16 map to seq 1..10, so the legacy watermark 9 is smaller
	// than the conversation counter and must still be converted to seq 3.
	seedBackfillMessages(dataStore, 1, 2, 7, 10)
	if err := client.HSet(ctx, directReadKey(1), "2", "9").Err(); err != nil {
		t.Fatalf("seed direct watermark: %v", err)
	}

	backfill := NewSeqBackfill(client, defaultKeyspace, dataStore, 10)
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "3" {
		t.Fatalf("direct watermark = %q, want 3", got)
	}
	if got := dataStore.convReads[[2]uint64{1, 2}].LastReadSeq; got != 3 {
		t.Fatalf("persisted direct watermark = %d, want 3", got)
	}
}

func TestSeqBackfillKeepsRedisOnlySeqWhenConversionStarted(t *testing.T) {
	client, _, dataStore := newEmptySeqBackfill(t)
	ctx := context.Background()
	seedBackfillMessages(dataStore, 1, 2, 7, 10)
	if err := client.HSet(ctx, directReadKey(1), "2", "3").Err(); err != nil {
		t.Fatalf("seed direct watermark: %v", err)
	}
	if err := client.Set(ctx, defaultKeyspace.sequenceConvertingKey(), "1", 0).Err(); err != nil {
		t.Fatalf("seed conversion marker: %v", err)
	}

	backfill := NewSeqBackfill(client, defaultKeyspace, dataStore, 10)
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "3" {
		t.Fatalf("direct watermark = %q, want 3", got)
	}
	if err := client.Exists(ctx, defaultKeyspace.sequenceConvertingKey()).Err(); err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if client.Exists(ctx, defaultKeyspace.sequenceConvertingKey()).Val() != 0 {
		t.Fatalf("conversion marker still present after success")
	}
}

func TestSeqBackfillDoesNotSkipRedisOnlyPairHandledByGroup(t *testing.T) {
	client, _, dataStore := newEmptySeqBackfill(t)
	ctx := context.Background()
	seedBackfillMessages(dataStore, 5, 7, 1, 10)
	seedBackfillGroupMessages(dataStore, 5, 2001, 5)
	// The group row (5, 7) is handled by the group loop; the DM watermark for
	// the same numeric pair must not be skipped.
	dataStore.groupRead[[2]uint64{5, 7}] = store.GroupMemberReadState{
		GroupID: 5, UserID: 7, LastReadMessageID: 2003, LastReadSeq: 5,
	}
	if err := client.HSet(ctx, directReadKey(5), "7", "3").Err(); err != nil {
		t.Fatalf("seed direct watermark: %v", err)
	}

	backfill := NewSeqBackfill(client, defaultKeyspace, dataStore, 10)
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := client.HGet(ctx, directReadKey(5), "7").Val(); got != "3" {
		t.Fatalf("direct watermark = %q, want 3", got)
	}
}
