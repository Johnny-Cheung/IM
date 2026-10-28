package messaging

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

type fakeGroupReadBackfillStore struct {
	messages      []store.GroupMessageIndex
	members       []store.GroupMemberRead
	own           []store.GroupLastOwnMessage
	projections   []store.GroupMembershipProjection
	counts        map[uint64]uint64
	written       []store.GroupMemberRead
	messageErr    error
	memberErr     error
	ownErr        error
	projectionErr error
	countErr      error
	writeErr      error
}

func (f *fakeGroupReadBackfillStore) ListGroupMessageIndex(
	_ context.Context,
	afterMessageID uint64,
	limit int,
) ([]store.GroupMessageIndex, error) {
	if f.messageErr != nil {
		return nil, f.messageErr
	}
	sort.Slice(f.messages, func(i, j int) bool {
		return f.messages[i].MessageID < f.messages[j].MessageID
	})
	result := make([]store.GroupMessageIndex, 0, limit)
	for _, item := range f.messages {
		if item.MessageID <= afterMessageID {
			continue
		}
		result = append(result, item)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeGroupReadBackfillStore) ListGroupMemberReads(
	_ context.Context,
	afterGroupID, afterUserID uint64,
	limit int,
) ([]store.GroupMemberRead, error) {
	if f.memberErr != nil {
		return nil, f.memberErr
	}
	sort.Slice(f.members, func(i, j int) bool {
		if f.members[i].GroupID != f.members[j].GroupID {
			return f.members[i].GroupID < f.members[j].GroupID
		}
		return f.members[i].UserID < f.members[j].UserID
	})
	result := make([]store.GroupMemberRead, 0, limit)
	for _, item := range f.members {
		if item.GroupID < afterGroupID {
			continue
		}
		if item.GroupID == afterGroupID && item.UserID <= afterUserID {
			continue
		}
		result = append(result, item)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeGroupReadBackfillStore) ListGroupLastOwnMessages(
	context.Context,
) ([]store.GroupLastOwnMessage, error) {
	if f.ownErr != nil {
		return nil, f.ownErr
	}
	return f.own, nil
}

func (f *fakeGroupReadBackfillStore) CountGroupMessagesByGroup(
	context.Context,
) (map[uint64]uint64, error) {
	if f.countErr != nil {
		return nil, f.countErr
	}
	if f.counts != nil {
		return f.counts, nil
	}
	counts := make(map[uint64]uint64)
	for _, item := range f.messages {
		counts[item.GroupID]++
	}
	return counts, nil
}

func (f *fakeGroupReadBackfillStore) ListGroupMembershipProjections(
	context.Context,
) ([]store.GroupMembershipProjection, error) {
	if f.projectionErr != nil {
		return nil, f.projectionErr
	}
	return f.projections, nil
}

func (f *fakeGroupReadBackfillStore) UpsertGroupRead(
	_ context.Context,
	groupID, userID, lastReadMessageID uint64,
) (bool, error) {
	if f.writeErr != nil {
		return false, f.writeErr
	}
	for index := range f.members {
		member := &f.members[index]
		if member.GroupID != groupID || member.UserID != userID {
			continue
		}
		if lastReadMessageID > member.LastReadMessageID {
			member.LastReadMessageID = lastReadMessageID
		}
		f.written = append(f.written, store.GroupMemberRead{
			GroupID:           groupID,
			UserID:            userID,
			LastReadMessageID: lastReadMessageID,
		})
		return true, nil
	}
	return false, nil
}

func newGroupReadBackfillTest(
	t *testing.T,
	dataStore GroupReadBackfillStore,
	batchSize int,
) (*GroupReadBackfill, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewGroupReadBackfill(client, defaultKeyspace, dataStore, batchSize), client
}

func TestGroupReadBackfillIndexesVerifiesAndMarksReady(t *testing.T) {
	dataStore := &fakeGroupReadBackfillStore{
		projections: []store.GroupMembershipProjection{
			{GroupID: 1, MemberIDs: []uint64{1, 2}},
			{GroupID: 2, MemberIDs: []uint64{3}},
		},
		messages: []store.GroupMessageIndex{
			{GroupID: 1, MessageID: 10},
			{GroupID: 1, MessageID: 20},
			{GroupID: 2, MessageID: 30},
		},
		members: []store.GroupMemberRead{
			{GroupID: 1, UserID: 1, LastReadMessageID: 20},
			{GroupID: 1, UserID: 2, LastReadMessageID: 0},
			{GroupID: 2, UserID: 3, LastReadMessageID: 0},
		},
		own: []store.GroupLastOwnMessage{
			{GroupID: 1, SenderID: 1, MessageID: 20},
			{GroupID: 2, SenderID: 3, MessageID: 30},
		},
	}
	backfill, client := newGroupReadBackfillTest(t, dataStore, 2)
	ctx := context.Background()

	result, err := backfill.Run(ctx)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if result.MessagesIndexed != 3 || result.GroupsRebuilt != 2 ||
		result.ReadPositionsIndexed != 3 || result.ReadPositionsWrittenBack != 1 ||
		result.GroupsVerified != 2 || result.ReadPositionsVerified != 3 {
		t.Fatalf("result = %#v", result)
	}

	for _, testCase := range []struct {
		group   uint64
		message uint64
		score   float64
	}{
		{group: 1, message: 10, score: 10},
		{group: 1, message: 20, score: 20},
		{group: 2, message: 30, score: 30},
	} {
		got := client.ZScore(
			ctx,
			defaultKeyspace.groupInboxKey(testCase.group),
			strconv.FormatUint(testCase.message, 10),
		).Val()
		if got != testCase.score {
			t.Fatalf("inbox group %d message %d = %v", testCase.group, testCase.message, got)
		}
	}
	for _, testCase := range []struct {
		group  uint64
		member uint64
		score  float64
	}{
		{group: 1, member: 1, score: 20},
		{group: 1, member: 2, score: 0},
		{group: 2, member: 3, score: 30},
	} {
		got := client.ZScore(
			ctx,
			defaultKeyspace.groupReadKey(testCase.group),
			strconv.FormatUint(testCase.member, 10),
		).Val()
		if got != testCase.score {
			t.Fatalf("read group %d member %d = %v", testCase.group, testCase.member, got)
		}
	}
	if got := client.SIsMember(ctx, defaultKeyspace.groupMembersKey(1), 2).Val(); !got {
		t.Fatalf("group 1 member 2 missing from members set")
	}
	if len(dataStore.written) != 1 || dataStore.written[0].GroupID != 2 ||
		dataStore.written[0].UserID != 3 || dataStore.written[0].LastReadMessageID != 30 {
		t.Fatalf("written back = %#v", dataStore.written)
	}
	if got := client.Get(ctx, defaultKeyspace.groupReadReadyKey()).Val(); got != "1" {
		t.Fatalf("ready marker = %q, want 1", got)
	}
}

func TestGroupReadBackfillBumpsOwnMessagesAndIsIdempotent(t *testing.T) {
	dataStore := &fakeGroupReadBackfillStore{
		projections: []store.GroupMembershipProjection{
			{GroupID: 1, MemberIDs: []uint64{1, 2}},
		},
		messages: []store.GroupMessageIndex{
			{GroupID: 1, MessageID: 10},
			{GroupID: 1, MessageID: 30},
		},
		members: []store.GroupMemberRead{
			{GroupID: 1, UserID: 1, LastReadMessageID: 0},
			{GroupID: 1, UserID: 2, LastReadMessageID: 10},
		},
		own: []store.GroupLastOwnMessage{
			{GroupID: 1, SenderID: 1, MessageID: 30},
		},
	}
	backfill, client := newGroupReadBackfillTest(t, dataStore, 10)
	ctx := context.Background()

	result, err := backfill.Run(ctx)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if result.ReadPositionsWrittenBack != 1 {
		t.Fatalf("written back = %d, want 1", result.ReadPositionsWrittenBack)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(1), "1").Val(); got != 30 {
		t.Fatalf("own message bump = %v, want 30", got)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(1), "2").Val(); got != 10 {
		t.Fatalf("member 2 position = %v, want 10", got)
	}
	if len(dataStore.written) != 1 || dataStore.written[0].UserID != 1 ||
		dataStore.written[0].LastReadMessageID != 30 {
		t.Fatalf("written back = %#v", dataStore.written)
	}

	if err := client.ZAdd(ctx, defaultKeyspace.groupReadKey(1), redis.Z{
		Score: 40, Member: "1",
	}).Err(); err != nil {
		t.Fatalf("raise redis position: %v", err)
	}
	dataStore.written = nil
	if _, err := backfill.Run(ctx); err != nil {
		t.Fatalf("rerun: %v", err)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupReadKey(1), "1").Val(); got != 40 {
		t.Fatalf("rerun lowered position to %v, want 40", got)
	}
	if len(dataStore.written) != 0 {
		t.Fatalf("rerun wrote back = %#v, want none", dataStore.written)
	}
}

func TestGroupReadBackfillVerificationFailureLeavesMarkerUnset(t *testing.T) {
	t.Run("inbox missing messages", func(t *testing.T) {
		dataStore := &fakeGroupReadBackfillStore{
			projections: []store.GroupMembershipProjection{
				{GroupID: 1, MemberIDs: []uint64{1}},
			},
			messages: []store.GroupMessageIndex{{GroupID: 1, MessageID: 10}},
			members:  []store.GroupMemberRead{{GroupID: 1, UserID: 1}},
			counts:   map[uint64]uint64{1: 2},
		}
		backfill, client := newGroupReadBackfillTest(t, dataStore, 10)
		ctx := context.Background()
		if _, err := backfill.Run(ctx); err == nil {
			t.Fatalf("backfill error = nil, want inbox verification failure")
		}
		if got := client.Exists(ctx, defaultKeyspace.groupReadReadyKey()).Val(); got != 0 {
			t.Fatalf("ready marker exists = %d, want 0", got)
		}
	})

	t.Run("read set does not match members", func(t *testing.T) {
		dataStore := &fakeGroupReadBackfillStore{
			projections: []store.GroupMembershipProjection{
				{GroupID: 1, MemberIDs: []uint64{1, 2}},
			},
			messages: []store.GroupMessageIndex{{GroupID: 1, MessageID: 10}},
			members:  []store.GroupMemberRead{{GroupID: 1, UserID: 1}},
		}
		backfill, client := newGroupReadBackfillTest(t, dataStore, 10)
		ctx := context.Background()
		if _, err := backfill.Run(ctx); err == nil {
			t.Fatalf("backfill error = nil, want size verification failure")
		}
		if got := client.Exists(ctx, defaultKeyspace.groupReadReadyKey()).Val(); got != 0 {
			t.Fatalf("ready marker exists = %d, want 0", got)
		}
	})
}

func TestGroupReadBackfillPropagatesStoreErrors(t *testing.T) {
	for name, configure := range map[string]func(*fakeGroupReadBackfillStore){
		"messages": func(fake *fakeGroupReadBackfillStore) {
			fake.messageErr = errors.New("messages failed")
		},
		"projections": func(fake *fakeGroupReadBackfillStore) {
			fake.projectionErr = errors.New("projections failed")
		},
		"members": func(fake *fakeGroupReadBackfillStore) {
			fake.memberErr = errors.New("members failed")
		},
		"own": func(fake *fakeGroupReadBackfillStore) {
			fake.ownErr = errors.New("own failed")
		},
		"counts": func(fake *fakeGroupReadBackfillStore) {
			fake.countErr = errors.New("counts failed")
		},
		"write": func(fake *fakeGroupReadBackfillStore) {
			fake.writeErr = errors.New("write failed")
			fake.members = []store.GroupMemberRead{{GroupID: 1, UserID: 1}}
			fake.own = []store.GroupLastOwnMessage{{GroupID: 1, SenderID: 1, MessageID: 10}}
			fake.messages = []store.GroupMessageIndex{{GroupID: 1, MessageID: 10}}
			fake.projections = []store.GroupMembershipProjection{
				{GroupID: 1, MemberIDs: []uint64{1}},
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			dataStore := &fakeGroupReadBackfillStore{}
			configure(dataStore)
			backfill, _ := newGroupReadBackfillTest(t, dataStore, 10)
			if _, err := backfill.Run(context.Background()); err == nil {
				t.Fatalf("backfill error = nil, want store error")
			}
		})
	}
}

func TestGroupReadBackfillRequiresDependencies(t *testing.T) {
	backfill := NewGroupReadBackfill(nil, defaultKeyspace, nil, 10)
	if _, err := backfill.Run(context.Background()); err == nil {
		t.Fatalf("backfill error = nil, want service unavailable")
	}
}
