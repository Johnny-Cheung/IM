package messaging

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

type fakeBackfillStore struct {
	messages   []store.MessageIndex
	reads      []store.ConversationRead
	pairs      []store.DirectPairCount
	messageErr error
	readErr    error
	pairErr    error
}

func (f *fakeBackfillStore) ListDirectMessageIndex(
	_ context.Context,
	afterID uint64,
	limit int,
) ([]store.MessageIndex, error) {
	if f.messageErr != nil {
		return nil, f.messageErr
	}
	sort.Slice(f.messages, func(i, j int) bool { return f.messages[i].ID < f.messages[j].ID })
	result := make([]store.MessageIndex, 0, limit)
	for _, item := range f.messages {
		if item.ID <= afterID {
			continue
		}
		result = append(result, item)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeBackfillStore) ListConversationReadsAfter(
	_ context.Context,
	afterUserID, afterPeerUserID uint64,
	limit int,
) ([]store.ConversationRead, error) {
	if f.readErr != nil {
		return nil, f.readErr
	}
	sort.Slice(f.reads, func(i, j int) bool {
		if f.reads[i].UserID != f.reads[j].UserID {
			return f.reads[i].UserID < f.reads[j].UserID
		}
		return f.reads[i].PeerUserID < f.reads[j].PeerUserID
	})
	result := make([]store.ConversationRead, 0, limit)
	for _, item := range f.reads {
		if item.UserID < afterUserID {
			continue
		}
		if item.UserID == afterUserID && item.PeerUserID <= afterPeerUserID {
			continue
		}
		result = append(result, item)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (f *fakeBackfillStore) CountDirectMessagesByPair(
	context.Context,
) ([]store.DirectPairCount, error) {
	if f.pairErr != nil {
		return nil, f.pairErr
	}
	if f.pairs != nil {
		return f.pairs, nil
	}
	counts := make(map[[2]uint64]uint64)
	for _, item := range f.messages {
		key := [2]uint64{item.RecipientID, item.SenderID}
		counts[key]++
	}
	result := make([]store.DirectPairCount, 0, len(counts))
	for key, count := range counts {
		result = append(result, store.DirectPairCount{
			RecipientID: key[0],
			SenderID:    key[1],
			Count:       count,
		})
	}
	return result, nil
}

func newBackfillTest(
	t *testing.T,
	store DirectReadBackfillStore,
	batchSize int,
) (*DirectReadBackfill, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewDirectReadBackfill(client, defaultKeyspace, store, batchSize), client
}

func TestDirectReadBackfillIndexesVerifiesAndMarksReady(t *testing.T) {
	dataStore := &fakeBackfillStore{
		messages: []store.MessageIndex{
			{ID: 10, SenderID: 1, RecipientID: 2},
			{ID: 20, SenderID: 2, RecipientID: 1},
			{ID: 30, SenderID: 1, RecipientID: 3},
		},
		reads: []store.ConversationRead{
			{UserID: 1, PeerUserID: 2, LastReadMessageID: 10},
			{UserID: 2, PeerUserID: 1, LastReadMessageID: 15},
		},
	}
	backfill, client := newBackfillTest(t, dataStore, 2)
	ctx := context.Background()

	result, err := backfill.Run(ctx)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if result.MessagesIndexed != 3 || result.ReadPositionsIndexed != 2 {
		t.Fatalf("result = %#v", result)
	}
	if result.PairsVerified != 3 || result.ReadPositionsVerified != 2 {
		t.Fatalf("verification counts = %#v", result)
	}

	for _, testCase := range []struct {
		key    string
		member string
		want   float64
	}{
		{directConversationKey(2, 1), "10", 10},
		{directConversationKey(1, 2), "20", 20},
		{directConversationKey(3, 1), "30", 30},
	} {
		if got := client.ZScore(ctx, testCase.key, testCase.member).Val(); got != testCase.want {
			t.Fatalf("%s member %s score = %v, want %v", testCase.key, testCase.member, got, testCase.want)
		}
	}
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "10" {
		t.Fatalf("read position 1->2 = %q, want 10", got)
	}
	if got := client.HGet(ctx, directReadKey(2), "1").Val(); got != "15" {
		t.Fatalf("read position 2->1 = %q, want 15", got)
	}
	if got := client.Get(ctx, directReadReadyKey()).Val(); got != "1" {
		t.Fatalf("ready marker = %q, want 1", got)
	}
}

func TestDirectReadBackfillFailsVerificationWithoutMarkingReady(t *testing.T) {
	dataStore := &fakeBackfillStore{
		messages: []store.MessageIndex{
			{ID: 10, SenderID: 1, RecipientID: 2},
		},
		pairs: []store.DirectPairCount{
			{RecipientID: 2, SenderID: 1, Count: 2},
		},
	}
	backfill, client := newBackfillTest(t, dataStore, 10)
	ctx := context.Background()

	if _, err := backfill.Run(ctx); err == nil {
		t.Fatalf("backfill error = nil, want verification failure")
	}
	if got := client.Exists(ctx, directReadReadyKey()).Val(); got != 0 {
		t.Fatalf("ready marker exists = %d, want 0", got)
	}
}

func TestDirectReadBackfillHandlesEmptyData(t *testing.T) {
	dataStore := &fakeBackfillStore{}
	backfill, client := newBackfillTest(t, dataStore, 10)
	ctx := context.Background()

	result, err := backfill.Run(ctx)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if result.MessagesIndexed != 0 || result.ReadPositionsIndexed != 0 {
		t.Fatalf("result = %#v", result)
	}
	if got := client.Get(ctx, directReadReadyKey()).Val(); got != "1" {
		t.Fatalf("ready marker = %q, want 1", got)
	}
}

func TestDirectReadBackfillPropagatesStoreErrors(t *testing.T) {
	for name, configure := range map[string]func(*fakeBackfillStore){
		"messages": func(store *fakeBackfillStore) {
			store.messageErr = errors.New("messages failed")
		},
		"reads": func(store *fakeBackfillStore) {
			store.readErr = errors.New("reads failed")
		},
		"pairs": func(store *fakeBackfillStore) {
			store.pairErr = errors.New("pairs failed")
		},
	} {
		t.Run(name, func(t *testing.T) {
			dataStore := &fakeBackfillStore{}
			configure(dataStore)
			backfill, _ := newBackfillTest(t, dataStore, 10)
			if _, err := backfill.Run(context.Background()); err == nil {
				t.Fatalf("backfill error = nil, want store error")
			}
		})
	}
}

func TestDirectReadBackfillRequiresDependencies(t *testing.T) {
	backfill := NewDirectReadBackfill(nil, defaultKeyspace, nil, 10)
	if _, err := backfill.Run(context.Background()); err == nil {
		t.Fatalf("backfill error = nil, want service unavailable")
	}
}
