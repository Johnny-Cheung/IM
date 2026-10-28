package messaging

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

type fakeSeqCalibrationStore struct {
	conversations []store.ConversationSeqMax
	groups        []store.GroupSeqMax
	err           error
}

func (f *fakeSeqCalibrationStore) ListConversationSeqMax(
	_ context.Context,
	afterLowID, afterHighID uint64,
	limit int,
) ([]store.ConversationSeqMax, error) {
	if f.err != nil {
		return nil, f.err
	}
	result := make([]store.ConversationSeqMax, 0, limit)
	for _, row := range f.conversations {
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

func (f *fakeSeqCalibrationStore) ListGroupSeqMax(
	_ context.Context,
	afterGroupID uint64,
	limit int,
) ([]store.GroupSeqMax, error) {
	if f.err != nil {
		return nil, f.err
	}
	result := make([]store.GroupSeqMax, 0, limit)
	for _, row := range f.groups {
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

func TestSeqCalibratorRaisesOnlyAndBatches(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	setDirectSequence(t, client, 1, 2, 10)
	setGroupSequence(t, client, 9, 9)
	fakeStore := &fakeSeqCalibrationStore{
		conversations: []store.ConversationSeqMax{
			{LowID: 1, HighID: 2, MaxSeq: 5},
			{LowID: 3, HighID: 4, MaxSeq: 7},
		},
		groups: []store.GroupSeqMax{
			{GroupID: 9, MaxSeq: 6},
			{GroupID: 10, MaxSeq: 3},
		},
	}
	calibrator := NewSeqCalibrator(client, defaultKeyspace, fakeStore, 1)
	processed, err := calibrator.Calibrate(ctx)
	if err != nil {
		t.Fatalf("Calibrate() error = %v", err)
	}
	if processed != 4 {
		t.Fatalf("processed = %d, want 4 conversations", processed)
	}
	if got := client.Get(ctx, directSequenceKey(1, 2)).Val(); got != "10" {
		t.Fatalf("existing counter lowered to %q, want 10", got)
	}
	if got := client.Get(ctx, directSequenceKey(3, 4)).Val(); got != "7" {
		t.Fatalf("missing counter = %q, want 7", got)
	}
	if got := client.Get(ctx, defaultKeyspace.groupSequenceKey(9)).Val(); got != "9" {
		t.Fatalf("existing group counter lowered to %q, want 9", got)
	}
	if got := client.Get(ctx, defaultKeyspace.groupSequenceKey(10)).Val(); got != "3" {
		t.Fatalf("missing group counter = %q, want 3", got)
	}
}

func TestSeqCalibratorReturnsStoreError(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	calibrator := NewSeqCalibrator(
		client, defaultKeyspace, &fakeSeqCalibrationStore{err: errors.New("mysql unavailable")}, 10,
	)
	if _, err := calibrator.Calibrate(context.Background()); err == nil {
		t.Fatal("Calibrate() error = nil, want store error")
	}
}
