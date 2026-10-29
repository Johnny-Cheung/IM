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

// TestSeqCalibratorRaisesToRedisIndexFloor covers a surviving accepted-but-
// unpersisted tail: MySQL knows only seq 3, the Redis index already has 5, so
// the counter must reach 5. An existing higher counter is never lowered.
func TestSeqCalibratorRaisesToRedisIndexFloor(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	setDirectSequence(t, client, 1, 2, 3)
	for seq := 1; seq <= 5; seq++ {
		if err := client.ZAdd(ctx, directPairKey(1, 2), redis.Z{
			Score:  float64(seq),
			Member: strconvUint(uint64(100 + seq)),
		}).Err(); err != nil {
			t.Fatalf("seed dm:pair score %d: %v", seq, err)
		}
	}
	setDirectSequence(t, client, 3, 4, 7)
	if err := client.ZAdd(ctx, directPairKey(3, 4), redis.Z{
		Score: 5, Member: "200",
	}).Err(); err != nil {
		t.Fatalf("seed dm:pair(3,4): %v", err)
	}

	setGroupSequence(t, client, 9, 2)
	for seq := 1; seq <= 6; seq++ {
		if err := client.ZAdd(ctx, defaultKeyspace.groupInboxKey(9), redis.Z{
			Score:  float64(seq),
			Member: strconvUint(uint64(300 + seq)),
		}).Err(); err != nil {
			t.Fatalf("seed inbox score %d: %v", seq, err)
		}
	}

	fakeStore := &fakeSeqCalibrationStore{
		conversations: []store.ConversationSeqMax{
			{LowID: 1, HighID: 2, MaxSeq: 3},
			{LowID: 3, HighID: 4, MaxSeq: 3},
		},
		groups: []store.GroupSeqMax{{GroupID: 9, MaxSeq: 2}},
	}
	calibrator := NewSeqCalibrator(client, defaultKeyspace, fakeStore, 10)
	if _, err := calibrator.Calibrate(ctx); err != nil {
		t.Fatalf("Calibrate() error = %v", err)
	}
	if got := client.Get(ctx, directSequenceKey(1, 2)).Val(); got != "5" {
		t.Fatalf("dm:seq = %q, want Redis index floor 5", got)
	}
	if got := client.Get(ctx, directSequenceKey(3, 4)).Val(); got != "7" {
		t.Fatalf("dm:seq = %q, want existing higher counter 7", got)
	}
	if got := client.Get(ctx, defaultKeyspace.groupSequenceKey(9)).Val(); got != "6" {
		t.Fatalf("group:seq = %q, want Redis index floor 6", got)
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
