package messaging

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestEnsureIndexEpochSeedsOnceAndKeepsValue(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	if err := client.Set(ctx, counterKey(), 50, 0).Err(); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	epoch, err := EnsureIndexEpoch(ctx, client, defaultKeyspace, 42)
	if err != nil {
		t.Fatalf("EnsureIndexEpoch(seed) error = %v", err)
	}
	if epoch != 50 {
		t.Fatalf("seed epoch = %d, want 50 (above accepted but unindexed ids)", epoch)
	}

	epoch, err = EnsureIndexEpoch(ctx, client, defaultKeyspace, 45)
	if err != nil {
		t.Fatalf("EnsureIndexEpoch(keep) error = %v", err)
	}
	if epoch != 50 {
		t.Fatalf("kept epoch = %d, want 50", epoch)
	}

	stored, err := client.Get(ctx, defaultKeyspace.indexEpochKey()).Result()
	if err != nil || stored != "50" {
		t.Fatalf("stored epoch = %q, err = %v", stored, err)
	}
}

func TestEnsureIndexEpochRaisesWhenCounterIsBehind(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	if err := client.Set(ctx, defaultKeyspace.indexEpochKey(), 42, 0).Err(); err != nil {
		t.Fatalf("seed epoch: %v", err)
	}
	if err := client.Set(ctx, counterKey(), 50, 0).Err(); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	epoch, err := EnsureIndexEpoch(ctx, client, defaultKeyspace, 60)
	if err != nil {
		t.Fatalf("EnsureIndexEpoch() error = %v", err)
	}
	if epoch != 60 {
		t.Fatalf("epoch = %d, want 60 after messages bypassed the index", epoch)
	}

	if err := client.Del(ctx, counterKey()).Err(); err != nil {
		t.Fatalf("delete counter: %v", err)
	}
	epoch, err = EnsureIndexEpoch(ctx, client, defaultKeyspace, 70)
	if err != nil {
		t.Fatalf("EnsureIndexEpoch(no counter) error = %v", err)
	}
	if epoch != 70 {
		t.Fatalf("epoch = %d, want 70 when the counter is missing", epoch)
	}
}
