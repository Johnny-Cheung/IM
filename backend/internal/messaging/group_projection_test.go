package messaging

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestGroupProjectionLifecycle(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	projection := NewGroupProjection(client, defaultKeyspace)
	ctx := context.Background()
	if err := projection.Replace(ctx, 9, []uint64{1, 2}, false); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	member, err := projection.IsMember(ctx, 9, 2)
	if err != nil || !member {
		t.Fatalf("IsMember() = %v, %v; want true", member, err)
	}
	if err := projection.BeginMutation(ctx, 9); err != nil {
		t.Fatalf("BeginMutation() error = %v", err)
	}
	if _, err := projection.IsMember(ctx, 9, 2); err != ErrMessageServiceUnavailable {
		t.Fatalf("IsMember() during mutation error = %v, want service unavailable", err)
	}
	if err := projection.EndMutation(ctx, 9); err != nil {
		t.Fatalf("EndMutation() error = %v", err)
	}
	if err := projection.Replace(ctx, 9, []uint64{1}, true); err != nil {
		t.Fatalf("Replace(dissolved) error = %v", err)
	}
	member, err = projection.IsMember(ctx, 9, 2)
	if err != nil || member {
		t.Fatalf("IsMember() after replace = %v, %v; want false", member, err)
	}
}

func TestGroupProjectionReplacePrunesStaleReadPositions(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	projection := NewGroupProjection(client, defaultKeyspace)
	ctx := context.Background()
	readKey := defaultKeyspace.groupReadKey(9)
	if err := client.ZAdd(ctx, readKey,
		redis.Z{Score: 100, Member: "1"},
		redis.Z{Score: 90, Member: "2"},
		redis.Z{Score: 80, Member: "3"},
	).Err(); err != nil {
		t.Fatalf("seed read positions: %v", err)
	}

	if err := projection.Replace(ctx, 9, []uint64{1, 2}, false); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	for _, member := range []string{"1", "2"} {
		if err := client.ZScore(ctx, readKey, member).Err(); err != nil {
			t.Fatalf("read position %s was pruned: %v", member, err)
		}
	}
	if err := client.ZScore(ctx, readKey, "3").Err(); !errors.Is(err, redis.Nil) {
		t.Fatalf("stale read position error = %v, want redis.Nil", err)
	}

	if err := projection.Replace(ctx, 9, []uint64{1}, true); err != nil {
		t.Fatalf("Replace(dissolved) error = %v", err)
	}
	if err := client.ZScore(ctx, readKey, "2").Err(); err != nil {
		t.Fatalf("dissolved replace pruned read positions: %v", err)
	}
}
