package messaging

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func TestFriendshipProjectionEnsureRemoveAndRebuild(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	projection := NewFriendshipProjection(client)
	ctx := context.Background()
	if err := projection.EnsurePair(ctx, 1, 2); err != nil {
		t.Fatalf("EnsurePair() error = %v", err)
	}
	if ok, err := projection.IsFriend(ctx, 1, 2); err != nil || !ok {
		t.Fatalf("IsFriend(1,2) = %v, %v; want true", ok, err)
	}
	if ok, err := projection.IsFriend(ctx, 2, 1); err != nil || !ok {
		t.Fatalf("IsFriend(2,1) = %v, %v; want true", ok, err)
	}

	if err := projection.RemovePair(ctx, 1, 2); err != nil {
		t.Fatalf("RemovePair() error = %v", err)
	}
	if ok, err := projection.IsFriend(ctx, 1, 2); err != nil || ok {
		t.Fatalf("IsFriend after remove = %v, %v; want false", ok, err)
	}

	if err := projection.EnsurePair(ctx, 9, 10); err != nil {
		t.Fatalf("seed stale pair: %v", err)
	}
	if err := projection.Rebuild(ctx, []store.FriendshipPair{{UserID: 3, FriendUserID: 4}}); err != nil {
		t.Fatalf("Rebuild() error = %v", err)
	}
	if ok, err := projection.IsFriend(ctx, 9, 10); err != nil || ok {
		t.Fatalf("stale pair after rebuild = %v, %v; want false", ok, err)
	}
	if ok, err := projection.IsFriend(ctx, 3, 4); err != nil || !ok {
		t.Fatalf("rebuilt pair = %v, %v; want true", ok, err)
	}
}
