package main

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestSeqBackfillLockReleaseOnlyDeletesOwnToken(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	key := "im:v12:backfill:lock"

	acquired, err := client.SetNX(ctx, key, "token-a", time.Minute).Result()
	if err != nil || !acquired {
		t.Fatalf("acquire lock = %t, %v", acquired, err)
	}
	second, err := client.SetNX(ctx, key, "token-b", time.Minute).Result()
	if err != nil || second {
		t.Fatalf("second acquire = %t, %v; want refused", second, err)
	}

	if _, err := client.Eval(
		ctx, releaseSeqBackfillLockScript, []string{key}, "token-b",
	).Result(); err != nil {
		t.Fatalf("release with a foreign token: %v", err)
	}
	if ttl := client.TTL(ctx, key).Val(); ttl <= 0 {
		t.Fatalf("lock removed by a foreign token (ttl %v)", ttl)
	}

	if _, err := client.Eval(
		ctx, releaseSeqBackfillLockScript, []string{key}, "token-a",
	).Result(); err != nil {
		t.Fatalf("release with the owner token: %v", err)
	}
	if exists := client.Exists(ctx, key).Val(); exists != 0 {
		t.Fatalf("lock still present after its owner released it")
	}
}
