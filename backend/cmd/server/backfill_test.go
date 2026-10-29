package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
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

func TestRefuseIncompleteRebuild(t *testing.T) {
	server, client, keyspace := newTestRedis(t)
	ctx := context.Background()

	if err := refuseIncompleteRebuild(ctx, client, keyspace); err != nil {
		t.Fatalf("no pending marker must allow backfill: %v", err)
	}
	if err := server.Set(messaging.RebuildPendingKey(keyspace), "1"); err != nil {
		t.Fatalf("set pending marker: %v", err)
	}
	err := refuseIncompleteRebuild(ctx, client, keyspace)
	if err == nil || !strings.Contains(err.Error(), "rebuild-redis") ||
		!strings.Contains(err.Error(), messaging.RebuildPendingKey(keyspace)) {
		t.Fatalf("pending marker error = %v", err)
	}
}

type fakeMaxMessageIDSource struct {
	maxID uint64
	err   error
}

func (f fakeMaxMessageIDSource) MaxServerMessageID(context.Context) (uint64, error) {
	return f.maxID, f.err
}

// TestRefuseMissingProjection covers the defence-in-depth guard: a direct
// -backfill-seq on a wiped namespace must not certify counters and indexes as a
// complete read model.
func TestRefuseMissingProjection(t *testing.T) {
	server, client, _ := newTestRedis(t)
	namespace, err := messaging.NewNamespace("im:v12")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	ctx := context.Background()
	prefix := namespace.RedisPrefix()
	counterKey := prefix + "messages:next_id"
	epochKey := prefix + "index:epoch"

	// An empty database is a legitimate first backfill.
	if err := refuseMissingProjection(ctx, client, namespace, fakeMaxMessageIDSource{}); err != nil {
		t.Fatalf("empty MySQL must allow backfill: %v", err)
	}

	// Numbered history with both projection markers present: only seq:ready was
	// lost, the backfill can rebuild the counters.
	if err := server.Set(counterKey, "42"); err != nil {
		t.Fatalf("set counter: %v", err)
	}
	if err := server.Set(epochKey, "0"); err != nil {
		t.Fatalf("set epoch: %v", err)
	}
	if err := refuseMissingProjection(ctx, client, namespace, fakeMaxMessageIDSource{maxID: 42}); err != nil {
		t.Fatalf("intact projection must allow backfill: %v", err)
	}

	// Both markers gone while MySQL holds history: the total-loss signature.
	server.Del(counterKey)
	server.Del(epochKey)
	err = refuseMissingProjection(ctx, client, namespace, fakeMaxMessageIDSource{maxID: 42})
	if err == nil || !strings.Contains(err.Error(), "rebuild-redis") ||
		!strings.Contains(err.Error(), "projection") {
		t.Fatalf("wiped projection error = %v, want the rebuild hint", err)
	}

	// One surviving marker is partial loss, which the audit repairs; it is not
	// the total-loss signature.
	if err := server.Set(counterKey, "42"); err != nil {
		t.Fatalf("restore counter: %v", err)
	}
	if err := refuseMissingProjection(ctx, client, namespace, fakeMaxMessageIDSource{maxID: 42}); err != nil {
		t.Fatalf("partial loss must allow backfill: %v", err)
	}

	// A MySQL read failure is reported instead of guessed away.
	if err := refuseMissingProjection(ctx, client, namespace, fakeMaxMessageIDSource{
		err: errors.New("mysql down"),
	}); err == nil || !strings.Contains(err.Error(), "max server message id") {
		t.Fatalf("MySQL error = %v", err)
	}
	if err := refuseMissingProjection(ctx, client, namespace, nil); err == nil ||
		!strings.Contains(err.Error(), "without MySQL") {
		t.Fatalf("missing source error = %v", err)
	}
}
