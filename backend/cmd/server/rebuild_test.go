package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
)

func TestValidateCommandModes(t *testing.T) {
	accepted := []serverOptions{
		{},
		{backfillSeq: true},
		{rebuildRedis: true},
		{dlqList: true},
		{dlqReplay: true},
		{dlqDiscard: true},
	}
	for _, options := range accepted {
		if err := validateCommandModes(options); err != nil {
			t.Fatalf("validateCommandModes(%#v) = %v", options, err)
		}
	}

	rejected := []struct {
		options serverOptions
		want    []string
	}{
		{
			options: serverOptions{backfillSeq: true, rebuildRedis: true},
			want:    []string{"-backfill-seq", "-rebuild-redis"},
		},
		{
			options: serverOptions{rebuildRedis: true, dlqList: true},
			want:    []string{"-rebuild-redis", "-dlq-list"},
		},
		{
			options: serverOptions{backfillSeq: true, dlqDiscard: true},
			want:    []string{"-backfill-seq", "-dlq-discard"},
		},
		{
			options: serverOptions{dlqList: true, dlqReplay: true},
			want:    []string{"-dlq-list", "-dlq-replay"},
		},
	}
	for _, testCase := range rejected {
		err := validateCommandModes(testCase.options)
		if err == nil {
			t.Fatalf("validateCommandModes(%#v) must fail", testCase.options)
		}
		for _, fragment := range testCase.want {
			if !strings.Contains(err.Error(), fragment) {
				t.Fatalf("error %q is missing %q", err, fragment)
			}
		}
	}
}

func TestRebuildSettings(t *testing.T) {
	batch, queueTimeout, err := rebuildSettings(serverOptions{
		rebuildBatch:        defaultRedisRebuildBatch,
		rebuildQueueTimeout: defaultRedisRebuildQueueTimeout,
	})
	if err != nil {
		t.Fatalf("rebuildSettings: %v", err)
	}
	if batch != defaultRedisRebuildBatch || queueTimeout != defaultRedisRebuildQueueTimeout {
		t.Fatalf("settings = %d, %s", batch, queueTimeout)
	}

	for _, invalid := range []serverOptions{
		{rebuildBatch: 0, rebuildQueueTimeout: time.Minute},
		{rebuildBatch: -1, rebuildQueueTimeout: time.Minute},
		{rebuildBatch: maxRedisRebuildBatch + 1, rebuildQueueTimeout: time.Minute},
		{rebuildBatch: 1000, rebuildQueueTimeout: 0},
		{rebuildBatch: 1000, rebuildQueueTimeout: -time.Second},
	} {
		if _, _, err := rebuildSettings(invalid); err == nil {
			t.Fatalf("rebuildSettings(%#v) must fail", invalid)
		}
	}

	if batch, _, err := rebuildSettings(serverOptions{
		rebuildBatch: maxRedisRebuildBatch, rebuildQueueTimeout: time.Minute,
	}); err != nil || batch != maxRedisRebuildBatch {
		t.Fatalf("maximum batch = %d, %v", batch, err)
	}
}

func TestCheckMaintenanceLocks(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := messaging.NewNamespace("im:v12")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	keyspace := messaging.NewKeyspace(namespace)
	ctx := context.Background()

	if err := checkMaintenanceLocks(ctx, client, keyspace); err != nil {
		t.Fatalf("no lock must allow startup: %v", err)
	}

	backfillKey := messaging.BackfillLockKey(keyspace)
	if err := server.Set(backfillKey, "token"); err != nil {
		t.Fatalf("set backfill lock: %v", err)
	}
	err = checkMaintenanceLocks(ctx, client, keyspace)
	if err == nil || !strings.Contains(err.Error(), "seq backfill") {
		t.Fatalf("backfill lock error = %v", err)
	}
	server.Del(backfillKey)

	rebuildKey := messaging.RebuildLockKey(keyspace)
	if err := server.Set(rebuildKey, "token"); err != nil {
		t.Fatalf("set rebuild lock: %v", err)
	}
	err = checkMaintenanceLocks(ctx, client, keyspace)
	if err == nil || !strings.Contains(err.Error(), "Redis rebuild") ||
		!strings.Contains(err.Error(), rebuildKey) {
		t.Fatalf("rebuild lock error = %v", err)
	}
	server.Del(rebuildKey)

	if err := checkMaintenanceLocks(ctx, client, keyspace); err != nil {
		t.Fatalf("removing the locks must allow startup: %v", err)
	}
}
