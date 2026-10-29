package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
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
	batch, queueTimeout, timeout, err := rebuildSettings(serverOptions{
		rebuildBatch:        defaultRedisRebuildBatch,
		rebuildQueueTimeout: defaultRedisRebuildQueueTimeout,
		rebuildTimeout:      defaultRedisRebuildTimeout,
	})
	if err != nil {
		t.Fatalf("rebuildSettings: %v", err)
	}
	if batch != defaultRedisRebuildBatch || queueTimeout != defaultRedisRebuildQueueTimeout ||
		timeout != defaultRedisRebuildTimeout {
		t.Fatalf("settings = %d, %s, %s", batch, queueTimeout, timeout)
	}

	for _, invalid := range []serverOptions{
		{rebuildBatch: 0, rebuildQueueTimeout: time.Minute, rebuildTimeout: time.Hour},
		{rebuildBatch: -1, rebuildQueueTimeout: time.Minute, rebuildTimeout: time.Hour},
		{rebuildBatch: maxRedisRebuildBatch + 1, rebuildQueueTimeout: time.Minute, rebuildTimeout: time.Hour},
		{rebuildBatch: 1000, rebuildQueueTimeout: 0, rebuildTimeout: time.Hour},
		{rebuildBatch: 1000, rebuildQueueTimeout: -time.Second, rebuildTimeout: time.Hour},
		{rebuildBatch: 1000, rebuildQueueTimeout: time.Minute, rebuildTimeout: 0},
		{rebuildBatch: 1000, rebuildQueueTimeout: time.Minute, rebuildTimeout: -time.Second},
		// The queue drain must finish inside the overall budget.
		{rebuildBatch: 1000, rebuildQueueTimeout: time.Hour, rebuildTimeout: time.Hour},
		{rebuildBatch: 1000, rebuildQueueTimeout: 2 * time.Hour, rebuildTimeout: time.Hour},
	} {
		if _, _, _, err := rebuildSettings(invalid); err == nil {
			t.Fatalf("rebuildSettings(%#v) must fail", invalid)
		}
	}

	if batch, _, timeout, err := rebuildSettings(serverOptions{
		rebuildBatch:        maxRedisRebuildBatch,
		rebuildQueueTimeout: time.Minute,
		rebuildTimeout:      time.Hour,
	}); err != nil || batch != maxRedisRebuildBatch || timeout != time.Hour {
		t.Fatalf("maximum batch = %d, %s, %v", batch, timeout, err)
	}
}

func TestRebuildQueueTimeoutMustBeShorterThanCommandTimeout(t *testing.T) {
	_, _, _, err := rebuildSettings(serverOptions{
		rebuildBatch:        1000,
		rebuildQueueTimeout: 30 * time.Minute,
		rebuildTimeout:      30 * time.Minute,
	})
	if err == nil ||
		!strings.Contains(err.Error(), "-rebuild-queue-timeout") ||
		!strings.Contains(err.Error(), "-rebuild-timeout") {
		t.Fatalf("error = %v, want both flags named", err)
	}
}

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client, messaging.Keyspace) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := messaging.NewNamespace("im:v12")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	return server, client, messaging.NewKeyspace(namespace)
}

func TestAcquireMaintenanceLockIsSymmetric(t *testing.T) {
	_, client, keyspace := newTestRedis(t)
	ctx := context.Background()
	rebuildLock := maintenanceLock{
		OwnKey:    messaging.RebuildLockKey(keyspace),
		OwnName:   "Redis rebuild",
		OtherKey:  messaging.BackfillLockKey(keyspace),
		OtherName: "seq backfill",
		Token:     "rebuild-token",
		TTL:       time.Hour,
	}
	backfillLock := maintenanceLock{
		OwnKey:    messaging.BackfillLockKey(keyspace),
		OwnName:   "seq backfill",
		OtherKey:  messaging.RebuildLockKey(keyspace),
		OtherName: "Redis rebuild",
		Token:     "backfill-token",
		TTL:       time.Hour,
	}

	if err := acquireMaintenanceLock(ctx, client, rebuildLock); err != nil {
		t.Fatalf("acquire rebuild lock: %v", err)
	}
	if ttl := client.PTTL(ctx, rebuildLock.OwnKey).Val(); ttl <= 0 {
		t.Fatalf("rebuild lock TTL = %v, want a positive PX", ttl)
	}
	err := acquireMaintenanceLock(ctx, client, backfillLock)
	if err == nil || !strings.Contains(err.Error(), "Redis rebuild is running") ||
		!strings.Contains(err.Error(), rebuildLock.OwnKey) {
		t.Fatalf("backfill acquisition error = %v", err)
	}
	err = acquireMaintenanceLock(ctx, client, rebuildLock)
	if err == nil || !strings.Contains(err.Error(), "another Redis rebuild is running") {
		t.Fatalf("second rebuild acquisition error = %v", err)
	}

	// The reverse direction must be refused too.
	if err := client.Del(ctx, rebuildLock.OwnKey).Err(); err != nil {
		t.Fatalf("delete rebuild lock: %v", err)
	}
	if err := acquireMaintenanceLock(ctx, client, backfillLock); err != nil {
		t.Fatalf("acquire backfill lock: %v", err)
	}
	err = acquireMaintenanceLock(ctx, client, rebuildLock)
	if err == nil || !strings.Contains(err.Error(), "seq backfill is running") ||
		!strings.Contains(err.Error(), backfillLock.OwnKey) {
		t.Fatalf("rebuild acquisition error = %v", err)
	}

	if err := acquireMaintenanceLock(ctx, client, maintenanceLock{
		OwnKey: "im:v12:x:lock", OwnName: "x", TTL: 0,
	}); err == nil {
		t.Fatal("a zero-TTL maintenance lock must be refused")
	}
}

func TestEnsureBackendStopped(t *testing.T) {
	original := maintenanceDial
	t.Cleanup(func() { maintenanceDial = original })

	var probed []string
	maintenanceDial = func(address string, timeout time.Duration) (net.Conn, error) {
		probed = append(probed, address)
		if timeout <= 0 {
			t.Fatalf("probe timeout = %v, want positive", timeout)
		}
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	if err := ensureBackendStopped("0.0.0.0:8199", "-rebuild-redis"); err != nil {
		t.Fatalf("refused port must allow the command: %v", err)
	}
	if len(probed) != 1 || probed[0] != "127.0.0.1:8199" {
		t.Fatalf("probed = %v, want 127.0.0.1:8199", probed)
	}

	// Without an explicit -addr the deployment port is checked.
	probed = nil
	if err := ensureBackendStopped("", "-backfill-seq"); err != nil {
		t.Fatalf("refused default port must allow the command: %v", err)
	}
	if len(probed) != 1 || probed[0] != "127.0.0.1:8198" {
		t.Fatalf("default probe = %v, want 127.0.0.1:8198", probed)
	}

	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() { _ = clientSide.Close(); _ = serverSide.Close() })
	maintenanceDial = func(string, time.Duration) (net.Conn, error) {
		return clientSide, nil
	}
	err := ensureBackendStopped("0.0.0.0:8199", "-rebuild-redis")
	if err == nil || !strings.Contains(err.Error(), "serving on 127.0.0.1:8199") {
		t.Fatalf("serving backend error = %v", err)
	}
	for _, fragment := range []string{"imctl backend-stop", "imctl rebuild-redis"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("error %q is missing %q", err, fragment)
		}
	}

	maintenanceDial = func(string, time.Duration) (net.Conn, error) {
		return nil, errors.New("network unreachable")
	}
	if err := ensureBackendStopped("0.0.0.0:8199", "-rebuild-redis"); err == nil ||
		!strings.Contains(err.Error(), "cannot verify") {
		t.Fatalf("indeterminate probe error = %v", err)
	}
}

func TestMaintenanceProbeTarget(t *testing.T) {
	if target, err := maintenanceProbeTarget("0.0.0.0:8199"); err != nil ||
		target != "127.0.0.1:8199" {
		t.Fatalf("target = %q, %v", target, err)
	}
	if target, err := maintenanceProbeTarget("10.0.0.5:9000"); err != nil ||
		target != "127.0.0.1:9000" {
		t.Fatalf("remote binds are probed over loopback: %q, %v", target, err)
	}
	if _, err := maintenanceProbeTarget("no-port"); err == nil {
		t.Fatal("an address without a port must fail")
	}
	options := serverOptions{}
	if got := options.maintenanceAddr(); got != defaultMaintenanceAddr {
		t.Fatalf("default maintenance addr = %q", got)
	}
	options = serverOptions{addr: "0.0.0.0:9000"}
	if got := options.maintenanceAddr(); got != defaultMaintenanceAddr {
		t.Fatalf("implicit -addr default maintenance addr = %q", got)
	}
	options = serverOptions{addr: "0.0.0.0:9000", addrSet: true}
	if got := options.maintenanceAddr(); got != "0.0.0.0:9000" {
		t.Fatalf("explicit -addr maintenance addr = %q", got)
	}
}

func TestSequenceNotReadyErrorSelectsTheCommand(t *testing.T) {
	server, client, keyspace := newTestRedis(t)
	ctx := context.Background()

	err := sequenceNotReadyError(ctx, client, keyspace)
	if err == nil || !strings.Contains(err.Error(), "backfill-seq") ||
		strings.Contains(err.Error(), "rebuild-redis") {
		t.Fatalf("missing ready without a pending marker = %v", err)
	}

	if err := server.Set(messaging.RebuildPendingKey(keyspace), "1"); err != nil {
		t.Fatalf("set pending marker: %v", err)
	}
	err = sequenceNotReadyError(ctx, client, keyspace)
	if err == nil || !strings.Contains(err.Error(), "rebuild-redis") ||
		strings.Contains(err.Error(), "backfill-seq") {
		t.Fatalf("pending marker message = %v", err)
	}
}
