package main

import (
	"context"
	"errors"
	"net"
	"reflect"
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
	refused := func(address string, timeout time.Duration) (net.Conn, error) {
		probed = append(probed, address)
		if timeout <= 0 {
			t.Fatalf("probe timeout = %v, want positive", timeout)
		}
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}

	// Without an explicit -addr every listener this binary can be running is
	// probed: the HTTP default and both deployment ports.
	maintenanceDial = refused
	if err := ensureBackendStopped("", false, "-rebuild-redis"); err != nil {
		t.Fatalf("refused default ports must allow the command: %v", err)
	}
	want := []string{"127.0.0.1:8080", "127.0.0.1:8198", "127.0.0.1:8199"}
	if !reflect.DeepEqual(probed, want) {
		t.Fatalf("default probes = %v, want %v", probed, want)
	}

	// An explicit wildcard -addr probes only that port, over loopback.
	probed = nil
	if err := ensureBackendStopped("0.0.0.0:8199", true, "-rebuild-redis"); err != nil {
		t.Fatalf("refused explicit port must allow the command: %v", err)
	}
	if len(probed) != 1 || probed[0] != "127.0.0.1:8199" {
		t.Fatalf("explicit probes = %v, want 127.0.0.1:8199", probed)
	}

	// An explicit host is honoured, not rewritten to loopback, so a backend
	// bound to a specific interface is still detected.
	probed = nil
	if err := ensureBackendStopped("10.0.0.5:9000", true, "-backfill-seq"); err != nil {
		t.Fatalf("refused explicit host must allow the command: %v", err)
	}
	if len(probed) != 1 || probed[0] != "10.0.0.5:9000" {
		t.Fatalf("host probes = %v, want 10.0.0.5:9000", probed)
	}

	// Any candidate that accepts refuses, even when earlier candidates refused.
	probed = nil
	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() { _ = clientSide.Close(); _ = serverSide.Close() })
	maintenanceDial = func(address string, _ time.Duration) (net.Conn, error) {
		probed = append(probed, address)
		if address == "127.0.0.1:8198" {
			return clientSide, nil
		}
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	err := ensureBackendStopped("", false, "-rebuild-redis")
	if err == nil || !strings.Contains(err.Error(), "serving on 127.0.0.1:8198") {
		t.Fatalf("serving backend error = %v", err)
	}
	if want := []string{"127.0.0.1:8080", "127.0.0.1:8198"}; !reflect.DeepEqual(probed, want) {
		t.Fatalf("probes = %v, want them to stop at the first listener that accepts", probed)
	}
	for _, fragment := range []string{"imctl backend-stop", "imctl rebuild-redis"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("error %q is missing %q", err, fragment)
		}
	}

	// A dial failure that is not a refusal fails closed.
	maintenanceDial = func(string, time.Duration) (net.Conn, error) {
		return nil, errors.New("network unreachable")
	}
	if err := ensureBackendStopped("10.0.0.5:9000", true, "-rebuild-redis"); err == nil ||
		!strings.Contains(err.Error(), "cannot verify") {
		t.Fatalf("indeterminate probe error = %v", err)
	}
}

// TestBackendStoppedBeforeClear covers the RedisRebuilder hook that repeats the
// probe after the queue drain and immediately before ClearNamespace.
func TestBackendStoppedBeforeClear(t *testing.T) {
	original := maintenanceDial
	t.Cleanup(func() { maintenanceDial = original })

	maintenanceDial = func(string, time.Duration) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	check := backendStoppedBeforeClear(
		serverOptions{addr: "0.0.0.0:8199", addrSet: true}, "-rebuild-redis",
	)
	if check == nil {
		t.Fatal("BeforeClear hook is nil")
	}
	if err := check(context.Background()); err != nil {
		t.Fatalf("a stopped backend must allow the rebuild: %v", err)
	}

	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() { _ = clientSide.Close(); _ = serverSide.Close() })
	maintenanceDial = func(string, time.Duration) (net.Conn, error) {
		return clientSide, nil
	}
	err := check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "serving on 127.0.0.1:8199") {
		t.Fatalf("serving backend before ClearNamespace = %v", err)
	}

	// A cancelled rebuild context aborts the hook without probing.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	maintenanceDial = func(string, time.Duration) (net.Conn, error) {
		t.Fatal("the hook must not dial with a cancelled context")
		return nil, nil
	}
	if err := check(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled hook error = %v, want context.Canceled", err)
	}
}

func TestRedisRebuildOptionsWireBeforeClear(t *testing.T) {
	_, _, keyspace := newTestRedis(t)
	namespace, err := messaging.NewNamespace("im:v12")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}

	options := redisRebuildOptions(
		serverOptions{addr: "0.0.0.0:8199", addrSet: true}, keyspace, namespace,
		250, time.Minute, nil,
	)
	if options.Keyspace != keyspace || options.Namespace.Base() != namespace.Base() ||
		options.Batch != 250 || options.DrainQueues != nil {
		t.Fatalf("options = %#v", options)
	}
	if options.BeforeClear == nil {
		t.Fatal("the rebuild options must carry the BeforeClear probe")
	}

	original := maintenanceDial
	t.Cleanup(func() { maintenanceDial = original })
	maintenanceDial = func(string, time.Duration) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	if err := options.BeforeClear(context.Background()); err != nil {
		t.Fatalf("BeforeClear with a stopped backend = %v", err)
	}
	maintenanceDial = func(string, time.Duration) (net.Conn, error) {
		return nil, errors.New("network unreachable")
	}
	if err := options.BeforeClear(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "cannot verify") {
		t.Fatalf("BeforeClear probe error = %v", err)
	}
}

func TestMaintenanceProbeTargets(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		addrSet bool
		want    []string
	}{
		{"wildcard host", "0.0.0.0:8199", true, []string{"127.0.0.1:8199"}},
		{"empty host", ":8199", true, []string{"127.0.0.1:8199"}},
		{"IPv6 wildcard", "[::]:8199", true, []string{"127.0.0.1:8199"}},
		{"loopback", "127.0.0.1:9000", true, []string{"127.0.0.1:9000"}},
		{"specific host", "10.0.0.5:9000", true, []string{"10.0.0.5:9000"}},
		{"unset", "", false, []string{"127.0.0.1:8080", "127.0.0.1:8198", "127.0.0.1:8199"}},
		{"implicit flag default", "127.0.0.1:8080", false,
			[]string{"127.0.0.1:8080", "127.0.0.1:8198", "127.0.0.1:8199"}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			targets, err := maintenanceProbeTargets(testCase.addr, testCase.addrSet)
			if err != nil {
				t.Fatalf("maintenanceProbeTargets(%q, %t): %v", testCase.addr, testCase.addrSet, err)
			}
			if !reflect.DeepEqual(targets, testCase.want) {
				t.Fatalf("targets = %v, want %v", targets, testCase.want)
			}
		})
	}
	if _, err := maintenanceProbeTargets("no-port", true); err == nil {
		t.Fatal("an address without a port must fail")
	}
	if _, err := maintenanceProbeTargets("10.0.0.5:9000", false); err != nil {
		t.Fatalf("an implicit -addr must fall back to the defaults: %v", err)
	}
}

// TestCheckMaintenanceLocks covers the startup gate that refuses a backend on a
// namespace a one-shot maintenance command is rebuilding.
func TestCheckMaintenanceLocks(t *testing.T) {
	server, client, keyspace := newTestRedis(t)
	ctx := context.Background()

	if err := checkMaintenanceLocks(ctx, client, keyspace); err != nil {
		t.Fatalf("no locks must allow startup: %v", err)
	}
	if err := server.Set(messaging.BackfillLockKey(keyspace), "token"); err != nil {
		t.Fatalf("set backfill lock: %v", err)
	}
	err := checkMaintenanceLocks(ctx, client, keyspace)
	if err == nil || !strings.Contains(err.Error(), "seq backfill") ||
		!strings.Contains(err.Error(), messaging.BackfillLockKey(keyspace)) {
		t.Fatalf("backfill lock error = %v", err)
	}
	server.Del(messaging.BackfillLockKey(keyspace))

	if err := server.Set(messaging.RebuildLockKey(keyspace), "token"); err != nil {
		t.Fatalf("set rebuild lock: %v", err)
	}
	err = checkMaintenanceLocks(ctx, client, keyspace)
	if err == nil || !strings.Contains(err.Error(), "Redis rebuild") ||
		!strings.Contains(err.Error(), messaging.RebuildLockKey(keyspace)) {
		t.Fatalf("rebuild lock error = %v", err)
	}
	server.Del(messaging.RebuildLockKey(keyspace))

	if err := checkMaintenanceLocks(ctx, client, keyspace); err != nil {
		t.Fatalf("deleted locks must allow startup again: %v", err)
	}
}

func TestSequenceNotReadyErrorSelectsTheCommand(t *testing.T) {
	server, client, keyspace := newTestRedis(t)
	ctx := context.Background()
	numbered := func(context.Context) (uint64, uint64, error) { return 0, 0, nil }

	// Total Redis loss: no marker and nothing left to number. A backfill would
	// certify a namespace without entities, feeds, previews or dedup.
	err := sequenceNotReadyError(ctx, client, keyspace, numbered)
	if err == nil || !strings.Contains(err.Error(), "rebuild-redis") ||
		strings.Contains(err.Error(), "backfill-seq") {
		t.Fatalf("wipe hint = %v", err)
	}

	// A pending marker wins even when MySQL looks fully numbered.
	if err := server.Set(messaging.RebuildPendingKey(keyspace), "1"); err != nil {
		t.Fatalf("set pending marker: %v", err)
	}
	err = sequenceNotReadyError(ctx, client, keyspace, numbered)
	if err == nil || !strings.Contains(err.Error(), "rebuild-redis") ||
		!strings.Contains(err.Error(), messaging.RebuildPendingKey(keyspace)) ||
		strings.Contains(err.Error(), "backfill-seq") {
		t.Fatalf("pending marker message = %v", err)
	}
	server.Del(messaging.RebuildPendingKey(keyspace))

	// Rows still without a seq need the switch, not a rebuild.
	for _, counts := range [][2]uint64{{2, 0}, {0, 3}, {4, 5}} {
		unnumbered := func(context.Context) (uint64, uint64, error) {
			return counts[0], counts[1], nil
		}
		err := sequenceNotReadyError(ctx, client, keyspace, unnumbered)
		if err == nil || !strings.Contains(err.Error(), "backfill-seq") ||
			strings.Contains(err.Error(), "rebuild-redis") {
			t.Fatalf("unnumbered hint for %v = %v", counts, err)
		}
	}

	// A MySQL failure is reported, never guessed away.
	failing := func(context.Context) (uint64, uint64, error) {
		return 0, 0, errors.New("mysql down")
	}
	if err := sequenceNotReadyError(ctx, client, keyspace, failing); err == nil ||
		!strings.Contains(err.Error(), "count unnumbered messages") {
		t.Fatalf("counter error = %v", err)
	}
	if err := sequenceNotReadyError(ctx, client, keyspace, nil); err == nil ||
		!strings.Contains(err.Error(), "without MySQL") {
		t.Fatalf("missing counter error = %v", err)
	}
}
