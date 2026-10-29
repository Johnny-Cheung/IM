package messaging

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeRunIDSource struct {
	mu    sync.Mutex
	id    string
	err   error
	calls int
}

func (f *fakeRunIDSource) RunID(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return "", f.err
	}
	return f.id, nil
}

func (f *fakeRunIDSource) set(id string) {
	f.mu.Lock()
	f.id = id
	f.mu.Unlock()
}

func (f *fakeRunIDSource) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type fakeCalibrator struct {
	mu               sync.Mutex
	gate             *SeqGate
	calls            int
	failures         int
	conversations    int
	closedAtCallTime []bool

	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *fakeCalibrator) Calibrate(context.Context) (int, error) {
	f.mu.Lock()
	f.calls++
	f.closedAtCallTime = append(f.closedAtCallTime, f.gate != nil && !f.gate.IsReady())
	failures := f.failures
	if failures > 0 {
		f.failures--
	}
	conversations := f.conversations
	started, release := f.started, f.release
	f.mu.Unlock()
	if failures > 0 {
		return 0, errors.New("mysql unavailable")
	}
	if started != nil {
		f.once.Do(func() { close(started) })
		<-release
	}
	return conversations, nil
}

func (f *fakeCalibrator) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeCalibrator) wasClosed(index int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return index < len(f.closedAtCallTime) && f.closedAtCallTime[index]
}

func newWatcherTest(
	t *testing.T,
) (*redis.Client, *miniredis.Miniredis, *fakeRunIDSource, *fakeCalibrator, *SeqGate, *SeqWatcher) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	gate := NewSeqGate()
	calibrator := &fakeCalibrator{gate: gate, conversations: 3}
	runIDs := &fakeRunIDSource{id: "run-a"}
	watcher := NewSeqWatcher(client, SeqWatcherOptions{
		Keyspace:     defaultKeyspace,
		Gate:         gate,
		Calibrator:   calibrator,
		RunIDs:       runIDs,
		InitialRunID: runIDs.id,
		Interval:     time.Hour,
		Backoff:      time.Millisecond,
		MaxBackoff:   2 * time.Millisecond,
	})
	return client, server, runIDs, calibrator, gate, watcher
}

func TestSeqWatcherFirstCheckOnlyRecordsRunID(t *testing.T) {
	client, _, _, calibrator, gate, watcher := newWatcherTest(t)
	seedSequenceReady(t, client)

	watcher.check(context.Background())
	if calibrator.callCount() != 0 {
		t.Fatalf("calibrated on the first check")
	}
	if err := gate.Ready(); err != nil {
		t.Fatalf("gate closed on the first check: %v", err)
	}
}

func TestSeqWatcherFirstCheckDetectsRestartSinceStartup(t *testing.T) {
	client, _, runIDs, calibrator, gate, watcher := newWatcherTest(t)
	seedSequenceReady(t, client)
	runIDs.set("run-b")

	watcher.check(context.Background())
	if calibrator.callCount() != 1 {
		t.Fatalf("calibrate calls = %d, want 1 after a restart during startup", calibrator.callCount())
	}
	if err := gate.Ready(); err != nil {
		t.Fatalf("gate not reopened after the first check: %v", err)
	}
}

func TestSeqWatcherFirstCheckCalibratesWithoutBaseline(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	seedSequenceReady(t, client)
	gate := NewSeqGate()
	calibrator := &fakeCalibrator{gate: gate, conversations: 2}
	watcher := NewSeqWatcher(client, SeqWatcherOptions{
		Keyspace:   defaultKeyspace,
		Gate:       gate,
		Calibrator: calibrator,
		RunIDs:     &fakeRunIDSource{id: "run-a"},
		Interval:   time.Hour,
		Backoff:    time.Millisecond,
		MaxBackoff: 2 * time.Millisecond,
	})

	watcher.check(context.Background())
	if calibrator.callCount() != 1 {
		t.Fatalf("calibrate calls = %d, want 1 without a primed run_id", calibrator.callCount())
	}
	if err := gate.Ready(); err != nil {
		t.Fatalf("gate not reopened: %v", err)
	}
}

// probeFailureClient fails a single Exists call so the watcher's post-
// calibration readiness probe can be exercised.
type probeFailureClient struct {
	redis.UniversalClient
	mu     sync.Mutex
	calls  int
	failAt int
}

func (c *probeFailureClient) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	c.mu.Lock()
	c.calls++
	fail := c.calls == c.failAt
	c.mu.Unlock()
	if !fail {
		return c.UniversalClient.Exists(ctx, keys...)
	}
	command := redis.NewIntCmd(ctx)
	command.SetErr(errors.New("probe failed"))
	return command
}

func TestSeqWatcherRetriesReadinessProbe(t *testing.T) {
	server := miniredis.RunT(t)
	base := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = base.Close() })
	seedSequenceReady(t, base)
	// Each check probes seq:ready then the maintenance locks; fail the readiness
	// probe right after calibration.
	client := &probeFailureClient{UniversalClient: base, failAt: 3}
	gate := NewSeqGate()
	calibrator := &fakeCalibrator{gate: gate, conversations: 3}
	runIDs := &fakeRunIDSource{id: "run-a"}
	watcher := NewSeqWatcher(client, SeqWatcherOptions{
		Keyspace:     defaultKeyspace,
		Gate:         gate,
		Calibrator:   calibrator,
		RunIDs:       runIDs,
		InitialRunID: "run-a",
		Interval:     time.Hour,
		Backoff:      time.Millisecond,
		MaxBackoff:   2 * time.Millisecond,
	})

	runIDs.set("run-b")
	watcher.check(context.Background())
	if calibrator.callCount() != 1 {
		t.Fatalf("calibrate calls = %d, want 1", calibrator.callCount())
	}
	if err := gate.Ready(); err != nil {
		t.Fatalf("gate stayed closed after a transient probe error: %v", err)
	}
	if status := watcher.Status(); status.LastError != "" {
		t.Fatalf("last error = %q, want cleared after the retry", status.LastError)
	}
}

func TestSeqWatcherRecalibratesOnRunIDChange(t *testing.T) {
	client, _, runIDs, calibrator, gate, watcher := newWatcherTest(t)
	seedSequenceReady(t, client)
	ctx := context.Background()
	watcher.check(ctx)

	runIDs.set("run-b")
	watcher.check(ctx)

	if calibrator.callCount() != 1 {
		t.Fatalf("calibrate calls = %d, want 1", calibrator.callCount())
	}
	if !calibrator.wasClosed(0) {
		t.Fatal("gate was open during calibration")
	}
	if err := gate.Ready(); err != nil {
		t.Fatalf("gate not reopened after calibration: %v", err)
	}
	status := watcher.Status()
	if !status.Ready || status.Conversations != 3 || status.LastCalibratedAt.IsZero() {
		t.Fatalf("status = %#v", status)
	}
	if status.RunID != "run-b" || status.Calibrating || status.WaitingSeqReady {
		t.Fatalf("status = %#v, want run-b and no calibration flags", status)
	}
}

func TestSeqWatcherReportsCalibrating(t *testing.T) {
	client, _, runIDs, calibrator, _, watcher := newWatcherTest(t)
	seedSequenceReady(t, client)
	started := make(chan struct{})
	release := make(chan struct{})
	calibrator.mu.Lock()
	calibrator.started = started
	calibrator.release = release
	calibrator.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		runIDs.set("run-b")
		watcher.check(context.Background())
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("calibration did not start")
	}
	if status := watcher.Status(); !status.Calibrating || status.Ready {
		t.Fatalf("status during calibration = %#v", status)
	}
	close(release)
	<-done
	if status := watcher.Status(); status.Calibrating || !status.Ready {
		t.Fatalf("status after calibration = %#v", status)
	}
}

func TestSeqWatcherKeepsGateClosedWhenReadyIsMissing(t *testing.T) {
	client, server, runIDs, calibrator, gate, watcher := newWatcherTest(t)
	seedSequenceReady(t, client)
	ctx := context.Background()
	watcher.check(ctx)

	runIDs.set("run-b")
	server.Del(sequenceReadyKey())
	watcher.check(ctx)
	if calibrator.callCount() != 1 {
		t.Fatalf("calibrate calls = %d, want 1", calibrator.callCount())
	}
	if gate.Ready() == nil {
		t.Fatal("gate open while seq:ready is missing")
	}
	if watcher.Status().Ready {
		t.Fatal("status ready while seq:ready is missing")
	}
	if status := watcher.Status(); !status.WaitingSeqReady || status.Calibrating {
		t.Fatalf("status while waiting for seq:ready = %#v", status)
	}

	watcher.check(ctx)
	if calibrator.callCount() != 1 {
		t.Fatalf("recalibrated again while waiting for readiness")
	}

	seedSequenceReady(t, client)
	watcher.check(ctx)
	if calibrator.callCount() != 2 {
		t.Fatalf("calibrate calls = %d, want 2", calibrator.callCount())
	}
	if err := gate.Ready(); err != nil {
		t.Fatalf("gate not reopened: %v", err)
	}
}

func TestSeqWatcherRetriesCalibrationWithBackoff(t *testing.T) {
	client, _, runIDs, calibrator, gate, watcher := newWatcherTest(t)
	seedSequenceReady(t, client)
	ctx := context.Background()
	watcher.check(ctx)

	runIDs.set("run-b")
	calibrator.failures = 2
	watcher.check(ctx)

	if calibrator.callCount() != 3 {
		t.Fatalf("calibrate calls = %d, want 3", calibrator.callCount())
	}
	if err := gate.Ready(); err != nil {
		t.Fatalf("gate not reopened after retries: %v", err)
	}
	if status := watcher.Status(); status.LastError != "" {
		t.Fatalf("last error = %q, want cleared after success", status.LastError)
	}
}

func TestSeqWatcherMaintenanceLockClosesGate(t *testing.T) {
	client, _, _, calibrator, gate, watcher := newWatcherTest(t)
	seedSequenceReady(t, client)
	ctx := context.Background()

	watcher.check(ctx)
	if err := gate.Ready(); err != nil {
		t.Fatalf("gate closed on the baseline check: %v", err)
	}

	if err := client.Set(ctx, RebuildLockKey(defaultKeyspace), "token", 0).Err(); err != nil {
		t.Fatalf("seed rebuild lock: %v", err)
	}
	watcher.check(ctx)
	if gate.Ready() == nil {
		t.Fatal("gate open while a rebuild lock exists")
	}
	status := watcher.Status()
	if status.Ready || !status.Maintenance || status.WaitingSeqReady {
		t.Fatalf("status with a rebuild lock = %#v", status)
	}
	if calibrator.callCount() != 0 {
		t.Fatalf("calibrate calls = %d, want 0 during maintenance", calibrator.callCount())
	}

	if err := client.Del(ctx, RebuildLockKey(defaultKeyspace)).Err(); err != nil {
		t.Fatalf("remove rebuild lock: %v", err)
	}
	watcher.check(ctx)
	if calibrator.callCount() != 1 {
		t.Fatalf("calibrate calls = %d, want 1 after maintenance", calibrator.callCount())
	}
	if err := gate.Ready(); err != nil {
		t.Fatalf("gate not reopened after maintenance: %v", err)
	}
	if status := watcher.Status(); status.Maintenance || !status.Ready {
		t.Fatalf("status after maintenance = %#v", status)
	}
}

func TestSeqWatcherMaintenanceLockAtStartupKeepsGateClosed(t *testing.T) {
	client, _, runIDs, calibrator, gate, watcher := newWatcherTest(t)
	seedSequenceReady(t, client)
	ctx := context.Background()
	if err := client.Set(ctx, BackfillLockKey(defaultKeyspace), "token", 0).Err(); err != nil {
		t.Fatalf("seed backfill lock: %v", err)
	}

	watcher.check(ctx)
	if gate.Ready() == nil {
		t.Fatal("gate open on the first check while a backfill lock exists")
	}
	if calibrator.callCount() != 0 {
		t.Fatalf("calibrate calls = %d, want 0 during maintenance", calibrator.callCount())
	}

	// A restart detected while the lock is held must not calibrate either.
	runIDs.set("run-b")
	watcher.check(ctx)
	if calibrator.callCount() != 0 {
		t.Fatalf("calibrate calls = %d, want 0 while locked", calibrator.callCount())
	}
	if err := gate.Ready(); err == nil {
		t.Fatal("gate reopened while the backfill lock still exists")
	}

	if err := client.Del(ctx, BackfillLockKey(defaultKeyspace)).Err(); err != nil {
		t.Fatalf("remove backfill lock: %v", err)
	}
	watcher.check(ctx)
	if calibrator.callCount() != 1 {
		t.Fatalf("calibrate calls = %d, want 1 once the lock is gone", calibrator.callCount())
	}
	if err := gate.Ready(); err != nil {
		t.Fatalf("gate not reopened: %v", err)
	}
	status := watcher.Status()
	if status.Maintenance || !status.Ready || status.RunID != "run-b" {
		t.Fatalf("status after maintenance = %#v", status)
	}
}

func TestSeqWatcherNotifyRunsImmediately(t *testing.T) {
	client, _, runIDs, calibrator, gate, watcher := newWatcherTest(t)
	seedSequenceReady(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		watcher.Run(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for runIDs.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	runIDs.set("run-b")
	watcher.Notify()
	for calibrator.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	if calibrator.callCount() != 1 {
		t.Fatalf("calibrate calls = %d, want 1", calibrator.callCount())
	}
	if err := gate.Ready(); err != nil {
		t.Fatalf("gate not reopened: %v", err)
	}
}

func TestParseRunIDInfo(t *testing.T) {
	runID, err := parseRunIDInfo("# Server\r\nredis_version:8.0.0\r\nrun_id:abc123\r\n")
	if err != nil || runID != "abc123" {
		t.Fatalf("parseRunIDInfo() = %q, %v", runID, err)
	}
	if _, err := parseRunIDInfo("# Server\r\nredis_version:8.0.0\r\n"); err == nil {
		t.Fatal("parseRunIDInfo() error = nil, want missing run_id error")
	}
}
