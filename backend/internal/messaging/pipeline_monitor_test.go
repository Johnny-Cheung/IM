package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
)

const pipelineMonitorBaseMillis int64 = 1700000000000

type pipelineMonitorTestEnv struct {
	monitor *PipelineMonitor
	client  *redis.Client
	server  *miniredis.Miniredis
	now     time.Time
	logs    []string
}

func newPipelineMonitorTestEnv(t *testing.T, options PipelineMonitorOptions) *pipelineMonitorTestEnv {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	env := &pipelineMonitorTestEnv{
		client: client,
		server: server,
		now:    time.UnixMilli(pipelineMonitorBaseMillis + 61000).UTC(),
	}
	if options.DirectRelay == nil {
		options.DirectRelay = func() RelayStatus {
			return RelayStatus{Connected: true, Relayed: 3, Recovered: 1}
		}
	}
	if options.DirectConsumer == nil {
		options.DirectConsumer = func() ConsumerStatus {
			return ConsumerStatus{Persisted: 2, DeadLettered: 1}
		}
	}
	if options.Now == nil {
		options.Now = func() time.Time { return env.now }
	}
	if options.Logf == nil {
		options.Logf = func(format string, args ...interface{}) {
			env.logs = append(env.logs, fmt.Sprintf(format, args...))
		}
	}
	options.Keyspace = defaultKeyspace
	options.Namespace = DefaultNamespace()
	env.monitor = NewPipelineMonitor(client, config.RabbitMQConfig{}, nil, options)
	return env
}

func (e *pipelineMonitorTestEnv) addEntry(t *testing.T, stream string, offsetMS int64) string {
	t.Helper()
	id := fmt.Sprintf("%d-0", pipelineMonitorBaseMillis+offsetMS)
	if _, err := e.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		ID:     id,
		Values: map[string]interface{}{"seq": "1", "body": "hello"},
	}).Result(); err != nil {
		t.Fatalf("xadd %s %s: %v", stream, id, err)
	}
	return id
}

func (e *pipelineMonitorTestEnv) sample(t *testing.T) PipelineStatus {
	t.Helper()
	e.monitor.sampleOnce(context.Background())
	return e.monitor.Status()
}

func TestPipelineMonitorReportsEmptyStream(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{})

	status := env.sample(t)

	if status.Direct.StreamLength != 0 || status.Direct.Unrelayed != 0 {
		t.Fatalf("stream lengths = %d/%d, want 0/0", status.Direct.StreamLength, status.Direct.Unrelayed)
	}
	if status.Direct.Pending != 0 {
		t.Fatalf("pending = %d, want 0", status.Direct.Pending)
	}
	if status.Direct.OldestAgeSeconds != 0 {
		t.Fatalf("oldest age = %d, want 0", status.Direct.OldestAgeSeconds)
	}
	if status.Direct.BacklogAlert {
		t.Fatal("backlog alert = true, want false")
	}
	if status.Direct.LastError != "" {
		t.Fatalf("last error = %q, want empty for a fresh stream", status.Direct.LastError)
	}
	if status.Direct.DeadLetterCount != nil {
		t.Fatalf("dead letter count = %d, want unknown", *status.Direct.DeadLetterCount)
	}
	if status.Group != nil {
		t.Fatalf("group chain = %#v, want disabled", status.Group)
	}
	if status.Samples != 1 {
		t.Fatalf("samples = %d, want 1", status.Samples)
	}
	if !status.SampledAt.Equal(env.now) {
		t.Fatalf("sampled at = %s, want %s", status.SampledAt, env.now)
	}
	if status.Direct.Relay.Relayed != 3 || status.Direct.Relay.Recovered != 1 {
		t.Fatalf("relay status = %#v", status.Direct.Relay)
	}
	if status.Direct.Consumer.Persisted != 2 {
		t.Fatalf("consumer status = %#v", status.Direct.Consumer)
	}
	if len(env.logs) != 0 {
		t.Fatalf("logs = %v, want none", env.logs)
	}
}

func TestPipelineMonitorReportsUnrelayedEntriesAndOldestAge(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{})
	stream := defaultKeyspace.messageStreamKey()
	for index := int64(0); index < 3; index++ {
		env.addEntry(t, stream, index*1000)
	}

	status := env.sample(t)

	if status.Direct.StreamLength != 3 || status.Direct.Unrelayed != 3 {
		t.Fatalf(
			"stream lengths = %d/%d, want 3/3",
			status.Direct.StreamLength, status.Direct.Unrelayed,
		)
	}
	if status.Direct.OldestAgeSeconds != 61 {
		t.Fatalf("oldest age = %d, want 61", status.Direct.OldestAgeSeconds)
	}
	if !status.Direct.BacklogAlert {
		t.Fatal("backlog alert = false, want true for a 61s old entry")
	}

	relaxed := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{
		BacklogLimit: 5,
		OldestLimit:  2 * time.Minute,
	})
	for index := int64(0); index < 3; index++ {
		relaxed.addEntry(t, stream, index*1000)
	}
	if relaxed.sample(t).Direct.BacklogAlert {
		t.Fatal("backlog alert = true, want false under raised thresholds")
	}

	counted := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{
		BacklogLimit: 2,
		OldestLimit:  time.Hour,
	})
	for index := int64(0); index < 3; index++ {
		counted.addEntry(t, stream, index*1000)
	}
	if !counted.sample(t).Direct.BacklogAlert {
		t.Fatal("backlog alert = false, want true for three unrelayed entries")
	}

	boundary := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{})
	boundary.now = time.UnixMilli(pipelineMonitorBaseMillis + 60000).UTC()
	boundary.addEntry(t, stream, 0)
	boundaryStatus := boundary.sample(t)
	if boundaryStatus.Direct.OldestAgeSeconds != 60 {
		t.Fatalf("oldest age = %d, want 60", boundaryStatus.Direct.OldestAgeSeconds)
	}
	if boundaryStatus.Direct.BacklogAlert {
		t.Fatal("backlog alert = true at exactly the 60s threshold, want false")
	}
}

func TestPipelineMonitorCountsPendingEntries(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{})
	ctx := context.Background()
	stream := defaultKeyspace.messageStreamKey()
	if err := env.client.XGroupCreateMkStream(
		ctx, stream, directRelayGroup(DefaultNamespace()), "0-0",
	).Err(); err != nil {
		t.Fatalf("create relay group: %v", err)
	}
	env.addEntry(t, stream, 0)
	env.addEntry(t, stream, 1000)
	if _, err := env.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    directRelayGroup(DefaultNamespace()),
		Consumer: directConsumerName(DefaultNamespace()),
		Streams:  []string{stream, ">"},
		Count:    10,
	}).Result(); err != nil {
		t.Fatalf("read group entries: %v", err)
	}

	status := env.sample(t)

	if status.Direct.StreamLength != 2 || status.Direct.Unrelayed != 2 {
		t.Fatalf(
			"stream lengths = %d/%d, want 2/2",
			status.Direct.StreamLength, status.Direct.Unrelayed,
		)
	}
	if status.Direct.Pending != 2 {
		t.Fatalf("pending = %d, want 2", status.Direct.Pending)
	}
	if status.Direct.LastError != "" {
		t.Fatalf("last error = %q, want empty", status.Direct.LastError)
	}
}

func TestPipelineMonitorReportsGroupChain(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{
		GroupRelay:    func() RelayStatus { return RelayStatus{Connected: true, Relayed: 4} },
		GroupConsumer: func() ConsumerStatus { return ConsumerStatus{Persisted: 3} },
	})
	env.addEntry(t, defaultKeyspace.groupMessageStreamKey(), 0)
	env.addEntry(t, defaultKeyspace.groupMessageStreamKey(), 1000)

	status := env.sample(t)

	if status.Group == nil {
		t.Fatal("group chain = nil, want enabled")
	}
	if status.Group.StreamLength != 2 || status.Group.Unrelayed != 2 {
		t.Fatalf(
			"group stream lengths = %d/%d, want 2/2",
			status.Group.StreamLength, status.Group.Unrelayed,
		)
	}
	if !status.Group.Relay.Connected || status.Group.Relay.Relayed != 4 {
		t.Fatalf("group relay = %#v", status.Group.Relay)
	}
	if status.Group.Consumer.Persisted != 3 {
		t.Fatalf("group consumer = %#v", status.Group.Consumer)
	}
	if status.Direct.StreamLength != 0 {
		t.Fatalf("direct stream length = %d, want 0", status.Direct.StreamLength)
	}
}

func TestPipelineMonitorLogsBacklogFlipOnce(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{
		BacklogLimit: 2,
		OldestLimit:  time.Hour,
	})
	stream := defaultKeyspace.messageStreamKey()
	env.addEntry(t, stream, 0)
	env.addEntry(t, stream, 1000)
	if env.sample(t).Direct.BacklogAlert {
		t.Fatal("backlog alert = true below the limit")
	}
	if len(env.logs) != 0 {
		t.Fatalf("logs = %v, want none", env.logs)
	}

	env.addEntry(t, stream, 2000)
	if !env.sample(t).Direct.BacklogAlert {
		t.Fatal("backlog alert = false above the limit")
	}
	if len(env.logs) != 1 || !strings.Contains(env.logs[0], "stream backlog") {
		t.Fatalf("logs = %v, want one stream backlog alert", env.logs)
	}

	env.sample(t)
	if len(env.logs) != 1 {
		t.Fatalf("logs = %v, want the alert logged only once", env.logs)
	}

	if err := env.client.Del(context.Background(), stream).Err(); err != nil {
		t.Fatalf("delete stream: %v", err)
	}
	if env.sample(t).Direct.BacklogAlert {
		t.Fatal("backlog alert = true after the stream drained")
	}
	if len(env.logs) != 2 || !strings.Contains(env.logs[1], "stream backlog recovered") {
		t.Fatalf("logs = %v, want one recovery line", env.logs)
	}

	env.sample(t)
	if len(env.logs) != 2 {
		t.Fatalf("logs = %v, want the recovery logged only once", env.logs)
	}
}

func TestPipelineMonitorLogsDeadLetterIncrease(t *testing.T) {
	counter := &fakePipelineDeadLetterCounter{
		counts: map[DeadLetterKind]int64{DeadLetterDirect: 2},
	}
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{DeadLetters: counter})

	status := env.sample(t)
	if status.Direct.DeadLetterCount == nil || *status.Direct.DeadLetterCount != 2 {
		t.Fatalf("dead letter count = %v, want 2", status.Direct.DeadLetterCount)
	}
	if len(env.logs) != 0 {
		t.Fatalf("logs = %v, want no log for the baseline count", env.logs)
	}

	counter.counts[DeadLetterDirect] = 5
	status = env.sample(t)
	if status.Direct.DeadLetterCount == nil || *status.Direct.DeadLetterCount != 5 {
		t.Fatalf("dead letter count = %v, want 5", status.Direct.DeadLetterCount)
	}
	if len(env.logs) != 1 || !strings.Contains(env.logs[0], "dead letters increased") ||
		!strings.Contains(env.logs[0], "total=5") {
		t.Fatalf("logs = %v, want one increase log with total 5", env.logs)
	}

	env.sample(t)
	if len(env.logs) != 1 {
		t.Fatalf("logs = %v, want no log without an increase", env.logs)
	}

	counter.err = errors.New("broker unavailable")
	status = env.sample(t)
	if status.Direct.DeadLetterCount != nil {
		t.Fatalf("dead letter count = %d, want unknown", *status.Direct.DeadLetterCount)
	}
	if status.Direct.DeadLetterError != "broker unavailable" {
		t.Fatalf("dead letter error = %q", status.Direct.DeadLetterError)
	}
	encoded, err := json.Marshal(status.Direct)
	if err != nil {
		t.Fatalf("marshal chain status: %v", err)
	}
	if !strings.Contains(string(encoded), `"dead_letter_count":null`) {
		t.Fatalf("chain json = %s, want a null dead letter count", encoded)
	}

	counter.err = nil
	counter.counts[DeadLetterDirect] = 6
	if got := env.sample(t); got.Direct.DeadLetterCount == nil || *got.Direct.DeadLetterCount != 6 {
		t.Fatalf("dead letter count = %v, want 6 after recovery", got.Direct.DeadLetterCount)
	}
	if len(env.logs) != 2 || !strings.Contains(env.logs[1], "total=6") {
		t.Fatalf("logs = %v, want an increase log with total 6", env.logs)
	}
}

func TestPipelineMonitorSurvivesRedisFailure(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{
		BacklogLimit: 1,
		OldestLimit:  time.Hour,
	})
	stream := defaultKeyspace.messageStreamKey()
	env.addEntry(t, stream, 0)
	env.addEntry(t, stream, 1000)
	if !env.sample(t).Direct.BacklogAlert {
		t.Fatal("backlog alert = false before the outage")
	}

	env.server.Close()

	status := env.sample(t)
	if status.Direct.LastError == "" {
		t.Fatal("last error = empty, want the Redis failure")
	}
	if status.LastError == "" {
		t.Fatal("top level last error = empty, want the Redis failure")
	}
	if !status.Direct.BacklogAlert {
		t.Fatal("backlog alert = false during an outage, want the previous decision")
	}
	if len(env.logs) != 1 {
		t.Fatalf("logs = %v, want no fake recovery during an outage", env.logs)
	}
	if !status.Direct.Relay.Connected {
		t.Fatalf("relay status = %#v, want it sampled independently of Redis", status.Direct.Relay)
	}
	if status.Direct.DeadLetterCount != nil {
		t.Fatalf("dead letter count = %d, want unknown without a broker", *status.Direct.DeadLetterCount)
	}
}

func TestPipelineMonitorAlertsOnStreamLengthWhenOldestLegFails(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{
		BacklogLimit: 1000,
		OldestLimit:  time.Hour,
	})
	stream := defaultKeyspace.messageStreamKey()
	for index := int64(0); index < 5000; index++ {
		env.addEntry(t, stream, index)
	}
	env.client.AddHook(&pipelineMonitorCommandFailer{commands: map[string]bool{"xrange": true}})

	status := env.sample(t)

	if status.Direct.Unrelayed != 5000 {
		t.Fatalf("unrelayed = %d, want 5000 from a successful XLEN", status.Direct.Unrelayed)
	}
	if status.Direct.LastError == "" {
		t.Fatal("last error = empty, want the XRANGE failure")
	}
	if !status.Direct.BacklogAlert {
		t.Fatal("backlog alert = false, want true from the successful XLEN")
	}
	if len(env.logs) != 1 || !strings.Contains(env.logs[0], "stream backlog") {
		t.Fatalf("logs = %v, want one stream backlog alert", env.logs)
	}
}

func TestPipelineMonitorAlertsOnStreamLengthWhenPendingLegFails(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{
		BacklogLimit: 2,
		OldestLimit:  time.Hour,
	})
	stream := defaultKeyspace.messageStreamKey()
	for index := int64(0); index < 3; index++ {
		env.addEntry(t, stream, index*1000)
	}
	env.client.AddHook(&pipelineMonitorCommandFailer{commands: map[string]bool{"xpending": true}})

	status := env.sample(t)

	if !status.Direct.BacklogAlert {
		t.Fatal("backlog alert = false, want true with a failed XPENDING")
	}
	if len(env.logs) != 1 || !strings.Contains(env.logs[0], "stream backlog") {
		t.Fatalf("logs = %v, want one stream backlog alert", env.logs)
	}
}

func TestPipelineMonitorKeepsBacklogAlertWhenAllStreamLegsFail(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{
		BacklogLimit: 2,
		OldestLimit:  time.Hour,
	})
	stream := defaultKeyspace.messageStreamKey()
	for index := int64(0); index < 3; index++ {
		env.addEntry(t, stream, index*1000)
	}
	if !env.sample(t).Direct.BacklogAlert {
		t.Fatal("backlog alert = false before the command failures")
	}
	env.client.AddHook(&pipelineMonitorCommandFailer{commands: map[string]bool{
		"xlen": true, "xrange": true, "xpending": true,
	}})

	status := env.sample(t)

	if status.Direct.LastError == "" {
		t.Fatal("last error = empty, want the injected failures")
	}
	if !status.Direct.BacklogAlert {
		t.Fatal("backlog alert = false after every stream leg failed, want the previous decision")
	}
	if len(env.logs) != 1 {
		t.Fatalf("logs = %v, want no fake recovery line", env.logs)
	}
}

func TestPipelineMonitorReportsReconcileStatus(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{
		Reconcile: func() ReconcileStatus {
			return ReconcileStatus{Repaired: 2, TotalRepaired: 9, Unresolved: 1}
		},
	})

	status := env.sample(t)

	if status.Reconcile.Repaired != 2 || status.Reconcile.TotalRepaired != 9 ||
		status.Reconcile.Unresolved != 1 {
		t.Fatalf("reconcile status = %#v", status.Reconcile)
	}
}

func TestPipelineStatusJSONKeys(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{
		GroupRelay:    func() RelayStatus { return RelayStatus{} },
		GroupConsumer: func() ConsumerStatus { return ConsumerStatus{} },
	})
	status := env.sample(t)

	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal pipeline status: %v", err)
	}
	for _, key := range []string{
		`"direct"`, `"group"`, `"reconcile"`, `"sampled_at"`, `"samples"`,
		`"stream_length"`, `"pending"`, `"oldest_age_seconds"`, `"unrelayed"`,
		`"backlog_alert"`, `"dead_letter_count"`, `"relay"`, `"consumer"`,
		`"connected"`, `"persisted"`,
	} {
		if !strings.Contains(string(encoded), key) {
			t.Fatalf("pipeline json = %s, want key %s", encoded, key)
		}
	}
}

func TestPipelineMonitorRunStopsOnCancel(t *testing.T) {
	env := newPipelineMonitorTestEnv(t, PipelineMonitorOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		env.monitor.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
	if env.monitor.Status().Samples != 1 {
		t.Fatalf("samples = %d, want one sample before stopping", env.monitor.Status().Samples)
	}
}

type fakePipelineDeadLetterCounter struct {
	counts map[DeadLetterKind]int64
	err    error
	calls  int
}

func (f *fakePipelineDeadLetterCounter) CountDeadLetters(
	_ context.Context,
	kind DeadLetterKind,
) (int64, error) {
	f.calls++
	if f.err != nil {
		return 0, f.err
	}
	return f.counts[kind], nil
}

func TestPipelineMonitorAMQPDeadLetterCounterWithoutBroker(t *testing.T) {
	counter := monitorAMQPDeadLetterCounter{
		dialer: func(config.RabbitMQConfig) (*amqp091.Connection, error) {
			return nil, errors.New("dial refused")
		},
	}
	if _, err := counter.CountDeadLetters(context.Background(), DeadLetterDirect); err == nil {
		t.Fatal("CountDeadLetters error = nil, want the dial failure")
	}
}

func TestPipelineMonitorAMQPDeadLetterCounterSkipsDialOnCanceledContext(t *testing.T) {
	dials := 0
	counter := &monitorAMQPDeadLetterCounter{
		open: func(context.Context) (monitorAMQPProber, error) {
			dials++
			return &fakeMonitorAMQPProber{}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := counter.CountDeadLetters(ctx, DeadLetterDirect); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if dials != 0 {
		t.Fatalf("dial calls = %d, want none", dials)
	}
}

func TestPipelineMonitorAMQPDeadLetterCounterReusesConnection(t *testing.T) {
	prober := &fakeMonitorAMQPProber{depth: 7}
	dials := 0
	counter := &monitorAMQPDeadLetterCounter{
		open: func(context.Context) (monitorAMQPProber, error) {
			dials++
			return prober, nil
		},
	}

	for index := 0; index < 2; index++ {
		depth, err := counter.CountDeadLetters(context.Background(), DeadLetterDirect)
		if err != nil || depth != 7 {
			t.Fatalf("sample %d: depth = %d, err = %v, want 7", index, depth, err)
		}
	}
	if dials != 1 {
		t.Fatalf("dial calls = %d, want one cached connection", dials)
	}
	if prober.closed != 0 {
		t.Fatalf("cached connection closed %d times, want 0", prober.closed)
	}

	counter.Close()
	if prober.closed != 1 {
		t.Fatalf("cached connection closed %d times after Close, want 1", prober.closed)
	}
}

func TestPipelineMonitorAMQPDeadLetterCounterBoundsProbe(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	prober := &fakeMonitorAMQPProber{depth: 3, block: release}
	counter := &monitorAMQPDeadLetterCounter{
		open: func(context.Context) (monitorAMQPProber, error) { return prober, nil },
		// An already-canceled probe context exercises the deadline path
		// without sleeping.
		probeContext: func(context.Context) (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, cancel
		},
	}

	if _, err := counter.CountDeadLetters(context.Background(), DeadLetterDirect); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want the probe timeout", err)
	}
	if prober.closed != 1 {
		t.Fatalf("connection closed %d times after a probe timeout, want 1", prober.closed)
	}
}

func TestPipelineMonitorAMQPDeadLetterCounterReopensAfterProbeError(t *testing.T) {
	failing := &fakeMonitorAMQPProber{err: errors.New("queue missing")}
	healthy := &fakeMonitorAMQPProber{depth: 4}
	probers := []monitorAMQPProber{failing, healthy}
	dials := 0
	counter := &monitorAMQPDeadLetterCounter{
		open: func(context.Context) (monitorAMQPProber, error) {
			prober := probers[dials]
			dials++
			return prober, nil
		},
	}

	if _, err := counter.CountDeadLetters(context.Background(), DeadLetterDirect); err == nil {
		t.Fatal("error = nil, want the probe failure")
	}
	if failing.closed != 1 {
		t.Fatalf("failed connection closed %d times, want 1", failing.closed)
	}

	depth, err := counter.CountDeadLetters(context.Background(), DeadLetterDirect)
	if err != nil || depth != 4 {
		t.Fatalf("depth = %d, err = %v, want 4 after reopening", depth, err)
	}
	if dials != 2 {
		t.Fatalf("dial calls = %d, want a reopen after the probe error", dials)
	}
}

// fakeMonitorAMQPConnection stands in for the dial result while the dial and
// channel-open legs are held open by the test.
type fakeMonitorAMQPConnection struct {
	mu          sync.Mutex
	closeCalls  int
	release     chan struct{}
	closedOnce  sync.Once
	channelSeen chan struct{}
	closedSeen  chan struct{}
}

func newFakeMonitorAMQPConnection() *fakeMonitorAMQPConnection {
	return &fakeMonitorAMQPConnection{
		release:     make(chan struct{}),
		channelSeen: make(chan struct{}, 1),
		closedSeen:  make(chan struct{}, 1),
	}
}

func (f *fakeMonitorAMQPConnection) Channel() (*amqp091.Channel, error) {
	select {
	case f.channelSeen <- struct{}{}:
	default:
	}
	<-f.release
	return nil, errors.New("connection closed")
}

func (f *fakeMonitorAMQPConnection) CloseDeadline(time.Time) error {
	f.mu.Lock()
	f.closeCalls++
	f.mu.Unlock()
	f.closedOnce.Do(func() { close(f.release) })
	select {
	case f.closedSeen <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeMonitorAMQPConnection) closes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeCalls
}

func TestMonitorOpenAMQPLinkSkipsDialOnCanceledContext(t *testing.T) {
	dials := 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	link, err := monitorOpenAMQPLink(
		ctx,
		func(config.RabbitMQConfig) (*amqp091.Connection, error) {
			dials++
			return nil, errors.New("must not dial")
		},
		config.RabbitMQConfig{},
	)
	if link != nil {
		t.Fatalf("link = %#v, want nil", link)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if dials != 0 {
		t.Fatalf("dial calls = %d, want none", dials)
	}
}

func TestMonitorOpenChannelBoundsStuckChannelOpen(t *testing.T) {
	connection := newFakeMonitorAMQPConnection()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-connection.channelSeen
		cancel()
	}()

	type openResult struct {
		channel *amqp091.Channel
		err     error
	}
	done := make(chan openResult, 1)
	go func() {
		channel, err := monitorOpenChannel(ctx, connection)
		done <- openResult{channel: channel, err: err}
	}()
	select {
	case result := <-done:
		if result.channel != nil {
			t.Fatalf("channel = %v, want nil", result.channel)
		}
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("error = %v, want context canceled", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("monitorOpenChannel did not honor cancellation")
	}
	if connection.closes() != 1 {
		t.Fatalf("connection closed %d times, want 1", connection.closes())
	}
}

func TestMonitorOpenAMQPLinkBoundsStuckDial(t *testing.T) {
	connection := newFakeMonitorAMQPConnection()
	dialStarted := make(chan struct{})
	dialRelease := make(chan struct{})
	connect := func() (monitorAMQPConnector, error) {
		close(dialStarted)
		<-dialRelease
		return connection, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-dialStarted
		cancel()
	}()

	type linkResult struct {
		link *monitorAMQPLink
		err  error
	}
	done := make(chan linkResult, 1)
	go func() {
		link, err := monitorOpenAMQPLinkContext(ctx, connect)
		done <- linkResult{link: link, err: err}
	}()
	select {
	case result := <-done:
		if result.link != nil {
			t.Fatalf("link = %#v, want nil", result.link)
		}
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("error = %v, want context canceled", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("monitorOpenAMQPLinkContext did not honor cancellation")
	}

	close(dialRelease)
	select {
	case <-connection.closedSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("a connection returned after cancellation was not closed")
	}
}

func TestPipelineMonitorAMQPDeadLetterCounterCloseDoesNotWaitForOpen(t *testing.T) {
	openStarted := make(chan struct{})
	openRelease := make(chan struct{})
	opened := &fakeMonitorAMQPProber{depth: 9}
	counter := &monitorAMQPDeadLetterCounter{
		open: func(context.Context) (monitorAMQPProber, error) {
			close(openStarted)
			<-openRelease
			return opened, nil
		},
	}

	results := make(chan error, 1)
	go func() {
		_, err := counter.CountDeadLetters(context.Background(), DeadLetterDirect)
		results <- err
	}()
	<-openStarted

	closed := make(chan struct{})
	go func() {
		counter.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked behind an in-flight open")
	}

	close(openRelease)
	select {
	case err := <-results:
		if !errors.Is(err, errMonitorDeadLetterClosed) {
			t.Fatalf("error = %v, want the closed sentinel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CountDeadLetters did not return after the open completed")
	}
	if opened.closed != 1 {
		t.Fatalf("late prober closed %d times, want 1", opened.closed)
	}
}

// pipelineMonitorCommandFailer injects Redis command failures while every
// other command still reaches miniredis.
type pipelineMonitorCommandFailer struct {
	commands map[string]bool
}

func (f *pipelineMonitorCommandFailer) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (f *pipelineMonitorCommandFailer) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if f.commands[cmd.Name()] {
			return fmt.Errorf("injected %s failure", cmd.Name())
		}
		return next(ctx, cmd)
	}
}

func (f *pipelineMonitorCommandFailer) ProcessPipelineHook(
	next redis.ProcessPipelineHook,
) redis.ProcessPipelineHook {
	return next
}

// fakeMonitorAMQPProber stands in for an AMQP connection and channel.
type fakeMonitorAMQPProber struct {
	depth  int64
	err    error
	block  <-chan struct{}
	closed int
}

func (f *fakeMonitorAMQPProber) QueueDepth(string) (int64, error) {
	if f.block != nil {
		<-f.block
	}
	return f.depth, f.err
}

func (f *fakeMonitorAMQPProber) Close() error {
	f.closed++
	return nil
}
