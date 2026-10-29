package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
)

type relayTestEnv struct {
	client *redis.Client
	server *miniredis.Miniredis
}

func newRelayTestEnv(t *testing.T) relayTestEnv {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return relayTestEnv{client: client, server: server}
}

func newDirectRelayTest(env relayTestEnv) *StreamRelay {
	relay := NewStreamRelayWithNamespace(
		env.client,
		config.RabbitMQConfig{},
		nil,
		defaultKeyspace,
		DefaultNamespace(),
	)
	relay.block = 10 * time.Millisecond
	return relay
}

func relayAddDirectEntry(t *testing.T, client *redis.Client, messageID, seq uint64) {
	t.Helper()
	_, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: messageStreamKey(),
		Values: map[string]interface{}{
			"id":                strconv.FormatUint(messageID, 10),
			"seq":               strconv.FormatUint(seq, 10),
			"sender_id":         "1",
			"recipient_id":      "2",
			"client_message_id": fmt.Sprintf("client-%d", messageID),
			"body":              "hello",
			"created_at_ms":     "1700000000000",
			"body_hash":         "hash",
		},
	}).Result()
	if err != nil {
		t.Fatalf("add direct stream entry: %v", err)
	}
}

type relayPublishCall struct {
	exchange   string
	routingKey string
	messageID  string
	payload    []byte
}

type fakeRelayPublisher struct {
	mu       sync.Mutex
	calls    []relayPublishCall
	errs     []error
	notified chan struct{}
}

func newFakeRelayPublisher() *fakeRelayPublisher {
	return &fakeRelayPublisher{notified: make(chan struct{}, 64)}
}

// failWith queues one publish failure for the next Publish call.
func (f *fakeRelayPublisher) failWith(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, err)
}

func (f *fakeRelayPublisher) Publish(
	_ context.Context,
	exchange, routingKey, messageID string,
	payload []byte,
) error {
	f.mu.Lock()
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		f.mu.Unlock()
		return err
	}
	f.calls = append(f.calls, relayPublishCall{
		exchange:   exchange,
		routingKey: routingKey,
		messageID:  messageID,
		payload:    append([]byte(nil), payload...),
	})
	f.mu.Unlock()
	select {
	case f.notified <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeRelayPublisher) snapshot() []relayPublishCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]relayPublishCall, len(f.calls))
	copy(result, f.calls)
	return result
}

// blockingRelayPublisher waits for the context like a publisher awaiting a
// broker confirm that never arrives.
type blockingRelayPublisher struct {
	started chan struct{}
}

func newBlockingRelayPublisher() *blockingRelayPublisher {
	return &blockingRelayPublisher{started: make(chan struct{}, 1)}
}

func (p *blockingRelayPublisher) Publish(
	ctx context.Context,
	_, _, _ string,
	_ []byte,
) error {
	select {
	case p.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

// confirmRelayPublisher drives the real confirm handling over a test channel.
type confirmRelayPublisher struct {
	confirms chan amqp091.Confirmation
}

func (p *confirmRelayPublisher) Publish(
	ctx context.Context,
	_, _, messageID string,
	_ []byte,
) error {
	return relayAwaitConfirm(ctx, p.confirms, messageID)
}

// runRelayUntil runs the relay loop until it published want entries and the
// stream drained, then cancels the context and waits for the loop to stop.
func runRelayUntil(
	t *testing.T,
	run func(context.Context, relayPublisher) error,
	publisher relayPublisher,
	notified <-chan struct{},
	want int,
	drained func() bool,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, publisher) }()
	for index := 0; index < want; index++ {
		select {
		case <-notified:
		case err := <-done:
			cancel()
			t.Fatalf("relay stopped after %d of %d publishes: %v", index, want, err)
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatalf("relay published %d of %d entries", index, want)
		}
	}
	deadline := time.After(5 * time.Second)
	for !drained() {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("stream not drained after %d publishes", want)
		case err := <-done:
			cancel()
			t.Fatalf("relay stopped before draining the stream: %v", err)
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not stop after cancel")
	}
}

func relayStreamDrained(client *redis.Client, stream string) func() bool {
	return func() bool {
		return client.XLen(context.Background(), stream).Val() == 0
	}
}

func relayPendingCount(t *testing.T, client *redis.Client, stream, group string) int64 {
	t.Helper()
	pending, err := client.XPending(context.Background(), stream, group).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	return pending.Count
}

func TestStreamRelayRecoversPendingEntriesBeforeNewOnes(t *testing.T) {
	env := newRelayTestEnv(t)
	ctx := context.Background()
	if err := ensureRelayGroup(ctx, env.client, defaultKeyspace, DefaultNamespace()); err != nil {
		t.Fatalf("ensure relay group: %v", err)
	}
	relayAddDirectEntry(t, env.client, 101, 1)
	relayAddDirectEntry(t, env.client, 102, 2)
	// A previous relay run read both entries and stopped before acknowledging.
	if _, err := env.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    directRelayGroup(DefaultNamespace()),
		Consumer: directConsumerName(DefaultNamespace()),
		Streams:  []string{messageStreamKey(), ">"},
		Count:    10,
	}).Result(); err != nil {
		t.Fatalf("seed pending entries: %v", err)
	}
	relayAddDirectEntry(t, env.client, 103, 3)

	relay := newDirectRelayTest(env)
	publisher := newFakeRelayPublisher()
	runRelayUntil(t, relay.runPublisher, publisher, publisher.notified, 3, relayStreamDrained(env.client, messageStreamKey()))

	calls := publisher.snapshot()
	wantIDs := []string{"101", "102", "103"}
	if len(calls) != len(wantIDs) {
		t.Fatalf("publishes = %d, want %d", len(calls), len(wantIDs))
	}
	for index, wantID := range wantIDs {
		if calls[index].messageID != wantID {
			t.Fatalf("publish %d message ID = %q, want %q", index, calls[index].messageID, wantID)
		}
		if calls[index].exchange != directExchangeName(DefaultNamespace()) ||
			calls[index].routingKey != directRoutingKey {
			t.Fatalf("publish %d destination = %q/%q", index, calls[index].exchange, calls[index].routingKey)
		}
	}
	if got := env.client.XLen(ctx, messageStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
	if got := relayPendingCount(t, env.client, messageStreamKey(), directRelayGroup(DefaultNamespace())); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	status := relay.Status()
	if status.Relayed != 3 || status.Recovered != 2 {
		t.Fatalf("status relayed/recovered = %d/%d, want 3/2", status.Relayed, status.Recovered)
	}
}

func TestStreamRelayKeepsEntryPendingOnPublishFailure(t *testing.T) {
	env := newRelayTestEnv(t)
	ctx := context.Background()
	if err := ensureRelayGroup(ctx, env.client, defaultKeyspace, DefaultNamespace()); err != nil {
		t.Fatalf("ensure relay group: %v", err)
	}
	relayAddDirectEntry(t, env.client, 201, 1)

	relay := newDirectRelayTest(env)
	publisher := newFakeRelayPublisher()
	publisher.failWith(errors.New("broker unavailable"))
	if err := relay.runPublisher(ctx, publisher); err == nil {
		t.Fatal("runPublisher() error = nil, want publish failure")
	}
	if got := env.client.XLen(ctx, messageStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after failure = %d, want 1", got)
	}
	if got := relayPendingCount(t, env.client, messageStreamKey(), directRelayGroup(DefaultNamespace())); got != 1 {
		t.Fatalf("pending after failure = %d, want 1", got)
	}
	if got := len(publisher.snapshot()); got != 0 {
		t.Fatalf("publishes after failure = %d, want 0", got)
	}

	// The next connection republishes the pending entry.
	next := newFakeRelayPublisher()
	runRelayUntil(t, relay.runPublisher, next, next.notified, 1, relayStreamDrained(env.client, messageStreamKey()))
	calls := next.snapshot()
	if len(calls) != 1 || calls[0].messageID != "201" {
		t.Fatalf("republished = %#v, want message 201", calls)
	}
	if got := env.client.XLen(ctx, messageStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length after recovery = %d, want 0", got)
	}
	if got := relay.Status().Recovered; got != 1 {
		t.Fatalf("recovered = %d, want 1", got)
	}
}

func TestStreamRelayKeepsEntryPendingOnNack(t *testing.T) {
	env := newRelayTestEnv(t)
	ctx := context.Background()
	if err := ensureRelayGroup(ctx, env.client, defaultKeyspace, DefaultNamespace()); err != nil {
		t.Fatalf("ensure relay group: %v", err)
	}
	relayAddDirectEntry(t, env.client, 202, 1)

	confirms := make(chan amqp091.Confirmation, 1)
	confirms <- amqp091.Confirmation{DeliveryTag: 1, Ack: false}
	relay := newDirectRelayTest(env)
	err := relay.runPublisher(ctx, &confirmRelayPublisher{confirms: confirms})
	if err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("runPublisher() error = %v, want rejection", err)
	}
	if got := env.client.XLen(ctx, messageStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after nack = %d, want 1", got)
	}
	if got := relayPendingCount(t, env.client, messageStreamKey(), directRelayGroup(DefaultNamespace())); got != 1 {
		t.Fatalf("pending after nack = %d, want 1", got)
	}
}

func TestStreamRelayKeepsEntryPendingOnClosedConfirmChannel(t *testing.T) {
	env := newRelayTestEnv(t)
	ctx := context.Background()
	if err := ensureRelayGroup(ctx, env.client, defaultKeyspace, DefaultNamespace()); err != nil {
		t.Fatalf("ensure relay group: %v", err)
	}
	relayAddDirectEntry(t, env.client, 203, 1)

	confirms := make(chan amqp091.Confirmation)
	close(confirms)
	relay := newDirectRelayTest(env)
	err := relay.runPublisher(ctx, &confirmRelayPublisher{confirms: confirms})
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("runPublisher() error = %v, want closed confirm channel", err)
	}
	if got := env.client.XLen(ctx, messageStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after closed confirms = %d, want 1", got)
	}
	if got := relayPendingCount(t, env.client, messageStreamKey(), directRelayGroup(DefaultNamespace())); got != 1 {
		t.Fatalf("pending after closed confirms = %d, want 1", got)
	}
}

func TestStreamRelayKeepsEntryPendingWhenShutdownAwaitsConfirm(t *testing.T) {
	env := newRelayTestEnv(t)
	if err := ensureRelayGroup(context.Background(), env.client, defaultKeyspace, DefaultNamespace()); err != nil {
		t.Fatalf("ensure relay group: %v", err)
	}
	relayAddDirectEntry(t, env.client, 204, 1)

	relay := newDirectRelayTest(env)
	publisher := newBlockingRelayPublisher()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.runPublisher(ctx, publisher) }()
	select {
	case <-publisher.started:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not publish")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not stop after cancel")
	}
	if got := env.client.XLen(context.Background(), messageStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after shutdown = %d, want 1", got)
	}
	if got := relayPendingCount(t, env.client, messageStreamKey(), directRelayGroup(DefaultNamespace())); got != 1 {
		t.Fatalf("pending after shutdown = %d, want 1", got)
	}
	if got := relay.Status().Relayed; got != 0 {
		t.Fatalf("relayed after shutdown = %d, want 0", got)
	}
}

func TestStreamRelayDropsMalformedPendingEntry(t *testing.T) {
	env := newRelayTestEnv(t)
	ctx := context.Background()
	if err := ensureRelayGroup(ctx, env.client, defaultKeyspace, DefaultNamespace()); err != nil {
		t.Fatalf("ensure relay group: %v", err)
	}
	if _, err := env.client.XAdd(ctx, &redis.XAddArgs{
		Stream: messageStreamKey(),
		Values: map[string]interface{}{"id": "301"},
	}).Result(); err != nil {
		t.Fatalf("add malformed entry: %v", err)
	}
	if _, err := env.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    directRelayGroup(DefaultNamespace()),
		Consumer: directConsumerName(DefaultNamespace()),
		Streams:  []string{messageStreamKey(), ">"},
		Count:    10,
	}).Result(); err != nil {
		t.Fatalf("seed malformed pending entry: %v", err)
	}

	relay := newDirectRelayTest(env)
	publisher := newFakeRelayPublisher()
	if err := relay.drainPending(ctx, publisher); err != nil {
		t.Fatalf("drainPending() error = %v", err)
	}
	if got := len(publisher.snapshot()); got != 0 {
		t.Fatalf("malformed entry published %d times, want 0", got)
	}
	if got := env.client.XLen(ctx, messageStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
	if got := relayPendingCount(t, env.client, messageStreamKey(), directRelayGroup(DefaultNamespace())); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
}

func TestAwaitRelayConfirmClassifiesFailures(t *testing.T) {
	acknowledged := make(chan amqp091.Confirmation, 1)
	acknowledged <- amqp091.Confirmation{Ack: true}
	if err := relayAwaitConfirm(context.Background(), acknowledged, "1"); err != nil {
		t.Fatalf("ack error = %v", err)
	}

	nacked := make(chan amqp091.Confirmation, 1)
	nacked <- amqp091.Confirmation{Ack: false}
	if err := relayAwaitConfirm(context.Background(), nacked, "2"); err == nil {
		t.Fatal("nack error = nil, want failure")
	}

	closed := make(chan amqp091.Confirmation)
	close(closed)
	if err := relayAwaitConfirm(context.Background(), closed, "3"); err == nil {
		t.Fatal("closed channel error = nil, want failure")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := relayAwaitConfirm(ctx, make(chan amqp091.Confirmation), "4"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}
}

func TestStreamRelayRunBacksOffAndResetsAfterProgress(t *testing.T) {
	env := newRelayTestEnv(t)
	relay := newDirectRelayTest(env)
	relay.dialer = func(config.RabbitMQConfig) (*amqp091.Connection, error) {
		return nil, errors.New("unused")
	}
	attempts := 0
	relay.connect = func(context.Context) error {
		attempts++
		if attempts == 3 {
			// A connection that relayed an entry resets the reconnect delay.
			relay.status.recordRelayed(false)
			return errors.New("connection lost")
		}
		return errors.New("dial failed")
	}
	var delays []time.Duration
	ctx, cancel := context.WithCancel(context.Background())
	relay.wait = func(_ context.Context, delay time.Duration) bool {
		delays = append(delays, delay)
		if len(delays) == 5 {
			cancel()
			return false
		}
		return true
	}
	if err := relay.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := []time.Duration{
		time.Second,
		2 * time.Second,
		time.Second,
		2 * time.Second,
		4 * time.Second,
	}
	if len(delays) != len(want) {
		t.Fatalf("delays = %v, want %v", delays, want)
	}
	for index, delay := range want {
		if delays[index] != delay {
			t.Fatalf("delays = %v, want %v", delays, want)
		}
	}
	status := relay.Status()
	if status.LastError != "dial failed" || status.LastErrorAt.IsZero() {
		t.Fatalf("status error = %q at %v", status.LastError, status.LastErrorAt)
	}
	if status.Relayed != 1 {
		t.Fatalf("relayed = %d, want 1", status.Relayed)
	}
}

func TestRelayBackoffDoublesToCapAndResets(t *testing.T) {
	backoff := newRelayBackoff(time.Second, 30*time.Second)
	want := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}
	for index, delay := range want {
		if got := backoff.next(); got != delay {
			t.Fatalf("next %d = %v, want %v", index, got, delay)
		}
	}
	backoff.reset()
	if got := backoff.next(); got != time.Second {
		t.Fatalf("next after reset = %v, want 1s", got)
	}
}
