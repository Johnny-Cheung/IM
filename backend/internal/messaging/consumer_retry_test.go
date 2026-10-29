package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// consumeFakeDelivery records acknowledgements so tests can assert that a
// delivery is never nacked and is acked at most once.
type consumeFakeDelivery struct {
	body      []byte
	messageID string
	acks      int
	nacks     int
	ackErr    error
}

func (d *consumeFakeDelivery) Body() []byte          { return d.body }
func (d *consumeFakeDelivery) MessageID() string     { return d.messageID }
func (d *consumeFakeDelivery) Ack(bool) error        { d.acks++; return d.ackErr }
func (d *consumeFakeDelivery) Nack(bool, bool) error { d.nacks++; return nil }

// consumeFakePublisher records dead letters and can fail a fixed number of
// publishes first.
type consumeFakePublisher struct {
	published    []dlqMessage
	publishCalls int
	failures     int
	err          error
	onPublish    func()
}

func (p *consumeFakePublisher) PublishDeadLetter(ctx context.Context, message dlqMessage) error {
	p.publishCalls++
	if p.onPublish != nil {
		p.onPublish()
	}
	if p.failures > 0 {
		p.failures--
		if p.err != nil {
			return p.err
		}
		return errors.New("dead letter publish failed")
	}
	p.published = append(p.published, message)
	return nil
}

// consumeFakeClock records waits and can cancel the context during one of them.
type consumeFakeClock struct {
	sleeps   []time.Duration
	onSleep  func(index int)
	cancelAt int
	cancel   context.CancelFunc
}

func (c *consumeFakeClock) sleep(ctx context.Context, delay time.Duration) bool {
	c.sleeps = append(c.sleeps, delay)
	if c.onSleep != nil {
		c.onSleep(len(c.sleeps))
	}
	if c.cancelAt > 0 && len(c.sleeps) == c.cancelAt {
		c.cancel()
		return false
	}
	return ctx.Err() == nil
}

type consumeFakeMessagePersister struct {
	calls int
	fail  func(call int) error
}

func (p *consumeFakeMessagePersister) PersistMessage(context.Context, store.Message) (bool, error) {
	p.calls++
	if p.fail != nil {
		if err := p.fail(p.calls); err != nil {
			return false, err
		}
	}
	return true, nil
}

// consumeMessagePersisterFunc lets a test observe consumer state from inside a
// persist call.
type consumeMessagePersisterFunc func(context.Context, store.Message) (bool, error)

func (f consumeMessagePersisterFunc) PersistMessage(
	ctx context.Context, message store.Message,
) (bool, error) {
	return f(ctx, message)
}

// consumeGroupPersisterFunc is the group-chain equivalent.
type consumeGroupPersisterFunc func(context.Context, store.GroupMessage) (bool, error)

func (f consumeGroupPersisterFunc) PersistGroupMessage(
	ctx context.Context, message store.GroupMessage,
) (bool, error) {
	return f(ctx, message)
}

func TestConsumeStatusTracksInFlight(t *testing.T) {
	var status consumeStatusState
	if inFlight := status.snapshot().InFlight; inFlight != 0 {
		t.Fatalf("initial in flight = %d, want 0", inFlight)
	}
	status.beginDelivery()
	status.beginDelivery()
	if inFlight := status.snapshot().InFlight; inFlight != 2 {
		t.Fatalf("in flight = %d, want 2", inFlight)
	}
	status.endDelivery()
	if inFlight := status.snapshot().InFlight; inFlight != 1 {
		t.Fatalf("in flight = %d, want 1", inFlight)
	}
	status.endDelivery()
	status.endDelivery()
	if inFlight := status.snapshot().InFlight; inFlight != 0 {
		t.Fatalf("in flight = %d, want 0 and never negative", inFlight)
	}
}

type consumeFakeGroupPersister struct {
	calls int
	fail  func(call int) error
}

func (p *consumeFakeGroupPersister) PersistGroupMessage(context.Context, store.GroupMessage) (bool, error) {
	p.calls++
	if p.fail != nil {
		if err := p.fail(p.calls); err != nil {
			return false, err
		}
	}
	return true, nil
}

func consumeAlwaysFail(err error) func(int) error {
	return func(int) error { return err }
}

func consumeTestRetryPolicy(clock *consumeFakeClock, at time.Time) consumeRetryPolicy {
	policy := defaultConsumeRetryPolicy()
	policy.sleep = clock.sleep
	policy.now = func() time.Time { return at }
	return policy
}

func consumeTestDirectBody(t *testing.T) []byte {
	t.Helper()
	payload, err := encodeDirectMessage(store.Message{
		ID:              42,
		Seq:             7,
		SenderID:        1,
		RecipientID:     2,
		ClientMessageID: "client-direct-1",
		Body:            "hello",
		CreatedAt:       time.UnixMilli(1700000000123).UTC(),
	})
	if err != nil {
		t.Fatalf("encodeDirectMessage() error = %v", err)
	}
	return payload
}

func consumeTestGroupBody(t *testing.T) []byte {
	t.Helper()
	payload, err := encodeGroupMessage(store.GroupMessage{
		ID:              7,
		Seq:             3,
		GroupID:         9,
		SenderID:        1,
		ClientMessageID: "client-group-1",
		Body:            "hi",
		CreatedAt:       time.UnixMilli(1700000000123).UTC(),
	})
	if err != nil {
		t.Fatalf("encodeGroupMessage() error = %v", err)
	}
	return payload
}

// consumeAssertDeadLetterHeaders checks the header contract shared by both
// chains.
func consumeAssertDeadLetterHeaders(t *testing.T, dead dlqMessage) {
	t.Helper()
	headers := dead.headers()
	if len(headers) != 5 {
		t.Fatalf("headers = %#v, want five entries", headers)
	}
	if headers[dlqHeaderReason] != dead.Reason {
		t.Fatalf("reason header = %v, want %v", headers[dlqHeaderReason], dead.Reason)
	}
	if headers[dlqHeaderAttempts] != dead.Attempts {
		t.Fatalf("attempts header = %v, want %d", headers[dlqHeaderAttempts], dead.Attempts)
	}
	if headers[dlqHeaderSource] != string(dead.Kind) {
		t.Fatalf("source header = %v, want %v", headers[dlqHeaderSource], dead.Kind)
	}
	firstFailedAt, ok := headers[dlqHeaderFirstFailedAt].(string)
	if !ok {
		t.Fatalf("first failed at header = %#v, want string", headers[dlqHeaderFirstFailedAt])
	}
	if _, err := time.Parse(time.RFC3339, firstFailedAt); err != nil {
		t.Fatalf("first failed at header = %q: %v", firstFailedAt, err)
	}
	if text, ok := headers[dlqHeaderError].(string); !ok || len(text) > dlqErrorLimit {
		t.Fatalf("error header = %#v", headers[dlqHeaderError])
	}
}

func TestConsumeRetryPolicyTransientDelay(t *testing.T) {
	policy := defaultConsumeRetryPolicy()
	want := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}
	for retry, delay := range want {
		if got := policy.transientDelay(uint64(retry)); got != delay {
			t.Fatalf("transientDelay(%d) = %v, want %v", retry, got, delay)
		}
	}
	if got := policy.transientDelay(1000); got != 30*time.Second {
		t.Fatalf("transientDelay(1000) = %v, want 30s", got)
	}
}

func TestConsumeRetryPolicyWithDefaults(t *testing.T) {
	policy := consumeRetryPolicy{}.withDefaults()
	if len(policy.messageDelays) != 5 {
		t.Fatalf("messageDelays = %v, want five delays", policy.messageDelays)
	}
	if policy.transientBase != time.Second || policy.transientMax != 30*time.Second {
		t.Fatalf("transient delays = %v/%v, want 1s/30s", policy.transientBase, policy.transientMax)
	}
	if policy.sleep == nil || policy.now == nil {
		t.Fatal("withDefaults() left sleep or now unset")
	}
}

func TestConsumeStatusStateIsSafeForConcurrentUse(t *testing.T) {
	var status consumeStatusState
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				status.recordPersisted(time.Now())
				status.recordError(errors.New("persist failed"), time.Now())
				status.recordTransientRetry()
				status.recordMessageRetry()
				status.recordDeadLettered()
				status.setRetrying(true)
				_ = status.snapshot()
				status.setRetrying(false)
			}
		}()
	}
	wg.Wait()
	got := status.snapshot()
	if got.Persisted != 800 || got.TransientRetries != 800 || got.MessageRetries != 800 || got.DeadLettered != 800 {
		t.Fatalf("snapshot = %#v", got)
	}
	if got.LastError != "persist failed" || got.Retrying {
		t.Fatalf("snapshot = %#v", got)
	}
}

func TestConsumerStatusJSONKeys(t *testing.T) {
	status := ConsumerStatus{
		LastError:        "boom",
		LastErrorAt:      time.Unix(1700000000, 0).UTC(),
		LastPersistedAt:  time.Unix(1700000001, 0).UTC(),
		Persisted:        1,
		TransientRetries: 2,
		MessageRetries:   3,
		DeadLettered:     4,
		Retrying:         true,
	}
	data, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, key := range []string{
		`"last_error"`,
		`"last_error_at"`,
		`"last_persisted_at"`,
		`"persisted"`,
		`"transient_retries"`,
		`"message_retries"`,
		`"dead_lettered"`,
		`"retrying"`,
	} {
		if !strings.Contains(string(data), key) {
			t.Fatalf("status JSON %s is missing %s", data, key)
		}
	}
}
