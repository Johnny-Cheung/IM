package messaging

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/rabbitmq/amqp091-go"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func TestGroupConsumerDeadLettersMessageClassAfterSixAttempts(t *testing.T) {
	body := consumeTestGroupBody(t)
	delivery := &consumeFakeDelivery{body: body, messageID: "7"}
	persister := &consumeFakeGroupPersister{fail: consumeAlwaysFail(&mysql.MySQLError{Number: 1452, Message: "foreign key"})}
	publisher := &consumeFakePublisher{}
	clock := &consumeFakeClock{}
	consumer := &GroupMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

	if err := consumer.handleDelivery(context.Background(), delivery, publisher); err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if persister.calls != 6 {
		t.Fatalf("persist calls = %d, want 6", persister.calls)
	}
	wantSleeps := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	if !reflect.DeepEqual(clock.sleeps, wantSleeps) {
		t.Fatalf("sleeps = %v, want %v", clock.sleeps, wantSleeps)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(publisher.published))
	}
	dead := publisher.published[0]
	if dead.Kind != DeadLetterGroup || dead.Reason != dlqReasonRetriesExhausted || dead.Attempts != 6 {
		t.Fatalf("dead letter = %#v", dead)
	}
	if string(dead.Body) != string(body) || dead.MessageID != "7" {
		t.Fatalf("dead letter body/message id changed: %#v", dead)
	}
	if dead.headers()[dlqHeaderSource] != "group" {
		t.Fatalf("source header = %v", dead.headers()[dlqHeaderSource])
	}
	if delivery.acks != 1 || delivery.nacks != 0 {
		t.Fatalf("acks/nacks = %d/%d, want 1/0", delivery.acks, delivery.nacks)
	}
	status := consumer.Status()
	if status.DeadLettered != 1 || status.MessageRetries != 5 || status.Persisted != 0 {
		t.Fatalf("status = %#v", status)
	}
}

func TestGroupConsumerTransientErrorThenSuccess(t *testing.T) {
	delivery := &consumeFakeDelivery{body: consumeTestGroupBody(t), messageID: "7"}
	persister := &consumeFakeGroupPersister{fail: func(call int) error {
		if call == 1 {
			return &net.DNSError{Err: "i/o timeout", Name: "mysql", IsTimeout: true}
		}
		return nil
	}}
	publisher := &consumeFakePublisher{}
	clock := &consumeFakeClock{}
	consumer := &GroupMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

	if err := consumer.handleDelivery(context.Background(), delivery, publisher); err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if persister.calls != 2 {
		t.Fatalf("persist calls = %d, want 2", persister.calls)
	}
	if want := []time.Duration{time.Second}; !reflect.DeepEqual(clock.sleeps, want) {
		t.Fatalf("sleeps = %v, want %v", clock.sleeps, want)
	}
	if len(publisher.published) != 0 || delivery.acks != 1 || delivery.nacks != 0 {
		t.Fatalf("published/acks/nacks = %d/%d/%d, want 0/1/0", len(publisher.published), delivery.acks, delivery.nacks)
	}
	if consumer.Status().TransientRetries != 1 {
		t.Fatalf("status = %#v", consumer.Status())
	}
}

func TestGroupConsumerMalformedBodyDeadLettersImmediately(t *testing.T) {
	body := []byte("not json")
	delivery := &consumeFakeDelivery{body: body, messageID: "7"}
	persister := &consumeFakeGroupPersister{}
	publisher := &consumeFakePublisher{}
	clock := &consumeFakeClock{}
	consumer := &GroupMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

	if err := consumer.handleDelivery(context.Background(), delivery, publisher); err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if persister.calls != 0 {
		t.Fatalf("persist calls = %d, want 0", persister.calls)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(publisher.published))
	}
	dead := publisher.published[0]
	if dead.Kind != DeadLetterGroup || dead.Reason != dlqReasonMalformed || dead.Attempts != 1 {
		t.Fatalf("dead letter = %#v", dead)
	}
	if string(dead.Body) != string(body) {
		t.Fatalf("dead letter body = %q, want original", dead.Body)
	}
	if delivery.acks != 1 || delivery.nacks != 0 {
		t.Fatalf("acks/nacks = %d/%d, want 1/0", delivery.acks, delivery.nacks)
	}
}

func TestGroupConsumerUnroutableDeadLetterReturnsForReconnect(t *testing.T) {
	delivery := &consumeFakeDelivery{body: consumeTestGroupBody(t), messageID: "7"}
	persister := &consumeFakeGroupPersister{fail: consumeAlwaysFail(errors.New("permanent"))}
	publisher := &consumeFakePublisher{
		failures: 2,
		err:      fmt.Errorf("group dead letter unroutable: %w", errDLQUnroutable),
	}
	clock := &consumeFakeClock{}
	consumer := &GroupMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

	err := consumer.handleDelivery(context.Background(), delivery, publisher)
	if err == nil || !errors.Is(err, errDLQUnroutable) {
		t.Fatalf("handleDelivery() error = %v, want an unroutable error for reconnect", err)
	}
	if delivery.acks != 0 || delivery.nacks != 0 {
		t.Fatalf("acks/nacks = %d/%d, want 0/0: the original delivery must stay unacked", delivery.acks, delivery.nacks)
	}
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1: an unroutable DLQ must not be retried in place", publisher.publishCalls)
	}
	if want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}; !reflect.DeepEqual(clock.sleeps, want) {
		t.Fatalf("sleeps = %v, want only the five message waits", clock.sleeps)
	}
}

func TestGroupConsumerReconnectLogResetsAfterConnectionReady(t *testing.T) {
	consumer := &GroupMessageConsumer{}
	var logs []string
	consumer.logf = func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}
	err := errors.New("dial failed")
	consumer.logReconnect(err)
	consumer.logReconnect(err)
	if len(logs) != 1 {
		t.Fatalf("logs = %v, want one line for the repeated failure", logs)
	}
	consumer.markConnectionReady()
	consumer.logReconnect(err)
	if len(logs) != 2 || logs[1] != logs[0] {
		t.Fatalf("logs = %v, want the later identical outage logged again", logs)
	}
}

func TestGroupConsumerRunBacksOffAndReportsReconnectErrors(t *testing.T) {
	consumer := NewGroupMessageConsumer(
		config.RabbitMQConfig{},
		nil,
		&consumeFakeGroupPersister{},
		DefaultNamespace(),
	)
	attempts := 0
	consumer.dialer = func(config.RabbitMQConfig) (*amqp091.Connection, error) {
		attempts++
		if attempts == 3 {
			// A connection that persisted a message resets the reconnect delay.
			consumer.status.recordPersisted(consumeTestNow)
			return nil, errors.New("connection lost")
		}
		return nil, errors.New("consume failed")
	}
	var delays []time.Duration
	consumer.wait = func(_ context.Context, delay time.Duration) bool {
		delays = append(delays, delay)
		return len(delays) < 5
	}
	var logs []string
	consumer.logf = func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	status := consumer.Status()
	const dialFailed = "dial RabbitMQ for group message consumer: consume failed"
	if status.LastError != dialFailed || status.LastErrorAt.IsZero() {
		t.Fatalf("status error = %q at %v", status.LastError, status.LastErrorAt)
	}
	wantLogs := []string{
		"group message consumer reconnecting: " + dialFailed,
		"group message consumer reconnecting: dial RabbitMQ for group message consumer: connection lost",
		"group message consumer reconnecting: " + dialFailed,
	}
	if !reflect.DeepEqual(logs, wantLogs) {
		t.Fatalf("logs = %v, want %v", logs, wantLogs)
	}
	wantDelays := []time.Duration{
		time.Second,
		2 * time.Second,
		time.Second,
		2 * time.Second,
		4 * time.Second,
	}
	if !reflect.DeepEqual(delays, wantDelays) {
		t.Fatalf("delays = %v, want %v", delays, wantDelays)
	}
}

func TestGroupConsumerCountsInFlightDeliveries(t *testing.T) {
	consumer := &GroupMessageConsumer{
		retry: consumeTestRetryPolicy(&consumeFakeClock{}, consumeTestNow),
	}
	observed := -1
	consumer.persister = consumeGroupPersisterFunc(
		func(context.Context, store.GroupMessage) (bool, error) {
			observed = consumer.Status().InFlight
			return true, nil
		},
	)
	delivery := &consumeFakeDelivery{body: consumeTestGroupBody(t), messageID: "7"}

	if err := consumer.handleDelivery(
		context.Background(), delivery, &consumeFakePublisher{},
	); err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if observed != 1 {
		t.Fatalf("in flight during persist = %d, want 1", observed)
	}
	if inFlight := consumer.Status().InFlight; inFlight != 0 {
		t.Fatalf("in flight after the ack = %d, want 0", inFlight)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	delivery = &consumeFakeDelivery{body: consumeTestGroupBody(t), messageID: "8"}
	if err := consumer.handleDelivery(
		canceled, delivery, &consumeFakePublisher{},
	); err != nil {
		t.Fatalf("handleDelivery() with a canceled context = %v", err)
	}
	if inFlight := consumer.Status().InFlight; inFlight != 0 {
		t.Fatalf("in flight after a canceled delivery = %d, want 0", inFlight)
	}
}

func TestGroupConsumerBatchPersistsAndAcksAll(t *testing.T) {
	deliveries := []consumeDelivery{
		&consumeFakeDelivery{body: consumeTestGroupBodyWith(1, 1), messageID: "1"},
		&consumeFakeDelivery{body: consumeTestGroupBodyWith(2, 2), messageID: "2"},
	}
	persister := &consumeFakeBatchGroupPersister{}
	publisher := &consumeFakePublisher{}
	consumer := &GroupMessageConsumer{
		persister:      persister,
		batchPersister: persister,
		retry:          consumeTestRetryPolicy(&consumeFakeClock{}, consumeTestNow),
	}

	if err := consumer.handleBatch(context.Background(), deliveries, publisher); err != nil {
		t.Fatalf("handleBatch() error = %v", err)
	}
	if len(persister.batchCalls) != 1 || len(persister.batchCalls[0]) != 2 {
		t.Fatalf("batch calls = %#v, want one call with two messages", persister.batchCalls)
	}
	if persister.calls != 0 {
		t.Fatalf("single persist calls = %d, want 0", persister.calls)
	}
	for index, delivery := range deliveries {
		fake := delivery.(*consumeFakeDelivery)
		if fake.acks != 1 || fake.nacks != 0 {
			t.Fatalf("delivery %d acks/nacks = %d/%d, want 1/0", index, fake.acks, fake.nacks)
		}
	}
	if status := consumer.Status(); status.Persisted != 2 || status.DeadLettered != 0 {
		t.Fatalf("status = %#v, want persisted 2", status)
	}
}

func TestGroupConsumerBatchFallsBackAndIsolatesConflict(t *testing.T) {
	deliveries := []consumeDelivery{
		&consumeFakeDelivery{body: consumeTestGroupBodyWith(11, 1), messageID: "11"},
		&consumeFakeDelivery{body: consumeTestGroupBodyWith(12, 2), messageID: "12"},
	}
	persister := &consumeFakeBatchGroupPersister{}
	persister.batchFail = func(int) error { return store.ErrBatchFallback }
	persister.fail = func(call int) error {
		if call == 1 {
			return store.ErrSeqConflict
		}
		return nil
	}
	publisher := &consumeFakePublisher{}
	consumer := &GroupMessageConsumer{
		persister:      persister,
		batchPersister: persister,
		retry:          consumeTestRetryPolicy(&consumeFakeClock{}, consumeTestNow),
	}

	if err := consumer.handleBatch(context.Background(), deliveries, publisher); err != nil {
		t.Fatalf("handleBatch() error = %v", err)
	}
	if len(persister.batchCalls) != 1 || persister.calls != 2 {
		t.Fatalf("batch calls/single calls = %d/%d, want 1/2", len(persister.batchCalls), persister.calls)
	}
	if len(publisher.published) != 1 || publisher.published[0].Kind != DeadLetterGroup {
		t.Fatalf("dead letters = %#v, want one group dead letter", publisher.published)
	}
	for index, delivery := range deliveries {
		fake := delivery.(*consumeFakeDelivery)
		if fake.acks != 1 || fake.nacks != 0 {
			t.Fatalf("delivery %d acks/nacks = %d/%d, want 1/0", index, fake.acks, fake.nacks)
		}
	}
	if status := consumer.Status(); status.Persisted != 1 || status.DeadLettered != 1 {
		t.Fatalf("status = %#v, want persisted 1/dead-lettered 1", status)
	}
}

func TestGroupConsumerBatchDeadLettersMalformedOnly(t *testing.T) {
	deliveries := []consumeDelivery{
		&consumeFakeDelivery{body: []byte("not json"), messageID: "bad"},
		&consumeFakeDelivery{body: consumeTestGroupBodyWith(31, 1), messageID: "31"},
	}
	persister := &consumeFakeBatchGroupPersister{}
	publisher := &consumeFakePublisher{}
	consumer := &GroupMessageConsumer{
		persister:      persister,
		batchPersister: persister,
		retry:          consumeTestRetryPolicy(&consumeFakeClock{}, consumeTestNow),
	}

	if err := consumer.handleBatch(context.Background(), deliveries, publisher); err != nil {
		t.Fatalf("handleBatch() error = %v", err)
	}
	if len(persister.batchCalls) != 1 || len(persister.batchCalls[0]) != 1 {
		t.Fatalf("batch calls = %#v, want one message", persister.batchCalls)
	}
	if len(publisher.published) != 1 || publisher.published[0].Reason != dlqReasonMalformed {
		t.Fatalf("dead letters = %#v, want one malformed", publisher.published)
	}
	for index, delivery := range deliveries {
		fake := delivery.(*consumeFakeDelivery)
		if fake.acks != 1 {
			t.Fatalf("delivery %d acks = %d, want 1", index, fake.acks)
		}
	}
	if status := consumer.Status(); status.Persisted != 1 || status.DeadLettered != 1 {
		t.Fatalf("status = %#v, want persisted 1/dead-lettered 1", status)
	}
}

func TestGroupConsumerBatchLoopFlushesOnLingerAndTracksInFlight(t *testing.T) {
	persister := &consumeFakeBatchGroupPersister{}
	publisher := &consumeFakePublisher{}
	consumer := &GroupMessageConsumer{
		persister:      persister,
		batchPersister: persister,
		batchSize:      100,
		batchWait:      5 * time.Millisecond,
		retry:          consumeTestRetryPolicy(&consumeFakeClock{}, consumeTestNow),
	}
	persisted := make(chan int, 1)
	persister.onBatch = func() {
		persisted <- consumer.Status().InFlight
	}
	deliveries := make(chan amqp091.Delivery, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.consumeBatches(ctx, deliveries, publisher) }()

	acknowledgers := make([]*consumeFakeAcknowledger, 0, 2)
	for index := uint64(1); index <= 2; index++ {
		delivery, acknowledger := consumeTestAMQPDelivery(consumeTestGroupBodyWith(index, index), index)
		acknowledgers = append(acknowledgers, acknowledger)
		deliveries <- delivery
	}

	select {
	case inFlight := <-persisted:
		if inFlight != 2 {
			t.Fatalf("in flight during batch persist = %d, want 2", inFlight)
		}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("batch did not flush on the linger timer")
	}
	deadline := time.Now().Add(time.Second)
	for consumer.Status().InFlight != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := consumer.Status().InFlight; got != 0 {
		cancel()
		t.Fatalf("in flight after batch = %d, want 0", got)
	}
	if len(persister.batchCalls) != 1 || len(persister.batchCalls[0]) != 2 {
		cancel()
		t.Fatalf("batch calls = %#v, want one call with two messages", persister.batchCalls)
	}
	for index, acknowledger := range acknowledgers {
		acks, nacks := acknowledger.counts()
		if acks != 1 || nacks != 0 {
			cancel()
			t.Fatalf("delivery %d acks/nacks = %d/%d, want 1/0", index, acks, nacks)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumeBatches did not stop after cancel")
	}
}
