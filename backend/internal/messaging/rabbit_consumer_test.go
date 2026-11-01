package messaging

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/rabbitmq/amqp091-go"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

var consumeTestNow = time.Date(2026, 10, 29, 4, 0, 0, 0, time.UTC)

func TestRabbitConsumerDeadLettersMessageClassAfterSixAttempts(t *testing.T) {
	body := consumeTestDirectBody(t)
	delivery := &consumeFakeDelivery{body: body, messageID: "42"}
	persister := &consumeFakeMessagePersister{fail: consumeAlwaysFail(&mysql.MySQLError{Number: 1406, Message: "Data too long"})}
	publisher := &consumeFakePublisher{}
	clock := &consumeFakeClock{}
	consumer := &RabbitMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

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
	if dead.Reason != dlqReasonRetriesExhausted || dead.Attempts != 6 {
		t.Fatalf("dead letter = %#v, want retries_exhausted with 6 attempts", dead)
	}
	if dead.Kind != DeadLetterDirect || dead.MessageID != "42" || string(dead.Body) != string(body) {
		t.Fatalf("dead letter = %#v, want original body and message id", dead)
	}
	if !dead.FirstFailedAt.Equal(consumeTestNow) {
		t.Fatalf("first failed at = %v, want %v", dead.FirstFailedAt, consumeTestNow)
	}
	if delivery.acks != 1 || delivery.nacks != 0 {
		t.Fatalf("acks/nacks = %d/%d, want 1/0", delivery.acks, delivery.nacks)
	}
	status := consumer.Status()
	if status.DeadLettered != 1 || status.MessageRetries != 5 || status.Persisted != 0 || status.Retrying {
		t.Fatalf("status = %#v", status)
	}
	headers := dead.headers()
	if headers[dlqHeaderAttempts] != 6 || headers[dlqHeaderReason] != dlqReasonRetriesExhausted {
		t.Fatalf("headers = %#v", headers)
	}
	if headers[dlqHeaderSource] != string(DeadLetterDirect) {
		t.Fatalf("source header = %v", headers[dlqHeaderSource])
	}
}

func TestRabbitConsumerMessageClassSucceedsOnThirdAttempt(t *testing.T) {
	delivery := &consumeFakeDelivery{body: consumeTestDirectBody(t), messageID: "42"}
	failure := &mysql.MySQLError{Number: 1452, Message: "foreign key"}
	persister := &consumeFakeMessagePersister{fail: func(call int) error {
		if call < 3 {
			return failure
		}
		return nil
	}}
	publisher := &consumeFakePublisher{}
	clock := &consumeFakeClock{}
	consumer := &RabbitMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

	if err := consumer.handleDelivery(context.Background(), delivery, publisher); err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if persister.calls != 3 {
		t.Fatalf("persist calls = %d, want 3", persister.calls)
	}
	if want := []time.Duration{time.Second, 2 * time.Second}; !reflect.DeepEqual(clock.sleeps, want) {
		t.Fatalf("sleeps = %v, want %v", clock.sleeps, want)
	}
	if len(publisher.published) != 0 {
		t.Fatalf("dead letters = %d, want 0", len(publisher.published))
	}
	if delivery.acks != 1 || delivery.nacks != 0 {
		t.Fatalf("acks/nacks = %d/%d, want 1/0", delivery.acks, delivery.nacks)
	}
	status := consumer.Status()
	if status.Persisted != 1 || status.MessageRetries != 2 || status.DeadLettered != 0 {
		t.Fatalf("status = %#v", status)
	}
}

func TestRabbitConsumerTransientErrorsRetryWithoutLimit(t *testing.T) {
	delivery := &consumeFakeDelivery{body: consumeTestDirectBody(t), messageID: "42"}
	persister := &consumeFakeMessagePersister{fail: func(call int) error {
		if call <= 40 {
			return fmt.Errorf("persist direct message: %w", mysql.ErrInvalidConn)
		}
		return nil
	}}
	publisher := &consumeFakePublisher{}
	clock := &consumeFakeClock{}
	consumer := &RabbitMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

	if err := consumer.handleDelivery(context.Background(), delivery, publisher); err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if persister.calls != 41 {
		t.Fatalf("persist calls = %d, want 41", persister.calls)
	}
	if len(clock.sleeps) != 40 {
		t.Fatalf("sleeps = %d, want 40", len(clock.sleeps))
	}
	wantFirst := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	if !reflect.DeepEqual(clock.sleeps[:5], wantFirst) {
		t.Fatalf("first sleeps = %v, want %v", clock.sleeps[:5], wantFirst)
	}
	for index, delay := range clock.sleeps[5:] {
		if delay != 30*time.Second {
			t.Fatalf("sleep %d = %v, want 30s cap", index+5, delay)
		}
	}
	if len(publisher.published) != 0 {
		t.Fatalf("dead letters = %d, want 0", len(publisher.published))
	}
	if delivery.acks != 1 || delivery.nacks != 0 {
		t.Fatalf("acks/nacks = %d/%d, want 1/0", delivery.acks, delivery.nacks)
	}
	status := consumer.Status()
	if status.TransientRetries != 40 || status.MessageRetries != 0 || status.DeadLettered != 0 || status.Persisted != 1 {
		t.Fatalf("status = %#v", status)
	}
}

func TestRabbitConsumerDeadLetterPublishFailureKeepsDeliveryUnacked(t *testing.T) {
	delivery := &consumeFakeDelivery{body: consumeTestDirectBody(t), messageID: "42"}
	persister := &consumeFakeMessagePersister{fail: consumeAlwaysFail(errors.New("permanent"))}
	publisher := &consumeFakePublisher{failures: 2}
	unackedDuringPublish := true
	publisher.onPublish = func() {
		if delivery.acks != 0 || delivery.nacks != 0 {
			unackedDuringPublish = false
		}
	}
	clock := &consumeFakeClock{}
	consumer := &RabbitMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

	if err := consumer.handleDelivery(context.Background(), delivery, publisher); err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if !unackedDuringPublish {
		t.Fatal("delivery was acknowledged before the dead letter publish succeeded")
	}
	if publisher.publishCalls != 3 || len(publisher.published) != 1 {
		t.Fatalf("publish calls/published = %d/%d, want 3/1", publisher.publishCalls, len(publisher.published))
	}
	wantTail := []time.Duration{time.Second, 2 * time.Second}
	if len(clock.sleeps) != 7 || !reflect.DeepEqual(clock.sleeps[5:], wantTail) {
		t.Fatalf("sleeps = %v, want five message waits followed by %v", clock.sleeps, wantTail)
	}
	if delivery.acks != 1 || delivery.nacks != 0 {
		t.Fatalf("acks/nacks = %d/%d, want 1/0", delivery.acks, delivery.nacks)
	}
	status := consumer.Status()
	if status.DeadLettered != 1 || status.TransientRetries != 2 {
		t.Fatalf("status = %#v", status)
	}
}

func TestRabbitConsumerClosedPublisherReturnsForReconnect(t *testing.T) {
	delivery := &consumeFakeDelivery{body: consumeTestDirectBody(t), messageID: "42"}
	persister := &consumeFakeMessagePersister{fail: consumeAlwaysFail(errors.New("permanent"))}
	publisher := &consumeFakePublisher{
		failures: 1,
		err:      fmt.Errorf("publish direct dead letter: %w", amqp091.ErrClosed),
	}
	clock := &consumeFakeClock{}
	consumer := &RabbitMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

	err := consumer.handleDelivery(context.Background(), delivery, publisher)
	if err == nil {
		t.Fatal("handleDelivery() error = nil, want a closed publisher error")
	}
	if delivery.acks != 0 || delivery.nacks != 0 {
		t.Fatalf("acks/nacks = %d/%d, want 0/0", delivery.acks, delivery.nacks)
	}
	if len(clock.sleeps) != 5 {
		t.Fatalf("sleeps = %v, want only the five message waits", clock.sleeps)
	}
	if publisher.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.publishCalls)
	}
}

func TestRabbitConsumerContextCancelDuringWaitLeavesDeliveryUnacked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivery := &consumeFakeDelivery{body: consumeTestDirectBody(t), messageID: "42"}
	persister := &consumeFakeMessagePersister{fail: consumeAlwaysFail(&mysql.MySQLError{Number: 1452})}
	publisher := &consumeFakePublisher{}
	clock := &consumeFakeClock{cancelAt: 1, cancel: cancel}
	consumer := &RabbitMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

	if err := consumer.handleDelivery(ctx, delivery, publisher); err != nil {
		t.Fatalf("handleDelivery() error = %v, want nil on shutdown", err)
	}
	if persister.calls != 1 {
		t.Fatalf("persist calls = %d, want 1", persister.calls)
	}
	if delivery.acks != 0 || delivery.nacks != 0 {
		t.Fatalf("acks/nacks = %d/%d, want 0/0", delivery.acks, delivery.nacks)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("publish calls = %d, want 0", publisher.publishCalls)
	}
	if consumer.Status().Retrying {
		t.Fatal("status still reports retrying after the wait returned")
	}
}

func TestRabbitConsumerConflictDeadLettersImmediately(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		reason string
	}{
		{name: "message conflict", err: fmt.Errorf("persist: %w", store.ErrMessageConflict), reason: dlqReasonConflict},
		{name: "seq conflict", err: fmt.Errorf("persist: %w", store.ErrSeqConflict), reason: dlqReasonSeqConflict},
		{name: "seq invalid", err: fmt.Errorf("persist: %w", store.ErrSeqInvalid), reason: dlqReasonSeqConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := consumeTestDirectBody(t)
			delivery := &consumeFakeDelivery{body: body, messageID: "42"}
			persister := &consumeFakeMessagePersister{fail: consumeAlwaysFail(tc.err)}
			publisher := &consumeFakePublisher{}
			clock := &consumeFakeClock{}
			consumer := &RabbitMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

			if err := consumer.handleDelivery(context.Background(), delivery, publisher); err != nil {
				t.Fatalf("handleDelivery() error = %v", err)
			}
			if persister.calls != 1 {
				t.Fatalf("persist calls = %d, want 1", persister.calls)
			}
			if len(clock.sleeps) != 0 {
				t.Fatalf("sleeps = %v, want none", clock.sleeps)
			}
			if len(publisher.published) != 1 {
				t.Fatalf("dead letters = %d, want 1", len(publisher.published))
			}
			dead := publisher.published[0]
			if dead.Reason != tc.reason || dead.Attempts != 1 {
				t.Fatalf("dead letter = %#v, want reason %q with 1 attempt", dead, tc.reason)
			}
			if string(dead.Body) != string(body) || dead.MessageID != "42" {
				t.Fatalf("dead letter body/message id changed: %#v", dead)
			}
			consumeAssertDeadLetterHeaders(t, dead)
			if delivery.acks != 1 || delivery.nacks != 0 {
				t.Fatalf("acks/nacks = %d/%d, want 1/0", delivery.acks, delivery.nacks)
			}
		})
	}
}

func TestRabbitConsumerMalformedBodyDeadLettersImmediately(t *testing.T) {
	body := []byte(`{"server_message_id":`)
	delivery := &consumeFakeDelivery{body: body, messageID: "42"}
	persister := &consumeFakeMessagePersister{}
	publisher := &consumeFakePublisher{}
	clock := &consumeFakeClock{}
	consumer := &RabbitMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

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
	if dead.Reason != dlqReasonMalformed || dead.Attempts != 1 || dead.Kind != DeadLetterDirect {
		t.Fatalf("dead letter = %#v", dead)
	}
	if string(dead.Body) != string(body) {
		t.Fatalf("dead letter body = %q, want original", dead.Body)
	}
	consumeAssertDeadLetterHeaders(t, dead)
	if delivery.acks != 1 || delivery.nacks != 0 {
		t.Fatalf("acks/nacks = %d/%d, want 1/0", delivery.acks, delivery.nacks)
	}
}

func TestRabbitConsumerReportsRetryingDuringWait(t *testing.T) {
	delivery := &consumeFakeDelivery{body: consumeTestDirectBody(t), messageID: "42"}
	failure := &mysql.MySQLError{Number: 1452}
	persister := &consumeFakeMessagePersister{fail: func(call int) error {
		if call < 3 {
			return failure
		}
		return nil
	}}
	publisher := &consumeFakePublisher{}
	clock := &consumeFakeClock{}
	consumer := &RabbitMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}
	observed := 0
	clock.onSleep = func(int) {
		observed++
		if !consumer.Status().Retrying {
			t.Fatalf("status does not report retrying during wait %d", observed)
		}
	}

	if err := consumer.handleDelivery(context.Background(), delivery, publisher); err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if observed != 2 {
		t.Fatalf("observed waits = %d, want 2", observed)
	}
	if consumer.Status().Retrying {
		t.Fatal("status still reports retrying after the delivery completed")
	}
}

func TestRabbitConsumerAckFailureIsReturned(t *testing.T) {
	delivery := &consumeFakeDelivery{
		body:      consumeTestDirectBody(t),
		messageID: "42",
		ackErr:    errors.New("channel closed"),
	}
	persister := &consumeFakeMessagePersister{}
	publisher := &consumeFakePublisher{}
	clock := &consumeFakeClock{}
	consumer := &RabbitMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

	err := consumer.handleDelivery(context.Background(), delivery, publisher)
	if err == nil {
		t.Fatal("handleDelivery() error = nil, want ack error")
	}
	if persister.calls != 1 || delivery.acks != 1 || delivery.nacks != 0 {
		t.Fatalf("calls/acks/nacks = %d/%d/%d", persister.calls, delivery.acks, delivery.nacks)
	}
	if consumer.Status().Persisted != 1 {
		t.Fatalf("status = %#v", consumer.Status())
	}
}

func TestRabbitConsumerUnroutableDeadLetterReturnsForReconnect(t *testing.T) {
	delivery := &consumeFakeDelivery{body: consumeTestDirectBody(t), messageID: "42"}
	persister := &consumeFakeMessagePersister{fail: consumeAlwaysFail(errors.New("permanent"))}
	publisher := &consumeFakePublisher{
		failures: 2,
		err:      fmt.Errorf("direct dead letter unroutable: %w", errDLQUnroutable),
	}
	clock := &consumeFakeClock{}
	consumer := &RabbitMessageConsumer{persister: persister, retry: consumeTestRetryPolicy(clock, consumeTestNow)}

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
	if consumer.Status().DeadLettered != 0 {
		t.Fatalf("status = %#v, want no dead letter recorded", consumer.Status())
	}
}

func TestRabbitConsumerReconnectLogResetsAfterConnectionReady(t *testing.T) {
	consumer := &RabbitMessageConsumer{}
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
	// The consumer calls this once a fresh connection has declared topology and
	// started consuming.
	consumer.markConnectionReady()
	consumer.logReconnect(err)
	if len(logs) != 2 || logs[1] != logs[0] {
		t.Fatalf("logs = %v, want the later identical outage logged again", logs)
	}
}

func TestRabbitConsumerRunBacksOffAndReportsReconnectErrors(t *testing.T) {
	consumer := NewRabbitMessageConsumer(
		config.RabbitMQConfig{},
		nil,
		&consumeFakeMessagePersister{},
	)
	attempts := 0
	consumer.dialer = func(config.RabbitMQConfig) (*amqp091.Connection, error) {
		attempts++
		if attempts == 7 {
			// A connection that persisted a message resets the reconnect delay.
			consumer.status.recordPersisted(consumeTestNow)
			return nil, errors.New("connection lost")
		}
		return nil, errors.New("dial failed")
	}
	var delays []time.Duration
	consumer.wait = func(_ context.Context, delay time.Duration) bool {
		delays = append(delays, delay)
		return len(delays) < 9
	}
	var logs []string
	consumer.logf = func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	status := consumer.Status()
	const dialFailed = "dial RabbitMQ for message consumer: dial failed"
	if status.LastError != dialFailed || status.LastErrorAt.IsZero() {
		t.Fatalf("status error = %q at %v", status.LastError, status.LastErrorAt)
	}
	if status.Persisted != 1 {
		t.Fatalf("persisted = %d, want 1", status.Persisted)
	}
	wantLogs := []string{
		"direct message consumer reconnecting: " + dialFailed,
		"direct message consumer reconnecting: dial RabbitMQ for message consumer: connection lost",
		"direct message consumer reconnecting: " + dialFailed,
	}
	if !reflect.DeepEqual(logs, wantLogs) {
		t.Fatalf("logs = %v, want %v", logs, wantLogs)
	}
	wantDelays := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		time.Second,
		2 * time.Second,
		4 * time.Second,
	}
	if !reflect.DeepEqual(delays, wantDelays) {
		t.Fatalf("delays = %v, want %v", delays, wantDelays)
	}
	if attempts != len(wantDelays) {
		t.Fatalf("attempts = %d, want %d", attempts, len(wantDelays))
	}
}

func TestRabbitConsumerCountsInFlightDeliveries(t *testing.T) {
	consumer := &RabbitMessageConsumer{
		retry: consumeTestRetryPolicy(&consumeFakeClock{}, consumeTestNow),
	}
	observed := -1
	consumer.persister = consumeMessagePersisterFunc(
		func(context.Context, store.Message) (bool, error) {
			observed = consumer.Status().InFlight
			return true, nil
		},
	)
	delivery := &consumeFakeDelivery{body: consumeTestDirectBody(t), messageID: "42"}

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

	// A canceled context returns from persist without persisting; the delivery
	// marker must still be cleared.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	delivery = &consumeFakeDelivery{body: consumeTestDirectBody(t), messageID: "43"}
	if err := consumer.handleDelivery(
		canceled, delivery, &consumeFakePublisher{},
	); err != nil {
		t.Fatalf("handleDelivery() with a canceled context = %v", err)
	}
	if inFlight := consumer.Status().InFlight; inFlight != 0 {
		t.Fatalf("in flight after a canceled delivery = %d, want 0", inFlight)
	}
}

func TestRabbitConsumerBatchPersistsAndAcksAll(t *testing.T) {
	deliveries := []consumeDelivery{
		&consumeFakeDelivery{body: consumeTestDirectBodyWith(1, 1), messageID: "1"},
		&consumeFakeDelivery{body: consumeTestDirectBodyWith(2, 2), messageID: "2"},
		&consumeFakeDelivery{body: consumeTestDirectBodyWith(3, 3), messageID: "3"},
	}
	persister := &consumeFakeBatchMessagePersister{}
	publisher := &consumeFakePublisher{}
	consumer := &RabbitMessageConsumer{
		persister:      persister,
		batchPersister: persister,
		retry:          consumeTestRetryPolicy(&consumeFakeClock{}, consumeTestNow),
	}

	if err := consumer.handleBatch(context.Background(), deliveries, publisher); err != nil {
		t.Fatalf("handleBatch() error = %v", err)
	}
	if len(persister.batchCalls) != 1 || len(persister.batchCalls[0]) != 3 {
		t.Fatalf("batch calls = %#v, want one call with three messages", persister.batchCalls)
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
	if status := consumer.Status(); status.Persisted != 3 || status.DeadLettered != 0 {
		t.Fatalf("status = %#v, want persisted 3", status)
	}
}

func TestRabbitConsumerBatchFallsBackAndIsolatesConflict(t *testing.T) {
	deliveries := []consumeDelivery{
		&consumeFakeDelivery{body: consumeTestDirectBodyWith(11, 1), messageID: "11"},
		&consumeFakeDelivery{body: consumeTestDirectBodyWith(12, 2), messageID: "12"},
		&consumeFakeDelivery{body: consumeTestDirectBodyWith(13, 3), messageID: "13"},
	}
	persister := &consumeFakeBatchMessagePersister{}
	persister.batchFail = func(int) error { return store.ErrBatchFallback }
	persister.fail = func(call int) error {
		if call == 2 {
			return store.ErrSeqConflict
		}
		return nil
	}
	publisher := &consumeFakePublisher{}
	consumer := &RabbitMessageConsumer{
		persister:      persister,
		batchPersister: persister,
		retry:          consumeTestRetryPolicy(&consumeFakeClock{}, consumeTestNow),
	}

	if err := consumer.handleBatch(context.Background(), deliveries, publisher); err != nil {
		t.Fatalf("handleBatch() error = %v", err)
	}
	if len(persister.batchCalls) != 1 || persister.calls != 3 {
		t.Fatalf("batch calls/single calls = %d/%d, want 1/3", len(persister.batchCalls), persister.calls)
	}
	if len(publisher.published) != 1 || publisher.published[0].Reason != dlqReasonSeqConflict {
		t.Fatalf("dead letters = %#v, want one seq conflict", publisher.published)
	}
	for index, delivery := range deliveries {
		fake := delivery.(*consumeFakeDelivery)
		if fake.acks != 1 || fake.nacks != 0 {
			t.Fatalf("delivery %d acks/nacks = %d/%d, want 1/0", index, fake.acks, fake.nacks)
		}
	}
	if status := consumer.Status(); status.Persisted != 2 || status.DeadLettered != 1 {
		t.Fatalf("status = %#v, want persisted 2/dead-lettered 1", status)
	}
}

func TestRabbitConsumerBatchRetriesTransientFailure(t *testing.T) {
	deliveries := []consumeDelivery{
		&consumeFakeDelivery{body: consumeTestDirectBodyWith(21, 1), messageID: "21"},
		&consumeFakeDelivery{body: consumeTestDirectBodyWith(22, 2), messageID: "22"},
	}
	persister := &consumeFakeBatchMessagePersister{}
	persister.batchFail = func(call int) error {
		if call == 1 {
			return mysql.ErrInvalidConn
		}
		return nil
	}
	publisher := &consumeFakePublisher{}
	clock := &consumeFakeClock{}
	consumer := &RabbitMessageConsumer{
		persister:      persister,
		batchPersister: persister,
		retry:          consumeTestRetryPolicy(clock, consumeTestNow),
	}

	if err := consumer.handleBatch(context.Background(), deliveries, publisher); err != nil {
		t.Fatalf("handleBatch() error = %v", err)
	}
	if len(persister.batchCalls) != 2 || persister.calls != 0 {
		t.Fatalf("batch/single calls = %d/%d, want 2/0", len(persister.batchCalls), persister.calls)
	}
	if want := []time.Duration{time.Second}; !reflect.DeepEqual(clock.sleeps, want) {
		t.Fatalf("sleeps = %v, want %v", clock.sleeps, want)
	}
	for index, delivery := range deliveries {
		fake := delivery.(*consumeFakeDelivery)
		if fake.acks != 1 {
			t.Fatalf("delivery %d acks = %d, want 1", index, fake.acks)
		}
	}
	if status := consumer.Status(); status.Persisted != 2 || status.TransientRetries != 2 {
		t.Fatalf("status = %#v, want persisted 2/transient retries 2", status)
	}
}

func TestRabbitConsumerBatchDeadLettersMalformedOnly(t *testing.T) {
	deliveries := []consumeDelivery{
		&consumeFakeDelivery{body: []byte("not json"), messageID: "bad"},
		&consumeFakeDelivery{body: consumeTestDirectBodyWith(31, 1), messageID: "31"},
	}
	persister := &consumeFakeBatchMessagePersister{}
	publisher := &consumeFakePublisher{}
	consumer := &RabbitMessageConsumer{
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

func TestRabbitConsumerBatchLoopFlushesOnLingerAndTracksInFlight(t *testing.T) {
	persister := &consumeFakeBatchMessagePersister{}
	publisher := &consumeFakePublisher{}
	consumer := &RabbitMessageConsumer{
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
	deliveries := make(chan amqp091.Delivery, 3)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.consumeBatches(ctx, deliveries, publisher) }()

	acknowledgers := make([]*consumeFakeAcknowledger, 0, 3)
	for index := uint64(1); index <= 3; index++ {
		delivery, acknowledger := consumeTestAMQPDelivery(consumeTestDirectBodyWith(index, index), index)
		acknowledgers = append(acknowledgers, acknowledger)
		deliveries <- delivery
	}

	select {
	case inFlight := <-persisted:
		if inFlight != 3 {
			t.Fatalf("in flight during batch persist = %d, want 3", inFlight)
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
	if len(persister.batchCalls) != 1 || len(persister.batchCalls[0]) != 3 {
		cancel()
		t.Fatalf("batch calls = %#v, want one call with three messages", persister.batchCalls)
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

func TestRabbitConsumerBatchLoopShutdownLeavesPendingUnacked(t *testing.T) {
	persister := &consumeFakeBatchMessagePersister{}
	consumer := &RabbitMessageConsumer{
		persister:      persister,
		batchPersister: persister,
		batchSize:      100,
		batchWait:      time.Second,
		retry:          consumeTestRetryPolicy(&consumeFakeClock{}, consumeTestNow),
	}
	deliveries := make(chan amqp091.Delivery, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.consumeBatches(ctx, deliveries, &consumeFakePublisher{}) }()

	delivery, acknowledger := consumeTestAMQPDelivery(consumeTestDirectBodyWith(1, 1), 1)
	deliveries <- delivery
	deadline := time.Now().Add(time.Second)
	for consumer.Status().InFlight != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := consumer.Status().InFlight; got != 1 {
		cancel()
		t.Fatalf("in flight before shutdown = %d, want 1", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumeBatches did not stop after cancel")
	}
	if got := consumer.Status().InFlight; got != 0 {
		t.Fatalf("in flight after shutdown = %d, want 0", got)
	}
	if acks, nacks := acknowledger.counts(); acks != 0 || nacks != 0 {
		t.Fatalf("acks/nacks after shutdown = %d/%d, want 0/0", acks, nacks)
	}
	if len(persister.batchCalls) != 0 {
		t.Fatalf("batch calls after shutdown = %d, want 0", len(persister.batchCalls))
	}
}
