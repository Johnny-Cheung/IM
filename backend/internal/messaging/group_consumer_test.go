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
