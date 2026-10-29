package messaging

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
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
