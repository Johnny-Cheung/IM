package messaging

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rabbitmq/amqp091-go"
)

func TestDeadLetterTopologyFor(t *testing.T) {
	namespace, err := NewNamespace("im:test")
	if err != nil {
		t.Fatalf("NewNamespace() error = %v", err)
	}

	direct := DeadLetterTopologyFor(namespace, DeadLetterDirect)
	wantDirect := DeadLetterTopology{
		Kind:             DeadLetterDirect,
		Exchange:         "im.test.messages.dead",
		Queue:            "im.test.messages.dead",
		RoutingKey:       "direct.dead",
		SourceExchange:   "im.test.messages",
		SourceRoutingKey: "direct.persist",
	}
	if direct != wantDirect {
		t.Fatalf("direct topology = %#v, want %#v", direct, wantDirect)
	}

	group := DeadLetterTopologyFor(namespace, DeadLetterGroup)
	wantGroup := DeadLetterTopology{
		Kind:             DeadLetterGroup,
		Exchange:         "im.test.group.messages.dead",
		Queue:            "im.test.group.messages.dead",
		RoutingKey:       "group.dead",
		SourceExchange:   "im.test.group.messages",
		SourceRoutingKey: "group.persist",
	}
	if group != wantGroup {
		t.Fatalf("group topology = %#v, want %#v", group, wantGroup)
	}

	if unknown := DeadLetterTopologyFor(namespace, DeadLetterKind("other")); unknown.Exchange != "" || unknown.Queue != "" {
		t.Fatalf("unknown topology = %#v, want empty names", unknown)
	}
}

func TestDeclareDeadLetterTopologyRejectsIncompleteTopology(t *testing.T) {
	if err := DeclareDeadLetterTopology(nil, DeadLetterTopology{Kind: DeadLetterDirect}); err == nil {
		t.Fatal("DeclareDeadLetterTopology() error = nil, want incomplete topology error")
	}
	if err := DeclareDeadLetterTopology(nil, DeadLetterTopologyFor(DefaultNamespace(), DeadLetterDirect)); err == nil {
		t.Fatal("DeclareDeadLetterTopology(nil channel) error = nil, want nil channel error")
	}
}

func TestDLQMessageHeaders(t *testing.T) {
	failedAt := time.Date(2026, 10, 29, 12, 0, 0, 0, time.FixedZone("CST", 8*3600))
	message := dlqMessage{
		Kind:          DeadLetterGroup,
		Body:          []byte(`{"server_message_id":7}`),
		MessageID:     "7",
		Reason:        dlqReasonRetriesExhausted,
		Error:         "persist group message: foreign key constraint fails",
		Attempts:      6,
		FirstFailedAt: failedAt,
	}
	headers := message.headers()
	if len(headers) != 5 {
		t.Fatalf("headers = %#v, want five entries", headers)
	}
	if got := headers[dlqHeaderReason]; got != dlqReasonRetriesExhausted {
		t.Fatalf("reason = %v", got)
	}
	if got := headers[dlqHeaderError]; got != message.Error {
		t.Fatalf("error = %v", got)
	}
	if got, ok := headers[dlqHeaderAttempts].(int); !ok || got != 6 {
		t.Fatalf("attempts = %#v, want int 6", headers[dlqHeaderAttempts])
	}
	if got := headers[dlqHeaderFirstFailedAt]; got != "2026-10-29T04:00:00Z" {
		t.Fatalf("first failed at = %v, want UTC RFC3339", got)
	}
	if got := headers[dlqHeaderSource]; got != "group" {
		t.Fatalf("source = %v, want group", got)
	}
	for key, value := range headers {
		if text, ok := value.(string); ok && strings.Contains(text, string(message.Body)) {
			t.Fatalf("header %s contains the message body", key)
		}
	}
	if !bytes.Equal(message.Body, []byte(`{"server_message_id":7}`)) {
		t.Fatalf("body = %q", message.Body)
	}
}

func TestDLQTruncateErrorKeepsValidUTF8(t *testing.T) {
	ascii := strings.Repeat("x", 600)
	if got := dlqTruncateError(ascii); len(got) != dlqErrorLimit {
		t.Fatalf("truncated length = %d, want %d", len(got), dlqErrorLimit)
	}

	multibyte := strings.Repeat("世", 300)
	got := dlqTruncateError(multibyte)
	if len(got) > dlqErrorLimit {
		t.Fatalf("truncated length = %d, want at most %d", len(got), dlqErrorLimit)
	}
	if !utf8.ValidString(got) {
		t.Fatal("truncated error is not valid UTF-8")
	}
	if !strings.HasPrefix(multibyte, got) {
		t.Fatal("truncated error is not a prefix of the original")
	}

	short := "short error"
	if got := dlqTruncateError(short); got != short {
		t.Fatalf("dlqTruncateError(short) = %q", got)
	}
}

func TestDLQAwaitConfirmClassifiesFailures(t *testing.T) {
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	canceled, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled()
	closedConfirms := make(chan amqp091.Confirmation)
	close(closedConfirms)

	returned := amqp091.Return{
		ReplyCode:  312,
		ReplyText:  "NO_ROUTE",
		Exchange:   "im.messages.dead",
		RoutingKey: "direct.dead",
	}
	tests := []struct {
		name     string
		confirms chan amqp091.Confirmation
		returns  chan amqp091.Return
		ctx      context.Context
		wantErr  error
		wantText string
	}{
		{
			name:     "matching ack",
			confirms: confirmationChan(5, true),
		},
		{
			name:     "stale tag skipped",
			confirms: confirmationsChan(amqp091.Confirmation{DeliveryTag: 4, Ack: false}, amqp091.Confirmation{DeliveryTag: 5, Ack: true}),
		},
		{
			name:     "unexpected tag",
			confirms: confirmationChan(6, true),
			wantText: "unexpected",
		},
		{
			name:     "nack",
			confirms: confirmationChan(5, false),
			wantText: "rejected",
		},
		{
			name:    "closed confirms",
			wantErr: dlqErrPublisherClosed,
		},
		{
			name:     "expired deadline",
			confirms: make(chan amqp091.Confirmation),
			ctx:      expired,
			wantErr:  context.DeadlineExceeded,
		},
		{
			name:     "canceled context",
			confirms: make(chan amqp091.Confirmation),
			ctx:      canceled,
			wantErr:  context.Canceled,
		},
		{
			name:     "return before confirm",
			confirms: confirmationChan(5, true),
			returns:  returnsChan(returned),
			wantText: "unroutable",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := testCase.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			confirms := testCase.confirms
			if confirms == nil {
				confirms = closedConfirms
			}
			err := dlqAwaitConfirm(ctx, confirms, testCase.returns, 5, "direct")
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("error = %v, want %v", err, testCase.wantErr)
				}
				return
			}
			if testCase.wantText != "" {
				if err == nil || !strings.Contains(err.Error(), testCase.wantText) {
					t.Fatalf("error = %v, want text %q", err, testCase.wantText)
				}
				return
			}
			if err != nil {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func confirmationChan(tag uint64, ack bool) chan amqp091.Confirmation {
	return confirmationsChan(amqp091.Confirmation{DeliveryTag: tag, Ack: ack})
}

func confirmationsChan(values ...amqp091.Confirmation) chan amqp091.Confirmation {
	confirms := make(chan amqp091.Confirmation, len(values))
	for _, value := range values {
		confirms <- value
	}
	return confirms
}

func returnsChan(values ...amqp091.Return) chan amqp091.Return {
	returns := make(chan amqp091.Return, len(values))
	for _, value := range values {
		returns <- value
	}
	return returns
}

// dlqTestPublishChannel is a broker-free dlqPublishChannel. It answers each
// publish with the configured return and confirmation in wire order.
type dlqTestPublishChannel struct {
	confirms   chan amqp091.Confirmation
	returns    chan amqp091.Return
	nextSeq    uint64
	ack        bool
	returned   *amqp091.Return
	publishErr error
	mandatory  []bool
	published  []amqp091.Publishing
}

func (f *dlqTestPublishChannel) Confirm(bool) error {
	return nil
}

func (f *dlqTestPublishChannel) NotifyPublish(c chan amqp091.Confirmation) chan amqp091.Confirmation {
	f.confirms = c
	return c
}

func (f *dlqTestPublishChannel) NotifyReturn(c chan amqp091.Return) chan amqp091.Return {
	f.returns = c
	return c
}

func (f *dlqTestPublishChannel) GetNextPublishSeqNo() uint64 {
	return f.nextSeq
}

func (f *dlqTestPublishChannel) PublishWithContext(
	ctx context.Context,
	exchange, key string,
	mandatory, immediate bool,
	msg amqp091.Publishing,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mandatory = append(f.mandatory, mandatory)
	f.published = append(f.published, msg)
	if f.publishErr != nil {
		return f.publishErr
	}
	if f.returned != nil {
		// A real broker returns the original message properties, including the
		// correlation id used to attribute the return to its publish.
		returned := *f.returned
		returned.CorrelationId = msg.CorrelationId
		f.returns <- returned
	}
	if f.ack {
		f.confirms <- amqp091.Confirmation{DeliveryTag: f.nextSeq, Ack: true}
	}
	return nil
}

func TestDLQAwaitPublishConfirmDrainsReturnBufferedAfterConfirm(t *testing.T) {
	returned := amqp091.Return{
		ReplyCode:     312,
		ReplyText:     "NO_ROUTE",
		Exchange:      "im.messages.dead",
		RoutingKey:    "direct.dead",
		CorrelationId: "7",
	}
	// The seam queues the return after the matching confirmation is consumed,
	// so the post-ack drain is exercised deterministically rather than through
	// select randomness.
	for iteration := 0; iteration < 50; iteration++ {
		returns := make(chan amqp091.Return, 1)
		err := dlqAwaitPublishConfirm(
			context.Background(),
			confirmationChan(5, true),
			returns,
			5,
			"7",
			"direct",
			func() { returns <- returned },
		)
		if err == nil || !errors.Is(err, errDLQUnroutable) {
			t.Fatalf("iteration %d: error = %v, want unroutable", iteration, err)
		}
	}
}

func TestDLQAwaitPublishConfirmIgnoresStaleReturn(t *testing.T) {
	stale := amqp091.Return{
		ReplyCode:     312,
		ReplyText:     "NO_ROUTE",
		Exchange:      "im.messages.dead",
		RoutingKey:    "direct.dead",
		CorrelationId: "6",
	}
	returns := returnsChan(stale)
	confirms := confirmationChan(5, true)
	if err := dlqAwaitPublishConfirm(context.Background(), confirms, returns, 5, "7", "direct", nil); err != nil {
		t.Fatalf("error = %v, want nil: a return from an earlier publish must be ignored", err)
	}
	if len(returns) != 0 {
		t.Fatalf("returns buffered = %d, want the stale return drained", len(returns))
	}
}

func TestDLQAwaitPublishConfirmSkipsStaleConfirm(t *testing.T) {
	confirms := confirmationsChan(
		amqp091.Confirmation{DeliveryTag: 4, Ack: false},
		amqp091.Confirmation{DeliveryTag: 5, Ack: true},
	)
	if err := dlqAwaitPublishConfirm(context.Background(), confirms, nil, 5, "7", "direct", nil); err != nil {
		t.Fatalf("error = %v, want nil: the stale nack must be skipped", err)
	}
}

func TestDLQAMQPPublisherRetiresAfterConfirmTimeout(t *testing.T) {
	topology := DeadLetterTopology{
		Kind:       DeadLetterDirect,
		Exchange:   "im.messages.dead",
		RoutingKey: "direct.dead",
	}
	fake := &dlqTestPublishChannel{nextSeq: 1}
	publisher, err := newDLQAMQPPublisher(fake, topology)
	if err != nil {
		t.Fatalf("newDLQAMQPPublisher() error = %v", err)
	}
	publisher.timeout = 20 * time.Millisecond
	message := dlqMessage{Kind: DeadLetterDirect, Body: []byte(`{}`), Reason: dlqReasonConflict}

	err = publisher.PublishDeadLetter(context.Background(), message)
	if !errors.Is(err, dlqErrPublisherClosed) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PublishDeadLetter() error = %v, want publisher-closed and deadline", err)
	}
	if len(fake.published) != 1 || fake.published[0].CorrelationId == "" {
		t.Fatalf("published = %#v, want one publish with a correlation id", fake.published)
	}
	// The late confirm or return for the timed-out publish must never be charged
	// to a later message: the publisher refuses reuse.
	err = publisher.PublishDeadLetter(context.Background(), message)
	if !errors.Is(err, dlqErrPublisherClosed) {
		t.Fatalf("second PublishDeadLetter() error = %v, want publisher-closed", err)
	}
	if len(fake.published) != 1 {
		t.Fatalf("published after retire = %d, want 1", len(fake.published))
	}
}

func TestDLQAMQPPublisherRetiresAfterInterruptedPublish(t *testing.T) {
	topology := DeadLetterTopology{
		Kind:       DeadLetterDirect,
		Exchange:   "im.messages.dead",
		RoutingKey: "direct.dead",
	}
	fake := &dlqTestPublishChannel{nextSeq: 1, publishErr: errors.New("connection write interrupted")}
	publisher, err := newDLQAMQPPublisher(fake, topology)
	if err != nil {
		t.Fatalf("newDLQAMQPPublisher() error = %v", err)
	}
	message := dlqMessage{Kind: DeadLetterDirect, Body: []byte(`{}`), Reason: dlqReasonConflict}

	err = publisher.PublishDeadLetter(context.Background(), message)
	if !errors.Is(err, dlqErrPublisherClosed) {
		t.Fatalf("PublishDeadLetter() error = %v, want publisher-closed", err)
	}
	if err := publisher.PublishDeadLetter(context.Background(), message); !errors.Is(err, dlqErrPublisherClosed) {
		t.Fatalf("second PublishDeadLetter() error = %v, want publisher-closed", err)
	}
	if len(fake.published) != 1 {
		t.Fatalf("published = %d, want the interrupted publish not retried on the same channel", len(fake.published))
	}
}

func TestDLQAMQPPublisherDrainsStaleReturnBeforePublish(t *testing.T) {
	topology := DeadLetterTopology{
		Kind:       DeadLetterDirect,
		Exchange:   "im.messages.dead",
		RoutingKey: "direct.dead",
	}
	fake := &dlqTestPublishChannel{nextSeq: 1, ack: true}
	publisher, err := newDLQAMQPPublisher(fake, topology)
	if err != nil {
		t.Fatalf("newDLQAMQPPublisher() error = %v", err)
	}
	fake.returns <- amqp091.Return{
		ReplyCode:     312,
		ReplyText:     "NO_ROUTE",
		Exchange:      topology.Exchange,
		RoutingKey:    topology.RoutingKey,
		CorrelationId: "stale-publish",
	}
	message := dlqMessage{Kind: DeadLetterDirect, Body: []byte(`{}`), Reason: dlqReasonConflict}
	if err := publisher.PublishDeadLetter(context.Background(), message); err != nil {
		t.Fatalf("PublishDeadLetter() error = %v, want nil: the stale return belongs to an earlier publish", err)
	}
}

func TestDLQAMQPPublisherRequiresRoutableMandatoryPublish(t *testing.T) {
	topology := DeadLetterTopology{
		Kind:       DeadLetterDirect,
		Exchange:   "im.messages.dead",
		RoutingKey: "direct.dead",
	}
	message := dlqMessage{
		Kind:      DeadLetterDirect,
		Body:      []byte(`{"server_message_id":7}`),
		MessageID: "7",
		Reason:    dlqReasonConflict,
	}

	t.Run("routable publish succeeds", func(t *testing.T) {
		fake := &dlqTestPublishChannel{nextSeq: 4, ack: true}
		publisher, err := newDLQAMQPPublisher(fake, topology)
		if err != nil {
			t.Fatalf("newDLQAMQPPublisher() error = %v", err)
		}
		if err := publisher.PublishDeadLetter(context.Background(), message); err != nil {
			t.Fatalf("PublishDeadLetter() error = %v", err)
		}
		if len(fake.mandatory) != 1 || !fake.mandatory[0] {
			t.Fatalf("mandatory flags = %v, want [true]", fake.mandatory)
		}
		if len(fake.published) != 1 || fake.published[0].MessageId != "7" {
			t.Fatalf("published = %#v", fake.published)
		}
	})

	t.Run("unroutable publish fails before ack", func(t *testing.T) {
		fake := &dlqTestPublishChannel{
			nextSeq: 9,
			ack:     true,
			returned: &amqp091.Return{
				ReplyCode:  312,
				ReplyText:  "NO_ROUTE",
				Exchange:   topology.Exchange,
				RoutingKey: topology.RoutingKey,
			},
		}
		publisher, err := newDLQAMQPPublisher(fake, topology)
		if err != nil {
			t.Fatalf("newDLQAMQPPublisher() error = %v", err)
		}
		err = publisher.PublishDeadLetter(context.Background(), message)
		if err == nil || !strings.Contains(err.Error(), "unroutable") {
			t.Fatalf("PublishDeadLetter() error = %v, want unroutable error", err)
		}
		if len(fake.mandatory) != 1 || !fake.mandatory[0] {
			t.Fatalf("mandatory flags = %v, want [true]", fake.mandatory)
		}
	})

	t.Run("publish error is returned", func(t *testing.T) {
		fake := &dlqTestPublishChannel{nextSeq: 1, publishErr: errors.New("channel closed")}
		publisher, err := newDLQAMQPPublisher(fake, topology)
		if err != nil {
			t.Fatalf("newDLQAMQPPublisher() error = %v", err)
		}
		if err := publisher.PublishDeadLetter(context.Background(), message); err == nil {
			t.Fatal("PublishDeadLetter() error = nil, want publish error")
		}
	})
}
