package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/rabbitmq/amqp091-go"
)

// DeadLetterKind identifies one message chain's dead-letter topology.
type DeadLetterKind string

const (
	DeadLetterDirect DeadLetterKind = "direct"
	DeadLetterGroup  DeadLetterKind = "group"
)

const dlqPublishTimeout = 10 * time.Second

const (
	dlqHeaderReason        = "x-dead-reason"
	dlqHeaderError         = "x-dead-error"
	dlqHeaderAttempts      = "x-dead-attempts"
	dlqHeaderFirstFailedAt = "x-dead-first-failed-at"
	dlqHeaderSource        = "x-dead-source"
)

const (
	dlqReasonMalformed        = "malformed"
	dlqReasonConflict         = "conflict"
	dlqReasonSeqConflict      = "seq_conflict"
	dlqReasonRetriesExhausted = "retries_exhausted"
)

const dlqErrorLimit = 500

// dlqErrPublisherClosed reports that the publishing channel cannot recover
// without a new connection. The consumer reconnects instead of retrying.
var dlqErrPublisherClosed = errors.New("dead letter publisher channel closed")

// DeadLetterTopology is the pair of exchanges and queues used to park messages
// that cannot be persisted. Source* names the main chain used by replay.
type DeadLetterTopology struct {
	Kind             DeadLetterKind
	Exchange         string
	Queue            string
	RoutingKey       string
	SourceExchange   string
	SourceRoutingKey string
}

// DeadLetterTopologyFor derives the dead-letter names for one chain. An
// unknown kind returns a zero topology that DeclareDeadLetterTopology rejects.
func DeadLetterTopologyFor(ns Namespace, kind DeadLetterKind) DeadLetterTopology {
	switch kind {
	case DeadLetterDirect:
		return DeadLetterTopology{
			Kind:             kind,
			Exchange:         ns.RabbitPrefix() + ".messages.dead",
			Queue:            ns.RabbitPrefix() + ".messages.dead",
			RoutingKey:       "direct.dead",
			SourceExchange:   directExchangeName(ns),
			SourceRoutingKey: directRoutingKey,
		}
	case DeadLetterGroup:
		return DeadLetterTopology{
			Kind:             kind,
			Exchange:         ns.RabbitPrefix() + ".group.messages.dead",
			Queue:            ns.RabbitPrefix() + ".group.messages.dead",
			RoutingKey:       "group.dead",
			SourceExchange:   groupExchangeName(ns),
			SourceRoutingKey: groupRoutingKey(),
		}
	default:
		return DeadLetterTopology{Kind: kind}
	}
}

// DeclareDeadLetterTopology creates the dead-letter exchange, queue and binding.
// The queue is durable without arguments, like the main queue.
func DeclareDeadLetterTopology(ch *amqp091.Channel, t DeadLetterTopology) error {
	if t.Exchange == "" || t.Queue == "" || t.RoutingKey == "" {
		return fmt.Errorf("dead letter topology for kind %q is incomplete", t.Kind)
	}
	if ch == nil {
		return fmt.Errorf("declare %s dead letter topology: channel is nil", t.Kind)
	}
	if err := ch.ExchangeDeclare(t.Exchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s dead letter exchange: %w", t.Kind, err)
	}
	if _, err := ch.QueueDeclare(t.Queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s dead letter queue: %w", t.Kind, err)
	}
	if err := ch.QueueBind(t.Queue, t.RoutingKey, t.Exchange, false, nil); err != nil {
		return fmt.Errorf("bind %s dead letter queue: %w", t.Kind, err)
	}
	return nil
}

// dlqMessage describes one delivery parked in the dead-letter queue. Body is
// kept byte-identical; only headers carry metadata.
type dlqMessage struct {
	Kind          DeadLetterKind
	Body          []byte
	MessageID     string
	Reason        string
	Error         string
	Attempts      int
	FirstFailedAt time.Time
}

func (m dlqMessage) headers() amqp091.Table {
	return amqp091.Table{
		dlqHeaderReason:        m.Reason,
		dlqHeaderError:         dlqTruncateError(m.Error),
		dlqHeaderAttempts:      m.Attempts,
		dlqHeaderFirstFailedAt: m.FirstFailedAt.UTC().Format(time.RFC3339),
		dlqHeaderSource:        string(m.Kind),
	}
}

// dlqTruncateError bounds the error header and keeps the cut on a rune
// boundary so the header stays valid UTF-8.
func dlqTruncateError(text string) string {
	if len(text) <= dlqErrorLimit {
		return text
	}
	cut := dlqErrorLimit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}

// consumeDeadLetterPublisher parks one delivery. The production implementation
// waits for a broker confirmation; failures are retried by the caller.
type consumeDeadLetterPublisher interface {
	PublishDeadLetter(context.Context, dlqMessage) error
}

// dlqPublishChannel is the AMQP channel surface the publisher uses. It is an
// interface so the publish/confirm sequence can be tested without a broker.
type dlqPublishChannel interface {
	Confirm(noWait bool) error
	NotifyPublish(chan amqp091.Confirmation) chan amqp091.Confirmation
	NotifyReturn(chan amqp091.Return) chan amqp091.Return
	GetNextPublishSeqNo() uint64
	PublishWithContext(
		ctx context.Context,
		exchange, key string,
		mandatory, immediate bool,
		msg amqp091.Publishing,
	) error
}

// dlqAMQPPublisher publishes dead letters on a confirm-mode channel. Publish
// calls are sequential, so the confirmation for one publish can be matched by
// its delivery tag.
type dlqAMQPPublisher struct {
	channel  dlqPublishChannel
	topology DeadLetterTopology
	confirms chan amqp091.Confirmation
	returns  chan amqp091.Return
	timeout  time.Duration
}

func newDLQAMQPPublisher(channel dlqPublishChannel, topology DeadLetterTopology) (*dlqAMQPPublisher, error) {
	if channel == nil {
		return nil, fmt.Errorf("open %s dead letter publisher: channel is nil", topology.Kind)
	}
	if err := channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable %s dead letter publisher confirms: %w", topology.Kind, err)
	}
	return &dlqAMQPPublisher{
		channel:  channel,
		topology: topology,
		confirms: channel.NotifyPublish(make(chan amqp091.Confirmation, 1)),
		returns:  channel.NotifyReturn(make(chan amqp091.Return, 1)),
		timeout:  dlqPublishTimeout,
	}, nil
}

func (p *dlqAMQPPublisher) PublishDeadLetter(ctx context.Context, message dlqMessage) error {
	if p == nil || p.channel == nil || p.confirms == nil {
		return fmt.Errorf("dead letter publisher is not ready")
	}
	timeout := p.timeout
	if timeout <= 0 {
		timeout = dlqPublishTimeout
	}
	publishCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	sequence := p.channel.GetNextPublishSeqNo()
	err := p.channel.PublishWithContext(publishCtx, p.topology.Exchange, p.topology.RoutingKey, true, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    message.MessageID,
		Headers:      message.headers(),
		Body:         message.Body,
	})
	if err != nil {
		return fmt.Errorf("publish %s dead letter: %w", message.Kind, err)
	}
	return dlqAwaitConfirm(publishCtx, p.confirms, p.returns, sequence, string(message.Kind))
}

// dlqAwaitConfirm waits for the confirmation of one sequential publish. The
// publish must be mandatory: a basic.return means the broker dropped an
// unroutable message even though it confirmed the publish, so a return seen
// before the matching confirmation is a failure.
func dlqAwaitConfirm(
	ctx context.Context,
	confirms <-chan amqp091.Confirmation,
	returns <-chan amqp091.Return,
	sequence uint64,
	label string,
) error {
	for {
		select {
		case confirmation, ok := <-confirms:
			if !ok {
				return fmt.Errorf("%w while waiting for %s confirmation", dlqErrPublisherClosed, label)
			}
			if confirmation.DeliveryTag < sequence {
				// Stale confirmation from a publish that already timed out.
				continue
			}
			if confirmation.DeliveryTag != sequence {
				return fmt.Errorf("unexpected %s dead letter confirmation tag %d, want %d", label, confirmation.DeliveryTag, sequence)
			}
			if !confirmation.Ack {
				return fmt.Errorf("RabbitMQ rejected %s dead letter", label)
			}
			// RabbitMQ sends basic.return before basic.ack; prefer a pending return.
			select {
			case returned, ok := <-returns:
				if !ok {
					return nil
				}
				return dlqUnroutableError(returned, label)
			default:
				return nil
			}
		case returned, ok := <-returns:
			if !ok {
				// The channel closed its return notifications; wait for the confirm.
				returns = nil
				continue
			}
			return dlqUnroutableError(returned, label)
		case <-ctx.Done():
			return fmt.Errorf("wait for %s dead letter confirmation: %w", label, ctx.Err())
		}
	}
}

func dlqUnroutableError(returned amqp091.Return, label string) error {
	return fmt.Errorf(
		"%s dead letter unroutable: exchange %q routing key %q: %s",
		label, returned.Exchange, returned.RoutingKey, returned.ReplyText,
	)
}
