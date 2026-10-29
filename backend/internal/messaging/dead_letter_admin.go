package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

const (
	dlqAdminMaxLimit       = 10000
	dlqAdminPublishTimeout = 10 * time.Second
)

// DeadLetterChannel is the AMQP channel surface used by the dead-letter admin
// commands. PublishConfirmed must publish and wait for a broker confirmation.
type DeadLetterChannel interface {
	Get(queue string, autoAck bool) (amqp091.Delivery, bool, error)
	PublishConfirmed(ctx context.Context, exchange, routingKey string, message amqp091.Publishing) error
	Ack(deliveryTag uint64) error
	Nack(deliveryTag uint64, multiple, requeue bool) error
	QueueDeclarePassive(name string) (amqp091.Queue, error)
}

// AMQPDeadLetterChannel implements DeadLetterChannel on a confirm-mode channel.
// PublishConfirmed calls are sequential so each confirmation can be matched by
// its delivery tag.
type AMQPDeadLetterChannel struct {
	channel  *amqp091.Channel
	confirms chan amqp091.Confirmation
	timeout  time.Duration
}

// NewAMQPDeadLetterChannel puts the channel into confirm mode so replays can
// wait for the broker before acknowledging the dead letter.
func NewAMQPDeadLetterChannel(channel *amqp091.Channel) (*AMQPDeadLetterChannel, error) {
	if channel == nil {
		return nil, fmt.Errorf("dead letter channel is nil")
	}
	if err := channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable dead letter publisher confirms: %w", err)
	}
	return &AMQPDeadLetterChannel{
		channel:  channel,
		confirms: channel.NotifyPublish(make(chan amqp091.Confirmation, 1)),
		timeout:  dlqAdminPublishTimeout,
	}, nil
}

func (c *AMQPDeadLetterChannel) Get(queue string, autoAck bool) (amqp091.Delivery, bool, error) {
	return c.channel.Get(queue, autoAck)
}

func (c *AMQPDeadLetterChannel) Ack(deliveryTag uint64) error {
	return c.channel.Ack(deliveryTag, false)
}

func (c *AMQPDeadLetterChannel) Nack(deliveryTag uint64, multiple, requeue bool) error {
	return c.channel.Nack(deliveryTag, multiple, requeue)
}

func (c *AMQPDeadLetterChannel) QueueDeclarePassive(name string) (amqp091.Queue, error) {
	return c.channel.QueueDeclarePassive(name, true, false, false, false, nil)
}

func (c *AMQPDeadLetterChannel) PublishConfirmed(
	ctx context.Context,
	exchange, routingKey string,
	message amqp091.Publishing,
) error {
	if c == nil || c.channel == nil || c.confirms == nil {
		return fmt.Errorf("dead letter channel is not ready")
	}
	timeout := c.timeout
	if timeout <= 0 {
		timeout = dlqAdminPublishTimeout
	}
	publishCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	sequence := c.channel.GetNextPublishSeqNo()
	if err := c.channel.PublishWithContext(publishCtx, exchange, routingKey, false, false, message); err != nil {
		return fmt.Errorf("publish replayed dead letter: %w", err)
	}
	for {
		select {
		case confirmation, ok := <-c.confirms:
			if !ok {
				return fmt.Errorf("publisher confirms closed while waiting for replay")
			}
			if confirmation.DeliveryTag < sequence {
				// Confirmation for a publish that already timed out.
				continue
			}
			if confirmation.DeliveryTag != sequence {
				return fmt.Errorf(
					"unexpected replay confirmation tag %d, want %d",
					confirmation.DeliveryTag, sequence,
				)
			}
			if !confirmation.Ack {
				return fmt.Errorf("RabbitMQ rejected replayed dead letter")
			}
			return nil
		case <-publishCtx.Done():
			return fmt.Errorf("wait for replay confirmation: %w", publishCtx.Err())
		}
	}
}

// DeadLetterEntry describes one message parked in a dead-letter queue. It
// carries envelope identifiers only, never the message body text.
type DeadLetterEntry struct {
	ID            uint64         `json:"id"`
	Kind          DeadLetterKind `json:"kind"`
	Reason        string         `json:"reason"`
	Error         string         `json:"error"`
	Attempts      int            `json:"attempts"`
	FirstFailedAt time.Time      `json:"first_failed_at"`
	Source        string         `json:"source"`
	BodySize      int            `json:"body_size"`
	SenderID      uint64         `json:"sender_id"`
	RecipientID   uint64         `json:"recipient_id"`
	GroupID       uint64         `json:"group_id"`
	Seq           uint64         `json:"seq"`
	SummaryError  string         `json:"summary_error,omitempty"`
}

// ReplayResult reports one replay run over a bounded part of a dead-letter
// queue.
type ReplayResult struct {
	Scanned  int `json:"scanned"`
	Replayed int `json:"replayed"`
	Skipped  int `json:"skipped"`
}

// ListDeadLetters reads up to limit dead letters and requeues all of them.
// The queue is inspected in a bounded window; requeued messages may change
// position, so repeated runs are not a stable snapshot.
func ListDeadLetters(
	ctx context.Context,
	channel DeadLetterChannel,
	topology DeadLetterTopology,
	limit int,
) ([]DeadLetterEntry, error) {
	if err := dlqAdminPrepare(ctx, channel, topology, limit); err != nil {
		return nil, err
	}
	entries := make([]DeadLetterEntry, 0, limit)
	pending := make([]uint64, 0, limit)
	defer func() { dlqAdminRequeue(channel, pending) }()

	_, err := dlqAdminEach(ctx, channel, topology, limit, func(delivery amqp091.Delivery) (bool, error) {
		entries = append(entries, dlqAdminEntry(topology.Kind, delivery))
		pending = append(pending, delivery.DeliveryTag)
		return false, nil
	})
	return entries, err
}

// ReplayDeadLetters republishes matching messages to the main chain and acks
// them only after the broker confirms. messageID zero means every message.
// A failed publish requeues that message and ends the run.
func ReplayDeadLetters(
	ctx context.Context,
	channel DeadLetterChannel,
	topology DeadLetterTopology,
	messageID uint64,
	limit int,
) (ReplayResult, error) {
	var result ReplayResult
	if err := dlqAdminPrepare(ctx, channel, topology, limit); err != nil {
		return result, err
	}
	pending := make([]uint64, 0, limit)
	defer func() { dlqAdminRequeue(channel, pending) }()

	_, err := dlqAdminEach(ctx, channel, topology, limit, func(delivery amqp091.Delivery) (bool, error) {
		result.Scanned++
		if !dlqAdminMatches(delivery, messageID) {
			pending = append(pending, delivery.DeliveryTag)
			result.Skipped++
			return false, nil
		}
		message := amqp091.Publishing{
			ContentType:  dlqAdminContentType(delivery),
			DeliveryMode: amqp091.Persistent,
			MessageId:    delivery.MessageId,
			Body:         delivery.Body,
		}
		if err := channel.PublishConfirmed(
			ctx, topology.SourceExchange, topology.SourceRoutingKey, message,
		); err != nil {
			_ = channel.Nack(delivery.DeliveryTag, false, true)
			return true, fmt.Errorf("replay dead letter %q: %w", delivery.MessageId, err)
		}
		if err := channel.Ack(delivery.DeliveryTag); err != nil {
			return true, fmt.Errorf("ack replayed dead letter %q: %w", delivery.MessageId, err)
		}
		result.Replayed++
		return false, nil
	})
	return result, err
}

// DiscardDeadLetters acks matching messages without republishing them.
// messageID zero means every message.
func DiscardDeadLetters(
	ctx context.Context,
	channel DeadLetterChannel,
	topology DeadLetterTopology,
	messageID uint64,
	limit int,
) (int, error) {
	if err := dlqAdminPrepare(ctx, channel, topology, limit); err != nil {
		return 0, err
	}
	discarded := 0
	pending := make([]uint64, 0, limit)
	defer func() { dlqAdminRequeue(channel, pending) }()

	_, err := dlqAdminEach(ctx, channel, topology, limit, func(delivery amqp091.Delivery) (bool, error) {
		if !dlqAdminMatches(delivery, messageID) {
			pending = append(pending, delivery.DeliveryTag)
			return false, nil
		}
		if err := channel.Ack(delivery.DeliveryTag); err != nil {
			return true, fmt.Errorf("discard dead letter %q: %w", delivery.MessageId, err)
		}
		discarded++
		return false, nil
	})
	return discarded, err
}

// DeadLetterCount reports the queue depth from a passive declare. It fails
// when the queue was never declared (for example, the group chain is off).
func DeadLetterCount(
	ctx context.Context,
	channel DeadLetterChannel,
	topology DeadLetterTopology,
) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if channel == nil {
		return 0, fmt.Errorf("inspect %s dead letter queue: channel is nil", topology.Kind)
	}
	if topology.Queue == "" {
		return 0, fmt.Errorf("dead letter topology for kind %q has no queue", topology.Kind)
	}
	queue, err := channel.QueueDeclarePassive(topology.Queue)
	if err != nil {
		return 0, fmt.Errorf("inspect %s dead letter queue: %w", topology.Kind, err)
	}
	return queue.Messages, nil
}

func dlqAdminPrepare(
	ctx context.Context,
	channel DeadLetterChannel,
	topology DeadLetterTopology,
	limit int,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if channel == nil {
		return fmt.Errorf("dead letter channel is nil")
	}
	if topology.Queue == "" || topology.SourceExchange == "" || topology.SourceRoutingKey == "" {
		return fmt.Errorf("dead letter topology for kind %q is incomplete", topology.Kind)
	}
	if limit < 1 || limit > dlqAdminMaxLimit {
		return fmt.Errorf("dead letter limit must be between 1 and %d", dlqAdminMaxLimit)
	}
	return nil
}

// dlqAdminEach walks a bounded window of the queue. Deliveries are left
// unacked so callers decide whether to ack or requeue them; seeing a message
// twice means the queue wrapped and the walk stops instead of looping.
func dlqAdminEach(
	ctx context.Context,
	channel DeadLetterChannel,
	topology DeadLetterTopology,
	limit int,
	visit func(amqp091.Delivery) (bool, error),
) (int, error) {
	seen := make(map[string]struct{}, limit)
	scanned := 0
	for scanned < limit {
		if err := ctx.Err(); err != nil {
			return scanned, err
		}
		delivery, ok, err := channel.Get(topology.Queue, false)
		if err != nil {
			return scanned, fmt.Errorf("read %s dead letter queue: %w", topology.Kind, err)
		}
		if !ok {
			return scanned, nil
		}
		scanned++
		key := dlqAdminDeliveryKey(delivery)
		if _, duplicate := seen[key]; duplicate {
			if err := channel.Nack(delivery.DeliveryTag, false, true); err != nil {
				return scanned, fmt.Errorf("requeue %s dead letter: %w", topology.Kind, err)
			}
			return scanned, nil
		}
		seen[key] = struct{}{}
		stop, err := visit(delivery)
		if err != nil {
			return scanned, err
		}
		if stop {
			return scanned, nil
		}
	}
	return scanned, nil
}

func dlqAdminRequeue(channel DeadLetterChannel, tags []uint64) {
	for _, tag := range tags {
		_ = channel.Nack(tag, false, true)
	}
}

func dlqAdminDeliveryKey(delivery amqp091.Delivery) string {
	if id := strings.TrimSpace(delivery.MessageId); id != "" {
		return "id:" + id
	}
	sum := sha256.Sum256(delivery.Body)
	return "body:" + hex.EncodeToString(sum[:16])
}

func dlqAdminMatches(delivery amqp091.Delivery, messageID uint64) bool {
	if messageID == 0 {
		return true
	}
	id, err := strconv.ParseUint(strings.TrimSpace(delivery.MessageId), 10, 64)
	return err == nil && id == messageID
}

func dlqAdminContentType(delivery amqp091.Delivery) string {
	if delivery.ContentType != "" {
		return delivery.ContentType
	}
	return "application/json"
}

type dlqAdminSummary struct {
	SenderID    uint64 `json:"sender_id"`
	RecipientID uint64 `json:"recipient_id"`
	GroupID     uint64 `json:"group_id"`
	Seq         uint64 `json:"seq"`
}

func dlqAdminEntry(kind DeadLetterKind, delivery amqp091.Delivery) DeadLetterEntry {
	entry := DeadLetterEntry{
		Kind:     kind,
		Reason:   dlqAdminHeaderString(delivery.Headers, dlqHeaderReason),
		Error:    dlqAdminHeaderString(delivery.Headers, dlqHeaderError),
		Attempts: dlqAdminHeaderInt(delivery.Headers, dlqHeaderAttempts),
		Source:   dlqAdminHeaderString(delivery.Headers, dlqHeaderSource),
		BodySize: len(delivery.Body),
	}
	if id, err := strconv.ParseUint(strings.TrimSpace(delivery.MessageId), 10, 64); err == nil {
		entry.ID = id
	}
	if raw := dlqAdminHeaderString(delivery.Headers, dlqHeaderFirstFailedAt); raw != "" {
		if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
			entry.FirstFailedAt = parsed.UTC()
		}
	}
	if err := dlqAdminSummarize(kind, delivery.Body, &entry); err != nil {
		entry.SummaryError = err.Error()
	}
	return entry
}

func dlqAdminSummarize(kind DeadLetterKind, body []byte, entry *DeadLetterEntry) error {
	var summary dlqAdminSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		return fmt.Errorf("decode message envelope: %w", err)
	}
	entry.SenderID = summary.SenderID
	entry.Seq = summary.Seq
	if kind == DeadLetterGroup {
		entry.GroupID = summary.GroupID
	} else {
		entry.RecipientID = summary.RecipientID
	}
	return nil
}

func dlqAdminHeaderString(headers amqp091.Table, key string) string {
	if headers == nil {
		return ""
	}
	switch value := headers[key].(type) {
	case string:
		return value
	case []byte:
		return string(value)
	case fmt.Stringer:
		return value.String()
	default:
		return ""
	}
}

func dlqAdminHeaderInt(headers amqp091.Table, key string) int {
	if headers == nil {
		return 0
	}
	switch value := headers[key].(type) {
	case int:
		return value
	case int8:
		return int(value)
	case int16:
		return int(value)
	case int32:
		return int(value)
	case int64:
		return int(value)
	case uint8:
		return int(value)
	case uint16:
		return int(value)
	case uint32:
		return int(value)
	case uint64:
		if value > uint64(^uint(0)>>1) {
			return 0
		}
		return int(value)
	case float32:
		return int(value)
	case float64:
		return int(value)
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return 0
		}
		return parsed
	case []byte:
		parsed, err := strconv.Atoi(strings.TrimSpace(string(value)))
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}
