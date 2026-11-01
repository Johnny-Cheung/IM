package messaging

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

//go:embed lua/ack_delete.lua
var ackDeleteScriptSource string

var ackDeleteScript = redis.NewScript(ackDeleteScriptSource)

type RabbitDialer func(config.RabbitMQConfig) (*amqp091.Connection, error)

const (
	relayDefaultBackoff        = time.Second
	relayDefaultMaxBackoff     = 30 * time.Second
	relayDefaultBlock          = 2 * time.Second
	relayDefaultBatchSize      = 100
	relayDefaultConfirmTimeout = 30 * time.Second
)

// RelayStatus is a snapshot of a stream relay for /ops/infra.
type RelayStatus struct {
	Connected     bool      `json:"connected"`
	LastError     string    `json:"last_error,omitempty"`
	LastErrorAt   time.Time `json:"last_error_at,omitempty"`
	LastRelayedAt time.Time `json:"last_relayed_at,omitempty"`
	Relayed       uint64    `json:"relayed"`
	Recovered     uint64    `json:"recovered"`
}

// relayStatusTracker guards the mutable relay status fields.
type relayStatusTracker struct {
	mu     sync.Mutex
	status RelayStatus
}

func (t *relayStatusTracker) snapshot() RelayStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.status
}

func (t *relayStatusTracker) setConnected(connected bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Connected = connected
}

func (t *relayStatusTracker) recordRelayed(recovered bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Relayed++
	if recovered {
		t.status.Recovered++
	}
	t.status.LastRelayedAt = time.Now().UTC()
}

func (t *relayStatusTracker) recordError(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.LastError = err.Error()
	t.status.LastErrorAt = time.Now().UTC()
}

func (t *relayStatusTracker) relayedTotal() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.status.Relayed
}

// relayBackoff yields reconnect delays of 1s doubling to a 30s cap.
type relayBackoff struct {
	current time.Duration
	initial time.Duration
	max     time.Duration
}

func newRelayBackoff(initial, max time.Duration) *relayBackoff {
	return &relayBackoff{current: initial, initial: initial, max: max}
}

func (b *relayBackoff) next() time.Duration {
	delay := b.current
	b.current *= 2
	if b.current > b.max {
		b.current = b.max
	}
	return delay
}

func (b *relayBackoff) reset() {
	b.current = b.initial
}

// relayPublishReceipt represents a publish whose broker confirmation is still
// pending. A relay publishes a whole batch before waiting on any receipt.
type relayPublishReceipt interface {
	Wait(ctx context.Context) error
}

// relayPublisher starts one publish and returns a receipt for its confirmation.
type relayPublisher interface {
	Publish(ctx context.Context, exchange, routingKey, messageID string, payload []byte) (relayPublishReceipt, error)
}

// amqpRelayPublisher publishes over one confirmed channel.
type amqpRelayPublisher struct {
	channel *amqp091.Channel
}

func newAMQPRelayPublisher(channel *amqp091.Channel) (*amqpRelayPublisher, error) {
	if err := channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}
	return &amqpRelayPublisher{channel: channel}, nil
}

func (p *amqpRelayPublisher) Publish(
	ctx context.Context,
	exchange, routingKey, messageID string,
	payload []byte,
) (relayPublishReceipt, error) {
	confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		routingKey,
		false,
		false,
		amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			MessageId:    messageID,
			Body:         payload,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("publish message %s: %w", messageID, err)
	}
	if confirmation == nil {
		return nil, fmt.Errorf("publisher confirms are not enabled for message %s", messageID)
	}
	return relayDeferredReceipt{confirmation: confirmation, messageID: messageID}, nil
}

type relayDeferredReceipt struct {
	confirmation relayConfirmation
	messageID    string
}

// relayConfirmation is implemented by amqp091.DeferredConfirmation; tests
// provide a fake so the nack/closed/error cases are exercised without a broker.
type relayConfirmation interface {
	WaitContext(context.Context) (bool, error)
}

// Wait treats a nack, a closed confirm channel and a timeout as failures. The
// caller must not acknowledge the Redis Stream entry in any of those cases.
func (r relayDeferredReceipt) Wait(ctx context.Context) error {
	acknowledged, err := r.confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acknowledged {
		return fmt.Errorf("RabbitMQ rejected message %s", r.messageID)
	}
	return nil
}

// StreamRelay moves accepted Redis Stream messages into RabbitMQ.
type StreamRelay struct {
	redis     redis.UniversalClient
	config    config.RabbitMQConfig
	dialer    RabbitDialer
	keyspace  Keyspace
	namespace Namespace

	status  *relayStatusTracker
	backoff *relayBackoff
	block   time.Duration
	batch   int
	// confirmTimeout bounds waiting for the whole batch's publisher confirms.
	confirmTimeout time.Duration
	// connect runs one connection attempt; tests replace it.
	connect func(context.Context) error
	// wait pauses between reconnect attempts; tests replace it.
	wait func(context.Context, time.Duration) bool
}

func NewStreamRelay(
	client redis.UniversalClient,
	rabbitConfig config.RabbitMQConfig,
	dialer RabbitDialer,
) *StreamRelay {
	return NewStreamRelayWithNamespace(client, rabbitConfig, dialer, defaultKeyspace, DefaultNamespace())
}

func NewStreamRelayWithNamespace(
	client redis.UniversalClient,
	rabbitConfig config.RabbitMQConfig,
	dialer RabbitDialer,
	keyspace Keyspace,
	namespace Namespace,
) *StreamRelay {
	relay := &StreamRelay{
		redis:          client,
		config:         rabbitConfig,
		dialer:         dialer,
		keyspace:       keyspace,
		namespace:      namespace,
		status:         &relayStatusTracker{},
		backoff:        newRelayBackoff(relayDefaultBackoff, relayDefaultMaxBackoff),
		block:          relayDefaultBlock,
		batch:          relayDefaultBatchSize,
		confirmTimeout: relayDefaultConfirmTimeout,
		wait:           waitMessaging,
	}
	relay.connect = relay.runConnection
	return relay
}

// Run blocks until the context is canceled. It reconnects with exponential
// backoff after a failed connection or publish; each new connection first
// redelivers this consumer's pending entries.
func (r *StreamRelay) Run(ctx context.Context) error {
	if r == nil || r.redis == nil || r.dialer == nil {
		return ErrMessageServiceUnavailable
	}
	r.ensureDefaults()
	for {
		relayedBefore := r.status.relayedTotal()
		err := r.connect(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("stream relay stopped")
		}
		if r.status.relayedTotal() > relayedBefore {
			r.backoff.reset()
		}
		r.status.recordError(err)
		if !r.wait(ctx, r.backoff.next()) {
			return nil
		}
	}
}

// ensureDefaults fills collaborators omitted by a zero-value relay.
func (r *StreamRelay) ensureDefaults() {
	if r.status == nil {
		r.status = &relayStatusTracker{}
	}
	if r.backoff == nil {
		r.backoff = newRelayBackoff(relayDefaultBackoff, relayDefaultMaxBackoff)
	}
	if r.block <= 0 {
		r.block = relayDefaultBlock
	}
	if r.batch <= 0 {
		r.batch = relayDefaultBatchSize
	}
	if r.confirmTimeout <= 0 {
		r.confirmTimeout = relayDefaultConfirmTimeout
	}
	if r.connect == nil {
		r.connect = r.runConnection
	}
	if r.wait == nil {
		r.wait = waitMessaging
	}
}

// Status returns a snapshot for /ops/infra.
func (r *StreamRelay) Status() RelayStatus {
	if r == nil || r.status == nil {
		return RelayStatus{}
	}
	return r.status.snapshot()
}

func (r *StreamRelay) runConnection(ctx context.Context) error {
	connection, err := r.dialer(r.config)
	if err != nil {
		return fmt.Errorf("dial RabbitMQ for stream relay: %w", err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ relay channel: %w", err)
	}
	defer channel.Close()
	if err := declareDirectTopology(channel, r.namespace); err != nil {
		return err
	}
	publisher, err := newAMQPRelayPublisher(channel)
	if err != nil {
		return err
	}
	if err := ensureRelayGroup(ctx, r.redis, r.keyspace, r.namespace); err != nil {
		return err
	}
	r.status.setConnected(true)
	defer r.status.setConnected(false)
	return r.runPublisher(ctx, publisher)
}

// runPublisher drains this consumer's pending entries, then relays new ones.
func (r *StreamRelay) runPublisher(ctx context.Context, publisher relayPublisher) error {
	if err := r.drainPending(ctx, publisher); err != nil {
		return err
	}
	for {
		streams, err := r.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    directRelayGroup(r.namespace),
			Consumer: directConsumerName(r.namespace),
			Streams:  []string{r.keyspace.messageStreamKey(), ">"},
			Count:    int64(r.batch),
			Block:    r.block,
		}).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read Redis message stream: %w", err)
		}
		if err := r.relayStreams(ctx, publisher, streams, false); err != nil {
			return err
		}
	}
}

// drainPending republishes entries this consumer read but never acknowledged,
// for example after a publish failure or a restart while awaiting a confirm.
func (r *StreamRelay) drainPending(ctx context.Context, publisher relayPublisher) error {
	for {
		streams, err := r.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    directRelayGroup(r.namespace),
			Consumer: directConsumerName(r.namespace),
			Streams:  []string{r.keyspace.messageStreamKey(), "0"},
			Count:    int64(r.batch),
		}).Result()
		if errors.Is(err, redis.Nil) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read pending stream messages: %w", err)
		}
		if relayStreamCount(streams) == 0 {
			return nil
		}
		if err := r.relayStreams(ctx, publisher, streams, true); err != nil {
			return err
		}
	}
}

func (r *StreamRelay) relayStreams(
	ctx context.Context,
	publisher relayPublisher,
	streams []redis.XStream,
	recovered bool,
) error {
	entries := make([]relayStreamEntry, 0, relayStreamCount(streams))
	for _, stream := range streams {
		for _, entry := range stream.Messages {
			message, err := streamMessage(entry.Values)
			if err != nil {
				// Entries are produced by our own Lua script. A malformed entry
				// cannot be retried successfully, so remove it with the batch.
				log.Printf("drop malformed direct stream entry %s: %v", entry.ID, err)
				entries = append(entries, relayStreamEntry{entryID: entry.ID})
				continue
			}
			payload, err := encodeDirectMessage(message)
			if err != nil {
				return err
			}
			entries = append(entries, relayStreamEntry{
				entryID:   entry.ID,
				messageID: fmt.Sprintf("%d", message.ID),
				payload:   payload,
				valid:     true,
			})
		}
	}
	if len(entries) == 0 {
		return nil
	}

	receipts := make([]relayPublishReceipt, 0, len(entries))
	for _, entry := range entries {
		if !entry.valid {
			continue
		}
		receipt, err := publisher.Publish(
			ctx,
			directExchangeName(r.namespace),
			directRoutingKey,
			entry.messageID,
			entry.payload,
		)
		if err != nil {
			// Shutdown while awaiting the confirm: return without ack so the next
			// connection recovers the entry.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("relay message %s: %w", entry.messageID, err)
		}
		if receipt == nil {
			return fmt.Errorf("relay message %s returned no confirmation receipt", entry.messageID)
		}
		receipts = append(receipts, receipt)
	}

	// Publish the whole batch before waiting for any confirmation. The channel
	// is dedicated to this relay and confirms are correlated per message.
	confirmTimeout := r.confirmTimeout
	if confirmTimeout <= 0 {
		confirmTimeout = relayDefaultConfirmTimeout
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, confirmTimeout)
	defer cancelWait()
	for _, receipt := range receipts {
		if err := receipt.Wait(waitCtx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("await relay confirm: %w", err)
		}
	}

	entryIDs := make([]string, 0, len(entries))
	for _, entry := range entries {
		entryIDs = append(entryIDs, entry.entryID)
	}
	if err := r.ackAndDeleteMany(ctx, entryIDs); err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.valid {
			r.status.recordRelayed(recovered)
		}
	}
	return nil
}

type relayStreamEntry struct {
	entryID   string
	messageID string
	payload   []byte
	valid     bool
}

func (r *StreamRelay) ackAndDeleteMany(ctx context.Context, entryIDs []string) error {
	if len(entryIDs) == 0 {
		return nil
	}
	args := make([]interface{}, 0, len(entryIDs)+1)
	args = append(args, directRelayGroup(r.namespace))
	for _, entryID := range entryIDs {
		args = append(args, entryID)
	}
	// Atomic ack+delete for the whole batch: a zero delete count (entry already
	// gone) is still success.
	if err := ackDeleteScript.Run(
		ctx,
		r.redis,
		[]string{r.keyspace.messageStreamKey()},
		args...,
	).Err(); err != nil {
		return fmt.Errorf("ack and delete stream messages: %w", err)
	}
	return nil
}

func relayStreamCount(streams []redis.XStream) int {
	count := 0
	for _, stream := range streams {
		count += len(stream.Messages)
	}
	return count
}

func ensureRelayGroup(ctx context.Context, client redis.UniversalClient, keyspace Keyspace, namespace Namespace) error {
	err := client.XGroupCreateMkStream(ctx, keyspace.messageStreamKey(), directRelayGroup(namespace), "0-0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create Redis relay group: %w", err)
	}
	return nil
}

func declareDirectTopology(channel *amqp091.Channel, namespace Namespace) error {
	exchange := directExchangeName(namespace)
	queue := directQueueName(namespace)
	if err := channel.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare direct message exchange: %w", err)
	}
	if _, err := channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare direct message queue: %w", err)
	}
	if err := channel.QueueBind(queue, directRoutingKey, exchange, false, nil); err != nil {
		return fmt.Errorf("bind direct message queue: %w", err)
	}
	return nil
}

func waitMessaging(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
