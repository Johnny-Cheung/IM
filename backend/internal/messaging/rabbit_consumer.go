package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

const (
	defaultConsumerPrefetch  = 100
	defaultConsumerBatchSize = 100
	defaultConsumerBatchWait = 10 * time.Millisecond
)

// MessagePersister is the MySQL side of the RabbitMQ consumer.
type MessagePersister interface {
	PersistMessage(context.Context, store.Message) (bool, error)
}

// MessageBatchPersister is an optional fast path. A persister that does not
// implement it keeps using PersistMessage one delivery at a time.
type MessageBatchPersister interface {
	PersistMessages(context.Context, []store.Message) error
}

// RabbitMessageConsumer persists RabbitMQ direct-message deliveries.
type RabbitMessageConsumer struct {
	config         config.RabbitMQConfig
	dialer         RabbitDialer
	persister      MessagePersister
	batchPersister MessageBatchPersister
	namespace      Namespace
	retry          consumeRetryPolicy
	status         consumeStatusState
	prefetch       int
	batchSize      int
	batchWait      time.Duration

	backoff *relayBackoff
	// wait pauses between reconnect attempts; tests replace it.
	wait func(context.Context, time.Duration) bool
	logf func(string, ...any)
	// lastLoggedError suppresses repeated log lines for the same failure.
	lastLoggedError string
}

func NewRabbitMessageConsumer(
	rabbitConfig config.RabbitMQConfig,
	dialer RabbitDialer,
	persister MessagePersister,
) *RabbitMessageConsumer {
	return NewRabbitMessageConsumerWithNamespace(rabbitConfig, dialer, persister, DefaultNamespace())
}

func NewRabbitMessageConsumerWithNamespace(
	rabbitConfig config.RabbitMQConfig,
	dialer RabbitDialer,
	persister MessagePersister,
	namespace Namespace,
) *RabbitMessageConsumer {
	consumer := &RabbitMessageConsumer{
		config:    rabbitConfig,
		dialer:    dialer,
		persister: persister,
		namespace: namespace,
		retry:     defaultConsumeRetryPolicy(),
	}
	if batch, ok := persister.(MessageBatchPersister); ok {
		consumer.batchPersister = batch
	}
	return consumer
}

// Status returns a snapshot of the consumer counters for /ops/infra.
func (c *RabbitMessageConsumer) Status() ConsumerStatus {
	if c == nil {
		return ConsumerStatus{}
	}
	return c.status.snapshot()
}

// Run blocks until the context is canceled and reconnects after consumer
// channel failures. It uses manual acknowledgement and a bounded batch
// prefetch; each queue has one worker.
func (c *RabbitMessageConsumer) Run(ctx context.Context) error {
	if c == nil || c.dialer == nil || c.persister == nil {
		return ErrMessageServiceUnavailable
	}
	c.ensureDefaults()
	for {
		persistedBefore := c.status.snapshot().Persisted
		err := c.runConnection(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("RabbitMQ message consumer stopped")
		}
		if c.status.snapshot().Persisted > persistedBefore {
			c.backoff.reset()
		}
		c.status.recordError(err, time.Now())
		c.logReconnect(err)
		if !c.wait(ctx, c.backoff.next()) {
			return nil
		}
	}
}

// logReconnect logs a reconnect failure unless the identical text was already
// logged for the current failure streak.
func (c *RabbitMessageConsumer) logReconnect(err error) {
	if err.Error() == c.lastLoggedError {
		return
	}
	c.logf("direct message consumer reconnecting: %v", err)
	c.lastLoggedError = err.Error()
}

// markConnectionReady clears the reconnect log de-duplication once a fresh
// connection has declared the topology and started consuming. A later outage
// with the same error text is then logged again instead of staying silent for
// the life of the process.
func (c *RabbitMessageConsumer) markConnectionReady() {
	c.lastLoggedError = ""
}

// ensureDefaults fills collaborators omitted by a zero-value consumer.
func (c *RabbitMessageConsumer) ensureDefaults() {
	if c.backoff == nil {
		c.backoff = newRelayBackoff(relayDefaultBackoff, relayDefaultMaxBackoff)
	}
	if c.wait == nil {
		c.wait = waitMessaging
	}
	if c.logf == nil {
		c.logf = log.Printf
	}
	if c.prefetch <= 0 {
		c.prefetch = defaultConsumerPrefetch
	}
	if c.batchSize <= 0 {
		c.batchSize = defaultConsumerBatchSize
	}
	if c.prefetch < c.batchSize {
		c.prefetch = c.batchSize
	}
	if c.batchWait <= 0 {
		c.batchWait = defaultConsumerBatchWait
	}
	if c.batchPersister == nil {
		if batch, ok := c.persister.(MessageBatchPersister); ok {
			c.batchPersister = batch
		}
	}
}

func (c *RabbitMessageConsumer) runConnection(ctx context.Context) error {
	connection, err := c.dialer(c.config)
	if err != nil {
		return fmt.Errorf("dial RabbitMQ for message consumer: %w", err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ consumer channel: %w", err)
	}
	defer channel.Close()
	if err := declareDirectTopology(channel, c.namespace); err != nil {
		return err
	}
	deadLetterTopology := DeadLetterTopologyFor(c.namespace, DeadLetterDirect)
	if err := DeclareDeadLetterTopology(channel, deadLetterTopology); err != nil {
		return err
	}
	publisher, err := newDLQAMQPPublisher(channel, deadLetterTopology)
	if err != nil {
		return err
	}
	if err := channel.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("set RabbitMQ consumer qos: %w", err)
	}
	deliveries, err := channel.Consume(
		directQueueName(c.namespace),
		directConsumerName(c.namespace),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume direct messages: %w", err)
	}
	// Topology is declared and the consumer is ready: a later identical failure
	// is a new incident and must be logged again.
	c.markConnectionReady()
	return c.consumeBatches(ctx, deliveries, publisher)
}

// consumeBatches collects deliveries until the batch is full or the short
// linger expires, then commits one transaction before acknowledging them.
func (c *RabbitMessageConsumer) consumeBatches(
	ctx context.Context,
	deliveries <-chan amqp091.Delivery,
	publisher consumeDeadLetterPublisher,
) error {
	c.ensureDefaults()
	batch := make([]consumeDelivery, 0, c.batchSize)
	var timer *time.Timer
	var timerC <-chan time.Time
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerC = nil
	}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := c.handleBatch(ctx, batch, publisher)
		for range batch {
			c.status.endDelivery()
		}
		batch = batch[:0]
		stopTimer()
		return err
	}
	defer func() {
		stopTimer()
		for range batch {
			c.status.endDelivery()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("RabbitMQ delivery channel closed")
			}
			c.status.beginDelivery()
			batch = append(batch, consumeAMQPDelivery{delivery: delivery})
			if len(batch) == 1 {
				timer = time.NewTimer(c.batchWait)
				timerC = timer.C
			}
			if len(batch) >= c.batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-timerC:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func (c *RabbitMessageConsumer) handleDelivery(
	ctx context.Context,
	delivery consumeDelivery,
	publisher consumeDeadLetterPublisher,
) error {
	// The delivery is unacknowledged for the whole decode/persist/ack path, so
	// a drain must see it and wait instead of treating the queue as empty.
	c.status.beginDelivery()
	defer c.status.endDelivery()
	return c.processDelivery(ctx, delivery, publisher)
}

func (c *RabbitMessageConsumer) deliveryJob(
	delivery consumeDelivery,
	publisher consumeDeadLetterPublisher,
) consumeDeliveryJob {
	return consumeDeliveryJob{
		delivery:  delivery,
		publisher: publisher,
		kind:      DeadLetterDirect,
		retry:     c.retry,
		status:    &c.status,
	}
}

func (c *RabbitMessageConsumer) processDelivery(
	ctx context.Context,
	delivery consumeDelivery,
	publisher consumeDeadLetterPublisher,
) error {
	job := c.deliveryJob(delivery, publisher)
	message, err := decodeDirectMessage(delivery.Body())
	if err != nil {
		log.Printf("dead-letter malformed direct message delivery: %v", err)
		job.label = "malformed direct message delivery"
		return job.deadLetterNow(ctx, dlqReasonMalformed, err)
	}
	job.label = fmt.Sprintf(
		"direct message id=%d sender=%d recipient=%d seq=%d",
		message.ID, message.SenderID, message.RecipientID, message.Seq,
	)
	return job.persist(ctx, func(ctx context.Context) error {
		_, err := c.persister.PersistMessage(ctx, message)
		return err
	})
}

// handleBatch decodes a delivery batch, dead-letters malformed entries
// individually, and persists the rest in one transaction when possible.
func (c *RabbitMessageConsumer) handleBatch(
	ctx context.Context,
	deliveries []consumeDelivery,
	publisher consumeDeadLetterPublisher,
) error {
	valid := make([]consumeDelivery, 0, len(deliveries))
	messages := make([]store.Message, 0, len(deliveries))
	for _, delivery := range deliveries {
		if ctx.Err() != nil {
			return nil
		}
		message, err := decodeDirectMessage(delivery.Body())
		if err != nil {
			log.Printf("dead-letter malformed direct message delivery: %v", err)
			job := c.deliveryJob(delivery, publisher)
			job.label = "malformed direct message delivery"
			if err := job.deadLetterNow(ctx, dlqReasonMalformed, err); err != nil {
				return err
			}
			continue
		}
		valid = append(valid, delivery)
		messages = append(messages, message)
	}
	if len(messages) == 0 {
		return nil
	}
	if c.batchPersister == nil {
		return c.persistIndividually(ctx, valid, publisher)
	}
	return c.persistBatch(ctx, valid, messages, publisher)
}

// persistBatch commits one transaction for the whole batch. Transient errors
// retry the batch; anything that may be caused by one message falls back to the
// existing per-message retry/dead-letter path.
func (c *RabbitMessageConsumer) persistBatch(
	ctx context.Context,
	deliveries []consumeDelivery,
	messages []store.Message,
	publisher consumeDeadLetterPublisher,
) error {
	retry := c.retry.withDefaults()
	var transientRetries uint64
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := c.batchPersister.PersistMessages(ctx, messages)
		if err == nil {
			c.status.recordPersistedN(len(messages), retry.now())
			for _, delivery := range deliveries {
				if err := delivery.Ack(false); err != nil {
					return fmt.Errorf("ack direct message batch delivery: %w", err)
				}
			}
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		now := retry.now()
		c.status.recordError(err, now)
		if errors.Is(err, store.ErrBatchFallback) {
			log.Printf("direct message batch requires individual persistence: %v", err)
			return c.persistIndividually(ctx, deliveries, publisher)
		}
		switch classifyPersistError(err) {
		case persistErrorTransient:
			delay := retry.transientDelay(transientRetries)
			transientRetries++
			c.status.recordTransientRetryN(len(messages))
			if !retry.wait(ctx, &c.status, delay) {
				return nil
			}
		default:
			log.Printf("direct message batch failed; falling back to individual persist: %v", err)
			return c.persistIndividually(ctx, deliveries, publisher)
		}
	}
}

func (c *RabbitMessageConsumer) persistIndividually(
	ctx context.Context,
	deliveries []consumeDelivery,
	publisher consumeDeadLetterPublisher,
) error {
	for _, delivery := range deliveries {
		if ctx.Err() != nil {
			return nil
		}
		if err := c.processDelivery(ctx, delivery, publisher); err != nil {
			return err
		}
	}
	return nil
}
