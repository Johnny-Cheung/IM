package messaging

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// MessagePersister is the MySQL side of the RabbitMQ consumer.
type MessagePersister interface {
	PersistMessage(context.Context, store.Message) (bool, error)
}

// RabbitMessageConsumer persists RabbitMQ direct-message deliveries.
type RabbitMessageConsumer struct {
	config    config.RabbitMQConfig
	dialer    RabbitDialer
	persister MessagePersister
	namespace Namespace
	retry     consumeRetryPolicy
	status    consumeStatusState

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
	return &RabbitMessageConsumer{
		config:    rabbitConfig,
		dialer:    dialer,
		persister: persister,
		namespace: namespace,
		retry:     defaultConsumeRetryPolicy(),
	}
}

// Status returns a snapshot of the consumer counters for /ops/infra.
func (c *RabbitMessageConsumer) Status() ConsumerStatus {
	if c == nil {
		return ConsumerStatus{}
	}
	return c.status.snapshot()
}

// Run blocks until the context is canceled and reconnects after consumer
// channel failures. It uses manual acknowledgement and prefetch one.
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
	if err := channel.Qos(1, 0, false); err != nil {
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

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("RabbitMQ delivery channel closed")
			}
			if err := c.handleDelivery(ctx, consumeAMQPDelivery{delivery: delivery}, publisher); err != nil {
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
	job := consumeDeliveryJob{
		delivery:  delivery,
		publisher: publisher,
		kind:      DeadLetterDirect,
		retry:     c.retry,
		status:    &c.status,
	}
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
