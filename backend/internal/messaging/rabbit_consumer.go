package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/rabbitmq/amqp091-go"
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
}

func NewRabbitMessageConsumer(
	rabbitConfig config.RabbitMQConfig,
	dialer RabbitDialer,
	persister MessagePersister,
) *RabbitMessageConsumer {
	return &RabbitMessageConsumer{
		config:    rabbitConfig,
		dialer:    dialer,
		persister: persister,
	}
}

// Run blocks until the context is canceled and reconnects after consumer
// channel failures. It uses manual acknowledgement and prefetch one.
func (c *RabbitMessageConsumer) Run(ctx context.Context) error {
	if c == nil || c.dialer == nil || c.persister == nil {
		return ErrMessageServiceUnavailable
	}
	for {
		err := c.runConnection(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("RabbitMQ message consumer stopped")
		}
		if !waitMessaging(ctx, time.Second) {
			return nil
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
	if err := declareDirectTopology(channel); err != nil {
		return err
	}
	if err := channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("set RabbitMQ consumer qos: %w", err)
	}
	deliveries, err := channel.Consume(
		directQueueName,
		directConsumerName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume direct messages: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("RabbitMQ delivery channel closed")
			}
			if err := c.handleDelivery(ctx, delivery); err != nil {
				return err
			}
		}
	}
}

func (c *RabbitMessageConsumer) handleDelivery(ctx context.Context, delivery amqp091.Delivery) error {
	message, err := decodeDirectMessage(delivery.Body)
	if err != nil {
		// A malformed message cannot be repaired by retrying. Ack it so a
		// poison message cannot block the basic v1.2 queue forever.
		return delivery.Ack(false)
	}
	if _, err := c.persister.PersistMessage(ctx, message); err != nil {
		if errors.Is(err, store.ErrMessageConflict) {
			return delivery.Ack(false)
		}
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return fmt.Errorf("nack message %d after persist error: %v (persist error: %w)", message.ID, nackErr, err)
		}
		return nil
	}
	return delivery.Ack(false)
}
