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

// GroupMessagePersister is the MySQL side of the group RabbitMQ consumer.
type GroupMessagePersister interface {
	PersistGroupMessage(context.Context, store.GroupMessage) (bool, error)
}

// GroupMessageConsumer persists group RabbitMQ deliveries.
type GroupMessageConsumer struct {
	config    config.RabbitMQConfig
	dialer    RabbitDialer
	persister GroupMessagePersister
	namespace Namespace
}

func NewGroupMessageConsumer(
	rabbitConfig config.RabbitMQConfig,
	dialer RabbitDialer,
	persister GroupMessagePersister,
	namespace Namespace,
) *GroupMessageConsumer {
	return &GroupMessageConsumer{config: rabbitConfig, dialer: dialer, persister: persister, namespace: namespace}
}

func (c *GroupMessageConsumer) Run(ctx context.Context) error {
	if c == nil || c.dialer == nil || c.persister == nil {
		return ErrMessageServiceUnavailable
	}
	for {
		err := c.runConnection(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("group RabbitMQ consumer stopped")
		}
		if !waitMessaging(ctx, time.Second) {
			return nil
		}
	}
}

func (c *GroupMessageConsumer) runConnection(ctx context.Context) error {
	connection, err := c.dialer(c.config)
	if err != nil {
		return fmt.Errorf("dial RabbitMQ for group message consumer: %w", err)
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ group consumer channel: %w", err)
	}
	defer channel.Close()
	if err := declareGroupTopology(channel, c.namespace); err != nil {
		return err
	}
	if err := channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("set RabbitMQ group consumer qos: %w", err)
	}
	deliveries, err := channel.Consume(
		groupQueueName(c.namespace),
		groupConsumerName(c.namespace),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume group messages: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("RabbitMQ group delivery channel closed")
			}
			if err := c.handleDelivery(ctx, delivery); err != nil {
				return err
			}
		}
	}
}

func (c *GroupMessageConsumer) handleDelivery(ctx context.Context, delivery amqp091.Delivery) error {
	message, err := decodeGroupMessage(delivery.Body)
	if err != nil {
		return delivery.Ack(false)
	}
	if _, err := c.persister.PersistGroupMessage(ctx, message); err != nil {
		if errors.Is(err, store.ErrMessageConflict) {
			return delivery.Ack(false)
		}
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return fmt.Errorf("nack group message %d after persist error: %v (persist error: %w)", message.ID, nackErr, err)
		}
		return nil
	}
	return delivery.Ack(false)
}
