package messaging

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
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
	retry     consumeRetryPolicy
	status    consumeStatusState
}

func NewGroupMessageConsumer(
	rabbitConfig config.RabbitMQConfig,
	dialer RabbitDialer,
	persister GroupMessagePersister,
	namespace Namespace,
) *GroupMessageConsumer {
	return &GroupMessageConsumer{
		config:    rabbitConfig,
		dialer:    dialer,
		persister: persister,
		namespace: namespace,
		retry:     defaultConsumeRetryPolicy(),
	}
}

// Status returns a snapshot of the consumer counters for /ops/infra.
func (c *GroupMessageConsumer) Status() ConsumerStatus {
	if c == nil {
		return ConsumerStatus{}
	}
	return c.status.snapshot()
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
	deadLetterTopology := DeadLetterTopologyFor(c.namespace, DeadLetterGroup)
	if err := DeclareDeadLetterTopology(channel, deadLetterTopology); err != nil {
		return err
	}
	publisher, err := newDLQAMQPPublisher(channel, deadLetterTopology)
	if err != nil {
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
			if err := c.handleDelivery(ctx, consumeAMQPDelivery{delivery: delivery}, publisher); err != nil {
				return err
			}
		}
	}
}

func (c *GroupMessageConsumer) handleDelivery(
	ctx context.Context,
	delivery consumeDelivery,
	publisher consumeDeadLetterPublisher,
) error {
	job := consumeDeliveryJob{
		delivery:  delivery,
		publisher: publisher,
		kind:      DeadLetterGroup,
		retry:     c.retry,
		status:    &c.status,
	}
	message, err := decodeGroupMessage(delivery.Body())
	if err != nil {
		log.Printf("dead-letter malformed group message delivery: %v", err)
		job.label = "malformed group message delivery"
		return job.deadLetterNow(ctx, dlqReasonMalformed, err)
	}
	job.label = fmt.Sprintf(
		"group message id=%d group=%d sender=%d seq=%d",
		message.ID, message.GroupID, message.SenderID, message.Seq,
	)
	return job.persist(ctx, func(ctx context.Context) error {
		_, err := c.persister.PersistGroupMessage(ctx, message)
		return err
	})
}
