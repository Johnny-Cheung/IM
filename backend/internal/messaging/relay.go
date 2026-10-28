package messaging

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type RabbitDialer func(config.RabbitMQConfig) (*amqp091.Connection, error)

// StreamRelay moves accepted Redis Stream messages into RabbitMQ.
type StreamRelay struct {
	redis     redis.UniversalClient
	config    config.RabbitMQConfig
	dialer    RabbitDialer
	keyspace  Keyspace
	namespace Namespace
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
	return &StreamRelay{redis: client, config: rabbitConfig, dialer: dialer, keyspace: keyspace, namespace: namespace}
}

// Run blocks until the context is canceled. It reconnects after basic
// connection or publish failures; pending-entry takeover is intentionally
// deferred to the reliability version.
func (r *StreamRelay) Run(ctx context.Context) error {
	if r == nil || r.redis == nil || r.dialer == nil {
		return ErrMessageServiceUnavailable
	}
	for {
		err := r.runConnection(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("stream relay stopped")
		}
		if !waitMessaging(ctx, time.Second) {
			return nil
		}
	}
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
	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}
	confirms := channel.NotifyPublish(make(chan amqp091.Confirmation, 1))
	if err := ensureRelayGroup(ctx, r.redis, r.keyspace, r.namespace); err != nil {
		return err
	}

	for {
		streams, err := r.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    directRelayGroup(r.namespace),
			Consumer: directConsumerName(r.namespace),
			Streams:  []string{r.keyspace.messageStreamKey(), ">"},
			Count:    10,
			Block:    2 * time.Second,
		}).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return fmt.Errorf("read Redis message stream: %w", err)
		}
		for _, stream := range streams {
			for _, entry := range stream.Messages {
				if err := r.relayEntry(ctx, channel, confirms, entry); err != nil {
					return err
				}
			}
		}
	}
}

func (r *StreamRelay) relayEntry(
	ctx context.Context,
	channel *amqp091.Channel,
	confirms <-chan amqp091.Confirmation,
	entry redis.XMessage,
) error {
	message, err := streamMessage(entry.Values)
	if err != nil {
		// Entries are produced by our own Lua script. A malformed entry cannot
		// be retried successfully, so acknowledge and remove it after logging.
		log.Printf("drop malformed direct stream entry %s: %v", entry.ID, err)
		if ackErr := r.redis.XAck(ctx, r.keyspace.messageStreamKey(), directRelayGroup(r.namespace), entry.ID).Err(); ackErr != nil {
			return fmt.Errorf("ack malformed stream entry %s: %w", entry.ID, ackErr)
		}
		if delErr := r.redis.XDel(ctx, r.keyspace.messageStreamKey(), entry.ID).Err(); delErr != nil {
			return fmt.Errorf("delete malformed stream entry %s: %w", entry.ID, delErr)
		}
		return nil
	}
	payload, err := encodeDirectMessage(message)
	if err != nil {
		return err
	}
	if err := channel.PublishWithContext(ctx, directExchangeName(r.namespace), directRoutingKey, false, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    fmt.Sprintf("%d", message.ID),
		Body:         payload,
	}); err != nil {
		return fmt.Errorf("publish message %d: %w", message.ID, err)
	}
	select {
	case confirmation := <-confirms:
		if !confirmation.Ack {
			return fmt.Errorf("RabbitMQ rejected message %d", message.ID)
		}
	case <-ctx.Done():
		return nil
	}
	if err := r.redis.XAck(ctx, r.keyspace.messageStreamKey(), directRelayGroup(r.namespace), entry.ID).Err(); err != nil {
		return fmt.Errorf("ack stream message %s: %w", entry.ID, err)
	}
	if err := r.redis.XDel(ctx, r.keyspace.messageStreamKey(), entry.ID).Err(); err != nil {
		return fmt.Errorf("delete stream message %s: %w", entry.ID, err)
	}
	return nil
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
