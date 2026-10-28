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

// GroupStreamRelay moves accepted group messages into the group RabbitMQ queue.
type GroupStreamRelay struct {
	redis     redis.UniversalClient
	config    config.RabbitMQConfig
	dialer    RabbitDialer
	keyspace  Keyspace
	namespace Namespace
}

func NewGroupStreamRelay(
	client redis.UniversalClient,
	rabbitConfig config.RabbitMQConfig,
	dialer RabbitDialer,
	keyspace Keyspace,
	namespace Namespace,
) *GroupStreamRelay {
	return &GroupStreamRelay{redis: client, config: rabbitConfig, dialer: dialer, keyspace: keyspace, namespace: namespace}
}

func (r *GroupStreamRelay) Run(ctx context.Context) error {
	if r == nil || r.redis == nil || r.dialer == nil {
		return ErrMessageServiceUnavailable
	}
	for {
		err := r.runConnection(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("group stream relay stopped")
		}
		if !waitMessaging(ctx, time.Second) {
			return nil
		}
	}
}

func (r *GroupStreamRelay) runConnection(ctx context.Context) error {
	connection, err := r.dialer(r.config)
	if err != nil {
		return fmt.Errorf("dial RabbitMQ for group stream relay: %w", err)
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ group relay channel: %w", err)
	}
	defer channel.Close()
	if err := declareGroupTopology(channel, r.namespace); err != nil {
		return err
	}
	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable RabbitMQ group publisher confirms: %w", err)
	}
	confirms := channel.NotifyPublish(make(chan amqp091.Confirmation, 1))
	if err := ensureGroupRelayGroup(ctx, r.redis, r.keyspace, r.namespace); err != nil {
		return err
	}
	for {
		streams, err := r.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupRelayName(r.namespace),
			Consumer: groupConsumerName(r.namespace),
			Streams:  []string{r.keyspace.groupMessageStreamKey(), ">"},
			Count:    10,
			Block:    2 * time.Second,
		}).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return fmt.Errorf("read Redis group message stream: %w", err)
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

func (r *GroupStreamRelay) relayEntry(
	ctx context.Context,
	channel *amqp091.Channel,
	confirms <-chan amqp091.Confirmation,
	entry redis.XMessage,
) error {
	message, err := groupStreamMessage(entry.Values)
	if err != nil {
		log.Printf("drop malformed group stream entry %s: %v", entry.ID, err)
		if ackErr := r.redis.XAck(ctx, r.keyspace.groupMessageStreamKey(), groupRelayName(r.namespace), entry.ID).Err(); ackErr != nil {
			return fmt.Errorf("ack malformed group stream entry %s: %w", entry.ID, ackErr)
		}
		if delErr := r.redis.XDel(ctx, r.keyspace.groupMessageStreamKey(), entry.ID).Err(); delErr != nil {
			return fmt.Errorf("delete malformed group stream entry %s: %w", entry.ID, delErr)
		}
		return nil
	}
	payload, err := encodeGroupMessage(message)
	if err != nil {
		return err
	}
	if err := channel.PublishWithContext(ctx, groupExchangeName(r.namespace), groupRoutingKey(), false, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    groupMessageIDString(message.ID),
		Body:         payload,
	}); err != nil {
		return fmt.Errorf("publish group message %d: %w", message.ID, err)
	}
	select {
	case confirmation := <-confirms:
		if !confirmation.Ack {
			return fmt.Errorf("RabbitMQ rejected group message %d", message.ID)
		}
	case <-ctx.Done():
		return nil
	}
	if err := r.redis.XAck(ctx, r.keyspace.groupMessageStreamKey(), groupRelayName(r.namespace), entry.ID).Err(); err != nil {
		return fmt.Errorf("ack group stream message %s: %w", entry.ID, err)
	}
	if err := r.redis.XDel(ctx, r.keyspace.groupMessageStreamKey(), entry.ID).Err(); err != nil {
		return fmt.Errorf("delete group stream message %s: %w", entry.ID, err)
	}
	return nil
}

func ensureGroupRelayGroup(ctx context.Context, client redis.UniversalClient, keyspace Keyspace, namespace Namespace) error {
	err := client.XGroupCreateMkStream(ctx, keyspace.groupMessageStreamKey(), groupRelayName(namespace), "0-0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create Redis group relay group: %w", err)
	}
	return nil
}

func declareGroupTopology(channel *amqp091.Channel, namespace Namespace) error {
	if err := channel.ExchangeDeclare(groupExchangeName(namespace), "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare group message exchange: %w", err)
	}
	if _, err := channel.QueueDeclare(groupQueueName(namespace), true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare group message queue: %w", err)
	}
	if err := channel.QueueBind(groupQueueName(namespace), groupRoutingKey(), groupExchangeName(namespace), false, nil); err != nil {
		return fmt.Errorf("bind group message queue: %w", err)
	}
	return nil
}
