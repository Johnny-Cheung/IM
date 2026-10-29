package messaging

import (
	"context"
	"errors"
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

	status  *relayStatusTracker
	backoff *relayBackoff
	block   time.Duration
	// connect runs one connection attempt; tests replace it.
	connect func(context.Context) error
	// wait pauses between reconnect attempts; tests replace it.
	wait func(context.Context, time.Duration) bool
}

func NewGroupStreamRelay(
	client redis.UniversalClient,
	rabbitConfig config.RabbitMQConfig,
	dialer RabbitDialer,
	keyspace Keyspace,
	namespace Namespace,
) *GroupStreamRelay {
	relay := &GroupStreamRelay{
		redis:     client,
		config:    rabbitConfig,
		dialer:    dialer,
		keyspace:  keyspace,
		namespace: namespace,
		status:    &relayStatusTracker{},
		backoff:   newRelayBackoff(relayDefaultBackoff, relayDefaultMaxBackoff),
		block:     relayDefaultBlock,
		wait:      waitMessaging,
	}
	relay.connect = relay.runConnection
	return relay
}

// Run blocks until the context is canceled. It reconnects with exponential
// backoff after a failed connection or publish; each new connection first
// redelivers this consumer's pending entries.
func (r *GroupStreamRelay) Run(ctx context.Context) error {
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
			err = fmt.Errorf("group stream relay stopped")
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

func (r *GroupStreamRelay) ensureDefaults() {
	if r.status == nil {
		r.status = &relayStatusTracker{}
	}
	if r.backoff == nil {
		r.backoff = newRelayBackoff(relayDefaultBackoff, relayDefaultMaxBackoff)
	}
	if r.block <= 0 {
		r.block = relayDefaultBlock
	}
	if r.connect == nil {
		r.connect = r.runConnection
	}
	if r.wait == nil {
		r.wait = waitMessaging
	}
}

// Status returns a snapshot for /ops/infra.
func (r *GroupStreamRelay) Status() RelayStatus {
	if r == nil || r.status == nil {
		return RelayStatus{}
	}
	return r.status.snapshot()
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
	publisher, err := newAMQPRelayPublisher(channel)
	if err != nil {
		return err
	}
	if err := ensureGroupRelayGroup(ctx, r.redis, r.keyspace, r.namespace); err != nil {
		return err
	}
	r.status.setConnected(true)
	defer r.status.setConnected(false)
	return r.runPublisher(ctx, publisher)
}

// runPublisher drains this consumer's pending entries, then relays new ones.
func (r *GroupStreamRelay) runPublisher(ctx context.Context, publisher relayPublisher) error {
	if err := r.drainPending(ctx, publisher); err != nil {
		return err
	}
	for {
		streams, err := r.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupRelayName(r.namespace),
			Consumer: groupConsumerName(r.namespace),
			Streams:  []string{r.keyspace.groupMessageStreamKey(), ">"},
			Count:    10,
			Block:    r.block,
		}).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read Redis group message stream: %w", err)
		}
		if err := r.relayStreams(ctx, publisher, streams, false); err != nil {
			return err
		}
	}
}

func (r *GroupStreamRelay) drainPending(ctx context.Context, publisher relayPublisher) error {
	for {
		streams, err := r.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupRelayName(r.namespace),
			Consumer: groupConsumerName(r.namespace),
			Streams:  []string{r.keyspace.groupMessageStreamKey(), "0"},
			Count:    100,
		}).Result()
		if errors.Is(err, redis.Nil) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read pending group stream messages: %w", err)
		}
		if relayStreamCount(streams) == 0 {
			return nil
		}
		if err := r.relayStreams(ctx, publisher, streams, true); err != nil {
			return err
		}
	}
}

func (r *GroupStreamRelay) relayStreams(
	ctx context.Context,
	publisher relayPublisher,
	streams []redis.XStream,
	recovered bool,
) error {
	for _, stream := range streams {
		for _, entry := range stream.Messages {
			if err := r.relayEntry(ctx, publisher, entry, recovered); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *GroupStreamRelay) relayEntry(
	ctx context.Context,
	publisher relayPublisher,
	entry redis.XMessage,
	recovered bool,
) error {
	message, err := groupStreamMessage(entry.Values)
	if err != nil {
		log.Printf("drop malformed group stream entry %s: %v", entry.ID, err)
		return r.ackAndDelete(ctx, entry.ID)
	}
	payload, err := encodeGroupMessage(message)
	if err != nil {
		return err
	}
	err = publisher.Publish(ctx, groupExchangeName(r.namespace), groupRoutingKey(), groupMessageIDString(message.ID), payload)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("relay group message %d: %w", message.ID, err)
	}
	if err := r.ackAndDelete(ctx, entry.ID); err != nil {
		return err
	}
	r.status.recordRelayed(recovered)
	return nil
}

func (r *GroupStreamRelay) ackAndDelete(ctx context.Context, entryID string) error {
	if err := r.redis.XAck(ctx, r.keyspace.groupMessageStreamKey(), groupRelayName(r.namespace), entryID).Err(); err != nil {
		return fmt.Errorf("ack group stream message %s: %w", entryID, err)
	}
	if err := r.redis.XDel(ctx, r.keyspace.groupMessageStreamKey(), entryID).Err(); err != nil {
		return fmt.Errorf("delete group stream message %s: %w", entryID, err)
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
