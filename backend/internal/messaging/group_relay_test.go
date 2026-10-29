package messaging

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
)

func newGroupRelayTest(env relayTestEnv) *GroupStreamRelay {
	relay := NewGroupStreamRelay(
		env.client,
		config.RabbitMQConfig{},
		nil,
		defaultKeyspace,
		DefaultNamespace(),
	)
	relay.block = 10 * time.Millisecond
	return relay
}

func relayAddGroupEntry(t *testing.T, client *redis.Client, messageID, seq uint64) {
	t.Helper()
	_, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: defaultKeyspace.groupMessageStreamKey(),
		Values: map[string]interface{}{
			"id":                strconv.FormatUint(messageID, 10),
			"seq":               strconv.FormatUint(seq, 10),
			"group_id":          "9",
			"sender_id":         "1",
			"client_message_id": "client-" + strconv.FormatUint(messageID, 10),
			"body":              "hello",
			"created_at_ms":     "1700000000000",
			"sender_username":   "alice",
			"sender_nickname":   "Alice",
		},
	}).Result()
	if err != nil {
		t.Fatalf("add group stream entry: %v", err)
	}
}

func relayGroupPendingCount(t *testing.T, client *redis.Client) int64 {
	t.Helper()
	return relayPendingCount(t, client, defaultKeyspace.groupMessageStreamKey(), groupRelayName(DefaultNamespace()))
}

func TestGroupStreamRelayRecoversPendingEntriesBeforeNewOnes(t *testing.T) {
	env := newRelayTestEnv(t)
	ctx := context.Background()
	if err := ensureGroupRelayGroup(ctx, env.client, defaultKeyspace, DefaultNamespace()); err != nil {
		t.Fatalf("ensure group relay group: %v", err)
	}
	relayAddGroupEntry(t, env.client, 401, 1)
	relayAddGroupEntry(t, env.client, 402, 2)
	if _, err := env.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupRelayName(DefaultNamespace()),
		Consumer: groupConsumerName(DefaultNamespace()),
		Streams:  []string{defaultKeyspace.groupMessageStreamKey(), ">"},
		Count:    10,
	}).Result(); err != nil {
		t.Fatalf("seed pending entries: %v", err)
	}
	relayAddGroupEntry(t, env.client, 403, 3)

	relay := newGroupRelayTest(env)
	publisher := newFakeRelayPublisher()
	runRelayUntil(t, relay.runPublisher, publisher, publisher.notified, 3, relayStreamDrained(env.client, defaultKeyspace.groupMessageStreamKey()))

	calls := publisher.snapshot()
	wantIDs := []string{"401", "402", "403"}
	if len(calls) != len(wantIDs) {
		t.Fatalf("publishes = %d, want %d", len(calls), len(wantIDs))
	}
	for index, wantID := range wantIDs {
		if calls[index].messageID != wantID {
			t.Fatalf("publish %d message ID = %q, want %q", index, calls[index].messageID, wantID)
		}
		if calls[index].exchange != groupExchangeName(DefaultNamespace()) ||
			calls[index].routingKey != groupRoutingKey() {
			t.Fatalf("publish %d destination = %q/%q", index, calls[index].exchange, calls[index].routingKey)
		}
	}
	if got := env.client.XLen(ctx, defaultKeyspace.groupMessageStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
	if got := relayGroupPendingCount(t, env.client); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	status := relay.Status()
	if status.Relayed != 3 || status.Recovered != 2 {
		t.Fatalf("status relayed/recovered = %d/%d, want 3/2", status.Relayed, status.Recovered)
	}
}

func TestGroupStreamRelayKeepsEntryPendingOnPublishFailure(t *testing.T) {
	env := newRelayTestEnv(t)
	ctx := context.Background()
	if err := ensureGroupRelayGroup(ctx, env.client, defaultKeyspace, DefaultNamespace()); err != nil {
		t.Fatalf("ensure group relay group: %v", err)
	}
	relayAddGroupEntry(t, env.client, 404, 1)

	relay := newGroupRelayTest(env)
	publisher := newFakeRelayPublisher()
	publisher.failWith(errors.New("broker unavailable"))
	if err := relay.runPublisher(ctx, publisher); err == nil {
		t.Fatal("runPublisher() error = nil, want publish failure")
	}
	if got := env.client.XLen(ctx, defaultKeyspace.groupMessageStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after failure = %d, want 1", got)
	}
	if got := relayGroupPendingCount(t, env.client); got != 1 {
		t.Fatalf("pending after failure = %d, want 1", got)
	}

	next := newFakeRelayPublisher()
	runRelayUntil(t, relay.runPublisher, next, next.notified, 1, relayStreamDrained(env.client, defaultKeyspace.groupMessageStreamKey()))
	calls := next.snapshot()
	if len(calls) != 1 || calls[0].messageID != "404" {
		t.Fatalf("republished = %#v, want message 404", calls)
	}
	if got := env.client.XLen(ctx, defaultKeyspace.groupMessageStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length after recovery = %d, want 0", got)
	}
	if got := relay.Status().Recovered; got != 1 {
		t.Fatalf("recovered = %d, want 1", got)
	}
}

func TestGroupStreamRelayKeepsEntryPendingOnNack(t *testing.T) {
	env := newRelayTestEnv(t)
	ctx := context.Background()
	if err := ensureGroupRelayGroup(ctx, env.client, defaultKeyspace, DefaultNamespace()); err != nil {
		t.Fatalf("ensure group relay group: %v", err)
	}
	relayAddGroupEntry(t, env.client, 405, 1)

	confirms := make(chan amqp091.Confirmation, 1)
	confirms <- amqp091.Confirmation{DeliveryTag: 1, Ack: false}
	relay := newGroupRelayTest(env)
	err := relay.runPublisher(ctx, &confirmRelayPublisher{confirms: confirms})
	if err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("runPublisher() error = %v, want rejection", err)
	}
	if got := env.client.XLen(ctx, defaultKeyspace.groupMessageStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after nack = %d, want 1", got)
	}
	if got := relayGroupPendingCount(t, env.client); got != 1 {
		t.Fatalf("pending after nack = %d, want 1", got)
	}
}

func TestGroupStreamRelayKeepsEntryPendingWhenShutdownAwaitsConfirm(t *testing.T) {
	env := newRelayTestEnv(t)
	if err := ensureGroupRelayGroup(context.Background(), env.client, defaultKeyspace, DefaultNamespace()); err != nil {
		t.Fatalf("ensure group relay group: %v", err)
	}
	relayAddGroupEntry(t, env.client, 406, 1)

	relay := newGroupRelayTest(env)
	publisher := newBlockingRelayPublisher()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.runPublisher(ctx, publisher) }()
	select {
	case <-publisher.started:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not publish")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not stop after cancel")
	}
	if got := env.client.XLen(context.Background(), defaultKeyspace.groupMessageStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after shutdown = %d, want 1", got)
	}
	if got := relayGroupPendingCount(t, env.client); got != 1 {
		t.Fatalf("pending after shutdown = %d, want 1", got)
	}
	if got := relay.Status().Relayed; got != 0 {
		t.Fatalf("relayed after shutdown = %d, want 0", got)
	}
}

func TestGroupStreamRelayDropsMalformedPendingEntry(t *testing.T) {
	env := newRelayTestEnv(t)
	ctx := context.Background()
	if err := ensureGroupRelayGroup(ctx, env.client, defaultKeyspace, DefaultNamespace()); err != nil {
		t.Fatalf("ensure group relay group: %v", err)
	}
	if _, err := env.client.XAdd(ctx, &redis.XAddArgs{
		Stream: defaultKeyspace.groupMessageStreamKey(),
		Values: map[string]interface{}{"id": "407"},
	}).Result(); err != nil {
		t.Fatalf("add malformed entry: %v", err)
	}
	if _, err := env.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupRelayName(DefaultNamespace()),
		Consumer: groupConsumerName(DefaultNamespace()),
		Streams:  []string{defaultKeyspace.groupMessageStreamKey(), ">"},
		Count:    10,
	}).Result(); err != nil {
		t.Fatalf("seed malformed pending entry: %v", err)
	}

	relay := newGroupRelayTest(env)
	publisher := newFakeRelayPublisher()
	if err := relay.drainPending(ctx, publisher); err != nil {
		t.Fatalf("drainPending() error = %v", err)
	}
	if got := len(publisher.snapshot()); got != 0 {
		t.Fatalf("malformed entry published %d times, want 0", got)
	}
	if got := env.client.XLen(ctx, defaultKeyspace.groupMessageStreamKey()).Val(); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
	if got := relayGroupPendingCount(t, env.client); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
}
