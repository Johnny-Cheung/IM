package messaging

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func TestRedisAcceptorAcceptsAndDeduplicates(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	if err := client.SAdd(context.Background(), friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{
		SeedFloor:    41,
		DedupTTL:     time.Hour,
		StreamMaxLen: 10,
	})
	createdAt := time.UnixMilli(1700000000123).UTC()
	request := DirectMessageRequest{
		SenderID:        1,
		RecipientID:     2,
		ClientMessageID: "client-001",
		Body:            "hello",
		CreatedAt:       createdAt,
	}

	first, err := acceptor.Accept(context.Background(), request)
	if err != nil {
		t.Fatalf("first Accept() error = %v", err)
	}
	if !first.Created || first.Message.ID != 42 {
		t.Fatalf("first result = %#v, want created ID 42", first)
	}
	if !first.Message.CreatedAt.Equal(createdAt) {
		t.Fatalf("created at = %v, want %v", first.Message.CreatedAt, createdAt)
	}

	body, err := client.HGet(context.Background(), messageKey(42), "body").Result()
	if err != nil || body != "hello" {
		t.Fatalf("stored body = %q, err = %v", body, err)
	}
	if got := client.ZScore(context.Background(), directFeedKey(1), "42").Val(); got != 42 {
		t.Fatalf("sender feed score = %v, want 42", got)
	}
	if got := client.ZScore(context.Background(), directFeedKey(2), "42").Val(); got != 42 {
		t.Fatalf("recipient feed score = %v, want 42", got)
	}
	if got := client.ZScore(context.Background(), directConversationKey(2, 1), "42").Val(); got != 42 {
		t.Fatalf("conversation index score = %v, want 42", got)
	}
	if got := client.HGet(context.Background(), directConversationLastKey(1), "2").Val(); got != "42" {
		t.Fatalf("sender last message = %q, want 42", got)
	}
	if got := client.HGet(context.Background(), directConversationLastKey(2), "1").Val(); got != "42" {
		t.Fatalf("recipient last message = %q, want 42", got)
	}
	if got := client.XLen(context.Background(), messageStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length = %d, want 1", got)
	}

	duplicate, err := acceptor.Accept(context.Background(), request)
	if err != nil {
		t.Fatalf("duplicate Accept() error = %v", err)
	}
	if duplicate.Created || duplicate.Message.ID != 42 {
		t.Fatalf("duplicate result = %#v, want existing ID 42", duplicate)
	}
	if got := client.XLen(context.Background(), messageStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after duplicate = %d, want 1", got)
	}

	conflictRequest := request
	conflictRequest.Body = "changed"
	if _, err := acceptor.Accept(context.Background(), conflictRequest); !errors.Is(err, store.ErrMessageConflict) {
		t.Fatalf("conflict error = %v, want ErrMessageConflict", err)
	}
}

func TestRedisAcceptorRejectsBeforeAllocatingID(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{SeedFloor: 9})
	request := DirectMessageRequest{
		SenderID:        1,
		RecipientID:     2,
		ClientMessageID: "client-002",
		Body:            "hello",
	}
	if _, err := acceptor.Accept(context.Background(), request); !errors.Is(err, store.ErrNotFriends) {
		t.Fatalf("not-friends error = %v, want ErrNotFriends", err)
	}
	if got := client.Exists(context.Background(), counterKey()).Val(); got != 0 {
		t.Fatalf("counter exists after rejected message = %d, want 0", got)
	}

	request.RecipientID = request.SenderID
	if _, err := acceptor.Accept(context.Background(), request); !errors.Is(err, store.ErrCannotMessageSelf) {
		t.Fatalf("self-message error = %v, want ErrCannotMessageSelf", err)
	}
}

func TestRedisAcceptorKeepsIndexAndEntitiesAligned(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	ctx := context.Background()
	if err := client.SAdd(ctx, friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{})

	const total = 12
	for index := 1; index <= total; index++ {
		if _, err := acceptor.Accept(ctx, DirectMessageRequest{
			SenderID:        1,
			RecipientID:     2,
			ClientMessageID: fmt.Sprintf("client-%03d", index),
			Body:            "hello",
		}); err != nil {
			t.Fatalf("accept message %d: %v", index, err)
		}
	}

	if got := client.ZCard(ctx, directFeedKey(1)).Val(); got != total {
		t.Fatalf("sender feed size = %d, want %d (indexes must not be trimmed)", got, total)
	}
	if got := client.ZCard(ctx, directFeedKey(2)).Val(); got != total {
		t.Fatalf("recipient feed size = %d, want %d (indexes must not be trimmed)", got, total)
	}
	if got := client.ZCard(ctx, directConversationKey(2, 1)).Val(); got != total {
		t.Fatalf("conversation index size = %d, want %d (indexes must not be trimmed)", got, total)
	}
	for id := uint64(1); id <= total; id++ {
		if got := client.Exists(ctx, messageKey(id)).Val(); got != 1 {
			t.Fatalf("entity %d missing for indexed message", id)
		}
		if ttl := client.TTL(ctx, messageKey(id)).Val(); ttl != -1 {
			t.Fatalf("entity %d ttl = %v, want no expiry", id, ttl)
		}
	}
}

func TestRedisAcceptorRejectsIDOverflow(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	if err := client.SAdd(context.Background(), friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{
		SeedFloor: store.MaxSafeMessageID,
	})
	_, err := acceptor.Accept(context.Background(), DirectMessageRequest{
		SenderID:        1,
		RecipientID:     2,
		ClientMessageID: "client-003",
		Body:            "hello",
	})
	if !errors.Is(err, store.ErrMessageIDExhausted) {
		t.Fatalf("overflow error = %v, want ErrMessageIDExhausted", err)
	}
}
