package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func seedDirectMessages(
	t *testing.T,
	client *redis.Client,
	senderID, recipientID uint64,
	count int,
) []uint64 {
	t.Helper()
	ctx := context.Background()
	if err := client.SAdd(ctx, friendshipKey(senderID), recipientID).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{SeedFloor: 10})
	ids := make([]uint64, 0, count)
	for index := 0; index < count; index++ {
		result, err := acceptor.Accept(ctx, DirectMessageRequest{
			SenderID:        senderID,
			RecipientID:     recipientID,
			ClientMessageID: "client-" + string(rune('a'+index)),
			Body:            "message-" + string(rune('1'+index)),
			CreatedAt:       time.UnixMilli(int64(1700000000000 + index)),
		})
		if err != nil {
			t.Fatalf("accept message %d: %v", index, err)
		}
		ids = append(ids, result.Message.ID)
	}
	return ids
}

func TestRedisMessageReaderListsHotConversationMessages(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	seedDirectMessages(t, client, 1, 2, 2)

	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
	page, err := reader.ReadConversation(ctx, 2, 1, 0, 10)
	if err != nil {
		t.Fatalf("ReadConversation() error = %v", err)
	}
	if !page.Complete {
		t.Fatal("complete = false, want true without an epoch boundary")
	}
	if page.Page.HasMore || len(page.Page.Messages) != 2 {
		t.Fatalf("page = %#v, want two messages without more", page.Page)
	}
	if page.Page.Messages[0].Body != "message-1" || page.Page.Messages[1].Body != "message-2" {
		t.Fatalf("messages = %#v", page.Page.Messages)
	}

	after, err := reader.ReadConversationAfter(ctx, 2, 1, page.Page.Messages[0].ID, 10)
	if err != nil {
		t.Fatalf("ReadConversationAfter() error = %v", err)
	}
	if !after.Complete || len(after.Page.Messages) != 1 || after.Page.Messages[0].Body != "message-2" {
		t.Fatalf("after page = %#v", after)
	}
}

func TestRedisMessageReaderInitialPageReturnsNewestMessages(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	seedDirectMessages(t, client, 1, 2, 5)

	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
	page, err := reader.ReadConversation(ctx, 2, 1, 0, 2)
	if err != nil {
		t.Fatalf("ReadConversation() error = %v", err)
	}
	if !page.Complete || !page.Page.HasMore || len(page.Page.Messages) != 2 {
		t.Fatalf("page = %#v, want newest two messages with more", page)
	}
	if page.Page.Messages[0].Body != "message-4" || page.Page.Messages[1].Body != "message-5" {
		t.Fatalf("messages = %#v, want message-4/message-5", page.Page.Messages)
	}
}

func TestRedisMessageReaderIsolatesConversations(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	seedDirectMessages(t, client, 1, 2, 2)
	seedDirectMessages(t, client, 3, 2, 3)

	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
	page, err := reader.ReadConversation(ctx, 2, 1, 0, 10)
	if err != nil {
		t.Fatalf("ReadConversation() error = %v", err)
	}
	if len(page.Page.Messages) != 2 {
		t.Fatalf("messages = %d, want only the 2 messages from user 1", len(page.Page.Messages))
	}
	for _, message := range page.Page.Messages {
		if message.SenderID != 1 || message.RecipientID != 2 {
			t.Fatalf("message = %#v, leaked from another conversation", message)
		}
	}
}

func TestRedisMessageReaderFallsBackBelowEpoch(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	ids := seedDirectMessages(t, client, 1, 2, 2)

	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, ids[0])
	page, err := reader.ReadConversation(ctx, 2, 1, 0, 10)
	if err != nil {
		t.Fatalf("ReadConversation() error = %v", err)
	}
	if len(page.Page.Messages) != 1 {
		t.Fatalf("messages = %d, want only ids above the epoch", len(page.Page.Messages))
	}
	if page.Complete {
		t.Fatal("complete = true, want fallback because older messages may exist below the epoch")
	}

	after, err := reader.ReadConversationAfter(ctx, 2, 1, ids[0], 10)
	if err != nil {
		t.Fatalf("ReadConversationAfter() error = %v", err)
	}
	if !after.Complete {
		t.Fatal("complete = false, want the tail above the epoch to be complete")
	}

	below, err := reader.ReadConversationAfter(ctx, 2, 1, 1, 10)
	if err != nil {
		t.Fatalf("ReadConversationAfter(below epoch) error = %v", err)
	}
	if below.Complete {
		t.Fatal("complete = true, want fallback for a cursor below the epoch")
	}
}

func TestRedisMessageReaderMissingEntityForcesFallback(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	ids := seedDirectMessages(t, client, 1, 2, 2)
	if err := client.Del(ctx, messageKey(ids[0])).Err(); err != nil {
		t.Fatalf("delete entity: %v", err)
	}

	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
	page, err := reader.ReadConversation(ctx, 2, 1, 0, 10)
	if err != nil {
		t.Fatalf("ReadConversation() error = %v", err)
	}
	if page.Complete {
		t.Fatal("complete = true, want fallback when an indexed entity is missing")
	}
}
