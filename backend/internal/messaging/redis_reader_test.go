package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisMessageReaderListsHotConversationMessages(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	if err := client.SAdd(ctx, friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{SeedFloor: 10})
	for index, body := range []string{"first", "second"} {
		result, err := acceptor.Accept(ctx, DirectMessageRequest{
			SenderID:        1,
			RecipientID:     2,
			ClientMessageID: "client-reader-" + string(rune('a'+index)),
			Body:            body,
			CreatedAt:       time.UnixMilli(int64(1700000000000 + index)),
		})
		if err != nil {
			t.Fatalf("Accept(%q) error = %v", body, err)
		}
		if !result.Created {
			t.Fatalf("Accept(%q) created = false", body)
		}
	}

	reader := NewRedisMessageReader(client)
	page, err := reader.ListMessages(ctx, 2, 1, 0, 10)
	if err != nil {
		t.Fatalf("ListMessages() error = %v", err)
	}
	if page.HasMore || len(page.Messages) != 2 {
		t.Fatalf("page = %#v, want two messages without more", page)
	}
	if page.Messages[0].Body != "first" || page.Messages[1].Body != "second" {
		t.Fatalf("messages = %#v", page.Messages)
	}

	after, err := reader.ListMessagesAfter(ctx, 2, 1, page.Messages[0].ID, 10)
	if err != nil {
		t.Fatalf("ListMessagesAfter() error = %v", err)
	}
	if len(after.Messages) != 1 || after.Messages[0].Body != "second" {
		t.Fatalf("after page = %#v", after)
	}
}
