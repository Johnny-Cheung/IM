package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestGroupMessageReaderReturnsNewestHotMessages(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	keyspace := defaultKeyspace

	for index := 0; index < 4; index++ {
		id := uint64(index + 1)
		if err := client.HSet(ctx, keyspace.groupMessageKey(id), map[string]interface{}{
			"id":                id,
			"group_id":          9,
			"sender_id":         1,
			"client_message_id": "group-reader-" + string(rune('a'+index)),
			"body":              "message-" + string(rune('1'+index)),
			"created_at_ms":     1700000000000 + index,
			"sender_username":   "alice",
			"sender_nickname":   "Alice",
		}).Err(); err != nil {
			t.Fatalf("seed group message: %v", err)
		}
		if err := client.ZAdd(ctx, keyspace.groupInboxKey(9), redis.Z{Score: float64(id), Member: id}).Err(); err != nil {
			t.Fatalf("seed group inbox: %v", err)
		}
	}

	reader := NewGroupMessageReader(client, keyspace)
	page, err := reader.ListGroupMessages(ctx, 1, 9, 0, 2)
	if err != nil {
		t.Fatalf("ListGroupMessages() error = %v", err)
	}
	if !page.HasMore || len(page.Messages) != 2 || page.Messages[0].ID != 3 || page.Messages[1].ID != 4 {
		t.Fatalf("page = %#v", page)
	}
	if page.Messages[0].CreatedAt.Before(time.UnixMilli(1700000000000)) {
		t.Fatal("created time was not decoded")
	}
}
