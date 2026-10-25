package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestGroupRedisAcceptorAcceptsDeduplicatesAndRejects(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	projection := NewGroupProjection(client, defaultKeyspace)
	if err := projection.Replace(ctx, 9, []uint64{1, 2}, false); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	acceptor := NewGroupRedisAcceptor(client, defaultKeyspace, RedisAcceptorOptions{SeedFloor: 10})
	request := GroupMessageRequest{
		SenderID:        1,
		GroupID:         9,
		ClientMessageID: "group-client-1",
		Body:            "hello group",
		CreatedAt:       time.UnixMilli(1700000000000),
		SenderUsername:  "alice",
		SenderNickname:  "Alice",
	}
	accepted, err := acceptor.Accept(ctx, request)
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	if !accepted.Created || accepted.Message.ID != 11 {
		t.Fatalf("accepted = %#v, want created ID 11", accepted)
	}

	duplicate, err := acceptor.Accept(ctx, request)
	if err != nil {
		t.Fatalf("duplicate Accept() error = %v", err)
	}
	if duplicate.Created || duplicate.Message.ID != accepted.Message.ID {
		t.Fatalf("duplicate = %#v, want same ID and Created=false", duplicate)
	}

	request.Body = "different"
	if _, err := acceptor.Accept(ctx, request); err != store.ErrMessageConflict {
		t.Fatalf("conflict error = %v, want %v", err, store.ErrMessageConflict)
	}

	nonMember := request
	nonMember.Body = "outside"
	nonMember.ClientMessageID = "group-client-2"
	nonMember.SenderID = 3
	if _, err := acceptor.Accept(ctx, nonMember); err != store.ErrGroupNotFound {
		t.Fatalf("non-member error = %v, want %v", err, store.ErrGroupNotFound)
	}
	current, err := client.Get(ctx, counterKey()).Uint64()
	if err != nil {
		t.Fatalf("counter = %v", err)
	}
	if current != 11 {
		t.Fatalf("counter = %d, want 11 after rejected request", current)
	}
}
