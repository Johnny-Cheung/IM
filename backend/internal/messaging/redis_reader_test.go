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
	seedSequenceReady(t, client)
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{SeedFloor: 10})
	ids := make([]uint64, 0, count)
	for index := 0; index < count; index++ {
		result, err := acceptor.Accept(ctx, DirectMessageRequest{
			SenderID:        senderID,
			RecipientID:     recipientID,
			ClientMessageID: "client-" + string(rune('a'+index)) + string(rune('a'+senderID)),
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
		t.Fatal("complete = false, want true")
	}
	if page.Page.HasMore || len(page.Page.Messages) != 2 {
		t.Fatalf("page = %#v, want two messages without more", page.Page)
	}
	if page.Page.Messages[0].Seq != 1 || page.Page.Messages[1].Seq != 2 {
		t.Fatalf("seqs = %#v", page.Page.Messages)
	}
	if page.Page.Messages[0].Body != "message-1" || page.Page.Messages[1].Body != "message-2" {
		t.Fatalf("messages = %#v", page.Page.Messages)
	}

	after, err := reader.ReadConversationAfter(ctx, 2, 1, page.Page.Messages[0].Seq, 10)
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
	if page.Page.Messages[0].Seq != 4 || page.Page.Messages[1].Seq != 5 {
		t.Fatalf("seqs = %#v, want 4 and 5", page.Page.Messages)
	}
}

func TestRedisMessageReaderHasMoreUsesRangeBoundaries(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	seedDirectMessages(t, client, 1, 2, 5)
	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)

	before, err := reader.ReadConversation(ctx, 2, 1, 4, 2)
	if err != nil {
		t.Fatalf("ReadConversation(before) error = %v", err)
	}
	if !before.Page.HasMore || len(before.Page.Messages) != 2 ||
		before.Page.Messages[0].Seq != 2 || before.Page.Messages[1].Seq != 3 {
		t.Fatalf("before page = %#v", before.Page)
	}

	oldest, err := reader.ReadConversation(ctx, 2, 1, 3, 2)
	if err != nil {
		t.Fatalf("ReadConversation(oldest) error = %v", err)
	}
	if oldest.Page.HasMore || len(oldest.Page.Messages) != 2 ||
		oldest.Page.Messages[0].Seq != 1 || oldest.Page.Messages[1].Seq != 2 {
		t.Fatalf("oldest page = %#v, want seqs 1 and 2 without more", oldest.Page)
	}

	// A limit=1 cursor walk must advance: the start of each page is the
	// exclusive before_seq of the next one.
	page, err := reader.ReadConversation(ctx, 2, 1, 3, 1)
	if err != nil {
		t.Fatalf("ReadConversation(limit 1) error = %v", err)
	}
	if !page.Page.HasMore || len(page.Page.Messages) != 1 || page.Page.Messages[0].Seq != 2 {
		t.Fatalf("limit 1 page = %#v, want seq 2 with more", page.Page)
	}
	page, err = reader.ReadConversation(ctx, 2, 1, page.Page.Messages[0].Seq, 1)
	if err != nil {
		t.Fatalf("ReadConversation(limit 1 cursor) error = %v", err)
	}
	if page.Page.HasMore || len(page.Page.Messages) != 1 || page.Page.Messages[0].Seq != 1 {
		t.Fatalf("limit 1 cursor page = %#v, want seq 1 without more", page.Page)
	}

	tail, err := reader.ReadConversationAfter(ctx, 2, 1, 3, 2)
	if err != nil {
		t.Fatalf("ReadConversationAfter(tail) error = %v", err)
	}
	if tail.Page.HasMore || len(tail.Page.Messages) != 2 ||
		tail.Page.Messages[0].Seq != 4 || tail.Page.Messages[1].Seq != 5 {
		t.Fatalf("tail page = %#v", tail.Page)
	}

	middle, err := reader.ReadConversationAfter(ctx, 2, 1, 1, 2)
	if err != nil {
		t.Fatalf("ReadConversationAfter(middle) error = %v", err)
	}
	if !middle.Page.HasMore || len(middle.Page.Messages) != 2 {
		t.Fatalf("middle page = %#v, want more", middle.Page)
	}

	caughtUp, err := reader.ReadConversationAfter(ctx, 2, 1, 5, 2)
	if err != nil {
		t.Fatalf("ReadConversationAfter(caught up) error = %v", err)
	}
	if len(caughtUp.Page.Messages) != 0 || !caughtUp.Complete || caughtUp.Page.HasMore {
		t.Fatalf("caught up page = %#v", caughtUp)
	}
}

func TestRedisMessageReaderAfterZeroStartsAtFirstSeq(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	seedDirectMessages(t, client, 1, 2, 5)
	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
	page, err := reader.ReadConversationAfter(ctx, 2, 1, 0, 2)
	if err != nil {
		t.Fatalf("ReadConversationAfter(0) error = %v", err)
	}
	if !page.Complete || !page.Page.HasMore || len(page.Page.Messages) != 2 ||
		page.Page.Messages[0].Seq != 1 || page.Page.Messages[1].Seq != 2 {
		t.Fatalf("page = %#v, want seqs 1 and 2 with more", page)
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

func TestRedisMessageReaderConversationIgnoresFeedEpoch(t *testing.T) {
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
	if !page.Complete || len(page.Page.Messages) != 2 {
		t.Fatalf("page = %#v, want the full seq range regardless of the feed epoch", page)
	}
}

func TestRedisMessageReaderReadsFeed(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	ids := seedDirectMessages(t, client, 1, 2, 2)

	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
	feed, err := reader.ReadFeed(ctx, 2, 0, 10)
	if err != nil {
		t.Fatalf("ReadFeed() error = %v", err)
	}
	if !feed.Complete || len(feed.Page.Messages) != 2 {
		t.Fatalf("feed = %#v, want both messages and complete", feed)
	}
	if feed.Page.Messages[0].Seq != 1 || feed.Page.Messages[1].Seq != 2 {
		t.Fatalf("feed seqs = %#v", feed.Page.Messages)
	}

	tail, err := reader.ReadFeed(ctx, 2, ids[0], 10)
	if err != nil {
		t.Fatalf("ReadFeed(after) error = %v", err)
	}
	if !tail.Complete || len(tail.Page.Messages) != 1 || tail.Page.Messages[0].ID != ids[1] {
		t.Fatalf("tail = %#v", tail)
	}
}

func TestRedisMessageReaderFeedBelowEpochIsIncomplete(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	ids := seedDirectMessages(t, client, 1, 2, 2)

	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, ids[0])
	feed, err := reader.ReadFeed(ctx, 2, 1, 10)
	if err != nil {
		t.Fatalf("ReadFeed() error = %v", err)
	}
	if feed.Complete {
		t.Fatal("complete = true, want false below the epoch")
	}
}

func TestRedisMessageReaderReturnsLastConversationMessages(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	ids := seedDirectMessages(t, client, 1, 2, 3)

	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
	recipientView, err := reader.LastConversationMessages(ctx, 2)
	if err != nil {
		t.Fatalf("LastConversationMessages() error = %v", err)
	}
	if got := recipientView[1]; got.ID != ids[2] || got.Seq != 3 {
		t.Fatalf("recipient last message = %#v, want %d seq 3", got, ids[2])
	}

	senderView, err := reader.LastConversationMessages(ctx, 1)
	if err != nil {
		t.Fatalf("LastConversationMessages(sender) error = %v", err)
	}
	if got := senderView[2]; got.ID != ids[2] || got.Seq != 3 {
		t.Fatalf("sender last message = %#v, want %d seq 3", got, ids[2])
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
	if len(page.Page.Messages) != 1 || page.Page.Messages[0].Seq != 2 {
		t.Fatalf("messages = %#v, want only the resolvable seq", page.Page.Messages)
	}
}

func TestRedisMessageReaderMissingIndexEntryForcesFallback(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	ids := seedDirectMessages(t, client, 1, 2, 3)
	if err := client.ZRem(ctx, directPairKey(1, 2), ids[1]).Err(); err != nil {
		t.Fatalf("remove index entry: %v", err)
	}

	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
	page, err := reader.ReadConversation(ctx, 2, 1, 0, 10)
	if err != nil {
		t.Fatalf("ReadConversation() error = %v", err)
	}
	if page.Complete {
		t.Fatal("complete = true, want false when a seq is missing from the index")
	}
}

func TestRedisMessageReaderEntityWithoutSeqIsMissing(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	ids := seedDirectMessages(t, client, 1, 2, 2)
	if err := client.HDel(ctx, messageKey(ids[1]), "seq").Err(); err != nil {
		t.Fatalf("remove entity seq: %v", err)
	}

	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
	page, err := reader.ReadConversation(ctx, 2, 1, 0, 10)
	if err != nil {
		t.Fatalf("ReadConversation() error = %v", err)
	}
	if page.Complete {
		t.Fatal("complete = true, want false when an entity lacks seq")
	}
}

func TestRedisMessageReaderMissingCounterForcesFallback(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	seedDirectMessages(t, client, 1, 2, 2)
	if err := client.Del(ctx, directSequenceKey(1, 2)).Err(); err != nil {
		t.Fatalf("delete counter: %v", err)
	}

	reader := NewRedisMessageReaderWithKeyspace(client, defaultKeyspace, 0)
	page, err := reader.ReadConversation(ctx, 2, 1, 0, 10)
	if err != nil {
		t.Fatalf("ReadConversation() error = %v", err)
	}
	if page.Complete {
		t.Fatal("complete = true, want fallback when the counter is missing")
	}
	if len(page.Page.Messages) != 0 {
		t.Fatalf("messages = %#v, want an empty hot page", page.Page.Messages)
	}
}

func TestReadDirectSequenceCounters(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	seedDirectMessages(t, client, 1, 2, 3)
	counters, err := ReadDirectSequenceCounters(
		ctx, client, defaultKeyspace, 1, []uint64{2, 3},
	)
	if err != nil {
		t.Fatalf("ReadDirectSequenceCounters() error = %v", err)
	}
	if counters[2] != 3 {
		t.Fatalf("counter for peer 2 = %d, want 3", counters[2])
	}
	if counters[3] != 0 {
		t.Fatalf("counter for peer 3 = %d, want 0", counters[3])
	}
}
