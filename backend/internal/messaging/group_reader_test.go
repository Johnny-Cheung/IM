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

	seedGroupMessages(t, client, 9, map[uint64]uint64{1: 1, 2: 2, 3: 3, 4: 4})
	setGroupSequence(t, client, 9, 4)

	reader := NewGroupMessageReader(client, keyspace)
	page, err := reader.ReadGroup(ctx, 1, 9, 0, 2)
	if err != nil {
		t.Fatalf("ReadGroup() error = %v", err)
	}
	if !page.Complete {
		t.Fatal("complete = false, want true")
	}
	if !page.Page.HasMore || len(page.Page.Messages) != 2 ||
		page.Page.Messages[0].Seq != 3 || page.Page.Messages[1].Seq != 4 {
		t.Fatalf("page = %#v, want seqs 3 and 4", page.Page)
	}
	if page.Page.Messages[0].CreatedAt.Before(time.UnixMilli(1700000000000)) {
		t.Fatal("created time was not decoded")
	}

	after, err := reader.ReadGroupAfter(ctx, 1, 9, 2, 10)
	if err != nil {
		t.Fatalf("ReadGroupAfter() error = %v", err)
	}
	if !after.Complete || len(after.Page.Messages) != 2 ||
		after.Page.Messages[0].Seq != 3 || after.Page.Messages[1].Seq != 4 {
		t.Fatalf("after page = %#v", after.Page)
	}
}

func TestGroupMessageReaderMissingEntityForcesFallback(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	seedGroupMessages(t, client, 9, map[uint64]uint64{1: 1, 2: 2})
	setGroupSequence(t, client, 9, 2)
	if err := client.Del(ctx, defaultKeyspace.groupMessageKey(1)).Err(); err != nil {
		t.Fatalf("delete entity: %v", err)
	}

	reader := NewGroupMessageReader(client, defaultKeyspace)
	page, err := reader.ReadGroup(ctx, 1, 9, 0, 10)
	if err != nil {
		t.Fatalf("ReadGroup() error = %v", err)
	}
	if page.Complete {
		t.Fatal("complete = true, want fallback when an entity is missing")
	}
	if len(page.Page.Messages) != 1 || page.Page.Messages[0].Seq != 2 {
		t.Fatalf("messages = %#v", page.Page.Messages)
	}
}

func TestGroupMessageReaderBeforeSeqIsExclusive(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	messages := make(map[uint64]uint64, 7)
	for seq := uint64(1); seq <= 7; seq++ {
		messages[100+seq] = seq
	}
	seedGroupMessages(t, client, 9, messages)
	setGroupSequence(t, client, 9, 7)

	reader := NewGroupMessageReader(client, defaultKeyspace)
	page, err := reader.ReadGroup(ctx, 1, 9, 7, 30)
	if err != nil {
		t.Fatalf("ReadGroup() error = %v", err)
	}
	if !page.Complete || page.Page.HasMore || len(page.Page.Messages) != 6 ||
		page.Page.Messages[0].Seq != 1 || page.Page.Messages[5].Seq != 6 {
		t.Fatalf("page = %#v, want seqs 1..6 without more", page)
	}

	first, err := reader.ReadGroup(ctx, 1, 9, 7, 1)
	if err != nil {
		t.Fatalf("ReadGroup(limit 1) error = %v", err)
	}
	if !first.Page.HasMore || len(first.Page.Messages) != 1 ||
		first.Page.Messages[0].Seq != 6 {
		t.Fatalf("limit 1 page = %#v, want seq 6 with more", first.Page)
	}
	second, err := reader.ReadGroup(ctx, 1, 9, first.Page.Messages[0].Seq, 1)
	if err != nil {
		t.Fatalf("ReadGroup(limit 1 cursor) error = %v", err)
	}
	if !second.Page.HasMore || len(second.Page.Messages) != 1 ||
		second.Page.Messages[0].Seq != 5 {
		t.Fatalf("limit 1 cursor page = %#v, want seq 5", second.Page)
	}

	empty, err := reader.ReadGroup(ctx, 1, 9, 1, 10)
	if err != nil {
		t.Fatalf("ReadGroup(before first) error = %v", err)
	}
	if !empty.Complete || empty.Page.HasMore || len(empty.Page.Messages) != 0 {
		t.Fatalf("empty page = %#v, want empty complete", empty)
	}
}

func TestGroupMessageReaderMissingCounterForcesFallback(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	seedGroupMessages(t, client, 9, map[uint64]uint64{1: 1})
	reader := NewGroupMessageReader(client, defaultKeyspace)
	page, err := reader.ReadGroup(ctx, 1, 9, 0, 10)
	if err != nil {
		t.Fatalf("ReadGroup() error = %v", err)
	}
	if page.Complete {
		t.Fatal("complete = true, want fallback when the group counter is missing")
	}
	if len(page.Page.Messages) != 0 {
		t.Fatalf("messages = %#v, want an empty hot page", page.Page.Messages)
	}
}

func TestReadGroupSequenceCounters(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()

	setGroupSequence(t, client, 9, 7)
	counters, err := ReadGroupSequenceCounters(
		ctx, client, defaultKeyspace, []uint64{9, 10},
	)
	if err != nil {
		t.Fatalf("ReadGroupSequenceCounters() error = %v", err)
	}
	if counters[9] != 7 || counters[10] != 0 {
		t.Fatalf("counters = %#v, want 9:7 10:0", counters)
	}
}
