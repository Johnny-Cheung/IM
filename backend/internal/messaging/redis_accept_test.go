package messaging

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func newAcceptTest(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	seedSequenceReady(t, client)
	return client, server
}

func TestRedisAcceptorAcceptsAndDeduplicates(t *testing.T) {
	client, _ := newAcceptTest(t)
	ctx := context.Background()
	if err := client.SAdd(ctx, friendshipKey(1), 2).Err(); err != nil {
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

	first, err := acceptor.Accept(ctx, request)
	if err != nil {
		t.Fatalf("first Accept() error = %v", err)
	}
	if !first.Created || first.Message.ID != 42 || first.Message.Seq != 1 {
		t.Fatalf("first result = %#v, want created ID 42 seq 1", first)
	}
	if !first.Message.CreatedAt.Equal(createdAt) {
		t.Fatalf("created at = %v, want %v", first.Message.CreatedAt, createdAt)
	}

	body, err := client.HGet(ctx, messageKey(42), "body").Result()
	if err != nil || body != "hello" {
		t.Fatalf("stored body = %q, err = %v", body, err)
	}
	if got := client.HGet(ctx, messageKey(42), "seq").Val(); got != "1" {
		t.Fatalf("entity seq = %q, want 1", got)
	}
	if got := client.ZScore(ctx, directFeedKey(1), "42").Val(); got != 42 {
		t.Fatalf("sender feed score = %v, want 42", got)
	}
	if got := client.ZScore(ctx, directFeedKey(2), "42").Val(); got != 42 {
		t.Fatalf("recipient feed score = %v, want 42", got)
	}
	if got := client.ZScore(ctx, directPairKey(2, 1), "42").Val(); got != 1 {
		t.Fatalf("dm:pair score = %v, want seq 1", got)
	}
	if got := client.Get(ctx, directSequenceKey(1, 2)).Val(); got != "1" {
		t.Fatalf("dm:seq = %q, want 1", got)
	}
	if got := client.HGet(ctx, directConversationLastKey(1), "2").Val(); got != "42" {
		t.Fatalf("sender last message = %q, want 42", got)
	}
	if got := client.HGet(ctx, directConversationLastKey(2), "1").Val(); got != "42" {
		t.Fatalf("recipient last message = %q, want 42", got)
	}
	events, err := client.XRange(ctx, messageStreamKey(), "-", "+").Result()
	if err != nil || len(events) != 1 {
		t.Fatalf("stream events = %d, err = %v", len(events), err)
	}
	if got := events[0].Values["seq"]; got != "1" {
		t.Fatalf("stream seq = %v, want 1", got)
	}

	duplicate, err := acceptor.Accept(ctx, request)
	if err != nil {
		t.Fatalf("duplicate Accept() error = %v", err)
	}
	if duplicate.Created || duplicate.Message.ID != 42 || duplicate.Message.Seq != 1 {
		t.Fatalf("duplicate result = %#v, want existing ID 42 seq 1", duplicate)
	}
	if got := client.Get(ctx, directSequenceKey(1, 2)).Val(); got != "1" {
		t.Fatalf("dm:seq after duplicate = %q, want 1", got)
	}
	if got := client.XLen(ctx, messageStreamKey()).Val(); got != 1 {
		t.Fatalf("stream length after duplicate = %d, want 1", got)
	}

	conflictRequest := request
	conflictRequest.Body = "changed"
	if _, err := acceptor.Accept(ctx, conflictRequest); !errors.Is(err, store.ErrMessageConflict) {
		t.Fatalf("conflict error = %v, want ErrMessageConflict", err)
	}
}

func TestRedisAcceptorUsesOneSequenceForBothDirections(t *testing.T) {
	client, _ := newAcceptTest(t)
	ctx := context.Background()
	if err := client.SAdd(ctx, friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	if err := client.SAdd(ctx, friendshipKey(2), 1).Err(); err != nil {
		t.Fatalf("seed reverse friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{})
	sends := []DirectMessageRequest{
		{SenderID: 1, RecipientID: 2, ClientMessageID: "a", Body: "1"},
		{SenderID: 2, RecipientID: 1, ClientMessageID: "b", Body: "2"},
		{SenderID: 1, RecipientID: 2, ClientMessageID: "c", Body: "3"},
	}
	for index, request := range sends {
		result, err := acceptor.Accept(ctx, request)
		if err != nil {
			t.Fatalf("accept %d: %v", index, err)
		}
		if result.Message.Seq != uint64(index+1) {
			t.Fatalf("message %d seq = %d, want %d", index, result.Message.Seq, index+1)
		}
	}
	if got := client.ZCard(ctx, directPairKey(1, 2)).Val(); got != 3 {
		t.Fatalf("dm:pair size = %d, want 3", got)
	}
	if got := client.Get(ctx, directSequenceKey(2, 1)).Val(); got != "3" {
		t.Fatalf("dm:seq = %q, want 3", got)
	}
}

func TestRedisAcceptorConcurrentSequencesHaveNoGapsOrDuplicates(t *testing.T) {
	client, _ := newAcceptTest(t)
	ctx := context.Background()
	if err := client.SAdd(ctx, friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{})

	const total = 20
	seqs := make(chan uint64, total)
	var wait sync.WaitGroup
	for index := 0; index < total; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			result, err := acceptor.Accept(ctx, DirectMessageRequest{
				SenderID:        1,
				RecipientID:     2,
				ClientMessageID: fmt.Sprintf("concurrent-%d", index),
				Body:            "hello",
			})
			if err != nil {
				t.Errorf("accept %d: %v", index, err)
				return
			}
			seqs <- result.Message.Seq
		}(index)
	}
	wait.Wait()
	close(seqs)

	collected := make([]uint64, 0, total)
	for seq := range seqs {
		collected = append(collected, seq)
	}
	sort.Slice(collected, func(i, j int) bool { return collected[i] < collected[j] })
	if len(collected) != total {
		t.Fatalf("accepted = %d, want %d", len(collected), total)
	}
	for index, seq := range collected {
		if seq != uint64(index+1) {
			t.Fatalf("seqs = %v, want 1..%d without gaps", collected, total)
		}
	}
	if got := client.Get(ctx, directSequenceKey(1, 2)).Val(); got != fmt.Sprint(total) {
		t.Fatalf("dm:seq = %q, want %d", got, total)
	}
}

func TestRedisAcceptorRejectsBeforeAllocating(t *testing.T) {
	client, _ := newAcceptTest(t)
	ctx := context.Background()
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{SeedFloor: 9})
	request := DirectMessageRequest{
		SenderID:        1,
		RecipientID:     2,
		ClientMessageID: "client-002",
		Body:            "hello",
	}
	if _, err := acceptor.Accept(ctx, request); !errors.Is(err, store.ErrNotFriends) {
		t.Fatalf("not-friends error = %v, want ErrNotFriends", err)
	}
	if got := client.Exists(ctx, counterKey()).Val(); got != 0 {
		t.Fatalf("global counter exists after rejected message = %d, want 0", got)
	}
	if got := client.Exists(ctx, directSequenceKey(1, 2)).Val(); got != 0 {
		t.Fatalf("dm:seq exists after rejected message = %d, want 0", got)
	}

	request.RecipientID = request.SenderID
	if _, err := acceptor.Accept(ctx, request); !errors.Is(err, store.ErrCannotMessageSelf) {
		t.Fatalf("self-message error = %v, want ErrCannotMessageSelf", err)
	}
}

func TestRedisAcceptorRequiresSequenceReady(t *testing.T) {
	client, server := newAcceptTest(t)
	ctx := context.Background()
	if err := client.SAdd(ctx, friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{})
	request := DirectMessageRequest{
		SenderID: 1, RecipientID: 2, ClientMessageID: "ready", Body: "hello",
	}

	if !server.Del(sequenceReadyKey()) {
		t.Fatal("remove readiness failed")
	}
	if _, err := acceptor.Accept(ctx, request); !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("missing seq:ready error = %v, want ErrSeqNotReady", err)
	}

	seedSequenceReady(t, client)
	gate := NewSeqGate()
	acceptor = NewRedisAcceptor(client, RedisAcceptorOptions{Gate: gate})
	gate.Close()
	if _, err := acceptor.Accept(ctx, request); !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("closed gate error = %v, want ErrSeqNotReady", err)
	}
	if !errors.Is(ErrSeqNotReady, ErrMessageServiceUnavailable) {
		t.Fatal("ErrSeqNotReady must wrap ErrMessageServiceUnavailable")
	}
	gate.Open()
	if _, err := acceptor.Accept(ctx, request); err != nil {
		t.Fatalf("open gate accept: %v", err)
	}
}

func TestRedisAcceptorRejectsLostCounterWithNonEmptyIndex(t *testing.T) {
	client, server := newAcceptTest(t)
	ctx := context.Background()
	if err := client.SAdd(ctx, friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{})
	if _, err := acceptor.Accept(ctx, DirectMessageRequest{
		SenderID: 1, RecipientID: 2, ClientMessageID: "first", Body: "hello",
	}); err != nil {
		t.Fatalf("accept first: %v", err)
	}
	if !server.Del(directSequenceKey(1, 2)) {
		t.Fatal("delete counter failed")
	}
	_, err := acceptor.Accept(ctx, DirectMessageRequest{
		SenderID: 1, RecipientID: 2, ClientMessageID: "second", Body: "hello",
	})
	if !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("lost counter error = %v, want ErrSeqNotReady", err)
	}
}

func TestRedisAcceptorRejectsSequenceOverflow(t *testing.T) {
	client, _ := newAcceptTest(t)
	ctx := context.Background()
	if err := client.SAdd(ctx, friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	setDirectSequence(t, client, 1, 2, store.MaxSafeSeq)
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{})
	_, err := acceptor.Accept(ctx, DirectMessageRequest{
		SenderID: 1, RecipientID: 2, ClientMessageID: "overflow", Body: "hello",
	})
	if !errors.Is(err, ErrSeqOverflow) {
		t.Fatalf("overflow error = %v, want ErrSeqOverflow", err)
	}
	if got := client.Exists(ctx, counterKey()).Val(); got != 0 {
		t.Fatalf("global counter consumed on seq overflow = %d, want 0", got)
	}
}

func TestRedisAcceptorDuplicateWithoutEntitySeqIsUnavailable(t *testing.T) {
	client, _ := newAcceptTest(t)
	ctx := context.Background()
	if err := client.SAdd(ctx, friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{})
	request := DirectMessageRequest{
		SenderID: 1, RecipientID: 2, ClientMessageID: "legacy", Body: "hello",
	}
	first, err := acceptor.Accept(ctx, request)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if err := client.HDel(ctx, messageKey(first.Message.ID), "seq").Err(); err != nil {
		t.Fatalf("remove entity seq: %v", err)
	}
	if _, err := acceptor.Accept(ctx, request); !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("legacy duplicate error = %v, want ErrSeqNotReady", err)
	}
}

func TestRedisAcceptorKeepsIndexAndEntitiesAligned(t *testing.T) {
	client, _ := newAcceptTest(t)
	ctx := context.Background()
	if err := client.SAdd(ctx, friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{})

	const total = 12
	for index := 1; index <= total; index++ {
		result, err := acceptor.Accept(ctx, DirectMessageRequest{
			SenderID:        1,
			RecipientID:     2,
			ClientMessageID: fmt.Sprintf("client-%03d", index),
			Body:            "hello",
		})
		if err != nil {
			t.Fatalf("accept message %d: %v", index, err)
		}
		if result.Message.Seq != uint64(index) {
			t.Fatalf("seq %d = %d", index, result.Message.Seq)
		}
	}

	if got := client.ZCard(ctx, directFeedKey(1)).Val(); got != total {
		t.Fatalf("sender feed size = %d, want %d", got, total)
	}
	if got := client.ZCard(ctx, directFeedKey(2)).Val(); got != total {
		t.Fatalf("recipient feed size = %d, want %d", got, total)
	}
	if got := client.ZCard(ctx, directPairKey(1, 2)).Val(); got != total {
		t.Fatalf("dm:pair size = %d, want %d", got, total)
	}
	for seq := uint64(1); seq <= total; seq++ {
		if got := client.Exists(ctx, messageKey(seq)).Val(); got != 1 {
			t.Fatalf("entity %d missing for indexed message", seq)
		}
		if ttl := client.TTL(ctx, messageKey(seq)).Val(); ttl != -1 {
			t.Fatalf("entity %d ttl = %v, want no expiry", seq, ttl)
		}
	}
}

func TestRedisAcceptorRejectsIDOverflow(t *testing.T) {
	client, _ := newAcceptTest(t)
	ctx := context.Background()
	if err := client.SAdd(ctx, friendshipKey(1), 2).Err(); err != nil {
		t.Fatalf("seed friendship: %v", err)
	}
	acceptor := NewRedisAcceptor(client, RedisAcceptorOptions{
		SeedFloor: store.MaxSafeMessageID,
	})
	_, err := acceptor.Accept(ctx, DirectMessageRequest{
		SenderID:        1,
		RecipientID:     2,
		ClientMessageID: "client-003",
		Body:            "hello",
	})
	if !errors.Is(err, store.ErrMessageIDExhausted) {
		t.Fatalf("overflow error = %v, want ErrMessageIDExhausted", err)
	}
}
