package messaging

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func newGroupAcceptTest(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	seedSequenceReady(t, client)
	return client, server
}

func seedGroupProjection(t *testing.T, client *redis.Client, groupID uint64, members []uint64) {
	t.Helper()
	projection := NewGroupProjection(client, defaultKeyspace)
	if err := projection.Replace(context.Background(), groupID, members, false); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
}

func TestGroupRedisAcceptorAcceptsDeduplicatesAndRejects(t *testing.T) {
	client, _ := newGroupAcceptTest(t)
	ctx := context.Background()
	seedGroupProjection(t, client, 9, []uint64{1, 2})

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
	if !accepted.Created || accepted.Message.ID != 11 || accepted.Message.Seq != 1 {
		t.Fatalf("accepted = %#v, want created ID 11 seq 1", accepted)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupInboxKey(9), "11").Val(); got != 1 {
		t.Fatalf("inbox score = %v, want seq 1", got)
	}
	if got := client.Get(ctx, defaultKeyspace.groupSequenceKey(9)).Val(); got != "1" {
		t.Fatalf("group:seq = %q, want 1", got)
	}
	if got := client.HGet(ctx, defaultKeyspace.groupMessageKey(11), "seq").Val(); got != "1" {
		t.Fatalf("entity seq = %q, want 1", got)
	}

	duplicate, err := acceptor.Accept(ctx, request)
	if err != nil {
		t.Fatalf("duplicate Accept() error = %v", err)
	}
	if duplicate.Created || duplicate.Message.ID != accepted.Message.ID || duplicate.Message.Seq != 1 {
		t.Fatalf("duplicate = %#v, want same ID and seq", duplicate)
	}
	if got := client.Get(ctx, defaultKeyspace.groupSequenceKey(9)).Val(); got != "1" {
		t.Fatalf("group:seq after duplicate = %q, want 1", got)
	}

	request.Body = "different"
	if _, err := acceptor.Accept(ctx, request); !errors.Is(err, store.ErrMessageConflict) {
		t.Fatalf("conflict error = %v, want %v", err, store.ErrMessageConflict)
	}

	nonMember := request
	nonMember.Body = "outside"
	nonMember.ClientMessageID = "group-client-2"
	nonMember.SenderID = 3
	if _, err := acceptor.Accept(ctx, nonMember); !errors.Is(err, store.ErrGroupNotFound) {
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

func TestGroupRedisAcceptorNeverTrimsMessageStream(t *testing.T) {
	// A behavioral check with a few entries cannot prove trimming is off: any
	// MAXLEN above three would still pass. Assert on the script text instead.
	if strings.Contains(groupAcceptScriptSource, "MAXLEN") {
		t.Fatalf("group_accept.lua trims the message stream:\n%s", groupAcceptScriptSource)
	}
	client, _ := newGroupAcceptTest(t)
	ctx := context.Background()
	seedGroupProjection(t, client, 9, []uint64{1, 2})
	acceptor := NewGroupRedisAcceptor(client, defaultKeyspace, RedisAcceptorOptions{})
	for index := 1; index <= 3; index++ {
		if _, err := acceptor.Accept(ctx, GroupMessageRequest{
			SenderID:        1,
			GroupID:         9,
			ClientMessageID: fmt.Sprintf("client-%d", index),
			Body:            "hello",
			SenderUsername:  "alice",
			SenderNickname:  "Alice",
		}); err != nil {
			t.Fatalf("accept %d: %v", index, err)
		}
	}
	if got := client.XLen(ctx, defaultKeyspace.groupMessageStreamKey()).Val(); got != 3 {
		t.Fatalf("stream length = %d, want 3 without trimming", got)
	}
}

func TestGroupRedisAcceptorConcurrentSequencesHaveNoGaps(t *testing.T) {
	client, _ := newGroupAcceptTest(t)
	ctx := context.Background()
	seedGroupProjection(t, client, 9, []uint64{1, 2})
	acceptor := NewGroupRedisAcceptor(client, defaultKeyspace, RedisAcceptorOptions{})

	const total = 15
	seqs := make(chan uint64, total)
	var wait sync.WaitGroup
	for index := 0; index < total; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			result, err := acceptor.Accept(ctx, GroupMessageRequest{
				SenderID:        1,
				GroupID:         9,
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
	for index, seq := range collected {
		if seq != uint64(index+1) {
			t.Fatalf("seqs = %v, want 1..%d", collected, total)
		}
	}
}

// TestGroupRedisAcceptorFailsClosedWhenMessageIDTaken mirrors the direct case:
// a reused global ID must never overwrite an existing group message entity.
func TestGroupRedisAcceptorFailsClosedWhenMessageIDTaken(t *testing.T) {
	client, _ := newGroupAcceptTest(t)
	ctx := context.Background()
	seedGroupProjection(t, client, 9, []uint64{1})
	const takenID = 42
	if err := client.HSet(ctx, defaultKeyspace.groupMessageKey(takenID), map[string]interface{}{
		"id": takenID, "body": "existing", "seq": 9,
	}).Err(); err != nil {
		t.Fatalf("seed existing entity: %v", err)
	}
	acceptor := NewGroupRedisAcceptor(
		client, defaultKeyspace, RedisAcceptorOptions{SeedFloor: takenID - 1},
	)
	_, err := acceptor.Accept(ctx, GroupMessageRequest{
		SenderID: 1, GroupID: 9, ClientMessageID: "taken-id", Body: "new body",
	})
	if !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("taken ID error = %v, want ErrSeqNotReady", err)
	}
	if got := client.HGet(ctx, defaultKeyspace.groupMessageKey(takenID), "body").Val(); got != "existing" {
		t.Fatalf("existing entity overwritten: body = %q", got)
	}
	if got := client.Get(ctx, counterKey()).Val(); got != "42" {
		t.Fatalf("global counter = %q, want consumed 42", got)
	}
	if got := client.Exists(ctx, defaultKeyspace.groupSequenceKey(9)).Val(); got != 0 {
		t.Fatalf("group:seq written after rejected ID = %d, want 0", got)
	}
	if got := client.ZCard(ctx, defaultKeyspace.groupInboxKey(9)).Val(); got != 0 {
		t.Fatalf("inbox written after rejected ID = %d, want 0", got)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupMessageStreamKey()).Val(); got != 0 {
		t.Fatalf("group stream written after rejected ID = %d, want 0", got)
	}
	if got := client.Exists(ctx, defaultKeyspace.groupDedupKey(9, 1, "taken-id")).Val(); got != 0 {
		t.Fatalf("group dedup written after rejected ID = %d, want 0", got)
	}
}

// TestGroupRedisAcceptorFailsClosedWhenSequenceTaken mirrors the direct case:
// a live seq in the inbox stops the accept before the counter is advanced.
func TestGroupRedisAcceptorFailsClosedWhenSequenceTaken(t *testing.T) {
	client, _ := newGroupAcceptTest(t)
	ctx := context.Background()
	seedGroupProjection(t, client, 9, []uint64{1})
	setGroupSequence(t, client, 9, 3)
	if err := client.ZAdd(ctx, defaultKeyspace.groupInboxKey(9), redis.Z{
		Score: 4, Member: "900",
	}).Err(); err != nil {
		t.Fatalf("seed inbox: %v", err)
	}
	acceptor := NewGroupRedisAcceptor(client, defaultKeyspace, RedisAcceptorOptions{})
	_, err := acceptor.Accept(ctx, GroupMessageRequest{
		SenderID: 1, GroupID: 9, ClientMessageID: "taken-seq", Body: "hello",
	})
	if !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("taken seq error = %v, want ErrSeqNotReady", err)
	}
	if got := client.Get(ctx, defaultKeyspace.groupSequenceKey(9)).Val(); got != "3" {
		t.Fatalf("group:seq = %q, want unchanged 3", got)
	}
	if got := client.Get(ctx, counterKey()).Val(); got != "1" {
		t.Fatalf("global counter = %q, want consumed 1", got)
	}
	if got := client.Exists(ctx, defaultKeyspace.groupMessageKey(1)).Val(); got != 0 {
		t.Fatalf("entity written after rejected seq = %d, want 0", got)
	}
	if got := client.ZCard(ctx, defaultKeyspace.groupInboxKey(9)).Val(); got != 1 {
		t.Fatalf("inbox size = %d, want only the seeded member", got)
	}
	if got := client.ZScore(ctx, defaultKeyspace.groupInboxKey(9), "900").Val(); got != 4 {
		t.Fatalf("seeded inbox member score = %v, want 4", got)
	}
	if got := client.XLen(ctx, defaultKeyspace.groupMessageStreamKey()).Val(); got != 0 {
		t.Fatalf("group stream written after rejected seq = %d, want 0", got)
	}
	if got := client.Exists(ctx, defaultKeyspace.groupDedupKey(9, 1, "taken-seq")).Val(); got != 0 {
		t.Fatalf("group dedup written after rejected seq = %d, want 0", got)
	}
}

func TestGroupRedisAcceptorRequiresSequenceReady(t *testing.T) {
	client, server := newGroupAcceptTest(t)
	ctx := context.Background()
	seedGroupProjection(t, client, 9, []uint64{1})
	acceptor := NewGroupRedisAcceptor(client, defaultKeyspace, RedisAcceptorOptions{})
	request := GroupMessageRequest{
		SenderID: 1, GroupID: 9, ClientMessageID: "ready", Body: "hello",
	}

	if !server.Del(sequenceReadyKey()) {
		t.Fatal("remove readiness failed")
	}
	if _, err := acceptor.Accept(ctx, request); !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("missing seq:ready error = %v, want ErrSeqNotReady", err)
	}

	seedSequenceReady(t, client)
	gate := NewSeqGate()
	acceptor = NewGroupRedisAcceptor(client, defaultKeyspace, RedisAcceptorOptions{Gate: gate})
	gate.Close()
	if _, err := acceptor.Accept(ctx, request); !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("closed gate error = %v, want ErrSeqNotReady", err)
	}
}

func TestGroupRedisAcceptorRejectsLostCounterAndOverflow(t *testing.T) {
	client, server := newGroupAcceptTest(t)
	ctx := context.Background()
	seedGroupProjection(t, client, 9, []uint64{1})
	acceptor := NewGroupRedisAcceptor(client, defaultKeyspace, RedisAcceptorOptions{})
	request := GroupMessageRequest{
		SenderID: 1, GroupID: 9, ClientMessageID: "first", Body: "hello",
	}
	if _, err := acceptor.Accept(ctx, request); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if !server.Del(defaultKeyspace.groupSequenceKey(9)) {
		t.Fatal("delete counter failed")
	}
	request.ClientMessageID = "second"
	if _, err := acceptor.Accept(ctx, request); !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("lost counter error = %v, want ErrSeqNotReady", err)
	}

	setGroupSequence(t, client, 9, store.MaxSafeSeq)
	request.ClientMessageID = "third"
	if _, err := acceptor.Accept(ctx, request); !errors.Is(err, ErrSeqOverflow) {
		t.Fatalf("overflow error = %v, want ErrSeqOverflow", err)
	}
}
