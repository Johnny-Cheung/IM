package messaging

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func newProjectionTest(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client, server
}

func TestProjectionWriterWritesDirectMessage(t *testing.T) {
	client, _ := newProjectionTest(t)
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	createdAt := now.Add(-time.Hour)
	writer := NewProjectionWriter(client, defaultKeyspace, ProjectionWriteOptions{
		DedupTTL: 7 * 24 * time.Hour,
		Now:      func() time.Time { return now },
	})
	message := store.Message{
		ID:              42,
		Seq:             3,
		SenderID:        1,
		RecipientID:     2,
		ClientMessageID: "client-1",
		Body:            "hello",
		CreatedAt:       createdAt,
	}

	written, err := writer.WriteDirectBatch(ctx, []store.Message{message})
	if err != nil {
		t.Fatalf("WriteDirectBatch() error = %v", err)
	}
	if written != 1 {
		t.Fatalf("WriteDirectBatch() written = %d, want 1", written)
	}

	fields, err := client.HGetAll(ctx, messageKey(42)).Result()
	if err != nil {
		t.Fatalf("read entity: %v", err)
	}
	want := map[string]string{
		"id":                "42",
		"sender_id":         "1",
		"recipient_id":      "2",
		"client_message_id": "client-1",
		"body":              "hello",
		"created_at_ms":     strconv.FormatInt(createdAt.UnixMilli(), 10),
		"body_hash":         hashMessageBody("hello"),
		"seq":               "3",
	}
	if len(fields) != len(want) {
		t.Fatalf("entity fields = %#v, want exactly %d fields", fields, len(want))
	}
	for field, value := range want {
		if fields[field] != value {
			t.Fatalf("entity %s = %q, want %q", field, fields[field], value)
		}
	}

	if score := client.ZScore(ctx, directPairKey(2, 1), "42").Val(); score != 3 {
		t.Fatalf("dm:pair score = %v, want seq 3", score)
	}
	if score := client.ZScore(ctx, directFeedKey(1), "42").Val(); score != 42 {
		t.Fatalf("sender feed score = %v, want global id 42", score)
	}
	if score := client.ZScore(ctx, directFeedKey(2), "42").Val(); score != 42 {
		t.Fatalf("recipient feed score = %v, want global id 42", score)
	}
	if got := client.HGet(ctx, directConversationLastKey(1), "2").Val(); got != "42" {
		t.Fatalf("sender conv-last = %q, want 42", got)
	}
	if got := client.HGet(ctx, directConversationLastKey(2), "1").Val(); got != "42" {
		t.Fatalf("recipient conv-last = %q, want 42", got)
	}
	if got := client.Get(ctx, directSequenceKey(1, 2)).Val(); got != "3" {
		t.Fatalf("dm:seq = %q, want 3", got)
	}

	dedup := dedupKey(1, 2, "client-1")
	dedupFields, err := client.HGetAll(ctx, dedup).Result()
	if err != nil {
		t.Fatalf("read dedup: %v", err)
	}
	wantDedup := map[string]string{
		"id":            "42",
		"created_at_ms": strconv.FormatInt(createdAt.UnixMilli(), 10),
		"body_hash":     hashMessageBody("hello"),
	}
	if len(dedupFields) != len(wantDedup) {
		t.Fatalf("dedup fields = %#v, want exactly %d", dedupFields, len(wantDedup))
	}
	for field, value := range wantDedup {
		if dedupFields[field] != value {
			t.Fatalf("dedup %s = %q, want %q", field, dedupFields[field], value)
		}
	}
	ttl, err := client.TTL(ctx, dedup).Result()
	if err != nil {
		t.Fatalf("read dedup ttl: %v", err)
	}
	wantTTL := 7*24*time.Hour - time.Hour
	if diff := ttl - wantTTL; diff < -2*time.Second || diff > 2*time.Second {
		t.Fatalf("dedup ttl = %v, want %v", ttl, wantTTL)
	}
}

func TestProjectionWriterCountersAndPreviewsOnlyMoveForward(t *testing.T) {
	client, _ := newProjectionTest(t)
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	writer := NewProjectionWriter(client, defaultKeyspace, ProjectionWriteOptions{
		Now: func() time.Time { return now },
	})

	if err := client.Set(ctx, directSequenceKey(1, 2), "99", 0).Err(); err != nil {
		t.Fatalf("seed counter: %v", err)
	}
	if err := client.HSet(ctx, directConversationLastKey(1), "2", "1000").Err(); err != nil {
		t.Fatalf("seed preview: %v", err)
	}
	first := store.Message{
		ID: 42, Seq: 3, SenderID: 1, RecipientID: 2,
		ClientMessageID: "c1", Body: "old", CreatedAt: now,
	}
	if _, err := writer.WriteDirectBatch(ctx, []store.Message{first}); err != nil {
		t.Fatalf("WriteDirectBatch() error = %v", err)
	}
	if got := client.Get(ctx, directSequenceKey(1, 2)).Val(); got != "99" {
		t.Fatalf("counter after lower seq = %q, want 99", got)
	}

	second := store.Message{
		ID: 1001, Seq: 4, SenderID: 2, RecipientID: 1,
		ClientMessageID: "c2", Body: "new", CreatedAt: now,
	}
	if _, err := writer.WriteDirectBatch(ctx, []store.Message{second}); err != nil {
		t.Fatalf("WriteDirectBatch() error = %v", err)
	}
	if got := client.HGet(ctx, directConversationLastKey(1), "2").Val(); got != "1001" {
		t.Fatalf("preview after newer message = %q, want 1001", got)
	}

	third := store.Message{
		ID: 7, Seq: 5, SenderID: 3, RecipientID: 4,
		ClientMessageID: "c3", Body: "fresh", CreatedAt: now,
	}
	if _, err := writer.WriteDirectBatch(ctx, []store.Message{third}); err != nil {
		t.Fatalf("WriteDirectBatch() error = %v", err)
	}
	if got := client.Get(ctx, directSequenceKey(3, 4)).Val(); got != "5" {
		t.Fatalf("new conversation counter = %q, want 5", got)
	}
}

func TestProjectionWriterSkipsDedupOutsideWindow(t *testing.T) {
	client, _ := newProjectionTest(t)
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	writer := NewProjectionWriter(client, defaultKeyspace, ProjectionWriteOptions{
		DedupTTL: 7 * 24 * time.Hour,
		Now:      func() time.Time { return now },
	})
	message := store.Message{
		ID: 1, Seq: 1, SenderID: 1, RecipientID: 2,
		ClientMessageID: "old",
		Body:            "body",
		CreatedAt:       now.Add(-8 * 24 * time.Hour),
	}
	if _, err := writer.WriteDirectBatch(ctx, []store.Message{message}); err != nil {
		t.Fatalf("WriteDirectBatch() error = %v", err)
	}
	if exists := client.Exists(ctx, dedupKey(1, 2, "old")).Val(); exists != 0 {
		t.Fatalf("dedup record for an expired message exists")
	}
	if exists := client.Exists(ctx, messageKey(1)).Val(); exists != 1 {
		t.Fatalf("entity for an expired message is missing")
	}
}

func TestProjectionWriterWritesGroupMessage(t *testing.T) {
	client, _ := newProjectionTest(t)
	ctx := context.Background()
	now := time.UnixMilli(1700000000000).UTC()
	createdAt := now.Add(-2 * time.Hour)
	writer := NewProjectionWriter(client, defaultKeyspace, ProjectionWriteOptions{
		DedupTTL: 7 * 24 * time.Hour,
		Now:      func() time.Time { return now },
	})
	message := store.GroupMessage{
		ID:              7,
		Seq:             2,
		GroupID:         9,
		SenderID:        1,
		Sender:          store.PublicUser{},
		ClientMessageID: "group-1",
		Body:            "hi",
		CreatedAt:       createdAt,
	}

	written, err := writer.WriteGroupBatch(ctx, []store.GroupMessage{message})
	if err != nil {
		t.Fatalf("WriteGroupBatch() error = %v", err)
	}
	if written != 1 {
		t.Fatalf("WriteGroupBatch() written = %d, want 1", written)
	}

	fields, err := client.HGetAll(ctx, defaultKeyspace.groupMessageKey(7)).Result()
	if err != nil {
		t.Fatalf("read group entity: %v", err)
	}
	want := map[string]string{
		"id":                "7",
		"group_id":          "9",
		"sender_id":         "1",
		"client_message_id": "group-1",
		"body":              "hi",
		"created_at_ms":     strconv.FormatInt(createdAt.UnixMilli(), 10),
		"sender_username":   "",
		"sender_nickname":   "",
		"body_hash":         hashMessageBody("hi"),
		"seq":               "2",
	}
	if len(fields) != len(want) {
		t.Fatalf("group entity fields = %#v, want exactly %d fields", fields, len(want))
	}
	for field, value := range want {
		if fields[field] != value {
			t.Fatalf("group entity %s = %q, want %q", field, fields[field], value)
		}
	}

	if score := client.ZScore(ctx, defaultKeyspace.groupInboxKey(9), "7").Val(); score != 2 {
		t.Fatalf("group inbox score = %v, want seq 2", score)
	}
	if got := client.Get(ctx, defaultKeyspace.groupSequenceKey(9)).Val(); got != "2" {
		t.Fatalf("group seq = %q, want 2", got)
	}
	if entries := client.ZCard(ctx, directFeedKey(1)).Val(); entries != 0 {
		t.Fatalf("group message wrote %d feed entries", entries)
	}

	dedup := defaultKeyspace.groupDedupKey(9, 1, "group-1")
	dedupFields, err := client.HGetAll(ctx, dedup).Result()
	if err != nil {
		t.Fatalf("read group dedup: %v", err)
	}
	if len(dedupFields) != 3 || dedupFields["id"] != "7" ||
		dedupFields["body_hash"] != hashMessageBody("hi") {
		t.Fatalf("group dedup fields = %#v", dedupFields)
	}
	ttl, err := client.TTL(ctx, dedup).Result()
	if err != nil {
		t.Fatalf("read group dedup ttl: %v", err)
	}
	wantTTL := 7*24*time.Hour - 2*time.Hour
	if diff := ttl - wantTTL; diff < -2*time.Second || diff > 2*time.Second {
		t.Fatalf("group dedup ttl = %v, want %v", ttl, wantTTL)
	}
}

func TestProjectionWriterRaisesWatermarksOnly(t *testing.T) {
	client, _ := newProjectionTest(t)
	ctx := context.Background()
	writer := NewProjectionWriter(client, defaultKeyspace, ProjectionWriteOptions{})

	if err := client.HSet(ctx, directReadKey(1), "2", "5").Err(); err != nil {
		t.Fatalf("seed direct watermark: %v", err)
	}
	raised, err := writer.RaiseDirectReadWatermarks(ctx, []store.ConversationReadState{
		{UserID: 1, PeerID: 2, LastReadSeq: 3},
	})
	if err != nil {
		t.Fatalf("RaiseDirectReadWatermarks(lower) error = %v", err)
	}
	if raised != 0 {
		t.Fatalf("lower watermark raised %d entries, want 0", raised)
	}
	raised, err = writer.RaiseDirectReadWatermarks(ctx, []store.ConversationReadState{
		{UserID: 1, PeerID: 2, LastReadSeq: 9},
		{UserID: 1, PeerID: 3, LastReadSeq: 0},
	})
	if err != nil {
		t.Fatalf("RaiseDirectReadWatermarks(higher) error = %v", err)
	}
	if raised != 1 {
		t.Fatalf("raised entries = %d, want 1", raised)
	}
	if got := client.HGet(ctx, directReadKey(1), "2").Val(); got != "9" {
		t.Fatalf("direct watermark = %q, want 9", got)
	}
	if exists := client.HExists(ctx, directReadKey(1), "3").Val(); exists {
		t.Fatalf("zero watermark was stored")
	}

	if err := client.ZAdd(ctx, defaultKeyspace.groupReadKey(9), redis.Z{Score: 5, Member: "1"}).Err(); err != nil {
		t.Fatalf("seed group watermark: %v", err)
	}
	raised, err = writer.RaiseGroupReadWatermarks(ctx, []store.GroupMemberReadState{
		{GroupID: 9, UserID: 1, LastReadSeq: 4},
	})
	if err != nil || raised != 0 {
		t.Fatalf("lower group watermark = %d, %v; want 0, nil", raised, err)
	}
	raised, err = writer.RaiseGroupReadWatermarks(ctx, []store.GroupMemberReadState{
		{GroupID: 9, UserID: 1, LastReadSeq: 9},
	})
	if err != nil || raised != 1 {
		t.Fatalf("higher group watermark = %d, %v; want 1, nil", raised, err)
	}
	if score := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "1").Val(); score != 9 {
		t.Fatalf("group watermark = %v, want 9", score)
	}

	raised, err = writer.RaiseGroupReadPositions(ctx, 9, map[uint64]uint64{
		1: 4,
		2: 6,
	})
	if err != nil || raised != 1 {
		t.Fatalf("RaiseGroupReadPositions = %d, %v; want 1, nil", raised, err)
	}
	if score := client.ZScore(ctx, defaultKeyspace.groupReadKey(9), "2").Val(); score != 6 {
		t.Fatalf("group position for member 2 = %v, want 6", score)
	}
}

func TestProjectionWriterRaiseCountersNeverLower(t *testing.T) {
	client, _ := newProjectionTest(t)
	ctx := context.Background()
	writer := NewProjectionWriter(client, defaultKeyspace, ProjectionWriteOptions{})

	if err := writer.RaiseDirectCounter(ctx, 1, 2, 5); err != nil {
		t.Fatalf("RaiseDirectCounter() error = %v", err)
	}
	if err := writer.RaiseDirectCounter(ctx, 2, 1, 3); err != nil {
		t.Fatalf("RaiseDirectCounter(lower) error = %v", err)
	}
	if got := client.Get(ctx, directSequenceKey(1, 2)).Val(); got != "5" {
		t.Fatalf("direct counter = %q, want 5", got)
	}
	if err := writer.RaiseGroupCounter(ctx, 9, 4); err != nil {
		t.Fatalf("RaiseGroupCounter() error = %v", err)
	}
	if err := writer.RaiseGroupCounter(ctx, 9, 2); err != nil {
		t.Fatalf("RaiseGroupCounter(lower) error = %v", err)
	}
	if got := client.Get(ctx, defaultKeyspace.groupSequenceKey(9)).Val(); got != "4" {
		t.Fatalf("group counter = %q, want 4", got)
	}
}

func TestProjectionWriterRejectsMessagesWithoutSeq(t *testing.T) {
	client, _ := newProjectionTest(t)
	ctx := context.Background()
	writer := NewProjectionWriter(client, defaultKeyspace, ProjectionWriteOptions{})

	if _, err := writer.WriteDirectBatch(ctx, []store.Message{
		{ID: 1, SenderID: 1, RecipientID: 2, Body: "x"},
	}); err == nil {
		t.Fatalf("WriteDirectBatch() accepted a message without seq")
	}
	if _, err := writer.WriteGroupBatch(ctx, []store.GroupMessage{
		{ID: 2, GroupID: 9, SenderID: 1, Body: "x"},
	}); err == nil {
		t.Fatalf("WriteGroupBatch() accepted a message without seq")
	}
}
