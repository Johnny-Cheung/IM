package messaging

import (
	"context"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
)

func seedSequenceReady(t *testing.T, client redis.UniversalClient) {
	t.Helper()
	if err := client.Set(
		context.Background(), sequenceReadyKey(), "1", 0,
	).Err(); err != nil {
		t.Fatalf("seed seq:ready: %v", err)
	}
}

func setDirectSequence(
	t *testing.T,
	client redis.UniversalClient,
	userID, peerID, seq uint64,
) {
	t.Helper()
	if err := client.Set(
		context.Background(),
		defaultKeyspace.directSequenceKey(userID, peerID),
		seq,
		0,
	).Err(); err != nil {
		t.Fatalf("seed direct seq: %v", err)
	}
}

func setGroupSequence(
	t *testing.T,
	client redis.UniversalClient,
	groupID, seq uint64,
) {
	t.Helper()
	if err := client.Set(
		context.Background(),
		defaultKeyspace.groupSequenceKey(groupID),
		seq,
		0,
	).Err(); err != nil {
		t.Fatalf("seed group seq: %v", err)
	}
}

func seedGroupMessages(
	t *testing.T,
	client redis.UniversalClient,
	groupID uint64,
	messages map[uint64]uint64,
) {
	t.Helper()
	ctx := context.Background()
	for messageID, seq := range messages {
		if err := client.HSet(ctx, defaultKeyspace.groupMessageKey(messageID), map[string]interface{}{
			"id":                messageID,
			"seq":               seq,
			"group_id":          groupID,
			"sender_id":         1,
			"client_message_id": "seed-" + strconvUint(messageID),
			"body":              "seed",
			"created_at_ms":     1700000000000,
			"sender_username":   "alice",
			"sender_nickname":   "Alice",
		}).Err(); err != nil {
			t.Fatalf("seed group message %d: %v", messageID, err)
		}
		if err := client.ZAdd(ctx, defaultKeyspace.groupInboxKey(groupID), redis.Z{
			Score:  float64(seq),
			Member: strconvUint(messageID),
		}).Err(); err != nil {
			t.Fatalf("seed group inbox %d: %v", messageID, err)
		}
	}
}

func strconvUint(value uint64) string {
	return strconv.FormatUint(value, 10)
}
