package messaging

import (
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func TestGroupMessageEnvelopeRoundTrip(t *testing.T) {
	original := store.GroupMessage{
		ID:              42,
		Seq:             7,
		GroupID:         9,
		SenderID:        3,
		Sender:          store.PublicUser{Username: "alice", Nickname: "Alice"},
		ClientMessageID: "group-envelope-1",
		Body:            "hello",
		CreatedAt:       time.UnixMilli(1700000000123).UTC(),
	}
	payload, err := encodeGroupMessage(original)
	if err != nil {
		t.Fatalf("encodeGroupMessage() error = %v", err)
	}
	decoded, err := decodeGroupMessage(payload)
	if err != nil {
		t.Fatalf("decodeGroupMessage() error = %v", err)
	}
	if decoded.ID != original.ID || decoded.Seq != original.Seq ||
		decoded.GroupID != original.GroupID || decoded.SenderID != original.SenderID ||
		decoded.ClientMessageID != original.ClientMessageID || decoded.Body != original.Body ||
		!decoded.CreatedAt.Equal(original.CreatedAt) || decoded.Sender.Username != original.Sender.Username {
		t.Fatalf("decoded = %#v, original = %#v", decoded, original)
	}
}

func TestGroupStreamMessageRequiresSeq(t *testing.T) {
	_, err := groupStreamMessage(map[string]interface{}{
		"id":                "42",
		"group_id":          "9",
		"sender_id":         "1",
		"client_message_id": "group-1",
		"body":              "hello",
		"created_at_ms":     "1700000000123",
	})
	if err == nil {
		t.Fatal("groupStreamMessage() error = nil, want missing seq error")
	}
	if _, err := decodeGroupMessage([]byte(`{"server_message_id":42,"group_id":9,"sender_id":1,"client_message_id":"g","created_at":"2023-11-14T22:13:20Z"}`)); err == nil {
		t.Fatal("decodeGroupMessage() error = nil, want missing seq error")
	}
}
