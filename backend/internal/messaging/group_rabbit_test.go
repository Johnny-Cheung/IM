package messaging

import (
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func TestGroupMessageEnvelopeRoundTrip(t *testing.T) {
	original := store.GroupMessage{
		ID:              42,
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
	if decoded.ID != original.ID || decoded.GroupID != original.GroupID || decoded.SenderID != original.SenderID ||
		decoded.ClientMessageID != original.ClientMessageID || decoded.Body != original.Body ||
		!decoded.CreatedAt.Equal(original.CreatedAt) || decoded.Sender.Username != original.Sender.Username {
		t.Fatalf("decoded = %#v, original = %#v", decoded, original)
	}
}
