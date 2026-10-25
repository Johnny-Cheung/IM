package messaging

import (
	"reflect"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func TestDirectMessageEnvelopeRoundTrip(t *testing.T) {
	message := store.Message{
		ID:              42,
		SenderID:        1,
		RecipientID:     2,
		ClientMessageID: "client-12345678",
		Body:            "hello\n世界",
		CreatedAt:       time.UnixMilli(1700000000123).UTC(),
	}
	payload, err := encodeDirectMessage(message)
	if err != nil {
		t.Fatalf("encodeDirectMessage() error = %v", err)
	}
	decoded, err := decodeDirectMessage(payload)
	if err != nil {
		t.Fatalf("decodeDirectMessage() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, message) {
		t.Fatalf("decoded = %#v, want %#v", decoded, message)
	}
}

func TestStreamMessageConvertsToStoreMessage(t *testing.T) {
	message, err := streamMessage(map[string]interface{}{
		"id":                "42",
		"sender_id":         "1",
		"recipient_id":      "2",
		"client_message_id": "client-12345678",
		"body":              "hello",
		"created_at_ms":     "1700000000123",
	})
	if err != nil {
		t.Fatalf("streamMessage() error = %v", err)
	}
	if message.ID != 42 || message.SenderID != 1 || message.RecipientID != 2 ||
		message.ClientMessageID != "client-12345678" || message.Body != "hello" ||
		message.CreatedAt.UnixMilli() != 1700000000123 {
		t.Fatalf("message = %#v", message)
	}
}
