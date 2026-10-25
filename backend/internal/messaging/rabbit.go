package messaging

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

const (
	directExchangeName = "im.v12.messages"
	directQueueName    = "im.v12.messages.persist"
	directRoutingKey   = "direct.persist"
	directConsumerName = "im-v12-direct"
	directRelayGroup   = "im-v12-relay"
)

type directMessageEnvelope struct {
	ServerMessageID uint64 `json:"server_message_id"`
	SenderID        uint64 `json:"sender_id"`
	RecipientID     uint64 `json:"recipient_id"`
	ClientMessageID string `json:"client_message_id"`
	Body            string `json:"body"`
	CreatedAt       string `json:"created_at"`
}

func encodeDirectMessage(message store.Message) ([]byte, error) {
	return json.Marshal(directMessageEnvelope{
		ServerMessageID: message.ID,
		SenderID:        message.SenderID,
		RecipientID:     message.RecipientID,
		ClientMessageID: message.ClientMessageID,
		Body:            message.Body,
		CreatedAt:       message.CreatedAt.UTC().Format(time.RFC3339Nano),
	})
}

func decodeDirectMessage(payload []byte) (store.Message, error) {
	var envelope directMessageEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return store.Message{}, fmt.Errorf("decode direct message payload: %w", err)
	}
	if envelope.ServerMessageID == 0 ||
		envelope.SenderID == 0 ||
		envelope.RecipientID == 0 ||
		envelope.ClientMessageID == "" {
		return store.Message{}, fmt.Errorf("decode direct message payload: missing identity")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, envelope.CreatedAt)
	if err != nil {
		return store.Message{}, fmt.Errorf("decode direct message timestamp: %w", err)
	}
	return store.Message{
		ID:              envelope.ServerMessageID,
		SenderID:        envelope.SenderID,
		RecipientID:     envelope.RecipientID,
		ClientMessageID: envelope.ClientMessageID,
		Body:            envelope.Body,
		CreatedAt:       createdAt.UTC(),
	}, nil
}

func streamMessage(values map[string]interface{}) (store.Message, error) {
	messageID, err := streamUint(values, "id")
	if err != nil {
		return store.Message{}, err
	}
	senderID, err := streamUint(values, "sender_id")
	if err != nil {
		return store.Message{}, err
	}
	recipientID, err := streamUint(values, "recipient_id")
	if err != nil {
		return store.Message{}, err
	}
	clientMessageID, err := streamString(values, "client_message_id")
	if err != nil {
		return store.Message{}, err
	}
	body, err := streamString(values, "body")
	if err != nil {
		return store.Message{}, err
	}
	createdAtMS, err := streamInt64(values, "created_at_ms")
	if err != nil {
		return store.Message{}, err
	}
	return store.Message{
		ID:              messageID,
		SenderID:        senderID,
		RecipientID:     recipientID,
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       time.UnixMilli(createdAtMS).UTC(),
	}, nil
}

func streamString(values map[string]interface{}, key string) (string, error) {
	value, ok := values[key]
	if !ok {
		return "", fmt.Errorf("stream message missing %s", key)
	}
	switch typed := value.(type) {
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	default:
		return "", fmt.Errorf("stream message field %s has type %T", key, value)
	}
}

func streamUint(values map[string]interface{}, key string) (uint64, error) {
	value, err := streamString(values, key)
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("stream message field %s is invalid", key)
	}
	return parsed, nil
}

func streamInt64(values map[string]interface{}, key string) (int64, error) {
	value, err := streamString(values, key)
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("stream message field %s is invalid", key)
	}
	return parsed, nil
}
