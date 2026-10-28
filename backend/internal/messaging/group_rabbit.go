package messaging

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

type groupMessageEnvelope struct {
	ServerMessageID uint64 `json:"server_message_id"`
	Seq             uint64 `json:"seq"`
	GroupID         uint64 `json:"group_id"`
	SenderID        uint64 `json:"sender_id"`
	ClientMessageID string `json:"client_message_id"`
	Body            string `json:"body"`
	CreatedAt       string `json:"created_at"`
	SenderUsername  string `json:"sender_username"`
	SenderNickname  string `json:"sender_nickname"`
}

func groupExchangeName(namespace Namespace) string {
	return namespace.RabbitPrefix() + ".group.messages"
}

func groupQueueName(namespace Namespace) string {
	return namespace.RabbitPrefix() + ".group.messages.persist"
}

func groupRoutingKey() string { return "group.persist" }

func groupConsumerName(namespace Namespace) string {
	return namespace.ConsumerPrefix() + "-group"
}

func groupRelayName(namespace Namespace) string {
	return namespace.ConsumerPrefix() + "-group-relay"
}

func encodeGroupMessage(message store.GroupMessage) ([]byte, error) {
	return json.Marshal(groupMessageEnvelope{
		ServerMessageID: message.ID,
		Seq:             message.Seq,
		GroupID:         message.GroupID,
		SenderID:        message.SenderID,
		ClientMessageID: message.ClientMessageID,
		Body:            message.Body,
		CreatedAt:       message.CreatedAt.UTC().Format(time.RFC3339Nano),
		SenderUsername:  message.Sender.Username,
		SenderNickname:  message.Sender.Nickname,
	})
}

func decodeGroupMessage(payload []byte) (store.GroupMessage, error) {
	var envelope groupMessageEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return store.GroupMessage{}, fmt.Errorf("decode group message payload: %w", err)
	}
	if envelope.ServerMessageID == 0 || envelope.Seq == 0 || envelope.GroupID == 0 ||
		envelope.SenderID == 0 || envelope.ClientMessageID == "" {
		return store.GroupMessage{}, fmt.Errorf("decode group message payload: missing identity")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, envelope.CreatedAt)
	if err != nil {
		return store.GroupMessage{}, fmt.Errorf("decode group message timestamp: %w", err)
	}
	return store.GroupMessage{
		ID:              envelope.ServerMessageID,
		Seq:             envelope.Seq,
		GroupID:         envelope.GroupID,
		SenderID:        envelope.SenderID,
		Sender:          store.PublicUser{Username: envelope.SenderUsername, Nickname: envelope.SenderNickname},
		ClientMessageID: envelope.ClientMessageID,
		Body:            envelope.Body,
		CreatedAt:       createdAt.UTC(),
	}, nil
}

func groupStreamMessage(values map[string]interface{}) (store.GroupMessage, error) {
	messageID, err := streamUint(values, "id")
	if err != nil {
		return store.GroupMessage{}, err
	}
	seq, err := streamUint(values, "seq")
	if err != nil {
		return store.GroupMessage{}, err
	}
	groupID, err := streamUint(values, "group_id")
	if err != nil {
		return store.GroupMessage{}, err
	}
	senderID, err := streamUint(values, "sender_id")
	if err != nil {
		return store.GroupMessage{}, err
	}
	clientMessageID, err := streamString(values, "client_message_id")
	if err != nil {
		return store.GroupMessage{}, err
	}
	body, err := streamString(values, "body")
	if err != nil {
		return store.GroupMessage{}, err
	}
	createdAtMS, err := streamInt64(values, "created_at_ms")
	if err != nil {
		return store.GroupMessage{}, err
	}
	username, err := streamStringOptional(values, "sender_username")
	if err != nil {
		return store.GroupMessage{}, err
	}
	nickname, err := streamStringOptional(values, "sender_nickname")
	if err != nil {
		return store.GroupMessage{}, err
	}
	return store.GroupMessage{
		ID:              messageID,
		Seq:             seq,
		GroupID:         groupID,
		SenderID:        senderID,
		Sender:          store.PublicUser{Username: username, Nickname: nickname},
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       time.UnixMilli(createdAtMS).UTC(),
	}, nil
}

func streamStringOptional(values map[string]interface{}, key string) (string, error) {
	value, ok := values[key]
	if !ok || value == nil {
		return "", nil
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

func groupMessageIDString(messageID uint64) string {
	return strconv.FormatUint(messageID, 10)
}
