package messaging

import "fmt"

const redisPrefix = "im:v12:"

func counterKey() string {
	return redisPrefix + "messages:next_id"
}

func friendshipKey(userID uint64) string {
	return fmt.Sprintf("%sfriends:%d", redisPrefix, userID)
}

func dedupKey(senderID, recipientID uint64, clientMessageID string) string {
	return fmt.Sprintf("%sdedup:%d:%d:%s", redisPrefix, senderID, recipientID, clientMessageID)
}

func messageKey(messageID uint64) string {
	return fmt.Sprintf("%smessages:%d", redisPrefix, messageID)
}

func outboxKey(userID uint64) string {
	return fmt.Sprintf("%soutbox:%d", redisPrefix, userID)
}

func inboxKey(userID uint64) string {
	return fmt.Sprintf("%sinbox:%d", redisPrefix, userID)
}

func messageStreamKey() string {
	return redisPrefix + "messages:stream"
}
