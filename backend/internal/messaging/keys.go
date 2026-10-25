package messaging

import "fmt"

const redisPrefix = DefaultMessagingNamespace + ":"

var defaultKeyspace = NewKeyspace(DefaultNamespace())

func counterKey() string {
	return defaultKeyspace.counterKey()
}

func friendshipKey(userID uint64) string {
	return defaultKeyspace.friendshipKey(userID)
}

func dedupKey(senderID, recipientID uint64, clientMessageID string) string {
	return defaultKeyspace.dedupKey(senderID, recipientID, clientMessageID)
}

func messageKey(messageID uint64) string {
	return defaultKeyspace.messageKey(messageID)
}

func outboxKey(userID uint64) string {
	return defaultKeyspace.outboxKey(userID)
}

func inboxKey(userID uint64) string {
	return defaultKeyspace.inboxKey(userID)
}

func messageStreamKey() string {
	return defaultKeyspace.messageStreamKey()
}

func (k Keyspace) counterKey() string {
	return k.prefixOrDefault() + "messages:next_id"
}

func (k Keyspace) friendshipKey(userID uint64) string {
	return fmt.Sprintf("%sfriends:%d", k.prefixOrDefault(), userID)
}

func (k Keyspace) dedupKey(senderID, recipientID uint64, clientMessageID string) string {
	return fmt.Sprintf("%sdedup:%d:%d:%s", k.prefixOrDefault(), senderID, recipientID, clientMessageID)
}

func (k Keyspace) messageKey(messageID uint64) string {
	return fmt.Sprintf("%smessages:%d", k.prefixOrDefault(), messageID)
}

func (k Keyspace) outboxKey(userID uint64) string {
	return fmt.Sprintf("%soutbox:%d", k.prefixOrDefault(), userID)
}

func (k Keyspace) inboxKey(userID uint64) string {
	return fmt.Sprintf("%sinbox:%d", k.prefixOrDefault(), userID)
}

func (k Keyspace) messageStreamKey() string {
	return k.prefixOrDefault() + "messages:stream"
}

func (k Keyspace) groupMembersKey(groupID uint64) string {
	return fmt.Sprintf("%sgroup:%d:members", k.prefixOrDefault(), groupID)
}

func (k Keyspace) groupDissolvedKey(groupID uint64) string {
	return fmt.Sprintf("%sgroup:%d:dissolved", k.prefixOrDefault(), groupID)
}

func (k Keyspace) groupSyncRequiredKey(groupID uint64) string {
	return fmt.Sprintf("%sgroup:%d:sync_required", k.prefixOrDefault(), groupID)
}

func (k Keyspace) groupInboxKey(groupID uint64) string {
	return fmt.Sprintf("%sgroup:%d:inbox", k.prefixOrDefault(), groupID)
}

func (k Keyspace) groupLastAcceptedKey(groupID uint64) string {
	return fmt.Sprintf("%sgroup:%d:last_accepted_id", k.prefixOrDefault(), groupID)
}

func (k Keyspace) groupDedupKey(groupID, senderID uint64, clientMessageID string) string {
	return fmt.Sprintf("%sgroup-dedup:%d:%d:%s", k.prefixOrDefault(), groupID, senderID, clientMessageID)
}

func (k Keyspace) groupMessageKey(messageID uint64) string {
	return fmt.Sprintf("%sgroup-messages:%d", k.prefixOrDefault(), messageID)
}

func (k Keyspace) groupMessageStreamKey() string {
	return k.prefixOrDefault() + "group-messages:stream"
}
