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

func directFeedKey(userID uint64) string {
	return defaultKeyspace.directFeedKey(userID)
}

func directSequenceKey(userID, peerID uint64) string {
	return defaultKeyspace.directSequenceKey(userID, peerID)
}

func directPairKey(userID, peerID uint64) string {
	return defaultKeyspace.directPairKey(userID, peerID)
}

func directConversationLastKey(userID uint64) string {
	return defaultKeyspace.directConversationLastKey(userID)
}

func directReadKey(userID uint64) string {
	return defaultKeyspace.directReadKey(userID)
}

func directReadStreamKey() string {
	return defaultKeyspace.directReadStreamKey()
}

func sequenceReadyKey() string {
	return defaultKeyspace.sequenceReadyKey()
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

// directFeedKey indexes every direct message the user participates in,
// regardless of direction. It backs whole-user incremental sync.
func (k Keyspace) directFeedKey(userID uint64) string {
	return fmt.Sprintf("%sdm:feed:%d", k.prefixOrDefault(), userID)
}

// directPairKey indexes every message of one private conversation, in both
// directions, scored by the conversation seq.
func (k Keyspace) directPairKey(userID, peerID uint64) string {
	low, high := sortedPair(userID, peerID)
	return fmt.Sprintf("%sdm:pair:%d:%d", k.prefixOrDefault(), low, high)
}

// directSequenceKey is the private conversation's seq counter.
func (k Keyspace) directSequenceKey(userID, peerID uint64) string {
	low, high := sortedPair(userID, peerID)
	return fmt.Sprintf("%sdm:seq:%d:%d", k.prefixOrDefault(), low, high)
}

// sortedPair normalises the two members of a private conversation.
func sortedPair(userID, peerID uint64) (uint64, uint64) {
	if userID < peerID {
		return userID, peerID
	}
	return peerID, userID
}

// directConversationLastKey maps each peer to the newest message exchanged
// with that peer, backing the conversation list.
func (k Keyspace) directConversationLastKey(userID uint64) string {
	return fmt.Sprintf("%sdm:conv-last:%d", k.prefixOrDefault(), userID)
}

// directReadKey stores the last read seq per peer for one user.
func (k Keyspace) directReadKey(userID uint64) string {
	return fmt.Sprintf("%sdm:read:%d", k.prefixOrDefault(), userID)
}

// directReadStreamKey carries read-position events for asynchronous
// persistence.
func (k Keyspace) directReadStreamKey() string {
	return k.prefixOrDefault() + "dm:read:stream"
}

// sequenceReadyKey marks that history and counters are complete.
func (k Keyspace) sequenceReadyKey() string {
	return k.prefixOrDefault() + "seq:ready"
}

// backfillLockKey is held while a seq backfill runs. The backend refuses to
// start while it exists, and a second backfill refuses to acquire it.
func (k Keyspace) backfillLockKey() string {
	return k.prefixOrDefault() + "backfill:lock"
}

// indexEpochKey records the highest message ID that is not guaranteed to be
// present in the private feed index. It is a global-ID score floor for
// dm:feed only and must never be used for conversation seq ranges.
func (k Keyspace) indexEpochKey() string {
	return k.prefixOrDefault() + "index:epoch"
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

// groupInboxKey indexes every group message, scored by the group seq.
func (k Keyspace) groupInboxKey(groupID uint64) string {
	return fmt.Sprintf("%sgroup:%d:inbox", k.prefixOrDefault(), groupID)
}

// groupSequenceKey is the group's seq counter.
func (k Keyspace) groupSequenceKey(groupID uint64) string {
	return fmt.Sprintf("%sgroup:%d:seq", k.prefixOrDefault(), groupID)
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

func (k Keyspace) groupReadKey(groupID uint64) string {
	return fmt.Sprintf("%sgroup:%d:read", k.prefixOrDefault(), groupID)
}

func (k Keyspace) groupReadStreamKey() string {
	return k.prefixOrDefault() + "group-read:stream"
}
