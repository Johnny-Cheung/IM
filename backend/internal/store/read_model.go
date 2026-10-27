package store

import (
	"context"
	"fmt"
)

// MessageIndex is the minimal direct-message row needed to rebuild the Redis
// read indexes.
type MessageIndex struct {
	ID          uint64
	SenderID    uint64
	RecipientID uint64
}

// ConversationRead is one persisted direct read position.
type ConversationRead struct {
	UserID            uint64
	PeerUserID        uint64
	LastReadMessageID uint64
}

// DirectPairCount is the number of persisted messages for one direction.
type DirectPairCount struct {
	RecipientID uint64
	SenderID    uint64
	Count       uint64
}

// ListDirectMessageIndex returns persisted direct messages after afterID,
// ordered by the public server message ID.
func (s *Store) ListDirectMessageIndex(
	ctx context.Context,
	afterID uint64,
	limit int,
) ([]MessageIndex, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT server_message_id, sender_id, recipient_id
		 FROM messages
		 WHERE server_message_id > ?
		 ORDER BY server_message_id
		 LIMIT ?`,
		afterID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list direct message index: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]MessageIndex, 0, limit)
	for rows.Next() {
		var item MessageIndex
		if err := rows.Scan(&item.ID, &item.SenderID, &item.RecipientID); err != nil {
			return nil, fmt.Errorf("scan direct message index: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate direct message index: %w", err)
	}
	return result, nil
}

// ListConversationReadsAfter returns persisted read positions after the given
// primary key, ordered by (user_id, peer_user_id).
func (s *Store) ListConversationReadsAfter(
	ctx context.Context,
	afterUserID, afterPeerUserID uint64,
	limit int,
) ([]ConversationRead, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT user_id, peer_user_id, last_read_message_id
		 FROM conversation_reads
		 WHERE (user_id, peer_user_id) > (?, ?)
		 ORDER BY user_id, peer_user_id
		 LIMIT ?`,
		afterUserID, afterPeerUserID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list conversation reads: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]ConversationRead, 0, limit)
	for rows.Next() {
		var item ConversationRead
		if err := rows.Scan(&item.UserID, &item.PeerUserID, &item.LastReadMessageID); err != nil {
			return nil, fmt.Errorf("scan conversation read: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate conversation reads: %w", err)
	}
	return result, nil
}

// CountDirectMessagesByPair returns persisted message counts per direction.
func (s *Store) CountDirectMessagesByPair(ctx context.Context) ([]DirectPairCount, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT recipient_id, sender_id, COUNT(*)
		 FROM messages
		 GROUP BY recipient_id, sender_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("count direct messages by pair: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]DirectPairCount, 0)
	for rows.Next() {
		var item DirectPairCount
		if err := rows.Scan(&item.RecipientID, &item.SenderID, &item.Count); err != nil {
			return nil, fmt.Errorf("scan direct message count: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate direct message counts: %w", err)
	}
	return result, nil
}
