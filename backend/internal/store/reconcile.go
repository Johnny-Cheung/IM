package store

import (
	"context"
	"fmt"
)

// ConversationSeqStat is the persisted numbered message count and highest seq
// of one private conversation. Reconciliation compares it with the Redis
// conversation counter.
type ConversationSeqStat struct {
	LowID  uint64
	HighID uint64
	Count  uint64
	MaxSeq uint64
}

// GroupSeqStat is the persisted numbered message count and highest seq of one
// group conversation.
type GroupSeqStat struct {
	GroupID uint64
	Count   uint64
	MaxSeq  uint64
}

// ListConversationSeqStats returns per-pair counts and max seq of numbered
// messages, paged by (low, high) in ascending order.
func (s *Store) ListConversationSeqStats(
	ctx context.Context,
	afterLowID, afterHighID uint64,
	limit int,
) ([]ConversationSeqStat, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT conversation_low_id, conversation_high_id,
		        COUNT(*), COALESCE(MAX(seq), 0)
		 FROM messages
		 WHERE seq IS NOT NULL
		   AND conversation_low_id IS NOT NULL
		   AND conversation_high_id IS NOT NULL
		   AND (conversation_low_id, conversation_high_id) > (?, ?)
		 GROUP BY conversation_low_id, conversation_high_id
		 ORDER BY conversation_low_id, conversation_high_id
		 LIMIT ?`,
		afterLowID, afterHighID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list conversation seq stats: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]ConversationSeqStat, 0, limit)
	for rows.Next() {
		var item ConversationSeqStat
		if err := rows.Scan(&item.LowID, &item.HighID, &item.Count, &item.MaxSeq); err != nil {
			return nil, fmt.Errorf("scan conversation seq stat: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate conversation seq stats: %w", err)
	}
	return result, nil
}

// ListGroupSeqStats returns per-group counts and max seq of numbered messages,
// paged by group ID in ascending order. Dissolved groups are included.
func (s *Store) ListGroupSeqStats(
	ctx context.Context,
	afterGroupID uint64,
	limit int,
) ([]GroupSeqStat, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT group_id, COUNT(*), COALESCE(MAX(seq), 0)
		 FROM group_messages
		 WHERE seq IS NOT NULL AND group_id > ?
		 GROUP BY group_id
		 ORDER BY group_id
		 LIMIT ?`,
		afterGroupID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list group seq stats: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]GroupSeqStat, 0, limit)
	for rows.Next() {
		var item GroupSeqStat
		if err := rows.Scan(&item.GroupID, &item.Count, &item.MaxSeq); err != nil {
			return nil, fmt.Errorf("scan group seq stat: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group seq stats: %w", err)
	}
	return result, nil
}

// ListConversationSeqs returns the stored seqs of one private conversation up
// to and including upTo, ascending.
func (s *Store) ListConversationSeqs(
	ctx context.Context,
	lowID, highID, upTo uint64,
) ([]uint64, error) {
	if upTo == 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq
		 FROM messages
		 WHERE conversation_low_id = ? AND conversation_high_id = ?
		   AND seq IS NOT NULL AND seq <= ?
		 ORDER BY seq`,
		lowID, highID, upTo,
	)
	if err != nil {
		return nil, fmt.Errorf("list conversation seqs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]uint64, 0, 64)
	for rows.Next() {
		var seq uint64
		if err := rows.Scan(&seq); err != nil {
			return nil, fmt.Errorf("scan conversation seq: %w", err)
		}
		result = append(result, seq)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate conversation seqs: %w", err)
	}
	return result, nil
}

// ListGroupSeqs returns the stored seqs of one group up to and including upTo,
// ascending.
func (s *Store) ListGroupSeqs(
	ctx context.Context,
	groupID, upTo uint64,
) ([]uint64, error) {
	if upTo == 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq
		 FROM group_messages
		 WHERE group_id = ? AND seq IS NOT NULL AND seq <= ?
		 ORDER BY seq`,
		groupID, upTo,
	)
	if err != nil {
		return nil, fmt.Errorf("list group seqs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]uint64, 0, 64)
	for rows.Next() {
		var seq uint64
		if err := rows.Scan(&seq); err != nil {
			return nil, fmt.Errorf("scan group seq: %w", err)
		}
		result = append(result, seq)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group seqs: %w", err)
	}
	return result, nil
}
