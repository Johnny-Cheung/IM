package store

import (
	"context"
	"fmt"
)

// ConversationSeqMax is one private conversation's persisted message count and
// highest seq. It feeds backfill verification and counter calibration.
type ConversationSeqMax struct {
	LowID  uint64
	HighID uint64
	MaxSeq uint64
	Count  uint64
}

// GroupSeqMax is one group conversation's persisted message count and highest
// seq.
type GroupSeqMax struct {
	GroupID uint64
	MaxSeq  uint64
	Count   uint64
}

// MessageSeqIndex is one persisted direct message with its conversation seq.
type MessageSeqIndex struct {
	ServerMessageID uint64
	Seq             uint64
	LowID           uint64
	HighID          uint64
	SenderID        uint64
	RecipientID     uint64
}

// GroupMessageSeqIndex is one persisted group message with its group seq.
type GroupMessageSeqIndex struct {
	ServerMessageID uint64
	Seq             uint64
	GroupID         uint64
	SenderID        uint64
}

// ConversationReadState is both read units of a direct conversation row.
type ConversationReadState struct {
	UserID            uint64
	PeerID            uint64
	LastReadMessageID uint64
	LastReadSeq       uint64
}

// GroupMemberReadState is both read units of a group member row.
type GroupMemberReadState struct {
	GroupID           uint64
	UserID            uint64
	LastReadMessageID uint64
	LastReadSeq       uint64
}

// ListConversationSeqMax returns per-pair message counts and max seq, paged by
// (low, high).
func (s *Store) ListConversationSeqMax(
	ctx context.Context,
	afterLowID, afterHighID uint64,
	limit int,
) ([]ConversationSeqMax, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT conversation_low_id, conversation_high_id,
		        COALESCE(MAX(seq), 0), COUNT(*)
		 FROM messages
		 WHERE conversation_low_id IS NOT NULL AND conversation_high_id IS NOT NULL
		   AND (conversation_low_id, conversation_high_id) > (?, ?)
		 GROUP BY conversation_low_id, conversation_high_id
		 ORDER BY conversation_low_id, conversation_high_id
		 LIMIT ?`,
		afterLowID, afterHighID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list conversation seq max: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]ConversationSeqMax, 0, limit)
	for rows.Next() {
		var item ConversationSeqMax
		if err := rows.Scan(&item.LowID, &item.HighID, &item.MaxSeq, &item.Count); err != nil {
			return nil, fmt.Errorf("scan conversation seq max: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate conversation seq max: %w", err)
	}
	return result, nil
}

// ListGroupSeqMax returns per-group message counts and max seq, paged by group
// ID. Dissolved groups are included.
func (s *Store) ListGroupSeqMax(
	ctx context.Context,
	afterGroupID uint64,
	limit int,
) ([]GroupSeqMax, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT group_id, COALESCE(MAX(seq), 0), COUNT(*)
		 FROM group_messages
		 WHERE group_id > ?
		 GROUP BY group_id
		 ORDER BY group_id
		 LIMIT ?`,
		afterGroupID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list group seq max: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]GroupSeqMax, 0, limit)
	for rows.Next() {
		var item GroupSeqMax
		if err := rows.Scan(&item.GroupID, &item.MaxSeq, &item.Count); err != nil {
			return nil, fmt.Errorf("scan group seq max: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group seq max: %w", err)
	}
	return result, nil
}

// FillMessageConversationPairs fills the nullable pair columns of up to limit
// messages and returns the number of rows changed. Callers repeat until zero.
func (s *Store) FillMessageConversationPairs(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	result, err := s.db.ExecContext(ctx,
		`UPDATE messages
		 SET conversation_low_id = LEAST(sender_id, recipient_id),
		     conversation_high_id = GREATEST(sender_id, recipient_id)
		 WHERE conversation_low_id IS NULL OR conversation_high_id IS NULL
		 ORDER BY id
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf("fill message conversation pairs: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read filled message conversation pairs: %w", err)
	}
	return int(affected), nil
}

// AssignMessageSeqs numbers up to limit still-unnumbered direct messages,
// continuing each conversation's existing maximum seq. It is idempotent and
// restartable, and callers repeat until it returns zero.
func (s *Store) AssignMessageSeqs(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin assign message seqs: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx,
		`SELECT id, conversation_low_id, conversation_high_id
		 FROM messages
		 WHERE seq IS NULL
		   AND conversation_low_id IS NOT NULL
		   AND conversation_high_id IS NOT NULL
		 ORDER BY server_message_id
		 LIMIT ?
		 FOR UPDATE`,
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf("list unnumbered messages: %w", err)
	}

	type pendingMessage struct {
		id            uint64
		lowID, highID uint64
	}
	pending := make([]pendingMessage, 0, limit)
	for rows.Next() {
		var item pendingMessage
		if err := rows.Scan(&item.id, &item.lowID, &item.highID); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan unnumbered message: %w", err)
		}
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate unnumbered messages: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close unnumbered messages: %w", err)
	}
	if len(pending) == 0 {
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("commit empty message seq assignment: %w", err)
		}
		return 0, nil
	}

	next := make(map[[2]uint64]uint64)
	processed := 0
	for _, item := range pending {
		key := [2]uint64{item.lowID, item.highID}
		seq, ok := next[key]
		if !ok {
			if err := tx.QueryRowContext(ctx,
				`SELECT COALESCE(MAX(seq), 0) FROM messages
				 WHERE conversation_low_id = ? AND conversation_high_id = ?`,
				item.lowID, item.highID,
			).Scan(&seq); err != nil {
				return 0, fmt.Errorf("read conversation seq watermark: %w", err)
			}
		}
		seq, err = nextSeqAfter(seq)
		if err != nil {
			return 0, err
		}
		next[key] = seq
		result, err := tx.ExecContext(ctx,
			`UPDATE messages SET seq = ? WHERE id = ? AND seq IS NULL`,
			seq, item.id,
		)
		if err != nil {
			return 0, fmt.Errorf("assign message seq: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("read assigned message seq rows: %w", err)
		}
		processed += int(affected)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit message seq assignment: %w", err)
	}
	return processed, nil
}

// AssignGroupMessageSeqs numbers up to limit still-unnumbered group messages,
// continuing each group's existing maximum seq. It is idempotent and
// restartable, and callers repeat until it returns zero.
func (s *Store) AssignGroupMessageSeqs(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin assign group message seqs: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx,
		`SELECT id, group_id
		 FROM group_messages
		 WHERE seq IS NULL
		 ORDER BY server_message_id
		 LIMIT ?
		 FOR UPDATE`,
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf("list unnumbered group messages: %w", err)
	}

	type pendingGroupMessage struct {
		id, groupID uint64
	}
	pending := make([]pendingGroupMessage, 0, limit)
	for rows.Next() {
		var item pendingGroupMessage
		if err := rows.Scan(&item.id, &item.groupID); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan unnumbered group message: %w", err)
		}
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate unnumbered group messages: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close unnumbered group messages: %w", err)
	}
	if len(pending) == 0 {
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("commit empty group seq assignment: %w", err)
		}
		return 0, nil
	}

	next := make(map[uint64]uint64)
	processed := 0
	for _, item := range pending {
		seq, ok := next[item.groupID]
		if !ok {
			if err := tx.QueryRowContext(ctx,
				`SELECT COALESCE(MAX(seq), 0) FROM group_messages WHERE group_id = ?`,
				item.groupID,
			).Scan(&seq); err != nil {
				return 0, fmt.Errorf("read group seq watermark: %w", err)
			}
		}
		seq, err = nextSeqAfter(seq)
		if err != nil {
			return 0, err
		}
		next[item.groupID] = seq
		result, err := tx.ExecContext(ctx,
			`UPDATE group_messages SET seq = ? WHERE id = ? AND seq IS NULL`,
			seq, item.id,
		)
		if err != nil {
			return 0, fmt.Errorf("assign group message seq: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("read assigned group message seq rows: %w", err)
		}
		processed += int(affected)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit group seq assignment: %w", err)
	}
	return processed, nil
}

// CountUnnumberedMessages returns how many direct and group messages still
// have a null seq.
func (s *Store) CountUnnumberedMessages(ctx context.Context) (uint64, uint64, error) {
	var direct, group uint64
	if err := s.db.QueryRowContext(ctx,
		`SELECT
		   COALESCE((SELECT COUNT(*) FROM messages WHERE seq IS NULL), 0),
		   COALESCE((SELECT COUNT(*) FROM group_messages WHERE seq IS NULL), 0)`,
	).Scan(&direct, &group); err != nil {
		return 0, 0, fmt.Errorf("count unnumbered messages: %w", err)
	}
	return direct, group, nil
}

// ListMessageSeqIndex returns direct messages after afterServerMessageID,
// ordered by the global message ID.
func (s *Store) ListMessageSeqIndex(
	ctx context.Context,
	afterServerMessageID uint64,
	limit int,
) ([]MessageSeqIndex, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT server_message_id, COALESCE(seq, 0),
		        COALESCE(conversation_low_id, 0), COALESCE(conversation_high_id, 0),
		        sender_id, recipient_id
		 FROM messages
		 WHERE server_message_id > ?
		 ORDER BY server_message_id
		 LIMIT ?`,
		afterServerMessageID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list message seq index: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]MessageSeqIndex, 0, limit)
	for rows.Next() {
		var item MessageSeqIndex
		if err := rows.Scan(
			&item.ServerMessageID,
			&item.Seq,
			&item.LowID,
			&item.HighID,
			&item.SenderID,
			&item.RecipientID,
		); err != nil {
			return nil, fmt.Errorf("scan message seq index: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate message seq index: %w", err)
	}
	return result, nil
}

// ListGroupMessageSeqIndex returns group messages after afterServerMessageID,
// ordered by the global message ID.
func (s *Store) ListGroupMessageSeqIndex(
	ctx context.Context,
	afterServerMessageID uint64,
	limit int,
) ([]GroupMessageSeqIndex, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT server_message_id, COALESCE(seq, 0), group_id, sender_id
		 FROM group_messages
		 WHERE server_message_id > ?
		 ORDER BY server_message_id
		 LIMIT ?`,
		afterServerMessageID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list group message seq index: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]GroupMessageSeqIndex, 0, limit)
	for rows.Next() {
		var item GroupMessageSeqIndex
		if err := rows.Scan(&item.ServerMessageID, &item.Seq, &item.GroupID, &item.SenderID); err != nil {
			return nil, fmt.Errorf("scan group message seq index: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group message seq index: %w", err)
	}
	return result, nil
}

// ListConversationReadStates returns both read units for direct conversations,
// paged by (user_id, peer_user_id).
func (s *Store) ListConversationReadStates(
	ctx context.Context,
	afterUserID, afterPeerID uint64,
	limit int,
) ([]ConversationReadState, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT user_id, peer_user_id, last_read_message_id, last_read_seq
		 FROM conversation_reads
		 WHERE (user_id, peer_user_id) > (?, ?)
		 ORDER BY user_id, peer_user_id
		 LIMIT ?`,
		afterUserID, afterPeerID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list conversation read states: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]ConversationReadState, 0, limit)
	for rows.Next() {
		var item ConversationReadState
		if err := rows.Scan(
			&item.UserID,
			&item.PeerID,
			&item.LastReadMessageID,
			&item.LastReadSeq,
		); err != nil {
			return nil, fmt.Errorf("scan conversation read state: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate conversation read states: %w", err)
	}
	return result, nil
}

// ListGroupMemberReadStates returns both read units for group members, paged by
// (group_id, user_id).
func (s *Store) ListGroupMemberReadStates(
	ctx context.Context,
	afterGroupID, afterUserID uint64,
	limit int,
) ([]GroupMemberReadState, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT group_id, user_id, last_read_message_id, last_read_seq
		 FROM group_members
		 WHERE (group_id, user_id) > (?, ?)
		 ORDER BY group_id, user_id
		 LIMIT ?`,
		afterGroupID, afterUserID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list group member read states: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]GroupMemberReadState, 0, limit)
	for rows.Next() {
		var item GroupMemberReadState
		if err := rows.Scan(
			&item.GroupID,
			&item.UserID,
			&item.LastReadMessageID,
			&item.LastReadSeq,
		); err != nil {
			return nil, fmt.Errorf("scan group member read state: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group member read states: %w", err)
	}
	return result, nil
}

// DirectSeqAtOrBefore returns the seq of the last message in the conversation
// whose global ID is not greater than serverMessageID, or zero when none.
func (s *Store) DirectSeqAtOrBefore(
	ctx context.Context,
	lowID, highID, serverMessageID uint64,
) (uint64, error) {
	var seq uint64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE((
		   SELECT seq FROM messages
		   WHERE conversation_low_id = ? AND conversation_high_id = ?
		     AND server_message_id <= ? AND seq IS NOT NULL
		   ORDER BY server_message_id DESC
		   LIMIT 1
		 ), 0)`,
		lowID, highID, serverMessageID,
	).Scan(&seq); err != nil {
		return 0, fmt.Errorf("convert direct read position: %w", err)
	}
	return seq, nil
}

// GroupSeqAtOrBefore returns the seq of the last message in the group whose
// global ID is not greater than serverMessageID, or zero when none.
func (s *Store) GroupSeqAtOrBefore(
	ctx context.Context,
	groupID, serverMessageID uint64,
) (uint64, error) {
	var seq uint64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE((
		   SELECT seq FROM group_messages
		   WHERE group_id = ? AND server_message_id <= ? AND seq IS NOT NULL
		   ORDER BY server_message_id DESC
		   LIMIT 1
		 ), 0)`,
		groupID, serverMessageID,
	).Scan(&seq); err != nil {
		return 0, fmt.Errorf("convert group read position: %w", err)
	}
	return seq, nil
}
