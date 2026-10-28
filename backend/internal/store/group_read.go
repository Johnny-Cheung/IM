package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// GroupMessageIndex is the minimal group-message row needed to rebuild the
// Redis group inbox.
type GroupMessageIndex struct {
	GroupID   uint64
	MessageID uint64
}

// GroupMemberRead is one persisted group read position.
type GroupMemberRead struct {
	GroupID           uint64
	UserID            uint64
	LastReadMessageID uint64
}

// GroupLastOwnMessage is the newest message a member sent to a group.
type GroupLastOwnMessage struct {
	GroupID   uint64
	SenderID  uint64
	MessageID uint64
}

// GroupMessageCount is the number of persisted messages in one group.
type GroupMessageCount struct {
	GroupID uint64
	Count   uint64
}

// ListGroupMessageIndex returns persisted group messages after afterMessageID,
// ordered by the public server message ID.
func (s *Store) ListGroupMessageIndex(
	ctx context.Context,
	afterMessageID uint64,
	limit int,
) ([]GroupMessageIndex, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT group_id, server_message_id
		 FROM group_messages
		 WHERE server_message_id > ?
		 ORDER BY server_message_id
		 LIMIT ?`,
		afterMessageID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list group message index: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]GroupMessageIndex, 0, limit)
	for rows.Next() {
		var item GroupMessageIndex
		if err := rows.Scan(&item.GroupID, &item.MessageID); err != nil {
			return nil, fmt.Errorf("scan group message index: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group message index: %w", err)
	}
	return result, nil
}

// ListGroupMemberReads returns persisted member read positions after the given
// primary key, ordered by (group_id, user_id).
func (s *Store) ListGroupMemberReads(
	ctx context.Context,
	afterGroupID, afterUserID uint64,
	limit int,
) ([]GroupMemberRead, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT group_id, user_id, last_read_message_id
		 FROM group_members
		 WHERE (group_id, user_id) > (?, ?)
		 ORDER BY group_id, user_id
		 LIMIT ?`,
		afterGroupID, afterUserID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list group member reads: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]GroupMemberRead, 0, limit)
	for rows.Next() {
		var item GroupMemberRead
		if err := rows.Scan(&item.GroupID, &item.UserID, &item.LastReadMessageID); err != nil {
			return nil, fmt.Errorf("scan group member read: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group member reads: %w", err)
	}
	return result, nil
}

// ListGroupLastOwnMessages returns the newest persisted message each member
// sent to each group.
func (s *Store) ListGroupLastOwnMessages(ctx context.Context) ([]GroupLastOwnMessage, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT group_id, sender_id, MAX(server_message_id)
		 FROM group_messages
		 GROUP BY group_id, sender_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("list group last own messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]GroupLastOwnMessage, 0)
	for rows.Next() {
		var item GroupLastOwnMessage
		if err := rows.Scan(&item.GroupID, &item.SenderID, &item.MessageID); err != nil {
			return nil, fmt.Errorf("scan group last own message: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group last own messages: %w", err)
	}
	return result, nil
}

// CountGroupMessagesByGroup returns the number of persisted messages per group.
func (s *Store) CountGroupMessagesByGroup(
	ctx context.Context,
) (map[uint64]uint64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT group_id, COUNT(*) FROM group_messages GROUP BY group_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("count group messages by group: %w", err)
	}
	defer func() { _ = rows.Close() }()

	counts := make(map[uint64]uint64)
	for rows.Next() {
		var groupID, count uint64
		if err := rows.Scan(&groupID, &count); err != nil {
			return nil, fmt.Errorf("scan group message count: %w", err)
		}
		counts[groupID] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group message counts: %w", err)
	}
	return counts, nil
}

// UpsertGroupRead advances a member's persisted group read position
// monotonically. The bool reports whether the member row exists.
func (s *Store) UpsertGroupRead(
	ctx context.Context,
	groupID, userID, lastReadMessageID uint64,
) (bool, error) {
	result, err := s.db.ExecContext(ctx,
		`UPDATE group_members
		 SET last_read_message_id = GREATEST(last_read_message_id, ?)
		 WHERE group_id = ? AND user_id = ?`,
		lastReadMessageID, groupID, userID,
	)
	if err != nil {
		return false, fmt.Errorf("upsert group read: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read upsert group read rows: %w", err)
	}
	if affected > 0 {
		return true, nil
	}

	// MySQL reports zero affected rows for an unchanged value unless the DSN
	// enables clientFoundRows, so check existence separately.
	var found int
	err = s.db.QueryRowContext(ctx,
		`SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?`,
		groupID, userID,
	).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check superseded group member: %w", err)
	}
	return true, nil
}

// GroupMessageSender returns the persisted sender of a group message.
func (s *Store) GroupMessageSender(
	ctx context.Context,
	groupID, messageID uint64,
) (uint64, bool, error) {
	var senderID uint64
	err := s.db.QueryRowContext(ctx,
		`SELECT sender_id FROM group_messages
		 WHERE group_id = ? AND server_message_id = ?`,
		groupID, messageID,
	).Scan(&senderID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("get group message sender: %w", err)
	}
	return senderID, true, nil
}

// InviteGroupMembersWithBaseline adds the inviter's friends to the group with
// a read baseline of max(newest persisted message, baselineFloor). It returns
// that baseline, or zero when every requested member already belongs.
func (s *Store) InviteGroupMembersWithBaseline(
	ctx context.Context,
	groupID, inviterID uint64,
	memberIDs []uint64,
	now time.Time,
	baselineFloor uint64,
) (uint64, error) {
	members, err := normalizeGroupMemberIDs(inviterID, memberIDs)
	if err != nil {
		return 0, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin invite group members transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := lockActiveGroup(ctx, tx, groupID); err != nil {
		return 0, err
	}

	var isMember int
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?)`,
		groupID, inviterID,
	).Scan(&isMember); err != nil {
		return 0, fmt.Errorf("check group inviter: %w", err)
	}
	if isMember == 0 {
		return 0, ErrGroupNotFound
	}

	memberRows, err := tx.QueryContext(ctx,
		`SELECT user_id FROM group_members WHERE group_id = ? ORDER BY user_id FOR UPDATE`,
		groupID,
	)
	if err != nil {
		return 0, fmt.Errorf("lock group members: %w", err)
	}
	existing := make(map[uint64]struct{})
	for memberRows.Next() {
		var memberID uint64
		if err := memberRows.Scan(&memberID); err != nil {
			_ = memberRows.Close()
			return 0, fmt.Errorf("scan locked group member: %w", err)
		}
		existing[memberID] = struct{}{}
	}
	if err := memberRows.Err(); err != nil {
		_ = memberRows.Close()
		return 0, fmt.Errorf("iterate locked group members: %w", err)
	}
	if err := memberRows.Close(); err != nil {
		return 0, fmt.Errorf("close locked group members: %w", err)
	}

	newMembers := make([]uint64, 0, len(members))
	for _, memberID := range members {
		if _, ok := existing[memberID]; ok {
			continue
		}
		newMembers = append(newMembers, memberID)
	}
	if len(newMembers) == 0 {
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("commit empty group invite: %w", err)
		}
		return 0, nil
	}
	if len(existing)+len(newMembers) > maxGroupMembers {
		return 0, ErrGroupFull
	}
	for _, memberID := range newMembers {
		if err := requireFriendship(ctx, tx, inviterID, memberID); err != nil {
			return 0, err
		}
	}

	var latestMessageID uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(server_message_id), 0) FROM group_messages WHERE group_id = ?`,
		groupID,
	).Scan(&latestMessageID); err != nil {
		return 0, fmt.Errorf("find latest group message: %w", err)
	}
	baseline := latestMessageID
	if baselineFloor > baseline {
		baseline = baselineFloor
	}
	for _, memberID := range newMembers {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO group_members
			   (group_id, user_id, joined_at, last_read_message_id)
			 VALUES (?, ?, ?, ?)`,
			groupID, memberID, now, baseline,
		); err != nil {
			return 0, fmt.Errorf("insert invited group member: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit group invite: %w", err)
	}
	return baseline, nil
}
