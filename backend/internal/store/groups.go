package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"
)

const maxGroupMembers = 100

var (
	ErrGroupNotFound       = errors.New("group not found")
	ErrNotGroupOwner       = errors.New("not group owner")
	ErrGroupFull           = errors.New("group full")
	ErrGroupMemberNotFound = errors.New("group member not found")
	ErrCannotKickSelf      = errors.New("cannot kick self")
	ErrInvalidGroupMembers = errors.New("invalid group members")
)

// Group is a chat group. A dissolved group keeps its data but is no longer
// accessible or listed.
type Group struct {
	ID          uint64
	Name        string
	OwnerID     uint64
	Owner       PublicUser
	CreatedAt   time.Time
	DissolvedAt *time.Time
}

// GroupMember is a member with their public identity.
type GroupMember struct {
	User     PublicUser
	IsOwner  bool
	JoinedAt time.Time
}

// GroupMembershipProjection is the MySQL source used to rebuild Redis group
// membership state.
type GroupMembershipProjection struct {
	GroupID   uint64
	MemberIDs []uint64
	Dissolved bool
}

// GroupDetail is the group information visible to one of its members.
type GroupDetail struct {
	Group       Group
	Members     []GroupMember
	MemberCount int
	IsOwner     bool
}

// GroupMessage is a persisted group chat message. ID is the public server
// message ID; the MySQL auto-increment primary key is internal only.
type GroupMessage struct {
	ID              uint64
	GroupID         uint64
	SenderID        uint64
	Sender          PublicUser
	ClientMessageID string
	Body            string
	CreatedAt       time.Time
}

// GroupMessagePage is one page of group history, ordered oldest to newest.
type GroupMessagePage struct {
	Messages []GroupMessage
	HasMore  bool
}

// GroupConversation is a group entry in the unified chat list.
type GroupConversation struct {
	Group       Group
	MemberCount int
	IsOwner     bool
	LastMessage *GroupMessage
	UnreadCount int
}

// CreateGroup creates a group owned by ownerID with the given members. Every
// member must be a friend of the owner.
func (s *Store) CreateGroup(
	ctx context.Context,
	ownerID uint64,
	name string,
	memberIDs []uint64,
	now time.Time,
) (GroupDetail, error) {
	members, err := normalizeGroupMemberIDs(ownerID, memberIDs)
	if err != nil {
		return GroupDetail{}, err
	}
	if len(members)+1 > maxGroupMembers {
		return GroupDetail{}, ErrGroupFull
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GroupDetail{}, fmt.Errorf("begin create group transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, memberID := range members {
		if err := requireFriendship(ctx, tx, ownerID, memberID); err != nil {
			return GroupDetail{}, err
		}
	}

	result, err := tx.ExecContext(ctx,
		`INSERT INTO chat_groups (name, owner_id, created_at) VALUES (?, ?, ?)`,
		name, ownerID, now,
	)
	if err != nil {
		return GroupDetail{}, fmt.Errorf("insert group: %w", err)
	}
	groupID, err := result.LastInsertId()
	if err != nil {
		return GroupDetail{}, fmt.Errorf("read group id: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO group_members (group_id, user_id, joined_at, last_read_message_id)
		 VALUES (?, ?, ?, 0)`,
		groupID, ownerID, now,
	); err != nil {
		return GroupDetail{}, fmt.Errorf("insert group owner: %w", err)
	}
	for _, memberID := range members {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO group_members (group_id, user_id, joined_at, last_read_message_id)
			 VALUES (?, ?, ?, 0)`,
			groupID, memberID, now,
		); err != nil {
			return GroupDetail{}, fmt.Errorf("insert group member: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return GroupDetail{}, fmt.Errorf("commit create group: %w", err)
	}
	return s.GetGroup(ctx, ownerID, uint64(groupID))
}

// ListGroupConversations returns groups the user belongs to with their latest
// message and unread count.
func (s *Store) ListGroupConversations(
	ctx context.Context,
	userID uint64,
) ([]GroupConversation, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT g.id, g.name, g.owner_id, owner.username, owner.nickname, g.created_at,
		        (SELECT COUNT(*) FROM group_members count_member
		         WHERE count_member.group_id = g.id)
		 FROM chat_groups g
		 JOIN group_members member
		   ON member.group_id = g.id AND member.user_id = ?
		 JOIN users owner ON owner.id = g.owner_id
		 WHERE g.dissolved_at IS NULL`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list group conversations: %w", err)
	}

	conversations := make([]GroupConversation, 0)
	indexByGroupID := make(map[uint64]int)
	for rows.Next() {
		var item GroupConversation
		var memberCount int64
		if err := rows.Scan(
			&item.Group.ID,
			&item.Group.Name,
			&item.Group.OwnerID,
			&item.Group.Owner.Username,
			&item.Group.Owner.Nickname,
			&item.Group.CreatedAt,
			&memberCount,
		); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan group conversation: %w", err)
		}
		item.MemberCount = int(memberCount)
		item.IsOwner = item.Group.OwnerID == userID
		indexByGroupID[item.Group.ID] = len(conversations)
		conversations = append(conversations, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate group conversations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close group conversations: %w", err)
	}
	if len(conversations) == 0 {
		return conversations, nil
	}

	latestRows, err := s.db.QueryContext(ctx,
		`SELECT gm.server_message_id, gm.group_id, gm.sender_id, sender.username, sender.nickname,
		        gm.client_message_id, gm.body, gm.created_at
		 FROM group_messages gm
		 JOIN chat_groups g ON g.id = gm.group_id AND g.dissolved_at IS NULL
		 JOIN group_members member
		   ON member.group_id = gm.group_id AND member.user_id = ?
		 JOIN users sender ON sender.id = gm.sender_id
		 WHERE gm.server_message_id IN (
		     SELECT MAX(latest.server_message_id)
		     FROM group_messages latest
		     JOIN group_members latest_member
		       ON latest_member.group_id = latest.group_id
		      AND latest_member.user_id = ?
		     GROUP BY latest.group_id
		 )`,
		userID, userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list latest group messages: %w", err)
	}
	for latestRows.Next() {
		var message GroupMessage
		if err := latestRows.Scan(
			&message.ID,
			&message.GroupID,
			&message.SenderID,
			&message.Sender.Username,
			&message.Sender.Nickname,
			&message.ClientMessageID,
			&message.Body,
			&message.CreatedAt,
		); err != nil {
			_ = latestRows.Close()
			return nil, fmt.Errorf("scan latest group message: %w", err)
		}
		if index, ok := indexByGroupID[message.GroupID]; ok {
			messageCopy := message
			conversations[index].LastMessage = &messageCopy
		}
	}
	if err := latestRows.Err(); err != nil {
		_ = latestRows.Close()
		return nil, fmt.Errorf("iterate latest group messages: %w", err)
	}
	if err := latestRows.Close(); err != nil {
		return nil, fmt.Errorf("close latest group messages: %w", err)
	}

	unreadRows, err := s.db.QueryContext(ctx,
		`SELECT gm.group_id, COUNT(*)
		 FROM group_messages gm
		 JOIN group_members member
		   ON member.group_id = gm.group_id AND member.user_id = ?
		 JOIN chat_groups g ON g.id = gm.group_id AND g.dissolved_at IS NULL
		 WHERE gm.server_message_id > member.last_read_message_id
		   AND gm.sender_id <> ?
		 GROUP BY gm.group_id`,
		userID, userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list group unread counts: %w", err)
	}
	for unreadRows.Next() {
		var groupID uint64
		var unread int64
		if err := unreadRows.Scan(&groupID, &unread); err != nil {
			_ = unreadRows.Close()
			return nil, fmt.Errorf("scan group unread count: %w", err)
		}
		if index, ok := indexByGroupID[groupID]; ok {
			conversations[index].UnreadCount = int(unread)
		}
	}
	if err := unreadRows.Err(); err != nil {
		_ = unreadRows.Close()
		return nil, fmt.Errorf("iterate group unread counts: %w", err)
	}
	if err := unreadRows.Close(); err != nil {
		return nil, fmt.Errorf("close group unread counts: %w", err)
	}

	sort.SliceStable(conversations, func(i, j int) bool {
		left, right := conversations[i], conversations[j]
		switch {
		case left.LastMessage != nil && right.LastMessage != nil:
			return left.LastMessage.CreatedAt.After(right.LastMessage.CreatedAt)
		case left.LastMessage != nil:
			return true
		case right.LastMessage != nil:
			return false
		}
		return left.Group.CreatedAt.After(right.Group.CreatedAt)
	})
	return conversations, nil
}

// GetGroup returns group information and members. Non-members cannot see the
// group, and dissolved groups are treated as not found.
func (s *Store) GetGroup(
	ctx context.Context,
	userID, groupID uint64,
) (GroupDetail, error) {
	var detail GroupDetail
	err := s.db.QueryRowContext(ctx,
		`SELECT g.id, g.name, g.owner_id, owner.username, owner.nickname, g.created_at
		 FROM chat_groups g
		 JOIN users owner ON owner.id = g.owner_id
		 JOIN group_members member
		   ON member.group_id = g.id AND member.user_id = ?
		 WHERE g.id = ? AND g.dissolved_at IS NULL`,
		userID, groupID,
	).Scan(
		&detail.Group.ID,
		&detail.Group.Name,
		&detail.Group.OwnerID,
		&detail.Group.Owner.Username,
		&detail.Group.Owner.Nickname,
		&detail.Group.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return GroupDetail{}, ErrGroupNotFound
	}
	if err != nil {
		return GroupDetail{}, fmt.Errorf("get group: %w", err)
	}
	detail.IsOwner = detail.Group.OwnerID == userID

	rows, err := s.db.QueryContext(ctx,
		`SELECT u.id, u.username, u.nickname, member.joined_at
		 FROM group_members member
		 JOIN users u ON u.id = member.user_id
		 WHERE member.group_id = ?
		 ORDER BY CASE WHEN member.user_id = ? THEN 0 ELSE 1 END,
		          member.joined_at ASC, u.username ASC`,
		groupID, detail.Group.OwnerID,
	)
	if err != nil {
		return GroupDetail{}, fmt.Errorf("list group members: %w", err)
	}
	defer func() { _ = rows.Close() }()

	detail.Members = make([]GroupMember, 0)
	for rows.Next() {
		var member GroupMember
		var memberID uint64
		if err := rows.Scan(
			&memberID,
			&member.User.Username,
			&member.User.Nickname,
			&member.JoinedAt,
		); err != nil {
			return GroupDetail{}, fmt.Errorf("scan group member: %w", err)
		}
		member.IsOwner = memberID == detail.Group.OwnerID
		detail.Members = append(detail.Members, member)
	}
	if err := rows.Err(); err != nil {
		return GroupDetail{}, fmt.Errorf("iterate group members: %w", err)
	}
	detail.MemberCount = len(detail.Members)
	return detail, nil
}

// IsGroupMember reports whether the user currently belongs to a live group.
func (s *Store) IsGroupMember(
	ctx context.Context,
	userID, groupID uint64,
) (bool, error) {
	var found int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1
		 FROM chat_groups g
		 JOIN group_members member
		   ON member.group_id = g.id AND member.user_id = ?
		 WHERE g.id = ? AND g.dissolved_at IS NULL`,
		userID, groupID,
	).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check group membership: %w", err)
	}
	return true, nil
}

// RenameGroup changes the group name when the user is the owner.
func (s *Store) RenameGroup(
	ctx context.Context,
	ownerID, groupID uint64,
	name string,
) error {
	return s.updateGroupAsOwner(ctx, ownerID, groupID, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`UPDATE chat_groups SET name = ? WHERE id = ?`,
			name, groupID,
		); err != nil {
			return fmt.Errorf("rename group: %w", err)
		}
		return nil
	})
}

// InviteGroupMembers adds the inviter's friends to the group. The whole
// invitation is rejected when any non-member is not the inviter's friend or
// the group would exceed its member limit.
func (s *Store) InviteGroupMembers(
	ctx context.Context,
	groupID, inviterID uint64,
	memberIDs []uint64,
	now time.Time,
) error {
	members, err := normalizeGroupMemberIDs(inviterID, memberIDs)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin invite group members transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := lockActiveGroup(ctx, tx, groupID); err != nil {
		return err
	}

	var isMember int
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?)`,
		groupID, inviterID,
	).Scan(&isMember); err != nil {
		return fmt.Errorf("check group inviter: %w", err)
	}
	if isMember == 0 {
		return ErrGroupNotFound
	}

	memberRows, err := tx.QueryContext(ctx,
		`SELECT user_id FROM group_members WHERE group_id = ? ORDER BY user_id FOR UPDATE`,
		groupID,
	)
	if err != nil {
		return fmt.Errorf("lock group members: %w", err)
	}
	existing := make(map[uint64]struct{})
	for memberRows.Next() {
		var memberID uint64
		if err := memberRows.Scan(&memberID); err != nil {
			_ = memberRows.Close()
			return fmt.Errorf("scan locked group member: %w", err)
		}
		existing[memberID] = struct{}{}
	}
	if err := memberRows.Err(); err != nil {
		_ = memberRows.Close()
		return fmt.Errorf("iterate locked group members: %w", err)
	}
	if err := memberRows.Close(); err != nil {
		return fmt.Errorf("close locked group members: %w", err)
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
			return fmt.Errorf("commit empty group invite: %w", err)
		}
		return nil
	}
	if len(existing)+len(newMembers) > maxGroupMembers {
		return ErrGroupFull
	}
	for _, memberID := range newMembers {
		if err := requireFriendship(ctx, tx, inviterID, memberID); err != nil {
			return err
		}
	}

	var latestMessageID uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(server_message_id), 0) FROM group_messages WHERE group_id = ?`,
		groupID,
	).Scan(&latestMessageID); err != nil {
		return fmt.Errorf("find latest group message: %w", err)
	}
	for _, memberID := range newMembers {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO group_members
			   (group_id, user_id, joined_at, last_read_message_id)
			 VALUES (?, ?, ?, ?)`,
			groupID, memberID, now, latestMessageID,
		); err != nil {
			return fmt.Errorf("insert invited group member: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit group invite: %w", err)
	}
	return nil
}

// KickGroupMember removes a member when the user is the group owner.
func (s *Store) KickGroupMember(
	ctx context.Context,
	ownerID, groupID, targetID uint64,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin kick group member transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	actualOwnerID, err := lockActiveGroup(ctx, tx, groupID)
	if err != nil {
		return err
	}
	if actualOwnerID != ownerID {
		return ErrNotGroupOwner
	}
	if targetID == ownerID {
		return ErrCannotKickSelf
	}

	result, err := tx.ExecContext(ctx,
		`DELETE FROM group_members WHERE group_id = ? AND user_id = ?`,
		groupID, targetID,
	)
	if err != nil {
		return fmt.Errorf("delete kicked group member: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read kicked group member rows: %w", err)
	}
	if affected == 0 {
		return ErrGroupMemberNotFound
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit group kick: %w", err)
	}
	return nil
}

// LeaveGroup removes the member. When the owner leaves, the group is
// dissolved instead. The returned bool reports whether the group was
// dissolved.
func (s *Store) LeaveGroup(
	ctx context.Context,
	userID, groupID uint64,
	now time.Time,
) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin leave group transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ownerID, err := lockActiveGroup(ctx, tx, groupID)
	if err != nil {
		return false, err
	}

	var isMember int
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?)`,
		groupID, userID,
	).Scan(&isMember); err != nil {
		return false, fmt.Errorf("check leaving group member: %w", err)
	}
	if isMember == 0 {
		return false, ErrGroupNotFound
	}

	if ownerID == userID {
		if _, err := tx.ExecContext(ctx,
			`UPDATE chat_groups SET dissolved_at = ? WHERE id = ?`,
			now, groupID,
		); err != nil {
			return false, fmt.Errorf("dissolve group: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit group dissolve: %w", err)
		}
		return true, nil
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM group_members WHERE group_id = ? AND user_id = ?`,
		groupID, userID,
	); err != nil {
		return false, fmt.Errorf("delete leaving group member: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit group leave: %w", err)
	}
	return false, nil
}

// ListGroupMemberIDs returns all member IDs, including the owner. Dissolved
// groups keep their membership rows so they can still be notified.
func (s *Store) ListGroupMemberIDs(ctx context.Context, groupID uint64) ([]uint64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT user_id FROM group_members WHERE group_id = ? ORDER BY user_id`,
		groupID,
	)
	if err != nil {
		return nil, fmt.Errorf("list group member ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	memberIDs := make([]uint64, 0)
	for rows.Next() {
		var memberID uint64
		if err := rows.Scan(&memberID); err != nil {
			return nil, fmt.Errorf("scan group member id: %w", err)
		}
		memberIDs = append(memberIDs, memberID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group member ids: %w", err)
	}
	return memberIDs, nil
}

// ListGroupMembershipProjections returns all groups and their current member
// rows for rebuilding the Redis group projection.
func (s *Store) ListGroupMembershipProjections(ctx context.Context) ([]GroupMembershipProjection, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT g.id, g.dissolved_at, member.user_id
		 FROM chat_groups g
		 LEFT JOIN group_members member ON member.group_id = g.id
		 ORDER BY g.id, member.user_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("list group membership projections: %w", err)
	}
	defer func() { _ = rows.Close() }()

	projections := make([]GroupMembershipProjection, 0)
	indexByGroupID := make(map[uint64]int)
	for rows.Next() {
		var groupID uint64
		var dissolvedAt sql.NullTime
		var memberID sql.NullInt64
		if err := rows.Scan(&groupID, &dissolvedAt, &memberID); err != nil {
			return nil, fmt.Errorf("scan group membership projection: %w", err)
		}
		index, ok := indexByGroupID[groupID]
		if !ok {
			index = len(projections)
			indexByGroupID[groupID] = index
			projections = append(projections, GroupMembershipProjection{
				GroupID:   groupID,
				Dissolved: dissolvedAt.Valid,
			})
		}
		if memberID.Valid {
			projections[index].MemberIDs = append(projections[index].MemberIDs, uint64(memberID.Int64))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group membership projections: %w", err)
	}
	return projections, nil
}

// ListGroupMessages returns a page of group messages. A beforeID of zero
// starts from the newest message. Messages are returned oldest first.
func (s *Store) ListGroupMessages(
	ctx context.Context,
	userID, groupID, beforeID uint64,
	limit int,
) (GroupMessagePage, error) {
	if err := s.ensureGroupMember(ctx, userID, groupID); err != nil {
		return GroupMessagePage{}, err
	}

	query := `SELECT gm.server_message_id, gm.group_id, gm.sender_id, sender.username, sender.nickname,
	                 gm.client_message_id, gm.body, gm.created_at
	          FROM group_messages gm
	          JOIN users sender ON sender.id = gm.sender_id
	          WHERE gm.group_id = ?`
	args := []any{groupID}
	if beforeID > 0 {
		query += ` AND gm.server_message_id < ?`
		args = append(args, beforeID)
	}
	query += ` ORDER BY gm.server_message_id DESC LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return GroupMessagePage{}, fmt.Errorf("list group messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	newestFirst := make([]GroupMessage, 0, limit+1)
	for rows.Next() {
		var message GroupMessage
		if err := rows.Scan(
			&message.ID,
			&message.GroupID,
			&message.SenderID,
			&message.Sender.Username,
			&message.Sender.Nickname,
			&message.ClientMessageID,
			&message.Body,
			&message.CreatedAt,
		); err != nil {
			return GroupMessagePage{}, fmt.Errorf("scan group message: %w", err)
		}
		newestFirst = append(newestFirst, message)
	}
	if err := rows.Err(); err != nil {
		return GroupMessagePage{}, fmt.Errorf("iterate group messages: %w", err)
	}

	hasMore := len(newestFirst) > limit
	if hasMore {
		newestFirst = newestFirst[:limit]
	}
	messages := make([]GroupMessage, len(newestFirst))
	for index := range newestFirst {
		messages[len(newestFirst)-1-index] = newestFirst[index]
	}
	return GroupMessagePage{Messages: messages, HasMore: hasMore}, nil
}

// ListGroupMessagesAfter returns messages newer than afterID, oldest first.
func (s *Store) ListGroupMessagesAfter(
	ctx context.Context,
	userID, groupID, afterID uint64,
	limit int,
) (GroupMessagePage, error) {
	if err := s.ensureGroupMember(ctx, userID, groupID); err != nil {
		return GroupMessagePage{}, err
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT gm.server_message_id, gm.group_id, gm.sender_id, sender.username, sender.nickname,
		        gm.client_message_id, gm.body, gm.created_at
		 FROM group_messages gm
		 JOIN users sender ON sender.id = gm.sender_id
		 WHERE gm.group_id = ? AND gm.server_message_id > ?
		 ORDER BY gm.server_message_id ASC LIMIT ?`,
		groupID, afterID, limit+1,
	)
	if err != nil {
		return GroupMessagePage{}, fmt.Errorf("list group messages after: %w", err)
	}
	defer func() { _ = rows.Close() }()

	messages := make([]GroupMessage, 0, limit+1)
	for rows.Next() {
		var message GroupMessage
		if err := rows.Scan(
			&message.ID,
			&message.GroupID,
			&message.SenderID,
			&message.Sender.Username,
			&message.Sender.Nickname,
			&message.ClientMessageID,
			&message.Body,
			&message.CreatedAt,
		); err != nil {
			return GroupMessagePage{}, fmt.Errorf("scan group message after: %w", err)
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return GroupMessagePage{}, fmt.Errorf("iterate group messages after: %w", err)
	}

	hasMore := len(messages) > limit
	if hasMore {
		messages = messages[:limit]
	}
	return GroupMessagePage{Messages: messages, HasMore: hasMore}, nil
}

// CreateGroupMessage saves a message when the sender is a member. The
// returned bool is false when the same client message was already saved.
func (s *Store) CreateGroupMessage(
	ctx context.Context,
	senderID, groupID uint64,
	clientMessageID, body string,
	now time.Time,
) (GroupMessage, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GroupMessage{}, false, fmt.Errorf("begin group message transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var dissolvedAt sql.NullTime
	if err := tx.QueryRowContext(ctx,
		`SELECT dissolved_at FROM chat_groups WHERE id = ? FOR UPDATE`,
		groupID,
	).Scan(&dissolvedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GroupMessage{}, false, ErrGroupNotFound
		}
		return GroupMessage{}, false, fmt.Errorf("lock group for message: %w", err)
	}
	if dissolvedAt.Valid {
		return GroupMessage{}, false, ErrGroupNotFound
	}

	var isMember int
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ?)`,
		groupID, senderID,
	).Scan(&isMember); err != nil {
		return GroupMessage{}, false, fmt.Errorf("check group message sender: %w", err)
	}
	if isMember == 0 {
		return GroupMessage{}, false, ErrGroupNotFound
	}

	existing, err := loadGroupMessageByClientID(ctx, tx, groupID, senderID, clientMessageID)
	if err == nil {
		if existing.Body != body {
			return GroupMessage{}, false, ErrMessageConflict
		}
		if err := tx.Commit(); err != nil {
			return GroupMessage{}, false, fmt.Errorf("commit existing group message: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return GroupMessage{}, false, err
	}

	serverMessageID, err := nextMessageID(ctx, tx)
	if err != nil {
		return GroupMessage{}, false, err
	}
	if serverMessageID > MaxSafeMessageID {
		return GroupMessage{}, false, ErrMessageIDExhausted
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO group_messages
		   (server_message_id, group_id, sender_id, client_message_id, body, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		serverMessageID, groupID, senderID, clientMessageID, body, now,
	); err != nil {
		return GroupMessage{}, false, fmt.Errorf("insert group message: %w", err)
	}
	sender, err := publicUserByID(ctx, tx, senderID)
	if err != nil {
		return GroupMessage{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return GroupMessage{}, false, fmt.Errorf("commit group message: %w", err)
	}
	return GroupMessage{
		ID:              serverMessageID,
		GroupID:         groupID,
		SenderID:        senderID,
		Sender:          sender,
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       now,
	}, true, nil
}

// AdvanceGroupReadThrough advances a member's group read position without
// requiring the message to be present in MySQL yet. The accepted watermark is
// supplied by the Redis message path.
func (s *Store) AdvanceGroupReadThrough(
	ctx context.Context,
	userID, groupID, throughID uint64,
) (uint64, bool, error) {
	if throughID == 0 {
		return 0, false, nil
	}
	if err := s.ensureGroupMember(ctx, userID, groupID); err != nil {
		return 0, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("begin advance group read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var existing uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT last_read_message_id
		 FROM group_members
		 WHERE group_id = ? AND user_id = ?
		 FOR UPDATE`,
		groupID, userID,
	).Scan(&existing); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, ErrGroupNotFound
		}
		return 0, false, fmt.Errorf("load group read position: %w", err)
	}
	if throughID <= existing {
		if err := tx.Commit(); err != nil {
			return 0, false, fmt.Errorf("commit unchanged group read position: %w", err)
		}
		return existing, false, nil
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE group_members SET last_read_message_id = ?
		 WHERE group_id = ? AND user_id = ?`,
		throughID, groupID, userID,
	); err != nil {
		return 0, false, fmt.Errorf("update group read position: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("commit group read position: %w", err)
	}
	return throughID, true, nil
}

// PersistGroupMessage inserts an asynchronously accepted group message
// idempotently. It does not re-check membership because acceptance already did.
func (s *Store) PersistGroupMessage(ctx context.Context, message GroupMessage) (bool, error) {
	if message.ID == 0 || message.ID > MaxSafeMessageID {
		return false, ErrMessageIDExhausted
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin persist group message transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx,
		`INSERT INTO group_messages
		   (server_message_id, group_id, sender_id, client_message_id, body, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE id = id`,
		message.ID,
		message.GroupID,
		message.SenderID,
		message.ClientMessageID,
		message.Body,
		message.CreatedAt,
	)
	if err != nil {
		return false, fmt.Errorf("persist group message: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read persist group message result: %w", err)
	}
	if rows == 1 {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit persisted group message: %w", err)
		}
		return true, nil
	}
	if rows != 0 {
		return false, fmt.Errorf("unexpected persist group message rows affected: %d", rows)
	}

	existing, err := loadGroupMessageByServerID(ctx, tx, message.ID)
	if errors.Is(err, sql.ErrNoRows) {
		existing, err = loadGroupMessageByClientID(ctx, tx, message.GroupID, message.SenderID, message.ClientMessageID)
	}
	if err != nil {
		return false, fmt.Errorf("load duplicate persisted group message: %w", err)
	}
	if !sameGroupMessage(existing, message) {
		return false, ErrMessageConflict
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit duplicate persisted group message: %w", err)
	}
	return false, nil
}

func sameGroupMessage(left, right GroupMessage) bool {
	return left.ID == right.ID &&
		left.GroupID == right.GroupID &&
		left.SenderID == right.SenderID &&
		left.ClientMessageID == right.ClientMessageID &&
		left.Body == right.Body &&
		left.CreatedAt.Truncate(time.Millisecond).Equal(right.CreatedAt.Truncate(time.Millisecond))
}

// MarkGroupRead advances the member's read position to the latest group
// message, capped at throughID when nonzero. The returned bool reports whether
// it advanced.
func (s *Store) MarkGroupRead(
	ctx context.Context,
	userID, groupID, throughID uint64,
) (uint64, bool, error) {
	if err := s.ensureGroupMember(ctx, userID, groupID); err != nil {
		return 0, false, err
	}

	var latest uint64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(server_message_id), 0) FROM group_messages
		 WHERE group_id = ? AND (? = 0 OR server_message_id <= ?)`,
		groupID, throughID, throughID,
	).Scan(&latest); err != nil {
		return 0, false, fmt.Errorf("find latest group message: %w", err)
	}
	if latest == 0 {
		return 0, false, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("begin mark group read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var existing uint64
	err = tx.QueryRowContext(ctx,
		`SELECT last_read_message_id
		 FROM group_members
		 WHERE group_id = ? AND user_id = ?
		 FOR UPDATE`,
		groupID, userID,
	).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, ErrGroupNotFound
	}
	if err != nil {
		return 0, false, fmt.Errorf("load group read position: %w", err)
	}

	advanced := false
	if latest > existing {
		if _, err := tx.ExecContext(ctx,
			`UPDATE group_members SET last_read_message_id = ?
			 WHERE group_id = ? AND user_id = ?`,
			latest, groupID, userID,
		); err != nil {
			return 0, false, fmt.Errorf("update group read position: %w", err)
		}
		advanced = true
	}

	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("commit group read position: %w", err)
	}
	if advanced {
		return latest, true, nil
	}
	return existing, false, nil
}

func (s *Store) ensureGroupMember(ctx context.Context, userID, groupID uint64) error {
	var isMember int
	if err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(
		   SELECT 1
		   FROM group_members member
		   JOIN chat_groups g ON g.id = member.group_id
		   WHERE member.group_id = ? AND member.user_id = ?
		     AND g.dissolved_at IS NULL
		 )`,
		groupID, userID,
	).Scan(&isMember); err != nil {
		return fmt.Errorf("check group membership: %w", err)
	}
	if isMember == 0 {
		return ErrGroupNotFound
	}
	return nil
}

func (s *Store) updateGroupAsOwner(
	ctx context.Context,
	ownerID, groupID uint64,
	update func(tx *sql.Tx) error,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin group owner transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	actualOwnerID, err := lockActiveGroup(ctx, tx, groupID)
	if err != nil {
		return err
	}
	if actualOwnerID != ownerID {
		return ErrNotGroupOwner
	}
	if err := update(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit group owner update: %w", err)
	}
	return nil
}

func lockActiveGroup(ctx context.Context, tx *sql.Tx, groupID uint64) (uint64, error) {
	var ownerID uint64
	var dissolvedAt sql.NullTime
	err := tx.QueryRowContext(ctx,
		`SELECT owner_id, dissolved_at FROM chat_groups WHERE id = ? FOR UPDATE`,
		groupID,
	).Scan(&ownerID, &dissolvedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrGroupNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("lock group: %w", err)
	}
	if dissolvedAt.Valid {
		return 0, ErrGroupNotFound
	}
	return ownerID, nil
}

func normalizeGroupMemberIDs(ownerID uint64, memberIDs []uint64) ([]uint64, error) {
	if len(memberIDs) == 0 {
		return nil, ErrInvalidGroupMembers
	}
	seen := make(map[uint64]struct{}, len(memberIDs))
	members := make([]uint64, 0, len(memberIDs))
	for _, memberID := range memberIDs {
		if memberID == 0 || memberID == ownerID {
			return nil, ErrInvalidGroupMembers
		}
		if _, ok := seen[memberID]; ok {
			continue
		}
		seen[memberID] = struct{}{}
		members = append(members, memberID)
	}
	if len(members) == 0 {
		return nil, ErrInvalidGroupMembers
	}
	return members, nil
}

func requireFriendship(ctx context.Context, tx *sql.Tx, userID, friendID uint64) error {
	var areFriends int
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM friendships WHERE user_id = ? AND friend_user_id = ?)`,
		userID, friendID,
	).Scan(&areFriends); err != nil {
		return fmt.Errorf("check group friendship: %w", err)
	}
	if areFriends == 0 {
		return ErrNotFriends
	}
	return nil
}

func loadGroupMessageByServerID(
	ctx context.Context,
	tx *sql.Tx,
	serverMessageID uint64,
) (GroupMessage, error) {
	var message GroupMessage
	err := tx.QueryRowContext(ctx,
		`SELECT gm.server_message_id, gm.group_id, gm.sender_id, sender.username, sender.nickname,
		        gm.client_message_id, gm.body, gm.created_at
		 FROM group_messages gm
		 JOIN users sender ON sender.id = gm.sender_id
		 WHERE gm.server_message_id = ?
		 FOR UPDATE`,
		serverMessageID,
	).Scan(
		&message.ID,
		&message.GroupID,
		&message.SenderID,
		&message.Sender.Username,
		&message.Sender.Nickname,
		&message.ClientMessageID,
		&message.Body,
		&message.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return GroupMessage{}, sql.ErrNoRows
	}
	if err != nil {
		return GroupMessage{}, fmt.Errorf("load group message by server id: %w", err)
	}
	return message, nil
}

func loadGroupMessageByClientID(
	ctx context.Context,
	tx *sql.Tx,
	groupID, senderID uint64,
	clientMessageID string,
) (GroupMessage, error) {
	var message GroupMessage
	err := tx.QueryRowContext(ctx,
		`SELECT gm.server_message_id, gm.group_id, gm.sender_id, sender.username, sender.nickname,
		        gm.client_message_id, gm.body, gm.created_at
		 FROM group_messages gm
		 JOIN users sender ON sender.id = gm.sender_id
		 WHERE gm.group_id = ? AND gm.sender_id = ? AND gm.client_message_id = ?
		 FOR UPDATE`,
		groupID, senderID, clientMessageID,
	).Scan(
		&message.ID,
		&message.GroupID,
		&message.SenderID,
		&message.Sender.Username,
		&message.Sender.Nickname,
		&message.ClientMessageID,
		&message.Body,
		&message.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return GroupMessage{}, sql.ErrNoRows
	}
	if err != nil {
		return GroupMessage{}, fmt.Errorf("load group message by client id: %w", err)
	}
	return message, nil
}
