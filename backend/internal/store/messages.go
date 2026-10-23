package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

var (
	ErrNotFriends        = errors.New("not friends")
	ErrCannotMessageSelf = errors.New("cannot message self")
)

// Message is a persisted one-to-one chat message.
type Message struct {
	ID              uint64
	SenderID        uint64
	RecipientID     uint64
	ClientMessageID string
	Body            string
	CreatedAt       time.Time
}

// MessagePage is one page of history, ordered oldest to newest.
type MessagePage struct {
	Messages []Message
	HasMore  bool
}

// Conversation is a chat list entry with the other user and latest message.
type Conversation struct {
	User                  PublicUser
	IsFriend              bool
	LastMessage           *Message
	UnreadCount           int
	PeerLastReadMessageID uint64
}

// ListConversations returns all friends plus users with existing messages,
// newest conversations first and friends without messages by nickname.
func (s *Store) ListConversations(ctx context.Context, userID uint64) ([]Conversation, error) {
	conversations := make([]Conversation, 0)

	friendRows, err := s.db.QueryContext(ctx,
		`SELECT u.username, u.nickname
		 FROM friendships f
		 JOIN users u ON u.id = f.friend_user_id
		 WHERE f.user_id = ?`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list conversation friends: %w", err)
	}
	friends := make(map[string]bool)
	for friendRows.Next() {
		var friend PublicUser
		if err := friendRows.Scan(&friend.Username, &friend.Nickname); err != nil {
			_ = friendRows.Close()
			return nil, fmt.Errorf("scan conversation friend: %w", err)
		}
		friends[friend.Username] = true
		conversations = append(conversations, Conversation{
			User:     friend,
			IsFriend: true,
		})
	}
	if err := friendRows.Err(); err != nil {
		_ = friendRows.Close()
		return nil, fmt.Errorf("iterate conversation friends: %w", err)
	}
	if err := friendRows.Close(); err != nil {
		return nil, fmt.Errorf("close conversation friends: %w", err)
	}

	latestRows, err := s.db.QueryContext(ctx,
		`SELECT m.id, m.sender_id, m.recipient_id, m.client_message_id,
		        m.body, m.created_at, u.username, u.nickname
		 FROM messages m
		 JOIN users u
		   ON u.id = CASE WHEN m.sender_id = ? THEN m.recipient_id ELSE m.sender_id END
		 WHERE m.id IN (
		     SELECT MAX(id)
		     FROM messages
		     WHERE sender_id = ? OR recipient_id = ?
		     GROUP BY CASE WHEN sender_id = ? THEN recipient_id ELSE sender_id END
		 )
		 ORDER BY m.id DESC`,
		userID, userID, userID, userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list latest messages: %w", err)
	}
	defer func() { _ = latestRows.Close() }()

	indexByUsername := make(map[string]int, len(conversations))
	for index := range conversations {
		indexByUsername[conversations[index].User.Username] = index
	}

	for latestRows.Next() {
		var message Message
		var other PublicUser
		if err := latestRows.Scan(
			&message.ID,
			&message.SenderID,
			&message.RecipientID,
			&message.ClientMessageID,
			&message.Body,
			&message.CreatedAt,
			&other.Username,
			&other.Nickname,
		); err != nil {
			return nil, fmt.Errorf("scan latest message: %w", err)
		}
		messageCopy := message
		if index, ok := indexByUsername[other.Username]; ok {
			conversations[index].LastMessage = &messageCopy
			continue
		}
		conversations = append(conversations, Conversation{
			User:        other,
			IsFriend:    false,
			LastMessage: &messageCopy,
		})
		indexByUsername[other.Username] = len(conversations) - 1
	}
	if err := latestRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate latest messages: %w", err)
	}

	unreadRows, err := s.db.QueryContext(ctx,
		`SELECT u.username, COUNT(*)
		 FROM messages m
		 JOIN users u ON u.id = m.sender_id
		 LEFT JOIN conversation_reads r
		   ON r.user_id = ? AND r.peer_user_id = m.sender_id
		 WHERE m.recipient_id = ?
		   AND m.id > COALESCE(r.last_read_message_id, 0)
		 GROUP BY u.username`,
		userID, userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list unread counts: %w", err)
	}
	for unreadRows.Next() {
		var username string
		var unread int64
		if err := unreadRows.Scan(&username, &unread); err != nil {
			_ = unreadRows.Close()
			return nil, fmt.Errorf("scan unread count: %w", err)
		}
		if index, ok := indexByUsername[username]; ok {
			conversations[index].UnreadCount = int(unread)
		}
	}
	if err := unreadRows.Err(); err != nil {
		_ = unreadRows.Close()
		return nil, fmt.Errorf("iterate unread counts: %w", err)
	}
	if err := unreadRows.Close(); err != nil {
		return nil, fmt.Errorf("close unread counts: %w", err)
	}

	peerReadRows, err := s.db.QueryContext(ctx,
		`SELECT u.username, r.last_read_message_id
		 FROM conversation_reads r
		 JOIN users u ON u.id = r.user_id
		 WHERE r.peer_user_id = ?`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list peer read positions: %w", err)
	}
	for peerReadRows.Next() {
		var username string
		var lastReadMessageID uint64
		if err := peerReadRows.Scan(&username, &lastReadMessageID); err != nil {
			_ = peerReadRows.Close()
			return nil, fmt.Errorf("scan peer read position: %w", err)
		}
		if index, ok := indexByUsername[username]; ok {
			conversations[index].PeerLastReadMessageID = lastReadMessageID
		}
	}
	if err := peerReadRows.Err(); err != nil {
		_ = peerReadRows.Close()
		return nil, fmt.Errorf("iterate peer read positions: %w", err)
	}
	if err := peerReadRows.Close(); err != nil {
		return nil, fmt.Errorf("close peer read positions: %w", err)
	}

	sort.SliceStable(conversations, func(i, j int) bool {
		left, right := conversations[i], conversations[j]
		switch {
		case left.LastMessage != nil && right.LastMessage != nil:
			return left.LastMessage.ID > right.LastMessage.ID
		case left.LastMessage != nil:
			return true
		case right.LastMessage != nil:
			return false
		}
		leftName := strings.ToLower(left.User.Nickname)
		rightName := strings.ToLower(right.User.Nickname)
		if leftName != rightName {
			return leftName < rightName
		}
		return left.User.Username < right.User.Username
	})

	return conversations, nil
}

// ListMessages returns a page of messages between two users. A beforeID of
// zero starts from the newest message. Messages are returned oldest first.
func (s *Store) ListMessages(
	ctx context.Context,
	userID, otherID, beforeID uint64,
	limit int,
) (MessagePage, error) {
	query := `SELECT id, sender_id, recipient_id, client_message_id, body, created_at
	          FROM messages
	          WHERE ((sender_id = ? AND recipient_id = ?)
	              OR (sender_id = ? AND recipient_id = ?))`
	args := []any{userID, otherID, otherID, userID}
	if beforeID > 0 {
		query += ` AND id < ?`
		args = append(args, beforeID)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return MessagePage{}, fmt.Errorf("list messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	newestFirst := make([]Message, 0, limit+1)
	for rows.Next() {
		var message Message
		if err := rows.Scan(
			&message.ID,
			&message.SenderID,
			&message.RecipientID,
			&message.ClientMessageID,
			&message.Body,
			&message.CreatedAt,
		); err != nil {
			return MessagePage{}, fmt.Errorf("scan message: %w", err)
		}
		newestFirst = append(newestFirst, message)
	}
	if err := rows.Err(); err != nil {
		return MessagePage{}, fmt.Errorf("iterate messages: %w", err)
	}

	hasMore := len(newestFirst) > limit
	if hasMore {
		newestFirst = newestFirst[:limit]
	}
	messages := make([]Message, len(newestFirst))
	for index := range newestFirst {
		messages[len(newestFirst)-1-index] = newestFirst[index]
	}
	return MessagePage{Messages: messages, HasMore: hasMore}, nil
}

// CreateMessage saves a message when the users are friends. The returned bool
// is false when the same client message was already saved.
func (s *Store) CreateMessage(
	ctx context.Context,
	senderID, recipientID uint64,
	clientMessageID, body string,
	now time.Time,
) (Message, bool, error) {
	if senderID == recipientID {
		return Message{}, false, ErrCannotMessageSelf
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Message{}, false, fmt.Errorf("begin message transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := lockUsers(ctx, tx, senderID, recipientID); err != nil {
		return Message{}, false, err
	}

	existing, err := loadMessageByClientID(ctx, tx, senderID, recipientID, clientMessageID)
	if err == nil {
		if err := tx.Commit(); err != nil {
			return Message{}, false, fmt.Errorf("commit existing message: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Message{}, false, err
	}

	var areFriends int
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM friendships WHERE user_id = ? AND friend_user_id = ?)`,
		senderID, recipientID,
	).Scan(&areFriends); err != nil {
		return Message{}, false, fmt.Errorf("check message friendship: %w", err)
	}
	if areFriends == 0 {
		return Message{}, false, ErrNotFriends
	}

	result, err := tx.ExecContext(ctx,
		`INSERT INTO messages
		   (sender_id, recipient_id, client_message_id, body, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		senderID, recipientID, clientMessageID, body, now,
	)
	if err != nil {
		return Message{}, false, fmt.Errorf("insert message: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Message{}, false, fmt.Errorf("read message id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Message{}, false, fmt.Errorf("commit message: %w", err)
	}
	return Message{
		ID:              uint64(id),
		SenderID:        senderID,
		RecipientID:     recipientID,
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       now,
	}, true, nil
}

func loadMessageByClientID(
	ctx context.Context,
	tx *sql.Tx,
	senderID, recipientID uint64,
	clientMessageID string,
) (Message, error) {
	var message Message
	err := tx.QueryRowContext(ctx,
		`SELECT id, sender_id, recipient_id, client_message_id, body, created_at
		 FROM messages
		 WHERE sender_id = ? AND recipient_id = ? AND client_message_id = ?
		 FOR UPDATE`,
		senderID, recipientID, clientMessageID,
	).Scan(
		&message.ID,
		&message.SenderID,
		&message.RecipientID,
		&message.ClientMessageID,
		&message.Body,
		&message.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Message{}, sql.ErrNoRows
	}
	if err != nil {
		return Message{}, fmt.Errorf("load message by client id: %w", err)
	}
	return message, nil
}

// ListMessagesAfter returns messages newer than afterID, oldest first.
func (s *Store) ListMessagesAfter(
	ctx context.Context,
	userID, otherID, afterID uint64,
	limit int,
) (MessagePage, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, sender_id, recipient_id, client_message_id, body, created_at
		 FROM messages
		 WHERE ((sender_id = ? AND recipient_id = ?)
		     OR (sender_id = ? AND recipient_id = ?))
		   AND id > ?
		 ORDER BY id ASC
		 LIMIT ?`,
		userID, otherID, otherID, userID, afterID, limit+1,
	)
	if err != nil {
		return MessagePage{}, fmt.Errorf("list messages after: %w", err)
	}
	defer func() { _ = rows.Close() }()

	messages := make([]Message, 0, limit+1)
	for rows.Next() {
		var message Message
		if err := rows.Scan(
			&message.ID,
			&message.SenderID,
			&message.RecipientID,
			&message.ClientMessageID,
			&message.Body,
			&message.CreatedAt,
		); err != nil {
			return MessagePage{}, fmt.Errorf("scan message after: %w", err)
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return MessagePage{}, fmt.Errorf("iterate messages after: %w", err)
	}

	hasMore := len(messages) > limit
	if hasMore {
		messages = messages[:limit]
	}
	return MessagePage{Messages: messages, HasMore: hasMore}, nil
}

// MarkConversationRead advances the user's read position to the latest
// incoming message from peer. The returned bool reports whether it advanced.
func (s *Store) MarkConversationRead(
	ctx context.Context,
	userID, peerID uint64,
	now time.Time,
) (uint64, bool, error) {
	if userID == peerID {
		return 0, false, ErrCannotMessageSelf
	}

	var latest uint64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(id), 0)
		 FROM messages
		 WHERE sender_id = ? AND recipient_id = ?`,
		peerID, userID,
	).Scan(&latest); err != nil {
		return 0, false, fmt.Errorf("find latest incoming message: %w", err)
	}
	if latest == 0 {
		return 0, false, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("begin mark read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var existing uint64
	err = tx.QueryRowContext(ctx,
		`SELECT last_read_message_id
		 FROM conversation_reads
		 WHERE user_id = ? AND peer_user_id = ?
		 FOR UPDATE`,
		userID, peerID,
	).Scan(&existing)

	advanced := false
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO conversation_reads
			   (user_id, peer_user_id, last_read_message_id, updated_at)
			 VALUES (?, ?, ?, ?)`,
			userID, peerID, latest, now,
		); err != nil {
			return 0, false, fmt.Errorf("insert conversation read: %w", err)
		}
		advanced = true
	case err != nil:
		return 0, false, fmt.Errorf("load conversation read: %w", err)
	case latest > existing:
		if _, err := tx.ExecContext(ctx,
			`UPDATE conversation_reads
			 SET last_read_message_id = ?, updated_at = ?
			 WHERE user_id = ? AND peer_user_id = ?`,
			latest, now, userID, peerID,
		); err != nil {
			return 0, false, fmt.Errorf("update conversation read: %w", err)
		}
		advanced = true
	}

	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("commit conversation read: %w", err)
	}
	if advanced {
		return latest, true, nil
	}
	return existing, false, nil
}
