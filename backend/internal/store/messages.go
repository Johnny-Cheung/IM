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

const MaxSafeMessageID uint64 = 1<<53 - 1

var (
	ErrNotFriends         = errors.New("not friends")
	ErrCannotMessageSelf  = errors.New("cannot message self")
	ErrMessageConflict    = errors.New("message conflict")
	ErrMessageIDExhausted = errors.New("message ID exhausted")
)

// Message is a persisted one-to-one chat message. ID is the public server
// message ID; the MySQL auto-increment primary key is internal only.
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
	UserID                uint64
	User                  PublicUser
	IsFriend              bool
	LastMessage           *Message
	UnreadCount           int
	PeerLastReadMessageID uint64
}

// ListPublicUsersByIDs returns public identities for the given user IDs.
func (s *Store) ListPublicUsersByIDs(
	ctx context.Context,
	ids []uint64,
) (map[uint64]PublicUser, error) {
	result := make(map[uint64]PublicUser, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for index, id := range ids {
		placeholders[index] = "?"
		args[index] = id
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, username, nickname
		 FROM users
		 WHERE id IN (`+strings.Join(placeholders, ",")+`)`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("list public users: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var id uint64
		var user PublicUser
		if err := rows.Scan(&id, &user.Username, &user.Nickname); err != nil {
			return nil, fmt.Errorf("scan public user: %w", err)
		}
		result[id] = user
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate public users: %w", err)
	}
	return result, nil
}

// ListConversationBasics returns friends plus users with existing messages,
// without per-conversation read state.
func (s *Store) ListConversationBasics(
	ctx context.Context,
	userID uint64,
) ([]Conversation, error) {
	conversations, _, err := s.listConversationBasics(ctx, userID)
	return conversations, err
}

// ListConversations returns all friends plus users with existing messages,
// newest conversations first and friends without messages by nickname.
func (s *Store) ListConversations(ctx context.Context, userID uint64) ([]Conversation, error) {
	conversations, indexByUsername, err := s.listConversationBasics(ctx, userID)
	if err != nil {
		return nil, err
	}

	unreadRows, err := s.db.QueryContext(ctx,
		`SELECT u.username, COUNT(*)
		 FROM messages m
		 JOIN users u ON u.id = m.sender_id
		 LEFT JOIN conversation_reads r
		   ON r.user_id = ? AND r.peer_user_id = m.sender_id
		 WHERE m.recipient_id = ?
		   AND m.server_message_id > COALESCE(r.last_read_message_id, 0)
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

	return conversations, nil
}

func (s *Store) listConversationBasics(
	ctx context.Context,
	userID uint64,
) ([]Conversation, map[string]int, error) {
	conversations := make([]Conversation, 0)

	friendRows, err := s.db.QueryContext(ctx,
		`SELECT u.id, u.username, u.nickname
		 FROM friendships f
		 JOIN users u ON u.id = f.friend_user_id
		 WHERE f.user_id = ?`,
		userID,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("list conversation friends: %w", err)
	}
	for friendRows.Next() {
		var conversation Conversation
		if err := friendRows.Scan(
			&conversation.UserID,
			&conversation.User.Username,
			&conversation.User.Nickname,
		); err != nil {
			_ = friendRows.Close()
			return nil, nil, fmt.Errorf("scan conversation friend: %w", err)
		}
		conversation.IsFriend = true
		conversations = append(conversations, conversation)
	}
	if err := friendRows.Err(); err != nil {
		_ = friendRows.Close()
		return nil, nil, fmt.Errorf("iterate conversation friends: %w", err)
	}
	if err := friendRows.Close(); err != nil {
		return nil, nil, fmt.Errorf("close conversation friends: %w", err)
	}

	latestRows, err := s.db.QueryContext(ctx,
		`SELECT m.server_message_id, m.sender_id, m.recipient_id, m.client_message_id,
		        m.body, m.created_at, u.username, u.nickname, u.id
		 FROM messages m
		 JOIN users u
		   ON u.id = CASE WHEN m.sender_id = ? THEN m.recipient_id ELSE m.sender_id END
		 WHERE m.server_message_id IN (
		     SELECT MAX(server_message_id)
		     FROM messages
		     WHERE sender_id = ? OR recipient_id = ?
		     GROUP BY CASE WHEN sender_id = ? THEN recipient_id ELSE sender_id END
		 )
		 ORDER BY m.server_message_id DESC`,
		userID, userID, userID, userID,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("list latest messages: %w", err)
	}

	indexByUsername := make(map[string]int, len(conversations))
	for index := range conversations {
		indexByUsername[conversations[index].User.Username] = index
	}

	for latestRows.Next() {
		var message Message
		var other PublicUser
		var otherID uint64
		if err := latestRows.Scan(
			&message.ID,
			&message.SenderID,
			&message.RecipientID,
			&message.ClientMessageID,
			&message.Body,
			&message.CreatedAt,
			&other.Username,
			&other.Nickname,
			&otherID,
		); err != nil {
			return nil, nil, fmt.Errorf("scan latest message: %w", err)
		}
		messageCopy := message
		if index, ok := indexByUsername[other.Username]; ok {
			conversations[index].UserID = otherID
			conversations[index].LastMessage = &messageCopy
			continue
		}
		conversations = append(conversations, Conversation{
			UserID:      otherID,
			User:        other,
			IsFriend:    false,
			LastMessage: &messageCopy,
		})
		indexByUsername[other.Username] = len(conversations) - 1
	}
	if err := latestRows.Err(); err != nil {
		_ = latestRows.Close()
		return nil, nil, fmt.Errorf("iterate latest messages: %w", err)
	}
	if err := latestRows.Close(); err != nil {
		return nil, nil, fmt.Errorf("close latest messages: %w", err)
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

	return conversations, indexByUsername, nil
}

// ListMessages returns a page of messages between two users. A beforeID of
// zero starts from the newest message. Messages are returned oldest first.
func (s *Store) ListMessages(
	ctx context.Context,
	userID, otherID, beforeID uint64,
	limit int,
) (MessagePage, error) {
	query := `SELECT server_message_id, sender_id, recipient_id, client_message_id, body, created_at
	          FROM messages
	          WHERE ((sender_id = ? AND recipient_id = ?)
	              OR (sender_id = ? AND recipient_id = ?))`
	args := []any{userID, otherID, otherID, userID}
	if beforeID > 0 {
		query += ` AND server_message_id < ?`
		args = append(args, beforeID)
	}
	query += ` ORDER BY server_message_id DESC LIMIT ?`
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
		if existing.Body != body {
			return Message{}, false, ErrMessageConflict
		}
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

	serverMessageID, err := nextMessageID(ctx, tx)
	if err != nil {
		return Message{}, false, err
	}
	if serverMessageID > MaxSafeMessageID {
		return Message{}, false, ErrMessageIDExhausted
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO messages
		   (server_message_id, sender_id, recipient_id, client_message_id, body, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		serverMessageID, senderID, recipientID, clientMessageID, body, now,
	); err != nil {
		return Message{}, false, fmt.Errorf("insert message: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Message{}, false, fmt.Errorf("commit message: %w", err)
	}
	return Message{
		ID:              serverMessageID,
		SenderID:        senderID,
		RecipientID:     recipientID,
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       now,
	}, true, nil
}

func nextMessageID(ctx context.Context, tx *sql.Tx) (uint64, error) {
	var sequenceID, directMax, groupMax uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT last_message_id
		 FROM message_id_sequences
		 WHERE scope = 'direct'
		 FOR UPDATE`,
	).Scan(&sequenceID); err != nil {
		return 0, fmt.Errorf("read message sequence: %w", err)
	}
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(server_message_id), 0) FROM messages`,
	).Scan(&directMax); err != nil {
		return 0, fmt.Errorf("read direct message watermark: %w", err)
	}
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(server_message_id), 0) FROM group_messages`,
	).Scan(&groupMax); err != nil {
		return 0, fmt.Errorf("read group message watermark: %w", err)
	}

	id := sequenceID
	if directMax > id {
		id = directMax
	}
	if groupMax > id {
		id = groupMax
	}
	if id >= MaxSafeMessageID {
		return 0, ErrMessageIDExhausted
	}
	id++
	if _, err := tx.ExecContext(ctx,
		`UPDATE message_id_sequences
		 SET last_message_id = ?
		 WHERE scope = 'direct'`,
		id,
	); err != nil {
		return 0, fmt.Errorf("advance message sequence: %w", err)
	}
	return id, nil
}

// MaxServerMessageID returns the highest public message ID currently stored.
func (s *Store) MaxServerMessageID(ctx context.Context) (uint64, error) {
	var id uint64
	if err := s.db.QueryRowContext(ctx,
		`SELECT GREATEST(
			COALESCE((SELECT MAX(server_message_id) FROM messages), 0),
			COALESCE((SELECT MAX(server_message_id) FROM group_messages), 0),
			COALESCE((SELECT last_message_id FROM message_id_sequences WHERE scope = 'direct'), 0)
		)`,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf("max server message id: %w", err)
	}
	return id, nil
}

// PersistMessage inserts an asynchronously accepted message idempotently.
// It does not re-check friendship because that decision was made at acceptance.
func (s *Store) PersistMessage(ctx context.Context, message Message) (bool, error) {
	if message.ID == 0 || message.ID > MaxSafeMessageID {
		return false, ErrMessageIDExhausted
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin persist message transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx,
		`INSERT INTO messages
		   (server_message_id, sender_id, recipient_id, client_message_id, body, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE id = id`,
		message.ID,
		message.SenderID,
		message.RecipientID,
		message.ClientMessageID,
		message.Body,
		message.CreatedAt,
	)
	if err != nil {
		return false, fmt.Errorf("persist message: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read persist message result: %w", err)
	}
	if rows == 1 {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit persisted message: %w", err)
		}
		return true, nil
	}
	if rows != 0 {
		return false, fmt.Errorf("unexpected persist message rows affected: %d", rows)
	}

	existing, err := loadMessageByServerID(ctx, tx, message.ID)
	if errors.Is(err, sql.ErrNoRows) {
		existing, err = loadMessageByClientID(
			ctx,
			tx,
			message.SenderID,
			message.RecipientID,
			message.ClientMessageID,
		)
	}
	if err != nil {
		return false, fmt.Errorf("load duplicate persisted message: %w", err)
	}
	if !sameMessage(existing, message) {
		return false, ErrMessageConflict
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit duplicate persisted message: %w", err)
	}
	return false, nil
}

func sameMessage(left, right Message) bool {
	return left.ID == right.ID &&
		left.SenderID == right.SenderID &&
		left.RecipientID == right.RecipientID &&
		left.ClientMessageID == right.ClientMessageID &&
		left.Body == right.Body &&
		left.CreatedAt.Truncate(time.Millisecond).Equal(right.CreatedAt.Truncate(time.Millisecond))
}

func loadMessageByServerID(ctx context.Context, tx *sql.Tx, serverMessageID uint64) (Message, error) {
	var message Message
	err := tx.QueryRowContext(ctx,
		`SELECT server_message_id, sender_id, recipient_id, client_message_id, body, created_at
		 FROM messages
		 WHERE server_message_id = ?
		 FOR UPDATE`,
		serverMessageID,
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
		return Message{}, fmt.Errorf("load message by server id: %w", err)
	}
	return message, nil
}

func loadMessageByClientID(
	ctx context.Context,
	tx *sql.Tx,
	senderID, recipientID uint64,
	clientMessageID string,
) (Message, error) {
	var message Message
	err := tx.QueryRowContext(ctx,
		`SELECT server_message_id, sender_id, recipient_id, client_message_id, body, created_at
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
		`SELECT server_message_id, sender_id, recipient_id, client_message_id, body, created_at
		 FROM messages
		 WHERE ((sender_id = ? AND recipient_id = ?)
		     OR (sender_id = ? AND recipient_id = ?))
		   AND server_message_id > ?
		 ORDER BY server_message_id ASC
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
// incoming message from peer, capped at throughID when nonzero. The returned
// bool reports whether it advanced.
func (s *Store) MarkConversationRead(
	ctx context.Context,
	userID, peerID, throughID uint64,
	now time.Time,
) (uint64, bool, error) {
	if userID == peerID {
		return 0, false, ErrCannotMessageSelf
	}

	var latest uint64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(server_message_id), 0)
		 FROM messages
		 WHERE sender_id = ? AND recipient_id = ?
		   AND (? = 0 OR server_message_id <= ?)`,
		peerID, userID, throughID, throughID,
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
