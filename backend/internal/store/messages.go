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

// MaxSafeSeq bounds the per-conversation sequence because Redis sorted-set
// scores are float64.
const MaxSafeSeq uint64 = 1<<53 - 1

var (
	ErrNotFriends         = errors.New("not friends")
	ErrCannotMessageSelf  = errors.New("cannot message self")
	ErrMessageConflict    = errors.New("message conflict")
	ErrMessageIDExhausted = errors.New("message ID exhausted")
	ErrSeqConflict        = errors.New("message seq conflict")
	ErrSeqExhausted       = errors.New("message seq exhausted")
	ErrSeqInvalid         = errors.New("message seq invalid")
)

// Message is a persisted one-to-one chat message. ID is the public server
// message ID; the MySQL auto-increment primary key is internal only. Seq is the
// per-conversation sequence and is zero for messages that predate numbering.
type Message struct {
	ID              uint64
	Seq             uint64
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
// LastSeq is the conversation counter from MySQL; PeerLastReadSeq is the other
// user's read watermark in seq units.
type Conversation struct {
	UserID          uint64
	User            PublicUser
	IsFriend        bool
	LastMessage     *Message
	UnreadCount     int
	LastSeq         uint64
	PeerLastReadSeq uint64
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

// HasDirectConversation reports whether the two users have exchanged a
// message. The pair may no longer be friends.
func (s *Store) HasDirectConversation(
	ctx context.Context,
	userID, peerID uint64,
) (bool, error) {
	var found int
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(
		   SELECT 1 FROM messages
		   WHERE (sender_id = ? AND recipient_id = ?)
		      OR (sender_id = ? AND recipient_id = ?)
		 )`,
		userID, peerID, peerID, userID,
	).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("check direct conversation: %w", err)
	}
	return found == 1, nil
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
		   AND COALESCE(m.seq, 0) > COALESCE(r.last_read_seq, 0)
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
		`SELECT u.username, r.last_read_seq
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
		var lastReadSeq uint64
		if err := peerReadRows.Scan(&username, &lastReadSeq); err != nil {
			_ = peerReadRows.Close()
			return nil, fmt.Errorf("scan peer read position: %w", err)
		}
		if index, ok := indexByUsername[username]; ok {
			conversations[index].PeerLastReadSeq = lastReadSeq
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
		`SELECT m.server_message_id, COALESCE(m.seq, 0), m.sender_id, m.recipient_id,
		        m.client_message_id, m.body, m.created_at, u.username, u.nickname, u.id,
		        (SELECT COALESCE(MAX(stats.seq), 0)
		         FROM messages stats
		         WHERE stats.conversation_low_id = LEAST(m.sender_id, m.recipient_id)
		           AND stats.conversation_high_id = GREATEST(m.sender_id, m.recipient_id))
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
		var otherID, lastSeq uint64
		if err := latestRows.Scan(
			&message.ID,
			&message.Seq,
			&message.SenderID,
			&message.RecipientID,
			&message.ClientMessageID,
			&message.Body,
			&message.CreatedAt,
			&other.Username,
			&other.Nickname,
			&otherID,
			&lastSeq,
		); err != nil {
			return nil, nil, fmt.Errorf("scan latest message: %w", err)
		}
		messageCopy := message
		if index, ok := indexByUsername[other.Username]; ok {
			conversations[index].UserID = otherID
			conversations[index].LastMessage = &messageCopy
			conversations[index].LastSeq = lastSeq
			continue
		}
		conversations = append(conversations, Conversation{
			UserID:      otherID,
			User:        other,
			IsFriend:    false,
			LastMessage: &messageCopy,
			LastSeq:     lastSeq,
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

// ListMessages returns a page of messages between two users ordered by the
// conversation seq. A beforeSeq of zero starts from the newest message.
// Messages are returned oldest first.
func (s *Store) ListMessages(
	ctx context.Context,
	userID, otherID, beforeSeq uint64,
	limit int,
) (MessagePage, error) {
	lowID, highID := directConversationPair(userID, otherID)
	query := `SELECT server_message_id, COALESCE(seq, 0), sender_id, recipient_id,
	                 client_message_id, body, created_at
	          FROM messages
	          WHERE conversation_low_id = ? AND conversation_high_id = ?`
	args := []any{lowID, highID}
	if beforeSeq > 0 {
		query += ` AND seq < ?`
		args = append(args, beforeSeq)
	}
	query += ` ORDER BY seq DESC LIMIT ?`
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
			&message.Seq,
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

	lowID, highID := directConversationPair(senderID, recipientID)
	// lockUsers holds both user rows for the rest of the transaction, which
	// serializes seq allocation for this pair.
	seq, err := nextDirectSeq(ctx, tx, lowID, highID)
	if err != nil {
		return Message{}, false, err
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO messages
		   (server_message_id, conversation_low_id, conversation_high_id, seq,
		    sender_id, recipient_id, client_message_id, body, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		serverMessageID, lowID, highID, seq, senderID, recipientID, clientMessageID, body, now,
	); err != nil {
		return Message{}, false, fmt.Errorf("insert message: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Message{}, false, fmt.Errorf("commit message: %w", err)
	}
	return Message{
		ID:              serverMessageID,
		Seq:             seq,
		SenderID:        senderID,
		RecipientID:     recipientID,
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       now,
	}, true, nil
}

// directConversationPair orders the two participants so that a conversation is
// identified by the same pair regardless of direction.
func directConversationPair(leftID, rightID uint64) (lowID, highID uint64) {
	if leftID <= rightID {
		return leftID, rightID
	}
	return rightID, leftID
}

// nextSeqAfter returns the next seq or a sentinel error once the safe range is
// exhausted. Callers allocate inside a transaction that serializes the
// conversation.
func nextSeqAfter(current uint64) (uint64, error) {
	if current >= MaxSafeSeq {
		return 0, ErrSeqExhausted
	}
	return current + 1, nil
}

func nextDirectSeq(ctx context.Context, tx *sql.Tx, lowID, highID uint64) (uint64, error) {
	var maxSeq uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM messages
		 WHERE conversation_low_id = ? AND conversation_high_id = ?`,
		lowID, highID,
	).Scan(&maxSeq); err != nil {
		return 0, fmt.Errorf("read direct conversation seq: %w", err)
	}
	return nextSeqAfter(maxSeq)
}

func nextGroupSeq(ctx context.Context, tx *sql.Tx, groupID uint64) (uint64, error) {
	var maxSeq uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM group_messages WHERE group_id = ?`,
		groupID,
	).Scan(&maxSeq); err != nil {
		return 0, fmt.Errorf("read group seq: %w", err)
	}
	return nextSeqAfter(maxSeq)
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
// A seq that already belongs to a different message returns ErrSeqConflict so
// the consumer can acknowledge instead of retrying forever.
func (s *Store) PersistMessage(ctx context.Context, message Message) (bool, error) {
	if message.ID == 0 || message.ID > MaxSafeMessageID {
		return false, ErrMessageIDExhausted
	}
	if message.Seq == 0 || message.Seq > MaxSafeSeq {
		return false, ErrSeqInvalid
	}
	lowID, highID := directConversationPair(message.SenderID, message.RecipientID)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin persist message transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx,
		`INSERT INTO messages
		   (server_message_id, conversation_low_id, conversation_high_id, seq,
		    sender_id, recipient_id, client_message_id, body, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE id = id`,
		message.ID,
		lowID,
		highID,
		message.Seq,
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
	if errors.Is(err, sql.ErrNoRows) {
		conflicting, conflictErr := loadMessageByConversationSeq(ctx, tx, lowID, highID, message.Seq)
		switch {
		case conflictErr == nil:
			return false, fmt.Errorf(
				"%w: conversation (%d,%d) seq %d belongs to message %d",
				ErrSeqConflict, lowID, highID, message.Seq, conflicting.ID,
			)
		case !errors.Is(conflictErr, sql.ErrNoRows):
			return false, conflictErr
		default:
			return false, ErrMessageConflict
		}
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
		left.Seq == right.Seq &&
		left.SenderID == right.SenderID &&
		left.RecipientID == right.RecipientID &&
		left.ClientMessageID == right.ClientMessageID &&
		left.Body == right.Body &&
		left.CreatedAt.Truncate(time.Millisecond).Equal(right.CreatedAt.Truncate(time.Millisecond))
}

func loadMessageByServerID(ctx context.Context, tx *sql.Tx, serverMessageID uint64) (Message, error) {
	var message Message
	err := tx.QueryRowContext(ctx,
		`SELECT server_message_id, COALESCE(seq, 0), sender_id, recipient_id,
		        client_message_id, body, created_at
		 FROM messages
		 WHERE server_message_id = ?
		 FOR UPDATE`,
		serverMessageID,
	).Scan(
		&message.ID,
		&message.Seq,
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
		`SELECT server_message_id, COALESCE(seq, 0), sender_id, recipient_id,
		        client_message_id, body, created_at
		 FROM messages
		 WHERE sender_id = ? AND recipient_id = ? AND client_message_id = ?
		 FOR UPDATE`,
		senderID, recipientID, clientMessageID,
	).Scan(
		&message.ID,
		&message.Seq,
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

func loadMessageByConversationSeq(
	ctx context.Context,
	tx *sql.Tx,
	lowID, highID, seq uint64,
) (Message, error) {
	var message Message
	err := tx.QueryRowContext(ctx,
		`SELECT server_message_id, COALESCE(seq, 0), sender_id, recipient_id,
		        client_message_id, body, created_at
		 FROM messages
		 WHERE conversation_low_id = ? AND conversation_high_id = ? AND seq = ?
		 FOR UPDATE`,
		lowID, highID, seq,
	).Scan(
		&message.ID,
		&message.Seq,
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
		return Message{}, fmt.Errorf("load message by conversation seq: %w", err)
	}
	return message, nil
}

// ListMessagesAfter returns messages newer than afterSeq in conversation seq
// order, oldest first.
func (s *Store) ListMessagesAfter(
	ctx context.Context,
	userID, otherID, afterSeq uint64,
	limit int,
) (MessagePage, error) {
	lowID, highID := directConversationPair(userID, otherID)
	rows, err := s.db.QueryContext(ctx,
		`SELECT server_message_id, COALESCE(seq, 0), sender_id, recipient_id,
		        client_message_id, body, created_at
		 FROM messages
		 WHERE conversation_low_id = ? AND conversation_high_id = ?
		   AND seq > ?
		 ORDER BY seq ASC
		 LIMIT ?`,
		lowID, highID, afterSeq, limit+1,
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
			&message.Seq,
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

// UpsertConversationRead advances a conversation read position monotonically.
// It is used by the asynchronous read-position persister and is idempotent
// under duplicate or out-of-order events. The retained legacy column is
// written as zero.
func (s *Store) UpsertConversationRead(
	ctx context.Context,
	userID, peerID, lastReadSeq uint64,
	updatedAt time.Time,
) error {
	if userID == peerID {
		return ErrCannotMessageSelf
	}
	if lastReadSeq == 0 {
		return nil
	}
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO conversation_reads
		   (user_id, peer_user_id, last_read_message_id, last_read_seq, updated_at)
		 VALUES (?, ?, 0, ?, ?) AS incoming
		 ON DUPLICATE KEY UPDATE
		   last_read_seq = GREATEST(
		     conversation_reads.last_read_seq,
		     incoming.last_read_seq
		   ),
		   updated_at = GREATEST(conversation_reads.updated_at, incoming.updated_at)`,
		userID, peerID, lastReadSeq, updatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert conversation read: %w", err)
	}
	return nil
}
