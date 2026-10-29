package store

import (
	"context"
	"database/sql"
	"fmt"
)

// scanRow is the Scan surface shared by *sql.Rows and *sql.Row.
type scanRow interface {
	Scan(dest ...any) error
}

// ListMessagesForRebuild returns one page of direct messages ordered by the
// public message ID, so the caller can page with the last returned ID. Rows
// without a seq are returned with Seq zero and must be rejected by the caller.
func (s *Store) ListMessagesForRebuild(
	ctx context.Context,
	afterID uint64,
	limit int,
) ([]Message, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT server_message_id, seq, sender_id, recipient_id,
		        client_message_id, body, created_at
		 FROM messages
		 WHERE server_message_id > ?
		 ORDER BY server_message_id ASC
		 LIMIT ?`,
		afterID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list messages for rebuild: %w", err)
	}
	defer func() { _ = rows.Close() }()

	messages := make([]Message, 0, limit)
	for rows.Next() {
		message, err := scanDirectMessage(rows)
		if err != nil {
			return nil, fmt.Errorf("scan message for rebuild: %w", err)
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages for rebuild: %w", err)
	}
	return messages, nil
}

// ListGroupMessagesForRebuild returns one page of group messages ordered by the
// public message ID. A missing sender user yields an empty sender snapshot
// instead of dropping the row.
func (s *Store) ListGroupMessagesForRebuild(
	ctx context.Context,
	afterID uint64,
	limit int,
) ([]GroupMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT gm.server_message_id, gm.seq, gm.group_id, gm.sender_id,
		        sender.username, sender.nickname,
		        gm.client_message_id, gm.body, gm.created_at
		 FROM group_messages gm
		 LEFT JOIN users sender ON sender.id = gm.sender_id
		 WHERE gm.server_message_id > ?
		 ORDER BY gm.server_message_id ASC
		 LIMIT ?`,
		afterID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list group messages for rebuild: %w", err)
	}
	defer func() { _ = rows.Close() }()

	messages := make([]GroupMessage, 0, limit)
	for rows.Next() {
		message, err := scanGroupMessage(rows)
		if err != nil {
			return nil, fmt.Errorf("scan group message for rebuild: %w", err)
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group messages for rebuild: %w", err)
	}
	return messages, nil
}

// ListMessagesBySeqRange returns one ascending page of numbered direct messages
// of a conversation after afterSeq, used to fill Redis gaps.
func (s *Store) ListMessagesBySeqRange(
	ctx context.Context,
	lowID, highID, afterSeq uint64,
	limit int,
) ([]Message, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT server_message_id, seq, sender_id, recipient_id,
		        client_message_id, body, created_at
		 FROM messages
		 WHERE conversation_low_id = ? AND conversation_high_id = ?
		   AND seq > ? AND seq IS NOT NULL
		 ORDER BY seq ASC
		 LIMIT ?`,
		lowID, highID, afterSeq, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list messages by seq range: %w", err)
	}
	defer func() { _ = rows.Close() }()

	messages := make([]Message, 0, limit)
	for rows.Next() {
		message, err := scanDirectMessage(rows)
		if err != nil {
			return nil, fmt.Errorf("scan message by seq range: %w", err)
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages by seq range: %w", err)
	}
	return messages, nil
}

// ListGroupMessagesBySeqRange returns one ascending page of numbered group
// messages after afterSeq, used to fill Redis gaps.
func (s *Store) ListGroupMessagesBySeqRange(
	ctx context.Context,
	groupID, afterSeq uint64,
	limit int,
) ([]GroupMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT gm.server_message_id, gm.seq, gm.group_id, gm.sender_id,
		        sender.username, sender.nickname,
		        gm.client_message_id, gm.body, gm.created_at
		 FROM group_messages gm
		 LEFT JOIN users sender ON sender.id = gm.sender_id
		 WHERE gm.group_id = ? AND gm.seq > ? AND gm.seq IS NOT NULL
		 ORDER BY gm.seq ASC
		 LIMIT ?`,
		groupID, afterSeq, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list group messages by seq range: %w", err)
	}
	defer func() { _ = rows.Close() }()

	messages := make([]GroupMessage, 0, limit)
	for rows.Next() {
		message, err := scanGroupMessage(rows)
		if err != nil {
			return nil, fmt.Errorf("scan group message by seq range: %w", err)
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group messages by seq range: %w", err)
	}
	return messages, nil
}

// CountMessagesInvolvingUser counts direct messages the user sent or received.
func (s *Store) CountMessagesInvolvingUser(ctx context.Context, userID uint64) (uint64, error) {
	var count uint64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE sender_id = ? OR recipient_id = ?`,
		userID, userID,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count messages involving user: %w", err)
	}
	return count, nil
}

// ListUserIDs returns one ascending page of user IDs used to rotate audits.
func (s *Store) ListUserIDs(ctx context.Context, afterID uint64, limit int) ([]uint64, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM users WHERE id > ? ORDER BY id ASC LIMIT ?`,
		afterID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list user IDs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	ids := make([]uint64, 0, limit)
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan user ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate user IDs: %w", err)
	}
	return ids, nil
}

func scanDirectMessage(row scanRow) (Message, error) {
	var message Message
	var seq sql.NullInt64
	if err := row.Scan(
		&message.ID,
		&seq,
		&message.SenderID,
		&message.RecipientID,
		&message.ClientMessageID,
		&message.Body,
		&message.CreatedAt,
	); err != nil {
		return Message{}, err
	}
	message.Seq = nullSeqAsZero(seq)
	return message, nil
}

func scanGroupMessage(row scanRow) (GroupMessage, error) {
	var message GroupMessage
	var seq sql.NullInt64
	var username, nickname sql.NullString
	if err := row.Scan(
		&message.ID,
		&seq,
		&message.GroupID,
		&message.SenderID,
		&username,
		&nickname,
		&message.ClientMessageID,
		&message.Body,
		&message.CreatedAt,
	); err != nil {
		return GroupMessage{}, err
	}
	message.Seq = nullSeqAsZero(seq)
	if username.Valid {
		message.Sender.Username = username.String
	}
	if nickname.Valid {
		message.Sender.Nickname = nickname.String
	}
	return message, nil
}

// nullSeqAsZero maps rows that predate numbering to seq zero; rebuild callers
// treat zero as "refuse to run".
func nullSeqAsZero(seq sql.NullInt64) uint64 {
	if !seq.Valid || seq.Int64 <= 0 {
		return 0
	}
	return uint64(seq.Int64)
}
