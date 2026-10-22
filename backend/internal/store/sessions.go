package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	RevokeReasonReplaced        = "replaced"
	RevokeReasonLogout          = "logout"
	RevokeReasonPasswordChanged = "password_changed"
)

var ErrSessionNotFound = errors.New("session not found")

// SessionUser combines a session with the user it belongs to.
type SessionUser struct {
	SessionID    uint64
	UserID       uint64
	ExpiresAt    time.Time
	RevokedAt    *time.Time
	RevokeReason string
	User         User
}

// CreateSession revokes the user's current active session and inserts a new
// one in a single transaction. Locking the user row serializes concurrent logins.
func (s *Store) CreateSession(
	ctx context.Context,
	userID uint64,
	tokenHash []byte,
	createdAt, expiresAt time.Time,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin session transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var lockedID uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT id FROM users WHERE id = ? FOR UPDATE`, userID,
	).Scan(&lockedID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrUserNotFound
		}
		return fmt.Errorf("lock user for session: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE sessions
		 SET revoked_at = ?, revoke_reason = ?
		 WHERE user_id = ? AND revoked_at IS NULL`,
		createdAt, RevokeReasonReplaced, userID,
	); err != nil {
		return fmt.Errorf("replace active session: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO sessions (user_id, token_hash, created_at, expires_at)
		 VALUES (?, ?, ?, ?)`,
		userID, tokenHash, createdAt, expiresAt,
	); err != nil {
		return fmt.Errorf("insert session: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit session: %w", err)
	}
	return nil
}

// GetSessionWithUser loads a session by token hash and joins its user.
func (s *Store) GetSessionWithUser(ctx context.Context, tokenHash []byte) (SessionUser, error) {
	var (
		session   SessionUser
		revokedAt sql.NullTime
		reason    sql.NullString
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT s.id, s.user_id, s.expires_at, s.revoked_at, s.revoke_reason,
		        u.id, u.username, u.nickname
		 FROM sessions s
		 JOIN users u ON u.id = s.user_id
		 WHERE s.token_hash = ?`,
		tokenHash,
	).Scan(
		&session.SessionID,
		&session.UserID,
		&session.ExpiresAt,
		&revokedAt,
		&reason,
		&session.User.ID,
		&session.User.Username,
		&session.User.Nickname,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionUser{}, ErrSessionNotFound
	}
	if err != nil {
		return SessionUser{}, fmt.Errorf("get session: %w", err)
	}

	if revokedAt.Valid {
		value := revokedAt.Time
		session.RevokedAt = &value
	}
	session.RevokeReason = reason.String
	return session, nil
}

// RevokeSession revokes a single session by token hash. Revoking an already
// revoked or unknown token is a no-op.
func (s *Store) RevokeSession(
	ctx context.Context,
	tokenHash []byte,
	reason string,
	revokedAt time.Time,
) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE sessions
		 SET revoked_at = ?, revoke_reason = ?
		 WHERE token_hash = ? AND revoked_at IS NULL`,
		revokedAt, reason, tokenHash,
	); err != nil {
		return fmt.Errorf("revoke session: %w", err)
	}
	return nil
}
