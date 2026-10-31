package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	RevokeReasonReplaced        = "replaced"
	RevokeReasonLogout          = "logout"
	RevokeReasonPasswordChanged = "password_changed"
)

var ErrSessionNotFound = errors.New("session not found")

const (
	// sessionDeadlockRetries bounds how many times an insert retries after
	// InnoDB rolls the transaction back with error 1213.
	sessionDeadlockRetries = 3
	// sessionRetryBackoff is the first retry delay; each further attempt doubles it.
	sessionRetryBackoff = 5 * time.Millisecond
	// mysqlErrDeadlock is ER_LOCK_DEADLOCK.
	mysqlErrDeadlock = 1213
)

// isMySQLDeadlock reports whether err is an InnoDB deadlock rollback. Those are
// expected under concurrency and worth retrying; every other error is not.
func isMySQLDeadlock(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDeadlock
}

// sleepContext waits for delay unless the context ends first.
func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

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
//
// Deadlocks here are expected under concurrent *registration* and are only
// partially mitigated, not fixed. The mechanism, confirmed by reproduction
// against MySQL 8.4 (REPEATABLE READ):
//
// The revoke UPDATE matches no row for a brand-new user, and under REPEATABLE
// READ a non-matching equality search still takes a gap lock — specifically on
// the supremum pseudo-record at the end of idx_sessions_user_active. Since a
// new user's id is always the current maximum, every concurrent registration
// lands in that same gap: two transactions each hold the supremum gap lock
// (gap locks are mutually compatible) and then each wait for the other's gap
// lock before its INSERT can take an insert intention lock. InnoDB breaks the
// cycle by rolling one back with error 1213.
//
// Because CreateUser commits before this call, a rolled-back transaction
// surfaces to the caller as a failure even though the user row already exists.
//
// The retry below only narrows the window: with no jitter, goroutines that
// collided once tend to wake together and collide again (observed: all 3
// retries deadlocked). The root fix is to run this transaction under READ
// COMMITTED, where the UPDATE takes no gap lock at all — same-user
// serialization is still guaranteed by the SELECT ... FOR UPDATE above.
// That change is deliberately not applied yet; see bench/README.md.
func (s *Store) CreateSession(
	ctx context.Context,
	userID uint64,
	tokenHash []byte,
	createdAt, expiresAt time.Time,
) error {
	var lastErr error
	for attempt := 0; attempt <= sessionDeadlockRetries; attempt++ {
		if attempt > 0 {
			// Back off so the retry does not collide with the transaction
			// that just won the lock. Without jitter this is weak: peers that
			// collided together wake together and can collide again. Callers
			// that need stronger guarantees add their own jittered retry
			// (see bench/cmd/register).
			if err := sleepContext(ctx, sessionRetryBackoff<<(attempt-1)); err != nil {
				return err
			}
		}
		lastErr = s.createSessionOnce(ctx, userID, tokenHash, createdAt, expiresAt)
		if !isMySQLDeadlock(lastErr) {
			return lastErr
		}
	}
	return fmt.Errorf("create session after %d retries: %w", sessionDeadlockRetries, lastErr)
}

// createSessionOnce runs one attempt of the session transaction.
func (s *Store) createSessionOnce(
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

// UpdatePasswordAndRevokeSessions updates the password hash and revokes every
// active session for the user in one transaction.
func (s *Store) UpdatePasswordAndRevokeSessions(
	ctx context.Context,
	userID uint64,
	passwordHash string,
	revokedAt time.Time,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin password transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var lockedID uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT id FROM users WHERE id = ? FOR UPDATE`, userID,
	).Scan(&lockedID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrUserNotFound
		}
		return fmt.Errorf("lock user for password change: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE users SET password_hash = ? WHERE id = ?`,
		passwordHash, userID,
	); err != nil {
		return fmt.Errorf("update password: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE sessions
		 SET revoked_at = ?, revoke_reason = ?
		 WHERE user_id = ? AND revoked_at IS NULL`,
		revokedAt, RevokeReasonPasswordChanged, userID,
	); err != nil {
		return fmt.Errorf("revoke sessions after password change: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit password change: %w", err)
	}
	return nil
}
