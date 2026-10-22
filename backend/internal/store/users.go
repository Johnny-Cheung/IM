package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

var (
	ErrUsernameTaken = errors.New("username taken")
	ErrUserNotFound  = errors.New("user not found")
)

// User is the internal user record. ID is never exposed by the API; Username
// is also the public user identifier.
type User struct {
	ID           uint64
	Username     string
	Nickname     string
	PasswordHash string
}

// CreateUser inserts a user and returns the generated internal ID.
func (s *Store) CreateUser(
	ctx context.Context,
	username, usernameKey, nickname, passwordHash string,
) (User, error) {
	result, err := s.db.ExecContext(ctx,
		`INSERT INTO users (username, username_key, nickname, password_hash)
		 VALUES (?, ?, ?, ?)`,
		username, usernameKey, nickname, passwordHash,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return User{}, ErrUsernameTaken
		}
		return User{}, fmt.Errorf("create user: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return User{}, fmt.Errorf("read created user id: %w", err)
	}
	return User{
		ID:           uint64(id),
		Username:     username,
		Nickname:     nickname,
		PasswordHash: passwordHash,
	}, nil
}

// GetUserByUsernameKey looks up a user by the lowercase unique key.
func (s *Store) GetUserByUsernameKey(ctx context.Context, usernameKey string) (User, error) {
	var user User
	err := s.db.QueryRowContext(ctx,
		`SELECT id, username, nickname, password_hash
		 FROM users WHERE username_key = ?`,
		usernameKey,
	).Scan(&user.ID, &user.Username, &user.Nickname, &user.PasswordHash)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrUserNotFound
	}
	if err != nil {
		return User{}, fmt.Errorf("get user by username: %w", err)
	}
	return user, nil
}

// GetUserByID loads a user by internal ID.
func (s *Store) GetUserByID(ctx context.Context, userID uint64) (User, error) {
	var user User
	err := s.db.QueryRowContext(ctx,
		`SELECT id, username, nickname, password_hash
		 FROM users WHERE id = ?`,
		userID,
	).Scan(&user.ID, &user.Username, &user.Nickname, &user.PasswordHash)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrUserNotFound
	}
	if err != nil {
		return User{}, fmt.Errorf("get user by id: %w", err)
	}
	return user, nil
}

// UpdateNickname changes a user's display nickname.
func (s *Store) UpdateNickname(ctx context.Context, userID uint64, nickname string) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE users SET nickname = ? WHERE id = ?`,
		nickname, userID,
	); err != nil {
		return fmt.Errorf("update nickname: %w", err)
	}
	return nil
}
