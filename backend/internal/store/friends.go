package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var (
	ErrCannotAddSelf         = errors.New("cannot add self")
	ErrAlreadyFriends        = errors.New("already friends")
	ErrFriendRequestExists   = errors.New("friend request exists")
	ErrFriendRequestNotFound = errors.New("friend request not found")
	ErrFriendNotFound        = errors.New("friend not found")
)

// Relationship describes how the current user relates to a target user.
type Relationship string

const (
	RelationshipSelf            Relationship = "self"
	RelationshipFriend          Relationship = "friend"
	RelationshipIncomingPending Relationship = "incoming_pending"
	RelationshipOutgoingPending Relationship = "outgoing_pending"
	RelationshipNone            Relationship = "none"
)

// PublicUser is the user information safe to expose to other users.
type PublicUser struct {
	Username string
	Nickname string
}

// FriendRequest is a pending request with the other party's public identity.
type FriendRequest struct {
	ID        uint64
	User      PublicUser
	CreatedAt time.Time
}

// FriendRequests contains requests received and sent by a user.
type FriendRequests struct {
	Incoming []FriendRequest
	Outgoing []FriendRequest
}

// FriendRequestResult is returned after sending a request. Accepted is true
// when a reverse request already existed and the users became friends.
type FriendRequestResult struct {
	Accepted  bool
	RequestID uint64
	Friend    PublicUser
}

// GetRelationship returns the relationship between two users.
func (s *Store) GetRelationship(ctx context.Context, viewerID, targetID uint64) (Relationship, error) {
	if viewerID == targetID {
		return RelationshipSelf, nil
	}

	var isFriend, outgoing, incoming int
	err := s.db.QueryRowContext(ctx,
		`SELECT
		   EXISTS(SELECT 1 FROM friendships WHERE user_id = ? AND friend_user_id = ?),
		   EXISTS(SELECT 1 FROM friend_requests WHERE from_user_id = ? AND to_user_id = ?),
		   EXISTS(SELECT 1 FROM friend_requests WHERE from_user_id = ? AND to_user_id = ?)`,
		viewerID, targetID,
		viewerID, targetID,
		targetID, viewerID,
	).Scan(&isFriend, &outgoing, &incoming)
	if err != nil {
		return RelationshipNone, fmt.Errorf("get relationship: %w", err)
	}

	switch {
	case isFriend == 1:
		return RelationshipFriend, nil
	case incoming == 1:
		return RelationshipIncomingPending, nil
	case outgoing == 1:
		return RelationshipOutgoingPending, nil
	default:
		return RelationshipNone, nil
	}
}

// ListFriends returns the user's friends ordered by nickname and username.
func (s *Store) ListFriends(ctx context.Context, userID uint64) ([]PublicUser, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT u.username, u.nickname
		 FROM friendships f
		 JOIN users u ON u.id = f.friend_user_id
		 WHERE f.user_id = ?
		 ORDER BY u.nickname, u.username`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list friends: %w", err)
	}
	defer func() { _ = rows.Close() }()

	friends := make([]PublicUser, 0)
	for rows.Next() {
		var friend PublicUser
		if err := rows.Scan(&friend.Username, &friend.Nickname); err != nil {
			return nil, fmt.Errorf("scan friend: %w", err)
		}
		friends = append(friends, friend)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate friends: %w", err)
	}
	return friends, nil
}

// ListFriendRequests returns incoming and outgoing pending requests, newest first.
func (s *Store) ListFriendRequests(ctx context.Context, userID uint64) (FriendRequests, error) {
	result := FriendRequests{
		Incoming: make([]FriendRequest, 0),
		Outgoing: make([]FriendRequest, 0),
	}

	incomingRows, err := s.db.QueryContext(ctx,
		`SELECT r.id, u.username, u.nickname, r.created_at
		 FROM friend_requests r
		 JOIN users u ON u.id = r.from_user_id
		 WHERE r.to_user_id = ?
		 ORDER BY r.created_at DESC, r.id DESC`,
		userID,
	)
	if err != nil {
		return FriendRequests{}, fmt.Errorf("list incoming friend requests: %w", err)
	}
	incoming, err := scanFriendRequests(incomingRows)
	if err != nil {
		return FriendRequests{}, err
	}
	result.Incoming = incoming

	outgoingRows, err := s.db.QueryContext(ctx,
		`SELECT r.id, u.username, u.nickname, r.created_at
		 FROM friend_requests r
		 JOIN users u ON u.id = r.to_user_id
		 WHERE r.from_user_id = ?
		 ORDER BY r.created_at DESC, r.id DESC`,
		userID,
	)
	if err != nil {
		return FriendRequests{}, fmt.Errorf("list outgoing friend requests: %w", err)
	}
	outgoing, err := scanFriendRequests(outgoingRows)
	if err != nil {
		return FriendRequests{}, err
	}
	result.Outgoing = outgoing

	return result, nil
}

// CreateFriendRequest sends a request or accepts an existing reverse request.
func (s *Store) CreateFriendRequest(
	ctx context.Context,
	fromID, toID uint64,
	now time.Time,
) (FriendRequestResult, error) {
	if fromID == toID {
		return FriendRequestResult{}, ErrCannotAddSelf
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FriendRequestResult{}, fmt.Errorf("begin friend request transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := lockUsers(ctx, tx, fromID, toID); err != nil {
		return FriendRequestResult{}, err
	}

	var alreadyFriends int
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM friendships WHERE user_id = ? AND friend_user_id = ?)`,
		fromID, toID,
	).Scan(&alreadyFriends); err != nil {
		return FriendRequestResult{}, fmt.Errorf("check friendship: %w", err)
	}
	if alreadyFriends == 1 {
		return FriendRequestResult{}, ErrAlreadyFriends
	}

	var existingID uint64
	err = tx.QueryRowContext(ctx,
		`SELECT id FROM friend_requests
		 WHERE from_user_id = ? AND to_user_id = ? FOR UPDATE`,
		fromID, toID,
	).Scan(&existingID)
	if err == nil {
		return FriendRequestResult{}, ErrFriendRequestExists
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return FriendRequestResult{}, fmt.Errorf("check existing friend request: %w", err)
	}

	var reverseID uint64
	err = tx.QueryRowContext(ctx,
		`SELECT id FROM friend_requests
		 WHERE from_user_id = ? AND to_user_id = ? FOR UPDATE`,
		toID, fromID,
	).Scan(&reverseID)
	if err == nil {
		if err := acceptMutualRequest(ctx, tx, fromID, toID, now); err != nil {
			return FriendRequestResult{}, err
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM friend_requests
			 WHERE (from_user_id = ? AND to_user_id = ?)
			    OR (from_user_id = ? AND to_user_id = ?)`,
			fromID, toID, toID, fromID,
		); err != nil {
			return FriendRequestResult{}, fmt.Errorf("delete mutual friend requests: %w", err)
		}
		friend, err := publicUserByID(ctx, tx, toID)
		if err != nil {
			return FriendRequestResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return FriendRequestResult{}, fmt.Errorf("commit mutual friend request: %w", err)
		}
		return FriendRequestResult{Accepted: true, Friend: friend}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return FriendRequestResult{}, fmt.Errorf("check reverse friend request: %w", err)
	}

	result, err := tx.ExecContext(ctx,
		`INSERT INTO friend_requests (from_user_id, to_user_id, created_at)
		 VALUES (?, ?, ?)`,
		fromID, toID, now,
	)
	if err != nil {
		return FriendRequestResult{}, fmt.Errorf("insert friend request: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return FriendRequestResult{}, fmt.Errorf("read friend request id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return FriendRequestResult{}, fmt.Errorf("commit friend request: %w", err)
	}
	return FriendRequestResult{RequestID: uint64(id)}, nil
}

// AcceptFriendRequest creates a mutual friendship and removes related requests.
func (s *Store) AcceptFriendRequest(
	ctx context.Context,
	userID, requestID uint64,
	now time.Time,
) (PublicUser, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PublicUser{}, fmt.Errorf("begin accept transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var fromID uint64
	err = tx.QueryRowContext(ctx,
		`SELECT from_user_id FROM friend_requests WHERE id = ? AND to_user_id = ?`,
		requestID, userID,
	).Scan(&fromID)
	if errors.Is(err, sql.ErrNoRows) {
		return PublicUser{}, ErrFriendRequestNotFound
	}
	if err != nil {
		return PublicUser{}, fmt.Errorf("load friend request: %w", err)
	}

	if err := lockUsers(ctx, tx, userID, fromID); err != nil {
		return PublicUser{}, err
	}

	var lockedFromID uint64
	err = tx.QueryRowContext(ctx,
		`SELECT from_user_id FROM friend_requests
		 WHERE id = ? AND to_user_id = ? FOR UPDATE`,
		requestID, userID,
	).Scan(&lockedFromID)
	if errors.Is(err, sql.ErrNoRows) {
		return PublicUser{}, ErrFriendRequestNotFound
	}
	if err != nil {
		return PublicUser{}, fmt.Errorf("lock friend request: %w", err)
	}
	fromID = lockedFromID

	if err := acceptMutualRequest(ctx, tx, fromID, userID, now); err != nil {
		return PublicUser{}, err
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM friend_requests
		 WHERE id = ? OR (from_user_id = ? AND to_user_id = ?)`,
		requestID, userID, fromID,
	); err != nil {
		return PublicUser{}, fmt.Errorf("delete accepted request: %w", err)
	}

	friend, err := publicUserByID(ctx, tx, fromID)
	if err != nil {
		return PublicUser{}, err
	}
	if err := tx.Commit(); err != nil {
		return PublicUser{}, fmt.Errorf("commit accept friend request: %w", err)
	}
	return friend, nil
}

// RejectFriendRequest deletes a pending request addressed to the user.
func (s *Store) RejectFriendRequest(ctx context.Context, userID, requestID uint64) error {
	result, err := s.db.ExecContext(ctx,
		`DELETE FROM friend_requests WHERE id = ? AND to_user_id = ?`,
		requestID, userID,
	)
	if err != nil {
		return fmt.Errorf("reject friend request: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read rejected request result: %w", err)
	}
	if affected == 0 {
		return ErrFriendRequestNotFound
	}
	return nil
}

// DeleteFriend removes both directions of a friendship and related requests.
func (s *Store) DeleteFriend(ctx context.Context, userID, friendID uint64) error {
	if userID == friendID {
		return ErrFriendNotFound
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete friend transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := lockUsers(ctx, tx, userID, friendID); err != nil {
		return err
	}

	result, err := tx.ExecContext(ctx,
		`DELETE FROM friendships
		 WHERE (user_id = ? AND friend_user_id = ?)
		    OR (user_id = ? AND friend_user_id = ?)`,
		userID, friendID, friendID, userID,
	)
	if err != nil {
		return fmt.Errorf("delete friendship: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read deleted friendship result: %w", err)
	}
	if affected == 0 {
		return ErrFriendNotFound
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM friend_requests
		 WHERE (from_user_id = ? AND to_user_id = ?)
		    OR (from_user_id = ? AND to_user_id = ?)`,
		userID, friendID, friendID, userID,
	); err != nil {
		return fmt.Errorf("delete friend requests after unfriend: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete friend: %w", err)
	}
	return nil
}

func lockUsers(ctx context.Context, tx *sql.Tx, firstID, secondID uint64) error {
	if firstID > secondID {
		firstID, secondID = secondID, firstID
	}
	for _, id := range []uint64{firstID, secondID} {
		var lockedID uint64
		if err := tx.QueryRowContext(ctx,
			`SELECT id FROM users WHERE id = ? FOR UPDATE`, id,
		).Scan(&lockedID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrUserNotFound
			}
			return fmt.Errorf("lock user %d: %w", id, err)
		}
	}
	return nil
}

func acceptMutualRequest(
	ctx context.Context,
	tx *sql.Tx,
	firstID, secondID uint64,
	now time.Time,
) error {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO friendships (user_id, friend_user_id, created_at)
		 VALUES (?, ?, ?), (?, ?, ?)
		 ON DUPLICATE KEY UPDATE created_at = created_at`,
		firstID, secondID, now,
		secondID, firstID, now,
	); err != nil {
		return fmt.Errorf("create friendship: %w", err)
	}
	return nil
}

type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func publicUserByID(ctx context.Context, queryer rowQuerier, userID uint64) (PublicUser, error) {
	var user PublicUser
	if err := queryer.QueryRowContext(ctx,
		`SELECT username, nickname FROM users WHERE id = ?`, userID,
	).Scan(&user.Username, &user.Nickname); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return PublicUser{}, ErrUserNotFound
		}
		return PublicUser{}, fmt.Errorf("get public user: %w", err)
	}
	return user, nil
}

func scanFriendRequests(rows *sql.Rows) ([]FriendRequest, error) {
	defer func() { _ = rows.Close() }()

	requests := make([]FriendRequest, 0)
	for rows.Next() {
		var request FriendRequest
		if err := rows.Scan(
			&request.ID,
			&request.User.Username,
			&request.User.Nickname,
			&request.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan friend request: %w", err)
		}
		requests = append(requests, request)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate friend requests: %w", err)
	}
	return requests, nil
}
