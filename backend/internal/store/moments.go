package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrMomentNotFound        = errors.New("moment not found")
	ErrMomentForbidden       = errors.New("moment forbidden")
	ErrMomentCommentNotFound = errors.New("moment comment not found")
)

// MomentVisibility controls who can see a moment.
type MomentVisibility string

const (
	MomentVisibilityFriends MomentVisibility = "friends"
	MomentVisibilityPrivate MomentVisibility = "private"
)

// Valid reports whether the visibility is one of the supported values.
func (v MomentVisibility) Valid() bool {
	return v == MomentVisibilityFriends || v == MomentVisibilityPrivate
}

// Moment is one text post. Author is filled by feed and detail queries;
// Comments carries the preview when the caller asks for one.
type Moment struct {
	ID           uint64
	AuthorID     uint64
	Author       PublicUser
	Body         string
	Visibility   MomentVisibility
	LikeCount    uint64
	CommentCount uint64
	CreatedAt    time.Time
	LikedByMe    bool
	Mine         bool
	Comments     []MomentComment
}

// MomentLike is one user's like on a moment.
type MomentLike struct {
	ID        uint64
	User      PublicUser
	CreatedAt time.Time
}

// MomentComment is one single-level comment on a moment.
type MomentComment struct {
	ID        uint64
	MomentID  uint64
	AuthorID  uint64
	Author    PublicUser
	Body      string
	CreatedAt time.Time
}

// ListFriendIDs returns the directed friendship rows of one user.
func (s *Store) ListFriendIDs(ctx context.Context, userID uint64) ([]uint64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT friend_user_id FROM friendships WHERE user_id = ? ORDER BY friend_user_id`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list friend ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	ids := make([]uint64, 0)
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan friend id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate friend ids: %w", err)
	}
	return ids, nil
}

// CreateMoment inserts one text moment and returns it without comments.
func (s *Store) CreateMoment(
	ctx context.Context,
	authorID uint64,
	body string,
	visibility MomentVisibility,
	now time.Time,
) (Moment, error) {
	if !visibility.Valid() {
		return Moment{}, fmt.Errorf("invalid moment visibility %q", visibility)
	}
	result, err := s.db.ExecContext(ctx,
		`INSERT INTO moments (author_id, body, visibility, created_at)
		 VALUES (?, ?, ?, ?)`,
		authorID, body, string(visibility), now,
	)
	if err != nil {
		return Moment{}, fmt.Errorf("create moment: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Moment{}, fmt.Errorf("read moment id: %w", err)
	}
	return Moment{
		ID:         uint64(id),
		AuthorID:   authorID,
		Body:       body,
		Visibility: visibility,
		CreatedAt:  now,
		Mine:       true,
	}, nil
}

const momentSelect = `SELECT m.id, m.author_id, u.username, u.nickname, m.body,
		m.visibility, m.like_count, m.comment_count, m.created_at,
		EXISTS(SELECT 1 FROM moment_likes l
		       WHERE l.moment_id = m.id AND l.user_id = ?)
	FROM moments m
	JOIN users u ON u.id = m.author_id`

// ListMoments returns the viewer's feed: own moments plus friends' moments
// visible to friends. beforeID is exclusive; zero starts at the newest page.
func (s *Store) ListMoments(
	ctx context.Context,
	viewerID uint64,
	friendIDs []uint64,
	beforeID uint64,
	limit int,
) ([]Moment, bool, error) {
	if limit <= 0 {
		return nil, false, fmt.Errorf("invalid moment limit")
	}
	args := make([]any, 0, len(friendIDs)+5)
	args = append(args, viewerID, viewerID)
	where := `m.deleted_at IS NULL AND (m.author_id = ?`
	if len(friendIDs) > 0 {
		where += ` OR (m.visibility = 'friends' AND m.author_id IN (` +
			strings.TrimSuffix(strings.Repeat("?,", len(friendIDs)), ",") + `))`
		for _, friendID := range friendIDs {
			args = append(args, friendID)
		}
	}
	where += `)`
	if beforeID > 0 {
		where += ` AND m.id < ?`
		args = append(args, beforeID)
	}

	query := momentSelect + ` WHERE ` + where + ` ORDER BY m.id DESC LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list moments: %w", err)
	}
	defer func() { _ = rows.Close() }()

	moments := make([]Moment, 0, limit+1)
	for rows.Next() {
		moment, err := scanMoment(rows, viewerID)
		if err != nil {
			return nil, false, err
		}
		moments = append(moments, moment)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate moments: %w", err)
	}

	hasMore := len(moments) > limit
	if hasMore {
		moments = moments[:limit]
	}
	return moments, hasMore, nil
}

type momentScanner interface {
	Scan(dest ...any) error
}

func scanMoment(row momentScanner, viewerID uint64) (Moment, error) {
	var moment Moment
	var visibility string
	if err := row.Scan(
		&moment.ID,
		&moment.AuthorID,
		&moment.Author.Username,
		&moment.Author.Nickname,
		&moment.Body,
		&visibility,
		&moment.LikeCount,
		&moment.CommentCount,
		&moment.CreatedAt,
		&moment.LikedByMe,
	); err != nil {
		return Moment{}, fmt.Errorf("scan moment: %w", err)
	}
	moment.Visibility = MomentVisibility(visibility)
	moment.Mine = moment.AuthorID == viewerID
	return moment, nil
}

// GetMoment returns one non-deleted moment. It does not check visibility;
// callers must apply the author/friend rule.
func (s *Store) GetMoment(ctx context.Context, viewerID, momentID uint64) (Moment, error) {
	row := s.db.QueryRowContext(ctx,
		momentSelect+` WHERE m.id = ? AND m.deleted_at IS NULL`,
		viewerID, momentID,
	)
	moment, err := scanMoment(row, viewerID)
	if errors.Is(err, sql.ErrNoRows) {
		return Moment{}, ErrMomentNotFound
	}
	if err != nil {
		return Moment{}, err
	}
	return moment, nil
}

// MomentCommentPreviews returns the newest comments of each requested moment.
func (s *Store) MomentCommentPreviews(
	ctx context.Context,
	momentIDs []uint64,
	perMoment int,
) (map[uint64][]MomentComment, error) {
	previews := make(map[uint64][]MomentComment, len(momentIDs))
	if len(momentIDs) == 0 || perMoment <= 0 {
		return previews, nil
	}
	args := make([]any, 0, len(momentIDs)+1)
	for _, momentID := range momentIDs {
		args = append(args, momentID)
	}
	args = append(args, perMoment)
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, moment_id, author_id, username, nickname, body, created_at
		 FROM (
		   SELECT c.id, c.moment_id, c.author_id, u.username, u.nickname,
		          c.body, c.created_at,
		          ROW_NUMBER() OVER (PARTITION BY c.moment_id ORDER BY c.id DESC) AS rn
		   FROM moment_comments c
		   JOIN users u ON u.id = c.author_id
		   WHERE c.deleted_at IS NULL AND c.moment_id IN (`+
			strings.TrimSuffix(strings.Repeat("?,", len(momentIDs)), ",")+`)
		 ) ranked
		 WHERE rn <= ?
		 ORDER BY moment_id ASC, id ASC`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("list moment comment previews: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var comment MomentComment
		if err := rows.Scan(
			&comment.ID,
			&comment.MomentID,
			&comment.AuthorID,
			&comment.Author.Username,
			&comment.Author.Nickname,
			&comment.Body,
			&comment.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan moment comment preview: %w", err)
		}
		previews[comment.MomentID] = append(previews[comment.MomentID], comment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate moment comment previews: %w", err)
	}
	return previews, nil
}

// DeleteMoment soft-deletes a moment owned by authorID.
func (s *Store) DeleteMoment(
	ctx context.Context,
	momentID, authorID uint64,
	now time.Time,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete moment: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var ownerID uint64
	err = tx.QueryRowContext(ctx,
		`SELECT author_id FROM moments WHERE id = ? AND deleted_at IS NULL FOR UPDATE`,
		momentID,
	).Scan(&ownerID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrMomentNotFound
	}
	if err != nil {
		return fmt.Errorf("load moment for delete: %w", err)
	}
	if ownerID != authorID {
		return ErrMomentForbidden
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE moments SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL`,
		now, momentID,
	); err != nil {
		return fmt.Errorf("delete moment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete moment: %w", err)
	}
	return nil
}

// LikeMoment likes a moment idempotently and returns the current like count.
func (s *Store) LikeMoment(
	ctx context.Context,
	momentID, userID uint64,
	now time.Time,
) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin like moment: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var likeCount uint64
	err = tx.QueryRowContext(ctx,
		`SELECT like_count FROM moments WHERE id = ? AND deleted_at IS NULL FOR UPDATE`,
		momentID,
	).Scan(&likeCount)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrMomentNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("load moment for like: %w", err)
	}

	result, err := tx.ExecContext(ctx,
		`INSERT IGNORE INTO moment_likes (moment_id, user_id, created_at)
		 VALUES (?, ?, ?)`,
		momentID, userID, now,
	)
	if err != nil {
		return 0, fmt.Errorf("insert moment like: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read moment like rows: %w", err)
	}
	if affected == 1 {
		if _, err := tx.ExecContext(ctx,
			`UPDATE moments SET like_count = like_count + 1 WHERE id = ?`,
			momentID,
		); err != nil {
			return 0, fmt.Errorf("increment moment like count: %w", err)
		}
		likeCount++
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit like moment: %w", err)
	}
	return likeCount, nil
}

// UnlikeMoment removes a like idempotently and returns the current like count.
func (s *Store) UnlikeMoment(
	ctx context.Context,
	momentID, userID uint64,
) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin unlike moment: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var likeCount uint64
	err = tx.QueryRowContext(ctx,
		`SELECT like_count FROM moments WHERE id = ? AND deleted_at IS NULL FOR UPDATE`,
		momentID,
	).Scan(&likeCount)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrMomentNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("load moment for unlike: %w", err)
	}

	result, err := tx.ExecContext(ctx,
		`DELETE FROM moment_likes WHERE moment_id = ? AND user_id = ?`,
		momentID, userID,
	)
	if err != nil {
		return 0, fmt.Errorf("delete moment like: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read moment unlike rows: %w", err)
	}
	if affected == 1 {
		if _, err := tx.ExecContext(ctx,
			`UPDATE moments
			 SET like_count = IF(like_count > 0, like_count - 1, 0)
			 WHERE id = ?`,
			momentID,
		); err != nil {
			return 0, fmt.Errorf("decrement moment like count: %w", err)
		}
		if likeCount > 0 {
			likeCount--
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit unlike moment: %w", err)
	}
	return likeCount, nil
}

// ListMomentLikes returns newest likes first.
func (s *Store) ListMomentLikes(
	ctx context.Context,
	momentID, beforeID uint64,
	limit int,
) ([]MomentLike, bool, error) {
	if limit <= 0 {
		return nil, false, fmt.Errorf("invalid moment like limit")
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT l.id, u.username, u.nickname, l.created_at
		 FROM moment_likes l
		 JOIN users u ON u.id = l.user_id
		 WHERE l.moment_id = ? AND (? = 0 OR l.id < ?)
		 ORDER BY l.id DESC
		 LIMIT ?`,
		momentID, beforeID, beforeID, limit+1,
	)
	if err != nil {
		return nil, false, fmt.Errorf("list moment likes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	likes := make([]MomentLike, 0, limit+1)
	for rows.Next() {
		var like MomentLike
		if err := rows.Scan(
			&like.ID,
			&like.User.Username,
			&like.User.Nickname,
			&like.CreatedAt,
		); err != nil {
			return nil, false, fmt.Errorf("scan moment like: %w", err)
		}
		likes = append(likes, like)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate moment likes: %w", err)
	}
	hasMore := len(likes) > limit
	if hasMore {
		likes = likes[:limit]
	}
	return likes, hasMore, nil
}

// ListMomentComments returns one page of older comments, ascending. beforeID
// is exclusive; zero returns the newest page.
func (s *Store) ListMomentComments(
	ctx context.Context,
	momentID, beforeID uint64,
	limit int,
) ([]MomentComment, bool, error) {
	if limit <= 0 {
		return nil, false, fmt.Errorf("invalid moment comment limit")
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT c.id, c.moment_id, c.author_id, u.username, u.nickname,
		        c.body, c.created_at
		 FROM moment_comments c
		 JOIN users u ON u.id = c.author_id
		 WHERE c.moment_id = ? AND c.deleted_at IS NULL
		   AND (? = 0 OR c.id < ?)
		 ORDER BY c.id DESC
		 LIMIT ?`,
		momentID, beforeID, beforeID, limit+1,
	)
	if err != nil {
		return nil, false, fmt.Errorf("list moment comments: %w", err)
	}
	defer func() { _ = rows.Close() }()

	newestFirst := make([]MomentComment, 0, limit+1)
	for rows.Next() {
		var comment MomentComment
		if err := rows.Scan(
			&comment.ID,
			&comment.MomentID,
			&comment.AuthorID,
			&comment.Author.Username,
			&comment.Author.Nickname,
			&comment.Body,
			&comment.CreatedAt,
		); err != nil {
			return nil, false, fmt.Errorf("scan moment comment: %w", err)
		}
		newestFirst = append(newestFirst, comment)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate moment comments: %w", err)
	}

	hasMore := len(newestFirst) > limit
	if hasMore {
		newestFirst = newestFirst[:limit]
	}
	comments := make([]MomentComment, len(newestFirst))
	for index, comment := range newestFirst {
		comments[len(newestFirst)-1-index] = comment
	}
	return comments, hasMore, nil
}

// CreateMomentComment inserts a comment and increments the moment counter.
func (s *Store) CreateMomentComment(
	ctx context.Context,
	momentID, authorID uint64,
	body string,
	now time.Time,
) (MomentComment, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MomentComment{}, fmt.Errorf("begin create moment comment: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var exists uint64
	err = tx.QueryRowContext(ctx,
		`SELECT id FROM moments WHERE id = ? AND deleted_at IS NULL FOR UPDATE`,
		momentID,
	).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return MomentComment{}, ErrMomentNotFound
	}
	if err != nil {
		return MomentComment{}, fmt.Errorf("load moment for comment: %w", err)
	}

	result, err := tx.ExecContext(ctx,
		`INSERT INTO moment_comments (moment_id, author_id, body, created_at)
		 VALUES (?, ?, ?, ?)`,
		momentID, authorID, body, now,
	)
	if err != nil {
		return MomentComment{}, fmt.Errorf("create moment comment: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return MomentComment{}, fmt.Errorf("read moment comment id: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE moments SET comment_count = comment_count + 1 WHERE id = ?`,
		momentID,
	); err != nil {
		return MomentComment{}, fmt.Errorf("increment moment comment count: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return MomentComment{}, fmt.Errorf("commit create moment comment: %w", err)
	}
	return MomentComment{
		ID:        uint64(id),
		MomentID:  momentID,
		AuthorID:  authorID,
		Body:      body,
		CreatedAt: now,
	}, nil
}

// DeleteMomentComment soft-deletes a comment. The comment author and the
// moment author may delete it.
func (s *Store) DeleteMomentComment(
	ctx context.Context,
	momentID, commentID, userID uint64,
	now time.Time,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete moment comment: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var commentAuthor, momentAuthor uint64
	var commentActive, momentActive bool
	err = tx.QueryRowContext(ctx,
		`SELECT c.author_id, c.deleted_at IS NULL, m.author_id, m.deleted_at IS NULL
		 FROM moment_comments c
		 JOIN moments m ON m.id = c.moment_id
		 WHERE c.id = ? AND c.moment_id = ?
		 FOR UPDATE`,
		commentID, momentID,
	).Scan(&commentAuthor, &commentActive, &momentAuthor, &momentActive)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrMomentCommentNotFound
	}
	if err != nil {
		return fmt.Errorf("load moment comment for delete: %w", err)
	}
	if !momentActive {
		return ErrMomentNotFound
	}
	if !commentActive {
		return ErrMomentCommentNotFound
	}
	if userID != commentAuthor && userID != momentAuthor {
		return ErrMomentForbidden
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE moment_comments SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL`,
		now, commentID,
	); err != nil {
		return fmt.Errorf("delete moment comment: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE moments
		 SET comment_count = IF(comment_count > 0, comment_count - 1, 0)
		 WHERE id = ?`,
		momentID,
	); err != nil {
		return fmt.Errorf("decrement moment comment count: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete moment comment: %w", err)
	}
	return nil
}

// MomentUnread reports whether a friend posted a new friends-visible moment
// after the viewer's last seen id.
func (s *Store) MomentUnread(
	ctx context.Context,
	viewerID uint64,
	friendIDs []uint64,
) (uint64, bool, error) {
	var lastSeenID uint64
	err := s.db.QueryRowContext(ctx,
		`SELECT last_seen_id FROM moment_seen WHERE user_id = ?`,
		viewerID,
	).Scan(&lastSeenID)
	if errors.Is(err, sql.ErrNoRows) {
		lastSeenID = 0
	} else if err != nil {
		return 0, false, fmt.Errorf("read moment seen: %w", err)
	}
	if len(friendIDs) == 0 {
		return lastSeenID, false, nil
	}

	args := make([]any, 0, len(friendIDs)+1)
	args = append(args, lastSeenID)
	for _, friendID := range friendIDs {
		args = append(args, friendID)
	}
	var hasNew int
	if err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(
		   SELECT 1 FROM moments
		   WHERE deleted_at IS NULL AND visibility = 'friends'
		     AND id > ? AND author_id IN (`+
			strings.TrimSuffix(strings.Repeat("?,", len(friendIDs)), ",")+`)
		 )`,
		args...,
	).Scan(&hasNew); err != nil {
		return 0, false, fmt.Errorf("check moment unread: %w", err)
	}
	return lastSeenID, hasNew == 1, nil
}

// MarkMomentsSeen moves the viewer's seen watermark forward and returns the
// effective value.
func (s *Store) MarkMomentsSeen(
	ctx context.Context,
	viewerID, lastSeenID uint64,
	now time.Time,
) (uint64, error) {
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO moment_seen (user_id, last_seen_id, updated_at)
		 VALUES (?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		   last_seen_id = IF(? > last_seen_id, ?, last_seen_id),
		   updated_at = ?`,
		viewerID, lastSeenID, now,
		lastSeenID, lastSeenID, now,
	); err != nil {
		return 0, fmt.Errorf("mark moments seen: %w", err)
	}
	var effective uint64
	if err := s.db.QueryRowContext(ctx,
		`SELECT last_seen_id FROM moment_seen WHERE user_id = ?`,
		viewerID,
	).Scan(&effective); err != nil {
		return 0, fmt.Errorf("read moment seen watermark: %w", err)
	}
	return effective, nil
}
