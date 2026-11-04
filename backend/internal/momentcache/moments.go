package momentcache

import (
	"context"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// CreateMoment commits to MySQL first and then projects the ID into the
// disposable feed/timeline cache. A Redis failure never turns a successful
// MySQL insert into an HTTP error.
func (s *Store) CreateMoment(
	ctx context.Context,
	authorID uint64,
	body string,
	visibility store.MomentVisibility,
	now time.Time,
) (store.Moment, error) {
	return s.createMoment(ctx, authorID, body, visibility, now)
}

// ListMoments serves a feed page from the materialized cache when it can prove
// the page is complete, otherwise it returns the authoritative MySQL page.
func (s *Store) ListMoments(
	ctx context.Context,
	viewerID uint64,
	friendIDs []uint64,
	beforeID uint64,
	limit int,
) ([]store.Moment, bool, error) {
	return s.listMoments(ctx, viewerID, friendIDs, beforeID, limit)
}

// GetMoment returns the MySQL moment with a warmed like snapshot applied.
func (s *Store) GetMoment(
	ctx context.Context,
	viewerID, momentID uint64,
) (store.Moment, error) {
	return s.getMoment(ctx, viewerID, momentID)
}

// DeleteMoment commits the soft delete first, then removes disposable cache
// entries. A cache failure leaves the MySQL delete successful.
func (s *Store) DeleteMoment(
	ctx context.Context,
	momentID, authorID uint64,
	now time.Time,
) error {
	return s.deleteMoment(ctx, momentID, authorID, now)
}

// LikeMoment keeps the existing MySQL transaction and updates a warmed like
// cache afterwards. The returned count is always the MySQL count.
func (s *Store) LikeMoment(
	ctx context.Context,
	momentID, userID uint64,
	now time.Time,
) (uint64, error) {
	return s.likeMoment(ctx, momentID, userID, now)
}

// UnlikeMoment mirrors LikeMoment.
func (s *Store) UnlikeMoment(
	ctx context.Context,
	momentID, userID uint64,
) (uint64, error) {
	return s.unlikeMoment(ctx, momentID, userID)
}
