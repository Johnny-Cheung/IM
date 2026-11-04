package momentcache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/httpapi"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/redis/go-redis/v9"
)

const (
	cacheFailureThreshold = 3
	cacheDegradedWindow   = 5 * time.Second
)

var errCacheUnavailable = fmt.Errorf("moments cache unavailable")

// Backend is the authoritative MySQL surface used by the cache wrapper.
type Backend interface {
	httpapi.MomentStore
	ListVisibleMomentsByIDs(
		ctx context.Context,
		viewerID uint64,
		friendIDs []uint64,
		momentIDs []uint64,
	) ([]store.Moment, error)
	ListAuthorMomentIDs(
		ctx context.Context,
		authorID, beforeID uint64,
		limit int,
	) ([]uint64, bool, error)
	ListMomentLikeUserIDs(
		ctx context.Context,
		momentIDs []uint64,
	) (map[uint64][]uint64, error)
}

// Store is an optional cache-aside wrapper around the authoritative Moments
// store. Every write succeeds or fails in MySQL first; Redis failures are
// absorbed and reads fall back to MySQL.
type Store struct {
	httpapi.MomentStore
	backend  Backend
	redis    redis.UniversalClient
	keys     Keyspace
	options  Options
	counters counters

	likeMu sync.Mutex

	likeDirty sync.Map
	feedDirty sync.Map

	failures      atomic.Int32
	degradedUntil atomic.Int64
	feedAllDirty  atomic.Bool
	lastErrorLog  atomic.Int64
}

// New wraps backend. A nil client or disabled option produces a pass-through
// store, which keeps the MySQL-only deployment path explicit and testable.
func New(
	backend Backend,
	client redis.UniversalClient,
	keys Keyspace,
	options Options,
) *Store {
	options = options.withDefaults()
	return &Store{
		MomentStore: backend,
		backend:     backend,
		redis:       client,
		keys:        keys,
		options:     options,
	}
}

// Status returns the latest process-local cache counters.
func (s *Store) Status() Status {
	if s == nil {
		return Status{}
	}
	return s.counters.snapshot(s.enabled(), s.degraded())
}

func (s *Store) enabled() bool {
	return s != nil && s.options.Enabled && s.redis != nil
}

func (s *Store) available() bool {
	if !s.enabled() {
		return false
	}
	until := s.degradedUntil.Load()
	return until == 0 || time.Now().UnixNano() >= until
}

func (s *Store) degraded() bool {
	return s.enabled() && (s.feedAllDirty.Load() || !s.available())
}

func (s *Store) cacheContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.options.OperationTimeout)
}

// projectionContext keeps a committed write from losing its best-effort cache
// projection just because the HTTP client disconnected. The work is still
// bounded, and MySQL has already committed before this context is used.
func projectionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

func (s *Store) recordRedisError(err error) {
	if s == nil || err == nil {
		return
	}
	s.counters.cacheErrors.Add(1)
	if s.failures.Add(1) >= cacheFailureThreshold {
		s.degradedUntil.Store(time.Now().Add(cacheDegradedWindow).UnixNano())
	}
	s.logErrorThrottled("moments cache Redis error: %v", err)
}

func (s *Store) recordRedisSuccess() {
	if s != nil {
		s.failures.Store(0)
	}
}

func (s *Store) markDegraded() {
	s.degradedUntil.Store(time.Now().Add(cacheDegradedWindow).UnixNano())
}

func (s *Store) markLikeDirty(momentID uint64) {
	s.likeDirty.Store(momentID, struct{}{})
}

func (s *Store) clearLikeDirty(momentID uint64) {
	s.likeDirty.Delete(momentID)
}

func (s *Store) likeIsDirty(momentID uint64) bool {
	_, ok := s.likeDirty.Load(momentID)
	return ok
}

func (s *Store) markFeedDirty(viewerID uint64) {
	if viewerID != 0 {
		s.feedDirty.Store(viewerID, struct{}{})
	}
}

func (s *Store) clearFeedDirty(viewerID uint64) {
	s.feedDirty.Delete(viewerID)
}

func (s *Store) feedIsDirty(viewerID uint64) bool {
	_, ok := s.feedDirty.Load(viewerID)
	return ok
}

func (s *Store) logErrorThrottled(format string, args ...any) {
	now := time.Now().UnixNano()
	last := s.lastErrorLog.Load()
	if last != 0 && now-last < int64(10*time.Second) {
		return
	}
	if s.lastErrorLog.CompareAndSwap(last, now) {
		log.Printf(format, args...)
	}
}

func randomToken() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("generate cache token: %w", err)
	}
	return hex.EncodeToString(data[:]), nil
}

func cacheScore(id uint64) float64 {
	return float64(id)
}

func parseUint(value string) (uint64, bool) {
	if value == "" {
		return 0, false
	}
	var id uint64
	if _, err := fmt.Sscanf(value, "%d", &id); err != nil {
		return 0, false
	}
	return id, true
}
