package momentcache

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/redis/go-redis/v9"
)

var errTooManyPullSources = errors.New("too many pull moment sources")

const (
	feedMetaFloor      = "floor"
	feedMetaFriends    = "friends_hash"
	feedMetaCount      = "count"
	feedLockTTL        = 30 * time.Second
	feedWarmMultiplier = 4
)

const feedFinalizeScript = `
local generation = redis.call('GET', KEYS[1])
if not generation then
  generation = '0'
end
if generation ~= ARGV[1] then
  if redis.call('GET', KEYS[4]) == ARGV[2] then
    redis.call('DEL', KEYS[4])
  end
  redis.call('DEL', KEYS[3])
  return 0
end
if redis.call('GET', KEYS[4]) ~= ARGV[2] then
  redis.call('DEL', KEYS[3])
  return 0
end

redis.call('ZUNIONSTORE', KEYS[2], 2, KEYS[2], KEYS[3], 'AGGREGATE', 'MAX')
local beforeTrim = redis.call('ZCARD', KEYS[2])
redis.call('ZREMRANGEBYRANK', KEYS[2], 0, -(tonumber(ARGV[5]) + 1))
local afterTrim = redis.call('ZCARD', KEYS[2])
local floor = tonumber(ARGV[3])
local minimum = 0
if afterTrim > 0 then
  minimum = tonumber(redis.call('ZRANGE', KEYS[2], 0, 0)[1])
end
if afterTrim == 0 then
  floor = 0
elseif floor == 0 and beforeTrim > tonumber(ARGV[5]) then
  floor = minimum
elseif floor > 0 and minimum > floor then
  floor = minimum
end

redis.call('HSET', KEYS[5],
  'floor', tostring(floor),
  'friends_hash', ARGV[4],
  'count', tostring(afterTrim))
redis.call('PEXPIRE', KEYS[2], ARGV[6])
redis.call('PEXPIRE', KEYS[5], ARGV[6])
redis.call('DEL', KEYS[3])
if redis.call('GET', KEYS[4]) == ARGV[2] then
  redis.call('DEL', KEYS[4])
end
return 1
`

const authorFinalizeScript = `
if redis.call('GET', KEYS[3]) ~= ARGV[1] then
  redis.call('DEL', KEYS[2])
  return 0
end

redis.call('ZUNIONSTORE', KEYS[1], 2, KEYS[1], KEYS[2], 'AGGREGATE', 'MAX')
local beforeTrim = redis.call('ZCARD', KEYS[1])
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[4]) + 1))
local afterTrim = redis.call('ZCARD', KEYS[1])
local floor = tonumber(ARGV[2])
local minimum = 0
if afterTrim > 0 then
  minimum = tonumber(redis.call('ZRANGE', KEYS[1], 0, 0)[1])
end
if afterTrim == 0 then
  floor = 0
elseif floor == 0 and beforeTrim > tonumber(ARGV[4]) then
  floor = minimum
elseif floor > 0 and minimum > floor then
  floor = minimum
end

redis.call('HSET', KEYS[4], 'floor', tostring(floor), 'count', tostring(afterTrim))
redis.call('PEXPIRE', KEYS[1], ARGV[3])
redis.call('PEXPIRE', KEYS[4], ARGV[3])
redis.call('DEL', KEYS[2])
if redis.call('GET', KEYS[3]) == ARGV[1] then
  redis.call('DEL', KEYS[3])
end
return 1
`

const feedPushScript = `
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
local card = redis.call('ZCARD', KEYS[1])
local trimmed = false
if card > tonumber(ARGV[2]) then
  trimmed = true
  redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[2]) + 1))
  if redis.call('EXISTS', KEYS[2]) == 1 then
    local minimum = redis.call('ZRANGE', KEYS[1], 0, 0)[1]
    if minimum then
      local floor = redis.call('HGET', KEYS[2], 'floor')
      if not floor or floor == '0' or tonumber(minimum) > tonumber(floor) then
        redis.call('HSET', KEYS[2], 'floor', minimum)
      end
    end
  end
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  if not trimmed then
    local previous = tonumber(redis.call('HGET', KEYS[2], 'count') or '0')
    card = redis.call('ZCARD', KEYS[1])
    if card < previous then
      redis.call('DEL', KEYS[2])
      return 1
    end
  end
  redis.call('HSET', KEYS[2], 'count', tostring(redis.call('ZCARD', KEYS[1])))
end
return 1
`

const releaseLockScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

var (
	feedFinalize   = redis.NewScript(feedFinalizeScript)
	authorFinalize = redis.NewScript(authorFinalizeScript)
	releaseLock    = redis.NewScript(releaseLockScript)
)

func (s *Store) listMoments(
	ctx context.Context,
	viewerID uint64,
	friendIDs []uint64,
	beforeID uint64,
	limit int,
) ([]store.Moment, bool, error) {
	if s.enabled() && s.feedAllDirty.Load() {
		s.counters.feedFallbacks.Add(1)
		return s.fallbackFeed(ctx, viewerID, friendIDs, beforeID, limit)
	}
	if s.enabled() && s.feedIsDirty(viewerID) {
		if err := s.invalidateViewers(ctx, []uint64{viewerID}); err == nil {
			s.clearFeedDirty(viewerID)
		} else {
			s.recordRedisError(err)
		}
		s.counters.feedFallbacks.Add(1)
		return s.fallbackFeed(ctx, viewerID, friendIDs, beforeID, limit)
	}
	if !s.available() || limit <= 0 || beforeID > (1<<53-1) {
		return s.fallbackFeed(ctx, viewerID, friendIDs, beforeID, limit)
	}

	moments, hasMore, ok, err := s.cachedFeedPage(ctx, viewerID, friendIDs, beforeID, limit)
	if err != nil {
		return nil, false, err
	}
	if ok {
		s.counters.feedHits.Add(1)
		s.enrichLikes(ctx, viewerID, moments)
		return moments, hasMore, nil
	}

	s.counters.feedFallbacks.Add(1)
	return s.fallbackFeed(ctx, viewerID, friendIDs, beforeID, limit)
}

func (s *Store) fallbackFeed(
	ctx context.Context,
	viewerID uint64,
	friendIDs []uint64,
	beforeID uint64,
	limit int,
) ([]store.Moment, bool, error) {
	moments, hasMore, err := s.backend.ListMoments(ctx, viewerID, friendIDs, beforeID, limit)
	if err != nil {
		return nil, false, err
	}
	s.enrichLikes(ctx, viewerID, moments)
	return moments, hasMore, nil
}

func (s *Store) cachedFeedPage(
	ctx context.Context,
	viewerID uint64,
	friendIDs []uint64,
	beforeID uint64,
	limit int,
) ([]store.Moment, bool, bool, error) {
	friendHash := hashFriendIDs(friendIDs)
	floor, ok, err := s.feedMeta(ctx, viewerID, friendHash)
	if err != nil {
		s.recordRedisError(err)
		return nil, false, false, nil
	}
	if !ok {
		warmed, warmErr := s.warmViewerFeed(ctx, viewerID, friendIDs, friendHash)
		if warmErr != nil {
			return nil, false, false, warmErr
		}
		if !warmed {
			return nil, false, false, nil
		}
		floor, ok, err = s.feedMeta(ctx, viewerID, friendHash)
		if err != nil {
			s.recordRedisError(err)
			return nil, false, false, nil
		}
		if !ok {
			return nil, false, false, nil
		}
	}

	pullAuthors, err := s.pullAuthorsFor(ctx, friendIDs)
	if err != nil {
		if !errors.Is(err, errTooManyPullSources) {
			s.recordRedisError(err)
		}
		return nil, false, false, nil
	}
	authorFloors := make([]uint64, 0, len(pullAuthors))
	for _, authorID := range pullAuthors {
		authorFloor, ready, authorErr := s.authorMeta(ctx, authorID)
		if authorErr != nil {
			s.recordRedisError(authorErr)
			return nil, false, false, nil
		}
		if !ready {
			warmed, warmErr := s.warmAuthorTimeline(ctx, authorID)
			if warmErr != nil {
				return nil, false, false, warmErr
			}
			if !warmed {
				return nil, false, false, nil
			}
			authorFloor, ready, authorErr = s.authorMeta(ctx, authorID)
			if authorErr != nil {
				s.recordRedisError(authorErr)
				return nil, false, false, nil
			}
			if !ready {
				return nil, false, false, nil
			}
		}
		authorFloors = append(authorFloors, authorFloor)
	}
	if len(pullAuthors) > 0 {
		s.counters.pullSources.Add(uint64(len(pullAuthors)))
	}

	cacheFloor := floor
	for _, authorFloor := range authorFloors {
		if authorFloor > cacheFloor {
			cacheFloor = authorFloor
		}
	}
	if cacheFloor > 0 && beforeID > 0 && beforeID <= cacheFloor {
		return nil, false, false, nil
	}

	candidateLimit := (limit + 1) * feedWarmMultiplier
	sources := 1 + len(pullAuthors)
	ids := make([][]uint64, 0, sources)
	truncated := false
	feedIDs, err := s.readFeedIDs(ctx, s.keys.feedKey(viewerID), beforeID, cacheFloor, candidateLimit)
	if err != nil {
		s.recordRedisError(err)
		return nil, false, false, nil
	}
	truncated = truncated || len(feedIDs) >= candidateLimit
	ids = append(ids, feedIDs)
	for _, authorID := range pullAuthors {
		authorIDs, readErr := s.readFeedIDs(
			ctx,
			s.keys.authorKey(authorID),
			beforeID,
			cacheFloor,
			candidateLimit,
		)
		if readErr != nil {
			s.recordRedisError(readErr)
			return nil, false, false, nil
		}
		truncated = truncated || len(authorIDs) >= candidateLimit
		ids = append(ids, authorIDs)
	}
	merged := mergeMomentIDs(ids)
	if len(merged) == 0 {
		if cacheFloor == 0 && !truncated {
			return []store.Moment{}, false, true, nil
		}
		return nil, false, false, nil
	}
	tooManyCandidates := truncated || len(merged) > candidateLimit
	if len(merged) > candidateLimit {
		merged = merged[:candidateLimit]
	}

	visible, err := s.backend.ListVisibleMomentsByIDs(ctx, viewerID, friendIDs, merged)
	if err != nil {
		return nil, false, false, err
	}
	if len(visible) >= limit+1 {
		return visible[:limit], true, true, nil
	}
	if cacheFloor == 0 && !tooManyCandidates {
		return visible, false, true, nil
	}
	return nil, false, false, nil
}

func (s *Store) readFeedIDs(
	ctx context.Context,
	key string,
	beforeID, floor uint64,
	count int,
) ([]uint64, error) {
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	minimum := "-inf"
	if floor > 0 {
		minimum = strconv.FormatUint(floor, 10)
	}
	maximum := "+inf"
	if beforeID > 0 {
		maximum = strconv.FormatUint(beforeID-1, 10)
	}
	values, err := s.redis.ZRevRangeByScore(cacheCtx, key, &redis.ZRangeBy{
		Min:    minimum,
		Max:    maximum,
		Offset: 0,
		Count:  int64(count),
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	ids := make([]uint64, 0, len(values))
	for _, value := range values {
		id, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func mergeMomentIDs(groups [][]uint64) []uint64 {
	seen := make(map[uint64]struct{})
	ids := make([]uint64, 0)
	for _, group := range groups {
		for _, id := range group {
			if id == 0 {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	return ids
}

func (s *Store) feedMeta(
	ctx context.Context,
	viewerID uint64,
	friendHash string,
) (uint64, bool, error) {
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	fields, err := s.redis.HGetAll(cacheCtx, s.keys.feedMetaKey(viewerID)).Result()
	if err != nil {
		return 0, false, err
	}
	if len(fields) == 0 || fields[feedMetaFriends] != friendHash {
		return 0, false, nil
	}
	floor, ok := parseUint(fields[feedMetaFloor])
	if !ok {
		return 0, false, nil
	}
	expectedCount, ok := parseUint(fields[feedMetaCount])
	if !ok {
		return 0, false, nil
	}
	card, err := s.redis.ZCard(cacheCtx, s.keys.feedKey(viewerID)).Result()
	if err != nil {
		return 0, false, err
	}
	if card != int64(expectedCount) {
		return 0, false, nil
	}
	return floor, true, nil
}

func (s *Store) authorMeta(ctx context.Context, authorID uint64) (uint64, bool, error) {
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	fields, err := s.redis.HGetAll(cacheCtx, s.keys.authorMetaKey(authorID)).Result()
	if err != nil {
		return 0, false, err
	}
	if len(fields) == 0 {
		return 0, false, nil
	}
	floor, ok := parseUint(fields[feedMetaFloor])
	if !ok {
		return 0, false, nil
	}
	expectedCount, ok := parseUint(fields[feedMetaCount])
	if !ok {
		return 0, false, nil
	}
	card, err := s.redis.ZCard(cacheCtx, s.keys.authorKey(authorID)).Result()
	if err != nil {
		return 0, false, err
	}
	if card != int64(expectedCount) {
		return 0, false, nil
	}
	return floor, true, nil
}

func (s *Store) pullAuthorsFor(ctx context.Context, friendIDs []uint64) ([]uint64, error) {
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	pull, err := s.redis.SMembers(cacheCtx, s.keys.pullAuthorsKey()).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	if len(pull) == 0 || len(friendIDs) == 0 {
		return nil, nil
	}
	pullSet := make(map[uint64]struct{}, len(pull))
	for _, value := range pull {
		id, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			continue
		}
		pullSet[id] = struct{}{}
	}
	authors := make([]uint64, 0)
	for _, friendID := range friendIDs {
		if _, ok := pullSet[friendID]; ok {
			authors = append(authors, friendID)
		}
	}
	if len(authors) > s.options.MaxPullSources {
		return nil, fmt.Errorf(
			"%w: %d > %d",
			errTooManyPullSources,
			len(authors),
			s.options.MaxPullSources,
		)
	}
	sort.Slice(authors, func(i, j int) bool { return authors[i] < authors[j] })
	return authors, nil
}

func (s *Store) warmViewerFeed(
	ctx context.Context,
	viewerID uint64,
	friendIDs []uint64,
	friendHash string,
) (bool, error) {
	if !s.available() {
		return false, nil
	}
	if _, ok, err := s.feedMeta(ctx, viewerID, friendHash); err == nil && ok {
		return true, nil
	}
	token, err := randomToken()
	if err != nil {
		return false, nil
	}
	acquired, err := s.acquireLock(ctx, s.keys.feedWarmLockKey(viewerID), token)
	if err != nil {
		s.recordRedisError(err)
		return false, nil
	}
	if !acquired {
		return false, nil
	}
	defer s.releaseLock(ctx, s.keys.feedWarmLockKey(viewerID), token)

	gen, err := s.readGeneration(ctx, viewerID)
	if err != nil {
		s.recordRedisError(err)
		return false, nil
	}
	moments, hasMore, err := s.backend.ListMoments(
		ctx,
		viewerID,
		friendIDs,
		0,
		s.options.FeedWarmLimit,
	)
	if err != nil {
		s.counters.warmErrors.Add(1)
		return false, err
	}
	floor := uint64(0)
	if hasMore && len(moments) > 0 {
		floor = moments[len(moments)-1].ID
	}
	tempKey := s.keys.feedTempKey(viewerID, token)
	if err := s.writeTempTimeline(ctx, tempKey, moments); err != nil {
		s.counters.warmErrors.Add(1)
		s.recordRedisError(err)
		return false, nil
	}
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	result, err := feedFinalize.Run(
		cacheCtx,
		s.redis,
		[]string{
			s.keys.feedGenKey(viewerID),
			s.keys.feedKey(viewerID),
			tempKey,
			s.keys.feedWarmLockKey(viewerID),
			s.keys.feedMetaKey(viewerID),
		},
		gen,
		token,
		strconv.FormatUint(floor, 10),
		friendHash,
		strconv.Itoa(s.options.FeedWarmLimit),
		strconv.FormatInt(s.options.FeedTTL.Milliseconds(), 10),
	).Int()
	if err != nil {
		s.counters.warmErrors.Add(1)
		s.recordRedisError(err)
		return false, nil
	}
	if result != 1 {
		return false, nil
	}
	s.clearFeedDirty(viewerID)
	s.counters.feedWarms.Add(1)
	s.recordRedisSuccess()
	return true, nil
}

func (s *Store) warmAuthorTimeline(ctx context.Context, authorID uint64) (bool, error) {
	if !s.available() {
		return false, nil
	}
	if _, ready, err := s.authorMeta(ctx, authorID); err == nil && ready {
		return true, nil
	}
	token, err := randomToken()
	if err != nil {
		return false, nil
	}
	acquired, err := s.acquireLock(ctx, s.keys.authorWarmLockKey(authorID), token)
	if err != nil {
		s.recordRedisError(err)
		return false, nil
	}
	if !acquired {
		return false, nil
	}
	defer s.releaseLock(ctx, s.keys.authorWarmLockKey(authorID), token)

	ids, hasMore, err := s.backend.ListAuthorMomentIDs(
		ctx,
		authorID,
		0,
		s.options.FeedWarmLimit,
	)
	if err != nil {
		return false, err
	}
	floor := uint64(0)
	if hasMore && len(ids) > 0 {
		floor = ids[len(ids)-1]
	}
	tempKey := s.keys.authorTempKey(authorID, token)
	if err := s.writeTempIDs(ctx, tempKey, ids); err != nil {
		s.counters.warmErrors.Add(1)
		s.recordRedisError(err)
		return false, nil
	}
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	result, err := authorFinalize.Run(
		cacheCtx,
		s.redis,
		[]string{
			s.keys.authorKey(authorID),
			tempKey,
			s.keys.authorWarmLockKey(authorID),
			s.keys.authorMetaKey(authorID),
		},
		token,
		strconv.FormatUint(floor, 10),
		strconv.FormatInt(s.options.FeedTTL.Milliseconds(), 10),
		strconv.Itoa(s.options.FeedWarmLimit),
	).Int()
	if err != nil {
		s.counters.warmErrors.Add(1)
		s.recordRedisError(err)
		return false, nil
	}
	if result != 1 {
		return false, nil
	}
	s.recordRedisSuccess()
	return true, nil
}

func (s *Store) writeTempTimeline(ctx context.Context, key string, moments []store.Moment) error {
	if len(moments) == 0 {
		return nil
	}
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	values := make([]redis.Z, 0, len(moments))
	for _, moment := range moments {
		values = append(values, redis.Z{Score: cacheScore(moment.ID), Member: moment.ID})
	}
	if err := s.redis.ZAdd(cacheCtx, key, values...).Err(); err != nil {
		return err
	}
	return s.redis.Expire(cacheCtx, key, feedLockTTL).Err()
}

func (s *Store) writeTempIDs(ctx context.Context, key string, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	values := make([]redis.Z, 0, len(ids))
	for _, id := range ids {
		values = append(values, redis.Z{Score: cacheScore(id), Member: id})
	}
	if err := s.redis.ZAdd(cacheCtx, key, values...).Err(); err != nil {
		return err
	}
	return s.redis.Expire(cacheCtx, key, feedLockTTL).Err()
}

func (s *Store) acquireLock(ctx context.Context, key, token string) (bool, error) {
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	return s.redis.SetNX(cacheCtx, key, token, feedLockTTL).Result()
}

func (s *Store) releaseLock(ctx context.Context, key, token string) {
	if !s.available() {
		return
	}
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	if _, err := releaseLock.Run(cacheCtx, s.redis, []string{key}, token).Result(); err != nil &&
		!errors.Is(err, redis.Nil) {
		s.recordRedisError(err)
	}
}

func (s *Store) readGeneration(ctx context.Context, viewerID uint64) (string, error) {
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	value, err := s.redis.Get(cacheCtx, s.keys.feedGenKey(viewerID)).Result()
	if errors.Is(err, redis.Nil) {
		return "0", nil
	}
	if err != nil {
		return "", err
	}
	return value, nil
}

func (s *Store) invalidateViewers(ctx context.Context, viewerIDs []uint64) error {
	if !s.available() {
		return errCacheUnavailable
	}
	if len(viewerIDs) == 0 {
		return nil
	}
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	_, err := s.redis.TxPipelined(cacheCtx, func(pipe redis.Pipeliner) error {
		seen := make(map[uint64]struct{}, len(viewerIDs))
		for _, viewerID := range viewerIDs {
			if viewerID == 0 {
				continue
			}
			if _, ok := seen[viewerID]; ok {
				continue
			}
			seen[viewerID] = struct{}{}
			pipe.Incr(cacheCtx, s.keys.feedGenKey(viewerID))
			pipe.Del(cacheCtx, s.keys.feedKey(viewerID))
			pipe.Del(cacheCtx, s.keys.feedMetaKey(viewerID))
		}
		return nil
	})
	if err != nil {
		s.recordRedisError(err)
		return err
	}
	s.recordRedisSuccess()
	return nil
}

// OnFriendshipChanged invalidates both viewers' materialized feeds. It is
// deliberately best-effort: the next read falls back to MySQL if the
// invalidation does not reach Redis.
func (s *Store) OnFriendshipChanged(ctx context.Context, userID, friendID uint64) {
	if !s.enabled() {
		return
	}
	cacheCtx, cancel := projectionContext(ctx)
	defer cancel()
	ctx = cacheCtx
	viewers := []uint64{userID, friendID}
	for _, viewerID := range viewers {
		s.markFeedDirty(viewerID)
	}
	if err := s.invalidateViewers(ctx, viewers); err != nil {
		return
	}
	for _, viewerID := range viewers {
		s.clearFeedDirty(viewerID)
	}
}

func (s *Store) createMoment(
	ctx context.Context,
	authorID uint64,
	body string,
	visibility store.MomentVisibility,
	now time.Time,
) (store.Moment, error) {
	moment, err := s.backend.CreateMoment(ctx, authorID, body, visibility, now)
	if err != nil {
		return store.Moment{}, err
	}
	cacheCtx, cancel := projectionContext(ctx)
	defer cancel()
	s.afterMomentCreated(cacheCtx, moment)
	return moment, nil
}

func (s *Store) afterMomentCreated(ctx context.Context, moment store.Moment) {
	if !s.enabled() {
		return
	}
	if moment.Visibility != store.MomentVisibilityFriends {
		s.addToOwnFeed(ctx, moment)
		return
	}
	friendIDs, err := s.backend.ListFriendIDs(ctx, moment.AuthorID)
	if err != nil {
		// Without the fanout list we cannot invalidate every affected viewer.
		// Fall back to MySQL for the rest of this process rather than risk a
		// feed silently missing the committed moment.
		s.feedAllDirty.Store(true)
		s.markDegraded()
		return
	}
	if err := s.fanoutMoment(ctx, moment, friendIDs); err != nil {
		viewers := append([]uint64{moment.AuthorID}, friendIDs...)
		for _, viewerID := range viewers {
			s.markFeedDirty(viewerID)
		}
		s.markDegraded()
		if invalidateErr := s.invalidateViewers(ctx, viewers); invalidateErr == nil {
			for _, viewerID := range viewers {
				s.clearFeedDirty(viewerID)
			}
		} else {
			s.recordRedisError(invalidateErr)
		}
		s.recordRedisError(err)
		return
	}
	s.recordRedisSuccess()
}

func (s *Store) addToOwnFeed(ctx context.Context, moment store.Moment) {
	if !s.enabled() {
		return
	}
	_, err := s.redis.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		return pipeFeedMoment(
			pipe,
			ctx,
			s.keys.feedKey(moment.AuthorID),
			s.keys.feedMetaKey(moment.AuthorID),
			moment.ID,
			s.options.FeedWarmLimit,
			s.options.FeedTTL,
		)
	})
	if err != nil {
		s.markFeedDirty(moment.AuthorID)
		if invalidateErr := s.invalidateViewers(ctx, []uint64{moment.AuthorID}); invalidateErr == nil {
			s.clearFeedDirty(moment.AuthorID)
		}
		s.recordRedisError(err)
		return
	}
	s.clearFeedDirty(moment.AuthorID)
	s.recordRedisSuccess()
}

func (s *Store) fanoutMoment(
	ctx context.Context,
	moment store.Moment,
	friendIDs []uint64,
) error {
	isPull, err := s.redis.SIsMember(
		ctx,
		s.keys.pullAuthorsKey(),
		formatID(moment.AuthorID),
	).Result()
	if err != nil {
		return err
	}
	pull := isPull || len(friendIDs) > s.options.PullThreshold
	_, err = s.redis.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		ownKey := s.keys.feedKey(moment.AuthorID)
		if err := pipeFeedMoment(
			pipe,
			ctx,
			ownKey,
			s.keys.feedMetaKey(moment.AuthorID),
			moment.ID,
			s.options.FeedWarmLimit,
			s.options.FeedTTL,
		); err != nil {
			return err
		}
		authorKey := s.keys.authorKey(moment.AuthorID)
		if err := pipeFeedMoment(
			pipe,
			ctx,
			authorKey,
			s.keys.authorMetaKey(moment.AuthorID),
			moment.ID,
			s.options.FeedWarmLimit,
			s.options.FeedTTL,
		); err != nil {
			return err
		}
		if pull {
			pipe.SAdd(ctx, s.keys.pullAuthorsKey(), formatID(moment.AuthorID))
			return nil
		}
		for _, friendID := range friendIDs {
			if friendID == moment.AuthorID {
				continue
			}
			if err := pipeFeedMoment(
				pipe,
				ctx,
				s.keys.feedKey(friendID),
				s.keys.feedMetaKey(friendID),
				moment.ID,
				s.options.FeedWarmLimit,
				s.options.FeedTTL,
			); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !pull {
		s.counters.pushRecipients.Add(uint64(len(friendIDs)))
	}
	return nil
}

func (s *Store) deleteMoment(
	ctx context.Context,
	momentID, authorID uint64,
	now time.Time,
) error {
	if err := s.backend.DeleteMoment(ctx, momentID, authorID, now); err != nil {
		return err
	}
	cacheCtx, cancel := projectionContext(ctx)
	defer cancel()
	s.deleteLikeCache(cacheCtx, momentID)
	s.removeMomentFromFeeds(cacheCtx, momentID, authorID)
	return nil
}

func (s *Store) removeMomentFromFeeds(ctx context.Context, momentID, authorID uint64) {
	if !s.available() {
		return
	}
	friendIDs, err := s.backend.ListFriendIDs(ctx, authorID)
	if err != nil {
		s.markDegraded()
		return
	}
	_, err = s.redis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRem(ctx, s.keys.authorKey(authorID), formatID(momentID))
		pipe.ZRem(ctx, s.keys.feedKey(authorID), formatID(momentID))
		for _, friendID := range friendIDs {
			if friendID == authorID {
				continue
			}
			pipe.ZRem(ctx, s.keys.feedKey(friendID), formatID(momentID))
		}
		return nil
	})
	if err != nil {
		s.recordRedisError(err)
		return
	}
	s.recordRedisSuccess()
}

func pipeFeedMoment(
	pipe redis.Pipeliner,
	ctx context.Context,
	key, metaKey string,
	momentID uint64,
	maxEntries int,
	ttl time.Duration,
) error {
	return pipe.Eval(
		ctx,
		feedPushScript,
		[]string{key, metaKey},
		formatID(momentID),
		strconv.Itoa(maxEntries),
		strconv.FormatInt(ttl.Milliseconds(), 10),
	).Err()
}

func hashFriendIDs(friendIDs []uint64) string {
	ids := append([]uint64(nil), friendIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	hash := fnv.New64a()
	for _, id := range ids {
		_, _ = hash.Write([]byte(strconv.FormatUint(id, 10)))
		_, _ = hash.Write([]byte{','})
	}
	return strconv.FormatUint(hash.Sum64(), 16)
}
