package momentcache

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/redis/go-redis/v9"
)

// likeApplyScript updates an already warmed cache after MySQL committed the
// like. The count argument is the authoritative MySQL count, so concurrent
// retries converge instead of drifting.
const likeApplyScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
if ARGV[1] == '1' then
  redis.call('SADD', KEYS[2], ARGV[2])
else
  redis.call('SREM', KEYS[2], ARGV[2])
end
redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
redis.call('PEXPIRE', KEYS[2], ARGV[4])
return 1
`

var likeApply = redis.NewScript(likeApplyScript)

type likeCacheRead struct {
	valid   bool
	count   uint64
	liked   bool
	countOK bool
	cardOK  bool
	likedOK bool
}

func (s *Store) getMoment(
	ctx context.Context,
	viewerID, momentID uint64,
) (store.Moment, error) {
	moment, err := s.backend.GetMoment(ctx, viewerID, momentID)
	if err != nil {
		return store.Moment{}, err
	}
	moments := []store.Moment{moment}
	s.enrichLikes(ctx, viewerID, moments)
	return moments[0], nil
}

func (s *Store) likeMoment(
	ctx context.Context,
	momentID, userID uint64,
	now time.Time,
) (uint64, error) {
	count, err := s.backend.LikeMoment(ctx, momentID, userID, now)
	if err != nil {
		return 0, err
	}
	cacheCtx, cancel := projectionContext(ctx)
	defer cancel()
	s.applyLikeChange(cacheCtx, momentID, userID, true, count)
	return count, nil
}

func (s *Store) unlikeMoment(
	ctx context.Context,
	momentID, userID uint64,
) (uint64, error) {
	count, err := s.backend.UnlikeMoment(ctx, momentID, userID)
	if err != nil {
		return 0, err
	}
	cacheCtx, cancel := projectionContext(ctx)
	defer cancel()
	s.applyLikeChange(cacheCtx, momentID, userID, false, count)
	return count, nil
}

func (s *Store) applyLikeChange(
	ctx context.Context,
	momentID, userID uint64,
	liked bool,
	count uint64,
) {
	if !s.enabled() {
		return
	}
	if !s.available() {
		s.markLikeDirty(momentID)
		return
	}
	s.likeMu.Lock()
	defer s.likeMu.Unlock()

	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	action := "0"
	if liked {
		action = "1"
	}
	_, err := likeApply.Run(
		cacheCtx,
		s.redis,
		[]string{s.keys.likeCountKey(momentID), s.keys.likeMembersKey(momentID)},
		action,
		strconv.FormatUint(userID, 10),
		strconv.FormatUint(count, 10),
		s.options.LikeTTL.Milliseconds(),
	).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		s.markLikeDirty(momentID)
		s.recordRedisError(err)
		return
	}
	s.clearLikeDirty(momentID)
	s.recordRedisSuccess()
}

// enrichLikes overrides the MySQL like fields with a warmed Redis snapshot.
// Cache misses are grouped into one MySQL snapshot query and installed as a
// complete Set/count pair. Any error leaves the authoritative MySQL values in
// place, so the caller never observes a partially warm cache.
func (s *Store) enrichLikes(ctx context.Context, viewerID uint64, moments []store.Moment) {
	if !s.available() || len(moments) == 0 {
		return
	}
	ids := make([]uint64, 0, len(moments))
	for _, moment := range moments {
		ids = append(ids, moment.ID)
	}
	reads, err := s.readLikeCaches(ctx, viewerID, ids)
	if err != nil {
		s.recordRedisError(err)
		return
	}

	missing := make([]int, 0)
	for index, read := range reads {
		if read.valid && !s.likeIsDirty(moments[index].ID) {
			moments[index].LikeCount = read.count
			moments[index].LikedByMe = read.liked
			s.counters.likeHits.Add(1)
		} else {
			missing = append(missing, index)
			s.counters.likeMisses.Add(1)
		}
	}
	if len(missing) == 0 {
		return
	}

	s.likeMu.Lock()
	defer s.likeMu.Unlock()

	// Re-check after taking the lock: another request may have warmed these.
	missingIDs := make([]uint64, 0, len(missing))
	for _, index := range missing {
		missingIDs = append(missingIDs, moments[index].ID)
	}
	recheck, err := s.readLikeCaches(ctx, viewerID, missingIDs)
	if err != nil {
		s.recordRedisError(err)
		return
	}
	stillMissing := make([]int, 0, len(missing))
	for offset, read := range recheck {
		index := missing[offset]
		if read.valid && !s.likeIsDirty(moments[index].ID) {
			moments[index].LikeCount = read.count
			moments[index].LikedByMe = read.liked
			s.counters.likeHits.Add(1)
		} else {
			stillMissing = append(stillMissing, index)
		}
	}
	if len(stillMissing) == 0 {
		return
	}

	snapshotIDs := make([]uint64, 0, len(stillMissing))
	for _, index := range stillMissing {
		snapshotIDs = append(snapshotIDs, moments[index].ID)
	}
	users, err := s.backend.ListMomentLikeUserIDs(ctx, snapshotIDs)
	if err != nil {
		return
	}
	installed := make([]int, 0, len(stillMissing))
	for _, index := range stillMissing {
		moment := moments[index]
		userIDs := users[moment.ID]
		if uint64(len(userIDs)) != moment.LikeCount {
			// A concurrent write or a genuinely inconsistent row: do not install
			// a set that claims to be complete.
			continue
		}
		installed = append(installed, index)
	}
	if len(installed) == 0 {
		return
	}
	if err := s.installLikeCaches(ctx, moments, installed, users); err != nil {
		for _, index := range installed {
			s.markLikeDirty(moments[index].ID)
		}
		s.recordRedisError(err)
		return
	}
	for _, index := range installed {
		moments[index].LikedByMe = containsID(users[moments[index].ID], viewerID)
		s.clearLikeDirty(moments[index].ID)
	}
	s.recordRedisSuccess()
}

func (s *Store) readLikeCaches(
	ctx context.Context,
	viewerID uint64,
	momentIDs []uint64,
) ([]likeCacheRead, error) {
	reads := make([]likeCacheRead, len(momentIDs))
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()

	pipe := s.redis.Pipeline()
	counts := make([]*redis.StringCmd, len(momentIDs))
	cards := make([]*redis.IntCmd, len(momentIDs))
	liked := make([]*redis.BoolCmd, len(momentIDs))
	for index, momentID := range momentIDs {
		counts[index] = pipe.Get(cacheCtx, s.keys.likeCountKey(momentID))
		cards[index] = pipe.SCard(cacheCtx, s.keys.likeMembersKey(momentID))
		liked[index] = pipe.SIsMember(cacheCtx, s.keys.likeMembersKey(momentID), formatID(viewerID))
	}
	if _, err := pipe.Exec(cacheCtx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	for index := range momentIDs {
		read := &reads[index]
		read.countOK = counts[index].Err() == nil
		read.cardOK = cards[index].Err() == nil
		read.likedOK = liked[index].Err() == nil
		if !read.countOK || !read.cardOK || !read.likedOK {
			continue
		}
		count, err := strconv.ParseUint(counts[index].Val(), 10, 64)
		if err != nil {
			continue
		}
		card := cards[index].Val()
		if card < 0 || uint64(card) != count {
			continue
		}
		read.valid = true
		read.count = count
		read.liked = liked[index].Val()
	}
	return reads, nil
}

func (s *Store) installLikeCaches(
	ctx context.Context,
	moments []store.Moment,
	indexes []int,
	users map[uint64][]uint64,
) error {
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	ttl := s.options.LikeTTL
	_, err := s.redis.TxPipelined(cacheCtx, func(pipe redis.Pipeliner) error {
		for _, index := range indexes {
			moment := moments[index]
			members := s.keys.likeMembersKey(moment.ID)
			countKey := s.keys.likeCountKey(moment.ID)
			pipe.Del(cacheCtx, members)
			userIDs := users[moment.ID]
			if len(userIDs) > 0 {
				values := make([]any, 0, len(userIDs))
				for _, userID := range userIDs {
					values = append(values, userID)
				}
				pipe.SAdd(cacheCtx, members, values...)
				pipe.Expire(cacheCtx, members, ttl)
			}
			pipe.Set(cacheCtx, countKey, strconv.FormatUint(moment.LikeCount, 10), ttl)
		}
		return nil
	})
	return err
}

func (s *Store) deleteLikeCache(ctx context.Context, momentID uint64) {
	if !s.available() {
		return
	}
	s.likeMu.Lock()
	defer s.likeMu.Unlock()
	cacheCtx, cancel := s.cacheContext(ctx)
	defer cancel()
	_, err := s.redis.TxPipelined(cacheCtx, func(pipe redis.Pipeliner) error {
		pipe.Del(cacheCtx, s.keys.likeMembersKey(momentID), s.keys.likeCountKey(momentID))
		pipe.Incr(cacheCtx, s.keys.likeEpochKey(momentID))
		pipe.Expire(cacheCtx, s.keys.likeEpochKey(momentID), s.options.LikeTTL)
		return nil
	})
	if err != nil {
		s.markLikeDirty(momentID)
		s.recordRedisError(err)
		return
	}
	s.clearLikeDirty(momentID)
	s.recordRedisSuccess()
}

func containsID(ids []uint64, target uint64) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}
