package momentcache

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/httpapi"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeMomentBackend struct {
	httpapi.MomentStore

	mu        sync.Mutex
	nextID    uint64
	friendErr error
	friends   map[uint64][]uint64
	users     map[uint64]store.PublicUser
	moments   []store.Moment
	deleted   map[uint64]bool
	likes     map[uint64]map[uint64]bool
}

func newFakeMomentBackend() *fakeMomentBackend {
	return &fakeMomentBackend{
		friends: make(map[uint64][]uint64),
		users:   make(map[uint64]store.PublicUser),
		deleted: make(map[uint64]bool),
		likes:   make(map[uint64]map[uint64]bool),
	}
}

func (f *fakeMomentBackend) addUser(id uint64, username, nickname string) {
	f.users[id] = store.PublicUser{Username: username, Nickname: nickname}
}

func (f *fakeMomentBackend) setFriends(userID uint64, friendIDs ...uint64) {
	f.friends[userID] = append([]uint64(nil), friendIDs...)
}

func (f *fakeMomentBackend) ListFriendIDs(
	_ context.Context,
	userID uint64,
) ([]uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.friendErr != nil {
		return nil, f.friendErr
	}
	return append([]uint64(nil), f.friends[userID]...), nil
}

func (f *fakeMomentBackend) CreateMoment(
	_ context.Context,
	authorID uint64,
	body string,
	visibility store.MomentVisibility,
	now time.Time,
) (store.Moment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	moment := store.Moment{
		ID:         f.nextID,
		AuthorID:   authorID,
		Author:     f.users[authorID],
		Body:       body,
		Visibility: visibility,
		CreatedAt:  now,
		Mine:       true,
	}
	f.moments = append(f.moments, moment)
	f.deleted[moment.ID] = false
	return moment, nil
}

func (f *fakeMomentBackend) ListMoments(
	_ context.Context,
	viewerID uint64,
	friendIDs []uint64,
	beforeID uint64,
	limit int,
) ([]store.Moment, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	friendSet := make(map[uint64]struct{}, len(friendIDs))
	for _, id := range friendIDs {
		friendSet[id] = struct{}{}
	}
	items := make([]store.Moment, 0)
	for _, moment := range f.moments {
		if f.deleted[moment.ID] || (beforeID > 0 && moment.ID >= beforeID) {
			continue
		}
		if !f.visibleLocked(viewerID, friendSet, moment) {
			continue
		}
		copy := moment
		copy.Mine = copy.AuthorID == viewerID
		copy.LikedByMe = f.likes[moment.ID][viewerID]
		items = append(items, copy)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID > items[j].ID })
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	return items, hasMore, nil
}

func (f *fakeMomentBackend) GetMoment(
	_ context.Context,
	viewerID, momentID uint64,
) (store.Moment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, moment := range f.moments {
		if moment.ID != momentID || f.deleted[momentID] {
			continue
		}
		moment.Mine = moment.AuthorID == viewerID
		moment.LikedByMe = f.likes[momentID][viewerID]
		return moment, nil
	}
	return store.Moment{}, store.ErrMomentNotFound
}

func (f *fakeMomentBackend) ListVisibleMomentsByIDs(
	_ context.Context,
	viewerID uint64,
	friendIDs []uint64,
	momentIDs []uint64,
) ([]store.Moment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	friendSet := make(map[uint64]struct{}, len(friendIDs))
	for _, id := range friendIDs {
		friendSet[id] = struct{}{}
	}
	wanted := make(map[uint64]struct{}, len(momentIDs))
	for _, id := range momentIDs {
		wanted[id] = struct{}{}
	}
	items := make([]store.Moment, 0, len(momentIDs))
	for _, moment := range f.moments {
		if _, ok := wanted[moment.ID]; !ok || f.deleted[moment.ID] {
			continue
		}
		if !f.visibleLocked(viewerID, friendSet, moment) {
			continue
		}
		copy := moment
		copy.Mine = copy.AuthorID == viewerID
		copy.LikedByMe = f.likes[moment.ID][viewerID]
		items = append(items, copy)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID > items[j].ID })
	return items, nil
}

func (f *fakeMomentBackend) ListAuthorMomentIDs(
	_ context.Context,
	authorID, beforeID uint64,
	limit int,
) ([]uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]uint64, 0)
	for _, moment := range f.moments {
		if moment.AuthorID != authorID ||
			f.deleted[moment.ID] ||
			moment.Visibility != store.MomentVisibilityFriends ||
			(beforeID > 0 && moment.ID >= beforeID) {
			continue
		}
		ids = append(ids, moment.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	hasMore := len(ids) > limit
	if hasMore {
		ids = ids[:limit]
	}
	return ids, hasMore, nil
}

func (f *fakeMomentBackend) ListMomentLikeUserIDs(
	_ context.Context,
	momentIDs []uint64,
) (map[uint64][]uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make(map[uint64][]uint64, len(momentIDs))
	for _, momentID := range momentIDs {
		for userID := range f.likes[momentID] {
			result[momentID] = append(result[momentID], userID)
		}
		sort.Slice(result[momentID], func(i, j int) bool {
			return result[momentID][i] < result[momentID][j]
		})
	}
	return result, nil
}

func (f *fakeMomentBackend) LikeMoment(
	_ context.Context,
	momentID, userID uint64,
	_ time.Time,
) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	moment := f.findLocked(momentID)
	if moment == nil {
		return 0, store.ErrMomentNotFound
	}
	if f.likes[momentID] == nil {
		f.likes[momentID] = make(map[uint64]bool)
	}
	if !f.likes[momentID][userID] {
		f.likes[momentID][userID] = true
		moment.LikeCount++
		f.updateLocked(*moment)
	}
	return moment.LikeCount, nil
}

func (f *fakeMomentBackend) UnlikeMoment(
	_ context.Context,
	momentID, userID uint64,
) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	moment := f.findLocked(momentID)
	if moment == nil {
		return 0, store.ErrMomentNotFound
	}
	if f.likes[momentID] != nil && f.likes[momentID][userID] {
		delete(f.likes[momentID], userID)
		if moment.LikeCount > 0 {
			moment.LikeCount--
		}
		f.updateLocked(*moment)
	}
	return moment.LikeCount, nil
}

func (f *fakeMomentBackend) DeleteMoment(
	_ context.Context,
	momentID, authorID uint64,
	_ time.Time,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	moment := f.findLocked(momentID)
	if moment == nil {
		return store.ErrMomentNotFound
	}
	if moment.AuthorID != authorID {
		return store.ErrMomentForbidden
	}
	f.deleted[momentID] = true
	return nil
}

func (f *fakeMomentBackend) visibleLocked(
	viewerID uint64,
	friendSet map[uint64]struct{},
	moment store.Moment,
) bool {
	if f.deleted[moment.ID] {
		return false
	}
	if moment.AuthorID == viewerID {
		return true
	}
	if moment.Visibility != store.MomentVisibilityFriends {
		return false
	}
	_, ok := friendSet[moment.AuthorID]
	return ok
}

func (f *fakeMomentBackend) findLocked(momentID uint64) *store.Moment {
	for index := range f.moments {
		if f.moments[index].ID == momentID && !f.deleted[momentID] {
			copy := f.moments[index]
			return &copy
		}
	}
	return nil
}

func (f *fakeMomentBackend) updateLocked(moment store.Moment) {
	for index := range f.moments {
		if f.moments[index].ID == moment.ID {
			f.moments[index] = moment
			return
		}
	}
}

func newMomentCacheTest(
	t *testing.T,
	options Options,
) (*Store, *fakeMomentBackend, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	backend := newFakeMomentBackend()
	backend.addUser(1, "alice", "Alice")
	backend.addUser(2, "bob", "Bob")
	backend.addUser(3, "carol", "Carol")
	options.Enabled = true
	if options.OperationTimeout == 0 {
		options.OperationTimeout = time.Second
	}
	cache := New(backend, client, NewKeyspace("im:test:"), options)
	return cache, backend, server, client
}

func TestKeyspaceIsolatesMomentKeys(t *testing.T) {
	first := NewKeyspace("im:v12")
	second := NewKeyspace("im:v13-verify")
	if got, want := first.feedKey(7), "im:v12:moments:feed:7"; got != want {
		t.Fatalf("feed key = %q, want %q", got, want)
	}
	if got, want := second.authorKey(7), "im:v13-verify:moments:author:7"; got != want {
		t.Fatalf("author key = %q, want %q", got, want)
	}
	if first.likeCountKey(7) == second.likeCountKey(7) {
		t.Fatal("keyspaces share a moments key")
	}
}

func TestFeedPushAndPrivateVisibility(t *testing.T) {
	cache, backend, _, client := newMomentCacheTest(t, Options{FeedWarmLimit: 100})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)

	moment, err := cache.CreateMoment(
		context.Background(),
		1,
		"hello",
		store.MomentVisibilityFriends,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create friends moment: %v", err)
	}
	if score, err := client.ZScore(
		context.Background(),
		cache.keys.feedKey(2),
		formatID(moment.ID),
	).Result(); err != nil || score != float64(moment.ID) {
		t.Fatalf("friend feed score = %v, err = %v", score, err)
	}

	page, hasMore, err := cache.ListMoments(
		context.Background(),
		2,
		[]uint64{1},
		0,
		20,
	)
	if err != nil {
		t.Fatalf("list feed: %v", err)
	}
	if hasMore || len(page) != 1 || page[0].ID != moment.ID || page[0].Mine {
		t.Fatalf("feed page = %#v, hasMore = %v", page, hasMore)
	}

	private, err := cache.CreateMoment(
		context.Background(),
		1,
		"secret",
		store.MomentVisibilityPrivate,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create private moment: %v", err)
	}
	if _, err := client.ZScore(
		context.Background(),
		cache.keys.authorKey(1),
		formatID(private.ID),
	).Result(); err == nil {
		t.Fatal("private moment entered the public author timeline")
	}
	if _, err := client.ZScore(
		context.Background(),
		cache.keys.feedKey(2),
		formatID(private.ID),
	).Result(); err == nil {
		t.Fatal("private moment entered a friend's feed")
	}
	page, _, err = cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil {
		t.Fatalf("list feed after private: %v", err)
	}
	for _, item := range page {
		if item.ID == private.ID {
			t.Fatal("friend saw a private moment")
		}
	}
}

func TestHighFanoutUsesPullTimeline(t *testing.T) {
	cache, backend, _, client := newMomentCacheTest(t, Options{
		FeedWarmLimit: 100,
		PullThreshold: 1,
	})
	backend.setFriends(1, 2, 3)
	backend.setFriends(2, 1)

	if _, _, err := cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20); err != nil {
		t.Fatalf("warm empty feed: %v", err)
	}

	moment, err := cache.CreateMoment(
		context.Background(),
		1,
		"many friends",
		store.MomentVisibilityFriends,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create pull moment: %v", err)
	}
	if _, err := client.ZScore(
		context.Background(),
		cache.keys.feedKey(2),
		formatID(moment.ID),
	).Result(); err == nil {
		t.Fatal("high fanout moment was pushed into a friend feed")
	}
	if member, err := client.SIsMember(
		context.Background(),
		cache.keys.pullAuthorsKey(),
		formatID(1),
	).Result(); err != nil || !member {
		t.Fatalf("pull marker = %v, err = %v", member, err)
	}

	page, _, err := cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil {
		t.Fatalf("pull feed: %v", err)
	}
	if len(page) != 1 || page[0].ID != moment.ID {
		t.Fatalf("pull page = %#v, want moment %d", page, moment.ID)
	}
	if cache.Status().PullSources == 0 {
		t.Fatal("pull source metric was not recorded")
	}
}

func TestFeedPaginationAndWarmFloorFallback(t *testing.T) {
	cache, backend, _, _ := newMomentCacheTest(t, Options{FeedWarmLimit: 5})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	for index := 0; index < 10; index++ {
		if _, err := cache.CreateMoment(
			context.Background(),
			1,
			"moment",
			store.MomentVisibilityFriends,
			time.Now().UTC(),
		); err != nil {
			t.Fatalf("create moment %d: %v", index, err)
		}
	}

	page, hasMore, err := cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil {
		t.Fatalf("list feed: %v", err)
	}
	if hasMore || len(page) != 10 {
		t.Fatalf("deep feed page len=%d hasMore=%v, want 10 false", len(page), hasMore)
	}
	if cache.Status().FeedFallbacks == 0 {
		t.Fatal("a page beyond the warm floor did not fall back to MySQL")
	}

	cache, backend, _, _ = newMomentCacheTest(t, Options{FeedWarmLimit: 100})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	for index := 0; index < 25; index++ {
		if _, err := cache.CreateMoment(
			context.Background(),
			1,
			"page",
			store.MomentVisibilityFriends,
			time.Now().UTC(),
		); err != nil {
			t.Fatalf("create page moment %d: %v", index, err)
		}
	}
	first, hasMore, err := cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if !hasMore || len(first) != 20 {
		t.Fatalf("first page len=%d hasMore=%v, want 20 true", len(first), hasMore)
	}
	second, hasMore, err := cache.ListMoments(
		context.Background(),
		2,
		[]uint64{1},
		first[len(first)-1].ID,
		20,
	)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if hasMore || len(second) != 5 {
		t.Fatalf("second page len=%d hasMore=%v, want 5 false", len(second), hasMore)
	}
	seen := make(map[uint64]struct{}, len(first))
	for _, moment := range first {
		seen[moment.ID] = struct{}{}
	}
	for _, moment := range second {
		if _, ok := seen[moment.ID]; ok {
			t.Fatalf("moment %d appeared on both pages", moment.ID)
		}
	}
}

func TestPushTrimKeepsFeedBounded(t *testing.T) {
	cache, backend, _, client := newMomentCacheTest(t, Options{FeedWarmLimit: 3})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	for index := 0; index < 10; index++ {
		if _, err := cache.CreateMoment(
			context.Background(),
			1,
			"bounded",
			store.MomentVisibilityFriends,
			time.Now().UTC(),
		); err != nil {
			t.Fatalf("create moment %d: %v", index, err)
		}
	}
	if card, err := client.ZCard(
		context.Background(),
		cache.keys.feedKey(2),
	).Result(); err != nil || card > 3 {
		t.Fatalf("friend feed card = %d, err = %v, want <= 3", card, err)
	}
	page, _, err := cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil || len(page) != 10 {
		t.Fatalf("bounded feed page len = %d, err = %v, want 10 via MySQL fallback", len(page), err)
	}
}

func TestMissingFeedKeyRewarmsFromMySQL(t *testing.T) {
	cache, backend, _, client := newMomentCacheTest(t, Options{FeedWarmLimit: 100})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	moment, err := cache.CreateMoment(
		context.Background(),
		1,
		"evicted feed",
		store.MomentVisibilityFriends,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create moment: %v", err)
	}
	if _, _, err := cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20); err != nil {
		t.Fatalf("warm feed: %v", err)
	}
	if err := client.Del(context.Background(), cache.keys.feedKey(2)).Err(); err != nil {
		t.Fatalf("evict feed key: %v", err)
	}
	page, _, err := cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil || len(page) != 1 || page[0].ID != moment.ID {
		t.Fatalf("rewarmed page = %#v, err = %v", page, err)
	}
}

func TestFriendshipChangeInvalidatesWarmFeed(t *testing.T) {
	cache, backend, _, _ := newMomentCacheTest(t, Options{FeedWarmLimit: 100})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	moment, err := cache.CreateMoment(
		context.Background(),
		1,
		"friends only",
		store.MomentVisibilityFriends,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create moment: %v", err)
	}
	page, _, err := cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil || len(page) != 1 || page[0].ID != moment.ID {
		t.Fatalf("initial feed = %#v, err = %v", page, err)
	}

	backend.setFriends(1)
	backend.setFriends(2)
	cache.OnFriendshipChanged(context.Background(), 1, 2)
	page, _, err = cache.ListMoments(context.Background(), 2, nil, 0, 20)
	if err != nil {
		t.Fatalf("list after unfriend: %v", err)
	}
	if len(page) != 0 {
		t.Fatalf("feed after unfriend = %#v, want empty", page)
	}
}

func TestLikeCacheWarmHitWriteAndFallback(t *testing.T) {
	cache, backend, server, client := newMomentCacheTest(t, Options{FeedWarmLimit: 100})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	moment, err := cache.CreateMoment(
		context.Background(),
		1,
		"like me",
		store.MomentVisibilityFriends,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create moment: %v", err)
	}

	got, err := cache.GetMoment(context.Background(), 2, moment.ID)
	if err != nil {
		t.Fatalf("get moment: %v", err)
	}
	if got.LikeCount != 0 || got.LikedByMe {
		t.Fatalf("initial like state = %d/%v", got.LikeCount, got.LikedByMe)
	}
	if _, err := client.Get(
		context.Background(),
		cache.keys.likeCountKey(moment.ID),
	).Result(); err != nil {
		t.Fatalf("like cache was not warmed: %v", err)
	}

	count, err := cache.LikeMoment(context.Background(), moment.ID, 2, time.Now().UTC())
	if err != nil || count != 1 {
		t.Fatalf("like = %d, err = %v", count, err)
	}
	if got, err := client.SCard(
		context.Background(),
		cache.keys.likeMembersKey(moment.ID),
	).Result(); err != nil || got != 1 {
		t.Fatalf("like set size = %d, err = %v", got, err)
	}
	got, err = cache.GetMoment(context.Background(), 2, moment.ID)
	if err != nil || got.LikeCount != 1 || !got.LikedByMe {
		t.Fatalf("cached like state = %#v, err = %v", got, err)
	}
	if cache.Status().LikeHits == 0 {
		t.Fatal("like cache did not record a hit")
	}

	count, err = cache.UnlikeMoment(context.Background(), moment.ID, 2)
	if err != nil || count != 0 {
		t.Fatalf("unlike = %d, err = %v", count, err)
	}
	got, err = cache.GetMoment(context.Background(), 2, moment.ID)
	if err != nil || got.LikeCount != 0 || got.LikedByMe {
		t.Fatalf("unliked state = %#v, err = %v", got, err)
	}

	if err := client.Del(
		context.Background(),
		cache.keys.likeMembersKey(moment.ID),
		cache.keys.likeCountKey(moment.ID),
	).Err(); err != nil {
		t.Fatalf("clear like cache: %v", err)
	}
	got, err = cache.GetMoment(context.Background(), 2, moment.ID)
	if err != nil || got.LikeCount != 0 || got.LikedByMe {
		t.Fatalf("re-warmed state = %#v, err = %v", got, err)
	}

	server.Close()
	if _, err := cache.GetMoment(context.Background(), 2, moment.ID); err != nil {
		t.Fatalf("MySQL fallback after Redis close: %v", err)
	}
}

func TestLikeCacheCardinalityMismatchRewarms(t *testing.T) {
	cache, backend, _, client := newMomentCacheTest(t, Options{FeedWarmLimit: 100})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	moment, err := cache.CreateMoment(
		context.Background(),
		1,
		"repair count",
		store.MomentVisibilityFriends,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create moment: %v", err)
	}
	if _, err := cache.GetMoment(context.Background(), 2, moment.ID); err != nil {
		t.Fatalf("warm like cache: %v", err)
	}
	if err := client.Set(
		context.Background(),
		cache.keys.likeCountKey(moment.ID),
		"5",
		time.Hour,
	).Err(); err != nil {
		t.Fatalf("corrupt like count: %v", err)
	}
	got, err := cache.GetMoment(context.Background(), 2, moment.ID)
	if err != nil || got.LikeCount != 0 || got.LikedByMe {
		t.Fatalf("repaired state = %#v, err = %v", got, err)
	}
	if count, err := client.Get(
		context.Background(),
		cache.keys.likeCountKey(moment.ID),
	).Result(); err != nil || count != "0" {
		t.Fatalf("repaired count = %q, err = %v", count, err)
	}
}

func TestLikeSnapshotMismatchIsNotInstalled(t *testing.T) {
	cache, backend, _, client := newMomentCacheTest(t, Options{FeedWarmLimit: 100})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	moment, err := cache.CreateMoment(
		context.Background(),
		1,
		"inconsistent count",
		store.MomentVisibilityFriends,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create moment: %v", err)
	}
	backend.likes[moment.ID] = map[uint64]bool{2: true}
	for index := range backend.moments {
		if backend.moments[index].ID == moment.ID {
			backend.moments[index].LikeCount = 2
		}
	}

	got, err := cache.GetMoment(context.Background(), 2, moment.ID)
	if err != nil {
		t.Fatalf("get moment: %v", err)
	}
	if got.LikeCount != 2 || !got.LikedByMe {
		t.Fatalf("MySQL fallback state = %#v", got)
	}
	if _, err := client.Get(
		context.Background(),
		cache.keys.likeCountKey(moment.ID),
	).Result(); err == nil {
		t.Fatal("mismatched like snapshot was installed")
	}
}

func TestFriendAddInvalidatesWarmFeed(t *testing.T) {
	cache, backend, _, _ := newMomentCacheTest(t, Options{FeedWarmLimit: 100})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	moment, err := cache.CreateMoment(
		context.Background(),
		1,
		"history",
		store.MomentVisibilityFriends,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create moment: %v", err)
	}
	backend.setFriends(1)
	backend.setFriends(2)
	page, _, err := cache.ListMoments(context.Background(), 2, nil, 0, 20)
	if err != nil {
		t.Fatalf("warm without friendship: %v", err)
	}
	if len(page) != 0 {
		t.Fatalf("feed before friendship = %#v, want empty", page)
	}
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	cache.OnFriendshipChanged(context.Background(), 1, 2)
	page, _, err = cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil {
		t.Fatalf("feed after friendship: %v", err)
	}
	if len(page) != 1 || page[0].ID != moment.ID {
		t.Fatalf("feed after friendship = %#v, want moment %d", page, moment.ID)
	}
}

func TestFeedRedisFailureFallsBackToMySQL(t *testing.T) {
	cache, backend, server, _ := newMomentCacheTest(t, Options{FeedWarmLimit: 100})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	moment, err := cache.CreateMoment(
		context.Background(),
		1,
		"survives redis",
		store.MomentVisibilityFriends,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create moment: %v", err)
	}
	server.Close()
	page, _, err := cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil {
		t.Fatalf("fallback feed: %v", err)
	}
	if len(page) != 1 || page[0].ID != moment.ID {
		t.Fatalf("fallback page = %#v, want moment %d", page, moment.ID)
	}
}

func TestFanoutListFailureFallsBackUntilRestart(t *testing.T) {
	cache, backend, _, _ := newMomentCacheTest(t, Options{FeedWarmLimit: 100})
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	backend.friendErr = context.DeadlineExceeded
	if _, err := cache.CreateMoment(
		context.Background(),
		1,
		"fanout unknown",
		store.MomentVisibilityFriends,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("create moment: %v", err)
	}
	page, _, err := cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil || len(page) != 1 {
		t.Fatalf("fallback feed = %#v, err = %v", page, err)
	}
	if !cache.Status().Degraded {
		t.Fatal("unknown fanout failure did not mark the cache degraded")
	}
}

func TestDisabledCacheIsPassThrough(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	backend := newFakeMomentBackend()
	backend.addUser(1, "alice", "Alice")
	backend.setFriends(1, 2)
	backend.setFriends(2, 1)
	cache := New(backend, client, NewKeyspace("im:disabled:"), Options{Enabled: false})
	moment, err := cache.CreateMoment(
		context.Background(), 1, "plain", store.MomentVisibilityFriends, time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create moment: %v", err)
	}
	page, _, err := cache.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil || len(page) != 1 || page[0].ID != moment.ID {
		t.Fatalf("pass-through page = %#v, err = %v", page, err)
	}
	if keys, err := client.Keys(context.Background(), "im:disabled:*").Result(); err != nil || len(keys) != 0 {
		t.Fatalf("disabled cache wrote keys = %v, err = %v", keys, err)
	}
	if cache.Status().Enabled {
		t.Fatal("disabled cache reports enabled")
	}
}

func TestKeyspacesDoNotShareData(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	first := newFakeMomentBackend()
	first.addUser(1, "alice", "Alice")
	first.setFriends(1, 2)
	first.setFriends(2, 1)
	second := newFakeMomentBackend()
	second.addUser(1, "alice", "Alice")
	second.setFriends(1, 2)
	second.setFriends(2, 1)
	cacheA := New(first, client, NewKeyspace("im:a:"), Options{Enabled: true})
	cacheB := New(second, client, NewKeyspace("im:b:"), Options{Enabled: true})
	if _, err := cacheA.CreateMoment(
		context.Background(), 1, "a", store.MomentVisibilityFriends, time.Now().UTC(),
	); err != nil {
		t.Fatalf("create in A: %v", err)
	}
	page, _, err := cacheB.ListMoments(context.Background(), 2, []uint64{1}, 0, 20)
	if err != nil {
		t.Fatalf("list in B: %v", err)
	}
	if len(page) != 0 {
		t.Fatalf("namespace B saw A's moments: %#v", page)
	}
}
