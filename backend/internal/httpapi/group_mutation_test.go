package httpapi

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func inviteTestStore(t *testing.T) *fakeStore {
	t.Helper()
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(time.Now().UTC())
	return dataStore
}

func fastGroupRepair(t *testing.T) {
	t.Helper()
	base, max := groupRepairBaseDelay, groupRepairMaxDelay
	groupRepairBaseDelay, groupRepairMaxDelay = 5*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() {
		groupRepairBaseDelay, groupRepairMaxDelay = base, max
	})
}

func TestInviteReadsWatermarkAfterMutation(t *testing.T) {
	dataStore := inviteTestStore(t)
	dataStore.groupMemberIDSequence = [][]uint64{{1, 2}, {1, 2, 3}}
	projector := &fakeGroupProjector{watermark: 55}
	router := testRouter(
		dataStore,
		WithGroupReadTracker(newFakeGroupReadTracker()),
		WithGroupProjector(projector),
		WithGroupAcceptedWatermarkReader(projector),
	)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/members", map[string]any{
		"usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if projector.watermarkBeforeBegin {
		t.Fatal("accepted watermark was read before the mutation blocked new sends")
	}
}

func TestInviteGetGroupFailureStillProjects(t *testing.T) {
	dataStore := inviteTestStore(t)
	dataStore.groupMemberIDSequence = [][]uint64{{1, 2}, {1, 2, 3}}
	dataStore.groupDetailErr = store.ErrGroupNotFound
	projector := &fakeGroupProjector{watermark: 55}
	router := testRouter(
		dataStore,
		WithGroupReadTracker(newFakeGroupReadTracker()),
		WithGroupProjector(projector),
		WithGroupAcceptedWatermarkReader(projector),
	)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/members", map[string]any{
		"usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if projector.replaces != 1 {
		t.Fatalf("projector = %#v, want the committed invite projected", projector)
	}
}

func TestKickGetGroupFailureStillProjects(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetailErr = store.ErrGroupNotFound
	dataStore.groupMemberIDs = []uint64{1}
	projector := &fakeGroupProjector{}
	router := testRouter(
		dataStore,
		WithGroupReadTracker(newFakeGroupReadTracker()),
		WithGroupProjector(projector),
	)

	recorder := doJSON(
		t, router, http.MethodDelete, "/api/groups/7/members/bob", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if projector.replaces != 1 {
		t.Fatalf("projector = %#v, want the committed kick projected", projector)
	}
}

func TestLeaveReadFailureStillProjects(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.groupMemberIDs = []uint64{1, 2}
	tracker := newFakeGroupReadTracker()
	tracker.removeErr = messaging.ErrMessageServiceUnavailable
	projector := &fakeGroupProjector{}
	router := testRouter(
		dataStore,
		WithGroupReadTracker(tracker),
		WithGroupProjector(projector),
	)

	recorder := doJSON(
		t, router, http.MethodPost, "/api/groups/7/leave", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if projector.replaces != 1 {
		t.Fatalf("projector = %#v, want the committed leave projected", projector)
	}
}

func TestGroupRepairSeedsMissingReadPositions(t *testing.T) {
	fastGroupRepair(t)
	dataStore := inviteTestStore(t)
	dataStore.groupMemberIDSequence = [][]uint64{{1, 2}, {1, 2, 3}}
	dataStore.inviteGroupBaseline = 60
	dataStore.groupProjections = []store.GroupMembershipProjection{{
		GroupID:   7,
		MemberIDs: []uint64{1, 2, 3},
		Positions: map[uint64]uint64{1: 0, 2: 0, 3: 60},
	}}
	tracker := newFakeGroupReadTracker()
	tracker.setSeedErr(messaging.ErrMessageServiceUnavailable)
	projector := &fakeGroupProjector{watermark: 55}
	api, router, _ := startChatServer(
		t,
		dataStore,
		WithGroupReadTracker(tracker),
		WithGroupProjector(projector),
		WithGroupAcceptedWatermarkReader(projector),
	)
	api.Start()
	t.Cleanup(api.Stop)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/members", map[string]any{
		"usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	tracker.setSeedErr(nil)

	waitFor(t, func() bool {
		for _, seed := range tracker.snapSeeds() {
			if seed.groupID == 7 && seed.userID == 3 && seed.position == 60 {
				return true
			}
		}
		return false
	})
}

func TestGroupRepairRetriesFailedProjection(t *testing.T) {
	fastGroupRepair(t)
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(time.Now().UTC())
	dataStore.groupMemberIDs = []uint64{1}
	dataStore.groupProjections = []store.GroupMembershipProjection{{
		GroupID:   7,
		MemberIDs: []uint64{1},
		Positions: map[uint64]uint64{1: 0},
	}}
	var failures atomic.Int32
	failures.Store(1)
	projector := &fakeGroupProjector{}
	projector.replaceFn = func() error {
		if failures.Add(-1) >= 0 {
			return errors.New("redis unavailable")
		}
		return nil
	}
	api, router, _ := startChatServer(
		t,
		dataStore,
		WithGroupReadTracker(newFakeGroupReadTracker()),
		WithGroupProjector(projector),
	)
	api.Start()
	t.Cleanup(api.Stop)

	recorder := doJSON(
		t, router, http.MethodDelete, "/api/groups/7/members/bob", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	waitFor(t, func() bool {
		projector.mu.Lock()
		defer projector.mu.Unlock()
		return projector.replaceSucceeded >= 1
	})
	projector.mu.Lock()
	defer projector.mu.Unlock()
	if projector.lastGroupID != 7 || len(projector.lastMembers) != 1 {
		t.Fatalf("projector = %#v", projector)
	}
}

func TestCreateGroupReplaceFailureIsRepaired(t *testing.T) {
	fastGroupRepair(t)
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(time.Now().UTC())
	dataStore.groupMemberIDs = []uint64{1, 2}
	dataStore.groupProjections = []store.GroupMembershipProjection{{
		GroupID:   7,
		MemberIDs: []uint64{1, 2},
		Positions: map[uint64]uint64{1: 0, 2: 0},
	}}
	var failures atomic.Int32
	failures.Store(1)
	projector := &fakeGroupProjector{}
	projector.replaceFn = func() error {
		if failures.Add(-1) >= 0 {
			return errors.New("redis unavailable")
		}
		return nil
	}
	api, router, _ := startChatServer(
		t,
		dataStore,
		WithGroupReadTracker(newFakeGroupReadTracker()),
		WithGroupProjector(projector),
	)
	api.Start()
	t.Cleanup(api.Stop)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups", map[string]any{
		"name":             "Team",
		"member_usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	waitFor(t, func() bool {
		projector.mu.Lock()
		defer projector.mu.Unlock()
		return projector.replaceSucceeded >= 1
	})
	projector.mu.Lock()
	defer projector.mu.Unlock()
	if projector.lastGroupID != 7 || len(projector.lastMembers) != 2 {
		t.Fatalf("projector = %#v", projector)
	}
}

type flakyGroupProjector struct {
	inner        *messaging.GroupProjection
	failReplaces atomic.Int32
	gate         chan struct{}
	started      chan struct{}
}

func (p *flakyGroupProjector) Replace(
	ctx context.Context,
	groupID uint64,
	memberIDs []uint64,
	dissolved bool,
) error {
	if p.failReplaces.Add(-1) >= 0 {
		return errors.New("redis blip")
	}
	if p.gate != nil {
		close(p.started)
		<-p.gate
	}
	return p.inner.Replace(ctx, groupID, memberIDs, dissolved)
}

func (p *flakyGroupProjector) BeginMutation(ctx context.Context, groupID uint64) error {
	return p.inner.BeginMutation(ctx, groupID)
}

func (p *flakyGroupProjector) EndMutation(ctx context.Context, groupID uint64) error {
	return p.inner.EndMutation(ctx, groupID)
}

func TestGroupRepairClearsSyncRequiredOnRealRedis(t *testing.T) {
	fastGroupRepair(t)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	prefix := messaging.DefaultNamespace().RedisPrefix()
	membersKey := prefix + "group:7:members"
	syncKey := prefix + "group:7:sync_required"
	if err := client.SAdd(ctx, membersKey, "1", "2").Err(); err != nil {
		t.Fatalf("seed members: %v", err)
	}
	if err := client.ZAdd(ctx, prefix+"group:7:inbox",
		redis.Z{Score: 10, Member: "10"},
	).Err(); err != nil {
		t.Fatalf("seed inbox: %v", err)
	}
	keyspace := messaging.NewKeyspace(messaging.DefaultNamespace())
	tracker := messaging.NewGroupReadTracker(
		client, messaging.GroupReadTrackerOptions{Keyspace: keyspace},
	)
	projector := &flakyGroupProjector{
		inner:   messaging.NewGroupProjection(client, keyspace),
		gate:    make(chan struct{}),
		started: make(chan struct{}),
	}
	projector.failReplaces.Store(1)

	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(time.Now().UTC())
	dataStore.groupMemberIDs = []uint64{1}
	dataStore.groupProjections = []store.GroupMembershipProjection{{
		GroupID:   7,
		MemberIDs: []uint64{1},
		Positions: map[uint64]uint64{1: 0},
	}}
	api, router, _ := startChatServer(
		t, dataStore, WithGroupReadTracker(tracker), WithGroupProjector(projector),
	)
	api.Start()
	t.Cleanup(api.Stop)

	recorder := doJSON(
		t, router, http.MethodDelete, "/api/groups/7/members/bob", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if exists, err := client.Exists(ctx, syncKey).Result(); err != nil || exists != 1 {
		t.Fatalf("sync flag = %d, %v; want fail-closed", exists, err)
	}
	if _, err := tracker.UnreadCounts(ctx, 1, []uint64{7}); !errors.Is(err, messaging.ErrMessageServiceUnavailable) {
		t.Fatalf("unread during failed projection = %v, want fail-closed", err)
	}
	select {
	case <-projector.started:
	case <-time.After(2 * time.Second):
		t.Fatal("repair did not run")
	}
	close(projector.gate)

	waitFor(t, func() bool {
		exists, err := client.Exists(ctx, syncKey).Result()
		return err == nil && exists == 0
	})
	if _, err := tracker.UnreadCounts(ctx, 1, []uint64{7}); err != nil {
		t.Fatalf("unread after repair: %v", err)
	}
	member, err := tracker.ReadStats(ctx, 7, 1, 10)
	if err != nil || member.MemberCount != 0 {
		t.Fatalf("read stats after repair = %#v, %v", member, err)
	}
}

func TestGroupRepairRunsAgainWhenScheduledDuringRepair(t *testing.T) {
	fastGroupRepair(t)
	dataStore := authenticatedChatStore()
	dataStore.groupProjections = []store.GroupMembershipProjection{{
		GroupID:   7,
		MemberIDs: []uint64{1},
		Positions: map[uint64]uint64{1: 0},
	}}
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	var calls atomic.Int32
	projector := &fakeGroupProjector{}
	projector.replaceFn = func() error {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
		}
		return nil
	}
	api, _, _ := startChatServer(
		t,
		dataStore,
		WithGroupReadTracker(newFakeGroupReadTracker()),
		WithGroupProjector(projector),
	)
	api.Start()
	t.Cleanup(api.Stop)

	api.groupRepairs.schedule(7)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first repair did not start")
	}
	api.groupRepairs.schedule(7)
	close(release)

	waitFor(t, func() bool { return calls.Load() >= 2 })
}
