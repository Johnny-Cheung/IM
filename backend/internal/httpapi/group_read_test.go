package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/auth"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type groupReadAdvanceCall struct {
	groupID   uint64
	userID    uint64
	throughID uint64
}

type groupReadManyCall struct {
	groupID   uint64
	userIDs   []uint64
	throughID uint64
}

type groupReadSeedCall struct {
	groupID  uint64
	userID   uint64
	position uint64
}

type fakeGroupReadTracker struct {
	mu sync.Mutex

	ready    bool
	readyErr error

	advanceFn    func(groupID, userID, throughID uint64) (messaging.GroupReadAdvance, error)
	advanceCtxFn func(context.Context, uint64, uint64, uint64) (messaging.GroupReadAdvance, error)
	advanceErr   error
	advances     []groupReadAdvanceCall

	manyFn  func(groupID uint64, userIDs []uint64, throughID uint64) ([]messaging.GroupReadAdvance, error)
	manyErr error
	many    []groupReadManyCall

	unread    map[uint64]int
	unreadErr error

	stats    messaging.GroupReadStats
	statsErr error

	senderID    uint64
	senderFound bool
	senderErr   error

	seedErr error
	seeds   []groupReadSeedCall

	removeErr error
	removes   []groupReadSeedCall
}

func newFakeGroupReadTracker() *fakeGroupReadTracker {
	return &fakeGroupReadTracker{ready: true}
}

func (f *fakeGroupReadTracker) ReadModelReady(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ready, f.readyErr
}

func (f *fakeGroupReadTracker) AdvanceRead(
	ctx context.Context,
	groupID, userID, throughID uint64,
	_ time.Time,
) (messaging.GroupReadAdvance, error) {
	f.mu.Lock()
	f.advances = append(f.advances, groupReadAdvanceCall{
		groupID: groupID, userID: userID, throughID: throughID,
	})
	ctxFn := f.advanceCtxFn
	fn := f.advanceFn
	err := f.advanceErr
	f.mu.Unlock()
	if ctxFn != nil {
		return ctxFn(ctx, groupID, userID, throughID)
	}
	if fn != nil {
		return fn(groupID, userID, throughID)
	}
	if err != nil {
		return messaging.GroupReadAdvance{}, err
	}
	return messaging.GroupReadAdvance{
		UserID:            userID,
		LastReadMessageID: throughID,
		Advanced:          throughID > 0,
	}, nil
}

func (f *fakeGroupReadTracker) AdvanceReadMany(
	_ context.Context,
	groupID uint64,
	userIDs []uint64,
	throughID uint64,
	_ time.Time,
) ([]messaging.GroupReadAdvance, error) {
	f.mu.Lock()
	f.many = append(f.many, groupReadManyCall{
		groupID: groupID, userIDs: append([]uint64{}, userIDs...), throughID: throughID,
	})
	fn := f.manyFn
	err := f.manyErr
	f.mu.Unlock()
	if fn != nil {
		return fn(groupID, userIDs, throughID)
	}
	if err != nil {
		return nil, err
	}
	advances := make([]messaging.GroupReadAdvance, 0, len(userIDs))
	for _, userID := range userIDs {
		advances = append(advances, messaging.GroupReadAdvance{
			UserID:            userID,
			LastReadMessageID: throughID,
			Advanced:          throughID > 0,
		})
	}
	return advances, nil
}

func (f *fakeGroupReadTracker) UnreadCounts(
	_ context.Context,
	_ uint64,
	groupIDs []uint64,
) (map[uint64]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unreadErr != nil {
		return nil, f.unreadErr
	}
	counts := make(map[uint64]int, len(groupIDs))
	for _, groupID := range groupIDs {
		counts[groupID] = f.unread[groupID]
	}
	return counts, nil
}

func (f *fakeGroupReadTracker) ReadStats(
	context.Context,
	uint64,
	uint64,
	uint64,
) (messaging.GroupReadStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats, f.statsErr
}

func (f *fakeGroupReadTracker) MessageSender(
	context.Context,
	uint64,
	uint64,
) (uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.senderID, f.senderFound, f.senderErr
}

func (f *fakeGroupReadTracker) SeedMember(
	_ context.Context,
	groupID, userID, position uint64,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seedErr != nil {
		return f.seedErr
	}
	f.seeds = append(f.seeds, groupReadSeedCall{
		groupID: groupID, userID: userID, position: position,
	})
	return nil
}

func (f *fakeGroupReadTracker) RemoveMember(
	_ context.Context,
	groupID, userID uint64,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removeErr != nil {
		return f.removeErr
	}
	f.removes = append(f.removes, groupReadSeedCall{groupID: groupID, userID: userID})
	return nil
}

func (f *fakeGroupReadTracker) snapAdvances() []groupReadAdvanceCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]groupReadAdvanceCall{}, f.advances...)
}

func (f *fakeGroupReadTracker) snapMany() []groupReadManyCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]groupReadManyCall{}, f.many...)
}

func (f *fakeGroupReadTracker) snapSeeds() []groupReadSeedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]groupReadSeedCall{}, f.seeds...)
}

func (f *fakeGroupReadTracker) snapRemoves() []groupReadSeedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]groupReadSeedCall{}, f.removes...)
}

func (f *fakeGroupReadTracker) setSeedErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seedErr = err
}

type recordingDirectReadTracker struct {
	mu sync.Mutex

	ready     bool
	calls     []directReadAdvanceCall
	advanceFn func(userID, peerID, throughID uint64) (messaging.DirectReadAdvance, error)
}

func newRecordingDirectReadTracker() *recordingDirectReadTracker {
	return &recordingDirectReadTracker{ready: true}
}

func (r *recordingDirectReadTracker) AdvanceRead(
	_ context.Context,
	userID, peerID, throughID uint64,
	_ time.Time,
) (messaging.DirectReadAdvance, error) {
	r.mu.Lock()
	r.calls = append(r.calls, directReadAdvanceCall{
		userID: userID, peerID: peerID, throughID: throughID,
	})
	fn := r.advanceFn
	r.mu.Unlock()
	if fn != nil {
		return fn(userID, peerID, throughID)
	}
	return messaging.DirectReadAdvance{
		LastReadMessageID: throughID,
		Candidate:         throughID,
		Advanced:          throughID > 0,
	}, nil
}

func (r *recordingDirectReadTracker) ReadPositions(context.Context, uint64) (map[uint64]uint64, error) {
	return map[uint64]uint64{}, nil
}

func (r *recordingDirectReadTracker) PeerReadPositions(
	context.Context, uint64, []uint64,
) (map[uint64]uint64, error) {
	return map[uint64]uint64{}, nil
}

func (r *recordingDirectReadTracker) UnreadCounts(
	context.Context, uint64, []uint64, map[uint64]uint64,
) (map[uint64]int, error) {
	return map[uint64]int{}, nil
}

func (r *recordingDirectReadTracker) ReadModelReady(context.Context) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ready, nil
}

func (r *recordingDirectReadTracker) snapshot() []directReadAdvanceCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]directReadAdvanceCall{}, r.calls...)
}

func chatSessions(users ...store.User) map[string]store.SessionUser {
	sessions := make(map[string]store.SessionUser, len(users))
	for _, user := range users {
		sessions[string(auth.HashSessionToken(testToken(user.Username)))] = store.SessionUser{
			ExpiresAt: time.Now().Add(time.Hour),
			User:      user,
		}
	}
	return sessions
}

func testToken(username string) string {
	return username + "-token"
}

func testCookie(username string) *http.Cookie {
	return &http.Cookie{Name: sessionCookieName, Value: testToken(username)}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func TestGroupReadStats(t *testing.T) {
	dataStore := authenticatedFriendStore()
	tracker := newFakeGroupReadTracker()
	tracker.senderFound = true
	tracker.senderID = 1
	tracker.stats = messaging.GroupReadStats{ReadCount: 3, MemberCount: 5}
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/read-stats?message_id=345", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeBody[groupReadStatsResponse](t, recorder)
	if response.MessageID != 345 || response.ReadCount != 3 ||
		response.MemberCount != 5 || response.AllRead {
		t.Fatalf("response = %#v", response)
	}
}

func TestGroupReadStatsRejectsOtherSender(t *testing.T) {
	dataStore := authenticatedFriendStore()
	tracker := newFakeGroupReadTracker()
	tracker.senderFound = true
	tracker.senderID = 2
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/read-stats?message_id=345", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusForbidden || errorCode(t, recorder) != "not_your_message" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestGroupReadStatsFallsBackToPersistedSender(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.groupMessageSender = 1
	dataStore.groupMessageSenderFound = true
	tracker := newFakeGroupReadTracker()
	tracker.stats = messaging.GroupReadStats{ReadCount: 1, MemberCount: 2, AllRead: false}
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/read-stats?message_id=345", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestGroupReadStatsMissingMessage(t *testing.T) {
	dataStore := authenticatedFriendStore()
	tracker := newFakeGroupReadTracker()
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/read-stats?message_id=345", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusNotFound || errorCode(t, recorder) != "message_not_found" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestGroupReadStatsRejectsInvalidMessageID(t *testing.T) {
	dataStore := authenticatedFriendStore()
	tracker := newFakeGroupReadTracker()
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	for _, query := range []string{"", "?message_id=", "?message_id=0", "?message_id=abc"} {
		recorder := doJSON(
			t, router, http.MethodGet, "/api/groups/7/read-stats"+query, nil, authTestCookie(),
		)
		if recorder.Code != http.StatusBadRequest || errorCode(t, recorder) != "invalid_request" {
			t.Fatalf("query %q status = %d, body = %s", query, recorder.Code, recorder.Body.String())
		}
	}
}

func TestGroupReadStatsRequiresReadModel(t *testing.T) {
	dataStore := authenticatedFriendStore()
	tracker := newFakeGroupReadTracker()
	tracker.ready = false
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/read-stats?message_id=345", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusServiceUnavailable ||
		errorCode(t, recorder) != "read_model_not_ready" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestGroupReadStatsMapsGroupErrors(t *testing.T) {
	dataStore := authenticatedFriendStore()
	tracker := newFakeGroupReadTracker()
	tracker.senderFound = true
	tracker.senderID = 1
	tracker.statsErr = messaging.ErrGroupNotMember
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/read-stats?message_id=345", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusNotFound || errorCode(t, recorder) != "group_not_found" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestGroupReadStatsRequiresSession(t *testing.T) {
	dataStore := authenticatedFriendStore()
	tracker := newFakeGroupReadTracker()
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/read-stats?message_id=345", nil, nil,
	)
	if recorder.Code != http.StatusUnauthorized || errorCode(t, recorder) != "unauthenticated" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestGroupReadStatsReturnsUnavailableOnRedisError(t *testing.T) {
	dataStore := authenticatedFriendStore()
	tracker := newFakeGroupReadTracker()
	tracker.senderFound = true
	tracker.senderID = 1
	tracker.statsErr = messaging.ErrMessageServiceUnavailable
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/read-stats?message_id=345", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusServiceUnavailable ||
		errorCode(t, recorder) != "message_service_unavailable" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestListConversationsUsesGroupReadTracker(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.groupConversationsWithoutUnread = []store.GroupConversation{
		{
			Group: store.Group{
				ID: 9, Name: "Team", OwnerID: 2,
				Owner:     store.PublicUser{Username: "bob", Nickname: "Bob"},
				CreatedAt: now,
			},
			MemberCount: 3,
		},
		{
			Group: store.Group{
				ID: 10, Name: "Other", OwnerID: 2,
				Owner:     store.PublicUser{Username: "bob", Nickname: "Bob"},
				CreatedAt: now.Add(-time.Minute),
			},
			MemberCount: 2,
		},
	}
	tracker := newFakeGroupReadTracker()
	tracker.unread = map[uint64]int{9: 5}
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(t, router, http.MethodGet, "/api/conversations", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeBody[conversationListResponse](t, recorder)
	unread := make(map[uint64]int)
	for _, item := range response.Conversations {
		if item.Group == nil {
			continue
		}
		unread[item.Group.ID] = item.UnreadCount
	}
	if unread[9] != 5 || unread[10] != 0 {
		t.Fatalf("unread = %#v", unread)
	}
}

func TestListConversationsRequiresGroupReadModel(t *testing.T) {
	dataStore := authenticatedChatStore()
	tracker := newFakeGroupReadTracker()
	tracker.ready = false
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(t, router, http.MethodGet, "/api/conversations", nil, authTestCookie())
	if recorder.Code != http.StatusServiceUnavailable ||
		errorCode(t, recorder) != "read_model_not_ready" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestListConversationsGroupUnreadFailureIsUnavailable(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.groupConversationsWithoutUnread = []store.GroupConversation{{
		Group: store.Group{
			ID: 9, Name: "Team", OwnerID: 2,
			Owner:     store.PublicUser{Username: "bob", Nickname: "Bob"},
			CreatedAt: time.Now().UTC(),
		},
		MemberCount: 2,
	}}
	tracker := newFakeGroupReadTracker()
	tracker.unreadErr = messaging.ErrMessageServiceUnavailable
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(t, router, http.MethodGet, "/api/conversations", nil, authTestCookie())
	if recorder.Code != http.StatusServiceUnavailable ||
		errorCode(t, recorder) != "message_service_unavailable" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestBootstrapUsesGroupReadTracker(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.groupConversationsWithoutUnread = []store.GroupConversation{{
		Group: store.Group{
			ID: 9, Name: "Team", OwnerID: 2,
			Owner:     store.PublicUser{Username: "bob", Nickname: "Bob"},
			CreatedAt: now,
		},
		MemberCount: 3,
	}}
	tracker := newFakeGroupReadTracker()
	tracker.unread = map[uint64]int{9: 4}
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(t, router, http.MethodPost, "/api/bootstrap", map[string]any{}, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeBody[bootstrapResponse](t, recorder)
	if len(response.Conversations) != 1 || response.Conversations[0].UnreadCount != 4 {
		t.Fatalf("conversations = %#v", response.Conversations)
	}
}

func TestBootstrapGroupReadModelNotReadyIsPartial(t *testing.T) {
	dataStore := authenticatedChatStore()
	tracker := newFakeGroupReadTracker()
	tracker.ready = false
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(t, router, http.MethodPost, "/api/bootstrap", map[string]any{}, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeBody[bootstrapResponse](t, recorder)
	found := false
	for _, section := range response.Partial {
		if section == "conversations" {
			found = true
		}
	}
	if !found {
		t.Fatalf("partial = %#v", response.Partial)
	}
}

func TestLegacyMarkGroupReadUsesTracker(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.groupMemberIDs = []uint64{1, 2}
	tracker := newFakeGroupReadTracker()
	tracker.advanceFn = func(_, _, throughID uint64) (messaging.GroupReadAdvance, error) {
		return messaging.GroupReadAdvance{
			UserID:            1,
			LastReadMessageID: throughID,
			Advanced:          true,
		}, nil
	}
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodPost, "/api/groups/7/read?through_id=42", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if response := decodeBody[readStateResponse](t, recorder); response.LastReadMessageID != 42 {
		t.Fatalf("response = %#v", response)
	}
	advances := tracker.snapAdvances()
	if len(advances) != 1 || advances[0].groupID != 7 ||
		advances[0].userID != 1 || advances[0].throughID != 42 {
		t.Fatalf("advances = %#v", advances)
	}
	if dataStore.markGroupReadThrough != 0 {
		t.Fatalf("MySQL mark group read was called: %#v", dataStore.markGroupReadThrough)
	}
}

func TestLegacyMarkGroupReadRequiresReadModel(t *testing.T) {
	dataStore := authenticatedChatStore()
	tracker := newFakeGroupReadTracker()
	tracker.ready = false
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodPost, "/api/groups/7/read?through_id=42", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusServiceUnavailable ||
		errorCode(t, recorder) != "read_model_not_ready" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func startChatServer(
	t *testing.T,
	dataStore *fakeStore,
	options ...Option,
) (*API, *gin.Engine, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	api := New(dataStore, options...)
	router := gin.New()
	api.Register(router)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return api, router, server.URL
}

func dialChatSocket(t *testing.T, baseURL, username string) *websocket.Conn {
	t.Helper()
	header := http.Header{"Cookie": []string{sessionCookieName + "=" + testToken(username)}}
	wsURL := "ws" + strings.TrimPrefix(baseURL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial websocket %s: %v", username, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func readWSEvent(t *testing.T, conn *websocket.Conn, timeout time.Duration) map[string]any {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket event: %v", err)
	}
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode websocket event %q: %v", payload, err)
	}
	return event
}

func readWSEventType(t *testing.T, conn *websocket.Conn, eventType string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		event := readWSEvent(t, conn, time.Until(deadline))
		if event["type"] == eventType {
			return event
		}
	}
	t.Fatalf("websocket event %q not received", eventType)
	return nil
}

func TestWebSocketFocusDirectAdvancesAndAcks(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	bob := store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore := &fakeStore{
		sessionByTokenHash: chatSessions(alice, bob),
		usersByKey:         map[string]store.User{"bob": bob},
		relationship:       store.RelationshipFriend,
	}
	tracker := newRecordingDirectReadTracker()
	tracker.advanceFn = func(_, _, _ uint64) (messaging.DirectReadAdvance, error) {
		return messaging.DirectReadAdvance{LastReadMessageID: 42, Advanced: true}, nil
	}
	api, _, baseURL := startChatServer(t, dataStore, WithDirectReadTracker(tracker))
	conn := dialChatSocket(t, baseURL, "alice")
	waitFor(t, func() bool { return api.hub.activeCount(1) == 1 })

	if err := conn.WriteJSON(map[string]any{
		"type": "focus", "kind": "direct", "username": "bob",
	}); err != nil {
		t.Fatalf("write focus frame: %v", err)
	}
	ack := readWSEventType(t, conn, "focus.ack")
	if ack["kind"] != "direct" || ack["username"] != "bob" ||
		ack["last_read_message_id"] != float64(42) || ack["error"] != nil {
		t.Fatalf("ack = %#v", ack)
	}
	calls := tracker.snapshot()
	if len(calls) != 1 || calls[0].userID != 1 || calls[0].peerID != 2 ||
		calls[0].throughID != 0 {
		t.Fatalf("advance calls = %#v", calls)
	}
	if !api.hub.isDirectFocus(1, 2) {
		t.Fatal("focus was not recorded on the connection")
	}
}

func TestWebSocketFocusDirectRejectsNonFriend(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	bob := store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore := &fakeStore{
		sessionByTokenHash: chatSessions(alice, bob),
		usersByKey:         map[string]store.User{"bob": bob},
		relationship:       store.RelationshipNone,
	}
	tracker := newRecordingDirectReadTracker()
	api, _, baseURL := startChatServer(t, dataStore, WithDirectReadTracker(tracker))
	conn := dialChatSocket(t, baseURL, "alice")
	waitFor(t, func() bool { return api.hub.activeCount(1) == 1 })

	if err := conn.WriteJSON(map[string]any{
		"type": "focus", "kind": "direct", "username": "bob",
	}); err != nil {
		t.Fatalf("write focus frame: %v", err)
	}
	ack := readWSEventType(t, conn, "focus.ack")
	if ack["error"] != "not_friend" || ack["kind"] != "direct" ||
		ack["username"] != "bob" {
		t.Fatalf("ack = %#v", ack)
	}
	if calls := tracker.snapshot(); len(calls) != 0 {
		t.Fatalf("advance calls = %#v", calls)
	}
	if api.hub.isDirectFocus(1, 2) {
		t.Fatal("rejected focus was recorded")
	}
}

func TestWebSocketFocusGroupAdvancesAndAcks(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	dataStore := &fakeStore{
		sessionByTokenHash: chatSessions(alice),
		groupMemberIDs:     []uint64{1, 2},
	}
	tracker := newFakeGroupReadTracker()
	tracker.advanceFn = func(groupID, userID, _ uint64) (messaging.GroupReadAdvance, error) {
		return messaging.GroupReadAdvance{
			UserID: userID, LastReadMessageID: 345, Advanced: true,
		}, nil
	}
	api, _, baseURL := startChatServer(t, dataStore, WithGroupReadTracker(tracker))
	conn := dialChatSocket(t, baseURL, "alice")
	waitFor(t, func() bool { return api.hub.activeCount(1) == 1 })

	if err := conn.WriteJSON(map[string]any{
		"type": "focus", "kind": "group", "group_id": 7,
	}); err != nil {
		t.Fatalf("write focus frame: %v", err)
	}
	ack := readWSEventType(t, conn, "focus.ack")
	if ack["kind"] != "group" || ack["group_id"] != float64(7) ||
		ack["last_read_message_id"] != float64(345) || ack["error"] != nil {
		t.Fatalf("ack = %#v", ack)
	}
	advances := tracker.snapAdvances()
	if len(advances) != 1 || advances[0].groupID != 7 || advances[0].userID != 1 ||
		advances[0].throughID != 0 {
		t.Fatalf("advances = %#v", advances)
	}
	users := api.hub.focusedGroupUsers(7)
	if len(users) != 1 || users[0] != 1 {
		t.Fatalf("focused users = %#v", users)
	}
}

func TestWebSocketFocusGroupRejectsNonMember(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	dataStore := &fakeStore{sessionByTokenHash: chatSessions(alice)}
	tracker := newFakeGroupReadTracker()
	tracker.advanceErr = messaging.ErrGroupNotMember
	api, _, baseURL := startChatServer(t, dataStore, WithGroupReadTracker(tracker))
	conn := dialChatSocket(t, baseURL, "alice")
	waitFor(t, func() bool { return api.hub.activeCount(1) == 1 })

	if err := conn.WriteJSON(map[string]any{
		"type": "focus", "kind": "group", "group_id": 7,
	}); err != nil {
		t.Fatalf("write focus frame: %v", err)
	}
	ack := readWSEventType(t, conn, "focus.ack")
	if ack["error"] != "not_group_member" || ack["kind"] != "group" ||
		ack["group_id"] != float64(7) {
		t.Fatalf("ack = %#v", ack)
	}
	if users := api.hub.focusedGroupUsers(7); len(users) != 0 {
		t.Fatalf("focused users = %#v", users)
	}
}

func TestWSRateLimiterAllowsBurstThenRefills(t *testing.T) {
	var limiter wsRateLimiter
	now := time.Now()
	allowed := 0
	for index := 0; index < 20; index++ {
		if limiter.allow(now) {
			allowed++
		}
	}
	if allowed != int(wsFrameBurst) {
		t.Fatalf("allowed = %d, want %d", allowed, int(wsFrameBurst))
	}
	if limiter.allow(now.Add(100 * time.Millisecond)) {
		t.Fatal("half a refill period granted a frame")
	}
	if !limiter.allow(now.Add(200 * time.Millisecond)) {
		t.Fatal("full refill period did not grant a frame")
	}
}

func TestWebSocketDropsExcessFramesWithoutClosing(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	bob := store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore := &fakeStore{
		sessionByTokenHash: chatSessions(alice, bob),
		usersByKey:         map[string]store.User{"bob": bob},
		relationship:       store.RelationshipFriend,
	}
	tracker := newRecordingDirectReadTracker()
	api, _, baseURL := startChatServer(t, dataStore, WithDirectReadTracker(tracker))
	conn := dialChatSocket(t, baseURL, "alice")
	waitFor(t, func() bool { return api.hub.activeCount(1) == 1 })

	for index := 0; index < 40; index++ {
		if err := conn.WriteJSON(map[string]any{"type": "blur"}); err != nil {
			t.Fatalf("write frame: %v", err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if api.hub.activeCount(1) != 1 {
		t.Fatal("excess frames closed the connection")
	}

	time.Sleep(300 * time.Millisecond)
	if err := conn.WriteJSON(map[string]any{
		"type": "focus", "kind": "direct", "username": "bob",
	}); err != nil {
		t.Fatalf("write focus frame: %v", err)
	}
	if ack := readWSEventType(t, conn, "focus.ack"); ack["kind"] != "direct" {
		t.Fatalf("ack = %#v", ack)
	}
}

func TestImplicitDirectReadAdvancesFocusedRecipientAndSender(t *testing.T) {
	now := time.Now().UTC()
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	bob := store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore := &fakeStore{
		sessionByTokenHash: chatSessions(alice, bob),
		usersByKey:         map[string]store.User{"alice": alice, "bob": bob},
		relationship:       store.RelationshipFriend,
		createdMessage: store.Message{
			ID:              21,
			SenderID:        1,
			RecipientID:     2,
			ClientMessageID: "client-12345678",
			Body:            "live hello",
			CreatedAt:       now,
		},
		createdMessageNew: true,
	}
	tracker := newRecordingDirectReadTracker()
	tracker.advanceFn = func(userID, peerID, throughID uint64) (messaging.DirectReadAdvance, error) {
		if throughID == 0 {
			return messaging.DirectReadAdvance{}, nil
		}
		return messaging.DirectReadAdvance{
			LastReadMessageID: throughID,
			Advanced:          true,
		}, nil
	}
	api, router, baseURL := startChatServer(t, dataStore, WithDirectReadTracker(tracker))
	api.Start()
	t.Cleanup(api.Stop)

	bobConn := dialChatSocket(t, baseURL, "bob")
	waitFor(t, func() bool { return api.hub.activeCount(2) == 1 })
	if err := bobConn.WriteJSON(map[string]any{
		"type": "focus", "kind": "direct", "username": "alice",
	}); err != nil {
		t.Fatalf("write focus frame: %v", err)
	}
	readWSEventType(t, bobConn, "focus.ack")
	before := len(tracker.snapshot())

	recorder := doJSON(t, router, http.MethodPost, "/api/conversations/bob/messages", map[string]string{
		"body":              "live hello",
		"client_message_id": "client-12345678",
	}, testCookie("alice"))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("send status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	waitFor(t, func() bool {
		sender, recipient := false, false
		for _, call := range tracker.snapshot()[before:] {
			if call.throughID != 21 {
				continue
			}
			if call.userID == 1 && call.peerID == 2 {
				sender = true
			}
			if call.userID == 2 && call.peerID == 1 {
				recipient = true
			}
		}
		return sender && recipient
	})
}

func TestImplicitReadMergesWithinWindow(t *testing.T) {
	tracker := newRecordingDirectReadTracker()
	api := New(&fakeStore{}, WithDirectReadTracker(tracker))
	api.Start()
	t.Cleanup(api.Stop)

	api.scheduleDirectImplicitRead(1, 2, "alice", "bob", 10)
	api.scheduleDirectImplicitRead(1, 2, "alice", "bob", 12)
	api.scheduleDirectImplicitRead(1, 2, "alice", "bob", 11)

	waitFor(t, func() bool {
		for _, call := range tracker.snapshot() {
			if call.throughID == 12 {
				return true
			}
		}
		return false
	})
	time.Sleep(150 * time.Millisecond)

	calls := tracker.snapshot()
	if len(calls) != 1 || calls[0].userID != 1 || calls[0].peerID != 2 ||
		calls[0].throughID != 12 {
		t.Fatalf("advance calls = %#v", calls)
	}
}

func TestImplicitGroupReadAdvancesViewersAndBroadcasts(t *testing.T) {
	now := time.Now().UTC()
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	bob := store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore := &fakeStore{
		sessionByTokenHash: chatSessions(alice, bob),
		usersByKey:         map[string]store.User{"alice": alice, "bob": bob},
		groupMemberIDs:     []uint64{1, 2},
		createdGroupMessage: store.GroupMessage{
			ID:              12,
			GroupID:         7,
			SenderID:        1,
			Sender:          store.PublicUser{Username: "alice", Nickname: "Alice"},
			ClientMessageID: "group-client-1",
			Body:            "live group hello",
			CreatedAt:       now,
		},
		createdGroupMessageNew: true,
	}
	tracker := newFakeGroupReadTracker()
	tracker.manyFn = func(groupID uint64, userIDs []uint64, throughID uint64) ([]messaging.GroupReadAdvance, error) {
		return []messaging.GroupReadAdvance{
			{UserID: 1, LastReadMessageID: throughID, Advanced: true},
		}, nil
	}
	tracker.advanceFn = func(groupID, userID, throughID uint64) (messaging.GroupReadAdvance, error) {
		return messaging.GroupReadAdvance{UserID: userID}, nil
	}
	api, router, baseURL := startChatServer(t, dataStore, WithGroupReadTracker(tracker))
	api.Start()
	t.Cleanup(api.Stop)

	aliceConn := dialChatSocket(t, baseURL, "alice")
	bobConn := dialChatSocket(t, baseURL, "bob")
	waitFor(t, func() bool {
		return api.hub.activeCount(1) == 1 && api.hub.activeCount(2) == 1
	})
	for _, conn := range []*websocket.Conn{aliceConn, bobConn} {
		if err := conn.WriteJSON(map[string]any{
			"type": "focus", "kind": "group", "group_id": 7,
		}); err != nil {
			t.Fatalf("write focus frame: %v", err)
		}
		readWSEventType(t, conn, "focus.ack")
	}
	before := len(tracker.snapMany())

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/messages", map[string]string{
		"body":              "live group hello",
		"client_message_id": "group-client-1",
	}, testCookie("alice"))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("send status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	waitFor(t, func() bool {
		for _, call := range tracker.snapMany()[before:] {
			if call.throughID != 12 {
				continue
			}
			hasAlice, hasBob := false, false
			for _, userID := range call.userIDs {
				if userID == 1 {
					hasAlice = true
				}
				if userID == 2 {
					hasBob = true
				}
			}
			return hasAlice && hasBob
		}
		return false
	})

	event := readWSEventType(t, bobConn, "group.read")
	if event["group_id"] != float64(7) {
		t.Fatalf("group read event = %#v", event)
	}
	readWSEventType(t, aliceConn, "group.message.created")
	_ = aliceConn.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	for {
		_, payload, err := aliceConn.ReadMessage()
		if err != nil {
			break
		}
		var received map[string]any
		if err := json.Unmarshal(payload, &received); err != nil {
			continue
		}
		if received["type"] == "group.read" {
			t.Fatal("reader received its own group.read event")
		}
	}

	senderCaughtUp := false
	for _, call := range tracker.snapAdvances() {
		if call.groupID == 7 && call.userID == 1 && call.throughID == 12 {
			senderCaughtUp = true
		}
	}
	if !senderCaughtUp {
		t.Fatalf("sender advance calls = %#v", tracker.snapAdvances())
	}
}

func TestAPIImplicitReadStops(t *testing.T) {
	api := New(&fakeStore{})
	api.Start()
	api.Start()

	stopped := make(chan struct{})
	go func() {
		api.Stop()
		api.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("API.Stop did not return")
	}
	select {
	case <-api.implicitReads.done:
	default:
		t.Fatal("implicit read loop is still running")
	}
}

type fakeGroupProjector struct {
	mu           sync.Mutex
	watermark    uint64
	watermarkErr error
	begins       int
	ends         int
	replaces     int

	watermarkBeforeBegin bool
	replaceFn            func() error
	lastGroupID          uint64
	lastMembers          []uint64
	lastDissolved        bool
	replaceSucceeded     int
}

func (p *fakeGroupProjector) Replace(
	_ context.Context,
	groupID uint64,
	memberIDs []uint64,
	dissolved bool,
) error {
	p.mu.Lock()
	p.replaces++
	p.lastGroupID = groupID
	p.lastMembers = append([]uint64{}, memberIDs...)
	p.lastDissolved = dissolved
	fn := p.replaceFn
	p.mu.Unlock()
	if fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}
	p.mu.Lock()
	p.replaceSucceeded++
	p.mu.Unlock()
	return nil
}

func (p *fakeGroupProjector) BeginMutation(context.Context, uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	return nil
}

func (p *fakeGroupProjector) EndMutation(context.Context, uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ends++
	return nil
}

func (p *fakeGroupProjector) LastAcceptedID(context.Context, uint64) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.begins == 0 {
		p.watermarkBeforeBegin = true
	}
	return p.watermark, p.watermarkErr
}

func TestCreateGroupSeedsReadPositions(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(now)
	dataStore.groupMemberIDs = []uint64{1, 2, 3}
	tracker := newFakeGroupReadTracker()
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(t, router, http.MethodPost, "/api/groups", map[string]any{
		"name":             "Team",
		"member_usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	seeds := tracker.snapSeeds()
	if len(seeds) != 3 {
		t.Fatalf("seeds = %#v", seeds)
	}
	for _, seed := range seeds {
		if seed.groupID != 7 || seed.position != 0 {
			t.Fatalf("seed = %#v", seed)
		}
	}
}

func TestInviteGroupMembersSeedsBaseline(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(now)
	dataStore.groupMemberIDSequence = [][]uint64{{1, 2}, {1, 2, 3}}
	dataStore.inviteGroupBaseline = 60
	tracker := newFakeGroupReadTracker()
	projector := &fakeGroupProjector{watermark: 55}
	router := testRouter(
		dataStore,
		WithGroupReadTracker(tracker),
		WithGroupProjector(projector),
		WithGroupAcceptedWatermarkReader(projector),
	)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/members", map[string]any{
		"usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if dataStore.inviteGroupCall.baselineFloor != 55 {
		t.Fatalf("baseline floor = %d, want 55", dataStore.inviteGroupCall.baselineFloor)
	}
	seeds := tracker.snapSeeds()
	if len(seeds) != 1 || seeds[0].groupID != 7 || seeds[0].userID != 3 ||
		seeds[0].position != 60 {
		t.Fatalf("seeds = %#v", seeds)
	}
	if projector.begins != 1 || projector.replaces != 1 || projector.ends != 0 {
		t.Fatalf("projector = %#v", projector)
	}
}

func TestInviteGroupMembersRejectsWhenWatermarkUnavailable(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(now)
	tracker := newFakeGroupReadTracker()
	projector := &fakeGroupProjector{watermarkErr: messaging.ErrMessageServiceUnavailable}
	router := testRouter(
		dataStore,
		WithGroupReadTracker(tracker),
		WithGroupProjector(projector),
		WithGroupAcceptedWatermarkReader(projector),
	)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/members", map[string]any{
		"usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusServiceUnavailable ||
		errorCode(t, recorder) != "message_service_unavailable" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if dataStore.inviteGroupCall.groupID != 0 {
		t.Fatalf("invite was called: %#v", dataStore.inviteGroupCall)
	}
}

func TestInviteGroupMembersSeedFailureStillProjects(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(now)
	dataStore.groupMemberIDSequence = [][]uint64{{1, 2}, {1, 2, 3}}
	dataStore.inviteGroupBaseline = 60
	tracker := newFakeGroupReadTracker()
	tracker.seedErr = messaging.ErrMessageServiceUnavailable
	projector := &fakeGroupProjector{watermark: 55}
	router := testRouter(
		dataStore,
		WithGroupReadTracker(tracker),
		WithGroupProjector(projector),
		WithGroupAcceptedWatermarkReader(projector),
	)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/members", map[string]any{
		"usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusServiceUnavailable ||
		errorCode(t, recorder) != "message_service_unavailable" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	// The invite committed, so the member set and the sync flag must be
	// refreshed even though seeding failed; the repair loop re-seeds.
	if projector.begins != 1 || projector.replaces != 1 || projector.ends != 0 {
		t.Fatalf("projector = %#v", projector)
	}
}

func TestKickGroupMemberRemovesReadPosition(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(now)
	dataStore.groupMemberIDs = []uint64{1}
	tracker := newFakeGroupReadTracker()
	projector := &fakeGroupProjector{}
	router := testRouter(
		dataStore,
		WithGroupReadTracker(tracker),
		WithGroupProjector(projector),
	)

	recorder := doJSON(
		t, router, http.MethodDelete, "/api/groups/7/members/bob", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	removes := tracker.snapRemoves()
	if len(removes) != 1 || removes[0].groupID != 7 || removes[0].userID != 2 {
		t.Fatalf("removes = %#v", removes)
	}
	if projector.begins != 1 || projector.replaces != 1 {
		t.Fatalf("projector = %#v", projector)
	}
}

func TestKickGroupMemberReadFailureStillProjects(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(now)
	dataStore.groupMemberIDs = []uint64{1}
	tracker := newFakeGroupReadTracker()
	tracker.removeErr = messaging.ErrMessageServiceUnavailable
	projector := &fakeGroupProjector{}
	router := testRouter(
		dataStore,
		WithGroupReadTracker(tracker),
		WithGroupProjector(projector),
	)

	recorder := doJSON(
		t, router, http.MethodDelete, "/api/groups/7/members/bob", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	// A failed read-position removal must not skip the member-set rewrite: the
	// rewrite clears sync_required and prunes the stale read entry.
	if projector.begins != 1 || projector.replaces != 1 || projector.ends != 0 {
		t.Fatalf("projector = %#v", projector)
	}
}

func TestLeaveGroupRemovesReadPosition(t *testing.T) {
	for _, dissolved := range []bool{false, true} {
		t.Run(map[bool]string{false: "member", true: "dissolved"}[dissolved], func(t *testing.T) {
			dataStore := authenticatedChatStore()
			dataStore.leaveGroupDissolved = dissolved
			dataStore.groupMemberIDs = []uint64{1, 2}
			tracker := newFakeGroupReadTracker()
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
			removes := tracker.snapRemoves()
			if dissolved {
				if len(removes) != 0 {
					t.Fatalf("removes = %#v", removes)
				}
				return
			}
			if len(removes) != 1 || removes[0].groupID != 7 || removes[0].userID != 1 {
				t.Fatalf("removes = %#v", removes)
			}
		})
	}
}
