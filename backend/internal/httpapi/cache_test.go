package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/auth"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
)

func TestSessionCacheHitExpiryAndMutation(t *testing.T) {
	cache := newSessionCache()
	key, ok := sessionCacheKeyFor(bytes.Repeat([]byte{1}, 32))
	if !ok {
		t.Fatal("session key was rejected")
	}
	now := time.Now().UTC()
	session := store.SessionUser{
		UserID:    7,
		ExpiresAt: now.Add(time.Hour),
		User:      store.User{ID: 7, Username: "alice", Nickname: "Alice"},
	}

	cache.put(key, session, cache.queryStart(), now)
	if got, ok := cache.get(key, now.Add(time.Minute)); !ok || got.UserID != 7 {
		t.Fatalf("cache hit = %#v, %t, want user 7", got, ok)
	}
	if _, ok := cache.get(key, now.Add(sessionCacheTTL+time.Minute)); ok {
		t.Fatal("expired session cache entry was returned")
	}

	cache.markMutation(7)
	if _, ok := cache.get(key, now.Add(time.Minute)); ok {
		t.Fatal("mutated session cache entry was returned")
	}
}

func TestSessionCacheRejectsFillRacingMutation(t *testing.T) {
	cache := newSessionCache()
	key, _ := sessionCacheKeyFor(bytes.Repeat([]byte{2}, 32))
	now := time.Now().UTC()
	queryStart := cache.queryStart()
	session := store.SessionUser{
		UserID:    8,
		ExpiresAt: now.Add(time.Hour),
		User:      store.User{ID: 8, Username: "bob", Nickname: "Bob"},
	}

	cache.markMutation(8)
	cache.put(key, session, queryStart, now)
	if _, ok := cache.get(key, now); ok {
		t.Fatal("cache fill that raced a mutation was stored")
	}

	mutation := cache.markMutation(8)
	cache.putFresh(key, session, mutation, now)
	if got, ok := cache.get(key, now); !ok || got.UserID != 8 {
		t.Fatalf("fresh cache entry = %#v, %t, want user 8", got, ok)
	}
}

func TestUserCacheHitExpiryAndMutation(t *testing.T) {
	cache := newUserCache()
	now := time.Now().UTC()
	user := store.User{ID: 9, Username: "carol", Nickname: "Carol"}

	cache.put("carol", user, cache.queryStart(), now)
	if got, ok := cache.get("carol", now.Add(time.Minute)); !ok || got.ID != 9 {
		t.Fatalf("cache hit = %#v, %t, want user 9", got, ok)
	}
	if _, ok := cache.get("carol", now.Add(userCacheTTL+time.Minute)); ok {
		t.Fatal("expired user cache entry was returned")
	}

	cache.markMutation("carol")
	if _, ok := cache.get("carol", now); ok {
		t.Fatal("mutated user cache entry was returned")
	}
}

func TestUserCacheRejectsFillRacingNicknameChange(t *testing.T) {
	cache := newUserCache()
	now := time.Now().UTC()
	queryStart := cache.queryStart()
	user := store.User{ID: 10, Username: "dave", Nickname: "Old"}

	cache.markMutation("dave")
	cache.put("dave", user, queryStart, now)
	if _, ok := cache.get("dave", now); ok {
		t.Fatal("user cache fill that raced a nickname change was stored")
	}

	mutation := cache.markMutation("dave")
	cache.putFresh("dave", store.User{ID: 10, Username: "dave", Nickname: "New"}, mutation, now)
	got, ok := cache.get("dave", now)
	if !ok || got.Nickname != "New" {
		t.Fatalf("fresh user cache entry = %#v, %t, want nickname New", got, ok)
	}
}

func TestSendMessageAfterWarmupDoesNotQueryMySQL(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	acceptor := &directMessageAcceptorStub{
		result: messaging.DirectMessageResult{
			Created: true,
			Message: store.Message{
				ID:              42,
				Seq:             4,
				SenderID:        1,
				RecipientID:     2,
				ClientMessageID: "client-12345678",
				Body:            "hello",
				CreatedAt:       time.Now().UTC(),
			},
		},
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithDirectMessageAcceptor(acceptor)).Register(router)

	for _, clientID := range []string{"client-12345678", "client-87654321"} {
		recorder := doJSON(
			t,
			router,
			http.MethodPost,
			"/api/conversations/bob/messages",
			map[string]string{
				"body":              "hello",
				"client_message_id": clientID,
			},
			authTestCookie(),
		)
		if recorder.Code != http.StatusCreated {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
	}

	if dataStore.sessionLookups != 1 {
		t.Fatalf("session lookups = %d, want 1", dataStore.sessionLookups)
	}
	if dataStore.userLookups != 1 {
		t.Fatalf("user lookups = %d, want 1", dataStore.userLookups)
	}
}

func TestSendGroupMessageUsesRedisRecipientsWithoutMySQL(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.createdGroupMessage = store.GroupMessage{
		ID:              12,
		Seq:             12,
		GroupID:         7,
		SenderID:        1,
		Sender:          store.PublicUser{Username: "alice", Nickname: "Alice"},
		ClientMessageID: "group-client-1",
		Body:            "hello",
		CreatedAt:       time.Now().UTC(),
	}
	dataStore.createdGroupMessageNew = true
	tracker := newFakeGroupReadTracker()
	tracker.memberIDs = []uint64{1, 2}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithGroupReadTracker(tracker)).Register(router)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/messages", map[string]string{
		"body":              "hello",
		"client_message_id": "group-client-1",
	}, authTestCookie())
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response sendGroupMessageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if dataStore.groupMemberLookups != 0 {
		t.Fatalf("group member MySQL lookups = %d, want 0", dataStore.groupMemberLookups)
	}
}

func TestLoginCachesNewSession(t *testing.T) {
	passwordHash, err := auth.HashPassword("password123")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	dataStore := &fakeStore{
		usersByKey: map[string]store.User{
			"alice": {
				ID:           1,
				Username:     "alice",
				Nickname:     "Alice",
				PasswordHash: passwordHash,
			},
		},
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore).Register(router)

	login := doJSON(t, router, http.MethodPost, "/api/auth/login", map[string]string{
		"username": "alice",
		"password": "password123",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login status = %d, body = %s", login.Code, login.Body.String())
	}
	cookie := responseCookie(t, login, sessionCookieName)
	if cookie == nil || cookie.Value == "" {
		t.Fatal("login did not set a session cookie")
	}

	me := doJSON(t, router, http.MethodGet, "/api/me", nil, cookie)
	if me.Code != http.StatusOK {
		t.Fatalf("me status = %d, body = %s", me.Code, me.Body.String())
	}
	if dataStore.sessionLookups != 0 {
		t.Fatalf("session lookups after login = %d, want 0", dataStore.sessionLookups)
	}
}

func TestSendGroupMessageFallsBackToMySQLRecipients(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.createdGroupMessage = store.GroupMessage{
		ID:              12,
		Seq:             12,
		GroupID:         7,
		SenderID:        1,
		Sender:          store.PublicUser{Username: "alice", Nickname: "Alice"},
		ClientMessageID: "group-client-1",
		Body:            "hello",
		CreatedAt:       time.Now().UTC(),
	}
	dataStore.createdGroupMessageNew = true
	dataStore.groupMemberIDs = []uint64{1, 2}
	tracker := newFakeGroupReadTracker()
	tracker.membersErr = errors.New("redis unavailable")

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithGroupReadTracker(tracker)).Register(router)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/messages", map[string]string{
		"body":              "hello",
		"client_message_id": "group-client-1",
	}, authTestCookie())
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if dataStore.groupMemberLookups != 1 {
		t.Fatalf("group member MySQL lookups = %d, want 1", dataStore.groupMemberLookups)
	}
}

func TestFindUserCachesLookup(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore).Register(router)

	for range 2 {
		recorder := doJSON(t, router, http.MethodGet, "/api/users/BOB", nil, authTestCookie())
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
	}
	if dataStore.userLookups != 1 {
		t.Fatalf("user lookups = %d, want 1", dataStore.userLookups)
	}
}

func TestRequireUserCacheSurvivesButLogoutInvalidates(t *testing.T) {
	dataStore := authenticatedFriendStore()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	api := New(dataStore)
	api.Register(router)

	first := doJSON(t, router, http.MethodGet, "/api/me", nil, authTestCookie())
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d", first.Code)
	}
	second := doJSON(t, router, http.MethodGet, "/api/me", nil, authTestCookie())
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d", second.Code)
	}
	if dataStore.sessionLookups != 1 {
		t.Fatalf("session lookups before logout = %d, want 1", dataStore.sessionLookups)
	}

	logout := doJSON(t, router, http.MethodPost, "/api/auth/logout", nil, authTestCookie())
	if logout.Code != http.StatusNoContent {
		t.Fatalf("logout status = %d", logout.Code)
	}
	// The fake lookups still return an active session, so this only proves the
	// cache was bypassed after logout, not that the real database revoked it.
	after := doJSON(t, router, http.MethodGet, "/api/me", nil, authTestCookie())
	if after.Code != http.StatusOK {
		t.Fatalf("after logout status = %d", after.Code)
	}
	if dataStore.sessionLookups != 2 {
		t.Fatalf("session lookups after logout = %d, want 2", dataStore.sessionLookups)
	}
}

func TestUpdatePasswordInvalidatesSessionCache(t *testing.T) {
	oldHash, err := auth.HashPassword("oldpassword")
	if err != nil {
		t.Fatalf("hash old password: %v", err)
	}
	dataStore := authenticatedFriendStore()
	dataStore.fullUser = store.User{
		ID:           1,
		Username:     "alice",
		Nickname:     "Alice",
		PasswordHash: oldHash,
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore).Register(router)

	if recorder := doJSON(t, router, http.MethodGet, "/api/me", nil, authTestCookie()); recorder.Code != http.StatusOK {
		t.Fatalf("warmup status = %d", recorder.Code)
	}
	if recorder := doJSON(t, router, http.MethodGet, "/api/me", nil, authTestCookie()); recorder.Code != http.StatusOK {
		t.Fatalf("cached status = %d", recorder.Code)
	}
	if dataStore.sessionLookups != 1 {
		t.Fatalf("session lookups before password change = %d, want 1", dataStore.sessionLookups)
	}

	recorder := doJSON(t, router, http.MethodPut, "/api/me/password", map[string]string{
		"old_password": "oldpassword",
		"new_password": "newpassword123",
	}, authTestCookie())
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("password status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	// The fake session stays active, so this checks that the cache was bypassed.
	after := doJSON(t, router, http.MethodGet, "/api/me", nil, authTestCookie())
	if after.Code != http.StatusOK {
		t.Fatalf("after password change status = %d", after.Code)
	}
	if dataStore.sessionLookups != 2 {
		t.Fatalf("session lookups after password change = %d, want 2", dataStore.sessionLookups)
	}
}
