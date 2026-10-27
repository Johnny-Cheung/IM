package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/auth"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
)

type fakeStore struct {
	user             store.User
	userErr          error
	createUserErr    error
	session          store.SessionUser
	sessionErr       error
	createSessionErr error
	revokeSessionErr error

	fullUser          store.User
	fullUserErr       error
	updateNicknameErr error
	updatePasswordErr error

	updatedNickname struct {
		userID   uint64
		nickname string
	}
	passwordChange struct {
		userID       uint64
		passwordHash string
		revokedAt    time.Time
	}

	relationship       store.Relationship
	relationshipErr    error
	friends            []store.PublicUser
	friendsErr         error
	friendRequests     store.FriendRequests
	friendRequestsErr  error
	createFriendResult store.FriendRequestResult
	createFriendErr    error
	acceptFriendResult store.PublicUser
	acceptFriendErr    error
	rejectFriendErr    error
	deleteFriendErr    error

	createdFriendRequest struct {
		fromID uint64
		toID   uint64
		now    time.Time
	}
	acceptedFriendRequest struct {
		userID    uint64
		requestID uint64
		now       time.Time
	}
	rejectedFriendRequest struct {
		userID    uint64
		requestID uint64
		now       time.Time
	}
	deletedFriend struct {
		userID   uint64
		friendID uint64
	}

	conversations     []store.Conversation
	conversationsErr  error
	messagePage       store.MessagePage
	messagePageErr    error
	messageAfterPage  store.MessagePage
	messageAfterErr   error
	markReadResult    uint64
	markReadAdvanced  bool
	markReadErr       error
	createdMessage    store.Message
	createdMessageNew bool
	createMessageErr  error
	createMessageCall struct {
		senderID        uint64
		recipientID     uint64
		clientMessageID string
		body            string
		now             time.Time
	}
	listMessagesCall struct {
		userID   uint64
		otherID  uint64
		beforeID uint64
		limit    int
	}
	listMessagesAfterCall struct {
		userID  uint64
		otherID uint64
		afterID uint64
		limit   int
	}
	markReadCall struct {
		userID    uint64
		peerID    uint64
		throughID uint64
		now       time.Time
	}

	groupConversations    []store.GroupConversation
	groupConversationsErr error
	groupMember           bool
	groupMessageCall      struct {
		userID   uint64
		groupID  uint64
		beforeID uint64
		limit    int
	}
	usersByKey      map[string]store.User
	groupDetail     store.GroupDetail
	groupDetailErr  error
	createGroupErr  error
	createGroupCall struct {
		ownerID   uint64
		name      string
		memberIDs []uint64
		now       time.Time
	}
	renameGroupErr  error
	inviteGroupErr  error
	inviteGroupCall struct {
		groupID   uint64
		inviterID uint64
		memberIDs []uint64
		now       time.Time
	}
	kickGroupErr           error
	leaveGroupDissolved    bool
	leaveGroupErr          error
	groupMemberIDs         []uint64
	groupMemberIDsErr      error
	groupMessagePage       store.GroupMessagePage
	groupMessagePageErr    error
	groupMessageAfterPage  store.GroupMessagePage
	groupMessageAfterErr   error
	createdGroupMessage    store.GroupMessage
	createdGroupMessageNew bool
	createGroupMessageErr  error
	createGroupMessageCall struct {
		senderID        uint64
		groupID         uint64
		clientMessageID string
		body            string
		now             time.Time
	}
	markGroupReadThrough  uint64
	markGroupReadResult   uint64
	markGroupReadAdvanced bool
	markGroupReadErr      error

	createdSession struct {
		userID    uint64
		tokenHash []byte
		createdAt time.Time
		expiresAt time.Time
	}
	revoked struct {
		tokenHash []byte
		reason    string
		at        time.Time
	}
}

func (f *fakeStore) CreateUser(
	_ context.Context,
	username, _, nickname, passwordHash string,
) (store.User, error) {
	if f.createUserErr != nil {
		return store.User{}, f.createUserErr
	}
	f.user = store.User{
		ID:           1,
		Username:     username,
		Nickname:     nickname,
		PasswordHash: passwordHash,
	}
	return f.user, nil
}

func (f *fakeStore) GetUserByUsernameKey(_ context.Context, usernameKey string) (store.User, error) {
	if f.userErr != nil {
		return store.User{}, f.userErr
	}
	if f.usersByKey != nil {
		user, ok := f.usersByKey[usernameKey]
		if !ok {
			return store.User{}, store.ErrUserNotFound
		}
		return user, nil
	}
	return f.user, nil
}

func (f *fakeStore) GetUserByID(context.Context, uint64) (store.User, error) {
	if f.fullUserErr != nil {
		return store.User{}, f.fullUserErr
	}
	return f.fullUser, nil
}

func (f *fakeStore) UpdateNickname(_ context.Context, userID uint64, nickname string) error {
	f.updatedNickname.userID = userID
	f.updatedNickname.nickname = nickname
	return f.updateNicknameErr
}

func (f *fakeStore) UpdatePasswordAndRevokeSessions(
	_ context.Context,
	userID uint64,
	passwordHash string,
	revokedAt time.Time,
) error {
	f.passwordChange.userID = userID
	f.passwordChange.passwordHash = passwordHash
	f.passwordChange.revokedAt = revokedAt
	return f.updatePasswordErr
}

func (f *fakeStore) GetRelationship(
	context.Context,
	uint64,
	uint64,
) (store.Relationship, error) {
	return f.relationship, f.relationshipErr
}

func (f *fakeStore) ListFriends(context.Context, uint64) ([]store.PublicUser, error) {
	return f.friends, f.friendsErr
}

func (f *fakeStore) ListFriendRequests(
	context.Context,
	uint64,
) (store.FriendRequests, error) {
	return f.friendRequests, f.friendRequestsErr
}

func (f *fakeStore) CreateFriendRequest(
	_ context.Context,
	fromID, toID uint64,
	now time.Time,
) (store.FriendRequestResult, error) {
	f.createdFriendRequest.fromID = fromID
	f.createdFriendRequest.toID = toID
	f.createdFriendRequest.now = now
	return f.createFriendResult, f.createFriendErr
}

func (f *fakeStore) AcceptFriendRequest(
	_ context.Context,
	userID, requestID uint64,
	now time.Time,
) (store.PublicUser, error) {
	f.acceptedFriendRequest.userID = userID
	f.acceptedFriendRequest.requestID = requestID
	f.acceptedFriendRequest.now = now
	return f.acceptFriendResult, f.acceptFriendErr
}

func (f *fakeStore) RejectFriendRequest(
	_ context.Context,
	userID, requestID uint64,
	now time.Time,
) error {
	f.rejectedFriendRequest.userID = userID
	f.rejectedFriendRequest.requestID = requestID
	f.rejectedFriendRequest.now = now
	return f.rejectFriendErr
}

func (f *fakeStore) DeleteFriend(_ context.Context, userID, friendID uint64) error {
	f.deletedFriend.userID = userID
	f.deletedFriend.friendID = friendID
	return f.deleteFriendErr
}

func (f *fakeStore) ListConversations(
	context.Context,
	uint64,
) ([]store.Conversation, error) {
	return f.conversations, f.conversationsErr
}

func (f *fakeStore) ListMessages(
	_ context.Context,
	userID, otherID, beforeID uint64,
	limit int,
) (store.MessagePage, error) {
	f.listMessagesCall.userID = userID
	f.listMessagesCall.otherID = otherID
	f.listMessagesCall.beforeID = beforeID
	f.listMessagesCall.limit = limit
	return f.messagePage, f.messagePageErr
}

func (f *fakeStore) ListMessagesAfter(
	_ context.Context,
	userID, otherID, afterID uint64,
	limit int,
) (store.MessagePage, error) {
	f.listMessagesAfterCall.userID = userID
	f.listMessagesAfterCall.otherID = otherID
	f.listMessagesAfterCall.afterID = afterID
	f.listMessagesAfterCall.limit = limit
	return f.messageAfterPage, f.messageAfterErr
}

func (f *fakeStore) MarkConversationRead(
	_ context.Context,
	userID, peerID, throughID uint64,
	now time.Time,
) (uint64, bool, error) {
	f.markReadCall.userID = userID
	f.markReadCall.peerID = peerID
	f.markReadCall.throughID = throughID
	f.markReadCall.now = now
	return f.markReadResult, f.markReadAdvanced, f.markReadErr
}

func (f *fakeStore) CreateMessage(
	_ context.Context,
	senderID, recipientID uint64,
	clientMessageID, body string,
	now time.Time,
) (store.Message, bool, error) {
	f.createMessageCall.senderID = senderID
	f.createMessageCall.recipientID = recipientID
	f.createMessageCall.clientMessageID = clientMessageID
	f.createMessageCall.body = body
	f.createMessageCall.now = now
	return f.createdMessage, f.createdMessageNew, f.createMessageErr
}

func (f *fakeStore) CreateGroup(
	_ context.Context,
	ownerID uint64,
	name string,
	memberIDs []uint64,
	now time.Time,
) (store.GroupDetail, error) {
	f.createGroupCall.ownerID = ownerID
	f.createGroupCall.name = name
	f.createGroupCall.memberIDs = memberIDs
	f.createGroupCall.now = now
	return f.groupDetail, f.createGroupErr
}

func (f *fakeStore) ListGroupConversations(
	context.Context,
	uint64,
) ([]store.GroupConversation, error) {
	return f.groupConversations, f.groupConversationsErr
}

func (f *fakeStore) GetGroup(context.Context, uint64, uint64) (store.GroupDetail, error) {
	return f.groupDetail, f.groupDetailErr
}

func (f *fakeStore) RenameGroup(context.Context, uint64, uint64, string) error {
	return f.renameGroupErr
}

func (f *fakeStore) InviteGroupMembers(
	_ context.Context,
	groupID, inviterID uint64,
	memberIDs []uint64,
	now time.Time,
) error {
	f.inviteGroupCall.groupID = groupID
	f.inviteGroupCall.inviterID = inviterID
	f.inviteGroupCall.memberIDs = memberIDs
	f.inviteGroupCall.now = now
	return f.inviteGroupErr
}

func (f *fakeStore) KickGroupMember(context.Context, uint64, uint64, uint64) error {
	return f.kickGroupErr
}

func (f *fakeStore) LeaveGroup(context.Context, uint64, uint64, time.Time) (bool, error) {
	return f.leaveGroupDissolved, f.leaveGroupErr
}

func (f *fakeStore) ListGroupMemberIDs(context.Context, uint64) ([]uint64, error) {
	return f.groupMemberIDs, f.groupMemberIDsErr
}

func (f *fakeStore) ListPublicUsersByIDs(
	_ context.Context,
	ids []uint64,
) (map[uint64]store.PublicUser, error) {
	result := make(map[uint64]store.PublicUser, len(ids))
	for _, id := range ids {
		for _, user := range f.usersByKey {
			if user.ID == id {
				result[id] = store.PublicUser{
					Username: user.Username,
					Nickname: user.Nickname,
				}
				break
			}
		}
	}
	return result, nil
}

func (f *fakeStore) IsGroupMember(
	context.Context,
	uint64,
	uint64,
) (bool, error) {
	return f.groupMember, nil
}

func (f *fakeStore) ListGroupMessages(
	_ context.Context,
	userID, groupID, beforeID uint64,
	limit int,
) (store.GroupMessagePage, error) {
	f.groupMessageCall.userID = userID
	f.groupMessageCall.groupID = groupID
	f.groupMessageCall.beforeID = beforeID
	f.groupMessageCall.limit = limit
	return f.groupMessagePage, f.groupMessagePageErr
}

func (f *fakeStore) ListGroupMessagesAfter(
	context.Context,
	uint64,
	uint64,
	uint64,
	int,
) (store.GroupMessagePage, error) {
	return f.groupMessageAfterPage, f.groupMessageAfterErr
}

func (f *fakeStore) CreateGroupMessage(
	_ context.Context,
	senderID, groupID uint64,
	clientMessageID, body string,
	now time.Time,
) (store.GroupMessage, bool, error) {
	f.createGroupMessageCall.senderID = senderID
	f.createGroupMessageCall.groupID = groupID
	f.createGroupMessageCall.clientMessageID = clientMessageID
	f.createGroupMessageCall.body = body
	f.createGroupMessageCall.now = now
	return f.createdGroupMessage, f.createdGroupMessageNew, f.createGroupMessageErr
}

func (f *fakeStore) MarkGroupRead(_ context.Context, _, _, throughID uint64) (uint64, bool, error) {
	f.markGroupReadThrough = throughID
	return f.markGroupReadResult, f.markGroupReadAdvanced, f.markGroupReadErr
}

func (f *fakeStore) CreateSession(
	_ context.Context,
	userID uint64,
	tokenHash []byte,
	createdAt, expiresAt time.Time,
) error {
	f.createdSession.userID = userID
	f.createdSession.tokenHash = tokenHash
	f.createdSession.createdAt = createdAt
	f.createdSession.expiresAt = expiresAt
	return f.createSessionErr
}

func (f *fakeStore) GetSessionWithUser(context.Context, []byte) (store.SessionUser, error) {
	if f.sessionErr != nil {
		return store.SessionUser{}, f.sessionErr
	}
	return f.session, nil
}

func (f *fakeStore) RevokeSession(
	_ context.Context,
	tokenHash []byte,
	reason string,
	revokedAt time.Time,
) error {
	f.revoked.tokenHash = tokenHash
	f.revoked.reason = reason
	f.revoked.at = revokedAt
	return f.revokeSessionErr
}

func testRouter(dataStore Store, options ...Option) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, options...).Register(router)
	return router
}

func doJSON(
	t *testing.T,
	router *gin.Engine,
	method, path string,
	body any,
	cookie *http.Cookie,
) *httptest.ResponseRecorder {
	t.Helper()

	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		reader = bytes.NewReader(payload)
	}

	request := httptest.NewRequest(method, path, reader)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if cookie != nil {
		request.AddCookie(cookie)
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func responseCookie(t *testing.T, recorder *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	for _, cookie := range recorder.Result().Cookies() {
		if cookie.Name == name {
			return cookie
		}
	}
	return nil
}

func errorCode(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	return response.Error.Code
}

func TestRegisterCreatesSession(t *testing.T) {
	dataStore := &fakeStore{}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/auth/register", map[string]string{
		"username": " Alice ",
		"nickname": " Alice ",
		"password": "password123",
	}, nil)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	cookie := responseCookie(t, recorder, sessionCookieName)
	if cookie == nil || cookie.Value == "" || !cookie.HttpOnly {
		t.Fatalf("session cookie = %#v, want non-empty HttpOnly cookie", cookie)
	}
	if dataStore.createdSession.userID != 1 {
		t.Fatalf("session user id = %d, want 1", dataStore.createdSession.userID)
	}
	if len(dataStore.createdSession.tokenHash) != 32 {
		t.Fatalf("session token hash length = %d, want 32", len(dataStore.createdSession.tokenHash))
	}
	if !auth.CheckPassword(dataStore.user.PasswordHash, "password123") {
		t.Fatal("stored password hash does not match password")
	}

	var response userResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Username != "Alice" || response.Nickname != "Alice" {
		t.Fatalf("response = %#v, want trimmed Alice", response)
	}
}

func TestRegisterRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		username string
		nickname string
		password string
		wantCode string
	}{
		{name: "invalid username", username: "ab", nickname: "Alice", password: "password123", wantCode: "invalid_username"},
		{name: "invalid nickname", username: "alice", nickname: "   ", password: "password123", wantCode: "invalid_nickname"},
		{name: "invalid password", username: "alice", nickname: "Alice", password: "short", wantCode: "invalid_password"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataStore := &fakeStore{}
			router := testRouter(dataStore)
			recorder := doJSON(t, router, http.MethodPost, "/api/auth/register", map[string]string{
				"username": tt.username,
				"nickname": tt.nickname,
				"password": tt.password,
			}, nil)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
			}
			if got := errorCode(t, recorder); got != tt.wantCode {
				t.Fatalf("error code = %q, want %q", got, tt.wantCode)
			}
			if cookie := responseCookie(t, recorder, sessionCookieName); cookie != nil {
				t.Fatalf("unexpected session cookie: %#v", cookie)
			}
		})
	}
}

func TestRegisterRejectsMalformedJSON(t *testing.T) {
	router := testRouter(&fakeStore{})
	request := httptest.NewRequest(http.MethodPost, "/api/auth/register", strings.NewReader("{"))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if got := errorCode(t, recorder); got != "invalid_request" {
		t.Fatalf("error code = %q, want invalid_request", got)
	}
}

func TestRegisterRejectsDuplicateUsername(t *testing.T) {
	dataStore := &fakeStore{createUserErr: store.ErrUsernameTaken}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/auth/register", map[string]string{
		"username": "alice",
		"nickname": "Alice",
		"password": "password123",
	}, nil)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	if got := errorCode(t, recorder); got != "username_taken" {
		t.Fatalf("error code = %q, want username_taken", got)
	}
}

func TestLoginCreatesSession(t *testing.T) {
	passwordHash, err := auth.HashPassword("password123")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	dataStore := &fakeStore{
		user: store.User{
			ID:           7,
			Username:     "alice",
			Nickname:     "Alice",
			PasswordHash: passwordHash,
		},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/auth/login", map[string]string{
		"username": "ALICE",
		"password": "password123",
	}, nil)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if cookie := responseCookie(t, recorder, sessionCookieName); cookie == nil || cookie.Value == "" {
		t.Fatal("session cookie is missing")
	}
	if dataStore.createdSession.userID != 7 {
		t.Fatalf("session user id = %d, want 7", dataStore.createdSession.userID)
	}
}

func TestLoginRejectsWrongPassword(t *testing.T) {
	passwordHash, err := auth.HashPassword("password123")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	dataStore := &fakeStore{
		user: store.User{ID: 7, Username: "alice", Nickname: "Alice", PasswordHash: passwordHash},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/auth/login", map[string]string{
		"username": "alice",
		"password": "wrong-password",
	}, nil)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
	if got := errorCode(t, recorder); got != "invalid_credentials" {
		t.Fatalf("error code = %q, want invalid_credentials", got)
	}
	if dataStore.createdSession.userID != 0 {
		t.Fatal("session was created for a wrong password")
	}
}

func TestLoginRejectsUnknownUser(t *testing.T) {
	dataStore := &fakeStore{userErr: store.ErrUserNotFound}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/auth/login", map[string]string{
		"username": "nobody",
		"password": "password123",
	}, nil)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
	if got := errorCode(t, recorder); got != "invalid_credentials" {
		t.Fatalf("error code = %q, want invalid_credentials", got)
	}
	if dataStore.createdSession.userID != 0 {
		t.Fatal("session was created for an unknown user")
	}
}

func TestMeRequiresSession(t *testing.T) {
	router := testRouter(&fakeStore{})
	recorder := doJSON(t, router, http.MethodGet, "/api/me", nil, nil)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
	if got := errorCode(t, recorder); got != "unauthenticated" {
		t.Fatalf("error code = %q, want unauthenticated", got)
	}
}

func TestMeReturnsCurrentUser(t *testing.T) {
	token, _, err := auth.NewSessionToken()
	if err != nil {
		t.Fatalf("create session token: %v", err)
	}
	dataStore := &fakeStore{
		session: store.SessionUser{
			ExpiresAt: time.Now().Add(time.Hour),
			User:      store.User{Username: "alice", Nickname: "Alice"},
		},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodGet, "/api/me", nil, &http.Cookie{
		Name:  sessionCookieName,
		Value: token,
	})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var response userResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Username != "alice" || response.Nickname != "Alice" {
		t.Fatalf("response = %#v, want alice/Alice", response)
	}
}

func TestMeReportsReplacedSession(t *testing.T) {
	revokedAt := time.Now().Add(-time.Minute)
	dataStore := &fakeStore{
		session: store.SessionUser{
			ExpiresAt:    time.Now().Add(time.Hour),
			RevokedAt:    &revokedAt,
			RevokeReason: store.RevokeReasonReplaced,
			User:         store.User{Username: "alice", Nickname: "Alice"},
		},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodGet, "/api/me", nil, &http.Cookie{
		Name:  sessionCookieName,
		Value: "old-token",
	})

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
	if got := errorCode(t, recorder); got != "session_replaced" {
		t.Fatalf("error code = %q, want session_replaced", got)
	}
}

func TestMeReportsExpiredSession(t *testing.T) {
	dataStore := &fakeStore{
		session: store.SessionUser{
			ExpiresAt: time.Now().Add(-time.Minute),
			User:      store.User{Username: "alice", Nickname: "Alice"},
		},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodGet, "/api/me", nil, &http.Cookie{
		Name:  sessionCookieName,
		Value: "expired-token",
	})

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
	if got := errorCode(t, recorder); got != "session_expired" {
		t.Fatalf("error code = %q, want session_expired", got)
	}
}

func TestLogoutRevokesAndClearsCookie(t *testing.T) {
	token, tokenHash, err := auth.NewSessionToken()
	if err != nil {
		t.Fatalf("create session token: %v", err)
	}
	dataStore := &fakeStore{}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/auth/logout", nil, &http.Cookie{
		Name:  sessionCookieName,
		Value: token,
	})

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if dataStore.revoked.reason != store.RevokeReasonLogout {
		t.Fatalf("revoke reason = %q, want %q", dataStore.revoked.reason, store.RevokeReasonLogout)
	}
	if string(dataStore.revoked.tokenHash) != string(tokenHash) {
		t.Fatal("logout revoked a different token hash")
	}
	cookie := responseCookie(t, recorder, sessionCookieName)
	if cookie == nil || cookie.Value != "" {
		t.Fatalf("cleared cookie = %#v, want empty value", cookie)
	}
	if !cookie.Expires.Before(time.Now()) {
		t.Fatalf("cleared cookie expiry = %v, want in the past", cookie.Expires)
	}
}
