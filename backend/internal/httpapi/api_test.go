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

func (f *fakeStore) GetUserByUsernameKey(context.Context, string) (store.User, error) {
	if f.userErr != nil {
		return store.User{}, f.userErr
	}
	return f.user, nil
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

func testRouter(dataStore Store) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore).Register(router)
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
