package httpapi

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/auth"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func TestUpdateNickname(t *testing.T) {
	dataStore := &fakeStore{
		session: store.SessionUser{
			ExpiresAt: time.Now().Add(time.Hour),
			User:      store.User{ID: 3, Username: "alice", Nickname: "Old"},
		},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPatch, "/api/me", map[string]string{
		"nickname": "  New Nickname  ",
	}, &http.Cookie{Name: sessionCookieName, Value: "token"})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var response userResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Username != "alice" || response.Nickname != "New Nickname" {
		t.Fatalf("response = %#v, want alice/New Nickname", response)
	}
	if dataStore.updatedNickname.userID != 3 || dataStore.updatedNickname.nickname != "New Nickname" {
		t.Fatalf("updated nickname = %#v, want user 3 / New Nickname", dataStore.updatedNickname)
	}
}

func TestUpdateNicknameRejectsInvalid(t *testing.T) {
	dataStore := &fakeStore{
		session: store.SessionUser{
			ExpiresAt: time.Now().Add(time.Hour),
			User:      store.User{ID: 3, Username: "alice", Nickname: "Old"},
		},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPatch, "/api/me", map[string]string{
		"nickname": "   ",
	}, &http.Cookie{Name: sessionCookieName, Value: "token"})

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if got := errorCode(t, recorder); got != "invalid_nickname" {
		t.Fatalf("error code = %q, want invalid_nickname", got)
	}
	if dataStore.updatedNickname.userID != 0 {
		t.Fatal("nickname was updated for an invalid input")
	}
}

func TestUpdatePassword(t *testing.T) {
	oldHash, err := auth.HashPassword("oldpassword")
	if err != nil {
		t.Fatalf("hash old password: %v", err)
	}
	dataStore := &fakeStore{
		session: store.SessionUser{
			ExpiresAt: time.Now().Add(time.Hour),
			User:      store.User{ID: 3, Username: "alice", Nickname: "Alice"},
		},
		fullUser: store.User{
			ID:           3,
			Username:     "alice",
			Nickname:     "Alice",
			PasswordHash: oldHash,
		},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPut, "/api/me/password", map[string]string{
		"old_password": "oldpassword",
		"new_password": "newpassword123",
	}, &http.Cookie{Name: sessionCookieName, Value: "token"})

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if dataStore.passwordChange.userID != 3 {
		t.Fatalf("password change user id = %d, want 3", dataStore.passwordChange.userID)
	}
	if !auth.CheckPassword(dataStore.passwordChange.passwordHash, "newpassword123") {
		t.Fatal("stored password hash does not match the new password")
	}
	cookie := responseCookie(t, recorder, sessionCookieName)
	if cookie == nil || cookie.Value != "" {
		t.Fatalf("cleared cookie = %#v, want empty value", cookie)
	}
	if !cookie.Expires.Before(time.Now()) {
		t.Fatalf("cleared cookie expiry = %v, want in the past", cookie.Expires)
	}
}

func TestUpdatePasswordRejectsWrongOldPassword(t *testing.T) {
	oldHash, err := auth.HashPassword("oldpassword")
	if err != nil {
		t.Fatalf("hash old password: %v", err)
	}
	dataStore := &fakeStore{
		session: store.SessionUser{
			ExpiresAt: time.Now().Add(time.Hour),
			User:      store.User{ID: 3, Username: "alice", Nickname: "Alice"},
		},
		fullUser: store.User{ID: 3, PasswordHash: oldHash},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPut, "/api/me/password", map[string]string{
		"old_password": "wrong-password",
		"new_password": "newpassword123",
	}, &http.Cookie{Name: sessionCookieName, Value: "token"})

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if got := errorCode(t, recorder); got != "invalid_old_password" {
		t.Fatalf("error code = %q, want invalid_old_password", got)
	}
	if dataStore.passwordChange.userID != 0 {
		t.Fatal("password was changed with a wrong old password")
	}
}

func TestUpdatePasswordRejectsInvalidNewPassword(t *testing.T) {
	dataStore := &fakeStore{
		session: store.SessionUser{
			ExpiresAt: time.Now().Add(time.Hour),
			User:      store.User{ID: 3, Username: "alice", Nickname: "Alice"},
		},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPut, "/api/me/password", map[string]string{
		"old_password": "oldpassword",
		"new_password": "short",
	}, &http.Cookie{Name: sessionCookieName, Value: "token"})

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if got := errorCode(t, recorder); got != "invalid_password" {
		t.Fatalf("error code = %q, want invalid_password", got)
	}
	if dataStore.passwordChange.userID != 0 {
		t.Fatal("password was changed with an invalid new password")
	}
}

func TestUpdatePasswordRequiresSession(t *testing.T) {
	router := testRouter(&fakeStore{})

	recorder := doJSON(t, router, http.MethodPut, "/api/me/password", map[string]string{
		"old_password": "oldpassword",
		"new_password": "newpassword123",
	}, nil)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
	if got := errorCode(t, recorder); got != "unauthenticated" {
		t.Fatalf("error code = %q, want unauthenticated", got)
	}
}
