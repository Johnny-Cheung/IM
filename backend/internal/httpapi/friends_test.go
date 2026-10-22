package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func authenticatedFriendStore() *fakeStore {
	return &fakeStore{
		session: store.SessionUser{
			ExpiresAt: time.Now().Add(time.Hour),
			User:      store.User{ID: 1, Username: "alice", Nickname: "Alice"},
		},
	}
}

func authTestCookie() *http.Cookie {
	return &http.Cookie{Name: sessionCookieName, Value: "token"}
}

func decodeBody[T any](t *testing.T, recorder *httptest.ResponseRecorder) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(recorder.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return value
}

func TestLookupUser(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.user = store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore.relationship = store.RelationshipNone
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodGet, "/api/users/BOB", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	response := decodeBody[userLookupResponse](t, recorder)
	if response.Username != "bob" || response.Nickname != "Bob" || response.Relationship != "none" {
		t.Fatalf("response = %#v, want bob/Bob/none", response)
	}
}

func TestLookupSelf(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.user = store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	dataStore.relationship = store.RelationshipSelf
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodGet, "/api/users/alice", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	response := decodeBody[userLookupResponse](t, recorder)
	if response.Relationship != "self" {
		t.Fatalf("relationship = %q, want self", response.Relationship)
	}
}

func TestLookupUserNotFound(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.userErr = store.ErrUserNotFound
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodGet, "/api/users/nobody", nil, authTestCookie())
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	if got := errorCode(t, recorder); got != "user_not_found" {
		t.Fatalf("error code = %q, want user_not_found", got)
	}
}

func TestSendFriendRequestPending(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.user = store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore.createFriendResult = store.FriendRequestResult{RequestID: 11}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/friend-requests", map[string]string{
		"username": "bob",
	}, authTestCookie())
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	response := decodeBody[sendFriendRequestResponse](t, recorder)
	if response.Status != "pending" || response.RequestID != 11 {
		t.Fatalf("response = %#v, want pending/11", response)
	}
	if dataStore.createdFriendRequest.fromID != 1 || dataStore.createdFriendRequest.toID != 2 {
		t.Fatalf("created request = %#v, want 1 -> 2", dataStore.createdFriendRequest)
	}
}

func TestSendFriendRequestAutoAccepted(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.user = store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore.createFriendResult = store.FriendRequestResult{
		Accepted: true,
		Friend:   store.PublicUser{Username: "bob", Nickname: "Bob"},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/friend-requests", map[string]string{
		"username": "bob",
	}, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	response := decodeBody[sendFriendRequestResponse](t, recorder)
	if response.Status != "accepted" || response.Friend == nil || response.Friend.Username != "bob" {
		t.Fatalf("response = %#v, want accepted/bob", response)
	}
}

func TestSendFriendRequestCannotAddSelf(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.user = store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/friend-requests", map[string]string{
		"username": "alice",
	}, authTestCookie())
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if got := errorCode(t, recorder); got != "cannot_add_self" {
		t.Fatalf("error code = %q, want cannot_add_self", got)
	}
	if dataStore.createdFriendRequest.fromID != 0 {
		t.Fatal("friend request was created for self")
	}
}

func TestSendFriendRequestErrors(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode string
	}{
		{name: "already friends", err: store.ErrAlreadyFriends, wantCode: "already_friends"},
		{name: "request exists", err: store.ErrFriendRequestExists, wantCode: "request_exists"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataStore := authenticatedFriendStore()
			dataStore.user = store.User{ID: 2, Username: "bob", Nickname: "Bob"}
			dataStore.createFriendErr = tt.err
			router := testRouter(dataStore)

			recorder := doJSON(t, router, http.MethodPost, "/api/friend-requests", map[string]string{
				"username": "bob",
			}, authTestCookie())
			if recorder.Code != http.StatusConflict {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
			}
			if got := errorCode(t, recorder); got != tt.wantCode {
				t.Fatalf("error code = %q, want %q", got, tt.wantCode)
			}
		})
	}
}

func TestListFriendRequests(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedFriendStore()
	dataStore.friendRequests = store.FriendRequests{
		Incoming: []store.FriendRequest{{
			ID:        5,
			User:      store.PublicUser{Username: "bob", Nickname: "Bob"},
			CreatedAt: now,
		}},
		Outgoing: []store.FriendRequest{{
			ID:        6,
			User:      store.PublicUser{Username: "carol", Nickname: "Carol"},
			CreatedAt: now,
		}},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodGet, "/api/friend-requests", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	response := decodeBody[friendRequestsResponse](t, recorder)
	if len(response.Incoming) != 1 || response.Incoming[0].ID != 5 ||
		len(response.Outgoing) != 1 || response.Outgoing[0].ID != 6 {
		t.Fatalf("response = %#v, want incoming 5 and outgoing 6", response)
	}
}

func TestAcceptFriendRequest(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.acceptFriendResult = store.PublicUser{Username: "bob", Nickname: "Bob"}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/friend-requests/5/accept", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	response := decodeBody[acceptFriendResponse](t, recorder)
	if response.Friend.Username != "bob" {
		t.Fatalf("friend = %#v, want bob", response.Friend)
	}
	if dataStore.acceptedFriendRequest.userID != 1 || dataStore.acceptedFriendRequest.requestID != 5 {
		t.Fatalf("accepted request = %#v, want user 1 request 5", dataStore.acceptedFriendRequest)
	}
}

func TestAcceptFriendRequestNotFound(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.acceptFriendErr = store.ErrFriendRequestNotFound
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/friend-requests/5/accept", nil, authTestCookie())
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	if got := errorCode(t, recorder); got != "request_not_found" {
		t.Fatalf("error code = %q, want request_not_found", got)
	}
}

func TestAcceptFriendRequestInvalidID(t *testing.T) {
	router := testRouter(authenticatedFriendStore())

	recorder := doJSON(t, router, http.MethodPost, "/api/friend-requests/abc/accept", nil, authTestCookie())
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if got := errorCode(t, recorder); got != "invalid_request" {
		t.Fatalf("error code = %q, want invalid_request", got)
	}
}

func TestRejectFriendRequest(t *testing.T) {
	dataStore := authenticatedFriendStore()
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/friend-requests/5/reject", nil, authTestCookie())
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if dataStore.rejectedFriendRequest.userID != 1 || dataStore.rejectedFriendRequest.requestID != 5 {
		t.Fatalf("rejected request = %#v, want user 1 request 5", dataStore.rejectedFriendRequest)
	}
}

func TestListFriends(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.friends = []store.PublicUser{
		{Username: "bob", Nickname: "Bob"},
		{Username: "carol", Nickname: "Carol"},
	}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodGet, "/api/friends", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	response := decodeBody[friendsResponse](t, recorder)
	if len(response.Friends) != 2 || response.Friends[0].Username != "bob" {
		t.Fatalf("friends = %#v, want bob and carol", response.Friends)
	}
}

func TestDeleteFriend(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.user = store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodDelete, "/api/friends/bob", nil, authTestCookie())
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if dataStore.deletedFriend.userID != 1 || dataStore.deletedFriend.friendID != 2 {
		t.Fatalf("deleted friend = %#v, want user 1 friend 2", dataStore.deletedFriend)
	}
}

func TestDeleteFriendNotFound(t *testing.T) {
	dataStore := authenticatedFriendStore()
	dataStore.user = store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore.deleteFriendErr = store.ErrFriendNotFound
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodDelete, "/api/friends/bob", nil, authTestCookie())
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	if got := errorCode(t, recorder); got != "friend_not_found" {
		t.Fatalf("error code = %q, want friend_not_found", got)
	}
}

func TestFriendEndpointsRequireSession(t *testing.T) {
	tests := []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/api/users/bob"},
		{method: http.MethodGet, path: "/api/friends"},
		{method: http.MethodPost, path: "/api/friend-requests"},
		{method: http.MethodGet, path: "/api/friend-requests"},
		{method: http.MethodDelete, path: "/api/friends/bob"},
	}

	router := testRouter(&fakeStore{})
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			recorder := doJSON(t, router, tt.method, tt.path, nil, nil)
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
			}
			if got := errorCode(t, recorder); got != "unauthenticated" {
				t.Fatalf("error code = %q, want unauthenticated", got)
			}
		})
	}
}
