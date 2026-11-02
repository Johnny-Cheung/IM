package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
)

func bootstrapTestStore() *fakeStore {
	dataStore := authenticatedFriendStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	return dataStore
}

func TestBootstrapReturnsMetadataOnly(t *testing.T) {
	now := time.Now().UTC()
	dataStore := bootstrapTestStore()
	dataStore.friends = []store.PublicUser{{Username: "bob", Nickname: "Bob"}}
	dataStore.conversations = []store.Conversation{{
		User:     store.PublicUser{Username: "bob", Nickname: "Bob"},
		IsFriend: true,
		LastSeq:  10,
		LastMessage: &store.Message{
			ID:              10,
			Seq:             10,
			SenderID:        2,
			RecipientID:     1,
			ClientMessageID: "client-00000010",
			Body:            "older",
			CreatedAt:       now.Add(-time.Minute),
		},
	}}
	dataStore.groupConversations = []store.GroupConversation{{
		Group: store.Group{
			ID:        9,
			Name:      "Team",
			OwnerID:   1,
			CreatedAt: now.Add(-time.Hour),
		},
		MemberCount: 2,
		IsOwner:     true,
		LastSeq:     22,
		LastMessage: &store.GroupMessage{
			ID:              22,
			Seq:             22,
			GroupID:         9,
			SenderID:        2,
			Sender:          store.PublicUser{Username: "bob", Nickname: "Bob"},
			ClientMessageID: "client-00000022",
			Body:            "group newer",
			CreatedAt:       now,
		},
	}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore).Register(router)

	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/bootstrap",
		map[string]any{},
		authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	response := decodeBody[bootstrapResponse](t, recorder)
	if response.User.Username != "alice" || response.User.Nickname != "Alice" {
		t.Fatalf("user = %#v", response.User)
	}
	if len(response.Conversations) != 2 {
		t.Fatalf("conversations = %d, want 2", len(response.Conversations))
	}
	if len(response.Friends) != 1 || response.Friends[0].Username != "bob" {
		t.Fatalf("friends = %#v", response.Friends)
	}
	if len(response.Partial) != 0 {
		t.Fatalf("partial = %#v, want empty", response.Partial)
	}
	for _, conversation := range response.Conversations {
		if conversation.Type == "group" && conversation.LastSeq != 22 {
			t.Fatalf("group last_seq = %#v", conversation)
		}
		if conversation.Type == "direct" && conversation.LastSeq != 10 {
			t.Fatalf("direct last_seq = %#v", conversation)
		}
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw response: %v", err)
	}
	for _, removed := range []string{"direct", "groups"} {
		if _, exists := raw[removed]; exists {
			t.Fatalf("bootstrap still returns %q: %s", removed, recorder.Body.String())
		}
	}
}

func TestBootstrapReturnsPartialSections(t *testing.T) {
	dataStore := bootstrapTestStore()
	dataStore.friendsErr = errors.New("database unavailable")
	dataStore.friends = []store.PublicUser{{Username: "bob", Nickname: "Bob"}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore).Register(router)

	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/bootstrap",
		map[string]any{},
		authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	response := decodeBody[bootstrapResponse](t, recorder)
	if len(response.Friends) != 0 {
		t.Fatalf("friends = %#v, want empty", response.Friends)
	}
	if !containsString(response.Partial, "friends") {
		t.Fatalf("partial = %#v, want friends", response.Partial)
	}
	if containsString(response.Partial, "direct") || containsString(response.Partial, "groups") {
		t.Fatalf("partial = %#v, must not contain removed sections", response.Partial)
	}
}

func TestBootstrapRequiresSession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(bootstrapTestStore()).Register(router)

	recorder := doJSON(t, router, http.MethodPost, "/api/bootstrap", map[string]any{}, nil)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}

func TestMessagesSyncRouteRemoved(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(bootstrapTestStore()).Register(router)

	recorder := doJSON(
		t,
		router,
		http.MethodGet,
		"/api/messages/sync?after_id=1",
		nil,
		authTestCookie(),
	)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
