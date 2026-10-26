package httpapi

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
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

func TestBootstrapReturnsAllSections(t *testing.T) {
	now := time.Now().UTC()
	dataStore := bootstrapTestStore()
	dataStore.friends = []store.PublicUser{{Username: "bob", Nickname: "Bob"}}
	dataStore.conversations = []store.Conversation{{
		User:     store.PublicUser{Username: "bob", Nickname: "Bob"},
		IsFriend: true,
		LastMessage: &store.Message{
			ID:              10,
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
		LastMessage: &store.GroupMessage{
			ID:              22,
			GroupID:         9,
			SenderID:        2,
			Sender:          store.PublicUser{Username: "bob", Nickname: "Bob"},
			ClientMessageID: "client-00000022",
			Body:            "group newer",
			CreatedAt:       now,
		},
	}}
	direct := &directReaderStub{feed: messaging.FeedPage{
		Page: store.MessagePage{Messages: []store.Message{
			{
				ID:              11,
				SenderID:        2,
				RecipientID:     1,
				ClientMessageID: "client-00000011",
				Body:            "direct one",
				CreatedAt:       now,
			},
			{
				ID:              12,
				SenderID:        1,
				RecipientID:     2,
				ClientMessageID: "client-00000012",
				Body:            "direct two",
				CreatedAt:       now.Add(time.Second),
			},
		}},
		Complete: true,
	}}
	group := &groupReaderStub{page: messaging.GroupConversationPage{
		Page: store.GroupMessagePage{Messages: []store.GroupMessage{
			{
				ID:              21,
				GroupID:         9,
				SenderID:        2,
				Sender:          store.PublicUser{Username: "bob", Nickname: "Bob"},
				ClientMessageID: "client-00000021",
				Body:            "group new",
				CreatedAt:       now,
			},
			{
				ID:              22,
				GroupID:         9,
				SenderID:        2,
				Sender:          store.PublicUser{Username: "bob", Nickname: "Bob"},
				ClientMessageID: "client-00000022",
				Body:            "group newer",
				CreatedAt:       now.Add(time.Second),
			},
		}},
		Complete: true,
	}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(
		dataStore,
		WithDirectMessageReader(direct),
		WithGroupMessageReader(group),
	).Register(router)

	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/bootstrap",
		map[string]any{
			"after_id": 10,
			"groups": []map[string]any{
				{"group_id": 9, "after_id": 20},
			},
		},
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
	if len(response.Direct.Messages) != 2 || response.Direct.Cursor != 12 {
		t.Fatalf("direct = %#v", response.Direct)
	}
	if !response.Direct.Complete {
		t.Fatal("direct complete = false, want true")
	}
	if response.Direct.Messages[0].Sender.Username != "bob" ||
		response.Direct.Messages[0].Recipient.Username != "alice" {
		t.Fatalf("direct participants = %#v", response.Direct.Messages[0])
	}
	if len(response.Groups) != 1 {
		t.Fatalf("groups = %#v", response.Groups)
	}
	if response.Groups[0].GroupID != 9 || len(response.Groups[0].Messages) != 2 ||
		response.Groups[0].Cursor != 22 {
		t.Fatalf("group delta = %#v", response.Groups[0])
	}
	if len(response.Partial) != 0 {
		t.Fatalf("partial = %#v, want empty", response.Partial)
	}
}

func TestBootstrapSkipsUnchangedAndUnknownGroups(t *testing.T) {
	now := time.Now().UTC()
	dataStore := bootstrapTestStore()
	dataStore.groupConversations = []store.GroupConversation{{
		Group: store.Group{
			ID:        9,
			Name:      "Team",
			OwnerID:   1,
			CreatedAt: now.Add(-time.Hour),
		},
		MemberCount: 2,
		LastMessage: &store.GroupMessage{
			ID:              20,
			GroupID:         9,
			SenderID:        2,
			Sender:          store.PublicUser{Username: "bob", Nickname: "Bob"},
			ClientMessageID: "client-00000020",
			Body:            "group",
			CreatedAt:       now,
		},
	}}
	group := &groupReaderStub{page: messaging.GroupConversationPage{Complete: true}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithGroupMessageReader(group)).Register(router)

	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/bootstrap",
		map[string]any{
			"groups": []map[string]any{
				{"group_id": 9, "after_id": 20},
				{"group_id": 99, "after_id": 5},
			},
		},
		authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if group.calls != 0 {
		t.Fatalf("group reader calls = %d, want 0", group.calls)
	}
	response := decodeBody[bootstrapResponse](t, recorder)
	if len(response.Groups) != 0 {
		t.Fatalf("groups = %#v, want empty", response.Groups)
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
		map[string]any{"after_id": 7},
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
	if !containsString(response.Partial, "direct") {
		t.Fatalf("partial = %#v, want direct", response.Partial)
	}
	if response.Direct.Complete {
		t.Fatal("direct complete = true without a reader, want false")
	}
	if response.Direct.Cursor != 7 {
		t.Fatalf("direct cursor = %d, want 7", response.Direct.Cursor)
	}
}

func TestBootstrapRejectsTooManyGroupCursors(t *testing.T) {
	dataStore := bootstrapTestStore()
	groups := make([]map[string]any, 0, bootstrapMaxGroupCursors+1)
	for index := 0; index <= bootstrapMaxGroupCursors; index++ {
		groups = append(groups, map[string]any{
			"group_id": index + 1,
			"after_id": 1,
		})
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore).Register(router)

	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/bootstrap",
		map[string]any{"groups": groups},
		authTestCookie(),
	)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if got := errorCode(t, recorder); got != "invalid_request" {
		t.Fatalf("error code = %q, want invalid_request", got)
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

func TestSyncMessagesReturnsCursor(t *testing.T) {
	now := time.Now().UTC()
	dataStore := bootstrapTestStore()
	reader := &directReaderStub{feed: messaging.FeedPage{
		Page: store.MessagePage{Messages: []store.Message{
			{
				ID:              5,
				SenderID:        2,
				RecipientID:     1,
				ClientMessageID: "client-00000005",
				Body:            "one",
				CreatedAt:       now,
			},
			{
				ID:              7,
				SenderID:        2,
				RecipientID:     1,
				ClientMessageID: "client-00000007",
				Body:            "two",
				CreatedAt:       now.Add(time.Second),
			},
		}},
		Complete: true,
	}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithDirectMessageReader(reader)).Register(router)

	recorder := doJSON(
		t,
		router,
		http.MethodGet,
		"/api/messages/sync?after_id=4",
		nil,
		authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	response := decodeBody[syncMessagePageResponse](t, recorder)
	if response.Cursor != 7 {
		t.Fatalf("cursor = %d, want 7", response.Cursor)
	}
	if len(response.Messages) != 2 {
		t.Fatalf("messages = %d, want 2", len(response.Messages))
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

func TestPlanBootstrapGroups(t *testing.T) {
	conversations := []conversationResponse{
		{
			Type:        "direct",
			User:        &publicUserResponse{Username: "bob"},
			LastMessage: &conversationMessageResponse{ID: 100},
		},
		{
			Type:        "group",
			Group:       &groupSummaryResponse{ID: 9},
			LastMessage: &conversationMessageResponse{ID: 20},
		},
		{
			Type:        "group",
			Group:       &groupSummaryResponse{ID: 10},
			LastMessage: &conversationMessageResponse{ID: 30},
		},
		{
			Type:  "group",
			Group: &groupSummaryResponse{ID: 11},
		},
		{
			Type:        "group",
			Group:       &groupSummaryResponse{ID: 12},
			LastMessage: &conversationMessageResponse{ID: 40},
		},
	}
	cursors := []bootstrapGroupCursor{
		{GroupID: 9, AfterID: 12},
		{GroupID: 9, AfterID: 15},
		{GroupID: 10, AfterID: 30},
		{GroupID: 11, AfterID: 5},
		{GroupID: 12, AfterID: 0},
		{GroupID: 99, AfterID: 1},
	}

	plan := planBootstrapGroups(conversations, cursors)
	if len(plan) != 1 {
		t.Fatalf("plan = %#v, want only group 9", plan)
	}
	if plan[0].GroupID != 9 || plan[0].AfterID != 15 {
		t.Fatalf("plan[0] = %#v, want group 9 after 15", plan[0])
	}
}

func TestPlanBootstrapGroupsPreservesConversationOrder(t *testing.T) {
	conversations := []conversationResponse{
		{Type: "group", Group: &groupSummaryResponse{ID: 2}, LastMessage: &conversationMessageResponse{ID: 50}},
		{Type: "group", Group: &groupSummaryResponse{ID: 1}, LastMessage: &conversationMessageResponse{ID: 40}},
	}
	cursors := []bootstrapGroupCursor{
		{GroupID: 1, AfterID: 10},
		{GroupID: 2, AfterID: 10},
	}

	plan := planBootstrapGroups(conversations, cursors)
	if len(plan) != 2 || plan[0].GroupID != 2 || plan[1].GroupID != 1 {
		t.Fatalf("plan = %#v, want groups 2 then 1", plan)
	}
}

func TestAssembleBootstrapGroupsTrimsTotalMessages(t *testing.T) {
	results := []bootstrapGroupResult{
		{
			groupID: 1,
			afterID: 10,
			ok:      true,
			response: bootstrapGroupResponse{
				GroupID: 1,
				Messages: []groupMessageResponse{
					{ID: 11},
					{ID: 12},
				},
				Cursor: 12,
			},
		},
		{
			groupID: 2,
			afterID: 20,
			ok:      true,
			response: bootstrapGroupResponse{
				GroupID: 2,
				Messages: []groupMessageResponse{
					{ID: 21},
					{ID: 22},
					{ID: 23},
				},
				Cursor: 23,
			},
		},
		{
			groupID: 3,
			afterID: 30,
			ok:      true,
			response: bootstrapGroupResponse{
				GroupID: 3,
				Messages: []groupMessageResponse{
					{ID: 31},
				},
				Cursor: 31,
			},
		},
		{
			groupID: 4,
			afterID: 40,
		},
	}

	assembled := assembleBootstrapGroups(results, 4)
	if len(assembled) != 3 {
		t.Fatalf("assembled = %#v, want three groups", assembled)
	}
	if len(assembled[0].Messages) != 2 || assembled[0].HasMore {
		t.Fatalf("group 1 = %#v, want two messages and has_more false", assembled[0])
	}
	if len(assembled[1].Messages) != 2 || !assembled[1].HasMore || assembled[1].Cursor != 22 {
		t.Fatalf("group 2 = %#v, want truncated at 22 with has_more", assembled[1])
	}
	if len(assembled[2].Messages) != 0 || !assembled[2].HasMore || assembled[2].Cursor != 30 {
		t.Fatalf("group 3 = %#v, want deferred with cursor 30", assembled[2])
	}
}
