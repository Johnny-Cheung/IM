package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

func groupTestDetail(now time.Time) store.GroupDetail {
	return store.GroupDetail{
		Group: store.Group{
			ID:        7,
			Name:      "Team",
			OwnerID:   1,
			Owner:     store.PublicUser{Username: "alice", Nickname: "Alice"},
			CreatedAt: now,
		},
		Members: []store.GroupMember{
			{
				User:     store.PublicUser{Username: "alice", Nickname: "Alice"},
				IsOwner:  true,
				JoinedAt: now,
			},
			{
				User:     store.PublicUser{Username: "bob", Nickname: "Bob"},
				JoinedAt: now,
			},
		},
		MemberCount: 2,
		IsOwner:     true,
	}
}

func TestListConversationsIncludesGroups(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.conversations = []store.Conversation{
		{
			User:     store.PublicUser{Username: "bob", Nickname: "Bob"},
			IsFriend: true,
			LastSeq:  4,
			LastMessage: &store.Message{
				ID:              4,
				Seq:             4,
				SenderID:        2,
				RecipientID:     1,
				ClientMessageID: "direct-12345678",
				Body:            "direct hello",
				CreatedAt:       now.Add(-2 * time.Minute),
			},
		},
	}
	dataStore.groupConversations = []store.GroupConversation{
		{
			Group: store.Group{
				ID:        9,
				Name:      "Team",
				OwnerID:   1,
				Owner:     store.PublicUser{Username: "alice", Nickname: "Alice"},
				CreatedAt: now.Add(-time.Hour),
			},
			MemberCount: 3,
			IsOwner:     true,
			UnreadCount: 5,
			LastSeq:     12,
			LastMessage: &store.GroupMessage{
				ID:              12,
				Seq:             12,
				GroupID:         9,
				SenderID:        2,
				Sender:          store.PublicUser{Username: "bob", Nickname: "Bob"},
				ClientMessageID: "group-12345678",
				Body:            "group hello",
				CreatedAt:       now,
			},
		},
		{
			Group: store.Group{
				ID:        10,
				Name:      "Empty",
				OwnerID:   2,
				Owner:     store.PublicUser{Username: "bob", Nickname: "Bob"},
				CreatedAt: now.Add(-time.Minute),
			},
			MemberCount: 2,
		},
	}

	router := testRouter(dataStore)
	recorder := doJSON(t, router, http.MethodGet, "/api/conversations", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	response := decodeBody[conversationListResponse](t, recorder)
	if len(response.Conversations) != 3 {
		t.Fatalf("conversation count = %d, want 3", len(response.Conversations))
	}

	first := response.Conversations[0]
	if first.Type != "group" || first.Group == nil || first.Group.ID != 9 ||
		first.Group.Owner.Username != "alice" || !first.IsOwner ||
		first.UnreadCount != 5 || first.LastSeq != 12 || first.LastMessage == nil ||
		first.LastMessage.GroupID != 9 || first.LastMessage.Seq != 12 ||
		first.LastMessage.Recipient != nil ||
		first.LastMessage.Body != "group hello" {
		t.Fatalf("first conversation = %#v", first)
	}
	second := response.Conversations[1]
	if second.Type != "direct" || second.User == nil || second.User.Username != "bob" ||
		second.LastSeq != 4 || second.LastMessage == nil ||
		second.LastMessage.Seq != 4 || second.LastMessage.Body != "direct hello" {
		t.Fatalf("second conversation = %#v", second)
	}
	third := response.Conversations[2]
	if third.Type != "group" || third.Group == nil || third.Group.ID != 10 ||
		third.LastMessage != nil {
		t.Fatalf("third conversation = %#v", third)
	}
}

func TestListConversationsKeepsGroupMySQLSeqFloor(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.groupConversationsWithoutUnread = []store.GroupConversation{{
		Group: store.Group{
			ID:        9,
			Name:      "Team",
			OwnerID:   2,
			Owner:     store.PublicUser{Username: "bob", Nickname: "Bob"},
			CreatedAt: now,
		},
		MemberCount: 3,
		LastSeq:     10,
		LastMessage: &store.GroupMessage{
			ID:              10,
			Seq:             10,
			GroupID:         9,
			SenderID:        2,
			Sender:          store.PublicUser{Username: "bob", Nickname: "Bob"},
			ClientMessageID: "group-00000010",
			Body:            "persisted before the lost counter tail",
			CreatedAt:       now,
		},
	}}
	tracker := newFakeGroupReadTracker()
	tracker.states = map[uint64]messaging.GroupConversationState{
		9: {LastSeq: 9, ReadSeq: 2, Unread: 7},
	}

	router := testRouter(dataStore, WithGroupReadTracker(tracker))
	recorder := doJSON(t, router, http.MethodGet, "/api/conversations", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeBody[conversationListResponse](t, recorder)
	if len(response.Conversations) != 1 {
		t.Fatalf("conversations = %#v", response.Conversations)
	}
	conversation := response.Conversations[0]
	if conversation.LastSeq != 10 {
		t.Fatalf("last_seq = %d, want the MySQL floor 10", conversation.LastSeq)
	}
	if conversation.LastMessage == nil || conversation.LastMessage.Seq != 10 {
		t.Fatalf("last message = %#v", conversation.LastMessage)
	}
	if conversation.UnreadCount != 7 {
		t.Fatalf("unread = %d, want 7", conversation.UnreadCount)
	}
}

func TestCreateGroup(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(now)

	router := testRouter(dataStore)
	recorder := doJSON(t, router, http.MethodPost, "/api/groups", map[string]any{
		"name":             "  Team  ",
		"member_usernames": []string{"BOB"},
	}, authTestCookie())
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	response := decodeBody[groupDetailResponse](t, recorder)
	if response.Group.ID != 7 || response.Group.Name != "Team" ||
		len(response.Members) != 2 || !response.IsOwner {
		t.Fatalf("group detail = %#v", response)
	}
	if dataStore.createGroupCall.ownerID != 1 ||
		dataStore.createGroupCall.name != "Team" ||
		len(dataStore.createGroupCall.memberIDs) != 1 ||
		dataStore.createGroupCall.memberIDs[0] != 2 {
		t.Fatalf("create call = %#v", dataStore.createGroupCall)
	}
}

func TestCreateGroupValidation(t *testing.T) {
	dataStore := authenticatedChatStore()
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups", map[string]any{
		"name":             "Team",
		"member_usernames": []string{},
	}, authTestCookie())
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("empty members status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if response := decodeBody[errorResponse](t, recorder); response.Error.Code != "invalid_members" {
		t.Fatalf("empty members error = %#v", response)
	}

	recorder = doJSON(t, router, http.MethodPost, "/api/groups", map[string]any{
		"name":             strings.Repeat("群", 31),
		"member_usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("long name status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if response := decodeBody[errorResponse](t, recorder); response.Error.Code != "invalid_group_name" {
		t.Fatalf("long name error = %#v", response)
	}
}

func TestCreateGroupRejectsNonFriend(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.createGroupErr = store.ErrNotFriends
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups", map[string]any{
		"name":             "Team",
		"member_usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
	response := decodeBody[errorResponse](t, recorder)
	if response.Error.Code != "not_friends" {
		t.Fatalf("error = %#v", response)
	}
}

func TestInviteGroupMembers(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(now)
	dataStore.groupMemberIDs = []uint64{1, 2}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/members", map[string]any{
		"usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if dataStore.inviteGroupCall.groupID != 7 ||
		dataStore.inviteGroupCall.inviterID != 1 ||
		len(dataStore.inviteGroupCall.memberIDs) != 1 ||
		dataStore.inviteGroupCall.memberIDs[0] != 2 {
		t.Fatalf("invite call = %#v", dataStore.inviteGroupCall)
	}
}

func TestInviteGroupMembersRejectsNonFriend(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.inviteGroupErr = store.ErrNotFriends
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/members", map[string]any{
		"usernames": []string{"bob"},
	}, authTestCookie())
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
	response := decodeBody[errorResponse](t, recorder)
	if response.Error.Code != "not_friends" {
		t.Fatalf("error = %#v", response)
	}
}

func TestKickGroupMember(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	dataStore.groupDetail = groupTestDetail(now)
	dataStore.groupMemberIDs = []uint64{1}
	router := testRouter(dataStore)

	recorder := doJSON(
		t, router, http.MethodDelete, "/api/groups/7/members/bob", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if response := decodeBody[groupDetailResponse](t, recorder); response.Group.ID != 7 {
		t.Fatalf("group detail = %#v", response)
	}

	dataStore.kickGroupErr = store.ErrNotGroupOwner
	recorder = doJSON(
		t, router, http.MethodDelete, "/api/groups/7/members/bob", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("not owner status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
	if response := decodeBody[errorResponse](t, recorder); response.Error.Code != "not_owner" {
		t.Fatalf("not owner error = %#v", response)
	}
}

func TestLeaveGroup(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.leaveGroupDissolved = true
	dataStore.groupMemberIDs = []uint64{1, 2}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/leave", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if response := decodeBody[leaveGroupResponse](t, recorder); !response.Dissolved {
		t.Fatalf("response = %#v, want dissolved", response)
	}
}

func TestListGroupMessages(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.groupMessagePage = store.GroupMessagePage{
		Messages: []store.GroupMessage{
			{
				ID:              11,
				Seq:             11,
				GroupID:         7,
				SenderID:        2,
				Sender:          store.PublicUser{Username: "bob", Nickname: "Bob"},
				ClientMessageID: "group-12345678",
				Body:            "hello group",
				CreatedAt:       now,
			},
		},
		HasMore: true,
	}
	router := testRouter(dataStore)

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/messages?before_seq=20&limit=30", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	response := decodeBody[groupMessagePageResponse](t, recorder)
	if !response.HasMore || len(response.Messages) != 1 ||
		response.Messages[0].GroupID != 7 || response.Messages[0].Seq != 11 ||
		response.Messages[0].Sender.Username != "bob" ||
		response.Messages[0].Body != "hello group" {
		t.Fatalf("page = %#v", response)
	}

	recorder = doJSON(
		t, router, http.MethodGet,
		"/api/groups/7/messages?before_seq=20&after_seq=10", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("conflicting cursor status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestListGroupMessagesAfterZeroStartsAtBeginning(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.groupMessageAfterPage = store.GroupMessagePage{}
	router := testRouter(dataStore)

	recorder := doJSON(
		t, router, http.MethodGet,
		"/api/groups/7/messages?after_seq=0&limit=10", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s",
			recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if dataStore.groupMessageAfterCall.afterSeq != 0 ||
		dataStore.groupMessageAfterCall.limit != 10 {
		t.Fatalf("after-seq call = %#v", dataStore.groupMessageAfterCall)
	}
	if dataStore.groupMessageCall.limit != 0 {
		t.Fatalf("before-seq path was used: %#v", dataStore.groupMessageCall)
	}
}

func TestListGroupMessagesCursorPresenceDecidesDirection(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.groupMessageAfterPage = store.GroupMessagePage{}
	router := testRouter(dataStore)

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/messages?after_seq=", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s",
			recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if dataStore.groupMessageAfterCall.afterSeq != 0 ||
		dataStore.groupMessageAfterCall.limit != defaultMessagePageSize {
		t.Fatalf("after-seq call = %#v", dataStore.groupMessageAfterCall)
	}
	if dataStore.groupMessageCall.limit != 0 {
		t.Fatalf("an empty after_seq took the before-seq path: %#v", dataStore.groupMessageCall)
	}
}

func TestListGroupMessagesRejectsTwoCursorsWhenOneIsEmpty(t *testing.T) {
	router := testRouter(authenticatedChatStore())

	for _, path := range []string{
		"/api/groups/7/messages?before_seq=5&after_seq=",
		"/api/groups/7/messages?before_seq=&after_seq=10",
	} {
		t.Run(path, func(t *testing.T) {
			recorder := doJSON(t, router, http.MethodGet, path, nil, authTestCookie())
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
			}
			if got := errorCode(t, recorder); got != "invalid_request" {
				t.Fatalf("error code = %q, want invalid_request", got)
			}
		})
	}
}

func TestSendGroupMessage(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.createdGroupMessage = store.GroupMessage{
		ID:              12,
		Seq:             12,
		GroupID:         7,
		SenderID:        1,
		Sender:          store.PublicUser{Username: "alice", Nickname: "Alice"},
		ClientMessageID: "group-client-1",
		Body:            "hello",
		CreatedAt:       now,
	}
	dataStore.createdGroupMessageNew = true
	dataStore.groupMemberIDs = []uint64{1, 2}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/messages", map[string]string{
		"body":              "hello",
		"client_message_id": "group-client-1",
	}, authTestCookie())
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	response := decodeBody[sendGroupMessageResponse](t, recorder)
	if response.Message.GroupID != 7 || response.Message.Seq != 12 ||
		response.Message.Body != "hello" ||
		response.Message.Sender.Username != "alice" {
		t.Fatalf("message = %#v", response.Message)
	}
	if dataStore.createGroupMessageCall.senderID != 1 ||
		dataStore.createGroupMessageCall.groupID != 7 ||
		dataStore.createGroupMessageCall.body != "hello" {
		t.Fatalf("create message call = %#v", dataStore.createGroupMessageCall)
	}
}

func TestSendGroupMessageSeqNotReady(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.createGroupMessageErr = messaging.ErrSeqNotReady
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/messages", map[string]string{
		"body":              "hello",
		"client_message_id": "group-client-1",
	}, authTestCookie())
	if recorder.Code != http.StatusServiceUnavailable ||
		errorCode(t, recorder) != "message_service_unavailable" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestSendGroupMessageValidation(t *testing.T) {
	dataStore := authenticatedChatStore()
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/messages", map[string]string{
		"body":              "   ",
		"client_message_id": "group-client-1",
	}, authTestCookie())
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if response := decodeBody[errorResponse](t, recorder); response.Error.Code != "invalid_message" {
		t.Fatalf("error = %#v", response)
	}

	dataStore.createGroupMessageErr = store.ErrGroupNotFound
	recorder = doJSON(t, router, http.MethodPost, "/api/groups/7/messages", map[string]string{
		"body":              "hello",
		"client_message_id": "group-client-1",
	}, authTestCookie())
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("missing group status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	if response := decodeBody[errorResponse](t, recorder); response.Error.Code != "group_not_found" {
		t.Fatalf("missing group error = %#v", response)
	}
}

func TestGetGroupNotFound(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.groupDetailErr = store.ErrGroupNotFound
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodGet, "/api/groups/7", nil, authTestCookie())
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	if response := decodeBody[errorResponse](t, recorder); response.Error.Code != "group_not_found" {
		t.Fatalf("error = %#v", response)
	}
}

func TestWebSocketReceivesGroupMessage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.createdGroupMessage = store.GroupMessage{
		ID:              12,
		Seq:             12,
		GroupID:         7,
		SenderID:        1,
		Sender:          store.PublicUser{Username: "alice", Nickname: "Alice"},
		ClientMessageID: "group-client-1",
		Body:            "live group hello",
		CreatedAt:       now,
	}
	dataStore.createdGroupMessageNew = true
	dataStore.groupMemberIDs = []uint64{1, 2}

	api := New(dataStore)
	router := gin.New()
	api.Register(router)
	server := httptest.NewServer(router)
	defer server.Close()

	header := http.Header{"Cookie": []string{sessionCookieName + "=token"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	deadline := time.Now().Add(2 * time.Second)
	for api.hub.activeCount(1) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if api.hub.activeCount(1) != 1 {
		t.Fatal("websocket client was not registered")
	}

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/messages", map[string]string{
		"body":              "live group hello",
		"client_message_id": "group-client-1",
	}, authTestCookie())
	if recorder.Code != http.StatusCreated {
		t.Fatalf("send status = %d, want %d", recorder.Code, http.StatusCreated)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket message: %v", err)
	}
	var event wsGroupMessageEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode websocket message: %v", err)
	}
	if event.Type != "group.message.created" || event.Message.GroupID != 7 ||
		event.Message.Seq != 12 || event.Message.Body != "live group hello" {
		t.Fatalf("event = %#v", event)
	}
}

func TestWebSocketReceivesGroupChanged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.groupDetail = groupTestDetail(now)
	dataStore.groupMemberIDs = []uint64{1, 2}

	api := New(dataStore)
	router := gin.New()
	api.Register(router)
	server := httptest.NewServer(router)
	defer server.Close()

	header := http.Header{"Cookie": []string{sessionCookieName + "=token"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	deadline := time.Now().Add(2 * time.Second)
	for api.hub.activeCount(1) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	recorder := doJSON(t, router, http.MethodPatch, "/api/groups/7", map[string]string{
		"name": "Renamed",
	}, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("rename status = %d, want %d", recorder.Code, http.StatusOK)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket event: %v", err)
	}
	var event wsGroupChangedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode group changed event: %v", err)
	}
	if event.Type != "group.changed" || event.GroupID != 7 || event.Action != "renamed" {
		t.Fatalf("event = %#v", event)
	}
}
