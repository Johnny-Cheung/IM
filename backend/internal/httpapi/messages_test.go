package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

func authenticatedChatStore() *fakeStore {
	dataStore := authenticatedFriendStore()
	dataStore.user = store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	return dataStore
}

func TestListConversations(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.conversations = []store.Conversation{
		{
			User:                  store.PublicUser{Username: "bob", Nickname: "Bob"},
			IsFriend:              true,
			UnreadCount:           2,
			PeerLastReadMessageID: 4,
			LastMessage: &store.Message{
				ID:              7,
				SenderID:        1,
				RecipientID:     2,
				ClientMessageID: "client-12345678",
				Body:            "hello",
				CreatedAt:       now,
			},
		},
		{
			User:     store.PublicUser{Username: "carol", Nickname: "Carol"},
			IsFriend: true,
		},
		{
			User:     store.PublicUser{Username: "dave", Nickname: "Dave"},
			IsFriend: false,
			LastMessage: &store.Message{
				ID:              6,
				SenderID:        4,
				RecipientID:     1,
				ClientMessageID: "client-87654321",
				Body:            "old message",
				CreatedAt:       now.Add(-time.Minute),
			},
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
	if first.User.Username != "bob" || !first.IsFriend ||
		first.LastMessage == nil || first.LastMessage.Sender.Username != "alice" ||
		first.LastMessage.Recipient.Username != "bob" ||
		first.UnreadCount != 2 || first.PeerLastReadMessageID != 4 {
		t.Fatalf("first conversation = %#v", first)
	}
	if response.Conversations[1].LastMessage != nil {
		t.Fatalf("carol last message = %#v, want nil", response.Conversations[1].LastMessage)
	}
	third := response.Conversations[2]
	if third.IsFriend || third.LastMessage == nil ||
		third.LastMessage.Sender.Username != "dave" ||
		third.LastMessage.Recipient.Username != "alice" {
		t.Fatalf("third conversation = %#v", third)
	}
}

func TestListMessages(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.messagePage = store.MessagePage{
		Messages: []store.Message{
			{
				ID:              9,
				SenderID:        2,
				RecipientID:     1,
				ClientMessageID: "client-12345678",
				Body:            "hi alice",
				CreatedAt:       now,
			},
		},
		HasMore: true,
	}
	router := testRouter(dataStore)

	recorder := doJSON(
		t, router, http.MethodGet,
		"/api/conversations/bob/messages?before_id=20&limit=10",
		nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.listMessagesCall.userID != 1 ||
		dataStore.listMessagesCall.otherID != 2 ||
		dataStore.listMessagesCall.beforeID != 20 ||
		dataStore.listMessagesCall.limit != 10 {
		t.Fatalf("list messages call = %#v", dataStore.listMessagesCall)
	}
	response := decodeBody[messagePageResponse](t, recorder)
	if !response.HasMore || len(response.Messages) != 1 {
		t.Fatalf("response = %#v", response)
	}
	message := response.Messages[0]
	if message.Body != "hi alice" || message.Sender.Username != "bob" ||
		message.Recipient.Username != "alice" {
		t.Fatalf("message = %#v", message)
	}
}

func TestListMessagesDefaults(t *testing.T) {
	dataStore := authenticatedChatStore()
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodGet, "/api/conversations/bob/messages", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.listMessagesCall.beforeID != 0 || dataStore.listMessagesCall.limit != defaultMessagePageSize {
		t.Fatalf("list messages call = %#v", dataStore.listMessagesCall)
	}
}

func TestListMessagesRejectsInvalidPaging(t *testing.T) {
	router := testRouter(authenticatedChatStore())

	for _, path := range []string{
		"/api/conversations/bob/messages?before_id=abc",
		"/api/conversations/bob/messages?limit=0",
		"/api/conversations/bob/messages?limit=51",
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

func TestListMessagesAfter(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.messageAfterPage = store.MessagePage{
		Messages: []store.Message{{
			ID:              12,
			SenderID:        1,
			RecipientID:     2,
			ClientMessageID: "client-12345678",
			Body:            "after cursor",
			CreatedAt:       now,
		}},
		HasMore: true,
	}
	router := testRouter(dataStore)

	recorder := doJSON(
		t, router, http.MethodGet,
		"/api/conversations/bob/messages?after_id=11&limit=10",
		nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.listMessagesAfterCall.userID != 1 ||
		dataStore.listMessagesAfterCall.otherID != 2 ||
		dataStore.listMessagesAfterCall.afterID != 11 ||
		dataStore.listMessagesAfterCall.limit != 10 {
		t.Fatalf("list messages after call = %#v", dataStore.listMessagesAfterCall)
	}
	response := decodeBody[messagePageResponse](t, recorder)
	if !response.HasMore || len(response.Messages) != 1 ||
		response.Messages[0].Body != "after cursor" {
		t.Fatalf("response = %#v", response)
	}
}

func TestListMessagesRejectsTwoCursors(t *testing.T) {
	router := testRouter(authenticatedChatStore())

	recorder := doJSON(
		t, router, http.MethodGet,
		"/api/conversations/bob/messages?before_id=20&after_id=10",
		nil, authTestCookie(),
	)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if got := errorCode(t, recorder); got != "invalid_request" {
		t.Fatalf("error code = %q, want invalid_request", got)
	}
}

func TestMarkConversationRead(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.markReadResult = 42
	dataStore.markReadAdvanced = true
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/conversations/bob/read", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.markReadCall.userID != 1 ||
		dataStore.markReadCall.peerID != 2 ||
		dataStore.markReadCall.now.IsZero() {
		t.Fatalf("mark read call = %#v", dataStore.markReadCall)
	}
	response := decodeBody[readStateResponse](t, recorder)
	if response.LastReadMessageID != 42 {
		t.Fatalf("last read message id = %d, want 42", response.LastReadMessageID)
	}
}

func TestMarkConversationReadToSelf(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.user = store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/conversations/alice/read", nil, authTestCookie())
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if got := errorCode(t, recorder); got != "cannot_message_self" {
		t.Fatalf("error code = %q, want cannot_message_self", got)
	}
}

func TestSendMessage(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.createdMessage = store.Message{
		ID:              15,
		SenderID:        1,
		RecipientID:     2,
		ClientMessageID: "client-12345678",
		Body:            "hello bob",
		CreatedAt:       now,
	}
	dataStore.createdMessageNew = true
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/conversations/bob/messages", map[string]string{
		"body":              "  hello bob  ",
		"client_message_id": "client-12345678",
	}, authTestCookie())
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	if dataStore.createMessageCall.senderID != 1 ||
		dataStore.createMessageCall.recipientID != 2 ||
		dataStore.createMessageCall.body != "hello bob" ||
		dataStore.createMessageCall.clientMessageID != "client-12345678" {
		t.Fatalf("create message call = %#v", dataStore.createMessageCall)
	}
	response := decodeBody[sendMessageResponse](t, recorder)
	if response.Message.ID != 15 || response.Message.Body != "hello bob" ||
		response.Message.Sender.Username != "alice" ||
		response.Message.Recipient.Username != "bob" {
		t.Fatalf("response = %#v", response)
	}
}

func TestSendMessageDuplicate(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.createdMessage = store.Message{
		ID:              15,
		SenderID:        1,
		RecipientID:     2,
		ClientMessageID: "client-12345678",
		Body:            "hello",
		CreatedAt:       time.Now().UTC(),
	}
	dataStore.createdMessageNew = false
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/conversations/bob/messages", map[string]string{
		"body":              "hello",
		"client_message_id": "client-12345678",
	}, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	response := decodeBody[sendMessageResponse](t, recorder)
	if response.Message.ID != 15 {
		t.Fatalf("message = %#v", response.Message)
	}
}

func TestSendMessageNotFriends(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.createMessageErr = store.ErrNotFriends
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/conversations/bob/messages", map[string]string{
		"body":              "hello",
		"client_message_id": "client-12345678",
	}, authTestCookie())
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
	if got := errorCode(t, recorder); got != "not_friends" {
		t.Fatalf("error code = %q, want not_friends", got)
	}
}

func TestSendMessageValidation(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		clientID string
		wantCode string
	}{
		{
			name:     "empty body",
			body:     "   ",
			clientID: "client-12345678",
			wantCode: "invalid_message",
		},
		{
			name:     "too long",
			body:     strings.Repeat("好", maxMessageRunes+1),
			clientID: "client-12345678",
			wantCode: "message_too_long",
		},
		{
			name:     "invalid client id",
			body:     "hello",
			clientID: "bad id!",
			wantCode: "invalid_client_message_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := testRouter(authenticatedChatStore())
			recorder := doJSON(t, router, http.MethodPost, "/api/conversations/bob/messages", map[string]string{
				"body":              tt.body,
				"client_message_id": tt.clientID,
			}, authTestCookie())
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
			}
			if got := errorCode(t, recorder); got != tt.wantCode {
				t.Fatalf("error code = %q, want %q", got, tt.wantCode)
			}
		})
	}
}

func TestSendMessageToSelf(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.user = store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodPost, "/api/conversations/alice/messages", map[string]string{
		"body":              "hello",
		"client_message_id": "client-12345678",
	}, authTestCookie())
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if got := errorCode(t, recorder); got != "cannot_message_self" {
		t.Fatalf("error code = %q, want cannot_message_self", got)
	}
}

func TestChatEndpointsRequireSession(t *testing.T) {
	tests := []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/api/conversations"},
		{method: http.MethodGet, path: "/api/conversations/bob/messages"},
		{method: http.MethodPost, path: "/api/conversations/bob/messages"},
		{method: http.MethodPost, path: "/api/conversations/bob/read"},
		{method: http.MethodGet, path: "/api/ws"},
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

func TestWebSocketReceivesSavedMessage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dataStore := authenticatedChatStore()
	dataStore.createdMessage = store.Message{
		ID:              21,
		SenderID:        1,
		RecipientID:     2,
		ClientMessageID: "client-12345678",
		Body:            "live hello",
		CreatedAt:       time.Now().UTC(),
	}
	dataStore.createdMessageNew = true

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

	recorder := doJSON(t, router, http.MethodPost, "/api/conversations/bob/messages", map[string]string{
		"body":              "live hello",
		"client_message_id": "client-12345678",
	}, authTestCookie())
	if recorder.Code != http.StatusCreated {
		t.Fatalf("send status = %d, want %d", recorder.Code, http.StatusCreated)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket message: %v", err)
	}
	var event struct {
		Type    string          `json:"type"`
		Message messageResponse `json:"message"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode websocket message: %v", err)
	}
	if event.Type != "message.created" || event.Message.ID != 21 ||
		event.Message.Body != "live hello" {
		t.Fatalf("event = %#v", event)
	}
}

func TestWebSocketReplacesExistingConnection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dataStore := authenticatedChatStore()
	api := New(dataStore)
	router := gin.New()
	api.Register(router)
	server := httptest.NewServer(router)
	defer server.Close()

	header := http.Header{"Cookie": []string{sessionCookieName + "=token"}}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/ws"

	first, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial first websocket: %v", err)
	}
	defer func() { _ = first.Close() }()

	deadline := time.Now().Add(2 * time.Second)
	for api.hub.activeCount(1) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	second, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial second websocket: %v", err)
	}
	defer func() { _ = second.Close() }()

	for api.hub.activeCount(1) != 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if api.hub.activeCount(1) != 1 {
		t.Fatal("expected one active websocket after replacement")
	}

	_ = first.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := first.ReadMessage(); !websocket.IsCloseError(err, wsCloseReplaced) {
		t.Fatalf("first connection error = %v, want close code %d", err, wsCloseReplaced)
	}
}

func TestWebSocketReceivesReadEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dataStore := authenticatedChatStore()
	dataStore.markReadResult = 42
	dataStore.markReadAdvanced = true
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

	recorder := doJSON(t, router, http.MethodPost, "/api/conversations/bob/read", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("read status = %d, want %d", recorder.Code, http.StatusOK)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket event: %v", err)
	}
	var event wsReadEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode read event: %v", err)
	}
	if event.Type != "messages.read" || event.Reader != "alice" ||
		event.Peer != "bob" || event.LastReadMessageID != 42 {
		t.Fatalf("event = %#v", event)
	}
}
