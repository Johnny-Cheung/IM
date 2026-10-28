package httpapi

import (
	"context"
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

func authenticatedChatStore() *fakeStore {
	dataStore := authenticatedFriendStore()
	dataStore.user = store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	// Group mutation handlers authorize membership before setting the sync flag;
	// tests that exercise a non-member set this back to false.
	dataStore.groupMember = true
	return dataStore
}

type directMessageAcceptorStub struct {
	request messaging.DirectMessageRequest
	result  messaging.DirectMessageResult
	err     error
}

func (s *directMessageAcceptorStub) Accept(
	_ context.Context,
	request messaging.DirectMessageRequest,
) (messaging.DirectMessageResult, error) {
	s.request = request
	return s.result, s.err
}

func TestSendMessageUsesDirectAcceptor(t *testing.T) {
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
				CreatedAt:       time.UnixMilli(1700000000123).UTC(),
			},
		},
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithDirectMessageAcceptor(acceptor)).Register(router)

	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/conversations/bob/messages",
		map[string]string{
			"body":              "hello",
			"client_message_id": "client-12345678",
		},
		authTestCookie(),
	)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	if acceptor.request.SenderID != 1 || acceptor.request.RecipientID != 2 ||
		acceptor.request.ClientMessageID != "client-12345678" || acceptor.request.Body != "hello" {
		t.Fatalf("accept request = %#v", acceptor.request)
	}
	response := decodeBody[sendMessageResponse](t, recorder)
	if response.Message.ID != 42 || response.Message.Seq != 4 ||
		response.Message.Body != "hello" {
		t.Fatalf("response message = %#v", response.Message)
	}
}

func TestSendMessageMapsDirectAcceptorErrors(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		statusCode int
		code       string
	}{
		{name: "conflict", err: store.ErrMessageConflict, statusCode: http.StatusConflict, code: "client_message_id_conflict"},
		{name: "unavailable", err: messaging.ErrMessageServiceUnavailable, statusCode: http.StatusServiceUnavailable, code: "message_service_unavailable"},
		{name: "seq not ready", err: messaging.ErrSeqNotReady, statusCode: http.StatusServiceUnavailable, code: "message_service_unavailable"},
		{name: "seq overflow", err: messaging.ErrSeqOverflow, statusCode: http.StatusServiceUnavailable, code: "message_service_unavailable"},
		{name: "id exhausted", err: store.ErrMessageIDExhausted, statusCode: http.StatusServiceUnavailable, code: "message_id_exhausted"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			dataStore := authenticatedFriendStore()
			dataStore.usersByKey = map[string]store.User{
				"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
			}
			acceptor := &directMessageAcceptorStub{err: testCase.err}
			gin.SetMode(gin.TestMode)
			router := gin.New()
			New(dataStore, WithDirectMessageAcceptor(acceptor)).Register(router)

			recorder := doJSON(
				t,
				router,
				http.MethodPost,
				"/api/conversations/bob/messages",
				map[string]string{
					"body":              "hello",
					"client_message_id": "client-12345678",
				},
				authTestCookie(),
			)
			if recorder.Code != testCase.statusCode {
				t.Fatalf("status = %d, want %d", recorder.Code, testCase.statusCode)
			}
			if got := errorCode(t, recorder); got != testCase.code {
				t.Fatalf("error code = %q, want %q", got, testCase.code)
			}
		})
	}
}

func TestListConversations(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.conversations = []store.Conversation{
		{
			User:            store.PublicUser{Username: "bob", Nickname: "Bob"},
			IsFriend:        true,
			UnreadCount:     2,
			LastSeq:         7,
			PeerLastReadSeq: 4,
			LastMessage: &store.Message{
				ID:              7,
				Seq:             7,
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
			LastSeq:  6,
			LastMessage: &store.Message{
				ID:              6,
				Seq:             6,
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
	if first.User == nil || first.User.Username != "bob" || !first.IsFriend ||
		first.LastMessage == nil || first.LastMessage.Recipient == nil ||
		first.LastMessage.Sender.Username != "alice" ||
		first.LastMessage.Recipient.Username != "bob" ||
		first.UnreadCount != 2 || first.LastSeq != 7 || first.PeerLastReadSeq != 4 || first.LastMessage.Seq != 7 {
		t.Fatalf("first conversation = %#v", first)
	}
	second := response.Conversations[1]
	if second.User == nil || second.IsFriend ||
		second.LastMessage == nil || second.LastMessage.Recipient == nil ||
		second.LastMessage.Sender.Username != "dave" ||
		second.LastMessage.Recipient.Username != "alice" ||
		second.LastSeq != 6 {
		t.Fatalf("second conversation = %#v", second)
	}
	third := response.Conversations[2]
	if third.User == nil || !third.IsFriend || third.LastMessage != nil {
		t.Fatalf("third conversation = %#v", third)
	}
}

type directReaderStub struct {
	page         messaging.ConversationPage
	err          error
	calls        int
	feed         messaging.FeedPage
	feedErr      error
	lastMessages map[uint64]store.Message
	lastErr      error
}

func (s *directReaderStub) ReadConversation(
	_ context.Context, _, _, _ uint64, _ int,
) (messaging.ConversationPage, error) {
	s.calls++
	return s.page, s.err
}

func (s *directReaderStub) ReadConversationAfter(
	_ context.Context, _, _, _ uint64, _ int,
) (messaging.ConversationPage, error) {
	s.calls++
	return s.page, s.err
}

func (s *directReaderStub) ReadFeed(
	_ context.Context, _, _ uint64, _ int,
) (messaging.FeedPage, error) {
	return s.feed, s.feedErr
}

func (s *directReaderStub) LastConversationMessages(
	_ context.Context, _ uint64,
) (map[uint64]store.Message, error) {
	return s.lastMessages, s.lastErr
}

func TestListMessagesServesCompleteRedisPageWithoutStore(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	reader := &directReaderStub{page: messaging.ConversationPage{
		Page: store.MessagePage{Messages: []store.Message{{
			ID:              42,
			Seq:             42,
			SenderID:        2,
			RecipientID:     1,
			ClientMessageID: "client-12345678",
			Body:            "from redis",
			CreatedAt:       now,
		}}},
		Complete: true,
	}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithDirectMessageReader(reader)).Register(router)

	recorder := doJSON(t, router, http.MethodGet, "/api/conversations/bob/messages", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.listMessagesCall.userID != 0 {
		t.Fatal("store was queried even though Redis covered the page")
	}

	var response messagePageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Messages) != 1 || response.Messages[0].Seq != 42 ||
		response.Messages[0].Body != "from redis" {
		t.Fatalf("messages = %#v", response.Messages)
	}
}

func TestListMessagesMergesHotPageWhenRedisIsIncomplete(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.messagePage = store.MessagePage{Messages: []store.Message{{
		ID:              41,
		Seq:             41,
		SenderID:        2,
		RecipientID:     1,
		ClientMessageID: "client-12345678",
		Body:            "from mysql",
		CreatedAt:       now,
	}}}
	reader := &directReaderStub{page: messaging.ConversationPage{
		Page: store.MessagePage{Messages: []store.Message{{
			ID:              42,
			Seq:             42,
			SenderID:        1,
			RecipientID:     2,
			ClientMessageID: "client-87654321",
			Body:            "from redis",
			CreatedAt:       now,
		}}},
		Complete: false,
	}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithDirectMessageReader(reader)).Register(router)

	recorder := doJSON(t, router, http.MethodGet, "/api/conversations/bob/messages", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.listMessagesCall.userID != 1 {
		t.Fatal("store was not queried for an incomplete Redis page")
	}

	var response messagePageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Messages) != 2 ||
		response.Messages[0].Body != "from mysql" ||
		response.Messages[1].Body != "from redis" {
		t.Fatalf("messages = %#v", response.Messages)
	}
}

func TestListMessagesFallsBackWhenCounterIsMissing(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.messagePage = store.MessagePage{Messages: []store.Message{{
		ID:              41,
		Seq:             41,
		SenderID:        2,
		RecipientID:     1,
		ClientMessageID: "client-12345678",
		Body:            "from mysql",
		CreatedAt:       now,
	}}}
	// The reader reports the empty, incomplete page produced by a lost counter.
	reader := &directReaderStub{page: messaging.ConversationPage{Complete: false}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithDirectMessageReader(reader)).Register(router)

	recorder := doJSON(t, router, http.MethodGet, "/api/conversations/bob/messages", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.listMessagesCall.userID != 1 {
		t.Fatal("store was not queried when the seq counter was missing")
	}

	var response messagePageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Messages) != 1 || response.Messages[0].Body != "from mysql" {
		t.Fatalf("messages = %#v, want the MySQL history", response.Messages)
	}
}

func TestSyncMessagesReturnsFeedWithPeers(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedFriendStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	reader := &directReaderStub{feed: messaging.FeedPage{
		Page: store.MessagePage{
			Messages: []store.Message{
				{
					ID:              11,
					Seq:             4,
					SenderID:        2,
					RecipientID:     1,
					ClientMessageID: "client-00000001",
					Body:            "incoming",
					CreatedAt:       now,
				},
				{
					ID:              12,
					Seq:             5,
					SenderID:        1,
					RecipientID:     2,
					ClientMessageID: "client-00000002",
					Body:            "outgoing",
					CreatedAt:       now,
				},
			},
			HasMore: true,
		},
		Complete: true,
	}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithDirectMessageReader(reader)).Register(router)

	recorder := doJSON(t, router, http.MethodGet, "/api/messages/sync?after_id=10&limit=20", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	var response syncMessagePageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.Complete || !response.HasMore || len(response.Messages) != 2 {
		t.Fatalf("response = %#v", response)
	}
	if response.Messages[0].Sender.Username != "bob" || response.Messages[0].Recipient.Username != "alice" {
		t.Fatalf("incoming message identity = %#v", response.Messages[0])
	}
	if response.Messages[1].Sender.Username != "alice" || response.Messages[1].Recipient.Username != "bob" {
		t.Fatalf("outgoing message identity = %#v", response.Messages[1])
	}
	if response.Messages[0].Body != "incoming" || response.Messages[1].Body != "outgoing" {
		t.Fatalf("bodies = %#v", response.Messages)
	}
	if response.Messages[0].Seq != 4 || response.Messages[1].Seq != 5 {
		t.Fatalf("sync seq = %#v", response.Messages)
	}
}

func TestSyncMessagesFallsBackWithoutRedis(t *testing.T) {
	router := testRouter(authenticatedFriendStore())

	recorder := doJSON(t, router, http.MethodGet, "/api/messages/sync?after_id=1", nil, authTestCookie())
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if got := errorCode(t, recorder); got != "message_service_unavailable" {
		t.Fatalf("error code = %q", got)
	}
}

func TestListConversationsUsesHotLastMessage(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedFriendStore()
	stored := store.Message{
		ID:              5,
		Seq:             5,
		SenderID:        2,
		RecipientID:     1,
		ClientMessageID: "client-00000005",
		Body:            "from mysql",
		CreatedAt:       now,
	}
	dataStore.conversations = []store.Conversation{{
		UserID:      2,
		User:        store.PublicUser{Username: "bob", Nickname: "Bob"},
		IsFriend:    true,
		LastMessage: &stored,
	}}
	reader := &directReaderStub{lastMessages: map[uint64]store.Message{
		2: {
			ID:              9,
			Seq:             9,
			SenderID:        2,
			RecipientID:     1,
			ClientMessageID: "client-00000009",
			Body:            "from redis",
			CreatedAt:       now,
		},
	}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithDirectMessageReader(reader)).Register(router)

	recorder := doJSON(t, router, http.MethodGet, "/api/conversations", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	var response conversationListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Conversations) != 1 {
		t.Fatalf("conversations = %d, want 1", len(response.Conversations))
	}
	last := response.Conversations[0].LastMessage
	if last == nil || last.ID != 9 || last.Body != "from redis" {
		t.Fatalf("last message = %#v, want the hot message", last)
	}
}

func TestListMessages(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.messagePage = store.MessagePage{
		Messages: []store.Message{
			{
				ID:              9,
				Seq:             3,
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
		"/api/conversations/bob/messages?before_seq=20&limit=10",
		nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.listMessagesCall.userID != 1 ||
		dataStore.listMessagesCall.otherID != 2 ||
		dataStore.listMessagesCall.beforeSeq != 20 ||
		dataStore.listMessagesCall.limit != 10 {
		t.Fatalf("list messages call = %#v", dataStore.listMessagesCall)
	}
	response := decodeBody[messagePageResponse](t, recorder)
	if !response.HasMore || len(response.Messages) != 1 {
		t.Fatalf("response = %#v", response)
	}
	message := response.Messages[0]
	if message.Seq != 3 || message.Body != "hi alice" || message.Sender.Username != "bob" ||
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
	if dataStore.listMessagesCall.beforeSeq != 0 || dataStore.listMessagesCall.limit != defaultMessagePageSize {
		t.Fatalf("list messages call = %#v", dataStore.listMessagesCall)
	}
}

func TestListMessagesRejectsInvalidPaging(t *testing.T) {
	router := testRouter(authenticatedChatStore())

	for _, path := range []string{
		"/api/conversations/bob/messages?before_seq=abc",
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
			Seq:             12,
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
		"/api/conversations/bob/messages?after_seq=11&limit=10",
		nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.listMessagesAfterCall.userID != 1 ||
		dataStore.listMessagesAfterCall.otherID != 2 ||
		dataStore.listMessagesAfterCall.afterSeq != 11 ||
		dataStore.listMessagesAfterCall.limit != 10 {
		t.Fatalf("list messages after call = %#v", dataStore.listMessagesAfterCall)
	}
	response := decodeBody[messagePageResponse](t, recorder)
	if !response.HasMore || len(response.Messages) != 1 ||
		response.Messages[0].Seq != 12 || response.Messages[0].Body != "after cursor" {
		t.Fatalf("response = %#v", response)
	}
}

func TestListMessagesRejectsTwoCursors(t *testing.T) {
	router := testRouter(authenticatedChatStore())

	recorder := doJSON(
		t, router, http.MethodGet,
		"/api/conversations/bob/messages?before_seq=20&after_seq=10",
		nil, authTestCookie(),
	)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if got := errorCode(t, recorder); got != "invalid_request" {
		t.Fatalf("error code = %q, want invalid_request", got)
	}
}

func TestSendMessage(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.createdMessage = store.Message{
		ID:              15,
		Seq:             9,
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
	if response.Message.ID != 15 || response.Message.Seq != 9 ||
		response.Message.Body != "hello bob" ||
		response.Message.Sender.Username != "alice" ||
		response.Message.Recipient.Username != "bob" {
		t.Fatalf("response = %#v", response)
	}
}

func TestSendMessageDuplicate(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.createdMessage = store.Message{
		ID:              15,
		Seq:             9,
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
	if response.Message.ID != 15 || response.Message.Seq != 9 {
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
		Seq:             11,
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
		event.Message.Seq != 11 || event.Message.Body != "live hello" {
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
