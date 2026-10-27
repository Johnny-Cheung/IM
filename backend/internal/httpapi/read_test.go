package httpapi

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

type directReadAdvanceCall struct {
	userID    uint64
	peerID    uint64
	throughID uint64
	now       time.Time
}

type fakeDirectReadTracker struct {
	ready      bool
	readyErr   error
	advance    func(userID, peerID, throughID uint64) (messaging.DirectReadAdvance, error)
	advanceErr error
	calls      []directReadAdvanceCall

	positions     map[uint64]uint64
	peerPositions map[uint64]uint64
	unread        map[uint64]int
	readErr       error
	peerErr       error
	unreadErr     error
}

func newFakeDirectReadTracker() *fakeDirectReadTracker {
	return &fakeDirectReadTracker{ready: true}
}

func (f *fakeDirectReadTracker) AdvanceRead(
	_ context.Context,
	userID, peerID, throughID uint64,
	now time.Time,
) (messaging.DirectReadAdvance, error) {
	f.calls = append(f.calls, directReadAdvanceCall{
		userID:    userID,
		peerID:    peerID,
		throughID: throughID,
		now:       now,
	})
	if f.advance != nil {
		return f.advance(userID, peerID, throughID)
	}
	if f.advanceErr != nil {
		return messaging.DirectReadAdvance{}, f.advanceErr
	}
	return messaging.DirectReadAdvance{
		LastReadMessageID: throughID,
		Candidate:         throughID,
		Advanced:          true,
	}, nil
}

func (f *fakeDirectReadTracker) ReadPositions(
	context.Context,
	uint64,
) (map[uint64]uint64, error) {
	if f.readErr != nil {
		return nil, f.readErr
	}
	if f.positions == nil {
		return map[uint64]uint64{}, nil
	}
	return f.positions, nil
}

func (f *fakeDirectReadTracker) PeerReadPositions(
	context.Context,
	uint64,
	[]uint64,
) (map[uint64]uint64, error) {
	if f.peerErr != nil {
		return nil, f.peerErr
	}
	if f.peerPositions == nil {
		return map[uint64]uint64{}, nil
	}
	return f.peerPositions, nil
}

func (f *fakeDirectReadTracker) UnreadCounts(
	context.Context,
	uint64,
	[]uint64,
	map[uint64]uint64,
) (map[uint64]int, error) {
	if f.unreadErr != nil {
		return nil, f.unreadErr
	}
	if f.unread == nil {
		return map[uint64]int{}, nil
	}
	return f.unread, nil
}

func (f *fakeDirectReadTracker) ReadModelReady(context.Context) (bool, error) {
	return f.ready, f.readyErr
}

func TestBatchMarkReadAdvancesMultipleConversations(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob":   {ID: 2, Username: "bob", Nickname: "Bob"},
		"carol": {ID: 3, Username: "carol", Nickname: "Carol"},
	}
	tracker := newFakeDirectReadTracker()
	router := testRouter(dataStore, WithDirectReadTracker(tracker))

	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/read",
		batchReadRequest{Reads: []batchReadItem{
			{Username: "bob", ThroughID: 105},
			{Username: "carol", ThroughID: 88},
		}},
		authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeBody[batchReadResponse](t, recorder)
	if len(response.Reads) != 2 {
		t.Fatalf("results = %#v, want 2", response.Reads)
	}
	if response.Reads[0].Username != "bob" ||
		response.Reads[0].LastReadMessageID != 105 ||
		!response.Reads[0].Advanced {
		t.Fatalf("first result = %#v", response.Reads[0])
	}
	if response.Reads[1].Username != "carol" || response.Reads[1].LastReadMessageID != 88 {
		t.Fatalf("second result = %#v", response.Reads[1])
	}
	if len(tracker.calls) != 2 {
		t.Fatalf("advance calls = %d, want 2", len(tracker.calls))
	}
	if tracker.calls[0].userID != 1 || tracker.calls[0].peerID != 2 ||
		tracker.calls[0].throughID != 105 {
		t.Fatalf("first call = %#v", tracker.calls[0])
	}
	if dataStore.markReadCall.userID != 0 {
		t.Fatalf("MySQL mark read was called: %#v", dataStore.markReadCall)
	}
}

func TestBatchMarkReadReportsItemErrors(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"alice": {ID: 1, Username: "alice", Nickname: "Alice"},
		"bob":   {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	tracker := newFakeDirectReadTracker()
	router := testRouter(dataStore, WithDirectReadTracker(tracker))

	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/read",
		batchReadRequest{Reads: []batchReadItem{
			{Username: "bob", ThroughID: 10},
			{Username: "alice", ThroughID: 11},
			{Username: "missing", ThroughID: 12},
			{Username: " ", ThroughID: 13},
		}},
		authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeBody[batchReadResponse](t, recorder)
	if len(response.Reads) != 4 {
		t.Fatalf("results = %#v, want 4", response.Reads)
	}
	if response.Reads[0].Error != "" || response.Reads[0].LastReadMessageID != 10 {
		t.Fatalf("first result = %#v", response.Reads[0])
	}
	if response.Reads[1].Error != "cannot_message_self" {
		t.Fatalf("self result = %#v", response.Reads[1])
	}
	if response.Reads[2].Error != "user_not_found" {
		t.Fatalf("missing result = %#v", response.Reads[2])
	}
	if response.Reads[3].Error != "invalid_username" {
		t.Fatalf("blank result = %#v", response.Reads[3])
	}
	if len(tracker.calls) != 1 {
		t.Fatalf("advance calls = %d, want 1", len(tracker.calls))
	}
}

func TestBatchMarkReadRejectsTooManyItems(t *testing.T) {
	dataStore := authenticatedChatStore()
	tracker := newFakeDirectReadTracker()
	router := testRouter(dataStore, WithDirectReadTracker(tracker))

	items := make([]batchReadItem, maxBatchReadItems+1)
	for index := range items {
		items[index] = batchReadItem{Username: "bob", ThroughID: uint64(index + 1)}
	}
	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/read",
		batchReadRequest{Reads: items},
		authTestCookie(),
	)
	if recorder.Code != http.StatusBadRequest || errorCode(t, recorder) != "invalid_request" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestBatchMarkReadRequiresReadModel(t *testing.T) {
	for name, configure := range map[string]func(*fakeDirectReadTracker){
		"not ready": func(tracker *fakeDirectReadTracker) {
			tracker.ready = false
		},
		"ready check failed": func(tracker *fakeDirectReadTracker) {
			tracker.readyErr = messaging.ErrMessageServiceUnavailable
		},
	} {
		t.Run(name, func(t *testing.T) {
			dataStore := authenticatedChatStore()
			dataStore.usersByKey = map[string]store.User{
				"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
			}
			tracker := newFakeDirectReadTracker()
			configure(tracker)
			router := testRouter(dataStore, WithDirectReadTracker(tracker))

			recorder := doJSON(
				t,
				router,
				http.MethodPost,
				"/api/read",
				batchReadRequest{Reads: []batchReadItem{{Username: "bob", ThroughID: 10}}},
				authTestCookie(),
			)
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
			}
			if len(tracker.calls) != 0 {
				t.Fatalf("advance calls = %d, want 0", len(tracker.calls))
			}
		})
	}
}

func TestBatchMarkReadReturnsServiceUnavailable(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.usersByKey = map[string]store.User{
		"bob": {ID: 2, Username: "bob", Nickname: "Bob"},
	}
	tracker := newFakeDirectReadTracker()
	tracker.advanceErr = messaging.ErrMessageServiceUnavailable
	router := testRouter(dataStore, WithDirectReadTracker(tracker))

	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/read",
		batchReadRequest{Reads: []batchReadItem{{Username: "bob", ThroughID: 10}}},
		authTestCookie(),
	)
	if recorder.Code != http.StatusServiceUnavailable ||
		errorCode(t, recorder) != "message_service_unavailable" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestBatchMarkReadRejectsMalformedBody(t *testing.T) {
	dataStore := authenticatedChatStore()
	tracker := newFakeDirectReadTracker()
	router := testRouter(dataStore, WithDirectReadTracker(tracker))

	recorder := doJSON(t, router, http.MethodPost, "/api/read", nil, authTestCookie())
	if recorder.Code != http.StatusBadRequest || errorCode(t, recorder) != "invalid_request" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestLegacyMarkReadUsesReadTracker(t *testing.T) {
	dataStore := authenticatedChatStore()
	tracker := newFakeDirectReadTracker()
	tracker.advance = func(_, _, throughID uint64) (messaging.DirectReadAdvance, error) {
		return messaging.DirectReadAdvance{
			LastReadMessageID: throughID,
			Advanced:          true,
		}, nil
	}
	router := testRouter(dataStore, WithDirectReadTracker(tracker))

	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/conversations/bob/read?through_id=42",
		nil,
		authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeBody[readStateResponse](t, recorder)
	if response.LastReadMessageID != 42 {
		t.Fatalf("last read = %d, want 42", response.LastReadMessageID)
	}
	if len(tracker.calls) != 1 || tracker.calls[0].throughID != 42 {
		t.Fatalf("tracker calls = %#v", tracker.calls)
	}
	if dataStore.markReadCall.userID != 0 {
		t.Fatalf("MySQL mark read was called: %#v", dataStore.markReadCall)
	}
}

func TestListConversationsUsesReadTracker(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.conversations = []store.Conversation{
		{
			UserID:      2,
			User:        store.PublicUser{Username: "bob", Nickname: "Bob"},
			IsFriend:    true,
			UnreadCount: 2,
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
			UserID:   3,
			User:     store.PublicUser{Username: "carol", Nickname: "Carol"},
			IsFriend: true,
		},
		{
			UserID:   4,
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
	tracker := newFakeDirectReadTracker()
	tracker.positions = map[uint64]uint64{2: 10, 4: 3}
	tracker.unread = map[uint64]int{2: 7, 3: 0, 4: 1}
	tracker.peerPositions = map[uint64]uint64{2: 5, 4: 3}
	router := testRouter(dataStore, WithDirectReadTracker(tracker))

	recorder := doJSON(t, router, http.MethodGet, "/api/conversations", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeBody[conversationListResponse](t, recorder)
	if len(response.Conversations) != 3 {
		t.Fatalf("conversations = %d, want 3", len(response.Conversations))
	}
	first := response.Conversations[0]
	if first.User == nil || first.User.Username != "bob" ||
		first.UnreadCount != 7 || first.PeerLastReadMessageID != 5 {
		t.Fatalf("first conversation = %#v", first)
	}
	second := response.Conversations[1]
	if second.User == nil || second.User.Username != "dave" ||
		second.UnreadCount != 1 || second.PeerLastReadMessageID != 3 {
		t.Fatalf("second conversation = %#v", second)
	}
	third := response.Conversations[2]
	if third.User == nil || third.User.Username != "carol" || third.UnreadCount != 0 {
		t.Fatalf("third conversation = %#v", third)
	}
}

func TestListConversationsRequiresReadModel(t *testing.T) {
	dataStore := authenticatedChatStore()
	tracker := newFakeDirectReadTracker()
	tracker.ready = false
	router := testRouter(dataStore, WithDirectReadTracker(tracker))

	recorder := doJSON(t, router, http.MethodGet, "/api/conversations", nil, authTestCookie())
	if recorder.Code != http.StatusServiceUnavailable ||
		errorCode(t, recorder) != "read_model_not_ready" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestBootstrapUsesReadTracker(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.conversations = []store.Conversation{
		{
			UserID:   2,
			User:     store.PublicUser{Username: "bob", Nickname: "Bob"},
			IsFriend: true,
			LastMessage: &store.Message{
				ID:              7,
				SenderID:        2,
				RecipientID:     1,
				ClientMessageID: "client-12345678",
				Body:            "hello",
				CreatedAt:       now,
			},
		},
	}
	dataStore.friends = []store.PublicUser{{Username: "bob", Nickname: "Bob"}}
	tracker := newFakeDirectReadTracker()
	tracker.positions = map[uint64]uint64{2: 5}
	tracker.unread = map[uint64]int{2: 4}
	tracker.peerPositions = map[uint64]uint64{2: 3}
	router := testRouter(dataStore, WithDirectReadTracker(tracker))

	recorder := doJSON(
		t,
		router,
		http.MethodPost,
		"/api/bootstrap",
		map[string]any{},
		authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeBody[bootstrapResponse](t, recorder)
	if len(response.Conversations) != 1 {
		t.Fatalf("conversations = %#v", response.Conversations)
	}
	item := response.Conversations[0]
	if item.User == nil || item.User.Username != "bob" ||
		item.UnreadCount != 4 || item.PeerLastReadMessageID != 3 {
		t.Fatalf("conversation = %#v", item)
	}
}
