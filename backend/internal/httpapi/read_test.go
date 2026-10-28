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
	userID     uint64
	peerID     uint64
	throughSeq uint64
	now        time.Time
}

type fakeDirectReadTracker struct {
	ready      bool
	readyErr   error
	advance    func(userID, peerID, throughSeq uint64) (messaging.DirectReadAdvance, error)
	advanceErr error
	calls      []directReadAdvanceCall

	states    map[uint64]messaging.DirectConversationState
	statesErr error
	stateIDs  [][]uint64
}

func newFakeDirectReadTracker() *fakeDirectReadTracker {
	return &fakeDirectReadTracker{ready: true}
}

func (f *fakeDirectReadTracker) AdvanceRead(
	_ context.Context,
	userID, peerID, throughSeq uint64,
	now time.Time,
) (messaging.DirectReadAdvance, error) {
	f.calls = append(f.calls, directReadAdvanceCall{
		userID:     userID,
		peerID:     peerID,
		throughSeq: throughSeq,
		now:        now,
	})
	if f.advance != nil {
		return f.advance(userID, peerID, throughSeq)
	}
	if f.advanceErr != nil {
		return messaging.DirectReadAdvance{}, f.advanceErr
	}
	return messaging.DirectReadAdvance{
		LastReadSeq: throughSeq,
		Candidate:   throughSeq,
		Advanced:    true,
	}, nil
}

func (f *fakeDirectReadTracker) States(
	_ context.Context,
	_ uint64,
	peerIDs []uint64,
) (map[uint64]messaging.DirectConversationState, error) {
	f.stateIDs = append(f.stateIDs, append([]uint64{}, peerIDs...))
	if f.statesErr != nil {
		return nil, f.statesErr
	}
	states := make(map[uint64]messaging.DirectConversationState, len(peerIDs))
	for _, peerID := range peerIDs {
		states[peerID] = f.states[peerID]
	}
	return states, nil
}

func (f *fakeDirectReadTracker) ReadModelReady(context.Context) (bool, error) {
	return f.ready, f.readyErr
}

func TestLegacyReadEndpointsAreGone(t *testing.T) {
	router := testRouter(authenticatedChatStore())

	for _, path := range []string{
		"/api/read",
		"/api/conversations/bob/read",
		"/api/groups/7/read",
	} {
		recorder := doJSON(t, router, http.MethodPost, path, map[string]any{}, authTestCookie())
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want %d", path, recorder.Code, http.StatusNotFound)
		}
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
			LastSeq:     4,
			LastMessage: &store.Message{
				ID:              7,
				Seq:             4,
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
			LastSeq:  3,
			LastMessage: &store.Message{
				ID:              6,
				Seq:             3,
				SenderID:        4,
				RecipientID:     1,
				ClientMessageID: "client-87654321",
				Body:            "old message",
				CreatedAt:       now.Add(-time.Minute),
			},
		},
	}
	tracker := newFakeDirectReadTracker()
	tracker.states = map[uint64]messaging.DirectConversationState{
		2: {LastSeq: 9, ReadSeq: 2, PeerReadSeq: 5, Unread: 7},
		3: {},
		4: {LastSeq: 4, ReadSeq: 3, PeerReadSeq: 3, Unread: 1},
	}
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
		first.UnreadCount != 7 || first.LastSeq != 9 || first.PeerLastReadSeq != 5 {
		t.Fatalf("first conversation = %#v", first)
	}
	if first.LastMessage == nil || first.LastMessage.Seq != 4 {
		t.Fatalf("first last message = %#v", first.LastMessage)
	}
	second := response.Conversations[1]
	if second.User == nil || second.User.Username != "dave" ||
		second.UnreadCount != 1 || second.LastSeq != 4 || second.PeerLastReadSeq != 3 {
		t.Fatalf("second conversation = %#v", second)
	}
	third := response.Conversations[2]
	if third.User == nil || third.User.Username != "carol" || third.UnreadCount != 0 ||
		third.LastSeq != 0 || third.PeerLastReadSeq != 0 {
		t.Fatalf("third conversation = %#v", third)
	}
	if len(tracker.stateIDs) != 1 || len(tracker.stateIDs[0]) != 3 {
		t.Fatalf("state calls = %#v, want one call for every peer", tracker.stateIDs)
	}
}

func TestListConversationsFallsBackToStoreWithoutRedis(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedChatStore()
	dataStore.conversations = []store.Conversation{{
		UserID:          2,
		User:            store.PublicUser{Username: "bob", Nickname: "Bob"},
		IsFriend:        true,
		UnreadCount:     3,
		LastSeq:         8,
		PeerLastReadSeq: 2,
		LastMessage: &store.Message{
			ID:              7,
			Seq:             8,
			SenderID:        2,
			RecipientID:     1,
			ClientMessageID: "client-12345678",
			Body:            "hello",
			CreatedAt:       now,
		},
	}}
	router := testRouter(dataStore)

	recorder := doJSON(t, router, http.MethodGet, "/api/conversations", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeBody[conversationListResponse](t, recorder)
	if len(response.Conversations) != 1 {
		t.Fatalf("conversations = %#v", response.Conversations)
	}
	item := response.Conversations[0]
	if item.LastSeq != 8 || item.PeerLastReadSeq != 2 || item.UnreadCount != 3 {
		t.Fatalf("conversation = %#v", item)
	}
	if item.LastMessage == nil || item.LastMessage.Seq != 8 {
		t.Fatalf("last message = %#v", item.LastMessage)
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
			LastSeq:  7,
			LastMessage: &store.Message{
				ID:              7,
				Seq:             7,
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
	tracker.states = map[uint64]messaging.DirectConversationState{
		2: {LastSeq: 7, ReadSeq: 3, PeerReadSeq: 3, Unread: 4},
	}
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
		item.UnreadCount != 4 || item.LastSeq != 7 || item.PeerLastReadSeq != 3 {
		t.Fatalf("conversation = %#v", item)
	}
}
