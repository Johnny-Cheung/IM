package httpapi

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func TestImplicitGroupReadAdvancesEverySender(t *testing.T) {
	tracker := newFakeGroupReadTracker()
	api := New(&fakeStore{}, WithGroupReadTracker(tracker))
	api.Start()
	t.Cleanup(api.Stop)

	api.scheduleGroupImplicitRead(7, 1, 10, []uint64{1, 2})
	api.scheduleGroupImplicitRead(7, 2, 11, []uint64{1, 2})

	waitFor(t, func() bool {
		positions := make(map[uint64]uint64)
		for _, call := range tracker.snapAdvances() {
			if call.groupID == 7 {
				positions[call.userID] = call.throughID
			}
		}
		return positions[1] == 10 && positions[2] == 11
	})
	time.Sleep(150 * time.Millisecond)

	for _, call := range tracker.snapAdvances() {
		if call.groupID != 7 {
			continue
		}
		want := map[uint64]uint64{1: 10, 2: 11}[call.userID]
		if call.throughID != want {
			t.Fatalf("advance = %#v, sender %d advanced to a message it did not send",
				call, call.userID)
		}
	}
}

func TestImplicitReadStopBoundsDrain(t *testing.T) {
	previous := implicitReadDrainTimeout
	implicitReadDrainTimeout = 200 * time.Millisecond
	t.Cleanup(func() { implicitReadDrainTimeout = previous })

	started := make(chan struct{}, 1)
	tracker := newFakeGroupReadTracker()
	tracker.advanceCtxFn = func(
		ctx context.Context, _, _, _ uint64,
	) (messaging.GroupReadAdvance, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return messaging.GroupReadAdvance{}, ctx.Err()
	}
	api := New(&fakeStore{}, WithGroupReadTracker(tracker))
	api.Start()
	for groupID := uint64(1); groupID <= 4; groupID++ {
		api.scheduleGroupImplicitRead(groupID, 1, 10, []uint64{1})
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("implicit read flush did not start")
	}
	begin := time.Now()
	api.Stop()
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Fatalf("Stop blocked for %v with 4 due batches", elapsed)
	}
}

func TestMarkGroupReadLogsStreamFailure(t *testing.T) {
	dataStore := authenticatedChatStore()
	dataStore.groupMemberIDs = []uint64{1, 2}
	tracker := newFakeGroupReadTracker()
	tracker.advanceFn = func(_, userID, throughID uint64) (messaging.GroupReadAdvance, error) {
		return messaging.GroupReadAdvance{
			UserID: userID, LastReadMessageID: throughID, Advanced: true, StreamError: true,
		}, nil
	}
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	var logged bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logged)
	defer log.SetOutput(previous)

	recorder := doJSON(
		t, router, http.MethodPost, "/api/groups/7/read?through_id=42", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(logged.String(), "group read stream append failed") {
		t.Fatalf("stream failure was not logged: %q", logged.String())
	}
}

func TestGroupFocusHidesGroupState(t *testing.T) {
	cases := map[string]error{
		"not_member": messaging.ErrGroupNotMember,
		"dissolved":  messaging.ErrGroupDissolved,
		"missing":    messaging.ErrMessageServiceUnavailable,
	}
	for name, advanceErr := range cases {
		t.Run(name, func(t *testing.T) {
			alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
			dataStore := &fakeStore{sessionByTokenHash: chatSessions(alice)}
			tracker := newFakeGroupReadTracker()
			tracker.advanceErr = advanceErr
			api, _, baseURL := startChatServer(t, dataStore, WithGroupReadTracker(tracker))
			conn := dialChatSocket(t, baseURL, "alice")
			waitFor(t, func() bool { return api.hub.activeCount(1) == 1 })

			if err := conn.WriteJSON(map[string]any{
				"type": "focus", "kind": "group", "group_id": 7,
			}); err != nil {
				t.Fatalf("write focus frame: %v", err)
			}
			ack := readWSEventType(t, conn, "focus.ack")
			if ack["error"] != "not_group_member" {
				t.Fatalf("ack = %#v, want one code for every unavailable group", ack)
			}
		})
	}
}

func TestWebSocketFocusDirectAllowsExistingConversation(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	bob := store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore := &fakeStore{
		sessionByTokenHash: chatSessions(alice, bob),
		usersByKey:         map[string]store.User{"bob": bob},
		relationship:       store.RelationshipNone,
		hasConversation:    true,
	}
	tracker := newRecordingDirectReadTracker()
	tracker.advanceFn = func(_, _, throughID uint64) (messaging.DirectReadAdvance, error) {
		return messaging.DirectReadAdvance{LastReadMessageID: throughID, Advanced: true}, nil
	}
	api, _, baseURL := startChatServer(t, dataStore, WithDirectReadTracker(tracker))
	conn := dialChatSocket(t, baseURL, "alice")
	waitFor(t, func() bool { return api.hub.activeCount(1) == 1 })

	if err := conn.WriteJSON(map[string]any{
		"type": "focus", "kind": "direct", "username": "bob",
	}); err != nil {
		t.Fatalf("write focus frame: %v", err)
	}
	ack := readWSEventType(t, conn, "focus.ack")
	if ack["error"] != nil || ack["kind"] != "direct" {
		t.Fatalf("ack = %#v, want an existing conversation focusable", ack)
	}
	if calls := tracker.snapshot(); len(calls) != 1 || calls[0].userID != 1 {
		t.Fatalf("advance calls = %#v", calls)
	}
	if !api.hub.isDirectFocus(1, 2) {
		t.Fatal("focus was not recorded on the connection")
	}
}

func TestGroupReadStatsHidesMessageExistenceFromNonMembers(t *testing.T) {
	dataStore := authenticatedFriendStore()
	tracker := newFakeGroupReadTracker()
	tracker.senderFound = true
	tracker.senderID = 2
	tracker.statsErr = messaging.ErrGroupNotMember
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/read-stats?message_id=345", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusNotFound || errorCode(t, recorder) != "group_not_found" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}
