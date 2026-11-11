package httpapi

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gorilla/websocket"
)

func directReadCursorFixture(t *testing.T) (*API, *recordingDirectReadTracker, *websocket.Conn, *websocket.Conn) {
	t.Helper()
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	bob := store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore := &fakeStore{
		sessionByTokenHash: chatSessions(alice, bob),
		usersByKey:         map[string]store.User{"alice": alice, "bob": bob},
		relationship:       store.RelationshipFriend,
	}
	tracker := newRecordingDirectReadTracker()
	api, _, baseURL := startChatServer(t, dataStore, WithDirectReadTracker(tracker))
	aliceConn := dialChatSocket(t, baseURL, "alice")
	bobConn := dialChatSocket(t, baseURL, "bob")
	waitFor(t, func() bool {
		return api.hub.activeCount(1) == 1 && api.hub.activeCount(2) == 1
	})
	if err := aliceConn.WriteJSON(map[string]any{
		"type": "focus", "kind": "direct", "username": "bob",
	}); err != nil {
		t.Fatalf("write focus frame: %v", err)
	}
	readWSEventType(t, aliceConn, "focus.ack")
	return api, tracker, aliceConn, bobConn
}

func TestWebSocketReadDirectAdvancesAndPublishes(t *testing.T) {
	_, tracker, aliceConn, bobConn := directReadCursorFixture(t)
	before := len(tracker.snapshot())

	if err := aliceConn.WriteJSON(map[string]any{
		"type": "read", "kind": "direct", "username": "bob", "last_read_seq": 42,
	}); err != nil {
		t.Fatalf("write read frame: %v", err)
	}

	waitFor(t, func() bool {
		for _, call := range tracker.snapshot()[before:] {
			if call.userID == 1 && call.peerID == 2 && call.throughSeq == 42 {
				return true
			}
		}
		return false
	})
	for _, conn := range []*websocket.Conn{aliceConn, bobConn} {
		event := readWSEventType(t, conn, "messages.read")
		if event["reader"] != "alice" || event["peer"] != "bob" ||
			event["last_read_seq"] != float64(42) {
			t.Fatalf("read event = %#v", event)
		}
	}
}

func TestWebSocketReadDirectRequiresMatchingFocus(t *testing.T) {
	_, tracker, aliceConn, _ := directReadCursorFixture(t)
	before := len(tracker.snapshot())

	if err := aliceConn.WriteJSON(map[string]any{
		"type": "read", "kind": "direct", "username": "carol", "last_read_seq": 7,
	}); err != nil {
		t.Fatalf("write mismatched read frame: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if calls := tracker.snapshot()[before:]; len(calls) != 0 {
		t.Fatalf("mismatched read advanced: %#v", calls)
	}

	if err := aliceConn.WriteJSON(map[string]any{
		"type": "read", "kind": "group", "group_id": 9, "last_read_seq": 7,
	}); err != nil {
		t.Fatalf("write wrong-kind read frame: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if calls := tracker.snapshot()[before:]; len(calls) != 0 {
		t.Fatalf("wrong-kind read advanced: %#v", calls)
	}
}

func TestWebSocketReadDirectIgnoresZeroSeq(t *testing.T) {
	_, tracker, aliceConn, _ := directReadCursorFixture(t)
	before := len(tracker.snapshot())

	if err := aliceConn.WriteJSON(map[string]any{
		"type": "read", "kind": "direct", "username": "bob", "last_read_seq": 0,
	}); err != nil {
		t.Fatalf("write zero read frame: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if calls := tracker.snapshot()[before:]; len(calls) != 0 {
		t.Fatalf("zero seq advanced: %#v", calls)
	}

	tracker.setAdvanceFn(func(u, p, s uint64) (messaging.DirectReadAdvance, error) {
		return messaging.DirectReadAdvance{LastReadSeq: 5, Candidate: 5}, nil
	})
	if err := aliceConn.WriteJSON(map[string]any{
		"type": "read", "kind": "direct", "username": "bob", "last_read_seq": 5,
	}); err != nil {
		t.Fatalf("write non-advancing read frame: %v", err)
	}
	waitFor(t, func() bool { return len(tracker.snapshot()) > before })
	assertNoWSEventType(t, aliceConn, "messages.read", 300*time.Millisecond)
}

func TestWebSocketReadGroupAdvancesWithoutBroadcast(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	bob := store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore := &fakeStore{sessionByTokenHash: chatSessions(alice, bob)}
	tracker := newFakeGroupReadTracker()
	tracker.memberIDs = []uint64{1, 2}
	api, _, baseURL := startChatServer(t, dataStore, WithGroupReadTracker(tracker))

	aliceConn := dialChatSocket(t, baseURL, "alice")
	bobConn := dialChatSocket(t, baseURL, "bob")
	waitFor(t, func() bool {
		return api.hub.activeCount(1) == 1 && api.hub.activeCount(2) == 1
	})
	for _, conn := range []*websocket.Conn{aliceConn, bobConn} {
		if err := conn.WriteJSON(map[string]any{
			"type": "focus", "kind": "group", "group_id": 7,
		}); err != nil {
			t.Fatalf("write focus frame: %v", err)
		}
		readWSEventType(t, conn, "focus.ack")
	}
	before := len(tracker.snapAdvances())

	if err := aliceConn.WriteJSON(map[string]any{
		"type": "read", "kind": "group", "group_id": 7, "last_read_seq": 128,
	}); err != nil {
		t.Fatalf("write group read frame: %v", err)
	}
	waitFor(t, func() bool {
		for _, call := range tracker.snapAdvances()[before:] {
			if call.groupID == 7 && call.userID == 1 && call.throughSeq == 128 {
				return true
			}
		}
		return false
	})
	// Group receipts are pulled by each sender's client; an advance must not
	// fan out to the whole group.
	assertNoWSEventType(t, bobConn, "group.read", 300*time.Millisecond)
	assertNoWSEventType(t, aliceConn, "group.read", 300*time.Millisecond)
}

func TestWebSocketReadGroupRequiresMatchingFocus(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	dataStore := &fakeStore{sessionByTokenHash: chatSessions(alice)}
	tracker := newFakeGroupReadTracker()
	api, _, baseURL := startChatServer(t, dataStore, WithGroupReadTracker(tracker))
	conn := dialChatSocket(t, baseURL, "alice")
	waitFor(t, func() bool { return api.hub.activeCount(1) == 1 })

	if err := conn.WriteJSON(map[string]any{
		"type": "focus", "kind": "group", "group_id": 7,
	}); err != nil {
		t.Fatalf("write focus frame: %v", err)
	}
	readWSEventType(t, conn, "focus.ack")
	before := len(tracker.snapAdvances())

	if err := conn.WriteJSON(map[string]any{
		"type": "read", "kind": "group", "group_id": 8, "last_read_seq": 12,
	}); err != nil {
		t.Fatalf("write mismatched group read frame: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if calls := tracker.snapAdvances()[before:]; len(calls) != 0 {
		t.Fatalf("mismatched group read advanced: %#v", calls)
	}
}

func TestWebSocketReadGroupTerminalErrorClearsFocus(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	dataStore := &fakeStore{sessionByTokenHash: chatSessions(alice)}
	tracker := newFakeGroupReadTracker()
	api, _, baseURL := startChatServer(t, dataStore, WithGroupReadTracker(tracker))
	conn := dialChatSocket(t, baseURL, "alice")
	waitFor(t, func() bool { return api.hub.activeCount(1) == 1 })

	if err := conn.WriteJSON(map[string]any{
		"type": "focus", "kind": "group", "group_id": 7,
	}); err != nil {
		t.Fatalf("write focus frame: %v", err)
	}
	readWSEventType(t, conn, "focus.ack")
	tracker.setAdvanceErr(messaging.ErrGroupNotMember)

	if err := conn.WriteJSON(map[string]any{
		"type": "read", "kind": "group", "group_id": 7, "last_read_seq": 12,
	}); err != nil {
		t.Fatalf("write group read frame: %v", err)
	}
	waitFor(t, func() bool { return !api.hub.isGroupFocus(1, 7) })
}

func assertNoWSEventType(
	t *testing.T,
	conn *websocket.Conn,
	eventType string,
	timeout time.Duration,
) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var event map[string]any
		if json.Unmarshal(payload, &event) != nil {
			continue
		}
		if event["type"] == eventType {
			t.Fatalf("unexpected %q event: %#v", eventType, event)
		}
	}
}
