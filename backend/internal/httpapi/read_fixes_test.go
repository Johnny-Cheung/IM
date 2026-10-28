package httpapi

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
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
				positions[call.userID] = call.throughSeq
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
		if call.throughSeq != want {
			t.Fatalf("advance = %#v, sender %d advanced to a message it did not send",
				call, call.userID)
		}
	}
}

func TestSendGroupMessageFallsBackToRedisRecipients(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	bob := store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore := &fakeStore{
		sessionByTokenHash:     chatSessions(alice, bob),
		groupMemberIDsErr:      errors.New("mysql: connection reset"),
		createdGroupMessageNew: true,
		createdGroupMessage: store.GroupMessage{
			ID: 12, Seq: 12, GroupID: 7, SenderID: 1,
			Sender:          store.PublicUser{Username: "alice", Nickname: "Alice"},
			ClientMessageID: "group-client-1",
			Body:            "hello",
			CreatedAt:       time.Now().UTC(),
		},
	}
	tracker := newFakeGroupReadTracker()
	tracker.memberIDs = []uint64{1, 2}
	api, router, baseURL := startChatServer(t, dataStore, WithGroupReadTracker(tracker))
	api.Start()
	t.Cleanup(api.Stop)
	conn := dialChatSocket(t, baseURL, "bob")
	waitFor(t, func() bool { return api.hub.activeCount(2) == 1 })

	recorder := doJSON(t, router, http.MethodPost, "/api/groups/7/messages", map[string]string{
		"body":              "hello",
		"client_message_id": "group-client-1",
	}, testCookie("alice"))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	event := readWSEventType(t, conn, "group.message.created")
	message, ok := event["message"].(map[string]any)
	if !ok || message["seq"] != float64(12) {
		t.Fatalf("event = %#v", event)
	}
	waitFor(t, func() bool {
		for _, advance := range tracker.snapAdvances() {
			if advance.groupID == 7 && advance.userID == 1 && advance.throughSeq == 12 {
				return true
			}
		}
		return false
	})
}

func TestImplicitReadFlushReportsRetryableFailures(t *testing.T) {
	direct := newFakeDirectReadTracker()
	api := New(&fakeStore{}, WithDirectReadTracker(direct))
	directBatch := implicitReadBatch{
		direct: true, senderID: 1, recipientID: 2, throughSeq: 5,
	}
	direct.advanceErr = errors.New("redis: connection reset")
	if ok := api.flushDirectImplicitRead(context.Background(), &directBatch); ok {
		t.Fatal("direct flush reported success after a Redis error")
	}
	direct.advanceErr = nil
	if ok := api.flushDirectImplicitRead(context.Background(), &directBatch); !ok {
		t.Fatal("direct flush reported failure without an error")
	}

	group := newFakeGroupReadTracker()
	groupAPI := New(&fakeStore{}, WithGroupReadTracker(group))
	groupBatch := implicitReadBatch{groupID: 7, senderID: 1, throughSeq: 5}
	group.advanceErr = errors.New("redis: connection reset")
	if ok := groupAPI.flushGroupImplicitRead(context.Background(), &groupBatch); ok {
		t.Fatal("group flush reported success after a Redis error")
	}
	group.advanceErr = messaging.ErrGroupDissolved
	if ok := groupAPI.flushGroupImplicitRead(context.Background(), &groupBatch); !ok {
		t.Fatal("a dissolved group must retire the batch")
	}
}

func TestImplicitReadRequeuesFailedAdvance(t *testing.T) {
	var attempts atomic.Int32
	tracker := newFakeGroupReadTracker()
	tracker.advanceFn = func(_, userID, throughSeq uint64) (messaging.GroupReadAdvance, error) {
		if attempts.Add(1) == 1 {
			return messaging.GroupReadAdvance{}, errors.New("redis: connection reset")
		}
		return messaging.GroupReadAdvance{
			UserID: userID, LastReadSeq: throughSeq, Advanced: true,
		}, nil
	}
	api := New(&fakeStore{}, WithGroupReadTracker(tracker))
	api.Start()
	t.Cleanup(api.Stop)

	api.scheduleGroupImplicitRead(7, 1, 10, []uint64{1})
	waitFor(t, func() bool { return attempts.Load() >= 2 })
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

func TestImplicitReadRequeuesAfterSlowBatch(t *testing.T) {
	previous := implicitReadDrainTimeout
	implicitReadDrainTimeout = 100 * time.Millisecond
	t.Cleanup(func() { implicitReadDrainTimeout = previous })

	var stalled atomic.Bool
	var mu sync.Mutex
	succeeded := make(map[uint64]bool)
	tracker := newFakeGroupReadTracker()
	tracker.advanceCtxFn = func(
		ctx context.Context, groupID, userID, throughSeq uint64,
	) (messaging.GroupReadAdvance, error) {
		if stalled.CompareAndSwap(false, true) {
			<-ctx.Done()
			return messaging.GroupReadAdvance{}, ctx.Err()
		}
		mu.Lock()
		succeeded[groupID] = true
		mu.Unlock()
		return messaging.GroupReadAdvance{
			UserID: userID, LastReadSeq: throughSeq, Advanced: true,
		}, nil
	}
	api := New(&fakeStore{}, WithGroupReadTracker(tracker))
	api.Start()
	t.Cleanup(api.Stop)
	for groupID := uint64(1); groupID <= 3; groupID++ {
		api.scheduleGroupImplicitRead(groupID, 1, 10, []uint64{1})
	}

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return succeeded[1] && succeeded[2] && succeeded[3] && stalled.Load()
	})
}

func TestImplicitReadRequeueOnlyChargesAttemptedBatch(t *testing.T) {
	tracker := newFakeDirectReadTracker()
	tracker.advanceErr = errors.New("redis: connection reset")
	api := New(&fakeStore{}, WithDirectReadTracker(tracker))
	reader := api.implicitReads

	const batchCount = 6
	batches := make([]*implicitReadBatch, 0, batchCount)
	for peerID := uint64(1); peerID <= batchCount; peerID++ {
		key := implicitReadKey{direct: true, first: 1, second: peerID}
		batch := &implicitReadBatch{
			key:         key,
			direct:      true,
			senderID:    1,
			recipientID: peerID,
			throughSeq:  5,
			queuedAt:    time.Now().Add(-2 * implicitReadWindow),
		}
		reader.pending[key] = batch
		batches = append(batches, batch)
	}

	reader.flushDue()

	attempted := 0
	for _, batch := range batches {
		if batch.attempts == 0 {
			if _, ok := reader.pending[batch.key]; !ok {
				t.Fatalf("untouched batch %#v was dropped", batch.key)
			}
			continue
		}
		attempted++
		if batch.attempts != 1 {
			t.Fatalf("attempts = %d, want 1 for one flush pass", batch.attempts)
		}
	}
	if attempted != 1 {
		t.Fatalf("attempted batches = %d, want exactly one", attempted)
	}
	if len(reader.pending) != batchCount {
		t.Fatalf("pending = %d, want %d", len(reader.pending), batchCount)
	}

	for pass := 0; pass < batchCount*(implicitReadMaxRetries+1)+batchCount; pass++ {
		if len(reader.pending) == 0 {
			break
		}
		reader.mu.Lock()
		for _, batch := range reader.pending {
			batch.queuedAt = time.Now().Add(-2 * implicitReadWindow)
		}
		reader.mu.Unlock()
		reader.flushDue()
	}
	if len(reader.pending) != 0 {
		t.Fatalf("pending = %d, want every batch retired", len(reader.pending))
	}

	attemptsByPeer := make(map[uint64]int)
	for _, call := range tracker.calls {
		if call.userID == 1 {
			attemptsByPeer[call.peerID]++
		}
	}
	for _, batch := range batches {
		if batch.attempts != implicitReadMaxRetries+1 {
			t.Fatalf("batch %#v attempts = %d, want %d",
				batch.key, batch.attempts, implicitReadMaxRetries+1)
		}
		if attemptsByPeer[batch.recipientID] != batch.attempts {
			t.Fatalf("peer %d was attempted %d times but charged %d",
				batch.recipientID, attemptsByPeer[batch.recipientID], batch.attempts)
		}
	}
}

func TestGroupFocusLogsStreamFailure(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	dataStore := &fakeStore{
		sessionByTokenHash: chatSessions(alice),
		groupMemberIDs:     []uint64{1, 2},
	}
	tracker := newFakeGroupReadTracker()
	tracker.advanceFn = func(_, userID, throughSeq uint64) (messaging.GroupReadAdvance, error) {
		return messaging.GroupReadAdvance{
			UserID: userID, LastReadSeq: throughSeq, Advanced: true, StreamError: true,
		}, nil
	}
	api, _, baseURL := startChatServer(t, dataStore, WithGroupReadTracker(tracker))
	conn := dialChatSocket(t, baseURL, "alice")
	waitFor(t, func() bool { return api.hub.activeCount(1) == 1 })

	var logged bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logged)
	defer log.SetOutput(previous)

	if err := conn.WriteJSON(map[string]any{
		"type": "focus", "kind": "group", "group_id": 7,
	}); err != nil {
		t.Fatalf("write focus frame: %v", err)
	}
	ack := readWSEventType(t, conn, "focus.ack")
	if ack["error"] != nil {
		t.Fatalf("ack = %#v", ack)
	}
	if !strings.Contains(logged.String(), "group read stream append failed") {
		t.Fatalf("stream failure was not logged: %q", logged.String())
	}
}

func TestGroupFocusHidesGroupState(t *testing.T) {
	cases := map[string]struct {
		err  error
		code string
	}{
		"not_member": {err: messaging.ErrGroupNotMember, code: "not_group_member"},
		"dissolved":  {err: messaging.ErrGroupDissolved, code: "not_group_member"},
		"missing":    {err: messaging.ErrMessageServiceUnavailable, code: "message_service_unavailable"},
		"seq_gate":   {err: messaging.ErrSeqNotReady, code: "message_service_unavailable"},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
			dataStore := &fakeStore{sessionByTokenHash: chatSessions(alice)}
			tracker := newFakeGroupReadTracker()
			tracker.advanceErr = testCase.err
			api, _, baseURL := startChatServer(t, dataStore, WithGroupReadTracker(tracker))
			conn := dialChatSocket(t, baseURL, "alice")
			waitFor(t, func() bool { return api.hub.activeCount(1) == 1 })

			if err := conn.WriteJSON(map[string]any{
				"type": "focus", "kind": "group", "group_id": 7,
			}); err != nil {
				t.Fatalf("write focus frame: %v", err)
			}
			ack := readWSEventType(t, conn, "focus.ack")
			if ack["error"] != testCase.code {
				t.Fatalf("ack = %#v, want error %q", ack, testCase.code)
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
	tracker.advanceFn = func(_, _, throughSeq uint64) (messaging.DirectReadAdvance, error) {
		return messaging.DirectReadAdvance{LastReadSeq: throughSeq, Advanced: true}, nil
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
	tracker.statsErr = messaging.ErrGroupNotMember
	router := testRouter(dataStore, WithGroupReadTracker(tracker))

	recorder := doJSON(
		t, router, http.MethodGet, "/api/groups/7/read-stats?seq=4", nil, authTestCookie(),
	)
	if recorder.Code != http.StatusNotFound || errorCode(t, recorder) != "group_not_found" {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestWebSocketFocusDirectReportsSeqGate(t *testing.T) {
	alice := store.User{ID: 1, Username: "alice", Nickname: "Alice"}
	bob := store.User{ID: 2, Username: "bob", Nickname: "Bob"}
	dataStore := &fakeStore{
		sessionByTokenHash: chatSessions(alice, bob),
		usersByKey:         map[string]store.User{"bob": bob},
		relationship:       store.RelationshipFriend,
	}
	tracker := newRecordingDirectReadTracker()
	tracker.advanceFn = func(_, _, _ uint64) (messaging.DirectReadAdvance, error) {
		return messaging.DirectReadAdvance{}, messaging.ErrSeqNotReady
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
	if ack["error"] != "message_service_unavailable" {
		t.Fatalf("ack = %#v, want a service-unavailable error", ack)
	}
	if api.hub.isDirectFocus(1, 2) {
		t.Fatal("focus was recorded while the gate was closed")
	}
}
