package store

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestMaxSafeSeq(t *testing.T) {
	if MaxSafeSeq != 1<<53-1 {
		t.Fatalf("MaxSafeSeq = %d, want %d", MaxSafeSeq, uint64(1<<53-1))
	}
}

func TestSeqMigrations(t *testing.T) {
	cases := []struct {
		name      string
		fragments []string
	}{
		{
			"023_add_message_conversation_seq.sql",
			[]string{
				"ALTER TABLE messages",
				"ADD COLUMN conversation_low_id BIGINT UNSIGNED NULL",
				"ADD COLUMN conversation_high_id BIGINT UNSIGNED NULL",
				"ADD COLUMN seq BIGINT UNSIGNED NULL",
			},
		},
		{
			"024_add_message_conversation_indexes.sql",
			[]string{
				"UNIQUE KEY uk_messages_conversation_seq (conversation_low_id, conversation_high_id, seq)",
				"KEY idx_messages_conversation_server (conversation_low_id, conversation_high_id, server_message_id)",
			},
		},
		{
			"025_add_group_message_seq.sql",
			[]string{"ALTER TABLE group_messages ADD COLUMN seq BIGINT UNSIGNED NULL"},
		},
		{
			"026_add_group_message_seq_unique.sql",
			[]string{"UNIQUE KEY uk_group_messages_group_seq (group_id, seq)"},
		},
		{
			"027_add_conversation_read_last_read_seq.sql",
			[]string{"last_read_seq BIGINT UNSIGNED NOT NULL DEFAULT 0"},
		},
		{
			"028_add_group_member_last_read_seq.sql",
			[]string{"last_read_seq BIGINT UNSIGNED NOT NULL DEFAULT 0"},
		},
	}
	for _, testCase := range cases {
		body, err := migrationFiles.ReadFile("migrations/" + testCase.name)
		if err != nil {
			t.Fatalf("read %s: %v", testCase.name, err)
		}
		sql := string(body)
		for _, fragment := range testCase.fragments {
			if !strings.Contains(sql, fragment) {
				t.Fatalf("%s does not contain %q", testCase.name, fragment)
			}
		}
		if strings.Count(sql, ";") != 1 {
			t.Fatalf("%s has %d statements, want exactly one", testCase.name, strings.Count(sql, ";"))
		}
		upper := strings.ToUpper(sql)
		for _, forbidden := range []string{"CHECK", "GENERATED", "UPDATE ", "INSERT "} {
			if strings.Contains(upper, forbidden) {
				t.Fatalf("%s contains forbidden %q: migrations only change structure", testCase.name, forbidden)
			}
		}
	}
}

func TestDirectConversationPair(t *testing.T) {
	cases := []struct {
		left, right uint64
		low, high   uint64
	}{
		{5, 3, 3, 5},
		{3, 5, 3, 5},
		{4, 4, 4, 4},
	}
	for _, testCase := range cases {
		low, high := directConversationPair(testCase.left, testCase.right)
		if low != testCase.low || high != testCase.high {
			t.Fatalf("directConversationPair(%d, %d) = (%d, %d), want (%d, %d)",
				testCase.left, testCase.right, low, high, testCase.low, testCase.high)
		}
	}
}

func TestNextSeqAfter(t *testing.T) {
	if seq, err := nextSeqAfter(0); err != nil || seq != 1 {
		t.Fatalf("nextSeqAfter(0) = (%d, %v), want (1, nil)", seq, err)
	}
	if seq, err := nextSeqAfter(MaxSafeSeq - 1); err != nil || seq != MaxSafeSeq {
		t.Fatalf("nextSeqAfter(max-1) = (%d, %v), want (%d, nil)", seq, err, MaxSafeSeq)
	}
	if _, err := nextSeqAfter(MaxSafeSeq); !errors.Is(err, ErrSeqExhausted) {
		t.Fatalf("nextSeqAfter(max) error = %v, want ErrSeqExhausted", err)
	}
}

func TestSameMessageComparesSeq(t *testing.T) {
	now := seqTestTime()
	base := Message{ID: 7, Seq: 3, SenderID: 1, RecipientID: 2, ClientMessageID: "c", Body: "b", CreatedAt: now}
	if !sameMessage(base, base) {
		t.Fatal("sameMessage() = false for identical messages")
	}
	other := base
	other.Seq = 4
	if sameMessage(base, other) {
		t.Fatal("sameMessage() ignores seq")
	}
}

func TestSameGroupMessageComparesSeq(t *testing.T) {
	now := seqTestTime()
	base := GroupMessage{ID: 7, Seq: 3, GroupID: 5, SenderID: 1, ClientMessageID: "c", Body: "b", CreatedAt: now}
	if !sameGroupMessage(base, base) {
		t.Fatal("sameGroupMessage() = false for identical messages")
	}
	other := base
	other.Seq = 4
	if sameGroupMessage(base, other) {
		t.Fatal("sameGroupMessage() ignores seq")
	}
}

func TestPersistMessageRejectsInvalidSeq(t *testing.T) {
	store := &Store{}
	ctx := context.Background()
	if _, err := store.PersistMessage(ctx, Message{ID: 1, Seq: 0}); !errors.Is(err, ErrSeqInvalid) {
		t.Fatalf("PersistMessage(seq 0) error = %v, want ErrSeqInvalid", err)
	}
	if _, err := store.PersistMessage(ctx, Message{ID: 1, Seq: MaxSafeSeq + 1}); !errors.Is(err, ErrSeqInvalid) {
		t.Fatalf("PersistMessage(seq too large) error = %v, want ErrSeqInvalid", err)
	}
	if _, err := store.PersistGroupMessage(ctx, GroupMessage{ID: 1, Seq: 0}); !errors.Is(err, ErrSeqInvalid) {
		t.Fatalf("PersistGroupMessage(seq 0) error = %v, want ErrSeqInvalid", err)
	}
	if _, err := store.PersistGroupMessage(ctx, GroupMessage{ID: 1, Seq: MaxSafeSeq + 1}); !errors.Is(err, ErrSeqInvalid) {
		t.Fatalf("PersistGroupMessage(seq too large) error = %v, want ErrSeqInvalid", err)
	}
}

func TestUpsertConversationReadSkipsZeroSeq(t *testing.T) {
	store := &Store{}
	ctx := context.Background()
	if err := store.UpsertConversationRead(ctx, 1, 2, 0, seqTestTime()); err != nil {
		t.Fatalf("UpsertConversationRead(seq 0) error = %v, want nil", err)
	}
	if err := store.UpsertConversationRead(ctx, 1, 1, 5, seqTestTime()); !errors.Is(err, ErrCannotMessageSelf) {
		t.Fatalf("UpsertConversationRead(self) error = %v, want ErrCannotMessageSelf", err)
	}
}

func messageColumns() []string {
	return []string{
		"server_message_id", "seq", "sender_id", "recipient_id",
		"client_message_id", "body", "created_at",
	}
}

func groupMessageColumns() []string {
	return []string{
		"server_message_id", "seq", "group_id", "sender_id",
		"username", "nickname", "client_message_id", "body", "created_at",
	}
}

func TestListMessagesUsesSeqCursor(t *testing.T) {
	now := seqTestTime()
	store, script := newScriptStore(t, scriptStep{
		match:   "FROM messages WHERE conversation_low_id = ? AND conversation_high_id = ?",
		columns: messageColumns(),
		rows: [][]driver.Value{
			{int64(22), int64(8), int64(1), int64(2), "c2", "two", now},
			{int64(21), int64(7), int64(2), int64(1), "c1", "one", now},
		},
	})

	page, err := store.ListMessages(context.Background(), 2, 1, 7, 10)
	if err != nil {
		t.Fatalf("ListMessages() error = %v", err)
	}
	if len(page.Messages) != 2 || page.Messages[0].Seq != 7 || page.Messages[1].Seq != 8 {
		t.Fatalf("ListMessages() = %+v, want seqs 7 then 8", page.Messages)
	}
	script.assertQueryContains(t, 0, "conversation_low_id = ?", "conversation_high_id = ?", "AND seq < ?", "ORDER BY seq DESC")
	script.assertArg(t, 0, 0, int64(1))
	script.assertArg(t, 0, 1, int64(2))
	script.assertArg(t, 0, 2, int64(7))
	script.assertArg(t, 0, 3, int64(11))
	script.assertDone(t)
}

func TestListMessagesAfterUsesSeqCursor(t *testing.T) {
	now := seqTestTime()
	store, script := newScriptStore(t, scriptStep{
		match:   "FROM messages WHERE",
		columns: messageColumns(),
		rows: [][]driver.Value{
			{int64(30), int64(9), int64(1), int64(2), "c9", "nine", now},
		},
	})

	page, err := store.ListMessagesAfter(context.Background(), 1, 2, 29, 10)
	if err != nil {
		t.Fatalf("ListMessagesAfter() error = %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].Seq != 9 {
		t.Fatalf("ListMessagesAfter() = %+v, want one message with seq 9", page.Messages)
	}
	script.assertQueryContains(t, 0, "seq > ?", "ORDER BY seq ASC")
	script.assertArg(t, 0, 2, int64(29))
	script.assertDone(t)
}

func TestListGroupMessagesUsesSeqCursor(t *testing.T) {
	now := seqTestTime()
	store, script := newScriptStore(t,
		scriptStep{
			match:   "SELECT EXISTS(",
			columns: []string{"found"},
			rows:    [][]driver.Value{{int64(1)}},
		},
		scriptStep{
			match:   "FROM group_messages gm",
			columns: groupMessageColumns(),
			rows: [][]driver.Value{
				{int64(40), int64(6), int64(7), int64(1), "alice", "Alice", "c6", "six", now},
				{int64(39), int64(5), int64(7), int64(2), "bob", "Bob", "c5", "five", now},
			},
		},
	)

	page, err := store.ListGroupMessages(context.Background(), 1, 7, 6, 10)
	if err != nil {
		t.Fatalf("ListGroupMessages() error = %v", err)
	}
	if len(page.Messages) != 2 || page.Messages[0].Seq != 5 || page.Messages[1].Seq != 6 {
		t.Fatalf("ListGroupMessages() = %+v, want seqs 5 then 6", page.Messages)
	}
	script.assertQueryContains(t, 1, "gm.group_id = ?", "AND gm.seq < ?", "ORDER BY gm.seq DESC")
	script.assertArg(t, 1, 1, int64(6))
	script.assertDone(t)
}

func TestListGroupMessagesAfterUsesSeqCursor(t *testing.T) {
	now := seqTestTime()
	store, script := newScriptStore(t,
		scriptStep{
			match:   "SELECT EXISTS(",
			columns: []string{"found"},
			rows:    [][]driver.Value{{int64(1)}},
		},
		scriptStep{
			match:   "FROM group_messages gm",
			columns: groupMessageColumns(),
			rows: [][]driver.Value{
				{int64(41), int64(7), int64(7), int64(1), "alice", "Alice", "c7", "seven", now},
			},
		},
	)

	page, err := store.ListGroupMessagesAfter(context.Background(), 1, 7, 6, 10)
	if err != nil {
		t.Fatalf("ListGroupMessagesAfter() error = %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].Seq != 7 {
		t.Fatalf("ListGroupMessagesAfter() = %+v, want one message with seq 7", page.Messages)
	}
	script.assertQueryContains(t, 1, "gm.seq > ?", "ORDER BY gm.seq ASC")
	script.assertArg(t, 1, 1, int64(6))
	script.assertDone(t)
}

func TestCreateMessageAllocatesSeq(t *testing.T) {
	now := seqTestTime()
	store, script := newScriptStore(t,
		scriptStep{match: "SELECT id FROM users WHERE id = ? FOR UPDATE", columns: []string{"id"}, rows: [][]driver.Value{{int64(1)}}},
		scriptStep{match: "SELECT id FROM users WHERE id = ? FOR UPDATE", columns: []string{"id"}, rows: [][]driver.Value{{int64(2)}}},
		scriptStep{match: "client_message_id = ?", columns: messageColumns()},
		scriptStep{match: "FROM friendships", columns: []string{"found"}, rows: [][]driver.Value{{int64(1)}}},
		scriptStep{match: "FROM message_id_sequences", columns: []string{"last_message_id"}, rows: [][]driver.Value{{int64(40)}}},
		scriptStep{match: "COALESCE(MAX(server_message_id), 0) FROM messages", columns: []string{"max"}, rows: [][]driver.Value{{int64(50)}}},
		scriptStep{match: "COALESCE(MAX(server_message_id), 0) FROM group_messages", columns: []string{"max"}, rows: [][]driver.Value{{int64(60)}}},
		scriptStep{match: "UPDATE message_id_sequences", affected: 1},
		scriptStep{match: "MAX(seq), 0) FROM messages WHERE conversation_low_id", columns: []string{"max"}, rows: [][]driver.Value{{int64(4)}}},
		scriptStep{match: "INSERT INTO messages", affected: 1},
	)

	message, created, err := store.CreateMessage(context.Background(), 2, 1, "client", "body", now)
	if err != nil {
		t.Fatalf("CreateMessage() error = %v", err)
	}
	if !created {
		t.Fatal("CreateMessage() created = false, want true")
	}
	if message.ID != 61 || message.Seq != 5 {
		t.Fatalf("CreateMessage() = %+v, want id 61 seq 5", message)
	}
	script.assertQueryContains(t, 9, "conversation_low_id", "conversation_high_id", "seq")
	script.assertArg(t, 9, 0, int64(61))
	script.assertArg(t, 9, 1, int64(1))
	script.assertArg(t, 9, 2, int64(2))
	script.assertArg(t, 9, 3, int64(5))
	script.assertDone(t)
}

func TestPersistMessageSeqConflict(t *testing.T) {
	now := seqTestTime()
	store, script := newScriptStore(t,
		scriptStep{match: "INSERT INTO messages", affected: 0},
		scriptStep{match: "WHERE server_message_id = ?", columns: messageColumns()},
		scriptStep{match: "client_message_id = ?", columns: messageColumns()},
		scriptStep{
			match:   "conversation_high_id = ? AND seq = ?",
			columns: messageColumns(),
			rows: [][]driver.Value{
				{int64(99), int64(5), int64(1), int64(2), "other", "other body", now},
			},
		},
	)

	_, err := store.PersistMessage(context.Background(), Message{
		ID: 100, Seq: 5, SenderID: 1, RecipientID: 2,
		ClientMessageID: "mine", Body: "mine", CreatedAt: now,
	})
	if !errors.Is(err, ErrSeqConflict) {
		t.Fatalf("PersistMessage() error = %v, want ErrSeqConflict", err)
	}
	script.assertQueryContains(t, 3, "conversation_low_id = ?", "conversation_high_id = ?", "seq = ?")
	script.assertArg(t, 3, 0, int64(1))
	script.assertArg(t, 3, 1, int64(2))
	script.assertArg(t, 3, 2, int64(5))
	script.assertDone(t)
}

func TestPersistMessageIsIdempotent(t *testing.T) {
	now := seqTestTime()
	message := Message{
		ID: 100, Seq: 5, SenderID: 1, RecipientID: 2,
		ClientMessageID: "mine", Body: "mine", CreatedAt: now,
	}
	store, script := newScriptStore(t,
		scriptStep{match: "INSERT INTO messages", affected: 0},
		scriptStep{
			match:   "WHERE server_message_id = ?",
			columns: messageColumns(),
			rows:    [][]driver.Value{{int64(100), int64(5), int64(1), int64(2), "mine", "mine", now}},
		},
	)

	created, err := store.PersistMessage(context.Background(), message)
	if err != nil {
		t.Fatalf("PersistMessage() error = %v", err)
	}
	if created {
		t.Fatal("PersistMessage() created = true for a duplicate, want false")
	}
	script.assertQueryContains(t, 0, "conversation_low_id", "conversation_high_id", "seq")
	script.assertDone(t)
}

func TestPersistGroupMessageSeqConflict(t *testing.T) {
	now := seqTestTime()
	store, script := newScriptStore(t,
		scriptStep{match: "INSERT INTO group_messages", affected: 0},
		scriptStep{match: "gm.server_message_id = ?", columns: groupMessageColumns()},
		scriptStep{match: "gm.client_message_id = ?", columns: groupMessageColumns()},
		scriptStep{
			match:   "gm.seq = ?",
			columns: groupMessageColumns(),
			rows: [][]driver.Value{
				{int64(88), int64(4), int64(7), int64(2), "bob", "Bob", "other", "other", now},
			},
		},
	)

	_, err := store.PersistGroupMessage(context.Background(), GroupMessage{
		ID: 89, Seq: 4, GroupID: 7, SenderID: 1,
		ClientMessageID: "mine", Body: "mine", CreatedAt: now,
	})
	if !errors.Is(err, ErrSeqConflict) {
		t.Fatalf("PersistGroupMessage() error = %v, want ErrSeqConflict", err)
	}
	script.assertQueryContains(t, 3, "gm.group_id = ?", "gm.seq = ?")
	script.assertDone(t)
}

func TestUpsertConversationReadWritesSeq(t *testing.T) {
	updatedAt := seqTestTime()
	store, script := newScriptStore(t,
		scriptStep{match: "INSERT INTO conversation_reads", affected: 1},
	)

	if err := store.UpsertConversationRead(context.Background(), 1, 2, 7, updatedAt); err != nil {
		t.Fatalf("UpsertConversationRead() error = %v", err)
	}
	script.assertQueryContains(t, 0, "last_read_message_id, last_read_seq", "GREATEST(", "conversation_reads.last_read_seq")
	script.assertArg(t, 0, 0, int64(1))
	script.assertArg(t, 0, 1, int64(2))
	script.assertArg(t, 0, 2, int64(7))
	script.assertDone(t)
}

func TestUpsertGroupReadWritesSeq(t *testing.T) {
	store, script := newScriptStore(t,
		scriptStep{match: "UPDATE group_members", affected: 1},
	)

	found, err := store.UpsertGroupRead(context.Background(), 7, 3, 5)
	if err != nil {
		t.Fatalf("UpsertGroupRead() error = %v", err)
	}
	if !found {
		t.Fatal("UpsertGroupRead() found = false, want true")
	}
	script.assertQueryContains(t, 0, "SET last_read_seq = GREATEST(last_read_seq, ?)")
	script.assertArg(t, 0, 0, int64(5))
	script.assertArg(t, 0, 1, int64(7))
	script.assertArg(t, 0, 2, int64(3))
	script.assertDone(t)
}

func TestAssignMessageSeqsContinuesPerConversation(t *testing.T) {
	store, script := newScriptStore(t,
		scriptStep{
			match:   "WHERE seq IS NULL",
			columns: []string{"id", "conversation_low_id", "conversation_high_id"},
			rows: [][]driver.Value{
				{int64(1), int64(1), int64(2)},
				{int64(2), int64(1), int64(2)},
				{int64(3), int64(3), int64(4)},
			},
		},
		scriptStep{match: "MAX(seq), 0) FROM messages WHERE conversation_low_id", columns: []string{"max"}, rows: [][]driver.Value{{int64(4)}}},
		scriptStep{match: "UPDATE messages SET seq = ?", affected: 1},
		scriptStep{match: "UPDATE messages SET seq = ?", affected: 1},
		scriptStep{match: "MAX(seq), 0) FROM messages WHERE conversation_low_id", columns: []string{"max"}, rows: [][]driver.Value{{int64(0)}}},
		scriptStep{match: "UPDATE messages SET seq = ?", affected: 1},
	)

	processed, err := store.AssignMessageSeqs(context.Background(), 100)
	if err != nil {
		t.Fatalf("AssignMessageSeqs() error = %v", err)
	}
	if processed != 3 {
		t.Fatalf("AssignMessageSeqs() processed = %d, want 3", processed)
	}
	script.assertArg(t, 2, 0, int64(5))
	script.assertArg(t, 2, 1, int64(1))
	script.assertArg(t, 3, 0, int64(6))
	script.assertArg(t, 3, 1, int64(2))
	script.assertArg(t, 5, 0, int64(1))
	script.assertArg(t, 5, 1, int64(3))
	script.assertDone(t)
}

func TestAssignMessageSeqsStopsWhenNothingPending(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "WHERE seq IS NULL",
		columns: []string{"id", "conversation_low_id", "conversation_high_id"},
	})

	processed, err := store.AssignMessageSeqs(context.Background(), 100)
	if err != nil {
		t.Fatalf("AssignMessageSeqs() error = %v", err)
	}
	if processed != 0 {
		t.Fatalf("AssignMessageSeqs() processed = %d, want 0", processed)
	}
	script.assertDone(t)
}

func TestAssignGroupMessageSeqsContinuesPerGroup(t *testing.T) {
	store, script := newScriptStore(t,
		scriptStep{
			match:   "WHERE seq IS NULL",
			columns: []string{"id", "group_id"},
			rows: [][]driver.Value{
				{int64(1), int64(7)},
				{int64(2), int64(7)},
			},
		},
		scriptStep{match: "MAX(seq), 0) FROM group_messages", columns: []string{"max"}, rows: [][]driver.Value{{int64(3)}}},
		scriptStep{match: "UPDATE group_messages SET seq = ?", affected: 1},
		scriptStep{match: "UPDATE group_messages SET seq = ?", affected: 1},
	)

	processed, err := store.AssignGroupMessageSeqs(context.Background(), 100)
	if err != nil {
		t.Fatalf("AssignGroupMessageSeqs() error = %v", err)
	}
	if processed != 2 {
		t.Fatalf("AssignGroupMessageSeqs() processed = %d, want 2", processed)
	}
	script.assertArg(t, 2, 0, int64(4))
	script.assertArg(t, 3, 0, int64(5))
	script.assertDone(t)
}

func TestFillMessageConversationPairs(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{match: "UPDATE messages", affected: 2})

	processed, err := store.FillMessageConversationPairs(context.Background(), 25)
	if err != nil {
		t.Fatalf("FillMessageConversationPairs() error = %v", err)
	}
	if processed != 2 {
		t.Fatalf("FillMessageConversationPairs() processed = %d, want 2", processed)
	}
	script.assertQueryContains(t, 0, "conversation_low_id = LEAST(sender_id, recipient_id)",
		"conversation_high_id = GREATEST(sender_id, recipient_id)", "LIMIT ?")
	script.assertArg(t, 0, 0, int64(25))
	script.assertDone(t)
}

func TestCountUnnumberedMessages(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "COUNT(*) FROM messages WHERE seq IS NULL",
		columns: []string{"direct", "group"},
		rows:    [][]driver.Value{{int64(2), int64(7)}},
	})

	direct, group, err := store.CountUnnumberedMessages(context.Background())
	if err != nil {
		t.Fatalf("CountUnnumberedMessages() error = %v", err)
	}
	if direct != 2 || group != 7 {
		t.Fatalf("CountUnnumberedMessages() = (%d, %d), want (2, 7)", direct, group)
	}
	script.assertDone(t)
}

func TestListConversationSeqMax(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "GROUP BY conversation_low_id, conversation_high_id",
		columns: []string{"low", "high", "max_seq", "count"},
		rows:    [][]driver.Value{{int64(1), int64(2), int64(9), int64(9)}},
	})

	items, err := store.ListConversationSeqMax(context.Background(), 1, 2, 50)
	if err != nil {
		t.Fatalf("ListConversationSeqMax() error = %v", err)
	}
	if len(items) != 1 || items[0].LowID != 1 || items[0].HighID != 2 || items[0].MaxSeq != 9 || items[0].Count != 9 {
		t.Fatalf("ListConversationSeqMax() = %+v", items)
	}
	script.assertArg(t, 0, 2, int64(50))
	script.assertDone(t)
}

func TestListGroupSeqMax(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "GROUP BY group_id",
		columns: []string{"group_id", "max_seq", "count"},
		rows:    [][]driver.Value{{int64(7), int64(4), int64(3)}},
	})

	items, err := store.ListGroupSeqMax(context.Background(), 6, 50)
	if err != nil {
		t.Fatalf("ListGroupSeqMax() error = %v", err)
	}
	if len(items) != 1 || items[0].GroupID != 7 || items[0].MaxSeq != 4 || items[0].Count != 3 {
		t.Fatalf("ListGroupSeqMax() = %+v", items)
	}
	script.assertArg(t, 0, 0, int64(6))
	script.assertDone(t)
}

func TestListMessageSeqIndex(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "COALESCE(conversation_low_id, 0)",
		columns: []string{"server_id", "seq", "low", "high", "sender", "recipient"},
		rows:    [][]driver.Value{{int64(10), int64(3), int64(1), int64(2), int64(2), int64(1)}},
	})

	items, err := store.ListMessageSeqIndex(context.Background(), 9, 50)
	if err != nil {
		t.Fatalf("ListMessageSeqIndex() error = %v", err)
	}
	if len(items) != 1 || items[0].ServerMessageID != 10 || items[0].Seq != 3 || items[0].LowID != 1 || items[0].HighID != 2 {
		t.Fatalf("ListMessageSeqIndex() = %+v", items)
	}
	script.assertDone(t)
}

func TestListGroupMessageSeqIndex(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "FROM group_messages WHERE server_message_id > ?",
		columns: []string{"server_id", "seq", "group_id", "sender"},
		rows:    [][]driver.Value{{int64(11), int64(4), int64(7), int64(2)}},
	})

	items, err := store.ListGroupMessageSeqIndex(context.Background(), 10, 50)
	if err != nil {
		t.Fatalf("ListGroupMessageSeqIndex() error = %v", err)
	}
	if len(items) != 1 || items[0].Seq != 4 || items[0].GroupID != 7 {
		t.Fatalf("ListGroupMessageSeqIndex() = %+v", items)
	}
	script.assertDone(t)
}

func TestListConversationReadStates(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "last_read_message_id, last_read_seq",
		columns: []string{"user", "peer", "old", "seq"},
		rows:    [][]driver.Value{{int64(1), int64(2), int64(10), int64(3)}},
	})

	items, err := store.ListConversationReadStates(context.Background(), 0, 0, 50)
	if err != nil {
		t.Fatalf("ListConversationReadStates() error = %v", err)
	}
	if len(items) != 1 || items[0].LastReadMessageID != 10 || items[0].LastReadSeq != 3 {
		t.Fatalf("ListConversationReadStates() = %+v", items)
	}
	script.assertDone(t)
}

func TestListGroupMemberReadStates(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "FROM group_members",
		columns: []string{"group", "user", "old", "seq"},
		rows:    [][]driver.Value{{int64(7), int64(1), int64(9), int64(4)}},
	})

	items, err := store.ListGroupMemberReadStates(context.Background(), 0, 0, 50)
	if err != nil {
		t.Fatalf("ListGroupMemberReadStates() error = %v", err)
	}
	if len(items) != 1 || items[0].LastReadMessageID != 9 || items[0].LastReadSeq != 4 {
		t.Fatalf("ListGroupMemberReadStates() = %+v", items)
	}
	script.assertDone(t)
}

func TestDirectSeqAtOrBefore(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "ORDER BY server_message_id DESC",
		columns: []string{"seq"},
		rows:    [][]driver.Value{{int64(12)}},
	})

	seq, err := store.DirectSeqAtOrBefore(context.Background(), 1, 2, 50)
	if err != nil {
		t.Fatalf("DirectSeqAtOrBefore() error = %v", err)
	}
	if seq != 12 {
		t.Fatalf("DirectSeqAtOrBefore() = %d, want 12", seq)
	}
	script.assertArg(t, 0, 0, int64(1))
	script.assertArg(t, 0, 1, int64(2))
	script.assertArg(t, 0, 2, int64(50))
	script.assertDone(t)
}

func TestGroupSeqAtOrBefore(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "ORDER BY server_message_id DESC",
		columns: []string{"seq"},
		rows:    [][]driver.Value{{int64(8)}},
	})

	seq, err := store.GroupSeqAtOrBefore(context.Background(), 7, 50)
	if err != nil {
		t.Fatalf("GroupSeqAtOrBefore() error = %v", err)
	}
	if seq != 8 {
		t.Fatalf("GroupSeqAtOrBefore() = %d, want 8", seq)
	}
	script.assertArg(t, 0, 0, int64(7))
	script.assertArg(t, 0, 1, int64(50))
	script.assertDone(t)
}

func TestGroupMessageSenderUsesSeq(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "WHERE group_id = ? AND seq = ?",
		columns: []string{"sender_id"},
		rows:    [][]driver.Value{{int64(42)}},
	})

	senderID, found, err := store.GroupMessageSender(context.Background(), 7, 3)
	if err != nil {
		t.Fatalf("GroupMessageSender() error = %v", err)
	}
	if !found || senderID != 42 {
		t.Fatalf("GroupMessageSender() = (%d, %v), want (42, true)", senderID, found)
	}
	script.assertQueryContains(t, 0, "group_id = ? AND seq = ?")
	script.assertArg(t, 0, 0, int64(7))
	script.assertArg(t, 0, 1, int64(3))
	script.assertDone(t)
}

func TestListGroupMembershipProjectionsUsesSeq(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "member.last_read_seq",
		columns: []string{"group_id", "dissolved_at", "user_id", "last_read_seq"},
		rows: [][]driver.Value{
			{int64(7), nil, int64(1), int64(5)},
			{int64(7), nil, int64(2), int64(0)},
			{int64(8), nil, nil, nil},
		},
	})

	projections, err := store.ListGroupMembershipProjections(context.Background())
	if err != nil {
		t.Fatalf("ListGroupMembershipProjections() error = %v", err)
	}
	if len(projections) != 2 {
		t.Fatalf("len(projections) = %d, want 2", len(projections))
	}
	if projections[0].Positions[1] != 5 || projections[0].Positions[2] != 0 {
		t.Fatalf("positions = %+v, want 1->5 and 2->0", projections[0].Positions)
	}
	if len(projections[1].MemberIDs) != 0 {
		t.Fatalf("empty group members = %+v, want none", projections[1].MemberIDs)
	}
	script.assertDone(t)
}

func TestListConversationsCarriesSeq(t *testing.T) {
	now := seqTestTime()
	store, script := newScriptStore(t,
		scriptStep{
			match:   "FROM friendships",
			columns: []string{"id", "username", "nickname"},
			rows:    [][]driver.Value{{int64(2), "bob", "Bob"}},
		},
		scriptStep{
			match:   "ORDER BY m.server_message_id DESC",
			columns: []string{"server_id", "seq", "sender", "recipient", "client", "body", "created_at", "username", "nickname", "other_id", "last_seq"},
			rows:    [][]driver.Value{{int64(10), int64(3), int64(2), int64(1), "c", "hi", now, "bob", "Bob", int64(2), int64(3)}},
		},
		scriptStep{
			match:   "COUNT(*)",
			columns: []string{"username", "unread"},
			rows:    [][]driver.Value{{"bob", int64(2)}},
		},
		scriptStep{
			match:   "r.last_read_seq",
			columns: []string{"username", "last_read_seq"},
			rows:    [][]driver.Value{{"bob", int64(1)}},
		},
	)

	conversations, err := store.ListConversations(context.Background(), 1)
	if err != nil {
		t.Fatalf("ListConversations() error = %v", err)
	}
	if len(conversations) != 1 {
		t.Fatalf("len(conversations) = %d, want 1", len(conversations))
	}
	conversation := conversations[0]
	if conversation.LastSeq != 3 || conversation.UnreadCount != 2 || conversation.PeerLastReadSeq != 1 {
		t.Fatalf("conversation = %+v, want last_seq 3, unread 2, peer_read 1", conversation)
	}
	if conversation.LastMessage == nil || conversation.LastMessage.Seq != 3 {
		t.Fatalf("last message = %+v, want seq 3", conversation.LastMessage)
	}
	script.assertQueryContains(t, 1, "COALESCE(MAX(stats.seq), 0)")
	script.assertQueryContains(t, 2, "COALESCE(m.seq, 0) > COALESCE(r.last_read_seq, 0)")
	script.assertDone(t)
}

func TestListGroupConversationsCarriesSeq(t *testing.T) {
	now := seqTestTime()
	store, script := newScriptStore(t,
		scriptStep{
			match:   "FROM chat_groups g",
			columns: []string{"id", "name", "owner_id", "owner_username", "owner_nickname", "created_at", "member_count"},
			rows:    [][]driver.Value{{int64(7), "Team", int64(1), "alice", "Alice", now, int64(2)}},
		},
		scriptStep{
			match:   "FROM group_messages gm",
			columns: []string{"server_id", "seq", "group_id", "sender", "username", "nickname", "client", "body", "created_at", "last_seq"},
			rows:    [][]driver.Value{{int64(11), int64(4), int64(7), int64(2), "bob", "Bob", "c", "hi", now, int64(4)}},
		},
		scriptStep{
			match:   "member.last_read_seq",
			columns: []string{"group_id", "unread"},
			rows:    [][]driver.Value{{int64(7), int64(3)}},
		},
	)

	conversations, err := store.ListGroupConversations(context.Background(), 1)
	if err != nil {
		t.Fatalf("ListGroupConversations() error = %v", err)
	}
	if len(conversations) != 1 {
		t.Fatalf("len(conversations) = %d, want 1", len(conversations))
	}
	conversation := conversations[0]
	if conversation.LastSeq != 4 || conversation.UnreadCount != 3 {
		t.Fatalf("conversation = %+v, want last_seq 4, unread 3", conversation)
	}
	if conversation.LastMessage == nil || conversation.LastMessage.Seq != 4 {
		t.Fatalf("last message = %+v, want seq 4", conversation.LastMessage)
	}
	script.assertQueryContains(t, 2, "COALESCE(gm.seq, 0) > member.last_read_seq")
	script.assertDone(t)
}

func TestCreateGroupMembersStartAtSeqZero(t *testing.T) {
	now := seqTestTime()
	store, script := newScriptStore(t,
		scriptStep{match: "FROM friendships", columns: []string{"found"}, rows: [][]driver.Value{{int64(1)}}},
		scriptStep{match: "INSERT INTO chat_groups", affected: 1, lastID: 7},
		scriptStep{match: "INSERT INTO group_members", affected: 1},
		scriptStep{match: "INSERT INTO group_members", affected: 1},
		scriptStep{
			match:   `SELECT g.id, g.name, g.owner_id`,
			columns: []string{"id", "name", "owner_id", "owner_username", "owner_nickname", "created_at"},
			rows:    [][]driver.Value{{int64(7), "Team", int64(1), "alice", "Alice", now}},
		},
		scriptStep{
			match:   "FROM group_members member",
			columns: []string{"id", "username", "nickname", "joined_at"},
			rows: [][]driver.Value{
				{int64(1), "alice", "Alice", now},
				{int64(2), "bob", "Bob", now},
			},
		},
	)

	detail, err := store.CreateGroup(context.Background(), 1, "Team", []uint64{2}, now)
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if detail.Group.ID != 7 {
		t.Fatalf("CreateGroup() group id = %d, want 7", detail.Group.ID)
	}
	script.assertQueryContains(t, 2, "last_read_seq", "VALUES (?, ?, ?, 0)")
	script.assertQueryContains(t, 3, "last_read_seq", "VALUES (?, ?, ?, 0)")
	script.assertDone(t)
}

func TestInviteGroupMembersBaselineUsesSeq(t *testing.T) {
	now := seqTestTime()
	store, script := newScriptStore(t,
		scriptStep{
			match:   "SELECT owner_id, dissolved_at FROM chat_groups",
			columns: []string{"owner_id", "dissolved_at"},
			rows:    [][]driver.Value{{int64(1), nil}},
		},
		scriptStep{match: "FROM group_members WHERE group_id = ? AND user_id = ?", columns: []string{"found"}, rows: [][]driver.Value{{int64(1)}}},
		scriptStep{match: "ORDER BY user_id FOR UPDATE", columns: []string{"user_id"}, rows: [][]driver.Value{{int64(1)}}},
		scriptStep{match: "FROM friendships", columns: []string{"found"}, rows: [][]driver.Value{{int64(1)}}},
		scriptStep{match: "MAX(seq), 0) FROM group_messages", columns: []string{"max"}, rows: [][]driver.Value{{int64(4)}}},
		scriptStep{match: "INSERT INTO group_members", affected: 1},
	)

	baseline, err := store.InviteGroupMembersWithBaseline(
		context.Background(), 7, 1, []uint64{2}, now, 6,
	)
	if err != nil {
		t.Fatalf("InviteGroupMembersWithBaseline() error = %v", err)
	}
	if baseline != 6 {
		t.Fatalf("baseline = %d, want 6", baseline)
	}
	script.assertQueryContains(t, 4, "MAX(seq), 0")
	script.assertQueryContains(t, 5, "last_read_seq", "VALUES (?, ?, ?, ?)")
	script.assertArg(t, 5, 3, int64(6))
	script.assertDone(t)
}
