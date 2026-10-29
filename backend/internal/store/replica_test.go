package store

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"
)

func TestListMessagesForRebuild(t *testing.T) {
	created := time.Date(2026, 10, 29, 8, 30, 15, 123000000, time.UTC)
	store, script := newScriptStore(t, scriptStep{
		match: "ORDER BY server_message_id ASC",
		columns: []string{
			"server_message_id", "seq", "sender_id", "recipient_id",
			"client_message_id", "body", "created_at",
		},
		rows: [][]driver.Value{
			{int64(4), int64(2), int64(10), int64(11), "c1", "hello", created},
			{int64(5), nil, int64(11), int64(10), "c2", "你好，世界", created.Add(time.Second)},
		},
	})

	messages, err := store.ListMessagesForRebuild(context.Background(), 3, 2)
	if err != nil {
		t.Fatalf("ListMessagesForRebuild() error = %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("len(messages) = %d, want 2", len(messages))
	}
	first := messages[0]
	if first.ID != 4 || first.Seq != 2 || first.SenderID != 10 || first.RecipientID != 11 ||
		first.ClientMessageID != "c1" || first.Body != "hello" {
		t.Fatalf("messages[0] = %+v, want ID 4 seq 2 sender 10 recipient 11 c1 hello", first)
	}
	if !first.CreatedAt.Equal(created) {
		t.Fatalf("messages[0].CreatedAt = %v, want %v", first.CreatedAt, created)
	}
	second := messages[1]
	if second.ID != 5 || second.Seq != 0 || second.Body != "你好，世界" {
		t.Fatalf("messages[1] = %+v, want ID 5 seq 0 unicode body", second)
	}

	// The cursor is the last returned public ID, not the internal primary key.
	script.assertQueryContains(t, 0,
		"SELECT server_message_id, seq, sender_id, recipient_id",
		"client_message_id, body, created_at",
		"FROM messages",
		"WHERE server_message_id > ?",
		"ORDER BY server_message_id ASC",
		"LIMIT ?",
	)
	script.assertArg(t, 0, 0, int64(3))
	script.assertArg(t, 0, 1, int64(2))
	script.assertDone(t)
}

func TestListGroupMessagesForRebuild(t *testing.T) {
	created := time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)
	store, script := newScriptStore(t, scriptStep{
		match: "FROM group_messages gm",
		columns: []string{
			"server_message_id", "seq", "group_id", "sender_id",
			"username", "nickname", "client_message_id", "body", "created_at",
		},
		rows: [][]driver.Value{
			{int64(7), int64(1), int64(20), int64(30), "alice", "Alice", "c1", "hi", created},
			{int64(8), nil, int64(20), int64(99), nil, nil, "c2", "ghost", created},
		},
	})

	messages, err := store.ListGroupMessagesForRebuild(context.Background(), 6, 2)
	if err != nil {
		t.Fatalf("ListGroupMessagesForRebuild() error = %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("len(messages) = %d, want 2", len(messages))
	}
	first := messages[0]
	if first.ID != 7 || first.Seq != 1 || first.GroupID != 20 || first.SenderID != 30 ||
		first.Sender.Username != "alice" || first.Sender.Nickname != "Alice" ||
		first.ClientMessageID != "c1" || first.Body != "hi" {
		t.Fatalf("messages[0] = %+v, want full sender snapshot", first)
	}
	second := messages[1]
	if second.ID != 8 || second.Seq != 0 || second.Sender.Username != "" || second.Sender.Nickname != "" {
		t.Fatalf("messages[1] = %+v, want empty sender and seq 0", second)
	}

	// A LEFT JOIN keeps messages whose sender row is gone.
	script.assertQueryContains(t, 0,
		"SELECT gm.server_message_id, gm.seq, gm.group_id, gm.sender_id",
		"sender.username, sender.nickname",
		"LEFT JOIN users sender ON sender.id = gm.sender_id",
		"WHERE gm.server_message_id > ?",
		"ORDER BY gm.server_message_id ASC",
		"LIMIT ?",
	)
	script.assertArg(t, 0, 0, int64(6))
	script.assertArg(t, 0, 1, int64(2))
	script.assertDone(t)
}

func TestListMessagesBySeqRange(t *testing.T) {
	created := time.Date(2026, 10, 29, 10, 0, 0, 500000000, time.UTC)
	store, script := newScriptStore(t, scriptStep{
		match: "conversation_low_id = ? AND conversation_high_id = ?",
		columns: []string{
			"server_message_id", "seq", "sender_id", "recipient_id",
			"client_message_id", "body", "created_at",
		},
		rows: [][]driver.Value{
			{int64(10), int64(3), int64(1), int64(2), "c3", "third", created},
			{int64(11), int64(4), int64(2), int64(1), "c4", "fourth", created},
		},
	})

	messages, err := store.ListMessagesBySeqRange(context.Background(), 1, 2, 2, 100)
	if err != nil {
		t.Fatalf("ListMessagesBySeqRange() error = %v", err)
	}
	if len(messages) != 2 || messages[0].Seq != 3 || messages[1].Seq != 4 {
		t.Fatalf("messages = %+v, want seqs 3 and 4", messages)
	}
	if messages[0].ID != 10 || messages[1].ID != 11 || messages[1].Body != "fourth" {
		t.Fatalf("messages = %+v, want server IDs 10 and 11 with bodies", messages)
	}
	script.assertQueryContains(t, 0,
		"WHERE conversation_low_id = ? AND conversation_high_id = ?",
		"seq > ?",
		"seq IS NOT NULL",
		"ORDER BY seq ASC",
		"LIMIT ?",
	)
	script.assertArg(t, 0, 0, int64(1))
	script.assertArg(t, 0, 1, int64(2))
	script.assertArg(t, 0, 2, int64(2))
	script.assertArg(t, 0, 3, int64(100))
	script.assertDone(t)
}

func TestListGroupMessagesBySeqRange(t *testing.T) {
	created := time.Date(2026, 10, 29, 11, 0, 0, 0, time.UTC)
	store, script := newScriptStore(t, scriptStep{
		match: "gm.group_id = ? AND gm.seq > ?",
		columns: []string{
			"server_message_id", "seq", "group_id", "sender_id",
			"username", "nickname", "client_message_id", "body", "created_at",
		},
		rows: [][]driver.Value{
			{int64(21), int64(5), int64(7), int64(2), "bob", "Bob", "c5", "fifth", created},
			{int64(22), int64(6), int64(7), int64(8), nil, nil, "c6", "sixth", created},
		},
	})

	messages, err := store.ListGroupMessagesBySeqRange(context.Background(), 7, 4, 50)
	if err != nil {
		t.Fatalf("ListGroupMessagesBySeqRange() error = %v", err)
	}
	if len(messages) != 2 || messages[0].Seq != 5 || messages[1].Seq != 6 {
		t.Fatalf("messages = %+v, want seqs 5 and 6", messages)
	}
	if messages[0].GroupID != 7 || messages[0].Sender.Username != "bob" || messages[0].Sender.Nickname != "Bob" {
		t.Fatalf("messages[0] = %+v, want group 7 with bob snapshot", messages[0])
	}
	if messages[1].Sender.Username != "" {
		t.Fatalf("messages[1].Sender = %+v, want empty snapshot", messages[1].Sender)
	}
	script.assertQueryContains(t, 0,
		"FROM group_messages gm",
		"LEFT JOIN users sender ON sender.id = gm.sender_id",
		"gm.group_id = ? AND gm.seq > ?",
		"gm.seq IS NOT NULL",
		"ORDER BY gm.seq ASC",
		"LIMIT ?",
	)
	script.assertArg(t, 0, 0, int64(7))
	script.assertArg(t, 0, 1, int64(4))
	script.assertArg(t, 0, 2, int64(50))
	script.assertDone(t)
}

func TestCountMessagesInvolvingUser(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "SELECT COUNT(*) FROM messages WHERE sender_id = ? OR recipient_id = ?",
		columns: []string{"count"},
		rows:    [][]driver.Value{{int64(42)}},
	})

	count, err := store.CountMessagesInvolvingUser(context.Background(), 9)
	if err != nil {
		t.Fatalf("CountMessagesInvolvingUser() error = %v", err)
	}
	if count != 42 {
		t.Fatalf("count = %d, want 42", count)
	}
	script.assertArg(t, 0, 0, int64(9))
	script.assertArg(t, 0, 1, int64(9))
	script.assertDone(t)
}

func TestListUserIDs(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "SELECT id FROM users",
		columns: []string{"id"},
		rows:    [][]driver.Value{{int64(5)}, {int64(9)}, {int64(12)}},
	})

	ids, err := store.ListUserIDs(context.Background(), 4, 3)
	if err != nil {
		t.Fatalf("ListUserIDs() error = %v", err)
	}
	if len(ids) != 3 || ids[0] != 5 || ids[1] != 9 || ids[2] != 12 {
		t.Fatalf("ids = %v, want [5 9 12]", ids)
	}
	script.assertQueryContains(t, 0,
		"SELECT id FROM users",
		"WHERE id > ?",
		"ORDER BY id ASC",
		"LIMIT ?",
	)
	script.assertArg(t, 0, 0, int64(4))
	script.assertArg(t, 0, 1, int64(3))
	script.assertDone(t)
}

func TestReplicaQueriesNonPositiveLimitSkipQuery(t *testing.T) {
	store, script := newScriptStore(t)

	for _, limit := range []int{0, -1} {
		messages, err := store.ListMessagesForRebuild(context.Background(), 1, limit)
		if err != nil || len(messages) != 0 {
			t.Fatalf("ListMessagesForRebuild(limit %d) = (%v, %v), want empty", limit, messages, err)
		}
		groupMessages, err := store.ListGroupMessagesForRebuild(context.Background(), 1, limit)
		if err != nil || len(groupMessages) != 0 {
			t.Fatalf("ListGroupMessagesForRebuild(limit %d) = (%v, %v), want empty", limit, groupMessages, err)
		}
		bySeq, err := store.ListMessagesBySeqRange(context.Background(), 1, 2, 3, limit)
		if err != nil || len(bySeq) != 0 {
			t.Fatalf("ListMessagesBySeqRange(limit %d) = (%v, %v), want empty", limit, bySeq, err)
		}
		groupSeq, err := store.ListGroupMessagesBySeqRange(context.Background(), 1, 2, limit)
		if err != nil || len(groupSeq) != 0 {
			t.Fatalf("ListGroupMessagesBySeqRange(limit %d) = (%v, %v), want empty", limit, groupSeq, err)
		}
		ids, err := store.ListUserIDs(context.Background(), 1, limit)
		if err != nil || len(ids) != 0 {
			t.Fatalf("ListUserIDs(limit %d) = (%v, %v), want empty", limit, ids, err)
		}
	}
	script.assertDone(t)
}
