package store

import (
	"context"
	"database/sql/driver"
	"testing"
)

func TestListConversationSeqStats(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "GROUP BY conversation_low_id, conversation_high_id",
		columns: []string{"low", "high", "count", "max_seq"},
		rows:    [][]driver.Value{{int64(1), int64(2), int64(3), int64(5)}},
	})

	stats, err := store.ListConversationSeqStats(context.Background(), 1, 2, 50)
	if err != nil {
		t.Fatalf("ListConversationSeqStats() error = %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("len(stats) = %d, want 1", len(stats))
	}
	if stats[0].LowID != 1 || stats[0].HighID != 2 || stats[0].Count != 3 || stats[0].MaxSeq != 5 {
		t.Fatalf("stats[0] = %+v, want 1/2 count 3 max 5", stats[0])
	}
	script.assertQueryContains(t, 0,
		"seq IS NOT NULL",
		"(conversation_low_id, conversation_high_id) > (?, ?)",
		"ORDER BY conversation_low_id, conversation_high_id",
		"LIMIT ?",
	)
	script.assertArg(t, 0, 0, int64(1))
	script.assertArg(t, 0, 1, int64(2))
	script.assertArg(t, 0, 2, int64(50))
	script.assertDone(t)
}

func TestListGroupSeqStats(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "FROM group_messages",
		columns: []string{"group_id", "count", "max_seq"},
		rows:    [][]driver.Value{{int64(7), int64(2), int64(4)}},
	})

	stats, err := store.ListGroupSeqStats(context.Background(), 6, 50)
	if err != nil {
		t.Fatalf("ListGroupSeqStats() error = %v", err)
	}
	if len(stats) != 1 || stats[0].GroupID != 7 || stats[0].Count != 2 || stats[0].MaxSeq != 4 {
		t.Fatalf("stats = %+v, want group 7 count 2 max 4", stats)
	}
	script.assertQueryContains(t, 0, "seq IS NOT NULL", "group_id > ?", "ORDER BY group_id", "LIMIT ?")
	script.assertArg(t, 0, 0, int64(6))
	script.assertArg(t, 0, 1, int64(50))
	script.assertDone(t)
}

func TestListConversationSeqs(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "FROM messages",
		columns: []string{"seq"},
		rows:    [][]driver.Value{{int64(2)}, {int64(5)}},
	})

	seqs, err := store.ListConversationSeqs(context.Background(), 1, 2, 5, 1, 100)
	if err != nil {
		t.Fatalf("ListConversationSeqs() error = %v", err)
	}
	if len(seqs) != 2 || seqs[0] != 2 || seqs[1] != 5 {
		t.Fatalf("seqs = %v, want [2 5]", seqs)
	}
	script.assertQueryContains(t, 0,
		"conversation_low_id = ?", "conversation_high_id = ?",
		"seq <= ?", "seq > ?", "ORDER BY seq", "LIMIT ?",
	)
	script.assertArg(t, 0, 0, int64(1))
	script.assertArg(t, 0, 1, int64(2))
	script.assertArg(t, 0, 2, int64(5))
	script.assertArg(t, 0, 3, int64(1))
	script.assertArg(t, 0, 4, int64(100))
	script.assertDone(t)
}

func TestListGroupSeqs(t *testing.T) {
	store, script := newScriptStore(t, scriptStep{
		match:   "FROM group_messages",
		columns: []string{"seq"},
		rows:    [][]driver.Value{{int64(1)}, {int64(4)}},
	})

	seqs, err := store.ListGroupSeqs(context.Background(), 7, 4, 2, 100)
	if err != nil {
		t.Fatalf("ListGroupSeqs() error = %v", err)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 4 {
		t.Fatalf("seqs = %v, want [1 4]", seqs)
	}
	script.assertQueryContains(t, 0, "group_id = ?", "seq <= ?", "seq > ?", "ORDER BY seq", "LIMIT ?")
	script.assertArg(t, 0, 0, int64(7))
	script.assertArg(t, 0, 1, int64(4))
	script.assertArg(t, 0, 2, int64(2))
	script.assertArg(t, 0, 3, int64(100))
	script.assertDone(t)
}

func TestListSeqsZeroLimitSkipsQuery(t *testing.T) {
	store, script := newScriptStore(t)

	direct, err := store.ListConversationSeqs(context.Background(), 1, 2, 5, 0, 0)
	if err != nil || len(direct) != 0 {
		t.Fatalf("ListConversationSeqs() = (%v, %v), want empty", direct, err)
	}
	group, err := store.ListGroupSeqs(context.Background(), 7, 5, 0, 0)
	if err != nil || len(group) != 0 {
		t.Fatalf("ListGroupSeqs() = (%v, %v), want empty", group, err)
	}
	script.assertDone(t)
}

func TestListConversationSeqStatsZeroLimit(t *testing.T) {
	store, script := newScriptStore(t)

	stats, err := store.ListConversationSeqStats(context.Background(), 0, 0, 0)
	if err != nil {
		t.Fatalf("ListConversationSeqStats() error = %v", err)
	}
	if len(stats) != 0 {
		t.Fatalf("stats = %v, want none", stats)
	}
	script.assertDone(t)
}
