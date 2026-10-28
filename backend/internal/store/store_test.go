package store

import (
	"context"
	"testing"
	"time"
)

func TestMigrationsAreEmbedded(t *testing.T) {
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}

	var names []string
	for _, entry := range entries {
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	if len(names) != 28 {
		t.Fatalf("migration count = %d (%v), want 28", len(names), names)
	}
	want := []string{
		"001_create_users.sql",
		"002_create_sessions.sql",
		"003_create_friend_requests.sql",
		"004_create_friendships.sql",
		"005_add_friend_request_history.sql",
		"006_add_pending_friend_request_unique.sql",
		"007_backfill_pending_friend_requests.sql",
		"008_drop_friend_request_direction_unique.sql",
		"009_create_messages.sql",
		"010_create_conversation_reads.sql",
		"011_create_chat_groups.sql",
		"012_create_group_members.sql",
		"013_create_group_messages.sql",
		"014_add_message_server_id.sql",
		"015_backfill_message_server_id.sql",
		"016_finalize_message_server_id.sql",
		"017_create_message_id_sequences.sql",
		"018_seed_message_id_sequences.sql",
		"019_add_group_message_server_id.sql",
		"020_backfill_group_message_server_id.sql",
		"021_finalize_group_message_server_id.sql",
		"022_advance_shared_message_sequence.sql",
		"023_add_message_conversation_seq.sql",
		"024_add_message_conversation_indexes.sql",
		"025_add_group_message_seq.sql",
		"026_add_group_message_seq_unique.sql",
		"027_add_conversation_read_last_read_seq.sql",
		"028_add_group_member_last_read_seq.sql",
	}
	for i, name := range want {
		if names[i] != name {
			t.Fatalf("migration names = %v, want %v", names, want)
		}
	}
}

func TestOpenRejectsInvalidDSN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := Open(ctx, "not-a-valid-dsn"); err == nil {
		t.Fatal("Open() error = nil, want connection error")
	}
}
