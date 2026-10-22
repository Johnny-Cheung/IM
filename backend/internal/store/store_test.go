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
	if len(names) != 4 {
		t.Fatalf("migration count = %d (%v), want 4", len(names), names)
	}
	want := []string{
		"001_create_users.sql",
		"002_create_sessions.sql",
		"003_create_friend_requests.sql",
		"004_create_friendships.sql",
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
