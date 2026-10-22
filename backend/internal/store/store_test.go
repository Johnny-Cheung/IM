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
	if len(names) != 2 {
		t.Fatalf("migration count = %d (%v), want 2", len(names), names)
	}
	if names[0] != "001_create_users.sql" || names[1] != "002_create_sessions.sql" {
		t.Fatalf("migration names = %v, want users then sessions", names)
	}
}

func TestOpenRejectsInvalidDSN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := Open(ctx, "not-a-valid-dsn"); err == nil {
		t.Fatal("Open() error = nil, want connection error")
	}
}
