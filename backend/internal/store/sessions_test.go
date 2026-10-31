package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// A deadlock rollback is the one error CreateSession retries; everything else
// must surface immediately so callers are not delayed by pointless attempts.
func TestIsMySQLDeadlock(t *testing.T) {
	deadlock := &mysql.MySQLError{Number: mysqlErrDeadlock, Message: "Deadlock found"}
	other := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"deadlock", deadlock, true},
		{"wrapped deadlock", fmt.Errorf("insert session: %w", deadlock), true},
		{"duplicate key", other, false},
		{"plain error", errors.New("boom"), false},
		{"joined with deadlock", errors.Join(errors.New("boom"), deadlock), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMySQLDeadlock(tc.err); got != tc.want {
				t.Fatalf("isMySQLDeadlock(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// The retry delay must stop early when the caller goes away, so a cancelled
// request does not keep waiting out the backoff.
func TestSleepContextReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	if err := sleepContext(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("sleepContext error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("sleepContext waited %v after cancellation, want an immediate return", elapsed)
	}
}

func TestSleepContextWaits(t *testing.T) {
	if err := sleepContext(context.Background(), 5*time.Millisecond); err != nil {
		t.Fatalf("sleepContext returned %v, want nil", err)
	}
}