package messaging

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func TestClassifyPersistError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want persistErrorClass
	}{
		{name: "mysql foreign key", err: &mysql.MySQLError{Number: 1452, Message: "foreign key"}, want: persistErrorMessage},
		{name: "mysql data too long", err: &mysql.MySQLError{Number: 1406, Message: "too long"}, want: persistErrorMessage},
		{name: "mysql out of range", err: &mysql.MySQLError{Number: 1264, Message: "out of range"}, want: persistErrorMessage},
		{name: "wrapped foreign key", err: fmt.Errorf("persist: %w", &mysql.MySQLError{Number: 1452}), want: persistErrorMessage},
		{name: "mysql lock wait timeout", err: &mysql.MySQLError{Number: 1205}, want: persistErrorTransient},
		{name: "mysql deadlock", err: &mysql.MySQLError{Number: 1213}, want: persistErrorTransient},
		{name: "mysql too many connections", err: &mysql.MySQLError{Number: 1040}, want: persistErrorTransient},
		{name: "mysql shutdown", err: &mysql.MySQLError{Number: 1053}, want: persistErrorTransient},
		{name: "mysql shutdown complete", err: &mysql.MySQLError{Number: 1077}, want: persistErrorTransient},
		{name: "mysql unknown error is message-specific", err: &mysql.MySQLError{Number: 1105}, want: persistErrorMessage},
		{name: "mysql connection aborted", err: &mysql.MySQLError{Number: 1152}, want: persistErrorTransient},
		{name: "mysql packet too large is message-specific", err: &mysql.MySQLError{Number: 1153}, want: persistErrorMessage},
		{name: "mysql read only", err: &mysql.MySQLError{Number: 1290}, want: persistErrorTransient},
		{name: "mysql query interrupted", err: &mysql.MySQLError{Number: 1317}, want: persistErrorTransient},
		{name: "mysql super read only", err: &mysql.MySQLError{Number: 1836}, want: persistErrorTransient},
		{name: "mysql connection killed", err: &mysql.MySQLError{Number: 1927}, want: persistErrorTransient},
		{name: "mysql server gone", err: &mysql.MySQLError{Number: 2006}, want: persistErrorTransient},
		{name: "mysql server lost", err: &mysql.MySQLError{Number: 2013}, want: persistErrorTransient},
		{name: "mysql max execution time", err: &mysql.MySQLError{Number: 3024}, want: persistErrorTransient},
		{name: "mysql inactivity disconnect", err: &mysql.MySQLError{Number: 4031}, want: persistErrorTransient},
		{name: "wrapped deadlock", err: fmt.Errorf("persist: %w", &mysql.MySQLError{Number: 1213}), want: persistErrorTransient},
		{name: "bad connection", err: driver.ErrBadConn, want: persistErrorTransient},
		{name: "wrapped bad connection", err: fmt.Errorf("query: %w", driver.ErrBadConn), want: persistErrorTransient},
		{name: "invalid connection", err: mysql.ErrInvalidConn, want: persistErrorTransient},
		{name: "packet out of sync", err: mysql.ErrPktSync, want: persistErrorTransient},
		{name: "malformed packet", err: fmt.Errorf("read: %w", mysql.ErrMalformPkt), want: persistErrorTransient},
		{name: "connection done", err: sql.ErrConnDone, want: persistErrorTransient},
		{name: "net timeout", err: &net.DNSError{Err: "timeout", Name: "mysql", IsTimeout: true}, want: persistErrorTransient},
		{name: "wrapped net timeout", err: fmt.Errorf("dial mysql: %w", &net.DNSError{IsTimeout: true}), want: persistErrorTransient},
		{name: "eof", err: io.EOF, want: persistErrorTransient},
		{name: "unexpected eof", err: fmt.Errorf("read: %w", io.ErrUnexpectedEOF), want: persistErrorTransient},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: persistErrorTransient},
		{name: "connection refused", err: fmt.Errorf("dial: %w", syscall.ECONNREFUSED), want: persistErrorTransient},
		{name: "connection reset", err: fmt.Errorf("read: %w", syscall.ECONNRESET), want: persistErrorTransient},
		{name: "context canceled is not transient", err: context.Canceled, want: persistErrorMessage},
		{name: "unknown error", err: errors.New("unexpected"), want: persistErrorMessage},
		{name: "message conflict", err: store.ErrMessageConflict, want: persistErrorConflict},
		{name: "wrapped message conflict", err: fmt.Errorf("persist: %w", store.ErrMessageConflict), want: persistErrorConflict},
		{name: "seq conflict", err: store.ErrSeqConflict, want: persistErrorConflict},
		{name: "wrapped seq conflict", err: fmt.Errorf("persist: %w", store.ErrSeqConflict), want: persistErrorConflict},
		{name: "seq invalid", err: store.ErrSeqInvalid, want: persistErrorConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyPersistError(tc.err); got != tc.want {
				t.Fatalf("classifyPersistError(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestDLQConflictReason(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{name: "message conflict", err: store.ErrMessageConflict, want: dlqReasonConflict},
		{name: "seq conflict", err: fmt.Errorf("persist: %w", store.ErrSeqConflict), want: dlqReasonSeqConflict},
		{name: "seq invalid", err: store.ErrSeqInvalid, want: dlqReasonSeqConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dlqConflictReason(tc.err); got != tc.want {
				t.Fatalf("dlqConflictReason(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
