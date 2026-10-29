package messaging

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/go-sql-driver/mysql"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// persistErrorClass selects the consumer reaction to a failed MySQL write.
type persistErrorClass string

const (
	// persistErrorTransient covers MySQL or network trouble that is unrelated
	// to the message itself. It is retried forever and never counted.
	persistErrorTransient persistErrorClass = "transient"
	// persistErrorMessage covers failures caused by one message. It gets a
	// fixed number of retries before the message is dead-lettered.
	persistErrorMessage persistErrorClass = "message"
	// persistErrorConflict means the write lost an idempotency race and cannot
	// succeed by retrying. It is dead-lettered immediately for inspection.
	persistErrorConflict persistErrorClass = "conflict"
)

// transientMySQLNumbers are server-side errors that survive a reconnect.
// Client-side 20xx numbers appear when go-sql-driver could not mask a broken
// connection behind driver.ErrBadConn. 1105 (ER_UNKNOWN_ERROR) and 1153
// (ER_NET_PACKET_TOO_LARGE) are deliberately absent: a deterministic occurrence
// is caused by the message itself, so they use the bounded message class
// instead of being retried forever.
var transientMySQLNumbers = map[uint16]bool{
	1040: true, // too many connections
	1041: true, // out of memory
	1053: true, // server shutdown in progress
	1077: true, // server shutdown complete
	1152: true, // connection aborted
	1205: true, // lock wait timeout exceeded
	1213: true, // deadlock found, transaction rolled back
	1290: true, // server in read-only mode
	1317: true, // query execution interrupted
	1836: true, // server in read-only mode
	1927: true, // connection killed
	2002: true, // cannot connect to server
	2003: true, // cannot connect to server
	2006: true, // server has gone away
	2013: true, // lost connection during query
	3024: true, // max execution time exceeded
	4031: true, // client disconnected because of inactivity
}

// classifyPersistError maps a failed store write to the consumer reaction.
// context.Canceled is not classified as transient: it means the consumer is
// shutting down and the caller checks its own context instead.
func classifyPersistError(err error) persistErrorClass {
	if errors.Is(err, store.ErrMessageConflict) ||
		errors.Is(err, store.ErrSeqConflict) ||
		errors.Is(err, store.ErrSeqInvalid) {
		return persistErrorConflict
	}
	if isTransientPersistError(err) {
		return persistErrorTransient
	}
	return persistErrorMessage
}

func isTransientPersistError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, mysql.ErrInvalidConn) ||
		errors.Is(err, mysql.ErrPktSync) ||
		errors.Is(err, mysql.ErrMalformPkt) ||
		errors.Is(err, sql.ErrConnDone) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return transientMySQLNumbers[mysqlErr.Number]
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// dlqConflictReason names the dead-letter reason for a conflict class error.
func dlqConflictReason(err error) string {
	if errors.Is(err, store.ErrSeqConflict) || errors.Is(err, store.ErrSeqInvalid) {
		return dlqReasonSeqConflict
	}
	return dlqReasonConflict
}
