package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// scriptStep is one expected database interaction. match is a whitespace
// normalized SQL substring; rows are returned for queries.
type scriptStep struct {
	match    string
	columns  []string
	rows     [][]driver.Value
	affected int64
	lastID   int64
}

type recordedCall struct {
	query string
	args  []driver.NamedValue
}

type script struct {
	t      *testing.T
	steps  []scriptStep
	calls  []recordedCall
	failed bool
}

var errScriptMismatch = errors.New("store test script mismatch")

func normalizeSQL(query string) string {
	return strings.Join(strings.Fields(query), " ")
}

func (s *script) next(query string, args []driver.NamedValue) (scriptStep, error) {
	s.calls = append(s.calls, recordedCall{query: normalizeSQL(query), args: args})
	if len(s.steps) == 0 {
		s.failed = true
		s.t.Errorf("unexpected query: %s", normalizeSQL(query))
		return scriptStep{}, errScriptMismatch
	}
	step := s.steps[0]
	if step.match != "" && !strings.Contains(normalizeSQL(query), normalizeSQL(step.match)) {
		s.failed = true
		s.t.Errorf("query %q does not contain %q", normalizeSQL(query), normalizeSQL(step.match))
		return scriptStep{}, errScriptMismatch
	}
	s.steps = s.steps[1:]
	return step, nil
}

func (s *script) assertDone(t *testing.T) {
	t.Helper()
	if len(s.steps) != 0 {
		t.Fatalf("%d scripted database steps were not used", len(s.steps))
	}
}

func (s *script) call(t *testing.T, index int) recordedCall {
	t.Helper()
	if index >= len(s.calls) {
		t.Fatalf("call %d missing, have %d calls", index, len(s.calls))
	}
	return s.calls[index]
}

func (s *script) assertQueryContains(t *testing.T, index int, fragments ...string) {
	t.Helper()
	query := s.call(t, index).query
	for _, fragment := range fragments {
		if !strings.Contains(query, fragment) {
			t.Fatalf("query %d = %q, want it to contain %q", index, query, fragment)
		}
	}
}

func (s *script) assertArg(t *testing.T, index, argIndex int, want any) {
	t.Helper()
	call := s.call(t, index)
	if argIndex >= len(call.args) {
		t.Fatalf("call %d has %d args, want arg %d", index, len(call.args), argIndex)
	}
	if fmt.Sprint(call.args[argIndex].Value) != fmt.Sprint(want) {
		t.Fatalf("call %d arg %d = %v, want %v", index, argIndex, call.args[argIndex].Value, want)
	}
}

type scriptDriver struct {
	script *script
}

func (d *scriptDriver) Open(string) (driver.Conn, error) {
	return &scriptConn{script: d.script}, nil
}

type scriptConn struct {
	script *script
}

func (c *scriptConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported by the store test driver")
}

func (c *scriptConn) Close() error { return nil }

func (c *scriptConn) Begin() (driver.Tx, error) { return scriptTx{}, nil }

func (c *scriptConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return scriptTx{}, nil
}

func (c *scriptConn) Ping(context.Context) error { return nil }

func (c *scriptConn) QueryContext(
	_ context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Rows, error) {
	step, err := c.script.next(query, args)
	if err != nil {
		return nil, err
	}
	return &scriptRows{columns: step.columns, rows: step.rows}, nil
}

func (c *scriptConn) ExecContext(
	_ context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	step, err := c.script.next(query, args)
	if err != nil {
		return nil, err
	}
	return scriptResult{affected: step.affected, lastID: step.lastID}, nil
}

type scriptResult struct {
	affected int64
	lastID   int64
}

func (r scriptResult) LastInsertId() (int64, error) { return r.lastID, nil }
func (r scriptResult) RowsAffected() (int64, error) { return r.affected, nil }

type scriptTx struct{}

func (scriptTx) Commit() error   { return nil }
func (scriptTx) Rollback() error { return nil }

type scriptRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

func (r *scriptRows) Columns() []string { return r.columns }
func (r *scriptRows) Close() error      { return nil }

func (r *scriptRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

var scriptDriverCounter uint64

// newScriptStore returns a Store backed by a scripted in-memory driver. Each
// query or exec consumes the next step.
func newScriptStore(t *testing.T, steps ...scriptStep) (*Store, *script) {
	t.Helper()
	runner := &script{t: t, steps: steps}
	name := fmt.Sprintf("store-script-%d", atomic.AddUint64(&scriptDriverCounter, 1))
	sql.Register(name, &scriptDriver{script: runner})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open scripted database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &Store{db: db}, runner
}

func seqTestTime() time.Time {
	return time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
}
