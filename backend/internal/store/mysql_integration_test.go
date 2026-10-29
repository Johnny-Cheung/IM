package store

// Real-MySQL coverage for the v1.8.2 reconciliation queries and for the
// reconcile-vs-consumer persist race. The whole file is skipped unless
// IM_TEST_MYSQL_DSN is set, so `go test ./...` stays hermetic.
//
// The test owns a throwaway schema named "<dsn database>_reconcile_test"
// ("im_reconcile_test" when the DSN names no database). It drops and recreates
// that schema on every run and drops it again on cleanup, so the DSN user needs
// CREATE/DROP on it, for example:
//
//	GRANT ALL ON `im_reconcile_test`.* TO 'im_app'@'%';
//
// Only schemas whose name ends in "_reconcile_test" are ever dropped.
//
// Run with:
//
//	IM_TEST_MYSQL_DSN='im_app:secret@tcp(127.0.0.1:13307)/im' \
//	  go test -race -count=1 -run TestMySQLReconcileIntegration ./internal/store/

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	integrationSchemaSuffix = "_reconcile_test"
	integrationSetupTimeout = 60 * time.Second
)

var integrationSchemaPattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)

func TestMySQLReconcileIntegration(t *testing.T) {
	st := openIntegrationStore(t)
	seed := seedIntegrationData(t, st)

	t.Run("migrations are applied and idempotent", func(t *testing.T) {
		assertMigrationCount(t, st)
		if err := st.Migrate(context.Background()); err != nil {
			t.Fatalf("second Migrate() error = %v, want nil", err)
		}
		assertMigrationCount(t, st)
	})

	t.Run("group by runs under only_full_group_by", func(t *testing.T) {
		var mode string
		if err := st.db.QueryRowContext(context.Background(), `SELECT @@SESSION.sql_mode`).Scan(&mode); err != nil {
			t.Fatalf("read sql_mode: %v", err)
		}
		if !strings.Contains(mode, "ONLY_FULL_GROUP_BY") {
			t.Fatalf("sql_mode = %q, want ONLY_FULL_GROUP_BY", mode)
		}
	})

	t.Run("conversation seq stats", func(t *testing.T) {
		cases := []struct {
			name                string
			afterLow, afterHigh uint64
			limit               int
			want                []ConversationSeqStat
		}{
			{
				name: "limit one from start", limit: 1,
				want: []ConversationSeqStat{{LowID: 1, HighID: 2, Count: 3, MaxSeq: 3}},
			},
			{
				name: "limit two from start", limit: 2,
				want: []ConversationSeqStat{
					{LowID: 1, HighID: 2, Count: 3, MaxSeq: 3},
					{LowID: 1, HighID: 3, Count: 2, MaxSeq: 3},
				},
			},
			{
				name: "page after first pair", afterLow: 1, afterHigh: 2, limit: 1,
				want: []ConversationSeqStat{{LowID: 1, HighID: 3, Count: 2, MaxSeq: 3}},
			},
			{
				name: "last pair", afterLow: 1, afterHigh: 3, limit: 2,
				want: []ConversationSeqStat{{LowID: 2, HighID: 4, Count: 1, MaxSeq: 1}},
			},
			{
				name: "page from inside the pair range", afterLow: 1, afterHigh: 4, limit: 10,
				want: []ConversationSeqStat{{LowID: 2, HighID: 4, Count: 1, MaxSeq: 1}},
			},
			{
				name: "empty final page", afterLow: 2, afterHigh: 4, limit: 10,
				want: []ConversationSeqStat{},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := st.ListConversationSeqStats(context.Background(), tc.afterLow, tc.afterHigh, tc.limit)
				if err != nil {
					t.Fatalf("ListConversationSeqStats(%d,%d,%d) error = %v", tc.afterLow, tc.afterHigh, tc.limit, err)
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("ListConversationSeqStats(%d,%d,%d) = %+v, want %+v",
						tc.afterLow, tc.afterHigh, tc.limit, got, tc.want)
				}
			})
		}
		if stats, err := st.ListConversationSeqStats(context.Background(), 0, 0, 0); err != nil || stats != nil {
			t.Fatalf("ListConversationSeqStats limit 0 = %+v, %v; want nil, nil", stats, err)
		}
	})

	t.Run("group seq stats", func(t *testing.T) {
		cases := []struct {
			name    string
			afterID uint64
			limit   int
			want    []GroupSeqStat
		}{
			{
				name: "limit one from start", limit: 1,
				want: []GroupSeqStat{{GroupID: 1, Count: 4, MaxSeq: 4}},
			},
			{
				name: "dissolved group still listed", afterID: 1, limit: 1,
				want: []GroupSeqStat{{GroupID: 2, Count: 2, MaxSeq: 3}},
			},
			{
				name: "empty final page skips unnumbered group", afterID: 2, limit: 10,
				want: []GroupSeqStat{},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := st.ListGroupSeqStats(context.Background(), tc.afterID, tc.limit)
				if err != nil {
					t.Fatalf("ListGroupSeqStats(%d,%d) error = %v", tc.afterID, tc.limit, err)
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("ListGroupSeqStats(%d,%d) = %+v, want %+v", tc.afterID, tc.limit, got, tc.want)
				}
			})
		}
		if stats, err := st.ListGroupSeqStats(context.Background(), 0, 0); err != nil || stats != nil {
			t.Fatalf("ListGroupSeqStats limit 0 = %+v, %v; want nil, nil", stats, err)
		}
	})

	t.Run("conversation seqs are ordered and bounded by upTo", func(t *testing.T) {
		cases := []struct {
			name                string
			lowID, highID, upTo uint64
			want                []uint64
		}{
			{name: "upTo zero", lowID: 1, highID: 3, upTo: 0, want: nil},
			{name: "upTo first seq", lowID: 1, highID: 3, upTo: 1, want: []uint64{1}},
			{name: "upTo stops before gap", lowID: 1, highID: 3, upTo: 2, want: []uint64{1}},
			{name: "upTo includes gap edge", lowID: 1, highID: 3, upTo: 3, want: []uint64{1, 3}},
			{name: "upTo past last seq", lowID: 1, highID: 3, upTo: 100, want: []uint64{1, 3}},
			{name: "dense conversation", lowID: 1, highID: 2, upTo: 3, want: []uint64{1, 2, 3}},
			{name: "pair without seq", lowID: 4, highID: 5, upTo: 10, want: []uint64{}},
			{name: "conversation without rows", lowID: 3, highID: 5, upTo: 10, want: []uint64{}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := listConversationSeqs(st, tc.lowID, tc.highID, tc.upTo, 0, 1000)
				if err != nil {
					t.Fatalf("ListConversationSeqs(%d,%d,%d) error = %v", tc.lowID, tc.highID, tc.upTo, err)
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("ListConversationSeqs(%d,%d,%d) = %v, want %v", tc.lowID, tc.highID, tc.upTo, got, tc.want)
				}
			})
		}
	})

	t.Run("conversation seq paging", func(t *testing.T) {
		cases := []struct {
			name     string
			upTo     uint64
			afterSeq uint64
			limit    int
			want     []uint64
		}{
			{name: "limit one first page", upTo: 3, limit: 1, want: []uint64{1}},
			{name: "limit one second page", upTo: 3, afterSeq: 1, limit: 1, want: []uint64{2}},
			{name: "limit one last page", upTo: 3, afterSeq: 2, limit: 1, want: []uint64{3}},
			{name: "limit one empty final page", upTo: 3, afterSeq: 3, limit: 1, want: []uint64{}},
			{name: "limit two first page", upTo: 3, limit: 2, want: []uint64{1, 2}},
			{name: "limit two last page", upTo: 3, afterSeq: 2, limit: 2, want: []uint64{3}},
			{name: "limit two empty final page", upTo: 3, afterSeq: 3, limit: 2, want: []uint64{}},
			{name: "upTo bounds the page", upTo: 2, limit: 10, want: []uint64{1, 2}},
			{name: "zero limit", upTo: 3, limit: 0, want: nil},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := listConversationSeqs(st, 1, 2, tc.upTo, tc.afterSeq, tc.limit)
				if err != nil {
					t.Fatalf("ListConversationSeqs(%d,%d,%d,%d,%d) error = %v",
						1, 2, tc.upTo, tc.afterSeq, tc.limit, err)
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("ListConversationSeqs(%d,%d,%d,%d,%d) = %v, want %v",
						1, 2, tc.upTo, tc.afterSeq, tc.limit, got, tc.want)
				}
			})
		}
	})

	t.Run("group seqs are ordered and bounded by upTo", func(t *testing.T) {
		cases := []struct {
			name    string
			groupID uint64
			upTo    uint64
			want    []uint64
		}{
			{name: "upTo zero", groupID: 2, upTo: 0, want: nil},
			{name: "upTo first seq", groupID: 2, upTo: 1, want: []uint64{1}},
			{name: "upTo stops before gap", groupID: 2, upTo: 2, want: []uint64{1}},
			{name: "upTo includes gap edge", groupID: 2, upTo: 3, want: []uint64{1, 3}},
			{name: "dense group", groupID: 1, upTo: 4, want: []uint64{1, 2, 3, 4}},
			{name: "group without seq", groupID: 3, upTo: 10, want: []uint64{}},
			{name: "group without rows", groupID: 4, upTo: 10, want: []uint64{}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := listGroupSeqs(st, tc.groupID, tc.upTo, 0, 1000)
				if err != nil {
					t.Fatalf("ListGroupSeqs(%d,%d) error = %v", tc.groupID, tc.upTo, err)
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("ListGroupSeqs(%d,%d) = %v, want %v", tc.groupID, tc.upTo, got, tc.want)
				}
			})
		}
	})

	t.Run("group seq paging", func(t *testing.T) {
		cases := []struct {
			name     string
			groupID  uint64
			upTo     uint64
			afterSeq uint64
			limit    int
			want     []uint64
		}{
			{name: "limit one first page", groupID: 1, upTo: 4, limit: 1, want: []uint64{1}},
			{name: "limit one second page", groupID: 1, upTo: 4, afterSeq: 1, limit: 1, want: []uint64{2}},
			{name: "limit one last page", groupID: 1, upTo: 4, afterSeq: 3, limit: 1, want: []uint64{4}},
			{name: "limit one empty final page", groupID: 1, upTo: 4, afterSeq: 4, limit: 1, want: []uint64{}},
			{name: "limit three first page", groupID: 1, upTo: 4, limit: 3, want: []uint64{1, 2, 3}},
			{name: "limit three last page", groupID: 1, upTo: 4, afterSeq: 3, limit: 3, want: []uint64{4}},
			{name: "limit three empty final page", groupID: 1, upTo: 4, afterSeq: 4, limit: 3, want: []uint64{}},
			{name: "page across the gap", groupID: 2, upTo: 3, afterSeq: 1, limit: 1, want: []uint64{3}},
			{name: "page after the gap", groupID: 2, upTo: 3, afterSeq: 3, limit: 1, want: []uint64{}},
			{name: "zero limit", groupID: 1, upTo: 4, limit: 0, want: nil},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := listGroupSeqs(st, tc.groupID, tc.upTo, tc.afterSeq, tc.limit)
				if err != nil {
					t.Fatalf("ListGroupSeqs(%d,%d,%d,%d) error = %v",
						tc.groupID, tc.upTo, tc.afterSeq, tc.limit, err)
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("ListGroupSeqs(%d,%d,%d,%d) = %v, want %v",
						tc.groupID, tc.upTo, tc.afterSeq, tc.limit, got, tc.want)
				}
			})
		}
	})

	t.Run("persist message race", func(t *testing.T) {
		message := Message{
			ID:              900001,
			Seq:             500,
			SenderID:        2,
			RecipientID:     5,
			ClientMessageID: "race-direct",
			Body:            "direct race body",
			CreatedAt:       seed.createdAt,
		}
		created := persistConcurrently(t, 2, func() (bool, error) {
			return st.PersistMessage(context.Background(), message)
		})
		assertPersistRace(t, created, "direct")
		assertStoredMessage(t, st, message)
		assertIdempotentDuplicate(t, st, message)

		conflict := message
		conflict.Body = "direct race body changed"
		if _, err := st.PersistMessage(context.Background(), conflict); !errors.Is(err, ErrMessageConflict) {
			t.Fatalf("same server id with different body error = %v, want ErrMessageConflict", err)
		}

		other := Message{
			ID:              900003,
			Seq:             message.Seq,
			SenderID:        5,
			RecipientID:     2,
			ClientMessageID: "race-direct-other",
			Body:            "another body",
			CreatedAt:       seed.createdAt,
		}
		if _, err := st.PersistMessage(context.Background(), other); !errors.Is(err, ErrSeqConflict) {
			t.Fatalf("same conversation seq with another message error = %v, want ErrSeqConflict", err)
		}
	})

	t.Run("persist group message race", func(t *testing.T) {
		message := GroupMessage{
			ID:              900002,
			Seq:             500,
			GroupID:         4,
			SenderID:        1,
			ClientMessageID: "race-group",
			Body:            "group race body",
			CreatedAt:       seed.createdAt,
		}
		created := persistConcurrently(t, 2, func() (bool, error) {
			return st.PersistGroupMessage(context.Background(), message)
		})
		assertPersistRace(t, created, "group")
		assertStoredGroupMessage(t, st, message)
		assertIdempotentDuplicateGroup(t, st, message)

		conflict := message
		conflict.Body = "group race body changed"
		if _, err := st.PersistGroupMessage(context.Background(), conflict); !errors.Is(err, ErrMessageConflict) {
			t.Fatalf("same server id with different body error = %v, want ErrMessageConflict", err)
		}

		other := GroupMessage{
			ID:              900004,
			Seq:             message.Seq,
			GroupID:         message.GroupID,
			SenderID:        2,
			ClientMessageID: "race-group-other",
			Body:            "another group body",
			CreatedAt:       seed.createdAt,
		}
		if _, err := st.PersistGroupMessage(context.Background(), other); !errors.Is(err, ErrSeqConflict) {
			t.Fatalf("same group seq with another message error = %v, want ErrSeqConflict", err)
		}
	})
}

// integrationFixture carries values the assertions need from the seed.
type integrationFixture struct {
	createdAt time.Time
}

// openIntegrationStore creates the dedicated test schema, applies the embedded
// migrations and returns a Store scoped to it.
func openIntegrationStore(t *testing.T) *Store {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("IM_TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("IM_TEST_MYSQL_DSN is not set; skipping real-MySQL integration test")
	}
	base, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse IM_TEST_MYSQL_DSN: %v", err)
	}
	schema, err := integrationSchemaName(base.DBName)
	if err != nil {
		t.Fatalf("derive test schema: %v", err)
	}

	cfg := *base
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.Timeout = 10 * time.Second
	// Production never sets either flag; the duplicate-detection path relies on
	// a no-op ON DUPLICATE KEY UPDATE reporting zero affected rows.
	cfg.ClientFoundRows = false
	cfg.MultiStatements = false
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	// Force the strict modes newer MySQL enables by default so the GROUP BY is
	// validated the way production runs it, even on a relaxed server.
	cfg.Params["sql_mode"] = "'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES'"

	adminCfg := cfg
	adminCfg.DBName = ""
	admin, err := sql.Open("mysql", adminCfg.FormatDSN())
	if err != nil {
		t.Fatalf("open MySQL admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	setupCtx, cancel := context.WithTimeout(context.Background(), integrationSetupTimeout)
	defer cancel()
	if _, err := admin.ExecContext(setupCtx, "DROP DATABASE IF EXISTS `"+schema+"`"); err != nil {
		t.Fatalf("drop test schema %s: %v", schema, err)
	}
	if _, err := admin.ExecContext(setupCtx,
		"CREATE DATABASE `"+schema+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		t.Fatalf("create test schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if _, err := admin.ExecContext(dropCtx, "DROP DATABASE IF EXISTS `"+schema+"`"); err != nil {
			t.Logf("test schema %s left behind: %v", schema, err)
		}
	})

	cfg.DBName = schema
	st, err := Open(context.Background(), cfg.FormatDSN())
	if err != nil {
		t.Fatalf("open test schema %s: %v", schema, err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(setupCtx); err != nil {
		t.Fatalf("migrate test schema %s: %v", schema, err)
	}
	return st
}

// integrationSchemaName derives a schema that is clearly owned by this test.
func integrationSchemaName(dbName string) (string, error) {
	name := strings.TrimSpace(dbName)
	if name == "" {
		name = "im"
	}
	if !strings.HasSuffix(name, integrationSchemaSuffix) {
		name += integrationSchemaSuffix
	}
	if !integrationSchemaPattern.MatchString(name) {
		return "", fmt.Errorf("schema %q is not a plain MySQL identifier", name)
	}
	return name, nil
}

func assertMigrationCount(t *testing.T, st *Store) {
	t.Helper()
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read embedded migrations: %v", err)
	}
	want := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			want++
		}
	}
	var got int
	if err := st.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM schema_migrations`).Scan(&got); err != nil {
		t.Fatalf("count applied migrations: %v", err)
	}
	if got != want {
		t.Fatalf("applied migrations = %d, want %d", got, want)
	}
}

// listConversationSeqs keeps the paging-aware call in one place.
func listConversationSeqs(st *Store, lowID, highID, upTo, afterSeq uint64, limit int) ([]uint64, error) {
	return st.ListConversationSeqs(context.Background(), lowID, highID, upTo, afterSeq, limit)
}

func listGroupSeqs(st *Store, groupID, upTo, afterSeq uint64, limit int) ([]uint64, error) {
	return st.ListGroupSeqs(context.Background(), groupID, upTo, afterSeq, limit)
}

func seedIntegrationData(t *testing.T, st *Store) integrationFixture {
	t.Helper()
	createdAt := time.Date(2026, 2, 3, 4, 5, 6, 123_000_000, time.UTC)
	fixture := integrationFixture{createdAt: createdAt}

	for _, index := range []int{1, 2, 3, 4, 5} {
		name := fmt.Sprintf("user%d", index)
		execSQL(t, st,
			`INSERT INTO users (id, username, username_key, nickname, password_hash)
			 VALUES (?, ?, ?, ?, ?)`,
			index, name, name, name, "hash")
	}
	for _, pair := range [][2]int{{1, 2}, {1, 3}, {2, 4}, {4, 5}} {
		execSQL(t, st,
			`INSERT INTO friendships (user_id, friend_user_id, created_at) VALUES (?, ?, ?)`,
			pair[0], pair[1], createdAt)
	}

	direct := []struct {
		serverID          uint64
		lowID, highID     any
		seq               any
		sender, recipient int
		clientID          string
	}{
		{1001, 1, 2, 1, 1, 2, "c-1001"},
		{1002, 1, 2, 2, 2, 1, "c-1002"},
		{1003, 1, 2, 3, 1, 2, "c-1003"},
		{1004, 1, 3, 3, 1, 3, "c-1004"},
		{1005, 1, 3, 1, 3, 1, "c-1005"},
		{1006, 2, 4, 1, 2, 4, "c-1006"},
		{2001, nil, nil, nil, 1, 2, "c-legacy-1"}, // pre-numbering row
		{2002, nil, nil, 9, 1, 2, "c-legacy-2"},   // seq without a conversation pair
		{2003, 4, 5, nil, 4, 5, "c-legacy-3"},     // conversation pair without a seq
	}
	for _, item := range direct {
		execSQL(t, st,
			`INSERT INTO messages
			   (server_message_id, conversation_low_id, conversation_high_id, seq,
			    sender_id, recipient_id, client_message_id, body, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			item.serverID, item.lowID, item.highID, item.seq,
			item.sender, item.recipient, item.clientID, "body-"+item.clientID, createdAt)
	}

	dissolvedAt := createdAt.Add(time.Hour)
	groups := []struct {
		id        int
		name      string
		owner     int
		dissolved any
	}{
		{1, "group one", 1, nil},
		{2, "group two", 1, dissolvedAt},
		{3, "group three", 2, nil},
		{4, "group four", 1, nil},
	}
	for _, group := range groups {
		execSQL(t, st,
			`INSERT INTO chat_groups (id, name, owner_id, created_at, dissolved_at) VALUES (?, ?, ?, ?, ?)`,
			group.id, group.name, group.owner, createdAt, group.dissolved)
	}
	for _, member := range [][2]int{{1, 1}, {1, 2}, {2, 1}, {3, 2}, {4, 1}} {
		execSQL(t, st,
			`INSERT INTO group_members (group_id, user_id, joined_at, last_read_message_id, last_read_seq)
			 VALUES (?, ?, ?, 0, 0)`,
			member[0], member[1], createdAt)
	}

	groupMessages := []struct {
		serverID uint64
		groupID  int
		seq      any
		sender   int
		clientID string
	}{
		{3001, 1, 1, 1, "c-3001"},
		{3002, 1, 2, 2, "c-3002"},
		{3003, 1, 3, 1, "c-3003"},
		{3004, 1, 4, 2, "c-3004"},
		{3006, 2, 3, 2, "c-3006"},
		{3005, 2, 1, 1, "c-3005"},
		{3007, 3, nil, 1, "c-3007"}, // unnumbered, must be ignored by the stats
	}
	for _, item := range groupMessages {
		execSQL(t, st,
			`INSERT INTO group_messages
			   (server_message_id, seq, group_id, sender_id, client_message_id, body, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			item.serverID, item.seq, item.groupID, item.sender, item.clientID, "body-"+item.clientID, createdAt)
	}
	return fixture
}

func execSQL(t *testing.T, st *Store, query string, args ...any) {
	t.Helper()
	if _, err := st.db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", strings.Join(strings.Fields(query), " "), err)
	}
}

// persistConcurrently starts racers that call persist at the same time and
// returns the created flags in racer order. Deadlocks and lock-wait timeouts are
// retried the way the consumer's transient class is, so a legitimate transient
// error does not hide the duplicate result under test.
func persistConcurrently(t *testing.T, racers int, persist func() (bool, error)) []bool {
	t.Helper()
	created := make([]bool, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(racers)
	for index := 0; index < racers; index++ {
		go func(racer int) {
			defer wg.Done()
			<-start
			created[racer], errs[racer] = persistWithTransientRetry(t, racer, persist)
		}(index)
	}
	close(start)
	wg.Wait()

	for racer, err := range errs {
		if err != nil {
			t.Fatalf("concurrent persist %d error = %v, want nil", racer, err)
		}
	}
	return created
}

func persistWithTransientRetry(t *testing.T, racer int, persist func() (bool, error)) (bool, error) {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		created, err := persist()
		if err == nil {
			return created, nil
		}
		if !isTransientMySQL(err) {
			return false, err
		}
		lastErr = err
		t.Logf("racer %d attempt %d transient error, retrying: %v", racer, attempt, err)
	}
	return false, lastErr
}

func isTransientMySQL(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 1205 || mysqlErr.Number == 1213
}

func assertPersistRace(t *testing.T, created []bool, chain string) {
	t.Helper()
	inserted := 0
	for _, flag := range created {
		if flag {
			inserted++
		}
	}
	if inserted != 1 {
		t.Fatalf("%s concurrent persist created flags = %v, want exactly one true", chain, created)
	}
}

func assertStoredMessage(t *testing.T, st *Store, message Message) {
	t.Helper()
	var count int
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messages WHERE server_message_id = ?`, message.ID,
	).Scan(&count); err != nil {
		t.Fatalf("count direct race rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("direct race rows = %d, want 1", count)
	}
	var seq uint64
	var clientID, body string
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT seq, client_message_id, body FROM messages WHERE server_message_id = ?`, message.ID,
	).Scan(&seq, &clientID, &body); err != nil {
		t.Fatalf("read direct race row: %v", err)
	}
	if seq != message.Seq || clientID != message.ClientMessageID || body != message.Body {
		t.Fatalf("direct race row = seq %d client %q body %q, want %d %q %q",
			seq, clientID, body, message.Seq, message.ClientMessageID, message.Body)
	}
}

func assertStoredGroupMessage(t *testing.T, st *Store, message GroupMessage) {
	t.Helper()
	var count int
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM group_messages WHERE server_message_id = ?`, message.ID,
	).Scan(&count); err != nil {
		t.Fatalf("count group race rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("group race rows = %d, want 1", count)
	}
	var seq uint64
	var clientID, body string
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT seq, client_message_id, body FROM group_messages WHERE server_message_id = ?`, message.ID,
	).Scan(&seq, &clientID, &body); err != nil {
		t.Fatalf("read group race row: %v", err)
	}
	if seq != message.Seq || clientID != message.ClientMessageID || body != message.Body {
		t.Fatalf("group race row = seq %d client %q body %q, want %d %q %q",
			seq, clientID, body, message.Seq, message.ClientMessageID, message.Body)
	}
}

func assertIdempotentDuplicate(t *testing.T, st *Store, message Message) {
	t.Helper()
	created, err := st.PersistMessage(context.Background(), message)
	if err != nil {
		t.Fatalf("repeat PersistMessage() error = %v, want nil", err)
	}
	if created {
		t.Fatal("repeat PersistMessage() created = true, want false")
	}
}

func assertIdempotentDuplicateGroup(t *testing.T, st *Store, message GroupMessage) {
	t.Helper()
	created, err := st.PersistGroupMessage(context.Background(), message)
	if err != nil {
		t.Fatalf("repeat PersistGroupMessage() error = %v, want nil", err)
	}
	if created {
		t.Fatal("repeat PersistGroupMessage() created = true, want false")
	}
}
