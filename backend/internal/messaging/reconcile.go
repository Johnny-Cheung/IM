package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// ReconcileStore is durable storage plus the two Redis-sourced persist methods
// the consumers use. Reconciliation repairs with exactly the same idempotent
// writes, never through the stream or the broker.
type ReconcileStore interface {
	ListConversationSeqStats(
		ctx context.Context,
		afterLowID, afterHighID uint64,
		limit int,
	) ([]store.ConversationSeqStat, error)
	ListGroupSeqStats(
		ctx context.Context,
		afterGroupID uint64,
		limit int,
	) ([]store.GroupSeqStat, error)
	ListConversationSeqs(
		ctx context.Context,
		lowID, highID, upTo, afterSeq uint64,
		limit int,
	) ([]uint64, error)
	ListGroupSeqs(ctx context.Context, groupID, upTo, afterSeq uint64, limit int) ([]uint64, error)
	PersistMessage(ctx context.Context, message store.Message) (bool, error)
	PersistGroupMessage(ctx context.Context, message store.GroupMessage) (bool, error)
	// Reverse-direction queries: MySQL rows missing from Redis and the read
	// watermarks that back them.
	ListMessagesBySeqRange(ctx context.Context, lowID, highID, afterSeq uint64, limit int) ([]store.Message, error)
	ListGroupMessagesBySeqRange(ctx context.Context, groupID, afterSeq uint64, limit int) ([]store.GroupMessage, error)
	CountMessagesInvolvingUser(ctx context.Context, userID uint64) (uint64, error)
	ListUserIDs(ctx context.Context, afterID uint64, limit int) ([]uint64, error)
	ListConversationReadStates(ctx context.Context, afterUserID, afterPeerID uint64, limit int) ([]store.ConversationReadState, error)
	ListGroupMemberReadStates(ctx context.Context, afterGroupID, afterUserID uint64, limit int) ([]store.GroupMemberReadState, error)
	// ListConversationBasics enumerates the peers a user has messages with,
	// which is how the feed audit finds the rows to restore.
	ListConversationBasics(ctx context.Context, userID uint64) ([]store.Conversation, error)
}

// ProjectionRepairer restores Redis read-model entries from durable rows. The
// reconciler only adds: it never deletes an index member, overwrites an
// existing entity or lowers a counter. Existing entities are only rewritten by
// WriteDirectBatch/WriteGroupBatch when the reconciler has proved the entity is
// absent; a present entity is either repaired by ProjectionIndexRepairer or
// left untouched and reported as needing a rebuild.
type ProjectionRepairer interface {
	WriteDirectBatch(context.Context, []store.Message) (int, error)
	WriteGroupBatch(context.Context, []store.GroupMessage) (int, error)
	RaiseDirectReadWatermarks(context.Context, []store.ConversationReadState) (int, error)
	RaiseGroupReadWatermarks(context.Context, []store.GroupMemberReadState) (int, error)
	RaiseDirectCounter(ctx context.Context, lowID, highID, seq uint64) error
	RaiseGroupCounter(ctx context.Context, groupID, seq uint64) error
}

// ProjectionIndexRepairer is the optional index-only extension of
// ProjectionRepairer. When the configured repairer implements it, a message
// entity that already exists and matches MySQL gets its conversation index,
// feed entries, preview and counter restored without rewriting the entity body.
// The reconciler falls back to WriteDirectBatch/WriteGroupBatch when it is not
// implemented, but only for entities it has proved match MySQL.
type ProjectionIndexRepairer interface {
	WriteDirectIndexBatch(context.Context, []store.Message) (int, error)
	WriteGroupIndexBatch(context.Context, []store.GroupMessage) (int, error)
}

// ReconcileOptions configures the background reconciler. Zero values select the
// defaults documented on each field.
type ReconcileOptions struct {
	Keyspace Keyspace
	Gate     *SeqGate
	// Interval between runs once the gate is open. Default 5m.
	Interval time.Duration
	// MinAge protects messages that are still in flight. Default 5m.
	MinAge time.Duration
	// Batch bounds one page of stats rows. Default 500.
	Batch int
	// MaxDetail bounds mismatched conversations repaired per phase per run.
	// Default 50.
	MaxDetail int
	Now       func() time.Time
	// LogEvery bounds how often the same unresolved item is logged. Default 1h.
	LogEvery time.Duration

	// Repair writes Redis projections back from MySQL. Nil disables reverse
	// repair: defects are still detected and counted, never written.
	Repair ProjectionRepairer
	// RepairLimit bounds Redis messages repaired per run. Default 200.
	RepairLimit int
	// RepairPerConversation bounds messages repaired for one conversation per
	// run. Default 50.
	RepairPerConversation int
	// NeedsRebuildMessages flags a conversation whose estimated Redis gap
	// exceeds this. Default 200.
	NeedsRebuildMessages int
	// NeedsRebuildRatio flags a conversation whose gap also exceeds this share
	// of its MySQL count, gated by RepairMinMessages. Default 0.2.
	NeedsRebuildRatio float64
	// RepairMinMessages is the absolute floor for the ratio rule. Default 20.
	RepairMinMessages int
	// DeepAuditInterval between entity and feed samples. Default 1h.
	DeepAuditInterval time.Duration
	// DeepAuditBoundary is how many oldest and newest messages of each audited
	// conversation are compared. Default 2.
	DeepAuditBoundary int
	// DeepAuditUsers is how many users are sampled for feed parity per audit.
	// Default 50.
	DeepAuditUsers int
}

// ReconcileResult is what one reconciliation run did.
type ReconcileResult struct {
	Scanned    uint64
	Mismatched uint64
	Repaired   uint64
	Unresolved uint64
	// InFlight counts missing seqs whose entity is younger than MinAge.
	InFlight uint64
	// Deferred counts conversations whose per-run missing-seq cap was hit; the
	// remainder is retried on the next run.
	Deferred uint64
	Duration time.Duration
	// CountersRaised counts Redis conversation counters raised from MySQL.
	CountersRaised uint64
	// RedisMissing counts Redis-side defects detected: missing counters, index
	// members, entities, feed entries and read watermarks.
	RedisMissing uint64
	// RedisRepaired counts Redis items written back: messages and counters.
	RedisRepaired uint64
	// WatermarksRaised counts read positions moved to the MySQL value.
	WatermarksRaised uint64
	// FeedRepairs counts dm:feed entries restored by the deep audit.
	FeedRepairs uint64
	// NeedsRebuild counts conversations and users flagged this run.
	NeedsRebuild uint64
}

// ReconcileStatus is a snapshot of the reconciler for /ops/infra.
type ReconcileStatus struct {
	Running           bool      `json:"running"`
	LastRunAt         time.Time `json:"last_run_at,omitempty"`
	LastDurationMS    int64     `json:"last_duration_ms"`
	Scanned           uint64    `json:"scanned"`
	Mismatched        uint64    `json:"mismatched"`
	Repaired          uint64    `json:"repaired"`
	Unresolved        uint64    `json:"unresolved"`
	InFlight          uint64    `json:"in_flight"`
	TotalRepaired     uint64    `json:"total_repaired"`
	LastError         string    `json:"last_error,omitempty"`
	LastResultSummary string    `json:"last_result_summary,omitempty"`
	// Reverse-direction repair counters.
	RedisMissing     uint64    `json:"redis_missing"`
	RedisRepaired    uint64    `json:"redis_repaired"`
	WatermarksRaised uint64    `json:"watermarks_raised"`
	FeedRepairs      uint64    `json:"feed_repairs"`
	NeedsRebuild     uint64    `json:"needs_rebuild"`
	LastDeepAuditAt  time.Time `json:"last_deep_audit_at,omitempty"`
	DeepAuditError   string    `json:"deep_audit_error,omitempty"`
}

const (
	defaultReconcileInterval  = 5 * time.Minute
	defaultReconcileMinAge    = 5 * time.Minute
	defaultReconcileBatch     = 500
	defaultReconcileMaxDetail = 50
	defaultReconcileLogEvery  = time.Hour
	reconcileLogKeyLimit      = 2048
	reconcileMaxLogsPerRun    = 500
	// reconcileMaxMissingPerConversation bounds the Redis lookups one damaged
	// conversation can trigger in a run; the walk resumes from its cursor next
	// run.
	reconcileMaxMissingPerConversation = 1000
	reconcileScanKeysPerRun            = 5000
	reconcileScanCountHint             = 500
	reconcileGatePollFloor             = time.Millisecond
	reconcileGatePollCeiling           = time.Second
	reconcileIncompleteEntityMsg       = "entity is incomplete"

	defaultRepairLimit           = 200
	defaultRepairPerConversation = 50
	defaultNeedsRebuildMessages  = 200
	defaultNeedsRebuildRatio     = 0.2
	defaultRepairMinMessages     = 20
	defaultDeepAuditInterval     = time.Hour
	defaultDeepAuditBoundary     = 2
	defaultDeepAuditUsers        = 50
	reconcileDeepAuditPerRun     = 50
	reconcileMaxReverseScan      = 5000
	reconcileReadStatesPerRun    = 2000
	reconcileFeedChecksPerUser   = 2000
	reconcileFeedWriteBatch      = 50
	// reconcileDeepRetryLimit bounds how many hourly audits retry the same
	// deferred row before it is reported as needing a rebuild and skipped, so a
	// permanently failing item cannot pin the deep-audit cursor forever.
	reconcileDeepRetryLimit = 3
)

// Reconciler compares Redis conversation counters with MySQL and repairs
// messages that were accepted but never persisted.
type Reconciler struct {
	client    redis.UniversalClient
	store     ReconcileStore
	keyspace  Keyspace
	gate      *SeqGate
	interval  time.Duration
	minAge    time.Duration
	logEvery  time.Duration
	batch     int
	maxDetail int
	now       func() time.Time

	repair                ProjectionRepairer
	repairLimit           int
	repairPerConversation int
	needsRebuildMessages  int
	needsRebuildRatio     float64
	repairMinMessages     int
	deepAuditInterval     time.Duration
	deepAuditBoundary     int
	deepAuditUsers        int

	runMu sync.Mutex

	mu                sync.Mutex
	status            ReconcileStatus
	logged            map[string]time.Time
	runLogs           int
	runSuppressed     uint64
	directScanCursor  uint64
	groupScanCursor   uint64
	directStatsCursor [2]uint64
	groupStatsCursor  uint64
	missingSeqs       map[reconcileSeqCursorKey]uint64
	reverseSeqs       map[reconcileSeqCursorKey]uint64
	feedCursors       map[uint64]reconcileFeedCursor
	directReadCursor  [2]uint64
	groupReadCursor   [2]uint64
	directDeepCursor  [2]uint64
	groupDeepCursor   uint64
	userDeepCursor    uint64
	// deepSeqs rotates the hourly middle-entity sample of each conversation.
	deepSeqs map[reconcileSeqCursorKey]uint64
	// deepRetries counts consecutive deferrals of one deep-audit row.
	deepRetries     map[reconcileSeqCursorKey]int
	userDeepRetries map[uint64]int
	lastDeepAudit   time.Time
}

// NewReconciler builds a reconciler. Only the client and store are required.
func NewReconciler(
	client redis.UniversalClient,
	dataStore ReconcileStore,
	options ReconcileOptions,
) *Reconciler {
	if options.Interval <= 0 {
		options.Interval = defaultReconcileInterval
	}
	if options.MinAge <= 0 {
		options.MinAge = defaultReconcileMinAge
	}
	if options.Batch <= 0 {
		options.Batch = defaultReconcileBatch
	}
	if options.MaxDetail <= 0 {
		options.MaxDetail = defaultReconcileMaxDetail
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.LogEvery <= 0 {
		options.LogEvery = defaultReconcileLogEvery
	}
	if options.RepairLimit <= 0 {
		options.RepairLimit = defaultRepairLimit
	}
	if options.RepairPerConversation <= 0 {
		options.RepairPerConversation = defaultRepairPerConversation
	}
	if options.NeedsRebuildMessages <= 0 {
		options.NeedsRebuildMessages = defaultNeedsRebuildMessages
	}
	if options.NeedsRebuildRatio <= 0 {
		options.NeedsRebuildRatio = defaultNeedsRebuildRatio
	}
	if options.RepairMinMessages <= 0 {
		options.RepairMinMessages = defaultRepairMinMessages
	}
	if options.DeepAuditInterval <= 0 {
		options.DeepAuditInterval = defaultDeepAuditInterval
	}
	if options.DeepAuditBoundary <= 0 {
		options.DeepAuditBoundary = defaultDeepAuditBoundary
	}
	if options.DeepAuditUsers <= 0 {
		options.DeepAuditUsers = defaultDeepAuditUsers
	}
	return &Reconciler{
		client:                client,
		store:                 dataStore,
		keyspace:              options.Keyspace,
		gate:                  options.Gate,
		interval:              options.Interval,
		minAge:                options.MinAge,
		logEvery:              options.LogEvery,
		batch:                 options.Batch,
		maxDetail:             options.MaxDetail,
		now:                   options.Now,
		repair:                options.Repair,
		repairLimit:           options.RepairLimit,
		repairPerConversation: options.RepairPerConversation,
		needsRebuildMessages:  options.NeedsRebuildMessages,
		needsRebuildRatio:     options.NeedsRebuildRatio,
		repairMinMessages:     options.RepairMinMessages,
		deepAuditInterval:     options.DeepAuditInterval,
		deepAuditBoundary:     options.DeepAuditBoundary,
		deepAuditUsers:        options.DeepAuditUsers,
		logged:                make(map[string]time.Time),
		missingSeqs:           make(map[reconcileSeqCursorKey]uint64),
		reverseSeqs:           make(map[reconcileSeqCursorKey]uint64),
		feedCursors:           make(map[uint64]reconcileFeedCursor),
		deepSeqs:              make(map[reconcileSeqCursorKey]uint64),
		deepRetries:           make(map[reconcileSeqCursorKey]int),
		userDeepRetries:       make(map[uint64]int),
		lastDeepAudit:         options.Now().UTC(),
	}
}

// Run waits for the gate, runs once, then runs every Interval until the context
// ends. Ticks with a closed gate are skipped.
func (r *Reconciler) Run(ctx context.Context) {
	if r == nil || r.client == nil || r.store == nil {
		return
	}
	poll := r.interval
	if poll > reconcileGatePollCeiling {
		poll = reconcileGatePollCeiling
	}
	if poll < reconcileGatePollFloor {
		poll = reconcileGatePollFloor
	}
	for !r.gateReady() {
		if !reconcileWait(ctx, poll) {
			return
		}
	}
	r.runLogged(ctx)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.gateReady() {
				continue
			}
			r.runLogged(ctx)
		}
	}
}

func (r *Reconciler) runLogged(ctx context.Context) {
	if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
		log.Printf("reconcile: %v", err)
	}
}

func (r *Reconciler) gateReady() bool {
	return r.gate == nil || r.gate.IsReady()
}

// RunOnce performs one reconciliation pass. A transient storage error aborts
// the pass with an error; the status keeps the partial counts.
func (r *Reconciler) RunOnce(ctx context.Context) (ReconcileResult, error) {
	if r == nil || r.client == nil || r.store == nil {
		return ReconcileResult{}, ErrMessageServiceUnavailable
	}
	if err := ctx.Err(); err != nil {
		return ReconcileResult{}, err
	}

	r.runMu.Lock()
	defer r.runMu.Unlock()

	start := r.now().UTC()
	r.mu.Lock()
	r.status.Running = true
	r.mu.Unlock()

	result, err := r.reconcile(ctx, start)

	duration := r.now().UTC().Sub(start)
	if duration < 0 {
		duration = 0
	}
	result.Duration = duration
	r.mu.Lock()
	suppressed := r.runSuppressed
	r.mu.Unlock()
	summary := fmt.Sprintf(
		"scanned=%d mismatched=%d repaired=%d unresolved=%d in_flight=%d deferred=%d "+
			"redis_missing=%d redis_repaired=%d counters=%d watermarks=%d feed_repairs=%d needs_rebuild=%d suppressed=%d",
		result.Scanned, result.Mismatched, result.Repaired, result.Unresolved,
		result.InFlight, result.Deferred, result.RedisMissing, result.RedisRepaired,
		result.CountersRaised, result.WatermarksRaised, result.FeedRepairs,
		result.NeedsRebuild, suppressed,
	)

	r.mu.Lock()
	r.status.Running = false
	r.status.LastRunAt = start
	r.status.LastDurationMS = duration.Milliseconds()
	r.status.Scanned = result.Scanned
	r.status.Mismatched = result.Mismatched
	r.status.Repaired = result.Repaired
	r.status.Unresolved = result.Unresolved
	r.status.InFlight = result.InFlight
	r.status.TotalRepaired += result.Repaired
	r.status.RedisMissing = result.RedisMissing
	r.status.RedisRepaired = result.RedisRepaired
	r.status.WatermarksRaised = result.WatermarksRaised
	r.status.FeedRepairs = result.FeedRepairs
	r.status.NeedsRebuild = result.NeedsRebuild
	r.status.LastResultSummary = summary
	// A cancelled context is a shutdown, not a reconciliation failure.
	if err == nil || ctx.Err() != nil {
		r.status.LastError = ""
	} else {
		r.status.LastError = err.Error()
	}
	r.mu.Unlock()

	if result.Mismatched > 0 || result.Repaired > 0 || result.Unresolved > 0 ||
		result.RedisMissing > 0 || result.RedisRepaired > 0 ||
		result.WatermarksRaised > 0 || result.FeedRepairs > 0 ||
		result.NeedsRebuild > 0 || suppressed > 0 {
		log.Printf("reconcile run: %s duration=%s", summary, duration)
	}
	return result, err
}

// Status returns a snapshot safe for concurrent readers.
func (r *Reconciler) Status() ReconcileStatus {
	if r == nil {
		return ReconcileStatus{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

func (r *Reconciler) reconcile(
	ctx context.Context,
	runAt time.Time,
) (ReconcileResult, error) {
	var result ReconcileResult
	r.mu.Lock()
	r.runLogs = 0
	r.runSuppressed = 0
	r.mu.Unlock()
	deepDue := r.deepAuditDue(runAt)
	budget := newReconcileRepairBudget(r.repairLimit)
	if deepDue {
		// Reserve half of the pass for the hourly deep audit so a busy 5-minute
		// reverse-gap repair cannot starve it.
		budget.reserve(r.repairLimit / 2)
	}

	seenPairs := make(map[[2]uint64]struct{})
	if err := r.reconcileDirect(ctx, runAt, &result, seenPairs, budget); err != nil {
		return result, err
	}
	seenGroups := make(map[uint64]struct{})
	if err := r.reconcileGroups(ctx, runAt, &result, seenGroups, budget); err != nil {
		return result, err
	}
	if err := r.reconcileReadWatermarks(ctx, runAt, &result); err != nil {
		return result, err
	}
	if err := r.reconcileOrphanDirectCounters(ctx, runAt, &result, seenPairs); err != nil {
		return result, err
	}
	if err := r.reconcileOrphanGroupCounters(ctx, runAt, &result, seenGroups); err != nil {
		return result, err
	}
	if deepDue {
		auditErr := r.deepAudit(ctx, runAt, &result, budget)
		r.recordDeepAudit(runAt, auditErr)
		if auditErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return result, ctxErr
			}
			log.Printf("reconcile deep audit: %v", auditErr)
		}
	}
	return result, nil
}

type reconcileDirectSuspect struct {
	lowID, highID, counter uint64
	// count and maxSeq are the MySQL stats the counter was compared against.
	// They let the repair skip the already-proven prefix.
	count, maxSeq uint64
}

// reconcileDirect pages the direct conversation stats and repairs at most
// MaxDetail suspects, starting after the previous run's cursor so a fixed set
// of permanently unresolved conversations cannot pin every run.
func (r *Reconciler) reconcileDirect(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	seen map[[2]uint64]struct{},
	budget *reconcileRepairBudget,
) error {
	suspects, err := r.scanDirectSuspects(ctx, runAt, result, seen, budget)
	if err != nil {
		return err
	}
	if len(suspects) == 0 {
		r.setDirectStatsCursor([2]uint64{})
		return nil
	}
	cursor := r.directStatsStart()
	start := 0
	for index, suspect := range suspects {
		if reconcilePairAfter([2]uint64{suspect.lowID, suspect.highID}, cursor) {
			start = index
			break
		}
	}
	attempted := 0
	for offset := 0; offset < len(suspects) && attempted < r.maxDetail; offset++ {
		suspect := suspects[(start+offset)%len(suspects)]
		attempted++
		lowerBound := reconcileSeqLowerBound(suspect.count, suspect.maxSeq, suspect.counter)
		if err := r.repairDirectConversation(ctx, runAt, suspect.lowID, suspect.highID, suspect.counter, lowerBound, result); err != nil {
			return err
		}
		// Advance only after a completed attempt so an aborted run retries it.
		r.setDirectStatsCursor([2]uint64{suspect.lowID, suspect.highID})
	}
	return nil
}

func (r *Reconciler) scanDirectSuspects(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	seen map[[2]uint64]struct{},
	budget *reconcileRepairBudget,
) ([]reconcileDirectSuspect, error) {
	afterLow, afterHigh := uint64(0), uint64(0)
	var suspects []reconcileDirectSuspect
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows, err := r.store.ListConversationSeqStats(ctx, afterLow, afterHigh, r.batch)
		if err != nil {
			return nil, fmt.Errorf("list conversation seq stats: %w", err)
		}
		if len(rows) == 0 {
			return suspects, nil
		}
		states, err := r.readDirectStates(ctx, rows)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row.LowID == 0 || row.HighID == 0 {
				continue
			}
			seen[[2]uint64{row.LowID, row.HighID}] = struct{}{}
			state := states[[2]uint64{row.LowID, row.HighID}]
			result.Scanned++
			// The reverse-direction check runs even when the counter is absent:
			// a missing dm:seq must be raised from MySQL MaxSeq (and a missing
			// index repaired) instead of being skipped as a rebuild concern.
			if err := r.checkDirectRedisGap(ctx, runAt, row, state, budget, result); err != nil {
				return nil, err
			}
			if state.counter == 0 {
				// Just raised from MySQL; there is no Redis-only tail to walk.
				continue
			}
			if state.counter <= row.MaxSeq && row.Count == row.MaxSeq {
				continue
			}
			result.Mismatched++
			suspects = append(suspects, reconcileDirectSuspect{
				lowID: row.LowID, highID: row.HighID, counter: state.counter,
				count: row.Count, maxSeq: row.MaxSeq,
			})
		}
		last := rows[len(rows)-1]
		afterLow, afterHigh = last.LowID, last.HighID
		if len(rows) < r.batch {
			return suspects, nil
		}
	}
}

func reconcilePairAfter(pair, cursor [2]uint64) bool {
	if pair[0] != cursor[0] {
		return pair[0] > cursor[0]
	}
	return pair[1] > cursor[1]
}

// reconcileGroups pages the group stats and repairs at most MaxDetail
// suspects, with its own budget and rotation separate from the direct phase.
func (r *Reconciler) reconcileGroups(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	seen map[uint64]struct{},
	budget *reconcileRepairBudget,
) error {
	suspects, err := r.scanGroupSuspects(ctx, runAt, result, seen, budget)
	if err != nil {
		return err
	}
	if len(suspects) == 0 {
		r.setGroupStatsCursor(0)
		return nil
	}
	cursor := r.groupStatsStart()
	start := 0
	for index, suspect := range suspects {
		if suspect.groupID > cursor {
			start = index
			break
		}
	}
	attempted := 0
	for offset := 0; offset < len(suspects) && attempted < r.maxDetail; offset++ {
		suspect := suspects[(start+offset)%len(suspects)]
		attempted++
		lowerBound := reconcileSeqLowerBound(suspect.count, suspect.maxSeq, suspect.counter)
		if err := r.repairGroupConversation(ctx, runAt, suspect.groupID, suspect.counter, lowerBound, result); err != nil {
			return err
		}
		// Advance only after a completed attempt so an aborted run retries it.
		r.setGroupStatsCursor(suspect.groupID)
	}
	return nil
}

type reconcileGroupSuspect struct {
	groupID, counter uint64
	count, maxSeq    uint64
}

func (r *Reconciler) scanGroupSuspects(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	seen map[uint64]struct{},
	budget *reconcileRepairBudget,
) ([]reconcileGroupSuspect, error) {
	afterGroupID := uint64(0)
	var suspects []reconcileGroupSuspect
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows, err := r.store.ListGroupSeqStats(ctx, afterGroupID, r.batch)
		if err != nil {
			return nil, fmt.Errorf("list group seq stats: %w", err)
		}
		if len(rows) == 0 {
			return suspects, nil
		}
		states, err := r.readGroupStates(ctx, rows)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row.GroupID == 0 {
				continue
			}
			seen[row.GroupID] = struct{}{}
			state := states[row.GroupID]
			result.Scanned++
			if err := r.checkGroupRedisGap(ctx, runAt, row, state, budget, result); err != nil {
				return nil, err
			}
			if state.counter == 0 {
				// Just raised from MySQL; there is no Redis-only tail to walk.
				continue
			}
			if state.counter <= row.MaxSeq && row.Count == row.MaxSeq {
				continue
			}
			result.Mismatched++
			suspects = append(suspects, reconcileGroupSuspect{
				groupID: row.GroupID, counter: state.counter,
				count: row.Count, maxSeq: row.MaxSeq,
			})
		}
		afterGroupID = rows[len(rows)-1].GroupID
		if len(rows) < r.batch {
			return suspects, nil
		}
	}
}

// reconcileRedisState is one conversation's Redis counter and index size, read
// in the same pipeline as the MySQL stats row.
type reconcileRedisState struct {
	counter uint64
	index   uint64
}

func (r *Reconciler) readDirectStates(
	ctx context.Context,
	rows []store.ConversationSeqStat,
) (map[[2]uint64]reconcileRedisState, error) {
	pipe := r.client.Pipeline()
	counters := make([]*redis.StringCmd, len(rows))
	indexes := make([]*redis.IntCmd, len(rows))
	for index, row := range rows {
		counters[index] = pipe.Get(ctx, r.keyspace.directSequenceKey(row.LowID, row.HighID))
		indexes[index] = pipe.ZCard(ctx, r.keyspace.directPairKey(row.LowID, row.HighID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read conversation seq counters: %w", err)
	}
	states := make(map[[2]uint64]reconcileRedisState, len(rows))
	for index, row := range rows {
		counter, err := reconcileCounterValue(counters[index])
		if err != nil {
			return nil, fmt.Errorf("read conversation seq counter: %w", err)
		}
		size, err := indexes[index].Result()
		if err != nil {
			return nil, fmt.Errorf("read conversation index size: %w", err)
		}
		states[[2]uint64{row.LowID, row.HighID}] = reconcileRedisState{
			counter: counter,
			index:   uint64(size),
		}
	}
	return states, nil
}

func (r *Reconciler) readGroupStates(
	ctx context.Context,
	rows []store.GroupSeqStat,
) (map[uint64]reconcileRedisState, error) {
	pipe := r.client.Pipeline()
	counters := make([]*redis.StringCmd, len(rows))
	indexes := make([]*redis.IntCmd, len(rows))
	for index, row := range rows {
		counters[index] = pipe.Get(ctx, r.keyspace.groupSequenceKey(row.GroupID))
		indexes[index] = pipe.ZCard(ctx, r.keyspace.groupInboxKey(row.GroupID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read group seq counters: %w", err)
	}
	states := make(map[uint64]reconcileRedisState, len(rows))
	for index, row := range rows {
		counter, err := reconcileCounterValue(counters[index])
		if err != nil {
			return nil, fmt.Errorf("read group seq counter: %w", err)
		}
		size, err := indexes[index].Result()
		if err != nil {
			return nil, fmt.Errorf("read group index size: %w", err)
		}
		states[row.GroupID] = reconcileRedisState{counter: counter, index: uint64(size)}
	}
	return states, nil
}

func reconcileCounterValue(command *redis.StringCmd) (uint64, error) {
	raw, err := command.Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(raw, 10, 64)
}

// reconcileSeqLowerBound returns the first seq that can be missing in MySQL.
// Seqs start at 1 and are unique per conversation, so COUNT(*) == MAX(seq)
// proves every seq in 1..MaxSeq is persisted and only the Redis tail above
// MaxSeq can be missing. Any other count means the prefix is not proven: at
// least one seq below MaxSeq is absent, or the counters disagree, so the walk
// must start at the beginning. The stats row and the Redis counter are read at
// different instants, but that drift is safe: the repair never deletes rows
// and new accepts only extend the tail, so the prefix proven by the snapshot
// stays proven. An external deletion after the snapshot shifts COUNT(*) away
// from MAX(seq) and the next run scans the prefix again. Tail seqs accepted
// after the snapshot are above MaxSeq and are exactly what the walk from
// MaxSeq+1 covers.
func reconcileSeqLowerBound(count, maxSeq, counter uint64) uint64 {
	if counter > maxSeq && count == maxSeq {
		return maxSeq + 1
	}
	return 1
}

func (r *Reconciler) repairDirectConversation(
	ctx context.Context,
	runAt time.Time,
	lowID, highID, counter, lowerBound uint64,
	result *ReconcileResult,
) error {
	if counter > store.MaxSafeSeq {
		// SeqCalibrator only raises counters, so an out-of-range one is
		// untrusted; walking it would mean billions of lookups.
		r.logUnresolved(runAt, fmt.Sprintf("direct:%d:%d:counter", lowID, highID),
			"sequence counter exceeds MaxSafeSeq")
		result.Unresolved++
		return nil
	}
	key := reconcileSeqCursorKey{first: lowID, second: highID}
	start := r.missingSeqStart(key, counter, lowerBound)
	next, truncated, err := walkMissingSeqs(
		ctx, start, counter, uint64(r.batch), reconcileMaxMissingPerConversation,
		func(ctx context.Context, afterSeq uint64) ([]uint64, error) {
			return r.store.ListConversationSeqs(ctx, lowID, highID, counter, afterSeq, r.batch)
		},
		func(seq uint64) error {
			outcome, err := r.repairDirectSeq(ctx, runAt, lowID, highID, seq)
			if err != nil {
				return err
			}
			result.add(outcome)
			return nil
		},
	)
	if err != nil {
		return err
	}
	r.setMissingSeqCursor(key, next, truncated)
	if truncated {
		result.Deferred++
	}
	return nil
}

func (r *Reconciler) repairGroupConversation(
	ctx context.Context,
	runAt time.Time,
	groupID, counter, lowerBound uint64,
	result *ReconcileResult,
) error {
	if counter > store.MaxSafeSeq {
		r.logUnresolved(runAt, fmt.Sprintf("group:%d:counter", groupID),
			"sequence counter exceeds MaxSafeSeq")
		result.Unresolved++
		return nil
	}
	key := reconcileSeqCursorKey{group: true, first: groupID}
	start := r.missingSeqStart(key, counter, lowerBound)
	next, truncated, err := walkMissingSeqs(
		ctx, start, counter, uint64(r.batch), reconcileMaxMissingPerConversation,
		func(ctx context.Context, afterSeq uint64) ([]uint64, error) {
			return r.store.ListGroupSeqs(ctx, groupID, counter, afterSeq, r.batch)
		},
		func(seq uint64) error {
			outcome, err := r.repairGroupSeq(ctx, runAt, groupID, seq)
			if err != nil {
				return err
			}
			result.add(outcome)
			return nil
		},
	)
	if err != nil {
		return err
	}
	r.setMissingSeqCursor(key, next, truncated)
	if truncated {
		result.Deferred++
	}
	return nil
}

type reconcileOutcome struct {
	repaired   bool
	unresolved bool
	inFlight   bool
}

func (r *ReconcileResult) add(outcome reconcileOutcome) {
	switch {
	case outcome.repaired:
		r.Repaired++
	case outcome.unresolved:
		r.Unresolved++
	case outcome.inFlight:
		r.InFlight++
	}
}

func (r *Reconciler) repairDirectSeq(
	ctx context.Context,
	runAt time.Time,
	lowID, highID, seq uint64,
) (reconcileOutcome, error) {
	logKey := fmt.Sprintf("direct:%d:%d:%d", lowID, highID, seq)
	messageID, found, err := r.indexMember(ctx, r.keyspace.directPairKey(lowID, highID), seq)
	if err != nil {
		return reconcileOutcome{}, err
	}
	if !found {
		r.logUnresolved(runAt, logKey, "index entry missing")
		return reconcileOutcome{unresolved: true}, nil
	}
	fields, err := r.entityFields(ctx, r.keyspace.messageKey(messageID))
	if err != nil {
		return reconcileOutcome{}, err
	}
	if len(fields) == 0 {
		r.logUnresolved(runAt, logKey, "entity missing")
		return reconcileOutcome{unresolved: true}, nil
	}
	message, err := reconcileDirectMessage(messageID, lowID, highID, seq, fields)
	if err != nil {
		r.logUnresolved(runAt, logKey, err.Error())
		return reconcileOutcome{unresolved: true}, nil
	}
	if r.youngerThanMinAge(message.CreatedAt, runAt) {
		return reconcileOutcome{inFlight: true}, nil
	}
	if _, err := r.store.PersistMessage(ctx, message); err != nil {
		return r.persistFailure(ctx, runAt, logKey, err)
	}
	return reconcileOutcome{repaired: true}, nil
}

func (r *Reconciler) repairGroupSeq(
	ctx context.Context,
	runAt time.Time,
	groupID, seq uint64,
) (reconcileOutcome, error) {
	logKey := fmt.Sprintf("group:%d:%d", groupID, seq)
	messageID, found, err := r.indexMember(ctx, r.keyspace.groupInboxKey(groupID), seq)
	if err != nil {
		return reconcileOutcome{}, err
	}
	if !found {
		r.logUnresolved(runAt, logKey, "index entry missing")
		return reconcileOutcome{unresolved: true}, nil
	}
	fields, err := r.entityFields(ctx, r.keyspace.groupMessageKey(messageID))
	if err != nil {
		return reconcileOutcome{}, err
	}
	if len(fields) == 0 {
		r.logUnresolved(runAt, logKey, "entity missing")
		return reconcileOutcome{unresolved: true}, nil
	}
	message, err := reconcileGroupMessage(messageID, groupID, seq, fields)
	if err != nil {
		r.logUnresolved(runAt, logKey, err.Error())
		return reconcileOutcome{unresolved: true}, nil
	}
	if r.youngerThanMinAge(message.CreatedAt, runAt) {
		return reconcileOutcome{inFlight: true}, nil
	}
	if _, err := r.store.PersistGroupMessage(ctx, message); err != nil {
		return r.persistFailure(ctx, runAt, logKey, err)
	}
	return reconcileOutcome{repaired: true}, nil
}

// persistFailure turns a failed repair into either an unresolved item or an
// aborted run. Unknown errors are permanent here: retrying them inside a run
// would only repeat the same failure.
func (r *Reconciler) persistFailure(
	ctx context.Context,
	runAt time.Time,
	logKey string,
	err error,
) (reconcileOutcome, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return reconcileOutcome{}, ctxErr
	}
	if classifyPersistError(err) == persistErrorTransient {
		return reconcileOutcome{}, fmt.Errorf("persist reconciled message %s: %w", logKey, err)
	}
	r.logUnresolved(runAt, logKey, "persist failed: "+err.Error())
	return reconcileOutcome{unresolved: true}, nil
}

// walkMissingSeqs emits the seqs in start..upTo absent from the ascending
// pages returned by fetch. It never holds more than one page, stops after
// maxMissing emissions, and returns the seq to resume from when it stopped
// early so a damaged conversation cannot monopolize a run.
func walkMissingSeqs(
	ctx context.Context,
	start, upTo, pageSize uint64,
	maxMissing int,
	fetch func(ctx context.Context, afterSeq uint64) ([]uint64, error),
	emit func(seq uint64) error,
) (next uint64, truncated bool, err error) {
	if start < 1 {
		start = 1
	}
	if start > upTo {
		return 0, false, nil
	}
	expect := start
	afterSeq := start - 1
	emitted := 0
	for {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		page, err := fetch(ctx, afterSeq)
		if err != nil {
			return 0, false, err
		}
		for _, seq := range page {
			if seq < expect {
				continue
			}
			for expect < seq {
				if emitted >= maxMissing {
					return expect, true, nil
				}
				if err := emit(expect); err != nil {
					return 0, false, err
				}
				emitted++
				expect++
			}
			expect = seq + 1
		}
		if pageSize == 0 || uint64(len(page)) < pageSize {
			break
		}
		last := page[len(page)-1]
		if last >= upTo || last <= afterSeq {
			break
		}
		afterSeq = last
	}
	for expect <= upTo {
		if emitted >= maxMissing {
			return expect, true, nil
		}
		if err := emit(expect); err != nil {
			return 0, false, err
		}
		emitted++
		expect++
	}
	return 0, false, nil
}

// reconcileSeqCursorKey identifies one conversation's resumable gap walk.
type reconcileSeqCursorKey struct {
	group  bool
	first  uint64
	second uint64
}

// missingSeqStart combines the resumed cursor with the lower bound proven by
// the MySQL stats. A cursor below the bound (from a run where the prefix was
// not proven) is ignored so the walk does not rescan it.
func (r *Reconciler) missingSeqStart(key reconcileSeqCursorKey, counter, lowerBound uint64) uint64 {
	if lowerBound < 1 {
		lowerBound = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	start := r.missingSeqs[key]
	if start < lowerBound {
		start = lowerBound
	}
	if start == 0 || start > counter {
		return lowerBound
	}
	return start
}

func (r *Reconciler) setMissingSeqCursor(key reconcileSeqCursorKey, next uint64, truncated bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !truncated {
		delete(r.missingSeqs, key)
		return
	}
	if next == 0 {
		next = 1
	}
	if _, exists := r.missingSeqs[key]; !exists && len(r.missingSeqs) >= reconcileLogKeyLimit {
		// Bounded state: dropping the cursor only restarts the walk at its
		// proven lower bound.
		return
	}
	r.missingSeqs[key] = next
}

func (r *Reconciler) indexMember(
	ctx context.Context,
	key string,
	seq uint64,
) (uint64, bool, error) {
	score := strconv.FormatUint(seq, 10)
	members, err := r.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min:   score,
		Max:   score,
		Count: 1,
	}).Result()
	if err != nil {
		return 0, false, fmt.Errorf("read conversation index: %w", err)
	}
	if len(members) == 0 {
		return 0, false, nil
	}
	messageID, err := strconv.ParseUint(members[0], 10, 64)
	if err != nil || messageID == 0 {
		return 0, false, fmt.Errorf("read conversation index: invalid member")
	}
	return messageID, true, nil
}

func (r *Reconciler) entityFields(
	ctx context.Context,
	key string,
) (map[string]string, error) {
	fields, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("read message entity: %w", err)
	}
	return fields, nil
}

func (r *Reconciler) youngerThanMinAge(createdAt, runAt time.Time) bool {
	return createdAt.After(runAt.Add(-r.minAge))
}

// reconcileDirectMessage rebuilds a store.Message from its entity hash.
func reconcileDirectMessage(
	messageID, lowID, highID, seq uint64,
	fields map[string]string,
) (store.Message, error) {
	id, err := reconcileEntityUint(fields, "id")
	if err != nil {
		return store.Message{}, err
	}
	entitySeq, err := reconcileEntityUint(fields, "seq")
	if err != nil {
		return store.Message{}, err
	}
	senderID, err := reconcileEntityUint(fields, "sender_id")
	if err != nil {
		return store.Message{}, err
	}
	recipientID, err := reconcileEntityUint(fields, "recipient_id")
	if err != nil {
		return store.Message{}, err
	}
	clientMessageID := strings.TrimSpace(fields["client_message_id"])
	body := fields["body"]
	if id != messageID || entitySeq != seq {
		return store.Message{}, errors.New("entity identity differs from the index")
	}
	// An empty body would persist a wrong row that dead-letters the real one.
	if senderID == 0 || recipientID == 0 || clientMessageID == "" ||
		strings.TrimSpace(body) == "" {
		return store.Message{}, errors.New(reconcileIncompleteEntityMsg)
	}
	pairLow, pairHigh := senderID, recipientID
	if pairLow > pairHigh {
		pairLow, pairHigh = pairHigh, pairLow
	}
	if pairLow != lowID || pairHigh != highID {
		return store.Message{}, errors.New("entity belongs to another conversation")
	}
	createdAtMS, err := reconcileEntityInt64(fields, "created_at_ms")
	if err != nil {
		return store.Message{}, err
	}
	return store.Message{
		ID:              messageID,
		Seq:             seq,
		SenderID:        senderID,
		RecipientID:     recipientID,
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       time.UnixMilli(createdAtMS).UTC(),
	}, nil
}

// reconcileGroupMessage rebuilds a store.GroupMessage from its entity hash,
// including the sender snapshot the entity was accepted with.
func reconcileGroupMessage(
	messageID, groupID, seq uint64,
	fields map[string]string,
) (store.GroupMessage, error) {
	id, err := reconcileEntityUint(fields, "id")
	if err != nil {
		return store.GroupMessage{}, err
	}
	entitySeq, err := reconcileEntityUint(fields, "seq")
	if err != nil {
		return store.GroupMessage{}, err
	}
	entityGroupID, err := reconcileEntityUint(fields, "group_id")
	if err != nil {
		return store.GroupMessage{}, err
	}
	senderID, err := reconcileEntityUint(fields, "sender_id")
	if err != nil {
		return store.GroupMessage{}, err
	}
	clientMessageID := strings.TrimSpace(fields["client_message_id"])
	body := fields["body"]
	senderUsername := strings.TrimSpace(fields["sender_username"])
	senderNickname := strings.TrimSpace(fields["sender_nickname"])
	if id != messageID || entitySeq != seq || entityGroupID != groupID {
		return store.GroupMessage{}, errors.New("entity identity differs from the index")
	}
	if senderID == 0 || clientMessageID == "" || strings.TrimSpace(body) == "" ||
		senderUsername == "" || senderNickname == "" {
		return store.GroupMessage{}, errors.New(reconcileIncompleteEntityMsg)
	}
	createdAtMS, err := reconcileEntityInt64(fields, "created_at_ms")
	if err != nil {
		return store.GroupMessage{}, err
	}
	return store.GroupMessage{
		ID:       messageID,
		Seq:      seq,
		GroupID:  groupID,
		SenderID: senderID,
		Sender: store.PublicUser{
			Username: senderUsername,
			Nickname: senderNickname,
		},
		ClientMessageID: clientMessageID,
		Body:            body,
		CreatedAt:       time.UnixMilli(createdAtMS).UTC(),
	}, nil
}

func reconcileEntityUint(fields map[string]string, key string) (uint64, error) {
	raw := strings.TrimSpace(fields[key])
	if raw == "" {
		return 0, fmt.Errorf("entity missing %s", key)
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("entity %s is invalid", key)
	}
	return value, nil
}

func reconcileEntityInt64(fields map[string]string, key string) (int64, error) {
	raw := strings.TrimSpace(fields[key])
	if raw == "" {
		return 0, fmt.Errorf("entity missing %s", key)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("entity %s is invalid", key)
	}
	return value, nil
}

// reconcileOrphanDirectCounters finds counters that have no MySQL rows at all.
// The SCAN is bounded and resumes across runs so a large keyspace is still
// covered without one run scanning everything.
func (r *Reconciler) reconcileOrphanDirectCounters(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	seen map[[2]uint64]struct{},
) error {
	cursor, keys, err := r.scanCounterKeys(
		ctx, r.keyspace.reconcileDirectCounterScanPattern(), r.directCursor(),
	)
	if err != nil {
		return err
	}
	defer r.setDirectCursor(cursor)
	if len(keys) == 0 {
		return nil
	}
	pipe := r.client.Pipeline()
	commands := make([]*redis.StringCmd, len(keys))
	for index, key := range keys {
		commands[index] = pipe.Get(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("read scanned conversation counters: %w", err)
	}
	attempted := 0
	for index, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		lowID, highID, ok := r.keyspace.reconcileParseDirectCounterKey(key)
		if !ok {
			continue
		}
		if _, exists := seen[[2]uint64{lowID, highID}]; exists {
			continue
		}
		counter, err := reconcileCounterValue(commands[index])
		if err != nil {
			return fmt.Errorf("read scanned conversation counter: %w", err)
		}
		if counter == 0 {
			continue
		}
		result.Scanned++
		result.Mismatched++
		if attempted >= r.maxDetail {
			continue
		}
		attempted++
		if err := r.repairDirectConversation(ctx, runAt, lowID, highID, counter, 1, result); err != nil {
			return err
		}
	}
	return nil
}

// reconcileOrphanGroupCounters is the group equivalent of
// reconcileOrphanDirectCounters.
func (r *Reconciler) reconcileOrphanGroupCounters(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	seen map[uint64]struct{},
) error {
	cursor, keys, err := r.scanCounterKeys(
		ctx, r.keyspace.reconcileGroupCounterScanPattern(), r.groupCursor(),
	)
	if err != nil {
		return err
	}
	defer r.setGroupCursor(cursor)
	if len(keys) == 0 {
		return nil
	}
	pipe := r.client.Pipeline()
	commands := make([]*redis.StringCmd, len(keys))
	for index, key := range keys {
		commands[index] = pipe.Get(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("read scanned group counters: %w", err)
	}
	attempted := 0
	for index, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		groupID, ok := r.keyspace.reconcileParseGroupCounterKey(key)
		if !ok {
			continue
		}
		if _, exists := seen[groupID]; exists {
			continue
		}
		counter, err := reconcileCounterValue(commands[index])
		if err != nil {
			return fmt.Errorf("read scanned group counter: %w", err)
		}
		if counter == 0 {
			continue
		}
		result.Scanned++
		result.Mismatched++
		if attempted >= r.maxDetail {
			continue
		}
		attempted++
		if err := r.repairGroupConversation(ctx, runAt, groupID, counter, 1, result); err != nil {
			return err
		}
	}
	return nil
}

// scanCounterKeys walks at most reconcileScanKeysPerRun keys from cursor and
// returns the cursor to resume from.
func (r *Reconciler) scanCounterKeys(
	ctx context.Context,
	pattern string,
	cursor uint64,
) (uint64, []string, error) {
	keys := make([]string, 0, reconcileScanCountHint)
	current := cursor
	for len(keys) < reconcileScanKeysPerRun {
		batch, next, err := r.client.Scan(ctx, current, pattern, reconcileScanCountHint).Result()
		if err != nil {
			return cursor, nil, fmt.Errorf("scan conversation counters: %w", err)
		}
		keys = append(keys, batch...)
		current = next
		if current == 0 {
			break
		}
	}
	return current, keys, nil
}

func (r *Reconciler) directCursor() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.directScanCursor
}

func (r *Reconciler) setDirectCursor(cursor uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.directScanCursor = cursor
}

func (r *Reconciler) groupCursor() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.groupScanCursor
}

func (r *Reconciler) setGroupCursor(cursor uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groupScanCursor = cursor
}

func (r *Reconciler) directStatsStart() [2]uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.directStatsCursor
}

func (r *Reconciler) setDirectStatsCursor(cursor [2]uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.directStatsCursor = cursor
}

func (r *Reconciler) groupStatsStart() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.groupStatsCursor
}

func (r *Reconciler) setGroupStatsCursor(cursor uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groupStatsCursor = cursor
}

// logUnresolved logs one unresolved item at most once per LogEvery. Items are
// suppressed rather than evicted when either the per-run cap or the key map is
// full, so a burst cannot break the per-item throttle.
func (r *Reconciler) logUnresolved(runAt time.Time, key, reason string) {
	r.logThrottled(runAt, "unresolved", key, reason)
}

// logThrottled logs one repair or unresolved item at most once per LogEvery.
// The throttle identity includes the event kind so a needs-rebuild warning and
// a repair error for the same subject never mask each other.
func (r *Reconciler) logThrottled(runAt time.Time, kind, key, reason string) {
	throttleKey := kind + "\x00" + key
	r.mu.Lock()
	if when, ok := r.logged[throttleKey]; ok && runAt.Sub(when) < r.logEvery {
		r.mu.Unlock()
		return
	}
	if r.runLogs >= reconcileMaxLogsPerRun {
		r.runSuppressed++
		r.mu.Unlock()
		return
	}
	if len(r.logged) >= reconcileLogKeyLimit {
		r.expireLoggedLocked(runAt)
		if len(r.logged) >= reconcileLogKeyLimit {
			r.runSuppressed++
			r.mu.Unlock()
			return
		}
	}
	r.runLogs++
	r.logged[throttleKey] = runAt
	r.mu.Unlock()
	log.Printf("reconcile %s %s: %s", kind, key, reason)
}

// expireLoggedLocked drops only entries whose LogEvery window elapsed. Fresh
// entries stay, which keeps the once-per-item guarantee under load.
func (r *Reconciler) expireLoggedLocked(now time.Time) {
	for key, when := range r.logged {
		if now.Sub(when) >= r.logEvery {
			delete(r.logged, key)
		}
	}
}

// reconcileRepairBudget bounds how many Redis messages one run writes across
// the 5-minute repair pass and the deep audit. reserved is a slice of limit the
// 5-minute pass may not spend, so the hourly deep audit always has room.
type reconcileRepairBudget struct {
	limit    int
	used     int
	reserved int
}

func newReconcileRepairBudget(limit int) *reconcileRepairBudget {
	return &reconcileRepairBudget{limit: limit}
}

// reserve sets aside a slice of the budget for the deep audit.
func (b *reconcileRepairBudget) reserve(count int) {
	if b == nil || count <= 0 {
		return
	}
	if count > b.limit {
		count = b.limit
	}
	b.reserved = count
}

// remaining is what any phase may still spend, including the deep audit.
func (b *reconcileRepairBudget) remaining() int {
	if b == nil || b.used >= b.limit {
		return 0
	}
	return b.limit - b.used
}

// quickRemaining is what the 5-minute pass may spend without touching the slice
// reserved for the deep audit.
func (b *reconcileRepairBudget) quickRemaining() int {
	if b == nil {
		return 0
	}
	usable := b.limit - b.reserved
	if usable <= 0 || b.used >= usable {
		return 0
	}
	return usable - b.used
}

func (b *reconcileRepairBudget) spend(count int) {
	if b == nil || count <= 0 {
		return
	}
	b.used += count
}

// conversationNeedsRebuild applies the two automatic-repair caps: an absolute
// gap and a ratio with an absolute floor, so a nearly empty conversation with
// one missing message is still repaired.
func (r *Reconciler) conversationNeedsRebuild(gap, count uint64) bool {
	if gap == 0 {
		return false
	}
	if gap > uint64(r.needsRebuildMessages) {
		return true
	}
	return float64(gap) > r.needsRebuildRatio*float64(count) &&
		gap >= uint64(r.repairMinMessages)
}

// checkDirectRedisGap compares one conversation against MySQL in the reverse
// direction. It raises a lagging counter and repairs a small index gap; a gap
// above the caps is reported as needing a rebuild instead.
func (r *Reconciler) checkDirectRedisGap(
	ctx context.Context,
	runAt time.Time,
	row store.ConversationSeqStat,
	state reconcileRedisState,
	budget *reconcileRepairBudget,
	result *ReconcileResult,
) error {
	if state.counter < row.MaxSeq {
		// A lagging counter would reissue seqs that MySQL already holds.
		result.RedisMissing++
		if r.repair != nil {
			if err := r.repair.RaiseDirectCounter(ctx, row.LowID, row.HighID, row.MaxSeq); err != nil {
				r.logThrottled(runAt, "repair-error",
					fmt.Sprintf("direct:%d:%d:counter", row.LowID, row.HighID),
					"raise counter: "+err.Error())
			} else {
				result.RedisRepaired++
				result.CountersRaised++
			}
		}
	}
	if state.index >= row.Count {
		return nil
	}
	gap := row.Count - state.index
	result.RedisMissing += gap
	if r.conversationNeedsRebuild(gap, row.Count) {
		result.NeedsRebuild++
		r.logThrottled(runAt, "needs-rebuild",
			fmt.Sprintf("direct:%d:%d:index", row.LowID, row.HighID),
			fmt.Sprintf("redis index gap %d of %d exceeds repair budget", gap, row.Count))
		return nil
	}
	return r.repairDirectIndexGap(ctx, runAt, row, budget, result)
}

func (r *Reconciler) checkGroupRedisGap(
	ctx context.Context,
	runAt time.Time,
	row store.GroupSeqStat,
	state reconcileRedisState,
	budget *reconcileRepairBudget,
	result *ReconcileResult,
) error {
	if state.counter < row.MaxSeq {
		result.RedisMissing++
		if r.repair != nil {
			if err := r.repair.RaiseGroupCounter(ctx, row.GroupID, row.MaxSeq); err != nil {
				r.logThrottled(runAt, "repair-error",
					fmt.Sprintf("group:%d:counter", row.GroupID),
					"raise counter: "+err.Error())
			} else {
				result.RedisRepaired++
				result.CountersRaised++
			}
		}
	}
	if state.index >= row.Count {
		return nil
	}
	gap := row.Count - state.index
	result.RedisMissing += gap
	if r.conversationNeedsRebuild(gap, row.Count) {
		result.NeedsRebuild++
		r.logThrottled(runAt, "needs-rebuild",
			fmt.Sprintf("group:%d:index", row.GroupID),
			fmt.Sprintf("redis inbox gap %d of %d exceeds repair budget", gap, row.Count))
		return nil
	}
	return r.repairGroupIndexGap(ctx, runAt, row, budget, result)
}

// repairDirectIndexGap finds messages absent from dm:pair (or carrying a stale
// score) in ascending seq order and writes at most one conversation's share of
// the per-run budget. The scan resumes from a cursor when it hits its own cap.
func (r *Reconciler) repairDirectIndexGap(
	ctx context.Context,
	runAt time.Time,
	row store.ConversationSeqStat,
	budget *reconcileRepairBudget,
	result *ReconcileResult,
) error {
	capacity := r.repairPerConversation
	if r.repair != nil {
		if remaining := budget.quickRemaining(); remaining < capacity {
			capacity = remaining
		}
	}
	if capacity <= 0 {
		return nil
	}
	key := reconcileSeqCursorKey{first: row.LowID, second: row.HighID}
	afterSeq := r.reverseSeqStart(key)
	missing := make([]store.Message, 0, capacity)
	scanned := 0
	completed := false
	for len(missing) < capacity && scanned < reconcileMaxReverseScan {
		pageSize := r.batch
		if remaining := reconcileMaxReverseScan - scanned; pageSize > remaining {
			pageSize = remaining
		}
		page, err := r.store.ListMessagesBySeqRange(ctx, row.LowID, row.HighID, afterSeq, pageSize)
		if err != nil {
			return fmt.Errorf("list messages by seq range: %w", err)
		}
		if len(page) == 0 {
			completed = true
			break
		}
		present, err := r.directIndexScores(ctx, row.LowID, row.HighID, page)
		if err != nil {
			return err
		}
		for index, message := range page {
			afterSeq = message.Seq
			if present[index] {
				continue
			}
			missing = append(missing, message)
			if len(missing) >= capacity {
				break
			}
		}
		scanned += len(page)
		if len(page) < pageSize {
			completed = true
			break
		}
	}
	if completed {
		afterSeq = 0
	}
	r.setReverseSeqCursor(key, afterSeq, !completed)
	return r.writeDirectRepairs(ctx, runAt, key, missing, budget, result, fmt.Sprintf("direct:%d:%d", row.LowID, row.HighID))
}

func (r *Reconciler) repairGroupIndexGap(
	ctx context.Context,
	runAt time.Time,
	row store.GroupSeqStat,
	budget *reconcileRepairBudget,
	result *ReconcileResult,
) error {
	capacity := r.repairPerConversation
	if r.repair != nil {
		if remaining := budget.quickRemaining(); remaining < capacity {
			capacity = remaining
		}
	}
	if capacity <= 0 {
		return nil
	}
	key := reconcileSeqCursorKey{group: true, first: row.GroupID}
	afterSeq := r.reverseSeqStart(key)
	missing := make([]store.GroupMessage, 0, capacity)
	scanned := 0
	completed := false
	for len(missing) < capacity && scanned < reconcileMaxReverseScan {
		pageSize := r.batch
		if remaining := reconcileMaxReverseScan - scanned; pageSize > remaining {
			pageSize = remaining
		}
		page, err := r.store.ListGroupMessagesBySeqRange(ctx, row.GroupID, afterSeq, pageSize)
		if err != nil {
			return fmt.Errorf("list group messages by seq range: %w", err)
		}
		if len(page) == 0 {
			completed = true
			break
		}
		present, err := r.groupIndexScores(ctx, row.GroupID, page)
		if err != nil {
			return err
		}
		for index, message := range page {
			afterSeq = message.Seq
			if present[index] {
				continue
			}
			missing = append(missing, message)
			if len(missing) >= capacity {
				break
			}
		}
		scanned += len(page)
		if len(page) < pageSize {
			completed = true
			break
		}
	}
	if completed {
		afterSeq = 0
	}
	r.setReverseSeqCursor(key, afterSeq, !completed)
	return r.writeGroupRepairs(ctx, runAt, key, missing, budget, result, fmt.Sprintf("group:%d", row.GroupID))
}

func (r *Reconciler) directIndexScores(
	ctx context.Context,
	lowID, highID uint64,
	messages []store.Message,
) ([]bool, error) {
	pipe := r.client.Pipeline()
	key := r.keyspace.directPairKey(lowID, highID)
	commands := make([]*redis.FloatCmd, len(messages))
	for index, message := range messages {
		commands[index] = pipe.ZScore(ctx, key, strconv.FormatUint(message.ID, 10))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read conversation index scores: %w", err)
	}
	present := make([]bool, len(messages))
	for index, command := range commands {
		score, err := command.Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read conversation index score: %w", err)
		}
		present[index] = score == float64(messages[index].Seq)
	}
	return present, nil
}

func (r *Reconciler) groupIndexScores(
	ctx context.Context,
	groupID uint64,
	messages []store.GroupMessage,
) ([]bool, error) {
	pipe := r.client.Pipeline()
	key := r.keyspace.groupInboxKey(groupID)
	commands := make([]*redis.FloatCmd, len(messages))
	for index, message := range messages {
		commands[index] = pipe.ZScore(ctx, key, strconv.FormatUint(message.ID, 10))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read group inbox scores: %w", err)
	}
	present := make([]bool, len(messages))
	for index, command := range commands {
		score, err := command.Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read group inbox score: %w", err)
		}
		present[index] = score == float64(messages[index].Seq)
	}
	return present, nil
}

func (r *Reconciler) writeDirectRepairs(
	ctx context.Context,
	runAt time.Time,
	cursorKey reconcileSeqCursorKey,
	messages []store.Message,
	budget *reconcileRepairBudget,
	result *ReconcileResult,
	key string,
) error {
	if len(messages) == 0 || r.repair == nil {
		return nil
	}
	// The reverse path must not overwrite an entity that already exists: only
	// an absent entity is fully projected from MySQL. A present entity is
	// compared with its MySQL row; a match gets an index-only repair (or a
	// byte-identical rewrite when the repairer has no index-only method) and a
	// divergence is reported instead of being overwritten.
	fields, err := r.directAuditEntityFields(ctx, messages)
	if err != nil {
		return err
	}
	absent := make([]store.Message, 0, len(messages))
	matching := make([]store.Message, 0, len(messages))
	diverged := false
	for index, message := range messages {
		if len(fields[index]) == 0 {
			absent = append(absent, message)
			continue
		}
		if !directEntityMatches(message, fields[index]) {
			diverged = true
			continue
		}
		matching = append(matching, message)
	}
	if diverged {
		result.NeedsRebuild++
		r.logThrottled(runAt, "needs-rebuild", key+":entity", "redis entity differs from MySQL")
	}
	written := 0
	if len(absent) > 0 {
		count, err := r.repair.WriteDirectBatch(ctx, absent)
		if err != nil {
			r.logThrottled(runAt, "repair-error", key+":index", "write missing messages: "+err.Error())
			r.setReverseSeqCursor(cursorKey, 0, false)
			return nil
		}
		written += count
	}
	if len(matching) > 0 {
		count, err := r.repairDirectIndexOnly(ctx, matching)
		if err != nil {
			r.logThrottled(runAt, "repair-error", key+":index", "write missing index entries: "+err.Error())
			r.setReverseSeqCursor(cursorKey, 0, false)
			return nil
		}
		written += count
	}
	result.RedisRepaired += uint64(written)
	budget.spend(written)
	return nil
}

// repairDirectIndexOnly restores the pair and feed entries of entities that are
// already present and equal to MySQL. It falls back to a full write, which
// converges the entity to the same bytes, only when the repairer offers no
// index-only method.
func (r *Reconciler) repairDirectIndexOnly(
	ctx context.Context,
	messages []store.Message,
) (int, error) {
	if indexer, ok := r.repair.(ProjectionIndexRepairer); ok {
		return indexer.WriteDirectIndexBatch(ctx, messages)
	}
	return r.repair.WriteDirectBatch(ctx, messages)
}

func (r *Reconciler) repairGroupIndexOnly(
	ctx context.Context,
	messages []store.GroupMessage,
) (int, error) {
	if indexer, ok := r.repair.(ProjectionIndexRepairer); ok {
		return indexer.WriteGroupIndexBatch(ctx, messages)
	}
	return r.repair.WriteGroupBatch(ctx, messages)
}

// directAuditEntityFields reads the entities of the given messages so the repair
// can tell absent from present, and present-equal from diverged.
func (r *Reconciler) directAuditEntityFields(
	ctx context.Context,
	messages []store.Message,
) ([]map[string]string, error) {
	pipe := r.client.Pipeline()
	commands := make([]*redis.MapStringStringCmd, len(messages))
	for index, message := range messages {
		commands[index] = pipe.HGetAll(ctx, r.keyspace.messageKey(message.ID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read direct entities for repair: %w", err)
	}
	fields := make([]map[string]string, len(messages))
	for index, command := range commands {
		value, err := command.Result()
		if err != nil {
			return nil, fmt.Errorf("read direct entity for repair: %w", err)
		}
		fields[index] = value
	}
	return fields, nil
}

func (r *Reconciler) groupAuditEntityFields(
	ctx context.Context,
	messages []store.GroupMessage,
) ([]map[string]string, error) {
	pipe := r.client.Pipeline()
	commands := make([]*redis.MapStringStringCmd, len(messages))
	for index, message := range messages {
		commands[index] = pipe.HGetAll(ctx, r.keyspace.groupMessageKey(message.ID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read group entities for repair: %w", err)
	}
	fields := make([]map[string]string, len(messages))
	for index, command := range commands {
		value, err := command.Result()
		if err != nil {
			return nil, fmt.Errorf("read group entity for repair: %w", err)
		}
		fields[index] = value
	}
	return fields, nil
}

func (r *Reconciler) writeGroupRepairs(
	ctx context.Context,
	runAt time.Time,
	cursorKey reconcileSeqCursorKey,
	messages []store.GroupMessage,
	budget *reconcileRepairBudget,
	result *ReconcileResult,
	key string,
) error {
	if len(messages) == 0 || r.repair == nil {
		return nil
	}
	fields, err := r.groupAuditEntityFields(ctx, messages)
	if err != nil {
		return err
	}
	absent := make([]store.GroupMessage, 0, len(messages))
	matching := make([]store.GroupMessage, 0, len(messages))
	diverged := false
	for index, message := range messages {
		if len(fields[index]) == 0 {
			absent = append(absent, message)
			continue
		}
		if !groupEntityMatches(message, fields[index]) {
			diverged = true
			continue
		}
		matching = append(matching, message)
	}
	if diverged {
		result.NeedsRebuild++
		r.logThrottled(runAt, "needs-rebuild", key+":entity", "redis entity differs from MySQL")
	}
	written := 0
	if len(absent) > 0 {
		count, err := r.repair.WriteGroupBatch(ctx, absent)
		if err != nil {
			r.logThrottled(runAt, "repair-error", key+":inbox", "write missing messages: "+err.Error())
			r.setReverseSeqCursor(cursorKey, 0, false)
			return nil
		}
		written += count
	}
	if len(matching) > 0 {
		count, err := r.repairGroupIndexOnly(ctx, matching)
		if err != nil {
			r.logThrottled(runAt, "repair-error", key+":inbox", "write missing inbox entries: "+err.Error())
			r.setReverseSeqCursor(cursorKey, 0, false)
			return nil
		}
		written += count
	}
	result.RedisRepaired += uint64(written)
	budget.spend(written)
	return nil
}

func (r *Reconciler) reverseSeqStart(key reconcileSeqCursorKey) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reverseSeqs[key]
}

func (r *Reconciler) setReverseSeqCursor(key reconcileSeqCursorKey, next uint64, truncated bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !truncated || next == 0 {
		delete(r.reverseSeqs, key)
		return
	}
	if _, exists := r.reverseSeqs[key]; !exists && len(r.reverseSeqs) >= reconcileLogKeyLimit {
		// Bounded state: dropping the cursor only rescans from the start.
		return
	}
	r.reverseSeqs[key] = next
}

// reconcileReadWatermarks compares both read watermarks with MySQL every run.
// Redis may legitimately be newer because async persistence is still in
// flight, so only an older Redis value is raised.
func (r *Reconciler) reconcileReadWatermarks(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
) error {
	if err := r.reconcileDirectReadWatermarks(ctx, runAt, result); err != nil {
		return err
	}
	return r.reconcileGroupReadWatermarks(ctx, runAt, result)
}

func (r *Reconciler) reconcileDirectReadWatermarks(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
) error {
	startUser, startPeer := r.directReadCursorState()
	afterUser, afterPeer := startUser, startPeer
	scanned := 0
	var raised []store.ConversationReadState
	for scanned < reconcileReadStatesPerRun {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := r.store.ListConversationReadStates(ctx, afterUser, afterPeer, r.batch)
		if err != nil {
			return fmt.Errorf("list conversation read states: %w", err)
		}
		if len(rows) == 0 {
			afterUser, afterPeer = 0, 0
			break
		}
		stale, err := r.staleDirectWatermarks(ctx, rows)
		if err != nil {
			return err
		}
		raised = append(raised, stale...)
		last := rows[len(rows)-1]
		afterUser, afterPeer = last.UserID, last.PeerID
		scanned += len(rows)
		if len(rows) < r.batch {
			afterUser, afterPeer = 0, 0
			break
		}
	}
	// Advance only past watermarks that were actually raised. On the first
	// failed chunk the cursor stays before the failed state so the next run
	// retries it instead of skipping a full rotation.
	handled := r.raiseDirectWatermarks(ctx, runAt, raised, result)
	if handled < len(raised) {
		if handled > 0 {
			last := raised[handled-1]
			r.setDirectReadCursor(last.UserID, last.PeerID)
		} else {
			r.setDirectReadCursor(startUser, startPeer)
		}
		return nil
	}
	r.setDirectReadCursor(afterUser, afterPeer)
	return nil
}

func (r *Reconciler) staleDirectWatermarks(
	ctx context.Context,
	rows []store.ConversationReadState,
) ([]store.ConversationReadState, error) {
	pipe := r.client.Pipeline()
	commands := make([]*redis.StringCmd, len(rows))
	for index, row := range rows {
		commands[index] = pipe.HGet(
			ctx,
			r.keyspace.directReadKey(row.UserID),
			strconv.FormatUint(row.PeerID, 10),
		)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read direct read watermarks: %w", err)
	}
	var stale []store.ConversationReadState
	for index, row := range rows {
		current, err := reconcileCounterValue(commands[index])
		if err != nil {
			return nil, fmt.Errorf("read direct read watermark: %w", err)
		}
		if current >= row.LastReadSeq {
			continue
		}
		stale = append(stale, row)
	}
	return stale, nil
}

// raiseDirectWatermarks raises the stale watermarks chunk by chunk and returns
// how many states were handled before the first failure. A nil repairer is
// detection-only, so everything counts as handled and the cursor advances.
func (r *Reconciler) raiseDirectWatermarks(
	ctx context.Context,
	runAt time.Time,
	states []store.ConversationReadState,
	result *ReconcileResult,
) int {
	if len(states) == 0 {
		return 0
	}
	result.RedisMissing += uint64(len(states))
	if r.repair == nil {
		return len(states)
	}
	for _, chunk := range reconcileChunks(len(states), r.batch) {
		raised, err := r.repair.RaiseDirectReadWatermarks(ctx, states[chunk[0]:chunk[1]])
		if err != nil {
			r.logThrottled(runAt, "repair-error", "direct:read", "raise watermarks: "+err.Error())
			return chunk[0]
		}
		result.WatermarksRaised += uint64(raised)
	}
	return len(states)
}

func (r *Reconciler) reconcileGroupReadWatermarks(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
) error {
	startGroup, startUser := r.groupReadCursorState()
	afterGroup, afterUser := startGroup, startUser
	scanned := 0
	var raised []store.GroupMemberReadState
	for scanned < reconcileReadStatesPerRun {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := r.store.ListGroupMemberReadStates(ctx, afterGroup, afterUser, r.batch)
		if err != nil {
			return fmt.Errorf("list group member read states: %w", err)
		}
		if len(rows) == 0 {
			afterGroup, afterUser = 0, 0
			break
		}
		stale, err := r.staleGroupWatermarks(ctx, rows)
		if err != nil {
			return err
		}
		raised = append(raised, stale...)
		last := rows[len(rows)-1]
		afterGroup, afterUser = last.GroupID, last.UserID
		scanned += len(rows)
		if len(rows) < r.batch {
			afterGroup, afterUser = 0, 0
			break
		}
	}
	handled := r.raiseGroupWatermarks(ctx, runAt, raised, result)
	if handled < len(raised) {
		if handled > 0 {
			last := raised[handled-1]
			r.setGroupReadCursor(last.GroupID, last.UserID)
		} else {
			r.setGroupReadCursor(startGroup, startUser)
		}
		return nil
	}
	r.setGroupReadCursor(afterGroup, afterUser)
	return nil
}

func (r *Reconciler) staleGroupWatermarks(
	ctx context.Context,
	rows []store.GroupMemberReadState,
) ([]store.GroupMemberReadState, error) {
	pipe := r.client.Pipeline()
	commands := make([]*redis.FloatCmd, len(rows))
	for index, row := range rows {
		commands[index] = pipe.ZScore(
			ctx,
			r.keyspace.groupReadKey(row.GroupID),
			strconv.FormatUint(row.UserID, 10),
		)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read group read watermarks: %w", err)
	}
	var stale []store.GroupMemberReadState
	for index, row := range rows {
		score, err := commands[index].Result()
		current := uint64(0)
		if err == nil {
			current = uint64(score)
		} else if !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("read group read watermark: %w", err)
		}
		if current >= row.LastReadSeq {
			continue
		}
		stale = append(stale, row)
	}
	return stale, nil
}

// raiseGroupWatermarks mirrors raiseDirectWatermarks for group members.
func (r *Reconciler) raiseGroupWatermarks(
	ctx context.Context,
	runAt time.Time,
	states []store.GroupMemberReadState,
	result *ReconcileResult,
) int {
	if len(states) == 0 {
		return 0
	}
	result.RedisMissing += uint64(len(states))
	if r.repair == nil {
		return len(states)
	}
	for _, chunk := range reconcileChunks(len(states), r.batch) {
		raised, err := r.repair.RaiseGroupReadWatermarks(ctx, states[chunk[0]:chunk[1]])
		if err != nil {
			r.logThrottled(runAt, "repair-error", "group:read", "raise watermarks: "+err.Error())
			return chunk[0]
		}
		result.WatermarksRaised += uint64(raised)
	}
	return len(states)
}

// reconcileChunks splits n items into [start, end) batches of at most size.
func reconcileChunks(n, size int) [][2]int {
	if size <= 0 {
		size = n
	}
	if n == 0 {
		return nil
	}
	chunks := make([][2]int, 0, (n+size-1)/size)
	for start := 0; start < n; start += size {
		end := start + size
		if end > n {
			end = n
		}
		chunks = append(chunks, [2]int{start, end})
	}
	return chunks
}

func (r *Reconciler) directReadCursorState() (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.directReadCursor[0], r.directReadCursor[1]
}

func (r *Reconciler) setDirectReadCursor(userID, peerID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.directReadCursor = [2]uint64{userID, peerID}
}

func (r *Reconciler) groupReadCursorState() (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.groupReadCursor[0], r.groupReadCursor[1]
}

func (r *Reconciler) setGroupReadCursor(groupID, userID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groupReadCursor = [2]uint64{groupID, userID}
}

func (r *Reconciler) directDeepCursorState() (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.directDeepCursor[0], r.directDeepCursor[1]
}

func (r *Reconciler) setDirectDeepCursor(lowID, highID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.directDeepCursor = [2]uint64{lowID, highID}
}

func (r *Reconciler) groupDeepCursorState() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.groupDeepCursor
}

func (r *Reconciler) setGroupDeepCursor(groupID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groupDeepCursor = groupID
}

func (r *Reconciler) userDeepCursorState() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.userDeepCursor
}

func (r *Reconciler) setUserDeepCursor(userID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.userDeepCursor = userID
}

func (r *Reconciler) deepAuditDue(runAt time.Time) bool {
	if r.deepAuditInterval <= 0 {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastDeepAudit.IsZero() || runAt.Sub(r.lastDeepAudit) >= r.deepAuditInterval
}

func (r *Reconciler) recordDeepAudit(runAt time.Time, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastDeepAudit = runAt
	r.status.LastDeepAuditAt = runAt
	if err != nil {
		r.status.DeepAuditError = err.Error()
		return
	}
	r.status.DeepAuditError = ""
}

// deepAudit rotates through a bounded window of conversations and users and
// compares sampled entities and feed sizes with MySQL. It never deletes or
// overwrites existing data.
func (r *Reconciler) deepAudit(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	budget *reconcileRepairBudget,
) error {
	if err := r.deepAuditDirectEntities(ctx, runAt, result, budget); err != nil {
		return err
	}
	if err := r.deepAuditGroupEntities(ctx, runAt, result, budget); err != nil {
		return err
	}
	return r.deepAuditFeeds(ctx, runAt, result, budget)
}

func (r *Reconciler) deepAuditDirectEntities(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	budget *reconcileRepairBudget,
) error {
	afterLow, afterHigh := r.directDeepCursorState()
	rows, err := r.store.ListConversationSeqStats(ctx, afterLow, afterHigh, reconcileDeepAuditPerRun)
	if err != nil {
		return fmt.Errorf("list conversation seq stats for deep audit: %w", err)
	}
	for index, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := reconcileSeqCursorKey{first: row.LowID, second: row.HighID}
		complete, err := r.auditDirectEntities(ctx, runAt, row, result, budget)
		if err != nil {
			return err
		}
		if !complete {
			if r.bumpDeepRetry(key) < reconcileDeepRetryLimit {
				// Keep the cursor before the deferred conversation so the next
				// hourly audit retries its repairs instead of waiting a full
				// rotation.
				if index > 0 {
					previous := rows[index-1]
					r.setDirectDeepCursor(previous.LowID, previous.HighID)
				}
				return nil
			}
			r.clearDeepRetry(key)
			result.NeedsRebuild++
			r.logThrottled(runAt, "needs-rebuild",
				fmt.Sprintf("direct:%d:%d:deferred", row.LowID, row.HighID),
				"sampled repair deferred across audits")
			continue
		}
		r.clearDeepRetry(key)
	}
	// Advance only after the page is done so an aborted audit retries it.
	if len(rows) < reconcileDeepAuditPerRun {
		r.setDirectDeepCursor(0, 0)
	} else {
		last := rows[len(rows)-1]
		r.setDirectDeepCursor(last.LowID, last.HighID)
	}
	return nil
}

// auditDirectEntities samples one conversation's boundary and rotating middle
// entities. It returns false when a repair was deferred by the budget or failed,
// so the caller can stop the page and retry the same conversation next audit.
func (r *Reconciler) auditDirectEntities(
	ctx context.Context,
	runAt time.Time,
	row store.ConversationSeqStat,
	result *ReconcileResult,
	budget *reconcileRepairBudget,
) (bool, error) {
	samples, advance, err := r.directBoundaryMessages(ctx, row)
	if err != nil {
		return false, err
	}
	if len(samples) == 0 {
		return true, nil
	}
	pipe := r.client.Pipeline()
	commands := make([]*redis.MapStringStringCmd, len(samples))
	for index, message := range samples {
		commands[index] = pipe.HGetAll(ctx, r.keyspace.messageKey(message.ID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return false, fmt.Errorf("read direct entities for deep audit: %w", err)
	}
	var missing []store.Message
	diverged := false
	for index, message := range samples {
		fields, err := commands[index].Result()
		if err != nil {
			return false, fmt.Errorf("read direct entity for deep audit: %w", err)
		}
		if len(fields) == 0 {
			missing = append(missing, message)
			continue
		}
		if !directEntityMatches(message, fields) {
			diverged = true
		}
	}
	if diverged {
		result.NeedsRebuild++
		r.logThrottled(runAt, "needs-rebuild",
			fmt.Sprintf("direct:%d:%d:entity", row.LowID, row.HighID),
			"redis entity differs from MySQL")
	}
	complete, err := r.writeSampledDirectRepairs(ctx, runAt, missing, budget, result)
	if complete && advance != nil {
		// The middle sample rotates only once this batch's repairs landed, so a
		// deferred middle entity is sampled again rather than skipped.
		advance()
	}
	return complete, err
}

func (r *Reconciler) writeSampledDirectRepairs(
	ctx context.Context,
	runAt time.Time,
	missing []store.Message,
	budget *reconcileRepairBudget,
	result *ReconcileResult,
) (bool, error) {
	if len(missing) == 0 {
		return true, nil
	}
	result.RedisMissing += uint64(len(missing))
	if r.repair == nil {
		return true, nil
	}
	limit := len(missing)
	if remaining := budget.remaining(); remaining < limit {
		limit = remaining
	}
	if limit <= 0 {
		return false, nil
	}
	for _, chunk := range reconcileChunks(limit, r.repairPerConversation) {
		batch := missing[chunk[0]:chunk[1]]
		written, err := r.repair.WriteDirectBatch(ctx, batch)
		if err != nil {
			r.logThrottled(runAt, "repair-error", "deep:direct:entity", "write missing entity: "+err.Error())
			return false, nil
		}
		result.RedisRepaired += uint64(written)
		budget.spend(written)
	}
	return limit == len(missing), nil
}

func (r *Reconciler) deepAuditGroupEntities(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	budget *reconcileRepairBudget,
) error {
	afterGroupID := r.groupDeepCursorState()
	rows, err := r.store.ListGroupSeqStats(ctx, afterGroupID, reconcileDeepAuditPerRun)
	if err != nil {
		return fmt.Errorf("list group seq stats for deep audit: %w", err)
	}
	for index, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := reconcileSeqCursorKey{group: true, first: row.GroupID}
		complete, err := r.auditGroupEntities(ctx, runAt, row, result, budget)
		if err != nil {
			return err
		}
		if !complete {
			if r.bumpDeepRetry(key) < reconcileDeepRetryLimit {
				if index > 0 {
					r.setGroupDeepCursor(rows[index-1].GroupID)
				}
				return nil
			}
			r.clearDeepRetry(key)
			result.NeedsRebuild++
			r.logThrottled(runAt, "needs-rebuild",
				fmt.Sprintf("group:%d:deferred", row.GroupID),
				"sampled repair deferred across audits")
			continue
		}
		r.clearDeepRetry(key)
	}
	if len(rows) < reconcileDeepAuditPerRun {
		r.setGroupDeepCursor(0)
	} else {
		r.setGroupDeepCursor(rows[len(rows)-1].GroupID)
	}
	return nil
}

func (r *Reconciler) auditGroupEntities(
	ctx context.Context,
	runAt time.Time,
	row store.GroupSeqStat,
	result *ReconcileResult,
	budget *reconcileRepairBudget,
) (bool, error) {
	samples, advance, err := r.groupBoundaryMessages(ctx, row)
	if err != nil {
		return false, err
	}
	if len(samples) == 0 {
		return true, nil
	}
	pipe := r.client.Pipeline()
	commands := make([]*redis.MapStringStringCmd, len(samples))
	for index, message := range samples {
		commands[index] = pipe.HGetAll(ctx, r.keyspace.groupMessageKey(message.ID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return false, fmt.Errorf("read group entities for deep audit: %w", err)
	}
	var missing []store.GroupMessage
	diverged := false
	for index, message := range samples {
		fields, err := commands[index].Result()
		if err != nil {
			return false, fmt.Errorf("read group entity for deep audit: %w", err)
		}
		if len(fields) == 0 {
			missing = append(missing, message)
			continue
		}
		if !groupEntityMatches(message, fields) {
			diverged = true
		}
	}
	if diverged {
		result.NeedsRebuild++
		r.logThrottled(runAt, "needs-rebuild",
			fmt.Sprintf("group:%d:entity", row.GroupID),
			"redis entity differs from MySQL")
	}
	complete, err := r.writeSampledGroupRepairs(ctx, runAt, missing, budget, result)
	if complete && advance != nil {
		advance()
	}
	return complete, err
}

func (r *Reconciler) writeSampledGroupRepairs(
	ctx context.Context,
	runAt time.Time,
	missing []store.GroupMessage,
	budget *reconcileRepairBudget,
	result *ReconcileResult,
) (bool, error) {
	if len(missing) == 0 {
		return true, nil
	}
	result.RedisMissing += uint64(len(missing))
	if r.repair == nil {
		return true, nil
	}
	limit := len(missing)
	if remaining := budget.remaining(); remaining < limit {
		limit = remaining
	}
	if limit <= 0 {
		return false, nil
	}
	for _, chunk := range reconcileChunks(limit, r.repairPerConversation) {
		batch := missing[chunk[0]:chunk[1]]
		written, err := r.repair.WriteGroupBatch(ctx, batch)
		if err != nil {
			r.logThrottled(runAt, "repair-error", "deep:group:entity", "write missing entity: "+err.Error())
			return false, nil
		}
		result.RedisRepaired += uint64(written)
		budget.spend(written)
	}
	return limit == len(missing), nil
}

// directBoundaryMessages samples the oldest and newest seqs of a conversation,
// plus one rotating middle batch, which together find a partial tail loss, a
// wiped prefix and an entity lost in the middle. advance commits the middle
// cursor and is nil when no middle batch was read; it must be called only after
// the batch's repairs completed.
func (r *Reconciler) directBoundaryMessages(
	ctx context.Context,
	row store.ConversationSeqStat,
) ([]store.Message, func(), error) {
	boundary := r.deepAuditBoundary
	if boundary <= 0 {
		return nil, nil, nil
	}
	first, err := r.store.ListMessagesBySeqRange(ctx, row.LowID, row.HighID, 0, boundary)
	if err != nil {
		return nil, nil, fmt.Errorf("sample oldest direct messages: %w", err)
	}
	if row.MaxSeq <= uint64(boundary) {
		return first, nil, nil
	}
	last, err := r.store.ListMessagesBySeqRange(
		ctx, row.LowID, row.HighID, row.MaxSeq-uint64(boundary), boundary,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("sample newest direct messages: %w", err)
	}
	middle, advance, err := r.directMiddleSample(ctx, row, boundary)
	if err != nil {
		return nil, nil, err
	}
	return reconcileUniqueDirect(append(append(first, last...), middle...)), advance, nil
}

// directMiddleSample reads one rotating batch of messages strictly between the
// boundary samples. The per-conversation cursor advances once per hourly audit
// and wraps back to the start of the middle window.
func (r *Reconciler) directMiddleSample(
	ctx context.Context,
	row store.ConversationSeqStat,
	boundary int,
) ([]store.Message, func(), error) {
	low := uint64(boundary) + 1
	if row.MaxSeq < low {
		return nil, nil, nil
	}
	high := row.MaxSeq - uint64(boundary)
	if high < low {
		return nil, nil, nil
	}
	key := reconcileSeqCursorKey{first: row.LowID, second: row.HighID}
	start := r.deepSeqStart(key, low, high)
	page, err := r.store.ListMessagesBySeqRange(ctx, row.LowID, row.HighID, start-1, boundary)
	if err != nil {
		return nil, nil, fmt.Errorf("sample rotating direct messages: %w", err)
	}
	if len(page) == 0 {
		return nil, func() { r.setDeepSeqCursor(key, 0, true) }, nil
	}
	next := page[len(page)-1].Seq + 1
	reset := next > high
	advance := func() { r.setDeepSeqCursor(key, next, reset) }
	middle := make([]store.Message, 0, len(page))
	for _, message := range page {
		if message.Seq <= high {
			middle = append(middle, message)
		}
	}
	return middle, advance, nil
}

func (r *Reconciler) groupBoundaryMessages(
	ctx context.Context,
	row store.GroupSeqStat,
) ([]store.GroupMessage, func(), error) {
	boundary := r.deepAuditBoundary
	if boundary <= 0 {
		return nil, nil, nil
	}
	first, err := r.store.ListGroupMessagesBySeqRange(ctx, row.GroupID, 0, boundary)
	if err != nil {
		return nil, nil, fmt.Errorf("sample oldest group messages: %w", err)
	}
	if row.MaxSeq <= uint64(boundary) {
		return first, nil, nil
	}
	last, err := r.store.ListGroupMessagesBySeqRange(
		ctx, row.GroupID, row.MaxSeq-uint64(boundary), boundary,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("sample newest group messages: %w", err)
	}
	middle, advance, err := r.groupMiddleSample(ctx, row, boundary)
	if err != nil {
		return nil, nil, err
	}
	return reconcileUniqueGroup(append(append(first, last...), middle...)), advance, nil
}

func (r *Reconciler) groupMiddleSample(
	ctx context.Context,
	row store.GroupSeqStat,
	boundary int,
) ([]store.GroupMessage, func(), error) {
	low := uint64(boundary) + 1
	if row.MaxSeq < low {
		return nil, nil, nil
	}
	high := row.MaxSeq - uint64(boundary)
	if high < low {
		return nil, nil, nil
	}
	key := reconcileSeqCursorKey{group: true, first: row.GroupID}
	start := r.deepSeqStart(key, low, high)
	page, err := r.store.ListGroupMessagesBySeqRange(ctx, row.GroupID, start-1, boundary)
	if err != nil {
		return nil, nil, fmt.Errorf("sample rotating group messages: %w", err)
	}
	if len(page) == 0 {
		return nil, func() { r.setDeepSeqCursor(key, 0, true) }, nil
	}
	next := page[len(page)-1].Seq + 1
	reset := next > high
	advance := func() { r.setDeepSeqCursor(key, next, reset) }
	middle := make([]store.GroupMessage, 0, len(page))
	for _, message := range page {
		if message.Seq <= high {
			middle = append(middle, message)
		}
	}
	return middle, advance, nil
}

// deepSeqStart returns the rotating middle-sample cursor, restarting when the
// stored position falls outside the conversation's current middle window.
func (r *Reconciler) deepSeqStart(key reconcileSeqCursorKey, low, high uint64) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	start := r.deepSeqs[key]
	if start < low || start > high {
		return low
	}
	return start
}

func (r *Reconciler) setDeepSeqCursor(key reconcileSeqCursorKey, next uint64, reset bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if reset || next == 0 {
		delete(r.deepSeqs, key)
		return
	}
	if _, exists := r.deepSeqs[key]; !exists && len(r.deepSeqs) >= reconcileLogKeyLimit {
		// Bounded state: dropping the cursor only restarts the sample at the
		// beginning of the middle window.
		return
	}
	r.deepSeqs[key] = next
}

// bumpDeepRetry counts one deferral of a deep-audit row.
func (r *Reconciler) bumpDeepRetry(key reconcileSeqCursorKey) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.deepRetries[key]; !exists && len(r.deepRetries) >= reconcileLogKeyLimit {
		return reconcileDeepRetryLimit
	}
	r.deepRetries[key]++
	return r.deepRetries[key]
}

func (r *Reconciler) clearDeepRetry(key reconcileSeqCursorKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.deepRetries, key)
}

func (r *Reconciler) bumpUserDeepRetry(userID uint64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.userDeepRetries[userID]; !exists && len(r.userDeepRetries) >= reconcileLogKeyLimit {
		return reconcileDeepRetryLimit
	}
	r.userDeepRetries[userID]++
	return r.userDeepRetries[userID]
}

func (r *Reconciler) clearUserDeepRetry(userID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.userDeepRetries, userID)
}

func reconcileUniqueDirect(messages []store.Message) []store.Message {
	seen := make(map[uint64]struct{}, len(messages))
	unique := make([]store.Message, 0, len(messages))
	for _, message := range messages {
		if _, ok := seen[message.ID]; ok {
			continue
		}
		seen[message.ID] = struct{}{}
		unique = append(unique, message)
	}
	return unique
}

func reconcileUniqueGroup(messages []store.GroupMessage) []store.GroupMessage {
	seen := make(map[uint64]struct{}, len(messages))
	unique := make([]store.GroupMessage, 0, len(messages))
	for _, message := range messages {
		if _, ok := seen[message.ID]; ok {
			continue
		}
		seen[message.ID] = struct{}{}
		unique = append(unique, message)
	}
	return unique
}

// directEntityMatches compares the immutable entity fields MySQL owns. A
// mismatch is not repaired because the auditor never overwrites an entity.
func directEntityMatches(message store.Message, fields map[string]string) bool {
	id, err := reconcileEntityUint(fields, "id")
	if err != nil || id != message.ID {
		return false
	}
	seq, err := reconcileEntityUint(fields, "seq")
	if err != nil || seq != message.Seq {
		return false
	}
	senderID, err := reconcileEntityUint(fields, "sender_id")
	if err != nil || senderID != message.SenderID {
		return false
	}
	recipientID, err := reconcileEntityUint(fields, "recipient_id")
	if err != nil || recipientID != message.RecipientID {
		return false
	}
	createdAtMS, err := reconcileEntityInt64(fields, "created_at_ms")
	if err != nil || createdAtMS != message.CreatedAt.UTC().Truncate(time.Millisecond).UnixMilli() {
		return false
	}
	return strings.TrimSpace(fields["client_message_id"]) == message.ClientMessageID &&
		fields["body"] == message.Body
}

func groupEntityMatches(message store.GroupMessage, fields map[string]string) bool {
	id, err := reconcileEntityUint(fields, "id")
	if err != nil || id != message.ID {
		return false
	}
	seq, err := reconcileEntityUint(fields, "seq")
	if err != nil || seq != message.Seq {
		return false
	}
	groupID, err := reconcileEntityUint(fields, "group_id")
	if err != nil || groupID != message.GroupID {
		return false
	}
	senderID, err := reconcileEntityUint(fields, "sender_id")
	if err != nil || senderID != message.SenderID {
		return false
	}
	createdAtMS, err := reconcileEntityInt64(fields, "created_at_ms")
	if err != nil || createdAtMS != message.CreatedAt.UTC().Truncate(time.Millisecond).UnixMilli() {
		return false
	}
	// Sender nicknames are mutable snapshots; only require them to be present.
	_, hasUsername := fields["sender_username"]
	_, hasNickname := fields["sender_nickname"]
	return hasUsername && hasNickname &&
		strings.TrimSpace(fields["client_message_id"]) == message.ClientMessageID &&
		fields["body"] == message.Body
}

func (r *Reconciler) deepAuditFeeds(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	budget *reconcileRepairBudget,
) error {
	cursor := r.userDeepCursorState()
	ids, err := r.store.ListUserIDs(ctx, cursor, r.deepAuditUsers)
	if err != nil {
		return fmt.Errorf("list user IDs for deep audit: %w", err)
	}
	for index, userID := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		complete, err := r.auditUserFeed(ctx, runAt, userID, result, budget)
		if err != nil {
			return err
		}
		if !complete {
			if r.bumpUserDeepRetry(userID) < reconcileDeepRetryLimit {
				// Keep the cursor before this user so the next hourly audit
				// retries the deferred feed/preview repairs.
				if index > 0 {
					r.setUserDeepCursor(ids[index-1])
				}
				return nil
			}
			r.clearUserDeepRetry(userID)
			result.NeedsRebuild++
			r.logThrottled(runAt, "needs-rebuild",
				fmt.Sprintf("user:%d:feed", userID),
				"sampled repair deferred across audits")
			continue
		}
		r.clearUserDeepRetry(userID)
	}
	if len(ids) < r.deepAuditUsers {
		r.setUserDeepCursor(0)
	} else {
		r.setUserDeepCursor(ids[len(ids)-1])
	}
	return nil
}

// auditUserFeed samples one user's feed size, conversation previews and, when
// the feed is short, the missing feed entries. It returns false when a repair
// was deferred by the budget or failed.
func (r *Reconciler) auditUserFeed(
	ctx context.Context,
	runAt time.Time,
	userID uint64,
	result *ReconcileResult,
	budget *reconcileRepairBudget,
) (bool, error) {
	previewsComplete, err := r.auditUserConversationLast(ctx, runAt, userID, result, budget)
	if err != nil {
		return false, err
	}
	count, err := r.store.CountMessagesInvolvingUser(ctx, userID)
	if err != nil {
		return false, fmt.Errorf("count messages involving user: %w", err)
	}
	if count == 0 {
		return previewsComplete, nil
	}
	key := r.keyspace.directFeedKey(userID)
	card, err := r.client.ZCard(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("read direct feed size: %w", err)
	}
	if card >= int64(count) {
		return previewsComplete, nil
	}
	gap := count - uint64(card)
	result.RedisMissing += gap
	if gap > uint64(r.repairPerConversation) {
		result.NeedsRebuild++
		r.logThrottled(runAt, "needs-rebuild",
			fmt.Sprintf("user:%d:feed", userID),
			fmt.Sprintf("direct feed gap %d exceeds repair budget", gap))
		return previewsComplete, nil
	}
	if r.repair == nil {
		return previewsComplete, nil
	}
	limit := int(gap)
	if remaining := budget.remaining(); remaining < limit {
		limit = remaining
	}
	if limit <= 0 {
		return false, nil
	}
	missing, err := r.missingFeedMessages(ctx, userID, limit)
	if err != nil {
		return false, err
	}
	for _, chunk := range reconcileChunks(len(missing), reconcileFeedWriteBatch) {
		batch := missing[chunk[0]:chunk[1]]
		written, err := r.repair.WriteDirectBatch(ctx, batch)
		if err != nil {
			r.logThrottled(runAt, "repair-error", fmt.Sprintf("user:%d:feed", userID),
				"write missing feed entries: "+err.Error())
			r.setFeedCursor(userID, reconcileFeedCursor{}, false)
			return false, nil
		}
		result.FeedRepairs += uint64(written)
		budget.spend(written)
	}
	// A budget-truncated walk leaves part of the detected gap for the next
	// audit, so the user cursor must not move past it.
	return previewsComplete && limit == int(gap), nil
}

// auditUserConversationLast compares each conversation preview with the newest
// MySQL message through the sampled user. A preview that is absent or older is
// restored by the projection writer only when its entity exists; otherwise the
// user is flagged as needing a rebuild.
func (r *Reconciler) auditUserConversationLast(
	ctx context.Context,
	runAt time.Time,
	userID uint64,
	result *ReconcileResult,
	budget *reconcileRepairBudget,
) (bool, error) {
	conversations, err := r.store.ListConversationBasics(ctx, userID)
	if err != nil {
		return false, fmt.Errorf("list user conversations for preview audit: %w", err)
	}
	if len(conversations) == 0 {
		return true, nil
	}
	previews, err := r.client.HGetAll(ctx, r.keyspace.directConversationLastKey(userID)).Result()
	if err != nil {
		return false, fmt.Errorf("read direct conversation previews: %w", err)
	}
	limit := r.repairPerConversation
	if remaining := budget.remaining(); remaining < limit {
		limit = remaining
	}
	var repairs []store.Message
	deferred := false
	for _, conversation := range conversations {
		peerID := conversation.UserID
		if peerID == 0 || peerID == userID || conversation.LastMessage == nil {
			continue
		}
		message := *conversation.LastMessage
		if message.ID == 0 {
			continue
		}
		current := reconcilePreviewValue(previews[strconv.FormatUint(peerID, 10)])
		if current >= message.ID {
			continue
		}
		result.RedisMissing++
		exists, err := r.client.Exists(ctx, r.keyspace.messageKey(message.ID)).Result()
		if err != nil {
			return false, fmt.Errorf("read conversation preview entity: %w", err)
		}
		if exists == 0 {
			result.NeedsRebuild++
			r.logThrottled(runAt, "needs-rebuild",
				fmt.Sprintf("user:%d:conv-last", userID),
				"newest conversation message entity is missing")
			continue
		}
		if r.repair == nil {
			continue
		}
		if len(repairs) >= limit {
			deferred = true
			continue
		}
		repairs = append(repairs, message)
	}
	if r.repair != nil && len(repairs) > 0 {
		written, err := r.repair.WriteDirectBatch(ctx, repairs)
		if err != nil {
			r.logThrottled(runAt, "repair-error", fmt.Sprintf("user:%d:conv-last", userID),
				"write conversation preview: "+err.Error())
			return false, nil
		}
		result.RedisRepaired += uint64(written)
		budget.spend(written)
	}
	return !deferred, nil
}

func reconcilePreviewValue(raw string) uint64 {
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0
	}
	return value
}

// reconcileFeedCursor resumes a capped feed scan at one conversation's seq.
type reconcileFeedCursor struct {
	peer uint64
	seq  uint64
}

func (r *Reconciler) feedCursorStart(userID uint64) reconcileFeedCursor {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.feedCursors[userID]
}

func (r *Reconciler) setFeedCursor(userID uint64, cursor reconcileFeedCursor, keep bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !keep {
		delete(r.feedCursors, userID)
		return
	}
	if _, exists := r.feedCursors[userID]; !exists && len(r.feedCursors) >= reconcileLogKeyLimit {
		return
	}
	r.feedCursors[userID] = cursor
}

// missingFeedMessages walks the user's conversations in MySQL and returns the
// rows absent from dm:feed, resuming from the previous run's cursor when its
// check budget was exhausted.
func (r *Reconciler) missingFeedMessages(
	ctx context.Context,
	userID uint64,
	limit int,
) ([]store.Message, error) {
	conversations, err := r.store.ListConversationBasics(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("list user conversations: %w", err)
	}
	// ListConversationBasics is ordered by latest message, not by peer. The
	// resume cursor below is peer-ordered, so sort a copy to make paging
	// monotonic and independent of recency.
	sort.SliceStable(conversations, func(i, j int) bool {
		return conversations[i].UserID < conversations[j].UserID
	})
	cursor := r.feedCursorStart(userID)
	start := 0
	if cursor.peer != 0 {
		start = sort.Search(len(conversations), func(index int) bool {
			return conversations[index].UserID >= cursor.peer
		})
		if start == len(conversations) {
			// The cursor peer no longer exists: restart the walk so a gap in
			// an earlier conversation is not skipped forever.
			start = 0
			cursor = reconcileFeedCursor{}
			r.setFeedCursor(userID, reconcileFeedCursor{}, false)
		}
	}
	key := r.keyspace.directFeedKey(userID)
	missing := make([]store.Message, 0, limit)
	checks := 0
	for index := start; index < len(conversations); index++ {
		peerID := conversations[index].UserID
		if peerID == 0 || peerID == userID {
			continue
		}
		lowID, highID := sortedPair(userID, peerID)
		afterSeq := uint64(0)
		if peerID == cursor.peer {
			afterSeq = cursor.seq
		}
		for len(missing) < limit && checks < reconcileFeedChecksPerUser {
			pageSize := r.batch
			if remaining := reconcileFeedChecksPerUser - checks; pageSize > remaining {
				pageSize = remaining
			}
			page, err := r.store.ListMessagesBySeqRange(ctx, lowID, highID, afterSeq, pageSize)
			if err != nil {
				return nil, fmt.Errorf("list messages for feed audit: %w", err)
			}
			if len(page) == 0 {
				break
			}
			present, err := r.feedMemberships(ctx, key, page)
			if err != nil {
				return nil, err
			}
			for pageIndex, message := range page {
				if present[pageIndex] {
					continue
				}
				missing = append(missing, message)
				if len(missing) >= limit {
					break
				}
			}
			checks += len(page)
			afterSeq = page[len(page)-1].Seq
			if len(page) < pageSize {
				break
			}
		}
		if len(missing) >= limit {
			// The detected gap is covered; a later audit finds anything left.
			r.setFeedCursor(userID, reconcileFeedCursor{}, false)
			return missing, nil
		}
		if checks >= reconcileFeedChecksPerUser {
			r.setFeedCursor(userID, reconcileFeedCursor{peer: peerID, seq: afterSeq}, true)
			return missing, nil
		}
	}
	r.setFeedCursor(userID, reconcileFeedCursor{}, false)
	return missing, nil
}

func (r *Reconciler) feedMemberships(
	ctx context.Context,
	key string,
	messages []store.Message,
) ([]bool, error) {
	pipe := r.client.Pipeline()
	commands := make([]*redis.FloatCmd, len(messages))
	for index, message := range messages {
		commands[index] = pipe.ZScore(ctx, key, strconv.FormatUint(message.ID, 10))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read direct feed scores: %w", err)
	}
	present := make([]bool, len(messages))
	for index := range messages {
		score, err := commands[index].Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read direct feed score: %w", err)
		}
		present[index] = score == float64(messages[index].ID)
	}
	return present, nil
}

func reconcileWait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
