package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	return &Reconciler{
		client:      client,
		store:       dataStore,
		keyspace:    options.Keyspace,
		gate:        options.Gate,
		interval:    options.Interval,
		minAge:      options.MinAge,
		logEvery:    options.LogEvery,
		batch:       options.Batch,
		maxDetail:   options.MaxDetail,
		now:         options.Now,
		logged:      make(map[string]time.Time),
		missingSeqs: make(map[reconcileSeqCursorKey]uint64),
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
		"scanned=%d mismatched=%d repaired=%d unresolved=%d in_flight=%d deferred=%d suppressed=%d",
		result.Scanned, result.Mismatched, result.Repaired, result.Unresolved,
		result.InFlight, result.Deferred, suppressed,
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
	r.status.LastResultSummary = summary
	// A cancelled context is a shutdown, not a reconciliation failure.
	if err == nil || ctx.Err() != nil {
		r.status.LastError = ""
	} else {
		r.status.LastError = err.Error()
	}
	r.mu.Unlock()

	if result.Mismatched > 0 || result.Repaired > 0 || result.Unresolved > 0 || suppressed > 0 {
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

	seenPairs := make(map[[2]uint64]struct{})
	if err := r.reconcileDirect(ctx, runAt, &result, seenPairs); err != nil {
		return result, err
	}
	seenGroups := make(map[uint64]struct{})
	if err := r.reconcileGroups(ctx, runAt, &result, seenGroups); err != nil {
		return result, err
	}
	if err := r.reconcileOrphanDirectCounters(ctx, runAt, &result, seenPairs); err != nil {
		return result, err
	}
	return result, r.reconcileOrphanGroupCounters(ctx, runAt, &result, seenGroups)
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
) error {
	suspects, err := r.scanDirectSuspects(ctx, result, seen)
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
	result *ReconcileResult,
	seen map[[2]uint64]struct{},
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
		counters, err := r.readDirectCounters(ctx, rows)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row.LowID == 0 || row.HighID == 0 {
				continue
			}
			seen[[2]uint64{row.LowID, row.HighID}] = struct{}{}
			counter := counters[[2]uint64{row.LowID, row.HighID}]
			if counter == 0 {
				continue
			}
			result.Scanned++
			if counter <= row.MaxSeq && row.Count == row.MaxSeq {
				continue
			}
			result.Mismatched++
			suspects = append(suspects, reconcileDirectSuspect{
				lowID: row.LowID, highID: row.HighID, counter: counter,
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
) error {
	suspects, err := r.scanGroupSuspects(ctx, result, seen)
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
	result *ReconcileResult,
	seen map[uint64]struct{},
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
		counters, err := r.readGroupCounters(ctx, rows)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row.GroupID == 0 {
				continue
			}
			seen[row.GroupID] = struct{}{}
			counter := counters[row.GroupID]
			if counter == 0 {
				continue
			}
			result.Scanned++
			if counter <= row.MaxSeq && row.Count == row.MaxSeq {
				continue
			}
			result.Mismatched++
			suspects = append(suspects, reconcileGroupSuspect{
				groupID: row.GroupID, counter: counter,
				count: row.Count, maxSeq: row.MaxSeq,
			})
		}
		afterGroupID = rows[len(rows)-1].GroupID
		if len(rows) < r.batch {
			return suspects, nil
		}
	}
}

func (r *Reconciler) readDirectCounters(
	ctx context.Context,
	rows []store.ConversationSeqStat,
) (map[[2]uint64]uint64, error) {
	pipe := r.client.Pipeline()
	commands := make([]*redis.StringCmd, len(rows))
	for index, row := range rows {
		commands[index] = pipe.Get(ctx, r.keyspace.directSequenceKey(row.LowID, row.HighID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read conversation seq counters: %w", err)
	}
	counters := make(map[[2]uint64]uint64, len(rows))
	for index, row := range rows {
		counter, err := reconcileCounterValue(commands[index])
		if err != nil {
			return nil, fmt.Errorf("read conversation seq counter: %w", err)
		}
		counters[[2]uint64{row.LowID, row.HighID}] = counter
	}
	return counters, nil
}

func (r *Reconciler) readGroupCounters(
	ctx context.Context,
	rows []store.GroupSeqStat,
) (map[uint64]uint64, error) {
	pipe := r.client.Pipeline()
	commands := make([]*redis.StringCmd, len(rows))
	for index, row := range rows {
		commands[index] = pipe.Get(ctx, r.keyspace.groupSequenceKey(row.GroupID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read group seq counters: %w", err)
	}
	counters := make(map[uint64]uint64, len(rows))
	for index, row := range rows {
		counter, err := reconcileCounterValue(commands[index])
		if err != nil {
			return nil, fmt.Errorf("read group seq counter: %w", err)
		}
		counters[row.GroupID] = counter
	}
	return counters, nil
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
	r.mu.Lock()
	if when, ok := r.logged[key]; ok && runAt.Sub(when) < r.logEvery {
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
	r.logged[key] = runAt
	r.mu.Unlock()
	log.Printf("reconcile unresolved %s: %s", key, reason)
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
