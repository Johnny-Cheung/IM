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
	ListConversationSeqs(ctx context.Context, lowID, highID, upTo uint64) ([]uint64, error)
	ListGroupSeqs(ctx context.Context, groupID, upTo uint64) ([]uint64, error)
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
	// MaxDetail bounds mismatched conversations repaired per run. Default 50.
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
	defaultReconcileInterval     = 5 * time.Minute
	defaultReconcileMinAge       = 5 * time.Minute
	defaultReconcileBatch        = 500
	defaultReconcileMaxDetail    = 50
	defaultReconcileLogEvery     = time.Hour
	reconcileLogKeyLimit         = 2048
	reconcileScanKeysPerRun      = 5000
	reconcileScanCountHint       = 500
	reconcileGatePollFloor       = time.Millisecond
	reconcileGatePollCeiling     = time.Second
	reconcileIncompleteEntityMsg = "entity is incomplete"
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

	mu               sync.Mutex
	status           ReconcileStatus
	logged           map[string]time.Time
	directScanCursor uint64
	groupScanCursor  uint64
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
		client:    client,
		store:     dataStore,
		keyspace:  options.Keyspace,
		gate:      options.Gate,
		interval:  options.Interval,
		minAge:    options.MinAge,
		logEvery:  options.LogEvery,
		batch:     options.Batch,
		maxDetail: options.MaxDetail,
		now:       options.Now,
		logged:    make(map[string]time.Time),
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
	summary := fmt.Sprintf(
		"scanned=%d mismatched=%d repaired=%d unresolved=%d in_flight=%d",
		result.Scanned, result.Mismatched, result.Repaired, result.Unresolved, result.InFlight,
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

	if result.Mismatched > 0 || result.Repaired > 0 {
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
	detail := 0

	seenPairs := make(map[[2]uint64]struct{})
	if err := r.reconcileDirect(ctx, runAt, &result, seenPairs, &detail); err != nil {
		return result, err
	}
	seenGroups := make(map[uint64]struct{})
	if err := r.reconcileGroups(ctx, runAt, &result, seenGroups, &detail); err != nil {
		return result, err
	}
	if err := r.reconcileOrphanDirectCounters(ctx, runAt, &result, seenPairs, &detail); err != nil {
		return result, err
	}
	return result, r.reconcileOrphanGroupCounters(ctx, runAt, &result, seenGroups, &detail)
}

// reconcileDirect pages the direct conversation stats and repairs mismatches.
func (r *Reconciler) reconcileDirect(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	seen map[[2]uint64]struct{},
	detail *int,
) error {
	afterLow, afterHigh := uint64(0), uint64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := r.store.ListConversationSeqStats(ctx, afterLow, afterHigh, r.batch)
		if err != nil {
			return fmt.Errorf("list conversation seq stats: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}
		counters, err := r.readDirectCounters(ctx, rows)
		if err != nil {
			return err
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
			if *detail >= r.maxDetail {
				continue
			}
			*detail++
			if err := r.repairDirectConversation(ctx, runAt, row.LowID, row.HighID, counter, result); err != nil {
				return err
			}
		}
		last := rows[len(rows)-1]
		afterLow, afterHigh = last.LowID, last.HighID
		if len(rows) < r.batch {
			return nil
		}
	}
}

// reconcileGroups pages the group stats and repairs mismatches.
func (r *Reconciler) reconcileGroups(
	ctx context.Context,
	runAt time.Time,
	result *ReconcileResult,
	seen map[uint64]struct{},
	detail *int,
) error {
	afterGroupID := uint64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := r.store.ListGroupSeqStats(ctx, afterGroupID, r.batch)
		if err != nil {
			return fmt.Errorf("list group seq stats: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}
		counters, err := r.readGroupCounters(ctx, rows)
		if err != nil {
			return err
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
			if *detail >= r.maxDetail {
				continue
			}
			*detail++
			if err := r.repairGroupConversation(ctx, runAt, row.GroupID, counter, result); err != nil {
				return err
			}
		}
		afterGroupID = rows[len(rows)-1].GroupID
		if len(rows) < r.batch {
			return nil
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

func (r *Reconciler) repairDirectConversation(
	ctx context.Context,
	runAt time.Time,
	lowID, highID, counter uint64,
	result *ReconcileResult,
) error {
	seqs, err := r.store.ListConversationSeqs(ctx, lowID, highID, counter)
	if err != nil {
		return fmt.Errorf("list conversation seqs: %w", err)
	}
	for _, seq := range reconcileMissingSeqs(seqs, counter) {
		if err := ctx.Err(); err != nil {
			return err
		}
		outcome, err := r.repairDirectSeq(ctx, runAt, lowID, highID, seq)
		if err != nil {
			return err
		}
		result.add(outcome)
	}
	return nil
}

func (r *Reconciler) repairGroupConversation(
	ctx context.Context,
	runAt time.Time,
	groupID, counter uint64,
	result *ReconcileResult,
) error {
	seqs, err := r.store.ListGroupSeqs(ctx, groupID, counter)
	if err != nil {
		return fmt.Errorf("list group seqs: %w", err)
	}
	for _, seq := range reconcileMissingSeqs(seqs, counter) {
		if err := ctx.Err(); err != nil {
			return err
		}
		outcome, err := r.repairGroupSeq(ctx, runAt, groupID, seq)
		if err != nil {
			return err
		}
		result.add(outcome)
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

// reconcileMissingSeqs returns the seqs in 1..upTo absent from stored, ascending.
func reconcileMissingSeqs(stored []uint64, upTo uint64) []uint64 {
	present := make(map[uint64]struct{}, len(stored))
	for _, seq := range stored {
		present[seq] = struct{}{}
	}
	missing := make([]uint64, 0)
	if upTo > uint64(len(stored)) {
		missing = make([]uint64, 0, upTo-uint64(len(stored)))
	}
	for seq := uint64(1); seq <= upTo; seq++ {
		if _, ok := present[seq]; !ok {
			missing = append(missing, seq)
		}
	}
	return missing
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
	if id != messageID || entitySeq != seq {
		return store.Message{}, errors.New("entity identity differs from the index")
	}
	if senderID == 0 || recipientID == 0 || clientMessageID == "" {
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
		Body:            fields["body"],
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
	if id != messageID || entitySeq != seq || entityGroupID != groupID {
		return store.GroupMessage{}, errors.New("entity identity differs from the index")
	}
	if senderID == 0 || clientMessageID == "" {
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
			Username: fields["sender_username"],
			Nickname: fields["sender_nickname"],
		},
		ClientMessageID: clientMessageID,
		Body:            fields["body"],
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
	detail *int,
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
		if *detail >= r.maxDetail {
			continue
		}
		*detail++
		if err := r.repairDirectConversation(ctx, runAt, lowID, highID, counter, result); err != nil {
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
	detail *int,
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
		if *detail >= r.maxDetail {
			continue
		}
		*detail++
		if err := r.repairGroupConversation(ctx, runAt, groupID, counter, result); err != nil {
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

// logUnresolved logs one unresolved item at most once per LogEvery. The dedupe
// map is bounded so an endless stream of distinct items cannot grow it.
func (r *Reconciler) logUnresolved(runAt time.Time, key, reason string) {
	r.mu.Lock()
	if when, ok := r.logged[key]; ok && runAt.Sub(when) < r.logEvery {
		r.mu.Unlock()
		return
	}
	if len(r.logged) >= reconcileLogKeyLimit {
		r.evictLoggedLocked(runAt)
	}
	r.logged[key] = runAt
	r.mu.Unlock()
	log.Printf("reconcile unresolved %s: %s", key, reason)
}

func (r *Reconciler) evictLoggedLocked(now time.Time) {
	for key, when := range r.logged {
		if now.Sub(when) >= r.logEvery {
			delete(r.logged, key)
		}
	}
	target := reconcileLogKeyLimit / 2
	for key := range r.logged {
		if len(r.logged) <= target {
			break
		}
		delete(r.logged, key)
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
