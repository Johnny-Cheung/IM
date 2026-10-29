package messaging

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// SeqCalibrationRunner is the calibration step the watcher repeats until it
// succeeds.
type SeqCalibrationRunner interface {
	Calibrate(ctx context.Context) (int, error)
}

// SeqRunIDSource reports the Redis server run_id so a restart can be detected.
type SeqRunIDSource interface {
	RunID(ctx context.Context) (string, error)
}

type redisRunIDSource struct {
	client redis.UniversalClient
}

func (s redisRunIDSource) RunID(ctx context.Context) (string, error) {
	if s.client == nil {
		return "", ErrMessageServiceUnavailable
	}
	info, err := s.client.Info(ctx, "server").Result()
	if err != nil {
		return "", fmt.Errorf("read redis server info: %w", err)
	}
	return parseRunIDInfo(info)
}

// ReadRunID reports the Redis server run_id. Callers can capture it before
// startup calibration so the watcher detects a restart that happens between
// calibration and its first check.
func ReadRunID(ctx context.Context, client redis.UniversalClient) (string, error) {
	return redisRunIDSource{client: client}.RunID(ctx)
}

func parseRunIDInfo(info string) (string, error) {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		value, ok := strings.CutPrefix(line, "run_id:")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		if value == "" {
			break
		}
		return value, nil
	}
	return "", fmt.Errorf("redis run_id missing")
}

const (
	defaultSeqWatchInterval   = 5 * time.Second
	defaultSeqWatchBackoff    = time.Second
	defaultSeqWatchMaxBackoff = 30 * time.Second
)

// SeqWatcherOptions configures the run_id watcher.
type SeqWatcherOptions struct {
	Keyspace   Keyspace
	Gate       *SeqGate
	Calibrator SeqCalibrationRunner
	RunIDs     SeqRunIDSource
	// InitialRunID is the run_id captured before startup calibration. When it
	// is set the first check compares against it instead of skipping, so a
	// restart during startup is still detected.
	InitialRunID string
	// InitialCalibratedAt and InitialConversations carry the startup
	// calibration into the status so /ops/infra is meaningful before the
	// watcher's first runtime recalibration.
	InitialCalibratedAt  time.Time
	InitialConversations int
	Interval             time.Duration
	Backoff              time.Duration
	MaxBackoff           time.Duration
}

// SeqWatcherStatus is the diagnostics surface for /ops/infra.
type SeqWatcherStatus struct {
	Ready            bool      `json:"ready"`
	RunID            string    `json:"run_id,omitempty"`
	Calibrating      bool      `json:"calibrating"`
	WaitingSeqReady  bool      `json:"waiting_seq_ready"`
	Maintenance      bool      `json:"maintenance"`
	LastCalibratedAt time.Time `json:"last_calibrated_at,omitempty"`
	Conversations    int       `json:"conversations"`
	LastError        string    `json:"last_error,omitempty"`
}

// SeqWatcher polls run_id and seq:ready, closing the gate and recalibrating
// counters whenever Redis restarts or the readiness marker is lost.
type SeqWatcher struct {
	client     redis.UniversalClient
	keyspace   Keyspace
	gate       *SeqGate
	calibrator SeqCalibrationRunner
	runIDs     SeqRunIDSource
	interval   time.Duration
	backoff    time.Duration
	maxBackoff time.Duration
	notify     chan struct{}

	mu               sync.Mutex
	initialized      bool
	lastRunID        string
	calibrating      bool
	waitingReady     bool
	maintenance      bool
	lastCalibratedAt time.Time
	conversations    int
	lastError        string
}

func NewSeqWatcher(
	client redis.UniversalClient,
	options SeqWatcherOptions,
) *SeqWatcher {
	if options.Interval <= 0 {
		options.Interval = defaultSeqWatchInterval
	}
	if options.Backoff <= 0 {
		options.Backoff = defaultSeqWatchBackoff
	}
	if options.MaxBackoff < options.Backoff {
		options.MaxBackoff = defaultSeqWatchMaxBackoff
	}
	runIDs := options.RunIDs
	if runIDs == nil {
		runIDs = redisRunIDSource{client: client}
	}
	return &SeqWatcher{
		client:           client,
		keyspace:         options.Keyspace,
		gate:             options.Gate,
		calibrator:       options.Calibrator,
		runIDs:           runIDs,
		interval:         options.Interval,
		backoff:          options.Backoff,
		maxBackoff:       options.MaxBackoff,
		notify:           make(chan struct{}, 1),
		initialized:      options.InitialRunID != "",
		lastRunID:        options.InitialRunID,
		lastCalibratedAt: options.InitialCalibratedAt,
		conversations:    options.InitialConversations,
	}
}

// Run checks immediately and then periodically until the context is cancelled.
// When the caller primed InitialRunID the first check only records the current
// run_id; otherwise it calibrates once.
func (w *SeqWatcher) Run(ctx context.Context) {
	if w == nil || w.client == nil || w.calibrator == nil {
		return
	}
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.check(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.notify:
			w.check(ctx)
		case <-ticker.C:
			w.check(ctx)
		}
	}
}

// Notify asks the watcher to check immediately, for example after another
// component saw a Redis error.
func (w *SeqWatcher) Notify() {
	if w == nil {
		return
	}
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// Status returns the current gate state and the last calibration outcome.
func (w *SeqWatcher) Status() SeqWatcherStatus {
	if w == nil {
		return SeqWatcherStatus{Ready: true}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return SeqWatcherStatus{
		Ready:            w.gate.IsReady(),
		RunID:            w.lastRunID,
		Calibrating:      w.calibrating,
		WaitingSeqReady:  w.waitingReady,
		Maintenance:      w.maintenance,
		LastCalibratedAt: w.lastCalibratedAt,
		Conversations:    w.conversations,
		LastError:        w.lastError,
	}
}

func (w *SeqWatcher) check(ctx context.Context) {
	runID, err := w.runIDs.RunID(ctx)
	if err != nil {
		w.recordError(err)
		return
	}
	ready, err := w.sequenceReady(ctx)
	if err != nil {
		w.recordError(err)
		return
	}
	maintenance, err := w.maintenanceRunning(ctx)
	if err != nil {
		w.recordError(err)
		return
	}

	w.mu.Lock()
	first := !w.initialized
	changed := first || runID != w.lastRunID
	leftMaintenance := w.maintenance && !maintenance
	w.initialized = true
	w.lastRunID = runID
	w.maintenance = maintenance
	waiting := w.waitingReady
	if maintenance {
		w.waitingReady = false
	}
	w.mu.Unlock()

	if maintenance {
		// A rebuild or seq backfill owns the namespace: keep writing closed and
		// never calibrate against a projection that is being rebuilt. The pair
		// of locks is the same exclusion the backend applies at startup.
		w.gate.Close()
		return
	}
	switch {
	case changed, leftMaintenance:
	case !ready && !waiting:
	case ready && waiting:
	default:
		return
	}
	w.recalibrate(ctx)
}

func (w *SeqWatcher) recalibrate(ctx context.Context) {
	w.gate.Close()
	w.mu.Lock()
	w.calibrating = true
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.calibrating = false
		w.mu.Unlock()
	}()
	backoff := w.backoff
	for {
		conversations, err := w.calibrator.Calibrate(ctx)
		if err == nil {
			w.mu.Lock()
			w.lastCalibratedAt = time.Now().UTC()
			w.conversations = conversations
			w.lastError = ""
			w.mu.Unlock()
			break
		}
		w.recordError(err)
		if !waitContext(ctx, backoff) {
			return
		}
		backoff *= 2
		if backoff > w.maxBackoff {
			backoff = w.maxBackoff
		}
	}

	// A single failed readiness probe must not leave the gate closed forever:
	// keep retrying so a transient Redis error resolves without waiting for a
	// run_id change or a lost readiness marker.
	backoff = w.backoff
	for {
		ready, err := w.sequenceReady(ctx)
		if err == nil {
			w.mu.Lock()
			w.waitingReady = !ready
			w.lastError = ""
			w.mu.Unlock()
			if ready {
				w.gate.Open()
			}
			return
		}
		w.recordError(err)
		if !waitContext(ctx, backoff) {
			return
		}
		backoff *= 2
		if backoff > w.maxBackoff {
			backoff = w.maxBackoff
		}
	}
}

func (w *SeqWatcher) sequenceReady(ctx context.Context) (bool, error) {
	exists, err := w.client.Exists(ctx, w.keyspace.sequenceReadyKey()).Result()
	if err != nil {
		return false, fmt.Errorf("read sequence readiness: %w", err)
	}
	return exists > 0, nil
}

// maintenanceRunning reports whether a rebuild or seq backfill holds its lock.
// Either command rebuilds counters under a live namespace, so the gate must
// stay closed for as long as it might be running.
func (w *SeqWatcher) maintenanceRunning(ctx context.Context) (bool, error) {
	exists, err := w.client.Exists(
		ctx, RebuildLockKey(w.keyspace), w.keyspace.backfillLockKey(),
	).Result()
	if err != nil {
		return false, fmt.Errorf("read maintenance locks: %w", err)
	}
	return exists > 0, nil
}

func (w *SeqWatcher) recordError(err error) {
	w.mu.Lock()
	w.lastError = err.Error()
	w.mu.Unlock()
}
