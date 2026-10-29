package messaging

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	monitorDefaultInterval   = 10 * time.Second
	monitorDefaultBacklog    = 1000
	monitorDefaultOldestAge  = 60 * time.Second
	monitorOldestEntryCount  = 1
	monitorMissingGroupError = "NOGROUP"
)

// PipelineChainStatus is the stream side of one message chain. Counts are read
// only; the monitor never acknowledges or deletes stream entries.
type PipelineChainStatus struct {
	StreamLength     uint64 `json:"stream_length"`
	Pending          uint64 `json:"pending"`
	OldestAgeSeconds int64  `json:"oldest_age_seconds"`
	Unrelayed        uint64 `json:"unrelayed"`
	BacklogAlert     bool   `json:"backlog_alert"`
	// DeadLetterCount is null while the broker cannot be reached.
	DeadLetterCount *int64         `json:"dead_letter_count"`
	DeadLetterError string         `json:"dead_letter_error,omitempty"`
	Relay           RelayStatus    `json:"relay"`
	Consumer        ConsumerStatus `json:"consumer"`
	LastError       string         `json:"last_error,omitempty"`
}

// PipelineStatus is the message pipeline section of /ops/infra.
type PipelineStatus struct {
	Direct    PipelineChainStatus  `json:"direct"`
	Group     *PipelineChainStatus `json:"group,omitempty"`
	Reconcile ReconcileStatus      `json:"reconcile"`
	SampledAt time.Time            `json:"sampled_at,omitempty"`
	Samples   uint64               `json:"samples"`
	LastError string               `json:"last_error,omitempty"`
}

// PipelineDeadLetterCounter reports dead-letter queue depth without consuming.
type PipelineDeadLetterCounter interface {
	CountDeadLetters(ctx context.Context, kind DeadLetterKind) (int64, error)
}

// PipelineMonitorOptions configures the sampler. The per-chain status
// functions are optional; a nil GroupRelay and GroupConsumer disable the group
// chain.
type PipelineMonitorOptions struct {
	Keyspace  Keyspace
	Namespace Namespace
	// Interval between samples. Default 10s.
	Interval time.Duration
	// BacklogLimit and OldestLimit are the two backlog alert thresholds.
	BacklogLimit uint64
	OldestLimit  time.Duration
	Now          func() time.Time
	// Logf receives alert flips and dead-letter increases. Default log.Printf.
	Logf func(format string, args ...interface{})
	// DeadLetters overrides the RabbitMQ queue-depth source.
	DeadLetters PipelineDeadLetterCounter

	DirectRelay    func() RelayStatus
	DirectConsumer func() ConsumerStatus
	GroupRelay     func() RelayStatus
	GroupConsumer  func() ConsumerStatus
	Reconcile      func() ReconcileStatus
}

// monitorChain is one chain's read-only sampling target.
type monitorChain struct {
	kind     DeadLetterKind
	stream   string
	group    string
	relay    func() RelayStatus
	consumer func() ConsumerStatus
}

// PipelineMonitor samples the message streams, relays, consumers and
// dead-letter queues so /ops/infra can serve the last snapshot cheaply.
type PipelineMonitor struct {
	redis       redis.UniversalClient
	chains      []monitorChain
	deadLetters PipelineDeadLetterCounter
	reconcile   func() ReconcileStatus
	interval    time.Duration
	backlog     uint64
	oldest      time.Duration
	now         func() time.Time
	logf        func(format string, args ...interface{})

	// Sampling runs on one goroutine; alerts and deadTotals are only touched
	// there. mu guards the published snapshot for concurrent Status readers.
	alerts     map[DeadLetterKind]bool
	deadTotals map[DeadLetterKind]int64

	mu     sync.Mutex
	status PipelineStatus
}

// NewPipelineMonitor builds a sampler for both chains. The RabbitMQ dialer
// backs the dead-letter queue count; pass nil only in tests that inject
// DeadLetters.
func NewPipelineMonitor(
	client redis.UniversalClient,
	rabbitConfig config.RabbitMQConfig,
	dialer RabbitDialer,
	options PipelineMonitorOptions,
) *PipelineMonitor {
	if options.Interval <= 0 {
		options.Interval = monitorDefaultInterval
	}
	if options.BacklogLimit == 0 {
		options.BacklogLimit = monitorDefaultBacklog
	}
	if options.OldestLimit <= 0 {
		options.OldestLimit = monitorDefaultOldestAge
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Logf == nil {
		options.Logf = log.Printf
	}

	monitor := &PipelineMonitor{
		redis:      client,
		reconcile:  options.Reconcile,
		interval:   options.Interval,
		backlog:    options.BacklogLimit,
		oldest:     options.OldestLimit,
		now:        options.Now,
		logf:       options.Logf,
		alerts:     make(map[DeadLetterKind]bool, 2),
		deadTotals: make(map[DeadLetterKind]int64, 2),
	}
	if options.DeadLetters != nil {
		monitor.deadLetters = options.DeadLetters
	} else if dialer != nil {
		monitor.deadLetters = monitorAMQPDeadLetterCounter{
			config:    rabbitConfig,
			dialer:    dialer,
			namespace: options.Namespace,
		}
	}

	monitor.chains = append(monitor.chains, monitorChain{
		kind:     DeadLetterDirect,
		stream:   options.Keyspace.messageStreamKey(),
		group:    directRelayGroup(options.Namespace),
		relay:    options.DirectRelay,
		consumer: options.DirectConsumer,
	})
	if options.GroupRelay != nil || options.GroupConsumer != nil {
		monitor.chains = append(monitor.chains, monitorChain{
			kind:     DeadLetterGroup,
			stream:   options.Keyspace.groupMessageStreamKey(),
			group:    groupRelayName(options.Namespace),
			relay:    options.GroupRelay,
			consumer: options.GroupConsumer,
		})
	}
	monitor.status = monitor.emptyStatus()
	return monitor
}

// Run samples once immediately, then every Interval until the context ends.
func (m *PipelineMonitor) Run(ctx context.Context) {
	if m == nil || m.redis == nil {
		return
	}
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		m.sampleOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Status returns the last sample, safe for concurrent readers.
func (m *PipelineMonitor) Status() PipelineStatus {
	if m == nil {
		return PipelineStatus{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	status := m.status
	if m.status.Group != nil {
		group := *m.status.Group
		status.Group = &group
	}
	return status
}

func (m *PipelineMonitor) emptyStatus() PipelineStatus {
	status := PipelineStatus{}
	for _, chain := range m.chains {
		chainStatus := PipelineChainStatus{
			Relay:    monitorRelayStatus(chain.relay),
			Consumer: monitorConsumerStatus(chain.consumer),
		}
		if chain.kind == DeadLetterGroup {
			status.Group = &chainStatus
		} else {
			status.Direct = chainStatus
		}
	}
	return status
}

func monitorRelayStatus(source func() RelayStatus) RelayStatus {
	if source == nil {
		return RelayStatus{}
	}
	return source()
}

func monitorConsumerStatus(source func() ConsumerStatus) ConsumerStatus {
	if source == nil {
		return ConsumerStatus{}
	}
	return source()
}

func (m *PipelineMonitor) sampleOnce(ctx context.Context) {
	status := PipelineStatus{
		SampledAt: m.now().UTC(),
		Direct:    PipelineChainStatus{},
	}
	if m.reconcile != nil {
		status.Reconcile = m.reconcile()
	}
	sampled := make([]monitorSample, 0, len(m.chains))
	for _, chain := range m.chains {
		chainStatus := m.sampleChain(ctx, chain)
		if chainStatus.LastError != "" {
			status.LastError = chainStatus.LastError
		}
		sampled = append(sampled, monitorSample{chain: chain, status: chainStatus})
		if chain.kind == DeadLetterGroup {
			group := chainStatus
			status.Group = &group
		} else {
			status.Direct = chainStatus
		}
	}
	m.trackAlerts(sampled)

	m.mu.Lock()
	status.Samples = m.status.Samples + 1
	m.status = status
	m.mu.Unlock()
}

type monitorSample struct {
	chain  monitorChain
	status PipelineChainStatus
}

func (m *PipelineMonitor) sampleChain(ctx context.Context, chain monitorChain) PipelineChainStatus {
	status := PipelineChainStatus{
		Relay:    monitorRelayStatus(chain.relay),
		Consumer: monitorConsumerStatus(chain.consumer),
	}
	sampleErr := error(nil)
	oldestAge := time.Duration(0)
	if m.redis == nil {
		sampleErr = fmt.Errorf("redis client is not configured")
	} else {
		oldestAge, sampleErr = m.sampleStream(ctx, chain, &status)
	}
	if sampleErr != nil {
		status.LastError = sampleErr.Error()
		// A Redis outage says nothing about the stream; keep the last alert
		// decision so a transient error does not fake a recovery.
		status.BacklogAlert = m.previousChainStatus(chain.kind).BacklogAlert
	} else {
		status.BacklogAlert = status.Unrelayed > m.backlog || oldestAge > m.oldest
	}
	m.sampleDeadLetters(ctx, chain, &status)
	return status
}

// sampleStream reads the stream counters. Individual failures do not discard
// the values read by the other commands. The oldest age is returned exactly
// for the alert threshold while the status keeps whole seconds.
func (m *PipelineMonitor) sampleStream(
	ctx context.Context,
	chain monitorChain,
	status *PipelineChainStatus,
) (time.Duration, error) {
	var oldestAge time.Duration
	var sampleErr error
	record := func(err error) {
		if err != nil && sampleErr == nil {
			sampleErr = err
		}
	}

	length, err := m.redis.XLen(ctx, chain.stream).Result()
	if err != nil {
		record(fmt.Errorf("read %s stream length: %w", chain.kind, err))
	} else {
		status.StreamLength = uint64(length)
		// The relay deletes entries after acknowledging them, so the stream
		// length is exactly what has not reached MySQL yet.
		status.Unrelayed = status.StreamLength
	}

	pending, err := m.pendingCount(ctx, chain)
	if err != nil {
		record(err)
	} else {
		status.Pending = pending
	}

	oldest, err := m.oldestEntryTime(ctx, chain.stream)
	if err != nil {
		record(fmt.Errorf("read oldest %s stream entry: %w", chain.kind, err))
	} else if !oldest.IsZero() {
		oldestAge = m.now().UTC().Sub(oldest)
		if oldestAge < 0 {
			oldestAge = 0
		}
		status.OldestAgeSeconds = int64(oldestAge / time.Second)
	}
	return oldestAge, sampleErr
}

func (m *PipelineMonitor) pendingCount(ctx context.Context, chain monitorChain) (uint64, error) {
	pending, err := m.redis.XPending(ctx, chain.stream, chain.group).Result()
	if err != nil {
		// Before the relay creates its group there is nothing pending. The
		// monitor must not create it, so treat NOGROUP as zero.
		if strings.Contains(strings.ToUpper(err.Error()), monitorMissingGroupError) {
			return 0, nil
		}
		return 0, fmt.Errorf("read %s pending count: %w", chain.kind, err)
	}
	if pending.Count < 0 {
		return 0, nil
	}
	return uint64(pending.Count), nil
}

func (m *PipelineMonitor) oldestEntryTime(ctx context.Context, stream string) (time.Time, error) {
	entries, err := m.redis.XRangeN(ctx, stream, "-", "+", monitorOldestEntryCount).Result()
	if err != nil {
		return time.Time{}, err
	}
	if len(entries) == 0 {
		return time.Time{}, nil
	}
	return monitorStreamEntryTime(entries[0].ID)
}

// monitorStreamEntryTime decodes the millisecond part of a Redis Stream ID.
func monitorStreamEntryTime(id string) (time.Time, error) {
	milliseconds, _, _ := strings.Cut(id, "-")
	parsed, err := strconv.ParseInt(milliseconds, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse stream entry id %q: %w", id, err)
	}
	return time.UnixMilli(parsed).UTC(), nil
}

func (m *PipelineMonitor) sampleDeadLetters(
	ctx context.Context,
	chain monitorChain,
	status *PipelineChainStatus,
) {
	if m.deadLetters == nil {
		return
	}
	count, err := m.deadLetters.CountDeadLetters(ctx, chain.kind)
	if err != nil {
		status.DeadLetterError = err.Error()
		status.DeadLetterCount = nil
		return
	}
	status.DeadLetterCount = &count
}

func (m *PipelineMonitor) previousChainStatus(kind DeadLetterKind) PipelineChainStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	if kind == DeadLetterGroup && m.status.Group != nil {
		return *m.status.Group
	}
	return m.status.Direct
}

// trackAlerts logs backlog flips and dead-letter increases once per change.
func (m *PipelineMonitor) trackAlerts(sampled []monitorSample) {
	for _, sample := range sampled {
		kind := sample.chain.kind
		status := sample.status
		if status.BacklogAlert != m.alerts[kind] {
			if status.BacklogAlert {
				m.logf(
					"stream backlog alert chain=%s unrelayed=%d pending=%d oldest_age_seconds=%d",
					kind, status.Unrelayed, status.Pending, status.OldestAgeSeconds,
				)
			} else {
				m.logf(
					"stream backlog recovered chain=%s unrelayed=%d pending=%d",
					kind, status.Unrelayed, status.Pending,
				)
			}
			m.alerts[kind] = status.BacklogAlert
		}
		if status.DeadLetterCount == nil {
			continue
		}
		if previous, seen := m.deadTotals[kind]; seen && *status.DeadLetterCount > previous {
			m.logf("dead letters increased chain=%s total=%d", kind, *status.DeadLetterCount)
		}
		m.deadTotals[kind] = *status.DeadLetterCount
	}
}

// monitorAMQPDeadLetterCounter reads a queue depth with a passive declare on a
// short-lived connection. A passive declare never creates topology.
type monitorAMQPDeadLetterCounter struct {
	config    config.RabbitMQConfig
	dialer    RabbitDialer
	namespace Namespace
}

func (c monitorAMQPDeadLetterCounter) CountDeadLetters(
	_ context.Context,
	kind DeadLetterKind,
) (int64, error) {
	topology := DeadLetterTopologyFor(c.namespace, kind)
	connection, err := c.dialer(c.config)
	if err != nil {
		return 0, fmt.Errorf("dial RabbitMQ for %s dead letter count: %w", kind, err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return 0, fmt.Errorf("open RabbitMQ channel for %s dead letter count: %w", kind, err)
	}
	defer channel.Close()

	queue, err := channel.QueueDeclarePassive(topology.Queue, true, false, false, false, nil)
	if err != nil {
		return 0, fmt.Errorf("inspect %s dead letter queue: %w", kind, err)
	}
	return int64(queue.Messages), nil
}
