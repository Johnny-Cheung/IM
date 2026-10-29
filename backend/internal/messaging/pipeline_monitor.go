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

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

const (
	monitorDefaultInterval        = 10 * time.Second
	monitorDefaultBacklog         = 1000
	monitorDefaultOldestAge       = 60 * time.Second
	monitorOldestEntryCount       = 1
	monitorMissingGroupError      = "NOGROUP"
	monitorDeadLetterProbeTimeout = 5 * time.Second
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

	// amqpDeadLetters is owned here so Run can close its cached connection.
	amqpDeadLetters *monitorAMQPDeadLetterCounter

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
		counter := &monitorAMQPDeadLetterCounter{
			config:    rabbitConfig,
			dialer:    dialer,
			namespace: options.Namespace,
		}
		monitor.deadLetters = counter
		monitor.amqpDeadLetters = counter
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
	defer m.closeAMQPDeadLetters()
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

func (m *PipelineMonitor) closeAMQPDeadLetters() {
	if m.amqpDeadLetters != nil {
		m.amqpDeadLetters.Close()
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
	previous := m.previousChainStatus(chain.kind).BacklogAlert
	var oldestAge time.Duration
	var lengthRead, oldestRead bool
	var sampleErr error
	if m.redis == nil {
		sampleErr = fmt.Errorf("redis client is not configured")
	} else {
		oldestAge, lengthRead, oldestRead, sampleErr = m.sampleStream(ctx, chain, &status)
	}
	if sampleErr != nil {
		status.LastError = sampleErr.Error()
	}
	// Each alert input keeps its own validity: a leg that failed inherits the
	// previous decision while a leg that was read is evaluated fresh, so one
	// broken command cannot suppress a backlog the others proved.
	alert := false
	if lengthRead {
		alert = status.Unrelayed > m.backlog
	} else {
		alert = previous
	}
	if oldestRead {
		alert = alert || oldestAge > m.oldest
	} else {
		alert = alert || previous
	}
	status.BacklogAlert = alert
	m.sampleDeadLetters(ctx, chain, &status)
	return status
}

// sampleStream reads the stream counters. Individual failures do not discard
// the values read by the other commands; the two alert inputs report whether
// they were actually read. The oldest age is returned exactly for the alert
// threshold while the status keeps whole seconds.
func (m *PipelineMonitor) sampleStream(
	ctx context.Context,
	chain monitorChain,
	status *PipelineChainStatus,
) (oldestAge time.Duration, lengthRead, oldestRead bool, sampleErr error) {
	record := func(err error) {
		if err != nil && sampleErr == nil {
			sampleErr = err
		}
	}

	length, err := m.redis.XLen(ctx, chain.stream).Result()
	if err != nil {
		record(fmt.Errorf("read %s stream length: %w", chain.kind, err))
	} else {
		lengthRead = true
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
	} else {
		oldestRead = true
		if !oldest.IsZero() {
			oldestAge = m.now().UTC().Sub(oldest)
			if oldestAge < 0 {
				oldestAge = 0
			}
			status.OldestAgeSeconds = int64(oldestAge / time.Second)
		}
	}
	return oldestAge, lengthRead, oldestRead, sampleErr
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

// monitorAMQPProber runs passive declares over one long-lived connection and
// channel. A passive declare never creates topology.
type monitorAMQPProber interface {
	QueueDepth(queue string) (int64, error)
	Close() error
}

// monitorAMQPConnector is the connection surface the monitor needs. It keeps
// the dial and channel-open legs testable without a broker.
type monitorAMQPConnector interface {
	Channel() (*amqp091.Channel, error)
	CloseDeadline(time.Time) error
}

// monitorAMQPLink is the amqp091-backed prober.
type monitorAMQPLink struct {
	connection monitorAMQPConnector
	channel    *amqp091.Channel
}

// monitorOpenAMQPLink bounds the dial and the channel-open RPC. amqp091 offers
// no context or per-call deadline for either, so both run in a goroutine and
// are abandoned with a bounded close when the context ends.
func monitorOpenAMQPLink(
	ctx context.Context,
	dialer RabbitDialer,
	cfg config.RabbitMQConfig,
) (*monitorAMQPLink, error) {
	openCtx, cancel := context.WithTimeout(ctx, monitorDeadLetterProbeTimeout)
	defer cancel()
	return monitorOpenAMQPLinkContext(openCtx, func() (monitorAMQPConnector, error) {
		return dialer(cfg)
	})
}

func monitorOpenAMQPLinkContext(
	ctx context.Context,
	connect func() (monitorAMQPConnector, error),
) (*monitorAMQPLink, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	connection, err := monitorDialAMQP(ctx, connect)
	if err != nil {
		return nil, err
	}
	channel, err := monitorOpenChannel(ctx, connection)
	if err != nil {
		return nil, err
	}
	return &monitorAMQPLink{connection: connection, channel: channel}, nil
}

// monitorDialAMQP runs a context-less dialer in its own goroutine and bounds
// the wait. A connection that arrives after the caller gave up is closed so it
// cannot leak; production dialers already carry their own dial timeout.
func monitorDialAMQP(
	ctx context.Context,
	connect func() (monitorAMQPConnector, error),
) (monitorAMQPConnector, error) {
	type dialResult struct {
		connection monitorAMQPConnector
		err        error
	}
	done := make(chan dialResult, 1)
	go func() {
		connection, err := connect()
		done <- dialResult{connection: connection, err: err}
	}()
	select {
	case result := <-done:
		return result.connection, result.err
	case <-ctx.Done():
		go func() {
			result := <-done
			if result.connection != nil {
				_ = result.connection.CloseDeadline(
					time.Now().Add(monitorDeadLetterProbeTimeout),
				)
			}
		}()
		return nil, ctx.Err()
	}
}

// monitorOpenChannel bounds connection.Channel(), which has no deadline of its
// own. Closing the connection unblocks the abandoned goroutine.
func monitorOpenChannel(
	ctx context.Context,
	connection monitorAMQPConnector,
) (*amqp091.Channel, error) {
	type channelResult struct {
		channel *amqp091.Channel
		err     error
	}
	done := make(chan channelResult, 1)
	go func() {
		channel, err := connection.Channel()
		done <- channelResult{channel: channel, err: err}
	}()
	select {
	case result := <-done:
		if result.err != nil {
			_ = connection.CloseDeadline(time.Now().Add(monitorDeadLetterProbeTimeout))
			return nil, result.err
		}
		return result.channel, nil
	case <-ctx.Done():
		_ = connection.CloseDeadline(time.Now().Add(monitorDeadLetterProbeTimeout))
		return nil, ctx.Err()
	}
}

func (l *monitorAMQPLink) QueueDepth(queue string) (int64, error) {
	declared, err := l.channel.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}
	return int64(declared.Messages), nil
}

// Close bounds the handshake so a wedged broker cannot stall shutdown.
func (l *monitorAMQPLink) Close() error {
	return l.connection.CloseDeadline(time.Now().Add(monitorDeadLetterProbeTimeout))
}

var errMonitorDeadLetterClosed = errors.New("dead letter monitor is closed")

// monitorAMQPDeadLetterCounter reads queue depths over one connection held
// across samples: opened on first use and reopened after a probe failure.
type monitorAMQPDeadLetterCounter struct {
	config    config.RabbitMQConfig
	dialer    RabbitDialer
	namespace Namespace

	// open replaces the dialer in tests.
	open func(context.Context) (monitorAMQPProber, error)
	// probeContext bounds one declare; nil uses monitorDeadLetterProbeTimeout.
	probeContext func(context.Context) (context.Context, context.CancelFunc)

	mu     sync.Mutex
	prober monitorAMQPProber
	closed bool
}

func (c *monitorAMQPDeadLetterCounter) CountDeadLetters(
	ctx context.Context,
	kind DeadLetterKind,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	prober, err := c.acquireProber(ctx)
	if err != nil {
		return 0, fmt.Errorf("open RabbitMQ connection for %s dead letter count: %w", kind, err)
	}
	depth, err := c.probe(ctx, prober, DeadLetterTopologyFor(c.namespace, kind).Queue)
	if err != nil {
		// The channel is closed after a 404 or a timeout; redial next sample.
		c.discardProber(prober)
		return 0, fmt.Errorf("inspect %s dead letter queue: %w", kind, err)
	}
	return depth, nil
}

// acquireProber reuses the cached connection. The dial and channel-open run
// without the mutex, so a slow broker never blocks Close or a later sample
// behind an unbounded open.
func (c *monitorAMQPDeadLetterCounter) acquireProber(
	ctx context.Context,
) (monitorAMQPProber, error) {
	c.mu.Lock()
	prober := c.prober
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, errMonitorDeadLetterClosed
	}
	if prober != nil {
		return prober, nil
	}

	opened, err := c.openProber(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	closed = c.closed
	winner := c.prober
	if !closed && winner == nil {
		c.prober = opened
	}
	c.mu.Unlock()
	if closed {
		_ = opened.Close()
		return nil, errMonitorDeadLetterClosed
	}
	if winner != nil {
		_ = opened.Close()
		return winner, nil
	}
	return opened, nil
}

func (c *monitorAMQPDeadLetterCounter) openProber(
	ctx context.Context,
) (monitorAMQPProber, error) {
	if c.open != nil {
		return c.open(ctx)
	}
	return monitorOpenAMQPLink(ctx, c.dialer, c.config)
}

// discardProber forgets prober only if it is still the cached one. The close
// itself runs outside the mutex.
func (c *monitorAMQPDeadLetterCounter) discardProber(prober monitorAMQPProber) {
	c.mu.Lock()
	if c.prober != prober {
		c.mu.Unlock()
		return
	}
	c.prober = nil
	c.mu.Unlock()
	_ = prober.Close()
}

// probe bounds the declare itself because amqp091 offers no per-call
// deadline; a stuck probe is abandoned with its connection.
func (c *monitorAMQPDeadLetterCounter) probe(
	ctx context.Context,
	prober monitorAMQPProber,
	queue string,
) (int64, error) {
	probeCtx, cancel := c.newProbeContext(ctx)
	defer cancel()

	type probeResult struct {
		depth int64
		err   error
	}
	done := make(chan probeResult, 1)
	go func() {
		depth, err := prober.QueueDepth(queue)
		done <- probeResult{depth: depth, err: err}
	}()
	select {
	case result := <-done:
		return result.depth, result.err
	case <-probeCtx.Done():
		return 0, probeCtx.Err()
	}
}

func (c *monitorAMQPDeadLetterCounter) newProbeContext(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	if c.probeContext != nil {
		return c.probeContext(ctx)
	}
	return context.WithTimeout(ctx, monitorDeadLetterProbeTimeout)
}

// Close releases the cached connection on monitor shutdown. The network close
// runs outside the mutex so it cannot block on an in-flight open.
func (c *monitorAMQPDeadLetterCounter) Close() {
	c.mu.Lock()
	c.closed = true
	prober := c.prober
	c.prober = nil
	c.mu.Unlock()
	if prober != nil {
		_ = prober.Close()
	}
}
