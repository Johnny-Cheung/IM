package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
)

const (
	defaultQueueDrainTimeout = 10 * time.Minute
	defaultQueueDrainPoll    = 500 * time.Millisecond
	// queueDrainStablePolls is how many consecutive clean polls end a drain.
	queueDrainStablePolls = 2
	// queueDrainStopGrace bounds the wait for consumers to acknowledge the
	// cancel. A delivery still unacknowledged is requeued by the broker and is
	// caught by the post-stop depth re-inspection.
	queueDrainStopGrace = 15 * time.Second
	// queueInspectTimeout bounds one passive declare so a wedged broker cannot
	// block the drain past the rebuild context.
	queueInspectTimeout = 10 * time.Second
)

// QueueDrainOptions configures draining the persist queues before a rebuild.
// The group queue is always inspected; GroupPersister is required as soon as it
// holds a message.
type QueueDrainOptions struct {
	Namespace       Namespace
	Rabbit          config.RabbitMQConfig
	Dialer          RabbitDialer
	DirectPersister MessagePersister
	GroupPersister  GroupMessagePersister
	// Timeout aborts the drain when the queues are still busy.
	Timeout time.Duration
	// PollEvery is the queue inspection interval.
	PollEvery time.Duration
	// Now is the clock the timeout deadline is measured against; tests inject
	// it. Nil means time.Now.
	Now  func() time.Time
	Logf func(string, ...interface{})
}

// drainObservation is one poll of both persist queues and their consumers.
// Backlog counts ready messages only: AMQP queue.declare-ok does not report
// unacknowledged deliveries, so the consumers' InFlight counters cover the
// deliveries they still hold.
type drainObservation struct {
	directBacklog      int
	groupBacklog       int
	directInFlight     int
	groupInFlight      int
	directPersisted    uint64
	groupPersisted     uint64
	directRetrying     bool
	groupRetrying      bool
	directDeadLettered uint64
	groupDeadLettered  uint64
}

// drainProgress tracks whether both queues stayed empty while consumers were
// idle. The persisted counters are compared poll to poll so a message that is
// acknowledged between two inspections still extends the drain.
type drainProgress struct {
	directPersisted uint64
	groupPersisted  uint64
	stable          int
	initialized     bool
}

// observe records one observation and reports whether the queues have been
// quiet for stablePolls consecutive polls.
func (p *drainProgress) observe(observation drainObservation, stablePolls int) bool {
	advanced := p.initialized &&
		(observation.directPersisted != p.directPersisted ||
			observation.groupPersisted != p.groupPersisted)
	p.directPersisted = observation.directPersisted
	p.groupPersisted = observation.groupPersisted
	p.initialized = true
	switch {
	case observation.directBacklog > 0,
		observation.groupBacklog > 0,
		observation.directInFlight > 0,
		observation.groupInFlight > 0,
		observation.directRetrying,
		observation.groupRetrying,
		advanced:
		p.stable = 0
		return false
	}
	p.stable++
	return p.stable >= stablePolls
}

// drainLoop is the broker-free polling core of DrainPersistQueues.
type drainLoop struct {
	pollEvery   time.Duration
	stablePolls int
	// timeout is a wall-clock budget, not a poll count: a slow inspect must not
	// stretch the drain far past it. Zero means no deadline.
	timeout time.Duration
	// now is the clock used for the deadline; tests inject it.
	now     func() time.Time
	observe func(context.Context) (drainObservation, error)
	wait    func(context.Context, time.Duration) bool
}

// run polls until both queues are quiet for the stable window, the deadline
// passes, or the context is canceled. The result carries the counters of the
// last observation in every case.
func (l drainLoop) run(ctx context.Context) (QueueDrainResult, error) {
	now := l.now
	if now == nil {
		now = time.Now
	}
	var deadline time.Time
	if l.timeout > 0 {
		deadline = now().Add(l.timeout)
	}
	var (
		result      QueueDrainResult
		progress    drainProgress
		observation drainObservation
	)
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		next, err := l.observe(ctx)
		if err != nil {
			return result, err
		}
		observation = next
		result = drainResultOf(observation)
		if progress.observe(observation, l.stablePolls) {
			return result, nil
		}
		if !deadline.IsZero() && !now().Before(deadline) {
			return result, fmt.Errorf(
				"persist queues did not drain within %s: direct_ready=%d "+
					"group_ready=%d direct_in_flight=%d group_in_flight=%d "+
					"direct_persisted=%d group_persisted=%d "+
					"direct_retrying=%t group_retrying=%t",
				l.timeout,
				observation.directBacklog,
				observation.groupBacklog,
				observation.directInFlight,
				observation.groupInFlight,
				observation.directPersisted,
				observation.groupPersisted,
				observation.directRetrying,
				observation.groupRetrying,
			)
		}
		waitFor := l.pollEvery
		if !deadline.IsZero() {
			if remaining := deadline.Sub(now()); remaining < waitFor {
				waitFor = remaining
			}
		}
		if !l.wait(ctx, waitFor) {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			return result, fmt.Errorf("persist queue drain stopped before the queues emptied")
		}
	}
}

func drainResultOf(observation drainObservation) QueueDrainResult {
	return QueueDrainResult{
		Direct:       int(observation.directPersisted),
		Group:        int(observation.groupPersisted),
		DeadLettered: int(observation.directDeadLettered + observation.groupDeadLettered),
	}
}

// DrainPersistQueues runs the direct consumer, and the group consumer when a
// group persister is configured, until both persist queues are empty and idle,
// then stops them. It never publishes and leaves dead-letter handling to the
// consumers.
func DrainPersistQueues(
	ctx context.Context,
	options QueueDrainOptions,
) (QueueDrainResult, error) {
	result := QueueDrainResult{}
	if options.Dialer == nil || options.DirectPersister == nil {
		return result, fmt.Errorf("persist queue drain requires a RabbitMQ dialer and a direct persister")
	}
	if !options.Rabbit.Enabled() {
		return result, fmt.Errorf("persist queue drain requires RabbitMQ configuration")
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultQueueDrainTimeout
	}
	pollEvery := options.PollEvery
	if pollEvery <= 0 {
		pollEvery = defaultQueueDrainPoll
	}
	logf := options.Logf
	if logf == nil {
		logf = log.Printf
	}
	namespace := options.Namespace

	directConsumer := NewRabbitMessageConsumerWithNamespace(
		options.Rabbit, options.Dialer, options.DirectPersister, namespace,
	)
	var groupConsumer *GroupMessageConsumer
	if options.GroupPersister != nil {
		groupConsumer = NewGroupMessageConsumer(
			options.Rabbit, options.Dialer, options.GroupPersister, namespace,
		)
	}
	directQueue := directQueueName(namespace)
	// The group queue is inspected even without a group persister: a rebuild
	// must never clear a namespace while group deliveries are still waiting.
	groupQueue := groupQueueName(namespace)

	inspector := newQueueDepthInspector(options.Rabbit, options.Dialer)
	defer inspector.Close()

	consumerCtx, stopConsumers := context.WithCancel(ctx)
	defer stopConsumers()

	consumersDone := make(chan struct{})
	var consumers sync.WaitGroup
	startConsumer := func(run func(context.Context) error) {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			if err := run(consumerCtx); err != nil && consumerCtx.Err() == nil {
				logf("persist queue drain consumer stopped: %v", err)
			}
		}()
	}
	startConsumer(directConsumer.Run)
	if groupConsumer != nil {
		startConsumer(groupConsumer.Run)
	}
	go func() {
		consumers.Wait()
		close(consumersDone)
	}()

	logf(
		"draining persist queues direct=%s group=%s timeout=%s",
		directQueue, groupQueue, timeout,
	)

	observe := func(ctx context.Context) (drainObservation, error) {
		if err := ctx.Err(); err != nil {
			return drainObservation{}, err
		}
		directBacklog, err := inspector.Depth(ctx, directQueue)
		if err != nil {
			return drainObservation{}, err
		}
		groupBacklog, err := inspector.Depth(ctx, groupQueue)
		if err != nil {
			return drainObservation{}, err
		}
		if err := requireGroupPersister(
			groupQueue, groupBacklog, groupConsumer != nil,
		); err != nil {
			return drainObservation{}, err
		}
		directStatus := directConsumer.Status()
		groupStatus := ConsumerStatus{}
		if groupConsumer != nil {
			groupStatus = groupConsumer.Status()
		}
		return drainObservation{
			directBacklog:      directBacklog,
			groupBacklog:       groupBacklog,
			directInFlight:     directStatus.InFlight,
			groupInFlight:      groupStatus.InFlight,
			directPersisted:    directStatus.Persisted,
			groupPersisted:     groupStatus.Persisted,
			directRetrying:     directStatus.Retrying,
			groupRetrying:      groupStatus.Retrying,
			directDeadLettered: directStatus.DeadLettered,
			groupDeadLettered:  groupStatus.DeadLettered,
		}, nil
	}

	loop := drainLoop{
		pollEvery:   pollEvery,
		stablePolls: queueDrainStablePolls,
		timeout:     timeout,
		now:         options.Now,
		observe:     observe,
		wait:        waitMessaging,
	}
	drained, err := loop.run(ctx)
	stopConsumers()
	if err != nil {
		return drained, err
	}
	if err := waitConsumers(consumersDone, queueDrainStopGrace); err != nil {
		return drained, err
	}
	// Stopping the consumers cancels anything still in flight, and the broker
	// requeues an unacknowledged delivery when the channel closes. The stable
	// window can therefore have observed an empty queue that is no longer empty:
	// re-inspect both depths before the caller is allowed to clear the
	// namespace.
	if err := verifyQueuesEmpty(ctx, inspector, directQueue, groupQueue); err != nil {
		return drained, err
	}
	logf(
		"persist queues drained direct=%d group=%d dead_lettered=%d",
		drained.Direct, drained.Group, drained.DeadLettered,
	)
	return drained, nil
}

// requireGroupPersister refuses a non-empty group queue that this drain cannot
// consume: the rebuild must never clear group data it did not drain.
func requireGroupPersister(queue string, backlog int, configured bool) error {
	if configured || backlog <= 0 {
		return nil
	}
	return fmt.Errorf(
		"group persist queue %s holds %d message(s) but no group persister "+
			"is configured; refusing to clear the namespace",
		queue, backlog,
	)
}

// queueDepthReader is the part of queueDepthInspector the drain needs.
type queueDepthReader interface {
	Depth(ctx context.Context, queue string) (int, error)
}

// verifyQueuesEmpty refuses the drain when a queue received a message after
// the consumers stopped. The AMQP channel close that requeues an unacked
// delivery completes before the consumer goroutine returns, so the depths read
// here are those the rebuild would otherwise wipe.
func verifyQueuesEmpty(
	ctx context.Context,
	inspector queueDepthReader,
	directQueue, groupQueue string,
) error {
	for _, queue := range []string{directQueue, groupQueue} {
		depth, err := inspector.Depth(ctx, queue)
		if err != nil {
			return fmt.Errorf("re-inspect queue %s after the drain: %w", queue, err)
		}
		if depth > 0 {
			return fmt.Errorf(
				"persist queue %s still holds %d message(s) after the drain stopped; "+
					"refusing to clear the namespace",
				queue, depth,
			)
		}
	}
	return nil
}

func waitConsumers(done <-chan struct{}, grace time.Duration) error {
	if grace <= 0 {
		grace = queueDrainStopGrace
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("persist queue consumers did not stop within %s", grace)
	}
}

// queueDepthInspector reads queue depth over its own connection so a consumer
// reconnect cannot break the drain check.
type queueDepthInspector struct {
	rabbit     config.RabbitMQConfig
	dialer     RabbitDialer
	connection *amqp091.Connection
	channel    *amqp091.Channel
}

func newQueueDepthInspector(
	rabbit config.RabbitMQConfig,
	dialer RabbitDialer,
) *queueDepthInspector {
	return &queueDepthInspector{rabbit: rabbit, dialer: dialer}
}

// Depth passively declares the queue and returns its ready message count. A
// missing queue is empty: the consumer declares it when it connects.
func (i *queueDepthInspector) Depth(ctx context.Context, queue string) (int, error) {
	if i == nil {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if i.channel == nil {
		if err := i.connect(); err != nil {
			return 0, err
		}
	}
	// AMQP calls do not take a context, so a stuck declare runs in a goroutine
	// and the connection is closed to release it on timeout or cancellation.
	type declareResult struct {
		declared amqp091.Queue
		err      error
	}
	channel := i.channel
	results := make(chan declareResult, 1)
	go func() {
		declared, err := channel.QueueDeclarePassive(queue, true, false, false, false, nil)
		results <- declareResult{declared: declared, err: err}
	}()
	timer := time.NewTimer(queueInspectTimeout)
	defer timer.Stop()
	select {
	case result := <-results:
		if result.err != nil {
			// The broker closes the channel on any declare failure.
			i.resetChannel()
			if isQueueMissing(result.err) {
				return 0, nil
			}
			return 0, fmt.Errorf("inspect RabbitMQ queue %s: %w", queue, result.err)
		}
		return result.declared.Messages, nil
	case <-ctx.Done():
		i.Close()
		return 0, ctx.Err()
	case <-timer.C:
		i.Close()
		return 0, fmt.Errorf("inspecting RabbitMQ queue %s timed out", queue)
	}
}

func (i *queueDepthInspector) connect() error {
	connection, err := i.dialer(i.rabbit)
	if err != nil {
		return fmt.Errorf("dial RabbitMQ for queue inspection: %w", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return fmt.Errorf("open RabbitMQ queue inspection channel: %w", err)
	}
	i.connection = connection
	i.channel = channel
	return nil
}

func (i *queueDepthInspector) resetChannel() {
	if i.channel != nil {
		_ = i.channel.Close()
		i.channel = nil
	}
}

// Close releases the inspection connection.
func (i *queueDepthInspector) Close() {
	if i == nil {
		return
	}
	i.resetChannel()
	if i.connection != nil {
		_ = i.connection.Close()
		i.connection = nil
	}
}

func isQueueMissing(err error) bool {
	var amqpErr *amqp091.Error
	return errors.As(err, &amqpErr) && amqpErr.Code == amqp091.NotFound
}
