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
	// cancel; a delivery in flight is retried by the broker when it expires.
	queueDrainStopGrace = 15 * time.Second
	// queueInspectTimeout bounds one passive declare so a wedged broker cannot
	// block the drain past the rebuild context.
	queueInspectTimeout = 10 * time.Second
)

// QueueDrainOptions configures draining the persist queues before a rebuild.
// The group chain is drained only when GroupPersister is set.
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
	Logf      func(string, ...interface{})
}

// drainObservation is one poll of both persist queues and their consumers.
// Backlog counts ready messages only: AMQP queue.declare-ok does not report
// unacknowledged deliveries, but those belong to our own consumers and are
// covered by the persisted stability window and the retrying flag.
type drainObservation struct {
	directBacklog      int
	groupBacklog       int
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
	maxPolls    int
	observe     func(context.Context) (drainObservation, error)
	wait        func(context.Context, time.Duration) bool
}

// run polls until both queues are quiet for the stable window, the attempt
// budget is spent, or the context is canceled. The result carries the counters
// of the last observation in every case.
func (l drainLoop) run(ctx context.Context) (QueueDrainResult, error) {
	var (
		result      QueueDrainResult
		progress    drainProgress
		observation drainObservation
	)
	for attempt := 1; ; attempt++ {
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
		if attempt >= l.maxPolls {
			return result, fmt.Errorf(
				"persist queues did not drain in %d polls: direct_ready=%d "+
					"group_ready=%d direct_persisted=%d group_persisted=%d "+
					"direct_retrying=%t group_retrying=%t",
				l.maxPolls,
				observation.directBacklog,
				observation.groupBacklog,
				observation.directPersisted,
				observation.groupPersisted,
				observation.directRetrying,
				observation.groupRetrying,
			)
		}
		if !l.wait(ctx, l.pollEvery) {
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

// DrainPersistQueues runs the existing direct and group consumers until both
// persist queues are empty and idle, then stops them. It never publishes and
// leaves dead-letter handling to the consumers.
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
	groupQueue := ""
	if groupConsumer != nil {
		groupQueue = groupQueueName(namespace)
	}

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
		groupBacklog := 0
		if groupConsumer != nil {
			groupBacklog, err = inspector.Depth(ctx, groupQueue)
			if err != nil {
				return drainObservation{}, err
			}
		}
		directStatus := directConsumer.Status()
		groupStatus := ConsumerStatus{}
		if groupConsumer != nil {
			groupStatus = groupConsumer.Status()
		}
		return drainObservation{
			directBacklog:      directBacklog,
			groupBacklog:       groupBacklog,
			directPersisted:    directStatus.Persisted,
			groupPersisted:     groupStatus.Persisted,
			directRetrying:     directStatus.Retrying,
			groupRetrying:      groupStatus.Retrying,
			directDeadLettered: directStatus.DeadLettered,
			groupDeadLettered:  groupStatus.DeadLettered,
		}, nil
	}

	maxPolls := 1
	if pollEvery > 0 {
		maxPolls += int(timeout / pollEvery)
	}
	loop := drainLoop{
		pollEvery:   pollEvery,
		stablePolls: queueDrainStablePolls,
		maxPolls:    maxPolls,
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
	logf(
		"persist queues drained direct=%d group=%d dead_lettered=%d",
		drained.Direct, drained.Group, drained.DeadLettered,
	)
	return drained, nil
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
