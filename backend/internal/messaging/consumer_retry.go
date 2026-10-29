package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// ConsumerStatus is the diagnostics surface of a RabbitMQ consumer.
type ConsumerStatus struct {
	LastError        string    `json:"last_error,omitempty"`
	LastErrorAt      time.Time `json:"last_error_at,omitempty"`
	LastPersistedAt  time.Time `json:"last_persisted_at,omitempty"`
	Persisted        uint64    `json:"persisted"`
	TransientRetries uint64    `json:"transient_retries"`
	MessageRetries   uint64    `json:"message_retries"`
	DeadLettered     uint64    `json:"dead_lettered"`
	Retrying         bool      `json:"retrying"`
	// InFlight is the number of deliveries the consumer holds unacknowledged
	// right now: decoded, persisting, acking or dead-lettering. A queue can
	// report zero ready messages while a delivery is still in flight.
	InFlight int `json:"in_flight"`
}

// consumeStatusState collects counters and timestamps shared by both chains.
// The zero value is ready to use.
type consumeStatusState struct {
	mu               sync.Mutex
	lastError        string
	lastErrorAt      time.Time
	lastPersistedAt  time.Time
	persisted        uint64
	transientRetries uint64
	messageRetries   uint64
	deadLettered     uint64
	retrying         bool
	inFlight         int
}

func (s *consumeStatusState) snapshot() ConsumerStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ConsumerStatus{
		LastError:        s.lastError,
		LastErrorAt:      s.lastErrorAt,
		LastPersistedAt:  s.lastPersistedAt,
		Persisted:        s.persisted,
		TransientRetries: s.transientRetries,
		MessageRetries:   s.messageRetries,
		DeadLettered:     s.deadLettered,
		Retrying:         s.retrying,
		InFlight:         s.inFlight,
	}
}

func (s *consumeStatusState) recordError(err error, at time.Time) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastError = err.Error()
	s.lastErrorAt = at.UTC()
}

func (s *consumeStatusState) recordPersisted(at time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persisted++
	s.lastPersistedAt = at.UTC()
}

func (s *consumeStatusState) recordTransientRetry() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transientRetries++
}

func (s *consumeStatusState) recordMessageRetry() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messageRetries++
}

func (s *consumeStatusState) recordDeadLettered() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadLettered++
}

func (s *consumeStatusState) setRetrying(retrying bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retrying = retrying
}

// beginDelivery marks one delivery as unacknowledged. A drain must not declare
// the queues empty while another consumer still holds a delivery.
func (s *consumeStatusState) beginDelivery() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight++
}

// endDelivery clears the marker set by beginDelivery, including on the
// context-canceled returns of persist and deadLetter.
func (s *consumeStatusState) endDelivery() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight > 0 {
		s.inFlight--
	}
}

// consumeDelivery is the part of amqp091.Delivery the consumers use.
type consumeDelivery interface {
	Body() []byte
	MessageID() string
	Ack(multiple bool) error
	Nack(multiple, requeue bool) error
}

type consumeAMQPDelivery struct {
	delivery amqp091.Delivery
}

func (d consumeAMQPDelivery) Body() []byte      { return d.delivery.Body }
func (d consumeAMQPDelivery) MessageID() string { return d.delivery.MessageId }

func (d consumeAMQPDelivery) Ack(multiple bool) error {
	return d.delivery.Ack(multiple)
}

func (d consumeAMQPDelivery) Nack(multiple, requeue bool) error {
	return d.delivery.Nack(multiple, requeue)
}

// consumeRetryPolicy is the injectable timing of in-process retries.
type consumeRetryPolicy struct {
	// messageDelays holds the waits after each message-class failure. Its
	// length is also the number of retries before a dead letter.
	messageDelays []time.Duration
	transientBase time.Duration
	transientMax  time.Duration
	sleep         func(context.Context, time.Duration) bool
	now           func() time.Time
}

func defaultConsumeRetryPolicy() consumeRetryPolicy {
	return consumeRetryPolicy{
		messageDelays: []time.Duration{
			time.Second,
			2 * time.Second,
			4 * time.Second,
			8 * time.Second,
			16 * time.Second,
		},
		transientBase: time.Second,
		transientMax:  30 * time.Second,
		sleep:         waitMessaging,
		now:           time.Now,
	}
}

func (p consumeRetryPolicy) withDefaults() consumeRetryPolicy {
	defaults := defaultConsumeRetryPolicy()
	if len(p.messageDelays) == 0 {
		p.messageDelays = defaults.messageDelays
	}
	if p.transientBase <= 0 {
		p.transientBase = defaults.transientBase
	}
	if p.transientMax <= 0 {
		p.transientMax = defaults.transientMax
	}
	if p.sleep == nil {
		p.sleep = defaults.sleep
	}
	if p.now == nil {
		p.now = defaults.now
	}
	return p
}

// transientDelay doubles from the base delay and caps at the maximum.
func (p consumeRetryPolicy) transientDelay(retry uint64) time.Duration {
	delay := p.transientBase
	for i := uint64(0); i < retry; i++ {
		if delay >= p.transientMax {
			return p.transientMax
		}
		delay *= 2
	}
	if delay > p.transientMax {
		return p.transientMax
	}
	return delay
}

// wait sleeps while exposing the retrying flag. It returns false when the
// context is canceled during the wait.
func (p consumeRetryPolicy) wait(ctx context.Context, status *consumeStatusState, delay time.Duration) bool {
	status.setRetrying(true)
	ok := p.sleep(ctx, delay)
	status.setRetrying(false)
	return ok
}

// consumeDeliveryJob ties one delivery to the state needed to retry, park or
// acknowledge it.
type consumeDeliveryJob struct {
	delivery  consumeDelivery
	publisher consumeDeadLetterPublisher
	kind      DeadLetterKind
	retry     consumeRetryPolicy
	status    *consumeStatusState
	label     string
}

// persist runs an idempotent write until it succeeds or the message must be
// dead-lettered. It leaves the delivery unacknowledged while waiting, so a
// crash or shutdown lets the broker redeliver it.
func (j consumeDeliveryJob) persist(ctx context.Context, write func(context.Context) error) error {
	retry := j.retry.withDefaults()
	var (
		attempts         int
		messageRetries   int
		transientRetries uint64
		firstFailedAt    time.Time
	)
	for {
		if ctx.Err() != nil {
			return nil
		}
		attempts++
		err := write(ctx)
		if err == nil {
			j.status.recordPersisted(retry.now())
			return j.ack()
		}
		if ctx.Err() != nil {
			return nil
		}
		now := retry.now()
		if firstFailedAt.IsZero() {
			firstFailedAt = now
		}
		j.status.recordError(err, now)
		switch classifyPersistError(err) {
		case persistErrorConflict:
			log.Printf("dead-letter %s: %v", j.label, err)
			return j.deadLetter(ctx, dlqConflictReason(err), err, attempts, firstFailedAt)
		case persistErrorTransient:
			delay := retry.transientDelay(transientRetries)
			transientRetries++
			j.status.recordTransientRetry()
			if !retry.wait(ctx, j.status, delay) {
				return nil
			}
		default:
			if messageRetries >= len(retry.messageDelays) {
				log.Printf("dead-letter %s after %d attempts: %v", j.label, attempts, err)
				return j.deadLetter(ctx, dlqReasonRetriesExhausted, err, attempts, firstFailedAt)
			}
			delay := retry.messageDelays[messageRetries]
			messageRetries++
			j.status.recordMessageRetry()
			if !retry.wait(ctx, j.status, delay) {
				return nil
			}
		}
	}
}

// deadLetterNow parks a delivery that failed before any persist attempt, such
// as a decode failure.
func (j consumeDeliveryJob) deadLetterNow(ctx context.Context, reason string, cause error) error {
	return j.deadLetter(ctx, reason, cause, 1, j.retry.withDefaults().now())
}

// deadLetter publishes the original message and acks only after the broker
// confirms. A publish failure is transient: the delivery stays unacked while
// the publish is retried with backoff. A closed publishing channel cannot
// recover in place, and an unroutable mandatory publish means the dead-letter
// queue or binding is missing; both are returned to trigger a reconnect, whose
// topology declaration recreates it. The unacked delivery is then redelivered
// by the broker.
func (j consumeDeliveryJob) deadLetter(ctx context.Context, reason string, cause error, attempts int, firstFailedAt time.Time) error {
	if j.publisher == nil {
		return fmt.Errorf("dead letter publisher is not configured for %s", j.kind)
	}
	retry := j.retry.withDefaults()
	message := dlqMessage{
		Kind:          j.kind,
		Body:          j.delivery.Body(),
		MessageID:     j.delivery.MessageID(),
		Reason:        reason,
		Error:         cause.Error(),
		Attempts:      attempts,
		FirstFailedAt: firstFailedAt,
	}
	for retryIndex := uint64(0); ; retryIndex++ {
		if ctx.Err() != nil {
			return nil
		}
		err := j.publisher.PublishDeadLetter(ctx, message)
		if err == nil {
			j.status.recordDeadLettered()
			return j.ack()
		}
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, amqp091.ErrClosed) || errors.Is(err, dlqErrPublisherClosed) || errors.Is(err, errDLQUnroutable) {
			j.status.recordError(err, retry.now())
			log.Printf("dead-letter %s returned to reconnect: %v", j.label, err)
			return fmt.Errorf("dead-letter %s: %w", j.label, err)
		}
		j.status.recordError(err, retry.now())
		j.status.recordTransientRetry()
		log.Printf("retry dead-letter %s after %d attempt(s): %v", j.label, retryIndex+1, err)
		if !retry.wait(ctx, j.status, retry.transientDelay(retryIndex)) {
			return nil
		}
	}
}

func (j consumeDeliveryJob) ack() error {
	if err := j.delivery.Ack(false); err != nil {
		return fmt.Errorf("ack %s delivery: %w", j.kind, err)
	}
	return nil
}
