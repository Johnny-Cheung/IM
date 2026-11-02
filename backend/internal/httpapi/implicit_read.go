package httpapi

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
)

const (
	// Read receipts tolerate seconds of delay: a longer window merges more
	// messages per conversation into one watermark advance, which cuts the
	// Redis pressure that competes with message acceptance. The queue cap is
	// sized for that longer window rather than for the old 100ms batch.
	implicitReadWindow     = time.Second
	implicitReadMaxPending = 4000
	implicitReadMaxRetries = 3
	implicitReadTimeout    = 5 * time.Second
)

// implicitReadDrainTimeout bounds one flush pass; batches left over are
// requeued, so a slow Redis call cannot lose them. Shutdown cancels the drain
// context instead of waiting for this deadline.
var implicitReadDrainTimeout = implicitReadTimeout

type implicitReadKey struct {
	direct bool
	first  uint64
	second uint64
}

type implicitReadBatch struct {
	key               implicitReadKey
	direct            bool
	senderID          uint64
	recipientID       uint64
	senderUsername    string
	recipientUsername string
	groupID           uint64
	throughSeq        uint64
	queuedAt          time.Time
	attempts          int
}

// implicitRead merges delivered messages per conversation and advances the
// reader positions off the request goroutine.
type implicitRead struct {
	api     *API
	mu      sync.Mutex
	pending map[implicitReadKey]*implicitReadBatch
	wake    chan struct{}
	stopCh  chan struct{}
	done    chan struct{}
	started bool
	stopped bool
	wg      sync.WaitGroup

	drainCtx    context.Context
	drainCancel context.CancelFunc
}

func newImplicitRead(api *API) *implicitRead {
	drainCtx, drainCancel := context.WithCancel(context.Background())
	return &implicitRead{
		api:         api,
		pending:     make(map[implicitReadKey]*implicitReadBatch),
		wake:        make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
		drainCtx:    drainCtx,
		drainCancel: drainCancel,
	}
}

func (r *implicitRead) start() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.mu.Unlock()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer close(r.done)
		r.run()
	}()
}

func (r *implicitRead) stop() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if !r.started || r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	close(r.stopCh)
	// Abort an in-flight drain: one hung Redis call must not hold shutdown.
	r.drainCancel()
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *implicitRead) run() {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		delay, pending := r.nextDelay()
		if !pending {
			select {
			case <-r.wake:
			case <-r.stopCh:
				return
			}
			continue
		}
		if delay > 0 {
			timer.Reset(delay)
			select {
			case <-timer.C:
			case <-r.wake:
				if !timer.Stop() {
					<-timer.C
				}
			case <-r.stopCh:
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
		}
		select {
		case <-r.stopCh:
			return
		default:
		}
		r.flushDue()
	}
}

func (r *implicitRead) nextDelay() (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) == 0 {
		return 0, false
	}
	now := time.Now()
	var earliest time.Time
	for _, batch := range r.pending {
		due := batch.queuedAt.Add(implicitReadWindow)
		if earliest.IsZero() || due.Before(earliest) {
			earliest = due
		}
	}
	return earliest.Sub(now), true
}

func (r *implicitRead) flushDue() {
	r.mu.Lock()
	now := time.Now()
	due := make([]*implicitReadBatch, 0, len(r.pending))
	for key, batch := range r.pending {
		if now.Sub(batch.queuedAt) >= implicitReadWindow {
			delete(r.pending, key)
			due = append(due, batch)
		}
	}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(r.drainCtx, implicitReadDrainTimeout)
	defer cancel()
	for index, batch := range due {
		select {
		case <-r.stopCh:
			return
		default:
		}
		if ctx.Err() != nil {
			// Nothing at or after index was attempted.
			r.requeue(due[index:], nil)
			return
		}
		if !r.flushBatch(ctx, batch) {
			// One stalled Redis call must not discard the batches behind it.
			r.requeue(due[index:], batch)
			return
		}
	}
}

func (r *implicitRead) flushBatch(ctx context.Context, batch *implicitReadBatch) bool {
	if batch.direct {
		return r.api.flushDirectImplicitRead(ctx, batch)
	}
	return r.api.flushGroupImplicitRead(ctx, batch)
}

// requeue puts unflushed batches back so a slow or failing Redis call retries
// instead of silently losing the readers' positions. Only the batch that was
// actually attempted consumes a retry attempt; batches that were never
// reached keep their full budget.
func (r *implicitRead) requeue(batches []*implicitReadBatch, attempted *implicitReadBatch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	for _, batch := range batches {
		if batch == attempted {
			batch.attempts++
			if batch.attempts > implicitReadMaxRetries {
				log.Printf("implicit read dropped after %d attempts: direct=%t group=%d seq=%d",
					batch.attempts, batch.direct, batch.groupID, batch.throughSeq)
				continue
			}
		}
		batch.queuedAt = time.Now()
		existing, ok := r.pending[batch.key]
		if ok {
			if batch.throughSeq > existing.throughSeq {
				existing.throughSeq = batch.throughSeq
			}
			if batch.senderUsername != "" {
				existing.senderUsername = batch.senderUsername
				existing.recipientUsername = batch.recipientUsername
			}
			if batch.attempts > existing.attempts {
				existing.attempts = batch.attempts
			}
			continue
		}
		if len(r.pending) >= implicitReadMaxPending {
			log.Printf("implicit read queue full, dropping: direct=%t group=%d seq=%d",
				batch.direct, batch.groupID, batch.throughSeq)
			continue
		}
		r.pending[batch.key] = batch
	}
}

func (r *implicitRead) enqueue(batch implicitReadBatch) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if !r.started || r.stopped {
		r.mu.Unlock()
		return
	}
	existing, ok := r.pending[batch.key]
	if ok {
		if batch.throughSeq > existing.throughSeq {
			existing.throughSeq = batch.throughSeq
		}
		if batch.senderUsername != "" {
			existing.senderUsername = batch.senderUsername
			existing.recipientUsername = batch.recipientUsername
		}
		r.mu.Unlock()
		return
	}
	if len(r.pending) >= implicitReadMaxPending {
		r.mu.Unlock()
		log.Printf("implicit read queue full, dropping: direct=%t group=%d seq=%d",
			batch.direct, batch.groupID, batch.throughSeq)
		return
	}
	batch.queuedAt = time.Now()
	copied := batch
	r.pending[batch.key] = &copied
	r.mu.Unlock()

	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (a *API) scheduleDirectImplicitRead(
	senderID, recipientID uint64,
	senderUsername, recipientUsername string,
	seq uint64,
) {
	if a.directReads == nil || seq == 0 {
		return
	}
	a.implicitReads.enqueue(implicitReadBatch{
		key:               implicitReadKey{direct: true, first: senderID, second: recipientID},
		direct:            true,
		senderID:          senderID,
		recipientID:       recipientID,
		senderUsername:    senderUsername,
		recipientUsername: recipientUsername,
		throughSeq:        seq,
	})
}

func (a *API) scheduleGroupImplicitRead(groupID, senderID, seq uint64) {
	if a.groupReads == nil || seq == 0 {
		return
	}
	a.implicitReads.enqueue(implicitReadBatch{
		// Keyed by sender: merging two senders would advance the first one to
		// the other's message and drop the second sender's own-message bump.
		key:        implicitReadKey{first: groupID, second: senderID},
		senderID:   senderID,
		groupID:    groupID,
		throughSeq: seq,
	})
}

// flushDirectImplicitRead advances the sender's own read position. Recipients
// report their rendered position through the websocket read cursor, so the
// server no longer infers a recipient read from delivery. A Redis or gate
// failure returns false so the caller retries instead of losing the position.
func (a *API) flushDirectImplicitRead(ctx context.Context, batch *implicitReadBatch) bool {
	if a.directReads == nil {
		return true
	}
	now := time.Now().UTC()

	result, err := a.directReads.AdvanceRead(
		ctx, batch.senderID, batch.recipientID, batch.throughSeq, now,
	)
	if err != nil {
		log.Printf("implicit direct read sender=%d peer=%d: %v",
			batch.senderID, batch.recipientID, err)
		return false
	}
	if result.StreamError {
		log.Printf("implicit direct read stream sender=%d peer=%d seq=%d",
			batch.senderID, batch.recipientID, result.LastReadSeq)
	}
	if result.Advanced {
		a.PublishRead(
			batch.senderID,
			batch.recipientID,
			batch.senderUsername,
			batch.recipientUsername,
			result.LastReadSeq,
		)
	}
	return true
}

// flushGroupImplicitRead advances only the sender's own read position. Group
// members report their rendered position through the websocket read cursor, so
// the server no longer scans every online connection for focused viewers.
func (a *API) flushGroupImplicitRead(ctx context.Context, batch *implicitReadBatch) bool {
	if a.groupReads == nil {
		return true
	}
	now := time.Now().UTC()

	advance, err := a.groupReads.AdvanceRead(
		ctx, batch.groupID, batch.senderID, batch.throughSeq, now,
	)
	if err != nil {
		if !groupGoneError(err) {
			log.Printf("implicit group read sender=%d group=%d: %v",
				batch.senderID, batch.groupID, err)
			return false
		}
		return true
	}
	logGroupReadStreamError(advance, batch.groupID)
	return true
}

// groupGoneError marks the expected race where the group was dissolved or the
// member removed between delivery and the asynchronous advance.
func groupGoneError(err error) bool {
	return errors.Is(err, messaging.ErrGroupDissolved) || errors.Is(err, messaging.ErrGroupNotMember)
}
