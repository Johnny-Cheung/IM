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
	implicitReadWindow     = 100 * time.Millisecond
	implicitReadMaxPending = 256
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
	memberIDs         []uint64
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
	now := time.Now()
	due := make([]*implicitReadBatch, 0, len(r.pending))
	r.mu.Lock()
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
			r.requeue(due[index:])
			return
		}
		if !r.flushBatch(ctx, batch) {
			// One stalled Redis call must not discard the batches behind it.
			r.requeue(due[index:])
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
// instead of silently losing the readers' positions.
func (r *implicitRead) requeue(batches []*implicitReadBatch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	for _, batch := range batches {
		batch.attempts++
		if batch.attempts > implicitReadMaxRetries {
			log.Printf("implicit read dropped after %d attempts: direct=%t group=%d seq=%d",
				batch.attempts, batch.direct, batch.groupID, batch.throughSeq)
			continue
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
			if len(batch.memberIDs) > 0 {
				existing.memberIDs = batch.memberIDs
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
		if len(batch.memberIDs) > 0 {
			existing.memberIDs = batch.memberIDs
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

func (a *API) scheduleGroupImplicitRead(
	groupID, senderID uint64,
	seq uint64,
	memberIDs []uint64,
) {
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
		memberIDs:  memberIDs,
	})
}

// flushDirectImplicitRead reports whether the batch can be retired. A Redis or
// gate failure returns false so the caller retries instead of losing the
// readers' positions.
func (a *API) flushDirectImplicitRead(ctx context.Context, batch *implicitReadBatch) bool {
	if a.directReads == nil {
		return true
	}
	ok := true
	now := time.Now().UTC()

	type advancedRead struct{ readerID, peerID, position uint64 }
	advanced := make([]advancedRead, 0, 2)
	if a.hub.isDirectFocus(batch.recipientID, batch.senderID) {
		result, err := a.directReads.AdvanceRead(
			ctx, batch.recipientID, batch.senderID, batch.throughSeq, now,
		)
		if err != nil {
			ok = false
			log.Printf("implicit direct read recipient=%d peer=%d: %v",
				batch.recipientID, batch.senderID, err)
		} else if result.Advanced {
			advanced = append(advanced, advancedRead{
				readerID: batch.recipientID,
				peerID:   batch.senderID,
				position: result.LastReadSeq,
			})
		}
	}

	result, err := a.directReads.AdvanceRead(
		ctx, batch.senderID, batch.recipientID, batch.throughSeq, now,
	)
	if err != nil {
		ok = false
		log.Printf("implicit direct read sender=%d peer=%d: %v",
			batch.senderID, batch.recipientID, err)
	} else if result.Advanced {
		advanced = append(advanced, advancedRead{
			readerID: batch.senderID,
			peerID:   batch.recipientID,
			position: result.LastReadSeq,
		})
	}

	for _, item := range advanced {
		readerName, peerName := batch.senderUsername, batch.recipientUsername
		if item.readerID == batch.recipientID {
			readerName, peerName = batch.recipientUsername, batch.senderUsername
		}
		a.PublishRead(item.readerID, item.peerID, readerName, peerName, item.position)
	}
	return ok
}

// flushGroupImplicitRead reports whether the batch can be retired. A dissolved
// group or a removed member is terminal; any other Redis or gate failure is
// retried.
func (a *API) flushGroupImplicitRead(ctx context.Context, batch *implicitReadBatch) bool {
	if a.groupReads == nil {
		return true
	}
	ok := true
	now := time.Now().UTC()
	advanced := make(map[uint64]struct{}, 4)

	viewers := a.hub.focusedGroupUsers(batch.groupID)
	if len(viewers) > 0 {
		advances, err := a.groupReads.AdvanceReadMany(
			ctx, batch.groupID, viewers, batch.throughSeq, now,
		)
		if err != nil {
			if !groupGoneError(err) {
				ok = false
				log.Printf("implicit group read group=%d viewers=%v: %v",
					batch.groupID, viewers, err)
			}
		} else {
			for _, advance := range advances {
				logGroupReadStreamError(advance, batch.groupID)
				if advance.Advanced {
					advanced[advance.UserID] = struct{}{}
				}
			}
		}
	}

	if batch.senderID != 0 {
		advance, err := a.groupReads.AdvanceRead(
			ctx, batch.groupID, batch.senderID, batch.throughSeq, now,
		)
		if err != nil {
			if !groupGoneError(err) {
				ok = false
				log.Printf("implicit group read sender=%d group=%d: %v",
					batch.senderID, batch.groupID, err)
			}
		} else {
			logGroupReadStreamError(advance, batch.groupID)
			if advance.Advanced {
				advanced[batch.senderID] = struct{}{}
			}
		}
	}

	if len(advanced) == 0 {
		return ok
	}
	// The sender's own advance is not a read by somebody else, so it must not
	// suppress the signal the sender needs for its receipt: exclude only the
	// readers that are not the batch sender.
	excluded := make(map[uint64]struct{}, len(advanced))
	for userID := range advanced {
		if userID != batch.senderID {
			excluded[userID] = struct{}{}
		}
	}
	if len(excluded) == 0 {
		return ok
	}
	a.publishGroupRead(batch.memberIDs, batch.groupID, excluded)
	return ok
}

// groupGoneError marks the expected race where the group was dissolved or the
// member removed between delivery and the asynchronous advance.
func groupGoneError(err error) bool {
	return errors.Is(err, messaging.ErrGroupDissolved) || errors.Is(err, messaging.ErrGroupNotMember)
}
