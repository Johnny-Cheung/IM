package httpapi

import (
	"context"
	"log"
	"sync"
	"time"
)

var (
	groupRepairBaseDelay = time.Second
	groupRepairMaxDelay  = 30 * time.Second
)

const groupProjectionTimeout = 5 * time.Second

// groupCommitContext detaches post-commit bookkeeping from the request: the
// MySQL mutation is committed, so a cancelled client must not leave the Redis
// projection half-updated.
func groupCommitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), groupProjectionTimeout)
}

// groupRepair retries Redis group projection after a failed membership sync so
// group:{G}:sync_required cannot wedge a group until the process restarts.
type groupRepair struct {
	api     *API
	mu      sync.Mutex
	pending map[uint64]*groupRepairEntry
	wake    chan struct{}
	stopCh  chan struct{}
	started bool
	stopped bool
	wg      sync.WaitGroup
}

type groupRepairEntry struct {
	attempts int
	nextAt   time.Time
	gen      uint64
}

func (r *groupRepair) schedule(groupID uint64) {
	if r == nil || groupID == 0 {
		return
	}
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	if entry, ok := r.pending[groupID]; ok {
		entry.gen++
		entry.nextAt = time.Now()
	} else {
		r.pending[groupID] = &groupRepairEntry{nextAt: time.Now(), gen: 1}
	}
	r.mu.Unlock()

	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *groupRepair) start() {
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
		r.run()
	}()
}

func (r *groupRepair) stop() {
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
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *groupRepair) run() {
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
		r.repairDue()
	}
}

func (r *groupRepair) nextDelay() (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) == 0 {
		return 0, false
	}
	now := time.Now()
	var earliest time.Time
	for _, entry := range r.pending {
		if earliest.IsZero() || entry.nextAt.Before(earliest) {
			earliest = entry.nextAt
		}
	}
	return earliest.Sub(now), true
}

func (r *groupRepair) repairDue() {
	type dueRepair struct {
		groupID uint64
		gen     uint64
	}
	now := time.Now()
	r.mu.Lock()
	due := make([]dueRepair, 0, len(r.pending))
	for groupID, entry := range r.pending {
		if !entry.nextAt.After(now) {
			entry.attempts++
			due = append(due, dueRepair{groupID: groupID, gen: entry.gen})
		}
	}
	r.mu.Unlock()

	for _, item := range due {
		select {
		case <-r.stopCh:
			return
		default:
		}
		if err := r.api.repairGroupProjection(item.groupID); err != nil {
			log.Printf("repair group %d projection: %v", item.groupID, err)
			r.mu.Lock()
			if entry, ok := r.pending[item.groupID]; ok {
				entry.nextAt = time.Now().Add(groupRepairDelay(entry.attempts))
			}
			r.mu.Unlock()
			continue
		}
		r.mu.Lock()
		if entry, ok := r.pending[item.groupID]; ok {
			if entry.gen == item.gen {
				delete(r.pending, item.groupID)
			} else {
				entry.attempts = 0
				entry.nextAt = time.Now()
			}
		}
		r.mu.Unlock()
	}
}

func groupRepairDelay(attempts int) time.Duration {
	delay := groupRepairBaseDelay
	for index := 1; index < attempts && delay < groupRepairMaxDelay; index++ {
		delay *= 2
	}
	if delay > groupRepairMaxDelay {
		delay = groupRepairMaxDelay
	}
	return delay
}

// repairGroupProjection rebuilds one group's Redis state from MySQL: member
// set, dissolved flag, sync flag and any missing read positions.
func (a *API) repairGroupProjection(groupID uint64) error {
	if a.groupProjector == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), groupProjectionTimeout)
	defer cancel()
	projections, err := a.store.ListGroupMembershipProjections(ctx)
	if err != nil {
		return err
	}
	for _, projection := range projections {
		if projection.GroupID != groupID {
			continue
		}
		if err := a.groupProjector.Replace(
			ctx, groupID, projection.MemberIDs, projection.Dissolved,
		); err != nil {
			return err
		}
		if a.groupReads == nil {
			return nil
		}
		for userID, position := range projection.Positions {
			if err := a.groupReads.SeedMember(ctx, groupID, userID, position); err != nil {
				return err
			}
		}
		log.Printf("repaired group %d projection", groupID)
		return nil
	}
	return nil
}

// groupMutation owns the sync_required flag set by BeginMutation and makes sure
// every exit path either refreshes the projection or queues a repair.
type groupMutation struct {
	api     *API
	groupID uint64
	begun   bool
	synced  bool
	aborted bool
	queued  bool
}

func (m *groupMutation) queueRepair() {
	if m.queued {
		return
	}
	m.queued = true
	m.api.groupRepairs.schedule(m.groupID)
}

func (a *API) beginGroupMutation(ctx context.Context, groupID uint64) (*groupMutation, error) {
	mutation := &groupMutation{api: a, groupID: groupID}
	if a.groupProjector == nil {
		return mutation, nil
	}
	if err := a.groupProjector.BeginMutation(ctx, groupID); err != nil {
		return nil, err
	}
	mutation.begun = true
	return mutation, nil
}

// abort clears the flag before the MySQL mutation commits.
func (m *groupMutation) abort() {
	if !m.begun || m.synced {
		return
	}
	ctx, cancel := groupCommitContext(context.Background())
	defer cancel()
	if err := m.api.groupProjector.EndMutation(ctx, m.groupID); err != nil {
		log.Printf("clear group %d sync flag: %v", m.groupID, err)
		m.queueRepair()
		return
	}
	m.aborted = true
}

// project refreshes the Redis projection from the committed MySQL state. A
// failed or unavailable member list is repaired in the background and the
// group stays fail-closed until then.
func (m *groupMutation) project(ctx context.Context, memberIDs []uint64, dissolved bool) {
	if !m.begun || m.synced {
		return
	}
	if len(memberIDs) == 0 {
		m.queueRepair()
		return
	}
	if err := m.api.groupProjector.Replace(
		ctx, m.groupID, memberIDs, dissolved,
	); err != nil {
		log.Printf("project group %d after membership change: %v", m.groupID, err)
		m.queueRepair()
		return
	}
	m.synced = true
}

// finish queues a repair when a committed mutation left the projection stale.
func (m *groupMutation) finish() {
	if !m.begun || m.synced || m.aborted {
		return
	}
	m.queueRepair()
}
