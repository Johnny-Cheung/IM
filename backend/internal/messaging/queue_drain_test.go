package messaging

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

func TestDrainProgressRequiresStableWindow(t *testing.T) {
	var progress drainProgress
	empty := drainObservation{directPersisted: 4, groupPersisted: 2}
	if progress.observe(empty, 2) {
		t.Fatal("the first clean poll must not finish the drain")
	}
	if !progress.observe(empty, 2) {
		t.Fatal("the second clean poll must finish the drain")
	}
}

func TestDrainProgressResetsOnBacklog(t *testing.T) {
	var progress drainProgress
	empty := drainObservation{directPersisted: 4}
	if progress.observe(empty, 2) {
		t.Fatal("one clean poll must not finish")
	}
	if progress.observe(drainObservation{directBacklog: 1, directPersisted: 4}, 2) {
		t.Fatal("a non-empty direct queue must not finish")
	}
	if progress.observe(empty, 2) {
		t.Fatal("the stable window must restart after a backlog")
	}
	if !progress.observe(empty, 2) {
		t.Fatal("two clean polls after the backlog must finish")
	}
}

func TestDrainProgressResetsOnGroupBacklog(t *testing.T) {
	var progress drainProgress
	if progress.observe(drainObservation{groupBacklog: 3}, 2) {
		t.Fatal("a non-empty group queue must not finish")
	}
}

func TestDrainProgressResetsOnAdvancedPersist(t *testing.T) {
	var progress drainProgress
	if progress.observe(drainObservation{directPersisted: 1}, 2) {
		t.Fatal("an advanced counter must restart the stable window")
	}
	if progress.observe(drainObservation{directPersisted: 2}, 2) {
		t.Fatal("another advanced counter must restart the stable window")
	}
	if progress.observe(drainObservation{directPersisted: 2, groupPersisted: 1}, 2) {
		t.Fatal("group progress must restart the stable window")
	}
	if progress.observe(drainObservation{directPersisted: 2, groupPersisted: 1}, 2) {
		t.Fatal("the first idle poll after progress must not finish")
	}
	if !progress.observe(drainObservation{directPersisted: 2, groupPersisted: 1}, 2) {
		t.Fatal("the second idle poll after progress must finish")
	}
}

func TestDrainProgressWaitsWhileRetrying(t *testing.T) {
	var progress drainProgress
	if progress.observe(drainObservation{directRetrying: true}, 2) {
		t.Fatal("a retrying direct consumer must not finish")
	}
	if progress.observe(drainObservation{groupRetrying: true}, 2) {
		t.Fatal("a retrying group consumer must not finish")
	}
	if progress.observe(drainObservation{}, 2) {
		t.Fatal("the stable window must restart after a retry")
	}
	if !progress.observe(drainObservation{}, 2) {
		t.Fatal("two idle polls after a retry must finish")
	}
}

func TestDrainLoopFinishesAfterStableWindow(t *testing.T) {
	observations := []drainObservation{
		{directPersisted: 3, groupPersisted: 1},
		{directPersisted: 3, groupPersisted: 1},
	}
	index := 0
	loop := drainLoop{
		pollEvery:   time.Second,
		stablePolls: 2,
		observe: func(context.Context) (drainObservation, error) {
			if index >= len(observations) {
				t.Fatalf("polled past the scripted observations at %d", index)
			}
			observation := observations[index]
			index++
			return observation, nil
		},
		wait: func(context.Context, time.Duration) bool { return true },
	}

	result, err := loop.run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if index != 2 {
		t.Fatalf("polls = %d, want 2", index)
	}
	if result.Direct != 3 || result.Group != 1 || result.DeadLettered != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestDrainLoopKeepsPollingWhilePersisting(t *testing.T) {
	persisted := []uint64{0, 1, 1, 1}
	index := 0
	loop := drainLoop{
		pollEvery:   time.Second,
		stablePolls: 2,
		observe: func(context.Context) (drainObservation, error) {
			observation := drainObservation{directPersisted: persisted[index]}
			index++
			return observation, nil
		},
		wait: func(context.Context, time.Duration) bool { return true },
	}

	result, err := loop.run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if index != len(persisted) {
		t.Fatalf("polls = %d, want %d", index, len(persisted))
	}
	if result.Direct != 1 {
		t.Fatalf("direct = %d, want 1", result.Direct)
	}
}

func TestDrainLoopTimesOutWithCounts(t *testing.T) {
	start := time.Date(2026, 10, 29, 4, 0, 0, 0, time.UTC)
	now := start
	loop := drainLoop{
		pollEvery:   time.Second,
		stablePolls: 2,
		timeout:     3 * time.Second,
		now:         func() time.Time { return now },
		observe: func(context.Context) (drainObservation, error) {
			now = now.Add(time.Second)
			return drainObservation{
				directBacklog:      4,
				groupBacklog:       2,
				directInFlight:     1,
				groupInFlight:      1,
				directPersisted:    7,
				groupPersisted:     5,
				directDeadLettered: 1,
			}, nil
		},
		wait: func(context.Context, time.Duration) bool { return true },
	}

	result, err := loop.run(context.Background())
	if err == nil {
		t.Fatal("run must fail when the queues never drain")
	}
	for _, fragment := range []string{
		"did not drain within 3s", "direct_ready=4", "group_ready=2",
		"direct_in_flight=1", "group_in_flight=1", "direct_persisted=7",
	} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("error %q is missing %q", err, fragment)
		}
	}
	if result.Direct != 7 || result.Group != 5 || result.DeadLettered != 1 {
		t.Fatalf("result = %#v; counts must survive the timeout", result)
	}
}

func TestDrainLoopDeadlineIsWallClockNotPollCount(t *testing.T) {
	// A poll that takes far longer than PollEvery must spend the wall-clock
	// budget, not one poll of it.
	start := time.Date(2026, 10, 29, 4, 0, 0, 0, time.UTC)
	now := start
	polls := 0
	loop := drainLoop{
		pollEvery:   time.Second,
		stablePolls: 2,
		timeout:     90 * time.Second,
		now:         func() time.Time { return now },
		observe: func(context.Context) (drainObservation, error) {
			polls++
			now = now.Add(time.Minute)
			return drainObservation{groupBacklog: 1}, nil
		},
		wait: func(context.Context, time.Duration) bool { return true },
	}

	_, err := loop.run(context.Background())
	if err == nil {
		t.Fatal("run must fail once the wall-clock deadline passes")
	}
	if polls != 2 {
		t.Fatalf("polls = %d, want 2: 90s at one minute per poll", polls)
	}
	if !strings.Contains(err.Error(), "did not drain within 1m30s") {
		t.Fatalf("error %q must name the wall-clock budget", err)
	}
}

func TestDrainProgressWaitsWhileInFlight(t *testing.T) {
	var progress drainProgress
	empty := drainObservation{directPersisted: 4}
	if progress.observe(empty, 2) {
		t.Fatal("one clean poll must not finish")
	}
	if progress.observe(drainObservation{directPersisted: 4, directInFlight: 1}, 2) {
		t.Fatal("a direct delivery in flight must not finish the drain")
	}
	if progress.observe(drainObservation{directPersisted: 4, groupInFlight: 1}, 2) {
		t.Fatal("a group delivery in flight must not finish the drain")
	}
	if progress.observe(empty, 2) {
		t.Fatal("the stable window must restart after an in-flight delivery")
	}
	if !progress.observe(empty, 2) {
		t.Fatal("two clean polls after the in-flight delivery must finish")
	}
}

func TestDrainLoopKeepsPollingWhileInFlight(t *testing.T) {
	observations := []drainObservation{
		{directPersisted: 1, directInFlight: 1},
		{directPersisted: 1, groupInFlight: 1},
		{directPersisted: 1},
		{directPersisted: 1},
	}
	index := 0
	loop := drainLoop{
		pollEvery:   time.Second,
		stablePolls: 2,
		observe: func(context.Context) (drainObservation, error) {
			if index >= len(observations) {
				t.Fatalf("polled past the scripted observations at %d", index)
			}
			observation := observations[index]
			index++
			return observation, nil
		},
		wait: func(context.Context, time.Duration) bool { return true },
	}

	if _, err := loop.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if index != len(observations) {
		t.Fatalf(
			"polls = %d, want %d: an in-flight delivery must restart the stable window",
			index, len(observations),
		)
	}
}

type fakeQueueDepthReader struct {
	depths map[string]int
	err    error
}

func (r fakeQueueDepthReader) Depth(_ context.Context, queue string) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.depths[queue], nil
}

func TestVerifyQueuesEmpty(t *testing.T) {
	ctx := context.Background()
	if err := verifyQueuesEmpty(ctx, fakeQueueDepthReader{depths: map[string]int{
		"direct": 0, "group": 0,
	}}, "direct", "group"); err != nil {
		t.Fatalf("empty queues must pass: %v", err)
	}
	for _, queue := range []string{"direct", "group"} {
		depths := map[string]int{"direct": 0, "group": 0}
		depths[queue] = 1
		err := verifyQueuesEmpty(ctx, fakeQueueDepthReader{depths: depths}, "direct", "group")
		if err == nil || !strings.Contains(err.Error(), queue) ||
			!strings.Contains(err.Error(), "still holds 1 message") {
			t.Fatalf("%s depth error = %v", queue, err)
		}
	}
	inspectErr := errors.New("declare failed")
	if err := verifyQueuesEmpty(
		ctx, fakeQueueDepthReader{err: inspectErr}, "direct", "group",
	); !errors.Is(err, inspectErr) {
		t.Fatalf("inspect error = %v, want %v", err, inspectErr)
	}
}

func TestDrainLoopStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	loop := drainLoop{
		pollEvery:   time.Second,
		stablePolls: 2,
		observe: func(context.Context) (drainObservation, error) {
			called = true
			return drainObservation{}, nil
		},
		wait: func(context.Context, time.Duration) bool { return true },
	}

	if _, err := loop.run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("run polled after the context was canceled")
	}
}

func TestDrainLoopStopsWhenWaitIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	loop := drainLoop{
		pollEvery:   time.Second,
		stablePolls: 2,
		observe: func(context.Context) (drainObservation, error) {
			return drainObservation{directBacklog: 1}, nil
		},
		wait: func(context.Context, time.Duration) bool {
			cancel()
			return false
		},
	}

	if _, err := loop.run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context.Canceled", err)
	}
}

func TestDrainLoopPropagatesInspectError(t *testing.T) {
	inspectErr := errors.New("inspect failed")
	loop := drainLoop{
		pollEvery:   time.Second,
		stablePolls: 2,
		observe: func(context.Context) (drainObservation, error) {
			return drainObservation{}, inspectErr
		},
		wait: func(context.Context, time.Duration) bool { return true },
	}

	if _, err := loop.run(context.Background()); !errors.Is(err, inspectErr) {
		t.Fatalf("run error = %v, want %v", err, inspectErr)
	}
}

func TestIsQueueMissing(t *testing.T) {
	if !isQueueMissing(&amqp091.Error{Code: amqp091.NotFound, Reason: "no queue"}) {
		t.Fatal("a 404 declare error must mean the queue is missing")
	}
	if isQueueMissing(&amqp091.Error{Code: amqp091.PreconditionFailed, Reason: "mismatch"}) {
		t.Fatal("a 406 declare error must not mean the queue is missing")
	}
	if isQueueMissing(errors.New("socket closed")) {
		t.Fatal("an unrelated error must not mean the queue is missing")
	}
}

type fakeDrainPersister struct{}

func (fakeDrainPersister) PersistMessage(context.Context, store.Message) (bool, error) {
	return true, nil
}

func (fakeDrainPersister) PersistGroupMessage(context.Context, store.GroupMessage) (bool, error) {
	return true, nil
}

func TestDrainPersistQueuesValidatesOptions(t *testing.T) {
	unusedDialer := func(config.RabbitMQConfig) (*amqp091.Connection, error) {
		t.Fatal("the drain must not dial when its options are invalid")
		return nil, nil
	}
	persister := fakeDrainPersister{}
	rabbit := config.RabbitMQConfig{URL: "amqp://guest:guest@127.0.0.1:1/"}
	cases := []struct {
		name    string
		options QueueDrainOptions
	}{
		{
			name:    "missing dialer",
			options: QueueDrainOptions{Rabbit: rabbit, DirectPersister: persister},
		},
		{
			name:    "missing persister",
			options: QueueDrainOptions{Rabbit: rabbit, Dialer: unusedDialer},
		},
		{
			name:    "missing RabbitMQ configuration",
			options: QueueDrainOptions{Dialer: unusedDialer, DirectPersister: persister},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := DrainPersistQueues(context.Background(), testCase.options); err == nil {
				t.Fatal("DrainPersistQueues must reject invalid options")
			}
		})
	}
}

func TestRequireGroupPersister(t *testing.T) {
	if err := requireGroupPersister("group.queue", 0, false); err != nil {
		t.Fatalf("an empty group queue needs no persister: %v", err)
	}
	if err := requireGroupPersister("group.queue", 3, true); err != nil {
		t.Fatalf("a configured persister drains any backlog: %v", err)
	}
	err := requireGroupPersister("group.queue", 3, false)
	if err == nil || !strings.Contains(err.Error(), "group.queue") ||
		!strings.Contains(err.Error(), "3 message(s)") ||
		!strings.Contains(err.Error(), "refusing to clear") {
		t.Fatalf("unconsumed group backlog error = %v", err)
	}
}
