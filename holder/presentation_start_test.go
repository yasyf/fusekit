package holder

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPresentationStartCoalescesWaiters(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	op := &presentationTestOperation{readyEntered: entered, readyRelease: release}
	start := newPresentationTestStart(t, t.Context(), newPresentationTestClock(), op)

	const waiters = 32
	results := make(chan error, waiters)
	for range waiters {
		go func() { results <- start.Ensure(t.Context()) }()
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("presentation start did not begin")
	}
	close(release)
	for range waiters {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if op.starts.Load() != 1 || op.readies.Load() != 1 {
		t.Fatalf("start/ready calls = %d/%d", op.starts.Load(), op.readies.Load())
	}
	if err := start.Ensure(t.Context()); err != nil {
		t.Fatalf("ready Ensure: %v", err)
	}
}

func TestPresentationStartWaiterCancellationDoesNotCancelOwnedAttempt(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	op := &presentationTestOperation{readyEntered: entered, readyRelease: release}
	start := newPresentationTestStart(t, t.Context(), newPresentationTestClock(), op)

	waiter, cancel := context.WithCancel(t.Context())
	canceled := make(chan error, 1)
	go func() { canceled <- start.Ensure(waiter) }()
	<-entered
	cancel()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter = %v", err)
	}
	joined := make(chan error, 1)
	go func() { joined <- start.Ensure(t.Context()) }()
	close(release)
	if err := <-joined; err != nil {
		t.Fatalf("joined waiter: %v", err)
	}
	if op.stops.Load() != 0 || op.waits.Load() != 0 {
		t.Fatalf("waiter cancellation stopped backend = %d/%d", op.stops.Load(), op.waits.Load())
	}
}

func TestPresentationStartQuarantineRetriesOnlyAfterEligibility(t *testing.T) {
	clock := newPresentationTestClock()
	want := errors.New("native unavailable")
	failed := &presentationTestOperation{startErr: want}
	recovered := &presentationTestOperation{}
	start := newPresentationTestStart(t, t.Context(), clock, failed, recovered)

	first := start.Ensure(t.Context())
	var failure *presentationStartFailure
	if !errors.As(first, &failure) || !errors.Is(first, want) {
		t.Fatalf("first failure = %v", first)
	}
	retryAt, ok := failure.RetryAt()
	if !ok || !retryAt.Equal(clock.Now().Add(time.Second)) {
		t.Fatalf("retry = %s, %t", retryAt, ok)
	}
	if got := start.Ensure(t.Context()); got != first {
		t.Fatalf("quarantine returned %p, want %p", got, first)
	}
	if failed.stops.Load() != 1 || failed.waits.Load() != 1 {
		t.Fatalf("failed attempt settlement = %d/%d", failed.stops.Load(), failed.waits.Load())
	}
	clock.Advance(time.Second)
	if err := start.Ensure(t.Context()); err != nil {
		t.Fatalf("eligible retry: %v", err)
	}
	if recovered.starts.Load() != 1 || recovered.readies.Load() != 1 {
		t.Fatalf("recovery calls = %d/%d", recovered.starts.Load(), recovered.readies.Load())
	}
}

func TestPresentationStartBackoffIsDeterministicAndCapped(t *testing.T) {
	clock := newPresentationTestClock()
	ops := make([]*presentationTestOperation, 7)
	for index := range ops {
		ops[index] = &presentationTestOperation{startErr: errors.New("failed")}
	}
	start := newPresentationTestStart(t, t.Context(), clock, ops...)
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for index, delay := range want {
		err := start.Ensure(t.Context())
		var failure *presentationStartFailure
		if !errors.As(err, &failure) {
			t.Fatalf("attempt %d = %v", index+1, err)
		}
		retryAt, ok := failure.RetryAt()
		if !ok || !retryAt.Equal(clock.Now().Add(delay)) {
			t.Fatalf("attempt %d retry = %s, want %s", index+1, retryAt, clock.Now().Add(delay))
		}
		clock.Advance(delay)
	}
}

func TestPresentationStartUnprovenSettlementIsTerminal(t *testing.T) {
	clock := newPresentationTestClock()
	unsettled := errors.New("worker was not reaped")
	op := &presentationTestOperation{startErr: errors.New("failed"), waitErr: unsettled}
	start := newPresentationTestStart(t, t.Context(), clock, op, &presentationTestOperation{})

	first := start.Ensure(t.Context())
	var failure *presentationStartFailure
	if !errors.As(first, &failure) || !failure.terminal ||
		!errors.Is(first, errPresentationShutdownIncomplete) || !errors.Is(first, unsettled) {
		t.Fatalf("terminal settlement = %v", first)
	}
	clock.Advance(time.Hour)
	if got := start.Ensure(t.Context()); got != first {
		t.Fatalf("terminal retry = %p, want stored %p", got, first)
	}
	if closeErr := start.Close(t.Context()); closeErr != first {
		t.Fatalf("terminal close = %p, want stored %p", closeErr, first)
	}
}

func TestPresentationStartLifetimeCanceledBeforeEnsureDoesNotAllocate(t *testing.T) {
	lifetime, cancel := context.WithCancel(t.Context())
	cancel()
	factory := &presentationTestFactory{operations: []presentationOperation{&presentationTestOperation{}}}
	start, err := newPresentationStartWithClock(
		lifetime, time.Second, time.Second, "broker", factory, newPresentationTestClock().Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	err = start.Ensure(t.Context())
	var failure *presentationStartFailure
	if !errors.As(err, &failure) || !failure.terminal || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lifetime = %v", err)
	}
	if factory.calls.Load() != 0 {
		t.Fatalf("factory calls after cancellation = %d", factory.calls.Load())
	}
}

func TestPresentationStartDetectsLossAndSettlesBeforeQuarantine(t *testing.T) {
	clock := newPresentationTestClock()
	lost := errors.New("native child exited")
	first := &presentationTestOperation{}
	second := &presentationTestOperation{}
	start := newPresentationTestStart(t, t.Context(), clock, first, second)
	if err := start.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	first.setHealth(lost)
	err := start.Ensure(t.Context())
	if !errors.Is(err, errPresentationBackendLost) || !errors.Is(err, lost) {
		t.Fatalf("lost backend = %v", err)
	}
	if first.stops.Load() != 1 || first.waits.Load() != 1 {
		t.Fatalf("lost backend settlement = %d/%d", first.stops.Load(), first.waits.Load())
	}
	clock.Advance(time.Second)
	if err := start.Ensure(t.Context()); err != nil {
		t.Fatalf("loss recovery: %v", err)
	}
	if second.starts.Load() != 1 {
		t.Fatalf("replacement starts = %d", second.starts.Load())
	}
}

func TestPresentationStartCloseStopsAndWaitsReadyOperation(t *testing.T) {
	op := &presentationTestOperation{}
	start := newPresentationTestStart(t, t.Context(), newPresentationTestClock(), op)
	if err := start.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := start.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if op.stops.Load() != 1 || op.waits.Load() != 1 {
		t.Fatalf("close settlement = %d/%d", op.stops.Load(), op.waits.Load())
	}
}

func TestPresentationStartAttemptTimeoutSettlesThenQuarantines(t *testing.T) {
	clock := newPresentationTestClock()
	op := &presentationTestOperation{readyWaitContext: true}
	factory := &presentationTestFactory{operations: []presentationOperation{op}}
	start, err := newPresentationStartWithClock(t.Context(), 10*time.Millisecond, time.Second, "native", factory, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	err = start.Ensure(t.Context())
	var failure *presentationStartFailure
	if !errors.As(err, &failure) || failure.terminal || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout = %v", err)
	}
	if op.stops.Load() != 1 || op.waits.Load() != 1 {
		t.Fatalf("timeout settlement = %d/%d", op.stops.Load(), op.waits.Load())
	}
}

type presentationTestFactory struct {
	mu         sync.Mutex
	operations []presentationOperation
	calls      atomic.Int64
}

func (f *presentationTestFactory) newPresentationOperation() (presentationOperation, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.operations) == 0 {
		return nil, errors.New("no presentation operation")
	}
	op := f.operations[0]
	f.operations = f.operations[1:]
	return op, nil
}

type presentationTestOperation struct {
	starts  atomic.Int64
	readies atomic.Int64
	stops   atomic.Int64
	waits   atomic.Int64

	startErr         error
	readyErr         error
	stopErr          error
	waitErr          error
	readyEntered     chan struct{}
	readyRelease     chan struct{}
	readyWaitContext bool

	mu        sync.Mutex
	healthErr error
}

func (o *presentationTestOperation) start(context.Context) error {
	o.starts.Add(1)
	return o.startErr
}

func (o *presentationTestOperation) ready(ctx context.Context) error {
	o.readies.Add(1)
	if o.readyEntered != nil {
		close(o.readyEntered)
	}
	if o.readyWaitContext {
		<-ctx.Done()
		return ctx.Err()
	}
	if o.readyRelease != nil {
		select {
		case <-o.readyRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return o.readyErr
}

func (o *presentationTestOperation) healthy() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.healthErr
}

func (o *presentationTestOperation) stop(context.Context) error {
	o.stops.Add(1)
	return o.stopErr
}

func (o *presentationTestOperation) wait(context.Context) error {
	o.waits.Add(1)
	return o.waitErr
}

func (o *presentationTestOperation) setHealth(err error) {
	o.mu.Lock()
	o.healthErr = err
	o.mu.Unlock()
}

func newPresentationTestStart(
	t *testing.T,
	lifetime context.Context,
	clock *presentationTestClock,
	operations ...*presentationTestOperation,
) *presentationStart {
	t.Helper()
	values := make([]presentationOperation, len(operations))
	for index := range operations {
		values[index] = operations[index]
	}
	start, err := newPresentationStartWithClock(
		lifetime, time.Second, time.Second, "native",
		&presentationTestFactory{operations: values}, clock.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	return start
}

type presentationTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newPresentationTestClock() *presentationTestClock {
	return &presentationTestClock{now: time.Unix(1_000_000, 0)}
}

func (c *presentationTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *presentationTestClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}
