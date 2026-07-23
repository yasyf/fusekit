package holder

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	errPresentationStartFailed        = errors.New("FuseKit runtime: presentation start failed")
	errPresentationBackendLost        = errors.New("FuseKit runtime: presentation backend lost")
	errPresentationShutdownIncomplete = errors.New("FuseKit runtime: presentation shutdown incomplete")
)

type presentationStartPhase uint8

const (
	presentationStartIdle presentationStartPhase = iota
	presentationStartRunning
	presentationStartReady
	presentationStartQuarantined
	presentationStartTerminal
)

type presentationStartFailure struct {
	name     string
	cause    error
	retryAt  time.Time
	terminal bool
}

func (e *presentationStartFailure) Error() string {
	if e.terminal {
		return fmt.Sprintf("%v: %s: terminal: %v", errPresentationStartFailed, e.name, e.cause)
	}
	return fmt.Sprintf("%v: %s: retry at %s: %v", errPresentationStartFailed, e.name, e.retryAt.Format(time.RFC3339Nano), e.cause)
}

func (e *presentationStartFailure) Unwrap() []error {
	return []error{errPresentationStartFailed, e.cause}
}

func (e *presentationStartFailure) RetryAt() (time.Time, bool) {
	return e.retryAt, !e.terminal
}

func (e *presentationStartFailure) RetryEligible(now time.Time) bool {
	return !e.terminal && !now.Before(e.retryAt)
}

// presentationOperation is sealed to holder-owned, settlement-proving backends.
type presentationOperation interface {
	start(context.Context) error
	ready(context.Context) error
	healthy() error
	stop(context.Context) error
	wait(context.Context) error
}

// presentationOperationFactory allocates only in-memory operation state.
type presentationOperationFactory interface {
	newPresentationOperation() (presentationOperation, error)
}

type presentationStart struct {
	lifetime          context.Context
	timeout           time.Duration
	settlementTimeout time.Duration
	name              string
	factory           presentationOperationFactory
	now               func() time.Time

	mu       sync.Mutex
	phase    presentationStartPhase
	done     chan struct{}
	failures uint
	err      *presentationStartFailure
	op       presentationOperation
	closed   bool
}

func newPresentationStart(
	lifetime context.Context,
	timeout time.Duration,
	settlementTimeout time.Duration,
	name string,
	factory presentationOperationFactory,
) (*presentationStart, error) {
	return newPresentationStartWithClock(lifetime, timeout, settlementTimeout, name, factory, time.Now)
}

func newPresentationStartWithClock(
	lifetime context.Context,
	timeout time.Duration,
	settlementTimeout time.Duration,
	name string,
	factory presentationOperationFactory,
	now func() time.Time,
) (*presentationStart, error) {
	switch {
	case lifetime == nil:
		return nil, errors.New("FuseKit runtime: presentation lifetime is required")
	case timeout <= 0:
		return nil, errors.New("FuseKit runtime: presentation start timeout must be positive")
	case settlementTimeout <= 0:
		return nil, errors.New("FuseKit runtime: presentation settlement timeout must be positive")
	case name == "":
		return nil, errors.New("FuseKit runtime: presentation name is required")
	case factory == nil:
		return nil, errors.New("FuseKit runtime: presentation operation factory is required")
	case now == nil:
		return nil, errors.New("FuseKit runtime: presentation clock is required")
	default:
		return &presentationStart{
			lifetime: lifetime, timeout: timeout, settlementTimeout: settlementTimeout,
			name: name, factory: factory, now: now,
		}, nil
	}
}

func (s *presentationStart) Ensure(ctx context.Context) error {
	if ctx == nil {
		return errors.New("FuseKit runtime: presentation waiter context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		err := s.terminalFailureLocked(errors.New("FuseKit runtime: presentation manager is closed"))
		s.mu.Unlock()
		return err
	}
	if lifetimeErr := s.lifetime.Err(); lifetimeErr != nil {
		if s.phase == presentationStartIdle || s.phase == presentationStartQuarantined || s.phase == presentationStartTerminal {
			err := s.terminalFailureLocked(lifetimeErr)
			s.mu.Unlock()
			return err
		}
		err := &presentationStartFailure{name: s.name, cause: lifetimeErr, terminal: true}
		s.mu.Unlock()
		return err
	}
	switch s.phase {
	case presentationStartReady:
		if err := s.op.healthy(); err == nil {
			s.mu.Unlock()
			return nil
		} else {
			s.beginSettlementLocked(errors.Join(errPresentationBackendLost, err))
		}
	case presentationStartTerminal:
		err := s.err
		s.mu.Unlock()
		return err
	case presentationStartQuarantined:
		if !s.err.RetryEligible(s.now()) {
			err := s.err
			s.mu.Unlock()
			return err
		}
		if err := s.beginStartLocked(); err != nil {
			s.mu.Unlock()
			return err
		}
	case presentationStartIdle:
		if err := s.beginStartLocked(); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	done := s.done
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		s.mu.Lock()
		err := s.err
		ready := s.phase == presentationStartReady
		s.mu.Unlock()
		if ready {
			return nil
		}
		return err
	}
}

func (s *presentationStart) beginStartLocked() error {
	op, err := s.factory.newPresentationOperation()
	if err != nil {
		return s.quarantineLocked(err)
	}
	if op == nil {
		return s.quarantineLocked(errors.New("FuseKit runtime: presentation operation is nil"))
	}
	s.phase = presentationStartRunning
	s.done = make(chan struct{})
	s.op = op
	s.err = nil
	go s.runStart(op)
	return nil
}

func (s *presentationStart) beginSettlementLocked(cause error) {
	s.phase = presentationStartRunning
	s.done = make(chan struct{})
	op := s.op
	s.err = nil
	go s.runSettlement(op, cause)
}

func (s *presentationStart) runStart(op presentationOperation) {
	ctx, cancel := context.WithTimeout(s.lifetime, s.timeout)
	err := op.start(ctx)
	if err == nil {
		err = op.ready(ctx)
	}
	if err == nil {
		err = ctx.Err()
	}
	cancel()
	if err != nil {
		s.finishSettlement(op, err)
		return
	}

	s.mu.Lock()
	if s.op != op || s.phase != presentationStartRunning {
		s.mu.Unlock()
		s.finishSettlement(op, errors.New("FuseKit runtime: presentation start lost ownership"))
		return
	}
	s.phase = presentationStartReady
	s.failures = 0
	s.err = nil
	close(s.done)
	s.mu.Unlock()
}

func (s *presentationStart) runSettlement(op presentationOperation, cause error) {
	s.finishSettlement(op, cause)
}

func (s *presentationStart) finishSettlement(op presentationOperation, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.settlementTimeout)
	stopErr := op.stop(ctx)
	waitErr := op.wait(ctx)
	ctxErr := ctx.Err()
	cancel()

	s.mu.Lock()
	if s.op != op {
		s.mu.Unlock()
		return
	}
	if stopErr != nil || waitErr != nil || ctxErr != nil {
		s.phase = presentationStartTerminal
		s.err = &presentationStartFailure{
			name:     s.name,
			cause:    errors.Join(errPresentationShutdownIncomplete, cause, stopErr, waitErr, ctxErr),
			terminal: true,
		}
	} else if lifetimeErr := s.lifetime.Err(); lifetimeErr != nil {
		s.phase = presentationStartTerminal
		s.err = &presentationStartFailure{name: s.name, cause: errors.Join(cause, lifetimeErr), terminal: true}
	} else {
		s.quarantineLocked(cause)
	}
	s.op = nil
	close(s.done)
	s.mu.Unlock()
}

func (s *presentationStart) quarantineLocked(cause error) *presentationStartFailure {
	delay := presentationRetryDelay(s.failures)
	s.failures++
	s.phase = presentationStartQuarantined
	s.err = &presentationStartFailure{name: s.name, cause: cause, retryAt: s.now().Add(delay)}
	return s.err
}

func (s *presentationStart) terminalFailureLocked(cause error) *presentationStartFailure {
	if s.phase == presentationStartTerminal && s.err != nil {
		return s.err
	}
	s.phase = presentationStartTerminal
	s.err = &presentationStartFailure{name: s.name, cause: cause, terminal: true}
	return s.err
}

func (s *presentationStart) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("FuseKit runtime: presentation close context is required")
	}
	for {
		s.mu.Lock()
		s.closed = true
		if s.phase == presentationStartRunning {
			done := s.done
			s.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return errors.Join(errPresentationShutdownIncomplete, ctx.Err())
			}
		}
		op := s.op
		terminalErr := s.err
		s.op = nil
		s.mu.Unlock()
		if op == nil {
			if errors.Is(terminalErr, errPresentationShutdownIncomplete) {
				return terminalErr
			}
			return nil
		}
		stopErr := op.stop(ctx)
		waitErr := op.wait(ctx)
		if stopErr != nil || waitErr != nil || ctx.Err() != nil {
			return errors.Join(errPresentationShutdownIncomplete, stopErr, waitErr, ctx.Err())
		}
		return nil
	}
}

func (s *presentationStart) status() (presentationStartPhase, *presentationStartFailure) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.phase, s.err
}

func presentationRetryDelay(failures uint) time.Duration {
	switch failures {
	case 0:
		return time.Second
	case 1:
		return 2 * time.Second
	case 2:
		return 4 * time.Second
	case 3:
		return 8 * time.Second
	case 4:
		return 16 * time.Second
	default:
		return 30 * time.Second
	}
}
