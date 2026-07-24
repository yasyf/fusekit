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
	presentationStartFailed
	presentationStartClosed
)

type presentationStartFailure struct {
	name  string
	cause error
}

func (e *presentationStartFailure) Error() string {
	return fmt.Sprintf("%v: %s: %v", errPresentationStartFailed, e.name, e.cause)
}

func (e *presentationStartFailure) Unwrap() []error {
	return []error{errPresentationStartFailed, e.cause}
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

	mu     sync.Mutex
	phase  presentationStartPhase
	done   chan struct{}
	err    error
	op     presentationOperation
	closed bool
}

func newPresentationStart(
	lifetime context.Context,
	timeout time.Duration,
	settlementTimeout time.Duration,
	name string,
	factory presentationOperationFactory,
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
	default:
		return &presentationStart{
			lifetime: lifetime, timeout: timeout, settlementTimeout: settlementTimeout,
			name: name, factory: factory,
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
		err := s.failLocked(errors.New("FuseKit runtime: presentation manager is closed"))
		s.mu.Unlock()
		return err
	}
	if lifetimeErr := s.lifetime.Err(); lifetimeErr != nil {
		err := s.failLocked(lifetimeErr)
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
	case presentationStartFailed, presentationStartClosed:
		err := s.err
		s.mu.Unlock()
		return err
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
		return s.failLocked(err)
	}
	if op == nil {
		return s.failLocked(errors.New("FuseKit runtime: presentation operation is nil"))
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
	s.phase = presentationStartFailed
	s.err = &presentationStartFailure{name: s.name, cause: errors.Join(cause, func() error {
		if stopErr != nil || waitErr != nil || ctxErr != nil {
			return errors.Join(errPresentationShutdownIncomplete, stopErr, waitErr, ctxErr)
		}
		return nil
	}())}
	if stopErr == nil && waitErr == nil && ctxErr == nil {
		s.op = nil
	}
	close(s.done)
	s.mu.Unlock()
}

func (s *presentationStart) failLocked(cause error) error {
	if (s.phase == presentationStartFailed || s.phase == presentationStartClosed) && s.err != nil {
		return s.err
	}
	s.phase = presentationStartFailed
	s.err = &presentationStartFailure{name: s.name, cause: cause}
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
		s.mu.Unlock()
		if errors.Is(terminalErr, errPresentationShutdownIncomplete) {
			return terminalErr
		}
		if op == nil {
			s.mu.Lock()
			s.phase = presentationStartClosed
			s.mu.Unlock()
			return nil
		}
		stopErr := op.stop(ctx)
		waitErr := op.wait(ctx)
		if stopErr != nil || waitErr != nil || ctx.Err() != nil {
			return errors.Join(errPresentationShutdownIncomplete, stopErr, waitErr, ctx.Err())
		}
		s.mu.Lock()
		s.op = nil
		s.phase = presentationStartClosed
		s.mu.Unlock()
		return nil
	}
}
