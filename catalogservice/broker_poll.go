package catalogservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalogproto"
)

// pollReplyMargin is withheld from a long-poll's park so an empty response
// still reaches the peer inside the operation's server deadline.
const pollReplyMargin = 500 * time.Millisecond

// brokerInstance is one bound broker lane: the successor to the withdrawn
// broker.open stream. The pump goroutine pulls commands from the runtime's
// BrokerSession into a bounded un-resulted window that broker.poll pages and
// broker.result settles; the instance id fences every poll and result against
// replacement.
type brokerInstance struct {
	id        catalogproto.BrokerInstanceID
	principal string
	session   daemonkit.Session
	broker    BrokerSession

	mu      sync.Mutex
	window  []catalogproto.BrokerCommand
	failure error
	wake    chan struct{}
	resume  chan struct{}

	settleOnce sync.Once
	stop       chan struct{}
	done       chan struct{}
}

func newBrokerInstance(principal string, session daemonkit.Session, broker BrokerSession) (*brokerInstance, error) {
	id, err := mintBrokerInstanceID()
	if err != nil {
		return nil, err
	}
	return &brokerInstance{
		id: id, principal: principal, session: session, broker: broker,
		wake:   make(chan struct{}),
		resume: make(chan struct{}, 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}, nil
}

// poison settles the instance with its terminal cause exactly once; the pump
// observes the stop signal and closes the runtime session with the cause.
func (i *brokerInstance) poison(cause error) {
	i.settleOnce.Do(func() {
		i.mu.Lock()
		i.failure = cause
		i.mu.Unlock()
		close(i.stop)
	})
}

// pageAfter returns the un-resulted commands past the cursor, or the terminal
// failure once the instance settles.
func (i *brokerInstance) pageAfter(cursor uint64) ([]catalogproto.BrokerCommand, uint64, <-chan struct{}, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.failure != nil {
		return nil, 0, nil, i.failure
	}
	var page []catalogproto.BrokerCommand
	for _, command := range i.window {
		if command.CommandID > cursor {
			page = append(page, command)
		}
	}
	if len(page) == 0 {
		return nil, 0, i.wake, nil
	}
	return page, page[len(page)-1].CommandID, nil, nil
}

// pump owns the instance's window: it pulls validated commands from the
// runtime session only while the un-resulted window sits under the outstanding
// bound — exactly the withdrawn serveBroker's backpressure — and terminates on
// replacement, peer disconnect, runtime settlement, or drain.
func (i *brokerInstance) pump(draining <-chan struct{}, finish func()) {
	var terminal error
	defer func() {
		i.poison(terminal)
		i.broker.Close(terminal)
		i.mu.Lock()
		wake := i.wake
		i.wake = make(chan struct{})
		i.mu.Unlock()
		close(wake)
		close(i.done)
		finish()
	}()
	var lastCommandID uint64
	for {
		i.mu.Lock()
		outstanding := len(i.window)
		i.mu.Unlock()
		var commands <-chan catalogproto.BrokerCommand
		if outstanding < int(catalogproto.MaxOutstandingBrokerCommands) {
			commands = i.broker.Commands()
		}
		select {
		case <-i.stop:
			i.mu.Lock()
			terminal = i.failure
			i.mu.Unlock()
			return
		case <-draining:
			terminal = &CodedError{Code: catalogproto.ErrorCodeUnavailable, Message: "catalog service: daemon is draining"}
			return
		case <-i.session.Disconnected():
			terminal = errBrokerSessionLost
			return
		case <-i.broker.Done():
			terminal = errBrokerSessionLost
			return
		case <-i.resume:
		case command, ok := <-commands:
			if !ok {
				if outstanding != 0 {
					terminal = &CodedError{
						Code:    catalogproto.ErrorCodeIntegrity,
						Message: "catalog service: broker command stream closed with pending commands",
					}
					return
				}
				terminal = &CodedError{Code: catalogproto.ErrorCodeUnavailable, Message: "catalog service: broker command stream closed"}
				return
			}
			if err := catalogproto.Validate(command); err != nil {
				terminal = &CodedError{Code: catalogproto.ErrorCodeIntegrity, Message: boundedErrorMessage(err.Error()), Cause: err}
				return
			}
			if command.CommandID <= lastCommandID || command.CommandID == ^uint64(0) {
				terminal = &CodedError{
					Code:    catalogproto.ErrorCodeIntegrity,
					Message: boundedErrorMessage(fmt.Sprintf("catalog service: broker command id %d is not strictly increasing", command.CommandID)),
				}
				return
			}
			lastCommandID = command.CommandID
			i.mu.Lock()
			i.window = append(i.window, command)
			wake := i.wake
			i.wake = make(chan struct{})
			i.mu.Unlock()
			close(wake)
		}
	}
}

// acceptResult correlates one posted result with its un-resulted command and
// hands it to the runtime session; an unmatched or refused result poisons the
// lane exactly as the withdrawn stream's integrity terminal did.
func (i *brokerInstance) acceptResult(ctx context.Context, result catalogproto.BrokerResult) error {
	i.mu.Lock()
	if i.failure != nil {
		failure := i.failure
		i.mu.Unlock()
		return failure
	}
	matched := -1
	for index, command := range i.window {
		if command.CommandID == result.CommandID {
			if command.Kind != result.Kind {
				break
			}
			matched = index
			break
		}
	}
	if matched < 0 {
		i.mu.Unlock()
		cause := &CodedError{Code: catalogproto.ErrorCodeIntegrity, Message: "catalog service: unmatched broker result"}
		i.poison(cause)
		return cause
	}
	i.window = append(i.window[:matched], i.window[matched+1:]...)
	i.mu.Unlock()
	select {
	case i.resume <- struct{}{}:
	default:
	}
	if err := i.broker.AcceptResult(ctx, result); err != nil {
		i.poison(err)
		return err
	}
	return nil
}

// bindBrokerInstance settles any incumbent instance for the principal, opens a
// fresh runtime broker session, and registers the replacement — the successor
// to the withdrawn replaceBroker, fenced by the minted instance id. A repeat
// bind from the already-bound session returns the live instance, so a broker
// that lost the bind response re-polls without retiring its own process.
func (s *Server) bindBrokerInstance(ctx context.Context, principal string, identity Identity) (*brokerInstance, error) {
	for {
		s.brokerMu.Lock()
		current := s.brokers[principal]
		if current == nil {
			s.brokerMu.Unlock()
			break
		}
		s.brokerMu.Unlock()
		if current.session.ID() == identity.Session.ID() {
			current.mu.Lock()
			live := current.failure == nil
			current.mu.Unlock()
			// A poisoned instance is never handed back, even while its pump is
			// still tearing down — the rebind settles it and opens fresh.
			if live {
				select {
				case <-current.done:
				default:
					return current, nil
				}
			}
		}
		current.poison(&CodedError{Code: catalogproto.ErrorCodeUnavailable, Message: "catalog service: broker instance replaced"})
		select {
		case <-current.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		s.brokerMu.Lock()
		if s.brokers[principal] == current {
			delete(s.brokers, principal)
		}
		s.brokerMu.Unlock()
	}
	broker, err := s.fileProvider.Broker.OpenBroker(ctx, identity, principal)
	if err != nil {
		return nil, err
	}
	if broker == nil || broker.Commands() == nil {
		if broker != nil {
			broker.Close(errors.New("catalog service: broker returned no command stream"))
		}
		return nil, &CodedError{Code: catalogproto.ErrorCodeIntegrity, Message: "catalog service: broker returned no command stream"}
	}
	instance, err := newBrokerInstance(principal, identity.Session, broker)
	if err != nil {
		broker.Close(err)
		return nil, err
	}
	s.brokerMu.Lock()
	if current := s.brokers[principal]; current != nil {
		select {
		case <-current.done:
		default:
			s.brokerMu.Unlock()
			broker.Close(errors.New("catalog service: broker bind lost a concurrent replacement"))
			return nil, &CodedError{Code: catalogproto.ErrorCodeUnavailable, Message: "catalog service: broker bind lost a concurrent replacement"}
		}
	}
	s.brokers[principal] = instance
	s.brokerMu.Unlock()
	go instance.pump(s.draining(), func() {
		s.brokerMu.Lock()
		if s.brokers[principal] == instance {
			delete(s.brokers, principal)
		}
		s.brokerMu.Unlock()
	})
	return instance, nil
}

// boundBrokerInstance resolves a named live instance, fencing stale ids and
// foreign sessions to the replacement that owns the lane now.
func (s *Server) boundBrokerInstance(principal string, id catalogproto.BrokerInstanceID, session daemonkit.Session) (*brokerInstance, error) {
	s.brokerMu.Lock()
	instance := s.brokers[principal]
	s.brokerMu.Unlock()
	if instance == nil || instance.id != id || instance.session.ID() != session.ID() {
		return nil, &CodedError{Code: catalogproto.ErrorCodeUnavailable, Message: "catalog service: broker instance is stale"}
	}
	return instance, nil
}

func (s *Server) draining() <-chan struct{} {
	if s.fileProvider == nil {
		return nil
	}
	return s.fileProvider.Broker.Draining()
}

func (s *Server) handleBrokerPoll(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.BrokerPollRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
		return encoded(catalogproto.BrokerPollResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()),
		})
	}
	_, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationBrokerPoll, 0, false)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.BrokerPollResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	var instance *brokerInstance
	if input.Instance == nil {
		instance, err = s.bindBrokerInstance(ctx, authorization.Principal, identity)
	} else {
		instance, err = s.boundBrokerInstance(authorization.Principal, *input.Instance, identity.Session)
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.BrokerPollResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	timer := time.NewTimer(pollWait(ctx, input.WaitMillis))
	defer timer.Stop()
	for {
		commands, next, wake, err := instance.pageAfter(input.Cursor)
		if err != nil {
			code, message := applicationError(err)
			return encoded(catalogproto.BrokerPollResponse{Protocol: catalogproto.Version, Code: code, Message: message})
		}
		if len(commands) > 0 {
			return encoded(catalogproto.BrokerPollResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				Instance: &instance.id, Commands: commands, NextCursor: next,
			})
		}
		select {
		case <-wake:
		case <-timer.C:
			return encoded(catalogproto.BrokerPollResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				Instance: &instance.id, Commands: []catalogproto.BrokerCommand{},
			})
		case <-ctx.Done():
			return encoded(catalogproto.BrokerPollResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				Instance: &instance.id, Commands: []catalogproto.BrokerCommand{},
			})
		}
	}
}

func (s *Server) handleBrokerResult(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.PostBrokerResultRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
		return encoded(catalogproto.PostBrokerResultResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()),
		})
	}
	_, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationBrokerResult, 0, false)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.PostBrokerResultResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	instance, err := s.boundBrokerInstance(authorization.Principal, input.Instance, identity.Session)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.PostBrokerResultResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if err := instance.acceptResult(ctx, input.Result); err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.PostBrokerResultResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.PostBrokerResultResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk})
}

// pollWait bounds a long-poll's park under both the protocol ceiling and the
// operation's server deadline, leaving the reply margin to deliver an empty
// page instead of a deadline error.
func pollWait(ctx context.Context, waitMillis uint32) time.Duration {
	if waitMillis > catalogproto.MaxPollWaitMillis {
		waitMillis = catalogproto.MaxPollWaitMillis
	}
	wait := time.Duration(waitMillis) * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok {
		budget := time.Until(deadline) - pollReplyMargin
		if budget < 0 {
			budget = 0
		}
		if wait > budget {
			wait = budget
		}
	}
	return wait
}
