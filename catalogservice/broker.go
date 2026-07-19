package catalogservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogproto"
)

const maxOutstandingBrokerCommands = 32

type brokerResultEvent struct {
	result catalogproto.BrokerResult
	err    error
}

func (s *Server) handleBrokerOpen(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.BrokerOpenRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return emptyBrokerStream(catalogproto.BrokerOpenResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(),
		})
	}
	_, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationBrokerOpen, 0, false)
	if err != nil {
		code, message := applicationError(err)
		return emptyBrokerStream(catalogproto.BrokerOpenResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	brokerContext, finish, err := s.replaceBroker(ctx, authorization.Principal)
	if err != nil {
		code, message := applicationError(err)
		return emptyBrokerStream(catalogproto.BrokerOpenResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	session, err := s.config.Broker.OpenBroker(brokerContext, identity, authorization.Principal)
	if err != nil {
		finish()
		code, message := applicationError(err)
		return emptyBrokerStream(catalogproto.BrokerOpenResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if session == nil || session.Commands() == nil {
		finish()
		if session != nil {
			session.Close(errors.New("catalog service: broker returned no command stream"))
		}
		return emptyBrokerStream(catalogproto.BrokerOpenResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: "broker returned no command stream",
		})
	}
	chunks := make(chan []byte)
	terminal := new(json.RawMessage)
	go serveBroker(brokerContext, finish, request.Chunks, session, chunks, terminal)
	return wire.StreamResponse{Chunks: chunks, Value: terminal}, nil
}

func (s *Server) handleCutoverDomains(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.CutoverDomainsRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.CutoverDomainsResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(),
		})
	}
	_, _, _, err := s.authorize(ctx, request, catalogproto.OperationBrokerCutoverDomains, 0, false)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.CutoverDomainsResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	proof, err := s.config.Broker.CutoverDomains(ctx, input.Plan)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.CutoverDomainsResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.CutoverDomainsResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Proof: &proof,
	})
}

func (s *Server) handleProveBrokerPeer(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.ProveBrokerPeerRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.ProveBrokerPeerResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(),
		})
	}
	_, _, _, err := s.authorize(ctx, request, catalogproto.OperationBrokerProvePeer, 0, false)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ProveBrokerPeerResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	proof, err := s.config.Broker.ProveBrokerPeer(ctx)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ProveBrokerPeerResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.ProveBrokerPeerResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Proof: &proof,
	})
}

func (s *Server) handleClaimDomainCutover(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.ClaimDomainCutoverRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.ClaimDomainCutoverResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(),
		})
	}
	_, _, _, err := s.authorize(ctx, request, catalogproto.OperationBrokerClaimCutover, 0, false)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ClaimDomainCutoverResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	claim, err := s.config.Broker.ClaimDomainCutover(ctx, input.Proof)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ClaimDomainCutoverResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.ClaimDomainCutoverResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Claim: &claim,
	})
}

func (s *Server) handleRecoverDomainCutoverClaim(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.RecoverDomainCutoverClaimRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.RecoverDomainCutoverClaimResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(),
		})
	}
	_, _, _, err := s.authorize(ctx, request, catalogproto.OperationBrokerRecoverCutoverClaim, 0, false)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.RecoverDomainCutoverClaimResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	claim, err := s.config.Broker.RecoverDomainCutoverClaim(ctx, input.Proof)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.RecoverDomainCutoverClaimResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.RecoverDomainCutoverClaimResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Claim: &claim,
	})
}

func (s *Server) handleRecoverDomainCutoverReceipt(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.RecoverDomainCutoverReceiptRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.RecoverDomainCutoverReceiptResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(),
		})
	}
	_, _, _, err := s.authorize(ctx, request, catalogproto.OperationBrokerRecoverCutoverReceipt, 0, false)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.RecoverDomainCutoverReceiptResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	receipt, err := s.config.Broker.RecoverDomainCutoverReceipt(ctx, input.Key)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.RecoverDomainCutoverReceiptResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.RecoverDomainCutoverReceiptResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Receipt: &receipt,
	})
}

func (s *Server) replaceBroker(ctx context.Context, principal string) (context.Context, func(), error) {
	s.brokerMu.Lock()
	if previous := s.brokers[principal]; previous != nil {
		previous.cancel()
		select {
		case <-previous.done:
		case <-ctx.Done():
			s.brokerMu.Unlock()
			return nil, nil, ctx.Err()
		}
	}
	brokerContext, cancel := context.WithCancel(ctx)
	slot := &brokerSlot{cancel: cancel, done: make(chan struct{})}
	s.brokers[principal] = slot
	s.brokerMu.Unlock()
	finish := func() {
		cancel()
		close(slot.done)
		s.brokerMu.Lock()
		if s.brokers[principal] == slot {
			delete(s.brokers, principal)
		}
		s.brokerMu.Unlock()
	}
	return brokerContext, finish, nil
}

func serveBroker(ctx context.Context, finish func(), input <-chan wire.Chunk, session BrokerSession, output chan<- []byte, terminal *json.RawMessage) {
	defer finish()
	defer close(output)
	resultEvents := make(chan brokerResultEvent)
	go readBrokerResults(ctx, input, resultEvents)
	pending := make(map[uint64]catalogproto.BrokerCommandKind)
	var lastCommandID uint64
	var terminalErr error
	defer func() { session.Close(terminalErr) }()
	for {
		var commands <-chan catalogproto.BrokerCommand
		if len(pending) < maxOutstandingBrokerCommands {
			commands = session.Commands()
		}
		select {
		case <-ctx.Done():
			terminalErr = ctx.Err()
			*terminal = mustEncode(catalogproto.BrokerOpenResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeUnavailable, Message: ctx.Err().Error(),
			})
			return
		case event := <-resultEvents:
			if event.err != nil {
				terminalErr = event.err
				code, message := applicationError(event.err)
				if errors.Is(event.err, io.EOF) {
					code, message = catalogproto.ErrorCodeUnavailable, "broker closed its result stream"
				}
				*terminal = mustEncode(catalogproto.BrokerOpenResponse{Protocol: catalogproto.Version, Code: code, Message: message})
				return
			}
			expected, ok := pending[event.result.CommandID]
			if !ok || expected != event.result.Kind {
				terminalErr = errors.New("catalog service: unmatched broker result")
				*terminal = mustEncode(catalogproto.BrokerOpenResponse{
					Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: terminalErr.Error(),
				})
				return
			}
			delete(pending, event.result.CommandID)
			if err := session.AcceptResult(ctx, event.result); err != nil {
				terminalErr = err
				code, message := applicationError(err)
				*terminal = mustEncode(catalogproto.BrokerOpenResponse{Protocol: catalogproto.Version, Code: code, Message: message})
				return
			}
		case command, ok := <-commands:
			if !ok {
				if len(pending) != 0 {
					terminalErr = errors.New("catalog service: broker command stream closed with pending commands")
					*terminal = mustEncode(catalogproto.BrokerOpenResponse{
						Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: terminalErr.Error(),
					})
					return
				}
				*terminal = mustEncode(catalogproto.BrokerOpenResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk})
				return
			}
			if err := catalogproto.Validate(command); err != nil {
				terminalErr = err
				*terminal = mustEncode(catalogproto.BrokerOpenResponse{
					Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: err.Error(),
				})
				return
			}
			if command.CommandID <= lastCommandID || command.CommandID == ^uint64(0) {
				terminalErr = fmt.Errorf("catalog service: broker command id %d is not strictly increasing", command.CommandID)
				*terminal = mustEncode(catalogproto.BrokerOpenResponse{
					Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: terminalErr.Error(),
				})
				return
			}
			lastCommandID = command.CommandID
			payload, err := catalogproto.Encode(command)
			if err != nil {
				terminalErr = err
				*terminal = mustEncode(catalogproto.BrokerOpenResponse{
					Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: err.Error(),
				})
				return
			}
			pending[command.CommandID] = command.Kind
			select {
			case output <- payload:
			case <-ctx.Done():
				terminalErr = ctx.Err()
				*terminal = mustEncode(catalogproto.BrokerOpenResponse{
					Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeUnavailable, Message: ctx.Err().Error(),
				})
				return
			}
		}
	}
}

func readBrokerResults(ctx context.Context, input <-chan wire.Chunk, events chan<- brokerResultEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case chunk, ok := <-input:
			if !ok {
				sendBrokerResultEvent(ctx, events, brokerResultEvent{err: io.EOF})
				return
			}
			if len(chunk.Payload) > 0 {
				var result catalogproto.BrokerResult
				if err := catalogproto.Decode(chunk.Payload, &result); err != nil {
					sendBrokerResultEvent(ctx, events, brokerResultEvent{err: &CodedError{
						Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(), Cause: err,
					}})
					return
				}
				if !sendBrokerResultEvent(ctx, events, brokerResultEvent{result: result}) {
					return
				}
			}
			if chunk.End {
				sendBrokerResultEvent(ctx, events, brokerResultEvent{err: io.EOF})
				return
			}
		}
	}
}

func sendBrokerResultEvent(ctx context.Context, events chan<- brokerResultEvent, event brokerResultEvent) bool {
	select {
	case events <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func emptyBrokerStream(response catalogproto.BrokerOpenResponse) (any, error) {
	payload, err := catalogproto.Encode(response)
	if err != nil {
		return nil, err
	}
	chunks := make(chan []byte)
	close(chunks)
	raw := json.RawMessage(payload)
	return wire.StreamResponse{Chunks: chunks, Value: &raw}, nil
}
