package sourceauthority

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/yasyf/daemonkit"
)

const (
	fseventsObserverBuild    = "fusekit-source-observer-v1"
	fseventsObserverProtocol = uint16(1)
	fseventsObserverChildArg = "--fusekit-source-observer-child"

	fseventsOpStage      = "fsevents.stage"
	fseventsOpOpen       = "fsevents.open"
	fseventsOpActivate   = "fsevents.activate"
	fseventsOpFlush      = "fsevents.flush"
	fseventsOpDrain      = "fsevents.drain"
	fseventsOpNack       = "fsevents.nack"
	fseventsOpClose      = "fsevents.close"
	fseventsCloseTimeout = 5 * time.Second

	maxObserverRoots              = 4096
	maxObserverCheckpoints        = 4096
	maxObserverPayloadBytes       = 1 << 20
	maxObserverCheckpointBytes    = 1 << 20
	maxObserverTerminalErrorBytes = 4 << 10
)

// ObserverProcessSpec identifies one private observer-child invocation.
type ObserverProcessSpec struct {
	Arguments []string
}

// ObserverProcess is one fixed-signed supervised child. Stop must terminate,
// reap, and durably untrack the exact process group before returning.
type ObserverProcess interface {
	// Business opens the unary lane the exact spawned child serves.
	Business(context.Context, daemonkit.Contract) (*daemonkit.Business, error)
	Stop(context.Context) error
}

// ObserverProcessLauncher starts the same fixed signed executable in observer
// child mode and returns only after launch has settled. If the context expires,
// it returns only after no child remains or with the exact owned process.
type ObserverProcessLauncher interface {
	LaunchSourceObserver(context.Context, ObserverProcessSpec) (ObserverProcess, error)
}

type fseventsProxyBackend struct {
	launcher       ObserverProcessLauncher
	deadlines      OperationDeadlines
	controlTimeout time.Duration
}

type fseventsProxyStream struct {
	opMu       sync.Mutex
	deliveryMu sync.Mutex
	mu         sync.Mutex

	process        ObserverProcess
	client         sourceSessionClient
	caller         spawnedCaller
	sink           DurableEventSink
	sinkCtx        context.Context
	cancelSink     context.CancelFunc
	checkpoints    []StreamCheckpoint
	eventErr       error
	closed         bool
	controlTimeout time.Duration
	eventsDone     chan struct{}
	stopOnce       sync.Once
	stopErr        error
	terminateOnce  sync.Once
	terminated     chan struct{}
	terminateErr   error
}

type observerOpenRequest struct {
	Protocol uint16               `json:"protocol"`
	Config   observerOpenManifest `json:"config"`
}

type observerCheckpointResponse struct {
	Protocol    uint16             `json:"protocol"`
	Checkpoints []StreamCheckpoint `json:"checkpoints"`
}

type observerRequest struct {
	Protocol uint16 `json:"protocol"`
}

type observerDrainRequest struct {
	Protocol uint16        `json:"protocol"`
	Cursor   uint64        `json:"cursor"`
	Wait     time.Duration `json:"wait"`
}

type observerDrainResponse struct {
	Protocol uint16     `json:"protocol"`
	Pending  bool       `json:"pending"`
	Sequence uint64     `json:"sequence,omitempty"`
	Batch    EventBatch `json:"batch,omitempty"`
}

type observerNackRequest struct {
	Protocol uint16 `json:"protocol"`
	Sequence uint64 `json:"sequence"`
	Error    string `json:"error"`
}

// NewFSEventsBackend returns a parent-side backend that never loads
// CoreServices. Every native stream lives in a fixed-signed supervised child.
func NewFSEventsBackend(
	launcher ObserverProcessLauncher,
	deadlines OperationDeadlines,
) (EventBackend, error) {
	if err := deadlines.validate(); err != nil {
		return nil, err
	}
	if launcher == nil {
		return nil, errors.New("sourceauthority: observer process launcher is required")
	}
	return &fseventsProxyBackend{
		launcher: launcher, deadlines: deadlines, controlTimeout: deadlines.ObserverControl,
	}, nil
}

// FSEventsObserverChildArguments returns the exact hard-cut child invocation.
func FSEventsObserverChildArguments() []string {
	return []string{fseventsObserverChildArg}
}

func (b *fseventsProxyBackend) Open(
	ctx context.Context,
	roots []RootSpec,
	resume []StreamCheckpoint,
	sink DurableEventSink,
) (EventStream, error) {
	if sink == nil {
		return nil, errors.New("sourceauthority: durable event sink is required")
	}
	deadlines, err := observerSpawnedDeadlines(b.deadlines)
	if err != nil {
		return nil, err
	}
	manifest, err := planObserverOpenPages(roots, resume)
	if err != nil {
		return nil, err
	}
	request := observerOpenRequest{Protocol: fseventsObserverProtocol, Config: manifest}
	if _, err := validateFSEventsOpen(roots, resume); err != nil {
		return nil, err
	}
	sinkBase := context.WithoutCancel(ctx)
	openCtx, cancelOpen := context.WithTimeout(ctx, b.controlTimeout)
	defer cancelOpen()
	arguments := FSEventsObserverChildArguments()
	process, launchErr := b.launcher.LaunchSourceObserver(openCtx, ObserverProcessSpec{
		Arguments: arguments,
	})
	if launchErr != nil || process == nil || openCtx.Err() != nil {
		var contextErr error
		if err := openCtx.Err(); err != nil {
			contextErr = fmt.Errorf("sourceauthority: launch observer child: %w", err)
		}
		var stopErr error
		if process != nil {
			stopErr = stopObserverProcessWithin(process, b.controlTimeout)
		}
		if launchErr == nil && process == nil {
			launchErr = errors.New("observer launcher returned no process")
		}
		if launchErr != nil {
			launchErr = fmt.Errorf("sourceauthority: launch observer child: %w", launchErr)
		}
		return nil, errors.Join(contextErr, launchErr, stopErr)
	}
	client, err := openObserverProcessSession(openCtx, process)
	if err != nil {
		stopErr := stopObserverProcess(process)
		return nil, errors.Join(fmt.Errorf("sourceauthority: connect observer child: %w", err), stopErr)
	}
	sinkCtx, cancelSink := context.WithCancel(sinkBase)
	stream := &fseventsProxyStream{
		process: process, client: client, caller: spawnedCaller{client: client, deadlines: deadlines},
		sink: sink, sinkCtx: sinkCtx, cancelSink: cancelSink,
		eventsDone: make(chan struct{}), terminated: make(chan struct{}),
		controlTimeout: b.controlTimeout,
	}

	var response observerCheckpointResponse
	if err := stream.open(openCtx, request, roots, resume, &response); err != nil {
		return nil, errors.Join(err, stream.terminateBeforeDrain())
	}
	if err := validateObserverCheckpoints(response); err != nil {
		return nil, errors.Join(err, stream.terminateBeforeDrain())
	}
	stream.mu.Lock()
	stream.checkpoints = cloneCheckpoints(response.Checkpoints)
	stream.mu.Unlock()
	go stream.runDrain()
	return stream, nil
}

func (s *fseventsProxyStream) Checkpoints() []StreamCheckpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneCheckpoints(s.checkpoints)
}

func (s *fseventsProxyStream) Activate(ctx context.Context) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.ready(); err != nil {
		return err
	}
	var response observerCheckpointResponse
	if err := s.call(ctx, fseventsOpActivate, observerRequest{Protocol: fseventsObserverProtocol}, &response); err != nil {
		return errors.Join(err, s.terminate())
	}
	s.deliveryMu.Lock()
	validationErr := validateObserverCheckpoints(response)
	if validationErr == nil {
		s.setCheckpoints(response.Checkpoints)
	}
	s.deliveryMu.Unlock()
	if validationErr != nil {
		return errors.Join(validationErr, s.terminate())
	}
	return nil
}

func (s *fseventsProxyStream) Flush(ctx context.Context) ([]StreamCheckpoint, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.ready(); err != nil {
		return nil, err
	}
	var response observerCheckpointResponse
	if err := s.call(ctx, fseventsOpFlush, observerRequest{Protocol: fseventsObserverProtocol}, &response); err != nil {
		return nil, errors.Join(err, s.terminate())
	}
	s.deliveryMu.Lock()
	validationErr := validateObserverCheckpoints(response)
	if validationErr == nil {
		s.setCheckpoints(response.Checkpoints)
	}
	s.deliveryMu.Unlock()
	if validationErr != nil {
		return nil, errors.Join(validationErr, s.terminate())
	}
	return cloneCheckpoints(response.Checkpoints), nil
}

func (s *fseventsProxyStream) Close() error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return s.terminate()
	}
	s.mu.Unlock()
	closeCtx, cancel := context.WithTimeout(context.Background(), fseventsCloseTimeout)
	defer cancel()
	var response observerCheckpointResponse
	callErr := s.call(closeCtx, fseventsOpClose, observerRequest{Protocol: fseventsObserverProtocol}, &response)
	if callErr == nil {
		s.deliveryMu.Lock()
		callErr = validateObserverCheckpoints(response)
		if callErr == nil {
			s.setCheckpoints(response.Checkpoints)
		}
		s.deliveryMu.Unlock()
	}
	return errors.Join(callErr, s.terminate())
}

// runDrain is the successor to the child's event push: one long-poll per
// batch, whose next call is the durable-delivery acknowledgement of the last.
func (s *fseventsProxyStream) runDrain() {
	defer close(s.eventsDone)
	var acknowledged uint64
	for {
		if s.drainDone() {
			return
		}
		response, err := s.drain(acknowledged)
		if err != nil {
			if s.drainDone() {
				return
			}
			s.setEventError(err)
			_ = s.abortTransport(err)
			return
		}
		if !response.Pending {
			continue
		}
		if response.Sequence != acknowledged+1 {
			err := errors.New("sourceauthority: observer child event sequence violated")
			s.setEventError(err)
			_ = s.abortTransport(err)
			return
		}
		s.deliveryMu.Lock()
		sinkErr := s.deliver(response.Batch)
		if delivered := sinkErr == nil || errors.Is(sinkErr, ErrSnapshotRequired); !delivered {
			nackErr := s.nack(response.Sequence, boundedObserverErrorMessage(sinkErr.Error()))
			s.deliveryMu.Unlock()
			err := errors.Join(sinkErr, nackErr)
			s.setEventError(err)
			_ = s.abortTransport(err)
			return
		}
		if err := s.advanceCheckpoint(response.Batch); err != nil {
			s.deliveryMu.Unlock()
			s.setEventError(err)
			_ = s.abortTransport(err)
			return
		}
		s.deliveryMu.Unlock()
		acknowledged = response.Sequence
	}
}

func (s *fseventsProxyStream) drainDone() bool {
	if s.sinkCtx.Err() != nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *fseventsProxyStream) drain(cursor uint64) (observerDrainResponse, error) {
	request := observerDrainRequest{
		Protocol: fseventsObserverProtocol, Cursor: cursor, Wait: observerDrainWait,
	}
	payload, err := marshalObserverControl(request)
	if err != nil {
		return observerDrainResponse{}, err
	}
	body, err := s.caller.call(s.sinkCtx, fseventsOpDrain, payload)
	if err != nil {
		return observerDrainResponse{}, fmt.Errorf("sourceauthority: observer %s: %w", fseventsOpDrain, err)
	}
	var response observerDrainResponse
	if err := decodeObserver(body, &response); err != nil {
		return observerDrainResponse{}, fmt.Errorf("sourceauthority: decode observer %s response: %w", fseventsOpDrain, err)
	}
	if response.Protocol != fseventsObserverProtocol ||
		(!response.Pending && (response.Sequence != 0 || response.Batch.Stream != "")) {
		return observerDrainResponse{}, errors.New("sourceauthority: observer child sent an invalid drain response")
	}
	return response, nil
}

func (s *fseventsProxyStream) nack(sequence uint64, message string) error {
	var response observerRequest
	err := s.call(s.sinkCtx, fseventsOpNack, observerNackRequest{
		Protocol: fseventsObserverProtocol, Sequence: sequence, Error: message,
	}, &response)
	if err != nil {
		return err
	}
	if response.Protocol != fseventsObserverProtocol {
		return errors.New("sourceauthority: observer negative acknowledgement was invalid")
	}
	return nil
}

func (s *fseventsProxyStream) deliver(batch EventBatch) error {
	deliveryCtx, cancelDelivery := context.WithTimeout(s.sinkCtx, s.controlTimeout)
	defer cancelDelivery()

	result := make(chan error, 1)
	go func() {
		result <- s.sink(deliveryCtx, batch)
	}()

	select {
	case err := <-result:
		return err
	case <-deliveryCtx.Done():
		transportErr := s.abortTransport(deliveryCtx.Err())
		sinkErr := <-result
		return errors.Join(deliveryCtx.Err(), sinkErr, transportErr)
	}
}

func (s *fseventsProxyStream) call(ctx context.Context, op string, request, response any) error {
	payload, err := marshalObserverControl(request)
	if err != nil {
		return err
	}
	body, err := s.caller.call(ctx, op, payload)
	if err != nil {
		return fmt.Errorf("sourceauthority: observer %s: %w", op, err)
	}
	if err := decodeObserver(body, response); err != nil {
		return fmt.Errorf("sourceauthority: decode observer %s response: %w", op, err)
	}
	return nil
}

func (s *fseventsProxyStream) open(
	ctx context.Context,
	request observerOpenRequest,
	roots []RootSpec,
	resume []StreamCheckpoint,
	response *observerCheckpointResponse,
) error {
	if err := sendObserverOpenPages(ctx, s.caller, roots, resume, request.Config); err != nil {
		return fmt.Errorf("sourceauthority: observer %s: %w", fseventsOpStage, err)
	}
	return s.call(ctx, fseventsOpOpen, request, response)
}

func (s *fseventsProxyStream) ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.eventErr
}

func (s *fseventsProxyStream) setCheckpoints(checkpoints []StreamCheckpoint) {
	s.mu.Lock()
	s.checkpoints = cloneCheckpoints(checkpoints)
	s.mu.Unlock()
}

func (s *fseventsProxyStream) advanceCheckpoint(batch EventBatch) error {
	if batch.Cursor == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := range s.checkpoints {
		if s.checkpoints[index].Identity == batch.Stream && s.checkpoints[index].RootEpoch == batch.RootEpoch &&
			(s.checkpoints[index].Cursor == batch.Predecessor || s.checkpoints[index].Cursor == batch.Cursor) {
			if s.checkpoints[index].Cursor == batch.Predecessor {
				s.checkpoints[index].Cursor = batch.Cursor
			}
			return nil
		}
	}
	return errors.New("sourceauthority: observer event escaped its checkpoint fence")
}

func (s *fseventsProxyStream) setEventError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.eventErr == nil {
		s.eventErr = err
	}
	s.mu.Unlock()
}

func (s *fseventsProxyStream) stopProcess() error {
	s.stopOnce.Do(func() { s.stopErr = stopObserverProcessWithin(s.process, s.controlTimeout) })
	return s.stopErr
}

func (s *fseventsProxyStream) abortTransport(cause error) error {
	s.setEventError(cause)
	return errors.Join(closeSourceSession(s.client, s.controlTimeout), s.stopProcess())
}

// terminateBeforeDrain retires a stream whose drain loop never started, so
// terminate's join on eventsDone would block forever.
func (s *fseventsProxyStream) terminateBeforeDrain() error {
	close(s.eventsDone)
	return s.terminate()
}

func (s *fseventsProxyStream) terminate() error {
	s.terminateOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.cancelSink()
		clientErr := closeSourceSession(s.client, s.controlTimeout)
		stopErr := s.stopProcess()
		<-s.eventsDone
		s.mu.Lock()
		s.terminateErr = errors.Join(s.eventErr, stopErr, clientErr)
		s.mu.Unlock()
		close(s.terminated)
	})
	<-s.terminated
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.terminateErr
}

func stopObserverProcess(process ObserverProcess) error {
	return stopObserverProcessWithin(process, fseventsCloseTimeout)
}

func stopObserverProcessWithin(process ObserverProcess, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return process.Stop(ctx)
}

func validateObserverCheckpoints(response observerCheckpointResponse) error {
	if response.Protocol != fseventsObserverProtocol {
		return errors.New("sourceauthority: observer protocol mismatch")
	}
	if err := validateCheckpoints(response.Checkpoints); err != nil {
		return err
	}
	return nil
}

func decodeObserver(payload []byte, target any) error {
	if len(payload) == 0 || len(payload) > maxObserverPayloadBytes {
		return errors.New("sourceauthority: observer payload exceeds its encoded budget")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("sourceauthority: trailing observer payload")
	}
	return validateObserverPayload(target, len(payload))
}

func marshalObserverControl(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("sourceauthority: encode observer payload: %w", err)
	}
	if err := validateObserverPayload(value, len(payload)); err != nil {
		return nil, err
	}
	return payload, nil
}

func validateObserverPayload(value any, encodedBytes int) error {
	if err := validateSourceTaskStrings(reflect.ValueOf(value)); err != nil {
		return err
	}
	switch message := value.(type) {
	case observerOpenRequest:
		return validateObserverOpenHeader(message.Config, encodedBytes)
	case *observerOpenRequest:
		return validateObserverOpenHeader(message.Config, encodedBytes)
	case observerCheckpointResponse:
		return validateObserverCheckpointBounds(len(message.Checkpoints), encodedBytes)
	case *observerCheckpointResponse:
		return validateObserverCheckpointBounds(len(message.Checkpoints), encodedBytes)
	case observerNackRequest:
		return validateObserverAckError(message.Error)
	case *observerNackRequest:
		return validateObserverAckError(message.Error)
	default:
		if encodedBytes > maxObserverPayloadBytes {
			return errors.New("sourceauthority: observer payload exceeds its encoded budget")
		}
		return nil
	}
}

func validateObserverOpenHeader(config observerOpenManifest, encodedBytes int) error {
	if err := validateObserverOpenManifest(config); err != nil {
		return err
	}
	if encodedBytes > sourceTaskJSONByteLimit {
		return errors.New("sourceauthority: observer open header exceeds its encoded budget")
	}
	return nil
}

func validateObserverCheckpointBounds(checkpoints, encodedBytes int) error {
	if checkpoints > maxObserverCheckpoints {
		return errors.New("sourceauthority: observer checkpoint response exceeds the item limit")
	}
	if encodedBytes > maxObserverCheckpointBytes {
		return errors.New("sourceauthority: observer checkpoint response exceeds the encoded budget")
	}
	return nil
}

func validateObserverAckError(message string) error {
	if len(message) > maxObserverTerminalErrorBytes || !utf8.ValidString(message) ||
		strings.IndexByte(message, 0) >= 0 {
		return errors.New("sourceauthority: observer acknowledgement error exceeds the terminal-message limit")
	}
	return nil
}

func boundedObserverErrorMessage(message string) string {
	if !utf8.ValidString(message) {
		message = strings.ToValidUTF8(message, "�")
	}
	message = strings.ReplaceAll(message, "\x00", "?")
	if len(message) <= maxObserverTerminalErrorBytes {
		return message
	}
	message = message[:maxObserverTerminalErrorBytes]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}

var _ EventBackend = (*fseventsProxyBackend)(nil)
var _ EventStream = (*fseventsProxyStream)(nil)
