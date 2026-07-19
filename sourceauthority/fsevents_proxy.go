package sourceauthority

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/wire"
)

const (
	fseventsObserverBuild    = "fusekit-source-observer-v1"
	fseventsObserverProtocol = uint16(1)
	fseventsObserverChildArg = "--fusekit-source-observer-child"

	fseventsOpOpen       wire.Op = "fsevents.open"
	fseventsOpActivate   wire.Op = "fsevents.activate"
	fseventsOpFlush      wire.Op = "fsevents.flush"
	fseventsOpAck        wire.Op = "fsevents.ack"
	fseventsOpClose      wire.Op = "fsevents.close"
	fseventsEventTopic           = "fsevents.batch"
	fseventsCloseTimeout         = 5 * time.Second

	maxObserverRoots              = 4096
	maxObserverCheckpoints        = 4096
	maxObserverPayloadBytes       = 1 << 20
	maxObserverCheckpointBytes    = 1 << 20
	maxObserverTerminalErrorBytes = 4 << 10
)

// ObserverProcessSpec identifies one private observer-child invocation.
type ObserverProcessSpec struct {
	Socket    string
	Arguments []string
}

// ObserverProcess is one fixed-signed supervised child. Stop must terminate,
// reap, and durably untrack the exact process group before returning.
type ObserverProcess interface {
	// Dial returns only a connection whose server peer matches the exact
	// supervised process record. Same-UID or path ownership alone is invalid.
	Dial(context.Context) (net.Conn, error)
	Stop(context.Context) error
}

// ObserverProcessLauncher starts the same fixed signed executable in observer
// child mode and returns only after launch has settled. If the context expires,
// it returns only after no child remains or with the exact owned process.
type ObserverProcessLauncher interface {
	LaunchSourceObserver(context.Context, ObserverProcessSpec) (ObserverProcess, error)
}

type fseventsProxyBackend struct {
	runtimeDir     string
	launcher       ObserverProcessLauncher
	controlTimeout time.Duration
}

type fseventsProxyStream struct {
	opMu       sync.Mutex
	deliveryMu sync.Mutex
	mu         sync.Mutex

	process        ObserverProcess
	client         *wire.Client
	sink           DurableEventSink
	sinkCtx        context.Context
	cancelSink     context.CancelFunc
	temporary      string
	checkpoints    []StreamCheckpoint
	nextEvent      uint64
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

type observerEvent struct {
	Protocol uint16     `json:"protocol"`
	Sequence uint64     `json:"sequence"`
	Batch    EventBatch `json:"batch"`
}

type observerAckRequest struct {
	Protocol  uint16 `json:"protocol"`
	Sequence  uint64 `json:"sequence"`
	Delivered bool   `json:"delivered"`
	Error     string `json:"error,omitempty"`
}

// NewFSEventsBackend returns a parent-side backend that never loads
// CoreServices. Every native stream lives in a fixed-signed supervised child.
func NewFSEventsBackend(
	runtimeDir string,
	launcher ObserverProcessLauncher,
	deadlines OperationDeadlines,
) (EventBackend, error) {
	if err := deadlines.validate(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(runtimeDir) || filepath.Clean(runtimeDir) != runtimeDir {
		return nil, errors.New("sourceauthority: observer runtime directory is invalid")
	}
	if len(filepath.Join(runtimeDir, "source-observer-0000000000", "observer.sock")) >= 100 {
		return nil, errors.New("sourceauthority: observer runtime directory exceeds the Unix socket path limit")
	}
	if launcher == nil {
		return nil, errors.New("sourceauthority: observer process launcher is required")
	}
	return &fseventsProxyBackend{
		runtimeDir: runtimeDir, launcher: launcher, controlTimeout: deadlines.ObserverControl,
	}, nil
}

// FSEventsObserverChildArguments returns the exact hard-cut child invocation.
func FSEventsObserverChildArguments(socketPath string) ([]string, error) {
	if !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath || len(socketPath) >= 100 {
		return nil, errors.New("sourceauthority: observer child socket path is invalid")
	}
	return []string{fseventsObserverChildArg, socketPath}, nil
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
	temporary, err := os.MkdirTemp(b.runtimeDir, "source-observer-")
	if err != nil {
		return nil, fmt.Errorf("sourceauthority: create observer socket directory: %w", err)
	}
	if err := os.Chmod(temporary, 0o700); err != nil {
		_ = os.Remove(temporary)
		return nil, fmt.Errorf("sourceauthority: secure observer socket directory: %w", err)
	}
	socketPath := filepath.Join(temporary, "observer.sock")
	arguments, err := FSEventsObserverChildArguments(socketPath)
	if err != nil {
		_ = os.Remove(temporary)
		return nil, err
	}
	process, launchErr := b.launcher.LaunchSourceObserver(openCtx, ObserverProcessSpec{
		Socket: socketPath, Arguments: arguments,
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
		removeErr := os.RemoveAll(temporary)
		if launchErr == nil && process == nil {
			launchErr = errors.New("observer launcher returned no process")
		}
		if launchErr != nil {
			launchErr = fmt.Errorf("sourceauthority: launch observer child: %w", launchErr)
		}
		return nil, errors.Join(contextErr, launchErr, stopErr, removeErr)
	}
	client, err := wire.NewClient(openCtx, wire.ClientConfig{
		Build:      fseventsObserverBuild,
		Dial:       process.Dial,
		EventQueue: 1,
	})
	if err != nil {
		stopErr := stopObserverProcess(process)
		_ = os.RemoveAll(temporary)
		return nil, errors.Join(fmt.Errorf("sourceauthority: connect observer child: %w", err), stopErr)
	}
	sinkCtx, cancelSink := context.WithCancel(sinkBase)
	stream := &fseventsProxyStream{
		process: process, client: client, sink: sink, sinkCtx: sinkCtx, cancelSink: cancelSink,
		temporary: temporary, eventsDone: make(chan struct{}), terminated: make(chan struct{}),
		controlTimeout: b.controlTimeout,
	}
	go stream.runEvents()

	var response observerCheckpointResponse
	if err := stream.open(openCtx, request, roots, resume, &response); err != nil {
		_ = stream.abortTransport(err)
		return nil, errors.Join(err, stream.terminate())
	}
	if err := validateObserverCheckpoints(response); err != nil {
		_ = stream.abortTransport(err)
		return nil, errors.Join(err, stream.terminate())
	}
	stream.mu.Lock()
	stream.checkpoints = cloneCheckpoints(response.Checkpoints)
	stream.mu.Unlock()
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
		_ = s.abortTransport(err)
		return errors.Join(err, s.terminate())
	}
	s.deliveryMu.Lock()
	validationErr := validateObserverCheckpoints(response)
	if validationErr == nil {
		s.setCheckpoints(response.Checkpoints)
	}
	s.deliveryMu.Unlock()
	if validationErr != nil {
		_ = s.abortTransport(validationErr)
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
		_ = s.abortTransport(err)
		return nil, errors.Join(err, s.terminate())
	}
	s.deliveryMu.Lock()
	validationErr := validateObserverCheckpoints(response)
	if validationErr == nil {
		s.setCheckpoints(response.Checkpoints)
	}
	s.deliveryMu.Unlock()
	if validationErr != nil {
		_ = s.abortTransport(validationErr)
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
	if callErr != nil {
		_ = s.abortTransport(callErr)
	}
	return errors.Join(callErr, s.terminate())
}

func (s *fseventsProxyStream) runEvents() {
	defer close(s.eventsDone)
	for event := range s.client.Events() {
		if event.Topic != fseventsEventTopic {
			err := errors.New("sourceauthority: observer child sent an unknown event")
			s.setEventError(err)
			_ = s.abortTransport(err)
			return
		}
		var envelope observerEvent
		if err := decodeObserver(event.Payload, &envelope); err != nil || envelope.Protocol != fseventsObserverProtocol {
			err := errors.New("sourceauthority: observer child sent an invalid event")
			s.setEventError(err)
			_ = s.abortTransport(err)
			return
		}
		s.mu.Lock()
		expected := s.nextEvent + 1
		if envelope.Sequence != expected || s.closed {
			s.mu.Unlock()
			err := errors.New("sourceauthority: observer child event sequence violated")
			s.setEventError(err)
			_ = s.abortTransport(err)
			return
		}
		s.nextEvent = expected
		s.mu.Unlock()

		s.deliveryMu.Lock()
		sinkErr := s.deliver(envelope.Batch)
		delivered := sinkErr == nil || errors.Is(sinkErr, ErrSnapshotRequired)
		ack := observerAckRequest{Protocol: fseventsObserverProtocol, Sequence: envelope.Sequence, Delivered: delivered}
		if !delivered {
			ack.Error = boundedObserverErrorMessage(sinkErr.Error())
		}
		var response observerRequest
		ackErr := s.call(s.sinkCtx, fseventsOpAck, ack, &response)
		if ackErr != nil || response.Protocol != fseventsObserverProtocol || !delivered {
			s.deliveryMu.Unlock()
			err := errors.Join(sinkErr, ackErr)
			if err == nil {
				err = errors.New("sourceauthority: observer acknowledgement was invalid")
			}
			s.setEventError(err)
			_ = s.abortTransport(err)
			return
		}
		if err := s.advanceCheckpoint(envelope.Batch); err != nil {
			s.deliveryMu.Unlock()
			s.setEventError(err)
			_ = s.abortTransport(err)
			return
		}
		s.deliveryMu.Unlock()
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if !closed {
		err := errors.New("sourceauthority: observer child event stream closed")
		s.setEventError(err)
		_ = s.abortTransport(err)
	}
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

func (s *fseventsProxyStream) call(ctx context.Context, op wire.Op, request, response any) error {
	ctx, cancel := context.WithTimeout(ctx, s.controlTimeout)
	defer cancel()
	payload, err := marshalObserverControl(request)
	if err != nil {
		return err
	}
	result, err := s.client.Call(ctx, op, "", payload)
	if err != nil {
		return fmt.Errorf("sourceauthority: observer %s: %w", op, err)
	}
	if result.Outcome != wire.Delivered || result.Response.Rejected || result.Response.Err != "" {
		detail := result.Response.Err
		if detail == "" {
			detail = result.Response.Reason
		}
		detail = boundedObserverErrorMessage(detail)
		return fmt.Errorf("sourceauthority: observer %s was not delivered: %s", op, detail)
	}
	if err := decodeObserver(result.Response.Payload, response); err != nil {
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
	ctx, cancel := context.WithTimeout(ctx, s.controlTimeout)
	defer cancel()
	payload, err := marshalObserverControl(request)
	if err != nil {
		return err
	}
	call, err := s.client.Open(ctx, fseventsOpOpen, "", payload, false)
	if err != nil {
		return fmt.Errorf("sourceauthority: observer %s: %w", fseventsOpOpen, err)
	}
	if err := sendObserverOpenPages(ctx, call, roots, resume, request.Config); err != nil {
		call.Cancel()
		return fmt.Errorf("sourceauthority: observer %s: %w", fseventsOpOpen, err)
	}
	result, err := call.Response(ctx)
	if err != nil {
		return fmt.Errorf("sourceauthority: observer %s: %w", fseventsOpOpen, err)
	}
	if result.Outcome != wire.Delivered || result.Response.Rejected || result.Response.Err != "" {
		detail := result.Response.Err
		if detail == "" {
			detail = result.Response.Reason
		}
		detail = boundedObserverErrorMessage(detail)
		return fmt.Errorf("sourceauthority: observer %s was not delivered: %s", fseventsOpOpen, detail)
	}
	if err := decodeObserver(result.Response.Payload, response); err != nil {
		return fmt.Errorf("sourceauthority: decode observer %s response: %w", fseventsOpOpen, err)
	}
	return nil
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
	return errors.Join(s.client.Abort(cause), s.stopProcess())
}

func (s *fseventsProxyStream) terminate() error {
	s.terminateOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.cancelSink()
		clientErr := s.client.Close()
		stopErr := s.stopProcess()
		<-s.eventsDone
		removeErr := os.RemoveAll(s.temporary)
		s.mu.Lock()
		s.terminateErr = errors.Join(s.eventErr, stopErr, clientErr, removeErr)
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
	case observerAckRequest:
		return validateObserverAckError(message.Error)
	case *observerAckRequest:
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
		message = strings.ToValidUTF8(message, "\uFFFD")
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
