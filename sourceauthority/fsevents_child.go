package sourceauthority

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/yasyf/daemonkit/wire"
)

type fseventsObserverChild struct {
	mu      sync.Mutex
	eventMu sync.Mutex
	backend EventBackend
	engine  EventStream
	session *wire.AcceptedSession
	next    uint64
	pending uint64
	acked   chan observerAckRequest
	cancel  context.CancelFunc
}

// RunFSEventsObserverChild recognizes and serves one exact observer-child
// invocation. Call it before normal signed-app startup.
func RunFSEventsObserverChild(ctx context.Context, arguments []string) (bool, error) {
	if len(arguments) == 0 || arguments[0] != fseventsObserverChildArg {
		return false, nil
	}
	if len(arguments) != 1 {
		return true, errors.New("sourceauthority: invalid observer child invocation")
	}
	conn, err := wire.NewDuplexConn(os.Stdin, os.Stdout)
	if err != nil {
		return true, fmt.Errorf("sourceauthority: open observer session: %w", err)
	}
	parent, err := wire.SpawnedParentSessionIdentity()
	if err != nil {
		_ = conn.Close()
		return true, err
	}
	return true, serveFSEventsObserverChild(ctx, conn, parent, newPlatformFSEventsEngine())
}

func serveFSEventsObserverChild(
	ctx context.Context,
	conn net.Conn,
	parent wire.SessionIdentity,
	backend EventBackend,
) error {
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	child := &fseventsObserverChild{backend: backend, cancel: cancel}
	server := &wire.Server{
		WireBuild: fseventsObserverBuild, Workers: 4, Backlog: 4, MaxSessions: 1,
		InboundQueue: 8, OutboundQueue: 8,
	}
	server.RegisterConcurrent(fseventsOpOpen, boundedObserverHandler(child.handleOpen))
	server.RegisterConcurrent(fseventsOpActivate, boundedObserverHandler(child.handleActivate))
	server.RegisterConcurrent(fseventsOpFlush, boundedObserverHandler(child.handleFlush))
	server.RegisterConcurrent(fseventsOpAck, boundedObserverHandler(child.handleAck))
	server.RegisterConcurrent(fseventsOpClose, boundedObserverHandler(child.handleClose))
	admit := func() (func(), error) { return func() {}, nil }
	ready := func() error { return nil }
	serveErr := server.ServeSession(serveCtx, conn, parent, ready, admit, admit)
	child.mu.Lock()
	engine := child.engine
	child.mu.Unlock()
	if engine != nil {
		serveErr = errors.Join(serveErr, engine.Close())
	}
	return serveErr
}

func boundedObserverHandler(
	handler func(context.Context, wire.Request) (any, error),
) func(context.Context, wire.Request) (any, error) {
	return func(ctx context.Context, request wire.Request) (any, error) {
		value, err := handler(ctx, request)
		if err != nil {
			return nil, errors.New(boundedObserverErrorMessage(err.Error()))
		}
		return value, nil
	}
}

func (c *fseventsObserverChild) handleOpen(ctx context.Context, request wire.Request) (any, error) {
	var input observerOpenRequest
	if err := decodeObserver(request.Payload, &input); err != nil || input.Protocol != fseventsObserverProtocol {
		return nil, errors.New("sourceauthority: invalid observer open request")
	}
	roots, resume, err := receiveObserverOpenPages(ctx, request.Chunks, input.Config)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.engine != nil || c.session != nil {
		c.mu.Unlock()
		return nil, errors.New("sourceauthority: observer stream is already open")
	}
	c.session = request.Session
	c.mu.Unlock()
	go func(session *wire.AcceptedSession) {
		<-session.Done()
		c.cancel()
	}(request.Session)
	engine, err := c.backend.Open(ctx, roots, resume, c.deliver)
	if err != nil {
		c.mu.Lock()
		c.session = nil
		c.mu.Unlock()
		return nil, err
	}
	c.mu.Lock()
	c.engine = engine
	c.mu.Unlock()
	return observerCheckpointResponse{Protocol: fseventsObserverProtocol, Checkpoints: engine.Checkpoints()}, nil
}

func (c *fseventsObserverChild) handleActivate(ctx context.Context, request wire.Request) (any, error) {
	engine, err := c.requestEngine(request)
	if err != nil {
		return nil, err
	}
	if err := engine.Activate(ctx); err != nil {
		return nil, err
	}
	return observerCheckpointResponse{Protocol: fseventsObserverProtocol, Checkpoints: engine.Checkpoints()}, nil
}

func (c *fseventsObserverChild) handleFlush(ctx context.Context, request wire.Request) (any, error) {
	engine, err := c.requestEngine(request)
	if err != nil {
		return nil, err
	}
	checkpoints, err := engine.Flush(ctx)
	if err != nil {
		return nil, err
	}
	return observerCheckpointResponse{Protocol: fseventsObserverProtocol, Checkpoints: checkpoints}, nil
}

func (c *fseventsObserverChild) handleAck(_ context.Context, request wire.Request) (any, error) {
	var input observerAckRequest
	if err := decodeObserver(request.Payload, &input); err != nil || input.Protocol != fseventsObserverProtocol ||
		input.Sequence == 0 || (!input.Delivered && input.Error == "") || (input.Delivered && input.Error != "") {
		return nil, errors.New("sourceauthority: invalid observer acknowledgement")
	}
	c.mu.Lock()
	if request.Session != c.session || c.pending != input.Sequence || c.acked == nil {
		c.mu.Unlock()
		return nil, errors.New("sourceauthority: observer acknowledgement escaped its event fence")
	}
	acked := c.acked
	c.mu.Unlock()
	select {
	case acked <- input:
		return observerRequest{Protocol: fseventsObserverProtocol}, nil
	default:
		return nil, errors.New("sourceauthority: duplicate observer acknowledgement")
	}
}

func (c *fseventsObserverChild) handleClose(ctx context.Context, request wire.Request) (any, error) {
	engine, err := c.requestEngine(request)
	if err != nil {
		return nil, err
	}
	if err := engine.Close(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	c.mu.Lock()
	checkpoints := engine.Checkpoints()
	c.engine = nil
	c.mu.Unlock()
	return observerCheckpointResponse{Protocol: fseventsObserverProtocol, Checkpoints: checkpoints}, nil
}

func (c *fseventsObserverChild) requestEngine(request wire.Request) (EventStream, error) {
	var input observerRequest
	if err := decodeObserver(request.Payload, &input); err != nil || input.Protocol != fseventsObserverProtocol {
		return nil, errors.New("sourceauthority: invalid observer request")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if request.Session != c.session || c.engine == nil {
		return nil, errors.New("sourceauthority: observer request escaped its session")
	}
	return c.engine, nil
}

func (c *fseventsObserverChild) deliver(ctx context.Context, batch EventBatch) error {
	c.eventMu.Lock()
	defer c.eventMu.Unlock()
	c.mu.Lock()
	if c.session == nil || c.pending != 0 || c.acked != nil {
		c.mu.Unlock()
		return errors.New("sourceauthority: observer event state is invalid")
	}
	c.next++
	sequence := c.next
	acked := make(chan observerAckRequest, 1)
	c.pending, c.acked = sequence, acked
	session := c.session
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.pending, c.acked = 0, nil
		c.mu.Unlock()
	}()
	payload, err := jsonMarshalObserver(observerEvent{
		Protocol: fseventsObserverProtocol, Sequence: sequence, Batch: batch,
	})
	if err != nil {
		return err
	}
	if err := session.PushEvent(ctx, wire.Event{Topic: fseventsEventTopic, Payload: payload}); err != nil {
		return err
	}
	select {
	case acknowledgement := <-acked:
		if !acknowledgement.Delivered {
			return errors.New(acknowledgement.Error)
		}
		return nil
	case <-session.Done():
		return errors.New("sourceauthority: observer parent disconnected before acknowledgement")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func jsonMarshalObserver(value any) ([]byte, error) {
	return marshalObserverControl(value)
}
