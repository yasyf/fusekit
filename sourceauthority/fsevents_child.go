package sourceauthority

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"
)

type fseventsObserverChild struct {
	mu      sync.Mutex
	eventMu sync.Mutex
	backend EventBackend
	engine  EventStream
	stage   observerOpenStage

	bound     bool
	sessionID uint64
	acked     uint64
	pending   *observerPendingBatch
	wake      chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
}

// observerPendingBatch is the one sequenced batch the child retains until a
// drain naming its sequence acknowledges durable delivery.
type observerPendingBatch struct {
	sequence uint64
	batch    EventBatch
	settled  chan observerDelivery
}

type observerDelivery struct {
	delivered bool
	message   string
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
	return true, serveFSEventsObserverChild(ctx, newPlatformFSEventsEngine())
}

func serveFSEventsObserverChild(ctx context.Context, backend EventBackend) error {
	child, cancel := newFSEventsObserverChild(ctx, backend)
	defer cancel()
	serveErr := daemonkit.ServeSpawned(child.ctx, observerSpawnedContract(), child.handle)
	child.mu.Lock()
	engine := child.engine
	child.engine = nil
	child.mu.Unlock()
	if engine != nil {
		serveErr = errors.Join(serveErr, engine.Close())
	}
	return serveErr
}

func newFSEventsObserverChild(
	ctx context.Context,
	backend EventBackend,
) (*fseventsObserverChild, context.CancelFunc) {
	serveCtx, cancel := context.WithCancel(ctx)
	child := &fseventsObserverChild{
		backend: backend, wake: make(chan struct{}), ctx: serveCtx, cancel: cancel,
	}
	return child, cancel
}

func (c *fseventsObserverChild) handle(ctx context.Context, request daemonkit.Request) (daemonkit.Reply, error) {
	handler, registered := observerHandlers(c)[request.Op]
	if !registered {
		return daemonkit.Reply{}, fmt.Errorf("sourceauthority: unknown observer operation %q", request.Op)
	}
	if err := c.bind(request); err != nil {
		return daemonkit.Reply{}, errors.New(boundedObserverErrorMessage(err.Error()))
	}
	value, err := handler(ctx, request)
	if err != nil {
		return daemonkit.Reply{}, errors.New(boundedObserverErrorMessage(err.Error()))
	}
	body, err := marshalObserverControl(value)
	if err != nil {
		return daemonkit.Reply{}, errors.New(boundedObserverErrorMessage(err.Error()))
	}
	return daemonkit.Reply{Body: body}, nil
}

func observerHandlers(
	child *fseventsObserverChild,
) map[string]func(context.Context, daemonkit.Request) (any, error) {
	return map[string]func(context.Context, daemonkit.Request) (any, error){
		fseventsOpStage:    child.handleStage,
		fseventsOpOpen:     child.handleOpen,
		fseventsOpActivate: child.handleActivate,
		fseventsOpFlush:    child.handleFlush,
		fseventsOpDrain:    child.handleDrain,
		fseventsOpNack:     child.handleNack,
		fseventsOpClose:    child.handleClose,
	}
}

// bind fences every request to the one session ServeSpawned admits, and drops
// the engine when that session settles.
func (c *fseventsObserverChild) bind(request daemonkit.Request) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.bound {
		c.bound, c.sessionID = true, request.Session.ID()
		if done := request.Session.Done(); done != nil {
			go func() {
				<-done
				c.cancel()
			}()
		}
		return nil
	}
	if request.Session.ID() != c.sessionID {
		return errors.New("sourceauthority: observer request escaped its session")
	}
	return nil
}

func (c *fseventsObserverChild) handleStage(_ context.Context, request daemonkit.Request) (any, error) {
	c.mu.Lock()
	opened := c.engine != nil
	c.mu.Unlock()
	if opened {
		return nil, errors.New("sourceauthority: observer stream is already open")
	}
	return c.stage.accept(request.Body)
}

func (c *fseventsObserverChild) handleOpen(ctx context.Context, request daemonkit.Request) (any, error) {
	var input observerOpenRequest
	if err := decodeObserver(request.Body, &input); err != nil || input.Protocol != fseventsObserverProtocol {
		return nil, errors.New("sourceauthority: invalid observer open request")
	}
	roots, resume, err := c.stage.settle(input.Config)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.engine != nil {
		c.mu.Unlock()
		return nil, errors.New("sourceauthority: observer stream is already open")
	}
	c.mu.Unlock()
	engine, err := c.backend.Open(ctx, roots, resume, c.deliver)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.engine = engine
	c.mu.Unlock()
	return observerCheckpointResponse{Protocol: fseventsObserverProtocol, Checkpoints: engine.Checkpoints()}, nil
}

func (c *fseventsObserverChild) handleActivate(ctx context.Context, request daemonkit.Request) (any, error) {
	engine, err := c.requestEngine(request)
	if err != nil {
		return nil, err
	}
	if err := engine.Activate(ctx); err != nil {
		return nil, err
	}
	return observerCheckpointResponse{Protocol: fseventsObserverProtocol, Checkpoints: engine.Checkpoints()}, nil
}

func (c *fseventsObserverChild) handleFlush(ctx context.Context, request daemonkit.Request) (any, error) {
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

// handleDrain acknowledges the retained batch its cursor names, then returns
// the next one, holding the call open until one exists or wait expires.
func (c *fseventsObserverChild) handleDrain(ctx context.Context, request daemonkit.Request) (any, error) {
	var input observerDrainRequest
	if err := decodeObserver(request.Body, &input); err != nil || input.Protocol != fseventsObserverProtocol ||
		input.Wait < 0 || input.Wait > observerDrainWait {
		return nil, errors.New("sourceauthority: invalid observer drain request")
	}
	if _, err := c.requestEngine(request); err != nil {
		return nil, err
	}
	c.mu.Lock()
	switch {
	case input.Cursor == c.acked:
	case input.Cursor == c.acked+1 && c.pending != nil && c.pending.sequence == input.Cursor:
		settled := c.pending
		c.pending, c.acked = nil, input.Cursor
		settled.settled <- observerDelivery{delivered: true}
	default:
		c.mu.Unlock()
		return nil, errors.New("sourceauthority: observer drain escaped its sequence fence")
	}
	timer := time.NewTimer(input.Wait)
	defer timer.Stop()
	for {
		if c.pending != nil {
			response := observerDrainResponse{
				Protocol: fseventsObserverProtocol, Pending: true,
				Sequence: c.pending.sequence, Batch: c.pending.batch,
			}
			c.mu.Unlock()
			return response, nil
		}
		wake := c.wake
		c.mu.Unlock()
		select {
		case <-wake:
		case <-timer.C:
			return observerDrainResponse{Protocol: fseventsObserverProtocol}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.ctx.Done():
			return nil, errors.New("sourceauthority: observer child is settling")
		}
		c.mu.Lock()
	}
}

func (c *fseventsObserverChild) handleNack(_ context.Context, request daemonkit.Request) (any, error) {
	var input observerNackRequest
	if err := decodeObserver(request.Body, &input); err != nil || input.Protocol != fseventsObserverProtocol ||
		input.Sequence == 0 || input.Error == "" {
		return nil, errors.New("sourceauthority: invalid observer negative acknowledgement")
	}
	c.mu.Lock()
	if c.pending == nil || c.pending.sequence != input.Sequence {
		c.mu.Unlock()
		return nil, errors.New("sourceauthority: observer negative acknowledgement escaped its event fence")
	}
	settled := c.pending
	c.pending = nil
	c.mu.Unlock()
	settled.settled <- observerDelivery{message: input.Error}
	return observerRequest{Protocol: fseventsObserverProtocol}, nil
}

func (c *fseventsObserverChild) handleClose(ctx context.Context, request daemonkit.Request) (any, error) {
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

func (c *fseventsObserverChild) requestEngine(request daemonkit.Request) (EventStream, error) {
	if request.Op == fseventsOpActivate || request.Op == fseventsOpFlush || request.Op == fseventsOpClose {
		var input observerRequest
		if err := decodeObserver(request.Body, &input); err != nil || input.Protocol != fseventsObserverProtocol {
			return nil, errors.New("sourceauthority: invalid observer request")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.engine == nil {
		return nil, errors.New("sourceauthority: observer request escaped its stream")
	}
	return c.engine, nil
}

func (c *fseventsObserverChild) deliver(ctx context.Context, batch EventBatch) error {
	c.eventMu.Lock()
	defer c.eventMu.Unlock()
	c.mu.Lock()
	if !c.bound || c.pending != nil {
		c.mu.Unlock()
		return errors.New("sourceauthority: observer event state is invalid")
	}
	pending := &observerPendingBatch{
		sequence: c.acked + 1, batch: batch, settled: make(chan observerDelivery, 1),
	}
	c.pending = pending
	close(c.wake)
	c.wake = make(chan struct{})
	c.mu.Unlock()
	select {
	case settlement := <-pending.settled:
		if !settlement.delivered {
			return errors.New(settlement.message)
		}
		return nil
	case <-c.ctx.Done():
		c.retract(pending)
		return errors.New("sourceauthority: observer parent disconnected before acknowledgement")
	case <-ctx.Done():
		c.retract(pending)
		return ctx.Err()
	}
}

func (c *fseventsObserverChild) retract(pending *observerPendingBatch) {
	c.mu.Lock()
	if c.pending == pending {
		c.pending = nil
	}
	c.mu.Unlock()
}
