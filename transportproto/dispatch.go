package transportproto

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit"
)

// ErrUnknownOperation means no handler is registered for a request's operation.
var ErrUnknownOperation = errors.New("transportproto: unknown operation")

// Handler answers one admitted business request with its encoded reply body.
// Peer identity is the request's own Caller and Session.
type Handler func(ctx context.Context, request daemonkit.Request) ([]byte, error)

// HandlerSpec binds one operation to its handler. Concurrent carries v0.20's
// dispatch class forward unchanged in shape, but selects nothing: v0.20 offloaded
// a concurrent handler to a bounded pool and ran the rest inline on the request's
// own goroutine, and v0.21 bounds every Product.Handle uniformly through
// Daemon.Concurrency, so neither class serializes against the other.
type HandlerSpec struct {
	Op         string
	Handler    Handler
	Concurrent bool
}

// Mux is the daemon socket's operation table: every registered operation with
// the exact server deadline dispatch imposes on it. The client's own deadline
// arrives with the request, so only the server half is declared here.
type Mux struct {
	routes map[string]route
}

type route struct {
	handler  Handler
	deadline time.Duration
}

// NewMux pairs every registered operation with its exact server deadline.
// Both sides must name the same operations, so an operation can neither be
// dispatched without a deadline nor given one it never reaches.
func NewMux(deadlines map[string]time.Duration, specs ...HandlerSpec) (*Mux, error) {
	routes := make(map[string]route, len(specs))
	for _, spec := range specs {
		if spec.Op == "" || spec.Handler == nil {
			return nil, errors.New("transportproto: handler spec requires an operation and handler")
		}
		if _, duplicate := routes[spec.Op]; duplicate {
			return nil, fmt.Errorf("transportproto: duplicate operation %q", spec.Op)
		}
		deadline, ok := deadlines[spec.Op]
		if !ok {
			return nil, fmt.Errorf("transportproto: operation %q has no deadline", spec.Op)
		}
		if deadline <= 0 {
			return nil, fmt.Errorf("transportproto: operation %q has a non-positive deadline", spec.Op)
		}
		routes[spec.Op] = route{handler: spec.Handler, deadline: deadline}
	}
	for op := range deadlines {
		if _, registered := routes[op]; !registered {
			return nil, fmt.Errorf("transportproto: deadline for unregistered operation %q", op)
		}
	}
	return &Mux{routes: routes}, nil
}

// Handle dispatches one request under its operation's server deadline.
// A daemonkit.Product delegates its own Handle to this method.
func (m *Mux) Handle(ctx context.Context, request daemonkit.Request) (daemonkit.Reply, error) {
	route, ok := m.routes[request.Op]
	if !ok {
		return daemonkit.Reply{}, fmt.Errorf("%w %q", ErrUnknownOperation, request.Op)
	}
	ctx, cancel := context.WithTimeout(ctx, route.deadline)
	defer cancel()
	body, err := route.handler(ctx, request)
	if err != nil {
		return daemonkit.Reply{}, err
	}
	return daemonkit.Reply{Body: body}, nil
}
