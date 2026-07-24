package holder

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/transportproto"
)

// BusinessHandler handles one product operation on the holder's existing
// admitted daemonkit session.
type BusinessHandler func(context.Context, wire.Request, *LocalTenantController) (any, error)

// BusinessHandlerSpec declares one product-owned ordinary operation.
type BusinessHandlerSpec struct {
	Op         wire.Op
	Handler    BusinessHandler
	Concurrent bool
}

type localControllerScope struct {
	mu     sync.Mutex
	cond   *sync.Cond
	open   bool
	active int
}

func newLocalControllerScope() *localControllerScope {
	scope := &localControllerScope{open: true}
	scope.cond = sync.NewCond(&scope.mu)
	return scope
}

func (s *localControllerScope) acquire() (func(), error) {
	if s == nil {
		return nil, ErrLocalTenantControllerUnavailable
	}
	s.mu.Lock()
	if !s.open {
		s.mu.Unlock()
		return nil, ErrLocalTenantControllerUnavailable
	}
	s.active++
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		s.active--
		if !s.open && s.active == 0 {
			s.cond.Broadcast()
		}
		s.mu.Unlock()
	}, nil
}

func (s *localControllerScope) close() {
	s.mu.Lock()
	s.open = false
	for s.active != 0 {
		s.cond.Wait()
	}
	s.mu.Unlock()
}

func registerBusinessHandlers(runtime *Runtime, specs []BusinessHandlerSpec) error {
	seen := make(map[wire.Op]struct{}, len(specs))
	for _, spec := range specs {
		if !strings.HasPrefix(string(spec.Op), "product.") || len(spec.Op) == len("product.") || spec.Handler == nil {
			return errors.New("FuseKit runtime: business handlers require a product.* operation and handler")
		}
		if _, duplicate := seen[spec.Op]; duplicate {
			return fmt.Errorf("FuseKit runtime: duplicate business operation %q", spec.Op)
		}
		seen[spec.Op] = struct{}{}
		handler := spec.Handler
		runtime.server.Register(wire.HandlerSpec{
			Op: spec.Op, Concurrent: spec.Concurrent,
			Handler: func(ctx context.Context, request wire.Request) (any, error) {
				if request.Session == nil || request.Session.Protected() || request.WireBuild != transportproto.WireBuild {
					return nil, errors.New("FuseKit runtime: business handler requires an exact ordinary session")
				}
				graph, err := runtime.graphs.Value(request.Publication)
				if err != nil {
					return nil, err
				}
				owner, err := tenantOwnerFromProductOwner(runtime.config.Owner)
				if err != nil {
					return nil, err
				}
				scope := newLocalControllerScope()
				defer scope.close()
				controller := &LocalTenantController{runtime: runtime, owner: owner, graph: graph, scope: scope}
				return handler(ctx, request, controller)
			},
		})
	}
	return nil
}
