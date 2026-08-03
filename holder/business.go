package holder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/transportproto"
)

// BusinessHandler handles one product operation on the holder's existing
// admitted daemonkit session.
type BusinessHandler func(context.Context, daemonkit.Request, *LocalTenantController) (any, error)

// BusinessHandlerSpec declares one product-owned ordinary operation.
type BusinessHandlerSpec struct {
	Op         string
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

func businessHandlerSpecs(
	runtime *Runtime,
	graph *runtimeGraph,
	specs []BusinessHandlerSpec,
) ([]transportproto.HandlerSpec, error) {
	seen := make(map[string]struct{}, len(specs))
	result := make([]transportproto.HandlerSpec, 0, len(specs))
	for _, spec := range specs {
		if !strings.HasPrefix(spec.Op, "product.") || len(spec.Op) == len("product.") || spec.Handler == nil {
			return nil, errors.New("FuseKit runtime: business handlers require a product.* operation and handler")
		}
		if _, duplicate := seen[spec.Op]; duplicate {
			return nil, fmt.Errorf("FuseKit runtime: duplicate business operation %q", spec.Op)
		}
		seen[spec.Op] = struct{}{}
		handler := spec.Handler
		result = append(result, transportproto.HandlerSpec{
			Op: spec.Op, Concurrent: spec.Concurrent,
			Handler: func(ctx context.Context, request daemonkit.Request) ([]byte, error) {
				owner, err := tenantOwnerFromProductOwner(runtime.config.Owner)
				if err != nil {
					return nil, err
				}
				scope := newLocalControllerScope()
				defer scope.close()
				controller := &LocalTenantController{runtime: runtime, owner: owner, graph: graph, scope: scope}
				value, err := handler(ctx, request, controller)
				if err != nil {
					return nil, err
				}
				body, err := json.Marshal(value)
				if err != nil {
					return nil, fmt.Errorf("FuseKit runtime: encode business result: %w", err)
				}
				return body, nil
			},
		})
	}
	return result, nil
}
