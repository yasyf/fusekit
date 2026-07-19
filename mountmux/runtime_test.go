package mountmux

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/tenant"
)

type fakeNative struct {
	mu         sync.Mutex
	starts     int
	closes     int
	root       string
	resolver   Resolver
	startError error
	closeError error
	closed     chan struct{}
}

func newFakeNative() *fakeNative { return &fakeNative{closed: make(chan struct{})} }

func (n *fakeNative) Start(_ context.Context, root string, resolver Resolver) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.starts++
	n.root = root
	n.resolver = resolver
	return n.startError
}

func (n *fakeNative) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closes++
	select {
	case <-n.closed:
	default:
		close(n.closed)
	}
	return n.closeError
}

func (n *fakeNative) counts() (int, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.starts, n.closes
}

type fakeController struct {
	mu       sync.Mutex
	specs    map[catalog.TenantID]tenant.TenantSpec
	active   map[catalog.TenantID]int
	changing map[catalog.TenantID]bool
	drained  map[catalog.TenantID]chan struct{}
	change   chan catalog.TenantID
}

func newFakeController(specs ...tenant.TenantSpec) *fakeController {
	c := &fakeController{
		specs: map[catalog.TenantID]tenant.TenantSpec{}, active: map[catalog.TenantID]int{},
		changing: map[catalog.TenantID]bool{}, drained: map[catalog.TenantID]chan struct{}{},
		change: make(chan catalog.TenantID, 8),
	}
	for _, spec := range specs {
		c.specs[spec.ID] = spec
	}
	return c
}

func (c *fakeController) RegisterTenant(_ context.Context, spec tenant.TenantSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.specs[spec.ID]; ok {
		if current == spec {
			return nil
		}
		return tenant.ErrTenantConflict
	}
	c.specs[spec.ID] = spec
	return nil
}

func (c *fakeController) ReplaceTenant(ctx context.Context, expected catalog.Generation, spec tenant.TenantSpec) error {
	if err := c.beginChange(spec.ID, expected); err != nil {
		return err
	}
	if err := c.awaitDrain(ctx, spec.ID); err != nil {
		c.endChange(spec.ID)
		return err
	}
	c.mu.Lock()
	c.specs[spec.ID] = spec
	c.changing[spec.ID] = false
	c.mu.Unlock()
	return nil
}

func (c *fakeController) RemoveTenant(ctx context.Context, id catalog.TenantID, generation catalog.Generation) error {
	if err := c.beginChange(id, generation); err != nil {
		return err
	}
	if err := c.awaitDrain(ctx, id); err != nil {
		c.endChange(id)
		return err
	}
	c.mu.Lock()
	delete(c.specs, id)
	delete(c.changing, id)
	c.mu.Unlock()
	return nil
}

func (c *fakeController) beginChange(id catalog.TenantID, generation catalog.Generation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	spec, ok := c.specs[id]
	if !ok {
		return tenant.ErrTenantNotFound
	}
	if spec.Generation != generation {
		return tenant.ErrGenerationConflict
	}
	c.changing[id] = true
	if c.active[id] > 0 {
		c.drained[id] = make(chan struct{})
	}
	select {
	case c.change <- id:
	default:
	}
	return nil
}

func (c *fakeController) awaitDrain(ctx context.Context, id catalog.TenantID) error {
	c.mu.Lock()
	drained := c.drained[id]
	c.mu.Unlock()
	if drained == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-drained:
		return nil
	}
}

func (c *fakeController) endChange(id catalog.TenantID) {
	c.mu.Lock()
	c.changing[id] = false
	c.mu.Unlock()
}

func (c *fakeController) AcquireGeneration(_ context.Context, id catalog.TenantID, generation catalog.Generation) (GenerationPin, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	spec, ok := c.specs[id]
	if !ok {
		return nil, tenant.ErrTenantNotFound
	}
	if c.changing[id] {
		return nil, tenant.ErrTenantChanging
	}
	if spec.Generation != generation {
		return nil, tenant.ErrGenerationConflict
	}
	c.active[id]++
	return &fakePin{controller: c, tenant: id}, nil
}

func (c *fakeController) Specs() []tenant.TenantSpec {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]tenant.TenantSpec, 0, len(c.specs))
	for _, spec := range c.specs {
		result = append(result, spec)
	}
	return result
}

func (c *fakeController) State(_ context.Context, id catalog.TenantID) (tenant.TenantState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	spec, ok := c.specs[id]
	if !ok {
		return tenant.TenantState{}, tenant.ErrTenantNotFound
	}
	return tenant.TenantState{Tenant: id, Generation: spec.Generation, Activated: spec.Generation}, nil
}

type fakePin struct {
	controller *fakeController
	tenant     catalog.TenantID
	once       sync.Once
}

func (p *fakePin) Prepare(_ context.Context, revision catalog.Revision) (tenant.TenantState, error) {
	p.controller.mu.Lock()
	defer p.controller.mu.Unlock()
	spec, ok := p.controller.specs[p.tenant]
	if !ok {
		return tenant.TenantState{}, tenant.ErrTenantNotFound
	}
	return tenant.TenantState{
		Requested: revision, Tenant: p.tenant, Generation: spec.Generation,
		Desired: revision, Observed: revision, Verified: revision, Applied: revision, Activated: spec.Generation,
	}, nil
}

func (p *fakePin) Release() {
	p.once.Do(func() {
		p.controller.mu.Lock()
		p.controller.active[p.tenant]--
		if p.controller.active[p.tenant] == 0 {
			if drained := p.controller.drained[p.tenant]; drained != nil {
				close(drained)
				delete(p.controller.drained, p.tenant)
			}
		}
		p.controller.mu.Unlock()
	})
}

func testSpec(root, id, name string, generation catalog.Generation) tenant.TenantSpec {
	return tenant.TenantSpec{
		OwnerID: tenant.OwnerID("owner"), ID: catalog.TenantID(id),
		PresentationRoot: filepath.Join(root, name), Backing: tenant.BackingSpec{Root: filepath.Join(root, "backing", id)},
		Content: tenant.ContentSource{ID: "source-" + id},
		Traits: tenant.TenantTraits{
			Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive,
			Presentations: catalog.PresentMount,
		},
		Generation: generation,
	}
}

func newRuntime(t *testing.T, controller *fakeController, native *fakeNative, fail func(string) error) (*Runtime, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "mount")
	runtime, err := New(Config{Root: root, Tenants: controller, Native: native, failpoint: fail})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return runtime, root
}

func TestRuntimeOwnsOneNativeRootAcrossZeroAndManyTenants(t *testing.T) {
	controller := newFakeController()
	native := newFakeNative()
	runtime, root := newRuntime(t, controller, native, nil)
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if starts, closes := native.counts(); starts != 1 || closes != 0 {
		t.Fatalf("native counts after empty Start = (%d, %d), want (1, 0)", starts, closes)
	}
	for index, name := range []string{"one", "two"} {
		spec := testSpec(root, "tenant-"+name, name, catalog.Generation(index+1))
		route := Route{Tenant: spec.ID, Generation: spec.Generation, Name: name}
		if err := runtime.Attach(t.Context(), spec, route); err != nil {
			t.Fatalf("Attach(%s): %v", name, err)
		}
		if err := runtime.Detach(t.Context(), spec.ID, spec.Generation); err != nil {
			t.Fatalf("Detach(%s): %v", name, err)
		}
		if starts, closes := native.counts(); starts != 1 || closes != 0 {
			t.Fatalf("native counts after %s detach = (%d, %d), want (1, 0)", name, starts, closes)
		}
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if starts, closes := native.counts(); starts != 1 || closes != 1 {
		t.Fatalf("native counts after Close = (%d, %d), want (1, 1)", starts, closes)
	}
}

func TestRouteConflictsAndGenerationFences(t *testing.T) {
	controller := newFakeController()
	native := newFakeNative()
	runtime, root := newRuntime(t, controller, native, nil)
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	one := testSpec(root, "tenant-one", "Alpha", 1)
	if err := runtime.Attach(t.Context(), one, Route{Tenant: one.ID, Generation: 1, Name: "Alpha"}); err != nil {
		t.Fatal(err)
	}
	two := testSpec(root, "tenant-two", "alpha", 1)
	if err := runtime.Attach(t.Context(), two, Route{Tenant: two.ID, Generation: 1, Name: "alpha"}); !errors.Is(err, ErrRouteConflict) {
		t.Fatalf("case-folded conflicting Attach = %v, want ErrRouteConflict", err)
	}
	if err := runtime.Detach(t.Context(), one.ID, 2); !errors.Is(err, tenant.ErrGenerationConflict) {
		t.Fatalf("stale Detach = %v, want generation conflict", err)
	}
	next := one
	next.Generation = 2
	if err := runtime.Replace(t.Context(), 2, next, Route{Tenant: next.ID, Generation: 2, Name: "Alpha"}); !errors.Is(err, tenant.ErrGenerationConflict) {
		t.Fatalf("stale Replace = %v, want generation conflict", err)
	}
}

func TestPinnedCallbackDrainsBeforeGenerationReplacement(t *testing.T) {
	native := newFakeNative()
	runtime, root := newRuntime(t, newFakeController(), native, nil)
	current := testSpec(root, "tenant-one", "one", 1)
	runtime.tenants = newFakeController(current)
	controller := runtime.tenants.(*fakeController)
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	pin, err := runtime.Pin(t.Context(), "one")
	if err != nil {
		t.Fatalf("Pin: %v", err)
	}
	next := current
	next.Generation = 2
	replaced := make(chan error, 1)
	go func() {
		replaced <- runtime.Replace(context.Background(), 1, next, Route{Tenant: next.ID, Generation: 2, Name: "one"})
	}()
	select {
	case <-controller.change:
	case <-time.After(time.Second):
		t.Fatal("replacement never entered transition")
	}
	select {
	case err := <-replaced:
		t.Fatalf("Replace completed while old callback was pinned: %v", err)
	default:
	}
	if route, err := runtime.Route(current.ID, 1); err != nil || route.Generation != 1 {
		t.Fatalf("old published route during drain = %+v, %v", route, err)
	}
	pin.Release()
	if err := <-replaced; err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if route, err := runtime.Route(current.ID, 2); err != nil || route.Generation != 2 {
		t.Fatalf("new published route = %+v, %v", route, err)
	}
}

func TestRouteFailpointsRecoverFromTenantSpecs(t *testing.T) {
	for _, point := range []string{failAfterTenantTransition, failAfterPublish} {
		t.Run(point, func(t *testing.T) {
			injected := errors.New("injected")
			armed := true
			fail := func(got string) error {
				if armed && got == point {
					armed = false
					return injected
				}
				return nil
			}
			controller := newFakeController()
			runtime, root := newRuntime(t, controller, newFakeNative(), fail)
			if err := runtime.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			spec := testSpec(root, "tenant-one", "one", 1)
			route := Route{Tenant: spec.ID, Generation: 1, Name: "one"}
			if err := runtime.Attach(t.Context(), spec, route); !errors.Is(err, injected) {
				t.Fatalf("Attach failpoint = %v, want injected", err)
			}
			if err := runtime.Recover(); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if got, err := runtime.Route(spec.ID, 1); err != nil || got != route {
				t.Fatalf("recovered route = %+v, %v, want %+v", got, err, route)
			}
		})
	}
}

func TestCloseWaitsForPinnedCallbacksAndClearsRoutes(t *testing.T) {
	root := filepath.Join(t.TempDir(), "mount")
	spec := testSpec(root, "tenant-one", "one", 1)
	controller := newFakeController(spec)
	native := newFakeNative()
	runtime, err := New(Config{Root: root, Tenants: controller, Native: native})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	pin, err := runtime.Pin(t.Context(), "one")
	if err != nil {
		t.Fatal(err)
	}
	if !runtime.Busy() {
		t.Fatal("runtime is not busy while callback is pinned")
	}
	closed := make(chan error, 1)
	go func() { closed <- runtime.Close() }()
	select {
	case <-native.closed:
		t.Fatal("native root closed before callback release")
	case <-time.After(25 * time.Millisecond):
	}
	pin.Release()
	if runtime.Busy() {
		t.Fatal("runtime remains busy after callback release")
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if routes := runtime.Routes(); len(routes) != 0 {
		t.Fatalf("routes after close = %+v, want empty", routes)
	}
}

func TestCloseContextTimeoutLeavesNativeRootOwnedAndRetryable(t *testing.T) {
	root := filepath.Join(t.TempDir(), "mount")
	spec := testSpec(root, "tenant-one", "one", 1)
	native := newFakeNative()
	runtime, err := New(Config{Root: root, Tenants: newFakeController(spec), Native: native})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	pin, err := runtime.Pin(t.Context(), "one")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := runtime.CloseContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseContext = %v, want deadline", err)
	}
	if _, closes := native.counts(); closes != 0 {
		t.Fatalf("native closes after failed drain = %d, want 0", closes)
	}
	if _, err := runtime.Pin(t.Context(), "one"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pin while draining = %v, want ErrClosed", err)
	}
	pin.Release()
	if err := runtime.Close(); err != nil {
		t.Fatalf("retry Close: %v", err)
	}
	if _, closes := native.counts(); closes != 1 {
		t.Fatalf("native closes after retry = %d, want 1", closes)
	}
}
