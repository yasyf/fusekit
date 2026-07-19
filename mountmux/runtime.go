// Package mountmux owns the single native mount and its immutable tenant routes.
package mountmux

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/tenant"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

var (
	// ErrClosed means the mount runtime no longer accepts work.
	ErrClosed = errors.New("mount mux: runtime closed")
	// ErrNotStarted means the native root has not been established.
	ErrNotStarted = errors.New("mount mux: runtime not started")
	// ErrStarted means Start was called more than once.
	ErrStarted = errors.New("mount mux: runtime already started")
	// ErrInvalidRoute means a route violates the single-root contract.
	ErrInvalidRoute = errors.New("mount mux: invalid route")
	// ErrRouteConflict means a tenant or root name is already bound differently.
	ErrRouteConflict = errors.New("mount mux: route conflict")
)

const (
	failAfterTenantTransition = "route.after_tenant_transition"
	failAfterPublish          = "route.after_publish"
	defaultCloseTimeout       = 30 * time.Second
)

// Route is one immutable root name to exact tenant-generation binding.
type Route struct {
	Tenant     catalog.TenantID
	Generation catalog.Generation
	Name       string
}

// NativeRoot owns the one kernel mount for a Runtime.
type NativeRoot interface {
	Start(context.Context, string, Resolver) error
	Close() error
}

// GenerationPin holds one tenant generation through a callback.
type GenerationPin interface {
	Prepare(context.Context, catalog.Revision) (tenant.TenantState, error)
	Release()
}

// TenantController is the exact tenant lifecycle surface required by Runtime.
type TenantController interface {
	RegisterTenant(context.Context, tenant.TenantSpec) error
	ReplaceTenant(context.Context, catalog.Generation, tenant.TenantSpec) error
	RemoveTenant(context.Context, catalog.TenantID, catalog.Generation) error
	AcquireGeneration(context.Context, catalog.TenantID, catalog.Generation) (GenerationPin, error)
	State(context.Context, catalog.TenantID) (tenant.TenantState, error)
	Specs() []tenant.TenantSpec
}

type tenantController struct{ runtime *tenant.TenantRuntime }

// BindTenantRuntime adapts the canonical tenant runtime without changing its lifecycle API.
func BindTenantRuntime(runtime *tenant.TenantRuntime) TenantController {
	if runtime == nil {
		return nil
	}
	return tenantController{runtime: runtime}
}

func (c tenantController) RegisterTenant(ctx context.Context, spec tenant.TenantSpec) error {
	return c.runtime.RegisterTenant(ctx, spec)
}

func (c tenantController) ReplaceTenant(ctx context.Context, expected catalog.Generation, spec tenant.TenantSpec) error {
	return c.runtime.ReplaceTenant(ctx, expected, spec)
}

func (c tenantController) RemoveTenant(ctx context.Context, id catalog.TenantID, generation catalog.Generation) error {
	return c.runtime.RemoveTenant(ctx, id, generation)
}

func (c tenantController) AcquireGeneration(ctx context.Context, id catalog.TenantID, generation catalog.Generation) (GenerationPin, error) {
	return c.runtime.AcquireGeneration(ctx, id, generation)
}

func (c tenantController) Specs() []tenant.TenantSpec { return c.runtime.Specs() }

func (c tenantController) State(ctx context.Context, id catalog.TenantID) (tenant.TenantState, error) {
	return c.runtime.State(ctx, id)
}

// Resolver pins a kernel callback to one immutable tenant generation.
type Resolver interface {
	Pin(context.Context, string) (*PinnedRoute, error)
}

// Config defines one process-lifetime native mux root.
type Config struct {
	Root    string
	Tenants TenantController
	Native  NativeRoot
	// CloseTimeout bounds callback drain before Close fails with the root intact.
	CloseTimeout time.Duration

	failpoint func(string) error
}

type routeEntry struct {
	route Route
	spec  tenant.TenantSpec
}

type routeSnapshot struct {
	byName   map[string]routeEntry
	byTenant map[catalog.TenantID]routeEntry
	changed  chan struct{}
}

// Runtime owns one native root for its entire started lifetime.
type Runtime struct {
	root         string
	tenants      TenantController
	native       NativeRoot
	fail         func(string) error
	closeTimeout time.Duration

	lifecycle sync.Mutex
	routes    atomic.Pointer[routeSnapshot]

	mu      sync.Mutex
	started bool
	closing bool
	closed  bool
	active  int
	drained chan struct{}
}

// PinnedRoute holds an exact tenant generation until Release.
type PinnedRoute struct {
	Route Route
	Spec  tenant.TenantSpec

	runtime *Runtime
	lease   GenerationPin
	once    sync.Once
}

// New constructs an unstarted single-root mount runtime.
func New(config Config) (*Runtime, error) {
	root := filepath.Clean(config.Root)
	switch {
	case !filepath.IsAbs(root):
		return nil, fmt.Errorf("%w: root %q is not absolute", ErrInvalidRoute, config.Root)
	case config.Tenants == nil:
		return nil, fmt.Errorf("%w: tenant runtime is required", ErrInvalidRoute)
	case config.Native == nil:
		return nil, fmt.Errorf("%w: native root is required", ErrInvalidRoute)
	}
	closeTimeout := config.CloseTimeout
	if closeTimeout <= 0 {
		closeTimeout = defaultCloseTimeout
	}
	runtime := &Runtime{
		root: root, tenants: config.Tenants, native: config.Native, fail: config.failpoint,
		closeTimeout: closeTimeout, drained: make(chan struct{}),
	}
	runtime.routes.Store(emptySnapshot())
	return runtime, nil
}

// Start recovers exact routes and establishes the sole native root once.
func (r *Runtime) Start(ctx context.Context) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.mu.Lock()
	switch {
	case r.closed || r.closing:
		r.mu.Unlock()
		return ErrClosed
	case r.started:
		r.mu.Unlock()
		return ErrStarted
	}
	r.mu.Unlock()

	next, err := r.snapshotFromSpecs(r.tenants.Specs())
	if err != nil {
		return err
	}
	r.swapSnapshot(next)
	if err := r.native.Start(ctx, r.root, r); err != nil {
		r.swapSnapshot(emptySnapshot())
		return fmt.Errorf("mount mux: establish native root: %w", err)
	}
	r.mu.Lock()
	r.started = true
	r.mu.Unlock()
	return nil
}

// Recover rebuilds the route table from the tenant runtime's authoritative specs.
func (r *Runtime) Recover() error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	if err := r.requireActive(); err != nil {
		return err
	}
	return r.publishSpecs()
}

// Attach registers one exact tenant generation and publishes its route.
func (r *Runtime) Attach(ctx context.Context, spec tenant.TenantSpec, route Route) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	if err := r.requireActive(); err != nil {
		return err
	}
	if err := r.validateBinding(spec, route); err != nil {
		return err
	}
	if err := r.rejectConflict(route); err != nil {
		return err
	}
	if err := r.tenants.RegisterTenant(ctx, spec); err != nil {
		return err
	}
	if err := r.trip(failAfterTenantTransition); err != nil {
		return err
	}
	if err := r.publishSpecs(); err != nil {
		return err
	}
	return r.trip(failAfterPublish)
}

// RegisterTenant registers one mount tenant using its fixed presentation root.
func (r *Runtime) RegisterTenant(ctx context.Context, spec tenant.TenantSpec) error {
	route, err := r.routeForSpec(spec)
	if err != nil {
		return err
	}
	return r.Attach(ctx, spec, route)
}

// Replace drains the old generation before atomically publishing the next.
func (r *Runtime) Replace(ctx context.Context, expected catalog.Generation, next tenant.TenantSpec, route Route) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	if err := r.requireActive(); err != nil {
		return err
	}
	if err := r.validateBinding(next, route); err != nil {
		return err
	}
	current, ok := r.routes.Load().byTenant[next.ID]
	if !ok {
		return tenant.ErrTenantNotFound
	}
	if current.route.Generation != expected {
		return fmt.Errorf("%w: got %d, current %d", tenant.ErrGenerationConflict, expected, current.route.Generation)
	}
	if other, ok := r.routes.Load().byName[routeKey(route.Name)]; ok && other.route.Tenant != route.Tenant {
		return fmt.Errorf("%w: name %q belongs to tenant %q", ErrRouteConflict, route.Name, other.route.Tenant)
	}
	if err := r.tenants.ReplaceTenant(ctx, expected, next); err != nil {
		return err
	}
	if err := r.trip(failAfterTenantTransition); err != nil {
		return err
	}
	if err := r.publishSpecs(); err != nil {
		return err
	}
	return r.trip(failAfterPublish)
}

// ReplaceTenant replaces one exact tenant generation and its immutable route.
func (r *Runtime) ReplaceTenant(ctx context.Context, expected catalog.Generation, next tenant.TenantSpec) error {
	route, err := r.routeForSpec(next)
	if err != nil {
		return err
	}
	return r.Replace(ctx, expected, next, route)
}

// Detach removes one exact tenant generation without touching the native root.
func (r *Runtime) Detach(ctx context.Context, id catalog.TenantID, generation catalog.Generation) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	if err := r.requireActive(); err != nil {
		return err
	}
	entry, ok := r.routes.Load().byTenant[id]
	if !ok {
		return tenant.ErrTenantNotFound
	}
	if entry.route.Generation != generation {
		return fmt.Errorf("%w: got %d, current %d", tenant.ErrGenerationConflict, generation, entry.route.Generation)
	}
	if err := r.tenants.RemoveTenant(ctx, id, generation); err != nil {
		return err
	}
	if err := r.trip(failAfterTenantTransition); err != nil {
		return err
	}
	if err := r.publishSpecs(); err != nil {
		return err
	}
	return r.trip(failAfterPublish)
}

// RemoveTenant removes one exact tenant generation without touching the native root.
func (r *Runtime) RemoveTenant(ctx context.Context, id catalog.TenantID, generation catalog.Generation) error {
	return r.Detach(ctx, id, generation)
}

// Route returns the exact published route for tenant.
func (r *Runtime) Route(id catalog.TenantID, generation catalog.Generation) (Route, error) {
	entry, ok := r.routes.Load().byTenant[id]
	if !ok {
		return Route{}, tenant.ErrTenantNotFound
	}
	if entry.route.Generation != generation {
		return Route{}, fmt.Errorf("%w: got %d, current %d", tenant.ErrGenerationConflict, generation, entry.route.Generation)
	}
	return entry.route, nil
}

// PrepareTenant converges one exact mounted tenant generation to revision.
func (r *Runtime) PrepareTenant(ctx context.Context, id catalog.TenantID, generation catalog.Generation, revision catalog.Revision) (tenant.TenantState, error) {
	if _, err := r.Route(id, generation); err != nil {
		return tenant.TenantState{}, err
	}
	lease, err := r.tenants.AcquireGeneration(ctx, id, generation)
	if err != nil {
		return tenant.TenantState{}, err
	}
	defer lease.Release()
	return lease.Prepare(ctx, revision)
}

// State returns the durable state of one exact mounted tenant generation.
func (r *Runtime) State(ctx context.Context, id catalog.TenantID, generation catalog.Generation) (tenant.TenantState, error) {
	if _, err := r.Route(id, generation); err != nil {
		return tenant.TenantState{}, err
	}
	state, err := r.tenants.State(ctx, id)
	if err != nil {
		return tenant.TenantState{}, err
	}
	if state.Generation != generation {
		return tenant.TenantState{}, fmt.Errorf("%w: got %d, current %d", tenant.ErrGenerationConflict, generation, state.Generation)
	}
	return state, nil
}

// Routes returns the current immutable bindings in stable tenant order.
func (r *Runtime) Routes() []Route {
	snapshot := r.routes.Load()
	routes := make([]Route, 0, len(snapshot.byTenant))
	for _, entry := range snapshot.byTenant {
		routes = append(routes, entry.route)
	}
	slices.SortFunc(routes, func(a, b Route) int { return strings.Compare(string(a.Tenant), string(b.Tenant)) })
	return routes
}

// Pin resolves name and holds its exact tenant generation through one callback.
func (r *Runtime) Pin(ctx context.Context, name string) (*PinnedRoute, error) {
	key := routeKey(name)
	for {
		snapshot := r.routes.Load()
		entry, ok := snapshot.byName[key]
		if !ok {
			return nil, catalog.ErrNotFound
		}
		lease, err := r.tenants.AcquireGeneration(ctx, entry.route.Tenant, entry.route.Generation)
		if err != nil {
			if errors.Is(err, tenant.ErrTenantChanging) || errors.Is(err, tenant.ErrGenerationConflict) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-snapshot.changed:
				}
				continue
			}
			return nil, err
		}
		r.mu.Lock()
		current, stillBound := r.routes.Load().byName[key]
		if r.closing || r.closed || !r.started {
			r.mu.Unlock()
			lease.Release()
			return nil, ErrClosed
		}
		if !stillBound || current.route != entry.route {
			r.mu.Unlock()
			lease.Release()
			continue
		}
		r.active++
		r.mu.Unlock()
		return &PinnedRoute{Route: entry.route, Spec: entry.spec, runtime: r, lease: lease}, nil
	}
}

// Release ends the exact-generation callback pin.
func (p *PinnedRoute) Release() {
	p.once.Do(func() {
		p.lease.Release()
		p.runtime.releasePin()
	})
}

// Close drains pins, clears routes, and gracefully releases the sole native root.
func (r *Runtime) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.closeTimeout)
	defer cancel()
	return r.CloseContext(ctx)
}

// CloseContext drains pins within ctx before releasing the sole native root.
func (r *Runtime) CloseContext(ctx context.Context) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	if !r.started && !r.closing {
		r.closed = true
		r.mu.Unlock()
		return r.native.Close()
	}
	if !r.closing {
		r.closing = true
		if r.active == 0 {
			close(r.drained)
		}
	}
	drained := r.drained
	r.mu.Unlock()
	select {
	case <-ctx.Done():
		return fmt.Errorf("mount mux: drain callbacks: %w", ctx.Err())
	case <-drained:
	}
	r.swapSnapshot(emptySnapshot())
	err := r.native.Close()
	r.mu.Lock()
	r.closed = true
	r.closing = false
	r.started = false
	r.mu.Unlock()
	if err != nil {
		return fmt.Errorf("mount mux: close native root: %w", err)
	}
	return nil
}

func (r *Runtime) releasePin() {
	r.mu.Lock()
	r.active--
	if r.closing && r.active == 0 {
		close(r.drained)
	}
	r.mu.Unlock()
}

func (r *Runtime) requireActive() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.closing {
		return ErrClosed
	}
	if !r.started {
		return ErrNotStarted
	}
	return nil
}

func (r *Runtime) validateBinding(spec tenant.TenantSpec, route Route) error {
	if route.Tenant != spec.ID || route.Generation != spec.Generation {
		return fmt.Errorf("%w: route identity does not match tenant spec", ErrInvalidRoute)
	}
	if !spec.Traits.Presentations.Has(catalog.PresentationMount) {
		return fmt.Errorf("%w: tenant has no mount presentation", ErrInvalidRoute)
	}
	if err := validateName(route.Name); err != nil {
		return err
	}
	want := filepath.Join(r.root, route.Name)
	if filepath.Clean(spec.PresentationRoot) != want {
		return fmt.Errorf("%w: presentation root %q, want %q", ErrInvalidRoute, spec.PresentationRoot, want)
	}
	return nil
}

func (r *Runtime) routeForSpec(spec tenant.TenantSpec) (Route, error) {
	relative, err := filepath.Rel(r.root, filepath.Clean(spec.PresentationRoot))
	if err != nil || relative == "." || filepath.Dir(relative) != "." {
		return Route{}, fmt.Errorf(
			"%w: presentation root %q is not a direct child of %q",
			ErrInvalidRoute, spec.PresentationRoot, r.root,
		)
	}
	route := Route{Tenant: spec.ID, Generation: spec.Generation, Name: relative}
	if err := r.validateBinding(spec, route); err != nil {
		return Route{}, err
	}
	return route, nil
}

func (r *Runtime) rejectConflict(route Route) error {
	snapshot := r.routes.Load()
	if existing, ok := snapshot.byTenant[route.Tenant]; ok {
		if existing.route == route {
			return nil
		}
		return fmt.Errorf("%w: tenant %q already bound", ErrRouteConflict, route.Tenant)
	}
	if existing, ok := snapshot.byName[routeKey(route.Name)]; ok {
		return fmt.Errorf("%w: name %q belongs to tenant %q", ErrRouteConflict, route.Name, existing.route.Tenant)
	}
	return nil
}

func (r *Runtime) publishSpecs() error {
	next, err := r.snapshotFromSpecs(r.tenants.Specs())
	if err != nil {
		return err
	}
	r.swapSnapshot(next)
	return nil
}

func (r *Runtime) swapSnapshot(next *routeSnapshot) {
	previous := r.routes.Swap(next)
	if previous != nil {
		close(previous.changed)
	}
}

func (r *Runtime) snapshotFromSpecs(specs []tenant.TenantSpec) (*routeSnapshot, error) {
	next := emptySnapshot()
	for _, spec := range specs {
		if !spec.Traits.Presentations.Has(catalog.PresentationMount) {
			continue
		}
		route, err := r.routeForSpec(spec)
		if err != nil {
			return nil, err
		}
		key := routeKey(route.Name)
		if existing, ok := next.byName[key]; ok {
			return nil, fmt.Errorf("%w: names %q and %q collide", ErrRouteConflict, existing.route.Name, route.Name)
		}
		entry := routeEntry{route: route, spec: spec}
		next.byName[key] = entry
		next.byTenant[route.Tenant] = entry
	}
	return next, nil
}

func (r *Runtime) trip(point string) error {
	if r.fail == nil {
		return nil
	}
	return r.fail(point)
}

func emptySnapshot() *routeSnapshot {
	return &routeSnapshot{
		byName: map[string]routeEntry{}, byTenant: map[catalog.TenantID]routeEntry{}, changed: make(chan struct{}),
	}
}

func validateName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\\`) || !norm.NFC.IsNormalString(name) {
		return fmt.Errorf("%w: invalid root name %q", ErrInvalidRoute, name)
	}
	return nil
}

func routeKey(name string) string { return cases.Fold().String(name) }
