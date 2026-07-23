// Package mountmux owns the single native mount and its immutable tenant routes.
package mountmux

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

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
	// MaxRoutePageSize bounds one immutable native-root route page.
	MaxRoutePageSize = 32
)

// Route is one immutable root name to exact tenant-generation binding.
type Route struct {
	Tenant     catalog.TenantID
	Generation catalog.Generation
	Name       string
}

// RouteCursor fences one page to an immutable route-table version.
type RouteCursor struct {
	Snapshot uint64
	After    string
}

// RoutePage contains one bounded stable-name-ordered route page.
type RoutePage struct {
	Snapshot uint64
	Routes   []Route
	Next     *RouteCursor
}

// NativeRoot owns the one kernel mount for a Runtime. Start may use resolver
// before reporting readiness and must settle every such pin before failing.
type NativeRoot interface {
	Start(context.Context, string, Resolver) error
	Close(context.Context) error
}

// GenerationPin holds one tenant generation through a callback.
type GenerationPin interface {
	Prepare(context.Context, catalog.Revision) (tenant.TenantState, error)
	Release()
}

// DomainRemover settles one exact File Provider domain before tenant retirement.
type DomainRemover interface {
	RemoveTenantDomain(context.Context, string, catalog.TenantID, catalog.Generation) error
	ProveTenantDomainRemoved(context.Context, string, catalog.TenantID, catalog.Generation) error
}

// TenantController is the exact tenant lifecycle surface required by Runtime.
type TenantController interface {
	ProvisionTenant(context.Context, tenant.TenantSpec) error
	ReplaceTenant(context.Context, catalog.Generation, tenant.TenantSpec) error
	RemoveTenant(context.Context, catalog.TenantID, catalog.Generation) error
	AcquireGeneration(context.Context, catalog.TenantID, catalog.Generation) (GenerationPin, error)
	State(context.Context, tenant.OwnerID, catalog.TenantID) (tenant.TenantStatus, error)
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

func (c tenantController) ProvisionTenant(ctx context.Context, spec tenant.TenantSpec) error {
	return c.runtime.ProvisionTenant(ctx, spec)
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

func (c tenantController) State(ctx context.Context, owner tenant.OwnerID, id catalog.TenantID) (tenant.TenantStatus, error) {
	return c.runtime.State(ctx, owner, id)
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
	Domains DomainRemover
	// CloseTimeout records a close deadline while exact settlement continues.
	CloseTimeout time.Duration

	failpoint func(string) error
}

type routeEntry struct {
	route Route
	spec  tenant.TenantSpec
}

type routeSnapshot struct {
	version  uint64
	byName   map[string]routeEntry
	byTenant map[catalog.TenantID]routeEntry
	ordered  []Route
	changed  chan struct{}
}

// Runtime owns one native root for its entire started lifetime.
type Runtime struct {
	root         string
	tenants      TenantController
	native       NativeRoot
	domains      DomainRemover
	fail         func(string) error
	closeTimeout time.Duration

	lifecycle sync.Mutex
	routes    atomic.Pointer[routeSnapshot]
	routeNext atomic.Uint64

	mu        sync.Mutex
	starting  bool
	started   bool
	closing   bool
	closed    bool
	active    int
	drained   chan struct{}
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// PinnedRoute holds an exact tenant generation until Release.
type PinnedRoute struct {
	Route Route
	Spec  tenant.TenantSpec

	release func() error
	once    sync.Once
	err     error
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
		root: root, tenants: config.Tenants, native: config.Native, domains: config.Domains, fail: config.failpoint,
		closeTimeout: closeTimeout, drained: make(chan struct{}), closeDone: make(chan struct{}),
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
	r.mu.Lock()
	r.starting = true
	r.mu.Unlock()
	if err := r.native.Start(ctx, r.root, r); err != nil {
		r.mu.Lock()
		r.starting = false
		r.mu.Unlock()
		r.swapSnapshot(emptySnapshot())
		return fmt.Errorf("mount mux: establish native root: %w", err)
	}
	r.mu.Lock()
	r.starting = false
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

// Provision durably creates one exact tenant generation and publishes its route.
func (r *Runtime) Provision(ctx context.Context, spec tenant.TenantSpec, route Route) error {
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
	if err := r.tenants.ProvisionTenant(ctx, spec); err != nil {
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

// ProvisionTenant durably creates one mount tenant using its fixed presentation root.
func (r *Runtime) ProvisionTenant(ctx context.Context, spec tenant.TenantSpec) error {
	route, err := r.routeForSpec(spec)
	if err != nil {
		return err
	}
	return r.Provision(ctx, spec, route)
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
func (r *Runtime) Detach(ctx context.Context, owner tenant.OwnerID, id catalog.TenantID, generation catalog.Generation) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	if err := r.requireActive(); err != nil {
		return err
	}
	entry, ok := r.routes.Load().byTenant[id]
	if !ok {
		if r.domains != nil {
			if err := r.domains.ProveTenantDomainRemoved(ctx, string(owner), id, generation); err == nil {
				return nil
			} else if !errors.Is(err, catalog.ErrNotFound) {
				return err
			}
		}
		return tenant.ErrTenantNotFound
	}
	if entry.spec.OwnerID != owner {
		return tenant.ErrTenantOwnerMismatch
	}
	if entry.route.Generation != generation {
		return fmt.Errorf("%w: got %d, current %d", tenant.ErrGenerationConflict, generation, entry.route.Generation)
	}
	fileProvider := entry.spec.FileProvider.Enabled
	if fileProvider {
		if r.domains == nil {
			return errors.New("mount mux: File Provider domain remover is required")
		}
		if err := r.domains.RemoveTenantDomain(ctx, string(owner), id, generation); err != nil {
			return err
		}
	}
	if err := r.tenants.RemoveTenant(ctx, id, generation); err != nil && (!fileProvider || !errors.Is(err, tenant.ErrTenantNotFound)) {
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
func (r *Runtime) RemoveTenant(ctx context.Context, id catalog.TenantID, generation catalog.Generation, owner tenant.OwnerID) error {
	return r.Detach(ctx, owner, id, generation)
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

// State returns one owner-fenced durable lifecycle snapshot.
func (r *Runtime) State(ctx context.Context, id catalog.TenantID, owner tenant.OwnerID) (tenant.TenantStatus, error) {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	if err := r.requireActive(); err != nil {
		return tenant.TenantStatus{}, err
	}
	entry, ok := r.routes.Load().byTenant[id]
	if !ok {
		return tenant.TenantStatus{}, tenant.ErrTenantNotFound
	}
	status, err := r.tenants.State(ctx, owner, id)
	if err != nil {
		return tenant.TenantStatus{}, err
	}
	if status.State.Tenant != id || status.State.Generation != entry.route.Generation {
		return tenant.TenantStatus{}, fmt.Errorf("%w: route and tenant state differ", catalog.ErrIntegrity)
	}
	return status, nil
}

// RoutePage returns one version-fenced immutable route page.
func (r *Runtime) RoutePage(_ context.Context, cursor RouteCursor, limit int) (RoutePage, error) {
	if limit <= 0 || limit > MaxRoutePageSize {
		return RoutePage{}, fmt.Errorf("%w: route page limit %d", ErrInvalidRoute, limit)
	}
	snapshot := r.routes.Load()
	if snapshot.version == 0 {
		return RoutePage{}, ErrNotStarted
	}
	if cursor.Snapshot != 0 && cursor.Snapshot != snapshot.version {
		return RoutePage{}, fmt.Errorf("%w: route snapshot changed", tenant.ErrGenerationConflict)
	}
	start := 0
	if cursor.After != "" {
		index, ok := exactRouteCursor(snapshot.ordered, cursor.After, strings.Compare)
		if !ok {
			return RoutePage{}, fmt.Errorf("%w: route cursor is not in snapshot", ErrInvalidRoute)
		}
		start = index + 1
	}
	end := min(start+limit, len(snapshot.ordered))
	page := RoutePage{
		Snapshot: snapshot.version,
		Routes:   slices.Clone(snapshot.ordered[start:end]),
	}
	if end < len(snapshot.ordered) {
		page.Next = &RouteCursor{Snapshot: snapshot.version, After: snapshot.ordered[end-1].Name}
	}
	return page, nil
}

// Busy reports whether a kernel callback holds an exact tenant generation.
func (r *Runtime) Busy() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active != 0
}

// Pin resolves name and holds its exact tenant generation through one callback.
func (r *Runtime) Pin(ctx context.Context, name string) (*PinnedRoute, error) {
	key := routeKey(name)
	for {
		r.mu.Lock()
		closing := r.closing || r.closed
		r.mu.Unlock()
		if closing {
			return nil, ErrClosed
		}
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
		if r.closing || r.closed || (!r.started && !r.starting) {
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
		return &PinnedRoute{Route: entry.route, Spec: entry.spec, release: func() error {
			lease.Release()
			r.releasePin()
			return nil
		}}, nil
	}
}

// Release ends the exact-generation callback pin.
func (p *PinnedRoute) Release() error {
	p.once.Do(func() {
		p.err = p.release()
	})
	return p.err
}

// Close drains pins, clears routes, and gracefully releases the sole native root.
func (r *Runtime) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.closeTimeout)
	defer cancel()
	return r.CloseContext(ctx)
}

// CloseContext starts exact terminal settlement and bounds this caller's wait.
func (r *Runtime) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closing = true
		if r.active == 0 {
			close(r.drained)
		}
		r.mu.Unlock()
		go r.finishClose()
	})
	select {
	case <-r.closeDone:
		r.mu.Lock()
		result := r.closeErr
		r.mu.Unlock()
		return errors.Join(result, ctx.Err())
	default:
	}
	select {
	case <-r.closeDone:
		r.mu.Lock()
		result := r.closeErr
		r.mu.Unlock()
		return errors.Join(result, ctx.Err())
	case <-ctx.Done():
		select {
		case <-r.closeDone:
			r.mu.Lock()
			result := r.closeErr
			r.mu.Unlock()
			return errors.Join(result, ctx.Err())
		default:
		}
		return fmt.Errorf("mount mux: close deadline elapsed before settlement: %w", ctx.Err())
	}
}

func (r *Runtime) finishClose() {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.mu.Lock()
	drained := r.drained
	r.mu.Unlock()
	r.swapSnapshot(emptySnapshot())
	nativeDone := make(chan error, 1)
	go func() { nativeDone <- r.native.Close(context.Background()) }()
	<-drained
	err := <-nativeDone
	r.mu.Lock()
	r.closed = true
	r.closing = false
	r.started = false
	if err != nil {
		err = fmt.Errorf("mount mux: close native root: %w", err)
	}
	r.closeErr = err
	r.mu.Unlock()
	close(r.closeDone)
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
	next.version = r.routeNext.Add(1)
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
		next.ordered = append(next.ordered, route)
	}
	slices.SortFunc(next.ordered, func(left, right Route) int {
		return strings.Compare(left.Name, right.Name)
	})
	return next, nil
}

func exactRouteCursor(
	ordered []Route,
	after string,
	compare func(string, string) int,
) (int, bool) {
	index := sort.Search(len(ordered), func(index int) bool {
		return compare(ordered[index].Name, after) >= 0
	})
	return index, index < len(ordered) && ordered[index].Name == after
}

func (r *Runtime) trip(point string) error {
	if r.fail == nil {
		return nil
	}
	return r.fail(point)
}

func emptySnapshot() *routeSnapshot {
	return &routeSnapshot{
		byName: map[string]routeEntry{}, byTenant: map[catalog.TenantID]routeEntry{},
		ordered: []Route{}, changed: make(chan struct{}),
	}
}

func validateName(name string) error {
	if name == "" || len(name) > catalog.MaxNameBytes || !utf8.ValidString(name) ||
		name == "." || name == ".." || strings.ContainsAny(name, `/\\`) || !norm.NFC.IsNormalString(name) {
		return fmt.Errorf("%w: invalid root name %q", ErrInvalidRoute, name)
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return fmt.Errorf("%w: invalid root name %q", ErrInvalidRoute, name)
		}
	}
	return nil
}

func routeKey(name string) string { return cases.Fold().String(name) }
