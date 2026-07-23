package mountmux

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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
	onStart    func(Resolver) error
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
	if n.onStart != nil {
		return n.onStart(resolver)
	}
	return n.startError
}

func (n *fakeNative) Close(context.Context) error {
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

type fakeDomainRemover struct {
	mu         sync.Mutex
	owner      string
	tenant     catalog.TenantID
	generation catalog.Generation
	called     chan struct{}
	release    chan struct{}
	confirmed  bool
}

func (r *fakeDomainRemover) RemoveTenantDomain(_ context.Context, owner string, id catalog.TenantID, generation catalog.Generation) error {
	r.mu.Lock()
	r.owner, r.tenant, r.generation = owner, id, generation
	called, release := r.called, r.release
	r.mu.Unlock()
	if called != nil {
		select {
		case <-called:
		default:
			close(called)
		}
	}
	if release != nil {
		<-release
	}
	r.mu.Lock()
	r.confirmed = true
	r.mu.Unlock()
	return nil
}

func (r *fakeDomainRemover) ProveTenantDomainRemoved(_ context.Context, owner string, id catalog.TenantID, generation catalog.Generation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.confirmed || r.owner != owner || r.tenant != id || r.generation != generation {
		return catalog.ErrNotFound
	}
	return nil
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

func (c *fakeController) ProvisionTenant(_ context.Context, spec tenant.TenantSpec) error {
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

func (c *fakeController) State(_ context.Context, owner tenant.OwnerID, id catalog.TenantID) (tenant.TenantStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	spec, ok := c.specs[id]
	if !ok {
		return tenant.TenantStatus{}, tenant.ErrTenantNotFound
	}
	if spec.OwnerID != owner {
		return tenant.TenantStatus{}, tenant.ErrTenantOwnerMismatch
	}
	return tenant.TenantStatus{
		Owner: owner, ReplacementEligible: true,
		State: tenant.TenantState{Tenant: id, Generation: spec.Generation, Activated: spec.Generation},
	}, nil
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
		Mount:   tenant.MountSpec{PresentationRoot: filepath.Join(root, name)},
		Backing: tenant.BackingSpec{Root: filepath.Join(root, "backing", id)},
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
		if err := runtime.Provision(t.Context(), spec, route); err != nil {
			t.Fatalf("Provision(%s): %v", name, err)
		}
		if err := runtime.Detach(t.Context(), spec.OwnerID, spec.ID, spec.Generation); err != nil {
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

func TestNativeRootCanPinPublishedRoutesBeforeStartAcknowledgesReadiness(t *testing.T) {
	root := filepath.Join(t.TempDir(), "mount")
	spec := testSpec(root, "tenant-one", "one", 1)
	native := newFakeNative()
	native.onStart = func(resolver Resolver) error {
		pin, err := resolver.Pin(t.Context(), "one")
		if err != nil {
			return err
		}
		if pin.Route.Tenant != spec.ID || pin.Route.Generation != spec.Generation {
			t.Fatalf("startup pin = %+v, want tenant %q generation %d", pin.Route, spec.ID, spec.Generation)
		}
		return pin.Release()
	}
	runtime, err := New(Config{Root: root, Tenants: newFakeController(spec), Native: native})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
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
	if err := runtime.Provision(t.Context(), one, Route{Tenant: one.ID, Generation: 1, Name: "Alpha"}); err != nil {
		t.Fatal(err)
	}
	two := testSpec(root, "tenant-two", "alpha", 1)
	if err := runtime.Provision(t.Context(), two, Route{Tenant: two.ID, Generation: 1, Name: "alpha"}); !errors.Is(err, ErrRouteConflict) {
		t.Fatalf("case-folded conflicting Provision = %v, want ErrRouteConflict", err)
	}
	if err := runtime.Detach(t.Context(), one.OwnerID, one.ID, 2); !errors.Is(err, tenant.ErrGenerationConflict) {
		t.Fatalf("stale Detach = %v, want generation conflict", err)
	}
	next := one
	next.Generation = 2
	if err := runtime.Replace(t.Context(), 2, next, Route{Tenant: next.ID, Generation: 2, Name: "Alpha"}); !errors.Is(err, tenant.ErrGenerationConflict) {
		t.Fatalf("stale Replace = %v, want generation conflict", err)
	}
}

func TestRouteNameHasExactPortableUTF8Bound(t *testing.T) {
	if err := validateName(strings.Repeat("a", catalog.MaxNameBytes)); err != nil {
		t.Fatalf("validateName(exact max): %v", err)
	}
	for name, value := range map[string]string{
		"over max":     strings.Repeat("a", catalog.MaxNameBytes+1),
		"invalid utf8": string([]byte{0xff}),
		"control":      "bad\u0001name",
		"slash":        "bad/name",
		"backslash":    `bad\name`,
		"dot":          ".",
		"dot dot":      "..",
	} {
		if err := validateName(value); !errors.Is(err, ErrInvalidRoute) {
			t.Fatalf("validateName(%s) = %v, want ErrInvalidRoute", name, err)
		}
	}
}

func TestFileProviderRemovalBlocksTenantAcknowledgementUntilDomainAbsence(t *testing.T) {
	root := filepath.Join(t.TempDir(), "mount")
	spec := testSpec(root, "tenant-domain", "Domain", 7)
	spec.Traits.Presentations = catalog.PresentMount | catalog.PresentFileProvider
	spec.FileProvider = tenant.FileProviderSpec{Enabled: true, PresentationInstanceID: "instance", DisplayName: "Domain"}
	controller := newFakeController()
	domains := &fakeDomainRemover{called: make(chan struct{}), release: make(chan struct{})}
	runtime, err := New(Config{Root: root, Tenants: controller, Native: newFakeNative(), Domains: domains})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ProvisionTenant(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	settled := make(chan error, 1)
	go func() { settled <- runtime.RemoveTenant(t.Context(), spec.ID, spec.Generation, spec.OwnerID) }()
	select {
	case <-domains.called:
	case <-time.After(time.Second):
		t.Fatal("domain removal was not requested")
	}
	select {
	case err := <-settled:
		t.Fatalf("tenant removal acknowledged before domain absence: %v", err)
	default:
	}
	if len(controller.Specs()) != 1 {
		t.Fatal("tenant state retired before domain absence")
	}
	close(domains.release)
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
	if len(controller.Specs()) != 0 {
		t.Fatal("tenant state remained after domain absence")
	}
	if err := runtime.RemoveTenant(t.Context(), spec.ID, spec.Generation, spec.OwnerID); err != nil {
		t.Fatalf("lost removal response replay = %v", err)
	}
}

func TestFileProviderRemovalRejectsWrongOwnerBeforeDomainCommand(t *testing.T) {
	root := filepath.Join(t.TempDir(), "mount")
	spec := testSpec(root, "tenant-domain-owner", "Domain", 3)
	spec.Traits.Presentations = catalog.PresentMount | catalog.PresentFileProvider
	spec.FileProvider = tenant.FileProviderSpec{Enabled: true, PresentationInstanceID: "instance", DisplayName: "Domain"}
	domains := &fakeDomainRemover{called: make(chan struct{})}
	runtime, err := New(Config{Root: root, Tenants: newFakeController(spec), Native: newFakeNative(), Domains: domains})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.RemoveTenant(t.Context(), spec.ID, spec.Generation, "wrong-owner"); !errors.Is(err, tenant.ErrTenantOwnerMismatch) {
		t.Fatalf("wrong owner removal = %v", err)
	}
	select {
	case <-domains.called:
		t.Fatal("domain command issued for wrong owner")
	default:
	}
}

func TestStateSerializesWithMultiGenerationReplacement(t *testing.T) {
	controller := newFakeController()
	runtime, root := newRuntime(t, controller, newFakeNative(), nil)
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	current := testSpec(root, "tenant", "tenant", 1)
	if err := runtime.ProvisionTenant(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	next := current
	next.Generation = 9
	type stateResult struct {
		status tenant.TenantStatus
		err    error
	}
	stateDone := make(chan stateResult, 1)
	replaceDone := make(chan error, 1)
	go func() {
		status, err := runtime.State(context.Background(), current.ID, current.OwnerID)
		stateDone <- stateResult{status: status, err: err}
	}()
	go func() { replaceDone <- runtime.ReplaceTenant(context.Background(), 1, next) }()
	state := <-stateDone
	if state.err != nil || state.status.Owner != current.OwnerID || state.status.State.Tenant != current.ID ||
		(state.status.State.Generation != 1 && state.status.State.Generation != 9) || !state.status.ReplacementEligible {
		t.Fatalf("racing state = %+v, %v", state.status, state.err)
	}
	if err := <-replaceDone; err != nil {
		t.Fatal(err)
	}
	final, err := runtime.State(t.Context(), current.ID, current.OwnerID)
	if err != nil || final.State.Generation != 9 {
		t.Fatalf("final state = %+v, %v", final, err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
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
	if err := pin.Release(); err != nil {
		t.Fatalf("release old route pin: %v", err)
	}
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
			if err := runtime.Provision(t.Context(), spec, route); !errors.Is(err, injected) {
				t.Fatalf("Provision failpoint = %v, want injected", err)
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
	case <-time.After(time.Second):
		t.Fatal("native root termination did not start while callback remained pinned")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before callback release: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if err := pin.Release(); err != nil {
		t.Fatalf("release close pin: %v", err)
	}
	if runtime.Busy() {
		t.Fatal("runtime remains busy after callback release")
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	page, err := runtime.RoutePage(context.Background(), RouteCursor{}, MaxRoutePageSize)
	if err != nil || len(page.Routes) != 0 {
		t.Fatalf("routes after close = %+v, %v, want empty", page, err)
	}
}

func TestCloseContextDeadlineBoundsPinSettlementWait(t *testing.T) {
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
	closed := make(chan error, 1)
	go func() { closed <- runtime.CloseContext(ctx) }()
	for {
		runtime.mu.Lock()
		closing := runtime.closing
		runtime.mu.Unlock()
		if closing {
			break
		}
	}
	<-ctx.Done()
	if err := <-closed; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseContext = %v, want bounded pin-settlement deadline", err)
	}
	if _, closes := native.counts(); closes != 1 {
		t.Fatalf("native closes before callback settlement = %d, want 1", closes)
	}
	if _, err := runtime.Pin(t.Context(), "one"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pin while draining = %v, want ErrClosed", err)
	}
	if err := pin.Release(); err != nil {
		t.Fatalf("release deadline pin: %v", err)
	}
	if _, closes := native.counts(); closes != 1 {
		t.Fatalf("native closes after exact settlement = %d, want 1", closes)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("second Close = %v, want terminal result without prior caller deadline", err)
	}
}

func TestCloseContextDeadlineBoundsNativeSettlementWait(t *testing.T) {
	root := filepath.Join(t.TempDir(), "mount")
	native := &blockingNativeClose{
		fakeNative: newFakeNative(),
		started:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	runtime, err := New(Config{Root: root, Tenants: newFakeController(), Native: native})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- runtime.CloseContext(ctx) }()
	<-native.started
	<-ctx.Done()
	if err := <-closed; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseContext = %v, want bounded native-settlement deadline", err)
	}
	close(native.release)
	if err := runtime.Close(); err != nil {
		t.Fatalf("second Close = %v, want terminal result without prior caller deadline", err)
	}
	if _, closes := native.counts(); closes != 1 {
		t.Fatalf("native closes after settlement = %d, want 1", closes)
	}
}

type blockingNativeClose struct {
	*fakeNative
	started chan struct{}
	release chan struct{}
}

func (n *blockingNativeClose) Close(ctx context.Context) error {
	close(n.started)
	<-n.release
	return n.fakeNative.Close(ctx)
}

func TestCloseReplaysNativeFailureAndJoinsLaterCallerDeadline(t *testing.T) {
	injected := errors.New("injected native close failure")
	native := newFakeNative()
	native.closeError = injected
	runtime, _ := newRuntime(t, newFakeController(), native, nil)
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.CloseContext(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("first CloseContext = %v, want native failure", err)
	}
	if err := runtime.Close(); !errors.Is(err, injected) {
		t.Fatalf("second Close = %v, want cached native failure", err)
	}
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	err := runtime.CloseContext(expired)
	if !errors.Is(err, injected) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expired replay = %v, want native failure and caller cancellation", err)
	}
	if _, closes := native.counts(); closes != 1 {
		t.Fatalf("native close calls = %d, want one", closes)
	}
}

func TestRoutePagesAreBoundedOrderedAndSnapshotFenced(t *testing.T) {
	root := filepath.Join(t.TempDir(), "mount")
	specs := make([]tenant.TenantSpec, 0, MaxRoutePageSize+3)
	for index := 0; index < MaxRoutePageSize+3; index++ {
		name := fmt.Sprintf("acct-%03d", MaxRoutePageSize+2-index)
		specs = append(specs, testSpec(root, "tenant-"+name, name, 1))
	}
	controller := newFakeController(specs...)
	runtime, err := New(Config{Root: root, Tenants: controller, Native: newFakeNative()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.RoutePage(t.Context(), RouteCursor{}, MaxRoutePageSize); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("RoutePage before Start = %v, want ErrNotStarted", err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	first, err := runtime.RoutePage(t.Context(), RouteCursor{}, MaxRoutePageSize)
	if err != nil {
		t.Fatalf("RoutePage(first): %v", err)
	}
	if len(first.Routes) != MaxRoutePageSize || first.Next == nil {
		t.Fatalf("first route page = %+v", first)
	}
	for index := 1; index < len(first.Routes); index++ {
		if first.Routes[index-1].Name >= first.Routes[index].Name {
			t.Fatalf("route page is not ordered at %d", index)
		}
	}
	second, err := runtime.RoutePage(t.Context(), *first.Next, MaxRoutePageSize)
	if err != nil {
		t.Fatalf("RoutePage(second): %v", err)
	}
	if len(second.Routes) != 3 || second.Next != nil || second.Snapshot != first.Snapshot {
		t.Fatalf("second route page = %+v", second)
	}
	added := testSpec(root, "tenant-added", "acct-added", 1)
	if err := runtime.Provision(t.Context(), added, Route{Tenant: added.ID, Generation: 1, Name: "acct-added"}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if _, err := runtime.RoutePage(t.Context(), *first.Next, MaxRoutePageSize); !errors.Is(err, tenant.ErrGenerationConflict) {
		t.Fatalf("stale route cursor = %v, want generation conflict", err)
	}
}

func TestExactRouteCursorUsesLogarithmicSearchAtTenThousandRoutes(t *testing.T) {
	const count = 10_000
	routes := make([]Route, count)
	for index := range routes {
		routes[index].Name = fmt.Sprintf("acct-%05d", index)
	}
	comparisons := 0
	compare := func(left, right string) int {
		comparisons++
		return strings.Compare(left, right)
	}
	pages := 0
	for afterIndex := MaxRoutePageSize - 1; afterIndex < len(routes); afterIndex += MaxRoutePageSize {
		index, ok := exactRouteCursor(routes, routes[afterIndex].Name, compare)
		if !ok || index != afterIndex {
			t.Fatalf("cursor %d resolved to %d, %v", afterIndex, index, ok)
		}
		pages++
	}
	maxComparisons := pages * 15
	if comparisons > maxComparisons {
		t.Fatalf("10k route cursor comparisons = %d, want <= %d", comparisons, maxComparisons)
	}
	if _, ok := exactRouteCursor(routes, "acct-missing", compare); ok {
		t.Fatal("missing route cursor resolved")
	}
}
