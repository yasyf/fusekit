package holder

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/tenant"
)

func TestAuthorityRegistryAcknowledgesInitialEmptyFleet(t *testing.T) {
	store := openHolderFleetCatalog(t)
	factoryCalls := 0
	registry := newHolderFleetRegistry(
		t, store,
		SourceAuthorityFleet{Owner: "product", Generation: 1},
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			factoryCalls++
			return newTestAuthority(), nil
		},
	)
	if err := registry.start(t.Context(), nil); err != nil {
		t.Fatalf("start empty fleet: %v", err)
	}
	closeHolderFleetRegistry(t, registry)
	if factoryCalls != 0 {
		t.Fatalf("empty fleet factory calls = %d, want 0", factoryCalls)
	}
	status, err := store.SourceAuthorityFleetHead(t.Context(), "product")
	if err != nil {
		t.Fatalf("SourceAuthorityFleetHead: %v", err)
	}
	if status.Pending != nil || status.Current == nil ||
		status.Current.Generation != 1 || status.Current.AuthorityCount != 0 {
		t.Fatalf("empty fleet status = %+v", status)
	}
}

func TestAuthorityRegistryRemovalRequiresClosedUnreferencedAuthority(t *testing.T) {
	t.Run("closes runtime fence before dependency-free retirement", func(t *testing.T) {
		store := openHolderFleetCatalog(t)
		oldAuthority := newTestAuthority()
		old := newHolderFleetRegistry(
			t, store,
			SourceAuthorityFleet{
				Owner: "product", Generation: 1,
				Authorities: []SourceAuthoritySpec{testSourceAuthoritySpec("alpha")},
			},
			func(context.Context, sourceauthority.Config) (managedAuthority, error) {
				return oldAuthority, nil
			},
		)
		if err := old.start(t.Context(), nil); err != nil {
			t.Fatalf("start generation 1: %v", err)
		}
		closeHolderFleetRegistry(t, old)
		if oldAuthority.closeCalls != 1 || oldAuthority.waitCalls != 1 {
			t.Fatalf(
				"old authority settlement = close %d wait %d, want 1/1",
				oldAuthority.closeCalls, oldAuthority.waitCalls,
			)
		}

		recorded := &holderFleetRecordingStore{Store: store}
		next := newHolderFleetRegistry(
			t, recorded,
			SourceAuthorityFleet{Owner: "product", Generation: 2},
			func(context.Context, sourceauthority.Config) (managedAuthority, error) {
				t.Fatal("empty replacement fleet started a runtime")
				return nil, nil
			},
		)
		if err := next.start(t.Context(), nil); err != nil {
			t.Fatalf("start empty generation 2: %v", err)
		}
		closeHolderFleetRegistry(t, next)
		if events := recorded.snapshot(); len(events) != 1 || events[0] != "retire:alpha" {
			t.Fatalf("removal catalog events = %v, want [retire:alpha] after prior terminal close", events)
		}
		status, err := store.SourceAuthorityFleetHead(t.Context(), "product")
		if err != nil {
			t.Fatalf("SourceAuthorityFleetHead: %v", err)
		}
		if status.Pending != nil || status.Current == nil ||
			status.Current.Generation != 2 || status.Current.AuthorityCount != 0 {
			t.Fatalf("retired fleet status = %+v", status)
		}
	})

	t.Run("desired tenant dependency blocks retirement", func(t *testing.T) {
		store := openHolderFleetCatalog(t)
		old := newHolderFleetRegistry(
			t, store,
			SourceAuthorityFleet{
				Owner: "product", Generation: 1,
				Authorities: []SourceAuthoritySpec{testSourceAuthoritySpec("alpha")},
			},
			func(context.Context, sourceauthority.Config) (managedAuthority, error) {
				return newTestAuthority(), nil
			},
		)
		if err := old.start(t.Context(), nil); err != nil {
			t.Fatalf("start generation 1: %v", err)
		}
		closeHolderFleetRegistry(t, old)
		provision := catalog.TenantProvision{
			OwnerID: "owner", Tenant: "acct-18",
			PresentationRoot: filepath.Join(t.TempDir(), "presentation"),
			BackingRoot:      filepath.Join(t.TempDir(), "backing"),
			ContentSourceID:  "alpha",
			Access:           tenant.ReadWrite,
			CasePolicy:       catalog.CaseSensitive,
			Presentations:    catalog.PresentMount,
			Generation:       1,
		}
		if _, err := store.ProvisionTenant(t.Context(), provision); err != nil {
			t.Fatalf("ProvisionTenant: %v", err)
		}

		next := newHolderFleetRegistry(
			t, store,
			SourceAuthorityFleet{Owner: "product", Generation: 2},
			func(context.Context, sourceauthority.Config) (managedAuthority, error) {
				t.Fatal("blocked replacement fleet started a runtime")
				return nil, nil
			},
		)
		if err := next.start(t.Context(), nil); !errors.Is(err, catalog.ErrMutationConflict) {
			t.Fatalf("start generation 2 with dependency = %v, want ErrMutationConflict", err)
		}
		closeHolderFleetRegistry(t, next)
		status, err := store.SourceAuthorityFleetHead(t.Context(), "product")
		if err != nil {
			t.Fatalf("SourceAuthorityFleetHead: %v", err)
		}
		if status.Current == nil || status.Current.Generation != 1 ||
			status.Pending == nil || status.Pending.Generation != 2 {
			t.Fatalf("blocked retirement status = %+v", status)
		}
	})
}

func TestAuthorityRegistryRejectsSameGenerationResurrectionAndAllowsHigherGeneration(t *testing.T) {
	store := openHolderFleetCatalog(t)
	empty := newHolderFleetRegistry(
		t, store,
		SourceAuthorityFleet{Owner: "product", Generation: 1},
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			t.Fatal("empty fleet started a runtime")
			return nil, nil
		},
	)
	if err := empty.start(t.Context(), nil); err != nil {
		t.Fatalf("start empty generation 1: %v", err)
	}
	closeHolderFleetRegistry(t, empty)

	var factoryCalls int
	resurrection := SourceAuthorityFleet{
		Owner: "product", Generation: 1,
		Authorities: []SourceAuthoritySpec{testSourceAuthoritySpec("alpha")},
	}
	same := newHolderFleetRegistry(
		t, store, resurrection,
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			factoryCalls++
			return newTestAuthority(), nil
		},
	)
	if err := same.start(t.Context(), nil); !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("same-generation resurrection = %v, want ErrMutationConflict", err)
	}
	closeHolderFleetRegistry(t, same)
	if factoryCalls != 0 {
		t.Fatalf("same-generation factory calls = %d, want 0", factoryCalls)
	}

	higher := newHolderFleetRegistry(
		t, store,
		SourceAuthorityFleet{
			Owner: "product", Generation: 2,
			Authorities: []SourceAuthoritySpec{testSourceAuthoritySpec("alpha")},
		},
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			factoryCalls++
			return newTestAuthority(), nil
		},
	)
	if err := higher.start(t.Context(), nil); err != nil {
		t.Fatalf("higher-generation reintroduction: %v", err)
	}
	closeHolderFleetRegistry(t, higher)
	if factoryCalls != 1 {
		t.Fatalf("higher-generation factory calls = %d, want 1", factoryCalls)
	}
	status, err := store.SourceAuthorityFleetHead(t.Context(), "product")
	if err != nil {
		t.Fatalf("SourceAuthorityFleetHead: %v", err)
	}
	if status.Pending != nil || status.Current == nil ||
		status.Current.Generation != 2 || status.Current.AuthorityCount != 1 {
		t.Fatalf("reintroduced fleet status = %+v", status)
	}
}

func TestAuthorityRegistryResumesPendingFleetAndAcknowledgementLoss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	first, err := catalog.Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open(first): %v", err)
	}
	empty := newHolderFleetRegistry(
		t, first,
		SourceAuthorityFleet{Owner: "product", Generation: 1},
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			t.Fatal("empty fleet started a runtime")
			return nil, nil
		},
	)
	if err := empty.start(t.Context(), nil); err != nil {
		t.Fatalf("start empty generation 1: %v", err)
	}
	closeHolderFleetRegistry(t, empty)

	specs := []PhysicalSourceSpec{
		testSourceAuthoritySpec("alpha"),
		testSourceAuthoritySpec("beta"),
	}
	authorities := []causal.SourceAuthorityID{specs[0].Authority, specs[1].Authority}
	declarations := []catalog.SourceAuthorityDeclaration{
		{
			Authority: specs[0].Authority, DriverID: specs[0].DriverID,
			DriverConfig: specs[0].DriverConfig, DeclarationDigest: specs[0].DeclarationDigest,
		},
		{
			Authority: specs[1].Authority, DriverID: specs[1].DriverID,
			DriverConfig: specs[1].DriverConfig, DeclarationDigest: specs[1].DeclarationDigest,
		},
	}
	digest, err := catalog.SourceAuthorityFleetDigest(authorities)
	if err != nil {
		t.Fatalf("SourceAuthorityFleetDigest: %v", err)
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatalf("SourceAuthorityFleetDeclarationsDigest: %v", err)
	}
	pending, err := first.ReconcileSourceAuthorityFleet(
		t.Context(),
		catalog.SourceAuthorityFleetReconcileRequest{
			Owner: "product", ExpectedGeneration: 1, Generation: 2,
			Sequence: 0, Declarations: declarations[:1],
			AuthorityCount: 2, AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
		},
	)
	if err != nil {
		t.Fatalf("stage first fleet page: %v", err)
	}
	if pending.Complete || pending.ReceivedCount != 1 || pending.NextSequence != 1 {
		t.Fatalf("partial pending state = %+v", pending)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	restarted, err := catalog.Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open(restarted): %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	var mu sync.Mutex
	factoryCalls := make(map[causal.SourceAuthorityID]int)
	factory := func(_ context.Context, config sourceauthority.Config) (managedAuthority, error) {
		mu.Lock()
		factoryCalls[config.Authority]++
		mu.Unlock()
		return newTestAuthority(), nil
	}
	fleet := SourceAuthorityFleet{
		Owner: "product", Generation: 2,
		Authorities: []SourceAuthoritySpec{
			testSourceAuthoritySpec("alpha"),
			testSourceAuthoritySpec("beta"),
		},
	}
	resumed := newHolderFleetRegistry(t, restarted, fleet, factory)
	if err := resumed.start(t.Context(), nil); err != nil {
		t.Fatalf("resume pending generation 2: %v", err)
	}
	closeHolderFleetRegistry(t, resumed)

	replayed := newHolderFleetRegistry(t, restarted, fleet, factory)
	if err := replayed.start(t.Context(), nil); err != nil {
		t.Fatalf("replay acknowledged generation 2: %v", err)
	}
	closeHolderFleetRegistry(t, replayed)
	mu.Lock()
	defer mu.Unlock()
	if factoryCalls["alpha"] != 2 || factoryCalls["beta"] != 2 {
		t.Fatalf("factory calls after resume and replay = %+v, want alpha=2 beta=2", factoryCalls)
	}
	status, err := restarted.SourceAuthorityFleetHead(t.Context(), "product")
	if err != nil {
		t.Fatalf("SourceAuthorityFleetHead: %v", err)
	}
	if status.Pending != nil || status.Current == nil ||
		status.Current.Generation != 2 || status.Current.AuthorityCount != 2 {
		t.Fatalf("resumed fleet status = %+v", status)
	}
}

func TestAuthorityRegistryRejectsStaleFleetOwner(t *testing.T) {
	store := openHolderFleetCatalog(t)
	current := newHolderFleetRegistry(
		t, store,
		SourceAuthorityFleet{
			Owner: "product", Generation: 1,
			Authorities: []SourceAuthoritySpec{testSourceAuthoritySpec("alpha")},
		},
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			return newTestAuthority(), nil
		},
	)
	if err := current.start(t.Context(), nil); err != nil {
		t.Fatalf("start current owner: %v", err)
	}
	closeHolderFleetRegistry(t, current)

	stale := newHolderFleetRegistry(
		t, store,
		SourceAuthorityFleet{
			Owner: "foreign", Generation: 1,
			Authorities: []SourceAuthoritySpec{testSourceAuthoritySpec("alpha")},
		},
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			t.Fatal("stale owner started a runtime")
			return nil, nil
		},
	)
	if err := stale.start(t.Context(), nil); !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("start stale owner = %v, want ErrMutationConflict", err)
	}
	closeHolderFleetRegistry(t, stale)
	if _, err := store.SourceAuthorityFleetHead(t.Context(), "foreign"); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("foreign fleet state = %v, want ErrNotFound", err)
	}
}

func TestAuthorityRegistryRejectsMalformedFleetAbortReceipt(t *testing.T) {
	store := openHolderFleetCatalog(t)
	spec := testSourceAuthoritySpec("source")
	process := testSourceRuntimeProcess()
	seedSourceAuthorityOpenRuntimeForTest(t, store, spec, process, [16]byte{1})
	pendingSpec := testSourceAuthoritySpec("source-pending")
	declarations := []catalog.SourceAuthorityDeclaration{{
		Authority: spec.Authority, DriverID: spec.DriverID,
		DriverConfig: spec.DriverConfig, DeclarationDigest: spec.DeclarationDigest,
	}, {
		Authority: pendingSpec.Authority, DriverID: pendingSpec.DriverID,
		DriverConfig: pendingSpec.DriverConfig, DeclarationDigest: pendingSpec.DeclarationDigest,
	}}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest(
		[]causal.SourceAuthorityID{spec.Authority, pendingSpec.Authority},
	)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileSourceAuthorityFleet(
		t.Context(), catalog.SourceAuthorityFleetReconcileRequest{
			Owner: "holder-test", ExpectedGeneration: 1, Generation: 2,
			Declarations: declarations[:1], Complete: false, AuthorityCount: 2,
			AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
		},
	); err != nil {
		t.Fatalf("stage pending fleet: %v", err)
	}

	registry := newHolderFleetRegistry(
		t,
		&malformedFleetAbortStore{Store: store},
		testSourceAuthorityFleet(spec),
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			return newTestAuthority(), nil
		},
	)
	if err := registry.start(t.Context(), nil); !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("start with malformed abort receipt = %v, want mutation conflict", err)
	}
	closeHolderFleetRegistry(t, registry)
}

func openHolderFleetCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatalf("Open catalog: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func newHolderFleetRegistry(
	t *testing.T,
	store sourceauthority.Store,
	fleet SourceAuthorityFleet,
	factory authorityRuntimeFactory,
) *authorityRegistry {
	t.Helper()
	registry, err := newAuthorityRegistry(
		store, fleet, factory,
		func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
			return testAuthorityExecutor{}, nil
		},
		nil, testSourceRuntimeProcess(), time.Second,
	)
	if err != nil {
		t.Fatalf("newAuthorityRegistry: %v", err)
	}
	return registry
}

func closeHolderFleetRegistry(t *testing.T, registry *authorityRegistry) {
	t.Helper()
	if err := registry.Close(context.Background()); err != nil &&
		!errors.Is(err, sourceauthority.ErrClosed) {
		t.Fatalf("close authority registry: %v", err)
	}
}

type holderFleetRecordingStore struct {
	sourceauthority.Store

	mu     sync.Mutex
	events []string
}

type malformedFleetAbortStore struct{ sourceauthority.Store }

func (s *malformedFleetAbortStore) AbortSourceAuthorityFleet(
	ctx context.Context,
	request catalog.SourceAuthorityFleetAbortRequest,
) (catalog.SourceAuthorityFleetAbortReceipt, error) {
	receipt, err := s.Store.AbortSourceAuthorityFleet(ctx, request)
	receipt.Owner = "wrong-owner"
	return receipt, err
}

func (s *holderFleetRecordingStore) CloseSourceAuthorityRuntime(
	ctx context.Context,
	fence catalog.SourceAuthorityRuntimeFence,
) error {
	s.mu.Lock()
	s.events = append(s.events, "close:"+string(fence.Authority))
	s.mu.Unlock()
	return s.Store.CloseSourceAuthorityRuntime(ctx, fence)
}

func (s *holderFleetRecordingStore) RetireSourceAuthority(
	ctx context.Context,
	request catalog.SourceAuthorityRetireRequest,
) (catalog.SourceAuthorityRetirementReceipt, error) {
	s.mu.Lock()
	s.events = append(s.events, "retire:"+string(request.Authority))
	s.mu.Unlock()
	return s.Store.RetireSourceAuthority(ctx, request)
}

func (s *holderFleetRecordingStore) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}
