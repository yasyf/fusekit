package holder

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/tenant"
)

func TestAuthorityRegistryRejectsDuplicateAndMissingDeclarations(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	factory := func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		t.Fatal("authority factory called for invalid fleet")
		return nil, nil
	}
	executors := func(SourceAuthoritySpec) (sourceauthority.Executor, error) { return testAuthorityExecutor{}, nil }
	alpha := testSourceAuthoritySpec("alpha")
	duplicateAuthority := testSourceAuthoritySpec("alpha")
	if _, err := newAuthorityRegistry(
		store, testSourceAuthorityFleet(alpha, duplicateAuthority), factory, executors,
		nil, testSourceRuntimeProcess(), time.Second,
	); err == nil {
		t.Fatal("duplicate authority identity was accepted")
	}
	registry, err := newAuthorityRegistry(
		store, testSourceAuthorityFleet(alpha), factory, executors,
		nil, testSourceRuntimeProcess(), time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.start(t.Context(), []tenant.TenantSpec{testAuthorityTenant("acct-18", "missing", 1)}); err == nil {
		t.Fatal("durable tenant with missing authority declaration was accepted")
	}
	registry.Cancel()
}

func TestAuthorityRegistryConstructsSemanticAuthorityWithoutPhysicalPolicyOrExecutor(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	spec := SemanticDriverSpec{
		Authority: "semantic", DeclarationDigest: sha256.Sum256([]byte("semantic declaration")),
		DriverID: "product-driver",
	}
	semantic := newTestAuthority()
	var semanticCalls int
	registry, err := newAuthorityRegistry(
		store, testSourceAuthorityFleet(spec),
		nil,
		nil,
		func(_ context.Context, got SemanticDriverSpec, tenants []tenant.TenantSpec) (managedAuthority, error) {
			semanticCalls++
			if !reflect.DeepEqual(got, spec) || len(tenants) != 1 || tenants[0].ID != "acct-18" {
				t.Fatalf("semantic construction = %+v, %+v", got, tenants)
			}
			return semantic, nil
		},
		testSourceRuntimeProcess(), time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.start(t.Context(), []tenant.TenantSpec{
		testAuthorityTenant("acct-18", "semantic", 1),
	}); err != nil {
		t.Fatal(err)
	}
	if semanticCalls != 1 {
		t.Fatalf("semantic construction calls = %d, want 1", semanticCalls)
	}
	if err := registry.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityRegistryRecoversOnlyCatalogDiscoveredSemanticReceiptOwners(t *testing.T) {
	runtime := newTestAuthority()
	store := &semanticReceiptAuthorityStore{pages: []catalog.SourceDriverReceiptAuthorityPage{
		{Authorities: []causal.SourceAuthorityID{"semantic"}}, {},
	}}
	registry := &authorityRegistry{
		catalog: store, started: true,
		bySource: map[string]*authorityDeclaration{
			"semantic": {
				spec:    SemanticDriverSpec{Authority: "semantic", DriverID: "driver"},
				runtime: runtime,
			},
		},
	}
	if err := registry.recoverSemanticReceipts(t.Context()); err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	recoveries := runtime.recoveries
	runtime.mu.Unlock()
	if recoveries != 1 || store.calls != 2 {
		t.Fatalf("receipt recovery = runtime %d pages %d, want 1 and 2", recoveries, store.calls)
	}

	unknown := &semanticReceiptAuthorityStore{pages: []catalog.SourceDriverReceiptAuthorityPage{{
		Authorities: []causal.SourceAuthorityID{"removed"},
	}}}
	registry.catalog = unknown
	if err := registry.recoverSemanticReceipts(t.Context()); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("removed authority receipt recovery = %v, want integrity", err)
	}
}

func TestAuthorityRegistryRecoversCompleteGroupedFleet(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var mu sync.Mutex
	started := make(map[causal.SourceAuthorityID][]tenant.TenantSpec)
	runtimes := make(map[causal.SourceAuthorityID]*testAuthority)
	factory := func(_ context.Context, config sourceauthority.Config) (managedAuthority, error) {
		runtime := newTestAuthority()
		mu.Lock()
		started[config.Authority] = append([]tenant.TenantSpec(nil), config.Tenants...)
		runtimes[config.Authority] = runtime
		mu.Unlock()
		return runtime, nil
	}
	registry, err := newAuthorityRegistry(store, testSourceAuthorityFleet(
		testSourceAuthoritySpec("beta"),
		testSourceAuthoritySpec("alpha"),
	), factory, func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}, nil, testSourceRuntimeProcess(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fleet := []tenant.TenantSpec{
		testAuthorityTenant("acct-20", "alpha", 1),
		testAuthorityTenant("acct-18", "alpha", 2),
	}
	if err := registry.start(t.Context(), fleet); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := registry.Close(context.Background()); err != nil {
			t.Errorf("close authority registry: %v", err)
		}
	})
	mu.Lock()
	defer mu.Unlock()
	alpha := started["alpha"]
	if len(alpha) != 2 || alpha[0].ID != "acct-18" || alpha[1].ID != "acct-20" {
		t.Fatalf("alpha recovered fleet = %#v", alpha)
	}
	if beta := started["beta"]; len(beta) != 0 {
		t.Fatalf("unused authority recovered fleet = %#v, want empty", beta)
	}
	if len(runtimes) != 2 {
		t.Fatalf("started authorities = %d, want 2", len(runtimes))
	}
}

func TestAuthorityRegistryCancellationCannotLeakAStartingRuntime(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	entered := make(chan struct{})
	release := make(chan struct{})
	authority := newTestAuthority()
	registry, err := newAuthorityRegistry(store, testSourceAuthorityFleet(
		testSourceAuthoritySpec("source"),
	), func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		close(entered)
		<-release
		return authority, nil
	}, func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}, nil, testSourceRuntimeProcess(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- registry.start(context.Background(), nil) }()
	<-entered
	registry.Cancel()
	close(release)
	if err := <-started; !errors.Is(err, sourceauthority.ErrClosed) {
		t.Fatalf("start after cancellation = %v, want closed", err)
	}
	select {
	case <-authority.done:
	default:
		t.Fatal("runtime returned after cancellation was not canceled and joined")
	}
}

func TestAuthorityReconfigurationSurvivesCallerCancellationAfterProvision(t *testing.T) {
	dir := shortTempDir(t)
	store, err := catalog.Open(t.Context(), filepath.Join(dir, "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	authority := newTestAuthority()
	authority.reconfigureStarted = make(chan struct{})
	authority.reconfigureRelease = make(chan struct{})
	authority.reconfigureCanceled = make(chan struct{})
	registry, err := newAuthorityRegistry(store, testSourceAuthorityFleet(
		testSourceAuthoritySpec("source"),
	), func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		return authority, nil
	}, func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}, nil, testSourceRuntimeProcess(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	runtime, err := tenant.NewRuntime(t.Context(), store, testPlanner{}, registry, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := closeTenantRuntime(runtime); err != nil {
			t.Errorf("close tenant runtime: %v", err)
		}
		_ = store.Close()
	})
	t.Cleanup(func() {
		if err := registry.Close(context.Background()); err != nil {
			t.Errorf("close authority registry: %v", err)
		}
	})
	controller := authorityTenantController{tenants: runtime, authorities: registry}
	requestContext, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- controller.ProvisionTenant(requestContext, testAuthorityTenant("acct-18", "source", 1))
	}()
	<-authority.reconfigureStarted
	cancel()
	close(authority.reconfigureRelease)
	if err := <-done; err != nil {
		t.Fatalf("provision after lost caller = %v", err)
	}
	select {
	case <-authority.reconfigureCanceled:
		t.Fatal("durable authority reconfiguration inherited the lost request context")
	default:
	}
	specs := runtime.Specs()
	if len(specs) != 1 || specs[0].ID != "acct-18" {
		t.Fatalf("durable fleet after lost caller = %#v", specs)
	}
	configured := authority.configurations()
	if len(configured) != 1 || len(configured[0]) != 1 || configured[0][0].ID != "acct-18" {
		t.Fatalf("authority fleet after lost caller = %#v", configured)
	}
}

func TestAuthorityPrepareRollsBackEveryAttemptedRuntime(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	alpha := newTestAuthority()
	beta := newTestAuthority()
	beta.reconfigureErr = func(call int, _ []tenant.TenantSpec) error {
		if call == 1 {
			return errors.New("injected uncertain reconfigure")
		}
		return nil
	}
	runtimes := map[causal.SourceAuthorityID]*testAuthority{"alpha": alpha, "beta": beta}
	registry, err := newAuthorityRegistry(store, testSourceAuthorityFleet(
		testSourceAuthoritySpec("alpha"), testSourceAuthoritySpec("beta"),
	), func(_ context.Context, config sourceauthority.Config) (managedAuthority, error) {
		return runtimes[config.Authority], nil
	}, func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}, nil, testSourceRuntimeProcess(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	before := []tenant.TenantSpec{
		testAuthorityTenant("acct-18", "alpha", 1),
		testAuthorityTenant("acct-20", "beta", 1),
	}
	if err := registry.start(t.Context(), before); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := registry.Close(context.Background()); err != nil {
			t.Errorf("close authority registry: %v", err)
		}
	})
	err = registry.Prepare(t.Context(), tenant.FleetTransition{Before: before})
	if err == nil || !strings.Contains(err.Error(), "injected uncertain reconfigure") {
		t.Fatalf("Prepare = %v, want injected failure", err)
	}
	for name, authority := range runtimes {
		configured := authority.configurations()
		if len(configured) != 2 {
			t.Fatalf("%s configurations = %#v, want attempted target and rollback", name, configured)
		}
		if len(configured[0]) != 0 {
			t.Fatalf("%s attempted fleet = %#v, want drained", name, configured[0])
		}
		if len(configured[1]) != 1 || configured[1][0].Content.ID != string(name) {
			t.Fatalf("%s rollback fleet = %#v, want exact before fleet", name, configured[1])
		}
	}
}

func TestAuthorityPrepareCancellationRollsBackUnderRegistryLifecycle(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	authority := newTestAuthority()
	authority.reconfigureStarted = make(chan struct{})
	authority.reconfigureRelease = make(chan struct{})
	authority.reconfigureIgnoresContext = true
	before := []tenant.TenantSpec{testAuthorityTenant("acct-18", "source", 1)}
	registry := newStartedTestAuthorityRegistry(t, store, authority, before)
	request, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- registry.Prepare(request, tenant.FleetTransition{Before: before, Drained: nil})
	}()
	<-authority.reconfigureStarted
	cancel()
	close(authority.reconfigureRelease)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Prepare after caller cancellation = %v, want cancellation", err)
	}
	configured := authority.configurations()
	if len(configured) != 2 || len(configured[0]) != 0 || !slices.Equal(configured[1], before) {
		t.Fatalf("Prepare cancellation rollback = %#v, want drained attempt then exact before fleet", configured)
	}
	select {
	case <-authority.done:
		t.Fatal("successful cancellation rollback failed the registry closed")
	default:
	}
}

func TestAuthorityPartialMultiAuthorityPrepareCancellationRestoresExactBefore(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	alpha := newTestAuthority()
	beta := newTestAuthority()
	beta.reconfigureStarted = make(chan struct{})
	beta.reconfigureRelease = make(chan struct{})
	beta.reconfigureCanceled = make(chan struct{})
	runtimes := map[causal.SourceAuthorityID]*testAuthority{"alpha": alpha, "beta": beta}
	registry, err := newAuthorityRegistry(store, testSourceAuthorityFleet(
		testSourceAuthoritySpec("alpha"), testSourceAuthoritySpec("beta"),
	), func(_ context.Context, config sourceauthority.Config) (managedAuthority, error) {
		return runtimes[config.Authority], nil
	}, func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}, nil, testSourceRuntimeProcess(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	before := []tenant.TenantSpec{
		testAuthorityTenant("acct-18", "alpha", 1),
		testAuthorityTenant("acct-20", "beta", 1),
	}
	if err := registry.start(t.Context(), before); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := registry.Close(context.Background()); err != nil {
			t.Errorf("close authority registry: %v", err)
		}
	})
	request, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- registry.Prepare(request, tenant.FleetTransition{Before: before, Drained: nil})
	}()
	<-beta.reconfigureStarted
	cancel()
	<-beta.reconfigureCanceled
	close(beta.reconfigureRelease)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("partial Prepare after cancellation = %v, want cancellation", err)
	}
	for source, runtime := range runtimes {
		configured := runtime.configurations()
		if source == "alpha" {
			if len(configured) != 2 || len(configured[0]) != 0 || len(configured[1]) != 1 || configured[1][0].Content.ID != "alpha" {
				t.Fatalf("alpha rollback = %#v", configured)
			}
		} else if len(configured) != 1 || len(configured[0]) != 1 || configured[0][0].Content.ID != "beta" {
			t.Fatalf("beta rollback = %#v", configured)
		}
	}
	select {
	case <-alpha.done:
		t.Fatal("partial cancellation rollback failed the registry closed")
	default:
	}
}

func TestAuthorityCommitAndAbortIgnoreCanceledCallerAfterDurability(t *testing.T) {
	for _, operation := range []string{"commit", "abort"} {
		t.Run(operation, func(t *testing.T) {
			store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			authority := newTestAuthority()
			before := []tenant.TenantSpec{testAuthorityTenant("acct-18", "source", 1)}
			committed := []tenant.TenantSpec{testAuthorityTenant("acct-18", "source", 2)}
			registry := newStartedTestAuthorityRegistry(t, store, authority, before)
			transition := tenant.FleetTransition{Before: before, Drained: nil, Committed: committed}
			if err := registry.Prepare(t.Context(), transition); err != nil {
				t.Fatal(err)
			}
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			var settleErr error
			var target []tenant.TenantSpec
			if operation == "commit" {
				settleErr = registry.Commit(canceled, transition)
				target = committed
			} else {
				settleErr = registry.Abort(canceled, transition)
				target = before
			}
			if settleErr != nil {
				t.Fatalf("%s with canceled caller: %v", operation, settleErr)
			}
			configured := authority.configurations()
			if len(configured) != 2 || !slices.Equal(configured[1], target) {
				t.Fatalf("%s settled fleet = %#v, want %#v", operation, configured, target)
			}
		})
	}
}

func TestAuthorityFleetTransientFailureUsesLifecycleBackoffAndRecovers(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	authority := newTestAuthority()
	authority.reconfigureErr = func(call int, _ []tenant.TenantSpec) error {
		if call == 1 {
			return errors.New("temporary observer outage")
		}
		return nil
	}
	registry := newStartedTestAuthorityRegistry(t, store, authority, nil)
	clock := newTestAuthorityClock()
	registry.retryClock = clock
	done := make(chan error, 1)
	go func() {
		done <- registry.Commit(t.Context(), tenant.FleetTransition{
			Committed: []tenant.TenantSpec{testAuthorityTenant("acct-18", "source", 1)},
		})
	}()
	if delay := <-clock.delays; delay != authorityInitialRetryDelay {
		t.Fatalf("retry delay = %v, want %v", delay, authorityInitialRetryDelay)
	}
	clock.tick <- time.Now()
	if err := <-done; err != nil {
		t.Fatalf("Commit after transient failure: %v", err)
	}
	if configured := authority.configurations(); len(configured) != 2 {
		t.Fatalf("reconfigurations = %#v, want one retry", configured)
	}
}

func TestAuthorityFleetTerminalCommitAndAbortFailClosed(t *testing.T) {
	for _, operation := range []string{"commit", "abort"} {
		t.Run(operation, func(t *testing.T) {
			store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			authority := newTestAuthority()
			before := []tenant.TenantSpec{testAuthorityTenant("acct-18", "source", 1)}
			registry := newStartedTestAuthorityRegistry(t, store, authority, before)
			transition := tenant.FleetTransition{
				Before: before, Drained: nil,
				Committed: []tenant.TenantSpec{testAuthorityTenant("acct-18", "source", 2)},
			}
			if err := registry.Prepare(t.Context(), transition); err != nil {
				t.Fatal(err)
			}
			authority.reconfigureErr = func(int, []tenant.TenantSpec) error {
				return sourceauthority.ErrInvalidPlan
			}
			if operation == "commit" {
				err = registry.Commit(t.Context(), transition)
			} else {
				err = registry.Abort(t.Context(), transition)
			}
			if !errors.Is(err, sourceauthority.ErrInvalidPlan) {
				t.Fatalf("%s = %v, want invalid plan", operation, err)
			}
			select {
			case <-authority.done:
			default:
				t.Fatalf("%s terminal failure did not cancel and join authority", operation)
			}
		})
	}
}

func TestAuthorityFleetShutdownInterruptsBackoffAndJoins(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	authority := newTestAuthority()
	authority.reconfigureErr = func(int, []tenant.TenantSpec) error {
		return errors.New("temporary observer outage")
	}
	registry := newStartedTestAuthorityRegistry(t, store, authority, nil)
	clock := newTestAuthorityClock()
	registry.retryClock = clock
	done := make(chan error, 1)
	go func() {
		done <- registry.Commit(context.Background(), tenant.FleetTransition{
			Committed: []tenant.TenantSpec{testAuthorityTenant("acct-18", "source", 1)},
		})
	}()
	<-clock.delays
	registry.Cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Commit after shutdown = %v, want cancellation", err)
	}
	select {
	case <-authority.done:
	default:
		t.Fatal("shutdown did not join authority")
	}
}

func TestAuthorityRegistryRoutesCommittedSourceMutationExactlyOnce(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	authority := newTestAuthority()
	registry := newStartedTestAuthorityRegistry(t, store, authority, nil)
	commit := tenant.SourceMutationCommit{OperationID: catalog.MutationID{1}, SourceID: "source"}
	if err := registry.SourceMutationCommitted(t.Context(), commit); err != nil {
		t.Fatal(err)
	}
	if err := registry.SourceMutationCommitted(t.Context(), tenant.SourceMutationCommit{
		OperationID: catalog.MutationID{2}, SourceID: "unknown",
	}); err == nil {
		t.Fatal("unknown source mutation commit was accepted")
	}
	if commits := authority.committedMutations(); !slices.Equal(commits, []tenant.SourceMutationCommit{commit}) {
		t.Fatalf("committed mutations = %#v", commits)
	}
}

func TestAuthorityRegistryStartErrorSettlesReturnedAuthorityExactly(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	startFailure := errors.New("authority factory failed after publication")
	waitFailure := errors.New("authority terminal failure")
	authority := newTestAuthority()
	authority.waitEntered = make(chan struct{})
	authority.waitRelease = make(chan struct{})
	authority.waitErr = waitFailure
	registry, err := newAuthorityRegistry(store, testSourceAuthorityFleet(
		testSourceAuthoritySpec("source"),
	), func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		return authority, startFailure
	}, func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}, nil, testSourceRuntimeProcess(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- registry.start(t.Context(), nil) }()
	<-authority.waitEntered
	select {
	case err := <-result:
		t.Fatalf("start returned before unpublished authority settled: %v", err)
	default:
	}
	close(authority.waitRelease)
	if err := <-result; !errors.Is(err, startFailure) || !errors.Is(err, waitFailure) {
		t.Fatalf("start error = %v, want factory and terminal failures", err)
	}
	if err := registry.Close(context.Background()); !errors.Is(err, waitFailure) {
		t.Fatalf("Close error = %v, want cached unpublished authority terminal failure", err)
	}
	authority.mu.Lock()
	waitCalls := authority.waitCalls
	authority.mu.Unlock()
	if waitCalls != 1 {
		t.Fatalf("authority Wait calls = %d, want 1", waitCalls)
	}
}

func TestAuthorityRegistryExecutorErrorClosesReturnedExecutorExactly(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	factoryFailure := errors.New("executor factory failed after publication")
	closeFailure := errors.New("executor close failure")
	executor := &closingTestAuthorityExecutor{closeErr: closeFailure}
	registry, err := newAuthorityRegistry(store, testSourceAuthorityFleet(
		testSourceAuthoritySpec("source"),
	), func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		t.Fatal("authority factory called after executor factory failure")
		return nil, nil
	}, func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return executor, factoryFailure
	}, nil, testSourceRuntimeProcess(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.start(t.Context(), nil); !errors.Is(err, factoryFailure) || !errors.Is(err, closeFailure) {
		t.Fatalf("start error = %v, want executor factory and close failures", err)
	}
	if err := registry.Close(context.Background()); !errors.Is(err, closeFailure) {
		t.Fatalf("Close error = %v, want cached executor close failure", err)
	}
	executor.mu.Lock()
	closes := executor.closes
	executor.mu.Unlock()
	if closes != 1 {
		t.Fatalf("executor closes = %d, want 1", closes)
	}
}

func TestAuthorityRegistryCloseJoinsOnceAndReplaysTerminalResult(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(shortTempDir(t), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	closeFailure := errors.New("close authority")
	waitFailure := errors.New("wait authority")
	authority := newTestAuthority()
	authority.closeEntered = make(chan struct{})
	authority.closeRelease = make(chan struct{})
	authority.closeErr = closeFailure
	authority.waitErr = waitFailure
	registry, err := newAuthorityRegistry(store, testSourceAuthorityFleet(
		testSourceAuthoritySpec("source"),
	), func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		return authority, nil
	}, func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}, nil, testSourceRuntimeProcess(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	closeCtx, cancelClose := context.WithCancel(t.Context())
	closeResult := make(chan error, 1)
	waitResult := make(chan error, 1)
	go func() { closeResult <- registry.Close(closeCtx) }()
	go func() { waitResult <- registry.Wait(context.Background()) }()
	<-authority.closeEntered
	cancelClose()
	select {
	case err := <-closeResult:
		t.Fatalf("Close returned before authority settled: %v", err)
	default:
	}
	select {
	case err := <-waitResult:
		t.Fatalf("Wait returned before authority settled: %v", err)
	default:
	}
	close(authority.closeRelease)
	if err := <-closeResult; !errors.Is(err, context.Canceled) ||
		!errors.Is(err, closeFailure) || !errors.Is(err, waitFailure) {
		t.Fatalf("Close error = %v, want caller cancellation and terminal failures", err)
	}
	if err := <-waitResult; errors.Is(err, context.Canceled) ||
		!errors.Is(err, closeFailure) || !errors.Is(err, waitFailure) {
		t.Fatalf("Wait error = %v, want only terminal failures", err)
	}
	for _, err := range []error{
		registry.Close(context.Background()),
		registry.Wait(context.Background()),
	} {
		if !errors.Is(err, closeFailure) || !errors.Is(err, waitFailure) {
			t.Fatalf("replayed terminal error = %v, want both terminal failures", err)
		}
	}
	authority.mu.Lock()
	closeCalls, waitCalls := authority.closeCalls, authority.waitCalls
	authority.mu.Unlock()
	if closeCalls != 1 || waitCalls != 1 {
		t.Fatalf("authority settlements = Close %d, Wait %d; want exactly one each", closeCalls, waitCalls)
	}
}

func newStartedTestAuthorityRegistry(
	t *testing.T,
	store *catalog.Catalog,
	authority *testAuthority,
	fleet []tenant.TenantSpec,
) *authorityRegistry {
	t.Helper()
	registry, err := newAuthorityRegistry(store, testSourceAuthorityFleet(
		testSourceAuthoritySpec("source"),
	), func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		return authority, nil
	}, func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}, nil, testSourceRuntimeProcess(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.start(t.Context(), fleet); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := registry.Close(context.Background()); err != nil && !errors.Is(err, sourceauthority.ErrClosed) {
			t.Errorf("close authority registry: %v", err)
		}
	})
	return registry
}

type testAuthorityClock struct {
	delays chan time.Duration
	tick   chan time.Time
}

func newTestAuthorityClock() *testAuthorityClock {
	return &testAuthorityClock{delays: make(chan time.Duration, 4), tick: make(chan time.Time, 4)}
}

func (c *testAuthorityClock) After(delay time.Duration) <-chan time.Time {
	c.delays <- delay
	return c.tick
}

func TestHolderClosesAuthorityBeforeCatalogState(t *testing.T) {
	dir := shortTempDir(t)
	config := testConfig(dir, "v1.0.0", newTestNative(nil))
	config.planner = nil
	config.catalogService = nil
	configureTestSourceFleet(&config, testSourceAuthoritySpec("source"))
	config.CatalogAuthorizer = testCatalogAuthorizer{}
	config.authorityExecutors = func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}
	var closeCatalogErr error
	var closeCalled bool
	config.authorityFactory = func(_ context.Context, authorityConfig sourceauthority.Config) (managedAuthority, error) {
		authority := newTestAuthority()
		authority.onClose = func() {
			closeCalled = true
			_, closeCatalogErr = authorityConfig.Store.SourceAuthorityFleetHead(
				context.Background(), authorityConfig.FleetOwner,
			)
		}
		return authority, nil
	}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	graph, published := runtime.graphs.Load()
	if !published {
		t.Fatal("runtime graph was not published")
	}
	closeRuntime(t, runtime, done)
	if !closeCalled {
		t.Fatal("authority close was not called")
	}
	if closeCatalogErr != nil {
		t.Fatalf("catalog closed before authority: %v", closeCatalogErr)
	}
	if _, err := graph.catalog.TopologyHead(t.Context(), config.Owner); err == nil {
		t.Fatal("catalog remained open after holder shutdown")
	}
}

func TestHolderShutdownDeadlineKeepsCatalogAliveUntilAuthoritySettles(t *testing.T) {
	dir := shortTempDir(t)
	config := testConfig(dir, "v1.0.0", newTestNative(nil))
	config.ShutdownTimeout = 10 * time.Millisecond
	configureTestSourceFleet(&config, testSourceAuthoritySpec("source"))
	config.authorityExecutors = func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}
	authority := newTestAuthority()
	authority.closeEntered = make(chan struct{})
	authority.closeRelease = make(chan struct{})
	var graph *runtimeGraph
	var closeCatalogErr error
	authoritySettled := make(chan struct{})
	authority.onClose = func() {
		defer close(authoritySettled)
		if graph == nil {
			closeCatalogErr = errors.New("holder runtime graph unavailable during authority close")
			return
		}
		_, closeCatalogErr = graph.catalog.TopologyHead(context.Background(), config.Owner)
	}
	config.authorityFactory = func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		return authority, nil
	}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	graph, published := runtime.graphs.Load()
	if !published {
		t.Fatal("runtime graph was not published")
	}
	closed := make(chan error, 1)
	go func() { closed <- runtime.Close(context.Background()) }()
	<-authority.closeEntered
	if err := <-closed; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want shutdown deadline", err)
	}
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want shutdown deadline", err)
	}
	if _, err := graph.catalog.TopologyHead(t.Context(), config.Owner); err != nil {
		t.Fatalf("catalog closed while authority remained unsettled: %v", err)
	}
	close(authority.closeRelease)
	<-authoritySettled
	if closeCatalogErr != nil {
		t.Fatalf("catalog closed before deadline-canceled authority settled: %v", closeCatalogErr)
	}
	settled, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := graph.workers.Wait(settled); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait for exact deadline-canceled shutdown: %v", err)
	}
	if err := settled.Err(); err != nil {
		t.Fatalf("exact deadline-canceled shutdown remained unsettled: %v", err)
	}
	if _, err := graph.catalog.TopologyHead(t.Context(), config.Owner); err == nil {
		t.Fatal("catalog remained open after exact deadline-canceled shutdown")
	}
}

func testSourceAuthoritySpec(authority causal.SourceAuthorityID) PhysicalSourceSpec {
	return PhysicalSourceSpec{
		Authority:         authority,
		DeclarationDigest: sha256.Sum256([]byte("declaration:" + authority)),
		DriverID:          "physical",
		Policy:            testAuthorityPolicy{},
	}
}

func testSourceRuntimeProcess() proc.Record {
	return proc.Record{
		PID: 4242, StartTime: "holder-start", Boot: "holder-boot",
		Comm: "holder", Generation: "holder-generation", RecoveryClass: proc.RecoverySourceOwner,
	}
}

func testSourceAuthorityFleet(specs ...SourceAuthoritySpec) SourceAuthorityFleet {
	return SourceAuthorityFleet{
		Owner: "holder-test", Generation: 1, Authorities: specs,
	}
}

func testAuthorityTenant(id catalog.TenantID, sourceID string, generation catalog.Generation) tenant.TenantSpec {
	return tenant.TenantSpec{
		OwnerID: "owner", ID: id, Mount: tenant.MountSpec{PresentationRoot: filepath.Join("/tmp", "presentation", string(id))},
		Backing: tenant.BackingSpec{Root: filepath.Join("/tmp", "backing", string(id))},
		Content: tenant.ContentSource{ID: sourceID},
		Traits: tenant.TenantTraits{
			Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive,
			Presentations: catalog.PresentMount,
		},
		Generation: generation,
	}
}

type testAuthority struct {
	mu sync.Mutex

	reconfigurations          [][]tenant.TenantSpec
	reconfigureStarted        chan struct{}
	reconfigureRelease        chan struct{}
	reconfigureCanceled       chan struct{}
	reconfigureIgnoresContext bool
	reconfigureErr            func(int, []tenant.TenantSpec) error
	commits                   []tenant.SourceMutationCommit
	recoveries                int
	recoveryErr               error
	startedOnce               sync.Once
	canceledOnce              sync.Once
	done                      chan struct{}
	closeOnce                 sync.Once
	doneOnce                  sync.Once
	onClose                   func()
	closeEntered              chan struct{}
	closeRelease              chan struct{}
	closeErr                  error
	waitErr                   error
	closeCalls                int
	waitCalls                 int
	waitEntered               chan struct{}
	waitRelease               chan struct{}
	waitEnteredOnce           sync.Once
}

type semanticReceiptAuthorityStore struct {
	sourceauthority.Store
	pages []catalog.SourceDriverReceiptAuthorityPage
	calls int
}

func (s *semanticReceiptAuthorityStore) PendingSourceDriverReceiptAuthorities(
	context.Context,
	causal.SourceAuthorityID,
	int,
) (catalog.SourceDriverReceiptAuthorityPage, error) {
	s.calls++
	if len(s.pages) == 0 {
		return catalog.SourceDriverReceiptAuthorityPage{}, nil
	}
	page := s.pages[0]
	s.pages = s.pages[1:]
	return page, nil
}

func newTestAuthority() *testAuthority { return &testAuthority{done: make(chan struct{})} }

func (a *testAuthority) Reconfigure(ctx context.Context, specs []tenant.TenantSpec) error {
	if a.reconfigureStarted != nil {
		a.startedOnce.Do(func() { close(a.reconfigureStarted) })
	}
	if a.reconfigureRelease != nil {
		if a.reconfigureIgnoresContext {
			<-a.reconfigureRelease
		} else {
			select {
			case <-a.reconfigureRelease:
			case <-ctx.Done():
				if a.reconfigureCanceled != nil {
					a.canceledOnce.Do(func() { close(a.reconfigureCanceled) })
				}
				return ctx.Err()
			}
		}
	}
	if err := ctx.Err(); err != nil && !a.reconfigureIgnoresContext {
		return err
	}
	a.mu.Lock()
	a.reconfigurations = append(a.reconfigurations, append([]tenant.TenantSpec(nil), specs...))
	call := len(a.reconfigurations)
	reconfigureErr := a.reconfigureErr
	a.mu.Unlock()
	if reconfigureErr != nil {
		return reconfigureErr(call, specs)
	}
	return nil
}

func (a *testAuthority) configurations() [][]tenant.TenantSpec {
	a.mu.Lock()
	defer a.mu.Unlock()
	result := make([][]tenant.TenantSpec, len(a.reconfigurations))
	for index := range a.reconfigurations {
		result[index] = append([]tenant.TenantSpec(nil), a.reconfigurations[index]...)
	}
	return result
}

func (*testAuthority) Barrier(context.Context, catalog.TenantID, catalog.Generation) (sourceauthority.BarrierResult, error) {
	return sourceauthority.BarrierResult{}, errors.New("unexpected barrier")
}

func (*testAuthority) PrepareSourceMutation(context.Context, tenant.SourceMutationStep) (tenant.SourceMutationOperation, error) {
	return tenant.SourceMutationOperation{}, errors.New("unexpected source mutation")
}

func (*testAuthority) ApplySourceMutation(
	context.Context,
	tenant.SourceMutationStep,
	tenant.SourceMutationOperation,
	tenant.SourceMutationContent,
) (tenant.SourceMutationApplyResult, error) {
	return tenant.SourceMutationApplyResult{}, errors.New("unexpected source mutation completion")
}

func (a *testAuthority) SourceMutationCommitted(_ context.Context, commit tenant.SourceMutationCommit) error {
	a.mu.Lock()
	a.commits = append(a.commits, commit)
	a.mu.Unlock()
	return nil
}

func (a *testAuthority) committedMutations() []tenant.SourceMutationCommit {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]tenant.SourceMutationCommit(nil), a.commits...)
}

func (a *testAuthority) RecoverCommittedReceipts(context.Context) error {
	a.mu.Lock()
	a.recoveries++
	err := a.recoveryErr
	a.mu.Unlock()
	return err
}

func (a *testAuthority) Close(context.Context) error {
	a.mu.Lock()
	a.closeCalls++
	a.mu.Unlock()
	a.closeOnce.Do(func() {
		if a.closeEntered != nil {
			close(a.closeEntered)
		}
		if a.closeRelease != nil {
			<-a.closeRelease
		}
		if a.onClose != nil {
			a.onClose()
		}
	})
	a.doneOnce.Do(func() { close(a.done) })
	return a.closeErr
}

func (a *testAuthority) Cancel() { a.doneOnce.Do(func() { close(a.done) }) }

func (a *testAuthority) Wait(ctx context.Context) error {
	a.mu.Lock()
	a.waitCalls++
	a.mu.Unlock()
	select {
	case <-a.done:
		if a.waitEntered != nil {
			a.waitEnteredOnce.Do(func() { close(a.waitEntered) })
		}
		if a.waitRelease != nil {
			<-a.waitRelease
		}
		return a.waitErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

type testAuthorityPolicy struct{}

func (testAuthorityPolicy) Roots(context.Context, []tenant.TenantSpec) ([]sourceauthority.RootSpec, error) {
	return nil, errors.New("unexpected roots")
}

func (testAuthorityPolicy) PlanDelta(context.Context, sourceauthority.IndexView, sourceauthority.EventBatch) (sourceauthority.DeltaPlan, error) {
	return sourceauthority.DeltaPlan{}, errors.New("unexpected delta")
}

func (testAuthorityPolicy) PlanSnapshot(
	context.Context,
	sourceauthority.SnapshotView,
	sourceauthority.SnapshotPlanCursor,
	int,
) (sourceauthority.SnapshotPlanPage, error) {
	return sourceauthority.SnapshotPlanPage{}, errors.New("unexpected snapshot")
}

func (testAuthorityPolicy) Materialize(context.Context, sourceauthority.MaterializerTask) (sourceauthority.Materialization, error) {
	return sourceauthority.Materialization{}, errors.New("unexpected materialization")
}

func (testAuthorityPolicy) PlanMutation(context.Context, sourceauthority.MutationRequest) (sourceauthority.MutationPlan, error) {
	return sourceauthority.MutationPlan{}, errors.New("unexpected mutation")
}

type testAuthorityExecutor struct{}

func (testAuthorityExecutor) RootIdentity(context.Context, sourceauthority.RootSpec) (sourceauthority.FileIdentity, error) {
	return sourceauthority.FileIdentity{}, errors.New("unexpected root identity")
}

func (testAuthorityExecutor) Stat(context.Context, sourceauthority.RootSpec, string) (sourceauthority.PhysicalEntry, error) {
	return sourceauthority.PhysicalEntry{}, errors.New("unexpected stat")
}

func (testAuthorityExecutor) BeginScan(context.Context, []sourceauthority.RootSpec) (sourceauthority.ScanSession, error) {
	return nil, errors.New("unexpected scan")
}

func (testAuthorityExecutor) Materialize(context.Context, sourceauthority.MaterializationTask) (sourceauthority.Materialization, error) {
	return sourceauthority.Materialization{}, errors.New("unexpected materialization")
}

func (testAuthorityExecutor) Open(context.Context, []sourceauthority.RootSpec, []sourceauthority.StreamCheckpoint, sourceauthority.DurableEventSink) (sourceauthority.EventStream, error) {
	return nil, errors.New("unexpected event stream")
}

func (testAuthorityExecutor) ApplyMutation(context.Context, sourceauthority.MutationTask) (sourceauthority.MutationReceipt, error) {
	return sourceauthority.MutationReceipt{}, errors.New("unexpected mutation")
}

func (testAuthorityExecutor) InspectMutation(
	context.Context,
	sourceauthority.MutationInspectionRequest,
) (sourceauthority.MutationInspection, error) {
	return sourceauthority.MutationInspection{}, errors.New("unexpected mutation inspection")
}

func (testAuthorityExecutor) AcknowledgeMutation(
	context.Context,
	causal.SourceAuthorityID,
	causal.Generation,
	catalog.MutationID,
	sourceauthority.Fingerprint,
) error {
	return errors.New("unexpected mutation acknowledgement")
}

func (testAuthorityExecutor) AbandonMutation(
	context.Context,
	causal.SourceAuthorityID,
	causal.Generation,
	catalog.MutationID,
) error {
	return errors.New("unexpected mutation abandonment")
}

func (testAuthorityExecutor) MutationTerminalProofPage(
	context.Context,
	causal.SourceAuthorityID,
	catalog.MutationID,
	int,
) (sourceauthority.MutationTerminalProofPage, error) {
	return sourceauthority.MutationTerminalProofPage{}, errors.New("unexpected mutation terminal proofs")
}

func (testAuthorityExecutor) ForgetMutation(
	context.Context,
	causal.SourceAuthorityID,
	sourceauthority.MutationTerminalProof,
) error {
	return errors.New("unexpected mutation forget")
}

func (testAuthorityExecutor) Close() error { return nil }

type closingTestAuthorityExecutor struct {
	testAuthorityExecutor
	mu       sync.Mutex
	closes   int
	closeErr error
}

func (e *closingTestAuthorityExecutor) Close() error {
	e.mu.Lock()
	e.closes++
	e.mu.Unlock()
	return e.closeErr
}
