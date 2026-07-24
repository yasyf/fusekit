package holder

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/tenant"
)

func TestPresentationLifecycleStartsDeclaredBackendsBeforeProvision(t *testing.T) {
	native := &presentationTestOperation{}
	broker := &presentationTestOperation{}
	manager, err := newPresentationManager(
		t.Context(), time.Second, time.Second,
		&presentationTestFactory{operations: []presentationOperation{native}},
		&presentationTestFactory{operations: []presentationOperation{broker}},
	)
	if err != nil {
		t.Fatal(err)
	}
	next := &presentationTestLifecycle{}
	runtime := presentationLifecycleRuntime{next: next, presentations: manager}

	if err := runtime.ProvisionTenant(t.Context(), presentationTestSpec("native", catalog.PresentMount)); err != nil {
		t.Fatal(err)
	}
	if native.starts.Load() != 1 || broker.starts.Load() != 0 {
		t.Fatalf("native provision starts = %d/%d", native.starts.Load(), broker.starts.Load())
	}
	if err := runtime.ProvisionTenant(t.Context(), presentationTestSpec("broker", catalog.PresentFileProvider)); err != nil {
		t.Fatal(err)
	}
	if native.starts.Load() != 1 || broker.starts.Load() != 1 || next.provisions.Load() != 2 {
		t.Fatalf(
			"backend starts = %d/%d; provisions = %d",
			native.starts.Load(), broker.starts.Load(), next.provisions.Load(),
		)
	}
}

func TestPresentationLifecycleColdRemoveStartsOnlyDeclaredFileProvider(t *testing.T) {
	native := &presentationTestOperation{}
	broker := &presentationTestOperation{}
	manager, err := newPresentationManager(
		t.Context(), time.Second, time.Second,
		&presentationTestFactory{operations: []presentationOperation{native}},
		&presentationTestFactory{operations: []presentationOperation{broker}},
	)
	if err != nil {
		t.Fatal(err)
	}
	next := &presentationTestLifecycle{}
	current := presentationTestSpec("mixed", catalog.PresentMount|catalog.PresentFileProvider)
	runtime := presentationLifecycleRuntime{
		next: next, presentations: manager,
		lookup: func(id catalog.TenantID) (tenant.TenantSpec, error) {
			if id != current.ID {
				return tenant.TenantSpec{}, tenant.ErrTenantNotFound
			}
			return current, nil
		},
	}

	if err := runtime.RemoveTenant(t.Context(), current.ID, current.Generation, current.OwnerID); err != nil {
		t.Fatal(err)
	}
	if native.starts.Load() != 0 || broker.starts.Load() != 1 || next.removals.Load() != 1 {
		t.Fatalf(
			"cold removal starts = %d/%d; removals = %d",
			native.starts.Load(), broker.starts.Load(), next.removals.Load(),
		)
	}
}

func TestPresentationLifecycleRemoveReplayDelegatesWithoutStartingBackends(t *testing.T) {
	native := &presentationTestOperation{}
	broker := &presentationTestOperation{}
	manager, err := newPresentationManager(
		t.Context(), time.Second, time.Second,
		&presentationTestFactory{operations: []presentationOperation{native}},
		&presentationTestFactory{operations: []presentationOperation{broker}},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("proved removed")
	next := &presentationTestLifecycle{removeErr: want}
	runtime := presentationLifecycleRuntime{
		next: next, presentations: manager,
		lookup: func(catalog.TenantID) (tenant.TenantSpec, error) {
			return tenant.TenantSpec{}, tenant.ErrTenantNotFound
		},
	}

	err = runtime.RemoveTenant(t.Context(), "removed", 7, "owner")
	if !errors.Is(err, want) {
		t.Fatalf("RemoveTenant replay = %v, want delegated proof", err)
	}
	if native.starts.Load() != 0 || broker.starts.Load() != 0 || next.removals.Load() != 1 {
		t.Fatalf(
			"replay starts = %d/%d; removals = %d",
			native.starts.Load(), broker.starts.Load(), next.removals.Load(),
		)
	}
}

type presentationTestLifecycle struct {
	provisions atomic.Int64
	removals   atomic.Int64
	removeErr  error
}

func (r *presentationTestLifecycle) ProvisionTenant(context.Context, tenant.TenantSpec) error {
	r.provisions.Add(1)
	return nil
}

func (*presentationTestLifecycle) ReplaceTenant(context.Context, catalog.Generation, tenant.TenantSpec) error {
	return nil
}

func (r *presentationTestLifecycle) RemoveTenant(
	context.Context,
	catalog.TenantID,
	catalog.Generation,
	tenant.OwnerID,
) error {
	r.removals.Add(1)
	return r.removeErr
}

func (*presentationTestLifecycle) State(
	context.Context,
	catalog.TenantID,
	tenant.OwnerID,
) (tenant.TenantStatus, error) {
	return tenant.TenantStatus{}, nil
}

func presentationTestSpec(id catalog.TenantID, presentations catalog.PresentationSet) tenant.TenantSpec {
	return tenant.TenantSpec{ID: id, Generation: 1, Traits: tenant.TenantTraits{Presentations: presentations}}
}

func TestNativePresentationPreparerWaitsForBackendBeforeRouteProof(t *testing.T) {
	operation := &presentationTestOperation{}
	manager, err := newPresentationManager(
		t.Context(), time.Second, time.Second,
		&presentationTestFactory{operations: []presentationOperation{operation}}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	var routes atomic.Int64
	preparer := nativePresentationPreparer{
		presentations: manager,
		route: func(id catalog.TenantID, generation catalog.Generation) error {
			routes.Add(1)
			if id != "tenant" || generation != 7 {
				return errors.New("wrong route identity")
			}
			if operation.starts.Load() != 1 || operation.readies.Load() != 1 {
				return errors.New("route checked before backend readiness")
			}
			return nil
		},
	}
	if err := preparer.PrepareMountPresentation(t.Context(), "tenant", 7); err != nil {
		t.Fatal(err)
	}
	if routes.Load() != 1 {
		t.Fatalf("route proof calls = %d, want one", routes.Load())
	}
}

func TestOwnedPresentationOperationProvesAsynchronousSettlement(t *testing.T) {
	closeEntered := make(chan struct{})
	release := make(chan struct{})
	operation, err := newOwnedPresentationOperation(
		func(context.Context) error { return nil },
		func() error { return nil },
		func() error {
			close(closeEntered)
			<-release
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := operation.stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-closeEntered
	deadline, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if err := operation.wait(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded wait = %v", err)
	}
	close(release)
	if err := operation.wait(t.Context()); err != nil {
		t.Fatalf("settled wait: %v", err)
	}
}

func TestPresentationManagerCloseCanFinishProofAfterBoundedWait(t *testing.T) {
	closeEntered := make(chan struct{})
	release := make(chan struct{})
	operation, err := newOwnedPresentationOperation(
		func(context.Context) error { return nil },
		func() error { return nil },
		func() error {
			close(closeEntered)
			<-release
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := newPresentationManager(
		t.Context(), time.Second, time.Millisecond,
		presentationOperationFactoryFunc(func() (presentationOperation, error) { return operation, nil }), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureNative(t.Context()); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- manager.Close(deadline) }()
	<-closeEntered
	if err := <-first; !errors.Is(err, errPresentationShutdownIncomplete) {
		t.Fatalf("bounded close = %v", err)
	}
	close(release)
	if err := manager.Close(t.Context()); err != nil {
		t.Fatalf("settled close: %v", err)
	}
}
