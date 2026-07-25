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

func TestPresentationLifecycleStartsOnlyDeclaredBackends(t *testing.T) {
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
	if native.starts.Load() != 1 || broker.starts.Load() != 1 {
		t.Fatalf("broker provision starts = %d/%d", native.starts.Load(), broker.starts.Load())
	}
	if err := runtime.ProvisionTenant(t.Context(), presentationTestSpec("both", catalog.PresentMount|catalog.PresentFileProvider)); err != nil {
		t.Fatal(err)
	}
	if native.starts.Load() != 1 || broker.starts.Load() != 1 || next.provisions.Load() != 3 {
		t.Fatalf("coalesced starts = %d/%d; provisions = %d", native.starts.Load(), broker.starts.Load(), next.provisions.Load())
	}
}

func TestPresentationPreparersJoinBackendBeforeProof(t *testing.T) {
	native := &presentationTestOperation{}
	manager, err := newPresentationManager(
		t.Context(), time.Second, time.Second,
		&presentationTestFactory{operations: []presentationOperation{native}},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	var routeCalls atomic.Int64
	mounts := nativePresentationPreparer{
		presentations: manager,
		route: func(id catalog.TenantID, generation catalog.Generation) error {
			routeCalls.Add(1)
			if id != "tenant" || generation != 7 {
				return errors.New("wrong native route")
			}
			return nil
		},
	}
	if err := mounts.PrepareMountPresentation(t.Context(), "tenant", 7); err != nil {
		t.Fatal(err)
	}
	if native.starts.Load() != 1 || routeCalls.Load() != 1 {
		t.Fatalf("native prepare calls = %d/%d", native.starts.Load(), routeCalls.Load())
	}

	broker := &presentationTestOperation{}
	manager, err = newPresentationManager(
		t.Context(), time.Second, time.Second,
		nil,
		&presentationTestFactory{operations: []presentationOperation{broker}},
	)
	if err != nil {
		t.Fatal(err)
	}
	next := &presentationTestFileProvider{}
	providers := fileProviderPresentationPreparer{presentations: manager, next: next}
	if _, err := providers.PrepareFileProviderPresentation(t.Context(), "tenant", 7); err != nil {
		t.Fatal(err)
	}
	if broker.starts.Load() != 1 || next.calls.Load() != 1 {
		t.Fatalf("broker prepare calls = %d/%d", broker.starts.Load(), next.calls.Load())
	}
}

type presentationTestLifecycle struct{ provisions atomic.Int64 }

func (r *presentationTestLifecycle) ProvisionTenant(context.Context, tenant.TenantSpec) error {
	r.provisions.Add(1)
	return nil
}

func (*presentationTestLifecycle) ReplaceTenant(context.Context, catalog.Generation, tenant.TenantSpec) error {
	return nil
}

func (*presentationTestLifecycle) RemoveTenant(context.Context, catalog.TenantID, catalog.Generation, tenant.OwnerID) error {
	return nil
}

func (*presentationTestLifecycle) State(context.Context, catalog.TenantID, tenant.OwnerID) (tenant.TenantStatus, error) {
	return tenant.TenantStatus{}, nil
}

type presentationTestFileProvider struct{ calls atomic.Int64 }

func (p *presentationTestFileProvider) PrepareFileProviderPresentation(
	context.Context,
	catalog.TenantID,
	catalog.Generation,
) (catalog.FileProviderDomain, error) {
	p.calls.Add(1)
	return catalog.FileProviderDomain{}, nil
}

func presentationTestSpec(id catalog.TenantID, presentations catalog.PresentationSet) tenant.TenantSpec {
	return tenant.TenantSpec{ID: id, Generation: 1, Traits: tenant.TenantTraits{Presentations: presentations}}
}
