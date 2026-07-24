package catalog

import (
	"errors"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestRecoverTenantPreparationsResetsOnlyProvenNonActiveOwner(t *testing.T) {
	c := newTestCatalog(t)
	definition := lifecycleTestProvision(t, "recover-preparation", 1)
	state, lease, _ := stageLifecycleForTest(t, c, definition)
	state = recordBackendForTest(t, c, state, lease, TenantBackendNative)
	application := lifecycleApplicationForTest(t, state, definition.Generation)
	pending := lifecyclePresentationForTest(t, state, definition.Generation, TenantBackendBroker)
	lostHolder := application.HolderRuntimeGeneration

	result, err := c.RecoverTenantPreparations(t.Context(), TenantPreparationRecoveryRequest{
		CurrentHolderRuntimeGeneration:  "new-holder",
		SettledHolderRuntimeGenerations: []string{lostHolder},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ResetApplications != 1 || result.ResetPresentations != 1 {
		t.Fatalf("recovery result = %+v", result)
	}
	recovered, err := c.TenantLifecycle(t.Context(), definition.OwnerID, definition.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	application = lifecycleApplicationForTest(t, recovered, definition.Generation)
	if application.Phase != TenantApplicationPending || application.ViewID != (StagedViewID{}) ||
		application.SourcePublication != (causal.OperationID{}) || application.HolderRuntimeGeneration != "" ||
		application.OperationID != (TenantOperationID{}) {
		t.Fatalf("recovered application = %+v", application)
	}
	for _, presentation := range recovered.Presentations {
		if presentation.Generation != definition.Generation {
			continue
		}
		if presentation.Phase != PresentationMaterializationPending || presentation.ViewID != (StagedViewID{}) ||
			presentation.BackendGeneration != "" || presentation.ObservedRevision != 0 ||
			presentation.HolderRuntimeGeneration != "" || presentation.OperationID != (TenantOperationID{}) {
			t.Fatalf("recovered presentation = %+v", presentation)
		}
	}
	if got := lifecyclePresentationForTest(t, recovered, definition.Generation, TenantBackendBroker); got != pending {
		t.Fatalf("unowned pending presentation changed: before=%+v after=%+v", pending, got)
	}
	var audits uint64
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM tenant_mutations WHERE tenant_id = ? AND kind = ? AND result_code = 'owner_lost'`,
		string(definition.Tenant), uint8(TenantMutationRecoverOwnerLost)).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("owner-lost audits = %d", audits)
	}
	replay, err := c.RecoverTenantPreparations(t.Context(), TenantPreparationRecoveryRequest{
		CurrentHolderRuntimeGeneration:  "new-holder",
		SettledHolderRuntimeGenerations: []string{lostHolder},
	})
	if err != nil || replay != (TenantPreparationRecoveryResult{}) {
		t.Fatalf("recovery replay = %+v, %v", replay, err)
	}
}

func TestRecoverTenantPreparationsLeavesCurrentOwnerResumable(t *testing.T) {
	c := newTestCatalog(t)
	definition := lifecycleTestProvision(t, "recover-current-preparation", 1)
	state, _, _ := stageLifecycleForTest(t, c, definition)
	application := lifecycleApplicationForTest(t, state, definition.Generation)

	result, err := c.RecoverTenantPreparations(t.Context(), TenantPreparationRecoveryRequest{
		CurrentHolderRuntimeGeneration: application.HolderRuntimeGeneration,
	})
	if err != nil || result != (TenantPreparationRecoveryResult{}) {
		t.Fatalf("current owner recovery = %+v, %v", result, err)
	}
	after, err := c.TenantLifecycle(t.Context(), definition.OwnerID, definition.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if got := lifecycleApplicationForTest(t, after, definition.Generation); got != application {
		t.Fatalf("current owner application changed: before=%+v after=%+v", application, got)
	}
}

func TestRecoverTenantPreparationsRejectsLivePresentationOwnerAtomically(t *testing.T) {
	for _, test := range []struct {
		name              string
		presentationOwner string
	}{
		{name: "current", presentationOwner: "new-holder"},
		{name: "unproven", presentationOwner: "live-holder"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestCatalog(t)
			definition := lifecycleTestProvision(t, "recover-live-presentation-"+test.name, 1)
			state, lease, _ := stageLifecycleForTest(t, c, definition)
			application := lifecycleApplicationForTest(t, state, definition.Generation)
			mutation := tenantMutationForTest(t, state.OwnerID, state.Intent.Revision)
			mutation.HolderRuntimeGeneration = test.presentationOwner
			state, err := c.RecordPresentation(t.Context(), PresentationReceipt{
				Mutation: mutation, Lease: lease, Backend: TenantBackendNative,
				BackendGeneration: "backend", ObservedRevision: lease.CatalogHead,
			})
			if err != nil {
				t.Fatal(err)
			}
			presentation := lifecyclePresentationForTest(t, state, definition.Generation, TenantBackendNative)

			_, err = c.RecoverTenantPreparations(t.Context(), TenantPreparationRecoveryRequest{
				CurrentHolderRuntimeGeneration:  "new-holder",
				SettledHolderRuntimeGenerations: []string{application.HolderRuntimeGeneration},
			})
			if !errors.Is(err, ErrTenantPreparationOwnershipConflict) {
				t.Fatalf("live presentation recovery = %v", err)
			}
			after, loadErr := c.TenantLifecycle(t.Context(), definition.OwnerID, definition.Tenant)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if got := lifecycleApplicationForTest(t, after, definition.Generation); got != application {
				t.Fatalf("application changed: before=%+v after=%+v", application, got)
			}
			if got := lifecyclePresentationForTest(t, after, definition.Generation, TenantBackendNative); got != presentation {
				t.Fatalf("presentation changed: before=%+v after=%+v", presentation, got)
			}
		})
	}
}

func TestRecoverTenantPreparationsResetsEveryProvenPresentationOwner(t *testing.T) {
	c := newTestCatalog(t)
	definition := lifecycleTestProvision(t, "recover-proven-presentation", 1)
	state, lease, _ := stageLifecycleForTest(t, c, definition)
	application := lifecycleApplicationForTest(t, state, definition.Generation)
	const presentationOwner = "other-settled-holder"
	mutation := tenantMutationForTest(t, state.OwnerID, state.Intent.Revision)
	mutation.HolderRuntimeGeneration = presentationOwner
	state, err := c.RecordPresentation(t.Context(), PresentationReceipt{
		Mutation: mutation, Lease: lease, Backend: TenantBackendNative,
		BackendGeneration: "backend", ObservedRevision: lease.CatalogHead,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.RecoverTenantPreparations(t.Context(), TenantPreparationRecoveryRequest{
		CurrentHolderRuntimeGeneration: "new-holder",
		SettledHolderRuntimeGenerations: []string{
			application.HolderRuntimeGeneration,
			presentationOwner,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ResetApplications != 1 || result.ResetPresentations != 1 {
		t.Fatalf("recovery result = %+v", result)
	}
	after, err := c.TenantLifecycle(t.Context(), definition.OwnerID, definition.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if got := lifecyclePresentationForTest(t, after, definition.Generation, TenantBackendNative); got.Phase != PresentationMaterializationPending || got.HolderRuntimeGeneration != "" ||
		got.OperationID != (TenantOperationID{}) {
		t.Fatalf("recovered presentation = %+v", got)
	}
}

func TestRecoverTenantPreparationsRequiresCanonicalOwnerFence(t *testing.T) {
	c := newTestCatalog(t)
	for _, test := range []struct {
		name    string
		request TenantPreparationRecoveryRequest
	}{
		{name: "missing current"},
		{name: "empty settled", request: TenantPreparationRecoveryRequest{
			CurrentHolderRuntimeGeneration: "current", SettledHolderRuntimeGenerations: []string{""},
		}},
		{name: "current settled", request: TenantPreparationRecoveryRequest{
			CurrentHolderRuntimeGeneration: "current", SettledHolderRuntimeGenerations: []string{"current"},
		}},
		{name: "duplicate", request: TenantPreparationRecoveryRequest{
			CurrentHolderRuntimeGeneration: "current", SettledHolderRuntimeGenerations: []string{"old", "old"},
		}},
		{name: "unordered", request: TenantPreparationRecoveryRequest{
			CurrentHolderRuntimeGeneration: "current", SettledHolderRuntimeGenerations: []string{"z", "a"},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := c.RecoverTenantPreparations(t.Context(), test.request); !errors.Is(err, ErrInvalidObject) {
				t.Fatalf("RecoverTenantPreparations error = %v, want invalid object", err)
			}
		})
	}
}

func TestRecoverTenantPreparationsRejectsUnprovenOwnerAtomically(t *testing.T) {
	c := newTestCatalog(t)
	definition := lifecycleTestProvision(t, "reject-unproven-preparation", 1)
	state, _, _ := stageLifecycleForTest(t, c, definition)
	before := lifecycleApplicationForTest(t, state, definition.Generation)

	_, err := c.RecoverTenantPreparations(t.Context(), TenantPreparationRecoveryRequest{
		CurrentHolderRuntimeGeneration: "new-holder",
	})
	if !errors.Is(err, ErrTenantPreparationOwnershipConflict) {
		t.Fatalf("unproven recovery = %v", err)
	}
	after, err := c.TenantLifecycle(t.Context(), definition.OwnerID, definition.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if got := lifecycleApplicationForTest(t, after, definition.Generation); got != before {
		t.Fatalf("unproven recovery changed application: before=%+v after=%+v", before, got)
	}
}

func TestRecoverTenantPreparationsNeverResetsActiveGeneration(t *testing.T) {
	c := newTestCatalog(t)
	definition := lifecycleTestProvision(t, "preserve-active-preparation", 1)
	state, lease, publication := stageLifecycleForTest(t, c, definition)
	for _, backend := range state.Target.RequiredBackends.Backends() {
		state = recordBackendForTest(t, c, state, lease, backend)
	}
	activated, err := c.ActivateTenant(t.Context(), ActivateTenantRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
		ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
		ExpectedActivationRevision: state.Activation.Revision,
		ExpectedActiveGeneration:   state.Activation.ActiveGeneration,
		CausePublications:          []causal.OperationID{publication},
		ExpectedTargetingRevision:  mustTargetingRevision(t, c, definition.Tenant),
	})
	if err != nil {
		t.Fatal(err)
	}
	application := lifecycleApplicationForTest(t, activated.State, definition.Generation)
	result, err := c.RecoverTenantPreparations(t.Context(), TenantPreparationRecoveryRequest{
		CurrentHolderRuntimeGeneration:  "new-holder",
		SettledHolderRuntimeGenerations: []string{application.HolderRuntimeGeneration},
	})
	if err != nil || result != (TenantPreparationRecoveryResult{}) {
		t.Fatalf("active recovery = %+v, %v", result, err)
	}
	after, err := c.TenantLifecycle(t.Context(), definition.OwnerID, definition.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if !after.Ready() || after.Activation.ActiveGeneration != definition.Generation ||
		lifecycleApplicationForTest(t, after, definition.Generation) != application {
		t.Fatalf("active lifecycle changed = %+v", after)
	}
}

func lifecycleApplicationForTest(t *testing.T, state TenantLifecycleState, generation Generation) TenantApplication {
	t.Helper()
	for _, application := range state.Applications {
		if application.Generation == generation {
			return application
		}
	}
	t.Fatalf("missing lifecycle application for generation %d", generation)
	return TenantApplication{}
}

func lifecyclePresentationForTest(
	t *testing.T,
	state TenantLifecycleState,
	generation Generation,
	backend TenantBackend,
) PresentationMaterialization {
	t.Helper()
	for _, presentation := range state.Presentations {
		if presentation.Generation == generation && presentation.Backend == backend {
			return presentation
		}
	}
	t.Fatalf("missing lifecycle presentation for generation %d backend %d", generation, backend)
	return PresentationMaterialization{}
}
