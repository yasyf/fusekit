package catalogworker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

type workerTenantStore interface {
	ProvisionTenant(context.Context, catalog.TenantProvision) (catalog.TenantProvision, error)
	LoadTenantState(context.Context, catalog.TenantID) (catalog.TenantStateRecord, error)
	SaveTenantState(context.Context, catalog.StateVersion, catalog.TenantStateRecord) (catalog.TenantStateRecord, error)
}

type workerCurrentTenantFixture struct {
	Provision         catalog.TenantProvision
	Object            catalog.Object
	Revision          catalog.Revision
	FleetOwner        catalog.SourceAuthorityFleetOwnerID
	DeclarationDigest [sha256.Size]byte
	Checkpoint        catalog.SourceDriverCheckpoint
}

func newTestManagerForDatabase(t *testing.T, database string) *Manager {
	t.Helper()
	manager, err := NewManager(t.Context(), ManagerConfig{
		Executable: "/test/product-helper", Database: database,
		ExpectedSignature: testChildSignature(), launcher: newTestProcessLauncher(t),
		ReadinessTimeout: 5 * time.Second, OperationTimeout: 10 * time.Second, StopTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}

func provisionWorkerTenantStateForTest(
	t *testing.T,
	store workerTenantStore,
	provision catalog.TenantProvision,
) catalog.TenantProvision {
	t.Helper()
	persisted, err := store.ProvisionTenant(t.Context(), provision)
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	if _, err := store.LoadTenantState(t.Context(), persisted.Tenant); err == nil {
		return persisted
	} else if !errors.Is(err, catalog.ErrStateNotFound) {
		t.Fatalf("LoadTenantState: %v", err)
	}
	if _, err := store.SaveTenantState(t.Context(), 0, catalog.TenantStateRecord{
		Tenant: persisted.Tenant, Generation: persisted.Generation,
	}); err != nil {
		t.Fatalf("SaveTenantState: %v", err)
	}
	return persisted
}

func installCurrentWorkerTenantForTest(
	t *testing.T,
	manager *Manager,
	provision catalog.TenantProvision,
) workerCurrentTenantFixture {
	t.Helper()
	authority := causal.SourceAuthorityID(provision.ContentSourceID)
	fleetOwner := catalog.SourceAuthorityFleetOwnerID("catalogworker-fixture-" + provision.Tenant)
	declarations, authoritiesDigest, declarationsDigest := testSourceAuthorityFleet(
		t, []causal.SourceAuthorityID{authority},
	)
	fleet, err := manager.ReconcileSourceAuthorityFleet(t.Context(), catalog.SourceAuthorityFleetReconcileRequest{
		Owner: fleetOwner, Generation: 1, Declarations: declarations, Complete: true,
		AuthorityCount: 1, AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
	})
	if err != nil {
		t.Fatalf("ReconcileSourceAuthorityFleet: %v", err)
	}
	if _, err := manager.AcknowledgeSourceAuthorityFleet(t.Context(), catalog.SourceAuthorityFleetAcknowledgement{
		Owner: fleetOwner, Generation: 1, AuthorityCount: 1,
		AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
		StageDigest: fleet.StageDigest,
	}); err != nil {
		t.Fatalf("AcknowledgeSourceAuthorityFleet: %v", err)
	}
	provision, err = manager.ProvisionTenant(t.Context(), provision)
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	content, err := managerCall(manager, t.Context(), func(client *Client) (catalog.ContentRef, error) {
		return client.StageContent(t.Context(), strings.NewReader("initial"))
	})
	if err != nil {
		t.Fatalf("StageContent: %v", err)
	}
	result := commitInitialWorkerPublicationForTest(
		t, manager, provision, content, fleetOwner, declarations[0].DeclarationDigest,
	)
	activateWorkerPublicationForTest(t, manager, provision, result.Checkpoint)
	if err := manager.AcknowledgeSourceDriverCommittedReceipt(t.Context(), result); err != nil {
		t.Fatalf("AcknowledgeSourceDriverCommittedReceipt: %v", err)
	}
	if err := manager.ForgetSourceDriverCommittedReceipt(t.Context(), result); err != nil {
		t.Fatalf("ForgetSourceDriverCommittedReceipt: %v", err)
	}
	root, err := manager.Root(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatalf("Root after activation: %v", err)
	}
	revision, err := manager.Head(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatalf("Head after activation: %v", err)
	}
	page, err := manager.Snapshot(t.Context(), provision.Tenant, catalog.EnumerationScope{
		Kind: catalog.EnumerationContainer, Presentation: catalog.PresentationMount, Parent: root.ID,
	}, revision, catalog.SnapshotCursor{}, 2)
	if err != nil {
		t.Fatalf("Snapshot after activation: %v", err)
	}
	if len(page.Objects) != 1 || page.Objects[0].Name != "settings.json" {
		t.Fatalf("snapshot objects = %+v, want settings.json", page.Objects)
	}
	return workerCurrentTenantFixture{
		Provision: provision, Object: page.Objects[0], Revision: revision,
		FleetOwner: fleetOwner, DeclarationDigest: declarations[0].DeclarationDigest,
		Checkpoint: result.Checkpoint,
	}
}

func commitInitialWorkerPublicationForTest(
	t *testing.T,
	manager *Manager,
	provision catalog.TenantProvision,
	content catalog.ContentRef,
	fleetOwner catalog.SourceAuthorityFleetOwnerID,
	declarationDigest [sha256.Size]byte,
) catalog.SourceDriverStageResult {
	t.Helper()
	authority := causal.SourceAuthorityID(provision.ContentSourceID)
	targets := []catalog.SourceDriverTarget{{Tenant: provision.Tenant, Generation: provision.Generation}}
	targetsDigest, err := catalog.SourceDriverTargetsDigest(targets)
	if err != nil {
		t.Fatal(err)
	}
	identity := catalog.SourceDriverStageIdentity{
		Authority: authority, FleetOwner: fleetOwner, AuthorityGeneration: 1,
		DeclarationDigest: declarationDigest, TargetCount: 1, TargetsDigest: targetsDigest,
		Operation: causal.OperationID{0x31}, SourceOperation: causal.OperationID{0x32},
		ChangeID: causal.ChangeID{0x33}, Cause: causal.CauseBootstrap,
		Mode: catalog.SourceDriverSnapshot, SnapshotReason: catalog.SourceDriverSnapshotInitial,
		ToToken: "catalogworker-fixture-current",
	}
	if err := manager.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatalf("BeginSourceDriverStage: %v", err)
	}
	pending, err := manager.PendingSourceDriverStage(t.Context(), authority)
	if err != nil || pending == nil {
		t.Fatalf("PendingSourceDriverStage after begin: %+v, %v", pending, err)
	}
	pageDigest := sha256.Sum256([]byte("catalogworker initial publication"))
	stage, err := manager.AppendSourceDriverStage(t.Context(), identity, catalog.SourceDriverStagePage{
		Digest: pageDigest, PredecessorDigest: catalog.SourceDriverPagePredecessorDigest(nil, [sha256.Size]byte{}),
		Entries: []catalog.SourceDriverStageEntry{{
			Tenant: provision.Tenant, Generation: provision.Generation, Key: "settings",
			Object: &catalog.SourceObject{
				Key: "settings", Name: "settings.json", Kind: catalog.KindFile, Mode: 0o600,
				ContentRevision: 1, Content: content, Visibility: catalog.Visibility{Mount: true},
			},
		}},
		Complete: true,
	})
	if err != nil {
		t.Fatalf("AppendSourceDriverStage: %v", err)
	}
	var last catalog.SourceDriverPreparationState
	for step := 0; step < 256; step++ {
		prepared, err := manager.PrepareSourceDriverPublicationBatch(t.Context(), identity)
		if err != nil {
			t.Fatalf("PrepareSourceDriverPublicationBatch step %d after %+v: %v", step, last, err)
		}
		last = prepared
		if prepared.Prepared {
			result, err := manager.CommitSourceDriverStage(t.Context(), stage)
			if err != nil {
				t.Fatalf("CommitSourceDriverStage: %v", err)
			}
			return result
		}
	}
	t.Fatal("source driver publication preparation did not converge")
	return catalog.SourceDriverStageResult{}
}

func activateWorkerPublicationForTest(
	t *testing.T,
	manager *Manager,
	provision catalog.TenantProvision,
	checkpoint catalog.SourceDriverCheckpoint,
) {
	t.Helper()
	state, err := manager.TenantLifecycle(t.Context(), provision.OwnerID, provision.Tenant)
	if err != nil || state.Target == nil {
		t.Fatalf("TenantLifecycle: %+v, %v", state, err)
	}
	mutation := func() catalog.TenantMutation {
		operation, err := catalog.NewTenantOperationID()
		if err != nil {
			t.Fatal(err)
		}
		return catalog.TenantMutation{
			OperationID: operation, HolderRuntimeGeneration: "catalogworker-fixture",
			OwnerID: provision.OwnerID, ExpectedIntentRevision: state.Intent.Revision,
		}
	}
	lease, state, err := manager.StageApplication(t.Context(), catalog.StageApplicationRequest{
		Mutation: mutation(), Tenant: provision.Tenant, Generation: provision.Generation,
		Authority: checkpoint.Authority, Publication: checkpoint.PublicationID,
		PublicationDigest: checkpoint.PublicationDigest,
	})
	if err != nil {
		t.Fatalf("StageApplication: %v", err)
	}
	for _, backend := range state.Target.RequiredBackends.Backends() {
		state, err = manager.RecordPresentation(t.Context(), catalog.PresentationReceipt{
			Mutation: mutation(), Lease: lease, Backend: backend,
			BackendGeneration: fmt.Sprintf("catalogworker-fixture-%d", backend),
			ObservedRevision:  lease.CatalogHead,
		})
		if err != nil {
			t.Fatalf("RecordPresentation: %v", err)
		}
	}
	targeting, err := manager.TenantTargetingRevision(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatalf("TenantTargetingRevision: %v", err)
	}
	if _, err := manager.ActivateTenant(t.Context(), catalog.ActivateTenantRequest{
		Mutation: mutation(), Tenant: provision.Tenant, Generation: provision.Generation,
		ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
		ExpectedActivationRevision: state.Activation.Revision,
		ExpectedActiveGeneration:   state.Activation.ActiveGeneration,
		CausePublications:          []causal.OperationID{checkpoint.PublicationID},
		ExpectedTargetingRevision:  targeting,
	}); err != nil {
		t.Fatalf("ActivateTenant: %v", err)
	}
}
