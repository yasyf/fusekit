package catalog

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
)

func TestTenantGenerationAndMutationReplayAreExact(t *testing.T) {
	c := newTestCatalog(t)
	definition := lifecycleTestProvision(t, "exact-generation", 1)
	mutation := tenantMutationForTest(t, definition.OwnerID, 0)
	created, err := c.SetTenantPresent(t.Context(), mutation, definition)
	if err != nil {
		t.Fatalf("SetTenantPresent: %v", err)
	}
	replayed, err := c.SetTenantPresent(t.Context(), mutation, definition)
	if err != nil {
		t.Fatalf("SetTenantPresent replay: %v", err)
	}
	if replayed.Intent.Revision != created.Intent.Revision || replayed.Intent.CurrentOperation != mutation.OperationID {
		t.Fatalf("replay changed intent: before=%+v after=%+v", created.Intent, replayed.Intent)
	}
	conflict := definition
	conflict.BackingRoot = filepath.Join(t.TempDir(), "different")
	if _, err := c.SetTenantPresent(t.Context(), mutation, conflict); !errors.Is(err, ErrTenantMutationConflict) {
		t.Fatalf("operation reuse = %v, want ErrTenantMutationConflict", err)
	}
	if _, err := c.SetTenantPresent(t.Context(), tenantMutationForTest(t, definition.OwnerID, created.Intent.Revision), conflict); !errors.Is(err, ErrTenantProvisionConflict) {
		t.Fatalf("same generation different spec = %v, want ErrTenantProvisionConflict", err)
	}
}

func TestTenantActivationRequiresEveryExactPresentationReceipt(t *testing.T) {
	c := newTestCatalog(t)
	definition := lifecycleTestProvision(t, "exact-receipts", 1)
	state, lease, publication := stageLifecycleForTest(t, c, definition)
	state = recordBackendForTest(t, c, state, lease, TenantBackendNative)
	if _, err := c.ActivateTenant(t.Context(), ActivateTenantRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
		ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
		ExpectedActivationRevision: state.Activation.Revision,
		ExpectedTargetingRevision:  mustTargetingRevision(t, c, definition.Tenant),
		CausePublications:          []causal.OperationID{publication},
	}); !errors.Is(err, ErrTenantLifecycleStale) {
		t.Fatalf("activate with missing receipt = %v, want stale", err)
	}
	state = recordBackendForTest(t, c, state, lease, TenantBackendBroker)
	activated, err := c.ActivateTenant(t.Context(), ActivateTenantRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
		ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
		ExpectedActivationRevision: state.Activation.Revision,
		ExpectedTargetingRevision:  mustTargetingRevision(t, c, definition.Tenant),
		CausePublications:          []causal.OperationID{publication},
	})
	if err != nil {
		t.Fatalf("ActivateTenant: %v", err)
	}
	if !activated.State.Ready() || activated.ChangeID == (causal.ActivationChangeID{}) || len(activated.Causes) != 1 {
		t.Fatalf("activation = %+v", activated)
	}
}

func TestTenantActivationTargetsOnlyExactInterestedLiveFileProvider(t *testing.T) {
	c := newTestCatalog(t)
	definition := lifecycleTestProvision(t, "targeting", 1)
	state, lease, publication := stageLifecycleForTest(t, c, definition)
	for _, backend := range state.Target.RequiredBackends.Backends() {
		state = recordBackendForTest(t, c, state, lease, backend)
	}
	domainID := causal.DomainID("targeting-domain")
	seedActivationTargetForTest(t, c, definition, lease, domainID)
	targetingRevision := mustTargetingRevision(t, c, definition.Tenant)
	activated, err := c.ActivateTenant(t.Context(), ActivateTenantRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
		ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
		ExpectedActivationRevision: state.Activation.Revision,
		ExpectedTargetingRevision:  targetingRevision,
		CausePublications:          []causal.OperationID{publication},
	})
	if err != nil {
		t.Fatalf("ActivateTenant: %v", err)
	}
	if len(activated.Targets) != 1 || activated.Targets[0].PresentationID != causal.PresentationID(domainID) ||
		activated.Targets[0].Backend != causal.BackendFileProvider ||
		len(activated.Targets[0].SignalTargets) != 1 ||
		!activated.Targets[0].SignalTargets[0].WorkingSet {
		t.Fatalf("targets = %+v", activated.Targets)
	}
	var outbox, signals uint64
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM activation_outbox WHERE activation_change_id = ?`, activated.ChangeID[:]).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM activation_outbox_signal_targets WHERE activation_change_id = ?`, activated.ChangeID[:]).Scan(&signals); err != nil {
		t.Fatal(err)
	}
	if outbox != 1 || signals != 1 {
		t.Fatalf("persisted targeting = outbox:%d signals:%d", outbox, signals)
	}
}

func TestReplacementKeepsOldActiveUntilFlipAndRetirement(t *testing.T) {
	c := newTestCatalog(t)
	first := lifecycleTestProvision(t, "replace", 1)
	if _, err := provisionTenantForTest(t, c, t.Context(), first); err != nil {
		t.Fatalf("provision first: %v", err)
	}
	before, err := c.TenantLifecycle(t.Context(), first.OwnerID, first.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.Generation = 2
	second.BackingRoot = filepath.Join(t.TempDir(), "replacement")
	intent, err := c.SetTenantPresent(t.Context(), tenantMutationForTest(t, first.OwnerID, before.Intent.Revision), second)
	if err != nil {
		t.Fatalf("SetTenantPresent replacement: %v", err)
	}
	if intent.Activation.ActiveGeneration != first.Generation || intent.Ready() {
		t.Fatalf("pre-flip state = %+v", intent)
	}
	state, lease, publication := stageExistingLifecycleForTest(t, c, intent)
	for _, backend := range state.Target.RequiredBackends.Backends() {
		state = recordBackendForTest(t, c, state, lease, backend)
	}
	activated, err := c.ActivateTenant(t.Context(), ActivateTenantRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   second.Tenant, Generation: second.Generation, ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
		ExpectedActivationRevision: state.Activation.Revision,
		ExpectedActiveGeneration:   first.Generation,
		ExpectedTargetingRevision:  mustTargetingRevision(t, c, second.Tenant),
		CausePublications:          []causal.OperationID{publication},
	})
	if err != nil {
		t.Fatalf("activate replacement: %v", err)
	}
	if activated.State.Activation.ActiveGeneration != second.Generation || activated.State.Ready() {
		t.Fatalf("replacement flip state = %+v", activated.State)
	}
	state = activated.State
	for _, row := range state.Presentations {
		if row.Generation == first.Generation && row.Phase == PresentationMaterializationRetiring {
			state, err = c.RetirePresentation(t.Context(), RetirementRequest{
				Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
				Tenant:   first.Tenant, Generation: first.Generation, Backend: row.Backend,
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	state, err = c.RetireApplication(t.Context(), RetirementRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   first.Tenant, Generation: first.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !state.Ready() {
		t.Fatalf("replacement not ready after retirement: %+v", state)
	}
}

func TestAbsentRetainsServingPointerUntilExactClear(t *testing.T) {
	c := newTestCatalog(t)
	definition := lifecycleTestProvision(t, "absent", 1)
	if _, err := provisionTenantForTest(t, c, t.Context(), definition); err != nil {
		t.Fatalf("provision: %v", err)
	}
	state, err := c.TenantLifecycle(t.Context(), definition.OwnerID, definition.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	state, err = c.SetTenantAbsent(t.Context(), tenantMutationForTest(t, state.OwnerID, state.Intent.Revision), definition.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent.Kind != TenantIntentAbsent || !state.Activation.Active() || !state.Activation.Retiring || state.Ready() {
		t.Fatalf("absent transition = %+v", state)
	}
	for _, row := range state.Presentations {
		if row.Generation == definition.Generation {
			state, err = c.RetirePresentation(t.Context(), RetirementRequest{
				Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
				Tenant:   definition.Tenant, Generation: definition.Generation, Backend: row.Backend,
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	state, err = c.RetireApplication(t.Context(), RetirementRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err = c.ClearTenantActivation(t.Context(), RetirementRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !state.Ready() || state.Activation.Active() {
		t.Fatalf("settled absence = %+v", state)
	}
}

func stageLifecycleForTest(
	t *testing.T,
	c *Catalog,
	definition TenantProvision,
) (TenantLifecycleState, StagedViewLease, causal.OperationID) {
	t.Helper()
	state, err := c.SetTenantPresent(t.Context(), tenantMutationForTest(t, definition.OwnerID, 0), definition)
	if err != nil {
		t.Fatal(err)
	}
	return stageExistingLifecycleForTest(t, c, state)
}

func stageExistingLifecycleForTest(
	t *testing.T,
	c *Catalog,
	state TenantLifecycleState,
) (TenantLifecycleState, StagedViewLease, causal.OperationID) {
	t.Helper()
	definition := state.Target.Definition
	if _, err := c.EnsureTenantNamespace(t.Context(), EnsureTenantNamespaceRequest{
		OwnerID: state.OwnerID, Tenant: definition.Tenant, Generation: definition.Generation,
		IntentRevision: state.Intent.Revision,
	}); err != nil {
		t.Fatal(err)
	}
	publication, digest, err := seedTenantPublicationForTest(t, c, t.Context(), definition)
	if err != nil {
		t.Fatal(err)
	}
	lease, state, err := c.StageApplication(t.Context(), StageApplicationRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
		Authority: causal.SourceAuthorityID(definition.ContentSourceID), Publication: publication,
		PublicationDigest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	return state, lease, publication
}

func recordBackendForTest(
	t *testing.T,
	c *Catalog,
	state TenantLifecycleState,
	lease StagedViewLease,
	backend TenantBackend,
) TenantLifecycleState {
	t.Helper()
	state, err := c.RecordPresentation(t.Context(), PresentationReceipt{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Lease:    lease, Backend: backend, BackendGeneration: "backend-generation",
		ObservedRevision: lease.CatalogHead,
	})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func mustTenantOperationID(t *testing.T) TenantOperationID {
	t.Helper()
	id, err := NewTenantOperationID()
	if err != nil {
		t.Fatalf("NewTenantOperationID: %v", err)
	}
	return id
}

func lifecycleTestProvision(t *testing.T, name string, generation Generation) TenantProvision {
	t.Helper()
	return testTenantProvision(t, name, generation)
}

func mustTargetingRevision(t *testing.T, c *Catalog, tenant TenantID) uint64 {
	t.Helper()
	revision, err := c.TenantTargetingRevision(t.Context(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func seedActivationTargetForTest(
	t *testing.T,
	c *Catalog,
	definition TenantProvision,
	lease StagedViewLease,
	domain causal.DomainID,
) {
	t.Helper()
	var root []byte
	if err := c.readDB.QueryRowContext(t.Context(), `SELECT root_id FROM tenants WHERE tenant = ?`,
		string(definition.Tenant)).Scan(&root); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO file_provider_domains(
    domain_id, tenant, owner_id, generation, root_id, access_mode,
    presentation_instance_id, display_name, public_path, activation_generation, registered
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', 'test-domain-runtime', 1)`, string(domain),
		string(definition.Tenant), definition.OwnerID, uint64(definition.Generation), root,
		uint8(definition.Access), definition.FileProvider.PresentationInstanceID,
		definition.FileProvider.DisplayName); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO file_provider_leases(lease_id, tenant, domain_id, generation, expires_unix_nano)
VALUES (?, ?, ?, ?, ?)`, "targeting-lease-"+string(definition.Tenant), string(definition.Tenant), string(domain),
		uint64(definition.Generation), time.Now().Add(time.Minute).UnixNano()); err != nil {
		t.Fatal(err)
	}
	rootID, err := objectID(root)
	if err != nil {
		t.Fatal(err)
	}
	seedEligibleMaterializedContainersForTargetTest(t, c, definition.Tenant, domain, definition.Generation, rootID)
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO changes(
    tenant, revision, scope_kind, presentation, scope_parent, scope_domain, scope_generation,
    sequence, kind, object_id, object_revision
) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, ?, 1)`, string(definition.Tenant), uint64(lease.CatalogHead),
		uint8(EnumerationWorkingSet), uint8(PresentationFileProvider), make([]byte, len(ObjectID{})),
		string(domain), uint64(definition.Generation), root); err != nil {
		t.Fatal(err)
	}
}

func seedEligibleMaterializedContainersForTargetTest(
	t *testing.T,
	c *Catalog,
	tenant TenantID,
	domain causal.DomainID,
	generation Generation,
	containers ...ObjectID,
) {
	t.Helper()
	snapshot, err := NewMaterializationSnapshotID()
	if err != nil {
		t.Fatal(err)
	}
	backing := []byte("target-test-backing:" + domain)
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO file_provider_materialization_owners(
    tenant, domain_id, generation, backing_store_identity, latest_epoch
) VALUES (?, ?, ?, ?, 1)`, string(tenant), string(domain), uint64(generation), backing); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO file_provider_materialization_heads(
    tenant, domain_id, generation, backing_store_identity, snapshot_id, epoch, revision, set_digest, eligible
) VALUES (?, ?, ?, ?, ?, 1, 1, ?, 1)`, string(tenant), string(domain), uint64(generation), backing,
		snapshot[:], make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	for _, container := range containers {
		if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO file_provider_materialized_containers(
    tenant, domain_id, generation, backing_store_identity, container_id
) VALUES (?, ?, ?, ?, ?)`, string(tenant), string(domain), uint64(generation), backing, container[:]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE tenant_targeting_heads SET revision = revision + 1 WHERE tenant_id = ?`, string(tenant)); err != nil {
		t.Fatal(err)
	}
}
