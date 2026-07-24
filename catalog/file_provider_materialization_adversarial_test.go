package catalog

import (
	"errors"
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
)

func TestFileProviderMaterializationPagesAreImmutableCompleteAndReplayExact(t *testing.T) {
	c, created, domain := newMaterializationFixture(t, "materialization-pages")
	identity := newMaterializationIdentity(t, created, domain, "backing")
	if _, err := c.BeginFileProviderMaterializationSnapshot(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	stage := func(sequence uint32, values ...ObjectID) error {
		return c.StageFileProviderMaterializationPage(t.Context(), FileProviderMaterializationPage{
			Identity: identity, Sequence: sequence, IDs: values,
		})
	}
	if err := stage(1, created.Root); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("page gap = %v, want ErrInvalidTransition", err)
	}
	if err := stage(0, created.Root); err != nil {
		t.Fatal(err)
	}
	if err := stage(0, created.Root); err != nil {
		t.Fatalf("exact page replay: %v", err)
	}
	if err := stage(0); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("conflicting page replay = %v, want ErrMutationConflict", err)
	}
	if _, err := c.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: identity, PageCount: 2,
	}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("incomplete commit = %v, want ErrInvalidTransition", err)
	}
	result, err := c.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: identity, PageCount: 1,
	})
	if err != nil || result.Added != 1 || result.Removed != 0 {
		t.Fatalf("complete commit = %+v, %v", result, err)
	}
	if err := stage(0, created.Root); err != nil {
		t.Fatalf("committed page exact replay: %v", err)
	}
	if err := stage(0); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("committed page conflict = %v, want ErrMutationConflict", err)
	}
	if _, err := c.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: identity, PageCount: 2,
	}); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("committed receipt conflict = %v, want ErrMutationConflict", err)
	}
}

func TestFileProviderMaterializationCommitNeverCreatesCatalogChangesOrNotifications(t *testing.T) {
	c, created, domain := newMaterializationFixture(t, "materialization-no-notify")
	beforeHead := mustCatalogHead(t, c, created.Tenant)
	beforeChanges := countCatalogRows(t, c, "changes")
	beforeOutbox := countCatalogRows(t, c, "activation_outbox")
	beforeSignals := countCatalogRows(t, c, "activation_outbox_signal_targets")
	commitMaterializationForTest(t, c, newMaterializationIdentity(t, created, domain, "backing"), created.Root)
	if got := mustCatalogHead(t, c, created.Tenant); got != beforeHead {
		t.Fatalf("catalog head = %d, want %d", got, beforeHead)
	}
	if got := countCatalogRows(t, c, "changes"); got != beforeChanges {
		t.Fatalf("changes = %d, want %d", got, beforeChanges)
	}
	if got := countCatalogRows(t, c, "activation_outbox"); got != beforeOutbox {
		t.Fatalf("activation outbox = %d, want %d", got, beforeOutbox)
	}
	if got := countCatalogRows(t, c, "activation_outbox_signal_targets"); got != beforeSignals {
		t.Fatalf("activation signal targets = %d, want %d", got, beforeSignals)
	}
}

func TestTenantActivationRequiresLiveLeaseAndEligibleMaterializedSet(t *testing.T) {
	tests := []struct {
		name       string
		leaseUntil time.Duration
		membership string
		wantTarget bool
	}{
		{name: "neither"},
		{name: "live-lease-only", leaseUntil: time.Minute},
		{name: "membership-only", membership: "eligible"},
		{name: "expired-lease", leaseUntil: -time.Minute, membership: "eligible"},
		{name: "suspended-membership", leaseUntil: time.Minute, membership: "suspended"},
		{name: "both", leaseUntil: time.Minute, membership: "eligible", wantTarget: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := newTestCatalog(t)
			definition := lifecycleTestProvision(t, "target-matrix-"+test.name, 1)
			state, lease, publication := stageLifecycleForTest(t, c, definition)
			for _, backend := range state.Target.RequiredBackends.Backends() {
				state = recordBackendForTest(t, c, state, lease, backend)
			}
			domain := causal.DomainID("target-matrix-domain-" + test.name)
			seedAdversarialDomainAndWorkingSetChange(t, c, definition, lease, domain)
			if test.leaseUntil != 0 {
				var root []byte
				if err := c.readDB.QueryRowContext(t.Context(), `SELECT root_id FROM tenants WHERE tenant = ?`,
					string(definition.Tenant)).Scan(&root); err != nil {
					t.Fatal(err)
				}
				if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO file_provider_leases(
    lease_id, tenant, domain_id, generation, root_id, presentation_instance_id,
    state, session_id, process_identity, policy_digest, resolution_digest,
    catalog_head, source_authority, source_publication, source_revision,
    activation_generation, expires_unix_nano
) VALUES (?, ?, ?, ?, ?, ?, 2, 'target-matrix-session', 'target-matrix-process',
    zeroblob(32), zeroblob(32), ?, ?, zeroblob(16), 1, 'target-matrix-activation', ?)`,
					"target-matrix-lease-"+test.name, string(definition.Tenant), string(domain),
					uint64(definition.Generation), root, definition.FileProvider.PresentationInstanceID,
					uint64(lease.CatalogHead), definition.ContentSourceID,
					time.Now().Add(test.leaseUntil).UnixNano()); err != nil {
					t.Fatal(err)
				}
			}
			if test.membership != "" {
				var root []byte
				if err := c.readDB.QueryRowContext(t.Context(), `SELECT root_id FROM tenants WHERE tenant = ?`,
					string(definition.Tenant)).Scan(&root); err != nil {
					t.Fatal(err)
				}
				rootID, err := objectID(root)
				if err != nil {
					t.Fatal(err)
				}
				seedEligibleMaterializedContainersForTargetTest(t, c, definition.Tenant, domain,
					definition.Generation, rootID)
				if test.membership == "suspended" {
					if err := c.SuspendFileProviderMaterialization(t.Context(), definition.Tenant, domain,
						definition.Generation); err != nil {
						t.Fatal(err)
					}
				}
			}
			activated, err := c.ActivateTenant(t.Context(), ActivateTenantRequest{
				Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
				Tenant:   definition.Tenant, Generation: definition.Generation,
				ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
				ExpectedActivationRevision: state.Activation.Revision,
				ExpectedTargetingRevision:  mustTargetingRevision(t, c, definition.Tenant),
				CausePublications:          []causal.OperationID{publication},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := len(activated.Targets); (got == 1) != test.wantTarget {
				t.Fatalf("target count = %d, wantTarget=%t", got, test.wantTarget)
			}
		})
	}
}

func countCatalogRows(t *testing.T, c *Catalog, table string) uint64 {
	t.Helper()
	query := map[string]string{
		"changes":                          `SELECT COUNT(*) FROM changes`,
		"activation_outbox":                `SELECT COUNT(*) FROM activation_outbox`,
		"activation_outbox_signal_targets": `SELECT COUNT(*) FROM activation_outbox_signal_targets`,
	}[table]
	if query == "" {
		t.Fatalf("unsupported table %q", table)
	}
	var count uint64
	if err := c.readDB.QueryRowContext(t.Context(), query).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func seedAdversarialDomainAndWorkingSetChange(
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
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', 'adversarial-domain-runtime', 1)`, string(domain),
		string(definition.Tenant), definition.OwnerID, uint64(definition.Generation), root,
		uint8(definition.Access), definition.FileProvider.PresentationInstanceID,
		definition.FileProvider.DisplayName); err != nil {
		t.Fatal(err)
	}
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
