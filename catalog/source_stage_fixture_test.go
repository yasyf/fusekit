package catalog

import (
	"crypto/sha256"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func sourceChange(revision uint64) causal.ChangeSet {
	var changeID causal.ChangeID
	var operationID causal.OperationID
	changeID[0], changeID[15] = byte(revision), 0x3a
	operationID[0], operationID[15] = byte(revision), 0x7c
	return causal.ChangeSet{
		SourceAuthority: "test-source", SourceRevision: causal.Revision(revision),
		ChangeID: changeID, OperationID: operationID, Cause: causal.CauseExternalUnattributed,
		AffectedKeys: []causal.LogicalKey{"config"},
	}
}

func sourceSnapshotChangeAtDriverHeadForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
) causal.ChangeSet {
	t.Helper()
	var revision uint64
	var operation, change []byte
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT head.source_revision, publication.source_operation_id, publication.change_id
FROM source_driver_publication_heads head
JOIN source_driver_publications publication
  ON publication.source_authority = head.source_authority
 AND publication.publication_id = head.publication_id
WHERE head.source_authority = ?`, string(authority)).Scan(&revision, &operation, &change); err != nil {
		t.Fatalf("read source driver head for snapshot: %v", err)
	}
	if len(operation) != len(causal.OperationID{}) || len(change) != len(causal.ChangeID{}) {
		t.Fatal("source driver head has invalid causal identity")
	}
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_watermarks(source_authority, source_revision, change_id, operation_id)
VALUES (?, ?, ?, ?)
ON CONFLICT(source_authority) DO UPDATE SET
    source_revision = excluded.source_revision,
    change_id = excluded.change_id,
    operation_id = excluded.operation_id`, string(authority), revision, change, operation); err != nil {
		t.Fatalf("align source snapshot watermark to driver head: %v", err)
	}
	result := sourceChange(revision + 1)
	result.SourceAuthority = authority
	result.AffectedKeys = nil
	return result
}

func sourceRootKey(provision TenantProvision) SourceObjectKey {
	return SourceObjectKey("root:" + string(provision.Tenant))
}

func provisionSourceMutationTenant(t *testing.T, c *Catalog, name string) TenantProvision {
	t.Helper()
	provision, err := provisionTenantForTest(t, c, t.Context(), testTenantProvision(t, name, 1))
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	return provision
}

func mustSourceObject(t *testing.T, c *Catalog, tenant TenantID, id ObjectID) Object {
	t.Helper()
	object, err := c.Lookup(t.Context(), tenant, PresentationMount, id)
	if err != nil {
		t.Fatal(err)
	}
	return object
}

func beginClaimedSourceMutation(
	t *testing.T,
	c *Catalog,
	tenant TenantID,
	intent MutationIntent,
) PreparedMutation {
	t.Helper()
	head, err := c.Head(t.Context(), tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	started, err := c.BeginMutation(t.Context(), tenant, head, intent)
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	prepared, err := c.ClaimMutation(t.Context(), started.OperationID, mustMutationOwner(t))
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	return prepared
}

func commitAuthoritativeMutationForTest(
	t *testing.T,
	c *Catalog,
	provision TenantProvision,
	declaration [sha256.Size]byte,
	targets []SourceDriverTarget,
	intent MutationIntent,
	resultKey SourceObjectKey,
	entries []SourceDriverStageEntry,
	toToken string,
	operationByte byte,
) SourceDriverStageResult {
	t.Helper()
	prepared := beginClaimedSourceMutation(t, c, provision.Tenant, intent)
	prepared, err := c.PrepareMutationSource(t.Context(), prepared.OperationID, *prepared.Claim)
	if err != nil {
		t.Fatalf("PrepareMutationSource: %v", err)
	}
	prepared, err = c.SetMutationSourceResult(t.Context(), prepared.OperationID, *prepared.Claim, SourceLocator{
		SourceAuthority: causal.SourceAuthorityID(provision.ContentSourceID),
		SourceRevision:  prepared.Source.Parent.SourceRevision,
		SourceKey:       resultKey,
	})
	if err != nil {
		t.Fatalf("SetMutationSourceResult: %v", err)
	}
	checkpoint, err := c.SourceDriverCheckpoint(t.Context(), causal.SourceAuthorityID(provision.ContentSourceID))
	if err != nil {
		t.Fatalf("SourceDriverCheckpoint: %v", err)
	}
	var predecessor uint64
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT source_revision FROM source_driver_publication_heads WHERE source_authority = ?`,
		provision.ContentSourceID).Scan(&predecessor); err != nil {
		t.Fatalf("source driver head: %v", err)
	}
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverMutation, 0,
		checkpoint.Token, toToken, causal.Revision(predecessor), operationByte,
	)
	identity.Authority = causal.SourceAuthorityID(provision.ContentSourceID)
	identity.FleetOwner = SourceAuthorityFleetOwnerID(provision.OwnerID)
	identity.Cause = intent.Origin.Cause
	identity.Origin = intent.Origin.Domain
	identity.OriginGeneration = intent.Origin.Generation
	identity.Mutation = prepared.OperationID
	identity.MutationTenant = provision.Tenant
	identity.MutationGeneration = provision.Generation
	identity.MutationResult = resultKey
	identity.MutationRequestDigest = sha256.Sum256([]byte("request:" + toToken))
	identity.MutationReceiptDigest = sha256.Sum256([]byte("receipt:" + toToken))
	identity.Claim = *prepared.Claim
	reserveSourceDriverMutationForTest(t, c, identity)
	if err := c.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatalf("BeginSourceDriverStage(mutation): %v", err)
	}
	pageDigest := sha256.Sum256([]byte("page:" + toToken))
	stage, err := c.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{
			Digest: pageDigest, Complete: true, Entries: entries,
		},
	))
	if err != nil {
		t.Fatalf("AppendSourceDriverStage(mutation): %v", err)
	}
	prepareSourceDriverPublicationForTest(t, c, identity)
	result, err := c.CommitSourceDriverMutation(t.Context(), stage)
	if err != nil {
		t.Fatalf("CommitSourceDriverMutation: %v", err)
	}
	return result
}

func TestAuthoritativeMutationFixtureCommitsThroughSourceDriver(t *testing.T) {
	store, provisions, declaration, targets := newSourceDriverCatalog(t, "authoritative-mutation")
	reset := sourceDriverIdentityAtHeadForTest(
		t, store, declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotReset,
		"", "reset-token", 91,
	)
	if err := store.BeginSourceDriverStage(t.Context(), reset); err != nil {
		t.Fatal(err)
	}
	stage, err := store.AppendSourceDriverStage(t.Context(), reset, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{
			Digest: sha256.Sum256([]byte("authoritative-reset")), Complete: true,
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	prepareSourceDriverPublicationForTest(t, store, reset)
	resetResult, err := store.CommitSourceDriverStage(t.Context(), stage)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeSourceDriverCommittedReceipt(t.Context(), resetResult); err != nil {
		t.Fatal(err)
	}
	if err := store.ForgetSourceDriverCommittedReceipt(t.Context(), resetResult); err != nil {
		t.Fatal(err)
	}
	provision := provisions[0]
	result := commitAuthoritativeMutationForTest(
		t, store, provision, declaration, targets,
		MutationIntent{
			SourceID: "driver", Origin: CausalOrigin{Cause: causal.CauseDaemonWrite}, Disposition: MutationDispositionNamespace,
			Create: &CreateMutation{Spec: CreateSpec{
				Parent: provision.Root, Name: "created", Kind: KindDirectory, Mode: 0o700,
				Visibility: Visibility{Mount: true, FileProvider: true},
			}},
		},
		"created",
		[]SourceDriverStageEntry{{
			Tenant: provision.Tenant, Generation: provision.Generation,
			ChangeSequence: 1, Key: "created",
			Object: &SourceObject{
				Key: "created", Name: "created", Kind: KindDirectory, Mode: 0o700,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
		"mutation-token", 92,
	)
	if result.MutationResult == nil || result.MutationResult.Namespace == nil ||
		result.MutationResult.Namespace.Primary.Name != "created" {
		t.Fatalf("authoritative mutation result = %+v", result.MutationResult)
	}
}

func sourceResultsEqual(left, right SourceResult) bool {
	return left == right
}

func sourceResultCommits(t *testing.T, c *Catalog, result SourceResult) []causal.CatalogCommit {
	t.Helper()
	var commits []causal.CatalogCommit
	var cursor SourceCommitCursor
	for {
		page, err := c.SourceCommits(t.Context(), result, cursor, SourceCommitPageLimit)
		if err != nil {
			t.Fatal(err)
		}
		commits = append(commits, page.Commits...)
		if page.Next == nil {
			return commits
		}
		cursor = *page.Next
	}
}
