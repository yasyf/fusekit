package catalog

import (
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

func sourceRootKey(provision TenantProvision) SourceObjectKey {
	return SourceObjectKey("root:" + string(provision.Tenant))
}

func provisionSourceMutationTenant(t *testing.T, c *Catalog, name string) TenantProvision {
	t.Helper()
	provision, err := c.ProvisionTenant(t.Context(), testTenantProvision(t, name, 1))
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
