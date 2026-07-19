package catalog

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceSnapshotObjectIdentityIsOpaqueDurableAndReplayStable(t *testing.T) {
	database := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	authority := causal.SourceAuthorityID("opaque-snapshot-identity")
	configureSourceObserverForIndexTest(t, c, authority)
	identity, page := sourceSnapshotIdentityPageForTest(
		t, c, authority, "initial", sourceChangeForAuthority(1, authority),
		"logical", "private-value-key",
	)
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, identity.Snapshot); err != nil {
		t.Fatal(err)
	}
	stageSourceSnapshotIdentityInputForTest(t, c, authority, identity.Snapshot)
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, page)
	if err != nil {
		t.Fatal(err)
	}
	staged := sourceSnapshotBindingIDForTest(t, c, authority, identity.Snapshot, "logical")
	var durableCount int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_object_ids
WHERE source_authority = ? AND source_key = ?`,
		string(authority), string(page.Bindings[0].SourceKey)).Scan(&durableCount); err != nil {
		t.Fatal(err)
	}
	if durableCount != 0 {
		t.Fatal("staged opaque identity escaped into durable source bindings before promotion")
	}
	if bytes.Contains(staged[:], []byte("private")) || bytes.Contains(staged[:], []byte("value-key")) {
		t.Fatalf("opaque object ID contains source-key bytes: %x", staged)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	replayed, err := c.AppendSourceSnapshotPublication(t.Context(), identity, page)
	if err != nil || replayed != ref {
		t.Fatalf("crash replay ref = %+v, %v; want %+v", replayed, err, ref)
	}
	if got := sourceSnapshotBindingIDForTest(t, c, authority, identity.Snapshot, "logical"); got != staged {
		t.Fatalf("crash replay changed staged identity: %x != %x", got, staged)
	}
	if _, err := c.PromoteSourceSnapshot(
		t.Context(), ref, sourceSnapshotSettlementForTest(ref, 0),
	); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT object_id FROM source_object_ids
WHERE source_authority = ? AND source_key = ?`,
		string(authority), string(page.Bindings[0].SourceKey)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	durable, err := objectID(raw)
	if err != nil {
		t.Fatal(err)
	}
	if durable != staged {
		t.Fatalf("promoted identity = %x, want staged %x", durable, staged)
	}
	replacement := identity
	replacement.Snapshot = "replacement"
	replacement.FenceDigest = sourceSnapshotFenceDigestForTest(t, c, authority)
	replacement.Change = sourceChangeForAuthority(2, authority)
	replacementPage := page
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, replacement.Snapshot); err != nil {
		t.Fatal(err)
	}
	stageSourceSnapshotIdentityInputForTest(t, c, authority, replacement.Snapshot)
	if err := c.BeginSourceSnapshotPublication(t.Context(), replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AppendSourceSnapshotPublication(t.Context(), replacement, replacementPage); err != nil {
		t.Fatal(err)
	}
	if got := sourceSnapshotBindingIDForTest(t, c, authority, replacement.Snapshot, "logical"); got != durable {
		t.Fatalf("replacement changed durable logical identity: %x != %x", got, durable)
	}
}

func TestAbortedSourceSnapshotDoesNotPromoteOpaqueIdentityReservation(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("aborted-snapshot-identity")
	configureSourceObserverForIndexTest(t, c, authority)
	identity, page := sourceSnapshotIdentityPageForTest(
		t, c, authority, "aborted", sourceChangeForAuthority(1, authority),
		"logical", "never-promoted",
	)
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, identity.Snapshot); err != nil {
		t.Fatal(err)
	}
	stageSourceSnapshotIdentityInputForTest(t, c, authority, identity.Snapshot)
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AppendSourceSnapshotPublication(t.Context(), identity, page); err != nil {
		t.Fatal(err)
	}
	_ = sourceSnapshotBindingIDForTest(t, c, authority, identity.Snapshot, "logical")
	if err := c.AbortSourceSnapshotPublication(t.Context(), authority, identity.Snapshot); err != nil {
		t.Fatal(err)
	}
	var durable, staged int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT
  (SELECT COUNT(*) FROM source_object_ids
   WHERE source_authority = ? AND source_key = ?),
  (SELECT COUNT(*) FROM source_snapshot_bindings
   WHERE source_authority = ? AND snapshot_id = ?)`,
		string(authority), "never-promoted", string(authority), identity.Snapshot).
		Scan(&durable, &staged); err != nil {
		t.Fatal(err)
	}
	if durable != 0 || staged != 0 {
		t.Fatalf("aborted identity residue durable=%d staged=%d", durable, staged)
	}
}

func sourceChangeForAuthority(revision causal.Revision, authority causal.SourceAuthorityID) causal.ChangeSet {
	change := sourceChange(uint64(revision))
	change.SourceAuthority = authority
	change.AffectedKeys = nil
	return change
}

func sourceSnapshotIdentityPageForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	snapshot string,
	change causal.ChangeSet,
	logical string,
	key SourceObjectKey,
) (SourceSnapshotIdentity, SourceSnapshotPublicationPage) {
	t.Helper()
	provisionSpec := testTenantProvision(t, "snapshot-identity-"+string(authority), 1)
	provisionSpec.ContentSourceID = string(authority)
	provision, err := c.ProvisionTenant(t.Context(), provisionSpec)
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	return SourceSnapshotIdentity{
			Authority: authority, AuthorityGeneration: 1, Snapshot: snapshot,
			FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority),
			Change:      change,
		}, SourceSnapshotPublicationPage{
			AffectedKeys: []causal.LogicalKey{"identity"},
			Roots: []SourceSnapshotRoot{{
				Tenant: provision.Tenant, Generation: provision.Generation,
				LogicalID: logical, RootKey: key,
			}},
			Bindings: []SourceSnapshotBinding{{
				LogicalID: logical, SourceKey: key,
				Inputs: []SourceIndexLocator{{RootID: "root", Relative: "input"}},
			}},
		}
}

func stageSourceSnapshotIdentityInputForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	snapshot string,
) {
	t.Helper()
	if err := c.AppendSourceSnapshotStagePage(
		t.Context(), authority, snapshot, SourceSnapshotPage{
			Records: []SourcePhysicalIndexRecord{{
				Authority: authority, RootID: "root", Relative: "input",
				FileIdentity: []byte("input-identity"), Kind: uint8(KindFile), Payload: []byte("input"),
			}},
			Next: SourceIndexLocator{RootID: "root", Relative: "input"},
		},
	); err != nil {
		t.Fatal(err)
	}
}

func sourceSnapshotBindingIDForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	snapshot string,
	logical string,
) ObjectID {
	t.Helper()
	var raw []byte
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT object_id FROM source_snapshot_bindings
WHERE source_authority = ? AND snapshot_id = ? AND logical_id = ?`,
		string(authority), snapshot, logical).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	id, err := objectID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
