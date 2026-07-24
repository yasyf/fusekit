package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
)

func sourceSnapshotFenceDigestForTest(t *testing.T, c *Catalog, authority causal.SourceAuthorityID) [32]byte {
	t.Helper()
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	proof, err := currentSourceSnapshotFenceProof(t.Context(), tx, authority)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := digestSourceSnapshotFence(proof)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func sourceSnapshotSettlementForTest(ref SourceSnapshotStageRef, through uint64) SourceSnapshotSettlement {
	return SourceSnapshotSettlement{
		Fence: SourceObserverSettlement{
			Authority: ref.Authority, Stream: "stream", RootEpoch: "epoch", Through: through,
			Operation: ref.Operation,
		},
		Snapshot: ref,
	}
}

func sourceSnapshotObjectForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	tenant TenantID,
	key SourceObjectKey,
) Object {
	t.Helper()
	object, err := scanObject(c.readDB.QueryRowContext(t.Context(), `SELECT `+objectColumns+`
FROM objects
WHERE tenant = ? AND object_id = (
    SELECT identity.object_id
    FROM source_object_ids identity
    JOIN source_object_bindings binding
      ON binding.source_authority = identity.source_authority
     AND binding.source_key = identity.source_key
    WHERE identity.source_authority = ? AND binding.tenant = ? AND identity.source_key = ?
)`, string(tenant), string(authority), string(tenant), string(key)))
	if err != nil {
		t.Fatalf("read promoted snapshot object %q: %v", key, err)
	}
	return object
}

func TestSourceSnapshotBeginRejectsForgedFenceWithoutResidue(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("forged-snapshot-fence")
	configureSourceObserverForIndexTest(t, c, authority)
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "forged"); err != nil {
		t.Fatal(err)
	}
	change := sourceChange(1)
	change.SourceAuthority = authority
	change.AffectedKeys = nil
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1,
		Snapshot: "forged", FenceDigest: [32]byte{0xff}, Change: change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("BeginSourceSnapshotPublication(forged fence) = %v, want conflict", err)
	}
	var publications, checkpoints int
	if err := c.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM source_snapshot_publications`).Scan(&publications); err != nil {
		t.Fatal(err)
	}
	if err := c.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM source_snapshot_fence_checkpoints`).Scan(&checkpoints); err != nil {
		t.Fatal(err)
	}
	if publications != 0 || checkpoints != 0 {
		t.Fatalf("forged fence residue = publications %d checkpoints %d", publications, checkpoints)
	}
}

func TestSourceSnapshotBeginRejectsAuthorityGenerationMismatchWithoutResidue(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("snapshot-generation-mismatch")
	configureSourceObserverForIndexTest(t, c, authority)
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "generation"); err != nil {
		t.Fatal(err)
	}
	change := sourceChange(1)
	change.SourceAuthority = authority
	change.AffectedKeys = nil
	identity := SourceSnapshotIdentity{
		Authority: authority, Snapshot: "generation",
		FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority), Change: change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("BeginSourceSnapshotPublication(zero generation) = %v, want invalid object", err)
	}
	identity.AuthorityGeneration = 2
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("BeginSourceSnapshotPublication(wrong generation) = %v, want conflict", err)
	}
	var publications int
	if err := c.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM source_snapshot_publications`).Scan(&publications); err != nil {
		t.Fatal(err)
	}
	if publications != 0 {
		t.Fatalf("generation mismatch left %d publication rows", publications)
	}
}

func TestSourceSnapshotBeginReplayAndPromotionUseCapturedFence(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("captured-snapshot-fence")
	configureSourceObserverForIndexTest(t, c, authority)
	provisionSpec := testTenantProvision(t, "captured-snapshot-fence", 1)
	provisionSpec.ContentSourceID = string(authority)
	provision, err := provisionTenantForTest(t, c, t.Context(), provisionSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_driver_target_epochs(source_authority, target_epoch) VALUES (?, 1)
ON CONFLICT(source_authority) DO NOTHING`, string(authority)); err != nil {
		t.Fatal(err)
	}
	var causalPublication, causalOperation, causalChange []byte
	var causalRevision uint64
	if err := c.db.QueryRowContext(t.Context(), `
SELECT publication_id, source_operation_id, change_id, source_revision
FROM source_driver_publications WHERE source_authority = ?`, string(authority)).Scan(
		&causalPublication, &causalOperation, &causalChange, &causalRevision,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_driver_publication_heads(source_authority, publication_id, source_revision, epoch)
VALUES (?, ?, ?, 1)
ON CONFLICT(source_authority) DO NOTHING`, string(authority), causalPublication, causalRevision); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_watermarks(source_authority, source_revision, change_id, operation_id)
VALUES (?, ?, ?, ?)`, string(authority), causalRevision, causalChange, causalOperation); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "captured"); err != nil {
		t.Fatal(err)
	}
	change := sourceChange(2)
	change.SourceAuthority = authority
	change.AffectedKeys = nil
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1,
		Snapshot: "captured", FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority), Change: change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	wrongGeneration := identity
	wrongGeneration.AuthorityGeneration++
	if _, err := c.AppendSourceSnapshotPublication(t.Context(), wrongGeneration, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"repair"},
	}); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("AppendSourceSnapshotPublication(wrong generation) = %v, want conflict", err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_observer_checkpoints SET native_event_id = native_event_id + 1
WHERE source_authority = ?`, string(authority)); err != nil {
		t.Fatal(err)
	}
	if current := sourceSnapshotFenceDigestForTest(t, c, authority); current == identity.FenceDigest {
		t.Fatal("test checkpoint drift did not change the current source fence")
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatalf("exact Begin replay after current fence drift: %v", err)
	}
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"repair"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation,
			LogicalID: "captured-root", RootKey: "captured-root",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.PromoteSourceSnapshot(t.Context(), ref, sourceSnapshotSettlementForTest(ref, 0))
	if err != nil {
		t.Fatalf("promotion with captured fence proof: %v", err)
	}
	if result.Revision != identity.Change.SourceRevision || len(sourceResultCommits(t, c, result)) != 1 {
		t.Fatalf("captured-fence result = %+v", result)
	}
	var sourceOperation, changeID, affectedDigest []byte
	var cause string
	var affectedCount uint64
	if err := c.db.QueryRowContext(t.Context(), `
SELECT source_operation_id, change_id, cause, affected_key_count, affected_keys_digest
FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ?`, string(authority), ref.Operation[:]).Scan(
		&sourceOperation, &changeID, &cause, &affectedCount, &affectedDigest,
	); err != nil {
		t.Fatal(err)
	}
	encodedAffected, err := json.Marshal([]causal.LogicalKey{"repair"})
	if err != nil {
		t.Fatal(err)
	}
	wantAffectedDigest := sha256.Sum256(encodedAffected)
	if !bytes.Equal(sourceOperation, identity.Change.OperationID[:]) ||
		!bytes.Equal(changeID, identity.Change.ChangeID[:]) || cause != string(identity.Change.Cause) ||
		affectedCount != 1 || !bytes.Equal(affectedDigest, wantAffectedDigest[:]) {
		t.Fatalf("snapshot anchor causal identity = operation %x, change %x, cause %q, affected %d/%x",
			sourceOperation, changeID, cause, affectedCount, affectedDigest)
	}
}

func TestSourceSnapshotBeginRejectsWatermarkGapWithoutResidue(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("snapshot-watermark-gap")
	configureSourceObserverForIndexTest(t, c, authority)
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "gap"); err != nil {
		t.Fatal(err)
	}
	change := sourceChange(2)
	change.SourceAuthority = authority
	change.AffectedKeys = nil
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1, Snapshot: "gap",
		FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority),
		Change:      change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); !errors.Is(err, ErrSourcePredecessor) {
		t.Fatalf("BeginSourceSnapshotPublication(gapped revision) = %v, want predecessor error", err)
	}
	watermark, err := c.SourceWatermark(t.Context(), authority)
	if err != nil || watermark != 0 {
		t.Fatalf("watermark after rejected gap = %d, %v", watermark, err)
	}
	var publications int
	if err := c.db.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_snapshot_publications WHERE source_authority = ?`,
		string(authority)).Scan(&publications); err != nil {
		t.Fatal(err)
	}
	if publications != 0 {
		t.Fatalf("gapped snapshot left %d publication rows", publications)
	}
	if err := c.AbortSourceSnapshotStage(t.Context(), authority, "gap"); err != nil {
		t.Fatal(err)
	}
}

func TestSourceSnapshotPromotionRejectsWatermarkDrift(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("snapshot-watermark-drift")
	configureSourceObserverForIndexTest(t, c, authority)
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "drift"); err != nil {
		t.Fatal(err)
	}
	change := sourceChange(1)
	change.SourceAuthority = authority
	change.AffectedKeys = nil
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1, Snapshot: "drift",
		FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority),
		Change:      change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"repair"},
	})
	if err != nil {
		t.Fatal(err)
	}
	driftChange := causal.ChangeID{0xff}
	driftOperation := causal.OperationID{0xff}
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_watermarks(source_authority, source_revision, change_id, operation_id)
VALUES (?, 1, ?, ?)`, string(authority), driftChange[:], driftOperation[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PromoteSourceSnapshot(
		t.Context(), ref, sourceSnapshotSettlementForTest(ref, 0),
	); !errors.Is(err, ErrSourcePredecessor) {
		t.Fatalf("PromoteSourceSnapshot(drifted watermark) = %v, want predecessor error", err)
	}
	watermark, err := c.SourceWatermark(t.Context(), authority)
	if err != nil || watermark != 1 {
		t.Fatalf("watermark after rejected drift = %d, %v", watermark, err)
	}
	if err := c.AbortSourceSnapshotPublication(t.Context(), authority, "drift"); err != nil {
		t.Fatal(err)
	}
}

func promoteObserverSnapshotForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	snapshot string,
	through uint64,
	mismatchAllActive bool,
) SourceSnapshotStageRef {
	t.Helper()
	provisionSpec := testTenantProvision(t, snapshot, 1)
	provisionSpec.ContentSourceID = string(authority)
	provision, err := provisionTenantForTest(t, c, t.Context(), provisionSpec)
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	change := sourceSnapshotChangeAtDriverHeadForTest(t, c, authority)
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1,
		Snapshot: snapshot, FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority), Change: change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatalf("BeginSourceSnapshotPublication: %v", err)
	}
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"repair"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "root", RootKey: "root-key",
		}},
	})
	if err != nil {
		t.Fatalf("AppendSourceSnapshotPublication: %v", err)
	}
	settlement := sourceSnapshotSettlementForTest(ref, through)
	settlement.MismatchAllActive = mismatchAllActive
	if _, err := c.PromoteSourceSnapshot(t.Context(), ref, settlement); err != nil {
		t.Fatalf("PromoteSourceSnapshot: %v", err)
	}
	return ref
}

func TestStagedSourceSnapshotOwnsPagesAndPromotesWithoutFleetPayload(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("test-source")
	configureSourceObserverForIndexTest(t, c, authority)
	provision, err := provisionTenantForTest(t, c, t.Context(), testTenantProvision(t, "staged-snapshot", 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "snapshot"); err != nil {
		t.Fatal(err)
	}
	if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, "snapshot", SourceSnapshotPage{
		Records: []SourcePhysicalIndexRecord{{
			Authority: authority, RootID: "root", Relative: "settings.json", FileIdentity: []byte("identity"),
			Kind: 2, Payload: []byte(`{"Exists":true}`),
		}},
		Next: SourceIndexLocator{RootID: "root", Relative: "settings.json"},
	}); err != nil {
		t.Fatal(err)
	}
	rootBinding, err := c.ReserveSourceAuthorityBinding(t.Context(), authority, "root-logical", "root-key")
	if err != nil {
		t.Fatal(err)
	}
	valueBinding, err := c.ReserveSourceAuthorityBinding(t.Context(), authority, "settings", "settings-key")
	if err != nil {
		t.Fatal(err)
	}
	change := sourceChange(1)
	change.AffectedKeys = nil
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1,
		Snapshot: "snapshot", FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority), Change: change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	first := stageTestContent(t, c, "value")
	page := SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"settings"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation,
			LogicalID: rootBinding.LogicalID, RootKey: rootBinding.SourceKey,
		}},
		Bindings: []SourceSnapshotBinding{{
			LogicalID: valueBinding.LogicalID, SourceKey: valueBinding.SourceKey,
			Fingerprint: [32]byte{1}, Inputs: []SourceIndexLocator{{RootID: "root", Relative: "settings.json"}},
		}},
		Objects: []SourceSnapshotProjection{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: valueBinding.LogicalID,
			Object: SourceObject{
				Key: valueBinding.SourceKey, Name: "settings.json", Kind: KindFile, Mode: 0o600,
				ContentRevision: 1, Content: first, Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	}
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, page)
	if err != nil {
		t.Fatal(err)
	}
	originalReplay, err := c.AppendSourceSnapshotPublication(t.Context(), identity, page)
	if err != nil || originalReplay != ref {
		t.Fatalf("original page replay = %+v, %v; want %+v", originalReplay, err, ref)
	}
	var claimedBy []byte
	if err := c.db.QueryRowContext(t.Context(), `SELECT source_operation_id FROM content_stages WHERE stage_id = ?`,
		first.Stage[:]).Scan(&claimedBy); err != nil || !bytes.Equal(claimedBy, change.OperationID[:]) {
		t.Fatalf("original page replay ownership = %x, %v", claimedBy, err)
	}
	second := stageTestContent(t, c, "value")
	replay := page
	replay.Objects = append([]SourceSnapshotProjection(nil), page.Objects...)
	replay.Objects[0].Object.Content = second
	replayed, err := c.AppendSourceSnapshotPublication(t.Context(), identity, replay)
	if err != nil || replayed != ref {
		t.Fatalf("lost-response page replay = %+v, %v; want %+v", replayed, err, ref)
	}
	var retryStage int
	if err := c.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM content_stages WHERE stage_id = ?`, second.Stage[:]).Scan(&retryStage); err != nil || retryStage != 0 {
		t.Fatalf("replayed page retained caller stage = %d, %v", retryStage, err)
	}
	settlement := sourceSnapshotSettlementForTest(ref, 0)
	result, err := c.PromoteSourceSnapshot(t.Context(), ref, settlement)
	if err != nil {
		t.Fatal(err)
	}
	commits := sourceResultCommits(t, c, result)
	if len(commits) != 1 || commits[0].Tenant != causal.TenantID(provision.Tenant) ||
		commits[0].CatalogRevision == 0 {
		t.Fatalf("promotion commits = %+v", commits)
	}
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	actualFingerprint, err := catalogFileProviderFingerprint(t.Context(), tx, provision.Tenant)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if commits[0].FileProviderFingerprint != actualFingerprint {
		t.Fatalf("staged File Provider proof drifted: commit=%x actual=%x",
			commits[0].FileProviderFingerprint, actualFingerprint)
	}
	repeated, err := c.PromoteSourceSnapshot(t.Context(), ref, settlement)
	if err != nil || !sourceResultsEqual(repeated, result) {
		t.Fatalf("promotion replay = %+v, %v; want %+v", repeated, err, result)
	}
	object, err := c.LookupName(t.Context(), provision.Tenant, PresentationFileProvider, provision.Root, "settings.json")
	if err != nil || object.Hash != first.Hash || object.Size != first.Size {
		t.Fatalf("promoted object = %+v, %v", object, err)
	}
	if err := c.AbortSourceSnapshotPublication(t.Context(), authority, "snapshot"); err != nil {
		t.Fatalf("abort settled snapshot replay = %v", err)
	}
}

func promoteSnapshotMetadataBaselineForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	provision TenantProvision,
) {
	t.Helper()
	const snapshot = "metadata-baseline"
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, snapshot, SourceSnapshotPage{
		Records: []SourcePhysicalIndexRecord{{
			Authority: authority, RootID: "root", Relative: "settings.json",
			FileIdentity: []byte("settings"), Kind: uint8(KindFile),
			Payload: []byte(`{"Exists":true}`),
		}},
		Next: SourceIndexLocator{RootID: "root", Relative: "settings.json"},
	}); err != nil {
		t.Fatal(err)
	}
	rootBinding, err := c.ReserveSourceAuthorityBinding(
		t.Context(), authority, "root-logical", sourceRootKey(provision),
	)
	if err != nil {
		t.Fatal(err)
	}
	valueBinding, err := c.ReserveSourceAuthorityBinding(
		t.Context(), authority, "settings-logical", "stable",
	)
	if err != nil {
		t.Fatal(err)
	}
	change := sourceSnapshotChangeAtDriverHeadForTest(t, c, authority)
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1, Snapshot: snapshot,
		FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority),
		Change:      change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"settings"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation,
			LogicalID: rootBinding.LogicalID, RootKey: rootBinding.SourceKey,
		}},
		Bindings: []SourceSnapshotBinding{{
			LogicalID: valueBinding.LogicalID, SourceKey: valueBinding.SourceKey,
			Fingerprint: [32]byte{1},
			Inputs:      []SourceIndexLocator{{RootID: "root", Relative: "settings.json"}},
		}},
		Objects: []SourceSnapshotProjection{{
			Tenant: provision.Tenant, Generation: provision.Generation,
			LogicalID: valueBinding.LogicalID,
			Object: SourceObject{
				Key: valueBinding.SourceKey, Name: "settings.json", Kind: KindFile, Mode: 0o600,
				ContentRevision: 1, Content: stageTestContent(t, c, "value"),
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PromoteSourceSnapshot(
		t.Context(), ref, sourceSnapshotSettlementForTest(ref, 0),
	); err != nil {
		t.Fatal(err)
	}
}

func TestStagedSourceSnapshotMetadataOnlyChangePreservesContentConvergence(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("test-source")
	configureSourceObserverForIndexTest(t, c, authority)
	provision, err := provisionTenantForTest(t, c, t.Context(), testTenantProvision(t, "snapshot-metadata", 1))
	if err != nil {
		t.Fatal(err)
	}
	promoteSnapshotMetadataBaselineForTest(t, c, authority, provision)
	before := sourceSnapshotObjectForTest(t, c, authority, provision.Tenant, "stable")
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE objects SET desired_revision = 22, observed_revision = 21,
    verified_revision = 20, applied_revision = 19
WHERE tenant = ? AND object_id = ?`,
		string(provision.Tenant), before.ID[:]); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "metadata"); err != nil {
		t.Fatal(err)
	}
	if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, "metadata", SourceSnapshotPage{
		Records: []SourcePhysicalIndexRecord{{
			Authority: authority, RootID: "root", Relative: "settings.json",
			FileIdentity: []byte("settings"), Kind: uint8(KindFile),
			Payload: []byte(`{"Exists":true}`),
		}},
		Next: SourceIndexLocator{RootID: "root", Relative: "settings.json"},
	}); err != nil {
		t.Fatal(err)
	}
	rootBinding, err := c.ReserveSourceAuthorityBinding(
		t.Context(), authority, "root-logical", sourceRootKey(provision),
	)
	if err != nil {
		t.Fatal(err)
	}
	valueBinding, err := c.ReserveSourceAuthorityBinding(
		t.Context(), authority, "settings-logical", "stable",
	)
	if err != nil {
		t.Fatal(err)
	}
	change := sourceSnapshotChangeAtDriverHeadForTest(t, c, authority)
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1, Snapshot: "metadata",
		FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority),
		Change:      change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"settings"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation,
			LogicalID: rootBinding.LogicalID, RootKey: rootBinding.SourceKey,
		}},
		Bindings: []SourceSnapshotBinding{{
			LogicalID: valueBinding.LogicalID, SourceKey: valueBinding.SourceKey,
			Fingerprint: [32]byte{2},
			Inputs:      []SourceIndexLocator{{RootID: "root", Relative: "settings.json"}},
		}},
		Objects: []SourceSnapshotProjection{{
			Tenant: provision.Tenant, Generation: provision.Generation,
			LogicalID: valueBinding.LogicalID,
			Object: SourceObject{
				Key: valueBinding.SourceKey, Name: "renamed.json", Kind: KindFile, Mode: 0o640,
				ContentRevision: 2, Content: stageTestContent(t, c, "value"),
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PromoteSourceSnapshot(
		t.Context(), ref, sourceSnapshotSettlementForTest(ref, 0),
	); err != nil {
		t.Fatal(err)
	}
	after := sourceSnapshotObjectForTest(t, c, authority, provision.Tenant, "stable")
	if after.ID != before.ID || after.ContentRevision != before.ContentRevision {
		t.Fatalf("metadata-only snapshot = %+v, want id=%s content_revision=%d",
			after, before.ID, before.ContentRevision)
	}
	var desired, observed, verified, applied uint64
	if err := c.db.QueryRowContext(t.Context(), `
SELECT desired_revision, observed_revision, verified_revision, applied_revision
FROM objects WHERE tenant = ? AND object_id = ?`,
		string(provision.Tenant), after.ID[:]).
		Scan(&desired, &observed, &verified, &applied); err != nil {
		t.Fatal(err)
	}
	if desired != 22 || observed != 21 || verified != 20 || applied != 19 {
		t.Fatalf("metadata-only snapshot convergence = %d/%d/%d/%d",
			desired, observed, verified, applied)
	}
}

func TestSettledSourceSnapshotReplaysAfterStageCleanupAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if c != nil {
			_ = c.Close()
		}
	})
	authority := causal.SourceAuthorityID("settled-source")
	configureSourceObserverForIndexTest(t, c, authority)
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "settled"); err != nil {
		t.Fatal(err)
	}
	ref := promoteObserverSnapshotForTest(t, c, authority, "settled", 0, false)
	settlement := sourceSnapshotSettlementForTest(ref, 0)
	want, err := c.PromoteSourceSnapshot(t.Context(), ref, settlement)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{
		"source_snapshot_stages", "source_snapshot_logical", "source_snapshot_sessions",
		"source_snapshot_publications", "source_snapshot_pages", "source_snapshot_affected",
		"source_snapshot_roots", "source_snapshot_bindings", "source_snapshot_objects", "content_stages",
	} {
		var count int
		if err := c.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("settled residue %s = %d, %v", table, count, err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c = nil
	c, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := c.PromoteSourceSnapshot(t.Context(), ref, settlement)
	if err != nil || !sourceResultsEqual(replayed, want) {
		t.Fatalf("promotion replay after cleanup = %+v, %v; want %+v", replayed, err, want)
	}
}

func TestInvalidSourceSnapshotAbortLeavesNoBindingOrContentResidue(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("aborted-source")
	configureSourceObserverForIndexTest(t, c, authority)
	provisionSpec := testTenantProvision(t, "aborted-snapshot", 1)
	provisionSpec.ContentSourceID = string(authority)
	provision, err := provisionTenantForTest(t, c, t.Context(), provisionSpec)
	if err != nil {
		t.Fatal(err)
	}
	durableTables := []string{
		"source_authority_bindings", "source_object_ids", "source_object_bindings",
	}
	baseline := make(map[string]int, len(durableTables))
	for _, table := range durableTables {
		var count int
		if err := c.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			t.Fatalf("read abort baseline %s: %v", table, err)
		}
		baseline[table] = count
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "aborted"); err != nil {
		t.Fatal(err)
	}
	if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, "aborted", SourceSnapshotPage{
		Records: []SourcePhysicalIndexRecord{{
			Authority: authority, RootID: "root", Relative: "value", FileIdentity: []byte("value-identity"),
			Kind: 2, Payload: []byte(`{"Exists":true}`),
		}},
		Next: SourceIndexLocator{RootID: "root", Relative: "value"},
	}); err != nil {
		t.Fatal(err)
	}
	change := sourceSnapshotChangeAtDriverHeadForTest(t, c, authority)
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1,
		Snapshot: "aborted", FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority), Change: change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	content := stageTestContent(t, c, "aborted")
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"value"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "root", RootKey: "root-key",
		}},
		Bindings: []SourceSnapshotBinding{{
			LogicalID: "value", SourceKey: "value-key", Inputs: []SourceIndexLocator{{RootID: "root", Relative: "value"}},
		}},
		Objects: []SourceSnapshotProjection{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "value",
			Object: SourceObject{
				Key: "value-key", Parent: "missing-parent", Name: "value", Kind: KindFile, Mode: 0o600,
				ContentRevision: 1, Content: content, Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PromoteSourceSnapshot(
		t.Context(), ref, sourceSnapshotSettlementForTest(ref, 0),
	); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("invalid promotion = %v, want invalid object", err)
	}
	if err := c.AbortSourceSnapshotStage(t.Context(), authority, "aborted"); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{
		"source_snapshot_stages", "source_snapshot_logical", "source_snapshot_sessions",
		"source_snapshot_publications", "source_snapshot_pages", "source_snapshot_affected",
		"source_snapshot_roots", "source_snapshot_bindings", "source_snapshot_objects",
		"content_stages",
	} {
		var count int
		if err := c.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("abort residue %s = %d, %v", table, count, err)
		}
	}
	for _, table := range durableTables {
		var count int
		if err := c.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != baseline[table] {
			t.Fatalf("abort changed durable %s = %d, %v; want %d", table, count, err, baseline[table])
		}
	}
}

func TestSourceSnapshotPromotionIgnoresForeignStagedRootRevision(t *testing.T) {
	c := newTestCatalog(t)
	primary := causal.SourceAuthorityID("primary-source")
	foreign := causal.SourceAuthorityID("foreign-source")
	configureSourceObserverForIndexTest(t, c, primary)
	configureSourceObserverForIndexTest(t, c, foreign)
	provisionSpec := testTenantProvision(t, "overlapping-snapshot", 1)
	provisionSpec.ContentSourceID = string(primary)
	provision, err := provisionTenantForTest(t, c, t.Context(), provisionSpec)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), primary, "initial"); err != nil {
		t.Fatal(err)
	}
	if err := c.AppendSourceSnapshotStagePage(t.Context(), primary, "initial", SourceSnapshotPage{
		Records: []SourcePhysicalIndexRecord{{
			Authority: primary, RootID: "root", Relative: "value", FileIdentity: []byte("initial-value"),
			Kind: 1, Payload: []byte(`{"Exists":true}`),
		}},
		Next: SourceIndexLocator{RootID: "root", Relative: "value"},
	}); err != nil {
		t.Fatal(err)
	}
	initialChange := sourceSnapshotChangeAtDriverHeadForTest(t, c, primary)
	initialIdentity := SourceSnapshotIdentity{
		Authority: primary, AuthorityGeneration: 1,
		Snapshot: "initial", FenceDigest: sourceSnapshotFenceDigestForTest(t, c, primary), Change: initialChange,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), initialIdentity); err != nil {
		t.Fatal(err)
	}
	initialRef, err := c.AppendSourceSnapshotPublication(t.Context(), initialIdentity, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"value"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "root", RootKey: "root-key",
		}},
		Bindings: []SourceSnapshotBinding{{
			LogicalID: "value", SourceKey: "value-key", Inputs: []SourceIndexLocator{{RootID: "root", Relative: "value"}},
		}},
		Objects: []SourceSnapshotProjection{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "value",
			Object: SourceObject{
				Key: "value-key", Name: "value", Kind: KindDirectory, Mode: 0o700,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PromoteSourceSnapshot(
		t.Context(), initialRef, sourceSnapshotSettlementForTest(initialRef, 0),
	); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), foreign, "foreign"); err != nil {
		t.Fatal(err)
	}
	foreignChange := sourceChange(1)
	foreignChange.SourceAuthority = foreign
	foreignChange.AffectedKeys = nil
	foreignIdentity := SourceSnapshotIdentity{
		Authority: foreign, AuthorityGeneration: 1,
		Snapshot: "foreign", FenceDigest: sourceSnapshotFenceDigestForTest(t, c, foreign), Change: foreignChange,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), foreignIdentity); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AppendSourceSnapshotPublication(t.Context(), foreignIdentity, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"foreign"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "foreign-root", RootKey: "foreign-root-key",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_snapshot_roots SET catalog_revision = 999
WHERE source_authority = ? AND snapshot_id = ?`, string(foreign), "foreign"); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), primary, "replacement"); err != nil {
		t.Fatal(err)
	}
	replacementChange := sourceSnapshotChangeAtDriverHeadForTest(t, c, primary)
	replacementIdentity := SourceSnapshotIdentity{
		Authority: primary, AuthorityGeneration: 1,
		Snapshot: "replacement", FenceDigest: sourceSnapshotFenceDigestForTest(t, c, primary), Change: replacementChange,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), replacementIdentity); err != nil {
		t.Fatal(err)
	}
	replacementRef, err := c.AppendSourceSnapshotPublication(t.Context(), replacementIdentity, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"value"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "root", RootKey: "root-key",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.PromoteSourceSnapshot(
		t.Context(), replacementRef, sourceSnapshotSettlementForTest(replacementRef, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	commits := sourceResultCommits(t, c, result)
	if len(commits) != 1 || commits[0].CatalogRevision == 999 {
		t.Fatalf("replacement commits = %+v", commits)
	}
	var rawObjectID []byte
	if err := c.db.QueryRowContext(t.Context(), `
SELECT object_id FROM source_object_ids
WHERE source_authority = ? AND source_key = ?`,
		string(primary), "value-key").Scan(&rawObjectID); err != nil {
		t.Fatal(err)
	}
	snapshotObjectID, err := objectID(rawObjectID)
	if err != nil {
		t.Fatal(err)
	}
	var tombstoneRevision uint64
	if err := c.db.QueryRowContext(t.Context(), `
SELECT revision FROM object_versions
WHERE tenant = ? AND object_id = ? AND tombstone = 1 ORDER BY revision DESC LIMIT 1`,
		string(provision.Tenant), snapshotObjectID[:]).Scan(&tombstoneRevision); err != nil {
		t.Fatal(err)
	}
	if tombstoneRevision != uint64(commits[0].CatalogRevision) {
		t.Fatalf("tombstone revision = %d, want %d", tombstoneRevision, commits[0].CatalogRevision)
	}
}

func TestStagedSourceSnapshotPagesTenThousandObjectsWithinHardBounds(t *testing.T) {
	if testing.Short() {
		t.Skip("large deterministic snapshot")
	}
	var appendedPages, setwisePromotions, rowSettlements int
	c := newFailpointCatalog(t, func(point string) error {
		switch point {
		case sourceSnapshotAfterAppendPage:
			appendedPages++
		case sourceSnapshotAfterSetwisePromotion:
			setwisePromotions++
		case sourceObserverBeforeRowSettlement:
			rowSettlements++
		}
		return nil
	})
	authority := causal.SourceAuthorityID("test-source")
	configureSourceObserverForIndexTest(t, c, authority)
	provision, err := provisionTenantForTest(t, c, t.Context(), testTenantProvision(t, "staged-snapshot-scale", 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "large"); err != nil {
		t.Fatal(err)
	}
	if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, "large", SourceSnapshotPage{
		Records: []SourcePhysicalIndexRecord{{
			Authority: authority, RootID: "root", Relative: "fleet", FileIdentity: []byte("fleet-identity"),
			Kind: 1, Payload: []byte(`{"Exists":true}`),
		}},
		Next: SourceIndexLocator{RootID: "root", Relative: "fleet"},
	}); err != nil {
		t.Fatal(err)
	}
	rootKey := SourceObjectKey("root-key")
	change := sourceSnapshotChangeAtDriverHeadForTest(t, c, authority)
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1,
		Snapshot: "large", FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority), Change: change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	total := testScaleCount(10_000)
	stageStarted := time.Now()
	var cursor string
	var ref SourceSnapshotStageRef
	for start := 0; start < total; start += SourceSnapshotPageLimit {
		end := min(start+SourceSnapshotPageLimit, total)
		page := SourceSnapshotPublicationPage{Cursor: cursor}
		if start == 0 {
			page.AffectedKeys = []causal.LogicalKey{"fleet"}
			page.Roots = []SourceSnapshotRoot{{
				Tenant: provision.Tenant, Generation: provision.Generation,
				LogicalID: "root-logical", RootKey: rootKey,
			}}
		}
		if end < total {
			page.Next = fmt.Sprintf("page-%05d", end)
		}
		for index := start; index < end; index++ {
			logical := fmt.Sprintf("logical-%05d", index)
			key := SourceObjectKey(fmt.Sprintf("key-%05d", index))
			fingerprint := [32]byte{}
			fingerprint[0] = byte(index)
			page.Bindings = append(page.Bindings, SourceSnapshotBinding{
				LogicalID: logical, SourceKey: key, Fingerprint: fingerprint,
				Inputs: []SourceIndexLocator{{RootID: "root", Relative: "fleet"}},
			})
			page.Objects = append(page.Objects, SourceSnapshotProjection{
				Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: logical,
				Object: SourceObject{
					Key: key, Name: fmt.Sprintf("item-%05d", index), Kind: KindDirectory,
					Mode: 0o700, Visibility: Visibility{Mount: true, FileProvider: true},
				},
			})
		}
		ref, err = c.AppendSourceSnapshotPublication(t.Context(), identity, page)
		if err != nil {
			t.Fatalf("append page %d: %v", start/SourceSnapshotPageLimit, err)
		}
		cursor = page.Next
	}
	stageElapsed := time.Since(stageStarted)
	t.Logf("staged %d objects in %s", total, stageElapsed)
	var pages, objects, largestPage int
	if err := c.db.QueryRowContext(t.Context(), `
SELECT COUNT(*), COALESCE(MAX(page_bytes), 0) FROM source_snapshot_pages
WHERE source_authority = ? AND snapshot_id = ?`, string(authority), "large").Scan(&pages, &largestPage); err != nil {
		t.Fatal(err)
	}
	if err := c.db.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_snapshot_objects WHERE source_authority = ? AND snapshot_id = ?`,
		string(authority), "large").Scan(&objects); err != nil {
		t.Fatal(err)
	}
	wantPages := (total + SourceSnapshotPageLimit - 1) / SourceSnapshotPageLimit
	if pages != wantPages || objects != total || largestPage > SourceSnapshotPageByteLimit {
		t.Fatalf("bounded stage = pages %d/%d objects %d/%d max page %d/%d", pages, wantPages,
			objects, total, largestPage, SourceSnapshotPageByteLimit)
	}
	promoteStarted := time.Now()
	if _, err := c.PromoteSourceSnapshot(
		t.Context(), ref, sourceSnapshotSettlementForTest(ref, 0),
	); err != nil {
		t.Fatal(err)
	}
	promoteElapsed := time.Since(promoteStarted)
	t.Logf("promoted %d objects in %s", total, promoteElapsed)
	t.Logf("%d-object stage+promotion completed in %s", total, stageElapsed+promoteElapsed)
	if appendedPages != wantPages || setwisePromotions != 1 || rowSettlements != 0 {
		t.Fatalf(
			"snapshot execution shape = %d page appends, %d set-wise promotions, %d row settlements; want %d, 1, 0",
			appendedPages, setwisePromotions, rowSettlements, wantPages,
		)
	}
	for _, index := range []int{0, total / 2, total - 1} {
		object := sourceSnapshotObjectForTest(
			t, c, authority, provision.Tenant, SourceObjectKey(fmt.Sprintf("key-%05d", index)),
		)
		if object.Kind != KindDirectory || object.Name != fmt.Sprintf("item-%05d", index) {
			t.Fatalf("promoted item %d = %+v", index, object)
		}
	}
}

func TestStagedSourceSnapshotReachabilityUsesParentIndex(t *testing.T) {
	c := newTestCatalog(t)
	rows, err := c.db.QueryContext(t.Context(), `
EXPLAIN QUERY PLAN
WITH RECURSIVE reachable(tenant, source_key) AS (
    SELECT tenant, source_key FROM source_snapshot_objects
    WHERE source_authority = ? AND snapshot_id = ? AND parent_key = ''
    UNION ALL
    SELECT child.tenant, child.source_key
    FROM source_snapshot_objects child
    JOIN reachable parent
      ON parent.tenant = child.tenant AND parent.source_key = child.parent_key
    WHERE child.source_authority = ? AND child.snapshot_id = ?
)
SELECT
    (SELECT COUNT(*) FROM source_snapshot_objects
     WHERE source_authority = ? AND snapshot_id = ?)
  - (SELECT COUNT(*) FROM reachable)`,
		"authority", "snapshot", "authority", "snapshot", "authority", "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close reachability plan rows: %v", err)
		}
	}()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(plan, "\n"), "source_snapshot_objects_parent") {
		t.Fatalf("snapshot reachability query plan = %q, want parent index", plan)
	}
}

func TestStagedSourceSnapshotPromotionUsesKeyedHotJoins(t *testing.T) {
	c := newTestCatalog(t)
	rows, err := c.db.QueryContext(t.Context(), `
EXPLAIN QUERY PLAN
SELECT staged.tenant, binding.object_id, root.catalog_revision
FROM source_snapshot_objects staged
JOIN source_snapshot_bindings binding
  ON binding.source_authority = staged.source_authority
 AND binding.snapshot_id = staged.snapshot_id AND binding.source_key = staged.source_key
JOIN source_snapshot_roots root
  ON root.source_authority = staged.source_authority AND root.snapshot_id = staged.snapshot_id
 AND root.tenant = staged.tenant AND root.generation = staged.generation
WHERE staged.source_authority = ? AND staged.snapshot_id = ?`, "authority", "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close promotion plan rows: %v", err)
		}
	}()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	for _, alias := range []string{"staged", "binding", "root"} {
		if !strings.Contains(joined, "SEARCH "+alias) || strings.Contains(joined, "SCAN "+alias) {
			t.Fatalf("snapshot promotion query plan = %q, want keyed search for %s", plan, alias)
		}
	}
}

func TestSourceSnapshotReplacementForeignKeysUseChildIndexes(t *testing.T) {
	c := newTestCatalog(t)
	tests := []struct {
		name      string
		statement string
		index     string
		args      []any
	}{
		{
			name: "snapshot objects by binding",
			statement: `DELETE FROM source_snapshot_bindings
WHERE source_authority = ? AND snapshot_id = ? AND logical_id = ?`,
			index: "source_snapshot_objects_binding",
			args:  []any{"authority", "snapshot", "logical"},
		},
		{
			name: "snapshot logicals by physical stage",
			statement: `DELETE FROM source_snapshot_stages
WHERE source_authority = ? AND snapshot_id = ? AND root_id = ? AND relative_path = ?`,
			index: "source_snapshot_logical_stage",
			args:  []any{"authority", "snapshot", "root", "file"},
		},
		{
			name: "physical logicals by physical index",
			statement: `DELETE FROM source_physical_index
WHERE source_authority = ? AND root_id = ? AND relative_path = ?`,
			index: "source_physical_logical_physical",
			args:  []any{"authority", "root", "file"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows, err := c.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+test.statement, test.args...)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := rows.Close(); err != nil {
					t.Errorf("close foreign-key plan rows: %v", err)
				}
			}()
			var plan []string
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				plan = append(plan, detail)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(plan, "\n"), test.index) {
				t.Fatalf("foreign-key child lookup plan = %q, want %s", plan, test.index)
			}
		})
	}
}

func TestStagedSourceSnapshotResolvesForwardParentAcrossPages(t *testing.T) {
	c, identity, provision := beginSnapshotGraphTest(t, "forward-parent")
	first := SourceSnapshotPublicationPage{
		Next:         "parent-page",
		AffectedKeys: []causal.LogicalKey{"fleet"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation,
			LogicalID: "root", RootKey: "root-key",
		}},
		Bindings: []SourceSnapshotBinding{{
			LogicalID: "a-child", SourceKey: "child-key",
			Inputs: []SourceIndexLocator{{RootID: "root", Relative: "fleet"}},
		}},
		Objects: []SourceSnapshotProjection{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "a-child",
			Object: SourceObject{
				Key: "child-key", Parent: "parent-key", Name: "child", Kind: KindDirectory, Mode: 0o700,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	}
	if _, err := c.AppendSourceSnapshotPublication(t.Context(), identity, first); err != nil {
		t.Fatal(err)
	}
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, SourceSnapshotPublicationPage{
		Cursor: "parent-page",
		Bindings: []SourceSnapshotBinding{{
			LogicalID: "z-parent", SourceKey: "parent-key",
			Inputs: []SourceIndexLocator{{RootID: "root", Relative: "fleet"}},
		}},
		Objects: []SourceSnapshotProjection{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "z-parent",
			Object: SourceObject{
				Key: "parent-key", Name: "parent", Kind: KindDirectory, Mode: 0o700,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PromoteSourceSnapshot(
		t.Context(), ref, sourceSnapshotSettlementForTest(ref, 0),
	); err != nil {
		t.Fatal(err)
	}
	parent := sourceSnapshotObjectForTest(t, c, identity.Authority, provision.Tenant, "parent-key")
	child := sourceSnapshotObjectForTest(t, c, identity.Authority, provision.Tenant, "child-key")
	if child.Parent != parent.ID {
		t.Fatalf("forward-parent child = %+v", child)
	}
}

func TestStagedSourceSnapshotRejectsCrossPageParentCycle(t *testing.T) {
	c, identity, provision := beginSnapshotGraphTest(t, "parent-cycle")
	if _, err := c.AppendSourceSnapshotPublication(t.Context(), identity, SourceSnapshotPublicationPage{
		Next:         "second",
		AffectedKeys: []causal.LogicalKey{"fleet"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "root", RootKey: "root-key",
		}},
		Bindings: []SourceSnapshotBinding{{
			LogicalID: "a", SourceKey: "a-key",
			Inputs: []SourceIndexLocator{{RootID: "root", Relative: "fleet"}},
		}},
		Objects: []SourceSnapshotProjection{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "a",
			Object: SourceObject{
				Key: "a-key", Parent: "b-key", Name: "a", Kind: KindDirectory, Mode: 0o700,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, SourceSnapshotPublicationPage{
		Cursor: "second",
		Bindings: []SourceSnapshotBinding{{
			LogicalID: "b", SourceKey: "b-key",
			Inputs: []SourceIndexLocator{{RootID: "root", Relative: "fleet"}},
		}},
		Objects: []SourceSnapshotProjection{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "b",
			Object: SourceObject{
				Key: "b-key", Parent: "a-key", Name: "b", Kind: KindDirectory, Mode: 0o700,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PromoteSourceSnapshot(
		t.Context(), ref, sourceSnapshotSettlementForTest(ref, 0),
	); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("PromoteSourceSnapshot(parent cycle) = %v, want invalid object", err)
	}
}

func TestStagedSourceSnapshotBlobVerificationDoesNotHoldWriter(t *testing.T) {
	blocker := newPointBlocker(contentBeforeVerify)
	c := newFailpointCatalog(t, blocker.fail)
	authority := causal.SourceAuthorityID("test-source")
	configureSourceObserverForIndexTest(t, c, authority)
	provision, err := provisionTenantForTest(t, c, t.Context(), testTenantProvision(t, "snapshot-preverify", 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "preverify"); err != nil {
		t.Fatal(err)
	}
	if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, "preverify", SourceSnapshotPage{
		Records: []SourcePhysicalIndexRecord{{
			Authority: authority, RootID: "root", Relative: "file", FileIdentity: []byte("file-identity"),
			Kind: 2, Payload: []byte(`{"Exists":true}`),
		}},
		Next: SourceIndexLocator{RootID: "root", Relative: "file"},
	}); err != nil {
		t.Fatal(err)
	}
	change := sourceSnapshotChangeAtDriverHeadForTest(t, c, authority)
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1,
		Snapshot: "preverify", FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority), Change: change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	content := stageTestContent(t, c, "verified outside the writer transaction")
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"file"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "root", RootKey: "root-key",
		}},
		Bindings: []SourceSnapshotBinding{{
			LogicalID: "file", SourceKey: "file-key",
			Inputs: []SourceIndexLocator{{RootID: "root", Relative: "file"}},
		}},
		Objects: []SourceSnapshotProjection{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "file",
			Object: SourceObject{
				Key: "file-key", Name: "file", Kind: KindFile, Mode: 0o600,
				ContentRevision: 1, Content: content, Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	blocker.arm()
	type promotionResult struct {
		result SourceResult
		err    error
	}
	result := make(chan promotionResult, 1)
	settlement := sourceSnapshotSettlementForTest(ref, 0)
	go func() {
		value, promoteErr := c.PromoteSourceSnapshot(t.Context(), ref, settlement)
		result <- promotionResult{result: value, err: promoteErr}
	}()
	awaitSignal(t, blocker.started, "snapshot blob verification")
	writeCtx, cancel := context.WithTimeout(t.Context(), concurrencyTestTimeout)
	unrelated := testTenantProvision(t, "snapshot-preverify-unrelated", 1)
	unrelated.ContentSourceID = "unrelated-source"
	_, writeErr := provisionTenantForTest(t, c, writeCtx, unrelated)
	cancel()
	if writeErr != nil {
		t.Fatalf("unrelated provision while snapshot blob verification blocked: %v", writeErr)
	}
	blocker.unblock()
	select {
	case promoted := <-result:
		if promoted.err != nil || promoted.result.Revision != ref.Revision {
			t.Fatalf("PromoteSourceSnapshot after preverification = %+v, %v", promoted.result, promoted.err)
		}
	case <-time.After(concurrencyTestTimeout):
		t.Fatal("timed out waiting for snapshot promotion")
	}
}

func beginSnapshotGraphTest(
	t *testing.T,
	snapshot string,
) (*Catalog, SourceSnapshotIdentity, TenantProvision) {
	t.Helper()
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("test-source")
	configureSourceObserverForIndexTest(t, c, authority)
	provision, err := provisionTenantForTest(t, c, t.Context(), testTenantProvision(t, snapshot, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, snapshot, SourceSnapshotPage{
		Records: []SourcePhysicalIndexRecord{{
			Authority: authority, RootID: "root", Relative: "fleet", FileIdentity: []byte("fleet-identity"),
			Kind: 1, Payload: []byte(`{"Exists":true}`),
		}},
		Next: SourceIndexLocator{RootID: "root", Relative: "fleet"},
	}); err != nil {
		t.Fatal(err)
	}
	change := sourceSnapshotChangeAtDriverHeadForTest(t, c, authority)
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1,
		Snapshot: snapshot, FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority), Change: change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	return c, identity, provision
}

func TestStagedSourceSnapshotRejectsOversizedPageBeforeOwnership(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("test-source")
	configureSourceObserverForIndexTest(t, c, authority)
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "oversized"); err != nil {
		t.Fatal(err)
	}
	change := sourceChange(1)
	change.AffectedKeys = nil
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1,
		Snapshot: "oversized", FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority), Change: change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	page := SourceSnapshotPublicationPage{AffectedKeys: make([]causal.LogicalKey, SourceSnapshotPageLimit+1)}
	for index := range page.AffectedKeys {
		page.AffectedKeys[index] = causal.LogicalKey(fmt.Sprintf("key-%04d", index))
	}
	if _, err := c.AppendSourceSnapshotPublication(t.Context(), identity, page); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("oversized page = %v, want invalid object", err)
	}
	var pages int
	if err := c.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM source_snapshot_pages`).Scan(&pages); err != nil || pages != 0 {
		t.Fatalf("oversized page mutated stage = %d, %v", pages, err)
	}
}

func TestSnapshotPageDigestIgnoresEphemeralStageID(t *testing.T) {
	left := SourceSnapshotPublicationPage{Objects: []SourceSnapshotProjection{{
		Tenant: "tenant", Generation: 1, LogicalID: "logical",
		Object: SourceObject{
			Key: "key", Name: "value", Kind: KindFile, ContentRevision: 1,
			Content:    ContentRef{Stage: StageID{1}, Hash: ContentHash{2}, Size: 3},
			Visibility: Visibility{Mount: true},
		},
	}}}
	right := left
	right.Objects = append([]SourceSnapshotProjection(nil), left.Objects...)
	right.Objects[0].Object.Content.Stage = StageID{9}
	identity := SourceSnapshotIdentity{
		Authority: "authority", AuthorityGeneration: 1, Snapshot: "snapshot",
		Change: causal.ChangeSet{SourceAuthority: "authority"},
	}
	leftDigest, _, leftErr := validateSourceSnapshotPage(identity, left)
	rightDigest, _, rightErr := validateSourceSnapshotPage(identity, right)
	if leftErr != nil || rightErr != nil || !bytes.Equal(leftDigest[:], rightDigest[:]) {
		t.Fatalf("semantic page digests = %x/%x, %v/%v", leftDigest, rightDigest, leftErr, rightErr)
	}
}
