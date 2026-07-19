package catalog

import (
	"errors"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceSnapshotBeginDistinguishesPendingStagesFromCommittedDriverHistory(t *testing.T) {
	store := newTestCatalog(t)
	authority := causal.SourceAuthorityID("snapshot-stage-gate")
	configureSourceObserverForIndexTest(t, store, authority)
	const snapshot = "snapshot-stage-gate"
	if err := store.BeginSourceSnapshotStage(t.Context(), authority, snapshot); err != nil {
		t.Fatal(err)
	}
	change := sourceChange(1)
	change.SourceAuthority = authority
	change.AffectedKeys = nil
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1, Snapshot: snapshot,
		FenceDigest: sourceSnapshotFenceDigestForTest(t, store, authority), Change: change,
	}

	physicalOperation := causal.OperationID{1}
	insertSourceSnapshotGateStage(t, store, authority, physicalOperation, 1, "stream", "epoch")
	if err := store.BeginSourceSnapshotPublication(t.Context(), identity); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("BeginSourceSnapshotPublication(pending physical stage) = %v, want conflict", err)
	}
	if _, err := store.db.ExecContext(t.Context(), `
DELETE FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`, string(authority), physicalOperation[:]); err != nil {
		t.Fatal(err)
	}

	driverOperation := causal.OperationID{2}
	insertSourceSnapshotGateStage(t, store, authority, driverOperation, 2, "", "")
	if err := store.BeginSourceSnapshotPublication(t.Context(), identity); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("BeginSourceSnapshotPublication(pending driver stage) = %v, want conflict", err)
	}
	if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO source_driver_stage_receipts(
    source_authority, stage_operation_id, mode, from_token, to_token, source_revision,
    target_count, targets_digest, stage_sequence, stage_item_count, stage_byte_count,
    stage_digest, identity_digest, result_json, result_digest
) VALUES (?, ?, 1, '', 'committed', 1, 1, zeroblob(32), 1, 0, 0,
          zeroblob(32), zeroblob(32), X'7b7d', zeroblob(32))`,
		string(authority), driverOperation[:]); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatalf("BeginSourceSnapshotPublication(committed driver history) = %v", err)
	}
}

func insertSourceSnapshotGateStage(
	t *testing.T,
	store *Catalog,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	kind uint8,
	stream string,
	epoch string,
) {
	t.Helper()
	if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO source_publication_stages(
    source_authority, stage_operation_id, fleet_owner_id, authority_generation,
    driver_id, declaration_digest, stage_kind, stream_identity, root_epoch,
    through_sequence, predecessor_revision, last_revision, next_sequence,
    item_count, byte_count, complete, aborting, identity_digest, rolling_digest
) VALUES (?, ?, ?, 1, 'test-driver', zeroblob(32), ?, ?, ?,
          0, 0, 0, 0, 0, 0, 0, 0, zeroblob(32), zeroblob(32))`,
		string(authority), operation[:], "test:"+string(authority), kind, stream, epoch); err != nil {
		t.Fatal(err)
	}
}
