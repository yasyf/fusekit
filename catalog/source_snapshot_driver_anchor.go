package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/yasyf/fusekit/causal"
)

const (
	sourceDriverPublicationSnapshotAnchor uint8 = 3
	sourceDriverSnapshotAnchorDomain            = "fusekit.source-driver-snapshot-anchor.v1\x00"
)

type sourceDriverSnapshotAnchorTarget struct {
	tenant     TenantID
	generation Generation
	root       SourceObjectKey
	rootID     ObjectID
	head       Revision
}

type sourceDriverSnapshotAnchorIdentity struct {
	Authority           causal.SourceAuthorityID
	Publication         causal.OperationID
	SourceRevision      causal.Revision
	FleetOwner          SourceAuthorityFleetOwnerID
	AuthorityGeneration causal.Generation
	DeclarationDigest   [sha256.Size]byte
	TargetEpoch         uint64
	TargetCount         uint64
	TargetsDigest       [sha256.Size]byte
}

func seedSourceDriverSnapshotAnchor(
	ctx context.Context,
	tx *sql.Tx,
	ref SourceSnapshotStageRef,
	identity SourceSnapshotIdentity,
) error {
	affectedCount, affectedDigest, err := sourceDriverSnapshotAnchorAffectedIdentity(ctx, tx, ref)
	if err != nil {
		return err
	}
	targets, targetDigest, err := sourceDriverSnapshotAnchorTargets(ctx, tx, ref)
	if err != nil {
		return err
	}
	var targetEpoch uint64
	if err := tx.QueryRowContext(ctx, `
SELECT target_epoch FROM source_driver_target_epochs WHERE source_authority = ?`,
		string(ref.Authority)).Scan(&targetEpoch); err != nil {
		return fmt.Errorf("catalog: read source snapshot driver target epoch: %w", err)
	}
	var fleetOwner string
	var declarationDigest []byte
	if err := tx.QueryRowContext(ctx, `
SELECT member.owner_id, member.declaration_digest
FROM source_authority_fleet_members member
JOIN source_authority_fleet_heads head
  ON head.owner_id = member.owner_id AND head.generation = member.generation
WHERE member.source_authority = ? AND member.generation = ?`,
		string(ref.Authority), uint64(identity.AuthorityGeneration)).Scan(
		&fleetOwner, &declarationDigest,
	); err != nil {
		return fmt.Errorf("catalog: read source snapshot driver declaration: %w", err)
	}
	if len(declarationDigest) != sha256.Size {
		return ErrIntegrity
	}
	var declaration [sha256.Size]byte
	copy(declaration[:], declarationDigest)
	anchorIdentity := sourceDriverSnapshotAnchorIdentity{
		Authority: ref.Authority, Publication: ref.Operation, SourceRevision: ref.Revision,
		FleetOwner: SourceAuthorityFleetOwnerID(fleetOwner), AuthorityGeneration: identity.AuthorityGeneration,
		DeclarationDigest: declaration, TargetEpoch: targetEpoch,
		TargetCount: uint64(len(targets)), TargetsDigest: targetDigest,
	}
	identityDigest, err := sourceDriverSnapshotAnchorIdentityDigest(anchorIdentity)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_heads(
    source_authority, publication_id, source_revision, epoch
) VALUES (?, zeroblob(0), 0, 0)
ON CONFLICT(source_authority) DO NOTHING`, string(ref.Authority)); err != nil {
		return mapConstraint(err)
	}
	var predecessor []byte
	var predecessorRevision, visibilityEpoch uint64
	if err := tx.QueryRowContext(ctx, `
SELECT publication_id, source_revision, epoch
FROM source_driver_publication_heads WHERE source_authority = ?`, string(ref.Authority)).Scan(
		&predecessor, &predecessorRevision, &visibilityEpoch,
	); err != nil {
		return fmt.Errorf("catalog: read source snapshot driver visibility: %w", err)
	}
	if len(predecessor) != 0 && len(predecessor) != len(causal.OperationID{}) {
		return ErrIntegrity
	}
	if predecessorRevision+1 != uint64(ref.Revision) {
		return ErrSourcePredecessor
	}
	var pageCount, objectCount, metadataBytes, contentBytes uint64
	if err := tx.QueryRowContext(ctx, `
SELECT page_count, object_count, metadata_bytes, content_bytes
FROM source_snapshot_publications
WHERE source_authority = ? AND snapshot_id = ?`, string(ref.Authority), ref.Snapshot).Scan(
		&pageCount, &objectCount, &metadataBytes, &contentBytes,
	); err != nil {
		return fmt.Errorf("catalog: read source snapshot driver publication proof: %w", err)
	}
	if pageCount == 0 {
		return ErrIntegrity
	}
	stageItems := objectCount + uint64(len(targets))
	if stageItems == 0 {
		stageItems = 1
	}
	stageBytes := metadataBytes + contentBytes
	if stageBytes == 0 {
		stageBytes = 1
	}
	if predecessor == nil {
		predecessor = []byte{}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publications(
    source_authority, publication_id, source_operation_id, change_id, cause,
    origin_domain, origin_generation, affected_key_count, affected_keys_digest,
    publication_kind, identity_digest,
    target_count, targets_digest, stage_sequence, stage_item_count, stage_byte_count,
    stage_digest, predecessor_publication_id, predecessor_revision, source_revision,
    expected_visibility_epoch, target_epoch, phase, cursor_tenant, cursor_key,
    initialized_target_count, prepared_target_count, item_count, byte_count,
    rolling_digest, prepared
) VALUES (?, ?, ?, ?, ?, '', 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, ?, 1)`,
		string(ref.Authority), ref.Operation[:], identity.Change.OperationID[:], identity.Change.ChangeID[:],
		string(identity.Change.Cause), affectedCount, affectedDigest[:],
		sourceDriverPublicationSnapshotAnchor, identityDigest[:],
		len(targets), targetDigest[:], pageCount, stageItems, stageBytes, ref.Digest[:],
		predecessor, predecessorRevision, uint64(ref.Revision), visibilityEpoch, targetEpoch,
		sourceDriverPublicationPrepared, len(targets), len(targets), stageItems, stageBytes, ref.Digest[:],
	); err != nil {
		return mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_affected(source_authority, publication_id, affected_key)
SELECT source_authority, ?, affected_key
FROM source_snapshot_affected
WHERE source_authority = ? AND snapshot_id = ?
ORDER BY affected_key`, ref.Operation[:], string(ref.Authority), ref.Snapshot); err != nil {
		return mapConstraint(err)
	}
	for _, target := range targets {
		if err := insertSourceDriverSnapshotAnchorTarget(
			ctx, tx, ref, predecessor, target,
		); err != nil {
			return err
		}
	}
	if err := copySourceDriverSnapshotAnchorState(ctx, tx, ref); err != nil {
		return err
	}
	for _, target := range targets {
		catalogFingerprint, providerFingerprint, count, err := sourceDriverSnapshotAnchorFingerprints(
			ctx, tx, ref, target,
		)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets
SET catalog_fingerprint = ?, file_provider_fingerprint = ?, catalog_state = ?, provider_state = ?,
    object_count = ?
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND prepared = 1`,
			catalogFingerprint[:], providerFingerprint[:], catalogFingerprint[:], providerFingerprint[:], count,
			string(ref.Authority), ref.Operation[:], string(target.tenant),
		)
		if err != nil {
			return mapConstraint(err)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return ErrMutationConflict
		}
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_heads
SET publication_id = ?, source_revision = ?, epoch = epoch + 1
WHERE source_authority = ? AND publication_id = ?
  AND source_revision = ? AND epoch = ?`,
		ref.Operation[:], uint64(ref.Revision), string(ref.Authority), predecessor,
		predecessorRevision, visibilityEpoch,
	)
	if err != nil {
		return mapConstraint(err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	checkpoint := SourceDriverCheckpoint{
		Authority: ref.Authority, FleetOwner: SourceAuthorityFleetOwnerID(fleetOwner),
		AuthorityGeneration: identity.AuthorityGeneration, DeclarationDigest: declaration,
		TargetEpoch: targetEpoch, TargetCount: uint64(len(targets)), TargetsDigest: targetDigest,
		Token: strconv.FormatUint(uint64(ref.Revision), 10), SourceRevision: ref.Revision,
		PublicationID: ref.Operation, PublicationDigest: ref.Digest,
		SourceOperation: ref.Operation, ChangeID: identity.Change.ChangeID, Cause: identity.Change.Cause,
	}
	checkpoint.TokenDigest = sourceDriverTokenDigest(checkpoint.Token)
	_, checkpointFound, err := readSourceDriverCheckpoint(ctx, tx, ref.Authority)
	if err != nil {
		return err
	}
	if err := persistSourceDriverCheckpoint(ctx, tx, checkpoint, checkpointFound); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM source_driver_checkpoint_targets WHERE source_authority = ?`, string(ref.Authority)); err != nil {
		return err
	}
	for _, target := range targets {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_checkpoint_targets(
    source_authority, tenant, generation, root_key, source_revision, catalog_revision
) VALUES (?, ?, ?, ?, ?, ?)`, string(ref.Authority), string(target.tenant), uint64(target.generation),
			string(target.root), uint64(ref.Revision), uint64(target.head)); err != nil {
			return mapConstraint(err)
		}
	}
	return nil
}

func sourceDriverSnapshotAnchorAffectedIdentity(
	ctx context.Context,
	tx *sql.Tx,
	ref SourceSnapshotStageRef,
) (uint64, [sha256.Size]byte, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT affected_key
FROM source_snapshot_affected
WHERE source_authority = ? AND snapshot_id = ?
ORDER BY affected_key`, string(ref.Authority), ref.Snapshot)
	if err != nil {
		return 0, [sha256.Size]byte{}, err
	}
	defer func() { _ = rows.Close() }()
	keys := make([]causal.LogicalKey, 0)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return 0, [sha256.Size]byte{}, err
		}
		keys = append(keys, causal.LogicalKey(key))
	}
	if err := rows.Err(); err != nil {
		return 0, [sha256.Size]byte{}, err
	}
	if len(keys) == 0 {
		return 0, [sha256.Size]byte{}, ErrIntegrity
	}
	encoded, err := json.Marshal(keys)
	if err != nil {
		return 0, [sha256.Size]byte{}, err
	}
	return uint64(len(keys)), sha256.Sum256(encoded), nil
}

func sourceDriverSnapshotAnchorTargets(
	ctx context.Context,
	tx *sql.Tx,
	ref SourceSnapshotStageRef,
) ([]sourceDriverSnapshotAnchorTarget, [sha256.Size]byte, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT root.tenant, root.generation, tenant.root_id, tenant.head
FROM source_snapshot_roots root
JOIN tenants tenant ON tenant.tenant = root.tenant
WHERE root.source_authority = ? AND root.snapshot_id = ?
ORDER BY root.tenant`, string(ref.Authority), ref.Snapshot)
	if err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	defer func() { _ = rows.Close() }()
	targets := make([]sourceDriverSnapshotAnchorTarget, 0)
	digestTargets := make([]SourceDriverTarget, 0)
	for rows.Next() {
		var tenant string
		var generation, head uint64
		var rawRoot []byte
		if err := rows.Scan(&tenant, &generation, &rawRoot, &head); err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		rootID, err := objectID(rawRoot)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		root, err := DeriveSourceDriverRootKey(ref.Authority, TenantID(tenant))
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		target := sourceDriverSnapshotAnchorTarget{
			tenant: TenantID(tenant), generation: Generation(generation), root: root,
			rootID: rootID, head: Revision(head),
		}
		if target.generation == 0 || target.head == 0 {
			return nil, [sha256.Size]byte{}, ErrIntegrity
		}
		targets = append(targets, target)
		digestTargets = append(digestTargets, SourceDriverTarget{Tenant: target.tenant, Generation: target.generation})
	}
	if err := rows.Err(); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	if len(targets) == 0 || len(targets) > SourceDriverTargetLimit {
		return nil, [sha256.Size]byte{}, ErrInvalidTransition
	}
	digest, err := SourceDriverTargetsDigest(digestTargets)
	return targets, digest, err
}

func sourceDriverSnapshotAnchorIdentityDigest(identity sourceDriverSnapshotAnchorIdentity) ([sha256.Size]byte, error) {
	payload, err := json.Marshal(identity)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(append([]byte(sourceDriverSnapshotAnchorDomain), payload...)), nil
}

func insertSourceDriverSnapshotAnchorTarget(
	ctx context.Context,
	tx *sql.Tx,
	ref SourceSnapshotStageRef,
	predecessor []byte,
	target sourceDriverSnapshotAnchorTarget,
) error {
	predecessorHead := target.head
	if len(predecessor) != 0 {
		var generation, head uint64
		var root string
		err := tx.QueryRowContext(ctx, `
SELECT generation, root_key, catalog_head
FROM source_driver_publication_targets
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND prepared = 1`,
			string(ref.Authority), predecessor, string(target.tenant)).Scan(&generation, &root, &head)
		switch {
		case err == nil && Generation(generation) == target.generation && SourceObjectKey(root) == target.root:
			predecessorHead = Revision(head)
		case err == nil:
		case errors.Is(err, sql.ErrNoRows):
		default:
			return err
		}
	}
	zero := [sha256.Size]byte{}
	catalogOperation := sourceCatalogOperation(ref.Operation, target.tenant)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_targets(
    source_authority, publication_id, tenant, generation, root_key, catalog_operation_id,
    predecessor_head, catalog_head, catalog_fingerprint, file_provider_fingerprint,
    changed, provider_changed, object_count, phase, cursor_key, cursor_object_id,
    cursor_revision, catalog_state, provider_state, next_change_sequence, prepared
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, '', zeroblob(0), 0, ?, ?, 0, 1)`,
		string(ref.Authority), ref.Operation[:], string(target.tenant), uint64(target.generation),
		string(target.root), catalogOperation[:], uint64(predecessorHead), uint64(target.head), zero[:], zero[:],
		sourceDriverTargetPrepared, zero[:], zero[:]); err != nil {
		return mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_objects(
    source_authority, publication_id, tenant, source_key, object_id, parent_id,
    revision, metadata_revision, content_revision, name, name_key, kind, mode, size,
    hash, link_target, desired_revision, observed_revision, verified_revision,
    applied_revision, mount_visible, file_provider_visible, tombstone
)
SELECT ?, ?, object.tenant, ?, object.object_id, object.parent_id,
       object.revision, object.metadata_revision, object.content_revision,
       object.name, object.name_key, object.kind, object.mode, object.size, object.hash,
       object.link_target, object.desired_revision, object.observed_revision,
       object.verified_revision, object.applied_revision, object.mount_visible,
       object.file_provider_visible, object.tombstone
FROM objects object
WHERE object.tenant = ? AND object.object_id = ?`, string(ref.Authority), ref.Operation[:],
		string(target.root), string(target.tenant), target.rootID[:]); err != nil {
		return mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_versions(
    source_authority, publication_id, tenant, object_id, parent_id, revision,
    metadata_revision, content_revision, name, name_key, kind, mode, size, hash,
    link_target, desired_revision, observed_revision, verified_revision, applied_revision,
    mount_visible, file_provider_visible, tombstone
)
SELECT ?, ?, object.tenant, object.object_id, object.parent_id, object.revision,
       object.metadata_revision, object.content_revision, object.name, object.name_key,
       object.kind, object.mode, object.size, object.hash, object.link_target,
       object.desired_revision, object.observed_revision, object.verified_revision,
       object.applied_revision, object.mount_visible, object.file_provider_visible,
       object.tombstone
FROM objects object
WHERE object.tenant = ? AND object.object_id = ?`, string(ref.Authority), ref.Operation[:],
		string(target.tenant), target.rootID[:]); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func copySourceDriverSnapshotAnchorState(ctx context.Context, tx *sql.Tx, ref SourceSnapshotStageRef) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_objects(
    source_authority, publication_id, tenant, source_key, object_id, parent_id,
    revision, metadata_revision, content_revision, name, name_key, kind, mode, size,
    hash, link_target, desired_revision, observed_revision, verified_revision,
    applied_revision, mount_visible, file_provider_visible, tombstone
)
SELECT binding.source_authority, ?, object.tenant, binding.source_key, object.object_id,
       object.parent_id, object.revision, object.metadata_revision, object.content_revision,
       object.name, object.name_key, object.kind, object.mode, object.size, object.hash,
       object.link_target, object.desired_revision, object.observed_revision,
       object.verified_revision, object.applied_revision, object.mount_visible,
       object.file_provider_visible, object.tombstone
FROM source_object_bindings binding
JOIN source_object_ids identity
  ON identity.source_authority = binding.source_authority AND identity.source_key = binding.source_key
JOIN objects object ON object.tenant = binding.tenant AND object.object_id = identity.object_id
JOIN source_snapshot_roots root
  ON root.source_authority = binding.source_authority AND root.snapshot_id = ? AND root.tenant = binding.tenant
JOIN tenants tenant ON tenant.tenant = object.tenant
WHERE binding.source_authority = ? AND object.object_id <> tenant.root_id
ORDER BY binding.tenant, binding.source_key`, ref.Operation[:], ref.Snapshot, string(ref.Authority)); err != nil {
		return mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_versions(
    source_authority, publication_id, tenant, object_id, parent_id, revision,
    metadata_revision, content_revision, name, name_key, kind, mode, size, hash,
    link_target, desired_revision, observed_revision, verified_revision, applied_revision,
    mount_visible, file_provider_visible, tombstone
)
SELECT binding.source_authority, ?, version.tenant, version.object_id, version.parent_id,
       version.revision, version.metadata_revision, version.content_revision, version.name,
       version.name_key, version.kind, version.mode, version.size, version.hash,
       version.link_target, version.desired_revision, version.observed_revision,
       version.verified_revision, version.applied_revision, version.mount_visible,
       version.file_provider_visible, version.tombstone
FROM source_object_bindings binding
JOIN source_object_ids identity
  ON identity.source_authority = binding.source_authority AND identity.source_key = binding.source_key
JOIN object_versions version
  ON version.tenant = binding.tenant AND version.object_id = identity.object_id
JOIN source_snapshot_roots root
  ON root.source_authority = binding.source_authority AND root.snapshot_id = ? AND root.tenant = binding.tenant
JOIN tenants tenant ON tenant.tenant = version.tenant
WHERE binding.source_authority = ? AND version.object_id <> tenant.root_id
  AND version.revision <= tenant.head
ORDER BY version.tenant, version.object_id, version.revision`,
		ref.Operation[:], ref.Snapshot, string(ref.Authority)); err != nil {
		return mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_changes(
    source_authority, publication_id, tenant, revision, scope_kind, presentation,
    scope_parent, scope_domain, scope_generation, sequence, kind, object_id, object_revision
)
SELECT binding.source_authority, ?, change.tenant, change.revision, change.scope_kind,
       change.presentation, change.scope_parent, change.scope_domain, change.scope_generation,
       change.sequence, change.kind, change.object_id, change.object_revision
FROM changes change
JOIN source_object_ids identity
  ON identity.source_authority = ? AND identity.object_id = change.object_id
JOIN source_object_bindings binding
  ON binding.source_authority = identity.source_authority AND binding.source_key = identity.source_key
 AND binding.tenant = change.tenant
JOIN source_snapshot_roots root
  ON root.source_authority = binding.source_authority AND root.snapshot_id = ? AND root.tenant = binding.tenant
JOIN tenants tenant ON tenant.tenant = change.tenant
WHERE change.revision <= tenant.head
ORDER BY change.tenant, change.revision, change.scope_kind, change.presentation,
         change.scope_parent, change.scope_domain, change.scope_generation, change.sequence`,
		ref.Operation[:], string(ref.Authority), ref.Snapshot); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func sourceDriverSnapshotAnchorFingerprints(
	ctx context.Context,
	tx *sql.Tx,
	ref SourceSnapshotStageRef,
	target sourceDriverSnapshotAnchorTarget,
) ([sha256.Size]byte, [sha256.Size]byte, uint64, error) {
	catalogFingerprint := chainSourceDriverFingerprint(
		"fusekit.source-driver-catalog.v1", [sha256.Size]byte{},
		encodeSourceDriverFingerprintRow(
			string(target.tenant), nil, string(target.root), 0, 0, int64(target.generation), nil, "", false, false,
		),
	)
	rows, err := tx.QueryContext(ctx, `
SELECT source_key, parent_id, name, kind, mode, size, hash, link_target,
       mount_visible, file_provider_visible
FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND tombstone = 0
ORDER BY source_key`, string(ref.Authority), ref.Operation[:], string(target.tenant))
	if err != nil {
		return [sha256.Size]byte{}, [sha256.Size]byte{}, 0, err
	}
	var count uint64
	for rows.Next() {
		var key, name, link string
		var parent, hash []byte
		var kind uint8
		var mode uint32
		var size int64
		var mount, provider bool
		if err := rows.Scan(&key, &parent, &name, &kind, &mode, &size, &hash, &link, &mount, &provider); err != nil {
			_ = rows.Close()
			return [sha256.Size]byte{}, [sha256.Size]byte{}, 0, err
		}
		catalogFingerprint = chainSourceDriverFingerprint(
			"fusekit.source-driver-catalog.v1", catalogFingerprint,
			encodeSourceDriverFingerprintRow(key, parent, name, kind, mode, size, hash, link, mount, provider),
		)
		count++
	}
	if err := rows.Close(); err != nil {
		return [sha256.Size]byte{}, [sha256.Size]byte{}, 0, err
	}
	if err := rows.Err(); err != nil {
		return [sha256.Size]byte{}, [sha256.Size]byte{}, 0, err
	}
	providerFingerprint := chainSourceDriverFingerprint(
		"fusekit.source-driver-file-provider.v1", [sha256.Size]byte{}, target.rootID[:],
	)
	rows, err = tx.QueryContext(ctx, `
SELECT object_id, parent_id, name, kind, mode, size, hash, link_target
FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND tombstone = 0 AND file_provider_visible = 1
ORDER BY object_id`, string(ref.Authority), ref.Operation[:], string(target.tenant))
	if err != nil {
		return [sha256.Size]byte{}, [sha256.Size]byte{}, 0, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, parent, hash []byte
		var name, link string
		var kind uint8
		var mode uint32
		var size int64
		if err := rows.Scan(&id, &parent, &name, &kind, &mode, &size, &hash, &link); err != nil {
			return [sha256.Size]byte{}, [sha256.Size]byte{}, 0, err
		}
		providerFingerprint = chainSourceDriverFingerprint(
			"fusekit.source-driver-file-provider.v1", providerFingerprint,
			encodeSourceDriverFingerprintRow(string(id), parent, name, kind, mode, size, hash, link, true, true),
		)
	}
	if err := rows.Err(); err != nil {
		return [sha256.Size]byte{}, [sha256.Size]byte{}, 0, err
	}
	return catalogFingerprint, providerFingerprint, count, nil
}
