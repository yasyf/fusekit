package catalog

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

const (
	snapshotBeforeTable = "temp.fusekit_snapshot_before"
	snapshotAfterTable  = "temp.fusekit_snapshot_after"
	snapshotEventsTable = "temp.fusekit_snapshot_events"
)

func (c *Catalog) promoteStagedSnapshotSetwise(
	ctx context.Context,
	tx *sql.Tx,
	ref SourceSnapshotStageRef,
	_ causal.ChangeSet,
) (resultErr error) {
	if err := bindStagedSnapshotRoots(ctx, tx, ref); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_object_bindings(source_authority, tenant, source_key)
SELECT DISTINCT object.source_authority, object.tenant, object.source_key
FROM source_snapshot_objects object
WHERE object.source_authority = ? AND object.snapshot_id = ?
ON CONFLICT(source_authority, tenant, source_key) DO NOTHING`, string(ref.Authority), ref.Snapshot); err != nil {
		return mapConstraint(err)
	}
	var invalid int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM source_snapshot_objects staged
JOIN source_snapshot_bindings binding
  ON binding.source_authority = staged.source_authority
 AND binding.snapshot_id = staged.snapshot_id AND binding.source_key = staged.source_key
JOIN objects current ON current.tenant = staged.tenant AND current.object_id = binding.object_id
WHERE staged.source_authority = ? AND staged.snapshot_id = ? AND current.kind <> staged.object_kind`,
		string(ref.Authority), ref.Snapshot).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("%w: source object kind changed", ErrInvalidObject)
	}
	if err := persistStagedSnapshotProjectionFingerprints(ctx, tx, ref); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE tenants SET head = head + 1
WHERE EXISTS (
    SELECT 1 FROM source_snapshot_roots root
    LEFT JOIN source_tenant_targets target
      ON target.source_authority = root.source_authority AND target.tenant = root.tenant
    WHERE root.source_authority = ? AND root.snapshot_id = ? AND root.tenant = tenants.tenant
      AND (target.tenant IS NULL OR target.catalog_fingerprint <> root.catalog_fingerprint)
)`, string(ref.Authority), ref.Snapshot); err != nil {
		return fmt.Errorf("catalog: advance staged snapshot tenants: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_snapshot_roots SET catalog_revision = CASE WHEN EXISTS (
    SELECT 1 FROM source_tenant_targets target
    WHERE target.source_authority = source_snapshot_roots.source_authority
      AND target.tenant = source_snapshot_roots.tenant
      AND target.catalog_fingerprint = source_snapshot_roots.catalog_fingerprint
) THEN 0 ELSE (
    SELECT head FROM tenants WHERE tenants.tenant = source_snapshot_roots.tenant
) END
WHERE source_authority = ? AND snapshot_id = ? AND catalog_revision = 0`,
		string(ref.Authority), ref.Snapshot); err != nil {
		return mapConstraint(err)
	}
	if err := createStagedSnapshotMutationTables(ctx, tx, ref); err != nil {
		return err
	}
	defer func() {
		resultErr = joinSnapshotPromotionError(resultErr, dropStagedSnapshotMutationTables(ctx, tx))
	}()
	if err := validateStagedSnapshotDeletions(ctx, tx); err != nil {
		return err
	}
	if err := appendStagedSnapshotVersions(ctx, tx); err != nil {
		return err
	}
	if err := applyStagedSnapshotObjects(ctx, tx); err != nil {
		return err
	}
	if err := appendStagedSnapshotChanges(ctx, tx); err != nil {
		return err
	}
	return nil
}

type stagedSnapshotFingerprintRoot struct {
	tenant     TenantID
	generation Generation
	root       SourceObjectKey
	rootObject ObjectID
}

func persistStagedSnapshotProjectionFingerprints(
	ctx context.Context,
	tx *sql.Tx,
	ref SourceSnapshotStageRef,
) error {
	var after TenantID
	for {
		rows, err := tx.QueryContext(ctx, `
SELECT root.tenant, root.generation, root.root_key, tenant.root_id
FROM source_snapshot_roots root
JOIN tenants tenant ON tenant.tenant = root.tenant
WHERE root.source_authority = ? AND root.snapshot_id = ? AND root.tenant > ?
ORDER BY root.tenant LIMIT ?`, string(ref.Authority), ref.Snapshot, string(after), SourceSnapshotPageLimit)
		if err != nil {
			return fmt.Errorf("catalog: page staged snapshot fingerprint roots: %w", err)
		}
		roots := make([]stagedSnapshotFingerprintRoot, 0, SourceSnapshotPageLimit)
		for rows.Next() {
			var tenant, root string
			var generation uint64
			var rawRoot []byte
			if err := rows.Scan(&tenant, &generation, &root, &rawRoot); err != nil {
				_ = rows.Close()
				return fmt.Errorf("catalog: scan staged snapshot fingerprint root: %w", err)
			}
			rootObject, err := objectID(rawRoot)
			if err != nil {
				_ = rows.Close()
				return err
			}
			roots = append(roots, stagedSnapshotFingerprintRoot{
				tenant: TenantID(tenant), generation: Generation(generation), root: SourceObjectKey(root),
				rootObject: rootObject,
			})
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("catalog: close staged snapshot fingerprint roots: %w", err)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("catalog: read staged snapshot fingerprint roots: %w", err)
		}
		for _, root := range roots {
			fingerprints, err := stagedSnapshotProjectionFingerprints(ctx, tx, ref, root)
			if err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `
UPDATE source_snapshot_roots
SET catalog_fingerprint = ?, file_provider_fingerprint = ?
WHERE source_authority = ? AND snapshot_id = ? AND tenant = ?
  AND catalog_fingerprint IS NULL AND file_provider_fingerprint IS NULL`,
				fingerprints.catalog[:], fingerprints.fileProvider[:],
				string(ref.Authority), ref.Snapshot, string(root.tenant))
			if err != nil {
				return mapConstraint(err)
			}
			if changed, _ := result.RowsAffected(); changed != 1 {
				return fmt.Errorf("%w: staged snapshot fingerprint ownership changed", ErrInvalidTransition)
			}
		}
		if len(roots) < SourceSnapshotPageLimit {
			return nil
		}
		after = roots[len(roots)-1].tenant
	}
}

func stagedSnapshotProjectionFingerprints(
	ctx context.Context,
	tx *sql.Tx,
	ref SourceSnapshotStageRef,
	root stagedSnapshotFingerprintRoot,
) (sourceTenantFingerprints, error) {
	catalogBuilder, err := newSourceCatalogFingerprintBuilder(root.generation, root.root)
	if err != nil {
		return sourceTenantFingerprints{}, err
	}
	rows, err := tx.QueryContext(ctx, `
SELECT object.source_key, object.parent_key, object.object_name, object.object_kind,
       object.object_mode, object.content_size, object.content_hash, object.link_target,
       object.mount_visible, object.file_provider_visible
FROM source_snapshot_objects object
WHERE object.source_authority = ? AND object.snapshot_id = ? AND object.tenant = ?
ORDER BY object.source_key`, string(ref.Authority), ref.Snapshot, string(root.tenant))
	if err != nil {
		return sourceTenantFingerprints{}, fmt.Errorf("catalog: query staged snapshot fingerprint objects: %w", err)
	}
	for rows.Next() {
		var key, parent, name, link string
		var kind uint8
		var mode uint32
		var size int64
		var rawHash []byte
		var mount, provider bool
		if err := rows.Scan(
			&key, &parent, &name, &kind, &mode, &size, &rawHash, &link, &mount, &provider,
		); err != nil {
			_ = rows.Close()
			return sourceTenantFingerprints{}, fmt.Errorf("catalog: scan staged snapshot fingerprint object: %w", err)
		}
		if len(rawHash) != len(ContentHash{}) {
			_ = rows.Close()
			return sourceTenantFingerprints{}, fmt.Errorf("%w: corrupt staged snapshot content hash", ErrIntegrity)
		}
		var contentHash ContentHash
		copy(contentHash[:], rawHash)
		if err := catalogBuilder.add(sourceFingerprintObject{
			key: SourceObjectKey(key), parent: SourceObjectKey(parent), name: name,
			kind: Kind(kind), mode: mode, size: size, hash: contentHash, linkTarget: link,
			visibility: Visibility{Mount: mount, FileProvider: provider},
		}); err != nil {
			_ = rows.Close()
			return sourceTenantFingerprints{}, err
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return sourceTenantFingerprints{}, fmt.Errorf("catalog: read staged snapshot fingerprint objects: %w", err)
	}
	if err := rows.Close(); err != nil {
		return sourceTenantFingerprints{}, err
	}
	fingerprints := sourceTenantFingerprints{catalog: catalogBuilder.sum()}
	providerBuilder, err := newFileProviderFingerprintBuilder(root.rootObject)
	if err != nil {
		return sourceTenantFingerprints{}, err
	}
	rows, err = tx.QueryContext(ctx, `
SELECT binding.object_id, COALESCE(parent_binding.object_id, tenant.root_id),
       object.object_name, object.object_kind, object.object_mode,
       object.content_size, object.content_hash, object.link_target
FROM source_snapshot_objects object
JOIN source_snapshot_bindings binding
  ON binding.source_authority = object.source_authority
 AND binding.snapshot_id = object.snapshot_id AND binding.source_key = object.source_key
JOIN tenants tenant ON tenant.tenant = object.tenant
LEFT JOIN source_snapshot_bindings parent_binding
  ON parent_binding.source_authority = object.source_authority
 AND parent_binding.snapshot_id = object.snapshot_id
 AND parent_binding.source_key = object.parent_key
WHERE object.source_authority = ? AND object.snapshot_id = ? AND object.tenant = ?
  AND object.file_provider_visible = 1
ORDER BY binding.object_id`, string(ref.Authority), ref.Snapshot, string(root.tenant))
	if err != nil {
		return sourceTenantFingerprints{}, err
	}
	for rows.Next() {
		var rawObject, rawParent, rawHash []byte
		var name, link string
		var kind uint8
		var mode uint32
		var size int64
		if err := rows.Scan(
			&rawObject, &rawParent, &name, &kind, &mode, &size, &rawHash, &link,
		); err != nil {
			_ = rows.Close()
			return sourceTenantFingerprints{}, err
		}
		object, err := objectID(rawObject)
		if err != nil {
			_ = rows.Close()
			return sourceTenantFingerprints{}, err
		}
		parent, err := objectID(rawParent)
		if err != nil {
			_ = rows.Close()
			return sourceTenantFingerprints{}, err
		}
		if len(rawHash) != len(ContentHash{}) {
			_ = rows.Close()
			return sourceTenantFingerprints{}, ErrIntegrity
		}
		var hash ContentHash
		copy(hash[:], rawHash)
		if err := providerBuilder.add(fileProviderFingerprintObject{
			id: object, parent: parent, name: name, kind: Kind(kind), mode: mode,
			size: size, hash: hash, linkTarget: link,
		}); err != nil {
			_ = rows.Close()
			return sourceTenantFingerprints{}, err
		}
	}
	if err := rows.Close(); err != nil {
		return sourceTenantFingerprints{}, err
	}
	fingerprints.fileProvider = providerBuilder.sum()
	return fingerprints, nil
}

func bindStagedSnapshotRoots(ctx context.Context, tx *sql.Tx, ref SourceSnapshotStageRef) error {
	var mismatched int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM source_snapshot_roots staged
JOIN source_tenant_roots durable
  ON durable.source_authority = staged.source_authority AND durable.tenant = staged.tenant
WHERE staged.source_authority = ? AND staged.snapshot_id = ? AND durable.root_key <> staged.root_key`,
		string(ref.Authority), ref.Snapshot).Scan(&mismatched); err != nil {
		return err
	}
	if mismatched != 0 {
		return ErrSourceLocatorStale
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_tenant_roots(source_authority, tenant, root_key)
SELECT source_authority, tenant, root_key FROM source_snapshot_roots
WHERE source_authority = ? AND snapshot_id = ?
ON CONFLICT(source_authority, tenant) DO NOTHING`, string(ref.Authority), ref.Snapshot); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func createStagedSnapshotMutationTables(ctx context.Context, tx *sql.Tx, ref SourceSnapshotStageRef) error {
	if err := dropStagedSnapshotMutationTables(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
CREATE TEMP TABLE fusekit_snapshot_before AS
SELECT object.*, root.catalog_revision AS target_revision
FROM objects object
JOIN source_object_bindings owned ON owned.tenant = object.tenant
JOIN source_object_ids identity
  ON identity.source_authority = owned.source_authority AND identity.source_key = owned.source_key
 AND identity.object_id = object.object_id
JOIN source_snapshot_roots root
  ON root.source_authority = owned.source_authority AND root.tenant = owned.tenant
WHERE root.source_authority = ? AND root.snapshot_id = ?`, string(ref.Authority), ref.Snapshot); err != nil {
		return fmt.Errorf("catalog: stage snapshot before image: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
CREATE UNIQUE INDEX temp.fusekit_snapshot_before_id ON fusekit_snapshot_before(tenant, object_id)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
CREATE TEMP TABLE fusekit_snapshot_after AS
SELECT staged.sequence, staged.tenant, binding.object_id,
       CASE WHEN staged.parent_key = '' THEN tenant.root_id ELSE parent.object_id END AS parent_id,
       root.catalog_revision AS revision,
       CASE WHEN current.object_id IS NOT NULL AND current.tombstone = 0
                  AND current.parent_id = CASE WHEN staged.parent_key = '' THEN tenant.root_id ELSE parent.object_id END
                  AND current.name = staged.object_name AND current.mode = staged.object_mode
                  AND current.link_target = staged.link_target
                  AND current.mount_visible = staged.mount_visible
                  AND current.file_provider_visible = staged.file_provider_visible
            THEN current.metadata_revision ELSE root.catalog_revision END AS metadata_revision,
       CASE WHEN current.object_id IS NOT NULL AND current.tombstone = 0
                  AND current.kind = staged.object_kind
                  AND current.size = staged.content_size AND current.hash = staged.content_hash
                  AND current.link_target = staged.link_target
            THEN current.content_revision ELSE staged.content_revision END AS content_revision,
       staged.object_name AS name, staged.name_key,
       staged.object_kind AS kind, staged.object_mode AS mode, staged.content_size AS size,
       staged.content_hash AS hash, staged.link_target,
       CASE WHEN current.object_id IS NOT NULL AND current.tombstone = 0
                  AND current.kind = staged.object_kind
                  AND current.size = staged.content_size AND current.hash = staged.content_hash
                  AND current.link_target = staged.link_target
            THEN current.desired_revision ELSE 0 END AS desired_revision,
       CASE WHEN current.object_id IS NOT NULL AND current.tombstone = 0
                  AND current.kind = staged.object_kind
                  AND current.size = staged.content_size AND current.hash = staged.content_hash
                  AND current.link_target = staged.link_target
            THEN current.observed_revision ELSE 0 END AS observed_revision,
       CASE WHEN current.object_id IS NOT NULL AND current.tombstone = 0
                  AND current.kind = staged.object_kind
                  AND current.size = staged.content_size AND current.hash = staged.content_hash
                  AND current.link_target = staged.link_target
            THEN current.verified_revision ELSE 0 END AS verified_revision,
       CASE WHEN current.object_id IS NOT NULL AND current.tombstone = 0
                  AND current.kind = staged.object_kind
                  AND current.size = staged.content_size AND current.hash = staged.content_hash
                  AND current.link_target = staged.link_target
            THEN current.applied_revision ELSE 0 END AS applied_revision,
       staged.mount_visible, staged.file_provider_visible, 0 AS tombstone,
       CASE WHEN current.object_id IS NOT NULL AND current.tombstone = 0
                  AND current.parent_id = CASE WHEN staged.parent_key = '' THEN tenant.root_id ELSE parent.object_id END
                  AND current.name = staged.object_name AND current.kind = staged.object_kind
                  AND current.mode = staged.object_mode
                  AND current.size = staged.content_size AND current.hash = staged.content_hash
                  AND current.link_target = staged.link_target
                  AND current.mount_visible = staged.mount_visible
                  AND current.file_provider_visible = staged.file_provider_visible
            THEN 0 ELSE 1 END AS changed
FROM source_snapshot_objects staged
JOIN source_snapshot_bindings binding
  ON binding.source_authority = staged.source_authority AND binding.snapshot_id = staged.snapshot_id
 AND binding.source_key = staged.source_key
JOIN source_snapshot_roots root
  ON root.source_authority = staged.source_authority AND root.snapshot_id = staged.snapshot_id
 AND root.tenant = staged.tenant AND root.generation = staged.generation
JOIN tenants tenant ON tenant.tenant = staged.tenant
LEFT JOIN source_snapshot_bindings parent
  ON parent.source_authority = staged.source_authority AND parent.snapshot_id = staged.snapshot_id
 AND parent.source_key = staged.parent_key
LEFT JOIN objects current ON current.tenant = staged.tenant AND current.object_id = binding.object_id
WHERE staged.source_authority = ? AND staged.snapshot_id = ?`, string(ref.Authority), ref.Snapshot); err != nil {
		return fmt.Errorf("catalog: stage snapshot after image: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
CREATE UNIQUE INDEX temp.fusekit_snapshot_after_id ON fusekit_snapshot_after(tenant, object_id)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, snapshotEventsSQL); err != nil {
		return fmt.Errorf("catalog: stage snapshot changes: %w", err)
	}
	return nil
}

const snapshotEventsSQL = `
CREATE TEMP TABLE fusekit_snapshot_events AS
WITH removed_or_changed AS (
    SELECT before.*, after.object_id AS after_id, after.parent_id AS after_parent,
           after.revision AS after_revision, after.mount_visible AS after_mount,
           after.file_provider_visible AS after_provider, after.changed AS after_changed
    FROM fusekit_snapshot_before before
    LEFT JOIN fusekit_snapshot_after after
      ON after.tenant = before.tenant AND after.object_id = before.object_id
    WHERE before.tombstone = 0 AND (after.object_id IS NULL OR after.changed = 1)
), changed_after AS (
    SELECT after.* FROM fusekit_snapshot_after after WHERE after.changed = 1
)
SELECT tenant, revision, scope_kind, presentation, scope_parent, scope_domain, scope_generation,
       kind, object_id, object_revision, event_order
FROM (
    SELECT before.tenant, before.target_revision AS revision, 2 AS scope_kind, 1 AS presentation,
           before.parent_id AS scope_parent, '' AS scope_domain, 0 AS scope_generation,
           1 AS kind, before.object_id, before.revision AS object_revision, before.object_id AS event_order
    FROM removed_or_changed before
    WHERE before.mount_visible = 1
      AND (before.after_id IS NULL OR before.after_mount = 0 OR before.after_parent <> before.parent_id)
    UNION ALL
    SELECT before.tenant, before.target_revision, 2, 2, before.parent_id, '', 0,
           1, before.object_id, before.revision, before.object_id
    FROM removed_or_changed before
    WHERE before.file_provider_visible = 1
      AND (before.after_id IS NULL OR before.after_provider = 0 OR before.after_parent <> before.parent_id)
    UNION ALL
    SELECT after.tenant, after.revision, 2, 1, after.parent_id, '', 0,
           2, after.object_id, after.revision, after.object_id
    FROM changed_after after WHERE after.mount_visible = 1
    UNION ALL
    SELECT after.tenant, after.revision, 2, 2, after.parent_id, '', 0,
           2, after.object_id, after.revision, after.object_id
    FROM changed_after after WHERE after.file_provider_visible = 1
    UNION ALL
    SELECT before.tenant, before.target_revision, 1, 2, zeroblob(16), interest.owner_domain,
           interest.owner_generation,
           CASE WHEN before.after_id IS NULL OR before.after_provider = 0 THEN 1 ELSE 2 END,
           before.object_id,
           CASE WHEN before.after_id IS NULL OR before.after_provider = 0
                THEN before.revision ELSE before.after_revision END,
           before.object_id
    FROM removed_or_changed before
    JOIN materialization_interests interest
      ON interest.tenant = before.tenant AND interest.object_id = before.object_id
     AND interest.owner_presentation = 2 AND interest.removed_revision IS NULL
    WHERE before.file_provider_visible = 1 OR before.after_provider = 1
    UNION ALL
    SELECT after.tenant, after.revision, 1, 2, zeroblob(16), interest.owner_domain,
           interest.owner_generation, 2, after.object_id, after.revision, after.object_id
    FROM changed_after after
    LEFT JOIN fusekit_snapshot_before before
      ON before.tenant = after.tenant AND before.object_id = after.object_id
    JOIN materialization_interests interest
      ON interest.tenant = after.tenant AND interest.object_id = after.object_id
     AND interest.owner_presentation = 2 AND interest.removed_revision IS NULL
    WHERE before.object_id IS NULL AND after.file_provider_visible = 1
)`

func validateStagedSnapshotDeletions(ctx context.Context, tx *sql.Tx) error {
	var blocked int
	if err := tx.QueryRowContext(ctx, `
WITH removed AS (
    SELECT before.tenant, before.object_id
    FROM fusekit_snapshot_before before
    LEFT JOIN fusekit_snapshot_after after
      ON after.tenant = before.tenant AND after.object_id = before.object_id
    WHERE before.tombstone = 0 AND after.object_id IS NULL
)
SELECT COUNT(*)
FROM removed parent
JOIN objects child
  ON child.tenant = parent.tenant AND child.parent_id = parent.object_id AND child.tombstone = 0
LEFT JOIN removed child_removed
  ON child_removed.tenant = child.tenant AND child_removed.object_id = child.object_id
LEFT JOIN fusekit_snapshot_after child_after
  ON child_after.tenant = child.tenant AND child_after.object_id = child.object_id
WHERE child_removed.object_id IS NULL AND child_after.object_id IS NULL`).Scan(&blocked); err != nil {
		return err
	}
	if blocked != 0 {
		return fmt.Errorf("%w: staged snapshot delete would orphan non-source children", ErrConflict)
	}
	return nil
}

func appendStagedSnapshotVersions(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO object_versions(
    tenant, object_id, parent_id, revision, metadata_revision, content_revision,
    name, name_key, kind, mode, size, hash, link_target, desired_revision, observed_revision,
    verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
)
SELECT before.tenant, before.object_id, before.parent_id, before.target_revision, before.target_revision,
       before.content_revision, before.name, before.name_key, before.kind, before.mode, before.size,
       before.hash, before.link_target, before.desired_revision, before.observed_revision,
       before.verified_revision, before.applied_revision, 0, 0, 1
FROM fusekit_snapshot_before before
LEFT JOIN fusekit_snapshot_after after
  ON after.tenant = before.tenant AND after.object_id = before.object_id
WHERE before.tombstone = 0 AND after.object_id IS NULL`); err != nil {
		return fmt.Errorf("catalog: append staged snapshot tombstones: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO object_versions(
    tenant, object_id, parent_id, revision, metadata_revision, content_revision,
    name, name_key, kind, mode, size, hash, link_target, desired_revision, observed_revision,
    verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
)
SELECT tenant, object_id, parent_id, revision, metadata_revision, content_revision,
       name, name_key, kind, mode, size, hash, link_target, desired_revision, observed_revision,
       verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
FROM fusekit_snapshot_after WHERE changed = 1`); err != nil {
		return fmt.Errorf("catalog: append staged snapshot object versions: %w", err)
	}
	return nil
}

func applyStagedSnapshotObjects(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
UPDATE objects AS object SET tombstone = 1
WHERE EXISTS (
    SELECT 1 FROM fusekit_snapshot_before before
    LEFT JOIN fusekit_snapshot_after after
      ON after.tenant = before.tenant AND after.object_id = before.object_id
    WHERE before.tenant = object.tenant AND before.object_id = object.object_id AND before.tombstone = 0
      AND (after.object_id IS NULL OR (after.changed = 1 AND
          (after.parent_id <> before.parent_id OR after.name <> before.name OR
           after.mount_visible <> before.mount_visible OR
           after.file_provider_visible <> before.file_provider_visible)))
)`); err != nil {
		return mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE objects AS object SET
    revision = (SELECT before.target_revision FROM fusekit_snapshot_before before
                WHERE before.tenant = object.tenant AND before.object_id = object.object_id),
    metadata_revision = (SELECT before.target_revision FROM fusekit_snapshot_before before
                         WHERE before.tenant = object.tenant AND before.object_id = object.object_id),
    mount_visible = 0, file_provider_visible = 0, tombstone = 1
WHERE EXISTS (
    SELECT 1 FROM fusekit_snapshot_before before
    LEFT JOIN fusekit_snapshot_after after
      ON after.tenant = before.tenant AND after.object_id = before.object_id
    WHERE before.tenant = object.tenant AND before.object_id = object.object_id
      AND before.tombstone = 0 AND after.object_id IS NULL
)`); err != nil {
		return mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE objects AS object SET (
    parent_id, revision, metadata_revision, content_revision, name, name_key, kind, mode,
    size, hash, link_target, desired_revision, observed_revision, verified_revision,
    applied_revision, mount_visible, file_provider_visible, tombstone
) = (
    SELECT parent_id, revision, metadata_revision, content_revision, name, name_key, kind, mode,
           size, hash, link_target, desired_revision, observed_revision, verified_revision,
           applied_revision, mount_visible, file_provider_visible, tombstone
    FROM fusekit_snapshot_after after
    WHERE after.tenant = object.tenant AND after.object_id = object.object_id AND after.changed = 1
)
WHERE EXISTS (
    SELECT 1 FROM fusekit_snapshot_after after
    WHERE after.tenant = object.tenant AND after.object_id = object.object_id AND after.changed = 1
)`); err != nil {
		return mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO objects(
    tenant, object_id, parent_id, revision, metadata_revision, content_revision,
    name, name_key, kind, mode, size, hash, link_target, desired_revision, observed_revision,
    verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
)
SELECT after.tenant, after.object_id, after.parent_id, after.revision, after.metadata_revision,
       after.content_revision, after.name, after.name_key, after.kind, after.mode, after.size,
       after.hash, after.link_target, after.desired_revision, after.observed_revision,
       after.verified_revision, after.applied_revision, after.mount_visible,
       after.file_provider_visible, after.tombstone
FROM fusekit_snapshot_after after
LEFT JOIN fusekit_snapshot_before before
  ON before.tenant = after.tenant AND before.object_id = after.object_id
WHERE before.object_id IS NULL`); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func appendStagedSnapshotChanges(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO changes(
    tenant, revision, scope_kind, presentation, scope_parent, scope_domain, scope_generation,
    sequence, kind, object_id, object_revision
)
SELECT tenant, revision, scope_kind, presentation, scope_parent, scope_domain, scope_generation,
       ROW_NUMBER() OVER (
           PARTITION BY tenant, revision, scope_kind, presentation, scope_parent, scope_domain, scope_generation
           ORDER BY event_order, kind
       ) - 1,
       kind, object_id, object_revision
FROM fusekit_snapshot_events`); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func dropStagedSnapshotMutationTables(ctx context.Context, tx *sql.Tx) error {
	for _, table := range []string{snapshotEventsTable, snapshotAfterTable, snapshotBeforeTable} {
		if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return err
		}
	}
	return nil
}

func joinSnapshotPromotionError(primary, cleanup error) error {
	if primary != nil {
		return primary
	}
	return cleanup
}
