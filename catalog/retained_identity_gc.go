package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// RetainedIdentityPageLimit bounds all retained-identity work in one
// transaction.
const RetainedIdentityPageLimit = 256

const retainedIdentityAfterDelete = "retained_identity.after_delete"

// RetainedIdentityGCResult reports one bounded retained-identity collection
// page.
type RetainedIdentityGCResult struct {
	Handles        int
	MutationPins   int
	ObjectVersions int
	Objects        int
	Owners         int
	More           bool
}

// CollectRetainedIdentityGarbage retires at most one shared page of identities
// that are unreachable from every valid catalog snapshot and live owner.
func (c *Catalog) CollectRetainedIdentityGarbage(
	ctx context.Context,
	tenant TenantID,
	safeFloor Revision,
) (RetainedIdentityGCResult, error) {
	if tenant == "" {
		return RetainedIdentityGCResult{}, fmt.Errorf("%w: empty retained-identity tenant", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return RetainedIdentityGCResult{}, fmt.Errorf("catalog: begin retained-identity collection: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var persisted uint64
	if err := tx.QueryRowContext(ctx,
		"SELECT floor FROM tenants WHERE tenant = ?", string(tenant)).Scan(&persisted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RetainedIdentityGCResult{}, ErrNotFound
		}
		return RetainedIdentityGCResult{}, fmt.Errorf("catalog: read retained-identity floor: %w", err)
	}
	if Revision(persisted) != safeFloor {
		return RetainedIdentityGCResult{}, fmt.Errorf(
			"%w: retained-identity floor %d, persisted %d",
			ErrInvalidTransition, safeFloor, persisted,
		)
	}
	result := RetainedIdentityGCResult{}
	remaining := RetainedIdentityPageLimit
	var more bool
	result.Handles, more, err = collectRetiredHandles(ctx, tx, tenant, remaining)
	if err != nil {
		return RetainedIdentityGCResult{}, err
	}
	remaining -= result.Handles
	result.More = result.More || more
	if remaining > 0 {
		result.MutationPins, more, err = collectRetiredMutationPins(ctx, tx, tenant, remaining)
		if err != nil {
			return RetainedIdentityGCResult{}, err
		}
		remaining -= result.MutationPins
		result.More = result.More || more
	}
	if remaining > 0 {
		result.ObjectVersions, more, err = collectTombstoneVersions(ctx, tx, tenant, safeFloor, remaining)
		if err != nil {
			return RetainedIdentityGCResult{}, err
		}
		remaining -= result.ObjectVersions
		result.More = result.More || more
	}
	if remaining > 0 {
		result.Objects, more, err = collectTombstoneObjects(ctx, tx, tenant, safeFloor, remaining)
		if err != nil {
			return RetainedIdentityGCResult{}, err
		}
		remaining -= result.Objects
		result.More = result.More || more
	}
	if remaining > 0 {
		result.Owners, more, err = collectRetiredOwners(ctx, tx, remaining)
		if err != nil {
			return RetainedIdentityGCResult{}, err
		}
		remaining -= result.Owners
		result.More = result.More || more
	}
	if remaining == 0 {
		result.More = true
	}
	if err := c.trip(retainedIdentityAfterDelete); err != nil {
		return RetainedIdentityGCResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return RetainedIdentityGCResult{}, fmt.Errorf("catalog: commit retained-identity collection: %w", err)
	}
	return result, nil
}

func collectRetiredHandles(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	limit int,
) (int, bool, error) {
	return collectClosedRetentionRows(ctx, tx, tenant, limit, "handles", "handle_id")
}

func collectRetiredMutationPins(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	limit int,
) (int, bool, error) {
	return collectClosedRetentionRows(ctx, tx, tenant, limit, "mutation_pins", "pin_id")
}

func collectClosedRetentionRows(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	limit int,
	table string,
	identity string,
) (int, bool, error) {
	query := `
SELECT retained.` + identity + `
FROM ` + table + ` retained
JOIN retention_owners owner
  ON owner.owner_id = retained.owner_id
 AND owner.session_owner = retained.session_owner
WHERE retained.tenant = ? AND retained.closed = 1 AND owner.retired = 1
ORDER BY retained.` + identity + `
LIMIT ?`
	rows, err := tx.QueryContext(ctx, query, string(tenant), limit+1)
	if err != nil {
		return 0, false, fmt.Errorf("catalog: select retired %s: %w", table, err)
	}
	ids, err := retainedIDs(rows)
	if err != nil {
		return 0, false, fmt.Errorf("catalog: read retired %s: %w", table, err)
	}
	more := len(ids) > limit
	if more {
		ids = ids[:limit]
	}
	statement := "DELETE FROM " + table + " WHERE " + identity + " = ? AND closed = 1"
	for _, id := range ids {
		result, err := tx.ExecContext(ctx, statement, id[:])
		if err != nil {
			return 0, false, fmt.Errorf("catalog: delete retired %s: %w", table, err)
		}
		if err := requireOneRow(result, "retired "+table); err != nil {
			return 0, false, err
		}
	}
	return len(ids), more, nil
}

type tombstoneVersion struct {
	object   ObjectID
	revision Revision
	kind     Kind
	hash     ContentHash
}

func collectTombstoneVersions(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	floor Revision,
	limit int,
) (int, bool, error) {
	rows, err := tx.QueryContext(ctx, tombstoneVersionCandidates,
		string(tenant), uint64(floor), limit+1)
	if err != nil {
		return 0, false, fmt.Errorf("catalog: select tombstoned object versions: %w", err)
	}
	var versions []tombstoneVersion
	for rows.Next() {
		var rawObject, rawHash []byte
		var revision uint64
		var kind uint8
		if err := rows.Scan(&rawObject, &revision, &kind, &rawHash); err != nil {
			_ = rows.Close()
			return 0, false, fmt.Errorf("catalog: scan tombstoned object version: %w", err)
		}
		if len(rawObject) != len(ObjectID{}) || len(rawHash) != len(ContentHash{}) {
			_ = rows.Close()
			return 0, false, fmt.Errorf("%w: tombstoned object version identity", ErrIntegrity)
		}
		var object ObjectID
		var hash ContentHash
		copy(object[:], rawObject)
		copy(hash[:], rawHash)
		versions = append(versions, tombstoneVersion{
			object: object, revision: Revision(revision), kind: Kind(kind), hash: hash,
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, false, fmt.Errorf("catalog: read tombstoned object versions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, false, fmt.Errorf("catalog: close tombstoned object versions: %w", err)
	}
	more := len(versions) > limit
	if more {
		versions = versions[:limit]
	}
	for _, version := range versions {
		if version.kind == KindFile {
			if _, err := tx.ExecContext(ctx,
				"INSERT OR IGNORE INTO blob_gc_candidates(hash) VALUES (?)",
				version.hash[:]); err != nil {
				return 0, false, fmt.Errorf("catalog: queue tombstoned object blob: %w", err)
			}
		}
		result, err := tx.ExecContext(ctx, `
DELETE FROM object_versions
WHERE tenant = ? AND object_id = ? AND revision = ?`,
			string(tenant), version.object[:], uint64(version.revision))
		if err != nil {
			return 0, false, fmt.Errorf("catalog: delete tombstoned object version: %w", err)
		}
		if err := requireOneRow(result, "tombstoned object version"); err != nil {
			return 0, false, err
		}
	}
	return len(versions), more, nil
}

const tombstoneReachability = `
  AND NOT EXISTS (
      SELECT 1
      FROM handles handle
      JOIN retention_owners handle_owner
        ON handle_owner.owner_id = handle.owner_id
       AND handle_owner.session_owner = handle.session_owner
      WHERE handle.tenant = object.tenant
        AND handle.object_id = object.object_id
        AND handle.closed = 0 AND handle_owner.retired = 0
  )
  AND NOT EXISTS (
      SELECT 1 FROM mutation_journal journal
      WHERE journal.tenant = object.tenant
        AND (journal.primary_object = object.object_id
          OR journal.secondary_object = object.object_id)
  )
  AND NOT EXISTS (
      SELECT 1 FROM changes change
      WHERE change.tenant = object.tenant AND change.object_id = object.object_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM file_provider_materialized_containers materialized
      WHERE materialized.tenant = object.tenant AND materialized.container_id = object.object_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM source_object_ids source_id
      WHERE source_id.object_id = object.object_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM source_snapshot_bindings snapshot
      WHERE snapshot.object_id = object.object_id
  )`

const tombstoneVersionReachability = `
  AND NOT EXISTS (
      SELECT 1
      FROM handles handle
      JOIN retention_owners handle_owner
        ON handle_owner.owner_id = handle.owner_id
       AND handle_owner.session_owner = handle.session_owner
      WHERE handle.tenant = object.tenant
        AND handle.object_id = object.object_id
        AND handle.object_revision = version.revision
        AND handle.closed = 0 AND handle_owner.retired = 0
  )`

const tombstoneVersionCandidates = `
SELECT version.object_id, version.revision, version.kind, version.hash
FROM objects object INDEXED BY objects_tombstone_gc
CROSS JOIN object_versions version
  ON version.tenant = object.tenant AND version.object_id = object.object_id
WHERE object.tenant = ? AND object.tombstone = 1 AND object.revision <= ?` +
	tombstoneVersionReachability + `
ORDER BY version.object_id, version.revision
LIMIT ?`

func collectTombstoneObjects(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	floor Revision,
	limit int,
) (int, bool, error) {
	query := `
SELECT object.object_id, object.kind, object.hash
FROM objects object INDEXED BY objects_tombstone_gc
WHERE object.tenant = ? AND object.tombstone = 1 AND object.revision <= ?
  AND NOT EXISTS (
      SELECT 1 FROM object_versions version
      WHERE version.tenant = object.tenant AND version.object_id = object.object_id
  )` + tombstoneReachability + `
ORDER BY object.revision, object.object_id
LIMIT ?`
	rows, err := tx.QueryContext(ctx, query, string(tenant), uint64(floor), limit+1)
	if err != nil {
		return 0, false, fmt.Errorf("catalog: select tombstoned objects: %w", err)
	}
	type tombstoneObject struct {
		id   ObjectID
		kind Kind
		hash ContentHash
	}
	var objects []tombstoneObject
	for rows.Next() {
		var rawID, rawHash []byte
		var kind uint8
		if err := rows.Scan(&rawID, &kind, &rawHash); err != nil {
			_ = rows.Close()
			return 0, false, fmt.Errorf("catalog: scan tombstoned object: %w", err)
		}
		if len(rawID) != len(ObjectID{}) || len(rawHash) != len(ContentHash{}) {
			_ = rows.Close()
			return 0, false, fmt.Errorf("%w: tombstoned object identity", ErrIntegrity)
		}
		var object tombstoneObject
		copy(object.id[:], rawID)
		object.kind = Kind(kind)
		copy(object.hash[:], rawHash)
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, false, fmt.Errorf("catalog: read tombstoned objects: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, false, fmt.Errorf("catalog: close tombstoned objects: %w", err)
	}
	more := len(objects) > limit
	if more {
		objects = objects[:limit]
	}
	for _, object := range objects {
		if object.kind == KindFile {
			if _, err := tx.ExecContext(ctx,
				"INSERT OR IGNORE INTO blob_gc_candidates(hash) VALUES (?)",
				object.hash[:]); err != nil {
				return 0, false, fmt.Errorf("catalog: queue tombstoned object blob: %w", err)
			}
		}
		result, err := tx.ExecContext(ctx, `
DELETE FROM objects
WHERE tenant = ? AND object_id = ? AND tombstone = 1 AND revision <= ?`,
			string(tenant), object.id[:], uint64(floor))
		if err != nil {
			return 0, false, fmt.Errorf("catalog: delete tombstoned object: %w", err)
		}
		if err := requireOneRow(result, "tombstoned object"); err != nil {
			return 0, false, err
		}
	}
	return len(objects), more, nil
}

func collectRetiredOwners(
	ctx context.Context,
	tx *sql.Tx,
	limit int,
) (int, bool, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT owner.owner_id, owner.session_owner
FROM retention_owners owner
WHERE owner.retired = 1
  AND NOT EXISTS (
      SELECT 1 FROM handles handle
      WHERE handle.owner_id = owner.owner_id
        AND handle.session_owner = owner.session_owner
  )
  AND NOT EXISTS (
      SELECT 1 FROM mutation_pins pin
      WHERE pin.owner_id = owner.owner_id
        AND pin.session_owner = owner.session_owner
  )
ORDER BY owner.owner_id, owner.session_owner
LIMIT ?`, limit+1)
	if err != nil {
		return 0, false, fmt.Errorf("catalog: select empty retired owners: %w", err)
	}
	type ownerID struct {
		generation HandleOwnerID
		session    string
	}
	var owners []ownerID
	for rows.Next() {
		var raw []byte
		var session string
		if err := rows.Scan(&raw, &session); err != nil {
			_ = rows.Close()
			return 0, false, fmt.Errorf("catalog: scan empty retired owner: %w", err)
		}
		if len(raw) != len(HandleOwnerID{}) {
			_ = rows.Close()
			return 0, false, fmt.Errorf("%w: retired owner generation length", ErrIntegrity)
		}
		var generation HandleOwnerID
		copy(generation[:], raw)
		owners = append(owners, ownerID{generation: generation, session: session})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, false, fmt.Errorf("catalog: read empty retired owners: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, false, fmt.Errorf("catalog: close empty retired owners: %w", err)
	}
	more := len(owners) > limit
	if more {
		owners = owners[:limit]
	}
	for _, owner := range owners {
		result, err := tx.ExecContext(ctx, `
DELETE FROM retention_owners
WHERE owner_id = ? AND session_owner = ? AND retired = 1`,
			owner.generation[:], owner.session)
		if err != nil {
			return 0, false, fmt.Errorf("catalog: delete empty retired owner: %w", err)
		}
		if err := requireOneRow(result, "empty retired owner"); err != nil {
			return 0, false, err
		}
	}
	return len(owners), more, nil
}

func retainedIDs(rows *sql.Rows) ([][16]byte, error) {
	var ids [][16]byte
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if len(raw) != len([16]byte{}) {
			_ = rows.Close()
			return nil, fmt.Errorf("%w: retained identity length", ErrIntegrity)
		}
		var id [16]byte
		copy(id[:], raw)
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return ids, nil
}

func requireOneRow(result sql.Result, label string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect deleted %s: %w", label, err)
	}
	if rows != 1 {
		return fmt.Errorf("%w: %s changed during collection", ErrIntegrity, label)
	}
	return nil
}
