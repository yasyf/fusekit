package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/yasyf/fusekit/causal"
)

const sourceSnapshotObjectInsertBatch = 1024

func (c *Catalog) appendSourceSnapshotPageRows(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceSnapshotIdentity,
	page SourceSnapshotPublicationPage,
) (int64, error) {
	if err := appendSourceSnapshotAffected(ctx, tx, identity, page.AffectedKeys); err != nil {
		return 0, err
	}
	rootBindings := make([]SourceSnapshotBinding, len(page.Roots))
	for index, root := range page.Roots {
		rootBindings[index] = SourceSnapshotBinding{LogicalID: root.LogicalID, SourceKey: root.RootKey}
	}
	if err := appendSourceSnapshotBindings(ctx, tx, identity, rootBindings); err != nil {
		return 0, err
	}
	if err := appendSourceSnapshotRoots(ctx, tx, identity, page.Roots); err != nil {
		return 0, err
	}
	if err := appendSourceSnapshotBindings(ctx, tx, identity, page.Bindings); err != nil {
		return 0, err
	}
	if err := appendSourceSnapshotInputs(ctx, tx, identity, page.Bindings); err != nil {
		return 0, err
	}
	return c.appendSourceSnapshotObjects(ctx, tx, identity, page.Objects)
}

func appendSourceSnapshotAffected(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceSnapshotIdentity,
	keys []causal.LogicalKey,
) error {
	if len(keys) == 0 {
		return nil
	}
	args := make([]any, 0, 2+len(keys))
	for _, key := range keys {
		args = append(args, string(key))
	}
	args = append(args, string(identity.Authority), identity.Snapshot)
	query := `
WITH incoming(affected_key) AS (VALUES ` + snapshotValues(len(keys), 1) + `)
INSERT INTO source_snapshot_affected(source_authority, snapshot_id, affected_key)
SELECT ?, ?, affected_key FROM incoming`
	result, err := tx.ExecContext(ctx, query, args...)
	return exactSnapshotRows(result, err, len(keys))
}

func appendSourceSnapshotBindings(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceSnapshotIdentity,
	bindings []SourceSnapshotBinding,
) error {
	if len(bindings) == 0 {
		return nil
	}
	args := make([]any, 0, len(bindings)*4+3)
	for _, binding := range bindings {
		objectID, err := sourceSnapshotBindingIdentity(
			ctx, tx, identity.Authority, identity.Snapshot, binding.LogicalID, binding.SourceKey,
		)
		if err != nil {
			return err
		}
		args = append(args, binding.LogicalID, string(binding.SourceKey), objectID[:], binding.Fingerprint[:])
	}
	args = append(args, string(identity.Authority), identity.Snapshot, string(identity.Authority))
	query := `
WITH incoming(logical_id, source_key, object_id, effective_fingerprint) AS (VALUES ` + snapshotValues(len(bindings), 4) + `)
INSERT INTO source_snapshot_bindings(
    source_authority, snapshot_id, logical_id, source_key, object_id, effective_fingerprint
)
SELECT ?, ?, incoming.logical_id, incoming.source_key,
       incoming.object_id, incoming.effective_fingerprint
FROM incoming
LEFT JOIN source_authority_bindings durable
  ON durable.source_authority = ? AND durable.logical_id = incoming.logical_id
WHERE durable.source_key IS NULL OR durable.source_key = incoming.source_key
ON CONFLICT(source_authority, snapshot_id, logical_id) DO UPDATE SET
    effective_fingerprint = excluded.effective_fingerprint
WHERE source_snapshot_bindings.source_key = excluded.source_key
  AND source_snapshot_bindings.object_id = excluded.object_id`
	result, err := tx.ExecContext(ctx, query, args...)
	if exactErr := exactSnapshotRows(result, err, len(bindings)); exactErr != nil {
		return fmt.Errorf("%w: source snapshot binding conflict: %v", ErrSourceObserverConflict, exactErr)
	}
	return nil
}

func sourceSnapshotBindingIdentity(
	ctx context.Context,
	tx *sql.Tx,
	authority causal.SourceAuthorityID,
	snapshot string,
	logical string,
	key SourceObjectKey,
) (ObjectID, error) {
	var stagedKey string
	var raw []byte
	err := tx.QueryRowContext(ctx, `
SELECT source_key, object_id
FROM source_snapshot_bindings
WHERE source_authority = ? AND snapshot_id = ? AND logical_id = ?`,
		string(authority), snapshot, logical).Scan(&stagedKey, &raw)
	if err == nil {
		if SourceObjectKey(stagedKey) != key {
			return ObjectID{}, ErrSourceObserverConflict
		}
		return objectID(raw)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ObjectID{}, err
	}
	var durableKey string
	err = tx.QueryRowContext(ctx, `
SELECT source_key
FROM source_authority_bindings
WHERE source_authority = ? AND logical_id = ?`,
		string(authority), logical).Scan(&durableKey)
	if err == nil && SourceObjectKey(durableKey) != key {
		return ObjectID{}, ErrSourceObserverConflict
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ObjectID{}, err
	}
	err = tx.QueryRowContext(ctx, `
SELECT object_id
FROM source_object_ids
WHERE source_authority = ? AND source_key = ?`,
		string(authority), string(key)).Scan(&raw)
	if err == nil {
		return objectID(raw)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ObjectID{}, err
	}
	id, err := NewObjectID()
	if err != nil {
		return ObjectID{}, err
	}
	return id, nil
}

func appendSourceSnapshotRoots(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceSnapshotIdentity,
	roots []SourceSnapshotRoot,
) error {
	if len(roots) == 0 {
		return nil
	}
	args := make([]any, 0, len(roots)*5+2)
	for _, root := range roots {
		operation := sourceCatalogOperation(identity.Change.OperationID, root.Tenant)
		args = append(args, string(root.Tenant), uint64(root.Generation), root.LogicalID, string(root.RootKey), operation[:])
	}
	args = append(args, string(identity.Authority), identity.Snapshot)
	query := `
WITH incoming(tenant, generation, logical_id, root_key, catalog_operation_id) AS (VALUES ` + snapshotValues(len(roots), 5) + `)
INSERT INTO source_snapshot_roots(
    source_authority, snapshot_id, tenant, generation, logical_id, root_key, catalog_operation_id
)
SELECT ?, ?, tenant, generation, logical_id, root_key, catalog_operation_id FROM incoming`
	result, err := tx.ExecContext(ctx, query, args...)
	return exactSnapshotRows(result, err, len(roots))
}

func appendSourceSnapshotInputs(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceSnapshotIdentity,
	bindings []SourceSnapshotBinding,
) error {
	count := 0
	for _, binding := range bindings {
		count += len(binding.Inputs)
	}
	if count == 0 {
		return nil
	}
	args := make([]any, 0, count*3+4)
	for _, binding := range bindings {
		for _, input := range binding.Inputs {
			args = append(args, binding.LogicalID, input.RootID, input.Relative)
		}
	}
	args = append(args, string(identity.Authority), identity.Snapshot, string(identity.Authority), identity.Snapshot)
	query := `
WITH incoming(logical_id, root_id, relative_path) AS (VALUES ` + snapshotValues(count, 3) + `)
INSERT INTO source_snapshot_logical(source_authority, snapshot_id, logical_id, root_id, relative_path)
SELECT ?, ?, incoming.logical_id, incoming.root_id, incoming.relative_path
FROM incoming
JOIN source_snapshot_stages physical
  ON physical.source_authority = ? AND physical.snapshot_id = ?
 AND physical.root_id = incoming.root_id AND physical.relative_path = incoming.relative_path`
	result, err := tx.ExecContext(ctx, query, args...)
	if exactErr := exactSnapshotRows(result, err, count); exactErr != nil {
		return fmt.Errorf("%w: snapshot input is outside its physical fence: %v", ErrInvalidObject, exactErr)
	}
	return nil
}

type sourceSnapshotObjectRow struct {
	projection SourceSnapshotProjection
	nameKey    string
	stage      any
	hash       []byte
	size       int64
}

func (c *Catalog) appendSourceSnapshotObjects(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceSnapshotIdentity,
	projections []SourceSnapshotProjection,
) (int64, error) {
	if len(projections) == 0 {
		return 0, nil
	}
	policies, err := sourceSnapshotTenantPolicies(ctx, tx, projections)
	if err != nil {
		return 0, err
	}
	rows := make([]sourceSnapshotObjectRow, len(projections))
	claimed := make(map[ContentRef]struct{})
	var contentBytes int64
	for index, projection := range projections {
		object := projection.Object
		if err := validateSourceObject(object); err != nil {
			return 0, err
		}
		if object.Key == "" || object.Key == object.Parent {
			return 0, ErrInvalidObject
		}
		objectSize, contentHash := catalogContent(object.Kind, object.Content, object.LinkTarget)
		row := sourceSnapshotObjectRow{
			projection: projection,
			nameKey:    normalizeName(policies[projection.Tenant], object.Name),
			hash:       contentHash[:],
			size:       objectSize,
		}
		if object.Kind == KindFile {
			claimed[object.Content] = struct{}{}
			row.stage = object.Content.Stage[:]
			row.size = object.Content.Size
			contentBytes += object.Content.Size
			if contentBytes < 0 || contentBytes > SourceSnapshotContentByteLimit {
				return 0, fmt.Errorf("%w: source snapshot page content exceeds quota", ErrInvalidObject)
			}
		}
		rows[index] = row
	}
	if err := c.claimSourceSnapshotContent(ctx, tx, identity.Change.OperationID, claimed); err != nil {
		return 0, err
	}
	for start := 0; start < len(rows); start += sourceSnapshotObjectInsertBatch {
		end := min(start+sourceSnapshotObjectInsertBatch, len(rows))
		if err := appendSourceSnapshotObjectBatch(ctx, tx, identity, rows[start:end]); err != nil {
			return 0, err
		}
	}
	return contentBytes, nil
}

func sourceSnapshotTenantPolicies(
	ctx context.Context,
	tx *sql.Tx,
	projections []SourceSnapshotProjection,
) (policies map[TenantID]CasePolicy, err error) {
	tenants := make([]TenantID, 0)
	seen := make(map[TenantID]struct{})
	for _, projection := range projections {
		if _, found := seen[projection.Tenant]; found {
			continue
		}
		seen[projection.Tenant] = struct{}{}
		tenants = append(tenants, projection.Tenant)
	}
	args := make([]any, len(tenants))
	for index, tenant := range tenants {
		args[index] = string(tenant)
	}
	rows, err := tx.QueryContext(ctx, `SELECT tenant, case_policy FROM tenants WHERE tenant IN (`+
		strings.TrimSuffix(strings.Repeat("?,", len(tenants)), ",")+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	policies = make(map[TenantID]CasePolicy, len(tenants))
	for rows.Next() {
		var tenant string
		var policy uint8
		if err := rows.Scan(&tenant, &policy); err != nil {
			return nil, err
		}
		policies[TenantID(tenant)] = CasePolicy(policy)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(policies) != len(tenants) {
		return nil, ErrNotFound
	}
	return policies, nil
}

func (c *Catalog) claimSourceSnapshotContent(
	ctx context.Context,
	tx *sql.Tx,
	operation causal.OperationID,
	refs map[ContentRef]struct{},
) error {
	if len(refs) == 0 {
		return nil
	}
	args := make([]any, 0, len(refs)*3+2)
	for ref := range refs {
		args = append(args, ref.Stage[:], ref.Hash[:], ref.Size)
	}
	args = append(args, operation[:], c.owner[:])
	query := `
WITH incoming(stage_id, hash, size) AS (VALUES ` + snapshotValues(len(refs), 3) + `)
UPDATE content_stages SET source_operation_id = ?
WHERE owner_id = ? AND mutation_id IS NULL AND source_operation_id IS NULL AND published = 1
  AND EXISTS (
    SELECT 1 FROM incoming
    WHERE incoming.stage_id = content_stages.stage_id
      AND incoming.hash = content_stages.hash AND incoming.size = content_stages.size
  )`
	result, err := tx.ExecContext(ctx, query, args...)
	if exactErr := exactSnapshotRows(result, err, len(refs)); exactErr != nil {
		return fmt.Errorf("%w: source content stage ownership changed: %v", ErrInvalidTransition, exactErr)
	}
	return nil
}

func appendSourceSnapshotObjectBatch(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceSnapshotIdentity,
	rows []sourceSnapshotObjectRow,
) error {
	args := make([]any, 0, len(rows)*16+6)
	for _, row := range rows {
		projection := row.projection
		object := projection.Object
		args = append(args,
			string(projection.Tenant), uint64(projection.Generation), projection.LogicalID,
			string(object.Key), string(object.Parent), object.Name, row.nameKey, uint8(object.Kind), object.Mode,
			uint64(object.ContentRevision), row.stage, row.hash, row.size, object.LinkTarget,
			boolInt(object.Visibility.Mount), boolInt(object.Visibility.FileProvider),
		)
	}
	authority := string(identity.Authority)
	args = append(args, authority, identity.Snapshot, authority, identity.Snapshot, authority, identity.Snapshot)
	query := `
WITH incoming(
    tenant, generation, logical_id, source_key, parent_key, object_name, name_key,
    object_kind, object_mode, content_revision, content_stage, content_hash, content_size,
    link_target, mount_visible, file_provider_visible
) AS (VALUES ` + snapshotValues(len(rows), 16) + `)
INSERT INTO source_snapshot_objects(
    source_authority, snapshot_id, tenant, generation, logical_id, source_key, parent_key,
    object_name, name_key, object_kind, object_mode, content_revision, content_stage,
    content_hash, content_size, link_target, mount_visible, file_provider_visible
)
SELECT ?, ?, incoming.tenant, incoming.generation, incoming.logical_id, incoming.source_key,
       incoming.parent_key, incoming.object_name, incoming.name_key, incoming.object_kind,
       incoming.object_mode, incoming.content_revision, incoming.content_stage, incoming.content_hash,
       incoming.content_size, incoming.link_target, incoming.mount_visible, incoming.file_provider_visible
FROM incoming
JOIN source_snapshot_bindings binding
  ON binding.source_authority = ? AND binding.snapshot_id = ?
 AND binding.logical_id = incoming.logical_id AND binding.source_key = incoming.source_key
JOIN source_snapshot_roots root
  ON root.source_authority = ? AND root.snapshot_id = ?
 AND root.tenant = incoming.tenant AND root.generation = incoming.generation`
	result, err := tx.ExecContext(ctx, query, args...)
	if exactErr := exactSnapshotRows(result, err, len(rows)); exactErr != nil {
		return fmt.Errorf("%w: source snapshot projection escaped its binding or fleet: %v",
			ErrSourceObserverConflict, exactErr)
	}
	return nil
}

func exactSnapshotRows(result sql.Result, err error, want int) error {
	if err != nil {
		return mapConstraint(err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != int64(want) {
		return fmt.Errorf("changed %d rows, want %d", changed, want)
	}
	return nil
}

func snapshotValues(rows, columns int) string {
	row := "(" + strings.TrimSuffix(strings.Repeat("?,", columns), ",") + ")"
	return strings.TrimSuffix(strings.Repeat(row+",", rows), ",")
}
