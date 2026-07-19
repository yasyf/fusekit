package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const objectColumns = `tenant, object_id, parent_id, revision, metadata_revision,
content_revision, name, kind, mode, size, hash, desired_revision,
observed_revision, verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone`

const versionColumns = `v.tenant, v.object_id, v.parent_id, v.revision, v.metadata_revision,
v.content_revision, v.name, v.kind, v.mode, v.size, v.hash, v.desired_revision,
v.observed_revision, v.verified_revision, v.applied_revision, v.mount_visible, v.file_provider_visible, v.tombstone`

const snapshotAfterAnchor = "snapshot.after_anchor"

type rowScanner interface {
	Scan(...any) error
}

// Head returns the tenant's current catalog revision.
func (c *Catalog) Head(ctx context.Context, tenant TenantID) (Revision, error) {
	head, _, err := revisionState(ctx, c.readDB, tenant)
	return head, err
}

// CompactionFloor returns the tenant's oldest valid revision anchor.
func (c *Catalog) CompactionFloor(ctx context.Context, tenant TenantID) (Revision, error) {
	_, floor, err := revisionState(ctx, c.readDB, tenant)
	return floor, err
}

// Root returns the tenant's stable root object.
func (c *Catalog) Root(ctx context.Context, tenant TenantID) (Object, error) {
	var raw []byte
	if err := c.readDB.QueryRowContext(ctx,
		"SELECT root_id FROM tenants WHERE tenant = ?", string(tenant)).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Object{}, ErrNotFound
		}
		return Object{}, fmt.Errorf("catalog: lookup tenant root: %w", err)
	}
	id, err := objectID(raw)
	if err != nil {
		return Object{}, err
	}
	return c.lookupAnyObject(ctx, tenant, id)
}

// Tenant returns immutable tenant identity and name-equivalence metadata.
func (c *Catalog) Tenant(ctx context.Context, tenant TenantID) (TenantMetadata, error) {
	var raw []byte
	var policy, presentations uint8
	if err := c.readDB.QueryRowContext(ctx, `
SELECT root_id, case_policy, presentation_set FROM tenants WHERE tenant = ?`, string(tenant)).Scan(&raw, &policy, &presentations); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return TenantMetadata{}, ErrNotFound
		}
		return TenantMetadata{}, fmt.Errorf("catalog: read tenant metadata: %w", err)
	}
	root, err := objectID(raw)
	if err != nil {
		return TenantMetadata{}, err
	}
	set := PresentationSet(presentations)
	if !set.valid() {
		return TenantMetadata{}, fmt.Errorf("%w: corrupt tenant presentation set %d", ErrIntegrity, set)
	}
	return TenantMetadata{Tenant: tenant, Root: root, CasePolicy: CasePolicy(policy), Presentations: set}, nil
}

// Lookup returns an object's current live revision in one presentation.
func (c *Catalog) Lookup(ctx context.Context, tenant TenantID, presentation Presentation, id ObjectID) (Object, error) {
	column, err := visibilityColumn(presentation)
	if err != nil {
		return Object{}, err
	}
	query := "SELECT " + objectColumns +
		" FROM objects WHERE tenant = ? AND object_id = ? AND tombstone = 0 AND " + column + " = 1"
	obj, err := scanObject(c.readDB.QueryRowContext(ctx, query, string(tenant), id[:]))
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: read object head: %w", err)
	}
	return obj, nil
}

func (c *Catalog) lookupAnyObject(ctx context.Context, tenant TenantID, id ObjectID) (Object, error) {
	query := "SELECT " + objectColumns + " FROM objects WHERE tenant = ? AND object_id = ? AND tombstone = 0"
	obj, err := scanObject(c.readDB.QueryRowContext(ctx, query, string(tenant), id[:]))
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: read object head: %w", err)
	}
	return obj, nil
}

// Inspect returns a live object without applying presentation visibility.
func (c *Catalog) Inspect(ctx context.Context, tenant TenantID, id ObjectID) (Object, error) {
	return c.lookupAnyObject(ctx, tenant, id)
}

// LookupName returns the live object bound in one presentation to parent and name.
func (c *Catalog) LookupName(ctx context.Context, tenant TenantID, presentation Presentation, parent ObjectID, name string) (Object, error) {
	column, err := visibilityColumn(presentation)
	if err != nil {
		return Object{}, err
	}
	policy, err := tenantCasePolicy(ctx, c.readDB, tenant)
	if err != nil {
		return Object{}, err
	}
	key := normalizeName(policy, name)
	query := "SELECT " + objectColumns +
		" FROM objects WHERE tenant = ? AND parent_id = ? AND name_key = ? AND tombstone = 0 AND " + column + " = 1"
	obj, err := scanObject(c.readDB.QueryRowContext(ctx, query, string(tenant), parent[:], key))
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: lookup object: %w", err)
	}
	return obj, nil
}

// Snapshot returns a stable scoped metadata-only page at revision.
func (c *Catalog) Snapshot(ctx context.Context, tenant TenantID, scope EnumerationScope, revision Revision, cursor SnapshotCursor, limit int) (SnapshotPage, error) {
	if limit <= 0 {
		return SnapshotPage{}, fmt.Errorf("%w: snapshot limit must be positive", ErrInvalidObject)
	}
	if err := validateEnumerationScope(scope); err != nil {
		return SnapshotPage{}, err
	}
	tx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return SnapshotPage{}, fmt.Errorf("catalog: begin snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	head, floor, err := revisionState(ctx, tx, tenant)
	if err != nil {
		return SnapshotPage{}, err
	}
	if revision == 0 {
		revision = head
	}
	if err := validateAnchor(revision, head, floor); err != nil {
		return SnapshotPage{}, err
	}
	if err := c.trip(snapshotAfterAnchor); err != nil {
		return SnapshotPage{}, err
	}

	var query string
	var args []any
	switch scope.Kind {
	case EnumerationContainer:
		column, err := visibilityColumn(scope.Presentation)
		if err != nil {
			return SnapshotPage{}, err
		}
		query = "SELECT " + versionColumns + `
FROM object_versions v
WHERE v.tenant = ? AND v.parent_id = ? AND v.object_id <> ?
  AND v.revision = (
	      SELECT MAX(v2.revision) FROM object_versions v2
	      WHERE v2.tenant = v.tenant AND v2.object_id = v.object_id AND v2.revision <= ?
	  )`
		query += "\n  AND v.tombstone = 0 AND v." + column + " = 1"
		args = []any{string(tenant), scope.Parent[:], scope.Parent[:], uint64(revision)}
	case EnumerationWorkingSet:
		query = "SELECT " + versionColumns + `
FROM (
    SELECT object_id FROM materialization_interests
    WHERE tenant = ? AND owner_presentation = ? AND owner_domain = ? AND owner_generation = ?
      AND created_revision <= ?
      AND (removed_revision IS NULL OR removed_revision > ?)
    GROUP BY object_id
) interested
JOIN object_versions v ON v.tenant = ? AND v.object_id = interested.object_id
WHERE v.revision = (
      SELECT MAX(v2.revision) FROM object_versions v2
      WHERE v2.tenant = v.tenant AND v2.object_id = v.object_id AND v2.revision <= ?
  )
  AND v.tombstone = 0
  AND v.file_provider_visible = 1`
		args = []any{string(tenant), uint8(scope.Presentation), string(scope.Domain), uint64(scope.Generation), uint64(revision), uint64(revision), string(tenant), uint64(revision)}
	}
	if cursor.After != nil {
		query += " AND v.object_id > ?"
		args = append(args, cursor.After[:])
	}
	query += " ORDER BY v.object_id LIMIT ?"
	args = append(args, limit+1)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return SnapshotPage{}, fmt.Errorf("catalog: query snapshot: %w", err)
	}
	defer func() { _ = rows.Close() }()

	objects := make([]Object, 0, limit+1)
	for rows.Next() {
		obj, err := scanObject(rows)
		if err != nil {
			return SnapshotPage{}, fmt.Errorf("catalog: scan snapshot: %w", err)
		}
		objects = append(objects, obj)
	}
	if err := rows.Err(); err != nil {
		return SnapshotPage{}, fmt.Errorf("catalog: read snapshot: %w", err)
	}
	page := SnapshotPage{Revision: revision, Objects: objects}
	if len(objects) > limit {
		page.Objects = objects[:limit]
		after := page.Objects[len(page.Objects)-1].ID
		page.Next = &SnapshotCursor{After: &after}
	}
	if err := tx.Commit(); err != nil {
		return SnapshotPage{}, fmt.Errorf("catalog: finish snapshot: %w", err)
	}
	return page, nil
}

// ChangesSince returns scoped whole mutation revisions strictly after anchor.
func (c *Catalog) ChangesSince(ctx context.Context, tenant TenantID, scope EnumerationScope, cursor ChangeCursor, limit int) (ChangePage, error) {
	if limit <= 0 {
		return ChangePage{}, fmt.Errorf("%w: change limit must be positive", ErrInvalidObject)
	}
	scopeKind, presentation, scopeParent, scopeDomain, scopeGeneration, err := enumerationScopeKey(scope)
	if err != nil {
		return ChangePage{}, err
	}
	tx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ChangePage{}, fmt.Errorf("catalog: begin changes: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	head, floor, err := revisionState(ctx, tx, tenant)
	if err != nil {
		return ChangePage{}, err
	}
	if err := validateAnchor(cursor.Revision, head, floor); err != nil {
		return ChangePage{}, err
	}

	changes, err := readChanges(ctx, tx, tenant, scopeKind, presentation, scopeParent, scopeDomain, scopeGeneration, cursor, limit+1)
	if err != nil {
		return ChangePage{}, err
	}
	complete := len(changes) <= limit
	next := ChangeCursor{Revision: head, Sequence: CompleteChangeSequence}
	if !complete {
		changes = changes[:limit]
		last := changes[len(changes)-1]
		next = ChangeCursor{Revision: last.Revision, Sequence: last.Sequence}
	}
	if err := tx.Commit(); err != nil {
		return ChangePage{}, fmt.Errorf("catalog: finish changes: %w", err)
	}
	return ChangePage{Floor: floor, Head: head, Next: next, Complete: complete, Changes: changes}, nil
}

func readChanges(ctx context.Context, tx *sql.Tx, tenant TenantID, scopeKind, presentation uint8, scopeParent []byte, scopeDomain string, scopeGeneration uint64, cursor ChangeCursor, limit int) ([]Change, error) {
	query := "SELECT c.revision, c.sequence, c.kind, " + versionColumns + `
FROM changes c
JOIN object_versions v
  ON v.tenant = c.tenant AND v.object_id = c.object_id AND v.revision = c.object_revision
WHERE c.tenant = ? AND c.scope_kind = ? AND c.presentation = ? AND c.scope_parent = ?
  AND c.scope_domain = ? AND c.scope_generation = ?
  AND (c.revision > ? OR (c.revision = ? AND c.sequence > ?))
ORDER BY c.revision, c.sequence LIMIT ?`
	rows, err := tx.QueryContext(ctx, query, string(tenant), scopeKind, presentation, scopeParent, scopeDomain, scopeGeneration,
		uint64(cursor.Revision), uint64(cursor.Revision), cursor.Sequence, limit)
	if err != nil {
		return nil, fmt.Errorf("catalog: query changes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var changes []Change
	for rows.Next() {
		var revision uint64
		var sequence uint32
		var kind uint8
		var obj Object
		obj, err = scanObjectWithPrefix(rows, &revision, &sequence, &kind)
		if err != nil {
			return nil, fmt.Errorf("catalog: scan change: %w", err)
		}
		changes = append(changes, Change{
			Revision: Revision(revision), Sequence: sequence,
			Kind: ChangeKind(kind), Object: obj,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: read changes: %w", err)
	}
	return changes, nil
}

func validateEnumerationScope(scope EnumerationScope) error {
	if _, err := visibilityColumn(scope.Presentation); err != nil {
		return err
	}
	switch scope.Kind {
	case EnumerationWorkingSet:
		if scope.Presentation != PresentationFileProvider {
			return fmt.Errorf("%w: working-set scope requires File Provider presentation", ErrInvalidObject)
		}
		if !zeroObjectID(scope.Parent) {
			return fmt.Errorf("%w: working-set scope carries a parent", ErrInvalidObject)
		}
		if scope.Domain == "" || scope.Generation == 0 {
			return fmt.Errorf("%w: working-set scope requires an exact domain generation", ErrInvalidObject)
		}
	case EnumerationContainer:
		if zeroObjectID(scope.Parent) {
			return fmt.Errorf("%w: container scope has no parent", ErrInvalidObject)
		}
		if scope.Domain != "" || scope.Generation != 0 {
			return fmt.Errorf("%w: container scope carries a domain", ErrInvalidObject)
		}
	default:
		return fmt.Errorf("%w: unknown enumeration scope %d", ErrInvalidObject, scope.Kind)
	}
	return nil
}

func enumerationScopeKey(scope EnumerationScope) (uint8, uint8, []byte, string, uint64, error) {
	if err := validateEnumerationScope(scope); err != nil {
		return 0, 0, nil, "", 0, err
	}
	return uint8(scope.Kind), uint8(scope.Presentation), scope.Parent[:], string(scope.Domain), uint64(scope.Generation), nil
}

func visibilityColumn(presentation Presentation) (string, error) {
	switch presentation {
	case PresentationMount:
		return "mount_visible", nil
	case PresentationFileProvider:
		return "file_provider_visible", nil
	default:
		return "", fmt.Errorf("%w: unknown presentation %d", ErrInvalidObject, presentation)
	}
}

func revisionState(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenant TenantID) (Revision, Revision, error) {
	var head, floor uint64
	if err := q.QueryRowContext(ctx,
		"SELECT head, floor FROM tenants WHERE tenant = ?", string(tenant)).Scan(&head, &floor); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, ErrNotFound
		}
		return 0, 0, fmt.Errorf("catalog: read revision state: %w", err)
	}
	return Revision(head), Revision(floor), nil
}

func tenantCasePolicy(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenant TenantID) (CasePolicy, error) {
	var policy uint8
	if err := q.QueryRowContext(ctx,
		"SELECT case_policy FROM tenants WHERE tenant = ?", string(tenant)).Scan(&policy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("catalog: read tenant case policy: %w", err)
	}
	return CasePolicy(policy), nil
}

func normalizeName(policy CasePolicy, name string) string {
	normalized := norm.NFC.String(name)
	if policy == CaseInsensitive {
		return cases.Fold().String(normalized)
	}
	return normalized
}

func validateAnchor(anchor, head, floor Revision) error {
	if anchor < floor {
		return &StaleAnchorError{Anchor: anchor, Floor: floor}
	}
	if anchor > head {
		return fmt.Errorf("%w: anchor %d exceeds head %d", ErrInvalidTransition, anchor, head)
	}
	return nil
}

func scanObject(s rowScanner) (Object, error) {
	return scanObjectWithPrefix(s)
}

func scanObjectWithPrefix(s rowScanner, prefix ...any) (Object, error) {
	var obj Object
	var tenant string
	var id, parent, hash []byte
	var revision, metadata, content uint64
	var desired, observed, verified, applied uint64
	var kind uint8
	var mode uint32
	var mountVisible, fileProviderVisible, tombstone bool
	dst := append(prefix,
		&tenant, &id, &parent, &revision, &metadata, &content,
		&obj.Name, &kind, &mode, &obj.Size, &hash,
		&desired, &observed, &verified, &applied, &mountVisible, &fileProviderVisible, &tombstone,
	)
	if err := s.Scan(dst...); err != nil {
		return Object{}, err
	}
	parsedID, err := objectID(id)
	if err != nil {
		return Object{}, err
	}
	parentID, err := objectID(parent)
	if err != nil {
		return Object{}, err
	}
	contentHash, err := contentHash(hash)
	if err != nil {
		return Object{}, err
	}
	obj.Tenant = TenantID(tenant)
	obj.ID = parsedID
	obj.Parent = parentID
	obj.Revision = Revision(revision)
	obj.MetadataRevision = Revision(metadata)
	obj.ContentRevision = Revision(content)
	obj.Kind = Kind(kind)
	obj.Mode = mode
	obj.Hash = contentHash
	obj.Convergence = Convergence{
		Desired: Revision(desired), Observed: Revision(observed),
		Verified: Revision(verified), Applied: Revision(applied),
	}
	obj.Visibility = Visibility{Mount: mountVisible, FileProvider: fileProviderVisible}
	obj.Tombstone = tombstone
	return obj, nil
}

func objectID(raw []byte) (ObjectID, error) {
	var id ObjectID
	if len(raw) != len(id) {
		return id, fmt.Errorf("catalog: corrupt object id length %d", len(raw))
	}
	copy(id[:], raw)
	return id, nil
}

func mutationID(raw []byte) (MutationID, error) {
	var id MutationID
	if len(raw) != len(id) {
		return id, fmt.Errorf("catalog: corrupt mutation id length %d", len(raw))
	}
	copy(id[:], raw)
	return id, nil
}

func contentHash(raw []byte) (ContentHash, error) {
	var hash ContentHash
	if len(raw) != len(hash) {
		return hash, fmt.Errorf("catalog: corrupt content hash length %d", len(raw))
	}
	copy(hash[:], raw)
	return hash, nil
}
