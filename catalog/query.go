package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/causal"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const objectColumns = `tenant, object_id, parent_id, revision, metadata_revision,
content_revision, name, kind, mode, size, hash, link_target, desired_revision,
observed_revision, verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone`

const versionColumns = `v.tenant, v.object_id, v.parent_id, v.revision, v.metadata_revision,
v.content_revision, v.name, v.kind, v.mode, v.size, v.hash, v.link_target, v.desired_revision,
v.observed_revision, v.verified_revision, v.applied_revision, v.mount_visible, v.file_provider_visible, v.tombstone`

const snapshotAfterAnchor = "snapshot.after_anchor"

const publicationVersionLineageCTE = `WITH RECURSIVE publication_lineage(
    publication_id, predecessor_publication_id
) AS (
    SELECT publication_id, predecessor_publication_id
    FROM source_driver_publications
    WHERE source_authority = ? AND publication_id = ?
    UNION
    SELECT predecessor.publication_id, predecessor.predecessor_publication_id
    FROM source_driver_publications predecessor
    JOIN publication_lineage successor
      ON predecessor.publication_id = successor.predecessor_publication_id
    WHERE predecessor.source_authority = ?
), ranked_publication_versions AS (
    SELECT version.*,
           ROW_NUMBER() OVER (
               PARTITION BY version.object_id
               ORDER BY version.revision DESC, publication.source_revision DESC
           ) AS version_rank
    FROM publication_lineage lineage
    JOIN source_driver_publications publication
      ON publication.source_authority = ?
     AND publication.publication_id = lineage.publication_id
    JOIN source_driver_publication_versions version
      ON version.source_authority = publication.source_authority
     AND version.publication_id = publication.publication_id
     AND version.tenant = ?
    WHERE version.revision <= ?
)`

type rowScanner interface {
	Scan(...any) error
}

// Head returns the tenant's current catalog revision.
func (c *Catalog) Head(ctx context.Context, tenant TenantID) (Revision, error) {
	tx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, fmt.Errorf("catalog: begin head lookup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	head, _, err := effectiveRevisionState(ctx, tx, tenant)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("catalog: finish head lookup: %w", err)
	}
	return head, nil
}

// CompactionFloor returns the tenant's oldest valid revision anchor.
func (c *Catalog) CompactionFloor(ctx context.Context, tenant TenantID) (Revision, error) {
	_, floor, err := effectiveRevisionState(ctx, c.readDB, tenant)
	return floor, err
}

// Root returns the tenant's stable root object.
func (c *Catalog) Root(ctx context.Context, tenant TenantID) (Object, error) {
	tx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Object{}, fmt.Errorf("catalog: begin tenant root lookup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var raw []byte
	if err := tx.QueryRowContext(ctx,
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
	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		return Object{}, err
	}
	obj, err := currentObjectFromView(ctx, tx, view, tenant, id, false, "")
	if err != nil {
		return Object{}, err
	}
	if err := tx.Commit(); err != nil {
		return Object{}, fmt.Errorf("catalog: finish tenant root lookup: %w", err)
	}
	return obj, nil
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
	tx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Object{}, fmt.Errorf("catalog: begin object lookup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		return Object{}, err
	}
	obj, err := currentObjectFromView(ctx, tx, view, tenant, id, false, column)
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: read object head: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Object{}, fmt.Errorf("catalog: finish object lookup: %w", err)
	}
	return obj, nil
}

// LookupAt returns the visible object state pinned by a catalog snapshot revision.
func (c *Catalog) LookupAt(
	ctx context.Context,
	tenant TenantID,
	presentation Presentation,
	id ObjectID,
	revision Revision,
) (Object, error) {
	column, err := visibilityColumn(presentation)
	if err != nil {
		return Object{}, err
	}
	tx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Object{}, fmt.Errorf("catalog: begin object snapshot lookup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		return Object{}, err
	}
	if err := validateAnchor(revision, view.head, view.floor); err != nil {
		return Object{}, err
	}
	var obj Object
	if len(view.publication) != 0 && revision == view.head {
		obj, err = currentObjectFromView(ctx, tx, view, tenant, id, false, column)
	} else if len(view.publication) != 0 {
		query := publicationVersionLineageCTE + "\nSELECT " + versionColumns + `
FROM ranked_publication_versions v
WHERE v.version_rank = 1 AND v.object_id = ?
  AND v.tombstone = 0 AND v.` + column + " = 1"
		obj, err = scanObject(tx.QueryRowContext(ctx, query,
			view.authority, view.publication, view.authority, view.authority,
			string(tenant), uint64(revision), id[:],
		))
	} else {
		query := "SELECT " + versionColumns + `
FROM object_versions v
WHERE v.tenant = ? AND v.object_id = ?
  AND v.revision = (
      SELECT MAX(v2.revision) FROM object_versions v2
      WHERE v2.tenant = v.tenant AND v2.object_id = v.object_id AND v2.revision <= ?
  )
  AND v.tombstone = 0 AND v.` + column + " = 1"
		obj, err = scanObject(tx.QueryRowContext(ctx, query, string(tenant), id[:], uint64(revision)))
	}
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: read object snapshot: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Object{}, fmt.Errorf("catalog: finish object snapshot lookup: %w", err)
	}
	return obj, nil
}

func (c *Catalog) lookupAnyObject(ctx context.Context, tenant TenantID, id ObjectID) (Object, error) {
	tx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Object{}, fmt.Errorf("catalog: begin object inspection: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		return Object{}, err
	}
	obj, err := currentObjectFromView(ctx, tx, view, tenant, id, false, "")
	if err != nil {
		return Object{}, err
	}
	if err := tx.Commit(); err != nil {
		return Object{}, fmt.Errorf("catalog: finish object inspection: %w", err)
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
	tx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Object{}, fmt.Errorf("catalog: begin name lookup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	policy, err := tenantCasePolicy(ctx, tx, tenant)
	if err != nil {
		return Object{}, err
	}
	key := normalizeName(policy, name)
	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		return Object{}, err
	}
	obj, err := currentNamedObjectFromView(ctx, tx, view, tenant, parent, key, column)
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: lookup object: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Object{}, fmt.Errorf("catalog: finish name lookup: %w", err)
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

	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		return SnapshotPage{}, err
	}
	if revision == 0 {
		revision = view.head
	}
	if err := validateAnchor(revision, view.head, view.floor); err != nil {
		return SnapshotPage{}, err
	}
	if err := c.trip(snapshotAfterAnchor); err != nil {
		return SnapshotPage{}, err
	}

	var query string
	var args []any
	publication := len(view.publication) != 0
	historicalPublication := publication && revision < view.head
	if historicalPublication {
		query = publicationVersionLineageCTE
		args = []any{view.authority, view.publication, view.authority, view.authority, string(tenant), uint64(revision)}
	}
	switch scope.Kind {
	case EnumerationContainer:
		column, err := visibilityColumn(scope.Presentation)
		if err != nil {
			return SnapshotPage{}, err
		}
		switch {
		case publication && !historicalPublication:
			query = "SELECT " + versionColumns + `
FROM source_driver_publication_objects v
WHERE v.source_authority = ? AND v.publication_id = ? AND v.tenant = ?
  AND v.parent_id = ? AND v.object_id <> ? AND v.revision <= ?`
			args = []any{view.authority, view.publication, string(tenant), scope.Parent[:], scope.Parent[:], uint64(view.head)}
		case historicalPublication:
			query += "\nSELECT " + versionColumns + `
FROM ranked_publication_versions v
WHERE v.version_rank = 1 AND v.parent_id = ? AND v.object_id <> ?`
			args = append(args, scope.Parent[:], scope.Parent[:])
		default:
			query = "SELECT " + versionColumns + `
FROM object_versions v
WHERE v.tenant = ? AND v.parent_id = ? AND v.object_id <> ?
  AND v.revision = (
	      SELECT MAX(v2.revision) FROM object_versions v2
	      WHERE v2.tenant = v.tenant AND v2.object_id = v.object_id AND v2.revision <= ?
	  )`
			args = []any{string(tenant), scope.Parent[:], scope.Parent[:], uint64(revision)}
		}
		query += "\n  AND v.tombstone = 0 AND v." + column + " = 1"
	case EnumerationWorkingSet:
		interest := `
FROM (
    SELECT object_id FROM materialization_interests
    WHERE tenant = ? AND owner_presentation = ? AND owner_domain = ? AND owner_generation = ?
      AND created_revision <= ?
      AND (removed_revision IS NULL OR removed_revision > ?)
    GROUP BY object_id
) interested`
		interestArgs := []any{string(tenant), uint8(scope.Presentation), string(scope.Domain), uint64(scope.Generation), uint64(revision), uint64(revision)}
		switch {
		case publication && !historicalPublication:
			query = "SELECT " + versionColumns + interest + `
JOIN source_driver_publication_objects v
  ON v.source_authority = ? AND v.publication_id = ?
 AND v.tenant = ? AND v.object_id = interested.object_id
WHERE v.revision <= ? AND v.tombstone = 0 AND v.file_provider_visible = 1`
			args = append(interestArgs, view.authority, view.publication, string(tenant), uint64(view.head))
		case historicalPublication:
			query += "\nSELECT " + versionColumns + interest + `
JOIN ranked_publication_versions v ON v.object_id = interested.object_id
WHERE v.version_rank = 1 AND v.tombstone = 0 AND v.file_provider_visible = 1`
			args = append(args, interestArgs...)
		default:
			query = "SELECT " + versionColumns + interest + `
JOIN object_versions v ON v.tenant = ? AND v.object_id = interested.object_id
WHERE v.revision = (
      SELECT MAX(v2.revision) FROM object_versions v2
      WHERE v2.tenant = v.tenant AND v2.object_id = v.object_id AND v2.revision <= ?
  )
  AND v.tombstone = 0
  AND v.file_provider_visible = 1`
			args = append(interestArgs, string(tenant), uint64(revision))
		}
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
	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		return ChangePage{}, err
	}
	if err := validateAnchor(cursor.Revision, view.head, view.floor); err != nil {
		return ChangePage{}, err
	}

	var changes []Change
	if len(view.publication) != 0 {
		changes, err = readPublicationChanges(
			ctx, tx, view, tenant, scopeKind, presentation, scopeParent,
			scopeDomain, scopeGeneration, cursor, limit+1,
		)
	} else {
		changes, err = readChanges(
			ctx, tx, tenant, scopeKind, presentation, scopeParent,
			scopeDomain, scopeGeneration, cursor, view.head, limit+1,
		)
	}
	if err != nil {
		return ChangePage{}, err
	}
	complete := len(changes) <= limit
	next := ChangeCursor{Revision: view.head, Sequence: CompleteChangeSequence}
	if !complete {
		changes = changes[:limit]
		last := changes[len(changes)-1]
		next = ChangeCursor{Revision: last.Revision, Sequence: last.Sequence}
	}
	if err := tx.Commit(); err != nil {
		return ChangePage{}, fmt.Errorf("catalog: finish changes: %w", err)
	}
	return ChangePage{Floor: view.floor, Head: view.head, Next: next, Complete: complete, Changes: changes}, nil
}

func readPublicationChanges(
	ctx context.Context,
	tx *sql.Tx,
	view catalogReadView,
	tenant TenantID,
	scopeKind, presentation uint8,
	scopeParent []byte,
	scopeDomain string,
	scopeGeneration uint64,
	cursor ChangeCursor,
	limit int,
) ([]Change, error) {
	query := `WITH RECURSIVE publication_lineage(publication_id, predecessor_publication_id) AS (
    SELECT publication_id, predecessor_publication_id
    FROM source_driver_publications
    WHERE source_authority = ? AND publication_id = ?
    UNION
    SELECT predecessor.publication_id, predecessor.predecessor_publication_id
    FROM source_driver_publications predecessor
    JOIN publication_lineage successor
      ON predecessor.publication_id = successor.predecessor_publication_id
    WHERE predecessor.source_authority = ?
)
SELECT change.revision, change.sequence, change.kind, ` + versionColumns + `
FROM publication_lineage change_lineage
JOIN source_driver_publication_changes change
  ON change.source_authority = ?
 AND change.publication_id = change_lineage.publication_id
 AND change.tenant = ?
JOIN publication_lineage version_lineage
JOIN source_driver_publication_versions v
  ON v.source_authority = change.source_authority
 AND v.publication_id = version_lineage.publication_id
 AND v.tenant = change.tenant
 AND v.object_id = change.object_id
 AND v.revision = change.object_revision
WHERE change.scope_kind = ? AND change.presentation = ? AND change.scope_parent = ?
  AND change.scope_domain = ? AND change.scope_generation = ?
  AND change.revision <= ?
  AND (change.revision > ? OR (change.revision = ? AND change.sequence > ?))
ORDER BY change.revision, change.sequence LIMIT ?`
	rows, err := tx.QueryContext(ctx, query,
		view.authority, view.publication, view.authority,
		view.authority, string(tenant), scopeKind, presentation, scopeParent,
		scopeDomain, scopeGeneration, uint64(view.head),
		uint64(cursor.Revision), uint64(cursor.Revision), cursor.Sequence, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("catalog: query active publication changes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanChanges(rows)
}

func readChanges(ctx context.Context, tx *sql.Tx, tenant TenantID, scopeKind, presentation uint8, scopeParent []byte, scopeDomain string, scopeGeneration uint64, cursor ChangeCursor, head Revision, limit int) ([]Change, error) {
	query := "SELECT c.revision, c.sequence, c.kind, " + versionColumns + `
FROM changes c
JOIN object_versions v
  ON v.tenant = c.tenant AND v.object_id = c.object_id AND v.revision = c.object_revision
WHERE c.tenant = ? AND c.scope_kind = ? AND c.presentation = ? AND c.scope_parent = ?
  AND c.scope_domain = ? AND c.scope_generation = ?
  AND c.revision <= ?
  AND (c.revision > ? OR (c.revision = ? AND c.sequence > ?))
ORDER BY c.revision, c.sequence LIMIT ?`
	rows, err := tx.QueryContext(ctx, query, string(tenant), scopeKind, presentation, scopeParent, scopeDomain, scopeGeneration,
		uint64(head), uint64(cursor.Revision), uint64(cursor.Revision), cursor.Sequence, limit)
	if err != nil {
		return nil, fmt.Errorf("catalog: query changes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanChanges(rows)
}

func scanChanges(rows *sql.Rows) ([]Change, error) {
	var changes []Change
	for rows.Next() {
		var revision uint64
		var sequence uint32
		var kind uint8
		obj, err := scanObjectWithPrefix(rows, &revision, &sequence, &kind)
		if err != nil {
			return nil, fmt.Errorf("catalog: scan change: %w", err)
		}
		changeKind := ChangeKind(kind)
		if changeKind != ChangeDelete && changeKind != ChangeUpsert {
			return nil, fmt.Errorf("%w: corrupt change kind %d", ErrIntegrity, kind)
		}
		changes = append(changes, Change{
			Revision: Revision(revision), Sequence: sequence,
			Kind: changeKind, Object: obj,
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

func currentObjectFromView(
	ctx context.Context,
	q interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	view catalogReadView,
	tenant TenantID,
	id ObjectID,
	tombstone bool,
	visibility string,
) (Object, error) {
	var query string
	var args []any
	if len(view.publication) != 0 {
		query = "SELECT " + versionColumns + `
FROM source_driver_publication_objects v
WHERE v.source_authority = ? AND v.publication_id = ?
  AND v.tenant = ? AND v.object_id = ? AND v.revision <= ? AND v.tombstone = ?`
		args = []any{view.authority, view.publication, string(tenant), id[:], uint64(view.head), tombstone}
	} else {
		query = "SELECT " + versionColumns + `
FROM object_versions v
WHERE v.tenant = ? AND v.object_id = ?
  AND v.revision = (SELECT MAX(v2.revision) FROM object_versions v2
      WHERE v2.tenant = v.tenant AND v2.object_id = v.object_id AND v2.revision <= ?)
  AND v.tombstone = ?`
		args = []any{string(tenant), id[:], uint64(view.head), tombstone}
	}
	if visibility != "" {
		query += " AND v." + visibility + " = 1"
	}
	obj, err := scanObject(q.QueryRowContext(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: read active object: %w", err)
	}
	return obj, nil
}

func currentNamedObjectFromView(
	ctx context.Context,
	q interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	view catalogReadView,
	tenant TenantID,
	parent ObjectID,
	nameKey string,
	visibility string,
) (Object, error) {
	var query string
	var args []any
	if len(view.publication) != 0 {
		query = "SELECT " + versionColumns + `
FROM source_driver_publication_objects v
WHERE v.source_authority = ? AND v.publication_id = ?
  AND v.tenant = ? AND v.parent_id = ? AND v.name_key = ?
  AND v.revision <= ? AND v.tombstone = 0 AND v.` + visibility + " = 1"
		args = []any{view.authority, view.publication, string(tenant), parent[:], nameKey, uint64(view.head)}
	} else {
		query = "SELECT " + versionColumns + `
FROM object_versions v
WHERE v.tenant = ? AND v.parent_id = ? AND v.name_key = ?
  AND v.revision = (SELECT MAX(v2.revision) FROM object_versions v2
      WHERE v2.tenant = v.tenant AND v2.object_id = v.object_id AND v2.revision <= ?)
  AND v.tombstone = 0 AND v.` + visibility + " = 1"
		args = []any{string(tenant), parent[:], nameKey, uint64(view.head)}
	}
	obj, err := scanObject(q.QueryRowContext(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: read active name binding: %w", err)
	}
	return obj, nil
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

// effectiveRevisionState reads the active semantic publication revision.
type catalogReadView struct {
	head        Revision
	floor       Revision
	authority   string
	publication []byte
}

func readCatalogView(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenant TenantID) (catalogReadView, error) {
	var storedHead, floor uint64
	var activeGeneration, activeHead, activeSourceRevision uint64
	var applicationHead, applicationSourceRevision uint64
	var publicationSourceRevision, targetHead, targetGeneration uint64
	var applicationPhase uint8
	var publicationPrepared, targetPrepared int
	var generationAuthority, authority string
	var activeView, applicationView, applicationHeadDigest, publication []byte
	if err := q.QueryRowContext(ctx, `
SELECT tenant.head, tenant.floor, generation.content_source_id,
       activation.active_generation, activation.active_view_id,
       activation.active_catalog_head, activation.source_revision,
       application.source_authority, application.source_publication_id,
       application.staged_view_id, application.staged_catalog_head, application.staged_head_digest,
       application.staged_source_revision, application.phase,
       publication.source_revision, publication.prepared,
       target.catalog_head, target.generation, target.prepared
FROM tenants tenant
JOIN tenant_activations activation
  ON activation.tenant_id = tenant.tenant AND activation.active_generation IS NOT NULL
JOIN tenant_generations generation
  ON generation.tenant_id = activation.tenant_id
 AND generation.generation = activation.active_generation
JOIN tenant_applications application
  ON application.tenant_id = activation.tenant_id
 AND application.generation = activation.active_generation
 AND application.staged_view_id = activation.active_view_id
JOIN source_driver_publications publication
  ON publication.source_authority = application.source_authority
 AND publication.publication_id = application.source_publication_id
JOIN source_driver_publication_targets target
  ON target.source_authority = application.source_authority
 AND target.publication_id = application.source_publication_id
 AND target.tenant = activation.tenant_id
 AND target.generation = activation.active_generation
WHERE tenant.tenant = ?`, string(tenant)).Scan(
		&storedHead, &floor, &generationAuthority,
		&activeGeneration, &activeView, &activeHead, &activeSourceRevision,
		&authority, &publication, &applicationView, &applicationHead, &applicationHeadDigest,
		&applicationSourceRevision, &applicationPhase,
		&publicationSourceRevision, &publicationPrepared,
		&targetHead, &targetGeneration, &targetPrepared,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return catalogReadView{}, ErrNotFound
		}
		return catalogReadView{}, fmt.Errorf("catalog: read active publication: %w", err)
	}
	if len(activeView) != len(StagedViewID{}) || len(applicationView) != len(StagedViewID{}) ||
		!equalBytes(activeView, applicationView) || len(applicationHeadDigest) != sha256.Size ||
		len(publication) != len(causal.OperationID{}) ||
		authority == "" || authority != generationAuthority || activeGeneration != targetGeneration ||
		storedHead != activeHead || activeHead != applicationHead || applicationHead != targetHead ||
		activeSourceRevision != applicationSourceRevision || applicationSourceRevision != publicationSourceRevision ||
		publicationPrepared != 1 || targetPrepared != 1 ||
		(applicationPhase != uint8(TenantApplicationStaged) && applicationPhase != uint8(TenantApplicationRetiring)) {
		return catalogReadView{}, fmt.Errorf("%w: active source publication target is missing", ErrIntegrity)
	}
	if activeHead == 0 || activeSourceRevision == 0 {
		return catalogReadView{}, fmt.Errorf("%w: invalid active publication identity", ErrIntegrity)
	}
	view := catalogReadView{
		head: Revision(activeHead), floor: Revision(floor), authority: authority,
		publication: append([]byte(nil), publication...),
	}
	if view.head < view.floor {
		return catalogReadView{}, fmt.Errorf("%w: active publication head precedes compaction floor", ErrIntegrity)
	}
	return view, nil
}

func effectiveRevisionState(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenant TenantID) (Revision, Revision, error) {
	view, err := readCatalogView(ctx, q, tenant)
	return view.head, view.floor, err
}

func pendingSourceDriverTarget(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenant TenantID) (bool, error) {
	var pending int
	if err := q.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM source_driver_stage_targets target
    JOIN source_publication_stages stage
      ON stage.source_authority = target.source_authority
     AND stage.stage_operation_id = target.stage_operation_id
     AND stage.stage_kind = 2
    WHERE target.tenant = ?
      AND NOT EXISTS (
          SELECT 1 FROM source_driver_stage_receipts receipt
          WHERE receipt.source_authority = target.source_authority
            AND receipt.stage_operation_id = target.stage_operation_id
      )
      AND NOT EXISTS (
          WITH RECURSIVE published(publication_id, predecessor_publication_id) AS (
              SELECT publication.publication_id, publication.predecessor_publication_id
              FROM source_driver_publication_heads head
              JOIN source_driver_publications publication
                ON publication.source_authority = head.source_authority
               AND publication.publication_id = head.publication_id
              WHERE head.source_authority = target.source_authority
              UNION
              SELECT predecessor.publication_id, predecessor.predecessor_publication_id
              FROM source_driver_publications predecessor
              JOIN published successor
                ON predecessor.publication_id = successor.predecessor_publication_id
              WHERE predecessor.source_authority = target.source_authority
          )
          SELECT 1 FROM published WHERE publication_id = target.stage_operation_id
      )
)`, string(tenant)).Scan(&pending); err != nil {
		return false, fmt.Errorf("catalog: inspect pending semantic publication: %w", err)
	}
	return pending != 0, nil
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
		&obj.Name, &kind, &mode, &obj.Size, &hash, &obj.LinkTarget,
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
	if err := validateStoredObject(obj); err != nil {
		return Object{}, fmt.Errorf("%w: corrupt object revision: %v", ErrIntegrity, err)
	}
	return obj, nil
}

func validateStoredObject(obj Object) error {
	if _, err := NewTenantID(string(obj.Tenant)); err != nil {
		return err
	}
	if zeroObjectID(obj.ID) || zeroObjectID(obj.Parent) ||
		obj.Revision == 0 || obj.MetadataRevision == 0 || obj.Size < 0 {
		return fmt.Errorf("incomplete object identity or revision")
	}
	if err := validateProofOrder(
		obj.Convergence.Desired,
		obj.Convergence.Observed,
		obj.Convergence.Verified,
		obj.Convergence.Applied,
	); err != nil {
		return err
	}
	if obj.Name == "" {
		if obj.ID != obj.Parent || obj.Kind != KindDirectory {
			return fmt.Errorf("empty name outside the tenant root")
		}
	} else if err := validateName(obj.Name); err != nil {
		return err
	}
	switch obj.Kind {
	case KindDirectory:
		if obj.ContentRevision != 0 || obj.Size != 0 ||
			obj.Hash != (ContentHash{}) || obj.LinkTarget != "" {
			return fmt.Errorf("directory carries content")
		}
	case KindFile:
		if obj.ContentRevision == 0 || obj.LinkTarget != "" {
			return fmt.Errorf("file content is incomplete")
		}
	case KindSymlink:
		if obj.ContentRevision == 0 {
			return fmt.Errorf("symlink content revision is zero")
		}
		if err := validateLinkTarget(obj.LinkTarget); err != nil {
			return err
		}
		target := []byte(obj.LinkTarget)
		if obj.Size != int64(len(target)) || obj.Hash != ContentHash(sha256.Sum256(target)) {
			return fmt.Errorf("symlink content identity does not match its target")
		}
	default:
		return fmt.Errorf("unknown object kind %d", obj.Kind)
	}
	return nil
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
