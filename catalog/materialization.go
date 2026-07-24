package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// VerifyMaterialization proves every visible child of an eligible materialized
// File Provider container at one catalog snapshot.
func (c *Catalog) VerifyMaterialization(
	ctx context.Context,
	tenant TenantID,
	generation Generation,
	revision Revision,
) error {
	if tenant == "" || generation == 0 || revision == 0 {
		return fmt.Errorf("%w: materialization verification identity is incomplete", ErrInvalidObject)
	}
	tx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("catalog: begin materialization verification: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		return err
	}
	var currentGeneration uint64
	if err := tx.QueryRowContext(ctx, `
SELECT generation FROM tenant_state WHERE tenant = ?`, string(tenant)).Scan(&currentGeneration); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("catalog: read materialization snapshot identity: %w", err)
	}
	if Generation(currentGeneration) != generation {
		return fmt.Errorf("%w: materialization snapshot generation or revision changed", ErrInvalidTransition)
	}
	if err := validateAnchor(revision, view.head, view.floor); err != nil {
		return err
	}
	var query string
	var args []any
	materialized := `
FROM (
    SELECT container.container_id
    FROM file_provider_materialization_heads head
    JOIN file_provider_materialized_containers container
      ON container.tenant = head.tenant AND container.domain_id = head.domain_id
     AND container.generation = head.generation
     AND container.backing_store_identity = head.backing_store_identity
    WHERE head.tenant = ? AND head.generation = ? AND head.eligible = 1
) materialized`
	materializedArgs := []any{string(tenant), uint64(generation)}
	switch {
	case len(view.publication) != 0 && revision == view.head:
		query = "SELECT " + versionColumns + materialized + `
JOIN source_driver_publication_objects v
  ON v.source_authority = ? AND v.publication_id = ?
 AND v.tenant = ? AND v.parent_id = materialized.container_id
WHERE v.revision <= ? AND v.tombstone = 0 AND v.file_provider_visible = 1
ORDER BY v.object_id`
		args = append(materializedArgs, view.authority, view.publication, string(tenant), uint64(view.head))
	case len(view.publication) != 0:
		query = publicationVersionLineageCTE + "\nSELECT " + versionColumns + materialized + `
JOIN ranked_publication_versions v ON v.parent_id = materialized.container_id
WHERE v.version_rank = 1 AND v.tombstone = 0 AND v.file_provider_visible = 1
ORDER BY v.object_id`
		args = []any{view.authority, view.publication, view.authority, view.authority, string(tenant), uint64(revision)}
		args = append(args, materializedArgs...)
	default:
		query = "SELECT " + versionColumns + materialized + `
JOIN object_versions v ON v.tenant = ? AND v.parent_id = materialized.container_id
WHERE v.revision = (
    SELECT MAX(v2.revision) FROM object_versions v2
    WHERE v2.tenant = v.tenant AND v2.object_id = v.object_id AND v2.revision <= ?
)
AND v.tombstone = 0 AND v.file_provider_visible = 1
ORDER BY v.object_id`
		args = append(materializedArgs, string(tenant), uint64(revision))
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("catalog: query materialization snapshot: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		object, err := scanObject(rows)
		if err != nil {
			return fmt.Errorf("catalog: scan materialization object: %w", err)
		}
		if object.Kind != KindFile {
			continue
		}
		file, err := c.openBlob(ctx, ContentRef{Hash: object.Hash, Size: object.Size})
		if err != nil {
			return fmt.Errorf("catalog: open materialization content: %w", err)
		}
		verifyErr := verifyOpenFile(file, ContentRef{Hash: object.Hash, Size: object.Size})
		closeErr := file.Close()
		if verifyErr != nil || closeErr != nil {
			return errors.Join(verifyErr, closeErr)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("catalog: read materialization snapshot: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("catalog: close materialization snapshot: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: finish materialization verification: %w", err)
	}
	return nil
}
