package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// VerifyMaterialization proves every interested File Provider object at one catalog snapshot.
func VerifyMaterialization(
	ctx context.Context,
	path string,
	tenant TenantID,
	generation Generation,
	revision Revision,
) error {
	if !exactAbsolutePath(path) || tenant == "" || generation == 0 || revision == 0 {
		return fmt.Errorf("%w: materialization verification identity is incomplete", ErrInvalidObject)
	}
	db, err := sql.Open("sqlite", sqliteDSN(path, false))
	if err != nil {
		return fmt.Errorf("catalog: open materialization snapshot: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("catalog: connect materialization snapshot: %w", err)
	}
	var head, floor, currentGeneration uint64
	if err := db.QueryRowContext(ctx, `
SELECT t.head, t.floor, s.generation
FROM tenants t JOIN tenant_state s ON s.tenant = t.tenant
WHERE t.tenant = ?`, string(tenant)).Scan(&head, &floor, &currentGeneration); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("catalog: read materialization snapshot identity: %w", err)
	}
	if Generation(currentGeneration) != generation || revision > Revision(head) || revision < Revision(floor) {
		return fmt.Errorf("%w: materialization snapshot generation or revision changed", ErrInvalidTransition)
	}
	var expected uint64
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(DISTINCT object_id) FROM materialization_interests
WHERE tenant = ? AND owner_presentation = ? AND owner_generation = ?
  AND created_revision <= ? AND (removed_revision IS NULL OR removed_revision > ?)`,
		string(tenant), uint8(PresentationFileProvider), uint64(generation), uint64(revision), uint64(revision)).Scan(&expected); err != nil {
		return fmt.Errorf("catalog: count materialization snapshot: %w", err)
	}
	rows, err := db.QueryContext(ctx, `
SELECT `+versionColumns+`
FROM (
    SELECT DISTINCT object_id FROM materialization_interests
    WHERE tenant = ? AND owner_presentation = ? AND owner_generation = ?
      AND created_revision <= ? AND (removed_revision IS NULL OR removed_revision > ?)
) interested
JOIN object_versions v ON v.tenant = ? AND v.object_id = interested.object_id
WHERE v.revision = (
    SELECT MAX(v2.revision) FROM object_versions v2
    WHERE v2.tenant = v.tenant AND v2.object_id = v.object_id AND v2.revision <= ?
)
AND v.tombstone = 0 AND v.file_provider_visible = 1
ORDER BY v.object_id`, string(tenant), uint8(PresentationFileProvider), uint64(generation), uint64(revision), uint64(revision),
		string(tenant), uint64(revision))
	if err != nil {
		return fmt.Errorf("catalog: query materialization snapshot: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var verified uint64
	for rows.Next() {
		object, err := scanObject(rows)
		if err != nil {
			return fmt.Errorf("catalog: scan materialization object: %w", err)
		}
		if object.Kind != KindFile {
			verified++
			continue
		}
		file, err := os.Open(filepath.Join(path+".blobs", blobName(object.Hash)))
		if err != nil {
			return fmt.Errorf("catalog: open materialization content: %w", err)
		}
		verifyErr := verifyOpenFile(file, ContentRef{Hash: object.Hash, Size: object.Size})
		closeErr := file.Close()
		if verifyErr != nil || closeErr != nil {
			return errors.Join(verifyErr, closeErr)
		}
		verified++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("catalog: read materialization snapshot: %w", err)
	}
	if verified != expected {
		return fmt.Errorf("%w: materialization snapshot resolved %d of %d interested objects", ErrIntegrity, verified, expected)
	}
	return nil
}
