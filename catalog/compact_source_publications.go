package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	sourceDriverPublicationSemantic  = 1
	sourceDriverPublicationCompacted = 2
)

const (
	sourcePublicationCompactTargets uint8 = iota + 1
	sourcePublicationCompactObjects
	sourcePublicationCompactVersions
	sourcePublicationCompactChanges
	sourcePublicationCompactSeal
)

const sourcePublicationCompactionDomain = "fusekit.source-publication-compaction.v1\x00"

const (
	sourcePublicationBeforeVisibilityCAS = "source_publication_compaction.before_visibility_cas"
	sourcePublicationAfterVisibilityCAS  = "source_publication_compaction.after_visibility_cas"
)

type sourcePublicationCompaction struct {
	authority string
	source    []byte
	target    []byte
	epoch     uint64
	phase     uint8
}

func (c *Catalog) compactSourceDriverPublicationPage(
	ctx context.Context,
	tx *sql.Tx,
	limit int,
) (retired int, more bool, resultErr error) {
	if limit < 1 {
		return 0, false, fmt.Errorf("%w: invalid publication compaction limit", ErrInvalidObject)
	}
	state, found, err := readSourcePublicationCompaction(ctx, tx)
	if err != nil {
		return 0, false, err
	}
	if !found {
		retired, more, err := compactOrphanSourcePublicationPage(ctx, tx, limit)
		if err != nil || retired != 0 || more {
			return retired, more, err
		}
		created, err := beginSourcePublicationCompaction(ctx, tx)
		if err != nil {
			return 0, false, err
		}
		if created {
			return 0, true, nil
		}
		return 0, false, nil
	}
	var active []byte
	var epoch, publicationTargetEpoch, currentTargetEpoch uint64
	if err := tx.QueryRowContext(ctx, `
SELECT visibility.active_publication_id, visibility.visibility_epoch,
       publication.target_epoch, target_epoch.target_epoch
FROM source_driver_visibility visibility
JOIN source_driver_publications publication
  ON publication.source_authority = visibility.source_authority
 AND publication.publication_id = visibility.active_publication_id
JOIN source_driver_target_epochs target_epoch
  ON target_epoch.source_authority = visibility.source_authority
WHERE visibility.source_authority = ?`, state.authority).Scan(
		&active, &epoch, &publicationTargetEpoch, &currentTargetEpoch,
	); err != nil {
		return 0, false, err
	}
	if !equalBytes(active, state.source) || epoch != state.epoch || publicationTargetEpoch != currentTargetEpoch {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM source_driver_publication_compactions WHERE source_authority = ?`,
			state.authority); err != nil {
			return 0, false, err
		}
		return 1, true, nil
	}
	var copied int
	switch state.phase {
	case sourcePublicationCompactTargets:
		copied, err = copySourcePublicationTargets(ctx, tx, state, limit)
	case sourcePublicationCompactObjects:
		copied, err = copySourcePublicationObjects(ctx, tx, state, limit)
	case sourcePublicationCompactVersions:
		copied, err = copyRetainedSourcePublicationVersions(ctx, tx, state, limit)
	case sourcePublicationCompactChanges:
		copied, err = copyRetainedSourcePublicationChanges(ctx, tx, state, limit)
	case sourcePublicationCompactSeal:
		return c.sealSourcePublicationCompaction(ctx, tx, state, limit)
	default:
		err = ErrIntegrity
	}
	if err != nil {
		return 0, false, err
	}
	if copied != 0 {
		return 0, true, nil
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_compactions SET phase = phase + 1
WHERE source_authority = ? AND compaction_publication_id = ? AND phase = ?`,
		state.authority, state.target, state.phase)
	if err != nil {
		return 0, false, err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return 0, false, ErrMutationConflict
	}
	return 0, true, nil
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func readSourcePublicationCompaction(
	ctx context.Context,
	tx *sql.Tx,
) (sourcePublicationCompaction, bool, error) {
	var state sourcePublicationCompaction
	err := tx.QueryRowContext(ctx, `
SELECT source_authority, source_publication_id, compaction_publication_id,
       expected_visibility_epoch, phase
FROM source_driver_publication_compactions ORDER BY source_authority LIMIT 1`).Scan(
		&state.authority, &state.source, &state.target, &state.epoch, &state.phase,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sourcePublicationCompaction{}, false, nil
	}
	if err != nil {
		return sourcePublicationCompaction{}, false, err
	}
	if len(state.source) != len(ObjectID{}) || len(state.target) != len(ObjectID{}) {
		return sourcePublicationCompaction{}, false, ErrIntegrity
	}
	return state, true, nil
}

func beginSourcePublicationCompaction(ctx context.Context, tx *sql.Tx) (bool, error) {
	var authority string
	var source []byte
	var epoch uint64
	err := tx.QueryRowContext(ctx, `
SELECT visibility.source_authority, visibility.active_publication_id, visibility.visibility_epoch
FROM source_driver_visibility visibility
JOIN source_driver_publications publication
  ON publication.source_authority = visibility.source_authority
 AND publication.publication_id = visibility.active_publication_id
WHERE length(publication.predecessor_publication_id) = 16
   OR EXISTS (
       SELECT 1
       FROM source_driver_publication_versions version
       JOIN tenants tenant ON tenant.tenant = version.tenant
       WHERE version.source_authority = publication.source_authority
         AND version.publication_id = publication.publication_id
         AND NOT (
             version.revision >= tenant.floor
             OR version.revision = (
                 SELECT MAX(baseline.revision)
                 FROM source_driver_publication_versions baseline
                 WHERE baseline.source_authority = version.source_authority
                   AND baseline.publication_id = version.publication_id
                   AND baseline.tenant = version.tenant
                   AND baseline.object_id = version.object_id
                   AND baseline.revision <= tenant.floor
             )
             OR EXISTS (
                 SELECT 1 FROM handles handle
                 WHERE handle.tenant = version.tenant
                   AND handle.object_id = version.object_id
                   AND handle.object_revision = version.revision AND handle.closed = 0
             )
             OR EXISTS (
                 SELECT 1 FROM source_driver_publication_changes change
                 WHERE change.source_authority = version.source_authority
                   AND change.publication_id = version.publication_id
                   AND change.tenant = version.tenant AND change.revision > tenant.floor
                   AND change.object_id = version.object_id
                   AND change.object_revision = version.revision
             )
         )
   )
   OR EXISTS (
       SELECT 1
       FROM source_driver_publication_changes change
       JOIN tenants tenant ON tenant.tenant = change.tenant
       WHERE change.source_authority = publication.source_authority
         AND change.publication_id = publication.publication_id
         AND change.revision <= tenant.floor
   )
ORDER BY visibility.source_authority LIMIT 1`).Scan(&authority, &source, &epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	generated, err := NewObjectID()
	if err != nil {
		return false, err
	}
	target := generated[:]
	digest := sourcePublicationCompactionDigest(authority, source, target, epoch)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publications(
    source_authority, publication_id, publication_kind, identity_digest,
    target_count, targets_digest, stage_sequence, stage_item_count, stage_byte_count,
    stage_digest, predecessor_publication_id, predecessor_revision, source_revision,
    expected_visibility_epoch, target_epoch, phase, cursor_tenant, cursor_key,
    initialized_target_count, prepared_target_count, item_count, byte_count,
    rolling_digest, prepared
)
SELECT source_authority, ?, ?, ?, target_count, targets_digest,
       stage_sequence, stage_item_count, stage_byte_count, stage_digest,
       zeroblob(0), 0, source_revision, ?, target_epoch, ?, '', '', 0, 0, 0, 0,
       zeroblob(32), 0
FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ?`,
		target, sourceDriverPublicationCompacted, digest[:], epoch,
		sourceDriverPublicationPreparing, authority, source); err != nil {
		return false, mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_compactions(
    source_authority, source_publication_id, compaction_publication_id,
    expected_visibility_epoch, phase
) VALUES (?, ?, ?, ?, ?)`, authority, source, target, epoch, sourcePublicationCompactTargets); err != nil {
		return false, mapConstraint(err)
	}
	return true, nil
}

func copySourcePublicationTargets(
	ctx context.Context, tx *sql.Tx, state sourcePublicationCompaction, limit int,
) (int, error) {
	result, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_targets(
    source_authority, publication_id, tenant, generation, root_key, catalog_operation_id,
    predecessor_head, catalog_head, catalog_fingerprint, file_provider_fingerprint,
    changed, provider_changed, object_count, phase, cursor_key, cursor_object_id,
    cursor_revision, catalog_state, provider_state, next_change_sequence, prepared
)
SELECT source.source_authority, ?, source.tenant, source.generation, source.root_key,
       source.catalog_operation_id,
       source.predecessor_head, source.catalog_head, source.catalog_fingerprint,
       source.file_provider_fingerprint, source.changed, source.provider_changed,
       source.object_count, source.phase, source.cursor_key, source.cursor_object_id,
       source.cursor_revision, source.catalog_state, source.provider_state,
       source.next_change_sequence, 1
FROM source_driver_publication_targets source
WHERE source.source_authority = ? AND source.publication_id = ?
  AND NOT EXISTS (
      SELECT 1 FROM source_driver_publication_targets target
      WHERE target.source_authority = source.source_authority
        AND target.publication_id = ? AND target.tenant = source.tenant
  )
ORDER BY source.tenant LIMIT ?`,
		state.target, state.authority, state.source, state.target, limit)
	if err != nil {
		return 0, mapConstraint(err)
	}
	return rowsAffectedInt(result)
}

func copySourcePublicationObjects(
	ctx context.Context, tx *sql.Tx, state sourcePublicationCompaction, limit int,
) (int, error) {
	result, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_objects(
    source_authority, publication_id, tenant, source_key, object_id, parent_id,
    revision, metadata_revision, content_revision, name, name_key, kind, mode, size,
    hash, link_target, desired_revision, observed_revision, verified_revision,
    applied_revision, mount_visible, file_provider_visible, tombstone
)
SELECT source.source_authority, ?, source.tenant, source.source_key, source.object_id,
       source.parent_id, source.revision, source.metadata_revision, source.content_revision,
       source.name, source.name_key, source.kind, source.mode, source.size, source.hash,
       source.link_target, source.desired_revision, source.observed_revision,
       source.verified_revision, source.applied_revision, source.mount_visible,
       source.file_provider_visible, source.tombstone
FROM source_driver_publication_objects source
WHERE source.source_authority = ? AND source.publication_id = ?
  AND NOT EXISTS (
      SELECT 1 FROM source_driver_publication_objects target
      WHERE target.source_authority = source.source_authority
        AND target.publication_id = ? AND target.tenant = source.tenant
        AND target.source_key = source.source_key
  )
ORDER BY source.tenant, source.source_key LIMIT ?`,
		state.target, state.authority, state.source, state.target, limit)
	if err != nil {
		return 0, mapConstraint(err)
	}
	return rowsAffectedInt(result)
}

func copyRetainedSourcePublicationVersions(
	ctx context.Context, tx *sql.Tx, state sourcePublicationCompaction, limit int,
) (int, error) {
	result, err := tx.ExecContext(ctx, `
WITH RECURSIVE lineage(publication_id, predecessor_publication_id) AS (
    SELECT publication_id, predecessor_publication_id
    FROM source_driver_publications
    WHERE source_authority = ? AND publication_id = ?
    UNION
    SELECT predecessor.publication_id, predecessor.predecessor_publication_id
    FROM source_driver_publications predecessor
    JOIN lineage successor ON predecessor.publication_id = successor.predecessor_publication_id
    WHERE predecessor.source_authority = ?
), all_versions AS (
    SELECT version.*, publication.source_revision,
           ROW_NUMBER() OVER (
               PARTITION BY version.tenant, version.object_id, version.revision
               ORDER BY publication.source_revision DESC, publication.publication_id DESC
           ) AS duplicate_rank
    FROM lineage
    JOIN source_driver_publications publication
      ON publication.source_authority = ? AND publication.publication_id = lineage.publication_id
    JOIN source_driver_publication_versions version
      ON version.source_authority = publication.source_authority
     AND version.publication_id = publication.publication_id
), retained AS (
    SELECT version.*
    FROM all_versions version
    JOIN tenants tenant ON tenant.tenant = version.tenant
    WHERE version.duplicate_rank = 1 AND (
        version.revision >= tenant.floor
        OR version.revision = (
            SELECT MAX(baseline.revision) FROM all_versions baseline
            WHERE baseline.tenant = version.tenant
              AND baseline.object_id = version.object_id
              AND baseline.revision <= tenant.floor
        )
        OR EXISTS (
            SELECT 1 FROM handles handle
            WHERE handle.tenant = version.tenant AND handle.object_id = version.object_id
              AND handle.object_revision = version.revision AND handle.closed = 0
        )
        OR EXISTS (
            SELECT 1
            FROM lineage change_lineage
            JOIN source_driver_publication_changes change
              ON change.source_authority = ?
             AND change.publication_id = change_lineage.publication_id
            WHERE change.tenant = version.tenant AND change.revision > tenant.floor
              AND change.object_id = version.object_id
              AND change.object_revision = version.revision
        )
    )
)
INSERT INTO source_driver_publication_versions(
    source_authority, publication_id, tenant, object_id, parent_id, revision,
    metadata_revision, content_revision, name, name_key, kind, mode, size, hash,
    link_target, desired_revision, observed_revision, verified_revision, applied_revision,
    mount_visible, file_provider_visible, tombstone
)
SELECT ?, ?, retained.tenant, retained.object_id, retained.parent_id, retained.revision,
       retained.metadata_revision, retained.content_revision, retained.name, retained.name_key,
       retained.kind, retained.mode, retained.size, retained.hash, retained.link_target,
       retained.desired_revision, retained.observed_revision, retained.verified_revision,
       retained.applied_revision, retained.mount_visible, retained.file_provider_visible,
       retained.tombstone
FROM retained
WHERE NOT EXISTS (
    SELECT 1 FROM source_driver_publication_versions target
    WHERE target.source_authority = ? AND target.publication_id = ?
      AND target.tenant = retained.tenant AND target.object_id = retained.object_id
      AND target.revision = retained.revision
)
ORDER BY retained.tenant, retained.object_id, retained.revision LIMIT ?`,
		state.authority, state.source, state.authority, state.authority, state.authority,
		state.authority, state.target, state.authority, state.target, limit)
	if err != nil {
		return 0, mapConstraint(err)
	}
	return rowsAffectedInt(result)
}

func copyRetainedSourcePublicationChanges(
	ctx context.Context, tx *sql.Tx, state sourcePublicationCompaction, limit int,
) (int, error) {
	result, err := tx.ExecContext(ctx, `
WITH RECURSIVE lineage(publication_id, predecessor_publication_id) AS (
    SELECT publication_id, predecessor_publication_id
    FROM source_driver_publications
    WHERE source_authority = ? AND publication_id = ?
    UNION
    SELECT predecessor.publication_id, predecessor.predecessor_publication_id
    FROM source_driver_publications predecessor
    JOIN lineage successor ON predecessor.publication_id = successor.predecessor_publication_id
    WHERE predecessor.source_authority = ?
), ranked AS (
    SELECT change.*, publication.source_revision,
           ROW_NUMBER() OVER (
               PARTITION BY change.tenant, change.revision, change.scope_kind,
                            change.presentation, change.scope_parent, change.scope_domain,
                            change.scope_generation, change.sequence
               ORDER BY publication.source_revision DESC, publication.publication_id DESC
           ) AS duplicate_rank
    FROM lineage
    JOIN source_driver_publications publication
      ON publication.source_authority = ? AND publication.publication_id = lineage.publication_id
    JOIN source_driver_publication_changes change
      ON change.source_authority = publication.source_authority
     AND change.publication_id = publication.publication_id
    JOIN tenants tenant ON tenant.tenant = change.tenant
    WHERE change.revision > tenant.floor
)
INSERT INTO source_driver_publication_changes(
    source_authority, publication_id, tenant, revision, scope_kind, presentation,
    scope_parent, scope_domain, scope_generation, sequence, kind, object_id, object_revision
)
SELECT ?, ?, ranked.tenant, ranked.revision, ranked.scope_kind, ranked.presentation,
       ranked.scope_parent, ranked.scope_domain, ranked.scope_generation, ranked.sequence,
       ranked.kind, ranked.object_id, ranked.object_revision
FROM ranked
WHERE ranked.duplicate_rank = 1 AND NOT EXISTS (
    SELECT 1 FROM source_driver_publication_changes target
    WHERE target.source_authority = ? AND target.publication_id = ?
      AND target.tenant = ranked.tenant AND target.revision = ranked.revision
      AND target.scope_kind = ranked.scope_kind AND target.presentation = ranked.presentation
      AND target.scope_parent = ranked.scope_parent AND target.scope_domain = ranked.scope_domain
      AND target.scope_generation = ranked.scope_generation AND target.sequence = ranked.sequence
)
ORDER BY ranked.tenant, ranked.revision, ranked.scope_kind, ranked.presentation,
         ranked.scope_parent, ranked.scope_domain, ranked.scope_generation, ranked.sequence
LIMIT ?`, state.authority, state.source, state.authority, state.authority,
		state.authority, state.target, state.authority, state.target, limit)
	if err != nil {
		return 0, mapConstraint(err)
	}
	return rowsAffectedInt(result)
}

func (c *Catalog) sealSourcePublicationCompaction(
	ctx context.Context, tx *sql.Tx, state sourcePublicationCompaction, limit int,
) (int, bool, error) {
	copySteps := []func(context.Context, *sql.Tx, sourcePublicationCompaction, int) (int, error){
		copySourcePublicationTargets,
		copySourcePublicationObjects,
		copyRetainedSourcePublicationVersions,
		copyRetainedSourcePublicationChanges,
	}
	for _, copyStep := range copySteps {
		copied, err := copyStep(ctx, tx, state, limit)
		if err != nil {
			return 0, false, err
		}
		if copied != 0 {
			return 0, true, nil
		}
	}
	if err := validateSourcePublicationCompaction(ctx, tx, state); err != nil {
		return 0, false, err
	}
	digest := sourcePublicationCompactionDigest(state.authority, state.source, state.target, state.epoch)
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publications
SET initialized_target_count = target_count, prepared_target_count = target_count,
    phase = ?, rolling_digest = ?, prepared = 1
WHERE source_authority = ? AND publication_id = ? AND publication_kind = ? AND prepared = 0`,
		sourceDriverPublicationPrepared, digest[:], state.authority, state.target,
		sourceDriverPublicationCompacted)
	if err != nil {
		return 0, false, err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return 0, false, ErrMutationConflict
	}
	if err := c.trip(sourcePublicationBeforeVisibilityCAS); err != nil {
		return 0, false, err
	}
	result, err = tx.ExecContext(ctx, `
UPDATE source_driver_visibility
SET active_publication_id = ?, visibility_epoch = visibility_epoch + 1
WHERE source_authority = ? AND active_publication_id = ? AND visibility_epoch = ?`,
		state.target, state.authority, state.source, state.epoch)
	if err != nil {
		return 0, false, err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return 0, false, ErrMutationConflict
	}
	if err := c.trip(sourcePublicationAfterVisibilityCAS); err != nil {
		return 0, false, err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM source_driver_publication_compactions WHERE source_authority = ?`,
		state.authority); err != nil {
		return 0, false, err
	}
	return 0, true, nil
}

func validateSourcePublicationCompaction(
	ctx context.Context, tx *sql.Tx, state sourcePublicationCompaction,
) error {
	var mismatch int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT tenant, generation, root_key, predecessor_head, catalog_head,
           catalog_fingerprint, file_provider_fingerprint, changed, provider_changed,
           object_count
    FROM source_driver_publication_targets
    WHERE source_authority = ? AND publication_id = ?
    EXCEPT
    SELECT tenant, generation, root_key, predecessor_head, catalog_head,
           catalog_fingerprint, file_provider_fingerprint, changed, provider_changed,
           object_count
    FROM source_driver_publication_targets
    WHERE source_authority = ? AND publication_id = ?
) OR EXISTS(
    SELECT tenant, generation, root_key, predecessor_head, catalog_head,
           catalog_fingerprint, file_provider_fingerprint, changed, provider_changed,
           object_count
    FROM source_driver_publication_targets
    WHERE source_authority = ? AND publication_id = ?
    EXCEPT
    SELECT tenant, generation, root_key, predecessor_head, catalog_head,
           catalog_fingerprint, file_provider_fingerprint, changed, provider_changed,
           object_count
    FROM source_driver_publication_targets
    WHERE source_authority = ? AND publication_id = ?
)`, state.authority, state.source, state.authority, state.target,
		state.authority, state.target, state.authority, state.source).Scan(&mismatch); err != nil {
		return err
	}
	if mismatch != 0 {
		return ErrIntegrity
	}
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT tenant, source_key, object_id, parent_id, revision, metadata_revision,
           content_revision, name, name_key, kind, mode, size, hash, link_target,
           desired_revision, observed_revision, verified_revision, applied_revision,
           mount_visible, file_provider_visible, tombstone
    FROM source_driver_publication_objects
    WHERE source_authority = ? AND publication_id = ?
    EXCEPT
    SELECT tenant, source_key, object_id, parent_id, revision, metadata_revision,
           content_revision, name, name_key, kind, mode, size, hash, link_target,
           desired_revision, observed_revision, verified_revision, applied_revision,
           mount_visible, file_provider_visible, tombstone
    FROM source_driver_publication_objects
    WHERE source_authority = ? AND publication_id = ?
) OR EXISTS(
    SELECT tenant, source_key, object_id, parent_id, revision, metadata_revision,
           content_revision, name, name_key, kind, mode, size, hash, link_target,
           desired_revision, observed_revision, verified_revision, applied_revision,
           mount_visible, file_provider_visible, tombstone
    FROM source_driver_publication_objects
    WHERE source_authority = ? AND publication_id = ?
    EXCEPT
    SELECT tenant, source_key, object_id, parent_id, revision, metadata_revision,
           content_revision, name, name_key, kind, mode, size, hash, link_target,
           desired_revision, observed_revision, verified_revision, applied_revision,
           mount_visible, file_provider_visible, tombstone
    FROM source_driver_publication_objects
    WHERE source_authority = ? AND publication_id = ?
)`, state.authority, state.source, state.authority, state.target,
		state.authority, state.target, state.authority, state.source).Scan(&mismatch); err != nil {
		return err
	}
	if mismatch != 0 {
		return ErrIntegrity
	}
	return nil
}

func sourcePublicationCompactionDigest(
	authority string, source, target []byte, epoch uint64,
) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(sourcePublicationCompactionDomain))
	_, _ = hash.Write([]byte(authority))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(source)
	_, _ = hash.Write(target)
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], epoch)
	_, _ = hash.Write(raw[:])
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func compactOrphanSourcePublicationPage(
	ctx context.Context, tx *sql.Tx, limit int,
) (int, bool, error) {
	var authority string
	var publication []byte
	err := tx.QueryRowContext(ctx, `
WITH RECURSIVE active_lineage(source_authority, publication_id, predecessor_publication_id) AS (
    SELECT visibility.source_authority, publication.publication_id,
           publication.predecessor_publication_id
    FROM source_driver_visibility visibility
    JOIN source_driver_publications publication
      ON publication.source_authority = visibility.source_authority
     AND publication.publication_id = visibility.active_publication_id
    UNION
    SELECT predecessor.source_authority, predecessor.publication_id,
           predecessor.predecessor_publication_id
    FROM source_driver_publications predecessor
    JOIN active_lineage successor
      ON predecessor.source_authority = successor.source_authority
     AND predecessor.publication_id = successor.predecessor_publication_id
)
SELECT candidate.source_authority, candidate.publication_id
FROM source_driver_publications candidate
JOIN source_driver_visibility visibility
  ON visibility.source_authority = candidate.source_authority
WHERE candidate.source_revision <= visibility.active_source_revision
  AND NOT EXISTS (
      SELECT 1 FROM active_lineage active
      WHERE active.source_authority = candidate.source_authority
        AND active.publication_id = candidate.publication_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM source_driver_publication_compactions compaction
      WHERE compaction.source_authority = candidate.source_authority
        AND (compaction.source_publication_id = candidate.publication_id
          OR compaction.compaction_publication_id = candidate.publication_id)
  )
  AND NOT EXISTS (
      SELECT 1
      FROM source_driver_stages stage
      JOIN source_publication_stages source_stage
        ON source_stage.source_authority = stage.source_authority
       AND source_stage.stage_operation_id = stage.stage_operation_id
      LEFT JOIN source_driver_stage_receipts receipt
        ON receipt.source_authority = stage.source_authority
       AND receipt.stage_operation_id = stage.stage_operation_id
      WHERE stage.source_authority = candidate.source_authority
        AND stage.stage_operation_id = candidate.publication_id
        AND receipt.stage_operation_id IS NULL
  )
ORDER BY candidate.source_authority, candidate.source_revision, candidate.publication_id
LIMIT 1`).Scan(&authority, &publication)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	tables := []string{
		"source_driver_publication_changes",
		"source_driver_publication_versions",
		"source_driver_publication_objects",
		"source_driver_publication_targets",
	}
	for _, table := range tables {
		if table == "source_driver_publication_versions" || table == "source_driver_publication_objects" {
			candidateStatement := `INSERT OR IGNORE INTO blob_gc_candidates(hash)
SELECT hash FROM ` + table + `
WHERE rowid IN (
    SELECT rowid FROM ` + table + `
    WHERE source_authority = ? AND publication_id = ? LIMIT ?
) AND kind = ? AND tombstone = 0`
			if _, err := tx.ExecContext(ctx, candidateStatement, authority, publication, limit, uint8(KindFile)); err != nil {
				return 0, false, err
			}
		}
		statement := `DELETE FROM ` + table + ` WHERE rowid IN (
    SELECT rowid FROM ` + table + `
    WHERE source_authority = ? AND publication_id = ? LIMIT ?
)`
		result, err := tx.ExecContext(ctx, statement, authority, publication, limit)
		if err != nil {
			return 0, false, err
		}
		count, err := rowsAffectedInt(result)
		if err != nil {
			return 0, false, err
		}
		if count != 0 {
			return count, true, nil
		}
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ?`, authority, publication)
	if err != nil {
		return 0, false, err
	}
	count, err := rowsAffectedInt(result)
	if err != nil {
		return 0, false, err
	}
	if count != 1 {
		return 0, false, ErrMutationConflict
	}
	return 1, true, nil
}
