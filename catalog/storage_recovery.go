package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	storageDeleteAfterIntent = "storage.delete_after_intent"
	storageDeleteAfterUnlink = "storage.delete_after_unlink"
	storageDeleteAfterSync   = "storage.delete_after_sync"
)

// StorageRecoveryResult reports one bounded storage-journal recovery page.
type StorageRecoveryResult struct {
	Recovered   int
	Quarantined int
	More        bool
}

// RecoverStorageTransitions settles one bounded page owned by retired catalog
// generations. Quarantined transitions remain durable for inspection.
func (c *Catalog) RecoverStorageTransitions(
	ctx context.Context,
	limit int,
) (StorageRecoveryResult, error) {
	if limit <= 0 || limit > MaintenancePageLimit {
		return StorageRecoveryResult{}, fmt.Errorf("%w: invalid storage recovery limit", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT transition.transition_id
FROM catalog_generations generation INDEXED BY catalog_generations_retired
JOIN storage_transitions transition INDEXED BY storage_transitions_owner
  ON transition.owner_id = generation.owner_id
WHERE generation.retired = 1 AND transition.quarantined = 0
ORDER BY generation.owner_id, transition.transition_id
LIMIT ?`, limit+1)
	if err != nil {
		return StorageRecoveryResult{}, fmt.Errorf("catalog: select storage recovery page: %w", err)
	}
	var ids []storageTransitionID
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return StorageRecoveryResult{}, fmt.Errorf("catalog: scan storage recovery transition: %w", err)
		}
		if len(raw) != len(storageTransitionID{}) {
			_ = rows.Close()
			return StorageRecoveryResult{}, fmt.Errorf("%w: invalid storage recovery identity", ErrIntegrity)
		}
		var id storageTransitionID
		copy(id[:], raw)
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return StorageRecoveryResult{}, fmt.Errorf("catalog: read storage recovery page: %w", err)
	}
	if err := rows.Close(); err != nil {
		return StorageRecoveryResult{}, fmt.Errorf("catalog: close storage recovery page: %w", err)
	}
	result := StorageRecoveryResult{More: len(ids) > limit}
	if result.More {
		ids = ids[:limit]
	}
	for _, id := range ids {
		if err := c.settleStorageTransition(ctx, id, true); err != nil {
			var quarantined bool
			queryErr := c.db.QueryRowContext(ctx, `
SELECT quarantined FROM storage_transitions WHERE transition_id = ?`, id[:]).
				Scan(&quarantined)
			if queryErr == nil && quarantined {
				result.Quarantined++
				continue
			}
			return result, err
		}
		result.Recovered++
	}
	return result, nil
}

func (c *Catalog) prepareRetiredTemporaryDelete(
	ctx context.Context,
	owner HandleOwnerID,
	stage StageID,
	name string,
) (storageTransitionID, error) {
	if !validBlobStorageName(name) || filepath.Base(name) != name {
		return storageTransitionID{}, fmt.Errorf("%w: invalid temporary delete name", ErrIntegrity)
	}
	id, err := newStorageTransitionID()
	if err != nil {
		return storageTransitionID{}, err
	}
	c.storage.mu.Lock()
	defer c.storage.mu.Unlock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return storageTransitionID{}, fmt.Errorf("catalog: begin retired temporary delete: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var size int64
	var state uint8
	if err := tx.QueryRowContext(ctx, `
SELECT entry.state, entry.size
FROM storage_entries entry
JOIN catalog_generations generation ON generation.owner_id = entry.owner_id
JOIN content_stages content
  ON content.stage_id = entry.stage_id AND content.owner_id = entry.owner_id
WHERE entry.name = ? AND entry.kind = ? AND entry.stage_id = ? AND entry.owner_id = ?
  AND generation.retired = 1
  AND content.published = 0
  AND content.mutation_id IS NULL
  AND content.source_operation_id IS NULL`,
		name, storageEntryTemporary, stage[:], owner[:]).Scan(&state, &size); err != nil {
		return storageTransitionID{}, fmt.Errorf("catalog: inspect retired temporary entry: %w", err)
	}
	if state != storageEntryStable {
		return storageTransitionID{}, fmt.Errorf("%w: retired temporary entry is not stable", ErrIntegrity)
	}
	existing, found, err := c.readStorageTransitionForStage(ctx, tx, owner, stage)
	if err != nil {
		return storageTransitionID{}, err
	}
	if found {
		if existing.kind != storageTransitionDeleteTemporary ||
			existing.sourceName != name || existing.size != size ||
			existing.quarantined {
			return storageTransitionID{}, fmt.Errorf(
				"%w: conflicting temporary delete transition", ErrIntegrity,
			)
		}
		if err := tx.Commit(); err != nil {
			return storageTransitionID{}, fmt.Errorf("catalog: confirm temporary delete intent: %w", err)
		}
		return existing.id, nil
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO storage_transitions(
    transition_id, kind, owner_id, stage_id, source_name, size, new_blob
) VALUES (?, ?, ?, ?, ?, ?, 0)`,
		id[:], storageTransitionDeleteTemporary, owner[:], stage[:], name, size); err != nil {
		return storageTransitionID{}, fmt.Errorf("catalog: journal retired temporary delete: %w", mapConstraint(err))
	}
	if err := tx.Commit(); err != nil {
		return storageTransitionID{}, fmt.Errorf("catalog: commit retired temporary delete intent: %w", err)
	}
	return id, nil
}

func (c *Catalog) deleteRetiredTemporaryStage(
	ctx context.Context,
	owner HandleOwnerID,
	stage StageID,
	name string,
) error {
	id, err := c.prepareRetiredTemporaryDelete(ctx, owner, stage, name)
	if err != nil {
		return err
	}
	if err := c.trip(storageDeleteAfterIntent); err != nil {
		return err
	}
	return c.settleStorageTransition(ctx, id, true)
}

func (c *Catalog) preparePublishedDelete(
	ctx context.Context,
	hash ContentHash,
) (storageTransitionID, error) {
	name := blobName(hash)
	id, err := newStorageTransitionID()
	if err != nil {
		return storageTransitionID{}, err
	}
	c.storage.mu.Lock()
	defer c.storage.mu.Unlock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return storageTransitionID{}, fmt.Errorf("catalog: begin published content delete: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var state uint8
	var size int64
	if err := tx.QueryRowContext(ctx, `
SELECT state, size
FROM storage_entries
WHERE name = ? AND kind = ? AND hash = ?`,
		name, storageEntryPublished, hash[:]).Scan(&state, &size); err != nil {
		return storageTransitionID{}, fmt.Errorf("catalog: inspect published content entry: %w", err)
	}
	if state != storageEntryStable {
		return storageTransitionID{}, fmt.Errorf("%w: published content entry is not stable", ErrIntegrity)
	}
	existing, found, err := c.readStorageTransitionForPublishedDelete(ctx, tx, hash)
	if err != nil {
		return storageTransitionID{}, err
	}
	if found {
		if existing.kind != storageTransitionDeletePublished ||
			existing.sourceName != name || existing.size != size ||
			existing.hash != hash || existing.quarantined {
			return storageTransitionID{}, fmt.Errorf(
				"%w: conflicting published delete transition", ErrIntegrity,
			)
		}
		if err := tx.Commit(); err != nil {
			return storageTransitionID{}, fmt.Errorf("catalog: confirm published delete intent: %w", err)
		}
		return existing.id, nil
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO storage_transitions(
    transition_id, kind, owner_id, source_name, hash, size, new_blob
) VALUES (?, ?, ?, ?, ?, ?, 0)`,
		id[:], storageTransitionDeletePublished, c.owner[:], name, hash[:], size); err != nil {
		return storageTransitionID{}, fmt.Errorf("catalog: journal published content delete: %w", mapConstraint(err))
	}
	if err := tx.Commit(); err != nil {
		return storageTransitionID{}, fmt.Errorf("catalog: commit published content delete intent: %w", err)
	}
	return id, nil
}

func (c *Catalog) deletePublishedBlob(ctx context.Context, hash ContentHash) (bool, error) {
	id, err := c.preparePublishedDelete(ctx, hash)
	if err != nil {
		return false, err
	}
	if err := c.trip(storageDeleteAfterIntent); err != nil {
		return false, err
	}
	if err := c.settleStorageTransition(ctx, id, false); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Catalog) settleStorageTransition(
	ctx context.Context,
	id storageTransitionID,
	retiredOnly bool,
) error {
	c.storage.mu.Lock()
	defer c.storage.mu.Unlock()
	return c.settleStorageTransitionLocked(ctx, id, retiredOnly, nil)
}

func (c *Catalog) settleStorageTransitionLocked(
	ctx context.Context,
	id storageTransitionID,
	retiredOnly bool,
	receipt *StorageQuarantineResolutionReceipt,
) error {
	if receipt == nil {
		stored, state, found, err := c.readStorageQuarantineResolution(ctx, c.db, id)
		if err != nil {
			return err
		}
		if found {
			if state == storageQuarantineResolutionSettled {
				var transitionExists bool
				if err := c.db.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM storage_transitions WHERE transition_id = ?
)`, id[:]).Scan(&transitionExists); err != nil {
					return fmt.Errorf("catalog: inspect settled storage transition: %w", err)
				}
				if transitionExists {
					return fmt.Errorf(
						"%w: settled storage receipt retained a transition", ErrIntegrity,
					)
				}
				return nil
			}
			if state != storageQuarantineResolutionPending {
				return fmt.Errorf(
					"%w: invalid storage quarantine resolution state", ErrIntegrity,
				)
			}
			receipt = &stored
		}
	}
	transition, err := c.readStorageTransition(ctx, c.db, id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			if receipt != nil {
				return fmt.Errorf(
					"%w: pending storage receipt lost its transition", ErrIntegrity,
				)
			}
			return nil
		}
		if errors.Is(err, ErrIntegrity) {
			return c.quarantineStorageTransition(ctx, id, err)
		}
		return err
	}
	if transition.quarantined {
		return fmt.Errorf("%w: storage transition quarantined: %s", ErrIntegrity, transition.reason)
	}
	if retiredOnly {
		var retired bool
		if err := c.db.QueryRowContext(ctx, `
SELECT retired FROM catalog_generations WHERE owner_id = ?`, transition.owner[:]).
			Scan(&retired); err != nil {
			return fmt.Errorf("catalog: inspect storage transition generation: %w", err)
		}
		if !retired {
			return fmt.Errorf("%w: storage transition generation is live", ErrInvalidTransition)
		}
	}
	switch transition.kind {
	case storageTransitionCreateTemporary, storageTransitionDeleteTemporary:
		err = c.settleTemporaryDeleteLocked(ctx, transition, receipt)
	case storageTransitionPublish:
		err = c.settlePublishRecoveryLocked(ctx, transition, receipt)
	case storageTransitionDeletePublished:
		err = c.settlePublishedDeleteLocked(ctx, transition, receipt)
	default:
		err = fmt.Errorf("%w: invalid storage transition kind", ErrIntegrity)
	}
	if err != nil && errors.Is(err, ErrIntegrity) {
		return c.quarantineStorageTransition(ctx, id, err)
	}
	return err
}

func (c *Catalog) readStorageTransition(
	ctx context.Context,
	query rowQuerier,
	id storageTransitionID,
) (storageTransition, error) {
	var transition storageTransition
	var rawID, rawOwner, rawStage, rawHash []byte
	err := query.QueryRowContext(ctx, `
SELECT transition_id, kind, owner_id, stage_id, source_name,
       target_name, hash, size, new_blob, quarantined, reason
FROM storage_transitions
WHERE transition_id = ?`, id[:]).Scan(
		&rawID, &transition.kind, &rawOwner, &rawStage, &transition.sourceName,
		&transition.targetName, &rawHash, &transition.size, &transition.newBlob,
		&transition.quarantined, &transition.reason,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return transition, fmt.Errorf("%w: storage transition is missing", ErrNotFound)
	}
	if err != nil {
		return transition, fmt.Errorf("catalog: read storage transition: %w", err)
	}
	if err := decodeStorageTransitionIdentity(
		&transition, rawID, rawOwner, rawStage, rawHash,
	); err != nil {
		return transition, err
	}
	if !validBlobStorageName(transition.sourceName) ||
		filepath.Base(transition.sourceName) != transition.sourceName ||
		transition.size < 0 {
		return transition, fmt.Errorf("%w: invalid storage transition payload", ErrIntegrity)
	}
	return transition, nil
}

func (c *Catalog) readStorageTransitionForStage(
	ctx context.Context,
	query rowQuerier,
	owner HandleOwnerID,
	stage StageID,
) (storageTransition, bool, error) {
	var raw []byte
	err := query.QueryRowContext(ctx, `
SELECT transition_id
FROM storage_transitions
WHERE owner_id = ? AND stage_id = ?`,
		owner[:], stage[:]).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return storageTransition{}, false, nil
	}
	if err != nil {
		return storageTransition{}, false, fmt.Errorf("catalog: inspect stage storage transition: %w", err)
	}
	if len(raw) != len(storageTransitionID{}) {
		return storageTransition{}, false, fmt.Errorf("%w: invalid stage transition identity", ErrIntegrity)
	}
	var id storageTransitionID
	copy(id[:], raw)
	transition, err := c.readStorageTransition(ctx, query, id)
	return transition, err == nil, err
}

func (c *Catalog) readStorageTransitionForPublishedDelete(
	ctx context.Context,
	query rowQuerier,
	hash ContentHash,
) (storageTransition, bool, error) {
	var raw []byte
	err := query.QueryRowContext(ctx, `
SELECT transition_id
FROM storage_transitions
WHERE kind = ? AND hash = ?`,
		storageTransitionDeletePublished, hash[:]).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return storageTransition{}, false, nil
	}
	if err != nil {
		return storageTransition{}, false, fmt.Errorf("catalog: inspect published delete transition: %w", err)
	}
	if len(raw) != len(storageTransitionID{}) {
		return storageTransition{}, false, fmt.Errorf("%w: invalid published delete identity", ErrIntegrity)
	}
	var id storageTransitionID
	copy(id[:], raw)
	transition, err := c.readStorageTransition(ctx, query, id)
	return transition, err == nil, err
}

func (c *Catalog) settleTemporaryDeleteLocked(
	ctx context.Context,
	transition storageTransition,
	receipt *StorageQuarantineResolutionReceipt,
) error {
	if transition.stage == (StageID{}) {
		return fmt.Errorf("%w: temporary delete has no stage", ErrIntegrity)
	}
	path := filepath.Join(c.blobDir, transition.sourceName)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("catalog: remove temporary content: %w", err)
	}
	if err := c.trip(storageDeleteAfterUnlink); err != nil {
		return err
	}
	if err := syncStorageDirectory(c.blobDir); err != nil {
		return fmt.Errorf("catalog: sync temporary content delete: %w", err)
	}
	if err := c.trip(storageDeleteAfterSync); err != nil {
		return err
	}
	return c.finishTemporaryDeleteLocked(ctx, transition, receipt)
}

func (c *Catalog) finishTemporaryDeleteLocked(
	ctx context.Context,
	transition storageTransition,
	receipt *StorageQuarantineResolutionReceipt,
) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin temporary delete settlement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	temporary, published, version, err := readStorageAccounting(ctx, tx)
	if err != nil {
		return err
	}
	if transition.size > temporary {
		return fmt.Errorf("%w: temporary delete accounting underflow", ErrIntegrity)
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM content_stages
WHERE stage_id = ? AND owner_id = ?`,
		transition.stage[:], transition.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: retire deleted temporary stage: %w", err)
	}
	if err := requireOneRow(result, "deleted temporary stage"); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `
DELETE FROM storage_entries
WHERE name = ? AND kind = ? AND stage_id = ? AND owner_id = ?`,
		transition.sourceName, storageEntryTemporary,
		transition.stage[:], transition.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: retire deleted temporary entry: %w", err)
	}
	if err := requireOneRow(result, "deleted temporary entry"); err != nil {
		return err
	}
	if err := deleteStorageTransition(ctx, tx, transition, receipt); err != nil {
		return err
	}
	accounting, err := tx.ExecContext(ctx, `
UPDATE storage_accounting
SET temporary_bytes = temporary_bytes - ?, version = version + 1
WHERE singleton = 1 AND version = ?`,
		transition.size, version)
	if err != nil {
		return fmt.Errorf("catalog: settle temporary delete accounting: %w", err)
	}
	if err := requireOneRow(accounting, "temporary delete accounting"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit temporary delete settlement: %w", err)
	}
	c.storage.usage = storageUsage{
		temporary: temporary - transition.size,
		published: published,
	}
	return nil
}

func (c *Catalog) settlePublishRecoveryLocked(
	ctx context.Context,
	transition storageTransition,
	receipt *StorageQuarantineResolutionReceipt,
) error {
	if transition.stage == (StageID{}) || transition.hash == (ContentHash{}) ||
		!validBlobStorageName(transition.targetName) ||
		filepath.Base(transition.targetName) != transition.targetName {
		return fmt.Errorf("%w: invalid publish recovery transition", ErrIntegrity)
	}
	ref := ContentRef{Stage: transition.stage, Hash: transition.hash, Size: transition.size}
	target := filepath.Join(c.blobDir, transition.targetName)
	if err := verifyBlobPath(target, ref); err != nil {
		if errors.Is(err, os.ErrNotExist) && transition.newBlob {
			return c.abortPublishRecoveryLocked(ctx, transition, receipt)
		}
		return c.quarantineStorageTransition(ctx, transition.id, err)
	}
	source := filepath.Join(c.blobDir, transition.sourceName)
	if err := os.Remove(source); err != nil && !errors.Is(err, os.ErrNotExist) {
		return c.quarantineStorageTransition(
			ctx, transition.id, fmt.Errorf("catalog: remove recovered publish source: %w", err),
		)
	}
	if err := c.trip(storageDeleteAfterUnlink); err != nil {
		return err
	}
	if err := syncStorageDirectory(c.blobDir); err != nil {
		return fmt.Errorf("catalog: sync recovered publish: %w", err)
	}
	if err := c.trip(storageDeleteAfterSync); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin recovered publish settlement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE content_stages
SET hash = ?, size = ?, published = 1
WHERE stage_id = ? AND owner_id = ? AND published = 0`,
		transition.hash[:], transition.size, transition.stage[:], transition.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: settle recovered content stage: %w", err)
	}
	if err := requireOneRow(result, "recovered content stage"); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `
DELETE FROM storage_entries
WHERE name = ? AND kind = ? AND state = ? AND size = ?
  AND stage_id = ? AND owner_id = ?`,
		transition.sourceName, storageEntryTemporary, storageEntryStable, transition.size,
		transition.stage[:], transition.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: retire recovered temporary entry: %w", err)
	}
	if err := requireOneRow(result, "recovered temporary entry"); err != nil {
		return err
	}
	if transition.newBlob {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO storage_entries(name, kind, state, size, hash)
VALUES (?, ?, ?, ?, ?)`,
			transition.targetName, storageEntryPublished, storageEntryStable,
			transition.size, transition.hash[:]); err != nil {
			return fmt.Errorf("catalog: record recovered published entry: %w", mapConstraint(err))
		}
	}
	if err := deleteStorageTransition(ctx, tx, transition, receipt); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit recovered publish settlement: %w", err)
	}
	return nil
}

func (c *Catalog) abortPublishRecoveryLocked(
	ctx context.Context,
	transition storageTransition,
	receipt *StorageQuarantineResolutionReceipt,
) error {
	source := filepath.Join(c.blobDir, transition.sourceName)
	if err := os.Remove(source); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("catalog: remove incomplete publish source: %w", err)
	}
	if err := c.trip(storageDeleteAfterUnlink); err != nil {
		return err
	}
	if err := syncStorageDirectory(c.blobDir); err != nil {
		return fmt.Errorf("catalog: sync incomplete publish abort: %w", err)
	}
	if err := c.trip(storageDeleteAfterSync); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin incomplete publish abort: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	temporary, published, version, err := readStorageAccounting(ctx, tx)
	if err != nil {
		return err
	}
	publishedDelta := int64(0)
	if transition.newBlob {
		publishedDelta = transition.size
	}
	if publishedDelta > published {
		return fmt.Errorf("%w: incomplete publish accounting underflow", ErrIntegrity)
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM content_stages WHERE stage_id = ? AND owner_id = ?`,
		transition.stage[:], transition.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: retire incomplete publish stage: %w", err)
	}
	if err := requireOneRow(result, "incomplete publish stage"); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `
DELETE FROM storage_entries
WHERE name = ? AND kind = ? AND stage_id = ? AND owner_id = ?`,
		transition.sourceName, storageEntryTemporary,
		transition.stage[:], transition.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: retire incomplete publish entry: %w", err)
	}
	if err := requireOneRow(result, "incomplete publish entry"); err != nil {
		return err
	}
	if err := deleteStorageTransition(ctx, tx, transition, receipt); err != nil {
		return err
	}
	if publishedDelta != 0 {
		accounting, err := tx.ExecContext(ctx, `
UPDATE storage_accounting
SET published_bytes = published_bytes - ?, version = version + 1
WHERE singleton = 1 AND version = ?`, publishedDelta, version)
		if err != nil {
			return fmt.Errorf("catalog: settle incomplete publish accounting: %w", err)
		}
		if err := requireOneRow(accounting, "incomplete publish accounting"); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit incomplete publish abort: %w", err)
	}
	c.storage.usage = storageUsage{
		temporary: temporary,
		published: published - publishedDelta,
	}
	return nil
}

func (c *Catalog) settlePublishedDeleteLocked(
	ctx context.Context,
	transition storageTransition,
	receipt *StorageQuarantineResolutionReceipt,
) error {
	if transition.hash == (ContentHash{}) {
		return fmt.Errorf("%w: published delete has no hash", ErrIntegrity)
	}
	referenced, err := c.blobEntryReferenced(ctx, transition.sourceName)
	if err != nil {
		return err
	}
	if referenced {
		ref := ContentRef{Hash: transition.hash, Size: transition.size}
		if err := verifyBlobPath(
			filepath.Join(c.blobDir, transition.sourceName), ref,
		); err != nil {
			return c.quarantineStorageTransition(ctx, transition.id, err)
		}
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("catalog: begin published delete cancellation: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		if err := deleteStorageTransition(ctx, tx, transition, receipt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"DELETE FROM blob_gc_candidates WHERE hash = ?", transition.hash[:]); err != nil {
			return fmt.Errorf("catalog: cancel referenced blob candidate: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("catalog: commit published delete cancellation: %w", err)
		}
		return nil
	}
	path := filepath.Join(c.blobDir, transition.sourceName)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("catalog: remove published content: %w", err)
	}
	if err := c.trip(storageDeleteAfterUnlink); err != nil {
		return err
	}
	if err := syncStorageDirectory(c.blobDir); err != nil {
		return fmt.Errorf("catalog: sync published content delete: %w", err)
	}
	if err := c.trip(storageDeleteAfterSync); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin published delete settlement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	temporary, published, version, err := readStorageAccounting(ctx, tx)
	if err != nil {
		return err
	}
	if transition.size > published {
		return fmt.Errorf("%w: published delete accounting underflow", ErrIntegrity)
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM storage_entries
WHERE name = ? AND kind = ? AND state = ? AND size = ? AND hash = ?`,
		transition.sourceName, storageEntryPublished, storageEntryStable,
		transition.size, transition.hash[:])
	if err != nil {
		return fmt.Errorf("catalog: retire published storage entry: %w", err)
	}
	if err := requireOneRow(result, "published storage entry"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		"DELETE FROM blob_gc_candidates WHERE hash = ?", transition.hash[:]); err != nil {
		return fmt.Errorf("catalog: retire published blob candidate: %w", err)
	}
	if err := deleteStorageTransition(ctx, tx, transition, receipt); err != nil {
		return err
	}
	accounting, err := tx.ExecContext(ctx, `
UPDATE storage_accounting
SET published_bytes = published_bytes - ?, version = version + 1
WHERE singleton = 1 AND version = ?`,
		transition.size, version)
	if err != nil {
		return fmt.Errorf("catalog: settle published delete accounting: %w", err)
	}
	if err := requireOneRow(accounting, "published delete accounting"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit published delete settlement: %w", err)
	}
	c.storage.usage = storageUsage{
		temporary: temporary,
		published: published - transition.size,
	}
	return nil
}

func readStorageAccounting(
	ctx context.Context,
	query rowQuerier,
) (temporary int64, published int64, version uint64, err error) {
	err = query.QueryRowContext(ctx, `
SELECT temporary_bytes, published_bytes, version
FROM storage_accounting WHERE singleton = 1`).Scan(&temporary, &published, &version)
	if err != nil {
		err = fmt.Errorf("catalog: read storage accounting: %w", err)
	}
	return temporary, published, version, err
}

func deleteStorageTransition(
	ctx context.Context,
	tx *sql.Tx,
	transition storageTransition,
	receipt *StorageQuarantineResolutionReceipt,
) error {
	result, err := tx.ExecContext(ctx, `
DELETE FROM storage_transitions
WHERE transition_id = ? AND kind = ? AND owner_id = ? AND quarantined = 0`,
		transition.id[:], transition.kind, transition.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: retire storage transition: %w", err)
	}
	if err := requireOneRow(result, "storage transition"); err != nil {
		return err
	}
	if receipt == nil {
		return nil
	}
	result, err = tx.ExecContext(ctx, `
UPDATE storage_quarantine_resolutions
SET state = ?
WHERE transition_id = ? AND token = ? AND resolution = ?
  AND state = ? AND outcome_digest = ?`,
		storageQuarantineResolutionSettled,
		receipt.ID[:], receipt.Token[:], receipt.Resolution,
		storageQuarantineResolutionPending, receipt.OutcomeDigest[:])
	if err != nil {
		return fmt.Errorf("catalog: settle storage quarantine receipt: %w", err)
	}
	return requireOneRow(result, "storage quarantine receipt")
}
