package catalog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

const (
	storageReservationWindow int64 = 1 << 20

	storageEntryTemporary uint8 = 1
	storageEntryPublished uint8 = 2

	storageEntryPending uint8 = 1
	storageEntryStable  uint8 = 2

	storageTransitionCreateTemporary StorageTransitionKind = 1
	storageTransitionPublish         StorageTransitionKind = 2
	storageTransitionDeleteTemporary StorageTransitionKind = 3
	storageTransitionDeletePublished StorageTransitionKind = 4
)

// StorageTransitionID identifies one durable filesystem/accounting transition.
type StorageTransitionID [16]byte

type storageTransitionID = StorageTransitionID

// StorageTransitionKind identifies the physical transition awaiting settlement.
type StorageTransitionKind uint8

type temporaryReservation struct {
	catalog    *Catalog
	ctx        context.Context
	transition storageTransitionID
	stage      StageID
	name       string
	reserved   int64
	written    int64
	active     bool
}

type quotaWriter struct {
	destination io.Writer
	reservation *temporaryReservation
}

type storageTransition struct {
	id          storageTransitionID
	kind        StorageTransitionKind
	owner       HandleOwnerID
	stage       StageID
	sourceName  string
	targetName  string
	hash        ContentHash
	size        int64
	newBlob     bool
	quarantined bool
	reason      string
}

func newStorageTransitionID() (storageTransitionID, error) {
	var id storageTransitionID
	if _, err := io.ReadFull(rand.Reader, id[:]); err != nil {
		return id, fmt.Errorf("catalog: allocate storage transition: %w", err)
	}
	return id, nil
}

func (c *Catalog) loadStorageAccounting(ctx context.Context) error {
	var usage storageUsage
	if err := c.db.QueryRowContext(ctx, `
SELECT temporary_bytes, published_bytes
FROM storage_accounting
WHERE singleton = 1`).Scan(&usage.temporary, &usage.published); err != nil {
		return fmt.Errorf("catalog: load storage accounting: %w", err)
	}
	if usage.temporary < 0 || usage.published < 0 ||
		usage.temporary > math.MaxInt64-usage.published {
		return fmt.Errorf("%w: invalid durable storage accounting", ErrIntegrity)
	}
	c.storage.mu.Lock()
	c.storage.usage = usage
	c.storage.mu.Unlock()
	return nil
}

func (c *Catalog) beginTemporaryContent(
	ctx context.Context,
	stage StageID,
	name string,
) (*temporaryReservation, error) {
	if stage == (StageID{}) || !validBlobStorageName(name) ||
		!strings.HasPrefix(name, ".stage-") || filepath.Base(name) != name {
		return nil, fmt.Errorf("%w: invalid temporary content identity", ErrInvalidObject)
	}
	transition, err := newStorageTransitionID()
	if err != nil {
		return nil, err
	}
	c.storage.mu.Lock()
	defer c.storage.mu.Unlock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("catalog: begin temporary content intent: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO content_stages(stage_id, owner_id, temp_name, published)
VALUES (?, ?, ?, 0)`, stage[:], c.owner[:], name); err != nil {
		return nil, fmt.Errorf("catalog: claim staged content: %w", mapConstraint(err))
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO storage_entries(name, kind, state, size, stage_id, owner_id)
VALUES (?, ?, ?, 0, ?, ?)`,
		name, storageEntryTemporary, storageEntryPending, stage[:], c.owner[:]); err != nil {
		return nil, fmt.Errorf("catalog: record temporary content intent: %w", mapConstraint(err))
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO storage_transitions(
    transition_id, kind, owner_id, stage_id, source_name, size, new_blob
) VALUES (?, ?, ?, ?, ?, 0, 0)`,
		transition[:], storageTransitionCreateTemporary, c.owner[:], stage[:], name); err != nil {
		return nil, fmt.Errorf("catalog: journal temporary content intent: %w", mapConstraint(err))
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("catalog: commit temporary content intent: %w", err)
	}
	return &temporaryReservation{
		catalog: c, ctx: ctx, transition: transition, stage: stage, name: name, active: true,
	}, nil
}

func (r *temporaryReservation) writer(destination io.Writer) *quotaWriter {
	return &quotaWriter{destination: destination, reservation: r}
}

func (w *quotaWriter) Write(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	r := w.reservation
	if r == nil || !r.active {
		return 0, errors.New("catalog: content reservation is settled")
	}
	needed := int64(len(buffer))
	if available := r.reserved - r.written; available < needed {
		if err := r.reserve(needed - available); err != nil {
			return 0, err
		}
	}
	written, err := w.destination.Write(buffer)
	if written < 0 || written > len(buffer) {
		return 0, errors.New("catalog: invalid staged content write count")
	}
	r.written += int64(written)
	return written, err
}

func (r *temporaryReservation) reserve(minimum int64) error {
	if minimum <= 0 || !r.active {
		return fmt.Errorf("%w: invalid temporary reservation growth", ErrInvalidTransition)
	}
	c := r.catalog
	c.storage.mu.Lock()
	defer c.storage.mu.Unlock()
	tx, err := c.db.BeginTx(r.ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin temporary reservation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var usage storageUsage
	var version uint64
	if err := tx.QueryRowContext(r.ctx, `
SELECT temporary_bytes, published_bytes, version
FROM storage_accounting WHERE singleton = 1`).
		Scan(&usage.temporary, &usage.published, &version); err != nil {
		return fmt.Errorf("catalog: read temporary reservation accounting: %w", err)
	}
	var reserved int64
	var kind StorageTransitionKind
	var quarantined bool
	if err := tx.QueryRowContext(r.ctx, `
SELECT kind, size, quarantined
FROM storage_transitions
WHERE transition_id = ? AND owner_id = ? AND stage_id = ?`,
		r.transition[:], c.owner[:], r.stage[:]).Scan(&kind, &reserved, &quarantined); err != nil {
		return fmt.Errorf("catalog: read temporary reservation intent: %w", err)
	}
	if kind != storageTransitionCreateTemporary || quarantined || reserved != r.reserved {
		return fmt.Errorf("%w: temporary reservation intent changed", ErrIntegrity)
	}
	available := min(
		c.storage.limits.ObjectContentBytes-reserved,
		c.storage.limits.TemporaryContentBytes-usage.temporary,
		c.storage.limits.TotalContentBytes-usage.temporary-usage.published,
	)
	if available < minimum {
		return temporaryQuotaError(c.storage.limits, usage, reserved, minimum)
	}
	window := min(storageReservationWindow, available)
	if window < minimum {
		window = minimum
	}
	accountingResult, err := tx.ExecContext(r.ctx, `
UPDATE storage_accounting
SET temporary_bytes = temporary_bytes + ?, version = version + 1
WHERE singleton = 1 AND version = ?`,
		window, version)
	if err != nil {
		return fmt.Errorf("catalog: reserve temporary accounting: %w", err)
	}
	if err := requireOneRow(accountingResult, "temporary reservation accounting"); err != nil {
		return err
	}
	result, err := tx.ExecContext(r.ctx, `
UPDATE storage_transitions
SET size = size + ?
WHERE transition_id = ? AND kind = ? AND size = ? AND quarantined = 0`,
		window, r.transition[:], storageTransitionCreateTemporary, reserved)
	if err != nil {
		return fmt.Errorf("catalog: extend temporary reservation intent: %w", err)
	}
	if err := requireOneRow(result, "temporary reservation intent"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit temporary reservation: %w", err)
	}
	r.reserved += window
	c.storage.usage.temporary = usage.temporary + window
	c.storage.usage.published = usage.published
	return nil
}

func temporaryQuotaError(
	limits StorageLimits,
	usage storageUsage,
	objectBytes int64,
	requested int64,
) error {
	checks := []struct {
		resource string
		limit    int64
		used     int64
	}{
		{"object content", limits.ObjectContentBytes, objectBytes},
		{"temporary content", limits.TemporaryContentBytes, usage.temporary},
		{"total content", limits.TotalContentBytes, usage.temporary + usage.published},
	}
	for _, check := range checks {
		if requested > check.limit-check.used {
			return &StorageQuotaError{
				Resource: check.resource, Limit: check.limit,
				Used: check.used, Requested: requested,
			}
		}
	}
	return fmt.Errorf("%w: temporary reservation capacity changed", ErrIntegrity)
}

func (r *temporaryReservation) finalizeTemporary(ctx context.Context) error {
	if !r.active || r.written < 0 || r.written > r.reserved {
		return fmt.Errorf("%w: invalid temporary reservation settlement", ErrIntegrity)
	}
	c := r.catalog
	c.storage.mu.Lock()
	defer c.storage.mu.Unlock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin temporary content settlement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var temporary, published int64
	var version uint64
	if err := tx.QueryRowContext(ctx, `
SELECT temporary_bytes, published_bytes, version
FROM storage_accounting WHERE singleton = 1`).
		Scan(&temporary, &published, &version); err != nil {
		return fmt.Errorf("catalog: read temporary settlement accounting: %w", err)
	}
	unused := r.reserved - r.written
	if unused > temporary {
		return fmt.Errorf("%w: temporary reservation accounting underflow", ErrIntegrity)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE storage_entries
SET state = ?, size = ?
WHERE name = ? AND kind = ? AND state = ? AND size = 0
  AND stage_id = ? AND owner_id = ?`,
		storageEntryStable, r.written, r.name, storageEntryTemporary,
		storageEntryPending, r.stage[:], c.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: settle temporary storage entry: %w", err)
	}
	if err := requireOneRow(result, "temporary storage entry"); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `
DELETE FROM storage_transitions
WHERE transition_id = ? AND kind = ? AND owner_id = ? AND stage_id = ?
  AND source_name = ? AND size = ? AND quarantined = 0`,
		r.transition[:], storageTransitionCreateTemporary, c.owner[:],
		r.stage[:], r.name, r.reserved)
	if err != nil {
		return fmt.Errorf("catalog: settle temporary storage transition: %w", err)
	}
	if err := requireOneRow(result, "temporary storage transition"); err != nil {
		return err
	}
	accountingResult, err := tx.ExecContext(ctx, `
UPDATE storage_accounting
SET temporary_bytes = temporary_bytes - ?, version = version + 1
WHERE singleton = 1 AND version = ?`,
		unused, version)
	if err != nil {
		return fmt.Errorf("catalog: reconcile temporary accounting: %w", err)
	}
	if err := requireOneRow(accountingResult, "temporary settlement accounting"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit temporary content settlement: %w", err)
	}
	r.reserved = r.written
	c.storage.usage = storageUsage{temporary: temporary - unused, published: published}
	return nil
}

func (c *Catalog) beginContentPublish(
	ctx context.Context,
	r *temporaryReservation,
	ref ContentRef,
	mutation *MutationID,
) (storageTransitionID, bool, error) {
	transition, err := newStorageTransitionID()
	if err != nil {
		return transition, false, err
	}
	targetName := blobName(ref.Hash)
	c.storage.mu.Lock()
	defer c.storage.mu.Unlock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return transition, false, fmt.Errorf("catalog: begin content publish intent: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var temporary, published int64
	var version uint64
	if err := tx.QueryRowContext(ctx, `
SELECT temporary_bytes, published_bytes, version
FROM storage_accounting WHERE singleton = 1`).
		Scan(&temporary, &published, &version); err != nil {
		return transition, false, fmt.Errorf("catalog: read publish accounting: %w", err)
	}
	if ref.Size != r.reserved || ref.Size > temporary {
		return transition, false, fmt.Errorf("%w: content publish reservation changed", ErrIntegrity)
	}
	var existingSize int64
	var existingState uint8
	existingErr := tx.QueryRowContext(ctx, `
SELECT state, size
FROM storage_entries
WHERE name = ? AND kind = ? AND hash = ?`,
		targetName, storageEntryPublished, ref.Hash[:]).Scan(&existingState, &existingSize)
	newBlob := errors.Is(existingErr, sql.ErrNoRows)
	if existingErr != nil && !newBlob {
		return transition, false, fmt.Errorf("catalog: inspect durable published entry: %w", existingErr)
	}
	if !newBlob && (existingState != storageEntryStable || existingSize != ref.Size) {
		return transition, false, fmt.Errorf("%w: published content ledger changed", ErrIntegrity)
	}
	if newBlob && ref.Size > c.storage.limits.PublishedContentBytes-published {
		return transition, false, &StorageQuotaError{
			Resource: "published content", Limit: c.storage.limits.PublishedContentBytes,
			Used: published, Requested: ref.Size,
		}
	}
	publishedDelta := int64(0)
	if newBlob {
		publishedDelta = ref.Size
	}
	accountingResult, err := tx.ExecContext(ctx, `
UPDATE storage_accounting
SET temporary_bytes = temporary_bytes - ?,
    published_bytes = published_bytes + ?,
    version = version + 1
WHERE singleton = 1 AND version = ?`,
		ref.Size, publishedDelta, version)
	if err != nil {
		return transition, false, fmt.Errorf("catalog: reserve publish accounting: %w", err)
	}
	if err := requireOneRow(accountingResult, "content publish accounting"); err != nil {
		return transition, false, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO storage_transitions(
    transition_id, kind, owner_id, stage_id, source_name,
    target_name, hash, size, new_blob
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		transition[:], storageTransitionPublish, c.owner[:], ref.Stage[:],
		r.name, targetName, ref.Hash[:], ref.Size, newBlob); err != nil {
		return transition, false, fmt.Errorf("catalog: journal content publish: %w", mapConstraint(err))
	}
	if err := validatePublishStageOwnership(ctx, tx, c.owner, ref.Stage, mutation); err != nil {
		return transition, false, err
	}
	if err := tx.Commit(); err != nil {
		return transition, false, fmt.Errorf("catalog: commit content publish intent: %w", err)
	}
	c.storage.usage = storageUsage{
		temporary: temporary - ref.Size,
		published: published + publishedDelta,
	}
	return transition, newBlob, nil
}

func validatePublishStageOwnership(
	ctx context.Context,
	tx *sql.Tx,
	owner HandleOwnerID,
	stage StageID,
	mutation *MutationID,
) error {
	statement := `
SELECT COUNT(*)
FROM content_stages
WHERE stage_id = ? AND published = 0`
	args := []any{stage[:]}
	if mutation == nil {
		statement += " AND owner_id = ? AND mutation_id IS NULL"
		args = append(args, owner[:])
	} else {
		statement += " AND mutation_id = ?"
		args = append(args, mutation[:])
	}
	var count int
	if err := tx.QueryRowContext(ctx, statement, args...).Scan(&count); err != nil {
		return fmt.Errorf("catalog: inspect publish stage ownership: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("%w: content stage ownership lost", ErrInvalidTransition)
	}
	return nil
}

func (c *Catalog) finishContentPublish(
	ctx context.Context,
	r *temporaryReservation,
	transition storageTransitionID,
	ref ContentRef,
	mutation *MutationID,
	newBlob bool,
) error {
	c.storage.mu.Lock()
	defer c.storage.mu.Unlock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin content publish settlement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	statement := `
UPDATE content_stages SET hash = ?, size = ?, published = 1
WHERE stage_id = ? AND published = 0`
	args := []any{ref.Hash[:], ref.Size, ref.Stage[:]}
	if mutation == nil {
		statement += " AND owner_id = ? AND mutation_id IS NULL"
		args = append(args, c.owner[:])
	} else {
		statement += " AND mutation_id = ?"
		args = append(args, mutation[:])
	}
	result, err := tx.ExecContext(ctx, statement, args...)
	if err != nil {
		return fmt.Errorf("catalog: publish content stage: %w", err)
	}
	if err := requireOneRow(result, "published content stage"); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `
DELETE FROM storage_entries
WHERE name = ? AND kind = ? AND state = ? AND size = ?
  AND stage_id = ? AND owner_id = ?`,
		r.name, storageEntryTemporary, storageEntryStable, ref.Size,
		ref.Stage[:], c.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: retire published temporary entry: %w", err)
	}
	if err := requireOneRow(result, "published temporary entry"); err != nil {
		return err
	}
	if newBlob {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO storage_entries(name, kind, state, size, hash)
VALUES (?, ?, ?, ?, ?)`,
			blobName(ref.Hash), storageEntryPublished, storageEntryStable,
			ref.Size, ref.Hash[:]); err != nil {
			return fmt.Errorf("catalog: record published content entry: %w", mapConstraint(err))
		}
	}
	result, err = tx.ExecContext(ctx, `
DELETE FROM storage_transitions
WHERE transition_id = ? AND kind = ? AND owner_id = ? AND stage_id = ?
  AND source_name = ? AND target_name = ? AND hash = ? AND size = ?
  AND new_blob = ? AND quarantined = 0`,
		transition[:], storageTransitionPublish, c.owner[:], ref.Stage[:],
		r.name, blobName(ref.Hash), ref.Hash[:], ref.Size, newBlob)
	if err != nil {
		return fmt.Errorf("catalog: settle content publish transition: %w", err)
	}
	if err := requireOneRow(result, "content publish transition"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit content publish settlement: %w", err)
	}
	r.active = false
	return nil
}

func (c *Catalog) abortTemporaryReservation(
	ctx context.Context,
	r *temporaryReservation,
) error {
	if r == nil || !r.active {
		return nil
	}
	transition, err := c.prepareTemporaryAbort(ctx, r)
	if err != nil {
		return err
	}
	if err := c.trip(storageDeleteAfterIntent); err != nil {
		return err
	}
	if err := c.settleStorageTransition(ctx, transition, false); err != nil {
		return err
	}
	r.active = false
	return nil
}

func (c *Catalog) prepareTemporaryAbort(
	ctx context.Context,
	r *temporaryReservation,
) (storageTransitionID, error) {
	c.storage.mu.Lock()
	defer c.storage.mu.Unlock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return storageTransitionID{}, fmt.Errorf("catalog: begin temporary content abort: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var transition storageTransition
	var rawID, rawOwner, rawStage, rawHash []byte
	err = tx.QueryRowContext(ctx, `
SELECT transition_id, kind, owner_id, stage_id, source_name,
       target_name, hash, size, new_blob, quarantined, reason
FROM storage_transitions
WHERE stage_id = ? AND owner_id = ?
ORDER BY transition_id
LIMIT 1`, r.stage[:], c.owner[:]).Scan(
		&rawID, &transition.kind, &rawOwner, &rawStage, &transition.sourceName,
		&transition.targetName, &rawHash, &transition.size, &transition.newBlob,
		&transition.quarantined, &transition.reason,
	)
	if errors.Is(err, sql.ErrNoRows) {
		var state uint8
		if err := tx.QueryRowContext(ctx, `
SELECT state, size
FROM storage_entries
WHERE name = ? AND kind = ? AND stage_id = ? AND owner_id = ?`,
			r.name, storageEntryTemporary, r.stage[:], c.owner[:]).
			Scan(&state, &transition.size); err != nil {
			return storageTransitionID{}, fmt.Errorf("catalog: read temporary entry for abort: %w", err)
		}
		if state != storageEntryStable {
			return storageTransitionID{}, fmt.Errorf("%w: temporary entry has no recovery transition", ErrIntegrity)
		}
		transition.id, err = newStorageTransitionID()
		if err != nil {
			return storageTransitionID{}, err
		}
		transition.kind = storageTransitionDeleteTemporary
		transition.owner = c.owner
		transition.stage = r.stage
		transition.sourceName = r.name
		if _, err := tx.ExecContext(ctx, `
INSERT INTO storage_transitions(
    transition_id, kind, owner_id, stage_id, source_name, size, new_blob
) VALUES (?, ?, ?, ?, ?, ?, 0)`,
			transition.id[:], transition.kind, c.owner[:], r.stage[:],
			r.name, transition.size); err != nil {
			return storageTransitionID{}, fmt.Errorf("catalog: journal temporary content abort: %w", mapConstraint(err))
		}
	} else if err != nil {
		return storageTransitionID{}, fmt.Errorf("catalog: read temporary content abort transition: %w", err)
	} else {
		if err := decodeStorageTransitionIdentity(
			&transition, rawID, rawOwner, rawStage, rawHash,
		); err != nil {
			return storageTransitionID{}, err
		}
		if transition.quarantined {
			return storageTransitionID{}, fmt.Errorf(
				"%w: storage transition quarantined: %s", ErrIntegrity, transition.reason,
			)
		}
	}
	if err := tx.Commit(); err != nil {
		return storageTransitionID{}, fmt.Errorf("catalog: commit temporary content abort intent: %w", err)
	}
	return transition.id, nil
}

func decodeStorageTransitionIdentity(
	transition *storageTransition,
	rawID, rawOwner, rawStage, rawHash []byte,
) error {
	if len(rawID) != len(storageTransitionID{}) || len(rawOwner) != len(HandleOwnerID{}) {
		return fmt.Errorf("%w: invalid storage transition identity", ErrIntegrity)
	}
	copy(transition.id[:], rawID)
	copy(transition.owner[:], rawOwner)
	if rawStage != nil {
		if len(rawStage) != len(StageID{}) {
			return fmt.Errorf("%w: invalid storage transition stage", ErrIntegrity)
		}
		copy(transition.stage[:], rawStage)
	}
	if rawHash != nil {
		if len(rawHash) != len(ContentHash{}) {
			return fmt.Errorf("%w: invalid storage transition hash", ErrIntegrity)
		}
		copy(transition.hash[:], rawHash)
	}
	return nil
}

func (c *Catalog) quarantineStorageTransition(
	ctx context.Context,
	transition storageTransitionID,
	cause error,
) error {
	reason := storageQuarantineReason(cause)
	result, err := c.db.ExecContext(context.WithoutCancel(ctx), `
UPDATE storage_transitions
SET quarantined = 1, reason = ?
WHERE transition_id = ? AND quarantined = 0`,
		reason, transition[:])
	if err != nil {
		return errors.Join(cause, fmt.Errorf("catalog: quarantine storage transition: %w", err))
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return errors.Join(cause, fmt.Errorf("catalog: inspect storage quarantine: %w", err))
	}
	if rows == 0 {
		var quarantined bool
		if err := c.db.QueryRowContext(
			context.WithoutCancel(ctx),
			"SELECT quarantined FROM storage_transitions WHERE transition_id = ?",
			transition[:],
		).Scan(&quarantined); err != nil || !quarantined {
			return errors.Join(cause, fmt.Errorf("%w: storage transition changed before quarantine", ErrIntegrity))
		}
	}
	return errors.Join(ErrIntegrity, cause)
}

func syncStorageDirectory(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := handle.Sync()
	closeErr := handle.Close()
	return errors.Join(syncErr, closeErr)
}
