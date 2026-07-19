package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const storageResolutionAfterClaim = "storage.resolution_after_claim"

const (
	storageQuarantineResolutionPending uint8 = 1
	storageQuarantineResolutionSettled uint8 = 2
)

// StorageQuarantinePageByteLimit bounds one inspection response.
const StorageQuarantinePageByteLimit = 1 << 20

// StorageQuarantineToken fences resolution to the exact inspected row.
type StorageQuarantineToken [32]byte

// StorageQuarantine describes one transition withheld from automatic recovery.
type StorageQuarantine struct {
	ID         StorageTransitionID
	Kind       StorageTransitionKind
	Owner      HandleOwnerID
	Stage      StageID
	SourceName string
	TargetName string
	Hash       ContentHash
	Size       int64
	NewBlob    bool
	Reason     string
	Token      StorageQuarantineToken
}

// StorageQuarantinePage is one bounded, stable-order inspection page.
type StorageQuarantinePage struct {
	Entries []StorageQuarantine
	More    bool
}

// StorageQuarantineResolutionReceipt is the durable exact outcome of one
// operator resolution. It remains replayable until explicitly acknowledged.
type StorageQuarantineResolutionReceipt struct {
	ID            StorageTransitionID
	Token         StorageQuarantineToken
	Resolution    StorageQuarantineResolution
	OutcomeDigest [32]byte
}

// StorageQuarantineResolution is an explicit operator decision.
type StorageQuarantineResolution uint8

const (
	// StorageQuarantineRetry verifies the inspected token and returns the
	// transition to ordinary recovery.
	StorageQuarantineRetry StorageQuarantineResolution = 1
	// StorageQuarantineDiscard verifies the inspected token and durably
	// abandons unpublished work or completes an already-journaled delete.
	StorageQuarantineDiscard StorageQuarantineResolution = 2
)

// Validate checks one quarantined transition returned for inspection.
func (q StorageQuarantine) Validate() error {
	if q.ID == (StorageTransitionID{}) || q.Owner == (HandleOwnerID{}) ||
		q.Token == (StorageQuarantineToken{}) ||
		!validBlobStorageName(q.SourceName) ||
		filepath.Base(q.SourceName) != q.SourceName ||
		q.Size < 0 || len(q.Reason) == 0 || len(q.Reason) > 4096 {
		return fmt.Errorf("%w: invalid storage quarantine", ErrInvalidObject)
	}
	switch q.Kind {
	case storageTransitionCreateTemporary, storageTransitionDeleteTemporary:
		if q.Stage == (StageID{}) || q.TargetName != "" ||
			q.Hash != (ContentHash{}) || q.NewBlob {
			return fmt.Errorf("%w: invalid temporary storage quarantine", ErrInvalidObject)
		}
	case storageTransitionPublish:
		if q.Stage == (StageID{}) || q.Hash == (ContentHash{}) ||
			!validBlobStorageName(q.TargetName) ||
			filepath.Base(q.TargetName) != q.TargetName {
			return fmt.Errorf("%w: invalid publish storage quarantine", ErrInvalidObject)
		}
	case storageTransitionDeletePublished:
		if q.Stage != (StageID{}) || q.TargetName != "" ||
			q.Hash == (ContentHash{}) || q.NewBlob {
			return fmt.Errorf("%w: invalid published-delete storage quarantine", ErrInvalidObject)
		}
	default:
		return fmt.Errorf("%w: unknown storage quarantine kind", ErrInvalidObject)
	}
	expected, err := storageQuarantineToken(q)
	if err != nil {
		return err
	}
	if expected != q.Token {
		return fmt.Errorf("%w: storage quarantine token mismatch", ErrIntegrity)
	}
	return nil
}

// InspectStorageQuarantine returns quarantined transitions after the opaque
// cursor. A zero cursor starts at the beginning.
func (c *Catalog) InspectStorageQuarantine(
	ctx context.Context,
	after StorageTransitionID,
	limit int,
) (StorageQuarantinePage, error) {
	if limit <= 0 || limit > MaintenancePageLimit {
		return StorageQuarantinePage{}, fmt.Errorf(
			"%w: invalid storage quarantine page limit", ErrInvalidObject,
		)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT transition_id, kind, owner_id, stage_id, source_name,
       target_name, hash, size, new_blob, reason
FROM storage_transitions
WHERE quarantined = 1 AND transition_id > ?
ORDER BY transition_id
LIMIT ?`, after[:], limit)
	if err != nil {
		return StorageQuarantinePage{}, fmt.Errorf("catalog: inspect storage quarantine: %w", err)
	}
	var entries []StorageQuarantine
	byteTruncated := false
	for rows.Next() {
		var entry StorageQuarantine
		var rawID, rawOwner, rawStage, rawHash []byte
		if err := rows.Scan(
			&rawID, &entry.Kind, &rawOwner, &rawStage, &entry.SourceName,
			&entry.TargetName, &rawHash, &entry.Size, &entry.NewBlob, &entry.Reason,
		); err != nil {
			_ = rows.Close()
			return StorageQuarantinePage{}, fmt.Errorf("catalog: scan storage quarantine: %w", err)
		}
		transition := storageTransition{kind: entry.Kind}
		if err := decodeStorageTransitionIdentity(
			&transition, rawID, rawOwner, rawStage, rawHash,
		); err != nil {
			_ = rows.Close()
			return StorageQuarantinePage{}, err
		}
		entry.ID = transition.id
		entry.Owner = transition.owner
		entry.Stage = transition.stage
		entry.Hash = transition.hash
		entry.Token, err = storageQuarantineToken(entry)
		if err != nil {
			_ = rows.Close()
			return StorageQuarantinePage{}, err
		}
		if byteTruncated {
			continue
		}
		candidate := append(entries, entry)
		encoded, err := json.Marshal(StorageQuarantinePage{
			Entries: candidate,
			More:    true,
		})
		if err != nil {
			_ = rows.Close()
			return StorageQuarantinePage{}, fmt.Errorf(
				"catalog: encode storage quarantine page: %w", err,
			)
		}
		if len(encoded) > StorageQuarantinePageByteLimit-4096 {
			byteTruncated = true
			continue
		}
		entries = candidate
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return StorageQuarantinePage{}, fmt.Errorf("catalog: read storage quarantine: %w", err)
	}
	if err := rows.Close(); err != nil {
		return StorageQuarantinePage{}, fmt.Errorf("catalog: close storage quarantine: %w", err)
	}
	cursor := after
	if len(entries) != 0 {
		cursor = entries[len(entries)-1].ID
	}
	var more bool
	if err := c.readDB.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM storage_transitions
    WHERE quarantined = 1 AND transition_id > ?
)`, cursor[:]).Scan(&more); err != nil {
		return StorageQuarantinePage{}, fmt.Errorf(
			"catalog: inspect storage quarantine continuation: %w", err,
		)
	}
	page := StorageQuarantinePage{Entries: entries, More: more || byteTruncated}
	return page, nil
}

// ResolveStorageQuarantine applies one verified operator decision. The token
// must come from the exact row returned by InspectStorageQuarantine.
func (c *Catalog) ResolveStorageQuarantine(
	ctx context.Context,
	id StorageTransitionID,
	token StorageQuarantineToken,
	resolution StorageQuarantineResolution,
) (StorageQuarantineResolutionReceipt, error) {
	if id == (StorageTransitionID{}) || token == (StorageQuarantineToken{}) {
		return StorageQuarantineResolutionReceipt{}, fmt.Errorf(
			"%w: invalid storage quarantine identity", ErrInvalidObject,
		)
	}
	if resolution != StorageQuarantineRetry && resolution != StorageQuarantineDiscard {
		return StorageQuarantineResolutionReceipt{}, fmt.Errorf(
			"%w: invalid storage quarantine resolution", ErrInvalidObject,
		)
	}
	c.storage.mu.Lock()
	receipt, state, found, err := c.readStorageQuarantineResolution(ctx, c.db, id)
	if err != nil {
		c.storage.mu.Unlock()
		return StorageQuarantineResolutionReceipt{}, err
	}
	if found {
		if receipt.Token != token || receipt.Resolution != resolution {
			c.storage.mu.Unlock()
			return StorageQuarantineResolutionReceipt{}, fmt.Errorf(
				"%w: storage quarantine resolution request changed", ErrMutationConflict,
			)
		}
		if state == storageQuarantineResolutionSettled {
			c.storage.mu.Unlock()
			return receipt, nil
		}
		if state != storageQuarantineResolutionPending {
			c.storage.mu.Unlock()
			return StorageQuarantineResolutionReceipt{}, fmt.Errorf(
				"%w: invalid storage quarantine resolution state", ErrIntegrity,
			)
		}
		if _, err := c.db.ExecContext(ctx, `
UPDATE storage_transitions
SET quarantined = 0, reason = ''
WHERE transition_id = ? AND quarantined = 1`,
			id[:]); err != nil {
			c.storage.mu.Unlock()
			return StorageQuarantineResolutionReceipt{}, fmt.Errorf(
				"catalog: resume storage quarantine resolution: %w", err,
			)
		}
	} else {
		receipt, err = c.claimStorageQuarantineResolution(
			ctx, id, token, resolution,
		)
		if err != nil {
			c.storage.mu.Unlock()
			return StorageQuarantineResolutionReceipt{}, err
		}
	}
	if err := c.trip(storageResolutionAfterClaim); err != nil {
		c.storage.mu.Unlock()
		return StorageQuarantineResolutionReceipt{}, err
	}
	if resolution == StorageQuarantineRetry {
		err = c.settleStorageTransitionLocked(ctx, id, false, &receipt)
	} else {
		err = c.discardStorageTransitionLocked(ctx, id, &receipt)
	}
	c.storage.mu.Unlock()
	if err != nil {
		return StorageQuarantineResolutionReceipt{}, c.quarantineStorageTransition(ctx, id, err)
	}
	return receipt, nil
}

// AcknowledgeStorageQuarantineResolution forgets one exact settled receipt.
// Repeating an acknowledgement after it was forgotten is idempotent.
func (c *Catalog) AcknowledgeStorageQuarantineResolution(
	ctx context.Context,
	receipt StorageQuarantineResolutionReceipt,
) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	c.storage.mu.Lock()
	defer c.storage.mu.Unlock()
	stored, state, found, err := c.readStorageQuarantineResolution(
		ctx, c.db, receipt.ID,
	)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if stored != receipt {
		return fmt.Errorf("%w: storage quarantine receipt changed", ErrMutationConflict)
	}
	if state != storageQuarantineResolutionSettled {
		return fmt.Errorf("%w: storage quarantine resolution is pending", ErrInvalidTransition)
	}
	result, err := c.db.ExecContext(ctx, `
DELETE FROM storage_quarantine_resolutions
WHERE transition_id = ? AND token = ? AND resolution = ?
  AND state = ? AND outcome_digest = ?`,
		receipt.ID[:], receipt.Token[:], receipt.Resolution,
		storageQuarantineResolutionSettled, receipt.OutcomeDigest[:])
	if err != nil {
		return fmt.Errorf("catalog: acknowledge storage quarantine resolution: %w", err)
	}
	return requireOneRow(result, "storage quarantine resolution acknowledgement")
}

// Validate checks one exact durable resolution receipt.
func (r StorageQuarantineResolutionReceipt) Validate() error {
	if r.ID == (StorageTransitionID{}) ||
		r.Token == (StorageQuarantineToken{}) ||
		r.OutcomeDigest == ([32]byte{}) ||
		(r.Resolution != StorageQuarantineRetry &&
			r.Resolution != StorageQuarantineDiscard) {
		return fmt.Errorf("%w: invalid storage quarantine resolution receipt", ErrInvalidObject)
	}
	return nil
}

func (c *Catalog) readStorageQuarantineResolution(
	ctx context.Context,
	query rowQuerier,
	id StorageTransitionID,
) (StorageQuarantineResolutionReceipt, uint8, bool, error) {
	var receipt StorageQuarantineResolutionReceipt
	var rawID, rawToken, rawOutcome []byte
	var state uint8
	err := query.QueryRowContext(ctx, `
SELECT transition_id, token, resolution, state, outcome_digest
FROM storage_quarantine_resolutions
WHERE transition_id = ?`, id[:]).Scan(
		&rawID, &rawToken, &receipt.Resolution, &state, &rawOutcome,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return receipt, 0, false, nil
	}
	if err != nil {
		return receipt, 0, false, fmt.Errorf(
			"catalog: read storage quarantine resolution: %w", err,
		)
	}
	if len(rawID) != len(receipt.ID) ||
		len(rawToken) != len(receipt.Token) ||
		len(rawOutcome) != len(receipt.OutcomeDigest) {
		return receipt, 0, false, fmt.Errorf(
			"%w: invalid storage quarantine receipt identity", ErrIntegrity,
		)
	}
	copy(receipt.ID[:], rawID)
	copy(receipt.Token[:], rawToken)
	copy(receipt.OutcomeDigest[:], rawOutcome)
	if err := receipt.Validate(); err != nil {
		return receipt, 0, false, errors.Join(ErrIntegrity, err)
	}
	return receipt, state, true, nil
}

func storageQuarantineOutcomeDigest(
	entry StorageQuarantine,
	resolution StorageQuarantineResolution,
) ([32]byte, error) {
	proof := struct {
		Domain     string
		Entry      StorageQuarantine
		Resolution StorageQuarantineResolution
	}{
		Domain: "fusekit.storage-quarantine-resolution.v1",
		Entry:  entry, Resolution: resolution,
	}
	encoded, err := json.Marshal(proof)
	if err != nil {
		return [32]byte{}, fmt.Errorf("catalog: encode storage quarantine outcome: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func (c *Catalog) claimStorageQuarantineResolution(
	ctx context.Context,
	id StorageTransitionID,
	token StorageQuarantineToken,
	resolution StorageQuarantineResolution,
) (StorageQuarantineResolutionReceipt, error) {
	transition, err := c.readStorageTransition(ctx, c.db, id)
	if err != nil {
		return StorageQuarantineResolutionReceipt{}, err
	}
	if !transition.quarantined {
		return StorageQuarantineResolutionReceipt{}, fmt.Errorf(
			"%w: storage transition is not quarantined", ErrInvalidTransition,
		)
	}
	entry := storageQuarantineFromTransition(transition)
	expected, err := storageQuarantineToken(entry)
	if err != nil {
		return StorageQuarantineResolutionReceipt{}, err
	}
	if expected != token {
		return StorageQuarantineResolutionReceipt{}, fmt.Errorf(
			"%w: storage quarantine changed after inspection", ErrInvalidTransition,
		)
	}
	receipt := StorageQuarantineResolutionReceipt{
		ID: id, Token: token, Resolution: resolution,
	}
	receipt.OutcomeDigest, err = storageQuarantineOutcomeDigest(entry, resolution)
	if err != nil {
		return StorageQuarantineResolutionReceipt{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return StorageQuarantineResolutionReceipt{}, fmt.Errorf(
			"catalog: begin storage quarantine resolution: %w", err,
		)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO storage_quarantine_resolutions(
    transition_id, token, resolution, state, outcome_digest
) VALUES (?, ?, ?, ?, ?)`,
		receipt.ID[:], receipt.Token[:], receipt.Resolution,
		storageQuarantineResolutionPending, receipt.OutcomeDigest[:]); err != nil {
		return StorageQuarantineResolutionReceipt{}, fmt.Errorf(
			"catalog: record storage quarantine resolution: %w", mapConstraint(err),
		)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE storage_transitions
SET quarantined = 0, reason = ''
WHERE transition_id = ? AND quarantined = 1 AND reason = ?`,
		id[:], transition.reason)
	if err != nil {
		return StorageQuarantineResolutionReceipt{}, fmt.Errorf(
			"catalog: claim storage quarantine: %w", err,
		)
	}
	if err := requireOneRow(result, "storage quarantine resolution"); err != nil {
		return StorageQuarantineResolutionReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return StorageQuarantineResolutionReceipt{}, fmt.Errorf(
			"catalog: commit storage quarantine resolution: %w", err,
		)
	}
	return receipt, nil
}

func storageQuarantineFromTransition(transition storageTransition) StorageQuarantine {
	return StorageQuarantine{
		ID: transition.id, Kind: transition.kind, Owner: transition.owner,
		Stage: transition.stage, SourceName: transition.sourceName,
		TargetName: transition.targetName, Hash: transition.hash,
		Size: transition.size, NewBlob: transition.newBlob, Reason: transition.reason,
	}
}

func storageQuarantineToken(entry StorageQuarantine) (StorageQuarantineToken, error) {
	entry.Token = StorageQuarantineToken{}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return StorageQuarantineToken{}, fmt.Errorf("catalog: encode storage quarantine token: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func (c *Catalog) discardStorageTransitionLocked(
	ctx context.Context,
	id StorageTransitionID,
	receipt *StorageQuarantineResolutionReceipt,
) error {
	transition, err := c.readStorageTransition(ctx, c.db, id)
	if err != nil {
		return err
	}
	if transition.kind != storageTransitionPublish {
		return c.settleStorageTransitionLocked(ctx, id, false, receipt)
	}
	if transition.stage == (StageID{}) || transition.hash == (ContentHash{}) {
		return fmt.Errorf("%w: invalid publish discard transition", ErrIntegrity)
	}
	source := filepath.Join(c.blobDir, transition.sourceName)
	if err := os.Remove(source); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("catalog: remove discarded publish source: %w", err)
	}
	if transition.newBlob {
		var exists bool
		if err := c.db.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM storage_entries
    WHERE name = ? AND kind = ? AND hash = ?
)`, transition.targetName, storageEntryPublished, transition.hash[:]).Scan(&exists); err != nil {
			return fmt.Errorf("catalog: inspect discarded publish target: %w", err)
		}
		if exists {
			return fmt.Errorf("%w: discarded publish target is now durable", ErrInvalidTransition)
		}
		target := filepath.Join(c.blobDir, transition.targetName)
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("catalog: remove discarded publish target: %w", err)
		}
	}
	if err := syncStorageDirectory(c.blobDir); err != nil {
		return fmt.Errorf("catalog: sync discarded publish: %w", err)
	}
	return c.abortPublishRecoveryLocked(ctx, transition, receipt)
}

func (c *Catalog) quarantineStorageFailure(
	ctx context.Context,
	hash ContentHash,
	size int64,
	cause error,
) error {
	id, err := newStorageTransitionID()
	if err != nil {
		return errors.Join(cause, err)
	}
	name := blobName(hash)
	reason := storageQuarantineReason(cause)
	_, err = c.db.ExecContext(ctx, `
INSERT OR IGNORE INTO storage_transitions(
    transition_id, kind, owner_id, source_name, hash, size,
    new_blob, quarantined, reason
) VALUES (?, ?, ?, ?, ?, ?, 0, 1, ?)`,
		id[:], storageTransitionDeletePublished, c.owner[:],
		name, hash[:], size, reason)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("catalog: quarantine storage failure: %w", err))
	}
	return errors.Join(ErrIntegrity, cause)
}

func storageQuarantineReason(cause error) string {
	reason := cause.Error()
	if len(reason) > 4096 {
		reason = reason[:4096]
	}
	if reason == "" {
		return "storage transition failed integrity validation"
	}
	return reason
}
