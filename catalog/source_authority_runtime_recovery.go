package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/causal"
)

const (
	// SourceAuthorityRuntimeRecoveryPageLimit bounds one recovery result page.
	SourceAuthorityRuntimeRecoveryPageLimit = 256
	// SourceAuthorityRuntimeRecoveryPageByteLimit bounds one encoded result page.
	SourceAuthorityRuntimeRecoveryPageByteLimit = 1 << 20
	// SourceAuthorityRuntimeRecoveryCountLimit bounds one process-owned epoch set.
	SourceAuthorityRuntimeRecoveryCountLimit = SourceAuthorityFleetAuthorityLimit
	// SourceAuthorityRuntimeRecoveryResultByteLimit bounds one assembled result.
	SourceAuthorityRuntimeRecoveryResultByteLimit = SourceAuthorityFleetByteLimit

	sourceAuthorityRuntimeRecoveryAfterClose = "source-authority.runtime-recovery-after-close"
)

// SourceAuthorityRuntimeRecoverySummary identifies one durable close-all result.
type SourceAuthorityRuntimeRecoverySummary struct {
	ReceiptDigest [32]byte
	ClosedCount   uint64
	ClosedDigest  [32]byte
}

// SourceAuthorityRuntimeRecoveryResult is the complete canonical set of epochs
// closed for one exact reaped process.
type SourceAuthorityRuntimeRecoveryResult struct {
	Summary SourceAuthorityRuntimeRecoverySummary
	Closed  []SourceAuthorityRuntimeFence
}

// SourceAuthorityRuntimeRecoveryPageRequest selects one bounded result page.
type SourceAuthorityRuntimeRecoveryPageRequest struct {
	Receipt proc.ReapReceipt
	After   uint64
	Limit   int
}

// SourceAuthorityRuntimeRecoveryPage is one canonical durable result page.
type SourceAuthorityRuntimeRecoveryPage struct {
	Summary SourceAuthorityRuntimeRecoverySummary
	Start   uint64
	Closed  []SourceAuthorityRuntimeFence
	Next    uint64
}

// Validate verifies a recovery summary against the exact daemonkit receipt.
func (s SourceAuthorityRuntimeRecoverySummary) Validate(receipt proc.ReapReceipt) error {
	if err := validateSourceAuthorityReapReceipt(receipt); err != nil {
		return err
	}
	if s.ReceiptDigest != receipt.Digest ||
		s.ClosedCount > SourceAuthorityRuntimeRecoveryCountLimit ||
		s.ClosedDigest == ([32]byte{}) {
		return fmt.Errorf("%w: invalid source authority runtime recovery summary", ErrInvalidObject)
	}
	return nil
}

// Validate verifies a complete canonical close-all result.
func (r SourceAuthorityRuntimeRecoveryResult) Validate(receipt proc.ReapReceipt) error {
	if err := r.Summary.Validate(receipt); err != nil {
		return err
	}
	if uint64(len(r.Closed)) != r.Summary.ClosedCount {
		return fmt.Errorf("%w: source authority runtime recovery result is incomplete", ErrIntegrity)
	}
	for index, fence := range r.Closed {
		if err := fence.Validate(); err != nil {
			return errors.Join(ErrIntegrity, err)
		}
		if index > 0 && compareSourceAuthorityRuntimeFence(r.Closed[index-1], fence) >= 0 {
			return fmt.Errorf("%w: source authority runtime recovery result is not canonical", ErrIntegrity)
		}
	}
	digest, err := SourceAuthorityRuntimeRecoveryDigest(receipt, r.Closed)
	if err != nil {
		return err
	}
	if digest != r.Summary.ClosedDigest {
		return fmt.Errorf("%w: source authority runtime recovery digest mismatch", ErrIntegrity)
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("catalog: encode source authority runtime recovery result: %w", err)
	}
	if len(encoded) > SourceAuthorityRuntimeRecoveryResultByteLimit {
		return fmt.Errorf("%w: source authority runtime recovery result exceeds byte limit", ErrIntegrity)
	}
	return nil
}

// Validate verifies one bounded canonical result page.
func (p SourceAuthorityRuntimeRecoveryPage) Validate(
	request SourceAuthorityRuntimeRecoveryPageRequest,
) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := p.Summary.Validate(request.Receipt); err != nil {
		return errors.Join(ErrIntegrity, err)
	}
	if p.Start != request.After+1 || len(p.Closed) == 0 || len(p.Closed) > request.Limit ||
		p.Start+uint64(len(p.Closed))-1 > p.Summary.ClosedCount {
		return fmt.Errorf("%w: invalid source authority runtime recovery page", ErrIntegrity)
	}
	for index, fence := range p.Closed {
		if err := fence.Validate(); err != nil {
			return errors.Join(ErrIntegrity, err)
		}
		if index > 0 && compareSourceAuthorityRuntimeFence(p.Closed[index-1], fence) >= 0 {
			return fmt.Errorf("%w: source authority runtime recovery page is not canonical", ErrIntegrity)
		}
	}
	last := p.Start + uint64(len(p.Closed)) - 1
	if (p.Next == 0 && last != p.Summary.ClosedCount) ||
		(p.Next != 0 && (p.Next != last || last >= p.Summary.ClosedCount)) {
		return fmt.Errorf("%w: invalid source authority runtime recovery continuation", ErrIntegrity)
	}
	encoded, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("catalog: encode source authority runtime recovery page: %w", err)
	}
	if len(encoded) > SourceAuthorityRuntimeRecoveryPageByteLimit {
		return fmt.Errorf("%w: source authority runtime recovery page exceeds byte limit", ErrIntegrity)
	}
	return nil
}

// Validate checks one bounded recovery page request.
func (r SourceAuthorityRuntimeRecoveryPageRequest) Validate() error {
	if err := validateSourceAuthorityReapReceipt(r.Receipt); err != nil {
		return err
	}
	if r.Limit <= 0 || r.Limit > SourceAuthorityRuntimeRecoveryPageLimit {
		return fmt.Errorf("%w: invalid source authority runtime recovery page limit", ErrInvalidObject)
	}
	return nil
}

// SourceAuthorityRuntimeRecoveryDigest returns the canonical closed-set digest.
func SourceAuthorityRuntimeRecoveryDigest(
	receipt proc.ReapReceipt,
	closed []SourceAuthorityRuntimeFence,
) ([32]byte, error) {
	if err := validateSourceAuthorityReapReceipt(receipt); err != nil {
		return [32]byte{}, err
	}
	if len(closed) > SourceAuthorityRuntimeRecoveryCountLimit {
		return [32]byte{}, fmt.Errorf("%w: invalid source authority runtime recovery count", ErrInvalidObject)
	}
	for index, fence := range closed {
		if err := fence.Validate(); err != nil {
			return [32]byte{}, err
		}
		if index > 0 && compareSourceAuthorityRuntimeFence(closed[index-1], fence) >= 0 {
			return [32]byte{}, fmt.Errorf("%w: source authority runtime recovery set is not canonical", ErrInvalidObject)
		}
	}
	canonical := closed
	if len(canonical) == 0 {
		canonical = []SourceAuthorityRuntimeFence{}
	}
	payload, err := json.Marshal(struct {
		Domain        string
		ReceiptDigest [32]byte
		Closed        []SourceAuthorityRuntimeFence
	}{
		Domain:        "fusekit.source-authority-runtime-recovery.v1",
		ReceiptDigest: receipt.Digest,
		Closed:        canonical,
	})
	if err != nil {
		return [32]byte{}, fmt.Errorf("catalog: encode source authority runtime recovery digest: %w", err)
	}
	if len(payload) > SourceAuthorityRuntimeRecoveryResultByteLimit {
		return [32]byte{}, fmt.Errorf("%w: source authority runtime recovery set exceeds byte limit", ErrInvalidObject)
	}
	return sha256.Sum256(payload), nil
}

// RecoverReapedSourceAuthorityRuntimes atomically closes every current runtime
// epoch owned by one exact reaped process and returns the complete durable set.
func (c *Catalog) RecoverReapedSourceAuthorityRuntimes(
	ctx context.Context,
	receipt proc.ReapReceipt,
) (SourceAuthorityRuntimeRecoveryResult, error) {
	summary, err := c.BeginRecoverReapedSourceAuthorityRuntimes(ctx, receipt)
	if err != nil {
		return SourceAuthorityRuntimeRecoveryResult{}, err
	}
	result := SourceAuthorityRuntimeRecoveryResult{
		Summary: summary,
		Closed:  make([]SourceAuthorityRuntimeFence, 0, summary.ClosedCount),
	}
	for after := uint64(0); summary.ClosedCount > 0; {
		page, err := c.SourceAuthorityRuntimeRecoveryPage(ctx, SourceAuthorityRuntimeRecoveryPageRequest{
			Receipt: receipt, After: after, Limit: SourceAuthorityRuntimeRecoveryPageLimit,
		})
		if err != nil {
			return SourceAuthorityRuntimeRecoveryResult{}, err
		}
		result.Closed = append(result.Closed, page.Closed...)
		if page.Next == 0 {
			break
		}
		after = page.Next
	}
	if err := result.Validate(receipt); err != nil {
		return SourceAuthorityRuntimeRecoveryResult{}, err
	}
	return result, nil
}

// BeginRecoverReapedSourceAuthorityRuntimes commits or replays one atomic
// close-all result and returns its bounded summary.
func (c *Catalog) BeginRecoverReapedSourceAuthorityRuntimes(
	ctx context.Context,
	receipt proc.ReapReceipt,
) (SourceAuthorityRuntimeRecoverySummary, error) {
	if err := validateSourceAuthorityReapReceipt(receipt); err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, err
	}
	ownerJSON, ownerDigest, err := sourceAuthorityRuntimeOwner(receipt.Record)
	if err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, err
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, fmt.Errorf("catalog: encode source authority reap receipt: %w", err)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, fmt.Errorf("catalog: begin source authority runtime recovery: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if summary, stored, found, err := readSourceAuthorityRuntimeRecoverySummary(
		ctx, tx, receipt.Digest,
	); err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, err
	} else if found {
		if !bytes.Equal(stored, receiptJSON) {
			return SourceAuthorityRuntimeRecoverySummary{}, fmt.Errorf(
				"%w: source authority runtime recovery receipt changed", ErrMutationConflict,
			)
		}
		if err := summary.Validate(receipt); err != nil {
			return SourceAuthorityRuntimeRecoverySummary{}, errors.Join(ErrIntegrity, err)
		}
		return summary, nil
	}
	processed, _, err := sourceAuthorityRuntimeRecoveryFloor(ctx, tx, receipt.LedgerID, true)
	if err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, err
	}
	if receipt.Sequence <= processed {
		return SourceAuthorityRuntimeRecoverySummary{}, errors.Join(
			ErrMutationConflict, proc.ErrReapReceiptStale,
		)
	}
	if receipt.Sequence != processed+1 {
		return SourceAuthorityRuntimeRecoverySummary{}, errors.Join(
			ErrMutationConflict, proc.ErrReapReceiptOrder,
		)
	}

	closed, err := sourceAuthorityRuntimeFencesForOwner(ctx, tx, ownerJSON, ownerDigest)
	if err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, err
	}
	closedDigest, err := SourceAuthorityRuntimeRecoveryDigest(receipt, closed)
	if err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, err
	}
	summary := SourceAuthorityRuntimeRecoverySummary{
		ReceiptDigest: receipt.Digest,
		ClosedCount:   uint64(len(closed)),
		ClosedDigest:  closedDigest,
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_runtime_recovery_receipts(
    receipt_digest, ledger_id, sequence, receipt_json, runtime_owner_json, runtime_owner_digest,
    closed_count, closed_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, receipt.Digest[:], receipt.LedgerID[:], receipt.Sequence,
		receiptJSON, ownerJSON, ownerDigest[:],
		len(closed), closedDigest[:]); err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, fmt.Errorf(
			"catalog: record source authority runtime recovery: %w", mapConstraint(err),
		)
	}
	statement, err := tx.PrepareContext(ctx, `
INSERT INTO source_authority_runtime_recovery_members(
    receipt_digest, ordinal, owner_id, generation, source_authority, runtime_epoch
) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, fmt.Errorf(
			"catalog: prepare source authority runtime recovery members: %w", err,
		)
	}
	for index, fence := range closed {
		if _, err := statement.ExecContext(ctx, receipt.Digest[:], index+1,
			string(fence.Owner), uint64(fence.Generation), string(fence.Authority), fence.Epoch[:]); err != nil {
			_ = statement.Close()
			return SourceAuthorityRuntimeRecoverySummary{}, fmt.Errorf(
				"catalog: record source authority runtime recovery member: %w", mapConstraint(err),
			)
		}
	}
	if err := statement.Close(); err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, fmt.Errorf(
			"catalog: close source authority runtime recovery member statement: %w", err,
		)
	}
	updated, err := tx.ExecContext(ctx, `
UPDATE source_authority_fleet_members
SET runtime_closed = 1
WHERE runtime_closed = 0
  AND runtime_owner_digest = ? AND runtime_owner_json = ?
  AND EXISTS (
      SELECT 1 FROM source_authority_fleet_heads head
      WHERE head.owner_id = source_authority_fleet_members.owner_id
        AND head.generation = source_authority_fleet_members.generation
  )`, ownerDigest[:], ownerJSON)
	if err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, fmt.Errorf(
			"catalog: close reaped source authority runtimes: %w", err,
		)
	}
	if count, err := updated.RowsAffected(); err != nil || count < 0 || count > int64(len(closed)) {
		return SourceAuthorityRuntimeRecoverySummary{}, errors.Join(
			fmt.Errorf("%w: source authority runtime recovery set changed", ErrMutationConflict), err,
		)
	}
	if err := c.trip(sourceAuthorityRuntimeRecoveryAfterClose); err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, err
	}
	advanced, err := tx.ExecContext(ctx, `
UPDATE source_authority_runtime_recovery_floors
SET processed_sequence = ?
WHERE ledger_id = ? AND processed_sequence = ?`, receipt.Sequence, receipt.LedgerID[:], processed)
	if err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, fmt.Errorf(
			"catalog: advance source authority runtime recovery floor: %w", err,
		)
	}
	if count, countErr := advanced.RowsAffected(); countErr != nil || count != 1 {
		return SourceAuthorityRuntimeRecoverySummary{}, errors.Join(
			fmt.Errorf("%w: source authority runtime recovery floor changed", ErrMutationConflict),
			countErr,
		)
	}
	if err := tx.Commit(); err != nil {
		return SourceAuthorityRuntimeRecoverySummary{}, fmt.Errorf(
			"catalog: commit source authority runtime recovery: %w", err,
		)
	}
	return summary, nil
}

// AcknowledgeSourceAuthorityRuntimeRecovery records the exact daemonkit
// acknowledgement floor that permits recovery-result compaction.
func (c *Catalog) AcknowledgeSourceAuthorityRuntimeRecovery(
	ctx context.Context,
	floor proc.ReapReceiptFloor,
) error {
	if err := validateSourceAuthorityRuntimeRecoveryFloor(floor); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source authority runtime recovery acknowledgement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	processed, acknowledged, err := sourceAuthorityRuntimeRecoveryFloor(
		ctx, tx, floor.LedgerID, false,
	)
	if err != nil {
		return err
	}
	if floor.Sequence < acknowledged {
		return errors.Join(ErrMutationConflict, proc.ErrReapReceiptStale)
	}
	if floor.Sequence == acknowledged {
		return nil
	}
	if floor.Sequence > processed {
		return errors.Join(ErrMutationConflict, proc.ErrReapReceiptOrder)
	}
	updated, err := tx.ExecContext(ctx, `
UPDATE source_authority_runtime_recovery_floors
SET acknowledged_sequence = ?
WHERE ledger_id = ? AND acknowledged_sequence = ?`,
		floor.Sequence, floor.LedgerID[:], acknowledged)
	if err != nil {
		return fmt.Errorf("catalog: acknowledge source authority runtime recovery: %w", err)
	}
	if count, countErr := updated.RowsAffected(); countErr != nil || count != 1 {
		return errors.Join(
			fmt.Errorf("%w: source authority runtime recovery floor changed", ErrMutationConflict),
			countErr,
		)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit source authority runtime recovery acknowledgement: %w", err)
	}
	return nil
}

// SourceAuthorityRuntimeRecoveryPage returns one durable bounded result page.
func (c *Catalog) SourceAuthorityRuntimeRecoveryPage(
	ctx context.Context,
	request SourceAuthorityRuntimeRecoveryPageRequest,
) (page SourceAuthorityRuntimeRecoveryPage, err error) {
	if err := request.Validate(); err != nil {
		return SourceAuthorityRuntimeRecoveryPage{}, err
	}
	receiptJSON, err := json.Marshal(request.Receipt)
	if err != nil {
		return SourceAuthorityRuntimeRecoveryPage{}, fmt.Errorf("catalog: encode source authority reap receipt: %w", err)
	}
	summary, stored, found, err := readSourceAuthorityRuntimeRecoverySummary(
		ctx, c.readDB, request.Receipt.Digest,
	)
	if err != nil {
		return SourceAuthorityRuntimeRecoveryPage{}, err
	}
	if !found || !bytes.Equal(stored, receiptJSON) {
		return SourceAuthorityRuntimeRecoveryPage{}, fmt.Errorf(
			"%w: unknown source authority runtime recovery receipt", ErrMutationConflict,
		)
	}
	if request.After >= summary.ClosedCount {
		return SourceAuthorityRuntimeRecoveryPage{}, fmt.Errorf(
			"%w: source authority runtime recovery cursor is exhausted", ErrInvalidObject,
		)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT ordinal, owner_id, generation, source_authority, runtime_epoch
FROM source_authority_runtime_recovery_members
WHERE receipt_digest = ? AND ordinal > ?
ORDER BY ordinal
LIMIT ?`, request.Receipt.Digest[:], request.After, request.Limit+1)
	if err != nil {
		return SourceAuthorityRuntimeRecoveryPage{}, fmt.Errorf(
			"catalog: read source authority runtime recovery page: %w", err,
		)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	page = SourceAuthorityRuntimeRecoveryPage{Summary: summary, Start: request.After + 1}
	more := false
	for rows.Next() {
		var ordinal uint64
		var owner string
		var generation uint64
		var authority string
		var rawEpoch []byte
		if err := rows.Scan(&ordinal, &owner, &generation, &authority, &rawEpoch); err != nil {
			return SourceAuthorityRuntimeRecoveryPage{}, fmt.Errorf(
				"catalog: scan source authority runtime recovery page: %w", err,
			)
		}
		if ordinal != request.After+uint64(len(page.Closed))+1 || len(rawEpoch) != 16 {
			return SourceAuthorityRuntimeRecoveryPage{}, fmt.Errorf(
				"%w: corrupt source authority runtime recovery member", ErrIntegrity,
			)
		}
		if len(page.Closed) == request.Limit {
			more = true
			break
		}
		fence := SourceAuthorityRuntimeFence{
			Owner: SourceAuthorityFleetOwnerID(owner), Generation: causal.Generation(generation),
			Authority: causal.SourceAuthorityID(authority),
		}
		copy(fence.Epoch[:], rawEpoch)
		candidate := append(page.Closed, fence)
		encoded, err := json.Marshal(SourceAuthorityRuntimeRecoveryPage{
			Summary: summary, Start: page.Start, Closed: candidate,
			Next: ordinal,
		})
		if err != nil {
			return SourceAuthorityRuntimeRecoveryPage{}, fmt.Errorf(
				"catalog: encode source authority runtime recovery page: %w", err,
			)
		}
		if len(encoded) > SourceAuthorityRuntimeRecoveryPageByteLimit-4096 {
			more = true
			break
		}
		page.Closed = candidate
	}
	if err := rows.Err(); err != nil {
		return SourceAuthorityRuntimeRecoveryPage{}, fmt.Errorf(
			"catalog: iterate source authority runtime recovery page: %w", err,
		)
	}
	if len(page.Closed) == 0 {
		return SourceAuthorityRuntimeRecoveryPage{}, fmt.Errorf(
			"%w: source authority runtime recovery page made no progress", ErrIntegrity,
		)
	}
	last := request.After + uint64(len(page.Closed))
	if more || last < summary.ClosedCount {
		page.Next = last
	}
	if err := page.Validate(request); err != nil {
		return SourceAuthorityRuntimeRecoveryPage{}, err
	}
	return page, nil
}

func validateSourceAuthorityReapReceipt(receipt proc.ReapReceipt) error {
	if err := receipt.Validate(); err != nil {
		return fmt.Errorf("%w: invalid source authority reap receipt: %v", ErrInvalidObject, err)
	}
	if receipt.Record.RecoveryClass != proc.RecoverySourceOwner {
		return fmt.Errorf("%w: reap receipt is not for a source owner", ErrInvalidObject)
	}
	if _, _, err := sourceAuthorityRuntimeOwner(receipt.Record); err != nil {
		return fmt.Errorf("%w: invalid source authority reap receipt owner", ErrInvalidObject)
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("catalog: encode source authority reap receipt: %w", err)
	}
	if len(encoded) > 32<<10 {
		return fmt.Errorf("%w: source authority reap receipt exceeds byte limit", ErrInvalidObject)
	}
	return nil
}

func validateSourceAuthorityRuntimeRecoveryFloor(floor proc.ReapReceiptFloor) error {
	if floor.LedgerID == (proc.ReceiptLedgerID{}) || floor.Sequence == 0 ||
		floor.RecoveryClass != proc.RecoverySourceOwner {
		return fmt.Errorf("%w: invalid source authority runtime recovery floor", ErrInvalidObject)
	}
	return nil
}

func sourceAuthorityRuntimeRecoveryFloor(
	ctx context.Context,
	tx *sql.Tx,
	ledgerID proc.ReceiptLedgerID,
	create bool,
) (uint64, uint64, error) {
	var storedLedger []byte
	var processed uint64
	var acknowledged uint64
	err := tx.QueryRowContext(ctx, `
SELECT ledger_id, processed_sequence, acknowledged_sequence
FROM source_authority_runtime_recovery_floors
WHERE singleton = 1`).Scan(&storedLedger, &processed, &acknowledged)
	if errors.Is(err, sql.ErrNoRows) {
		if !create {
			return 0, 0, fmt.Errorf(
				"%w: unknown source authority runtime recovery ledger", ErrMutationConflict,
			)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_runtime_recovery_floors(
    ledger_id, singleton, processed_sequence, acknowledged_sequence
) VALUES (?, 1, 0, 0)`, ledgerID[:]); err != nil {
			return 0, 0, fmt.Errorf(
				"catalog: initialize source authority runtime recovery floor: %w", mapConstraint(err),
			)
		}
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("catalog: read source authority runtime recovery floor: %w", err)
	}
	if len(storedLedger) != len(ledgerID) || !bytes.Equal(storedLedger, ledgerID[:]) {
		return 0, 0, fmt.Errorf(
			"%w: source authority runtime recovery ledger changed", ErrMutationConflict,
		)
	}
	if acknowledged > processed {
		return 0, 0, fmt.Errorf("%w: invalid source authority runtime recovery floor", ErrIntegrity)
	}
	return processed, acknowledged, nil
}

func sourceAuthorityRuntimeFencesForOwner(
	ctx context.Context,
	tx *sql.Tx,
	ownerJSON []byte,
	ownerDigest [32]byte,
) (closed []SourceAuthorityRuntimeFence, err error) {
	rows, err := tx.QueryContext(ctx, `
SELECT member.owner_id, member.generation, member.source_authority, member.runtime_epoch
FROM source_authority_fleet_members member INDEXED BY source_authority_fleet_members_runtime_owner
JOIN source_authority_fleet_heads head
  ON head.owner_id = member.owner_id AND head.generation = member.generation
WHERE member.runtime_owner_digest = ? AND member.runtime_owner_json = ?
ORDER BY member.owner_id, member.generation, member.source_authority`, ownerDigest[:], ownerJSON)
	if err != nil {
		return nil, fmt.Errorf("catalog: select reaped source authority runtimes: %w", err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	closed = make([]SourceAuthorityRuntimeFence, 0, SourceAuthorityRuntimeRecoveryPageLimit)
	for rows.Next() {
		var owner string
		var generation uint64
		var authority string
		var rawEpoch []byte
		if err := rows.Scan(&owner, &generation, &authority, &rawEpoch); err != nil {
			return nil, fmt.Errorf("catalog: scan reaped source authority runtime: %w", err)
		}
		if len(rawEpoch) != 16 {
			return nil, fmt.Errorf("%w: corrupt source authority runtime epoch", ErrIntegrity)
		}
		fence := SourceAuthorityRuntimeFence{
			Owner: SourceAuthorityFleetOwnerID(owner), Generation: causal.Generation(generation),
			Authority: causal.SourceAuthorityID(authority),
		}
		copy(fence.Epoch[:], rawEpoch)
		if err := fence.Validate(); err != nil {
			return nil, errors.Join(ErrIntegrity, err)
		}
		if len(closed) > 0 && compareSourceAuthorityRuntimeFence(closed[len(closed)-1], fence) >= 0 {
			return nil, fmt.Errorf("%w: source authority runtime recovery query is not canonical", ErrIntegrity)
		}
		closed = append(closed, fence)
		if len(closed) > SourceAuthorityRuntimeRecoveryCountLimit {
			return nil, fmt.Errorf("%w: source authority runtime recovery exceeds count limit", ErrIntegrity)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: iterate reaped source authority runtimes: %w", err)
	}
	return closed, nil
}

func readSourceAuthorityRuntimeRecoverySummary(
	ctx context.Context,
	query rowQuerier,
	digest [32]byte,
) (SourceAuthorityRuntimeRecoverySummary, []byte, bool, error) {
	var summary SourceAuthorityRuntimeRecoverySummary
	var rawReceiptDigest, receiptJSON, rawClosedDigest []byte
	err := query.QueryRowContext(ctx, `
SELECT receipt_digest, receipt_json, closed_count, closed_digest
FROM source_authority_runtime_recovery_receipts
WHERE receipt_digest = ?`, digest[:]).Scan(
		&rawReceiptDigest, &receiptJSON, &summary.ClosedCount, &rawClosedDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return summary, nil, false, nil
	}
	if err != nil {
		return summary, nil, false, fmt.Errorf("catalog: read source authority runtime recovery summary: %w", err)
	}
	if len(rawReceiptDigest) != 32 || len(rawClosedDigest) != 32 || len(receiptJSON) == 0 {
		return summary, nil, false, fmt.Errorf("%w: corrupt source authority runtime recovery summary", ErrIntegrity)
	}
	copy(summary.ReceiptDigest[:], rawReceiptDigest)
	copy(summary.ClosedDigest[:], rawClosedDigest)
	return summary, receiptJSON, true, nil
}

func compareSourceAuthorityRuntimeFence(left, right SourceAuthorityRuntimeFence) int {
	if result := bytes.Compare([]byte(left.Owner), []byte(right.Owner)); result != 0 {
		return result
	}
	if left.Generation < right.Generation {
		return -1
	}
	if left.Generation > right.Generation {
		return 1
	}
	return bytes.Compare([]byte(left.Authority), []byte(right.Authority))
}
