package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"

	"github.com/yasyf/fusekit/causal"
)

const sourceObserverSettlementStatementPoint = "source_observer.settlement_statement"

func (c *Catalog) commitNormalizedSourcePublicationStage(
	ctx context.Context,
	tx *sql.Tx,
	expected SourcePublicationStageRef,
) (SourcePublicationStageResult, error) {
	var stageKind uint8
	var predecessor, last uint64
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return SourcePublicationStageResult{}, err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT stage_kind, predecessor_revision, last_revision
FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(expected.Authority), expected.Operation[:],
	).Scan(&stageKind, &predecessor, &last); err != nil {
		return SourcePublicationStageResult{}, err
	}
	if stageKind != 1 || last != uint64(expected.Revision) || last < predecessor || last-predecessor > 1 {
		return SourcePublicationStageResult{}, ErrSourcePredecessor
	}

	var checkpointOwner string
	var checkpointGeneration, checkpointRevision uint64
	var checkpointDeclaration, checkpointOperation, checkpointChange []byte
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return SourcePublicationStageResult{}, err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT fleet_owner_id, authority_generation, declaration_digest, source_revision,
       source_operation_id, change_id
FROM source_driver_checkpoints WHERE source_authority = ?`,
		string(expected.Authority),
	).Scan(
		&checkpointOwner, &checkpointGeneration, &checkpointDeclaration, &checkpointRevision,
		&checkpointOperation, &checkpointChange,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SourcePublicationStageResult{}, ErrSourcePredecessor
		}
		return SourcePublicationStageResult{}, err
	}
	if SourceAuthorityFleetOwnerID(checkpointOwner) != expected.FleetOwner ||
		causal.Generation(checkpointGeneration) != expected.FleetGeneration ||
		len(checkpointDeclaration) != sha256.Size ||
		!bytes.Equal(checkpointDeclaration, expected.DeclarationDigest[:]) ||
		checkpointRevision != last || len(checkpointOperation) != len(causal.OperationID{}) ||
		len(checkpointChange) != len(causal.ChangeID{}) {
		return SourcePublicationStageResult{}, ErrSourcePredecessor
	}

	count := last - predecessor
	first := predecessor
	if count != 0 {
		first++
		var headerPredecessor uint64
		var headerOperation, headerChange []byte
		var complete int
		if err := c.sourceObserverSettlementStatement(); err != nil {
			return SourcePublicationStageResult{}, err
		}
		if err := tx.QueryRowContext(ctx, `
SELECT predecessor_revision, operation_id, change_id, complete
FROM source_publication_stage_revisions
WHERE source_authority = ? AND stage_operation_id = ? AND source_revision = ?`,
			string(expected.Authority), expected.Operation[:], last,
		).Scan(&headerPredecessor, &headerOperation, &headerChange, &complete); err != nil {
			return SourcePublicationStageResult{}, err
		}
		if headerPredecessor != predecessor || complete != 1 ||
			!bytes.Equal(headerOperation, checkpointOperation) || !bytes.Equal(headerChange, checkpointChange) {
			return SourcePublicationStageResult{}, ErrSourcePredecessor
		}
		if err := c.advanceSourceObserverWatermark(
			ctx, tx, expected.Authority, causal.Revision(predecessor), causal.Revision(last),
			headerChange, headerOperation,
		); err != nil {
			return SourcePublicationStageResult{}, err
		}
	} else if err := c.requireSourceObserverWatermark(ctx, tx, expected.Authority, causal.Revision(last)); err != nil {
		return SourcePublicationStageResult{}, err
	}
	if err := c.settleStagedSourceObserver(ctx, tx, expected); err != nil {
		return SourcePublicationStageResult{}, err
	}
	return SourcePublicationStageResult{
		Authority: expected.Authority, FleetOwner: expected.FleetOwner,
		FleetGeneration: expected.FleetGeneration, DriverID: expected.DriverID,
		DeclarationDigest: expected.DeclarationDigest,
		Operation:         expected.Operation, First: causal.Revision(first), Last: causal.Revision(last),
		Count: count, Digest: expected.Digest,
	}, nil
}

func (c *Catalog) advanceSourceObserverWatermark(
	ctx context.Context,
	tx *sql.Tx,
	authority causal.SourceAuthorityID,
	predecessor, revision causal.Revision,
	change, operation []byte,
) error {
	var current uint64
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	err := tx.QueryRowContext(ctx, `
SELECT source_revision FROM source_watermarks WHERE source_authority = ?`,
		string(authority),
	).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		if predecessor != 0 {
			return ErrSourcePredecessor
		}
	} else if err != nil {
		return err
	} else if causal.Revision(current) != predecessor {
		return ErrSourcePredecessor
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO source_watermarks(source_authority, source_revision, change_id, operation_id)
VALUES (?, ?, ?, ?)
ON CONFLICT(source_authority) DO UPDATE SET
    source_revision = excluded.source_revision,
    change_id = excluded.change_id,
    operation_id = excluded.operation_id
WHERE source_watermarks.source_revision = ?`,
		string(authority), uint64(revision), change, operation, uint64(predecessor),
	)
	if err != nil {
		return mapConstraint(err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrSourcePredecessor
	}
	return nil
}

func (c *Catalog) requireSourceObserverWatermark(
	ctx context.Context,
	tx *sql.Tx,
	authority causal.SourceAuthorityID,
	revision causal.Revision,
) error {
	var current uint64
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT source_revision FROM source_watermarks WHERE source_authority = ?`,
		string(authority),
	).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSourcePredecessor
		}
		return err
	}
	if causal.Revision(current) != revision {
		return ErrSourcePredecessor
	}
	return nil
}

func (c *Catalog) settleStagedSourceObserver(
	ctx context.Context,
	tx *sql.Tx,
	expected SourcePublicationStageRef,
) error {
	var streamIdentity, rootEpoch string
	var lastReceived, lastApplied uint64
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT stream_identity, root_epoch, last_received_sequence, last_applied_sequence
FROM source_observer_streams WHERE source_authority = ?`,
		string(expected.Authority)).
		Scan(&streamIdentity, &rootEpoch, &lastReceived, &lastApplied); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	var stagedStream, stagedEpoch string
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT stream_identity, root_epoch FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(expected.Authority), expected.Operation[:]).
		Scan(&stagedStream, &stagedEpoch); err != nil {
		return err
	}
	if streamIdentity != stagedStream || rootEpoch != stagedEpoch ||
		expected.Through > lastReceived || expected.Through < lastApplied {
		return ErrSourceObserverConflict
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_physical_index
WHERE source_authority = ? AND EXISTS (
    SELECT 1 FROM source_publication_stage_index_deletes staged
    WHERE staged.source_authority = source_physical_index.source_authority
      AND staged.stage_operation_id = ?
      AND staged.root_id = source_physical_index.root_id
      AND staged.relative_path = source_physical_index.relative_path
)`, string(expected.Authority), expected.Operation[:]); err != nil {
		return err
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_physical_logical
WHERE source_authority = ? AND EXISTS (
    SELECT 1 FROM source_publication_stage_index staged
    WHERE staged.source_authority = source_physical_logical.source_authority
      AND staged.stage_operation_id = ?
      AND staged.root_id = source_physical_logical.root_id
      AND staged.relative_path = source_physical_logical.relative_path
)`, string(expected.Authority), expected.Operation[:]); err != nil {
		return err
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_physical_index(
    source_authority, root_id, relative_path, file_identity, physical_kind,
    metadata_fingerprint, content_fingerprint, payload
)
SELECT source_authority, root_id, relative_path, file_identity, object_kind,
       metadata_fingerprint, content_fingerprint, payload
FROM source_publication_stage_index
WHERE source_authority = ? AND stage_operation_id = ?
ON CONFLICT(source_authority, root_id, relative_path) DO UPDATE SET
    file_identity = excluded.file_identity,
    physical_kind = excluded.physical_kind,
    metadata_fingerprint = excluded.metadata_fingerprint,
    content_fingerprint = excluded.content_fingerprint,
    payload = excluded.payload`,
		string(expected.Authority), expected.Operation[:]); err != nil {
		return mapConstraint(err)
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_physical_logical(source_authority, logical_id, root_id, relative_path)
SELECT source_authority, logical_id, root_id, relative_path
FROM source_publication_stage_index_logical
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(expected.Authority), expected.Operation[:]); err != nil {
		return mapConstraint(err)
	}
	var invalid int
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM source_publication_stage_bindings staged
LEFT JOIN source_authority_bindings durable
  ON durable.source_authority = staged.source_authority
 AND durable.logical_id = staged.logical_id
 AND durable.source_key = staged.source_key
WHERE staged.source_authority = ? AND staged.stage_operation_id = ?
  AND durable.logical_id IS NULL`,
		string(expected.Authority), expected.Operation[:]).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return ErrSourceObserverConflict
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_authority_bindings AS durable
SET effective_fingerprint = (
    SELECT staged.effective_fingerprint
    FROM source_publication_stage_bindings staged
    WHERE staged.source_authority = durable.source_authority
      AND staged.stage_operation_id = ? AND staged.logical_id = durable.logical_id
)
WHERE durable.source_authority = ? AND EXISTS (
    SELECT 1 FROM source_publication_stage_bindings staged
    WHERE staged.source_authority = durable.source_authority
      AND staged.stage_operation_id = ? AND staged.logical_id = durable.logical_id
      AND staged.source_key = durable.source_key
)`, expected.Operation[:], string(expected.Authority), expected.Operation[:]); err != nil {
		return err
	}
	if err := c.validateStagedMutationSettlements(ctx, tx, expected); err != nil {
		return err
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_mutation_expectations
WHERE source_authority = ? AND state = ? AND EXISTS (
    SELECT 1 FROM source_publication_stage_mutations staged
    WHERE staged.source_authority = source_mutation_expectations.source_authority
      AND staged.stage_operation_id = ? AND staged.mutation_id = source_mutation_expectations.operation_id
      AND staged.matched = 1
)`, string(expected.Authority), SourceMutationExpectationComplete, expected.Operation[:]); err != nil {
		return err
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_mutation_expectations
WHERE source_authority = ? AND state = ? AND EXISTS (
    SELECT 1 FROM source_publication_stage_mutations staged
    WHERE staged.source_authority = source_mutation_expectations.source_authority
      AND staged.stage_operation_id = ? AND staged.mutation_id = source_mutation_expectations.operation_id
      AND staged.matched = 0
)`, string(expected.Authority), SourceMutationExpectationComplete, expected.Operation[:]); err != nil {
		return err
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_mutation_expectations SET state = ?
WHERE source_authority = ? AND state = ? AND EXISTS (
    SELECT 1 FROM source_publication_stage_mutations staged
    WHERE staged.source_authority = source_mutation_expectations.source_authority
      AND staged.stage_operation_id = ? AND staged.mutation_id = source_mutation_expectations.operation_id
      AND staged.matched = 0
)`, SourceMutationExpectationRepairPublished, string(expected.Authority),
		SourceMutationExpectationRepairRequired, expected.Operation[:]); err != nil {
		return err
	}
	return c.settleSourceObserverTx(ctx, tx, SourceObserverSettlement{
		Authority: expected.Authority,
		Stream:    stagedStream,
		RootEpoch: stagedEpoch,
		Through:   expected.Through,
		Operation: expected.Operation,
	})
}

func (c *Catalog) validateStagedMutationSettlements(
	ctx context.Context,
	tx *sql.Tx,
	expected SourcePublicationStageRef,
) error {
	var invalid int
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM source_publication_stage_mutations staged
LEFT JOIN source_mutation_expectations expectation
  ON expectation.source_authority = staged.source_authority
 AND expectation.operation_id = staged.mutation_id
WHERE staged.source_authority = ? AND staged.stage_operation_id = ?
  AND (
      (staged.matched = 1 AND expectation.state <> ?)
      OR
      (staged.matched = 0 AND expectation.state NOT IN (?, ?))
      OR expectation.operation_id IS NULL
  )`,
		string(expected.Authority), expected.Operation[:],
		SourceMutationExpectationComplete, SourceMutationExpectationComplete,
		SourceMutationExpectationRepairRequired).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return ErrSourceObserverConflict
	}
	return nil
}

func (c *Catalog) sourceObserverSettlementStatement() error {
	return c.trip(sourceObserverSettlementStatementPoint)
}
