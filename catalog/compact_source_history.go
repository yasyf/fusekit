package catalog

import (
	"context"
	"database/sql"
	"fmt"
)

const sourceHistoryCompactionPage = 256

type sourceHistoryCompactionResult struct {
	operations                   int
	publicationReceipts          int
	driverReceipts               int
	settlementReceipts           int
	configurationReceipts        int
	authorityRetirementReceipts  int
	fleetAcknowledgementReceipts int
	fleetAbortReceipts           int
	runtimeRecoveryReceipts      int
	fleetMembers                 int
	fleetGenerations             int
	more                         bool
}

func compactSettledSourceHistory(
	ctx context.Context,
	tx *sql.Tx,
	limit int,
) (result sourceHistoryCompactionResult, resultErr error) {
	if limit < 1 {
		return sourceHistoryCompactionResult{}, fmt.Errorf("%w: invalid source history compaction limit", ErrInvalidObject)
	}
	if _, err := tx.ExecContext(ctx, `
DROP TABLE IF EXISTS temp.fusekit_compact_source_operations;
CREATE TEMP TABLE fusekit_compact_source_operations(operation_id BLOB PRIMARY KEY) WITHOUT ROWID;`); err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: create source history compaction pages: %w", err)
	}
	defer func() {
		_, cleanupErr := tx.ExecContext(ctx, `DROP TABLE IF EXISTS temp.fusekit_compact_source_operations;`)
		if resultErr == nil && cleanupErr != nil {
			resultErr = fmt.Errorf("catalog: drop source history compaction pages: %w", cleanupErr)
		}
	}()
	if err := ctx.Err(); err != nil {
		return sourceHistoryCompactionResult{}, err
	}
	remaining := limit
	if _, err := tx.ExecContext(ctx, `
INSERT INTO temp.fusekit_compact_source_operations(operation_id)
SELECT operation.operation_id
FROM source_operations operation
WHERE NOT EXISTS (
    SELECT 1 FROM source_watermarks watermark WHERE watermark.operation_id = operation.operation_id
)
AND NOT EXISTS (
    SELECT 1
    FROM source_commits source_commit
    JOIN tenants tenant ON tenant.tenant = source_commit.tenant
    WHERE source_commit.source_operation_id = operation.operation_id
      AND source_commit.catalog_revision >= tenant.floor
)
ORDER BY operation.source_authority, operation.source_revision
LIMIT ?`, remaining); err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: select compactable source operations: %w", err)
	}
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM temp.fusekit_compact_source_operations`).Scan(&result.operations); err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: count source operation compaction page: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_commits
WHERE source_operation_id IN (SELECT operation_id FROM temp.fusekit_compact_source_operations);
DELETE FROM source_operations
WHERE operation_id IN (SELECT operation_id FROM temp.fusekit_compact_source_operations);`); err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: compact settled source history page: %w", err)
	}
	result.more = result.more || result.operations == remaining
	remaining -= result.operations
	if remaining == 0 {
		return result, nil
	}
	deleted, err := tx.ExecContext(ctx, `
DELETE FROM source_publication_stage_receipts
WHERE rowid IN (
    SELECT receipt.rowid
    FROM source_publication_stage_receipts receipt
    WHERE EXISTS (
        SELECT 1 FROM source_publication_stage_receipts newer
        WHERE newer.source_authority = receipt.source_authority
          AND newer.rowid > receipt.rowid
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_operations operation
        WHERE operation.source_authority = receipt.source_authority
          AND operation.source_revision BETWEEN receipt.first_revision AND receipt.last_revision
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_watermarks watermark
        WHERE watermark.source_authority = receipt.source_authority
          AND watermark.source_revision BETWEEN receipt.first_revision AND receipt.last_revision
    )
    ORDER BY receipt.source_authority, receipt.last_revision, receipt.rowid
    LIMIT ?
)`, remaining)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: compact source publication receipts: %w", err)
	}
	result.publicationReceipts, err = rowsAffectedInt(deleted)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: count compacted source publication receipts: %w", err)
	}
	result.more = result.more || result.publicationReceipts == remaining
	remaining -= result.publicationReceipts
	if remaining == 0 {
		return result, nil
	}
	deleted, err = tx.ExecContext(ctx, `
DELETE FROM source_driver_stage_receipts
WHERE rowid IN (
    SELECT receipt.rowid
    FROM source_driver_stage_receipts receipt
    WHERE receipt.forgotten = 1
      AND EXISTS (
          SELECT 1 FROM source_driver_stage_receipts newer
          WHERE newer.source_authority = receipt.source_authority
            AND newer.rowid > receipt.rowid
      )
      AND NOT EXISTS (
          SELECT 1
          FROM source_driver_publication_targets target
          JOIN tenants tenant ON tenant.tenant = target.tenant
          WHERE target.source_authority = receipt.source_authority
            AND target.publication_id = receipt.stage_operation_id
            AND target.catalog_head >= tenant.floor
      )
    ORDER BY receipt.source_authority, receipt.source_revision, receipt.rowid
    LIMIT ?
)`, remaining)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: compact source driver receipts: %w", err)
	}
	result.driverReceipts, err = rowsAffectedInt(deleted)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: count compacted source driver receipts: %w", err)
	}
	result.more = result.more || result.driverReceipts == remaining
	remaining -= result.driverReceipts
	if remaining == 0 {
		return result, nil
	}
	deleted, err = tx.ExecContext(ctx, `
DELETE FROM source_observer_settlement_receipts
WHERE rowid IN (
    SELECT receipt.rowid
    FROM source_observer_settlement_receipts receipt
    WHERE receipt.acknowledged = 1
      AND EXISTS (
        SELECT 1 FROM source_observer_settlement_receipts newer
        WHERE newer.source_authority = receipt.source_authority
          AND newer.rowid > receipt.rowid
    )
    ORDER BY receipt.source_authority, receipt.rowid
    LIMIT ?
)`, remaining)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: compact source observer settlement receipts: %w", err)
	}
	result.settlementReceipts, err = rowsAffectedInt(deleted)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: count compacted source observer settlement receipts: %w", err)
	}
	result.more = result.more || result.settlementReceipts == remaining
	remaining -= result.settlementReceipts
	if remaining == 0 {
		return result, nil
	}
	deleted, err = tx.ExecContext(ctx, `
DELETE FROM source_observer_configuration_receipts
WHERE rowid IN (
    SELECT receipt.rowid
    FROM source_observer_configuration_receipts receipt
    WHERE receipt.acknowledged = 1
      AND (
        EXISTS (
            SELECT 1 FROM source_observer_configuration_receipts newer
            WHERE newer.source_authority = receipt.source_authority
              AND newer.rowid > receipt.rowid
        )
        OR EXISTS (
            SELECT 1
            FROM source_authority_fleet_heads head
            JOIN source_authority_fleets fleet
              ON fleet.owner_id = head.owner_id
             AND fleet.generation = head.generation
            JOIN source_authority_fleet_ack_receipts acknowledgement
              ON acknowledgement.owner_id = fleet.owner_id
             AND acknowledgement.generation = fleet.generation
             AND acknowledgement.authority_count = fleet.authority_count
             AND acknowledgement.authorities_digest = fleet.authorities_digest
             AND acknowledgement.declarations_digest = fleet.declarations_digest
             AND acknowledgement.acknowledgement_digest = fleet.acknowledgement_digest
            WHERE head.owner_id = receipt.fleet_owner_id
              AND head.generation > receipt.fleet_generation
              AND NOT EXISTS (
                  SELECT 1
                  FROM source_authority_fleet_members current_member
                  WHERE current_member.owner_id = head.owner_id
                    AND current_member.generation = head.generation
                    AND current_member.source_authority = receipt.source_authority
              )
        )
    )
    ORDER BY receipt.source_authority, receipt.rowid
    LIMIT ?
)`, remaining)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: compact source observer configuration receipts: %w", err)
	}
	result.configurationReceipts, err = rowsAffectedInt(deleted)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: count compacted source observer configuration receipts: %w", err)
	}
	result.more = result.more || result.configurationReceipts == remaining
	remaining -= result.configurationReceipts
	if remaining == 0 {
		return result, nil
	}
	deleted, err = tx.ExecContext(ctx, `
DELETE FROM source_authority_retirement_receipts
WHERE rowid IN (
    SELECT receipt.rowid
    FROM source_authority_retirement_receipts receipt
    WHERE EXISTS (
        SELECT 1
        FROM source_authority_fleet_ack_receipts acknowledgement
        WHERE acknowledgement.owner_id = receipt.owner_id
          AND acknowledgement.generation = receipt.generation
          AND acknowledgement.expected_generation = receipt.expected_generation
          AND acknowledgement.stage_digest = receipt.stage_digest
    )
    ORDER BY receipt.owner_id, receipt.generation, receipt.source_authority
    LIMIT ?
)`, remaining)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: compact source authority retirement receipts: %w", err)
	}
	result.authorityRetirementReceipts, err = rowsAffectedInt(deleted)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: count compacted source authority retirement receipts: %w", err)
	}
	result.more = result.more || result.authorityRetirementReceipts == remaining
	remaining -= result.authorityRetirementReceipts
	if remaining == 0 {
		return result, nil
	}
	deleted, err = tx.ExecContext(ctx, `
DELETE FROM source_authority_fleet_ack_receipts
WHERE rowid IN (
    SELECT receipt.rowid
    FROM source_authority_fleet_ack_receipts receipt
    WHERE EXISTS (
        SELECT 1
        FROM source_authority_fleet_ack_receipts newer
        WHERE newer.owner_id = receipt.owner_id
          AND newer.generation > receipt.generation
    )
      AND NOT EXISTS (
        SELECT 1
        FROM source_authority_retirement_receipts retirement
        WHERE retirement.owner_id = receipt.owner_id
          AND retirement.generation = receipt.generation
    )
    ORDER BY receipt.owner_id, receipt.generation
    LIMIT ?
)`, remaining)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: compact source authority fleet acknowledgement receipts: %w", err)
	}
	result.fleetAcknowledgementReceipts, err = rowsAffectedInt(deleted)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: count compacted source authority fleet acknowledgement receipts: %w", err)
	}
	result.more = result.more || result.fleetAcknowledgementReceipts == remaining
	remaining -= result.fleetAcknowledgementReceipts
	if remaining == 0 {
		return result, nil
	}
	deleted, err = tx.ExecContext(ctx, `
DELETE FROM source_authority_fleet_abort_receipts
WHERE rowid IN (
    SELECT receipt.rowid
    FROM source_authority_fleet_abort_receipts receipt
    JOIN source_authority_fleet_heads head
      ON head.owner_id = receipt.owner_id
     AND head.generation > receipt.expected_generation
    JOIN source_authority_fleets fleet
      ON fleet.owner_id = head.owner_id AND fleet.generation = head.generation
    JOIN source_authority_fleet_ack_receipts acknowledgement
      ON acknowledgement.owner_id = head.owner_id
     AND acknowledgement.generation = head.generation
     AND acknowledgement.authority_count = fleet.authority_count
     AND acknowledgement.authorities_digest = fleet.authorities_digest
     AND acknowledgement.declarations_digest = fleet.declarations_digest
     AND acknowledgement.acknowledgement_digest = fleet.acknowledgement_digest
    ORDER BY receipt.owner_id, receipt.generation
    LIMIT ?
)`, remaining)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: compact source authority fleet abort receipts: %w", err)
	}
	result.fleetAbortReceipts, err = rowsAffectedInt(deleted)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: count compacted source authority fleet abort receipts: %w", err)
	}
	result.more = result.more || result.fleetAbortReceipts == remaining
	remaining -= result.fleetAbortReceipts
	if remaining == 0 {
		return result, nil
	}
	deleted, err = tx.ExecContext(ctx, `
DELETE FROM source_authority_runtime_recovery_receipts
WHERE receipt_digest IN (
    SELECT receipt.receipt_digest
    FROM source_authority_runtime_recovery_receipts receipt
    JOIN source_authority_runtime_recovery_floors floor
      ON floor.ledger_id = receipt.ledger_id
     AND receipt.sequence <= floor.acknowledged_sequence
    ORDER BY receipt.sequence
    LIMIT ?
)`, remaining)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: compact source authority runtime recovery receipts: %w", err)
	}
	result.runtimeRecoveryReceipts, err = rowsAffectedInt(deleted)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: count compacted source authority runtime recovery receipts: %w", err)
	}
	result.more = result.more || result.runtimeRecoveryReceipts == remaining
	remaining -= result.runtimeRecoveryReceipts
	if remaining == 0 {
		return result, nil
	}
	deleted, err = tx.ExecContext(ctx, `
DELETE FROM source_authority_fleet_members
WHERE rowid IN (
    SELECT member.rowid
    FROM source_authority_fleet_members member
    WHERE EXISTS (
        SELECT 1
        FROM source_authority_fleet_heads head
        WHERE head.owner_id = member.owner_id
          AND head.generation > member.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_observer_streams stream
        WHERE stream.fleet_owner_id = member.owner_id
          AND stream.fleet_generation = member.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_observer_configuration_stages stage
        WHERE stage.fleet_owner_id = member.owner_id
          AND stage.fleet_generation = member.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_observer_configuration_receipts receipt
        WHERE receipt.fleet_owner_id = member.owner_id
          AND receipt.fleet_generation = member.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_driver_checkpoints checkpoint
        WHERE checkpoint.fleet_owner_id = member.owner_id
          AND checkpoint.authority_generation = member.generation
          AND checkpoint.source_authority = member.source_authority
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_driver_stages stage
        WHERE stage.fleet_owner_id = member.owner_id
          AND stage.authority_generation = member.generation
          AND stage.source_authority = member.source_authority
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_fleet_ack_receipts receipt
        WHERE receipt.owner_id = member.owner_id
          AND receipt.generation = member.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_retirement_receipts receipt
        WHERE receipt.owner_id = member.owner_id
          AND receipt.expected_generation = member.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_retirement_receipts receipt
        WHERE receipt.owner_id = member.owner_id
          AND receipt.generation = member.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_fleet_abort_receipts receipt
        WHERE receipt.owner_id = member.owner_id
          AND receipt.expected_generation = member.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_fleet_abort_receipts receipt
        WHERE receipt.owner_id = member.owner_id
          AND receipt.generation = member.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_claims claim
        WHERE claim.owner_id = member.owner_id
          AND claim.current_generation = member.generation
    )
    ORDER BY member.owner_id, member.generation, member.source_authority
    LIMIT ?
)`, remaining)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: compact source authority fleet members: %w", err)
	}
	result.fleetMembers, err = rowsAffectedInt(deleted)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: count compacted source authority fleet members: %w", err)
	}
	result.more = result.more || result.fleetMembers == remaining
	remaining -= result.fleetMembers
	if remaining == 0 {
		return result, nil
	}
	deleted, err = tx.ExecContext(ctx, `
DELETE FROM source_authority_fleets
WHERE rowid IN (
    SELECT fleet.rowid
    FROM source_authority_fleets fleet
    WHERE EXISTS (
        SELECT 1
        FROM source_authority_fleet_heads head
        WHERE head.owner_id = fleet.owner_id
          AND head.generation > fleet.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_fleet_members member
        WHERE member.owner_id = fleet.owner_id
          AND member.generation = fleet.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_observer_streams stream
        WHERE stream.fleet_owner_id = fleet.owner_id
          AND stream.fleet_generation = fleet.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_observer_configuration_stages stage
        WHERE stage.fleet_owner_id = fleet.owner_id
          AND stage.fleet_generation = fleet.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_observer_configuration_receipts receipt
        WHERE receipt.fleet_owner_id = fleet.owner_id
          AND receipt.fleet_generation = fleet.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_fleet_ack_receipts receipt
        WHERE receipt.owner_id = fleet.owner_id
          AND receipt.generation = fleet.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_retirement_receipts receipt
        WHERE receipt.owner_id = fleet.owner_id
          AND receipt.expected_generation = fleet.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_retirement_receipts receipt
        WHERE receipt.owner_id = fleet.owner_id
          AND receipt.generation = fleet.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_fleet_abort_receipts receipt
        WHERE receipt.owner_id = fleet.owner_id
          AND receipt.expected_generation = fleet.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_fleet_abort_receipts receipt
        WHERE receipt.owner_id = fleet.owner_id
          AND receipt.generation = fleet.generation
    )
      AND NOT EXISTS (
        SELECT 1 FROM source_authority_claims claim
        WHERE claim.owner_id = fleet.owner_id
          AND claim.current_generation = fleet.generation
    )
    ORDER BY fleet.owner_id, fleet.generation
    LIMIT ?
)`, remaining)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: compact source authority fleet generations: %w", err)
	}
	result.fleetGenerations, err = rowsAffectedInt(deleted)
	if err != nil {
		return sourceHistoryCompactionResult{}, fmt.Errorf("catalog: count compacted source authority fleet generations: %w", err)
	}
	result.more = result.more || result.fleetGenerations == remaining
	return result, nil
}

func rowsAffectedInt(result sql.Result) (int, error) {
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if count < 0 || int64(int(count)) != count {
		return 0, ErrIntegrity
	}
	return int(count), nil
}
