package catalog

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

const sourceDriverStageGCDeleteLimit = 128

type sourceDriverStageGCResult struct {
	Table         string
	Deleted       int
	ParentDeleted bool
	Complete      bool
}

type sourceDriverStageGCTable struct {
	name string
	key  string
}

var sourceDriverStageGCTables = [...]sourceDriverStageGCTable{
	{name: "source_driver_stage_entries", key: "source_authority = ? AND stage_operation_id = ?"},
	{name: "source_publication_stage_affected", key: "source_authority = ? AND stage_operation_id = ?"},
	{name: "source_publication_stage_index_logical", key: "source_authority = ? AND stage_operation_id = ?"},
	{name: "source_publication_stage_index_deletes", key: "source_authority = ? AND stage_operation_id = ?"},
	{name: "source_publication_stage_bindings", key: "source_authority = ? AND stage_operation_id = ?"},
	{name: "source_publication_stage_mutations", key: "source_authority = ? AND stage_operation_id = ?"},
	{name: "source_publication_stage_pages", key: "source_authority = ? AND stage_operation_id = ?"},
	{name: "source_publication_stage_index", key: "source_authority = ? AND stage_operation_id = ?"},
	{name: "source_publication_stage_revisions", key: "source_authority = ? AND stage_operation_id = ?"},
	{name: "source_driver_stage_targets", key: "source_authority = ? AND stage_operation_id = ?"},
	{name: "source_driver_stages", key: "source_authority = ? AND stage_operation_id = ?"},
}

// drainSourceDriverStageRows removes at most one bounded child batch. Its caller
// must first fence the stage terminal and release every claimed content stage.
func (c *Catalog) drainSourceDriverStageRows(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
) (sourceDriverStageGCResult, error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil || operation == (causal.OperationID{}) {
		return sourceDriverStageGCResult{}, fmt.Errorf("%w: invalid source driver stage collection identity", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return sourceDriverStageGCResult{}, fmt.Errorf("catalog: begin source driver stage collection: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	present, err := sourceDriverStageGCParentPresent(ctx, tx, authority, operation)
	if err != nil {
		return sourceDriverStageGCResult{}, err
	}
	if !present {
		if err := tx.Commit(); err != nil {
			return sourceDriverStageGCResult{}, fmt.Errorf("catalog: finish absent source driver stage collection: %w", err)
		}
		return sourceDriverStageGCResult{Complete: true}, nil
	}
	claimed, err := sourceDriverStageGCClaimsContent(ctx, tx, authority, operation)
	if err != nil {
		return sourceDriverStageGCResult{}, err
	}
	if claimed {
		return sourceDriverStageGCResult{}, ErrInvalidTransition
	}

	for _, table := range sourceDriverStageGCTables {
		deleted, err := deleteSourceDriverStageGCBatch(ctx, tx, table, authority, operation)
		if err != nil {
			return sourceDriverStageGCResult{}, err
		}
		if deleted == 0 {
			continue
		}
		if err := tx.Commit(); err != nil {
			return sourceDriverStageGCResult{}, fmt.Errorf("catalog: commit source driver stage %s collection: %w", table.name, err)
		}
		return sourceDriverStageGCResult{Table: table.name, Deleted: deleted}, nil
	}

	result, err := tx.ExecContext(ctx, `
DELETE FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`, string(authority), operation[:])
	if err != nil {
		return sourceDriverStageGCResult{}, fmt.Errorf("catalog: delete empty source driver stage: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return sourceDriverStageGCResult{}, fmt.Errorf("catalog: inspect source driver stage collection: %w", err)
	}
	if deleted != 1 {
		return sourceDriverStageGCResult{}, ErrMutationConflict
	}
	if err := tx.Commit(); err != nil {
		return sourceDriverStageGCResult{}, fmt.Errorf("catalog: commit source driver stage parent collection: %w", err)
	}
	return sourceDriverStageGCResult{
		Table: "source_publication_stages", Deleted: 1, ParentDeleted: true, Complete: true,
	}, nil
}

func sourceDriverStageGCParentPresent(
	ctx context.Context,
	tx *sql.Tx,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
) (bool, error) {
	var present int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_publication_stages
    WHERE source_authority = ? AND stage_operation_id = ?
)`, string(authority), operation[:]).Scan(&present); err != nil {
		return false, fmt.Errorf("catalog: inspect source driver stage collection parent: %w", err)
	}
	return present != 0, nil
}

func sourceDriverStageGCClaimsContent(
	ctx context.Context,
	tx *sql.Tx,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
) (bool, error) {
	var claimed int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM source_driver_stage_entries entry
    JOIN content_stages content ON content.stage_id = entry.content_stage
    WHERE entry.source_authority = ? AND entry.stage_operation_id = ?
      AND entry.content_stage IS NOT NULL AND content.source_operation_id = ?
)`, string(authority), operation[:], operation[:]).Scan(&claimed); err != nil {
		return false, fmt.Errorf("catalog: inspect source driver content claims before collection: %w", err)
	}
	return claimed != 0, nil
}

func deleteSourceDriverStageGCBatch(
	ctx context.Context,
	tx *sql.Tx,
	table sourceDriverStageGCTable,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
) (int, error) {
	statement := "DELETE FROM " + table.name + " WHERE rowid IN (" +
		"SELECT rowid FROM " + table.name + " WHERE " + table.key + " ORDER BY rowid LIMIT ?)"
	result, err := tx.ExecContext(ctx, statement, string(authority), operation[:], sourceDriverStageGCDeleteLimit)
	if err != nil {
		return 0, fmt.Errorf("catalog: collect source driver stage %s: %w", table.name, err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("catalog: inspect source driver stage %s collection: %w", table.name, err)
	}
	if deleted < 0 || deleted > sourceDriverStageGCDeleteLimit {
		return 0, ErrIntegrity
	}
	return int(deleted), nil
}
