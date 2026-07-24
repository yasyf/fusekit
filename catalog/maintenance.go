package catalog

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// MaintenancePageLimit bounds every production catalog-maintenance phase.
const MaintenancePageLimit = 256

const (
	compactMutationAfterSelect = "compact.mutation_after_select"
	compactMutationAfterDelete = "compact.mutation_after_delete"
	maintenanceBlobAfterSelect = "maintenance.blob_after_select"
)

// MaintenancePhase identifies the one bounded phase performed by a step.
type MaintenancePhase uint8

const (
	MaintenanceIdle MaintenancePhase = iota
	MaintenanceAdvanceFloor
	MaintenanceChanges
	MaintenanceMutations
	MaintenanceRetainedIdentities
	MaintenanceSourceHistory
	MaintenanceBrokerAttempts
	MaintenanceFileProviderLeases
	MaintenanceStorageTransitions
	MaintenanceContentStages
	MaintenanceCatalogGenerations
	MaintenanceBlobs
)

// MaintenanceResult reports one bounded tenant-maintenance step.
type MaintenanceResult struct {
	Phase   MaintenancePhase
	Floor   Revision
	Retired int
	More    bool
}

// GlobalMaintenanceResult reports one bounded catalog-wide maintenance step.
type GlobalMaintenanceResult struct {
	Phase                                       MaintenancePhase
	Retired                                     int
	SourceOperations                            int
	PublicationReceipts                         int
	SettlementReceipts                          int
	ConfigurationReceipts                       int
	SourceAuthorityRetirementReceipts           int
	SourceAuthorityFleetAcknowledgementReceipts int
	SourceAuthorityFleetAbortReceipts           int
	SourceAuthorityFleetMembers                 int
	SourceAuthorityFleetGenerations             int
	More                                        bool
}

type tenantMaintenancePhase uint8

const (
	tenantMaintenanceFloor tenantMaintenancePhase = iota + 1
	tenantMaintenanceChanges
	tenantMaintenanceMutations
	tenantMaintenanceRetainedIdentities
	tenantMaintenancePhaseCount = 4
)

// MaintainTenant performs at most one bounded maintenance phase for a tenant.
func (c *Catalog) MaintainTenant(
	ctx context.Context,
	tenant TenantID,
	now time.Time,
) (MaintenanceResult, error) {
	if now.IsZero() {
		return MaintenanceResult{}, fmt.Errorf("%w: maintenance time is zero", ErrInvalidObject)
	}
	for range tenantMaintenancePhaseCount {
		phase, err := c.claimTenantMaintenancePhase(ctx, tenant)
		if err != nil {
			return MaintenanceResult{}, err
		}
		result, err := c.maintainTenantPhase(ctx, tenant, now, phase)
		if err != nil {
			return MaintenanceResult{}, err
		}
		if result.Retired > 0 || result.More {
			result.More = true
			return result, nil
		}
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return MaintenanceResult{}, fmt.Errorf("catalog: begin idle tenant maintenance: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, floor, err := revisionState(ctx, tx, tenant)
	if err != nil {
		return MaintenanceResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return MaintenanceResult{}, fmt.Errorf("catalog: finish idle tenant maintenance: %w", err)
	}
	return MaintenanceResult{Floor: floor}, nil
}

func (c *Catalog) claimTenantMaintenancePhase(
	ctx context.Context,
	tenant TenantID,
) (tenantMaintenancePhase, error) {
	var raw uint8
	err := c.db.QueryRowContext(ctx, `
UPDATE catalog_maintenance
SET next_phase = CASE next_phase WHEN ? THEN 1 ELSE next_phase + 1 END
WHERE tenant = ?
RETURNING CASE next_phase WHEN 1 THEN ? ELSE next_phase - 1 END`,
		tenantMaintenancePhaseCount, string(tenant), tenantMaintenancePhaseCount).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: tenant has no maintenance task", ErrIntegrity)
	}
	if err != nil {
		return 0, fmt.Errorf("catalog: claim tenant maintenance phase: %w", err)
	}
	phase := tenantMaintenancePhase(raw)
	if phase < tenantMaintenanceFloor || phase > tenantMaintenanceRetainedIdentities {
		return 0, fmt.Errorf("%w: invalid tenant maintenance phase", ErrIntegrity)
	}
	return phase, nil
}

func (c *Catalog) maintainTenantPhase(
	ctx context.Context,
	tenant TenantID,
	now time.Time,
	phase tenantMaintenancePhase,
) (MaintenanceResult, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return MaintenanceResult{}, fmt.Errorf("catalog: begin tenant maintenance: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	head, floor, err := revisionState(ctx, tx, tenant)
	if err != nil {
		return MaintenanceResult{}, err
	}
	switch phase {
	case tenantMaintenanceFloor:
		safe, err := safeMaintenanceFloor(ctx, tx, tenant, head, floor, now)
		if err != nil {
			return MaintenanceResult{}, err
		}
		if safe <= floor {
			if err := tx.Commit(); err != nil {
				return MaintenanceResult{}, fmt.Errorf("catalog: finish empty floor maintenance: %w", err)
			}
			return MaintenanceResult{Floor: floor}, nil
		}
		result, err := tx.ExecContext(ctx, `
UPDATE tenants SET floor = ?
WHERE tenant = ? AND floor = ? AND head >= ?`,
			uint64(safe), string(tenant), uint64(floor), uint64(safe))
		if err != nil {
			return MaintenanceResult{}, fmt.Errorf("catalog: advance maintenance floor: %w", err)
		}
		if err := requireOneRow(result, "maintenance floor"); err != nil {
			return MaintenanceResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return MaintenanceResult{}, fmt.Errorf("catalog: commit maintenance floor: %w", err)
		}
		return MaintenanceResult{
			Phase: MaintenanceAdvanceFloor, Floor: safe, Retired: 1, More: true,
		}, nil
	case tenantMaintenanceChanges:
		retired, err := compactChangePage(ctx, tx, tenant, floor)
		if err != nil {
			return MaintenanceResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return MaintenanceResult{}, fmt.Errorf("catalog: commit change maintenance: %w", err)
		}
		if retired == 0 {
			return MaintenanceResult{Floor: floor}, nil
		}
		return MaintenanceResult{
			Phase: MaintenanceChanges, Floor: floor, Retired: retired, More: true,
		}, nil
	case tenantMaintenanceMutations:
		retired, more, err := c.compactCommittedMutations(ctx, tx, tenant, floor)
		if err != nil {
			return MaintenanceResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return MaintenanceResult{}, fmt.Errorf("catalog: commit mutation maintenance: %w", err)
		}
		if retired == 0 && !more {
			return MaintenanceResult{Floor: floor}, nil
		}
		return MaintenanceResult{
			Phase: MaintenanceMutations, Floor: floor, Retired: retired, More: more,
		}, nil
	case tenantMaintenanceRetainedIdentities:
		if err := tx.Commit(); err != nil {
			return MaintenanceResult{}, fmt.Errorf("catalog: prepare retained identity maintenance: %w", err)
		}
		retained, err := c.CollectRetainedIdentityGarbage(ctx, tenant, floor)
		if err != nil {
			return MaintenanceResult{}, err
		}
		total := retained.Handles + retained.MutationPins + retained.Interests +
			retained.ObjectVersions + retained.Objects + retained.Owners
		if total == 0 && !retained.More {
			return MaintenanceResult{Floor: floor}, nil
		}
		return MaintenanceResult{
			Phase: MaintenanceRetainedIdentities, Floor: floor,
			Retired: total, More: retained.More,
		}, nil
	default:
		return MaintenanceResult{}, fmt.Errorf("%w: invalid tenant maintenance phase", ErrIntegrity)
	}
}

func safeMaintenanceFloor(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	head Revision,
	current Revision,
	now time.Time,
) (Revision, error) {
	var applied uint64
	if err := tx.QueryRowContext(ctx, `
SELECT applied_revision
FROM tenant_state
WHERE tenant = ?`, string(tenant)).Scan(&applied); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return current, nil
		}
		return 0, fmt.Errorf("catalog: read maintenance convergence: %w", err)
	}
	target := min(head, Revision(applied))
	var handle, mutation, interest sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
SELECT
    (SELECT MIN(opened_head)
     FROM handles
     WHERE tenant = ? AND closed = 0),
    (SELECT MIN(target_revision)
     FROM mutation_pins
     WHERE tenant = ? AND closed = 0),
    (SELECT MIN(desired_revision)
     FROM materialization_interests
	     WHERE tenant = ? AND removed_revision IS NULL)`,
		string(tenant), string(tenant), string(tenant)).
		Scan(&handle, &mutation, &interest); err != nil {
		return 0, fmt.Errorf("catalog: read maintenance retention anchors: %w", err)
	}
	for _, anchor := range []sql.NullInt64{handle, mutation, interest} {
		if !anchor.Valid {
			continue
		}
		if anchor.Int64 < 0 {
			return 0, fmt.Errorf("%w: negative maintenance anchor", ErrIntegrity)
		}
		target = min(target, Revision(anchor.Int64))
	}
	var liveLeases, matchedLeases uint64
	var leaseObserved sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*), COUNT(engine.presentation_id),
       MIN(COALESCE(engine.observed_catalog_head, 0))
FROM file_provider_leases lease
LEFT JOIN convergence_outbox engine
  ON engine.presentation_id = lease.domain_id
 AND engine.tenant_id = lease.tenant
 AND engine.tenant_generation = lease.generation
 AND engine.state = ?
 AND NOT EXISTS (
     SELECT 1 FROM convergence_outbox newer
     WHERE newer.presentation_id = engine.presentation_id
       AND newer.tenant_id = engine.tenant_id
       AND newer.tenant_generation = engine.tenant_generation
       AND newer.state = ?
       AND newer.expected_activation_revision > engine.expected_activation_revision
 )
WHERE lease.tenant = ? AND lease.expires_unix_nano > ?`,
		uint8(activationOutboxAcked), uint8(activationOutboxAcked), string(tenant), now.UnixNano()).Scan(
		&liveLeases, &matchedLeases, &leaseObserved,
	); err != nil {
		return 0, fmt.Errorf("catalog: read maintenance lease anchors: %w", err)
	}
	if liveLeases > 0 {
		if matchedLeases != liveLeases || !leaseObserved.Valid || leaseObserved.Int64 < 0 {
			target = current
		} else {
			target = min(target, Revision(leaseObserved.Int64))
		}
	}
	if target < current {
		return current, nil
	}
	return target, nil
}

func compactChangePage(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	floor Revision,
) (int, error) {
	rows, err := tx.QueryContext(ctx, `
DELETE FROM changes
WHERE rowid IN (
    SELECT rowid
    FROM changes
    WHERE tenant = ? AND revision < ?
    ORDER BY revision, sequence
    LIMIT ?
)
RETURNING revision`,
		string(tenant), uint64(floor), MaintenancePageLimit)
	if err != nil {
		return 0, fmt.Errorf("catalog: compact change page: %w", err)
	}
	retired := 0
	for rows.Next() {
		var revision uint64
		if err := rows.Scan(&revision); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("catalog: scan compacted change: %w", err)
		}
		retired++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("catalog: read compacted changes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("catalog: close compacted changes: %w", err)
	}
	return retired, nil
}

func (c *Catalog) compactCommittedMutations(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	floor Revision,
) (int, bool, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT journal.mutation_id
FROM mutation_journal journal
LEFT JOIN prepared_mutations prepared
  ON prepared.mutation_id = journal.mutation_id
WHERE journal.tenant = ? AND journal.revision <= ?
  AND (prepared.mutation_id IS NULL OR prepared.state = ?)
  AND NOT EXISTS (
      SELECT 1 FROM mutation_pins pin
      WHERE pin.mutation_id = journal.mutation_id AND pin.closed = 0
  )
  AND NOT EXISTS (
      SELECT 1 FROM source_mutation_expectations expectation
      WHERE expectation.operation_id = journal.mutation_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM source_publication_stage_mutations staged
      WHERE staged.mutation_id = journal.mutation_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM source_commits source_commit
      WHERE source_commit.catalog_operation_id = journal.mutation_id
  )
ORDER BY journal.revision, journal.mutation_id
LIMIT ?`,
		string(tenant), uint64(floor), uint8(MutationCommitted), MaintenancePageLimit+1)
	if err != nil {
		return 0, false, fmt.Errorf("catalog: select committed mutations for compaction: %w", err)
	}
	var ids []MutationID
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return 0, false, fmt.Errorf("catalog: scan compactable mutation: %w", err)
		}
		id, err := mutationID(raw)
		if err != nil {
			_ = rows.Close()
			return 0, false, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, false, fmt.Errorf("catalog: read compactable mutation: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, false, fmt.Errorf("catalog: close compactable mutation: %w", err)
	}
	more := len(ids) > MaintenancePageLimit
	if more {
		ids = ids[:MaintenancePageLimit]
	}
	if err := c.trip(compactMutationAfterSelect); err != nil {
		return 0, false, err
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx,
			"DELETE FROM prepared_mutations WHERE mutation_id = ? AND state = ?",
			id[:], uint8(MutationCommitted)); err != nil {
			return 0, false, fmt.Errorf("catalog: compact prepared mutation: %w", err)
		}
		result, err := tx.ExecContext(ctx,
			"DELETE FROM mutation_journal WHERE mutation_id = ?", id[:])
		if err != nil {
			return 0, false, fmt.Errorf("catalog: compact mutation journal: %w", err)
		}
		if err := requireOneRow(result, "compacted mutation journal"); err != nil {
			return 0, false, err
		}
	}
	if err := c.trip(compactMutationAfterDelete); err != nil {
		return 0, false, err
	}
	return len(ids), more, nil
}

type globalMaintenancePhase uint8

const (
	globalMaintenanceSourceHistory globalMaintenancePhase = iota + 1
	globalMaintenanceBrokerAttempts
	globalMaintenanceFileProviderLeases
	globalMaintenanceStorageTransitions
	globalMaintenanceContentStages
	globalMaintenanceCatalogGenerations
	globalMaintenanceBlobs
	globalMaintenancePhaseCount = 7
)

// MaintainGlobal performs at most one bounded catalog-wide maintenance phase.
func (c *Catalog) MaintainGlobal(
	ctx context.Context,
	now time.Time,
) (GlobalMaintenanceResult, error) {
	if now.IsZero() {
		return GlobalMaintenanceResult{}, fmt.Errorf("%w: maintenance time is zero", ErrInvalidObject)
	}
	for range globalMaintenancePhaseCount {
		phase, err := c.claimGlobalMaintenancePhase(ctx)
		if err != nil {
			return GlobalMaintenanceResult{}, err
		}
		result, err := c.maintainGlobalPhase(ctx, now, phase)
		if err != nil {
			return GlobalMaintenanceResult{}, err
		}
		if result.Retired > 0 || result.More {
			result.More = true
			return result, nil
		}
	}
	return GlobalMaintenanceResult{}, nil
}

func (c *Catalog) claimGlobalMaintenancePhase(ctx context.Context) (globalMaintenancePhase, error) {
	var raw uint8
	err := c.db.QueryRowContext(ctx, `
UPDATE catalog_global_maintenance
SET next_phase = CASE next_phase WHEN ? THEN 1 ELSE next_phase + 1 END
WHERE singleton = 1
RETURNING CASE next_phase WHEN 1 THEN ? ELSE next_phase - 1 END`,
		globalMaintenancePhaseCount, globalMaintenancePhaseCount).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: global maintenance state is missing", ErrIntegrity)
	}
	if err != nil {
		return 0, fmt.Errorf("catalog: claim global maintenance phase: %w", err)
	}
	phase := globalMaintenancePhase(raw)
	if phase < globalMaintenanceSourceHistory || phase > globalMaintenanceBlobs {
		return 0, fmt.Errorf("%w: invalid global maintenance phase", ErrIntegrity)
	}
	return phase, nil
}

func (c *Catalog) maintainGlobalPhase(
	ctx context.Context,
	now time.Time,
	phase globalMaintenancePhase,
) (GlobalMaintenanceResult, error) {
	switch phase {
	case globalMaintenanceSourceHistory:
		return c.maintainSourceHistoryPage(ctx)
	case globalMaintenanceBrokerAttempts:
		attempts, err := c.CompactBrokerCommandAttempts(ctx, MaintenancePageLimit)
		if err != nil {
			return GlobalMaintenanceResult{}, err
		}
		return GlobalMaintenanceResult{
			Phase: MaintenanceBrokerAttempts, Retired: attempts.Attempts, More: attempts.More,
		}, nil
	case globalMaintenanceFileProviderLeases:
		leases, err := c.CompactExpiredFileProviderLeases(ctx, now, MaintenancePageLimit)
		if err != nil {
			return GlobalMaintenanceResult{}, err
		}
		return GlobalMaintenanceResult{
			Phase: MaintenanceFileProviderLeases, Retired: leases.Leases, More: leases.More,
		}, nil
	case globalMaintenanceStorageTransitions:
		recovery, err := c.RecoverStorageTransitions(ctx, MaintenancePageLimit)
		if err != nil {
			return GlobalMaintenanceResult{}, err
		}
		return GlobalMaintenanceResult{
			Phase:   MaintenanceStorageTransitions,
			Retired: recovery.Recovered,
			More:    recovery.More,
		}, nil
	case globalMaintenanceContentStages:
		stages, more, err := c.compactContentStagePage(ctx)
		if err != nil {
			return GlobalMaintenanceResult{}, err
		}
		return GlobalMaintenanceResult{
			Phase: MaintenanceContentStages, Retired: stages, More: more,
		}, nil
	case globalMaintenanceCatalogGenerations:
		generations, err := c.CollectRetiredCatalogGenerations(ctx)
		if err != nil {
			return GlobalMaintenanceResult{}, err
		}
		return GlobalMaintenanceResult{
			Phase:   MaintenanceCatalogGenerations,
			Retired: generations.RetentionOwners + generations.Generations,
			More:    generations.More,
		}, nil
	case globalMaintenanceBlobs:
		blobs, more, err := c.compactBlobCandidatePage(ctx)
		if err != nil {
			return GlobalMaintenanceResult{}, err
		}
		return GlobalMaintenanceResult{
			Phase: MaintenanceBlobs, Retired: blobs, More: more,
		}, nil
	default:
		return GlobalMaintenanceResult{}, fmt.Errorf("%w: invalid global maintenance phase", ErrIntegrity)
	}
}

func (c *Catalog) maintainSourceHistoryPage(
	ctx context.Context,
) (GlobalMaintenanceResult, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return GlobalMaintenanceResult{}, fmt.Errorf("catalog: begin global maintenance: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	publicationRows, publicationMore, err := c.compactSourceDriverPublicationPage(
		ctx, tx, MaintenancePageLimit,
	)
	if err != nil {
		return GlobalMaintenanceResult{}, err
	}
	if publicationRows != 0 || publicationMore {
		if err := tx.Commit(); err != nil {
			return GlobalMaintenanceResult{}, fmt.Errorf("catalog: commit publication maintenance: %w", err)
		}
		return GlobalMaintenanceResult{
			Phase: MaintenanceSourceHistory, Retired: publicationRows, More: publicationMore,
		}, nil
	}
	history, err := compactSettledSourceHistory(ctx, tx, MaintenancePageLimit)
	if err != nil {
		return GlobalMaintenanceResult{}, err
	}
	historyTotal := history.operations + history.publicationReceipts +
		history.settlementReceipts + history.configurationReceipts +
		history.authorityRetirementReceipts + history.fleetAcknowledgementReceipts +
		history.fleetAbortReceipts + history.fleetMembers + history.fleetGenerations
	if err := tx.Commit(); err != nil {
		return GlobalMaintenanceResult{}, fmt.Errorf("catalog: commit source maintenance: %w", err)
	}
	return GlobalMaintenanceResult{
		Phase: MaintenanceSourceHistory, Retired: historyTotal,
		SourceOperations:                            history.operations,
		PublicationReceipts:                         history.publicationReceipts,
		SettlementReceipts:                          history.settlementReceipts,
		ConfigurationReceipts:                       history.configurationReceipts,
		SourceAuthorityRetirementReceipts:           history.authorityRetirementReceipts,
		SourceAuthorityFleetAcknowledgementReceipts: history.fleetAcknowledgementReceipts,
		SourceAuthorityFleetAbortReceipts:           history.fleetAbortReceipts,
		SourceAuthorityFleetMembers:                 history.fleetMembers,
		SourceAuthorityFleetGenerations:             history.fleetGenerations,
		More:                                        history.more,
	}, nil
}

type compactContentStage struct {
	owner     HandleOwnerID
	id        StageID
	tempName  string
	published bool
	hash      ContentHash
}

func (c *Catalog) compactContentStagePage(ctx context.Context) (int, bool, error) {
	var rawOwner []byte
	err := c.readDB.QueryRowContext(ctx, `
SELECT generation.owner_id
FROM catalog_generations generation
WHERE generation.retired = 1
  AND EXISTS (
      SELECT 1
      FROM content_stages stage
      WHERE stage.owner_id = generation.owner_id
        AND stage.mutation_id IS NULL
        AND stage.source_operation_id IS NULL
  )
ORDER BY generation.owner_id
LIMIT 1`).Scan(&rawOwner)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("catalog: select retired generation with content stages: %w", err)
	}
	if len(rawOwner) != len(HandleOwnerID{}) {
		return 0, false, fmt.Errorf("%w: retired stage owner length", ErrIntegrity)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT stage_id, temp_name, published, hash
FROM content_stages
WHERE owner_id = ? AND mutation_id IS NULL AND source_operation_id IS NULL
ORDER BY stage_id
LIMIT ?`, rawOwner, MaintenancePageLimit+1)
	if err != nil {
		return 0, false, fmt.Errorf("catalog: select abandoned content stages: %w", err)
	}
	var stages []compactContentStage
	for rows.Next() {
		var rawID, rawHash []byte
		var stage compactContentStage
		if err := rows.Scan(&rawID, &stage.tempName, &stage.published, &rawHash); err != nil {
			_ = rows.Close()
			return 0, false, fmt.Errorf("catalog: scan abandoned content stage: %w", err)
		}
		if len(rawID) != len(StageID{}) {
			_ = rows.Close()
			return 0, false, fmt.Errorf("%w: abandoned content stage id length", ErrIntegrity)
		}
		copy(stage.id[:], rawID)
		copy(stage.owner[:], rawOwner)
		if stage.published {
			if len(rawHash) != len(ContentHash{}) {
				_ = rows.Close()
				return 0, false, fmt.Errorf("%w: abandoned content hash length", ErrIntegrity)
			}
			copy(stage.hash[:], rawHash)
		} else if rawHash != nil {
			_ = rows.Close()
			return 0, false, fmt.Errorf("%w: unpublished abandoned stage has hash", ErrIntegrity)
		}
		if !validBlobStorageName(stage.tempName) || filepath.Base(stage.tempName) != stage.tempName {
			_ = rows.Close()
			return 0, false, fmt.Errorf("%w: invalid abandoned stage name", ErrIntegrity)
		}
		stages = append(stages, stage)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, false, fmt.Errorf("catalog: read abandoned content stages: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, false, fmt.Errorf("catalog: close abandoned content stages: %w", err)
	}
	more := len(stages) > MaintenancePageLimit
	if more {
		stages = stages[:MaintenancePageLimit]
	}
	for _, stage := range stages {
		if !stage.published {
			if err := c.deleteRetiredTemporaryStage(
				ctx, stage.owner, stage.id, stage.tempName,
			); err != nil {
				return 0, false, fmt.Errorf("catalog: remove abandoned content stage: %w", err)
			}
			continue
		}
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return 0, false, fmt.Errorf("catalog: begin abandoned stage retirement: %w", err)
		}
		result, err := tx.ExecContext(ctx, `
DELETE FROM content_stages
WHERE stage_id = ?
  AND mutation_id IS NULL AND source_operation_id IS NULL
  AND EXISTS (
      SELECT 1
      FROM catalog_generations generation
      WHERE generation.owner_id = content_stages.owner_id
        AND generation.retired = 1
  )`, stage.id[:])
		if err != nil {
			_ = tx.Rollback()
			return 0, false, fmt.Errorf("catalog: retire abandoned content stage: %w", err)
		}
		if err := requireOneRow(result, "abandoned content stage"); err != nil {
			_ = tx.Rollback()
			return 0, false, err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT OR IGNORE INTO blob_gc_candidates(hash) VALUES (?)", stage.hash[:]); err != nil {
			_ = tx.Rollback()
			return 0, false, fmt.Errorf("catalog: enqueue abandoned content blob: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return 0, false, fmt.Errorf("catalog: commit abandoned content stage: %w", err)
		}
	}
	return len(stages), more, nil
}

func (c *Catalog) compactBlobCandidatePage(ctx context.Context) (int, bool, error) {
	rows, err := c.readDB.QueryContext(ctx, `
SELECT hash
FROM blob_gc_candidates
ORDER BY hash
LIMIT ?`, MaintenancePageLimit+1)
	if err != nil {
		return 0, false, fmt.Errorf("catalog: select blob GC candidates: %w", err)
	}
	var hashes []ContentHash
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return 0, false, fmt.Errorf("catalog: scan blob GC candidate: %w", err)
		}
		if len(raw) != len(ContentHash{}) {
			_ = rows.Close()
			return 0, false, fmt.Errorf("%w: blob GC candidate hash length", ErrIntegrity)
		}
		var hash ContentHash
		copy(hash[:], raw)
		hashes = append(hashes, hash)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, false, fmt.Errorf("catalog: read blob GC candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, false, fmt.Errorf("catalog: close blob GC candidates: %w", err)
	}
	more := len(hashes) > MaintenancePageLimit
	if more {
		hashes = hashes[:MaintenancePageLimit]
	}
	if err := c.trip(maintenanceBlobAfterSelect); err != nil {
		return 0, false, err
	}
	for _, hash := range hashes {
		name := hex.EncodeToString(hash[:])
		unlock := c.blobGates.lock(name)
		referenced, err := c.blobEntryReferenced(ctx, name)
		if err != nil {
			unlock()
			return 0, false, err
		}
		if !referenced {
			if _, err := c.deletePublishedBlob(ctx, hash); err != nil {
				unlock()
				return 0, false, fmt.Errorf("catalog: remove unreferenced blob %q: %w", name, err)
			}
		} else if _, err := c.db.ExecContext(ctx,
			"DELETE FROM blob_gc_candidates WHERE hash = ?", hash[:]); err != nil {
			unlock()
			return 0, false, fmt.Errorf("catalog: retire blob GC candidate: %w", err)
		}
		unlock()
	}
	return len(hashes), more, nil
}

func (c *Catalog) blobEntryReferenced(ctx context.Context, name string) (bool, error) {
	if strings.HasPrefix(name, ".stage-") {
		var live bool
		if err := c.readDB.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM content_stages stage
    JOIN catalog_generations generation ON generation.owner_id = stage.owner_id
    WHERE stage.temp_name = ?
      AND (
          generation.retired = 0
          OR stage.mutation_id IS NOT NULL
          OR stage.source_operation_id IS NOT NULL
      )
)`, name).Scan(&live); err != nil {
			return false, fmt.Errorf("catalog: recheck staged file %q: %w", name, err)
		}
		return live, nil
	}
	raw, err := hex.DecodeString(name)
	if err != nil || len(raw) != len(ContentHash{}) {
		return false, fmt.Errorf("catalog: invalid blob name %q", name)
	}
	var live bool
	if err := c.readDB.QueryRowContext(ctx, `
SELECT EXISTS(
    WITH RECURSIVE active_publications(
        source_authority, publication_id, predecessor_publication_id
    ) AS (
        SELECT visibility.source_authority, publication.publication_id,
               publication.predecessor_publication_id
        FROM source_driver_publication_heads visibility
        JOIN source_driver_publications publication
          ON publication.source_authority = visibility.source_authority
         AND publication.publication_id = visibility.publication_id
        UNION
        SELECT predecessor.source_authority, predecessor.publication_id,
               predecessor.predecessor_publication_id
        FROM source_driver_publications predecessor
        JOIN active_publications successor
          ON predecessor.source_authority = successor.source_authority
         AND predecessor.publication_id = successor.predecessor_publication_id
    )
    SELECT 1 FROM object_versions
    WHERE kind = ? AND tombstone = 0 AND hash = ?
    UNION ALL
    SELECT 1
    FROM active_publications active
    JOIN source_driver_publication_objects object
      ON object.source_authority = active.source_authority
     AND object.publication_id = active.publication_id
    WHERE object.kind = ? AND object.tombstone = 0 AND object.hash = ?
    UNION ALL
    SELECT 1
    FROM active_publications active
    JOIN source_driver_publication_versions version
      ON version.source_authority = active.source_authority
     AND version.publication_id = active.publication_id
    WHERE version.kind = ? AND version.tombstone = 0 AND version.hash = ?
    UNION ALL
    SELECT 1
    FROM handles handle
    JOIN source_driver_publication_versions version
      ON version.tenant = handle.tenant
     AND version.object_id = handle.object_id
     AND version.revision = handle.object_revision
    WHERE handle.closed = 0 AND version.kind = ? AND version.tombstone = 0 AND version.hash = ?
    UNION ALL
    SELECT 1
    FROM content_stages stage
    JOIN catalog_generations generation ON generation.owner_id = stage.owner_id
    WHERE stage.published = 1 AND stage.hash = ?
      AND (
          generation.retired = 0
          OR stage.mutation_id IS NOT NULL
          OR stage.source_operation_id IS NOT NULL
      )
)`, uint8(KindFile), raw,
		uint8(KindFile), raw, uint8(KindFile), raw, uint8(KindFile), raw,
		raw).Scan(&live); err != nil {
		return false, fmt.Errorf("catalog: recheck blob %q: %w", name, err)
	}
	return live, nil
}
