package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/yasyf/fusekit/causal"
)

type convergenceOutboxState uint8

const (
	outboxPending convergenceOutboxState = iota + 1
	outboxClaimed
	outboxSettled
)

func defaultCausalOrigin(kind MutationKind) CausalOrigin {
	switch kind {
	case MutationCreateTenant:
		return CausalOrigin{Cause: causal.CauseMigration}
	default:
		panic(fmt.Sprintf("catalog: mutation kind %d requires an explicit causal origin", kind))
	}
}

func validateCausalOrigin(origin CausalOrigin) error {
	change := causal.ChangeSet{
		Cause: origin.Cause, Origin: origin.Domain, OriginGeneration: origin.Generation,
	}
	if origin.Change != nil {
		change = *origin.Change
		if origin.Cause != "" && origin.Cause != change.Cause {
			return fmt.Errorf("%w: causal origin cause changed", ErrInvalidObject)
		}
		if origin.Domain != "" && origin.Domain != change.Origin {
			return fmt.Errorf("%w: causal origin domain changed", ErrInvalidObject)
		}
		if origin.Generation != 0 && origin.Generation != change.OriginGeneration {
			return fmt.Errorf("%w: causal origin generation changed", ErrInvalidObject)
		}
	}
	switch change.Cause {
	case causal.CauseProviderMutation, causal.CauseOnDemand:
		if change.Origin == "" || change.OriginGeneration == 0 {
			return fmt.Errorf("%w: domain-scoped causal origin is incomplete", ErrInvalidObject)
		}
	case causal.CauseDaemonWrite, causal.CauseExternalUnattributed, causal.CauseMigration:
		if change.Origin != "" || change.OriginGeneration != 0 {
			return fmt.Errorf("%w: non-provider causal origin carries a domain", ErrInvalidObject)
		}
	default:
		return fmt.Errorf("%w: unknown causal origin %q", ErrInvalidObject, change.Cause)
	}
	if origin.Change != nil {
		if err := validateSourceChange(change); err != nil {
			return err
		}
	}
	return nil
}

func validateSourceChange(change causal.ChangeSet) error {
	if change.SourceAuthority == "" || change.SourceRevision == 0 || change.ChangeID == (causal.ChangeID{}) || change.OperationID == (causal.OperationID{}) {
		return fmt.Errorf("%w: incomplete source change identity", ErrInvalidObject)
	}
	if len(change.AffectedKeys) == 0 {
		return fmt.Errorf("%w: source change has no affected keys", ErrInvalidObject)
	}
	for index, key := range change.AffectedKeys {
		if key == "" || (index > 0 && change.AffectedKeys[index-1] >= key) {
			return fmt.Errorf("%w: source change keys are not sorted and unique", ErrInvalidObject)
		}
	}
	return nil
}

func insertConvergenceOutbox(
	ctx context.Context,
	tx *sql.Tx,
	operation MutationID,
	tenant TenantID,
	catalogRevision Revision,
	origin CausalOrigin,
) error {
	keys, err := fileProviderChangeKeys(ctx, tx, tenant, catalogRevision)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	if err := validateCausalOrigin(origin); err != nil {
		return err
	}
	change, targets, sourceAdvanced, err := convergenceChange(ctx, tx, operation, tenant, origin, keys)
	if err != nil {
		return err
	}
	if err := insertConvergenceChange(ctx, tx, change, targets, sourceAdvanced); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_outbox(catalog_operation_id, change_id, tenant, catalog_revision, state)
VALUES (?, ?, ?, ?, ?)`, operation[:], change.ChangeID[:], string(tenant), uint64(catalogRevision), uint8(outboxPending)); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func fileProviderChangeKeys(ctx context.Context, tx *sql.Tx, tenant TenantID, revision Revision) ([]causal.LogicalKey, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT object_id FROM changes
WHERE tenant = ? AND revision = ? AND presentation = ?
ORDER BY object_id`, string(tenant), uint64(revision), uint8(PresentationFileProvider))
	if err != nil {
		return nil, fmt.Errorf("catalog: query File Provider convergence changes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var keys []causal.LogicalKey
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("catalog: scan File Provider convergence change: %w", err)
		}
		id, err := objectID(raw)
		if err != nil {
			return nil, err
		}
		keys = append(keys, causal.LogicalKey(fmt.Sprintf("object:%x", id[:])))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: read File Provider convergence changes: %w", err)
	}
	return keys, nil
}

func convergenceChange(
	ctx context.Context,
	tx *sql.Tx,
	operation MutationID,
	tenant TenantID,
	origin CausalOrigin,
	derivedKeys []causal.LogicalKey,
) (causal.ChangeSet, []causal.TenantID, bool, error) {
	if origin.Change != nil {
		change := cloneCausalChange(*origin.Change)
		targets := append([]causal.TenantID(nil), origin.Targets...)
		if err := validateTargetTenants(targets, causal.TenantID(tenant)); err != nil {
			return causal.ChangeSet{}, nil, false, err
		}
		return change, targets, false, nil
	}
	authority, err := sourceAuthorityForTenant(ctx, tx, tenant)
	if err != nil {
		return causal.ChangeSet{}, nil, false, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_source(source_authority, head) VALUES (?, 0)
ON CONFLICT(source_authority) DO NOTHING`, string(authority)); err != nil {
		return causal.ChangeSet{}, nil, false, fmt.Errorf("catalog: initialize convergence source head: %w", err)
	}
	var rawSource uint64
	if err := tx.QueryRowContext(ctx, `
UPDATE convergence_source SET head = head + 1 WHERE source_authority = ? RETURNING head`, string(authority)).Scan(&rawSource); err != nil {
		return causal.ChangeSet{}, nil, false, fmt.Errorf("catalog: allocate convergence source revision: %w", err)
	}
	change := causal.ChangeSet{
		SourceAuthority:  authority,
		SourceRevision:   causal.Revision(rawSource),
		ChangeID:         outboxChangeID(operation),
		OperationID:      causal.OperationID(operation),
		Cause:            origin.Cause,
		Origin:           origin.Domain,
		OriginGeneration: origin.Generation,
		AffectedKeys:     append([]causal.LogicalKey(nil), derivedKeys...),
	}
	return change, []causal.TenantID{causal.TenantID(tenant)}, true, nil
}

func sourceAuthorityForTenant(ctx context.Context, query rowQuerier, tenant TenantID) (causal.SourceAuthorityID, error) {
	_ = ctx
	_ = query
	return causal.SourceAuthorityID("fusekit.local:" + string(tenant)), nil
}

func validateTargetTenants(targets []causal.TenantID, current causal.TenantID) error {
	if len(targets) == 0 || !slices.IsSorted(targets) {
		return fmt.Errorf("%w: source change targets are empty or unsorted", ErrInvalidObject)
	}
	found := false
	for index, target := range targets {
		if target == "" || (index > 0 && targets[index-1] == target) {
			return fmt.Errorf("%w: source change targets are empty or duplicated", ErrInvalidObject)
		}
		found = found || target == current
	}
	if !found {
		return fmt.Errorf("%w: source change does not target tenant %q", ErrInvalidObject, current)
	}
	return nil
}

func insertConvergenceChange(ctx context.Context, tx *sql.Tx, change causal.ChangeSet, targets []causal.TenantID, sourceAdvanced bool) error {
	affected, err := json.Marshal(change.AffectedKeys)
	if err != nil {
		return fmt.Errorf("catalog: encode convergence affected keys: %w", err)
	}
	targetPayload, err := json.Marshal(targets)
	if err != nil {
		return fmt.Errorf("catalog: encode convergence target tenants: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO convergence_changes(
    change_id, source_operation_id, source_authority, source_revision, cause, origin_domain,
    origin_generation, affected_keys_json, target_tenants_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(change_id) DO NOTHING`, change.ChangeID[:], change.OperationID[:], string(change.SourceAuthority), uint64(change.SourceRevision),
		string(change.Cause), string(change.Origin), uint64(change.OriginGeneration), affected, targetPayload)
	if err != nil {
		return mapConstraint(err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect convergence source insertion: %w", err)
	}
	if inserted == 1 && !sourceAdvanced {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_source(source_authority, head) VALUES (?, 0)
ON CONFLICT(source_authority) DO NOTHING`, string(change.SourceAuthority)); err != nil {
			return fmt.Errorf("catalog: initialize convergence source head: %w", err)
		}
		var head uint64
		if err := tx.QueryRowContext(ctx, `
UPDATE convergence_source SET head = ?
WHERE source_authority = ? AND head < ?
RETURNING head`, uint64(change.SourceRevision), string(change.SourceAuthority), uint64(change.SourceRevision)).Scan(&head); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: source revision %d does not advance convergence head", ErrInvalidTransition, change.SourceRevision)
		} else if err != nil {
			return fmt.Errorf("catalog: advance convergence source head: %w", err)
		}
	}
	if inserted == 0 && sourceAdvanced {
		return fmt.Errorf("%w: generated source change identity already exists", ErrMutationConflict)
	}
	stored, storedTargets, err := readConvergenceChange(ctx, tx, change.ChangeID)
	if err != nil {
		return err
	}
	if !equalCausalChange(stored, change) || !slices.Equal(storedTargets, targets) {
		return fmt.Errorf("%w: source change identity was reused with different metadata", ErrMutationConflict)
	}
	return nil
}

// ClaimConvergenceOutbox returns and durably claims the oldest complete source-change batch.
func (c *Catalog) ClaimConvergenceOutbox(ctx context.Context) (*causal.OutboxBatch, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("catalog: begin convergence outbox claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var rawChange []byte
	if err := tx.QueryRowContext(ctx, `
SELECT c.change_id
FROM convergence_changes c
JOIN convergence_outbox o ON o.change_id = c.change_id
WHERE o.state <> ?
ORDER BY c.rowid LIMIT 1`, uint8(outboxSettled)).Scan(&rawChange); errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("catalog: finish empty convergence outbox claim: %w", err)
		}
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("catalog: select convergence outbox batch: %w", err)
	}
	if len(rawChange) != len(causal.ChangeID{}) {
		return nil, fmt.Errorf("%w: corrupt convergence change identity", ErrIntegrity)
	}
	var changeID causal.ChangeID
	copy(changeID[:], rawChange)
	change, targets, err := readConvergenceChange(ctx, tx, changeID)
	if err != nil {
		return nil, err
	}
	commits, states, err := readOutboxCommits(ctx, tx, changeID)
	if err != nil {
		return nil, err
	}
	if !commitTargetsEqual(commits, targets) {
		if len(commits) < len(targets) {
			if err := tx.Commit(); err != nil {
				return nil, fmt.Errorf("catalog: finish incomplete convergence outbox batch: %w", err)
			}
			return nil, nil
		}
		return nil, fmt.Errorf("%w: convergence outbox targets do not match source change", ErrIntegrity)
	}
	for index := range commits {
		if states[index] == outboxSettled {
			return nil, fmt.Errorf("%w: convergence outbox batch is partially settled", ErrIntegrity)
		}
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE convergence_outbox SET state = ? WHERE change_id = ? AND state = ?`,
		uint8(outboxClaimed), changeID[:], uint8(outboxPending)); err != nil {
		return nil, fmt.Errorf("catalog: claim convergence outbox batch: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("catalog: commit convergence outbox claim: %w", err)
	}
	return &causal.OutboxBatch{Change: change, Commits: commits}, nil
}

func readOutboxCommits(ctx context.Context, tx *sql.Tx, change causal.ChangeID) ([]causal.CatalogCommit, []convergenceOutboxState, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT catalog_operation_id, tenant, catalog_revision, state
FROM convergence_outbox WHERE change_id = ? ORDER BY tenant`, change[:])
	if err != nil {
		return nil, nil, fmt.Errorf("catalog: query convergence outbox commits: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var commits []causal.CatalogCommit
	var states []convergenceOutboxState
	for rows.Next() {
		var operation []byte
		var tenant string
		var revision uint64
		var state uint8
		if err := rows.Scan(&operation, &tenant, &revision, &state); err != nil {
			return nil, nil, fmt.Errorf("catalog: scan convergence outbox commit: %w", err)
		}
		if len(operation) != len(MutationID{}) {
			return nil, nil, fmt.Errorf("%w: corrupt catalog operation identity", ErrIntegrity)
		}
		var operationID MutationID
		copy(operationID[:], operation)
		commit := causal.CatalogCommit{Tenant: causal.TenantID(tenant), CatalogRevision: causal.CatalogRevision(revision)}
		if err := validateOutboxCatalogIdentity(ctx, tx, operationID, commit); err != nil {
			return nil, nil, err
		}
		commits = append(commits, commit)
		states = append(states, convergenceOutboxState(state))
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("catalog: read convergence outbox commits: %w", err)
	}
	return commits, states, nil
}

func commitTargetsEqual(commits []causal.CatalogCommit, targets []causal.TenantID) bool {
	if len(commits) != len(targets) {
		return false
	}
	for index := range commits {
		if commits[index].Tenant != targets[index] {
			return false
		}
	}
	return true
}

// SettleConvergenceOutbox idempotently retires one complete causal batch after engine durability.
func (c *Catalog) SettleConvergenceOutbox(ctx context.Context, change causal.ChangeID) error {
	result, err := c.db.ExecContext(ctx, `
UPDATE convergence_outbox SET state = ? WHERE change_id = ? AND state IN (?, ?)`,
		uint8(outboxSettled), change[:], uint8(outboxPending), uint8(outboxClaimed))
	if err != nil {
		return fmt.Errorf("catalog: settle convergence outbox batch: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect convergence outbox settlement: %w", err)
	}
	if changed > 0 {
		return nil
	}
	var unsettled int
	if err := c.readDB.QueryRowContext(ctx, `
SELECT COUNT(*) FROM convergence_outbox WHERE change_id = ? AND state <> ?`,
		change[:], uint8(outboxSettled)).Scan(&unsettled); err != nil {
		return fmt.Errorf("catalog: inspect settled convergence outbox batch: %w", err)
	}
	if unsettled != 0 {
		return fmt.Errorf("%w: convergence outbox batch remains unsettled", ErrInvalidTransition)
	}
	var exists bool
	if err := c.readDB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM convergence_changes WHERE change_id = ?)", change[:]).Scan(&exists); err != nil {
		return fmt.Errorf("catalog: inspect convergence change: %w", err)
	}
	if !exists {
		return ErrNotFound
	}
	return nil
}

func readConvergenceChange(ctx context.Context, query rowQuerier, change causal.ChangeID) (causal.ChangeSet, []causal.TenantID, error) {
	var operation, affected, targets []byte
	var source, generation uint64
	var authority, cause, origin string
	if err := query.QueryRowContext(ctx, `
SELECT source_operation_id, source_authority, source_revision, cause, origin_domain, origin_generation,
       affected_keys_json, target_tenants_json
FROM convergence_changes WHERE change_id = ?`, change[:]).Scan(
		&operation, &authority, &source, &cause, &origin, &generation, &affected, &targets,
	); err != nil {
		return causal.ChangeSet{}, nil, err
	}
	if len(operation) != len(causal.OperationID{}) {
		return causal.ChangeSet{}, nil, fmt.Errorf("%w: corrupt source operation identity", ErrIntegrity)
	}
	var operationID causal.OperationID
	copy(operationID[:], operation)
	var keys []causal.LogicalKey
	var targetTenants []causal.TenantID
	if err := json.Unmarshal(affected, &keys); err != nil {
		return causal.ChangeSet{}, nil, fmt.Errorf("%w: corrupt convergence affected keys", ErrIntegrity)
	}
	if err := json.Unmarshal(targets, &targetTenants); err != nil {
		return causal.ChangeSet{}, nil, fmt.Errorf("%w: corrupt convergence targets", ErrIntegrity)
	}
	if len(targetTenants) == 0 {
		return causal.ChangeSet{}, nil, fmt.Errorf("%w: empty convergence targets", ErrIntegrity)
	}
	result := causal.ChangeSet{
		SourceAuthority: causal.SourceAuthorityID(authority),
		SourceRevision:  causal.Revision(source), ChangeID: change, OperationID: operationID,
		Cause: causal.Cause(cause), Origin: causal.DomainID(origin), OriginGeneration: causal.Generation(generation),
		AffectedKeys: keys,
	}
	if err := validateCausalOrigin(CausalOrigin{Change: &result}); err != nil {
		return causal.ChangeSet{}, nil, fmt.Errorf("%w: corrupt convergence source change", ErrIntegrity)
	}
	if err := validateTargetTenants(targetTenants, targetTenants[0]); err != nil {
		return causal.ChangeSet{}, nil, fmt.Errorf("%w: corrupt convergence target tenants", ErrIntegrity)
	}
	return result, targetTenants, nil
}

func validateOutboxCatalogIdentity(ctx context.Context, query rowQuerier, operation MutationID, commit causal.CatalogCommit) error {
	var exists bool
	if err := query.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM mutation_journal
    WHERE mutation_id = ? AND tenant = ? AND revision = ?
    UNION ALL
    SELECT 1 FROM source_commits
    WHERE catalog_operation_id = ? AND tenant = ? AND catalog_revision = ?
)`, operation[:], string(commit.Tenant), uint64(commit.CatalogRevision),
		operation[:], string(commit.Tenant), uint64(commit.CatalogRevision)).Scan(&exists); err != nil {
		return fmt.Errorf("catalog: validate convergence outbox identity: %w", err)
	}
	if !exists {
		return fmt.Errorf("%w: convergence outbox does not match its catalog mutation", ErrIntegrity)
	}
	return nil
}

func outboxChangeID(operation MutationID) causal.ChangeID {
	payload := append([]byte("fusekit.catalog.change\x00"), operation[:]...)
	digest := sha256.Sum256(payload)
	var id causal.ChangeID
	copy(id[:], digest[:len(id)])
	return id
}

func cloneCausalChange(change causal.ChangeSet) causal.ChangeSet {
	change.AffectedKeys = append([]causal.LogicalKey(nil), change.AffectedKeys...)
	return change
}

func equalCausalChange(left, right causal.ChangeSet) bool {
	return left.SourceAuthority == right.SourceAuthority && left.SourceRevision == right.SourceRevision && left.ChangeID == right.ChangeID &&
		left.OperationID == right.OperationID && left.Cause == right.Cause && left.Origin == right.Origin &&
		left.OriginGeneration == right.OriginGeneration && slices.Equal(left.AffectedKeys, right.AffectedKeys)
}
