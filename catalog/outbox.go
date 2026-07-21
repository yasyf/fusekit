package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
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

type outboxClaimState uint8

const (
	outboxClaimActive outboxClaimState = iota + 1
	outboxClaimSettled
)

type outboxClaimRecord struct {
	state            outboxClaimState
	current          causal.OutboxCursor
	lastValid        bool
	lastBefore       causal.OutboxCursor
	settlementDigest []byte
}

// ConvergenceOutboxPageLimit is the hard maximum for one claimed outbox page.
const ConvergenceOutboxPageLimit = 256

func defaultCausalOrigin(kind MutationKind) CausalOrigin {
	switch kind {
	case MutationCreateTenant:
		return CausalOrigin{Cause: causal.CauseBootstrap}
	default:
		panic(fmt.Sprintf("catalog: mutation kind %d requires an explicit causal origin", kind))
	}
}

func validateCausalOrigin(origin CausalOrigin) error {
	change := causal.ChangeSet{
		Cause: origin.Cause, Origin: origin.Domain, OriginGeneration: origin.Generation,
	}
	switch change.Cause {
	case causal.CauseProviderMutation, causal.CauseOnDemand:
		if change.Origin == "" || change.OriginGeneration == 0 {
			return fmt.Errorf("%w: domain-scoped causal origin is incomplete", ErrInvalidObject)
		}
	case causal.CauseDaemonWrite, causal.CauseExternalUnattributed, causal.CauseBootstrap:
		if change.Origin != "" || change.OriginGeneration != 0 {
			return fmt.Errorf("%w: non-provider causal origin carries a domain", ErrInvalidObject)
		}
	default:
		return fmt.Errorf("%w: unknown causal origin %q", ErrInvalidObject, change.Cause)
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
	if change.Cause == causal.CauseOnDemand {
		return fmt.Errorf("%w: on-demand work is not an authoritative source change", ErrInvalidObject)
	}
	if err := validateCausalOrigin(CausalOrigin{
		Cause: change.Cause, Domain: change.Origin, Generation: change.OriginGeneration,
	}); err != nil {
		return err
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
	fingerprint, err := catalogFileProviderFingerprint(ctx, tx, tenant)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_outbox(
    catalog_operation_id, change_id, tenant, catalog_revision, file_provider_fingerprint, state
)
VALUES (?, ?, ?, ?, ?, ?)`, operation[:], change.ChangeID[:], string(tenant), uint64(catalogRevision),
		fingerprint[:], uint8(outboxPending)); err != nil {
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
		OperationID:      causalOperationID(operation),
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

func insertConvergenceChange(ctx context.Context, tx *sql.Tx, change causal.ChangeSet, targets []causal.TenantID, sourceAdvanced bool) error {
	result, err := tx.ExecContext(ctx, `
INSERT INTO convergence_changes(
    change_id, source_operation_id, source_authority, source_revision, cause, origin_domain, origin_generation
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(change_id) DO NOTHING`, change.ChangeID[:], change.OperationID[:], string(change.SourceAuthority), uint64(change.SourceRevision),
		string(change.Cause), string(change.Origin), uint64(change.OriginGeneration))
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
	if inserted == 1 {
		for _, key := range change.AffectedKeys {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_change_affected(change_id, affected_key) VALUES (?, ?)`, change.ChangeID[:], string(key)); err != nil {
				return mapConstraint(err)
			}
		}
		for _, tenant := range targets {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_change_targets(change_id, tenant) VALUES (?, ?)`, change.ChangeID[:], string(tenant)); err != nil {
				return mapConstraint(err)
			}
		}
		return nil
	}
	if sourceAdvanced {
		return fmt.Errorf("%w: generated source change identity already exists", ErrMutationConflict)
	}
	return validateStoredConvergenceChange(ctx, tx, change, targets)
}

// ClaimConvergenceOutbox durably claims the oldest complete source change.
func (c *Catalog) ClaimConvergenceOutbox(ctx context.Context) (*causal.OutboxClaim, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("catalog: begin convergence outbox claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var rawChange []byte
	var headerState uint8
	for _, state := range []convergenceOutboxState{outboxClaimed, outboxPending} {
		err := tx.QueryRowContext(ctx, `
SELECT c.change_id, c.outbox_state
FROM convergence_changes c
WHERE c.outbox_state = ?
ORDER BY c.rowid LIMIT 1`, uint8(state)).Scan(&rawChange, &headerState)
		if err == nil {
			break
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("catalog: select convergence outbox batch: %w", err)
		}
	}
	if len(rawChange) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("catalog: finish empty convergence outbox claim: %w", err)
		}
		return nil, nil
	}
	if len(rawChange) != len(causal.ChangeID{}) {
		return nil, fmt.Errorf("%w: corrupt convergence change identity", ErrIntegrity)
	}
	var changeID causal.ChangeID
	copy(changeID[:], rawChange)
	var missing, extra int
	if err := tx.QueryRowContext(ctx, `SELECT
    EXISTS(
        SELECT 1 FROM convergence_change_targets target
        LEFT JOIN convergence_outbox outbox
          ON outbox.change_id = target.change_id AND outbox.tenant = target.tenant
        WHERE target.change_id = ? AND outbox.tenant IS NULL
    ),
    EXISTS(
        SELECT 1 FROM convergence_outbox outbox
        LEFT JOIN convergence_change_targets target
          ON target.change_id = outbox.change_id AND target.tenant = outbox.tenant
        WHERE outbox.change_id = ? AND target.tenant IS NULL
    )`, changeID[:], changeID[:]).Scan(&missing, &extra); err != nil {
		return nil, fmt.Errorf("catalog: validate convergence outbox claim: %w", err)
	}
	if missing != 0 || extra != 0 {
		return nil, fmt.Errorf("%w: convergence outbox target set is incomplete or corrupt", ErrIntegrity)
	}
	var pending, claimed, settled int
	if err := tx.QueryRowContext(ctx, `
SELECT
    COALESCE(SUM(state = ?), 0),
    COALESCE(SUM(state = ?), 0),
    COALESCE(SUM(state = ?), 0)
FROM convergence_outbox WHERE change_id = ?`,
		uint8(outboxPending), uint8(outboxClaimed), uint8(outboxSettled), changeID[:],
	).Scan(&pending, &claimed, &settled); err != nil {
		return nil, fmt.Errorf("catalog: inspect convergence outbox claim state: %w", err)
	}
	switch convergenceOutboxState(headerState) {
	case outboxPending:
		if claimed != 0 || settled != 0 {
			return nil, fmt.Errorf("%w: pending convergence header has non-pending target rows", ErrIntegrity)
		}
		headerResult, err := tx.ExecContext(ctx, `
UPDATE convergence_changes SET outbox_state = ?
WHERE change_id = ? AND outbox_state = ?`,
			uint8(outboxClaimed), changeID[:], uint8(outboxPending))
		if err != nil {
			return nil, fmt.Errorf("catalog: claim convergence outbox header: %w", err)
		}
		changed, err := headerResult.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("catalog: inspect convergence outbox header claim: %w", err)
		}
		if changed != 1 {
			return nil, fmt.Errorf("%w: convergence outbox header claim raced", ErrInvalidTransition)
		}
		targetResult, err := tx.ExecContext(ctx, `
UPDATE convergence_outbox SET state = ? WHERE change_id = ? AND state = ?`,
			uint8(outboxClaimed), changeID[:], uint8(outboxPending))
		if err != nil {
			return nil, fmt.Errorf("catalog: claim convergence outbox batch: %w", err)
		}
		targetChanged, err := targetResult.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("catalog: inspect convergence outbox target claim: %w", err)
		}
		if targetChanged != int64(pending) {
			return nil, fmt.Errorf("%w: convergence outbox target claim count changed", ErrIntegrity)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_outbox_claims(
    change_id, state, next_sequence, after_key, after_tenant,
    last_valid, last_before_key, last_before_tenant, settlement_digest
) VALUES (?, 1, 0, '', '', 0, '', '', X'')`, changeID[:]); err != nil {
			return nil, fmt.Errorf("catalog: persist convergence outbox claim: %w", mapConstraint(err))
		}
	case outboxClaimed:
		if pending != 0 || settled != 0 {
			return nil, fmt.Errorf("%w: claimed convergence header has non-claimed target rows", ErrIntegrity)
		}
	default:
		return nil, fmt.Errorf("%w: convergence outbox header has invalid claim state", ErrIntegrity)
	}
	record, err := readOutboxClaim(ctx, tx, changeID)
	if err != nil {
		return nil, err
	}
	if record.state != outboxClaimActive {
		return nil, fmt.Errorf("%w: unsettled outbox has a terminal claim", ErrIntegrity)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("catalog: commit convergence outbox claim: %w", err)
	}
	cursor := record.current
	if record.lastValid {
		cursor = record.lastBefore
	}
	return &causal.OutboxClaim{ChangeID: changeID, Cursor: cursor}, nil
}

// PageConvergenceOutbox returns one bounded page of an exact claimed change.
func (c *Catalog) PageConvergenceOutbox(
	ctx context.Context,
	claim causal.OutboxClaim,
) (causal.OutboxPage, error) {
	if claim.ChangeID == (causal.ChangeID{}) {
		return causal.OutboxPage{}, fmt.Errorf("%w: invalid convergence outbox page request", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return causal.OutboxPage{}, fmt.Errorf("catalog: begin convergence outbox page: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, err := readOutboxClaim(ctx, tx, claim.ChangeID)
	if err != nil {
		return causal.OutboxPage{}, err
	}
	if record.state != outboxClaimActive {
		return causal.OutboxPage{}, fmt.Errorf("%w: convergence outbox claim is already settled", ErrInvalidTransition)
	}
	var headerState uint8
	if err := tx.QueryRowContext(ctx, `
SELECT outbox_state FROM convergence_changes WHERE change_id = ?`, claim.ChangeID[:]).Scan(&headerState); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return causal.OutboxPage{}, ErrNotFound
		}
		return causal.OutboxPage{}, fmt.Errorf("catalog: read convergence outbox page header state: %w", err)
	}
	if convergenceOutboxState(headerState) != outboxClaimed {
		return causal.OutboxPage{}, fmt.Errorf("%w: active claim has an unclaimed convergence header", ErrIntegrity)
	}
	current := claim.Cursor == record.current && len(record.settlementDigest) == 0
	replay := record.lastValid && claim.Cursor == record.lastBefore &&
		claim.Cursor.Sequence+1 == record.current.Sequence
	if !current && !replay {
		return causal.OutboxPage{}, fmt.Errorf("%w: convergence outbox cursor is not the current or replayable page", ErrInvalidTransition)
	}
	change, err := readConvergenceChangeHeader(ctx, tx, claim.ChangeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return causal.OutboxPage{}, ErrNotFound
		}
		return causal.OutboxPage{}, err
	}
	keyRows, err := tx.QueryContext(ctx, `
SELECT affected_key FROM convergence_change_affected
WHERE change_id = ? AND affected_key > ?
ORDER BY affected_key LIMIT ?`, claim.ChangeID[:], string(claim.Cursor.AfterKey), ConvergenceOutboxPageLimit+1)
	if err != nil {
		return causal.OutboxPage{}, fmt.Errorf("catalog: query convergence outbox keys: %w", err)
	}
	var keys []causal.LogicalKey
	for keyRows.Next() {
		var key string
		if err := keyRows.Scan(&key); err != nil {
			_ = keyRows.Close()
			return causal.OutboxPage{}, fmt.Errorf("catalog: scan convergence outbox key: %w", err)
		}
		keys = append(keys, causal.LogicalKey(key))
	}
	if err := keyRows.Close(); err != nil {
		return causal.OutboxPage{}, fmt.Errorf("catalog: close convergence outbox keys: %w", err)
	}
	if err := keyRows.Err(); err != nil {
		return causal.OutboxPage{}, fmt.Errorf("catalog: read convergence outbox keys: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `
SELECT outbox.catalog_operation_id, outbox.tenant, outbox.catalog_revision,
       outbox.file_provider_fingerprint, outbox.state,
       EXISTS(
           SELECT 1 FROM mutation_journal mutation
           WHERE mutation.mutation_id = outbox.catalog_operation_id
             AND mutation.tenant = outbox.tenant
             AND mutation.revision = outbox.catalog_revision
           UNION ALL
           SELECT 1 FROM source_commits source_commit
           WHERE source_commit.catalog_operation_id = outbox.catalog_operation_id
             AND source_commit.tenant = outbox.tenant
             AND source_commit.catalog_revision = outbox.catalog_revision
       )
FROM convergence_outbox outbox
WHERE outbox.change_id = ? AND outbox.tenant > ?
ORDER BY outbox.tenant LIMIT ?`, claim.ChangeID[:], string(claim.Cursor.AfterTenant), ConvergenceOutboxPageLimit+1)
	if err != nil {
		return causal.OutboxPage{}, fmt.Errorf("catalog: query convergence outbox commits: %w", err)
	}
	defer func() { _ = rows.Close() }()
	commits := make([]causal.CatalogCommit, 0, ConvergenceOutboxPageLimit+1)
	for rows.Next() {
		var operation []byte
		var tenant string
		var revision uint64
		var rawFingerprint []byte
		var state uint8
		var exact bool
		if err := rows.Scan(&operation, &tenant, &revision, &rawFingerprint, &state, &exact); err != nil {
			return causal.OutboxPage{}, fmt.Errorf("catalog: scan convergence outbox commit: %w", err)
		}
		if len(operation) != len(MutationID{}) || len(rawFingerprint) != len(causal.CatalogCommit{}.FileProviderFingerprint) ||
			convergenceOutboxState(state) != outboxClaimed || !exact {
			return causal.OutboxPage{}, fmt.Errorf("%w: corrupt or unclaimed convergence outbox commit", ErrIntegrity)
		}
		commit := causal.CatalogCommit{Tenant: causal.TenantID(tenant), CatalogRevision: causal.CatalogRevision(revision)}
		copy(commit.FileProviderFingerprint[:], rawFingerprint)
		commits = append(commits, commit)
	}
	if err := rows.Err(); err != nil {
		return causal.OutboxPage{}, fmt.Errorf("catalog: read convergence outbox commits: %w", err)
	}
	page := causal.OutboxPage{Change: change, Commits: commits}
	moreKeys := len(keys) > ConvergenceOutboxPageLimit
	if moreKeys {
		keys = keys[:ConvergenceOutboxPageLimit]
	}
	page.Change.AffectedKeys = keys
	moreCommits := len(page.Commits) > ConvergenceOutboxPageLimit
	if moreCommits {
		page.Commits = page.Commits[:ConvergenceOutboxPageLimit]
	}
	next := claim.Cursor
	next.Sequence++
	if len(keys) != 0 {
		next.AfterKey = keys[len(keys)-1]
	}
	if len(page.Commits) != 0 {
		next.AfterTenant = page.Commits[len(page.Commits)-1].Tenant
	}
	terminal := !moreKeys && !moreCommits
	settlementDigest := []byte{}
	if terminal {
		var keyCount, commitCount uint64
		if err := tx.QueryRowContext(ctx, `SELECT
    (SELECT COUNT(*) FROM convergence_change_affected WHERE change_id = ?),
    (SELECT COUNT(*) FROM convergence_outbox WHERE change_id = ?)`,
			claim.ChangeID[:], claim.ChangeID[:]).Scan(&keyCount, &commitCount); err != nil {
			return causal.OutboxPage{}, fmt.Errorf("catalog: count terminal convergence outbox page: %w", err)
		}
		digest := convergenceOutboxSettlementDigest(change, next, keyCount, commitCount)
		settlementDigest = digest[:]
		page.Settlement = &causal.OutboxSettlement{ChangeID: claim.ChangeID, Digest: digest}
	} else {
		page.Next = &next
	}
	if current {
		result, err := tx.ExecContext(ctx, `
UPDATE convergence_outbox_claims
SET next_sequence = ?, after_key = ?, after_tenant = ?,
    last_valid = 1, last_before_key = ?, last_before_tenant = ?,
    settlement_digest = ?
WHERE change_id = ? AND state = 1
  AND next_sequence = ? AND after_key = ? AND after_tenant = ?
  AND length(settlement_digest) = 0`,
			next.Sequence, string(next.AfterKey), string(next.AfterTenant),
			string(claim.Cursor.AfterKey), string(claim.Cursor.AfterTenant), settlementDigest,
			claim.ChangeID[:], claim.Cursor.Sequence, string(claim.Cursor.AfterKey), string(claim.Cursor.AfterTenant))
		if err != nil {
			return causal.OutboxPage{}, fmt.Errorf("catalog: advance convergence outbox page: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return causal.OutboxPage{}, fmt.Errorf("catalog: inspect convergence outbox page advance: %w", err)
		}
		if changed != 1 {
			return causal.OutboxPage{}, fmt.Errorf("%w: convergence outbox page raced another consumer", ErrInvalidTransition)
		}
	} else {
		if next != record.current || terminal != (len(record.settlementDigest) == len(causal.OutboxSettlement{}.Digest)) {
			return causal.OutboxPage{}, fmt.Errorf("%w: replayed convergence outbox page does not match its durable successor", ErrIntegrity)
		}
		if terminal && !slices.Equal(record.settlementDigest, settlementDigest) {
			return causal.OutboxPage{}, fmt.Errorf("%w: replayed convergence outbox settlement changed", ErrIntegrity)
		}
	}
	if err := tx.Commit(); err != nil {
		return causal.OutboxPage{}, fmt.Errorf("catalog: finish convergence outbox page: %w", err)
	}
	return page, nil
}

// SettleConvergenceOutbox idempotently retires one fully paged claim.
func (c *Catalog) SettleConvergenceOutbox(ctx context.Context, settlement causal.OutboxSettlement) error {
	if settlement.ChangeID == (causal.ChangeID{}) || settlement.Digest == ([32]byte{}) {
		return fmt.Errorf("%w: incomplete convergence outbox settlement", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin convergence outbox settlement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, err := readOutboxClaim(ctx, tx, settlement.ChangeID)
	if err != nil {
		return err
	}
	if len(record.settlementDigest) != len(settlement.Digest) ||
		!slices.Equal(record.settlementDigest, settlement.Digest[:]) {
		return fmt.Errorf("%w: convergence outbox settlement proof mismatch", ErrMutationConflict)
	}
	var headerState uint8
	if err := tx.QueryRowContext(ctx, `
SELECT outbox_state FROM convergence_changes WHERE change_id = ?`, settlement.ChangeID[:]).Scan(&headerState); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("catalog: read convergence outbox header state: %w", err)
	}
	if record.state == outboxClaimSettled {
		if convergenceOutboxState(headerState) != outboxSettled {
			return fmt.Errorf("%w: settled claim has an unsettled convergence header", ErrIntegrity)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("catalog: finish replayed convergence outbox settlement: %w", err)
		}
		return nil
	}
	var pending, claimed, settled int
	if err := tx.QueryRowContext(ctx, `
SELECT
    COALESCE(SUM(state = ?), 0),
    COALESCE(SUM(state = ?), 0),
    COALESCE(SUM(state = ?), 0)
FROM convergence_outbox WHERE change_id = ?`,
		uint8(outboxPending), uint8(outboxClaimed), uint8(outboxSettled), settlement.ChangeID[:],
	).Scan(&pending, &claimed, &settled); err != nil {
		return fmt.Errorf("catalog: inspect terminal convergence outbox claim: %w", err)
	}
	if convergenceOutboxState(headerState) != outboxClaimed || pending != 0 || settled != 0 {
		return fmt.Errorf("%w: terminal convergence outbox is not wholly claimed", ErrIntegrity)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE convergence_outbox SET state = ?
WHERE change_id = ? AND state = ?`,
		uint8(outboxSettled), settlement.ChangeID[:], uint8(outboxClaimed))
	if err != nil {
		return fmt.Errorf("catalog: settle convergence outbox batch: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect convergence outbox settlement: %w", err)
	}
	if changed != int64(claimed) {
		return fmt.Errorf("%w: convergence outbox settlement row count changed", ErrIntegrity)
	}
	headerResult, err := tx.ExecContext(ctx, `
UPDATE convergence_changes SET outbox_state = ?
WHERE change_id = ? AND outbox_state = ?`,
		uint8(outboxSettled), settlement.ChangeID[:], uint8(outboxClaimed))
	if err != nil {
		return fmt.Errorf("catalog: settle convergence outbox header: %w", err)
	}
	headerChanged, err := headerResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect convergence outbox header settlement: %w", err)
	}
	if headerChanged != 1 {
		return fmt.Errorf("%w: convergence outbox header settlement raced", ErrInvalidTransition)
	}
	claimResult, err := tx.ExecContext(ctx, `
UPDATE convergence_outbox_claims SET state = 2
WHERE change_id = ? AND state = 1 AND settlement_digest = ?`,
		settlement.ChangeID[:], settlement.Digest[:])
	if err != nil {
		return fmt.Errorf("catalog: persist convergence outbox settlement: %w", err)
	}
	claimChanged, err := claimResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect convergence outbox proof settlement: %w", err)
	}
	if claimChanged != 1 {
		return fmt.Errorf("%w: convergence outbox proof settlement raced", ErrInvalidTransition)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit convergence outbox settlement: %w", err)
	}
	return nil
}

func readOutboxClaim(ctx context.Context, query rowQuerier, change causal.ChangeID) (outboxClaimRecord, error) {
	var record outboxClaimRecord
	var state uint8
	var nextSequence uint64
	var afterKey, afterTenant, lastBeforeKey, lastBeforeTenant string
	var lastValid bool
	if err := query.QueryRowContext(ctx, `
SELECT state, next_sequence, after_key, after_tenant,
       last_valid, last_before_key, last_before_tenant, settlement_digest
FROM convergence_outbox_claims WHERE change_id = ?`, change[:]).Scan(
		&state, &nextSequence, &afterKey, &afterTenant,
		&lastValid, &lastBeforeKey, &lastBeforeTenant, &record.settlementDigest,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return outboxClaimRecord{}, fmt.Errorf("%w: convergence outbox claim is missing", ErrIntegrity)
		}
		return outboxClaimRecord{}, fmt.Errorf("catalog: read convergence outbox claim: %w", err)
	}
	record.state = outboxClaimState(state)
	record.current = causal.OutboxCursor{
		Sequence: nextSequence, AfterKey: causal.LogicalKey(afterKey), AfterTenant: causal.TenantID(afterTenant),
	}
	record.lastValid = lastValid
	if lastValid {
		if nextSequence == 0 {
			return outboxClaimRecord{}, fmt.Errorf("%w: convergence outbox claim predecessor underflow", ErrIntegrity)
		}
		record.lastBefore = causal.OutboxCursor{
			Sequence: nextSequence - 1, AfterKey: causal.LogicalKey(lastBeforeKey), AfterTenant: causal.TenantID(lastBeforeTenant),
		}
	}
	if record.state != outboxClaimActive && record.state != outboxClaimSettled {
		return outboxClaimRecord{}, fmt.Errorf("%w: invalid convergence outbox claim state", ErrIntegrity)
	}
	if len(record.settlementDigest) != 0 && len(record.settlementDigest) != len(causal.OutboxSettlement{}.Digest) {
		return outboxClaimRecord{}, fmt.Errorf("%w: invalid convergence outbox settlement digest", ErrIntegrity)
	}
	if record.state == outboxClaimSettled && len(record.settlementDigest) == 0 {
		return outboxClaimRecord{}, fmt.Errorf("%w: settled convergence outbox has no proof", ErrIntegrity)
	}
	return record, nil
}

func convergenceOutboxSettlementDigest(
	change causal.ChangeSet,
	cursor causal.OutboxCursor,
	keyCount, commitCount uint64,
) [32]byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte("fusekit.convergence.outbox.settlement.v1\x00"))
	writeOutboxDigestField(digest, change.ChangeID[:])
	writeOutboxDigestField(digest, change.OperationID[:])
	writeOutboxDigestField(digest, []byte(change.SourceAuthority))
	writeOutboxDigestUint64(digest, uint64(change.SourceRevision))
	writeOutboxDigestField(digest, []byte(change.Cause))
	writeOutboxDigestField(digest, []byte(change.Origin))
	writeOutboxDigestUint64(digest, uint64(change.OriginGeneration))
	writeOutboxDigestUint64(digest, cursor.Sequence)
	writeOutboxDigestField(digest, []byte(cursor.AfterKey))
	writeOutboxDigestField(digest, []byte(cursor.AfterTenant))
	writeOutboxDigestUint64(digest, keyCount)
	writeOutboxDigestUint64(digest, commitCount)
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result
}

type outboxDigestWriter interface {
	Write([]byte) (int, error)
}

func writeOutboxDigestField(digest outboxDigestWriter, value []byte) {
	writeOutboxDigestUint64(digest, uint64(len(value)))
	_, _ = digest.Write(value)
}

func writeOutboxDigestUint64(digest outboxDigestWriter, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}

type convergenceQuerier interface {
	rowQuerier
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func readConvergenceChangeHeader(ctx context.Context, query rowQuerier, change causal.ChangeID) (causal.ChangeSet, error) {
	var wrongState, missing, extra bool
	if err := query.QueryRowContext(ctx, `
SELECT EXISTS(
           SELECT 1 FROM convergence_outbox
           WHERE change_id = change.change_id AND state <> ?
       ),
       EXISTS(
           SELECT 1 FROM convergence_change_targets target
           LEFT JOIN convergence_outbox outbox
             ON outbox.change_id = target.change_id AND outbox.tenant = target.tenant
           WHERE target.change_id = change.change_id AND outbox.tenant IS NULL
       ),
       EXISTS(
           SELECT 1 FROM convergence_outbox outbox
           LEFT JOIN convergence_change_targets target
             ON target.change_id = outbox.change_id AND target.tenant = outbox.tenant
           WHERE outbox.change_id = change.change_id AND target.tenant IS NULL
       )
FROM convergence_changes change
WHERE change_id = ?`, uint8(outboxClaimed), change[:]).Scan(
		&wrongState, &missing, &extra,
	); err != nil {
		return causal.ChangeSet{}, err
	}
	if wrongState || missing || extra {
		return causal.ChangeSet{}, fmt.Errorf("%w: corrupt or unclaimed convergence change", ErrIntegrity)
	}
	return readConvergenceChangeMetadata(ctx, query, change)
}

func validateStoredConvergenceChange(
	ctx context.Context,
	query convergenceQuerier,
	change causal.ChangeSet,
	targets []causal.TenantID,
) error {
	stored, err := readConvergenceChangeMetadata(ctx, query, change.ChangeID)
	if err != nil {
		return err
	}
	if !equalConvergenceChangeMetadata(stored, change) {
		return fmt.Errorf("%w: source change identity was reused with different metadata", ErrMutationConflict)
	}
	keyRows, err := query.QueryContext(ctx, `
SELECT affected_key FROM convergence_change_affected WHERE change_id = ? ORDER BY affected_key`, change.ChangeID[:])
	if err != nil {
		return err
	}
	keyIndex := 0
	for keyRows.Next() {
		var key string
		if err := keyRows.Scan(&key); err != nil {
			_ = keyRows.Close()
			return err
		}
		if keyIndex >= len(change.AffectedKeys) || causal.LogicalKey(key) != change.AffectedKeys[keyIndex] {
			_ = keyRows.Close()
			return fmt.Errorf("%w: source change identity was reused with different affected keys", ErrMutationConflict)
		}
		keyIndex++
	}
	if err := keyRows.Err(); err != nil {
		_ = keyRows.Close()
		return err
	}
	if err := keyRows.Close(); err != nil {
		return err
	}
	if keyIndex != len(change.AffectedKeys) {
		return fmt.Errorf("%w: source change identity was reused with different affected keys", ErrMutationConflict)
	}
	targetRows, err := query.QueryContext(ctx, `
SELECT tenant FROM convergence_change_targets WHERE change_id = ? ORDER BY tenant`, change.ChangeID[:])
	if err != nil {
		return err
	}
	targetIndex := 0
	for targetRows.Next() {
		var tenant string
		if err := targetRows.Scan(&tenant); err != nil {
			_ = targetRows.Close()
			return err
		}
		if targetIndex >= len(targets) || causal.TenantID(tenant) != targets[targetIndex] {
			_ = targetRows.Close()
			return fmt.Errorf("%w: source change identity was reused with different targets", ErrMutationConflict)
		}
		targetIndex++
	}
	if err := targetRows.Err(); err != nil {
		_ = targetRows.Close()
		return err
	}
	if err := targetRows.Close(); err != nil {
		return err
	}
	if targetIndex != len(targets) {
		return fmt.Errorf("%w: source change identity was reused with different targets", ErrMutationConflict)
	}
	return nil
}

func equalConvergenceChangeMetadata(left, right causal.ChangeSet) bool {
	return left.SourceAuthority == right.SourceAuthority &&
		left.SourceRevision == right.SourceRevision &&
		left.ChangeID == right.ChangeID &&
		left.OperationID == right.OperationID &&
		left.Cause == right.Cause &&
		left.Origin == right.Origin &&
		left.OriginGeneration == right.OriginGeneration
}

func outboxChangeID(operation MutationID) causal.ChangeID {
	payload := append([]byte("fusekit.catalog.change\x00"), operation[:]...)
	digest := sha256.Sum256(payload)
	var id causal.ChangeID
	copy(id[:], digest[:len(id)])
	return id
}

func causalOperationID(mutation MutationID) causal.OperationID {
	payload := append([]byte("fusekit.catalog.causal-operation\x00"), mutation[:]...)
	digest := sha256.Sum256(payload)
	var id causal.OperationID
	copy(id[:], digest[:len(id)])
	return id
}
