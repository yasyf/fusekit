package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/yasyf/fusekit/causal"
)

const (
	sourceDriverPreparationPageLimit = 128
	sourceDriverPreparationByteLimit = 1 << 20
	sourceDriverObjectPageLimit      = 24

	sourceDriverPublicationInitializing uint8 = iota + 1
	sourceDriverPublicationPreparing
	sourceDriverPublicationContent
	sourceDriverPublicationPrepared
)

const (
	sourceDriverTargetRoot uint8 = iota + 1
	sourceDriverTargetObjects
	sourceDriverTargetVersions
	sourceDriverTargetBaselineChanges
	sourceDriverTargetValidate
	sourceDriverTargetCatalogFingerprint
	sourceDriverTargetProviderFingerprint
	sourceDriverTargetChanges
	sourceDriverTargetInterestChanges
	sourceDriverTargetPrepared
)

const sourceDriverPreparationAfterBatchPoint = "source_driver.after_preparation_batch"

// SourceDriverPreparationState describes one durable authority publication preparation step.
type SourceDriverPreparationState struct {
	Authority               string
	Publication             [16]byte
	SourceRevision          uint64
	ExpectedVisibilityEpoch uint64
	TargetEpoch             uint64
	Phase                   uint8
	Target                  TenantID
	TargetPhase             uint8
	Rows                    uint64
	Bytes                   uint64
	PreparedTargets         uint64
	TargetCount             uint64
	Digest                  [sha256.Size]byte
	Prepared                bool
	Published               bool
}

// SourceDriverTargetDeclarationState is one durable target declaration checkpoint.
type SourceDriverTargetDeclarationState struct {
	TargetEpoch   uint64
	DeclaredCount uint64
	TargetCount   uint64
	Digest        [sha256.Size]byte
	Prepared      bool
}

type sourceDriverPreparedTarget struct {
	tenant                  TenantID
	generation              Generation
	root                    SourceObjectKey
	predecessorHead         Revision
	catalogHead             Revision
	phase                   uint8
	cursorKey               string
	cursorObject            []byte
	cursorRevision          Revision
	catalogFingerprint      [sha256.Size]byte
	fileProviderFingerprint [sha256.Size]byte
	changed                 bool
	providerChanged         bool
	objectCount             uint64
	nextChangeSequence      uint64
}

type sourceDriverPreparedObject struct {
	key     SourceObjectKey
	nameKey string
	object  Object
}

// PrepareSourceDriverTargetDeclarationBatch persists at most one bounded target page.
func (c *Catalog) PrepareSourceDriverTargetDeclarationBatch(
	ctx context.Context,
	identity SourceDriverStageIdentity,
) (SourceDriverTargetDeclarationState, error) {
	if _, err := validateSourceDriverStageIdentity(identity); err != nil {
		return SourceDriverTargetDeclarationState{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceDriverTargetDeclarationState{}, err
	}
	defer func() { _ = tx.Rollback() }()
	state := SourceDriverPreparationState{TargetCount: identity.TargetCount}
	if _, err := c.prepareSourceDriverStageTargetPage(ctx, tx, identity, &state); err != nil {
		return SourceDriverTargetDeclarationState{}, err
	}
	var digest []byte
	var prepared int
	result := SourceDriverTargetDeclarationState{TargetCount: identity.TargetCount}
	if err := tx.QueryRowContext(ctx, `
SELECT target_epoch, declared_target_count, target_digest_state, targets_prepared
FROM source_driver_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(identity.Authority), identity.Operation[:],
	).Scan(&result.TargetEpoch, &result.DeclaredCount, &digest, &prepared); err != nil {
		return SourceDriverTargetDeclarationState{}, err
	}
	if result.TargetEpoch == 0 || result.DeclaredCount > result.TargetCount || len(digest) != sha256.Size {
		return SourceDriverTargetDeclarationState{}, ErrIntegrity
	}
	copy(result.Digest[:], digest)
	result.Prepared = prepared != 0
	if err := tx.Commit(); err != nil {
		return SourceDriverTargetDeclarationState{}, err
	}
	if err := c.trip(sourceDriverPreparationAfterBatchPoint); err != nil {
		return SourceDriverTargetDeclarationState{}, err
	}
	return result, nil
}

// SourceDriverStageTargets pages the exact persisted target declaration for one stage.
func (c *Catalog) SourceDriverStageTargets(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	after TenantID,
	limit int,
) (targets []SourceDriverTarget, err error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil || operation == (causal.OperationID{}) ||
		limit < 1 || limit > sourceDriverTargetBatchSize {
		return nil, ErrInvalidTransition
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT tenant, generation
FROM source_driver_stage_targets
WHERE source_authority = ? AND stage_operation_id = ? AND tenant > ?
ORDER BY tenant LIMIT ?`, string(authority), operation[:], string(after), limit)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	targets = make([]SourceDriverTarget, 0, limit)
	previous := after
	for rows.Next() {
		var tenant string
		var generation uint64
		if err := rows.Scan(&tenant, &generation); err != nil {
			return nil, err
		}
		target := SourceDriverTarget{Tenant: TenantID(tenant), Generation: Generation(generation)}
		if target.Tenant <= previous || target.Generation == 0 {
			return nil, ErrIntegrity
		}
		targets = append(targets, target)
		previous = target.Tenant
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		var exists int
		if err := c.readDB.QueryRowContext(ctx, `
SELECT 1 FROM source_driver_stages WHERE source_authority = ? AND stage_operation_id = ?`,
			string(authority), operation[:],
		).Scan(&exists); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

// PrepareSourceDriverPublicationBatch advances at most one bounded durable preparation page.
// It never changes an authority visibility pointer, tenant head, current object, or convergence row.
func (c *Catalog) PrepareSourceDriverPublicationBatch(
	ctx context.Context,
	identity SourceDriverStageIdentity,
) (SourceDriverPreparationState, error) {
	identityDigest, err := validateSourceDriverStageIdentity(identity)
	if err != nil {
		return SourceDriverPreparationState{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceDriverPreparationState{}, fmt.Errorf("catalog: begin source driver preparation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	state := SourceDriverPreparationState{
		Authority: string(identity.Authority), Publication: identity.Operation,
		SourceRevision: uint64(identity.Predecessor + 1), TargetCount: identity.TargetCount,
	}
	targetsPrepared, err := c.prepareSourceDriverStageTargetPage(ctx, tx, identity, &state)
	if err != nil {
		return SourceDriverPreparationState{}, err
	}
	if !targetsPrepared {
		if err := tx.Commit(); err != nil {
			return SourceDriverPreparationState{}, fmt.Errorf("catalog: commit source driver target preparation: %w", err)
		}
		if err := c.trip(sourceDriverPreparationAfterBatchPoint); err != nil {
			return SourceDriverPreparationState{}, err
		}
		return state, nil
	}
	normalized, err := c.normalizeSourceDriverStageBatch(ctx, tx, identity)
	if err != nil {
		return SourceDriverPreparationState{}, err
	}
	if !normalized {
		if err := tx.Commit(); err != nil {
			return SourceDriverPreparationState{}, fmt.Errorf("catalog: commit source driver normalization: %w", err)
		}
		if err := c.trip(sourceDriverPreparationAfterBatchPoint); err != nil {
			return SourceDriverPreparationState{}, err
		}
		return state, nil
	}
	if err := initializeSourceDriverPublication(ctx, tx, identity, identityDigest); err != nil {
		return SourceDriverPreparationState{}, err
	}
	state, err = readSourceDriverPreparationState(ctx, tx, identity)
	if err != nil {
		return SourceDriverPreparationState{}, err
	}
	if err := validateSourceDriverTargetEpoch(ctx, tx, identity.Authority, state.TargetEpoch); err != nil {
		return SourceDriverPreparationState{}, err
	}
	if state.Prepared {
		if err := tx.Commit(); err != nil {
			return SourceDriverPreparationState{}, err
		}
		return state, nil
	}
	switch state.Phase {
	case sourceDriverPublicationInitializing:
		if err := initializeSourceDriverTargetPage(ctx, tx, identity, &state); err != nil {
			return SourceDriverPreparationState{}, err
		}
	case sourceDriverPublicationPreparing:
		if err := c.prepareSourceDriverTargetPage(ctx, tx, identity, &state); err != nil {
			return SourceDriverPreparationState{}, err
		}
	case sourceDriverPublicationContent:
		if err := c.prepareSourceDriverContentPage(ctx, tx, identity, &state); err != nil {
			return SourceDriverPreparationState{}, err
		}
	default:
		return SourceDriverPreparationState{}, ErrIntegrity
	}
	if err := tx.Commit(); err != nil {
		return SourceDriverPreparationState{}, fmt.Errorf("catalog: commit source driver preparation: %w", err)
	}
	if err := c.trip(sourceDriverPreparationAfterBatchPoint); err != nil {
		return SourceDriverPreparationState{}, err
	}
	return state, nil
}

func (c *Catalog) prepareSourceDriverStageTargetPage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	state *SourceDriverPreparationState,
) (bool, error) {
	var cursor string
	var digestState []byte
	var prepared int
	if err := tx.QueryRowContext(ctx, `
SELECT target_epoch, target_cursor, declared_target_count, target_digest_state, targets_prepared
FROM source_driver_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(
		&state.TargetEpoch, &cursor, &state.Rows, &digestState, &prepared,
	); err != nil {
		return false, err
	}
	if len(digestState) != sourceDriverTargetsDigestStateSize || state.Rows > identity.TargetCount {
		return false, ErrIntegrity
	}
	if err := validateSourceDriverTargetEpoch(ctx, tx, identity.Authority, state.TargetEpoch); err != nil {
		return false, err
	}
	if prepared != 0 {
		return true, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT intent.tenant_id, intent.target_generation
FROM tenant_intents intent
JOIN tenant_generations generation
  ON generation.tenant_id = intent.tenant_id AND generation.generation = intent.target_generation
WHERE generation.content_source_id = ? AND intent.tenant_id > ? AND intent.state = ?
ORDER BY intent.tenant_id LIMIT ?`, string(identity.Authority), cursor, uint8(TenantIntentPresent), sourceDriverTargetBatchSize)
	if err != nil {
		return false, err
	}
	targets := make([]sourceDriverTargetState, 0, sourceDriverTargetBatchSize)
	for rows.Next() {
		var tenant string
		var generation uint64
		if err := rows.Scan(&tenant, &generation); err != nil {
			_ = rows.Close()
			return false, err
		}
		root, err := DeriveSourceDriverRootKey(identity.Authority, TenantID(tenant))
		if err != nil {
			_ = rows.Close()
			return false, err
		}
		head, err := sourceDriverExpectedCatalogRevision(ctx, tx, identity.Authority, TenantID(tenant))
		if err != nil {
			_ = rows.Close()
			return false, err
		}
		targets = append(targets, sourceDriverTargetState{
			SourceDriverTarget: SourceDriverTarget{Tenant: TenantID(tenant), Generation: Generation(generation)},
			RootKey:            root, CatalogRevision: head,
		})
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if len(targets) == 0 {
		digest, err := finishSourceDriverTargetsDigestState(digestState)
		if err != nil {
			return false, err
		}
		if state.Rows != identity.TargetCount || digest != identity.TargetsDigest {
			return false, ErrMutationConflict
		}
		if err := advanceSourceDriverCheckpointTargetDeclaration(
			ctx, tx, identity, state.TargetEpoch,
		); err != nil {
			return false, err
		}
		result, err := tx.ExecContext(ctx, `
UPDATE source_driver_stages SET targets_prepared = 1
WHERE source_authority = ? AND stage_operation_id = ? AND targets_prepared = 0
  AND target_epoch = ? AND target_cursor = ? AND declared_target_count = ?
  AND target_digest_state = ?`, string(identity.Authority), identity.Operation[:], state.TargetEpoch,
			cursor, state.Rows, digestState)
		if err != nil {
			return false, err
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return false, ErrMutationConflict
		}
		return false, nil
	}
	if state.Rows+uint64(len(targets)) > identity.TargetCount {
		return false, ErrMutationConflict
	}
	priorDigestState := append([]byte(nil), digestState...)
	prior := TenantID(cursor)
	for _, target := range targets {
		if err := ensureSourceDriverStageTarget(ctx, tx, identity, target); err != nil {
			return false, err
		}
		digestState, err = appendSourceDriverTargetsDigestState(
			digestState, prior, target.SourceDriverTarget,
		)
		if err != nil {
			return false, err
		}
		prior = target.Tenant
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_stages
SET target_cursor = ?, declared_target_count = declared_target_count + ?, target_digest_state = ?
WHERE source_authority = ? AND stage_operation_id = ? AND targets_prepared = 0
  AND target_epoch = ? AND target_cursor = ? AND declared_target_count = ?
  AND target_digest_state = ?`, string(prior), len(targets), digestState,
		string(identity.Authority), identity.Operation[:], state.TargetEpoch, cursor, state.Rows,
		priorDigestState)
	if err != nil {
		return false, err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return false, ErrMutationConflict
	}
	state.Rows += uint64(len(targets))
	return false, nil
}

func advanceSourceDriverCheckpointTargetDeclaration(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	targetEpoch uint64,
) error {
	checkpoint, found, err := readSourceDriverCheckpoint(ctx, tx, identity.Authority)
	if err != nil || !found {
		return err
	}
	if checkpoint.FleetOwner != identity.FleetOwner ||
		checkpoint.AuthorityGeneration != identity.AuthorityGeneration ||
		checkpoint.DeclarationDigest != identity.DeclarationDigest ||
		checkpoint.SourceRevision != identity.Predecessor || targetEpoch < checkpoint.TargetEpoch {
		return ErrGenerationMismatch
	}
	if targetEpoch == checkpoint.TargetEpoch {
		if checkpoint.TargetCount != identity.TargetCount || checkpoint.TargetsDigest != identity.TargetsDigest {
			return ErrGenerationMismatch
		}
		return nil
	}
	if (identity.Mode != SourceDriverSnapshot && identity.Mode != SourceDriverMutation) ||
		identity.SnapshotReason != SourceDriverSnapshotReset {
		return ErrSourceRequiresSnapshot
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_checkpoints
SET target_epoch = ?, target_count = ?, targets_digest = ?, snapshot_required = ?
WHERE source_authority = ? AND target_epoch = ? AND source_revision = ?`,
		targetEpoch, identity.TargetCount, identity.TargetsDigest[:], uint8(SourceDriverSnapshotReset),
		string(identity.Authority), checkpoint.TargetEpoch, uint64(checkpoint.SourceRevision))
	if err != nil {
		return mapConstraint(err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	return nil
}

func sourceDriverExpectedCatalogRevision(
	ctx context.Context,
	tx *sql.Tx,
	authority causal.SourceAuthorityID,
	tenant TenantID,
) (Revision, error) {
	head, _, err := effectiveRevisionState(ctx, tx, tenant)
	if err == nil {
		return head, nil
	}
	if !errors.Is(err, ErrIntegrity) {
		return 0, err
	}
	var active int
	if queryErr := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM tenant_activations activation
    JOIN tenant_generations generation
      ON generation.tenant_id = activation.tenant_id
     AND generation.generation = activation.active_generation
    WHERE generation.content_source_id = ? AND activation.tenant_id = ?
)`, string(authority), string(tenant)).Scan(&active); queryErr != nil {
		return 0, queryErr
	}
	if active != 0 {
		return 0, err
	}
	head, _, err = revisionState(ctx, tx, tenant)
	return head, err
}

func validateSourceDriverTargetEpoch(
	ctx context.Context,
	query sourceDriverCheckpointQueryer,
	authority causal.SourceAuthorityID,
	expected uint64,
) error {
	var current uint64
	if err := query.QueryRowContext(ctx, `
SELECT target_epoch FROM source_driver_target_epochs WHERE source_authority = ?`, string(authority)).Scan(&current); err != nil {
		return err
	}
	if current != expected {
		return ErrMutationConflict
	}
	return nil
}

func initializeSourceDriverPublication(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	identityDigest [sha256.Size]byte,
) error {
	state, found, err := readSourceDriverStageState(ctx, tx, identity.Authority)
	if err != nil {
		return err
	}
	if !found || state.Identity != identity || state.Stage.Revision != identity.Predecessor+1 ||
		len(state.Cursor) != 0 {
		return ErrInvalidTransition
	}
	var complete, aborting int
	if err := tx.QueryRowContext(ctx, `
SELECT complete, aborting FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&complete, &aborting); err != nil {
		return err
	}
	if complete != 1 || aborting != 0 {
		return ErrInvalidTransition
	}
	var targetEpoch uint64
	var targetsPrepared int
	if err := tx.QueryRowContext(ctx, `
SELECT target_epoch, targets_prepared FROM source_driver_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&targetEpoch, &targetsPrepared); err != nil {
		return err
	}
	if targetsPrepared != 1 {
		return ErrInvalidTransition
	}
	if err := validateSourceDriverTargetEpoch(ctx, tx, identity.Authority, targetEpoch); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_heads(
    source_authority, publication_id, source_revision, epoch
) VALUES (?, zeroblob(0), 0, 0)
ON CONFLICT(source_authority) DO NOTHING`, string(identity.Authority)); err != nil {
		return mapConstraint(err)
	}
	var predecessor []byte
	var predecessorRevision, visibilityEpoch uint64
	if err := tx.QueryRowContext(ctx, `
SELECT publication_id, source_revision, epoch
FROM source_driver_publication_heads WHERE source_authority = ?`, string(identity.Authority)).Scan(
		&predecessor, &predecessorRevision, &visibilityEpoch,
	); err != nil {
		return err
	}
	if len(predecessor) != 0 && len(predecessor) != len(identity.Operation) {
		return ErrIntegrity
	}
	if predecessor == nil {
		predecessor = []byte{}
	}
	if predecessorRevision != uint64(identity.Predecessor) ||
		((len(predecessor) == 0) != (predecessorRevision == 0)) {
		return ErrSourcePredecessor
	}
	affectedKeysDigest := sha256.Sum256([]byte("driver"))
	result, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publications(
    source_authority, publication_id, source_operation_id, change_id, cause,
    origin_domain, origin_generation, affected_key_count, affected_keys_digest,
    predecessor_publication_id,
    predecessor_revision, source_revision, expected_visibility_epoch,
    target_epoch, identity_digest, target_count, targets_digest, stage_sequence, stage_item_count,
    stage_byte_count, stage_digest, initialized_target_count, prepared_target_count,
    phase, cursor_tenant, cursor_key, item_count, byte_count, rolling_digest, prepared
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, 1, ?,
    ?,
    ?, ?, ?,
    ?, ?, ?, ?, ?, ?,
    ?, ?, 0, 0,
    ?, '', '', ?, 0, zeroblob(32), 0
)
ON CONFLICT(source_authority, publication_id) DO NOTHING`,
		string(identity.Authority), identity.Operation[:], identity.SourceOperation[:], identity.ChangeID[:],
		string(identity.Cause), string(identity.Origin), uint64(identity.OriginGeneration), affectedKeysDigest[:], predecessor,
		predecessorRevision, uint64(identity.Predecessor+1), visibilityEpoch, targetEpoch,
		identityDigest[:], identity.TargetCount, identity.TargetsDigest[:], state.Stage.Sequence,
		state.Stage.Items, state.Stage.Bytes, state.Stage.Digest[:], sourceDriverPublicationInitializing,
		identity.TargetCount,
	)
	if err != nil {
		return mapConstraint(err)
	}
	if changed, _ := result.RowsAffected(); changed != 0 {
		return nil
	}
	var storedDigest, storedTargetsDigest, storedStageDigest []byte
	var storedPredecessor []byte
	var storedRevision, storedSourceRevision, storedEpoch, storedTargetEpoch, targetCount uint64
	var stageSequence, stageItems, stageBytes uint64
	if err := tx.QueryRowContext(ctx, `
SELECT predecessor_publication_id, predecessor_revision, source_revision,
	       expected_visibility_epoch, target_epoch, identity_digest, target_count, targets_digest,
	       stage_sequence, stage_item_count, stage_byte_count, stage_digest
FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(
		&storedPredecessor, &storedRevision, &storedSourceRevision, &storedEpoch, &storedTargetEpoch,
		&storedDigest, &targetCount,
		&storedTargetsDigest, &stageSequence, &stageItems, &stageBytes, &storedStageDigest,
	); err != nil {
		return err
	}
	if !bytes.Equal(storedPredecessor, predecessor) || storedRevision != predecessorRevision ||
		storedEpoch != visibilityEpoch || storedTargetEpoch != targetEpoch ||
		!bytes.Equal(storedDigest, identityDigest[:]) ||
		targetCount != identity.TargetCount || !bytes.Equal(storedTargetsDigest, identity.TargetsDigest[:]) ||
		storedSourceRevision != uint64(identity.Predecessor+1) || stageSequence != state.Stage.Sequence ||
		stageItems != state.Stage.Items || stageBytes != state.Stage.Bytes ||
		!bytes.Equal(storedStageDigest, state.Stage.Digest[:]) {
		return ErrMutationConflict
	}
	return nil
}

func readSourceDriverPreparationState(
	ctx context.Context,
	query sourceDriverCheckpointQueryer,
	identity SourceDriverStageIdentity,
) (SourceDriverPreparationState, error) {
	state := SourceDriverPreparationState{Authority: string(identity.Authority), Publication: identity.Operation}
	var prepared int
	var digest, active []byte
	if err := query.QueryRowContext(ctx, `
SELECT publication.phase, publication.source_revision, publication.expected_visibility_epoch,
       publication.target_epoch, publication.item_count, publication.byte_count, publication.prepared_target_count,
       publication.target_count, publication.rolling_digest, publication.prepared,
       visibility.publication_id
FROM source_driver_publications publication
JOIN source_driver_publication_heads visibility
  ON visibility.source_authority = publication.source_authority
WHERE publication.source_authority = ? AND publication.publication_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(
		&state.Phase, &state.SourceRevision, &state.ExpectedVisibilityEpoch, &state.TargetEpoch,
		&state.Rows, &state.Bytes,
		&state.PreparedTargets, &state.TargetCount, &digest, &prepared, &active,
	); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return SourceDriverPreparationState{}, err
		}
		var targetsPrepared int
		if stageErr := query.QueryRowContext(ctx, `
SELECT target_epoch, declared_target_count, targets_prepared
FROM source_driver_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
			string(identity.Authority), identity.Operation[:]).Scan(
			&state.TargetEpoch, &state.Rows, &targetsPrepared,
		); stageErr != nil {
			return SourceDriverPreparationState{}, stageErr
		}
		state.SourceRevision = uint64(identity.Predecessor + 1)
		state.TargetCount = identity.TargetCount
		return state, nil
	}
	state.Prepared = prepared != 0
	if len(digest) != sha256.Size || (len(active) != 0 && len(active) != len(identity.Operation)) {
		return SourceDriverPreparationState{}, ErrIntegrity
	}
	copy(state.Digest[:], digest)
	state.Published = bytes.Equal(active, identity.Operation[:])
	return state, nil
}

func initializeSourceDriverTargetPage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	state *SourceDriverPreparationState,
) error {
	var after string
	if err := tx.QueryRowContext(ctx, `
SELECT cursor_tenant FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&after); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `
SELECT staged.tenant, staged.generation, staged.root_key, staged.expected_catalog_revision,
       publication.predecessor_publication_id,
       predecessor.generation, predecessor.root_key, predecessor.catalog_head
FROM source_driver_stage_targets staged
JOIN source_driver_publications publication
  ON publication.source_authority = staged.source_authority
 AND publication.publication_id = staged.stage_operation_id
LEFT JOIN source_driver_publication_targets predecessor
  ON predecessor.source_authority = staged.source_authority
 AND predecessor.publication_id = publication.predecessor_publication_id
 AND predecessor.tenant = staged.tenant AND predecessor.prepared = 1
WHERE staged.source_authority = ? AND staged.stage_operation_id = ? AND staged.tenant > ?
ORDER BY staged.tenant LIMIT ?`, string(identity.Authority), identity.Operation[:], after,
		sourceDriverPreparationPageLimit)
	if err != nil {
		return err
	}
	type target struct {
		tenant          string
		generation      uint64
		root            string
		head            uint64
		predecessor     []byte
		priorGeneration sql.NullInt64
		priorRoot       sql.NullString
		priorHead       sql.NullInt64
	}
	targets := make([]target, 0, sourceDriverPreparationPageLimit)
	for rows.Next() {
		var value target
		if err := rows.Scan(&value.tenant, &value.generation, &value.root, &value.head,
			&value.predecessor, &value.priorGeneration, &value.priorRoot, &value.priorHead); err != nil {
			_ = rows.Close()
			return err
		}
		if len(value.predecessor) != 0 {
			if len(value.predecessor) != len(identity.Operation) || !value.priorGeneration.Valid ||
				!value.priorRoot.Valid || !value.priorHead.Valid || value.priorHead.Int64 <= 0 ||
				uint64(value.priorGeneration.Int64) != value.generation || value.priorRoot.String != value.root {
				_ = rows.Close()
				return ErrMutationConflict
			}
			value.head = uint64(value.priorHead.Int64)
		}
		targets = append(targets, value)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, value := range targets {
		var rawRoot []byte
		if err := tx.QueryRowContext(ctx, `SELECT root_id FROM tenants WHERE tenant = ?`, value.tenant).Scan(&rawRoot); err != nil {
			return err
		}
		if len(rawRoot) != len(ObjectID{}) {
			return ErrIntegrity
		}
		catalogSeed := chainSourceDriverFingerprint(
			"fusekit.source-driver-catalog.v1", [sha256.Size]byte{},
			encodeSourceDriverFingerprintRow(value.tenant, nil, value.root, 0, 0, int64(value.generation), nil, "", false, false),
		)
		providerSeed := chainSourceDriverFingerprint(
			"fusekit.source-driver-file-provider.v1", [sha256.Size]byte{}, rawRoot,
		)
		catalogOperation := sourceCatalogOperation(identity.SourceOperation, TenantID(value.tenant))
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_targets(
    source_authority, publication_id, tenant, generation, root_key, catalog_operation_id,
    predecessor_head, catalog_head, phase, cursor_key, cursor_object_id,
    cursor_revision, catalog_fingerprint, file_provider_fingerprint, catalog_state, provider_state,
    changed, provider_changed, object_count, next_change_sequence, prepared
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', zeroblob(0), 0,
	          ?, ?, ?, ?, 0, 0, 0, 0, 0)`,
			string(identity.Authority), identity.Operation[:], value.tenant, value.generation,
			value.root, catalogOperation[:], value.head, value.head, sourceDriverTargetRoot,
			catalogSeed[:], providerSeed[:], catalogSeed[:], providerSeed[:]); err != nil {
			return mapConstraint(err)
		}
	}
	if len(targets) == 0 {
		result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publications SET phase = ?, cursor_tenant = ''
WHERE source_authority = ? AND publication_id = ? AND phase = ?
  AND initialized_target_count = target_count`,
			sourceDriverPublicationPreparing, string(identity.Authority), identity.Operation[:],
			sourceDriverPublicationInitializing)
		if err != nil {
			return err
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return ErrIntegrity
		}
		state.Phase = sourceDriverPublicationPreparing
		return nil
	}
	last := targets[len(targets)-1].tenant
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publications
SET cursor_tenant = ?, initialized_target_count = initialized_target_count + ?,
    item_count = item_count + ?
WHERE source_authority = ? AND publication_id = ? AND phase = ? AND cursor_tenant = ?`,
		last, len(targets), len(targets), string(identity.Authority), identity.Operation[:],
		sourceDriverPublicationInitializing, after)
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	state.Rows += uint64(len(targets))
	return nil
}

func (c *Catalog) prepareSourceDriverTargetPage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	state *SourceDriverPreparationState,
) error {
	target, found, err := readNextSourceDriverPreparedTarget(ctx, tx, identity)
	if err != nil {
		return err
	}
	if !found {
		if state.PreparedTargets != state.TargetCount {
			return ErrIntegrity
		}
		result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publications SET phase = ?, cursor_key = ''
WHERE source_authority = ? AND publication_id = ? AND phase = ?
	  AND prepared_target_count = target_count`, sourceDriverPublicationContent,
			string(identity.Authority), identity.Operation[:], sourceDriverPublicationPreparing)
		if err != nil {
			return err
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return ErrMutationConflict
		}
		state.Phase = sourceDriverPublicationContent
		return nil
	}
	state.Target = target.tenant
	state.TargetPhase = target.phase
	var rows, byteCount int
	switch target.phase {
	case sourceDriverTargetRoot:
		rows, byteCount, err = seedSourceDriverTargetRoot(ctx, tx, identity, &target)
	case sourceDriverTargetObjects:
		rows, byteCount, err = c.prepareSourceDriverObjectPage(ctx, tx, identity, &target)
	case sourceDriverTargetVersions:
		rows, byteCount, err = prepareSourceDriverVersionPage(ctx, tx, identity, &target)
	case sourceDriverTargetBaselineChanges:
		rows, byteCount, err = prepareSourceDriverBaselineChangePage(ctx, tx, identity, &target)
	case sourceDriverTargetValidate:
		rows, byteCount, err = validateSourceDriverPreparedPage(ctx, tx, identity, &target)
	case sourceDriverTargetCatalogFingerprint:
		rows, byteCount, err = fingerprintSourceDriverCatalogPage(ctx, tx, identity, &target)
	case sourceDriverTargetProviderFingerprint:
		rows, byteCount, err = fingerprintSourceDriverProviderPage(ctx, tx, identity, &target)
	case sourceDriverTargetChanges:
		rows, byteCount, err = prepareSourceDriverChangePage(ctx, tx, identity, &target)
	case sourceDriverTargetInterestChanges:
		rows, byteCount, err = prepareSourceDriverInterestChangePage(ctx, tx, identity, &target)
	case sourceDriverTargetPrepared:
		err = finalizeSourceDriverPreparedTarget(ctx, tx, identity, &target, state)
	default:
		err = ErrIntegrity
	}
	if err != nil {
		return err
	}
	if rows > sourceDriverPreparationPageLimit || byteCount > sourceDriverPreparationByteLimit {
		return ErrIntegrity
	}
	if rows != 0 || byteCount != 0 {
		if _, err := tx.ExecContext(ctx, `
UPDATE source_driver_publications SET item_count = item_count + ?, byte_count = byte_count + ?
WHERE source_authority = ? AND publication_id = ?`, rows, byteCount,
			string(identity.Authority), identity.Operation[:]); err != nil {
			return err
		}
		state.Rows += uint64(rows)
		state.Bytes += uint64(byteCount)
	}
	state.TargetPhase = target.phase
	return nil
}

func (c *Catalog) prepareSourceDriverContentPage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	state *SourceDriverPreparationState,
) error {
	var after string
	if err := tx.QueryRowContext(ctx, `
SELECT cursor_key FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ? AND phase = ?`,
		string(identity.Authority), identity.Operation[:], sourceDriverPublicationContent).Scan(&after); err != nil {
		return err
	}
	if after != "" {
		if raw, err := hex.DecodeString(after); err != nil || len(raw) != len(StageID{}) {
			return ErrIntegrity
		}
	}
	rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT content_stage, content_hash, content_size
FROM source_driver_stage_entries entry
WHERE entry.source_authority = ? AND entry.stage_operation_id = ?
  AND entry.action = 2 AND entry.content_stage IS NOT NULL AND hex(entry.content_stage) > ?
  AND NOT EXISTS (
      SELECT 1 FROM source_driver_stage_entries newer
      WHERE newer.source_authority = entry.source_authority
        AND newer.stage_operation_id = entry.stage_operation_id
        AND newer.tenant = entry.tenant AND newer.source_key = entry.source_key
        AND newer.change_sequence > entry.change_sequence
  )
ORDER BY content_stage LIMIT ?`, string(identity.Authority), identity.Operation[:], after,
		sourceDriverPreparationPageLimit)
	if err != nil {
		return err
	}
	refs := make([]ContentRef, 0, sourceDriverPreparationPageLimit)
	for rows.Next() {
		var rawStage, rawHash []byte
		var size int64
		if err := rows.Scan(&rawStage, &rawHash, &size); err != nil {
			_ = rows.Close()
			return err
		}
		if len(rawStage) != len(StageID{}) || len(rawHash) != len(ContentHash{}) {
			_ = rows.Close()
			return ErrIntegrity
		}
		var ref ContentRef
		copy(ref.Stage[:], rawStage)
		copy(ref.Hash[:], rawHash)
		ref.Size = size
		refs = append(refs, ref)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(refs) == 0 {
		var claims int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM content_stages WHERE source_operation_id = ?`, identity.Operation[:]).Scan(&claims); err != nil {
			return err
		}
		if claims != 0 {
			return ErrIntegrity
		}
		result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publications SET phase = ?, prepared = 1
WHERE source_authority = ? AND publication_id = ? AND phase = ?
  AND prepared_target_count = target_count`, sourceDriverPublicationPrepared,
			string(identity.Authority), identity.Operation[:], sourceDriverPublicationContent)
		if err != nil {
			return err
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return ErrMutationConflict
		}
		state.Phase = sourceDriverPublicationPrepared
		state.Prepared = true
		return nil
	}
	for _, ref := range refs {
		if err := c.consumeSourceContent(ctx, tx, identity.Operation, ref); err != nil {
			return err
		}
	}
	last := strings.ToUpper(hex.EncodeToString(refs[len(refs)-1].Stage[:]))
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publications
SET cursor_key = ?, item_count = item_count + ?, byte_count = byte_count + ?
WHERE source_authority = ? AND publication_id = ? AND phase = ? AND cursor_key = ?`,
		last, len(refs), len(refs)*(len(StageID{})+len(ContentHash{})+8),
		string(identity.Authority), identity.Operation[:], sourceDriverPublicationContent, after)
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	state.Rows += uint64(len(refs))
	state.Bytes += uint64(len(refs) * (len(StageID{}) + len(ContentHash{}) + 8))
	return nil
}

func seedSourceDriverTargetRoot(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
) (int, int, error) {
	var predecessor []byte
	if err := tx.QueryRowContext(ctx, `
SELECT predecessor_publication_id FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&predecessor); err != nil {
		return 0, 0, err
	}
	if len(predecessor) != 0 {
		if len(predecessor) != len(identity.Operation) {
			return 0, 0, ErrIntegrity
		}
		if err := advanceSourceDriverPreparedTarget(
			ctx, tx, identity, target, sourceDriverTargetObjects,
		); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}
	var rawRoot []byte
	if err := tx.QueryRowContext(ctx, `SELECT root_id FROM tenants WHERE tenant = ?`,
		string(target.tenant)).Scan(&rawRoot); err != nil {
		return 0, 0, err
	}
	root, err := objectID(rawRoot)
	if err != nil {
		return 0, 0, err
	}
	object, err := currentObject(ctx, tx, target.tenant, root, false)
	if err != nil {
		return 0, 0, err
	}
	if object.ID != root || object.Parent != root || object.Revision == 0 || object.Revision > target.predecessorHead ||
		object.Kind != KindDirectory || object.Name != "" || object.Tombstone {
		return 0, 0, ErrIntegrity
	}
	prepared := sourceDriverPreparedObject{key: target.root, object: object}
	if err := insertSourceDriverPreparedObject(ctx, tx, identity, prepared); err != nil {
		return 0, 0, err
	}
	if err := insertSourceDriverPreparedVersion(ctx, tx, identity, prepared); err != nil {
		return 0, 0, err
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets
SET phase = ?, object_count = object_count + 1
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND phase = ?`,
		sourceDriverTargetObjects, string(identity.Authority), identity.Operation[:],
		string(target.tenant), sourceDriverTargetRoot)
	if err != nil {
		return 0, 0, err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return 0, 0, ErrMutationConflict
	}
	target.phase = sourceDriverTargetObjects
	target.objectCount++
	return 2, len(target.root) + 2*256, nil
}

func readNextSourceDriverPreparedTarget(
	ctx context.Context,
	query sourceDriverCheckpointQueryer,
	identity SourceDriverStageIdentity,
) (sourceDriverPreparedTarget, bool, error) {
	var target sourceDriverPreparedTarget
	var rawCatalog, rawProvider []byte
	var changed, providerChanged int
	err := query.QueryRowContext(ctx, `
SELECT tenant, generation, root_key, predecessor_head, catalog_head, phase,
       cursor_key, cursor_object_id, cursor_revision, catalog_fingerprint,
       file_provider_fingerprint, changed, provider_changed, object_count,
       next_change_sequence
FROM source_driver_publication_targets
WHERE source_authority = ? AND publication_id = ? AND prepared = 0
ORDER BY tenant LIMIT 1`, string(identity.Authority), identity.Operation[:]).Scan(
		&target.tenant, &target.generation, &target.root, &target.predecessorHead,
		&target.catalogHead, &target.phase, &target.cursorKey, &target.cursorObject,
		&target.cursorRevision, &rawCatalog, &rawProvider, &changed, &providerChanged,
		&target.objectCount, &target.nextChangeSequence,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sourceDriverPreparedTarget{}, false, nil
	}
	if err != nil {
		return sourceDriverPreparedTarget{}, false, err
	}
	if len(rawCatalog) != sha256.Size || len(rawProvider) != sha256.Size ||
		(len(target.cursorObject) != 0 && len(target.cursorObject) != len(ObjectID{})) {
		return sourceDriverPreparedTarget{}, false, ErrIntegrity
	}
	if len(target.cursorObject) == 0 {
		target.cursorObject = make([]byte, 0)
	}
	copy(target.catalogFingerprint[:], rawCatalog)
	copy(target.fileProviderFingerprint[:], rawProvider)
	target.changed = changed != 0
	target.providerChanged = providerChanged != 0
	return target, true, nil
}

func (c *Catalog) prepareSourceDriverObjectPage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
) (int, int, error) {
	keys, err := sourceDriverCandidateKeys(ctx, tx, identity, *target)
	if err != nil {
		return 0, 0, err
	}
	if len(keys) == 0 {
		next := sourceDriverTargetValidate
		var predecessor []byte
		if err := tx.QueryRowContext(ctx, `
SELECT predecessor_publication_id FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ?`,
			string(identity.Authority), identity.Operation[:]).Scan(&predecessor); err != nil {
			return 0, 0, err
		}
		if len(predecessor) == 0 {
			next = sourceDriverTargetVersions
		}
		if err := advanceSourceDriverPreparedTarget(ctx, tx, identity, target, next); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}
	used := 0
	processed := 0
	written := 0
	insertedCount := 0
	changedAny := false
	providerChangedAny := false
	var lastKey SourceObjectKey
	for _, key := range keys {
		prepared, found, changed, providerChanged, err := c.sourceDriverCandidateObject(
			ctx, tx, identity, *target, key,
		)
		if err != nil {
			return 0, 0, err
		}
		rowBytes := len(key) + 256
		rowWrites := 0
		if found {
			rowWrites++
			rowBytes += len(prepared.object.Name) + len(prepared.nameKey) + len(prepared.object.LinkTarget)
			if changed {
				rowWrites++
				rowBytes *= 2
			}
		}
		if rowBytes > sourceDriverPreparationByteLimit {
			return 0, 0, fmt.Errorf("%w: prepared source object exceeds byte limit", ErrInvalidObject)
		}
		if processed != 0 && used+rowBytes > sourceDriverPreparationByteLimit {
			break
		}
		if found {
			if err := insertSourceDriverPreparedObject(ctx, tx, identity, prepared); err != nil {
				return 0, 0, err
			}
			if changed {
				if err := insertSourceDriverPreparedVersion(ctx, tx, identity, prepared); err != nil {
					return 0, 0, err
				}
			}
		}
		written += rowWrites
		if found {
			insertedCount++
		}
		changedAny = changedAny || changed
		providerChangedAny = providerChangedAny || providerChanged
		lastKey = key
		target.cursorKey = string(key)
		target.changed = target.changed || changed
		target.providerChanged = target.providerChanged || providerChanged
		processed++
		used += rowBytes
	}
	if processed != 0 {
		if err := updateSourceDriverPreparedTargetFlags(
			ctx, tx, identity, target.tenant, lastKey, insertedCount, changedAny, providerChangedAny,
		); err != nil {
			return 0, 0, err
		}
	}
	return written, used, nil
}

func sourceDriverCandidateKeys(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target sourceDriverPreparedTarget,
) ([]SourceObjectKey, error) {
	rows, err := tx.QueryContext(ctx, `
WITH publication AS (
    SELECT predecessor_publication_id
    FROM source_driver_publications
    WHERE source_authority = ? AND publication_id = ?
), keys(source_key) AS (
    SELECT object.source_key
    FROM source_driver_publication_objects object, publication
    WHERE object.source_authority = ? AND object.publication_id = publication.predecessor_publication_id
      AND object.tenant = ? AND length(publication.predecessor_publication_id) = 16
    UNION
    SELECT binding.source_key
    FROM source_object_bindings binding
    JOIN source_object_ids identity
      ON identity.source_authority = binding.source_authority
     AND identity.source_key = binding.source_key
    JOIN objects object ON object.tenant = binding.tenant AND object.object_id = identity.object_id
    CROSS JOIN publication
    WHERE binding.source_authority = ? AND binding.tenant = ?
      AND length(publication.predecessor_publication_id) = 0
    UNION
    SELECT source_key FROM source_driver_stage_entries
    WHERE source_authority = ? AND stage_operation_id = ? AND tenant = ?
)
SELECT source_key FROM keys WHERE source_key > ? ORDER BY source_key LIMIT ?`,
		string(identity.Authority), identity.Operation[:], string(identity.Authority), string(target.tenant),
		string(identity.Authority), string(target.tenant),
		string(identity.Authority), identity.Operation[:], string(target.tenant),
		target.cursorKey, sourceDriverObjectPageLimit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	keys := make([]SourceObjectKey, 0, sourceDriverObjectPageLimit)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, SourceObjectKey(key))
	}
	return keys, rows.Err()
}

func (c *Catalog) sourceDriverCandidateObject(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target sourceDriverPreparedTarget,
	key SourceObjectKey,
) (sourceDriverPreparedObject, bool, bool, bool, error) {
	baseline, baselineFound, err := readSourceDriverPreparationBaseline(ctx, tx, identity, target, key)
	if err != nil {
		return sourceDriverPreparedObject{}, false, false, false, err
	}
	var deleted int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_driver_stage_entries entry
    WHERE entry.source_authority = ? AND entry.stage_operation_id = ?
      AND entry.tenant = ? AND entry.source_key = ? AND entry.action = 1
      AND NOT EXISTS (
          SELECT 1 FROM source_driver_stage_entries newer
          WHERE newer.source_authority = entry.source_authority
            AND newer.stage_operation_id = entry.stage_operation_id
            AND newer.tenant = entry.tenant AND newer.source_key = entry.source_key
            AND newer.change_sequence > entry.change_sequence
      )
)`, string(identity.Authority), identity.Operation[:],
		string(target.tenant), string(key)).Scan(&deleted); err != nil {
		return sourceDriverPreparedObject{}, false, false, false, err
	}
	if deleted != 0 {
		if !baselineFound || baseline.object.Tombstone {
			return sourceDriverPreparedObject{}, false, false, false, nil
		}
		candidate := baseline
		candidate.object.Revision = target.predecessorHead + 1
		candidate.object.MetadataRevision = candidate.object.Revision
		candidate.object.Visibility = Visibility{}
		candidate.object.Tombstone = true
		return candidate, true, true, baseline.object.Visibility.FileProvider, nil
	}
	staged, found, err := readSourceDriverStagedObject(ctx, tx, identity, target, key)
	if err != nil {
		return sourceDriverPreparedObject{}, false, false, false, err
	}
	if !found {
		return baseline, baselineFound, false, false, nil
	}
	id, err := sourceDriverPreparedObjectIdentity(ctx, tx, identity, key)
	if err != nil {
		return sourceDriverPreparedObject{}, false, false, false, err
	}
	var parent ObjectID
	if staged.parent == "" {
		var rawParent []byte
		if err := tx.QueryRowContext(ctx, `SELECT root_id FROM tenants WHERE tenant = ?`,
			string(target.tenant)).Scan(&rawParent); err != nil {
			return sourceDriverPreparedObject{}, false, false, false, err
		}
		parent, err = objectID(rawParent)
		if err != nil {
			return sourceDriverPreparedObject{}, false, false, false, err
		}
	} else {
		parent, err = sourceObjectIdentity(ctx, tx, identity.Authority, staged.parent)
		if err != nil {
			return sourceDriverPreparedObject{}, false, false, false, err
		}
	}
	candidate := staged.prepared
	candidate.key = key
	candidate.object.ID = id
	candidate.object.Parent = parent
	candidate.object.Tenant = target.tenant
	candidate.object.Revision = target.predecessorHead + 1
	candidate.object.MetadataRevision = candidate.object.Revision
	contentSame := baselineFound && !baseline.object.Tombstone &&
		baseline.object.Kind == candidate.object.Kind && baseline.object.Size == candidate.object.Size &&
		baseline.object.Hash == candidate.object.Hash && baseline.object.LinkTarget == candidate.object.LinkTarget
	metadataSame := baselineFound && !baseline.object.Tombstone && baseline.object.Parent == parent &&
		baseline.object.Name == candidate.object.Name && baseline.object.Mode == candidate.object.Mode &&
		baseline.object.LinkTarget == candidate.object.LinkTarget &&
		baseline.object.Visibility == candidate.object.Visibility
	if contentSame {
		candidate.object.ContentRevision = baseline.object.ContentRevision
		candidate.object.Convergence = baseline.object.Convergence
	}
	if metadataSame {
		candidate.object.MetadataRevision = baseline.object.MetadataRevision
	}
	changed := !baselineFound || baseline.object.Tombstone || !contentSame || !metadataSame ||
		baseline.object.Kind != candidate.object.Kind
	if baselineFound && baseline.object.Kind != candidate.object.Kind {
		return sourceDriverPreparedObject{}, false, false, false,
			fmt.Errorf("%w: source object kind changed", ErrInvalidObject)
	}
	if !changed {
		candidate.object.Revision = baseline.object.Revision
	}
	providerChanged := changed && ((baselineFound && baseline.object.Visibility.FileProvider) ||
		candidate.object.Visibility.FileProvider)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_object_bindings(source_authority, tenant, source_key)
VALUES (?, ?, ?) ON CONFLICT(source_authority, tenant, source_key) DO NOTHING`,
		string(identity.Authority), string(target.tenant), string(key)); err != nil {
		return sourceDriverPreparedObject{}, false, false, false, mapConstraint(err)
	}
	return candidate, true, changed, providerChanged, nil
}

func sourceDriverPreparedObjectIdentity(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	key SourceObjectKey,
) (ObjectID, error) {
	id, err := sourceObjectIdentity(ctx, tx, identity.Authority, key)
	if !errors.Is(err, ErrMutationActive) || identity.Mode != SourceDriverMutation ||
		key != identity.MutationResult || identity.Mutation == (MutationID{}) {
		return id, err
	}
	var reservation []byte
	if err := tx.QueryRowContext(ctx, `
SELECT mutation_id FROM source_key_reservations
WHERE source_authority = ? AND source_key = ?`,
		string(identity.Authority), string(key)).Scan(&reservation); err != nil {
		return ObjectID{}, err
	}
	if !bytes.Equal(reservation, identity.Mutation[:]) {
		return ObjectID{}, ErrMutationConflict
	}
	id, err = NewObjectID()
	if err != nil {
		return ObjectID{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_object_ids(source_authority, source_key, object_id) VALUES (?, ?, ?)`,
		string(identity.Authority), string(key), id[:]); err != nil {
		return ObjectID{}, mapConstraint(err)
	}
	return id, nil
}

type sourceDriverStagedPrepared struct {
	parent   SourceObjectKey
	prepared sourceDriverPreparedObject
}

func readSourceDriverStagedObject(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target sourceDriverPreparedTarget,
	key SourceObjectKey,
) (sourceDriverStagedPrepared, bool, error) {
	var value sourceDriverStagedPrepared
	var kind uint8
	var hash []byte
	var mount, provider bool
	err := tx.QueryRowContext(ctx, `
SELECT parent_key, object_name, object_kind, object_mode, content_revision,
       content_hash, content_size, link_target, mount_visible, file_provider_visible
FROM source_driver_stage_entries entry
WHERE entry.source_authority = ? AND entry.stage_operation_id = ?
  AND entry.tenant = ? AND entry.source_key = ? AND entry.action = 2
  AND NOT EXISTS (
      SELECT 1 FROM source_driver_stage_entries newer
      WHERE newer.source_authority = entry.source_authority
        AND newer.stage_operation_id = entry.stage_operation_id
        AND newer.tenant = entry.tenant AND newer.source_key = entry.source_key
        AND newer.change_sequence > entry.change_sequence
  )`, string(identity.Authority), identity.Operation[:],
		string(target.tenant), string(key)).Scan(
		&value.parent, &value.prepared.object.Name, &kind,
		&value.prepared.object.Mode, &value.prepared.object.ContentRevision, &hash,
		&value.prepared.object.Size, &value.prepared.object.LinkTarget, &mount, &provider,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sourceDriverStagedPrepared{}, false, nil
	}
	if err != nil {
		return sourceDriverStagedPrepared{}, false, err
	}
	if len(hash) != len(ContentHash{}) {
		return sourceDriverStagedPrepared{}, false, ErrIntegrity
	}
	policy, err := tenantCasePolicy(ctx, tx, target.tenant)
	if err != nil {
		return sourceDriverStagedPrepared{}, false, err
	}
	value.prepared.nameKey = normalizeName(policy, value.prepared.object.Name)
	value.prepared.object.Kind = Kind(kind)
	copy(value.prepared.object.Hash[:], hash)
	value.prepared.object.Visibility = Visibility{Mount: mount, FileProvider: provider}
	return value, true, nil
}

func readSourceDriverPreparationBaseline(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target sourceDriverPreparedTarget,
	key SourceObjectKey,
) (sourceDriverPreparedObject, bool, error) {
	var predecessor []byte
	if err := tx.QueryRowContext(ctx, `
SELECT predecessor_publication_id FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&predecessor); err != nil {
		return sourceDriverPreparedObject{}, false, err
	}
	query := `SELECT source_key, object_id, parent_id, revision, metadata_revision, content_revision,
       name, name_key, kind, mode, size, hash, link_target, desired_revision, observed_revision,
       verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND source_key = ?`
	args := []any{string(identity.Authority), predecessor, string(target.tenant), string(key)}
	if len(predecessor) == 0 {
		query = `SELECT binding.source_key, object.object_id, object.parent_id, object.revision,
       object.metadata_revision, object.content_revision, object.name, object.name_key,
       object.kind, object.mode, object.size, object.hash, object.link_target,
       object.desired_revision, object.observed_revision, object.verified_revision,
       object.applied_revision, object.mount_visible, object.file_provider_visible, object.tombstone
FROM source_object_bindings binding
JOIN source_object_ids identity
  ON identity.source_authority = binding.source_authority AND identity.source_key = binding.source_key
JOIN objects object ON object.tenant = binding.tenant AND object.object_id = identity.object_id
WHERE binding.source_authority = ? AND binding.tenant = ? AND binding.source_key = ?`
		args = []any{string(identity.Authority), string(target.tenant), string(key)}
	}
	value, err := scanSourceDriverPreparedObject(tx.QueryRowContext(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return sourceDriverPreparedObject{}, false, nil
	}
	if err != nil {
		return sourceDriverPreparedObject{}, false, err
	}
	value.object.Tenant = target.tenant
	return value, true, nil
}

func scanSourceDriverPreparedObject(row *sql.Row) (sourceDriverPreparedObject, error) {
	var value sourceDriverPreparedObject
	var rawID, rawParent, rawHash []byte
	var revision, metadata, content, desired, observed, verified, applied uint64
	var kind uint8
	var mount, provider, tombstone bool
	if err := row.Scan(&value.key, &rawID, &rawParent, &revision, &metadata, &content,
		&value.object.Name, &value.nameKey, &kind, &value.object.Mode, &value.object.Size,
		&rawHash, &value.object.LinkTarget, &desired, &observed, &verified, &applied,
		&mount, &provider, &tombstone); err != nil {
		return sourceDriverPreparedObject{}, err
	}
	var err error
	value.object.ID, err = objectID(rawID)
	if err != nil {
		return sourceDriverPreparedObject{}, err
	}
	value.object.Parent, err = objectID(rawParent)
	if err != nil || len(rawHash) != len(ContentHash{}) {
		return sourceDriverPreparedObject{}, ErrIntegrity
	}
	copy(value.object.Hash[:], rawHash)
	value.object.Revision = Revision(revision)
	value.object.MetadataRevision = Revision(metadata)
	value.object.ContentRevision = Revision(content)
	value.object.Kind = Kind(kind)
	value.object.Convergence = Convergence{
		Desired: Revision(desired), Observed: Revision(observed),
		Verified: Revision(verified), Applied: Revision(applied),
	}
	value.object.Visibility = Visibility{Mount: mount, FileProvider: provider}
	value.object.Tombstone = tombstone
	return value, nil
}

func insertSourceDriverPreparedObject(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	prepared sourceDriverPreparedObject,
) error {
	obj := prepared.object
	_, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_objects(
    source_authority, publication_id, tenant, source_key, object_id, parent_id,
    revision, metadata_revision, content_revision, name, name_key, kind, mode, size,
    hash, link_target, desired_revision, observed_revision, verified_revision,
    applied_revision, mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(identity.Authority), identity.Operation[:], string(obj.Tenant), string(prepared.key),
		obj.ID[:], obj.Parent[:], uint64(obj.Revision), uint64(obj.MetadataRevision),
		uint64(obj.ContentRevision), obj.Name, prepared.nameKey, uint8(obj.Kind), obj.Mode,
		obj.Size, obj.Hash[:], obj.LinkTarget, uint64(obj.Convergence.Desired),
		uint64(obj.Convergence.Observed), uint64(obj.Convergence.Verified),
		uint64(obj.Convergence.Applied), obj.Visibility.Mount, obj.Visibility.FileProvider, obj.Tombstone)
	if err != nil {
		return mapConstraint(err)
	}
	return nil
}

func insertSourceDriverPreparedVersion(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	prepared sourceDriverPreparedObject,
) error {
	obj := prepared.object
	_, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_versions(
    source_authority, publication_id, tenant, object_id, parent_id, revision,
    metadata_revision, content_revision, name, name_key, kind, mode, size, hash,
    link_target, desired_revision, observed_revision, verified_revision, applied_revision,
    mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(identity.Authority), identity.Operation[:], string(obj.Tenant), obj.ID[:], obj.Parent[:],
		uint64(obj.Revision), uint64(obj.MetadataRevision), uint64(obj.ContentRevision), obj.Name,
		prepared.nameKey, uint8(obj.Kind), obj.Mode, obj.Size, obj.Hash[:], obj.LinkTarget,
		uint64(obj.Convergence.Desired), uint64(obj.Convergence.Observed),
		uint64(obj.Convergence.Verified), uint64(obj.Convergence.Applied),
		obj.Visibility.Mount, obj.Visibility.FileProvider, obj.Tombstone)
	if err != nil {
		return mapConstraint(err)
	}
	return nil
}

func updateSourceDriverPreparedTargetFlags(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	tenant TenantID,
	key SourceObjectKey,
	inserted int,
	changed, providerChanged bool,
) error {
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets
SET cursor_key = ?, object_count = object_count + ?,
    changed = MAX(changed, ?), provider_changed = MAX(provider_changed, ?)
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND phase = ?`,
		string(key), inserted, sourceDriverBoolInt(changed), sourceDriverBoolInt(providerChanged),
		string(identity.Authority), identity.Operation[:], string(tenant), sourceDriverTargetObjects)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrMutationConflict
	}
	return nil
}

func prepareSourceDriverVersionPage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
) (int, int, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT version.object_id, version.parent_id, version.revision, version.metadata_revision,
       version.content_revision, version.name, version.name_key, version.kind, version.mode,
       version.size, version.hash, version.link_target, version.desired_revision,
       version.observed_revision, version.verified_revision, version.applied_revision,
       version.mount_visible, version.file_provider_visible, version.tombstone
FROM object_versions version
JOIN source_object_ids identity
  ON identity.source_authority = ? AND identity.object_id = version.object_id
JOIN source_object_bindings binding
  ON binding.source_authority = identity.source_authority AND binding.source_key = identity.source_key
 AND binding.tenant = version.tenant
WHERE version.tenant = ? AND version.revision <= ?
  AND (version.object_id > ? OR (version.object_id = ? AND version.revision > ?))
ORDER BY version.object_id, version.revision LIMIT ?`, string(identity.Authority), string(target.tenant),
		uint64(target.predecessorHead), target.cursorObject, target.cursorObject,
		uint64(target.cursorRevision), sourceDriverPreparationPageLimit)
	if err != nil {
		return 0, 0, err
	}
	type rawVersion struct {
		obj     Object
		nameKey string
	}
	values := make([]rawVersion, 0, sourceDriverPreparationPageLimit)
	used := 0
	for rows.Next() {
		var value rawVersion
		var rawID, rawParent, rawHash []byte
		var kind uint8
		var mount, provider bool
		if err := rows.Scan(&rawID, &rawParent, &value.obj.Revision, &value.obj.MetadataRevision,
			&value.obj.ContentRevision, &value.obj.Name, &value.nameKey, &kind, &value.obj.Mode,
			&value.obj.Size, &rawHash, &value.obj.LinkTarget, &value.obj.Convergence.Desired,
			&value.obj.Convergence.Observed, &value.obj.Convergence.Verified,
			&value.obj.Convergence.Applied, &mount, &provider, &value.obj.Tombstone); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		value.obj.ID, err = objectID(rawID)
		if err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		value.obj.Parent, err = objectID(rawParent)
		if err != nil || len(rawHash) != len(ContentHash{}) {
			_ = rows.Close()
			return 0, 0, ErrIntegrity
		}
		copy(value.obj.Hash[:], rawHash)
		value.obj.Tenant = target.tenant
		value.obj.Kind = Kind(kind)
		value.obj.Visibility = Visibility{Mount: mount, FileProvider: provider}
		rowBytes := len(value.obj.Name) + len(value.nameKey) + len(value.obj.LinkTarget) + 256
		if rowBytes > sourceDriverPreparationByteLimit || (len(values) != 0 && used+rowBytes > sourceDriverPreparationByteLimit) {
			_ = rows.Close()
			if len(values) == 0 {
				return 0, 0, fmt.Errorf("%w: source version exceeds byte limit", ErrInvalidObject)
			}
			break
		}
		values = append(values, value)
		used += rowBytes
	}
	if err := rows.Close(); err != nil {
		return 0, 0, err
	}
	if len(values) == 0 {
		if err := advanceSourceDriverPreparedTarget(ctx, tx, identity, target, sourceDriverTargetBaselineChanges); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}
	for _, value := range values {
		obj := value.obj
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_versions(
    source_authority, publication_id, tenant, object_id, parent_id, revision,
    metadata_revision, content_revision, name, name_key, kind, mode, size, hash,
    link_target, desired_revision, observed_revision, verified_revision, applied_revision,
    mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(identity.Authority), identity.Operation[:], string(obj.Tenant), obj.ID[:], obj.Parent[:],
			uint64(obj.Revision), uint64(obj.MetadataRevision), uint64(obj.ContentRevision), obj.Name,
			value.nameKey, uint8(obj.Kind), obj.Mode, obj.Size, obj.Hash[:], obj.LinkTarget,
			uint64(obj.Convergence.Desired), uint64(obj.Convergence.Observed),
			uint64(obj.Convergence.Verified), uint64(obj.Convergence.Applied),
			obj.Visibility.Mount, obj.Visibility.FileProvider, obj.Tombstone); err != nil {
			return 0, 0, mapConstraint(err)
		}
		target.cursorObject = append(target.cursorObject[:0], obj.ID[:]...)
		target.cursorRevision = obj.Revision
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets SET cursor_object_id = ?, cursor_revision = ?
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND phase = ?`,
		target.cursorObject, uint64(target.cursorRevision), string(identity.Authority), identity.Operation[:],
		string(target.tenant), sourceDriverTargetVersions); err != nil {
		return 0, 0, err
	}
	return len(values), used, nil
}

func prepareSourceDriverBaselineChangePage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
) (int, int, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT change.rowid, change.revision, change.scope_kind, change.presentation,
       change.scope_parent, change.scope_domain, change.scope_generation,
       change.sequence, change.kind, change.object_id, change.object_revision
FROM changes change
JOIN source_object_ids identity
  ON identity.source_authority = ? AND identity.object_id = change.object_id
JOIN source_object_bindings binding
  ON binding.source_authority = identity.source_authority AND binding.source_key = identity.source_key
 AND binding.tenant = change.tenant
WHERE change.tenant = ? AND change.revision <= ? AND change.rowid > ?
ORDER BY change.rowid LIMIT ?`, string(identity.Authority), string(target.tenant),
		uint64(target.predecessorHead), uint64(target.cursorRevision), sourceDriverPreparationPageLimit)
	if err != nil {
		return 0, 0, err
	}
	type baselineChange struct {
		rowID, revision, scopeGeneration, sequence, objectRevision uint64
		scopeKind, presentation, kind                              uint8
		scopeParent, objectID                                      []byte
		scopeDomain                                                string
	}
	values := make([]baselineChange, 0, sourceDriverPreparationPageLimit)
	used := 0
	for rows.Next() {
		var value baselineChange
		if err := rows.Scan(&value.rowID, &value.revision, &value.scopeKind, &value.presentation,
			&value.scopeParent, &value.scopeDomain, &value.scopeGeneration, &value.sequence,
			&value.kind, &value.objectID, &value.objectRevision); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		rowBytes := len(value.scopeDomain) + 96
		if rowBytes > sourceDriverPreparationByteLimit || (len(values) != 0 && used+rowBytes > sourceDriverPreparationByteLimit) {
			_ = rows.Close()
			if len(values) == 0 {
				return 0, 0, ErrInvalidObject
			}
			break
		}
		values = append(values, value)
		used += rowBytes
	}
	if err := rows.Close(); err != nil {
		return 0, 0, err
	}
	if len(values) == 0 {
		if err := advanceSourceDriverPreparedTarget(ctx, tx, identity, target, sourceDriverTargetValidate); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}
	for _, value := range values {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_changes(
    source_authority, publication_id, tenant, revision, scope_kind, presentation,
    scope_parent, scope_domain, scope_generation, sequence, kind, object_id, object_revision
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(identity.Authority), identity.Operation[:], string(target.tenant), value.revision,
			value.scopeKind, value.presentation, value.scopeParent, value.scopeDomain,
			value.scopeGeneration, value.sequence, value.kind, value.objectID, value.objectRevision); err != nil {
			return 0, 0, mapConstraint(err)
		}
		target.cursorRevision = Revision(value.rowID)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets SET cursor_revision = ?
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND phase = ?`,
		uint64(target.cursorRevision), string(identity.Authority), identity.Operation[:],
		string(target.tenant), sourceDriverTargetBaselineChanges)
	if err != nil {
		return 0, 0, err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return 0, 0, ErrMutationConflict
	}
	return len(values), used, nil
}

func validateSourceDriverPreparedPage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
) (int, int, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT object_id, parent_id, name_key, mount_visible, file_provider_visible, tombstone
FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND object_id > ?
ORDER BY object_id LIMIT ?`, string(identity.Authority), identity.Operation[:], string(target.tenant),
		target.cursorObject, sourceDriverPreparationPageLimit)
	if err != nil {
		return 0, 0, err
	}
	type row struct {
		id, parent []byte
		nameKey    string
		mount      bool
		provider   bool
		tombstone  bool
	}
	values := make([]row, 0, sourceDriverPreparationPageLimit)
	for rows.Next() {
		var value row
		if err := rows.Scan(&value.id, &value.parent, &value.nameKey, &value.mount,
			&value.provider, &value.tombstone); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		values = append(values, value)
	}
	if err := rows.Close(); err != nil {
		return 0, 0, err
	}
	if len(values) == 0 {
		if err := advanceSourceDriverPreparedTarget(
			ctx, tx, identity, target, sourceDriverTargetCatalogFingerprint,
		); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}
	for _, value := range values {
		if value.tombstone {
			var child int
			if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM objects child
    LEFT JOIN source_object_ids identity
      ON identity.source_authority = ? AND identity.object_id = child.object_id
    LEFT JOIN source_object_bindings binding
      ON binding.source_authority = identity.source_authority
     AND binding.source_key = identity.source_key AND binding.tenant = child.tenant
    WHERE child.tenant = ? AND child.parent_id = ? AND child.tombstone = 0
      AND binding.source_key IS NULL
)`, string(identity.Authority), string(target.tenant), value.id).Scan(&child); err != nil {
				return 0, 0, err
			}
			if child != 0 {
				return 0, 0, ErrConflict
			}
		}
		if !value.tombstone && (value.mount || value.provider) {
			var collision int
			if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM objects existing
    LEFT JOIN source_object_ids identity
      ON identity.source_authority = ? AND identity.object_id = existing.object_id
    LEFT JOIN source_object_bindings binding
      ON binding.source_authority = identity.source_authority
     AND binding.source_key = identity.source_key AND binding.tenant = existing.tenant
    WHERE existing.tenant = ? AND existing.parent_id = ? AND existing.name_key = ?
      AND existing.tombstone = 0 AND existing.object_id <> ? AND binding.source_key IS NULL
      AND ((? = 1 AND existing.mount_visible = 1)
        OR (? = 1 AND existing.file_provider_visible = 1))
)`, string(identity.Authority), string(target.tenant), value.parent, value.nameKey, value.id,
				value.mount, value.provider).Scan(&collision); err != nil {
				return 0, 0, err
			}
			if collision != 0 {
				return 0, 0, ErrConflict
			}
		}
		if !value.tombstone {
			var root []byte
			if err := tx.QueryRowContext(ctx, `SELECT root_id FROM tenants WHERE tenant = ?`,
				string(target.tenant)).Scan(&root); err != nil {
				return 0, 0, err
			}
			if !bytes.Equal(value.parent, root) {
				var parent int
				if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND object_id = ? AND tombstone = 0)`, string(identity.Authority), identity.Operation[:],
					string(target.tenant), value.parent).Scan(&parent); err != nil {
					return 0, 0, err
				}
				if parent == 0 {
					return 0, 0, ErrConflict
				}
			}
		}
		target.cursorObject = append(target.cursorObject[:0], value.id...)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets SET cursor_object_id = ?
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND phase = ?`,
		target.cursorObject, string(identity.Authority), identity.Operation[:], string(target.tenant),
		sourceDriverTargetValidate); err != nil {
		return 0, 0, err
	}
	return len(values), len(values) * 64, nil
}

func fingerprintSourceDriverCatalogPage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
) (int, int, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT source_key, parent_id, name, kind, mode, size, hash, link_target,
       mount_visible, file_provider_visible
FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND tombstone = 0 AND source_key > ?
ORDER BY source_key LIMIT ?`, string(identity.Authority), identity.Operation[:], string(target.tenant),
		target.cursorKey, sourceDriverPreparationPageLimit)
	if err != nil {
		return 0, 0, err
	}
	values := make([][]byte, 0, sourceDriverPreparationPageLimit)
	keys := make([]string, 0, sourceDriverPreparationPageLimit)
	used := 0
	for rows.Next() {
		var key, name, link string
		var parent, hash []byte
		var kind uint8
		var mode uint32
		var size int64
		var mount, provider bool
		if err := rows.Scan(&key, &parent, &name, &kind, &mode, &size, &hash, &link,
			&mount, &provider); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		encoded := encodeSourceDriverFingerprintRow(key, parent, name, kind, mode, size, hash, link, mount, provider)
		if len(encoded) > sourceDriverPreparationByteLimit || (len(values) != 0 && used+len(encoded) > sourceDriverPreparationByteLimit) {
			_ = rows.Close()
			if len(values) == 0 {
				return 0, 0, ErrInvalidObject
			}
			break
		}
		values = append(values, encoded)
		keys = append(keys, key)
		used += len(encoded)
	}
	if err := rows.Close(); err != nil {
		return 0, 0, err
	}
	if len(values) == 0 {
		if err := advanceSourceDriverPreparedTarget(
			ctx, tx, identity, target, sourceDriverTargetProviderFingerprint,
		); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}
	digest := target.catalogFingerprint
	for _, encoded := range values {
		digest = chainSourceDriverFingerprint("fusekit.source-driver-catalog.v1", digest, encoded)
	}
	target.catalogFingerprint = digest
	target.cursorKey = keys[len(keys)-1]
	if _, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets
SET cursor_key = ?, catalog_fingerprint = ?
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND phase = ?`,
		target.cursorKey, digest[:], string(identity.Authority), identity.Operation[:],
		string(target.tenant), sourceDriverTargetCatalogFingerprint); err != nil {
		return 0, 0, err
	}
	return len(values), used, nil
}

func fingerprintSourceDriverProviderPage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
) (int, int, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT object_id, parent_id, name, kind, mode, size, hash, link_target
FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND tombstone = 0 AND file_provider_visible = 1 AND object_id > ?
ORDER BY object_id LIMIT ?`, string(identity.Authority), identity.Operation[:], string(target.tenant),
		target.cursorObject, sourceDriverPreparationPageLimit)
	if err != nil {
		return 0, 0, err
	}
	values := make([][]byte, 0, sourceDriverPreparationPageLimit)
	ids := make([][]byte, 0, sourceDriverPreparationPageLimit)
	used := 0
	for rows.Next() {
		var id, parent, hash []byte
		var name, link string
		var kind uint8
		var mode uint32
		var size int64
		if err := rows.Scan(&id, &parent, &name, &kind, &mode, &size, &hash, &link); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		encoded := encodeSourceDriverFingerprintRow(string(id), parent, name, kind, mode, size, hash, link, true, true)
		if len(encoded) > sourceDriverPreparationByteLimit || (len(values) != 0 && used+len(encoded) > sourceDriverPreparationByteLimit) {
			_ = rows.Close()
			if len(values) == 0 {
				return 0, 0, ErrInvalidObject
			}
			break
		}
		values = append(values, encoded)
		ids = append(ids, append([]byte(nil), id...))
		used += len(encoded)
	}
	if err := rows.Close(); err != nil {
		return 0, 0, err
	}
	if len(values) == 0 {
		if err := advanceSourceDriverPreparedTarget(ctx, tx, identity, target, sourceDriverTargetChanges); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}
	digest := target.fileProviderFingerprint
	for _, encoded := range values {
		digest = chainSourceDriverFingerprint("fusekit.source-driver-file-provider.v1", digest, encoded)
	}
	target.fileProviderFingerprint = digest
	target.cursorObject = ids[len(ids)-1]
	if _, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets
SET cursor_object_id = ?, file_provider_fingerprint = ?
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND phase = ?`,
		target.cursorObject, digest[:], string(identity.Authority), identity.Operation[:],
		string(target.tenant), sourceDriverTargetProviderFingerprint); err != nil {
		return 0, 0, err
	}
	return len(values), used, nil
}

func prepareSourceDriverChangePage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
) (int, int, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT object_id, parent_id, revision, mount_visible, file_provider_visible, tombstone
FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND revision = ? AND object_id > ?
ORDER BY object_id LIMIT 20`, string(identity.Authority), identity.Operation[:], string(target.tenant),
		uint64(target.predecessorHead+1), target.cursorObject)
	if err != nil {
		return 0, 0, err
	}
	type value struct {
		id, parent []byte
		revision   uint64
		mount      bool
		provider   bool
		tombstone  bool
	}
	values := make([]value, 0, 20)
	for rows.Next() {
		var item value
		if err := rows.Scan(&item.id, &item.parent, &item.revision, &item.mount,
			&item.provider, &item.tombstone); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		values = append(values, item)
	}
	if err := rows.Close(); err != nil {
		return 0, 0, err
	}
	if len(values) == 0 {
		if err := advanceSourceDriverPreparedTarget(ctx, tx, identity, target, sourceDriverTargetInterestChanges); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}
	inserted := 0
	for _, item := range values {
		baseline, baselineFound, err := readSourceDriverPreparationBaselineByID(ctx, tx, identity, *target, item.id)
		if err != nil {
			return 0, 0, err
		}
		if baselineFound && !baseline.object.Tombstone && baseline.object.Visibility.Mount &&
			(item.tombstone || !item.mount || !bytes.Equal(item.parent, baseline.object.Parent[:])) {
			if err := insertSourceDriverPreparedChange(ctx, tx, identity, target, 2, 1,
				baseline.object.Parent[:], "", 0, ChangeDelete, item.id, baseline.object.Revision); err != nil {
				return 0, 0, err
			}
			inserted++
		}
		if baselineFound && !baseline.object.Tombstone && baseline.object.Visibility.FileProvider &&
			(item.tombstone || !item.provider || !bytes.Equal(item.parent, baseline.object.Parent[:])) {
			if err := insertSourceDriverPreparedChange(ctx, tx, identity, target, 2, 2,
				baseline.object.Parent[:], "", 0, ChangeDelete, item.id, baseline.object.Revision); err != nil {
				return 0, 0, err
			}
			inserted++
		}
		if !item.tombstone && item.mount {
			if err := insertSourceDriverPreparedChange(ctx, tx, identity, target, 2, 1,
				item.parent, "", 0, ChangeUpsert, item.id, Revision(item.revision)); err != nil {
				return 0, 0, err
			}
			inserted++
		}
		if !item.tombstone && item.provider {
			if err := insertSourceDriverPreparedChange(ctx, tx, identity, target, 2, 2,
				item.parent, "", 0, ChangeUpsert, item.id, Revision(item.revision)); err != nil {
				return 0, 0, err
			}
			inserted++
		}
		target.cursorObject = append(target.cursorObject[:0], item.id...)
	}
	if inserted > sourceDriverPreparationPageLimit {
		return 0, 0, ErrIntegrity
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets SET cursor_object_id = ?, next_change_sequence = ?
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND phase = ?`,
		target.cursorObject, target.nextChangeSequence, string(identity.Authority), identity.Operation[:],
		string(target.tenant), sourceDriverTargetChanges); err != nil {
		return 0, 0, err
	}
	return inserted, inserted * 96, nil
}

func prepareSourceDriverInterestChangePage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
) (int, int, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT object.object_id, object.revision, object.file_provider_visible, object.tombstone,
       interest.owner_domain, interest.owner_generation
FROM source_driver_publication_objects object
JOIN materialization_interests interest
  ON interest.tenant = object.tenant AND interest.object_id = object.object_id
 AND interest.owner_presentation = 2 AND interest.removed_revision IS NULL
WHERE object.source_authority = ? AND object.publication_id = ? AND object.tenant = ?
  AND object.revision = ?
  AND (object.object_id > ?
    OR (object.object_id = ? AND interest.owner_domain > ?)
    OR (object.object_id = ? AND interest.owner_domain = ? AND interest.owner_generation > ?))
ORDER BY object.object_id, interest.owner_domain, interest.owner_generation
LIMIT ?`, string(identity.Authority), identity.Operation[:], string(target.tenant),
		uint64(target.predecessorHead+1), target.cursorObject, target.cursorObject, target.cursorKey,
		target.cursorObject, target.cursorKey, uint64(target.cursorRevision), sourceDriverPreparationPageLimit)
	if err != nil {
		return 0, 0, err
	}
	type interestChange struct {
		id         []byte
		revision   Revision
		provider   bool
		tombstone  bool
		domain     string
		generation uint64
	}
	values := make([]interestChange, 0, sourceDriverPreparationPageLimit)
	used := 0
	for rows.Next() {
		var value interestChange
		if err := rows.Scan(&value.id, &value.revision, &value.provider, &value.tombstone,
			&value.domain, &value.generation); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		rowBytes := len(value.domain) + 96
		if rowBytes > sourceDriverPreparationByteLimit || (len(values) != 0 && used+rowBytes > sourceDriverPreparationByteLimit) {
			_ = rows.Close()
			if len(values) == 0 {
				return 0, 0, ErrInvalidObject
			}
			break
		}
		values = append(values, value)
		used += rowBytes
	}
	if err := rows.Close(); err != nil {
		return 0, 0, err
	}
	if len(values) == 0 {
		if err := advanceSourceDriverPreparedTarget(ctx, tx, identity, target, sourceDriverTargetPrepared); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}
	zeroParent := make([]byte, len(ObjectID{}))
	for _, value := range values {
		baseline, baselineFound, err := readSourceDriverPreparationBaselineByID(
			ctx, tx, identity, *target, value.id,
		)
		if err != nil {
			return 0, 0, err
		}
		kind := ChangeUpsert
		objectRevision := value.revision
		if value.tombstone || !value.provider {
			if !baselineFound || baseline.object.Tombstone || !baseline.object.Visibility.FileProvider {
				target.cursorObject = append(target.cursorObject[:0], value.id...)
				target.cursorKey = value.domain
				target.cursorRevision = Revision(value.generation)
				continue
			}
			kind = ChangeDelete
			objectRevision = baseline.object.Revision
		}
		if err := insertSourceDriverPreparedChange(
			ctx, tx, identity, target, 1, 2, zeroParent, value.domain, value.generation,
			kind, value.id, objectRevision,
		); err != nil {
			return 0, 0, err
		}
		target.cursorObject = append(target.cursorObject[:0], value.id...)
		target.cursorKey = value.domain
		target.cursorRevision = Revision(value.generation)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets
SET cursor_object_id = ?, cursor_key = ?, cursor_revision = ?, next_change_sequence = ?
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND phase = ?`,
		target.cursorObject, target.cursorKey, uint64(target.cursorRevision), target.nextChangeSequence,
		string(identity.Authority), identity.Operation[:], string(target.tenant),
		sourceDriverTargetInterestChanges)
	if err != nil {
		return 0, 0, err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return 0, 0, ErrMutationConflict
	}
	return len(values), used, nil
}

func readSourceDriverPreparationBaselineByID(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target sourceDriverPreparedTarget,
	id []byte,
) (sourceDriverPreparedObject, bool, error) {
	var predecessor []byte
	if err := tx.QueryRowContext(ctx, `
SELECT predecessor_publication_id FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&predecessor); err != nil {
		return sourceDriverPreparedObject{}, false, err
	}
	query := `SELECT source_key, object_id, parent_id, revision, metadata_revision, content_revision,
       name, name_key, kind, mode, size, hash, link_target, desired_revision, observed_revision,
       verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND object_id = ?`
	args := []any{string(identity.Authority), predecessor, string(target.tenant), id}
	if len(predecessor) == 0 {
		query = `SELECT binding.source_key, object.object_id, object.parent_id, object.revision,
       object.metadata_revision, object.content_revision, object.name, object.name_key,
       object.kind, object.mode, object.size, object.hash, object.link_target,
       object.desired_revision, object.observed_revision, object.verified_revision,
       object.applied_revision, object.mount_visible, object.file_provider_visible, object.tombstone
FROM objects object
JOIN source_object_ids identity
  ON identity.source_authority = ? AND identity.object_id = object.object_id
JOIN source_object_bindings binding
  ON binding.source_authority = identity.source_authority AND binding.source_key = identity.source_key
 AND binding.tenant = object.tenant
WHERE object.tenant = ? AND object.object_id = ?`
		args = []any{string(identity.Authority), string(target.tenant), id}
	}
	value, err := scanSourceDriverPreparedObject(tx.QueryRowContext(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return sourceDriverPreparedObject{}, false, nil
	}
	if err != nil {
		return sourceDriverPreparedObject{}, false, err
	}
	value.object.Tenant = target.tenant
	return value, true, nil
}

func insertSourceDriverPreparedChange(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
	scopeKind, presentation uint8,
	parent []byte,
	domain string,
	generation uint64,
	kind ChangeKind,
	id []byte,
	revision Revision,
) error {
	if target.nextChangeSequence >= uint64(CompleteChangeSequence) {
		return ErrIntegrity
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_changes(
    source_authority, publication_id, tenant, revision, scope_kind, presentation,
    scope_parent, scope_domain, scope_generation, sequence, kind, object_id, object_revision
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(identity.Authority), identity.Operation[:], string(target.tenant),
		uint64(target.predecessorHead+1), scopeKind, presentation, parent, domain, generation,
		target.nextChangeSequence, uint8(kind), id, uint64(revision))
	if err != nil {
		return mapConstraint(err)
	}
	target.nextChangeSequence++
	return nil
}

func finalizeSourceDriverPreparedTarget(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
	state *SourceDriverPreparationState,
) error {
	head := target.predecessorHead
	if target.changed {
		head++
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets
SET catalog_head = ?, prepared = 1
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND phase = ? AND prepared = 0`, uint64(head), string(identity.Authority), identity.Operation[:],
		string(target.tenant), sourceDriverTargetPrepared)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrMutationConflict
	}
	var rolling []byte
	if err := tx.QueryRowContext(ctx, `
SELECT rolling_digest FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ? AND phase = ?`,
		string(identity.Authority), identity.Operation[:], sourceDriverPublicationPreparing).Scan(&rolling); err != nil {
		return err
	}
	if len(rolling) != sha256.Size {
		return ErrIntegrity
	}
	var prior [sha256.Size]byte
	copy(prior[:], rolling)
	digest := chainSourceDriverFingerprint(
		"fusekit.source-driver-publication-target.v1", prior,
		encodeSourceDriverPreparedTargetDigest(*target, head),
	)
	result, err = tx.ExecContext(ctx, `
UPDATE source_driver_publications
SET prepared_target_count = prepared_target_count + 1, rolling_digest = ?
WHERE source_authority = ? AND publication_id = ? AND phase = ?
	  AND prepared_target_count < target_count`, digest[:], string(identity.Authority), identity.Operation[:],
		sourceDriverPublicationPreparing)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrIntegrity
	}
	target.catalogHead = head
	state.PreparedTargets++
	return nil
}

func advanceSourceDriverPreparedTarget(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target *sourceDriverPreparedTarget,
	next uint8,
) error {
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_targets
SET phase = ?, cursor_key = '', cursor_object_id = zeroblob(0), cursor_revision = 0
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND phase = ?`,
		next, string(identity.Authority), identity.Operation[:], string(target.tenant), target.phase)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrMutationConflict
	}
	target.phase = next
	target.cursorKey = ""
	target.cursorObject = nil
	target.cursorRevision = 0
	return nil
}

func encodeSourceDriverFingerprintRow(
	key string,
	parent []byte,
	name string,
	kind uint8,
	mode uint32,
	size int64,
	hash []byte,
	link string,
	mount, provider bool,
) []byte {
	buffer := bytes.NewBuffer(make([]byte, 0, len(key)+len(parent)+len(name)+len(hash)+len(link)+64))
	write := func(value []byte) {
		_ = binary.Write(buffer, binary.BigEndian, uint64(len(value)))
		_, _ = buffer.Write(value)
	}
	write([]byte(key))
	write(parent)
	write([]byte(name))
	_ = buffer.WriteByte(kind)
	_ = binary.Write(buffer, binary.BigEndian, mode)
	_ = binary.Write(buffer, binary.BigEndian, size)
	write(hash)
	write([]byte(link))
	_ = buffer.WriteByte(byte(sourceDriverBoolInt(mount)))
	_ = buffer.WriteByte(byte(sourceDriverBoolInt(provider)))
	return buffer.Bytes()
}

func chainSourceDriverFingerprint(domain string, prior [sha256.Size]byte, row []byte) [sha256.Size]byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte(domain))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(prior[:])
	_, _ = digest.Write(row)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func encodeSourceDriverPreparedTargetDigest(target sourceDriverPreparedTarget, head Revision) []byte {
	buffer := bytes.NewBuffer(make([]byte, 0, len(target.tenant)+len(target.root)+160))
	write := func(value []byte) {
		_ = binary.Write(buffer, binary.BigEndian, uint64(len(value)))
		_, _ = buffer.Write(value)
	}
	write([]byte(target.tenant))
	_ = binary.Write(buffer, binary.BigEndian, uint64(target.generation))
	write([]byte(target.root))
	_ = binary.Write(buffer, binary.BigEndian, uint64(target.predecessorHead))
	_ = binary.Write(buffer, binary.BigEndian, uint64(head))
	write(target.catalogFingerprint[:])
	write(target.fileProviderFingerprint[:])
	_ = buffer.WriteByte(byte(sourceDriverBoolInt(target.changed)))
	_ = buffer.WriteByte(byte(sourceDriverBoolInt(target.providerChanged)))
	_ = binary.Write(buffer, binary.BigEndian, target.objectCount)
	return buffer.Bytes()
}

func sourceDriverBoolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
