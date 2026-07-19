package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

const (
	sourceDriverAfterContentClaimPoint = "source_driver.after_content_claim"
)

// PendingSourceDriverStage returns the one authority actor's durable pending stage.
func (c *Catalog) PendingSourceDriverStage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
) (*SourceDriverStageState, error) {
	state, found, err := readSourceDriverStageState(ctx, c.readDB, authority)
	if err != nil || !found {
		return nil, err
	}
	return &state, nil
}

// ValidateSourceDriverTargetEpoch proves the authority's current target-set fence in O(1).
func (c *Catalog) ValidateSourceDriverTargetEpoch(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	targetEpoch uint64,
) error {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil || targetEpoch == 0 {
		return ErrInvalidTransition
	}
	current, err := c.SourceDriverTargetEpoch(ctx, authority)
	if errors.Is(err, ErrNotFound) || err == nil && current != targetEpoch {
		return ErrGenerationMismatch
	}
	return err
}

// SourceDriverTargetEpoch returns the authority's monotonic target topology fence in O(1).
func (c *Catalog) SourceDriverTargetEpoch(
	ctx context.Context,
	authority causal.SourceAuthorityID,
) (uint64, error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil {
		return 0, ErrInvalidObject
	}
	var current uint64
	err := c.readDB.QueryRowContext(ctx, `
SELECT target_epoch FROM source_driver_target_epochs WHERE source_authority = ?`,
		string(authority),
	).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	if current == 0 {
		return 0, ErrIntegrity
	}
	return current, nil
}

// BeginSourceDriverStage begins one authority-wide, target-set-fenced publication.
func (c *Catalog) BeginSourceDriverStage(ctx context.Context, identity SourceDriverStageIdentity) error {
	identityDigest, err := validateSourceDriverStageIdentity(identity)
	if err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source driver stage: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var receiptDigest []byte
	err = tx.QueryRowContext(ctx, `
SELECT identity_digest FROM source_driver_stage_receipts
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&receiptDigest)
	if err == nil {
		if !bytes.Equal(receiptDigest, identityDigest[:]) {
			return ErrMutationConflict
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := requireSourceDriverFleetMember(
		ctx, tx, identity.Authority, identity.FleetOwner,
		identity.AuthorityGeneration, identity.DeclarationDigest,
	); err != nil {
		return err
	}
	var driverID string
	if err := tx.QueryRowContext(ctx, `
SELECT driver_id FROM source_authority_fleet_members
WHERE owner_id = ? AND generation = ? AND source_authority = ?`,
		string(identity.FleetOwner), uint64(identity.AuthorityGeneration), string(identity.Authority)).
		Scan(&driverID); err != nil {
		return err
	}
	if err := ValidateSourceDriverID(driverID); err != nil {
		return ErrIntegrity
	}
	var targetEpoch uint64
	if err := tx.QueryRowContext(ctx, `
SELECT target_epoch FROM source_driver_target_epochs WHERE source_authority = ?`,
		string(identity.Authority)).Scan(&targetEpoch); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrMutationConflict
		}
		return err
	}
	targetDigestState, err := newSourceDriverTargetsDigestState(identity.TargetCount)
	if err != nil {
		return err
	}
	checkpoint, checkpointFound, err := readSourceDriverCheckpoint(ctx, tx, identity.Authority)
	if err != nil {
		return err
	}
	if err := validateSourceDriverTransition(identity, targetEpoch, checkpoint, checkpointFound); err != nil {
		return err
	}
	if identity.Mode == SourceDriverMutation {
		if err := validateSourceDriverPreparedMutation(ctx, tx, identity, targetEpoch); err != nil {
			return err
		}
	}
	current, found, err := readSourceDriverStageState(ctx, tx, identity.Authority)
	if err != nil {
		return err
	}
	if found {
		storedDigest, digestErr := validateSourceDriverStageIdentity(current.Identity)
		if digestErr != nil || storedDigest != identityDigest || current.Identity != identity {
			return ErrMutationConflict
		}
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stages(
    source_authority, stage_operation_id, fleet_owner_id, authority_generation,
    driver_id, declaration_digest, stage_kind, stream_identity, root_epoch,
    through_sequence, predecessor_revision, last_revision, next_sequence,
    item_count, byte_count, complete, aborting, identity_digest, rolling_digest
) VALUES (?, ?, ?, ?, ?, ?, 2, '', '', 0, ?, ?, 0, 0, 0, 0, 0, ?, ?)`,
		string(identity.Authority), identity.Operation[:], string(identity.FleetOwner),
		uint64(identity.AuthorityGeneration), driverID, identity.DeclarationDigest[:], uint64(identity.Predecessor),
		uint64(identity.Predecessor), identityDigest[:], identityDigest[:]); err != nil {
		return mapConstraint(err)
	}
	var mutation any
	var requestDigest any
	var mutationReceiptDigestArg any
	var claimOwner any
	var claimEpoch any
	if identity.Mode == SourceDriverMutation {
		mutation = identity.Mutation[:]
		requestDigest = identity.MutationRequestDigest[:]
		mutationReceiptDigestArg = identity.MutationReceiptDigest[:]
		claimOwner = identity.Claim.Owner[:]
		claimEpoch = identity.Claim.Epoch
	}
	fromTokenDigest := sourceDriverTokenDigest(identity.FromToken)
	toTokenDigest := sourceDriverTokenDigest(identity.ToToken)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_stages(
    source_authority, stage_operation_id, fleet_owner_id, authority_generation,
    declaration_digest, target_count, targets_digest, source_operation_id, change_id,
    cause, origin_domain, origin_generation, mode, snapshot_reason,
    from_token, from_token_digest, to_token, to_token_digest, predecessor_revision,
    target_epoch, target_cursor, declared_target_count, target_digest_state, targets_prepared,
    driver_cursor, driver_page_digest, mutation_id, mutation_tenant,
    mutation_generation, mutation_result_key, mutation_request_digest, mutation_receipt_digest,
    claim_owner, claim_epoch, identity_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0, ?, 0, X'', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(identity.Authority), identity.Operation[:], string(identity.FleetOwner),
		uint64(identity.AuthorityGeneration), identity.DeclarationDigest[:], identity.TargetCount,
		identity.TargetsDigest[:], identity.SourceOperation[:], identity.ChangeID[:], string(identity.Cause),
		string(identity.Origin), uint64(identity.OriginGeneration), uint8(identity.Mode),
		uint8(identity.SnapshotReason), identity.FromToken, fromTokenDigest[:],
		identity.ToToken, toTokenDigest[:], uint64(identity.Predecessor), targetEpoch, targetDigestState,
		make([]byte, sha256.Size), mutation, string(identity.MutationTenant), uint64(identity.MutationGeneration),
		string(identity.MutationResult), requestDigest, mutationReceiptDigestArg, claimOwner, claimEpoch, identityDigest[:]); err != nil {
		return mapConstraint(err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit source driver stage begin: %w", err)
	}
	return nil
}

// AppendSourceDriverStage durably appends one exact tuple-ordered page.
func (c *Catalog) AppendSourceDriverStage(
	ctx context.Context,
	identity SourceDriverStageIdentity,
	page SourceDriverStagePage,
) (SourceDriverStageState, error) {
	identityDigest, err := validateSourceDriverStageIdentity(identity)
	if err != nil {
		return SourceDriverStageState{}, err
	}
	pageBytes, err := validateSourceDriverStagePage(identity.Mode, page)
	if err != nil {
		return SourceDriverStageState{}, err
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		return SourceDriverStageState{}, fmt.Errorf("catalog: encode source driver page: %w", err)
	}
	encodedDigest := sha256.Sum256(encoded)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceDriverStageState{}, fmt.Errorf("catalog: begin source driver append: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	state, found, err := readSourceDriverStageState(ctx, tx, identity.Authority)
	if err != nil {
		return SourceDriverStageState{}, err
	}
	if !found {
		return SourceDriverStageState{}, ErrNotFound
	}
	storedDigest, err := validateSourceDriverStageIdentity(state.Identity)
	if err != nil || storedDigest != identityDigest || state.Identity != identity {
		return SourceDriverStageState{}, ErrMutationConflict
	}
	predecessorCursor, predecessorPage, err := readSourceDriverPagePredecessor(
		ctx, tx, identity, state, page.Sequence,
	)
	if err != nil {
		return SourceDriverStageState{}, err
	}
	if page.PredecessorDigest != SourceDriverPagePredecessorDigest(predecessorCursor, predecessorPage) {
		return SourceDriverStageState{}, ErrInvalidTransition
	}
	if page.Sequence < state.Stage.Sequence {
		replayed, err := readSourceDriverStagePageReplay(ctx, tx, identity, page.Sequence, encodedDigest)
		if err != nil {
			return SourceDriverStageState{}, err
		}
		if err := tx.Commit(); err != nil {
			return SourceDriverStageState{}, err
		}
		return replayed, nil
	}
	if page.Sequence != state.Stage.Sequence {
		return SourceDriverStageState{}, ErrInvalidTransition
	}
	var complete, aborting int
	if err := tx.QueryRowContext(ctx, `
SELECT complete, aborting FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&complete, &aborting); err != nil {
		return SourceDriverStageState{}, err
	}
	if complete != 0 || aborting != 0 {
		return SourceDriverStageState{}, ErrInvalidTransition
	}
	if err := validateSourceDriverEntryContinuation(ctx, tx, identity, page.Entries); err != nil {
		return SourceDriverStageState{}, err
	}
	var priorTenant TenantID
	for _, entry := range page.Entries {
		if entry.Tenant != priorTenant {
			if err := ensureSourceDriverStageEntryTarget(ctx, tx, identity, entry); err != nil {
				return SourceDriverStageState{}, err
			}
			priorTenant = entry.Tenant
		}
		if err := c.appendSourceDriverStageEntry(ctx, tx, identity, entry); err != nil {
			return SourceDriverStageState{}, err
		}
	}
	items := uint64(len(page.Entries))
	if items == 0 {
		items = 1
	}
	if state.Stage.Items+items > SourcePublicationStageItemLimit ||
		state.Stage.Bytes+uint64(pageBytes) > SourcePublicationStageByteLimit {
		return SourceDriverStageState{}, fmt.Errorf("%w: source driver stage limit exceeded", ErrInvalidObject)
	}
	if page.Complete {
		state.Stage.Revision = identity.Predecessor + 1
	}
	rolling := sourcePublicationStageRollingDigest(state.Stage.Digest, encodedDigest)
	cursor := page.Cursor
	if cursor == nil {
		cursor = []byte{}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stage_pages(
    source_authority, stage_operation_id, sequence, page_digest, rolling_digest,
    page_item_count, page_byte_count, driver_cursor, driver_page_digest,
    cumulative_revision, cumulative_item_count, cumulative_byte_count
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(identity.Authority), identity.Operation[:],
		page.Sequence, encodedDigest[:], rolling[:], items, pageBytes, cursor, page.Digest[:],
		uint64(state.Stage.Revision), state.Stage.Items+items, state.Stage.Bytes+uint64(pageBytes)); err != nil {
		return SourceDriverStageState{}, mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_publication_stages SET
    last_revision = ?, next_sequence = next_sequence + 1,
    item_count = item_count + ?, byte_count = byte_count + ?, complete = ?, rolling_digest = ?
WHERE source_authority = ? AND stage_operation_id = ? AND next_sequence = ?`,
		uint64(state.Stage.Revision), items, pageBytes, boolInt(page.Complete), rolling[:],
		string(identity.Authority), identity.Operation[:], page.Sequence); err != nil {
		return SourceDriverStageState{}, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_driver_stages SET driver_cursor = ?, driver_page_digest = ?
WHERE source_authority = ? AND stage_operation_id = ?`, cursor, page.Digest[:],
		string(identity.Authority), identity.Operation[:]); err != nil {
		return SourceDriverStageState{}, err
	}
	if err := tx.Commit(); err != nil {
		return SourceDriverStageState{}, fmt.Errorf("catalog: commit source driver append: %w", err)
	}
	state.Stage.Sequence++
	state.Stage.Items += items
	state.Stage.Bytes += uint64(pageBytes)
	state.Stage.Digest = rolling
	state.Cursor = append([]byte(nil), cursor...)
	state.PageDigest = page.Digest
	return state, nil
}

func ensureSourceDriverStageEntryTarget(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	entry SourceDriverStageEntry,
) error {
	var generation uint64
	if err := tx.QueryRowContext(ctx, `
SELECT generation FROM desired_tenants
WHERE tenant = ? AND content_source_id = ?`, string(entry.Tenant), string(identity.Authority)).Scan(&generation); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrGenerationMismatch
		}
		return err
	}
	if Generation(generation) != entry.Generation {
		return ErrGenerationMismatch
	}
	root, err := DeriveSourceDriverRootKey(identity.Authority, entry.Tenant)
	if err != nil {
		return err
	}
	head, _, err := effectiveRevisionState(ctx, tx, entry.Tenant)
	if err != nil {
		return err
	}
	return ensureSourceDriverStageTarget(ctx, tx, identity, sourceDriverTargetState{
		SourceDriverTarget: SourceDriverTarget{Tenant: entry.Tenant, Generation: entry.Generation},
		RootKey:            root, CatalogRevision: head,
	})
}

func ensureSourceDriverStageTarget(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	target sourceDriverTargetState,
) error {
	result, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_stage_targets(
    source_authority, stage_operation_id, tenant, generation, root_key, expected_catalog_revision
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(source_authority, stage_operation_id, tenant) DO UPDATE SET
    generation = source_driver_stage_targets.generation
WHERE source_driver_stage_targets.generation = excluded.generation
  AND source_driver_stage_targets.root_key = excluded.root_key
  AND source_driver_stage_targets.expected_catalog_revision = excluded.expected_catalog_revision`,
		string(identity.Authority), identity.Operation[:], string(target.Tenant), uint64(target.Generation),
		string(target.RootKey), uint64(target.CatalogRevision))
	if err != nil {
		return mapConstraint(err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	return nil
}

func readSourceDriverPagePredecessor(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	state SourceDriverStageState,
	sequence uint64,
) ([]byte, [sha256.Size]byte, error) {
	if sequence > state.Stage.Sequence {
		return nil, [sha256.Size]byte{}, ErrInvalidTransition
	}
	if sequence == state.Stage.Sequence {
		return state.Cursor, state.PageDigest, nil
	}
	if sequence == 0 {
		return nil, [sha256.Size]byte{}, nil
	}
	var cursor, rawPageDigest []byte
	if err := tx.QueryRowContext(ctx, `
SELECT driver_cursor, driver_page_digest
FROM source_publication_stage_pages
WHERE source_authority = ? AND stage_operation_id = ? AND sequence = ?`,
		string(identity.Authority), identity.Operation[:], sequence-1,
	).Scan(&cursor, &rawPageDigest); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	if len(rawPageDigest) != sha256.Size {
		return nil, [sha256.Size]byte{}, ErrIntegrity
	}
	var pageDigest [sha256.Size]byte
	copy(pageDigest[:], rawPageDigest)
	return cursor, pageDigest, nil
}

// AbortSourceDriverStage drops one exact pending semantic publication.
func (c *Catalog) AbortSourceDriverStage(ctx context.Context, identity SourceDriverStageIdentity) error {
	digest, err := validateSourceDriverStageIdentity(identity)
	if err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	state, found, err := readSourceDriverStageState(ctx, tx, identity.Authority)
	if err != nil {
		return err
	}
	if !found {
		return tx.Commit()
	}
	stored, storedErr := validateSourceDriverStageIdentity(state.Identity)
	if storedErr != nil || stored != digest || state.Identity != identity {
		return ErrMutationConflict
	}
	if _, err := tx.ExecContext(ctx, `UPDATE source_publication_stages SET aborting = 1
WHERE source_authority = ? AND stage_operation_id = ?`, string(identity.Authority), identity.Operation[:]); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	after := make([]byte, len(StageID{}))
	for {
		rows, err := c.readDB.QueryContext(ctx, `
SELECT DISTINCT content_stage, content_hash, content_size
FROM source_driver_stage_entries
WHERE source_authority = ? AND stage_operation_id = ?
  AND content_stage IS NOT NULL AND content_stage > ?
ORDER BY content_stage LIMIT ?`, string(identity.Authority), identity.Operation[:], after, ReleaseUnclaimedContentLimit)
		if err != nil {
			return err
		}
		refs := make([]ContentRef, 0, ReleaseUnclaimedContentLimit)
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
			copy(after, rawStage)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if len(refs) != 0 {
			releaseTx, err := c.db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			for _, ref := range refs {
				if _, err := releaseTx.ExecContext(ctx, `
UPDATE content_stages SET source_operation_id = NULL
WHERE stage_id = ? AND source_operation_id = ?`, ref.Stage[:], identity.Operation[:]); err != nil {
					_ = releaseTx.Rollback()
					return err
				}
			}
			if err := releaseTx.Commit(); err != nil {
				return err
			}
			if err := c.ReleaseUnclaimedContent(context.WithoutCancel(ctx), refs); err != nil {
				return err
			}
		}
		if len(refs) < ReleaseUnclaimedContentLimit {
			break
		}
	}
	finalTx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = finalTx.Rollback() }()
	if _, err := finalTx.ExecContext(ctx, `DELETE FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ? AND aborting = 1`,
		string(identity.Authority), identity.Operation[:]); err != nil {
		return err
	}
	return finalTx.Commit()
}

func validateSourceDriverTransition(
	identity SourceDriverStageIdentity,
	targetEpoch uint64,
	checkpoint SourceDriverCheckpoint,
	found bool,
) error {
	if !found {
		if identity.Mode != SourceDriverSnapshot || identity.SnapshotReason != SourceDriverSnapshotInitial ||
			identity.Predecessor != 0 {
			return ErrSourceRequiresSnapshot
		}
		return nil
	}
	if checkpoint.FleetOwner != identity.FleetOwner ||
		checkpoint.AuthorityGeneration != identity.AuthorityGeneration ||
		checkpoint.DeclarationDigest != identity.DeclarationDigest ||
		checkpoint.SourceRevision != identity.Predecessor {
		return ErrSourcePredecessor
	}
	if identity.Mode == SourceDriverMutation {
		if targetEpoch < checkpoint.TargetEpoch {
			return ErrGenerationMismatch
		}
		if checkpoint.Token != identity.FromToken {
			return ErrSourcePredecessor
		}
		topologyChanged := targetEpoch > checkpoint.TargetEpoch ||
			checkpoint.TargetCount != identity.TargetCount || checkpoint.TargetsDigest != identity.TargetsDigest
		if topologyChanged {
			if identity.SnapshotReason != SourceDriverSnapshotReset ||
				(checkpoint.SnapshotRequired != 0 && checkpoint.SnapshotRequired != SourceDriverSnapshotReset) {
				return ErrSourceRequiresSnapshot
			}
			return nil
		}
		if checkpoint.SnapshotRequired != 0 {
			if identity.SnapshotReason != SourceDriverSnapshotReset ||
				checkpoint.SnapshotRequired != SourceDriverSnapshotReset {
				return ErrSourceRequiresSnapshot
			}
			return nil
		}
		if identity.SnapshotReason != 0 {
			return ErrSourceRequiresSnapshot
		}
		return nil
	} else {
		if targetEpoch < checkpoint.TargetEpoch {
			return ErrGenerationMismatch
		}
		if targetEpoch > checkpoint.TargetEpoch {
			if identity.Mode != SourceDriverSnapshot || identity.SnapshotReason != SourceDriverSnapshotReset {
				return ErrSourceRequiresSnapshot
			}
			return nil
		}
		if checkpoint.TargetCount != identity.TargetCount || checkpoint.TargetsDigest != identity.TargetsDigest {
			return ErrGenerationMismatch
		}
	}
	if identity.Mode == SourceDriverSnapshot {
		if checkpoint.SnapshotRequired == 0 || checkpoint.SnapshotRequired != identity.SnapshotReason {
			return ErrSourceRequiresSnapshot
		}
		return nil
	}
	if checkpoint.SnapshotRequired != 0 {
		return ErrSourceRequiresSnapshot
	}
	if checkpoint.Token != identity.FromToken {
		return ErrSourcePredecessor
	}
	return nil
}

func validateSourceDriverPreparedMutation(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	_ uint64,
) error {
	reservation, reserved, err := readSourceDriverMutationReservation(ctx, tx, identity.Mutation)
	if err != nil {
		return err
	}
	if !reserved {
		return ErrNotFound
	}
	if reservation.Claim != identity.Claim || reservation.Authority != identity.Authority ||
		reservation.Target != (SourceDriverTarget{Tenant: identity.MutationTenant, Generation: identity.MutationGeneration}) ||
		reservation.FromToken != identity.FromToken ||
		reservation.Operation != identity.Operation || reservation.SourceOperation != identity.SourceOperation ||
		reservation.ChangeID != identity.ChangeID || reservation.RequestDigest != identity.MutationRequestDigest ||
		!reservation.TargetsPrepared || !reservation.RequestBound || reservation.Committed ||
		reservation.Receipt == nil ||
		reservation.Receipt.ToToken != identity.ToToken || reservation.Receipt.Result != identity.MutationResult ||
		reservation.Receipt.Digest != identity.MutationReceiptDigest {
		return ErrMutationConflict
	}
	record, found, err := readPreparedMutation(ctx, tx, identity.Mutation)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	provision, provisionFound, err := tenantProvision(ctx, tx, identity.MutationTenant)
	if err != nil {
		return err
	}
	if !provisionFound || provision.Generation != identity.MutationGeneration ||
		provision.ContentSourceID != string(identity.Authority) {
		return ErrGenerationMismatch
	}
	if record.Tenant != identity.MutationTenant || record.State != MutationApplying ||
		record.Claim == nil || *record.Claim != identity.Claim || record.Source == nil {
		return ErrMutationClaimed
	}
	if record.Intent.Origin.Cause != identity.Cause || record.Intent.Origin.Domain != identity.Origin ||
		record.Intent.Origin.Generation != identity.OriginGeneration {
		return ErrMutationConflict
	}
	if (record.Kind == MutationCreate) != (identity.MutationResult != "") {
		return ErrMutationConflict
	}
	if record.SourceResult != nil && (record.SourceResult.SourceAuthority != identity.Authority ||
		record.SourceResult.SourceKey != identity.MutationResult) {
		return ErrSourceLocatorStale
	}
	expectedHead, _, err := effectiveRevisionState(ctx, tx, identity.MutationTenant)
	if err != nil {
		return err
	}
	if expectedHead != record.ExpectedHead {
		return ErrMutationConflict
	}
	for _, locator := range []*SourceLocator{record.Source.Object, record.Source.Parent, record.Source.Target} {
		if locator != nil && locator.SourceAuthority != identity.Authority {
			return ErrSourceLocatorStale
		}
	}
	return nil
}

func validateSourceDriverEntryContinuation(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	entries []SourceDriverStageEntry,
) error {
	if len(entries) == 0 {
		return nil
	}
	var tenant, key string
	var generation, sequence uint64
	err := tx.QueryRowContext(ctx, `
SELECT tenant, generation, change_sequence, source_key
FROM source_driver_stage_entries
WHERE source_authority = ? AND stage_operation_id = ?
ORDER BY tenant DESC, generation DESC, change_sequence DESC, source_key DESC LIMIT 1`,
		string(identity.Authority), identity.Operation[:]).Scan(&tenant, &generation, &sequence, &key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	prior := SourceDriverStageEntry{
		Tenant: TenantID(tenant), Generation: Generation(generation),
		ChangeSequence: sequence, Key: SourceObjectKey(key),
	}
	if compareSourceDriverEntry(identity.Mode, prior, entries[0]) >= 0 {
		return ErrInvalidTransition
	}
	return nil
}

func (c *Catalog) appendSourceDriverStageEntry(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	entry SourceDriverStageEntry,
) error {
	var declaredGeneration uint64
	if err := tx.QueryRowContext(ctx, `
SELECT generation FROM source_driver_stage_targets
WHERE source_authority = ? AND stage_operation_id = ? AND tenant = ?`,
		string(identity.Authority), identity.Operation[:], string(entry.Tenant)).Scan(&declaredGeneration); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrGenerationMismatch
		}
		return err
	}
	if Generation(declaredGeneration) != entry.Generation {
		return ErrGenerationMismatch
	}
	action := 1
	object := SourceObject{Content: ContentRef{}}
	if entry.Object != nil {
		action = 2
		object = *entry.Object
	}
	if object.Kind == KindFile {
		if err := c.claimSourceDriverContent(ctx, tx, identity, object.Content); err != nil {
			return err
		}
	}
	var stage any
	if object.Kind == KindFile {
		stage = object.Content.Stage[:]
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_stage_entries(
    source_authority, stage_operation_id, source_key, tenant, generation, change_sequence,
    action, parent_key, object_name, object_kind, object_mode, content_revision,
    content_stage, content_hash, content_size, link_target, mount_visible, file_provider_visible
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(identity.Authority), identity.Operation[:], string(entry.Key), string(entry.Tenant),
		uint64(entry.Generation), entry.ChangeSequence, action, string(object.Parent), object.Name,
		uint8(object.Kind), object.Mode, uint64(object.ContentRevision), stage, object.Content.Hash[:],
		object.Content.Size, object.LinkTarget, boolInt(object.Visibility.Mount),
		boolInt(object.Visibility.FileProvider)); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func (c *Catalog) claimSourceDriverContent(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	ref ContentRef,
) error {
	claimedErr := c.validateSourceContentRef(ctx, tx, identity.Operation, KindFile, ref)
	if claimedErr == nil {
		return nil
	}
	if !errors.Is(claimedErr, ErrNotFound) {
		return claimedErr
	}
	mutationErr := c.verifyMutationContentRef(ctx, tx, identity.Mutation, KindFile, ref)
	if mutationErr == nil {
		result, err := tx.ExecContext(ctx, `
UPDATE content_stages SET mutation_id = NULL, source_operation_id = ?
WHERE stage_id = ? AND mutation_id = ? AND source_operation_id IS NULL AND published = 1`,
			identity.Operation[:], ref.Stage[:], identity.Mutation[:])
		if err != nil {
			return fmt.Errorf("catalog: transfer mutation content to source driver: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("catalog: inspect mutation content transfer: %w", err)
		}
		if changed != 1 {
			return fmt.Errorf("%w: mutation content ownership changed", ErrInvalidTransition)
		}
		return c.trip(sourceDriverAfterContentClaimPoint)
	}
	if !errors.Is(mutationErr, ErrNotFound) {
		return mutationErr
	}
	if err := c.verifyContentRef(ctx, tx, KindFile, ref); err != nil {
		return err
	}
	if err := c.claimSourceContent(ctx, tx, identity.Operation, ref); err != nil {
		return err
	}
	return c.trip(sourceDriverAfterContentClaimPoint)
}

func (c *Catalog) normalizeSourceDriverStageBatch(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
) (bool, error) {
	state, found, err := readSourceDriverStageState(ctx, tx, identity.Authority)
	if err != nil {
		return false, err
	}
	if !found {
		return false, ErrNotFound
	}
	if state.Identity != identity || state.Stage.Sequence == 0 || state.Stage.Revision != identity.Predecessor+1 ||
		len(state.Cursor) != 0 {
		return false, ErrInvalidTransition
	}
	var complete, aborting int
	if err := tx.QueryRowContext(ctx, `
SELECT complete, aborting FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&complete, &aborting); err != nil {
		return false, err
	}
	if complete == 0 || aborting != 0 {
		return false, ErrInvalidTransition
	}
	revision := identity.Predecessor + 1
	mode := SourceDelta
	if identity.Mode == SourceDriverSnapshot {
		mode = SourceSnapshot
	}
	publication := SourcePublicationStageIdentity{
		Authority: identity.Authority, FleetOwner: state.Stage.FleetOwner,
		FleetGeneration: state.Stage.FleetGeneration, DriverID: state.Stage.DriverID,
		DeclarationDigest: state.Stage.DeclarationDigest,
		Operation:         identity.Operation, Predecessor: identity.Predecessor,
	}
	ref := SourcePublicationStageRef{
		Authority: identity.Authority, FleetOwner: state.Stage.FleetOwner,
		FleetGeneration: state.Stage.FleetGeneration, DriverID: state.Stage.DriverID,
		DeclarationDigest: state.Stage.DeclarationDigest,
		Operation:         identity.Operation, Revision: identity.Predecessor,
	}
	header := SourcePublicationStageHeader{Mode: mode, Predecessor: identity.Predecessor, Change: causal.ChangeSet{
		SourceAuthority: identity.Authority, SourceRevision: revision, ChangeID: identity.ChangeID,
		OperationID: identity.SourceOperation, Cause: identity.Cause, Origin: identity.Origin,
		OriginGeneration: identity.OriginGeneration,
	}}
	var revisionComplete int
	err = tx.QueryRowContext(ctx, `
SELECT complete FROM source_publication_stage_revisions
WHERE source_authority = ? AND stage_operation_id = ? AND source_revision = ?`,
		string(identity.Authority), identity.Operation[:], uint64(revision)).Scan(&revisionComplete)
	if errors.Is(err, sql.ErrNoRows) {
		if err := appendSourcePublicationStageHeader(ctx, tx, publication, ref, header); err != nil {
			return false, err
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if revisionComplete != 0 {
		return true, nil
	}
	affected, err := nextSourceDriverAffectedKeys(ctx, tx, identity, revision)
	if err != nil {
		return false, err
	}
	if len(affected) != 0 {
		if err := appendSourcePublicationStageAffected(ctx, tx, publication, affected); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := completeSourcePublicationStageRevision(ctx, tx, publication, revision); err != nil {
		return false, err
	}
	return true, nil
}

func nextSourceDriverAffectedKeys(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	revision causal.Revision,
) ([]SourcePublicationAffected, error) {
	var after string
	if err := tx.QueryRowContext(ctx, `
SELECT last_affected_key FROM source_publication_stage_revisions
WHERE source_authority = ? AND stage_operation_id = ? AND source_revision = ?`,
		string(identity.Authority), identity.Operation[:], uint64(revision)).Scan(&after); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT source_key FROM source_driver_stage_entries
WHERE source_authority = ? AND stage_operation_id = ? AND source_key > ?
ORDER BY source_key LIMIT ?`, string(identity.Authority), identity.Operation[:], after,
		SourcePublicationStagePageItemLimit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]SourcePublicationAffected, 0, SourcePublicationStagePageItemLimit)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		result = append(result, SourcePublicationAffected{Revision: revision, Key: causal.LogicalKey(key)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) != 0 || after != "" {
		return result, nil
	}
	var any int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM source_driver_stage_entries
WHERE source_authority = ? AND stage_operation_id = ?)`,
		string(identity.Authority), identity.Operation[:]).Scan(&any); err != nil {
		return nil, err
	}
	if any == 0 {
		return []SourcePublicationAffected{{Revision: revision, Key: "driver"}}, nil
	}
	return nil, nil
}

func readSourceDriverStagePageReplay(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	sequence uint64,
	digest [sha256.Size]byte,
) (SourceDriverStageState, error) {
	var stored, rolling, cursor, pageDigest []byte
	var revision, items, byteCount, targetEpoch uint64
	if err := tx.QueryRowContext(ctx, `
SELECT page.page_digest, page.rolling_digest, page.driver_cursor, page.driver_page_digest,
       page.cumulative_revision, page.cumulative_item_count, page.cumulative_byte_count,
       driver.target_epoch
FROM source_publication_stage_pages page
JOIN source_driver_stages driver
  ON driver.source_authority = page.source_authority
 AND driver.stage_operation_id = page.stage_operation_id
WHERE page.source_authority = ? AND page.stage_operation_id = ? AND page.sequence = ?`,
		string(identity.Authority), identity.Operation[:], sequence).Scan(
		&stored, &rolling, &cursor, &pageDigest, &revision, &items, &byteCount, &targetEpoch,
	); err != nil {
		return SourceDriverStageState{}, err
	}
	if !bytes.Equal(stored, digest[:]) || len(rolling) != sha256.Size || len(pageDigest) != sha256.Size || targetEpoch == 0 {
		return SourceDriverStageState{}, ErrMutationConflict
	}
	ref, found, err := readSourcePublicationStageOperation(ctx, tx, identity.Authority, identity.Operation)
	if err != nil {
		return SourceDriverStageState{}, err
	}
	if !found || ref.Operation != identity.Operation {
		return SourceDriverStageState{}, ErrMutationConflict
	}
	state := SourceDriverStageState{
		Identity: identity, TargetEpoch: targetEpoch, Cursor: append([]byte(nil), cursor...),
	}
	state.Stage = SourcePublicationStageRef{
		Authority: identity.Authority, FleetOwner: ref.FleetOwner,
		FleetGeneration: ref.FleetGeneration, DriverID: ref.DriverID,
		DeclarationDigest: ref.DeclarationDigest,
		Operation:         identity.Operation, Revision: causal.Revision(revision),
		Sequence: sequence + 1, Items: items, Bytes: byteCount,
	}
	copy(state.Stage.Digest[:], rolling)
	copy(state.PageDigest[:], pageDigest)
	return state, nil
}

func readSourceDriverStageState(
	ctx context.Context,
	query sourceDriverCheckpointQueryer,
	authority causal.SourceAuthorityID,
) (SourceDriverStageState, bool, error) {
	var ref SourcePublicationStageRef
	var stageOperation, stageDeclaration, stageDigest []byte
	var stageOwner, driverID string
	var fleetGeneration, revision, sequence, items, byteCount uint64
	err := query.QueryRowContext(ctx, `
SELECT publication.stage_operation_id, publication.fleet_owner_id,
       publication.authority_generation, publication.driver_id,
       publication.declaration_digest, publication.through_sequence,
       publication.last_revision, publication.next_sequence,
       publication.item_count, publication.byte_count, publication.rolling_digest
FROM source_publication_stages publication
JOIN source_driver_stages driver
  ON driver.source_authority = publication.source_authority
 AND driver.stage_operation_id = publication.stage_operation_id
LEFT JOIN source_driver_stage_receipts receipt
  ON receipt.source_authority = publication.source_authority
 AND receipt.stage_operation_id = publication.stage_operation_id
WHERE publication.source_authority = ? AND receipt.stage_operation_id IS NULL
ORDER BY publication.rowid LIMIT 1`, string(authority)).Scan(
		&stageOperation, &stageOwner, &fleetGeneration, &driverID, &stageDeclaration,
		&ref.Through, &revision, &sequence, &items, &byteCount, &stageDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceDriverStageState{}, false, nil
	}
	if err != nil {
		return SourceDriverStageState{}, false, err
	}
	if len(stageOperation) != len(causal.OperationID{}) || len(stageDeclaration) != sha256.Size || len(stageDigest) != sha256.Size {
		return SourceDriverStageState{}, false, ErrIntegrity
	}
	ref.Authority = authority
	ref.FleetOwner = SourceAuthorityFleetOwnerID(stageOwner)
	ref.FleetGeneration = causal.Generation(fleetGeneration)
	ref.DriverID = driverID
	copy(ref.DeclarationDigest[:], stageDeclaration)
	copy(ref.Operation[:], stageOperation)
	ref.Revision = causal.Revision(revision)
	ref.Sequence, ref.Items, ref.Bytes = sequence, items, byteCount
	copy(ref.Digest[:], stageDigest)
	var state SourceDriverStageState
	var owner, cause, origin, fromToken, toToken, mutationTenant, mutationResult string
	var declaration, targets, operation, change, cursor, pageDigest, mutation, mutationRequestDigest, mutationReceiptDigest, claimOwner, identityDigest []byte
	var authorityGeneration, targetCount, originGeneration, mode, snapshotReason, predecessor, targetEpoch, mutationGeneration uint64
	var claimEpoch sql.NullInt64
	err = query.QueryRowContext(ctx, `
SELECT fleet_owner_id, authority_generation, declaration_digest, target_count, targets_digest,
       source_operation_id, change_id, cause, origin_domain, origin_generation, mode,
       snapshot_reason, from_token, to_token, predecessor_revision, target_epoch, driver_cursor,
       driver_page_digest, mutation_id, mutation_tenant, mutation_generation,
       mutation_result_key, mutation_request_digest, mutation_receipt_digest,
       claim_owner, claim_epoch, identity_digest
FROM source_driver_stages WHERE source_authority = ? AND stage_operation_id = ?`,
		string(authority), ref.Operation[:]).Scan(
		&owner, &authorityGeneration, &declaration, &targetCount, &targets, &operation, &change,
		&cause, &origin, &originGeneration, &mode, &snapshotReason, &fromToken, &toToken,
		&predecessor, &targetEpoch, &cursor, &pageDigest, &mutation, &mutationTenant, &mutationGeneration,
		&mutationResult, &mutationRequestDigest, &mutationReceiptDigest,
		&claimOwner, &claimEpoch, &identityDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceDriverStageState{}, false, ErrIntegrity
	}
	if err != nil {
		return SourceDriverStageState{}, false, err
	}
	if targetEpoch == 0 || len(declaration) != sha256.Size || len(targets) != sha256.Size || len(operation) != len(causal.OperationID{}) ||
		len(change) != len(causal.ChangeID{}) || len(pageDigest) != sha256.Size || len(identityDigest) != sha256.Size {
		return SourceDriverStageState{}, false, ErrIntegrity
	}
	identity := SourceDriverStageIdentity{
		Authority: authority, FleetOwner: SourceAuthorityFleetOwnerID(owner),
		AuthorityGeneration: causal.Generation(authorityGeneration), TargetCount: targetCount,
		Cause: causal.Cause(cause), Origin: causal.DomainID(origin), OriginGeneration: causal.Generation(originGeneration),
		Mode: SourceDriverMode(mode), SnapshotReason: SourceDriverSnapshotReason(snapshotReason),
		FromToken: fromToken, ToToken: toToken, Predecessor: causal.Revision(predecessor),
		MutationTenant: TenantID(mutationTenant), MutationGeneration: Generation(mutationGeneration),
		MutationResult: SourceObjectKey(mutationResult),
	}
	copy(identity.DeclarationDigest[:], declaration)
	copy(identity.TargetsDigest[:], targets)
	identity.Operation = ref.Operation
	copy(identity.SourceOperation[:], operation)
	copy(identity.ChangeID[:], change)
	if mutation != nil {
		if len(mutation) != len(MutationID{}) || len(claimOwner) != len(MutationOwnerID{}) ||
			len(mutationRequestDigest) != sha256.Size || len(mutationReceiptDigest) != sha256.Size ||
			!claimEpoch.Valid || claimEpoch.Int64 <= 0 {
			return SourceDriverStageState{}, false, ErrIntegrity
		}
		copy(identity.Mutation[:], mutation)
		copy(identity.MutationRequestDigest[:], mutationRequestDigest)
		copy(identity.MutationReceiptDigest[:], mutationReceiptDigest)
		copy(identity.Claim.Owner[:], claimOwner)
		identity.Claim.Epoch = uint64(claimEpoch.Int64)
	}
	computed, err := validateSourceDriverStageIdentity(identity)
	if err != nil || !bytes.Equal(computed[:], identityDigest) {
		return SourceDriverStageState{}, false, ErrIntegrity
	}
	state.Identity = identity
	state.Stage = ref
	state.TargetEpoch = targetEpoch
	state.Cursor = append([]byte(nil), cursor...)
	copy(state.PageDigest[:], pageDigest)
	return state, true, nil
}
