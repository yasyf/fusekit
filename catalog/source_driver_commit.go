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
	sourceDriverBeforeVisibilityCASPoint   = "source_driver.before_visibility_cas"
	sourceDriverAfterVisibilityCASPoint    = "source_driver.after_visibility_cas"
	sourceDriverAfterVisibilityCommitPoint = "source_driver.after_visibility_commit"
	sourceDriverFinalCommitStatementPoint  = "source_driver.final_commit_statement"
)

// CommitSourceDriverStage atomically publishes a non-mutation authority stage.
func (c *Catalog) CommitSourceDriverStage(
	ctx context.Context,
	state SourceDriverStageState,
) (SourceDriverStageResult, error) {
	if state.Identity.Mode == SourceDriverMutation {
		return SourceDriverStageResult{}, ErrInvalidTransition
	}
	return c.commitSourceDriverStage(ctx, state, false)
}

// CommitSourceDriverMutation atomically publishes a driver mutation and settles its prepared intent.
func (c *Catalog) CommitSourceDriverMutation(
	ctx context.Context,
	state SourceDriverStageState,
) (SourceDriverStageResult, error) {
	if state.Identity.Mode != SourceDriverMutation {
		return SourceDriverStageResult{}, ErrInvalidTransition
	}
	return c.commitSourceDriverStage(ctx, state, true)
}

func (c *Catalog) commitSourceDriverStage(
	ctx context.Context,
	expected SourceDriverStageState,
	mutation bool,
) (SourceDriverStageResult, error) {
	identityDigest, err := validateSourceDriverStageIdentity(expected.Identity)
	if err != nil || expected.Stage.Authority != expected.Identity.Authority ||
		expected.Stage.Operation != expected.Identity.Operation || expected.Stage.Sequence == 0 ||
		expected.Stage.Revision != expected.Identity.Predecessor+1 || expected.Stage.Digest == ([sha256.Size]byte{}) ||
		expected.PageDigest == ([sha256.Size]byte{}) {
		return SourceDriverStageResult{}, fmt.Errorf("%w: incomplete source driver stage proof", ErrInvalidObject)
	}
	if result, found, receiptErr := readSourceDriverStageReceipt(
		ctx, c.readDB, expected, identityDigest,
	); receiptErr != nil {
		return SourceDriverStageResult{}, receiptErr
	} else if found {
		if err := c.drainSourceDriverStage(ctx, expected.Identity.Authority, expected.Identity.Operation); err != nil {
			return SourceDriverStageResult{}, err
		}
		return result, nil
	}
	preparation, err := readSourceDriverPreparationState(ctx, c.readDB, expected.Identity)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SourceDriverStageResult{}, ErrInvalidTransition
		}
		return SourceDriverStageResult{}, err
	}
	if !preparation.Prepared || preparation.TargetEpoch != expected.TargetEpoch ||
		preparation.SourceRevision != uint64(expected.Identity.Predecessor+1) ||
		preparation.TargetCount != expected.Identity.TargetCount {
		return SourceDriverStageResult{}, ErrIntegrity
	}
	checkpoint := SourceDriverCheckpoint{
		Authority: expected.Identity.Authority, FleetOwner: expected.Identity.FleetOwner,
		AuthorityGeneration: expected.Identity.AuthorityGeneration,
		DeclarationDigest:   expected.Identity.DeclarationDigest,
		TargetEpoch:         expected.TargetEpoch,
		TargetCount:         expected.Identity.TargetCount, TargetsDigest: expected.Identity.TargetsDigest,
		Token: expected.Identity.ToToken, TokenDigest: sourceDriverTokenDigest(expected.Identity.ToToken),
		PublicationID:     expected.Identity.Operation,
		PublicationDigest: expected.Stage.Digest,
		SourceRevision:    expected.Identity.Predecessor + 1,
		SourceOperation:   expected.Identity.SourceOperation, ChangeID: expected.Identity.ChangeID,
		Cause: expected.Identity.Cause, Origin: expected.Identity.Origin,
		OriginGeneration: expected.Identity.OriginGeneration,
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceDriverStageResult{}, fmt.Errorf("catalog: begin source driver commit: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if result, found, receiptErr := readSourceDriverStageReceipt(ctx, tx, expected, identityDigest); receiptErr != nil {
		return SourceDriverStageResult{}, receiptErr
	} else if found {
		if err := tx.Commit(); err != nil {
			return SourceDriverStageResult{}, err
		}
		if err := c.drainSourceDriverStage(ctx, expected.Identity.Authority, expected.Identity.Operation); err != nil {
			return SourceDriverStageResult{}, err
		}
		return result, nil
	}
	current, found, err := readSourceDriverStageState(ctx, tx, expected.Identity.Authority)
	if err != nil {
		return SourceDriverStageResult{}, err
	}
	if !found {
		return SourceDriverStageResult{}, ErrNotFound
	}
	if !equalSourceDriverStageState(current, expected) {
		return SourceDriverStageResult{}, ErrMutationConflict
	}
	var complete, aborting int
	if err := tx.QueryRowContext(ctx, `
SELECT complete, aborting FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(expected.Identity.Authority), expected.Identity.Operation[:]).Scan(&complete, &aborting); err != nil {
		return SourceDriverStageResult{}, err
	}
	if complete == 0 || aborting != 0 {
		return SourceDriverStageResult{}, ErrInvalidTransition
	}
	if err := requireImmutableSourceDriverFleetMember(
		ctx, tx, expected.Identity.Authority, expected.Identity.FleetOwner,
		expected.Identity.AuthorityGeneration, expected.Identity.DeclarationDigest,
	); err != nil {
		return SourceDriverStageResult{}, err
	}
	checkpointBefore, checkpointFound, err := readSourceDriverCheckpoint(ctx, tx, expected.Identity.Authority)
	if err != nil {
		return SourceDriverStageResult{}, err
	}
	if err := validateSourceDriverTransition(expected.Identity, expected.TargetEpoch, checkpointBefore, checkpointFound); err != nil {
		return SourceDriverStageResult{}, err
	}
	if err := validatePreparedSourceDriverPublication(ctx, tx, expected, identityDigest, preparation); err != nil {
		return SourceDriverStageResult{}, err
	}
	var mutationResult *SourceDriverMutationResult
	if mutation {
		result, err := c.settleSourceDriverMutation(ctx, tx, expected.Identity)
		if err != nil {
			return SourceDriverStageResult{}, err
		}
		mutationResult = &result
	}
	stageResult := SourcePublicationStageResult{
		Authority: expected.Identity.Authority, FleetOwner: expected.Stage.FleetOwner,
		FleetGeneration: expected.Stage.FleetGeneration, DriverID: expected.Stage.DriverID,
		DeclarationDigest: expected.Stage.DeclarationDigest,
		Operation:         expected.Identity.Operation, First: expected.Identity.Predecessor + 1,
		Last: expected.Identity.Predecessor + 1, Count: 1, Digest: expected.Stage.Digest,
	}
	result := SourceDriverStageResult{
		Identity: expected.Identity, Proof: expected.Stage, Stage: stageResult,
		Checkpoint: checkpoint, MutationResult: mutationResult,
	}
	receiptDigest, _, err := sourceDriverStageResultDigest(result)
	if err != nil {
		return SourceDriverStageResult{}, err
	}
	result.ReceiptDigest = receiptDigest
	encoded, err := json.Marshal(result)
	if err != nil {
		return SourceDriverStageResult{}, fmt.Errorf("catalog: encode source driver receipt: %w", err)
	}
	if err := c.trip(sourceDriverBeforeVisibilityCASPoint); err != nil {
		return SourceDriverStageResult{}, err
	}
	if err := c.trip(sourceDriverFinalCommitStatementPoint); err != nil {
		return SourceDriverStageResult{}, err
	}
	updated, err := tx.ExecContext(ctx, `
UPDATE source_driver_publication_heads AS head
SET publication_id = ?, source_revision = ?, epoch = epoch + 1
WHERE head.source_authority = ?
  AND head.epoch = ?
  AND EXISTS (
      SELECT 1 FROM source_driver_publications publication
      WHERE publication.source_authority = head.source_authority
        AND publication.publication_id = ? AND publication.prepared = 1
        AND publication.expected_visibility_epoch = head.epoch
        AND publication.target_epoch = (
            SELECT target_epoch FROM source_driver_target_epochs
            WHERE source_authority = publication.source_authority
        )
        AND publication.predecessor_publication_id = head.publication_id
        AND publication.predecessor_revision = head.source_revision
        AND publication.source_revision = ?
  )`, expected.Identity.Operation[:], uint64(checkpoint.SourceRevision), string(expected.Identity.Authority),
		preparation.ExpectedVisibilityEpoch, expected.Identity.Operation[:], uint64(checkpoint.SourceRevision))
	if err != nil {
		return SourceDriverStageResult{}, mapConstraint(err)
	}
	if changed, _ := updated.RowsAffected(); changed != 1 {
		return SourceDriverStageResult{}, ErrMutationConflict
	}
	if err := c.trip(sourceDriverAfterVisibilityCASPoint); err != nil {
		return SourceDriverStageResult{}, err
	}
	if err := c.trip(sourceDriverFinalCommitStatementPoint); err != nil {
		return SourceDriverStageResult{}, err
	}
	if err := persistSourceDriverCheckpoint(ctx, tx, checkpoint, checkpointFound); err != nil {
		return SourceDriverStageResult{}, err
	}
	if expected.Identity.ObserverStream != "" {
		if err := c.settleStagedSourceObserver(ctx, tx, expected.Stage); err != nil {
			return SourceDriverStageResult{}, err
		}
	}
	if err := c.trip(sourceDriverFinalCommitStatementPoint); err != nil {
		return SourceDriverStageResult{}, err
	}
	if err := insertSourceDriverStageReceipt(ctx, tx, result, identityDigest, encoded); err != nil {
		return SourceDriverStageResult{}, err
	}
	if result.MutationResult != nil && result.MutationResult.Private != nil {
		if err := insertPrivateMutationReceipt(ctx, tx, result, encoded); err != nil {
			return SourceDriverStageResult{}, err
		}
	}
	if mutation {
		updated, err := tx.ExecContext(ctx, `
UPDATE source_driver_mutation_reservations SET committed = 1
WHERE mutation_id = ? AND stage_operation_id = ? AND source_operation_id = ?
  AND change_id = ? AND request_bound = 1 AND receipt_digest = ? AND committed = 0`,
			expected.Identity.Mutation[:], expected.Identity.Operation[:], expected.Identity.SourceOperation[:],
			expected.Identity.ChangeID[:], expected.Identity.MutationReceiptDigest[:])
		if err != nil {
			return SourceDriverStageResult{}, err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return SourceDriverStageResult{}, ErrMutationConflict
		}
	}
	if err := tx.Commit(); err != nil {
		return SourceDriverStageResult{}, fmt.Errorf("catalog: commit source driver stage: %w", err)
	}
	if err := c.trip(sourceDriverAfterVisibilityCommitPoint); err != nil {
		return SourceDriverStageResult{}, err
	}
	if err := c.drainSourceDriverStage(ctx, expected.Identity.Authority, expected.Identity.Operation); err != nil {
		return SourceDriverStageResult{}, err
	}
	return result, nil
}

func insertPrivateMutationReceipt(
	ctx context.Context,
	tx *sql.Tx,
	result SourceDriverStageResult,
	encoded []byte,
) error {
	if result.MutationResult == nil || result.MutationResult.Kind != SourceDriverMutationPrivate ||
		result.MutationResult.Private == nil || result.ReceiptDigest == ([sha256.Size]byte{}) {
		return ErrInvalidTransition
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO private_mutation_receipts(
    mutation_id, source_authority, stage_operation_id, result_json, result_digest, state
) VALUES (?, ?, ?, ?, ?, 1)`,
		result.Identity.Mutation[:], string(result.Identity.Authority), result.Identity.Operation[:],
		encoded, result.ReceiptDigest[:]); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func validatePreparedSourceDriverPublication(
	ctx context.Context,
	tx *sql.Tx,
	expected SourceDriverStageState,
	identityDigest [sha256.Size]byte,
	preparation SourceDriverPreparationState,
) error {
	var predecessor, storedIdentity, targetsDigest, stageDigest, publicationDigest []byte
	var predecessorRevision, sourceRevision, epoch, targetEpoch, currentTargetEpoch, targetCount uint64
	var stageSequence, stageItems, stageBytes, initializedTargets, preparedTargets uint64
	var prepared int
	if err := tx.QueryRowContext(ctx, `
SELECT predecessor_publication_id, predecessor_revision, source_revision,
       expected_visibility_epoch, publication.target_epoch,
       (SELECT target_epoch FROM source_driver_target_epochs
        WHERE source_authority = publication.source_authority),
       identity_digest, target_count, targets_digest,
       stage_sequence, stage_item_count, stage_byte_count, stage_digest,
       initialized_target_count, prepared_target_count, rolling_digest, prepared
FROM source_driver_publications publication
WHERE publication.source_authority = ? AND publication.publication_id = ?`, string(expected.Identity.Authority),
		expected.Identity.Operation[:]).Scan(
		&predecessor, &predecessorRevision, &sourceRevision, &epoch, &targetEpoch, &currentTargetEpoch,
		&storedIdentity,
		&targetCount, &targetsDigest, &stageSequence, &stageItems, &stageBytes, &stageDigest,
		&initializedTargets, &preparedTargets, &publicationDigest, &prepared,
	); err != nil {
		return err
	}
	if len(predecessor) != 0 && len(predecessor) != len(expected.Identity.Operation) {
		return ErrIntegrity
	}
	if predecessorRevision != uint64(expected.Identity.Predecessor) ||
		sourceRevision != uint64(expected.Identity.Predecessor+1) ||
		epoch != preparation.ExpectedVisibilityEpoch || targetEpoch != preparation.TargetEpoch ||
		currentTargetEpoch != targetEpoch ||
		!bytes.Equal(storedIdentity, identityDigest[:]) || targetCount != expected.Identity.TargetCount ||
		!bytes.Equal(targetsDigest, expected.Identity.TargetsDigest[:]) ||
		stageSequence != expected.Stage.Sequence || stageItems != expected.Stage.Items ||
		stageBytes != expected.Stage.Bytes || !bytes.Equal(stageDigest, expected.Stage.Digest[:]) ||
		initializedTargets != targetCount || preparedTargets != targetCount || prepared != 1 ||
		len(publicationDigest) != sha256.Size || !bytes.Equal(publicationDigest, preparation.Digest[:]) ||
		preparation.Published {
		return ErrMutationConflict
	}
	return nil
}

func releaseSourceDriverMutationReservation(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
) error {
	if identity.MutationResult == "" {
		return nil
	}
	var reserved []byte
	err := tx.QueryRowContext(ctx, `
SELECT mutation_id FROM source_key_reservations
WHERE source_authority = ? AND source_key = ?`,
		string(identity.Authority), string(identity.MutationResult)).Scan(&reserved)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(reserved, identity.Mutation[:]) {
		return ErrMutationConflict
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM source_key_reservations
WHERE source_authority = ? AND source_key = ? AND mutation_id = ?`,
		string(identity.Authority), string(identity.MutationResult), identity.Mutation[:])
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	return nil
}

func persistSourceDriverCheckpoint(
	ctx context.Context,
	tx *sql.Tx,
	checkpoint SourceDriverCheckpoint,
	exists bool,
) error {
	if exists {
		result, err := tx.ExecContext(ctx, `
UPDATE source_driver_checkpoints SET
    fleet_owner_id = ?, authority_generation = ?, declaration_digest = ?,
    target_epoch = ?, target_count = ?, targets_digest = ?, source_operation_id = ?, change_id = ?,
    cause = ?, origin_domain = ?, origin_generation = ?, applied_token = ?,
    token_digest = ?, source_revision = ?, snapshot_required = 0
WHERE source_authority = ?`, string(checkpoint.FleetOwner), uint64(checkpoint.AuthorityGeneration),
			checkpoint.DeclarationDigest[:], checkpoint.TargetEpoch, checkpoint.TargetCount, checkpoint.TargetsDigest[:],
			checkpoint.SourceOperation[:], checkpoint.ChangeID[:], string(checkpoint.Cause), string(checkpoint.Origin),
			uint64(checkpoint.OriginGeneration), checkpoint.Token, checkpoint.TokenDigest[:],
			uint64(checkpoint.SourceRevision), string(checkpoint.Authority))
		if err != nil {
			return mapConstraint(err)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return ErrMutationConflict
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_checkpoints(
    source_authority, fleet_owner_id, authority_generation, declaration_digest,
    target_epoch, target_count, targets_digest, source_operation_id, change_id, cause,
    origin_domain, origin_generation, applied_token, token_digest, source_revision, snapshot_required
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
			string(checkpoint.Authority), string(checkpoint.FleetOwner), uint64(checkpoint.AuthorityGeneration),
			checkpoint.DeclarationDigest[:], checkpoint.TargetEpoch, checkpoint.TargetCount, checkpoint.TargetsDigest[:],
			checkpoint.SourceOperation[:], checkpoint.ChangeID[:], string(checkpoint.Cause), string(checkpoint.Origin),
			uint64(checkpoint.OriginGeneration), checkpoint.Token, checkpoint.TokenDigest[:],
			uint64(checkpoint.SourceRevision)); err != nil {
			return mapConstraint(err)
		}
	}
	return nil
}

func (c *Catalog) settleSourceDriverMutation(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
) (SourceDriverMutationResult, error) {
	record, found, err := readPreparedMutation(ctx, tx, identity.Mutation)
	if err != nil {
		return SourceDriverMutationResult{}, err
	}
	if !found || record.Tenant != identity.MutationTenant || record.State != MutationApplying ||
		record.Claim == nil || *record.Claim != identity.Claim {
		return SourceDriverMutationResult{}, ErrMutationClaimed
	}
	var rawRevision uint64
	if err := tx.QueryRowContext(ctx, `
SELECT catalog_head FROM source_driver_publication_targets
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND generation = ? AND prepared = 1`, string(identity.Authority), identity.Operation[:],
		string(identity.MutationTenant), uint64(identity.MutationGeneration)).Scan(&rawRevision); err != nil {
		return SourceDriverMutationResult{}, err
	}
	if isPrivatePreparedCreate(record.PreparedMutation) {
		if Revision(rawRevision) != record.ExpectedHead {
			return SourceDriverMutationResult{}, ErrMutationConflict
		}
		private, err := c.settlePrivateSourceDriverCreate(ctx, tx, identity, record)
		if err != nil {
			return SourceDriverMutationResult{}, err
		}
		if err := commitPreparedSourceDriverMutation(ctx, tx, identity); err != nil {
			return SourceDriverMutationResult{}, err
		}
		return SourceDriverMutationResult{Kind: SourceDriverMutationPrivate, Private: &private}, nil
	}
	revision := Revision(rawRevision)
	if revision != record.ExpectedHead+1 {
		return SourceDriverMutationResult{}, ErrMutationConflict
	}
	primary, secondary, err := sourceDriverMutationObjects(ctx, tx, identity, record.PreparedMutation)
	if err != nil {
		return SourceDriverMutationResult{}, err
	}
	if err := insertMutation(ctx, tx, record.OperationID, record.Tenant, record.Kind, record.digest, revision, primary, secondary); err != nil {
		return SourceDriverMutationResult{}, err
	}
	result, err := sourceDriverNamespaceMutationResult(
		ctx, tx, identity, record.PreparedMutation, revision, primary, secondary,
	)
	if err != nil {
		return SourceDriverMutationResult{}, err
	}
	if record.Kind == MutationReplace {
		if err := consumePrivateSourceDriverPromotion(ctx, tx, identity, record, result.Primary, result.Mutation.Revision); err != nil {
			return SourceDriverMutationResult{}, err
		}
	}
	if err := commitPreparedSourceDriverMutation(ctx, tx, identity); err != nil {
		return SourceDriverMutationResult{}, err
	}
	return SourceDriverMutationResult{Kind: SourceDriverMutationNamespace, Namespace: &result}, nil
}

func commitPreparedSourceDriverMutation(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
) error {
	updated, err := tx.ExecContext(ctx, `
UPDATE prepared_mutations SET state = ?
WHERE mutation_id = ? AND state = ? AND claim_owner = ? AND claim_epoch = ?`,
		uint8(MutationCommitted), identity.Mutation[:], uint8(MutationApplying),
		identity.Claim.Owner[:], identity.Claim.Epoch)
	if err != nil {
		return err
	}
	if changed, _ := updated.RowsAffected(); changed != 1 {
		return ErrMutationClaimed
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM source_key_reservations WHERE mutation_id = ?`,
		identity.Mutation[:]); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM content_stages WHERE mutation_id = ?`,
		identity.Mutation[:]); err != nil {
		return err
	}
	return nil
}

func isPrivatePreparedCreate(prepared PreparedMutation) bool {
	return prepared.Kind == MutationCreate && prepared.Intent.Create != nil &&
		prepared.Intent.Create.Spec.Visibility == (Visibility{})
}

func (c *Catalog) settlePrivateSourceDriverCreate(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	record preparedRecord,
) (PrivateMutationResult, error) {
	if identity.MutationResult == "" || record.Source == nil || record.Source.Parent == nil ||
		record.SourceResult == nil || record.SourceResult.SourceKey != identity.MutationResult {
		return PrivateMutationResult{}, ErrMutationConflict
	}
	var parentKey, name, linkTarget string
	var kind uint8
	var mode uint32
	var contentRevision uint64
	var rawHash []byte
	var size int64
	var mount, provider int
	err := tx.QueryRowContext(ctx, `
SELECT parent_key, object_name, object_kind, object_mode, content_revision,
       content_hash, content_size, link_target, mount_visible, file_provider_visible
FROM source_driver_stage_entries entry
WHERE entry.source_authority = ? AND entry.stage_operation_id = ?
  AND entry.tenant = ? AND entry.generation = ? AND entry.source_key = ? AND entry.action = 2
  AND NOT EXISTS (
      SELECT 1 FROM source_driver_stage_entries newer
      WHERE newer.source_authority = entry.source_authority
        AND newer.stage_operation_id = entry.stage_operation_id
        AND newer.tenant = entry.tenant AND newer.source_key = entry.source_key
        AND newer.change_sequence > entry.change_sequence
  )`, string(identity.Authority), identity.Operation[:], string(identity.MutationTenant),
		uint64(identity.MutationGeneration), string(identity.MutationResult)).Scan(
		&parentKey, &name, &kind, &mode, &contentRevision, &rawHash, &size,
		&linkTarget, &mount, &provider,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return PrivateMutationResult{}, ErrMutationConflict
	}
	if err != nil {
		return PrivateMutationResult{}, err
	}
	if mount != 0 || provider != 0 || len(rawHash) != len(ContentHash{}) ||
		SourceObjectKey(parentKey) != record.Source.Parent.SourceKey {
		return PrivateMutationResult{}, ErrMutationConflict
	}
	parent, err := sourceObjectIdentity(ctx, tx, identity.Authority, SourceObjectKey(parentKey))
	if err != nil {
		return PrivateMutationResult{}, err
	}
	id, err := sourceDriverPreparedObjectIdentity(ctx, tx, identity, identity.MutationResult)
	if err != nil {
		return PrivateMutationResult{}, err
	}
	policy, err := tenantCasePolicy(ctx, tx, identity.MutationTenant)
	if err != nil {
		return PrivateMutationResult{}, err
	}
	var hash ContentHash
	copy(hash[:], rawHash)
	result := PrivateMutationResult{
		Mutation: identity.Mutation, Tenant: identity.MutationTenant,
		Generation: identity.MutationGeneration, ObjectID: id, Parent: parent,
		Name: name, Kind: Kind(kind), Mode: mode, ContentRevision: Revision(contentRevision),
		Size: size, Hash: hash, LinkTarget: linkTarget,
		SourceAuthority: identity.Authority, SourceKey: identity.MutationResult,
		SourceOperation: identity.SourceOperation, SourceRevision: identity.Predecessor + 1,
		CreatedAgainstHead: record.ExpectedHead,
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO private_mutation_objects(
    tenant, object_id, mutation_id, generation, source_authority, source_key,
    source_operation_id, source_revision, created_against_head, source_id,
    cause, origin_domain, origin_generation, parent_id, name, name_key, kind, mode,
    content_revision, size, hash, link_target
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(result.Tenant), result.ObjectID[:], result.Mutation[:], uint64(result.Generation),
		string(result.SourceAuthority), string(result.SourceKey), result.SourceOperation[:],
		uint64(result.SourceRevision), uint64(result.CreatedAgainstHead), record.Intent.SourceID,
		string(record.Intent.Origin.Cause), string(record.Intent.Origin.Domain),
		uint64(record.Intent.Origin.Generation), result.Parent[:], result.Name,
		normalizeName(policy, result.Name), uint8(result.Kind), result.Mode,
		uint64(result.ContentRevision), result.Size, result.Hash[:], result.LinkTarget,
	); err != nil {
		return PrivateMutationResult{}, mapConstraint(err)
	}
	return result, nil
}

func consumePrivateSourceDriverPromotion(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	record preparedRecord,
	published Object,
	revision Revision,
) error {
	if record.Source == nil || record.Source.Private == nil {
		return nil
	}
	private, found, err := readPrivatePromotionSource(
		ctx, tx, record.Tenant, record.Intent.Replace.Source, record.Intent.SourceID,
	)
	if err != nil {
		return err
	}
	if !found {
		var state uint8
		var terminal []byte
		var terminalRevision sql.NullInt64
		if err := tx.QueryRowContext(ctx, `
SELECT state, terminal_mutation_id, terminal_catalog_revision
FROM private_mutation_receipts WHERE mutation_id = ?`,
			record.Source.Private.Creator[:]).Scan(&state, &terminal, &terminalRevision); err != nil {
			return err
		}
		if state != 2 || !bytes.Equal(terminal, identity.Mutation[:]) || !terminalRevision.Valid ||
			Revision(terminalRevision.Int64) != revision {
			return ErrMutationConflict
		}
		return nil
	}
	if private.SourceAuthority != identity.Authority ||
		private.Generation != identity.MutationGeneration ||
		private.Mutation != record.Source.Private.Creator || record.Source.Object == nil ||
		record.Source.Object.SourceKey != private.SourceKey ||
		private.SourceID != record.Intent.SourceID || private.Origin != record.Intent.Origin ||
		published.ID != private.ObjectID || published.Tombstone ||
		published.Visibility == (Visibility{}) {
		return ErrMutationConflict
	}
	deleted, err := tx.ExecContext(ctx, `
DELETE FROM private_mutation_objects
WHERE tenant = ? AND object_id = ? AND mutation_id = ? AND generation = ?
  AND source_authority = ? AND source_key = ?`,
		string(private.Tenant), private.ObjectID[:], private.Mutation[:], uint64(private.Generation),
		string(private.SourceAuthority), string(private.SourceKey))
	if err != nil {
		return err
	}
	if changed, _ := deleted.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	retired, err := tx.ExecContext(ctx, `
UPDATE private_mutation_receipts
SET state = 2, terminal_mutation_id = ?, terminal_catalog_revision = ?
WHERE mutation_id = ? AND state = 1
  AND terminal_mutation_id IS NULL AND terminal_catalog_revision IS NULL`,
		identity.Mutation[:], uint64(revision), private.Mutation[:])
	if err != nil {
		return err
	}
	if changed, _ := retired.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	return nil
}

func sourceDriverMutationObjects(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	prepared PreparedMutation,
) (ObjectID, ObjectID, error) {
	var primary, secondary ObjectID
	lookup := func(key SourceObjectKey) (ObjectID, error) {
		var raw []byte
		if err := tx.QueryRowContext(ctx, `
SELECT object_id FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND source_key = ?`,
			string(identity.Authority), identity.Operation[:], string(prepared.Tenant), string(key)).Scan(&raw); err != nil {
			return ObjectID{}, err
		}
		return objectID(raw)
	}
	switch prepared.Kind {
	case MutationCreate:
		if identity.MutationResult == "" {
			return primary, secondary, ErrSourceLocatorMissing
		}
		var err error
		primary, err = lookup(identity.MutationResult)
		return primary, secondary, err
	case MutationRevise:
		return prepared.Intent.Revise.Object, secondary, nil
	case MutationDelete:
		return prepared.Intent.Delete.Object, secondary, nil
	case MutationReplace:
		return prepared.Intent.Replace.Source, prepared.Intent.Replace.Target, nil
	default:
		return primary, secondary, ErrInvalidTransition
	}
}

func sourceDriverNamespaceMutationResult(
	ctx context.Context,
	tx *sql.Tx,
	identity SourceDriverStageIdentity,
	prepared PreparedMutation,
	revision Revision,
	primary, secondary ObjectID,
) (NamespaceMutationResult, error) {
	read := func(id ObjectID) (Object, error) {
		query := "SELECT " + objectColumns + ` FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND object_id = ?`
		object, err := scanObject(tx.QueryRowContext(
			ctx, query, string(identity.Authority), identity.Operation[:], string(prepared.Tenant), id[:],
		))
		if errors.Is(err, sql.ErrNoRows) {
			return Object{}, ErrNotFound
		}
		if err == nil && object.Revision != revision {
			return Object{}, ErrMutationConflict
		}
		return object, err
	}
	primaryObject, err := read(primary)
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	result := NamespaceMutationResult{Mutation: MutationRecord{
		ID: prepared.OperationID, Tenant: prepared.Tenant, Kind: prepared.Kind,
		Revision: revision, Primary: primary,
	}, Primary: primaryObject}
	if !zeroObjectID(secondary) {
		secondaryObject, err := read(secondary)
		if err != nil {
			return NamespaceMutationResult{}, err
		}
		result.Mutation.Secondary = secondary
		result.Secondary = &secondaryObject
	}
	return result, nil
}

func insertSourceDriverStageReceipt(
	ctx context.Context,
	tx *sql.Tx,
	result SourceDriverStageResult,
	identityDigest [sha256.Size]byte,
	encoded []byte,
) error {
	identity := result.Identity
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_stage_receipts(
    source_authority, stage_operation_id, mode, from_token, to_token, source_revision,
    target_count, targets_digest, stage_sequence, stage_item_count, stage_byte_count,
    stage_digest, identity_digest, result_json, result_digest, mutation_id,
    mutation_request_digest, mutation_receipt_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(identity.Authority),
		identity.Operation[:], uint8(identity.Mode), identity.FromToken, identity.ToToken,
		uint64(result.Checkpoint.SourceRevision), identity.TargetCount, identity.TargetsDigest[:],
		result.Proof.Sequence, result.Proof.Items, result.Proof.Bytes, result.Proof.Digest[:],
		identityDigest[:], encoded, result.ReceiptDigest[:], nullableSourceDriverMutation(identity),
		nullableSourceDriverDigest(identity.Mode, identity.MutationRequestDigest),
		nullableSourceDriverDigest(identity.Mode, identity.MutationReceiptDigest)); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func nullableSourceDriverMutation(identity SourceDriverStageIdentity) any {
	if identity.Mode != SourceDriverMutation {
		return nil
	}
	return identity.Mutation[:]
}

func nullableSourceDriverDigest(mode SourceDriverMode, digest [sha256.Size]byte) any {
	if mode != SourceDriverMutation {
		return nil
	}
	return digest[:]
}

func readSourceDriverStageReceipt(
	ctx context.Context,
	query sourceDriverCheckpointQueryer,
	expected SourceDriverStageState,
	identityDigest [sha256.Size]byte,
) (SourceDriverStageResult, bool, error) {
	var storedIdentity, payload, storedResult, stageDigest []byte
	var sequence, items, byteCount uint64
	err := query.QueryRowContext(ctx, `
SELECT identity_digest, result_json, result_digest,
       stage_sequence, stage_item_count, stage_byte_count, stage_digest
FROM source_driver_stage_receipts
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(expected.Identity.Authority), expected.Identity.Operation[:]).Scan(
		&storedIdentity, &payload, &storedResult, &sequence, &items, &byteCount, &stageDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceDriverStageResult{}, false, nil
	}
	if err != nil {
		return SourceDriverStageResult{}, false, err
	}
	if !bytes.Equal(storedIdentity, identityDigest[:]) || sequence != expected.Stage.Sequence ||
		items != expected.Stage.Items || byteCount != expected.Stage.Bytes ||
		!bytes.Equal(stageDigest, expected.Stage.Digest[:]) || len(storedResult) != sha256.Size {
		return SourceDriverStageResult{}, false, ErrMutationConflict
	}
	var result SourceDriverStageResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return SourceDriverStageResult{}, false, ErrIntegrity
	}
	digest, _, err := sourceDriverStageResultDigest(result)
	if err != nil || !bytes.Equal(storedResult, digest[:]) || result.ReceiptDigest != digest ||
		result.Checkpoint.TargetEpoch != expected.TargetEpoch ||
		!equalSourceDriverStageState(
			SourceDriverStageState{Identity: result.Identity, Stage: result.Proof, TargetEpoch: result.Checkpoint.TargetEpoch},
			SourceDriverStageState{Identity: expected.Identity, Stage: expected.Stage, TargetEpoch: expected.TargetEpoch},
		) {
		return SourceDriverStageResult{}, false, ErrIntegrity
	}
	return result, true, nil
}

// PendingSourceDriverCommittedReceipt returns the oldest committed result not yet acknowledged by its runtime.
func (c *Catalog) PendingSourceDriverCommittedReceipt(
	ctx context.Context,
	authority causal.SourceAuthorityID,
) (*SourceDriverCommittedReceipt, error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil {
		return nil, fmt.Errorf("%w: invalid source driver receipt authority", ErrInvalidObject)
	}
	var operation, payload, storedDigest []byte
	var acknowledged, forgotten int
	err := c.readDB.QueryRowContext(ctx, `
SELECT stage_operation_id, result_json, result_digest, acknowledged, forgotten
FROM source_driver_stage_receipts
WHERE source_authority = ? AND forgotten = 0
ORDER BY source_revision, rowid LIMIT 1`, string(authority)).Scan(
		&operation, &payload, &storedDigest, &acknowledged, &forgotten,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("catalog: read pending source driver receipt: %w", err)
	}
	result, err := decodeSourceDriverCommittedReceipt(authority, operation, payload, storedDigest)
	if err != nil {
		return nil, err
	}
	return &SourceDriverCommittedReceipt{Result: result, Acknowledged: acknowledged != 0, Forgotten: forgotten != 0}, nil
}

// PendingSourceDriverReceiptAuthorities returns authorities whose committed receipts still require settlement.
func (c *Catalog) PendingSourceDriverReceiptAuthorities(
	ctx context.Context,
	after causal.SourceAuthorityID,
	limit int,
) (SourceDriverReceiptAuthorityPage, error) {
	if (after != "" && causal.ValidateSourceAuthorityID(after) != nil) || limit < 1 ||
		limit > SourceDriverReceiptAuthorityPageLimit {
		return SourceDriverReceiptAuthorityPage{}, fmt.Errorf("%w: invalid source driver receipt authority page", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT source_authority
FROM source_driver_stage_receipts
WHERE forgotten = 0 AND source_authority > ?
GROUP BY source_authority
ORDER BY source_authority LIMIT ?`, string(after), limit+1)
	if err != nil {
		return SourceDriverReceiptAuthorityPage{}, fmt.Errorf("catalog: page source driver receipt authorities: %w", err)
	}
	defer func() { _ = rows.Close() }()
	values := make([]causal.SourceAuthorityID, 0, limit+1)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return SourceDriverReceiptAuthorityPage{}, err
		}
		authority := causal.SourceAuthorityID(raw)
		if err := causal.ValidateSourceAuthorityID(authority); err != nil {
			return SourceDriverReceiptAuthorityPage{}, ErrIntegrity
		}
		values = append(values, authority)
	}
	if err := rows.Err(); err != nil {
		return SourceDriverReceiptAuthorityPage{}, err
	}
	page := SourceDriverReceiptAuthorityPage{Authorities: values}
	if len(page.Authorities) > limit {
		page.Authorities = page.Authorities[:limit]
		page.Next = page.Authorities[len(page.Authorities)-1]
	}
	return page, nil
}

// CommittedSourceDriverMutation returns the exact committed result for one mutation operation.
func (c *Catalog) CommittedSourceDriverMutation(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	mutation MutationID,
) (*SourceDriverCommittedReceipt, error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil || mutation == (MutationID{}) {
		return nil, fmt.Errorf("%w: invalid committed source driver mutation", ErrInvalidObject)
	}
	var operation, payload, storedDigest []byte
	var acknowledged, forgotten int
	err := c.readDB.QueryRowContext(ctx, `
SELECT stage_operation_id, result_json, result_digest, acknowledged, forgotten
FROM source_driver_stage_receipts
WHERE source_authority = ? AND mutation_id = ?`, string(authority), mutation[:]).Scan(
		&operation, &payload, &storedDigest, &acknowledged, &forgotten,
	)
	if errors.Is(err, sql.ErrNoRows) {
		err = c.readDB.QueryRowContext(ctx, `
SELECT stage_operation_id, result_json, result_digest
FROM private_mutation_receipts
WHERE source_authority = ? AND mutation_id = ?`, string(authority), mutation[:]).Scan(
			&operation, &payload, &storedDigest,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("catalog: read durable private mutation receipt: %w", err)
		}
		acknowledged, forgotten = 1, 1
	}
	if err != nil {
		return nil, fmt.Errorf("catalog: read committed source driver mutation: %w", err)
	}
	result, err := decodeSourceDriverCommittedReceipt(authority, operation, payload, storedDigest)
	if err != nil {
		return nil, err
	}
	if result.Identity.Mode != SourceDriverMutation || result.Identity.Mutation != mutation {
		return nil, ErrIntegrity
	}
	return &SourceDriverCommittedReceipt{Result: result, Acknowledged: acknowledged != 0, Forgotten: forgotten != 0}, nil
}

// AcknowledgeSourceDriverCommittedReceipt records exact runtime delivery of one durable result.
func (c *Catalog) AcknowledgeSourceDriverCommittedReceipt(
	ctx context.Context,
	result SourceDriverStageResult,
) error {
	digest, _, err := sourceDriverStageResultDigest(result)
	if err != nil || digest != result.ReceiptDigest {
		return fmt.Errorf("%w: invalid source driver receipt acknowledgement", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source driver receipt acknowledgement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	updated, err := tx.ExecContext(ctx, `
UPDATE source_driver_stage_receipts SET acknowledged = 1
WHERE source_authority = ? AND stage_operation_id = ? AND result_digest = ?`,
		string(result.Identity.Authority), result.Identity.Operation[:], result.ReceiptDigest[:])
	if err != nil {
		return err
	}
	changed, _ := updated.RowsAffected()
	if changed == 0 {
		var stored []byte
		var acknowledged int
		err := tx.QueryRowContext(ctx, `
SELECT result_digest, acknowledged FROM source_driver_stage_receipts
WHERE source_authority = ? AND stage_operation_id = ?`,
			string(result.Identity.Authority), result.Identity.Operation[:]).Scan(&stored, &acknowledged)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !bytes.Equal(stored, result.ReceiptDigest[:]) || acknowledged == 0 {
			return ErrMutationConflict
		}
	}
	return tx.Commit()
}

// ForgetSourceDriverCommittedReceipt records exact source-side receipt retirement after acknowledgement.
func (c *Catalog) ForgetSourceDriverCommittedReceipt(
	ctx context.Context,
	result SourceDriverStageResult,
) error {
	digest, _, err := sourceDriverStageResultDigest(result)
	if err != nil || digest != result.ReceiptDigest {
		return fmt.Errorf("%w: invalid source driver receipt forget", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source driver receipt forget: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	updated, err := tx.ExecContext(ctx, `
UPDATE source_driver_stage_receipts SET forgotten = 1
WHERE source_authority = ? AND stage_operation_id = ? AND result_digest = ?
  AND acknowledged = 1 AND forgotten = 0`,
		string(result.Identity.Authority), result.Identity.Operation[:], result.ReceiptDigest[:])
	if err != nil {
		return err
	}
	changed, _ := updated.RowsAffected()
	if changed == 0 {
		var stored []byte
		var acknowledged, forgotten int
		err := tx.QueryRowContext(ctx, `
SELECT result_digest, acknowledged, forgotten FROM source_driver_stage_receipts
WHERE source_authority = ? AND stage_operation_id = ?`,
			string(result.Identity.Authority), result.Identity.Operation[:]).Scan(&stored, &acknowledged, &forgotten)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !bytes.Equal(stored, result.ReceiptDigest[:]) || acknowledged == 0 || forgotten == 0 {
			return ErrMutationConflict
		}
	}
	if result.Identity.Mode == SourceDriverMutation {
		deleted, err := tx.ExecContext(ctx, `
DELETE FROM source_driver_mutation_reservations
WHERE mutation_id = ? AND stage_operation_id = ? AND receipt_digest = ?`,
			result.Identity.Mutation[:], result.Identity.Operation[:], result.Identity.MutationReceiptDigest[:])
		if err != nil {
			return err
		}
		if removed, _ := deleted.RowsAffected(); changed != 0 && removed != 1 {
			return ErrMutationConflict
		}
	}
	return tx.Commit()
}

func decodeSourceDriverCommittedReceipt(
	authority causal.SourceAuthorityID,
	operation, payload, storedDigest []byte,
) (SourceDriverStageResult, error) {
	if len(operation) != len(causal.OperationID{}) || len(storedDigest) != sha256.Size {
		return SourceDriverStageResult{}, ErrIntegrity
	}
	var result SourceDriverStageResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return SourceDriverStageResult{}, ErrIntegrity
	}
	digest, _, err := sourceDriverStageResultDigest(result)
	if err != nil || !bytes.Equal(storedDigest, digest[:]) || result.ReceiptDigest != digest ||
		result.Identity.Authority != authority || !bytes.Equal(operation, result.Identity.Operation[:]) {
		return SourceDriverStageResult{}, ErrIntegrity
	}
	if _, err := validateSourceDriverStageIdentity(result.Identity); err != nil {
		return SourceDriverStageResult{}, ErrIntegrity
	}
	return result, nil
}

func sourceDriverStageResultDigest(result SourceDriverStageResult) ([sha256.Size]byte, []byte, error) {
	if err := validateSourceDriverMutationResult(result); err != nil {
		return [sha256.Size]byte{}, nil, err
	}
	result.ReceiptDigest = [sha256.Size]byte{}
	encoded, err := json.Marshal(result)
	if err != nil {
		return [sha256.Size]byte{}, nil, err
	}
	digest := sha256.Sum256(append([]byte("fusekit.source-driver-receipt.v1\x00"), encoded...))
	return digest, encoded, nil
}

func validateSourceDriverMutationResult(result SourceDriverStageResult) error {
	if result.Identity.Mode != SourceDriverMutation {
		if result.MutationResult != nil {
			return ErrInvalidTransition
		}
		return nil
	}
	if result.MutationResult == nil ||
		(result.MutationResult.Private == nil) == (result.MutationResult.Namespace == nil) {
		return ErrInvalidTransition
	}
	if private := result.MutationResult.Private; private != nil {
		if result.MutationResult.Kind != SourceDriverMutationPrivate {
			return ErrInvalidTransition
		}
		if result.Identity.MutationResult == "" || private.Mutation != result.Identity.Mutation ||
			private.Tenant != result.Identity.MutationTenant ||
			private.Generation != result.Identity.MutationGeneration ||
			private.SourceAuthority != result.Identity.Authority ||
			private.SourceKey != result.Identity.MutationResult ||
			private.SourceOperation != result.Identity.SourceOperation ||
			private.SourceRevision != result.Checkpoint.SourceRevision || private.ObjectID == (ObjectID{}) {
			return ErrMutationConflict
		}
		return nil
	}
	namespace := result.MutationResult.Namespace
	if result.MutationResult.Kind != SourceDriverMutationNamespace {
		return ErrInvalidTransition
	}
	if namespace.Mutation.ID != result.Identity.Mutation || namespace.Mutation.Tenant != result.Identity.MutationTenant {
		return ErrMutationConflict
	}
	return nil
}

func equalSourceDriverStageState(left, right SourceDriverStageState) bool {
	return left.Identity == right.Identity && left.Stage == right.Stage && left.TargetEpoch == right.TargetEpoch &&
		bytes.Equal(left.Cursor, right.Cursor) && left.PageDigest == right.PageDigest
}
