package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

// ReserveSourceDriverMutation durably binds one external mutation before source I/O starts.
func (c *Catalog) ReserveSourceDriverMutation(
	ctx context.Context,
	request SourceDriverMutationReservationRequest,
) (SourceDriverMutationReservation, error) {
	if err := validateSourceDriverMutationReservationRequest(request); err != nil {
		return SourceDriverMutationReservation{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceDriverMutationReservation{}, fmt.Errorf("catalog: begin source driver mutation reservation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if existing, found, err := readSourceDriverMutationReservation(ctx, tx, request.Mutation); err != nil {
		return SourceDriverMutationReservation{}, err
	} else if found {
		if existing.SourceDriverMutationReservationRequest != request {
			return SourceDriverMutationReservation{}, ErrMutationConflict
		}
		if err := tx.Commit(); err != nil {
			return SourceDriverMutationReservation{}, err
		}
		return existing, nil
	}
	record, found, err := readPreparedMutation(ctx, tx, request.Mutation)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if !found {
		return SourceDriverMutationReservation{}, ErrNotFound
	}
	if record.Tenant != request.Target.Tenant || record.State != MutationApplying ||
		record.Claim == nil || *record.Claim != request.Claim || record.Source == nil {
		return SourceDriverMutationReservation{}, ErrMutationClaimed
	}
	for _, locator := range []*SourceLocator{record.Source.Object, record.Source.Parent, record.Source.Target} {
		if locator != nil && locator.SourceAuthority != request.Authority {
			return SourceDriverMutationReservation{}, ErrSourceLocatorStale
		}
	}
	if err := requireSourceDriverFleetMember(
		ctx, tx, request.Authority, request.FleetOwner,
		request.AuthorityGeneration, request.DeclarationDigest,
	); err != nil {
		return SourceDriverMutationReservation{}, err
	}
	provision, provisioned, err := tenantProvision(ctx, tx, request.Target.Tenant)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if !provisioned || provision.Generation != request.Target.Generation ||
		provision.ContentSourceID != string(request.Authority) {
		return SourceDriverMutationReservation{}, ErrGenerationMismatch
	}
	checkpoint, checkpointFound, err := readSourceDriverCheckpoint(ctx, tx, request.Authority)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if !checkpointFound || checkpoint.SnapshotRequired != 0 || checkpoint.FleetOwner != request.FleetOwner ||
		checkpoint.AuthorityGeneration != request.AuthorityGeneration ||
		checkpoint.DeclarationDigest != request.DeclarationDigest || checkpoint.TargetCount != request.TargetCount ||
		checkpoint.TargetsDigest != request.TargetsDigest || checkpoint.Token != request.FromToken ||
		checkpoint.SourceRevision != request.Predecessor {
		return SourceDriverMutationReservation{}, ErrSourcePredecessor
	}
	var targetEpoch uint64
	if err := tx.QueryRowContext(ctx, `
SELECT target_epoch FROM source_driver_target_epochs WHERE source_authority = ?`,
		string(request.Authority)).Scan(&targetEpoch); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SourceDriverMutationReservation{}, ErrGenerationMismatch
		}
		return SourceDriverMutationReservation{}, err
	}
	var tenantReservation []byte
	err = tx.QueryRowContext(ctx, `
SELECT mutation_id FROM source_driver_mutation_reservations
WHERE committed = 0 AND (mutation_tenant = ? OR source_authority = ?)`,
		string(request.Target.Tenant), string(request.Authority)).Scan(&tenantReservation)
	if err == nil {
		return SourceDriverMutationReservation{}, ErrMutationActive
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SourceDriverMutationReservation{}, err
	}
	var collision int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM source_publication_stages WHERE stage_operation_id = ?)
    OR EXISTS(SELECT 1 FROM source_driver_stages WHERE source_operation_id = ? OR change_id = ?)`,
		request.Operation[:], request.SourceOperation[:], request.ChangeID[:]).Scan(&collision); err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if collision != 0 {
		return SourceDriverMutationReservation{}, ErrMutationConflict
	}
	digestState, err := newSourceDriverTargetsDigestState(request.TargetCount)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_mutation_reservations(
    mutation_id, claim_owner, claim_epoch, source_authority, fleet_owner_id,
    authority_generation, declaration_digest, target_count, targets_digest,
    mutation_tenant, mutation_generation, from_token, predecessor_revision,
    stage_operation_id, source_operation_id, change_id,
    target_epoch, target_digest_state
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		request.Mutation[:], request.Claim.Owner[:], request.Claim.Epoch, string(request.Authority),
		string(request.FleetOwner), uint64(request.AuthorityGeneration), request.DeclarationDigest[:],
		request.TargetCount, request.TargetsDigest[:], string(request.Target.Tenant),
		uint64(request.Target.Generation), request.FromToken, uint64(request.Predecessor),
		request.Operation[:], request.SourceOperation[:], request.ChangeID[:],
		targetEpoch, digestState); err != nil {
		return SourceDriverMutationReservation{}, mapConstraint(err)
	}
	reservation, found, err := readSourceDriverMutationReservation(ctx, tx, request.Mutation)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if !found {
		return SourceDriverMutationReservation{}, ErrIntegrity
	}
	if err := tx.Commit(); err != nil {
		return SourceDriverMutationReservation{}, fmt.Errorf("catalog: commit source driver mutation reservation: %w", err)
	}
	return reservation, nil
}

// ReleaseUnboundSourceDriverMutationReservation releases a pre-I/O reservation exactly.
func (c *Catalog) ReleaseUnboundSourceDriverMutationReservation(
	ctx context.Context,
	mutation MutationID,
	claim MutationClaim,
	targetEpoch uint64,
) error {
	if mutation == (MutationID{}) || validateMutationClaim(claim) != nil || targetEpoch == 0 {
		return fmt.Errorf("%w: invalid source driver mutation reservation release", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	reservation, found, err := readSourceDriverMutationReservation(ctx, tx, mutation)
	if err != nil {
		return err
	}
	if !found {
		record, prepared, err := readPreparedMutation(ctx, tx, mutation)
		if err != nil {
			return err
		}
		if !prepared || record.Claim == nil || *record.Claim != claim {
			return ErrNotFound
		}
		return tx.Commit()
	}
	if reservation.Claim != claim {
		return ErrMutationClaimed
	}
	if reservation.TargetEpoch != targetEpoch || reservation.RequestBound || reservation.Receipt != nil ||
		reservation.Committed {
		return ErrMutationConflict
	}
	deleted, err := tx.ExecContext(ctx, `
DELETE FROM source_driver_mutation_reservations
WHERE mutation_id = ? AND claim_owner = ? AND claim_epoch = ? AND target_epoch = ?
  AND request_bound = 0 AND receipt_to_token IS NULL AND committed = 0`,
		mutation[:], claim.Owner[:], claim.Epoch, targetEpoch)
	if err != nil {
		return err
	}
	if changed, _ := deleted.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	return tx.Commit()
}

// BindSourceDriverMutationRequest binds the exact external request after target preparation.
func (c *Catalog) BindSourceDriverMutationRequest(
	ctx context.Context,
	mutation MutationID,
	claim MutationClaim,
	digest [sha256.Size]byte,
) (SourceDriverMutationReservation, error) {
	if mutation == (MutationID{}) || validateMutationClaim(claim) != nil || digest == ([sha256.Size]byte{}) {
		return SourceDriverMutationReservation{}, fmt.Errorf("%w: invalid source driver mutation request binding", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	reservation, found, err := readSourceDriverMutationReservation(ctx, tx, mutation)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if !found {
		return SourceDriverMutationReservation{}, ErrNotFound
	}
	if reservation.Claim != claim {
		return SourceDriverMutationReservation{}, ErrMutationClaimed
	}
	if !reservation.TargetsPrepared || reservation.Receipt != nil || reservation.Committed {
		return SourceDriverMutationReservation{}, ErrInvalidTransition
	}
	if err := validateSourceDriverTargetEpoch(ctx, tx, reservation.Authority, reservation.TargetEpoch); err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if reservation.RequestBound {
		if reservation.RequestDigest != digest {
			return SourceDriverMutationReservation{}, ErrMutationConflict
		}
		if err := tx.Commit(); err != nil {
			return SourceDriverMutationReservation{}, err
		}
		return reservation, nil
	}
	updated, err := tx.ExecContext(ctx, `
UPDATE source_driver_mutation_reservations
SET request_digest = ?, request_bound = 1
WHERE mutation_id = ? AND claim_owner = ? AND claim_epoch = ?
	AND targets_prepared = 1 AND request_bound = 0 AND receipt_to_token IS NULL AND committed = 0`,
		digest[:], mutation[:], claim.Owner[:], claim.Epoch)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if changed, _ := updated.RowsAffected(); changed != 1 {
		return SourceDriverMutationReservation{}, ErrMutationConflict
	}
	reservation.RequestDigest = digest
	reservation.RequestBound = true
	if err := tx.Commit(); err != nil {
		return SourceDriverMutationReservation{}, err
	}
	return reservation, nil
}

// SourceDriverMutationReservation returns one exact durable pre-I/O reservation.
func (c *Catalog) SourceDriverMutationReservation(
	ctx context.Context,
	mutation MutationID,
) (SourceDriverMutationReservation, error) {
	if mutation == (MutationID{}) {
		return SourceDriverMutationReservation{}, fmt.Errorf("%w: mutation id is zero", ErrInvalidObject)
	}
	reservation, found, err := readSourceDriverMutationReservation(ctx, c.readDB, mutation)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if !found {
		return SourceDriverMutationReservation{}, ErrNotFound
	}
	return reservation, nil
}

// ActiveSourceDriverMutationReservation returns the authority's unique pre-commit reservation.
func (c *Catalog) ActiveSourceDriverMutationReservation(
	ctx context.Context,
	authority causal.SourceAuthorityID,
) (*SourceDriverMutationReservation, error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil {
		return nil, fmt.Errorf("%w: invalid source driver mutation reservation authority", ErrInvalidObject)
	}
	var rawMutation []byte
	err := c.readDB.QueryRowContext(ctx, `
SELECT mutation_id FROM source_driver_mutation_reservations
WHERE source_authority = ? AND committed = 0`, string(authority)).Scan(&rawMutation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(rawMutation) != len(MutationID{}) {
		return nil, ErrIntegrity
	}
	var mutation MutationID
	copy(mutation[:], rawMutation)
	reservation, found, err := readSourceDriverMutationReservation(ctx, c.readDB, mutation)
	if err != nil {
		return nil, err
	}
	if !found || reservation.Authority != authority || reservation.Committed {
		return nil, ErrIntegrity
	}
	return &reservation, nil
}

// PrepareSourceDriverMutationReservationBatch advances at most 128 exact target rows.
func (c *Catalog) PrepareSourceDriverMutationReservationBatch(
	ctx context.Context,
	mutation MutationID,
	claim MutationClaim,
) (SourceDriverMutationReservation, error) {
	if mutation == (MutationID{}) || validateMutationClaim(claim) != nil {
		return SourceDriverMutationReservation{}, fmt.Errorf("%w: invalid source driver mutation reservation batch", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	reservation, found, err := readSourceDriverMutationReservation(ctx, tx, mutation)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if !found {
		return SourceDriverMutationReservation{}, ErrNotFound
	}
	if reservation.Claim != claim {
		return SourceDriverMutationReservation{}, ErrMutationClaimed
	}
	if reservation.Committed {
		return SourceDriverMutationReservation{}, ErrInvalidTransition
	}
	if reservation.TargetsPrepared {
		if err := tx.Commit(); err != nil {
			return SourceDriverMutationReservation{}, err
		}
		return reservation, nil
	}
	if reservation.RequestBound {
		return SourceDriverMutationReservation{}, ErrIntegrity
	}
	if err := validateSourceDriverTargetEpoch(ctx, tx, reservation.Authority, reservation.TargetEpoch); err != nil {
		return SourceDriverMutationReservation{}, err
	}
	var digestState []byte
	if err := tx.QueryRowContext(ctx, `
SELECT target_digest_state FROM source_driver_mutation_reservations WHERE mutation_id = ?`,
		mutation[:]).Scan(&digestState); err != nil {
		return SourceDriverMutationReservation{}, err
	}
	rows, err := tx.QueryContext(ctx, `
SELECT tenant, generation FROM desired_tenants
WHERE content_source_id = ? AND tenant > ? ORDER BY tenant LIMIT ?`,
		string(reservation.Authority), string(reservation.TargetCursor), sourceDriverTargetBatchSize)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	targets := make([]SourceDriverTarget, 0, sourceDriverTargetBatchSize)
	for rows.Next() {
		var tenant string
		var generation uint64
		if err := rows.Scan(&tenant, &generation); err != nil {
			_ = rows.Close()
			return SourceDriverMutationReservation{}, err
		}
		targets = append(targets, SourceDriverTarget{Tenant: TenantID(tenant), Generation: Generation(generation)})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return SourceDriverMutationReservation{}, err
	}
	if err := rows.Close(); err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if len(targets) == 0 {
		digest, err := finishSourceDriverTargetsDigestState(digestState)
		if err != nil {
			return SourceDriverMutationReservation{}, err
		}
		if reservation.DeclaredTargetCount != reservation.TargetCount || digest != reservation.TargetsDigest {
			return SourceDriverMutationReservation{}, ErrMutationConflict
		}
		updated, err := tx.ExecContext(ctx, `
UPDATE source_driver_mutation_reservations SET targets_prepared = 1
WHERE mutation_id = ? AND claim_owner = ? AND claim_epoch = ? AND targets_prepared = 0
  AND target_epoch = ? AND target_cursor = ? AND declared_target_count = ? AND target_digest_state = ?`,
			mutation[:], claim.Owner[:], claim.Epoch, reservation.TargetEpoch, string(reservation.TargetCursor),
			reservation.DeclaredTargetCount, digestState)
		if err != nil {
			return SourceDriverMutationReservation{}, err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return SourceDriverMutationReservation{}, ErrMutationConflict
		}
	} else {
		if reservation.DeclaredTargetCount+uint64(len(targets)) > reservation.TargetCount {
			return SourceDriverMutationReservation{}, ErrMutationConflict
		}
		priorState := append([]byte(nil), digestState...)
		prior := reservation.TargetCursor
		for _, target := range targets {
			inserted, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_mutation_reservation_targets(mutation_id, tenant, generation)
VALUES (?, ?, ?)
ON CONFLICT(mutation_id, tenant) DO UPDATE
SET generation = source_driver_mutation_reservation_targets.generation
WHERE source_driver_mutation_reservation_targets.generation = excluded.generation`,
				mutation[:], string(target.Tenant), uint64(target.Generation))
			if err != nil {
				return SourceDriverMutationReservation{}, mapConstraint(err)
			}
			if changed, _ := inserted.RowsAffected(); changed != 1 {
				return SourceDriverMutationReservation{}, ErrMutationConflict
			}
			var appendErr error
			digestState, appendErr = appendSourceDriverTargetsDigestState(digestState, prior, target)
			if appendErr != nil {
				return SourceDriverMutationReservation{}, appendErr
			}
			prior = target.Tenant
		}
		updated, err := tx.ExecContext(ctx, `
UPDATE source_driver_mutation_reservations
SET target_cursor = ?, declared_target_count = declared_target_count + ?, target_digest_state = ?
WHERE mutation_id = ? AND claim_owner = ? AND claim_epoch = ? AND targets_prepared = 0
  AND target_epoch = ? AND target_cursor = ? AND declared_target_count = ? AND target_digest_state = ?`,
			string(prior), len(targets), digestState, mutation[:], claim.Owner[:], claim.Epoch,
			reservation.TargetEpoch, string(reservation.TargetCursor), reservation.DeclaredTargetCount, priorState)
		if err != nil {
			return SourceDriverMutationReservation{}, err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return SourceDriverMutationReservation{}, ErrMutationConflict
		}
	}
	reservation, found, err = readSourceDriverMutationReservation(ctx, tx, mutation)
	if err != nil || !found {
		return SourceDriverMutationReservation{}, err
	}
	if err := tx.Commit(); err != nil {
		return SourceDriverMutationReservation{}, err
	}
	return reservation, nil
}

// SourceDriverMutationReservationTargets pages one fully prepared immutable target set.
func (c *Catalog) SourceDriverMutationReservationTargets(
	ctx context.Context,
	mutation MutationID,
	after TenantID,
	limit int,
) (SourceDriverTargetPage, error) {
	if mutation == (MutationID{}) || limit < 1 || limit > sourceDriverTargetBatchSize {
		return SourceDriverTargetPage{}, fmt.Errorf("%w: invalid source driver mutation target page", ErrInvalidObject)
	}
	reservation, err := c.SourceDriverMutationReservation(ctx, mutation)
	if err != nil {
		return SourceDriverTargetPage{}, err
	}
	if !reservation.TargetsPrepared {
		return SourceDriverTargetPage{}, ErrInvalidTransition
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT tenant, generation FROM source_driver_mutation_reservation_targets
WHERE mutation_id = ? AND tenant > ? ORDER BY tenant LIMIT ?`, mutation[:], string(after), limit+1)
	if err != nil {
		return SourceDriverTargetPage{}, err
	}
	defer func() { _ = rows.Close() }()
	values := make([]SourceDriverTarget, 0, limit+1)
	for rows.Next() {
		var tenant string
		var generation uint64
		if err := rows.Scan(&tenant, &generation); err != nil {
			return SourceDriverTargetPage{}, err
		}
		values = append(values, SourceDriverTarget{Tenant: TenantID(tenant), Generation: Generation(generation)})
	}
	if err := rows.Err(); err != nil {
		return SourceDriverTargetPage{}, err
	}
	page := SourceDriverTargetPage{Targets: values}
	if len(page.Targets) > limit {
		page.Targets = page.Targets[:limit]
		page.Next = page.Targets[len(page.Targets)-1].Tenant
	}
	return page, nil
}

// RecordSourceDriverMutationReceipt binds one exact external result once.
func (c *Catalog) RecordSourceDriverMutationReceipt(
	ctx context.Context,
	mutation MutationID,
	claim MutationClaim,
	proof SourceDriverMutationReceiptProof,
) (SourceDriverMutationReservation, error) {
	if mutation == (MutationID{}) || validateMutationClaim(claim) != nil || !validSourceDriverToken(proof.ToToken) ||
		proof.Digest == ([sha256.Size]byte{}) || (proof.Result != "" && !validSourceKey(proof.Result)) {
		return SourceDriverMutationReservation{}, fmt.Errorf("%w: invalid source driver mutation receipt", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	reservation, found, err := readSourceDriverMutationReservation(ctx, tx, mutation)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if !found {
		return SourceDriverMutationReservation{}, ErrNotFound
	}
	if reservation.Claim != claim {
		return SourceDriverMutationReservation{}, ErrMutationClaimed
	}
	if !reservation.TargetsPrepared || !reservation.RequestBound || reservation.Committed ||
		proof.ToToken == reservation.FromToken {
		return SourceDriverMutationReservation{}, ErrInvalidTransition
	}
	if reservation.Receipt != nil {
		if *reservation.Receipt != proof {
			return SourceDriverMutationReservation{}, ErrMutationConflict
		}
		if err := tx.Commit(); err != nil {
			return SourceDriverMutationReservation{}, err
		}
		return reservation, nil
	}
	record, found, err := readPreparedMutation(ctx, tx, mutation)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if !found || record.State != MutationApplying || record.Claim == nil || *record.Claim != claim {
		return SourceDriverMutationReservation{}, ErrMutationClaimed
	}
	if record.Kind == MutationCreate {
		if proof.Result == "" || record.SourceResult == nil ||
			record.SourceResult.SourceAuthority != reservation.Authority || record.SourceResult.SourceKey != proof.Result {
			return SourceDriverMutationReservation{}, ErrSourceLocatorStale
		}
	} else if proof.Result != "" {
		return SourceDriverMutationReservation{}, ErrMutationConflict
	}
	updated, err := tx.ExecContext(ctx, `
UPDATE source_driver_mutation_reservations
SET receipt_to_token = ?, receipt_result_key = ?, receipt_digest = ?
WHERE mutation_id = ? AND claim_owner = ? AND claim_epoch = ?
  AND targets_prepared = 1 AND request_bound = 1 AND receipt_to_token IS NULL`,
		proof.ToToken, string(proof.Result), proof.Digest[:], mutation[:], claim.Owner[:], claim.Epoch)
	if err != nil {
		return SourceDriverMutationReservation{}, err
	}
	if changed, _ := updated.RowsAffected(); changed != 1 {
		return SourceDriverMutationReservation{}, ErrMutationConflict
	}
	reservation.Receipt = &proof
	if err := tx.Commit(); err != nil {
		return SourceDriverMutationReservation{}, err
	}
	return reservation, nil
}

func readSourceDriverMutationReservation(
	ctx context.Context,
	query sourceDriverCheckpointQueryer,
	mutation MutationID,
) (SourceDriverMutationReservation, bool, error) {
	var reservation SourceDriverMutationReservation
	var rawMutation, claimOwner, declaration, targets, operation, sourceOperation, change, requestDigest, digestState, receiptDigest []byte
	var authority, owner, tenant, fromToken, cursor string
	var authorityGeneration, targetCount, generation, predecessor, targetEpoch, declaredCount, claimEpoch uint64
	var prepared, requestBound, committed int
	var receiptToken, receiptResult sql.NullString
	err := query.QueryRowContext(ctx, `
SELECT mutation_id, claim_owner, claim_epoch, source_authority, fleet_owner_id,
       authority_generation, declaration_digest, target_count, targets_digest,
       mutation_tenant, mutation_generation, from_token, predecessor_revision,
       stage_operation_id, source_operation_id, change_id, request_digest, request_bound, committed,
       target_epoch, declared_target_count, target_cursor, target_digest_state,
       targets_prepared, receipt_to_token, receipt_result_key, receipt_digest
FROM source_driver_mutation_reservations WHERE mutation_id = ?`, mutation[:]).Scan(
		&rawMutation, &claimOwner, &claimEpoch, &authority, &owner, &authorityGeneration,
		&declaration, &targetCount, &targets, &tenant, &generation, &fromToken, &predecessor,
		&operation, &sourceOperation, &change, &requestDigest, &requestBound, &committed, &targetEpoch, &declaredCount,
		&cursor, &digestState, &prepared, &receiptToken, &receiptResult, &receiptDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceDriverMutationReservation{}, false, nil
	}
	if err != nil {
		return SourceDriverMutationReservation{}, false, err
	}
	if len(rawMutation) != len(MutationID{}) || len(claimOwner) != len(MutationOwnerID{}) ||
		len(declaration) != sha256.Size || len(targets) != sha256.Size || len(operation) != len(causal.OperationID{}) ||
		len(sourceOperation) != len(causal.OperationID{}) || len(change) != len(causal.ChangeID{}) ||
		len(digestState) != sourceDriverTargetsDigestStateSize || requestBound < 0 || requestBound > 1 ||
		committed < 0 || committed > 1 ||
		(requestBound == 0 && requestDigest != nil) || (requestBound == 1 && len(requestDigest) != sha256.Size) ||
		(receiptToken.Valid != receiptResult.Valid) || (receiptToken.Valid != (receiptDigest != nil)) ||
		(receiptDigest != nil && len(receiptDigest) != sha256.Size) {
		return SourceDriverMutationReservation{}, false, ErrIntegrity
	}
	copy(reservation.Mutation[:], rawMutation)
	copy(reservation.Claim.Owner[:], claimOwner)
	reservation.Claim.Epoch = claimEpoch
	reservation.Authority = causal.SourceAuthorityID(authority)
	reservation.FleetOwner = SourceAuthorityFleetOwnerID(owner)
	reservation.AuthorityGeneration = causal.Generation(authorityGeneration)
	copy(reservation.DeclarationDigest[:], declaration)
	reservation.TargetCount = targetCount
	copy(reservation.TargetsDigest[:], targets)
	reservation.Target = SourceDriverTarget{Tenant: TenantID(tenant), Generation: Generation(generation)}
	reservation.FromToken = fromToken
	reservation.Predecessor = causal.Revision(predecessor)
	copy(reservation.Operation[:], operation)
	copy(reservation.SourceOperation[:], sourceOperation)
	copy(reservation.ChangeID[:], change)
	copy(reservation.RequestDigest[:], requestDigest)
	reservation.RequestBound = requestBound != 0
	reservation.Committed = committed != 0
	reservation.TargetEpoch = targetEpoch
	reservation.DeclaredTargetCount = declaredCount
	reservation.TargetCursor = TenantID(cursor)
	reservation.TargetsPrepared = prepared != 0
	if receiptToken.Valid {
		proof := SourceDriverMutationReceiptProof{ToToken: receiptToken.String, Result: SourceObjectKey(receiptResult.String)}
		copy(proof.Digest[:], receiptDigest)
		reservation.Receipt = &proof
	}
	if err := validateSourceDriverMutationReservationRequest(reservation.SourceDriverMutationReservationRequest); err != nil ||
		reservation.TargetEpoch == 0 || reservation.DeclaredTargetCount > reservation.TargetCount ||
		(reservation.TargetsPrepared && (reservation.DeclaredTargetCount != reservation.TargetCount ||
			reservation.TargetsDigest != [sha256.Size]byte(digestState))) ||
		(reservation.Receipt != nil && !reservation.RequestBound) ||
		(reservation.Committed && reservation.Receipt == nil) ||
		(reservation.Receipt != nil && (!validSourceDriverToken(reservation.Receipt.ToToken) ||
			reservation.Receipt.ToToken == reservation.FromToken || reservation.Receipt.Digest == ([sha256.Size]byte{}))) {
		return SourceDriverMutationReservation{}, false, ErrIntegrity
	}
	return reservation, true, nil
}

func validateSourceDriverMutationReservationRequest(request SourceDriverMutationReservationRequest) error {
	if request.Mutation == (MutationID{}) || validateMutationClaim(request.Claim) != nil ||
		causal.ValidateSourceAuthorityID(request.Authority) != nil ||
		ValidateSourceAuthorityFleetOwnerID(request.FleetOwner) != nil || request.AuthorityGeneration == 0 ||
		request.DeclarationDigest == ([sha256.Size]byte{}) || request.TargetCount == 0 ||
		request.TargetCount > SourceDriverTargetLimit || request.TargetsDigest == ([sha256.Size]byte{}) ||
		request.Target.Tenant == "" || request.Target.Generation == 0 || !validSourceDriverToken(request.FromToken) ||
		request.Predecessor == 0 || request.Operation == (causal.OperationID{}) ||
		request.SourceOperation == (causal.OperationID{}) || request.SourceOperation == request.Operation ||
		request.ChangeID == (causal.ChangeID{}) {
		return fmt.Errorf("%w: invalid source driver mutation reservation", ErrInvalidObject)
	}
	if _, err := NewTenantID(string(request.Target.Tenant)); err != nil {
		return fmt.Errorf("%w: invalid source driver mutation target", ErrInvalidObject)
	}
	return nil
}
