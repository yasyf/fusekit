package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

type sourceDriverTargetState struct {
	SourceDriverTarget
	RootKey         SourceObjectKey
	CatalogRevision Revision
}

// SourceDriverCheckpoint returns the authority-wide semantic source checkpoint.
func (c *Catalog) SourceDriverCheckpoint(
	ctx context.Context,
	authority causal.SourceAuthorityID,
) (SourceDriverCheckpoint, error) {
	checkpoint, found, err := readSourceDriverCheckpoint(ctx, c.readDB, authority)
	if err != nil {
		return SourceDriverCheckpoint{}, err
	}
	if !found {
		return SourceDriverCheckpoint{}, ErrNotFound
	}
	return checkpoint, nil
}

// SourceDriverTargetCheckpoint returns one tenant projection watermark.
func (c *Catalog) SourceDriverTargetCheckpoint(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	tenant TenantID,
	generation Generation,
) (SourceDriverTargetCheckpoint, error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil || tenant == "" || generation == 0 {
		return SourceDriverTargetCheckpoint{}, fmt.Errorf("%w: invalid source driver target checkpoint", ErrInvalidObject)
	}
	var checkpoint SourceDriverTargetCheckpoint
	var targetTenant, root string
	var targetGeneration, targetEpoch, sourceRevision, catalogRevision, checkpointRevision, snapshotRequired uint64
	err := c.readDB.QueryRowContext(ctx, `
SELECT target.tenant, target.generation, target.root_key, visibility.active_source_revision,
       target.catalog_head, checkpoint.target_epoch, checkpoint.source_revision, checkpoint.snapshot_required
FROM source_driver_visibility visibility
JOIN source_driver_publication_targets target
  ON target.source_authority = visibility.source_authority
 AND target.publication_id = visibility.active_publication_id
JOIN source_driver_checkpoints checkpoint
  ON checkpoint.source_authority = visibility.source_authority
WHERE visibility.source_authority = ? AND target.tenant = ?`, string(authority), string(tenant)).Scan(
		&targetTenant, &targetGeneration, &root, &sourceRevision, &catalogRevision, &targetEpoch,
		&checkpointRevision, &snapshotRequired,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceDriverTargetCheckpoint{}, ErrNotFound
	}
	if err != nil {
		return SourceDriverTargetCheckpoint{}, fmt.Errorf("catalog: read source driver target checkpoint: %w", err)
	}
	if Generation(targetGeneration) != generation {
		return SourceDriverTargetCheckpoint{}, ErrMutationConflict
	}
	checkpoint.SourceDriverTarget = SourceDriverTarget{
		Tenant: TenantID(targetTenant), Generation: Generation(targetGeneration),
	}
	checkpoint.RootKey = SourceObjectKey(root)
	checkpoint.TargetEpoch = targetEpoch
	checkpoint.SourceRevision = causal.Revision(sourceRevision)
	checkpoint.CatalogRevision = Revision(catalogRevision)
	derived, err := DeriveSourceDriverRootKey(authority, checkpoint.Tenant)
	if err != nil || checkpoint.Tenant != tenant || checkpoint.RootKey != derived || checkpoint.CatalogRevision == 0 ||
		checkpoint.TargetEpoch == 0 ||
		(checkpoint.SourceRevision == 0 && snapshotRequired == 0) ||
		(checkpoint.SourceRevision != 0 && checkpoint.SourceRevision != causal.Revision(checkpointRevision)) {
		return SourceDriverTargetCheckpoint{}, ErrIntegrity
	}
	return checkpoint, nil
}

// SourceDriverCommittedTargetCheckpoints pages the immutable target watermarks
// for one committed authority publication.
func (c *Catalog) SourceDriverCommittedTargetCheckpoints(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	after TenantID,
	limit int,
) (SourceDriverTargetCheckpointPage, error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil || operation == (causal.OperationID{}) ||
		limit < 1 || limit > SourceDriverTargetCheckpointPageLimit {
		return SourceDriverTargetCheckpointPage{}, fmt.Errorf("%w: invalid committed target page", ErrInvalidObject)
	}
	if after != "" {
		if _, err := NewTenantID(string(after)); err != nil {
			return SourceDriverTargetCheckpointPage{}, fmt.Errorf("%w: invalid committed target cursor", ErrInvalidObject)
		}
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT target.tenant, target.generation, target.root_key, publication.source_revision,
	   target.catalog_head, publication.target_epoch
FROM source_driver_stage_receipts receipt
JOIN source_driver_publications publication
  ON publication.source_authority = receipt.source_authority
 AND publication.publication_id = receipt.stage_operation_id
JOIN source_driver_publication_targets target
  ON target.source_authority = publication.source_authority
 AND target.publication_id = publication.publication_id
WHERE receipt.source_authority = ? AND receipt.stage_operation_id = ?
  AND publication.prepared = 1 AND target.prepared = 1 AND target.tenant > ?
ORDER BY target.tenant LIMIT ?`, string(authority), operation[:], string(after), limit+1)
	if err != nil {
		return SourceDriverTargetCheckpointPage{}, err
	}
	defer func() { _ = rows.Close() }()
	values := make([]SourceDriverTargetCheckpoint, 0, limit+1)
	for rows.Next() {
		var tenant, root string
		var generation, sourceRevision, catalogRevision, targetEpoch uint64
		if err := rows.Scan(
			&tenant, &generation, &root, &sourceRevision, &catalogRevision, &targetEpoch,
		); err != nil {
			return SourceDriverTargetCheckpointPage{}, err
		}
		checkpoint := SourceDriverTargetCheckpoint{
			SourceDriverTarget: SourceDriverTarget{
				Tenant: TenantID(tenant), Generation: Generation(generation),
			},
			RootKey: SourceObjectKey(root), SourceRevision: causal.Revision(sourceRevision),
			CatalogRevision: Revision(catalogRevision), TargetEpoch: targetEpoch,
		}
		derived, deriveErr := DeriveSourceDriverRootKey(authority, checkpoint.Tenant)
		if deriveErr != nil || checkpoint.Generation == 0 || checkpoint.SourceRevision == 0 ||
			checkpoint.CatalogRevision == 0 || checkpoint.TargetEpoch == 0 || checkpoint.RootKey != derived {
			return SourceDriverTargetCheckpointPage{}, ErrIntegrity
		}
		values = append(values, checkpoint)
	}
	if err := rows.Err(); err != nil {
		return SourceDriverTargetCheckpointPage{}, err
	}
	page := SourceDriverTargetCheckpointPage{Targets: values}
	if len(page.Targets) > limit {
		page.Targets = page.Targets[:limit]
		page.Next = page.Targets[len(page.Targets)-1].Tenant
	}
	return page, nil
}

// RequireSourceDriverSnapshot marks the exact authority checkpoint as reset-required.
func (c *Catalog) RequireSourceDriverSnapshot(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	token string,
	reason SourceDriverSnapshotReason,
) (SourceDriverCheckpoint, error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil || !validSourceDriverToken(token) ||
		(reason != SourceDriverSnapshotReset && reason != SourceDriverSnapshotExpiredFloor) {
		return SourceDriverCheckpoint{}, fmt.Errorf("%w: invalid source driver snapshot request", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceDriverCheckpoint{}, fmt.Errorf("catalog: begin source driver snapshot request: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	checkpoint, found, err := readSourceDriverCheckpoint(ctx, tx, authority)
	if err != nil {
		return SourceDriverCheckpoint{}, err
	}
	if !found {
		return SourceDriverCheckpoint{}, ErrNotFound
	}
	if checkpoint.Token != token {
		return SourceDriverCheckpoint{}, ErrSourcePredecessor
	}
	if checkpoint.SnapshotRequired == 0 || reason > checkpoint.SnapshotRequired {
		if _, err := tx.ExecContext(ctx, `
UPDATE source_driver_checkpoints SET snapshot_required = ?
WHERE source_authority = ?`, uint8(reason), string(authority)); err != nil {
			return SourceDriverCheckpoint{}, fmt.Errorf("catalog: require source driver snapshot: %w", err)
		}
		checkpoint.SnapshotRequired = reason
	}
	if err := tx.Commit(); err != nil {
		return SourceDriverCheckpoint{}, fmt.Errorf("catalog: commit source driver snapshot request: %w", err)
	}
	return checkpoint, nil
}

// RebindSourceDriverCheckpoint fences an authority checkpoint to a newer immutable declaration.
func (c *Catalog) RebindSourceDriverCheckpoint(
	ctx context.Context,
	request SourceDriverCheckpointRebind,
) (SourceDriverCheckpoint, error) {
	if request.Expected.Authority == "" || request.AuthorityGeneration <= request.Expected.AuthorityGeneration ||
		request.DeclarationDigest == ([sha256.Size]byte{}) {
		return SourceDriverCheckpoint{}, fmt.Errorf("%w: invalid source driver checkpoint rebind", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceDriverCheckpoint{}, fmt.Errorf("catalog: begin source driver checkpoint rebind: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, found, err := readSourceDriverCheckpoint(ctx, tx, request.Expected.Authority)
	if err != nil {
		return SourceDriverCheckpoint{}, err
	}
	if !found {
		return SourceDriverCheckpoint{}, ErrNotFound
	}
	replayed := request.Expected
	replayed.AuthorityGeneration = request.AuthorityGeneration
	replayed.DeclarationDigest = request.DeclarationDigest
	replayed.SnapshotRequired = SourceDriverSnapshotReset
	if current != request.Expected && current != replayed {
		return SourceDriverCheckpoint{}, ErrMutationConflict
	}
	if err := requireSourceDriverFleetMember(
		ctx, tx, current.Authority, current.FleetOwner,
		request.AuthorityGeneration, request.DeclarationDigest,
	); err != nil {
		return SourceDriverCheckpoint{}, err
	}
	if current == replayed {
		if err := tx.Commit(); err != nil {
			return SourceDriverCheckpoint{}, err
		}
		return current, nil
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_driver_stages stage
    LEFT JOIN source_driver_stage_receipts receipt
      ON receipt.source_authority = stage.source_authority
     AND receipt.stage_operation_id = stage.stage_operation_id
    WHERE stage.source_authority = ? AND receipt.stage_operation_id IS NULL
)`,
		string(current.Authority)).Scan(&pending); err != nil {
		return SourceDriverCheckpoint{}, err
	}
	if pending != 0 {
		return SourceDriverCheckpoint{}, ErrMutationActive
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_driver_checkpoints
SET authority_generation = ?, declaration_digest = ?, snapshot_required = ?
WHERE source_authority = ?`, uint64(request.AuthorityGeneration), request.DeclarationDigest[:],
		uint8(SourceDriverSnapshotReset), string(current.Authority)); err != nil {
		return SourceDriverCheckpoint{}, mapConstraint(err)
	}
	current.AuthorityGeneration = request.AuthorityGeneration
	current.DeclarationDigest = request.DeclarationDigest
	current.SnapshotRequired = SourceDriverSnapshotReset
	if err := tx.Commit(); err != nil {
		return SourceDriverCheckpoint{}, fmt.Errorf("catalog: commit source driver checkpoint rebind: %w", err)
	}
	return current, nil
}

func requireSourceDriverFleetMember(
	ctx context.Context,
	query sourceAuthorityFleetQueryer,
	authority causal.SourceAuthorityID,
	owner SourceAuthorityFleetOwnerID,
	generation causal.Generation,
	declaration [sha256.Size]byte,
) error {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil ||
		ValidateSourceAuthorityFleetOwnerID(owner) != nil || generation == 0 || declaration == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: invalid source driver fleet fence", ErrInvalidObject)
	}
	var stored []byte
	err := query.QueryRowContext(ctx, `
SELECT member.declaration_digest
FROM source_authority_fleet_heads head
JOIN source_authority_fleet_members member
  ON member.owner_id = head.owner_id AND member.generation = head.generation
WHERE head.owner_id = ? AND head.generation = ? AND member.source_authority = ?`,
		string(owner), uint64(generation), string(authority)).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrSourceObserverConflict
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(stored, declaration[:]) {
		return ErrSourceObserverConflict
	}
	return nil
}

func requireImmutableSourceDriverFleetMember(
	ctx context.Context,
	query sourceAuthorityFleetQueryer,
	authority causal.SourceAuthorityID,
	owner SourceAuthorityFleetOwnerID,
	generation causal.Generation,
	declaration [sha256.Size]byte,
) error {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil ||
		ValidateSourceAuthorityFleetOwnerID(owner) != nil || generation == 0 ||
		declaration == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: invalid immutable source driver fleet fence", ErrInvalidObject)
	}
	var stored []byte
	err := query.QueryRowContext(ctx, `
SELECT declaration_digest FROM source_authority_fleet_members
WHERE owner_id = ? AND generation = ? AND source_authority = ?`,
		string(owner), uint64(generation), string(authority)).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrSourceObserverConflict
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(stored, declaration[:]) {
		return ErrSourceObserverConflict
	}
	return nil
}

type sourceDriverCheckpointQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readSourceDriverCheckpoint(
	ctx context.Context,
	query sourceDriverCheckpointQueryer,
	authority causal.SourceAuthorityID,
) (SourceDriverCheckpoint, bool, error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil {
		return SourceDriverCheckpoint{}, false, fmt.Errorf("%w: invalid source driver authority", ErrInvalidObject)
	}
	var checkpoint SourceDriverCheckpoint
	var storedAuthority, owner, token, cause, origin string
	var authorityGeneration, targetEpoch, targetCount, sourceRevision, originGeneration, snapshotReason uint64
	var declaration, targets, tokenDigest, operation, change []byte
	err := query.QueryRowContext(ctx, `
SELECT source_authority, fleet_owner_id, authority_generation, declaration_digest,
       target_epoch, target_count, targets_digest, source_operation_id, change_id, cause,
       origin_domain, origin_generation, applied_token, token_digest,
       source_revision, snapshot_required
FROM source_driver_checkpoints WHERE source_authority = ?`, string(authority)).Scan(
		&storedAuthority, &owner, &authorityGeneration, &declaration, &targetEpoch, &targetCount, &targets,
		&operation, &change, &cause, &origin, &originGeneration, &token, &tokenDigest,
		&sourceRevision, &snapshotReason,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceDriverCheckpoint{}, false, nil
	}
	if err != nil {
		return SourceDriverCheckpoint{}, false, fmt.Errorf("catalog: read source driver checkpoint: %w", err)
	}
	if len(declaration) != sha256.Size || len(targets) != sha256.Size || len(tokenDigest) != sha256.Size ||
		len(operation) != len(causal.OperationID{}) || len(change) != len(causal.ChangeID{}) {
		return SourceDriverCheckpoint{}, false, ErrIntegrity
	}
	checkpoint.Authority = causal.SourceAuthorityID(storedAuthority)
	checkpoint.FleetOwner = SourceAuthorityFleetOwnerID(owner)
	checkpoint.AuthorityGeneration = causal.Generation(authorityGeneration)
	copy(checkpoint.DeclarationDigest[:], declaration)
	checkpoint.TargetEpoch = targetEpoch
	checkpoint.TargetCount = targetCount
	copy(checkpoint.TargetsDigest[:], targets)
	copy(checkpoint.SourceOperation[:], operation)
	copy(checkpoint.ChangeID[:], change)
	checkpoint.Cause = causal.Cause(cause)
	checkpoint.Origin = causal.DomainID(origin)
	checkpoint.OriginGeneration = causal.Generation(originGeneration)
	checkpoint.Token = token
	copy(checkpoint.TokenDigest[:], tokenDigest)
	checkpoint.SourceRevision = causal.Revision(sourceRevision)
	checkpoint.SnapshotRequired = SourceDriverSnapshotReason(snapshotReason)
	if checkpoint.TokenDigest != sourceDriverTokenDigest(checkpoint.Token) {
		return SourceDriverCheckpoint{}, false, ErrIntegrity
	}
	if checkpoint.Authority != authority || checkpoint.FleetOwner == "" ||
		checkpoint.AuthorityGeneration == 0 || checkpoint.DeclarationDigest == ([sha256.Size]byte{}) || checkpoint.TargetEpoch == 0 ||
		checkpoint.TargetCount == 0 || checkpoint.TargetCount > SourceDriverTargetLimit ||
		checkpoint.TargetsDigest == ([sha256.Size]byte{}) || checkpoint.SourceRevision == 0 ||
		checkpoint.SourceOperation == (causal.OperationID{}) || checkpoint.ChangeID == (causal.ChangeID{}) ||
		(checkpoint.SnapshotRequired != 0 && checkpoint.SnapshotRequired != SourceDriverSnapshotReset &&
			checkpoint.SnapshotRequired != SourceDriverSnapshotExpiredFloor) {
		return SourceDriverCheckpoint{}, false, ErrIntegrity
	}
	return checkpoint, true, nil
}
