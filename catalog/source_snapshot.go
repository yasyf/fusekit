package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/yasyf/fusekit/causal"
)

const (
	SourceSnapshotPageLimit                   = 256
	SourceSnapshotPageInputLimit              = 2048
	SourceSnapshotPageObjectLimit             = 2048
	SourceSnapshotPageByteLimit               = 2 << 20
	SourceSnapshotTotalObjectLimit            = 10_000_000
	SourceSnapshotTotalByteLimit              = 2 << 30
	SourceSnapshotContentByteLimit      int64 = 64 << 30
	sourceSnapshotCursorLimit                 = 512
	sourceSnapshotAfterAppendPage             = "source_snapshot.after_append_page"
	sourceSnapshotAfterSetwisePromotion       = "source_snapshot.after_setwise_promotion"
)

// SourceSnapshotIdentity fixes one staged snapshot publication to its source fence and causal identity.
type SourceSnapshotIdentity struct {
	Authority           causal.SourceAuthorityID
	AuthorityGeneration causal.Generation
	Snapshot            string
	FenceDigest         [32]byte
	Change              causal.ChangeSet
}

// SourceSnapshotRoot is one complete-fleet tenant root declaration.
type SourceSnapshotRoot struct {
	Tenant     TenantID
	Generation Generation
	LogicalID  string
	RootKey    SourceObjectKey
}

// SourceSnapshotBinding records one logical materialization and its stable opaque key.
type SourceSnapshotBinding struct {
	LogicalID   string
	SourceKey   SourceObjectKey
	Fingerprint [32]byte
	Inputs      []SourceIndexLocator
}

// SourceSnapshotProjection is one normalized staged catalog object.
type SourceSnapshotProjection struct {
	Tenant     TenantID
	Generation Generation
	LogicalID  string
	Object     SourceObject
}

// SourceSnapshotPublicationPage is one bounded page of a complete snapshot plan and its output.
type SourceSnapshotPublicationPage struct {
	Cursor       string
	Next         string
	AffectedKeys []causal.LogicalKey
	Roots        []SourceSnapshotRoot
	Bindings     []SourceSnapshotBinding
	Objects      []SourceSnapshotProjection
}

// SourceSnapshotStageRef is the compact durable reference needed to replay promotion.
type SourceSnapshotStageRef struct {
	Authority   causal.SourceAuthorityID
	Snapshot    string
	FenceDigest [32]byte
	Digest      [32]byte
	Operation   causal.OperationID
	Revision    causal.Revision
}

type sourceSnapshotFenceCheckpoint struct {
	Identity  string
	Cursor    uint64
	RootEpoch string
}

type sourceSnapshotFenceProof struct {
	Authority           causal.SourceAuthorityID
	AuthorityGeneration causal.Generation
	Streams             []sourceSnapshotFenceCheckpoint
	Inbox               uint64
	RootDigest          [32]byte
	FleetDigest         [32]byte
}

// BeginSourceSnapshotPublication starts one exact catalog-backed publication stage.
func (c *Catalog) BeginSourceSnapshotPublication(ctx context.Context, identity SourceSnapshotIdentity) error {
	if identity.Authority == "" || identity.AuthorityGeneration == 0 ||
		identity.Snapshot == "" || len(identity.Snapshot) > sourceSnapshotCursorLimit ||
		identity.Change.SourceAuthority != identity.Authority || identity.Change.SourceRevision == 0 ||
		identity.Change.ChangeID == (causal.ChangeID{}) || identity.Change.OperationID == (causal.OperationID{}) ||
		(identity.Change.Cause != causal.CauseExternalUnattributed && identity.Change.Cause != causal.CauseBootstrap) ||
		identity.Change.Origin != "" ||
		identity.Change.OriginGeneration != 0 || len(identity.Change.AffectedKeys) != 0 {
		return fmt.Errorf("%w: incomplete source snapshot identity", ErrInvalidObject)
	}
	initial, err := sourceSnapshotInitialDigest(identity)
	if err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source snapshot publication: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var current string
	if err := tx.QueryRowContext(ctx, `SELECT snapshot_id FROM source_snapshot_sessions WHERE source_authority = ?`,
		string(identity.Authority)).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSourceObserverConflict
		}
		return err
	}
	if current != identity.Snapshot {
		return ErrSourceObserverConflict
	}
	var incremental int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM source_publication_stages stage
    LEFT JOIN source_driver_stage_receipts receipt
      ON receipt.source_authority = stage.source_authority
     AND receipt.stage_operation_id = stage.stage_operation_id
    WHERE stage.source_authority = ?
      AND (stage.stage_kind = 1 OR receipt.stage_operation_id IS NULL)
)`, string(identity.Authority)).Scan(&incremental); err != nil {
		return err
	}
	if incremental != 0 {
		return ErrSourceObserverConflict
	}
	var existing int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM source_snapshot_publications WHERE source_authority = ?`,
		string(identity.Authority)).Scan(&existing); err != nil {
		return err
	}
	if existing != 0 {
		var snapshot string
		var operation, change, fence, rolling []byte
		var cause string
		var revision, authorityGeneration uint64
		var pageCount uint64
		if err := tx.QueryRowContext(ctx, `
SELECT snapshot_id, operation_id, change_id, source_revision, cause, fence_digest,
       fence_authority_generation, rolling_digest, page_count
FROM source_snapshot_publications WHERE source_authority = ?`, string(identity.Authority)).
			Scan(&snapshot, &operation, &change, &revision, &cause, &fence,
				&authorityGeneration, &rolling, &pageCount); err != nil {
			return err
		}
		storedFence, err := stagedSourceSnapshotFenceDigest(ctx, tx, identity.Authority, identity.Snapshot)
		if err != nil {
			return err
		}
		if snapshot != identity.Snapshot || !bytes.Equal(operation, identity.Change.OperationID[:]) ||
			!bytes.Equal(change, identity.Change.ChangeID[:]) || revision != uint64(identity.Change.SourceRevision) ||
			cause != string(identity.Change.Cause) ||
			authorityGeneration != uint64(identity.AuthorityGeneration) ||
			!bytes.Equal(fence, identity.FenceDigest[:]) || storedFence != identity.FenceDigest ||
			(pageCount == 0 && !bytes.Equal(rolling, initial[:])) {
			return ErrSourceObserverConflict
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("catalog: finish source snapshot publication lookup: %w", err)
		}
		return nil
	}
	if err := validateSourceSnapshotWatermark(ctx, tx, identity.Change); err != nil {
		return err
	}
	proof, err := currentSourceSnapshotFenceProof(ctx, tx, identity.Authority)
	if err != nil {
		return err
	}
	if proof.AuthorityGeneration != identity.AuthorityGeneration {
		return ErrSourceObserverConflict
	}
	derivedFence, err := digestSourceSnapshotFence(proof)
	if err != nil {
		return err
	}
	if derivedFence != identity.FenceDigest {
		return ErrSourceObserverConflict
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_snapshot_publications(
    source_authority, snapshot_id, operation_id, change_id, source_revision, cause, fence_digest,
    fence_authority_generation, fence_inbox, fence_root_digest, fence_fleet_digest,
    next_cursor, complete, rolling_digest, page_count, last_affected_key, last_tenant, last_logical,
    affected_count, root_count, binding_count, object_count, metadata_bytes, content_bytes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0, ?, 0, '', '', '', 0, 0, 0, 0, 0, 0)`, string(identity.Authority), identity.Snapshot,
		identity.Change.OperationID[:], identity.Change.ChangeID[:], uint64(identity.Change.SourceRevision),
		string(identity.Change.Cause), identity.FenceDigest[:], uint64(identity.AuthorityGeneration),
		proof.Inbox, proof.RootDigest[:], proof.FleetDigest[:], initial[:]); err != nil {
		return mapConstraint(err)
	}
	for _, checkpoint := range proof.Streams {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_snapshot_fence_checkpoints(
    source_authority, snapshot_id, stream_identity, native_event_id, root_epoch
) VALUES (?, ?, ?, ?, ?)`, string(identity.Authority), identity.Snapshot, checkpoint.Identity,
			checkpoint.Cursor, checkpoint.RootEpoch); err != nil {
			return mapConstraint(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit source snapshot publication: %w", err)
	}
	return nil
}

// AbortSourceSnapshotPublication abandons one exact, unpromoted publication stage.
func (c *Catalog) AbortSourceSnapshotPublication(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	snapshot string,
) error {
	if authority == "" || snapshot == "" {
		return fmt.Errorf("%w: incomplete source snapshot publication abort", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source snapshot publication abort: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := abortSourceSnapshotPublicationTx(ctx, tx, authority, snapshot); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit source snapshot publication abort: %w", err)
	}
	return nil
}

func abortSourceSnapshotPublicationTx(
	ctx context.Context,
	tx *sql.Tx,
	authority causal.SourceAuthorityID,
	snapshot string,
) error {
	var operation []byte
	err := tx.QueryRowContext(ctx, `
SELECT operation_id FROM source_snapshot_publications
WHERE source_authority = ? AND snapshot_id = ?`, string(authority), snapshot).Scan(&operation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var promoted int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM source_operations WHERE operation_id = ?`, operation).Scan(&promoted); err != nil {
		return err
	}
	if promoted != 0 {
		return ErrSourceObserverConflict
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM content_stages
WHERE source_operation_id = ? AND stage_id IN (
    SELECT content_stage FROM source_snapshot_objects
    WHERE source_authority = ? AND snapshot_id = ? AND content_stage IS NOT NULL
)`, operation, string(authority), snapshot); err != nil {
		return fmt.Errorf("catalog: release staged snapshot content: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_snapshot_publications WHERE source_authority = ? AND snapshot_id = ?`,
		string(authority), snapshot); err != nil {
		return fmt.Errorf("catalog: discard staged snapshot publication: %w", err)
	}
	return nil
}

// AppendSourceSnapshotPublication appends and takes ownership of one exact bounded page.
func (c *Catalog) AppendSourceSnapshotPublication(
	ctx context.Context,
	identity SourceSnapshotIdentity,
	page SourceSnapshotPublicationPage,
) (result SourceSnapshotStageRef, resultErr error) {
	pageDigest, pageBytes, err := validateSourceSnapshotPage(identity, page)
	if err != nil {
		return SourceSnapshotStageRef{}, err
	}
	if pageBytes > SourceSnapshotPageByteLimit {
		return SourceSnapshotStageRef{}, fmt.Errorf("%w: source snapshot page exceeds byte limit", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceSnapshotStageRef{}, fmt.Errorf("catalog: begin source snapshot append: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var operation, change, fence, rolling []byte
	var revision, authorityGeneration uint64
	var next string
	var complete int
	var pages, affectedCount, rootCount, bindingCount, objectCount uint64
	var metadataBytes, contentBytes uint64
	var lastAffected, lastTenant, lastLogical string
	if err := tx.QueryRowContext(ctx, `
SELECT operation_id, change_id, source_revision, fence_digest, fence_authority_generation,
       next_cursor, complete, rolling_digest,
       page_count, last_affected_key, last_tenant, last_logical,
       affected_count, root_count, binding_count, object_count, metadata_bytes, content_bytes
FROM source_snapshot_publications WHERE source_authority = ? AND snapshot_id = ?`, string(identity.Authority), identity.Snapshot).
		Scan(&operation, &change, &revision, &fence, &authorityGeneration, &next, &complete, &rolling, &pages,
			&lastAffected, &lastTenant, &lastLogical, &affectedCount, &rootCount, &bindingCount, &objectCount,
			&metadataBytes, &contentBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SourceSnapshotStageRef{}, ErrSourceObserverConflict
		}
		return SourceSnapshotStageRef{}, err
	}
	if !bytes.Equal(operation, identity.Change.OperationID[:]) || !bytes.Equal(change, identity.Change.ChangeID[:]) ||
		revision != uint64(identity.Change.SourceRevision) ||
		authorityGeneration != uint64(identity.AuthorityGeneration) ||
		!bytes.Equal(fence, identity.FenceDigest[:]) ||
		len(rolling) != sha256.Size {
		return SourceSnapshotStageRef{}, ErrSourceObserverConflict
	}
	if complete != 0 || next != page.Cursor {
		var replayNext string
		var replayPage, replayRolling []byte
		err := tx.QueryRowContext(ctx, `
SELECT next_cursor, page_digest, rolling_digest FROM source_snapshot_pages
WHERE source_authority = ? AND snapshot_id = ? AND cursor = ?`,
			string(identity.Authority), identity.Snapshot, page.Cursor).Scan(&replayNext, &replayPage, &replayRolling)
		if errors.Is(err, sql.ErrNoRows) {
			return SourceSnapshotStageRef{}, ErrSourceObserverConflict
		}
		if err != nil {
			return SourceSnapshotStageRef{}, err
		}
		if replayNext != page.Next || !bytes.Equal(replayPage, pageDigest[:]) ||
			len(replayRolling) != sha256.Size {
			return SourceSnapshotStageRef{}, ErrSourceObserverConflict
		}
		if err := c.releaseReplayedSnapshotContent(ctx, tx, identity.Change.OperationID, page.Objects); err != nil {
			return SourceSnapshotStageRef{}, err
		}
		var replayDigest [32]byte
		copy(replayDigest[:], replayRolling)
		return SourceSnapshotStageRef{
			Authority: identity.Authority, Snapshot: identity.Snapshot, FenceDigest: identity.FenceDigest,
			Digest: replayDigest, Operation: identity.Change.OperationID, Revision: identity.Change.SourceRevision,
		}, tx.Commit()
	}
	if len(page.AffectedKeys) > 0 && string(page.AffectedKeys[0]) <= lastAffected ||
		len(page.Roots) > 0 && string(page.Roots[0].Tenant) <= lastTenant ||
		len(page.Bindings) > 0 && page.Bindings[0].LogicalID <= lastLogical {
		return SourceSnapshotStageRef{}, fmt.Errorf("%w: source snapshot page does not continue global ordering", ErrInvalidObject)
	}
	pageContentBytes, err := c.appendSourceSnapshotPageRows(ctx, tx, identity, page)
	if err != nil {
		return SourceSnapshotStageRef{}, err
	}
	if err := c.trip(sourceSnapshotAfterAppendPage); err != nil {
		return SourceSnapshotStageRef{}, err
	}
	var old [32]byte
	copy(old[:], rolling)
	nextDigest := sourceSnapshotRollingDigest(old, pageDigest)
	newComplete := page.Next == ""
	if len(page.AffectedKeys) > 0 {
		lastAffected = string(page.AffectedKeys[len(page.AffectedKeys)-1])
	}
	if len(page.Roots) > 0 {
		lastTenant = string(page.Roots[len(page.Roots)-1].Tenant)
	}
	if len(page.Bindings) > 0 {
		lastLogical = page.Bindings[len(page.Bindings)-1].LogicalID
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_snapshot_pages(
    source_authority, snapshot_id, cursor, next_cursor, page_digest, rolling_digest, page_bytes
) VALUES (?, ?, ?, ?, ?, ?, ?)`, string(identity.Authority), identity.Snapshot, page.Cursor, page.Next,
		pageDigest[:], nextDigest[:], pageBytes); err != nil {
		return SourceSnapshotStageRef{}, mapConstraint(err)
	}
	resultSQL, err := tx.ExecContext(ctx, `
UPDATE source_snapshot_publications SET
    next_cursor = ?, complete = ?, rolling_digest = ?, page_count = page_count + 1,
    last_affected_key = ?, last_tenant = ?, last_logical = ?,
    affected_count = affected_count + ?, root_count = root_count + ?,
    binding_count = binding_count + ?, object_count = object_count + ?,
    metadata_bytes = metadata_bytes + ?, content_bytes = content_bytes + ?
WHERE source_authority = ? AND snapshot_id = ? AND next_cursor = ? AND complete = 0 AND rolling_digest = ?
  AND object_count + ? <= ? AND metadata_bytes + ? <= ? AND content_bytes + ? <= ?`,
		page.Next, boolInt(newComplete), nextDigest[:], lastAffected, lastTenant, lastLogical,
		len(page.AffectedKeys), len(page.Roots), len(page.Bindings), len(page.Objects), pageBytes, pageContentBytes, string(identity.Authority),
		identity.Snapshot, page.Cursor, rolling, len(page.Objects), SourceSnapshotTotalObjectLimit,
		pageBytes, SourceSnapshotTotalByteLimit, pageContentBytes, SourceSnapshotContentByteLimit)
	if err != nil {
		return SourceSnapshotStageRef{}, mapConstraint(err)
	}
	if changed, _ := resultSQL.RowsAffected(); changed != 1 {
		return SourceSnapshotStageRef{}, fmt.Errorf("%w: source snapshot publication exceeds cumulative quota or lost ownership", ErrInvalidObject)
	}
	if err := tx.Commit(); err != nil {
		return SourceSnapshotStageRef{}, fmt.Errorf("catalog: commit source snapshot append: %w", err)
	}
	return SourceSnapshotStageRef{
		Authority: identity.Authority, Snapshot: identity.Snapshot, FenceDigest: identity.FenceDigest,
		Digest: nextDigest, Operation: identity.Change.OperationID, Revision: identity.Change.SourceRevision,
	}, nil
}

func (c *Catalog) releaseReplayedSnapshotContent(
	ctx context.Context,
	tx *sql.Tx,
	operation causal.OperationID,
	objects []SourceSnapshotProjection,
) error {
	seen := make(map[ContentRef]struct{})
	for _, projection := range objects {
		if projection.Object.Kind != KindFile {
			continue
		}
		ref := projection.Object.Content
		if _, duplicate := seen[ref]; duplicate {
			continue
		}
		seen[ref] = struct{}{}
		var owner, sourceOperation, hash []byte
		var size int64
		var published int
		if err := tx.QueryRowContext(ctx, `
SELECT owner_id, source_operation_id, published, hash, size
FROM content_stages WHERE stage_id = ?`, ref.Stage[:]).
			Scan(&owner, &sourceOperation, &published, &hash, &size); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: replayed snapshot content disappeared", ErrInvalidTransition)
			}
			return fmt.Errorf("catalog: inspect replayed snapshot content: %w", err)
		}
		if !bytes.Equal(owner, c.owner[:]) || published != 1 || !bytes.Equal(hash, ref.Hash[:]) || size != ref.Size {
			return fmt.Errorf("%w: replayed snapshot content ownership changed", ErrInvalidTransition)
		}
		switch {
		case sourceOperation == nil:
			if _, err := tx.ExecContext(ctx, `DELETE FROM content_stages WHERE stage_id = ?`, ref.Stage[:]); err != nil {
				return fmt.Errorf("catalog: release replayed snapshot content: %w", err)
			}
		case bytes.Equal(sourceOperation, operation[:]):
			// The caller retried the original page after ownership transferred.
		default:
			return fmt.Errorf("%w: replayed snapshot content belongs to another operation", ErrInvalidTransition)
		}
	}
	return nil
}

func validateSourceSnapshotPage(identity SourceSnapshotIdentity, page SourceSnapshotPublicationPage) ([32]byte, int, error) {
	if identity.Authority == "" || identity.AuthorityGeneration == 0 ||
		identity.Snapshot == "" || identity.Change.SourceAuthority != identity.Authority ||
		len(page.Cursor) > sourceSnapshotCursorLimit || len(page.Next) > sourceSnapshotCursorLimit ||
		(page.Next != "" && page.Next == page.Cursor) ||
		len(page.AffectedKeys) > SourceSnapshotPageLimit || len(page.Roots) > SourceSnapshotPageLimit ||
		len(page.Bindings) > SourceSnapshotPageLimit || len(page.Objects) > SourceSnapshotPageObjectLimit {
		return [32]byte{}, 0, fmt.Errorf("%w: invalid source snapshot page bounds", ErrInvalidObject)
	}
	if len(page.AffectedKeys)+len(page.Roots)+len(page.Bindings)+len(page.Objects) == 0 && page.Next != "" {
		return [32]byte{}, 0, fmt.Errorf("%w: empty source snapshot page did not terminate", ErrInvalidObject)
	}
	for index, key := range page.AffectedKeys {
		if key == "" || (index > 0 && page.AffectedKeys[index-1] >= key) {
			return [32]byte{}, 0, fmt.Errorf("%w: snapshot affected keys are not sorted and unique", ErrInvalidObject)
		}
	}
	for index, root := range page.Roots {
		if root.Tenant == "" || root.Generation == 0 || root.LogicalID == "" || !validSourceKey(root.RootKey) ||
			(index > 0 && page.Roots[index-1].Tenant >= root.Tenant) {
			return [32]byte{}, 0, fmt.Errorf("%w: snapshot roots are not sorted and complete", ErrInvalidObject)
		}
	}
	inputs := 0
	for index, binding := range page.Bindings {
		if binding.LogicalID == "" || !validSourceKey(binding.SourceKey) || len(binding.Inputs) == 0 ||
			(index > 0 && page.Bindings[index-1].LogicalID >= binding.LogicalID) {
			return [32]byte{}, 0, fmt.Errorf("%w: snapshot bindings are not sorted and complete", ErrInvalidObject)
		}
		inputs += len(binding.Inputs)
		for inputIndex, input := range binding.Inputs {
			if input.RootID == "" || input.Relative == "" || (inputIndex > 0 &&
				(binding.Inputs[inputIndex-1].RootID > input.RootID ||
					(binding.Inputs[inputIndex-1].RootID == input.RootID && binding.Inputs[inputIndex-1].Relative >= input.Relative))) {
				return [32]byte{}, 0, fmt.Errorf("%w: snapshot inputs are not sorted and unique", ErrInvalidObject)
			}
		}
	}
	if inputs > SourceSnapshotPageInputLimit {
		return [32]byte{}, 0, fmt.Errorf("%w: snapshot page has too many inputs", ErrInvalidObject)
	}
	for _, projection := range page.Objects {
		if projection.Tenant == "" || projection.Generation == 0 || projection.LogicalID == "" {
			return [32]byte{}, 0, fmt.Errorf("%w: incomplete snapshot projection", ErrInvalidObject)
		}
		if err := validateSourceObject(projection.Object); err != nil {
			return [32]byte{}, 0, err
		}
	}
	payload, err := json.Marshal(page)
	if err != nil {
		return [32]byte{}, 0, err
	}
	canonical := page
	canonical.AffectedKeys = slices.Clone(page.AffectedKeys)
	canonical.Roots = slices.Clone(page.Roots)
	canonical.Bindings = slices.Clone(page.Bindings)
	canonical.Objects = slices.Clone(page.Objects)
	for index := range canonical.Bindings {
		canonical.Bindings[index].Inputs = slices.Clone(page.Bindings[index].Inputs)
	}
	for index := range canonical.Objects {
		canonical.Objects[index].Object.Content.Stage = StageID{}
	}
	canonicalPayload, err := json.Marshal(canonical)
	if err != nil {
		return [32]byte{}, 0, err
	}
	return sha256.Sum256(canonicalPayload), len(payload), nil
}

func sourceSnapshotInitialDigest(identity SourceSnapshotIdentity) ([32]byte, error) {
	payload, err := json.Marshal(identity)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(payload), nil
}

func currentSourceSnapshotFenceProof(
	ctx context.Context,
	query *sql.Tx,
	authority causal.SourceAuthorityID,
) (sourceSnapshotFenceProof, error) {
	proof := sourceSnapshotFenceProof{Authority: authority}
	var rootDigest, fleetDigest []byte
	if err := query.QueryRowContext(ctx, `
SELECT fleet_generation, last_received_sequence, root_set_digest, fleet_digest
FROM source_observer_streams WHERE source_authority = ?`, string(authority)).
		Scan(&proof.AuthorityGeneration, &proof.Inbox, &rootDigest, &fleetDigest); err != nil {
		return sourceSnapshotFenceProof{}, err
	}
	if len(rootDigest) != sha256.Size || len(fleetDigest) != sha256.Size {
		return sourceSnapshotFenceProof{}, ErrIntegrity
	}
	copy(proof.RootDigest[:], rootDigest)
	copy(proof.FleetDigest[:], fleetDigest)
	rows, err := query.QueryContext(ctx, `
SELECT stream_identity, native_event_id, root_epoch
FROM source_observer_checkpoints WHERE source_authority = ? ORDER BY stream_identity`, string(authority))
	if err != nil {
		return sourceSnapshotFenceProof{}, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var checkpoint sourceSnapshotFenceCheckpoint
		if err := rows.Scan(&checkpoint.Identity, &checkpoint.Cursor, &checkpoint.RootEpoch); err != nil {
			return sourceSnapshotFenceProof{}, err
		}
		proof.Streams = append(proof.Streams, checkpoint)
	}
	if err := rows.Err(); err != nil {
		return sourceSnapshotFenceProof{}, err
	}
	if len(proof.Streams) == 0 {
		return sourceSnapshotFenceProof{}, ErrIntegrity
	}
	return proof, nil
}

func stagedSourceSnapshotFenceDigest(
	ctx context.Context,
	query *sql.Tx,
	authority causal.SourceAuthorityID,
	snapshot string,
) ([32]byte, error) {
	proof := sourceSnapshotFenceProof{Authority: authority}
	var rootDigest, fleetDigest []byte
	if err := query.QueryRowContext(ctx, `
SELECT fence_authority_generation, fence_inbox, fence_root_digest, fence_fleet_digest
FROM source_snapshot_publications WHERE source_authority = ? AND snapshot_id = ?`,
		string(authority), snapshot).Scan(&proof.AuthorityGeneration, &proof.Inbox, &rootDigest, &fleetDigest); err != nil {
		return [32]byte{}, err
	}
	if len(rootDigest) != sha256.Size || len(fleetDigest) != sha256.Size {
		return [32]byte{}, ErrIntegrity
	}
	copy(proof.RootDigest[:], rootDigest)
	copy(proof.FleetDigest[:], fleetDigest)
	rows, err := query.QueryContext(ctx, `
SELECT stream_identity, native_event_id, root_epoch
FROM source_snapshot_fence_checkpoints
WHERE source_authority = ? AND snapshot_id = ? ORDER BY stream_identity`, string(authority), snapshot)
	if err != nil {
		return [32]byte{}, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var checkpoint sourceSnapshotFenceCheckpoint
		if err := rows.Scan(&checkpoint.Identity, &checkpoint.Cursor, &checkpoint.RootEpoch); err != nil {
			return [32]byte{}, err
		}
		proof.Streams = append(proof.Streams, checkpoint)
	}
	if err := rows.Err(); err != nil {
		return [32]byte{}, err
	}
	if len(proof.Streams) == 0 {
		return [32]byte{}, ErrIntegrity
	}
	return digestSourceSnapshotFence(proof)
}

func digestSourceSnapshotFence(proof sourceSnapshotFenceProof) ([32]byte, error) {
	payload, err := json.Marshal(proof)
	if err != nil {
		return [32]byte{}, fmt.Errorf("catalog: encode source snapshot fence: %w", err)
	}
	return sha256.Sum256(payload), nil
}

func sourceSnapshotRollingDigest(prior, page [32]byte) [32]byte {
	var payload [sha256.Size*2 + 8]byte
	copy(payload[:sha256.Size], prior[:])
	copy(payload[sha256.Size:sha256.Size*2], page[:])
	binary.BigEndian.PutUint64(payload[sha256.Size*2:], uint64(len(payload)))
	return sha256.Sum256(payload[:])
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (c *Catalog) settleSourceSnapshotTx(
	ctx context.Context,
	tx *sql.Tx,
	settlement SourceSnapshotSettlement,
) error {
	fence := settlement.Fence
	ref := settlement.Snapshot
	if fence.Authority == "" || fence.Stream == "" || fence.RootEpoch == "" ||
		fence.Operation == (causal.OperationID{}) || ref.Authority != fence.Authority ||
		ref.Snapshot == "" || ref.Operation != fence.Operation || ref.Digest == ([32]byte{}) ||
		ref.FenceDigest == ([32]byte{}) || ref.Revision == 0 {
		return fmt.Errorf("%w: invalid source snapshot settlement", ErrInvalidObject)
	}
	stream, found, err := readSourceObserverStream(ctx, tx, fence.Authority)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	if stream.Stream != fence.Stream || stream.RootEpoch != fence.RootEpoch ||
		fence.Through < stream.LastApplied {
		return ErrSourceObserverConflict
	}
	operation, found, err := readSourceOperation(ctx, tx, ref.Operation)
	if err != nil {
		return err
	}
	if !found || operation.digest != ref.Digest || !snapshotSourceResultMatchesRef(operation.result, ref) {
		return ErrSourceObserverConflict
	}
	var staged string
	err = tx.QueryRowContext(ctx, `
SELECT snapshot_id FROM source_snapshot_sessions WHERE source_authority = ?`,
		string(fence.Authority)).Scan(&staged)
	if errors.Is(err, sql.ErrNoRows) {
		var snapshotID string
		var appliedOperation, appliedDigest, appliedFence []byte
		if stream.LastApplied != fence.Through {
			return ErrSourceObserverConflict
		}
		if err := tx.QueryRowContext(ctx, `
SELECT applied_snapshot_id, applied_snapshot_operation, applied_snapshot_digest, applied_snapshot_fence
FROM source_observer_streams WHERE source_authority = ?`, string(fence.Authority)).
			Scan(&snapshotID, &appliedOperation, &appliedDigest, &appliedFence); err != nil {
			return err
		}
		if snapshotID != ref.Snapshot || !bytes.Equal(appliedOperation, ref.Operation[:]) ||
			!bytes.Equal(appliedDigest, ref.Digest[:]) || !bytes.Equal(appliedFence, ref.FenceDigest[:]) {
			return ErrSourceObserverConflict
		}
		return nil
	}
	if err != nil {
		return err
	}
	if staged != ref.Snapshot {
		return ErrSourceObserverConflict
	}
	var stagedOperation, rolling, stagedFence, requestHash []byte
	var revision uint64
	var complete int
	if err := tx.QueryRowContext(ctx, `
SELECT publication.operation_id, publication.source_revision, publication.fence_digest,
       publication.rolling_digest, publication.complete, operation.request_hash
FROM source_snapshot_publications publication
JOIN source_operations operation ON operation.operation_id = publication.operation_id
WHERE publication.source_authority = ? AND publication.snapshot_id = ?`,
		string(fence.Authority), ref.Snapshot).
		Scan(&stagedOperation, &revision, &stagedFence, &rolling, &complete, &requestHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSourceObserverConflict
		}
		return err
	}
	if complete != 1 || revision != uint64(ref.Revision) || !bytes.Equal(stagedOperation, ref.Operation[:]) ||
		!bytes.Equal(stagedFence, ref.FenceDigest[:]) || !bytes.Equal(rolling, ref.Digest[:]) ||
		!bytes.Equal(requestHash, ref.Digest[:]) {
		return ErrSourceObserverConflict
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_physical_index WHERE source_authority = ?`, string(fence.Authority)); err != nil {
		return fmt.Errorf("catalog: replace source physical index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_physical_index(
    source_authority, root_id, relative_path, file_identity, physical_kind,
    metadata_fingerprint, content_fingerprint, payload
)
SELECT source_authority, root_id, relative_path, file_identity, physical_kind,
       metadata_fingerprint, content_fingerprint, payload
FROM source_snapshot_stages WHERE source_authority = ? AND snapshot_id = ?`,
		string(fence.Authority), ref.Snapshot); err != nil {
		return mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_physical_logical(source_authority, logical_id, root_id, relative_path)
SELECT source_authority, logical_id, root_id, relative_path
FROM source_snapshot_logical WHERE source_authority = ? AND snapshot_id = ?`,
		string(fence.Authority), ref.Snapshot); err != nil {
		return mapConstraint(err)
	}
	if settlement.MismatchAllActive {
		if _, err := tx.ExecContext(ctx, `
DELETE FROM source_mutation_expectations
WHERE source_authority = ? AND state = ?`,
			string(fence.Authority), SourceMutationExpectationArmed); err != nil {
			return fmt.Errorf("catalog: settle all mismatched source mutations: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE source_mutation_expectations SET state = ?
WHERE source_authority = ? AND state = ?`,
			SourceMutationExpectationRepairPublished, string(fence.Authority),
			SourceMutationExpectationRepairRequired); err != nil {
			return fmt.Errorf("catalog: publish all source mutation repairs: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_observer_inbox
WHERE source_authority = ? AND sequence <= ?
  AND NOT EXISTS (
      SELECT 1 FROM source_mutation_expectations WHERE source_authority = ?
  )`, string(fence.Authority), fence.Through, string(fence.Authority)); err != nil {
		return fmt.Errorf("catalog: settle source observer inbox: %w", err)
	}
	received := max(stream.LastReceived, fence.Through)
	if _, err := tx.ExecContext(ctx, `
UPDATE source_observer_streams SET
    applied_snapshot_id = ?, applied_snapshot_operation = ?, applied_snapshot_digest = ?, applied_snapshot_fence = ?,
    last_received_sequence = ?, last_applied_sequence = ?,
    state = CASE WHEN state = ? THEN state ELSE ? END,
    quarantine_detail = ''
WHERE source_authority = ?`,
		ref.Snapshot, ref.Operation[:], ref.Digest[:], ref.FenceDigest[:],
		received, fence.Through, uint8(SourceObserverStreamResetRequired),
		uint8(SourceObserverIncremental), string(fence.Authority)); err != nil {
		return fmt.Errorf("catalog: advance source snapshot observer fence: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_snapshot_stages WHERE source_authority = ? AND snapshot_id = ?`,
		string(fence.Authority), ref.Snapshot); err != nil {
		return fmt.Errorf("catalog: clear settled source snapshot stage: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_snapshot_sessions WHERE source_authority = ? AND snapshot_id = ?`,
		string(fence.Authority), ref.Snapshot); err != nil {
		return fmt.Errorf("catalog: clear settled source snapshot session: %w", err)
	}
	return nil
}

// PromoteSourceSnapshot atomically promotes one complete snapshot and settles its observer fence.
func (c *Catalog) PromoteSourceSnapshot(
	ctx context.Context,
	ref SourceSnapshotStageRef,
	settlement SourceSnapshotSettlement,
) (result SourceResult, err error) {
	if ref.Authority == "" || ref.Snapshot == "" || ref.Operation == (causal.OperationID{}) || ref.Revision == 0 {
		return SourceResult{}, fmt.Errorf("%w: incomplete source snapshot promotion", ErrInvalidObject)
	}
	if settlement.Fence.Authority != ref.Authority || settlement.Fence.Operation != ref.Operation ||
		settlement.Snapshot != ref {
		return SourceResult{}, fmt.Errorf("%w: source snapshot settlement does not match promotion", ErrInvalidObject)
	}
	existing, found, err := readSourceOperation(ctx, c.readDB, ref.Operation)
	if err != nil {
		return SourceResult{}, err
	}
	if found {
		if existing.digest != ref.Digest || !snapshotSourceResultMatchesRef(existing.result, ref) {
			return SourceResult{}, ErrMutationConflict
		}
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return SourceResult{}, fmt.Errorf("catalog: begin repeated source snapshot settlement: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		if err := c.settleSourceSnapshotTx(ctx, tx, settlement); err != nil {
			return SourceResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return SourceResult{}, fmt.Errorf("catalog: commit repeated source snapshot settlement: %w", err)
		}
		return existing.result, nil
	}
	if err := c.verifyStagedSnapshotContent(ctx, ref); err != nil {
		return SourceResult{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceResult{}, fmt.Errorf("catalog: begin source snapshot promotion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	existing, found, err = readSourceOperation(ctx, tx, ref.Operation)
	if err != nil {
		return SourceResult{}, err
	}
	if found {
		if existing.digest != ref.Digest || !snapshotSourceResultMatchesRef(existing.result, ref) {
			return SourceResult{}, ErrMutationConflict
		}
		if err := c.settleSourceSnapshotTx(ctx, tx, settlement); err != nil {
			return SourceResult{}, err
		}
		return existing.result, tx.Commit()
	}
	identity, err := readStagedSnapshotIdentity(ctx, tx, ref)
	if err != nil {
		return SourceResult{}, err
	}
	if err := validateSourceSnapshotWatermark(ctx, tx, identity.Change); err != nil {
		return SourceResult{}, err
	}
	if err := validateStagedSnapshotFleet(ctx, tx, ref); err != nil {
		return SourceResult{}, err
	}
	if err := promoteStagedSnapshotBindings(ctx, tx, ref); err != nil {
		return SourceResult{}, err
	}
	if err := c.promoteStagedSnapshotSetwise(ctx, tx, ref, identity.Change); err != nil {
		return SourceResult{}, err
	}
	if err := c.trip(sourceSnapshotAfterSetwisePromotion); err != nil {
		return SourceResult{}, err
	}
	result = SourceResult{
		Authority: ref.Authority, Revision: ref.Revision, ChangeID: identity.Change.ChangeID, Operation: ref.Operation,
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return SourceResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_operations(
    operation_id, change_id, source_authority, source_revision,
    predecessor_revision, mode, request_hash, result_json
) VALUES (?, ?, ?, ?, 0, ?, ?, ?)`, ref.Operation[:], identity.Change.ChangeID[:], string(ref.Authority),
		uint64(ref.Revision), uint8(SourceSnapshot), ref.Digest[:], encoded); err != nil {
		return SourceResult{}, mapConstraint(err)
	}
	if err := insertStagedSnapshotCommits(ctx, tx, ref); err != nil {
		return SourceResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_watermarks(source_authority, source_revision, change_id, operation_id)
VALUES (?, ?, ?, ?)
ON CONFLICT(source_authority) DO UPDATE SET
    source_revision = excluded.source_revision,
    change_id = excluded.change_id,
    operation_id = excluded.operation_id`, string(ref.Authority), uint64(ref.Revision),
		identity.Change.ChangeID[:], ref.Operation[:]); err != nil {
		return SourceResult{}, mapConstraint(err)
	}
	if err := upsertStagedSnapshotTargets(ctx, tx, ref); err != nil {
		return SourceResult{}, err
	}
	if err := seedSourceDriverSnapshotAnchor(ctx, tx, ref, identity); err != nil {
		return SourceResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_authority_bindings
SET effective_fingerprint = (
    SELECT b.effective_fingerprint FROM source_snapshot_bindings b
    WHERE b.source_authority = source_authority_bindings.source_authority
      AND b.snapshot_id = ? AND b.logical_id = source_authority_bindings.logical_id
)
WHERE source_authority = ? AND EXISTS (
    SELECT 1 FROM source_snapshot_bindings b
    WHERE b.source_authority = source_authority_bindings.source_authority
      AND b.snapshot_id = ? AND b.logical_id = source_authority_bindings.logical_id
)`, ref.Snapshot, string(ref.Authority), ref.Snapshot); err != nil {
		return SourceResult{}, mapConstraint(err)
	}
	if err := consumeStagedSnapshotContent(ctx, c, tx, ref); err != nil {
		return SourceResult{}, err
	}
	if err := c.settleSourceSnapshotTx(ctx, tx, settlement); err != nil {
		return SourceResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SourceResult{}, fmt.Errorf("catalog: commit source snapshot promotion: %w", err)
	}
	return result, nil
}

func validateSourceSnapshotWatermark(
	ctx context.Context,
	tx *sql.Tx,
	change causal.ChangeSet,
) error {
	var current uint64
	err := tx.QueryRowContext(ctx, `
SELECT source_revision FROM source_watermarks WHERE source_authority = ?`,
		string(change.SourceAuthority)).Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("catalog: read source snapshot watermark: %w", err)
	}
	if change.SourceRevision != causal.Revision(current)+1 {
		return ErrSourcePredecessor
	}
	return nil
}

func (c *Catalog) verifyStagedSnapshotContent(ctx context.Context, ref SourceSnapshotStageRef) error {
	after := make([]byte, len(StageID{}))
	for {
		rows, err := c.readDB.QueryContext(ctx, `
SELECT DISTINCT content_stage, content_hash, content_size
FROM source_snapshot_objects
WHERE source_authority = ? AND snapshot_id = ? AND content_stage IS NOT NULL AND content_stage > ?
ORDER BY content_stage LIMIT ?`, string(ref.Authority), ref.Snapshot, after, SourceSnapshotPageLimit)
		if err != nil {
			return fmt.Errorf("catalog: page staged snapshot content for verification: %w", err)
		}
		var refs []ContentRef
		for rows.Next() {
			var stage, hash []byte
			var size int64
			if err := rows.Scan(&stage, &hash, &size); err != nil {
				_ = rows.Close()
				return err
			}
			if len(stage) != len(StageID{}) || len(hash) != len(ContentHash{}) {
				_ = rows.Close()
				return ErrIntegrity
			}
			var value ContentRef
			copy(value.Stage[:], stage)
			copy(value.Hash[:], hash)
			value.Size = size
			refs = append(refs, value)
			after = slices.Clone(stage)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, value := range refs {
			if err := c.validateSourceContentRef(ctx, c.readDB, ref.Operation, KindFile, value); err != nil {
				return fmt.Errorf("catalog: verify staged snapshot content: %w", err)
			}
			if err := c.verifyContentBlob(ctx, value); err != nil {
				return fmt.Errorf("catalog: verify staged snapshot content blob: %w", err)
			}
		}
		if len(refs) < SourceSnapshotPageLimit {
			return nil
		}
	}
}

func snapshotSourceResultMatchesRef(result SourceResult, ref SourceSnapshotStageRef) bool {
	return result.Authority == ref.Authority && result.Revision == ref.Revision && result.Operation == ref.Operation &&
		result.ChangeID != (causal.ChangeID{})
}

func promoteStagedSnapshotBindings(ctx context.Context, tx *sql.Tx, ref SourceSnapshotStageRef) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_object_ids(source_authority, source_key, object_id)
SELECT source_authority, source_key, object_id FROM source_snapshot_bindings
WHERE source_authority = ? AND snapshot_id = ?
ON CONFLICT(source_authority, source_key) DO NOTHING`, string(ref.Authority), ref.Snapshot); err != nil {
		return mapConstraint(err)
	}
	var mismatched int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM source_snapshot_bindings staged
JOIN source_object_ids durable
  ON durable.source_authority = staged.source_authority AND durable.source_key = staged.source_key
WHERE staged.source_authority = ? AND staged.snapshot_id = ? AND durable.object_id <> staged.object_id`,
		string(ref.Authority), ref.Snapshot).Scan(&mismatched); err != nil {
		return err
	}
	if mismatched != 0 {
		return ErrMutationConflict
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_bindings(source_authority, logical_id, source_key, effective_fingerprint)
SELECT source_authority, logical_id, source_key, effective_fingerprint
FROM source_snapshot_bindings WHERE source_authority = ? AND snapshot_id = ?
ON CONFLICT(source_authority, logical_id) DO UPDATE SET
    effective_fingerprint = excluded.effective_fingerprint
WHERE source_authority_bindings.source_key = excluded.source_key`, string(ref.Authority), ref.Snapshot); err != nil {
		return mapConstraint(err)
	}
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM source_snapshot_bindings staged
JOIN source_authority_bindings durable
  ON durable.source_authority = staged.source_authority AND durable.logical_id = staged.logical_id
WHERE staged.source_authority = ? AND staged.snapshot_id = ? AND durable.source_key <> staged.source_key`,
		string(ref.Authority), ref.Snapshot).Scan(&mismatched); err != nil {
		return err
	}
	if mismatched != 0 {
		return ErrSourceObserverConflict
	}
	return nil
}

func readStagedSnapshotIdentity(ctx context.Context, tx *sql.Tx, ref SourceSnapshotStageRef) (SourceSnapshotIdentity, error) {
	var operation, change, fence, digest []byte
	var cause string
	var revision, authorityGeneration uint64
	var complete int
	var affected uint64
	err := tx.QueryRowContext(ctx, `
SELECT operation_id, change_id, source_revision, cause, fence_digest,
       fence_authority_generation, rolling_digest, complete, affected_count
FROM source_snapshot_publications WHERE source_authority = ? AND snapshot_id = ?`, string(ref.Authority), ref.Snapshot).
		Scan(&operation, &change, &revision, &cause, &fence, &authorityGeneration, &digest, &complete, &affected)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceSnapshotIdentity{}, ErrNotFound
	}
	if err != nil {
		return SourceSnapshotIdentity{}, err
	}
	if complete != 1 || affected == 0 || authorityGeneration == 0 ||
		!bytes.Equal(operation, ref.Operation[:]) || revision != uint64(ref.Revision) ||
		!bytes.Equal(fence, ref.FenceDigest[:]) || !bytes.Equal(digest, ref.Digest[:]) || len(change) != len(causal.ChangeID{}) {
		return SourceSnapshotIdentity{}, ErrSourceObserverConflict
	}
	derivedFence, err := stagedSourceSnapshotFenceDigest(ctx, tx, ref.Authority, ref.Snapshot)
	if err != nil {
		return SourceSnapshotIdentity{}, err
	}
	if derivedFence != ref.FenceDigest {
		return SourceSnapshotIdentity{}, ErrSourceObserverConflict
	}
	var changeID causal.ChangeID
	copy(changeID[:], change)
	if causal.Cause(cause) != causal.CauseExternalUnattributed && causal.Cause(cause) != causal.CauseBootstrap {
		return SourceSnapshotIdentity{}, ErrIntegrity
	}
	return SourceSnapshotIdentity{
		Authority: ref.Authority, AuthorityGeneration: causal.Generation(authorityGeneration),
		Snapshot: ref.Snapshot, FenceDigest: ref.FenceDigest,
		Change: causal.ChangeSet{
			SourceAuthority: ref.Authority, SourceRevision: ref.Revision, ChangeID: changeID,
			OperationID: ref.Operation, Cause: causal.Cause(cause),
		},
	}, nil
}

func validateStagedSnapshotFleet(ctx context.Context, tx *sql.Tx, ref SourceSnapshotStageRef) error {
	var invalid int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM (
    SELECT intent.tenant_id FROM tenant_intents intent
    JOIN tenant_generations generation
      ON generation.tenant_id = intent.tenant_id AND generation.generation = intent.target_generation
    LEFT JOIN source_snapshot_roots r
      ON r.source_authority = ? AND r.snapshot_id = ? AND r.tenant = intent.tenant_id
     AND r.generation = intent.target_generation
    WHERE generation.content_source_id = ? AND intent.state = ? AND r.tenant IS NULL
    UNION ALL
    SELECT r.tenant FROM source_snapshot_roots r
    LEFT JOIN tenant_intents intent ON intent.tenant_id = r.tenant AND intent.state = ?
    LEFT JOIN tenant_generations generation
      ON generation.tenant_id = intent.tenant_id AND generation.generation = intent.target_generation
    WHERE r.source_authority = ? AND r.snapshot_id = ?
      AND (intent.tenant_id IS NULL OR generation.content_source_id <> ? OR intent.target_generation <> r.generation)
)`, string(ref.Authority), ref.Snapshot, string(ref.Authority), uint8(TenantIntentPresent),
		uint8(TenantIntentPresent), string(ref.Authority), ref.Snapshot, string(ref.Authority)).Scan(&invalid); err != nil {
		return fmt.Errorf("catalog: validate staged snapshot fleet: %w", err)
	}
	if invalid != 0 {
		return fmt.Errorf("%w: staged source snapshot does not cover the exact fleet", ErrGenerationMismatch)
	}
	if err := validateStagedSnapshotNamespace(ctx, tx, ref); err != nil {
		return err
	}
	return nil
}

func validateStagedSnapshotNamespace(ctx context.Context, tx *sql.Tx, ref SourceSnapshotStageRef) error {
	var invalid int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM source_snapshot_objects child
LEFT JOIN source_snapshot_objects parent
  ON parent.source_authority = child.source_authority
 AND parent.snapshot_id = child.snapshot_id
 AND parent.tenant = child.tenant
 AND parent.source_key = child.parent_key
WHERE child.source_authority = ? AND child.snapshot_id = ? AND child.parent_key <> ''
  AND (parent.source_key IS NULL OR parent.object_kind <> ?)`,
		string(ref.Authority), ref.Snapshot, uint8(KindDirectory)).Scan(&invalid); err != nil {
		return fmt.Errorf("catalog: validate staged snapshot parents: %w", err)
	}
	if invalid != 0 {
		return fmt.Errorf("%w: staged snapshot parent is missing or not a directory", ErrInvalidObject)
	}
	var nested int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM source_snapshot_objects
    WHERE source_authority = ? AND snapshot_id = ? AND parent_key <> ''
)`, string(ref.Authority), ref.Snapshot).Scan(&nested); err != nil {
		return fmt.Errorf("catalog: inspect staged snapshot namespace depth: %w", err)
	}
	if nested == 0 {
		return nil
	}
	if err := tx.QueryRowContext(ctx, `
WITH RECURSIVE reachable(tenant, source_key) AS (
    SELECT tenant, source_key FROM source_snapshot_objects
    WHERE source_authority = ? AND snapshot_id = ? AND parent_key = ''
    UNION ALL
    SELECT child.tenant, child.source_key
    FROM source_snapshot_objects child
    JOIN reachable parent
      ON parent.tenant = child.tenant AND parent.source_key = child.parent_key
    WHERE child.source_authority = ? AND child.snapshot_id = ?
)
SELECT
    (SELECT COUNT(*) FROM source_snapshot_objects
     WHERE source_authority = ? AND snapshot_id = ?)
  - (SELECT COUNT(*) FROM reachable)`,
		string(ref.Authority), ref.Snapshot, string(ref.Authority), ref.Snapshot,
		string(ref.Authority), ref.Snapshot).Scan(&invalid); err != nil {
		return fmt.Errorf("catalog: validate staged snapshot graph: %w", err)
	}
	if invalid != 0 {
		return fmt.Errorf("%w: staged snapshot namespace contains a cycle", ErrInvalidObject)
	}
	return nil
}

func insertStagedSnapshotCommits(
	ctx context.Context,
	tx *sql.Tx,
	ref SourceSnapshotStageRef,
) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_commits(
    catalog_operation_id, source_operation_id, tenant, generation, catalog_revision,
    catalog_fingerprint, file_provider_fingerprint
)
SELECT catalog_operation_id, ?, tenant, generation, catalog_revision,
       catalog_fingerprint, file_provider_fingerprint
FROM source_snapshot_roots
WHERE source_authority = ? AND snapshot_id = ? AND catalog_revision > 0
ORDER BY tenant`, ref.Operation[:], string(ref.Authority), ref.Snapshot); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func upsertStagedSnapshotTargets(ctx context.Context, tx *sql.Tx, ref SourceSnapshotStageRef) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_tenant_targets(
    source_authority, tenant, source_revision, change_id, source_operation_id,
    catalog_revision, catalog_fingerprint, file_provider_fingerprint
)
SELECT root.source_authority, root.tenant, publication.source_revision, publication.change_id,
       publication.operation_id, tenant.head, root.catalog_fingerprint, root.file_provider_fingerprint
FROM source_snapshot_roots root
JOIN source_snapshot_publications publication
  ON publication.source_authority = root.source_authority AND publication.snapshot_id = root.snapshot_id
JOIN tenants tenant ON tenant.tenant = root.tenant
WHERE root.source_authority = ? AND root.snapshot_id = ?
ORDER BY root.tenant
ON CONFLICT(source_authority, tenant) DO UPDATE SET
    source_revision = excluded.source_revision,
    change_id = excluded.change_id,
    source_operation_id = excluded.source_operation_id,
    catalog_revision = excluded.catalog_revision,
    catalog_fingerprint = excluded.catalog_fingerprint,
    file_provider_fingerprint = excluded.file_provider_fingerprint`,
		string(ref.Authority), ref.Snapshot); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func consumeStagedSnapshotContent(ctx context.Context, c *Catalog, tx *sql.Tx, ref SourceSnapshotStageRef) error {
	after := make([]byte, len(StageID{}))
	for {
		rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT content_stage, content_hash, content_size
FROM source_snapshot_objects
WHERE source_authority = ? AND snapshot_id = ? AND content_stage IS NOT NULL AND content_stage > ?
ORDER BY content_stage LIMIT ?`, string(ref.Authority), ref.Snapshot, after, SourceSnapshotPageLimit)
		if err != nil {
			return err
		}
		var refs []ContentRef
		for rows.Next() {
			var stage, hash []byte
			var size int64
			if err := rows.Scan(&stage, &hash, &size); err != nil {
				_ = rows.Close()
				return err
			}
			if len(stage) != len(StageID{}) || len(hash) != len(ContentHash{}) {
				_ = rows.Close()
				return ErrIntegrity
			}
			var value ContentRef
			copy(value.Stage[:], stage)
			copy(value.Hash[:], hash)
			value.Size = size
			refs = append(refs, value)
			after = slices.Clone(stage)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, value := range refs {
			if err := c.consumeSourceContent(ctx, tx, ref.Operation, value); err != nil {
				return err
			}
		}
		if len(refs) < SourceSnapshotPageLimit {
			return nil
		}
	}
}
