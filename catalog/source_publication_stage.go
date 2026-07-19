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
	SourcePublicationStagePageItemLimit = 256
	SourcePublicationStagePageByteLimit = 1 << 20
	SourcePublicationStageItemLimit     = 10_000_000
	SourcePublicationStageByteLimit     = 2 << 30
)

// SourcePublicationStageIdentity fences one durable incremental publication chain.
type SourcePublicationStageIdentity struct {
	Authority         causal.SourceAuthorityID
	FleetOwner        SourceAuthorityFleetOwnerID
	FleetGeneration   causal.Generation
	DriverID          string
	DeclarationDigest [sha256.Size]byte
	Operation         causal.OperationID
	Stream            string
	RootEpoch         string
	Through           uint64
	Predecessor       causal.Revision
}

// SourcePublicationStageHeader begins one revision in a staged predecessor chain.
type SourcePublicationStageHeader struct {
	Mode        SourceMode
	Predecessor causal.Revision
	Change      causal.ChangeSet
}

// SourcePublicationAffected records one affected key for one staged revision.
type SourcePublicationAffected struct {
	Revision causal.Revision
	Key      causal.LogicalKey
}

// SourcePublicationStagePage is one bounded, exact-replay append.
type SourcePublicationStagePage struct {
	Sequence            uint64
	Header              *SourcePublicationStageHeader
	Affected            []SourcePublicationAffected
	Index               []SourcePhysicalIndexRecord
	Deletes             []SourceIndexLocator
	Bindings            []SourceAuthorityBindingRecord
	MatchedMutations    []MutationID
	MismatchedMutations []MutationID
	Complete            bool
}

// SourcePublicationStageRef proves the complete durable staged chain.
type SourcePublicationStageRef struct {
	Authority         causal.SourceAuthorityID
	FleetOwner        SourceAuthorityFleetOwnerID
	FleetGeneration   causal.Generation
	DriverID          string
	DeclarationDigest [sha256.Size]byte
	Operation         causal.OperationID
	Through           uint64
	Revision          causal.Revision
	Sequence          uint64
	Items             uint64
	Bytes             uint64
	Digest            [32]byte
}

// SourcePublicationStageResult proves atomic publication and observer settlement.
type SourcePublicationStageResult struct {
	Authority         causal.SourceAuthorityID
	FleetOwner        SourceAuthorityFleetOwnerID
	FleetGeneration   causal.Generation
	DriverID          string
	DeclarationDigest [sha256.Size]byte
	Operation         causal.OperationID
	First             causal.Revision
	Last              causal.Revision
	Count             uint64
	Digest            [32]byte
}

func validateSourcePublicationStageIdentity(identity SourcePublicationStageIdentity) error {
	if identity.Authority == "" || ValidateSourceAuthorityFleetOwnerID(identity.FleetOwner) != nil ||
		identity.FleetGeneration == 0 || ValidateSourceDriverID(identity.DriverID) != nil ||
		identity.DeclarationDigest == ([sha256.Size]byte{}) || identity.Operation == (causal.OperationID{}) ||
		identity.Stream == "" || identity.RootEpoch == "" {
		return fmt.Errorf("%w: incomplete source publication stage identity", ErrInvalidObject)
	}
	return nil
}

func validateSourcePublicationStagePage(page SourcePublicationStagePage) (int, error) {
	items := len(page.Affected) + len(page.Index) + len(page.Deletes) + len(page.Bindings) +
		len(page.MatchedMutations) + len(page.MismatchedMutations)
	if page.Header != nil {
		items++
		if (page.Header.Mode != SourceSnapshot && page.Header.Mode != SourceDelta) ||
			len(page.Header.Change.AffectedKeys) != 0 ||
			page.Header.Change.SourceRevision == 0 ||
			page.Header.Change.SourceRevision != page.Header.Predecessor+1 ||
			page.Header.Change.OperationID == (causal.OperationID{}) {
			return 0, fmt.Errorf("%w: invalid staged source publication header", ErrInvalidObject)
		}
		change := page.Header.Change
		change.AffectedKeys = []causal.LogicalKey{"staged"}
		if err := validateSourceChange(change); err != nil {
			return 0, err
		}
	}
	for index, affected := range page.Affected {
		if affected.Revision == 0 || affected.Key == "" ||
			(index > 0 && (page.Affected[index-1].Revision > affected.Revision ||
				(page.Affected[index-1].Revision == affected.Revision && page.Affected[index-1].Key >= affected.Key))) {
			return 0, fmt.Errorf("%w: staged affected keys are not sorted and unique", ErrInvalidObject)
		}
	}
	for index := range page.Index {
		if len(page.Index[index].FileIdentity) == 0 ||
			len(page.Index[index].FileIdentity) > SourcePhysicalIdentityByteLimit {
			return 0, fmt.Errorf("%w: staged source file identity limit exceeded", ErrInvalidObject)
		}
		if index > 0 && compareSourceIndexRecord(page.Index[index-1], page.Index[index]) >= 0 {
			return 0, fmt.Errorf("%w: staged source index is not sorted and unique", ErrInvalidObject)
		}
		for logicalIndex, logical := range page.Index[index].Logical {
			if logical == "" || (logicalIndex > 0 && page.Index[index].Logical[logicalIndex-1] >= logical) {
				return 0, fmt.Errorf("%w: staged source logical bindings are not sorted and unique", ErrInvalidObject)
			}
		}
	}
	for index := range page.Deletes {
		if page.Deletes[index].RootID == "" || page.Deletes[index].Relative == "" ||
			(index > 0 && compareSourceIndexLocator(page.Deletes[index-1], page.Deletes[index]) >= 0) {
			return 0, fmt.Errorf("%w: staged source deletes are not sorted and unique", ErrInvalidObject)
		}
	}
	for index, binding := range page.Bindings {
		if binding.Authority == "" || binding.LogicalID == "" || !validSourceKey(binding.SourceKey) ||
			(index > 0 && page.Bindings[index-1].LogicalID >= binding.LogicalID) {
			return 0, fmt.Errorf("%w: staged source bindings are not sorted and unique", ErrInvalidObject)
		}
	}
	if !sortedMutationIDs(page.MatchedMutations) || !sortedMutationIDs(page.MismatchedMutations) {
		return 0, fmt.Errorf("%w: staged mutation settlements are not sorted and unique", ErrInvalidObject)
	}
	for matched, mismatched := 0, 0; matched < len(page.MatchedMutations) && mismatched < len(page.MismatchedMutations); {
		switch bytes.Compare(page.MatchedMutations[matched][:], page.MismatchedMutations[mismatched][:]) {
		case 0:
			return 0, fmt.Errorf("%w: staged mutation settlement overlaps", ErrInvalidObject)
		case -1:
			matched++
		default:
			mismatched++
		}
	}
	if items == 0 || items > SourcePublicationStagePageItemLimit {
		return 0, fmt.Errorf("%w: staged publication page item limit exceeded", ErrInvalidObject)
	}
	if sourcePublicationStagePageRawBytes(page) > SourcePublicationStagePageByteLimit {
		return 0, fmt.Errorf("%w: staged publication page raw-byte limit exceeded", ErrInvalidObject)
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		return 0, fmt.Errorf("catalog: encode staged publication page: %w", err)
	}
	if len(encoded) > SourcePublicationStagePageByteLimit {
		return 0, fmt.Errorf("%w: staged publication page byte limit exceeded", ErrInvalidObject)
	}
	return len(encoded), nil
}

func sourcePublicationStagePageRawBytes(page SourcePublicationStagePage) int {
	total := 0
	if page.Header != nil {
		total += len(page.Header.Change.SourceAuthority) + len(page.Header.Change.Cause) +
			len(page.Header.Change.Origin) + len(page.Header.Change.OperationID) +
			len(page.Header.Change.ChangeID)
	}
	for _, affected := range page.Affected {
		total += len(affected.Key)
	}
	for _, record := range page.Index {
		total += len(record.Authority) + len(record.RootID) + len(record.Relative) +
			len(record.FileIdentity) + len(record.MetadataFingerprint) +
			len(record.ContentFingerprint) + len(record.Payload)
		for _, logical := range record.Logical {
			total += len(logical)
		}
	}
	for _, locator := range page.Deletes {
		total += len(locator.RootID) + len(locator.Relative)
	}
	for _, binding := range page.Bindings {
		total += len(binding.Authority) + len(binding.LogicalID) + len(binding.SourceKey) +
			len(binding.Fingerprint)
	}
	total += len(page.MatchedMutations)*len(MutationID{}) +
		len(page.MismatchedMutations)*len(MutationID{})
	return total
}

func compareSourceIndexLocator(left, right SourceIndexLocator) int {
	if value := bytes.Compare([]byte(left.RootID), []byte(right.RootID)); value != 0 {
		return value
	}
	return bytes.Compare([]byte(left.Relative), []byte(right.Relative))
}

func sortedMutationIDs(values []MutationID) bool {
	for index, value := range values {
		if value == (MutationID{}) || (index > 0 && bytes.Compare(values[index-1][:], value[:]) >= 0) {
			return false
		}
	}
	return true
}

// PendingSourcePublicationStage returns the one authority-owned durable stage.
func (c *Catalog) PendingSourcePublicationStage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
) (*SourcePublicationStageRef, error) {
	if authority == "" {
		return nil, fmt.Errorf("%w: empty staged publication authority", ErrInvalidObject)
	}
	ref, found, err := readSourcePublicationStage(ctx, c.readDB, authority)
	if err != nil || !found {
		return nil, err
	}
	return &ref, nil
}

// BeginSourcePublicationStage begins one exact authority publication chain.
func (c *Catalog) BeginSourcePublicationStage(
	ctx context.Context,
	identity SourcePublicationStageIdentity,
) error {
	if err := validateSourcePublicationStageIdentity(identity); err != nil {
		return err
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return fmt.Errorf("catalog: encode source publication stage identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source publication stage: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stream, found, err := readSourceObserverStream(ctx, tx, identity.Authority)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	if stream.Stream != identity.Stream || stream.RootEpoch != identity.RootEpoch ||
		identity.Through > stream.LastReceived {
		return ErrSourceObserverConflict
	}
	var driverID string
	var declaration []byte
	if err := tx.QueryRowContext(ctx, `
SELECT member.driver_id, member.declaration_digest
FROM source_authority_fleet_heads head
JOIN source_authority_fleet_members member
  ON member.owner_id = head.owner_id AND member.generation = head.generation
WHERE head.owner_id = ? AND head.generation = ? AND member.source_authority = ?`,
		string(identity.FleetOwner), uint64(identity.FleetGeneration), string(identity.Authority)).
		Scan(&driverID, &declaration); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSourceObserverConflict
		}
		return err
	}
	if driverID != identity.DriverID || !bytes.Equal(declaration, identity.DeclarationDigest[:]) {
		return ErrSourceObserverConflict
	}
	var snapshot int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_snapshot_publications WHERE source_authority = ?
)`, string(identity.Authority)).Scan(&snapshot); err != nil {
		return err
	}
	if snapshot != 0 {
		return ErrSourceObserverConflict
	}
	current, found, err := readSourcePublicationStage(ctx, tx, identity.Authority)
	if err != nil {
		return err
	}
	if found {
		if current.Operation != identity.Operation || current.Through != identity.Through ||
			current.Revision != identity.Predecessor || current.Sequence != 0 ||
			current.Items != 0 || current.Bytes != 0 || current.Digest != digest {
			return ErrMutationConflict
		}
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stages(
    source_authority, stage_operation_id, fleet_owner_id, authority_generation,
    driver_id, declaration_digest, stream_identity, root_epoch,
    through_sequence, predecessor_revision, last_revision, next_sequence,
    item_count, byte_count, complete, aborting, identity_digest, rolling_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, ?, ?)`,
		string(identity.Authority), identity.Operation[:], string(identity.FleetOwner),
		uint64(identity.FleetGeneration), identity.DriverID, identity.DeclarationDigest[:],
		identity.Stream, identity.RootEpoch,
		identity.Through, uint64(identity.Predecessor), uint64(identity.Predecessor), digest[:], digest[:]); err != nil {
		return mapConstraint(err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit source publication stage begin: %w", err)
	}
	return nil
}

// AppendSourcePublicationStage durably appends one bounded exact-replay page.
func (c *Catalog) AppendSourcePublicationStage(
	ctx context.Context,
	identity SourcePublicationStageIdentity,
	page SourcePublicationStagePage,
) (SourcePublicationStageRef, error) {
	if err := validateSourcePublicationStageIdentity(identity); err != nil {
		return SourcePublicationStageRef{}, err
	}
	return c.appendSourcePublicationStage(ctx, identity, page)
}

func (c *Catalog) appendSourcePublicationStage(
	ctx context.Context,
	identity SourcePublicationStageIdentity,
	page SourcePublicationStagePage,
) (SourcePublicationStageRef, error) {
	pageBytes, err := validateSourcePublicationStagePage(page)
	if err != nil {
		return SourcePublicationStageRef{}, err
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		return SourcePublicationStageRef{}, fmt.Errorf("catalog: encode source publication stage page: %w", err)
	}
	pageDigest := sha256.Sum256(encoded)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourcePublicationStageRef{}, fmt.Errorf("catalog: begin source publication stage append: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	ref, found, err := readSourcePublicationStage(ctx, tx, identity.Authority)
	if err != nil {
		return SourcePublicationStageRef{}, err
	}
	if !found {
		return SourcePublicationStageRef{}, ErrNotFound
	}
	if err := validateStoredSourcePublicationStageIdentity(ctx, tx, identity); err != nil {
		return SourcePublicationStageRef{}, err
	}
	if ref.Operation != identity.Operation || ref.Through != identity.Through {
		return SourcePublicationStageRef{}, ErrMutationConflict
	}
	if page.Sequence < ref.Sequence {
		var stored, rolling []byte
		var revision, items, byteCount uint64
		if err := tx.QueryRowContext(ctx, `
SELECT page_digest, rolling_digest, cumulative_revision, cumulative_item_count, cumulative_byte_count
FROM source_publication_stage_pages
WHERE source_authority = ? AND stage_operation_id = ? AND sequence = ?`,
			string(identity.Authority), identity.Operation[:], page.Sequence).
			Scan(&stored, &rolling, &revision, &items, &byteCount); err != nil {
			return SourcePublicationStageRef{}, err
		}
		if !bytes.Equal(stored, pageDigest[:]) || len(rolling) != sha256.Size {
			return SourcePublicationStageRef{}, ErrMutationConflict
		}
		if err := tx.Commit(); err != nil {
			return SourcePublicationStageRef{}, err
		}
		var digest [sha256.Size]byte
		copy(digest[:], rolling)
		return SourcePublicationStageRef{
			Authority: identity.Authority, FleetOwner: identity.FleetOwner,
			FleetGeneration: identity.FleetGeneration, DriverID: identity.DriverID,
			DeclarationDigest: identity.DeclarationDigest,
			Operation:         identity.Operation, Through: identity.Through,
			Revision: causal.Revision(revision), Sequence: page.Sequence + 1,
			Items: items, Bytes: byteCount, Digest: digest,
		}, nil
	}
	if page.Sequence != ref.Sequence {
		return SourcePublicationStageRef{}, ErrInvalidTransition
	}
	var kind, complete, aborting int
	if err := tx.QueryRowContext(ctx, `
SELECT stage_kind, complete, aborting FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&kind, &complete, &aborting); err != nil {
		return SourcePublicationStageRef{}, err
	}
	if kind != 1 {
		return SourcePublicationStageRef{}, ErrMutationConflict
	}
	if page.Header != nil && page.Header.Mode != SourceDelta {
		return SourcePublicationStageRef{}, ErrInvalidTransition
	}
	if complete != 0 || aborting != 0 {
		return SourcePublicationStageRef{}, ErrInvalidTransition
	}
	items := uint64(sourcePublicationStagePageItems(page))
	if ref.Items+items > SourcePublicationStageItemLimit ||
		ref.Bytes+uint64(pageBytes) > SourcePublicationStageByteLimit {
		return SourcePublicationStageRef{}, fmt.Errorf("%w: source publication stage limit exceeded", ErrInvalidObject)
	}
	if page.Header != nil {
		if err := appendSourcePublicationStageHeader(ctx, tx, identity, ref, *page.Header); err != nil {
			return SourcePublicationStageRef{}, err
		}
		ref.Revision = page.Header.Change.SourceRevision
	}
	if err := appendSourcePublicationStageAffected(ctx, tx, identity, page.Affected); err != nil {
		return SourcePublicationStageRef{}, err
	}
	if err := appendSourcePublicationStageSettlement(ctx, tx, identity, page); err != nil {
		return SourcePublicationStageRef{}, err
	}
	if page.Complete {
		if err := completeSourcePublicationStage(ctx, tx, identity, ref.Revision); err != nil {
			return SourcePublicationStageRef{}, err
		}
	}
	rolling := sourcePublicationStageRollingDigest(ref.Digest, pageDigest)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stage_pages(
    source_authority, stage_operation_id, sequence, page_digest, rolling_digest,
    page_item_count, page_byte_count, cumulative_revision, cumulative_item_count, cumulative_byte_count
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(identity.Authority), identity.Operation[:], page.Sequence,
		pageDigest[:], rolling[:], items, pageBytes, uint64(ref.Revision),
		ref.Items+items, ref.Bytes+uint64(pageBytes)); err != nil {
		return SourcePublicationStageRef{}, mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_publication_stages SET
    last_revision = ?, next_sequence = next_sequence + 1,
    item_count = item_count + ?, byte_count = byte_count + ?,
    complete = ?, rolling_digest = ?
WHERE source_authority = ? AND stage_operation_id = ? AND next_sequence = ?`,
		uint64(ref.Revision), items, pageBytes, boolInt(page.Complete), rolling[:],
		string(identity.Authority), identity.Operation[:], page.Sequence); err != nil {
		return SourcePublicationStageRef{}, err
	}
	if err := tx.Commit(); err != nil {
		return SourcePublicationStageRef{}, fmt.Errorf("catalog: commit source publication stage append: %w", err)
	}
	ref.Sequence++
	ref.Items += items
	ref.Bytes += uint64(pageBytes)
	ref.Digest = rolling
	return ref, nil
}

func sourcePublicationStagePageItems(page SourcePublicationStagePage) int {
	items := len(page.Affected) + len(page.Index) + len(page.Deletes) + len(page.Bindings) +
		len(page.MatchedMutations) + len(page.MismatchedMutations)
	if page.Header != nil {
		items++
	}
	return items
}

func sourcePublicationStageRollingDigest(prior, page [32]byte) [32]byte {
	value := make([]byte, 0, len(prior)+len(page))
	value = append(value, prior[:]...)
	value = append(value, page[:]...)
	return sha256.Sum256(value)
}

type sourcePublicationStageRow interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readSourcePublicationStage(
	ctx context.Context,
	query sourcePublicationStageRow,
	authority causal.SourceAuthorityID,
) (SourcePublicationStageRef, bool, error) {
	var ref SourcePublicationStageRef
	var operation, declaration, digest []byte
	var owner, driverID string
	var fleetGeneration uint64
	var revision, sequence, items, byteCount uint64
	err := query.QueryRowContext(ctx, `
SELECT stage_operation_id, fleet_owner_id, authority_generation, driver_id,
       declaration_digest, through_sequence, last_revision, next_sequence,
       item_count, byte_count, rolling_digest
FROM source_publication_stages WHERE source_authority = ? AND stage_kind = 1`, string(authority)).Scan(
		&operation, &owner, &fleetGeneration, &driverID, &declaration,
		&ref.Through, &revision, &sequence, &items, &byteCount, &digest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SourcePublicationStageRef{}, false, nil
	}
	if err != nil {
		return SourcePublicationStageRef{}, false, err
	}
	if len(operation) != len(causal.OperationID{}) || len(declaration) != sha256.Size || len(digest) != sha256.Size {
		return SourcePublicationStageRef{}, false, ErrIntegrity
	}
	ref.Authority = authority
	ref.FleetOwner = SourceAuthorityFleetOwnerID(owner)
	ref.FleetGeneration = causal.Generation(fleetGeneration)
	ref.DriverID = driverID
	copy(ref.DeclarationDigest[:], declaration)
	copy(ref.Operation[:], operation)
	ref.Revision = causal.Revision(revision)
	ref.Sequence, ref.Items, ref.Bytes = sequence, items, byteCount
	copy(ref.Digest[:], digest)
	return ref, true, nil
}

func readSourcePublicationStageOperation(
	ctx context.Context,
	query sourcePublicationStageRow,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
) (SourcePublicationStageRef, bool, error) {
	var ref SourcePublicationStageRef
	var storedOperation, declaration, digest []byte
	var owner, driverID string
	var fleetGeneration uint64
	var revision, sequence, items, byteCount uint64
	err := query.QueryRowContext(ctx, `
SELECT stage_operation_id, fleet_owner_id, authority_generation, driver_id,
       declaration_digest, through_sequence, last_revision, next_sequence,
       item_count, byte_count, rolling_digest
FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`, string(authority), operation[:]).Scan(
		&storedOperation, &owner, &fleetGeneration, &driverID, &declaration,
		&ref.Through, &revision, &sequence, &items, &byteCount, &digest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SourcePublicationStageRef{}, false, nil
	}
	if err != nil {
		return SourcePublicationStageRef{}, false, err
	}
	if len(storedOperation) != len(causal.OperationID{}) || len(declaration) != sha256.Size || len(digest) != sha256.Size {
		return SourcePublicationStageRef{}, false, ErrIntegrity
	}
	ref.Authority = authority
	ref.FleetOwner = SourceAuthorityFleetOwnerID(owner)
	ref.FleetGeneration = causal.Generation(fleetGeneration)
	ref.DriverID = driverID
	copy(ref.DeclarationDigest[:], declaration)
	copy(ref.Operation[:], storedOperation)
	ref.Revision = causal.Revision(revision)
	ref.Sequence, ref.Items, ref.Bytes = sequence, items, byteCount
	copy(ref.Digest[:], digest)
	return ref, true, nil
}

func validateStoredSourcePublicationStageIdentity(
	ctx context.Context,
	query sourcePublicationStageRow,
	identity SourcePublicationStageIdentity,
) error {
	encoded, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	expected := sha256.Sum256(encoded)
	var operation, declaration, digest []byte
	var owner, driverID, stream, epoch string
	var generation uint64
	var through, predecessor uint64
	if err := query.QueryRowContext(ctx, `
SELECT stage_operation_id, fleet_owner_id, authority_generation, driver_id,
       declaration_digest, stream_identity, root_epoch, through_sequence,
       predecessor_revision, identity_digest
FROM source_publication_stages WHERE source_authority = ? AND stage_operation_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&operation, &owner, &generation, &driverID, &declaration,
		&stream, &epoch, &through, &predecessor, &digest); err != nil {
		return err
	}
	if !bytes.Equal(operation, identity.Operation[:]) || stream != identity.Stream ||
		owner != string(identity.FleetOwner) || generation != uint64(identity.FleetGeneration) ||
		driverID != identity.DriverID || !bytes.Equal(declaration, identity.DeclarationDigest[:]) ||
		epoch != identity.RootEpoch || through != identity.Through ||
		predecessor != uint64(identity.Predecessor) || !bytes.Equal(digest, expected[:]) {
		return ErrMutationConflict
	}
	return nil
}

func appendSourcePublicationStageHeader(
	ctx context.Context,
	tx *sql.Tx,
	identity SourcePublicationStageIdentity,
	ref SourcePublicationStageRef,
	header SourcePublicationStageHeader,
) error {
	if header.Change.SourceAuthority != identity.Authority ||
		header.Change.OperationID == identity.Operation ||
		header.Predecessor != ref.Revision ||
		header.Change.SourceRevision != ref.Revision+1 {
		return ErrSourcePredecessor
	}
	if ref.Revision > identity.Predecessor {
		if err := completeSourcePublicationStageRevision(ctx, tx, identity, ref.Revision); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stage_revisions(
    source_authority, stage_operation_id, source_revision, predecessor_revision,
    mode, operation_id, change_id, cause, origin_domain, origin_generation,
    last_affected_key, complete
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0)`,
		string(identity.Authority), identity.Operation[:], uint64(header.Change.SourceRevision),
		uint64(header.Predecessor), uint8(header.Mode), header.Change.OperationID[:],
		header.Change.ChangeID[:], string(header.Change.Cause), string(header.Change.Origin),
		uint64(header.Change.OriginGeneration))
	if err != nil {
		return mapConstraint(err)
	}
	return nil
}

func appendSourcePublicationStageAffected(
	ctx context.Context,
	tx *sql.Tx,
	identity SourcePublicationStageIdentity,
	affected []SourcePublicationAffected,
) error {
	for _, value := range affected {
		var last string
		var complete int
		if err := tx.QueryRowContext(ctx, `
SELECT last_affected_key, complete FROM source_publication_stage_revisions
WHERE source_authority = ? AND stage_operation_id = ? AND source_revision = ?`,
			string(identity.Authority), identity.Operation[:], uint64(value.Revision)).Scan(&last, &complete); err != nil {
			return err
		}
		if complete != 0 || (last != "" && last >= string(value.Key)) {
			return ErrInvalidTransition
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stage_affected(
    source_authority, stage_operation_id, source_revision, affected_key
) VALUES (?, ?, ?, ?)`, string(identity.Authority), identity.Operation[:],
			uint64(value.Revision), string(value.Key)); err != nil {
			return mapConstraint(err)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE source_publication_stage_revisions SET last_affected_key = ?
WHERE source_authority = ? AND stage_operation_id = ? AND source_revision = ?`,
			string(value.Key), string(identity.Authority), identity.Operation[:], uint64(value.Revision)); err != nil {
			return err
		}
	}
	return nil
}

func appendSourcePublicationStageSettlement(
	ctx context.Context,
	tx *sql.Tx,
	identity SourcePublicationStageIdentity,
	page SourcePublicationStagePage,
) error {
	if len(page.Index) > 0 {
		var root, relative string
		err := tx.QueryRowContext(ctx, `
SELECT root_id, relative_path FROM source_publication_stage_index
WHERE source_authority = ? AND stage_operation_id = ?
ORDER BY root_id DESC, relative_path DESC LIMIT 1`,
			string(identity.Authority), identity.Operation[:]).Scan(&root, &relative)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && compareSourceIndexLocator(
			SourceIndexLocator{RootID: root, Relative: relative},
			SourceIndexLocator{RootID: page.Index[0].RootID, Relative: page.Index[0].Relative},
		) >= 0 {
			return ErrInvalidTransition
		}
	}
	if len(page.Deletes) > 0 {
		var root, relative string
		err := tx.QueryRowContext(ctx, `
SELECT root_id, relative_path FROM source_publication_stage_index_deletes
WHERE source_authority = ? AND stage_operation_id = ?
ORDER BY root_id DESC, relative_path DESC LIMIT 1`,
			string(identity.Authority), identity.Operation[:]).Scan(&root, &relative)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && compareSourceIndexLocator(
			SourceIndexLocator{RootID: root, Relative: relative}, page.Deletes[0],
		) >= 0 {
			return ErrInvalidTransition
		}
	}
	if len(page.Bindings) > 0 {
		var logical string
		err := tx.QueryRowContext(ctx, `
SELECT logical_id FROM source_publication_stage_bindings
WHERE source_authority = ? AND stage_operation_id = ?
ORDER BY logical_id DESC LIMIT 1`,
			string(identity.Authority), identity.Operation[:]).Scan(&logical)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && logical >= page.Bindings[0].LogicalID {
			return ErrInvalidTransition
		}
	}
	for _, group := range []struct {
		values  []MutationID
		matched bool
	}{
		{values: page.MatchedMutations, matched: true},
		{values: page.MismatchedMutations, matched: false},
	} {
		if len(group.values) == 0 {
			continue
		}
		var raw []byte
		err := tx.QueryRowContext(ctx, `
SELECT mutation_id FROM source_publication_stage_mutations
WHERE source_authority = ? AND stage_operation_id = ? AND matched = ?
ORDER BY mutation_id DESC LIMIT 1`,
			string(identity.Authority), identity.Operation[:], boolInt(group.matched)).Scan(&raw)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && bytes.Compare(raw, group.values[0][:]) >= 0 {
			return ErrInvalidTransition
		}
	}
	for _, record := range page.Index {
		if record.Authority != identity.Authority || record.RootID == "" || record.Relative == "" ||
			len(record.FileIdentity) == 0 || len(record.Payload) == 0 {
			return fmt.Errorf("%w: invalid staged source index record", ErrInvalidObject)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stage_index(
    source_authority, stage_operation_id, root_id, relative_path, file_identity,
    object_kind, metadata_fingerprint, content_fingerprint, payload
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(identity.Authority), identity.Operation[:],
			record.RootID, record.Relative, record.FileIdentity, record.Kind,
			record.MetadataFingerprint[:], record.ContentFingerprint[:], record.Payload); err != nil {
			return mapConstraint(err)
		}
		var deleted int
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_publication_stage_index_deletes
    WHERE source_authority = ? AND stage_operation_id = ? AND root_id = ? AND relative_path = ?
)`, string(identity.Authority), identity.Operation[:], record.RootID, record.Relative).Scan(&deleted); err != nil {
			return err
		}
		if deleted != 0 {
			return ErrConflict
		}
		for _, logical := range record.Logical {
			if logical == "" {
				return fmt.Errorf("%w: empty staged source logical", ErrInvalidObject)
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stage_index_logical(
    source_authority, stage_operation_id, root_id, relative_path, logical_id
) VALUES (?, ?, ?, ?, ?)`, string(identity.Authority), identity.Operation[:],
				record.RootID, record.Relative, logical); err != nil {
				return mapConstraint(err)
			}
		}
	}
	for _, locator := range page.Deletes {
		var indexed int
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_publication_stage_index
    WHERE source_authority = ? AND stage_operation_id = ? AND root_id = ? AND relative_path = ?
)`, string(identity.Authority), identity.Operation[:], locator.RootID, locator.Relative).Scan(&indexed); err != nil {
			return err
		}
		if indexed != 0 {
			return ErrConflict
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stage_index_deletes(
    source_authority, stage_operation_id, root_id, relative_path
) VALUES (?, ?, ?, ?)`, string(identity.Authority), identity.Operation[:],
			locator.RootID, locator.Relative); err != nil {
			return mapConstraint(err)
		}
	}
	for _, binding := range page.Bindings {
		if binding.Authority != identity.Authority {
			return ErrMutationConflict
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stage_bindings(
    source_authority, stage_operation_id, logical_id, source_key, effective_fingerprint
) VALUES (?, ?, ?, ?, ?)`, string(identity.Authority), identity.Operation[:],
			binding.LogicalID, string(binding.SourceKey), binding.Fingerprint[:]); err != nil {
			return mapConstraint(err)
		}
	}
	for _, operation := range page.MatchedMutations {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stage_mutations(
    source_authority, stage_operation_id, mutation_id, matched
) VALUES (?, ?, ?, 1)`, string(identity.Authority), identity.Operation[:], operation[:]); err != nil {
			return mapConstraint(err)
		}
	}
	for _, operation := range page.MismatchedMutations {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_publication_stage_mutations(
    source_authority, stage_operation_id, mutation_id, matched
) VALUES (?, ?, ?, 0)`, string(identity.Authority), identity.Operation[:], operation[:]); err != nil {
			return mapConstraint(err)
		}
	}
	return nil
}

func completeSourcePublicationStage(
	ctx context.Context,
	tx *sql.Tx,
	identity SourcePublicationStageIdentity,
	revision causal.Revision,
) error {
	if revision < identity.Predecessor {
		return fmt.Errorf("%w: invalid source publication stage revision", ErrInvalidObject)
	}
	if revision == identity.Predecessor {
		return nil
	}
	return completeSourcePublicationStageRevision(ctx, tx, identity, revision)
}

func completeSourcePublicationStageRevision(
	ctx context.Context,
	tx *sql.Tx,
	identity SourcePublicationStageIdentity,
	revision causal.Revision,
) error {
	var affected int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM source_publication_stage_affected
WHERE source_authority = ? AND stage_operation_id = ? AND source_revision = ?`,
		string(identity.Authority), identity.Operation[:], uint64(revision)).Scan(&affected); err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("%w: incomplete staged source revision", ErrInvalidTransition)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_publication_stage_revisions SET complete = 1
WHERE source_authority = ? AND stage_operation_id = ? AND source_revision = ? AND complete = 0`,
		string(identity.Authority), identity.Operation[:], uint64(revision))
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrInvalidTransition
	}
	return nil
}

// CommitSourcePublicationStage atomically publishes and settles one complete stage.
func (c *Catalog) CommitSourcePublicationStage(
	ctx context.Context,
	expected SourcePublicationStageRef,
) (SourcePublicationStageResult, error) {
	if expected.Authority == "" || expected.Operation == (causal.OperationID{}) ||
		expected.Sequence == 0 || expected.Digest == ([32]byte{}) {
		return SourcePublicationStageResult{}, fmt.Errorf("%w: incomplete source publication stage proof", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourcePublicationStageResult{}, fmt.Errorf("catalog: begin staged source publication commit: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, found, err := readSourcePublicationStageOperation(ctx, tx, expected.Authority, expected.Operation)
	if err != nil {
		return SourcePublicationStageResult{}, err
	}
	if !found {
		result, found, err := readSourcePublicationStageReceipt(ctx, tx, expected)
		if err != nil || !found {
			if err != nil {
				return SourcePublicationStageResult{}, err
			}
			return SourcePublicationStageResult{}, ErrNotFound
		}
		if err := tx.Commit(); err != nil {
			return SourcePublicationStageResult{}, err
		}
		return result, nil
	}
	if current != expected {
		return SourcePublicationStageResult{}, ErrMutationConflict
	}
	var kind, complete, aborting int
	if err := tx.QueryRowContext(ctx, `
SELECT stage_kind, complete, aborting FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(expected.Authority), expected.Operation[:]).Scan(&kind, &complete, &aborting); err != nil {
		return SourcePublicationStageResult{}, err
	}
	if kind != 1 {
		return SourcePublicationStageResult{}, ErrMutationConflict
	}
	if complete == 0 || aborting != 0 {
		return SourcePublicationStageResult{}, ErrInvalidTransition
	}
	result, err := c.commitNormalizedSourcePublicationStage(ctx, tx, expected)
	if err != nil {
		return SourcePublicationStageResult{}, err
	}
	if err := insertSourcePublicationStageReceipt(ctx, tx, expected, result); err != nil {
		return SourcePublicationStageResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(expected.Authority), expected.Operation[:]); err != nil {
		return SourcePublicationStageResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SourcePublicationStageResult{}, fmt.Errorf("catalog: commit staged source publication: %w", err)
	}
	return result, nil
}

func insertSourcePublicationStageReceipt(
	ctx context.Context,
	tx *sql.Tx,
	expected SourcePublicationStageRef,
	result SourcePublicationStageResult,
) error {
	digest, err := sourcePublicationStageResultDigest(result)
	if err != nil {
		return err
	}
	if result.Count == 0 {
		_, err = tx.ExecContext(ctx, `
INSERT INTO source_observer_settlement_receipts(
    source_authority, stage_operation_id, fleet_owner_id, authority_generation,
    driver_id, declaration_digest, through_sequence, source_revision,
    stage_sequence, stage_item_count, stage_byte_count, stage_digest, receipt_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(expected.Authority), expected.Operation[:], string(expected.FleetOwner),
			uint64(expected.FleetGeneration), expected.DriverID, expected.DeclarationDigest[:], expected.Through,
			uint64(result.Last), expected.Sequence, expected.Items, expected.Bytes,
			expected.Digest[:], digest[:])
		if err != nil {
			return mapConstraint(err)
		}
		return nil
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO source_publication_stage_receipts(
    source_authority, stage_operation_id, fleet_owner_id, authority_generation,
    driver_id, declaration_digest, through_sequence,
    first_revision, last_revision, revision_count,
    stage_sequence, stage_item_count, stage_byte_count, stage_digest, receipt_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(expected.Authority), expected.Operation[:], string(expected.FleetOwner),
		uint64(expected.FleetGeneration), expected.DriverID, expected.DeclarationDigest[:], expected.Through,
		uint64(result.First), uint64(result.Last), result.Count,
		expected.Sequence, expected.Items, expected.Bytes, expected.Digest[:], digest[:])
	if err != nil {
		return mapConstraint(err)
	}
	return nil
}

// AcknowledgeSourceObserverSettlement records that one exact zero-change commit response reached its caller.
func (c *Catalog) AcknowledgeSourceObserverSettlement(
	ctx context.Context,
	expected SourcePublicationStageRef,
) error {
	if expected.Authority == "" || expected.Operation == (causal.OperationID{}) ||
		expected.Sequence == 0 || expected.Items == 0 || expected.Bytes == 0 ||
		expected.Digest == ([32]byte{}) {
		return fmt.Errorf("%w: incomplete source observer settlement acknowledgement", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var owner, driverID string
	var declaration, stageDigest []byte
	var generation, through, revision, sequence, items, byteCount uint64
	err = tx.QueryRowContext(ctx, `
SELECT fleet_owner_id, authority_generation, driver_id, declaration_digest,
       through_sequence, source_revision, stage_sequence, stage_item_count,
       stage_byte_count, stage_digest
FROM source_observer_settlement_receipts
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(expected.Authority), expected.Operation[:]).Scan(
		&owner, &generation, &driverID, &declaration,
		&through, &revision, &sequence, &items, &byteCount, &stageDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrMutationConflict
	}
	if err != nil {
		return err
	}
	if owner != string(expected.FleetOwner) || generation != uint64(expected.FleetGeneration) ||
		driverID != expected.DriverID || !bytes.Equal(declaration, expected.DeclarationDigest[:]) ||
		through != expected.Through || causal.Revision(revision) != expected.Revision ||
		sequence != expected.Sequence || items != expected.Items || byteCount != expected.Bytes ||
		!bytes.Equal(stageDigest, expected.Digest[:]) {
		return ErrMutationConflict
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_observer_settlement_receipts
SET acknowledged = 1
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(expected.Authority), expected.Operation[:])
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	return tx.Commit()
}

func readSourcePublicationStageReceipt(
	ctx context.Context,
	query sourcePublicationStageRow,
	expected SourcePublicationStageRef,
) (SourcePublicationStageResult, bool, error) {
	var operation, declaration, stageDigest, receiptDigest []byte
	var owner, driverID string
	var generation, through, first, last, count, sequence, items, byteCount uint64
	err := query.QueryRowContext(ctx, `
SELECT stage_operation_id, fleet_owner_id, authority_generation, driver_id, declaration_digest,
       through_sequence, first_revision, last_revision, revision_count,
       stage_sequence, stage_item_count, stage_byte_count, stage_digest, receipt_digest
FROM source_publication_stage_receipts
WHERE source_authority = ? AND stage_operation_id = ?
UNION ALL
SELECT stage_operation_id, fleet_owner_id, authority_generation, driver_id, declaration_digest,
       through_sequence, source_revision, source_revision, 0,
       stage_sequence, stage_item_count, stage_byte_count, stage_digest, receipt_digest
FROM source_observer_settlement_receipts
WHERE source_authority = ? AND stage_operation_id = ?
LIMIT 1`,
		string(expected.Authority), expected.Operation[:],
		string(expected.Authority), expected.Operation[:]).
		Scan(&operation, &owner, &generation, &driverID, &declaration,
			&through, &first, &last, &count, &sequence, &items, &byteCount, &stageDigest, &receiptDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return SourcePublicationStageResult{}, false, nil
	}
	if err != nil {
		return SourcePublicationStageResult{}, false, err
	}
	if len(operation) != len(causal.OperationID{}) || len(declaration) != sha256.Size || len(stageDigest) != sha256.Size ||
		len(receiptDigest) != sha256.Size {
		return SourcePublicationStageResult{}, false, ErrIntegrity
	}
	var storedOperation causal.OperationID
	var storedDigest [sha256.Size]byte
	copy(storedOperation[:], operation)
	copy(storedDigest[:], stageDigest)
	stored := SourcePublicationStageRef{
		Authority: expected.Authority, FleetOwner: SourceAuthorityFleetOwnerID(owner),
		FleetGeneration: causal.Generation(generation), DriverID: driverID,
		Operation: storedOperation, Through: through,
		Revision: causal.Revision(last), Sequence: sequence, Items: items, Bytes: byteCount,
		Digest: storedDigest,
	}
	copy(stored.DeclarationDigest[:], declaration)
	if stored != expected {
		return SourcePublicationStageResult{}, false, ErrMutationConflict
	}
	result := SourcePublicationStageResult{
		Authority: expected.Authority, FleetOwner: stored.FleetOwner,
		FleetGeneration: stored.FleetGeneration, DriverID: stored.DriverID,
		DeclarationDigest: stored.DeclarationDigest, Operation: storedOperation,
		First: causal.Revision(first), Last: causal.Revision(last), Count: count, Digest: storedDigest,
	}
	digest, err := sourcePublicationStageResultDigest(result)
	if err != nil {
		return SourcePublicationStageResult{}, false, err
	}
	if !bytes.Equal(receiptDigest, digest[:]) {
		return SourcePublicationStageResult{}, false, ErrIntegrity
	}
	return result, true, nil
}

func sourcePublicationStageResultDigest(result SourcePublicationStageResult) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("catalog: encode source publication stage receipt: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

// AbortSourcePublicationStage releases one exact incomplete publication stage.
func (c *Catalog) AbortSourcePublicationStage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
) error {
	if authority == "" || operation == (causal.OperationID{}) {
		return fmt.Errorf("%w: incomplete source publication stage abort", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, found, err := readSourcePublicationStage(ctx, tx, authority)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if current.Operation != operation {
		return ErrMutationConflict
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_publication_stages SET aborting = 1
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(authority), operation[:]); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	finalTx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = finalTx.Rollback() }()
	if _, err := finalTx.ExecContext(ctx, `
DELETE FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ? AND aborting = 1`,
		string(authority), operation[:]); err != nil {
		return err
	}
	return finalTx.Commit()
}
