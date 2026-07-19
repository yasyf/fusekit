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
	SourceObserverConfigurationPageLimit     = 256
	SourceObserverConfigurationPageByteLimit = 1 << 20
	SourceObserverConfigurationRecordLimit   = 10_000
	SourceObserverConfigurationByteLimit     = 64 << 20
)

// SourceObserverConfigurationIdentity fences one normalized observer configuration.
type SourceObserverConfigurationIdentity struct {
	Authority         causal.SourceAuthorityID
	FleetOwner        SourceAuthorityFleetOwnerID
	FleetGeneration   causal.Generation
	Operation         causal.OperationID
	Stream            string
	RootEpoch         string
	RootDigest        [32]byte
	FleetDigest       [32]byte
	Reset             bool
	RootCount         uint64
	CheckpointCount   uint64
	RootsDigest       [32]byte
	CheckpointsDigest [32]byte
}

// SourceObserverConfigurationRef is an exact cumulative configuration-stage proof.
type SourceObserverConfigurationRef struct {
	Authority   causal.SourceAuthorityID
	Operation   causal.OperationID
	Sequence    uint64
	Roots       uint64
	Checkpoints uint64
	Bytes       uint64
	Digest      [32]byte
}

// SourceObserverRootAppendPage is one bounded ordered configuration fragment.
type SourceObserverRootAppendPage struct {
	Sequence uint64
	Records  []SourceObserverRootRecord
}

// SourceObserverCheckpointAppendPage is one bounded ordered configuration fragment.
type SourceObserverCheckpointAppendPage struct {
	Sequence uint64
	Records  []SourceObserverCheckpointRecord
}

// SourceObserverRootPage is one bounded root read page.
type SourceObserverRootPage struct {
	Records []SourceObserverRootRecord
	Next    string
}

// SourceObserverCheckpointPage is one bounded checkpoint read page.
type SourceObserverCheckpointPage struct {
	Records []SourceObserverCheckpointRecord
	Next    string
}

// BeginSourceObserverConfiguration begins one exact normalized configuration stage.
func (c *Catalog) BeginSourceObserverConfiguration(
	ctx context.Context,
	identity SourceObserverConfigurationIdentity,
) error {
	if err := validateSourceObserverConfigurationIdentity(identity); err != nil {
		return err
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	identityDigest := sha256.Sum256(encoded)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var member int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_authority_fleet_members
    WHERE owner_id = ? AND generation = ? AND source_authority = ?
)`, string(identity.FleetOwner), uint64(identity.FleetGeneration), string(identity.Authority)).
		Scan(&member); err != nil {
		return err
	}
	if member == 0 {
		return ErrGenerationMismatch
	}
	zero := [32]byte{}
	result, err := tx.ExecContext(ctx, `
INSERT INTO source_observer_configuration_stages(
    source_authority, fleet_owner_id, fleet_generation, operation_id,
    stream_identity, root_epoch, root_set_digest,
    fleet_digest, reset, expected_root_count, expected_checkpoint_count,
    expected_roots_digest, expected_checkpoints_digest, next_sequence, root_count,
    checkpoint_count, byte_count, phase, last_root_id, last_checkpoint_stream,
    identity_digest, rolling_digest, roots_digest, checkpoints_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 1, '', '', ?, ?, ?, ?)
ON CONFLICT(source_authority) DO NOTHING`,
		string(identity.Authority), string(identity.FleetOwner), uint64(identity.FleetGeneration),
		identity.Operation[:], identity.Stream, identity.RootEpoch,
		identity.RootDigest[:], identity.FleetDigest[:], boolInt(identity.Reset),
		identity.RootCount, identity.CheckpointCount, identity.RootsDigest[:],
		identity.CheckpointsDigest[:], identityDigest[:], identityDigest[:], zero[:], zero[:])
	if err != nil {
		return mapConstraint(err)
	}
	if inserted, _ := result.RowsAffected(); inserted == 0 {
		var operation, storedDigest []byte
		if err := tx.QueryRowContext(ctx, `
SELECT operation_id, identity_digest FROM source_observer_configuration_stages
WHERE source_authority = ?`, string(identity.Authority)).Scan(&operation, &storedDigest); err != nil {
			return err
		}
		if !bytes.Equal(operation, identity.Operation[:]) || !bytes.Equal(storedDigest, identityDigest[:]) {
			return ErrSourceObserverConflict
		}
	}
	return tx.Commit()
}

// AppendSourceObserverConfigurationRoots appends one exact bounded root page.
func (c *Catalog) AppendSourceObserverConfigurationRoots(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	page SourceObserverRootAppendPage,
) (SourceObserverConfigurationRef, error) {
	if authority == "" || operation == (causal.OperationID{}) || len(page.Records) == 0 ||
		len(page.Records) > SourceObserverConfigurationPageLimit {
		return SourceObserverConfigurationRef{}, fmt.Errorf("%w: invalid source observer root page", ErrInvalidObject)
	}
	for index, record := range page.Records {
		if err := validateSourceObserverRoot(record); err != nil ||
			(index > 0 && page.Records[index-1].ID >= record.ID) {
			if err != nil {
				return SourceObserverConfigurationRef{}, err
			}
			return SourceObserverConfigurationRef{}, fmt.Errorf("%w: source observer roots are not exact and ordered", ErrInvalidObject)
		}
	}
	encoded, err := json.Marshal(page)
	if err != nil || len(encoded) > SourceObserverConfigurationPageByteLimit {
		return SourceObserverConfigurationRef{}, fmt.Errorf("%w: source observer root page exceeds byte quota", ErrInvalidObject)
	}
	return c.appendSourceObserverConfigurationPage(ctx, authority, operation, page.Sequence, 1, encoded, func(
		tx *sql.Tx,
		last string,
		categoryDigest [32]byte,
	) (string, [32]byte, error) {
		for _, record := range page.Records {
			if last != "" && last >= record.ID {
				return "", [32]byte{}, ErrInvalidTransition
			}
			value, err := json.Marshal(record)
			if err != nil {
				return "", [32]byte{}, err
			}
			categoryDigest = sourceObserverConfigurationRollingDigest(categoryDigest, sha256.Sum256(value))
			if _, err := tx.ExecContext(ctx, `
INSERT INTO source_observer_configuration_roots(
    source_authority, operation_id, root_id, generation, path, volume_uuid,
    root_inode, root_birthtime_sec, root_birthtime_nsec, root_kind
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				string(authority), operation[:], record.ID, record.Generation, record.Path,
				record.VolumeUUID, record.Inode, record.BirthSec, record.BirthNsec, record.Kind); err != nil {
				return "", [32]byte{}, mapConstraint(err)
			}
			last = record.ID
		}
		return last, categoryDigest, nil
	})
}

// AppendSourceObserverConfigurationCheckpoints appends one exact bounded checkpoint page.
func (c *Catalog) AppendSourceObserverConfigurationCheckpoints(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	page SourceObserverCheckpointAppendPage,
) (SourceObserverConfigurationRef, error) {
	if authority == "" || operation == (causal.OperationID{}) || len(page.Records) == 0 ||
		len(page.Records) > SourceObserverConfigurationPageLimit {
		return SourceObserverConfigurationRef{}, fmt.Errorf("%w: invalid source observer checkpoint page", ErrInvalidObject)
	}
	for index, record := range page.Records {
		if err := validateSourceObserverCheckpoint(record); err != nil ||
			(index > 0 && page.Records[index-1].Stream >= record.Stream) {
			if err != nil {
				return SourceObserverConfigurationRef{}, err
			}
			return SourceObserverConfigurationRef{}, fmt.Errorf("%w: source observer checkpoints are not exact and ordered", ErrInvalidObject)
		}
	}
	encoded, err := json.Marshal(page)
	if err != nil || len(encoded) > SourceObserverConfigurationPageByteLimit {
		return SourceObserverConfigurationRef{}, fmt.Errorf("%w: source observer checkpoint page exceeds byte quota", ErrInvalidObject)
	}
	return c.appendSourceObserverConfigurationPage(ctx, authority, operation, page.Sequence, 2, encoded, func(
		tx *sql.Tx,
		last string,
		categoryDigest [32]byte,
	) (string, [32]byte, error) {
		for _, record := range page.Records {
			if last != "" && last >= record.Stream {
				return "", [32]byte{}, ErrInvalidTransition
			}
			value, err := json.Marshal(record)
			if err != nil {
				return "", [32]byte{}, err
			}
			categoryDigest = sourceObserverConfigurationRollingDigest(categoryDigest, sha256.Sum256(value))
			if _, err := tx.ExecContext(ctx, `
INSERT INTO source_observer_configuration_checkpoints(
    source_authority, operation_id, stream_identity, root_epoch, native_event_id
) VALUES (?, ?, ?, ?, ?)`, string(authority), operation[:], record.Stream,
				record.RootEpoch, record.EventID); err != nil {
				return "", [32]byte{}, mapConstraint(err)
			}
			last = record.Stream
		}
		return last, categoryDigest, nil
	})
}

type appendSourceObserverConfigurationRecords func(*sql.Tx, string, [32]byte) (string, [32]byte, error)

func (c *Catalog) appendSourceObserverConfigurationPage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	sequence uint64,
	kind int,
	encoded []byte,
	appendRecords appendSourceObserverConfigurationRecords,
) (SourceObserverConfigurationRef, error) {
	pageDigest := sha256.Sum256(encoded)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceObserverConfigurationRef{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var storedKind int
	var storedDigest, storedRolling []byte
	var roots, checkpoints, storedBytes uint64
	err = tx.QueryRowContext(ctx, `
SELECT kind, page_digest, rolling_digest, cumulative_root_count,
       cumulative_checkpoint_count, cumulative_byte_count
FROM source_observer_configuration_pages
WHERE source_authority = ? AND operation_id = ? AND sequence = ?`,
		string(authority), operation[:], sequence).
		Scan(&storedKind, &storedDigest, &storedRolling, &roots, &checkpoints, &storedBytes)
	if err == nil {
		if storedKind != kind || !bytes.Equal(storedDigest, pageDigest[:]) {
			return SourceObserverConfigurationRef{}, ErrSourceObserverConflict
		}
		return SourceObserverConfigurationRef{
			Authority: authority, Operation: operation, Sequence: sequence + 1,
			Roots: roots, Checkpoints: checkpoints, Bytes: storedBytes, Digest: bytesToDigest(storedRolling),
		}, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SourceObserverConfigurationRef{}, err
	}
	var next, rootCount, checkpointCount, byteCount, expectedRoots, expectedCheckpoints uint64
	var phase int
	var lastRoot, lastCheckpoint string
	var rolling, rootsDigest, checkpointsDigest []byte
	if err := tx.QueryRowContext(ctx, `
SELECT next_sequence, root_count, checkpoint_count, byte_count,
       expected_root_count, expected_checkpoint_count, phase,
       last_root_id, last_checkpoint_stream, rolling_digest, roots_digest, checkpoints_digest
FROM source_observer_configuration_stages
WHERE source_authority = ? AND operation_id = ?`,
		string(authority), operation[:]).
		Scan(&next, &rootCount, &checkpointCount, &byteCount,
			&expectedRoots, &expectedCheckpoints, &phase,
			&lastRoot, &lastCheckpoint, &rolling, &rootsDigest, &checkpointsDigest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SourceObserverConfigurationRef{}, ErrNotFound
		}
		return SourceObserverConfigurationRef{}, err
	}
	if next != sequence || phase > kind || (kind == 1 && phase != 1) {
		return SourceObserverConfigurationRef{}, ErrInvalidTransition
	}
	if kind == 2 && phase == 1 {
		if rootCount != expectedRoots {
			return SourceObserverConfigurationRef{}, ErrInvalidTransition
		}
		phase = 2
	}
	var categoryDigest [32]byte
	last := lastRoot
	if kind == 1 {
		copy(categoryDigest[:], rootsDigest)
	} else {
		last = lastCheckpoint
		copy(categoryDigest[:], checkpointsDigest)
	}
	last, categoryDigest, err = appendRecords(tx, last, categoryDigest)
	if err != nil {
		return SourceObserverConfigurationRef{}, err
	}
	var prior [32]byte
	copy(prior[:], rolling)
	nextRolling := sourceObserverConfigurationRollingDigest(prior, pageDigest)
	if kind == 1 {
		rootCount += uint64(sourceObserverConfigurationPageRecords(encoded, kind))
		if rootCount > expectedRoots {
			return SourceObserverConfigurationRef{}, ErrInvalidTransition
		}
		lastRoot = last
		rootsDigest = categoryDigest[:]
	} else {
		checkpointCount += uint64(sourceObserverConfigurationPageRecords(encoded, kind))
		if checkpointCount > expectedCheckpoints {
			return SourceObserverConfigurationRef{}, ErrInvalidTransition
		}
		lastCheckpoint = last
		checkpointsDigest = categoryDigest[:]
	}
	byteCount += uint64(len(encoded))
	if byteCount > SourceObserverConfigurationByteLimit {
		return SourceObserverConfigurationRef{}, fmt.Errorf("%w: source observer configuration byte limit exceeded", ErrInvalidObject)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_observer_configuration_stages SET
    next_sequence = ?, root_count = ?, checkpoint_count = ?, byte_count = ?, phase = ?,
    last_root_id = ?, last_checkpoint_stream = ?, rolling_digest = ?,
    roots_digest = ?, checkpoints_digest = ?
WHERE source_authority = ? AND operation_id = ?`,
		sequence+1, rootCount, checkpointCount, byteCount, phase, lastRoot, lastCheckpoint,
		nextRolling[:], rootsDigest, checkpointsDigest, string(authority), operation[:]); err != nil {
		return SourceObserverConfigurationRef{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_observer_configuration_pages(
    source_authority, operation_id, sequence, kind, page_digest, rolling_digest,
    cumulative_root_count, cumulative_checkpoint_count, cumulative_byte_count
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(authority), operation[:], sequence, kind, pageDigest[:], nextRolling[:],
		rootCount, checkpointCount, byteCount); err != nil {
		return SourceObserverConfigurationRef{}, mapConstraint(err)
	}
	if err := tx.Commit(); err != nil {
		return SourceObserverConfigurationRef{}, err
	}
	return SourceObserverConfigurationRef{
		Authority: authority, Operation: operation, Sequence: sequence + 1,
		Roots: rootCount, Checkpoints: checkpointCount, Bytes: byteCount, Digest: nextRolling,
	}, nil
}

func sourceObserverConfigurationPageRecords(encoded []byte, kind int) int {
	if kind == 1 {
		var page SourceObserverRootAppendPage
		_ = json.Unmarshal(encoded, &page)
		return len(page.Records)
	}
	var page SourceObserverCheckpointAppendPage
	_ = json.Unmarshal(encoded, &page)
	return len(page.Records)
}

// CommitSourceObserverConfiguration atomically replaces one exact observer configuration.
func (c *Catalog) CommitSourceObserverConfiguration(
	ctx context.Context,
	expected SourceObserverConfigurationRef,
) (SourceObserverStreamRecord, error) {
	if expected.Authority == "" || expected.Operation == (causal.OperationID{}) ||
		expected.Sequence == 0 || expected.Roots == 0 || expected.Checkpoints == 0 ||
		expected.Bytes == 0 || expected.Digest == ([32]byte{}) {
		return SourceObserverStreamRecord{}, fmt.Errorf("%w: incomplete source observer configuration proof", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceObserverStreamRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()
	current, found, err := readSourceObserverConfigurationRef(ctx, tx, expected.Authority, expected.Operation)
	if err != nil {
		return SourceObserverStreamRecord{}, err
	}
	if !found {
		stream, found, err := readSourceObserverConfigurationReceipt(ctx, tx, expected)
		if err != nil || !found {
			if err != nil {
				return SourceObserverStreamRecord{}, err
			}
			return SourceObserverStreamRecord{}, ErrNotFound
		}
		if err := tx.Commit(); err != nil {
			return SourceObserverStreamRecord{}, err
		}
		return stream, nil
	}
	if current != expected {
		return SourceObserverStreamRecord{}, ErrMutationConflict
	}
	var identity SourceObserverConfigurationIdentity
	var operation, rootDigest, fleetDigest, rootsDigest, checkpointsDigest []byte
	var reset int
	if err := tx.QueryRowContext(ctx, `
SELECT fleet_owner_id, fleet_generation, operation_id,
       stream_identity, root_epoch, root_set_digest, fleet_digest,
       reset, expected_root_count, expected_checkpoint_count,
       expected_roots_digest, expected_checkpoints_digest
FROM source_observer_configuration_stages
WHERE source_authority = ? AND operation_id = ?`,
		string(expected.Authority), expected.Operation[:]).
		Scan(&identity.FleetOwner, &identity.FleetGeneration, &operation,
			&identity.Stream, &identity.RootEpoch, &rootDigest, &fleetDigest,
			&reset, &identity.RootCount, &identity.CheckpointCount, &rootsDigest, &checkpointsDigest); err != nil {
		return SourceObserverStreamRecord{}, err
	}
	identity.Authority = expected.Authority
	identity.Operation = expected.Operation
	identity.Reset = reset != 0
	identity.RootDigest = bytesToDigest(rootDigest)
	identity.FleetDigest = bytesToDigest(fleetDigest)
	identity.RootsDigest = bytesToDigest(rootsDigest)
	identity.CheckpointsDigest = bytesToDigest(checkpointsDigest)
	if !bytes.Equal(operation, expected.Operation[:]) ||
		identity.RootCount != expected.Roots || identity.CheckpointCount != expected.Checkpoints {
		return SourceObserverStreamRecord{}, ErrMutationConflict
	}
	var actualRoots, actualCheckpoints []byte
	if err := tx.QueryRowContext(ctx, `
SELECT roots_digest, checkpoints_digest
FROM source_observer_configuration_stages
WHERE source_authority = ? AND operation_id = ?`,
		string(expected.Authority), expected.Operation[:]).Scan(&actualRoots, &actualCheckpoints); err != nil {
		return SourceObserverStreamRecord{}, err
	}
	if !bytes.Equal(actualRoots, identity.RootsDigest[:]) ||
		!bytes.Equal(actualCheckpoints, identity.CheckpointsDigest[:]) {
		return SourceObserverStreamRecord{}, ErrMutationConflict
	}
	stream, found, err := readSourceObserverStream(ctx, tx, expected.Authority)
	if err != nil {
		return SourceObserverStreamRecord{}, err
	}
	setsChanged := true
	if found {
		setsChanged, err = sourceObserverConfigurationSetsChanged(ctx, tx, expected)
		if err != nil {
			return SourceObserverStreamRecord{}, err
		}
	}
	changed := identity.Reset || !found || setsChanged ||
		stream.FleetOwner != identity.FleetOwner ||
		stream.FleetGeneration != identity.FleetGeneration ||
		stream.Stream != identity.Stream || stream.RootEpoch != identity.RootEpoch ||
		stream.RootDigest != identity.RootDigest || stream.FleetDigest != identity.FleetDigest
	if changed && found {
		var pending int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM source_publication_stages WHERE source_authority = ?`,
			string(expected.Authority)).Scan(&pending); err != nil {
			return SourceObserverStreamRecord{}, err
		}
		if pending != 0 {
			return SourceObserverStreamRecord{}, fmt.Errorf(
				"%w: source publication stage crosses an observer fence", ErrSourceObserverConflict,
			)
		}
	}
	if !found {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_observer_streams(
    source_authority, fleet_owner_id, fleet_generation,
    stream_identity, root_epoch, root_set_digest, fleet_digest,
    last_received_sequence, last_applied_sequence, state, quarantine_detail
) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, '')`,
			string(expected.Authority), string(identity.FleetOwner), uint64(identity.FleetGeneration),
			identity.Stream, identity.RootEpoch,
			identity.RootDigest[:], identity.FleetDigest[:], uint8(SourceObserverSnapshotRequired)); err != nil {
			return SourceObserverStreamRecord{}, mapConstraint(err)
		}
	} else if changed {
		for _, query := range []string{
			`DELETE FROM source_observer_inbox WHERE source_authority = ?`,
			`DELETE FROM source_physical_index WHERE source_authority = ?`,
			`DELETE FROM source_snapshot_stages WHERE source_authority = ?`,
			`DELETE FROM source_snapshot_sessions WHERE source_authority = ?`,
			`DELETE FROM source_observer_roots WHERE source_authority = ?`,
			`DELETE FROM source_observer_checkpoints WHERE source_authority = ?`,
		} {
			if _, err := tx.ExecContext(ctx, query, string(expected.Authority)); err != nil {
				return SourceObserverStreamRecord{}, err
			}
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE source_observer_streams SET
    fleet_owner_id = ?, fleet_generation = ?,
    stream_identity = ?, root_epoch = ?, root_set_digest = ?, fleet_digest = ?,
    last_received_sequence = 0, last_applied_sequence = 0,
    applied_snapshot_id = '', applied_snapshot_operation = X'',
    applied_snapshot_digest = X'', applied_snapshot_fence = X'',
    state = ?, quarantine_detail = ''
WHERE source_authority = ?`,
			string(identity.FleetOwner), uint64(identity.FleetGeneration),
			identity.Stream, identity.RootEpoch, identity.RootDigest[:], identity.FleetDigest[:],
			uint8(SourceObserverSnapshotRequired), string(expected.Authority)); err != nil {
			return SourceObserverStreamRecord{}, err
		}
	}
	if changed {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_observer_roots(
    source_authority, root_id, generation, path, volume_uuid, root_inode,
    root_birthtime_sec, root_birthtime_nsec, root_kind, root_set_digest
)
SELECT source_authority, root_id, generation, path, volume_uuid, root_inode,
       root_birthtime_sec, root_birthtime_nsec, root_kind, ?
FROM source_observer_configuration_roots
WHERE source_authority = ? AND operation_id = ?`,
			identity.RootDigest[:], string(expected.Authority), expected.Operation[:]); err != nil {
			return SourceObserverStreamRecord{}, mapConstraint(err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_observer_checkpoints(
    source_authority, stream_identity, root_epoch, native_event_id
)
SELECT source_authority, stream_identity, root_epoch, native_event_id
FROM source_observer_configuration_checkpoints
WHERE source_authority = ? AND operation_id = ?`,
			string(expected.Authority), expected.Operation[:]); err != nil {
			return SourceObserverStreamRecord{}, mapConstraint(err)
		}
	}
	stream, found, err = readSourceObserverStream(ctx, tx, expected.Authority)
	if err != nil || !found {
		if err != nil {
			return SourceObserverStreamRecord{}, err
		}
		return SourceObserverStreamRecord{}, ErrIntegrity
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_observer_configuration_receipts(
    source_authority, fleet_owner_id, fleet_generation,
    operation_id, sequence, root_count, checkpoint_count,
    byte_count, stage_digest, stream_identity, root_epoch, root_set_digest,
    fleet_digest, last_received_sequence, last_applied_sequence, state, quarantine_detail
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(expected.Authority), string(stream.FleetOwner), uint64(stream.FleetGeneration),
		expected.Operation[:], expected.Sequence, expected.Roots,
		expected.Checkpoints, expected.Bytes, expected.Digest[:], stream.Stream, stream.RootEpoch,
		stream.RootDigest[:], stream.FleetDigest[:], stream.LastReceived, stream.LastApplied,
		uint8(stream.Mode), stream.Quarantine); err != nil {
		return SourceObserverStreamRecord{}, mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_observer_configuration_stages
WHERE source_authority = ? AND operation_id = ?`,
		string(expected.Authority), expected.Operation[:]); err != nil {
		return SourceObserverStreamRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return SourceObserverStreamRecord{}, err
	}
	return stream, nil
}

// AbortSourceObserverConfiguration discards one exact incomplete configuration.
func (c *Catalog) AbortSourceObserverConfiguration(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
) error {
	if authority == "" || operation == (causal.OperationID{}) {
		return fmt.Errorf("%w: incomplete source observer configuration abort", ErrInvalidObject)
	}
	result, err := c.db.ExecContext(ctx, `
DELETE FROM source_observer_configuration_stages
WHERE source_authority = ? AND operation_id = ?`, string(authority), operation[:])
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed == 0 {
		var exists int
		if err := c.readDB.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_observer_configuration_receipts
    WHERE source_authority = ? AND operation_id = ?
)`, string(authority), operation[:]).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return ErrNotFound
		}
	}
	return nil
}

// AcknowledgeSourceObserverConfiguration records that one exact commit response reached its caller.
func (c *Catalog) AcknowledgeSourceObserverConfiguration(
	ctx context.Context,
	expected SourceObserverConfigurationRef,
) error {
	if expected.Authority == "" || expected.Operation == (causal.OperationID{}) ||
		expected.Sequence == 0 || expected.Roots == 0 || expected.Checkpoints == 0 ||
		expected.Bytes == 0 || expected.Digest == ([32]byte{}) {
		return fmt.Errorf("%w: incomplete source observer configuration acknowledgement", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, found, err := readSourceObserverConfigurationReceipt(ctx, tx, expected); err != nil {
		return err
	} else if !found {
		return ErrMutationConflict
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_observer_configuration_receipts
SET acknowledged = 1
WHERE source_authority = ? AND operation_id = ?`,
		string(expected.Authority), expected.Operation[:])
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	return tx.Commit()
}

// SourceObserverRootsPage returns one bounded root-ID-ordered page.
func (c *Catalog) SourceObserverRootsPage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	after string,
	limit int,
) (SourceObserverRootPage, error) {
	if authority == "" || limit < 1 || limit > SourceObserverConfigurationPageLimit {
		return SourceObserverRootPage{}, fmt.Errorf("%w: invalid source observer root page", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT root_id, generation, path, volume_uuid, root_inode,
       root_birthtime_sec, root_birthtime_nsec, root_kind
FROM source_observer_roots
WHERE source_authority = ? AND root_id > ?
ORDER BY root_id LIMIT ?`, string(authority), after, limit+1)
	if err != nil {
		return SourceObserverRootPage{}, err
	}
	defer func() { _ = rows.Close() }()
	page := SourceObserverRootPage{Records: make([]SourceObserverRootRecord, 0, limit)}
	for rows.Next() {
		if len(page.Records) == limit {
			page.Next = page.Records[len(page.Records)-1].ID
			break
		}
		var record SourceObserverRootRecord
		if err := rows.Scan(&record.ID, &record.Generation, &record.Path, &record.VolumeUUID,
			&record.Inode, &record.BirthSec, &record.BirthNsec, &record.Kind); err != nil {
			return SourceObserverRootPage{}, err
		}
		candidate := append(page.Records, record)
		if encoded, err := json.Marshal(SourceObserverRootPage{Records: candidate, Next: record.ID}); err != nil ||
			len(encoded) > SourceObserverConfigurationPageByteLimit {
			if err != nil {
				return SourceObserverRootPage{}, err
			}
			if len(page.Records) == 0 {
				return SourceObserverRootPage{}, fmt.Errorf("%w: oversized source observer root", ErrIntegrity)
			}
			page.Next = page.Records[len(page.Records)-1].ID
			break
		}
		page.Records = candidate
	}
	return page, rows.Err()
}

// SourceObserverCheckpointsPage returns one bounded stream-ID-ordered page.
func (c *Catalog) SourceObserverCheckpointsPage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	after string,
	limit int,
) (SourceObserverCheckpointPage, error) {
	if authority == "" || limit < 1 || limit > SourceObserverConfigurationPageLimit {
		return SourceObserverCheckpointPage{}, fmt.Errorf("%w: invalid source observer checkpoint page", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT stream_identity, root_epoch, native_event_id
FROM source_observer_checkpoints
WHERE source_authority = ? AND stream_identity > ?
ORDER BY stream_identity LIMIT ?`, string(authority), after, limit+1)
	if err != nil {
		return SourceObserverCheckpointPage{}, err
	}
	defer func() { _ = rows.Close() }()
	page := SourceObserverCheckpointPage{Records: make([]SourceObserverCheckpointRecord, 0, limit)}
	for rows.Next() {
		if len(page.Records) == limit {
			page.Next = page.Records[len(page.Records)-1].Stream
			break
		}
		var record SourceObserverCheckpointRecord
		if err := rows.Scan(&record.Stream, &record.RootEpoch, &record.EventID); err != nil {
			return SourceObserverCheckpointPage{}, err
		}
		candidate := append(page.Records, record)
		if encoded, err := json.Marshal(SourceObserverCheckpointPage{Records: candidate, Next: record.Stream}); err != nil ||
			len(encoded) > SourceObserverConfigurationPageByteLimit {
			if err != nil {
				return SourceObserverCheckpointPage{}, err
			}
			if len(page.Records) == 0 {
				return SourceObserverCheckpointPage{}, fmt.Errorf("%w: oversized source observer checkpoint", ErrIntegrity)
			}
			page.Next = page.Records[len(page.Records)-1].Stream
			break
		}
		page.Records = candidate
	}
	return page, rows.Err()
}

func readSourceObserverConfigurationRef(
	ctx context.Context,
	query interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
) (SourceObserverConfigurationRef, bool, error) {
	var ref SourceObserverConfigurationRef
	var digest []byte
	err := query.QueryRowContext(ctx, `
SELECT next_sequence, root_count, checkpoint_count, byte_count, rolling_digest
FROM source_observer_configuration_stages
WHERE source_authority = ? AND operation_id = ?`, string(authority), operation[:]).
		Scan(&ref.Sequence, &ref.Roots, &ref.Checkpoints, &ref.Bytes, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceObserverConfigurationRef{}, false, nil
	}
	if err != nil {
		return SourceObserverConfigurationRef{}, false, err
	}
	if len(digest) != sha256.Size {
		return SourceObserverConfigurationRef{}, false, ErrIntegrity
	}
	ref.Authority = authority
	ref.Operation = operation
	ref.Digest = bytesToDigest(digest)
	return ref, true, nil
}

func readSourceObserverConfigurationReceipt(
	ctx context.Context,
	tx *sql.Tx,
	expected SourceObserverConfigurationRef,
) (SourceObserverStreamRecord, bool, error) {
	var ref SourceObserverConfigurationRef
	var stageDigest, rootDigest, fleetDigest []byte
	var stream SourceObserverStreamRecord
	var mode uint8
	err := tx.QueryRowContext(ctx, `
SELECT fleet_owner_id, fleet_generation,
       sequence, root_count, checkpoint_count, byte_count, stage_digest,
       stream_identity, root_epoch, root_set_digest, fleet_digest,
       last_received_sequence, last_applied_sequence, state, quarantine_detail
FROM source_observer_configuration_receipts
WHERE source_authority = ? AND operation_id = ?`,
		string(expected.Authority), expected.Operation[:]).
		Scan(&stream.FleetOwner, &stream.FleetGeneration,
			&ref.Sequence, &ref.Roots, &ref.Checkpoints, &ref.Bytes, &stageDigest,
			&stream.Stream, &stream.RootEpoch, &rootDigest, &fleetDigest,
			&stream.LastReceived, &stream.LastApplied, &mode, &stream.Quarantine)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceObserverStreamRecord{}, false, nil
	}
	if err != nil {
		return SourceObserverStreamRecord{}, false, err
	}
	if ValidateSourceAuthorityFleetOwnerID(stream.FleetOwner) != nil || stream.FleetGeneration == 0 ||
		len(stageDigest) != sha256.Size || len(rootDigest) != sha256.Size ||
		len(fleetDigest) != sha256.Size ||
		SourceObserverMode(mode) < SourceObserverSnapshotRequired ||
		SourceObserverMode(mode) > SourceObserverStreamResetRequired {
		return SourceObserverStreamRecord{}, false, ErrIntegrity
	}
	ref.Authority = expected.Authority
	ref.Operation = expected.Operation
	ref.Digest = bytesToDigest(stageDigest)
	if ref != expected {
		return SourceObserverStreamRecord{}, false, ErrMutationConflict
	}
	stream.Authority = expected.Authority
	stream.RootDigest = bytesToDigest(rootDigest)
	stream.FleetDigest = bytesToDigest(fleetDigest)
	stream.Mode = SourceObserverMode(mode)
	return stream, true, nil
}

func sourceObserverConfigurationSetsChanged(
	ctx context.Context,
	tx *sql.Tx,
	ref SourceObserverConfigurationRef,
) (bool, error) {
	var changed int
	if err := tx.QueryRowContext(ctx, `
SELECT
  EXISTS(
    SELECT root_id, generation, path, volume_uuid, root_inode,
           root_birthtime_sec, root_birthtime_nsec, root_kind
    FROM source_observer_configuration_roots
    WHERE source_authority = ? AND operation_id = ?
    EXCEPT
    SELECT root_id, generation, path, volume_uuid, root_inode,
           root_birthtime_sec, root_birthtime_nsec, root_kind
    FROM source_observer_roots WHERE source_authority = ?
  )
  OR EXISTS(
    SELECT root_id, generation, path, volume_uuid, root_inode,
           root_birthtime_sec, root_birthtime_nsec, root_kind
    FROM source_observer_roots WHERE source_authority = ?
    EXCEPT
    SELECT root_id, generation, path, volume_uuid, root_inode,
           root_birthtime_sec, root_birthtime_nsec, root_kind
    FROM source_observer_configuration_roots
    WHERE source_authority = ? AND operation_id = ?
  )
  OR EXISTS(
    SELECT stream_identity, root_epoch, native_event_id
    FROM source_observer_configuration_checkpoints
    WHERE source_authority = ? AND operation_id = ?
    EXCEPT
    SELECT stream_identity, root_epoch, native_event_id
    FROM source_observer_checkpoints WHERE source_authority = ?
  )
  OR EXISTS(
    SELECT stream_identity, root_epoch, native_event_id
    FROM source_observer_checkpoints WHERE source_authority = ?
    EXCEPT
    SELECT stream_identity, root_epoch, native_event_id
    FROM source_observer_configuration_checkpoints
    WHERE source_authority = ? AND operation_id = ?
  )`,
		string(ref.Authority), ref.Operation[:], string(ref.Authority),
		string(ref.Authority), string(ref.Authority), ref.Operation[:],
		string(ref.Authority), ref.Operation[:], string(ref.Authority),
		string(ref.Authority), string(ref.Authority), ref.Operation[:]).Scan(&changed); err != nil {
		return false, err
	}
	return changed != 0, nil
}

// SourceObserverRootsDigest returns the ordered normalized root digest.
func SourceObserverRootsDigest(records []SourceObserverRootRecord) ([32]byte, error) {
	var digest [32]byte
	for index, record := range records {
		if err := validateSourceObserverRoot(record); err != nil ||
			(index > 0 && records[index-1].ID >= record.ID) {
			if err != nil {
				return [32]byte{}, err
			}
			return [32]byte{}, fmt.Errorf("%w: source observer roots are not exact and ordered", ErrInvalidObject)
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return [32]byte{}, err
		}
		digest = sourceObserverConfigurationRollingDigest(digest, sha256.Sum256(encoded))
	}
	return digest, nil
}

// SourceObserverCheckpointsDigest returns the ordered normalized checkpoint digest.
func SourceObserverCheckpointsDigest(records []SourceObserverCheckpointRecord) ([32]byte, error) {
	var digest [32]byte
	for index, record := range records {
		if err := validateSourceObserverCheckpoint(record); err != nil ||
			(index > 0 && records[index-1].Stream >= record.Stream) {
			if err != nil {
				return [32]byte{}, err
			}
			return [32]byte{}, fmt.Errorf("%w: source observer checkpoints are not exact and ordered", ErrInvalidObject)
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return [32]byte{}, err
		}
		digest = sourceObserverConfigurationRollingDigest(digest, sha256.Sum256(encoded))
	}
	return digest, nil
}

func sourceObserverConfigurationRollingDigest(prior, next [32]byte) [32]byte {
	value := make([]byte, 0, sha256.Size*2)
	value = append(value, prior[:]...)
	value = append(value, next[:]...)
	return sha256.Sum256(value)
}

func validateSourceObserverConfigurationIdentity(identity SourceObserverConfigurationIdentity) error {
	if identity.Authority == "" || ValidateSourceAuthorityFleetOwnerID(identity.FleetOwner) != nil ||
		identity.FleetGeneration == 0 || identity.Operation == (causal.OperationID{}) ||
		identity.Stream == "" || identity.RootEpoch == "" ||
		identity.RootDigest == ([32]byte{}) || identity.FleetDigest == ([32]byte{}) ||
		identity.RootCount == 0 || identity.CheckpointCount == 0 ||
		identity.RootCount > SourceObserverConfigurationRecordLimit ||
		identity.CheckpointCount > SourceObserverConfigurationRecordLimit ||
		identity.RootsDigest == ([32]byte{}) || identity.CheckpointsDigest == ([32]byte{}) {
		return fmt.Errorf("%w: incomplete source observer configuration identity", ErrInvalidObject)
	}
	return nil
}

func validateSourceObserverRoot(record SourceObserverRootRecord) error {
	if record.ID == "" || record.Generation == 0 || record.Path == "" ||
		record.VolumeUUID == "" || record.Inode == 0 || record.Kind < 1 || record.Kind > 2 ||
		record.BirthNsec < 0 || record.BirthNsec >= 1_000_000_000 {
		return fmt.Errorf("%w: invalid source observer root", ErrInvalidObject)
	}
	return nil
}

func validateSourceObserverCheckpoint(record SourceObserverCheckpointRecord) error {
	if record.Stream == "" || record.RootEpoch == "" {
		return fmt.Errorf("%w: invalid source observer checkpoint", ErrInvalidObject)
	}
	return nil
}
