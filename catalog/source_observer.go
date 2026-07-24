package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/yasyf/fusekit/causal"
)

var (
	// ErrSourceObserverSnapshotRequired means the durable stream cannot continue incrementally.
	ErrSourceObserverSnapshotRequired = errors.New("catalog: source observer snapshot required")
	// ErrSourceObserverConflict means immutable observer state was replayed with different bytes.
	ErrSourceObserverConflict = errors.New("catalog: source observer conflict")
	// ErrSourceObserverInboxCoalesced means an observation was durably covered by a required snapshot instead of retained as payload.
	ErrSourceObserverInboxCoalesced = errors.New("catalog: source observer inbox coalesced")
)

const (
	sourceObserverInboxMaxRows   = 4096
	sourceObserverInboxMaxBytes  = 16 << 20
	sourceObserverInboxMaxEvents = 1 << 20

	sourceMutationWindowMaxRows   = 256
	sourceMutationWindowMaxBytes  = 4 << 20
	sourceMutationWindowMaxEvents = 1 << 16

	sourceSnapshotPhysicalPageLimit = 256
	sourceSnapshotPhysicalPageBytes = 1 << 20
	sourceSnapshotPhysicalMaxRows   = 10_000_000
	sourceSnapshotPhysicalMaxBytes  = 2 << 30

	sourceObserverBeforeRowSettlement = "source_observer.before_row_settlement"
)

// SourceObserverInboxPageLimit is the hard maximum retained correlation page.
const SourceObserverInboxPageLimit = 64

const (
	// SourceMutationExpectationPageLimit is the hard mutation-plan page maximum.
	SourceMutationExpectationPageLimit = 256
	// SourceMutationExpectationPageByteLimit is the hard encoded mutation-plan page maximum.
	SourceMutationExpectationPageByteLimit = 1 << 20
	// SourcePhysicalIndexPageLimit is the hard physical-index page maximum.
	SourcePhysicalIndexPageLimit = 256
	// SourcePhysicalIndexPageByteLimit is the hard encoded physical-index page maximum.
	SourcePhysicalIndexPageByteLimit = 1 << 20
	// SourcePhysicalIdentityByteLimit is the hard opaque physical-identity maximum.
	SourcePhysicalIdentityByteLimit = 4096
)

const (
	SourceMutationExpectationPlanned uint8 = iota + 1
	SourceMutationExpectationComplete
	SourceMutationExpectationArmed
	SourceMutationExpectationRepairRequired
	SourceMutationExpectationRepairPublished
)

// SourceObserverMode is the durable state of one authority observer.
type SourceObserverMode uint8

const (
	SourceObserverSnapshotRequired SourceObserverMode = iota + 1
	SourceObserverIncremental
	SourceObserverQuarantined
	SourceObserverStreamResetRequired
)

// SourceObserverRootRecord is one exact durable root declaration.
type SourceObserverRootRecord struct {
	ID         string
	Generation uint64
	Path       string
	VolumeUUID string
	Inode      uint64
	BirthSec   int64
	BirthNsec  int64
	Kind       uint8
}

// SourceObserverCheckpointRecord is one backend-native stream resume position.
type SourceObserverCheckpointRecord struct {
	Stream    string
	RootEpoch string
	EventID   uint64
}

// SourceObserverAppliedCheckpointRecord is one persisted per-stream applied watermark.
type SourceObserverAppliedCheckpointRecord struct {
	Stream          string
	RootEpoch       string
	EventID         uint64
	ReceivedEventID uint64
	Sequence        uint64
}

// SourceObserverStreamRecord is the durable cursor and repair state.
type SourceObserverStreamRecord struct {
	Authority       causal.SourceAuthorityID
	FleetOwner      SourceAuthorityFleetOwnerID
	FleetGeneration causal.Generation
	Stream          string
	RootEpoch       string
	RootDigest      [32]byte
	FleetDigest     [32]byte
	LastReceived    uint64
	LastApplied     uint64
	Mode            SourceObserverMode
	Quarantine      string
}

// SourceObserverInboxRecord is one immutable cursor batch.
type SourceObserverInboxRecord struct {
	Authority           causal.SourceAuthorityID
	Stream              string
	RootEpoch           string
	Sequence            uint64
	PredecessorSequence uint64
	NativePredecessor   uint64
	NativeCursor        uint64
	EventCount          uint64
	Digest              [32]byte
	Payload             []byte
}

// SourcePhysicalIndexRecord is one durable physical locator and its logical bindings.
type SourcePhysicalIndexRecord struct {
	Authority           causal.SourceAuthorityID
	RootID              string
	Relative            string
	FileIdentity        []byte
	Kind                uint8
	MetadataFingerprint [32]byte
	ContentFingerprint  [32]byte
	Logical             []string
	Payload             []byte
}

// SourceAuthorityBindingRecord retains one opaque source key independently of path lifetime.
type SourceAuthorityBindingRecord struct {
	Authority   causal.SourceAuthorityID
	LogicalID   string
	SourceKey   SourceObjectKey
	Fingerprint [32]byte
}

// SourceMutationExpectationRecord is one durable provider-write plan awaiting an exact echo.
type SourceMutationExpectationRecord struct {
	Operation     MutationID
	Authority     causal.SourceAuthorityID
	Tenant        TenantID
	Generation    Generation
	Origin        CausalOrigin
	Digest        [32]byte
	Payload       []byte
	ReceiptDigest [32]byte
	Receipt       []byte
	State         uint8
}

// SourceMutationExpectationPage is one bounded operation-ordered mutation-plan page.
type SourceMutationExpectationPage struct {
	Records []SourceMutationExpectationRecord
	Next    MutationID
}

// SourcePhysicalIndexPage is one bounded locator-ordered physical-index page.
type SourcePhysicalIndexPage struct {
	Records []SourcePhysicalIndexRecord
	Next    SourceIndexLocator
}

// SourceObserverState is the bounded durable authority control state.
type SourceObserverState struct {
	Stream             SourceObserverStreamRecord
	Roots              []SourceObserverRootRecord
	Checkpoints        []SourceObserverCheckpointRecord
	AppliedCheckpoints []SourceObserverAppliedCheckpointRecord
	Inbox              []SourceObserverInboxRecord
}

// SourceIndexLocator selects one physical index row.
type SourceIndexLocator struct {
	RootID   string
	Relative string
}

// SourceObserverSettlement atomically advances the applied cursor and derived observer state.
type SourceObserverSettlement struct {
	Authority causal.SourceAuthorityID
	Stream    string
	RootEpoch string
	Through   uint64
	Operation causal.OperationID
}

// SourceSnapshotSettlement atomically promotes one snapshot and its observer fence.
type SourceSnapshotSettlement struct {
	Fence             SourceObserverSettlement
	Snapshot          SourceSnapshotStageRef
	MismatchAllActive bool
}

// SourceSnapshotPage is one bounded page from a staged authoritative scan.
type SourceSnapshotPage struct {
	Records []SourcePhysicalIndexRecord
	Next    SourceIndexLocator
}

// SourceObserverInboxPage is one bounded retained mutation-correlation page.
type SourceObserverInboxPage struct {
	Records []SourceObserverInboxRecord
	Next    uint64
}

// SourceObserverStream returns one authority's durable stream fence.
func (c *Catalog) SourceObserverStream(ctx context.Context, authority causal.SourceAuthorityID) (SourceObserverStreamRecord, error) {
	if authority == "" {
		return SourceObserverStreamRecord{}, fmt.Errorf("%w: empty source authority", ErrInvalidObject)
	}
	stream, found, err := readSourceObserverStream(ctx, c.readDB, authority)
	if err != nil {
		return SourceObserverStreamRecord{}, err
	}
	if !found {
		return SourceObserverStreamRecord{}, ErrNotFound
	}
	return stream, nil
}

// SourceObserverNextInbox returns only the next unapplied durable event batch.
func (c *Catalog) SourceObserverNextInbox(ctx context.Context, authority causal.SourceAuthorityID, after uint64) (*SourceObserverInboxRecord, error) {
	var record SourceObserverInboxRecord
	var digest []byte
	err := c.readDB.QueryRowContext(ctx, `
SELECT inbox.stream_identity, inbox.root_epoch, inbox.sequence, inbox.predecessor_sequence,
       inbox.predecessor_event, inbox.through_event, inbox.event_count, inbox.payload_digest, inbox.payload
FROM source_observer_inbox AS inbox
WHERE inbox.source_authority = ? AND inbox.sequence > ?
  AND NOT EXISTS (
      SELECT 1 FROM source_observer_checkpoints AS checkpoint
      WHERE checkpoint.source_authority = inbox.source_authority
        AND checkpoint.stream_identity = inbox.stream_identity
        AND checkpoint.root_epoch = inbox.root_epoch
        AND checkpoint.applied_event_id >= inbox.through_event
  )
ORDER BY inbox.sequence LIMIT 1`, string(authority), after).Scan(
		&record.Stream, &record.RootEpoch, &record.Sequence, &record.PredecessorSequence,
		&record.NativePredecessor, &record.NativeCursor, &record.EventCount, &digest, &record.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(digest) != sha256.Size || sha256.Sum256(record.Payload) != bytesToDigest(digest) {
		return nil, fmt.Errorf("%w: corrupt source observer inbox", ErrIntegrity)
	}
	record.Authority = authority
	record.Digest = bytesToDigest(digest)
	return &record, nil
}

// SourceObserverInboxPage returns one bounded retained mutation-correlation page.
func (c *Catalog) SourceObserverInboxPage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	afterExclusive uint64,
	throughInclusive uint64,
	limit int,
) (SourceObserverInboxPage, error) {
	if authority == "" || throughInclusive < afterExclusive || limit < 1 || limit > SourceObserverInboxPageLimit {
		return SourceObserverInboxPage{}, fmt.Errorf("%w: invalid source observer inbox page", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT stream_identity, root_epoch, sequence, predecessor_sequence, predecessor_event, through_event, event_count, payload_digest, payload
FROM source_observer_inbox
WHERE source_authority = ? AND sequence > ? AND sequence <= ? ORDER BY sequence LIMIT ?`,
		string(authority), afterExclusive, throughInclusive, limit+1)
	if err != nil {
		return SourceObserverInboxPage{}, err
	}
	defer func() { _ = rows.Close() }()
	page := SourceObserverInboxPage{Records: make([]SourceObserverInboxRecord, 0, limit)}
	for rows.Next() {
		var record SourceObserverInboxRecord
		var digest []byte
		if err := rows.Scan(&record.Stream, &record.RootEpoch, &record.Sequence, &record.PredecessorSequence,
			&record.NativePredecessor, &record.NativeCursor, &record.EventCount, &digest, &record.Payload); err != nil {
			return SourceObserverInboxPage{}, err
		}
		if len(digest) != sha256.Size || sha256.Sum256(record.Payload) != bytesToDigest(digest) {
			return SourceObserverInboxPage{}, fmt.Errorf("%w: corrupt source observer inbox", ErrIntegrity)
		}
		record.Authority, record.Digest = authority, bytesToDigest(digest)
		if len(page.Records) == limit {
			page.Next = page.Records[len(page.Records)-1].Sequence
			break
		}
		page.Records = append(page.Records, record)
	}
	if err := rows.Err(); err != nil {
		return SourceObserverInboxPage{}, err
	}
	return page, nil
}

// AppendSourceObserverInbox durably appends one exact continuous event range.
func (c *Catalog) AppendSourceObserverInbox(ctx context.Context, record SourceObserverInboxRecord) (uint64, error) {
	discontinuity := record.NativeCursor == 0 && record.NativePredecessor == 0
	if record.Authority == "" || record.Stream == "" || record.RootEpoch == "" ||
		record.EventCount == 0 || (!discontinuity && record.NativePredecessor >= record.NativeCursor) ||
		len(record.Payload) == 0 || sha256.Sum256(record.Payload) != record.Digest {
		return 0, fmt.Errorf("%w: invalid source observer inbox record", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("catalog: begin source observer inbox append: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stream, found, err := readSourceObserverStream(ctx, tx, record.Authority)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrNotFound
	}
	if stream.Mode == SourceObserverQuarantined {
		return 0, fmt.Errorf("%w: %s", ErrIntegrity, stream.Quarantine)
	}
	var replaySequence, replayPredecessor, replayEvents uint64
	var replayEpoch string
	var replayDigest []byte
	err = tx.QueryRowContext(ctx, `
SELECT sequence, predecessor_event, root_epoch, event_count, payload_digest
FROM source_observer_inbox_receipts
WHERE source_authority = ? AND stream_identity = ? AND root_epoch = ? AND through_event = ?`,
		string(record.Authority), record.Stream, record.RootEpoch, record.NativeCursor).
		Scan(&replaySequence, &replayPredecessor, &replayEpoch, &replayEvents, &replayDigest)
	if err == nil {
		if replayPredecessor != record.NativePredecessor || replayEpoch != record.RootEpoch ||
			replayEvents != record.EventCount || !bytes.Equal(replayDigest, record.Digest[:]) {
			return 0, ErrSourceObserverConflict
		}
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("catalog: commit source observer inbox replay: %w", err)
		}
		return replaySequence, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("catalog: inspect source observer inbox replay: %w", err)
	}
	var checkpointEvent uint64
	var checkpointEpoch string
	checkpointErr := tx.QueryRowContext(ctx, `
SELECT root_epoch, native_event_id FROM source_observer_checkpoints
WHERE source_authority = ? AND stream_identity = ?`, string(record.Authority), record.Stream).Scan(&checkpointEpoch, &checkpointEvent)
	if checkpointErr != nil && !errors.Is(checkpointErr, sql.ErrNoRows) {
		return 0, fmt.Errorf("catalog: read native source observer checkpoint: %w", checkpointErr)
	}
	if stream.Mode == SourceObserverSnapshotRequired {
		if checkpointErr != nil || checkpointEpoch != record.RootEpoch {
			return 0, ErrSourceObserverConflict
		}
		switch checkpointEvent {
		case record.NativePredecessor:
			if _, err := tx.ExecContext(ctx, `
UPDATE source_observer_checkpoints SET native_event_id = ?
WHERE source_authority = ? AND stream_identity = ? AND native_event_id = ?`, record.NativeCursor,
				string(record.Authority), record.Stream, record.NativePredecessor); err != nil {
				return 0, fmt.Errorf("catalog: advance coalesced source observer checkpoint: %w", err)
			}
		case record.NativeCursor:
		default:
			if record.NativeCursor < checkpointEvent {
				break
			}
			return 0, ErrSourceObserverConflict
		}
		if err := requireSourceObserverSnapshotTx(ctx, tx, record.Authority); err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM source_observer_inbox WHERE source_authority = ?`, string(record.Authority)); err != nil {
			return 0, fmt.Errorf("catalog: clear coalesced source observer inbox: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE source_observer_streams SET last_received_sequence = last_applied_sequence WHERE source_authority = ?`,
			string(record.Authority)); err != nil {
			return 0, fmt.Errorf("catalog: reset coalesced source observer sequence: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("catalog: commit coalesced source observer observation: %w", err)
		}
		return 0, errors.Join(ErrSourceObserverSnapshotRequired, ErrSourceObserverInboxCoalesced)
	}
	if errors.Is(checkpointErr, sql.ErrNoRows) || (!discontinuity && (checkpointEpoch != record.RootEpoch || checkpointEvent != record.NativePredecessor)) {
		if err := requireSourceObserverSnapshotTx(ctx, tx, record.Authority); err != nil {
			return 0, err
		}
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("catalog: commit source observer discontinuity: %w", err)
		}
		return 0, ErrSourceObserverSnapshotRequired
	}
	if !discontinuity {
		var inboxRows, inboxBytes, inboxEvents uint64
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(SUM(length(payload)), 0), COALESCE(SUM(event_count), 0)
FROM source_observer_inbox WHERE source_authority = ?`, string(record.Authority)).Scan(&inboxRows, &inboxBytes, &inboxEvents); err != nil {
			return 0, fmt.Errorf("catalog: measure source observer inbox: %w", err)
		}
		var correlationActive int
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM source_mutation_expectations WHERE source_authority = ? AND state = 3)`,
			string(record.Authority)).Scan(&correlationActive); err != nil {
			return 0, fmt.Errorf("catalog: inspect source mutation correlation window: %w", err)
		}
		maxRows, maxBytes, maxEvents := uint64(sourceObserverInboxMaxRows), uint64(sourceObserverInboxMaxBytes), uint64(sourceObserverInboxMaxEvents)
		if correlationActive != 0 {
			maxRows, maxBytes, maxEvents = sourceMutationWindowMaxRows, sourceMutationWindowMaxBytes, sourceMutationWindowMaxEvents
		}
		if inboxRows+1 > maxRows || inboxBytes+uint64(len(record.Payload)) > maxBytes || inboxEvents+record.EventCount > maxEvents {
			if _, err := tx.ExecContext(ctx, `
UPDATE source_observer_checkpoints SET native_event_id = ?
WHERE source_authority = ? AND stream_identity = ? AND native_event_id = ?`, record.NativeCursor,
				string(record.Authority), record.Stream, record.NativePredecessor); err != nil {
				return 0, fmt.Errorf("catalog: advance overflow source observer checkpoint: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM source_observer_inbox WHERE source_authority = ?`, string(record.Authority)); err != nil {
				return 0, fmt.Errorf("catalog: coalesce overflowing source observer inbox: %w", err)
			}
			if err := requireSourceObserverSnapshotTx(ctx, tx, record.Authority); err != nil {
				return 0, fmt.Errorf("catalog: require source observer snapshot after inbox overflow: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return 0, fmt.Errorf("catalog: commit source observer inbox overflow: %w", err)
			}
			return 0, errors.Join(ErrSourceObserverSnapshotRequired, ErrSourceObserverInboxCoalesced)
		}
	}
	sequence := stream.LastReceived + 1
	result, err := tx.ExecContext(ctx, `
INSERT INTO source_observer_inbox(
    source_authority, sequence, predecessor_sequence, stream_identity,
    predecessor_event, through_event, root_epoch, event_count, payload_digest, payload
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(record.Authority), sequence, stream.LastReceived,
		record.Stream, record.NativePredecessor, record.NativeCursor, record.RootEpoch, record.EventCount, record.Digest[:], record.Payload)
	if err != nil {
		return 0, mapConstraint(err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("catalog: inspect source observer inbox append: %w", err)
	}
	if inserted != 1 {
		return 0, ErrSourceObserverConflict
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_observer_inbox_receipts(
    source_authority, sequence, predecessor_sequence, stream_identity,
    predecessor_event, through_event, root_epoch, event_count, payload_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(record.Authority), sequence, stream.LastReceived,
		record.Stream, record.NativePredecessor, record.NativeCursor, record.RootEpoch, record.EventCount, record.Digest[:]); err != nil {
		return 0, mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_observer_streams SET last_received_sequence = ? WHERE source_authority = ?`,
		sequence, string(record.Authority)); err != nil {
		return 0, fmt.Errorf("catalog: advance source observer received sequence: %w", err)
	}
	if discontinuity {
		if err := requireSourceObserverSnapshotTx(ctx, tx, record.Authority); err != nil {
			return 0, fmt.Errorf("catalog: persist source observer discontinuity: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE source_observer_streams SET state = ?
WHERE source_authority = ?`, uint8(SourceObserverStreamResetRequired), string(record.Authority)); err != nil {
			return 0, fmt.Errorf("catalog: require source observer stream replacement: %w", err)
		}
	} else {
		checkpointResult, err := tx.ExecContext(ctx, `
UPDATE source_observer_checkpoints SET native_event_id = ?
WHERE source_authority = ? AND stream_identity = ? AND native_event_id = ?`, record.NativeCursor,
			string(record.Authority), record.Stream, record.NativePredecessor)
		if err != nil {
			return 0, fmt.Errorf("catalog: advance native source observer checkpoint: %w", err)
		}
		if changed, _ := checkpointResult.RowsAffected(); changed != 1 {
			return 0, ErrSourceObserverConflict
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("catalog: commit source observer inbox append: %w", err)
	}
	return sequence, nil
}

// RequireSourceObserverSnapshot moves one authority to explicit repair state.
func (c *Catalog) RequireSourceObserverSnapshot(ctx context.Context, authority causal.SourceAuthorityID) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireSourceObserverSnapshotTx(ctx, tx, authority); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit required source observer snapshot: %w", err)
	}
	return nil
}

func requireSourceObserverSnapshotTx(ctx context.Context, tx *sql.Tx, authority causal.SourceAuthorityID) error {
	if authority == "" {
		return fmt.Errorf("%w: empty source authority", ErrInvalidObject)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_mutation_expectations SET state = ?
WHERE source_authority = ? AND state IN (?, ?, ?)`, SourceMutationExpectationRepairRequired, string(authority),
		SourceMutationExpectationPlanned, SourceMutationExpectationComplete, SourceMutationExpectationArmed); err != nil {
		return fmt.Errorf("catalog: fence source mutation expectations for snapshot repair: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_observer_streams SET
    state = CASE WHEN state = ? THEN state ELSE ? END,
    quarantine_detail = ''
WHERE source_authority = ?`, uint8(SourceObserverStreamResetRequired), uint8(SourceObserverSnapshotRequired),
		string(authority))
	if err != nil {
		return fmt.Errorf("catalog: require source observer snapshot: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrNotFound
	}
	return nil
}

// QuarantineSourceObserver records a fail-closed authority state.
func (c *Catalog) QuarantineSourceObserver(ctx context.Context, authority causal.SourceAuthorityID, detail string) error {
	if authority == "" || detail == "" {
		return fmt.Errorf("%w: incomplete source observer quarantine", ErrInvalidObject)
	}
	result, err := c.db.ExecContext(ctx, `
UPDATE source_observer_streams SET state = ?, quarantine_detail = ? WHERE source_authority = ?`,
		uint8(SourceObserverQuarantined), detail, string(authority))
	if err != nil {
		return fmt.Errorf("catalog: quarantine source observer: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrNotFound
	}
	return nil
}

// BeginSourceSnapshotStage replaces any abandoned candidate for one authority.
func (c *Catalog) BeginSourceSnapshotStage(ctx context.Context, authority causal.SourceAuthorityID, snapshot string) error {
	if authority == "" || snapshot == "" {
		return fmt.Errorf("%w: incomplete source snapshot stage", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source snapshot stage: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var prior string
	err = tx.QueryRowContext(ctx, `SELECT snapshot_id FROM source_snapshot_sessions WHERE source_authority = ?`,
		string(authority)).Scan(&prior)
	if err == nil {
		if err := abortSourceSnapshotPublicationTx(ctx, tx, authority, prior); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("catalog: inspect prior source snapshot stage: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM source_snapshot_stages WHERE source_authority = ?`, string(authority)); err != nil {
		return fmt.Errorf("catalog: clear source snapshot stage: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_snapshot_sessions(source_authority, snapshot_id) VALUES (?, ?)
ON CONFLICT(source_authority) DO UPDATE SET
    snapshot_id = excluded.snapshot_id, physical_count = 0, physical_bytes = 0`, string(authority), snapshot); err != nil {
		return mapConstraint(err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit source snapshot stage: %w", err)
	}
	return nil
}

// AbortSourceSnapshotStage discards exactly one still-owned snapshot candidate.
func (c *Catalog) AbortSourceSnapshotStage(ctx context.Context, authority causal.SourceAuthorityID, snapshot string) error {
	if authority == "" || snapshot == "" {
		return fmt.Errorf("%w: incomplete source snapshot abort", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source snapshot abort: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var current string
	err = tx.QueryRowContext(ctx, `SELECT snapshot_id FROM source_snapshot_sessions WHERE source_authority = ?`,
		string(authority)).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return tx.Commit()
	}
	if err != nil {
		return fmt.Errorf("catalog: inspect source snapshot abort: %w", err)
	}
	if current != snapshot {
		return ErrSourceObserverConflict
	}
	if err := abortSourceSnapshotPublicationTx(ctx, tx, authority, snapshot); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM source_snapshot_stages WHERE source_authority = ? AND snapshot_id = ?`,
		string(authority), snapshot); err != nil {
		return fmt.Errorf("catalog: discard source snapshot stage: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM source_snapshot_sessions WHERE source_authority = ? AND snapshot_id = ?`,
		string(authority), snapshot); err != nil {
		return fmt.Errorf("catalog: discard source snapshot session: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit source snapshot abort: %w", err)
	}
	return nil
}

// AppendSourceSnapshotStagePage appends one bounded, ordered candidate page.
func (c *Catalog) AppendSourceSnapshotStagePage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	snapshot string,
	page SourceSnapshotPage,
) error {
	records := page.Records
	if authority == "" || snapshot == "" || len(records) == 0 || len(records) > sourceSnapshotPhysicalPageLimit {
		return fmt.Errorf("%w: incomplete source snapshot page", ErrInvalidObject)
	}
	encoded, err := json.Marshal(page)
	if err != nil || len(encoded) > sourceSnapshotPhysicalPageBytes {
		return fmt.Errorf("%w: source snapshot physical page exceeds byte quota", ErrInvalidObject)
	}
	last := records[len(records)-1]
	if page.Next != (SourceIndexLocator{RootID: last.RootID, Relative: last.Relative}) {
		return fmt.Errorf("%w: source snapshot page cursor mismatch", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source snapshot page: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var current string
	if err := tx.QueryRowContext(ctx, `SELECT snapshot_id FROM source_snapshot_sessions WHERE source_authority = ?`,
		string(authority)).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSourceObserverConflict
		}
		return err
	}
	if current != snapshot {
		return ErrSourceObserverConflict
	}
	var prior SourceIndexLocator
	err = tx.QueryRowContext(ctx, `
SELECT root_id, relative_path FROM source_snapshot_stages
WHERE source_authority = ? AND snapshot_id = ?
ORDER BY root_id DESC, relative_path DESC LIMIT 1`,
		string(authority), snapshot).Scan(&prior.RootID, &prior.Relative)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && compareSourceIndexLocator(prior, SourceIndexLocator{
		RootID: records[0].RootID, Relative: records[0].Relative,
	}) >= 0 {
		return ErrInvalidTransition
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_snapshot_sessions SET
    physical_count = physical_count + ?, physical_bytes = physical_bytes + ?
WHERE source_authority = ? AND snapshot_id = ?
  AND physical_count + ? <= ? AND physical_bytes + ? <= ?`,
		len(records), len(encoded), string(authority), snapshot,
		len(records), sourceSnapshotPhysicalMaxRows, len(encoded), sourceSnapshotPhysicalMaxBytes)
	if err != nil {
		return mapConstraint(err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return fmt.Errorf("%w: source snapshot physical stage exceeds cumulative quota", ErrInvalidObject)
	}
	for index, record := range records {
		if record.Authority != authority || record.RootID == "" || record.Relative == "" ||
			len(record.FileIdentity) == 0 || len(record.FileIdentity) > SourcePhysicalIdentityByteLimit ||
			record.Kind < 1 || record.Kind > 3 || len(record.Payload) == 0 ||
			(index > 0 && compareSourceIndexRecord(records[index-1], record) >= 0) {
			return fmt.Errorf("%w: invalid source snapshot page", ErrInvalidObject)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_snapshot_stages(
    source_authority, snapshot_id, root_id, relative_path, file_identity,
    physical_kind, metadata_fingerprint, content_fingerprint, payload
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(authority), snapshot, record.RootID, record.Relative,
			record.FileIdentity, record.Kind, record.MetadataFingerprint[:], record.ContentFingerprint[:], record.Payload); err != nil {
			return mapConstraint(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit source snapshot page: %w", err)
	}
	return nil
}

// SourceSnapshotStagePage returns at most limit staged rows after an exact locator.
func (c *Catalog) SourceSnapshotStagePage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	snapshot string,
	after SourceIndexLocator,
	limit int,
) (SourceSnapshotPage, error) {
	if authority == "" || snapshot == "" || limit < 1 || limit > sourceSnapshotPhysicalPageLimit {
		return SourceSnapshotPage{}, fmt.Errorf("%w: invalid source snapshot page request", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT root_id, relative_path, file_identity, physical_kind, metadata_fingerprint,
       content_fingerprint, payload
FROM source_snapshot_stages
WHERE source_authority = ? AND snapshot_id = ?
  AND (root_id > ? OR (root_id = ? AND relative_path > ?))
ORDER BY root_id, relative_path LIMIT ?`, string(authority), snapshot, after.RootID, after.RootID, after.Relative, limit+1)
	if err != nil {
		return SourceSnapshotPage{}, fmt.Errorf("catalog: page source snapshot stage: %w", err)
	}
	defer func() { _ = rows.Close() }()
	page := SourceSnapshotPage{Records: make([]SourcePhysicalIndexRecord, 0, limit)}
	for rows.Next() {
		if len(page.Records) == limit {
			last := page.Records[len(page.Records)-1]
			page.Next = SourceIndexLocator{RootID: last.RootID, Relative: last.Relative}
			break
		}
		record, err := scanSourcePhysicalRecord(rows, authority)
		if err != nil {
			return SourceSnapshotPage{}, err
		}
		candidate := append(page.Records, record)
		encoded, err := json.Marshal(SourceSnapshotPage{Records: candidate})
		if err != nil {
			return SourceSnapshotPage{}, err
		}
		if len(encoded) > sourceSnapshotPhysicalPageBytes {
			if len(page.Records) == 0 {
				return SourceSnapshotPage{}, fmt.Errorf("%w: oversized source snapshot stage record", ErrIntegrity)
			}
			last := page.Records[len(page.Records)-1]
			page.Next = SourceIndexLocator{RootID: last.RootID, Relative: last.Relative}
			break
		}
		page.Records = candidate
	}
	if err := rows.Err(); err != nil {
		return SourceSnapshotPage{}, err
	}
	return page, nil
}

// BindSourceSnapshotStage records logical inputs without loading the snapshot into memory.
func (c *Catalog) BindSourceSnapshotStage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	snapshot string,
	bindings map[SourceIndexLocator][]string,
) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for locator, logical := range bindings {
		slices.Sort(logical)
		logical = slices.Compact(logical)
		for _, id := range logical {
			if id == "" {
				return fmt.Errorf("%w: empty source logical binding", ErrInvalidObject)
			}
		}
		result, err := tx.ExecContext(ctx, `
UPDATE source_snapshot_stages SET relative_path = relative_path
WHERE source_authority = ? AND snapshot_id = ? AND root_id = ? AND relative_path = ?`,
			string(authority), snapshot, locator.RootID, locator.Relative)
		if err != nil {
			return err
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return ErrSourceObserverConflict
		}
		if _, err := tx.ExecContext(ctx, `
DELETE FROM source_snapshot_logical
WHERE source_authority = ? AND snapshot_id = ? AND root_id = ? AND relative_path = ?`,
			string(authority), snapshot, locator.RootID, locator.Relative); err != nil {
			return fmt.Errorf("catalog: replace staged source logical bindings: %w", err)
		}
		for _, id := range logical {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO source_snapshot_logical(source_authority, snapshot_id, logical_id, root_id, relative_path)
VALUES (?, ?, ?, ?, ?)`, string(authority), snapshot, id, locator.RootID, locator.Relative); err != nil {
				return mapConstraint(err)
			}
		}
	}
	return tx.Commit()
}

// ReserveSourceAuthorityBinding returns one persistent opaque key for a logical identity.
func (c *Catalog) ReserveSourceAuthorityBinding(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	logical string,
	key SourceObjectKey,
) (SourceAuthorityBindingRecord, error) {
	if authority == "" || logical == "" || key == "" {
		return SourceAuthorityBindingRecord{}, fmt.Errorf("%w: incomplete source authority binding", ErrInvalidObject)
	}
	zero := [32]byte{}
	if _, err := c.db.ExecContext(ctx, `
INSERT INTO source_authority_bindings(source_authority, logical_id, source_key, effective_fingerprint)
VALUES (?, ?, ?, ?) ON CONFLICT(source_authority, logical_id) DO NOTHING`, string(authority), logical, string(key), zero[:]); err != nil {
		return SourceAuthorityBindingRecord{}, mapConstraint(err)
	}
	var stored string
	var fingerprint []byte
	if err := c.readDB.QueryRowContext(ctx, `
SELECT source_key, effective_fingerprint FROM source_authority_bindings
WHERE source_authority = ? AND logical_id = ?`, string(authority), logical).Scan(&stored, &fingerprint); err != nil {
		return SourceAuthorityBindingRecord{}, fmt.Errorf("catalog: read source authority binding: %w", err)
	}
	if len(fingerprint) != sha256.Size {
		return SourceAuthorityBindingRecord{}, fmt.Errorf("%w: corrupt source authority binding", ErrIntegrity)
	}
	var digest [32]byte
	copy(digest[:], fingerprint)
	return SourceAuthorityBindingRecord{Authority: authority, LogicalID: logical, SourceKey: SourceObjectKey(stored), Fingerprint: digest}, nil
}

// SettleSourceObserver atomically advances derived state after an idempotent catalog apply.
func (c *Catalog) SettleSourceObserver(ctx context.Context, settlement SourceObserverSettlement) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source observer settlement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := c.settleSourceObserverTx(ctx, tx, settlement); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit source observer settlement: %w", err)
	}
	return nil
}

func (c *Catalog) settleSourceObserverTx(
	ctx context.Context,
	tx *sql.Tx,
	settlement SourceObserverSettlement,
) error {
	if settlement.Authority == "" || settlement.Stream == "" || settlement.RootEpoch == "" ||
		settlement.Operation == (causal.OperationID{}) {
		return fmt.Errorf("%w: incomplete source observer settlement", ErrInvalidObject)
	}
	stream, found, err := readSourceObserverStream(ctx, tx, settlement.Authority)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	if stream.Stream != settlement.Stream || stream.RootEpoch != settlement.RootEpoch ||
		settlement.Through == 0 || settlement.Through > stream.LastReceived {
		return ErrSourceObserverConflict
	}
	var recordStream, recordEpoch string
	var predecessor, cursor uint64
	var recordedOperation []byte
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT stream_identity, root_epoch, predecessor_event, through_event, settlement_operation_id
FROM source_observer_inbox_receipts
WHERE source_authority = ? AND sequence = ?`, string(settlement.Authority), settlement.Through).
		Scan(&recordStream, &recordEpoch, &predecessor, &cursor, &recordedOperation); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSourceObserverConflict
		}
		return err
	}
	var appliedEvent uint64
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT applied_event_id FROM source_observer_checkpoints
WHERE source_authority = ? AND stream_identity = ? AND root_epoch = ?`,
		string(settlement.Authority), recordStream, recordEpoch).Scan(&appliedEvent); err != nil {
		return err
	}
	switch {
	case appliedEvent >= cursor:
		if bytes.Equal(recordedOperation, settlement.Operation[:]) {
			return nil
		}
		return ErrSourceObserverConflict
	case appliedEvent != predecessor:
		return ErrSourceObserverConflict
	}
	if len(recordedOperation) != 0 && !bytes.Equal(recordedOperation, settlement.Operation[:]) {
		return ErrSourceObserverConflict
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_observer_inbox_receipts
SET settlement_operation_id = ?
WHERE source_authority = ? AND sequence = ? AND settlement_operation_id IS NULL`,
		settlement.Operation[:], string(settlement.Authority), settlement.Through)
	if err != nil {
		return fmt.Errorf("catalog: record source observer settlement proof: %w", err)
	}
	if len(recordedOperation) == 0 {
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			if err != nil {
				return err
			}
			return ErrSourceObserverConflict
		}
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `
UPDATE source_observer_checkpoints
SET applied_event_id = ?, applied_sequence = ?
WHERE source_authority = ? AND stream_identity = ? AND root_epoch = ?
  AND applied_event_id = ?`, cursor, settlement.Through, string(settlement.Authority),
		recordStream, recordEpoch, predecessor)
	if err != nil {
		return fmt.Errorf("catalog: advance source observer stream watermark: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return err
		}
		return ErrSourceObserverConflict
	}
	var firstUnapplied sql.NullInt64
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `
SELECT MIN(inbox.sequence)
FROM source_observer_inbox AS inbox
JOIN source_observer_checkpoints AS checkpoint
  ON checkpoint.source_authority = inbox.source_authority
 AND checkpoint.stream_identity = inbox.stream_identity
 AND checkpoint.root_epoch = inbox.root_epoch
WHERE inbox.source_authority = ?
  AND inbox.through_event > checkpoint.applied_event_id`, string(settlement.Authority)).Scan(&firstUnapplied); err != nil {
		return err
	}
	contiguous := stream.LastReceived
	if firstUnapplied.Valid {
		contiguous = uint64(firstUnapplied.Int64 - 1)
	}
	contiguous = max(contiguous, stream.LastApplied)
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_observer_inbox
WHERE source_authority = ?
  AND EXISTS (
      SELECT 1 FROM source_observer_checkpoints AS checkpoint
      WHERE checkpoint.source_authority = source_observer_inbox.source_authority
        AND checkpoint.stream_identity = source_observer_inbox.stream_identity
        AND checkpoint.root_epoch = source_observer_inbox.root_epoch
        AND checkpoint.applied_event_id >= source_observer_inbox.through_event
  )
  AND NOT EXISTS (
      SELECT 1 FROM source_mutation_expectations WHERE source_authority = ?
  )`, string(settlement.Authority), string(settlement.Authority)); err != nil {
		return fmt.Errorf("catalog: settle source observer inbox: %w", err)
	}
	if err := c.sourceObserverSettlementStatement(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE source_observer_streams SET
    last_received_sequence = ?,
    last_applied_sequence = ?,
    state = CASE WHEN state = ? THEN state ELSE ? END,
    quarantine_detail = ''
WHERE source_authority = ?`, stream.LastReceived, contiguous, uint8(SourceObserverStreamResetRequired),
		uint8(SourceObserverIncremental), string(settlement.Authority)); err != nil {
		return fmt.Errorf("catalog: advance source observer applied cursor: %w", err)
	}
	return nil
}

// CompleteSourceMutationRepair clears one published repair only after worker-journal cleanup.
func (c *Catalog) CompleteSourceMutationRepair(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation MutationID,
) error {
	if authority == "" || operation == (MutationID{}) {
		return fmt.Errorf("%w: invalid source mutation repair completion", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var state uint8
	err = tx.QueryRowContext(ctx, `
SELECT state FROM source_mutation_expectations WHERE operation_id = ? AND source_authority = ?`,
		operation[:], string(authority)).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	if state != SourceMutationExpectationRepairPublished {
		return ErrSourceObserverConflict
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_mutation_expectations WHERE operation_id = ? AND source_authority = ? AND state = ?`,
		operation[:], string(authority), SourceMutationExpectationRepairPublished); err != nil {
		return fmt.Errorf("catalog: clear published source mutation repair: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_observer_inbox
WHERE source_authority = ?
  AND EXISTS (
      SELECT 1 FROM source_observer_checkpoints AS checkpoint
      WHERE checkpoint.source_authority = source_observer_inbox.source_authority
        AND checkpoint.stream_identity = source_observer_inbox.stream_identity
        AND checkpoint.root_epoch = source_observer_inbox.root_epoch
        AND checkpoint.applied_event_id >= source_observer_inbox.through_event
  )
  AND NOT EXISTS (
    SELECT 1 FROM source_mutation_expectations WHERE source_authority = ?
)`, string(authority), string(authority)); err != nil {
		return fmt.Errorf("catalog: clear retained repaired source inbox: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit source mutation repair completion: %w", err)
	}
	return nil
}

// PutSourceMutationExpectation durably reserves one exact mutation plan before dispatch.
func (c *Catalog) PutSourceMutationExpectation(ctx context.Context, record SourceMutationExpectationRecord) error {
	if record.Operation == (MutationID{}) || record.Authority == "" || record.Tenant == "" || record.Generation == 0 ||
		len(record.Payload) == 0 || sha256.Sum256(record.Payload) != record.Digest {
		return fmt.Errorf("%w: invalid source mutation expectation", ErrInvalidObject)
	}
	if encoded, err := json.Marshal(record); err != nil || len(encoded) > SourceMutationExpectationPageByteLimit {
		return fmt.Errorf("%w: source mutation expectation exceeds byte quota", ErrInvalidObject)
	}
	origin, err := json.Marshal(record.Origin)
	if err != nil {
		return err
	}
	result, err := c.db.ExecContext(ctx, `
INSERT INTO source_mutation_expectations(
    operation_id, source_authority, tenant, generation, causal_origin, payload_digest, payload,
    receipt_digest, receipt, state
) VALUES (?, ?, ?, ?, ?, ?, ?, X'', X'', ?) ON CONFLICT(operation_id) DO NOTHING`, record.Operation[:], string(record.Authority),
		string(record.Tenant), uint64(record.Generation), origin, record.Digest[:], record.Payload, SourceMutationExpectationPlanned)
	if err != nil {
		return mapConstraint(err)
	}
	inserted, err := result.RowsAffected()
	if err != nil || inserted == 1 {
		return err
	}
	existing, err := c.SourceMutationExpectation(ctx, record.Authority, record.Operation)
	if err != nil {
		return err
	}
	if existing.Tenant != record.Tenant || existing.Generation != record.Generation ||
		existing.Origin.Cause != record.Origin.Cause ||
		existing.Origin.Domain != record.Origin.Domain || existing.Origin.Generation != record.Origin.Generation ||
		existing.Digest != record.Digest || !bytes.Equal(existing.Payload, record.Payload) {
		return ErrSourceObserverConflict
	}
	return nil
}

// CompleteSourceMutationExpectation stores the exact runtime-owned post-state receipt.
func (c *Catalog) CompleteSourceMutationExpectation(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation MutationID,
	receipt []byte,
) error {
	if authority == "" || operation == (MutationID{}) || len(receipt) == 0 {
		return fmt.Errorf("%w: invalid source mutation receipt", ErrInvalidObject)
	}
	if len(receipt) > SourceMutationExpectationPageByteLimit {
		return fmt.Errorf("%w: source mutation receipt exceeds byte quota", ErrInvalidObject)
	}
	digest := sha256.Sum256(receipt)
	result, err := c.db.ExecContext(ctx, `
UPDATE source_mutation_expectations SET receipt_digest = ?, receipt = ?, state = ?
WHERE operation_id = ? AND source_authority = ? AND state = ? AND length(receipt) = 0`,
		digest[:], receipt, SourceMutationExpectationComplete, operation[:], string(authority), SourceMutationExpectationPlanned)
	if err != nil {
		return fmt.Errorf("catalog: complete source mutation expectation: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed == 1 {
		return nil
	}
	record, err := c.SourceMutationExpectation(ctx, authority, operation)
	if err != nil {
		return err
	}
	if record.State < 2 || record.ReceiptDigest != digest || !bytes.Equal(record.Receipt, receipt) {
		return ErrSourceObserverConflict
	}
	return nil
}

// RecoverSourceMutationExpectationReceipt records a lost-response worker receipt
// without reopening an expectation already fenced for snapshot repair.
func (c *Catalog) RecoverSourceMutationExpectationReceipt(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation MutationID,
	receipt []byte,
) error {
	if authority == "" || operation == (MutationID{}) || len(receipt) == 0 {
		return fmt.Errorf("%w: invalid recovered source mutation receipt", ErrInvalidObject)
	}
	if len(receipt) > SourceMutationExpectationPageByteLimit {
		return fmt.Errorf("%w: recovered source mutation receipt exceeds byte quota", ErrInvalidObject)
	}
	digest := sha256.Sum256(receipt)
	result, err := c.db.ExecContext(ctx, `
UPDATE source_mutation_expectations
SET receipt_digest = ?, receipt = ?,
    state = CASE WHEN state = ? THEN ? ELSE state END
WHERE operation_id = ? AND source_authority = ? AND length(receipt) = 0
  AND state IN (?, ?, ?)`,
		digest[:], receipt, SourceMutationExpectationPlanned, SourceMutationExpectationComplete,
		operation[:], string(authority), SourceMutationExpectationPlanned,
		SourceMutationExpectationRepairRequired, SourceMutationExpectationRepairPublished)
	if err != nil {
		return fmt.Errorf("catalog: recover source mutation expectation receipt: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed == 1 {
		return nil
	}
	record, err := c.SourceMutationExpectation(ctx, authority, operation)
	if err != nil {
		return err
	}
	if record.ReceiptDigest != digest || !bytes.Equal(record.Receipt, receipt) ||
		(record.State != SourceMutationExpectationComplete && record.State != SourceMutationExpectationRepairRequired &&
			record.State != SourceMutationExpectationRepairPublished) {
		return ErrSourceObserverConflict
	}
	return nil
}

// SourceMutationExpectation returns one exact mutation plan.
func (c *Catalog) SourceMutationExpectation(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation MutationID,
) (SourceMutationExpectationRecord, error) {
	if authority == "" || operation == (MutationID{}) {
		return SourceMutationExpectationRecord{}, fmt.Errorf("%w: incomplete source mutation expectation", ErrInvalidObject)
	}
	record, err := scanSourceMutationExpectation(c.readDB.QueryRowContext(ctx, `
SELECT operation_id, tenant, generation, causal_origin, payload_digest, payload, receipt_digest, receipt, state
FROM source_mutation_expectations
WHERE source_authority = ? AND operation_id = ?`, string(authority), operation[:]), authority)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceMutationExpectationRecord{}, ErrNotFound
	}
	return record, err
}

// SourceMutationExpectationsPage returns one bounded operation-ordered mutation-plan page.
func (c *Catalog) SourceMutationExpectationsPage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	after MutationID,
	limit int,
) (SourceMutationExpectationPage, error) {
	if authority == "" || limit < 1 || limit > SourceMutationExpectationPageLimit {
		return SourceMutationExpectationPage{}, fmt.Errorf("%w: invalid source mutation expectation page", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT operation_id, tenant, generation, causal_origin, payload_digest, payload, receipt_digest, receipt, state
FROM source_mutation_expectations
WHERE source_authority = ? AND operation_id > ?
ORDER BY operation_id LIMIT ?`, string(authority), after[:], limit+1)
	if err != nil {
		return SourceMutationExpectationPage{}, fmt.Errorf("catalog: page source mutation expectations: %w", err)
	}
	defer func() { _ = rows.Close() }()
	page := SourceMutationExpectationPage{Records: make([]SourceMutationExpectationRecord, 0, limit)}
	for rows.Next() {
		if len(page.Records) == limit {
			page.Next = page.Records[len(page.Records)-1].Operation
			break
		}
		record, err := scanSourceMutationExpectation(rows, authority)
		if err != nil {
			return SourceMutationExpectationPage{}, err
		}
		candidate := append(page.Records, record)
		encoded, err := json.Marshal(SourceMutationExpectationPage{Records: candidate})
		if err != nil {
			return SourceMutationExpectationPage{}, err
		}
		if len(encoded) > SourceMutationExpectationPageByteLimit {
			if len(page.Records) == 0 {
				return SourceMutationExpectationPage{}, fmt.Errorf("%w: oversized source mutation expectation", ErrIntegrity)
			}
			page.Next = page.Records[len(page.Records)-1].Operation
			break
		}
		page.Records = candidate
	}
	if err := rows.Err(); err != nil {
		return SourceMutationExpectationPage{}, err
	}
	return page, nil
}

func scanSourceMutationExpectation(
	row interface{ Scan(...any) error },
	authority causal.SourceAuthorityID,
) (SourceMutationExpectationRecord, error) {
	var rawOperation, rawOrigin, digest, payload, receiptDigest, receipt []byte
	var tenant string
	var generation uint64
	var state uint8
	if err := row.Scan(
		&rawOperation, &tenant, &generation, &rawOrigin, &digest, &payload, &receiptDigest, &receipt, &state,
	); err != nil {
		return SourceMutationExpectationRecord{}, err
	}
	receiptAbsent := len(receiptDigest) == 0 && len(receipt) == 0
	receiptValid := len(receiptDigest) == sha256.Size && len(receipt) > 0 &&
		sha256.Sum256(receipt) == bytesToDigest(receiptDigest)
	if len(rawOperation) != len(MutationID{}) || len(digest) != sha256.Size ||
		sha256.Sum256(payload) != bytesToDigest(digest) ||
		state < SourceMutationExpectationPlanned || state > SourceMutationExpectationRepairPublished ||
		(!receiptAbsent && !receiptValid) {
		return SourceMutationExpectationRecord{}, fmt.Errorf("%w: corrupt source mutation expectation", ErrIntegrity)
	}
	var operation MutationID
	copy(operation[:], rawOperation)
	var origin CausalOrigin
	if err := json.Unmarshal(rawOrigin, &origin); err != nil {
		return SourceMutationExpectationRecord{}, fmt.Errorf("%w: corrupt source mutation origin", ErrIntegrity)
	}
	return SourceMutationExpectationRecord{
		Operation: operation, Authority: authority, Tenant: TenantID(tenant), Generation: Generation(generation),
		Origin: origin, Digest: bytesToDigest(digest), Payload: append([]byte(nil), payload...),
		ReceiptDigest: bytesToDigest(receiptDigest), Receipt: append([]byte(nil), receipt...), State: state,
	}, nil
}

// SourceObserverBindingForKey resolves one opaque source key.
func (c *Catalog) SourceObserverBindingForKey(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	key SourceObjectKey,
) (SourceAuthorityBindingRecord, error) {
	if authority == "" || !validSourceKey(key) {
		return SourceAuthorityBindingRecord{}, fmt.Errorf("%w: incomplete source authority binding", ErrInvalidObject)
	}
	var logical string
	var fingerprint []byte
	if err := c.readDB.QueryRowContext(ctx, `
SELECT logical_id, effective_fingerprint FROM source_authority_bindings
WHERE source_authority = ? AND source_key = ?`, string(authority), string(key)).Scan(&logical, &fingerprint); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SourceAuthorityBindingRecord{}, ErrNotFound
		}
		return SourceAuthorityBindingRecord{}, err
	}
	if len(fingerprint) != sha256.Size {
		return SourceAuthorityBindingRecord{}, fmt.Errorf("%w: corrupt source authority binding", ErrIntegrity)
	}
	return SourceAuthorityBindingRecord{
		Authority: authority, LogicalID: logical, SourceKey: key, Fingerprint: bytesToDigest(fingerprint),
	}, nil
}

// SourceObserverBindingIndexPage returns one bounded locator-ordered physical page for a source key.
func (c *Catalog) SourceObserverBindingIndexPage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	key SourceObjectKey,
	after SourceIndexLocator,
	limit int,
) (SourcePhysicalIndexPage, error) {
	binding, err := c.SourceObserverBindingForKey(ctx, authority, key)
	if err != nil {
		return SourcePhysicalIndexPage{}, err
	}
	if limit < 1 || limit > SourcePhysicalIndexPageLimit {
		return SourcePhysicalIndexPage{}, fmt.Errorf("%w: invalid source binding index page", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT physical.root_id, physical.relative_path, physical.file_identity, physical.physical_kind,
       physical.metadata_fingerprint, physical.content_fingerprint, physical.payload
FROM source_physical_logical logical
JOIN source_physical_index physical
  ON physical.source_authority = logical.source_authority
 AND physical.root_id = logical.root_id
 AND physical.relative_path = logical.relative_path
WHERE logical.source_authority = ? AND logical.logical_id = ?
  AND (logical.root_id > ? OR (logical.root_id = ? AND logical.relative_path > ?))
ORDER BY logical.root_id, logical.relative_path
LIMIT ?`, string(authority), binding.LogicalID, after.RootID, after.RootID, after.Relative, limit+1)
	if err != nil {
		return SourcePhysicalIndexPage{}, err
	}
	defer func() { _ = rows.Close() }()
	page := SourcePhysicalIndexPage{Records: make([]SourcePhysicalIndexRecord, 0, limit)}
	for rows.Next() {
		if len(page.Records) == limit {
			last := page.Records[len(page.Records)-1]
			page.Next = SourceIndexLocator{RootID: last.RootID, Relative: last.Relative}
			break
		}
		record, err := scanSourcePhysicalRecord(rows, authority)
		if err != nil {
			return SourcePhysicalIndexPage{}, err
		}
		if err := c.loadSourcePhysicalLogicals(ctx, &record); err != nil {
			return SourcePhysicalIndexPage{}, err
		}
		candidate := append(page.Records, record)
		encoded, err := json.Marshal(SourcePhysicalIndexPage{Records: candidate})
		if err != nil {
			return SourcePhysicalIndexPage{}, err
		}
		if len(encoded) > SourcePhysicalIndexPageByteLimit {
			if len(page.Records) == 0 {
				return SourcePhysicalIndexPage{}, fmt.Errorf("%w: oversized source physical index record", ErrIntegrity)
			}
			last := page.Records[len(page.Records)-1]
			page.Next = SourceIndexLocator{RootID: last.RootID, Relative: last.Relative}
			break
		}
		page.Records = candidate
	}
	if err := rows.Err(); err != nil {
		return SourcePhysicalIndexPage{}, err
	}
	return page, nil
}

// SourceWatermark returns the exact catalog source head, or zero before the first snapshot.
func (c *Catalog) SourceWatermark(ctx context.Context, authority causal.SourceAuthorityID) (causal.Revision, error) {
	var revision uint64
	err := c.readDB.QueryRowContext(ctx, `
SELECT source_revision FROM source_watermarks WHERE source_authority = ?`, string(authority)).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("catalog: read source watermark: %w", err)
	}
	return causal.Revision(revision), nil
}

type observerRow interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readSourceObserverStream(ctx context.Context, query observerRow, authority causal.SourceAuthorityID) (SourceObserverStreamRecord, bool, error) {
	var stream SourceObserverStreamRecord
	var rootDigest, fleetDigest []byte
	var mode uint8
	err := query.QueryRowContext(ctx, `
SELECT fleet_owner_id, fleet_generation, stream_identity, root_epoch, root_set_digest, fleet_digest,
       last_received_sequence, last_applied_sequence, state, quarantine_detail
FROM source_observer_streams WHERE source_authority = ?`, string(authority)).Scan(
		&stream.FleetOwner, &stream.FleetGeneration, &stream.Stream, &stream.RootEpoch,
		&rootDigest, &fleetDigest, &stream.LastReceived, &stream.LastApplied, &mode, &stream.Quarantine)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceObserverStreamRecord{}, false, nil
	}
	if err != nil {
		return SourceObserverStreamRecord{}, false, fmt.Errorf("catalog: read source observer stream: %w", err)
	}
	if ValidateSourceAuthorityFleetOwnerID(stream.FleetOwner) != nil || stream.FleetGeneration == 0 ||
		len(rootDigest) != sha256.Size || len(fleetDigest) != sha256.Size ||
		mode < uint8(SourceObserverSnapshotRequired) || mode > uint8(SourceObserverStreamResetRequired) ||
		stream.LastApplied > stream.LastReceived {
		return SourceObserverStreamRecord{}, false, fmt.Errorf("%w: corrupt source observer stream", ErrIntegrity)
	}
	stream.Authority = authority
	stream.RootDigest = bytesToDigest(rootDigest)
	stream.FleetDigest = bytesToDigest(fleetDigest)
	stream.Mode = SourceObserverMode(mode)
	return stream, true, nil
}

// SourcePhysicalIndexRecordsPage returns one bounded locator-ordered physical-index page.
func (c *Catalog) SourcePhysicalIndexRecordsPage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	after SourceIndexLocator,
	limit int,
) (SourcePhysicalIndexPage, error) {
	if authority == "" || limit < 1 || limit > SourcePhysicalIndexPageLimit {
		return SourcePhysicalIndexPage{}, fmt.Errorf("%w: invalid source physical index page", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT root_id, relative_path, file_identity, physical_kind, metadata_fingerprint,
       content_fingerprint, payload
FROM source_physical_index
WHERE source_authority = ?
  AND (root_id > ? OR (root_id = ? AND relative_path > ?))
ORDER BY root_id, relative_path
LIMIT ?`, string(authority), after.RootID, after.RootID, after.Relative, limit+1)
	if err != nil {
		return SourcePhysicalIndexPage{}, err
	}
	defer func() { _ = rows.Close() }()
	page := SourcePhysicalIndexPage{Records: make([]SourcePhysicalIndexRecord, 0, limit)}
	for rows.Next() {
		if len(page.Records) == limit {
			last := page.Records[len(page.Records)-1]
			page.Next = SourceIndexLocator{RootID: last.RootID, Relative: last.Relative}
			break
		}
		record, err := scanSourcePhysicalRecord(rows, authority)
		if err != nil {
			return SourcePhysicalIndexPage{}, err
		}
		if err := c.loadSourcePhysicalLogicals(ctx, &record); err != nil {
			return SourcePhysicalIndexPage{}, err
		}
		candidate := append(page.Records, record)
		encoded, err := json.Marshal(SourcePhysicalIndexPage{Records: candidate})
		if err != nil {
			return SourcePhysicalIndexPage{}, err
		}
		if len(encoded) > SourcePhysicalIndexPageByteLimit {
			if len(page.Records) == 0 {
				return SourcePhysicalIndexPage{}, fmt.Errorf("%w: oversized source physical index record", ErrIntegrity)
			}
			last := page.Records[len(page.Records)-1]
			page.Next = SourceIndexLocator{RootID: last.RootID, Relative: last.Relative}
			break
		}
		page.Records = candidate
	}
	if err := rows.Err(); err != nil {
		return SourcePhysicalIndexPage{}, err
	}
	return page, nil
}

// SourcePhysicalIndexRecordByIdentity resolves one globally unique physical identity.
func (c *Catalog) SourcePhysicalIndexRecordByIdentity(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	identity []byte,
) (SourcePhysicalIndexRecord, error) {
	if authority == "" || len(identity) == 0 || len(identity) > SourcePhysicalIdentityByteLimit {
		return SourcePhysicalIndexRecord{}, fmt.Errorf("%w: incomplete source physical identity", ErrInvalidObject)
	}
	record, err := scanSourcePhysicalRecord(c.readDB.QueryRowContext(ctx, `
SELECT root_id, relative_path, file_identity, physical_kind, metadata_fingerprint,
       content_fingerprint, payload
FROM source_physical_index
WHERE source_authority = ? AND file_identity = ?`, string(authority), identity), authority)
	if errors.Is(err, sql.ErrNoRows) {
		return SourcePhysicalIndexRecord{}, ErrNotFound
	}
	if err == nil {
		err = c.loadSourcePhysicalLogicals(ctx, &record)
	}
	return record, err
}

func (c *Catalog) loadSourcePhysicalLogicals(ctx context.Context, record *SourcePhysicalIndexRecord) error {
	rows, err := c.readDB.QueryContext(ctx, `
SELECT logical_id FROM source_physical_logical
WHERE source_authority = ? AND root_id = ? AND relative_path = ? ORDER BY logical_id`,
		string(record.Authority), record.RootID, record.Relative)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var logical string
		if err := rows.Scan(&logical); err != nil {
			return err
		}
		record.Logical = append(record.Logical, logical)
	}
	return rows.Err()
}

type sourcePhysicalScanner interface {
	Scan(...any) error
}

func scanSourcePhysicalRecord(scanner sourcePhysicalScanner, authority causal.SourceAuthorityID) (SourcePhysicalIndexRecord, error) {
	var record SourcePhysicalIndexRecord
	var metadata, content []byte
	if err := scanner.Scan(&record.RootID, &record.Relative, &record.FileIdentity, &record.Kind,
		&metadata, &content, &record.Payload); err != nil {
		return SourcePhysicalIndexRecord{}, err
	}
	if len(metadata) != sha256.Size || len(content) != sha256.Size ||
		record.Kind < 1 || record.Kind > 3 || len(record.Payload) == 0 ||
		len(record.FileIdentity) == 0 || len(record.FileIdentity) > SourcePhysicalIdentityByteLimit {
		return SourcePhysicalIndexRecord{}, fmt.Errorf("%w: corrupt source physical index", ErrIntegrity)
	}
	record.Authority = authority
	record.MetadataFingerprint = bytesToDigest(metadata)
	record.ContentFingerprint = bytesToDigest(content)
	return record, nil
}

func compareSourceIndexRecord(left, right SourcePhysicalIndexRecord) int {
	if left.RootID < right.RootID {
		return -1
	}
	if left.RootID > right.RootID {
		return 1
	}
	if left.Relative < right.Relative {
		return -1
	}
	if left.Relative > right.Relative {
		return 1
	}
	return 0
}

func bytesToDigest(value []byte) (result [32]byte) {
	copy(result[:], value)
	return result
}
