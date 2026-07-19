package catalog

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/yasyf/fusekit/causal"
)

// ConvergenceEnginePageLimit is the fixed maximum number of records per state page.
const ConvergenceEnginePageLimit = 256

const (
	maxConvergenceEnginePages       = 65536
	maxConvergenceEnginePageBytes   = 4 << 20
	convergenceEngineProofRetention = 256
)

// ConvergenceEngineOperation identifies one exact staged state transition.
type ConvergenceEngineOperation [16]byte

// NewConvergenceEngineOperation allocates an opaque state transition identity.
func NewConvergenceEngineOperation() (ConvergenceEngineOperation, error) {
	var operation ConvergenceEngineOperation
	if _, err := io.ReadFull(rand.Reader, operation[:]); err != nil {
		return operation, fmt.Errorf("catalog: allocate convergence engine operation: %w", err)
	}
	return operation, nil
}

// ConvergenceEngineHeader is the CAS fence for normalized engine state.
type ConvergenceEngineHeader struct {
	Version  uint64
	Revision causal.Revision
}

// ConvergenceEngineHead is one authority ordering and deduplication watermark.
type ConvergenceEngineHead struct {
	Authority  causal.SourceAuthorityID
	Head       causal.Revision
	DedupFloor causal.Revision
}

// ConvergenceEngineDomain is one normalized domain state row.
type ConvergenceEngineDomain struct {
	Tenant                  causal.TenantID
	Domain                  causal.DomainID
	Generation              causal.Generation
	Fingerprint             [32]byte
	CatalogRevision         causal.CatalogRevision
	NotifiedCatalogRevision causal.CatalogRevision
	ObservedCatalogRevision causal.CatalogRevision
	Desired                 causal.Revision
	Notified                causal.Revision
	Observed                causal.Revision
	Demanded                bool
	Forced                  bool
	PendingSent             time.Time
	QuarantineSince         time.Time
	QuarantineUntil         time.Time
	Applicable              causal.ChangeSet
	DesiredChange           causal.ChangeSet
	NotifiedChange          causal.ChangeSet
	ObservedChange          causal.ChangeSet
}

// ConvergenceEngineChange is one bounded applied-change journal header.
type ConvergenceEngineChange struct {
	Change         causal.ChangeSet
	EngineRevision causal.Revision
	AffectedCount  uint64
	AffectedDigest [32]byte
}

// ConvergenceEngineOutbox is the normalized durable page cursor for one claim.
type ConvergenceEngineOutbox struct {
	Change         causal.ChangeSet
	Cursor         causal.OutboxCursor
	Settlement     *causal.OutboxSettlement
	EngineRevision causal.Revision
	CommitCount    uint64
	AffectedCount  uint64
	AffectedDigest [32]byte
}

// ConvergenceEngineCursor independently continues every normalized state relation.
type ConvergenceEngineCursor struct {
	AfterHead   causal.SourceAuthorityID
	AfterDomain causal.DomainID
	AfterChange causal.ChangeID
}

// ConvergenceEnginePage is one bounded recovery page.
type ConvergenceEnginePage struct {
	Header  ConvergenceEngineHeader
	Heads   []ConvergenceEngineHead
	Domains []ConvergenceEngineDomain
	Changes []ConvergenceEngineChange
	Outbox  *ConvergenceEngineOutbox
	Next    *ConvergenceEngineCursor
}

// ConvergenceEngineDeltaPage is one bounded staged transition fragment.
type ConvergenceEngineDeltaPage struct {
	UpsertHeads   []ConvergenceEngineHead
	DeleteHeads   []causal.SourceAuthorityID
	UpsertDomains []ConvergenceEngineDomain
	DeleteDomains []causal.DomainID
	UpsertChanges []ConvergenceEngineChange
	DeleteChanges []causal.ChangeID
	ResetOutbox   bool
	Outbox        *ConvergenceEngineOutbox
}

// ConvergenceEngineStage is one exact mutation page.
type ConvergenceEngineStage struct {
	Operation       ConvergenceEngineOperation
	ExpectedVersion uint64
	TargetRevision  causal.Revision
	Sequence        uint32
	PageCount       uint32
	Page            ConvergenceEngineDeltaPage
}

// ConvergenceEngineHead returns the current normalized state fence.
func (c *Catalog) ConvergenceEngineHead(ctx context.Context) (ConvergenceEngineHeader, error) {
	var header ConvergenceEngineHeader
	if err := c.readDB.QueryRowContext(ctx, `
SELECT version, revision FROM convergence_engine WHERE singleton = 1`,
	).Scan(&header.Version, &header.Revision); err != nil {
		return ConvergenceEngineHeader{}, fmt.Errorf("catalog: read convergence engine head: %w", err)
	}
	return header, nil
}

// StageConvergenceEngineMutation durably stages one bounded exact transition page.
func (c *Catalog) StageConvergenceEngineMutation(ctx context.Context, stage ConvergenceEngineStage) error {
	if stage.Operation == (ConvergenceEngineOperation{}) || stage.PageCount == 0 ||
		stage.PageCount > maxConvergenceEnginePages || stage.Sequence >= stage.PageCount ||
		convergenceDeltaCount(stage.Page) > ConvergenceEnginePageLimit ||
		(stage.Page.Outbox != nil && !stage.Page.ResetOutbox) {
		return fmt.Errorf("%w: invalid convergence engine stage", ErrInvalidObject)
	}
	payload, err := json.Marshal(stage.Page)
	if err != nil {
		return fmt.Errorf("catalog: encode convergence engine stage: %w", err)
	}
	if len(payload) > maxConvergenceEnginePageBytes {
		return fmt.Errorf("%w: convergence engine stage exceeds byte limit", ErrStorageQuota)
	}
	digest := sha256.Sum256(payload)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin convergence engine stage: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_engine_mutations(operation_id, expected_version, target_revision, page_count, state)
VALUES (?, ?, ?, ?, 1)
ON CONFLICT(operation_id) DO NOTHING`,
		stage.Operation[:], stage.ExpectedVersion, uint64(stage.TargetRevision), stage.PageCount,
	); err != nil {
		return fmt.Errorf("catalog: initialize convergence engine stage: %w", mapConstraint(err))
	}
	var expected, revision uint64
	var pageCount uint32
	var state uint8
	if err := tx.QueryRowContext(ctx, `
SELECT expected_version, target_revision, page_count, state
FROM convergence_engine_mutations WHERE operation_id = ?`, stage.Operation[:],
	).Scan(&expected, &revision, &pageCount, &state); err != nil {
		return fmt.Errorf("catalog: read convergence engine stage: %w", err)
	}
	if expected != stage.ExpectedVersion || revision != uint64(stage.TargetRevision) ||
		pageCount != stage.PageCount || state != 1 {
		return fmt.Errorf("%w: convergence engine operation was reused", ErrMutationConflict)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_engine_mutation_pages(operation_id, sequence, digest, payload)
VALUES (?, ?, ?, ?)
ON CONFLICT(operation_id, sequence) DO NOTHING`,
		stage.Operation[:], stage.Sequence, digest[:], payload,
	); err != nil {
		return fmt.Errorf("catalog: stage convergence engine page: %w", mapConstraint(err))
	}
	var storedDigest, storedPayload []byte
	if err := tx.QueryRowContext(ctx, `
SELECT digest, payload FROM convergence_engine_mutation_pages
WHERE operation_id = ? AND sequence = ?`, stage.Operation[:], stage.Sequence,
	).Scan(&storedDigest, &storedPayload); err != nil {
		return fmt.Errorf("catalog: verify convergence engine page: %w", err)
	}
	if !bytes.Equal(storedDigest, digest[:]) || !bytes.Equal(storedPayload, payload) {
		return fmt.Errorf("%w: convergence engine page changed on replay", ErrMutationConflict)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit convergence engine stage: %w", err)
	}
	return nil
}

// DiscardUnpublishedConvergenceEngineMutations removes crash-abandoned staging at one recovered fence.
func (c *Catalog) DiscardUnpublishedConvergenceEngineMutations(ctx context.Context, version uint64) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin abandoned convergence engine cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var current uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT version FROM convergence_engine WHERE singleton = 1`,
	).Scan(&current); err != nil {
		return fmt.Errorf("catalog: fence abandoned convergence engine cleanup: %w", err)
	}
	if current != version {
		return fmt.Errorf("%w: convergence engine version is %d, want %d", ErrStateConflict, current, version)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM convergence_engine_mutations WHERE state = 1`); err != nil {
		return fmt.Errorf("catalog: discard unpublished convergence engine mutations: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit abandoned convergence engine cleanup: %w", err)
	}
	return nil
}

// PublishConvergenceEngineMutation atomically installs every staged page.
func (c *Catalog) PublishConvergenceEngineMutation(
	ctx context.Context,
	operation ConvergenceEngineOperation,
) (ConvergenceEngineHeader, error) {
	if operation == (ConvergenceEngineOperation{}) {
		return ConvergenceEngineHeader{}, fmt.Errorf("%w: empty convergence engine operation", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return ConvergenceEngineHeader{}, fmt.Errorf("catalog: begin convergence engine publish: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var expected, target uint64
	var pageCount uint32
	var state uint8
	if err := tx.QueryRowContext(ctx, `
SELECT expected_version, target_revision, page_count, state
FROM convergence_engine_mutations WHERE operation_id = ?`, operation[:],
	).Scan(&expected, &target, &pageCount, &state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ConvergenceEngineHeader{}, ErrNotFound
		}
		return ConvergenceEngineHeader{}, fmt.Errorf("catalog: read convergence engine publish: %w", err)
	}
	var version, revision uint64
	if err := tx.QueryRowContext(ctx, `
SELECT version, revision FROM convergence_engine WHERE singleton = 1`,
	).Scan(&version, &revision); err != nil {
		return ConvergenceEngineHeader{}, fmt.Errorf("catalog: fence convergence engine publish: %w", err)
	}
	if state == 2 {
		if version != expected+1 || revision != target {
			return ConvergenceEngineHeader{}, fmt.Errorf("%w: published convergence engine proof changed", ErrIntegrity)
		}
		if err := tx.Commit(); err != nil {
			return ConvergenceEngineHeader{}, fmt.Errorf("catalog: finish replayed convergence engine publish: %w", err)
		}
		return ConvergenceEngineHeader{Version: version, Revision: causal.Revision(revision)}, nil
	}
	if version != expected {
		return ConvergenceEngineHeader{}, fmt.Errorf("%w: convergence engine version is %d, want %d", ErrStateConflict, version, expected)
	}
	var storedPageCount uint32
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM convergence_engine_mutation_pages
WHERE operation_id = ?`, operation[:]).Scan(&storedPageCount); err != nil {
		return ConvergenceEngineHeader{}, fmt.Errorf("catalog: count convergence engine pages: %w", err)
	}
	if storedPageCount != pageCount {
		return ConvergenceEngineHeader{}, fmt.Errorf("%w: convergence engine page count is incomplete", ErrIntegrity)
	}
	for sequence := uint32(0); sequence < pageCount; sequence++ {
		var payload []byte
		if err := tx.QueryRowContext(ctx, `
SELECT payload FROM convergence_engine_mutation_pages
WHERE operation_id = ? AND sequence = ?`, operation[:], sequence).Scan(&payload); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ConvergenceEngineHeader{}, fmt.Errorf("%w: convergence engine page sequence is incomplete", ErrIntegrity)
			}
			return ConvergenceEngineHeader{}, fmt.Errorf("catalog: read convergence engine page: %w", err)
		}
		var page ConvergenceEngineDeltaPage
		if err := decodeExactJSON(payload, &page); err != nil {
			return ConvergenceEngineHeader{}, err
		}
		if err := applyConvergenceEngineDelta(ctx, tx, page); err != nil {
			return ConvergenceEngineHeader{}, err
		}
	}
	result, err := tx.ExecContext(ctx, `
UPDATE convergence_engine SET version = version + 1, revision = ?
WHERE singleton = 1 AND version = ?`, target, expected)
	if err != nil {
		return ConvergenceEngineHeader{}, fmt.Errorf("catalog: publish convergence engine head: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return ConvergenceEngineHeader{}, fmt.Errorf("%w: convergence engine publish lost CAS", ErrStateConflict)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE convergence_engine_mutations SET state = 2 WHERE operation_id = ? AND state = 1`,
		operation[:]); err != nil {
		return ConvergenceEngineHeader{}, fmt.Errorf("catalog: settle convergence engine mutation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM convergence_engine_mutations
WHERE state = 2 AND rowid NOT IN (
	SELECT rowid FROM convergence_engine_mutations
	WHERE state = 2 ORDER BY rowid DESC LIMIT ?
)`, convergenceEngineProofRetention); err != nil {
		return ConvergenceEngineHeader{}, fmt.Errorf("catalog: compact convergence engine mutation proofs: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ConvergenceEngineHeader{}, fmt.Errorf("catalog: commit convergence engine publish: %w", err)
	}
	return ConvergenceEngineHeader{Version: expected + 1, Revision: causal.Revision(target)}, nil
}

func convergenceDeltaCount(page ConvergenceEngineDeltaPage) int {
	count := len(page.UpsertHeads) + len(page.DeleteHeads) + len(page.UpsertDomains) + len(page.DeleteDomains) +
		len(page.UpsertChanges) + len(page.DeleteChanges)
	if page.ResetOutbox {
		count++
	}
	return count
}

func decodeExactJSON(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: decode convergence engine page: %v", ErrIntegrity, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: convergence engine page has trailing data", ErrIntegrity)
	}
	return nil
}

func causalHeader(change causal.ChangeSet) causal.ChangeSet {
	change.AffectedKeys = nil
	return change
}

func convergenceBoolInt(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func unixNano(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}

func timeFromUnixNano(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}
