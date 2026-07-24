package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

const (
	// FileProviderMaterializationPageLimit bounds one durable snapshot stage page.
	FileProviderMaterializationPageLimit = 1_000
	materializationSnapshotCollecting    = 1
	materializationSnapshotCommitted     = 2
	materializationSnapshotSuperseded    = 3
)

// FileProviderMaterializationIdentity fences one system backing store incarnation.
type FileProviderMaterializationIdentity struct {
	Tenant               TenantID
	Domain               causal.DomainID
	Generation           Generation
	Snapshot             MaterializationSnapshotID
	BackingStoreIdentity []byte
}

// FileProviderMaterializationPage is one canonical bounded container-ID page.
type FileProviderMaterializationPage struct {
	Identity FileProviderMaterializationIdentity
	Sequence uint32
	IDs      []ObjectID
}

// FileProviderMaterializationCommit atomically publishes one complete staged set.
type FileProviderMaterializationCommit struct {
	Identity  FileProviderMaterializationIdentity
	PageCount uint32
}

// FileProviderMaterializationResult is one idempotent set publication outcome.
type FileProviderMaterializationResult struct {
	Revision uint64
	Added    uint64
	Removed  uint64
}

// BeginFileProviderMaterializationSnapshot durably captures one collection fence.
func (c *Catalog) BeginFileProviderMaterializationSnapshot(
	ctx context.Context,
	identity FileProviderMaterializationIdentity,
) (uint64, error) {
	if err := validateFileProviderMaterializationIdentity(identity); err != nil {
		return 0, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("catalog: begin File Provider materialization snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateFileProviderMaterializationRoute(ctx, tx, identity); err != nil {
		return 0, err
	}
	stored, found, err := readFileProviderMaterializationSnapshot(ctx, tx, identity.Snapshot)
	if err != nil {
		return 0, err
	}
	if found {
		if !sameFileProviderMaterializationIdentity(stored.identity, identity) {
			return 0, ErrMutationConflict
		}
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("catalog: finish materialization snapshot replay: %w", err)
		}
		return stored.epoch, nil
	}
	var epoch uint64
	err = tx.QueryRowContext(ctx, `
INSERT INTO file_provider_materialization_owners(
    tenant, domain_id, generation, backing_store_identity, latest_epoch
) VALUES (?, ?, ?, ?, 1)
ON CONFLICT(tenant) DO UPDATE SET
    backing_store_identity = excluded.backing_store_identity,
    latest_epoch = file_provider_materialization_owners.latest_epoch + 1
WHERE file_provider_materialization_owners.domain_id = excluded.domain_id
  AND file_provider_materialization_owners.generation = excluded.generation
RETURNING latest_epoch`, string(identity.Tenant), string(identity.Domain),
		uint64(identity.Generation), identity.BackingStoreIdentity).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrGenerationMismatch
	}
	if err != nil {
		return 0, fmt.Errorf("catalog: allocate materialization snapshot epoch: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE file_provider_materialization_snapshots
SET state = ?
WHERE tenant = ? AND domain_id = ? AND generation = ? AND state = ? AND epoch < ?`,
		materializationSnapshotSuperseded, string(identity.Tenant), string(identity.Domain),
		uint64(identity.Generation), materializationSnapshotCollecting, epoch); err != nil {
		return 0, fmt.Errorf("catalog: supersede prior materialization snapshot: %w", err)
	}
	initialDigest := initialMaterializationSetDigest()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO file_provider_materialization_snapshots(
    snapshot_id, epoch, tenant, domain_id, generation, backing_store_identity,
    state, page_count, item_count, last_page_count, last_container_id,
    committed_revision, added_count, removed_count, set_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, X'', 0, 0, 0, ?)`,
		identity.Snapshot[:], epoch, string(identity.Tenant), string(identity.Domain),
		uint64(identity.Generation), identity.BackingStoreIdentity, materializationSnapshotCollecting,
		initialDigest[:]); err != nil {
		return 0, mapConstraint(err)
	}
	changed, err := tx.ExecContext(ctx, `
UPDATE file_provider_materialization_heads SET eligible = 0
WHERE tenant = ? AND domain_id = ? AND generation = ? AND eligible = 1
  AND backing_store_identity <> ?`, string(identity.Tenant), string(identity.Domain),
		uint64(identity.Generation), identity.BackingStoreIdentity)
	if err != nil {
		return 0, fmt.Errorf("catalog: fence replaced materialization identity: %w", err)
	}
	if rows, _ := changed.RowsAffected(); rows != 0 {
		if err := advanceTenantTargetingRevision(ctx, tx, identity.Tenant); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("catalog: commit File Provider materialization snapshot: %w", err)
	}
	return epoch, nil
}

// SuspendFileProviderMaterialization makes retained membership ineligible while
// the system backing-store identity is unavailable.
func (c *Catalog) SuspendFileProviderMaterialization(
	ctx context.Context,
	tenant TenantID,
	domain causal.DomainID,
	generation Generation,
) error {
	identity := FileProviderMaterializationIdentity{Tenant: tenant, Domain: domain, Generation: generation}
	if tenant == "" || domain == "" || generation == 0 {
		return fmt.Errorf("%w: incomplete File Provider materialization route", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin materialization suspension: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateFileProviderMaterializationRoute(ctx, tx, identity); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO file_provider_materialization_owners(
    tenant, domain_id, generation, backing_store_identity, latest_epoch
) VALUES (?, ?, ?, X'', 1)
ON CONFLICT(tenant) DO UPDATE SET
    backing_store_identity = X'',
    latest_epoch = file_provider_materialization_owners.latest_epoch + 1
WHERE file_provider_materialization_owners.domain_id = excluded.domain_id
  AND file_provider_materialization_owners.generation = excluded.generation`,
		string(tenant), string(domain), uint64(generation)); err != nil {
		return fmt.Errorf("catalog: fence unavailable backing store: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE file_provider_materialization_snapshots
SET state = ?
WHERE tenant = ? AND domain_id = ? AND generation = ? AND state = ?`,
		materializationSnapshotSuperseded, string(tenant), string(domain), uint64(generation),
		materializationSnapshotCollecting); err != nil {
		return fmt.Errorf("catalog: supersede unavailable materialization snapshots: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE file_provider_materialization_heads SET eligible = 0
WHERE tenant = ? AND domain_id = ? AND generation = ? AND eligible = 1`,
		string(tenant), string(domain), uint64(generation))
	if err != nil {
		return fmt.Errorf("catalog: suspend File Provider materialization: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 0 {
		if err := advanceTenantTargetingRevision(ctx, tx, tenant); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit materialization suspension: %w", err)
	}
	return nil
}

// StageFileProviderMaterializationPage durably appends one immutable page.
func (c *Catalog) StageFileProviderMaterializationPage(
	ctx context.Context,
	page FileProviderMaterializationPage,
) error {
	if err := validateFileProviderMaterializationIdentity(page.Identity); err != nil {
		return err
	}
	if len(page.IDs) > FileProviderMaterializationPageLimit {
		return fmt.Errorf("%w: materialization page exceeds limit", ErrInvalidObject)
	}
	for index, id := range page.IDs {
		if zeroObjectID(id) || (index != 0 && bytes.Compare(page.IDs[index-1][:], id[:]) >= 0) {
			return fmt.Errorf("%w: materialization page is not strictly ordered", ErrInvalidObject)
		}
	}
	digest := materializationPageDigest(page.IDs)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin materialization page: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateFileProviderMaterializationRoute(ctx, tx, page.Identity); err != nil {
		return err
	}
	stored, found, err := readFileProviderMaterializationSnapshot(ctx, tx, page.Identity.Snapshot)
	if err != nil {
		return err
	}
	if !found || !sameFileProviderMaterializationIdentity(stored.identity, page.Identity) {
		return ErrNotFound
	}
	var existing []byte
	err = tx.QueryRowContext(ctx, `
SELECT page_hash FROM file_provider_materialization_snapshot_pages
WHERE snapshot_id = ? AND sequence = ?`, page.Identity.Snapshot[:], page.Sequence).Scan(&existing)
	if err == nil {
		if !bytes.Equal(existing, digest[:]) {
			return ErrMutationConflict
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("catalog: read materialization page replay: %w", err)
	}
	if stored.state != materializationSnapshotCollecting {
		return ErrInvalidTransition
	}
	if stored.pageCount != page.Sequence {
		return fmt.Errorf("%w: materialization page sequence is not contiguous", ErrInvalidTransition)
	}
	if page.Sequence != 0 && stored.lastPageCount != FileProviderMaterializationPageLimit {
		return fmt.Errorf("%w: nonterminal materialization page is not full", ErrInvalidTransition)
	}
	if page.Sequence != 0 && len(page.IDs) == 0 {
		return fmt.Errorf("%w: only an empty materialization set may have an empty page", ErrInvalidObject)
	}
	if len(page.IDs) != 0 && len(stored.lastContainerID) != 0 {
		if bytes.Compare(stored.lastContainerID, page.IDs[0][:]) >= 0 {
			return fmt.Errorf("%w: materialization pages are not globally ordered", ErrInvalidObject)
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO file_provider_materialization_snapshot_pages(snapshot_id, sequence, page_hash, item_count)
VALUES (?, ?, ?, ?)`, page.Identity.Snapshot[:], page.Sequence, digest[:], len(page.IDs)); err != nil {
		return mapConstraint(err)
	}
	for _, id := range page.IDs {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO file_provider_materialization_snapshot_items(snapshot_id, sequence, container_id)
VALUES (?, ?, ?)`, page.Identity.Snapshot[:], page.Sequence, id[:]); err != nil {
			return mapConstraint(err)
		}
	}
	if err := validateStagedMaterializedContainerPage(ctx, tx, page.Identity, page.Sequence, uint64(len(page.IDs))); err != nil {
		return err
	}
	nextDigest := extendMaterializationSetDigest(stored.setDigest, page.Sequence, page.IDs)
	lastContainer := []byte{}
	if len(page.IDs) != 0 {
		lastContainer = page.IDs[len(page.IDs)-1][:]
	}
	result, err := tx.ExecContext(ctx, `
UPDATE file_provider_materialization_snapshots
SET page_count = page_count + 1, item_count = item_count + ?, last_page_count = ?,
    last_container_id = ?, set_digest = ?
WHERE snapshot_id = ? AND state = ? AND page_count = ?`, len(page.IDs), len(page.IDs),
		lastContainer, nextDigest[:], page.Identity.Snapshot[:], materializationSnapshotCollecting, page.Sequence)
	if err != nil {
		return fmt.Errorf("catalog: advance materialization snapshot accumulator: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit materialization page: %w", err)
	}
	return nil
}

// CommitFileProviderMaterializationSnapshot atomically replaces one complete set.
func (c *Catalog) CommitFileProviderMaterializationSnapshot(
	ctx context.Context,
	commit FileProviderMaterializationCommit,
) (FileProviderMaterializationResult, error) {
	if err := validateFileProviderMaterializationIdentity(commit.Identity); err != nil {
		return FileProviderMaterializationResult{}, err
	}
	if commit.PageCount == 0 {
		return FileProviderMaterializationResult{}, fmt.Errorf("%w: materialization snapshot has no terminal page", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return FileProviderMaterializationResult{}, fmt.Errorf("catalog: begin materialization commit: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateFileProviderMaterializationRoute(ctx, tx, commit.Identity); err != nil {
		return FileProviderMaterializationResult{}, err
	}
	stored, found, err := readFileProviderMaterializationSnapshot(ctx, tx, commit.Identity.Snapshot)
	if err != nil {
		return FileProviderMaterializationResult{}, err
	}
	if !found || !sameFileProviderMaterializationIdentity(stored.identity, commit.Identity) {
		return FileProviderMaterializationResult{}, ErrNotFound
	}
	if stored.state == materializationSnapshotCommitted {
		if stored.pageCount != commit.PageCount {
			return FileProviderMaterializationResult{}, ErrMutationConflict
		}
		if err := tx.Commit(); err != nil {
			return FileProviderMaterializationResult{}, err
		}
		return FileProviderMaterializationResult{Revision: stored.revision, Added: stored.added, Removed: stored.removed}, nil
	}
	var newestEpoch uint64
	if err := tx.QueryRowContext(ctx, `
SELECT latest_epoch FROM file_provider_materialization_owners
WHERE tenant = ? AND domain_id = ? AND generation = ?`, string(commit.Identity.Tenant),
		string(commit.Identity.Domain), uint64(commit.Identity.Generation)).Scan(&newestEpoch); err != nil {
		return FileProviderMaterializationResult{}, fmt.Errorf("catalog: read newest materialization epoch: %w", err)
	}
	if newestEpoch != stored.epoch {
		return FileProviderMaterializationResult{}, ErrGenerationMismatch
	}
	if stored.pageCount != commit.PageCount {
		return FileProviderMaterializationResult{}, fmt.Errorf("%w: materialization snapshot pages are incomplete", ErrInvalidTransition)
	}
	expectedItemCount := uint64(stored.lastPageCount)
	if stored.pageCount > 1 {
		if stored.lastPageCount == 0 {
			return FileProviderMaterializationResult{}, fmt.Errorf("%w: materialization snapshot has an empty trailing page", ErrIntegrity)
		}
		expectedItemCount += uint64(stored.pageCount-1) * uint64(FileProviderMaterializationPageLimit)
	}
	if stored.itemCount != expectedItemCount {
		return FileProviderMaterializationResult{}, fmt.Errorf("%w: materialization snapshot item count is inconsistent", ErrIntegrity)
	}
	setDigest := stored.setDigest
	head, headFound, err := readFileProviderMaterializationHead(ctx, tx, commit.Identity.Tenant)
	if err != nil {
		return FileProviderMaterializationResult{}, err
	}
	if headFound && head.epoch > stored.epoch {
		return FileProviderMaterializationResult{}, ErrGenerationMismatch
	}
	sameBinding := headFound && head.domain == commit.Identity.Domain &&
		head.generation == commit.Identity.Generation &&
		bytes.Equal(head.backingStoreIdentity, commit.Identity.BackingStoreIdentity)
	unchanged := sameBinding && head.eligible && head.setDigest == setDigest
	result := FileProviderMaterializationResult{}
	if unchanged {
		result.Revision = head.revision
	} else {
		result.Revision = 1
		if headFound {
			result.Revision = head.revision + 1
		}
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM file_provider_materialized_containers current
WHERE current.tenant = ? AND NOT EXISTS (
    SELECT 1 FROM file_provider_materialization_snapshot_items staged
    WHERE staged.snapshot_id = ? AND staged.container_id = current.container_id
)`, string(commit.Identity.Tenant), commit.Identity.Snapshot[:]).Scan(&result.Removed); err != nil {
			return FileProviderMaterializationResult{}, fmt.Errorf("catalog: count retired materialized containers: %w", err)
		}
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM file_provider_materialization_snapshot_items staged
WHERE staged.snapshot_id = ? AND NOT EXISTS (
    SELECT 1 FROM file_provider_materialized_containers current
    WHERE current.tenant = ? AND current.container_id = staged.container_id
)`, commit.Identity.Snapshot[:], string(commit.Identity.Tenant)).Scan(&result.Added); err != nil {
			return FileProviderMaterializationResult{}, fmt.Errorf("catalog: count added materialized containers: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
DELETE FROM file_provider_materialized_containers
WHERE tenant = ? AND NOT EXISTS (
    SELECT 1 FROM file_provider_materialization_snapshot_items staged
    WHERE staged.snapshot_id = ?
      AND staged.container_id = file_provider_materialized_containers.container_id
)`, string(commit.Identity.Tenant), commit.Identity.Snapshot[:]); err != nil {
			return FileProviderMaterializationResult{}, fmt.Errorf("catalog: retire absent materialized containers: %w", err)
		}
		if !sameBinding {
			if _, err := tx.ExecContext(ctx, `
UPDATE file_provider_materialized_containers
SET domain_id = ?, generation = ?, backing_store_identity = ?
WHERE tenant = ? AND EXISTS (
    SELECT 1 FROM file_provider_materialization_snapshot_items staged
    WHERE staged.snapshot_id = ?
      AND staged.container_id = file_provider_materialized_containers.container_id
)`, string(commit.Identity.Domain), uint64(commit.Identity.Generation),
				commit.Identity.BackingStoreIdentity, string(commit.Identity.Tenant),
				commit.Identity.Snapshot[:]); err != nil {
				return FileProviderMaterializationResult{}, fmt.Errorf("catalog: rebind retained materialized containers: %w", err)
			}
		}
		_, err := tx.ExecContext(ctx, `
INSERT INTO file_provider_materialized_containers(
    tenant, domain_id, generation, backing_store_identity, container_id
)
SELECT ?, ?, ?, ?, staged.container_id
FROM file_provider_materialization_snapshot_items staged
WHERE staged.snapshot_id = ? AND NOT EXISTS (
    SELECT 1 FROM file_provider_materialized_containers current
    WHERE current.tenant = ? AND current.container_id = staged.container_id
)`, string(commit.Identity.Tenant), string(commit.Identity.Domain), uint64(commit.Identity.Generation),
			commit.Identity.BackingStoreIdentity, commit.Identity.Snapshot[:],
			string(commit.Identity.Tenant))
		if err != nil {
			return FileProviderMaterializationResult{}, mapConstraint(err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO file_provider_materialization_heads(
    tenant, domain_id, generation, backing_store_identity, snapshot_id, epoch, revision, set_digest, eligible
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
ON CONFLICT(tenant) DO UPDATE SET
    domain_id = excluded.domain_id, generation = excluded.generation,
    backing_store_identity = excluded.backing_store_identity, snapshot_id = excluded.snapshot_id,
    epoch = excluded.epoch, revision = excluded.revision, set_digest = excluded.set_digest, eligible = 1`,
			string(commit.Identity.Tenant), string(commit.Identity.Domain), uint64(commit.Identity.Generation),
			commit.Identity.BackingStoreIdentity, commit.Identity.Snapshot[:], stored.epoch,
			result.Revision, setDigest[:]); err != nil {
			return FileProviderMaterializationResult{}, mapConstraint(err)
		}
		if err := advanceTenantTargetingRevision(ctx, tx, commit.Identity.Tenant); err != nil {
			return FileProviderMaterializationResult{}, err
		}
	}
	if unchanged {
		if _, err := tx.ExecContext(ctx, `
UPDATE file_provider_materialization_heads SET snapshot_id = ?, epoch = ? WHERE tenant = ?`,
			commit.Identity.Snapshot[:], stored.epoch, string(commit.Identity.Tenant)); err != nil {
			return FileProviderMaterializationResult{}, err
		}
	}
	updated, err := tx.ExecContext(ctx, `
UPDATE file_provider_materialization_snapshots
SET state = ?, committed_revision = ?, added_count = ?, removed_count = ?
WHERE snapshot_id = ? AND state = ?`, materializationSnapshotCommitted,
		result.Revision, result.Added, result.Removed, commit.Identity.Snapshot[:],
		materializationSnapshotCollecting)
	if err != nil {
		return FileProviderMaterializationResult{}, fmt.Errorf("catalog: record materialization commit: %w", err)
	}
	if changed, _ := updated.RowsAffected(); changed != 1 {
		return FileProviderMaterializationResult{}, ErrMutationConflict
	}
	if err := tx.Commit(); err != nil {
		return FileProviderMaterializationResult{}, fmt.Errorf("catalog: commit materialization set: %w", err)
	}
	return result, nil
}

// HasEligibleFileProviderMaterializedContainers reports whether an eligible
// authoritative container set is nonempty.
func (c *Catalog) HasEligibleFileProviderMaterializedContainers(ctx context.Context, tenant TenantID) (bool, error) {
	var demand int
	if err := c.readDB.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM file_provider_materialization_heads head
    JOIN file_provider_materialized_containers container
      ON container.tenant = head.tenant AND container.domain_id = head.domain_id
     AND container.generation = head.generation
     AND container.backing_store_identity = head.backing_store_identity
    WHERE head.tenant = ? AND head.eligible = 1
)`, string(tenant)).Scan(&demand); err != nil {
		return false, fmt.Errorf("catalog: inspect File Provider materialization demand: %w", err)
	}
	return demand != 0, nil
}

type storedMaterializationSnapshot struct {
	identity        FileProviderMaterializationIdentity
	epoch           uint64
	state           uint8
	pageCount       uint32
	itemCount       uint64
	lastPageCount   uint32
	lastContainerID []byte
	setDigest       [sha256.Size]byte
	revision        uint64
	added           uint64
	removed         uint64
}

func readFileProviderMaterializationSnapshot(
	ctx context.Context,
	query interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	id MaterializationSnapshotID,
) (storedMaterializationSnapshot, bool, error) {
	var stored storedMaterializationSnapshot
	var tenant, domain string
	var snapshot, presentation, digest []byte
	var generation uint64
	err := query.QueryRowContext(ctx, `
SELECT snapshot_id, epoch, tenant, domain_id, generation, backing_store_identity,
       state, page_count, item_count, last_page_count, last_container_id, set_digest,
       committed_revision, added_count, removed_count
FROM file_provider_materialization_snapshots WHERE snapshot_id = ?`, id[:]).Scan(
		&snapshot, &stored.epoch, &tenant, &domain, &generation, &presentation,
		&stored.state, &stored.pageCount, &stored.itemCount, &stored.lastPageCount,
		&stored.lastContainerID, &digest, &stored.revision, &stored.added, &stored.removed,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storedMaterializationSnapshot{}, false, nil
	}
	if err != nil {
		return storedMaterializationSnapshot{}, false, fmt.Errorf("catalog: read materialization snapshot: %w", err)
	}
	if !bytes.Equal(snapshot, id[:]) || len(digest) != sha256.Size ||
		(len(stored.lastContainerID) != 0 && len(stored.lastContainerID) != len(ObjectID{})) {
		return storedMaterializationSnapshot{}, false, ErrIntegrity
	}
	copy(stored.setDigest[:], digest)
	stored.identity = FileProviderMaterializationIdentity{
		Tenant: TenantID(tenant), Domain: causal.DomainID(domain), Generation: Generation(generation),
		Snapshot: id, BackingStoreIdentity: append([]byte(nil), presentation...),
	}
	return stored, true, nil
}

type materializationHead struct {
	domain               causal.DomainID
	generation           Generation
	backingStoreIdentity []byte
	epoch                uint64
	revision             uint64
	setDigest            [sha256.Size]byte
	eligible             bool
}

func readFileProviderMaterializationHead(
	ctx context.Context,
	query interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	tenant TenantID,
) (materializationHead, bool, error) {
	var head materializationHead
	var domain string
	var generation, eligible uint64
	var digest []byte
	err := query.QueryRowContext(ctx, `
SELECT domain_id, generation, backing_store_identity, epoch, revision, set_digest, eligible
FROM file_provider_materialization_heads WHERE tenant = ?`, string(tenant)).Scan(
		&domain, &generation, &head.backingStoreIdentity, &head.epoch, &head.revision, &digest, &eligible,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return materializationHead{}, false, nil
	}
	if err != nil {
		return materializationHead{}, false, fmt.Errorf("catalog: read materialization head: %w", err)
	}
	if len(digest) != sha256.Size {
		return materializationHead{}, false, ErrIntegrity
	}
	head.domain = causal.DomainID(domain)
	head.generation = Generation(generation)
	head.eligible = eligible != 0
	copy(head.setDigest[:], digest)
	return head, true, nil
}

func validateFileProviderMaterializationIdentity(identity FileProviderMaterializationIdentity) error {
	if identity.Tenant == "" || identity.Domain == "" || identity.Generation == 0 ||
		identity.Snapshot == (MaterializationSnapshotID{}) || len(identity.BackingStoreIdentity) == 0 ||
		len(identity.BackingStoreIdentity) > 256 {
		return fmt.Errorf("%w: incomplete File Provider materialization identity", ErrInvalidObject)
	}
	return nil
}

func validateFileProviderMaterializationRoute(
	ctx context.Context,
	query interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	identity FileProviderMaterializationIdentity,
) error {
	var exact int
	if err := query.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM file_provider_domains
    WHERE tenant = ? AND domain_id = ? AND generation = ? AND registered = 1
)`, string(identity.Tenant), string(identity.Domain), uint64(identity.Generation)).Scan(&exact); err != nil {
		return fmt.Errorf("catalog: validate materialization route: %w", err)
	}
	if exact == 0 {
		return ErrGenerationMismatch
	}
	return nil
}

func sameFileProviderMaterializationIdentity(left, right FileProviderMaterializationIdentity) bool {
	return left.Tenant == right.Tenant && left.Domain == right.Domain &&
		left.Generation == right.Generation && left.Snapshot == right.Snapshot &&
		bytes.Equal(left.BackingStoreIdentity, right.BackingStoreIdentity)
}

func materializationPageDigest(ids []ObjectID) [sha256.Size]byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte("fusekit.file-provider-materialization-page.v1\x00"))
	for _, id := range ids {
		_, _ = digest.Write(id[:])
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func initialMaterializationSetDigest() [sha256.Size]byte {
	return sha256.Sum256([]byte("fusekit.file-provider-materialization-set.v1\x00"))
}

func extendMaterializationSetDigest(
	previous [sha256.Size]byte,
	sequence uint32,
	ids []ObjectID,
) [sha256.Size]byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte("fusekit.file-provider-materialization-set-page.v1\x00"))
	_, _ = digest.Write(previous[:])
	var encoded [8]byte
	binary.BigEndian.PutUint32(encoded[:4], sequence)
	binary.BigEndian.PutUint32(encoded[4:], uint32(len(ids)))
	_, _ = digest.Write(encoded[:])
	pageDigest := materializationPageDigest(ids)
	_, _ = digest.Write(pageDigest[:])
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func validateStagedMaterializedContainerPage(
	ctx context.Context,
	tx *sql.Tx,
	identity FileProviderMaterializationIdentity,
	sequence uint32,
	expected uint64,
) error {
	view, err := readCatalogView(ctx, tx, identity.Tenant)
	if err != nil {
		return err
	}
	var actual uint64
	err = tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM file_provider_materialization_snapshot_items staged
JOIN source_driver_publication_objects object
  ON object.source_authority = ? AND object.publication_id = ? AND object.tenant = ?
 AND object.object_id = staged.container_id
WHERE staged.snapshot_id = ? AND object.kind = ? AND object.tombstone = 0
  AND object.file_provider_visible = 1 AND staged.sequence = ?`, view.authority, view.publication,
		string(identity.Tenant), identity.Snapshot[:], uint8(KindDirectory), sequence).Scan(&actual)
	if err != nil {
		return fmt.Errorf("catalog: validate materialized container page: %w", err)
	}
	if actual != expected {
		return fmt.Errorf("%w: materialized page contains unknown or non-container objects", ErrInvalidObject)
	}
	return nil
}

func advanceTenantTargetingRevision(ctx context.Context, tx *sql.Tx, tenant TenantID) error {
	result, err := tx.ExecContext(ctx, `
UPDATE tenant_targeting_heads SET revision = revision + 1 WHERE tenant_id = ?`, string(tenant))
	if err != nil {
		return fmt.Errorf("catalog: advance materialization targeting revision: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrNotFound
	}
	return nil
}

func retireFileProviderMaterialization(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	domain causal.DomainID,
	generation Generation,
) error {
	var effective int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM file_provider_materialization_heads
    WHERE tenant = ? AND domain_id = ? AND generation = ? AND eligible = 1
)`, string(tenant), string(domain), uint64(generation)).Scan(&effective); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM file_provider_materialized_containers
WHERE tenant = ? AND domain_id = ? AND generation = ?`, string(tenant), string(domain), uint64(generation)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM file_provider_materialization_heads
WHERE tenant = ? AND domain_id = ? AND generation = ?`, string(tenant), string(domain), uint64(generation)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM file_provider_materialization_snapshots
WHERE tenant = ? AND domain_id = ? AND generation = ?`, string(tenant), string(domain), uint64(generation)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM file_provider_materialization_owners
WHERE tenant = ? AND domain_id = ? AND generation = ?`, string(tenant), string(domain), uint64(generation)); err != nil {
		return err
	}
	if effective != 0 {
		return advanceTenantTargetingRevision(ctx, tx, tenant)
	}
	return nil
}
