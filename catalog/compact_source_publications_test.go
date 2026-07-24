package catalog

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

type sourcePublicationGCFixture struct {
	provision       TenantProvision
	oldFile         Object
	newFile         Object
	stable          Object
	oldRef          ContentRef
	newRef          ContentRef
	oldPub          []byte
	newPub          []byte
	sourceRevision  uint64
	visibilityEpoch uint64
}

func TestSourcePublicationCompactionPreservesAnchorsHandlesAndReclaimsBlobs(t *testing.T) {
	store := newTestCatalog(t)
	fixture := installSourcePublicationGCFixture(t, store)
	handle, err := store.OpenAt(
		t.Context(), testRetentionOwner, fixture.provision.Tenant, PresentationMount,
		fixture.provision.Generation, fixture.oldFile.ID, fixture.oldFile.Revision,
	)
	if err != nil {
		t.Fatalf("OpenAt(old publication version): %v", err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE tenants SET floor = 3 WHERE tenant = ?`,
		string(fixture.provision.Tenant)); err != nil {
		t.Fatal(err)
	}
	var notificationsBefore int
	if err := store.readDB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM activation_outbox`).Scan(&notificationsBefore); err != nil {
		t.Fatal(err)
	}
	runSourcePublicationCompaction(t, store, 1)

	assertCompactedSourcePublication(t, store, fixture, 3)
	buffer := make([]byte, fixture.oldRef.Size)
	read, err := handle.ReadAt(buffer, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read pinned old publication version: %v", err)
	}
	if read != len(buffer) || string(buffer) != "old-publication" {
		t.Fatalf("pinned old bytes = %q (%d)", buffer, read)
	}
	var stale *StaleAnchorError
	if _, err := store.LookupAt(
		t.Context(), fixture.provision.Tenant, PresentationMount,
		fixture.oldFile.ID, fixture.oldFile.Revision,
	); !errors.As(err, &stale) {
		t.Fatalf("expired anchor lookup = %v, want StaleAnchorError", err)
	}
	oldName := contentName(fixture.oldRef.Hash)
	referenced, err := store.blobEntryReferenced(t.Context(), oldName)
	if err != nil || !referenced {
		t.Fatalf("live-handle blob reference = %t, %v", referenced, err)
	}
	var notificationsAfter int
	if err := store.readDB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM activation_outbox`).Scan(&notificationsAfter); err != nil {
		t.Fatal(err)
	}
	if notificationsAfter != notificationsBefore {
		t.Fatalf("compaction emitted %d notifications", notificationsAfter-notificationsBefore)
	}

	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Forget(t.Context()); err != nil {
		t.Fatal(err)
	}
	runSourcePublicationCompaction(t, store, 1)
	for step := 0; step < 16; step++ {
		_, more, err := store.compactBlobCandidatePage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			break
		}
	}
	referenced, err = store.blobEntryReferenced(t.Context(), oldName)
	if err != nil || referenced {
		t.Fatalf("retired-handle blob reference = %t, %v", referenced, err)
	}
	if _, err := os.Stat(filepath.Join(store.blobDir, blobName(fixture.oldRef.Hash))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old publication blob still exists: %v", err)
	}
}

func TestSourcePublicationCompactionResumesAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	fixture := installSourcePublicationGCFixture(t, store)
	if _, err := store.db.ExecContext(t.Context(), `UPDATE tenants SET floor = 3 WHERE tenant = ?`,
		string(fixture.provision.Tenant)); err != nil {
		t.Fatal(err)
	}
	for step := 0; step < 3; step++ {
		_, more := runOneSourcePublicationCompactionPage(t, store, 1)
		if !more {
			t.Fatal("compaction finished before crash checkpoint")
		}
	}
	var active, predecessor []byte
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT visibility.publication_id, publication.predecessor_publication_id
FROM source_driver_publication_heads visibility
JOIN source_driver_publications publication
  ON publication.source_authority = visibility.source_authority
 AND publication.publication_id = visibility.publication_id
WHERE visibility.source_authority = 'driver-authority'`).Scan(&active, &predecessor); err != nil {
		t.Fatal(err)
	}
	if !equalBytes(active, fixture.newPub) || !equalBytes(predecessor, fixture.oldPub) {
		t.Fatalf("partial compaction mutated active publication active=%x predecessor=%x", active, predecessor)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	runSourcePublicationCompaction(t, store, 1)
	assertCompactedSourcePublication(t, store, fixture, 3)
}

func TestSourcePublicationCompactionLosesExactFencesWithoutChangingVisibility(t *testing.T) {
	t.Run("target epoch", func(t *testing.T) {
		store := newTestCatalog(t)
		fixture := installSourcePublicationGCFixture(t, store)
		target := beginSourcePublicationCompactionForTest(t, store)
		if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_driver_target_epochs SET target_epoch = target_epoch + 1
WHERE source_authority = 'driver-authority'`); err != nil {
			t.Fatal(err)
		}
		retired, more := runOneSourcePublicationCompactionPage(t, store, 1)
		if retired != 1 || !more {
			t.Fatalf("stale target-epoch compaction = retired %d more %t", retired, more)
		}
		assertSourcePublicationVisibility(t, store, fixture.newPub, fixture.sourceRevision, 3)
		drainOrphanSourcePublicationForTest(t, store, target)
		assertSourcePublicationVisibility(t, store, fixture.newPub, fixture.sourceRevision, 3)
	})

	t.Run("semantic publication", func(t *testing.T) {
		store := newTestCatalog(t)
		fixture := installSourcePublicationGCFixture(t, store)
		target := beginSourcePublicationCompactionForTest(t, store)
		semantic := []byte("semantic-winner!")
		root, err := store.Root(t.Context(), fixture.provision.Tenant)
		if err != nil {
			t.Fatal(err)
		}
		insertVisibilityPublication(t, store, semantic, fixture.newPub,
			fixture.sourceRevision+1, fixture.sourceRevision, 3, 3,
			[]Object{root, fixture.newFile, fixture.stable}, nil, fixture.newFile)
		if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_driver_publication_heads
SET publication_id = ?, source_revision = ?, epoch = epoch + 1
WHERE source_authority = 'driver-authority' AND publication_id = ?`,
			semantic, fixture.sourceRevision+1, fixture.newPub); err != nil {
			t.Fatal(err)
		}
		retired, more := runOneSourcePublicationCompactionPage(t, store, 1)
		if retired != 1 || !more {
			t.Fatalf("semantic-winner compaction = retired %d more %t", retired, more)
		}
		assertSourcePublicationVisibility(t, store, semantic, fixture.sourceRevision+1, 3)
		drainOrphanSourcePublicationForTest(t, store, target)
		assertSourcePublicationVisibility(t, store, semantic, fixture.sourceRevision+1, 3)
	})
}

func TestSourcePublicationCompactionVisibilityCASIsOldOrNew(t *testing.T) {
	for _, point := range []string{
		sourcePublicationBeforeVisibilityCAS,
		sourcePublicationAfterVisibilityCAS,
	} {
		t.Run(point, func(t *testing.T) {
			store := newTestCatalog(t)
			fixture := installSourcePublicationGCFixture(t, store)
			for step := 0; step < 64; step++ {
				var phase uint8
				err := store.readDB.QueryRowContext(t.Context(), `
SELECT phase FROM source_driver_publication_compactions
WHERE source_authority = 'driver-authority'`).Scan(&phase)
				if err == nil && phase == sourcePublicationCompactSeal {
					break
				}
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					t.Fatal(err)
				}
				runOneSourcePublicationCompactionPage(t, store, 1)
				if step == 63 {
					t.Fatal("compaction did not reach seal")
				}
			}
			injected := errors.New("compaction visibility crash")
			store.failpoint = func(candidate string) error {
				if candidate == point {
					return injected
				}
				return nil
			}
			tx, err := store.db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.compactSourceDriverPublicationPage(t.Context(), tx, 1); !errors.Is(err, injected) {
				_ = tx.Rollback()
				t.Fatalf("seal at %s = %v, want injected", point, err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			store.failpoint = nil
			assertSourcePublicationVisibility(t, store, fixture.newPub, fixture.sourceRevision, 3)
			runSourcePublicationCompaction(t, store, 1)
			assertCompactedSourcePublication(t, store, fixture, 3)
		})
	}
	t.Run("committed response lost", func(t *testing.T) {
		store := newTestCatalog(t)
		fixture := installSourcePublicationGCFixture(t, store)
		target := beginSourcePublicationCompactionForTest(t, store)
		for step := 0; step < 64; step++ {
			var phase uint8
			if err := store.readDB.QueryRowContext(t.Context(), `
SELECT phase FROM source_driver_publication_compactions
WHERE source_authority = 'driver-authority'`).Scan(&phase); err != nil {
				t.Fatal(err)
			}
			if phase == sourcePublicationCompactSeal {
				break
			}
			runOneSourcePublicationCompactionPage(t, store, 1)
			if step == 63 {
				t.Fatal("compaction did not reach seal")
			}
		}
		var path string
		if err := store.db.QueryRowContext(t.Context(), `
SELECT file FROM pragma_database_list WHERE name = 'main'`).Scan(&path); err != nil {
			t.Fatal(err)
		}
		tx, err := store.db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.compactSourceDriverPublicationPage(t.Context(), tx, 1); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err := alignTenantApplicationsWithSourceHeadForTest(t.Context(), tx); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		reopened, err := Open(t.Context(), path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reopened.Close() })
		assertSourcePublicationVisibility(t, reopened, target, fixture.sourceRevision, 3)
		drainOrphanSourcePublicationForTest(t, reopened, fixture.newPub)
		assertCompactedSourcePublication(t, reopened, fixture, 3)
	})
}

func TestSourcePublicationOrphanGCDoesNotDeletePreparedSemanticStage(t *testing.T) {
	store, provision, declaration, targets := openAtomicVisibilityCatalog(
		t, filepath.Join(t.TempDir(), "catalog.sqlite"),
	)
	defer func() { _ = store.Close() }()
	baseline := sourceDriverIdentityAtHeadForTest(
		t, store, declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotReset,
		"", "baseline-token", 201,
	)
	baselineState := appendAtomicVisibilityObject(t, store, baseline, provision, "old")
	prepareAtomicVisibilityPublication(t, store, baseline)
	result, err := store.CommitSourceDriverStage(t.Context(), baselineState)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeSourceDriverCommittedReceipt(t.Context(), result); err != nil {
		t.Fatal(err)
	}
	if err := store.ForgetSourceDriverCommittedReceipt(t.Context(), result); err != nil {
		t.Fatal(err)
	}
	collectAtomicVisibilityStage(t, store, baseline)
	next := sourceDriverIdentityAtHeadForTest(
		t, store, declaration, targets, SourceDriverDelta, 0,
		"baseline-token", "next-token", 202,
	)
	nextState := appendAtomicVisibilityObject(t, store, next, provision, "new")
	prepareAtomicVisibilityPublication(t, store, next)
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := compactOrphanSourcePublicationPage(t.Context(), tx, 256); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitSourceDriverStage(t.Context(), nextState); err != nil {
		t.Fatalf("commit prepared semantic publication after orphan GC: %v", err)
	}
}

func installSourcePublicationGCFixture(t *testing.T, store *Catalog) sourcePublicationGCFixture {
	t.Helper()
	provisionSpec := testTenantProvision(t, "publication-gc", 1)
	provisionSpec.ContentSourceID = "driver-authority"
	provision, err := provisionTenantForTest(t, store, t.Context(), provisionSpec)
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.Root(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	oldRef := stageTestContent(t, store, "old-publication")
	newRef := stageTestContent(t, store, "new-publication")
	id, err := NewObjectID()
	if err != nil {
		t.Fatal(err)
	}
	oldFile := Object{
		Tenant: provision.Tenant, ID: id, Parent: provision.Root,
		Revision: 2, MetadataRevision: 2, ContentRevision: 2,
		Name: "settings.json", Kind: KindFile, Mode: 0o600,
		Size: oldRef.Size, Hash: oldRef.Hash,
		Visibility: Visibility{Mount: true, FileProvider: true},
	}
	newFile := oldFile
	newFile.Revision = 3
	newFile.MetadataRevision = 3
	newFile.ContentRevision = 3
	newFile.Size = newRef.Size
	newFile.Hash = newRef.Hash
	stableID, err := NewObjectID()
	if err != nil {
		t.Fatal(err)
	}
	stable := Object{
		Tenant: provision.Tenant, ID: stableID, Parent: provision.Root,
		Revision: 2, MetadataRevision: 2, Name: "stable", Kind: KindDirectory,
		Mode: 0o700, Visibility: Visibility{Mount: true, FileProvider: true},
	}
	oldPub := []byte("old-publication!")
	newPub := []byte("new-publication!")
	var predecessor []byte
	var predecessorRevision, visibilityEpoch uint64
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT publication_id, source_revision, epoch
FROM source_driver_publication_heads WHERE source_authority = 'driver-authority'`).Scan(
		&predecessor, &predecessorRevision, &visibilityEpoch,
	); err != nil {
		t.Fatal(err)
	}
	oldRevision := predecessorRevision + 1
	newRevision := oldRevision + 1
	insertVisibilityPublication(t, store, oldPub, predecessor, oldRevision, predecessorRevision, 1, 2,
		[]Object{root, oldFile, stable}, []Object{root, oldFile, stable}, oldFile)
	insertVisibilityPublication(t, store, newPub, oldPub, newRevision, oldRevision, 2, 3,
		[]Object{root, newFile, stable}, []Object{newFile}, newFile)
	if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO source_driver_target_epochs(source_authority, target_epoch)
VALUES ('driver-authority', 1)
ON CONFLICT(source_authority) DO UPDATE SET target_epoch = excluded.target_epoch`); err != nil {
		t.Fatal(err)
	}
	visibilityEpoch++
	if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_driver_publication_heads
SET publication_id = ?, source_revision = ?, epoch = ?
WHERE source_authority = 'driver-authority'`, newPub, newRevision, visibilityEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `
UPDATE tenants SET head = 3 WHERE tenant = ?;
DELETE FROM content_stages WHERE stage_id IN (?, ?)`,
		string(provision.Tenant), oldRef.Stage[:], newRef.Stage[:]); err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := alignTenantApplicationsWithSourceHeadForTest(t.Context(), tx); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := prepareTestMaintenance(t.Context(), store, provision.Tenant, 3); err != nil {
		t.Fatal(err)
	}
	return sourcePublicationGCFixture{
		provision: provision, oldFile: oldFile, newFile: newFile, stable: stable,
		oldRef: oldRef, newRef: newRef, oldPub: oldPub, newPub: newPub,
		sourceRevision: newRevision, visibilityEpoch: visibilityEpoch,
	}
}

func runSourcePublicationCompaction(t *testing.T, store *Catalog, limit int) {
	t.Helper()
	for step := 0; step < 512; step++ {
		_, more := runOneSourcePublicationCompactionPage(t, store, limit)
		if !more {
			return
		}
	}
	t.Fatal("source publication compaction did not converge")
}

func beginSourcePublicationCompactionForTest(t *testing.T, store *Catalog) []byte {
	t.Helper()
	_, more := runOneSourcePublicationCompactionPage(t, store, 1)
	if !more {
		t.Fatal("source publication compaction did not start")
	}
	var target []byte
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT compaction_publication_id FROM source_driver_publication_compactions
WHERE source_authority = 'driver-authority'`).Scan(&target); err != nil {
		t.Fatal(err)
	}
	return target
}

func drainOrphanSourcePublicationForTest(t *testing.T, store *Catalog, publication []byte) {
	t.Helper()
	for step := 0; step < 64; step++ {
		var exists bool
		if err := store.readDB.QueryRowContext(t.Context(), `
SELECT EXISTS(SELECT 1 FROM source_driver_publications WHERE publication_id = ?)`,
			publication).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			return
		}
		tx, err := store.db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := compactOrphanSourcePublicationPage(t.Context(), tx, 1); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("orphan source publication did not retire")
}

func assertSourcePublicationVisibility(
	t *testing.T, store *Catalog, publication []byte, sourceRevision uint64, head Revision,
) {
	t.Helper()
	var active []byte
	var revision uint64
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT publication_id, source_revision
FROM source_driver_publication_heads WHERE source_authority = 'driver-authority'`).Scan(
		&active, &revision,
	); err != nil {
		t.Fatal(err)
	}
	visibleHead, err := store.Head(t.Context(), "publication-gc")
	if err != nil || !equalBytes(active, publication) || revision != sourceRevision || visibleHead != head {
		t.Fatalf("visibility active=%x revision=%d head=%d err=%v", active, revision, visibleHead, err)
	}
}

func runOneSourcePublicationCompactionPage(t *testing.T, store *Catalog, limit int) (int, bool) {
	t.Helper()
	before := sourcePublicationChildRows(t, store)
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, found, err := readSourcePublicationCompaction(t.Context(), tx)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("read source publication compaction: %v", err)
	}
	if !found {
		created, err := seedSourcePublicationCompactionForTest(t.Context(), tx)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("seed source publication compaction: %v", err)
		}
		if created {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			return 0, true
		}
	}
	retired, more, err := store.compactSourceDriverPublicationPage(t.Context(), tx, limit)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("compact source publication page: %v", err)
	}
	if err := alignTenantApplicationsWithSourceHeadForTest(t.Context(), tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("align active tenant application: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	after := sourcePublicationChildRows(t, store)
	delta := after - before
	if delta < 0 {
		delta = -delta
	}
	if delta > limit {
		t.Fatalf("publication compaction changed %d child rows with limit %d", delta, limit)
	}
	return retired, more
}

func seedSourcePublicationCompactionForTest(ctx context.Context, tx *sql.Tx) (bool, error) {
	var authority string
	var source []byte
	var epoch uint64
	err := tx.QueryRowContext(ctx, `
SELECT visibility.source_authority, visibility.publication_id, visibility.epoch
FROM source_driver_publication_heads visibility
JOIN source_driver_publications publication
  ON publication.source_authority = visibility.source_authority
 AND publication.publication_id = visibility.publication_id
WHERE length(publication.predecessor_publication_id) = 16
   OR EXISTS (
       SELECT 1
       FROM source_driver_publication_versions version
       JOIN tenants tenant ON tenant.tenant = version.tenant
       WHERE version.source_authority = publication.source_authority
         AND version.publication_id = publication.publication_id
         AND NOT (
             version.revision >= tenant.floor
             OR version.revision = (
                 SELECT MAX(baseline.revision)
                 FROM source_driver_publication_versions baseline
                 WHERE baseline.source_authority = version.source_authority
                   AND baseline.publication_id = version.publication_id
                   AND baseline.tenant = version.tenant
                   AND baseline.object_id = version.object_id
                   AND baseline.revision <= tenant.floor
             )
             OR EXISTS (
                 SELECT 1 FROM handles handle
                 WHERE handle.tenant = version.tenant
                   AND handle.object_id = version.object_id
                   AND handle.object_revision = version.revision AND handle.closed = 0
             )
             OR EXISTS (
                 SELECT 1 FROM source_driver_publication_changes change
                 WHERE change.source_authority = version.source_authority
                   AND change.publication_id = version.publication_id
                   AND change.tenant = version.tenant AND change.revision > tenant.floor
                   AND change.object_id = version.object_id
                   AND change.object_revision = version.revision
             )
         )
   )
   OR EXISTS (
       SELECT 1
       FROM source_driver_publication_changes change
       JOIN tenants tenant ON tenant.tenant = change.tenant
       WHERE change.source_authority = publication.source_authority
         AND change.publication_id = publication.publication_id
         AND change.revision <= tenant.floor
   )
ORDER BY visibility.source_authority LIMIT 1`).Scan(&authority, &source, &epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	generated, err := NewObjectID()
	if err != nil {
		return false, err
	}
	target := generated[:]
	sourceOperation := causal.OperationID(generated)
	changeID := causal.ChangeID(generated)
	changeID[0] ^= 0x80
	digest := sourcePublicationCompactionDigest(authority, source, target, epoch)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publications(
    source_authority, publication_id, source_operation_id, change_id, cause,
    origin_domain, origin_generation, affected_key_count, affected_keys_digest,
    publication_kind, identity_digest,
    target_count, targets_digest, stage_sequence, stage_item_count, stage_byte_count,
    stage_digest, predecessor_publication_id, predecessor_revision, source_revision,
    expected_visibility_epoch, target_epoch, phase, cursor_tenant, cursor_key,
    initialized_target_count, prepared_target_count, item_count, byte_count,
    rolling_digest, prepared
)
SELECT source_authority, ?, ?, ?, 'external_unattributed', '', 0,
       affected_key_count, affected_keys_digest, ?, ?, target_count, targets_digest,
       stage_sequence, stage_item_count, stage_byte_count, stage_digest,
       zeroblob(0), 0, source_revision, ?, target_epoch, ?, '', '', 0, 0, 0, 0,
       zeroblob(32), 0
FROM source_driver_publications
WHERE source_authority = ? AND publication_id = ?`, target, sourceOperation[:], changeID[:],
		sourceDriverPublicationCompacted, digest[:], epoch, sourceDriverPublicationPreparing,
		authority, source); err != nil {
		return false, mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_compactions(
    source_authority, source_publication_id, compaction_publication_id,
    expected_visibility_epoch, phase
) VALUES (?, ?, ?, ?, ?)`, authority, source, target, epoch, sourcePublicationCompactTargets); err != nil {
		return false, mapConstraint(err)
	}
	return true, nil
}

func alignTenantApplicationsWithSourceHeadForTest(ctx context.Context, tx *sql.Tx) error {
	type application struct {
		tenant                  string
		generation, revision    uint64
		publication, headDigest []byte
		publicationDigest       []byte
		catalogHead             uint64
	}
	rows, err := tx.QueryContext(ctx, `
SELECT application.tenant_id, application.generation, head.publication_id,
       head.source_revision, target.catalog_head, target.catalog_fingerprint,
       publication.stage_digest
FROM tenant_applications application
JOIN source_driver_publication_heads head
  ON head.source_authority = application.source_authority
JOIN source_driver_publications publication
  ON publication.source_authority = head.source_authority
 AND publication.publication_id = head.publication_id
JOIN source_driver_publication_targets target
  ON target.source_authority = head.source_authority
 AND target.publication_id = head.publication_id
 AND target.tenant = application.tenant_id
 AND target.generation = application.generation
WHERE application.phase IN (?, ?)`, uint8(TenantApplicationStaged),
		uint8(TenantApplicationRetiring))
	if err != nil {
		return err
	}
	var applications []application
	for rows.Next() {
		var row application
		if err := rows.Scan(&row.tenant, &row.generation, &row.publication, &row.revision,
			&row.catalogHead, &row.headDigest, &row.publicationDigest); err != nil {
			_ = rows.Close()
			return err
		}
		applications = append(applications, row)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, row := range applications {
		if _, err := tx.ExecContext(ctx, `
UPDATE tenant_applications SET
    source_publication_id = ?, staged_catalog_head = ?, staged_head_digest = ?,
    staged_source_revision = ?, publication_digest = ?
WHERE tenant_id = ? AND generation = ?`, row.publication, row.catalogHead, row.headDigest,
			row.revision, row.publicationDigest, row.tenant, row.generation); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE tenant_activations SET active_catalog_head = ?, source_revision = ?
WHERE tenant_id = ? AND active_generation = ?`, row.catalogHead, row.revision,
			row.tenant, row.generation); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE tenant_activation_causes SET
    publication_id = ?, source_revision = ?,
    source_operation_id = (
        SELECT source_operation_id FROM source_driver_publications
        WHERE source_authority = tenant_activation_causes.source_authority
          AND publication_id = ?
    ),
    change_id = (
        SELECT change_id FROM source_driver_publications
        WHERE source_authority = tenant_activation_causes.source_authority
          AND publication_id = ?
    )
WHERE source_authority = (
    SELECT source_authority FROM tenant_applications
    WHERE tenant_id = ? AND generation = ?
)
  AND activation_change_id IN (
      SELECT activation_change_id FROM tenant_activation_changes
      WHERE tenant_id = ? AND generation = ?
  )`, row.publication, row.revision, row.publication, row.publication,
			row.tenant, row.generation, row.tenant, row.generation); err != nil {
			return err
		}
	}
	return nil
}

func sourcePublicationChildRows(t *testing.T, store *Catalog) int {
	t.Helper()
	var count int
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT
  (SELECT COUNT(*) FROM source_driver_publication_targets) +
  (SELECT COUNT(*) FROM source_driver_publication_objects) +
  (SELECT COUNT(*) FROM source_driver_publication_versions) +
  (SELECT COUNT(*) FROM source_driver_publication_changes)`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertCompactedSourcePublication(
	t *testing.T, store *Catalog, fixture sourcePublicationGCFixture, wantHead Revision,
) {
	t.Helper()
	head, err := store.Head(t.Context(), fixture.provision.Tenant)
	if err != nil || head != wantHead {
		t.Fatalf("compacted head = %d, %v, want %d", head, err, wantHead)
	}
	current, err := store.Lookup(
		t.Context(), fixture.provision.Tenant, PresentationMount, fixture.newFile.ID,
	)
	if err != nil || current.Hash != fixture.newRef.Hash || current.Revision != fixture.newFile.Revision {
		t.Fatalf("compacted current object = %+v, %v", current, err)
	}
	page, err := store.Snapshot(t.Context(), fixture.provision.Tenant, EnumerationScope{
		Kind: EnumerationContainer, Presentation: PresentationMount, Parent: fixture.provision.Root,
	}, wantHead, SnapshotCursor{}, 10)
	if err != nil || len(page.Objects) != 2 {
		t.Fatalf("compacted snapshot = %+v, %v", page, err)
	}
	var foundCurrent, foundBaseline bool
	for _, object := range page.Objects {
		foundCurrent = foundCurrent || (object.ID == fixture.newFile.ID && object.Hash == fixture.newRef.Hash)
		foundBaseline = foundBaseline || (object.ID == fixture.stable.ID && object.Revision == fixture.stable.Revision)
	}
	if !foundCurrent || !foundBaseline {
		t.Fatalf("compacted snapshot lost current=%t floor-baseline=%t: %+v",
			foundCurrent, foundBaseline, page.Objects)
	}
	changes, err := store.ChangesSince(t.Context(), fixture.provision.Tenant, EnumerationScope{
		Kind: EnumerationContainer, Presentation: PresentationMount, Parent: fixture.provision.Root,
	}, CompleteChangeCursor(wantHead), 10)
	if err != nil || len(changes.Changes) != 0 || !changes.Complete {
		t.Fatalf("compacted changes = %+v, %v", changes, err)
	}
	var publications, kind, prepared int
	var predecessor, active []byte
	var sourceRevision, epoch uint64
	var identityDigest, rollingDigest []byte
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT (SELECT COUNT(*) FROM source_driver_publications),
       publication_kind, prepared, predecessor_publication_id,
       publication.source_revision, visibility.epoch,
       publication.identity_digest, publication.rolling_digest,
       visibility.publication_id
FROM source_driver_publications publication
JOIN source_driver_publication_heads visibility
  ON visibility.source_authority = publication.source_authority
 AND visibility.publication_id = publication.publication_id
WHERE publication.source_authority = 'driver-authority'`).Scan(
		&publications, &kind, &prepared, &predecessor, &sourceRevision, &epoch,
		&identityDigest, &rollingDigest, &active,
	); err != nil {
		t.Fatal(err)
	}
	wantDigest := sourcePublicationCompactionDigest(
		"driver-authority", fixture.newPub, active, fixture.visibilityEpoch,
	)
	if publications != 1 || kind != sourceDriverPublicationCompacted || prepared != 1 ||
		len(predecessor) != 0 || sourceRevision != fixture.sourceRevision || epoch != fixture.visibilityEpoch+1 ||
		!equalBytes(identityDigest, wantDigest[:]) || !equalBytes(rollingDigest, wantDigest[:]) {
		t.Fatalf("compacted publication count=%d kind=%d prepared=%d predecessor=%x revision=%d epoch=%d identity=%x rolling=%x",
			publications, kind, prepared, predecessor, sourceRevision, epoch, identityDigest, rollingDigest)
	}
}

func contentName(hash ContentHash) string {
	const digits = "0123456789abcdef"
	name := make([]byte, len(hash)*2)
	for index, value := range hash {
		name[index*2] = digits[value>>4]
		name[index*2+1] = digits[value&0x0f]
	}
	return string(name)
}
