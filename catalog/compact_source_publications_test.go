package catalog

import (
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type sourcePublicationGCFixture struct {
	provision TenantProvision
	oldFile   Object
	newFile   Object
	stable    Object
	oldRef    ContentRef
	newRef    ContentRef
	oldPub    []byte
	newPub    []byte
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
		`SELECT COUNT(*) FROM convergence_outbox`).Scan(&notificationsBefore); err != nil {
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
		`SELECT COUNT(*) FROM convergence_outbox`).Scan(&notificationsAfter); err != nil {
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
		assertSourcePublicationVisibility(t, store, fixture.newPub, 2, 3)
		drainOrphanSourcePublicationForTest(t, store, target)
		assertSourcePublicationVisibility(t, store, fixture.newPub, 2, 3)
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
		insertVisibilityPublication(t, store, semantic, fixture.newPub, 3, 2, 3, 3,
			[]Object{root, fixture.newFile, fixture.stable}, nil, fixture.newFile)
		if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_driver_publication_heads
SET publication_id = ?, source_revision = 3, epoch = epoch + 1
WHERE source_authority = 'driver-authority' AND publication_id = ?`,
			semantic, fixture.newPub); err != nil {
			t.Fatal(err)
		}
		retired, more := runOneSourcePublicationCompactionPage(t, store, 1)
		if retired != 1 || !more {
			t.Fatalf("semantic-winner compaction = retired %d more %t", retired, more)
		}
		assertSourcePublicationVisibility(t, store, semantic, 3, 3)
		drainOrphanSourcePublicationForTest(t, store, target)
		assertSourcePublicationVisibility(t, store, semantic, 3, 3)
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
			assertSourcePublicationVisibility(t, store, fixture.newPub, 2, 3)
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
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		reopened, err := Open(t.Context(), path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reopened.Close() })
		assertSourcePublicationVisibility(t, reopened, target, 2, 3)
		drainOrphanSourcePublicationForTest(t, reopened, fixture.newPub)
		assertCompactedSourcePublication(t, reopened, fixture, 3)
	})
}

func TestSourcePublicationOrphanGCDoesNotDeletePreparedSemanticStage(t *testing.T) {
	store, provision, declaration, targets := openAtomicVisibilityCatalog(
		t, filepath.Join(t.TempDir(), "catalog.sqlite"),
	)
	defer func() { _ = store.Close() }()
	baseline := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "baseline-token", 0, 201,
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
	next := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverDelta, 0,
		"baseline-token", "next-token", 1, 202,
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
	insertVisibilityPublication(t, store, oldPub, nil, 1, 0, 1, 2,
		[]Object{root, oldFile, stable}, []Object{root, oldFile, stable}, oldFile)
	insertVisibilityPublication(t, store, newPub, oldPub, 2, 1, 2, 3,
		[]Object{root, newFile, stable}, []Object{newFile}, newFile)
	if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO source_driver_target_epochs(source_authority, target_epoch)
VALUES ('driver-authority', 1)
ON CONFLICT(source_authority) DO UPDATE SET target_epoch = excluded.target_epoch`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO source_driver_publication_heads(
    source_authority, publication_id, source_revision, epoch
) VALUES ('driver-authority', ?, 2, 2)`, newPub); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `
UPDATE tenants SET head = 3 WHERE tenant = ?;
DELETE FROM content_stages WHERE stage_id IN (?, ?)`,
		string(provision.Tenant), oldRef.Stage[:], newRef.Stage[:]); err != nil {
		t.Fatal(err)
	}
	return sourcePublicationGCFixture{
		provision: provision, oldFile: oldFile, newFile: newFile, stable: stable,
		oldRef: oldRef, newRef: newRef, oldPub: oldPub, newPub: newPub,
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
	retired, more, err := store.compactSourceDriverPublicationPage(t.Context(), tx, limit)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
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
	wantDigest := sourcePublicationCompactionDigest("driver-authority", fixture.newPub, active, 2)
	if publications != 1 || kind != sourceDriverPublicationCompacted || prepared != 1 ||
		len(predecessor) != 0 || sourceRevision != 2 || epoch != 3 ||
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
