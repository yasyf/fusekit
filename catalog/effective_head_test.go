package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestActiveSourcePublicationIsAnExactVisibilityPointer(t *testing.T) {
	store, provisions, _, _ := newSourceDriverCatalog(t, "active-publication")
	provision := provisions[0]
	root, err := store.Root(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	firstObject := Object{
		Tenant: provision.Tenant, ID: ObjectID{101}, Parent: root.ID,
		Revision: 2, MetadataRevision: 2, Name: "first", Kind: KindDirectory,
		Visibility: Visibility{Mount: true, FileProvider: true},
	}
	secondObject := Object{
		Tenant: provision.Tenant, ID: ObjectID{102}, Parent: root.ID,
		Revision: 3, MetadataRevision: 3, Name: "second", Kind: KindDirectory,
		Visibility: Visibility{Mount: true, FileProvider: true},
	}
	firstPublication := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	secondPublication := []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}
	insertVisibilityPublication(t, store, firstPublication, nil, 1, 0, 1, 2,
		[]Object{root, firstObject}, []Object{root, firstObject}, firstObject)
	if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO source_driver_publication_heads(
    source_authority, publication_id, source_revision, epoch
) VALUES ('driver-authority', ?, 1, 1)`, firstPublication); err != nil {
		t.Fatal(err)
	}

	assertVisiblePublicationObject(t, store, provision, "first", firstObject.ID, 2)
	fingerprintBefore := activeFileProviderFingerprint(t, store, provision.Tenant)
	mutableOnly := Object{
		Tenant: provision.Tenant, ID: ObjectID{103}, Parent: root.ID,
		Revision: 3, MetadataRevision: 3, Name: "mutable-only", Kind: KindDirectory,
		Visibility: Visibility{Mount: true, FileProvider: true},
	}
	mutableTx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeNewObject(t.Context(), mutableTx, mutableOnly); err != nil {
		_ = mutableTx.Rollback()
		t.Fatal(err)
	}
	if err := mutableTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if fingerprintAfter := activeFileProviderFingerprint(t, store, provision.Tenant); fingerprintAfter != fingerprintBefore {
		t.Fatalf("active publication fingerprint observed mutable objects")
	}
	if _, err := store.BeginMutation(t.Context(), provision.Tenant, 1, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(),
		Create: &CreateMutation{Spec: CreateSpec{
			Parent: provision.Root, Name: "stale", Kind: KindDirectory,
			Visibility: Visibility{Mount: true},
		}},
	}); !errors.Is(err, errMutationHeadChanged) {
		t.Fatalf("BeginMutation at predecessor head = %v, want head changed", err)
	}
	if _, err := store.BeginMutation(t.Context(), provision.Tenant, 2, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(),
		Create: &CreateMutation{Spec: CreateSpec{
			Parent: provision.Root, Name: "first", Kind: KindDirectory,
			Visibility: Visibility{Mount: true},
		}},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("BeginMutation conflicting with active publication = %v, want conflict", err)
	}
	insertVisibilityPublication(t, store, secondPublication, firstPublication, 2, 1, 2, 3,
		[]Object{root, firstObject, secondObject}, []Object{secondObject}, secondObject)
	if _, err := store.LookupName(t.Context(), provision.Tenant, PresentationMount, root.ID, "second"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("prepared inactive publication leaked through lookup: %v", err)
	}
	if head, err := store.Head(t.Context(), provision.Tenant); err != nil || head != 2 {
		t.Fatalf("head before pointer flip = %d, %v, want 2", head, err)
	}
	if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_driver_publication_heads
SET publication_id = ?, source_revision = 2, epoch = 2
WHERE source_authority = 'driver-authority'
  AND publication_id = ? AND epoch = 1`, secondPublication, firstPublication); err != nil {
		t.Fatal(err)
	}
	assertVisiblePublicationObject(t, store, provision, "second", secondObject.ID, 3)
	if historical, err := store.LookupAt(
		t.Context(), provision.Tenant, PresentationMount, firstObject.ID, 2,
	); err != nil || historical.ID != firstObject.ID {
		t.Fatalf("historical lineage lookup = %+v, %v", historical, err)
	}
	historicalPage, err := store.Snapshot(t.Context(), provision.Tenant, EnumerationScope{
		Kind: EnumerationContainer, Presentation: PresentationMount, Parent: root.ID,
	}, 2, SnapshotCursor{}, 10)
	if err != nil || len(historicalPage.Objects) != 1 || historicalPage.Objects[0].ID != firstObject.ID {
		t.Fatalf("historical lineage snapshot = %+v, %v", historicalPage, err)
	}
	lineageChanges, err := store.ChangesSince(t.Context(), provision.Tenant, EnumerationScope{
		Kind: EnumerationContainer, Presentation: PresentationMount, Parent: root.ID,
	}, CompleteChangeCursor(1), 10)
	if err != nil || len(lineageChanges.Changes) != 2 ||
		lineageChanges.Changes[0].Object.ID != firstObject.ID ||
		lineageChanges.Changes[1].Object.ID != secondObject.ID {
		t.Fatalf("lineage changes = %+v, %v", lineageChanges, err)
	}
}

func activeFileProviderFingerprint(t *testing.T, store *Catalog, tenant TenantID) [32]byte {
	t.Helper()
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	fingerprint, err := catalogFileProviderFingerprint(t.Context(), tx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

func TestActiveSourcePublicationHandlePinsPublicationHistory(t *testing.T) {
	store, provisions, _, _ := newSourceDriverCatalog(t, "active-handle")
	provision := provisions[0]
	root, err := store.Root(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	file := createTestFile(t, store, provision.Tenant, root.ID, "content", "payload")
	if _, err := store.AddInterest(
		t.Context(), provision.Tenant, file.Revision, file.ID,
		fileProviderInterestOwner("active-handle-domain"), file.ContentRevision,
	); err != nil {
		t.Fatal(err)
	}
	publication := []byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3}
	insertVisibilityPublication(t, store, publication, nil, 1, 0, 1, file.Revision,
		[]Object{root, file}, []Object{root, file}, file)
	if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO source_driver_publication_heads(
    source_authority, publication_id, source_revision, epoch
) VALUES ('driver-authority', ?, 1, 1)`, publication); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(),
		`DELETE FROM objects WHERE tenant = ? AND object_id = ?`, string(provision.Tenant), file.ID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(),
		`DELETE FROM object_versions WHERE tenant = ? AND object_id = ?`, string(provision.Tenant), file.ID[:]); err != nil {
		t.Fatal(err)
	}
	handle, err := store.OpenAt(
		t.Context(), testRetentionOwner, provision.Tenant, PresentationFileProvider,
		provision.Generation, file.ID, file.Revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(handle)
	if err != nil || string(content) != "payload" {
		t.Fatalf("publication handle content = %q, %v", content, err)
	}
	var openedHead uint64
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT opened_head FROM handles WHERE handle_id = ?`, handle.Handle.ID[:]).Scan(&openedHead); err != nil {
		t.Fatal(err)
	}
	if Revision(openedHead) != file.Revision {
		t.Fatalf("opened head = %d, want %d", openedHead, file.Revision)
	}
	if err := store.VerifyMaterialization(
		t.Context(), provision.Tenant, provision.Generation, file.Revision,
	); err != nil {
		t.Fatalf("VerifyMaterialization(active publication): %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	tombstone := file
	tombstone.Revision = file.Revision + 1
	tombstone.MetadataRevision = tombstone.Revision
	tombstone.Tombstone = true
	tombstone.Visibility = Visibility{}
	successor := []byte{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}
	insertVisibilityPublication(t, store, successor, publication, 2, 1, file.Revision, tombstone.Revision,
		[]Object{root, tombstone}, []Object{tombstone}, tombstone)
	if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_driver_publication_heads
SET publication_id = ?, source_revision = 2, epoch = 2
WHERE source_authority = 'driver-authority' AND publication_id = ? AND epoch = 1`,
		successor, publication); err != nil {
		t.Fatal(err)
	}
	referenced, err := store.blobEntryReferenced(t.Context(), hex.EncodeToString(file.Hash[:]))
	if err != nil || !referenced {
		t.Fatalf("historical active-lineage blob referenced = %t, %v", referenced, err)
	}
}

func assertVisiblePublicationObject(
	t *testing.T,
	store *Catalog,
	provision TenantProvision,
	name string,
	want ObjectID,
	wantHead Revision,
) {
	t.Helper()
	head, err := store.Head(t.Context(), provision.Tenant)
	if err != nil || head != wantHead {
		t.Fatalf("Head = %d, %v, want %d", head, err, wantHead)
	}
	got, err := store.LookupName(t.Context(), provision.Tenant, PresentationMount, provision.Root, name)
	if err != nil || got.ID != want {
		t.Fatalf("LookupName(%s) = %+v, %v, want %s", name, got, err, want)
	}
	got, err = store.Lookup(t.Context(), provision.Tenant, PresentationMount, want)
	if err != nil || got.ID != want {
		t.Fatalf("Lookup(%s) = %+v, %v", want, got, err)
	}
	root, err := store.Root(t.Context(), provision.Tenant)
	if err != nil || root.ID != provision.Root {
		t.Fatalf("Root = %+v, %v, want %s", root, err, provision.Root)
	}
	page, err := store.Snapshot(t.Context(), provision.Tenant, EnumerationScope{
		Kind: EnumerationContainer, Presentation: PresentationMount, Parent: provision.Root,
	}, 0, SnapshotCursor{}, 10)
	if err != nil || page.Revision != wantHead {
		t.Fatalf("active snapshot = %+v, %v", page, err)
	}
	changes, err := store.ChangesSince(t.Context(), provision.Tenant, EnumerationScope{
		Kind: EnumerationContainer, Presentation: PresentationMount, Parent: provision.Root,
	}, CompleteChangeCursor(wantHead-1), 10)
	if err != nil || len(changes.Changes) != 1 || changes.Changes[0].Object.ID != want {
		t.Fatalf("active changes = %+v, %v, want one %s", changes, err, want)
	}
}

func insertVisibilityPublication(
	t *testing.T,
	store *Catalog,
	publication, predecessor []byte,
	sourceRevision, predecessorRevision uint64,
	predecessorHead, head Revision,
	objects, versions []Object,
	changed Object,
) {
	t.Helper()
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if predecessor == nil {
		predecessor = []byte{}
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_driver_target_epochs(source_authority, target_epoch)
VALUES ('driver-authority', 1)
ON CONFLICT(source_authority) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	sourceOperation := causal.OperationID(publication)
	changeID := causal.ChangeID(publication)
	changeID[0] ^= 0x80
	affectedDigest := sha256.Sum256([]byte("visibility"))
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_driver_publications(
    source_authority, publication_id, source_operation_id, change_id, cause,
    origin_domain, origin_generation, affected_key_count, affected_keys_digest,
    identity_digest, target_count, targets_digest,
    stage_sequence, stage_item_count, stage_byte_count, stage_digest, predecessor_publication_id,
    predecessor_revision, source_revision, expected_visibility_epoch, target_epoch,
	phase, cursor_tenant, cursor_key, initialized_target_count, prepared_target_count,
	item_count, byte_count, rolling_digest, prepared
) VALUES ('driver-authority', ?, ?, ?, 'external_unattributed', '', 0, 1, ?, ?, 1, ?, 1, 1, 1, ?, ?, ?, ?, ?,
          (SELECT target_epoch FROM source_driver_target_epochs WHERE source_authority = 'driver-authority'),
          16, '', '', 1, 1, ?, 0, ?, 1)`,
		publication, sourceOperation[:], changeID[:], affectedDigest[:],
		make([]byte, 32), make([]byte, 32), make([]byte, 32), predecessor,
		predecessorRevision, sourceRevision, sourceRevision-1, len(objects), make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	catalogOperation := sourceCatalogOperation(sourceOperation, changed.Tenant)
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_driver_publication_targets(
    source_authority, publication_id, tenant, generation, root_key, catalog_operation_id,
    predecessor_head, catalog_head, catalog_fingerprint, file_provider_fingerprint,
    changed, provider_changed, object_count, phase, cursor_key, cursor_object_id,
    cursor_revision, catalog_state, provider_state, next_change_sequence, prepared
) VALUES ('driver-authority', ?, ?, ?, 'root', ?, ?, ?, ?, ?, 1, 1, ?, 16, '', X'', 0, X'', X'', 1, 1)`,
		publication, string(changed.Tenant), uint64(1), catalogOperation[:],
		uint64(predecessorHead), uint64(head),
		make([]byte, 32), make([]byte, 32), len(objects)); err != nil {
		t.Fatal(err)
	}
	for index := range objects {
		insertPublicationObject(t, tx, publication, "key-"+objects[index].Name, objects[index], true)
	}
	for index := range versions {
		insertPublicationObject(t, tx, publication, "", versions[index], false)
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_driver_publication_changes(
    source_authority, publication_id, tenant, revision, scope_kind, presentation,
    scope_parent, scope_domain, scope_generation, sequence, kind, object_id, object_revision
) VALUES ('driver-authority', ?, ?, ?, ?, ?, ?, '', 0, 0, ?, ?, ?)`,
		publication, string(changed.Tenant), uint64(changed.Revision), uint8(EnumerationContainer),
		uint8(PresentationMount), changed.Parent[:], uint8(ChangeUpsert), changed.ID[:], uint64(changed.Revision)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func insertPublicationObject(
	t *testing.T,
	tx *sql.Tx,
	publication []byte,
	key string,
	obj Object,
	current bool,
) {
	t.Helper()
	args := objectArgs(obj, normalizeName(CaseSensitive, obj.Name))
	if current {
		values := append([]any{"driver-authority", publication, string(obj.Tenant), key}, args[1:]...)
		if _, err := tx.ExecContext(context.Background(), `
INSERT INTO source_driver_publication_objects(
    source_authority, publication_id, tenant, source_key, object_id, parent_id,
    revision, metadata_revision, content_revision, name, name_key, kind, mode, size,
    hash, link_target, desired_revision, observed_revision, verified_revision,
    applied_revision, mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, values...); err != nil {
			t.Fatal(err)
		}
		return
	}
	values := append([]any{"driver-authority", publication}, args...)
	if _, err := tx.ExecContext(context.Background(), `
INSERT INTO source_driver_publication_versions(
    source_authority, publication_id, tenant, object_id, parent_id, revision,
    metadata_revision, content_revision, name, name_key, kind, mode, size, hash,
    link_target, desired_revision, observed_revision, verified_revision,
    applied_revision, mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, values...); err != nil {
		t.Fatal(err)
	}
}
