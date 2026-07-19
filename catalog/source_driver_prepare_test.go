package catalog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
)

func TestSourceDriverPreparationIsBoundedDurableAndInvisible(t *testing.T) {
	store, provisions, declaration, targets := newSourceDriverCatalog(t, "prepared-driver")
	baseline, err := store.Create(t.Context(), provisions[0].Tenant, CreateSpec{
		Parent: provisions[0].Root, Name: "baseline-source-object", Kind: KindDirectory,
		Visibility: Visibility{Mount: true, FileProvider: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO source_object_ids(source_authority, source_key, object_id) VALUES (?, ?, ?)`,
		"driver-authority", "baseline-source-object", baseline.ID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO source_object_bindings(source_authority, tenant, source_key) VALUES (?, ?, ?)`,
		"driver-authority", string(provisions[0].Tenant), "baseline-source-object"); err != nil {
		t.Fatal(err)
	}
	var baselineChanges, baselineHead int
	if err := store.db.QueryRowContext(t.Context(), `
SELECT
  (SELECT COUNT(*) FROM changes WHERE tenant = ? AND object_id = ?),
  (SELECT head FROM tenants WHERE tenant = ?)`, string(provisions[0].Tenant), baseline.ID[:],
		string(provisions[0].Tenant)).Scan(&baselineChanges, &baselineHead); err != nil {
		t.Fatal(err)
	}
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "prepared-token", 0, 57,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	content := stageTestContent(t, store, "prepared source content")
	entries := make([]SourceDriverStageEntry, 300)
	for index := range entries {
		key := SourceObjectKey(fmt.Sprintf("prepared-%03d", index))
		entries[index] = SourceDriverStageEntry{
			Tenant: provisions[0].Tenant, Generation: provisions[0].Generation, Key: key,
			Object: &SourceObject{
				Key: key, Name: string(key), Kind: KindDirectory,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}
	}
	entries[0].Object.Kind = KindFile
	entries[0].Object.ContentRevision = 1
	entries[0].Object.Content = content
	first, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{
			Digest: [sha256.Size]byte{57}, Cursor: []byte("prepared-next"), Entries: entries[:256],
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(
		first, SourceDriverStagePage{Digest: [sha256.Size]byte{58}, Complete: true, Entries: entries[256:]},
	)); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("preparation interrupted")
	store.failpoint = func(point string) error {
		if point == sourceDriverPreparationAfterBatchPoint {
			return boom
		}
		return nil
	}
	if _, err := store.PrepareSourceDriverPublicationBatch(t.Context(), identity); !errors.Is(err, boom) {
		t.Fatalf("PrepareSourceDriverPublicationBatch = %v, want failpoint", err)
	}
	store.failpoint = nil

	var path string
	if err := store.db.QueryRowContext(t.Context(), `
SELECT file FROM pragma_database_list WHERE name = 'main'`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	previous, err := readSourceDriverPreparationState(t.Context(), reopened.readDB, identity)
	if err != nil {
		t.Fatal(err)
	}
	phases := make(map[uint8]int)
	seededInterests := false
	for calls := 0; !previous.Prepared; calls++ {
		if calls > 64 {
			t.Fatalf("preparation did not converge: %+v", previous)
		}
		next, err := reopened.PrepareSourceDriverPublicationBatch(t.Context(), identity)
		if err != nil {
			t.Fatalf("preparation call %d after %+v: %v", calls, previous, err)
		}
		if next.Rows < previous.Rows || next.Bytes < previous.Bytes ||
			next.Rows-previous.Rows > sourceDriverPreparationPageLimit ||
			next.Bytes-previous.Bytes > sourceDriverPreparationByteLimit {
			t.Fatalf("unbounded preparation step: before=%+v after=%+v", previous, next)
		}
		phases[next.TargetPhase]++
		if !seededInterests && next.TargetPhase == sourceDriverTargetValidate {
			var objectID []byte
			if err := reopened.db.QueryRowContext(t.Context(), `
SELECT object_id FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND revision = (SELECT predecessor_head + 1 FROM source_driver_publication_targets
                  WHERE source_authority = ? AND publication_id = ? AND tenant = ?)
ORDER BY source_key LIMIT 1`, string(identity.Authority), identity.Operation[:],
				string(provisions[0].Tenant), string(identity.Authority), identity.Operation[:],
				string(provisions[0].Tenant)).Scan(&objectID); err != nil {
				t.Fatal(err)
			}
			for index := 0; index < 200; index++ {
				interestID, err := NewObjectID()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := reopened.db.ExecContext(t.Context(), `
INSERT INTO materialization_interests(
    interest_id, tenant, object_id, owner_presentation, owner_domain,
    owner_generation, desired_revision, created_revision, removed_revision
) VALUES (?, ?, ?, 2, ?, 1, 1, 1, NULL)`, interestID[:], string(provisions[0].Tenant),
					objectID, fmt.Sprintf("prepared-domain-%03d", index)); err != nil {
					t.Fatal(err)
				}
			}
			seededInterests = true
		}
		previous = next
	}
	if previous.Published || previous.PreparedTargets != 1 || previous.TargetCount != 1 ||
		previous.Digest == ([sha256.Size]byte{}) {
		t.Fatalf("terminal preparation = %+v", previous)
	}
	var objects, candidateObjects, versions, changes, targetsPrepared, targetPredecessor, visibilityRevision, tenantHead, convergence, contentClaims int
	if err := reopened.readDB.QueryRowContext(t.Context(), `
SELECT
  (SELECT COUNT(*) FROM source_driver_publication_objects
   WHERE source_authority = ? AND publication_id = ? AND tombstone = 0),
  (SELECT COUNT(*) FROM source_driver_publication_objects
	   WHERE source_authority = ? AND publication_id = ?
	     AND revision = (SELECT predecessor_head + 1 FROM source_driver_publication_targets
	                     WHERE source_authority = ? AND publication_id = ?)),
  (SELECT COUNT(*) FROM source_driver_publication_versions
   WHERE source_authority = ? AND publication_id = ?),
  (SELECT COUNT(*) FROM source_driver_publication_changes
   WHERE source_authority = ? AND publication_id = ?),
  (SELECT COUNT(*) FROM source_driver_publication_targets
   WHERE source_authority = ? AND publication_id = ? AND prepared = 1),
  (SELECT predecessor_head FROM source_driver_publication_targets
   WHERE source_authority = ? AND publication_id = ?),
  (SELECT active_source_revision FROM source_driver_visibility WHERE source_authority = ?),
  (SELECT head FROM tenants WHERE tenant = ?),
	  (SELECT COUNT(*) FROM convergence_changes WHERE source_authority = ?),
	  (SELECT COUNT(*) FROM content_stages WHERE source_operation_id = ?)`,
		string(identity.Authority), identity.Operation[:], string(identity.Authority), identity.Operation[:],
		string(identity.Authority), identity.Operation[:],
		string(identity.Authority), identity.Operation[:],
		string(identity.Authority), identity.Operation[:],
		string(identity.Authority), identity.Operation[:],
		string(identity.Authority), identity.Operation[:],
		string(identity.Authority), string(provisions[0].Tenant), string(identity.Authority), identity.Operation[:]).Scan(
		&objects, &candidateObjects, &versions, &changes, &targetsPrepared, &targetPredecessor,
		&visibilityRevision, &tenantHead, &convergence, &contentClaims,
	); err != nil {
		t.Fatal(err)
	}
	if objects != len(entries)+2 || candidateObjects != len(entries) || versions != len(entries)+2 ||
		changes != len(entries)*2+200+baselineChanges || targetsPrepared != 1 ||
		visibilityRevision != 0 || tenantHead != baselineHead || convergence != 0 || contentClaims != 0 {
		t.Fatalf("prepared state objects=%d candidate=%d versions=%d changes=%d (want %d) targets=%d predecessor=%d visibility=%d head=%d (want %d) convergence=%d claims=%d phases=%v",
			objects, candidateObjects, versions, changes, len(entries)*2+200+baselineChanges,
			targetsPrepared, targetPredecessor, visibilityRevision, tenantHead, baselineHead,
			convergence, contentClaims, phases)
	}
}

func TestSourceDriverTargetDeclarationIsPagedAndEpochFenced(t *testing.T) {
	names := make([]string, sourceDriverTargetBatchSize+1)
	for index := range names {
		names[index] = fmt.Sprintf("target-page-%03d", index)
	}
	store, provisions, declaration, targets := newSourceDriverCatalog(t, names...)
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "target-page-token", 0, 59,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	var declared, normalized int
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT
  (SELECT COUNT(*) FROM source_driver_stage_targets
   WHERE source_authority = ? AND stage_operation_id = ?),
  (SELECT COUNT(*) FROM source_publication_stage_revisions
   WHERE source_authority = ? AND stage_operation_id = ?)`,
		string(identity.Authority), identity.Operation[:], string(identity.Authority), identity.Operation[:]).Scan(
		&declared, &normalized,
	); err != nil {
		t.Fatal(err)
	}
	if declared != 0 || normalized != 0 {
		t.Fatalf("BeginSourceDriverStage declared=%d normalized=%d, want fixed-row begin", declared, normalized)
	}
	state, err := store.PrepareSourceDriverTargetDeclarationBatch(t.Context(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if state.DeclaredCount != sourceDriverTargetBatchSize || state.TargetEpoch == 0 || state.Prepared {
		t.Fatalf("first target page = %+v", state)
	}
	page, err := store.SourceDriverStageTargets(
		t.Context(), identity.Authority, identity.Operation, "", sourceDriverTargetBatchSize,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != sourceDriverTargetBatchSize || page[0] != targets[0] || page[len(page)-1] != targets[len(page)-1] {
		t.Fatalf("persisted target page has wrong exact bounds: len=%d first=%+v want=%+v last=%+v want=%+v", len(page), page[0], targets[0], page[len(page)-1], targets[len(page)-1])
	}
	var path string
	if err := store.db.QueryRowContext(t.Context(), `
SELECT file FROM pragma_database_list WHERE name = 'main'`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	replayed, err := reopened.SourceDriverStageTargets(
		t.Context(), identity.Authority, identity.Operation, "", sourceDriverTargetBatchSize,
	)
	if err != nil || len(replayed) != len(page) {
		t.Fatalf("reopened target page len=%d err=%v", len(replayed), err)
	}
	for index := range page {
		if replayed[index] != page[index] {
			t.Fatalf("reopened target %d = %+v, want %+v", index, replayed[index], page[index])
		}
	}
	for calls := 0; !state.Prepared; calls++ {
		if calls > 2 {
			t.Fatalf("target declaration did not converge: %+v", state)
		}
		state, err = reopened.PrepareSourceDriverTargetDeclarationBatch(t.Context(), identity)
		if err != nil {
			t.Fatal(err)
		}
	}
	if state.DeclaredCount != uint64(len(targets)) || state.Digest != identity.TargetsDigest {
		t.Fatalf("completed target declaration = %+v", state)
	}
	if _, err := reopened.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{
			Digest: [sha256.Size]byte{59}, Complete: true,
		},
	)); err != nil {
		t.Fatal(err)
	}
	if err := reopened.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_publication_stage_revisions
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&normalized); err != nil {
		t.Fatal(err)
	}
	if normalized != 0 {
		t.Fatalf("terminal AppendSourceDriverStage normalized %d revisions", normalized)
	}
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT
  (SELECT COUNT(*) FROM source_driver_stage_targets
   WHERE source_authority = ? AND stage_operation_id = ?),
  (SELECT COUNT(*) FROM source_driver_publications
   WHERE source_authority = ? AND publication_id = ?)`,
		string(identity.Authority), identity.Operation[:], string(identity.Authority), identity.Operation[:]).Scan(
		&declared, &normalized,
	); err != nil {
		t.Fatal(err)
	}
	if declared != len(targets) || normalized != 0 {
		t.Fatalf("completed target declaration=%d publication=%d", declared, normalized)
	}
	if _, err := reopened.db.ExecContext(t.Context(), `
UPDATE desired_tenants SET generation = generation + 1 WHERE tenant = ?`,
		string(provisions[0].Tenant)); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.PrepareSourceDriverPublicationBatch(t.Context(), identity); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("preparation after target epoch change = %v, want mutation conflict", err)
	}
}
