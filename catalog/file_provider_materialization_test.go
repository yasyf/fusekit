package catalog

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestFileProviderMaterializationSnapshotIsDurableExactAndSemanticNoOpOnSameSet(t *testing.T) {
	c, created, domain := newMaterializationFixture(t, "materialization-replay")
	identity := newMaterializationIdentity(t, created, domain, "backing-a")
	catalogHead := mustCatalogHead(t, c, created.Tenant)
	targetingBefore := mustTargetingRevision(t, c, created.Tenant)

	first := commitMaterializationForTest(t, c, identity, created.Root)
	if first.Revision != 1 || first.Added != 1 || first.Removed != 0 {
		t.Fatalf("first commit = %+v", first)
	}
	targetingAfter := mustTargetingRevision(t, c, created.Tenant)
	if targetingAfter <= targetingBefore {
		t.Fatalf("targeting revision did not advance: %d -> %d", targetingBefore, targetingAfter)
	}
	if replay, err := c.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: identity, PageCount: 1,
	}); err != nil || replay != first {
		t.Fatalf("lost-response replay = %+v, %v; want %+v", replay, err, first)
	}

	secondIdentity := newMaterializationIdentity(t, created, domain, "backing-a")
	second := commitMaterializationForTest(t, c, secondIdentity, created.Root)
	if second.Revision != first.Revision || second.Added != 0 || second.Removed != 0 {
		t.Fatalf("same-set commit = %+v; want revision %d and zero diff", second, first.Revision)
	}
	if got := mustTargetingRevision(t, c, created.Tenant); got != targetingAfter {
		t.Fatalf("same-set targeting revision = %d, want %d", got, targetingAfter)
	}
	if got := mustCatalogHead(t, c, created.Tenant); got != catalogHead {
		t.Fatalf("materialization changed catalog head = %d, want %d", got, catalogHead)
	}

	path := catalogDatabasePath(t, c)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if replay, err := reopened.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: secondIdentity, PageCount: 1,
	}); err != nil || replay != second {
		t.Fatalf("restart replay = %+v, %v; want %+v", replay, err, second)
	}
}

func TestFileProviderMaterializationNewerBeginFencesOlderCommit(t *testing.T) {
	c, created, domain := newMaterializationFixture(t, "materialization-fence")
	older := newMaterializationIdentity(t, created, domain, "backing")
	newer := newMaterializationIdentity(t, created, domain, "backing")
	beginAndStageMaterializationForTest(t, c, older, created.Root)
	beginAndStageMaterializationForTest(t, c, newer, created.Root)
	if _, err := c.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: older, PageCount: 1,
	}); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("stale commit = %v, want ErrGenerationMismatch", err)
	}
	result, err := c.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: newer, PageCount: 1,
	})
	if err != nil || result.Revision != 1 {
		t.Fatalf("newest commit = %+v, %v", result, err)
	}
}

func TestFileProviderMaterializationConcurrentBeginsAllocateDistinctOwnerEpochs(t *testing.T) {
	c, created, domain := newMaterializationFixture(t, "materialization-concurrent")
	identities := []FileProviderMaterializationIdentity{
		newMaterializationIdentity(t, created, domain, "backing"),
		newMaterializationIdentity(t, created, domain, "backing"),
	}
	start := make(chan struct{})
	type outcome struct {
		epoch uint64
		err   error
	}
	outcomes := make([]outcome, len(identities))
	var wait sync.WaitGroup
	for index := range identities {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			outcomes[index].epoch, outcomes[index].err = c.BeginFileProviderMaterializationSnapshot(t.Context(), identities[index])
		}(index)
	}
	close(start)
	wait.Wait()
	for index, outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf("begin %d: %v", index, outcome.err)
		}
	}
	if outcomes[0].epoch == outcomes[1].epoch {
		t.Fatalf("concurrent epochs alias: %+v", outcomes)
	}
}

func TestFileProviderMaterializationBackingIdentityAndNilFenceLastGoodSet(t *testing.T) {
	c, created, domain := newMaterializationFixture(t, "materialization-backing")
	first := newMaterializationIdentity(t, created, domain, "backing-a")
	commitMaterializationForTest(t, c, first, created.Root)
	if demand, err := c.HasEligibleFileProviderMaterializedContainers(t.Context(), created.Tenant); err != nil || !demand {
		t.Fatalf("initial demand = %t, %v", demand, err)
	}
	second := newMaterializationIdentity(t, created, domain, "backing-b")
	if _, err := c.BeginFileProviderMaterializationSnapshot(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if demand, err := c.HasEligibleFileProviderMaterializedContainers(t.Context(), created.Tenant); err != nil || demand {
		t.Fatalf("changed-identity demand = %t, %v", demand, err)
	}
	if err := c.SuspendFileProviderMaterialization(t.Context(), created.Tenant, domain.DomainID, created.Generation); err != nil {
		t.Fatal(err)
	}
	var retained int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM file_provider_materialized_containers WHERE tenant = ?`, string(created.Tenant)).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 1 {
		t.Fatalf("nil backing identity discarded last-good set: %d", retained)
	}
}

func TestFileProviderMaterializationRejectsPartialNonterminalPage(t *testing.T) {
	c, created, domain := newMaterializationFixture(t, "materialization-page-boundary")
	identity := newMaterializationIdentity(t, created, domain, "backing")
	if _, err := c.BeginFileProviderMaterializationSnapshot(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	page := FileProviderMaterializationPage{Identity: identity, Sequence: 0, IDs: []ObjectID{created.Root}}
	if err := c.StageFileProviderMaterializationPage(t.Context(), page); err != nil {
		t.Fatal(err)
	}
	if err := c.StageFileProviderMaterializationPage(t.Context(), page); err != nil {
		t.Fatalf("exact page replay: %v", err)
	}
	if err := c.StageFileProviderMaterializationPage(t.Context(), FileProviderMaterializationPage{
		Identity: identity, Sequence: 1, IDs: []ObjectID{created.Root},
	}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("page after partial nonterminal = %v, want ErrInvalidTransition", err)
	}
}

func TestFileProviderMaterializationTenThousandUnchangedCommitUsesAccumulatorOnly(t *testing.T) {
	c, created, domain := newMaterializationFixture(t, "materialization-10k-unchanged")
	first := commitMaterializationForTest(t, c,
		newMaterializationIdentity(t, created, domain, "backing"), created.Root)
	identity := newMaterializationIdentity(t, created, domain, "backing")
	if _, err := c.BeginFileProviderMaterializationSnapshot(t.Context(), identity); err != nil {
		t.Fatal(err)
	}

	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var last ObjectID
	for page := uint32(0); page < 10; page++ {
		ids := make([]ObjectID, FileProviderMaterializationPageLimit)
		for index := range ids {
			binary.BigEndian.PutUint64(ids[index][8:], uint64(page)*FileProviderMaterializationPageLimit+uint64(index)+1)
			last = ids[index]
		}
		digest := materializationPageDigest(ids)
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO file_provider_materialization_snapshot_pages(snapshot_id, sequence, page_hash, item_count)
VALUES (?, ?, ?, ?)`, identity.Snapshot[:], page, digest[:], len(ids)); err != nil {
			t.Fatal(err)
		}
		for _, id := range ids {
			if _, err := tx.ExecContext(t.Context(), `
INSERT INTO file_provider_materialization_snapshot_items(snapshot_id, sequence, container_id)
VALUES (?, ?, ?)`, identity.Snapshot[:], page, id[:]); err != nil {
				t.Fatal(err)
			}
		}
	}
	var headDigest []byte
	if err := tx.QueryRowContext(t.Context(), `
SELECT set_digest FROM file_provider_materialization_heads WHERE tenant = ?`, string(created.Tenant)).Scan(&headDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
UPDATE file_provider_materialization_snapshots
SET page_count = 10, item_count = 10000, last_page_count = 1000,
    last_container_id = ?, set_digest = ?
WHERE snapshot_id = ?`, last[:], headDigest, identity.Snapshot[:]); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := c.db.ExecContext(t.Context(), `
CREATE TABLE test_materialization_commit_writes(kind TEXT NOT NULL);
CREATE TRIGGER test_materialization_item_delete AFTER DELETE ON file_provider_materialization_snapshot_items
BEGIN INSERT INTO test_materialization_commit_writes(kind) VALUES ('item-delete'); END;
CREATE TRIGGER test_materialization_member_insert AFTER INSERT ON file_provider_materialized_containers
BEGIN INSERT INTO test_materialization_commit_writes(kind) VALUES ('member-insert'); END;
CREATE TRIGGER test_materialization_member_update AFTER UPDATE ON file_provider_materialized_containers
BEGIN INSERT INTO test_materialization_commit_writes(kind) VALUES ('member-update'); END;
CREATE TRIGGER test_materialization_member_delete AFTER DELETE ON file_provider_materialized_containers
BEGIN INSERT INTO test_materialization_commit_writes(kind) VALUES ('member-delete'); END;`); err != nil {
		t.Fatal(err)
	}
	catalogHead := mustCatalogHead(t, c, created.Tenant)
	targetingHead := mustTargetingRevision(t, c, created.Tenant)
	var changesBefore, outboxBefore uint64
	if err := c.readDB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM changes`).Scan(&changesBefore); err != nil {
		t.Fatal(err)
	}
	if err := c.readDB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM activation_outbox`).Scan(&outboxBefore); err != nil {
		t.Fatal(err)
	}
	result, err := c.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: identity, PageCount: 10,
	})
	if err != nil || result != (FileProviderMaterializationResult{Revision: first.Revision}) {
		t.Fatalf("unchanged 10k commit = %+v, %v", result, err)
	}
	var writes, retained, committed uint64
	if err := c.readDB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM test_materialization_commit_writes`).Scan(&writes); err != nil {
		t.Fatal(err)
	}
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM file_provider_materialization_snapshot_items WHERE snapshot_id = ?`, identity.Snapshot[:]).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM file_provider_materialization_snapshots WHERE snapshot_id = ? AND state = ?`,
		identity.Snapshot[:], materializationSnapshotCommitted).Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if writes != 0 || retained != 10_000 || committed != 1 {
		t.Fatalf("final commit writes=%d retained=%d receipts=%d", writes, retained, committed)
	}
	if got := mustCatalogHead(t, c, created.Tenant); got != catalogHead {
		t.Fatalf("catalog head = %d, want %d", got, catalogHead)
	}
	if got := mustTargetingRevision(t, c, created.Tenant); got != targetingHead {
		t.Fatalf("targeting head = %d, want %d", got, targetingHead)
	}
	var changesAfter, outboxAfter uint64
	if err := c.readDB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM changes`).Scan(&changesAfter); err != nil {
		t.Fatal(err)
	}
	if err := c.readDB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM activation_outbox`).Scan(&outboxAfter); err != nil {
		t.Fatal(err)
	}
	if changesAfter != changesBefore || outboxAfter != outboxBefore {
		t.Fatalf("semantic emissions changed: changes %d->%d outbox %d->%d",
			changesBefore, changesAfter, outboxBefore, outboxAfter)
	}

	pageZero := make([]ObjectID, FileProviderMaterializationPageLimit)
	for index := range pageZero {
		binary.BigEndian.PutUint64(pageZero[index][8:], uint64(index)+1)
	}
	var stagedBefore int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*)
FROM file_provider_materialization_snapshot_items item
JOIN file_provider_materialization_snapshots snapshot ON snapshot.snapshot_id = item.snapshot_id
WHERE snapshot.state = ?`, materializationSnapshotCommitted).Scan(&stagedBefore); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE catalog_global_maintenance SET next_phase = ? WHERE singleton = 1`,
		globalMaintenanceFileProviderMaterializationStages); err != nil {
		t.Fatal(err)
	}
	maintenance, err := c.MaintainGlobal(t.Context(), testMaintenanceNow())
	if err != nil || maintenance.Phase != MaintenanceFileProviderMaterializationStages ||
		maintenance.Retired < 1 || maintenance.Retired > MaintenancePageLimit || !maintenance.More {
		t.Fatalf("first materialization stage maintenance = %+v, %v", maintenance, err)
	}
	if err := c.StageFileProviderMaterializationPage(t.Context(), FileProviderMaterializationPage{
		Identity: identity, Sequence: 0, IDs: pageZero,
	}); err != nil {
		t.Fatalf("exact page replay after member reclamation: %v", err)
	}
	if err := c.StageFileProviderMaterializationPage(t.Context(), FileProviderMaterializationPage{
		Identity: identity, Sequence: 0, IDs: []ObjectID{created.Root},
	}); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("conflicting page replay after member reclamation = %v, want ErrMutationConflict", err)
	}
	retired := maintenance.Retired
	for call := 0; maintenance.More && call < stagedBefore/MaintenancePageLimit+2; call++ {
		count, more, compactErr := c.compactFileProviderMaterializationStagePage(t.Context())
		if compactErr != nil {
			t.Fatalf("materialization stage maintenance %d: %v", call, compactErr)
		}
		if count < 1 || count > MaintenancePageLimit {
			t.Fatalf("materialization stage maintenance %d retired %d", call, count)
		}
		retired += count
		maintenance.More = more
	}
	if maintenance.More || retired != stagedBefore {
		t.Fatalf("materialization stage maintenance retired=%d/%d more=%t", retired, stagedBefore, maintenance.More)
	}
	var stagedAfter, pageReceipts int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM file_provider_materialization_snapshot_items`).Scan(&stagedAfter); err != nil {
		t.Fatal(err)
	}
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM file_provider_materialization_snapshot_pages WHERE snapshot_id = ?`,
		identity.Snapshot[:]).Scan(&pageReceipts); err != nil {
		t.Fatal(err)
	}
	if stagedAfter != 0 || pageReceipts != 10 {
		t.Fatalf("reclaimed members=%d page receipts=%d", stagedAfter, pageReceipts)
	}
	if replay, err := c.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: identity, PageCount: 10,
	}); err != nil || replay != result {
		t.Fatalf("commit replay after member reclamation = %+v, %v; want %+v", replay, err, result)
	}

	collecting := newMaterializationIdentity(t, created, domain, "backing")
	beginAndStageMaterializationForTest(t, c, collecting, created.Root)
	if count, more, err := c.compactFileProviderMaterializationStagePage(t.Context()); err != nil || count != 0 || more {
		t.Fatalf("collecting snapshot maintenance = %d, %t, %v", count, more, err)
	}
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM file_provider_materialization_snapshot_items WHERE snapshot_id = ?`,
		collecting.Snapshot[:]).Scan(&stagedAfter); err != nil || stagedAfter != 1 {
		t.Fatalf("collecting snapshot members = %d, %v", stagedAfter, err)
	}
	current := newMaterializationIdentity(t, created, domain, "backing")
	beginAndStageMaterializationForTest(t, c, current, created.Root)
	var supersededState int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT state FROM file_provider_materialization_snapshots WHERE snapshot_id = ?`,
		collecting.Snapshot[:]).Scan(&supersededState); err != nil || supersededState != materializationSnapshotSuperseded {
		t.Fatalf("superseded snapshot state = %d, %v", supersededState, err)
	}
	if count, more, err := c.compactFileProviderMaterializationStagePage(t.Context()); err != nil || count != 1 || more {
		t.Fatalf("superseded collecting snapshot maintenance = %d, %t, %v", count, more, err)
	}
	var supersededMembers, currentMembers int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM file_provider_materialization_snapshot_items WHERE snapshot_id = ?`,
		collecting.Snapshot[:]).Scan(&supersededMembers); err != nil {
		t.Fatal(err)
	}
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM file_provider_materialization_snapshot_items WHERE snapshot_id = ?`,
		current.Snapshot[:]).Scan(&currentMembers); err != nil {
		t.Fatal(err)
	}
	if supersededMembers != 0 || currentMembers != 1 {
		t.Fatalf("collecting members superseded=%d current=%d", supersededMembers, currentMembers)
	}
	if err := c.StageFileProviderMaterializationPage(t.Context(), FileProviderMaterializationPage{
		Identity: collecting, Sequence: 0, IDs: []ObjectID{created.Root},
	}); err != nil {
		t.Fatalf("superseded page replay after member reclamation: %v", err)
	}
	if _, err := c.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: collecting, PageCount: 1,
	}); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("superseded commit after member reclamation = %v, want ErrGenerationMismatch", err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
DROP TRIGGER test_materialization_item_delete;
DROP TRIGGER test_materialization_member_insert;
DROP TRIGGER test_materialization_member_update;
DROP TRIGGER test_materialization_member_delete;
DROP TABLE test_materialization_commit_writes;`); err != nil {
		t.Fatal(err)
	}

	path := catalogDatabasePath(t, c)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.StageFileProviderMaterializationPage(t.Context(), FileProviderMaterializationPage{
		Identity: identity, Sequence: 0, IDs: pageZero,
	}); err != nil {
		t.Fatalf("page replay after reclamation restart: %v", err)
	}
	if replay, err := reopened.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: identity, PageCount: 10,
	}); err != nil || replay != result {
		t.Fatalf("commit replay after reclamation restart = %+v, %v; want %+v", replay, err, result)
	}
	var reclaimed int
	if err := reopened.readDB.QueryRowContext(t.Context(), `
SELECT members_reclaimed FROM file_provider_materialization_snapshots WHERE snapshot_id = ?`,
		identity.Snapshot[:]).Scan(&reclaimed); err != nil || reclaimed != 1 {
		t.Fatalf("reclamation cursor after restart = %d, %v", reclaimed, err)
	}
	phase, err := reopened.claimGlobalMaintenancePhase(t.Context())
	if err != nil || phase != globalMaintenanceSourceHistory {
		t.Fatalf("global phase after phase-8 rotation restart = %d, %v", phase, err)
	}
}

func TestFileProviderMaterializationReclamationQueryPlanIsKeyed(t *testing.T) {
	c, _, _ := newMaterializationFixture(t, "materialization-reclamation-plan")
	candidate := materializationQueryPlan(t, c, `
SELECT snapshot_id
FROM file_provider_materialization_snapshots
WHERE state IN (?, ?) AND members_reclaimed = 0
ORDER BY state, snapshot_id
LIMIT 1`, materializationSnapshotCommitted, materializationSnapshotSuperseded)
	if strings.Contains(candidate, "USE TEMP B-TREE") ||
		strings.Contains(candidate, "SCAN file_provider_materialization_snapshots") ||
		!strings.Contains(candidate, "file_provider_materialization_snapshots_reclaim") {
		t.Fatalf("candidate plan is not reclaim-indexed:\n%s", candidate)
	}
	assertBoundedMaterializationItemPlan(t, "count", materializationQueryPlan(t, c, `
SELECT COUNT(*) FROM (
    SELECT 1
    FROM file_provider_materialization_snapshot_items
    WHERE snapshot_id = ?
    ORDER BY container_id
    LIMIT ?
)`, make([]byte, len(MaterializationSnapshotID{})), MaintenancePageLimit+1))
	assertBoundedMaterializationItemPlan(t, "delete", materializationQueryPlan(t, c, `
DELETE FROM file_provider_materialization_snapshot_items
WHERE rowid IN (
    SELECT rowid
    FROM file_provider_materialization_snapshot_items
    WHERE snapshot_id = ?
    ORDER BY container_id
    LIMIT ?
)`, make([]byte, len(MaterializationSnapshotID{})), MaintenancePageLimit))
}

func TestFileProviderMaterializationSuspendSupersedesCollectingSnapshot(t *testing.T) {
	c, created, domain := newMaterializationFixture(t, "materialization-suspend-reclamation")
	identity := newMaterializationIdentity(t, created, domain, "backing")
	beginAndStageMaterializationForTest(t, c, identity, created.Root)
	if err := c.SuspendFileProviderMaterialization(
		t.Context(), created.Tenant, domain.DomainID, created.Generation,
	); err != nil {
		t.Fatal(err)
	}
	var state int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT state FROM file_provider_materialization_snapshots WHERE snapshot_id = ?`,
		identity.Snapshot[:]).Scan(&state); err != nil || state != materializationSnapshotSuperseded {
		t.Fatalf("suspended snapshot state = %d, %v", state, err)
	}
	if retired, more, err := c.compactFileProviderMaterializationStagePage(t.Context()); err != nil || retired != 1 || more {
		t.Fatalf("suspended snapshot maintenance = %d, %t, %v", retired, more, err)
	}
	if err := c.StageFileProviderMaterializationPage(t.Context(), FileProviderMaterializationPage{
		Identity: identity, Sequence: 0, IDs: []ObjectID{created.Root},
	}); err != nil {
		t.Fatalf("suspended page replay after reclamation: %v", err)
	}
	if _, err := c.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: identity, PageCount: 1,
	}); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("suspended commit after reclamation = %v, want ErrGenerationMismatch", err)
	}
}

func materializationQueryPlan(t *testing.T, c *Catalog, query string, arguments ...any) string {
	t.Helper()
	rows, err := c.readDB.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, arguments...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(details, "\n")
}

func assertBoundedMaterializationItemPlan(t *testing.T, operation, plan string) {
	t.Helper()
	if strings.Contains(plan, "USE TEMP B-TREE") ||
		strings.Contains(plan, "SCAN file_provider_materialization_snapshot_items") ||
		!strings.Contains(plan, "SEARCH file_provider_materialization_snapshot_items USING COVERING INDEX") {
		t.Fatalf("%s plan is not bounded by the snapshot-items key:\n%s", operation, plan)
	}
}

func newMaterializationFixture(t *testing.T, name string) (*Catalog, TenantProvision, FileProviderDomain) {
	t.Helper()
	c := openDomainTestCatalog(t)
	provision := testTenantProvision(t, name, 1)
	created, err := provisionTenantForTest(t, c, t.Context(), provision)
	if err != nil {
		t.Fatal(err)
	}
	domain, found, err := c.FileProviderDomainForTenant(t.Context(), created.Tenant)
	if err != nil || !found {
		t.Fatalf("domain = %+v, %t, %v", domain, found, err)
	}
	domain.PublicPath = filepath.Join(t.TempDir(), "Domain")
	domain.ActivationGeneration = "test-activation"
	domain.Registered = true
	if err := c.ConfirmFileProviderDomain(t.Context(), domain); err != nil {
		t.Fatal(err)
	}
	return c, created, domain
}

func newMaterializationIdentity(
	t *testing.T,
	provision TenantProvision,
	domain FileProviderDomain,
	backing string,
) FileProviderMaterializationIdentity {
	t.Helper()
	snapshot, err := NewMaterializationSnapshotID()
	if err != nil {
		t.Fatal(err)
	}
	return FileProviderMaterializationIdentity{
		Tenant: provision.Tenant, Domain: domain.DomainID, Generation: provision.Generation,
		Snapshot: snapshot, BackingStoreIdentity: []byte(backing),
	}
}

func beginAndStageMaterializationForTest(
	t *testing.T,
	c *Catalog,
	identity FileProviderMaterializationIdentity,
	ids ...ObjectID,
) {
	t.Helper()
	if _, err := c.BeginFileProviderMaterializationSnapshot(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	if err := c.StageFileProviderMaterializationPage(t.Context(), FileProviderMaterializationPage{
		Identity: identity, Sequence: 0, IDs: ids,
	}); err != nil {
		t.Fatal(err)
	}
}

func commitMaterializationForTest(
	t *testing.T,
	c *Catalog,
	identity FileProviderMaterializationIdentity,
	ids ...ObjectID,
) FileProviderMaterializationResult {
	t.Helper()
	beginAndStageMaterializationForTest(t, c, identity, ids...)
	result, err := c.CommitFileProviderMaterializationSnapshot(t.Context(), FileProviderMaterializationCommit{
		Identity: identity, PageCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func catalogDatabasePath(t *testing.T, c *Catalog) string {
	t.Helper()
	var path string
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT file FROM pragma_database_list WHERE name = 'main'`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	return path
}
