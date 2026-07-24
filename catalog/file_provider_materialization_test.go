package catalog

import (
	"encoding/binary"
	"errors"
	"path/filepath"
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
