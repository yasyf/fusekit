package catalog

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestHandleAndMutationPinReceiptsReplayUntilExplicitForget(t *testing.T) {
	ctx := t.Context()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "retention-replay", CaseSensitive)
	file := createTestFile(t, c, tenant, root.ID, "file", "payload")
	owner, err := NewRetentionOwner("native:retention-replay")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := c.OpenAt(
		ctx, owner, tenant, PresentationMount, 1, file.ID, file.Revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("handle close replay: %v", err)
	}
	assertRetentionRows(t, c, owner, 1, 0)
	if err := handle.Forget(ctx); err != nil {
		t.Fatal(err)
	}
	if err := handle.Forget(ctx); err != nil {
		t.Fatalf("handle forget replay: %v", err)
	}
	assertRetentionRows(t, c, owner, 0, 0)

	ref := stageTestContent(t, c, "next")
	prepared, err := c.BeginMutation(
		ctx, tenant, mustCatalogHead(t, c, tenant),
		testMountCreateIntent(root.ID, "next", ref),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.finishTestNamespaceMutation(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	pin, err := c.PinMutation(ctx, owner, tenant, prepared.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CloseMutationPin(ctx, pin); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseMutationPin(ctx, pin); err != nil {
		t.Fatalf("mutation pin close replay: %v", err)
	}
	assertRetentionRows(t, c, owner, 0, 1)
	if err := c.ForgetMutationPin(ctx, pin); err != nil {
		t.Fatal(err)
	}
	if err := c.ForgetMutationPin(ctx, pin); err != nil {
		t.Fatalf("mutation pin forget replay: %v", err)
	}
	assertRetentionRows(t, c, owner, 0, 0)

	retirement, err := c.RetireRetentionOwner(ctx, owner)
	if err != nil || retirement.More || retirement.Closed != 0 {
		t.Fatalf("RetireRetentionOwner = %+v, %v", retirement, err)
	}
	if _, err := c.OpenAt(
		ctx, owner, tenant, PresentationMount, 1, file.ID, file.Revision,
	); !errors.Is(err, ErrHandleClosed) {
		t.Fatalf("open after owner retirement = %v, want handle closed", err)
	}
	if err := handle.Forget(ctx); !errors.Is(err, ErrHandleClosed) {
		t.Fatalf("forget after owner retirement = %v, want handle closed", err)
	}
}

func TestRetentionOwnerRetirementAndCollectionArePageBounded(t *testing.T) {
	ctx := t.Context()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "retention-pages", CaseSensitive)
	file := createTestFile(t, c, tenant, root.ID, "file", "payload")
	owner, err := NewRetentionOwner("native:retention-pages")
	if err != nil {
		t.Fatal(err)
	}
	const total = RetainedIdentityPageLimit + 37
	for index := 0; index < total; index++ {
		handle, err := c.OpenAt(
			ctx, owner, tenant, PresentationMount, 1, file.ID, file.Revision,
		)
		if err != nil {
			t.Fatalf("OpenAt(%d): %v", index, err)
		}
		if err := handle.file.Close(); err != nil {
			t.Fatalf("simulate descriptor settlement(%d): %v", index, err)
		}
	}
	first, err := c.RetireRetentionOwner(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	if first.Closed != RetainedIdentityPageLimit || !first.More {
		t.Fatalf("first retirement = %+v", first)
	}
	second, err := c.RetireRetentionOwner(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	if second.Closed != total-RetainedIdentityPageLimit || second.More {
		t.Fatalf("second retirement = %+v", second)
	}
	var retired, live int
	if err := c.db.QueryRow(`
SELECT owner.retired, COUNT(handle.handle_id)
FROM retention_owners owner
LEFT JOIN handles handle
  ON handle.owner_id = owner.owner_id AND handle.session_owner = owner.session_owner
WHERE owner.owner_id = ? AND owner.session_owner = ?
GROUP BY owner.retired`, c.owner[:], string(owner)).Scan(&retired, &live); err != nil {
		t.Fatal(err)
	}
	if retired != 1 || live != total {
		t.Fatalf("retired owner state = retired %d handles %d", retired, live)
	}
	for {
		page, err := c.CollectRetainedIdentityGarbage(ctx, tenant, 0)
		if err != nil {
			t.Fatal(err)
		}
		retired := page.Handles + page.MutationPins + page.Interests +
			page.ObjectVersions + page.Objects + page.Owners
		if retired > RetainedIdentityPageLimit {
			t.Fatalf("collection page exceeded bound: %+v", page)
		}
		if !page.More {
			break
		}
	}
	assertRetentionRows(t, c, owner, 0, 0)
	var owners int
	if err := c.db.QueryRow(`
SELECT COUNT(*) FROM retention_owners
WHERE owner_id = ? AND session_owner = ?`, c.owner[:], string(owner)).Scan(&owners); err != nil {
		t.Fatal(err)
	}
	if owners != 0 {
		t.Fatalf("retired owner rows = %d, want 0", owners)
	}
}

func TestPriorGenerationRetirementWaitsForManifestHandoff(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	tenant, root := createTestTenant(t, first, "retention-handoff", CaseSensitive)
	ref := stageTestContent(t, first, "payload")
	prepared, err := first.BeginMutation(
		ctx, tenant, mustCatalogHead(t, first, tenant),
		testMountCreateIntent(root.ID, "file", ref),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.finishTestNamespaceMutation(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	oldOwner, err := NewRetentionOwner("native:old-worker-session")
	if err != nil {
		t.Fatal(err)
	}
	oldPin, err := first.PinMutation(ctx, oldOwner, tenant, prepared.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	file := createTestFile(t, first, tenant, root.ID, "retained", "payload")
	const retainedHandles = RetainedIdentityPageLimit + 1
	for index := 0; index < retainedHandles; index++ {
		handle, err := first.OpenAt(
			ctx, oldOwner, tenant, PresentationMount, 1, file.ID, file.Revision,
		)
		if err != nil {
			t.Fatalf("OpenAt(%d): %v", index, err)
		}
		if err := handle.file.Close(); err != nil {
			t.Fatalf("simulate descriptor loss(%d): %v", index, err)
		}
	}
	oldGeneration := first.owner
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	var oldClosed, oldRetired bool
	if err := second.db.QueryRow(`
SELECT pin.closed, owner.retired
FROM mutation_pins pin
JOIN retention_owners owner
  ON owner.owner_id = pin.owner_id AND owner.session_owner = pin.session_owner
WHERE pin.pin_id = ?`, oldPin.ID[:]).Scan(&oldClosed, &oldRetired); err != nil {
		t.Fatal(err)
	}
	if oldClosed || oldRetired {
		t.Fatal("prior pin retired before manifest handoff")
	}
	if retirement, err := second.RetirePriorRetentionOwners(ctx); err != nil ||
		retirement.Closed != 0 || retirement.More {
		t.Fatalf("retirement without generation proof = %+v, %v", retirement, err)
	}
	newOwner, err := NewRetentionOwner("native:new-worker-session")
	if err != nil {
		t.Fatal(err)
	}
	newPin, err := second.PinMutation(ctx, newOwner, tenant, prepared.OperationID)
	if err != nil {
		t.Fatalf("recover pin: %v", err)
	}
	for {
		retirement, err := second.RetirePriorCatalogGenerations(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !retirement.More {
			break
		}
	}
	for {
		retirement, err := second.RetirePriorRetentionOwners(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !retirement.More {
			break
		}
	}
	if err := second.db.QueryRow(`
SELECT pin.closed, owner.retired
FROM mutation_pins pin
JOIN retention_owners owner
  ON owner.owner_id = pin.owner_id AND owner.session_owner = pin.session_owner
WHERE pin.pin_id = ?`, oldPin.ID[:]).Scan(&oldClosed, &oldRetired); err != nil {
		t.Fatal(err)
	}
	if !oldClosed || !oldRetired {
		t.Fatal("prior pin remained live after manifest handoff")
	}
	var livePriorHandles int
	if err := second.db.QueryRow(`
SELECT COUNT(*) FROM handles
WHERE owner_id = ? AND session_owner = ? AND closed = 0`,
		oldGeneration[:], string(oldOwner)).Scan(&livePriorHandles); err != nil {
		t.Fatal(err)
	}
	if livePriorHandles != 0 {
		t.Fatalf("prior generation retained %d live handles after bounded retirement", livePriorHandles)
	}
	var current int
	if err := second.db.QueryRow(`
SELECT COUNT(*) FROM mutation_pins
WHERE pin_id = ? AND owner_id = ? AND session_owner = ? AND closed = 0`,
		newPin.ID[:], second.owner[:], string(newOwner)).Scan(&current); err != nil {
		t.Fatal(err)
	}
	if current != 1 || oldGeneration == second.owner {
		t.Fatalf("current recovered pin count=%d generation_reused=%t", current, oldGeneration == second.owner)
	}
}

func TestRetainedIdentityCollectionRollbackAndReplay(t *testing.T) {
	ctx := t.Context()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "retention-rollback", CaseSensitive)
	file := createTestFile(t, c, tenant, root.ID, "file", "payload")
	owner, err := NewRetentionOwner("native:retention-rollback")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := c.OpenAt(
		ctx, owner, tenant, PresentationMount, 1, file.ID, file.Revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RetireRetentionOwner(ctx, owner); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("retained identity rollback")
	c.failpoint = func(point string) error {
		if point == retainedIdentityAfterDelete {
			return boom
		}
		return nil
	}
	if _, err := c.CollectRetainedIdentityGarbage(ctx, tenant, 0); !errors.Is(err, boom) {
		t.Fatalf("CollectRetainedIdentityGarbage failpoint = %v, want boom", err)
	}
	assertRetentionRows(t, c, owner, 1, 0)
	c.failpoint = nil
	result, err := c.CollectRetainedIdentityGarbage(ctx, tenant, 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.Handles != 1 || result.Owners != 1 {
		t.Fatalf("replayed retained identity collection = %+v", result)
	}
	assertRetentionRows(t, c, owner, 0, 0)
}

func TestSnapshotHandleCloseRetriesAfterTransactionalFailure(t *testing.T) {
	ctx := t.Context()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "handle-close-retry", CaseSensitive)
	file := createTestFile(t, c, tenant, root.ID, "file", "payload")
	owner, err := NewRetentionOwner("native:handle-close-retry")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := c.OpenAt(
		ctx, owner, tenant, PresentationMount, 1, file.ID, file.Revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("handle close transaction")
	c.failpoint = func(point string) error {
		if point == handleAfterClose {
			return boom
		}
		return nil
	}
	if err := handle.Close(); !errors.Is(err, boom) {
		t.Fatalf("first close = %v, want failpoint", err)
	}
	var closed bool
	if err := c.db.QueryRowContext(ctx,
		"SELECT closed FROM handles WHERE handle_id = ?",
		handle.Handle.ID[:]).Scan(&closed); err != nil {
		t.Fatal(err)
	}
	if closed {
		t.Fatal("failed close transaction committed durable retirement")
	}
	c.failpoint = nil
	if err := handle.Close(); err != nil {
		t.Fatalf("retried close: %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("close replay: %v", err)
	}
}

func TestPriorCatalogGenerationRetirementIsBoundedAndCrashReplayable(t *testing.T) {
	ctx := t.Context()
	c := newTestCatalog(t)
	var currentRetired bool
	if err := c.db.QueryRowContext(ctx,
		"SELECT retired FROM catalog_generations WHERE owner_id = ?",
		c.owner[:]).Scan(&currentRetired); err != nil {
		t.Fatal(err)
	}
	if currentRetired {
		t.Fatal("current catalog generation registered retired")
	}
	const total = RetainedIdentityPageLimit + 17
	var blockedGeneration HandleOwnerID
	for index := 1; index <= total; index++ {
		var generation HandleOwnerID
		generation[0] = 0xff
		generation[13] = byte(index >> 16)
		generation[14] = byte(index >> 8)
		generation[15] = byte(index)
		if generation == c.owner {
			t.Fatal("fixture generation collided with current owner")
		}
		if _, err := c.db.ExecContext(ctx, `
INSERT INTO catalog_generations(owner_id, retired) VALUES (?, 0)`,
			generation[:]); err != nil {
			t.Fatal(err)
		}
		if index == total {
			blockedGeneration = generation
		}
	}
	var stage StageID
	stage[0] = 1
	if _, err := c.db.ExecContext(ctx, `
INSERT INTO content_stages(stage_id, owner_id, temp_name, published)
VALUES (?, ?, '.stage-retained-generation', 0)`,
		stage[:], blockedGeneration[:]); err != nil {
		t.Fatal(err)
	}
	var transition [16]byte
	transition[0] = 2
	if _, err := c.db.ExecContext(ctx, `
INSERT INTO storage_entries(name, kind, state, size, stage_id, owner_id)
VALUES ('.storage-retained-generation', 1, 1, 0, ?, ?)`,
		stage[:], blockedGeneration[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(ctx, `
INSERT INTO storage_transitions(
    transition_id, kind, owner_id, stage_id, source_name, size, new_blob,
    quarantined, reason
) VALUES (?, 1, ?, ?, '.storage-retained-generation', 0, 0, 1, 'retained fixture')`,
		transition[:], blockedGeneration[:], stage[:]); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("catalog generation retirement")
	c.failpoint = func(point string) error {
		if point == retentionGenerationAfterRetire {
			return boom
		}
		return nil
	}
	if _, err := c.RetirePriorCatalogGenerations(ctx); !errors.Is(err, boom) {
		t.Fatalf("failed generation retirement = %v, want failpoint", err)
	}
	var retired int
	if err := c.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM catalog_generations
WHERE owner_id <> ? AND retired = 1`, c.owner[:]).Scan(&retired); err != nil {
		t.Fatal(err)
	}
	if retired != 0 {
		t.Fatalf("failed generation retirement committed %d rows", retired)
	}
	c.failpoint = nil
	first, err := c.RetirePriorCatalogGenerations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Retired != RetainedIdentityPageLimit || !first.More {
		t.Fatalf("first generation retirement = %+v", first)
	}
	second, err := c.RetirePriorCatalogGenerations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second.Retired != total-RetainedIdentityPageLimit || second.More {
		t.Fatalf("second generation retirement = %+v", second)
	}
	if err := c.db.QueryRowContext(ctx, `
SELECT
  (SELECT COUNT(*) FROM catalog_generations WHERE owner_id <> ? AND retired = 1),
  (SELECT retired FROM catalog_generations WHERE owner_id = ?)`,
		c.owner[:], c.owner[:]).Scan(&retired, &currentRetired); err != nil {
		t.Fatal(err)
	}
	if retired != total || currentRetired {
		t.Fatalf("generation retirement prior=%d current=%t", retired, currentRetired)
	}
	collected, err := c.CollectRetiredCatalogGenerations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if collected.RetentionOwners != 0 ||
		collected.Generations != RetainedIdentityPageLimit || !collected.More {
		t.Fatalf("first generation collection = %+v", collected)
	}
	collected, err = c.CollectRetiredCatalogGenerations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if collected.RetentionOwners != 0 ||
		collected.Generations != total-RetainedIdentityPageLimit-1 || collected.More {
		t.Fatalf("blocked generation collection = %+v", collected)
	}
	var blocked int
	if err := c.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM catalog_generations
WHERE owner_id = ? AND retired = 1`, blockedGeneration[:]).Scan(&blocked); err != nil {
		t.Fatal(err)
	}
	if blocked != 1 {
		t.Fatalf("content-stage generation fence rows = %d, want 1", blocked)
	}
	if _, err := c.db.ExecContext(ctx,
		"DELETE FROM content_stages WHERE stage_id = ?", stage[:]); err != nil {
		t.Fatal(err)
	}
	collected, err = c.CollectRetiredCatalogGenerations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if collected.RetentionOwners != 0 || collected.Generations != 0 || collected.More {
		t.Fatalf("storage-blocked generation collection = %+v", collected)
	}
	if _, err := c.db.ExecContext(ctx,
		"DELETE FROM storage_transitions WHERE transition_id = ?", transition[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(ctx,
		"DELETE FROM storage_entries WHERE name = '.storage-retained-generation'"); err != nil {
		t.Fatal(err)
	}
	collected, err = c.CollectRetiredCatalogGenerations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if collected.RetentionOwners != 0 || collected.Generations != 1 || collected.More {
		t.Fatalf("drained generation collection = %+v", collected)
	}
}

func TestRemovedInterestReceiptSurvivesPinnedMutationReplay(t *testing.T) {
	ctx := t.Context()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "interest-replay", CaseSensitive)
	file := createTestFile(t, c, tenant, root.ID, "file", "payload")
	interest, err := c.AddInterest(
		ctx, tenant, mustCatalogHead(t, c, tenant), file.ID,
		fileProviderInterestOwner("interest-replay"), file.ContentRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := c.RemoveInterest(
		ctx, tenant, mustCatalogHead(t, c, tenant), interest.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	var rawMutation []byte
	if err := c.db.QueryRowContext(ctx, `
SELECT mutation_id FROM mutation_journal
WHERE tenant = ? AND kind = ? AND primary_object = ?`,
		string(tenant), uint8(MutationRemoveInterest), interest.ID[:]).
		Scan(&rawMutation); err != nil {
		t.Fatal(err)
	}
	if len(rawMutation) != len(MutationID{}) {
		t.Fatalf("remove-interest mutation id length = %d", len(rawMutation))
	}
	var mutation MutationID
	copy(mutation[:], rawMutation)
	owner, err := NewRetentionOwner("native:interest-replay")
	if err != nil {
		t.Fatal(err)
	}
	pin, err := c.PinMutation(ctx, owner, tenant, mutation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(ctx,
		"UPDATE tenants SET floor = ? WHERE tenant = ?",
		uint64(removed.RemovedRevision), string(tenant)); err != nil {
		t.Fatal(err)
	}
	result, err := c.CollectRetainedIdentityGarbage(
		ctx, tenant, removed.RemovedRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Interests != 0 {
		t.Fatalf("pinned remove-interest receipt collected: %+v", result)
	}
	if err := c.CloseMutationPin(ctx, pin); err != nil {
		t.Fatal(err)
	}
	if err := c.ForgetMutationPin(ctx, pin); err != nil {
		t.Fatal(err)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM prepared_mutations
WHERE mutation_id IN (
    SELECT mutation_id FROM mutation_journal
    WHERE tenant = ? AND (primary_object = ? OR secondary_object = ?)
)`, string(tenant), interest.ID[:], interest.ID[:]); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM mutation_journal
WHERE tenant = ? AND (primary_object = ? OR secondary_object = ?)`,
		string(tenant), interest.ID[:], interest.ID[:]); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	result, err = c.CollectRetainedIdentityGarbage(
		ctx, tenant, removed.RemovedRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Interests != 1 {
		t.Fatalf("settled remove-interest collection = %+v", result)
	}
}

func TestZeroTenantRestartCollectsEmptyRetiredOwnerAndGeneration(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewRetentionOwner("native:empty-session")
	if err != nil {
		t.Fatal(err)
	}
	if retirement, err := first.RetireRetentionOwner(ctx, owner); err != nil ||
		retirement.Closed != 0 || retirement.More {
		t.Fatalf("empty owner retirement = %+v, %v", retirement, err)
	}
	priorGeneration := first.owner
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	generations, err := second.RetirePriorCatalogGenerations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if generations.Retired != 1 || generations.More {
		t.Fatalf("empty prior generation retirement = %+v", generations)
	}
	collected, err := second.CollectRetiredCatalogGenerations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if collected.RetentionOwners != 1 || collected.Generations != 1 || collected.More {
		t.Fatalf("zero-tenant generation collection = %+v", collected)
	}
	var residue int
	if err := second.db.QueryRowContext(ctx, `
SELECT
  (SELECT COUNT(*) FROM retention_owners WHERE owner_id = ?),
  (SELECT COUNT(*) FROM catalog_generations WHERE owner_id = ?)`,
		priorGeneration[:], priorGeneration[:]).Scan(&residue, &generations.Retired); err != nil {
		t.Fatal(err)
	}
	if residue != 0 || generations.Retired != 0 {
		t.Fatalf("zero-tenant residue owners=%d generations=%d",
			residue, generations.Retired)
	}
}

func TestRetainedIdentityQueriesUseBoundedIndexes(t *testing.T) {
	c := newTestCatalog(t)
	tenant, _ := createTestTenant(t, c, "retention-plan", CaseSensitive)
	owner, err := NewRetentionOwner("native:retention-plan")
	if err != nil {
		t.Fatal(err)
	}
	plans := []struct {
		name      string
		statement string
		args      []any
		indexes   []string
	}{
		{
			name: "owner retirement",
			statement: `
SELECT kind, identity, tenant
FROM (
    SELECT 1 AS kind, handle_id AS identity, tenant
    FROM handles
    WHERE owner_id = ? AND session_owner = ? AND closed = 0
    UNION ALL
    SELECT 2 AS kind, pin_id AS identity, tenant
    FROM mutation_pins
    WHERE owner_id = ? AND session_owner = ? AND closed = 0
)
ORDER BY kind, identity
LIMIT ?`,
			args: []any{
				c.owner[:], string(owner), c.owner[:], string(owner),
				RetainedIdentityPageLimit + 1,
			},
			indexes: []string{"handles_owner_state", "mutation_pins_owner_state"},
		},
		{
			name: "closed handles",
			statement: `
SELECT retained.handle_id
FROM handles retained
JOIN retention_owners owner
  ON owner.owner_id = retained.owner_id
 AND owner.session_owner = retained.session_owner
WHERE retained.tenant = ? AND retained.closed = 1 AND owner.retired = 1
ORDER BY retained.handle_id
LIMIT ?`,
			args:    []any{string(tenant), RetainedIdentityPageLimit + 1},
			indexes: []string{"handles_owner_state"},
		},
		{
			name: "expired interests",
			statement: `
SELECT interest_id
FROM materialization_interests interest
WHERE tenant = ? AND removed_revision IS NOT NULL AND removed_revision <= ?
  AND NOT EXISTS (
      SELECT 1 FROM mutation_journal journal
      WHERE journal.tenant = interest.tenant
        AND (journal.primary_object = interest.interest_id
          OR journal.secondary_object = interest.interest_id)
  )
ORDER BY removed_revision, interest_id
LIMIT ?`,
			args: []any{
				string(tenant), uint64(100), RetainedIdentityPageLimit + 1,
			},
			indexes: []string{"materialization_interests_removed_gc"},
		},
		{
			name: "retired generations",
			statement: `
SELECT generation.owner_id
FROM catalog_generations generation
WHERE generation.retired = 1
  AND NOT EXISTS (
      SELECT 1 FROM retention_owners owner
      WHERE owner.owner_id = generation.owner_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM content_stages stage
      WHERE stage.owner_id = generation.owner_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM storage_entries entry INDEXED BY storage_entries_generation
      WHERE entry.owner_id = generation.owner_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM storage_transitions transition INDEXED BY storage_transitions_owner
      WHERE transition.owner_id = generation.owner_id
  )
ORDER BY generation.owner_id
LIMIT ?`,
			args: []any{RetainedIdentityPageLimit + 1},
			indexes: []string{
				"catalog_generations_retired", "content_stages_owner",
				"storage_entries_generation", "storage_transitions_owner",
			},
		},
		{
			name:      "tombstoned versions",
			statement: tombstoneVersionCandidates,
			args: []any{
				string(tenant), uint64(100), RetainedIdentityPageLimit + 1,
			},
			indexes: []string{"objects_tombstone_gc"},
		},
	}
	for _, test := range plans {
		t.Run(test.name, func(t *testing.T) {
			rows, err := c.readDB.QueryContext(
				t.Context(), "EXPLAIN QUERY PLAN "+test.statement, test.args...,
			)
			if err != nil {
				t.Fatal(err)
			}
			var details []string
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					_ = rows.Close()
					t.Fatal(err)
				}
				details = append(details, detail)
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			plan := strings.Join(details, "\n")
			for _, index := range test.indexes {
				if !strings.Contains(plan, index) {
					t.Fatalf("%s query plan missing %s:\n%s",
						test.name, index, fmt.Sprint(plan))
				}
			}
		})
	}
}

func assertRetentionRows(
	t *testing.T,
	c *Catalog,
	owner RetentionOwner,
	handles int,
	pins int,
) {
	t.Helper()
	var gotHandles, gotPins int
	if err := c.db.QueryRow(`
SELECT
  (SELECT COUNT(*) FROM handles WHERE owner_id = ? AND session_owner = ?),
  (SELECT COUNT(*) FROM mutation_pins WHERE owner_id = ? AND session_owner = ?)`,
		c.owner[:], string(owner), c.owner[:], string(owner)).Scan(&gotHandles, &gotPins); err != nil {
		t.Fatal(err)
	}
	if gotHandles != handles || gotPins != pins {
		t.Fatalf("retention rows handles=%d pins=%d, want %d/%d",
			gotHandles, gotPins, handles, pins)
	}
}
