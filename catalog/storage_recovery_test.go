package catalog

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStorageTemporaryDeleteIntentReplaysExactlyAfterCrash(t *testing.T) {
	for _, point := range []string{
		storageDeleteAfterIntent,
		storageDeleteAfterUnlink,
		storageDeleteAfterSync,
	} {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "catalog.sqlite")
			first, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			owner := first.owner
			stage, name := writeDurableTemporaryStage(t, first, "pending")
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}

			recovered, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := recovered.Close(); err != nil {
					t.Errorf("close recovered catalog: %v", err)
				}
			}()
			retireAllPriorCatalogGenerations(t, recovered)
			boom := errors.New("storage delete crash")
			fired := false
			recovered.failpoint = func(candidate string) error {
				if !fired && candidate == point {
					fired = true
					return boom
				}
				return nil
			}
			err = recovered.deleteRetiredTemporaryStage(ctx, owner, stage, name)
			if !errors.Is(err, boom) {
				t.Fatalf("deleteRetiredTemporaryStage = %v, want crash", err)
			}
			assertStorageRowCount(t, recovered, "storage_transitions", 1)
			recovered.failpoint = nil
			if err := recovered.deleteRetiredTemporaryStage(ctx, owner, stage, name); err != nil {
				t.Fatalf("replay delete: %v", err)
			}
			assertStorageRowCount(t, recovered, "storage_transitions", 0)
			assertStorageRowCount(t, recovered, "storage_entries", 0)
			assertStorageRowCount(t, recovered, "content_stages", 0)
			assertStorageAccounting(t, recovered, 0, 0)
			if _, err := os.Stat(filepath.Join(recovered.blobDir, name)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("temporary file remains after replay: %v", err)
			}
		})
	}
}

func TestStoragePublishedDeleteIntentReplaysWithoutDoubleAccounting(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	ref, err := c.StageContent(ctx, strings.NewReader("published-delete"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(ctx,
		"DELETE FROM content_stages WHERE stage_id = ?", ref.Stage[:]); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("published delete crash")
	fired := false
	c.failpoint = func(candidate string) error {
		if !fired && candidate == storageDeleteAfterIntent {
			fired = true
			return boom
		}
		return nil
	}
	if _, err := c.deletePublishedBlob(ctx, ref.Hash); !errors.Is(err, boom) {
		t.Fatalf("deletePublishedBlob = %v, want crash", err)
	}
	assertStorageRowCount(t, c, "storage_transitions", 1)
	c.failpoint = nil
	if _, err := c.deletePublishedBlob(ctx, ref.Hash); err != nil {
		t.Fatalf("replay published delete: %v", err)
	}
	assertStorageRowCount(t, c, "storage_transitions", 0)
	assertStorageRowCount(t, c, "storage_entries", 0)
	assertStorageAccounting(t, c, 0, 0)
}

func TestStorageQuarantineRequiresExactInspectionToken(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	const body = "repairable-content"
	ref, err := c.StageContent(ctx, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(c.blobPath(ref.Hash)); err != nil {
		t.Fatal(err)
	}
	if err := c.verifyContentBlob(ctx, ref); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("verify missing blob = %v, want integrity quarantine", err)
	}
	page, err := c.InspectStorageQuarantine(ctx, StorageTransitionID{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.More {
		t.Fatalf("quarantine page = %+v, want exactly one entry", page)
	}
	entry := page.Entries[0]
	wrong := entry.Token
	wrong[0] ^= 0xff
	if _, err := c.ResolveStorageQuarantine(
		ctx, entry.ID, wrong, StorageQuarantineRetry,
	); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("resolve with stale token = %v, want invalid transition", err)
	}
	if err := os.WriteFile(c.blobPath(ref.Hash), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncStorageDirectory(c.blobDir); err != nil {
		t.Fatal(err)
	}
	receipt, err := c.ResolveStorageQuarantine(
		ctx, entry.ID, entry.Token, StorageQuarantineRetry,
	)
	if err != nil {
		t.Fatalf("verified retry: %v", err)
	}
	replayed, err := c.ResolveStorageQuarantine(
		ctx, entry.ID, entry.Token, StorageQuarantineRetry,
	)
	if err != nil || replayed != receipt {
		t.Fatalf("resolution replay = %+v, %v; want %+v", replayed, err, receipt)
	}
	if err := c.AcknowledgeStorageQuarantineResolution(ctx, receipt); err != nil {
		t.Fatalf("acknowledge resolution: %v", err)
	}
	if err := c.AcknowledgeStorageQuarantineResolution(ctx, receipt); err != nil {
		t.Fatalf("repeat resolution acknowledgement: %v", err)
	}
	page, err = c.InspectStorageQuarantine(ctx, StorageTransitionID{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 0 {
		t.Fatalf("quarantine remains after repair: %+v", page.Entries)
	}
	if err := c.verifyContentBlob(ctx, ref); err != nil {
		t.Fatalf("verify repaired content: %v", err)
	}
}

func TestStorageQuarantineResolutionLostResponseRecoversAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	const body = "lost-resolution-response"
	ref, err := c.StageContent(ctx, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(c.blobPath(ref.Hash)); err != nil {
		t.Fatal(err)
	}
	if err := c.verifyContentBlob(ctx, ref); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("quarantine missing blob: %v", err)
	}
	page, err := c.InspectStorageQuarantine(ctx, StorageTransitionID{}, 1)
	if err != nil || len(page.Entries) != 1 {
		t.Fatalf("InspectStorageQuarantine = %+v, %v", page, err)
	}
	if err := os.WriteFile(c.blobPath(ref.Hash), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("resolution response lost")
	c.failpoint = func(point string) error {
		if point == storageResolutionAfterClaim {
			return boom
		}
		return nil
	}
	if _, err := c.ResolveStorageQuarantine(
		ctx, page.Entries[0].ID, page.Entries[0].Token, StorageQuarantineRetry,
	); !errors.Is(err, boom) {
		t.Fatalf("ResolveStorageQuarantine = %v, want lost response", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Errorf("close recovered catalog: %v", err)
		}
	}()
	retireAllPriorCatalogGenerations(t, c)
	recovery, err := c.RecoverStorageTransitions(ctx, MaintenancePageLimit)
	if err != nil {
		t.Fatalf("RecoverStorageTransitions: %v", err)
	}
	if recovery.Recovered != 1 || recovery.Quarantined != 0 {
		t.Fatalf("recovery = %+v, want one recovered transition", recovery)
	}
	receipt, err := c.ResolveStorageQuarantine(
		ctx, page.Entries[0].ID, page.Entries[0].Token, StorageQuarantineRetry,
	)
	if err != nil {
		t.Fatalf("replay lost resolution response: %v", err)
	}
	if err := c.AcknowledgeStorageQuarantineResolution(ctx, receipt); err != nil {
		t.Fatalf("acknowledge replayed resolution: %v", err)
	}
	assertStorageRowCount(t, c, "storage_transitions", 0)
}

func TestStorageQuarantineResolutionAndMaintenanceSerialize(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	const body = "resolution-maintenance-race"
	ref, err := first.StageContent(ctx, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(first.blobPath(ref.Hash)); err != nil {
		t.Fatal(err)
	}
	if err := first.verifyContentBlob(ctx, ref); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("quarantine missing blob: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	c, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Errorf("close recovered catalog: %v", err)
		}
	}()
	retireAllPriorCatalogGenerations(t, c)
	page, err := c.InspectStorageQuarantine(ctx, StorageTransitionID{}, 1)
	if err != nil || len(page.Entries) != 1 {
		t.Fatalf("InspectStorageQuarantine = %+v, %v", page, err)
	}
	if err := os.WriteFile(c.blobPath(ref.Hash), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(ctx, `
UPDATE catalog_global_maintenance
SET next_phase = ?
WHERE singleton = 1`, globalMaintenanceStorageTransitions); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		_, err := c.ResolveStorageQuarantine(
			ctx, page.Entries[0].ID, page.Entries[0].Token, StorageQuarantineRetry,
		)
		errs <- err
	}()
	go func() {
		<-start
		_, err := c.MaintainGlobal(ctx, testMaintenanceNow())
		errs <- err
	}()
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent quarantine resolution: %v", err)
		}
	}
	assertStorageRowCount(t, c, "storage_transitions", 0)
}

func TestPersistedStorageEnumContract(t *testing.T) {
	if storageEntryTemporary != 1 || storageEntryPublished != 2 ||
		storageEntryPending != 1 || storageEntryStable != 2 ||
		storageTransitionCreateTemporary != 1 ||
		storageTransitionPublish != 2 ||
		storageTransitionDeleteTemporary != 3 ||
		storageTransitionDeletePublished != 4 ||
		StorageQuarantineRetry != 1 || StorageQuarantineDiscard != 2 ||
		storageQuarantineResolutionPending != 1 ||
		storageQuarantineResolutionSettled != 2 {
		t.Fatal("persisted storage enum values drifted from the hard schema")
	}
	c := newTestCatalog(t)
	var entryDDL, transitionDDL string
	if err := c.db.QueryRowContext(t.Context(), `
SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'storage_entries'`).
		Scan(&entryDDL); err != nil {
		t.Fatal(err)
	}
	if err := c.db.QueryRowContext(t.Context(), `
SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'storage_transitions'`).
		Scan(&transitionDDL); err != nil {
		t.Fatal(err)
	}
	for _, contract := range []struct {
		ddl  string
		want string
	}{
		{entryDDL, "kind IN (1, 2)"},
		{entryDDL, "state IN (1, 2)"},
		{transitionDDL, "kind BETWEEN 1 AND 4"},
	} {
		if !strings.Contains(contract.ddl, contract.want) {
			t.Fatalf("storage DDL lost %q contract:\n%s", contract.want, contract.ddl)
		}
	}
}

func TestStorageQuarantineInspectionHonorsByteCapBeforeEntryLimit(t *testing.T) {
	c := newTestCatalog(t)
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	const entries = MaintenancePageLimit
	reason := strings.Repeat("x", 4096)
	for index := 1; index <= entries; index++ {
		var id StorageTransitionID
		var hash ContentHash
		binary.BigEndian.PutUint64(id[len(id)-8:], uint64(index))
		binary.BigEndian.PutUint64(hash[len(hash)-8:], uint64(index))
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO storage_transitions(
    transition_id, kind, owner_id, source_name, hash, size,
    new_blob, quarantined, reason
) VALUES (?, ?, ?, ?, ?, 1, 0, 1, ?)`,
			id[:], storageTransitionDeletePublished, c.owner[:],
			blobName(hash), hash[:], reason); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert quarantine %d: %v", index, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var after StorageTransitionID
	seen := 0
	first := true
	for {
		page, err := c.InspectStorageQuarantine(
			t.Context(), after, MaintenancePageLimit,
		)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) > StorageQuarantinePageByteLimit {
			t.Fatalf("encoded page bytes = %d, limit %d", len(encoded), StorageQuarantinePageByteLimit)
		}
		if first {
			if len(page.Entries) >= MaintenancePageLimit || !page.More {
				t.Fatalf(
					"first max-reason page = %d more=%v, want byte truncation",
					len(page.Entries), page.More,
				)
			}
			first = false
		}
		if len(page.Entries) == 0 {
			t.Fatal("quarantine inspection made no cursor progress")
		}
		seen += len(page.Entries)
		after = page.Entries[len(page.Entries)-1].ID
		if !page.More {
			break
		}
	}
	if seen != entries {
		t.Fatalf("paged quarantines = %d, want %d", seen, entries)
	}
}

func TestStorageTransitionDDLRejectsMalformedAndImmutablePayloads(t *testing.T) {
	c := newTestCatalog(t)
	growthStage, err := NewStageID()
	if err != nil {
		t.Fatal(err)
	}
	growthName := fmt.Sprintf(".stage-%x", growthStage[:])
	growth, err := c.beginTemporaryContent(t.Context(), growthStage, growthName)
	if err != nil {
		t.Fatal(err)
	}
	if err := growth.reserve(1); err != nil {
		t.Fatalf("allowed create-temporary size growth: %v", err)
	}
	if err := c.abortTemporaryReservation(t.Context(), growth); err != nil {
		t.Fatalf("settle growth fixture: %v", err)
	}

	stage, err := NewStageID()
	if err != nil {
		t.Fatal(err)
	}
	hash := patternDigest(8)
	id, err := newStorageTransitionID()
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.db.ExecContext(t.Context(), `
INSERT INTO storage_transitions(
    transition_id, kind, owner_id, stage_id, source_name, hash, size, new_blob
) VALUES (?, ?, ?, ?, ?, ?, 8, 0)`,
		id[:], storageTransitionDeletePublished, c.owner[:], stage[:],
		blobName(hash), hash[:])
	if err == nil {
		t.Fatal("malformed published delete transition was accepted")
	}

	stage, name := writeDurableTemporaryStage(t, c, "window")
	var transition []byte
	if err := c.db.QueryRowContext(t.Context(), `
SELECT transition_id
FROM storage_transitions
WHERE stage_id = ?`, stage[:]).Scan(&transition); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("stable temporary stage unexpectedly retained create transition %x", transition)
	}
	deleteID, err := c.prepareTemporaryAbort(t.Context(), &temporaryReservation{
		catalog: c, stage: stage, name: name, active: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE storage_transitions SET source_name = ? WHERE transition_id = ?`,
		".stage-"+strings.Repeat("0", 32), deleteID[:]); err == nil {
		t.Fatal("storage transition payload rewrite was accepted")
	}
}

func TestStorageRecoveryPlanUsesRetiredGenerationAndOwnerIndexes(t *testing.T) {
	c := newTestCatalog(t)
	rows, err := c.db.QueryContext(t.Context(), `EXPLAIN QUERY PLAN
SELECT transition.transition_id
FROM catalog_generations generation INDEXED BY catalog_generations_retired
JOIN storage_transitions transition INDEXED BY storage_transitions_owner
  ON transition.owner_id = generation.owner_id
WHERE generation.retired = 1 AND transition.quarantined = 0
ORDER BY generation.owner_id, transition.transition_id
LIMIT ?`, MaintenancePageLimit+1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close storage recovery plan rows: %v", err)
		}
	}()
	var details string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details += "\n" + detail
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, index := range []string{
		"catalog_generations_retired",
		"storage_transitions_owner",
	} {
		if !strings.Contains(details, index) {
			t.Fatalf("query plan does not use %s:%s", index, details)
		}
	}
}

func writeDurableTemporaryStage(
	t *testing.T,
	c *Catalog,
	body string,
) (StageID, string) {
	t.Helper()
	stage, err := NewStageID()
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf(".stage-%x", stage[:])
	reservation, err := c.beginTemporaryContent(t.Context(), stage, name)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(
		filepath.Join(c.blobDir, name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reservation.writer(file).Write([]byte(body)); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := syncStorageDirectory(c.blobDir); err != nil {
		t.Fatal(err)
	}
	if err := reservation.finalizeTemporary(t.Context()); err != nil {
		t.Fatal(err)
	}
	return stage, name
}

func retireAllPriorCatalogGenerations(t *testing.T, c *Catalog) {
	t.Helper()
	for {
		result, err := c.RetirePriorCatalogGenerations(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if !result.More {
			return
		}
	}
}

func assertStorageRowCount(t *testing.T, c *Catalog, table string, want int) {
	t.Helper()
	var got int
	if err := c.db.QueryRowContext(
		t.Context(), "SELECT COUNT(*) FROM "+table,
	).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s rows = %d, want %d", table, got, want)
	}
}

func assertStorageAccounting(
	t *testing.T,
	c *Catalog,
	wantTemporary, wantPublished int64,
) {
	t.Helper()
	var temporary, published int64
	if err := c.db.QueryRowContext(t.Context(), `
SELECT temporary_bytes, published_bytes
FROM storage_accounting
WHERE singleton = 1`).Scan(&temporary, &published); err != nil {
		t.Fatal(err)
	}
	if temporary != wantTemporary || published != wantPublished {
		t.Fatalf(
			"storage accounting = (%d,%d), want (%d,%d)",
			temporary, published, wantTemporary, wantPublished,
		)
	}
}
