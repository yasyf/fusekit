package catalog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenWithStorageLimitsConfiguresSQLiteCeilings(t *testing.T) {
	limits := testStorageLimits()
	c, err := OpenWithStorageLimits(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"), limits)
	if err != nil {
		t.Fatalf("OpenWithStorageLimits: %v", err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Errorf("close catalog: %v", err)
		}
	}()

	var pageSize, maxPages, checkpointPages, walBytes int64
	for query, destination := range map[string]*int64{
		"PRAGMA page_size":          &pageSize,
		"PRAGMA max_page_count":     &maxPages,
		"PRAGMA wal_autocheckpoint": &checkpointPages,
		"PRAGMA journal_size_limit": &walBytes,
	} {
		if err := c.db.QueryRowContext(t.Context(), query).Scan(destination); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	if maxPages != limits.DatabaseBytes/pageSize {
		t.Fatalf("max_page_count = %d, want %d", maxPages, limits.DatabaseBytes/pageSize)
	}
	if checkpointPages != limits.WALCheckpointBytes/pageSize {
		t.Fatalf("wal_autocheckpoint = %d, want %d", checkpointPages, limits.WALCheckpointBytes/pageSize)
	}
	if walBytes != limits.WALBytes {
		t.Fatalf("journal_size_limit = %d, want %d", walBytes, limits.WALBytes)
	}
}

func TestOpenRejectsExistingDatabaseBeyondConfiguredCeiling(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	var pageSize, pageCount int64
	if err := c.db.QueryRowContext(t.Context(), "PRAGMA page_size").Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	if err := c.db.QueryRowContext(t.Context(), "PRAGMA page_count").Scan(&pageCount); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if pageCount <= 1 {
		t.Fatalf("page_count = %d, need more than one page", pageCount)
	}
	limits := testStorageLimits()
	limits.DatabaseBytes = (pageCount - 1) * pageSize
	_, err = OpenWithStorageLimits(t.Context(), path, limits)
	var quota *StorageQuotaError
	if !errors.As(err, &quota) || quota.Resource != "database" ||
		quota.Used != pageCount*pageSize || quota.Limit != limits.DatabaseBytes {
		t.Fatalf("OpenWithStorageLimits = %v, want existing database quota error", err)
	}
}

func TestWorkerWALBudgetStopsAtFirstPinnedCheckpoint(t *testing.T) {
	limits := testStorageLimits()
	limits.WALCheckpointBytes = 4096
	limits.WALBytes = 64 << 10
	c, err := OpenWithStorageLimits(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"), limits)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Errorf("close catalog: %v", err)
		}
	}()
	reader, err := c.readDB.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Rollback(); err != nil {
			t.Errorf("rollback pinned WAL reader: %v", err)
		}
	}()
	var sequence uint64
	if err := reader.QueryRowContext(t.Context(), "SELECT last_command_id FROM broker_sequence WHERE singleton = 1").Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if err := c.EnforceWorkerWALBudget(t.Context()); err != nil {
		t.Fatalf("pre-write budget: %v", err)
	}
	if _, err := c.NextBrokerCommandID(t.Context()); err != nil {
		t.Fatalf("bounded write: %v", err)
	}
	if err := c.EnforceWorkerWALBudget(t.Context()); err == nil {
		t.Fatal("post-write budget succeeded despite pinned WAL reader")
	}
	pinnedSize, err := c.walSize()
	if err != nil {
		t.Fatal(err)
	}
	if pinnedSize <= 0 || pinnedSize > limits.WALBytes {
		t.Fatalf("pinned WAL size = %d, want 1..%d", pinnedSize, limits.WALBytes)
	}
	if err := c.EnforceWorkerWALBudget(t.Context()); err == nil {
		t.Fatal("next mutation admission succeeded despite pinned WAL")
	}
	unchangedSize, err := c.walSize()
	if err != nil {
		t.Fatal(err)
	}
	if unchangedSize != pinnedSize {
		t.Fatalf("rejected admission grew WAL from %d to %d", pinnedSize, unchangedSize)
	}
	if err := reader.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := c.EnforceWorkerWALBudget(t.Context()); err != nil {
		t.Fatalf("budget recovery after reader release: %v", err)
	}
	recoveredSize, err := c.walSize()
	if err != nil {
		t.Fatal(err)
	}
	if recoveredSize >= limits.WALCheckpointBytes {
		t.Fatalf("recovered WAL size = %d, want below %d", recoveredSize, limits.WALCheckpointBytes)
	}
}

func TestStageContentEnforcesObjectPublishedAndTotalQuotas(t *testing.T) {
	tests := []struct {
		name     string
		limits   StorageLimits
		first    string
		second   string
		resource string
	}{
		{
			name: "object", limits: contentTestLimits(4, 8, 16, 24),
			second: "12345", resource: "object content",
		},
		{
			name: "published", limits: contentTestLimits(8, 8, 6, 16),
			first: "1234", second: "567", resource: "published content",
		},
		{
			name: "total", limits: contentTestLimits(6, 6, 6, 6),
			first: "1234", second: "567", resource: "total content",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := OpenWithStorageLimits(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"), test.limits)
			if err != nil {
				t.Fatalf("OpenWithStorageLimits: %v", err)
			}
			defer func() {
				if err := c.Close(); err != nil {
					t.Errorf("close catalog: %v", err)
				}
			}()
			if test.first != "" {
				if _, err := c.StageContent(t.Context(), strings.NewReader(test.first)); err != nil {
					t.Fatalf("StageContent(first): %v", err)
				}
			}
			_, err = c.StageContent(t.Context(), strings.NewReader(test.second))
			var quota *StorageQuotaError
			if !errors.Is(err, ErrStorageQuota) || !errors.As(err, &quota) {
				t.Fatalf("StageContent = %v, want StorageQuotaError", err)
			}
			if quota.Resource != test.resource {
				t.Fatalf("quota resource = %q, want %q", quota.Resource, test.resource)
			}
			var temporary int64
			if err := c.db.QueryRowContext(t.Context(), `
SELECT temporary_bytes
FROM storage_accounting
WHERE singleton = 1`).Scan(&temporary); err != nil {
				t.Fatalf("read storage accounting: %v", err)
			}
			if temporary != 0 {
				t.Fatalf("temporary usage after rejection = %d, want 0", temporary)
			}
		})
	}
}

func TestStageContentAccountsConcurrentTemporaryBytes(t *testing.T) {
	limits := contentTestLimits(4, 4, 16, 16)
	c, err := OpenWithStorageLimits(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"), limits)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Errorf("close catalog: %v", err)
		}
	}()

	staged := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := c.StageContent(context.Background(), &prefixBlockingReader{
			prefix: []byte("123"), staged: staged, release: release,
		})
		result <- err
	}()
	<-staged
	_, err = c.StageContent(t.Context(), strings.NewReader("45"))
	var quota *StorageQuotaError
	if !errors.As(err, &quota) || quota.Resource != "temporary content" {
		t.Fatalf("concurrent StageContent = %v, want temporary content quota", err)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("first StageContent: %v", err)
	}
}

func TestStorageUsageRecoversAcrossReopen(t *testing.T) {
	limits := contentTestLimits(8, 8, 6, 16)
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := OpenWithStorageLimits(t.Context(), path, limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.StageContent(t.Context(), strings.NewReader("1234")); err != nil {
		t.Fatalf("StageContent(first): %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	c, err = OpenWithStorageLimits(t.Context(), path, limits)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Errorf("close reopened catalog: %v", err)
		}
	}()
	_, err = c.StageContent(t.Context(), strings.NewReader("567"))
	var quota *StorageQuotaError
	if !errors.As(err, &quota) || quota.Resource != "published content" || quota.Used != 4 {
		t.Fatalf("StageContent after reopen = %v, want published usage 4", err)
	}
}

func TestStageContentDeduplicationDoesNotDoubleChargePublishedQuota(t *testing.T) {
	limits := contentTestLimits(4, 4, 4, 8)
	c, err := OpenWithStorageLimits(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"), limits)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Errorf("close catalog: %v", err)
		}
	}()
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := c.StageContent(t.Context(), bytes.NewBufferString("same")); err != nil {
			t.Fatalf("StageContent(%d): %v", attempt, err)
		}
	}
	if c.storage.usage.published != 4 || c.storage.usage.temporary != 0 {
		t.Fatalf("usage = %+v, want published=4 temporary=0", c.storage.usage)
	}
}

func TestOneByteWritesUseBoundedAccountingWindows(t *testing.T) {
	c := newTestCatalog(t)
	var before uint64
	if err := c.db.QueryRowContext(t.Context(), `
SELECT version FROM storage_accounting WHERE singleton = 1`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	const bytes = 100_000
	ref, err := c.StageContent(t.Context(), &oneByteReader{remaining: bytes})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Size != bytes {
		t.Fatalf("staged size = %d, want %d", ref.Size, bytes)
	}
	var after uint64
	if err := c.db.QueryRowContext(t.Context(), `
SELECT version FROM storage_accounting WHERE singleton = 1`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if delta := after - before; delta != 3 {
		t.Fatalf("accounting version delta = %d, want 3 bounded settlements", delta)
	}
}

func TestOpenLoadsHundredThousandBlobLedgerWithoutDirectoryEnumeration(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := tx.PrepareContext(ctx, `
INSERT INTO storage_entries(name, kind, state, size, hash)
VALUES (?, ?, ?, 1, ?)`)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	const entries = 100_000
	for i := uint64(1); i <= entries; i++ {
		var hash ContentHash
		binary.BigEndian.PutUint64(hash[len(hash)-8:], i)
		if _, err := statement.ExecContext(
			ctx, blobName(hash), storageEntryPublished, storageEntryStable, hash[:],
		); err != nil {
			_ = statement.Close()
			_ = tx.Rollback()
			t.Fatalf("insert ledger entry %d: %v", i, err)
		}
	}
	if err := statement.Close(); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE storage_accounting
SET published_bytes = ?, version = version + 1
WHERE singleton = 1`, entries); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	blobDir := c.blobDir
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(blobDir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(blobDir, 0o700) })
	c, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("O(1) reopen with unreadable blob directory: %v", err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Errorf("close reopened catalog: %v", err)
		}
	}()
	if c.storage.usage.temporary != 0 || c.storage.usage.published != entries {
		t.Fatalf("loaded storage usage = %+v, want published=%d", c.storage.usage, entries)
	}
}

func TestStorageAccountingCASRejectsStaleVersion(t *testing.T) {
	c := newTestCatalog(t)
	result, err := c.db.ExecContext(t.Context(), `
UPDATE storage_accounting
SET temporary_bytes = temporary_bytes + 1, version = version + 1
WHERE singleton = 1 AND version = 99`)
	if err != nil {
		t.Fatal(err)
	}
	if err := requireOneRow(result, "stale storage accounting"); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("stale accounting CAS = %v, want integrity error", err)
	}
	assertStorageAccounting(t, c, 0, 0)
}

func testStorageLimits() StorageLimits {
	return StorageLimits{
		DatabaseBytes: 64 << 20, WALBytes: 1 << 20, WALCheckpointBytes: 64 << 10,
		ObjectContentBytes: 1 << 20, TemporaryContentBytes: 1 << 20,
		PublishedContentBytes: 1 << 20, TotalContentBytes: 2 << 20,
	}
}

func contentTestLimits(object, temporary, published, total int64) StorageLimits {
	limits := testStorageLimits()
	limits.ObjectContentBytes = object
	limits.TemporaryContentBytes = temporary
	limits.PublishedContentBytes = published
	limits.TotalContentBytes = total
	return limits
}

type prefixBlockingReader struct {
	prefix  []byte
	staged  chan struct{}
	release chan struct{}
	done    bool
}

type oneByteReader struct {
	remaining int
}

func (r *oneByteReader) Read(buffer []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	buffer[0] = byte(r.remaining)
	r.remaining--
	return 1, nil
}

func (r *prefixBlockingReader) Read(buffer []byte) (int, error) {
	if !r.done {
		r.done = true
		copied := copy(buffer, r.prefix)
		close(r.staged)
		return copied, nil
	}
	<-r.release
	return 0, io.EOF
}
