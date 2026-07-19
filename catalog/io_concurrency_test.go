package catalog

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const concurrencyTestTimeout = 2 * time.Second

func TestBlockedStageReaderDoesNotHoldCatalogTransaction(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "blocked-stage-read", CaseSensitive)
	current := createTestFile(t, c, tenant, root.ID, "current", "old")
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan contentResult, 1)
	go func() {
		ref, err := c.StageContent(ctx, &blockingReader{
			Reader:  strings.NewReader("streamed"),
			started: started,
			release: release,
		})
		result <- contentResult{ref: ref, err: err}
	}()
	awaitSignal(t, started, "stage reader")
	assertCatalogResponsive(t, c, tenant, current, "reader-renamed")
	close(release)
	staged := awaitResult(t, result, "StageContent")
	if staged.err != nil {
		t.Fatalf("StageContent: %v", staged.err)
	}
	if _, err := c.Create(ctx, mustMutation(t), tenant, fileSpec(root.ID, "streamed", staged.ref, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestBlockedContentHashDoesNotHoldCatalogTransaction(t *testing.T) {
	ctx := context.Background()
	blocker := newPointBlocker(contentBeforeVerify)
	c := newFailpointCatalog(t, blocker.fail)
	tenant, root := createTestTenant(t, c, "blocked-hash", CaseSensitive)
	current := createTestFile(t, c, tenant, root.ID, "current", "old")
	ref := stageTestContent(t, c, "new")
	blocker.arm()
	result := make(chan objectResult, 1)
	go func() {
		obj, err := c.Create(ctx, mustMutation(t), tenant, fileSpec(root.ID, "new", ref, 1))
		result <- objectResult{object: obj, err: err}
	}()
	awaitSignal(t, blocker.started, "content hash")
	assertCatalogResponsive(t, c, tenant, current, "hash-renamed")
	blocker.unblock()
	created := awaitObject(t, result, "Create")
	if created.err != nil {
		t.Fatalf("Create: %v", created.err)
	}
}

func TestBlockedBlobDirectorySyncDoesNotHoldCatalogTransaction(t *testing.T) {
	ctx := context.Background()
	blocker := newPointBlocker(contentBeforeDirSync)
	c := newFailpointCatalog(t, blocker.fail)
	tenant, root := createTestTenant(t, c, "blocked-fsync", CaseSensitive)
	current := createTestFile(t, c, tenant, root.ID, "current", "old")
	blocker.arm()
	result := make(chan contentResult, 1)
	go func() {
		ref, err := c.StageContent(ctx, bytes.NewBufferString("new"))
		result <- contentResult{ref: ref, err: err}
	}()
	awaitSignal(t, blocker.started, "blob directory sync")
	assertCatalogResponsive(t, c, tenant, current, "fsync-renamed")
	blocker.unblock()
	staged := awaitResult(t, result, "StageContent")
	if staged.err != nil {
		t.Fatalf("StageContent: %v", staged.err)
	}
	if _, err := c.Create(ctx, mustMutation(t), tenant, fileSpec(root.ID, "new", staged.ref, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestSlowSnapshotDoesNotReserveWriter(t *testing.T) {
	ctx := context.Background()
	blocker := newPointBlocker(snapshotAfterAnchor)
	c := newFailpointCatalog(t, blocker.fail)
	tenant, root := createTestTenant(t, c, "slow-snapshot", CaseSensitive)
	createTestFile(t, c, tenant, root.ID, "existing", "old")
	ref := stageTestContent(t, c, "new")
	blocker.arm()
	snapshotResult := make(chan struct {
		page SnapshotPage
		err  error
	}, 1)
	go func() {
		page, err := c.Snapshot(ctx, tenant, EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}, 0, SnapshotCursor{}, 20)
		snapshotResult <- struct {
			page SnapshotPage
			err  error
		}{page: page, err: err}
	}()
	awaitSignal(t, blocker.started, "snapshot")
	writeCtx, cancel := context.WithTimeout(ctx, concurrencyTestTimeout)
	created, err := c.Create(writeCtx, mustMutation(t), tenant, fileSpec(root.ID, "concurrent", ref, 1))
	cancel()
	if err != nil {
		t.Fatalf("Create while snapshot blocked: %v", err)
	}
	blocker.unblock()
	result := awaitSnapshot(t, snapshotResult)
	if result.err != nil {
		t.Fatalf("Snapshot: %v", result.err)
	}
	if containsObject(result.page.Objects, created.ID) {
		t.Fatal("snapshot crossed its pinned revision")
	}
}

func TestGCRechecksStageInsertedAfterMark(t *testing.T) {
	ctx := context.Background()
	blocker := newPointBlocker(compactAfterMark)
	c := newFailpointCatalog(t, blocker.fail)
	tenant, root := createTestTenant(t, c, "gc-stage-race", CaseSensitive)
	blocker.arm()
	compactResult := make(chan error, 1)
	go func() { compactResult <- c.Compact(ctx, tenant, 1) }()
	awaitSignal(t, blocker.started, "blob GC mark")
	streamed := make(chan struct{})
	stageResult := make(chan contentResult, 1)
	go func() {
		ref, err := c.StageContent(ctx, &eofSignalReader{
			Reader: bytes.NewBufferString("racing-stage"),
			done:   streamed,
		})
		stageResult <- contentResult{ref: ref, err: err}
	}()
	awaitSignal(t, streamed, "stage stream completion")
	staged := awaitResult(t, stageResult, "StageContent while GC marked")
	if staged.err != nil {
		t.Fatalf("StageContent while GC marked: %v", staged.err)
	}
	blocker.unblock()
	if err := awaitError(t, compactResult, "Compact"); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if _, err := c.Create(ctx, mustMutation(t), tenant, fileSpec(root.ID, "racing", staged.ref, 1)); err != nil {
		t.Fatalf("Create after concurrent GC: %v", err)
	}
}

func TestOpenAtVerificationDoesNotHoldCatalogTransaction(t *testing.T) {
	ctx := context.Background()
	blocker := newPointBlocker(contentAfterOpen)
	c := newFailpointCatalog(t, blocker.fail)
	tenant, _ := createTestTenant(t, c, "blocked-open", CaseSensitive)
	root, err := c.Root(ctx, tenant)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	object := createTestFile(t, c, tenant, root.ID, "file", "content")
	blocker.arm()
	result := make(chan struct {
		handle *SnapshotHandle
		err    error
	}, 1)
	go func() {
		handle, err := c.OpenAt(ctx, tenant, PresentationFileProvider, 1, object.ID, object.Revision)
		result <- struct {
			handle *SnapshotHandle
			err    error
		}{handle: handle, err: err}
	}()
	awaitSignal(t, blocker.started, "open verification")
	assertCatalogResponsive(t, c, tenant, object, "file-renamed")
	stageResult := make(chan contentResult, 1)
	go func() {
		ref, err := c.StageContent(ctx, bytes.NewBufferString("unrelated-publish"))
		stageResult <- contentResult{ref: ref, err: err}
	}()
	staged := awaitResult(t, stageResult, "unrelated StageContent publication")
	if staged.err != nil {
		t.Fatalf("unrelated StageContent publication: %v", staged.err)
	}
	blocker.unblock()
	opened := awaitHandle(t, result)
	if opened.err != nil {
		t.Fatalf("OpenAt: %v", opened.err)
	}
	defer func() {
		if err := opened.handle.Close(); err != nil {
			t.Errorf("Close handle: %v", err)
		}
	}()
	content, err := io.ReadAll(opened.handle)
	if err != nil || string(content) != "content" {
		t.Fatalf("opened content = %q, %v", content, err)
	}
	if _, err := c.Create(ctx, mustMutation(t), tenant, fileSpec(root.ID, "published", staged.ref, 1)); err != nil {
		t.Fatalf("Create(unrelated publication): %v", err)
	}
}

type pointBlocker struct {
	point   string
	armed   atomic.Bool
	started chan struct{}
	release chan struct{}
}

func newPointBlocker(point string) *pointBlocker {
	return &pointBlocker{point: point, started: make(chan struct{}), release: make(chan struct{})}
}

func (b *pointBlocker) arm() { b.armed.Store(true) }

func (b *pointBlocker) unblock() { close(b.release) }

func (b *pointBlocker) fail(point string) error {
	if point == b.point && b.armed.CompareAndSwap(true, false) {
		close(b.started)
		<-b.release
	}
	return nil
}

type eofSignalReader struct {
	io.Reader
	done chan struct{}
	once sync.Once
}

func (r *eofSignalReader) Read(buffer []byte) (int, error) {
	count, err := r.Reader.Read(buffer)
	if err == io.EOF {
		r.once.Do(func() { close(r.done) })
	}
	return count, err
}

type contentResult struct {
	ref ContentRef
	err error
}

type objectResult struct {
	object Object
	err    error
}

func newFailpointCatalog(t *testing.T, fp failpoint) *Catalog {
	t.Helper()
	c, err := open(context.Background(), filepath.Join(t.TempDir(), "catalog.sqlite"), fp)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func assertCatalogResponsive(t *testing.T, c *Catalog, tenant TenantID, object Object, name string) Object {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), concurrencyTestTimeout)
	defer cancel()
	if _, err := c.Head(ctx, tenant); err != nil {
		t.Fatalf("Head while I/O blocked: %v", err)
	}
	if _, err := c.Lookup(ctx, tenant, PresentationFileProvider, object.ID); err != nil {
		t.Fatalf("Lookup while I/O blocked: %v", err)
	}
	revised, err := c.Revise(ctx, mustMutation(t), tenant, object.ID, RevisionSpec{
		Parent: object.Parent, Name: name, Mode: object.Mode, Convergence: object.Convergence, Visibility: object.Visibility,
	})
	if err != nil {
		t.Fatalf("metadata mutation while I/O blocked: %v", err)
	}
	if revised.Hash != object.Hash || revised.Size != object.Size || revised.ContentRevision != object.ContentRevision {
		t.Fatalf("metadata mutation changed content: before=%+v after=%+v", object, revised)
	}
	return revised
}

func awaitSignal(t *testing.T, signal <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(concurrencyTestTimeout):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func awaitResult(t *testing.T, result <-chan contentResult, operation string) contentResult {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(concurrencyTestTimeout):
		t.Fatalf("timed out waiting for %s", operation)
		return contentResult{}
	}
}

func awaitObject(t *testing.T, result <-chan objectResult, operation string) objectResult {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(concurrencyTestTimeout):
		t.Fatalf("timed out waiting for %s", operation)
		return objectResult{}
	}
}

func awaitError(t *testing.T, result <-chan error, operation string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(concurrencyTestTimeout):
		t.Fatalf("timed out waiting for %s", operation)
		return nil
	}
}

func awaitSnapshot(t *testing.T, result <-chan struct {
	page SnapshotPage
	err  error
}) struct {
	page SnapshotPage
	err  error
} {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(concurrencyTestTimeout):
		t.Fatal("timed out waiting for Snapshot")
		return struct {
			page SnapshotPage
			err  error
		}{}
	}
}

func awaitHandle(t *testing.T, result <-chan struct {
	handle *SnapshotHandle
	err    error
}) struct {
	handle *SnapshotHandle
	err    error
} {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(concurrencyTestTimeout):
		t.Fatal("timed out waiting for OpenAt")
		return struct {
			handle *SnapshotHandle
			err    error
		}{}
	}
}
