package catalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStageContentFailpointsPublishOnlyAtAtomicRename(t *testing.T) {
	points := []string{
		contentAfterWrite,
		contentAfterSync,
		contentBeforePublish,
		contentAfterPublish,
		contentAfterDirSync,
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "catalog.sqlite")
			boom := errors.New("stage crash")
			fired := false
			c, err := open(ctx, path, func(candidate string) error {
				if !fired && candidate == point {
					fired = true
					return boom
				}
				return nil
			})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			_, err = c.StageContent(ctx, &patternReader{remaining: 4096})
			if !errors.Is(err, boom) {
				t.Fatalf("StageContent err = %v, want crash", err)
			}
			digest := patternDigest(4096)
			_, statErr := os.Stat(c.blobPath(digest))
			published := point == contentAfterPublish || point == contentAfterDirSync
			if published && statErr != nil {
				t.Fatalf("published blob missing after %s: %v", point, statErr)
			}
			if !published && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("blob exists before rename at %s: %v", point, statErr)
			}
			if err := c.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			recovered, err := Open(ctx, path)
			if err != nil {
				t.Fatalf("Open(recovered): %v", err)
			}
			t.Cleanup(func() { _ = recovered.Close() })
			ref, err := recovered.StageContent(ctx, &patternReader{remaining: 4096})
			if err != nil {
				t.Fatalf("StageContent(retry): %v", err)
			}
			if ref.Hash != digest || ref.Size != 4096 {
				t.Fatalf("recovered ref = %+v", ref)
			}
		})
	}
}

func TestLargeContentStreamsThroughPinnedReaderAt(t *testing.T) {
	const size = int64(32 << 20)
	c := newTestCatalog(t)
	ref, err := c.StageContent(context.Background(), &patternReader{remaining: size})
	if err != nil {
		t.Fatalf("StageContent: %v", err)
	}
	if ref.Size != size || ref.Hash != patternDigest(size) {
		t.Fatalf("large ref = %+v", ref)
	}
	tenant, root := createTestTenant(t, c, "large", CaseSensitive)
	ensureTestGeneration(t, c, tenant, 1)
	object, err := c.Create(context.Background(), tenant, fileSpec(root.ID, "large.bin", ref, 1))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	handle, err := c.OpenAt(context.Background(), testRetentionOwner, tenant, PresentationFileProvider, 1, object.ID, object.Revision)
	if err != nil {
		t.Fatalf("Open handle: %v", err)
	}
	defer func() {
		if err := handle.Close(); err != nil {
			t.Errorf("Close handle: %v", err)
		}
	}()
	buffer := make([]byte, 4096)
	offset := size - int64(len(buffer))
	if _, err := handle.ReadAt(buffer, offset); err != nil {
		t.Fatalf("ReadAt tail: %v", err)
	}
	for i, got := range buffer {
		want := byte((offset + int64(i)) % 251)
		if got != want {
			t.Fatalf("tail[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestRestartAbandonsUnconsumedContentStage(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	createTestTenant(t, c, "restart-stage", CaseSensitive)
	ref, err := c.StageContent(ctx, &patternReader{remaining: 8192})
	if err != nil {
		t.Fatalf("StageContent: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(restart): %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	for {
		retirement, err := c.RetirePriorCatalogGenerations(ctx)
		if err != nil {
			t.Fatalf("RetirePriorCatalogGenerations: %v", err)
		}
		if !retirement.More {
			break
		}
	}
	compactTestContentUntilIdle(t, c)
	var stages int
	if err := c.db.QueryRow("SELECT COUNT(*) FROM content_stages WHERE stage_id = ?", ref.Stage[:]).Scan(&stages); err != nil {
		t.Fatalf("count abandoned stages: %v", err)
	}
	if stages != 0 {
		t.Fatalf("abandoned stage rows = %d, want 0", stages)
	}
	if _, err := os.Stat(c.blobPath(ref.Hash)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned staged blob stat = %v, want absent", err)
	}
}

func TestMaintenancePreservesStageFromUnretiredCatalogGeneration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(first): %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	tenant, root := createTestTenant(t, first, "live-prior-stage", CaseSensitive)
	ref := stageTestContent(t, first, "lost-response-content")

	second, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(second): %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if _, err := second.db.ExecContext(ctx,
		"INSERT INTO blob_gc_candidates(hash) VALUES (?)", ref.Hash[:]); err != nil {
		t.Fatalf("enqueue shared live-stage blob candidate: %v", err)
	}
	for call := 0; call < globalMaintenancePhaseCount; call++ {
		if _, err := second.MaintainGlobal(ctx, testMaintenanceNow()); err != nil {
			t.Fatalf("MaintainGlobal(%d): %v", call, err)
		}
	}
	var stages int
	if err := second.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM content_stages WHERE stage_id = ?", ref.Stage[:]).
		Scan(&stages); err != nil {
		t.Fatalf("count live prior stage: %v", err)
	}
	if stages != 1 {
		t.Fatalf("live prior stage rows = %d, want 1", stages)
	}
	if _, err := os.Stat(second.blobPath(ref.Hash)); err != nil {
		t.Fatalf("live prior stage blob removed by unrelated candidate: %v", err)
	}
	if _, err := first.Create(ctx, tenant, fileSpec(root.ID, "replayed", ref, 1)); err != nil {
		t.Fatalf("consume live prior stage after maintenance: %v", err)
	}
}

func TestCompactDefersBlobCollectionDuringContentStream(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "stage-race", CaseSensitive)
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan struct {
		ref ContentRef
		err error
	}, 1)
	go func() {
		ref, err := c.StageContent(ctx, &blockingReader{
			Reader:  strings.NewReader("streamed-content"),
			started: started,
			release: release,
		})
		result <- struct {
			ref ContentRef
			err error
		}{ref: ref, err: err}
	}()
	<-started

	compactTestContentUntilIdle(t, c)
	var tempName string
	if err := c.db.QueryRow(`
SELECT temp_name FROM content_stages WHERE owner_id = ? AND published = 0`, c.owner[:]).Scan(&tempName); err != nil {
		t.Fatalf("read pending stage: %v", err)
	}
	if _, err := os.Stat(filepath.Join(c.blobDir, tempName)); err != nil {
		t.Fatalf("pending stage file missing after Compact: %v", err)
	}
	close(release)
	staged := <-result
	if staged.err != nil {
		t.Fatalf("StageContent: %v", staged.err)
	}
	if _, err := c.Create(ctx, tenant, fileSpec(root.ID, "streamed", staged.ref, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestCompactProtectsPublishedUnconsumedContentStage(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "published-stage", CaseSensitive)
	ref := stageTestContent(t, c, "delayed-create")
	compactTestContentUntilIdle(t, c)
	if _, err := os.Stat(c.blobPath(ref.Hash)); err != nil {
		t.Fatalf("published stage missing after Compact: %v", err)
	}
	if _, err := c.Create(ctx, tenant, fileSpec(root.ID, "delayed", ref, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestReleaseUnclaimedContentOwnsExactStagesSharingOneBlob(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	createTestTenant(t, c, "release-shared-stage", CaseSensitive)
	first := stageTestContent(t, c, "shared-stage-content")
	second := stageTestContent(t, c, "shared-stage-content")
	if first.Stage == second.Stage || first.Hash != second.Hash {
		t.Fatalf("stage identity/hash = (%x, %x, %x, %x)", first.Stage, second.Stage, first.Hash, second.Hash)
	}
	if err := c.ReleaseUnclaimedContent(ctx, []ContentRef{first, first}); err != nil {
		t.Fatalf("ReleaseUnclaimedContent(first): %v", err)
	}
	if err := c.ReleaseUnclaimedContent(ctx, []ContentRef{first}); err != nil {
		t.Fatalf("ReleaseUnclaimedContent(first retry): %v", err)
	}
	if _, err := os.Stat(c.blobPath(first.Hash)); err != nil {
		t.Fatalf("shared blob removed while second stage owns it: %v", err)
	}
	if err := c.ReleaseUnclaimedContent(ctx, []ContentRef{second}); err != nil {
		t.Fatalf("ReleaseUnclaimedContent(second): %v", err)
	}
	compactTestContentUntilIdle(t, c)
	if _, err := os.Stat(c.blobPath(first.Hash)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released shared blob stat = %v, want absent", err)
	}
}

func TestReleaseUnclaimedContentRejectsOversizedSetBeforeMutation(t *testing.T) {
	c := newTestCatalog(t)
	ref := stageTestContent(t, c, "bounded-release")
	refs := make([]ContentRef, ReleaseUnclaimedContentLimit+1)
	for index := range refs {
		refs[index] = ref
	}
	if err := c.ReleaseUnclaimedContent(t.Context(), refs); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("ReleaseUnclaimedContent(over limit) = %v, want ErrInvalidObject", err)
	}
	if err := c.ReleaseUnclaimedContent(t.Context(), []ContentRef{ref}); err != nil {
		t.Fatalf("ReleaseUnclaimedContent after rejection: %v", err)
	}
}

type blockingReader struct {
	*strings.Reader
	started chan struct{}
	release chan struct{}
	once    bool
}

func compactTestContentUntilIdle(t *testing.T, c *Catalog) {
	t.Helper()
	for step := 0; step < 16; step++ {
		_, stageMore, err := c.compactContentStagePage(t.Context())
		if err != nil {
			t.Fatalf("compact content stage: %v", err)
		}
		_, blobMore, err := c.compactBlobCandidatePage(t.Context())
		if err != nil {
			t.Fatalf("compact content blob: %v", err)
		}
		if !stageMore && !blobMore {
			return
		}
	}
	t.Fatal("content compaction did not converge")
}

func (r *blockingReader) Read(buffer []byte) (int, error) {
	if !r.once {
		r.once = true
		close(r.started)
		<-r.release
	}
	return r.Reader.Read(buffer)
}

type patternReader struct {
	offset    int64
	remaining int64
}

func (r *patternReader) Read(buffer []byte) (int, error) {
	if len(buffer) > 64<<10 {
		return 0, fmt.Errorf("non-streaming read buffer: %d", len(buffer))
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	count := len(buffer)
	if int64(count) > r.remaining {
		count = int(r.remaining)
	}
	for i := 0; i < count; i++ {
		buffer[i] = byte((r.offset + int64(i)) % 251)
	}
	r.offset += int64(count)
	r.remaining -= int64(count)
	return count, nil
}

func patternDigest(size int64) ContentHash {
	digest := sha256.New()
	_, _ = io.Copy(digest, &patternReader{remaining: size})
	var hash ContentHash
	copy(hash[:], digest.Sum(nil))
	return hash
}
