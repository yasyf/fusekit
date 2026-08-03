package sourcedriverservice

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/sourcedriver"
)

func testSettingsContentRef(t *testing.T, client *Client) sourcedriver.ContentRef {
	t.Helper()
	targetSet := declareServiceTestTargetSet(t, client)
	head, err := client.Refresh(t.Context(), testAuthority)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	page, err := client.Snapshot(t.Context(), testAuthority, sourcedriver.SnapshotRequest{
		TargetSet: targetSet, Revision: head.Revision, Limit: 16,
	})
	if err != nil || len(page.Objects) != 1 || page.Objects[0].Content == nil {
		t.Fatalf("Snapshot = %+v, %v", page, err)
	}
	return *page.Objects[0].Content
}

func openContentHandles(t *testing.T, server *Server) int {
	t.Helper()
	server.mu.Lock()
	defer server.mu.Unlock()
	open := 0
	for _, state := range server.sessions {
		state.mu.Lock()
		open += len(state.handles)
		state.mu.Unlock()
	}
	return open
}

func TestCancellingTheOpeningContextAbortsReadsAndReleasesTheHandle(t *testing.T) {
	client, _, server := startTestSourceDriverLane(t, newTestDriver())
	ref := testSettingsContentRef(t, client)
	opening, abandon := context.WithCancel(t.Context())
	opened, err := client.OpenContent(opening, testAuthority, ref)
	if err != nil {
		t.Fatalf("OpenContent: %v", err)
	}
	if open := openContentHandles(t, server); open != 1 {
		t.Fatalf("open handles after OpenContent = %d, want 1", open)
	}
	abandon()
	if _, err := io.ReadAll(opened); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadAll after the opening context was cancelled = %v, want context.Canceled", err)
	}
	if err := opened.Wait(t.Context()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want context.Canceled", err)
	}
	if open := openContentHandles(t, server); open != 0 {
		t.Fatalf("open handles after an aborted read = %d, want 0", open)
	}
}

func TestCancellingWaitReleasesThePinnedHandle(t *testing.T) {
	client, _, server := startTestSourceDriverLane(t, newTestDriver())
	ref := testSettingsContentRef(t, client)
	opened, err := client.OpenContent(t.Context(), testAuthority, ref)
	if err != nil {
		t.Fatalf("OpenContent: %v", err)
	}
	abandoned, abandon := context.WithCancel(t.Context())
	abandon()
	if err := opened.Wait(abandoned); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want context.Canceled", err)
	}
	if open := openContentHandles(t, server); open != 0 {
		t.Fatalf("open handles after a cancelled Wait = %d, want 0", open)
	}
}

func TestStagingRestartsWhenAnApplyRetriesFromBegin(t *testing.T) {
	state := newSessionState()
	t.Cleanup(func() {
		if err := state.release(); err != nil {
			t.Errorf("release: %v", err)
		}
	})
	body := []byte("retried-body")
	request := sourcedriver.MutationRequest{
		HasContent: true, ContentSize: int64(len(body)), ContentHash: catalog.ContentHash(sha256.Sum256(body)),
	}
	if err := state.begin("operation", request); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := state.chunk("operation", 1, []byte("stranded")); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if err := state.chunk("operation", 2, []byte("partial")); err != nil {
		t.Fatalf("second chunk: %v", err)
	}
	if err := state.begin("operation", request); err != nil {
		t.Fatalf("retry from begin: %v", err)
	}
	if err := state.chunk("operation", 1, body); err != nil {
		t.Fatalf("retried chunk: %v", err)
	}
	pending, err := state.takeApply("operation")
	if err != nil {
		t.Fatalf("takeApply: %v", err)
	}
	source, err := pending.upload.source(request.ContentSize, request.ContentHash)
	if err != nil {
		t.Fatalf("sealing the retried body: %v", err)
	}
	if err := source.Settle(nil); err != nil {
		t.Fatalf("settle staged source: %v", err)
	}
}
