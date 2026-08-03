package catalogservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
)

func TestContentHandleSurfacesRemoteCloseFailureAtEOF(t *testing.T) {
	content := &closeErrorReader{
		Reader: bytes.NewReader([]byte("content")),
		err:    catalog.ErrIntegrity,
	}
	handle := namespaceContentHandle(content)
	_, _, err := handle.page(t.Context(), 0, streamBufferSize)
	if !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("EOF page with failing close = %v, want integrity", err)
	}
	if content.closes != 1 {
		t.Fatalf("close count = %d, want 1", content.closes)
	}
}

func TestContentHandleClosesOnceAfterReadFailure(t *testing.T) {
	content := &closeErrorReader{
		Reader: errorReader{err: errors.New("read failed")},
	}
	handle := namespaceContentHandle(content)
	if _, _, err := handle.page(t.Context(), 0, 16); err == nil {
		t.Fatal("failing read paged successfully")
	}
	if content.closes != 1 {
		t.Fatalf("close count = %d, want 1", content.closes)
	}
	if _, _, err := handle.page(t.Context(), 0, 16); err == nil {
		t.Fatal("settled handle paged successfully")
	}
	if content.closes != 1 {
		t.Fatalf("settled handle re-closed content: %d", content.closes)
	}
}

func TestContentHandlePagesSequentiallyAndRefusesSkew(t *testing.T) {
	handle := namespaceContentHandle(io.NopCloser(bytes.NewReader([]byte("sequential-content"))))
	first, eof, err := handle.page(t.Context(), 0, 10)
	if err != nil || eof || string(first) != "sequential" {
		t.Fatalf("first page = %q, %t, %v", first, eof, err)
	}
	if _, _, err := handle.page(t.Context(), 3, 10); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("skewed offset = %v, want integrity", err)
	}
	rest, eof, err := handle.page(t.Context(), 10, 64)
	if err != nil || !eof || string(rest) != "-content" {
		t.Fatalf("final page = %q, %t, %v", rest, eof, err)
	}
	replay, eof, err := handle.page(t.Context(), 18, 64)
	if err != nil || !eof || len(replay) != 0 {
		t.Fatalf("post-EOF page = %q, %t, %v", replay, eof, err)
	}
}

func TestContentHandleCloseInterruptsBlockedRead(t *testing.T) {
	content := &blockingContent{started: make(chan struct{}), release: make(chan struct{})}
	handle := namespaceContentHandle(content)
	paged := make(chan error, 1)
	go func() {
		_, _, err := handle.page(t.Context(), 0, 16)
		paged <- err
	}()
	select {
	case <-content.started:
	case <-time.After(time.Second):
		t.Fatal("page did not reach the blocked reader")
	}
	handle.release(t.Context())
	select {
	case err := <-paged:
		if err == nil {
			t.Fatal("blocked page returned nil after release")
		}
	case <-time.After(time.Second):
		t.Fatal("release did not interrupt the blocked page")
	}
}

func TestPrivateContentHandleCloseBeforeEOFAbortsSource(t *testing.T) {
	source := &recordingSource{Reader: bytes.NewReader([]byte("private")), done: make(chan struct{})}
	handle := privateContentHandle(source)
	if _, _, err := handle.page(t.Context(), 0, 3); err != nil {
		t.Fatalf("partial page: %v", err)
	}
	handle.release(t.Context())
	if source.cause == nil {
		t.Fatal("early close settled the private source without a cause")
	}
	full := &recordingSource{Reader: bytes.NewReader([]byte("private")), done: make(chan struct{})}
	fullHandle := privateContentHandle(full)
	if _, eof, err := fullHandle.page(t.Context(), 0, 64); err != nil || !eof {
		t.Fatalf("full page = %t, %v", eof, err)
	}
	if !full.settled || full.cause != nil {
		t.Fatalf("EOF settle = %t, cause %v", full.settled, full.cause)
	}
}

func TestMutationUploadRefusesSequenceSkew(t *testing.T) {
	upload, err := newMutationUpload()
	if err != nil {
		t.Fatalf("newMutationUpload: %v", err)
	}
	t.Cleanup(func() { _ = upload.close() })
	if err := upload.write(2, []byte("early")); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("skipped sequence = %v, want integrity", err)
	}
	if err := upload.write(1, []byte("first")); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if err := upload.write(1, []byte("replay")); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("replayed sequence = %v, want integrity", err)
	}
	if err := upload.write(3, []byte("gap")); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("gapped sequence = %v, want integrity", err)
	}
}

func TestMutationUploadSealsExactTotalAndDigest(t *testing.T) {
	body := []byte("staged-mutation-body")
	digestBytes := sha256.Sum256(body)
	digest := hex.EncodeToString(digestBytes[:])

	seal := func(t *testing.T) *mutationUpload {
		t.Helper()
		upload, err := newMutationUpload()
		if err != nil {
			t.Fatalf("newMutationUpload: %v", err)
		}
		if err := upload.write(1, body[:7]); err != nil {
			t.Fatal(err)
		}
		if err := upload.write(2, body[7:]); err != nil {
			t.Fatal(err)
		}
		return upload
	}

	upload := seal(t)
	if _, err := upload.source(uint64(len(body)-1), digest); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("short total = %v, want integrity", err)
	}
	if _, err := upload.source(uint64(len(body)), strings.Repeat("e", 64)); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("wrong digest = %v, want integrity", err)
	}
	source, err := upload.source(uint64(len(body)), digest)
	if err != nil {
		t.Fatalf("exact seal: %v", err)
	}
	replayed, err := io.ReadAll(source)
	if err != nil || string(replayed) != string(body) {
		t.Fatalf("replayed body = %q, %v", replayed, err)
	}
	if err := source.Settle(nil); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if err := source.Wait(t.Context()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestApplicationErrorEnforcesExactRemoteMessageBound(t *testing.T) {
	exact := strings.Repeat("x", remoteErrorMessageBytes)
	_, message := applicationError(errors.New(exact))
	if message != exact {
		t.Fatalf("exact-bound message changed: got %d bytes", len(message))
	}
	_, message = applicationError(errors.New(exact + "y"))
	if len(message) != remoteErrorMessageBytes || !strings.HasSuffix(message, "...") {
		t.Fatalf("over-bound message = %d bytes, %q suffix", len(message), message[len(message)-3:])
	}
}

type closeErrorReader struct {
	io.Reader
	err    error
	closes int
}

func (r *closeErrorReader) Close() error {
	r.closes++
	return r.err
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

// recordingSource records how the handle settles it, standing in for a
// private open's contentstream.Source.
type recordingSource struct {
	io.Reader

	mu      sync.Mutex
	settled bool
	cause   error
	once    sync.Once
	done    chan struct{}
}

func (s *recordingSource) Settle(cause error) error {
	s.once.Do(func() {
		s.mu.Lock()
		s.settled = true
		s.cause = cause
		s.mu.Unlock()
		close(s.done)
	})
	return nil
}

func (s *recordingSource) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
