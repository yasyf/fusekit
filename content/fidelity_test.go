package content_test

// Spike-3 wire-fidelity regressions: a cc-notes-shaped consumer (per-open
// edit buffers committed ONLY on flush, nanosecond version-keyed mtimes,
// external commits visible on the next open) served over the REAL
// BridgeServer/BridgeClient hop must keep those semantics byte-exact.

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
)

var fidelitySockSeq atomic.Int64

// fidelitySock returns a unix socket path short enough for macOS's 104-char
// sun_path limit (t.TempDir blows past it).
func fidelitySock(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "fkfid")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, fmt.Sprintf("b%d.sock", fidelitySockSeq.Add(1)))
}

// serveFidelity runs a real BridgeServer over a unix socket and returns the
// real BridgeClient the holder's tree view uses.
func serveFidelity(t *testing.T, src content.Source) *content.BridgeClient {
	t.Helper()
	sock := fidelitySock(t)
	srv := &content.BridgeServer{Socket: sock, Source: src, Version: "fidelity-contentd"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("bridge Run returned before serving: %v", err)
		default:
		}
		if conn, err := net.Dial("unix", sock); err == nil {
			conn.Close()
			return content.NewBridgeClient(sock)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("bridge socket never came up")
	return nil
}

const fidelityDomain = "cc-notes"

// version is one committed snapshot: bytes, the second-granular updated wall
// time, and the chain-tip SHA.
type version struct {
	data       []byte
	updatedSec int64
	sha        string
}

// backingStore simulates cc-notes' git-object store: an append-only version
// chain per path whose every commit mints a fresh chain-tip SHA.
type backingStore struct {
	mu    sync.Mutex
	files map[string][]version
}

func newBackingStore() *backingStore { return &backingStore{files: map[string][]version{}} }

func (s *backingStore) commit(path string, data []byte, updatedSec int64) version {
	s.mu.Lock()
	defer s.mu.Unlock()
	chain := s.files[path]
	h := sha1.New()
	fmt.Fprintf(h, "%s|%d|", path, len(chain))
	h.Write(data)
	v := version{data: append([]byte(nil), data...), updatedSec: updatedSec, sha: hex.EncodeToString(h.Sum(nil))}
	s.files[path] = append(chain, v)
	return v
}

func (s *backingStore) tip(path string) (version, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	chain := s.files[path]
	if len(chain) == 0 {
		return version{}, false
	}
	return chain[len(chain)-1], true
}

type editBuf struct {
	path     string
	snapshot []byte
	dirty    []byte
	isDirty  bool
}

// contentd is the cc-notes-shaped renderer: content.HandleTree (per-open
// snapshot + edit buffer, commit on Flush with a Release backstop) over the
// backing store. A WritableTree-only source would commit per write, which is
// NOT cc-notes; the token path is the one under test.
type contentd struct {
	store *backingStore

	mu      sync.Mutex
	tokens  map[string]*editBuf
	nextTok int

	commitUpdatedSec int64
}

func newContentd(store *backingStore) *contentd {
	return &contentd{store: store, tokens: map[string]*editBuf{}}
}

var (
	_ content.Source     = (*contentd)(nil)
	_ content.Tree       = (*contentd)(nil)
	_ content.HandleTree = (*contentd)(nil)
)

// entryFor renders a tip with cc-notes' exact mtime scheme:
// updatedSec*1e9 + VersionNsec(chainTipSHA).
func entryFor(name string, v version) content.Entry {
	mtime := v.updatedSec*1_000_000_000 + fusekit.VersionNsec(v.sha)
	return content.Entry{
		Name:    name,
		Kind:    content.EntrySynth,
		Version: v.sha,
		Size:    int64(len(v.data)),
		Mtime:   mtime,
		Birth:   mtime,
	}
}

func (c *contentd) Manifest(string) ([]content.Entry, error) {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	var out []content.Entry
	for p, chain := range c.store.files {
		out = append(out, entryFor(p, chain[len(chain)-1]))
	}
	return out, nil
}

func (c *contentd) ReadSynth(_, name string) ([]byte, error) {
	v, ok := c.store.tip(name)
	if !ok {
		return nil, classErr{class: content.ClassNotFound, msg: "no such path " + name}
	}
	return v.data, nil
}

func (c *contentd) WriteThrough(_, name string, data []byte) error {
	c.store.commit(name, data, c.commitUpdatedSec)
	return nil
}

func (c *contentd) Classify(string) content.EntryKind { return content.EntrySynth }

func (c *contentd) Stat(_, name string) (content.Entry, error) {
	v, ok := c.store.tip(name)
	if !ok {
		return content.Entry{}, classErr{class: content.ClassNotFound, msg: "no such path " + name}
	}
	return entryFor(name, v), nil
}

func (c *contentd) List(domain, _ string) ([]content.Entry, error) { return c.Manifest(domain) }

func (c *contentd) ReadAt(_, name string, ofst int64, size int) ([]byte, error) {
	v, ok := c.store.tip(name)
	if !ok {
		return nil, classErr{class: content.ClassNotFound, msg: "no such path " + name}
	}
	return sliceAt(v.data, ofst, size), nil
}

func (c *contentd) Readlink(string, string) (string, error) {
	return "", classErr{class: content.ClassInvalid, msg: "not a symlink"}
}

func (c *contentd) OpenHandle(_, name string) (string, content.Entry, error) {
	v, ok := c.store.tip(name)
	if !ok {
		return "", content.Entry{}, classErr{class: content.ClassNotFound, msg: "no such path " + name}
	}
	c.mu.Lock()
	c.nextTok++
	tok := fmt.Sprintf("tok-%d", c.nextTok)
	c.tokens[tok] = &editBuf{path: name, snapshot: append([]byte(nil), v.data...)}
	c.mu.Unlock()
	return tok, entryFor(name, v), nil
}

func (c *contentd) buf(tok string) (*editBuf, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.tokens[tok]
	if !ok {
		return nil, classErr{class: content.ClassNotFound, msg: "unknown token " + tok}
	}
	return b, nil
}

func (c *contentd) ReadAtHandle(_, _, token string, ofst int64, size int) ([]byte, error) {
	b, err := c.buf(token)
	if err != nil {
		return nil, err
	}
	return sliceAt(b.snapshot, ofst, size), nil // open-time snapshot, immune to concurrent commits
}

func (c *contentd) WriteAtHandle(_, _, token string, ofst int64, data []byte) error {
	b, err := c.buf(token)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !b.isDirty {
		b.dirty = append([]byte(nil), b.snapshot...)
	}
	b.dirty = writeInto(b.dirty, ofst, data)
	b.isDirty = true
	return nil // NOT committed to the store — that is Flush's job
}

func (c *contentd) TruncateHandle(_, _, token string, size int64) error {
	b, err := c.buf(token)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !b.isDirty {
		b.dirty = append([]byte(nil), b.snapshot...)
	}
	b.dirty = resize(b.dirty, size)
	b.isDirty = true
	return nil
}

func (c *contentd) commitLocked(b *editBuf) {
	c.store.commit(b.path, b.dirty, c.commitUpdatedSec)
	b.snapshot = append([]byte(nil), b.dirty...)
	b.isDirty = false
}

func (c *contentd) FlushHandle(_, _, token string) error {
	b, err := c.buf(token)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if b.isDirty {
		c.commitLocked(b) // the commit boundary
	}
	return nil
}

func (c *contentd) ReleaseHandle(_, _, token string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.tokens[token]
	if !ok {
		return classErr{class: content.ClassNotFound, msg: "unknown token " + token}
	}
	if b.isDirty {
		c.commitLocked(b) // backstop commit
	}
	delete(c.tokens, token)
	return nil
}

func (c *contentd) ReleaseAllHandles(string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tokens = map[string]*editBuf{}
	return nil
}

type classErr struct {
	class string
	msg   string
}

func (e classErr) Error() string { return e.msg }
func (e classErr) Class() string { return e.class }

func sliceAt(data []byte, ofst int64, size int) []byte {
	if ofst < 0 || ofst >= int64(len(data)) {
		return nil
	}
	end := min(ofst+int64(size), int64(len(data)))
	return append([]byte(nil), data[ofst:end]...)
}

func writeInto(buf []byte, ofst int64, data []byte) []byte {
	if end := ofst + int64(len(data)); end > int64(len(buf)) {
		buf = resize(buf, end)
	}
	copy(buf[ofst:], data)
	return buf
}

func resize(buf []byte, size int64) []byte {
	if size <= int64(len(buf)) {
		return append([]byte(nil), buf[:size]...)
	}
	return append(append([]byte(nil), buf...), make([]byte, size-int64(len(buf)))...)
}

func mustRead(t *testing.T, h *content.Handle, ofst int64, size int) []byte {
	t.Helper()
	b, err := h.ReadAt(context.Background(), ofst, size)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	return b
}

// TestFidelityCommitOnFlush: writes buffer per open token handle and commit
// to the backing store ONLY on Flush — over the real wire.
func TestFidelityCommitOnFlush(t *testing.T) {
	store := newBackingStore()
	cd := newContentd(store)
	cd.commitUpdatedSec = 1_000_000
	store.commit("/notes/x.md", []byte("hello"), 1_000_000)
	client := serveFidelity(t, cd)
	ctx := context.Background()

	h, err := client.OpenHandle(ctx, fidelityDomain, "/notes/x.md")
	if err != nil {
		t.Fatalf("OpenHandle: %v", err)
	}
	if got := string(mustRead(t, h, 0, 64)); got != "hello" {
		t.Fatalf("snapshot read = %q, want hello", got)
	}
	if err := h.Truncate(ctx, 0); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := h.WriteAt(ctx, 0, []byte("world!!")); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if v, _ := store.tip("/notes/x.md"); string(v.data) != "hello" {
		t.Fatalf("store committed BEFORE flush: %q — commit-on-flush violated", v.data)
	}
	// A concurrent reader sees the committed bytes, not the dirty buffer.
	if got, _ := client.ReadAt(ctx, fidelityDomain, "/notes/x.md", 0, 64); string(got) != "hello" {
		t.Fatalf("uncommitted write leaked to path read: %q", got)
	}

	if err := h.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if v, _ := store.tip("/notes/x.md"); string(v.data) != "world!!" {
		t.Fatalf("store after flush = %q, want world!! — flush did not commit", v.data)
	}
	if got, _ := client.ReadAt(ctx, fidelityDomain, "/notes/x.md", 0, 64); string(got) != "world!!" {
		t.Fatalf("committed bytes not visible post-flush: %q", got)
	}
	_ = h.Release(ctx)
}

// TestFidelityReleaseBackstop: a dirty handle never Flushed still commits on
// Release, over the wire.
func TestFidelityReleaseBackstop(t *testing.T) {
	store := newBackingStore()
	cd := newContentd(store)
	cd.commitUpdatedSec = 1_000_000
	store.commit("/notes/y.md", []byte("aaaa"), 1_000_000)
	client := serveFidelity(t, cd)
	ctx := context.Background()

	h, err := client.OpenHandle(ctx, fidelityDomain, "/notes/y.md")
	if err != nil {
		t.Fatalf("OpenHandle: %v", err)
	}
	if err := h.WriteAt(ctx, 0, []byte("bbbb")); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if v, _ := store.tip("/notes/y.md"); string(v.data) != "aaaa" {
		t.Fatalf("store committed before release: %q", v.data)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if v, _ := store.tip("/notes/y.md"); string(v.data) != "bbbb" {
		t.Fatalf("release backstop did not commit: %q", v.data)
	}
}

// TestFidelityMtimeNsecSurvivesWire: the consumer's Entry.Mtime (Unix nanos =
// updatedSec*1e9 + VersionNsec(chainTipSHA)) crosses the JSON wire BYTE-EXACT,
// and a SAME-SECOND version change produces a DISTINCT wire mtime — the cache
// defeat cc-notes needs.
func TestFidelityMtimeNsecSurvivesWire(t *testing.T) {
	store := newBackingStore()
	cd := newContentd(store)
	const sameSec = int64(1_700_000_000)
	cd.commitUpdatedSec = sameSec
	v0 := store.commit("/notes/z.md", []byte("v0-body"), sameSec)
	client := serveFidelity(t, cd)
	ctx := context.Background()

	e0, err := client.Stat(ctx, fidelityDomain, "/notes/z.md")
	if err != nil {
		t.Fatalf("Stat v0: %v", err)
	}
	want0 := sameSec*1_000_000_000 + fusekit.VersionNsec(v0.sha)
	if e0.Mtime != want0 {
		t.Fatalf("wire mtime v0 = %d, want %d (nsec not byte-exact over wire)", e0.Mtime, want0)
	}
	if e0.Version != v0.sha {
		t.Fatalf("wire version = %q, want the chain tip %q", e0.Version, v0.sha)
	}

	v1 := store.commit("/notes/z.md", []byte("v1-body"), sameSec)
	e1, err := client.Stat(ctx, fidelityDomain, "/notes/z.md")
	if err != nil {
		t.Fatalf("Stat v1: %v", err)
	}
	if want1 := sameSec*1_000_000_000 + fusekit.VersionNsec(v1.sha); e1.Mtime != want1 {
		t.Fatalf("wire mtime v1 = %d, want %d", e1.Mtime, want1)
	}
	if e0.Mtime/1_000_000_000 != e1.Mtime/1_000_000_000 {
		t.Fatalf("expected same second; got %d vs %d", e0.Mtime/1e9, e1.Mtime/1e9)
	}
	if e0.Mtime == e1.Mtime {
		t.Fatalf("same-second version change produced IDENTICAL wire mtime %d — cache defeat impossible", e0.Mtime)
	}
	if e0.Version == e1.Version {
		t.Fatal("same-second version change produced an identical wire version")
	}
}

// TestFidelityFreshAfterExternalChange: an EXTERNAL store change is visible
// to the next open and to path reads, while an already-open handle keeps its
// open-time snapshot.
func TestFidelityFreshAfterExternalChange(t *testing.T) {
	store := newBackingStore()
	cd := newContentd(store)
	cd.commitUpdatedSec = 2_000_000
	store.commit("/tasks/t.json", []byte(`{"v":1}`), 2_000_000)
	client := serveFidelity(t, cd)
	ctx := context.Background()

	h1, err := client.OpenHandle(ctx, fidelityDomain, "/tasks/t.json")
	if err != nil {
		t.Fatalf("OpenHandle h1: %v", err)
	}
	if got := string(mustRead(t, h1, 0, 64)); got != `{"v":1}` {
		t.Fatalf("h1 snapshot = %q", got)
	}
	// External CLI commit.
	store.commit("/tasks/t.json", []byte(`{"v":2,"extra":true}`), 2_000_000)
	if got := string(mustRead(t, h1, 0, 64)); got != `{"v":1}` {
		t.Fatalf("h1 snapshot tore across external commit: %q", got)
	}
	h2, err := client.OpenHandle(ctx, fidelityDomain, "/tasks/t.json")
	if err != nil {
		t.Fatalf("OpenHandle h2: %v", err)
	}
	if got := string(mustRead(t, h2, 0, 64)); got != `{"v":2,"extra":true}` {
		t.Fatalf("fresh open did not see external change: %q", got)
	}
	if got, _ := client.ReadAt(ctx, fidelityDomain, "/tasks/t.json", 0, 64); string(got) != `{"v":2,"extra":true}` {
		t.Fatalf("path read stale after external change: %q", got)
	}
	_ = h1.Release(ctx)
	_ = h2.Release(ctx)
}
