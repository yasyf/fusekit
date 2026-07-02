package content

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func shortSockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ct")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// serveBridge runs a BridgeServer over src and returns its socket once it accepts.
func serveBridge(t *testing.T, src Source) string {
	t.Helper()
	socket := filepath.Join(shortSockDir(t), "b.sock")
	srv := &BridgeServer{Socket: socket, Source: src, Version: "v-test"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("BridgeServer.Run did not exit within 2s")
		}
	})
	cl := NewBridgeClient(socket)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cl.Available() {
			return socket
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("bridge socket never came up")
	return ""
}

// classErr is a ClassedError test double.
type classErr struct {
	msg, class string
}

func (e classErr) Error() string { return e.msg }
func (e classErr) Class() string { return e.class }

// fakeTree is an in-memory content.Tree. statErr forces Stat to fail with a
// classed error, exercising ErrClass propagation.
type fakeTree struct {
	statErr error
}

func (fakeTree) Manifest(string) ([]Entry, error)          { return nil, nil }
func (fakeTree) ReadSynth(string, string) ([]byte, error)  { return nil, nil }
func (fakeTree) WriteThrough(string, string, []byte) error { return nil }
func (fakeTree) Classify(string) EntryKind                 { return EntrySynth }
func (t fakeTree) Stat(domain, name string) (Entry, error) {
	if t.statErr != nil {
		return Entry{}, t.statErr
	}
	return Entry{Name: name, Kind: EntrySynth, Version: "v9", Size: 7}, nil
}
func (fakeTree) List(domain, name string) ([]Entry, error) {
	return []Entry{{Name: "a", Kind: EntrySynth}, {Name: "b", Kind: EntrySymlink}}, nil
}
func (fakeTree) ReadAt(domain, name string, ofst int64, size int) ([]byte, error) {
	data := []byte("0123456789")
	if ofst >= int64(len(data)) {
		return nil, nil
	}
	end := ofst + int64(size)
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return data[ofst:end], nil
}
func (fakeTree) Readlink(domain, name string) (string, error) { return "/abs/" + name, nil }

func TestBridgeTreeRoundTrip(t *testing.T) {
	cl := NewBridgeClient(serveBridge(t, fakeTree{}))
	ctx := context.Background()

	t.Run("stat", func(t *testing.T) {
		got, err := cl.Stat(ctx, "d", "x")
		if err != nil {
			t.Fatalf("Stat = %v", err)
		}
		want := Entry{Name: "x", Kind: EntrySynth, Version: "v9", Size: 7}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Stat = %+v, want %+v", got, want)
		}
	})
	t.Run("list", func(t *testing.T) {
		got, err := cl.List(ctx, "d", "/")
		if err != nil || len(got) != 2 || got[0].Name != "a" || got[1].Kind != EntrySymlink {
			t.Fatalf("List = %+v, %v", got, err)
		}
	})
	t.Run("readat", func(t *testing.T) {
		got, err := cl.ReadAt(ctx, "d", "x", 3, 4)
		if err != nil || string(got) != "3456" {
			t.Fatalf("ReadAt(3,4) = %q, %v; want \"3456\"", got, err)
		}
	})
	t.Run("readlink", func(t *testing.T) {
		got, err := cl.Readlink(ctx, "d", "lnk")
		if err != nil || got != "/abs/lnk" {
			t.Fatalf("Readlink = %q, %v", got, err)
		}
	})
}

// fakeSourceOnly implements Source but NOT Tree, so the server answers Tree ops
// with a ClassUnsupported capability miss.
type fakeSourceOnly struct{}

func (fakeSourceOnly) Manifest(string) ([]Entry, error)          { return nil, nil }
func (fakeSourceOnly) ReadSynth(string, string) ([]byte, error)  { return nil, nil }
func (fakeSourceOnly) WriteThrough(string, string, []byte) error { return nil }
func (fakeSourceOnly) Classify(string) EntryKind                 { return EntrySymlink }

func TestBridgeTreeOpsUnsupportedOnSourceOnly(t *testing.T) {
	cl := NewBridgeClient(serveBridge(t, fakeSourceOnly{}))
	_, err := cl.Stat(context.Background(), "d", "x")
	if err == nil {
		t.Fatal("Stat on a Source-only consumer = nil, want a capability miss")
	}
	if !IsUnsupported(err) {
		t.Fatalf("IsUnsupported(%v) = false, want true", err)
	}
	assertClass(t, err, ClassUnsupported)
}

func TestBridgeErrClassPropagates(t *testing.T) {
	cl := NewBridgeClient(serveBridge(t, fakeTree{statErr: classErr{msg: "bad name", class: ClassInvalid}}))
	_, err := cl.Stat(context.Background(), "d", "x")
	if err == nil {
		t.Fatal("Stat = nil, want a classed error")
	}
	assertClass(t, err, ClassInvalid)
}

// assertClass fails unless err carries the wanted wire error class.
func assertClass(t *testing.T, err error, want string) {
	t.Helper()
	var ce ClassedError
	if !errors.As(err, &ce) {
		t.Fatalf("err %v is not a ClassedError", err)
	}
	if ce.Class() != want {
		t.Fatalf("class = %q, want %q", ce.Class(), want)
	}
}

// --- writable-tree and handle-tree fakes ---

// Fixed attr values fakeWritableTree.Stat serves, pinning that the additive
// Entry fields round-trip the wire.
const (
	fakeMtime = int64(1_111)
	fakeBirth = int64(222)
	fakeIno   = uint64(42)
)

// fakeWritableTree is an in-memory WritableTree: a flat namespace of files and
// dirs behind one mutex (Source implementations must be concurrency-safe).
// Failures carry the wire class a real consumer would send.
type fakeWritableTree struct {
	mu    sync.Mutex
	files map[string][]byte
	dirs  map[string]bool
}

func newFakeWritableTree() *fakeWritableTree {
	return &fakeWritableTree{files: map[string][]byte{}, dirs: map[string]bool{"/": true}}
}

func (t *fakeWritableTree) Manifest(string) ([]Entry, error)          { return nil, nil }
func (t *fakeWritableTree) ReadSynth(string, string) ([]byte, error)  { return nil, nil }
func (t *fakeWritableTree) WriteThrough(string, string, []byte) error { return nil }
func (t *fakeWritableTree) Classify(string) EntryKind                 { return EntrySynth }

func (t *fakeWritableTree) Stat(_, name string) (Entry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dirs[name] {
		return Entry{Name: name, Kind: EntryDir, Mtime: fakeMtime, Birth: fakeBirth, Ino: fakeIno}, nil
	}
	if buf, ok := t.files[name]; ok {
		return Entry{Name: name, Kind: EntrySynth, Size: int64(len(buf)), Mtime: fakeMtime, Birth: fakeBirth, Ino: fakeIno}, nil
	}
	return Entry{}, classErr{msg: "no entry " + name, class: ClassNotFound}
}

func (t *fakeWritableTree) List(_, name string) ([]Entry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.dirs[name] {
		return nil, classErr{msg: "not a dir: " + name, class: ClassNotFound}
	}
	var out []Entry
	for p, buf := range t.files {
		if path.Dir(p) == name {
			out = append(out, Entry{Name: path.Base(p), Kind: EntrySynth, Size: int64(len(buf))})
		}
	}
	for p := range t.dirs {
		if p != "/" && path.Dir(p) == name {
			out = append(out, Entry{Name: path.Base(p), Kind: EntryDir})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (t *fakeWritableTree) ReadAt(_, name string, ofst int64, size int) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	buf, ok := t.files[name]
	if !ok {
		return nil, classErr{msg: "no entry " + name, class: ClassNotFound}
	}
	return sliceAt(buf, ofst, size), nil
}

func (t *fakeWritableTree) Readlink(_, name string) (string, error) {
	return "", classErr{msg: "not a symlink: " + name, class: ClassInvalid}
}

func (t *fakeWritableTree) Create(_, name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dirs[name] {
		return classErr{msg: name + " is a dir", class: ClassInvalid}
	}
	if _, ok := t.files[name]; !ok {
		t.files[name] = []byte{}
	}
	return nil
}

func (t *fakeWritableTree) WriteAt(_, name string, ofst int64, data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	buf, ok := t.files[name]
	if !ok {
		return classErr{msg: "no entry " + name, class: ClassNotFound}
	}
	t.files[name] = writeAt(buf, ofst, data)
	return nil
}

func (t *fakeWritableTree) Truncate(_, name string, size int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	buf, ok := t.files[name]
	if !ok {
		return classErr{msg: "no entry " + name, class: ClassNotFound}
	}
	t.files[name] = resizeBuf(buf, size)
	return nil
}

func (t *fakeWritableTree) Unlink(_, name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dirs[name] {
		return classErr{msg: name + " is a dir", class: ClassPerm}
	}
	if _, ok := t.files[name]; !ok {
		return classErr{msg: "no entry " + name, class: ClassNotFound}
	}
	delete(t.files, name)
	return nil
}

func (t *fakeWritableTree) Rename(_, oldName, newName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	buf, ok := t.files[oldName]
	if !ok {
		return classErr{msg: "no entry " + oldName, class: ClassNotFound}
	}
	delete(t.files, oldName)
	t.files[newName] = buf
	return nil
}

func (t *fakeWritableTree) Mkdir(_, name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dirs[name] {
		return classErr{msg: name + " exists", class: ClassInvalid}
	}
	t.dirs[name] = true
	return nil
}

func sliceAt(buf []byte, ofst int64, size int) []byte {
	if ofst >= int64(len(buf)) {
		return nil
	}
	end := ofst + int64(size)
	if end > int64(len(buf)) {
		end = int64(len(buf))
	}
	return append([]byte(nil), buf[ofst:end]...)
}

func writeAt(buf []byte, ofst int64, data []byte) []byte {
	if end := ofst + int64(len(data)); end > int64(len(buf)) {
		buf = resizeBuf(buf, end)
	}
	copy(buf[ofst:], data)
	return buf
}

func resizeBuf(buf []byte, size int64) []byte {
	if size <= int64(len(buf)) {
		return buf[:size]
	}
	return append(buf, make([]byte, size-int64(len(buf)))...)
}

// fakeHandle is one open handle: a snapshot buffer plus dirty/flushed state,
// mirroring cc-notes' per-fh edit buffer.
type fakeHandle struct {
	name    string
	buf     []byte
	dirty   bool
	flushed bool
}

// fakeHandleTree layers a realistic token registry over fakeWritableTree:
// OpenHandle snapshots the file per token, handle reads serve that snapshot,
// handle writes mutate the buffer, and Flush/Release commit it back — so the
// tests exercise the token contract (unknown → not-found, wrong name →
// invalid, release-all drops everything) end to end. flushErr injects a
// deterministic commit verdict; emptyToken exercises the server's
// token-contract check.
type fakeHandleTree struct {
	*fakeWritableTree
	flushErr   error
	emptyToken bool

	hmu     sync.Mutex
	nextTok int
	handles map[string]*fakeHandle
}

func newFakeHandleTree() *fakeHandleTree {
	return &fakeHandleTree{fakeWritableTree: newFakeWritableTree(), handles: map[string]*fakeHandle{}}
}

func (t *fakeHandleTree) OpenHandle(_, name string) (string, Entry, error) {
	if t.emptyToken {
		return "", Entry{}, nil
	}
	buf, err := t.ReadAt("", name, 0, 1<<20)
	if err != nil {
		return "", Entry{}, err
	}
	e := Entry{Name: name, Kind: EntrySynth, Size: int64(len(buf)), Mtime: fakeMtime, Birth: fakeBirth, Ino: fakeIno}
	t.hmu.Lock()
	defer t.hmu.Unlock()
	t.nextTok++
	tok := fmt.Sprintf("tok-%d", t.nextTok)
	t.handles[tok] = &fakeHandle{name: name, buf: buf}
	return tok, e, nil
}

// lookup resolves a token per the HandleTree contract. Caller holds hmu.
func (t *fakeHandleTree) lookup(name, token string) (*fakeHandle, error) {
	h, ok := t.handles[token]
	if !ok {
		return nil, classErr{msg: "unknown token " + token, class: ClassNotFound}
	}
	if h.name != name {
		return nil, classErr{msg: fmt.Sprintf("token %s is bound to %s, not %s", token, h.name, name), class: ClassInvalid}
	}
	return h, nil
}

func (t *fakeHandleTree) ReadAtHandle(_, name, token string, ofst int64, size int) ([]byte, error) {
	t.hmu.Lock()
	defer t.hmu.Unlock()
	h, err := t.lookup(name, token)
	if err != nil {
		return nil, err
	}
	return sliceAt(h.buf, ofst, size), nil
}

func (t *fakeHandleTree) WriteAtHandle(_, name, token string, ofst int64, data []byte) error {
	t.hmu.Lock()
	defer t.hmu.Unlock()
	h, err := t.lookup(name, token)
	if err != nil {
		return err
	}
	h.buf = writeAt(h.buf, ofst, data)
	h.dirty = true
	h.flushed = false
	return nil
}

func (t *fakeHandleTree) TruncateHandle(_, name, token string, size int64) error {
	t.hmu.Lock()
	defer t.hmu.Unlock()
	h, err := t.lookup(name, token)
	if err != nil {
		return err
	}
	h.buf = resizeBuf(h.buf, size)
	h.dirty = true
	h.flushed = false
	return nil
}

func (t *fakeHandleTree) FlushHandle(_, name, token string) error {
	t.hmu.Lock()
	defer t.hmu.Unlock()
	h, err := t.lookup(name, token)
	if err != nil {
		return err
	}
	return t.commit(h)
}

// commit persists the handle buffer, honoring the injected verdict. Caller
// holds hmu.
func (t *fakeHandleTree) commit(h *fakeHandle) error {
	if t.flushErr != nil {
		return t.flushErr
	}
	t.mu.Lock()
	t.files[h.name] = append([]byte(nil), h.buf...)
	t.mu.Unlock()
	h.dirty = false
	h.flushed = true
	return nil
}

func (t *fakeHandleTree) ReleaseHandle(_, name, token string) error {
	t.hmu.Lock()
	defer t.hmu.Unlock()
	h, err := t.lookup(name, token)
	if err != nil {
		return err
	}
	delete(t.handles, token)
	if h.dirty && !h.flushed {
		return t.commit(h)
	}
	return nil
}

func (t *fakeHandleTree) ReleaseAllHandles(string) error {
	t.hmu.Lock()
	defer t.hmu.Unlock()
	clear(t.handles)
	return nil
}

// --- write-op tests ---

func TestBridgeWritableTreeRoundTrip(t *testing.T) {
	ft := newFakeWritableTree()
	cl := NewBridgeClient(serveBridge(t, ft))
	ctx := context.Background()

	t.Run("create then stat round-trips the additive attrs", func(t *testing.T) {
		if err := cl.Create(ctx, "d", "/a.md"); err != nil {
			t.Fatalf("Create = %v", err)
		}
		got, err := cl.Stat(ctx, "d", "/a.md")
		if err != nil {
			t.Fatalf("Stat = %v", err)
		}
		want := Entry{Name: "/a.md", Kind: EntrySynth, Size: 0, Mtime: fakeMtime, Birth: fakeBirth, Ino: fakeIno}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Stat = %+v, want %+v", got, want)
		}
	})
	t.Run("writeat then readat", func(t *testing.T) {
		if err := cl.WriteAt(ctx, "d", "/a.md", 0, []byte("hello world")); err != nil {
			t.Fatalf("WriteAt = %v", err)
		}
		if err := cl.WriteAt(ctx, "d", "/a.md", 6, []byte("earth")); err != nil {
			t.Fatalf("WriteAt(6) = %v", err)
		}
		got, err := cl.ReadAt(ctx, "d", "/a.md", 0, 64)
		if err != nil || string(got) != "hello earth" {
			t.Fatalf("ReadAt = %q, %v; want \"hello earth\"", got, err)
		}
	})
	t.Run("writeat past the end zero-fills", func(t *testing.T) {
		if err := cl.Create(ctx, "d", "/sparse"); err != nil {
			t.Fatalf("Create = %v", err)
		}
		if err := cl.WriteAt(ctx, "d", "/sparse", 3, []byte("x")); err != nil {
			t.Fatalf("WriteAt = %v", err)
		}
		got, err := cl.ReadAt(ctx, "d", "/sparse", 0, 64)
		if err != nil || string(got) != "\x00\x00\x00x" {
			t.Fatalf("ReadAt = %q, %v; want \"\\x00\\x00\\x00x\"", got, err)
		}
	})
	t.Run("truncate shrinks and grows", func(t *testing.T) {
		if err := cl.Truncate(ctx, "d", "/a.md", 5); err != nil {
			t.Fatalf("Truncate(5) = %v", err)
		}
		if e, err := cl.Stat(ctx, "d", "/a.md"); err != nil || e.Size != 5 {
			t.Fatalf("Stat after shrink = %+v, %v; want size 5", e, err)
		}
		if err := cl.Truncate(ctx, "d", "/a.md", 7); err != nil {
			t.Fatalf("Truncate(7) = %v", err)
		}
		got, err := cl.ReadAt(ctx, "d", "/a.md", 0, 64)
		if err != nil || string(got) != "hello\x00\x00" {
			t.Fatalf("ReadAt after grow = %q, %v", got, err)
		}
	})
	t.Run("mkdir then list", func(t *testing.T) {
		if err := cl.Mkdir(ctx, "d", "/notes"); err != nil {
			t.Fatalf("Mkdir = %v", err)
		}
		got, err := cl.List(ctx, "d", "/")
		if err != nil {
			t.Fatalf("List = %v", err)
		}
		want := []Entry{
			{Name: "a.md", Kind: EntrySynth, Size: 7},
			{Name: "notes", Kind: EntryDir},
			{Name: "sparse", Kind: EntrySynth, Size: 4},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("List = %+v, want %+v", got, want)
		}
	})
	t.Run("rename moves the bytes", func(t *testing.T) {
		if err := cl.Rename(ctx, "d", "/a.md", "/b.md"); err != nil {
			t.Fatalf("Rename = %v", err)
		}
		_, err := cl.Stat(ctx, "d", "/a.md")
		assertClass(t, err, ClassNotFound)
		got, err := cl.ReadAt(ctx, "d", "/b.md", 0, 64)
		if err != nil || string(got) != "hello\x00\x00" {
			t.Fatalf("ReadAt(/b.md) = %q, %v", got, err)
		}
	})
	t.Run("unlink removes and is not idempotent", func(t *testing.T) {
		if err := cl.Unlink(ctx, "d", "/b.md"); err != nil {
			t.Fatalf("Unlink = %v", err)
		}
		_, err := cl.Stat(ctx, "d", "/b.md")
		assertClass(t, err, ClassNotFound)
		assertClass(t, cl.Unlink(ctx, "d", "/b.md"), ClassNotFound)
	})
	t.Run("writes on a missing name carry the consumer class", func(t *testing.T) {
		assertClass(t, cl.WriteAt(ctx, "d", "/nope", 0, []byte("x")), ClassNotFound)
		assertClass(t, cl.Truncate(ctx, "d", "/nope", 0), ClassNotFound)
		assertClass(t, cl.Rename(ctx, "d", "/nope", "/np2"), ClassNotFound)
	})
}

func TestBridgeWriteOpsUnsupportedOnReadOnlyTree(t *testing.T) {
	cl := NewBridgeClient(serveBridge(t, fakeTree{}))
	ctx := context.Background()
	cases := []struct {
		name string
		call func() error
	}{
		{"create", func() error { return cl.Create(ctx, "d", "/x") }},
		{"writeat", func() error { return cl.WriteAt(ctx, "d", "/x", 0, []byte("b")) }},
		{"truncate", func() error { return cl.Truncate(ctx, "d", "/x", 0) }},
		{"unlink", func() error { return cl.Unlink(ctx, "d", "/x") }},
		{"rename", func() error { return cl.Rename(ctx, "d", "/x", "/y") }},
		{"mkdir", func() error { return cl.Mkdir(ctx, "d", "/x") }},
		{"open", func() error { _, err := cl.OpenHandle(ctx, "d", "/x"); return err }},
		{"releaseall", func() error { return cl.ReleaseAllHandles(ctx, "d") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatal("call = nil, want a capability miss")
			}
			if !IsUnsupported(err) {
				t.Fatalf("IsUnsupported(%v) = false, want true", err)
			}
			assertClass(t, err, ClassUnsupported)
		})
	}
	// The misses must be verdicts, not casualties: the server still answers.
	if _, err := cl.Stat(ctx, "d", "x"); err != nil {
		t.Fatalf("Stat after write-op misses = %v, want the server still serving", err)
	}
}

func TestBridgeTokenOpsUnsupportedWithoutHandleTree(t *testing.T) {
	// A writable but handle-less consumer must never silently serve a
	// token-scoped op path-wise; the miss tells the holder its token is dead.
	cl := NewBridgeClient(serveBridge(t, newFakeWritableTree()))
	h := &Handle{c: cl, Domain: "d", Name: "/x", Token: "stale"}
	ctx := context.Background()
	cases := []struct {
		name string
		call func() error
	}{
		{"readat", func() error { _, err := h.ReadAt(ctx, 0, 8); return err }},
		{"writeat", func() error { return h.WriteAt(ctx, 0, []byte("b")) }},
		{"truncate", func() error { return h.Truncate(ctx, 0) }},
		{"flush", func() error { return h.Flush(ctx) }},
		{"release", func() error { return h.Release(ctx) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatal("call = nil, want a capability miss")
			}
			if !IsUnsupported(err) {
				t.Fatalf("IsUnsupported(%v) = false, want true", err)
			}
			assertClass(t, err, ClassUnsupported)
		})
	}
}

func TestBridgeUnknownOpReadsAsUnsupported(t *testing.T) {
	// The reply an old (pre-write-ops) server gives for any newly minted op:
	// class-less unknown-op. IsUnsupported must read it as a capability miss,
	// and it must never carry a class (version skew is not a verdict).
	cl := NewBridgeClient(serveBridge(t, fakeTree{}))
	resp, err := cl.do(context.Background(), BridgeRequest{Op: "op-from-the-future"})
	if err != nil {
		t.Fatalf("do = %v", err)
	}
	rerr := bridgeRespErr(resp)
	if rerr == nil {
		t.Fatal("unknown op = OK, want an error")
	}
	var ce ClassedError
	if !errors.As(rerr, &ce) || ce.Class() != "" {
		t.Fatalf("unknown op = %v, want a class-less reply", rerr)
	}
	if !IsUnsupported(rerr) {
		t.Fatalf("IsUnsupported(%v) = false, want true", rerr)
	}
}

// --- handle-token tests ---

func TestBridgeHandleLifecycle(t *testing.T) {
	ft := newFakeHandleTree()
	ft.files["/notes/a.md"] = []byte("v1 render")
	ft.dirs["/notes"] = true
	cl := NewBridgeClient(serveBridge(t, ft))
	ctx := context.Background()

	h, err := cl.OpenHandle(ctx, "d", "/notes/a.md")
	if err != nil {
		t.Fatalf("OpenHandle = %v", err)
	}
	if h.Token == "" {
		t.Fatal("OpenHandle returned an empty token")
	}
	// The open reply carries the snapshot's entry: Size is the exact length of
	// the bytes ReadAt serves for this token (the holder sizes the open by it),
	// and the attr fields round-trip like a Stat's.
	wantSnap := Entry{Name: "/notes/a.md", Kind: EntrySynth, Size: 9, Mtime: fakeMtime, Birth: fakeBirth, Ino: fakeIno}
	if !reflect.DeepEqual(h.Snapshot, wantSnap) {
		t.Fatalf("Snapshot = %+v, want %+v", h.Snapshot, wantSnap)
	}

	t.Run("snapshot is immune to a concurrent commit", func(t *testing.T) {
		if err := cl.WriteAt(ctx, "d", "/notes/a.md", 0, []byte("external!")); err != nil {
			t.Fatalf("path WriteAt = %v", err)
		}
		got, err := h.ReadAt(ctx, 0, 64)
		if err != nil || string(got) != "v1 render" {
			t.Fatalf("handle ReadAt = %q, %v; want the open-time snapshot", got, err)
		}
	})
	t.Run("buffered edit commits on flush", func(t *testing.T) {
		if err := h.WriteAt(ctx, 0, []byte("edited")); err != nil {
			t.Fatalf("handle WriteAt = %v", err)
		}
		if err := h.Truncate(ctx, 6); err != nil {
			t.Fatalf("handle Truncate = %v", err)
		}
		if got, err := h.ReadAt(ctx, 0, 64); err != nil || string(got) != "edited" {
			t.Fatalf("handle ReadAt = %q, %v; want \"edited\"", got, err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatalf("Flush = %v", err)
		}
		if got, err := cl.ReadAt(ctx, "d", "/notes/a.md", 0, 64); err != nil || string(got) != "edited" {
			t.Fatalf("path ReadAt after flush = %q, %v; want \"edited\"", got, err)
		}
	})
	t.Run("token reuse after release fails not-found", func(t *testing.T) {
		if err := h.Release(ctx); err != nil {
			t.Fatalf("Release = %v", err)
		}
		_, err := h.ReadAt(ctx, 0, 8)
		assertClass(t, err, ClassNotFound)
		assertClass(t, h.Release(ctx), ClassNotFound)
	})
}

func TestBridgeReleaseBackstopCommits(t *testing.T) {
	ft := newFakeHandleTree()
	ft.files["/n.md"] = []byte("old")
	cl := NewBridgeClient(serveBridge(t, ft))
	ctx := context.Background()

	h, err := cl.OpenHandle(ctx, "d", "/n.md")
	if err != nil {
		t.Fatalf("OpenHandle = %v", err)
	}
	if err := h.WriteAt(ctx, 0, []byte("new")); err != nil {
		t.Fatalf("handle WriteAt = %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("Release = %v", err)
	}
	got, err := cl.ReadAt(ctx, "d", "/n.md", 0, 8)
	if err != nil || string(got) != "new" {
		t.Fatalf("ReadAt after backstop release = %q, %v; want \"new\"", got, err)
	}
}

func TestBridgeHandleTokenErrors(t *testing.T) {
	ft := newFakeHandleTree()
	ft.files["/a"], ft.files["/b"] = []byte("aa"), []byte("bb")
	cl := NewBridgeClient(serveBridge(t, ft))
	ctx := context.Background()

	t.Run("unknown token", func(t *testing.T) {
		h := &Handle{c: cl, Domain: "d", Name: "/a", Token: "no-such-token"}
		_, err := h.ReadAt(ctx, 0, 8)
		assertClass(t, err, ClassNotFound)
	})
	t.Run("token bound to another name", func(t *testing.T) {
		h, err := cl.OpenHandle(ctx, "d", "/a")
		if err != nil {
			t.Fatalf("OpenHandle = %v", err)
		}
		wrong := &Handle{c: cl, Domain: "d", Name: "/b", Token: h.Token}
		_, err = wrong.ReadAt(ctx, 0, 8)
		assertClass(t, err, ClassInvalid)
	})
}

func TestBridgeReleaseAllHandles(t *testing.T) {
	ft := newFakeHandleTree()
	ft.files["/a"], ft.files["/b"] = []byte("aa"), []byte("bb")
	cl := NewBridgeClient(serveBridge(t, ft))
	ctx := context.Background()

	ha, err := cl.OpenHandle(ctx, "d", "/a")
	if err != nil {
		t.Fatalf("OpenHandle(/a) = %v", err)
	}
	hb, err := cl.OpenHandle(ctx, "d", "/b")
	if err != nil {
		t.Fatalf("OpenHandle(/b) = %v", err)
	}
	if err := cl.ReleaseAllHandles(ctx, "d"); err != nil {
		t.Fatalf("ReleaseAllHandles = %v", err)
	}
	_, err = ha.ReadAt(ctx, 0, 8)
	assertClass(t, err, ClassNotFound)
	_, err = hb.ReadAt(ctx, 0, 8)
	assertClass(t, err, ClassNotFound)
	// Tokenless serving is untouched by the sweep.
	if got, err := cl.ReadAt(ctx, "d", "/a", 0, 8); err != nil || string(got) != "aa" {
		t.Fatalf("path ReadAt after releaseall = %q, %v; want \"aa\"", got, err)
	}
}

func TestBridgeFlushPropagatesCommitVerdict(t *testing.T) {
	ft := newFakeHandleTree()
	ft.files["/a.md"] = []byte("good")
	ft.flushErr = classErr{msg: "unparseable note", class: ClassInvalid}
	cl := NewBridgeClient(serveBridge(t, ft))
	ctx := context.Background()

	h, err := cl.OpenHandle(ctx, "d", "/a.md")
	if err != nil {
		t.Fatalf("OpenHandle = %v", err)
	}
	if err := h.WriteAt(ctx, 0, []byte("bad!")); err != nil {
		t.Fatalf("handle WriteAt = %v", err)
	}
	assertClass(t, h.Flush(ctx), ClassInvalid)
	// The rejected buffer must not have committed.
	if got, err := cl.ReadAt(ctx, "d", "/a.md", 0, 8); err != nil || string(got) != "good" {
		t.Fatalf("path ReadAt after rejected flush = %q, %v; want \"good\"", got, err)
	}
}

func TestBridgeOpenRejectsEmptyToken(t *testing.T) {
	ft := newFakeHandleTree()
	ft.files["/a"] = []byte("x")
	ft.emptyToken = true
	cl := NewBridgeClient(serveBridge(t, ft))
	_, err := cl.OpenHandle(context.Background(), "d", "/a")
	if err == nil || !strings.Contains(err.Error(), "empty token") {
		t.Fatalf("OpenHandle = %v, want the empty-token contract failure", err)
	}
}

// --- wire freeze ---

func TestBridgeWireFreeze(t *testing.T) {
	if BridgeProtoVersion != 1 {
		t.Fatalf("BridgeProtoVersion = %d; the bridge protocol is frozen at 1 (additive-only)", BridgeProtoVersion)
	}
	ops := map[BridgeOp]string{
		BridgeOpManifest: "manifest", BridgeOpRead: "read", BridgeOpWrite: "write", BridgeOpClassify: "classify",
		BridgeOpStat: "stat", BridgeOpList: "list", BridgeOpReadAt: "readat", BridgeOpReadlink: "readlink",
		BridgeOpCreate: "create", BridgeOpWriteAt: "writeat", BridgeOpTruncate: "truncate",
		BridgeOpUnlink: "unlink", BridgeOpRename: "rename", BridgeOpMkdir: "mkdir",
		BridgeOpOpen: "open", BridgeOpFlush: "flush", BridgeOpRelease: "release", BridgeOpReleaseAll: "releaseall",
	}
	for op, want := range ops {
		if string(op) != want {
			t.Errorf("op = %q, want %q", op, want)
		}
	}
	for got, want := range map[string]string{
		ClassNotFound: "not-found", ClassInvalid: "invalid", ClassPerm: "perm",
		ClassTransient: "transient", ClassDeterministic: "deterministic", ClassUnsupported: "unsupported",
	} {
		if got != want {
			t.Errorf("class = %q, want %q", got, want)
		}
	}
	for kind, want := range map[EntryKind]string{
		EntrySymlink: "symlink", EntrySynth: "synth", EntryPrivate: "private", EntryDir: "dir",
	} {
		if string(kind) != want {
			t.Errorf("kind = %q, want %q", kind, want)
		}
	}

	req := BridgeRequest{Proto: 1, Op: "x", Domain: "d", Name: "n", Data: []byte("z"), Ofst: 7, Size: 3, To: "t2", Length: 9, Token: "tok"}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	wantReq := `{"proto":1,"op":"x","domain":"d","name":"n","data":"eg==","ofst":7,"size":3,"to":"t2","length":9,"token":"tok"}`
	if string(reqJSON) != wantReq {
		t.Errorf("request bytes = %s\nwant %s", reqJSON, wantReq)
	}

	e := Entry{Name: "a", Kind: EntrySynth, Version: "1", Size: 2, Target: "T", Private: true, Freshness: []string{"f"}, Mtime: 3, Birth: 4, Ino: 5}
	entryJSON := `{"name":"a","kind":"synth","version":"1","size":2,"target":"T","private":true,"freshness":["f"],"mtime":3,"birth":4,"ino":5}`
	resp := BridgeResponse{Proto: 1, OK: true, Error: "e", ErrClass: "c", Version: "v", Entries: []Entry{e}, Kind: "k", Data: []byte("z"), Item: &e, Target: "tg", Token: "tk"}
	respJSON, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	wantResp := `{"proto":1,"ok":true,"error":"e","err_class":"c","version":"v","entries":[` + entryJSON + `],"kind":"k","data":"eg==","item":` + entryJSON + `,"target":"tg","token":"tk"}`
	if string(respJSON) != wantResp {
		t.Errorf("response bytes = %s\nwant %s", respJSON, wantResp)
	}
}

// v018Entry and v018Response replicate the v0.18.0 wire structs field for
// field — the shape a pre-write-ops client decodes replies into. They must
// never track the current structs; they pin the vintage.
type v018Entry struct {
	Name      string   `json:"name"`
	Kind      string   `json:"kind"`
	Version   string   `json:"version"`
	Size      int64    `json:"size"`
	Target    string   `json:"target"`
	Private   bool     `json:"private"`
	Freshness []string `json:"freshness"`
}

type v018Response struct {
	Proto    int         `json:"proto"`
	OK       bool        `json:"ok"`
	Error    string      `json:"error"`
	ErrClass string      `json:"err_class"`
	Version  string      `json:"version"`
	Entries  []v018Entry `json:"entries"`
	Kind     string      `json:"kind"`
	Data     []byte      `json:"data"`
	Item     *v018Entry  `json:"item"`
	Target   string      `json:"target"`
}

// rawExchange performs one bridge exchange with pre-encoded request bytes and
// returns the raw reply line — the transport a foreign-vintage client uses,
// bypassing this module's structs entirely.
func rawExchange(t *testing.T, socket, reqLine string) []byte {
	t.Helper()
	conn, err := net.DialTimeout("unix", socket, time.Second)
	if err != nil {
		t.Fatalf("dial bridge: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(append([]byte(reqLine), '\n')); err != nil {
		t.Fatalf("send request: %v", err)
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return line
}

// TestBridgeV018ReadOnlyExchangeRoundTrips pins the additive-v1 discipline
// against the shipped vintage: each request below is the EXACT line a v0.18.0
// BridgeClient emitted (no to/length/token fields), and every reply must
// decode into the v0.18 response shape with unknown fields REFUSED — a
// read-only exchange's reply bytes never grow keys an old client has no field
// for. The consumer is a plain Tree (the only surface v0.18 knew), so the
// Entry attrs added since (mtime/birth/ino) stay absent too.
func TestBridgeV018ReadOnlyExchangeRoundTrips(t *testing.T) {
	socket := serveBridge(t, fakeTree{})
	cases := []struct {
		name  string
		req   string
		check func(t *testing.T, resp v018Response)
	}{
		{
			"manifest",
			`{"proto":1,"op":"manifest","domain":"d"}`,
			func(t *testing.T, resp v018Response) {
				if len(resp.Entries) != 0 {
					t.Fatalf("entries = %+v, want none", resp.Entries)
				}
			},
		},
		{
			"read",
			`{"proto":1,"op":"read","domain":"d","name":"x"}`,
			func(t *testing.T, resp v018Response) {},
		},
		{
			"write",
			`{"proto":1,"op":"write","domain":"d","name":"x","data":"aGk="}`,
			func(t *testing.T, resp v018Response) {},
		},
		{
			"classify",
			`{"proto":1,"op":"classify","name":"x"}`,
			func(t *testing.T, resp v018Response) {
				if resp.Kind != "synth" {
					t.Fatalf("kind = %q, want synth", resp.Kind)
				}
			},
		},
		{
			"stat",
			`{"proto":1,"op":"stat","domain":"d","name":"x"}`,
			func(t *testing.T, resp v018Response) {
				if resp.Item == nil || resp.Item.Name != "x" || resp.Item.Version != "v9" || resp.Item.Size != 7 {
					t.Fatalf("item = %+v, want x/v9/7", resp.Item)
				}
			},
		},
		{
			"list",
			`{"proto":1,"op":"list","domain":"d","name":"/"}`,
			func(t *testing.T, resp v018Response) {
				if len(resp.Entries) != 2 || resp.Entries[0].Name != "a" || resp.Entries[1].Kind != "symlink" {
					t.Fatalf("entries = %+v, want [a b]", resp.Entries)
				}
			},
		},
		{
			"readat",
			`{"proto":1,"op":"readat","domain":"d","name":"x","ofst":3,"size":4}`,
			func(t *testing.T, resp v018Response) {
				if string(resp.Data) != "3456" {
					t.Fatalf("data = %q, want \"3456\"", resp.Data)
				}
			},
		},
		{
			"readlink",
			`{"proto":1,"op":"readlink","domain":"d","name":"lnk"}`,
			func(t *testing.T, resp v018Response) {
				if resp.Target != "/abs/lnk" {
					t.Fatalf("target = %q, want /abs/lnk", resp.Target)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			line := rawExchange(t, socket, tc.req)
			dec := json.NewDecoder(bytes.NewReader(line))
			dec.DisallowUnknownFields()
			var resp v018Response
			if err := dec.Decode(&resp); err != nil {
				t.Fatalf("reply does not decode into the v0.18 shape: %v\nreply: %s", err, line)
			}
			if !resp.OK || resp.Proto != 1 {
				t.Fatalf("reply = ok %v proto %d, want an OK proto-1 reply (%s)", resp.OK, resp.Proto, line)
			}
			tc.check(t, resp)
		})
	}
}
