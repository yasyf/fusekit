package holderfs

import (
	"bytes"
	"errors"
	"fmt"
	"path"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/fusekit/content"
)

// cerr is a ClassedError test double carrying a wire class.
type cerr struct{ msg, class string }

func (e cerr) Error() string { return e.msg }
func (e cerr) Class() string { return e.class }

func enoent() int { return -int(syscall.ENOENT) }

// treeFake is an in-memory content.Tree served over a real bridge: a flat map
// of files, dirs, and links plus per-op counters and a block gate that models
// a hung consumer. It implements Tree ONLY — the writable and handle
// capabilities layer on via embedding, so the bridge's interface assertions
// see exactly the surface each test means to offer.
type treeFake struct {
	mu       sync.Mutex
	files    map[string][]byte
	dirs     map[string]bool
	links    map[string]string
	mtimes   map[string]int64  // consumer-reported Mtime per path
	inos     map[string]uint64 // consumer identity keys; 0 = none supplied
	counts   map[string]int    // "op:path" -> calls
	block    chan struct{}     // non-nil: every Tree op waits on it
	flushErr error
	truncErr error // non-nil: TruncateHandle fails with it after token lookup

	// statExit, when non-nil, parks Stat BETWEEN computing its reply and
	// returning it — the shape of a stale answer already read from consumer
	// state while newer truth lands. statParked counts goroutines holding a
	// computed reply, so a test can rendezvous deterministically.
	statExit   chan struct{}
	statParked int
}

func newTreeFake() *treeFake {
	return &treeFake{
		files:  map[string][]byte{},
		dirs:   map[string]bool{"/": true},
		links:  map[string]string{},
		mtimes: map[string]int64{},
		inos:   map[string]uint64{},
		counts: map[string]int{},
	}
}

func (f *treeFake) gate(op, name string) {
	f.mu.Lock()
	f.counts[op+":"+name]++
	blk := f.block
	f.mu.Unlock()
	if blk != nil {
		<-blk
	}
}

func (f *treeFake) count(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[key]
}

func (f *treeFake) setBlock(ch chan struct{}) {
	f.mu.Lock()
	f.block = ch
	f.mu.Unlock()
}

func (f *treeFake) setStatExitBlock(ch chan struct{}) {
	f.mu.Lock()
	f.statExit = ch
	f.mu.Unlock()
}

// statExitGate is deferred by Stat before it takes the state lock, so it runs
// after the reply is computed and the lock released. Callers rendezvous on
// parkedStats before mutating newer truth under the parked reply.
func (f *treeFake) statExitGate() {
	f.mu.Lock()
	blk := f.statExit
	if blk != nil {
		f.statParked++
	}
	f.mu.Unlock()
	if blk != nil {
		<-blk
	}
}

func (f *treeFake) parkedStats() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statParked
}

// snapshot copies the op counters, so a test can assert the exact bridge
// traffic one call produced.
func (f *treeFake) snapshot() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int, len(f.counts))
	for k, v := range f.counts {
		out[k] = v
	}
	return out
}

func (f *treeFake) put(name string, data []byte, mtime int64, ino uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[name] = data
	f.mtimes[name] = mtime
	f.inos[name] = ino
}

// putDir seeds a directory entry with a consumer mtime and identity key (0 =
// path-keyed), the dir-side counterpart of put.
func (f *treeFake) putDir(name string, mtime int64, ino uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dirs[name] = true
	f.mtimes[name] = mtime
	f.inos[name] = ino
}

func (f *treeFake) bytes(name string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.files[name]...)
}

func (f *treeFake) Manifest(string) ([]content.Entry, error)  { return nil, nil }
func (f *treeFake) ReadSynth(string, string) ([]byte, error)  { return nil, nil }
func (f *treeFake) WriteThrough(string, string, []byte) error { return nil }
func (f *treeFake) Classify(string) content.EntryKind         { return content.EntrySynth }

func (f *treeFake) Stat(_, name string) (content.Entry, error) {
	f.gate("stat", name)
	defer f.statExitGate() // deferred first: runs after the mu defer releases the lock
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dirs[name] {
		return content.Entry{Name: path.Base(name), Kind: content.EntryDir, Mtime: f.mtimes[name], Ino: f.inos[name]}, nil
	}
	if target, ok := f.links[name]; ok {
		return content.Entry{Name: path.Base(name), Kind: content.EntrySymlink, Target: target, Mtime: f.mtimes[name]}, nil
	}
	if buf, ok := f.files[name]; ok {
		return content.Entry{Name: path.Base(name), Kind: content.EntrySynth, Size: int64(len(buf)), Mtime: f.mtimes[name], Ino: f.inos[name]}, nil
	}
	return content.Entry{}, cerr{"no entry " + name, content.ClassNotFound}
}

func (f *treeFake) List(_, name string) ([]content.Entry, error) {
	f.gate("list", name)
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirs[name] {
		return nil, cerr{"not a dir: " + name, content.ClassNotFound}
	}
	var out []content.Entry
	for p, buf := range f.files {
		if path.Dir(p) == name {
			out = append(out, content.Entry{Name: path.Base(p), Kind: content.EntrySynth, Size: int64(len(buf)), Mtime: f.mtimes[p], Ino: f.inos[p]})
		}
	}
	for p := range f.dirs {
		if p != "/" && path.Dir(p) == name {
			out = append(out, content.Entry{Name: path.Base(p), Kind: content.EntryDir, Mtime: f.mtimes[p], Ino: f.inos[p]})
		}
	}
	for p, target := range f.links {
		if path.Dir(p) == name {
			out = append(out, content.Entry{Name: path.Base(p), Kind: content.EntrySymlink, Target: target})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (f *treeFake) ReadAt(_, name string, ofst int64, size int) ([]byte, error) {
	f.gate("readat", name)
	f.mu.Lock()
	defer f.mu.Unlock()
	buf, ok := f.files[name]
	if !ok {
		return nil, cerr{"no entry " + name, content.ClassNotFound}
	}
	if ofst >= int64(len(buf)) {
		return nil, nil
	}
	end := ofst + int64(size)
	if end > int64(len(buf)) {
		end = int64(len(buf))
	}
	return append([]byte(nil), buf[ofst:end]...), nil
}

func (f *treeFake) Readlink(_, name string) (string, error) {
	f.gate("readlink", name)
	f.mu.Lock()
	defer f.mu.Unlock()
	if target, ok := f.links[name]; ok {
		return target, nil
	}
	return "", cerr{"not a symlink: " + name, content.ClassInvalid}
}

// treeFakeRW layers WritableTree over treeFake.
type treeFakeRW struct{ *treeFake }

func newTreeFakeRW() treeFakeRW { return treeFakeRW{newTreeFake()} }

func (f treeFakeRW) Create(_, name string) error {
	f.gate("create", name)
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.files[name]; !ok {
		f.files[name] = []byte{}
	}
	return nil
}

func (f treeFakeRW) WriteAt(_, name string, ofst int64, data []byte) error {
	f.gate("writeat", name)
	f.mu.Lock()
	defer f.mu.Unlock()
	buf, ok := f.files[name]
	if !ok {
		return cerr{"no entry " + name, content.ClassNotFound}
	}
	f.files[name] = bufWriteAt(buf, ofst, data)
	return nil
}

func (f treeFakeRW) Truncate(_, name string, size int64) error {
	f.gate("truncate", name)
	f.mu.Lock()
	defer f.mu.Unlock()
	buf, ok := f.files[name]
	if !ok {
		return cerr{"no entry " + name, content.ClassNotFound}
	}
	f.files[name] = bufResize(buf, size)
	return nil
}

func (f treeFakeRW) Unlink(_, name string) error {
	f.gate("unlink", name)
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.files[name]; !ok {
		return cerr{"no entry " + name, content.ClassNotFound}
	}
	delete(f.files, name)
	return nil
}

// reparentKeys moves every key rooted at oldName (oldName itself and each
// descendant under oldName+"/") to the newName root within one map. The
// consumer's List derives a dir's children by path.Dir, so re-keying the maps
// is all a directory rename needs for listings to reflect the move.
func reparentKeys[V any](m map[string]V, oldName, newName string) {
	var moved []string
	for k := range m {
		if k == oldName || strings.HasPrefix(k, oldName+"/") {
			moved = append(moved, k)
		}
	}
	for _, k := range moved {
		m[newName+k[len(oldName):]] = m[k]
		delete(m, k)
	}
}

// anyKeyUnder reports whether any key in m lives under prefix.
func anyKeyUnder[V any](m map[string]V, prefix string) bool {
	for k := range m {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

func (f treeFakeRW) Rename(_, oldName, newName string) error {
	f.gate("rename", oldName)
	f.mu.Lock()
	defer f.mu.Unlock()
	// A directory (or any path with descendants) reparents its whole subtree:
	// every key rooted at oldName moves to the newName root across all maps.
	// Identity (f.inos) and consumer mtime (f.mtimes) follow each object because
	// they are keyed by the same paths.
	prefix := oldName + "/"
	if f.dirs[oldName] || anyKeyUnder(f.files, prefix) || anyKeyUnder(f.dirs, prefix) || anyKeyUnder(f.links, prefix) {
		reparentKeys(f.files, oldName, newName)
		reparentKeys(f.dirs, oldName, newName)
		reparentKeys(f.links, oldName, newName)
		reparentKeys(f.inos, oldName, newName)
		reparentKeys(f.mtimes, oldName, newName)
		return nil
	}
	buf, ok := f.files[oldName]
	if !ok {
		return cerr{"no entry " + oldName, content.ClassNotFound}
	}
	delete(f.files, oldName)
	f.files[newName] = buf
	// The consumer identity key follows its object across the rename.
	f.inos[newName] = f.inos[oldName]
	f.mtimes[newName] = f.mtimes[oldName]
	delete(f.inos, oldName)
	delete(f.mtimes, oldName)
	return nil
}

func (f treeFakeRW) Mkdir(_, name string) error {
	f.gate("mkdir", name)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dirs[name] = true
	return nil
}

// treeFakeH layers HandleTree over treeFakeRW: a real token registry with a
// snapshot buffer per token, dirty tracking, and a flush-verdict injection.
type treeFakeH struct {
	treeFakeRW
	hmu     sync.Mutex
	nextTok int
	tokens  map[string]*fakeTok
}

type fakeTok struct {
	name  string
	buf   []byte
	dirty bool
}

func newTreeFakeH() *treeFakeH {
	return &treeFakeH{treeFakeRW: newTreeFakeRW(), tokens: map[string]*fakeTok{}}
}

func (f *treeFakeH) OpenHandle(_, name string) (string, content.Entry, error) {
	f.gate("open", name)
	f.mu.Lock()
	buf, ok := f.files[name]
	snap := append([]byte(nil), buf...)
	e := content.Entry{Name: path.Base(name), Kind: content.EntrySynth, Size: int64(len(snap)), Mtime: f.mtimes[name], Ino: f.inos[name]}
	f.mu.Unlock()
	if !ok {
		return "", content.Entry{}, cerr{"no entry " + name, content.ClassNotFound}
	}
	f.hmu.Lock()
	defer f.hmu.Unlock()
	f.nextTok++
	tok := fmt.Sprintf("tok-%d", f.nextTok)
	f.tokens[tok] = &fakeTok{name: name, buf: snap}
	return tok, e, nil
}

func (f *treeFakeH) tok(name, token string) (*fakeTok, error) {
	h, ok := f.tokens[token]
	if !ok {
		return nil, cerr{"unknown token " + token, content.ClassNotFound}
	}
	if h.name != name {
		return nil, cerr{"token bound to " + h.name, content.ClassInvalid}
	}
	return h, nil
}

func (f *treeFakeH) ReadAtHandle(_, name, token string, ofst int64, size int) ([]byte, error) {
	f.gate("readat-h", name)
	f.hmu.Lock()
	defer f.hmu.Unlock()
	h, err := f.tok(name, token)
	if err != nil {
		return nil, err
	}
	if ofst >= int64(len(h.buf)) {
		return nil, nil
	}
	end := ofst + int64(size)
	if end > int64(len(h.buf)) {
		end = int64(len(h.buf))
	}
	return append([]byte(nil), h.buf[ofst:end]...), nil
}

func (f *treeFakeH) WriteAtHandle(_, name, token string, ofst int64, data []byte) error {
	f.gate("writeat-h", name)
	f.hmu.Lock()
	defer f.hmu.Unlock()
	h, err := f.tok(name, token)
	if err != nil {
		return err
	}
	h.buf = bufWriteAt(h.buf, ofst, data)
	h.dirty = true
	return nil
}

func (f *treeFakeH) TruncateHandle(_, name, token string, size int64) error {
	f.gate("truncate-h", name)
	f.hmu.Lock()
	defer f.hmu.Unlock()
	h, err := f.tok(name, token)
	if err != nil {
		return err
	}
	f.mu.Lock()
	terr := f.truncErr
	f.mu.Unlock()
	if terr != nil {
		return terr
	}
	h.buf = bufResize(h.buf, size)
	h.dirty = true
	return nil
}

func (f *treeFakeH) FlushHandle(_, name, token string) error {
	f.gate("flush", name)
	f.hmu.Lock()
	defer f.hmu.Unlock()
	h, err := f.tok(name, token)
	if err != nil {
		return err
	}
	f.mu.Lock()
	ferr := f.flushErr
	f.mu.Unlock()
	if ferr != nil {
		return ferr
	}
	f.mu.Lock()
	f.files[name] = append([]byte(nil), h.buf...)
	f.mu.Unlock()
	h.dirty = false
	return nil
}

func (f *treeFakeH) ReleaseHandle(_, name, token string) error {
	f.gate("release", name)
	f.hmu.Lock()
	defer f.hmu.Unlock()
	if _, err := f.tok(name, token); err != nil {
		return err
	}
	delete(f.tokens, token)
	return nil
}

func (f *treeFakeH) ReleaseAllHandles(string) error {
	f.gate("releaseall", "")
	f.hmu.Lock()
	defer f.hmu.Unlock()
	clear(f.tokens)
	return nil
}

func (f *treeFakeH) tokenCount() int {
	f.hmu.Lock()
	defer f.hmu.Unlock()
	return len(f.tokens)
}

// newTestView serves src over a real bridge and returns a view on it.
func newTestView(t *testing.T, src content.Source) *treeView {
	t.Helper()
	return newTreeView("d", content.NewBridgeClient(serveContent(t, src)))
}

func shrinkTreeWaits(t *testing.T, op, fresh time.Duration) {
	t.Helper()
	oldOp, oldFresh := treeOpWait, treeFreshFor
	treeOpWait, treeFreshFor = op, fresh
	t.Cleanup(func() { treeOpWait, treeFreshFor = oldOp, oldFresh })
}

// countsDelta reports the op counters that moved between two snapshots.
func countsDelta(before, after map[string]int) map[string]int {
	delta := map[string]int{}
	for k, v := range after {
		if d := v - before[k]; d != 0 {
			delta[k] = d
		}
	}
	return delta
}

// waitDelta polls until the op-count delta since before equals want — for
// calls whose bridge traffic is scheduled off the caller's path.
func waitDelta(t *testing.T, f *treeFake, before, want map[string]int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := countsDelta(before, f.snapshot())
		if reflect.DeepEqual(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("op delta = %v, want %v", got, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestTreeErrnoMapping pins the wire-class → errno translation, including the
// mutation-side EROFS for a capability miss (read-only tenant or old server).
func TestTreeErrnoMapping(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		want      int
		wantWrite int
	}{
		{"not-found", cerr{"x", content.ClassNotFound}, -int(syscall.ENOENT), -int(syscall.ENOENT)},
		{"invalid", cerr{"x", content.ClassInvalid}, -int(syscall.EINVAL), -int(syscall.EINVAL)},
		{"perm", cerr{"x", content.ClassPerm}, -int(syscall.EPERM), -int(syscall.EPERM)},
		{"transient", cerr{"x", content.ClassTransient}, -int(syscall.EIO), -int(syscall.EIO)},
		{"class-less", errors.New("boom"), -int(syscall.EIO), -int(syscall.EIO)},
		{"unsupported", cerr{"x", content.ClassUnsupported}, -int(syscall.EIO), -int(syscall.EROFS)},
		{"unknown-op vintage", errors.New("unknown op: writeat"), -int(syscall.EIO), -int(syscall.EROFS)},
		{"bridge unavailable", fmt.Errorf("%w: dial", content.ErrBridgeUnavailable), -int(syscall.EIO), -int(syscall.EIO)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classErrno(tc.err); got != tc.want {
				t.Errorf("classErrno = %d, want %d", got, tc.want)
			}
			if got := writeErrno(tc.err); got != tc.wantWrite {
				t.Errorf("writeErrno = %d, want %d", got, tc.wantWrite)
			}
		})
	}
}

// TestTreeViewRootIsLocal pins that "/" is the holder's own structural object:
// a stable dir with a minted ino, answered with zero Stat RPCs ever.
func TestTreeViewRootIsLocal(t *testing.T) {
	f := newTreeFakeH()
	v := newTestView(t, f)
	for i := 0; i < 3; i++ {
		st, rc := v.getattr("/")
		if rc != 0 || st.kind != content.EntryDir {
			t.Fatalf("getattr(/) = (%+v, %d), want a dir", st, rc)
		}
		if st.ino != treeInoBase {
			t.Fatalf("root ino = %d, want the first minted id %d", st.ino, treeInoBase)
		}
	}
	if n := f.count("stat:/"); n != 0 {
		t.Fatalf("consumer saw %d root stats, want 0 (the root is structural)", n)
	}
}

// TestTreeViewMintedInosStable pins W2's inode rule for tree mode: the served
// fileid is minted once per entry, stable across Stat, List, refresh, and
// rename, and is never the consumer's raw identity key.
func TestTreeViewMintedInosStable(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a", []byte("aa"), 100, 0)  // path-keyed (no consumer identity)
	f.put("/b", []byte("bb"), 100, 42) // consumer-keyed identity 42
	v := newTestView(t, f)

	stA, rc := v.getattr("/a")
	if rc != 0 {
		t.Fatalf("getattr(/a) = %d", rc)
	}
	stB, rc := v.getattr("/b")
	if rc != 0 {
		t.Fatalf("getattr(/b) = %d", rc)
	}
	if stA.ino < treeInoBase || stB.ino < treeInoBase {
		t.Fatalf("minted inos = %d, %d; want >= %d (never below the minted pool)", stA.ino, stB.ino, treeInoBase)
	}
	if stB.ino == 42 {
		t.Fatal("served /b's raw consumer identity key as its ino")
	}
	if stA.ino == stB.ino {
		t.Fatal("two entries share one minted ino")
	}

	// Readdir lists the same minted inos.
	ents, rc := v.readdir("/")
	if rc != 0 {
		t.Fatalf("readdir(/) = %d", rc)
	}
	byName := map[string]treeStat{}
	for _, e := range ents {
		byName[e.name] = e.st
	}
	if byName["a"].ino != stA.ino || byName["b"].ino != stB.ino {
		t.Fatalf("readdir inos = %d, %d; want %d, %d", byName["a"].ino, byName["b"].ino, stA.ino, stB.ino)
	}

	// A refresh does not re-mint.
	if err := v.fetchStat("/a"); err != nil {
		t.Fatalf("fetchStat(/a) = %v", err)
	}
	if st, _ := v.getattr("/a"); st.ino != stA.ino {
		t.Fatalf("ino after refresh = %d, want %d", st.ino, stA.ino)
	}

	// Renames keep the fileid: consumer-keyed by identity, path-keyed by transfer.
	if rc := v.rename("/b", "/b2"); rc != 0 {
		t.Fatalf("rename(/b, /b2) = %d", rc)
	}
	if st, rc := v.getattr("/b2"); rc != 0 || st.ino != stB.ino {
		t.Fatalf("getattr(/b2) = (%d, ino %d), want ino %d (identity-keyed rename)", rc, st.ino, stB.ino)
	}
	if rc := v.rename("/a", "/a2"); rc != 0 {
		t.Fatalf("rename(/a, /a2) = %d", rc)
	}
	if st, rc := v.getattr("/a2"); rc != 0 || st.ino != stA.ino {
		t.Fatalf("getattr(/a2) = (%d, ino %d), want ino %d (path-keyed transfer)", rc, st.ino, stA.ino)
	}
	if _, rc := v.getattr("/a"); rc != enoent() {
		t.Fatalf("getattr(/a) after rename = %d, want ENOENT", rc)
	}
}

// TestTreeViewLateIdentityRekeysMintedIno pins the registry re-key rule: a
// file created through the mount is path-keyed until the consumer assigns its
// entity identity (typically at first commit), and the registry must follow —
// the path key dies and the identity key adopts the SAME minted ino. Without
// the re-key, the editor atomic-save flow (create x.tmp, rename onto x,
// create x.tmp again) leaves the stale path key handing the renamed file's
// fileid to a brand-new file: two live files, one served ino.
func TestTreeViewLateIdentityRekeysMintedIno(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := newTreeFakeH()
	v := newTestView(t, f)

	// Save 1: /x.tmp is created through the mount — no consumer identity yet.
	fh, rc := v.create("/x.tmp")
	if rc != 0 {
		t.Fatalf("create(/x.tmp) = %d", rc)
	}
	if n := v.write(fh, []byte("v1"), 0); n != 2 {
		t.Fatalf("write = %d", n)
	}
	if rc := v.flush(fh); rc != 0 {
		t.Fatalf("flush = %d", rc)
	}
	// The consumer assigns entity identity 77 at commit; the post-close
	// refresh lands it on the still-path-keyed node.
	f.put("/x.tmp", []byte("v1"), t1.UnixNano(), 77)
	if rc := v.release(fh); rc != 0 {
		t.Fatalf("release = %d", rc)
	}
	if err := v.fetchStat("/x.tmp"); err != nil {
		t.Fatalf("fetchStat(/x.tmp) = %v", err)
	}
	st, rc := v.getattr("/x.tmp")
	if rc != 0 {
		t.Fatalf("getattr(/x.tmp) = %d", rc)
	}
	inoX := st.ino

	// The atomic-save rename: the identity keeps the fileid.
	if rc := v.rename("/x.tmp", "/x"); rc != 0 {
		t.Fatalf("rename(/x.tmp, /x) = %d", rc)
	}
	if st, rc := v.getattr("/x"); rc != 0 || st.ino != inoX {
		t.Fatalf("getattr(/x) = (ino %d, rc %d), want ino %d across the rename", st.ino, rc, inoX)
	}

	// Save 2: the NEXT file created at /x.tmp is a new object and must mint a
	// fresh fileid — a stale "p:/x.tmp" key would hand it inoX.
	fh2, rc := v.create("/x.tmp")
	if rc != 0 {
		t.Fatalf("second create(/x.tmp) = %d", rc)
	}
	st2, rc := v.getattr("/x.tmp")
	if rc != 0 {
		t.Fatalf("getattr(re-created /x.tmp) = %d", rc)
	}
	if st2.ino == inoX {
		t.Fatal("the re-created /x.tmp serves the renamed file's fileid — the stale path key was resurrected")
	}
	if rc := v.release(fh2); rc != 0 {
		t.Fatalf("release(fh2) = %d", rc)
	}

	// /x still answers inoX after a fresh consumer verdict (identity-keyed).
	if err := v.fetchStat("/x"); err != nil {
		t.Fatalf("fetchStat(/x) = %v", err)
	}
	if st, _ := v.getattr("/x"); st.ino != inoX {
		t.Fatalf("getattr(/x) after refresh = ino %d, want the stable %d", st.ino, inoX)
	}

	// The re-key MOVED the mapping, not merely dropped the path key: a
	// consumer-side rename of the entity to a path the holder has never seen
	// must find the SAME minted fileid by identity.
	f.mu.Lock()
	f.files["/y"] = f.files["/x"]
	f.inos["/y"] = 77
	f.mtimes["/y"] = f.mtimes["/x"]
	delete(f.files, "/x")
	delete(f.inos, "/x")
	delete(f.mtimes, "/x")
	f.mu.Unlock()
	if st, rc := v.getattr("/y"); rc != 0 || st.ino != inoX {
		t.Fatalf("getattr(/y) after a consumer-side rename = (ino %d, rc %d), want ino %d (the identity key must carry the minted ino)", st.ino, rc, inoX)
	}
}

// TestTreeViewRenameClearsReplacedDestKey pins the other stale-key hole:
// renaming ONTO an existing path-keyed file replaces that object, so its "p:"
// registry entry must die with it — otherwise a file later created at the
// dest path (after the survivor moves on) resurrects the dead file's fileid
// while an open handle on the replaced file still serves it.
func TestTreeViewRenameClearsReplacedDestKey(t *testing.T) {
	f := newTreeFakeH()
	f.put("/dst", []byte("old"), 0, 0)   // path-keyed
	f.put("/src", []byte("new!"), 0, 55) // identity-keyed
	v := newTestView(t, f)

	stDst, rc := v.getattr("/dst")
	if rc != 0 {
		t.Fatalf("getattr(/dst) = %d", rc)
	}
	stSrc, rc := v.getattr("/src")
	if rc != 0 {
		t.Fatalf("getattr(/src) = %d", rc)
	}
	fh, rc := v.open("/dst", syscall.O_RDONLY) // the replaced file stays open
	if rc != 0 {
		t.Fatalf("open(/dst) = %d", rc)
	}

	// /src replaces /dst, then moves on: no live object answers to the dest
	// name anymore, so its path key must be gone.
	if rc := v.rename("/src", "/dst"); rc != 0 {
		t.Fatalf("rename(/src, /dst) = %d", rc)
	}
	if rc := v.rename("/dst", "/final"); rc != 0 {
		t.Fatalf("rename(/dst, /final) = %d", rc)
	}
	if st, rc := v.getattr("/final"); rc != 0 || st.ino != stSrc.ino {
		t.Fatalf("getattr(/final) = (ino %d, rc %d), want the survivor's %d", st.ino, rc, stSrc.ino)
	}

	// A brand-new file at the dest path must mint a fresh fileid; resurrecting
	// the replaced file's would alias it with the still-open handle.
	fh2, rc := v.create("/dst")
	if rc != 0 {
		t.Fatalf("create(/dst) = %d", rc)
	}
	stNew, rc := v.getattr("/dst")
	if rc != 0 {
		t.Fatalf("getattr(new /dst) = %d", rc)
	}
	if stNew.ino == stDst.ino {
		t.Fatal("the new /dst resurrected the replaced file's fileid — stale \"p:\" dest key")
	}
	if st, rc := v.getattrHandle(fh); rc != 0 || st.ino != stDst.ino {
		t.Fatalf("getattrHandle(replaced /dst) = (ino %d, rc %d), want the original %d", st.ino, rc, stDst.ino)
	}
	if rc := v.release(fh2); rc != 0 {
		t.Fatalf("release(fh2) = %d", rc)
	}
	if rc := v.release(fh); rc != 0 {
		t.Fatalf("release(fh) = %d", rc)
	}
}

// TestTreeViewRenameDirectoryReparentsChildren pins the subtree re-key: when a
// directory is renamed, every cached descendant node moves to its new path with
// its minted ino, kind, and mtime intact. Without it, a readdir of the new dir
// mints blank (ino 0, kind "") dirents for its children, the old child paths
// resolve as ghosts, and a path-keyed child held open across the parent rename
// is handed a different served fileid — the invalidation-under-open churn W2
// forbids. Both keying modes are covered: path-keyed descendants (the
// fileid-stability-at-risk case) and identity-keyed ones.
func TestTreeViewRenameDirectoryReparentsChildren(t *testing.T) {
	cases := []struct {
		name string
		base uint64 // consumer identity base; 0 = path-keyed (no Entry.Ino)
	}{
		{"path-keyed", 0},
		{"identity-keyed", 700},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// id yields a distinct nonzero consumer identity per entry when the
			// mode is identity-keyed, else 0 so the holder keys the entry by path.
			id := func(n uint64) uint64 {
				if tc.base == 0 {
					return 0
				}
				return tc.base + n
			}
			f := newTreeFakeH()
			f.putDir("/d", 100, id(0))
			f.put("/d/a", []byte("aa"), 101, id(1))
			f.putDir("/d/sub", 102, id(2))
			f.put("/d/sub/b", []byte("bbb"), 103, id(3))
			v := newTestView(t, f)

			// Warm the whole subtree: list the dir, then stat each node so every
			// one carries a minted ino, its kind, and a nonzero served mtime.
			ents, rc := v.readdir("/d")
			if rc != 0 {
				t.Fatalf("readdir(/d) = %d", rc)
			}
			listed := map[string]bool{}
			for _, e := range ents {
				listed[e.name] = true
			}
			if len(listed) != 2 || !listed["a"] || !listed["sub"] {
				t.Fatalf("readdir(/d) listed %v, want a and sub", listed)
			}

			type snap struct {
				ino   uint64
				kind  content.EntryKind
				mtime time.Time
			}
			before := map[string]snap{}
			for _, p := range []string{"/d", "/d/a", "/d/sub", "/d/sub/b"} {
				st, rc := v.getattr(p)
				if rc != 0 {
					t.Fatalf("getattr(%s) = %d", p, rc)
				}
				if st.ino < treeInoBase {
					t.Fatalf("getattr(%s) ino = %d, want a minted id >= %d", p, st.ino, treeInoBase)
				}
				if st.mtime.IsZero() {
					t.Fatalf("getattr(%s) mtime is zero before rename", p)
				}
				before[p] = snap{st.ino, st.kind, st.mtime}
			}
			if before["/d"].kind != content.EntryDir || before["/d/sub"].kind != content.EntryDir {
				t.Fatalf("dir kinds = %q, %q; want directories", before["/d"].kind, before["/d/sub"].kind)
			}
			if before["/d/a"].kind != content.EntrySynth || before["/d/sub/b"].kind != content.EntrySynth {
				t.Fatalf("file kinds = %q, %q; want regular files", before["/d/a"].kind, before["/d/sub/b"].kind)
			}

			if rc := v.rename("/d", "/e"); rc != 0 {
				t.Fatalf("rename(/d, /e) = %d", rc)
			}

			// Every object serves its ORIGINAL minted ino and kind at its NEW
			// path, with a nonzero mtime that never regresses — the subtree moved
			// without re-minting or invalidating.
			for np, op := range map[string]string{"/e": "/d", "/e/a": "/d/a", "/e/sub": "/d/sub", "/e/sub/b": "/d/sub/b"} {
				st, rc := v.getattr(np)
				if rc != 0 {
					t.Fatalf("getattr(%s) after rename = %d", np, rc)
				}
				was := before[op]
				if st.ino == 0 {
					t.Fatalf("getattr(%s) served ino 0 — a blank node was minted at the new path", np)
				}
				if st.ino != was.ino {
					t.Fatalf("getattr(%s) ino = %d, want the preserved %d (fileid churn under rename)", np, st.ino, was.ino)
				}
				if st.kind != was.kind {
					t.Fatalf("getattr(%s) kind = %q, want the preserved %q", np, st.kind, was.kind)
				}
				if st.mtime.IsZero() || st.mtime.Before(was.mtime) {
					t.Fatalf("getattr(%s) mtime = %v, want nonzero and >= %v", np, st.mtime, was.mtime)
				}
			}

			// The moved subdir is still a directory and its leaf still a file —
			// the blank-node bug flips a subdir to a regular file (kind "").
			if st, _ := v.getattr("/e/sub"); st.kind != content.EntryDir {
				t.Fatalf("getattr(/e/sub) kind = %q, want a directory", st.kind)
			}
			if st, _ := v.getattr("/e/sub/b"); st.kind != content.EntrySynth {
				t.Fatalf("getattr(/e/sub/b) kind = %q, want a regular file", st.kind)
			}

			// readdir of the new dir lists both children with their preserved
			// inos — no ino-0 (blank READDIRPLUS) dirent, correct kinds.
			ents, rc = v.readdir("/e")
			if rc != 0 {
				t.Fatalf("readdir(/e) = %d", rc)
			}
			byName := map[string]treeStat{}
			for _, e := range ents {
				if e.st.ino == 0 {
					t.Fatalf("readdir(/e) child %q served ino 0 — blank dirent", e.name)
				}
				byName[e.name] = e.st
			}
			if byName["a"].ino != before["/d/a"].ino || byName["sub"].ino != before["/d/sub"].ino {
				t.Fatalf("readdir(/e) inos = a:%d sub:%d; want a:%d sub:%d", byName["a"].ino, byName["sub"].ino, before["/d/a"].ino, before["/d/sub"].ino)
			}
			if byName["sub"].kind != content.EntryDir {
				t.Fatalf("readdir(/e) sub kind = %q, want a directory", byName["sub"].kind)
			}

			// No ghost at the old paths: the renamed dir and every old child
			// answer ENOENT.
			for _, op := range []string{"/d", "/d/a", "/d/sub", "/d/sub/b"} {
				if _, rc := v.getattr(op); rc != enoent() {
					t.Fatalf("getattr(%s) after rename = %d, want ENOENT (ghost at old path)", op, rc)
				}
			}
		})
	}
}

// TestTreeViewServedMtimeMonotonic pins W2's mtime rule: the served mtime is a
// high-water mark — a consumer whose reported mtime regresses (re-render,
// clock skew) never rewinds what the NFS client has seen.
func TestTreeViewServedMtimeMonotonic(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2, t3 := t1.Add(time.Hour), t1.Add(2*time.Hour)

	f := newTreeFakeH()
	f.put("/a", []byte("x"), t2.UnixNano(), 0)
	v := newTestView(t, f)

	if st, rc := v.getattr("/a"); rc != 0 || !st.mtime.Equal(t2) {
		t.Fatalf("served mtime = %v (rc %d), want %v", st.mtime, rc, t2)
	}
	f.put("/a", []byte("x"), t1.UnixNano(), 0) // consumer regresses
	if err := v.fetchStat("/a"); err != nil {
		t.Fatalf("fetchStat = %v", err)
	}
	if st, _ := v.getattr("/a"); !st.mtime.Equal(t2) {
		t.Fatalf("served mtime after consumer regression = %v, want the %v high-water mark", st.mtime, t2)
	}
	f.put("/a", []byte("x"), t3.UnixNano(), 0) // consumer advances
	if err := v.fetchStat("/a"); err != nil {
		t.Fatalf("fetchStat = %v", err)
	}
	if st, _ := v.getattr("/a"); !st.mtime.Equal(t3) {
		t.Fatalf("served mtime after consumer advance = %v, want %v", st.mtime, t3)
	}
}

// TestTreeViewPinsAttrsWhileOpen pins W2's open rule: while a handle is open,
// a landed refresh must not change the path attrs (no invalidation under an
// open file); the change surfaces after the last release. A second open takes
// the CURRENT cache truth — never the elder pin — and moves the pin forward.
func TestTreeViewPinsAttrsWhileOpen(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a", []byte("v1 render"), time.Now().UnixNano(), 0)
	v := newTestView(t, f)

	fh, rc := v.open("/a", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("open = %d", rc)
	}
	pinned, _ := v.getattr("/a")
	if pinned.size != 9 {
		t.Fatalf("pinned size = %d, want 9", pinned.size)
	}

	// The consumer commits a longer render and the background refresh lands.
	f.put("/a", []byte("version-2!"), time.Now().UnixNano(), 0)
	if err := v.fetchStat("/a"); err != nil {
		t.Fatalf("fetchStat = %v", err)
	}
	if st, _ := v.getattr("/a"); st.size != 9 || !st.mtime.Equal(pinned.mtime) {
		t.Fatalf("attrs changed under an open handle: size %d mtime %v; want pinned 9 / %v", st.size, st.mtime, pinned.mtime)
	}

	// A second open snapshots current truth (10) and the pin follows it.
	fh2, rc := v.open("/a", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("second open = %d", rc)
	}
	if st, _ := v.getattr("/a"); st.size != 10 {
		t.Fatalf("pin after newer open = %d, want 10 (newest open wins)", st.size)
	}
	if rc := v.release(fh2); rc != 0 {
		t.Fatalf("release(fh2) = %d", rc)
	}
	if st, _ := v.getattr("/a"); st.size != 10 {
		t.Fatalf("pin after newer close = %d, want 10 (never retreats)", st.size)
	}
	if rc := v.release(fh); rc != 0 {
		t.Fatalf("release(fh) = %d", rc)
	}
	if st, _ := v.getattr("/a"); st.size != 10 {
		t.Fatalf("size after last close = %d, want 10 (refresh surfaces)", st.size)
	}
}

// TestTreeViewOpenSnapshotSizeAuthoritative pins the open-coherence rule for
// token handles: the served size (handle Getattr AND the open's path pin) is
// the OPEN REPLY's snapshot size, never the serve-stale stat cache's. A
// consumer commit landing between the last stat refresh and the open (routine
// for a CRDT-merging consumer; the stale window is unbounded after idle) must
// not cap reads at the wrong length — a grown snapshot read torn to the old
// length, a shrunk one zero-padded. The snapshot's mtime rolls the served
// high-water mark forward with it.
func TestTreeViewOpenSnapshotSizeAuthoritative(t *testing.T) {
	shrinkTreeWaits(t, 2*time.Second, time.Hour) // a warm cache never refreshes on its own
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	cases := []struct {
		name string
		v2   string
	}{
		{"snapshot grew past the cached size", "version-2 grew!"},
		{"snapshot shrank under the cached size", "v2!"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newTreeFakeH()
			f.put("/a.md", []byte("v1 render"), t1.UnixNano(), 0)
			v := newTestView(t, f)
			if st, rc := v.getattr("/a.md"); rc != 0 || st.size != 9 {
				t.Fatalf("warming getattr = (size %d, rc %d), want 9", st.size, rc)
			}

			// The consumer commits between the stat refresh and the open; the
			// cache still says 9 when the open acquires its token.
			f.put("/a.md", []byte(tc.v2), t2.UnixNano(), 0)
			fh, rc := v.open("/a.md", syscall.O_RDONLY)
			if rc != 0 {
				t.Fatalf("open = %d", rc)
			}
			want := int64(len(tc.v2))
			if st, rc := v.getattrHandle(fh); rc != 0 || st.size != want || !st.mtime.Equal(t2) {
				t.Fatalf("getattrHandle = (size %d, mtime %v, rc %d), want the snapshot's (%d, %v)",
					st.size, st.mtime, rc, want, t2)
			}
			if st, _ := v.getattr("/a.md"); st.size != want || !st.mtime.Equal(t2) {
				t.Fatalf("pinned path stat = (size %d, mtime %v), want the snapshot's (%d, %v)",
					st.size, st.mtime, want, t2)
			}
			// The bytes match the served size exactly: a kernel capping reads
			// at the served size reads the whole snapshot, nothing torn.
			buf := make([]byte, want)
			if n := v.read(fh, buf, 0); int64(n) != want || string(buf[:max(n, 0)]) != tc.v2 {
				t.Fatalf("read = (%d, %q), want the full snapshot %q", n, buf[:max(n, 0)], tc.v2)
			}
			if n := v.read(fh, make([]byte, 8), want); n != 0 {
				t.Fatalf("read past the snapshot = %d, want 0", n)
			}
			if rc := v.release(fh); rc != 0 {
				t.Fatalf("release = %d", rc)
			}
		})
	}
}

// TestTreeViewStaleFetchNeverFlapsSize pins the gen guard — W2's "no size
// flap" rule for tree mode: a Stat whose reply was computed BEFORE a local
// mutation landed must be discarded when it arrives after, so the served size
// (and its NFS invalidation) can never flap back to stale consumer truth.
func TestTreeViewStaleFetchNeverFlapsSize(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a", []byte("aa"), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), 0)
	v := newTestView(t, f)
	if _, rc := v.getattr("/a"); rc != 0 {
		t.Fatalf("warming getattr = %d", rc)
	}

	// Park a refresh with its stale reply (size 2) already computed.
	gate := make(chan struct{})
	f.setStatExitBlock(gate)
	fetchDone := make(chan error, 1)
	go func() { fetchDone <- v.fetchStat("/a") }()
	deadline := time.Now().Add(2 * time.Second)
	for f.parkedStats() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the refresh never parked at the consumer's exit gate")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Newer local truth lands: size 5, gen bumped.
	f.setStatExitBlock(nil) // the mutation's own Stat traffic must not park
	if rc := v.truncatePath("/a", 5); rc != 0 {
		t.Fatalf("truncatePath = %d", rc)
	}
	st, _ := v.getattr("/a")
	if st.size != 5 {
		t.Fatalf("size after local truncate = %d, want 5", st.size)
	}

	// The stale reply arrives — and must be discarded.
	close(gate)
	if err := <-fetchDone; err != nil {
		t.Fatalf("fetchStat = %v", err)
	}
	after, _ := v.getattr("/a")
	if after.size != 5 {
		t.Fatalf("served size after the stale fetch landed = %d, want 5 (gen guard)", after.size)
	}
	if after.mtime.Before(st.mtime) {
		t.Fatalf("served mtime regressed: %v -> %v", st.mtime, after.mtime)
	}
}

// TestTreeViewServesStaleOffHandler pins the serve-stale rule: a warm node
// answers immediately from cache while the consumer hangs, and the refresh
// runs off the caller's path.
func TestTreeViewServesStaleOffHandler(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a", []byte("warm"), time.Now().UnixNano(), 0)
	v := newTestView(t, f)
	if _, rc := v.getattr("/a"); rc != 0 {
		t.Fatalf("warming getattr = %d", rc)
	}

	shrinkTreeWaits(t, 2*time.Second, time.Nanosecond) // everything reads stale
	hang := make(chan struct{})
	f.setBlock(hang)

	done := make(chan treeStat, 1)
	go func() {
		st, _ := v.getattr("/a")
		done <- st
	}()
	select {
	case st := <-done:
		if st.size != 4 {
			t.Fatalf("stale getattr size = %d, want last-good 4", st.size)
		}
	case <-time.After(time.Second):
		t.Fatal("getattr blocked on the hung consumer — stat is NOT off-handler")
	}

	// Release the parked refresh; the consumer's answer eventually lands.
	f.put("/a", []byte("warm+1"), time.Now().UnixNano(), 0)
	close(hang)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st, _ := v.getattr("/a"); st.size == 6 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("refresh never landed after the consumer unblocked")
}

// TestTreeViewColdMissBounded pins the first-touch bound: an uncached path
// against a hung consumer fails EIO within treeOpWait instead of parking the
// caller, and the detached fetch still warms the cache for the retry.
func TestTreeViewColdMissBounded(t *testing.T) {
	f := newTreeFakeH()
	f.put("/cold", []byte("xx"), time.Now().UnixNano(), 0)
	v := newTestView(t, f)

	shrinkTreeWaits(t, 50*time.Millisecond, 2*time.Second)
	hang := make(chan struct{})
	f.setBlock(hang)

	start := time.Now()
	_, rc := v.getattr("/cold")
	if rc != -int(syscall.EIO) {
		t.Fatalf("cold getattr against a hung consumer = %d, want EIO", rc)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("cold getattr took %v; the wait must be bounded near treeOpWait", elapsed)
	}

	f.setBlock(nil)
	close(hang) // the detached fetch completes and stores
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st, rc := v.getattr("/cold"); rc == 0 && st.size == 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the detached fetch never warmed the cache for the retry")
}

// TestTreeViewListSeedsChildStats pins the READDIRPLUS warm-up: one List
// seeds every child's stat, so the per-name Getattrs that follow a readdir
// cost zero bridge RPCs.
func TestTreeViewListSeedsChildStats(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a", []byte("aaa"), time.Now().UnixNano(), 7)
	v := newTestView(t, f)

	if _, rc := v.readdir("/"); rc != 0 {
		t.Fatalf("readdir = %d", rc)
	}
	st, rc := v.getattr("/a")
	if rc != 0 || st.size != 3 {
		t.Fatalf("getattr(/a) = (%+v, %d), want the seeded size 3", st, rc)
	}
	if n := f.count("stat:/a"); n != 0 {
		t.Fatalf("consumer saw %d stats for a list-seeded child, want 0", n)
	}
}

// TestTreeViewBridgeOpRouting is the tree-mode parallel of TestRealRouting:
// every core op crosses the bridge as EXACTLY its own op — nothing extra,
// nothing path-wise for a token handle, nothing token-wise for a tokenless
// one. Each step asserts the full op-count delta, so a stray companion RPC
// fails the step that produced it.
func TestTreeViewBridgeOpRouting(t *testing.T) {
	shrinkTreeWaits(t, 2*time.Second, time.Hour) // no mid-table freshness expiry

	t.Run("path-wise over a tokenless WritableTree", func(t *testing.T) {
		f := newTreeFakeRW()
		f.put("/g", []byte("gg"), 0, 0)
		f.put("/w", []byte("ww"), 0, 0)
		f.put("/t", []byte("tt"), 0, 0)
		f.put("/u", []byte("uu"), 0, 0)
		f.put("/r1", []byte("rr"), 0, 0)
		f.mu.Lock()
		f.links["/lnk"] = "/abs"
		f.mu.Unlock()
		v := newTestView(t, f)

		var fhW, fhC uint64
		steps := []struct {
			name string
			op   func() int
			want map[string]int
		}{
			{"getattr routes stat", func() int { _, rc := v.getattr("/g"); return rc },
				map[string]int{"stat:/g": 1}},
			{"readlink routes readlink", func() int { _, rc := v.readlink("/lnk"); return rc },
				map[string]int{"readlink:/lnk": 1}},
			{"readdir routes list", func() int { _, rc := v.readdir("/"); return rc },
				map[string]int{"list:/": 1}},
			{"tokenless open routes a snapshot readat, never open", func() int {
				var rc int
				fhW, rc = v.open("/w", syscall.O_RDWR)
				return rc
			}, map[string]int{"readat:/w": 1}},
			{"write routes writeat", func() int { return errnoOnly(v.write(fhW, []byte("xy"), 0)) },
				map[string]int{"writeat:/w": 1}},
			{"handle truncate routes truncate", func() int { return v.truncateHandle(fhW, 1) },
				map[string]int{"truncate:/w": 1}},
			{"path truncate routes truncate", func() int { return v.truncatePath("/t", 1) },
				map[string]int{"truncate:/t": 1}},
			{"create routes create plus its snapshot readat", func() int {
				var rc int
				fhC, rc = v.create("/c")
				return rc
			}, map[string]int{"create:/c": 1, "readat:/c": 1}},
			{"unlink routes unlink", func() int { return v.unlink("/u") },
				map[string]int{"unlink:/u": 1}},
			{"rename routes rename", func() int { return v.rename("/r1", "/r2") },
				map[string]int{"rename:/r1": 1}},
			{"mkdir routes mkdir", func() int { return v.mkdir("/m") },
				map[string]int{"mkdir:/m": 1}},
		}
		for _, tc := range steps {
			before := f.snapshot()
			if rc := tc.op(); rc != 0 {
				t.Fatalf("%s: rc = %d", tc.name, rc)
			}
			if got := countsDelta(before, f.snapshot()); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("%s: op delta = %v, want %v", tc.name, got, tc.want)
			}
		}
		// A tokenless release sends NO release op — only the post-close
		// refresh, which lands off the caller's path.
		before := f.snapshot()
		if rc := v.release(fhW); rc != 0 {
			t.Fatalf("release = %d", rc)
		}
		waitDelta(t, f.treeFake, before, map[string]int{"stat:/w": 1})
		if rc := v.release(fhC); rc != 0 {
			t.Fatalf("release(create fh) = %d", rc)
		}
	})

	t.Run("token ops over a HandleTree", func(t *testing.T) {
		f := newTreeFakeH()
		f.put("/x", []byte("xx"), 0, 0)
		v := newTestView(t, f)
		if _, rc := v.getattr("/x"); rc != 0 { // warm the stat outside the table
			t.Fatalf("warming getattr = %d", rc)
		}

		var fh uint64
		buf := make([]byte, 4)
		steps := []struct {
			name string
			op   func() int
			want map[string]int
		}{
			{"open routes open", func() int {
				var rc int
				fh, rc = v.open("/x", syscall.O_RDWR)
				return rc
			}, map[string]int{"open:/x": 1}},
			{"read routes the token readat", func() int { return errnoOnly(v.read(fh, buf, 0)) },
				map[string]int{"readat-h:/x": 1}},
			{"write routes the token writeat", func() int { return errnoOnly(v.write(fh, []byte("yy"), 0)) },
				map[string]int{"writeat-h:/x": 1}},
			{"truncate routes the token truncate", func() int { return v.truncateHandle(fh, 1) },
				map[string]int{"truncate-h:/x": 1}},
			{"flush routes flush", func() int { return v.flush(fh) },
				map[string]int{"flush:/x": 1}},
			{"a clean flush routes nothing", func() int { return v.flush(fh) },
				map[string]int{}},
		}
		for _, tc := range steps {
			before := f.snapshot()
			if rc := tc.op(); rc != 0 {
				t.Fatalf("%s: rc = %d", tc.name, rc)
			}
			if got := countsDelta(before, f.snapshot()); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("%s: op delta = %v, want %v", tc.name, got, tc.want)
			}
		}
		// Release drops the token AND schedules the post-close refresh.
		before := f.snapshot()
		if rc := v.release(fh); rc != 0 {
			t.Fatalf("release = %d", rc)
		}
		waitDelta(t, f.treeFake, before, map[string]int{"release:/x": 1, "stat:/x": 1})
	})
}

// errnoOnly maps a read/write result to the errno-returning shape the routing
// table asserts on: a byte count reads as success, a negative errno passes
// through.
func errnoOnly(n int) int {
	if n < 0 {
		return n
	}
	return 0
}

// TestTreeViewAppleDoubleBlocked pins W1b for every tree op: "._" basenames
// answer EACCES on creating ops and ENOENT everywhere else, at any depth,
// without a single bridge RPC; consumer-listed "._" entries never list; and
// near-miss names (".foo", "..data", "x._y") pass untouched.
func TestTreeViewAppleDoubleBlocked(t *testing.T) {
	f := newTreeFakeH()
	f.put("/.foo", []byte("1"), 0, 0)
	f.put("/..data", []byte("2"), 0, 0)
	f.put("/x._y", []byte("3"), 0, 0)
	f.put("/._litter", []byte("4"), 0, 0) // consumer-side litter must never serve
	v := newTestView(t, f)

	eacces := -int(syscall.EACCES)
	cases := []struct {
		name string
		op   func() int
		want int
	}{
		{"getattr", func() int { _, rc := v.getattr("/._x"); return rc }, enoent()},
		{"getattr nested", func() int { _, rc := v.getattr("/d/._x"); return rc }, enoent()},
		{"open read", func() int { _, rc := v.open("/._x", syscall.O_RDONLY); return rc }, enoent()},
		{"open create", func() int { _, rc := v.open("/._x", syscall.O_CREAT|syscall.O_WRONLY); return rc }, eacces},
		{"create", func() int { _, rc := v.create("/._x"); return rc }, eacces},
		{"mkdir", func() int { return v.mkdir("/._x") }, eacces},
		{"unlink", func() int { return v.unlink("/._litter") }, enoent()},
		{"rename source", func() int { return v.rename("/._x", "/y") }, enoent()},
		{"rename dest", func() int { return v.rename("/x._y", "/._x") }, eacces},
		{"readlink", func() int { _, rc := v.readlink("/._x"); return rc }, enoent()},
		{"truncate path", func() int { return v.truncatePath("/._x", 0) }, enoent()},
		{"utimens", func() int { return v.utimens("/._x") }, enoent()},
		{"chmod", func() int { return v.chmod("/._x") }, enoent()},
		{"readdir", func() int { _, rc := v.readdir("/._d"); return rc }, enoent()},
		{"opendir", func() int { return v.opendir("/._d") }, enoent()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.op(); got != tc.want {
				t.Errorf("errno = %d, want %d", got, tc.want)
			}
		})
	}

	// No blocked name ever crossed the bridge.
	for _, key := range []string{"stat:/._x", "open:/._x", "create:/._x", "unlink:/._litter"} {
		if n := f.count(key); n != 0 {
			t.Errorf("consumer saw %d %s RPCs; AppleDouble blocking must precede the bridge", n, key)
		}
	}

	ents, rc := v.readdir("/")
	if rc != 0 {
		t.Fatalf("readdir(/) = %d", rc)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.name)
	}
	sort.Strings(names)
	want := []string{"..data", ".foo", "x._y"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("readdir names = %v, want %v (._litter hidden, near-misses served)", names, want)
	}
	for _, name := range want {
		if _, rc := v.getattr("/" + name); rc != 0 {
			t.Errorf("getattr(/%s) = %d, want 0 (not an AppleDouble name)", name, rc)
		}
	}
}

// TestTreeViewWritesEROFSOnReadOnlyTree pins the honest read-only verdict:
// every mutation against a Tree-only consumer answers EROFS — a capability
// verdict, never EIO noise or a hang.
func TestTreeViewWritesEROFSOnReadOnlyTree(t *testing.T) {
	f := newTreeFake()
	f.put("/a", []byte("ro"), time.Now().UnixNano(), 0)
	v := newTestView(t, f)

	fh, rc := v.open("/a", syscall.O_RDWR)
	if rc != 0 {
		t.Fatalf("open(/a, O_RDWR) = %d (opens succeed; writes carry the verdict)", rc)
	}
	erofs := -int(syscall.EROFS)
	cases := []struct {
		name string
		op   func() int
	}{
		{"create", func() int { _, rc := v.create("/new"); return rc }},
		{"write", func() int { return v.write(fh, []byte("x"), 0) }},
		{"truncate handle", func() int { return v.truncateHandle(fh, 0) }},
		{"truncate path", func() int { return v.truncatePath("/a", 0) }},
		{"unlink", func() int { return v.unlink("/a") }},
		{"rename", func() int { return v.rename("/a", "/b") }},
		{"mkdir", func() int { return v.mkdir("/sub") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.op(); got != erofs {
				t.Errorf("errno = %d, want EROFS %d", got, erofs)
			}
		})
	}
	// The read side still serves.
	buf := make([]byte, 8)
	if n := v.read(fh, buf, 0); n != 2 || string(buf[:2]) != "ro" {
		t.Fatalf("read = %d %q, want the snapshot \"ro\"", n, buf[:2])
	}
}

// TestTreeViewTokenHandleLifecycle pins token routing end to end: reads and
// writes go through the consumer's token ops (never path-wise), the handle's
// own writes move its attrs, flush commits, and release drops the token.
func TestTreeViewTokenHandleLifecycle(t *testing.T) {
	f := newTreeFakeH()
	f.put("/n.md", []byte("v1 render"), time.Now().UnixNano(), 0)
	v := newTestView(t, f)

	fh, rc := v.open("/n.md", syscall.O_RDWR)
	if rc != 0 {
		t.Fatalf("open = %d", rc)
	}
	if f.tokenCount() != 1 {
		t.Fatalf("consumer tokens = %d, want 1", f.tokenCount())
	}

	// Reads route through the token snapshot, immune to a concurrent commit.
	f.put("/n.md", []byte("external overwrite"), time.Now().UnixNano(), 0)
	buf := make([]byte, 64)
	if n := v.read(fh, buf, 0); n != 9 || string(buf[:n]) != "v1 render" {
		t.Fatalf("token read = %q, want the open-time snapshot", buf[:max(n, 0)])
	}
	if f.count("readat-h:/n.md") == 0 || f.count("readat:/n.md") != 0 {
		t.Fatalf("reads went path-wise (handle=%d path=%d); token handles must never fall back",
			f.count("readat-h:/n.md"), f.count("readat:/n.md"))
	}

	// Writes route through the token buffer and roll the handle attrs forward.
	if n := v.write(fh, []byte("edited-v2!"), 0); n != 10 {
		t.Fatalf("write = %d, want 10", n)
	}
	if f.count("writeat-h:/n.md") != 1 || f.count("writeat:/n.md") != 0 {
		t.Fatalf("writes went path-wise (handle=%d path=%d)", f.count("writeat-h:/n.md"), f.count("writeat:/n.md"))
	}
	if st, rc := v.getattrHandle(fh); rc != 0 || st.size != 10 {
		t.Fatalf("getattrHandle = (%+v, %d), want size 10", st, rc)
	}
	if st, _ := v.getattr("/n.md"); st.size != 10 {
		t.Fatalf("path getattr while writing = %d, want the writer's 10", st.size)
	}

	// Flush commits; a clean handle does not re-flush.
	if rc := v.flush(fh); rc != 0 {
		t.Fatalf("flush = %d", rc)
	}
	if got := string(f.bytes("/n.md")); got != "edited-v2!" {
		t.Fatalf("consumer bytes after flush = %q, want \"edited-v2!\"", got)
	}
	if rc := v.flush(fh); rc != 0 {
		t.Fatalf("second flush = %d", rc)
	}
	if n := f.count("flush:/n.md"); n != 1 {
		t.Fatalf("consumer saw %d flushes, want 1 (clean handles are not re-flushed)", n)
	}

	if rc := v.release(fh); rc != 0 {
		t.Fatalf("release = %d", rc)
	}
	if f.tokenCount() != 0 {
		t.Fatalf("consumer tokens after release = %d, want 0", f.tokenCount())
	}
	if rc := v.read(fh, buf, 0); rc != -int(syscall.EBADF) {
		t.Fatalf("read after release = %d, want EBADF", rc)
	}
}

// TestTreeViewOpenTruncFailureReleasesToken pins the holder-side failure path
// after token acquisition: when the O_TRUNC leg of an open fails, the open
// fails with the consumer's verdict AND the just-acquired token is released —
// a failed open must never leak a token or its attr pin.
func TestTreeViewOpenTruncFailureReleasesToken(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a", []byte("aaaa"), time.Now().UnixNano(), 0)
	f.truncErr = cerr{"immutable note", content.ClassPerm}
	v := newTestView(t, f)

	if _, rc := v.open("/a", syscall.O_WRONLY|syscall.O_TRUNC); rc != -int(syscall.EPERM) {
		t.Fatalf("open(O_TRUNC) with a refusing consumer = %d, want EPERM", rc)
	}
	if n := f.tokenCount(); n != 0 {
		t.Fatalf("consumer tokens after the failed open = %d, want 0 (released on the failure path)", n)
	}
	// The failed open's pin is gone too: a consumer-side change surfaces. A
	// leaked pin would serve the pinned 4 forever; polling rides out the failed
	// open's own detached post-close refresh.
	f.put("/a", []byte("aaaaaa"), time.Now().UnixNano(), 0)
	waitServedSize(t, v, "/a", 6)

	// The success path holds exactly one live token and a zeroed handle.
	f.mu.Lock()
	f.truncErr = nil
	f.mu.Unlock()
	fh, rc := v.open("/a", syscall.O_WRONLY|syscall.O_TRUNC)
	if rc != 0 {
		t.Fatalf("open(O_TRUNC) = %d", rc)
	}
	if st, rc := v.getattrHandle(fh); rc != 0 || st.size != 0 {
		t.Fatalf("handle stat after O_TRUNC = (%+v, %d), want size 0", st, rc)
	}
	if n := f.tokenCount(); n != 1 {
		t.Fatalf("consumer tokens after the successful open = %d, want 1", n)
	}
	if rc := v.release(fh); rc != 0 {
		t.Fatalf("release = %d", rc)
	}
}

// TestTreeViewReleaseToleratesDeadToken pins release against a token that died
// consumer-side (another generation's ReleaseAllHandles sweep): the fs-level
// release still succeeds — the consumer's error is logged, never surfaced (the
// kernel discards Release status) — the handle closes, and the pin lifts.
func TestTreeViewReleaseToleratesDeadToken(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a", []byte("aa"), time.Now().UnixNano(), 0)
	v := newTestView(t, f)

	fh, rc := v.open("/a", syscall.O_RDWR)
	if rc != 0 {
		t.Fatalf("open = %d", rc)
	}
	if err := f.ReleaseAllHandles(""); err != nil { // the token dies out from under the holder
		t.Fatal(err)
	}
	if rc := v.release(fh); rc != 0 {
		t.Fatalf("release with a dead consumer token = %d, want 0", rc)
	}
	buf := make([]byte, 4)
	if rc := v.read(fh, buf, 0); rc != -int(syscall.EBADF) {
		t.Fatalf("read after release = %d, want EBADF", rc)
	}
	// The pin lifted with the close: consumer growth surfaces on refresh. A
	// wedged pin would serve 2 forever; polling rides out the release's own
	// detached refresh.
	f.put("/a", []byte("aaaa"), time.Now().UnixNano(), 0)
	waitServedSize(t, v, "/a", 4)
}

// waitServedSize re-fetches p until its served size reaches want — for
// assertions that race a release's detached post-close refresh. A pinned or
// wedged node never gets there, so the property under test still fails loud.
func waitServedSize(t *testing.T, v *treeView, p string, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := v.fetchStat(p); err != nil {
			t.Fatalf("fetchStat(%s) = %v", p, err)
		}
		st, rc := v.getattr(p)
		if rc == 0 && st.size == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("served size of %s = %d (rc %d), want %d", p, st.size, rc, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestTreeViewFlushVerdictPropagates pins the commit-verdict path: a rejected
// save surfaces as the flush errno and the rejected buffer must not commit.
func TestTreeViewFlushVerdictPropagates(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a.md", []byte("good"), time.Now().UnixNano(), 0)
	f.flushErr = cerr{"unparseable note", content.ClassInvalid}
	v := newTestView(t, f)

	fh, rc := v.open("/a.md", syscall.O_WRONLY)
	if rc != 0 {
		t.Fatalf("open = %d", rc)
	}
	if n := v.write(fh, []byte("bad!"), 0); n != 4 {
		t.Fatalf("write = %d", n)
	}
	if rc := v.flush(fh); rc != -int(syscall.EINVAL) {
		t.Fatalf("flush of a rejected save = %d, want EINVAL", rc)
	}
	if got := string(f.bytes("/a.md")); got != "good" {
		t.Fatalf("consumer bytes after rejected flush = %q; the buffer must not commit", got)
	}
}

// TestTreeViewTokenlessSnapshot pins the plain-Tree fallback selected ONLY by
// the IsUnsupported capability verdict: the open captures a local snapshot
// (reads never tear across a consumer commit), writes forward path-wise, and
// the handle reads its own writes.
func TestTreeViewTokenlessSnapshot(t *testing.T) {
	f := newTreeFakeRW()
	f.put("/a", []byte("snapshot"), time.Now().UnixNano(), 0)
	v := newTestView(t, f)

	fh, rc := v.open("/a", syscall.O_RDWR)
	if rc != 0 {
		t.Fatalf("open = %d", rc)
	}
	// The OpenHandle capability miss is the bridge's verdict (the consumer has
	// no handle surface to call); the snapshot arrives via path-wise ReadAt.
	if f.count("readat:/a") == 0 {
		t.Fatal("tokenless open fetched no snapshot over path ReadAt")
	}

	f.put("/a", []byte("external!"), time.Now().UnixNano(), 0)
	buf := make([]byte, 64)
	if n := v.read(fh, buf, 0); n != 8 || string(buf[:n]) != "snapshot" {
		t.Fatalf("read = %q, want the open-time snapshot", buf[:max(n, 0)])
	}

	if n := v.write(fh, []byte("SNAP"), 0); n != 4 {
		t.Fatalf("write = %d", n)
	}
	if f.count("writeat:/a") != 1 {
		t.Fatalf("consumer saw %d path writes, want 1", f.count("writeat:/a"))
	}
	if got := string(f.bytes("/a")); got != "SNAPrnal!" {
		t.Fatalf("consumer bytes = %q, want the path-wise write applied over \"external!\"", got)
	}
	if n := v.read(fh, buf, 0); n != 8 || string(buf[:n]) != "SNAPshot" {
		t.Fatalf("read-your-writes = %q, want \"SNAPshot\"", buf[:max(n, 0)])
	}
	if rc := v.truncateHandle(fh, 4); rc != 0 {
		t.Fatalf("truncate = %d", rc)
	}
	if st, _ := v.getattrHandle(fh); st.size != 4 {
		t.Fatalf("size after truncate = %d, want 4", st.size)
	}
	if rc := v.release(fh); rc != 0 {
		t.Fatalf("release = %d", rc)
	}
}

// TestTreeViewTokenlessConcurrentReadWriteNoTear pins the hmu rule on the
// tokenless snapshot buffer: the read-side copy runs under the handle lock, so
// a concurrent write or truncate — which mutates the SAME backing array in
// place (completeWrite's bufWriteAt/bufResize) — can never tear a read.
// Concurrent READ+WRITE on one open file is fuse-t's nominal dispatch
// (writeback and readahead ride separate workers), not an edge case. Every
// write below fills the whole buffer with one byte and truncate only empties
// it, so ANY non-uniform read is a torn copy; CI's -race leg enforces the
// stronger no-unlocked-access guarantee on the same interleaving.
func TestTreeViewTokenlessConcurrentReadWriteNoTear(t *testing.T) {
	const size = 32 << 10
	f := newTreeFakeRW()
	f.put("/a", bytes.Repeat([]byte{'a'}, size), 0, 0)
	v := newTestView(t, f)

	fh, rc := v.open("/a", syscall.O_RDWR)
	if rc != 0 {
		t.Fatalf("open = %d", rc)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 40; i++ {
			fill := byte('b' + i%2)
			if n := v.write(fh, bytes.Repeat([]byte{fill}, size), 0); n != size {
				t.Errorf("write %d = %d, want %d", i, n, size)
				return
			}
			if rc := v.truncateHandle(fh, 0); rc != 0 {
				t.Errorf("truncate %d = %d", i, rc)
				return
			}
		}
	}()

	buf := make([]byte, size)
	for reads := 0; ; reads++ {
		n := v.read(fh, buf, 0)
		if n < 0 {
			t.Fatalf("read %d = %d", reads, n)
		}
		for i := 1; i < n; i++ {
			if buf[i] != buf[0] {
				t.Fatalf("torn read (iteration %d): buf[%d] = %q, buf[0] = %q — the snapshot copy raced a writer",
					reads, i, buf[i], buf[0])
			}
		}
		select {
		case <-done:
			if rc := v.release(fh); rc != 0 {
				t.Fatalf("release = %d", rc)
			}
			return
		default:
		}
	}
}

// TestTreeViewMutationsMaintainNamespace pins that local mutations update the
// served namespace immediately — no window where a created file is missing
// from its parent or an unlinked one still resolves.
func TestTreeViewMutationsMaintainNamespace(t *testing.T) {
	f := newTreeFakeH()
	v := newTestView(t, f)

	fh, rc := v.create("/new.md")
	if rc != 0 {
		t.Fatalf("create = %d", rc)
	}
	if st, rc := v.getattr("/new.md"); rc != 0 || st.size != 0 {
		t.Fatalf("getattr(new) = (%+v, %d), want an empty file", st, rc)
	}
	ents, rc := v.readdir("/")
	if rc != 0 || len(ents) != 1 || ents[0].name != "new.md" {
		t.Fatalf("readdir after create = %+v (%d), want [new.md]", ents, rc)
	}
	if n := v.write(fh, []byte("hello"), 0); n != 5 {
		t.Fatalf("write = %d", n)
	}
	if rc := v.flush(fh); rc != 0 {
		t.Fatalf("flush = %d", rc)
	}
	if rc := v.release(fh); rc != 0 {
		t.Fatalf("release = %d", rc)
	}
	if st, _ := v.getattr("/new.md"); st.size != 5 {
		t.Fatalf("size after committed write = %d, want 5", st.size)
	}

	if rc := v.mkdir("/sub"); rc != 0 {
		t.Fatalf("mkdir = %d", rc)
	}
	if st, rc := v.getattr("/sub"); rc != 0 || st.kind != content.EntryDir {
		t.Fatalf("getattr(/sub) = (%+v, %d), want a dir", st, rc)
	}
	if rc := v.opendir("/sub"); rc != 0 {
		t.Fatalf("opendir(/sub) = %d", rc)
	}
	if rc := v.opendir("/new.md"); rc != -int(syscall.ENOTDIR) {
		t.Fatalf("opendir(file) = %d, want ENOTDIR", rc)
	}
	if ents, rc := v.readdir("/sub"); rc != 0 || len(ents) != 0 {
		t.Fatalf("readdir(/sub) = %+v (%d), want empty", ents, rc)
	}

	if rc := v.unlink("/new.md"); rc != 0 {
		t.Fatalf("unlink = %d", rc)
	}
	if _, rc := v.getattr("/new.md"); rc != enoent() {
		t.Fatalf("getattr after unlink = %d, want ENOENT", rc)
	}
	ents, rc = v.readdir("/")
	if rc != 0 {
		t.Fatalf("readdir = %d", rc)
	}
	for _, e := range ents {
		if e.name == "new.md" {
			t.Fatal("unlinked entry still listed")
		}
	}
}

// TestTreeViewNotFoundVerdictCached pins negative caching: a consumer ENOENT
// is a verdict served from cache within the freshness window, not an RPC per
// Getattr poll.
func TestTreeViewNotFoundVerdictCached(t *testing.T) {
	f := newTreeFakeH()
	v := newTestView(t, f)
	for i := 0; i < 3; i++ {
		if _, rc := v.getattr("/ghost"); rc != enoent() {
			t.Fatalf("getattr(/ghost) = %d, want ENOENT", rc)
		}
	}
	if n := f.count("stat:/ghost"); n != 1 {
		t.Fatalf("consumer saw %d stats for a cached ENOENT verdict, want 1", n)
	}
}

// TestTreeViewReadlink pins the symlink path: targets serve from the bridge
// (or the Stat's Target seed) and non-links answer their consumer class.
func TestTreeViewReadlink(t *testing.T) {
	f := newTreeFakeH()
	f.mu.Lock()
	f.links["/lnk"] = "/abs/target"
	f.mu.Unlock()
	f.put("/plain", []byte("x"), 0, 0)
	v := newTestView(t, f)

	target, rc := v.readlink("/lnk")
	if rc != 0 || target != "/abs/target" {
		t.Fatalf("readlink = (%q, %d), want /abs/target", target, rc)
	}
	if st, rc := v.getattr("/lnk"); rc != 0 || st.kind != content.EntrySymlink {
		t.Fatalf("getattr(/lnk) = (%+v, %d), want a symlink", st, rc)
	}
	if _, rc := v.readlink("/plain"); rc != -int(syscall.EINVAL) {
		t.Fatalf("readlink(plain file) = %d, want EINVAL", rc)
	}
}

// TestTreeViewSweepHandles pins the crash-recovery sweep: a HandleTree
// consumer's stale tokens die, a plain-Tree consumer's capability miss is
// tolerated, and a real failure is loud.
func TestTreeViewSweepHandles(t *testing.T) {
	t.Run("drops stale tokens", func(t *testing.T) {
		f := newTreeFakeH()
		f.put("/a", []byte("x"), 0, 0)
		v := newTestView(t, f)
		if _, _, err := f.OpenHandle("d", "/a"); err != nil { // a prior generation's leak
			t.Fatal(err)
		}
		if err := v.sweepHandles(); err != nil {
			t.Fatalf("sweepHandles = %v", err)
		}
		if f.tokenCount() != 0 {
			t.Fatalf("tokens after sweep = %d, want 0", f.tokenCount())
		}
	})
	t.Run("tolerates a tokenless consumer", func(t *testing.T) {
		v := newTestView(t, newTreeFakeRW())
		if err := v.sweepHandles(); err != nil {
			t.Fatalf("sweepHandles on a plain WritableTree = %v, want nil (capability miss)", err)
		}
	})
	t.Run("unreachable bridge is loud", func(t *testing.T) {
		v := newTreeView("d", deadClient(t))
		if err := v.sweepHandles(); err == nil {
			t.Fatal("sweepHandles over a dead socket = nil, want an error")
		}
	})
}

// TestTreeViewPrewarmRoot pins Build's fail-loud contract: a reachable Tree
// consumer pre-warms the root listing; an unreachable bridge and a Tree-less
// consumer both refuse the mount instead of serving an empty tree.
func TestTreeViewPrewarmRoot(t *testing.T) {
	t.Run("warms the root listing", func(t *testing.T) {
		f := newTreeFakeH()
		f.put("/a", []byte("x"), 0, 0)
		v := newTestView(t, f)
		if err := v.prewarmRoot(); err != nil {
			t.Fatalf("prewarmRoot = %v", err)
		}
		if ents, rc := v.readdir("/"); rc != 0 || len(ents) != 1 {
			t.Fatalf("readdir after prewarm = %+v (%d)", ents, rc)
		}
		if n := f.count("list:/"); n != 1 {
			t.Fatalf("consumer saw %d root lists, want 1 (prewarm serves the readdir)", n)
		}
	})
	t.Run("unreachable bridge fails loud", func(t *testing.T) {
		v := newTreeView("d", deadClient(t))
		err := v.prewarmRoot()
		if !errors.Is(err, content.ErrBridgeUnavailable) {
			t.Fatalf("prewarmRoot over a dead socket = %v, want ErrBridgeUnavailable", err)
		}
	})
	t.Run("a Source-only consumer fails loud", func(t *testing.T) {
		v := newTestView(t, &fakeContent{})
		err := v.prewarmRoot()
		if err == nil || !strings.Contains(err.Error(), "content.Tree") {
			t.Fatalf("prewarmRoot on a Source-only consumer = %v, want the Tree capability refusal", err)
		}
	})
}

// TestTreeViewFlushWithinDrainsDirty pins teardown draining: dirty token
// handles commit within the grace, clean ones cost no RPC.
func TestTreeViewFlushWithinDrainsDirty(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a", []byte("old"), 0, 0)
	f.put("/b", []byte("clean"), 0, 0)
	v := newTestView(t, f)

	dirtyFh, rc := v.open("/a", syscall.O_WRONLY)
	if rc != 0 {
		t.Fatalf("open(/a) = %d", rc)
	}
	if _, rc := v.open("/b", syscall.O_RDONLY); rc != 0 {
		t.Fatalf("open(/b) = %d", rc)
	}
	if n := v.write(dirtyFh, []byte("new"), 0); n != 3 {
		t.Fatalf("write = %d", n)
	}
	if !v.flushWithin(2 * time.Second) {
		t.Fatal("flushWithin = false, want the dirty commit drained")
	}
	if got := string(f.bytes("/a")); got != "new" {
		t.Fatalf("consumer bytes after drain = %q, want \"new\"", got)
	}
	if n := f.count("flush:/b"); n != 0 {
		t.Fatalf("consumer saw %d flushes for a clean handle, want 0", n)
	}
}
