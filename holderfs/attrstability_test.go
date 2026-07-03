//go:build fuse && cgo && darwin

package holderfs

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
)

// flakySource is a bridge content.Source whose synth reads can be flipped
// between failing and serving fixed bytes, modeling a consumer that is
// reachable (the manifest answers) but cannot compute content yet.
type flakySource struct {
	mu      sync.Mutex
	entries []content.Entry
	bytes   []byte
	fail    bool
}

func (s *flakySource) set(fail bool, bytes string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail, s.bytes = fail, []byte(bytes)
}

func (s *flakySource) Manifest(string) ([]content.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries, nil
}

func (s *flakySource) ReadSynth(string, string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return nil, errors.New("consumer cannot compute content yet")
	}
	return s.bytes, nil
}

func (s *flakySource) WriteThrough(string, string, []byte) error { return nil }
func (s *flakySource) Classify(string) content.EntryKind         { return content.EntrySynth }

// newSynthFS builds a holderFS over real temp dirs with one synth entry ".x"
// whose writePath sits under Base, mimicking Build's wiring (minted ino,
// seeded cache) without a manifest round-trip. seed == "" leaves the writePath
// absent (the deliberate cold case).
func newSynthFS(t *testing.T, fc *fakeContent, seed string, freshness []string) (*holderFS, *synthView, string) {
	t.Helper()
	base, priv := t.TempDir(), t.TempDir()
	writePath := filepath.Join(base, ".x")
	if seed != "" {
		if err := os.WriteFile(writePath, []byte(seed), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), writePath, freshness)
	v.ino = sharedLinkInoBase + 7
	v.seedFromWritePath()
	fs := &holderFS{
		base:         base,
		privateRoot:  priv,
		privateExact: map[string]bool{},
		shared:       map[string]sharedEntry{},
		synth:        map[string]*synthView{"/.x": v},
		synthFhs:     map[uint64]*synthHandle{},
		nextSynthFh:  synthFhBase,
		fileFhs:      map[uint64]string{},
		dirFhs:       map[uint64]struct{}{},
	}
	return fs, v, writePath
}

func getattrPath(t *testing.T, fs *holderFS, path string) fuse.Stat_t {
	t.Helper()
	var st fuse.Stat_t
	if rc := fs.Getattr(path, &st, ^uint64(0)); rc != 0 {
		t.Fatalf("Getattr(%s) = %d, want 0", path, rc)
	}
	return st
}

// readdirRootStats returns the fill stats Readdir("/") produced, keyed by
// name; a nil value records a name filled without a stat.
func readdirRootStats(t *testing.T, fs *holderFS) map[string]*fuse.Stat_t {
	t.Helper()
	stats := map[string]*fuse.Stat_t{}
	rc := fs.Readdir("/", func(name string, st *fuse.Stat_t, _ int64) bool {
		if st != nil {
			c := *st
			stats[name] = &c
		} else {
			stats[name] = nil
		}
		return true
	}, 0, 0)
	if rc != 0 {
		t.Fatalf("Readdir(/) = %d, want 0", rc)
	}
	return stats
}

func tsAfter(a, b fuse.Timespec) bool {
	return a.Sec > b.Sec || (a.Sec == b.Sec && a.Nsec > b.Nsec)
}

// TestBuildMintsInoAndSeedsSynthCache pins Build's W2 wiring end-to-end: every
// synth entry gets a distinct minted ino from the sharedLinkInoBase pool, a
// present writePath seeds the cache (steady size and readable bytes even while
// the consumer cannot answer), the consumer's answer still lands once it can,
// and a synth entry with no writePath stays ENOENT and unlisted.
func TestBuildMintsInoAndSeedsSynthCache(t *testing.T) {
	base, priv, dir := t.TempDir(), t.TempDir(), t.TempDir()
	for name, data := range map[string]string{
		".claude.json": "SEEDED-CLAUDE",
		".seeded.json": "SEEDED!",
	} {
		if err := os.WriteFile(filepath.Join(priv, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	src := &flakySource{
		fail: true,
		entries: []content.Entry{
			{Name: "projects", Kind: content.EntrySymlink, Target: filepath.Join(base, "projects")},
			{Name: ".claude.json", Kind: content.EntrySynth, Private: true},
			{Name: ".seeded.json", Kind: content.EntrySynth, Private: true},
			{Name: "missing.json", Kind: content.EntrySynth},
		},
	}
	cfg, err := Build(fusekit.MountSpec{
		Base: base, Dir: dir, PrivateRoot: priv,
		ContentSocket:   serveContent(t, src),
		Domain:          "d",
		PrivatePrefixes: []string{".claude.json"},
	})
	if err != nil {
		t.Fatal(err)
	}
	fs, ok := cfg.FS.(*holderFS)
	if !ok {
		t.Fatalf("cfg.FS is %T, want *holderFS", cfg.FS)
	}

	wantInos := map[string]uint64{
		"/.claude.json": sharedLinkInoBase + 1, // the symlink took +0
		"/.seeded.json": sharedLinkInoBase + 2,
		"/missing.json": sharedLinkInoBase + 3,
	}
	for path, want := range wantInos {
		v := fs.synth[path]
		if v == nil {
			t.Fatalf("Build produced no synth view for %s", path)
		}
		if v.ino != want {
			t.Errorf("minted ino for %s = %d, want %d", path, v.ino, want)
		}
	}
	if got := fs.shared["projects"].stat.Ino; got != sharedLinkInoBase {
		t.Errorf("shared symlink ino = %d, want %d", got, sharedLinkInoBase)
	}

	// Seeded entries serve their writePath bytes while the consumer fails.
	for path, seed := range map[string]string{"/.claude.json": "SEEDED-CLAUDE", "/.seeded.json": "SEEDED!"} {
		st := getattrPath(t, fs, path)
		if st.Size != int64(len(seed)) {
			t.Errorf("Getattr(%s) size = %d, want seeded %d", path, st.Size, len(seed))
		}
		if st.Ino != wantInos[path] {
			t.Errorf("Getattr(%s) ino = %d, want minted %d", path, st.Ino, wantInos[path])
		}
	}
	rc, fh := fs.Open("/.claude.json", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open(/.claude.json) = %d, want 0 (a seeded cache must serve, not EIO)", rc)
	}
	buf := make([]byte, 64)
	if n := fs.Read("/.claude.json", buf, 0, fh); string(buf[:n]) != "SEEDED-CLAUDE" {
		t.Fatalf("Read = %q, want the seeded bytes", buf[:n])
	}
	fs.Release("/.claude.json", fh)

	// The no-writePath entry stays absent: ENOENT and unlisted.
	var st fuse.Stat_t
	if rc := fs.Getattr("/missing.json", &st, ^uint64(0)); rc != -int(syscall.ENOENT) {
		t.Fatalf("Getattr(/missing.json) = %d, want ENOENT", rc)
	}
	stats := readdirRootStats(t, fs)
	if _, listed := stats["missing.json"]; listed {
		t.Error("Readdir listed missing.json, which has no backing writePath")
	}
	// ".claude.json" flows through the PrivateRoot merge loop (private prefix);
	// ".seeded.json" through the synth loop. Both must carry the minted ino.
	for _, name := range []string{".claude.json", ".seeded.json"} {
		got := stats[name]
		if got == nil {
			t.Fatalf("Readdir did not fill a stat for %s; got %v", name, stats)
		}
		if got.Ino != wantInos["/"+name] {
			t.Errorf("Readdir ino for %s = %d, want minted %d", name, got.Ino, wantInos["/"+name])
		}
	}

	// Seeding must not freeze the cache: once the consumer answers, its bytes win.
	src.set(false, "MERGED-BIGGER-THAN-SEED")
	waitCache(t, fs.synth["/.claude.json"], "MERGED-BIGGER-THAN-SEED")
	if st := getattrPath(t, fs, "/.claude.json"); st.Size != int64(len("MERGED-BIGGER-THAN-SEED")) {
		t.Fatalf("Getattr size after the consumer answered = %d, want %d", st.Size, len("MERGED-BIGGER-THAN-SEED"))
	}
}

// TestSynthInoStableAcrossWriteThroughAndRefresh pins W2's highest-likelihood
// panic delta: the fileid served for a synth entry is the minted synthetic
// ino, constant across the consumer's atomic-rename write-through (which
// re-mints writePath's REAL ino) and across cache refreshes — and identical
// from path Getattr, read-handle Getattr, writable-fd Getattr, and Readdir.
func TestSynthInoStableAcrossWriteThroughAndRefresh(t *testing.T) {
	fc := &fakeContent{readBytes: []byte("v1")}
	fs, v, writePath := newSynthFS(t, fc, "seeded", nil)

	if st := getattrPath(t, fs, "/.x"); st.Ino != v.ino {
		t.Fatalf("path Getattr ino = %d, want minted %d", st.Ino, v.ino)
	}
	var real0 syscall.Stat_t
	if err := syscall.Lstat(writePath, &real0); err != nil {
		t.Fatal(err)
	}
	if v.ino == real0.Ino {
		t.Fatalf("minted ino %d equals writePath's real ino; the real fileid must never serve", v.ino)
	}

	// The consumer's atomic save through the mount: write a temp, rename it over
	// the synth path. writePath's real ino changes; the served one must not.
	rc, wfh := fs.Create("/.x.tmp1", syscall.O_WRONLY, 0o600)
	if rc != 0 {
		t.Fatalf("Create(/.x.tmp1) = %d, want 0", rc)
	}
	if n := fs.Write("/.x.tmp1", []byte("rewritten"), 0, wfh); n != len("rewritten") {
		t.Fatalf("Write = %d, want %d", n, len("rewritten"))
	}
	fs.Release("/.x.tmp1", wfh)
	if rc := fs.Rename("/.x.tmp1", "/.x"); rc != 0 {
		t.Fatalf("Rename = %d, want 0", rc)
	}
	if !v.flushWithin(2 * time.Second) {
		t.Fatal("write-through did not drain")
	}
	var real1 syscall.Stat_t
	if err := syscall.Lstat(writePath, &real1); err != nil {
		t.Fatal(err)
	}
	if real1.Ino == real0.Ino {
		t.Fatal("rename did not change writePath's real ino; the test lost its churn")
	}
	if st := getattrPath(t, fs, "/.x"); st.Ino != v.ino {
		t.Fatalf("path Getattr ino after write-through = %d, want minted %d (real ino churned %d -> %d)",
			st.Ino, v.ino, real0.Ino, real1.Ino)
	}

	// A refresh replacing the cache must not move the ino either.
	fc.mu.Lock()
	fc.readBytes = []byte("v2")
	fc.mu.Unlock()
	waitCache(t, v, "v2")
	if st := getattrPath(t, fs, "/.x"); st.Ino != v.ino {
		t.Fatalf("path Getattr ino after refresh = %d, want minted %d", st.Ino, v.ino)
	}

	// Read-handle Getattr serves the minted ino.
	rc, fh := fs.Open("/.x", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open = %d, want 0", rc)
	}
	var st fuse.Stat_t
	if rc := fs.Getattr("/.x", &st, fh); rc != 0 || st.Ino != v.ino {
		t.Fatalf("read-handle Getattr = rc %d ino %d, want 0/%d", rc, st.Ino, v.ino)
	}
	fs.Release("/.x", fh)

	// A writable open is a real writePath fd; its Getattr must still mask the
	// real ino with the minted one.
	rc, wfh2 := fs.Open("/.x", syscall.O_WRONLY)
	if rc != 0 {
		t.Fatalf("writable Open = %d, want 0", rc)
	}
	if rc := fs.Getattr("/.x", &st, wfh2); rc != 0 || st.Ino != v.ino {
		t.Fatalf("writable-fd Getattr = rc %d ino %d, want 0/%d", rc, st.Ino, v.ino)
	}
	fs.Release("/.x", wfh2)

	// Readdir's fill stat carries the minted ino (base-names loop).
	got := readdirRootStats(t, fs)[".x"]
	if got == nil {
		t.Fatal("Readdir did not fill a stat for the synth entry")
	}
	if got.Ino != v.ino {
		t.Fatalf("Readdir ino = %d, want minted %d", got.Ino, v.ino)
	}
}

// TestSynthSizeNeverFlapsWhileConsumerUnavailable pins the cold→warm size-flap
// fix at the vnop level: with writePath present, the seeded cache serves a
// steady size and readable bytes even while every bridge read hangs — never
// EIO, never the raw-then-cached size discontinuity — and the consumer's
// answer still surfaces once it arrives (no open handles pinning it).
func TestSynthSizeNeverFlapsWhileConsumerUnavailable(t *testing.T) {
	hang := make(chan struct{})
	fc := &fakeContent{readBytes: []byte("MERGED-LONGER"), readBlock: hang}
	fs, v, _ := newSynthFS(t, fc, "COMMITTED", nil)

	seedLen := int64(len("COMMITTED"))
	for i := 0; i < 3; i++ {
		if st := getattrPath(t, fs, "/.x"); st.Size != seedLen {
			t.Fatalf("Getattr #%d size = %d, want steady seeded %d", i, st.Size, seedLen)
		}
	}
	rc, fh := fs.Open("/.x", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open = %d, want 0 (a seeded cache must serve, not EIO)", rc)
	}
	buf := make([]byte, 64)
	if n := fs.Read("/.x", buf, 0, fh); int64(n) != seedLen || string(buf[:n]) != "COMMITTED" {
		t.Fatalf("Read = %d %q, want the seeded bytes", n, buf[:n])
	}
	var st fuse.Stat_t
	if rc := fs.Getattr("/.x", &st, fh); rc != 0 || st.Size != seedLen {
		t.Fatalf("handle Getattr = rc %d size %d, want 0/%d (must match Read)", rc, st.Size, seedLen)
	}
	fs.Release("/.x", fh)

	close(hang) // the consumer comes alive; its answer must land, not freeze out
	waitCache(t, v, "MERGED-LONGER")
	if st := getattrPath(t, fs, "/.x"); st.Size != int64(len("MERGED-LONGER")) {
		t.Fatalf("Getattr size after the consumer answered = %d, want %d", st.Size, len("MERGED-LONGER"))
	}
}

// TestSynthMissingWritePathStaysAbsent pins the deliberate cold case: no
// writePath and no consumer answer means ENOENT on Getattr, EIO on Open (the
// cache has nothing to serve), and no Readdir listing — the entry must not
// flicker into existence.
func TestSynthMissingWritePathStaysAbsent(t *testing.T) {
	hang := make(chan struct{})
	defer close(hang) // drain the parked bridge read before cleanup
	fc := &fakeContent{readBytes: []byte("late"), readBlock: hang}
	fs, _, _ := newSynthFS(t, fc, "", nil)

	var st fuse.Stat_t
	if rc := fs.Getattr("/.x", &st, ^uint64(0)); rc != -int(syscall.ENOENT) {
		t.Fatalf("Getattr = %d, want ENOENT", rc)
	}
	if rc, _ := fs.Open("/.x", syscall.O_RDONLY); rc != -int(syscall.EIO) {
		t.Fatalf("Open = %d, want EIO", rc)
	}
	if _, listed := readdirRootStats(t, fs)[".x"]; listed {
		t.Fatal("Readdir listed a synth entry with no backing writePath")
	}
}

// TestSynthMtimeMonotonicAcrossFreshnessVanish pins the vanished-freshness
// regression at the vnop level: the served Mtim is the freshness high-water
// mark and never rewinds to writePath's older mtime — a rewind reads as a
// change and re-triggers page invalidation. The historical dates also pin the
// first-incarnation contract: no floor, on-disk mtimes serve untouched.
func TestSynthMtimeMonotonicAcrossFreshnessVanish(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	t2, t3 := t1.Add(time.Hour), t1.Add(2*time.Hour)
	fc := &fakeContent{readBytes: []byte("v1")}
	fresh := filepath.Join(t.TempDir(), "fresh")
	fs, _, writePath := newSynthFS(t, fc, "COMMITTED", []string{fresh})

	writeAt := func(p string, at time.Time) {
		t.Helper()
		if err := os.WriteFile(p, []byte("f"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(writePath, t1, t1); err != nil {
		t.Fatal(err)
	}
	writeAt(fresh, t2)

	if got := getattrPath(t, fs, "/.x").Mtim; got != tsOf(t2) {
		t.Fatalf("Mtim = %+v, want the freshness file's %+v", got, tsOf(t2))
	}
	if err := os.Remove(fresh); err != nil {
		t.Fatal(err)
	}
	if got := getattrPath(t, fs, "/.x").Mtim; got != tsOf(t2) {
		t.Fatalf("Mtim after the freshness file vanished = %+v, want the %+v high-water mark", got, tsOf(t2))
	}
	writeAt(fresh, t3)
	if got := getattrPath(t, fs, "/.x").Mtim; got != tsOf(t3) {
		t.Fatalf("Mtim after the freshness file advanced = %+v, want %+v", got, tsOf(t3))
	}
}

// TestSynthAttrsAdvanceAcrossIncarnationsSameSize pins the re-attach coherence
// contract at the vnop level, in the VM-proven failure shape (validate-mux
// fileid-drill cycle 1): the same logical synth entry is rebuilt — a mux
// detach/re-attach constructs a fresh holderFS, and with it fresh synthViews,
// per incarnation — with writePath untouched on disk and the refreshed content
// the SAME byte length. go-nfsv4 reuses path-keyed client fileids across the
// re-attach and mints the NFSv4 change attribute from the served ctime, and the
// macOS NFSv4 client invalidates cached pages only when change moves
// (NFS_CHANGED), so with ino and size identical the served Ctim (and Mtim) must
// advance strictly past everything the previous incarnation served — a repeat
// validates the client's stale pages and the old incarnation's bytes keep
// serving. Within one incarnation the attrs must hold static and agree across
// path Getattr, handle Getattr, and Readdir: churn there is the panic-adjacent
// instability the stabilization forbids.
func TestSynthAttrsAdvanceAcrossIncarnationsSameSize(t *testing.T) {
	base, priv := t.TempDir(), t.TempDir()
	writePath := filepath.Join(base, ".x")
	if err := os.WriteFile(writePath, []byte("committed"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Age the backing mtime: the FIRST incarnation must serve it untouched (no
	// floor), and only the re-attach floor may move the second incarnation's
	// attrs (ctime cannot be backdated; it lands at the WriteFile/Chtimes time,
	// which the second incarnation's floor must clear).
	past := time.Now().Add(-24 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(writePath, past, past); err != nil {
		t.Fatal(err)
	}

	incarnation := func(payload string) *holderFS {
		t.Helper()
		fc := &fakeContent{readBytes: []byte(payload)}
		v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), writePath, nil)
		v.ino = sharedLinkInoBase + 7
		v.seedFromWritePath()
		v.refreshOnce()
		return &holderFS{
			base:         base,
			privateRoot:  priv,
			privateExact: map[string]bool{},
			shared:       map[string]sharedEntry{},
			synth:        map[string]*synthView{"/.x": v},
			synthFhs:     map[uint64]*synthHandle{},
			nextSynthFh:  synthFhBase,
			fileFhs:      map[uint64]string{},
			dirFhs:       map[uint64]struct{}{},
		}
	}
	readAll := func(fs *holderFS) string {
		t.Helper()
		rc, fh := fs.Open("/.x", syscall.O_RDONLY)
		if rc != 0 {
			t.Fatalf("Open = %d, want 0", rc)
		}
		defer fs.Release("/.x", fh)
		buf := make([]byte, 64)
		n := fs.Read("/.x", buf, 0, fh)
		return string(buf[:n])
	}

	fs1 := incarnation("PAYLOAD-A")
	st1 := getattrPath(t, fs1, "/.x")
	if st1.Mtim != tsOf(past) {
		t.Fatalf("incarnation 1 Mtim = %+v, want the genuine on-disk %+v — a first incarnation must never floor a pre-existing file's timestamps", st1.Mtim, tsOf(past))
	}
	if !tsAfter(st1.Ctim, tsOf(past)) {
		t.Fatalf("incarnation 1 Ctim = %+v, want the real (recent) ctime, after the backdated era %+v", st1.Ctim, tsOf(past))
	}
	// Static within the incarnation, and identical across every attr surface.
	for i := 0; i < 3; i++ {
		st := getattrPath(t, fs1, "/.x")
		if st.Ctim != st1.Ctim || st.Mtim != st1.Mtim || st.Ino != st1.Ino {
			t.Fatalf("path Getattr #%d = (ino %d, mtim %+v, ctim %+v), want the stable (ino %d, mtim %+v, ctim %+v)",
				i, st.Ino, st.Mtim, st.Ctim, st1.Ino, st1.Mtim, st1.Ctim)
		}
	}
	rc, fh := fs1.Open("/.x", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open = %d, want 0", rc)
	}
	var hst fuse.Stat_t
	if rc := fs1.Getattr("/.x", &hst, fh); rc != 0 || hst.Ctim != st1.Ctim || hst.Mtim != st1.Mtim {
		t.Fatalf("handle Getattr = rc %d (mtim %+v, ctim %+v), want 0 with the path's (%+v, %+v) — surfaces must never disagree",
			rc, hst.Mtim, hst.Ctim, st1.Mtim, st1.Ctim)
	}
	fs1.Release("/.x", fh)
	if got := readdirRootStats(t, fs1)[".x"]; got == nil || got.Ctim != st1.Ctim {
		t.Fatalf("Readdir stat = %+v, want Ctim %+v matching Getattr", got, st1.Ctim)
	}
	if got := readAll(fs1); got != "PAYLOAD-A" {
		t.Fatalf("incarnation 1 Read = %q, want PAYLOAD-A", got)
	}

	// Deliberately back-to-back — no sleep: a real detach/re-attach can rebuild
	// within one clock quantum, and strict advance must not lean on the wall
	// clock ticking between builds (mintAttrFloor's value-chained per-writePath
	// floor owns the guarantee, clock-free).
	fs2 := incarnation("PAYLOAD-B") // same byte length: only mtime/ctime can signal the change
	st2 := getattrPath(t, fs2, "/.x")
	if st2.Ino != st1.Ino {
		t.Fatalf("ino across incarnations = %d -> %d; the minted fileid must be reproducible", st1.Ino, st2.Ino)
	}
	if st2.Size != st1.Size {
		t.Fatalf("size across incarnations = %d -> %d; the test lost its same-size shape", st1.Size, st2.Size)
	}
	if !tsAfter(st2.Ctim, st1.Ctim) {
		t.Fatalf("Ctim across incarnations = %+v -> %+v, want strictly increasing — go-nfsv4's change attribute rides the served ctime, and a repeat validates the previous incarnation's cached pages",
			st1.Ctim, st2.Ctim)
	}
	if !tsAfter(st2.Mtim, st1.Mtim) {
		t.Fatalf("Mtim across incarnations = %+v -> %+v, want strictly increasing", st1.Mtim, st2.Mtim)
	}
	if got := readAll(fs2); got != "PAYLOAD-B" {
		t.Fatalf("incarnation 2 Read = %q, want the new same-size PAYLOAD-B", got)
	}
}

// TestSynthPathGetattrPinsToNewestOpenHandle pins W2 item 4: while any read
// handle is open, path Getattr serves the newest open's snapshot size and
// mtime — a background refresh must never land an invalidation on a file the
// client holds open or mapped — the pin never retreats when a newer handle
// closes before an elder, each handle's own Getattr keeps matching its Read
// bytes, and the refreshed attrs surface only after the last close.
func TestSynthPathGetattrPinsToNewestOpenHandle(t *testing.T) {
	t1 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	t2, t3 := t1.Add(time.Hour), t1.Add(2*time.Hour)
	fc := &fakeContent{readBytes: []byte("v1")}
	fresh := filepath.Join(t.TempDir(), "fresh")
	fs, v, writePath := newSynthFS(t, fc, "v1", []string{fresh})

	writeAt := func(p string, at time.Time) {
		t.Helper()
		if err := os.WriteFile(p, []byte("f"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(writePath, t1, t1); err != nil {
		t.Fatal(err)
	}
	writeAt(fresh, t2)

	rc, fhA := fs.Open("/.x", syscall.O_RDONLY) // snapshot: "v1", mtime t2
	if rc != 0 {
		t.Fatalf("Open A = %d, want 0", rc)
	}
	sizeA, sizeB := int64(len("v1")), int64(len("v2-longer"))

	// The consumer commits new content while A is open.
	fc.mu.Lock()
	fc.readBytes = []byte("v2-longer")
	fc.mu.Unlock()
	writeAt(fresh, t3)
	waitCache(t, v, "v2-longer")

	if st := getattrPath(t, fs, "/.x"); st.Size != sizeA || st.Mtim != tsOf(t2) {
		t.Fatalf("path Getattr while A open = (size %d, mtim %+v), want pinned (%d, %+v)", st.Size, st.Mtim, sizeA, tsOf(t2))
	}
	var st fuse.Stat_t
	if rc := fs.Getattr("/.x", &st, fhA); rc != 0 || st.Size != sizeA || st.Mtim != tsOf(t2) {
		t.Fatalf("handle A Getattr = rc %d (size %d, mtim %+v), want 0 (%d, %+v)", rc, st.Size, st.Mtim, sizeA, tsOf(t2))
	}
	buf := make([]byte, 64)
	if n := fs.Read("/.x", buf, 0, fhA); string(buf[:n]) != "v1" {
		t.Fatalf("Read A = %q, want its open-time snapshot v1", buf[:n])
	}

	// A newer open snapshots the refreshed cache; path Getattr tracks it, the
	// elder handle keeps its own snapshot (its Getattr must match its Read).
	rc, fhB := fs.Open("/.x", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open B = %d, want 0", rc)
	}
	if st := getattrPath(t, fs, "/.x"); st.Size != sizeB || st.Mtim != tsOf(t3) {
		t.Fatalf("path Getattr with B open = (size %d, mtim %+v), want newest (%d, %+v)", st.Size, st.Mtim, sizeB, tsOf(t3))
	}
	if rc := fs.Getattr("/.x", &st, fhA); rc != 0 || st.Size != sizeA {
		t.Fatalf("handle A Getattr after B opened = rc %d size %d, want 0/%d (own snapshot)", rc, st.Size, sizeA)
	}
	if rc := fs.Getattr("/.x", &st, fhB); rc != 0 || st.Size != sizeB {
		t.Fatalf("handle B Getattr = rc %d size %d, want 0/%d", rc, st.Size, sizeB)
	}
	if n := fs.Read("/.x", buf, 0, fhB); string(buf[:n]) != "v2-longer" {
		t.Fatalf("Read B = %q, want v2-longer", buf[:n])
	}

	// The newer handle closes first: the pin must NOT retreat to A's older
	// snapshot — that would serve a size/mtime regression under a still-open file.
	fs.Release("/.x", fhB)
	if st := getattrPath(t, fs, "/.x"); st.Size != sizeB || st.Mtim != tsOf(t3) {
		t.Fatalf("path Getattr after B closed (A still open) = (size %d, mtim %+v), want pinned (%d, %+v)", st.Size, st.Mtim, sizeB, tsOf(t3))
	}

	// Only the last close surfaces the refreshed attrs.
	fs.Release("/.x", fhA)
	if st := getattrPath(t, fs, "/.x"); st.Size != sizeB || st.Mtim != tsOf(t3) {
		t.Fatalf("path Getattr after last close = (size %d, mtim %+v), want (%d, %+v)", st.Size, st.Mtim, sizeB, tsOf(t3))
	}
}

// TestProbeMtimeStaticAcrossGetattrsAdvancesOnOpen pins W2 item 5: the wedge
// probe's mtime advances ONLY at open — enough for the NFS client's open-time
// revalidation to drop cached pages, and the per-open nonce catches any replay
// regardless — and holds static across Getattrs, so the probe no longer
// invalidates pages on every attribute poll.
func TestProbeMtimeStaticAcrossGetattrsAdvancesOnOpen(t *testing.T) {
	v := newProbeView()
	mtim := func() fuse.Timespec {
		t.Helper()
		var st fuse.Stat_t
		if rc := v.getattr(&st); rc != 0 {
			t.Fatalf("getattr = %d, want 0", rc)
		}
		return st.Mtim
	}
	first := mtim()
	for i := 0; i < 3; i++ {
		if got := mtim(); got != first {
			t.Fatalf("Getattr #%d advanced Mtim %+v -> %+v; it must hold static between opens", i, first, got)
		}
	}
	rc, fh := v.open(syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("open = %d, want 0", rc)
	}
	afterOpen := mtim()
	if !tsAfter(afterOpen, first) {
		t.Fatalf("Mtim after open = %+v, want strictly after %+v", afterOpen, first)
	}
	if got := mtim(); got != afterOpen {
		t.Fatalf("Getattr after open advanced Mtim again: %+v -> %+v", afterOpen, got)
	}
	v.release(fh)
	rc, fh = v.open(syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("second open = %d, want 0", rc)
	}
	defer v.release(fh)
	if got := mtim(); !tsAfter(got, afterOpen) {
		t.Fatalf("Mtim after the second open = %+v, want strictly after %+v", got, afterOpen)
	}
}
