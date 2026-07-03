//go:build fuse && cgo && darwin

package holderfs

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
)

// muxDirent is one entry a muxFakeTenant lists from its root.
type muxDirent struct {
	name string
	ino  uint64
}

// muxFakeTenant is a minimal tenant filesystem for mux tests: it serves a
// fixed path→ino table, reports a configurable root ino (the shared real
// base-dir ino in the aliasing tests), and records every child-relative path
// it is delegated.
type muxFakeTenant struct {
	fuse.FileSystemBase
	rootIno     uint64
	inos        map[string]uint64
	dirents     []muxDirent
	statfsFiles uint64

	paths   []string
	renames [][2]string
	links   [][2]string
}

func (f *muxFakeTenant) record(path string) { f.paths = append(f.paths, path) }

func (f *muxFakeTenant) Getattr(path string, stat *fuse.Stat_t, _ uint64) int {
	f.record(path)
	if path == "/" {
		*stat = fuse.Stat_t{Ino: f.rootIno, Mode: fuse.S_IFDIR | 0o755, Nlink: 2}
		return 0
	}
	ino, ok := f.inos[path]
	if !ok {
		return -int(syscall.ENOENT)
	}
	*stat = fuse.Stat_t{Ino: ino, Mode: fuse.S_IFREG | 0o644, Nlink: 1}
	return 0
}

func (f *muxFakeTenant) Open(path string, _ int) (int, uint64) {
	f.record(path)
	if _, ok := f.inos[path]; !ok {
		return -int(syscall.ENOENT), ^uint64(0)
	}
	return 0, 7
}

func (f *muxFakeTenant) Read(path string, buff []byte, _ int64, _ uint64) int {
	f.record(path)
	return copy(buff, "data")
}

func (f *muxFakeTenant) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, _ int64, _ uint64) int {
	f.record(path)
	fill(".", nil, 0)
	fill("..", nil, 0)
	for _, e := range f.dirents {
		st := fuse.Stat_t{Ino: e.ino, Mode: fuse.S_IFREG | 0o644, Nlink: 1}
		if !fill(e.name, &st, 0) {
			return 0
		}
	}
	return 0
}

func (f *muxFakeTenant) Rename(oldpath, newpath string) int {
	f.renames = append(f.renames, [2]string{oldpath, newpath})
	return 0
}

func (f *muxFakeTenant) Link(oldpath, newpath string) int {
	f.links = append(f.links, [2]string{oldpath, newpath})
	return 0
}

func (f *muxFakeTenant) Statfs(path string, stat *fuse.Statfs_t) int {
	f.record(path)
	stat.Files = f.statfsFiles
	return 0
}

func newMuxTenant(rootIno uint64) *muxFakeTenant {
	return &muxFakeTenant{rootIno: rootIno, inos: map[string]uint64{}}
}

func mustAttach(t *testing.T, fs *muxFS, name string, child fuse.FileSystemInterface) {
	t.Helper()
	if err := fs.Attach(name, child); err != nil {
		t.Fatalf("Attach(%q) = %v, want nil", name, err)
	}
}

func TestSplitMux(t *testing.T) {
	cases := []struct {
		path, name, rest string
	}{
		{"/", "", "/"},
		{"/acct", "acct", "/"},
		{"/acct/x", "acct", "/x"},
		{"/acct/x/y.json", "acct", "/x/y.json"},
	}
	for _, c := range cases {
		if name, rest := splitMux(c.path); name != c.name || rest != c.rest {
			t.Errorf("splitMux(%q) = (%q, %q), want (%q, %q)", c.path, name, rest, c.name, c.rest)
		}
	}
}

func TestMuxRoutingPrefixStrip(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	a := newMuxTenant(101)
	a.inos["/x/y"] = 202
	mustAttach(t, fs, "a", a)

	var st fuse.Stat_t
	if rc := fs.Getattr("/a/x/y", &st, ^uint64(0)); rc != 0 {
		t.Fatalf("Getattr(/a/x/y) = %d, want 0", rc)
	}
	if len(a.paths) != 1 || a.paths[0] != "/x/y" {
		t.Fatalf("delegated paths = %v, want [/x/y] (prefix stripped)", a.paths)
	}
	a.paths = nil
	if rc := fs.Getattr("/a", &st, ^uint64(0)); rc != 0 {
		t.Fatalf("Getattr(/a) = %d, want 0", rc)
	}
	if len(a.paths) != 1 || a.paths[0] != "/" {
		t.Fatalf("tenant-root delegation = %v, want [/]", a.paths)
	}
	if rc := fs.Getattr("/zzz/x", &st, ^uint64(0)); rc != -int(syscall.ENOENT) {
		t.Fatalf("Getattr on unattached tenant = %d, want -ENOENT", rc)
	}
}

func TestMuxRootGetattr(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	var st1, st2 fuse.Stat_t
	if rc := fs.Getattr("/", &st1, ^uint64(0)); rc != 0 {
		t.Fatalf("Getattr(/) = %d, want 0", rc)
	}
	if st1.Mode != fuse.S_IFDIR|0o755 {
		t.Errorf("root mode = %#o, want S_IFDIR|0755", st1.Mode)
	}
	if st1.Ino != fusekit.SynthInoFloor {
		t.Errorf("root ino = %d, want SynthInoFloor (%d)", st1.Ino, fusekit.SynthInoFloor)
	}
	mustAttach(t, fs, "a", newMuxTenant(101))
	if rc := fs.Getattr("/", &st2, ^uint64(0)); rc != 0 {
		t.Fatalf("second Getattr(/) = %d, want 0", rc)
	}
	if st2.Mtim != st1.Mtim {
		t.Errorf("root mtime moved across an attach: %v -> %v; must stay fixed at mount", st1.Mtim, st2.Mtim)
	}
}

// TestMuxInoRemapPartition pins the fileid partition: two tenants minting
// IDENTICAL synth inos serve disjoint remapped inos, real backing inos pass
// through untouched, and a child minting past its slot span answers -EIO
// rather than aliasing a neighbor slot.
func TestMuxInoRemapPartition(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	a, b := newMuxTenant(101), newMuxTenant(101)
	for _, c := range []*muxFakeTenant{a, b} {
		c.inos["/s"] = fusekit.SynthInoFloor
		c.inos["/t"] = fusekit.SynthInoFloor + 1
		c.inos["/r"] = 12345                                           // real backing ino
		c.inos["/high"] = fusekit.SynthInoFloor + muxSlotSpan - 2      // last USABLE slot offset
		c.inos["/rootalias"] = fusekit.SynthInoFloor + muxSlotSpan - 1 // reserved for the tenant root
		c.inos["/over"] = fusekit.SynthInoFloor + muxSlotSpan          // past the slot span
	}
	mustAttach(t, fs, "a", a)
	mustAttach(t, fs, "b", b)

	get := func(path string) uint64 {
		t.Helper()
		var st fuse.Stat_t
		if rc := fs.Getattr(path, &st, ^uint64(0)); rc != 0 {
			t.Fatalf("Getattr(%s) = %d, want 0", path, rc)
		}
		return st.Ino
	}
	served := map[string]uint64{}
	for _, p := range []string{"/a/s", "/a/t", "/b/s", "/b/t"} {
		served[p] = get(p)
	}
	seen := map[uint64]string{}
	for p, ino := range served {
		if ino < fusekit.SynthInoFloor {
			t.Errorf("served ino for %s = %d, below SynthInoFloor: a synth ino leaked unremapped", p, ino)
		}
		if prev, dup := seen[ino]; dup {
			t.Errorf("served ino %d aliases %s and %s", ino, prev, p)
		}
		seen[ino] = p
	}
	if got := get("/a/r"); got != 12345 {
		t.Errorf("real ino for /a/r = %d, want 12345 passthrough", got)
	}
	if got := get("/b/r"); got != 12345 {
		t.Errorf("real ino for /b/r = %d, want 12345 passthrough", got)
	}
	// Slot-span boundary: /a takes slot 1, so its last USABLE offset (muxSlotSpan-2)
	// remaps to SynthInoFloor + 2*muxSlotSpan - 2 — one below the slot's reserved
	// tenant-root fileid. The reserved offset itself (muxSlotSpan-1 == slotRootIno's
	// offset) and anything past the span answer -EIO from Getattr, so a child object
	// can never wear its own tenant root's fileid.
	if got := get("/a/high"); got != fusekit.SynthInoFloor+1*muxSlotSpan+muxSlotSpan-2 {
		t.Errorf("last-usable-offset ino for /a/high = %d, want %d (slot-remapped)", got, fusekit.SynthInoFloor+1*muxSlotSpan+muxSlotSpan-2)
	}
	var st fuse.Stat_t
	if rc := fs.Getattr("/a/rootalias", &st, ^uint64(0)); rc != -int(syscall.EIO) {
		t.Errorf("Getattr on reserved tenant-root offset (muxSlotSpan-1) = %d, want -EIO (would alias slotRootIno)", rc)
	}
	if rc := fs.Getattr("/a/over", &st, ^uint64(0)); rc != -int(syscall.EIO) {
		t.Errorf("Getattr on over-span synth ino = %d, want -EIO", rc)
	}
}

// TestMuxTenantRootIno pins that a tenant root's served fileid is the slot's
// minted root ino — never the real base-dir ino, which every tenant shares.
func TestMuxTenantRootIno(t *testing.T) {
	const sharedBaseIno = 424242
	fs := newMuxFS("/fake/mnt")
	mustAttach(t, fs, "a", newMuxTenant(sharedBaseIno))
	mustAttach(t, fs, "b", newMuxTenant(sharedBaseIno))

	var sa, sb fuse.Stat_t
	if rc := fs.Getattr("/a", &sa, ^uint64(0)); rc != 0 {
		t.Fatalf("Getattr(/a) = %d", rc)
	}
	if rc := fs.Getattr("/b", &sb, ^uint64(0)); rc != 0 {
		t.Fatalf("Getattr(/b) = %d", rc)
	}
	if sa.Ino == sharedBaseIno || sb.Ino == sharedBaseIno {
		t.Fatalf("tenant root served the real base-dir ino %d: aliasing hazard", sharedBaseIno)
	}
	if sa.Ino == sb.Ino {
		t.Fatalf("both tenant roots serve ino %d: aliasing hazard", sa.Ino)
	}
	if sa.Ino != slotRootIno(1) || sb.Ino != slotRootIno(2) {
		t.Errorf("tenant root inos = (%d, %d), want (%d, %d)", sa.Ino, sb.Ino, slotRootIno(1), slotRootIno(2))
	}
}

// TestMuxSlotNonReuseAcrossReattach pins that a detached tenant's slot is
// never reused: a re-attach mints a NEW slot, so its objects never wear a
// fileid the NFS client may still cache for the old generation's.
func TestMuxSlotNonReuseAcrossReattach(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	a := newMuxTenant(101)
	a.inos["/s"] = fusekit.SynthInoFloor
	mustAttach(t, fs, "a", a)

	get := func(path string) uint64 {
		t.Helper()
		var st fuse.Stat_t
		if rc := fs.Getattr(path, &st, ^uint64(0)); rc != 0 {
			t.Fatalf("Getattr(%s) = %d, want 0", path, rc)
		}
		return st.Ino
	}
	oldRoot, oldSynth := get("/a"), get("/a/s")

	fs.Detach("a")
	mustAttach(t, fs, "a", a)
	newRoot, newSynth := get("/a"), get("/a/s")

	if newRoot == oldRoot {
		t.Errorf("re-attached tenant root reused ino %d", oldRoot)
	}
	if newSynth == oldSynth {
		t.Errorf("re-attached tenant synth entry reused ino %d", oldSynth)
	}
	mustAttach(t, fs, "b", newMuxTenant(101))
	if got := get("/b"); got == oldRoot || got == newRoot {
		t.Errorf("new tenant b reused a prior slot's root ino %d", got)
	}
}

func TestMuxLiveAttachDetach(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	a := newMuxTenant(101)
	a.inos["/f"] = 555

	var st fuse.Stat_t
	if rc := fs.Getattr("/a/f", &st, ^uint64(0)); rc != -int(syscall.ENOENT) {
		t.Fatalf("pre-attach Getattr = %d, want -ENOENT", rc)
	}
	mustAttach(t, fs, "a", a)
	if !fs.Attached("a") {
		t.Fatal("Attached(a) = false after Attach")
	}
	if rc := fs.Getattr("/a/f", &st, ^uint64(0)); rc != 0 {
		t.Fatalf("post-attach Getattr = %d, want 0", rc)
	}
	fs.Detach("a")
	if fs.Attached("a") {
		t.Fatal("Attached(a) = true after Detach")
	}
	if rc := fs.Getattr("/a/f", &st, ^uint64(0)); rc != -int(syscall.ENOENT) {
		t.Fatalf("post-detach Getattr = %d, want -ENOENT", rc)
	}
}

// TestMuxDetachBLeavesAOpen pins that detaching one tenant does not disturb
// another's open handle: ops on A's path keep routing after B is gone.
func TestMuxDetachBLeavesAOpen(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	a, b := newMuxTenant(101), newMuxTenant(102)
	a.inos["/f"] = 555
	b.inos["/f"] = 556
	mustAttach(t, fs, "a", a)
	mustAttach(t, fs, "b", b)

	rc, fh := fs.Open("/a/f", os.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open(/a/f) = %d, want 0", rc)
	}
	fs.Detach("b")

	buf := make([]byte, 8)
	if n := fs.Read("/a/f", buf, 0, fh); n != 4 || string(buf[:n]) != "data" {
		t.Fatalf("Read(/a/f) after detach of b = (%d, %q), want (4, data)", n, buf[:n])
	}
	if n := fs.Read("/b/f", buf, 0, 7); n != -int(syscall.ENOENT) {
		t.Fatalf("Read(/b/f) after detach = %d, want -ENOENT", n)
	}
}

func TestMuxCrossTenantRenameLink(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	a, b := newMuxTenant(101), newMuxTenant(102)
	mustAttach(t, fs, "a", a)
	mustAttach(t, fs, "b", b)

	if rc := fs.Rename("/a/x", "/b/y"); rc != -int(syscall.EXDEV) {
		t.Errorf("cross-tenant Rename = %d, want -EXDEV", rc)
	}
	if rc := fs.Link("/a/x", "/b/y"); rc != -int(syscall.EXDEV) {
		t.Errorf("cross-tenant Link = %d, want -EXDEV", rc)
	}
	if rc := fs.Rename("/a/x", "/a/y"); rc != 0 {
		t.Fatalf("same-tenant Rename = %d, want 0", rc)
	}
	if len(a.renames) != 1 || a.renames[0] != [2]string{"/x", "/y"} {
		t.Errorf("delegated rename = %v, want [[/x /y]] (prefixes stripped)", a.renames)
	}
	if rc := fs.Link("/a/x", "/a/y"); rc != 0 {
		t.Fatalf("same-tenant Link = %d, want 0", rc)
	}
	if len(a.links) != 1 || a.links[0] != [2]string{"/x", "/y"} {
		t.Errorf("delegated link = %v, want [[/x /y]]", a.links)
	}
}

// TestMuxRootMutationsEPERM pins that every mutating op on the mux root or a
// depth-1 name (attached or not) is refused: tenant membership is
// Attach/Detach's alone.
func TestMuxRootMutationsEPERM(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	mustAttach(t, fs, "a", newMuxTenant(101))

	ops := []struct {
		name string
		op   func(path string) int
	}{
		{"create", func(p string) int { rc, _ := fs.Create(p, os.O_RDWR, 0o644); return rc }},
		{"mknod", func(p string) int { return fs.Mknod(p, 0o644, 0) }},
		{"mkdir", func(p string) int { return fs.Mkdir(p, 0o755) }},
		{"unlink", fs.Unlink},
		{"rmdir", fs.Rmdir},
		{"rename-to-root", func(p string) int { return fs.Rename("/a/x", p) }},
		{"rename-from-root", func(p string) int { return fs.Rename(p, "/a/x") }},
		{"link-to-root", func(p string) int { return fs.Link("/a/x", p) }},
		{"symlink", func(p string) int { return fs.Symlink("target", p) }},
		{"chmod", func(p string) int { return fs.Chmod(p, 0o600) }},
		{"chown", func(p string) int { return fs.Chown(p, 1, 1) }},
		{"utimens", func(p string) int { return fs.Utimens(p, nil) }},
		{"truncate", func(p string) int { return fs.Truncate(p, 0, ^uint64(0)) }},
		{"setxattr", func(p string) int { return fs.Setxattr(p, "n", nil, 0) }},
		{"removexattr", func(p string) int { return fs.Removexattr(p, "n") }},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			for _, path := range []string{"/", "/a", "/newname"} {
				if rc := op.op(path); rc != -int(syscall.EPERM) {
					t.Errorf("%s(%s) = %d, want -EPERM", op.name, path, rc)
				}
			}
		})
	}
}

func TestMuxAppleDoubleAtRoot(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	if err := fs.Attach("._sidecar", newMuxTenant(101)); err == nil {
		t.Fatal("Attach of an AppleDouble tenant name succeeded, want refusal")
	}
	var st fuse.Stat_t
	if rc := fs.Getattr("/._x", &st, ^uint64(0)); rc != -int(syscall.ENOENT) {
		t.Errorf("Getattr(/._x) = %d, want -ENOENT", rc)
	}
	if rc, _ := fs.Open("/._x", os.O_RDONLY); rc != -int(syscall.ENOENT) {
		t.Errorf("Open(/._x) = %d, want -ENOENT", rc)
	}
	if rc, _ := fs.Open("/._x", os.O_RDONLY|syscall.O_CREAT); rc != -int(syscall.EACCES) {
		t.Errorf("Open(/._x, O_CREAT) = %d, want -EACCES", rc)
	}
	if rc, _ := fs.Create("/._x", os.O_RDWR, 0o644); rc != -int(syscall.EACCES) {
		t.Errorf("Create(/._x) = %d, want -EACCES", rc)
	}
	if rc := fs.Unlink("/._x"); rc != -int(syscall.ENOENT) {
		t.Errorf("Unlink(/._x) = %d, want -ENOENT", rc)
	}
	if rc := fs.Mkdir("/._x", 0o755); rc != -int(syscall.EACCES) {
		t.Errorf("Mkdir(/._x) = %d, want -EACCES", rc)
	}
}

func TestMuxAttachValidation(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	for _, name := range []string{"", ".", "..", "a/b", "/a"} {
		if err := fs.Attach(name, newMuxTenant(1)); err == nil {
			t.Errorf("Attach(%q) succeeded, want refusal", name)
		}
	}
	mustAttach(t, fs, "a", newMuxTenant(1))
	if err := fs.Attach("a", newMuxTenant(2)); err == nil {
		t.Error("double Attach(a) succeeded, want refusal")
	}
}

func TestMuxRootReaddir(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	mustAttach(t, fs, "bbb", newMuxTenant(101))
	mustAttach(t, fs, "aaa", newMuxTenant(102))

	var names []string
	inos := map[string]uint64{}
	rc := fs.Readdir("/", func(name string, st *fuse.Stat_t, _ int64) bool {
		names = append(names, name)
		if st != nil {
			inos[name] = st.Ino
		}
		return true
	}, 0, ^uint64(0))
	if rc != 0 {
		t.Fatalf("Readdir(/) = %d, want 0", rc)
	}
	want := []string{".", "..", "aaa", "bbb"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("root listing = %v, want %v", names, want)
	}
	// bbb attached first: slot 1; aaa second: slot 2.
	if inos["bbb"] != slotRootIno(1) || inos["aaa"] != slotRootIno(2) {
		t.Errorf("listed tenant inos = %v, want bbb=%d aaa=%d (minted slot root inos)", inos, slotRootIno(1), slotRootIno(2))
	}
	fs.Detach("bbb")
	names = nil
	if rc := fs.Readdir("/", func(name string, _ *fuse.Stat_t, _ int64) bool {
		names = append(names, name)
		return true
	}, 0, ^uint64(0)); rc != 0 {
		t.Fatalf("Readdir(/) after detach = %d, want 0", rc)
	}
	if strings.Join(names, ",") != ".,..,aaa" {
		t.Errorf("root listing after detach = %v, want [. .. aaa]", names)
	}
}

// TestMuxReaddirRemapsChildStats pins the second ino-bearing surface: a
// delegated Readdir's fill stats are slot-remapped (real inos pass through),
// and an over-span child ino fails the whole listing with -EIO.
func TestMuxReaddirRemapsChildStats(t *testing.T) {
	fs := newMuxFS("/fake/mnt")
	a := newMuxTenant(101)
	// synth (offset 3) and high (the last USABLE offset, muxSlotSpan-2) both
	// remap; real passes through.
	a.dirents = []muxDirent{
		{"synth", fusekit.SynthInoFloor + 3},
		{"real", 777},
		{"high", fusekit.SynthInoFloor + muxSlotSpan - 2},
	}
	mustAttach(t, fs, "a", a)

	inos := map[string]uint64{}
	rc := fs.Readdir("/a", func(name string, st *fuse.Stat_t, _ int64) bool {
		if st != nil {
			inos[name] = st.Ino
		}
		return true
	}, 0, ^uint64(0))
	if rc != 0 {
		t.Fatalf("Readdir(/a) = %d, want 0", rc)
	}
	wantSynth := fusekit.SynthInoFloor + 1*muxSlotSpan + 3
	if inos["synth"] != wantSynth {
		t.Errorf("listed synth ino = %d, want %d (slot-remapped)", inos["synth"], wantSynth)
	}
	if inos["real"] != 777 {
		t.Errorf("listed real ino = %d, want 777 passthrough", inos["real"])
	}
	wantHigh := fusekit.SynthInoFloor + 1*muxSlotSpan + muxSlotSpan - 2
	if inos["high"] != wantHigh {
		t.Errorf("listed last-usable-offset ino = %d, want %d (slot-remapped)", inos["high"], wantHigh)
	}

	// The reserved tenant-root offset (muxSlotSpan-1) and anything past the span
	// each fail the WHOLE listing with -EIO — the second ino-bearing surface must
	// refuse a child object that would alias its tenant root or a neighbor slot.
	base := []muxDirent{{"synth", fusekit.SynthInoFloor + 3}, {"real", 777}}
	for _, tc := range []struct {
		name string
		ino  uint64
	}{
		{"reserved-root-offset", fusekit.SynthInoFloor + muxSlotSpan - 1},
		{"over-span", fusekit.SynthInoFloor + muxSlotSpan},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a.dirents = append(append([]muxDirent(nil), base...), muxDirent{tc.name, tc.ino})
			if rc := fs.Readdir("/a", func(string, *fuse.Stat_t, int64) bool { return true }, 0, ^uint64(0)); rc != -int(syscall.EIO) {
				t.Errorf("Readdir with %s child ino = %d, want -EIO", tc.name, rc)
			}
		})
	}
}

func TestMuxStatfs(t *testing.T) {
	root := filepath.Join(t.TempDir(), "mnt")
	fs := newMuxFS(root)
	a := newMuxTenant(101)
	a.statfsFiles = 7777
	mustAttach(t, fs, "a", a)

	var st fuse.Statfs_t
	if rc := fs.Statfs("/", &st); rc != 0 {
		t.Fatalf("Statfs(/) = %d, want 0", rc)
	}
	if st.Bsize == 0 || st.Blocks == 0 {
		t.Errorf("root Statfs geometry = bsize %d blocks %d, want the mux root's parent volume", st.Bsize, st.Blocks)
	}
	var sub fuse.Statfs_t
	if rc := fs.Statfs("/a/deep", &sub); rc != 0 {
		t.Fatalf("Statfs(/a/deep) = %d, want 0", rc)
	}
	if sub.Files != 7777 {
		t.Errorf("subtree Statfs.Files = %d, want 7777 (delegated)", sub.Files)
	}
	if rc := fs.Statfs("/zzz/x", &sub); rc != -int(syscall.ENOENT) {
		t.Errorf("Statfs on unattached tenant = %d, want -ENOENT", rc)
	}
}

func TestBuildMuxConfig(t *testing.T) {
	cases := []struct {
		name        string
		spec        fusekit.MountSpec
		wantOpts    []string
		refuseOpts  []string
		wantVolname string
	}{
		{
			name:        "defaults",
			spec:        fusekit.MountSpec{Base: "/fake/parent", Dir: "/fake/parent/mnt", ContentMode: fusekit.ContentModeMux},
			wantOpts:    []string{"volname=holder-mnt", "nobrowse", "rwsize=1048576", "noattrcache"},
			wantVolname: "holder-mnt",
		},
		{
			name: "attrcache_passthrough",
			spec: fusekit.MountSpec{
				Base: "/fake/parent", Dir: "/fake/parent/mnt", ContentMode: fusekit.ContentModeMux,
				AttrCache: true, AttrCacheTimeout: 2 * time.Second,
			},
			wantOpts:   []string{"attrcache-timeout=2"},
			refuseOpts: []string{"noattrcache"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Build(tc.spec)
			if err != nil {
				t.Fatalf("Build = %v", err)
			}
			if _, ok := cfg.FS.(fusekit.SubtreeHost); !ok {
				t.Fatalf("mux Config.FS is %T, which does not implement SubtreeHost", cfg.FS)
			}
			if !cfg.ClearCarcass {
				t.Error("ClearCarcass = false, want true")
			}
			if cfg.ForceOnWedge {
				t.Error("ForceOnWedge = true, want false (graceful-only holder)")
			}
			opts := strings.Join(cfg.Options, " ")
			for _, want := range tc.wantOpts {
				if !strings.Contains(opts, want) {
					t.Errorf("options %q missing %q", opts, want)
				}
			}
			for _, refuse := range tc.refuseOpts {
				if strings.Contains(opts, refuse) {
					t.Errorf("options %q must not contain %q", opts, refuse)
				}
			}
		})
	}
}

// TestMuxSharedInoBases pins that both child modes mint from the shared
// SynthInoFloor the mux remap keys on — a drifted base would leak unremapped
// synthetic fileids into a shared mount.
func TestMuxSharedInoBases(t *testing.T) {
	if sharedLinkInoBase != fusekit.SynthInoFloor {
		t.Errorf("sharedLinkInoBase = %d, want fusekit.SynthInoFloor (%d)", sharedLinkInoBase, fusekit.SynthInoFloor)
	}
	if treeInoBase != fusekit.SynthInoFloor {
		t.Errorf("treeInoBase = %d, want fusekit.SynthInoFloor (%d)", treeInoBase, fusekit.SynthInoFloor)
	}
}

// TestHolderFSReleaseAllClosesFds pins the fd half of the detach-time handle
// release: ReleaseAll closes every open real fd this mirror handed the kernel —
// files (Open/Create) and dirs (Opendir) — so a mux detach that makes the mirror
// unroutable never leaks the fds the kernel's later Release/Releasedir can no
// longer reach. No bridge runs here, so the closed fds cannot be reused before
// the Fstat check.
func TestHolderFSReleaseAllClosesFds(t *testing.T) {
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "plain"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	fs := newReleaseTestFS(base, t.TempDir(), nil)

	rc, ffh := fs.Open("/plain", os.O_RDWR)
	if rc != 0 {
		t.Fatalf("Open(/plain) = %d, want 0", rc)
	}
	rc, dfh := fs.Opendir("/")
	if rc != 0 {
		t.Fatalf("Opendir(/) = %d, want 0", rc)
	}
	if got := trackedFdCount(fs); got != 2 {
		t.Fatalf("tracked fds = %d, want 2 (file + dir)", got)
	}

	fs.ReleaseAll()

	if got := trackedFdCount(fs); got != 0 {
		t.Fatalf("tracked fds after ReleaseAll = %d, want 0", got)
	}
	for name, fd := range map[string]uint64{"file": ffh, "dir": dfh} {
		var st syscall.Stat_t
		if err := syscall.Fstat(int(fd), &st); err != syscall.EBADF {
			t.Errorf("%s fd %d still open after ReleaseAll: Fstat err = %v, want EBADF", name, fd, err)
		}
	}
}

// TestHolderFSReleaseAllSchedulesDirtySynthWriteThrough pins the write-through
// half: ReleaseAll routes a still-open dirty synth fd through the mirror's own
// Release, which schedules the entry's write-through — so a detach never drops
// the last synth edit. FlushWithin then drains the durable bytes to the consumer.
func TestHolderFSReleaseAllSchedulesDirtySynthWriteThrough(t *testing.T) {
	fc := &fakeContent{readBytes: []byte("v1")}
	client := content.NewBridgeClient(serveContent(t, fc))
	priv := t.TempDir()
	writePath := filepath.Join(priv, ".x")
	v := newSynthView(".x", "d", client, writePath, nil)
	v.ino = sharedLinkInoBase
	fs := newReleaseTestFS(t.TempDir(), priv, map[string]*synthView{"/.x": v})

	rc, sfh := fs.Create("/.x", os.O_RDWR, 0o600)
	if rc != 0 {
		t.Fatalf("Create(/.x) = %d, want 0", rc)
	}
	if n := fs.Write("/.x", []byte("NEWDATA"), 0, sfh); n != 7 {
		t.Fatalf("Write(/.x) = %d, want 7", n)
	}
	if got := trackedFdCount(fs); got != 1 {
		t.Fatalf("tracked fds = %d, want 1 (the writable synth fd)", got)
	}

	fs.ReleaseAll()

	if got := trackedFdCount(fs); got != 0 {
		t.Fatalf("tracked fds after ReleaseAll = %d, want 0 (fd closed)", got)
	}
	if !v.flushWithin(2 * time.Second) {
		t.Fatal("ReleaseAll did not schedule the dirty synth write-through")
	}
	if w := fc.wrote(); len(w) != 1 || string(w[0]) != "NEWDATA" {
		t.Fatalf("write-through payloads = %q, want one NEWDATA (re-read from the durable file)", w)
	}
}

// newReleaseTestFS builds a minimal holderFS for the ReleaseAll tests, mirroring
// Build's field init for the surfaces the fd-tracking path touches.
func newReleaseTestFS(base, priv string, synth map[string]*synthView) *holderFS {
	if synth == nil {
		synth = map[string]*synthView{}
	}
	return &holderFS{
		base:         base,
		privateRoot:  priv,
		privateExact: map[string]bool{},
		shared:       map[string]sharedEntry{},
		synth:        synth,
		synthFhs:     map[uint64]*synthHandle{},
		nextSynthFh:  synthFhBase,
		fileFhs:      map[uint64]string{},
		dirFhs:       map[uint64]struct{}{},
	}
}

func trackedFdCount(fs *holderFS) int {
	fs.synthMu.Lock()
	defer fs.synthMu.Unlock()
	return len(fs.fileFhs) + len(fs.dirFhs)
}

// TestBuildRejectsUnknownMode still refuses garbage after the mux dispatch
// was added.
func TestBuildRejectsUnknownMode(t *testing.T) {
	_, err := Build(fusekit.MountSpec{Base: "/b", Dir: "/d", ContentMode: "bogus"})
	if err == nil {
		t.Fatal("Build with unknown content mode succeeded, want error")
	}
	if !strings.Contains(err.Error(), "unknown content mode") {
		t.Fatalf("Build error = %v, want the unknown-content-mode refusal", err)
	}
}
