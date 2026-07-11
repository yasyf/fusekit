//go:build fuse && cgo && darwin

package holderfs

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
)

// buildTreeFS builds a tree-mode Config over src and returns its treeFS.
func buildTreeFS(t *testing.T, src content.Source, probePath string) (*treeFS, fusekit.Config) {
	t.Helper()
	spec := fusekit.MountSpec{
		Base:          "/repo/notes",
		Dir:           "/mnt/notes",
		Owner:         "cc-notes",
		ContentSocket: serveContent(t, src),
		Domain:        "d",
		ContentMode:   fusekit.ContentModeTree,
		ProbePath:     probePath,
	}
	built, err := Build(spec)
	if err != nil {
		t.Fatalf("Build(tree) = %v", err)
	}
	fs, ok := built.FS.(*treeFS)
	if !ok {
		t.Fatalf("Build(tree) FS = %T, want *treeFS", built.FS)
	}
	return fs, built
}

// TestBuildTreeMode pins the Build wiring: ContentMode "tree" produces a
// treeFS with source mode's mount options (no namedattr — the panic
// mitigation applies to tree tenants from day one), a tree-aware readiness
// probe, and fail-loud behavior for a missing socket, an unreachable bridge,
// and a Tree-less consumer.
func TestBuildTreeMode(t *testing.T) {
	t.Run("wires a treeFS", func(t *testing.T) {
		f := newTreeFakeH()
		f.put("/a", []byte("x"), time.Now().UnixNano(), 0)
		fs, cfg := buildTreeFS(t, f, "")
		if cfg.Base != "/repo/notes" || cfg.Dir != "/mnt/notes" {
			t.Fatalf("cfg endpoints = (%s, %s)", cfg.Base, cfg.Dir)
		}
		if cfg.Ready == nil {
			t.Fatal("cfg.Ready = nil; tree mode must not fall back to MountAlive (the nominal base never shows through)")
		}
		opts := strings.Join(cfg.Options, " ")
		for _, want := range []string{"volname=holder-notes", "nobrowse", "noattrcache", "rwsize=1048576"} {
			if !strings.Contains(opts, want) {
				t.Errorf("options %q missing %q", opts, want)
			}
		}
		if strings.Contains(opts, "namedattr") {
			t.Errorf("options %q carry namedattr; the nfs_vinvalbuf2 mitigation must hold in tree mode", opts)
		}
		if fs.probe != nil {
			t.Fatal("probe view built with no ProbePath")
		}
		// Build's prewarm already listed the root: readdir costs no extra RPC.
		var names []string
		rc := fs.Readdir("/", func(name string, _ *fuse.Stat_t, _ int64) bool {
			names = append(names, name)
			return true
		}, 0, ^uint64(0))
		if rc != 0 || len(names) != 3 { // ".", "..", "a"
			t.Fatalf("Readdir = %v (%d), want [. .. a]", names, rc)
		}
		if n := f.count("list:/"); n != 1 {
			t.Fatalf("consumer saw %d root lists, want 1 (Build prewarm serves the readdir)", n)
		}
		if n := f.count("releaseall:"); n != 1 {
			t.Fatalf("consumer saw %d ReleaseAllHandles sweeps at Build, want 1", n)
		}
	})
	t.Run("requires a content socket", func(t *testing.T) {
		_, err := Build(fusekit.MountSpec{Base: "/r", Dir: "/m", ContentMode: fusekit.ContentModeTree})
		if err == nil || !strings.Contains(err.Error(), "content socket") {
			t.Fatalf("Build without a socket = %v, want the socket refusal", err)
		}
	})
	t.Run("unreachable bridge fails loud", func(t *testing.T) {
		_, err := Build(fusekit.MountSpec{
			Base: "/r", Dir: "/m", ContentMode: fusekit.ContentModeTree,
			ContentSocket: filepath.Join(t.TempDir(), "no.sock"),
		})
		if !errors.Is(err, content.ErrBridgeUnavailable) {
			t.Fatalf("Build over a dead socket = %v, want ErrBridgeUnavailable (drivers classify it retryable)", err)
		}
	})
	t.Run("a Source-only consumer fails loud", func(t *testing.T) {
		_, err := Build(fusekit.MountSpec{
			Base: "/r", Dir: "/m", ContentMode: fusekit.ContentModeTree,
			ContentSocket: serveContent(t, &fakeContent{}),
		})
		if err == nil || !strings.Contains(err.Error(), "content.Tree") {
			t.Fatalf("Build on a Source-only consumer = %v, want the Tree capability refusal", err)
		}
	})
}

// TestTreeFSNoLocalBaseIO is the tree-mode parallel of TestRealRouting's
// routing contract, stated structurally: the spec's Base is a NOMINAL identity
// key, so a real, populated Base dir must never be read (its entries do not
// exist through the mount, and never list) and never gain, lose, or change an
// entry — no matter which vnop runs. Every op below is served by the bridge
// consumer; any local fallthrough would either surface the sentinel or mutate
// the Base dir, and both are asserted against.
func TestTreeFSNoLocalBaseIO(t *testing.T) {
	base := t.TempDir()
	sentinel := filepath.Join(base, "base-only.txt")
	if err := os.WriteFile(sentinel, []byte("local"), 0o644); err != nil {
		t.Fatal(err)
	}

	f := newTreeFakeH()
	f.put("/remote.md", []byte("remote"), time.Now().UnixNano(), 0)
	f.mu.Lock()
	f.links["/lnk"] = "/abs"
	f.mu.Unlock()

	built, err := Build(fusekit.MountSpec{
		// Base is nominal (never read); mountd refuses dir == base, so tenants
		// always mount at a dedicated dir.
		Base: base, Dir: t.TempDir(),
		Owner: "cc-notes", Domain: "d",
		ContentMode:   fusekit.ContentModeTree,
		ContentSocket: serveContent(t, f),
	})
	if err != nil {
		t.Fatalf("Build = %v", err)
	}
	fs, ok := built.FS.(*treeFS)
	if !ok {
		t.Fatalf("Build FS = %T, want *treeFS", built.FS)
	}

	// A name that exists ONLY in the local Base must not resolve or list.
	var st fuse.Stat_t
	if rc := fs.Getattr("/base-only.txt", &st, ^uint64(0)); rc != -int(syscall.ENOENT) {
		t.Fatalf("Getattr(base-only) = %d, want ENOENT — the local Base must never be read", rc)
	}
	var names []string
	if rc := fs.Readdir("/", func(name string, _ *fuse.Stat_t, _ int64) bool {
		names = append(names, name)
		return true
	}, 0, ^uint64(0)); rc != 0 {
		t.Fatalf("Readdir = %d", rc)
	}
	sort.Strings(names)
	if got := strings.Join(names, ","); got != ".,..,lnk,remote.md" {
		t.Fatalf("Readdir names = %v, want only the consumer's entries", names)
	}

	// Drive the full vnop surface; everything answers from the consumer.
	if rc := fs.Getattr("/remote.md", &st, ^uint64(0)); rc != 0 || st.Size != 6 {
		t.Fatalf("Getattr(remote) = rc %d size %d", rc, st.Size)
	}
	rc, fh := fs.Open("/remote.md", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open = %d", rc)
	}
	buf := make([]byte, 16)
	if n := fs.Read("/remote.md", buf, 0, fh); n != 6 || string(buf[:6]) != "remote" {
		t.Fatalf("Read = %d %q", n, buf[:max(n, 0)])
	}
	if rc := fs.Release("/remote.md", fh); rc != 0 {
		t.Fatalf("Release = %d", rc)
	}
	rc, wfh := fs.Create("/new.md", syscall.O_WRONLY, 0o644)
	if rc != 0 {
		t.Fatalf("Create = %d", rc)
	}
	if n := fs.Write("/new.md", []byte("hello"), 0, wfh); n != 5 {
		t.Fatalf("Write = %d", n)
	}
	if rc := fs.Flush("/new.md", wfh); rc != 0 {
		t.Fatalf("Flush = %d", rc)
	}
	if rc := fs.Release("/new.md", wfh); rc != 0 {
		t.Fatalf("Release(new) = %d", rc)
	}
	if rc := fs.Truncate("/new.md", 2, ^uint64(0)); rc != 0 {
		t.Fatalf("Truncate = %d", rc)
	}
	if rc := fs.Rename("/new.md", "/moved.md"); rc != 0 {
		t.Fatalf("Rename = %d", rc)
	}
	if rc := fs.Unlink("/moved.md"); rc != 0 {
		t.Fatalf("Unlink = %d", rc)
	}
	if rc := fs.Mkdir("/sub", 0o755); rc != 0 {
		t.Fatalf("Mkdir = %d", rc)
	}
	if rc, _ := fs.Opendir("/sub"); rc != 0 {
		t.Fatalf("Opendir = %d", rc)
	}
	fs.Releasedir("/sub", ^uint64(0))
	if rc, target := fs.Readlink("/lnk"); rc != 0 || target != "/abs" {
		t.Fatalf("Readlink = (%d, %q)", rc, target)
	}
	if rc := fs.Chmod("/remote.md", 0o600); rc != 0 {
		t.Fatalf("Chmod = %d", rc)
	}
	if rc := fs.Utimens("/remote.md", []fuse.Timespec{{}, {}}); rc != 0 {
		t.Fatalf("Utimens = %d", rc)
	}
	for name, op := range map[string]func() int{
		"rmdir":   func() int { return fs.Rmdir("/sub") },
		"link":    func() int { return fs.Link("/remote.md", "/hard") },
		"symlink": func() int { return fs.Symlink("/remote.md", "/soft") },
		"chown":   func() int { return fs.Chown("/remote.md", 0, 0) },
	} {
		if rc := op(); rc != -int(syscall.EPERM) {
			t.Errorf("%s = %d, want EPERM", name, rc)
		}
	}
	var sfs fuse.Statfs_t
	if rc := fs.Statfs("/", &sfs); rc != 0 {
		t.Fatalf("Statfs = %d", rc)
	}

	// The Base dir was never touched: exactly the seeded entry, bytes intact.
	ents, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 || ents[0].Name() != "base-only.txt" {
		var got []string
		for _, e := range ents {
			got = append(got, e.Name())
		}
		t.Fatalf("Base entries after the vnop sweep = %v, want the untouched seed only", got)
	}
	data, err := os.ReadFile(sentinel)
	if err != nil || string(data) != "local" {
		t.Fatalf("Base sentinel = %q, %v; the nominal Base must never be written", data, err)
	}
}

// TestTreeFSGetattrConversion pins the treeStat → fuse.Stat_t mapping: fixed
// holder presentation modes, minted inos, and the view's stabilized attrs.
func TestTreeFSGetattrConversion(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a.md", []byte("hello"), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), 7)
	f.mu.Lock()
	f.dirs["/sub"] = true
	f.links["/lnk"] = "/abs/t"
	f.mu.Unlock()
	fs, _ := buildTreeFS(t, f, "")

	var st fuse.Stat_t
	if rc := fs.Getattr("/", &st, ^uint64(0)); rc != 0 || st.Mode != fuse.S_IFDIR|0o755 || st.Nlink != 2 {
		t.Fatalf("Getattr(/) = rc %d mode %#o nlink %d, want a 0755 dir", rc, st.Mode, st.Nlink)
	}
	if rc := fs.Getattr("/a.md", &st, ^uint64(0)); rc != 0 {
		t.Fatalf("Getattr(/a.md) = %d", rc)
	}
	if st.Mode != fuse.S_IFREG|0o644 || st.Size != 5 || st.Ino < treeInoBase || st.Ino == 7 {
		t.Fatalf("Getattr(/a.md) = mode %#o size %d ino %d; want a 0644 file, size 5, minted ino", st.Mode, st.Size, st.Ino)
	}
	if st.Mtim.Sec == 0 || st.Blksize != 4096 {
		t.Fatalf("Getattr(/a.md) mtime/blk = %+v/%d; times and geometry must fill", st.Mtim, st.Blksize)
	}
	if rc := fs.Getattr("/sub", &st, ^uint64(0)); rc != 0 || st.Mode != fuse.S_IFDIR|0o755 {
		t.Fatalf("Getattr(/sub) = rc %d mode %#o, want a dir", rc, st.Mode)
	}
	if rc := fs.Getattr("/lnk", &st, ^uint64(0)); rc != 0 || st.Mode != fuse.S_IFLNK|0o777 {
		t.Fatalf("Getattr(/lnk) = rc %d mode %#o, want a symlink", rc, st.Mode)
	}
	if rc, target := fs.Readlink("/lnk"); rc != 0 || target != "/abs/t" {
		t.Fatalf("Readlink(/lnk) = (%d, %q)", rc, target)
	}
	if rc := fs.Getattr("/._side", &st, ^uint64(0)); rc != -int(syscall.ENOENT) {
		t.Fatalf("Getattr(._side) = %d, want ENOENT (W1b)", rc)
	}
	var sfs fuse.Statfs_t
	if rc := fs.Statfs("/", &sfs); rc != 0 || sfs.Bsize != 4096 || sfs.Namemax != 255 {
		t.Fatalf("Statfs = rc %d %+v, want the synthetic geometry", rc, sfs)
	}
}

// TestTreeFSWriteRoundTrip drives the vnop surface end to end: Create, Write,
// handle Getattr, Flush (the commit), Release, then a fresh Open/Read serving
// the committed bytes.
func TestTreeFSWriteRoundTrip(t *testing.T) {
	f := newTreeFakeH()
	fs, _ := buildTreeFS(t, f, "")

	rc, fh := fs.Create("/n.md", syscall.O_WRONLY, 0o644)
	if rc != 0 {
		t.Fatalf("Create = %d", rc)
	}
	if n := fs.Write("/n.md", []byte("hello"), 0, fh); n != 5 {
		t.Fatalf("Write = %d, want 5", n)
	}
	var st fuse.Stat_t
	if rc := fs.Getattr("/n.md", &st, fh); rc != 0 || st.Size != 5 {
		t.Fatalf("handle Getattr = rc %d size %d, want 5", rc, st.Size)
	}
	if rc := fs.Flush("/n.md", fh); rc != 0 {
		t.Fatalf("Flush = %d", rc)
	}
	if rc := fs.Release("/n.md", fh); rc != 0 {
		t.Fatalf("Release = %d", rc)
	}
	if got := string(f.bytes("/n.md")); got != "hello" {
		t.Fatalf("consumer bytes = %q, want the committed hello", got)
	}

	rc, rfh := fs.Open("/n.md", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open = %d", rc)
	}
	buf := make([]byte, 16)
	if n := fs.Read("/n.md", buf, 0, rfh); n != 5 || string(buf[:5]) != "hello" {
		t.Fatalf("Read = %d %q, want hello", n, buf[:5])
	}
	if rc := fs.Release("/n.md", rfh); rc != 0 {
		t.Fatalf("Release(read) = %d", rc)
	}

	// Fsync forwards the same commit verdict as Flush.
	f.flushErr = cerr{"unparseable", content.ClassInvalid}
	rc, wfh := fs.Open("/n.md", syscall.O_WRONLY)
	if rc != 0 {
		t.Fatalf("Open(write) = %d", rc)
	}
	if n := fs.Write("/n.md", []byte("bad"), 0, wfh); n != 3 {
		t.Fatalf("Write = %d", n)
	}
	if rc := fs.Fsync("/n.md", false, wfh); rc != -int(syscall.EINVAL) {
		t.Fatalf("Fsync of a rejected save = %d, want EINVAL", rc)
	}
}

// TestTreeFSRefusalsAndAcks pins the deliberate non-bridge vnop semantics:
// Rmdir/Link/Symlink/Chown are refused (no bridge op exists), Chmod/Utimens
// are acknowledged and ignored (the consumer owns presentation and times),
// and the xattr surface is virtual.
func TestTreeFSRefusalsAndAcks(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a", []byte("x"), time.Now().UnixNano(), 0)
	f.mu.Lock()
	f.dirs["/sub"] = true
	f.mu.Unlock()
	fs, _ := buildTreeFS(t, f, "")

	eperm := -int(syscall.EPERM)
	cases := []struct {
		name string
		op   func() int
		want int
	}{
		{"rmdir", func() int { return fs.Rmdir("/sub") }, eperm},
		{"rmdir appledouble", func() int { return fs.Rmdir("/._d") }, -int(syscall.ENOENT)},
		{"link", func() int { return fs.Link("/a", "/b") }, eperm},
		{"link appledouble source", func() int { return fs.Link("/._x", "/b") }, -int(syscall.ENOENT)},
		{"link appledouble dest", func() int { return fs.Link("/a", "/._b") }, -int(syscall.EACCES)},
		{"symlink", func() int { return fs.Symlink("/a", "/l") }, eperm},
		{"symlink appledouble dest", func() int { return fs.Symlink("/a", "/._l") }, -int(syscall.EACCES)},
		{"chown", func() int { return fs.Chown("/a", 0, 0) }, eperm},
		{"chown appledouble", func() int { return fs.Chown("/._x", 0, 0) }, -int(syscall.ENOENT)},
		{"chmod existing acks", func() int { return fs.Chmod("/a", 0o600) }, 0},
		{"chmod missing", func() int { return fs.Chmod("/ghost", 0o600) }, -int(syscall.ENOENT)},
		{"utimens existing acks", func() int { return fs.Utimens("/a", []fuse.Timespec{{}, {}}) }, 0},
		{"utimens missing", func() int { return fs.Utimens("/ghost", []fuse.Timespec{{}, {}}) }, -int(syscall.ENOENT)},
		{"setxattr", func() int { return fs.Setxattr("/a", "user.x", nil, 0) }, eperm},
		{"removexattr", func() int { return fs.Removexattr("/a", "user.x") }, eperm},
		{"listxattr", func() int { return fs.Listxattr("/a", func(string) bool { return true }) }, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.op(); got != tc.want {
				t.Errorf("errno = %d, want %d", got, tc.want)
			}
		})
	}
	if rc, _ := fs.Getxattr("/a", "user.x"); rc != -int(syscall.ENOATTR) {
		t.Fatalf("Getxattr = %d, want ENOATTR", rc)
	}
	// A chmod ack must not have dirtied anything consumer-side.
	if got := string(f.bytes("/a")); got != "x" {
		t.Fatalf("consumer bytes after acks = %q, want untouched", got)
	}
}

// TestTreeFSProbeRouting pins the virtual wedge probe in tree mode: served on
// its path, hidden from listings, immune to mutation — as in source mode.
func TestTreeFSProbeRouting(t *testing.T) {
	f := newTreeFakeH()
	f.put("/a", []byte("x"), time.Now().UnixNano(), 0)
	fs, _ := buildTreeFS(t, f, "/.ccn-probe")
	if fs.probe == nil {
		t.Fatal("no probe view built for a ProbePath spec")
	}

	var st fuse.Stat_t
	if rc := fs.Getattr("/.ccn-probe", &st, ^uint64(0)); rc != 0 || st.Size != probeSize {
		t.Fatalf("Getattr(probe) = rc %d size %d, want the virtual probe", rc, st.Size)
	}
	rc, fh := fs.Open("/.ccn-probe", syscall.O_RDONLY)
	if rc != 0 || !probeFh(fh) {
		t.Fatalf("Open(probe) = (%d, %d), want a probe handle", rc, fh)
	}
	buf := make([]byte, 64)
	if n := fs.Read("/.ccn-probe", buf, 0, fh); n != 64 {
		t.Fatalf("Read(probe) = %d, want 64", n)
	}
	if n := fs.Write("/.ccn-probe", buf, 0, fh); n != -int(syscall.EBADF) {
		t.Fatalf("Write(probe) = %d, want EBADF", n)
	}
	if rc := fs.Release("/.ccn-probe", fh); rc != 0 {
		t.Fatalf("Release(probe) = %d", rc)
	}
	var names []string
	fs.Readdir("/", func(name string, _ *fuse.Stat_t, _ int64) bool {
		names = append(names, name)
		return true
	}, 0, ^uint64(0))
	for _, n := range names {
		if n == ".ccn-probe" {
			t.Fatal("the virtual probe listed in Readdir")
		}
	}
	for name, op := range map[string]func() int{
		"unlink":   func() int { return fs.Unlink("/.ccn-probe") },
		"mkdir":    func() int { return fs.Mkdir("/.ccn-probe", 0o755) },
		"truncate": func() int { return fs.Truncate("/.ccn-probe", 0, ^uint64(0)) },
		"rename":   func() int { return fs.Rename("/.ccn-probe", "/x") },
		"utimens":  func() int { return fs.Utimens("/.ccn-probe", []fuse.Timespec{{}, {}}) },
	} {
		if rc := op(); rc != -int(syscall.EPERM) {
			t.Errorf("%s(probe) = %d, want EPERM", name, rc)
		}
	}
	rc, _ = fs.Create("/.ccn-probe", syscall.O_WRONLY, 0o644)
	if rc != -int(syscall.EPERM) {
		t.Errorf("Create(probe) = %d, want EPERM", rc)
	}
}

// TestTreeReadyFn pins tree-mode readiness: with a probe path it defers to the
// virtual-probe lstat; without one it keys on the dir being a live mountpoint
// (a plain directory must read not-ready — MountAlive's base-visibility check
// is structurally wrong for a nominal base and must not be the fallback).
func TestTreeReadyFn(t *testing.T) {
	t.Run("probe path", func(t *testing.T) {
		dir := t.TempDir()
		ready := treeReadyFn(fusekit.MountSpec{Dir: dir, Base: "/nominal", ProbePath: "/.p"})
		if ready() {
			t.Fatal("ready with no probe present = true, want false")
		}
		if err := os.WriteFile(filepath.Join(dir, ".p"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if !ready() {
			t.Fatal("ready with the probe present = false, want true")
		}
	})
	t.Run("no probe requires a mountpoint", func(t *testing.T) {
		dir := t.TempDir()
		// A populated plain dir would read ready under MountAlive(base=dir);
		// the tree probe must not.
		if err := os.WriteFile(filepath.Join(dir, "file"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		ready := treeReadyFn(fusekit.MountSpec{Dir: dir, Base: dir})
		if ready() {
			t.Fatal("ready on a plain directory = true; tree readiness requires a live mountpoint")
		}
	})
}
