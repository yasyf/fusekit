//go:build fuse && cgo

// Live fuse-t mount tests (FUSEKIT_LIVE=1); the runner must pin fuse-t's
// library: CGOFUSE_LIBFUSE_PATH=/usr/local/lib/libfuse-t.dylib on macOS.

package fusekit

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

// NewLiveProbeFS exposes the unexported probeFS to the external holder live
// test (an internal test importing mountd would be an import cycle).
func NewLiveProbeFS(root string) fuse.FileSystemInterface { return &probeFS{root: root} }

// liveScratch makes a scratch root under /tmp — not t.TempDir(): paths stay
// short (the external holder test's socket must fit macOS's 104-byte sun_path
// limit) and off ~/.claude and the real pool.
func liveScratch(t *testing.T) (src, mnt string) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "fusekit-live-")
	if err != nil {
		t.Fatalf("scratch root: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	src = filepath.Join(root, "src")
	mnt = filepath.Join(root, "mnt")
	for _, d := range []string{src, mnt} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	return src, mnt
}

// forceCleanupMount force-unmounts dir and clears its carcass on test exit — a
// stranded wedged fuse-t mount can freeze the machine. Register after
// liveScratch: t.Cleanup is LIFO, so unmount precedes dir removal. dir MUST be
// a scratch mountpoint, never a real account dir or ~/.claude.
func forceCleanupMount(t *testing.T, dir string) {
	t.Helper()
	t.Cleanup(func() {
		_ = ForceUnmount(dir)
		_ = ClearCarcass(dir)
	})
}

// statMtimeNsec reads path's mtime nanoseconds via ModTime(), not platform
// Stat_t fields, so the file compiles on any GOOS.
func statMtimeNsec(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return int64(fi.ModTime().Nanosecond()), nil
}

// pollMtimeNsec polls path's mtime nanoseconds until they equal want: even with
// noattrcache the macOS NFS client serves a just-cached mtime for a few seconds
// (observed ~5s) before revalidating.
func pollMtimeNsec(t *testing.T, path string, want int64) int64 {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		nsec, err := statMtimeNsec(path)
		if err != nil {
			t.Fatalf("stat through mount: %v", err)
		}
		if nsec == want {
			return nsec
		}
		if time.Now().After(deadline) {
			t.Fatalf("Mtim.Nsec did not reach %d within 30s (last %d) — the cache-defeat override did not round-trip", want, nsec)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestLiveMountRoundTrip proves a live fuse-t mount serves the backing dir's
// bytes and tears down clean.
func TestLiveMountRoundTrip(t *testing.T) {
	if os.Getenv("FUSEKIT_LIVE") != "1" {
		t.Skip("set FUSEKIT_LIVE=1 for live fuse-t mount tests")
	}
	src, mnt := liveScratch(t)
	forceCleanupMount(t, mnt)

	const name, payload = "hello.txt", "round-trip through fuse-t\n"
	if err := os.WriteFile(filepath.Join(src, name), []byte(payload), 0o600); err != nil {
		t.Fatalf("seed backing file: %v", err)
	}

	h, err := Mount(Config{
		Base:      src,
		Dir:       mnt,
		FS:        &probeFS{root: src},
		Options:   MountOptions{Volname: "fusekit-live", NoBrowse: true}.Build(),
		Ready:     func() bool { return MountAlive(src, mnt) },
		Wait:      probeWait,
		FirstWait: probeFirstWait,
	})
	if err != nil {
		t.Fatalf("mount: %v", err)
	}

	through := filepath.Join(mnt, name)
	fi, err := os.Stat(through)
	if err != nil {
		t.Fatalf("stat through mount: %v", err)
	}
	if fi.Size() != int64(len(payload)) {
		t.Errorf("size through mount = %d, want %d", fi.Size(), len(payload))
	}
	got, err := os.ReadFile(through)
	if err != nil {
		t.Fatalf("read through mount: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("bytes through mount = %q, want %q", got, payload)
	}

	if err := h.Unmount(); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	if Mounted(mnt) {
		t.Fatalf("%s still Mounted() after Unmount — wedge", mnt)
	}
}

// TestLiveCacheDefeatMtime proves the cache-defeat per-version mtime override
// survives the fuse-t/NFS round-trip.
func TestLiveCacheDefeatMtime(t *testing.T) {
	if os.Getenv("FUSEKIT_LIVE") != "1" {
		t.Skip("set FUSEKIT_LIVE=1 for live fuse-t mount tests")
	}
	src, mnt := liveScratch(t)
	forceCleanupMount(t, mnt)

	const name = "doc.md"
	if err := os.WriteFile(filepath.Join(src, name), []byte("body"), 0o600); err != nil {
		t.Fatalf("seed backing file: %v", err)
	}

	// version is atomic: the fuse serving goroutine reads it. Stored to 1 before
	// Mount so the readiness probe's stat can't let the NFS client cache a
	// version-0 mtime.
	var version atomic.Int64
	seedFor := func(v int64) string { return fmt.Sprintf("tip-%d", v) }
	version.Store(1)

	h, err := Mount(Config{
		Base:      src,
		Dir:       mnt,
		FS:        &probeFS{root: src},
		Options:   MountOptions{Volname: "fusekit-live-cd", NoBrowse: true}.Build(),
		Ready:     func() bool { return MountAlive(src, mnt) },
		Wait:      probeWait,
		FirstWait: probeFirstWait,
		CacheDefeat: &CacheDefeat{
			VersionSeed: func(path string, _ *fuse.Stat_t) string {
				if filepath.Base(path) == name {
					return seedFor(version.Load())
				}
				return ""
			},
		},
	})
	if err != nil {
		t.Fatalf("mount: %v", err)
	}

	through := filepath.Join(mnt, name)

	want1 := VersionNsec(seedFor(1))
	nsec1, err := statMtimeNsec(through)
	if err != nil {
		t.Fatalf("stat at version 1: %v", err)
	}
	if nsec1 != want1 {
		t.Fatalf("Mtim.Nsec at version 1 = %d, want VersionNsec(%q) = %d", nsec1, seedFor(1), want1)
	}

	version.Store(2)
	want2 := VersionNsec(seedFor(2))
	if want1 == want2 {
		t.Fatalf("test bug: seeds %q and %q hash to the same VersionNsec %d; pick distinct seeds", seedFor(1), seedFor(2), want1)
	}
	nsec2 := pollMtimeNsec(t, through, want2)
	if nsec2 == nsec1 {
		t.Fatalf("Mtim.Nsec unchanged across the version bump: %d — the cache-defeat override did not round-trip", nsec1)
	}

	if err := h.Unmount(); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	if Mounted(mnt) {
		t.Fatalf("%s still Mounted() after Unmount — wedge", mnt)
	}
}
