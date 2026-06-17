//go:build fuse && cgo

// This file holds the FUSEKIT_LIVE-gated live round-trip tests for the
// in-process mount path: they prove the library actually mounts on a real
// fuse-t mount and that the cache-defeat decorator's per-version mtime override
// survives the kernel/NFS round-trip. They are GATED: each self-skips unless
// FUSEKIT_LIVE=1, so a normal `go test -tags fuse ./...` run never touches a
// real mount. fuse-t must be present and its library pinned by the caller
// (CGOFUSE_LIBFUSE_PATH=/usr/local/lib/libfuse-t.dylib on macOS).
//
// SAFETY: every live mount is force-cleaned on EVERY exit path (forceCleanupMount
// registers a t.Cleanup that runs fusekit.ForceUnmount + ClearCarcass against a
// SCRATCH temp mountpoint only) — a stranded wedged fuse-t mount can freeze the
// machine, and a failed/panicked test must never leave one behind. No holder is
// ever kill -9'd here. The scratch dirs live under /tmp (never ~/.claude, never
// a real pool account dir).
//
// The holder live round-trip (TestLiveHolderRoundTrip) lives in
// holder_live_test.go as package fusekit_test, not here: it must import the
// mountd package, and an internal `package fusekit` test importing mountd is an
// import cycle (mountd imports fusekit). The external test reuses the unexported
// probeFS through NewLiveProbeFS below — a test-only bridge, never compiled into
// the production binary.

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

// NewLiveProbeFS is a test-only bridge exposing the unexported probeFS
// passthrough to the external holder live test (holder_live_test.go,
// package fusekit_test), which cannot reach probeFS across the package
// boundary. It lives in this _test.go file, so it is never part of the
// production fusekit package. root is the directory the returned FS mirrors.
func NewLiveProbeFS(root string) fuse.FileSystemInterface { return &probeFS{root: root} }

// liveScratch makes a fresh scratch root under /tmp with src and mnt subdirs,
// registering its removal. /tmp (not t.TempDir()) keeps paths short — the holder
// socket in the external test must fit macOS's 104-byte sun_path limit, and a
// uniform scratch root keeps every live mountpoint off ~/.claude and the real
// pool. The mountpoint returned is always a throwaway, the only kind
// forceCleanupMount may force-unmount.
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

// forceCleanupMount registers a t.Cleanup that force-unmounts dir and clears any
// dead-mount carcass, so a failed or panicked live test never strands a wedged
// fuse-t mount. It is registered AFTER liveScratch's removal cleanup, so it runs
// FIRST (LIFO): unmount, then remove the dir. dir MUST be a scratch temp
// mountpoint — never a real account dir or ~/.claude.
func forceCleanupMount(t *testing.T, dir string) {
	t.Helper()
	t.Cleanup(func() {
		_ = ForceUnmount(dir)
		_ = ClearCarcass(dir)
	})
}

// statMtimeNsec returns the nanosecond component of path's mtime as seen through
// the mount. ModTime().Nanosecond() reads the same Mtim.Nsec the cache-defeat
// decorator writes, portably (no platform Stat_t fields), so this file compiles
// under `fuse && cgo` on any GOOS even though fuse-t itself is macOS.
func statMtimeNsec(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return int64(fi.ModTime().Nanosecond()), nil
}

// pollMtimeNsec stats path through the mount until its mtime nanoseconds equal
// want or a 30s deadline elapses, returning the matched value. It absorbs the
// NFS attribute-cache lag after a version bump: even with noattrcache the macOS
// client holds a just-cached mtime for a few seconds (observed ~5s) before it
// revalidates, so the poll returns the instant the new value surfaces and the
// generous ceiling only bounds a genuine failure — the cache-defeat override
// never round-tripping through the mount at all.
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

// TestLiveMountRoundTrip mounts a probeFS passthrough at a scratch mountpoint,
// writes a file into the backing dir, then stats and reads it THROUGH the mount
// and asserts the bytes match — proving the library actually serves a live
// fuse-t mount. It then unmounts and asserts the mountpoint is no longer Mounted
// (a clean teardown, no wedge).
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

// TestLiveCacheDefeatMtime mounts a probeFS wrapped by the cache-defeat
// decorator with a VersionSeed that bumps per "version", then stats one path
// across a version bump and asserts the mtime nanoseconds changed to
// VersionNsec(seed). It proves the per-version mtime override survives the
// fuse-t/NFS round-trip — the whole point of the decorator (forcing an NFS
// client to revalidate even when a save lands in the same wall-clock second).
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

	// version flips between two seeds; the decorator folds the current seed into
	// the stat's mtime nanoseconds on every Getattr. Read from the fuse serving
	// goroutine, so the counter is atomic. It is set to 1 BEFORE Mount so the
	// mount-up readiness probe (which stats the file) never observes the zero
	// "version 0" — otherwise the NFS client can cache that version-0 mtime and
	// serve it for the immediate first stat below.
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

	// Bump the version. noattrcache is forced on darwin, but the NFS client may
	// briefly hold the version-1 attributes it just cached for nsec1, so poll a
	// short window for the revalidated, version-2 mtime to surface.
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
