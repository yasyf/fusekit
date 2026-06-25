//go:build fuse && cgo

// This file holds the FUSEKIT_LIVE-gated live holder round-trip test. It proves
// the mount-holder wire protocol round-trips against a REAL fuse-t mount: a
// mountd.Server driving a real *fusekit.MountSet (which mounts a probeFS
// passthrough), exercised end-to-end over a unix socket by a mountd.Client —
// Health, Mount, List, an idempotent re-Mount, Unmount, Shutdown, WaitGone.
//
// It lives in package fusekit_test (external), NOT package fusekit, because it
// must import mountd, and mountd imports fusekit — so an internal `package
// fusekit` test importing mountd is an import cycle. An external test package is
// a leaf (nothing imports it), so it may import both fusekit and mountd. It
// reuses the unexported probeFS through fusekit.NewLiveProbeFS, the test-only
// bridge declared in live_test.go (same directory, same test binary).
//
// It also stays in the ROOT directory rather than mountd/ so the project's live
// run command — `go test -tags fuse -run Live .` — which targets the root
// package, actually exercises it.
//
// SAFETY: the holder shuts down gracefully (no kill -9), and a t.Cleanup
// force-unmounts + clears the carcass of the scratch mountpoint on every exit
// path so a failed test can never strand a wedged fuse-t mount. The scratch root
// is under /tmp (short enough for the socket's sun_path limit; never ~/.claude).

package fusekit_test

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/mountd"
)

func TestLiveHolderRoundTrip(t *testing.T) {
	if os.Getenv("FUSEKIT_LIVE") != "1" {
		t.Skip("set FUSEKIT_LIVE=1 for live fuse-t mount tests")
	}

	root, err := os.MkdirTemp("/tmp", "fusekit-holder-")
	if err != nil {
		t.Fatalf("scratch root: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	src := filepath.Join(root, "src")
	mnt := filepath.Join(root, "mnt")
	for _, d := range []string{src, mnt} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	socket := filepath.Join(root, "m.sock")

	// Force-cleanup safety net, registered AFTER the RemoveAll cleanup so it runs
	// FIRST (LIFO): unmount the scratch mountpoint, then remove the dir. A
	// stranded wedged fuse-t mount can freeze the machine.
	t.Cleanup(func() {
		_ = fusekit.ForceUnmount(mnt)
		_ = fusekit.ClearCarcass(mnt)
	})

	const probeName, probeBody = "probe.txt", "ok"
	if err := os.WriteFile(filepath.Join(src, probeName), []byte(probeBody), 0o600); err != nil {
		t.Fatalf("seed backing file: %v", err)
	}

	// The real in-process fuse host the holder drives: a MountSet that mounts a
	// probeFS passthrough of base at dir, with kernel-truth liveness from the
	// package's own Mounted + MountAlive.
	host := &fusekit.MountSet{
		Build: func(spec fusekit.MountSpec) (fusekit.Config, error) {
			return fusekit.Config{
				Base:      spec.Base,
				Dir:       spec.Dir,
				FS:        fusekit.NewLiveProbeFS(spec.Base),
				Options:   fusekit.MountOptions{Volname: "fusekit-holder", NoBrowse: true}.Build(),
				Ready:     func() bool { return fusekit.MountAlive(spec.Base, spec.Dir) },
				Wait:      8 * time.Second,
				FirstWait: 14 * time.Second,
			}, nil
		},
		StateFn: func(base, dir string) (mounted, alive bool) {
			return fusekit.Mounted(dir), fusekit.MountAlive(base, dir)
		},
	}

	srv := &mountd.Server{
		Socket:  socket,
		Host:    host,
		Version: "vLIVE",
		Log:     log.New(io.Discard, "", 0),
	}
	ctx, cancel := context.WithCancel(context.Background())
	// defer cancel ends Run even on an early t.Fatalf: ctx-cancel makes Run stop
	// accepting, drain, and sweep its own mounts down — belt to the force-cleanup
	// braces above.
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	cl := mountd.NewClient(socket)
	waitUp(t, cl)

	ver, err := cl.Health()
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if ver != "vLIVE" {
		t.Fatalf("health version = %q, want vLIVE", ver)
	}

	if err := cl.Mount(src, mnt); err != nil {
		t.Fatalf("mount: %v", err)
	}

	mounts, err := cl.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(mounts) != 1 {
		t.Fatalf("list = %+v, want exactly one row", mounts)
	}
	if m := mounts[0]; m.Dir != mnt || m.Base != src || !m.Live {
		t.Fatalf("list row = %+v, want live %s <- %s", m, mnt, src)
	}

	// The mount really serves: read the seeded file THROUGH the holder-established
	// mount.
	if got, err := os.ReadFile(filepath.Join(mnt, probeName)); err != nil || string(got) != probeBody {
		t.Fatalf("read through holder mount = %q (err %v), want %q", got, err, probeBody)
	}

	// A second Mount of the exact live pair is an idempotent OK.
	if err := cl.Mount(src, mnt); err != nil {
		t.Fatalf("idempotent re-mount: %v", err)
	}

	if err := cl.Unmount(src, mnt); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	if fusekit.Mounted(mnt) {
		t.Fatalf("%s still Mounted() after holder Unmount — wedge", mnt)
	}

	failed, err := cl.Shutdown()
	if err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if len(failed) != 0 {
		t.Fatalf("shutdown reported failed dirs %+v, want a clean sweep", failed)
	}
	if !cl.WaitGone(5 * time.Second) {
		t.Fatal("holder socket still live after shutdown")
	}
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
}

// waitUp blocks until the holder socket accepts a connection or a deadline
// elapses.
func waitUp(t *testing.T, cl *mountd.Client) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cl.Available() {
		if time.Now().After(deadline) {
			t.Fatal("holder socket never came up")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
