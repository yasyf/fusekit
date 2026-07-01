//go:build fuse && cgo

// FUSEKIT_LIVE-gated round trip of the mount-holder wire protocol against a
// real fuse-t mount. External test package: mountd imports fusekit, so an
// internal test importing mountd would cycle; lives at the repo root so
// `go test -tags fuse -run Live .` exercises it.
//
// SAFETY: the holder shuts down gracefully (no kill -9) and a t.Cleanup
// force-unmounts + clears the carcass on every exit path, so a failed run
// cannot strand a wedged fuse-t mount. Scratch root is under /tmp: short
// enough for the socket's sun_path limit, and never ~/.claude.

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

	// Registered after RemoveAll so it runs first (LIFO): unmount before the
	// dir is removed. A stranded wedged fuse-t mount can freeze the machine.
	t.Cleanup(func() {
		_ = fusekit.ForceUnmount(mnt)
		_ = fusekit.ClearCarcass(mnt)
	})

	const probeName, probeBody = "probe.txt", "ok"
	if err := os.WriteFile(filepath.Join(src, probeName), []byte(probeBody), 0o600); err != nil {
		t.Fatalf("seed backing file: %v", err)
	}

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
	// Belt to the force-unmount cleanup: ctx-cancel makes Run sweep its own
	// mounts down even on an early t.Fatalf.
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

	if got, err := os.ReadFile(filepath.Join(mnt, probeName)); err != nil || string(got) != probeBody {
		t.Fatalf("read through holder mount = %q (err %v), want %q", got, err, probeBody)
	}

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
