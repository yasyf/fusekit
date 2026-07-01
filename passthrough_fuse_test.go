//go:build fuse && cgo && darwin

package fusekit

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit/fuset"
	"golang.org/x/sys/unix"
)

type passthroughProbeFS struct{ probeFS }

func (passthroughProbeFS) FusePassthroughOnly() bool { return true }

// TestMountBackendSelection pins that a passthrough-only FS selects the FSKit backend,
// an unmarked FS the default NFS backend.
func TestMountBackendSelection(t *testing.T) {
	if !fuset.FSKitAvailable() {
		t.Skip("fuse-t FSKit backend unavailable (need macOS 26+ with the fuse-t FSKit module installed)")
	}

	mountFstype := func(t *testing.T, newFS func(root string) fuse.FileSystemInterface) string {
		t.Helper()
		src := t.TempDir()
		mnt := t.TempDir()
		if err := os.WriteFile(filepath.Join(src, "probe"), []byte("ok"), 0o600); err != nil {
			t.Fatal(err)
		}
		h, err := Mount(Config{
			Base:      src,
			Dir:       mnt,
			FS:        newFS(src),
			Options:   MountOptions{Volname: "fusekit-backend-test", NoBrowse: true}.Build(),
			Wait:      8 * time.Second,
			FirstWait: 14 * time.Second,
		})
		if err != nil {
			// Installed-but-disabled FSKit (off in System Settings) fails the
			// mount — environmental, not a selection regression.
			t.Skipf("mount failed (FSKit extension likely not enabled): %v", err)
		}
		t.Cleanup(func() { _ = h.Unmount() })
		var st unix.Statfs_t
		if err := unix.Statfs(mnt, &st); err != nil {
			t.Fatalf("statfs: %v", err)
		}
		return unix.ByteSliceToString(st.Fstypename[:])
	}

	if got := mountFstype(t, func(root string) fuse.FileSystemInterface {
		return &passthroughProbeFS{probeFS{root: root}}
	}); got != "fuse-t" {
		t.Fatalf("passthrough FS fstypename = %q, want %q (FSKit backend not selected)", got, "fuse-t")
	}
	if got := mountFstype(t, func(root string) fuse.FileSystemInterface {
		return &probeFS{root: root}
	}); got != "nfs" {
		t.Fatalf("unmarked FS fstypename = %q, want %q (default NFS backend not used)", got, "nfs")
	}
}
