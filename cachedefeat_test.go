//go:build fuse && cgo

package fusekit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/winfsp/cgofuse/fuse"
)

// wrap decorates a probeFS (fresh temp dir, one file) with cd, exercised
// directly — no real mount; the logic is pure callback dispatch.
func wrap(t *testing.T, cd CacheDefeat) *cacheDefeatFS {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("hello"), 0o600); err != nil {
		t.Fatalf("seed probe file: %v", err)
	}
	return &cacheDefeatFS{FileSystemInterface: &probeFS{root: dir}, cd: cd}
}

// TestCacheDefeatGetattrMtimeNsec pins that a non-empty VersionSeed overrides
// Getattr's Mtim.Nsec with VersionNsec(seed), and "" leaves it untouched.
func TestCacheDefeatGetattrMtimeNsec(t *testing.T) {
	const seed = "tip-abc123"
	fs := wrap(t, CacheDefeat{
		VersionSeed: func(path string, _ *fuse.Stat_t) string {
			if path == "/file" {
				return seed
			}
			return ""
		},
	})

	var stat fuse.Stat_t
	if rc := fs.Getattr("/file", &stat, ^uint64(0)); rc != 0 {
		t.Fatalf("Getattr(/file) rc = %d, want 0", rc)
	}
	if got, want := stat.Mtim.Nsec, VersionNsec(seed); got != want {
		t.Fatalf("Getattr Mtim.Nsec = %d, want VersionNsec(%q) = %d", got, seed, want)
	}

	// probeFS Nsec comes from the real ModTime (sub-second bits incidental), so
	// prove the empty-seed no-override by comparing against the inner FS.
	var rootStat fuse.Stat_t
	if rc := fs.Getattr("/", &rootStat, ^uint64(0)); rc != 0 {
		t.Fatalf("Getattr(/) rc = %d, want 0", rc)
	}
	var innerRoot fuse.Stat_t
	if rc := fs.FileSystemInterface.Getattr("/", &innerRoot, ^uint64(0)); rc != 0 {
		t.Fatalf("inner Getattr(/) rc = %d, want 0", rc)
	}
	if rootStat.Mtim.Nsec != innerRoot.Mtim.Nsec {
		t.Fatalf("empty seed must leave Nsec untouched: decorated = %d, inner = %d", rootStat.Mtim.Nsec, innerRoot.Mtim.Nsec)
	}
}

// TestCacheDefeatCommitOnFlushAndFsync pins that Commit runs after the inner
// handler on both Flush and Fsync with path+fh forwarded, and a non-zero
// commit errno replaces the inner success rc.
func TestCacheDefeatCommitOnFlushAndFsync(t *testing.T) {
	type call struct {
		path string
		fh   uint64
	}
	var calls []call
	ret := 0
	fs := wrap(t, CacheDefeat{
		Commit: func(path string, fh uint64) int {
			calls = append(calls, call{path, fh})
			return ret
		},
	})

	if rc := fs.Flush("/file", 7); rc != 0 {
		t.Fatalf("Flush rc = %d, want 0", rc)
	}
	if rc := fs.Fsync("/file", false, 7); rc != 0 {
		t.Fatalf("Fsync rc = %d, want 0", rc)
	}
	if len(calls) != 2 {
		t.Fatalf("Commit ran %d times, want 2 (one per Flush, one per Fsync)", len(calls))
	}
	for i, c := range calls {
		if c.path != "/file" || c.fh != 7 {
			t.Fatalf("Commit call %d = %+v, want {/file 7}", i, c)
		}
	}

	ret = -fuse.EIO
	if rc := fs.Flush("/file", 7); rc != -fuse.EIO {
		t.Fatalf("Flush with failing Commit rc = %d, want %d", rc, -fuse.EIO)
	}
	if rc := fs.Fsync("/file", false, 7); rc != -fuse.EIO {
		t.Fatalf("Fsync with failing Commit rc = %d, want %d", rc, -fuse.EIO)
	}
}

// TestCacheDefeatForwardsXattr pins that optional xattr ops promote unchanged
// through the embedded inner FS interface.
func TestCacheDefeatForwardsXattr(t *testing.T) {
	fs := wrap(t, CacheDefeat{})

	rc, val := fs.Getxattr("/file", probeXattrName)
	if rc != len(probeXattrValue) {
		t.Fatalf("Getxattr rc = %d, want %d", rc, len(probeXattrValue))
	}
	if string(val) != probeXattrValue {
		t.Fatalf("Getxattr value = %q, want %q", val, probeXattrValue)
	}

	var listed []string
	if rc := fs.Listxattr("/file", func(name string) bool {
		listed = append(listed, name)
		return true
	}); rc != 0 {
		t.Fatalf("Listxattr rc = %d, want 0", rc)
	}
	if len(listed) != 1 || listed[0] != probeXattrName {
		t.Fatalf("Listxattr listed %v, want [%q]", listed, probeXattrName)
	}
}
