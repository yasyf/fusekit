package fileproviderd

import (
	"os"
	"path/filepath"
	"testing"
)

// TestAtomicSymlinkCreates pins the happy path: a fresh link path becomes a
// symlink to target.
func TestAtomicSymlinkCreates(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "cloud-root")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "acct-01")
	if err := AtomicSymlink(link, target); err != nil {
		t.Fatalf("AtomicSymlink = %v, want nil", err)
	}
	got, err := os.Readlink(link)
	if err != nil {
		t.Fatalf("Readlink = %v, want the link", err)
	}
	if got != target {
		t.Fatalf("link target = %q, want %q", got, target)
	}
}

// TestAtomicSymlinkIdempotent pins that an already-correct link is a no-op.
func TestAtomicSymlinkIdempotent(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "cloud-root")
	link := filepath.Join(dir, "acct-01")
	if err := AtomicSymlink(link, target); err != nil {
		t.Fatal(err)
	}
	if err := AtomicSymlink(link, target); err != nil {
		t.Fatalf("second AtomicSymlink = %v, want nil (idempotent)", err)
	}
}

// TestAtomicSymlinkReplacesStaleLink pins that a symlink pointing elsewhere is
// atomically retargeted — but ONLY a symlink, never real data.
func TestAtomicSymlinkReplacesStaleLink(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "acct-01")
	if err := os.Symlink(filepath.Join(dir, "old"), link); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "new")
	if err := AtomicSymlink(link, want); err != nil {
		t.Fatalf("AtomicSymlink over a stale link = %v, want nil", err)
	}
	got, _ := os.Readlink(link)
	if got != want {
		t.Fatalf("link target = %q, want %q", got, want)
	}
}

// TestAtomicSymlinkRefusesToClobberRealDir is the safety-critical case: the link
// path holds a REAL directory of account state (a prior symlink/FUSE backend).
// AtomicSymlink must refuse and leave the dir and its contents untouched — a
// bare os.Symlink-after-os.Remove would have destroyed it.
func TestAtomicSymlinkRefusesToClobberRealDir(t *testing.T) {
	dir := t.TempDir()
	acct := filepath.Join(dir, "acct-01")
	if err := os.MkdirAll(acct, 0o700); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(acct, "credentials")
	if err := os.WriteFile(keep, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := AtomicSymlink(acct, filepath.Join(dir, "cloud-root"))
	if err == nil {
		t.Fatal("AtomicSymlink over a real dir succeeded, want a fail-closed refusal")
	}
	if !contains(err.Error(), "non-symlink already exists") {
		t.Errorf("err = %v, want the non-symlink refusal", err)
	}
	// The real dir and its file must survive untouched.
	if data, rerr := os.ReadFile(keep); rerr != nil || string(data) != "secret" {
		t.Fatalf("account state was destroyed: data=%q err=%v", data, rerr)
	}
}

// TestAtomicSymlinkRefusesToClobberRealFile pins the same guard for a plain file
// at the link path.
func TestAtomicSymlinkRefusesToClobberRealFile(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "acct-01")
	if err := os.WriteFile(link, []byte("real"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := AtomicSymlink(link, filepath.Join(dir, "cloud-root")); err == nil {
		t.Fatal("AtomicSymlink over a real file succeeded, want refusal")
	}
	if data, _ := os.ReadFile(link); string(data) != "real" {
		t.Fatalf("real file clobbered: %q", data)
	}
}

func TestAtomicSymlinkValidatesArgs(t *testing.T) {
	if err := AtomicSymlink("", "/target"); err == nil {
		t.Error("empty linkPath accepted, want error")
	}
	if err := AtomicSymlink("/link", ""); err == nil {
		t.Error("empty target accepted, want error")
	}
}

// TestRemoveSymlink pins that RemoveSymlink retracts a link, no-ops an absent
// path, and refuses — fail closed — to remove a real dir.
func TestRemoveSymlink(t *testing.T) {
	t.Run("removes a symlink", func(t *testing.T) {
		dir := t.TempDir()
		link := filepath.Join(dir, "acct-01")
		if err := os.Symlink(filepath.Join(dir, "x"), link); err != nil {
			t.Fatal(err)
		}
		if err := RemoveSymlink(link); err != nil {
			t.Fatalf("RemoveSymlink = %v, want nil", err)
		}
		if _, err := os.Lstat(link); !os.IsNotExist(err) {
			t.Fatalf("link still present: %v", err)
		}
	})
	t.Run("absent path is a no-op", func(t *testing.T) {
		if err := RemoveSymlink(filepath.Join(t.TempDir(), "missing")); err != nil {
			t.Fatalf("RemoveSymlink on a missing path = %v, want nil", err)
		}
	})
	t.Run("refuses to remove a real dir", func(t *testing.T) {
		dir := t.TempDir()
		acct := filepath.Join(dir, "acct-01")
		if err := os.MkdirAll(acct, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := RemoveSymlink(acct); err == nil {
			t.Fatal("RemoveSymlink on a real dir succeeded, want refusal")
		}
		if _, err := os.Stat(acct); err != nil {
			t.Fatalf("real dir was removed: %v", err)
		}
	})
}
