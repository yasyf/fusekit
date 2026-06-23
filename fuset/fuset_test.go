package fuset

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInstalledStatsThePath(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "libfuse-t.dylib")
	if err := os.WriteFile(present, []byte("stub"), 0o644); err != nil {
		t.Fatal(err)
	}
	absent := filepath.Join(dir, "missing.dylib")

	if !installed(present) {
		t.Errorf("installed(%q) = false, want true for an existing file", present)
	}
	if installed(absent) {
		t.Errorf("installed(%q) = true, want false for a missing file", absent)
	}
}

// A broken symlink must read as not-installed: os.Stat follows the link and
// fails, which is the right answer — a dangling libfuse-t link cannot be
// dlopened.
func TestInstalledBrokenSymlinkIsAbsent(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "libfuse-t.dylib")
	if err := os.Symlink(filepath.Join(dir, "nonexistent"), link); err != nil {
		t.Fatal(err)
	}
	if installed(link) {
		t.Errorf("installed(broken symlink) = true, want false")
	}
}

func TestConstantsAreTheFuseTFacts(t *testing.T) {
	if Cask != "macos-fuse-t/homebrew-cask/fuse-t" {
		t.Errorf("Cask = %q", Cask)
	}
	if Dylib != "/usr/local/lib/libfuse-t.dylib" {
		t.Errorf("Dylib = %q", Dylib)
	}
}
