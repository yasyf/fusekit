package state

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirPaths(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	d := Dir{App: "cc-pool"}
	if got, want := d.Root(), filepath.Join(home, ".cc-pool"); got != want {
		t.Errorf("Root() = %q, want %q", got, want)
	}
	if got, want := d.Path("daemon.sock"), filepath.Join(home, ".cc-pool", "daemon.sock"); got != want {
		t.Errorf("Path() = %q, want %q", got, want)
	}
}

func TestDirEnsure(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	d := Dir{App: "cc-squash"}
	if err := d.Ensure(); err != nil {
		t.Fatalf("Ensure() = %v", err)
	}
	info, err := os.Stat(d.Root())
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("Root() is not a directory")
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Errorf("Root() perm = %o, want 700", perm)
	}
}

func TestAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "status.json") // a missing parent dir is created
	data := []byte(`{"ok":true}`)
	if err := AtomicWrite(path, data, 0o600); err != nil {
		t.Fatalf("AtomicWrite() = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("content = %q, want %q", got, data)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("perm = %o, want 600", perm)
	}
	if entries, _ := os.ReadDir(filepath.Dir(path)); len(entries) != 1 {
		t.Errorf("dir has %d entries, want 1 (the temp file must not survive)", len(entries))
	}
}
