package holder

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPreparePresentationRootCreatesPrivateLeaf(t *testing.T) {
	runtimeDirectory := t.TempDir()
	root := filepath.Join(runtimeDirectory, "mount")
	if err := preparePresentationRoot(runtimeDirectory, root); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("presentation root mode = %v, want private directory", info.Mode())
	}
}

func TestPreparePresentationRootDoesNotTraverseExistingLeaf(t *testing.T) {
	runtimeDirectory := t.TempDir()
	root := filepath.Join(runtimeDirectory, "mount")
	if err := os.Mkdir(root, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := preparePresentationRoot(runtimeDirectory, root); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o711 {
		t.Fatalf("existing presentation root mode = %#o, want unchanged 0711", info.Mode().Perm())
	}
}

func TestPreparePresentationRootRejectsNonChild(t *testing.T) {
	runtimeDirectory := t.TempDir()
	if err := preparePresentationRoot(runtimeDirectory, filepath.Join(runtimeDirectory, "nested", "mount")); err == nil {
		t.Fatal("nested presentation root accepted")
	}
}
