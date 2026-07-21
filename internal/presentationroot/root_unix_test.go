//go:build darwin || linux

package presentationroot

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareCreatesExactPresentationRoot(t *testing.T) {
	root := filepath.Join(realTempDir(t), "mount")
	if err := Prepare(root); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		t.Fatalf("created root = mode %v", info.Mode())
	}
	if err := Validate(root); err != nil {
		t.Fatalf("Validate(created root) = %v", err)
	}
	if err := Prepare(root); err != nil {
		t.Fatalf("Prepare(existing safe root) = %v", err)
	}
}

func TestPresentationRootRejectsUnsafeState(t *testing.T) {
	t.Run("non-exact path", func(t *testing.T) {
		root := filepath.Join(realTempDir(t), "nested") + string(filepath.Separator) + ".." + string(filepath.Separator) + "mount"
		if err := Prepare(root); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Prepare = %v, want ErrInvalid", err)
		}
	})
	t.Run("symlink leaf", func(t *testing.T) {
		parent := realTempDir(t)
		target := filepath.Join(parent, "target")
		if err := os.Mkdir(target, 0o700); err != nil {
			t.Fatal(err)
		}
		root := filepath.Join(parent, "mount")
		if err := os.Symlink(target, root); err != nil {
			t.Fatal(err)
		}
		assertInvalid(t, root)
	})
	t.Run("symlink ancestor", func(t *testing.T) {
		parent := realTempDir(t)
		target := filepath.Join(parent, "target")
		if err := os.Mkdir(target, 0o700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(parent, "link")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		assertInvalid(t, filepath.Join(link, "mount"))
	})
	t.Run("non-directory", func(t *testing.T) {
		root := filepath.Join(realTempDir(t), "mount")
		if err := os.WriteFile(root, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		assertInvalid(t, root)
	})
	t.Run("wrong owner", func(t *testing.T) {
		root := existingRoot(t)
		original := currentUID
		currentUID = func() int { return original() + 1 }
		t.Cleanup(func() { currentUID = original })
		assertInvalid(t, root)
	})
	t.Run("wrong permissions", func(t *testing.T) {
		root := existingRoot(t)
		if err := os.Chmod(root, 0o755); err != nil {
			t.Fatal(err)
		}
		assertInvalid(t, root)
	})
	t.Run("nonempty", func(t *testing.T) {
		root := existingRoot(t)
		if err := os.WriteFile(filepath.Join(root, "entry"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		assertInvalid(t, root)
	})
	t.Run("already mounted", func(t *testing.T) {
		root := existingRoot(t)
		original := mountedAt
		mountedAt = func(path string) (bool, error) { return path == root, nil }
		t.Cleanup(func() { mountedAt = original })
		assertInvalid(t, root)
	})
	t.Run("mount table failure", func(t *testing.T) {
		root := existingRoot(t)
		original := mountedAt
		mountedAt = func(string) (bool, error) { return false, errors.New("unavailable") }
		t.Cleanup(func() { mountedAt = original })
		assertInvalid(t, root)
	})
}

func assertInvalid(t *testing.T, root string) {
	t.Helper()
	if err := Prepare(root); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Prepare(%q) = %v, want ErrInvalid", root, err)
	}
	if err := Validate(root); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Validate(%q) = %v, want ErrInvalid", root, err)
	}
}

func existingRoot(t *testing.T) string {
	t.Helper()
	root := filepath.Join(realTempDir(t), "mount")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func realTempDir(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/private/tmp", "fusekit-presentation-root-")
	if err != nil {
		root, err = os.MkdirTemp("/tmp", "fusekit-presentation-root-")
	}
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(resolved) })
	return resolved
}
