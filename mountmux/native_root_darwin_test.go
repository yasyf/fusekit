//go:build darwin && cgo && fuse

package mountmux

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRequireNativePresentationRootChecksMountTableBeforeLeaf(t *testing.T) {
	lstatCalled := false
	err := requireNativePresentationRoot("/private/tmp/native", func(string) (bool, error) {
		return true, nil
	}, func(string) (os.FileInfo, error) {
		lstatCalled = true
		return nil, errors.New("must not inspect mounted root")
	})
	if err == nil {
		t.Fatal("mounted native root accepted")
	}
	if lstatCalled {
		t.Fatal("mounted native root was traversed")
	}
}

func TestRequireNativePresentationRootAcceptsExactPrivateDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "mount")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := requireNativePresentationRoot(root, func(string) (bool, error) {
		return false, nil
	}, os.Lstat); err != nil {
		t.Fatal(err)
	}
}

func TestRequireNativePresentationRootRejectsUnsafeLeaf(t *testing.T) {
	parent := t.TempDir()
	regular := filepath.Join(parent, "regular")
	if err := os.WriteFile(regular, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	open := filepath.Join(parent, "open")
	if err := os.Mkdir(open, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(open, link); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(parent, "missing")
	for _, root := range []string{regular, open, link, missing} {
		if err := requireNativePresentationRoot(root, func(string) (bool, error) {
			return false, nil
		}, os.Lstat); err == nil {
			t.Fatalf("unsafe native root %q accepted", root)
		}
	}
}

func TestNativeMountpointPresentFindsRoot(t *testing.T) {
	mounted, err := nativeMountpointPresent("/")
	if err != nil {
		t.Fatal(err)
	}
	if !mounted {
		t.Fatal("root mountpoint not found")
	}
}
