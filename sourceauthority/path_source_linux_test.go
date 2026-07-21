//go:build linux

package sourceauthority

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLinuxRootIdentitySurvivesOwnedNamespaceMutation(t *testing.T) {
	t.Parallel()
	rootPath := canonicalTemporaryDirectory(t)
	root := testPinnedRoot(t, RootSpec{
		Authority: "authority", ID: "root", Path: rootPath, Kind: RootDirectory, Generation: 1,
	})
	if err := os.WriteFile(filepath.Join(rootPath, "value"), []byte("value"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateRootPathStillPinned(root); err != nil {
		t.Fatalf("owned namespace mutation changed root identity: %v", err)
	}
}

func TestLinuxObjectIdentitySurvivesRename(t *testing.T) {
	t.Parallel()
	rootPath := canonicalTemporaryDirectory(t)
	from := filepath.Join(rootPath, "from")
	if err := os.WriteFile(from, []byte("value"), 0o600); err != nil {
		t.Fatal(err)
	}
	root := testPinnedRoot(t, RootSpec{
		Authority: "authority", ID: "root", Path: rootPath, Kind: RootDirectory, Generation: 1,
	})
	before, err := (securePathSource{}).Stat(t.Context(), root, "from")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(from, filepath.Join(rootPath, "to")); err != nil {
		t.Fatal(err)
	}
	after, err := (securePathSource{}).Stat(t.Context(), root, "to")
	if err != nil {
		t.Fatal(err)
	}
	if before.Identity != after.Identity {
		t.Fatalf("identity changed across rename: before %+v after %+v", before.Identity, after.Identity)
	}
}
