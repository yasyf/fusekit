package overlay_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/yasyf/fusekit/overlay"
)

// ExampleProviderFor_symlink shows the always-available symlink path end to end:
// build a Spec that classifies one private file and one excluded dir, ask
// ProviderFor for the symlink provider (no holder, no fuse), and Setup an account
// dir against a shared base. The resulting account dir links every shared entry,
// makes the excluded entry a private local dir, and never links the private file.
func ExampleProviderFor_symlink() {
	base, err := os.MkdirTemp("", "overlay-example-base-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(base)
	account, err := os.MkdirTemp("", "overlay-example-account-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(account)

	// Seed the shared base: a shared dir, a shared file, a private file, and an
	// excluded dir (which becomes a private local dir in the account).
	if err := os.MkdirAll(filepath.Join(base, "projects"), 0o700); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(filepath.Join(base, "cache"), 0o700); err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(base, "settings.txt"), []byte("shared"), 0o600); err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(base, "identity.json"), []byte("private"), 0o600); err != nil {
		panic(err)
	}

	excluded := map[string]bool{"cache": true}
	spec := overlay.Spec{
		// Private names: the one identity file, plus every excluded name (the
		// invariant ProviderFor relies on — every Excluded name must satisfy
		// IsPrivate).
		IsPrivate: func(name string) bool {
			return name == "identity.json" || excluded[name]
		},
		Excluded:        excluded,
		Shared:          map[string]bool{"projects": true},
		Skip:            map[string]bool{".DS_Store": true},
		PassthroughOnly: false,
	}

	prov, err := overlay.ProviderFor(overlay.BackendSymlink, spec)
	if err != nil {
		panic(err)
	}
	if err := prov.Setup(base, account); err != nil {
		panic(err)
	}

	// Print a deterministic, sorted summary of the account dir's shape. Only the
	// entry name and kind are stable — temp paths and symlink targets are not.
	entries, err := os.ReadDir(account)
	if err != nil {
		panic(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		fi, err := os.Lstat(filepath.Join(account, name))
		if err != nil {
			panic(err)
		}
		var kind string
		switch {
		case fi.Mode()&os.ModeSymlink != 0:
			kind = "symlink"
		case fi.IsDir():
			kind = "private dir"
		default:
			kind = "regular"
		}
		fmt.Printf("%s: %s\n", name, kind)
	}

	// Output:
	// cache: private dir
	// projects: symlink
	// settings.txt: symlink
}
