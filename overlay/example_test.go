package overlay_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/yasyf/fusekit/overlay"
)

// ExampleProviderFor_symlink shows the symlink path end to end: a Spec classifying
// one private file and one excluded dir yields an account dir that links every
// shared entry, makes the excluded entry a private local dir, and never links the
// private file.
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
		// Every Excluded name must also satisfy IsPrivate — an invariant ProviderFor
		// relies on.
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
	if err := prov.Reconcile(context.Background(), base, account); err != nil {
		panic(err)
	}

	// Sorted name+kind summary only: temp paths and symlink targets are not stable
	// enough for the Output block.
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
