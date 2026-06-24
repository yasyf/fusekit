package fileproviderd

import (
	"fmt"
	"os"
	"path/filepath"
)

// AtomicSymlink ensures linkPath is a symlink pointing at target, swapping it
// into place atomically and refusing — fail CLOSED — to clobber a path that
// exists and is NOT already a symlink.
//
// This is the bridge a File Provider overlay needs: the OS surfaces a domain
// under ~/Library/CloudStorage/<App>-<Name>/, but the consumer's canonical
// account dir string (which it hashes for a Keychain service name, byte for
// byte) must stay put. The fix is to make the account dir a symlink INTO the
// domain root. That account dir may already hold a real directory of account
// state from a prior symlink/FUSE backend, so the clobber guard is
// safety-critical: a bare os.Symlink-after-os.Remove would delete real account
// data. AtomicSymlink removes only a symlink it is replacing; anything else is
// refused loudly.
//
// The swap is atomic: the new link is created at a sibling temp path and
// renamed over linkPath, so a concurrent reader never observes a missing or
// half-written link, and a stale target is replaced without a remove-then-create
// gap. An already-correct link is a no-op (no needless rename).
func AtomicSymlink(linkPath, target string) error {
	if linkPath == "" || target == "" {
		return fmt.Errorf("AtomicSymlink: linkPath and target are required")
	}
	switch cur, err := os.Readlink(linkPath); {
	case err == nil && cur == target:
		return nil // already correct
	case err != nil:
		// Not a symlink (or absent). If something IS there and it is not a
		// symlink, refuse: it may be a real account dir whose destruction this
		// guard exists to prevent.
		if _, statErr := os.Lstat(linkPath); statErr == nil {
			return fmt.Errorf("refusing to replace %q with a symlink: a non-symlink already exists there", linkPath)
		}
	}

	if err := os.MkdirAll(filepath.Dir(linkPath), 0o700); err != nil {
		return fmt.Errorf("ensure symlink parent dir: %w", err)
	}
	tmp, err := tempSymlink(linkPath, target)
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, linkPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("swap symlink %q -> %q: %w", linkPath, target, err)
	}
	return nil
}

// tempSymlink creates a fresh symlink to target at a unique sibling of linkPath
// and returns its path, for an atomic rename into place. The sibling lives in
// linkPath's own directory so the rename is same-filesystem (atomic). A
// pre-existing temp from a crashed prior call is removed before the create.
func tempSymlink(linkPath, target string) (string, error) {
	for i := 0; ; i++ {
		tmp := fmt.Sprintf("%s.tmplink-%d-%d", linkPath, os.Getpid(), i)
		err := os.Symlink(target, tmp)
		if err == nil {
			return tmp, nil
		}
		if !os.IsExist(err) {
			return "", fmt.Errorf("create temp symlink %q -> %q: %w", tmp, target, err)
		}
		// A leftover temp from a crashed call: a symlink we own, safe to clear.
		if _, lerr := os.Readlink(tmp); lerr == nil {
			if rerr := os.Remove(tmp); rerr != nil {
				return "", fmt.Errorf("remove stale temp symlink %q: %w", tmp, rerr)
			}
			continue
		}
		return "", fmt.Errorf("temp symlink path %q is occupied by a non-symlink", tmp)
	}
}

// RemoveSymlink removes linkPath when (and only when) it is a symlink, returning
// nil for an absent path. It refuses — fail CLOSED — to remove a non-symlink, so
// a Teardown that tears down the bridge can never delete a real account dir that
// somehow occupies the path. The companion app owns deregistering the domain;
// this only retracts the bridge link.
func RemoveSymlink(linkPath string) error {
	fi, err := os.Lstat(linkPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lstat %q: %w", linkPath, err)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		return fmt.Errorf("refusing to remove %q: it is not a symlink", linkPath)
	}
	if err := os.Remove(linkPath); err != nil {
		return fmt.Errorf("remove symlink %q: %w", linkPath, err)
	}
	return nil
}
