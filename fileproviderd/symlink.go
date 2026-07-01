package fileproviderd

import (
	"fmt"
	"os"
	"path/filepath"
)

// AtomicSymlink ensures linkPath is a symlink to target, swapping it in
// atomically and refusing — fail CLOSED — to clobber a non-symlink (it removes
// only a symlink it replaces).
//
// It bridges a File Provider overlay: the OS surfaces the domain under
// ~/Library/CloudStorage/, but the consumer's canonical account-dir string,
// hashed byte-for-byte into a Keychain service name, must stay put — so that dir
// becomes a symlink into the domain root. The clobber guard is safety-critical:
// the dir may hold real account state from a prior backend that a bare
// remove-then-symlink would destroy. The sibling-temp-then-rename swap never
// exposes a missing or half-written link; an already-correct link is a no-op.
func AtomicSymlink(linkPath, target string) error {
	if linkPath == "" || target == "" {
		return fmt.Errorf("AtomicSymlink: linkPath and target are required")
	}
	switch cur, err := os.Readlink(linkPath); {
	case err == nil && cur == target:
		return nil // already correct
	case err != nil:
		// Readlink failed: absent, or present but not a symlink — refuse the latter.
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
// (same directory, so the rename into place is same-filesystem and atomic),
// returning its path.
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

// RemoveSymlink removes linkPath only when it is a symlink (nil for an absent
// path), refusing — fail CLOSED — to remove a non-symlink so a bridge Teardown
// can never delete a real account dir occupying the path. The companion app owns
// deregistering the domain; this only retracts the bridge link.
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
