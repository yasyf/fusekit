package overlay

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/fusekit"
)

// SymlinkProvider symlinks each top-level entry of base into accountDir, except
// Spec's Excluded entries (which get private empty dirs) and Skipped entries.
type SymlinkProvider struct {
	// Spec classifies each top-level entry.
	Spec Spec
}

var _ Provider = (*SymlinkProvider)(nil)

// Backend reports BackendSymlink.
func (p *SymlinkProvider) Backend() Backend { return BackendSymlink }

// PrivateRoot is accountDir itself: private files live alongside the symlinks.
func (p *SymlinkProvider) PrivateRoot(accountDir string) string { return accountDir }

// Reconcile asserts each top-level entry's shape in accountDir: a symlink for
// shared entries, a private dir for excluded ones, no provider-owned symlink for
// private ones. It also removes stale provider-owned links after a base entry is
// removed or reclassified. Ownership is exact: only a link whose target equals
// base/name is removed; foreign symlinks and real data are never clobbered.
// Per-entry failures are joined so one conflict neither blocks nor masks others.
func (p *SymlinkProvider) Reconcile(ctx context.Context, base, accountDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if accountDir == base || accountDir == "" {
		return fmt.Errorf("refusing to overlay base dir %q onto itself", accountDir)
	}
	// Symlinks into a live fuse mirror pass through to real base (non-private
	// paths redirect to base) — refuse.
	if fusekit.Mounted(accountDir) {
		return fmt.Errorf("refusing to lay symlinks in %q: it is a live mountpoint", accountDir)
	}
	// Materialize guaranteed-shared entries so the loop links them even before
	// they exist in base.
	for name := range p.Spec.Shared {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(base, name), 0o700); err != nil {
			return fmt.Errorf("ensure shared base dir %q: %w", name, err)
		}
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return fmt.Errorf("read base dir: %w", err)
	}
	if err := os.MkdirAll(accountDir, 0o700); err != nil {
		return err
	}
	desiredShared := make(map[string]bool, len(entries))
	var errs []error
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		name := e.Name()
		if p.Spec.Skipped(name) {
			continue
		}
		dst := filepath.Join(accountDir, name)
		if p.Spec.Excluded[name] {
			if _, err := removeOwnedSymlink(base, name, dst); err != nil {
				errs = append(errs, err)
				continue
			}
			if err := assertPrivateDir(dst); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if p.Spec.IsPrivate(name) {
			removed, err := removeOwnedSymlink(base, name, dst)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if !removed {
				if target, err := os.Readlink(dst); err == nil {
					errs = append(errs, fmt.Errorf("private entry %q links to %q; refusing to remove a foreign symlink", name, target))
				}
			}
			continue
		}
		desiredShared[name] = true
		if err := assertSymlink(filepath.Join(base, name), dst); err != nil {
			errs = append(errs, err)
		}
	}
	accountEntries, err := os.ReadDir(accountDir)
	if err != nil {
		return errors.Join(append(errs, fmt.Errorf("read account dir: %w", err))...)
	}
	for _, e := range accountEntries {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		if desiredShared[e.Name()] {
			continue
		}
		if _, err := removeOwnedSymlink(base, e.Name(), filepath.Join(accountDir, e.Name())); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Check verifies every shared top-level entry of base is correctly linked in
// accountDir, every excluded entry is a real local dir, and no stale provider-owned
// link remains. It is read-only.
func (p *SymlinkProvider) Check(ctx context.Context, base, accountDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return fmt.Errorf("read base dir: %w", err)
	}
	desiredShared := make(map[string]bool, len(entries))
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := e.Name()
		if p.Spec.Skipped(name) {
			continue
		}
		dst := filepath.Join(accountDir, name)
		if p.Spec.Excluded[name] {
			if fi, err := os.Lstat(dst); err != nil || fi.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("excluded entry %q is missing or a symlink", name)
			}
			continue
		}
		if p.Spec.IsPrivate(name) {
			// Only bad state for a private file: a stale symlink from before the
			// name was classified private.
			if fi, err := os.Lstat(dst); err == nil && fi.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("private entry %q is a symlink (stale shared link; run a heal)", name)
			}
			continue
		}
		desiredShared[name] = true
		target, err := os.Readlink(dst)
		if err != nil {
			return fmt.Errorf("entry %q is not a symlink: %w", name, err)
		}
		if target != filepath.Join(base, name) {
			return fmt.Errorf("entry %q links to %q, want %q", name, target, filepath.Join(base, name))
		}
	}
	accountEntries, err := os.ReadDir(accountDir)
	if err != nil {
		return fmt.Errorf("read account dir: %w", err)
	}
	for _, e := range accountEntries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if desiredShared[e.Name()] {
			continue
		}
		target, err := os.Readlink(filepath.Join(accountDir, e.Name()))
		if err == nil && target == filepath.Join(base, e.Name()) {
			return fmt.Errorf("stale provider symlink %q remains", e.Name())
		}
	}
	return nil
}

// Teardown removes the account dir's overlay — safe because shared entries are
// symlinks (removal never touches base) and excluded entries are the account's
// own private dirs. Refuses base as a guard against misuse. The warning is
// always empty: the symlink backend is fully in-process.
func (p *SymlinkProvider) Teardown(ctx context.Context, base, accountDir string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if accountDir == base || accountDir == "" {
		return "", fmt.Errorf("refusing to tear down base dir %q", accountDir)
	}
	// Through a live fuse mirror, excluded dirs resolve to the private backing
	// root — RemoveAll would destroy that account state.
	if fusekit.Mounted(accountDir) {
		return "", fmt.Errorf("refusing to tear down %q: it is a live mountpoint", accountDir)
	}
	entries, err := os.ReadDir(accountDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		// Remove only exact provider-owned symlinks and excluded private dirs;
		// leave foreign symlinks and unexpected real data untouched.
		full := filepath.Join(accountDir, e.Name())
		fi, err := os.Lstat(full)
		if err != nil {
			continue
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			if _, err := removeOwnedSymlink(base, e.Name(), full); err != nil {
				return "", err
			}
			continue
		}
		if p.Spec.Excluded[e.Name()] {
			if err := os.RemoveAll(full); err != nil {
				return "", err
			}
		}
	}
	return "", nil
}

// assertSymlink ensures dst is a symlink to target. A different symlink is not
// provably provider-owned, so it is refused rather than replaced.
func assertSymlink(target, dst string) error {
	if cur, err := os.Readlink(dst); err == nil {
		if cur == target {
			return nil // already correct
		}
		return fmt.Errorf("cannot link %q: symlink points to %q, want %q; refusing to replace a foreign link", dst, cur, target)
	} else if _, statErr := os.Lstat(dst); statErr == nil {
		// dst exists but is not a symlink — do not clobber real data.
		return fmt.Errorf("cannot link %q: a non-symlink already exists there", dst)
	}
	if err := os.Symlink(target, dst); err != nil {
		return fmt.Errorf("symlink %q -> %q: %w", dst, target, err)
	}
	return nil
}

// removeOwnedSymlink removes dst only when it is a symlink whose target exactly
// matches the target this provider creates for name. It returns whether it removed
// the link. A foreign symlink, real path, or absent path is left untouched.
func removeOwnedSymlink(base, name, dst string) (bool, error) {
	target, err := os.Readlink(dst)
	if err != nil {
		return false, nil
	}
	if target != filepath.Join(base, name) {
		return false, nil
	}
	if err := os.Remove(dst); err != nil {
		return false, fmt.Errorf("remove stale provider link %q: %w", dst, err)
	}
	return true, nil
}

// assertPrivateDir ensures dst is a real (non-symlink) directory.
func assertPrivateDir(dst string) error {
	if fi, err := os.Lstat(dst); err == nil {
		if fi.IsDir() {
			return nil
		}
		return fmt.Errorf("excluded path %q exists as a non-dir", dst)
	}
	return os.MkdirAll(dst, 0o700)
}
