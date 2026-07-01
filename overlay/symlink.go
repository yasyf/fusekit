package overlay

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/fusekit"
)

// SymlinkProvider symlinks each top-level entry of base into accountDir, except
// Spec's Excluded entries (which get private empty dirs) and Skip entries.
type SymlinkProvider struct {
	// Spec classifies each top-level entry.
	Spec Spec
}

var _ Provider = (*SymlinkProvider)(nil)

// Backend reports BackendSymlink.
func (p *SymlinkProvider) Backend() Backend { return BackendSymlink }

// PrivateRoot is accountDir itself: private files live alongside the symlinks.
func (p *SymlinkProvider) PrivateRoot(accountDir string) string { return accountDir }

// Setup creates accountDir and asserts all links. Idempotent.
func (p *SymlinkProvider) Setup(base, accountDir string) error {
	if err := os.MkdirAll(accountDir, 0o700); err != nil {
		return fmt.Errorf("mkdir account dir: %w", err)
	}
	return p.Sync(base, accountDir)
}

// Sync asserts each top-level entry's shape in accountDir: a symlink for shared
// entries, a private dir for excluded ones, no symlink for private ones.
// Per-entry failures are joined so one conflict neither blocks nor masks others.
// Refuses base itself: self-overlay would replace real entries with self-links.
func (p *SymlinkProvider) Sync(base, accountDir string) error {
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
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if p.Spec.Skip[name] {
			continue
		}
		dst := filepath.Join(accountDir, name)
		if p.Spec.Excluded[name] {
			if err := assertPrivateDir(dst); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if p.Spec.IsPrivate(name) {
			if err := assertNoSymlink(dst); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if err := assertSymlink(filepath.Join(base, name), dst); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Health verifies every shared top-level entry of base is correctly linked in
// accountDir and every excluded entry is a real local dir.
func (p *SymlinkProvider) Health(base, accountDir string) error {
	entries, err := os.ReadDir(base)
	if err != nil {
		return fmt.Errorf("read base dir: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if p.Spec.Skip[name] {
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
		target, err := os.Readlink(dst)
		if err != nil {
			return fmt.Errorf("entry %q is not a symlink: %w", name, err)
		}
		if target != filepath.Join(base, name) {
			return fmt.Errorf("entry %q links to %q, want %q", name, target, filepath.Join(base, name))
		}
	}
	return nil
}

// Teardown removes the account dir's overlay — safe because shared entries are
// symlinks (removal never touches base) and excluded entries are the account's
// own private dirs. Refuses base as a guard against misuse.
func (p *SymlinkProvider) Teardown(base, accountDir string) error {
	if accountDir == base || accountDir == "" {
		return fmt.Errorf("refusing to tear down base dir %q", accountDir)
	}
	// Through a live fuse mirror, excluded dirs resolve to the private backing
	// root — RemoveAll would destroy that account state.
	if fusekit.Mounted(accountDir) {
		return fmt.Errorf("refusing to tear down %q: it is a live mountpoint", accountDir)
	}
	entries, err := os.ReadDir(accountDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		// Remove only symlinks and our excluded private dirs; leave anything
		// unexpected so we never destroy real user data.
		full := filepath.Join(accountDir, e.Name())
		fi, err := os.Lstat(full)
		if err != nil {
			continue
		}
		if fi.Mode()&os.ModeSymlink != 0 || p.Spec.Excluded[e.Name()] {
			if err := os.RemoveAll(full); err != nil {
				return err
			}
		}
	}
	return nil
}

// assertSymlink ensures dst is a symlink to target, replacing wrong links.
func assertSymlink(target, dst string) error {
	if cur, err := os.Readlink(dst); err == nil {
		if cur == target {
			return nil // already correct
		}
		if err := os.Remove(dst); err != nil {
			return fmt.Errorf("remove stale link %q: %w", dst, err)
		}
	} else if _, statErr := os.Lstat(dst); statErr == nil {
		// dst exists but is not a symlink — do not clobber real data.
		return fmt.Errorf("cannot link %q: a non-symlink already exists there", dst)
	}
	if err := os.Symlink(target, dst); err != nil {
		return fmt.Errorf("symlink %q -> %q: %w", dst, target, err)
	}
	return nil
}

// assertNoSymlink removes a symlink at dst if present. A symlink at a private
// name is necessarily our own stale artifact (the source never creates one
// there), so removing it never touches base or real data.
func assertNoSymlink(dst string) error {
	fi, err := os.Lstat(dst)
	if err != nil || fi.Mode()&os.ModeSymlink == 0 {
		return nil
	}
	if err := os.Remove(dst); err != nil {
		return fmt.Errorf("remove stale private link %q: %w", dst, err)
	}
	return nil
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
