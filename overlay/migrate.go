package overlay

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ResolvedConflictLogf surfaces every file collision moveEntry resolves by
// last-write-wins, so recovery is observable, never silent data loss. A no-op by
// default; each driving process wires it at startup. Assigned once before any
// sweep or conversion runs, so no lock guards it.
var ResolvedConflictLogf = func(_ string, _ ...any) {}

// Overlay conversion and crash-repair primitives, untagged so even a non-fuse
// binary can recognize a fuse private backing dir, move stranded files back, and
// refuse symlink ops on a live mountpoint.

// FusePrivateRoot is the fuse provider's per-account private backing dir
// (accountDir + ".private"): private entries (spec.IsPrivate names) physically
// live there while the fuse overlay is up, the mirror redirecting their paths so
// they stay visible through the mount. Never exported as a config dir, never
// hashed for a service name.
func FusePrivateRoot(accountDir string) string {
	return accountDir + ".private"
}

// MovePrivateEntries relocates every top-level private entry (spec.IsPrivate
// names, including Excluded dirs) between private roots via same-volume rename,
// leaving shared symlinks and unclassified entries untouched. Idempotent and
// resumable: already-moved entries are skipped, so re-running after a crash
// converges. Existing destinations are reconciled by moveEntry; per-entry
// failures collected with errors.Join.
func MovePrivateEntries(from, to string, spec Spec) error {
	if from == "" || to == "" {
		return fmt.Errorf("move private entries: empty root (from %q, to %q)", from, to)
	}
	if from == to {
		return fmt.Errorf("move private entries: from and to are both %q", from)
	}
	entries, err := os.ReadDir(from)
	if err != nil {
		return fmt.Errorf("read private root %q: %w", from, err)
	}
	if err := os.MkdirAll(to, 0o700); err != nil {
		return fmt.Errorf("mkdir private root %q: %w", to, err)
	}
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if !spec.IsPrivate(name) {
			continue
		}
		src := filepath.Join(from, name)
		// A symlink at a private name is a stale artifact from before the name was
		// classified private (cf. assertNoSymlink): remove it, never move it.
		if fi, err := os.Lstat(src); err == nil && fi.Mode()&os.ModeSymlink != 0 {
			if err := os.Remove(src); err != nil {
				errs = append(errs, fmt.Errorf("remove stale private link %q: %w", src, err))
			}
			continue
		}
		if err := moveEntry(src, filepath.Join(to, name), spec); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// MoveSharedOrphans relocates every top-level SHARED orphan (a real, non-symlink
// entry that is neither Skipped nor spec.IsPrivate — the symlink provider's "linked
// into base" default class) between roots via moveEntry. Such orphans appear when
// a session wrote to a bare account mountpoint while its fuse mirror was
// force-unmounted; the retreat to the symlink overlay must move them into base
// BEFORE laying the links, or assertSymlink refuses to clobber a non-symlink and
// the retreat fails.
//
// Symmetric to MovePrivateEntries, with two deliberate differences:
//   - classifies by exclusion (!Skipped && !IsPrivate), so identity, credentials,
//     and excluded private dirs never reach base;
//   - LEAVES an already-correct symlink in place (for a shared name the symlink
//     is the desired end state), so a retreat resumed after partial linking
//     converges instead of un-linking.
//
// Reconciliation and error handling as moveEntry; idempotent and resumable.
func MoveSharedOrphans(from, to string, spec Spec) error {
	if from == "" || to == "" {
		return fmt.Errorf("move shared orphans: empty root (from %q, to %q)", from, to)
	}
	if from == to {
		return fmt.Errorf("move shared orphans: from and to are both %q", from)
	}
	entries, err := os.ReadDir(from)
	if err != nil {
		return fmt.Errorf("read account dir %q: %w", from, err)
	}
	if err := os.MkdirAll(to, 0o700); err != nil {
		return fmt.Errorf("mkdir base dir %q: %w", to, err)
	}
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if spec.Skipped(name) || spec.IsPrivate(name) {
			continue
		}
		src := filepath.Join(from, name)
		// A symlink at a shared name is the overlay's own link into base and the
		// desired end state: leave it so a resumed retreat converges.
		if fi, err := os.Lstat(src); err == nil && fi.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if err := moveEntry(src, filepath.Join(to, name), spec); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// moveEntry renames src to dst, reconciling an existing destination by kind:
//   - dst missing: plain rename.
//   - both directories: merged child-by-child (mergeDir).
//   - both regular files: a collision from an abnormal shutdown; RESOLVED by
//     resolveFileConflict, not refused — refusing dead-locks the account (the
//     mount sweep and the symlink retreat hit it from opposite directions).
//   - file vs. directory: genuine corruption; fail loud with both copies intact.
func moveEntry(src, dst string, spec Spec) error {
	dfi, err := os.Lstat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("stat %q: %w", dst, err)
		}
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("move %q: %w", src, err)
		}
		return nil
	}
	sfi, err := os.Lstat(src)
	if err != nil {
		return fmt.Errorf("stat %q: %w", src, err)
	}
	switch {
	case sfi.IsDir() && dfi.IsDir():
		return mergeDir(src, dst, spec)
	case sfi.Mode().IsRegular() && dfi.Mode().IsRegular():
		return resolveFileConflict(src, dst, sfi, dfi)
	default:
		return fmt.Errorf("entry type mismatch: %q and %q are not both regular files or both directories; refusing to clobber across types", src, dst)
	}
}

// resolveFileConflict reconciles a regular-file collision (both roots hold the
// file after an abnormal shutdown). Identical bytes: drop src, keep dst.
// Differing bytes: last-write-wins — the newer mtime survives at dst, ties keep
// dst. Reported through ResolvedConflictLogf so it is observable.
func resolveFileConflict(src, dst string, sfi, dfi os.FileInfo) error {
	same, err := sameContent(src, dst)
	if err != nil {
		return err
	}
	if same {
		if err := os.Remove(src); err != nil {
			return fmt.Errorf("drop identical duplicate %q: %w", src, err)
		}
		ResolvedConflictLogf("resolved file conflict on %s (identical duplicate discarded)", dst)
		return nil
	}
	if sfi.ModTime().After(dfi.ModTime()) {
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("replace stale %q with newer %q: %w", dst, src, err)
		}
		ResolvedConflictLogf("resolved file conflict on %s (kept newer copy, discarded stale duplicate)", dst)
		return nil
	}
	if err := os.Remove(src); err != nil {
		return fmt.Errorf("discard stale duplicate %q: %w", src, err)
	}
	ResolvedConflictLogf("resolved file conflict on %s (kept newer copy, discarded stale duplicate)", dst)
	return nil
}

// sameContent reports whether two files hold identical bytes. Runs only on a
// (rare) regular-file collision, so a full read-and-compare needs no size or
// hash shortcut.
func sameContent(a, b string) (bool, error) {
	ab, err := os.ReadFile(a) //nolint:gosec // G304: a/b are overlay-managed paths under the consumer's state dir, compared during conversion
	if err != nil {
		return false, fmt.Errorf("read %q: %w", a, err)
	}
	bb, err := os.ReadFile(b) //nolint:gosec // G304: a/b are overlay-managed paths under the consumer's state dir, compared during conversion
	if err != nil {
		return false, fmt.Errorf("read %q: %w", b, err)
	}
	return bytes.Equal(ab, bb), nil
}

// mergeDir moves src's children into dst (recursing into shared subdirs) and
// removes the then-empty src. Skipped entries (OS cruft) are dropped, not merged.
func mergeDir(src, dst string, spec Spec) error {
	children, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("read %q: %w", src, err)
	}
	var errs []error
	for _, c := range children {
		if spec.Skipped(c.Name()) {
			if err := os.Remove(filepath.Join(src, c.Name())); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if err := moveEntry(filepath.Join(src, c.Name()), filepath.Join(dst, c.Name()), spec); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if err := os.Remove(src); err != nil {
		return fmt.Errorf("remove merged dir %q: %w", src, err)
	}
	return nil
}

// HasPrivateEntries reports whether dir holds meaningful per-account private
// state: a private file, or a private dir with at least one entry. The empty
// excluded dirs fuse Setup pre-creates do not count — shape, not state. A missing
// dir has none.
func HasPrivateEntries(dir string, spec Spec) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("read private root %q: %w", dir, err)
	}
	for _, e := range entries {
		if !spec.IsPrivate(e.Name()) {
			continue
		}
		if !e.IsDir() {
			return true, nil
		}
		children, err := os.ReadDir(filepath.Join(dir, e.Name()))
		if err != nil {
			return false, fmt.Errorf("read private dir %q: %w", filepath.Join(dir, e.Name()), err)
		}
		if len(children) > 0 {
			return true, nil
		}
	}
	return false, nil
}
