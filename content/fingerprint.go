package content

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
)

// freshnessAbsent marks an ENOENT freshness path in a digest: a stable, valid
// state (the file legitimately does not exist yet), distinct from every present
// (mtime_ns, size) pair, so an absent→present transition changes the digest.
const freshnessAbsent = "absent"

// FreshnessVersion derives a deterministic freshness key over paths: a hex sha256
// of each path's (path, mtime_ns, size), hashed in the given order. A missing path
// (ENOENT) contributes a stable absent marker — a valid state, not an error — so an
// absent file has a defined version that changes the moment it appears. Any other
// lstat errno fails loud.
func FreshnessVersion(paths []string) (string, error) {
	h := sha256.New()
	for _, p := range paths {
		if err := hashLstat(h, p); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Fingerprint derives a deterministic content fingerprint over entries: a hex
// sha256 over each entry's identity (name, kind, target, private, version, size)
// and, for every Freshness path, its live (mtime_ns, size) — or the absent marker
// on ENOENT. Entries are sorted by Name first so the digest is independent of
// manifest order; each entry's Freshness paths hash in their given order. A
// non-ENOENT lstat errno fails loud.
func Fingerprint(entries []Entry) (string, error) {
	sorted := append([]Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	h := sha256.New()
	for _, e := range sorted {
		fmt.Fprintf(h, "%s\x00%s\x00%s\x00%t\x00%s\x00%d\n", e.Name, e.Kind, e.Target, e.Private, e.Version, e.Size)
		for _, p := range e.Freshness {
			if err := hashLstat(h, p); err != nil {
				return "", err
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// hashLstat folds one path's lstat identity into h: (path, mtime_ns, size) when it
// exists, (path, absent) on ENOENT, and a loud error on any other errno.
func hashLstat(h io.Writer, p string) error {
	fi, err := os.Lstat(p)
	switch {
	case err == nil:
		fmt.Fprintf(h, "%s\x00%d\x00%d\n", p, fi.ModTime().UnixNano(), fi.Size())
		return nil
	case errors.Is(err, fs.ErrNotExist):
		fmt.Fprintf(h, "%s\x00%s\n", p, freshnessAbsent)
		return nil
	default:
		return fmt.Errorf("freshness lstat %s: %w", p, err)
	}
}
