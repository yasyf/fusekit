package content

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
)

// freshPresent and freshAbsent tag an lstat outcome in a digest with a
// fixed-width, self-delimiting marker: absent is a stable, valid state (the file
// legitimately does not exist yet), distinct from every present (mtime_ns, size)
// pair, so an absent→present transition changes the digest.
const (
	freshAbsent  uint64 = 0
	freshPresent uint64 = 1
)

// writeFrame folds a variable-length field into h with an 8-byte length prefix, so
// no field value can forge a boundary by embedding a delimiter. h is a hash (Write
// never errors).
func writeFrame(h io.Writer, b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(b)
}

// writeUint folds an integer into h as a fixed-width, self-delimiting 8-byte value.
func writeUint(h io.Writer, v uint64) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], v)
	_, _ = h.Write(n[:])
}

// FreshnessVersion derives a deterministic freshness key over paths: a hex sha256
// of each path's (path, mtime_ns, size), hashed in the given order. A missing path
// (ENOENT) contributes a stable absent marker — a valid state, not an error — so an
// absent file has a defined version that changes the moment it appears. Any other
// lstat errno fails loud. Fields are length-prefixed or fixed-width, so no crafted
// path can collide two distinct path lists.
func FreshnessVersion(paths []string) (string, error) {
	h := sha256.New()
	writeUint(h, uint64(len(paths)))
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
// non-ENOENT lstat errno fails loud. Every variable-length field is length-prefixed
// and every count/number fixed-width, so the encoding is self-delimiting: a crafted
// Version (opaque, unrestricted) cannot forge a field or entry boundary.
func Fingerprint(entries []Entry) (string, error) {
	sorted := append([]Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	h := sha256.New()
	writeUint(h, uint64(len(sorted)))
	for _, e := range sorted {
		writeFrame(h, []byte(e.Name))
		writeFrame(h, []byte(e.Kind))
		writeFrame(h, []byte(e.Target))
		var priv uint64
		if e.Private {
			priv = 1
		}
		writeUint(h, priv)
		writeFrame(h, []byte(e.Version))
		writeUint(h, uint64(e.Size))
		writeUint(h, uint64(len(e.Freshness)))
		for _, p := range e.Freshness {
			if err := hashLstat(h, p); err != nil {
				return "", err
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// hashLstat folds one path's lstat identity into h: (path, present, mtime_ns, size)
// when it exists, (path, absent) on ENOENT, and a loud error on any other errno.
// The path is length-prefixed and the marker/numbers fixed-width, so the record is
// self-delimiting.
func hashLstat(h io.Writer, p string) error {
	fi, err := os.Lstat(p)
	switch {
	case err == nil:
		writeFrame(h, []byte(p))
		writeUint(h, freshPresent)
		writeUint(h, uint64(fi.ModTime().UnixNano()))
		writeUint(h, uint64(fi.Size()))
		return nil
	case errors.Is(err, fs.ErrNotExist):
		writeFrame(h, []byte(p))
		writeUint(h, freshAbsent)
		return nil
	default:
		return fmt.Errorf("freshness lstat %s: %w", p, err)
	}
}
