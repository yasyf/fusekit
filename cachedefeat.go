//go:build fuse && cgo

// This file holds the cache-defeat decorator, the one genuinely new fusekit
// design. It relocates cc-notes' NFS data-cache defeats — the per-version
// mtime-nanosecond override (versionNsec) and the Fsync/Flush commit — behind a
// flag (Config.CacheDefeat) so cc-notes opts in and cc-pool leaves it nil
// (byte-identical runtime to today). The rich commit error-handling
// (transient-vs-deterministic, draft-preserve) stays INSIDE the consumer's
// Commit callback; fusekit only runs it on both write boundaries.

package fusekit

import "github.com/winfsp/cgofuse/fuse"

// CacheDefeat carries the two callbacks that defeat an NFS client's caches over
// a fuse-t mount. Both are optional: a nil callback skips its hook. It is wired
// in via Config.CacheDefeat and applied by Mount through the cacheDefeatFS
// decorator.
type CacheDefeat struct {
	// VersionSeed returns a per-version seed for the file at path (typically a
	// chain-tip SHA). When it returns a non-empty seed, the decorator overrides
	// the Getattr stat's mtime nanoseconds with VersionNsec(seed): entity
	// timestamps have second granularity, so a save landing in the same second
	// would otherwise leave the mtime unchanged and the NFS client would keep
	// serving its own written pages over the differing canonical render.
	// Folding the seed into the nanoseconds makes every version a visible mtime
	// change, forcing a data-cache revalidation. An empty seed leaves the
	// Nsec untouched. The stat is the one the inner FS just filled (read-only
	// here beyond the Nsec write).
	VersionSeed func(path string, stat *fuse.Stat_t) string

	// Commit runs after the inner FS's Flush AND Fsync handlers (on both,
	// because fuse-t's NFS client issues a COMMIT before the close(2) flush and
	// reports ITS failure to the writer while flush errors are swallowed —
	// committing on both is what makes a bad save fail loudly at close). It
	// returns a fuse errno: zero is success, and a non-zero value replaces the
	// inner handler's success rc so the failure reaches the writer. The
	// consumer's transient-vs-deterministic error handling lives inside this
	// callback, not in fusekit.
	Commit func(path string, fh uint64) int
}

// cacheDefeatFS decorates an inner cgofuse filesystem with the cache-defeat
// hooks. It embeds fuse.FileSystemInterface (the inner FS) so every op it does
// not override — including the optional xattr ops (Getxattr/Listxattr) — is
// forwarded to the inner FS unchanged. Only Getattr, Flush, and Fsync are
// overridden.
type cacheDefeatFS struct {
	fuse.FileSystemInterface
	cd CacheDefeat
}

// Getattr forwards to the inner FS, then — on success and when VersionSeed
// yields a non-empty seed — overrides the stat's mtime nanoseconds with
// VersionNsec(seed) so every version is a distinct mtime to the NFS client.
func (c *cacheDefeatFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	rc := c.FileSystemInterface.Getattr(path, stat, fh)
	if rc == 0 && c.cd.VersionSeed != nil {
		if seed := c.cd.VersionSeed(path, stat); seed != "" {
			stat.Mtim.Nsec = VersionNsec(seed)
		}
	}
	return rc
}

// Flush forwards to the inner FS, then runs the commit hook. A non-zero commit
// errno replaces the inner handler's success so the failure reaches close(2).
func (c *cacheDefeatFS) Flush(path string, fh uint64) int {
	rc := c.FileSystemInterface.Flush(path, fh)
	if rc == 0 && c.cd.Commit != nil {
		if cr := c.cd.Commit(path, fh); cr != 0 {
			return cr
		}
	}
	return rc
}

// Fsync forwards to the inner FS, then runs the commit hook — the same commit
// as Flush, because fuse-t reports the COMMIT (fsync) errno to the writer while
// swallowing the flush errno.
func (c *cacheDefeatFS) Fsync(path string, datasync bool, fh uint64) int {
	rc := c.FileSystemInterface.Fsync(path, datasync, fh)
	if rc == 0 && c.cd.Commit != nil {
		if cr := c.cd.Commit(path, fh); cr != 0 {
			return cr
		}
	}
	return rc
}
