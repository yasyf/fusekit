//go:build fuse && cgo

package fusekit

import "github.com/winfsp/cgofuse/fuse"

// CacheDefeat carries callbacks that defeat an NFS client's caches over a
// fuse-t mount; a nil callback skips its hook.
type CacheDefeat struct {
	// VersionSeed returns a per-version seed for path; non-empty overrides the
	// stat's mtime nsec with VersionNsec(seed). Mtimes are second-granular, so
	// a same-second save otherwise leaves the NFS client serving its own
	// written pages over the differing canonical render.
	VersionSeed func(path string, stat *fuse.Stat_t) string

	// Commit runs after both Flush and Fsync; a non-zero return replaces the
	// inner handler's success rc. fuse-t's NFS client COMMITs before the
	// close(2) flush and surfaces only the COMMIT errno (flush errors are
	// swallowed), so hooking both makes a bad save fail loudly at close.
	Commit func(path string, fh uint64) int
}

type cacheDefeatFS struct {
	fuse.FileSystemInterface
	cd CacheDefeat
}

func (c *cacheDefeatFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	rc := c.FileSystemInterface.Getattr(path, stat, fh)
	if rc == 0 && c.cd.VersionSeed != nil {
		if seed := c.cd.VersionSeed(path, stat); seed != "" {
			stat.Mtim.Nsec = VersionNsec(seed)
		}
	}
	return rc
}

func (c *cacheDefeatFS) Flush(path string, fh uint64) int {
	rc := c.FileSystemInterface.Flush(path, fh)
	if rc == 0 && c.cd.Commit != nil {
		if cr := c.cd.Commit(path, fh); cr != 0 {
			return cr
		}
	}
	return rc
}

func (c *cacheDefeatFS) Fsync(path string, datasync bool, fh uint64) int {
	rc := c.FileSystemInterface.Fsync(path, datasync, fh)
	if rc == 0 && c.cd.Commit != nil {
		if cr := c.cd.Commit(path, fh); cr != 0 {
			return cr
		}
	}
	return rc
}
