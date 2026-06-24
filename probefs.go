//go:build fuse && cgo

// This file holds probeFS, a minimal read-only passthrough of a real directory.
// It is HostProbe's throwaway mount target and the -tags fuse test fixture the
// cache-defeat forwarding test mounts
// nothing-but-wraps. It implements just enough of the cgofuse interface to
// serve Getattr/Open/Read/Readdir over the backing dir, plus a fixed xattr on
// Getxattr/Listxattr so the decorator's optional-op forwarding can be pinned.

package fusekit

import (
	"os"
	"path/filepath"

	"github.com/winfsp/cgofuse/fuse"
)

// probeXattrName and probeXattrValue are the single extended attribute probeFS
// reports, so the cache-defeat forwarding test can assert a wrapped probeFS
// still promotes Getxattr/Listxattr to the inner FS.
const (
	probeXattrName  = "user.fusekit.probe"
	probeXattrValue = "ok"
)

// probeFS is a read-only passthrough of root. Beyond the four ops needed to
// stat, open, read, and list files, it serves a single fixed xattr; every other
// op falls through to FileSystemBase (-ENOSYS), which is all a throwaway probe
// (and the forwarding test) needs.
type probeFS struct {
	fuse.FileSystemBase
	root string
}

// full maps a mount-relative path to its backing path under root.
func (p *probeFS) full(path string) string { return filepath.Join(p.root, filepath.FromSlash(path)) }

// Getattr stats the backing path and fills a minimal stat (mode, size, times)
// from the os.FileInfo — enough for the kernel to read the file and for the
// cache-defeat decorator to override the mtime nanoseconds.
func (p *probeFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	fi, err := os.Lstat(p.full(path))
	if err != nil {
		return -fuse.ENOENT
	}
	mode := uint32(fuse.S_IFREG | 0o644)
	nlink := uint32(1)
	if fi.IsDir() {
		mode = uint32(fuse.S_IFDIR | 0o755)
		nlink = 2
	}
	mt := fuse.Timespec{Sec: fi.ModTime().Unix(), Nsec: int64(fi.ModTime().Nanosecond())}
	*stat = fuse.Stat_t{
		Mode:    mode,
		Nlink:   nlink,
		Size:    fi.Size(),
		Atim:    mt,
		Mtim:    mt,
		Ctim:    mt,
		Blksize: 4096,
		Blocks:  (fi.Size() + 511) / 512,
	}
	return 0
}

// Open accepts any existing path; the probe FS is read-only and stateless, so
// it hands back the zero file handle.
func (p *probeFS) Open(path string, flags int) (int, uint64) {
	if _, err := os.Lstat(p.full(path)); err != nil {
		return -fuse.ENOENT, ^uint64(0)
	}
	return 0, 0
}

// Read serves bytes from the backing file.
func (p *probeFS) Read(path string, buff []byte, ofst int64, fh uint64) int {
	data, err := os.ReadFile(p.full(path))
	if err != nil {
		return -fuse.EIO
	}
	if ofst >= int64(len(data)) {
		return 0
	}
	return copy(buff, data[ofst:])
}

// Readdir lists the backing directory, filling nil stats so the kernel issues a
// per-name Getattr.
func (p *probeFS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	entries, err := os.ReadDir(p.full(path))
	if err != nil {
		return -fuse.ENOENT
	}
	fill(".", nil, 0)
	fill("..", nil, 0)
	for _, e := range entries {
		if !fill(e.Name(), nil, 0) {
			break
		}
	}
	return 0
}

// Opendir accepts any existing directory.
func (p *probeFS) Opendir(path string) (int, uint64) {
	fi, err := os.Lstat(p.full(path))
	if err != nil || !fi.IsDir() {
		return -fuse.ENOENT, ^uint64(0)
	}
	return 0, 0
}

// Flush succeeds (the probe FS is read-only and has no buffered state). It
// exists so a cache-defeat Commit hook layered over probeFS runs.
func (p *probeFS) Flush(path string, fh uint64) int { return 0 }

// Fsync succeeds, for the same reason as Flush.
func (p *probeFS) Fsync(path string, datasync bool, fh uint64) int { return 0 }

// Getxattr serves the single fixed probe xattr and ENOATTR for anything else.
func (p *probeFS) Getxattr(path string, name string) (int, []byte) {
	if name == probeXattrName {
		return len(probeXattrValue), []byte(probeXattrValue)
	}
	return -fuse.ENOATTR, nil
}

// Listxattr lists the single fixed probe xattr.
func (p *probeFS) Listxattr(path string, fill func(name string) bool) int {
	fill(probeXattrName)
	return 0
}
