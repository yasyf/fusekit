//go:build fuse && cgo

package fusekit

import (
	"os"
	"path/filepath"

	"github.com/winfsp/cgofuse/fuse"
)

// The single xattr probeFS serves, so the forwarding test can pin that a
// wrapper still promotes Getxattr/Listxattr to the inner FS.
const (
	probeXattrName  = "user.fusekit.probe"
	probeXattrValue = "ok"
)

// probeFS is a minimal read-only passthrough of root: HostProbe's throwaway
// mount target and the cache-defeat forwarding test's fixture. Unimplemented
// ops fall through to FileSystemBase (-ENOSYS).
type probeFS struct {
	fuse.FileSystemBase
	root string
}

func (p *probeFS) full(path string) string { return filepath.Join(p.root, filepath.FromSlash(path)) }

// Getattr fills a minimal stat; real mtimes matter — the cache-defeat
// decorator overrides their nanoseconds.
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

func (p *probeFS) Open(path string, flags int) (int, uint64) {
	if _, err := os.Lstat(p.full(path)); err != nil {
		return -fuse.ENOENT, ^uint64(0)
	}
	return 0, 0
}

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

// Readdir fills nil stats so the kernel issues a per-name Getattr.
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

func (p *probeFS) Opendir(path string) (int, uint64) {
	fi, err := os.Lstat(p.full(path))
	if err != nil || !fi.IsDir() {
		return -fuse.ENOENT, ^uint64(0)
	}
	return 0, 0
}

// Flush exists so a cache-defeat Commit hook layered over probeFS runs.
func (p *probeFS) Flush(path string, fh uint64) int { return 0 }

// Fsync exists for the same reason as Flush.
func (p *probeFS) Fsync(path string, datasync bool, fh uint64) int { return 0 }

func (p *probeFS) Getxattr(path string, name string) (int, []byte) {
	if name == probeXattrName {
		return len(probeXattrValue), []byte(probeXattrValue)
	}
	return -fuse.ENOATTR, nil
}

func (p *probeFS) Listxattr(path string, fill func(name string) bool) int {
	fill(probeXattrName)
	return 0
}
