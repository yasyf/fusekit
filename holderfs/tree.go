//go:build fuse && cgo && darwin

package holderfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
)

// treeFS is the tree-mode holder filesystem: a thin fuse shell over treeView.
// EVERY op is remote — Getattr from Stat, Readdir from List, reads from
// ReadAt (token-keyed per open handle), Readlink from Readlink, mutations
// through the WritableTree ops — with NO local Base syscalls anywhere; the
// spec's Base is a nominal identity key the fs never touches. The semantics
// (serve-stale caching, attr stability, AppleDouble blocking) live in
// treeview.go; this file only routes vnops, serves the virtual wedge probe,
// and converts to fuse types.
type treeFS struct {
	fuse.FileSystemBase
	view      *treeView
	probe     *probeView
	probePath string // "/.ccn-probe" or ""
}

var (
	_ fusekit.Flusher         = (*treeFS)(nil)
	_ fusekit.PassthroughOnly = (*treeFS)(nil)
)

// FusePassthroughOnly is false: every byte served is handler-generated and
// read handles are token-keyed on fi->fh, which fuse-t's FSKit backend does
// not honor, so fusekit keeps the NFS backend.
func (fs *treeFS) FusePassthroughOnly() bool { return false }

// buildTree constructs the tree-mode Config for spec. It FAILS LOUD when the
// consumer bridge is unreachable or its source does not implement
// content.Tree — an empty or partial tree must never mount — then sweeps
// stale handle tokens (content.HandleTree's crash-recovery contract) before
// serving.
func buildTree(spec fusekit.MountSpec) (fusekit.Config, error) {
	if spec.ContentSocket == "" {
		return fusekit.Config{}, fmt.Errorf("holderfs: tree mode for %s requires a content socket", spec.Dir)
	}
	view := newTreeView(spec.Domain, content.NewBridgeClient(spec.ContentSocket))
	if err := view.prewarmRoot(); err != nil {
		return fusekit.Config{}, fmt.Errorf("holderfs: tree root for %s: %w", spec.Domain, err)
	}
	if err := view.sweepHandles(); err != nil {
		return fusekit.Config{}, fmt.Errorf("holderfs: %w", err)
	}

	fs := &treeFS{view: view, probePath: spec.ProbePath}
	if spec.ProbePath != "" {
		fs.probe = newProbeView()
	}
	return fusekit.Config{
		Base: spec.Base,
		Dir:  spec.Dir,
		FS:   fs,
		Options: fusekit.MountOptions{
			// No NamedAttr, as in source mode: the NFSv4 named-attribute vnode
			// path is implicated in macOS nfs_vinvalbuf2 kernel panics; the
			// AppleDouble ._ fallback is blocked outright instead (isAppleDouble).
			Volname:  "holder-" + filepath.Base(spec.Dir),
			NoBrowse: true,
			Extra:    []string{"rwsize=1048576"},
			// Per-mount attr-cache opt-in (default false = noattrcache); tree
			// mode serves stabilized attrs (treeview.go), as source mode does.
			AttrCache:        spec.AttrCache,
			AttrCacheTimeout: spec.AttrCacheTimeout,
		}.Build(),
		Ready:     treeReadyFn(spec),
		Wait:      mountWait,
		FirstWait: firstMountWait,
	}, nil
}

// treeReadyFn is the tree mount's come-up probe. With a probe path it defers
// to readyFn's virtual-probe lstat. Without one, MountAlive is structurally
// wrong here — the nominal Base's entries never show through a tree mount —
// so liveness is the mountpoint being a mount plus the root answering a
// readdir (host.go's servingRoot), which only the live holder can serve.
func treeReadyFn(spec fusekit.MountSpec) func() bool {
	if spec.ProbePath != "" {
		return readyFn(spec)
	}
	dir := spec.Dir
	return func() bool { return fusekit.Mounted(dir) && servingRoot(dir) }
}

// FlushWithin drains dirty token handles' commits before teardown, so an
// uncommitted editor buffer reaches the consumer before the mount goes away.
func (fs *treeFS) FlushWithin(grace time.Duration) bool {
	return fs.view.flushWithin(grace)
}

// fillTreeStat converts a served treeStat to the fuse stat: modes are the
// holder's fixed presentation (the consumer owns none), times and sizes are
// the view's stabilized values, and the ino is always the minted one.
func fillTreeStat(stat *fuse.Stat_t, st treeStat) {
	mode, nlink := uint32(fuse.S_IFREG|0o644), uint32(1)
	switch st.kind {
	case content.EntryDir:
		mode, nlink = fuse.S_IFDIR|0o755, 2
	case content.EntrySymlink:
		mode = fuse.S_IFLNK | 0o777
	}
	mt, bt := tsOf(st.mtime), tsOf(st.birth)
	*stat = fuse.Stat_t{
		Ino:      st.ino,
		Mode:     mode,
		Nlink:    nlink,
		Uid:      uint32(os.Getuid()),
		Gid:      uint32(os.Getgid()),
		Size:     st.size,
		Atim:     mt,
		Mtim:     mt,
		Ctim:     mt,
		Birthtim: bt,
		Blksize:  4096,
		Blocks:   (st.size + 511) / 512,
	}
}

// Statfs serves fixed synthetic volume geometry: there is no local backing
// filesystem to report, and the values only need to be stable and roomy.
func (fs *treeFS) Statfs(_ string, stat *fuse.Statfs_t) int {
	*stat = fuse.Statfs_t{
		Bsize:   4096,
		Frsize:  4096,
		Blocks:  1 << 30,
		Bfree:   1 << 29,
		Bavail:  1 << 29,
		Files:   1 << 20,
		Ffree:   1 << 19,
		Namemax: 255,
	}
	return 0
}

func (fs *treeFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	if fs.probe != nil && (probeFh(fh) || path == fs.probePath) {
		return fs.probe.getattr(stat)
	}
	if treeFh(fh) {
		st, rc := fs.view.getattrHandle(fh)
		if rc != 0 {
			return rc
		}
		fillTreeStat(stat, st)
		return 0
	}
	st, rc := fs.view.getattr(path)
	if rc != 0 {
		return rc
	}
	fillTreeStat(stat, st)
	return 0
}

func (fs *treeFS) Opendir(path string) (int, uint64) {
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.ENOTDIR), ^uint64(0)
	}
	return fs.view.opendir(path), ^uint64(0)
}

func (fs *treeFS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, _ int64, _ uint64) int {
	ents, rc := fs.view.readdir(path)
	if rc != 0 {
		return rc
	}
	fill(".", nil, 0)
	fill("..", nil, 0)
	probeName := strings.TrimPrefix(fs.probePath, "/")
	for _, e := range ents {
		if path == "/" && fs.probe != nil && e.name == probeName {
			continue // the virtual probe is never listed
		}
		var st fuse.Stat_t
		fillTreeStat(&st, e.st)
		if !fill(e.name, &st, 0) {
			return 0
		}
	}
	return 0
}

func (fs *treeFS) Releasedir(_ string, _ uint64) int { return 0 }

func (fs *treeFS) Readlink(path string) (int, string) {
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EINVAL), ""
	}
	target, rc := fs.view.readlink(path)
	return rc, target
}

func (fs *treeFS) Open(path string, flags int) (int, uint64) {
	if path == fs.probePath && fs.probe != nil {
		return fs.probe.open(flags)
	}
	fh, rc := fs.view.open(path, flags)
	if rc != 0 {
		return rc, ^uint64(0)
	}
	return 0, fh
}

func (fs *treeFS) Create(path string, _ int, _ uint32) (int, uint64) {
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM), ^uint64(0)
	}
	fh, rc := fs.view.create(path)
	if rc != 0 {
		return rc, ^uint64(0)
	}
	return 0, fh
}

func (fs *treeFS) Read(_ string, buff []byte, ofst int64, fh uint64) int {
	if fs.probe != nil && probeFh(fh) {
		return fs.probe.read(fh, buff, ofst)
	}
	return fs.view.read(fh, buff, ofst)
}

func (fs *treeFS) Write(_ string, buff []byte, ofst int64, fh uint64) int {
	if probeFh(fh) {
		return -int(syscall.EBADF) // the probe is read-only
	}
	return fs.view.write(fh, buff, ofst)
}

func (fs *treeFS) Truncate(path string, size int64, fh uint64) int {
	if (path == fs.probePath && fs.probe != nil) || probeFh(fh) {
		return -int(syscall.EPERM)
	}
	if treeFh(fh) {
		return fs.view.truncateHandle(fh, size)
	}
	return fs.view.truncatePath(path, size)
}

// Flush and Fsync forward the dirty token handle's commit verdict — the only
// chance a rejected save (parse EINVAL, immutable EPERM) has to reach the
// writer, since a fuse Release status is kernel-discarded.
func (fs *treeFS) Flush(_ string, fh uint64) int {
	if probeFh(fh) {
		return 0
	}
	return fs.view.flush(fh)
}

func (fs *treeFS) Fsync(_ string, _ bool, fh uint64) int {
	if probeFh(fh) {
		return 0
	}
	return fs.view.flush(fh)
}

func (fs *treeFS) Release(_ string, fh uint64) int {
	if fs.probe != nil && probeFh(fh) {
		fs.probe.release(fh)
		return 0
	}
	return fs.view.release(fh)
}

func (fs *treeFS) Unlink(path string) int {
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return fs.view.unlink(path)
}

func (fs *treeFS) Mkdir(path string, _ uint32) int {
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return fs.view.mkdir(path)
}

// Rmdir is refused: the bridge write surface has no directory-removal op (a
// tree consumer's directories are its own presentation), so the holder
// answers EPERM rather than mistranslating it onto Unlink.
func (fs *treeFS) Rmdir(path string) int {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT)
	}
	return -int(syscall.EPERM)
}

func (fs *treeFS) Rename(oldpath string, newpath string) int {
	if fs.probe != nil && (oldpath == fs.probePath || newpath == fs.probePath) {
		return -int(syscall.EPERM)
	}
	return fs.view.rename(oldpath, newpath)
}

// Link and Symlink are refused: hard links do not exist consumer-side, and
// the bridge has no symlink-creation op (symlinks are served, never created
// through the mount).
func (fs *treeFS) Link(oldpath string, newpath string) int {
	if isAppleDouble(oldpath) {
		return -int(syscall.ENOENT)
	}
	if isAppleDouble(newpath) {
		return -int(syscall.EACCES)
	}
	return -int(syscall.EPERM)
}

func (fs *treeFS) Symlink(_ string, newpath string) int {
	if isAppleDouble(newpath) {
		return -int(syscall.EACCES)
	}
	return -int(syscall.EPERM)
}

// Chmod is acknowledged and ignored (treeView.chmod): the consumer owns the
// presentation — no mode crosses the bridge — and editors chmod inside their
// save paths, so refusing would break saves for zero benefit.
func (fs *treeFS) Chmod(path string, _ uint32) int {
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return fs.view.chmod(path)
}

// Chown is refused: ownership is not a concept the bridge carries, and no
// legitimate save path chowns.
func (fs *treeFS) Chown(path string, _ uint32, _ uint32) int {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT)
	}
	return -int(syscall.EPERM)
}

// Utimens is acknowledged and ignored (treeView.utimens): the served times
// are the consumer's, kept monotonic by the view.
func (fs *treeFS) Utimens(path string, _ []fuse.Timespec) int {
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return fs.view.utimens(path)
}

// The xattr surface is virtual, as on the probe: no entry has xattrs and
// mutations are refused. The macOS NFS client never reaches these (the mount
// runs without namedattr); they answer direct-Serve and Linux consumers.

func (fs *treeFS) Setxattr(_ string, _ string, _ []byte, _ int) int {
	return -int(syscall.EPERM)
}

func (fs *treeFS) Getxattr(_ string, _ string) (int, []byte) {
	return -int(syscall.ENOATTR), nil
}

func (fs *treeFS) Listxattr(_ string, _ func(name string) bool) int { return 0 }

func (fs *treeFS) Removexattr(_ string, _ string) int { return -int(syscall.EPERM) }
