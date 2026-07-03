//go:build fuse && cgo && darwin

// Package holderfs is the generic content filesystem the shared fuse holder
// serves: a passthrough mirror of a local Base with live-symlink carve-outs and
// private redirects, plus synthetic entries whose bytes are computed by the
// consumer over the bridge (read OFF the fuse handler path, written through in
// the background). In tree mode (fusekit.ContentModeTree) there is no local
// base at all: every op is served from the consumer's content.Tree over the
// bridge (tree.go/treeview.go). It holds no consumer domain knowledge — the
// merge, the classification, and the version strategy all live behind
// content.Source.
package holderfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
	"golang.org/x/sys/unix"
)

const (
	mountWait      = 8 * time.Second
	firstMountWait = 14 * time.Second
	manifestTries  = 3
	manifestPause  = 500 * time.Millisecond
	// sharedLinkInoBase is the pool minted synthetic inode IDs come from — live
	// symlink carve-outs and synth entries alike — kept clear of real backing
	// inode numbers, so the NFS client never aliases a synthetic object with a
	// real one and a synth entry's fileid stays fixed while write-throughs
	// re-mint writePath's real ino underneath it.
	sharedLinkInoBase = uint64(1) << 62
)

// sharedEntry is a precomputed live-symlink presentation (base target + synthetic
// S_IFLNK stat) for a shared top-level entry, fixed for the mount's life so
// Getattr/Readlink serve it with zero syscalls.
type sharedEntry struct {
	target string
	stat   fuse.Stat_t
}

// synthHandle is one open read handle's snapshot of a synth entry — bytes and
// served mtime captured at open, so a chunked NFS read never tears across a
// mid-read refresh and the handle's Getattr never changes under the open file.
type synthHandle struct {
	v     *synthView
	buf   []byte
	mtime fuse.Timespec
}

// holderFS is the generic content mirror: most ops pass through to Base, with
// carve-outs for live symlinks (shared), PrivateRoot redirects (private + the
// consumer's atomic-write temps), bridge-backed synth entries, and the virtual
// wedge probe.
type holderFS struct {
	fuse.FileSystemBase
	base            string
	privateRoot     string
	privateExact    map[string]bool        // EntryPrivate top-level names
	privatePrefixes []string               // names equal-or-prefixed by these back onto PrivateRoot
	shared          map[string]sharedEntry // top-level name -> live-symlink presentation
	synth           map[string]*synthView  // fuse path ("/.claude.json") -> view
	probe           *probeView
	probePath       string // "/.ccp-probe" or ""

	synthMu     sync.Mutex
	synthFhs    map[uint64]*synthHandle
	nextSynthFh uint64
}

var (
	_ fusekit.Flusher         = (*holderFS)(nil)
	_ fusekit.PassthroughOnly = (*holderFS)(nil)
)

// FusePassthroughOnly is false: the mirror serves handler-generated synth
// content keyed on file handles, which fuse-t's FSKit backend does not honor, so
// fusekit keeps the NFS backend.
func (fs *holderFS) FusePassthroughOnly() bool { return false }

// Build constructs the holder Config for spec, classifies each manifest entry, and
// pre-warms every synth cache off the fuse path so the first read never blocks. It
// FAILS LOUD when content was wired but the consumer is unreachable — a bare
// passthrough would serve the raw Base file and route writes into it — while no
// content socket is a deliberate pure Base passthrough (the probe/capability case).
func Build(spec fusekit.MountSpec) (fusekit.Config, error) {
	switch spec.ContentMode {
	case "", fusekit.ContentModeSource:
	case fusekit.ContentModeTree:
		return buildTree(spec)
	default:
		return fusekit.Config{}, fmt.Errorf("holderfs: unknown content mode %q", spec.ContentMode)
	}

	client := content.NewBridgeClient(spec.ContentSocket)
	fs := &holderFS{
		base:            spec.Base,
		privateRoot:     spec.PrivateRoot,
		privateExact:    map[string]bool{},
		privatePrefixes: spec.PrivatePrefixes,
		shared:          map[string]sharedEntry{},
		synth:           map[string]*synthView{},
		probePath:       spec.ProbePath,
		synthFhs:        map[uint64]*synthHandle{},
		nextSynthFh:     synthFhBase,
	}
	if spec.ProbePath != "" {
		fs.probe = newProbeView()
	}

	if spec.ContentSocket != "" {
		manifest, err := fetchManifest(client, spec.Domain)
		if err != nil {
			return fusekit.Config{}, fmt.Errorf("holderfs: manifest for %s: %w", spec.Domain, err)
		}
		uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
		ts := tsOf(time.Now())
		ino := sharedLinkInoBase
		for _, e := range manifest {
			switch e.Kind {
			case content.EntrySymlink:
				fs.shared[e.Name] = sharedEntry{
					target: e.Target,
					stat: fuse.Stat_t{
						Ino: ino, Mode: fuse.S_IFLNK | 0o777, Nlink: 1, Uid: uid, Gid: gid,
						Size: int64(len(e.Target)), Atim: ts, Mtim: ts, Ctim: ts, Birthtim: ts,
					},
				}
				ino++
			case content.EntryPrivate:
				fs.privateExact[e.Name] = true
			case content.EntrySynth:
				writePath := filepath.Join(spec.Base, e.Name)
				if e.Private {
					writePath = filepath.Join(spec.PrivateRoot, e.Name)
				}
				v := newSynthView(e.Name, spec.Domain, client, writePath, e.Freshness)
				v.ino = ino // stable minted fileid; never writePath's rename-churned one
				ino++
				v.seedFromWritePath() // durable last-committed bytes: no cold→warm size flap
				v.refreshOnce()       // pre-warm off the fuse path
				fs.synth["/"+e.Name] = v
			}
		}
	}

	return fusekit.Config{
		Base: spec.Base,
		Dir:  spec.Dir,
		FS:   fs,
		Options: fusekit.MountOptions{
			// No NamedAttr: the NFSv4 named-attribute vnode path is implicated in
			// macOS nfs_vinvalbuf2 kernel panics; the AppleDouble ._ fallback it
			// prevented is blocked outright instead (isAppleDouble).
			Volname:  "holder-" + filepath.Base(spec.Dir),
			NoBrowse: true,
			Extra:    []string{"rwsize=1048576"},
			// Per-mount attr-cache opt-in (default false = noattrcache). Synth
			// entries are cache-safe (v0.23.0: minted mount-lifetime inode,
			// monotonic mtime/size, open-pinned snapshots), but any top-level
			// entry the manifest does NOT classify is live passthrough to Base
			// — an external create/rewrite of an unmanifested entry after
			// mount stays torn-read-exposed under a cache. Opting in is sound
			// only when the manifest carves out every externally-mutable entry.
			AttrCache:        spec.AttrCache,
			AttrCacheTimeout: spec.AttrCacheTimeout,
		}.Build(),
		Ready:     readyFn(spec),
		Wait:      mountWait,
		FirstWait: firstMountWait,
		// ForceOnWedge stays false: the shared cmd/holder is deliberately graceful-only
		// — its death-sweep (logout, reboot, SIGTERM) must NEVER MNT_FORCE a busy mount
		// past its mapped pages, which panics the kernel (nfs_vinvalbuf2: ubc_msync failed).
		ClearCarcass: true,
	}, nil
}

// readyFn is the mount's come-up liveness probe. With a probe path it lstats THAT
// (probeView.getattr answers 0 once the NFS server is live) instead of the generic
// MountAlive, whose lexicographically-first Base entry is a PrivatePrefixes-redirected
// dotfile with no PrivateRoot backing — a clean -ENOENT that reads "not live" until
// timeout, stalling every come-up. With no probe there are no redirects, so
// MountAlive is correct.
func readyFn(spec fusekit.MountSpec) func() bool {
	dir := spec.Dir
	if spec.ProbePath == "" {
		base := spec.Base
		return func() bool { return fusekit.MountAlive(base, dir) }
	}
	probe := filepath.Join(dir, strings.TrimPrefix(spec.ProbePath, "/"))
	return func() bool {
		if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
			return false // pre-mount the dir may not exist yet
		}
		_, err := os.Lstat(probe)
		return err == nil // resolves only once the holder serves the mount
	}
}

func fetchManifest(client *content.BridgeClient, domain string) ([]content.Entry, error) {
	var err error
	for i := 0; i < manifestTries; i++ {
		var entries []content.Entry
		if entries, err = client.Manifest(context.Background(), domain); err == nil {
			return entries, nil
		}
		time.Sleep(manifestPause)
	}
	return nil, err
}

// FlushWithin drains every synth view's pending write-through before teardown,
// so a commit in flight reaches the consumer before the mount goes away.
func (fs *holderFS) FlushWithin(grace time.Duration) bool {
	ok := true
	for _, v := range fs.synth {
		if !v.flushWithin(grace) {
			ok = false
		}
	}
	return ok
}

// real maps a fuse path to its backing file: a synth entry to its writePath, a
// private name (or consumer temp prefix) under PrivateRoot, else under Base.
func (fs *holderFS) real(path string) string {
	if v, ok := fs.synth[path]; ok {
		return v.writePath
	}
	rel := filepath.FromSlash(path)
	if fs.isPrivate(topComponent(path)) {
		return filepath.Join(fs.privateRoot, rel)
	}
	return filepath.Join(fs.base, rel)
}

// isPrivate reports whether a top-level name backs onto PrivateRoot: an EntryPrivate
// name or private-prefix match, plus its AppleDouble "._<name>" sidecar (which must
// colocate with its parent).
func (fs *holderFS) isPrivate(name string) bool {
	n := strings.TrimPrefix(name, "._")
	if fs.privateExact[n] {
		return true
	}
	for _, p := range fs.privatePrefixes {
		if strings.HasPrefix(n, p) {
			return true
		}
	}
	return false
}

func topComponent(path string) string {
	p := strings.TrimPrefix(path, "/")
	if i := strings.IndexByte(p, '/'); i >= 0 {
		p = p[:i]
	}
	return p
}

func isTopLevel(path string) bool {
	p := strings.TrimPrefix(path, "/")
	return p != "" && !strings.ContainsRune(p, '/')
}

func (fs *holderFS) sharedEntryFor(path string) (sharedEntry, bool) {
	if !isTopLevel(path) {
		return sharedEntry{}, false
	}
	e, ok := fs.shared[strings.TrimPrefix(path, "/")]
	return e, ok
}

func (fs *holderFS) sharedLink(path string) (string, bool) {
	e, ok := fs.sharedEntryFor(path)
	return e.target, ok
}

// openSynth opens a read handle over a synth view's cached snapshot, capturing
// the bytes and served mtime the handle answers with until release, and pins
// the view's path attrs to that snapshot for as long as any handle is open. A
// cold cache (consumer never answered and no writePath to seed from) returns
// EIO — never a block on the bridge.
func (fs *holderFS) openSynth(v *synthView) (int, uint64) {
	buf, ok := v.currentBytes()
	if !ok {
		return -int(syscall.EIO), ^uint64(0)
	}
	mtime := v.servedMtime()
	v.pinOpen(int64(len(buf)), mtime)
	fs.synthMu.Lock()
	fh := fs.nextSynthFh
	fs.nextSynthFh++
	fs.synthFhs[fh] = &synthHandle{v: v, buf: buf, mtime: tsOf(mtime)}
	fs.synthMu.Unlock()
	return 0, fh
}

func (fs *holderFS) synthHandleFor(fh uint64) (*synthHandle, bool) {
	fs.synthMu.Lock()
	defer fs.synthMu.Unlock()
	h, ok := fs.synthFhs[fh]
	return h, ok
}

func (fs *holderFS) releaseSynthHandle(fh uint64) {
	fs.synthMu.Lock()
	h, ok := fs.synthFhs[fh]
	delete(fs.synthFhs, fh)
	fs.synthMu.Unlock()
	if ok {
		h.v.unpinOpen()
	}
}

func errno(err error) int {
	if err == nil {
		return 0
	}
	var e syscall.Errno
	if errors.As(err, &e) {
		return -int(e)
	}
	return -int(syscall.EIO)
}

func (fs *holderFS) Statfs(path string, stat *fuse.Statfs_t) int {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT)
	}
	if path == fs.probePath && fs.probe != nil {
		path = "/"
	}
	var s syscall.Statfs_t
	if err := syscall.Statfs(fs.real(path), &s); err != nil {
		return errno(err)
	}
	stat.Bsize = uint64(s.Bsize)
	stat.Frsize = uint64(s.Bsize)
	stat.Blocks = s.Blocks
	stat.Bfree = s.Bfree
	stat.Bavail = s.Bavail
	stat.Files = s.Files
	stat.Ffree = s.Ffree
	stat.Namemax = 255
	return 0
}

func (fs *holderFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT)
	}
	if fs.probe != nil && (probeFh(fh) || path == fs.probePath) {
		return fs.probe.getattr(stat)
	}
	if synthFh(fh) {
		return fs.getattrSynthHandle(fh, stat)
	}
	var st syscall.Stat_t
	if fh != ^uint64(0) {
		if err := syscall.Fstat(int(fh), &st); err != nil {
			return errno(err)
		}
		copyStat(stat, &st)
		if v, ok := fs.synth[path]; ok {
			// A writable open on a synth entry is a real writePath fd; its size is
			// the writer's own truth, but the churning real ino must never serve.
			stat.Ino = v.ino
		}
		return 0
	}
	if e, ok := fs.sharedEntryFor(path); ok {
		*stat = e.stat
		return 0
	}
	if v, ok := fs.synth[path]; ok {
		return fs.getattrSynthPath(v, stat)
	}
	if err := syscall.Lstat(fs.real(path), &st); err != nil {
		return errno(err)
	}
	copyStat(stat, &st)
	return 0
}

// getattrSynthHandle answers Getattr for a synth read handle: the writePath's
// mode/owner with the handle's open-time snapshot — the minted stable ino, the
// snapshot's size (which MUST equal what Read returns or the NFS client serves
// truncated reads), and the snapshot's mtime, so nothing ever changes under
// the open file.
func (fs *holderFS) getattrSynthHandle(fh uint64, stat *fuse.Stat_t) int {
	h, ok := fs.synthHandleFor(fh)
	if !ok {
		return -int(syscall.EBADF)
	}
	var st syscall.Stat_t
	if err := syscall.Lstat(h.v.writePath, &st); err != nil {
		return errno(err)
	}
	copyStat(stat, &st)
	stat.Ino = h.v.ino
	stat.Size = int64(len(h.buf))
	stat.Mtim = h.mtime
	return 0
}

// getattrSynthPath answers a path Getattr for a synth entry: writePath's
// mode/owner with the minted stable ino — never writePath's real fileid, which
// every atomic-rename write-through re-mints — plus the served size and
// monotonic mtime. While any read handle is open, size and mtime pin to the
// newest open's snapshot, so a background refresh never lands an invalidation
// on a file the client holds open or mapped (the nfs_vinvalbuf2 panic loop);
// the change surfaces on the first Getattr after the last close. A missing
// writePath is ENOENT, matching Readdir; a cold cache (writePath created
// through the mount before the consumer ever answered) serves the raw
// writePath size, the only size there is.
func (fs *holderFS) getattrSynthPath(v *synthView, stat *fuse.Stat_t) int {
	var st syscall.Stat_t
	if err := syscall.Lstat(v.writePath, &st); err != nil {
		return errno(err)
	}
	copyStat(stat, &st)
	stat.Ino = v.ino
	if size, mtime, ok := v.pinnedAttrs(); ok {
		stat.Size = size
		stat.Mtim = tsOf(mtime)
		return 0
	}
	if buf, ok := v.currentBytes(); ok {
		stat.Size = int64(len(buf))
	}
	stat.Mtim = tsOf(v.servedMtime())
	return 0
}

func (fs *holderFS) Open(path string, flags int) (int, uint64) {
	if isAppleDouble(path) {
		if flags&syscall.O_CREAT != 0 {
			return -int(syscall.EACCES), ^uint64(0)
		}
		return -int(syscall.ENOENT), ^uint64(0)
	}
	if path == fs.probePath && fs.probe != nil {
		return fs.probe.open(flags)
	}
	if v, ok := fs.synth[path]; ok && flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return fs.openSynth(v)
	}
	fd, err := syscall.Open(fs.real(path), flags, 0)
	if err != nil {
		return errno(err), ^uint64(0)
	}
	return 0, uint64(fd)
}

func (fs *holderFS) Create(path string, flags int, mode uint32) (int, uint64) {
	if isAppleDouble(path) {
		return -int(syscall.EACCES), ^uint64(0)
	}
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM), ^uint64(0)
	}
	fd, err := syscall.Open(fs.real(path), flags|syscall.O_CREAT, mode)
	if err != nil {
		return errno(err), ^uint64(0)
	}
	return 0, uint64(fd)
}

func (fs *holderFS) Read(_ string, buff []byte, ofst int64, fh uint64) int {
	if fs.probe != nil && probeFh(fh) {
		return fs.probe.read(fh, buff, ofst)
	}
	if synthFh(fh) {
		h, ok := fs.synthHandleFor(fh)
		if !ok {
			return -int(syscall.EBADF)
		}
		if ofst < 0 {
			return -int(syscall.EINVAL)
		}
		if ofst >= int64(len(h.buf)) {
			return 0
		}
		return copy(buff, h.buf[ofst:])
	}
	n, err := syscall.Pread(int(fh), buff, ofst)
	if err != nil {
		return errno(err)
	}
	return n
}

func (fs *holderFS) Write(path string, buff []byte, ofst int64, fh uint64) int {
	if probeFh(fh) || synthFh(fh) {
		return -int(syscall.EBADF) // probe and synth read handles are read-only
	}
	n, err := syscall.Pwrite(int(fh), buff, ofst)
	if err != nil {
		return errno(err)
	}
	if v, ok := fs.synth[path]; ok {
		v.markDirty(fh)
	}
	return n
}

func (fs *holderFS) Truncate(path string, size int64, fh uint64) int {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT)
	}
	if (path == fs.probePath && fs.probe != nil) || probeFh(fh) {
		return -int(syscall.EPERM)
	}
	if synthFh(fh) {
		return -int(syscall.EINVAL)
	}
	var err error
	if fh != ^uint64(0) {
		err = syscall.Ftruncate(int(fh), size)
		if err == nil {
			if v, ok := fs.synth[path]; ok {
				v.markDirty(fh)
			}
		}
	} else {
		err = syscall.Truncate(fs.real(path), size)
	}
	return errno(err)
}

func (fs *holderFS) Fsync(_ string, _ bool, fh uint64) int {
	if probeFh(fh) || synthFh(fh) {
		return 0
	}
	return errno(syscall.Fsync(int(fh)))
}

func (fs *holderFS) Release(path string, fh uint64) int {
	if fs.probe != nil && probeFh(fh) {
		fs.probe.release(fh)
		return 0
	}
	if synthFh(fh) {
		fs.releaseSynthHandle(fh)
		return 0
	}
	st := errno(syscall.Close(int(fh)))
	if v, ok := fs.synth[path]; ok && v.takeDirty(fh) {
		v.scheduleWriteThrough()
	}
	return st
}

func (fs *holderFS) Opendir(path string) (int, uint64) {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT), ^uint64(0)
	}
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.ENOTDIR), ^uint64(0)
	}
	fd, err := syscall.Open(fs.real(path), syscall.O_RDONLY, 0)
	if err != nil {
		return errno(err), ^uint64(0)
	}
	return 0, uint64(fd)
}

func (fs *holderFS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, _ int64, _ uint64) int {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT)
	}
	dir, err := os.Open(fs.real(path))
	if err != nil {
		return errno(err)
	}
	defer func() { _ = dir.Close() }()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return errno(err)
	}
	probeName := strings.TrimPrefix(fs.probePath, "/")
	fill(".", nil, 0)
	fill("..", nil, 0)
	seen := map[string]bool{}
	for _, name := range names {
		seen[name] = true
		if isAppleDouble(name) {
			continue // AppleDouble sidecars never list
		}
		if path == "/" && fs.probe != nil && name == probeName {
			continue // the virtual probe is never listed
		}
		if !fill(name, fs.synthStat(path, name), 0) {
			return 0
		}
	}
	if path != "/" {
		return 0
	}
	// Private files live only in PrivateRoot; merge them into the root listing.
	if priv, err := os.ReadDir(fs.privateRoot); err == nil {
		for _, e := range priv {
			if seen[e.Name()] || isAppleDouble(e.Name()) || !fs.isPrivate(e.Name()) {
				continue
			}
			seen[e.Name()] = true
			if !fill(e.Name(), fs.synthStat(path, e.Name()), 0) {
				return 0
			}
		}
	}
	// A synth entry backed in PrivateRoot but not matching a private prefix is missed
	// by both loops above; list it when its backing exists, so Readdir never names a
	// file that Getattr/open cannot resolve.
	for p, v := range fs.synth {
		name := strings.TrimPrefix(p, "/")
		if seen[name] || isAppleDouble(name) {
			continue
		}
		var st fuse.Stat_t
		if fs.getattrSynthPath(v, &st) != 0 {
			continue // no writePath backing: unlisted, matching Getattr's ENOENT
		}
		if !fill(name, &st, 0) {
			return 0
		}
	}
	return 0
}

// synthStat returns the served stat for a root directory entry backed by a
// synth view, so every Readdir fill lists the minted stable ino — a listing
// must never hand the client writePath's churning real fileid. Non-synth
// entries return nil; the client discovers their attrs via Getattr.
func (fs *holderFS) synthStat(dir, name string) *fuse.Stat_t {
	if dir != "/" {
		return nil
	}
	v, ok := fs.synth["/"+name]
	if !ok {
		return nil
	}
	var st fuse.Stat_t
	if fs.getattrSynthPath(v, &st) != 0 {
		return nil
	}
	return &st
}

func (fs *holderFS) Releasedir(_ string, fh uint64) int {
	return errno(syscall.Close(int(fh)))
}

func (fs *holderFS) Mkdir(path string, mode uint32) int {
	if isAppleDouble(path) {
		return -int(syscall.EACCES)
	}
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Mkdir(fs.real(path), mode))
}

func (fs *holderFS) Unlink(path string) int {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT)
	}
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Unlink(fs.real(path)))
}

func (fs *holderFS) Rmdir(path string) int {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT)
	}
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Rmdir(fs.real(path)))
}

func (fs *holderFS) Link(oldpath string, newpath string) int {
	if isAppleDouble(oldpath) {
		return -int(syscall.ENOENT)
	}
	if isAppleDouble(newpath) {
		return -int(syscall.EACCES)
	}
	if fs.probe != nil && (oldpath == fs.probePath || newpath == fs.probePath) {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Link(fs.real(oldpath), fs.real(newpath)))
}

func (fs *holderFS) Symlink(target string, newpath string) int {
	if isAppleDouble(newpath) {
		return -int(syscall.EACCES)
	}
	if newpath == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Symlink(target, fs.real(newpath)))
}

func (fs *holderFS) Readlink(path string) (int, string) {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT), ""
	}
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EINVAL), ""
	}
	if target, ok := fs.sharedLink(path); ok {
		return 0, target
	}
	buf := make([]byte, 4096)
	n, err := syscall.Readlink(fs.real(path), buf)
	if err != nil {
		return errno(err), ""
	}
	return 0, string(buf[:n])
}

func (fs *holderFS) Rename(oldpath string, newpath string) int {
	if isAppleDouble(oldpath) {
		return -int(syscall.ENOENT)
	}
	if isAppleDouble(newpath) {
		return -int(syscall.EACCES)
	}
	if fs.probe != nil && (oldpath == fs.probePath || newpath == fs.probePath) {
		return -int(syscall.EPERM)
	}
	st := errno(syscall.Rename(fs.real(oldpath), fs.real(newpath)))
	if st == 0 {
		if v, ok := fs.synth[newpath]; ok {
			// A consumer's atomic save (tmp + rename) committed the durable file;
			// schedule its write-through. The rename status is ALWAYS returned (the
			// commit already happened) and the RPC runs off this handler.
			v.scheduleWriteThrough()
		}
	}
	return st
}

func (fs *holderFS) Chmod(path string, mode uint32) int {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT)
	}
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Chmod(fs.real(path), mode))
}

func (fs *holderFS) Chown(path string, uid uint32, gid uint32) int {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT)
	}
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return errno(syscall.Lchown(fs.real(path), int(uid), int(gid)))
}

func (fs *holderFS) Utimens(path string, tmsp []fuse.Timespec) int {
	if isAppleDouble(path) {
		return -int(syscall.ENOENT)
	}
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	if len(tmsp) < 2 {
		return errno(syscall.EINVAL)
	}
	tv := []syscall.Timeval{
		{Sec: tmsp[0].Sec, Usec: int32(tmsp[0].Nsec / 1000)},
		{Sec: tmsp[1].Sec, Usec: int32(tmsp[1].Nsec / 1000)},
	}
	return errno(syscall.Utimes(fs.real(path), tv))
}

// The xattr ops pass through via x/sys/unix's L-variants (never following symlinks).
// The holder mounts without namedattr (see Build), so the macOS NFS client fails
// xattr ops ENOTSUP client-side and never reaches these handlers; they stay correct
// for direct-Serve and Linux consumers. The probe answers virtually (no xattrs;
// mutations EPERM).

func (fs *holderFS) Setxattr(path string, name string, value []byte, flags int) int {
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return setxattrErrno(unix.Lsetxattr(fs.real(path), name, value, flags))
}

// setxattrErrno translates ENOTSUP to EPERM — the one status that trips xnu's
// AppleDouble ._ fallback — while passing every other error through.
func setxattrErrno(err error) int {
	if errors.Is(err, unix.ENOTSUP) {
		return -int(syscall.EPERM)
	}
	return errno(err)
}

func (fs *holderFS) Getxattr(path string, name string) (int, []byte) {
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.ENOATTR), nil
	}
	backing := fs.real(path)
	for {
		sz, err := unix.Lgetxattr(backing, name, nil)
		if err != nil {
			return errno(err), nil
		}
		if sz == 0 {
			return 0, []byte{}
		}
		buf := make([]byte, sz)
		n, err := unix.Lgetxattr(backing, name, buf)
		if errors.Is(err, unix.ERANGE) {
			continue
		}
		if err != nil {
			return errno(err), nil
		}
		return 0, buf[:n]
	}
}

func (fs *holderFS) Listxattr(path string, fill func(name string) bool) int {
	if path == fs.probePath && fs.probe != nil {
		return 0
	}
	backing := fs.real(path)
	var buf []byte
	for {
		sz, err := unix.Llistxattr(backing, nil)
		if err != nil {
			return errno(err)
		}
		if sz == 0 {
			return 0
		}
		buf = make([]byte, sz)
		n, err := unix.Llistxattr(backing, buf)
		if errors.Is(err, unix.ERANGE) {
			continue
		}
		if err != nil {
			return errno(err)
		}
		buf = buf[:n]
		break
	}
	for _, name := range strings.Split(string(buf), "\x00") {
		if name == "" {
			continue
		}
		if !fill(name) {
			return 0
		}
	}
	return 0
}

func (fs *holderFS) Removexattr(path string, name string) int {
	if path == fs.probePath && fs.probe != nil {
		return -int(syscall.EPERM)
	}
	return errno(unix.Lremovexattr(fs.real(path), name))
}

// tsOf converts a time.Time to a fuse.Timespec.
func tsOf(t time.Time) fuse.Timespec {
	return fuse.Timespec{Sec: t.Unix(), Nsec: int64(t.Nanosecond())}
}

func copyStat(dst *fuse.Stat_t, src *syscall.Stat_t) {
	dst.Dev = uint64(src.Dev)
	dst.Ino = uint64(src.Ino)
	dst.Mode = uint32(src.Mode)
	dst.Nlink = uint32(src.Nlink)
	dst.Uid = src.Uid
	dst.Gid = src.Gid
	dst.Rdev = uint64(src.Rdev)
	dst.Size = src.Size
	dst.Atim = fuse.Timespec{Sec: src.Atimespec.Sec, Nsec: src.Atimespec.Nsec}
	dst.Mtim = fuse.Timespec{Sec: src.Mtimespec.Sec, Nsec: src.Mtimespec.Nsec}
	dst.Ctim = fuse.Timespec{Sec: src.Ctimespec.Sec, Nsec: src.Ctimespec.Nsec}
	dst.Birthtim = fuse.Timespec{Sec: src.Birthtimespec.Sec, Nsec: src.Birthtimespec.Nsec}
	dst.Blksize = int64(src.Blksize)
	dst.Blocks = src.Blocks
	dst.Flags = src.Flags
}
