//go:build fuse && cgo && darwin

package holderfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit"
)

// Mux mode: ONE native fuse-t mount (one go-nfsv4 server) serving every
// tenant of a MuxRoot as a subtree. muxFS routes each vnop to the attached
// tenant filesystem named by the path's first component, with the prefix
// stripped, so source-mode (holderFS) and tree-mode (treeFS) children serve
// unmodified. Per-tenant attach/detach is a logical map operation with no
// kernel involvement — the forced-unmount surface that feeds the macOS
// nfs_vinvalbuf2 panic shrinks to the one native root.
//
// Inode namespace (kernel-panic-adjacent invariant): the whole mount is one
// NFS fileid space, so tenants must never alias.
//
//   - Real backing inos (< fusekit.SynthInoFloor) pass through untouched: all
//     backing files live on one APFS volume, so they are globally unique.
//   - Child-minted synthetic inos (>= SynthInoFloor) are slot-remapped:
//     Attach allocates a monotonically increasing uint32 slot, NEVER reused
//     for the native mount's lifetime; served ino = SynthInoFloor +
//     slot*muxSlotSpan + (ino - SynthInoFloor). The TOP offset of every slot
//     (muxSlotSpan-1) is reserved for that slot's tenant root (slotRootIno), so
//     a child offset >= muxSlotSpan-1 answers -EIO (unreachable for real
//     manifests) rather than aliasing its own tenant root or a neighbor slot.
//     Slot 0 is reserved for mux-global objects (the root's own ino is
//     SynthInoFloor). Non-reuse means a re-attached tenant's new
//     objects never wear a fileid the NFS client may still cache for old
//     ones: a fileid change under a path reads "file replaced" (correct);
//     fileid REUSE for a different object is the aliasing hazard, made
//     structurally impossible.
//   - Tenant roots: every tenant's backing base can be the same real dir, so
//     Getattr("/name") delegates to the child's root Getattr and then FORCES
//     the slot's minted root ino (slotRootIno) — the real base-dir ino must
//     never serve, or ten tenant roots would alias one fileid.
//
// The remap applies at exactly the ino-bearing surfaces: Getattr results and
// Readdir fill stats. File handles need no remapping — every cgofuse op
// carries the path, so fh-bearing ops route to the owning child; fh values
// may collide numerically across children but are always resolved by the
// path-routed child.
//
// Children NEVER receive Init or Destroy: the fuse host delivers them to the
// mux root exactly once per native mount, and attach/detach happen while it
// is live. Neither holderFS nor treeFS implements them; a future child that
// does must not rely on them here.

// muxSlotSpan is one tenant slot's synthetic-ino span (2^30 minted objects
// per attach — unreachable for real manifests).
const muxSlotSpan = uint64(1) << 30

// muxReadyWait bounds one readiness os.Stat of the mux root: the probe runs
// against a just-created NFS mount that can wedge, and Ready is polled — a
// hung stat must cost one detached goroutine, never a parked caller.
var muxReadyWait = 2 * time.Second

// muxReadyProbes joins bounded readiness stats per mux root dir.
var muxReadyProbes fusekit.StatProbes[bool]

// muxChild is one attached tenant: its filesystem and its slot in the
// synthetic-ino partition.
type muxChild struct {
	slot uint32
	fs   fuse.FileSystemInterface
}

// muxFS is the mux native root filesystem. The children map is guarded by an
// RWMutex; lookups take the read lock, Attach/Detach the write lock, and no
// lock is ever held across a delegated child op (in-flight ops holding a
// child ref complete against intact backing files even after its detach).
type muxFS struct {
	fuse.FileSystemBase
	parent   string // mux root's parent dir, backing Statfs("/")
	uid, gid uint32
	mtime    fuse.Timespec // root mtime, fixed at mount

	mu       sync.RWMutex
	children map[string]*muxChild
	nextSlot uint32 // monotonic; slot 0 reserved for mux-global objects
}

var (
	_ fusekit.SubtreeHost     = (*muxFS)(nil)
	_ fusekit.PassthroughOnly = (*muxFS)(nil)
)

// FusePassthroughOnly is false: children serve handler-generated synthetic
// content keyed on file handles, which fuse-t's FSKit backend does not honor,
// so fusekit keeps the NFS backend.
func (fs *muxFS) FusePassthroughOnly() bool { return false }

func newMuxFS(dir string) *muxFS {
	return &muxFS{
		parent:   filepath.Dir(dir),
		uid:      uint32(os.Getuid()),
		gid:      uint32(os.Getgid()),
		mtime:    tsOf(time.Now()),
		children: map[string]*muxChild{},
		nextSlot: 1,
	}
}

// buildMux constructs the Config for a mux native root (ContentModeMux). The
// filesystem starts empty; MountSet attaches tenants after the mount is live.
func buildMux(spec fusekit.MountSpec) (fusekit.Config, error) {
	fs := newMuxFS(spec.Dir)
	return fusekit.Config{
		Base: spec.Base,
		Dir:  spec.Dir,
		FS:   fs,
		Options: fusekit.MountOptions{
			// No NamedAttr, as in source/tree mode: the NFSv4 named-attribute
			// vnode path is implicated in macOS nfs_vinvalbuf2 kernel panics;
			// AppleDouble ._ sidecars are blocked outright instead.
			Volname:  "holder-" + filepath.Base(spec.Dir),
			NoBrowse: true,
			Extra:    []string{"rwsize=1048576"},
			// Per-mount attr-cache opt-in (default false = noattrcache),
			// carried from the FIRST tenant's spec; MountSet refuses a later
			// tenant that disagrees (ErrMuxMismatch).
			AttrCache:        spec.AttrCache,
			AttrCacheTimeout: spec.AttrCacheTimeout,
		}.Build(),
		Ready:     muxReadyFn(spec.Dir),
		Wait:      mountWait,
		FirstWait: firstMountWait,
		// ForceOnWedge stays false — the shared holder is graceful-only (see Build).
		// inherited from the first tenant (empty = force).
		ClearCarcass: true,
	}, nil
}

// muxReadyFn is the mux root's come-up probe: the kernel mount table names
// dir (Mounted never touches the mount) plus one bounded os.Stat of dir —
// the root Getattr is synthetic, so the stat answering proves the NFS server
// is live. There is no tenant to probe yet; subtree readiness is each
// tenant's own Ready (MountSet waits on it after Attach).
func muxReadyFn(dir string) func() bool {
	return func() bool {
		if !fusekit.Mounted(dir) {
			return false
		}
		ok, answered := muxReadyProbes.Do(dir, muxReadyWait, func() bool {
			fi, err := os.Stat(dir)
			return err == nil && fi.IsDir()
		})
		return answered && ok
	}
}

// Attach registers a tenant under name, allocating its slot. Names are one
// path component; AppleDouble names are blocked at the root (W1b). A name
// already attached is refused — the caller detaches first (re-attach mints a
// NEW slot, never reusing the old one). Note the slot discipline is handler-
// side defense-in-depth only: go-nfsv4 mints its own path-keyed client
// fileids, so a re-attached tenant reclaims its pre-detach fileids — content
// coherence across the re-attach rides the fresh child's synth incarnation
// floors (mintAttrFloor, chained per writePath one nanosecond past everything
// the prior incarnation served), which advance the served-ctime baseline —
// and with it the NFSv4 change attribute — past the prior incarnation's,
// even on a back-to-back detach/attach.
func (fs *muxFS) Attach(name string, child fuse.FileSystemInterface) error {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') {
		return fmt.Errorf("holderfs: invalid mux tenant name %q", name)
	}
	if isAppleDouble(name) {
		return fmt.Errorf("holderfs: AppleDouble mux tenant name %q is blocked", name)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.children[name]; ok {
		return fmt.Errorf("holderfs: mux tenant %q is already attached", name)
	}
	if fs.nextSlot == 0 {
		// The uint32 allocator wrapped; reusing slot 0 (or any slot) would
		// alias fileids across tenant generations. Fail loud.
		return fmt.Errorf("holderfs: mux tenant slots exhausted")
	}
	fs.children[name] = &muxChild{slot: fs.nextSlot, fs: child}
	fs.nextSlot++
	return nil
}

// Detach removes the tenant: later lookups answer ENOENT; in-flight ops that
// already hold the child ref complete against intact backing files. No
// kernel involvement, no MNT_FORCE — the native mount stays untouched.
func (fs *muxFS) Detach(name string) {
	fs.mu.Lock()
	delete(fs.children, name)
	fs.mu.Unlock()
}

// Attached reports whether name is currently served.
func (fs *muxFS) Attached(name string) bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	_, ok := fs.children[name]
	return ok
}

// splitMux splits a fuse path into its tenant name and the child-relative
// rest ("/" when the path names the tenant root itself; name "" means the
// mux root).
func splitMux(path string) (name, rest string) {
	p := strings.TrimPrefix(path, "/")
	if p == "" {
		return "", "/"
	}
	if i := strings.IndexByte(p, '/'); i >= 0 {
		return p[:i], p[i:]
	}
	return p, "/"
}

// child resolves an attached tenant by name.
func (fs *muxFS) child(name string) (*muxChild, bool) {
	fs.mu.RLock()
	c, ok := fs.children[name]
	fs.mu.RUnlock()
	return c, ok
}

// route resolves path to its tenant and child-relative rest; rc != 0 when the
// tenant is not attached (or path is the mux root, which no routed op serves).
func (fs *muxFS) route(path string) (c *muxChild, rest string, rc int) {
	name, rest := splitMux(path)
	if name == "" {
		return nil, "", -int(syscall.ENOENT)
	}
	if rest == "/" && isAppleDouble(name) {
		return nil, "", -int(syscall.ENOENT)
	}
	c, ok := fs.child(name)
	if !ok {
		return nil, "", -int(syscall.ENOENT)
	}
	return c, rest, 0
}

// remapIno maps a child-served inode into the mux fileid space: real backing
// inos pass through, synthetic ones re-base into the slot's span. The top
// offset of the span (muxSlotSpan-1) is RESERVED for the tenant root
// (slotRootIno), so a child object minting it — or anything past the span —
// gets ok=false and the caller answers -EIO rather than aliasing the slot's own
// tenant root or a neighbor slot. Both bounds are unreachable for real manifests
// (a child would have to mint 2^30-1 synthetic objects).
func remapIno(slot uint32, ino uint64) (uint64, bool) {
	if ino < fusekit.SynthInoFloor {
		return ino, true
	}
	off := ino - fusekit.SynthInoFloor
	if off >= muxSlotSpan-1 {
		return 0, false
	}
	return fusekit.SynthInoFloor + uint64(slot)*muxSlotSpan + off, true
}

// slotRootIno is a tenant root's minted fileid: the top ino of its slot's span
// (slot base + muxSlotSpan - 1) — NEVER the child's real base-dir ino, which
// every tenant may share. remapIno reserves this exact offset (rejecting a child
// object that mints it with -EIO), so a tenant root can never alias one of its
// own slot's child objects.
func slotRootIno(slot uint32) uint64 {
	return fusekit.SynthInoFloor + uint64(slot)*muxSlotSpan + muxSlotSpan - 1
}

// rootStat fills the synthetic mux-root stat: a plain 0755 directory whose
// ino is SynthInoFloor (slot 0) and whose mtime is fixed at mount — attach
// and detach deliberately do not advance it (an mtime bump invalidates
// cached pages under the root, panic-adjacent churn for zero benefit).
func (fs *muxFS) rootStat(stat *fuse.Stat_t) {
	*stat = fuse.Stat_t{
		Ino:      fusekit.SynthInoFloor,
		Mode:     fuse.S_IFDIR | 0o755,
		Nlink:    2,
		Uid:      fs.uid,
		Gid:      fs.gid,
		Atim:     fs.mtime,
		Mtim:     fs.mtime,
		Ctim:     fs.mtime,
		Birthtim: fs.mtime,
		Blksize:  4096,
	}
}

// tenantRootStat fills a tenant root's served stat for the root Readdir: the
// minted directory presentation under the slot's root ino.
func (fs *muxFS) tenantRootStat(stat *fuse.Stat_t, slot uint32) {
	fs.rootStat(stat)
	stat.Ino = slotRootIno(slot)
}

// mutateRootErrno is the verdict for a mutating op on the mux root or a
// depth-1 name: tenant membership is Attach/Detach's alone, so every root-
// level mutation is refused. AppleDouble names answer their blocking errnos
// (creating ops EACCES, the rest ENOENT) so xnu's ._ fallback sees the same
// refusal shape as everywhere else in the holder.
func mutateRootErrno(name string, creating bool) int {
	if isAppleDouble(name) {
		if creating {
			return -int(syscall.EACCES)
		}
		return -int(syscall.ENOENT)
	}
	return -int(syscall.EPERM)
}

func (fs *muxFS) Statfs(path string, stat *fuse.Statfs_t) int {
	if name, _ := splitMux(path); name == "" {
		// The root's geometry is the real filesystem under the mux root's
		// parent — the volume every source-mode tenant actually writes to.
		var s syscall.Statfs_t
		if err := syscall.Statfs(fs.parent, &s); err != nil {
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
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Statfs(rest, stat)
}

func (fs *muxFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	name, rest := splitMux(path)
	if name == "" {
		fs.rootStat(stat)
		return 0
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	if rest == "/" {
		// Tenant root: the child answers with its real base-dir stat, whose
		// ino every tenant may share — force the slot's minted root ino.
		if rc := c.fs.Getattr("/", stat, fh); rc != 0 {
			return rc
		}
		stat.Ino = slotRootIno(c.slot)
		return 0
	}
	if rc := c.fs.Getattr(rest, stat, fh); rc != 0 {
		return rc
	}
	ino, ok := remapIno(c.slot, stat.Ino)
	if !ok {
		return -int(syscall.EIO)
	}
	stat.Ino = ino
	return 0
}

func (fs *muxFS) Opendir(path string) (int, uint64) {
	name, _ := splitMux(path)
	if name == "" {
		return 0, ^uint64(0) // the root is the mux's own object; nothing to open
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc, ^uint64(0)
	}
	return c.fs.Opendir(rest)
}

func (fs *muxFS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	name, _ := splitMux(path)
	if name == "" {
		fill(".", nil, 0)
		fill("..", nil, 0)
		fs.mu.RLock()
		names := make([]string, 0, len(fs.children))
		slots := make(map[string]uint32, len(fs.children))
		for n, c := range fs.children {
			names = append(names, n)
			slots[n] = c.slot
		}
		fs.mu.RUnlock()
		sort.Strings(names)
		for _, n := range names {
			var st fuse.Stat_t
			fs.tenantRootStat(&st, slots[n])
			if !fill(n, &st, 0) {
				return 0
			}
		}
		return 0
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	overflow := false
	wrapped := func(n string, st *fuse.Stat_t, o int64) bool {
		if st == nil {
			return fill(n, nil, o)
		}
		s := *st // never mutate the child's stat in place
		ino, ok := remapIno(c.slot, s.Ino)
		if !ok {
			overflow = true
			return false
		}
		s.Ino = ino
		return fill(n, &s, o)
	}
	rc = c.fs.Readdir(rest, wrapped, ofst, fh)
	if overflow {
		return -int(syscall.EIO)
	}
	return rc
}

func (fs *muxFS) Releasedir(path string, fh uint64) int {
	name, _ := splitMux(path)
	if name == "" {
		return 0
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Releasedir(rest, fh)
}

func (fs *muxFS) Fsyncdir(path string, datasync bool, fh uint64) int {
	name, _ := splitMux(path)
	if name == "" {
		return 0
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Fsyncdir(rest, datasync, fh)
}

func (fs *muxFS) Mknod(path string, mode uint32, dev uint64) int {
	name, rest := splitMux(path)
	if rest == "/" {
		return mutateRootErrno(name, true)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Mknod(rest, mode, dev)
}

func (fs *muxFS) Mkdir(path string, mode uint32) int {
	name, rest := splitMux(path)
	if rest == "/" {
		return mutateRootErrno(name, true)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Mkdir(rest, mode)
}

func (fs *muxFS) Unlink(path string) int {
	name, rest := splitMux(path)
	if rest == "/" {
		return mutateRootErrno(name, false)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Unlink(rest)
}

func (fs *muxFS) Rmdir(path string) int {
	name, rest := splitMux(path)
	if rest == "/" {
		return mutateRootErrno(name, false)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Rmdir(rest)
}

func (fs *muxFS) Link(oldpath string, newpath string) int {
	on, or := splitMux(oldpath)
	nn, nr := splitMux(newpath)
	if or == "/" {
		return mutateRootErrno(on, false)
	}
	if nr == "/" {
		return mutateRootErrno(nn, true)
	}
	if on != nn {
		return -int(syscall.EXDEV) // hard links never cross tenants
	}
	c, ok := fs.child(on)
	if !ok {
		return -int(syscall.ENOENT)
	}
	return c.fs.Link(or, nr)
}

func (fs *muxFS) Symlink(target string, newpath string) int {
	name, rest := splitMux(newpath)
	if rest == "/" {
		return mutateRootErrno(name, true)
	}
	c, rest, rc := fs.route(newpath)
	if rc != 0 {
		return rc
	}
	return c.fs.Symlink(target, rest)
}

func (fs *muxFS) Readlink(path string) (int, string) {
	name, rest := splitMux(path)
	if name == "" {
		return -int(syscall.EINVAL), ""
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc, ""
	}
	if rest == "/" {
		return -int(syscall.EINVAL), "" // a tenant root is always a directory
	}
	return c.fs.Readlink(rest)
}

func (fs *muxFS) Rename(oldpath string, newpath string) int {
	on, or := splitMux(oldpath)
	nn, nr := splitMux(newpath)
	if or == "/" {
		return mutateRootErrno(on, false)
	}
	if nr == "/" {
		return mutateRootErrno(nn, true)
	}
	if on != nn {
		return -int(syscall.EXDEV) // renames never cross tenants
	}
	c, ok := fs.child(on)
	if !ok {
		return -int(syscall.ENOENT)
	}
	return c.fs.Rename(or, nr)
}

func (fs *muxFS) Chmod(path string, mode uint32) int {
	name, rest := splitMux(path)
	if rest == "/" {
		return mutateRootErrno(name, false)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Chmod(rest, mode)
}

func (fs *muxFS) Chown(path string, uid uint32, gid uint32) int {
	name, rest := splitMux(path)
	if rest == "/" {
		return mutateRootErrno(name, false)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Chown(rest, uid, gid)
}

func (fs *muxFS) Utimens(path string, tmsp []fuse.Timespec) int {
	name, rest := splitMux(path)
	if rest == "/" {
		return mutateRootErrno(name, false)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Utimens(rest, tmsp)
}

func (fs *muxFS) Access(path string, mask uint32) int {
	name, _ := splitMux(path)
	if name == "" {
		return 0
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Access(rest, mask)
}

func (fs *muxFS) Create(path string, flags int, mode uint32) (int, uint64) {
	name, rest := splitMux(path)
	if rest == "/" {
		return mutateRootErrno(name, true), ^uint64(0)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc, ^uint64(0)
	}
	return c.fs.Create(rest, flags, mode)
}

func (fs *muxFS) Open(path string, flags int) (int, uint64) {
	name, rest := splitMux(path)
	if name == "" {
		return -int(syscall.EISDIR), ^uint64(0)
	}
	if rest == "/" && isAppleDouble(name) {
		if flags&syscall.O_CREAT != 0 {
			return -int(syscall.EACCES), ^uint64(0)
		}
		return -int(syscall.ENOENT), ^uint64(0)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc, ^uint64(0)
	}
	return c.fs.Open(rest, flags)
}

func (fs *muxFS) Truncate(path string, size int64, fh uint64) int {
	name, rest := splitMux(path)
	if rest == "/" {
		return mutateRootErrno(name, false)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Truncate(rest, size, fh)
}

func (fs *muxFS) Read(path string, buff []byte, ofst int64, fh uint64) int {
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Read(rest, buff, ofst, fh)
}

func (fs *muxFS) Write(path string, buff []byte, ofst int64, fh uint64) int {
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Write(rest, buff, ofst, fh)
}

func (fs *muxFS) Flush(path string, fh uint64) int {
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Flush(rest, fh)
}

func (fs *muxFS) Release(path string, fh uint64) int {
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Release(rest, fh)
}

func (fs *muxFS) Fsync(path string, datasync bool, fh uint64) int {
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Fsync(rest, datasync, fh)
}

func (fs *muxFS) Setxattr(path string, name string, value []byte, flags int) int {
	tenant, rest := splitMux(path)
	if rest == "/" {
		return mutateRootErrno(tenant, false)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Setxattr(rest, name, value, flags)
}

func (fs *muxFS) Getxattr(path string, name string) (int, []byte) {
	tenant, _ := splitMux(path)
	if tenant == "" {
		return -int(syscall.ENOATTR), nil // the synthetic root has no xattrs
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc, nil
	}
	return c.fs.Getxattr(rest, name)
}

func (fs *muxFS) Listxattr(path string, fill func(name string) bool) int {
	tenant, _ := splitMux(path)
	if tenant == "" {
		return 0
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Listxattr(rest, fill)
}

func (fs *muxFS) Removexattr(path string, name string) int {
	tenant, rest := splitMux(path)
	if rest == "/" {
		return mutateRootErrno(tenant, false)
	}
	c, rest, rc := fs.route(path)
	if rc != 0 {
		return rc
	}
	return c.fs.Removexattr(rest, name)
}
