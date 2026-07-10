package holderfs

// Tree mode: the fully-remote counterpart of synth.go. A tree tenant has NO
// local backing tree — every stat, listing, read, and write is answered from
// the consumer's content.Tree over the bridge. This file is the pure core the
// fuse-tagged shell (tree.go) dispatches into; it owns the semantics so the
// attr-stability and AppleDouble rules are testable without a fuse build.
//
// Two rules carried over from the nfs_vinvalbuf2 panic postmortem apply to
// every path through this file:
//
//   - W2 attr stability: inodes are minted per entry and stable for the
//     mount's life (the consumer's Entry.Ino is an identity KEY, never served
//     raw), the served mtime is a per-node high-water mark that never
//     regresses, and while any handle is open the path attrs pin to the
//     newest open's snapshot — a background refresh must never land an
//     invalidation on a file the client holds open or mapped.
//   - W1b AppleDouble blocking: "._" basenames answer EACCES on creating ops
//     and ENOENT everywhere else, and are filtered from listings — even when
//     the consumer names one.
//
// Freshness follows synth.go's serve-stale discipline: a warm node answers
// from cache and schedules its refresh OFF the handler; only the very first
// touch of a path waits on the bridge, bounded by treeOpWait and joined per
// path (fusekit.StatProbes), so a hung consumer costs one detached goroutine
// per path — never a parked fuse-t worker — and the detached fetch still
// warms the cache for the client's retry.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
)

const (
	// treeInoBase is the minted-inode pool for tree entries — the same
	// fusekit.SynthInoFloor range source mode draws sharedLinkInoBase from (a
	// holder fs instance serves one mode, so the pools never meet; same value
	// as ever). Minted IDs keep the served fileid space the holder's own: a
	// consumer re-render or rename can never re-mint a fileid under an open
	// file — and the shared floor is what the mux filesystem's slot remapping
	// keys on.
	treeInoBase = fusekit.SynthInoFloor
	// treeFhBase is the first tree handle ID, sharing synth's pool above the
	// probe range ([1<<61, 1<<62)); tree mode mints no kernel fds at all.
	treeFhBase = synthFhBase
	// treeRootTries and treeRootPause bound Build's root pre-warm, matching
	// fetchManifest's patience with a consumer that is still binding its
	// bridge socket.
	treeRootTries = 3
	treeRootPause = 500 * time.Millisecond
)

// treeOpWait bounds one fuse handler's wait on the bridge — the first touch of
// a cold path and every data-plane RPC (read/write/open/flush). It sits below
// mountd's liveProbeTimeout reasoning: a slow consumer must surface as a
// bounded EIO, never a fuse-t worker parked long enough to fail liveness.
// treeFreshFor is the served-cache freshness window: within it a node answers
// with zero RPC; past it the node still answers immediately and a refresh is
// scheduled off the handler. Vars so tests can shrink them.
var (
	treeOpWait   = 2 * time.Second
	treeFreshFor = 2 * time.Second
)

func treeFh(fh uint64) bool { return fh >= treeFhBase && fh != ^uint64(0) }

// treeStat is the served attribute snapshot for one path — the pure shape the
// fuse shell converts to a fuse.Stat_t.
type treeStat struct {
	kind  content.EntryKind
	size  int64
	mtime time.Time
	birth time.Time
	ino   uint64
}

// treeDirent is one Readdir entry: the name plus its full served stat, so a
// listing always hands the client minted inos and pinned attrs — never a
// consumer-raw fileid.
type treeDirent struct {
	name string
	st   treeStat
}

// treeNode is the cached serving state for one fuse path. All fields are
// guarded by treeView.mu; nothing here does I/O.
type treeNode struct {
	stat     content.Entry // last consumer verdict (kind/size/target/…)
	statOK   bool
	notFound bool      // cached ENOENT verdict (a verdict, not an error)
	statAt   time.Time // freshness clock for stat/notFound
	target   string    // readlink cache (seeded from Entry.Target)
	targetOK bool
	targetAt time.Time
	children []string // child names, AppleDouble-filtered, consumer order
	listOK   bool
	listAt   time.Time
	// gen counts local mutations (create/write/truncate/unlink/rename/mkdir).
	// A fetch samples it before its RPC and discards a result that raced a
	// newer local mutation — a late stat must never flap the served size back.
	gen uint64
	// ino is the minted fileid, fixed for the mount's life (mintInoLocked).
	ino uint64
	// mtimeHWM is the highest mtime ever served; it never regresses even when
	// the consumer's reported mtime does.
	mtimeHWM time.Time
	birth    time.Time // first non-zero birth seen; stable thereafter
	// openPins/pinSize/pinMtime: while any handle is open, path Getattr and
	// Readdir serve the pinned (size, mtime) — the newest open or write wins,
	// and the pin never retreats (synthView's rule, same rationale).
	openPins int
	pinSize  int64
	pinMtime time.Time
	lastErr  string // last logged fetch failure, deduped
}

// treeHandle is one open fuse handle. A token handle (remote != nil) forwards
// per-chunk reads and writes to the consumer's token-keyed snapshot/edit
// buffer; a tokenless handle (plain-Tree consumer) captures a local snapshot
// buffer at open — reads never tear across a consumer commit — and mirrors
// its own writes into it so read-your-writes holds while each write is
// forwarded path-wise. Fields are guarded by treeView.hmu.
type treeHandle struct {
	path   string
	node   *treeNode
	remote *content.Handle // nil = tokenless
	buf    []byte          // tokenless snapshot + local edit mirror
	size   int64
	mtime  time.Time
	dirty  bool // token handle holds uncommitted consumer-side edits
}

// treeView serves one tree-mode mount: the node cache, the minted-ino
// registry, and the open handles. It holds no consumer domain knowledge.
type treeView struct {
	domain string
	client *content.BridgeClient

	mu      sync.Mutex
	nodes   map[string]*treeNode
	inos    map[string]uint64 // identity key -> minted ino
	nextIno uint64

	hmu     sync.Mutex
	handles map[uint64]*treeHandle
	nextFh  uint64

	// flights joins concurrent fetches per cache key ("s:"/"l:"/"r:" + path)
	// and bounds the caller's wait; a timed-out fetch keeps running detached
	// and warms the cache for the retry. The int is the fetch's errno verdict.
	flights fusekit.StatProbes[int]
}

func newTreeView(domain string, client *content.BridgeClient) *treeView {
	v := &treeView{
		domain:  domain,
		client:  client,
		nodes:   map[string]*treeNode{},
		inos:    map[string]uint64{},
		nextIno: treeInoBase,
		handles: map[uint64]*treeHandle{},
		nextFh:  treeFhBase,
	}
	// The root is the holder's own object, not consumer data: it exists by
	// definition, its dir-ness is structural, and its times start at build and
	// advance monotonically with membership changes — so "/" never needs (or
	// issues) a bridge Stat.
	now := time.Now()
	root := v.nodeLocked("/")
	root.stat = content.Entry{Name: "/", Kind: content.EntryDir}
	root.statOK = true
	root.statAt = now
	root.mtimeHWM = now
	root.birth = now
	root.ino = v.mintInoLocked("/", content.Entry{})
	return v
}

// opCtx bounds one handler-path bridge RPC by treeOpWait.
func opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), treeOpWait)
}

// classOf extracts the wire error class, or "" for a class-less error.
func classOf(err error) string {
	var ce content.ClassedError
	if errors.As(err, &ce) {
		return ce.Class()
	}
	return ""
}

// classErrno maps a bridge failure to a fuse errno. The read-side default is
// EIO: transient classes, class-less failures, and an unreachable bridge all
// read as an I/O fault, never a verdict.
func classErrno(err error) int {
	switch classOf(err) {
	case content.ClassNotFound:
		return -int(syscall.ENOENT)
	case content.ClassInvalid:
		return -int(syscall.EINVAL)
	case content.ClassPerm:
		return -int(syscall.EPERM)
	}
	return -int(syscall.EIO)
}

// writeErrno is classErrno for mutations: a capability miss — a read-only
// Tree, or an old server that predates the write ops — answers EROFS, the
// honest read-only-filesystem verdict.
func writeErrno(err error) int {
	if content.IsUnsupported(err) {
		return -int(syscall.EROFS)
	}
	return classErrno(err)
}

func errnoOf(err error) int {
	if err == nil {
		return 0
	}
	return classErrno(err)
}

// --- node cache -------------------------------------------------------------

// nodeLocked returns path's node, creating a blank one. Caller holds mu.
func (v *treeView) nodeLocked(p string) *treeNode {
	n, ok := v.nodes[p]
	if !ok {
		n = &treeNode{}
		v.nodes[p] = n
	}
	return n
}

// mintInoLocked returns the minted ino for an entry, keyed by the consumer's
// stable identity (Entry.Ino) when it supplies one — so the minted fileid
// follows the object across renames — else by path. Minted once, fixed for
// the mount's life, and NEVER the consumer's raw value. Caller holds mu.
func (v *treeView) mintInoLocked(p string, e content.Entry) uint64 {
	key := "p:" + p
	if e.Ino != 0 {
		key = fmt.Sprintf("i:%d", e.Ino)
	}
	if ino, ok := v.inos[key]; ok {
		return ino
	}
	ino := v.nextIno
	v.nextIno++
	v.inos[key] = ino
	return ino
}

// rekeyInoLocked moves a node's minted ino from its path key to the
// consumer's identity key the moment the identity is first learned. A file
// created through the mount is path-keyed until the consumer assigns its
// entity identity (typically at first commit); without the re-key the stale
// path key would hand the SAME minted fileid to the next file created at that
// path once this one is renamed away — the editor atomic-save flow (write
// x.tmp, rename onto x, write x.tmp again) would leave two live files sharing
// one served fileid. When the identity is already registered (the consumer
// claims two paths are one entity — a contract violation), the node keeps its
// own minted ino and only the stale path key dies: a minted fileid never
// changes mid-life. Caller holds mu.
func (v *treeView) rekeyInoLocked(n *treeNode, p string, id uint64) {
	delete(v.inos, "p:"+p)
	key := fmt.Sprintf("i:%d", id)
	if _, ok := v.inos[key]; !ok {
		v.inos[key] = n.ino
	}
}

// storeStatLocked replaces a node's consumer verdict, minting its ino on
// first sight — re-keying it when the consumer's identity arrives later —
// and rolling the monotonic attr floors forward.
func (v *treeView) storeStatLocked(n *treeNode, p string, e content.Entry) {
	prevIno := n.stat.Ino
	n.stat = e
	n.statOK = true
	n.notFound = false
	n.statAt = time.Now()
	if n.ino == 0 {
		n.ino = v.mintInoLocked(p, e)
	} else if e.Ino != 0 && prevIno == 0 {
		v.rekeyInoLocked(n, p, e.Ino)
	}
	if mt := time.Unix(0, e.Mtime); e.Mtime != 0 && mt.After(n.mtimeHWM) {
		n.mtimeHWM = mt
	}
	// A consumer that supplies no Mtime must not serve the epoch: seed the
	// high-water mark once at first sight; it stays stable (and monotonic)
	// until a real consumer time or local write moves it.
	if n.mtimeHWM.IsZero() {
		n.mtimeHWM = time.Now()
	}
	if n.birth.IsZero() {
		switch {
		case e.Birth != 0:
			n.birth = time.Unix(0, e.Birth)
		case e.Mtime != 0:
			n.birth = time.Unix(0, e.Mtime)
		default:
			n.birth = time.Now()
		}
	}
	if e.Target != "" {
		n.target, n.targetOK, n.targetAt = e.Target, true, n.statAt
	}
}

// rawStatLocked composes the node's stat from the cache truth alone: minted
// ino, monotonic mtime — no open pin.
func (v *treeView) rawStatLocked(n *treeNode) treeStat {
	return treeStat{kind: n.stat.Kind, size: n.stat.Size, mtime: n.mtimeHWM, birth: n.birth, ino: n.ino}
}

// servedStatLocked composes the stat the client sees: the cache truth, and —
// while any handle is open — the pinned size/mtime.
func (v *treeView) servedStatLocked(n *treeNode) treeStat {
	st := v.rawStatLocked(n)
	if n.openPins > 0 {
		st.size, st.mtime = n.pinSize, n.pinMtime
	}
	return st
}

// bumpMtimeLocked rolls a node's mtime high-water mark to now, always forward.
func (n *treeNode) bumpMtimeLocked() {
	if now := time.Now(); now.After(n.mtimeHWM) {
		n.mtimeHWM = now
	} else {
		n.mtimeHWM = n.mtimeHWM.Add(time.Nanosecond)
	}
}

// pin/unpin mirror synthView's open-attr pin: the newest open or write wins
// and the pin never retreats to an elder handle's snapshot.
func (n *treeNode) pinOpenLocked(size int64, mtime time.Time) {
	n.openPins++
	n.pinSize, n.pinMtime = size, mtime
}

func (n *treeNode) pinWriteLocked(size int64, mtime time.Time) {
	if n.openPins > 0 {
		n.pinSize, n.pinMtime = size, mtime
	}
}

func (n *treeNode) unpinLocked() { n.openPins-- }

// logFetchErr logs a fetch failure once per distinct message; the node keeps
// serving its last-good state, so the log is the only trace of a sick consumer.
func (v *treeView) logFetchErr(n *treeNode, what, p string, err error) {
	v.mu.Lock()
	logIt := n.lastErr != err.Error()
	n.lastErr = err.Error()
	v.mu.Unlock()
	if logIt {
		log.Printf("holderfs: tree %s %s/%s: %v", what, v.domain, p, err)
	}
}

// --- fetch + freshness ------------------------------------------------------

// fetchStat pulls path's consumer stat and stores the verdict — including a
// cached ClassNotFound — unless a local mutation outran the RPC (gen guard):
// the local truth is newer and a stale answer must never flap the served size.
func (v *treeView) fetchStat(p string) error {
	v.mu.Lock()
	n := v.nodeLocked(p)
	gen := n.gen
	v.mu.Unlock()

	e, err := v.client.Stat(context.Background(), v.domain, p)

	v.mu.Lock()
	defer v.mu.Unlock()
	if n.gen != gen {
		return nil
	}
	switch {
	case err == nil:
		v.storeStatLocked(n, p, e)
		return nil
	case classOf(err) == content.ClassNotFound:
		n.statOK, n.notFound, n.statAt = false, true, time.Now()
		return err
	default:
		// Transient: never cached; the next touch retries.
		return err
	}
}

// fetchList pulls a directory's children, filters AppleDouble names (W1b: a
// consumer-supplied "._" entry must never list), seeds first-sight child
// stats from the entries — the READDIRPLUS warm-up that spares a stat RPC per
// child — and bumps the dir's mtime when membership changed.
func (v *treeView) fetchList(p string) error {
	v.mu.Lock()
	n := v.nodeLocked(p)
	gen := n.gen
	v.mu.Unlock()

	entries, err := v.client.List(context.Background(), v.domain, p)

	v.mu.Lock()
	defer v.mu.Unlock()
	if n.gen != gen {
		return nil
	}
	if err != nil {
		if classOf(err) == content.ClassNotFound {
			n.statOK, n.listOK, n.notFound = false, false, true
			n.statAt, n.listAt = time.Now(), time.Now()
		}
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if isAppleDouble(e.Name) {
			continue
		}
		names = append(names, e.Name)
		child := v.nodeLocked(path.Join(p, e.Name))
		// Seed only first sight: a child's own stat verdict (or a local
		// mutation) is newer truth than its parent's listing row.
		if !child.statOK && child.gen == 0 {
			v.storeStatLocked(child, path.Join(p, e.Name), e)
		}
	}
	if !equalNames(n.children, names) {
		n.bumpMtimeLocked()
	}
	n.children = names
	n.listOK, n.listAt = true, time.Now()
	n.notFound = false
	// A successful List proves dir-ness even when the node was never statted.
	if !n.statOK {
		v.storeStatLocked(n, p, content.Entry{Name: path.Base(p), Kind: content.EntryDir})
	}
	return nil
}

// fetchReadlink pulls and caches a symlink target.
func (v *treeView) fetchReadlink(p string) error {
	target, err := v.client.Readlink(context.Background(), v.domain, p)
	v.mu.Lock()
	defer v.mu.Unlock()
	n := v.nodeLocked(p)
	if err != nil {
		return err
	}
	n.target, n.targetOK, n.targetAt = target, true, time.Now()
	return nil
}

// ensure is the serve-stale core shared by stat/list/readlink: a warm cache
// answers at once (scheduling an off-handler refresh when past treeFreshFor);
// only a cold cache waits, bounded by treeOpWait and joined per key. ok=false
// means the cold fetch did not answer in time — the caller fails EIO while
// the detached fetch warms the cache for the retry.
func (v *treeView) ensure(key string, warm func() (fresh, cached bool), fetch func() error) (errno int) {
	fresh, cached := warm()
	if cached {
		if !fresh {
			v.flights.Do(key, 0, func() int { return errnoOf(fetch()) })
		}
		return 0
	}
	rc, ok := v.flights.Do(key, treeOpWait, func() int { return errnoOf(fetch()) })
	if !ok {
		return -int(syscall.EIO)
	}
	return rc
}

func (v *treeView) ensureStat(p string) int {
	if p == "/" {
		return 0 // structural: never fetched, never stale
	}
	return v.ensure("s:"+p, func() (bool, bool) {
		v.mu.Lock()
		defer v.mu.Unlock()
		n := v.nodeLocked(p)
		cached := n.statOK || n.notFound
		return time.Since(n.statAt) < treeFreshFor, cached
	}, func() error {
		err := v.fetchStat(p)
		if err != nil && classOf(err) != content.ClassNotFound {
			v.logFetchErr(v.node(p), "stat", p, err)
		}
		return err
	})
}

func (v *treeView) ensureList(p string) int {
	return v.ensure("l:"+p, func() (bool, bool) {
		v.mu.Lock()
		defer v.mu.Unlock()
		n := v.nodeLocked(p)
		cached := n.listOK || n.notFound
		return time.Since(n.listAt) < treeFreshFor, cached
	}, func() error {
		err := v.fetchList(p)
		if err != nil && classOf(err) != content.ClassNotFound {
			v.logFetchErr(v.node(p), "list", p, err)
		}
		return err
	})
}

func (v *treeView) node(p string) *treeNode {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.nodeLocked(p)
}

func equalNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- build-time helpers -----------------------------------------------------

// prewarmRoot fetches the root listing, retrying while the consumer's bridge
// comes up. It FAILS LOUD — an unreachable or Tree-less consumer must fail
// the mount (mountd classifies ErrBridgeUnavailable as content-unavailable),
// never degrade into an empty tree.
func (v *treeView) prewarmRoot() error {
	var err error
	for i := 0; i < treeRootTries; i++ {
		if err = v.fetchList("/"); err == nil {
			return nil
		}
		if content.IsUnsupported(err) {
			return fmt.Errorf("consumer source does not implement content.Tree: %w", err)
		}
		time.Sleep(treeRootPause)
	}
	return err
}

// sweepHandles is the crash-recovery release-all documented on
// content.HandleTree: tokens leaked by a prior holder generation die on this
// generation's first call. A plain-Tree consumer answers IsUnsupported —
// tokenless mode, nothing to sweep.
func (v *treeView) sweepHandles() error {
	err := v.client.ReleaseAllHandles(context.Background(), v.domain)
	if err != nil && !content.IsUnsupported(err) {
		return fmt.Errorf("release stale handles for %s: %w", v.domain, err)
	}
	return nil
}

// --- vnop cores ---------------------------------------------------------------

// statPath resolves a path's served stat. AppleDouble names never resolve
// (W1b). pinned selects the client-facing view (the open pin applies); open
// passes false — it needs only existence and kind, and never an elder open's
// pinned view: the new handle's size comes from its own snapshot (newHandle),
// not from any cached stat.
func (v *treeView) statPath(p string, pinned bool) (treeStat, int) {
	if isAppleDouble(p) {
		return treeStat{}, -int(syscall.ENOENT)
	}
	if rc := v.ensureStat(p); rc != 0 {
		return treeStat{}, rc
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	n := v.nodeLocked(p)
	if n.notFound || !n.statOK {
		return treeStat{}, -int(syscall.ENOENT)
	}
	if pinned {
		return v.servedStatLocked(n), 0
	}
	return v.rawStatLocked(n), 0
}

// getattr answers a path Getattr with the client-facing (pinned) view.
func (v *treeView) getattr(p string) (treeStat, int) { return v.statPath(p, true) }

// getattrHandle answers Getattr for an open handle: the handle's own size and
// mtime (its writes are its truth) under the node's minted ino — nothing
// changes under the open file except what the handle itself did.
func (v *treeView) getattrHandle(fh uint64) (treeStat, int) {
	v.hmu.Lock()
	h, ok := v.handles[fh]
	if !ok {
		v.hmu.Unlock()
		return treeStat{}, -int(syscall.EBADF)
	}
	size, mtime := h.size, h.mtime
	n := h.node
	v.hmu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()
	return treeStat{kind: n.stat.Kind, size: size, mtime: mtime, birth: n.birth, ino: n.ino}, 0
}

// readdir lists a directory from the cached (or bounded-fetched) child set,
// serving each child's full minted-ino stat.
func (v *treeView) readdir(p string) ([]treeDirent, int) {
	if isAppleDouble(p) {
		return nil, -int(syscall.ENOENT)
	}
	if rc := v.ensureList(p); rc != 0 {
		return nil, rc
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	n := v.nodeLocked(p)
	if n.notFound {
		return nil, -int(syscall.ENOENT)
	}
	out := make([]treeDirent, 0, len(n.children))
	for _, name := range n.children {
		child := v.nodeLocked(path.Join(p, name))
		out = append(out, treeDirent{name: name, st: v.servedStatLocked(child)})
	}
	return out, 0
}

// opendir verifies p is a servable directory.
func (v *treeView) opendir(p string) int {
	if isAppleDouble(p) {
		return -int(syscall.ENOENT)
	}
	st, rc := v.getattr(p)
	if rc != 0 {
		return rc
	}
	if st.kind != content.EntryDir {
		return -int(syscall.ENOTDIR)
	}
	return 0
}

// readlink resolves a symlink target, cached with the same freshness rules.
func (v *treeView) readlink(p string) (string, int) {
	if isAppleDouble(p) {
		return "", -int(syscall.ENOENT)
	}
	rc := v.ensure("r:"+p, func() (bool, bool) {
		v.mu.Lock()
		defer v.mu.Unlock()
		n := v.nodeLocked(p)
		return time.Since(n.targetAt) < treeFreshFor, n.targetOK
	}, func() error { return v.fetchReadlink(p) })
	if rc != 0 {
		return "", rc
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.nodeLocked(p).target, 0
}

// open opens a handle on an existing file: a consumer token when the source
// implements HandleTree, else a tokenless local snapshot. O_CREAT on a
// missing name routes through create.
func (v *treeView) open(p string, flags int) (uint64, int) {
	if isAppleDouble(p) {
		if flags&syscall.O_CREAT != 0 {
			return 0, -int(syscall.EACCES)
		}
		return 0, -int(syscall.ENOENT)
	}
	st, rc := v.statPath(p, false)
	if rc == -int(syscall.ENOENT) && flags&syscall.O_CREAT != 0 {
		return v.create(p)
	}
	if rc != 0 {
		return 0, rc
	}
	if st.kind == content.EntryDir {
		return 0, -int(syscall.EISDIR)
	}
	fh, errno := v.newHandle(p)
	if errno != 0 {
		return 0, errno
	}
	if flags&syscall.O_TRUNC != 0 && flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		if rc := v.truncateHandle(fh, 0); rc != 0 {
			v.release(fh)
			return 0, rc
		}
	}
	return fh, 0
}

// create makes p exist consumer-side and opens a writable handle on it.
func (v *treeView) create(p string) (uint64, int) {
	if isAppleDouble(p) {
		return 0, -int(syscall.EACCES)
	}
	ctx, cancel := opCtx()
	err := v.client.Create(ctx, v.domain, p)
	cancel()
	if err != nil {
		return 0, writeErrno(err)
	}
	v.mu.Lock()
	n := v.applyEntryLocked(p, content.Entry{Name: path.Base(p), Kind: content.EntrySynth})
	n.bumpMtimeLocked()
	v.mu.Unlock()
	return v.newHandle(p)
}

// newHandle acquires a consumer token for p, or — when the consumer does not
// implement HandleTree — captures a tokenless snapshot of its bytes. Token
// misses NEVER silently downgrade a live token consumer: only the
// IsUnsupported capability verdict selects tokenless mode.
//
// The handle's size is the SNAPSHOT's — the open reply's Entry for a token,
// len(buf) for a tokenless copy — never the stat cache's: the kernel caps
// reads at the size served at open, and the cache can straddle a consumer
// commit (the serve-stale window is unbounded after idle), so a cached size
// would tear reads against the snapshot's bytes — a grown snapshot read
// truncated to the old length, a shrunk one zero-padded.
func (v *treeView) newHandle(p string) (uint64, int) {
	v.mu.Lock()
	n := v.nodeLocked(p)
	gen := n.gen
	v.mu.Unlock()

	ctx, cancel := opCtx()
	remote, err := v.client.OpenHandle(ctx, v.domain, p)
	cancel()
	var buf []byte
	var size int64
	switch {
	case err == nil:
		size = remote.Snapshot.Size
	case content.IsUnsupported(err):
		remote = nil
		var rerr error
		if buf, rerr = v.readAll(p); rerr != nil {
			return 0, classErrno(rerr)
		}
		size = int64(len(buf))
	default:
		return 0, classErrno(err)
	}

	v.mu.Lock()
	if remote != nil {
		v.absorbSnapshotLocked(n, p, remote.Snapshot, gen)
	}
	mtime := n.mtimeHWM
	n.pinOpenLocked(size, mtime)
	v.mu.Unlock()

	h := &treeHandle{path: p, node: n, remote: remote, buf: buf, size: size, mtime: mtime}
	v.hmu.Lock()
	fh := v.nextFh
	v.nextFh++
	v.handles[fh] = h
	v.hmu.Unlock()
	return fh, 0
}

// absorbSnapshotLocked folds an open reply's snapshot entry into the node: the
// snapshot mtime rolls the monotonic high-water mark forward, a first-seen
// consumer identity re-keys the minted ino, and — gen-guarded, exactly like a
// fetchStat verdict — the cached size follows the snapshot, so the path stat
// cannot keep serving a size staler than the bytes the new handle reads.
// Caller holds mu.
func (v *treeView) absorbSnapshotLocked(n *treeNode, p string, e content.Entry, gen uint64) {
	if mt := time.Unix(0, e.Mtime); e.Mtime != 0 && mt.After(n.mtimeHWM) {
		n.mtimeHWM = mt
	}
	// n.ino != 0 excludes a node a racing rename blanked between the open's
	// stat and its token acquisition: there is no minted ino to re-key yet.
	if e.Ino != 0 && n.ino != 0 && n.stat.Ino == 0 {
		v.rekeyInoLocked(n, p, e.Ino)
		n.stat.Ino = e.Ino
	}
	if n.gen == gen {
		n.stat.Size = e.Size
		n.statAt = time.Now()
	}
}

// readAll snapshots a tokenless file's bytes at open, chunked; each chunk RPC
// is bounded so a hung consumer fails the open instead of parking the worker.
func (v *treeView) readAll(p string) ([]byte, error) {
	const chunk = 1 << 20
	var out []byte
	for {
		ctx, cancel := opCtx()
		data, err := v.client.ReadAt(ctx, v.domain, p, int64(len(out)), chunk)
		cancel()
		if err != nil {
			return nil, err
		}
		out = append(out, data...)
		if len(data) < chunk {
			return out, nil
		}
	}
}

// read serves one chunk: token handles forward to the consumer's open-time
// snapshot; tokenless handles copy from the local one. The tokenless copy runs
// UNDER hmu: a concurrent write or truncate on the same handle mutates the
// snapshot's backing array in place (completeWrite's bufWriteAt/bufResize hold
// this lock), so an unlocked copy is a data race — a torn read. The copy is
// memory-to-memory, so no I/O is held under the lock; only the token RPC runs
// outside it.
func (v *treeView) read(fh uint64, buff []byte, ofst int64) int {
	v.hmu.Lock()
	h, ok := v.handles[fh]
	if !ok {
		v.hmu.Unlock()
		return -int(syscall.EBADF)
	}
	if h.remote == nil {
		defer v.hmu.Unlock()
		if ofst < 0 {
			return -int(syscall.EINVAL)
		}
		if ofst >= int64(len(h.buf)) {
			return 0
		}
		return copy(buff, h.buf[ofst:])
	}
	remote := h.remote
	v.hmu.Unlock()
	if ofst < 0 {
		return -int(syscall.EINVAL)
	}
	ctx, cancel := opCtx()
	data, err := remote.ReadAt(ctx, ofst, len(buff))
	cancel()
	if err != nil {
		return classErrno(err)
	}
	return copy(buff, data)
}

// write forwards one chunk — WriteAtHandle for token handles, path-wise for
// tokenless (mirrored into the local snapshot for read-your-writes) — then
// rolls the handle's size/mtime and the node pin forward.
func (v *treeView) write(fh uint64, buff []byte, ofst int64) int {
	v.hmu.Lock()
	h, ok := v.handles[fh]
	if !ok {
		v.hmu.Unlock()
		return -int(syscall.EBADF)
	}
	remote, p := h.remote, h.path
	v.hmu.Unlock()
	if ofst < 0 {
		return -int(syscall.EINVAL)
	}

	ctx, cancel := opCtx()
	var err error
	if remote != nil {
		err = remote.WriteAt(ctx, ofst, buff)
	} else {
		err = v.client.WriteAt(ctx, v.domain, p, ofst, buff)
	}
	cancel()
	if err != nil {
		return writeErrno(err)
	}
	v.completeWrite(h, func() {
		if h.remote == nil {
			h.buf = bufWriteAt(h.buf, ofst, buff)
		}
		if end := ofst + int64(len(buff)); end > h.size {
			h.size = end
		}
	})
	return len(buff)
}

// truncateHandle resizes through an open handle.
func (v *treeView) truncateHandle(fh uint64, size int64) int {
	v.hmu.Lock()
	h, ok := v.handles[fh]
	if !ok {
		v.hmu.Unlock()
		return -int(syscall.EBADF)
	}
	remote, p := h.remote, h.path
	v.hmu.Unlock()

	ctx, cancel := opCtx()
	var err error
	if remote != nil {
		err = remote.Truncate(ctx, size)
	} else {
		err = v.client.Truncate(ctx, v.domain, p, size)
	}
	cancel()
	if err != nil {
		return writeErrno(err)
	}
	v.completeWrite(h, func() {
		if h.remote == nil {
			h.buf = bufResize(h.buf, size)
		}
		h.size = size
	})
	return 0
}

// completeWrite applies a successful mutation's local bookkeeping: handle
// state under hmu, then the node's gen/pin/mtime under mu — the writer's own
// truth serves until the post-close refresh.
func (v *treeView) completeWrite(h *treeHandle, apply func()) {
	now := time.Now()
	v.hmu.Lock()
	apply()
	h.mtime = now
	if h.remote != nil {
		h.dirty = true
	}
	size := h.size
	v.hmu.Unlock()

	v.mu.Lock()
	h.node.gen++
	h.node.bumpMtimeLocked()
	h.node.stat.Size = size
	h.node.statAt = now
	h.node.pinWriteLocked(size, h.node.mtimeHWM)
	v.mu.Unlock()
}

// truncatePath resizes without an open handle.
func (v *treeView) truncatePath(p string, size int64) int {
	if isAppleDouble(p) {
		return -int(syscall.ENOENT)
	}
	ctx, cancel := opCtx()
	err := v.client.Truncate(ctx, v.domain, p, size)
	cancel()
	if err != nil {
		return writeErrno(err)
	}
	v.mu.Lock()
	n := v.nodeLocked(p)
	n.gen++
	n.stat.Size = size
	n.statAt = time.Now()
	n.bumpMtimeLocked()
	n.pinWriteLocked(size, n.mtimeHWM)
	v.mu.Unlock()
	return 0
}

// flush forwards the commit verdict for a dirty token handle — the one op
// whose error a writer sees at its fsync/close boundary (a fuse Release
// status is kernel-discarded). Tokenless writes were already committed
// per-chunk, so there is nothing to flush.
func (v *treeView) flush(fh uint64) int {
	v.hmu.Lock()
	h, ok := v.handles[fh]
	if !ok {
		v.hmu.Unlock()
		return -int(syscall.EBADF)
	}
	remote, dirty := h.remote, h.dirty
	v.hmu.Unlock()
	if remote == nil || !dirty {
		return 0
	}
	ctx, cancel := opCtx()
	err := remote.Flush(ctx)
	cancel()
	if err != nil {
		return writeErrno(err)
	}
	v.hmu.Lock()
	h.dirty = false
	v.hmu.Unlock()
	return 0
}

// release drops the handle: the consumer token is released (its backstop
// commit is the consumer's; a failure is logged, never surfaced — the kernel
// discards Release status), the attr pin lifts, and a post-close refresh is
// scheduled so the consumer's committed truth surfaces.
func (v *treeView) release(fh uint64) int {
	v.hmu.Lock()
	h, ok := v.handles[fh]
	delete(v.handles, fh)
	v.hmu.Unlock()
	if !ok {
		return -int(syscall.EBADF)
	}
	if h.remote != nil {
		ctx, cancel := opCtx()
		if err := h.remote.Release(ctx); err != nil {
			log.Printf("holderfs: tree release %s/%s: %v", v.domain, h.path, err)
		}
		cancel()
	}
	v.mu.Lock()
	h.node.unpinLocked()
	v.mu.Unlock()
	v.flights.Do("s:"+h.path, 0, func() int { return errnoOf(v.fetchStat(h.path)) })
	return 0
}

// unlink removes a name consumer-side and from the served namespace.
func (v *treeView) unlink(p string) int {
	if isAppleDouble(p) {
		return -int(syscall.ENOENT)
	}
	ctx, cancel := opCtx()
	err := v.client.Unlink(ctx, v.domain, p)
	cancel()
	if err != nil {
		return writeErrno(err)
	}
	v.mu.Lock()
	v.applyRemoveLocked(p)
	v.mu.Unlock()
	return 0
}

// mkdir creates a directory consumer-side and seeds it locally.
func (v *treeView) mkdir(p string) int {
	if isAppleDouble(p) {
		return -int(syscall.EACCES)
	}
	ctx, cancel := opCtx()
	err := v.client.Mkdir(ctx, v.domain, p)
	cancel()
	if err != nil {
		return writeErrno(err)
	}
	v.mu.Lock()
	n := v.applyEntryLocked(p, content.Entry{Name: path.Base(p), Kind: content.EntryDir})
	n.listOK, n.listAt, n.children = true, time.Now(), nil
	v.mu.Unlock()
	return 0
}

// rename moves a name and, when it is a directory, its whole cached subtree.
// The minted ino follows every object: consumer-keyed identities move by
// construction (they are keyed "i:<id>", path-independent); path-keyed ones
// are transferred to the new path, so a rename never re-mints a fileid — a
// descendant held open across a parent rename keeps its served fileid (W2
// forbids invalidation under an open file) and a readdir of the new path never
// mints blank dirents. Each moved node's gen bumps so an in-flight fetch keyed
// on the old path discards its result. The dest name's own path key dies with
// the object it named — a replaced dest's fileid must never resurrect under a
// file later created at that path.
func (v *treeView) rename(oldp, newp string) int {
	if isAppleDouble(oldp) {
		return -int(syscall.ENOENT)
	}
	if isAppleDouble(newp) {
		return -int(syscall.EACCES)
	}
	ctx, cancel := opCtx()
	err := v.client.Rename(ctx, v.domain, oldp, newp)
	cancel()
	if err != nil {
		return writeErrno(err)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	// Materialize the source so the collect-and-move below always includes it
	// (the fuse layer only renames a looked-up vnode, but keep the invariant
	// explicit: the top node must land at newp).
	v.nodeLocked(oldp)
	// A successful rename RPC guarantees the oldp and newp subtrees are disjoint
	// (you cannot rename a dir into its own descendant, nor onto a non-empty
	// ancestor), so no moved-to key (rooted at newp) collides with a not-yet-
	// moved source key (rooted at oldp). Collect the source keys before mutating
	// v.nodes — ranging a map while deleting from it is unsafe.
	var moved []string
	for k := range v.nodes {
		if k == oldp || strings.HasPrefix(k, oldp+"/") {
			moved = append(moved, k)
		}
	}
	for _, k := range moved {
		nk := newp + k[len(oldp):]
		n := v.nodes[k]
		n.gen++
		// The rename proves the object exists at nk: clear any not-found a
		// racing release-refresh cached while the consumer-side move deleted oldp.
		n.notFound = false
		delete(v.inos, "p:"+nk)
		// Only path-keyed ("p:") entries move; identity-keyed ("i:<id>") ones
		// are path-independent and need no move. Transferring the path key keeps
		// the minted fileid alive at the new path.
		if n.stat.Ino == 0 && n.ino != 0 {
			delete(v.inos, "p:"+k)
			v.inos["p:"+nk] = n.ino
		}
		if dst, ok := v.nodes[nk]; ok && dst != n {
			dst.gen++ // a racing fetch for the replaced dest must not resurrect it
		}
		delete(v.nodes, k)
		v.nodes[nk] = n
	}
	v.nodes[newp].statAt = time.Now()
	v.removeChildLocked(path.Dir(oldp), path.Base(oldp))
	v.addChildLocked(path.Dir(newp), path.Base(newp))
	// The old top path caches an immediate not-found; the old child paths are
	// simply dropped, so a later lookup recreates+fetches and the consumer
	// answers ENOENT — no ghost survives at the old location.
	blank := &treeNode{gen: 1, notFound: true, statAt: time.Now()}
	v.nodes[oldp] = blank
	return 0
}

// utimens acknowledges a SETATTR of times without applying it: the consumer
// owns the tree's times (served monotonically), and refusing here would break
// save tooling (touch, cp -p, editor save paths) for zero benefit. Deliberate
// semantics, not a fallback.
func (v *treeView) utimens(p string) int {
	if isAppleDouble(p) {
		return -int(syscall.ENOENT)
	}
	_, rc := v.getattr(p)
	return rc
}

// chmod acknowledges mode changes the same way: the consumer owns presentation
// (no mode crosses the bridge), and editors chmod in their save paths.
func (v *treeView) chmod(p string) int { return v.utimens(p) }

// applyEntryLocked seeds a locally created entry as fresh truth and links it
// into its parent's listing. Caller holds mu.
func (v *treeView) applyEntryLocked(p string, e content.Entry) *treeNode {
	n := v.nodeLocked(p)
	n.gen++
	v.storeStatLocked(n, p, e)
	v.addChildLocked(path.Dir(p), path.Base(p))
	return n
}

// applyRemoveLocked records a locally removed entry: a fresh not-found
// verdict, delisted from its parent. Caller holds mu.
func (v *treeView) applyRemoveLocked(p string) {
	n := v.nodeLocked(p)
	n.gen++
	n.statOK, n.listOK, n.notFound = false, false, true
	n.statAt = time.Now()
	v.removeChildLocked(path.Dir(p), path.Base(p))
}

func (v *treeView) addChildLocked(dir, name string) {
	n := v.nodeLocked(dir)
	n.gen++
	n.bumpMtimeLocked()
	if !n.listOK {
		return
	}
	for _, c := range n.children {
		if c == name {
			return
		}
	}
	n.children = append(n.children, name)
}

func (v *treeView) removeChildLocked(dir, name string) {
	n := v.nodeLocked(dir)
	n.gen++
	n.bumpMtimeLocked()
	if !n.listOK {
		return
	}
	kept := n.children[:0]
	for _, c := range n.children {
		if c != name {
			kept = append(kept, c)
		}
	}
	n.children = kept
}

// flushWithin drains every dirty token handle's commit before teardown, each
// RPC bounded; false means at least one commit did not land within grace.
func (v *treeView) flushWithin(grace time.Duration) bool {
	v.hmu.Lock()
	var dirty []uint64
	for fh, h := range v.handles {
		if h.remote != nil && h.dirty {
			dirty = append(dirty, fh)
		}
	}
	v.hmu.Unlock()

	deadline := time.Now().Add(grace)
	ok := true
	for _, fh := range dirty {
		if grace > 0 && !time.Now().Before(deadline) {
			return false
		}
		// EBADF means the handle was released mid-drain — its own release
		// already committed it; not a drain failure.
		if rc := v.flush(fh); rc != 0 && rc != -int(syscall.EBADF) {
			log.Printf("holderfs: tree drain flush %s (fh %d): errno %d", v.domain, fh, rc)
			ok = false
		}
	}
	return ok
}

// bufWriteAt writes data into buf at ofst, zero-filling any gap.
func bufWriteAt(buf []byte, ofst int64, data []byte) []byte {
	if end := ofst + int64(len(data)); end > int64(len(buf)) {
		buf = bufResize(buf, end)
	}
	copy(buf[ofst:], data)
	return buf
}

// bufResize resizes buf, zero-filling growth.
func bufResize(buf []byte, size int64) []byte {
	if size <= int64(len(buf)) {
		return buf[:size]
	}
	return append(buf, make([]byte, size-int64(len(buf)))...)
}
