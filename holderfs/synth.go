package holderfs

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/fusekit/content"
)

// synthFhBase is the first handle ID for synthetic read handles, above real
// kernel fds (small ints) and probe handles ([1<<61, 1<<62)).
const synthFhBase = uint64(1) << 62

func synthFh(fh uint64) bool { return fh >= synthFhBase && fh != ^uint64(0) }

// synthView serves one synthetic entry whose bytes the consumer computes over the
// bridge. Reads serve a holder-side cached snapshot refreshed OFF the fuse handler
// path, so a hung consumer is stale-but-served, never a parked fuse-t worker. A
// writable open passes through to a durable local file (writePath); its close
// schedules a background write-through. Merge/split lives consumer-side, so this
// view holds no domain knowledge.
type synthView struct {
	name      string // entry name the consumer knows (e.g. ".claude.json")
	domain    string
	client    *content.BridgeClient
	writePath string   // durable local backing for writable opens
	freshness []string // local files whose (mtime,size) gate the cached bytes
	// ino is the minted synthetic inode served for this entry, fixed for the
	// mount's life (assigned by Build from the sharedLinkInoBase pool). The NFS
	// client must never see writePath's real ino: every atomic-rename
	// write-through re-mints that fileid, and a fileid change under an open
	// file drives the invalidation churn implicated in the macOS
	// nfs_vinvalbuf2 kernel panics.
	ino uint64

	mu       sync.Mutex
	cacheSig string
	cacheBuf []byte
	cacheOK  bool
	mtimeHWM time.Time // highest mtime ever served; seeded to the incarnation floor, never regresses (servedMtime)
	ctimeHWM time.Time // highest ctime ever served; seeded to the incarnation floor, never regresses (servedCtime)
	openPins int       // open read handles; while > 0 path Getattr serves the pin
	pinSize  int64
	pinMtime time.Time
	readErr  error
	writeErr error
	dirtyFds map[uint64]struct{}

	refreshing     bool
	refreshPending bool

	wtRunning bool
	wtPending bool
	wtIdle    chan struct{}
}

func newSynthView(name, domain string, client *content.BridgeClient, writePath string, freshness []string) *synthView {
	floor := mintAttrFloor(writePath)
	return &synthView{
		name:      name,
		domain:    domain,
		client:    client,
		writePath: writePath,
		freshness: freshness,
		mtimeHWM:  floor,
		ctimeHWM:  floor,
		dirtyFds:  map[uint64]struct{}{},
	}
}

// attrFloors records, per writePath, the highest served-attr value any
// incarnation of that entry has served or been floored at in this process. It
// exists for re-attach coherence: every mux detach/re-attach builds a fresh
// view over the SAME writePath, go-nfsv4 mints the NFSv4 change attribute FROM
// THE SERVED CTIME (mtime is only its zero-ctime fallback), the macOS NFSv4
// client invalidates cached pages only when change moves (NFS_CHANGED), and
// its fileids are path-keyed — a re-attached tenant reclaims its old fileid —
// so with an equal size a repeated ctime leaves the client serving the
// PREVIOUS incarnation's pages (VM-proven: validate-mux fileid-cycle 1 served
// cycle 0's payload for 20s). The registry is in-memory and process-wide,
// which is exactly the hazard's scope: a holder restart tears down the mount,
// so a new process never faces a client cache primed by an old incarnation.
var (
	attrFloorMu sync.Mutex
	attrFloors  = map[string]time.Time{}
)

// mintAttrFloor returns the served-attr floor for a new incarnation of the
// entry backed by writePath: zero — no floor, real on-disk timestamps serve
// untouched — when no earlier incarnation in this process ever served an attr
// for it, else one nanosecond past everything earlier incarnations served, so
// the new incarnation's ctime baseline (and with it the NFSv4 change
// attribute) strictly advances even when the on-disk state is byte- and
// stamp-identical across a back-to-back detach/re-attach. Chaining on the
// recorded values rather than the wall clock keeps the guarantee under clock
// ties and backward steps, and keeps first mounts serving genuine attrs: a
// production single-tenant mount must never floor a pre-existing file's
// timestamps to mount-start time.
func mintAttrFloor(writePath string) time.Time {
	attrFloorMu.Lock()
	defer attrFloorMu.Unlock()
	prior, ok := attrFloors[writePath]
	if !ok {
		return time.Time{}
	}
	floor := prior.Add(time.Nanosecond)
	attrFloors[writePath] = floor
	return floor
}

// recordAttrServed raises writePath's registry mark to t, so the next
// incarnation's floor (mintAttrFloor) lands strictly past every attr this one
// served. Called on every servedMtime/servedCtime return — the only paths a
// synth attr reaches a client through.
func recordAttrServed(writePath string, t time.Time) {
	attrFloorMu.Lock()
	if t.After(attrFloors[writePath]) {
		attrFloors[writePath] = t
	}
	attrFloorMu.Unlock()
}

// seedFromWritePath warms a cold cache with writePath's bytes — the durable
// last-committed content — so the served size never flaps cold→warm when the
// consumer is slow or unreachable at mount time. The freshness signature is
// left stale, so the first access still schedules a bridge refresh and the
// consumer's answer wins as soon as it arrives. A missing writePath stays
// cold: the entry remains ENOENT/unlisted until the consumer supplies content.
func (v *synthView) seedFromWritePath() {
	buf, err := os.ReadFile(v.writePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("holderfs: seed %s/%s: %v", v.domain, v.name, err)
		}
		return
	}
	v.mu.Lock()
	if !v.cacheOK {
		v.cacheBuf, v.cacheOK = buf, true
	}
	v.mu.Unlock()
}

// servedMtime returns the mtime to serve for the entry: the max of writePath's
// and every freshness file's mtime, floored at the highest value ever
// returned. The floor makes the served mtime monotonic — a vanished freshness
// file must not rewind the mtime the NFS client has already seen, since a
// rewind reads as a change and re-triggers page invalidation on open files.
// The floor starts at the incarnation floor (mintAttrFloor): zero for a first
// incarnation — the real on-disk mtime serves untouched — and strictly past
// the previous incarnation's served attrs on a re-attach, so a rebuilt view
// never repeats a baseline an earlier one already served for the same on-disk
// state. Every returned value is recorded so the next incarnation's floor can
// clear it.
func (v *synthView) servedMtime() time.Time {
	var cand time.Time
	if fi, err := os.Lstat(v.writePath); err == nil {
		cand = fi.ModTime()
	}
	for _, p := range v.freshness {
		if fi, err := os.Lstat(p); err == nil && fi.ModTime().After(cand) {
			cand = fi.ModTime()
		}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if cand.After(v.mtimeHWM) {
		v.mtimeHWM = cand
	}
	recordAttrServed(v.writePath, v.mtimeHWM)
	return v.mtimeHWM
}

// servedCtime returns the ctime to serve for the entry: writePath's real ctime
// floored at the highest value ever returned, seeded to the view's incarnation
// floor (mintAttrFloor). On a first incarnation the floor is zero and the real
// ctime serves untouched; on a re-attach the floor is what advances the NFSv4
// change attribute across incarnations — go-nfsv4 derives change from the
// served ctime, and a re-attached tenant reclaims its path-keyed fileid, so a
// repeated ctime would validate the previous incarnation's cached pages. The
// high-water mark keeps the served ctime monotonic within the incarnation too:
// a real ctime landing below a value already served (a write-through committed
// after a backward wall-clock step) must not rewind the attribute —
// NFS_CHANGED compares change for inequality, so a rewind reads as a change
// and lands an invalidation on a file the client may hold open (the
// nfs_vinvalbuf2 churn the attr stabilization exists to prevent). Absent a
// real replacement of writePath the served value is inert: the mark only moves
// when the real ctime moves past it. Every returned value is recorded so the
// next incarnation's floor can clear it.
func (v *synthView) servedCtime(real time.Time) time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()
	if real.After(v.ctimeHWM) {
		v.ctimeHWM = real
	}
	recordAttrServed(v.writePath, v.ctimeHWM)
	return v.ctimeHWM
}

// pinOpen records a newly opened read handle's snapshot as the (size, mtime)
// path Getattr serves while any handle stays open, so a background refresh
// never lands an invalidation on a file the client holds open or mapped. The
// newest open always wins — its size must match what its reads return, and its
// mtime is the monotonic served mtime — so the pin only moves forward; it
// never retreats to an elder still-open handle's snapshot when a newer one
// closes.
func (v *synthView) pinOpen(size int64, mtime time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.openPins++
	v.pinSize, v.pinMtime = size, mtime
}

// unpinOpen releases one open pin; at zero the pin clears and refresh-driven
// attr changes surface on the next path Getattr.
func (v *synthView) unpinOpen() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.openPins--
}

// pinnedAttrs returns the frozen (size, mtime) while any read handle is open.
func (v *synthView) pinnedAttrs() (int64, time.Time, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.openPins == 0 {
		return 0, time.Time{}, false
	}
	return v.pinSize, v.pinMtime, true
}

// freshSig digests the freshness files' (mtime, size). An empty signature (no
// freshness paths, or none stattable) reads as always-stale, so every access
// schedules a refresh — correct, just RPC-heavier.
func (v *synthView) freshSig() string {
	sig, _ := v.freshState()
	return sig
}

// freshState returns the freshness signature plus each stattable freshness
// file's mtime, in manifest order — the per-file stamps refreshOnce's
// stability check needs to spot a signature-preserving rewrite (mtimeInWindow).
func (v *synthView) freshState() (string, []time.Time) {
	var b strings.Builder
	var mtimes []time.Time
	any := false
	for _, p := range v.freshness {
		if fi, err := os.Lstat(p); err == nil {
			any = true
			fmt.Fprintf(&b, "%d:%d;", fi.ModTime().UnixNano(), fi.Size())
			mtimes = append(mtimes, fi.ModTime())
		} else {
			b.WriteString("-;")
		}
	}
	if !any {
		return "", nil
	}
	return b.String(), mtimes
}

// currentBytes returns the cached snapshot, scheduling an off-handler refresh
// when the freshness signature has changed or the cache is cold. It NEVER blocks
// on the bridge: a stale or cold cache returns the last-good bytes (ok=false
// when the consumer has never answered).
func (v *synthView) currentBytes() ([]byte, bool) {
	sig := v.freshSig()
	v.mu.Lock()
	fresh := v.cacheOK && sig != "" && v.cacheSig == sig
	buf, ok := v.cacheBuf, v.cacheOK
	v.mu.Unlock()
	if !fresh {
		v.scheduleRefresh()
	}
	return buf, ok
}

// scheduleRefresh starts the background refresh worker, coalescing a request
// arriving mid-refresh into one more pass.
func (v *synthView) scheduleRefresh() {
	v.mu.Lock()
	if v.refreshing {
		v.refreshPending = true
		v.mu.Unlock()
		return
	}
	v.refreshing = true
	v.mu.Unlock()
	go v.refreshLoop()
}

func (v *synthView) refreshLoop() {
	for {
		v.refreshOnce()
		v.mu.Lock()
		if v.refreshPending {
			v.refreshPending = false
			v.mu.Unlock()
			continue
		}
		v.refreshing = false
		v.mu.Unlock()
		return
	}
}

// refreshRetries bounds refreshOnce's freshness-stability retries: a bridge
// read that straddled a freshness-file rewrite is re-issued at most this many
// times in one pass before the pass gives up, keeping the last-good cache.
const refreshRetries = 3

// refreshRetryDelay spaces the stability retries. An immediate re-read lands
// back inside the very writer window that tore the first one — and would triple
// the bridge read volume for nothing under sustained churn — so each retry
// waits for the writer to quiesce. The delay also moves the wall clock past the
// prior attempt's ambiguity window, so a retry's mtimeInWindow check can
// resolve (a stamp inside one attempt's [t0, t1] is strictly before the next
// attempt's t0). Only the refresh worker and Build's synchronous pre-warm ever
// sleep here — never a fuse handler.
const refreshRetryDelay = 25 * time.Millisecond

// mtimeInWindow reports whether any freshness mtime falls inside [t0, t1] —
// the pass's read window. Equal pre/post signatures cannot rule out a
// truncate-then-rewrite that completed inside the window and landed back on a
// file's prior (mtime, size): that needs the rewrite's stamp — the wall clock
// at completion — to exactly reproduce the prior mtime, which requires the
// clock to pass through that mtime inside the window (virtualized timers make
// such time.Now() ties real). A stamp before t0 is safe — any in-window write
// restamps at or past t0, moving the signature — and a stamp after t1 is safe
// the same way from the other side (an in-window write restamps at or before
// t1, below the stamp; and a future-dated freshness file must not wedge
// refresh forever). Assumes stamps come from the same wall clock at filesystem
// nanosecond granularity (APFS — the holder's backing volumes) and that the
// clock does not step backward inside the millisecond-scale window; the
// bounded retries and last-good cache confine a violation to one pass.
func mtimeInWindow(mtimes []time.Time, t0, t1 time.Time) bool {
	for _, m := range mtimes {
		if !m.Before(t0) && !m.After(t1) {
			return true
		}
	}
	return false
}

// refreshOnce fetches the synth bytes over the bridge and replaces the cache. It
// runs only on the worker goroutine or Build's pre-warm, never a fuse handler.
// The freshness signature is sampled BEFORE the read and re-checked AFTER it: a
// consumer render that straddled a freshness-file rewrite (a non-atomic writer's
// truncate window, say) can carry torn or empty bytes, and no single signature
// attributes them — installing them would serve the torn snapshot until the next
// access happens to reschedule. A moved signature — or an unmoved one whose
// stamps the window makes ambiguous (mtimeInWindow) — discards the bytes and
// re-reads after refreshRetryDelay, bounded by refreshRetries; exhaustion keeps
// the last-good cache and fails loud in the log. Either way convergence holds:
// an installed signature always brackets its bytes, and a discarded pass leaves
// cacheSig differing from the file's resting signature, so the next access
// reschedules once the writer quiesces.
func (v *synthView) refreshOnce() {
	var buf []byte
	var sig string
	var err error
	stable := false
	for i := 0; i < refreshRetries && !stable; i++ {
		if i > 0 {
			time.Sleep(refreshRetryDelay)
		}
		t0 := time.Now()
		var mtimes []time.Time
		sig, mtimes = v.freshState()
		buf, err = v.client.Read(context.Background(), v.domain, v.name)
		if err != nil {
			break
		}
		same := v.freshSig() == sig
		t1 := time.Now()
		stable = same && !mtimeInWindow(mtimes, t0, t1)
	}
	if err == nil && !stable {
		err = fmt.Errorf("freshness signature moved under %d consecutive bridge reads; snapshot discarded as torn", refreshRetries)
	}
	v.mu.Lock()
	logIt := err != nil && (v.readErr == nil || v.readErr.Error() != err.Error())
	if err != nil {
		v.readErr = err
	} else {
		v.cacheBuf, v.cacheSig, v.cacheOK, v.readErr = buf, sig, true, nil
	}
	v.mu.Unlock()
	if logIt {
		log.Printf("holderfs: read %s/%s: %v", v.domain, v.name, err)
	}
}

// markDirty records that a real writePath fd mutated the file, so its Release
// runs the write-through. A write-capable fd that never wrote stays clean.
func (v *synthView) markDirty(fh uint64) {
	v.mu.Lock()
	v.dirtyFds[fh] = struct{}{}
	v.mu.Unlock()
}

// takeDirty reports whether fh was dirty and clears the flag — kernel fds are
// reused, so the flag must not outlive the Release.
func (v *synthView) takeDirty(fh uint64) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, dirty := v.dirtyFds[fh]
	delete(v.dirtyFds, fh)
	return dirty
}

// scheduleWriteThrough requests a write-through and returns at once — the bridge RPC
// runs on the worker, never a fuse handler, so a hung consumer cannot wedge the
// mount. A commit arriving mid-cycle coalesces into one more cycle, which re-reads
// the durable file so the latest state wins.
func (v *synthView) scheduleWriteThrough() {
	v.mu.Lock()
	if v.wtRunning {
		v.wtPending = true
		v.mu.Unlock()
		return
	}
	v.wtRunning = true
	v.mu.Unlock()
	go v.writeThroughLoop()
}

func (v *synthView) writeThroughLoop() {
	for {
		err := v.writeThrough()
		v.mu.Lock()
		logIt := err != nil && (v.writeErr == nil || v.writeErr.Error() != err.Error())
		v.writeErr = err
		pending := v.wtPending
		if pending {
			v.wtPending = false
		} else {
			v.wtRunning = false
			if v.wtIdle != nil {
				close(v.wtIdle)
				v.wtIdle = nil
			}
		}
		v.mu.Unlock()
		if logIt {
			log.Printf("holderfs: write-through %s/%s: %v", v.domain, v.name, err)
		}
		if pending {
			continue
		}
		return
	}
}

// writeThrough re-reads the durable local file and ships it to the consumer, which
// persists it however the domain requires. A missing file is a no-op: the consumer
// owns the source of truth.
func (v *synthView) writeThrough() error {
	payload, err := os.ReadFile(v.writePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("write-through %s/%s: read backing: %w", v.domain, v.name, err)
	}
	return v.client.Write(context.Background(), v.domain, v.name, payload)
}

// flushWithin waits up to d for an in-flight write-through to drain, so teardown sees
// the last write reach the consumer. The bound only guards a genuinely stuck consumer
// (the file read and bounded RPC cannot hang on a wedged mirror). d <= 0 waits
// indefinitely.
func (v *synthView) flushWithin(d time.Duration) bool {
	v.mu.Lock()
	if !v.wtRunning {
		v.mu.Unlock()
		return true
	}
	if v.wtIdle == nil {
		v.wtIdle = make(chan struct{})
	}
	ch := v.wtIdle
	v.mu.Unlock()

	if d <= 0 {
		<-ch
		return true
	}
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}
