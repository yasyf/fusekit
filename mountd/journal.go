package mountd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/internal/carcass"
	"github.com/yasyf/fusekit/proc"
	"github.com/yasyf/fusekit/state"
)

// DefaultJournalPath returns the durable spec-journal path for a holder bound
// at socket: holder-specs.json beside the socket, so the cask holder's journal
// is ~/.fusekit/holder-specs.json and a private holder's lives in its own
// socket dir.
func DefaultJournalPath(socket string) string {
	return filepath.Join(filepath.Dir(socket), "holder-specs.json")
}

// mountEntry journals one active mount: the full MountSpec a successor holder
// needs to re-Setup it. Field tags mirror the proto-1 Request names.
type mountEntry struct {
	Base             string        `json:"base"`
	Dir              string        `json:"dir"`
	Owner            string        `json:"owner,omitempty"`
	MuxRoot          string        `json:"mux_root,omitempty"`
	ContentSocket    string        `json:"content_socket,omitempty"`
	Domain           string        `json:"domain,omitempty"`
	PrivateRoot      string        `json:"private_root,omitempty"`
	ContentMode      string        `json:"content_mode,omitempty"`
	ProbePath        string        `json:"probe_path,omitempty"`
	PrivatePrefixes  []string      `json:"private_prefixes,omitempty"`
	AttrCache        bool          `json:"attr_cache,omitempty"`
	AttrCacheTimeout time.Duration `json:"attr_cache_timeout,omitempty"`
}

func mountEntryOf(spec fusekit.MountSpec) mountEntry {
	return mountEntry{
		Base:             spec.Base,
		Dir:              spec.Dir,
		Owner:            spec.Owner,
		MuxRoot:          spec.MuxRoot,
		ContentSocket:    spec.ContentSocket,
		Domain:           spec.Domain,
		PrivateRoot:      spec.PrivateRoot,
		ContentMode:      spec.ContentMode,
		ProbePath:        spec.ProbePath,
		PrivatePrefixes:  spec.PrivatePrefixes,
		AttrCache:        spec.AttrCache,
		AttrCacheTimeout: spec.AttrCacheTimeout,
	}
}

// equal reports field-for-field spec identity (PrivatePrefixes by value).
func (e mountEntry) equal(o mountEntry) bool {
	return e.Base == o.Base && e.Dir == o.Dir && e.Owner == o.Owner &&
		e.MuxRoot == o.MuxRoot && e.ContentSocket == o.ContentSocket &&
		e.Domain == o.Domain && e.PrivateRoot == o.PrivateRoot &&
		e.ContentMode == o.ContentMode && e.ProbePath == o.ProbePath &&
		e.AttrCache == o.AttrCache && e.AttrCacheTimeout == o.AttrCacheTimeout &&
		slices.Equal(e.PrivatePrefixes, o.PrivatePrefixes)
}

func (e mountEntry) mountRequest() Request {
	return Request{
		Op:               OpMount,
		Base:             e.Base,
		Dir:              e.Dir,
		Owner:            e.Owner,
		MuxRoot:          e.MuxRoot,
		ContentSocket:    e.ContentSocket,
		Domain:           e.Domain,
		PrivateRoot:      e.PrivateRoot,
		ContentMode:      e.ContentMode,
		ProbePath:        e.ProbePath,
		PrivatePrefixes:  e.PrivatePrefixes,
		AttrCache:        e.AttrCache,
		AttrCacheTimeout: e.AttrCacheTimeout,
	}
}

// bridgeEntry journals one hosted content bridge.
type bridgeEntry struct {
	Owner           string   `json:"owner"`
	BridgeSocket    string   `json:"bridge_socket"`
	ContentSocket   string   `json:"content_socket"`
	PrivatePrefixes []string `json:"private_prefixes,omitempty"`
}

func (e bridgeEntry) addRequest() Request {
	return Request{
		Op:              OpAddBridge,
		Owner:           e.Owner,
		BridgeSocket:    e.BridgeSocket,
		ContentSocket:   e.ContentSocket,
		PrivatePrefixes: e.PrivatePrefixes,
	}
}

// journalFile is the on-disk journal shape (journal v2: re-serve identity
// ONLY — no policy fields). Durable state a SUCCESSOR holder generation
// parses; a legacy journal's idle_policy/carcass_policy fields decode away
// via Go's default unknown-field ignoring.
type journalFile struct {
	Mounts  []mountEntry  `json:"mounts,omitempty"`
	Bridges []bridgeEntry `json:"bridges,omitempty"`
}

// journal mirrors the server's active mounts and bridges to disk so a
// successor holder can replay them. mu guards the in-memory maps and is
// memory-only — never held across I/O — so bridge mutations can stage under
// Server.bridgeMu. writeMu serializes save from snapshot through write, so
// saves land in snapshot order and an older snapshot can never overwrite a
// newer file.
type journal struct {
	path string

	mu      sync.Mutex
	writeMu sync.Mutex
	mounts  map[string]mountEntry
	bridges map[string]bridgeEntry
}

func newJournal(path string) *journal {
	return &journal{path: path, mounts: map[string]mountEntry{}, bridges: map[string]bridgeEntry{}}
}

// openJournal loads the journal at path; a missing file is an empty journal,
// a malformed one an error. Row paths are canonicalized exactly like the wire
// ingress (canonReq: Clean, absolute-only) so a pre-canonical row can never
// replay under an alias of a canonical key; a non-absolute row is dropped and
// reported in dropped for the caller to surface loudly.
func openJournal(path string) (j *journal, dropped []string, err error) {
	j = newJournal(path)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return j, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read journal %s: %w", path, err)
	}
	var f journalFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, nil, fmt.Errorf("parse journal %s: %w", path, err)
	}
	for _, m := range f.Mounts {
		if !canonEntry(&m) {
			dropped = append(dropped, fmt.Sprintf("mount %q (non-absolute path)", m.Dir))
			continue
		}
		j.mounts[m.Dir] = m
	}
	for _, b := range f.Bridges {
		j.bridges[b.Owner] = b
	}
	return j, dropped, nil
}

// canonEntry canonicalizes a loaded row's path fields in place, mirroring
// canonReq; false means a non-absolute path (the row must be dropped).
func canonEntry(m *mountEntry) bool {
	for _, f := range []*string{&m.Base, &m.Dir, &m.MuxRoot} {
		if *f == "" {
			continue
		}
		if !filepath.IsAbs(*f) {
			return false
		}
		*f = filepath.Clean(*f)
	}
	return true
}

// putMount stages then persists dir's row. A failed save ROLLS the in-memory
// row back: memory must never advance past disk, or an identical retry would
// compare equal and no-op while the file stays stale (T-6).
func (j *journal) putMount(spec fusekit.MountSpec) error {
	j.mu.Lock()
	prev, had := j.mounts[spec.Dir]
	j.mounts[spec.Dir] = mountEntryOf(spec)
	j.mu.Unlock()
	if err := j.save(); err != nil {
		j.mu.Lock()
		if had {
			j.mounts[spec.Dir] = prev
		} else {
			delete(j.mounts, spec.Dir)
		}
		j.mu.Unlock()
		return err
	}
	return nil
}

// dropMount removes dir's row. NO rollback, deliberately asymmetric with
// putMount: a drop mirrors kernel state that ALREADY changed (the mount is
// gone), so memory keeps the drop; a failed save gets one immediate retry
// here and heals on any later save (full snapshot). The error is the
// caller's persist-warning, never an op failure.
func (j *journal) dropMount(dir string) error {
	j.mu.Lock()
	_, ok := j.mounts[dir]
	delete(j.mounts, dir)
	j.mu.Unlock()
	if !ok {
		return nil
	}
	if err := j.save(); err != nil {
		return j.save()
	}
	return nil
}

// stageBridge and stageBridgeGone mutate only the in-memory mirror — no I/O —
// so the Server calls them under bridgeMu; the caller persists with save after
// releasing its lock.
func (j *journal) stageBridge(e bridgeEntry) {
	j.mu.Lock()
	j.bridges[e.Owner] = e
	j.mu.Unlock()
}

func (j *journal) stageBridgeGone(owner string) {
	j.mu.Lock()
	delete(j.bridges, owner)
	j.mu.Unlock()
}

func (j *journal) putBridge(e bridgeEntry) error {
	j.stageBridge(e)
	return j.save()
}

// dropBridge removes owner's bridge row, dropMount's no-rollback twin.
func (j *journal) dropBridge(owner string) error {
	j.mu.Lock()
	_, ok := j.bridges[owner]
	delete(j.bridges, owner)
	j.mu.Unlock()
	if !ok {
		return nil
	}
	if err := j.save(); err != nil {
		return j.save()
	}
	return nil
}

// drainClean is Run's clean-shutdown drain: bridges drop — after a clean stop
// consumers re-establish them — while mount entries are kept. Post-sweep the
// journal holds exactly the mounts whose teardown failed; each outlives this
// process as a carcass the successor's replay clears (carcass proof v2) or
// surfaces. Reports how many mount entries were kept.
func (j *journal) drainClean() (kept int, err error) {
	j.mu.Lock()
	j.bridges = map[string]bridgeEntry{}
	kept = len(j.mounts)
	j.mu.Unlock()
	return kept, j.save()
}

func (j *journal) snapshot() (mounts []mountEntry, bridges []bridgeEntry) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.sortedMounts(), j.sortedBridges()
}

// counts reports the journaled mount and bridge entry counts (OpHealth).
func (j *journal) counts() (mounts, bridges int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.mounts), len(j.bridges)
}

// mount returns dir's journaled entry, when one exists.
func (j *journal) mount(dir string) (mountEntry, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	e, ok := j.mounts[dir]
	return e, ok
}

// sortedMounts and sortedBridges are called under j.mu.
func (j *journal) sortedMounts() []mountEntry {
	mounts := make([]mountEntry, 0, len(j.mounts))
	for _, m := range j.mounts {
		mounts = append(mounts, m)
	}
	sort.Slice(mounts, func(i, k int) bool { return mounts[i].Dir < mounts[k].Dir })
	return mounts
}

func (j *journal) sortedBridges() []bridgeEntry {
	bridges := make([]bridgeEntry, 0, len(j.bridges))
	for _, b := range j.bridges {
		bridges = append(bridges, b)
	}
	sort.Slice(bridges, func(i, k int) bool { return bridges[i].Owner < bridges[k].Owner })
	return bridges
}

// save persists a fresh snapshot, holding writeMu from snapshot through write
// so saves land in snapshot order.
func (j *journal) save() error {
	j.writeMu.Lock()
	defer j.writeMu.Unlock()
	j.mu.Lock()
	f := journalFile{Mounts: j.sortedMounts(), Bridges: j.sortedBridges()}
	j.mu.Unlock()
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal journal: %w", err)
	}
	if err := state.AtomicWrite(j.path, data, 0o600); err != nil {
		return fmt.Errorf("write journal: %w", err)
	}
	return nil
}

// The journal hooks below co-update the disk mirror with the in-memory
// registries. Mount hooks run under the per-dir claim, which already spans
// registry mutation → journal write; a MOUNT-side write failure FAILS THE OP
// to the client (the mount stays up and putMount rolled its row back, so a
// retry re-attempts the write — never an OK over a stale file). Bridge hooks
// are two-phase because no claim exists: stageBridge/stageBridgeGone are
// memory-only and run in the same s.bridgeMu critical section as the
// s.bridges mutation they mirror — so journal ordering matches registry
// ordering and a racing add and reclaim can never journal a removed bridge
// (or drop a live one) — while flushJournal does the write after the lock is
// released, loud but non-failing.

// refreshJournalRow rewrites dir's journal row when ANY spec field differs
// from the journaled one: the journal is re-serve identity, and an idempotent
// mount OK must never leave a successor replaying a stale spec. A write
// failure is the caller's to surface (T-6).
func (s *Server) refreshJournalRow(spec fusekit.MountSpec) error {
	if s.journal == nil {
		return nil
	}
	want := mountEntryOf(spec)
	if cur, ok := s.journal.mount(spec.Dir); ok && cur.equal(want) {
		return nil
	}
	s.Log.Printf("journal: rewriting %s (idempotent mount with a changed spec)", spec.Dir)
	return s.journalMount(spec)
}

func (s *Server) journalMount(spec fusekit.MountSpec) error {
	if s.journal == nil {
		return nil
	}
	if err := s.journal.putMount(spec); err != nil {
		s.Log.Printf("journal: record mount %s: %v", spec.Dir, err)
		return err
	}
	return nil
}

// journalUnmount drops dir's journal row; the error is a persist-warning
// (logged here, surfaced by okWithWarning) — never an op failure, because
// the kernel unmount it mirrors already happened.
func (s *Server) journalUnmount(dir string) error {
	if s.journal == nil {
		return nil
	}
	if err := s.journal.dropMount(dir); err != nil {
		s.Log.Printf("journal: drop mount %s: %v", dir, err)
		return err
	}
	return nil
}

func (s *Server) stageBridge(req Request) {
	if s.journal == nil {
		return
	}
	s.journal.stageBridge(bridgeEntry{Owner: req.Owner, BridgeSocket: req.BridgeSocket, ContentSocket: req.ContentSocket, PrivatePrefixes: req.PrivatePrefixes})
}

func (s *Server) stageBridgeGone(owner string) {
	if s.journal == nil {
		return
	}
	s.journal.stageBridgeGone(owner)
}

// flushJournal persists the staged bridge state; a failed save gets one
// immediate retry and the error back as the caller's persist-warning (memory
// keeps kernel truth; the next save heals the file).
func (s *Server) flushJournal() error {
	if s.journal == nil {
		return nil
	}
	err := s.journal.save()
	if err != nil {
		err = s.journal.save()
	}
	if err != nil {
		s.Log.Printf("journal: flush: %v", err)
	}
	return err
}

// Replay seams: vars so tests shrink the retry schedule and pin the reap roots.
var (
	replayAttempts = 3
	replayBackoff  = proc.Backoff{Base: time.Second, Cap: 4 * time.Second}
	clearCarcass   = carcass.Clear
	reapOrphans    = carcass.ReapOrphaned
)

// replayJournal restores the journaled mounts and bridges on a fresh start,
// between the socket bind and the accept loop: it clears prior-generation
// carcasses, reaps orphaned go-nfsv4 servers under the journaled roots
// (superseding the old one-shot `--reap-root` holder flag), then re-establishes
// each bridge and re-Setups each mount. It never fails startup: a
// persistently failing entry is dropped from the journal loudly and the holder
// serves whatever succeeded.
func (s *Server) replayJournal(ctx context.Context) {
	mounts, bridges := s.journal.snapshot()
	if len(mounts) == 0 && len(bridges) == 0 {
		return
	}
	s.Log.Printf("journal: replaying %d mount(s), %d bridge(s) from %s", len(mounts), len(bridges), s.journal.path)

	// The REPLAY CARCASS CLEAR. THE INVARIANT: force-unmount exists at
	// EXACTLY two holder-internal sites — the pre-mount carcass clear
	// (clearCarcassAndMount) and this replay carcass clear — both executed
	// under a seized lease EX fence with carcass proof v2 = (stat answers
	// IMMEDIATELY with ENOTCONN/EIO/EPERM/EACCES) ∧ (mount identity pinned) ∧
	// (the mount's go-nfsv4 server proven dead BEFORE forcing,
	// pid-reuse-proof). A hanging stat is NEVER proof, anywhere. No public
	// fusekit API offers force (internal/carcass).
	//
	// A mux tenant's kernel mountpoint is its native root, so the clear runs
	// on the deduped roots, not logical tenant dirs, and each clear runs
	// under the seized lease fence of the root plus every journaled tenant,
	// plus the lease-dir subtree scan (an unjournaled tenant's live lease
	// defers the root too). A busy lease, a hanging stat, or an undetermined
	// verdict defers the root: its carcass stays, loudly, with the holder's
	// provenance, its entries stay journaled for the next generation, and its
	// mounts are not replayed under it. The orphan reap runs per root, under
	// the held fence, only when the root was NOT deferred; its kill decision
	// is proven-dead-gated and re-confirmed at kill time, never a live
	// mount's and never a hanging one's.
	//
	// Legacy journals carried carcass_policy fields ("defer" rows included);
	// they decode away without a shim, deliberately: no journal has EVER
	// existed in deployment (the running fleet holder predates the journal
	// feature — spike-proven), and carcass proof v2 only forces mounts whose
	// server is proven dead, which preserves defer's panic-safety intent.
	roots := mountRoots(mounts)
	deferred := map[string]bool{}
	for _, root := range roots {
		seize := []string{root}
		for _, m := range mounts {
			if m.MuxRoot == root {
				seize = append(seize, m.Dir)
			}
		}
		fence, err := s.seizeLeases(seize...)
		if err != nil {
			deferred[root] = true
			s.Log.Printf("journal: deferring carcass clear and replay of %s: %v", root, err)
			continue
		}
		// The subtree scan re-runs as clearCarcass's preForce hook,
		// immediately before the force syscall (see clearCarcassAndMount's
		// residual-race note).
		scan := func() error {
			if dir, busy := s.subtreeLeaseHeld(root, fence); busy {
				return fmt.Errorf("%w: session lease held on %s", errSubtreeLeaseHeld, dir)
			}
			return nil
		}
		if err := scan(); err != nil {
			deferred[root] = true
			s.Log.Printf("journal: deferring carcass clear and replay of %s: %v", root, err)
			fence.Release()
			continue
		}
		if err := clearCarcass(root, scan); err != nil {
			s.Log.Printf("journal: clear carcass %s: %v", root, err)
			deferred[root] = true
			if s.parkPendingForce("replay", root, err, fence) {
				continue // the fence stays parked until the force resolves
			}
		} else if pids := reapOrphans([]string{root}); len(pids) > 0 {
			s.Log.Printf("journal: reaped %d orphaned go-nfsv4 server(s) under %s: %v", len(pids), root, pids)
		}
		fence.Release()
	}

	// A false replayOp with ctx still live means the entry is gone for good, so
	// it leaves the journal; on cancellation mid-replay the entry survives for
	// the next generation.
	//
	// Bridges re-establish BEFORE mounts: a tree-mode mount serves through its
	// owner's bridge, and mount-first would burn that mount's retries against
	// a not-yet-up bridge (ClassContentUnavailable) and drop its entry from
	// the journal for good.
	for _, b := range bridges {
		if ok := s.replayOp(ctx, "bridge "+b.Owner, func() Response { return s.handleAddBridge(b.addRequest()) }); !ok && ctx.Err() == nil {
			// Replay runs single-threaded before the accept loop, so bare
			// staging cannot race a registry mutation.
			s.stageBridgeGone(b.Owner)
			s.flushJournal()
		}
	}
	for _, m := range mounts {
		if deferred[rootOf(m)] {
			s.Log.Printf("journal: %s kept for the next generation (its root's carcass clear was deferred)", m.Dir)
			continue
		}
		if ok := s.replayOp(ctx, "mount "+m.Dir, func() Response { return s.handleMount(m.mountRequest()) }); !ok && ctx.Err() == nil {
			s.journalUnmount(m.Dir)
		}
	}
}

// rootOf is a journaled mount's kernel mountpoint: Dir for a plain mount, the
// shared MuxRoot for a mux tenant.
func rootOf(m mountEntry) string {
	if m.MuxRoot != "" {
		return m.MuxRoot
	}
	return m.Dir
}

// mountRoots returns the deduped, sorted kernel mountpoints of the journaled
// mounts: Dir for a plain mount, the shared MuxRoot for a mux tenant.
func mountRoots(mounts []mountEntry) []string {
	seen := map[string]bool{}
	var roots []string
	for _, m := range mounts {
		root := rootOf(m)
		if seen[root] {
			continue
		}
		seen[root] = true
		roots = append(roots, root)
	}
	sort.Strings(roots)
	return roots
}

// replayOp runs one replay op with per-entry backoff. false means the entry
// did not come back — or ctx ended mid-retry, which the caller distinguishes.
func (s *Server) replayOp(ctx context.Context, what string, op func() Response) bool {
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return false
		}
		resp := op()
		if resp.OK {
			s.Log.Printf("journal: replayed %s", what)
			return true
		}
		if attempt >= replayAttempts {
			s.Log.Printf("journal: replay %s failed after %d attempt(s); dropping it: %s", what, attempt, resp.Error)
			return false
		}
		s.Log.Printf("journal: replay %s attempt %d/%d failed: %s", what, attempt, replayAttempts, resp.Error)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(replayBackoff.After(attempt)):
		}
	}
}
