package mountd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/yasyf/fusekit"
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

// openJournal loads the journal at path; a missing file is an empty journal, a
// malformed one an error.
func openJournal(path string) (*journal, error) {
	j := newJournal(path)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return j, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read journal %s: %w", path, err)
	}
	var f journalFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse journal %s: %w", path, err)
	}
	for _, m := range f.Mounts {
		j.mounts[m.Dir] = m
	}
	for _, b := range f.Bridges {
		j.bridges[b.Owner] = b
	}
	return j, nil
}

func (j *journal) putMount(spec fusekit.MountSpec) error {
	j.mu.Lock()
	j.mounts[spec.Dir] = mountEntryOf(spec)
	j.mu.Unlock()
	return j.save()
}

func (j *journal) dropMount(dir string) error {
	j.mu.Lock()
	_, ok := j.mounts[dir]
	delete(j.mounts, dir)
	j.mu.Unlock()
	if !ok {
		return nil
	}
	return j.save()
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

func (j *journal) dropBridge(owner string) error {
	j.mu.Lock()
	_, ok := j.bridges[owner]
	delete(j.bridges, owner)
	j.mu.Unlock()
	if !ok {
		return nil
	}
	return j.save()
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
// registry mutation → journal write. Bridge hooks are two-phase because no
// such claim exists: stageBridge/stageBridgeGone are memory-only and run in
// the same s.bridgeMu critical section as the s.bridges mutation they mirror
// — so journal ordering matches registry ordering and a racing add and
// reclaim can never journal a removed bridge (or drop a live one) — while
// flushJournal does the write after the lock is released. A journal write
// failure never fails the op — the mount or bridge is already live; the
// journal is recovery state — but it is loud.

// refreshJournalRow rewrites dir's journal row when ANY spec field differs
// from the journaled one: the journal is re-serve identity, and an idempotent
// mount OK must never leave a successor replaying a stale spec.
func (s *Server) refreshJournalRow(spec fusekit.MountSpec) {
	if s.journal == nil {
		return
	}
	want := mountEntryOf(spec)
	if cur, ok := s.journal.mount(spec.Dir); ok && cur.equal(want) {
		return
	}
	s.Log.Printf("journal: rewriting %s (idempotent mount with a changed spec)", spec.Dir)
	s.journalMount(spec)
}

func (s *Server) journalMount(spec fusekit.MountSpec) {
	if s.journal == nil {
		return
	}
	if err := s.journal.putMount(spec); err != nil {
		s.Log.Printf("journal: record mount %s: %v", spec.Dir, err)
	}
}

func (s *Server) journalUnmount(dir string) {
	if s.journal == nil {
		return
	}
	if err := s.journal.dropMount(dir); err != nil {
		s.Log.Printf("journal: drop mount %s: %v", dir, err)
	}
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

func (s *Server) flushJournal() {
	if s.journal == nil {
		return
	}
	if err := s.journal.save(); err != nil {
		s.Log.Printf("journal: flush: %v", err)
	}
}

// Replay seams: vars so tests shrink the retry schedule and pin the reap roots.
var (
	replayAttempts = 3
	replayBackoff  = proc.Backoff{Base: time.Second, Cap: 4 * time.Second}
	clearCarcass   = fusekit.ClearCarcass
	reapOrphans    = fusekit.ReapOrphanedServers
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

	// The REPLAY CARCASS CLEAR — the second of exactly two force-capable
	// sites (the other is the pre-mount clear). A mux tenant's kernel
	// mountpoint is its native root, so the clear runs on the deduped roots,
	// not logical tenant dirs, and each clear runs under the seized lease
	// fence of the root plus every journaled tenant. A busy lease defers the
	// root: its carcass stays, loudly, with the holder's provenance, its
	// entries stay journaled for the next generation, and its mounts are not
	// replayed under it. ClearCarcass itself forces IFF carcass proof v2
	// holds — a hanging stat defers too. The orphan reap still covers every
	// root: its kill decision is carcass-confirmed and re-confirmed at kill
	// time, never a live mount's.
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
		if err := clearCarcass(root); err != nil {
			s.Log.Printf("journal: clear carcass %s: %v", root, err)
			if errors.Is(err, fusekit.ErrCarcassUndetermined) || errors.Is(err, fusekit.ErrUnmountWedged) {
				deferred[root] = true
			}
		}
		fence.Release()
	}
	if pids := reapOrphans(roots); len(pids) > 0 {
		s.Log.Printf("journal: reaped %d orphaned go-nfsv4 server(s) from a prior generation: %v", len(pids), pids)
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
