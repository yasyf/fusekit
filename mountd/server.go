package mountd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/proc"
)

// Server is the running mount holder. It owns a registry of the mounts IT
// established — the in-process host's internal registry is private to the
// host — and reports it through list with per-entry kernel liveness.
// Base is deliberately not a field: it arrives per-request, so the holder
// carries no desired state at all.
type Server struct {
	// Socket is the holder's unix socket path.
	Socket string
	// Host is the in-process fuse host that hosts the mounts. nil means
	// this binary cannot host mounts; Run fails immediately and loudly.
	Host Host
	// Probe answers OpProbe with a throwaway in-process capability mount
	// (capability + TCC grant are per-process, so it must run here). It returns
	// the probe's success and, on failure, the classified mount error (a hard
	// ErrMountFailed vs a pending ErrMountNotLive). nil reports (false, nil).
	Probe func() (bool, error)
	// Version is reported verbatim in the OpHealth reply. It is the CONSUMER's
	// version (e.g. its version.String()), never fusekit's: a daemon that
	// compares the holder's wire Version to its own would replace-loop the
	// holder forever if fusekit's module version leaked onto the wire.
	Version string
	// Log receives per-op outcomes. nil defaults to stderr.
	Log *log.Logger

	// triggerShutdown cancels Run's context, ending the holder (OpShutdown).
	// It is set in Run before the accept loop starts; the go-statement that
	// spawns each handler establishes the happens-before, so handlers read it
	// without a lock.
	triggerShutdown context.CancelFunc

	// wg tracks connection handlers; Run waits for them to drain before the
	// final unmount-all sweep.
	wg sync.WaitGroup

	mu       sync.Mutex
	registry map[string]mountRow // dir -> the mount this holder established
	inflight map[string]bool     // dir -> a mount/unmount holds the dir mid-I/O
	// epochs is the per-dir (re)mount counter behind mountRow.Epoch. It lives
	// outside the registry so it survives the deregister between a dead
	// mirror's teardown and its remount — monotonic per dir for this holder
	// process's lifetime, never reset or deleted.
	epochs map[string]uint64
}

// mountRow is one registry entry: the base a dir mirrors, which (re)mount of
// the dir this holder is on, and when the current mount was established.
type mountRow struct {
	Base      string
	Owner     string
	Epoch     uint64
	MountedAt time.Time
}

// Run binds the holder socket and serves until ctx is cancelled, the process
// is signalled (SIGTERM/SIGINT), or an OpShutdown lands. On the way out it
// stops accepting, drains in-flight handlers, then unmounts everything it
// owns — each teardown individually bounded by the provider's grace timers,
// per-dir outcomes logged.
func (s *Server) Run(ctx context.Context) error {
	if s.Host == nil {
		return errors.New("mountd: this binary cannot host fuse mounts; install the fuse build")
	}
	if s.Log == nil {
		s.Log = log.New(os.Stderr, "[mountd] ", log.LstdFlags)
	}
	s.initState()

	ln, lock, err := s.listen()
	if err != nil {
		return err
	}
	// The flock on lock is the cross-process guarantee that only this holder
	// may stale-check, remove, bind, or unlink the socket path. It must
	// outlive the listener (Close releases it), so this defer is registered
	// first and runs last.
	defer lock.Close()
	// closeListener unlinks the socket exactly once. *net.UnixListener.Close
	// unlinks the socket file and is NOT idempotent: a second Close (the late
	// deferred one, after a slow teardown) would delete a successor holder's
	// freshly-bound socket. The sync.Once pins the unlink to the first close,
	// at ctx-cancel time. No explicit os.Remove for the same reason.
	var closeOnce sync.Once
	closeListener := func() { closeOnce.Do(func() { _ = ln.Close() }) }
	defer closeListener()

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// stop cancels ctx, so it doubles as the over-the-socket shutdown trigger
	// (OpShutdown). Set before the accept loop spawns any handler.
	s.triggerShutdown = stop

	s.Log.Printf("mountd %s started; socket=%s", s.Version, s.Socket)

	// Break the accept loop on shutdown.
	go func() {
		<-ctx.Done()
		// Proof of trigger receipt before the wg.Wait drain: if a handler then
		// wedges in-flight, the unified holder log still shows the holder began
		// shutting down (the drain never reaches "mountd stopped").
		s.Log.Printf("shutdown trigger received; closing listener")
		closeListener()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			// Back off on a transient accept error (e.g. EMFILE) instead of
			// busy-spinning a core until the next shutdown.
			s.Log.Printf("accept: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.handle(conn) }()
	}

	s.wg.Wait()
	// Handlers are drained, so every claim is free and this sweep cannot
	// contend. It also catches dirs an OpShutdown sweep reported busy and any
	// mounts that landed after that sweep's snapshot.
	s.unmountAll()
	s.Log.Printf("mountd stopped")
	return nil
}

// initState resets the registry, the in-flight gate, and the epoch counters.
// Run calls it before serving; handler-level tests call it to dispatch without
// a socket.
func (s *Server) initState() {
	s.registry = map[string]mountRow{}
	s.inflight = map[string]bool{}
	s.epochs = map[string]uint64{}
}

// listen binds the unix socket with 0600 perms via proc.SingleEntrant. Unlike
// the daemon, the holder NEVER evicts a live peer — a live holder hosts mounts
// that consumer sessions run on, and replacing it would rip those mounts out from
// under them. A socket file with no live listener behind it is stale: removed
// and rebound. The flock single-entrancy (and the never-unlink-the-lock
// invariant) lives in proc.SingleEntrant; this builder supplies only the holder
// contention policy — refuse always — through Evict.
//
// Evict probes the holder's own Health: a peer that answers is refused naming
// its version; otherwise (false, nil) reports no live peer, which SingleEntrant
// binds over when the lock was free and refuses (ErrPeerStarting, re-wrapped
// here with the holder's refusal copy) when the lock is still contended.
func (s *Server) listen() (net.Listener, *os.File, error) {
	ln, lock, err := proc.SingleEntrant{
		Socket: s.Socket,
		Evict: func() (bool, error) {
			if ver, herr := NewClient(s.Socket).Health(); herr == nil {
				return false, fmt.Errorf("a mount holder (%s) already serves %s; refusing to start", ver, s.Socket)
			}
			return false, nil
		},
	}.Listen()
	if errors.Is(err, proc.ErrPeerStarting) {
		return nil, nil, fmt.Errorf("another mount holder owns %s.lock but does not answer health yet (it may still be starting); refusing to start", s.Socket)
	}
	if err != nil {
		return nil, nil, err
	}
	return ln, lock, nil
}

// opDeadline bounds one connection by its op: probe performs a real throwaway
// mount, mount waits out the provider's bounded mount-or-timeout, unmount its
// bounded graceful-then-forced teardown, and shutdown sweeps every mount.
// Each deadline is coupled to its client timeout, which sits ABOVE it (Mount
// 25s/20s, Unmount 17s/15s, Shutdown 65s/60s) so the op deadline is the
// binding bound — a blown client deadline reads ErrHolderUnavailable and
// would mask the holder's real error class.
func opDeadline(op Op) time.Duration {
	switch op {
	case OpProbe, OpMount:
		return 20 * time.Second
	case OpUnmount:
		return 15 * time.Second
	case OpShutdown:
		return 60 * time.Second
	default:
		return 10 * time.Second
	}
}

// handle serves one connection: one request, one response.
func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(opDeadline("")))
	var req Request
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		writeResp(conn, Response{OK: false, Error: "bad request: " + err.Error()})
		return
	}
	_ = conn.SetDeadline(time.Now().Add(opDeadline(req.Op)))
	writeResp(conn, s.dispatch(req))
}

func writeResp(conn net.Conn, r Response) {
	r.Proto = MountProtoVersion
	_ = json.NewEncoder(conn).Encode(r)
}

func (s *Server) dispatch(req Request) Response {
	switch req.Op {
	case OpHealth:
		return Response{OK: true, Version: s.Version}
	case OpProbe:
		return s.handleProbe()
	case OpMount:
		return s.handleMount(req)
	case OpUnmount:
		return s.handleUnmount(req)
	case OpList:
		return s.handleList(req)
	case OpReclaim:
		return s.handleReclaim(req)
	case OpShutdown:
		return s.handleShutdown()
	default:
		return Response{OK: false, Error: "unknown op: " + string(req.Op)}
	}
}

func (s *Server) handleProbe() Response {
	if s.Probe == nil {
		return Response{OK: true, FuseOK: false}
	}
	ok, err := s.Probe()
	if err != nil {
		// The RPC itself succeeded (OK: true); the throwaway probe MOUNT failed.
		// Carry its classification so the driver learns WHY — a hard
		// ErrMountFailed (fuse unavailable on this machine) vs a pending ClassTCC
		// (the grant may still land) — instead of a bare FuseOK=false.
		return Response{OK: true, FuseOK: false, ErrClass: mountErrClass(err), Error: err.Error()}
	}
	return Response{OK: true, FuseOK: ok}
}

// claim takes dir's in-flight gate: concurrent ops on the SAME dir serialize
// (the second gets a busy error) while different dirs proceed concurrently —
// the holder serves the daemon and N CLIs at once, and the provider's Setup
// has its own registry check-then-act window that two same-dir mounts would
// race. The claim — not the mutex — owns the dir across the provider I/O;
// release returns the gate.
func (s *Server) claim(dir string) (release func(), ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[dir] {
		return nil, false
	}
	s.inflight[dir] = true
	return func() {
		s.mu.Lock()
		delete(s.inflight, dir)
		s.mu.Unlock()
	}, true
}

// liveWithin reports whether dir is a live mirror of base, bounded by
// liveProbeTimeout; a probe that outlives the bound reads dead. The kernel
// truth comes from the in-process host's State pair (probeMount, host.go).
func (s *Server) liveWithin(base, dir string) bool {
	st, ok := probeMount(s.Host.State, base, dir)
	return ok && st.mounted && st.alive
}

// registered returns dir's registry row, if any.
func (s *Server) registered(dir string) (row mountRow, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok = s.registry[dir]
	return row, ok
}

// deregister drops dir's registry row.
func (s *Server) deregister(dir string) {
	s.mu.Lock()
	delete(s.registry, dir)
	s.mu.Unlock()
}

func (s *Server) handleMount(req Request) Response {
	if req.Base == "" || req.Dir == "" {
		return Response{OK: false, Error: "mount: base and dir are required"}
	}
	if req.Dir == req.Base {
		return Response{OK: false, Error: fmt.Sprintf("mount: refusing dir == base (%s)", req.Dir)}
	}
	release, ok := s.claim(req.Dir)
	if !ok {
		return Response{OK: false, ErrClass: ClassBusy, Error: "busy: another operation is in flight on " + req.Dir}
	}
	defer release()
	spec := mountSpec(req)

	if row, ok := s.registered(req.Dir); ok {
		if row.Owner != req.Owner {
			return Response{
				OK:       false,
				ErrClass: ClassForeignMount,
				Error:    fmt.Sprintf("mount: %s is owned by another consumer (%q), not %q; unmount it first", req.Dir, row.Owner, req.Owner),
			}
		}
		if row.Base != req.Base {
			return Response{
				OK:       false,
				ErrClass: ClassBaseMismatch,
				Error:    fmt.Sprintf("mount: %s already mirrors %s, not %s; unmount it first", req.Dir, row.Base, req.Base),
			}
		}
		// Bounded: a wedged mirror's stats never return, and a wedged probe
		// reads dead — routing into the forced teardown below, the designed
		// recovery — instead of hanging the handler. A shallow-live mirror is
		// idempotently OK here; detecting and healing a partial wedge
		// (shallow-alive, bulk reads hang) is the daemon's job now — it
		// deep-probes the mirror and tears a wedged one down (mountFuse) before
		// it issues this Mount, so a remount RPC only ever lands after the
		// corpse is gone and this path remounts a clean dir.
		if s.liveWithin(req.Base, req.Dir) {
			return Response{OK: true} // idempotent: this exact mount is held and live
		}
		// Mount is ensure-mounted: the registered mirror died while the holder
		// lived (external umount, fuse-t fault). The provider's Setup
		// early-returns on its own stale row, so the corpse must come down
		// before the remount.
		s.drain(req.Dir)
		err := s.Host.Teardown(req.Base, req.Dir)
		// Drop the row regardless of outcome, exactly like handleUnmount: the
		// provider dropped its handle, so the row would be a lie.
		s.deregister(req.Dir)
		if err != nil {
			class := ClassMountFailed
			if errors.Is(err, fusekit.ErrUnmountWedged) {
				class = ClassWedged
			}
			s.Log.Printf("remount %s: tear down dead mirror: %v", req.Dir, err)
			return Response{OK: false, ErrClass: class, Error: fmt.Sprintf("remount %s: tear down dead mirror: %v", req.Dir, err)}
		}
		s.Log.Printf("remounting dead mirror %s <- %s", req.Dir, req.Base)
		// The corpse is down (Teardown verifies the mountpoint is gone before
		// returning nil), so skip the foreign-mount check and remount.
		return s.setupAndRegister(spec)
	}
	// Never stack mounts: a mountpoint with no registry row belongs to
	// someone else (a dead holder's carcass, or not ours at all). Bounded, and
	// fail closed: a carcass can be a wedged mirror whose stat never returns,
	// and an unanswered probe must read as a foreign mountpoint — refusing,
	// never stacking a mount over it or hanging the handler with the dir's
	// claim held (every retry would then read busy forever).
	if st, ok := probeMount(s.Host.State, req.Base, req.Dir); !ok || st.mounted {
		return Response{
			OK:       false,
			ErrClass: ClassForeignMount,
			Error:    fmt.Sprintf("mount: %s is already a mountpoint this holder does not own; unmount it first", req.Dir),
		}
	}
	return s.setupAndRegister(spec)
}

// mountErrClass maps a provider mount error to its wire error class. Ordered:
// ErrMountTimeout (proven grant, transient) classifies before ErrMountNotLive
// (the presumed-TCC condition) — mount-timeout is the honest verdict whenever
// the proven-grant sentinel is present. Anything else — including
// fusekit.ErrMountFailed, a hard mount(2) rejection — is ClassMountFailed, so a
// hard failure never reaches the driver wearing the TCC walkthrough.
func mountErrClass(err error) string {
	switch {
	case errors.Is(err, fusekit.ErrMountTimeout):
		return ClassMountTimeout
	case errors.Is(err, fusekit.ErrMountNotLive):
		return ClassTCC
	default:
		return ClassMountFailed
	}
}

// mountSpec lifts the mount's content wiring off the request into the host spec.
func mountSpec(req Request) fusekit.MountSpec {
	return fusekit.MountSpec{
		Base:            req.Base,
		Dir:             req.Dir,
		Owner:           req.Owner,
		ContentSocket:   req.ContentSocket,
		Domain:          req.Domain,
		PrivateRoot:     req.PrivateRoot,
		ContentMode:     req.ContentMode,
		ProbePath:       req.ProbePath,
		PrivatePrefixes: req.PrivatePrefixes,
	}
}

// drainGrace bounds the pre-teardown write-through drain. It sits above the
// content bridge's full RPC ceiling (dial+op ≈ 5.5s) so a slow-but-completing
// final write-through lands before a process-exit shutdown abandons it, while
// still bounding a genuinely hung consumer (whose private file is the durable
// source of truth). It fits well under OpUnmount's 15s / OpShutdown's 60s.
const drainGrace = 6 * time.Second

// drain flushes dir's pending background write-through before teardown when the
// host supports it; a host without the capability is a no-op.
func (s *Server) drain(dir string) {
	if d, ok := s.Host.(Drainer); ok {
		d.Drain(dir, drainGrace)
	}
}

// setupAndRegister mounts spec via the provider and records the mount: a fresh
// registry row with a bumped epoch and the mount time. The caller holds dir's
// in-flight claim.
func (s *Server) setupAndRegister(spec fusekit.MountSpec) Response {
	if err := s.Host.Setup(spec); err != nil {
		s.Log.Printf("mount %s <- %s: %v", spec.Dir, spec.Base, err)
		return Response{OK: false, ErrClass: mountErrClass(err), Error: err.Error()}
	}
	s.mu.Lock()
	s.epochs[spec.Dir]++
	s.registry[spec.Dir] = mountRow{Base: spec.Base, Owner: spec.Owner, Epoch: s.epochs[spec.Dir], MountedAt: time.Now()}
	s.mu.Unlock()
	s.Log.Printf("mounted %s <- %s", spec.Dir, spec.Base)
	return Response{OK: true}
}

func (s *Server) handleUnmount(req Request) Response {
	if req.Base == "" || req.Dir == "" {
		return Response{OK: false, Error: "unmount: base and dir are required"}
	}
	if req.Dir == req.Base {
		return Response{OK: false, Error: fmt.Sprintf("unmount: refusing dir == base (%s)", req.Dir)}
	}
	release, ok := s.claim(req.Dir)
	if !ok {
		return Response{OK: false, ErrClass: ClassBusy, Error: "busy: another operation is in flight on " + req.Dir}
	}
	defer release()

	row, ok := s.registered(req.Dir)
	base := row.Base
	if !ok {
		// Bounded, and fail closed: a probe that does not answer (a wedged
		// carcass) must read still-mounted, routing into the provider's
		// bounded forced teardown below — never an OK no-op for a dir that may
		// still be a live mountpoint, and never a hung handler.
		if st, ok := probeMount(s.Host.State, req.Base, req.Dir); ok && !st.mounted {
			return Response{OK: true} // not mounted at all: no-op
		}
		// A carcass: a mountpoint with no registry row (a dead holder's
		// leftover). Teardown needs base only for its base==dir refusal, so
		// the request's Base serves.
		base = req.Base
	}
	s.drain(req.Dir)
	err := s.Host.Teardown(base, req.Dir)
	// Drop the registry row regardless of outcome: the provider already
	// dropped its handle, so a row for a dir the holder can no longer operate
	// on would be a lie. Honesty about a wedged unmount comes from the error.
	s.deregister(req.Dir)
	if err != nil {
		class := ""
		if errors.Is(err, fusekit.ErrUnmountWedged) {
			class = ClassWedged
		}
		s.Log.Printf("unmount %s: %v", req.Dir, err)
		return Response{OK: false, ErrClass: class, Error: err.Error()}
	}
	s.Log.Printf("unmounted %s", req.Dir)
	return Response{OK: true}
}

func (s *Server) handleList(req Request) Response {
	// Liveness is kernel truth, and both halves matter: mounted is the
	// device-id mountpoint check (a dead mirror exposes the underlying dir,
	// whose leftover entries can make mountAlive's visibility stat lie) and
	// mountAlive confirms base's contents show through. Both are stat-side
	// I/O the registry lock must not span (snapshotRegistry released it) and
	// either can wedge with its mirror, so the entries are probed in parallel,
	// each bounded by liveProbeTimeout: one wedged mirror reads Live=false —
	// the driver heals it through the bounded forced-teardown remount path —
	// while its healthy siblings keep reporting true within the deadline.
	snap := s.snapshotRegistry()
	dirs := make([]string, 0, len(snap))
	for dir, row := range snap {
		if req.Owner != "" && row.Owner != req.Owner {
			continue
		}
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	mounts := make([]MountInfo, len(dirs))
	var wg sync.WaitGroup
	for i, dir := range dirs {
		row := snap[dir]
		mounts[i] = MountInfo{Dir: dir, Base: row.Base, Owner: row.Owner, Epoch: row.Epoch}
		if !row.MountedAt.IsZero() {
			mounts[i].MountedAt = row.MountedAt.Unix()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Live is shallow kernel truth only — mountpoint present and base
			// visible. Detecting a partial wedge (shallow-alive, bulk reads
			// hang) is the daemon's job: it deep-probes through the same kernel
			// mount and keeps its own per-dir verdict, so the holder ships no
			// deep verdict at all.
			mounts[i].Live = s.liveWithin(row.Base, dir)
		}()
	}
	wg.Wait()
	return Response{OK: true, Mounts: mounts}
}

// handleShutdown sweeps every owned mount, replies with the dirs that failed
// to come down (empty means clean), then cancels Run's context. Cancelling
// the ctx closes the listener, never this live connection, so the reply
// (written by handle after dispatch returns) still lands.
func (s *Server) handleShutdown() Response {
	// A shared holder won't let one tenant shut down another's mounts.
	if owners := s.distinctOwners(); len(owners) > 1 {
		return Response{OK: false, Error: fmt.Sprintf("shutdown refused: holder serves %d owners %v; reclaim per-owner instead", len(owners), owners)}
	}
	// Log the true count before the sweep: Run's post-drain unmountAll runs
	// again after this and sees zero, so this is the only place the OpShutdown
	// path reports how many mounts it owned.
	s.Log.Printf("shutdown: sweeping %d owned mount(s)", len(s.snapshotRegistry()))
	failed := s.unmountAll()
	s.triggerShutdown()
	return Response{OK: true, Mounts: failed}
}

func (s *Server) distinctOwners() []string {
	seen := map[string]bool{}
	for _, row := range s.snapshotRegistry() {
		if row.Owner != "" {
			seen[row.Owner] = true
		}
	}
	owners := make([]string, 0, len(seen))
	for o := range seen {
		owners = append(owners, o)
	}
	sort.Strings(owners)
	return owners
}

func (s *Server) handleReclaim(req Request) Response {
	if req.Owner == "" {
		return Response{OK: false, Error: "reclaim: owner is required"}
	}
	return Response{OK: true, Mounts: s.unmountOwned(req.Owner)}
}

// snapshotRegistry copies the registry under the lock so callers can do I/O
// against the entries lock-free.
func (s *Server) snapshotRegistry() map[string]mountRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := make(map[string]mountRow, len(s.registry))
	for dir, row := range s.registry {
		snap[dir] = row
	}
	return snap
}

// unmountAll sweeps every mount (shutdown); unmountOwned sweeps one owner's. sweep
// claims each dir so it can't race an in-flight op (a busy dir is reported failed)
// and bounds each Teardown; it returns the dirs still mounted.
func (s *Server) unmountAll() []MountInfo { return s.sweep(func(mountRow) bool { return true }) }

func (s *Server) unmountOwned(owner string) []MountInfo {
	return s.sweep(func(r mountRow) bool { return r.Owner == owner })
}

func (s *Server) sweep(match func(mountRow) bool) []MountInfo {
	snap := s.snapshotRegistry()
	dirs := make([]string, 0, len(snap))
	for dir, row := range snap {
		if match(row) {
			dirs = append(dirs, dir)
		}
	}
	sort.Strings(dirs)

	var failed []MountInfo
	for _, dir := range dirs {
		base := snap[dir].Base
		release, ok := s.claim(dir)
		if !ok {
			s.Log.Printf("sweep: %s busy; leaving it to its in-flight op", dir)
			failed = append(failed, MountInfo{Dir: dir, Base: base, Live: true})
			continue
		}
		s.drain(dir)
		err := s.Host.Teardown(base, dir)
		s.deregister(dir)
		release()
		if err != nil {
			s.Log.Printf("sweep unmount %s: %v", dir, err)
			failed = append(failed, MountInfo{Dir: dir, Base: base, Live: true})
			continue
		}
		s.Log.Printf("sweep unmounted %s", dir)
	}
	return failed
}
