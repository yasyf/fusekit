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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/internal/carcass"
	"github.com/yasyf/fusekit/lease"
	"github.com/yasyf/fusekit/proc"
)

// Server is the running mount holder. Its registry holds only the mounts it
// established; the in-process host's internal registry is private to the host.
type Server struct {
	// Socket is the holder's unix socket path.
	Socket string
	// Host is the in-process fuse host. nil means this binary cannot host
	// mounts; Run fails immediately and loudly.
	Host Host
	// Probe answers OpProbe with a throwaway in-process capability mount
	// (capability + TCC grant are per-process, so it must run here); on
	// failure it returns the classified mount error. nil reports (false, nil).
	Probe func() (bool, error)
	// Version is reported verbatim in the OpHealth reply. It is the CONSUMER's
	// version, never fusekit's: a daemon comparing the wire Version to its own
	// would replace-loop the holder if fusekit's module version leaked.
	Version string
	// Log receives per-op outcomes. nil defaults to stderr.
	Log *log.Logger
	// JournalPath, when set, is the durable spec journal (see
	// DefaultJournalPath): every active mount and bridge is mirrored there,
	// and a fresh Run replays it before accepting connections. Empty disables
	// journaling (embedded and handler-level test servers).
	JournalPath string
	// RetireSkew, when set on a journaling holder, reports whether this
	// process's build is version-skewed against the installed one (see
	// SkewCheck). Run polls it and self-retires on skew: drain gated on each
	// journaled mount's lease being free, then exit so the LaunchAgent
	// relaunches the installed build, which replays the journal. nil disables
	// self-retire.
	RetireSkew func() (skewed bool, reason string, err error)
	// LeaseDir is the session-lease directory (lease.DefaultRoot for the cask
	// holder). Required: every teardown action seizes the dir's lease here,
	// and the retire gate probes it.
	LeaseDir string

	// triggerShutdown cancels Run's context (the self-retire exit). Set before
	// the accept loop; the handler go-statement's happens-before lets the
	// retire loop read it without a lock.
	triggerShutdown context.CancelFunc

	// reArmSignals re-registers the server's SIGINT/SIGTERM subscription
	// (idempotent signal.Notify on the one channel). It rides every MountSpec
	// (ReArmSignals) so fusekit.Mount's post-ready signal.Reset — which
	// defuses cgofuse's MNT_FORCE-on-SIGTERM handler process-wide — is
	// immediately followed by the holder re-arming its graceful shutdown. Set
	// by Run before any mount (replay included); nil for handler-level tests.
	reArmSignals func()

	// runCtx is Run's lifetime; wedge-clear watchers exit on it. Background
	// for handler-level test servers. wedgeEvery snapshots wedgeRecheck at
	// initState so a detached watcher never reads the swappable package var.
	runCtx     context.Context
	wedgeEvery time.Duration

	wg sync.WaitGroup
	// parkWG tracks every park watcher (pending teardown or force): Run's
	// shutdown waits on it, unbounded, so process exit never drops an EX-fence
	// fd while an unmount or umount -f is still in flight.
	parkWG sync.WaitGroup

	// replayDone flips once Run's journal replay finished (trivially, when
	// journaling is off) — reported in health so tests and doctors can wait on
	// replay instead of racing the earlier socket bind.
	replayDone atomic.Bool

	mu       sync.Mutex
	registry map[string]mountRow // dir -> the mount this holder established
	inflight map[string]bool     // dir -> a mount/unmount holds the dir mid-I/O
	// parkedDirs maps a dir whose fence and claims are parked on an in-flight
	// teardown or force to a channel closed AFTER the watcher released them —
	// the retire abort's park-aware wait point.
	parkedDirs map[string]chan struct{}
	// wedged maps a dir whose park resolved to a FINAL WEDGE (or whose
	// pending teardown broke the host contract) to its still-held lease fence
	// — a STRONG reference for the process lifetime, so the fence's fd can
	// never be GC-finalized (dropping LOCK_EX) while the in-memory claim
	// remains. Surfaced in health as WedgedDirs; released only by
	// watchWedgeClear observing the mount gone, or by process exit.
	wedged map[string]*seizedFence
	// epochs backs mountRow.Epoch. It lives outside the registry so it
	// survives the deregister between a dead mirror's teardown and its
	// remount — monotonic per dir for this process's lifetime, never reset.
	epochs map[string]uint64

	// bridges is the hosted content-bridge registry (Track C), keyed by owner and
	// kept SEPARATE from the mount registry so no mount sweep/converge/carcass
	// path ever iterates it. bridgeCtx is every bridge runner's parent context,
	// cancelled when Run's context is (process shutdown).
	bridgeMu  sync.Mutex
	bridges   map[string]*bridgeRow
	bridgeCtx context.Context

	// persistWarns records journal persist-failures whose op already acked OK
	// and returned (a parked bridge removal's late flush), keyed by surface;
	// health joins them into Response.Warning so they stay observable. The
	// file itself heals on the next full-snapshot save.
	warnMu       sync.Mutex
	persistWarns map[string]string

	// journal is the disk mirror behind JournalPath; nil when journaling is
	// off. Set by Run before any handler goroutine starts (or directly by
	// tests), so handlers read it without a lock.
	journal *journal

	// retiring, once set by the self-retire loop, bounces NEW mount and bridge
	// requests at dispatch with retryable ClassBusy so the drain converges;
	// every other op (unmount, reclaim, attest, health) still serves, and the
	// retire sweep's own internal remounts bypass the gate.
	retiring atomic.Bool

	// retired marks a shutdown the self-retire path triggered: Run PRESERVES
	// the journal so the relaunched successor replays it. Any other exit — an
	// external signal (bootout/logout/reboot SIGTERM) or ctx cancel —
	// drains the journal: it is crash+retire recovery only, never
	// survives-a-clean-stop state, and a successor on next login must not
	// race consumers re-establishing.
	retired atomic.Bool

	// retireMu guards the retire status OpHealth reads while the retire tick
	// writes: the strike history (proc.Strikes is not concurrency-safe), the
	// storm-breaker park deadline, and the idle-gate deferral.
	retireMu             sync.Mutex
	strikes              *proc.Strikes
	parkedUntil          time.Time
	retireDeferredDir    string
	retireDeferredReason string
}

type mountRow struct {
	Base      string
	Owner     string
	Epoch     uint64
	MountedAt time.Time
	// MuxRoot is the row's native mount root when Dir is a logical subtree of a
	// shared mux mount; empty for a plain one-mount-per-dir row. It drives the
	// per-MuxRoot serialization claim and the plain/mux collision checks.
	MuxRoot string
}

// Run binds the holder socket and serves until ctx is cancelled or the
// process is signalled (SIGTERM/SIGINT); it then drains
// in-flight handlers and unmounts everything it owns, each teardown bounded
// by the provider's grace timers.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if s.Log == nil {
		s.Log = log.New(os.Stderr, "[mountd] ", log.LstdFlags)
	}
	s.initState()

	ln, lock, err := s.listen()
	if err != nil {
		return err
	}
	// The flock is the cross-process guarantee that only this holder may
	// stale-check, remove, bind, or unlink the socket path. It must outlive
	// the listener (Close releases it), so this defer runs last.
	defer lock.Close()
	// *net.UnixListener.Close unlinks the socket file and is NOT idempotent:
	// a late second Close would delete a successor holder's freshly-bound
	// socket, so the Once pins the unlink to the first close. No explicit
	// os.Remove for the same reason.
	var closeOnce sync.Once
	closeListener := func() { closeOnce.Do(func() { _ = ln.Close() }) }
	defer closeListener()

	// Re-armable subscription, NOT signal.NotifyContext: every fusekit.Mount
	// defuses cgofuse's signal handler with a process-wide signal.Reset once
	// the mount is live — which also unsubscribes this channel — and the
	// ReArmSignals hook riding every spec re-Notifies it (idempotent), so
	// holder shutdown stays graceful while cgofuse can never MNT_FORCE on
	// SIGTERM.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sigc := make(chan os.Signal, 1)
	s.reArmSignals = func() { signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM) }
	s.reArmSignals()
	defer signal.Stop(sigc)
	go func() {
		select {
		case sig := <-sigc:
			s.Log.Printf("%v received; shutting down", sig)
			cancel()
		case <-ctx.Done():
		}
	}()
	s.triggerShutdown = cancel
	s.runCtx = ctx
	// Every bridge runner derives from this context, so a shutdown/signal cancels
	// them and Run's wg.Wait drains their serve + replay loops.
	s.bridgeCtx = ctx

	s.Log.Printf("mountd %s started; socket=%s", s.Version, s.Socket)

	go func() {
		<-ctx.Done()
		s.Log.Printf("shutdown trigger received; closing listener")
		closeListener()
	}()
	// The journal replays between the bind and the accept loop: the socket is
	// claimed (no second holder can race the reap/remounts) and no wire op can
	// interleave with the replay's registry writes. A corrupt journal never
	// blocks serving — consumers re-mount and the journal rebuilds.
	if s.JournalPath != "" {
		j, dropped, err := openJournal(s.JournalPath)
		if err != nil {
			s.Log.Printf("journal: %v; starting with an empty journal", err)
			j = newJournal(s.JournalPath)
		}
		for _, d := range dropped {
			s.Log.Printf("journal: DROPPING pre-canonical row %s; it will not replay", d)
		}
		s.journal = j
		s.replayJournal(ctx)
	}
	s.replayDone.Store(true)
	if s.journal != nil && s.RetireSkew != nil {
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.retireLoop(ctx) }()
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			// Back off on a transient accept error (e.g. EMFILE) instead of busy-spinning.
			s.Log.Printf("accept: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.handle(conn) }()
	}

	s.wg.Wait()
	// Every claim is free post-drain; this sweep takes down everything the
	// holder still owns, each teardown lease-gated and graceful.
	s.unmountAll()
	// Park watchers hold EX fences whose fds die with this process: an
	// unresolved unmount or umount -f in flight must keep the holder alive —
	// unbounded and loud; the cask relauncher supervises. SIGKILL residual:
	// an operator kill drops the fence fds anyway — accepted; the journaled
	// rows and the successor's carcass proof cover it.
	s.mu.Lock()
	parked := len(s.parkedDirs)
	s.mu.Unlock()
	if parked > 0 {
		s.Log.Printf("shutdown: WAITING on %d unresolved parked teardown/force watcher(s) before releasing the process (fence fds must not drop mid-flight)", parked)
	}
	s.parkWG.Wait()
	if s.journal != nil && !s.retired.Load() {
		kept, err := s.journal.drainClean()
		switch {
		case err != nil:
			s.Log.Printf("journal: drain on clean shutdown: %v", err)
		case kept > 0:
			s.Log.Printf("journal: %d mount(s) survived the final sweep; keeping their entries for the successor to clear or surface", kept)
		default:
			s.Log.Printf("journal: drained on clean shutdown; consumers re-establish")
		}
	}
	s.Log.Printf("mountd stopped")
	return nil
}

// Validate reports the configuration errors Run would refuse with, without
// binding the socket — the construction-time check for holder mains.
func (s *Server) Validate() error {
	if s.Host == nil {
		return errors.New("mountd: this binary cannot host fuse mounts; install the fuse build")
	}
	// Structural, not error-time: a Host that surfaced ErrTeardownPending
	// without the resolution channel would strand parked fences.
	if _, ok := s.Host.(TeardownPender); !ok {
		return fmt.Errorf("mountd: Host %T must implement TeardownPender (return nil from TeardownDone if it never pends)", s.Host)
	}
	if s.LeaseDir == "" {
		return errors.New("mountd: LeaseDir is required (lease.DefaultRoot for the cask holder)")
	}
	return nil
}

// initState resets per-run state; handler-level tests call it to dispatch
// without a socket.
func (s *Server) initState() {
	s.registry = map[string]mountRow{}
	s.inflight = map[string]bool{}
	s.parkedDirs = map[string]chan struct{}{}
	s.wedged = map[string]*seizedFence{}
	s.epochs = map[string]uint64{}
	s.bridges = map[string]*bridgeRow{}
	s.persistWarns = map[string]string{}
	// Defaults for handler-level tests that dispatch without Run; Run overrides
	// them with the signalled context so shutdown cancels every bridge runner
	// and wedge-clear watcher.
	if s.bridgeCtx == nil {
		s.bridgeCtx = context.Background()
	}
	if s.runCtx == nil {
		s.runCtx = context.Background()
	}
	s.wedgeEvery = wedgeRecheck
}

// listen binds the unix socket via proc.SingleEntrant with a refuse-always
// Evict: unlike the daemon, the holder NEVER evicts a live peer — a live
// holder hosts mounts consumer sessions run on, and replacing it would rip
// them out from under those sessions.
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
		return nil, nil, fmt.Errorf("another mount holder owns %s.lock but does not answer health yet (it may still be starting); refusing to start: %w", s.Socket, err)
	}
	if err != nil {
		return nil, nil, err
	}
	return ln, lock, nil
}

// opDeadline bounds one connection by its op. Each deadline sits BELOW its
// client timeout (Mount 25s/20s, Unmount 17s/15s, Shutdown 65s/60s, Health
// 2s/1s) so the op deadline is the binding bound — a blown client deadline
// reads ErrHolderUnavailable and would mask the holder's real error class.
func opDeadline(op Op) time.Duration {
	switch op {
	case OpHello, OpHealth:
		return time.Second
	case OpProbe, OpMount:
		return 20 * time.Second
	case OpUnmount:
		return 15 * time.Second
	case OpReclaim:
		return 60 * time.Second
	default:
		return 10 * time.Second
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(opDeadline("")))
	var req Request
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		writeResp(conn, Response{OK: false, Error: "bad request: " + err.Error()})
		return
	}
	if req.Proto != MountProtoVersion {
		writeResp(conn, Response{
			OK:       false,
			ErrClass: ClassProtoMismatch,
			Error: fmt.Sprintf("holder speaks proto %d, request is proto %d: upgrade the consumer to a holder-v2 fusekit, or `brew upgrade --cask fusekit-holder` if the consumer is the newer side",
				MountProtoVersion, req.Proto),
		})
		return
	}
	_ = conn.SetDeadline(time.Now().Add(opDeadline(req.Op)))
	writeResp(conn, s.dispatch(req))
}

func writeResp(conn net.Conn, r Response) {
	r.Proto = MountProtoVersion
	_ = json.NewEncoder(conn).Encode(r)
}

// unknownOpPrefix is the frozen wire text for an op this holder does not
// recognize (an op minted after it shipped); consumers gate capabilities on
// OpHello features, so this is only ever a bug surface.
const unknownOpPrefix = "unknown op:"

func (s *Server) dispatch(req Request) Response {
	switch req.Op {
	case OpHello:
		return s.handleHello()
	case OpHealth:
		return s.handleHealth()
	case OpProbe:
		return s.handleProbe()
	}
	// Owner is required and validOwner-checked on every op past this point.
	if !validOwner(req.Owner) {
		return Response{OK: false, ErrClass: ClassInvalidOwner, Error: fmt.Sprintf("%s: owner %q must be a safe single path segment", req.Op, req.Owner)}
	}
	if resp, bad := canonReq(&req); bad {
		return resp
	}
	switch req.Op {
	case OpMount:
		if resp, bounced := s.retiringBusy("mount"); bounced {
			return resp
		}
		return s.handleMount(req)
	case OpUnmount:
		return s.handleUnmount(req)
	case OpList:
		return s.handleList(req)
	case OpReclaim:
		return s.handleReclaim(req)
	case OpLeases:
		return s.handleLeases(req)
	case OpAddBridge:
		if resp, bounced := s.retiringBusy("addbridge"); bounced {
			return resp
		}
		return s.handleAddBridge(req)
	case OpRemoveBridge:
		return s.handleRemoveBridge(req)
	case OpBridges:
		return s.handleBridges(req)
	default:
		return Response{OK: false, Error: unknownOpPrefix + " " + string(req.Op)}
	}
}

// canonReq canonicalizes the request's path fields EXACTLY ONCE, at the wire
// ingress: filepath.Clean of an absolute path — pure string, NO symlink
// resolution — so the canonical spelling is byte-identical everywhere
// downstream (lease key, registry, journal, mount ops) and /x/./mnt can never
// bypass /x/mnt's fence. Non-absolute paths are refused.
func canonReq(req *Request) (Response, bool) {
	for _, f := range []*string{&req.Base, &req.Dir, &req.MuxRoot} {
		if *f == "" {
			continue
		}
		if !filepath.IsAbs(*f) {
			return Response{OK: false, Error: fmt.Sprintf("%s: path %q must be absolute", req.Op, *f)}, true
		}
		*f = filepath.Clean(*f)
	}
	return Response{}, false
}

// handleHello answers the capability negotiation: proto (stamped by
// writeResp), the holder's version, and its feature set.
func (s *Server) handleHello() Response {
	return Response{OK: true, Version: s.Version, Features: HolderFeatures}
}

// handleHealth answers the liveness probe plus the additive status snapshot,
// reading the retire state under the tick's own locks/atomics.
func (s *Server) handleHealth() Response {
	resp := Response{OK: true, Version: s.Version, Retiring: s.retiring.Load(), ReplayDone: s.replayDone.Load()}
	s.retireMu.Lock()
	if !s.parkedUntil.IsZero() {
		resp.ParkedUntil = s.parkedUntil.Unix()
	}
	if s.strikes != nil {
		for _, t := range s.strikes.Times() {
			resp.RetireStrikes = append(resp.RetireStrikes, t.Unix())
		}
	}
	resp.RetireDeferredDir = s.retireDeferredDir
	resp.RetireDeferredReason = s.retireDeferredReason
	s.retireMu.Unlock()
	if s.journal != nil {
		resp.JournalMounts, resp.JournalBridges = s.journal.counts()
	}
	if s.LeaseDir != "" {
		if infos, err := lease.List(s.LeaseDir); err == nil {
			resp.LeasesTotal = len(infos)
			for _, in := range infos {
				if in.Held {
					resp.LeasesHeld++
				}
			}
		}
	}
	s.mu.Lock()
	for dir := range s.wedged {
		resp.WedgedDirs = append(resp.WedgedDirs, dir)
	}
	s.mu.Unlock()
	sort.Strings(resp.WedgedDirs)
	resp.Warning = s.persistWarnings()
	return resp
}

// recordPersistWarn / clearPersistWarn track a journal persist-failure whose
// op already acked OK and returned (a parked bridge removal's late flush);
// persistWarnings joins them for health.
func (s *Server) recordPersistWarn(key, msg string) {
	s.warnMu.Lock()
	s.persistWarns[key] = msg
	s.warnMu.Unlock()
}

func (s *Server) clearPersistWarn(key string) {
	s.warnMu.Lock()
	delete(s.persistWarns, key)
	s.warnMu.Unlock()
}

func (s *Server) persistWarnings() string {
	s.warnMu.Lock()
	defer s.warnMu.Unlock()
	msgs := make([]string, 0, len(s.persistWarns))
	for _, m := range s.persistWarns {
		msgs = append(msgs, m)
	}
	sort.Strings(msgs)
	return strings.Join(msgs, "; ")
}

// handleLeases answers the read-only lease-file diagnostic: lease files with
// held/free state and the acquirer's advisory header. Owner-scoped by default
// (advisory Header.Owner match, like list/bridges); all:true widens to the
// read-only cross-tenant view (doctor). Probing never tears anything down
// (lease.Probe releases immediately).
func (s *Server) handleLeases(req Request) Response {
	infos, err := lease.List(s.LeaseDir)
	if err != nil {
		return Response{OK: false, Error: "leases: " + err.Error()}
	}
	out := make([]LeaseInfo, 0, len(infos))
	for _, in := range infos {
		if !req.All && in.Header.Owner != req.Owner {
			continue
		}
		li := LeaseInfo{File: in.File, Held: in.Held, Dir: in.Header.Dir, Owner: in.Header.Owner, PID: in.Header.PID, Argv0: in.Header.Argv0}
		if !in.Header.Started.IsZero() {
			li.Started = in.Header.Started.Unix()
		}
		out = append(out, li)
	}
	return Response{OK: true, Leases: out}
}

// seizedFence is the set of lease fences one teardown action holds; Release
// drops them in reverse order.
type seizedFence struct{ fences []*lease.Fence }

func (f *seizedFence) Release() {
	for i := len(f.fences) - 1; i >= 0; i-- {
		_ = f.fences[i].Release()
	}
}

// seizeLeases seizes each dir's lease exclusively, in order, or fails with
// the busy dir's provenance (lease.ErrBusy). The returned fence spans the
// caller's ENTIRE teardown action — the in-kernel TOCTOU guard against a
// session acquiring mid-action.
func (s *Server) seizeLeases(dirs ...string) (*seizedFence, error) {
	fence := &seizedFence{}
	for _, d := range dirs {
		f, err := lease.Seize(s.LeaseDir, d)
		if err != nil {
			fence.Release()
			return nil, err
		}
		fence.fences = append(fence.fences, f)
	}
	return fence, nil
}

// subtreeLeaseHeld reports a HELD session lease whose advisory Header.Dir is
// root or lies under it, excluding the caller's own seized fence files. The
// seize set covers only journal-known dirs, so an UNJOURNALED subtree's live
// lease must still defer a root clear. Advisory headers only widen protection
// (defer more), never authorize action; a list failure or an unattributable
// held lease fails closed (busy).
func (s *Server) subtreeLeaseHeld(root string, own *seizedFence) (string, bool) {
	infos, err := lease.List(s.LeaseDir)
	if err != nil {
		s.Log.Printf("lease scan under %s: %v (fail-closed: busy)", root, err)
		return root, true
	}
	owned := map[string]bool{}
	for _, f := range own.fences {
		owned[f.Path()] = true
	}
	for _, in := range infos {
		if !in.Held || owned[in.File] {
			continue
		}
		if in.Header.Dir == "" || in.Header.Dir == root || isUnder(in.Header.Dir, root) {
			return in.Header.Dir, true
		}
	}
	return "", false
}

// leaseBusy maps a seize failure onto the wire: lease-held reads ClassBusy
// with the acquirer's provenance in Error; anything else is a plain error.
func leaseBusy(op string, err error) Response {
	if errors.Is(err, lease.ErrBusy) {
		return Response{OK: false, ErrClass: ClassBusy, Error: op + ": " + err.Error()}
	}
	return Response{OK: false, Error: op + ": " + err.Error()}
}

func (s *Server) handleProbe() Response {
	if s.Probe == nil {
		return Response{OK: true, FuseOK: false}
	}
	ok, err := s.Probe()
	if err != nil {
		// The RPC succeeded (OK: true); the throwaway probe MOUNT failed. Carry
		// its class so the driver learns why — hard fuse-unavailable vs pending
		// TCC — instead of a bare FuseOK=false.
		return Response{OK: true, FuseOK: false, ErrClass: mountErrClass(err), Error: err.Error()}
	}
	return Response{OK: true, FuseOK: ok}
}

// claim takes dir's in-flight gate: same-dir ops serialize (the second reads
// busy), different dirs proceed concurrently. The claim — not the mutex —
// owns the dir across the provider I/O, whose Setup has a registry
// check-then-act window two same-dir mounts would race.
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

func (s *Server) liveWithin(base, dir string) bool {
	st, ok := probeMount(s.Host.State, base, dir)
	return ok && st.mounted && st.alive
}

func (s *Server) registered(dir string) (row mountRow, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok = s.registry[dir]
	return row, ok
}

// deregister drops dir's row and journal entry; the error is the journal
// drop's persist-warning (the kernel state already changed — never a failure).
func (s *Server) deregister(dir string) error {
	s.mu.Lock()
	delete(s.registry, dir)
	s.mu.Unlock()
	return s.journalUnmount(dir)
}

func (s *Server) handleMount(req Request) Response {
	if req.Base == "" || req.Dir == "" {
		return Response{OK: false, Error: "mount: base and dir are required"}
	}
	// A mirror mounted over its own base would recurse into itself. Tree mode
	// gets no carve-out even though its Base is nominal (never read): mounting
	// over the base would shadow the consumer's backing tree from the consumer
	// itself, and handleUnmount refuses dir == base, so the mount could never
	// come down through the wire. Tree tenants mount at a dedicated dir.
	if req.Dir == req.Base {
		return Response{OK: false, Error: fmt.Sprintf("mount: refusing dir == base (%s)", req.Dir)}
	}
	if req.MuxRoot != "" {
		if resp, bad := validateMuxShape(req); bad {
			return resp
		}
	}
	if resp, bad := s.muxCollision(req); bad {
		return resp
	}

	release, ok := s.claim(req.Dir)
	if !ok {
		return Response{OK: false, ErrClass: ClassBusy, Error: "busy: another operation is in flight on " + req.Dir}
	}
	// parked: a pending teardown transferred the claims (and fence) to a
	// watcher goroutine — the deferred releases must not fire (P-8).
	var parked bool
	defer func() {
		if !parked {
			release()
		}
	}()

	// A mux mount serializes on its MuxRoot as well as its Dir: establishing (or,
	// on the last detach, unmounting) the ONE native mount must never race a
	// sibling tenant's. The claim is non-blocking, so contention bounces as
	// retryable ClassBusy — never a deadlock (fixed dir-then-root order) or a
	// block. It is held across Host.Setup, so same-root tenants serialize; for a
	// single MuxRoot with a handful of tenants that cost is negligible, and the
	// alternative (a claim released before the child's bridge RPC) cannot close
	// the establish-vs-last-detach race across the atomic Host.Setup/Teardown seam.
	var rootRelease func()
	if req.MuxRoot != "" {
		rootRelease, ok = s.claim(req.MuxRoot)
		if !ok {
			return Response{OK: false, ErrClass: ClassBusy, Error: "busy: another operation is in flight on mux root " + req.MuxRoot}
		}
		defer func() {
			if !parked {
				rootRelease()
			}
		}()
	}

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
		if row.MuxRoot != req.MuxRoot {
			return Response{
				OK:       false,
				ErrClass: ClassMuxMismatch,
				Error:    fmt.Sprintf("mount: %s is registered as %s, not %s; unmount it first", req.Dir, topoName(row.MuxRoot), topoName(req.MuxRoot)),
			}
		}
		// Bounded, fail closed: a wedged probe reads dead, routing into the
		// teardown ladder below instead of hanging the handler. Shallow-live is
		// idempotently OK — partial-wedge detection is the daemon's
		// (MountInfo.Live), and it tears a wedged mirror down before issuing
		// this Mount.
		if s.liveWithin(req.Base, req.Dir) {
			// Idempotent OK — but the journal is re-serve identity, so a spec
			// that drifted (content wiring, attr cache) rewrites its row first.
			// A failed rewrite fails the op (mount stays up, row rolled back)
			// so the retry re-attempts the write instead of no-opping stale.
			if err := s.refreshJournalRow(spec); err != nil {
				return Response{OK: false, Error: fmt.Sprintf("mount %s: journal write failed (mount stays up; retry): %v", req.Dir, err)}
			}
			return Response{OK: true}
		}
		// The registered mirror died while the holder lived (external umount,
		// fuse-t fault, a detached mux subtree). The lease ladder governs the
		// teardown: a live session lease defers with provenance; otherwise the
		// corpse comes down gracefully under the held fence — for a mux
		// subtree a logical detach, not a kernel unmount.
		fence, err := s.seizeLeases(leaseDirs(req.Dir, row.MuxRoot)...)
		if err != nil {
			return leaseBusy("remount "+req.Dir, err)
		}
		defer func() {
			if !parked {
				fence.Release()
			}
		}()
		s.drain(req.Dir)
		err = s.Host.Teardown(req.Base, req.Dir)
		// Drop the row regardless of outcome, as in handleUnmount.
		warn := s.deregister(req.Dir)
		if err != nil {
			parked = s.parkPendingTeardown("remount", req.Dir, kernelRoot(req.Dir, row.MuxRoot), err, fence, true, rootRelease, release)
			class := unmountErrClass(err)
			if class == "" {
				class = ClassMountFailed
			}
			s.Log.Printf("remount %s: tear down dead mirror: %v", req.Dir, err)
			resp := Response{OK: false, ErrClass: class, Error: fmt.Sprintf("remount %s: tear down dead mirror: %v", req.Dir, err)}
			if warn != nil {
				resp.Warning = "journal: " + warn.Error()
			}
			return resp
		}
		s.Log.Printf("remounting dead mirror %s <- %s", req.Dir, req.Base)
		// Teardown verified the mountpoint is gone; skip the foreign-mount
		// check. The fence stays held across the remount.
		resp, serr := s.setupAndRegister(spec)
		if serr != nil {
			parked = s.parkPendingTeardown("remount", req.Dir, kernelRoot(req.Dir, row.MuxRoot), serr, fence, true, rootRelease, release)
		}
		if warn != nil && resp.Warning == "" {
			resp.Warning = "journal: " + warn.Error()
		}
		return resp
	}
	// Never stack mounts. For a mux tenant the subtree is never its own kernel
	// mountpoint (it lives in the shared native mount), so the foreign-mount
	// probe targets the ROOT — and only on the FIRST attach: once the root is
	// ours a mountpoint there is ours, not a carcass to refuse.
	if req.MuxRoot != "" {
		if !s.rootEstablished(req.MuxRoot) {
			st, ok := probeMount(s.Host.State, filepath.Dir(req.MuxRoot), req.MuxRoot)
			if !ok {
				return Response{OK: false, ErrClass: ClassWedged, Error: fmt.Sprintf("mount: mux root %s stat did not answer; not proven dead — deferring (a hanging stat is never a carcass)", req.MuxRoot)}
			}
			if st.mounted {
				resp, p := s.clearCarcassAndMount(spec, req.MuxRoot, append(s.journaledTenants(req.MuxRoot, req.Dir), req.Dir, req.MuxRoot), rootRelease, release)
				parked = p
				return resp
			}
		}
		resp, serr := s.setupAndRegister(spec)
		if serr != nil {
			// A failed FIRST-child setup can leave the empty root's unmount in
			// flight (fusekit.ErrTeardownPending); park the claims on it.
			parked = s.parkPendingTeardown("mount", req.Dir, req.MuxRoot, serr, nil, true, rootRelease, release)
		}
		return resp
	}
	// Never stack mounts: a rowless mountpoint is not ours (a dead holder's
	// carcass, or foreign). Fail closed: an unanswered probe reads not-proven
	// — refuse, never stack over it or hang with the claim held (retries
	// would then read busy forever), and NEVER force under a hanging stat.
	st, ok := probeMount(s.Host.State, req.Base, req.Dir)
	if !ok {
		return Response{OK: false, ErrClass: ClassWedged, Error: fmt.Sprintf("mount: %s stat did not answer; not proven dead — deferring (a hanging stat is never a carcass)", req.Dir)}
	}
	if st.mounted {
		resp, p := s.clearCarcassAndMount(spec, req.Dir, []string{req.Dir}, release)
		parked = p
		return resp
	}
	resp, serr := s.setupAndRegister(spec)
	if serr != nil {
		parked = s.parkPendingTeardown("mount", req.Dir, req.Dir, serr, nil, true, release)
	}
	return resp
}

// clearCarcassAndMount is the PRE-MOUNT CARCASS CLEAR. THE INVARIANT:
// force-unmount exists at EXACTLY two holder-internal sites — this pre-mount
// carcass clear and the replay carcass clear (replayJournal) — both executed
// under a seized lease EX fence with carcass proof v2 = (stat answers
// IMMEDIATELY with ENOTCONN/EIO/EPERM/EACCES) ∧ (mount identity pinned) ∧
// (the mount's go-nfsv4 server proven dead BEFORE forcing, pid-reuse-proof).
// A hanging stat is NEVER proof, anywhere. No public fusekit API offers
// force (internal/carcass). Residual, accepted: cgofuse's own
// SIGTERM→MNT_FORCE handler is defused per mount right after mount-ready
// (fusekit.Config.ReArmSignals), so a TERM in that narrow pre-Reset window
// can force that single fresh mount — empty, no dirty pages; at steady state
// cgofuse is fully defused.
//
// A rowless mountpoint at root blocks spec's mount: under the seized fence
// (busy defers with provenance) plus the lease-dir subtree scan (an
// unjournaled tenant's live lease also defers) — re-run IMMEDIATELY before
// the force syscall via carcass.Clear's preForce hook — carcass.Clear forces
// IFF the proof holds, and a root that still stats healthy afterwards is a
// LIVE foreign mount, refused. The fence spans clear + remount, and
// Host.Setup fails loud on a mount that appears in the gap — Mount never
// clears. Reports parked=true when a pending force or setup unwind
// transferred the fence and the caller's releases to a watcher.
//
// Residual race, by design: the pre-force re-scan cannot be kernel-atomic
// across a lease-file set, so an Acquire can land in the last instant. The
// mount at that point is PROVEN dead (server dead, dead-errno stat): that
// acquirer's session was already doomed — the force converts its dead-errno
// into ENOENT; panic-safety is unaffected. The lease package documents the
// consumer's probe-after-Acquire contract.
func (s *Server) clearCarcassAndMount(spec fusekit.MountSpec, root string, seize []string, releases ...func()) (resp Response, parked bool) {
	fence, err := s.seizeLeases(seize...)
	if err != nil {
		return leaseBusy("mount "+spec.Dir, err), false
	}
	defer func() {
		if !parked {
			fence.Release()
		}
	}()
	scan := func() error {
		if dir, busy := s.subtreeLeaseHeld(root, fence); busy {
			return fmt.Errorf("%w: session lease held on %s", errSubtreeLeaseHeld, dir)
		}
		return nil
	}
	if err := scan(); err != nil {
		return Response{OK: false, ErrClass: ClassBusy, Error: fmt.Sprintf("mount %s: carcass clear of %s deferred: %v", spec.Dir, root, err)}, false
	}
	if err := clearCarcass(root, scan); err != nil {
		if errors.Is(err, errSubtreeLeaseHeld) {
			return Response{OK: false, ErrClass: ClassBusy, Error: fmt.Sprintf("mount %s: carcass clear of %s deferred: %v", spec.Dir, root, err)}, false
		}
		parked = s.parkPendingForce("mount", root, err, fence, releases...)
		return Response{OK: false, ErrClass: ClassWedged, Error: fmt.Sprintf("mount %s: carcass at %s: %v", spec.Dir, root, err)}, parked
	}
	if st, ok := probeMount(s.Host.State, filepath.Dir(root), root); !ok || st.mounted {
		return Response{
			OK:       false,
			ErrClass: ClassForeignMount,
			Error:    fmt.Sprintf("mount: %s is a live mountpoint this holder does not own; unmount it first", root),
		}, false
	}
	resp, serr := s.setupAndRegister(spec)
	if serr != nil {
		parked = s.parkPendingTeardown("mount", spec.Dir, root, serr, fence, true, releases...)
	}
	return resp, parked
}

// errSubtreeLeaseHeld defers a carcass clear on a held subtree lease —
// including one acquired between the caller's scan and the force syscall
// (carcass.Clear re-runs the scan as its preForce hook).
var errSubtreeLeaseHeld = errors.New("carcass clear deferred")

// journaledTenants returns the journaled subtree dirs of muxRoot other than
// exclude — the sibling leases a root carcass clear must also seize.
func (s *Server) journaledTenants(muxRoot, exclude string) []string {
	if s.journal == nil {
		return nil
	}
	mounts, _ := s.journal.snapshot()
	var dirs []string
	for _, m := range mounts {
		if m.MuxRoot == muxRoot && m.Dir != exclude {
			dirs = append(dirs, m.Dir)
		}
	}
	return dirs
}

// leaseDirs is the lease set one dir's teardown must seize: the dir, plus its
// mux root when it is a subtree (mux-root busy = root lease held or any
// subtree's lease held).
func leaseDirs(dir, muxRoot string) []string {
	if muxRoot != "" {
		return []string{dir, muxRoot}
	}
	return []string{dir}
}

// validateMuxShape checks a mux request's static geometry: MuxRoot absolute, Dir
// a direct child of MuxRoot, and MuxRoot outside Base (a native mount inside the
// shared base would shadow it, and the subtree Dir would then land under the
// base too). These are malformed-request refusals — plain errors, no class; the
// registry-collision refusals (ClassMuxMismatch) are muxCollision's.
func validateMuxShape(req Request) (Response, bool) {
	if !filepath.IsAbs(req.MuxRoot) {
		return Response{OK: false, Error: fmt.Sprintf("mount: mux root %q must be absolute", req.MuxRoot)}, true
	}
	if filepath.Dir(req.Dir) != req.MuxRoot {
		return Response{OK: false, Error: fmt.Sprintf("mount: mux dir %q must be a direct child of root %q", req.Dir, req.MuxRoot)}, true
	}
	if req.MuxRoot == req.Base || isUnder(req.MuxRoot, req.Base) {
		return Response{OK: false, Error: fmt.Sprintf("mount: mux root %q must not be the base %q or under it", req.MuxRoot, req.Base)}, true
	}
	return Response{}, false
}

// muxCollision refuses a mount whose topology conflicts with a registered row: a
// plain mount whose Dir is already a mux native root, or a mux mount whose root
// path is already a plain mount. Registry state (ClassMuxMismatch), never a
// mount verdict — the driver unmounts the conflicting row and retries.
func (s *Server) muxCollision(req Request) (Response, bool) {
	snap := s.snapshotRegistry()
	if req.MuxRoot == "" {
		for _, row := range snap {
			if row.MuxRoot == req.Dir {
				return Response{
					OK:       false,
					ErrClass: ClassMuxMismatch,
					Error:    fmt.Sprintf("mount: %s serves mux subtrees; unmount its tenants before a plain mount there", req.Dir),
				}, true
			}
		}
		return Response{}, false
	}
	if row, ok := snap[req.MuxRoot]; ok && row.MuxRoot == "" {
		return Response{
			OK:       false,
			ErrClass: ClassMuxMismatch,
			Error:    fmt.Sprintf("mount: mux root %s is already a plain mount; unmount it first", req.MuxRoot),
		}, true
	}
	return Response{}, false
}

// rootEstablished reports whether the holder already serves a subtree of muxRoot
// — i.e. its native mount is up. Callers hold the MuxRoot claim, so the answer
// is stable across the establish decision. Beyond the registry it consults the
// provider (MuxRootHolder): a wedged last-child unmount deregisters the row but
// leaves the native mount up, so the row is gone while the root is still ours —
// a later tenant must re-attach to that surviving mount, not refuse it as
// foreign (and the next last-detach retries the graceful unmount).
func (s *Server) rootEstablished(muxRoot string) bool {
	s.mu.Lock()
	for _, row := range s.registry {
		if row.MuxRoot == muxRoot {
			s.mu.Unlock()
			return true
		}
	}
	s.mu.Unlock()
	if h, ok := s.Host.(MuxRootHolder); ok {
		return h.HoldsMuxRoot(muxRoot)
	}
	return false
}

// isUnder reports whether path is strictly nested under base.
func isUnder(path, base string) bool {
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// topoName renders a row's topology for a mismatch message.
func topoName(muxRoot string) string {
	if muxRoot == "" {
		return "a plain mount"
	}
	return "a subtree of mux root " + muxRoot
}

// mountErrClass maps a provider mount error to its wire error class. Ordered:
// ErrMountTimeout (proven grant, transient) classifies before ErrMountNotLive
// (presumed TCC); anything else is ClassMountFailed, so a hard failure never
// reaches the driver wearing the TCC walkthrough.
func mountErrClass(err error) string {
	switch {
	case errors.Is(err, content.ErrBridgeUnavailable):
		return ClassContentUnavailable
	case errors.Is(err, fusekit.ErrMuxMismatch):
		return ClassMuxMismatch
	case errors.Is(err, fusekit.ErrMountTimeout):
		return ClassMountTimeout
	case errors.Is(err, fusekit.ErrMountNotLive):
		return ClassTCC
	default:
		return ClassMountFailed
	}
}

func mountSpec(req Request) fusekit.MountSpec {
	return fusekit.MountSpec{
		Base:             req.Base,
		Dir:              req.Dir,
		Owner:            req.Owner,
		MuxRoot:          req.MuxRoot,
		ContentSocket:    req.ContentSocket,
		Domain:           req.Domain,
		PrivateRoot:      req.PrivateRoot,
		ContentMode:      req.ContentMode,
		ProbePath:        req.ProbePath,
		PrivatePrefixes:  req.PrivatePrefixes,
		AttrCache:        req.AttrCache,
		AttrCacheTimeout: req.AttrCacheTimeout,
	}
}

// drainGrace bounds the pre-teardown write-through drain: above the content
// bridge's full RPC ceiling (dial+op ≈ 5.5s) so a slow final write-through
// lands, under OpUnmount's 15s / OpReclaim's 60s; a hung consumer's private
// file remains the durable source of truth.
const drainGrace = 6 * time.Second

func (s *Server) drain(dir string) {
	if d, ok := s.Host.(Drainer); ok {
		d.Drain(dir, drainGrace)
	}
}

// setupAndRegister mounts spec and records its registry row under a bumped
// epoch. The caller holds dir's in-flight claim. The second return is the
// SETUP error only — non-nil when the host refused the mount (a mux
// first-child unwind can carry fusekit.ErrTeardownPending in it; the caller
// parks on that) — never the journal-write failure, whose mount is up.
func (s *Server) setupAndRegister(spec fusekit.MountSpec) (Response, error) {
	// Every mount carries the server's signal re-arm hook: fusekit.Mount's
	// post-ready signal.Reset (the cgofuse defuse) unsubscribes the holder's
	// own shutdown channel too.
	spec.ReArmSignals = s.reArmSignals
	if err := s.Host.Setup(spec); err != nil {
		s.Log.Printf("mount %s <- %s: %v", spec.Dir, spec.Base, err)
		return Response{OK: false, ErrClass: mountErrClass(err), Error: err.Error()}, err
	}
	s.mu.Lock()
	s.epochs[spec.Dir]++
	s.registry[spec.Dir] = mountRow{Base: spec.Base, Owner: spec.Owner, Epoch: s.epochs[spec.Dir], MountedAt: time.Now(), MuxRoot: spec.MuxRoot}
	s.mu.Unlock()
	if err := s.journalMount(spec); err != nil {
		// The mount is up and registered; only the durable mirror is stale.
		// Fail the op so the driver retries — the retry lands on the
		// idempotent path, whose refresh re-attempts the write (T-6).
		return Response{OK: false, Error: fmt.Sprintf("mount %s: mounted but the journal write failed (retry): %v", spec.Dir, err)}, nil
	}
	s.Log.Printf("mounted %s <- %s", spec.Dir, spec.Base)
	return Response{OK: true}, nil
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
	// parked: a pending teardown transferred the claims and fence to a
	// watcher goroutine — the deferred releases must not fire (P-8).
	var parked bool
	defer func() {
		if !parked {
			release()
		}
	}()

	row, ok := s.registered(req.Dir)
	// Owner misfire guard, NOT a security boundary (Owner is client-asserted
	// over a same-UID socket): a row registered with an owner may only be
	// unmounted by that owner. An ownerless row stays open to any owner —
	// legacy single-consumer mounts, and carcass teardown, keep working.
	if ok && row.Owner != "" && row.Owner != req.Owner {
		return Response{OK: false, ErrClass: ClassOwnerMismatch, Error: fmt.Sprintf("unmount: %s is owned by %q, not %q", req.Dir, row.Owner, req.Owner)}
	}
	base := row.Base
	muxRoot := row.MuxRoot
	if !ok {
		// A rowless dir may still be journal-owned (a lease-deferred replay
		// keeps the row without a registry entry): enforce the journal row's
		// owner exactly like a registry row's, so a foreign owner can neither
		// tear the dir down nor delete the row — and inherit its MuxRoot so
		// the ladder below covers the same lease set a registered teardown's
		// would.
		if s.journal != nil {
			if je, jok := s.journal.mount(req.Dir); jok {
				if je.Owner != "" && je.Owner != req.Owner {
					return Response{OK: false, ErrClass: ClassOwnerMismatch, Error: fmt.Sprintf("unmount: %s is journaled to %q, not %q", req.Dir, je.Owner, req.Owner)}
				}
				muxRoot = je.MuxRoot
			}
		}
		// Teardown needs base only for its base==dir refusal, so the
		// request's Base serves.
		base = req.Base
	}
	// A mux subtree serializes its detach on the MuxRoot too: the last
	// child's native unmount must not race a sibling establish/detach.
	// Non-blocking (retryable ClassBusy), fixed dir-then-root order.
	var rootRelease func()
	if muxRoot != "" {
		var rok bool
		rootRelease, rok = s.claim(muxRoot)
		if !rok {
			return Response{OK: false, ErrClass: ClassBusy, Error: "busy: another operation is in flight on mux root " + muxRoot}
		}
		defer func() {
			if !parked {
				rootRelease()
			}
		}()
	}
	// The lease ladder — journal-only rows INCLUDED: a held lease defers with
	// the acquirer's provenance BEFORE any journal drop, so an active
	// session's lease-deferred replay row survives a reclaim; the seized
	// fence spans the whole graceful teardown.
	fence, err := s.seizeLeases(leaseDirs(req.Dir, muxRoot)...)
	if err != nil {
		return leaseBusy("unmount "+req.Dir, err)
	}
	defer func() {
		if !parked {
			fence.Release()
		}
	}()
	if !ok {
		// Fail closed: an unanswered probe (a wedged carcass) reads
		// still-mounted, routing into the bounded graceful teardown — never an
		// OK no-op for a possibly-live mountpoint, never a hung handler. A
		// rowless mux subtree reads not-mounted (it is never its own kernel
		// mountpoint) and no-ops here.
		if st, pok := probeMount(s.Host.State, req.Base, req.Dir); pok && !st.mounted {
			// Drop any surviving journal entry UNDER the seized fence: a
			// retire sweep leaves exactly this state — row dropped, journal
			// kept, kernel unmounted — and an acked owner Unmount must not
			// let a successor resurrect it.
			return okWithWarning(s.journalUnmount(req.Dir)) // not mounted at all: no-op
		}
		// A carcass (rowless mountpoint) comes down gracefully or not at all
		// — the pre-mount clear is the force site.
	}
	s.drain(req.Dir)
	err = s.Host.Teardown(base, req.Dir)
	// Drop row + journal even on a wedge (the provider RESTORED its handle):
	// an explicit owner unmount must never resurrect via replay — a leftover
	// carcass is the successor's ReapOrphanedServers pass. Deliberately
	// asymmetric with retireSweep, which keeps its wedge-survivors' rows.
	warn := s.deregister(req.Dir)
	if err != nil {
		parked = s.parkPendingTeardown("unmount", req.Dir, kernelRoot(req.Dir, muxRoot), err, fence, true, rootRelease, release)
		s.Log.Printf("unmount %s: %v", req.Dir, err)
		resp := Response{OK: false, ErrClass: unmountErrClass(err), Error: err.Error()}
		if warn != nil {
			// The error response must not swallow the journal persist-warning:
			// the row is already dropped and a successor could replay it.
			resp.Warning = "journal: " + warn.Error()
		}
		return resp
	}
	s.Log.Printf("unmounted %s", req.Dir)
	return okWithWarning(warn)
}

// unmountErrClass maps a teardown error to its wire class: a prompt EBUSY
// refusal is retryable ClassBusy (the frozen protocol contract); any other
// wedge — a pending verdict included — is ClassWedged.
func unmountErrClass(err error) string {
	switch {
	case errors.Is(err, fusekit.ErrTeardownPending):
		return ClassWedged
	case errors.Is(err, fusekit.ErrMountBusy):
		return ClassBusy
	case errors.Is(err, fusekit.ErrUnmountWedged):
		return ClassWedged
	default:
		return ""
	}
}

// okWithWarning is an OK response carrying a journal persist-warning: the
// kernel operation succeeded and must never be reported failed; the stale
// file heals on the next save (full snapshot).
func okWithWarning(warn error) Response {
	resp := Response{OK: true}
	if warn != nil {
		resp.Warning = "journal: " + warn.Error()
	}
	return resp
}

func (s *Server) handleList(req Request) Response {
	// Live semantics are MountInfo's. The probes are stat-side I/O the
	// registry lock must not span, and any one can wedge with its mirror, so
	// entries are probed in parallel, each bounded: a wedged mirror reads
	// Live=false while healthy siblings still answer within the deadline.
	snap := s.snapshotRegistry()
	dirs := make([]string, 0, len(snap))
	for dir, row := range snap {
		if !req.All && row.Owner != req.Owner {
			continue
		}
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	mounts := make([]MountInfo, len(dirs))
	var wg sync.WaitGroup
	for i, dir := range dirs {
		row := snap[dir]
		mounts[i] = MountInfo{Dir: dir, Base: row.Base, Owner: row.Owner, Epoch: row.Epoch, MuxRoot: row.MuxRoot}
		if !row.MountedAt.IsZero() {
			mounts[i].MountedAt = row.MountedAt.Unix()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			mounts[i].Live = s.liveWithin(row.Base, dir)
		}()
	}
	wg.Wait()
	return Response{OK: true, Mounts: mounts}
}

// handleReclaim sweeps the owner's mounts and bridge; EVERY persist-warning
// along the way — sweep journal drops, journal-only unmount warnings, the
// bridge drop — aggregates into the OK reply's Warning, never silently
// swallowed (a clean-looking reclaim over a stale journal would let a
// successor resurrect reclaimed mounts).
func (s *Server) handleReclaim(req Request) Response {
	if req.Owner == "" {
		return Response{OK: false, Error: "reclaim: owner is required"}
	}
	failed, warnings := s.unmountOwned(req.Owner)
	jfailed, jwarnings := s.reclaimJournalOnly(req.Owner)
	failed = append(failed, jfailed...)
	warnings = append(warnings, jwarnings...)
	if werr := s.reclaimBridge(req.Owner); werr != nil {
		warnings = append(warnings, "journal: "+werr.Error())
	}
	return Response{OK: true, Mounts: failed, Warning: strings.Join(warnings, "; ")}
}

// reclaimJournalOnly sweeps the owner's ROWLESS journal entries — a
// lease-deferred replay keeps the row without a registry entry, and a
// reclaim that ignored it would hand the entry to the successor's replay.
// Each goes through handleUnmount: same owner guard, same lease ladder (a
// held session lease answers busy and the row SURVIVES). OK-reply warnings
// bubble up.
func (s *Server) reclaimJournalOnly(owner string) (failed []MountInfo, warnings []string) {
	if s.journal == nil {
		return nil, nil
	}
	mounts, _ := s.journal.snapshot()
	for _, m := range mounts {
		if m.Owner != owner {
			continue
		}
		if _, ok := s.registered(m.Dir); ok {
			continue // rowful: unmountOwned's, or re-established since
		}
		resp := s.handleUnmount(Request{Op: OpUnmount, Base: m.Base, Dir: m.Dir, Owner: owner})
		if !resp.OK {
			s.Log.Printf("reclaim: journal-only %s: %s", m.Dir, resp.Error)
			failed = append(failed, MountInfo{Dir: m.Dir, Base: m.Base, Owner: m.Owner, MuxRoot: m.MuxRoot, Live: true})
			continue
		}
		if resp.Warning != "" {
			warnings = append(warnings, m.Dir+": "+resp.Warning)
		}
	}
	return failed, warnings
}

// kernelRoot is a dir's kernel mountpoint: its mux root when it is a
// subtree, else the dir itself — the path a park watcher re-stats at
// resolution.
func kernelRoot(dir, muxRoot string) string {
	if muxRoot != "" {
		return muxRoot
	}
	return dir
}

// parkPendingTeardown transfers the fence and claims to a detached watcher
// when the provider reports dir's graceful unmount STILL IN FLIGHT
// (fusekit.ErrTeardownPending): releasing them now could hand the dir to a
// new session an instant before the parked unmount lands beneath it (P-8).
// The watcher's exit is defined — the provider's resolution channel closes
// when the parked unmount CALL returns (after registry reconciliation);
// until then the dir stays claimed (ClassBusy) and its lease fence held. A
// contract violation (no TeardownPender, or a nil channel behind a pending
// verdict) fails CLOSED: the claims and fence stay held, loudly — an unknown
// outcome never releases early. dropJournal controls whether a clean
// resolution deregisters dir (registry row + journal row); the retire sweep
// passes false so its journal survives for the successor's replay. nil
// releases entries are skipped. Reports whether the transfer happened; the
// caller must then suppress its own deferred releases.
func (s *Server) parkPendingTeardown(what, dir, kroot string, err error, fence *seizedFence, dropJournal bool, releases ...func()) bool {
	if !errors.Is(err, fusekit.ErrTeardownPending) {
		return false
	}
	tp, ok := s.Host.(TeardownPender)
	if !ok {
		// Unreachable behind Validate; loud for handler-level embedders. The
		// fence is stored as wedge state — a strong reference, so GC can never
		// finalize its fd and drop LOCK_EX — and surfaced in health.
		s.Log.Printf("%s %s: HOST CONTRACT VIOLATION: ErrTeardownPending from a Host without TeardownPender; keeping the dir fenced and claimed (fail closed; health: wedged_dirs)", what, dir)
		s.storeWedge(dir, fence)
		return true
	}
	ch := tp.TeardownDone(dir)
	if ch == nil {
		s.Log.Printf("%s %s: HOST CONTRACT VIOLATION: ErrTeardownPending with no resolution channel; keeping the dir fenced and claimed (fail closed; health: wedged_dirs)", what, dir)
		s.storeWedge(dir, fence)
		return true
	}
	s.Log.Printf("%s %s: graceful teardown still in flight; parking the lease fence and claims until the unmount call resolves", what, dir)
	s.parkWatcher(what+" (in-flight teardown)", dir, kroot, ch, fence, dropJournal, releases...)
	return true
}

// parkPendingForce is parkPendingTeardown's carcass-force twin: a bounded
// force that timed out (carcass.PendingForce) may still land, so the fence
// and claims transfer to a watcher keyed on the force process's actual exit —
// never released while the late unmount can land under a fresh session. The
// journal is never dropped here: a deferred root's tenant rows stay for the
// next generation.
func (s *Server) parkPendingForce(what, dir string, err error, fence *seizedFence, releases ...func()) bool {
	var pf *carcass.PendingForce
	if !errors.As(err, &pf) {
		return false
	}
	s.Log.Printf("%s %s: FORCE STILL IN FLIGHT past its bound; parking the lease fence and claims until the umount process exits", what, dir)
	s.parkWatcher(what+" (in-flight force)", dir, dir, pf.Done, fence, false, releases...)
	return true
}

// parkWatcher owns one park, keyed on BOTH dir and its kernel root (a
// sibling tenant's remount collides on the root claim, so awaitPark must
// find the park under either). When ch resolves it re-reads kernel truth —
// FRESH and bounded, never joined onto an in-flight pre-resolution probe,
// which could re-serve a verdict sampled before the call returned: kroot
// gone means the teardown landed — the registry row drops either way (the
// mount is gone, the row would lie; dropJournal decides the JOURNAL row
// alone, so a retire/force park's replay intent survives) and the fence and
// claims release; kroot still mounted (or unanswered) is a FINAL WEDGE — the
// fence transfers to wedge state (a strong, GC-proof reference surfaced in
// health) and the claims stay held, watched by watchWedgeClear, so no new
// session lands on a mount in an unknowable state. Either way the park
// itself resolves (parkedDirs entry removed, channel closed) — never a
// silent infinite park. Every watcher is on parkWG; Run's shutdown waits
// them out.
func (s *Server) parkWatcher(what, dir, kroot string, ch <-chan struct{}, fence *seizedFence, dropJournal bool, releases ...func()) {
	released := make(chan struct{})
	s.mu.Lock()
	s.parkedDirs[dir] = released
	if kroot != dir {
		s.parkedDirs[kroot] = released
	}
	s.mu.Unlock()
	s.parkWG.Add(1)
	go func() {
		defer s.parkWG.Done()
		<-ch
		st, ok := probeMountFresh(s.Host.State, filepath.Dir(kroot), kroot)
		if gone := ok && !st.mounted; gone {
			s.resolveGone(what, dir, dropJournal, fence, releases...)
			s.Log.Printf("%s %s: resolved; fence and claims released", what, dir)
		} else {
			s.storeWedge(dir, fence)
			s.Log.Printf("%s %s: RESOLVED TO A FINAL WEDGE: the call returned but %s still reads mounted; keeping the dir fenced and claimed (health: wedged_dirs) until the wedge clears or the holder exits", what, dir, kroot)
			s.watchWedgeClear(what, dir, kroot, dropJournal, releases...)
		}
		s.mu.Lock()
		if s.parkedDirs[dir] == released {
			delete(s.parkedDirs, dir)
		}
		if s.parkedDirs[kroot] == released {
			delete(s.parkedDirs, kroot)
		}
		s.mu.Unlock()
		close(released)
	}()
}

// resolveGone reconciles a park (or wedge) whose kernel mount is observed
// gone: the registry row drops unconditionally, the journal row per the
// park's intent, then the fence and claims release — in that order, so a new
// Setup can never race a stale row.
func (s *Server) resolveGone(what, dir string, dropJournal bool, fence *seizedFence, releases ...func()) {
	if dropJournal {
		if werr := s.deregister(dir); werr != nil {
			s.Log.Printf("%s %s: resolved clean but the journal drop failed (heals on the next save): %v", what, dir, werr)
		}
	} else {
		s.mu.Lock()
		delete(s.registry, dir)
		s.mu.Unlock()
	}
	if fence != nil {
		fence.Release()
	}
	for i := len(releases) - 1; i >= 0; i-- {
		if releases[i] != nil {
			releases[i]()
		}
	}
}

// storeWedge pins dir's fence in server state for the process lifetime: the
// seizedFence owns *os.Files whose GC finalizer would silently close the fds
// — dropping LOCK_EX while the in-memory claim remains — the moment the
// watcher goroutine exits. nil fences still record the dir for health.
func (s *Server) storeWedge(dir string, fence *seizedFence) {
	s.mu.Lock()
	s.wedged[dir] = fence
	s.mu.Unlock()
}

// wedgeRecheck paces watchWedgeClear's kernel re-probe; a var so tests
// shrink it.
var wedgeRecheck = 30 * time.Second

// watchWedgeClear re-probes a resolved-to-wedge dir until the mount is
// observed gone — an operator's external unmount — then releases the stored
// fence and claims and reconciles the rows exactly like a clean resolution.
// Detached from parkWG (the unmount call already returned; shutdown must not
// hang on a permanent wedge) with a defined exit: the wedge clears, or
// runCtx ends. A never-cleared fence dies with the process fd — the accepted
// residual.
func (s *Server) watchWedgeClear(what, dir, kroot string, dropJournal bool, releases ...func()) {
	go func() {
		t := time.NewTicker(s.wedgeEvery)
		defer t.Stop()
		for {
			select {
			case <-s.runCtx.Done():
				return
			case <-t.C:
			}
			st, ok := probeMountFresh(s.Host.State, filepath.Dir(kroot), kroot)
			if !ok || st.mounted {
				continue
			}
			s.mu.Lock()
			fence := s.wedged[dir]
			delete(s.wedged, dir)
			s.mu.Unlock()
			s.resolveGone(what, dir, dropJournal, fence, releases...)
			s.Log.Printf("%s %s: WEDGE CLEARED: %s no longer reads mounted; fence and claims released", what, dir, kroot)
			return
		}
	}()
}

// awaitPark waits (bounded) for dir's parked fence and claims to release —
// the retire abort's remount would otherwise bounce off the sweep's own
// still-parked claim.
func (s *Server) awaitPark(dir string, bound time.Duration) bool {
	s.mu.Lock()
	ch := s.parkedDirs[dir]
	s.mu.Unlock()
	if ch == nil {
		return true
	}
	select {
	case <-ch:
		return true
	case <-time.After(bound):
		return false
	}
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

// unmountAll sweeps every mount; unmountOwned sweeps one owner's. sweep
// claims each dir (a busy dir is reported failed, not raced) and returns the
// dirs still mounted plus any journal persist-warnings.
func (s *Server) unmountAll() []MountInfo {
	failed, warnings := s.sweep(func(mountRow) bool { return true })
	for _, w := range warnings {
		s.Log.Printf("final sweep: %s", w)
	}
	return failed
}

func (s *Server) unmountOwned(owner string) ([]MountInfo, []string) {
	return s.sweep(func(r mountRow) bool { return r.Owner == owner })
}

func (s *Server) sweep(match func(mountRow) bool) ([]MountInfo, []string) {
	snap := s.snapshotRegistry()
	dirs := make([]string, 0, len(snap))
	for dir, row := range snap {
		if match(row) {
			dirs = append(dirs, dir)
		}
	}
	sort.Strings(dirs)

	var failed []MountInfo
	var warnings []string
	for _, dir := range dirs {
		row := snap[dir]
		base := row.Base
		release, ok := s.claim(dir)
		if !ok {
			s.Log.Printf("sweep: %s busy; leaving it to its in-flight op", dir)
			failed = append(failed, MountInfo{Dir: dir, Base: base, Live: true})
			continue
		}
		// Revalidate the row UNDER the claim: between the snapshot and here the
		// dir may have been unmounted and re-mounted by another owner (or
		// re-epoch'd); tearing that replacement down on the stale snapshot
		// would delete a foreign row.
		cur, live := s.registered(dir)
		if !live || cur.Owner != row.Owner || cur.Epoch != row.Epoch {
			s.Log.Printf("sweep: %s changed since the snapshot (now %+v); skipping", dir, cur)
			release()
			continue
		}
		row, base = cur, cur.Base
		// A mux subtree's Teardown may unmount the shared native root (its last
		// child), so — exactly like handleMount/handleUnmount — it serializes on
		// the MuxRoot as well as the dir, in the same fixed dir-then-root order.
		// Without the root claim a sweep could latch last=true and unmount the
		// native root out from under a concurrent same-root handleMount that saw
		// the not-yet-deregistered row as rootEstablished. A busy root reports the
		// row failed and releases the dir claim, exactly like a busy dir; the root
		// claim spans Teardown+deregister and is released after.
		var rootRelease func()
		if row.MuxRoot != "" {
			var rok bool
			if rootRelease, rok = s.claim(row.MuxRoot); !rok {
				s.Log.Printf("sweep: mux root %s busy; leaving %s to its in-flight op", row.MuxRoot, dir)
				release()
				failed = append(failed, MountInfo{Dir: dir, Base: base, Live: true})
				continue
			}
		}
		fence, ferr := s.seizeLeases(leaseDirs(dir, row.MuxRoot)...)
		if ferr != nil {
			s.Log.Printf("sweep: %s lease busy; leaving it to its holder: %v", dir, ferr)
			if rootRelease != nil {
				rootRelease()
			}
			release()
			failed = append(failed, MountInfo{Dir: dir, Base: base, Live: true})
			continue
		}
		s.drain(dir)
		err := s.Host.Teardown(base, dir)
		// A failed teardown keeps the journal entry (the successor's replay
		// clears or surfaces the still-up mount) and, for a plain
		// mount, the row — the provider restored the handle. A wedged mux
		// tenant is already detached, so only its lying row drops (as in
		// retireSweep).
		if err == nil {
			if werr := s.deregister(dir); werr != nil {
				warnings = append(warnings, fmt.Sprintf("%s: journal: %v", dir, werr))
			}
		} else if row.MuxRoot != "" {
			s.mu.Lock()
			delete(s.registry, dir)
			s.mu.Unlock()
		}
		if !s.parkPendingTeardown("sweep", dir, kernelRoot(dir, row.MuxRoot), err, fence, true, rootRelease, release) {
			fence.Release()
			if rootRelease != nil {
				rootRelease()
			}
			release()
		}
		if err != nil {
			s.Log.Printf("sweep unmount %s: %v", dir, err)
			failed = append(failed, MountInfo{Dir: dir, Base: base, Live: true})
			continue
		}
		s.Log.Printf("sweep unmounted %s", dir)
	}
	return failed, warnings
}
