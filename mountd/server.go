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
	"github.com/yasyf/fusekit/content"
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

	// triggerShutdown cancels Run's context (OpShutdown). Set before the accept
	// loop; the handler go-statement's happens-before lets handlers read it
	// without a lock.
	triggerShutdown context.CancelFunc

	wg sync.WaitGroup

	mu       sync.Mutex
	registry map[string]mountRow // dir -> the mount this holder established
	inflight map[string]bool     // dir -> a mount/unmount holds the dir mid-I/O
	// epochs backs mountRow.Epoch. It lives outside the registry so it
	// survives the deregister between a dead mirror's teardown and its
	// remount — monotonic per dir for this process's lifetime, never reset.
	epochs map[string]uint64
}

type mountRow struct {
	Base      string
	Owner     string
	Epoch     uint64
	MountedAt time.Time
}

// Run binds the holder socket and serves until ctx is cancelled, the process
// is signalled (SIGTERM/SIGINT), or an OpShutdown lands; it then drains
// in-flight handlers and unmounts everything it owns, each teardown bounded
// by the provider's grace timers.
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

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	s.triggerShutdown = stop

	s.Log.Printf("mountd %s started; socket=%s", s.Version, s.Socket)

	go func() {
		<-ctx.Done()
		s.Log.Printf("shutdown trigger received; closing listener")
		closeListener()
	}()
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
	// Every claim is free post-drain; this sweep catches dirs an OpShutdown
	// sweep reported busy and mounts that landed after its snapshot.
	s.unmountAll()
	s.Log.Printf("mountd stopped")
	return nil
}

// initState resets per-run state; handler-level tests call it to dispatch
// without a socket.
func (s *Server) initState() {
	s.registry = map[string]mountRow{}
	s.inflight = map[string]bool{}
	s.epochs = map[string]uint64{}
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
		return nil, nil, fmt.Errorf("another mount holder owns %s.lock but does not answer health yet (it may still be starting); refusing to start", s.Socket)
	}
	if err != nil {
		return nil, nil, err
	}
	return ln, lock, nil
}

// opDeadline bounds one connection by its op. Each deadline sits BELOW its
// client timeout (Mount 25s/20s, Unmount 17s/15s, Shutdown 65s/60s) so the op
// deadline is the binding bound — a blown client deadline reads
// ErrHolderUnavailable and would mask the holder's real error class.
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

func (s *Server) deregister(dir string) {
	s.mu.Lock()
	delete(s.registry, dir)
	s.mu.Unlock()
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
		// Bounded, fail closed: a wedged probe reads dead, routing into the
		// forced teardown below instead of hanging the handler. Shallow-live is
		// idempotently OK — partial-wedge detection is the daemon's
		// (MountInfo.Live), and it tears a wedged mirror down before issuing
		// this Mount.
		if s.liveWithin(req.Base, req.Dir) {
			return Response{OK: true} // idempotent: this exact mount is held and live
		}
		// The registered mirror died while the holder lived (external umount,
		// fuse-t fault). The provider's Setup early-returns on its own stale
		// row, so the corpse must come down before the remount.
		s.drain(req.Dir)
		err := s.Host.Teardown(req.Base, req.Dir)
		// Drop the row regardless of outcome, as in handleUnmount.
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
		// Teardown verified the mountpoint is gone; skip the foreign-mount check.
		return s.setupAndRegister(spec)
	}
	// Never stack mounts: a rowless mountpoint is not ours (a dead holder's
	// carcass, or foreign). Fail closed: an unanswered probe (wedged carcass)
	// reads foreign — refuse, never stack over it or hang with the claim held
	// (retries would then read busy forever).
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
// (presumed TCC); anything else is ClassMountFailed, so a hard failure never
// reaches the driver wearing the TCC walkthrough.
func mountErrClass(err error) string {
	switch {
	case errors.Is(err, content.ErrBridgeUnavailable):
		return ClassContentUnavailable
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
// lands, under OpUnmount's 15s / OpShutdown's 60s; a hung consumer's private
// file remains the durable source of truth.
const drainGrace = 6 * time.Second

func (s *Server) drain(dir string) {
	if d, ok := s.Host.(Drainer); ok {
		d.Drain(dir, drainGrace)
	}
}

// setupAndRegister mounts spec and records its registry row under a bumped
// epoch. The caller holds dir's in-flight claim.
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
		// Fail closed: an unanswered probe (a wedged carcass) reads
		// still-mounted, routing into the bounded forced teardown — never an
		// OK no-op for a possibly-live mountpoint, never a hung handler.
		if st, ok := probeMount(s.Host.State, req.Base, req.Dir); ok && !st.mounted {
			return Response{OK: true} // not mounted at all: no-op
		}
		// A carcass (rowless mountpoint). Teardown needs base only for its
		// base==dir refusal, so the request's Base serves.
		base = req.Base
	}
	s.drain(req.Dir)
	err := s.Host.Teardown(base, req.Dir)
	// Drop the row regardless of outcome: the provider dropped its handle, so
	// the row would be a lie; wedge honesty comes from the error.
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
	// Live semantics are MountInfo's. The probes are stat-side I/O the
	// registry lock must not span, and any one can wedge with its mirror, so
	// entries are probed in parallel, each bounded: a wedged mirror reads
	// Live=false while healthy siblings still answer within the deadline.
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
			mounts[i].Live = s.liveWithin(row.Base, dir)
		}()
	}
	wg.Wait()
	return Response{OK: true, Mounts: mounts}
}

// handleShutdown sweeps every owned mount, replies with the dirs that failed
// to come down, then cancels Run's context — that closes the listener, never
// this live connection, so the reply still lands.
func (s *Server) handleShutdown() Response {
	if owners := s.distinctOwners(); len(owners) > 1 {
		return Response{OK: false, Error: fmt.Sprintf("shutdown refused: holder serves %d owners %v; reclaim per-owner instead", len(owners), owners)}
	}
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

// unmountAll sweeps every mount; unmountOwned sweeps one owner's. sweep
// claims each dir (a busy dir is reported failed, not raced) and returns the
// dirs still mounted.
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
