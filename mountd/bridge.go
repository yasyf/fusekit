package mountd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/fusekit/content"
)

// bridgeState is a hosted bridge's lifecycle phase, surfaced in BridgeInfo.State.
type bridgeState string

const (
	bridgeStarting       bridgeState = "starting"
	bridgeServing        bridgeState = "serving"
	bridgeConsentPending bridgeState = "consent-pending"
	bridgeBindFailed     bridgeState = "bind-failed"
)

// Bridge bind-retry ladder and teardown grants. Vars, not consts, so tests
// shrink them off multi-second waits. The stop grant outlasts the drain grant
// so stopBridge reliably waits the runner's bounded drain out — a same-owner
// re-add must never race the old runner on the shared spool dir — while staying
// well under OpReclaim's server deadline.
var (
	bridgeBindBackoff    = 5 * time.Second
	bridgeBindMaxBackoff = 2 * time.Minute
	bridgeBindConfirm    = 2 * time.Second
	bridgeBindPoll       = 20 * time.Millisecond
	bridgeDrainGrace     = 2 * time.Second
	bridgeStopGrace      = 3 * time.Second
)

// bridgeRow is one hosted content bridge, kept in the Server's bridges map —
// SEPARATE from the mount registry, so no mount sweep, converge, or carcass path
// can ever see it. One bridge per owner.
type bridgeRow struct {
	owner    string
	bindSock string
	relay    *content.RelaySource
	server   *content.BridgeServer

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	// stopping is set true while reclaimBridge tears this row down; it is guarded
	// by Server.bridgeMu and keeps the row visible so a same-owner add refuses
	// (ClassBusy) instead of racing a second live relay onto the same spool dir.
	stopping bool

	stateMu sync.Mutex
	state   bridgeState
	lastErr string
	// upstream is guarded by stateMu (adopt writes it, bridgeInfos reads it).
	upstream string
}

func (row *bridgeRow) setState(st bridgeState, lastErr string) {
	row.stateMu.Lock()
	row.state, row.lastErr = st, lastErr
	row.stateMu.Unlock()
}

func (row *bridgeRow) snapshotState() (bridgeState, string) {
	row.stateMu.Lock()
	defer row.stateMu.Unlock()
	return row.state, row.lastErr
}

func (row *bridgeRow) setUpstream(u string) {
	row.stateMu.Lock()
	row.upstream = u
	row.stateMu.Unlock()
}

func (row *bridgeRow) getUpstream() string {
	row.stateMu.Lock()
	defer row.stateMu.Unlock()
	return row.upstream
}

// startBridge launches a row's serve loop and its spool-replay loop, both under
// the row's context and tracked on the Server's wait group so Run drains them on
// shutdown. row.done closes only when BOTH have exited — the row's spool dir is
// not free for a successor while either can still touch it. A var so
// registry-shape tests stub it and drive the map without spawning real socket
// binders.
var startBridge = func(s *Server, row *bridgeRow) {
	var wg sync.WaitGroup
	wg.Add(2)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer wg.Done()
		s.runBridge(row)
	}()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer wg.Done()
		row.relay.Replay(row.ctx)
	}()
	go func() {
		wg.Wait()
		close(row.done)
	}()
}

// validOwner reports whether owner is a safe single path segment. The wire
// Owner names the on-disk spool dir (~/.fusekit/spool/<owner>), so a value like
// "x/../victim" would clean to a DIFFERENT tenant's spool dir while keying a
// distinct bridges-map entry — the foreign-owner refusal would miss it, and the
// second bridge would load and exfiltrate the victim's spooled bytes. Reject
// anything that is not a lone segment.
func validOwner(owner string) bool {
	if owner == "" || owner == "." || owner == ".." {
		return false
	}
	if strings.ContainsRune(owner, '/') || strings.ContainsRune(owner, os.PathSeparator) || strings.ContainsRune(owner, 0) {
		return false
	}
	return filepath.Clean(owner) == owner && filepath.Base(owner) == owner
}

// sameOwnerVerdict applies the same-owner AddBridge rules to an existing row,
// called under s.bridgeMu. It refuses while the bridge is stopping (retryable
// ClassBusy — a reclaim holds the spool dir), refuses a changed bind socket
// (non-retryable ClassBridgeSocketChanged — the appex would dial a socket the
// holder never bound; the consumer must RemoveBridge first), and otherwise
// adopts in place. It never calls bridgeInfos (which would re-lock bridgeMu).
func sameOwnerVerdict(row *bridgeRow, req Request) (adopted bool, refusal Response) {
	if row.stopping {
		return false, Response{OK: false, ErrClass: ClassBusy, Error: fmt.Sprintf("addbridge: owner %q bridge is stopping; retry", req.Owner)}
	}
	if row.bindSock != req.BridgeSocket {
		return false, Response{OK: false, ErrClass: ClassBridgeSocketChanged, Error: fmt.Sprintf("addbridge: owner %q is bound to %s; RemoveBridge before rebinding to %s", req.Owner, row.bindSock, req.BridgeSocket)}
	}
	row.setUpstream(req.ContentSocket)
	row.relay.Adopt(req.ContentSocket, req.PrivatePrefixes)
	return true, Response{}
}

func (s *Server) handleAddBridge(req Request) Response {
	if !validOwner(req.Owner) {
		return Response{OK: false, ErrClass: ClassInvalidOwner, Error: fmt.Sprintf("addbridge: owner %q must be a safe single path segment", req.Owner)}
	}
	if req.BridgeSocket == "" || !filepath.IsAbs(req.BridgeSocket) {
		return Response{OK: false, Error: "addbridge: bridge_socket must be an absolute path"}
	}
	if req.ContentSocket == "" {
		return Response{OK: false, Error: "addbridge: content_socket is required"}
	}

	s.bridgeMu.Lock()
	// A different owner colliding on the same bind socket is refused: the holder
	// never stacks a second binder on one socket (the bridge analog of
	// ClassForeignMount).
	for _, row := range s.bridges {
		if row.owner != req.Owner && row.bindSock == req.BridgeSocket {
			s.bridgeMu.Unlock()
			return Response{OK: false, ErrClass: ClassForeignBridge, Error: fmt.Sprintf("addbridge: %s is already bound by owner %q", req.BridgeSocket, row.owner)}
		}
	}
	// Same owner: adopt in place (keeping caches and spool warm) — but only for an
	// identical bind socket, and never while a reclaim is stopping it.
	if row, ok := s.bridges[req.Owner]; ok {
		adopted, refusal := sameOwnerVerdict(row, req)
		if adopted {
			s.stageBridge(req)
		}
		s.bridgeMu.Unlock()
		if !adopted {
			return refusal
		}
		resp := okWithWarning(s.flushJournal())
		resp.Bridges = s.bridgeInfos(req.Owner)
		return resp
	}
	s.bridgeMu.Unlock()

	relay, err := content.NewRelaySource(content.RelayConfig{
		Owner:           req.Owner,
		SpoolDir:        holderSpoolDir(req.Owner),
		Upstream:        req.ContentSocket,
		PrivatePrefixes: req.PrivatePrefixes,
		Log:             s.Log,
	})
	if err != nil {
		return Response{OK: false, Error: fmt.Sprintf("addbridge: %v", err)}
	}
	ctx, cancel := context.WithCancel(s.bridgeCtx)
	row := &bridgeRow{
		owner:    req.Owner,
		bindSock: req.BridgeSocket,
		relay:    relay,
		server:   &content.BridgeServer{Socket: req.BridgeSocket, Source: relay, Version: s.Version, Log: s.Log},
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		state:    bridgeStarting,
		upstream: req.ContentSocket,
	}

	s.bridgeMu.Lock()
	// Re-check under the lock: a concurrent add for the same owner or a colliding
	// socket may have landed while the relay was constructed. Discard the freshly
	// built (unstarted) relay in either case.
	if existing, ok := s.bridges[req.Owner]; ok {
		adopted, refusal := sameOwnerVerdict(existing, req)
		if adopted {
			s.stageBridge(req)
		}
		s.bridgeMu.Unlock()
		cancel()
		if !adopted {
			return refusal
		}
		resp := okWithWarning(s.flushJournal())
		resp.Bridges = s.bridgeInfos(req.Owner)
		return resp
	}
	for _, other := range s.bridges {
		if other.bindSock == req.BridgeSocket {
			s.bridgeMu.Unlock()
			cancel()
			return Response{OK: false, ErrClass: ClassForeignBridge, Error: fmt.Sprintf("addbridge: %s is already bound by owner %q", req.BridgeSocket, other.owner)}
		}
	}
	s.bridges[req.Owner] = row
	s.stageBridge(req)
	s.bridgeMu.Unlock()
	warn := s.flushJournal()

	startBridge(s, row)
	resp := okWithWarning(warn)
	resp.Bridges = s.bridgeInfos(req.Owner)
	return resp
}

// handleRemoveBridge tears down req.Owner's own bridge only. The cross-owner
// refusal is a misfire guard, not a security boundary (Owner is
// client-asserted over a same-UID socket): the registry keys bridges by
// owner, so the refusal pins the invariant that a remove can never reach
// another owner's row even if the keying ever drifts.
func (s *Server) handleRemoveBridge(req Request) Response {
	if !validOwner(req.Owner) {
		return Response{OK: false, ErrClass: ClassInvalidOwner, Error: fmt.Sprintf("removebridge: owner %q must be a safe single path segment", req.Owner)}
	}
	s.bridgeMu.Lock()
	row, ok := s.bridges[req.Owner]
	s.bridgeMu.Unlock()
	if ok && row.owner != req.Owner {
		return Response{OK: false, ErrClass: ClassOwnerMismatch, Error: fmt.Sprintf("removebridge: bridge is owned by %q, not %q", row.owner, req.Owner)}
	}
	resp := okWithWarning(s.reclaimBridge(req.Owner))
	resp.Bridges = s.bridgeInfos(req.Owner)
	return resp
}

func (s *Server) handleBridges(req Request) Response {
	owner := req.Owner
	if req.All {
		owner = "" // read-only cross-tenant view
	}
	return Response{OK: true, Bridges: s.bridgeInfos(owner)}
}

// reclaimBridge stops and drains the owner's bridge, then drops its registry
// row. It is Reclaim's and RemoveBridge's per-owner teardown; the durable spool
// survives on disk for a successor. The returned error is a journal
// persist-warning, never a failure.
//
// The row stays in the map — marked stopping — across the stop, so a same-owner
// AddBridge racing in refuses (ClassBusy) instead of constructing a second live
// RelaySource over the same spool dir while this one is still draining. The
// invariant: at most one live relay per spool dir, ever. The row is removed
// only once BOTH the runner and the replay goroutine have exited (row.done);
// a stop that outlives the grace PARKS — the row stays stopping, a detached
// watcher finishes the removal when they finally exit — rather than freeing
// the owner over a still-live relay.
func (s *Server) reclaimBridge(owner string) error {
	s.bridgeMu.Lock()
	row, ok := s.bridges[owner]
	if !ok || row.stopping {
		s.bridgeMu.Unlock()
		return nil // absent, or another reclaim already owns the stop
	}
	row.stopping = true
	s.bridgeMu.Unlock()

	if !s.stopBridge(row) {
		s.Log.Printf("bridge %s: parking its removal until the runner and replay exit (the spool stays owned)", owner)
		go func() {
			<-row.done
			if err := s.finishBridgeReclaim(owner, row); err != nil {
				s.Log.Printf("bridge %s: parked removal journal flush: %v", owner, err)
			}
			s.Log.Printf("bridge %s: parked removal complete", owner)
		}()
		return nil
	}
	return s.finishBridgeReclaim(owner, row)
}

// finishBridgeReclaim drops the fully-stopped row and persists the removal.
func (s *Server) finishBridgeReclaim(owner string, row *bridgeRow) error {
	s.bridgeMu.Lock()
	removed := false
	if cur, ok := s.bridges[owner]; ok && cur == row {
		delete(s.bridges, owner)
		s.stageBridgeGone(owner)
		removed = true
	}
	s.bridgeMu.Unlock()
	if !removed {
		return nil
	}
	return s.flushJournal()
}

// stopBridge cancels a bridge's context and waits (bounded) for its runner
// AND replay goroutine to exit; the runner attempts a bounded spool drain on
// the way out. false means they outlived the grace.
func (s *Server) stopBridge(row *bridgeRow) bool {
	row.cancel()
	select {
	case <-row.done:
		return true
	case <-time.After(bridgeStopGrace):
		s.Log.Printf("bridge %s: runner/replay did not stop within %s", row.owner, bridgeStopGrace)
		return false
	}
}

// bridgeInfos snapshots the bridges (scoped to owner; empty owner = all). It
// reads each relay's pending count outside the bridge lock — never held across a
// relay call.
func (s *Server) bridgeInfos(owner string) []BridgeInfo {
	s.bridgeMu.Lock()
	rows := make([]*bridgeRow, 0, len(s.bridges))
	for _, row := range s.bridges {
		if owner != "" && row.owner != owner {
			continue
		}
		rows = append(rows, row)
	}
	s.bridgeMu.Unlock()

	infos := make([]BridgeInfo, 0, len(rows))
	for _, row := range rows {
		st, lastErr := row.snapshotState()
		infos = append(infos, BridgeInfo{
			Owner:         row.owner,
			Socket:        row.bindSock,
			State:         string(st),
			PendingWrites: row.relay.PendingWrites(),
			Upstream:      row.getUpstream(),
			LastErr:       lastErr,
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Owner < infos[j].Owner })
	return infos
}

// runBridge serves a bridge's content.BridgeServer with capped-backoff retry,
// porting the daemon's serveFPBridge semantics: the socket-dir MkdirAll is
// inside the loop so a retry picks up a late group-container approval, an
// os.ErrPermission bind parks the bridge as consent-pending, and any other bind
// error is bind-failed. On exit (its context cancelled) it makes one bounded
// best-effort spool drain; the socket dies with the process, the spool survives.
func (s *Server) runBridge(row *bridgeRow) {
	defer s.drainBridge(row)

	backoff := bridgeBindBackoff
	cl := content.NewBridgeClient(row.bindSock)
	for {
		if row.ctx.Err() != nil {
			return
		}
		row.setState(bridgeStarting, "")
		err := os.MkdirAll(filepath.Dir(row.bindSock), 0o700)
		if err == nil {
			err = s.serveBridgeOnce(row, cl)
		}
		if err == nil || row.ctx.Err() != nil {
			return
		}
		if errors.Is(err, os.ErrPermission) {
			row.setState(bridgeConsentPending, err.Error())
			s.Log.Printf("bridge %s: bind %s parked on the group-container consent; retrying in %s: %v", row.owner, row.bindSock, backoff, err)
		} else {
			row.setState(bridgeBindFailed, err.Error())
			s.Log.Printf("bridge %s: serve %s failed; retrying in %s: %v", row.owner, row.bindSock, backoff, err)
		}
		select {
		case <-row.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = minBackoff(backoff*2, bridgeBindMaxBackoff)
	}
}

// serveBridgeOnce runs the bridge server until its context is cancelled or the
// bind fails. It confirms the socket came up before declaring serving, so a fast
// bind failure is classified from the returned error rather than mislabeled.
func (s *Server) serveBridgeOnce(row *bridgeRow, cl *content.BridgeClient) error {
	errc := make(chan error, 1)
	go func() { errc <- row.server.Run(row.ctx) }()

	deadline := time.Now().Add(bridgeBindConfirm)
	for time.Now().Before(deadline) {
		select {
		case err := <-errc:
			return err // Run returned before coming up: a bind failure (or an immediate cancel)
		default:
		}
		if row.ctx.Err() != nil {
			return <-errc
		}
		if cl.Available() {
			row.setState(bridgeServing, "")
			return <-errc
		}
		time.Sleep(bridgeBindPoll)
	}
	// Bound but Available never confirmed within the window; treat it as serving
	// and let a later failure reclassify.
	row.setState(bridgeServing, "")
	return <-errc
}

func (s *Server) drainBridge(row *bridgeRow) {
	ctx, cancel := context.WithTimeout(context.Background(), bridgeDrainGrace)
	defer cancel()
	row.relay.Drain(ctx)
}

// holderSpoolRoot is the base of every bridge owner's durable write spool,
// ~/.fusekit/spool. A var so tests redirect it off the real home; a missing home
// falls back to the process cwd so the spool still has a home.
var holderSpoolRoot = func() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	return filepath.Join(home, ".fusekit", "spool")
}()

// holderSpoolDir is a bridge owner's durable write-spool directory,
// holderSpoolRoot/<owner>. The owner is a stable identifier.
func holderSpoolDir(owner string) string {
	return filepath.Join(holderSpoolRoot, owner)
}

func minBackoff(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
