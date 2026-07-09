package mountd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
	upstream string
	relay    *content.RelaySource
	server   *content.BridgeServer

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	stateMu sync.Mutex
	state   bridgeState
	lastErr string
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

// startBridge launches a row's serve loop and its spool-replay loop, both under
// the row's context and tracked on the Server's wait group so Run drains them on
// shutdown. A var so registry-shape tests stub it and drive the map without
// spawning real socket binders.
var startBridge = func(s *Server, row *bridgeRow) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runBridge(row)
	}()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		row.relay.Replay(row.ctx)
	}()
}

func (s *Server) handleAddBridge(req Request) Response {
	if req.Owner == "" {
		return Response{OK: false, Error: "addbridge: owner is required"}
	}
	if req.BridgeSocket == "" {
		return Response{OK: false, Error: "addbridge: bridge_socket is required"}
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
	// Same owner: idempotent adopt — re-point the relay's upstream and prefixes
	// in place, keeping its caches and spool warm (the serve-stale win across a
	// consumer daemon restart).
	if row, ok := s.bridges[req.Owner]; ok {
		row.upstream = req.ContentSocket
		row.relay.Adopt(req.ContentSocket, req.PrivatePrefixes)
		s.bridgeMu.Unlock()
		return Response{OK: true, Bridges: s.bridgeInfos(req.Owner)}
	}
	s.bridgeMu.Unlock()

	relay, err := content.NewRelaySource(content.RelayConfig{
		Owner:           req.Owner,
		SpoolDir:        holderSpoolDir(req.Owner),
		Upstream:        req.ContentSocket,
		PrivatePrefixes: req.PrivatePrefixes,
	})
	if err != nil {
		return Response{OK: false, Error: fmt.Sprintf("addbridge: %v", err)}
	}
	ctx, cancel := context.WithCancel(s.bridgeCtx)
	row := &bridgeRow{
		owner:    req.Owner,
		bindSock: req.BridgeSocket,
		upstream: req.ContentSocket,
		relay:    relay,
		server:   &content.BridgeServer{Socket: req.BridgeSocket, Source: relay, Version: s.Version, Log: s.Log},
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		state:    bridgeStarting,
	}

	s.bridgeMu.Lock()
	// Re-check under the lock: a concurrent add for the same owner or a colliding
	// socket may have landed while the relay was constructed.
	if existing, ok := s.bridges[req.Owner]; ok {
		existing.upstream = req.ContentSocket
		existing.relay.Adopt(req.ContentSocket, req.PrivatePrefixes)
		s.bridgeMu.Unlock()
		cancel()
		return Response{OK: true, Bridges: s.bridgeInfos(req.Owner)}
	}
	for _, other := range s.bridges {
		if other.bindSock == req.BridgeSocket {
			s.bridgeMu.Unlock()
			cancel()
			return Response{OK: false, ErrClass: ClassForeignBridge, Error: fmt.Sprintf("addbridge: %s is already bound by owner %q", req.BridgeSocket, other.owner)}
		}
	}
	s.bridges[req.Owner] = row
	s.bridgeMu.Unlock()

	startBridge(s, row)
	return Response{OK: true, Bridges: s.bridgeInfos(req.Owner)}
}

func (s *Server) handleRemoveBridge(req Request) Response {
	if req.Owner == "" {
		return Response{OK: false, Error: "removebridge: owner is required"}
	}
	s.reclaimBridge(req.Owner)
	return Response{OK: true, Bridges: s.bridgeInfos(req.Owner)}
}

func (s *Server) handleBridges(req Request) Response {
	return Response{OK: true, Bridges: s.bridgeInfos(req.Owner)}
}

// reclaimBridge stops and drains the owner's bridge, dropping its registry row.
// It is Reclaim's and RemoveBridge's per-owner teardown; the durable spool
// survives on disk for a successor.
func (s *Server) reclaimBridge(owner string) {
	s.bridgeMu.Lock()
	row, ok := s.bridges[owner]
	if ok {
		delete(s.bridges, owner)
	}
	s.bridgeMu.Unlock()
	if ok {
		s.stopBridge(row)
	}
}

// stopBridge cancels a bridge's context and waits (bounded) for its runner to
// exit; the runner attempts a bounded spool drain on the way out.
func (s *Server) stopBridge(row *bridgeRow) {
	row.cancel()
	select {
	case <-row.done:
	case <-time.After(bridgeStopGrace):
		s.Log.Printf("bridge %s: runner did not stop within %s", row.owner, bridgeStopGrace)
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
			Upstream:      row.upstream,
			LastErr:       lastErr,
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Owner < infos[j].Owner })
	return infos
}

// bridgeOwners returns the distinct owners of hosted bridges, for the Shutdown
// owner accounting (a holder hosting another owner's live bridge refuses a
// cross-owner Shutdown).
func (s *Server) bridgeOwners() []string {
	s.bridgeMu.Lock()
	defer s.bridgeMu.Unlock()
	owners := make([]string, 0, len(s.bridges))
	for owner := range s.bridges {
		if owner != "" {
			owners = append(owners, owner)
		}
	}
	return owners
}

// runBridge serves a bridge's content.BridgeServer with capped-backoff retry,
// porting the daemon's serveFPBridge semantics: the socket-dir MkdirAll is
// inside the loop so a retry picks up a late group-container approval, an
// os.ErrPermission bind parks the bridge as consent-pending, and any other bind
// error is bind-failed. On exit (its context cancelled) it makes one bounded
// best-effort spool drain; the socket dies with the process, the spool survives.
func (s *Server) runBridge(row *bridgeRow) {
	defer close(row.done)
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
