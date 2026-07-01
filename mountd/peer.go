package mountd

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// ErrUnreachable means the holder socket could not be dialed, so it has no
// peer — distinct from the holder refusing a kill.
var ErrUnreachable = errors.New("socket unreachable")

// peerPIDFn and killProc are test seams: assert the kill path without dialing
// a real socket or signalling a real process.
var (
	peerPIDFn = peerPID
	killProc  = syscall.Kill
)

// PeerPID reads the holder socket's peer pid via getsockopt(LOCAL_PEERPID).
// ErrUnreachable if the socket cannot be dialed. Darwin-only: off darwin it
// reports an unsupported error.
func (c *Client) PeerPID() (int, error) {
	return peerPIDFn(c.Socket)
}

// PeerAlive reports whether the holder socket has a live peer without
// signalling anything; a dial failure (no peer) reads false.
func (c *Client) PeerAlive() bool {
	_, err := peerPIDFn(c.Socket)
	return err == nil
}

// Kill force-terminates the socket's current peer, identified by its
// credentials (LOCAL_PEERPID) never by name, so it can only hit the exact
// process on the other end. Never signals pid<=1 or the caller itself. Returns
// the killed pid (0 if the peer is gone or is us) and any error but ESRCH
// (already dead), or ErrUnreachable when the socket has no peer.
func (c *Client) Kill() (int, error) {
	pid, err := peerPIDFn(c.Socket)
	if err != nil {
		return 0, err
	}
	return killResolved(pid)
}

// KillPeer is Kill gated on peer identity: the socket's peer is resolved,
// compared against wantPID, and (on match) signalled in one step, never
// re-resolved. A separate check-then-Kill would re-dial and could SIGKILL a
// successor that rebound the socket between check and kill; here a mismatched
// peer is refused with no signal. Returns the killed pid (0 when nothing was
// signalled) and ErrUnreachable when the socket has no peer.
func (c *Client) KillPeer(wantPID int) (int, error) {
	pid, err := peerPIDFn(c.Socket)
	if err != nil {
		return 0, err
	}
	if pid != wantPID {
		return 0, fmt.Errorf("socket %s is held by pid %d, not pid %d; refusing to kill", c.Socket, pid, wantPID)
	}
	return killResolved(pid)
}

func killResolved(pid int) (int, error) {
	if pid <= 1 || pid == os.Getpid() {
		return 0, nil
	}
	if err := killProc(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return pid, fmt.Errorf("kill socket peer pid %d: %w", pid, err)
	}
	return pid, nil
}
