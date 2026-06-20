package mountd

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// ErrUnreachable means the holder socket could not be dialed, so it has no
// peer. It is errors.Is-stable: callers that distinguish "no holder there"
// from "the holder refused the kill" match on it.
var ErrUnreachable = errors.New("socket unreachable")

// peerPIDFn and killProc are overridable in tests so the kill path can be
// asserted without dialing a real socket or signalling a real process.
var (
	peerPIDFn = peerPID
	killProc  = syscall.Kill
)

// PeerPID reads the pid of the process on the other end of the holder socket
// via getsockopt(LOCAL_PEERPID). ErrUnreachable if the socket cannot be dialed.
// Darwin-only: off darwin it reports an unsupported error.
func (c *Client) PeerPID() (int, error) {
	return peerPIDFn(c.Socket)
}

// PeerAlive reports whether the holder socket currently has a live peer — a
// cheap liveness check that never signals anything. A dial failure (no peer)
// reads false.
func (c *Client) PeerAlive() bool {
	_, err := peerPIDFn(c.Socket)
	return err == nil
}

// Kill force-terminates the process currently holding the holder socket,
// identified by its peer credentials (LOCAL_PEERPID) — never by name, so it
// can only target the exact process on the other end of this socket. It never
// signals pid<=1 or the caller's own process. Returns the killed pid (0 if the
// peer is gone or is us) and any error other than ESRCH (already dead), or
// ErrUnreachable when the socket has no peer at all.
func (c *Client) Kill() (int, error) {
	pid, err := peerPIDFn(c.Socket)
	if err != nil {
		return 0, err
	}
	return killResolved(pid)
}

// KillPeer is Kill gated on peer identity: the socket's current peer is
// resolved and compared against wantPID in one step, and the signal — when it
// fires — lands on that same resolved pid, never on a re-resolved one. A
// separate check-then-Kill would re-dial inside Kill and SIGKILL whoever holds
// the socket at kill time, so a successor that bound the socket between the
// check and the kill could be shot; here a mismatched peer is refused with no
// signal sent. Returns the killed pid (0 when nothing was signalled) and
// ErrUnreachable when the socket has no peer at all.
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

// killResolved SIGKILLs an already-resolved peer pid, sparing pid<=1 and the
// caller's own process; ESRCH (already dead) is success.
func killResolved(pid int) (int, error) {
	if pid <= 1 || pid == os.Getpid() {
		return 0, nil
	}
	if err := killProc(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return pid, fmt.Errorf("kill socket peer pid %d: %w", pid, err)
	}
	return pid, nil
}
