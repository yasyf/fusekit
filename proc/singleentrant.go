package proc

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const defaultEvictPollTimeout = 30 * time.Second

// SingleEntrant makes the stale-check/remove/bind of a unix socket single-entrant
// across processes via an exclusive flock on Socket+".lock". The caller must hold the
// returned lock for the listener's lifetime, else a losing starter unlinks the winner's
// live socket (net.UnixListener.Close unlinks by path). The lock file is never removed:
// unlinking a held lock would let a third process flock a fresh inode.
type SingleEntrant struct {
	// Socket is the unix socket path to bind.
	Socket string
	// Evict decides what to do about a live peer; consulted at most once per
	// Listen. Required. On a contended lock, evicted=true polls for the
	// evictee's flock and evicted=false refuses with ErrPeerStarting.
	Evict func() (evicted bool, err error)
	// Timeout bounds the post-evict lock poll; zero means a sensible default.
	Timeout time.Duration
}

// Listen binds the socket (0600), consulting Evict on contention, and returns
// the listener and the held lock.
func (se SingleEntrant) Listen() (net.Listener, *os.File, error) {
	if err := os.MkdirAll(filepath.Dir(se.Socket), 0o700); err != nil {
		return nil, nil, fmt.Errorf("ensure socket dir: %w", err)
	}
	lock, err := os.OpenFile(se.Socket+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open socket lock: %w", err)
	}
	contended := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil
	// Evict runs even on a free lock: a live peer may predate the lock discipline.
	evicted, eerr := se.Evict()
	switch {
	case eerr != nil:
		lock.Close()
		return nil, nil, eerr
	case evicted && contended:
		if err := se.pollLock(lock); err != nil {
			lock.Close()
			return nil, nil, err
		}
	case !evicted && contended:
		lock.Close()
		return nil, nil, ErrPeerStarting
	}
	_ = os.Remove(se.Socket) // stale socket: the lock is ours and any live peer was evicted
	ln, err := net.Listen("unix", se.Socket)
	if err != nil {
		lock.Close()
		return nil, nil, fmt.Errorf("listen on %s: %w", se.Socket, err)
	}
	if err := os.Chmod(se.Socket, 0o600); err != nil {
		ln.Close()
		lock.Close()
		return nil, nil, fmt.Errorf("chmod %s: %w", se.Socket, err)
	}
	return ln, lock, nil
}

// pollLock polls rather than failing on the first refusal: the evictee's flock
// is released only at its process exit, which can lag socket death by seconds.
func (se SingleEntrant) pollLock(lock *os.File) error {
	timeout := se.Timeout
	if timeout <= 0 {
		timeout = defaultEvictPollTimeout
	}
	deadline := time.Now().Add(timeout)
	for {
		err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w within %s: %w", ErrLockStillHeld, timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
