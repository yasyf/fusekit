package proc

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// defaultEvictPollTimeout bounds the post-evict lock-poll loop when
// SingleEntrant.Timeout is zero. A freshly started evictee can spend seconds in
// non-cancellable startup work before its process exit releases the flock.
const defaultEvictPollTimeout = 30 * time.Second

// SingleEntrant binds a unix socket under an exclusive flock, making the
// stale-check / remove / bind sequence single-entrant across processes. The
// flock on Socket+".lock" is returned to the caller, which must hold it for the
// listener's lifetime: it is the cross-process guarantee that only this process
// may stale-check, remove, bind, or unlink the socket path.
//
// Without the lock, two concurrently starting processes both see a dead socket,
// and the loser's os.Remove can unlink the winner's freshly-bound socket; worse,
// *net.UnixListener.Close unlinks by PATH, so the loser would delete the
// winner's live socket again at its own exit. The lock file itself is NEVER
// removed: unlinking a held lock file would let a third process flock a fresh
// inode while the old inode's lock is still held, reopening the race.
//
// The ONLY policy that differs between consumers — what to do when a live peer
// is in the way — is the Evict callback: a holder always refuses; a daemon
// evicts a version-skewed peer then reacquires.
type SingleEntrant struct {
	// Socket is the unix socket path to bind. The lock file is Socket+".lock".
	Socket string
	// Evict decides the contention policy. It owns the wire: it probes the
	// owner's Health and returns one of three verdicts:
	//
	//	(true, nil)   a live peer was evicted (or told to step down) — make way.
	//	              When the lock was contended, Listen polls it up to Timeout
	//	              before binding; when the lock was already ours, Listen binds.
	//	(false, nil)  no live peer answered. A free lock binds; a still-contended
	//	              lock (a peer that holds the flock but is mid-start, not yet
	//	              answering) is refused with ErrPeerStarting.
	//	(false, err)  refuse the bind with err (e.g. a same-version double start).
	//
	// It is consulted at most once per Listen. Required.
	Evict func() (evicted bool, err error)
	// Timeout bounds the post-evict lock-poll loop. Zero means a sensible
	// default.
	Timeout time.Duration
}

// Listen binds the socket with 0600 perms, consulting Evict for the contention
// policy, and returns the listener and the held lock file. The caller must Close
// the lock (which releases the flock) only after the listener is closed. Errors
// are wrapped once with %w.
func (se SingleEntrant) Listen() (net.Listener, *os.File, error) {
	// Only the socket's parent dir is needed; deriving it from Socket keeps
	// tests off any real state dir.
	if err := os.MkdirAll(filepath.Dir(se.Socket), 0o700); err != nil {
		return nil, nil, fmt.Errorf("ensure socket dir: %w", err)
	}
	lock, err := os.OpenFile(se.Socket+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open socket lock: %w", err)
	}
	contended := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil
	// Evict runs once regardless of contention. A contended lock that Evict made
	// way for is polled until the evictee's process exit releases it; a free lock
	// still runs Evict as defense in depth against a live peer predating the lock
	// discipline.
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
		// The lock is held but no peer answered: an owner mid-start (post-flock,
		// pre-bind). Refuse — the caller (e.g. launchd) retries against whatever
		// it becomes — rather than binding over a still-contended lock.
		lock.Close()
		return nil, nil, ErrPeerStarting
	}
	// Free lock (evicted or not) and post-evict-poll cases fall through: the lock
	// is ours.
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

// pollLock re-flocks lock (the same fd) until it is acquired or the post-evict
// deadline elapses. Eviction is observed at socket death, but the evictee's
// flock is released only at its process exit — a freshly started evictee can
// spend seconds in non-cancellable startup work — so the loser polls rather than
// failing on the first refusal.
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
