package proc

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// refuse is an Evict that never makes way: a live peer answered and is refused.
func refuse(err error) func() (bool, error) {
	return func() (bool, error) { return false, err }
}

// noPeer is an Evict that found no live peer: (false, nil).
func noPeer() (bool, error) { return false, nil }

// makeWay is an Evict that evicted a live peer: (true, nil).
func makeWay() (bool, error) { return true, nil }

// dial reports whether socket accepts a connection.
func dial(socket string) bool {
	conn, err := net.DialTimeout("unix", socket, 200*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// TestSingleEntrantBindsWhenFree: a free lock binds, the socket accepts
// connections, and Evict runs exactly once (as defense in depth) reporting no
// peer.
func TestSingleEntrantBindsWhenFree(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	var evicts atomic.Int32
	se := SingleEntrant{
		Socket: socket,
		Evict: func() (bool, error) {
			evicts.Add(1)
			return false, nil
		},
	}
	ln, lock, err := se.Listen()
	if err != nil {
		t.Fatalf("Listen on a free lock = %v, want a bound listener", err)
	}
	defer lock.Close()
	defer ln.Close()

	if !dial(socket) {
		t.Fatal("socket did not accept a connection after binding")
	}
	if got := evicts.Load(); got != 1 {
		t.Errorf("Evict ran %d times, want exactly 1 (defense in depth)", got)
	}
	// Perms are 0600.
	fi, err := os.Stat(socket)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("socket perms = %o, want 0600", got)
	}
}

// TestSingleEntrantRefusesLivePeer: the lock is held by a live peer that Evict
// refuses (false, err) — Listen returns that exact err and binds nothing.
func TestSingleEntrantRefusesLivePeer(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	// Contend the lock from another fd in this process (flock contends between
	// open file descriptions), standing in for a peer that won the lock.
	held, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}

	refusal := errors.New("a peer already serves this socket; refusing to start")
	_, _, err = SingleEntrant{Socket: socket, Evict: refuse(refusal)}.Listen()
	if !errors.Is(err, refusal) {
		t.Fatalf("Listen against a refused live peer = %v, want the Evict refusal", err)
	}
	// The loser must not have created the socket: its os.Remove on a believed
	// stale socket is exactly the hazard the lock prevents.
	if _, statErr := os.Stat(socket); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("a refused bind must not create the socket; stat err = %v", statErr)
	}
}

// TestSingleEntrantRefusesStartingPeer: the lock is contended but Evict found no
// live peer (it is mid-start) — Listen refuses with ErrPeerStarting and binds
// nothing.
func TestSingleEntrantRefusesStartingPeer(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	held, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}

	_, _, err = SingleEntrant{Socket: socket, Evict: noPeer}.Listen()
	if !errors.Is(err, ErrPeerStarting) {
		t.Fatalf("Listen against a contended starting peer = %v, want ErrPeerStarting", err)
	}
	if _, statErr := os.Stat(socket); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("a refused bind must not create the socket; stat err = %v", statErr)
	}
}

// TestSingleEntrantEvictsAndReacquires: the lock is contended but Evict makes
// way (true, nil) and releases the lock shortly after — Listen polls the lock,
// reacquires it, and binds.
func TestSingleEntrantEvictsAndReacquires(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	held, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	// Evict "makes way": release the contending lock from a goroutine after a
	// short delay, modeling the evictee's process exit releasing its flock.
	released := make(chan struct{})
	evict := func() (bool, error) {
		go func() {
			time.Sleep(150 * time.Millisecond)
			held.Close() // releasing the flock
			close(released)
		}()
		return true, nil
	}

	ln, lock, err := SingleEntrant{Socket: socket, Evict: evict, Timeout: 5 * time.Second}.Listen()
	if err != nil {
		t.Fatalf("Listen after eviction = %v, want a reacquired bind", err)
	}
	defer lock.Close()
	defer ln.Close()
	<-released
	if !dial(socket) {
		t.Fatal("socket did not accept a connection after reacquiring the lock")
	}
}

// TestSingleEntrantEvictTimesOut: Evict makes way but the lock is never released
// — Listen polls up to Timeout then refuses with ErrLockStillHeld.
func TestSingleEntrantEvictTimesOut(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	held, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}

	_, _, err = SingleEntrant{Socket: socket, Evict: makeWay, Timeout: 200 * time.Millisecond}.Listen()
	if !errors.Is(err, ErrLockStillHeld) {
		t.Fatalf("Listen with an unreleased lock = %v, want ErrLockStillHeld", err)
	}
}

// TestSingleEntrantNeverUnlinksLock: a refused bind, a successful bind, and the
// listener+lock close must all leave the lock file in place — unlinking a held
// lock would reopen the start race.
func TestSingleEntrantNeverUnlinksLock(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	lockPath := socket + ".lock"

	// A successful bind, then full teardown.
	ln, lock, err := SingleEntrant{Socket: socket, Evict: noPeer}.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock file missing while bound: %v", err)
	}
	ln.Close()
	lock.Close()
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock file unlinked after teardown: %v (the lock must never be removed)", err)
	}

	// A refused bind against a held lock must also leave the lock file.
	held, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if _, _, err := (SingleEntrant{Socket: socket, Evict: refuse(errors.New("busy"))}).Listen(); err == nil {
		t.Fatal("Listen against a held lock succeeded, want refusal")
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock file unlinked by a refused bind: %v", err)
	}
}

// TestSingleEntrantReclaimsStaleSocket: a stale socket file with no live
// listener behind it (free lock) is removed and rebound.
func TestSingleEntrantReclaimsStaleSocket(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	// Manufacture a stale socket: bind, keep the file on close, close.
	stale, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socket); err != nil {
		t.Fatalf("precondition: stale socket should remain after close: %v", err)
	}

	ln, lock, err := SingleEntrant{Socket: socket, Evict: noPeer}.Listen()
	if err != nil {
		t.Fatalf("Listen over a stale socket = %v, want a rebound listener", err)
	}
	defer lock.Close()
	defer ln.Close()
	if !dial(socket) {
		t.Fatal("socket did not accept a connection after reclaiming the stale file")
	}
}
