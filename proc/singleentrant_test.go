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

func refuse(err error) func() (bool, error) {
	return func() (bool, error) { return false, err }
}

func noPeer() (bool, error) { return false, nil }

func makeWay() (bool, error) { return true, nil }

func dial(socket string) bool {
	conn, err := net.DialTimeout("unix", socket, 200*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

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
	fi, err := os.Stat(socket)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("socket perms = %o, want 0600", got)
	}
}

func TestSingleEntrantRefusesLivePeer(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	// flock contends between open file descriptions: an in-process fd models a peer.
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
	// A loser's os.Remove of a believed-stale socket is the hazard the lock prevents.
	if _, statErr := os.Stat(socket); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("a refused bind must not create the socket; stat err = %v", statErr)
	}
}

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

func TestSingleEntrantEvictsAndReacquires(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	held, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	// Delayed Close models the evictee's process exit releasing its flock.
	released := make(chan struct{})
	evict := func() (bool, error) {
		go func() {
			time.Sleep(150 * time.Millisecond)
			held.Close()
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

func TestSingleEntrantNeverUnlinksLock(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	lockPath := socket + ".lock"

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

func TestSingleEntrantReclaimsStaleSocket(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
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
