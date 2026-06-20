package mountd

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

// killCall records one killProc invocation so tests can assert the signal and
// target without ever signalling a real process.
type killCall struct {
	pid int
	sig syscall.Signal
}

// setPeerSeams overrides peerPIDFn and killProc for the duration of a test,
// restoring both afterward so no test ever signals a real process.
func setPeerSeams(t *testing.T, lp func(string) (int, error), kp func(int, syscall.Signal) error) {
	t.Helper()
	oldLP, oldKP := peerPIDFn, killProc
	peerPIDFn, killProc = lp, kp
	t.Cleanup(func() { peerPIDFn, killProc = oldLP, oldKP })
}

// TestKillSpares pins that we never signal our own process, pid 0, or pid 1 —
// the peer PID is read from peer credentials, but a bug there must never turn
// into a self-kill or an init-kill.
func TestKillSpares(t *testing.T) {
	for _, tc := range []struct {
		name string
		pid  int
	}{
		{"self", os.Getpid()},
		{"pid0", 0},
		{"pid1", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			setPeerSeams(t,
				func(string) (int, error) { return tc.pid, nil },
				func(int, syscall.Signal) error { called = true; return nil })
			pid, err := (&Client{Socket: "unused.sock"}).Kill()
			if pid != 0 || err != nil {
				t.Fatalf("Kill(%s) = (%d, %v), want (0, nil)", tc.name, pid, err)
			}
			if called {
				t.Fatalf("killProc was called for spared pid %d", tc.pid)
			}
		})
	}
}

// TestKillSignals pins the signal sent and the error handling: a live peer
// gets a SIGKILL, ESRCH (already dead) is success, EPERM surfaces.
func TestKillSignals(t *testing.T) {
	for _, tc := range []struct {
		name    string
		killErr error
		wantErr bool
	}{
		{"alive", nil, false},
		{"already-dead-ESRCH", syscall.ESRCH, false},
		{"no-perm-EPERM", syscall.EPERM, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got killCall
			setPeerSeams(t,
				func(string) (int, error) { return 999001, nil },
				func(pid int, sig syscall.Signal) error { got = killCall{pid, sig}; return tc.killErr })
			pid, err := (&Client{Socket: "unused.sock"}).Kill()
			if pid != 999001 {
				t.Fatalf("killed pid = %d, want 999001", pid)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if got.pid != 999001 || got.sig != syscall.SIGKILL {
				t.Fatalf("kill call = %+v, want SIGKILL to 999001", got)
			}
		})
	}
}

// TestKillLookupError pins that an unreachable socket is reported, not turned
// into a kill of pid 0.
func TestKillLookupError(t *testing.T) {
	called := false
	setPeerSeams(t,
		func(string) (int, error) { return 0, ErrUnreachable },
		func(int, syscall.Signal) error { called = true; return nil })
	pid, err := (&Client{Socket: "unused.sock"}).Kill()
	if pid != 0 || !errors.Is(err, ErrUnreachable) {
		t.Fatalf("Kill = (%d, %v), want (0, ErrUnreachable)", pid, err)
	}
	if called {
		t.Fatal("killProc was called despite a lookup error")
	}
}

// TestKillPeerMatch pins that a peer equal to wantPID is signalled.
func TestKillPeerMatch(t *testing.T) {
	var got killCall
	setPeerSeams(t,
		func(string) (int, error) { return 999002, nil },
		func(pid int, sig syscall.Signal) error { got = killCall{pid, sig}; return nil })
	pid, err := (&Client{Socket: "unused.sock"}).KillPeer(999002)
	if pid != 999002 || err != nil {
		t.Fatalf("KillPeer(match) = (%d, %v), want (999002, nil)", pid, err)
	}
	if got.pid != 999002 || got.sig != syscall.SIGKILL {
		t.Fatalf("kill call = %+v, want SIGKILL to 999002", got)
	}
}

// TestKillPeerMismatch pins that a peer that no longer matches wantPID — a
// successor that bound the socket between gate time and now — is refused with
// no signal sent.
func TestKillPeerMismatch(t *testing.T) {
	called := false
	setPeerSeams(t,
		func(string) (int, error) { return 999003, nil },
		func(int, syscall.Signal) error { called = true; return nil })
	pid, err := (&Client{Socket: "unused.sock"}).KillPeer(999002)
	if pid != 0 || err == nil {
		t.Fatalf("KillPeer(mismatch) = (%d, %v), want (0, refusal err)", pid, err)
	}
	if called {
		t.Fatal("killProc was called for a mismatched peer")
	}
}

// TestKillPeerUnreachable pins that an unreachable socket is reported, never a
// kill of pid 0.
func TestKillPeerUnreachable(t *testing.T) {
	called := false
	setPeerSeams(t,
		func(string) (int, error) { return 0, ErrUnreachable },
		func(int, syscall.Signal) error { called = true; return nil })
	pid, err := (&Client{Socket: "unused.sock"}).KillPeer(999002)
	if pid != 0 || !errors.Is(err, ErrUnreachable) {
		t.Fatalf("KillPeer = (%d, %v), want (0, ErrUnreachable)", pid, err)
	}
	if called {
		t.Fatal("killProc was called despite a lookup error")
	}
}

// TestPeerAlive pins the liveness check: a resolvable peer reads alive, an
// unreachable socket reads not-alive — and it never signals.
func TestPeerAlive(t *testing.T) {
	noKill := func(int, syscall.Signal) error { return nil }
	setPeerSeams(t, func(string) (int, error) { return 4242, nil }, noKill)
	if !(&Client{Socket: "unused.sock"}).PeerAlive() {
		t.Fatal("PeerAlive = false for a resolvable peer")
	}
	setPeerSeams(t, func(string) (int, error) { return 0, ErrUnreachable }, noKill)
	if (&Client{Socket: "unused.sock"}).PeerAlive() {
		t.Fatal("PeerAlive = true for an unreachable socket")
	}
}
