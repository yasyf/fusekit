package mountd

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

type killCall struct {
	pid int
	sig syscall.Signal
}

func setPeerSeams(t *testing.T, lp func(string) (int, error), kp func(int, syscall.Signal) error) {
	t.Helper()
	oldLP, oldKP := peerPIDFn, killProc
	peerPIDFn, killProc = lp, kp
	t.Cleanup(func() { peerPIDFn, killProc = oldLP, oldKP })
}

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
