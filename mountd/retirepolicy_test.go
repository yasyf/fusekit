package mountd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"

	"github.com/yasyf/fusekit/proc"
)

// TestRetirePolicyKillRefusesWithoutCapture: a capture error (no gate-time pid)
// refuses the kill BEFORE the peer seam — no signal, no peer resolve.
func TestRetirePolicyKillRefusesWithoutCapture(t *testing.T) {
	setPeerSeams(t,
		func(string) (int, error) { t.Fatal("peerPIDFn called despite an uncaptured pid"); return 0, nil },
		func(int, syscall.Signal) error { t.Fatal("killProc called despite an uncaptured pid"); return nil })

	a := &RetirePolicy{Client: NewClient("unused.sock")}
	a.SetCapturedPID(0, errors.New("pid unresolved at gate time"))
	pid, err := a.Kill()
	if pid != 0 {
		t.Errorf("Kill pid = %d, want 0 (refused)", pid)
	}
	if err == nil || !strings.Contains(err.Error(), "not captured at gate time") {
		t.Errorf("Kill err = %v, want a 'not captured at gate time' refusal", err)
	}
}

// TestRetirePolicyKillMapsUnreachable: a vanished peer (ErrUnreachable from
// KillPeer) maps to proc.ErrChildUnavailable so proc's reapWedged reads "nothing
// to kill, socket free."
func TestRetirePolicyKillMapsUnreachable(t *testing.T) {
	setPeerSeams(t,
		func(string) (int, error) { return 0, ErrUnreachable },
		func(int, syscall.Signal) error { t.Fatal("killProc called for an unreachable peer"); return nil })

	a := &RetirePolicy{Client: NewClient("unused.sock")}
	a.SetCapturedPID(4242, nil)
	pid, err := a.Kill()
	if pid != 0 {
		t.Errorf("Kill pid = %d, want 0", pid)
	}
	if !errors.Is(err, proc.ErrChildUnavailable) {
		t.Errorf("Kill err = %v, want errors.Is proc.ErrChildUnavailable", err)
	}
	// The underlying ErrUnreachable stays in the chain for context.
	if !errors.Is(err, ErrUnreachable) {
		t.Errorf("Kill err = %v, want the underlying ErrUnreachable preserved", err)
	}
}

// TestRetirePolicyKillSignalsCapturedPID: with a captured pid and a matching peer,
// Kill signals ONLY that pid.
func TestRetirePolicyKillSignalsCapturedPID(t *testing.T) {
	const capturedPID = 5566
	var killed killCall
	setPeerSeams(t,
		func(string) (int, error) { return capturedPID, nil },
		func(pid int, sig syscall.Signal) error { killed = killCall{pid, sig}; return nil })

	a := &RetirePolicy{Client: NewClient("unused.sock")}
	a.SetCapturedPID(capturedPID, nil)
	pid, err := a.Kill()
	if err != nil {
		t.Fatalf("Kill = %v, want nil", err)
	}
	if pid != capturedPID {
		t.Errorf("Kill pid = %d, want the captured pid %d", pid, capturedPID)
	}
	if killed.pid != capturedPID || killed.sig != syscall.SIGKILL {
		t.Errorf("kill = %+v, want SIGKILL to the captured pid %d", killed, capturedPID)
	}
}

// TestRetirePolicyKillRefusesMismatchedPeer: a successor that rebound the socket
// between gate time and now (peer != capturedPID) is refused with no signal — the
// peer-gated guarantee carried through the adapter.
func TestRetirePolicyKillRefusesMismatchedPeer(t *testing.T) {
	const capturedPID = 6001
	const successorPID = 6002
	called := false
	setPeerSeams(t,
		func(string) (int, error) { return successorPID, nil },
		func(int, syscall.Signal) error { called = true; return nil })

	a := &RetirePolicy{Client: NewClient("unused.sock")}
	a.SetCapturedPID(capturedPID, nil)
	pid, err := a.Kill()
	if pid != 0 || err == nil {
		t.Fatalf("Kill against a mismatched peer = (%d, %v), want (0, refusal)", pid, err)
	}
	if called {
		t.Fatal("killProc signalled a mismatched successor; KillPeer must refuse it")
	}
}

// TestRetirePolicyKillUsesSeamOverride: a consumer that wires KillPeer routes the
// reap through its own seam (the package-level peerPIDFn/killProc are unreachable
// across packages), and the ErrUnreachable->ErrChildUnavailable mapping still
// applies to the seam's result.
func TestRetirePolicyKillUsesSeamOverride(t *testing.T) {
	setPeerSeams(t,
		func(string) (int, error) {
			t.Fatal("Client.KillPeer's peerPIDFn called despite a KillPeer seam")
			return 0, nil
		},
		func(int, syscall.Signal) error {
			t.Fatal("Client.KillPeer's killProc called despite a KillPeer seam")
			return nil
		})

	t.Run("seam signals the captured pid", func(t *testing.T) {
		const capturedPID = 7788
		var gotWant int
		a := &RetirePolicy{
			Client:   NewClient("unused.sock"),
			KillPeer: func(wantPID int) (int, error) { gotWant = wantPID; return wantPID, nil },
		}
		a.SetCapturedPID(capturedPID, nil)
		pid, err := a.Kill()
		if err != nil || pid != capturedPID {
			t.Fatalf("Kill = (%d, %v), want (%d, nil)", pid, err, capturedPID)
		}
		if gotWant != capturedPID {
			t.Errorf("KillPeer seam got wantPID %d, want the captured pid %d", gotWant, capturedPID)
		}
	})

	t.Run("seam ErrUnreachable maps to ErrChildUnavailable", func(t *testing.T) {
		a := &RetirePolicy{
			Client:   NewClient("unused.sock"),
			KillPeer: func(int) (int, error) { return 0, ErrUnreachable },
		}
		a.SetCapturedPID(4242, nil)
		_, err := a.Kill()
		if !errors.Is(err, proc.ErrChildUnavailable) {
			t.Errorf("Kill err = %v, want errors.Is proc.ErrChildUnavailable", err)
		}
	})
}

// TestRetirePolicyShutdownReturnsRPCErrorAndFiresHook: Shutdown returns the RPC
// error verbatim and fires OnShutdown(failed, err) — proc routes on that error.
func TestRetirePolicyShutdownReturnsRPCErrorAndFiresHook(t *testing.T) {
	// A holder that fails Shutdown with a wedged class: the RPC error must reach
	// proc, and OnShutdown must see it plus the failed-dir set.
	h := newClosableHolder(t, func(req string) string {
		if strings.Contains(req, `"op":"shutdown"`) {
			return fmt.Sprintf(`{"proto":1,"ok":false,"error":"sweep wedged","err_class":%q}`, ClassWedged)
		}
		return shutdownOKReply
	})

	var gotFailed []MountInfo
	var gotErr error
	hookFired := false
	a := &RetirePolicy{
		Client: NewClient(h.socket),
		OnShutdown: func(failed []MountInfo, err error) {
			hookFired = true
			gotFailed, gotErr = failed, err
		},
	}
	err := a.Shutdown(context.Background())
	if err == nil || !errors.Is(err, ErrUnmountWedged) {
		t.Fatalf("Shutdown err = %v, want the RPC's ErrUnmountWedged", err)
	}
	if !hookFired {
		t.Fatal("OnShutdown was not fired")
	}
	if !errors.Is(gotErr, ErrUnmountWedged) {
		t.Errorf("OnShutdown err = %v, want the same RPC error", gotErr)
	}
	if gotFailed != nil {
		t.Errorf("OnShutdown failed = %v, want nil for an errored RPC", gotFailed)
	}
}

// TestRetirePolicyShutdownCleanFiresHookWithNilError: a clean Shutdown returns nil
// and still fires OnShutdown(failed, nil) so the consumer can act on the swept set.
func TestRetirePolicyShutdownCleanFiresHookWithNilError(t *testing.T) {
	h := newClosableHolder(t, func(string) string { return shutdownOKReply })
	hookFired := false
	var gotErr error
	a := &RetirePolicy{
		Client:     NewClient(h.socket),
		OnShutdown: func(_ []MountInfo, err error) { hookFired = true; gotErr = err },
	}
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown of a clean holder = %v, want nil", err)
	}
	if !hookFired || gotErr != nil {
		t.Errorf("OnShutdown fired=%v err=%v, want fired with nil err", hookFired, gotErr)
	}
}

// TestRetirePolicyReconcileRoutes: each proc ReconcileKind routes to its matching
// callback, and a nil callback for the fired kind is a safe no-op.
func TestRetirePolicyReconcileRoutes(t *testing.T) {
	kinds := []struct {
		name string
		kind proc.ReconcileKind
		idx  int
	}{
		{"ChildDied", proc.ChildDied, 0},
		{"Respawned", proc.Respawned, 1},
		{"ReplaceSucceeded", proc.ReplaceSucceeded, 2},
		{"ReplaceAborted", proc.ReplaceAborted, 3},
	}
	for _, tc := range kinds {
		t.Run(tc.name, func(t *testing.T) {
			var hits [4]int
			a := &RetirePolicy{
				Client:             NewClient("unused.sock"),
				OnChildDied:        func(context.Context) { hits[0]++ },
				OnRespawned:        func(context.Context) { hits[1]++ },
				OnReplaceSucceeded: func(context.Context) { hits[2]++ },
				OnReplaceAborted:   func(context.Context) { hits[3]++ },
			}
			a.Reconcile(context.Background(), proc.ReconcileEvent{Kind: tc.kind})
			for i, n := range hits {
				want := 0
				if i == tc.idx {
					want = 1
				}
				if n != want {
					t.Errorf("after %s: callback[%d] fired %d times, want %d", tc.name, i, n, want)
				}
			}
		})
	}
}

// TestRetirePolicyReconcileNilCallbackIsNoOp: a nil callback for the fired kind
// must not panic (every transition routes through a nil-guard).
func TestRetirePolicyReconcileNilCallbackIsNoOp(t *testing.T) {
	a := &RetirePolicy{Client: NewClient("unused.sock")} // all callbacks nil
	for _, kind := range []proc.ReconcileKind{proc.ChildDied, proc.Respawned, proc.ReplaceSucceeded, proc.ReplaceAborted} {
		a.Reconcile(context.Background(), proc.ReconcileEvent{Kind: kind}) // must not panic
	}
}
