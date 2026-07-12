//go:build fuse && cgo

package fusekit

import (
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

// TestDefuseCgofuseSignalsUnsubscribesAndReArms pins R5-1: the post-ready
// defuse globally unsubscribes cgofuse's SIGTERM channel (simulated by a
// stand-in Notify'd before the Reset, exactly as hostInit subscribes sigc)
// and synchronously re-arms the embedder's own handler — so a delivered
// SIGTERM reaches ONLY the re-armed channel and can never trigger cgofuse's
// MNT_FORCE goroutine.
func TestDefuseCgofuseSignalsUnsubscribesAndReArms(t *testing.T) {
	standIn := make(chan os.Signal, 1) // cgofuse's host.sigc stand-in
	signal.Notify(standIn, syscall.SIGTERM)
	defer signal.Stop(standIn)

	reArmed := make(chan os.Signal, 1)
	defer signal.Stop(reArmed)
	called := false
	defuseCgofuseSignals(func() {
		called = true
		signal.Notify(reArmed, syscall.SIGTERM)
	})
	if !called {
		t.Fatal("ReArmSignals hook was not invoked synchronously after the Reset")
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("deliver SIGTERM: %v", err)
	}
	select {
	case <-reArmed:
	case <-time.After(2 * time.Second):
		t.Fatal("re-armed handler never received SIGTERM")
	}
	select {
	case sig := <-standIn:
		t.Fatalf("defused stand-in channel still received %v — cgofuse's goroutine would have MNT_FORCEd", sig)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestDefuseCgofuseSignalsNilReArm pins the documented nil contract: no
// re-arm hook means the app owns signal handling; the Reset alone must not
// panic or leave a subscriber behind.
func TestDefuseCgofuseSignalsNilReArm(t *testing.T) {
	standIn := make(chan os.Signal, 1)
	signal.Notify(standIn, syscall.SIGTERM)
	defer signal.Stop(standIn)

	defuseCgofuseSignals(nil)

	// Re-subscribe a guard so the delivered signal cannot kill the process.
	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGTERM)
	defer signal.Stop(guard)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("deliver SIGTERM: %v", err)
	}
	select {
	case <-guard:
	case <-time.After(2 * time.Second):
		t.Fatal("guard handler never received SIGTERM")
	}
	select {
	case sig := <-standIn:
		t.Fatalf("defused stand-in channel still received %v", sig)
	case <-time.After(100 * time.Millisecond):
	}
}
