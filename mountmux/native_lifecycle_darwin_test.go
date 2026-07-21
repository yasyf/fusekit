//go:build darwin && cgo && fuse

package mountmux

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func TestNativeMountSettlementUsesOneRegularUnmountAndJoinsHost(t *testing.T) {
	mount := &nativeMount{done: make(chan struct{}), result: true}
	called := make(chan struct{})
	deadline := make(chan time.Time)
	badCall := make(chan string, 1)
	result := make(chan error, 1)
	go func() {
		result <- mount.settle("/private/tmp/fusekit", nativeSettlementOps{
			unmount: func(root string, flags int) error {
				if root != "/private/tmp/fusekit" || flags != 0 {
					badCall <- root
				}
				close(called)
				return nil
			},
			after: func(time.Duration) <-chan time.Time { return deadline },
		})
	}()
	<-called
	select {
	case root := <-badCall:
		t.Fatalf("unmount root = %q, want exact root and flags=0", root)
	default:
	}
	select {
	case err := <-result:
		t.Fatalf("settlement returned before host join: %v", err)
	default:
	}
	close(mount.done)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestNativeMountSettlementHasNoForcedFallback(t *testing.T) {
	mount := &nativeMount{done: make(chan struct{}), result: true}
	sentinel := errors.New("busy")
	called := make(chan struct{})
	deadline := make(chan time.Time)
	badFlags := make(chan int, 1)
	result := make(chan error, 1)
	go func() {
		result <- mount.settle("/private/tmp/fusekit", nativeSettlementOps{
			unmount: func(_ string, flags int) error {
				if flags != 0 {
					badFlags <- flags
				}
				close(called)
				return sentinel
			},
			after: func(time.Duration) <-chan time.Time { return deadline },
		})
	}()
	<-called
	select {
	case flags := <-badFlags:
		t.Fatalf("unmount flags = %d, want 0", flags)
	default:
	}
	close(mount.done)
	err := <-result
	if !errors.Is(err, sentinel) || !errors.Is(err, ErrNativeMount) {
		t.Fatalf("settlement = %v, want native mount and unmount errors", err)
	}
}

func TestNativeMountSettlementSkipsUnmountAfterHostExit(t *testing.T) {
	mount := &nativeMount{done: make(chan struct{}), result: false}
	close(mount.done)
	if err := mount.settle("/private/tmp/fusekit", nativeSettlementOps{
		unmount: func(string, int) error {
			t.Fatal("already-exited host was unmounted")
			return nil
		},
		after: func(time.Duration) <-chan time.Time {
			t.Fatal("already-exited host allocated a settlement deadline")
			return nil
		},
	}); !errors.Is(err, ErrNativeMount) {
		t.Fatalf("settlement = %v, want native mount failure", err)
	}
}

func TestNativeMountSettlementDeadlineDoesNotJoinBlockedUnmount(t *testing.T) {
	mount := &nativeMount{done: make(chan struct{}), result: true}
	deadline := make(chan time.Time, 1)
	configured := make(chan time.Duration, 1)
	flags := make(chan int, 2)
	release := make(chan struct{})
	unmountReturned := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- mount.settle("/private/tmp/fusekit", nativeSettlementOps{
			unmount: func(_ string, value int) error {
				flags <- value
				<-release
				close(unmountReturned)
				return nil
			},
			after: func(value time.Duration) <-chan time.Time {
				configured <- value
				return deadline
			},
		})
	}()
	if got := <-configured; got != nativeSettlementTimeout {
		t.Fatalf("settlement timeout = %s, want %s", got, nativeSettlementTimeout)
	}
	if got := <-flags; got != 0 {
		t.Fatalf("unmount flags = %d, want 0", got)
	}
	deadline <- time.Now()
	err := <-result
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrNativeMount) {
		t.Fatalf("settlement = %v, want explicit native deadline failure", err)
	}
	select {
	case <-unmountReturned:
		t.Fatal("settlement joined blocked unmount syscall")
	default:
	}
	if len(flags) != 0 {
		t.Fatalf("extra unmount attempts = %d, want 0", len(flags))
	}
	close(release)
	<-unmountReturned
	close(mount.done)
}

func TestNativeMountSettlementDeadlineDoesNotJoinBlockedHost(t *testing.T) {
	mount := &nativeMount{done: make(chan struct{}), result: true}
	deadline := make(chan time.Time, 1)
	unmounted := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- mount.settle("/private/tmp/fusekit", nativeSettlementOps{
			unmount: func(_ string, flags int) error {
				if flags != 0 {
					return errors.New("forced unmount")
				}
				close(unmounted)
				return nil
			},
			after: func(time.Duration) <-chan time.Time { return deadline },
		})
	}()
	<-unmounted
	deadline <- time.Now()
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrNativeMount) {
		t.Fatalf("settlement = %v, want explicit native deadline failure", err)
	}
	close(mount.done)
}

func TestRearmNativeSignalsDefusesCgofuseSubscriber(t *testing.T) {
	standIn := make(chan os.Signal, 1)
	signal.Notify(standIn, syscall.SIGTERM)
	t.Cleanup(func() { signal.Stop(standIn) })
	lifetime, stop := rearmNativeSignals(context.Background())
	defer stop()
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lifetime.Done():
	case <-time.After(time.Second):
		t.Fatal("FuseKit signal context did not receive SIGTERM")
	}
	select {
	case value := <-standIn:
		t.Fatalf("cgofuse signal subscriber remained armed: %v", value)
	case <-time.After(50 * time.Millisecond):
	}
}
