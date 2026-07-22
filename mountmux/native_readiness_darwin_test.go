//go:build darwin

package mountmux

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/fusekit/mountproto"
)

func TestAwaitNativeReadinessDoesNotAcceptInitWithoutMount(t *testing.T) {
	initialized := closedInitializedSignal()
	ctx, cancel := context.WithCancel(t.Context())
	var tableCalls, throughCalls atomic.Int64
	ops := testNativeReadinessOps()
	ops.mountTable = func() ([]nativeMountEntry, error) {
		tableCalls.Add(1)
		cancel()
		return nil, nil
	}
	ops.statRoot = func(string) error { throughCalls.Add(1); return nil }
	ops.readRoot = func(string) error { throughCalls.Add(1); return nil }
	_, err := awaitNativeReadiness(ctx, "/Volumes/FuseKit", initialized, func() uint64 { return 1 }, ops)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("awaitNativeReadiness = %v, want canceled mount wait", err)
	}
	if tableCalls.Load() != 1 || throughCalls.Load() != 0 {
		t.Fatalf("calls = table %d, through %d", tableCalls.Load(), throughCalls.Load())
	}
}

func TestAwaitNativeReadinessRejectsWrongMountedFilesystem(t *testing.T) {
	initialized := closedInitializedSignal()
	ops := testNativeReadinessOps()
	ops.mountTable = func() ([]nativeMountEntry, error) {
		return []nativeMountEntry{{mountpoint: "/Volumes/FuseKit", filesystem: "apfs", source: "/dev/disk1"}}, nil
	}
	_, err := awaitNativeReadiness(t.Context(), "/Volumes/FuseKit", initialized, func() uint64 { return 1 }, ops)
	if err == nil || !strings.Contains(err.Error(), `filesystem "apfs" from "/dev/disk1"`) {
		t.Fatalf("awaitNativeReadiness = %v, want exact filesystem rejection", err)
	}
}

func TestAwaitNativeReadinessRequiresThroughMountAndCatalogProof(t *testing.T) {
	sentinel := errors.New("injected through-path failure")
	tests := []struct {
		name    string
		statErr error
		readErr error
		advance bool
		want    string
	}{
		{name: "stat", statErr: sentinel, advance: true, want: "through-mount stat"},
		{name: "readdir", readErr: sentinel, advance: true, want: "through-mount readdir"},
		{name: "catalog", advance: false, want: "did not reach the catalog"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			initialized := closedInitializedSignal()
			var epoch atomic.Uint64
			epoch.Store(7)
			ops := testNativeReadinessOps()
			ops.statRoot = func(string) error { return test.statErr }
			ops.readRoot = func(string) error {
				if test.advance && test.readErr == nil {
					epoch.Add(1)
				}
				return test.readErr
			}
			_, err := awaitNativeReadiness(t.Context(), "/Volumes/FuseKit", initialized, epoch.Load, ops)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("awaitNativeReadiness = %v, want %q", err, test.want)
			}
		})
	}
}

func TestAwaitNativeReadinessAcceptsExactMountAfterServedRootRead(t *testing.T) {
	initialized := closedInitializedSignal()
	var epoch atomic.Uint64
	var sequence []string
	ops := testNativeReadinessOps()
	ops.mountTable = func() ([]nativeMountEntry, error) {
		sequence = append(sequence, "table")
		return exactNativeMountTable("/Volumes/FuseKit"), nil
	}
	ops.statRoot = func(string) error {
		sequence = append(sequence, "stat")
		return nil
	}
	ops.readRoot = func(string) error {
		sequence = append(sequence, "readdir")
		epoch.Add(1)
		return nil
	}
	proof, err := awaitNativeReadiness(t.Context(), "/Volumes/FuseKit", initialized, epoch.Load, ops)
	if err != nil {
		t.Fatalf("awaitNativeReadiness: %v", err)
	}
	wantSource, err := mountproto.NativeMountSource("/Volumes/FuseKit")
	if err != nil {
		t.Fatalf("native mount source: %v", err)
	}
	if proof.presentationRoot != "/Volumes/FuseKit" || proof.filesystem != mountproto.NativeMountFilesystem ||
		proof.source != wantSource || proof.catalogEpoch != 1 {
		t.Fatalf("native proof = %#v", proof)
	}
	if got := strings.Join(sequence, ","); got != "table,stat,readdir" {
		t.Fatalf("sequence = %q", got)
	}
}

func TestAwaitNativeReadinessDefersDeadlineToParentAndHonorsCancellation(t *testing.T) {
	t.Run("through proof beyond removed inner deadline", func(t *testing.T) {
		initialized := closedInitializedSignal()
		var epoch atomic.Uint64
		ops := testNativeReadinessOps()
		ops.statRoot = func(string) error {
			time.Sleep(2100 * time.Millisecond)
			return nil
		}
		ops.readRoot = func(string) error { epoch.Add(1); return nil }
		if _, err := awaitNativeReadiness(t.Context(), "/Volumes/FuseKit", initialized, epoch.Load, ops); err != nil {
			t.Fatalf("awaitNativeReadiness after legacy two-second boundary: %v", err)
		}
	})

	t.Run("canceled before init", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := awaitNativeReadiness(ctx, "/Volumes/FuseKit", make(chan struct{}), func() uint64 { return 0 }, testNativeReadinessOps())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("awaitNativeReadiness = %v, want cancellation", err)
		}
	})

	t.Run("canceled during through proof", func(t *testing.T) {
		initialized := closedInitializedSignal()
		ctx, cancel := context.WithCancel(t.Context())
		blocked := make(chan struct{})
		defer close(blocked)
		entered := make(chan struct{})
		ops := testNativeReadinessOps()
		ops.statRoot = func(string) error {
			close(entered)
			<-blocked
			return nil
		}
		result := make(chan error, 1)
		go func() {
			_, err := awaitNativeReadiness(ctx, "/Volumes/FuseKit", initialized, func() uint64 { return 0 }, ops)
			result <- err
		}()
		<-entered
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("awaitNativeReadiness = %v, want parent cancellation", err)
		}
	})
}

func TestNativeReadinessOrchestrationRejectsExitAndReturnsOnCancel(t *testing.T) {
	t.Run("init and exit simultaneous", func(t *testing.T) {
		mountDone := make(chan struct{})
		close(mountDone)
		err := awaitNativeInitialization(t.Context(), mountDone, closedInitializedSignal())
		if !errors.Is(err, ErrNativeMount) {
			t.Fatalf("awaitNativeInitialization = %v, want host exit", err)
		}
	})

	t.Run("proof and exit simultaneous", func(t *testing.T) {
		mountDone := make(chan struct{})
		close(mountDone)
		ready := make(chan nativeReadinessResult, 1)
		ready <- nativeReadinessResult{proof: nativeMountProof{catalogEpoch: 1}}
		_, err := awaitNativeProof(t.Context(), mountDone, ready)
		if !errors.Is(err, ErrNativeMount) {
			t.Fatalf("awaitNativeProof = %v, want host exit", err)
		}
	})

	t.Run("cancel does not join mount", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := awaitNativeInitialization(ctx, make(chan struct{}), make(chan struct{}))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("awaitNativeInitialization = %v, want cancellation", err)
		}
	})
}

func TestRequireExactNativeMountFailsWhenProofDisappears(t *testing.T) {
	err := requireExactNativeMount("/Volumes/FuseKit", func() ([]nativeMountEntry, error) { return nil, nil })
	if err == nil || !strings.Contains(err.Error(), "disappeared") {
		t.Fatalf("requireExactNativeMount = %v", err)
	}
}

func TestExactNativeMountRejectsDuplicatesAndAcceptsKernelSpelling(t *testing.T) {
	if mounted, err := exactNativeMount("/Volumes/FuseKit", exactNativeMountTable("/Volumes/FuseKit")); err != nil || !mounted {
		t.Fatalf("exactNativeMount = %t, %v", mounted, err)
	}
	duplicate := append(exactNativeMountTable("/Volumes/FuseKit"), exactNativeMountTable("/Volumes/FuseKit")...)
	if _, err := exactNativeMount("/Volumes/FuseKit", duplicate); err == nil {
		t.Fatal("duplicate native mount was accepted")
	}
	candidates := nativeMountCandidates("/tmp/FuseKit")
	if len(candidates) != 2 {
		t.Fatalf("/tmp candidates = %v", candidates)
	}
	source, err := mountproto.NativeMountSource("/tmp/FuseKit")
	if err != nil {
		t.Fatalf("native mount source: %v", err)
	}
	alternate := []nativeMountEntry{{mountpoint: candidates[1], filesystem: "nfs", source: source}}
	if mounted, err := exactNativeMount("/tmp/FuseKit", alternate); err != nil || !mounted {
		t.Fatalf("alternate kernel spelling = %t, %v", mounted, err)
	}
}

func TestExactNativeMountDerivesSourceFromPresentationRoot(t *testing.T) {
	for _, root := range []string{
		"/Users/yasyf/.cc-pool/accounts",
		"/private/tmp/mount",
		"/Volumes/other",
	} {
		t.Run(filepath.Base(root), func(t *testing.T) {
			mounted, err := exactNativeMount(root, exactNativeMountTable(root))
			if err != nil || !mounted {
				t.Fatalf("exactNativeMount(%q) = %t, %v", root, mounted, err)
			}
		})
	}
}

func testNativeReadinessOps() nativeReadinessOps {
	return nativeReadinessOps{
		mountTable: func() ([]nativeMountEntry, error) { return exactNativeMountTable("/Volumes/FuseKit"), nil },
		statRoot:   func(string) error { return nil }, readRoot: func(string) error { return nil },
		pollInterval: time.Millisecond,
	}
}

func exactNativeMountTable(root string) []nativeMountEntry {
	source, err := mountproto.NativeMountSource(root)
	if err != nil {
		panic(err)
	}
	return []nativeMountEntry{{
		mountpoint: root, filesystem: mountproto.NativeMountFilesystem, source: source,
	}}
}

func closedInitializedSignal() <-chan struct{} {
	initialized := make(chan struct{})
	close(initialized)
	return initialized
}
