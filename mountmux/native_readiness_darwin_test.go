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
	var tableCalls atomic.Int64
	ops := testNativeReadinessOps()
	ops.mountTable = func() ([]nativeMountEntry, error) {
		tableCalls.Add(1)
		cancel()
		return nil, nil
	}
	_, err := awaitNativeMountIdentity(ctx, "/Volumes/FuseKit", initialized, ops)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("awaitNativeMountIdentity = %v, want canceled mount wait", err)
	}
	if tableCalls.Load() != 1 {
		t.Fatalf("mount table calls = %d, want one", tableCalls.Load())
	}
}

func TestAwaitNativeReadinessRejectsWrongMountedFilesystem(t *testing.T) {
	initialized := closedInitializedSignal()
	ops := testNativeReadinessOps()
	ops.mountTable = func() ([]nativeMountEntry, error) {
		return []nativeMountEntry{{mountpoint: "/Volumes/FuseKit", filesystem: "apfs", source: "/dev/disk1"}}, nil
	}
	_, err := awaitNativeMountIdentity(t.Context(), "/Volumes/FuseKit", initialized, ops)
	if err == nil || !strings.Contains(err.Error(), `filesystem "apfs" from "/dev/disk1"`) {
		t.Fatalf("awaitNativeMountIdentity = %v, want exact filesystem rejection", err)
	}
}

func TestAwaitNativeMountIdentityNeverSelfProbes(t *testing.T) {
	initialized := closedInitializedSignal()
	var sequence []string
	ops := testNativeReadinessOps()
	ops.mountTable = func() ([]nativeMountEntry, error) {
		sequence = append(sequence, "table")
		return exactNativeMountTable("/Volumes/FuseKit"), nil
	}
	identity, err := awaitNativeMountIdentity(t.Context(), "/Volumes/FuseKit", initialized, ops)
	if err != nil {
		t.Fatalf("awaitNativeMountIdentity: %v", err)
	}
	wantSource, err := mountproto.NativeMountSource("/Volumes/FuseKit")
	if err != nil {
		t.Fatalf("native mount source: %v", err)
	}
	if identity.presentationRoot != "/Volumes/FuseKit" || identity.filesystem != mountproto.NativeMountFilesystem ||
		identity.source != wantSource {
		t.Fatalf("native identity = %#v", identity)
	}
	if got := strings.Join(sequence, ","); got != "table" {
		t.Fatalf("sequence = %q", got)
	}
}

func TestAwaitNativeMountIdentityHonorsPreCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := awaitNativeMountIdentity(ctx, "/Volumes/FuseKit", make(chan struct{}), testNativeReadinessOps())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("awaitNativeMountIdentity = %v, want cancellation", err)
	}
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
		ready <- nativeReadinessResult{mount: nativeMountIdentity{presentationRoot: "/Volumes/FuseKit"}}
		_, err := awaitNativeIdentity(t.Context(), mountDone, ready)
		if !errors.Is(err, ErrNativeMount) {
			t.Fatalf("awaitNativeIdentity = %v, want host exit", err)
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
		mountTable:   func() ([]nativeMountEntry, error) { return exactNativeMountTable("/Volumes/FuseKit"), nil },
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
