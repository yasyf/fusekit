package holder

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/fuset"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountservice"
)

type fakeManagedProcess struct {
	record proc.Record
	done   chan struct{}
	once   sync.Once
	stops  atomic.Int64
}

func newFakeManagedProcess(record proc.Record) *fakeManagedProcess {
	return &fakeManagedProcess{record: record, done: make(chan struct{})}
}

func (p *fakeManagedProcess) Record() proc.Record { return p.record }

func (p *fakeManagedProcess) Wait(ctx context.Context) error {
	select {
	case <-p.done:
		return supervise.ErrProcessStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *fakeManagedProcess) Stop(context.Context) error {
	p.stops.Add(1)
	p.once.Do(func() { close(p.done) })
	return nil
}

func TestNativeProcessRequiresExactTrackedPeerAndStopsOnSessionLoss(t *testing.T) {
	record := proc.Record{
		PID: 4242, StartTime: "start-1", Boot: "boot-1", Generation: "generation-1",
		ProcessGroup: true, SessionID: 4242,
	}
	process := newFakeManagedProcess(record)
	specs := make(chan supervise.ProcessSpec, 1)
	native := newNativeProcess(nativeProcessConfig{
		start: func(ctx context.Context, spec supervise.ProcessSpec) (managedProcess, error) {
			specs <- spec
			if err := spec.Ready(ctx, record); err != nil {
				_ = process.Stop(context.Background())
				return nil, err
			}
			return process, nil
		},
		socket: "/tmp/fusekit-runtime/socket", executable: "/Applications/FuseKit.app/Contents/MacOS/FuseKit",
		options: []string{"-ovolname=FuseKit"},
	})
	started := make(chan error, 1)
	go func() { started <- native.Start(t.Context(), "/Volumes/FuseKit", nil) }()
	spec := <-specs
	if spec.Path != "/Applications/FuseKit.app/Contents/MacOS/FuseKit" {
		t.Fatalf("managed path = %q", spec.Path)
	}
	child, recognized, err := mountmux.ParseNativeChildArguments(spec.Args)
	if err != nil || !recognized || child.Socket != "/tmp/fusekit-runtime/socket" || child.Root != "/Volumes/FuseKit" {
		t.Fatalf("native child contract = %#v, %t, %v", child, recognized, err)
	}
	assertNativeEnvironment(t, spec.Env)

	session := &wire.AcceptedSession{}
	wrong := mountservice.Identity{
		Peer: wire.Peer{PID: record.PID, StartTime: "reused-pid", Boot: "boot-1"}, Session: session,
	}
	if err := native.Bind(t.Context(), wrong); !errors.Is(err, mountservice.ErrUnauthorized) {
		t.Fatalf("PID-reused Bind = %v, want unauthorized", err)
	}
	wrongBoot := mountservice.Identity{
		Peer: wire.Peer{PID: record.PID, StartTime: record.StartTime, Boot: "previous-boot"}, Session: session,
	}
	if err := native.Bind(t.Context(), wrongBoot); !errors.Is(err, mountservice.ErrUnauthorized) {
		t.Fatalf("cross-boot Bind = %v, want unauthorized", err)
	}
	exact := mountservice.Identity{
		Peer: wire.Peer{PID: record.PID, StartTime: record.StartTime, Boot: "boot-1"}, Session: session,
	}
	if err := native.Bind(t.Context(), exact); err != nil {
		t.Fatalf("exact Bind: %v", err)
	}
	if err := native.Ready(t.Context(), mountservice.Identity{Peer: exact.Peer, Session: &wire.AcceptedSession{}}); !errors.Is(err, mountservice.ErrUnauthorized) {
		t.Fatalf("wrong-session Ready = %v, want unauthorized", err)
	}
	if err := native.Ready(t.Context(), exact); err != nil {
		t.Fatalf("exact Ready: %v", err)
	}
	if err := <-started; err != nil {
		t.Fatalf("Start: %v", err)
	}
	if state := native.HealthState(); state != daemon.StateHealthy {
		t.Fatalf("health = %q, want healthy", state)
	}

	native.Unbind(exact)
	if process.stops.Load() == 0 {
		t.Fatal("session loss did not stop the exact managed process")
	}
	if state := native.HealthState(); state != daemon.StateFailed {
		t.Fatalf("health after session loss = %q, want failed", state)
	}
	if err := native.Close(t.Context()); !errors.Is(err, ErrNativeProcessUnavailable) {
		t.Fatalf("Close = %v, want native process unavailable", err)
	}
}

func TestValidateNativeExecutableRejectsUnstablePaths(t *testing.T) {
	directory := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateNativeExecutable(executable); err != nil {
		t.Fatalf("executable rejected: %v", err)
	}
	symlink := filepath.Join(directory, "current")
	if err := os.Symlink(executable, symlink); err != nil {
		t.Fatal(err)
	}
	if err := validateNativeExecutable(symlink); err == nil {
		t.Fatal("symlink executable accepted")
	}
	unclean := filepath.Dir(executable) + "/../" + filepath.Base(filepath.Dir(executable)) + "/" + filepath.Base(executable)
	if err := validateNativeExecutable(unclean); err == nil {
		t.Fatal("non-canonical executable accepted")
	}
	other := filepath.Join(directory, "other-holder")
	if err := os.WriteFile(other, []byte("holder"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := validateNativeExecutable(other); err == nil {
		t.Fatal("different executable accepted")
	}
}

func TestNativeProcessReadinessFailureStopsTrackedChildBeforeReturning(t *testing.T) {
	record := proc.Record{
		PID: 5151, StartTime: "start-blocked", Boot: "boot-1", Generation: "generation-1",
		ProcessGroup: true, SessionID: 5151,
	}
	process := newFakeManagedProcess(record)
	native := newNativeProcess(nativeProcessConfig{
		start: func(ctx context.Context, spec supervise.ProcessSpec) (managedProcess, error) {
			readyCtx, cancel := context.WithCancel(ctx)
			cancel()
			err := spec.Ready(readyCtx, record)
			_ = process.Stop(context.Background())
			return nil, err
		},
		socket: "/tmp/fusekit-runtime/socket", executable: "/Applications/FuseKit.app/Contents/MacOS/FuseKit",
	})
	if err := native.Start(t.Context(), "/Volumes/FuseKit", nil); err == nil {
		t.Fatal("readiness failure started native process")
	}
	if process.stops.Load() != 1 {
		t.Fatalf("readiness failure stops = %d, want one", process.stops.Load())
	}
	if state := native.HealthState(); state != daemon.StateFailed {
		t.Fatalf("health = %q, want failed", state)
	}
}

func assertNativeEnvironment(t *testing.T, environment []string) {
	t.Helper()
	var matches []string
	for _, entry := range environment {
		if strings.HasPrefix(entry, "CGOFUSE_LIBFUSE_PATH=") {
			matches = append(matches, entry)
		}
	}
	want := "CGOFUSE_LIBFUSE_PATH=" + fuset.Dylib
	if len(matches) != 1 || matches[0] != want {
		t.Fatalf("native FUSE environment = %v, want [%q]", matches, want)
	}
}
