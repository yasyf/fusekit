package holder

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
)

const (
	testNativeLibrary = "/Users/example/Applications/ProductHelper.app/Contents/Frameworks/libfuse-t.dylib"
	testNativeDigest  = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

type fakeManagedProcess struct {
	record proc.Record
	done   chan struct{}
	once   sync.Once
	stops  atomic.Int64
	start  func(context.Context) error
}

func newFakeManagedProcess(record proc.Record) *fakeManagedProcess {
	return &fakeManagedProcess{record: record, done: make(chan struct{})}
}

func (p *fakeManagedProcess) Record() proc.Record { return p.record }

func (p *fakeManagedProcess) Start(ctx context.Context) error {
	if p.start != nil {
		return p.start(ctx)
	}
	return nil
}

func (p *fakeManagedProcess) Done() <-chan struct{} { return p.done }

func (p *fakeManagedProcess) Exit() (proc.ProcessExit, bool) {
	select {
	case <-p.done:
		return proc.ProcessExit{Stopped: true}, true
	default:
		return proc.ProcessExit{}, false
	}
}

func (p *fakeManagedProcess) Stop(context.Context) error {
	p.stops.Add(1)
	p.once.Do(func() { close(p.done) })
	return nil
}

type gatedManagedProcess struct {
	record   proc.Record
	entered  chan struct{}
	release  chan struct{}
	done     chan struct{}
	stopErr  error
	stopOnce sync.Once
	doneOnce sync.Once
	stops    atomic.Int64
}

func (p *gatedManagedProcess) Record() proc.Record { return p.record }

func (p *gatedManagedProcess) Start(context.Context) error { return nil }

func (p *gatedManagedProcess) Done() <-chan struct{} { return p.done }

func (p *gatedManagedProcess) Exit() (proc.ProcessExit, bool) {
	select {
	case <-p.done:
		exit := proc.ProcessExit{Stopped: true}
		if p.stopErr != nil {
			exit.Error = p.stopErr.Error()
		}
		return exit, true
	default:
		return proc.ProcessExit{}, false
	}
}

func (p *gatedManagedProcess) Stop(context.Context) error {
	p.stops.Add(1)
	p.stopOnce.Do(func() { close(p.entered) })
	<-p.release
	p.doneOnce.Do(func() { close(p.done) })
	return p.stopErr
}

func TestNativeProcessCloseJoinsOnceAndReplaysTerminalResult(t *testing.T) {
	t.Parallel()
	terminalErr := errors.New("native stop failed")
	process := &gatedManagedProcess{
		entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{}), stopErr: terminalErr,
	}
	native := newNativeProcess(nativeProcessConfig{})
	native.phase = nativeProcessLive
	native.process = process

	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- native.Close(ctx) }()
	go func() { second <- native.Close(context.Background()) }()
	<-process.entered
	cancel()
	select {
	case err := <-first:
		t.Fatalf("canceled Close returned before exact process settlement: %v", err)
	case err := <-second:
		t.Fatalf("concurrent Close returned before exact process settlement: %v", err)
	default:
	}
	close(process.release)
	if err := <-first; !errors.Is(err, context.Canceled) || !errors.Is(err, terminalErr) {
		t.Fatalf("canceled Close = %v, want caller cancellation and terminal result", err)
	}
	if err := <-second; !errors.Is(err, terminalErr) || errors.Is(err, context.Canceled) {
		t.Fatalf("concurrent Close = %v, want terminal result only", err)
	}
	if err := native.Close(context.Background()); !errors.Is(err, terminalErr) {
		t.Fatalf("repeated Close = %v, want cached terminal result", err)
	}
	if stops := process.stops.Load(); stops != 1 {
		t.Fatalf("physical stop calls = %d, want 1", stops)
	}
}

func TestNativeProcessTransportLossDoesNotWaitForResourceSettlement(t *testing.T) {
	process := newFakeManagedProcess(proc.Record{PID: 42})
	session := &wire.AcceptedSession{}
	done := make(chan struct{})
	settled := make(chan struct{})
	native := newNativeProcess(nativeProcessConfig{})
	native.phase = nativeProcessLive
	native.process = process
	native.bound = &wireSession{session: session, done: done, settled: settled}
	identity := mountservice.Identity{Session: session}

	go native.watch(process)
	if err := process.Stop(context.Background()); err != nil {
		t.Fatalf("simulate reaped process: %v", err)
	}
	if phase := native.RuntimeHealth("activation-1").NativePhase; phase != mountproto.NativePhaseLive {
		t.Fatalf("phase before transport loss = %q, want live", phase)
	}
	native.Unbind(identity)
	if phase := native.RuntimeHealth("activation-1").NativePhase; phase != mountproto.NativePhaseFailed {
		t.Fatalf("phase after transport loss = %q, want failed", phase)
	}

	deadline := time.Now().Add(time.Second)
	for {
		native.mu.Lock()
		reaped := native.process == nil
		native.mu.Unlock()
		if reaped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reaped native process remained retained without resource settlement")
		}
		runtime.Gosched()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := native.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded Close = %v, want resource-settlement deadline", err)
	}
	native.Settled(identity, nil)
	if err := native.Close(context.Background()); !errors.Is(err, ErrNativeProcessUnavailable) {
		t.Fatalf("settled Close = %v, want unavailable after transport loss", err)
	}
}

func TestNativeProcessStartingSessionLossRejectsReplacementAfterReadiness(t *testing.T) {
	record := proc.Record{
		PID: 4242, StartTime: "start-1", Boot: "boot-1", Generation: "generation-1",
		ProcessGroup: true, SessionID: 4242, RecoveryClass: proc.RecoveryNativeMount,
	}
	process := newFakeManagedProcess(record)
	specs := make(chan proc.SpawnConfig, 1)
	releaseStart := make(chan struct{})
	process.start = func(context.Context) error {
		<-releaseStart
		return nil
	}
	native := newNativeProcess(nativeProcessConfig{
		prepare: func(_ context.Context, spec proc.SpawnConfig, role trust.PeerRole, _, _ io.Writer) (managedProcess, error) {
			if role != NativeChildRole {
				t.Fatalf("native role = %q", role)
			}
			specs <- spec
			return process, nil
		},
		socket: "/tmp/fusekit-runtime/socket", executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		library: testNativeLibrary, librarySHA256: testNativeDigest,
		confirmMount: func(context.Context, string, string) error { return nil },
	})
	started := make(chan error, 1)
	go func() { started <- native.Start(t.Context(), "/Volumes/FuseKit", nil) }()
	<-specs
	peer := wire.Peer{PID: record.PID, StartTime: record.StartTime, Boot: record.Boot}
	first := mountservice.Identity{Peer: peer, Session: &wire.AcceptedSession{}}
	if err := native.Bind(t.Context(), first); err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if err := native.Mounted(t.Context(), first, testNativeMountIdentity("/Volumes/FuseKit"), testNativeProbeToken()); err != nil {
		t.Fatalf("first Mounted: %v", err)
	}
	if err := native.Ready(t.Context(), first, testNativeMountProof("/Volumes/FuseKit")); err != nil {
		t.Fatalf("first Ready: %v", err)
	}
	native.Unbind(first)
	native.Settled(first, nil)
	second := mountservice.Identity{Peer: peer, Session: &wire.AcceptedSession{}}
	if err := native.Bind(t.Context(), second); !errors.Is(err, ErrNativeProcessUnavailable) {
		t.Fatalf("replacement Bind after starting-session loss = %v, want unavailable", err)
	}
	close(releaseStart)
	if err := <-started; !errors.Is(err, ErrNativeProcessUnavailable) {
		t.Fatalf("Start after authenticated session loss = %v, want unavailable", err)
	}
}

func TestNativeProcessCloseJoinsInFlightStartSettlement(t *testing.T) {
	t.Parallel()
	process := newFakeManagedProcess(proc.Record{
		PID: 4242, StartTime: "start-close", Boot: "boot-1", Generation: "generation-1",
		ProcessGroup: true, SessionID: 4242, RecoveryClass: proc.RecoveryNativeMount,
	})
	entered := make(chan struct{})
	release := make(chan struct{})
	native := newNativeProcess(nativeProcessConfig{
		prepare: func(context.Context, proc.SpawnConfig, trust.PeerRole, io.Writer, io.Writer) (managedProcess, error) {
			close(entered)
			<-release
			return process, nil
		},
		socket: "/tmp/fusekit-runtime/socket", executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		library: testNativeLibrary, librarySHA256: testNativeDigest,
	})
	started := make(chan error, 1)
	go func() { started <- native.Start(t.Context(), "/Volumes/FuseKit", nil) }()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- native.Close(context.Background()) }()
	for {
		native.mu.Lock()
		phase := native.phase
		native.mu.Unlock()
		if phase == nativeProcessClosing {
			break
		}
		runtime.Gosched()
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before launch settlement: %v", err)
	default:
	}
	close(release)
	if err := <-started; !errors.Is(err, ErrNativeProcessUnavailable) {
		t.Fatalf("Start = %v, want unavailable after concurrent Close", err)
	}
	if err := <-closed; !errors.Is(err, ErrNativeProcessUnavailable) {
		t.Fatalf("Close = %v, want cached start terminal result", err)
	}
	if stops := process.stops.Load(); stops != 1 {
		t.Fatalf("late launched process stops = %d, want 1", stops)
	}
}

func TestNativeProcessStartErrorStopsReturnedProcessAndCachesResult(t *testing.T) {
	t.Parallel()
	startErr := errors.New("launcher failed after process creation")
	process := newFakeManagedProcess(proc.Record{})
	native := newNativeProcess(nativeProcessConfig{
		prepare: func(context.Context, proc.SpawnConfig, trust.PeerRole, io.Writer, io.Writer) (managedProcess, error) {
			return process, startErr
		},
		socket: "/tmp/fusekit-runtime/socket", executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		library: testNativeLibrary, librarySHA256: testNativeDigest,
	})
	if err := native.Start(t.Context(), "/Volumes/FuseKit", nil); !errors.Is(err, startErr) {
		t.Fatalf("Start = %v, want launcher terminal error", err)
	}
	if stops := process.stops.Load(); stops != 1 {
		t.Fatalf("returned process stops = %d, want 1", stops)
	}
	if err := native.Close(context.Background()); !errors.Is(err, startErr) {
		t.Fatalf("Close = %v, want cached launcher terminal error", err)
	}
}

func TestNativeProcessRejectsTypedNilProcess(t *testing.T) {
	t.Parallel()
	native := newNativeProcess(nativeProcessConfig{
		prepare: func(context.Context, proc.SpawnConfig, trust.PeerRole, io.Writer, io.Writer) (managedProcess, error) {
			var process *fakeManagedProcess
			return process, nil
		},
		socket: "/tmp/fusekit-runtime/socket", executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		library: testNativeLibrary, librarySHA256: testNativeDigest,
	})
	err := native.Start(t.Context(), "/Volumes/FuseKit", nil)
	if err == nil || !strings.Contains(err.Error(), "preparer returned no process") {
		t.Fatalf("Start = %v, want missing process rejection", err)
	}
	if closeErr := native.Close(context.Background()); !errors.Is(closeErr, err) {
		t.Fatalf("Close = %v, want cached start failure %v", closeErr, err)
	}
}

func TestNativeProcessValidatesBundledLibraryBeforeLaunchAndReadiness(t *testing.T) {
	tamper := errors.New("bundled library tampered")
	starts := 0
	native := newNativeProcess(nativeProcessConfig{
		prepare: func(context.Context, proc.SpawnConfig, trust.PeerRole, io.Writer, io.Writer) (managedProcess, error) {
			starts++
			return nil, nil
		},
		socket: "/tmp/fusekit-runtime/socket", executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		library: testNativeLibrary, librarySHA256: testNativeDigest,
		validateLibrary: func(path, digest string) error {
			if path != testNativeLibrary || digest != testNativeDigest {
				t.Fatalf("validator inputs = %q %q", path, digest)
			}
			return tamper
		},
	})
	if err := native.Start(t.Context(), "/Volumes/FuseKit", nil); !errors.Is(err, tamper) {
		t.Fatalf("tampered pre-launch library = %v", err)
	}
	if starts != 0 {
		t.Fatalf("tampered library launched %d processes", starts)
	}
	if err := native.Ready(t.Context(), mountservice.Identity{}, testNativeMountProof("/Volumes/FuseKit")); !errors.Is(err, tamper) {
		t.Fatalf("tampered pre-ready library = %v", err)
	}
}

func TestNativeProcessMountedUsesExternalProofBeforeReady(t *testing.T) {
	record := proc.Record{PID: 4242, StartTime: "start-1", Boot: "boot-1"}
	session := &wire.AcceptedSession{}
	identity := mountservice.Identity{
		Peer:    wire.Peer{PID: record.PID, StartTime: record.StartTime, Boot: record.Boot},
		Session: session,
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	native := newNativeProcess(nativeProcessConfig{confirmMount: func(ctx context.Context, root, token string) error {
		if root != "/Volumes/FuseKit" {
			t.Fatalf("external proof root = %q", root)
		}
		if token != testNativeProbeToken() {
			t.Fatalf("external proof token = %q", token)
		}
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	native.phase = nativeProcessStarting
	native.root = "/Volumes/FuseKit"
	native.record = record
	native.recordSet = true
	native.bound = &wireSession{session: session, done: make(chan struct{}), settled: make(chan struct{})}
	native.readyResult = make(chan error, 1)

	mounted := make(chan error, 1)
	go func() {
		mounted <- native.Mounted(t.Context(), identity, testNativeMountIdentity("/Volumes/FuseKit"), testNativeProbeToken())
	}()
	<-entered
	if err := native.Mounted(t.Context(), identity, testNativeMountIdentity("/Volumes/FuseKit"), strings.Repeat("b", 64)); !errors.Is(err, ErrNativeProcessUnavailable) {
		t.Fatalf("overlapping Mounted = %v, want unavailable", err)
	}
	if err := native.Ready(t.Context(), identity, testNativeMountProof("/Volumes/FuseKit")); !errors.Is(err, mountservice.ErrUnauthorized) {
		t.Fatalf("Ready before external proof = %v, want unauthorized", err)
	}
	close(release)
	if err := <-mounted; err != nil {
		t.Fatalf("Mounted: %v", err)
	}
	if err := native.Ready(t.Context(), identity, testNativeMountProof("/Volumes/FuseKit")); err != nil {
		t.Fatalf("Ready after external proof: %v", err)
	}
}

func TestNativeProcessMountedExternalProofFailureAndCancellation(t *testing.T) {
	for _, test := range []struct {
		name    string
		confirm func(context.Context, string, string) error
		want    error
	}{
		{name: "failure", confirm: func(context.Context, string, string) error { return catalog.ErrIntegrity }, want: catalog.ErrIntegrity},
		{name: "cancellation", confirm: func(ctx context.Context, _, _ string) error { <-ctx.Done(); return ctx.Err() }, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := proc.Record{PID: 4242, StartTime: "start-1", Boot: "boot-1"}
			session := &wire.AcceptedSession{}
			identity := mountservice.Identity{
				Peer:    wire.Peer{PID: record.PID, StartTime: record.StartTime, Boot: record.Boot},
				Session: session,
			}
			native := newNativeProcess(nativeProcessConfig{confirmMount: test.confirm})
			native.phase = nativeProcessStarting
			native.root = "/Volumes/FuseKit"
			native.record = record
			native.recordSet = true
			native.bound = &wireSession{session: session, done: make(chan struct{}), settled: make(chan struct{})}
			ctx := t.Context()
			if test.name == "cancellation" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			err := native.Mounted(ctx, identity, testNativeMountIdentity("/Volumes/FuseKit"), testNativeProbeToken())
			if !errors.Is(err, test.want) {
				t.Fatalf("Mounted = %v, want %v", err, test.want)
			}
			if native.mounted || native.probing {
				t.Fatalf("failed external proof published state: mounted=%t probing=%t", native.mounted, native.probing)
			}
		})
	}
}

func TestNativeProcessRequiresExactTrackedPeerAndStopsOnSessionLoss(t *testing.T) {
	record := proc.Record{
		PID: 4242, StartTime: "start-1", Boot: "boot-1", Generation: "generation-1",
		ProcessGroup: true, SessionID: 4242, RecoveryClass: proc.RecoveryNativeMount,
	}
	process := newFakeManagedProcess(record)
	specs := make(chan proc.SpawnConfig, 1)
	native := newNativeProcess(nativeProcessConfig{
		prepare: func(_ context.Context, spec proc.SpawnConfig, role trust.PeerRole, _, _ io.Writer) (managedProcess, error) {
			if role != NativeChildRole {
				t.Fatalf("native role = %q", role)
			}
			specs <- spec
			return process, nil
		},
		socket: "/tmp/fusekit-runtime/socket", executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		signature: proc.SignatureDigest{1},
		library:   testNativeLibrary, librarySHA256: testNativeDigest,
		options:      []string{"-ovolname=FuseKit"},
		confirmMount: func(context.Context, string, string) error { return nil },
	})
	started := make(chan error, 1)
	go func() { started <- native.Start(t.Context(), "/Volumes/FuseKit", nil) }()
	spec := <-specs
	if spec.Executable != "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper" {
		t.Fatalf("managed path = %q", spec.Executable)
	}
	if spec.RecoveryClass != proc.RecoveryNativeMount {
		t.Fatalf("recovery class = %d, want native mount", spec.RecoveryClass)
	}
	child, recognized, err := mountmux.ParseNativeChildArguments(spec.Args)
	if err != nil || !recognized || child.Socket != "/tmp/fusekit-runtime/socket" || child.Root != "/Volumes/FuseKit" ||
		child.Library != testNativeLibrary || child.LibrarySHA256 != testNativeDigest {
		t.Fatalf("native child contract = %#v, %t, %v", child, recognized, err)
	}
	assertNativeEnvironment(t, spec.Env)
	if !spec.RequiresPeerFence || spec.ExpectedSignature == nil || *spec.ExpectedSignature != (proc.SignatureDigest{1}) {
		t.Fatalf("native fence = required %t signature %v", spec.RequiresPeerFence, spec.ExpectedSignature)
	}
	if spec.Stdin != proc.StdioNull || spec.Stdout != proc.StdioNull || spec.Stderr != proc.StdioNull {
		t.Fatalf("native stdio = %d/%d/%d, want bounded null topology", spec.Stdin, spec.Stdout, spec.Stderr)
	}

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
	if err := native.Mounted(t.Context(), mountservice.Identity{Peer: exact.Peer, Session: &wire.AcceptedSession{}}, testNativeMountIdentity("/Volumes/FuseKit"), testNativeProbeToken()); !errors.Is(err, mountservice.ErrUnauthorized) {
		t.Fatalf("wrong-session Mounted = %v, want unauthorized", err)
	}
	if err := native.Mounted(t.Context(), exact, testNativeMountIdentity("/Volumes/Other"), testNativeProbeToken()); err == nil || !strings.Contains(err.Error(), "different presentation root") {
		t.Fatalf("wrong-root Mounted = %v, want exact root rejection", err)
	}
	if err := native.Mounted(t.Context(), exact, testNativeMountIdentity("/Volumes/FuseKit"), testNativeProbeToken()); err != nil {
		t.Fatalf("exact Mounted: %v", err)
	}
	wrongProof := testNativeMountProof("/Volumes/Other")
	if err := native.Ready(t.Context(), exact, wrongProof); err == nil || !strings.Contains(err.Error(), "different presentation root") {
		t.Fatalf("wrong-root Ready = %v, want exact presentation-root rejection", err)
	}
	if err := native.Ready(t.Context(), mountservice.Identity{Peer: exact.Peer, Session: &wire.AcceptedSession{}}, testNativeMountProof("/Volumes/FuseKit")); !errors.Is(err, mountservice.ErrUnauthorized) {
		t.Fatalf("wrong-session Ready = %v, want unauthorized", err)
	}
	if err := native.Ready(t.Context(), exact, testNativeMountProof("/Volumes/FuseKit")); err != nil {
		t.Fatalf("exact Ready: %v", err)
	}
	if err := <-started; err != nil {
		t.Fatalf("Start: %v", err)
	}
	if state := native.HealthState(); state != daemon.StateHealthy {
		t.Fatalf("health = %q, want healthy", state)
	}
	health := native.RuntimeHealth("activation-1")
	if health.ActivationGeneration != "activation-1" || health.NativePhase != mountproto.NativePhaseLive ||
		health.NativeMount == nil || *health.NativeMount != testNativeMountProof("/Volumes/FuseKit") {
		t.Fatalf("runtime health = %#v", health)
	}
	health.NativeMount.RootReadEpoch = 99
	if got := native.RuntimeHealth("activation-1").NativeMount.RootReadEpoch; got != 1 {
		t.Fatalf("runtime health exposed mutable proof: epoch = %d", got)
	}

	settlement := errors.New("injected native session settlement")
	native.Unbind(exact)
	native.Settled(exact, settlement)
	if process.stops.Load() == 0 {
		t.Fatal("session loss did not stop the exact managed process")
	}
	if state := native.HealthState(); state != daemon.StateFailed {
		t.Fatalf("health after session loss = %q, want failed", state)
	}
	failedHealth := native.RuntimeHealth("activation-1")
	if phase := failedHealth.NativePhase; phase != mountproto.NativePhaseFailed {
		t.Fatalf("runtime phase after session loss = %q, want failed", phase)
	}
	if failedHealth.NativeMount != nil {
		t.Fatalf("runtime health retained stale mount proof after session loss: %#v", failedHealth.NativeMount)
	}
	if err := native.Close(t.Context()); !errors.Is(err, ErrNativeProcessUnavailable) || !errors.Is(err, settlement) {
		t.Fatalf("Close = %v, want native process unavailable and settlement failure", err)
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
		ProcessGroup: true, SessionID: 5151, RecoveryClass: proc.RecoveryNativeMount,
	}
	process := newFakeManagedProcess(record)
	process.start = func(context.Context) error { return context.Canceled }
	native := newNativeProcess(nativeProcessConfig{
		prepare: func(context.Context, proc.SpawnConfig, trust.PeerRole, io.Writer, io.Writer) (managedProcess, error) {
			return process, nil
		},
		socket: "/tmp/fusekit-runtime/socket", executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		library: testNativeLibrary, librarySHA256: testNativeDigest,
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
	want := "CGOFUSE_LIBFUSE_PATH=" + testNativeLibrary
	if len(matches) != 1 || matches[0] != want {
		t.Fatalf("native FUSE environment = %v, want [%q]", matches, want)
	}
}

func TestValidateNativeMountProofDerivesSourceFromPresentationRoot(t *testing.T) {
	for _, root := range []string{
		"/Users/yasyf/.cc-pool/accounts",
		"/private/tmp/mount",
		"/Volumes/other",
	} {
		t.Run(filepath.Base(root), func(t *testing.T) {
			if err := validateNativeMountProof(root, testNativeMountProof(root)); err != nil {
				t.Fatalf("validateNativeMountProof(%q): %v", root, err)
			}
		})
	}
}

func testNativeMountProof(root string) mountservice.NativeMountProof {
	source, err := mountproto.NativeMountSource(root)
	if err != nil {
		panic(err)
	}
	return mountservice.NativeMountProof{
		PresentationRoot: root,
		Filesystem:       mountproto.NativeMountFilesystem,
		Source:           source,
		RootReadEpoch:    1,
	}
}

func testNativeProbeToken() string { return strings.Repeat("a", 64) }

func testNativeMountIdentity(root string) mountservice.NativeMountIdentity {
	proof := testNativeMountProof(root)
	return mountservice.NativeMountIdentity{
		PresentationRoot: proof.PresentationRoot,
		Filesystem:       proof.Filesystem,
		Source:           proof.Source,
	}
}
