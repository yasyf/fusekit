package holder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/internal/recoveryid"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
)

// ErrNativeProcessUnavailable means the exact managed native child is not live.
var ErrNativeProcessUnavailable = errors.New("FuseKit runtime: native process unavailable")

type nativeController interface {
	mountmux.NativeRoot
	Bind(context.Context, mountservice.Identity) error
	Mounted(context.Context, mountservice.Identity, mountservice.NativeMountIdentity, string) error
	Ready(context.Context, mountservice.Identity, mountservice.NativeMountProof) error
	Unbind(mountservice.Identity)
	Settled(mountservice.Identity, error)
	HealthState() daemon.State
	RuntimeHealth(string) mountservice.RuntimeHealth
}

type nativeProcessConfig struct {
	prepare          processPrepare
	socket           string
	executable       string
	signature        proc.SignatureDigest
	library          string
	librarySHA256    string
	validateLibrary  func(string, string) error
	confirmMount     func(context.Context, string, string) error
	options          []string
	readinessTimeout time.Duration
	stdout           io.Writer
	stderr           io.Writer
}

type nativeProcessPhase uint8

const (
	nativeProcessIdle nativeProcessPhase = iota
	nativeProcessStarting
	nativeProcessLive
	nativeProcessFailed
	nativeProcessClosing
	nativeProcessClosed
)

type nativeProcess struct {
	config nativeProcessConfig

	mu            sync.Mutex
	phase         nativeProcessPhase
	record        proc.Record
	recordReady   chan struct{}
	startDone     chan struct{}
	recordSet     bool
	readyResult   chan error
	ready         bool
	probing       bool
	mounted       bool
	bound         *wireSession
	settling      chan struct{}
	settlement    *wireSession
	process       managedProcess
	failure       error
	root          string
	mountProof    *mountservice.NativeMountProof
	mountIdentity *mountservice.NativeMountIdentity
	probeID       string

	closeOnce   sync.Once
	processDone chan struct{}
	closeDone   chan struct{}
	closeErr    error
}

type wireSession struct {
	session *wire.AcceptedSession
	done    chan struct{}
	settled chan struct{}
}

func newNativeProcess(config nativeProcessConfig) *nativeProcess {
	if config.confirmMount == nil {
		config.confirmMount = func(context.Context, string, string) error { return mountmux.ErrNativeMount }
	}
	return &nativeProcess{
		config: config, phase: nativeProcessIdle,
		processDone: make(chan struct{}), closeDone: make(chan struct{}),
	}
}

func (n *nativeProcess) Start(ctx context.Context, root string, _ mountmux.Resolver) error {
	if n.config.validateLibrary != nil {
		if err := n.config.validateLibrary(n.config.library, n.config.librarySHA256); err != nil {
			return fmt.Errorf("FuseKit runtime: validate bundled fuse-t before native launch: %w", err)
		}
	}
	arguments, err := mountmux.NativeChildArguments(mountmux.NativeChildConfig{
		Socket: n.config.socket, Root: root, Library: n.config.library,
		LibrarySHA256: n.config.librarySHA256, Options: append([]string(nil), n.config.options...),
	})
	if err != nil {
		return err
	}
	n.mu.Lock()
	if n.phase != nativeProcessIdle {
		n.mu.Unlock()
		return fmt.Errorf("%w: start in phase %d", ErrNativeProcessUnavailable, n.phase)
	}
	n.setPhaseLocked(nativeProcessStarting)
	n.root = filepath.Clean(root)
	n.mountProof = nil
	n.mountIdentity = nil
	n.probeID = ""
	n.probing = false
	n.mounted = false
	n.recordReady = make(chan struct{})
	n.readyResult = make(chan error, 1)
	n.startDone = make(chan struct{})
	startDone := n.startDone
	n.mu.Unlock()
	defer close(startDone)

	stdoutMode := proc.StdioNull
	if n.config.stdout != nil {
		stdoutMode = proc.StdioPipe
	}
	stderrMode := proc.StdioNull
	if n.config.stderr != nil {
		stderrMode = proc.StdioPipe
	}
	process, err := n.config.prepare(ctx, proc.SpawnConfig{
		RecoveryID:        recoveryid.NativeMount,
		Executable:        n.config.executable,
		Args:              arguments,
		Env:               nativeEnvironment(os.Environ(), n.config.library),
		Stdin:             proc.StdioNull,
		Stdout:            stdoutMode,
		Stderr:            stderrMode,
		RequiresPeerFence: true,
		ExpectedSignature: &n.config.signature,
	}, NativeChildRole, n.config.stdout, n.config.stderr)
	if nilManagedValue(process) {
		process = nil
	}
	if err != nil {
		var stopErr error
		if process != nil {
			stopErr = process.Stop(context.Background())
		}
		n.awaitUnbound()
		resultErr := errors.Join(fmt.Errorf("FuseKit runtime: start native process: %w", err), stopErr)
		n.failStart(resultErr)
		return resultErr
	}
	if process == nil {
		resultErr := errors.New("FuseKit runtime: native process preparer returned no process")
		n.failStart(resultErr)
		return resultErr
	}
	if err := n.admitPreparedProcess(process); err != nil {
		stopErr := process.Stop(context.Background())
		resultErr := errors.Join(err, stopErr)
		n.failStart(resultErr)
		return resultErr
	}
	readyCtx := ctx
	var cancel context.CancelFunc
	if n.config.readinessTimeout > 0 {
		readyCtx, cancel = context.WithTimeout(ctx, n.config.readinessTimeout)
		defer cancel()
	}
	if err := process.Start(readyCtx); err != nil {
		stopErr := process.Stop(context.Background())
		n.awaitUnbound()
		resultErr := errors.Join(fmt.Errorf("FuseKit runtime: dispatch native process: %w", err), stopErr)
		n.failStart(resultErr)
		return resultErr
	}
	if err := n.awaitNativeReady(readyCtx); err != nil {
		stopErr := process.Stop(context.Background())
		n.awaitUnbound()
		resultErr := errors.Join(fmt.Errorf("FuseKit runtime: await native readiness: %w", err), stopErr)
		n.failStart(resultErr)
		return resultErr
	}

	n.mu.Lock()
	valid := n.phase == nativeProcessStarting && n.ready && n.bound != nil && n.record == process.Record()
	if valid {
		n.process = process
		n.setPhaseLocked(nativeProcessLive)
	}
	n.mu.Unlock()
	if !valid {
		stopErr := process.Stop(context.WithoutCancel(ctx))
		n.awaitUnbound()
		resultErr := errors.Join(ErrNativeProcessUnavailable, stopErr)
		n.failStart(resultErr)
		return resultErr
	}
	go n.watch(process)
	return nil
}

func (n *nativeProcess) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	n.closeOnce.Do(func() {
		go func() {
			err := n.shutdown()
			n.mu.Lock()
			n.closeErr = err
			n.mu.Unlock()
			close(n.closeDone)
		}()
	})
	<-n.processDone
	select {
	case <-n.closeDone:
		n.mu.Lock()
		closeErr := n.closeErr
		n.mu.Unlock()
		return errors.Join(closeErr, ctx.Err())
	default:
	}
	n.mu.Lock()
	pending := n.settlement != nil
	n.mu.Unlock()
	if !pending {
		<-n.closeDone
		n.mu.Lock()
		closeErr := n.closeErr
		n.mu.Unlock()
		return errors.Join(closeErr, ctx.Err())
	}
	select {
	case <-n.closeDone:
		n.mu.Lock()
		closeErr := n.closeErr
		n.mu.Unlock()
		return errors.Join(closeErr, ctx.Err())
	case <-ctx.Done():
		return fmt.Errorf("FuseKit runtime: close native process before resource settlement: %w", ctx.Err())
	}
}

func (n *nativeProcess) shutdown() error {
	n.mu.Lock()
	var startDone chan struct{}
	switch n.phase {
	case nativeProcessClosed:
		err := n.failure
		n.mu.Unlock()
		return err
	case nativeProcessIdle:
		n.setPhaseLocked(nativeProcessClosed)
		n.mu.Unlock()
		close(n.processDone)
		return nil
	default:
		if n.phase == nativeProcessStarting {
			startDone = n.startDone
		}
		n.setPhaseLocked(nativeProcessClosing)
	}
	n.mu.Unlock()
	if startDone != nil {
		<-startDone
	}

	var err error
	n.mu.Lock()
	process := n.process
	n.mu.Unlock()
	if process != nil {
		err = process.Stop(context.Background())
	}
	n.awaitUnbound()
	n.mu.Lock()
	n.process = nil
	n.record = proc.Record{}
	n.mu.Unlock()
	close(n.processDone)
	n.awaitSettlement()
	n.mu.Lock()
	err = errors.Join(err, n.failure)
	n.setPhaseLocked(nativeProcessClosed)
	n.mu.Unlock()
	return err
}

func (n *nativeProcess) Bind(ctx context.Context, identity mountservice.Identity) error {
	n.mu.Lock()
	if n.phase != nativeProcessStarting || n.recordReady == nil {
		n.mu.Unlock()
		return ErrNativeProcessUnavailable
	}
	recordReady := n.recordReady
	n.mu.Unlock()
	select {
	case <-recordReady:
	case <-ctx.Done():
		return ctx.Err()
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.phase != nativeProcessStarting || !n.recordSet || identity.Session == nil ||
		!identity.Peer.MatchesProcess(n.record) {
		return mountservice.ErrUnauthorized
	}
	if n.bound != nil || n.settling != nil || n.settlement != nil {
		return ErrNativeProcessUnavailable
	}
	n.bound = &wireSession{
		session: identity.Session, done: make(chan struct{}), settled: make(chan struct{}),
	}
	return nil
}

func (n *nativeProcess) Ready(
	_ context.Context,
	identity mountservice.Identity,
	proof mountservice.NativeMountProof,
) error {
	if n.config.validateLibrary != nil {
		if err := n.config.validateLibrary(n.config.library, n.config.librarySHA256); err != nil {
			return fmt.Errorf("FuseKit runtime: revalidate bundled fuse-t before readiness: %w", err)
		}
	}
	n.mu.Lock()
	if n.phase != nativeProcessStarting || !n.mounted || n.mountIdentity == nil ||
		n.bound == nil || n.bound.session != identity.Session ||
		!identity.Peer.MatchesProcess(n.record) {
		n.mu.Unlock()
		return mountservice.ErrUnauthorized
	}
	if err := validateNativeMountProof(n.root, proof); err != nil {
		n.mu.Unlock()
		return err
	}
	if *n.mountIdentity != (mountservice.NativeMountIdentity{
		PresentationRoot: proof.PresentationRoot,
		Filesystem:       proof.Filesystem,
		Source:           proof.Source,
	}) {
		n.mu.Unlock()
		return errors.New("FuseKit runtime: native readiness proof does not match mounted identity")
	}
	if n.ready {
		n.mu.Unlock()
		return ErrNativeProcessUnavailable
	}
	probeID := n.probeID
	n.ready = true
	n.mountProof = &proof
	result := n.readyResult
	n.mu.Unlock()
	writeHolderNativeReadinessEvent(n.config.stderr, "native_ready_admitted", probeID, "ok", proof.RootReadEpoch)
	result <- nil
	writeHolderNativeReadinessEvent(n.config.stderr, "native_ready_committed", probeID, "ok", proof.RootReadEpoch)
	return nil
}

func (n *nativeProcess) Mounted(
	ctx context.Context,
	identity mountservice.Identity,
	mount mountservice.NativeMountIdentity,
	probeToken string,
) error {
	n.mu.Lock()
	if n.phase != nativeProcessStarting || n.bound == nil || n.bound.session != identity.Session ||
		!identity.Peer.MatchesProcess(n.record) {
		n.mu.Unlock()
		return mountservice.ErrUnauthorized
	}
	if n.probing || n.mounted {
		n.mu.Unlock()
		return ErrNativeProcessUnavailable
	}
	if err := validateNativeMountIdentity(n.root, mount); err != nil {
		n.mu.Unlock()
		return err
	}
	if _, err := mountmux.NativeProbeChildArguments(mountmux.NativeProbeChildConfig{
		Root: n.root, Token: probeToken,
	}); err != nil {
		n.mu.Unlock()
		return err
	}
	probeID, err := mountmux.NativeProbeID(probeToken)
	if err != nil {
		n.mu.Unlock()
		return err
	}
	n.probing = true
	root := n.root
	confirm := n.config.confirmMount
	n.mu.Unlock()
	writeHolderNativeReadinessEvent(n.config.stderr, "native_mounted_admitted", probeID, "ok", 0)

	err = confirm(ctx, root, probeToken)

	n.mu.Lock()
	n.probing = false
	if err != nil {
		n.mu.Unlock()
		writeHolderNativeReadinessEvent(n.config.stderr, "native_mounted_probe", probeID, "error", 0)
		return fmt.Errorf("FuseKit runtime: external native mount proof: %w", err)
	}
	if n.phase != nativeProcessStarting || n.bound == nil || n.bound.session != identity.Session ||
		!identity.Peer.MatchesProcess(n.record) {
		n.mu.Unlock()
		return mountservice.ErrUnauthorized
	}
	n.mounted = true
	n.probeID = probeID
	value := mount
	n.mountIdentity = &value
	n.mu.Unlock()
	writeHolderNativeReadinessEvent(n.config.stderr, "native_mounted_probe", probeID, "ok", 0)
	return nil
}

func (n *nativeProcess) Unbind(identity mountservice.Identity) {
	n.mu.Lock()
	if n.bound == nil || n.bound.session != identity.Session {
		n.mu.Unlock()
		return
	}
	session := n.bound
	done := session.done
	n.bound = nil
	n.settling = done
	n.settlement = session
	process := n.process
	starting := n.phase == nativeProcessStarting
	if n.phase == nativeProcessStarting || n.phase == nativeProcessLive {
		n.setPhaseLocked(nativeProcessFailed)
		n.failure = fmt.Errorf("%w: session was lost", ErrNativeProcessUnavailable)
	}
	ready := n.ready
	result := n.readyResult
	n.mu.Unlock()
	if starting && !ready {
		result <- errors.New("FuseKit runtime: native process session closed before readiness")
	}
	if process != nil {
		stopErr := process.Stop(context.Background())
		n.mu.Lock()
		n.failure = errors.Join(n.failure, stopErr)
		n.mu.Unlock()
	}
	close(done)
	n.mu.Lock()
	if n.settling == done {
		n.settling = nil
	}
	n.mu.Unlock()
}

func (n *nativeProcess) Settled(identity mountservice.Identity, settlement error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.settlement == nil || n.settlement.session != identity.Session {
		return
	}
	settlementDone := n.settlement.settled
	n.settlement = nil
	if settlement != nil {
		n.failure = errors.Join(n.failure, fmt.Errorf("FuseKit runtime: native session settlement: %w", settlement))
	}
	close(settlementDone)
}

func (n *nativeProcess) HealthState() daemon.State {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch n.phase {
	case nativeProcessLive:
		return daemon.StateHealthy
	case nativeProcessFailed:
		return daemon.StateFailed
	default:
		return daemon.StateDegraded
	}
}

func (n *nativeProcess) RuntimeHealth(activationGeneration string) mountservice.RuntimeHealth {
	n.mu.Lock()
	defer n.mu.Unlock()
	health := mountservice.RuntimeHealth{
		ActivationGeneration: activationGeneration,
		NativePhase:          protocolNativePhase(n.phase),
	}
	if n.phase == nativeProcessLive && n.mountProof != nil {
		proof := *n.mountProof
		health.NativeMount = &proof
	}
	return health
}

func (n *nativeProcess) setPhaseLocked(phase nativeProcessPhase) {
	n.phase = phase
	if phase != nativeProcessLive {
		n.mountProof = nil
	}
}

func protocolNativePhase(phase nativeProcessPhase) mountproto.NativePhase {
	switch phase {
	case nativeProcessIdle:
		return mountproto.NativePhaseIdle
	case nativeProcessStarting:
		return mountproto.NativePhaseStarting
	case nativeProcessLive:
		return mountproto.NativePhaseLive
	case nativeProcessFailed:
		return mountproto.NativePhaseFailed
	case nativeProcessClosing:
		return mountproto.NativePhaseClosing
	case nativeProcessClosed:
		return mountproto.NativePhaseClosed
	default:
		panic(fmt.Sprintf("FuseKit runtime: invalid native process phase %d", phase))
	}
}

func validateNativeMountProof(root string, proof mountservice.NativeMountProof) error {
	if err := validateNativeMountIdentity(root, mountservice.NativeMountIdentity{
		PresentationRoot: proof.PresentationRoot,
		Filesystem:       proof.Filesystem,
		Source:           proof.Source,
	}); err != nil {
		return err
	}
	if proof.RootReadEpoch == 0 {
		return errors.New("FuseKit runtime: native readiness proof has no root-read through-proof")
	}
	return nil
}

func validateNativeMountIdentity(root string, mount mountservice.NativeMountIdentity) error {
	if filepath.Clean(mount.PresentationRoot) != root || mount.PresentationRoot != filepath.Clean(mount.PresentationRoot) {
		return errors.New("FuseKit runtime: native readiness proof names a different presentation root")
	}
	expectedSource, err := mountproto.NativeMountSource(root)
	if err != nil {
		return fmt.Errorf("FuseKit runtime: derive native mount source: %w", err)
	}
	if mount.Filesystem != mountproto.NativeMountFilesystem || mount.Source != expectedSource {
		return fmt.Errorf(
			"FuseKit runtime: native readiness proof has filesystem %q from %q, want %q from %q",
			mount.Filesystem, mount.Source, mountproto.NativeMountFilesystem, expectedSource,
		)
	}
	return nil
}

func (n *nativeProcess) admitPreparedProcess(process managedProcess) error {
	record := process.Record()
	if err := validateNativeProcessRecord(record); err != nil {
		return err
	}
	n.mu.Lock()
	if n.phase != nativeProcessStarting || n.recordSet {
		n.mu.Unlock()
		return ErrNativeProcessUnavailable
	}
	n.record = record
	n.recordSet = true
	close(n.recordReady)
	n.mu.Unlock()
	return nil
}

func (n *nativeProcess) awaitNativeReady(ctx context.Context) error {
	n.mu.Lock()
	if n.phase != nativeProcessStarting || !n.recordSet {
		n.mu.Unlock()
		return ErrNativeProcessUnavailable
	}
	result := n.readyResult
	n.mu.Unlock()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func validateNativeProcessRecord(record proc.Record) error {
	if err := record.Validate(); err != nil {
		return fmt.Errorf("FuseKit runtime: native process identity: %w", err)
	}
	if !record.ProcessGroup || record.SessionID != record.PID {
		return errors.New("FuseKit runtime: native process does not own its dedicated session")
	}
	return nil
}

func (n *nativeProcess) watch(process managedProcess) {
	<-process.Done()
	exit, ok := process.Exit()
	var err error
	if !ok {
		err = errors.New("FuseKit runtime: native process reaped without completion result")
	} else if exit.Error != "" {
		err = errors.New(exit.Error)
	} else if exit.Code != 0 {
		err = fmt.Errorf("FuseKit runtime: native process exited with status %d", exit.Code)
	}
	n.awaitUnbound()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.process != process {
		return
	}
	n.process = nil
	n.record = proc.Record{}
	if n.phase == nativeProcessLive {
		n.setPhaseLocked(nativeProcessFailed)
		n.failure = errors.Join(ErrNativeProcessUnavailable, err)
	}
}

func (n *nativeProcess) awaitUnbound() {
	for {
		n.mu.Lock()
		if n.bound == nil && n.settling == nil {
			n.mu.Unlock()
			return
		}
		done := n.settling
		if done == nil {
			done = n.bound.done
		}
		n.mu.Unlock()
		<-done
	}
}

func (n *nativeProcess) awaitSettlement() {
	for {
		n.mu.Lock()
		settlement := n.settlement
		n.mu.Unlock()
		if settlement == nil {
			return
		}
		<-settlement.settled
	}
}

func (n *nativeProcess) failStart(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.recordReady != nil && !n.recordSet {
		close(n.recordReady)
	}
	n.setPhaseLocked(nativeProcessFailed)
	n.record = proc.Record{}
	n.process = nil
	n.failure = errors.Join(n.failure, err)
}

func validateNativeExecutable(path string) error {
	if err := validateAbsolutePath("native executable", path); err != nil {
		return err
	}
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("FuseKit runtime: resolve current executable: %w", err)
	}
	if path != self {
		return fmt.Errorf("FuseKit runtime: native executable %q is not the current fixed app executable %q", path, self)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("FuseKit runtime: inspect native executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return errors.New("FuseKit runtime: native executable is not an executable regular file")
	}
	return nil
}

func validateAbsolutePath(name, path string) error {
	clean := filepath.Clean(path)
	if !filepath.IsAbs(path) || clean != path {
		return fmt.Errorf("FuseKit runtime: %s %q is not an exact absolute path", name, path)
	}
	return nil
}

func sanitizedChildEnvironment(environment []string) []string {
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "PATH" || key == "LANG" || key == "CGOFUSE_LIBFUSE_PATH" {
			continue
		}
		result = append(result, entry)
	}
	return result
}

func nativeEnvironment(environment []string, library string) []string {
	return append(sanitizedChildEnvironment(environment), "CGOFUSE_LIBFUSE_PATH="+library)
}

var _ nativeController = (*nativeProcess)(nil)
