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
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
)

// ErrNativeProcessUnavailable means the exact managed native child is not live.
var ErrNativeProcessUnavailable = errors.New("holder: native process unavailable")

type nativeController interface {
	mountmux.NativeRoot
	Bind(context.Context, mountservice.Identity) error
	Ready(context.Context, mountservice.Identity, mountservice.NativeMountProof) error
	Unbind(mountservice.Identity)
	Settled(mountservice.Identity, error)
	HealthState() daemon.State
	RuntimeHealth(string) mountservice.RuntimeHealth
}

type managedProcess interface {
	Record() proc.Record
	Wait(context.Context) error
	Stop(context.Context) error
}

type nativeProcessConfig struct {
	start            func(context.Context, supervise.ProcessSpec) (managedProcess, error)
	socket           string
	executable       string
	library          string
	librarySHA256    string
	validateLibrary  func(string, string) error
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

	mu          sync.Mutex
	phase       nativeProcessPhase
	record      proc.Record
	recordReady chan struct{}
	startDone   chan struct{}
	recordSet   bool
	readyResult chan error
	ready       bool
	bound       *wireSession
	settling    chan struct{}
	settlement  *wireSession
	process     managedProcess
	failure     error
	root        string
	mountProof  *mountservice.NativeMountProof

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
	return &nativeProcess{
		config: config, phase: nativeProcessIdle,
		processDone: make(chan struct{}), closeDone: make(chan struct{}),
	}
}

func (n *nativeProcess) Start(ctx context.Context, root string, _ mountmux.Resolver) error {
	if n.config.validateLibrary != nil {
		if err := n.config.validateLibrary(n.config.library, n.config.librarySHA256); err != nil {
			return fmt.Errorf("holder: validate bundled fuse-t before native launch: %w", err)
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
	n.phase = nativeProcessStarting
	n.root = filepath.Clean(root)
	n.mountProof = nil
	n.recordReady = make(chan struct{})
	n.readyResult = make(chan error, 1)
	n.startDone = make(chan struct{})
	startDone := n.startDone
	n.mu.Unlock()
	defer close(startDone)

	process, err := n.config.start(ctx, supervise.ProcessSpec{
		Path: n.config.executable, Args: arguments, Env: nativeEnvironment(os.Environ(), n.config.library),
		Stdout: n.config.stdout, Stderr: n.config.stderr,
		RecoveryClass: proc.RecoveryNativeMount,
		Ready:         n.awaitReady, ReadinessTimeout: n.config.readinessTimeout,
	})
	if nilManagedValue(process) {
		process = nil
	}
	if err != nil {
		var stopErr error
		if process != nil {
			stopErr = process.Stop(context.Background())
		}
		n.awaitUnbound()
		resultErr := errors.Join(fmt.Errorf("holder: start native process: %w", err), stopErr)
		n.failStart(resultErr)
		return resultErr
	}
	if process == nil {
		resultErr := errors.New("holder: native process starter returned no process")
		n.failStart(resultErr)
		return resultErr
	}

	n.mu.Lock()
	valid := n.phase == nativeProcessStarting && n.ready && n.bound != nil && n.record == process.Record()
	if valid {
		n.process = process
		n.phase = nativeProcessLive
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
		return fmt.Errorf("holder: close native process before resource settlement: %w", ctx.Err())
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
		n.phase = nativeProcessClosed
		n.mu.Unlock()
		close(n.processDone)
		return nil
	default:
		if n.phase == nativeProcessStarting {
			startDone = n.startDone
		}
		n.phase = nativeProcessClosing
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
	n.phase = nativeProcessClosed
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
			return fmt.Errorf("holder: revalidate bundled fuse-t before readiness: %w", err)
		}
	}
	n.mu.Lock()
	if n.phase != nativeProcessStarting || n.bound == nil || n.bound.session != identity.Session ||
		!identity.Peer.MatchesProcess(n.record) {
		n.mu.Unlock()
		return mountservice.ErrUnauthorized
	}
	if err := validateNativeMountProof(n.root, proof); err != nil {
		n.mu.Unlock()
		return err
	}
	if n.ready {
		n.mu.Unlock()
		return ErrNativeProcessUnavailable
	}
	n.ready = true
	n.mountProof = &proof
	result := n.readyResult
	n.mu.Unlock()
	result <- nil
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
		n.phase = nativeProcessFailed
		n.failure = fmt.Errorf("%w: session was lost", ErrNativeProcessUnavailable)
	}
	ready := n.ready
	result := n.readyResult
	n.mu.Unlock()
	if starting && !ready {
		result <- errors.New("holder: native process session closed before readiness")
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
		n.failure = errors.Join(n.failure, fmt.Errorf("holder: native session settlement: %w", settlement))
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
	if n.mountProof != nil {
		proof := *n.mountProof
		health.NativeMount = &proof
	}
	return health
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
		panic(fmt.Sprintf("holder: invalid native process phase %d", phase))
	}
}

func validateNativeMountProof(root string, proof mountservice.NativeMountProof) error {
	if filepath.Clean(proof.PresentationRoot) != root || proof.PresentationRoot != filepath.Clean(proof.PresentationRoot) {
		return errors.New("holder: native readiness proof names a different presentation root")
	}
	expectedSource, err := mountproto.NativeMountSource(root)
	if err != nil {
		return fmt.Errorf("holder: derive native mount source: %w", err)
	}
	if proof.Filesystem != mountproto.NativeMountFilesystem || proof.Source != expectedSource {
		return fmt.Errorf(
			"holder: native readiness proof has filesystem %q from %q, want %q from %q",
			proof.Filesystem, proof.Source, mountproto.NativeMountFilesystem, expectedSource,
		)
	}
	if proof.CatalogEpoch == 0 {
		return errors.New("holder: native readiness proof has no catalog through-proof")
	}
	return nil
}

func (n *nativeProcess) awaitReady(ctx context.Context, record proc.Record) error {
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
		return fmt.Errorf("holder: native process identity: %w", err)
	}
	if !record.ProcessGroup || record.SessionID != record.PID {
		return errors.New("holder: native process does not own its dedicated session")
	}
	return nil
}

func (n *nativeProcess) watch(process managedProcess) {
	err := process.Wait(context.Background())
	n.awaitUnbound()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.process != process {
		return
	}
	n.process = nil
	n.record = proc.Record{}
	if n.phase == nativeProcessLive {
		n.phase = nativeProcessFailed
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
	n.phase = nativeProcessFailed
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
		return fmt.Errorf("holder: resolve current executable: %w", err)
	}
	if path != self {
		return fmt.Errorf("holder: native executable %q is not the current fixed app executable %q", path, self)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("holder: inspect native executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return errors.New("holder: native executable is not an executable regular file")
	}
	return nil
}

func validateAbsolutePath(name, path string) error {
	clean := filepath.Clean(path)
	if !filepath.IsAbs(path) || clean != path {
		return fmt.Errorf("holder: %s %q is not an exact absolute path", name, path)
	}
	return nil
}

func sanitizedChildEnvironment(environment []string) []string {
	const key = "CGOFUSE_LIBFUSE_PATH="
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		if !strings.HasPrefix(entry, key) {
			result = append(result, entry)
		}
	}
	return result
}

func nativeEnvironment(environment []string, library string) []string {
	return append(sanitizedChildEnvironment(environment), "CGOFUSE_LIBFUSE_PATH="+library)
}

var _ nativeController = (*nativeProcess)(nil)
