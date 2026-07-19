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
	"github.com/yasyf/fusekit/fuset"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountservice"
)

// ErrNativeProcessUnavailable means the exact managed native child is not live.
var ErrNativeProcessUnavailable = errors.New("holder: native process unavailable")

type nativeController interface {
	mountmux.NativeRoot
	Bind(context.Context, mountservice.Identity) error
	Ready(context.Context, mountservice.Identity) error
	Unbind(mountservice.Identity)
	HealthState() daemon.State
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
	recordSet   bool
	readyResult chan error
	ready       bool
	bound       *wireSession
	settling    chan struct{}
	process     managedProcess
	failure     error
}

type wireSession struct {
	session *wire.AcceptedSession
	done    chan struct{}
}

func newNativeProcess(config nativeProcessConfig) *nativeProcess {
	return &nativeProcess{config: config, phase: nativeProcessIdle}
}

func (n *nativeProcess) Start(ctx context.Context, root string, _ mountmux.Resolver) error {
	arguments, err := mountmux.NativeChildArguments(mountmux.NativeChildConfig{
		Socket: n.config.socket, Root: root, Options: append([]string(nil), n.config.options...),
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
	n.recordReady = make(chan struct{})
	n.readyResult = make(chan error, 1)
	n.mu.Unlock()

	process, err := n.config.start(ctx, supervise.ProcessSpec{
		Path: n.config.executable, Args: arguments, Env: nativeEnvironment(os.Environ()),
		Stdout: n.config.stdout, Stderr: n.config.stderr,
		Ready: n.awaitReady, ReadinessTimeout: n.config.readinessTimeout,
	})
	if err != nil {
		n.awaitUnbound()
		n.failStart()
		return fmt.Errorf("holder: start native process: %w", err)
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
		n.failStart()
		return errors.Join(ErrNativeProcessUnavailable, stopErr)
	}
	go n.watch(process)
	return nil
}

func (n *nativeProcess) Close(ctx context.Context) error {
	n.mu.Lock()
	switch n.phase {
	case nativeProcessClosed:
		n.mu.Unlock()
		return nil
	case nativeProcessIdle:
		n.phase = nativeProcessClosed
		n.mu.Unlock()
		return nil
	default:
		n.phase = nativeProcessClosing
	}
	process := n.process
	n.mu.Unlock()

	var err error
	if process != nil {
		err = process.Stop(ctx)
	}
	n.awaitUnbound()
	n.mu.Lock()
	err = errors.Join(err, n.failure)
	n.process = nil
	n.record = proc.Record{}
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
	if n.bound != nil || n.settling != nil {
		return ErrNativeProcessUnavailable
	}
	n.bound = &wireSession{session: identity.Session, done: make(chan struct{})}
	return nil
}

func (n *nativeProcess) Ready(_ context.Context, identity mountservice.Identity) error {
	n.mu.Lock()
	if n.phase != nativeProcessStarting || n.bound == nil || n.bound.session != identity.Session ||
		!identity.Peer.MatchesProcess(n.record) {
		n.mu.Unlock()
		return mountservice.ErrUnauthorized
	}
	if n.ready {
		n.mu.Unlock()
		return ErrNativeProcessUnavailable
	}
	n.ready = true
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
	done := n.bound.done
	n.bound = nil
	n.settling = done
	process := n.process
	starting := n.phase == nativeProcessStarting
	if n.phase == nativeProcessLive {
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

func (n *nativeProcess) failStart() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.recordReady != nil && !n.recordSet {
		close(n.recordReady)
	}
	n.phase = nativeProcessFailed
	n.record = proc.Record{}
	n.process = nil
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

func nativeEnvironment(environment []string) []string {
	const key = "CGOFUSE_LIBFUSE_PATH="
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, key) {
			result = append(result, entry)
		}
	}
	return append(result, key+fuset.Dylib)
}

var _ nativeController = (*nativeProcess)(nil)
