package holder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/sourceauthority"
)

const observerSocketPollInterval = 10 * time.Millisecond

type sourceProcessLauncher struct {
	start            func(context.Context, supervise.ProcessSpec) (managedProcess, error)
	executable       string
	readinessTimeout time.Duration
	stdout           io.Writer
	stderr           io.Writer
}

func (l sourceProcessLauncher) LaunchSourceObserver(
	ctx context.Context,
	spec sourceauthority.ObserverProcessSpec,
) (sourceauthority.ObserverProcess, error) {
	return l.launch(ctx, spec.Socket, spec.Arguments, proc.RecoveryObserver)
}

func (l sourceProcessLauncher) LaunchSourceTask(
	ctx context.Context,
	spec sourceauthority.SourceTaskProcessSpec,
) (sourceauthority.SourceTaskProcess, error) {
	return l.launch(ctx, spec.Socket, spec.Arguments, proc.RecoveryTask)
}

func (l sourceProcessLauncher) launch(
	ctx context.Context,
	socket string,
	arguments []string,
	recoveryClass proc.RecoveryClass,
) (*sourceChildProcess, error) {
	if l.start == nil {
		return nil, errors.New("holder: source child process starter is required")
	}
	if err := validateAbsolutePath("source child executable", l.executable); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(socket) || filepath.Clean(socket) != socket {
		return nil, errors.New("holder: source child socket path is invalid")
	}
	if len(arguments) == 0 {
		return nil, errors.New("holder: source child arguments are required")
	}
	var (
		readyMu     sync.Mutex
		readyRecord proc.Record
		readySet    bool
	)
	process, err := l.start(ctx, supervise.ProcessSpec{
		Path: l.executable, Args: append([]string(nil), arguments...),
		Env: sanitizedChildEnvironment(os.Environ()), Stdout: l.stdout, Stderr: l.stderr,
		RecoveryClass:    recoveryClass,
		ReadinessTimeout: l.readinessTimeout,
		Ready: func(ctx context.Context, record proc.Record) error {
			if err := validateSourceProcessRecord(record); err != nil {
				return err
			}
			if err := waitForSourceSocket(ctx, socket); err != nil {
				return err
			}
			readyMu.Lock()
			readyRecord = record
			readySet = true
			readyMu.Unlock()
			return nil
		},
	})
	var child *sourceChildProcess
	if !nilManagedValue(process) {
		child = newSourceChildProcess(process, func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		})
	}
	if err != nil {
		var settlementErr error
		if child != nil {
			settlementErr = child.Stop(context.Background())
		}
		return nil, errors.Join(fmt.Errorf("holder: start source child: %w", err), settlementErr)
	}
	if child == nil {
		return nil, errors.New("holder: source child starter returned no process")
	}
	readyMu.Lock()
	publishedRecord, published := readyRecord, readySet
	readyMu.Unlock()
	if !published || process.Record() != publishedRecord {
		return nil, errors.Join(
			errors.New("holder: source child process identity changed during readiness"),
			child.Stop(context.Background()),
		)
	}
	return child, nil
}

type sourceChildProcess struct {
	process managedProcess
	dial    func(context.Context) (net.Conn, error)

	mu           sync.Mutex
	terminalOnce sync.Once
	terminalDone chan struct{}
	terminalErr  error
	stopOnce     sync.Once
	stopDone     chan struct{}
	stopErr      error
}

func newSourceChildProcess(
	process managedProcess,
	dial func(context.Context) (net.Conn, error),
) *sourceChildProcess {
	return &sourceChildProcess{
		process: process, dial: dial,
		terminalDone: make(chan struct{}),
		stopDone:     make(chan struct{}),
	}
}

func (p *sourceChildProcess) Dial(ctx context.Context) (net.Conn, error) {
	conn, err := p.dial(ctx)
	if err != nil {
		return nil, err
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("holder: source child returned a non-Unix connection")
	}
	peer, err := wire.PeerFromConn(unixConn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("holder: authenticate source child peer: %w", err)
	}
	if !peer.MatchesProcess(p.process.Record()) {
		_ = conn.Close()
		return nil, errors.New("holder: source child process identity mismatch")
	}
	return conn, nil
}

func (p *sourceChildProcess) Wait(ctx context.Context) error {
	p.terminalOnce.Do(func() {
		go func() {
			err := p.process.Wait(context.Background())
			p.mu.Lock()
			p.terminalErr = err
			p.mu.Unlock()
			close(p.terminalDone)
		}()
	})
	select {
	case <-p.terminalDone:
		p.mu.Lock()
		err := p.terminalErr
		p.mu.Unlock()
		return errors.Join(err, ctx.Err())
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *sourceChildProcess) Stop(ctx context.Context) error {
	p.stopOnce.Do(func() {
		stopErr := p.process.Stop(context.Background())
		terminalErr := p.Wait(context.Background())
		p.mu.Lock()
		p.stopErr = errors.Join(stopErr, terminalErr)
		p.mu.Unlock()
		close(p.stopDone)
	})
	<-p.stopDone
	p.mu.Lock()
	err := p.stopErr
	p.mu.Unlock()
	return errors.Join(err, ctx.Err())
}

func validateSourceProcessRecord(record proc.Record) error {
	if err := record.Validate(); err != nil {
		return fmt.Errorf("holder: source child process identity: %w", err)
	}
	if !record.ProcessGroup || record.SessionID != record.PID {
		return errors.New("holder: source child process does not own its dedicated session")
	}
	return nil
}

func waitForSourceSocket(ctx context.Context, socket string) error {
	for {
		info, err := os.Lstat(socket)
		switch {
		case err == nil && info.Mode()&os.ModeSocket != 0:
			return nil
		case err == nil:
			return errors.New("holder: source child readiness path is not a Unix socket")
		case !errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("holder: inspect source child socket: %w", err)
		}
		timer := time.NewTimer(observerSocketPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

var _ sourceauthority.ObserverProcessLauncher = sourceProcessLauncher{}
var _ sourceauthority.SourceTaskProcessLauncher = sourceProcessLauncher{}
var _ sourceauthority.ObserverProcess = (*sourceChildProcess)(nil)
var _ sourceauthority.SourceTaskProcess = (*sourceChildProcess)(nil)
