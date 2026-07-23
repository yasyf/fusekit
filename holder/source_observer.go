package holder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/fusekit/sourceauthority"
)

type managedSessionProcess interface {
	managedProcess
	Conn() net.Conn
}

type sourceProcessLauncher struct {
	startSession     func(context.Context, supervise.SessionProcessSpec) (managedSessionProcess, error)
	executable       string
	readinessTimeout time.Duration
	stderr           io.Writer
}

func (l sourceProcessLauncher) LaunchSourceObserver(
	ctx context.Context,
	spec sourceauthority.ObserverProcessSpec,
) (sourceauthority.ObserverProcess, error) {
	return l.launch(ctx, spec.Arguments, proc.RecoveryObserver)
}

func (l sourceProcessLauncher) LaunchSourceTask(
	ctx context.Context,
	spec sourceauthority.SourceTaskProcessSpec,
) (sourceauthority.SourceTaskProcess, error) {
	return l.launch(ctx, spec.Arguments, proc.RecoveryTask)
}

func (l sourceProcessLauncher) launch(
	ctx context.Context,
	arguments []string,
	recoveryClass proc.RecoveryClass,
) (*sourceChildProcess, error) {
	if l.startSession == nil {
		return nil, errors.New("FuseKit runtime: source child process starter is required")
	}
	if err := validateAbsolutePath("source child executable", l.executable); err != nil {
		return nil, err
	}
	if len(arguments) == 0 {
		return nil, errors.New("FuseKit runtime: source child arguments are required")
	}
	process, err := l.startSession(ctx, supervise.SessionProcessSpec{
		Path: l.executable, Args: append([]string(nil), arguments...),
		Env: sanitizedChildEnvironment(os.Environ()), Stderr: l.stderr,
		RecoveryClass:    recoveryClass,
		ReadinessTimeout: l.readinessTimeout,
	})
	var child *sourceChildProcess
	if !nilManagedValue(process) {
		child = newSourceChildProcess(process)
	}
	if err != nil {
		var settlementErr error
		if child != nil {
			settlementErr = child.Stop(context.Background())
		}
		return nil, errors.Join(fmt.Errorf("FuseKit runtime: start source child: %w", err), settlementErr)
	}
	if child == nil {
		return nil, errors.New("FuseKit runtime: source child starter returned no process")
	}
	if err := validateSourceProcessRecord(process.Record()); err != nil {
		return nil, errors.Join(
			err,
			child.Stop(context.Background()),
		)
	}
	return child, nil
}

type sourceChildProcess struct {
	process managedSessionProcess

	mu           sync.Mutex
	claimed      bool
	terminalOnce sync.Once
	terminalDone chan struct{}
	terminalErr  error
	stopOnce     sync.Once
	stopDone     chan struct{}
	stopErr      error
}

func newSourceChildProcess(process managedSessionProcess) *sourceChildProcess {
	return &sourceChildProcess{
		process:      process,
		terminalDone: make(chan struct{}),
		stopDone:     make(chan struct{}),
	}
}

func (p *sourceChildProcess) Dial(ctx context.Context) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.claimed {
		return nil, errors.New("FuseKit runtime: source child session already claimed")
	}
	conn := p.process.Conn()
	if conn == nil {
		return nil, errors.New("FuseKit runtime: source child returned no managed session")
	}
	p.claimed = true
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
		return fmt.Errorf("FuseKit runtime: source child process identity: %w", err)
	}
	if !record.ProcessGroup || record.SessionID != record.PID {
		return errors.New("FuseKit runtime: source child process does not own its dedicated session")
	}
	return nil
}

var _ sourceauthority.ObserverProcessLauncher = sourceProcessLauncher{}
var _ sourceauthority.SourceTaskProcessLauncher = sourceProcessLauncher{}
var _ sourceauthority.ObserverProcess = (*sourceChildProcess)(nil)
var _ sourceauthority.SourceTaskProcess = (*sourceChildProcess)(nil)
