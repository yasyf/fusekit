package holder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/sourceauthority"
)

type sourceProcessLauncher struct {
	manager    *proc.Manager
	executable string
	signature  proc.SignatureDigest
	stderr     io.Writer
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
	if l.manager == nil {
		return nil, errors.New("FuseKit runtime: source child process manager is required")
	}
	if err := validateAbsolutePath("source child executable", l.executable); err != nil {
		return nil, err
	}
	if len(arguments) == 0 {
		return nil, errors.New("FuseKit runtime: source child arguments are required")
	}
	stderrMode := proc.StdioNull
	if l.stderr != nil {
		stderrMode = proc.StdioPipe
	}
	request, err := proc.NewSpawnRequest(proc.SpawnConfig{
		RecoveryClass:     recoveryClass,
		Executable:        l.executable,
		Args:              append([]string(nil), arguments...),
		Env:               sanitizedChildEnvironment(os.Environ()),
		Stdin:             proc.StdioNull,
		Stdout:            proc.StdioNull,
		Stderr:            stderrMode,
		SpawnedSession:    true,
		ExpectedSignature: &l.signature,
	})
	if err != nil {
		return nil, fmt.Errorf("FuseKit runtime: construct source child request: %w", err)
	}
	child, receipt, err := l.manager.Prepare(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("FuseKit runtime: prepare source child: %w", err)
	}
	process := newSourceChildProcess(child)
	if l.stderr != nil {
		pipe, pipeErr := child.TakeStderr()
		if pipeErr != nil {
			return nil, errors.Join(pipeErr, process.Stop(context.Background()))
		}
		process.output = copySourceChildOutput(pipe, l.stderr)
	}
	if err := child.Start(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("FuseKit runtime: dispatch source child: %w", err), process.Stop(context.Background()))
	}
	endpoint, err := child.ClaimSpawnedSession(ctx, receipt)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("FuseKit runtime: claim source child session: %w", err), process.Stop(context.Background()))
	}
	process.endpoint = endpoint
	return process, nil
}

type sourceChildProcess struct {
	child    *proc.PreparedChild
	endpoint proc.SpawnedSessionEndpoint
	output   <-chan error

	mu           sync.Mutex
	claimed      bool
	terminalOnce sync.Once
	terminalDone chan struct{}
	terminalErr  error
	stopOnce     sync.Once
	stopDone     chan struct{}
	stopErr      error
}

func newSourceChildProcess(child *proc.PreparedChild) *sourceChildProcess {
	return &sourceChildProcess{
		child: child, terminalDone: make(chan struct{}), stopDone: make(chan struct{}),
	}
}

func (p *sourceChildProcess) SessionEndpoint(ctx context.Context) (proc.SpawnedSessionEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return proc.SpawnedSessionEndpoint{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.claimed {
		return proc.SpawnedSessionEndpoint{}, errors.New("FuseKit runtime: source child session already claimed")
	}
	p.claimed = true
	return p.endpoint, nil
}

func (p *sourceChildProcess) Wait(ctx context.Context) error {
	p.terminalOnce.Do(func() {
		go func() {
			<-p.child.Done()
			var result error
			if p.output != nil {
				result = <-p.output
			}
			exit, ok := p.child.Exit()
			if !ok {
				result = errors.Join(result, errors.New("FuseKit runtime: source child exit proof is unavailable"))
			} else if !exit.Stopped && exit.Code != 0 {
				result = errors.Join(result, fmt.Errorf("FuseKit runtime: source child exited with status %d", exit.Code))
			}
			p.mu.Lock()
			p.terminalErr = result
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
		go func() {
			stopErr := p.child.Stop(context.Background())
			terminalErr := p.Wait(context.Background())
			p.mu.Lock()
			p.stopErr = errors.Join(stopErr, terminalErr)
			p.mu.Unlock()
			close(p.stopDone)
		}()
	})
	select {
	case <-p.stopDone:
		p.mu.Lock()
		err := p.stopErr
		p.mu.Unlock()
		return errors.Join(err, ctx.Err())
	case <-ctx.Done():
		return ctx.Err()
	}
}

func copySourceChildOutput(pipe *os.File, destination io.Writer) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(destination, pipe)
		done <- errors.Join(copyErr, pipe.Close())
		close(done)
	}()
	return done
}

var _ sourceauthority.ObserverProcessLauncher = sourceProcessLauncher{}
var _ sourceauthority.SourceTaskProcessLauncher = sourceProcessLauncher{}
var _ sourceauthority.ObserverProcess = (*sourceChildProcess)(nil)
var _ sourceauthority.SourceTaskProcess = (*sourceChildProcess)(nil)
