package holder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/internal/recoveryid"
	"github.com/yasyf/fusekit/sourceauthority"
)

type sourceProcessLauncher struct {
	owner      *processOwner
	executable string
	exec       daemonkit.Serving
	stderr     io.Writer
}

func (l sourceProcessLauncher) LaunchSourceObserver(
	ctx context.Context,
	spec sourceauthority.ObserverProcessSpec,
) (sourceauthority.ObserverProcess, error) {
	return l.launch(ctx, spec.Arguments, recoveryid.SourceObserver)
}

func (l sourceProcessLauncher) LaunchSourceTask(
	ctx context.Context,
	spec sourceauthority.SourceTaskProcessSpec,
) (sourceauthority.SourceTaskProcess, error) {
	return l.launch(ctx, spec.Arguments, recoveryid.SourceTask)
}

func (l sourceProcessLauncher) launch(
	ctx context.Context,
	arguments []string,
	recoveryID recoveryid.ID,
) (*sourceChildProcess, error) {
	if l.owner == nil {
		return nil, errors.New("FuseKit runtime: source child process owner is required")
	}
	if err := validateAbsolutePath("source child executable", l.executable); err != nil {
		return nil, err
	}
	if len(arguments) == 0 {
		return nil, errors.New("FuseKit runtime: source child arguments are required")
	}
	child, err := l.owner.spawn(ctx, managedSpawnConfig{
		id: recoveryID,
		cmd: daemonkit.Cmd{
			Path:    l.executable,
			Args:    append([]string(nil), arguments...),
			Env:     sanitizedChildEnvironment(os.Environ()),
			Session: true,
			Exec:    l.exec,
		},
		channel: daemonkit.ChannelHandoff,
	}, l.stderr)
	if err != nil {
		return nil, fmt.Errorf("FuseKit runtime: dispatch source child: %w", err)
	}
	return &sourceChildProcess{child: child, stopDone: make(chan struct{})}, nil
}

type sourceChildProcess struct {
	child *ownedChild

	mu      sync.Mutex
	stopped bool

	stopOnce sync.Once
	stopDone chan struct{}
	stopErr  error
}

func (p *sourceChildProcess) Business(
	ctx context.Context,
	contract daemonkit.Contract,
) (*daemonkit.Business, error) {
	return p.child.child.Business(ctx, contract)
}

func (p *sourceChildProcess) Child() *daemonkit.Child { return p.child.child }

func (p *sourceChildProcess) Wait(ctx context.Context) error {
	select {
	case <-p.child.settled:
	case <-ctx.Done():
		return ctx.Err()
	}
	exit, ok := p.child.Exit()
	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	var result error
	switch {
	case !ok:
		result = errors.New("FuseKit runtime: source child exit proof is unavailable")
	case stopped:
	case exit.Signal != 0:
		result = fmt.Errorf("FuseKit runtime: source child died by signal %d", exit.Signal)
	case exit.Code != 0:
		result = fmt.Errorf("FuseKit runtime: source child exited with status %d", exit.Code)
	}
	return errors.Join(result, ctx.Err())
}

func (p *sourceChildProcess) Stop(ctx context.Context) error {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.stopped = true
		p.mu.Unlock()
		go func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), childSettlementTimeout)
			_, stopErr := p.child.child.Stop(stopCtx)
			cancel()
			<-p.child.settled
			p.mu.Lock()
			p.stopErr = stopErr
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

var _ sourceauthority.ObserverProcessLauncher = sourceProcessLauncher{}
var _ sourceauthority.SourceTaskProcessLauncher = sourceProcessLauncher{}
var _ sourceauthority.ObserverProcess = (*sourceChildProcess)(nil)
var _ sourceauthority.SourceTaskProcess = (*sourceChildProcess)(nil)
