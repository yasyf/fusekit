package holder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
)

type managedProcess interface {
	Record() proc.Record
	Start(context.Context) error
	Done() <-chan struct{}
	Exit() (proc.ProcessExit, bool)
	Stop(context.Context) error
}

type processPrepare func(
	context.Context,
	proc.SpawnConfig,
	trust.PeerRole,
	io.Writer,
	io.Writer,
) (managedProcess, error)

type processFence interface {
	Start(context.Context, *proc.PreparedChild) (*daemon.TrustedChild, error)
}

type processFenceArm func(proc.ProcessReceipt) (processFence, error)

type managedProcessPreparer struct {
	manager *proc.Manager
	arm     processFenceArm
}

type preparedManagedProcess struct {
	child   *proc.PreparedChild
	fence   processFence
	receipt proc.ProcessReceipt
	record  proc.Record
	role    trust.PeerRole
	done    <-chan struct{}
	pipes   []<-chan error
	outputs []*ownedProcessWriter

	mu            sync.Mutex
	started       bool
	settlementErr error
	settlement    chan struct{}
}

type ownedProcessWriter struct {
	io.Writer
	closer io.Closer
}

func (p managedProcessPreparer) Prepare(
	ctx context.Context,
	config proc.SpawnConfig,
	role trust.PeerRole,
	stdout io.Writer,
	stderr io.Writer,
) (managedProcess, error) {
	outputs := ownedProcessWriters(stdout, stderr)
	fail := func(cause error) (managedProcess, error) {
		return nil, errors.Join(cause, closeOwnedProcessWriters(outputs))
	}
	if p.manager == nil || p.arm == nil {
		return fail(errors.New("FuseKit runtime: managed process preparer is incomplete"))
	}
	if role == "" {
		return fail(errors.New("FuseKit runtime: managed process role is required"))
	}
	if config.Stdin != proc.StdioNull || (config.Stdout == proc.StdioPipe) != (stdout != nil) ||
		(config.Stderr == proc.StdioPipe) != (stderr != nil) {
		return fail(errors.New("FuseKit runtime: managed process stdio topology is inconsistent"))
	}
	request, err := proc.NewSpawnRequest(config)
	if err != nil {
		return fail(fmt.Errorf("FuseKit runtime: construct managed process request: %w", err))
	}
	child, receipt, err := p.manager.Prepare(ctx, request)
	if err != nil {
		return fail(fmt.Errorf("FuseKit runtime: prepare managed process: %w", err))
	}
	stopPrepared := func(cause error, pipes ...*os.File) (managedProcess, error) {
		for _, pipe := range pipes {
			if pipe != nil {
				_ = pipe.Close()
			}
		}
		return nil, errors.Join(cause, child.Stop(context.Background()), closeOwnedProcessWriters(outputs))
	}
	var stdoutPipe, stderrPipe *os.File
	if config.Stdout == proc.StdioPipe {
		stdoutPipe, err = child.TakeStdout()
		if err != nil {
			return stopPrepared(fmt.Errorf("FuseKit runtime: take managed stdout: %w", err))
		}
	}
	if config.Stderr == proc.StdioPipe {
		stderrPipe, err = child.TakeStderr()
		if err != nil {
			return stopPrepared(fmt.Errorf("FuseKit runtime: take managed stderr: %w", err), stdoutPipe)
		}
	}
	fence, err := p.arm(receipt)
	if err != nil {
		return stopPrepared(fmt.Errorf("FuseKit runtime: arm managed process fence: %w", err), stdoutPipe, stderrPipe)
	}
	record, err := managedProcessRecord(receipt, config.RecoveryClass)
	if err != nil {
		return stopPrepared(err, stdoutPipe, stderrPipe)
	}
	process := &preparedManagedProcess{
		child: child, fence: fence, receipt: receipt, record: record, role: role,
		done: child.Done(), outputs: outputs, settlement: make(chan struct{}),
	}
	if stdoutPipe != nil {
		process.pipes = append(process.pipes, copyProcessPipe(stdoutPipe, stdout))
	}
	if stderrPipe != nil {
		process.pipes = append(process.pipes, copyProcessPipe(stderrPipe, stderr))
	}
	go process.settleOutputs()
	return process, nil
}

func ownedProcessWriters(writers ...io.Writer) []*ownedProcessWriter {
	seen := make(map[*ownedProcessWriter]struct{})
	var result []*ownedProcessWriter
	for _, writer := range writers {
		owned, ok := writer.(*ownedProcessWriter)
		if !ok || owned == nil {
			continue
		}
		if _, exists := seen[owned]; exists {
			continue
		}
		seen[owned] = struct{}{}
		result = append(result, owned)
	}
	return result
}

func closeOwnedProcessWriters(writers []*ownedProcessWriter) error {
	var result error
	for _, writer := range writers {
		if writer != nil && writer.closer != nil {
			result = errors.Join(result, writer.closer.Close())
		}
	}
	return result
}

func managedProcessRecord(receipt proc.ProcessReceipt, class proc.RecoveryClass) (proc.Record, error) {
	identity := receipt.ProcessIdentity()
	record := proc.Record{
		RecoveryClass: class,
		PID:           identity.PID,
		StartTime:     identity.StartTime,
		Boot:          identity.Boot,
		Comm:          identity.Comm,
		Executable:    identity.Executable,
		AuditToken:    identity.AuditToken,
		Generation:    receipt.OwnerGeneration(),
		ProcessGroup:  true,
		SessionID:     identity.PID,
	}
	if err := record.Validate(); err != nil {
		return proc.Record{}, fmt.Errorf("FuseKit runtime: validate prepared process record: %w", err)
	}
	return record, nil
}

func copyProcessPipe(pipe *os.File, destination io.Writer) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(destination, pipe)
		done <- errors.Join(copyErr, pipe.Close())
		close(done)
	}()
	return done
}

func (p *preparedManagedProcess) Record() proc.Record { return p.record }

func (p *preparedManagedProcess) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return proc.ErrChildStarted
	}
	p.started = true
	p.mu.Unlock()
	trusted, err := p.fence.Start(ctx, p.child)
	if err != nil {
		return err
	}
	if trusted.ProcessIdentity() != p.receipt.ProcessIdentity() ||
		trusted.RequestDigest() != p.receipt.RequestDigest() ||
		trusted.Executable() != p.receipt.ExpectedExecutable() {
		return errors.Join(daemon.ErrFenceMismatch, p.child.Stop(context.Background()))
	}
	expectedSignature, ok := p.receipt.ExpectedSignature()
	if !ok || trusted.SignatureDigest() != expectedSignature {
		return errors.Join(daemon.ErrFenceMismatch, p.child.Stop(context.Background()))
	}
	if trusted.Role() != p.role {
		return errors.Join(daemon.ErrFenceMismatch, p.child.Stop(context.Background()))
	}
	return nil
}

func (p *preparedManagedProcess) Done() <-chan struct{} { return p.settlement }

func (p *preparedManagedProcess) Exit() (proc.ProcessExit, bool) { return p.child.Exit() }

func (p *preparedManagedProcess) Stop(ctx context.Context) error {
	err := p.child.Stop(ctx)
	if err != nil {
		return err
	}
	select {
	case <-p.settlement:
		p.mu.Lock()
		settlementErr := p.settlementErr
		p.mu.Unlock()
		return errors.Join(err, settlementErr)
	case <-ctx.Done():
		return errors.Join(err, proc.ErrChildSettlementIncomplete, ctx.Err())
	}
}

func (p *preparedManagedProcess) settleOutputs() {
	<-p.done
	var result error
	for _, done := range p.pipes {
		result = errors.Join(result, <-done)
	}
	result = errors.Join(result, closeOwnedProcessWriters(p.outputs))
	p.mu.Lock()
	p.settlementErr = result
	p.mu.Unlock()
	close(p.settlement)
}
