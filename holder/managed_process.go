package holder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

// childSettlementTimeout is the whole budget one child teardown is worth. Every
// daemonkit verb these teardowns reach refuses a context carrying no deadline,
// and the callers that reach them carry none — a daemon's lifetime context, or
// a spawn cleanup's stripped by context.WithoutCancel — so each teardown states
// this budget itself rather than passing the caller's context through.
const childSettlementTimeout = 10 * time.Second

// spawnTimeout is the whole budget one supervised spawn is worth — slot
// admission, the suspended spawn, and the durable record — when the caller
// states no deadline of its own. daemonkit's Spawn refuses a context carrying
// none, and the broker relaunch loop reaches it on exactly that.
const spawnTimeout = 30 * time.Second

// budgeted states budget as ctx's deadline when ctx carries none. A caller that
// stated its own keeps it: the budget is this package's default, never an
// override of a deadline the caller chose.
func budgeted(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	if _, stated := ctx.Deadline(); stated {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, budget)
}

// ownedSpawner is the supervised-child lane. daemonkit.Ctx and
// *daemonkit.Owned share it, the same seam shape as workerRunner.
type ownedSpawner interface {
	Spawn(context.Context, daemonkit.Cmd, daemonkit.Channel, io.Writer) (*daemonkit.Child, error)
}

// workerSlots is the fusekit-owned counting semaphore that replaces the
// withdrawn pool capacities: spawn and run admission blocks on a slot and
// honors the caller's deadline.
type workerSlots struct{ slots chan struct{} }

func newWorkerSlots(limit int) *workerSlots {
	return &workerSlots{slots: make(chan struct{}, limit)}
}

func (s *workerSlots) acquire(ctx context.Context) (func(), error) {
	select {
	case s.slots <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-s.slots }) }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("FuseKit runtime: acquire worker slot: %w", ctx.Err())
	}
}

// processOwner supervises every holder-spawned child: admission through the
// fusekit-owned worker reservations, durable identity in the process ledger,
// and settlement that untracks the record once the exit is observed.
type processOwner struct {
	spawner    ownedSpawner
	runner     workerRunner
	spawnSlots *workerSlots
	runSlots   *workerSlots
	ledger     *processLedger
	settling   sync.WaitGroup

	settledMu   sync.Mutex
	settledErrs []error
}

// waitSettled joins every in-flight child settlement and returns what their
// durable untracking cost. Each settlement writes the process ledger, so the
// runtime can neither report itself settled before they land nor call itself
// clean when one of them failed.
func (o *processOwner) waitSettled() error {
	o.settling.Wait()
	o.settledMu.Lock()
	defer o.settledMu.Unlock()
	return errors.Join(o.settledErrs...)
}

// Run is the disposable-command lane.
func (o *processOwner) Run(ctx context.Context, cmd daemonkit.Cmd) (daemonkit.RunResult, error) {
	release, err := o.runSlots.acquire(ctx)
	if err != nil {
		return daemonkit.RunResult{}, err
	}
	defer release()
	return o.runner.Run(ctx, cmd)
}

type managedSpawnConfig struct {
	id      recoveryid.ID
	cmd     daemonkit.Cmd
	channel daemonkit.Channel
}

func (o *processOwner) spawn(
	ctx context.Context,
	config managedSpawnConfig,
	stderr io.Writer,
) (*ownedChild, error) {
	ctx, cancel := budgeted(ctx, spawnTimeout)
	defer cancel()
	release, err := o.spawnSlots.acquire(ctx)
	if err != nil {
		return nil, err
	}
	child, err := o.spawner.Spawn(ctx, config.cmd, config.channel, stderr)
	if err != nil {
		release()
		return nil, fmt.Errorf("FuseKit runtime: spawn supervised child: %w", err)
	}
	owned := &ownedChild{child: child, release: release, settled: make(chan struct{})}
	record, err := captureProcessRecord(
		child.PID(), config.cmd.Path, config.id, o.ledger.Generation(), config.cmd.Session,
	)
	switch {
	case errors.Is(err, errNoProcess):
		// The child settled before capture: nothing durable can be at risk, and
		// the exit remains observable through Done.
	case err != nil:
		stopErr := stopAbandonedChild(ctx, child)
		release()
		return nil, errors.Join(err, stopErr)
	default:
		if trackErr := o.ledger.Track(record); trackErr != nil {
			stopErr := stopAbandonedChild(ctx, child)
			release()
			return nil, errors.Join(trackErr, stopErr)
		}
		owned.record = record
		owned.tracked = true
	}
	o.settling.Add(1)
	go func() {
		defer o.settling.Done()
		owned.settle(o.ledger)
		owned.mu.Lock()
		settleErr := owned.settleErr
		owned.mu.Unlock()
		if settleErr == nil {
			return
		}
		o.settledMu.Lock()
		o.settledErrs = append(o.settledErrs, settleErr)
		o.settledMu.Unlock()
	}()
	return owned, nil
}

// stopAbandonedChild settles a child whose durable record never landed. The
// stop must outlive the caller's cancellation, and context.WithoutCancel strips
// the deadline along with it, so the settlement budget is stated fresh.
func stopAbandonedChild(ctx context.Context, child *daemonkit.Child) error {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), childSettlementTimeout)
	defer cancel()
	_, err := child.Stop(stopCtx)
	return err
}

// spawnerFor adapts the owner to one recovery barrier, satisfying
// catalogworker.Spawner and any consumer that only needs raw children.
func (o *processOwner) spawnerFor(id recoveryid.ID) *recoverySpawner {
	return &recoverySpawner{owner: o, id: id}
}

type recoverySpawner struct {
	owner *processOwner
	id    recoveryid.ID
}

func (s *recoverySpawner) Spawn(
	ctx context.Context,
	cmd daemonkit.Cmd,
	channel daemonkit.Channel,
	stderr io.Writer,
) (*daemonkit.Child, error) {
	owned, err := s.owner.spawn(ctx, managedSpawnConfig{id: s.id, cmd: cmd, channel: channel}, stderr)
	if err != nil {
		return nil, err
	}
	return owned.child, nil
}

// ownedChild is one spawned child bound to its durable record, worker slot,
// and settlement.
type ownedChild struct {
	child   *daemonkit.Child
	record  catalog.ProcessRecord
	tracked bool
	release func()

	mu        sync.Mutex
	exit      daemonkit.Exit
	exited    bool
	settleErr error
	settled   chan struct{}
}

func (c *ownedChild) settle(ledger *processLedger) {
	exit := <-c.child.Done()
	var err error
	if c.tracked {
		err = ledger.Untrack(c.record)
	}
	c.mu.Lock()
	c.exit = exit
	c.exited = true
	c.settleErr = err
	c.mu.Unlock()
	c.release()
	close(c.settled)
}

func (c *ownedChild) Exit() (daemonkit.Exit, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exit, c.exited
}

type managedProcess interface {
	Record() catalog.ProcessRecord
	Start(context.Context) error
	Done() <-chan struct{}
	Exit() (daemonkit.Exit, bool)
	Stop(context.Context) error
}

type settledManagedProcess interface {
	Settled() bool
}

type processPrepare func(managedSpawnConfig, io.Writer) (managedProcess, error)

type managedProcessPreparer struct {
	owner *processOwner
}

func (p managedProcessPreparer) Prepare(
	config managedSpawnConfig,
	stderr io.Writer,
) (managedProcess, error) {
	if p.owner == nil {
		return nil, errors.New("FuseKit runtime: managed process preparer is incomplete")
	}
	return &preparedManagedProcess{
		owner: p.owner, config: config, stderr: stderr, done: make(chan struct{}),
	}, nil
}

type ownedProcessWriter struct {
	io.Writer
	closer io.Closer
}

// preparedManagedProcess defers the spawn to Start, so a consumer arms its
// bind gates before the child can run its first instruction.
type preparedManagedProcess struct {
	owner  *processOwner
	config managedSpawnConfig
	stderr io.Writer
	done   chan struct{}

	mu      sync.Mutex
	started bool
	stopped bool
	child   *ownedChild
}

func (p *preparedManagedProcess) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.started || p.stopped {
		p.mu.Unlock()
		return errors.New("FuseKit runtime: managed process already dispatched")
	}
	p.started = true
	p.mu.Unlock()
	child, err := p.owner.spawn(ctx, p.config, p.stderr)
	if err != nil {
		p.mu.Lock()
		p.stopped = true
		p.mu.Unlock()
		close(p.done)
		p.closeOwnedWriter()
		return err
	}
	p.mu.Lock()
	p.child = child
	p.mu.Unlock()
	go func() {
		<-child.settled
		p.closeOwnedWriter()
		close(p.done)
	}()
	return nil
}

func (p *preparedManagedProcess) closeOwnedWriter() {
	if owned, ok := p.stderr.(*ownedProcessWriter); ok && owned != nil && owned.closer != nil {
		_ = owned.closer.Close()
	}
}

func (p *preparedManagedProcess) Record() catalog.ProcessRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.child == nil {
		return catalog.ProcessRecord{}
	}
	return p.child.record
}

func (p *preparedManagedProcess) Done() <-chan struct{} { return p.done }

func (p *preparedManagedProcess) Exit() (daemonkit.Exit, bool) {
	p.mu.Lock()
	child := p.child
	p.mu.Unlock()
	if child == nil {
		return daemonkit.Exit{}, false
	}
	return child.Exit()
}

func (p *preparedManagedProcess) Settled() bool {
	select {
	case <-p.done:
		_, ok := p.Exit()
		p.mu.Lock()
		defer p.mu.Unlock()
		return ok || !p.started
	default:
		return false
	}
}

func (p *preparedManagedProcess) Stop(ctx context.Context) error {
	p.mu.Lock()
	child := p.child
	started := p.started
	alreadyStopped := p.stopped
	p.stopped = true
	p.mu.Unlock()
	if !started {
		if !alreadyStopped {
			close(p.done)
			p.closeOwnedWriter()
		}
		return nil
	}
	if child == nil {
		<-p.done
		return nil
	}
	ctx, cancel := budgeted(ctx, childSettlementTimeout)
	defer cancel()
	_, stopErr := child.child.Stop(ctx)
	select {
	case <-p.done:
		child.mu.Lock()
		settleErr := child.settleErr
		child.mu.Unlock()
		return errors.Join(stopErr, settleErr)
	case <-ctx.Done():
		return errors.Join(stopErr, fmt.Errorf("FuseKit runtime: managed process settlement incomplete: %w", ctx.Err()))
	}
}

func managedProcessSettled(process managedProcess) bool {
	settled, ok := process.(settledManagedProcess)
	return ok && settled.Settled()
}
