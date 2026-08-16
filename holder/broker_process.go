package holder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

const brokerChildModeArgument = "--fusekit-broker-child"

type brokerProcessStart = processPrepare

type brokerChannelServe func(context.Context, managedProcess) error

var errMissingBrokerProcess = errors.New("FuseKit runtime: signed broker launcher returned no process")

type brokerProcessSlot struct {
	record  catalog.ProcessRecord
	process managedProcess
	bound   bool
}

type brokerProcessOwner struct {
	plan     RuntimePlan
	lifetime context.Context
	serve    brokerChannelServe
	start    brokerProcessStart

	launchMu  sync.Mutex
	mu        sync.Mutex
	launching bool
	records   map[catalog.BrokerProcessIdentity]*brokerProcessSlot
	settled   map[catalog.BrokerProcessIdentity]struct{}
	changed   chan struct{}
}

func newBrokerProcessOwner(
	plan RuntimePlan,
	lifetime context.Context,
	serve brokerChannelServe,
	start brokerProcessStart,
) (*brokerProcessOwner, error) {
	if err := plan.validate(); err != nil {
		return nil, err
	}
	if _, ok := plan.Broker(); !ok {
		return nil, errors.New("FuseKit runtime: File Provider broker is not configured")
	}
	if start == nil {
		return nil, errors.New("FuseKit runtime: broker process launcher is required")
	}
	if lifetime == nil {
		return nil, errors.New("FuseKit runtime: broker lifetime context is required")
	}
	if serve == nil {
		return nil, errors.New("FuseKit runtime: broker spawn channel server is required")
	}
	return &brokerProcessOwner{
		plan: plan, lifetime: lifetime, serve: serve, start: start,
		records: make(map[catalog.BrokerProcessIdentity]*brokerProcessSlot),
		settled: make(map[catalog.BrokerProcessIdentity]struct{}),
		changed: make(chan struct{}),
	}, nil
}

func brokerProcessSpec(plan RuntimePlan) (managedSpawnConfig, error) {
	broker, ok := plan.Broker()
	if !ok {
		return managedSpawnConfig{}, errors.New("FuseKit runtime: File Provider broker is not configured")
	}
	return managedSpawnConfig{
		id: recoveryid.Broker,
		cmd: daemonkit.Cmd{
			Path:    broker.Deployment.Executable,
			Args:    []string{brokerChildModeArgument},
			Env:     sanitizedChildEnvironment(os.Environ()),
			Session: true,
			Exec:    daemonkit.ServingSigned(broker.Requirement),
		},
		channel: daemonkit.ChannelHandoff,
	}, nil
}

// BindBroker fences the accepting session by PID against the durably launched
// child: daemonkit owns the live process, so PID reuse is excluded while the
// slot exists. A bind racing the in-flight launch waits for the record.
func (o *brokerProcessOwner) BindBroker(
	ctx context.Context,
	caller daemonkit.Caller,
) (catalog.BrokerProcessIdentity, error) {
	for {
		o.mu.Lock()
		var matched catalog.BrokerProcessIdentity
		for identity, slot := range o.records {
			if identity.PID != caller.PID {
				continue
			}
			if slot.bound {
				o.mu.Unlock()
				return catalog.BrokerProcessIdentity{}, errors.New("FuseKit runtime: signed broker process is already bound")
			}
			if matched != (catalog.BrokerProcessIdentity{}) {
				o.mu.Unlock()
				return catalog.BrokerProcessIdentity{}, errors.New("FuseKit runtime: ambiguous signed broker process identity")
			}
			matched = identity
		}
		if matched != (catalog.BrokerProcessIdentity{}) {
			o.records[matched].bound = true
			o.signalChangedLocked()
			o.mu.Unlock()
			return matched, nil
		}
		launching := o.launching
		changed := o.changed
		o.mu.Unlock()
		if !launching {
			return catalog.BrokerProcessIdentity{}, errors.New("FuseKit runtime: signed broker process was not durably launched")
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return catalog.BrokerProcessIdentity{}, fmt.Errorf("FuseKit runtime: await signed broker launch identity: %w", ctx.Err())
		}
	}
}

func (o *brokerProcessOwner) RetireBroker(
	ctx context.Context,
	identity catalog.BrokerProcessIdentity,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	done := ctx.Done()
	var ctxErr error
	for {
		if ctxErr == nil {
			if err := ctx.Err(); err != nil {
				ctxErr = fmt.Errorf("FuseKit runtime: retire signed broker: %w", err)
				done = nil
			}
		}
		o.mu.Lock()
		slot, ok := o.records[identity]
		if !ok {
			if _, settled := o.settled[identity]; settled {
				delete(o.settled, identity)
				o.mu.Unlock()
				return ctxErr
			}
			o.mu.Unlock()
			return errors.Join(ctxErr, errors.New("FuseKit runtime: signed broker process identity is not owned"))
		}
		if slot.process != nil {
			process := slot.process
			o.mu.Unlock()
			stopErr := process.Stop(context.Background())
			if ctxErr == nil {
				if err := ctx.Err(); err != nil {
					ctxErr = fmt.Errorf("FuseKit runtime: retire signed broker: %w", err)
				}
			}
			if stopErr != nil && !managedProcessSettled(process) {
				return errors.Join(ctxErr, fmt.Errorf("FuseKit runtime: retire signed broker: %w", stopErr))
			}
			o.mu.Lock()
			delete(o.records, identity)
			o.signalChangedLocked()
			o.mu.Unlock()
			return errors.Join(ctxErr, stopErr)
		}
		changed := o.changed
		o.mu.Unlock()
		if done == nil {
			<-changed
			continue
		}
		select {
		case <-changed:
		case <-done:
			ctxErr = fmt.Errorf("FuseKit runtime: await signed broker launch settlement: %w", ctx.Err())
			done = nil
		}
	}
}

func (o *brokerProcessOwner) StartBroker(ctx context.Context) error {
	o.launchMu.Lock()
	defer o.launchMu.Unlock()
	if o.available() {
		return nil
	}
	logFile, err := os.OpenFile(
		filepath.Join(o.plan.Paths().Directory, "broker.log"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0o600,
	)
	if err != nil {
		return fmt.Errorf("FuseKit runtime: open signed broker log: %w", err)
	}
	output := &ownedProcessWriter{Writer: logFile, closer: logFile}

	config, err := brokerProcessSpec(o.plan)
	if err != nil {
		return errors.Join(err, logFile.Close())
	}
	o.setLaunching(true)
	defer o.setLaunching(false)
	process, err := o.start(config, output)
	if err != nil {
		return errors.Join(fmt.Errorf("FuseKit runtime: start signed broker: %w", err), logFile.Close())
	}
	if nilManagedValue(process) {
		return errors.Join(errMissingBrokerProcess, logFile.Close())
	}
	if err := process.Start(ctx); err != nil {
		return errors.Join(
			fmt.Errorf("FuseKit runtime: dispatch signed broker: %w", err),
			process.Stop(context.Background()),
		)
	}
	record := process.Record()
	if err := o.expect(record); err != nil {
		return errors.Join(err, process.Stop(context.Background()))
	}
	go o.serveSpawnChannel(process)
	expected := brokerCatalogProcessIdentity(record)
	if err := o.awaitBound(ctx, expected); err != nil {
		retainErr := o.retainFailedStartProcess(expected, process)
		stopErr := process.Stop(context.Background())
		if stopErr == nil || managedProcessSettled(process) {
			o.settleFailedStart(expected)
		}
		return errors.Join(err, retainErr, stopErr)
	}
	o.mu.Lock()
	slot, ok := o.records[expected]
	if !ok || !slot.bound {
		o.mu.Unlock()
		retainErr := o.retainFailedStartProcess(expected, process)
		stopErr := process.Stop(context.WithoutCancel(ctx))
		if stopErr == nil || managedProcessSettled(process) {
			o.settleFailedStart(expected)
		}
		return errors.Join(
			errors.New("FuseKit runtime: signed broker launch completed without exact bind"), retainErr, stopErr,
		)
	}
	slot.process = process
	o.signalChangedLocked()
	o.mu.Unlock()
	return nil
}

// serveSpawnChannel ends on its own at child exit — the socketpair peer closes
// with the process — or at daemon drain through the lifetime context.
func (o *brokerProcessOwner) serveSpawnChannel(process managedProcess) {
	if err := o.serve(o.lifetime, process); err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("FuseKit runtime: signed broker spawn channel ended", "err", err)
	}
}

func (o *brokerProcessOwner) setLaunching(launching bool) {
	o.mu.Lock()
	o.launching = launching
	o.signalChangedLocked()
	o.mu.Unlock()
}

func (o *brokerProcessOwner) retainFailedStartProcess(
	identity catalog.BrokerProcessIdentity,
	process managedProcess,
) error {
	if nilManagedValue(process) {
		return errMissingBrokerProcess
	}
	if identity == (catalog.BrokerProcessIdentity{}) || process.Record() != o.record(identity) {
		return errors.New("FuseKit runtime: failed signed broker launch returned a substituted process")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	slot, ok := o.records[identity]
	if !ok {
		return errors.New("FuseKit runtime: failed signed broker launch lost its durable identity")
	}
	slot.process = process
	o.signalChangedLocked()
	return nil
}

func (o *brokerProcessOwner) expect(record catalog.ProcessRecord) error {
	if err := record.Validate(); err != nil {
		return fmt.Errorf("FuseKit runtime: validate signed broker process record: %w", err)
	}
	identity := brokerCatalogProcessIdentity(record)
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.records) != 0 {
		return errors.New("FuseKit runtime: another signed broker process is already expected")
	}
	delete(o.settled, identity)
	o.records[identity] = &brokerProcessSlot{record: record}
	o.signalChangedLocked()
	return nil
}

func (o *brokerProcessOwner) awaitBound(
	ctx context.Context,
	identity catalog.BrokerProcessIdentity,
) error {
	for {
		o.mu.Lock()
		slot, ok := o.records[identity]
		if ok && slot.bound {
			o.mu.Unlock()
			return nil
		}
		if !ok {
			o.mu.Unlock()
			return errors.New("FuseKit runtime: signed broker expectation disappeared before bind")
		}
		changed := o.changed
		o.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("FuseKit runtime: await exact signed broker bind: %w", ctx.Err())
		}
	}
}

func (o *brokerProcessOwner) settleFailedStart(identity catalog.BrokerProcessIdentity) {
	if identity == (catalog.BrokerProcessIdentity{}) {
		return
	}
	o.mu.Lock()
	if slot, ok := o.records[identity]; ok {
		if slot.bound {
			o.settled[identity] = struct{}{}
		}
		delete(o.records, identity)
		o.signalChangedLocked()
	}
	o.mu.Unlock()
}

func (o *brokerProcessOwner) record(identity catalog.BrokerProcessIdentity) catalog.ProcessRecord {
	o.mu.Lock()
	defer o.mu.Unlock()
	if slot := o.records[identity]; slot != nil {
		return slot.record
	}
	return catalog.ProcessRecord{}
}

// VerifyCaller fences a session peer by PID against the durably launched
// signed broker.
func (o *brokerProcessOwner) VerifyCaller(caller daemonkit.Caller) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for identity := range o.records {
		if identity.PID == caller.PID {
			return nil
		}
	}
	return errors.New("FuseKit runtime: caller is not the owned signed broker process")
}

func (o *brokerProcessOwner) available() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.records) != 0
}

func (o *brokerProcessOwner) signalChangedLocked() {
	close(o.changed)
	o.changed = make(chan struct{})
}

func brokerCatalogProcessIdentity(record catalog.ProcessRecord) catalog.BrokerProcessIdentity {
	return catalog.BrokerProcessIdentity{
		PID: record.PID, StartTime: record.StartTime, Boot: record.Boot,
		Generation: record.Generation.String(),
	}
}

var _ interface {
	BindBroker(context.Context, daemonkit.Caller) (catalog.BrokerProcessIdentity, error)
	RetireBroker(context.Context, catalog.BrokerProcessIdentity) error
	StartBroker(context.Context) error
} = (*brokerProcessOwner)(nil)
