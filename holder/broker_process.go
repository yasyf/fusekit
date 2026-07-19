package holder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
)

const (
	brokerChildModeArgument    = "--fusekit-broker-child"
	brokerDaemonSocketArgument = "--fusekit-daemon-socket"
)

type managedBrokerProcess interface {
	Record() proc.Record
	Stop(context.Context) error
}

type brokerProcessStart func(context.Context, supervise.ProcessSpec) (managedBrokerProcess, error)

var errMissingBrokerProcess = errors.New("holder: signed broker launcher returned no process")

type brokerProcessSlot struct {
	record  proc.Record
	process managedBrokerProcess
	bound   bool
}

type brokerProcessOwner struct {
	plan  RuntimePlan
	start brokerProcessStart

	launchMu sync.Mutex
	mu       sync.Mutex
	records  map[catalog.BrokerProcessIdentity]*brokerProcessSlot
	settled  map[catalog.BrokerProcessIdentity]struct{}
	changed  chan struct{}
}

func newBrokerProcessOwner(plan RuntimePlan, start brokerProcessStart) (*brokerProcessOwner, error) {
	if err := plan.validate(); err != nil {
		return nil, err
	}
	if _, ok := plan.Broker(); !ok {
		return nil, errors.New("holder: File Provider broker is not configured")
	}
	if start == nil {
		return nil, errors.New("holder: broker process launcher is required")
	}
	return &brokerProcessOwner{
		plan: plan, start: start,
		records: make(map[catalog.BrokerProcessIdentity]*brokerProcessSlot),
		settled: make(map[catalog.BrokerProcessIdentity]struct{}),
		changed: make(chan struct{}),
	}, nil
}

func brokerProcessSpec(plan RuntimePlan) (supervise.ProcessSpec, error) {
	broker, ok := plan.Broker()
	if !ok {
		return supervise.ProcessSpec{}, errors.New("holder: File Provider broker is not configured")
	}
	return supervise.ProcessSpec{
		Path: broker.Deployment.Executable, RecoveryClass: proc.RecoveryBroker,
		Env: sanitizedChildEnvironment(os.Environ()),
		Args: []string{
			brokerChildModeArgument,
			brokerDaemonSocketArgument,
			plan.Paths().Socket,
		},
	}, nil
}

func (o *brokerProcessOwner) BindBroker(
	_ context.Context,
	peer wire.Peer,
) (catalog.BrokerProcessIdentity, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var matched catalog.BrokerProcessIdentity
	for identity, slot := range o.records {
		if identity.PID != peer.PID || identity.StartTime != peer.StartTime || identity.Boot != peer.Boot {
			continue
		}
		if slot.bound {
			return catalog.BrokerProcessIdentity{}, errors.New("holder: signed broker process is already bound")
		}
		if matched != (catalog.BrokerProcessIdentity{}) {
			return catalog.BrokerProcessIdentity{}, errors.New("holder: ambiguous signed broker process identity")
		}
		matched = identity
	}
	if matched == (catalog.BrokerProcessIdentity{}) {
		return catalog.BrokerProcessIdentity{}, errors.New("holder: signed broker process was not durably launched")
	}
	o.records[matched].bound = true
	o.signalChangedLocked()
	return matched, nil
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
				ctxErr = fmt.Errorf("holder: retire signed broker: %w", err)
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
			return errors.Join(ctxErr, errors.New("holder: signed broker process identity is not owned"))
		}
		if slot.process != nil {
			process := slot.process
			o.mu.Unlock()
			stopErr := process.Stop(context.Background())
			if ctxErr == nil {
				if err := ctx.Err(); err != nil {
					ctxErr = fmt.Errorf("holder: retire signed broker: %w", err)
				}
			}
			if stopErr != nil {
				return errors.Join(ctxErr, fmt.Errorf("holder: retire signed broker: %w", stopErr))
			}
			o.mu.Lock()
			delete(o.records, identity)
			o.signalChangedLocked()
			o.mu.Unlock()
			return ctxErr
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
			ctxErr = fmt.Errorf("holder: await signed broker launch settlement: %w", ctx.Err())
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
		return fmt.Errorf("holder: open signed broker log: %w", err)
	}
	defer func() { _ = logFile.Close() }()

	spec, err := brokerProcessSpec(o.plan)
	if err != nil {
		return err
	}
	spec.Stdout, spec.Stderr = logFile, logFile
	var expected catalog.BrokerProcessIdentity
	spec.Recorded = func(_ context.Context, record proc.Record) error {
		expected = brokerCatalogProcessIdentity(record)
		return o.expect(record)
	}
	spec.Ready = func(ctx context.Context, record proc.Record) error {
		return o.awaitBound(ctx, brokerCatalogProcessIdentity(record))
	}
	process, err := o.start(ctx, spec)
	if err != nil {
		startErr := fmt.Errorf("holder: start signed broker: %w", err)
		if nilManagedValue(process) {
			o.settleFailedStart(expected)
			return startErr
		}
		retainErr := o.retainFailedStartProcess(expected, process)
		stopErr := process.Stop(context.Background())
		if stopErr == nil {
			o.settleFailedStart(expected)
			return errors.Join(startErr, retainErr)
		}
		return errors.Join(
			startErr,
			fmt.Errorf("holder: stop failed signed broker launch: %w", stopErr),
			retainErr,
		)
	}
	if nilManagedValue(process) {
		o.settleFailedStart(expected)
		return errMissingBrokerProcess
	}
	if process.Record() != o.record(expected) {
		stopErr := process.Stop(context.WithoutCancel(ctx))
		o.settleFailedStart(expected)
		return errors.Join(errors.New("holder: signed broker launcher returned substituted process"), stopErr)
	}
	o.mu.Lock()
	slot, ok := o.records[expected]
	if !ok || !slot.bound {
		o.mu.Unlock()
		stopErr := process.Stop(context.WithoutCancel(ctx))
		o.settleFailedStart(expected)
		return errors.Join(errors.New("holder: signed broker launch completed without exact bind"), stopErr)
	}
	slot.process = process
	o.signalChangedLocked()
	o.mu.Unlock()
	return nil
}

func (o *brokerProcessOwner) retainFailedStartProcess(
	identity catalog.BrokerProcessIdentity,
	process managedBrokerProcess,
) error {
	if nilManagedValue(process) {
		return errMissingBrokerProcess
	}
	if identity == (catalog.BrokerProcessIdentity{}) || process.Record() != o.record(identity) {
		return errors.New("holder: failed signed broker launch returned a substituted process")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	slot, ok := o.records[identity]
	if !ok {
		return errors.New("holder: failed signed broker launch lost its durable identity")
	}
	slot.process = process
	o.signalChangedLocked()
	return nil
}

func (o *brokerProcessOwner) expect(record proc.Record) error {
	if err := record.Validate(); err != nil {
		return fmt.Errorf("holder: validate signed broker process record: %w", err)
	}
	identity := brokerCatalogProcessIdentity(record)
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.records) != 0 {
		return errors.New("holder: another signed broker process is already expected")
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
			return errors.New("holder: signed broker expectation disappeared before bind")
		}
		changed := o.changed
		o.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("holder: await exact signed broker bind: %w", ctx.Err())
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

func (o *brokerProcessOwner) record(identity catalog.BrokerProcessIdentity) proc.Record {
	o.mu.Lock()
	defer o.mu.Unlock()
	if slot := o.records[identity]; slot != nil {
		return slot.record
	}
	return proc.Record{}
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

func brokerCatalogProcessIdentity(record proc.Record) catalog.BrokerProcessIdentity {
	return catalog.BrokerProcessIdentity{
		PID: record.PID, StartTime: record.StartTime, Boot: record.Boot,
		Generation: record.Generation,
	}
}

var _ interface {
	BindBroker(context.Context, wire.Peer) (catalog.BrokerProcessIdentity, error)
	RetireBroker(context.Context, catalog.BrokerProcessIdentity) error
	StartBroker(context.Context) error
} = (*brokerProcessOwner)(nil)
