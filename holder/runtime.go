// Package holder composes one signed-app filesystem runtime from daemonkit and FuseKit.
package holder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"
	dkpaths "github.com/yasyf/daemonkit/paths"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/catalogworker"
	"github.com/yasyf/fusekit/convergence"
	"github.com/yasyf/fusekit/internal/presentationroot"
	"github.com/yasyf/fusekit/internal/recoveryid"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"

	"github.com/yasyf/fusekit/causal"
)

const (
	defaultWorkerLimit = 8

	nativeWorkerReservations   = 1
	catalogWorkerReservations  = 1
	disposableWorkerReserve    = 1
	brokerProcessReservations  = 1
	sourceObserverReservations = 1

	defaultShutdownTimeout = 30 * time.Second
)

// Config defines the complete process-lifetime holder runtime embedded by one signed app.
type Config struct {
	Plan         RuntimePlan
	RuntimeBuild string
	Trust        RuntimeTrust

	Owner             catalog.SourceAuthorityFleetOwnerID
	Drivers           DriverFactories
	CatalogAuthorizer catalogservice.Authorizer
	// WorkerLimit bounds the native child, catalog worker, source observers,
	// disposable operations, and the signed broker when File Provider is present.
	WorkerLimit             int
	NativeOptions           []string
	NativeReadinessTimeout  time.Duration
	NativeStderr            io.Writer
	RuntimeStderr           io.Writer
	SourceStderr            io.Writer
	CatalogReadinessTimeout time.Duration
	CatalogOperationTimeout time.Duration
	CatalogStderr           io.Writer
	Authorizer              mountservice.Authorizer

	ShutdownTimeout  time.Duration
	BusinessHandlers []BusinessHandlerSpec

	native             nativeController
	protectedPeer      func(context.Context, daemonkit.Caller) error
	planner            tenant.Planner
	authorityFactory   authorityRuntimeFactory
	authorityExecutors authorityExecutorFactory
	semanticFactory    semanticAuthorityFactory
	catalogService     func(context.Context, *catalogworker.Manager, *tenant.TenantRuntime) (catalogservice.CoreConfig, error)
	catalogManager     func(context.Context, catalogworker.ManagerConfig) (*catalogworker.Manager, error)
	brokerStart        brokerProcessStart
	fleetTransitions   tenant.FleetTransitionHook
	wireMaxSessions    int
}

// Runtime owns the daemon lifecycle, catalog, tenant actors, workers, and one native root.
type Runtime struct {
	config Config
	paths  RuntimePaths
	daemon daemonkit.Daemon
	socket string

	runMu  sync.Mutex
	cancel context.CancelFunc

	ready chan struct{}
	done  chan struct{}

	graphMu sync.Mutex
	graph   *runtimeGraph

	graphSettleOnce sync.Once
	graphSettleDone chan struct{}
	settlingGraph   *runtimeGraph
}

type processRecoveryProof struct {
	complete bool
}

type brokerRecoverer interface {
	Recover(context.Context) error
}

func recoverBrokerAfterProcesses(
	ctx context.Context,
	proof processRecoveryProof,
	broker brokerRecoverer,
) error {
	if !proof.complete {
		return errors.New("FuseKit runtime: broker recovery requires settled prior process generations")
	}
	return broker.Recover(ctx)
}

// New constructs an unstarted hard-versioned holder runtime.
func New(ctx context.Context, config Config) (*Runtime, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("FuseKit runtime: initialize: %w", err)
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	paths := config.Plan.Paths()
	if err := prepareRuntimeDirectory(config.Plan.deployment.home, paths.Directory); err != nil {
		return nil, err
	}
	if native, ok := config.Plan.NativePresentation(); ok {
		if err := presentationroot.Prepare(native.PresentationRoot); err != nil {
			return nil, fmt.Errorf("FuseKit runtime: prepare presentation root: %w", err)
		}
	}
	trust, err := runtimeTrust(config)
	if err != nil {
		return nil, err
	}
	agent := config.Plan.Deployment().Agent()
	socket, err := dkpaths.Socket(agent.Label)
	if err != nil {
		return nil, fmt.Errorf("FuseKit runtime: derive daemon socket: %w", err)
	}
	app := config.Plan.Application()
	program, err := daemonkit.InBundle(
		app.AppPath, filepath.Join("Contents", "MacOS", app.Runtime.ExecutableName),
	)
	if err != nil {
		return nil, fmt.Errorf("FuseKit runtime: resolve bundled program: %w", err)
	}
	runtime := &Runtime{
		config: config, paths: paths, socket: socket,
		daemon: daemonkit.Daemon{
			Label:       daemonkit.Label(agent.Label),
			Program:     program,
			Schemas:     []daemonkit.Schema{daemonkit.Schema(transportproto.WireBuild)},
			Trust:       trust,
			Restart:     daemonkit.RestartAlways,
			Shutdown:    daemonkit.Grace(shutdownTimeout(config.ShutdownTimeout)),
			Concurrency: config.wireMaxSessions,
		},
		ready:           make(chan struct{}),
		done:            make(chan struct{}),
		graphSettleDone: make(chan struct{}),
	}
	return runtime, nil
}

// Daemon returns the one shared daemon declaration launcher and daemon read.
func (r *Runtime) Daemon() daemonkit.Daemon { return r.daemon }

// Run serves the daemon until ctx is cancelled or the product stops, and
// returns only after the drain has settled the published graph.
func (r *Runtime) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	r.runMu.Lock()
	r.cancel = cancel
	r.runMu.Unlock()
	defer cancel()
	drained, err := daemonkit.Serve(runCtx, r.daemon, r.start)
	// Unconditionally, because the product's Close stage has usually settled the
	// graph already and cleared it: daemonkit logs a shutdown-stage failure
	// rather than returning it, so this is the only path that carries the
	// settlement error to the caller. The deadlineless context is what makes it
	// the authoritative one — it joins the settlement instead of abandoning it,
	// so the error here is the settled result rather than a drain-budget timeout.
	settleCtx, cancelSettle := context.Background(), context.CancelFunc(func() {})
	if len(drained.Abandoned) != 0 {
		// Except when daemonkit gave a stage up: that work may never settle, and
		// parking Run on it forever would outlast the process daemonkit already
		// unparked. Bound the join so Run reports what settled.
		settleCtx, cancelSettle = context.WithTimeout(
			context.Background(), shutdownTimeout(r.config.ShutdownTimeout),
		)
	}
	settleErr := r.settleGraph(settleCtx)
	cancelSettle()
	close(r.done)
	return errors.Join(err, settleErr)
}

// WaitReady waits for the served holder graph.
func (r *Runtime) WaitReady(ctx context.Context) error {
	select {
	case <-r.ready:
		return nil
	case <-r.done:
		return errors.New("FuseKit runtime: daemon returned before readiness")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close begins the drain and waits for the runtime to settle.
func (r *Runtime) Close(ctx context.Context) error {
	r.runMu.Lock()
	cancel := r.cancel
	r.runMu.Unlock()
	if cancel == nil {
		return errors.New("FuseKit runtime: runtime is not running")
	}
	cancel()
	return r.Wait(ctx)
}

// Wait joins the serving daemon and its settled graph.
func (r *Runtime) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) currentGraph() *runtimeGraph {
	select {
	case <-r.ready:
	default:
		return nil
	}
	return r.currentGraphAny()
}

func (r *Runtime) currentGraphAny() *runtimeGraph {
	r.graphMu.Lock()
	defer r.graphMu.Unlock()
	return r.graph
}

func (r *Runtime) settleGraph(ctx context.Context) error {
	r.graphSettleOnce.Do(func() {
		r.graphMu.Lock()
		r.settlingGraph = r.graph
		r.graph = nil
		r.graphMu.Unlock()
		close(r.graphSettleDone)
	})
	<-r.graphSettleDone
	if r.settlingGraph == nil {
		return nil
	}
	// Per caller rather than cached once: Wait mixes the caller's own deadline
	// into its result, so caching the first caller's value would let a drain
	// budget that expired before settlement replace the settlement error with a
	// timeout — permanently, for every later caller. The settlement itself still
	// runs exactly once, latched inside ownedWorkers.
	return r.settlingGraph.workers.Wait(ctx)
}

func (r *Runtime) start(c daemonkit.Ctx) (daemonkit.Product, error) {
	graph, err := r.activate(c, r.config, r.paths)
	if err != nil {
		return nil, err
	}
	product, err := r.newProduct(graph)
	if err != nil {
		return nil, errors.Join(err, closeActivationGraph(graph))
	}
	r.graphMu.Lock()
	r.graph = graph
	r.graphMu.Unlock()
	graph.reportHealth()
	close(r.ready)
	return product, nil
}

// runtimeProduct is the daemonkit.Product one activation publishes: the mux
// answers business, and the drain settles the whole graph.
type runtimeProduct struct {
	runtime *Runtime
	graph   *runtimeGraph
	mux     *transportproto.Mux
}

func (p *runtimeProduct) Handle(ctx context.Context, request daemonkit.Request) (daemonkit.Reply, error) {
	return p.mux.Handle(ctx, request)
}

func (p *runtimeProduct) Drain(daemonkit.Budget) error {
	p.graph.workers.Close()
	return nil
}

func (p *runtimeProduct) Close(budget daemonkit.Budget) error {
	ctx, cancel := budget.Context(context.Background())
	defer cancel()
	return p.runtime.settleGraph(ctx)
}

func (r *Runtime) newProduct(graph *runtimeGraph) (*runtimeProduct, error) {
	_, native := r.config.Plan.NativePresentation()
	mountSpecs, err := mountservice.Register(
		mountservice.Routes{Native: native},
		func(daemonkit.Request) (*mountservice.Server, error) {
			if graph.mountService == nil {
				return nil, mountservice.ErrUnauthorized
			}
			return graph.mountService, nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("FuseKit runtime: register mount routes: %w", err)
	}
	_, fileProvider := r.config.Plan.Broker()
	catalogSpecs, err := catalogservice.Register(
		catalogservice.Routes{FileProvider: fileProvider},
		func(daemonkit.Request) (*catalogservice.Server, error) {
			if graph.catalogService == nil {
				return nil, catalogservice.ErrBrokerStreamAbsent
			}
			return graph.catalogService, nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("FuseKit runtime: register catalog routes: %w", err)
	}
	businessSpecs, err := businessHandlerSpecs(r, graph, r.config.BusinessHandlers)
	if err != nil {
		return nil, err
	}
	specs := make([]transportproto.HandlerSpec, 0, len(mountSpecs)+len(catalogSpecs)+len(businessSpecs))
	specs = append(specs, mountSpecs...)
	specs = append(specs, catalogSpecs...)
	specs = append(specs, businessSpecs...)
	deadlines := make(map[string]time.Duration, len(specs))
	for _, spec := range specs {
		deadlines[spec.Op] = r.config.CatalogOperationTimeout
	}
	mux, err := transportproto.NewMux(deadlines, specs...)
	if err != nil {
		return nil, fmt.Errorf("FuseKit runtime: build operation mux: %w", err)
	}
	return &runtimeProduct{runtime: r, graph: graph, mux: mux}, nil
}

func (r *Runtime) activate(
	c daemonkit.Ctx,
	config Config,
	paths RuntimePaths,
) (graph *runtimeGraph, err error) {
	startup := c.Context
	lifetime := c.Context
	graph = &runtimeGraph{}
	built := false
	defer func() {
		if !built {
			err = errors.Join(err, closeActivationGraph(graph))
			graph = nil
		}
	}()

	readiness := runtimeReadinessReporter{
		stderr: config.RuntimeStderr, runtimeBuild: config.RuntimeBuild,
	}
	ledger, err := openProcessLedger(paths.ProcessStore)
	if err != nil {
		return graph, err
	}
	if err := ledger.Reclaim(c.Reclaimed); err != nil {
		return graph, fmt.Errorf("FuseKit runtime: reclaim prior process generations: %w", err)
	}
	graph.ledger = ledger
	ownerID := runtimeOwnerRecoveryID(config.Plan)
	graph.runtimeOwnerRecord, err = ledger.RegisterOwner(ownerID)
	if err != nil {
		return graph, fmt.Errorf("FuseKit runtime: register current runtime owner: %w", err)
	}
	readiness.activationGeneration = graph.runtimeOwnerRecord.Generation.String()
	limit := workerLimit(config.WorkerLimit)
	owner := &processOwner{
		spawner: c, runner: c,
		spawnSlots: newWorkerSlots(limit), runSlots: newWorkerSlots(limit),
		ledger: ledger,
	}
	graph.pool = owner
	requirement := config.Plan.RuntimeRequirement()
	runtimeExec := daemonkit.ServingSigned(requirement)
	processRecovery := processRecoveryProof{complete: true}

	managerFactory := config.catalogManager
	if managerFactory == nil {
		managerFactory = catalogworker.NewManager
	}
	graph.catalog, err = managerFactory(lifetime, catalogworker.ManagerConfig{
		Processes: owner.spawnerFor(recoveryid.CatalogWorker), Exec: runtimeExec,
		Executable: config.Plan.RuntimeExecutable(),
		Database:   paths.Catalog, Stderr: config.CatalogStderr,
		ReadinessTimeout: config.CatalogReadinessTimeout,
		OperationTimeout: config.CatalogOperationTimeout,
		StopTimeout:      shutdownTimeout(config.ShutdownTimeout),
	})
	if err != nil {
		return graph, fmt.Errorf("FuseKit runtime: create catalog worker manager: %w", err)
	}
	readiness.report("receipts", "settling", nil)
	if err := recoverProcessGroupReceipts(startup, ledger, recoveryid.CatalogWorker); err != nil {
		return graph, err
	}
	if err := recoverBrokerReceipts(startup, ledger, graph.catalog); err != nil {
		return graph, err
	}
	if err := recoverProcessGroupReceipts(startup, ledger, recoveryid.SourceObserver); err != nil {
		return graph, err
	}
	if err := recoverProcessGroupReceipts(startup, ledger, recoveryid.SourceTask); err != nil {
		return graph, err
	}
	if err := recoverProcessGroupReceipts(startup, ledger, recoveryid.NativeMount); err != nil {
		return graph, err
	}
	if err := recoverSourceOwnerReceipts(startup, ledger, graph.catalog); err != nil {
		return graph, err
	}
	if err := requireNoReceiptLiabilities(
		startup, ledger, recoveryid.SourceDriver, recoveryid.Holder,
	); err != nil {
		return graph, err
	}
	desired, err := (topologyReconciler{store: graph.catalog, owner: config.Owner}).resnapshot(startup)
	if err != nil {
		return graph, fmt.Errorf("FuseKit runtime: recover desired topology: %w", err)
	}
	sourceFleet, err := config.Drivers.sourceFleet(startup, desired)
	if err != nil {
		return graph, fmt.Errorf("FuseKit runtime: resolve desired source fleet: %w", err)
	}
	if len(sourceFleet.Authorities) != 0 && !config.Plan.SourceCapable() {
		return graph, errors.New("FuseKit runtime: desired source authorities require a source-capable runtime plan")
	}
	graph.authorities = &authorityRouter{}
	sourceRuntimeEnabled := len(config.Drivers.entries) != 0 || desired.Head.Fleet != nil
	launcher := sourceProcessLauncher{
		owner: owner, executable: config.Plan.RuntimeExecutable(),
		exec: runtimeExec, stderr: config.SourceStderr,
	}
	buildAuthorities := func(fleet SourceAuthorityFleet) (*authorityRegistry, error) {
		if len(fleet.Authorities) != 0 && !config.Plan.SourceCapable() {
			return nil, errors.New("FuseKit runtime: desired source authorities require a source-capable runtime plan")
		}
		if err := validateSourceFleetWorkerCapacity(config, fleet); err != nil {
			return nil, err
		}
		factory := config.authorityFactory
		if factory == nil {
			factory = func(ctx context.Context, authorityConfig sourceauthority.Config) (managedAuthority, error) {
				return sourceauthority.NewRuntime(ctx, authorityConfig)
			}
		}
		executors := config.authorityExecutors
		if executors == nil {
			executors = func(spec SourceAuthoritySpec) (sourceauthority.Executor, error) {
				authority, digest := sourceAuthorityIdentity(spec)
				return sourceauthority.NewExecutor(
					paths.Directory, launcher, launcher, sourceauthority.StandardOperationDeadlines(),
					sourceauthority.SourceTaskIdentity{
						Owner: fleet.Owner, FleetGeneration: fleet.Generation,
						Authority: authority, AuthorityGeneration: fleet.Generation,
						DriverID: sourceAuthorityDriverID(spec), DeclarationDigest: digest,
						DriverConfig: append([]byte(nil), sourceAuthorityDriverConfig(spec)...),
					},
				)
			}
		}
		semantic := config.semanticFactory
		if semantic == nil {
			semantic = func(ctx context.Context, spec SemanticDriverSpec, tenants []tenant.TenantSpec) (managedAuthority, error) {
				return newSemanticAuthority(
					ctx, graph.catalog, launcher, fleet, spec, tenants,
				)
			}
		}
		return newAuthorityRegistry(
			graph.catalog, fleet, factory, executors, semantic,
			graph.runtimeOwnerRecord,
			shutdownTimeout(config.ShutdownTimeout),
		)
	}
	var initialAuthorities *authorityRegistry
	if sourceFleet.Generation != 0 {
		initialAuthorities, err = buildAuthorities(sourceFleet)
		if err != nil {
			return graph, err
		}
	}

	planner := config.planner
	if planner == nil {
		standard := tenant.StandardPlanner{}
		if sourceRuntimeEnabled {
			standard.SourceMutation = graph.authorities
		}
		planner = standard
	}
	fleets := config.fleetTransitions
	if sourceRuntimeEnabled {
		fleets = graph.authorities
	}
	_, brokerConfigured := config.Plan.Broker()
	_, nativeConfigured := config.Plan.NativePresentation()
	fleets = topologyFleetTransitions{
		next: fleets, nativeCapable: nativeConfigured, fileProviderCapable: brokerConfigured,
	}
	graph.tenants, err = tenant.NewRuntime(startup, graph.catalog, planner, fleets, desired.Tenants)
	if err != nil {
		return graph, fmt.Errorf("FuseKit runtime: create tenant runtime: %w", err)
	}
	graph.tenantSpecs = graph.tenants
	graph.tenantRetirements = graph.catalog
	if initialAuthorities != nil {
		if err := initialAuthorities.start(startup, graph.tenants.Specs()); err != nil {
			return graph, fmt.Errorf("FuseKit runtime: start source authorities: %w", err)
		}
		if err := initialAuthorities.recoverSemanticReceipts(startup); err != nil {
			return graph, fmt.Errorf("FuseKit runtime: recover semantic source receipts: %w", err)
		}
		if err := graph.authorities.installInitial(initialAuthorities); err != nil {
			return graph, err
		}
	}
	if err := requireNoSourceDriverCatalogLiabilities(startup, graph.catalog); err != nil {
		return graph, err
	}
	if err := recoverSourceDriverReceipts(startup, ledger, graph.catalog); err != nil {
		return graph, err
	}
	if err := recoverHolderReceipts(startup, ledger); err != nil {
		return graph, err
	}
	if err := requireNoReceiptLiabilities(startup, ledger); err != nil {
		return graph, err
	}
	readiness.report("receipts", "settled", nil)
	if err := graph.tenants.Recover(startup); err != nil {
		return graph, fmt.Errorf("FuseKit runtime: recover tenant runtime: %w", err)
	}
	graph.topology, err = newTopologyController(
		graph.catalog, config.Owner, config.Drivers, graph.authorities,
		buildAuthorities, desired,
	)
	if err != nil {
		return graph, err
	}
	runtimeBroker, brokerConfigured := config.Plan.Broker()
	if err := validatePresentationCapabilities(nativeConfigured, brokerConfigured, graph.tenants.Specs()); err != nil {
		return graph, err
	}
	if err := graph.catalog.BindTenantPreparer(func(
		prepareCtx context.Context,
		tenantID catalog.TenantID,
		generation catalog.Generation,
		revision catalog.Revision,
	) error {
		lease, err := graph.tenants.AcquireGeneration(prepareCtx, tenantID, generation)
		if err != nil {
			return err
		}
		defer lease.Release()
		state, err := lease.Prepare(prepareCtx, revision)
		if err != nil {
			return err
		}
		if !state.Prepared() {
			return fmt.Errorf("%w: tenant preparation did not converge", catalog.ErrIntegrity)
		}
		return nil
	}); err != nil {
		return graph, fmt.Errorf("FuseKit runtime: bind catalog worker tenant preparer: %w", err)
	}

	if nativeConfigured {
		graph.native = config.native
	}
	prepare := managedProcessPreparer{owner: owner}.Prepare
	if nativeConfigured && graph.native == nil {
		library, librarySHA256, ok := config.Plan.FUSELibrary()
		if !ok {
			return graph, errors.New("FuseKit runtime: native presentation lacks FUSE library")
		}
		graph.native = newNativeProcess(nativeProcessConfig{
			prepare: prepare,
			confirmMount: func(ctx context.Context, root, token string) error {
				return runNativeMountProbe(
					ctx, graph.pool, config.Plan.RuntimeExecutable(), runtimeExec, root, token, config.NativeStderr,
				)
			},
			socket: r.socket, executable: config.Plan.RuntimeExecutable(), exec: runtimeExec,
			library: library, librarySHA256: librarySHA256, validateLibrary: validateBundledFUSEBytes,
			options: append([]string(nil), config.NativeOptions...), readinessTimeout: config.NativeReadinessTimeout,
			stderr: config.NativeStderr,
		})
	}
	runtimePeer := config.protectedPeer
	if runtimePeer == nil {
		native := graph.native
		runtimePeer = func(ctx context.Context, caller daemonkit.Caller) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if native == nil {
				return mountservice.ErrUnauthorized
			}
			return native.VerifyCaller(caller)
		}
	}
	var catalogCore catalogservice.CoreConfig
	var fileProviderConfig *catalogservice.FileProviderConfig
	if config.catalogService != nil {
		catalogCore, err = config.catalogService(startup, graph.catalog, graph.tenants)
	} else {
		if brokerConfigured {
			brokerRequirement := runtimeBroker.Requirement
			designatedRequirement, requirementErr := bundleCodeRequirement(
				brokerRequirement.TeamID, brokerRequirement.SigningIdentifier,
			)
			if requirementErr != nil {
				return graph, fmt.Errorf("FuseKit runtime: render broker designated requirement: %w", requirementErr)
			}
			startBroker := config.brokerStart
			if startBroker == nil {
				startBroker = prepare
			}
			brokerOwner, ownerErr := newBrokerProcessOwner(config.Plan, r.socket, startBroker)
			if ownerErr != nil {
				return graph, fmt.Errorf("FuseKit runtime: create broker process owner: %w", ownerErr)
			}
			brokerPeer := func(ctx context.Context, caller daemonkit.Caller) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				return brokerOwner.VerifyCaller(caller)
			}
			graph.broker, err = catalogservice.NewRuntimeBroker(lifetime, graph.catalog, catalogservice.BrokerIdentity{
				ProductBuild: config.RuntimeBuild, Executable: runtimeBroker.Deployment.Executable,
				DesignatedRequirement:       designatedRequirement,
				EntitlementValidationDigest: brokerRequirement.Digest(),
			}, graph.runtimeOwnerRecord.Generation.String(), brokerOwner)
			if err == nil {
				err = recoverBrokerAfterProcesses(startup, processRecovery, graph.broker)
			}
			if err == nil {
				graph.engine, err = convergence.New(startup, convergence.Config{
					Store: graph.catalog, Notifier: graph.broker,
					RuntimeGeneration: graph.runtimeOwnerRecord.Generation.String(),
					HolderOperation:   causal.OperationID(graph.runtimeOwnerRecord.Generation),
				})
			}
			if err == nil {
				graph.critical, err = newCriticalReadinessCoordinator(
					lifetime, graph.broker, graph.pool, config.Plan.RuntimeExecutable(), runtimeExec,
				)
			}
			if err == nil {
				graph.broker.SetReady(func() { _ = graph.engine.Tick(context.Background()) })
				config := catalogservice.FileProviderConfig{
					Activations: catalogservice.ActivationAdapter{Runtime: graph.tenants, Engine: graph.engine},
					Broker:      graph.broker, Materialization: graph.catalog, CriticalFetches: graph.critical,
					ProtectedPeer: brokerPeer,
				}
				fileProviderConfig = &config
			}
		}
		catalogCore = productionCatalogCore(
			graph.catalog, graph.tenants, graph.engine,
			enabledAuthorityRouter(graph.authorities, sourceRuntimeEnabled), graph.topology,
			config.Owner, config.CatalogAuthorizer, graph.broker, graph.critical,
			graph.runtimeOwnerRecord.Generation.String(),
		)
	}
	if err != nil {
		return graph, fmt.Errorf("FuseKit runtime: configure catalog service: %w", err)
	}

	tenantController := mountmux.BindTenantRuntime(graph.tenants)
	if sourceRuntimeEnabled {
		tenantController = authorityTenantController{tenants: graph.tenants, authorities: graph.authorities}
	}
	var lifecycle mountservice.Runtime
	var nativeService *mountservice.NativeConfig
	if nativeConfigured {
		graph.mount, err = mountmux.New(mountmux.Config{
			Root: paths.PresentationRoot, Tenants: tenantController, Native: graph.native,
			Domains: graph.broker,
		})
		if err != nil {
			return graph, fmt.Errorf("FuseKit runtime: create mount runtime: %w", err)
		}
		nativeCatalog, nativeErr := newNativeCatalog(graph.catalog)
		if nativeErr != nil {
			return graph, fmt.Errorf("FuseKit runtime: create native catalog adapter: %w", nativeErr)
		}
		mountAdapter := mountSessionAdapter{runtime: graph.mount, native: graph.native}
		lifecycle = graph.mount
		nativeService = &mountservice.NativeConfig{
			Sessions: mountAdapter, Catalog: nativeCatalog, ProtectedPeer: runtimePeer,
		}
	} else {
		lifecycle = &tenantLifecycleRuntime{tenants: graph.tenants, domains: graph.broker}
	}
	graph.presentations, err = newPresentationManager(
		lifetime,
		config.CatalogOperationTimeout,
		shutdownTimeout(config.ShutdownTimeout),
		nativePresentationFactory(graph.mount, graph.native),
		brokerPresentationFactory(graph.broker),
	)
	if err != nil {
		return graph, fmt.Errorf("FuseKit runtime: create presentation manager: %w", err)
	}
	lifecycle = presentationLifecycleRuntime{
		next: lifecycle, presentations: graph.presentations,
		lookup: func(id catalog.TenantID) (tenant.TenantSpec, error) {
			for _, spec := range graph.tenants.Specs() {
				if spec.ID == id {
					return spec, nil
				}
			}
			return tenant.TenantSpec{}, tenant.ErrTenantNotFound
		},
	}
	if config.catalogService == nil {
		preparation, ok := catalogCore.Preparation.(catalogservice.PreparationAdapter)
		if !ok {
			return graph, errors.New("FuseKit runtime: production preparation adapter is not exact")
		}
		if nativeConfigured {
			preparation.Mounts = nativePresentationPreparer{
				presentations: graph.presentations,
				route: func(id catalog.TenantID, generation catalog.Generation) error {
					_, routeErr := graph.mount.Route(id, generation)
					return routeErr
				},
			}
		}
		if brokerConfigured && graph.broker != nil {
			preparer := fileProviderPresentationPreparer{presentations: graph.presentations, next: graph.broker}
			preparation.Presentations = preparer
		}
		catalogCore.Preparation = preparation
	}
	graph.tenantLifecycle = lifecycle
	graph.tenantPreparation = catalogCore.Preparation
	graph.presentationLeases = catalogCore.Leases
	graph.sourceFleets = catalogCore.SourceFleets
	graph.activationGeneration = graph.runtimeOwnerRecord.Generation.String()
	tenantOwner, err := tenantOwnerFromProductOwner(config.Owner)
	if err != nil {
		return graph, err
	}
	graph.mountService, err = mountservice.New(mountservice.Config{
		Runtime: lifecycle,
		Authorizer: productTenantLifecycleAuthorizer{
			next: config.Authorizer, owner: tenantOwner,
		},
		Native: nativeService,
	})
	if err != nil {
		return graph, err
	}
	catalogCore.Authorizer = protectedProductAdminAuthorizer{
		next: catalogCore.Authorizer, principal: string(config.Owner),
	}
	graph.catalogService, err = catalogservice.New(catalogCore, fileProviderConfig)
	if err != nil {
		return graph, err
	}
	if graph.engine != nil {
		if err := graph.engine.Pump(startup); err != nil {
			return graph, fmt.Errorf("FuseKit runtime: pump convergence outbox: %w", err)
		}
	}
	graph.topology.Start(lifetime)

	graph.reportHealth = healthReporter(c.Report, config, graph)
	graph.workers = &ownedWorkers{
		mount: graph.mount, tenants: graph.tenants, engine: graph.engine, broker: graph.broker,
		catalog: graph.catalog, authorities: graph.authorities, topology: graph.topology,
		presentations: graph.presentations, pool: graph.pool,
		ledger: graph.ledger, runtimeOwnerRecord: graph.runtimeOwnerRecord,
	}
	readiness.report("published", "ready", nil)
	built = true
	return graph, nil
}

func healthReporter(report func([]byte), config Config, graph *runtimeGraph) func() {
	return func() {
		health := mountservice.RuntimeHealth{NativePhase: mountproto.NativePhaseDisabled}
		if graph.native != nil {
			health = graph.native.RuntimeHealth(graph.activationGeneration)
		}
		health.RuntimeBuild = config.RuntimeBuild
		health.RuntimeProtocol = mountproto.RuntimeProtocolVersion
		health.ProcessGeneration = graph.runtimeOwnerRecord.Generation.String()
		health.ActivationGeneration = graph.activationGeneration
		health.State = mountproto.RuntimeStateHealthy
		health.ReadinessPhase = mountproto.ReadinessPhaseReady
		health.ReadinessStep = mountproto.ReadinessStepPublished
		health.Ready = true
		health.BrokerPhase = mountproto.BrokerPhaseDisabled
		if graph.broker != nil {
			switch graph.broker.ReadinessPhase() {
			case catalogservice.RuntimeBrokerStarting:
				health.BrokerPhase = mountproto.BrokerPhaseStarting
			case catalogservice.RuntimeBrokerLive:
				health.BrokerPhase = mountproto.BrokerPhaseLive
			default:
				health.BrokerPhase = mountproto.BrokerPhaseFailed
			}
		}
		detail, err := json.Marshal(health)
		if err != nil {
			return
		}
		report(detail)
	}
}

func validateConfig(config Config) error {
	requiredWorkers := fixedWorkerReservations(config)
	if config.Plan.SourceCapable() {
		requiredWorkers += sourceObserverReservations
	}
	switch {
	case config.RuntimeBuild == "":
		return errors.New("FuseKit runtime: build is required")
	case config.RuntimeBuild != config.Plan.BuildID():
		return fmt.Errorf("FuseKit runtime: build %q does not match runtime plan build %q", config.RuntimeBuild, config.Plan.BuildID())
	case catalog.ValidateSourceAuthorityFleetOwnerID(config.Owner) != nil:
		return errors.New("FuseKit runtime: immutable product owner is required")
	case config.WorkerLimit < 0 || config.WorkerLimit == 1:
		return errors.New("FuseKit runtime: worker limit must be zero or at least two")
	case workerLimit(config.WorkerLimit) < requiredWorkers:
		return fmt.Errorf(
			"FuseKit runtime: worker limit must reserve %d source/native/catalog/process slots",
			requiredWorkers,
		)
	case config.NativeReadinessTimeout < 0:
		return errors.New("FuseKit runtime: native readiness timeout must not be negative")
	case config.CatalogReadinessTimeout <= 0:
		return errors.New("FuseKit runtime: positive catalog readiness timeout is required")
	case config.CatalogOperationTimeout <= 0:
		return errors.New("FuseKit runtime: positive catalog hard operation timeout is required")
	case config.wireMaxSessions < 0:
		return errors.New("FuseKit runtime: maximum wire sessions must not be negative")
	case config.Authorizer == nil:
		return errors.New("FuseKit runtime: authorizer is required")
	case config.catalogService == nil && config.CatalogAuthorizer == nil:
		return errors.New("FuseKit runtime: catalog authorizer is required")
	}
	if err := config.Plan.validate(); err != nil {
		return err
	}
	_, nativeConfigured := config.Plan.NativePresentation()
	if !nativeConfigured && config.native != nil {
		return errors.New("FuseKit runtime: File Provider-only runtime cannot declare a native controller")
	}
	if nativeConfigured && config.native == nil {
		if err := validateNativeExecutable(config.Plan.RuntimeExecutable()); err != nil {
			return err
		}
	}
	return nil
}

func fixedWorkerReservations(config Config) int {
	result := catalogWorkerReservations + disposableWorkerReserve
	if _, ok := config.Plan.NativePresentation(); ok {
		result += nativeWorkerReservations
	}
	if _, ok := config.Plan.Broker(); ok {
		result += brokerProcessReservations
	}
	return result
}

func validateSourceFleetWorkerCapacity(config Config, fleet SourceAuthorityFleet) error {
	observers := 0
	for _, spec := range fleet.Authorities {
		if _, ok := spec.(PhysicalSourceSpec); ok {
			observers++
		}
	}
	required := fixedWorkerReservations(config) + observers
	if workerLimit(config.WorkerLimit) < required {
		return fmt.Errorf(
			"FuseKit runtime: worker limit %d cannot run %d source observers with %d fixed reservations",
			workerLimit(config.WorkerLimit), observers, fixedWorkerReservations(config),
		)
	}
	return nil
}

func runtimeOwnerRecoveryID(plan RuntimePlan) recoveryid.ID {
	if plan.SourceCapable() {
		return recoveryid.SourceOwner
	}
	return recoveryid.Holder
}

func workerLimit(limit int) int {
	if limit > 0 {
		return limit
	}
	return defaultWorkerLimit
}

func shutdownTimeout(timeout time.Duration) time.Duration {
	if timeout > 0 {
		return timeout
	}
	return defaultShutdownTimeout
}

type runtimeGraph struct {
	workers              *ownedWorkers
	reportHealth         func()
	mount                *mountmux.Runtime
	mountService         *mountservice.Server
	tenantLifecycle      mountservice.Runtime
	tenantPreparation    catalogservice.PreparationService
	presentationLeases   catalogservice.FileProviderLeaseStore
	sourceFleets         catalogservice.SourceFleetService
	activationGeneration string
	catalogService       *catalogservice.Server
	tenants              *tenant.TenantRuntime
	tenantSpecs          tenantSpecSource
	tenantRetirements    tenantRetirementProver
	catalog              *catalogworker.Manager
	pool                 *processOwner
	engine               *convergence.Engine
	broker               *catalogservice.RuntimeBroker
	critical             *criticalReadinessCoordinator
	authorities          *authorityRouter
	topology             *topologyController
	presentations        *presentationManager
	native               nativeController
	ledger               *processLedger
	runtimeOwnerRecord   catalog.ProcessRecord
}

type productTenantLifecycleAuthorizer struct {
	next  mountservice.Authorizer
	owner tenant.OwnerID
}

func tenantOwnerFromProductOwner(owner catalog.SourceAuthorityFleetOwnerID) (tenant.OwnerID, error) {
	if err := catalog.ValidateSourceAuthorityFleetOwnerID(owner); err != nil {
		return "", fmt.Errorf("FuseKit runtime: validate immutable product owner for tenant lifecycle: %w", err)
	}
	return tenant.OwnerID(owner), nil
}

func (a productTenantLifecycleAuthorizer) Authorize(
	ctx context.Context,
	identity mountservice.Identity,
	operation mountproto.Operation,
	tenantID catalog.TenantID,
	generation catalog.Generation,
) (tenant.OwnerID, error) {
	switch operation {
	case mountproto.OperationTenantProvision, mountproto.OperationTenantReplace, mountproto.OperationTenantRemove:
	default:
		return a.next.Authorize(ctx, identity, operation, tenantID, generation)
	}
	owner, err := a.next.Authorize(ctx, identity, operation, tenantID, generation)
	if err != nil {
		return owner, err
	}
	if owner != a.owner {
		return "", fmt.Errorf(
			"%w: tenant lifecycle owner %q is not immutable owner %q",
			mountservice.ErrUnauthorized, owner, a.owner,
		)
	}
	return owner, nil
}

func (a productTenantLifecycleAuthorizer) AuthorizeNative(
	ctx context.Context,
	identity mountservice.Identity,
	operation mountproto.Operation,
) error {
	return a.next.AuthorizeNative(ctx, identity, operation)
}

type protectedProductAdminAuthorizer struct {
	next      catalogservice.Authorizer
	principal string
}

func (a protectedProductAdminAuthorizer) Authorize(
	ctx context.Context,
	identity catalogservice.Identity,
	operation catalogproto.Operation,
	route catalogservice.Route,
) (catalogservice.Authorization, error) {
	authorization, err := a.next.Authorize(ctx, identity, operation, route)
	if err != nil || authorization.Role != catalogservice.RoleProductAdmin {
		return authorization, err
	}
	if authorization.Principal != a.principal {
		return catalogservice.Authorization{}, fmt.Errorf(
			"%w: product admin principal %q is not immutable owner %q",
			mountservice.ErrUnauthorized, authorization.Principal, a.principal,
		)
	}
	return authorization, nil
}

type runtimeReadinessReporter struct {
	stderr               io.Writer
	runtimeBuild         string
	activationGeneration string
}

func (s runtimeReadinessReporter) report(step, result string, err error) {
	if s.stderr == nil {
		return
	}
	if err == nil {
		_, _ = fmt.Fprintf(
			s.stderr,
			"fusekit.runtime_readiness step=%s result=%s runtime_build=%q activation_generation=%q\n",
			step, result, s.runtimeBuild, s.activationGeneration,
		)
		return
	}
	_, _ = fmt.Fprintf(
		s.stderr,
		"fusekit.runtime_readiness step=%s result=%s runtime_build=%q activation_generation=%q error=%q\n",
		step, result, s.runtimeBuild, s.activationGeneration, err,
	)
}

type ownedWorkers struct {
	mount              *mountmux.Runtime
	tenants            *tenant.TenantRuntime
	engine             *convergence.Engine
	broker             *catalogservice.RuntimeBroker
	catalog            *catalogworker.Manager
	authorities        *authorityRouter
	topology           *topologyController
	presentations      *presentationManager
	pool               *processOwner
	ledger             *processLedger
	runtimeOwnerRecord catalog.ProcessRecord

	closeOnce      sync.Once
	cancelOnce     sync.Once
	mu             sync.Mutex
	brokerCloseErr error
	mountCloseErr  error
	wait           terminalSettlement
}

type terminalSettlement struct {
	once sync.Once
	done chan struct{}
	err  error
}

func (s *terminalSettlement) run(
	ctx context.Context,
	settle func() error,
	cancel func(),
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.once.Do(func() {
		s.done = make(chan struct{})
		go func() {
			s.err = settle()
			close(s.done)
		}()
	})
	select {
	case <-s.done:
		return errors.Join(s.err, ctx.Err())
	case <-ctx.Done():
		cancel()
		select {
		case <-s.done:
			return errors.Join(s.err, ctx.Err())
		default:
			return ctx.Err()
		}
	}
}

func (w *ownedWorkers) Close() {
	w.closeOnce.Do(func() {
		if w.tenants != nil {
			w.tenants.Close()
		}
	})
}

func (w *ownedWorkers) Cancel() {
	w.cancelOnce.Do(func() {
		if w.topology != nil {
			w.topology.Cancel()
		}
		if w.tenants != nil {
			w.tenants.Cancel()
		}
		if w.authorities != nil {
			w.authorities.Cancel()
		}
	})
}

func (w *ownedWorkers) Wait(ctx context.Context) error {
	w.Close()
	return w.wait.run(ctx, w.settle, w.Cancel)
}

func (w *ownedWorkers) settle() error {
	background := context.Background()
	var topologyErr error
	if w.topology != nil {
		topologyErr = w.topology.Close(background)
	}
	var presentationErr error
	if w.presentations != nil {
		presentationErr = w.presentations.Close(background)
	} else {
		if w.broker != nil {
			presentationErr = errors.Join(presentationErr, w.broker.Close(background))
		}
		if w.mount != nil {
			presentationErr = errors.Join(presentationErr, w.mount.CloseContext(background))
		}
	}
	var tenantErr error
	if w.tenants != nil {
		tenantErr = w.tenants.Wait(background)
	}
	var authorityErr error
	if w.authorities != nil {
		authorityErr = errors.Join(
			w.authorities.Close(background),
			w.authorities.Wait(background),
		)
	}
	var engineErr error
	if w.engine != nil {
		engineErr = w.engine.Close()
	}
	var catalogErr error
	if w.catalog != nil {
		catalogErr = w.catalog.Close()
	}
	// After every worker that owns a child has closed, so each settlement's
	// durable untrack lands before the runtime owner's own record is retired.
	var childErr error
	if w.pool != nil {
		childErr = w.pool.waitSettled()
	}
	w.mu.Lock()
	w.brokerCloseErr = presentationErr
	w.mountCloseErr = nil
	w.mu.Unlock()
	result := errors.Join(
		presentationErr,
		topologyErr, tenantErr, authorityErr, engineErr, catalogErr, childErr,
	)
	if result == nil && w.ledger != nil {
		result = untrackRuntimeOwner(w.ledger, w.runtimeOwnerRecord)
	}
	return result
}

func closeActivationGraph(graph *runtimeGraph) error {
	if graph == nil {
		return nil
	}
	background := context.Background()
	var result error
	if graph.presentations != nil {
		result = errors.Join(result, graph.presentations.Close(background))
	} else {
		if graph.broker != nil {
			result = errors.Join(result, graph.broker.Close(background))
		}
		if graph.mount != nil {
			result = errors.Join(result, graph.mount.CloseContext(background))
		}
	}
	if graph.topology != nil {
		result = errors.Join(result, graph.topology.Close(background))
	}
	if graph.tenants != nil {
		result = errors.Join(result, closeTenantRuntime(graph.tenants))
	}
	if graph.authorities != nil {
		graph.authorities.Cancel()
		result = errors.Join(
			result,
			graph.authorities.Close(background),
			graph.authorities.Wait(background),
		)
	}
	if graph.engine != nil {
		result = errors.Join(result, graph.engine.Close())
	}
	if graph.catalog != nil {
		result = errors.Join(result, graph.catalog.Close())
	}
	// The failed-activation twin of ownedWorkers.settle's join: a graph that
	// never published still spawned children, and their durable untracking must
	// land before this returns.
	if graph.pool != nil {
		result = errors.Join(result, graph.pool.waitSettled())
	}
	if result == nil && graph.ledger != nil {
		result = untrackRuntimeOwner(graph.ledger, graph.runtimeOwnerRecord)
	}
	return result
}

func untrackRuntimeOwner(ledger *processLedger, owner catalog.ProcessRecord) error {
	if owner.PID == 0 {
		return nil
	}
	return ledger.Untrack(owner)
}

func closeTenantRuntime(runtime *tenant.TenantRuntime) error {
	runtime.Close()
	runtime.Cancel()
	return runtime.Wait(context.Background())
}

func productionCatalogCore(
	store *catalogworker.Manager,
	runtime *tenant.TenantRuntime,
	engine *convergence.Engine,
	authorities *authorityRouter,
	topology *topologyController,
	owner catalog.SourceAuthorityFleetOwnerID,
	authorizer catalogservice.Authorizer,
	presentations catalogservice.FileProviderPresentationPreparer,
	critical catalogservice.CriticalReadinessPreparer,
	activationGeneration string,
) catalogservice.CoreConfig {
	preparation := productionPreparationAdapter(store, runtime, engine, authorities, presentations, critical, activationGeneration)
	return catalogservice.CoreConfig{
		Reader:       catalogservice.CatalogReader{Store: store},
		Mutations:    catalogservice.MutationAdapter{Store: store, Runtime: runtime, Engine: engine},
		Preparation:  preparation,
		Leases:       store,
		SourceFleets: sourceFleetService{store: store, topology: topology, owner: owner},
		Authorizer:   authorizer,
	}
}

func enabledAuthorityRouter(router *authorityRouter, enabled bool) *authorityRouter {
	if !enabled {
		return nil
	}
	return router
}

func productionPreparationAdapter(
	store *catalogworker.Manager,
	runtime *tenant.TenantRuntime,
	engine *convergence.Engine,
	authorities *authorityRouter,
	presentations catalogservice.FileProviderPresentationPreparer,
	critical catalogservice.CriticalReadinessPreparer,
	activationGeneration string,
) catalogservice.PreparationAdapter {
	var barrier sourceauthority.Barrier
	if authorities != nil {
		barrier = preparationBarrier{tenants: runtime, authorities: authorities}
	}
	return catalogservice.PreparationAdapter{
		Runtime: runtime, Engine: engine, Barrier: barrier,
		Presentations: presentations, CriticalObjects: store, PresentationLeases: store,
		CriticalReadiness:    critical,
		ActivationGeneration: activationGeneration,
	}
}

type mountSessionAdapter struct {
	runtime *mountmux.Runtime
	native  nativeController
}

func (a mountSessionAdapter) Bind(ctx context.Context, identity mountservice.Identity) error {
	return a.native.Bind(ctx, identity)
}

func (a mountSessionAdapter) Mounted(
	ctx context.Context,
	identity mountservice.Identity,
	mount mountservice.NativeMountIdentity,
	probeToken string,
) error {
	return a.native.Mounted(ctx, identity, mount, probeToken)
}

func (a mountSessionAdapter) Ready(
	ctx context.Context,
	identity mountservice.Identity,
	proof mountservice.NativeMountProof,
) error {
	return a.native.Ready(ctx, identity, proof)
}

func (a mountSessionAdapter) Unbind(identity mountservice.Identity) { a.native.Unbind(identity) }

func (a mountSessionAdapter) Settled(identity mountservice.Identity, settlement error) {
	a.native.Settled(identity, settlement)
}

func (a mountSessionAdapter) RoutePage(
	ctx context.Context,
	snapshot uint64,
	after string,
	limit int,
) (mountservice.NativeRoutePage, error) {
	page, err := a.runtime.RoutePage(ctx, mountmux.RouteCursor{Snapshot: snapshot, After: after}, limit)
	if err != nil {
		return mountservice.NativeRoutePage{}, err
	}
	result := make([]mountservice.NativeRoute, len(page.Routes))
	for index, route := range page.Routes {
		result[index] = mountservice.NativeRoute{Name: route.Name, Tenant: route.Tenant, Generation: route.Generation}
	}
	response := mountservice.NativeRoutePage{Snapshot: page.Snapshot, Routes: result}
	if page.Next != nil {
		response.Next = page.Next.After
	}
	return response, nil
}

func (a mountSessionAdapter) Pin(ctx context.Context, name string) (mountservice.NativePin, error) {
	pin, err := a.runtime.Pin(ctx, name)
	if err != nil {
		return mountservice.NativePin{}, err
	}
	return mountservice.NativePin{
		Route: mountservice.NativeRoute{Name: pin.Route.Name, Tenant: pin.Route.Tenant, Generation: pin.Route.Generation},
		Spec:  pin.Spec, Release: pin.Release,
	}, nil
}
