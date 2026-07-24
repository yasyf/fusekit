// Package holder composes one signed-app filesystem runtime from daemonkit and FuseKit.
package holder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/catalogworker"
	"github.com/yasyf/fusekit/convergence"
	"github.com/yasyf/fusekit/internal/presentationroot"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

const (
	defaultWorkerLimit = 8

	nativeWorkerReservations   = 1
	catalogWorkerReservations  = 1
	disposableWorkerReserve    = 1
	brokerProcessReservations  = 1
	sourceObserverReservations = 1
)

// Config defines the complete process-lifetime holder runtime embedded by one signed app.
type Config struct {
	Plan              RuntimePlan
	RuntimeBuild      string
	TrustRequirements RuntimeTrustRequirements
	// StopControlStore consumes the consumer's exact durable stop receipt.
	StopControlStore *proc.FileStore

	Owner             catalog.SourceAuthorityFleetOwnerID
	Drivers           DriverFactories
	CatalogAuthorizer catalogservice.Authorizer
	// WorkerLimit bounds the native child, catalog worker, source observers,
	// disposable operations, and the signed broker when File Provider is present.
	WorkerLimit             int
	NativeOptions           []string
	NativeReadinessTimeout  time.Duration
	NativeStdout            io.Writer
	NativeStderr            io.Writer
	RuntimeStderr           io.Writer
	SourceReadinessTimeout  time.Duration
	SourceStderr            io.Writer
	CatalogReadinessTimeout time.Duration
	CatalogOperationTimeout time.Duration
	CatalogStderr           io.Writer
	Authorizer              mountservice.Authorizer

	ShutdownTimeout time.Duration
	Signals         <-chan os.Signal

	native              nativeController
	protectedPeer       func(context.Context, wire.Peer) error
	protectedExecutable string
	generation          func() (string, error)
	planner             tenant.Planner
	authorityFactory    authorityRuntimeFactory
	authorityExecutors  authorityExecutorFactory
	semanticFactory     semanticAuthorityFactory
	catalogService      func(context.Context, *catalogworker.Manager, *tenant.TenantRuntime) (catalogservice.CoreConfig, error)
	catalogManager      func(context.Context, catalogworker.ManagerConfig) (*catalogworker.Manager, error)
	brokerStart         brokerProcessStart
	fleetTransitions    tenant.FleetTransitionHook
	wireMaxSessions     int
	peerVerifyTimeout   time.Duration
	currentIdentity     func() (proc.Identity, error)
}

// Runtime owns the daemon listener, catalog, tenant actors, workers, and one native root.
type Runtime struct {
	daemon        *daemon.Runtime
	graphs        *daemon.PublicationSlot[*runtimeGraph]
	config        Config
	paths         RuntimePaths
	server        *wire.Server
	ownerRegistry *durableProcessRegistry
	children      *proc.Manager
	workers       *worker.Pool

	graphMu sync.Mutex
	graph   *runtimeGraph
}

type processRecoverer interface {
	Recover(context.Context) error
}

type brokerRecoverer interface {
	Recover(context.Context) error
}

type processRecoveryProof struct {
	complete bool
}

func recoverProcessGeneration(
	ctx context.Context,
	processes processRecoverer,
) (processRecoveryProof, error) {
	err := processes.Recover(ctx)
	if err != nil {
		return processRecoveryProof{}, err
	}
	return processRecoveryProof{complete: true}, nil
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
	ownerRegistry, err := processRegistry(paths.ProcessStore, config.generation)
	if err != nil {
		return nil, err
	}
	children, err := proc.NewManager(workerLimit(config.WorkerLimit), ownerRegistry.Reaper)
	if err != nil {
		return nil, fmt.Errorf("FuseKit runtime: create process manager: %w", err)
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: workerLimit(config.WorkerLimit), QueueCapacity: workerLimit(config.WorkerLimit),
		MaxTotalRun:   30 * time.Second,
		MaxStdinBytes: 64 << 10, MaxStdoutBytes: 64 << 10, MaxStderrBytes: 64 << 10,
	}, ownerRegistry.Reaper)
	if err != nil {
		return nil, fmt.Errorf("FuseKit runtime: create disposable worker pool: %w", err)
	}
	policy, err := runtimeTrustPolicy(config)
	if err != nil {
		return nil, err
	}
	runtime := &Runtime{
		config: config, paths: paths, server: &wire.Server{
			WireBuild: transportproto.WireBuild, MaxSessions: config.wireMaxSessions,
			PeerVerificationTimeout: config.peerVerifyTimeout,
		},
		ownerRegistry: ownerRegistry, children: children, workers: workers,
	}
	observation, err := mountservice.RuntimeHealthObservation(
		runtimeHealthObservation{runtime: runtime}, config.Authorizer,
	)
	if err != nil {
		return nil, err
	}
	daemonRuntime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: paths.Socket, RuntimeBuild: config.RuntimeBuild, RuntimeProtocol: int(mountproto.RuntimeProtocolVersion),
		Wire: runtime.server, TrustPolicy: policy, StopControlStore: config.StopControlStore,
		Observations: []wire.ObservationRoute{observation},
		Workers:      workers, Children: children,
		ShutdownTimeout: config.ShutdownTimeout, Signals: config.Signals,
	})
	if err != nil {
		return nil, fmt.Errorf("FuseKit runtime: create daemon runtime: %w", err)
	}
	runtime.daemon = daemonRuntime
	runtime.graphs = daemon.NewPublicationSlot[*runtimeGraph](daemonRuntime)
	_, native := config.Plan.NativePresentation()
	if err := mountservice.Register(runtime.server, mountservice.Routes{Native: native}, runtime.resolveMountService); err != nil {
		return nil, fmt.Errorf("FuseKit runtime: register mount routes: %w", err)
	}
	_, fileProvider := config.Plan.Broker()
	if err := catalogservice.Register(runtime.server, catalogservice.Routes{FileProvider: fileProvider}, runtime.resolveCatalogService); err != nil {
		return nil, fmt.Errorf("FuseKit runtime: register catalog routes: %w", err)
	}
	return runtime, nil
}

func (r *Runtime) resolveMountService(request wire.Request) (*mountservice.Server, error) {
	graph, ok := r.graphs.LoadPinned(request.Publication)
	if !ok || graph == nil || graph.mountService == nil {
		return nil, daemon.ErrPublicationStale
	}
	return graph.mountService, nil
}

func (r *Runtime) resolveCatalogService(request wire.Request) (*catalogservice.Server, error) {
	graph, ok := r.graphs.LoadPinned(request.Publication)
	if !ok || graph == nil || graph.catalogService == nil {
		return nil, daemon.ErrPublicationStale
	}
	return graph.catalogService, nil
}

// Run acquires the daemon generation, publishes one exact graph, and joins it.
func (r *Runtime) Run(ctx context.Context) error {
	activation, err := r.daemon.Begin(ctx)
	if err != nil {
		return err
	}
	if err := r.activate(activation, r.config, r.paths); err != nil {
		_ = activation.Fail(err)
		return errors.Join(err, r.daemon.Wait(context.Background()))
	}
	r.graphMu.Lock()
	graph := r.graph
	r.graphMu.Unlock()
	if graph == nil {
		err := errors.New("FuseKit runtime: activation produced no graph")
		_ = activation.Fail(err)
		return errors.Join(err, r.daemon.Wait(context.Background()))
	}
	if err := graph.readiness.BeforeReady(activation.Context()); err != nil {
		graph.readiness.AfterReady(err)
		_ = activation.Fail(err)
		return errors.Join(err, r.settleGraph(), r.daemon.Wait(context.Background()))
	}
	publication, err := r.graphs.Stage(activation, graph)
	if err != nil {
		graph.readiness.AfterReady(err)
		_ = activation.Fail(err)
		return errors.Join(err, r.settleGraph(), r.daemon.Wait(context.Background()))
	}
	if err := activation.CommitReady(publication); err != nil {
		graph.readiness.AfterReady(err)
		_ = activation.Fail(err)
		return errors.Join(err, r.settleGraph(), r.daemon.Wait(context.Background()))
	}
	graph.readiness.AfterReady(nil)
	done := make(chan error, 1)
	go func() { done <- r.daemon.Wait(context.Background()) }()
	select {
	case waitErr := <-done:
		return errors.Join(waitErr, r.settleGraph())
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), shutdownTimeout(r.config.ShutdownTimeout))
		defer cancel()
		closeErr := r.daemon.Close(shutdown)
		waitErr := <-done
		return errors.Join(ctx.Err(), closeErr, waitErr, r.settleGraph())
	}
}

// WaitReady waits for the committed holder graph.
func (r *Runtime) WaitReady(ctx context.Context) error { return r.daemon.WaitReady(ctx) }

// Close drains daemon admission, settles daemon-owned processes, and closes the graph.
func (r *Runtime) Close(ctx context.Context) error {
	err := r.daemon.Close(ctx)
	if err != nil {
		return err
	}
	return r.settleGraph()
}

// Wait joins the daemon and then settles the published graph.
func (r *Runtime) Wait(ctx context.Context) error {
	err := r.daemon.Wait(ctx)
	if err != nil {
		return err
	}
	return r.settleGraph()
}

// Health returns daemonkit's exact lifecycle state.
func (r *Runtime) Health(ctx context.Context) (daemon.Health, error) { return r.daemon.Health(ctx) }

func (r *Runtime) settleGraph() error {
	r.graphMu.Lock()
	graph := r.graph
	r.graph = nil
	r.graphMu.Unlock()
	return closeActivationGraph(graph)
}

func (r *Runtime) activate(
	activation daemon.Activation,
	config Config,
	paths RuntimePaths,
) (err error) {
	startup := activation.Context()
	lifetime := activation.Context()
	graph := &runtimeGraph{}
	graph.bootstrap = &bootstrapGate{}
	built := false
	defer func() {
		if !built {
			err = errors.Join(err, closeActivationGraph(graph))
		}
	}()

	ownerRegistry := r.ownerRegistry
	processRecovery := processRecoveryProof{complete: true}
	currentIdentity := config.currentIdentity
	if currentIdentity == nil {
		currentIdentity = proc.CurrentIdentity
	}
	identity, err := currentIdentity()
	if err != nil {
		return fmt.Errorf("FuseKit runtime: identify current runtime owner: %w", err)
	}
	ownerClass := runtimeOwnerRecoveryClass(config.Plan)
	graph.runtimeOwnerRecord, err = ownerRegistry.RegisterOwner(startup, identity, ownerClass)
	if err != nil {
		return fmt.Errorf("FuseKit runtime: register current runtime owner: %w", err)
	}
	graph.ownerRegistry = ownerRegistry
	graph.pool = r.workers
	graph.children = r.children
	managerFactory := config.catalogManager
	if managerFactory == nil {
		managerFactory = catalogworker.NewManager
	}
	graph.catalog, err = managerFactory(lifetime, catalogworker.ManagerConfig{
		Pool: graph.pool, Executable: config.Plan.RuntimeExecutable(),
		Database: paths.Catalog, Stderr: config.CatalogStderr,
		ReadinessTimeout: config.CatalogReadinessTimeout,
		OperationTimeout: config.CatalogOperationTimeout,
		StopTimeout:      shutdownTimeout(config.ShutdownTimeout),
	})
	if err != nil {
		return fmt.Errorf("FuseKit runtime: create catalog worker manager: %w", err)
	}
	if err := recoverProcessGroupReceipts(startup, ownerRegistry, proc.RecoveryCatalogWorker); err != nil {
		return err
	}
	if err := recoverBrokerReceipts(startup, ownerRegistry, graph.catalog); err != nil {
		return err
	}
	if err := recoverProcessGroupReceipts(startup, ownerRegistry, proc.RecoveryTrust); err != nil {
		return err
	}
	if err := recoverProcessGroupReceipts(startup, ownerRegistry, proc.RecoveryObserver); err != nil {
		return err
	}
	if err := recoverProcessGroupReceipts(startup, ownerRegistry, proc.RecoveryTask); err != nil {
		return err
	}
	if err := recoverProcessGroupReceipts(startup, ownerRegistry, proc.RecoveryNativeMount); err != nil {
		return err
	}
	if err := recoverSourceOwnerReceipts(startup, ownerRegistry, graph.catalog); err != nil {
		return err
	}
	if err := requireNoReceiptLiabilities(
		startup, ownerRegistry, proc.RecoverySourceDriver, proc.RecoveryHolder,
	); err != nil {
		return err
	}
	desired, err := (topologyReconciler{store: graph.catalog, owner: config.Owner}).resnapshot(startup)
	if err != nil {
		return fmt.Errorf("FuseKit runtime: recover desired topology: %w", err)
	}
	sourceFleet, err := config.Drivers.sourceFleet(startup, desired)
	if err != nil {
		return fmt.Errorf("FuseKit runtime: resolve desired source fleet: %w", err)
	}
	if len(sourceFleet.Authorities) != 0 && !config.Plan.SourceCapable() {
		return errors.New("FuseKit runtime: desired source authorities require a source-capable runtime plan")
	}
	graph.authorities = &authorityRouter{}
	sourceRuntimeEnabled := len(config.Drivers.entries) != 0 || desired.Head.Fleet != nil
	launcher := sourceProcessLauncher{
		startSession: func(ctx context.Context, spec supervise.SessionProcessSpec) (managedSessionProcess, error) {
			process, startErr := graph.pool.StartSession(ctx, spec)
			if process == nil {
				return nil, startErr
			}
			return process, startErr
		},
		executable: config.Plan.RuntimeExecutable(), readinessTimeout: config.SourceReadinessTimeout,
		stderr: config.SourceStderr,
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
			return err
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
	graph.tenants, err = tenant.NewRuntime(startup, graph.catalog, graph.pool, planner, fleets, desired.Tenants)
	if err != nil {
		return fmt.Errorf("FuseKit runtime: create tenant runtime: %w", err)
	}
	if initialAuthorities != nil {
		if err := initialAuthorities.start(startup, graph.tenants.Specs()); err != nil {
			return fmt.Errorf("FuseKit runtime: start source authorities: %w", err)
		}
		if err := initialAuthorities.recoverSemanticReceipts(startup); err != nil {
			return fmt.Errorf("FuseKit runtime: recover semantic source receipts: %w", err)
		}
		if err := graph.authorities.installInitial(initialAuthorities); err != nil {
			return err
		}
	}
	if err := requireNoSourceDriverCatalogLiabilities(startup, graph.catalog); err != nil {
		return err
	}
	if err := recoverSourceDriverReceipts(startup, ownerRegistry, graph.catalog); err != nil {
		return err
	}
	if err := recoverHolderReceipts(startup, ownerRegistry); err != nil {
		return err
	}
	if err := requireNoReceiptLiabilities(startup, ownerRegistry); err != nil {
		return err
	}
	if err := graph.tenants.Recover(startup); err != nil {
		return fmt.Errorf("FuseKit runtime: recover tenant runtime: %w", err)
	}
	graph.topology, err = newTopologyController(
		graph.catalog, config.Owner, config.Drivers, graph.authorities,
		buildAuthorities, desired,
	)
	if err != nil {
		return err
	}
	runtimeBroker, brokerConfigured := config.Plan.Broker()
	if err := validatePresentationCapabilities(nativeConfigured, brokerConfigured, graph.tenants.Specs()); err != nil {
		return err
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
		return fmt.Errorf("FuseKit runtime: bind catalog worker tenant preparer: %w", err)
	}

	if nativeConfigured {
		graph.native = config.native
	}
	if nativeConfigured && graph.native == nil {
		library, librarySHA256, ok := config.Plan.FUSELibrary()
		if !ok {
			return errors.New("FuseKit runtime: native presentation lacks FUSE library")
		}
		graph.native = newNativeProcess(nativeProcessConfig{
			start: func(ctx context.Context, spec supervise.ProcessSpec) (managedProcess, error) {
				process, startErr := graph.pool.Start(ctx, spec)
				if process == nil {
					return nil, startErr
				}
				return process, startErr
			},
			confirmMount: func(ctx context.Context, root, token string) error {
				return runNativeMountProbe(
					ctx, graph.pool, config.Plan.RuntimeExecutable(), root, token, config.NativeStderr,
				)
			},
			socket: paths.Socket, executable: config.Plan.RuntimeExecutable(),
			library: library, librarySHA256: librarySHA256, validateLibrary: validateBundledFUSEBytes,
			options: append([]string(nil), config.NativeOptions...), readinessTimeout: config.NativeReadinessTimeout,
			stdout: config.NativeStdout, stderr: config.NativeStderr,
		})
	}
	protectedVerifier := config.protectedPeer
	requirement := config.Plan.RuntimeRequirement()
	if protectedVerifier == nil {
		protectedVerifier = func(ctx context.Context, peer wire.Peer) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return (trust.Policy{Requirement: &requirement}).Check(peer)
		}
	}
	protectedExecutable := config.protectedExecutable
	if protectedExecutable == "" {
		protectedExecutable = config.Plan.RuntimeExecutable()
	}
	runtimePeer := candidateProtectedPeer(protectedExecutable, protectedVerifier)
	var catalogCore catalogservice.CoreConfig
	var fileProviderConfig *catalogservice.FileProviderConfig
	if config.catalogService != nil {
		catalogCore, err = config.catalogService(startup, graph.catalog, graph.tenants)
	} else {
		if brokerConfigured {
			brokerRequirement := runtimeBroker.Requirement
			brokerPeer := candidateProtectedPeer(runtimeBroker.Deployment.Executable, func(ctx context.Context, peer wire.Peer) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				return (trust.Policy{Requirement: &brokerRequirement}).Check(peer)
			})
			designatedRequirement, requirementErr := brokerRequirement.DRString()
			if requirementErr != nil {
				return fmt.Errorf("FuseKit runtime: render broker designated requirement: %w", requirementErr)
			}
			entitlementValidationDigest, digestErr := brokerRequirement.ValidationDigest()
			if digestErr != nil {
				return fmt.Errorf("FuseKit runtime: digest broker trust requirement: %w", digestErr)
			}
			startBroker := config.brokerStart
			if startBroker == nil {
				startBroker = func(ctx context.Context, spec supervise.ProcessSpec) (managedBrokerProcess, error) {
					process, startErr := graph.pool.Start(ctx, spec)
					if process == nil {
						return nil, startErr
					}
					return process, startErr
				}
			}
			brokerOwner, ownerErr := newBrokerProcessOwner(config.Plan, startBroker)
			if ownerErr != nil {
				return fmt.Errorf("FuseKit runtime: create broker process owner: %w", ownerErr)
			}
			graph.broker, err = catalogservice.NewRuntimeBroker(lifetime, graph.catalog, catalogservice.BrokerIdentity{
				ProductBuild: config.RuntimeBuild, Executable: runtimeBroker.Deployment.Executable,
				DesignatedRequirement:       designatedRequirement,
				EntitlementValidationDigest: entitlementValidationDigest,
			}, graph.runtimeOwnerRecord.Generation, brokerOwner)
			if err == nil {
				err = recoverBrokerAfterProcesses(startup, processRecovery, graph.broker)
			}
			if err == nil {
				var persistence *convergence.CatalogPersistence
				persistence, err = convergence.NewCatalogPersistence(graph.catalog)
				if err == nil {
					var resolver *convergence.CatalogResolver
					resolver, err = convergence.NewCatalogResolver(graph.catalog, nil)
					if err != nil {
						return fmt.Errorf("FuseKit runtime: create convergence resolver: %w", err)
					}
					graph.engine, err = convergence.New(startup, convergence.Config{
						Resolver: resolver,
						Notifier: graph.broker, Persistence: persistence,
					})
				}
			}
			if err == nil {
				graph.broker.SetReady(func() { _ = graph.engine.Tick(context.Background()) })
				config := catalogservice.FileProviderConfig{
					Preparation: productionPreparationAdapter(
						graph.tenants, graph.engine, enabledAuthorityRouter(graph.authorities, sourceRuntimeEnabled),
						graph.broker, graph.runtimeOwnerRecord.Generation,
					),
					Convergence: catalogservice.ConvergenceAdapter{Runtime: graph.tenants, Engine: graph.engine},
					Broker:      graph.broker, ProtectedPeer: brokerPeer,
				}
				fileProviderConfig = &config
			}
		}
		catalogCore = productionCatalogCore(
			graph.catalog, graph.tenants, graph.engine,
			enabledAuthorityRouter(graph.authorities, sourceRuntimeEnabled), graph.topology,
			config.Owner, config.CatalogAuthorizer, graph.broker, graph.runtimeOwnerRecord.Generation,
		)
	}
	if err != nil {
		return fmt.Errorf("FuseKit runtime: configure catalog service: %w", err)
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
			return fmt.Errorf("FuseKit runtime: create mount runtime: %w", err)
		}
		nativeCatalog, nativeErr := newNativeCatalog(graph.catalog)
		if nativeErr != nil {
			return fmt.Errorf("FuseKit runtime: create native catalog adapter: %w", nativeErr)
		}
		mountAdapter := mountSessionAdapter{runtime: graph.mount, native: graph.native}
		lifecycle = graph.mount
		nativeService = &mountservice.NativeConfig{
			Sessions: mountAdapter, Catalog: nativeCatalog, ProtectedPeer: runtimePeer,
		}
	} else {
		lifecycle = &tenantLifecycleRuntime{tenants: graph.tenants, domains: graph.broker}
	}
	tenantOwner, err := tenantOwnerFromProductOwner(config.Owner)
	if err != nil {
		return err
	}
	graph.mountService, err = mountservice.New(mountservice.Config{
		Runtime: lifecycle,
		Authorizer: productTenantLifecycleAuthorizer{
			next: config.Authorizer, owner: tenantOwner,
		},
		Native: nativeService,
	})
	if err != nil {
		return err
	}
	catalogCore.Authorizer = protectedProductAdminAuthorizer{
		next: catalogCore.Authorizer, principal: string(config.Owner), protectedPeer: runtimePeer,
	}
	graph.catalogService, err = catalogservice.New(catalogCore, fileProviderConfig)
	if err != nil {
		return err
	}
	if graph.engine != nil {
		if err := graph.engine.Drain(startup); err != nil {
			return fmt.Errorf("FuseKit runtime: drain convergence outbox: %w", err)
		}
	}
	graph.topology.Start(lifetime)

	graph.readiness = &runtimeReadiness{
		mount: graph.mount, bootstrap: graph.bootstrap, broker: graph.broker,
		stderr: config.RuntimeStderr, runtimeBuild: config.RuntimeBuild,
		activationGeneration: graph.runtimeOwnerRecord.Generation,
		settle: func(ctx context.Context) error {
			return requireNoReceiptLiabilities(ctx, ownerRegistry)
		},
	}
	graph.workers = &ownedWorkers{
		mount: graph.mount, tenants: graph.tenants, engine: graph.engine, broker: graph.broker,
		catalog: graph.catalog, authorities: graph.authorities, topology: graph.topology,
		ownerRegistry: graph.ownerRegistry, runtimeOwnerRecord: graph.runtimeOwnerRecord,
	}
	r.graphMu.Lock()
	r.graph = graph
	r.graphMu.Unlock()
	built = true
	return nil
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
	case config.StopControlStore == nil:
		return errors.New("FuseKit runtime: stop-control store is required")
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
	case config.SourceReadinessTimeout < 0:
		return errors.New("FuseKit runtime: source readiness timeout must not be negative")
	case config.CatalogReadinessTimeout <= 0:
		return errors.New("FuseKit runtime: positive catalog readiness timeout is required")
	case config.CatalogOperationTimeout <= 0:
		return errors.New("FuseKit runtime: positive catalog hard operation timeout is required")
	case config.peerVerifyTimeout < 0:
		return errors.New("FuseKit runtime: peer verification timeout must not be negative")
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

func runtimeOwnerRecoveryClass(plan RuntimePlan) proc.RecoveryClass {
	if plan.SourceCapable() {
		return proc.RecoverySourceOwner
	}
	return proc.RecoveryHolder
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
	return daemon.DefaultShutdownTimeout
}

type bootstrapPhase uint32

const (
	bootstrapStarting bootstrapPhase = iota
	bootstrapPublishing
	bootstrapReady
	bootstrapFailed
)

type bootstrapStep uint32

const (
	bootstrapListener bootstrapStep = iota
	bootstrapNative
	bootstrapBroker
	bootstrapReceipts
	bootstrapPublished
)

const bootstrapPhaseShift = 8

type bootstrapGate struct{ state atomic.Uint32 }

func bootstrapState(phase bootstrapPhase, step bootstrapStep) uint32 {
	return uint32(phase)<<bootstrapPhaseShift | uint32(step)
}

func (g *bootstrapGate) advance(step bootstrapStep) {
	g.state.Store(bootstrapState(bootstrapStarting, step))
}

func (g *bootstrapGate) open() { g.state.Store(bootstrapState(bootstrapReady, bootstrapPublished)) }

func (g *bootstrapGate) publish() {
	g.state.Store(bootstrapState(bootstrapPublishing, bootstrapReceipts))
}

func (g *bootstrapGate) fail() {
	for {
		current := g.state.Load()
		step := bootstrapStep(current & ((1 << bootstrapPhaseShift) - 1))
		if step == bootstrapPublished {
			step = bootstrapReceipts
		}
		if g.state.CompareAndSwap(current, bootstrapState(bootstrapFailed, step)) {
			return
		}
	}
}

func (g *bootstrapGate) current() bootstrapPhase {
	return bootstrapPhase(g.state.Load() >> bootstrapPhaseShift)
}

func (g *bootstrapGate) readiness() (mountproto.ReadinessPhase, mountproto.ReadinessStep) {
	state := g.state.Load()
	var phase mountproto.ReadinessPhase
	switch bootstrapPhase(state >> bootstrapPhaseShift) {
	case bootstrapStarting:
		phase = mountproto.ReadinessPhaseStarting
	case bootstrapPublishing:
		phase = mountproto.ReadinessPhaseStarting
	case bootstrapReady:
		phase = mountproto.ReadinessPhaseReady
	default:
		phase = mountproto.ReadinessPhaseFailed
	}
	var step mountproto.ReadinessStep
	switch bootstrapStep(state & ((1 << bootstrapPhaseShift) - 1)) {
	case bootstrapListener:
		step = mountproto.ReadinessStepListener
	case bootstrapNative:
		step = mountproto.ReadinessStepNative
	case bootstrapBroker:
		step = mountproto.ReadinessStepBroker
	case bootstrapReceipts:
		step = mountproto.ReadinessStepReceipts
	default:
		step = mountproto.ReadinessStepPublished
	}
	return phase, step
}

type runtimeGraph struct {
	readiness          *runtimeReadiness
	workers            *ownedWorkers
	bootstrap          *bootstrapGate
	mount              *mountmux.Runtime
	mountService       *mountservice.Server
	catalogService     *catalogservice.Server
	tenants            *tenant.TenantRuntime
	catalog            *catalogworker.Manager
	pool               *worker.Pool
	children           *proc.Manager
	engine             *convergence.Engine
	broker             *catalogservice.RuntimeBroker
	authorities        *authorityRouter
	topology           *topologyController
	native             nativeController
	ownerRegistry      *durableProcessRegistry
	runtimeOwnerRecord proc.Record
}

type productTenantLifecycleAuthorizer struct {
	next  mountservice.Authorizer
	owner tenant.OwnerID
}

func (a productTenantLifecycleAuthorizer) AuthorizeObservation(
	ctx context.Context,
	identity mountservice.ObservationIdentity,
	operation mountproto.Operation,
) error {
	return a.next.AuthorizeObservation(ctx, identity, operation)
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
			trust.ErrUntrustedPeer, owner, a.owner,
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
	next          catalogservice.Authorizer
	principal     string
	protectedPeer func(context.Context, wire.Peer) error
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
			trust.ErrUntrustedPeer, authorization.Principal, a.principal,
		)
	}
	if a.protectedPeer == nil {
		return catalogservice.Authorization{}, errors.New("FuseKit runtime: product admin protected-peer verifier is required")
	}
	if err := a.protectedPeer(ctx, identity.Peer); err != nil {
		return catalogservice.Authorization{}, fmt.Errorf("FuseKit runtime: authenticate product admin: %w", err)
	}
	return authorization, nil
}

func candidateProtectedPeer(executable string, verify func(context.Context, wire.Peer) error) func(context.Context, wire.Peer) error {
	return func(ctx context.Context, peer wire.Peer) error {
		if peer.Executable != executable {
			return fmt.Errorf("%w: executable %q is not %q", trust.ErrUntrustedPeer, peer.Executable, executable)
		}
		return verify(ctx, peer)
	}
}

type runtimeReadiness struct {
	mount     *mountmux.Runtime
	bootstrap *bootstrapGate
	broker    *catalogservice.RuntimeBroker
	settle    func(context.Context) error
	stderr    io.Writer

	runtimeBuild         string
	activationGeneration string
}

func (s *runtimeReadiness) reportReadiness(step, result string, err error) {
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

func (s *runtimeReadiness) BeforeReady(ctx context.Context) error {
	s.reportReadiness("listener", "starting", nil)
	s.reportReadiness("listener", "live", nil)
	if s.mount != nil {
		s.bootstrap.advance(bootstrapNative)
		s.reportReadiness("native", "starting", nil)
		if err := s.mount.Start(ctx); err != nil {
			s.reportReadiness("native", "failed", err)
			s.bootstrap.fail()
			return fmt.Errorf("FuseKit runtime: start native root: %w", err)
		}
		s.reportReadiness("native", "live", nil)
	} else {
		s.reportReadiness("native", "disabled", nil)
	}
	if s.broker != nil {
		s.bootstrap.advance(bootstrapBroker)
		s.reportReadiness("broker", "starting", nil)
		if err := s.broker.Start(ctx); err != nil {
			s.reportReadiness("broker", "failed", err)
			s.bootstrap.fail()
			return fmt.Errorf("FuseKit runtime: start signed broker: %w", err)
		}
		s.reportReadiness("broker", "live", nil)
	} else {
		s.reportReadiness("broker", "disabled", nil)
	}
	s.bootstrap.advance(bootstrapReceipts)
	s.reportReadiness("receipts", "settling", nil)
	if s.settle == nil {
		err := errors.New("FuseKit runtime: receipt settlement barrier is required")
		s.reportReadiness("receipts", "failed", err)
		s.bootstrap.fail()
		return err
	}
	if err := s.settle(ctx); err != nil {
		s.reportReadiness("receipts", "failed", err)
		s.bootstrap.fail()
		return fmt.Errorf("FuseKit runtime: settle process recovery receipts: %w", err)
	}
	s.reportReadiness("receipts", "settled", nil)
	s.bootstrap.publish()
	s.reportReadiness("published", "publishing", nil)
	return nil
}

func (s *runtimeReadiness) AfterReady(err error) {
	if err != nil {
		s.reportReadiness("published", "failed", err)
		s.bootstrap.fail()
		return
	}
	s.bootstrap.open()
	s.reportReadiness("published", "ready", nil)
}

func (s *runtimeReadiness) Published() bool {
	return s.bootstrap.current() == bootstrapReady
}

type ownedWorkers struct {
	mount              *mountmux.Runtime
	tenants            *tenant.TenantRuntime
	engine             *convergence.Engine
	broker             *catalogservice.RuntimeBroker
	catalog            *catalogworker.Manager
	authorities        *authorityRouter
	topology           *topologyController
	ownerRegistry      *durableProcessRegistry
	runtimeOwnerRecord proc.Record

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
		if w.engine != nil {
			w.engine.Cancel()
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
	var brokerErr error
	if w.broker != nil {
		brokerErr = w.broker.Close(background)
	}
	var mountErr error
	if w.mount != nil {
		mountErr = w.mount.CloseContext(background)
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
		closeErr := w.engine.Close(background)
		if errors.Is(closeErr, convergence.ErrClosed) {
			closeErr = nil
		}
		engineErr = errors.Join(closeErr, w.engine.Wait(background))
	}
	var catalogErr error
	if w.catalog != nil {
		catalogErr = w.catalog.Close()
	}
	w.mu.Lock()
	w.brokerCloseErr = brokerErr
	w.mountCloseErr = mountErr
	w.mu.Unlock()
	result := errors.Join(
		brokerErr, mountErr,
		topologyErr, tenantErr, authorityErr, engineErr, catalogErr,
	)
	if result == nil && w.ownerRegistry != nil {
		result = untrackRuntimeOwner(background, w.ownerRegistry, w.runtimeOwnerRecord)
	}
	return result
}

func (w *ownedWorkers) handoff(ctx context.Context) error {
	if w.mount != nil {
		if err := w.mount.CloseContext(ctx); err != nil {
			return err
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return errors.Join(w.brokerCloseErr, w.mountCloseErr)
}

func closeActivationGraph(graph *runtimeGraph) error {
	if graph == nil {
		return nil
	}
	background := context.Background()
	var result error
	if graph.broker != nil {
		result = errors.Join(result, graph.broker.Close(background))
	}
	if graph.mount != nil {
		result = errors.Join(result, graph.mount.CloseContext(background))
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
		graph.engine.Cancel()
		closeErr := graph.engine.Close(background)
		if errors.Is(closeErr, convergence.ErrClosed) {
			closeErr = nil
		}
		result = errors.Join(result, closeErr, graph.engine.Wait(background))
	}
	if graph.catalog != nil {
		result = errors.Join(result, graph.catalog.Close())
	}
	if result == nil && graph.ownerRegistry != nil {
		result = untrackRuntimeOwner(background, graph.ownerRegistry, graph.runtimeOwnerRecord)
	}
	return result
}

func untrackRuntimeOwner(
	ctx context.Context,
	registry *durableProcessRegistry,
	owner proc.Record,
) error {
	if owner.PID == 0 {
		return nil
	}
	return registry.Untrack(ctx, owner)
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
	activationGeneration string,
) catalogservice.CoreConfig {
	preparation := productionPreparationAdapter(runtime, engine, authorities, presentations, activationGeneration)
	return catalogservice.CoreConfig{
		Reader:       catalogservice.CatalogReader{Store: store},
		Mutations:    catalogservice.MutationAdapter{Store: store, Runtime: runtime, Engine: engine},
		Preparation:  preparation,
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
	runtime *tenant.TenantRuntime,
	engine *convergence.Engine,
	authorities *authorityRouter,
	presentations catalogservice.FileProviderPresentationPreparer,
	activationGeneration string,
) catalogservice.PreparationAdapter {
	var barrier sourceauthority.Barrier
	if authorities != nil {
		barrier = preparationBarrier{tenants: runtime, authorities: authorities}
	}
	return catalogservice.PreparationAdapter{
		Runtime: runtime, Engine: engine, Barrier: barrier,
		Presentations: presentations, ActivationGeneration: activationGeneration,
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

type runtimeHealthObservation struct {
	runtime *Runtime
}

func (a runtimeHealthObservation) Health(ctx context.Context) (mountservice.RuntimeHealth, error) {
	if a.runtime == nil {
		return mountservice.RuntimeHealth{}, errors.New("FuseKit runtime: runtime is nil")
	}
	if a.runtime.daemon == nil {
		return mountservice.RuntimeHealth{}, errors.New("FuseKit runtime: daemon runtime is nil")
	}
	daemonHealth, err := a.runtime.daemon.Health(ctx)
	if err != nil {
		return mountservice.RuntimeHealth{}, fmt.Errorf("FuseKit runtime: observe daemon runtime health: %w", err)
	}
	if daemonHealth.RuntimeBuild == "" {
		return mountservice.RuntimeHealth{}, errors.New("FuseKit runtime: runtime build is empty")
	}
	if daemonHealth.RuntimeProtocol != int(mountproto.RuntimeProtocolVersion) {
		return mountservice.RuntimeHealth{}, fmt.Errorf(
			"FuseKit runtime: runtime protocol %d is not exact version %d",
			daemonHealth.RuntimeProtocol, mountproto.RuntimeProtocolVersion,
		)
	}
	if daemonHealth.PID <= 0 {
		return mountservice.RuntimeHealth{}, errors.New("FuseKit runtime: runtime PID is invalid")
	}
	if daemonHealth.ProcessGeneration == "" {
		return mountservice.RuntimeHealth{}, errors.New("FuseKit runtime: process generation is empty")
	}
	graph, ok := a.runtime.graphs.Load()
	if !ok {
		return mountservice.RuntimeHealth{}, daemon.ErrPublicationUnavailable
	}
	record := graph.runtimeOwnerRecord
	if record.Generation == "" {
		return mountservice.RuntimeHealth{}, errors.New("FuseKit runtime: runtime owner generation is empty")
	}
	state, err := mountRuntimeState(daemonHealth.State)
	if err != nil {
		return mountservice.RuntimeHealth{}, err
	}
	health := mountservice.RuntimeHealth{NativePhase: mountproto.NativePhaseDisabled}
	if graph.native != nil {
		health = graph.native.RuntimeHealth(record.Generation)
	}
	health.RuntimeBuild = daemonHealth.RuntimeBuild
	health.RuntimeProtocol = mountproto.RuntimeProtocolVersion
	health.RuntimePID = int64(daemonHealth.PID)
	health.ProcessGeneration = daemonHealth.ProcessGeneration
	health.ActivationGeneration = record.Generation
	health.State = state
	health.Draining = daemonHealth.Draining
	health.Busy = daemonHealth.Busy
	health.ReadinessPhase, health.ReadinessStep = graph.bootstrap.readiness()
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
	if health.ReadinessPhase == mountproto.ReadinessPhaseReady &&
		health.NativePhase != mountproto.NativePhaseDisabled && health.NativePhase != mountproto.NativePhaseLive {
		health.ReadinessStep = mountproto.ReadinessStepNative
		if health.NativePhase == mountproto.NativePhaseIdle || health.NativePhase == mountproto.NativePhaseStarting {
			health.ReadinessPhase = mountproto.ReadinessPhaseStarting
		} else {
			health.ReadinessPhase = mountproto.ReadinessPhaseFailed
		}
	}
	if health.ReadinessPhase == mountproto.ReadinessPhaseReady &&
		health.BrokerPhase != mountproto.BrokerPhaseDisabled && health.BrokerPhase != mountproto.BrokerPhaseLive {
		health.ReadinessStep = mountproto.ReadinessStepBroker
		if health.BrokerPhase == mountproto.BrokerPhaseStarting {
			health.ReadinessPhase = mountproto.ReadinessPhaseStarting
		} else {
			health.ReadinessPhase = mountproto.ReadinessPhaseFailed
		}
	}
	if daemonHealth.Draining {
		health.State = mountproto.RuntimeStateDraining
		health.ReadinessPhase = mountproto.ReadinessPhaseDraining
		health.ReadinessStep = mountproto.ReadinessStepPublished
	} else if health.ReadinessPhase == mountproto.ReadinessPhaseReady && !daemonHealth.Ready {
		health.ReadinessPhase = mountproto.ReadinessPhaseStarting
		health.ReadinessStep = mountproto.ReadinessStepReceipts
	} else if health.State == mountproto.RuntimeStateFailed {
		health.ReadinessPhase = mountproto.ReadinessPhaseFailed
	}
	nativeReady := health.NativePhase == mountproto.NativePhaseDisabled ||
		health.NativePhase == mountproto.NativePhaseLive && health.NativeMount != nil
	health.Ready = daemonHealth.Ready && !health.Draining &&
		health.ReadinessPhase == mountproto.ReadinessPhaseReady &&
		health.ReadinessStep == mountproto.ReadinessStepPublished &&
		nativeReady &&
		(health.BrokerPhase == mountproto.BrokerPhaseDisabled || health.BrokerPhase == mountproto.BrokerPhaseLive)
	return health, nil
}

func mountRuntimeState(state daemon.State) (mountproto.RuntimeState, error) {
	switch state {
	case daemon.StateHealthy:
		return mountproto.RuntimeStateHealthy, nil
	case daemon.StateDegraded:
		return mountproto.RuntimeStateDegraded, nil
	case daemon.StateFailed:
		return mountproto.RuntimeStateFailed, nil
	default:
		return "", fmt.Errorf("FuseKit runtime: invalid runtime state %q", state)
	}
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

var _ supervise.WorkerRegistry = (*proc.Reaper)(nil)
