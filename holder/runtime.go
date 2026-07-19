// Package holder composes one signed-app filesystem runtime from daemonkit and FuseKit.
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

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/wire/lifeproto"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/convergence"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

const defaultWorkerLimit = 8

// Config defines the complete process-lifetime holder runtime embedded by one signed app.
type Config struct {
	Plan  Plan
	Build string

	SourceMutation    tenant.SourceMutationPlanner
	CatalogAuthorizer catalogservice.Authorizer
	// WorkerLimit bounds the native child and every disposable tenant worker
	// together. Zero uses eight; one cannot make forward progress.
	WorkerLimit            int
	NativeOptions          []string
	NativeReadinessTimeout time.Duration
	NativeStdout           io.Writer
	NativeStderr           io.Writer
	Authorizer             mountservice.Authorizer

	ShutdownTimeout time.Duration
	Signals         <-chan os.Signal

	native         nativeController
	workerRegistry supervise.WorkerRegistry
	protectedPeer  func(wire.Peer) error
	generation     func() (string, error)
	planner        tenant.Planner
	catalogService func(context.Context, *catalog.Catalog, *tenant.TenantRuntime) (catalogservice.Config, error)
}

// Runtime owns the daemon listener, catalog, tenant actors, workers, and one native root.
type Runtime struct {
	daemon  *daemon.Runtime
	mount   *mountmux.Runtime
	tenants *tenant.TenantRuntime
	catalog *catalog.Catalog
	engine  *convergence.Engine
	broker  *catalogservice.RuntimeBroker
}

// New constructs an unstarted hard-versioned holder runtime.
func New(ctx context.Context, config Config) (*Runtime, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	paths := config.Plan.Paths()
	if err := prepareRuntimeDirectory(config.Plan.home, paths.Directory); err != nil {
		return nil, err
	}
	store, err := catalog.Open(ctx, paths.Catalog)
	if err != nil {
		return nil, fmt.Errorf("holder: open catalog: %w", err)
	}
	registry := config.workerRegistry
	if registry == nil {
		registry, err = processRegistry(paths.ProcessStore, config.generation)
		if err != nil {
			_ = store.Close()
			return nil, err
		}
	}
	workers, err := supervise.NewPool(workerLimit(config.WorkerLimit), registry)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("holder: create worker pool: %w", err)
	}
	planner := config.planner
	if planner == nil {
		planner = tenant.StandardPlanner{
			Executable: config.Plan.Executable(), CatalogPath: paths.Catalog,
			SourceMutation: config.SourceMutation,
		}
	}
	tenants, err := tenant.NewRuntime(ctx, store, workers, planner)
	if err != nil {
		workers.Close()
		workers.Cancel()
		_ = workers.Wait(context.WithoutCancel(ctx))
		_ = store.Close()
		return nil, fmt.Errorf("holder: create tenant runtime: %w", err)
	}
	native := config.native
	if native == nil {
		native = newNativeProcess(nativeProcessConfig{
			start: func(ctx context.Context, spec supervise.ProcessSpec) (managedProcess, error) {
				return workers.Start(ctx, spec)
			},
			socket: paths.Socket, executable: config.Plan.Executable(),
			options: append([]string(nil), config.NativeOptions...), readinessTimeout: config.NativeReadinessTimeout,
			stdout: config.NativeStdout, stderr: config.NativeStderr,
		})
	}
	protectedPeer := config.protectedPeer
	requirement := config.Plan.Requirement()
	if protectedPeer == nil {
		policy := trust.Policy{Requirement: &requirement}
		protectedPeer = policy.Check
	}
	designatedRequirement, err := requirement.DRString()
	if err != nil {
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, fmt.Errorf("holder: render broker designated requirement: %w", err)
	}
	entitlementValidationDigest, err := requirement.ValidationDigest()
	if err != nil {
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, fmt.Errorf("holder: digest broker trust requirement: %w", err)
	}
	server := &wire.Server{Build: transportproto.Build}
	var engine *convergence.Engine
	var broker *catalogservice.RuntimeBroker
	var catalogConfig catalogservice.Config
	if config.catalogService != nil {
		catalogConfig, err = config.catalogService(ctx, store, tenants)
	} else {
		broker, err = catalogservice.NewRuntimeBroker(store, catalogservice.BrokerIdentity{
			ProductBuild: config.Build, Executable: config.Plan.Executable(),
			DesignatedRequirement:       designatedRequirement,
			EntitlementValidationDigest: entitlementValidationDigest,
		})
		if err == nil {
			var persistence *convergence.CatalogPersistence
			persistence, err = convergence.NewCatalogPersistence(store)
			if err == nil {
				engine, err = convergence.New(ctx, convergence.Config{
					Resolver: convergence.CatalogResolver{Catalog: store},
					Notifier: broker, Persistence: persistence,
				})
			}
		}
		if err == nil {
			broker.SetReady(func() { _ = engine.Tick(context.Background()) })
			catalogConfig = productionCatalogService(store, tenants, engine, broker, config.CatalogAuthorizer)
		}
	}
	if err != nil {
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, fmt.Errorf("holder: configure catalog service: %w", err)
	}
	mount, err := mountmux.New(mountmux.Config{
		Root: paths.PresentationRoot, Tenants: mountmux.BindTenantRuntime(tenants), Native: native,
		Domains: broker,
	})
	if err != nil {
		closeConvergence(engine, broker)
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, fmt.Errorf("holder: create mount runtime: %w", err)
	}
	if _, err := mountservice.Register(server, mountservice.Config{
		Runtime: mount, NativeSessions: mountSessionAdapter{runtime: mount, native: native}, Authorizer: config.Authorizer,
		ProtectedNativePeer: protectedPeer,
	}); err != nil {
		closeConvergence(engine, broker)
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, err
	}
	catalogConfig.ProtectedPeer = protectedPeer
	if _, err := catalogservice.Register(server, catalogConfig); err != nil {
		closeConvergence(engine, broker)
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, err
	}
	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Dial: wire.UnixDialer(paths.Socket), Build: transportproto.Build,
	}}
	owned := &ownedWorkers{
		mount: mount, tenants: tenants, engine: engine, broker: broker,
		closeTimeout: shutdownTimeout(config.ShutdownTimeout),
	}
	recoverRuntime := tenants.Recover
	if engine != nil {
		recoverRuntime = func(ctx context.Context) error {
			if err := tenants.Recover(ctx); err != nil {
				return err
			}
			return engine.Drain(ctx)
		}
	}
	daemonRuntime, err := daemon.NewRuntime(daemon.RuntimeConfig{
		Socket: paths.Socket, Build: config.Build, Protocol: lifeproto.Version,
		Peer: peer, Contract: daemon.ResourceOwner, WaitMode: daemon.SocketRelease,
		Admission: &drain.Intake{}, Server: &startingServer{mount: mount, server: server, recover: recoverRuntime},
		Workers: owned, State: store, Resources: peerResource{peer: peer},
		Handoff: owned.handoff, Busy: mount.Busy,
		HealthState:     native.HealthState,
		ShutdownTimeout: config.ShutdownTimeout, Signals: config.Signals,
	})
	if err != nil {
		closeConvergence(engine, broker)
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, fmt.Errorf("holder: create daemon runtime: %w", err)
	}
	server.RegisterLifecycle(daemonRuntime)
	return &Runtime{daemon: daemonRuntime, mount: mount, tenants: tenants, catalog: store, engine: engine, broker: broker}, nil
}

// Run acquires listener ownership, establishes the one native root, and serves until shutdown.
func (r *Runtime) Run(ctx context.Context) error { return r.daemon.Run(ctx) }

// Close requests orderly shutdown and waits for every owned resource to settle.
func (r *Runtime) Close(ctx context.Context) error { return r.daemon.Close(ctx) }

// Health returns the composed daemon and mount-callback state.
func (r *Runtime) Health(ctx context.Context) (daemon.Health, error) { return r.daemon.Health(ctx) }

func validateConfig(config Config) error {
	switch {
	case config.Build == "":
		return errors.New("holder: build is required")
	case config.planner == nil && config.SourceMutation == nil:
		return errors.New("holder: source mutation planner is required")
	case config.WorkerLimit < 0 || config.WorkerLimit == 1:
		return errors.New("holder: worker limit must be zero or at least two")
	case config.NativeReadinessTimeout < 0:
		return errors.New("holder: native readiness timeout must not be negative")
	case config.Authorizer == nil:
		return errors.New("holder: authorizer is required")
	case config.catalogService == nil && config.CatalogAuthorizer == nil:
		return errors.New("holder: catalog authorizer is required")
	}
	if err := config.Plan.validate(); err != nil {
		return err
	}
	if config.native == nil {
		if err := validateNativeExecutable(config.Plan.Executable()); err != nil {
			return err
		}
	}
	return nil
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

type startingServer struct {
	mount   *mountmux.Runtime
	server  *wire.Server
	recover func(context.Context) error
}

func (s *startingServer) Serve(
	ctx context.Context,
	listener net.Listener,
	admit func() (func(), error),
	admitLifecycle func() (func(), error),
) error {
	if err := s.recover(ctx); err != nil {
		return fmt.Errorf("holder: recover tenant runtime: %w", err)
	}
	serveCtx, cancel := context.WithCancel(ctx)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- s.server.Serve(serveCtx, listener, admit, admitLifecycle)
	}()
	if err := s.mount.Start(ctx); err != nil {
		cancel()
		_ = s.server.CloseIntake()
		return errors.Join(fmt.Errorf("holder: start native root: %w", err), <-serveDone)
	}
	select {
	case err := <-serveDone:
		cancel()
		return err
	case <-ctx.Done():
		cancel()
		return <-serveDone
	}
}

func (s *startingServer) CloseIntake() error { return s.server.CloseIntake() }

type ownedWorkers struct {
	mount        *mountmux.Runtime
	tenants      *tenant.TenantRuntime
	engine       *convergence.Engine
	broker       *catalogservice.RuntimeBroker
	closeTimeout time.Duration

	closeOnce sync.Once
	mu        sync.Mutex
	closeErr  error
}

func (w *ownedWorkers) Close() {
	w.closeOnce.Do(func() {
		if w.broker != nil {
			w.broker.Close()
		}
		var engineErr error
		if w.engine != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), w.closeTimeout)
			engineErr = w.engine.Close(closeCtx)
			cancel()
		}
		err := w.mount.Close()
		w.mu.Lock()
		w.closeErr = errors.Join(engineErr, err)
		w.mu.Unlock()
		w.tenants.Close()
	})
}

func (w *ownedWorkers) Cancel() {
	if w.engine != nil {
		w.engine.Cancel()
	}
	if w.broker != nil {
		w.broker.Close()
	}
	w.tenants.Cancel()
}

func (w *ownedWorkers) Wait(ctx context.Context) error {
	var engineErr error
	if w.engine != nil {
		engineErr = w.engine.Wait(ctx)
	}
	tenantErr := w.tenants.Wait(ctx)
	w.mu.Lock()
	closeErr := w.closeErr
	w.mu.Unlock()
	if errors.Is(closeErr, context.Canceled) || errors.Is(closeErr, context.DeadlineExceeded) {
		if retryErr := w.mount.Close(); retryErr == nil {
			w.mu.Lock()
			w.closeErr = nil
			w.mu.Unlock()
			closeErr = nil
		} else {
			closeErr = errors.Join(closeErr, retryErr)
		}
	}
	return errors.Join(closeErr, engineErr, tenantErr)
}

func (w *ownedWorkers) handoff(ctx context.Context) error {
	if err := w.mount.CloseContext(ctx); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closeErr
}

type peerResource struct{ peer io.Closer }

func (r peerResource) Close() error { return r.peer.Close() }

func closeTenantRuntime(runtime *tenant.TenantRuntime) {
	runtime.Close()
	runtime.Cancel()
	_ = runtime.Wait(context.Background())
}

func closeConvergence(engine *convergence.Engine, broker *catalogservice.RuntimeBroker) {
	if broker != nil {
		broker.Close()
	}
	if engine == nil {
		return
	}
	_ = engine.Close(context.Background())
	engine.Cancel()
	_ = engine.Wait(context.Background())
}

func productionCatalogService(
	store *catalog.Catalog,
	runtime *tenant.TenantRuntime,
	engine *convergence.Engine,
	broker *catalogservice.RuntimeBroker,
	authorizer catalogservice.Authorizer,
) catalogservice.Config {
	return catalogservice.Config{
		Reader:      catalogservice.CatalogReader{Catalog: store},
		Mutations:   catalogservice.MutationAdapter{Catalog: store, Runtime: runtime, Engine: engine},
		Sources:     catalogservice.SourceAdapter{Catalog: store, Engine: engine},
		Preparation: catalogservice.PreparationAdapter{Runtime: runtime, Engine: engine},
		Convergence: catalogservice.ConvergenceAdapter{Runtime: runtime, Engine: engine},
		Broker:      broker, Authorizer: authorizer,
	}
}

type mountSessionAdapter struct {
	runtime *mountmux.Runtime
	native  nativeController
}

func (a mountSessionAdapter) Bind(ctx context.Context, identity mountservice.Identity) error {
	return a.native.Bind(ctx, identity)
}

func (a mountSessionAdapter) Ready(ctx context.Context, identity mountservice.Identity) error {
	return a.native.Ready(ctx, identity)
}

func (a mountSessionAdapter) Unbind(identity mountservice.Identity) { a.native.Unbind(identity) }

func (a mountSessionAdapter) Routes(ctx context.Context) ([]mountservice.NativeRoute, error) {
	routes, err := a.runtime.Routes(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]mountservice.NativeRoute, len(routes))
	for index, route := range routes {
		result[index] = mountservice.NativeRoute{Name: route.Name, Tenant: route.Tenant, Generation: route.Generation}
	}
	return result, nil
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
