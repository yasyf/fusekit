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
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

const defaultWorkerLimit = 8

// Config defines the complete process-lifetime holder runtime embedded by one signed app.
type Config struct {
	Socket      string
	Root        string
	CatalogPath string
	Build       string

	Planner        tenant.Planner
	WorkerRegistry supervise.WorkerRegistry
	// WorkerLimit bounds the native child and every disposable tenant worker
	// together. Zero uses eight; one cannot make forward progress.
	WorkerLimit int
	// NativeExecutable is the canonical absolute path to this consumer's fixed,
	// signed app executable. Its designated requirement must survive upgrades,
	// and its entry point must implement mountmux.ParseNativeChildArguments.
	NativeExecutable       string
	NativeOptions          []string
	NativeReadinessTimeout time.Duration
	NativeStdout           io.Writer
	NativeStderr           io.Writer
	Authorizer             mountservice.Authorizer
	// PeerRequirement pins the signed consumer UID, designated requirement,
	// Team ID, hardened runtime, and entitlements for every protected session.
	PeerRequirement trust.Requirement
	CatalogService  func(context.Context, *catalog.Catalog, *tenant.TenantRuntime) (catalogservice.Config, error)

	ShutdownTimeout time.Duration
	Signals         <-chan os.Signal

	native     nativeController
	trustCheck func(wire.Peer) error
}

// Runtime owns the daemon listener, catalog, tenant actors, workers, and one native root.
type Runtime struct {
	daemon  *daemon.Runtime
	mount   *mountmux.Runtime
	tenants *tenant.TenantRuntime
	catalog *catalog.Catalog
}

// New constructs an unstarted hard-versioned holder runtime.
func New(ctx context.Context, config Config) (*Runtime, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	store, err := catalog.Open(ctx, config.CatalogPath)
	if err != nil {
		return nil, fmt.Errorf("holder: open catalog: %w", err)
	}
	workers, err := supervise.NewPool(workerLimit(config.WorkerLimit), config.WorkerRegistry)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("holder: create worker pool: %w", err)
	}
	tenants, err := tenant.NewRuntime(store, workers, config.Planner)
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
			socket: config.Socket, executable: config.NativeExecutable,
			options: append([]string(nil), config.NativeOptions...), readinessTimeout: config.NativeReadinessTimeout,
			stdout: config.NativeStdout, stderr: config.NativeStderr,
		})
	}
	mount, err := mountmux.New(mountmux.Config{
		Root: config.Root, Tenants: mountmux.BindTenantRuntime(tenants), Native: native,
	})
	if err != nil {
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, fmt.Errorf("holder: create mount runtime: %w", err)
	}

	trustCheck := config.trustCheck
	if trustCheck == nil {
		policy := trust.Policy{Requirement: &config.PeerRequirement}
		trustCheck = policy.Check
	}
	server := &wire.Server{Build: transportproto.Build, Trust: trustCheck}
	if _, err := mountservice.Register(server, mountservice.Config{
		Runtime: mount, NativeSessions: mountSessionAdapter{runtime: mount, native: native}, Authorizer: config.Authorizer,
	}); err != nil {
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, err
	}
	catalogConfig, err := config.CatalogService(ctx, store, tenants)
	if err != nil {
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, fmt.Errorf("holder: configure catalog service: %w", err)
	}
	if _, err := catalogservice.Register(server, catalogConfig); err != nil {
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, err
	}
	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Dial: wire.UnixDialer(config.Socket), Build: transportproto.Build,
	}}
	owned := &ownedWorkers{mount: mount, tenants: tenants}
	daemonRuntime, err := daemon.NewRuntime(daemon.RuntimeConfig{
		Socket: config.Socket, Build: config.Build, Protocol: lifeproto.Version,
		Peer: peer, Contract: daemon.ResourceOwner, WaitMode: daemon.SocketRelease,
		Admission: &drain.Intake{}, Server: &startingServer{mount: mount, server: server},
		Workers: owned, State: store, Resources: peerResource{peer: peer},
		Handoff: owned.handoff, Busy: mount.Busy,
		HealthState:     native.HealthState,
		ShutdownTimeout: config.ShutdownTimeout, Signals: config.Signals,
	})
	if err != nil {
		closeTenantRuntime(tenants)
		_ = store.Close()
		return nil, fmt.Errorf("holder: create daemon runtime: %w", err)
	}
	server.RegisterLifecycle(daemonRuntime)
	return &Runtime{daemon: daemonRuntime, mount: mount, tenants: tenants, catalog: store}, nil
}

// Run acquires listener ownership, establishes the one native root, and serves until shutdown.
func (r *Runtime) Run(ctx context.Context) error { return r.daemon.Run(ctx) }

// Close requests orderly shutdown and waits for every owned resource to settle.
func (r *Runtime) Close(ctx context.Context) error { return r.daemon.Close(ctx) }

// Health returns the composed daemon and mount-callback state.
func (r *Runtime) Health(ctx context.Context) (daemon.Health, error) { return r.daemon.Health(ctx) }

func validateConfig(config Config) error {
	switch {
	case config.Socket == "":
		return errors.New("holder: socket is required")
	case config.Root == "":
		return errors.New("holder: root is required")
	case config.CatalogPath == "":
		return errors.New("holder: catalog path is required")
	case config.Build == "":
		return errors.New("holder: build is required")
	case config.Planner == nil:
		return errors.New("holder: planner is required")
	case config.WorkerRegistry == nil:
		return errors.New("holder: worker registry is required")
	case config.WorkerLimit < 0 || config.WorkerLimit == 1:
		return errors.New("holder: worker limit must be zero or at least two")
	case config.NativeReadinessTimeout < 0:
		return errors.New("holder: native readiness timeout must not be negative")
	case config.Authorizer == nil:
		return errors.New("holder: authorizer is required")
	case config.CatalogService == nil:
		return errors.New("holder: catalog service is required")
	}
	if config.trustCheck == nil {
		if _, err := config.PeerRequirement.DRString(); err != nil {
			return fmt.Errorf("holder: peer requirement: %w", err)
		}
	}
	if config.native == nil {
		if err := validateNativeExecutable(config.NativeExecutable); err != nil {
			return err
		}
	}
	return errors.Join(
		validateAbsolutePath("socket", config.Socket),
		validateAbsolutePath("root", config.Root),
		validateAbsolutePath("catalog path", config.CatalogPath),
	)
}

func workerLimit(limit int) int {
	if limit > 0 {
		return limit
	}
	return defaultWorkerLimit
}

type startingServer struct {
	mount  *mountmux.Runtime
	server *wire.Server
}

func (s *startingServer) Serve(
	ctx context.Context,
	listener net.Listener,
	admit func() (func(), error),
	admitLifecycle func() (func(), error),
) error {
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
	mount   *mountmux.Runtime
	tenants *tenant.TenantRuntime

	closeOnce sync.Once
	mu        sync.Mutex
	closeErr  error
}

func (w *ownedWorkers) Close() {
	w.closeOnce.Do(func() {
		err := w.mount.Close()
		w.mu.Lock()
		w.closeErr = err
		w.mu.Unlock()
		w.tenants.Close()
	})
}

func (w *ownedWorkers) Cancel() { w.tenants.Cancel() }

func (w *ownedWorkers) Wait(ctx context.Context) error {
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
	return errors.Join(closeErr, tenantErr)
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
