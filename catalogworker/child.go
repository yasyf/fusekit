// Package catalogworker isolates every catalog database and blob-store syscall
// in one daemonkit-managed process generation.
package catalogworker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/transportproto"
)

const childMode = "--fusekit-catalog-worker-v1"

const (
	childSessionWorkers           = 8
	childSessionBacklog           = 32
	childSessionInboundQueue      = 64
	childSessionOutboundQueue     = 128
	childSessionStreamQueue       = 16
	childSessionEventQueue        = 16
	childSessionHandshakeTimeout  = 10 * time.Second
	childSessionWriteTimeout      = 10 * time.Second
	childSessionSettlementTimeout = 5 * time.Second
	childSessionServerDeadline    = 5 * time.Minute
	childSessionClientDeadline    = 6 * time.Minute
)

// ChildArguments returns the exact fixed-executable catalog worker invocation.
func ChildArguments(database, generation, runtime string) ([]string, error) {
	if !filepath.IsAbs(database) || generation == "" ||
		!validNativeWriteToken(runtime) {
		return nil, errors.New("catalog worker: absolute database path and generation/runtime are required")
	}
	return []string{childMode, database, generation, runtime}, nil
}

// RunChild recognizes and serves one catalog worker process generation.
func RunChild(ctx context.Context, arguments []string) (bool, error) {
	if len(arguments) == 0 || arguments[0] != childMode {
		return false, nil
	}
	if len(arguments) != 4 {
		return true, errors.New("catalog worker: malformed child arguments")
	}
	database, generation, runtime := arguments[1], arguments[2], arguments[3]
	if _, err := ChildArguments(database, generation, runtime); err != nil {
		return true, err
	}
	identity, err := proc.ClaimSpawnedSessionIdentity(ctx)
	if err != nil {
		return true, fmt.Errorf("catalog worker: claim spawned session: %w", err)
	}
	if err := proc.CloseInheritedFDs(); err != nil {
		return true, fmt.Errorf("catalog worker: close inherited descriptors: %w", err)
	}
	return true, runChildSession(ctx, database, generation, runtime, identity)
}

func runChildSession(
	ctx context.Context,
	database, generation, runtime string,
	identity proc.SpawnedSessionIdentity,
) error {
	ladder, err := generatedLadder(childSessionServerDeadline, childSessionClientDeadline)
	if err != nil {
		return fmt.Errorf("catalog worker: construct deadline ladder: %w", err)
	}
	return runChildService(ctx, database, generation, runtime, func(runCtx context.Context, handlers []wire.HandlerSpec) error {
		return wire.RunSpawnedSession(runCtx, wire.SpawnedSessionConfig{
			Identity: identity, WireBuild: transportproto.WireBuild,
			Ladder: ladder, Limits: childSessionLimits(childSessionHandshakeTimeout), Handlers: handlers,
		})
	})
}

func runChildService(
	ctx context.Context,
	database, generation, runtime string,
	serve func(context.Context, []wire.HandlerSpec) error,
) error {
	if serve == nil {
		return errors.New("catalog worker: spawned session server is required")
	}
	if err := recoverNativeWrites(database+".native-writes", runtime); err != nil {
		return fmt.Errorf("catalog worker: recover native writes: %w", err)
	}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(context.Canceled)
	identity, err := proc.Probe(os.Getpid())
	if err != nil {
		return fmt.Errorf("catalog worker: probe process identity: %w", err)
	}
	store, err := catalog.OpenWithStorageLimits(runCtx, database, catalog.DefaultStorageLimits())
	if err != nil {
		return fmt.Errorf("catalog worker: open catalog: %w", err)
	}
	defer func() { _ = store.Close() }()
	identityHeader := WorkerIdentity{
		PID: identity.PID, StartTime: identity.StartTime, Boot: identity.Boot, Generation: generation,
	}
	service, err := newServer(runCtx, store, identityHeader, database)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(catalogMaintenanceInterval)
	defer ticker.Stop()
	maintenance, err := startMaintenanceScheduler(
		runCtx, store, &service.mutation, ticker.C, cancelRun,
	)
	if err != nil {
		return err
	}
	service.maintenance = maintenance
	serveErr := serve(runCtx, generatedHandlers(service))
	cancelRun(serveErr)
	maintenanceErr := maintenance.Wait()
	handleErr := service.closeSnapshotHandles()
	if serveErr != nil && !errors.Is(serveErr, context.Canceled) {
		return errors.Join(
			fmt.Errorf("catalog worker: serve: %w", serveErr),
			maintenanceErr,
			handleErr,
		)
	}
	return errors.Join(ctx.Err(), maintenanceErr, handleErr)
}

func childSessionLimits(handshake time.Duration) wire.SessionLimits {
	return wire.SessionLimits{
		Workers: childSessionWorkers, Backlog: childSessionBacklog, MaxFrame: maxFrameSize,
		InboundQueue: childSessionInboundQueue, OutboundQueue: childSessionOutboundQueue,
		StreamQueue: childSessionStreamQueue, EventQueue: childSessionEventQueue,
		HandshakeTimeout: handshake, WriteTimeout: childSessionWriteTimeout,
		CancelSettlementTimeout: childSessionSettlementTimeout,
	}
}
