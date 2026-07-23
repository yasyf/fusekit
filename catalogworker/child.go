// Package catalogworker isolates every catalog database and blob-store syscall
// in one daemonkit-managed process generation.
package catalogworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/transportproto"
)

const childMode = "--fusekit-catalog-worker-v1"

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
	conn, err := wire.NewDuplexConn(os.Stdin, os.Stdout)
	if err != nil {
		return true, fmt.Errorf("catalog worker: open managed session: %w", err)
	}
	parent, err := wire.SpawnedParentSessionIdentity()
	if err != nil {
		_ = conn.Close()
		return true, err
	}
	return true, runChildSession(ctx, database, generation, runtime, conn, parent)
}

func runChildSession(
	ctx context.Context,
	database, generation, runtime string,
	conn net.Conn,
	parent wire.SessionIdentity,
) error {
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
	server := &wire.Server{
		WireBuild: transportproto.WireBuild, MaxFrame: maxFrameSize,
		Log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	service, err := register(runCtx, server, store, identityHeader, database)
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
	admit := func() (func(), error) { return func() {}, nil }
	ready := func() error { return nil }
	serveErr := server.ServeSession(runCtx, conn, parent, ready, admit, admit)
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
