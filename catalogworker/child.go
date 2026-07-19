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
	"strconv"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/transportproto"
)

const childMode = "--fusekit-catalog-worker-v1"

// ChildArguments returns the exact fixed-executable catalog worker invocation.
func ChildArguments(socket, database, generation, runtime string) ([]string, error) {
	if !filepath.IsAbs(socket) || !filepath.IsAbs(database) || generation == "" ||
		!validNativeWriteToken(runtime) {
		return nil, errors.New("catalog worker: absolute socket/database paths and generation/runtime are required")
	}
	return []string{childMode, socket, database, generation, runtime}, nil
}

// RunChild recognizes and serves one catalog worker process generation.
func RunChild(ctx context.Context, arguments []string) (bool, error) {
	if len(arguments) == 0 || arguments[0] != childMode {
		return false, nil
	}
	if len(arguments) != 5 {
		return true, errors.New("catalog worker: malformed child arguments")
	}
	socket, database, generation, runtime := arguments[1], arguments[2], arguments[3], arguments[4]
	if _, err := ChildArguments(socket, database, generation, runtime); err != nil {
		return true, err
	}
	if err := recoverNativeWrites(database+".native-writes", runtime); err != nil {
		return true, fmt.Errorf("catalog worker: recover native writes: %w", err)
	}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(context.Canceled)
	identity, err := proc.Probe(os.Getpid())
	if err != nil {
		return true, fmt.Errorf("catalog worker: probe process identity: %w", err)
	}
	store, err := catalog.OpenWithStorageLimits(runCtx, database, catalog.DefaultStorageLimits())
	if err != nil {
		return true, fmt.Errorf("catalog worker: open catalog: %w", err)
	}
	defer func() { _ = store.Close() }()
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return true, fmt.Errorf("catalog worker: remove stale socket: %w", err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return true, fmt.Errorf("catalog worker: listen: %w", err)
	}
	defer func() { _ = listener.Close() }()
	defer func() { _ = os.Remove(socket) }()
	if err := os.Chmod(socket, 0o600); err != nil {
		return true, fmt.Errorf("catalog worker: secure socket: %w", err)
	}
	identityHeader := WorkerIdentity{
		PID: identity.PID, StartTime: identity.StartTime, Boot: identity.Boot, Generation: generation,
	}
	server := &wire.Server{
		Build: transportproto.Build, MaxFrame: maxFrameSize,
		Log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	service, err := register(runCtx, server, store, identityHeader, database)
	if err != nil {
		return true, err
	}
	ticker := time.NewTicker(catalogMaintenanceInterval)
	defer ticker.Stop()
	maintenance, err := startMaintenanceScheduler(
		runCtx, store, &service.mutation, ticker.C, cancelRun,
	)
	if err != nil {
		return true, err
	}
	service.maintenance = maintenance
	admit := func() (func(), error) { return func() {}, nil }
	ready := func() error { return nil }
	serveErr := server.Serve(runCtx, listener, ready, admit, admit)
	cancelRun(serveErr)
	maintenanceErr := maintenance.Wait()
	handleErr := service.closeSnapshotHandles()
	if serveErr != nil && !errors.Is(serveErr, context.Canceled) {
		return true, errors.Join(
			fmt.Errorf("catalog worker: serve: %w", serveErr),
			maintenanceErr,
			handleErr,
		)
	}
	return true, errors.Join(ctx.Err(), maintenanceErr, handleErr)
}

func generationSocket(base string, generation uint64) string {
	return base + ".catalog-worker-" + strconv.FormatUint(generation, 16) + ".sock"
}
