package sourceauthority

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"
)

type testSourceSession struct {
	runtime *daemon.Runtime
	client  *wire.Client
	dir     string

	closeOnce sync.Once
	closeErr  error
}

type testSourceSessionClient struct {
	*wire.Client
	closeSession func() error
}

func (c testSourceSessionClient) OpenStream(
	ctx context.Context,
	op wire.Op,
	tenant string,
	payload []byte,
	endInput bool,
) (*wire.ClientCall, error) {
	return c.Open(ctx, op, tenant, payload, endInput)
}

func (c testSourceSessionClient) Close() error {
	if c.closeSession != nil {
		return c.closeSession()
	}
	return c.Client.Close()
}

func (c testSourceSessionClient) Abort(cause error) error {
	abortErr := c.Client.Abort(cause)
	if c.closeSession == nil {
		return abortErr
	}
	return errors.Join(abortErr, c.closeSession())
}

func startTestSourceSession(
	ctx context.Context,
	build string,
	handlers []wire.HandlerSpec,
) (*testSourceSession, error) {
	directory, err := os.MkdirTemp("", "fusekit-source-session-")
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*testSourceSession, error) {
		return nil, errors.Join(cause, os.RemoveAll(directory))
	}
	operations := make([]wire.Op, len(handlers))
	for index, handler := range handlers {
		operations[index] = handler.Op
	}
	ladder, err := spawnedLadder(operations, 30*time.Second)
	if err != nil {
		return fail(err)
	}
	server := &wire.Server{
		WireBuild: build, Ladder: ladder, Workers: 8, Backlog: 8,
		InboundQueue: 16, OutboundQueue: 16, StreamQueue: 4,
		PeerVerificationTimeout: 4 * time.Second,
		HandshakeTimeout:        5 * time.Second, WriteTimeout: 30 * time.Second,
		Log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	for _, handler := range handlers {
		server.Register(handler)
	}
	reaper := func(name string) *proc.Reaper {
		return &proc.Reaper{
			Store: &proc.FileStore{Path: filepath.Join(directory, name+".db")}, Generation: sourceAuthorityOwnerGeneration(name),
			Grace: 10 * time.Millisecond, Settlement: time.Second,
		}
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 8, QueueCapacity: 8, MaxTotalRun: 5 * time.Second,
		MaxStdinBytes: 64 << 10, MaxStdoutBytes: 64 << 10, MaxStderrBytes: 64 << 10,
	}, reaper("workers"))
	if err != nil {
		return fail(err)
	}
	children, err := proc.NewManager(8, reaper("children"))
	if err != nil {
		return fail(err)
	}
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(), AllowUnprotected: true,
	})
	if err != nil {
		return fail(err)
	}
	socket := filepath.Join(directory, "runtime.sock")
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: socket, RuntimeBuild: build, RuntimeProtocol: 1,
		Wire: server, TrustPolicy: policy,
		StopControlStore: &proc.FileStore{Path: filepath.Join(directory, "stop.db")},
		Workers:          workers, Children: children, ShutdownTimeout: 2 * time.Second,
	})
	if err != nil {
		return fail(err)
	}
	slot := daemon.NewPublicationSlot[struct{}](runtime)
	activation, err := runtime.Begin(ctx)
	if err != nil {
		return fail(err)
	}
	publication, err := slot.Stage(activation, struct{}{})
	if err != nil {
		_ = activation.Fail(err)
		return fail(errors.Join(err, runtime.Wait(context.Background())))
	}
	if err := activation.CommitReady(publication); err != nil {
		_ = activation.Fail(err)
		return fail(errors.Join(err, runtime.Wait(context.Background())))
	}
	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(socket), WireBuild: build, Role: trust.UnprotectedRole, Ladder: ladder,
		OutboundQueue: 16, StreamQueue: 4, EventQueue: 8,
	})
	if err != nil {
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return fail(errors.Join(err, runtime.Close(shutdown)))
	}
	return &testSourceSession{runtime: runtime, client: client, dir: directory}, nil
}

func (s *testSourceSession) Close() error {
	s.closeOnce.Do(func() {
		clientErr := s.client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.closeErr = errors.Join(clientErr, s.runtime.Close(ctx), os.RemoveAll(s.dir))
	})
	return s.closeErr
}
