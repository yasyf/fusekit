package sourceauthority

import (
	"context"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

type sourceSessionClient interface {
	Call(context.Context, wire.Op, string, []byte) (wire.Result, error)
	OpenStream(context.Context, wire.Op, string, []byte, bool) (*wire.ClientCall, error)
	Events() <-chan wire.Event
	Close() error
	Abort(error) error
}

type internalSourceSessionProcess interface {
	openSourceSession(context.Context) (sourceSessionClient, error)
}

var observerSpawnedLimits = wire.SessionLimits{
	Workers: 4, Backlog: 4, MaxFrame: 2 << 20,
	InboundQueue: 8, OutboundQueue: 8, StreamQueue: 4, EventQueue: 1,
	HandshakeTimeout: 5 * time.Second, WriteTimeout: 30 * time.Second,
	CancelSettlementTimeout: 5 * time.Second,
}

var sourceTaskSpawnedLimits = wire.SessionLimits{
	Workers: 8, Backlog: 8, MaxFrame: 2 << 20,
	InboundQueue: 16, OutboundQueue: 16, StreamQueue: 4, EventQueue: 1,
	HandshakeTimeout: 5 * time.Second, WriteTimeout: 30 * time.Second,
	CancelSettlementTimeout: 5 * time.Second,
}

func observerSpawnedLadder() (wire.Ladder, error) {
	return spawnedLadder([]wire.Op{
		fseventsOpOpen, fseventsOpActivate, fseventsOpFlush, fseventsOpAck, fseventsOpClose,
	}, 30*time.Second)
}

func sourceTaskSpawnedLadder() (wire.Ladder, error) {
	return spawnedLadder([]wire.Op{
		sourceTaskOpRootIdentity, sourceTaskOpStat, sourceTaskOpScan, sourceTaskOpMaterialize,
		sourceTaskOpMutation, sourceTaskOpMutationGet, sourceTaskOpMutationAck,
		sourceTaskOpMutationDrop, sourceTaskOpMutationList, sourceTaskOpMutationGC,
	}, 5*time.Minute)
}

func spawnedLadder(operations []wire.Op, serverTimeout time.Duration) (wire.Ladder, error) {
	server := make(map[wire.Op]time.Duration, len(operations))
	client := make(map[wire.Op]time.Duration, len(operations))
	for _, operation := range operations {
		server[operation] = serverTimeout
		client[operation] = serverTimeout + 5*time.Second
	}
	return wire.NewLadder(server, client)
}

func newObserverSpawnedClient(
	ctx context.Context,
	endpoint proc.SpawnedSessionEndpoint,
) (*wire.SpawnedClient, error) {
	ladder, err := observerSpawnedLadder()
	if err != nil {
		return nil, err
	}
	return wire.NewSpawnedClient(ctx, wire.SpawnedClientConfig{
		Endpoint: endpoint, WireBuild: fseventsObserverBuild,
		Ladder: ladder, Limits: observerSpawnedLimits,
	})
}

func newSourceTaskSpawnedClient(
	ctx context.Context,
	endpoint proc.SpawnedSessionEndpoint,
) (*wire.SpawnedClient, error) {
	ladder, err := sourceTaskSpawnedLadder()
	if err != nil {
		return nil, err
	}
	return wire.NewSpawnedClient(ctx, wire.SpawnedClientConfig{
		Endpoint: endpoint, WireBuild: sourceTaskBuild,
		Ladder: ladder, Limits: sourceTaskSpawnedLimits,
	})
}

func openObserverProcessSession(ctx context.Context, process ObserverProcess) (sourceSessionClient, error) {
	if internal, ok := process.(internalSourceSessionProcess); ok {
		return internal.openSourceSession(ctx)
	}
	endpoint, err := process.SessionEndpoint(ctx)
	if err != nil {
		return nil, err
	}
	return newObserverSpawnedClient(ctx, endpoint)
}

func openSourceTaskProcessSession(ctx context.Context, process SourceTaskProcess) (sourceSessionClient, error) {
	if internal, ok := process.(internalSourceSessionProcess); ok {
		return internal.openSourceSession(ctx)
	}
	endpoint, err := process.SessionEndpoint(ctx)
	if err != nil {
		return nil, err
	}
	return newSourceTaskSpawnedClient(ctx, endpoint)
}
