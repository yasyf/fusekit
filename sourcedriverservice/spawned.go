package sourcedriverservice

import (
	"context"
	"errors"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/sourcedriverproto"
)

var spawnedLimits = wire.SessionLimits{
	Workers: 4, Backlog: 8, MaxFrame: 2 << 20,
	InboundQueue: 16, OutboundQueue: 16, StreamQueue: 4, EventQueue: 1,
	HandshakeTimeout: 5 * time.Second, WriteTimeout: 30 * time.Second,
	CancelSettlementTimeout: 5 * time.Second,
}

func spawnedLadder() (wire.Ladder, error) {
	operations := []wire.Op{
		wire.Op(sourcedriverproto.OperationRefresh),
		wire.Op(sourcedriverproto.OperationInspectTargetSet),
		wire.Op(sourcedriverproto.OperationDeclareTargetSet),
		wire.Op(sourcedriverproto.OperationSnapshot),
		wire.Op(sourcedriverproto.OperationChangesSince),
		wire.Op(sourcedriverproto.OperationOpenContent),
		wire.Op(sourcedriverproto.OperationApplyMutation),
		wire.Op(sourcedriverproto.OperationInspectMutation),
		wire.Op(sourcedriverproto.OperationSettleMutation),
	}
	server := make(map[wire.Op]time.Duration, len(operations))
	client := make(map[wire.Op]time.Duration, len(operations))
	for _, operation := range operations {
		server[operation] = 5 * time.Minute
		client[operation] = 5*time.Minute + 5*time.Second
	}
	return wire.NewLadder(server, client)
}

func spawnedClientConfig(endpoint proc.SpawnedSessionEndpoint) (wire.SpawnedClientConfig, error) {
	ladder, err := spawnedLadder()
	if err != nil {
		return wire.SpawnedClientConfig{}, err
	}
	return wire.SpawnedClientConfig{
		Endpoint: endpoint, WireBuild: sourcedriverproto.Build,
		Ladder: ladder, Limits: spawnedLimits,
	}, nil
}

// RunSpawnedSession serves one exact inherited SourceDriver child session.
func RunSpawnedSession(
	ctx context.Context,
	identity proc.SpawnedSessionIdentity,
	driver sourcedriver.Driver,
) error {
	if driver == nil {
		return errors.New("source driver service: driver is required")
	}
	ladder, err := spawnedLadder()
	if err != nil {
		return err
	}
	service := &Server{driver: driver}
	return wire.RunSpawnedSession(ctx, wire.SpawnedSessionConfig{
		Identity: identity, WireBuild: sourcedriverproto.Build,
		Ladder: ladder, Limits: spawnedLimits, Handlers: service.handlerSpecs(),
	})
}
