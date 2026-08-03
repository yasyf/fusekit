package sourcedriverservice

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/sourcedriverproto"
	"github.com/yasyf/fusekit/transportproto"
)

const (
	// spawnedPayloadBytes is the payload one SourceDriver session carries; the
	// contract frame sizes from it, never from a daemonkit default.
	spawnedPayloadBytes daemonkit.Bytes = 2 << 20
	spawnedConcurrency                  = 4

	spawnedServerDeadline = 5 * time.Minute
	spawnedClientDeadline = spawnedServerDeadline + 5*time.Second
)

func spawnedOperations() []sourcedriverproto.Operation {
	return []sourcedriverproto.Operation{
		sourcedriverproto.OperationRefresh,
		sourcedriverproto.OperationInspectTargetSet,
		sourcedriverproto.OperationDeclareTargetSet,
		sourcedriverproto.OperationSnapshot,
		sourcedriverproto.OperationChangesSince,
		sourcedriverproto.OperationOpenContent,
		sourcedriverproto.OperationReadContent,
		sourcedriverproto.OperationCloseContent,
		sourcedriverproto.OperationApplyMutationBegin,
		sourcedriverproto.OperationApplyMutationChunk,
		sourcedriverproto.OperationApplyMutationCommit,
		sourcedriverproto.OperationInspectMutation,
		sourcedriverproto.OperationSettleMutation,
	}
}

func spawnedDeadlines(deadline time.Duration) map[string]time.Duration {
	operations := spawnedOperations()
	deadlines := make(map[string]time.Duration, len(operations))
	for _, operation := range operations {
		deadlines[string(operation)] = deadline
	}
	return deadlines
}

// SpawnedContract is the exact session one supervised SourceDriver child serves.
func SpawnedContract() daemonkit.Contract {
	return daemonkit.Contract{
		Schema:      daemonkit.Schema(sourcedriverproto.Build),
		MaxFrame:    transportproto.FrameForPayload(spawnedPayloadBytes),
		Concurrency: spawnedConcurrency,
	}
}

// SpawnedLimits is the exact declaration a launcher conveys on a SourceDriver
// spawn; the child adopts the same one, so the two ends cannot skew.
func SpawnedLimits() daemonkit.Limits {
	contract := SpawnedContract()
	return daemonkit.Limits{MaxFrame: contract.MaxFrame, Concurrency: contract.Concurrency}
}

// RunSpawnedSession serves one exact inherited SourceDriver child session.
func RunSpawnedSession(ctx context.Context, driver sourcedriver.Driver) (err error) {
	if driver == nil {
		return errors.New("source driver service: driver is required")
	}
	service := newServer(driver)
	defer func() {
		err = errors.Join(err, service.release())
	}()
	mux, err := transportproto.NewMux(spawnedDeadlines(spawnedServerDeadline), service.handlerSpecs()...)
	if err != nil {
		return fmt.Errorf("source driver service: build operation mux: %w", err)
	}
	return daemonkit.ServeSpawned(ctx, SpawnedContract(), mux.Handle)
}
