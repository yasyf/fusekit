package sourceauthority

import (
	"context"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/transportproto"
)

const (
	// observerPayloadBytes and sourceTaskPayloadBytes are the payloads each
	// spawned session carries. Every contract frame and every durable record
	// bound in this package sizes from them, never from a wire default.
	observerPayloadBytes   = 2 << 20
	sourceTaskPayloadBytes = 2 << 20

	observerSpawnedPayload   daemonkit.Bytes = observerPayloadBytes
	sourceTaskSpawnedPayload daemonkit.Bytes = sourceTaskPayloadBytes

	observerSpawnedConcurrency   = 4
	sourceTaskSpawnedConcurrency = 8

	observerDrainWait = 20 * time.Second
)

// sourceSessionClient is the unary business lane one supervised child serves.
// *daemonkit.Business satisfies it; an in-process session substitutes for it.
type sourceSessionClient interface {
	Call(context.Context, string, []byte) (daemonkit.Reply, error)
	Close(context.Context) error
}

type internalSourceSessionProcess interface {
	openSourceSession(context.Context) (sourceSessionClient, error)
}

func observerSpawnedContract() daemonkit.Contract {
	return daemonkit.Contract{
		Schema:      fseventsObserverBuild,
		MaxFrame:    transportproto.FrameForPayload(observerSpawnedPayload),
		Concurrency: observerSpawnedConcurrency,
	}
}

func sourceTaskSpawnedContract() daemonkit.Contract {
	return daemonkit.Contract{
		Schema:      sourceTaskBuild,
		MaxFrame:    transportproto.FrameForPayload(sourceTaskSpawnedPayload),
		Concurrency: sourceTaskSpawnedConcurrency,
	}
}

// ObserverSpawnedLimits is the exact session declaration a launcher conveys on
// an observer-child spawn; the child adopts the same one, so the two ends of
// one handoff cannot skew.
func ObserverSpawnedLimits() daemonkit.Limits {
	contract := observerSpawnedContract()
	return daemonkit.Limits{MaxFrame: contract.MaxFrame, Concurrency: contract.Concurrency}
}

// SourceTaskSpawnedLimits is the source-task lane's equivalent declaration.
func SourceTaskSpawnedLimits() daemonkit.Limits {
	contract := sourceTaskSpawnedContract()
	return daemonkit.Limits{MaxFrame: contract.MaxFrame, Concurrency: contract.Concurrency}
}

func observerSpawnedDeadlines(deadlines OperationDeadlines) (map[string]time.Duration, error) {
	if err := deadlines.validate(); err != nil {
		return nil, err
	}
	return map[string]time.Duration{
		fseventsOpStage:    deadlines.ObserverControl,
		fseventsOpOpen:     deadlines.ObserverControl,
		fseventsOpActivate: deadlines.ObserverControl,
		fseventsOpFlush:    deadlines.ObserverControl,
		fseventsOpClose:    deadlines.ObserverControl,
		fseventsOpNack:     deadlines.ObserverControl,
		fseventsOpDrain:    observerDrainWait + deadlines.ObserverControl,
	}, nil
}

func sourceTaskSpawnedDeadlines(deadlines OperationDeadlines) (map[string]time.Duration, error) {
	if err := deadlines.validate(); err != nil {
		return nil, err
	}
	return map[string]time.Duration{
		sourceTaskOpRootIdentity:    deadlines.Unary,
		sourceTaskOpStat:            deadlines.Unary,
		sourceTaskOpStage:           deadlines.Unary,
		sourceTaskOpScan:            deadlines.Scan,
		sourceTaskOpScanRead:        deadlines.Scan,
		sourceTaskOpMaterialize:     deadlines.Materialize,
		sourceTaskOpMaterializeRead: deadlines.Materialize,
		sourceTaskOpMaterializeDone: deadlines.Materialize,
		sourceTaskOpMutation:        deadlines.Mutation,
		sourceTaskOpUpload:          deadlines.Mutation,
		sourceTaskOpMutationGet:     deadlines.Mutation,
		sourceTaskOpMutationAck:     deadlines.Mutation,
		sourceTaskOpMutationDrop:    deadlines.Mutation,
		sourceTaskOpMutationList:    deadlines.Mutation,
		sourceTaskOpMutationGC:      deadlines.Mutation,
	}, nil
}

// spawnedCaller issues one lane's unary calls under that lane's exact deadline.
type spawnedCaller struct {
	client    sourceSessionClient
	deadlines map[string]time.Duration
}

func (c spawnedCaller) call(ctx context.Context, op string, body []byte) ([]byte, error) {
	deadline, declared := c.deadlines[op]
	if !declared {
		return nil, fmt.Errorf("sourceauthority: operation %q has no spawned deadline", op)
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	reply, err := c.client.Call(ctx, op, body)
	if err != nil {
		return nil, err
	}
	return reply.Body, nil
}

func openObserverProcessSession(ctx context.Context, process ObserverProcess) (sourceSessionClient, error) {
	if internal, ok := process.(internalSourceSessionProcess); ok {
		return internal.openSourceSession(ctx)
	}
	return process.Business(ctx, observerSpawnedContract())
}

func openSourceTaskProcessSession(ctx context.Context, process SourceTaskProcess) (sourceSessionClient, error) {
	if internal, ok := process.(internalSourceSessionProcess); ok {
		return internal.openSourceSession(ctx)
	}
	return process.Business(ctx, sourceTaskSpawnedContract())
}

func closeSourceSession(client sourceSessionClient, timeout time.Duration) error {
	if client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.Close(ctx)
}
