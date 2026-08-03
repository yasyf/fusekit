package sourceauthority

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

func TestSpawnedContractsDeclareEveryExactOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handlers   map[string]func(context.Context, daemonkit.Request) (any, error)
		deadlines  func(OperationDeadlines) (map[string]time.Duration, error)
		operations []string
	}{
		{
			name: "observer", handlers: observerHandlers(&fseventsObserverChild{}),
			deadlines: observerSpawnedDeadlines,
			operations: []string{
				fseventsOpStage, fseventsOpOpen, fseventsOpActivate, fseventsOpFlush,
				fseventsOpDrain, fseventsOpNack, fseventsOpClose,
			},
		},
		{
			name: "source task", handlers: sourceTaskHandlers(&sourceTaskChild{}),
			deadlines: sourceTaskSpawnedDeadlines,
			operations: []string{
				sourceTaskOpStage, sourceTaskOpUpload, sourceTaskOpRootIdentity, sourceTaskOpStat,
				sourceTaskOpScan, sourceTaskOpScanRead, sourceTaskOpMaterialize,
				sourceTaskOpMaterializeRead, sourceTaskOpMaterializeDone, sourceTaskOpMutation,
				sourceTaskOpMutationGet, sourceTaskOpMutationAck, sourceTaskOpMutationDrop,
				sourceTaskOpMutationList, sourceTaskOpMutationGC,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := make([]string, 0, len(test.handlers))
			for operation, handler := range test.handlers {
				if handler == nil {
					t.Fatalf("operation %q has no handler", operation)
				}
				got = append(got, operation)
			}
			want := slices.Clone(test.operations)
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("operations = %q, want %q", got, want)
			}
			deadlines, err := test.deadlines(StandardOperationDeadlines())
			if err != nil {
				t.Fatal(err)
			}
			if len(deadlines) != len(test.operations) {
				t.Fatalf("deadlines cover %d operations, want %d", len(deadlines), len(test.operations))
			}
			for _, operation := range test.operations {
				if deadline, declared := deadlines[operation]; !declared || deadline <= 0 {
					t.Fatalf("deadlines[%q] = (%s, %t)", operation, deadline, declared)
				}
			}
			if _, err := test.deadlines(OperationDeadlines{}); err == nil {
				t.Fatal("invalid operation deadlines must refuse a deadline table")
			}
		})
	}
}

func TestSpawnedContractFramesCarryTheirDeclaredPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		contract daemonkit.Contract
		payload  daemonkit.Bytes
	}{
		{name: "observer", contract: observerSpawnedContract(), payload: observerSpawnedPayload},
		{name: "source task", contract: sourceTaskSpawnedContract(), payload: sourceTaskSpawnedPayload},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if test.contract.Schema == "" || test.contract.Concurrency <= 0 {
				t.Fatalf("contract = %+v", test.contract)
			}
			if detail := daemonkit.MaxDetail(test.contract.MaxFrame); detail < test.payload {
				t.Fatalf("MaxDetail(%d) = %d, want at least %d", test.contract.MaxFrame, detail, test.payload)
			}
		})
	}
}

func TestSourceTaskMessageBudgetsFitTheSessionPayload(t *testing.T) {
	t.Parallel()

	for name, budget := range map[string]int{
		"response":     sourceTaskResponseByteLimit,
		"request":      sourceTaskJSONByteLimit,
		"scan read":    maxScanReadBytes,
		"config page":  sourceTaskPageByteLimit,
		"upload chunk": sourceTaskChunkSize,
	} {
		if daemonkit.Bytes(budget) > sourceTaskSpawnedPayload {
			t.Fatalf("%s budget %d exceeds the %d the session carries", name, budget, sourceTaskSpawnedPayload)
		}
	}
	if daemonkit.Bytes(maxObserverPayloadBytes) > observerSpawnedPayload {
		t.Fatalf(
			"observer payload budget %d exceeds the %d the session carries",
			maxObserverPayloadBytes, observerSpawnedPayload,
		)
	}
}

func TestSpawnedLimitsMatchTheirContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		limits   daemonkit.Limits
		contract daemonkit.Contract
	}{
		{name: "observer", limits: ObserverSpawnedLimits(), contract: observerSpawnedContract()},
		{name: "source task", limits: SourceTaskSpawnedLimits(), contract: sourceTaskSpawnedContract()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			want := daemonkit.Limits{
				MaxFrame: test.contract.MaxFrame, Concurrency: test.contract.Concurrency,
			}
			if test.limits != want {
				t.Fatalf("limits = %+v, want %+v", test.limits, want)
			}
		})
	}
}

func TestSpawnedCallerRefusesAnUndeclaredOperation(t *testing.T) {
	t.Parallel()

	deadlines, err := sourceTaskSpawnedDeadlines(StandardOperationDeadlines())
	if err != nil {
		t.Fatal(err)
	}
	caller := spawnedCaller{client: refusingSessionClient{}, deadlines: deadlines}
	if _, err := caller.call(t.Context(), "source.undeclared", nil); err == nil {
		t.Fatal("an operation with no deadline must refuse before it is dispatched")
	}
	if _, err := caller.call(t.Context(), sourceTaskOpStat, nil); !errors.Is(err, errRefusingSession) {
		t.Fatalf("call = %v, want %v", err, errRefusingSession)
	}
}

var errRefusingSession = errors.New("refusing session")

type refusingSessionClient struct{}

func (refusingSessionClient) Call(ctx context.Context, _ string, _ []byte) (daemonkit.Reply, error) {
	if _, stated := ctx.Deadline(); !stated {
		return daemonkit.Reply{}, errors.New("spawned caller must impose its operation deadline")
	}
	return daemonkit.Reply{}, errRefusingSession
}

func (refusingSessionClient) Close(context.Context) error { return nil }
