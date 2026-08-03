package sourcedriverservice

import (
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/sourcedriverproto"
	"github.com/yasyf/fusekit/transportproto"
)

func TestSpawnedContractDeclaresEveryExactV1Handler(t *testing.T) {
	handlers := newServer(nil).handlerSpecs()
	server := spawnedDeadlines(spawnedServerDeadline)
	client := spawnedDeadlines(spawnedClientDeadline)
	want := map[string]struct{}{
		string(sourcedriverproto.OperationRefresh):             {},
		string(sourcedriverproto.OperationInspectTargetSet):    {},
		string(sourcedriverproto.OperationDeclareTargetSet):    {},
		string(sourcedriverproto.OperationSnapshot):            {},
		string(sourcedriverproto.OperationChangesSince):        {},
		string(sourcedriverproto.OperationOpenContent):         {},
		string(sourcedriverproto.OperationReadContent):         {},
		string(sourcedriverproto.OperationCloseContent):        {},
		string(sourcedriverproto.OperationApplyMutationBegin):  {},
		string(sourcedriverproto.OperationApplyMutationChunk):  {},
		string(sourcedriverproto.OperationApplyMutationCommit): {},
		string(sourcedriverproto.OperationInspectMutation):     {},
		string(sourcedriverproto.OperationSettleMutation):      {},
	}
	if len(handlers) != len(want) {
		t.Fatalf("handlers = %d, want %d", len(handlers), len(want))
	}
	for _, handler := range handlers {
		if _, declared := want[handler.Op]; !declared {
			t.Fatalf("unexpected handler %q", handler.Op)
		}
		delete(want, handler.Op)
		if handler.Handler == nil || !handler.Concurrent {
			t.Fatalf("handler %q = %#v, want concurrent implementation", handler.Op, handler)
		}
		if server[handler.Op] <= 0 || client[handler.Op] <= server[handler.Op] {
			t.Fatalf("handler %q deadlines = %s/%s", handler.Op, server[handler.Op], client[handler.Op])
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing handlers = %v", want)
	}
	if _, err := transportproto.NewMux(server, handlers...); err != nil {
		t.Fatalf("NewMux: %v", err)
	}
}

func TestSpawnedContractAndConveyedLimitsCannotSkew(t *testing.T) {
	contract := SpawnedContract()
	limits := SpawnedLimits()
	if contract.Schema != daemonkit.Schema(sourcedriverproto.Build) {
		t.Fatalf("contract schema = %q, want %q", contract.Schema, sourcedriverproto.Build)
	}
	if contract.MaxFrame != limits.MaxFrame || contract.Concurrency != limits.Concurrency {
		t.Fatalf("contract %+v does not match conveyed limits %+v", contract, limits)
	}
	if detail := daemonkit.MaxDetail(contract.MaxFrame); detail < spawnedPayloadBytes {
		t.Fatalf("contract carries %d payload bytes, want at least %d", detail, spawnedPayloadBytes)
	}
}
