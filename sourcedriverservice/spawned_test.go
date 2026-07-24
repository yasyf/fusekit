package sourcedriverservice

import (
	"testing"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/sourcedriverproto"
)

func TestSpawnedContractDeclaresEveryExactV1Handler(t *testing.T) {
	service := &Server{}
	handlers := service.handlerSpecs()
	ladder, err := spawnedLadder()
	if err != nil {
		t.Fatal(err)
	}
	want := map[wire.Op]struct{}{
		wire.Op(sourcedriverproto.OperationRefresh):          {},
		wire.Op(sourcedriverproto.OperationInspectTargetSet): {},
		wire.Op(sourcedriverproto.OperationDeclareTargetSet): {},
		wire.Op(sourcedriverproto.OperationSnapshot):         {},
		wire.Op(sourcedriverproto.OperationChangesSince):     {},
		wire.Op(sourcedriverproto.OperationOpenContent):      {},
		wire.Op(sourcedriverproto.OperationApplyMutation):    {},
		wire.Op(sourcedriverproto.OperationInspectMutation):  {},
		wire.Op(sourcedriverproto.OperationSettleMutation):   {},
	}
	if len(handlers) != len(want) {
		t.Fatalf("handlers = %d, want %d", len(handlers), len(want))
	}
	for _, handler := range handlers {
		if _, ok := want[handler.Op]; !ok {
			t.Fatalf("unexpected handler %q", handler.Op)
		}
		delete(want, handler.Op)
		if handler.Handler == nil || !handler.Concurrent {
			t.Fatalf("handler %q = %#v, want concurrent implementation", handler.Op, handler)
		}
		server, client, ok := ladder.Deadlines(handler.Op)
		if !ok || server <= 0 || client <= server {
			t.Fatalf("handler %q ladder = %s/%s/%t", handler.Op, server, client, ok)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing handlers = %v", want)
	}
}
