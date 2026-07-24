package sourceauthority

import (
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
)

func TestSpawnedContractsDeclareEveryExactHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handlers   []wire.HandlerSpec
		operations []wire.Op
		ladder     func() (wire.Ladder, error)
		server     time.Duration
		limits     wire.SessionLimits
	}{
		{
			name: "observer", handlers: observerHandlerSpecs(&fseventsObserverChild{}),
			operations: []wire.Op{fseventsOpOpen, fseventsOpActivate, fseventsOpFlush, fseventsOpAck, fseventsOpClose},
			ladder:     observerSpawnedLadder, server: 30 * time.Second, limits: observerSpawnedLimits,
		},
		{
			name: "source task", handlers: sourceTaskHandlerSpecs(&sourceTaskChild{}),
			operations: []wire.Op{
				sourceTaskOpRootIdentity, sourceTaskOpStat, sourceTaskOpScan, sourceTaskOpMaterialize,
				sourceTaskOpMutation, sourceTaskOpMutationGet, sourceTaskOpMutationAck,
				sourceTaskOpMutationDrop, sourceTaskOpMutationList, sourceTaskOpMutationGC,
			},
			ladder: sourceTaskSpawnedLadder, server: 5 * time.Minute, limits: sourceTaskSpawnedLimits,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := make([]wire.Op, len(test.handlers))
			for index, handler := range test.handlers {
				if handler.Handler == nil || !handler.Concurrent {
					t.Fatalf("handler %q is not a concurrent static handler", handler.Op)
				}
				got[index] = handler.Op
			}
			if !reflect.DeepEqual(got, test.operations) {
				t.Fatalf("operations = %q, want %q", got, test.operations)
			}
			ladder, err := test.ladder()
			if err != nil {
				t.Fatal(err)
			}
			for _, operation := range test.operations {
				server, client, ok := ladder.Deadlines(operation)
				if !ok || server != test.server || client != test.server+5*time.Second {
					t.Fatalf("ladder[%q] = (%s, %s, %t)", operation, server, client, ok)
				}
			}
			if test.limits.Workers <= 0 || test.limits.Backlog <= 0 || test.limits.MaxFrame <= 0 ||
				test.limits.InboundQueue <= 0 || test.limits.OutboundQueue <= 0 || test.limits.StreamQueue <= 0 ||
				test.limits.EventQueue <= 0 || test.limits.HandshakeTimeout <= 0 || test.limits.WriteTimeout <= 0 ||
				test.limits.CancelSettlementTimeout <= 0 {
				t.Fatal("spawned session limits must all be positive")
			}
		})
	}
}
