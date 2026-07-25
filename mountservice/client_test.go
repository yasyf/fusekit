package mountservice

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/mountproto"
)

func TestObservationClientFailsImmediatelyWithoutPeer(t *testing.T) {
	var dials atomic.Int64
	client, err := NewObservationClient(t.Context(), wire.ClientConfig{
		Dial: func(context.Context) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("no peer")
		},
	})
	if err == nil || client != nil {
		t.Fatalf("NewObservationClient = %#v, %v", client, err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("observation dials = %d, want exactly one", got)
	}
}

func TestMountReplayPolicy(t *testing.T) {
	idempotent := []mountproto.Operation{
		mountproto.OperationTenantProvision,
		mountproto.OperationTenantState,
		mountproto.OperationRuntimeHealth,
	}
	for _, operation := range idempotent {
		if got := mountReplayPolicy(operation); got != wire.ReplayIdempotent {
			t.Fatalf("mountReplayPolicy(%q) = %v, want ReplayIdempotent", operation, got)
		}
	}

	nonReplayable := []mountproto.Operation{
		mountproto.OperationTenantReplace,
		mountproto.OperationTenantRemove,
		mountproto.OperationNativeWriteCommit,
	}
	for _, operation := range nonReplayable {
		if got := mountReplayPolicy(operation); got != wire.ReplayProvenNonDispatch {
			t.Fatalf("mountReplayPolicy(%q) = %v, want ReplayProvenNonDispatch", operation, got)
		}
	}
}
