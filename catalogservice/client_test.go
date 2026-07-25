package catalogservice

import (
	"testing"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogproto"
)

func TestCatalogReplayPolicy(t *testing.T) {
	idempotent := []catalogproto.Operation{
		catalogproto.OperationCatalogRoot,
		catalogproto.OperationCatalogHead,
		catalogproto.OperationCatalogSnapshot,
		catalogproto.OperationCatalogChangesSince,
		catalogproto.OperationCatalogLookup,
		catalogproto.OperationCatalogLookupName,
		catalogproto.OperationSourceAuthorityPublishDesiredFleet,
		catalogproto.OperationSourceAuthorityReadDesiredFleet,
		catalogproto.OperationTenantPrepare,
		catalogproto.OperationActivationAck,
	}
	for _, operation := range idempotent {
		if got := catalogReplayPolicy(operation); got != wire.ReplayIdempotent {
			t.Fatalf("catalogReplayPolicy(%q) = %v, want ReplayIdempotent", operation, got)
		}
	}

	nonReplayable := []catalogproto.Operation{
		catalogproto.OperationCatalogMutate,
		catalogproto.OperationBrokerForward,
	}
	for _, operation := range nonReplayable {
		if got := catalogReplayPolicy(operation); got != wire.ReplayProvenNonDispatch {
			t.Fatalf("catalogReplayPolicy(%q) = %v, want ReplayProvenNonDispatch", operation, got)
		}
	}
}
