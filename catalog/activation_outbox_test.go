package catalog

import (
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/convergence"
)

func TestActivationDeliveryNotSentRetriesSameIdentity(t *testing.T) {
	c, activation := targetedActivationForTest(t, "not-sent")
	now := time.Unix(100, 0).UTC()
	first := claimActivationForTest(t, c, now, 1)
	if first.Event.ActivationChangeID != activation.ChangeID || first.Attempt != 1 {
		t.Fatalf("first claim = %+v", first)
	}
	if err := c.RecordDelivery(t.Context(), convergence.DeliveryResult{
		Key: first.Event.Key(), ClaimToken: first.ClaimToken, Outcome: convergence.DeliveryNotSent,
		Failure: convergence.DeliveryFailure{Code: "not_sent", Detail: "broker unavailable"},
	}); err != nil {
		t.Fatalf("RecordDelivery(NotSent): %v", err)
	}
	second := claimActivationForTest(t, c, now.Add(time.Second), 2)
	if second.Event.Key() != first.Event.Key() || second.Attempt != 2 {
		t.Fatalf("second claim = %+v, want same activation attempt 2", second)
	}
}

func TestActivationDeliverySentRequiresExactAcknowledgement(t *testing.T) {
	c, _ := targetedActivationForTest(t, "sent-ack")
	now := time.Unix(200, 0).UTC()
	claim := claimActivationForTest(t, c, now, 1)
	deadline := now.Add(convergence.AckTimeout)
	if err := c.RecordDelivery(t.Context(), convergence.DeliveryResult{
		Key: claim.Event.Key(), ClaimToken: claim.ClaimToken,
		Outcome: convergence.DeliverySent, AckDeadline: deadline,
	}); err != nil {
		t.Fatalf("RecordDelivery(Sent): %v", err)
	}
	bad := activationAckForEvent(claim.Event)
	bad.ObservedHeadDigest[0] ^= 0xff
	if err := c.AcknowledgeDelivery(t.Context(), bad); err == nil {
		t.Fatal("AcknowledgeDelivery accepted mismatched head digest")
	}
	if err := c.AcknowledgeDelivery(t.Context(), activationAckForEvent(claim.Event)); err != nil {
		t.Fatalf("AcknowledgeDelivery: %v", err)
	}
	next, err := c.ClaimDelivery(t.Context(), convergence.ClaimRequest{
		RuntimeGeneration: "runtime-1", HolderOperation: causal.OperationID{8},
		ClaimToken: causal.OperationID{9}, ClaimedAt: now.Add(time.Second),
	})
	if err != nil || next != nil {
		t.Fatalf("claim after ack = %+v, %v", next, err)
	}
}

func TestActivationDeliveryDeadOwnerBecomesUnknownAndIsNeverReplayed(t *testing.T) {
	c, _ := targetedActivationForTest(t, "dead-owner")
	now := time.Unix(300, 0).UTC()
	claim := claimActivationForTest(t, c, now, 1)
	if err := c.RecoverDeliveries(t.Context(), "runtime-2", now.Add(time.Second)); err != nil {
		t.Fatalf("RecoverDeliveries: %v", err)
	}
	next, err := c.ClaimDelivery(t.Context(), convergence.ClaimRequest{
		RuntimeGeneration: "runtime-2", HolderOperation: causal.OperationID{10},
		ClaimToken: causal.OperationID{11}, ClaimedAt: now.Add(2 * time.Second),
	})
	if err != nil || next != nil {
		t.Fatalf("replayed ambiguous delivery = %+v, %v", next, err)
	}
	if err := c.AcknowledgeDelivery(t.Context(), activationAckForEvent(claim.Event)); err != nil {
		t.Fatalf("acknowledge recovered delivery: %v", err)
	}
}

func TestActivationDeliveryWindowIsGloballyBounded(t *testing.T) {
	c := newTestCatalog(t)
	for index := 0; index < convergence.MaxAwaiting+1; index++ {
		targetedActivationInCatalogForTest(t, c, "window-"+string(rune('a'+index)))
	}
	now := time.Unix(400, 0).UTC()
	for index := 0; index < convergence.MaxAwaiting; index++ {
		claim := claimActivationForTest(t, c, now.Add(time.Duration(index)*time.Second), uint64(index+1))
		if err := c.RecordDelivery(t.Context(), convergence.DeliveryResult{
			Key: claim.Event.Key(), ClaimToken: claim.ClaimToken,
			Outcome: convergence.DeliverySent, AckDeadline: now.Add(convergence.AckTimeout),
		}); err != nil {
			t.Fatalf("RecordDelivery(%d): %v", index, err)
		}
	}
	blocked, err := c.ClaimDelivery(t.Context(), convergence.ClaimRequest{
		RuntimeGeneration: "runtime-1", HolderOperation: causal.OperationID{12},
		ClaimToken: causal.OperationID{13}, ClaimedAt: now.Add(3 * time.Second),
	})
	if err != nil || blocked != nil {
		t.Fatalf("claim beyond global window = %+v, %v", blocked, err)
	}
}

func TestActivationDeliveryTimeoutQuarantinesWithoutReplay(t *testing.T) {
	c, _ := targetedActivationForTest(t, "timeout")
	now := time.Unix(500, 0).UTC()
	claim := claimActivationForTest(t, c, now, 1)
	deadline := now.Add(convergence.AckTimeout)
	if err := c.RecordDelivery(t.Context(), convergence.DeliveryResult{
		Key: claim.Event.Key(), ClaimToken: claim.ClaimToken,
		Outcome: convergence.DeliveryUnknown, AckDeadline: deadline,
		Failure: convergence.DeliveryFailure{Code: "connection_lost", Detail: "delivery ambiguous"},
	}); err != nil {
		t.Fatalf("RecordDelivery(Unknown): %v", err)
	}
	if err := c.QuarantineExpired(t.Context(), deadline); err != nil {
		t.Fatalf("QuarantineExpired: %v", err)
	}
	next, err := c.ClaimDelivery(t.Context(), convergence.ClaimRequest{
		RuntimeGeneration: "runtime-1", HolderOperation: causal.OperationID{14},
		ClaimToken: causal.OperationID{15}, ClaimedAt: deadline.Add(time.Second),
	})
	if err != nil || next != nil {
		t.Fatalf("replayed quarantined delivery = %+v, %v", next, err)
	}
}

func targetedActivationForTest(t *testing.T, name string) (*Catalog, TenantActivationResult) {
	t.Helper()
	c := newTestCatalog(t)
	return c, targetedActivationInCatalogForTest(t, c, name)
}

func targetedActivationInCatalogForTest(t *testing.T, c *Catalog, name string) TenantActivationResult {
	t.Helper()
	definition := lifecycleTestProvision(t, name, 1)
	state, lease, publication := stageLifecycleForTest(t, c, definition)
	for _, backend := range state.Target.RequiredBackends.Backends() {
		state = recordBackendForTest(t, c, state, lease, backend)
	}
	seedActivationTargetForTest(t, c, definition, lease, causal.DomainID(name+"-domain"))
	activation, err := c.ActivateTenant(t.Context(), ActivateTenantRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
		ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
		ExpectedActivationRevision: state.Activation.Revision,
		ExpectedTargetingRevision:  mustTargetingRevision(t, c, definition.Tenant),
		CausePublications:          []causal.OperationID{publication},
	})
	if err != nil {
		t.Fatalf("ActivateTenant: %v", err)
	}
	return activation
}

func claimActivationForTest(
	t *testing.T,
	c *Catalog,
	now time.Time,
	attempt uint64,
) *convergence.DeliveryClaim {
	t.Helper()
	claim, err := c.ClaimDelivery(t.Context(), convergence.ClaimRequest{
		RuntimeGeneration: "runtime-1", HolderOperation: causal.OperationID{1},
		ClaimToken: causal.OperationID{byte(attempt + 1)}, ClaimedAt: now,
	})
	if err != nil {
		t.Fatalf("ClaimDelivery: %v", err)
	}
	if claim == nil {
		t.Fatal("ClaimDelivery returned nil")
	}
	return claim
}

func activationAckForEvent(event causal.ActivationEvent) causal.ActivationAck {
	return causal.ActivationAck{
		ActivationChangeID: event.ActivationChangeID,
		TenantID:           event.TenantID, TenantGeneration: event.TenantGeneration,
		PresentationID: event.PresentationID, Backend: event.Backend,
		ObservedActivationRevision: event.ActivationRevision,
		ObservedCatalogHead:        event.CatalogHead, ObservedHeadDigest: event.HeadDigest,
	}
}
