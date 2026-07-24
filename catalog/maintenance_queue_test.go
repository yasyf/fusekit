package catalog

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/convergence"
)

func TestMaintenanceQueueIsFairAndLatestWins(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	first, firstRoot := createTestTenant(t, c, "maintenance-first", CaseSensitive)
	second, _ := createTestTenant(t, c, "maintenance-second", CaseSensitive)

	task, found, err := c.ClaimMaintenance(ctx)
	if err != nil || !found || task.Tenant != first || task.DirtyRevision != 1 {
		t.Fatalf("ClaimMaintenance(first) = %+v, %t, %v", task, found, err)
	}
	createTestFile(t, c, first, firstRoot.ID, "new", "content")
	if err := c.FinishMaintenance(ctx, task, false); err != nil {
		t.Fatalf("FinishMaintenance(latest wins): %v", err)
	}

	task, found, err = c.ClaimMaintenance(ctx)
	if err != nil || !found || task.Tenant != second {
		t.Fatalf("ClaimMaintenance(second) = %+v, %t, %v", task, found, err)
	}
	if err := c.FinishMaintenance(ctx, task, true); err != nil {
		t.Fatalf("FinishMaintenance(requeue): %v", err)
	}

	task, found, err = c.ClaimMaintenance(ctx)
	if err != nil || !found || task.Tenant != first || task.DirtyRevision != 2 {
		t.Fatalf("ClaimMaintenance(updated first) = %+v, %t, %v", task, found, err)
	}
	if err := c.FinishMaintenance(ctx, task, false); err != nil {
		t.Fatalf("FinishMaintenance(updated first): %v", err)
	}

	task, found, err = c.ClaimMaintenance(ctx)
	if err != nil || !found || task.Tenant != second {
		t.Fatalf("ClaimMaintenance(rotated second) = %+v, %t, %v", task, found, err)
	}
}

func TestMaintenanceQueueRecoversInterruptedClaim(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	tenant, err := NewTenantID("maintenance-recovery")
	if err != nil {
		t.Fatalf("NewTenantID: %v", err)
	}
	if _, err := c.CreateTenant(ctx, tenant, CaseSensitive, PresentMount); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	claimed, found, err := c.ClaimMaintenance(ctx)
	if err != nil || !found {
		t.Fatalf("ClaimMaintenance = %+v, %t, %v", claimed, found, err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, found, err := reopened.ClaimMaintenance(ctx); err != nil || found {
		t.Fatalf("ClaimMaintenance before recovery found=%t err=%v", found, err)
	}
	if err := reopened.RecoverMaintenanceClaims(ctx); err != nil {
		t.Fatalf("RecoverMaintenanceClaims: %v", err)
	}
	recovered, found, err := reopened.ClaimMaintenance(ctx)
	if err != nil || !found || recovered != claimed {
		t.Fatalf("ClaimMaintenance after recovery = %+v, %t, %v; want %+v", recovered, found, err, claimed)
	}
}

func TestMaintenanceQueueRecoversInterruptedClaimsInFixedPages(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	const total = maintenanceClaimPageLimit + 17
	for index := 0; index < total; index++ {
		var root ObjectID
		binary.BigEndian.PutUint64(root[8:], uint64(index+1))
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tenants(tenant, root_id, case_policy, presentation_set, head, floor)
VALUES (?, ?, ?, ?, 1, 0)`,
			fmt.Sprintf("maintenance-recovery-%04d", index), root[:],
			uint8(CaseSensitive), uint8(PresentMount)); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert tenant %d: %v", index, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE catalog_maintenance SET running_revision = dirty_revision`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("mark interrupted claims: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit interrupted claims: %v", err)
	}
	if err := c.RecoverMaintenanceClaims(ctx); err != nil {
		t.Fatalf("RecoverMaintenanceClaims: %v", err)
	}
	var recovered int
	if err := c.readDB.QueryRowContext(ctx, `
SELECT COUNT(*) FROM catalog_maintenance WHERE running_revision = 0`).Scan(&recovered); err != nil {
		t.Fatalf("count recovered claims: %v", err)
	}
	if recovered != total {
		t.Fatalf("recovered claims = %d, want %d", recovered, total)
	}
}

func TestMaintenanceQueueEnqueuesTenantWhenActivationIsAcknowledged(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	definition := lifecycleTestProvision(t, "maintenance-ack", 1)
	state, lease, publication := stageLifecycleForTest(t, c, definition)
	for _, backend := range state.Target.RequiredBackends.Backends() {
		state = recordBackendForTest(t, c, state, lease, backend)
	}
	domain := causal.DomainID("maintenance-ack-domain")
	seedActivationTargetForTest(t, c, definition, lease, domain)
	activated, err := c.ActivateTenant(ctx, ActivateTenantRequest{
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
	for {
		task, found, err := c.ClaimMaintenance(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
		if err := c.FinishMaintenance(ctx, task, false); err != nil {
			t.Fatal(err)
		}
	}
	var holder, token causal.OperationID
	holder[0], token[0] = 1, 2
	claimedAt := time.Unix(1, 0).UTC()
	claim, err := c.ClaimDelivery(ctx, convergence.ClaimRequest{
		RuntimeGeneration: "maintenance-runtime", HolderOperation: holder,
		ClaimToken: token, ClaimedAt: claimedAt,
	})
	if err != nil || claim == nil {
		t.Fatalf("ClaimDelivery = %+v, %v", claim, err)
	}
	if err := c.RecordDelivery(ctx, convergence.DeliveryResult{
		Key: claim.Event.Key(), ClaimToken: token, Outcome: convergence.DeliverySent,
		AckDeadline: claimedAt.Add(convergence.AckTimeout),
	}); err != nil {
		t.Fatalf("RecordDelivery: %v", err)
	}
	if err := c.AcknowledgeDelivery(ctx, causal.ActivationAck{
		ActivationChangeID: claim.Event.ActivationChangeID,
		TenantID:           claim.Event.TenantID, TenantGeneration: claim.Event.TenantGeneration,
		PresentationID: claim.Event.PresentationID, Backend: claim.Event.Backend,
		ObservedActivationRevision: claim.Event.ActivationRevision,
		ObservedCatalogHead:        claim.Event.CatalogHead, ObservedHeadDigest: activated.State.Target.SpecHash,
	}); err == nil {
		t.Fatal("AcknowledgeDelivery accepted the tenant spec digest as a catalog-head digest")
	}
	if err := c.AcknowledgeDelivery(ctx, causal.ActivationAck{
		ActivationChangeID: claim.Event.ActivationChangeID,
		TenantID:           claim.Event.TenantID, TenantGeneration: claim.Event.TenantGeneration,
		PresentationID: claim.Event.PresentationID, Backend: claim.Event.Backend,
		ObservedActivationRevision: claim.Event.ActivationRevision,
		ObservedCatalogHead:        claim.Event.CatalogHead, ObservedHeadDigest: lease.HeadDigest,
	}); err != nil {
		t.Fatalf("AcknowledgeDelivery: %v", err)
	}
	requeued, found, err := c.ClaimMaintenance(ctx)
	if err != nil || !found || requeued.Tenant != definition.Tenant {
		t.Fatalf("ClaimMaintenance(after acknowledgement) = %+v, %t, %v", requeued, found, err)
	}
}
