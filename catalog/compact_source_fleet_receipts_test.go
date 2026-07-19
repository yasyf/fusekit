package catalog

import (
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceFleetReceiptCompactionWaitsForAcknowledgementAndRetainsLatest(t *testing.T) {
	c := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("fleet-receipt-compaction")

	first := reconcileSourceAuthorityFleetForTest(t, c, owner, 0, 1, "alpha", "beta")
	acknowledgeSourceAuthorityFleetForTest(t, c, first)
	second := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2)
	retirementRequests := []SourceAuthorityRetireRequest{
		{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			Authority: causal.SourceAuthorityID("alpha"), StageDigest: second.state.StageDigest,
		},
		{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			Authority: causal.SourceAuthorityID("beta"), StageDigest: second.state.StageDigest,
		},
	}
	var firstRetirement SourceAuthorityRetirementReceipt
	for index, request := range retirementRequests {
		retirement, err := c.RetireSourceAuthority(t.Context(), request)
		if err != nil {
			t.Fatalf("RetireSourceAuthority(%s): %v", request.Authority, err)
		}
		if index == 0 {
			firstRetirement = retirement
		}
	}

	beforeAcknowledgement := compactSourceHistoryForTest(t, c, 1)
	if beforeAcknowledgement.authorityRetirementReceipts != 0 {
		t.Fatalf("unacknowledged retirement compacted: %+v", beforeAcknowledgement)
	}
	assertSourceFleetReceiptCounts(t, c, owner, 2, 1)
	replayedRetirement, err := c.RetireSourceAuthority(t.Context(), retirementRequests[0])
	if err != nil {
		t.Fatalf("RetireSourceAuthority(replay before acknowledgement): %v", err)
	}
	if replayedRetirement != firstRetirement {
		t.Fatalf("retirement replay = %+v, want %+v", replayedRetirement, firstRetirement)
	}

	latest := acknowledgeSourceAuthorityFleetForTest(t, c, second)
	firstCompaction := compactSourceHistoryForTest(t, c, 1)
	if firstCompaction.authorityRetirementReceipts != 1 ||
		firstCompaction.fleetAcknowledgementReceipts != 0 {
		t.Fatalf("first fleet receipt compaction = %+v, want one retirement", firstCompaction)
	}
	assertSourceFleetReceiptCounts(t, c, owner, 1, 2)

	secondCompaction := compactSourceHistoryForTest(t, c, 1)
	if secondCompaction.authorityRetirementReceipts != 1 ||
		secondCompaction.fleetAcknowledgementReceipts != 0 {
		t.Fatalf("second fleet receipt compaction = %+v, want final retirement", secondCompaction)
	}
	assertSourceFleetReceiptCounts(t, c, owner, 0, 2)

	thirdCompaction := compactSourceHistoryForTest(t, c, 1)
	if thirdCompaction.authorityRetirementReceipts != 0 ||
		thirdCompaction.fleetAcknowledgementReceipts != 1 {
		t.Fatalf("third fleet receipt compaction = %+v, want superseded acknowledgement", thirdCompaction)
	}
	assertSourceFleetReceiptCounts(t, c, owner, 0, 1)

	replayedLatest, err := c.AcknowledgeSourceAuthorityFleet(t.Context(), second.ack)
	if err != nil {
		t.Fatalf("AcknowledgeSourceAuthorityFleet(latest replay): %v", err)
	}
	if replayedLatest != latest {
		t.Fatalf("latest acknowledgement replay = %+v, want %+v", replayedLatest, latest)
	}
}

func assertSourceFleetReceiptCounts(
	t *testing.T,
	c *Catalog,
	owner SourceAuthorityFleetOwnerID,
	retirements, acknowledgements int,
) {
	t.Helper()
	for table, want := range map[string]int{
		"source_authority_retirement_receipts": retirements,
		"source_authority_fleet_ack_receipts":  acknowledgements,
	} {
		var got int
		if err := c.readDB.QueryRowContext(
			t.Context(),
			"SELECT COUNT(*) FROM "+table+" WHERE owner_id = ?",
			string(owner),
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s rows = %d, want %d", table, got, want)
		}
	}
}
