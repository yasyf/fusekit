package catalog

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceFleetGenerationCompactionIsBoundedAndKeepsLatestReplayFence(t *testing.T) {
	c := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("fleet-generation-compaction")

	first := reconcileSourceAuthorityFleetForTest(t, c, owner, 0, 1, "alpha")
	acknowledgeSourceAuthorityFleetForTest(t, c, first)
	second := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2, "beta")
	if _, err := c.RetireSourceAuthority(
		t.Context(),
		SourceAuthorityRetireRequest{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			Authority: "alpha", StageDigest: second.state.StageDigest,
		},
	); err != nil {
		t.Fatalf("RetireSourceAuthority(alpha): %v", err)
	}
	acknowledgeSourceAuthorityFleetForTest(t, c, second)

	_, abortedStage := stageIncompleteSourceAuthorityFleetForTest(t, c, owner, 2, 3, "beta")
	abortRequest := SourceAuthorityFleetAbortRequest{
		Owner: owner, ExpectedGeneration: 2, Generation: 3,
		StageDigest: abortedStage.StageDigest,
	}
	aborted, err := c.AbortSourceAuthorityFleet(t.Context(), abortRequest)
	if err != nil {
		t.Fatalf("AbortSourceAuthorityFleet: %v", err)
	}
	compactSourceFleetHistoryToQuiescenceForTest(t, c, 1)
	assertSourceFleetTableCountForTest(
		t, c, "source_authority_fleet_abort_receipts", owner, 1,
	)
	replayedAbort, err := c.AbortSourceAuthorityFleet(t.Context(), abortRequest)
	if err != nil || replayedAbort != aborted {
		t.Fatalf("abort replay before superseding head = %+v, %v; want %+v",
			replayedAbort, err, aborted)
	}

	fourth := reconcileSourceAuthorityFleetForTest(t, c, owner, 2, 4, "beta")
	latest := acknowledgeSourceAuthorityFleetForTest(t, c, fourth)
	compactSourceFleetHistoryToQuiescenceForTest(t, c, 1)

	assertSourceFleetTableCountForTest(
		t, c, "source_authority_retirement_receipts", owner, 0,
	)
	assertSourceFleetTableCountForTest(
		t, c, "source_authority_fleet_abort_receipts", owner, 0,
	)
	assertSourceFleetTableCountForTest(
		t, c, "source_authority_fleet_ack_receipts", owner, 1,
	)
	assertSourceFleetTableCountForTest(t, c, "source_authority_fleets", owner, 1)
	assertSourceFleetTableCountForTest(t, c, "source_authority_fleet_members", owner, 1)
	replayedLatest, err := c.AcknowledgeSourceAuthorityFleet(t.Context(), fourth.ack)
	if err != nil || replayedLatest != latest {
		t.Fatalf("latest acknowledgement replay = %+v, %v; want %+v",
			replayedLatest, err, latest)
	}
}

func TestSourceFleetAbortCompactionAcceptsAcknowledgedGenerationBelowAbortedTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	owner := SourceAuthorityFleetOwnerID("fleet-abort-future-target")
	first := reconcileSourceAuthorityFleetForTest(t, c, owner, 0, 1, "alpha")
	acknowledgeSourceAuthorityFleetForTest(t, c, first)

	_, future := stageIncompleteSourceAuthorityFleetForTest(t, c, owner, 1, 100, "beta")
	abortRequest := SourceAuthorityFleetAbortRequest{
		Owner: owner, ExpectedGeneration: 1, Generation: 100,
		StageDigest: future.StageDigest,
	}
	receipt, err := c.AbortSourceAuthorityFleet(t.Context(), abortRequest)
	if err != nil {
		t.Fatal(err)
	}
	compactSourceFleetHistoryToQuiescenceForTest(t, c, 1)
	replayed, err := c.AbortSourceAuthorityFleet(t.Context(), abortRequest)
	if err != nil || replayed != receipt {
		t.Fatalf("abort replay before successor acknowledgement = %+v, %v; want %+v",
			replayed, err, receipt)
	}

	successor := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 3, "beta")
	if _, err := c.RetireSourceAuthority(
		t.Context(),
		SourceAuthorityRetireRequest{
			Owner: owner, ExpectedGeneration: 1, Generation: 3,
			Authority: "alpha", StageDigest: successor.state.StageDigest,
		},
	); err != nil {
		t.Fatal(err)
	}
	acknowledgeSourceAuthorityFleetForTest(t, c, successor)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	compactSourceFleetHistoryToQuiescenceForTest(t, c, 1)

	assertSourceFleetTableCountForTest(
		t, c, "source_authority_fleet_abort_receipts", owner, 0,
	)
	if _, err := c.AbortSourceAuthorityFleet(
		t.Context(), abortRequest,
	); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("compacted future abort replay = %v, want ErrGenerationMismatch", err)
	}
}

func TestSourceFleetAbortCompactionAcceptsAcknowledgedRestageAtAbortedTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	owner := SourceAuthorityFleetOwnerID("fleet-abort-restaged-target")
	first := reconcileSourceAuthorityFleetForTest(t, c, owner, 0, 1, "alpha")
	acknowledgeSourceAuthorityFleetForTest(t, c, first)

	_, abortedStage := stageIncompleteSourceAuthorityFleetForTest(t, c, owner, 1, 2, "beta")
	abortRequest := SourceAuthorityFleetAbortRequest{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		StageDigest: abortedStage.StageDigest,
	}
	receipt, err := c.AbortSourceAuthorityFleet(t.Context(), abortRequest)
	if err != nil {
		t.Fatal(err)
	}
	compactSourceFleetHistoryToQuiescenceForTest(t, c, 1)
	replayed, err := c.AbortSourceAuthorityFleet(t.Context(), abortRequest)
	if err != nil || replayed != receipt {
		t.Fatalf("abort replay before restaged acknowledgement = %+v, %v; want %+v",
			replayed, err, receipt)
	}

	restaged := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2, "beta")
	if _, err := c.RetireSourceAuthority(
		t.Context(),
		SourceAuthorityRetireRequest{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			Authority: "alpha", StageDigest: restaged.state.StageDigest,
		},
	); err != nil {
		t.Fatal(err)
	}
	acknowledgeSourceAuthorityFleetForTest(t, c, restaged)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	compactSourceFleetHistoryToQuiescenceForTest(t, c, 1)

	assertSourceFleetTableCountForTest(
		t, c, "source_authority_fleet_abort_receipts", owner, 0,
	)
	if _, err := c.AbortSourceAuthorityFleet(
		t.Context(), abortRequest,
	); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("compacted restaged abort replay = %v, want ErrGenerationMismatch", err)
	}
}

func TestSourceFleetGenerationCompactionWaitsForObserverGenerationDependencies(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("fleet-generation-dependent")
	owner := SourceAuthorityFleetOwnerID("test:" + authority)
	roots := []SourceObserverRootRecord{{
		ID: "root", Generation: 1, Path: "/source", VolumeUUID: "volume", Inode: 1, Kind: 1,
	}}
	checkpoints := []SourceObserverCheckpointRecord{{
		Stream: "stream-1", RootEpoch: "epoch-1", EventID: 1,
	}}
	firstConfiguration := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{1}, "stream-1", "epoch-1", roots, checkpoints,
	)
	firstRef, _ := commitSourceObserverConfigurationForReceiptTest(
		t, c, firstConfiguration, roots, checkpoints,
	)
	if err := c.AcknowledgeSourceObserverConfiguration(t.Context(), firstRef); err != nil {
		t.Fatal(err)
	}
	second := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2, authority)
	acknowledgeSourceAuthorityFleetForTest(t, c, second)

	compactSourceFleetHistoryToQuiescenceForTest(t, c, MaintenancePageLimit)
	assertSourceFleetGenerationCountForTest(t, c, owner, 1, 1)
	assertSourceFleetMemberGenerationCountForTest(t, c, owner, 1, 1)

	secondConfiguration := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{2}, "stream-2", "epoch-2", roots, checkpoints,
	)
	secondConfiguration.FleetGeneration = 2
	secondConfiguration.FleetDigest = sha256.Sum256([]byte("fleet-generation-2"))
	secondRef, _ := commitSourceObserverConfigurationForReceiptTest(
		t, c, secondConfiguration, roots, checkpoints,
	)
	if err := c.AcknowledgeSourceObserverConfiguration(t.Context(), secondRef); err != nil {
		t.Fatal(err)
	}

	compactSourceFleetHistoryToQuiescenceForTest(t, c, MaintenancePageLimit)
	assertSourceFleetGenerationCountForTest(t, c, owner, 1, 0)
	assertSourceFleetMemberGenerationCountForTest(t, c, owner, 1, 0)
	assertSourceFleetGenerationCountForTest(t, c, owner, 2, 1)
	assertSourceFleetMemberGenerationCountForTest(t, c, owner, 2, 1)
}

func TestSourceFleetGenerationCompactionReleasesRetiredConfigurationReceipt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	authority := causal.SourceAuthorityID("fleet-retired-configuration")
	owner := SourceAuthorityFleetOwnerID("test:" + authority)
	roots := []SourceObserverRootRecord{{
		ID: "root", Generation: 1, Path: "/source", VolumeUUID: "volume", Inode: 1, Kind: 1,
	}}
	checkpoints := []SourceObserverCheckpointRecord{{
		Stream: "stream-1", RootEpoch: "epoch-1", EventID: 1,
	}}
	identity := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{3}, "stream-1", "epoch-1", roots, checkpoints,
	)
	ref, _ := commitSourceObserverConfigurationForReceiptTest(
		t, c, identity, roots, checkpoints,
	)
	if err := c.AcknowledgeSourceObserverConfiguration(t.Context(), ref); err != nil {
		t.Fatal(err)
	}

	empty := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2)
	if _, err := c.RetireSourceAuthority(
		t.Context(),
		SourceAuthorityRetireRequest{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			Authority: authority, StageDigest: empty.state.StageDigest,
		},
	); err != nil {
		t.Fatal(err)
	}
	acknowledgeSourceAuthorityFleetForTest(t, c, empty)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}

	compactSourceFleetHistoryToQuiescenceForTest(t, c, 1)
	assertSourceReceiptMissingForTest(
		t, c, "source_observer_configuration_receipts", "operation_id",
		authority, identity.Operation,
	)
	assertSourceFleetGenerationCountForTest(t, c, owner, 1, 0)
	assertSourceFleetMemberGenerationCountForTest(t, c, owner, 1, 0)
	assertSourceFleetGenerationCountForTest(t, c, owner, 2, 1)

	if _, err := c.CommitSourceObserverConfiguration(
		t.Context(), ref,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale configuration commit = %v, want ErrNotFound", err)
	}
	if err := c.BeginSourceObserverConfiguration(
		t.Context(), identity,
	); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("stale configuration begin = %v, want ErrGenerationMismatch", err)
	}
	var stages int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_observer_configuration_stages
WHERE source_authority = ?`, string(authority)).Scan(&stages); err != nil {
		t.Fatal(err)
	}
	if stages != 0 {
		t.Fatalf("stale configuration stages = %d, want 0", stages)
	}
}

func TestSourceFleetGenerationCompactionRetainsLatestConfigurationForCurrentAuthority(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("fleet-current-configuration")
	owner := SourceAuthorityFleetOwnerID("test:" + authority)
	roots := []SourceObserverRootRecord{{
		ID: "root", Generation: 1, Path: "/source", VolumeUUID: "volume", Inode: 1, Kind: 1,
	}}
	checkpoints := []SourceObserverCheckpointRecord{{
		Stream: "stream-1", RootEpoch: "epoch-1", EventID: 1,
	}}
	identity := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{4}, "stream-1", "epoch-1", roots, checkpoints,
	)
	ref, stream := commitSourceObserverConfigurationForReceiptTest(
		t, c, identity, roots, checkpoints,
	)
	if err := c.AcknowledgeSourceObserverConfiguration(t.Context(), ref); err != nil {
		t.Fatal(err)
	}

	successor := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2, authority)
	acknowledgeSourceAuthorityFleetForTest(t, c, successor)
	compactSourceFleetHistoryToQuiescenceForTest(t, c, 1)

	assertSourceReceiptAcknowledgementForTest(
		t, c, "source_observer_configuration_receipts", "operation_id",
		authority, identity.Operation, 1,
	)
	assertSourceFleetGenerationCountForTest(t, c, owner, 1, 1)
	assertSourceFleetMemberGenerationCountForTest(t, c, owner, 1, 1)
	replayed, err := c.CommitSourceObserverConfiguration(t.Context(), ref)
	if err != nil || replayed != stream {
		t.Fatalf("current configuration replay = %+v, %v; want %+v", replayed, err, stream)
	}
}

func TestSourceFleetGenerationCompactionRollbackIsRestartDeterministic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	owner := SourceAuthorityFleetOwnerID("fleet-compaction-rollback")
	first := reconcileSourceAuthorityFleetForTest(t, c, owner, 0, 1, "alpha")
	acknowledgeSourceAuthorityFleetForTest(t, c, first)
	second := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2, "beta")
	if _, err := c.RetireSourceAuthority(
		t.Context(),
		SourceAuthorityRetireRequest{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			Authority: "alpha", StageDigest: second.state.StageDigest,
		},
	); err != nil {
		t.Fatal(err)
	}
	acknowledgeSourceAuthorityFleetForTest(t, c, second)

	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := compactSettledSourceHistory(t.Context(), tx, 1)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if result.authorityRetirementReceipts != 1 {
		_ = tx.Rollback()
		t.Fatalf("rolled-back compaction = %+v, want one retirement receipt", result)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	assertSourceFleetTableCountForTest(
		t, c, "source_authority_retirement_receipts", owner, 1,
	)
	assertSourceFleetTableCountForTest(
		t, c, "source_authority_fleet_ack_receipts", owner, 2,
	)
	assertSourceFleetTableCountForTest(t, c, "source_authority_fleets", owner, 2)

	compactSourceFleetHistoryToQuiescenceForTest(t, c, 1)
	assertSourceFleetTableCountForTest(
		t, c, "source_authority_retirement_receipts", owner, 0,
	)
	assertSourceFleetTableCountForTest(
		t, c, "source_authority_fleet_ack_receipts", owner, 1,
	)
	assertSourceFleetTableCountForTest(t, c, "source_authority_fleets", owner, 1)
}

func TestSourceFleetGenerationCompactionWaitsForCurrentAuthorityClaims(t *testing.T) {
	c := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("fleet-generation-claimed")
	first := reconcileSourceAuthorityFleetForTest(t, c, owner, 0, 1, "alpha")
	acknowledgeSourceAuthorityFleetForTest(t, c, first)
	second := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2, "beta")
	if _, err := c.RetireSourceAuthority(
		t.Context(),
		SourceAuthorityRetireRequest{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			Authority: "alpha", StageDigest: second.state.StageDigest,
		},
	); err != nil {
		t.Fatal(err)
	}
	acknowledgeSourceAuthorityFleetForTest(t, c, second)

	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_authority_claims(
    source_authority, owner_id, current_generation, current_declaration_digest
)
SELECT source_authority, owner_id, generation, declaration_digest
FROM source_authority_fleet_members
WHERE owner_id = ? AND generation = ? AND source_authority = ?`,
		string(owner), 1, "alpha",
	); err != nil {
		t.Fatalf("insert historical authority claim: %v", err)
	}

	compactSourceFleetHistoryToQuiescenceForTest(t, c, MaintenancePageLimit)
	assertSourceFleetGenerationCountForTest(t, c, owner, 1, 1)
	assertSourceFleetMemberGenerationCountForTest(t, c, owner, 1, 1)

	if _, err := c.db.ExecContext(t.Context(), `
DELETE FROM source_authority_claims
WHERE owner_id = ? AND current_generation = ?`, string(owner), 1); err != nil {
		t.Fatalf("delete historical authority claim: %v", err)
	}
	compactSourceFleetHistoryToQuiescenceForTest(t, c, MaintenancePageLimit)
	assertSourceFleetGenerationCountForTest(t, c, owner, 1, 0)
	assertSourceFleetMemberGenerationCountForTest(t, c, owner, 1, 0)
	assertSourceFleetGenerationCountForTest(t, c, owner, 2, 1)
	assertSourceFleetMemberGenerationCountForTest(t, c, owner, 2, 1)
}

func TestSourceFleetGenerationCompactionReferenceProbesUseCoveringIndexes(t *testing.T) {
	c := newTestCatalog(t)
	for _, test := range []struct {
		name  string
		query string
		args  []any
		index string
	}{
		{
			name: "streams",
			query: `SELECT 1 FROM source_observer_streams
WHERE fleet_owner_id = ? AND fleet_generation = ? LIMIT 1`,
			args:  []any{"owner", 1},
			index: "source_observer_streams_fleet",
		},
		{
			name: "configuration stages",
			query: `SELECT 1 FROM source_observer_configuration_stages
WHERE fleet_owner_id = ? AND fleet_generation = ? LIMIT 1`,
			args:  []any{"owner", 1},
			index: "source_observer_configuration_stages_fleet",
		},
		{
			name: "configuration receipts",
			query: `SELECT 1 FROM source_observer_configuration_receipts
WHERE fleet_owner_id = ? AND fleet_generation = ? LIMIT 1`,
			args:  []any{"owner", 1},
			index: "source_observer_configuration_receipts_fleet",
		},
		{
			name: "retirement expected generation",
			query: `SELECT 1 FROM source_authority_retirement_receipts
WHERE owner_id = ? AND expected_generation = ? LIMIT 1`,
			args:  []any{"owner", 1},
			index: "source_authority_retirement_receipts_expected",
		},
		{
			name: "abort expected generation",
			query: `SELECT 1 FROM source_authority_fleet_abort_receipts
WHERE owner_id = ? AND expected_generation = ? LIMIT 1`,
			args:  []any{"owner", 1},
			index: "source_authority_fleet_abort_receipts_expected",
		},
		{
			name: "current claims",
			query: `SELECT 1 FROM source_authority_claims
WHERE owner_id = ? AND current_generation = ? LIMIT 1`,
			args:  []any{"owner", 1},
			index: "source_authority_claims_current",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := sourceFleetCompactionQueryPlanForTest(t, c, test.query, test.args...)
			if !strings.Contains(plan, test.index) {
				t.Fatalf("query plan %q does not use %s", plan, test.index)
			}
		})
	}
}

func TestSourceReceiptCompactionOrderAndNewerProbesUseBoundedIndexes(t *testing.T) {
	c := newTestCatalog(t)
	for _, test := range []struct {
		name           string
		table          string
		acknowledged   string
		authorityRowID string
	}{
		{
			name:           "configuration",
			table:          "source_observer_configuration_receipts",
			acknowledged:   "source_observer_configuration_receipts_ack_order",
			authorityRowID: "source_observer_configuration_receipts_authority_rowid",
		},
		{
			name:           "settlement",
			table:          "source_observer_settlement_receipts",
			acknowledged:   "source_observer_settlement_receipts_ack_order",
			authorityRowID: "source_observer_settlement_receipts_authority_rowid",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidatePlan := sourceFleetCompactionQueryPlanForTest(
				t, c, `SELECT receipt.rowid FROM `+test.table+` receipt
WHERE receipt.acknowledged = 1
ORDER BY receipt.source_authority, receipt.rowid
LIMIT ?`, 1,
			)
			assertSourceReceiptCompactionPlanForTest(
				t, candidatePlan, test.acknowledged, "receipt",
			)
			newerPlan := sourceFleetCompactionQueryPlanForTest(
				t, c, `SELECT 1 FROM `+test.table+` newer
WHERE newer.source_authority = ? AND newer.rowid > ? LIMIT 1`, "authority", 1,
			)
			assertSourceReceiptCompactionPlanForTest(
				t, newerPlan, test.authorityRowID, "newer",
			)
		})
	}
}

func compactSourceFleetHistoryToQuiescenceForTest(t *testing.T, c *Catalog, limit int) {
	t.Helper()
	for call := 0; ; call++ {
		result := compactSourceHistoryForTest(t, c, limit)
		total := result.operations + result.publicationReceipts +
			result.settlementReceipts + result.configurationReceipts +
			result.authorityRetirementReceipts + result.fleetAcknowledgementReceipts +
			result.fleetAbortReceipts + result.runtimeRecoveryReceipts +
			result.fleetMembers + result.fleetGenerations
		if total > limit {
			t.Fatalf("source fleet compaction exceeded shared budget: %+v", result)
		}
		if !result.more {
			return
		}
		if call > 100 {
			t.Fatal("source fleet compaction did not converge")
		}
	}
}

func assertSourceFleetTableCountForTest(
	t *testing.T,
	c *Catalog,
	table string,
	owner SourceAuthorityFleetOwnerID,
	want int,
) {
	t.Helper()
	var got int
	if err := c.readDB.QueryRowContext(
		t.Context(), "SELECT COUNT(*) FROM "+table+" WHERE owner_id = ?", string(owner),
	).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s rows = %d, want %d", table, got, want)
	}
}

func assertSourceFleetGenerationCountForTest(
	t *testing.T,
	c *Catalog,
	owner SourceAuthorityFleetOwnerID,
	generation causal.Generation,
	want int,
) {
	t.Helper()
	var got int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_authority_fleets
WHERE owner_id = ? AND generation = ?`, string(owner), uint64(generation)).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("fleet generation %d rows = %d, want %d", generation, got, want)
	}
}

func assertSourceFleetMemberGenerationCountForTest(
	t *testing.T,
	c *Catalog,
	owner SourceAuthorityFleetOwnerID,
	generation causal.Generation,
	want int,
) {
	t.Helper()
	var got int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_authority_fleet_members
WHERE owner_id = ? AND generation = ?`, string(owner), uint64(generation)).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("fleet member generation %d rows = %d, want %d", generation, got, want)
	}
}

func sourceFleetCompactionQueryPlanForTest(
	t *testing.T,
	c *Catalog,
	query string,
	args ...any,
) string {
	t.Helper()
	rows, err := c.readDB.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close query plan rows: %v", err)
		}
	}()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(details, "\n")
}

func assertSourceReceiptCompactionPlanForTest(
	t *testing.T,
	plan string,
	index string,
	alias string,
) {
	t.Helper()
	if !strings.Contains(plan, index) {
		t.Fatalf("query plan %q does not use %s", plan, index)
	}
	if strings.Contains(plan, "USE TEMP B-TREE") {
		t.Fatalf("query plan %q uses a temporary B-tree", plan)
	}
	for _, line := range strings.Split(plan, "\n") {
		if strings.Contains(line, "SCAN "+alias) && !strings.Contains(line, index) {
			t.Fatalf("query plan %q full-scans %s", plan, alias)
		}
	}
}
