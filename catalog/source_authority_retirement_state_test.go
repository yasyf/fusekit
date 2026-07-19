package catalog

import (
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceAuthorityRetirementDeletesConfiguredRuntimeStateAndAllowsReintroduction(
	t *testing.T,
) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("retired-configured-authority")
	owner := SourceAuthorityFleetOwnerID("test:" + authority)
	roots := []SourceObserverRootRecord{{
		ID: "root", Generation: 1, Path: "/source", VolumeUUID: "volume", Inode: 1, Kind: 1,
	}}
	checkpoints := []SourceObserverCheckpointRecord{{
		Stream: "stream-1", RootEpoch: "epoch-1", EventID: 7,
	}}
	configuration := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{1}, "stream-1", "epoch-1", roots, checkpoints,
	)
	configured := stageSourceObserverConfigurationForTest(
		t, c, configuration, roots, checkpoints,
	)
	if configured.FleetOwner != owner || configured.FleetGeneration != 1 {
		t.Fatalf("configured stream fence = %s/%d, want %s/1",
			configured.FleetOwner, configured.FleetGeneration, owner)
	}
	assertSourceAuthorityRuntimeStatePresentForRetirementTest(t, c, authority)

	empty := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2)
	request := SourceAuthorityRetireRequest{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		Authority: authority, StageDigest: empty.state.StageDigest,
	}
	retired, err := c.RetireSourceAuthority(t.Context(), request)
	if err != nil {
		t.Fatalf("RetireSourceAuthority: %v", err)
	}
	assertSourceAuthorityRuntimeStatePresentForRetirementTest(t, c, authority)
	var priorMember int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_authority_fleet_members
WHERE owner_id = ? AND generation = 1 AND source_authority = ?`,
		string(owner), string(authority)).Scan(&priorMember); err != nil {
		t.Fatal(err)
	}
	if priorMember != 1 {
		t.Fatalf("generation-1 member after retirement proof = %d, want 1 until acknowledgement", priorMember)
	}

	replayed, err := c.RetireSourceAuthority(t.Context(), request)
	if err != nil {
		t.Fatalf("RetireSourceAuthority(replay before acknowledgement): %v", err)
	}
	if replayed != retired {
		t.Fatalf("retirement replay = %+v, want %+v", replayed, retired)
	}
	acknowledgeSourceAuthorityFleetForTest(t, c, empty)
	assertSourceAuthorityRuntimeStateAbsentForRetirementTest(t, c, authority)
	if count := sourceAuthorityRuntimeTableCountForRetirementTest(
		t, c, "source_observer_configuration_receipts", authority,
	); count != 1 {
		t.Fatalf("unacknowledged configuration receipts = %d, want 1", count)
	}
	replayed, err = c.RetireSourceAuthority(t.Context(), request)
	if err != nil {
		t.Fatalf("RetireSourceAuthority(replay after acknowledgement): %v", err)
	}
	if replayed != retired {
		t.Fatalf("retirement replay = %+v, want %+v", replayed, retired)
	}

	reintroduced := reconcileSourceAuthorityFleetForTest(t, c, owner, 2, 3, authority)
	acknowledgeSourceAuthorityFleetForTest(t, c, reintroduced)
	reintroducedConfiguration := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{3}, "stream-3", "epoch-3", roots, checkpoints,
	)
	reintroducedConfiguration.FleetGeneration = 3
	reconfigured := stageSourceObserverConfigurationForTest(
		t, c, reintroducedConfiguration, roots, checkpoints,
	)
	if reconfigured.FleetOwner != owner || reconfigured.FleetGeneration != 3 ||
		reconfigured.Stream != "stream-3" {
		t.Fatalf("reintroduced stream = %+v, want owner %s generation 3", reconfigured, owner)
	}
}

func assertSourceAuthorityRuntimeStatePresentForRetirementTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
) {
	t.Helper()
	for _, table := range []string{
		"source_observer_streams",
		"source_observer_roots",
		"source_observer_checkpoints",
		"source_observer_configuration_receipts",
	} {
		if count := sourceAuthorityRuntimeTableCountForRetirementTest(
			t, c, table, authority,
		); count == 0 {
			t.Fatalf("%s rows = 0, want configured state", table)
		}
	}
}

func assertSourceAuthorityRuntimeStateAbsentForRetirementTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
) {
	t.Helper()
	for _, table := range []string{
		"source_snapshot_publications",
		"source_snapshot_sessions",
		"source_snapshot_logical",
		"source_snapshot_stages",
		"source_publication_stages",
		"source_observer_configuration_stages",
		"source_mutation_expectations",
		"source_key_reservations",
		"source_physical_logical",
		"source_physical_index",
		"source_observer_inbox",
		"source_observer_checkpoints",
		"source_observer_roots",
		"source_object_ids",
		"source_observer_streams",
	} {
		if count := sourceAuthorityRuntimeTableCountForRetirementTest(
			t, c, table, authority,
		); count != 0 {
			t.Fatalf("%s rows = %d, want 0 after retirement", table, count)
		}
	}
}

func sourceAuthorityRuntimeTableCountForRetirementTest(
	t *testing.T,
	c *Catalog,
	table string,
	authority causal.SourceAuthorityID,
) int {
	t.Helper()
	var count int
	if err := c.readDB.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM "+table+" WHERE source_authority = ?",
		string(authority),
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
