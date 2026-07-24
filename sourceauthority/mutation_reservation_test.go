package sourceauthority

import (
	"database/sql"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func reserveSourceMutationExpectationForRuntimeTest(
	t *testing.T,
	store *catalog.Catalog,
	record catalog.SourceMutationExpectationRecord,
) error {
	t.Helper()
	stream, err := store.SourceObserverStream(t.Context(), record.Authority)
	if err != nil {
		return err
	}
	var checkpoints []catalog.SourceObserverCheckpointRecord
	for after := ""; ; {
		page, err := store.SourceObserverCheckpointsPage(
			t.Context(), record.Authority, after, catalog.SourceObserverConfigurationPageLimit,
		)
		if err != nil {
			return err
		}
		checkpoints = append(checkpoints, page.Records...)
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	var applied []catalog.SourceObserverAppliedCheckpointRecord
	for after := ""; ; {
		page, err := store.SourceObserverAppliedCheckpointsPage(
			t.Context(), record.Authority, after, catalog.SourceObserverConfigurationPageLimit,
		)
		if err != nil {
			return err
		}
		applied = append(applied, page.Records...)
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	checkpointsDigest, err := catalog.SourceObserverCheckpointsDigest(checkpoints)
	if err != nil {
		return err
	}
	appliedDigest, err := catalog.SourceObserverAppliedCheckpointsDigest(applied)
	if err != nil {
		return err
	}
	return store.ReserveSourceMutationExpectation(t.Context(), catalog.SourceMutationExpectationReservation{
		Record: record, Stream: stream.Stream, RootEpoch: stream.RootEpoch,
		LastReceived: stream.LastReceived, LastApplied: stream.LastApplied,
		CheckpointsDigest: checkpointsDigest, AppliedCheckpointsDigest: appliedDigest,
	})
}

func markSourceObserverIncrementalForRuntimeTest(
	t *testing.T,
	databasePath string,
	authority causal.SourceAuthorityID,
) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if _, err := database.ExecContext(t.Context(), `
UPDATE source_observer_streams SET state = ? WHERE source_authority = ?`,
		uint8(catalog.SourceObserverIncremental), string(authority)); err != nil {
		t.Fatal(err)
	}
}
