package catalog

import (
	"errors"
	"fmt"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceObserverConfigurationStagesExactPagesAndCommitsSetwise(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("configuration-stage")
	roots := make([]SourceObserverRootRecord, 300)
	checkpoints := make([]SourceObserverCheckpointRecord, 300)
	for index := range roots {
		roots[index] = SourceObserverRootRecord{
			ID: fmt.Sprintf("root-%04d", index), Generation: 1,
			Path: fmt.Sprintf("/roots/%04d", index), VolumeUUID: "volume",
			Inode: uint64(index + 1), Kind: 1,
		}
		checkpoints[index] = SourceObserverCheckpointRecord{
			Stream: fmt.Sprintf("stream-%04d", index), RootEpoch: "epoch",
			EventID: uint64(index), AppliedEventID: uint64(index),
		}
	}
	identity := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{1}, "stream", "epoch", roots, checkpoints,
	)
	ensureSourceObserverFleetForTest(t, c, identity)
	if err := c.BeginSourceObserverConfiguration(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceObserverConfiguration(t.Context(), identity); err != nil {
		t.Fatalf("exact begin replay: %v", err)
	}
	firstRoots := SourceObserverRootAppendPage{
		Records: roots[:SourceObserverConfigurationPageLimit],
	}
	firstRef, err := c.AppendSourceObserverConfigurationRoots(
		t.Context(), authority, identity.Operation, firstRoots,
	)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := c.AppendSourceObserverConfigurationRoots(
		t.Context(), authority, identity.Operation, firstRoots,
	)
	if err != nil || replayed != firstRef {
		t.Fatalf("exact root-page replay = %+v, %v; want %+v", replayed, err, firstRef)
	}
	conflict := firstRoots
	conflict.Records = append([]SourceObserverRootRecord(nil), conflict.Records...)
	conflict.Records[0].Path = "/different"
	if _, err := c.AppendSourceObserverConfigurationRoots(
		t.Context(), authority, identity.Operation, conflict,
	); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("changed root-page replay = %v, want conflict", err)
	}
	if _, err := c.AppendSourceObserverConfigurationRoots(
		t.Context(), authority, identity.Operation,
		SourceObserverRootAppendPage{
			Sequence: firstRef.Sequence,
			Records:  []SourceObserverRootRecord{roots[SourceObserverConfigurationPageLimit-1]},
		},
	); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("non-monotonic root continuation = %v, want invalid transition", err)
	}
	ref, err := c.AppendSourceObserverConfigurationRoots(
		t.Context(), authority, identity.Operation,
		SourceObserverRootAppendPage{
			Sequence: firstRef.Sequence,
			Records:  roots[SourceObserverConfigurationPageLimit:],
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for start := 0; start < len(checkpoints); start += SourceObserverConfigurationPageLimit {
		end := min(start+SourceObserverConfigurationPageLimit, len(checkpoints))
		ref, err = c.AppendSourceObserverConfigurationCheckpoints(
			t.Context(), authority, identity.Operation,
			SourceObserverCheckpointAppendPage{
				Sequence: ref.Sequence,
				Records:  checkpoints[start:end],
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	stream, err := c.CommitSourceObserverConfiguration(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	replayedStream, err := c.CommitSourceObserverConfiguration(t.Context(), ref)
	if err != nil || replayedStream != stream {
		t.Fatalf("lost-response commit replay = %+v, %v; want %+v", replayedStream, err, stream)
	}
	if stream.Stream != identity.Stream || stream.RootEpoch != identity.RootEpoch ||
		stream.RootDigest != identity.RootDigest || stream.FleetDigest != identity.FleetDigest {
		t.Fatalf("committed stream = %+v", stream)
	}
	assertSourceObserverConfigurationPages(t, c, authority, roots, checkpoints)
}

func TestSourceObserverConfigurationReplacementRemovesPriorRowsAtomically(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("configuration-replacement")
	oldRoots := []SourceObserverRootRecord{{
		ID: "old-root", Generation: 1, Path: "/old", VolumeUUID: "volume", Inode: 1, Kind: 1,
	}}
	oldCheckpoints := []SourceObserverCheckpointRecord{{
		Stream: "old-stream", RootEpoch: "old-epoch", EventID: 4, AppliedEventID: 4,
	}}
	stageSourceObserverConfigurationForTest(
		t, c, sourceObserverConfigurationIdentityForTest(
			t, authority, causal.OperationID{1}, "old", "old-epoch", oldRoots, oldCheckpoints,
		), oldRoots, oldCheckpoints,
	)
	newRoots := []SourceObserverRootRecord{{
		ID: "new-root", Generation: 2, Path: "/new", VolumeUUID: "volume", Inode: 2, Kind: 1,
	}}
	newCheckpoints := []SourceObserverCheckpointRecord{{
		Stream: "new-stream", RootEpoch: "new-epoch", EventID: 9, AppliedEventID: 9,
	}}
	identity := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{2}, "new", "new-epoch", newRoots, newCheckpoints,
	)
	identity.Reset = true
	stageSourceObserverConfigurationForTest(t, c, identity, newRoots, newCheckpoints)
	assertSourceObserverConfigurationPages(t, c, authority, newRoots, newCheckpoints)
}

func sourceObserverConfigurationIdentityForTest(
	t *testing.T,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	stream string,
	epoch string,
	roots []SourceObserverRootRecord,
	checkpoints []SourceObserverCheckpointRecord,
) SourceObserverConfigurationIdentity {
	t.Helper()
	rootsDigest, err := SourceObserverRootsDigest(roots)
	if err != nil {
		t.Fatal(err)
	}
	checkpointsDigest, err := SourceObserverCheckpointsDigest(checkpoints)
	if err != nil {
		t.Fatal(err)
	}
	return SourceObserverConfigurationIdentity{
		Authority: authority, FleetOwner: SourceAuthorityFleetOwnerID("test:" + authority),
		FleetGeneration: 1, Operation: operation, Stream: stream, RootEpoch: epoch,
		RootDigest: [32]byte{1}, FleetDigest: [32]byte{2},
		RootCount: uint64(len(roots)), CheckpointCount: uint64(len(checkpoints)),
		RootsDigest: rootsDigest, CheckpointsDigest: checkpointsDigest,
	}
}

func stageSourceObserverConfigurationForTest(
	t *testing.T,
	c *Catalog,
	identity SourceObserverConfigurationIdentity,
	roots []SourceObserverRootRecord,
	checkpoints []SourceObserverCheckpointRecord,
) SourceObserverStreamRecord {
	t.Helper()
	ensureSourceObserverFleetForTest(t, c, identity)
	if err := c.BeginSourceObserverConfiguration(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	var ref SourceObserverConfigurationRef
	var err error
	for start := 0; start < len(roots); start += SourceObserverConfigurationPageLimit {
		end := min(start+SourceObserverConfigurationPageLimit, len(roots))
		ref, err = c.AppendSourceObserverConfigurationRoots(
			t.Context(), identity.Authority, identity.Operation,
			SourceObserverRootAppendPage{Sequence: ref.Sequence, Records: roots[start:end]},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	for start := 0; start < len(checkpoints); start += SourceObserverConfigurationPageLimit {
		end := min(start+SourceObserverConfigurationPageLimit, len(checkpoints))
		ref, err = c.AppendSourceObserverConfigurationCheckpoints(
			t.Context(), identity.Authority, identity.Operation,
			SourceObserverCheckpointAppendPage{Sequence: ref.Sequence, Records: checkpoints[start:end]},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	stream, err := c.CommitSourceObserverConfiguration(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func ensureSourceObserverFleetForTest(
	t *testing.T,
	c *Catalog,
	identity SourceObserverConfigurationIdentity,
) {
	t.Helper()
	if _, err := c.SourceAuthorityFleetHead(t.Context(), identity.FleetOwner); errors.Is(err, ErrNotFound) {
		stage := reconcileSourceAuthorityFleetForTest(
			t, c, identity.FleetOwner, 0, identity.FleetGeneration, identity.Authority,
		)
		acknowledgeSourceAuthorityFleetForTest(t, c, stage)
	} else if err != nil {
		t.Fatal(err)
	}
}

func assertSourceObserverConfigurationPages(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	wantRoots []SourceObserverRootRecord,
	wantCheckpoints []SourceObserverCheckpointRecord,
) {
	t.Helper()
	var roots []SourceObserverRootRecord
	for after := ""; ; {
		page, err := c.SourceObserverRootsPage(
			t.Context(), authority, after, SourceObserverConfigurationPageLimit,
		)
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, page.Records...)
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	var checkpoints []SourceObserverCheckpointRecord
	for after := ""; ; {
		page, err := c.SourceObserverCheckpointsPage(
			t.Context(), authority, after, SourceObserverConfigurationPageLimit,
		)
		if err != nil {
			t.Fatal(err)
		}
		checkpoints = append(checkpoints, page.Records...)
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	if fmt.Sprint(roots) != fmt.Sprint(wantRoots) ||
		fmt.Sprint(checkpoints) != fmt.Sprint(wantCheckpoints) {
		t.Fatalf("configuration pages roots=%+v checkpoints=%+v", roots, checkpoints)
	}
}
