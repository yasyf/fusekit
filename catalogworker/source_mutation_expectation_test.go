package catalogworker

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourceauthority"
)

func TestManagerRecoversSourceMutationExpectationReceipt(t *testing.T) {
	manager, _ := newTestManager(t)
	authority := causal.SourceAuthorityID("recover-source-mutation-receipt")
	tenantProvision := testTenantProvision(t, "recover-receipt")
	tenantProvision.ContentSourceID = string(authority)
	provision, err := manager.ProvisionTenant(t.Context(), tenantProvision)
	if err != nil {
		t.Fatal(err)
	}
	configureSourceMutationExpectationWorkerTest(t, manager, authority, provision)
	operation := catalog.MutationID{1}
	payload := []byte("mutation-plan")
	record := catalog.SourceMutationExpectationRecord{
		Operation:  operation,
		Authority:  authority,
		Tenant:     provision.Tenant,
		Generation: provision.Generation,
		Origin: catalog.CausalOrigin{
			Cause: causal.CauseDaemonWrite,
		},
		Digest:  sha256.Sum256(payload),
		Payload: payload,
	}
	if err := reserveSourceMutationExpectationWorkerTest(t, manager, record); err != nil {
		t.Fatal(err)
	}

	receipt := []byte("runtime-owned-receipt")
	if err := manager.RecoverSourceMutationExpectationReceipt(
		t.Context(), authority, operation, receipt,
	); err != nil {
		t.Fatal(err)
	}
	if err := manager.RecoverSourceMutationExpectationReceipt(
		t.Context(), authority, operation, receipt,
	); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if err := manager.RecoverSourceMutationExpectationReceipt(
		t.Context(), authority, operation, []byte("different-receipt"),
	); !errors.Is(err, catalog.ErrSourceObserverConflict) {
		t.Fatalf("different replay = %v, want source observer conflict", err)
	}

	got, err := manager.SourceMutationExpectation(t.Context(), authority, operation)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != catalog.SourceMutationExpectationComplete ||
		got.ReceiptDigest != sha256.Sum256(receipt) || !bytes.Equal(got.Receipt, receipt) {
		t.Fatalf("recovered expectation = %+v", got)
	}
}

func configureSourceMutationExpectationWorkerTest(
	t *testing.T,
	manager *Manager,
	authority causal.SourceAuthorityID,
	provision catalog.TenantProvision,
) {
	t.Helper()
	owner := catalog.SourceAuthorityFleetOwnerID("recover-source-mutation-receipt-owner")
	declarations, authoritiesDigest, declarationsDigest := testSourceAuthorityFleet(
		t, []causal.SourceAuthorityID{authority},
	)
	stage, err := manager.ReconcileSourceAuthorityFleet(t.Context(), catalog.SourceAuthorityFleetReconcileRequest{
		Owner:              owner,
		Generation:         1,
		Declarations:       declarations,
		Complete:           true,
		AuthorityCount:     1,
		AuthoritiesDigest:  authoritiesDigest,
		DeclarationsDigest: declarationsDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcknowledgeSourceAuthorityFleet(
		t.Context(), catalog.SourceAuthorityFleetAcknowledgement{
			Owner: owner, Generation: 1, AuthorityCount: 1,
			AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
			StageDigest: stage.StageDigest,
		},
	); err != nil {
		t.Fatal(err)
	}

	roots := []catalog.SourceObserverRootRecord{{
		ID: "root", Generation: 1, Path: "/source", VolumeUUID: "volume", Inode: 1, Kind: 1,
	}}
	checkpoints := []catalog.SourceObserverCheckpointRecord{{Stream: "stream", RootEpoch: "epoch"}}
	rootsDigest, err := catalog.SourceObserverRootsDigest(roots)
	if err != nil {
		t.Fatal(err)
	}
	checkpointsDigest, err := catalog.SourceObserverCheckpointsDigest(checkpoints)
	if err != nil {
		t.Fatal(err)
	}
	identity := catalog.SourceObserverConfigurationIdentity{
		Authority: authority, FleetOwner: owner, FleetGeneration: 1,
		Operation: causal.OperationID{1}, Stream: "stream", RootEpoch: "epoch",
		RootDigest: [32]byte{1}, FleetDigest: [32]byte{2},
		RootCount: 1, CheckpointCount: 1,
		RootsDigest: rootsDigest, CheckpointsDigest: checkpointsDigest,
	}
	if err := manager.BeginSourceObserverConfiguration(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	ref, err := manager.AppendSourceObserverConfigurationRoots(
		t.Context(), authority, identity.Operation,
		catalog.SourceObserverRootAppendPage{Records: roots},
	)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = manager.AppendSourceObserverConfigurationCheckpoints(
		t.Context(), authority, identity.Operation,
		catalog.SourceObserverCheckpointAppendPage{Sequence: ref.Sequence, Records: checkpoints},
	)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := manager.CommitSourceObserverConfiguration(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := sourceauthority.Fence{
		Authority: authority, AuthorityGeneration: stream.FleetGeneration,
		Inbox:       sourceauthority.InboxSequence(stream.LastReceived),
		RootDigest:  sourceauthority.Fingerprint(stream.RootDigest),
		FleetDigest: sourceauthority.Fingerprint(stream.FleetDigest),
	}
	for _, checkpoint := range checkpoints {
		fence.Streams = append(fence.Streams, sourceauthority.StreamCheckpoint{
			Identity:  sourceauthority.StreamIdentity(checkpoint.Stream),
			Cursor:    sourceauthority.EventID(checkpoint.EventID),
			RootEpoch: sourceauthority.RootEpoch(checkpoint.RootEpoch),
		})
	}
	encodedFence, err := json.Marshal(fence)
	if err != nil {
		t.Fatal(err)
	}
	watermark, err := manager.SourceWatermark(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := "recover-source-mutation-receipt"
	if err := manager.BeginSourceSnapshotStage(t.Context(), authority, snapshot); err != nil {
		t.Fatalf("begin source snapshot stage: %v", err)
	}
	operation := causal.OperationID{2}
	snapshotIdentity := catalog.SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: stream.FleetGeneration,
		Snapshot: snapshot, FenceDigest: sha256.Sum256(encodedFence),
		Change: causal.ChangeSet{
			SourceAuthority: authority, SourceRevision: watermark + 1,
			ChangeID: causal.ChangeID(operation), OperationID: operation,
			Cause: causal.CauseBootstrap,
		},
	}
	if err := manager.BeginSourceSnapshotPublication(t.Context(), snapshotIdentity); err != nil {
		t.Fatalf("begin source snapshot publication: %v", err)
	}
	rootKey, err := catalog.DeriveSourceDriverRootKey(authority, provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	logicalRoot := causal.LogicalKey("recover-source-mutation-receipt-root")
	snapshotRef, err := manager.AppendSourceSnapshotPublication(
		t.Context(), snapshotIdentity,
		catalog.SourceSnapshotPublicationPage{
			AffectedKeys: []causal.LogicalKey{logicalRoot},
			Roots: []catalog.SourceSnapshotRoot{{
				Tenant: provision.Tenant, Generation: provision.Generation,
				LogicalID: string(logicalRoot), RootKey: rootKey,
			}},
		},
	)
	if err != nil {
		t.Fatalf("append source snapshot publication: %v", err)
	}
	if _, err := manager.PromoteSourceSnapshot(t.Context(), snapshotRef, catalog.SourceSnapshotSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: authority, Stream: stream.Stream, RootEpoch: stream.RootEpoch,
			Through: stream.LastReceived, Operation: snapshotRef.Operation,
		},
		Snapshot: snapshotRef,
	}); err != nil {
		t.Fatalf("promote source snapshot: %v", err)
	}
}

func reserveSourceMutationExpectationWorkerTest(
	t *testing.T,
	manager *Manager,
	record catalog.SourceMutationExpectationRecord,
) error {
	t.Helper()
	stream, err := manager.SourceObserverStream(t.Context(), record.Authority)
	if err != nil {
		return err
	}
	checkpoints, err := manager.SourceObserverCheckpointsPage(
		t.Context(), record.Authority, "", catalog.SourceObserverConfigurationPageLimit,
	)
	if err != nil {
		return err
	}
	applied, err := manager.SourceObserverAppliedCheckpointsPage(
		t.Context(), record.Authority, "", catalog.SourceObserverConfigurationPageLimit,
	)
	if err != nil {
		return err
	}
	checkpointsDigest, err := catalog.SourceObserverCheckpointsDigest(checkpoints.Records)
	if err != nil {
		return err
	}
	appliedDigest, err := catalog.SourceObserverAppliedCheckpointsDigest(applied.Records)
	if err != nil {
		return err
	}
	return manager.ReserveSourceMutationExpectation(t.Context(), catalog.SourceMutationExpectationReservation{
		Record: record, Stream: stream.Stream, RootEpoch: stream.RootEpoch,
		LastReceived: stream.LastReceived, LastApplied: stream.LastApplied,
		CheckpointsDigest: checkpointsDigest, AppliedCheckpointsDigest: appliedDigest,
	})
}
