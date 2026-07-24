package sourceauthority

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

type rolloverRecoveryExecutor struct {
	*fakeExecutor
	inspection   MutationInspection
	acknowledged int
	abandoned    int
	applied      int
	statted      int
}

func (e *rolloverRecoveryExecutor) InspectMutation(
	_ context.Context,
	request MutationInspectionRequest,
) (MutationInspection, error) {
	inspection := e.inspection
	if inspection.State == MutationInspectionNotFound {
		return inspection, nil
	}
	inspection.ExpectationDigest = request.ExpectationDigest
	inspection.Intent = Fingerprint{1}
	if inspection.Receipt != nil {
		receipt := *inspection.Receipt
		receipt.OperationID = request.Operation
		inspection.Receipt = &receipt
	}
	if inspection.Terminal != nil {
		proof := *inspection.Terminal
		proof.Authority = request.Authority
		proof.AuthorityGeneration = request.AuthorityGeneration
		proof.Operation = request.Operation
		inspection.Terminal = &proof
	}
	return inspection, nil
}

func (e *rolloverRecoveryExecutor) AcknowledgeMutation(
	_ context.Context,
	authority causal.SourceAuthorityID,
	generation causal.Generation,
	operation catalog.MutationID,
	digest Fingerprint,
) error {
	e.acknowledged++
	e.inspection = MutationInspection{
		State: MutationInspectionTerminal,
		Terminal: &MutationTerminalProof{
			Authority: authority, AuthorityGeneration: generation, Operation: operation,
			Outcome: MutationAcknowledged, Digest: digest,
		},
	}
	return nil
}

func (e *rolloverRecoveryExecutor) AbandonMutation(
	_ context.Context,
	authority causal.SourceAuthorityID,
	generation causal.Generation,
	operation catalog.MutationID,
) error {
	e.abandoned++
	e.inspection = MutationInspection{
		State: MutationInspectionTerminal,
		Terminal: &MutationTerminalProof{
			Authority: authority, AuthorityGeneration: generation, Operation: operation,
			Outcome: MutationAbandoned,
		},
	}
	return nil
}

func (e *rolloverRecoveryExecutor) ApplyMutation(context.Context, MutationTask) (MutationReceipt, error) {
	e.applied++
	return MutationReceipt{}, errors.New("rollover recovery executed a source mutation")
}

func (e *rolloverRecoveryExecutor) Stat(context.Context, RootSpec, string) (PhysicalEntry, error) {
	e.statted++
	return PhysicalEntry{}, errors.New("rollover recovery probed the source filesystem")
}

func TestPriorGenerationMutationRecoverySurvivesRestartInEveryInspectionState(t *testing.T) {
	tests := []struct {
		name       string
		inspection MutationInspection
		wantAck    int
		wantDrop   int
		quarantine bool
	}{
		{name: "not-found", inspection: MutationInspection{State: MutationInspectionNotFound}},
		{name: "active-unapplied", inspection: MutationInspection{State: MutationInspectionActiveUnapplied}, wantDrop: 1},
		{name: "applied", inspection: MutationInspection{
			State: MutationInspectionApplied,
			Receipt: &MutationReceipt{Digest: Fingerprint{9}, Effects: []PhysicalEntry{{
				Root: "root", Relative: "settings.json", Exists: true, Kind: PhysicalFile,
				Identity: FileIdentity{VolumeUUID: "volume", Inode: 9, BirthtimeSec: 1},
			}}},
		}, wantAck: 1},
		{name: "terminal", inspection: MutationInspection{
			State:    MutationInspectionTerminal,
			Terminal: &MutationTerminalProof{Outcome: MutationAbandoned},
		}},
		{name: "consumed", inspection: MutationInspection{
			State:    MutationInspectionConsumed,
			Terminal: &MutationTerminalProof{Outcome: MutationAbandoned},
		}, quarantine: true},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalogPath := filepath.Join(t.TempDir(), "catalog.sqlite")
			store, err := catalog.Open(t.Context(), catalogPath)
			if err != nil {
				t.Fatal(err)
			}
			configureSourceObserverForTest(t, store, observerConfiguration{
				Authority: testAuthority, FleetOwner: "rollover-owner", FleetGeneration: 1,
				Stream: "stream", RootEpoch: "epoch", RootDigest: [32]byte{1}, FleetDigest: [32]byte{2},
				Roots: []catalog.SourceObserverRootRecord{{
					ID: "root", Generation: 1, Path: "/root", VolumeUUID: "volume", Inode: 1, Kind: uint8(RootDirectory),
				}},
				Checkpoints: []catalog.SourceObserverCheckpointRecord{{Stream: "stream", RootEpoch: "epoch"}},
			})
			operation := catalog.MutationID{byte(index + 1), 31}
			origin := catalog.CausalOrigin{Cause: causal.CauseDaemonWrite}
			step := tenant.SourceMutationStep{
				TenantID: "tenant", Generation: 1, OperationID: operation,
				SourceID: string(testAuthority), ExpectedHead: 1, Origin: origin,
			}
			envelope, err := json.Marshal(mutationEnvelope{
				Request:   MutationRequest{Step: step},
				Operation: tenant.SourceMutationOperation{OperationID: operation, SourceID: string(testAuthority)},
				Plan: MutationPlan{Effects: []ExpectedEffect{{
					Path:    PathRef{Root: "root", Relative: "settings.json"},
					Outcome: MutationPresent, Kind: PhysicalFile,
				}}},
				Fence: Fence{Authority: testAuthority, AuthorityGeneration: 1},
			})
			if err != nil {
				t.Fatal(err)
			}
			markSourceObserverIncrementalForRuntimeTest(t, catalogPath, testAuthority)
			if err := reserveSourceMutationExpectationForRuntimeTest(t, store, catalog.SourceMutationExpectationRecord{
				Operation: operation, Authority: testAuthority, Tenant: "tenant", Generation: 1,
				Origin: origin, Digest: sha256.Sum256(envelope), Payload: envelope,
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = catalog.Open(t.Context(), catalogPath)
			if err != nil {
				t.Fatal(err)
			}
			executor := &rolloverRecoveryExecutor{
				fakeExecutor: &fakeExecutor{fakePathSource: newFakePathSource(0), fakeBackend: newFakeBackend()},
				inspection:   test.inspection,
			}
			runtime := &Runtime{catalog: store, authority: testAuthority, fleetGeneration: 2, executor: executor}
			repaired, recoveryErr := runtime.recoverPriorGenerationMutationLiabilities(t.Context())
			if test.quarantine {
				if !errors.Is(recoveryErr, ErrQuarantined) || repaired {
					t.Fatalf("consumed recovery = repaired %v, err %v", repaired, recoveryErr)
				}
				if executor.applied != 0 || executor.statted != 0 || executor.acknowledged != 0 || executor.abandoned != 0 {
					t.Fatalf("consumed recovery performed work: %+v", executor)
				}
				_ = store.Close()
				return
			}
			if recoveryErr != nil || !repaired {
				t.Fatalf("prior-generation recovery = repaired %v, err %v", repaired, recoveryErr)
			}
			if executor.acknowledged != test.wantAck || executor.abandoned != test.wantDrop ||
				executor.applied != 0 || executor.statted != 0 {
				t.Fatalf("recovery worker calls = ack %d abandon %d apply %d stat %d",
					executor.acknowledged, executor.abandoned, executor.applied, executor.statted)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = catalog.Open(t.Context(), catalogPath)
			if err != nil {
				t.Fatal(err)
			}
			runtime.catalog = store
			record, err := store.SourceMutationExpectation(t.Context(), testAuthority, operation)
			if err != nil || record.State != catalog.SourceMutationExpectationRepairRequired ||
				(test.name == "applied") != (len(record.Receipt) != 0) {
				t.Fatalf("recovered expectation after restart = %+v, %v", record, err)
			}
			if err := store.BeginSourceSnapshotStage(t.Context(), testAuthority, "rollover-repair"); err != nil {
				t.Fatal(err)
			}
			repairOperation := causal.OperationID{byte(index + 40)}
			ref := stageRepairSnapshotForTest(t, store, testAuthority, "rollover-repair", repairOperation)
			if _, err := store.PromoteSourceSnapshot(t.Context(), ref, catalog.SourceSnapshotSettlement{
				Fence: catalog.SourceObserverSettlement{
					Authority: testAuthority, Stream: "stream", RootEpoch: "epoch", Operation: repairOperation,
				},
				Snapshot: ref, MismatchAllActive: true,
			}); err != nil {
				t.Fatal(err)
			}
			if err := runtime.cleanupPublishedMutationRepairs(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := store.SourceMutationExpectation(t.Context(), testAuthority, operation); !errors.Is(err, catalog.ErrNotFound) {
				t.Fatalf("settled rollover liability remained: %v", err)
			}
			if executor.applied != 0 || executor.statted != 0 {
				t.Fatalf("rollover cleanup reached source I/O: apply %d stat %d", executor.applied, executor.statted)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func (e *rolloverRecoveryExecutor) String() string {
	return fmt.Sprintf("state=%d ack=%d abandon=%d apply=%d stat=%d", e.inspection.State,
		e.acknowledged, e.abandoned, e.applied, e.statted)
}
