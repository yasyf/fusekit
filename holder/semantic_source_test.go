package holder

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/sourcedriverruntime"
	"github.com/yasyf/fusekit/tenant"
)

func TestSemanticAuthorityReplacesImmutableTargetGenerationExactly(t *testing.T) {
	authority := &semanticAuthority{closeDone: make(chan struct{})}
	var mu sync.Mutex
	var opened [][]catalog.SourceDriverTarget
	var generations []*testSemanticGeneration
	authority.open = func(_ context.Context, targets []catalog.SourceDriverTarget) (semanticGenerationRuntime, error) {
		generation := &testSemanticGeneration{}
		mu.Lock()
		opened = append(opened, append([]catalog.SourceDriverTarget(nil), targets...))
		generations = append(generations, generation)
		mu.Unlock()
		return generation, nil
	}

	first := []tenant.TenantSpec{
		testAuthorityTenant("tenant-b", "semantic", 1),
		testAuthorityTenant("tenant-a", "semantic", 1),
	}
	if err := authority.Reconfigure(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if len(opened) != 1 || opened[0][0].Tenant != "tenant-a" || opened[0][1].Tenant != "tenant-b" {
		t.Fatalf("first exact target set = %+v", opened)
	}
	if err := authority.Reconfigure(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if len(opened) != 1 || generations[0].closeCalls() != 0 {
		t.Fatalf("same target set replaced generation: opens=%d closes=%d", len(opened), generations[0].closeCalls())
	}

	second := []tenant.TenantSpec{
		testAuthorityTenant("tenant-a", "semantic", 2),
		testAuthorityTenant("tenant-b", "semantic", 1),
	}
	if err := authority.Reconfigure(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if len(opened) != 2 || generations[0].recoveryCalls() != 1 || generations[0].closeCalls() != 1 {
		t.Fatalf(
			"replacement = opens %d, prior recoveries %d closes %d; want 2, 1, 1",
			len(opened), generations[0].recoveryCalls(), generations[0].closeCalls(),
		)
	}
	if opened[1][0].Generation != 2 || opened[1][1].Generation != 1 {
		t.Fatalf("replacement target generations = %+v", opened[1])
	}
	if err := authority.Reconfigure(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if generations[1].closeCalls() != 1 {
		t.Fatalf("drained generation closes = %d, want 1", generations[1].closeCalls())
	}
	if err := authority.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSemanticAuthorityFailedReplacementLeavesNoSubstitutedGeneration(t *testing.T) {
	injected := errors.New("open failed")
	first := &testSemanticGeneration{}
	authority := &semanticAuthority{
		generation: first,
		targets:    []catalog.SourceDriverTarget{{Tenant: "tenant", Generation: 1}},
		closeDone:  make(chan struct{}),
		open: func(context.Context, []catalog.SourceDriverTarget) (semanticGenerationRuntime, error) {
			return nil, injected
		},
	}
	err := authority.Reconfigure(t.Context(), []tenant.TenantSpec{testAuthorityTenant("tenant", "semantic", 2)})
	if !errors.Is(err, injected) {
		t.Fatalf("Reconfigure = %v, want injected failure", err)
	}
	if first.closeCalls() != 1 {
		t.Fatalf("prior generation closes = %d, want 1", first.closeCalls())
	}
	authority.mu.RLock()
	generation, targets := authority.generation, authority.targets
	authority.mu.RUnlock()
	if generation != nil || len(targets) != 0 {
		t.Fatalf("failed replacement published generation=%v targets=%+v", generation, targets)
	}
	if err := authority.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSemanticAuthorityFailsClosedWhenPriorGenerationCannotSettle(t *testing.T) {
	injected := errors.New("prior generation did not settle")
	prior := &testSemanticGeneration{closeErr: injected}
	opens := 0
	authority := &semanticAuthority{
		generation: prior,
		targets:    []catalog.SourceDriverTarget{{Tenant: "tenant", Generation: 1}},
		closeDone:  make(chan struct{}),
		open: func(context.Context, []catalog.SourceDriverTarget) (semanticGenerationRuntime, error) {
			opens++
			return &testSemanticGeneration{}, nil
		},
	}
	err := authority.Reconfigure(t.Context(), []tenant.TenantSpec{testAuthorityTenant("tenant", "semantic", 2)})
	if !errors.Is(err, injected) || opens != 0 {
		t.Fatalf("unsettled replacement = %v, opens %d; want failure before open", err, opens)
	}
	if err := authority.Reconfigure(t.Context(), nil); !errors.Is(err, sourceauthority.ErrClosed) {
		t.Fatalf("reconfigure after unsettled generation = %v, want closed", err)
	}
	if err := authority.Close(t.Context()); !errors.Is(err, injected) {
		t.Fatalf("Close = %v, want cached terminal failure", err)
	}
}

func TestSemanticSourceMutationCommitSettlesExactDriverReceipt(t *testing.T) {
	injected := errors.New("driver acknowledgement unavailable")
	generation := &testSemanticGeneration{settleErr: injected}
	authority := &semanticAuthority{
		authority: "semantic", generation: generation,
		targets:   []catalog.SourceDriverTarget{{Tenant: "tenant", Generation: 1}},
		closeDone: make(chan struct{}),
	}
	operation := catalog.MutationID{1}
	if err := authority.SourceMutationCommitted(t.Context(), tenant.SourceMutationCommit{
		OperationID: operation, SourceID: "semantic",
	}); !errors.Is(err, injected) {
		t.Fatalf("settlement failure = %v, want injected error", err)
	}
	if got := generation.settledMutation(); got != operation {
		t.Fatalf("settled mutation = %s, want %s", got, operation)
	}
	if err := authority.SourceMutationCommitted(t.Context(), tenant.SourceMutationCommit{
		OperationID: operation, SourceID: "other",
	}); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("mismatched source commit = %v, want integrity", err)
	}
	authority.mu.Lock()
	authority.generation = nil
	authority.mu.Unlock()
	if err := authority.SourceMutationCommitted(t.Context(), tenant.SourceMutationCommit{
		OperationID: operation, SourceID: "semantic",
	}); !errors.Is(err, sourceauthority.ErrClosed) {
		t.Fatalf("commit without active generation = %v, want closed", err)
	}
	if err := authority.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSemanticReceiptRecoveryOpensExactRetiredTargetGeneration(t *testing.T) {
	targets := []catalog.SourceDriverTarget{{Tenant: "retired", Generation: 3}}
	digest, err := catalog.SourceDriverTargetsDigest(targets)
	if err != nil {
		t.Fatal(err)
	}
	receipt := catalog.SourceDriverCommittedReceipt{Result: catalog.SourceDriverStageResult{
		Identity: catalog.SourceDriverStageIdentity{
			Authority: "semantic", TargetCount: 1, TargetsDigest: digest,
		},
	}}
	recovery := &testSemanticGeneration{}
	var opened []catalog.SourceDriverTarget
	authority := &semanticAuthority{
		authority: "semantic", closeDone: make(chan struct{}),
		pending: func(context.Context, causal.SourceAuthorityID) (*catalog.SourceDriverCommittedReceipt, error) {
			return &receipt, nil
		},
		targetPage: func(context.Context, causal.SourceAuthorityID, causal.OperationID, catalog.TenantID, int) (catalog.SourceDriverTargetCheckpointPage, error) {
			return catalog.SourceDriverTargetCheckpointPage{
				Targets: []catalog.SourceDriverTargetCheckpoint{{SourceDriverTarget: targets[0]}},
			}, nil
		},
		open: func(_ context.Context, targets []catalog.SourceDriverTarget) (semanticGenerationRuntime, error) {
			opened = append([]catalog.SourceDriverTarget(nil), targets...)
			return recovery, nil
		},
	}
	if err := authority.RecoverCommittedReceipts(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(opened, targets) || recovery.recoveryCalls() != 1 || recovery.closeCalls() != 1 {
		t.Fatalf("recovery generation = targets %+v recoveries %d closes %d", opened, recovery.recoveryCalls(), recovery.closeCalls())
	}
}

type testSemanticGeneration struct {
	mu          sync.Mutex
	closes      int
	settled     catalog.MutationID
	settleErr   error
	recoveries  int
	recoveryErr error
	closeErr    error
}

func (*testSemanticGeneration) Reconcile(context.Context) (sourcedriverruntime.ReconcileResult, error) {
	return sourcedriverruntime.ReconcileResult{}, nil
}

func (*testSemanticGeneration) ApplyPreparedMutation(
	context.Context,
	catalog.PreparedMutation,
	contentstream.Source,
) (sourcedriverruntime.MutationResult, error) {
	return sourcedriverruntime.MutationResult{}, nil
}

func (g *testSemanticGeneration) SettleCommittedMutation(
	_ context.Context,
	operation catalog.MutationID,
) (sourcedriverruntime.MutationResult, error) {
	g.mu.Lock()
	g.settled = operation
	err := g.settleErr
	g.mu.Unlock()
	return sourcedriverruntime.MutationResult{}, err
}

func (g *testSemanticGeneration) RecoverCommittedReceipts(context.Context) error {
	g.mu.Lock()
	g.recoveries++
	err := g.recoveryErr
	g.mu.Unlock()
	return err
}

func (g *testSemanticGeneration) Close() error {
	g.mu.Lock()
	g.closes++
	err := g.closeErr
	g.mu.Unlock()
	return err
}

func (g *testSemanticGeneration) closeCalls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closes
}

func (g *testSemanticGeneration) settledMutation() catalog.MutationID {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.settled
}

func (g *testSemanticGeneration) recoveryCalls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.recoveries
}
