package sourceauthority

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

type escapedGenerationExecutor struct {
	*fakeExecutor
	proof  MutationTerminalProof
	forgot bool
}

type sparseTerminalProofExecutor struct {
	*fakeExecutor
	pages   map[catalog.MutationID]MutationTerminalProofPage
	queries []catalog.MutationID
	forgot  []MutationTerminalProof
}

func (e *sparseTerminalProofExecutor) MutationTerminalProofPage(
	_ context.Context,
	_ causal.SourceAuthorityID,
	after catalog.MutationID,
	_ int,
) (MutationTerminalProofPage, error) {
	e.queries = append(e.queries, after)
	return e.pages[after], nil
}

func (e *sparseTerminalProofExecutor) ForgetMutation(
	_ context.Context,
	_ causal.SourceAuthorityID,
	proof MutationTerminalProof,
) error {
	e.forgot = append(e.forgot, proof)
	return nil
}

func (e *escapedGenerationExecutor) MutationTerminalProofPage(
	context.Context,
	causal.SourceAuthorityID,
	catalog.MutationID,
	int,
) (MutationTerminalProofPage, error) {
	return MutationTerminalProofPage{Proofs: []MutationTerminalProof{e.proof}, Next: e.proof.Operation}, nil
}

func (e *escapedGenerationExecutor) ForgetMutation(
	context.Context,
	causal.SourceAuthorityID,
	MutationTerminalProof,
) error {
	e.forgot = true
	return nil
}

func TestRuntimeFenceCarriesAuthorityGeneration(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{authority: "authority", fleetGeneration: 9}
	fence := runtime.fence(catalog.SourceObserverState{}, nil)
	if fence.Authority != runtime.authority || fence.AuthorityGeneration != runtime.fleetGeneration {
		t.Fatalf("runtime fence identity = %+v", fence)
	}
}

func TestSourceTaskContractsRejectZeroAuthorityGeneration(t *testing.T) {
	t.Parallel()
	proof := MutationTerminalProof{
		Authority: "authority", Operation: catalog.MutationID{1}, Outcome: MutationAbandoned,
	}
	if err := validateMutationTerminalProof(proof); err == nil {
		t.Fatal("zero-generation mutation terminal proof was accepted")
	}
	proof.AuthorityGeneration = causal.Generation(1)
	if err := validateMutationTerminalProof(proof); err != nil {
		t.Fatalf("complete mutation terminal proof = %v", err)
	}
	if _, _, err := encodeSourceTaskMutationProofPage(0, Fingerprint{}, []MutationTerminalProof{{
		Authority: "authority", Operation: catalog.MutationID{2}, Outcome: MutationAbandoned,
	}}); err == nil {
		t.Fatal("zero-generation mutation proof page was encoded")
	}

	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{3}, "value", []byte("value"))
	task.Fence.AuthorityGeneration = 0
	if _, _, _, err := encodeMutationRequest(task); err == nil {
		t.Fatal("zero-generation source mutation request was encoded")
	}

	executor := &supervisedExecutor{}
	if _, err := executor.MutationTerminalProofPage(t.Context(), root.Authority, catalog.MutationID{}, 0); err == nil {
		t.Fatal("zero-limit mutation proof listing was started")
	}
}

func TestMutationTerminalStateIsIsolatedByAuthorityGeneration(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{4}, "value", []byte("value"))
	executor := &supervisedExecutor{
		runtimeDir: runtimeDir,
		launcher:   &testSourceTaskLauncher{pathSource: &testFullPathSource{}},
		identity:   testSourceTaskIdentity(),
	}
	receipt, err := executor.ApplyMutation(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	wrongGeneration := task.Fence.AuthorityGeneration + 1
	if err := executor.AcknowledgeMutation(
		t.Context(), root.Authority, wrongGeneration, task.OperationID, receipt.Digest,
	); err == nil {
		t.Fatal("a newer authority generation settled an older mutation")
	}
	if err := executor.AcknowledgeMutation(
		t.Context(), root.Authority, task.Fence.AuthorityGeneration, task.OperationID, receipt.Digest,
	); err != nil {
		t.Fatal(err)
	}
	page, err := executor.MutationTerminalProofPage(
		t.Context(), root.Authority, catalog.MutationID{}, MutationTerminalProofPageLimit,
	)
	if err != nil || len(page.Proofs) != 1 || page.Proofs[0].AuthorityGeneration != task.Fence.AuthorityGeneration {
		t.Fatalf("authority-wide terminal proofs = %+v, %v", page, err)
	}
}

func TestRuntimeForgetsOrphanTerminalProofFromEarlierAuthorityGeneration(t *testing.T) {
	t.Parallel()
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	executor := &escapedGenerationExecutor{
		fakeExecutor: &fakeExecutor{},
		proof: MutationTerminalProof{
			Authority: "authority", AuthorityGeneration: 1,
			Operation: catalog.MutationID{5}, Outcome: MutationAbandoned,
		},
	}
	runtime := &Runtime{
		catalog: store, authority: "authority", fleetGeneration: 2, executor: executor,
	}
	if err := runtime.cleanupTerminalMutationProofs(t.Context()); err != nil {
		t.Fatalf("old-generation orphan cleanup = %v", err)
	}
	if !executor.forgot {
		t.Fatal("old-generation orphan terminal proof was not forgotten")
	}
}

func TestRuntimePagesPastSparseForeignMutationJournals(t *testing.T) {
	t.Parallel()
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	firstCursor := catalog.MutationID{1}
	proof := MutationTerminalProof{
		Authority: "authority", AuthorityGeneration: 1,
		Operation: catalog.MutationID{2}, Outcome: MutationAbandoned,
	}
	lastCursor := catalog.MutationID{3}
	executor := &sparseTerminalProofExecutor{
		fakeExecutor: &fakeExecutor{},
		pages: map[catalog.MutationID]MutationTerminalProofPage{
			{}:          {Next: firstCursor, More: true},
			firstCursor: {Proofs: []MutationTerminalProof{proof}, Next: lastCursor},
		},
	}
	runtime := &Runtime{catalog: store, authority: "authority", fleetGeneration: 2, executor: executor}
	if err := runtime.cleanupTerminalMutationProofs(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(executor.queries) != 2 || executor.queries[0] != (catalog.MutationID{}) ||
		executor.queries[1] != firstCursor {
		t.Fatalf("proof page cursors = %v", executor.queries)
	}
	if len(executor.forgot) != 1 || executor.forgot[0] != proof {
		t.Fatalf("forgotten proof = %+v", executor.forgot)
	}
}
