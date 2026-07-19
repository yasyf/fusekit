package sourceauthority

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"golang.org/x/sys/unix"
)

func TestSourceTaskMutationIsRawIdempotentAndExplicitlyAcknowledged(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{1}, "value", []byte("durable"))
	launcher := &testSourceTaskLauncher{pathSource: &testFullPathSource{}}
	executor := &supervisedExecutor{runtimeDir: runtimeDir, launcher: launcher, identity: testSourceTaskIdentity()}
	inspectionRequest := MutationInspectionRequest{
		Authority: task.Fence.Authority, AuthorityGeneration: task.Fence.AuthorityGeneration,
		Operation: task.OperationID, ExpectationDigest: task.ExpectationDigest,
	}
	inspection, err := executor.InspectMutation(t.Context(), inspectionRequest)
	if err != nil || inspection.State != MutationInspectionNotFound {
		t.Fatalf("missing mutation inspection = %+v, %v", inspection, err)
	}
	first, err := executor.ApplyMutation(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	second, err := executor.ApplyMutation(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest || !samePhysical(first.Effects[0], second.Effects[0]) {
		t.Fatal("lost-response replay did not return the exact durable receipt")
	}
	inspection, err = executor.InspectMutation(t.Context(), inspectionRequest)
	if err != nil || inspection.State != MutationInspectionApplied || inspection.Receipt == nil ||
		inspection.Receipt.Digest != first.Digest || inspection.ExpectationDigest != task.ExpectationDigest ||
		inspection.Intent == (Fingerprint{}) {
		t.Fatalf("applied mutation inspection = %+v, %v", inspection, err)
	}
	got, err := os.ReadFile(filepath.Join(root.Path, "value"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("durable")) {
		t.Fatalf("mutated content = %q", got)
	}
	if err := executor.AcknowledgeMutation(t.Context(), task.Fence.Authority, task.Fence.AuthorityGeneration, task.OperationID, Fingerprint{1}); err == nil {
		t.Fatal("mismatched receipt acknowledgement was accepted")
	}
	if err := executor.AcknowledgeMutation(t.Context(), task.Fence.Authority, task.Fence.AuthorityGeneration, task.OperationID, first.Digest); err != nil {
		t.Fatal(err)
	}
	if err := executor.AcknowledgeMutation(t.Context(), task.Fence.Authority, task.Fence.AuthorityGeneration, task.OperationID, first.Digest); err != nil {
		t.Fatalf("exact acknowledgement retry = %v", err)
	}
	inspection, err = executor.InspectMutation(t.Context(), inspectionRequest)
	if err != nil || inspection.State != MutationInspectionTerminal || inspection.Terminal == nil ||
		inspection.Terminal.Digest != first.Digest {
		t.Fatalf("terminal mutation inspection = %+v, %v", inspection, err)
	}
	if err := executor.AbandonMutation(t.Context(), task.Fence.Authority, task.Fence.AuthorityGeneration, task.OperationID); err == nil {
		t.Fatal("cross-kind terminal retry was accepted")
	}
	proofs, err := mutationProofsForTest(t, executor, task.Fence.Authority)
	if err != nil || len(proofs) != 1 || proofs[0].Digest != first.Digest {
		t.Fatalf("terminal proofs = %+v, %v", proofs, err)
	}
	if err := executor.ForgetMutation(t.Context(), task.Fence.Authority, proofs[0]); err != nil {
		t.Fatal(err)
	}
	inspection, err = executor.InspectMutation(t.Context(), inspectionRequest)
	if err != nil || inspection.State != MutationInspectionConsumed || inspection.Terminal == nil ||
		inspection.Terminal.Digest != first.Digest {
		t.Fatalf("consumed mutation inspection = %+v, %v", inspection, err)
	}
	consumed, exists, err := loadMutationOperation(t.Context(), runtimeDir, mutationOperationIdentityForTask(task))
	if err != nil || !exists || !consumed.Consumed || consumed.Intent == (Fingerprint{}) || consumed.Task != nil ||
		consumed.Terminal == nil || *consumed.Terminal != proofs[0] {
		t.Fatalf("consumed mutation tombstone = %+v, exists %v, err %v", consumed, exists, err)
	}
	proofs, err = mutationProofsForTest(t, executor, task.Fence.Authority)
	if err != nil || len(proofs) != 0 {
		t.Fatalf("consumed mutation remained pending cleanup: %+v, %v", proofs, err)
	}
}

func TestMutationConsumedTombstoneSurvivesProcessRestart(t *testing.T) {
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{91}, "value", []byte("durable"))
	proof := consumeMutationTask(t, runtimeDir, task)
	detachedRoot := root.Path + "-detached"
	if err := os.Rename(root.Path, detachedRoot); err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(t.TempDir(), "consumed-restart.json")
	payload, err := json.Marshal(struct {
		RuntimeDir string
		Task       MutationTask
		Proof      MutationTerminalProof
	}{runtimeDir, task, proof})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixturePath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"same", "different"} {
		command := exec.Command(os.Args[0], "-test.run=^TestMutationConsumedTombstoneProcessHelper$", "-test.v")
		command.Env = append(os.Environ(),
			"FUSEKIT_MUTATION_TOMBSTONE_HELPER=1",
			"FUSEKIT_MUTATION_TOMBSTONE_FIXTURE="+fixturePath,
			"FUSEKIT_MUTATION_TOMBSTONE_MODE="+mode,
		)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%s-request process restart = %v\n%s", mode, err, output)
		}
		got, err := os.ReadFile(filepath.Join(detachedRoot, "value"))
		if err != nil || string(got) != "durable" {
			t.Fatalf("%s-request process restart changed source = %q, %v", mode, got, err)
		}
	}
	journal, exists, err := loadMutationOperation(t.Context(), runtimeDir, mutationOperationIdentityForTask(task))
	if err != nil || !exists || !journal.Consumed || journal.Terminal == nil || *journal.Terminal != proof {
		t.Fatalf("process-restarted tombstone = %+v, exists %v, err %v", journal, exists, err)
	}
}

func TestMutationConsumedTombstoneProcessHelper(t *testing.T) {
	if os.Getenv("FUSEKIT_MUTATION_TOMBSTONE_HELPER") != "1" {
		return
	}
	payload, err := os.ReadFile(os.Getenv("FUSEKIT_MUTATION_TOMBSTONE_FIXTURE"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		RuntimeDir string
		Task       MutationTask
		Proof      MutationTerminalProof
	}
	if err := json.Unmarshal(payload, &fixture); err != nil {
		t.Fatal(err)
	}
	if err := validateMutationJournalDirectory(t.Context(), fixture.RuntimeDir); err != nil {
		t.Fatalf("consumed journal startup validation = %v", err)
	}
	journal, exists, err := loadMutationOperation(
		t.Context(), fixture.RuntimeDir, mutationOperationIdentityForTask(fixture.Task),
	)
	if err != nil || !exists || !journal.Consumed || journal.Terminal == nil || *journal.Terminal != fixture.Proof {
		t.Fatalf("consumed journal inspection = %+v, exists %v, err %v", journal, exists, err)
	}
	mode := os.Getenv("FUSEKIT_MUTATION_TOMBSTONE_MODE")
	if mode == "different" {
		fixture.Task.Program.Actions[0].Data = []byte("different")
	}
	payloads := mutationTestPayloads(t, fixture.RuntimeDir, fixture.Task)
	defer func() { _ = payloads.Close() }()
	_, err = applyMutationTask(t.Context(), fixture.RuntimeDir, fixture.Task, payloads, func(int) error {
		return errors.New("consumed mutation executed an action")
	})
	switch mode {
	case "same":
		if !errors.Is(err, errMutationConsumed) {
			t.Fatalf("same consumed request = %v, want consumed", err)
		}
	case "different":
		if !errors.Is(err, catalog.ErrMutationConflict) {
			t.Fatalf("different consumed request = %v, want conflict", err)
		}
	default:
		t.Fatalf("unknown tombstone helper mode %q", mode)
	}
}

func TestMutationForgetIsCrashAtomicAtReplacementBoundaries(t *testing.T) {
	for _, phase := range []mutationJournalStorePhase{
		mutationJournalStorePrepared, mutationJournalStoreReplaced, mutationJournalStoreDurable,
	} {
		phase := phase
		t.Run(string(rune('0'+phase)), func(t *testing.T) {
			runtimeDir := shortTaskRuntimeDir(t)
			root := mutationTestRoot(t)
			task := mutationWriteTask(t, root, catalog.MutationID{byte(91 + phase)}, "value", []byte("durable"))
			proof := terminalMutationTask(t, runtimeDir, task)
			injected := errors.New("injected journal replacement failure")
			err := forgetMutationJournalWithFailpoint(
				t.Context(), runtimeDir, proof,
				func(current mutationJournalStorePhase) error {
					if current == phase {
						return injected
					}
					return nil
				},
			)
			if !errors.Is(err, injected) {
				t.Fatalf("forget failpoint %d = %v", phase, err)
			}
			if err := validateMutationJournalDirectory(t.Context(), runtimeDir); err != nil {
				t.Fatalf("journal after failpoint %d = %v", phase, err)
			}
			active, activeExists, err := loadMutationJournal(t.Context(), runtimeDir, task.OperationID)
			if err != nil || !activeExists || active.Terminal == nil || *active.Terminal != proof {
				t.Fatalf("active journal after failpoint %d = %+v, exists %v, err %v", phase, active, activeExists, err)
			}
			consumed, consumedExists, err := loadConsumedMutationAt(
				t.Context(), runtimeDir, mutationOperationIdentityForTask(task),
			)
			if err != nil || consumedExists != (phase != mutationJournalStorePrepared) ||
				consumedExists && (!consumed.Consumed || consumed.Terminal == nil || *consumed.Terminal != proof) {
				t.Fatalf("consumed journal after failpoint %d = %+v, exists %v, err %v", phase, consumed, consumedExists, err)
			}
			page, err := mutationTerminalProofPage(
				t.Context(), runtimeDir, task.Fence.Authority, catalog.MutationID{}, MutationTerminalProofPageLimit,
			)
			proofs := page.Proofs
			if err != nil || phase != mutationJournalStorePrepared && len(proofs) != 0 ||
				phase == mutationJournalStorePrepared && len(proofs) != 1 {
				t.Fatalf("proofs after failpoint %d = %+v, %v", phase, proofs, err)
			}
			if err := forgetMutationJournal(t.Context(), runtimeDir, proof); err != nil {
				t.Fatalf("forget retry after failpoint %d = %v", phase, err)
			}
			if _, activeExists, err := loadMutationJournal(t.Context(), runtimeDir, task.OperationID); err != nil || activeExists {
				t.Fatalf("active journal after forget retry %d = exists %v, err %v", phase, activeExists, err)
			}
			journal, exists, err := loadMutationOperation(
				t.Context(), runtimeDir, mutationOperationIdentityForTask(task),
			)
			if err != nil || !exists || !journal.Consumed || journal.Task != nil || journal.Intent == (Fingerprint{}) {
				t.Fatalf("final tombstone after failpoint %d = %+v, exists %v, err %v", phase, journal, exists, err)
			}
		})
	}
}

func terminalMutationTask(t *testing.T, runtimeDir string, task MutationTask) MutationTerminalProof {
	t.Helper()
	payloads := mutationTestPayloads(t, runtimeDir, task)
	defer func() { _ = payloads.Close() }()
	receipt, err := applyMutationTask(t.Context(), runtimeDir, task, payloads, nil)
	if err != nil {
		t.Fatal(err)
	}
	proof := MutationTerminalProof{
		Authority: task.Fence.Authority, AuthorityGeneration: task.Fence.AuthorityGeneration, Operation: task.OperationID,
		Outcome: MutationAcknowledged, Digest: receipt.Digest,
	}
	if err := settleMutationJournal(t.Context(), runtimeDir, proof); err != nil {
		t.Fatal(err)
	}
	return proof
}

func consumeMutationTask(t *testing.T, runtimeDir string, task MutationTask) MutationTerminalProof {
	t.Helper()
	proof := terminalMutationTask(t, runtimeDir, task)
	if err := forgetMutationJournal(t.Context(), runtimeDir, proof); err != nil {
		t.Fatal(err)
	}
	return proof
}

func TestSourceTaskMutationStreamsRequestContentWithoutJSONBodyEncoding(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{24}, "value", []byte("placeholder"))
	task.Program.Actions[0].Data = nil
	task.Program.Actions[0].UseRequestContent = true
	task.Content = byteTaskContent("streamed-request-content")
	executor := &supervisedExecutor{
		runtimeDir: runtimeDir, launcher: &testSourceTaskLauncher{pathSource: &testFullPathSource{}},
		identity: testSourceTaskIdentity(),
	}
	receipt, err := executor.ApplyMutation(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := executor.InspectMutation(t.Context(), MutationInspectionRequest{
		Authority: task.Fence.Authority, AuthorityGeneration: task.Fence.AuthorityGeneration,
		Operation: task.OperationID, ExpectationDigest: task.ExpectationDigest,
	})
	if err != nil || inspection.State != MutationInspectionApplied || inspection.RequestContent == nil ||
		inspection.RequestContent.Size != int64(len("streamed-request-content")) ||
		inspection.RequestContent.Digest != sha256.Sum256([]byte("streamed-request-content")) {
		t.Fatalf("request-content mutation inspection = %+v, %v", inspection, err)
	}
	got, err := os.ReadFile(filepath.Join(root.Path, "value"))
	if err != nil || string(got) != "streamed-request-content" {
		t.Fatalf("request-content mutation = %q, %v", got, err)
	}
	if err := executor.AcknowledgeMutation(t.Context(), task.Fence.Authority, task.Fence.AuthorityGeneration, task.OperationID, receipt.Digest); err != nil {
		t.Fatal(err)
	}
	if err := executor.ForgetMutation(t.Context(), task.Fence.Authority, MutationTerminalProof{
		Authority: task.Fence.Authority, AuthorityGeneration: task.Fence.AuthorityGeneration,
		Operation: task.OperationID, Outcome: MutationAcknowledged, Digest: receipt.Digest,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMutationConsumedRequestDigestRejectsEveryReplayWithoutExecuting(t *testing.T) {
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	base := mutationWriteTask(t, root, catalog.MutationID{93}, "value", nil)
	base.Program.Actions[0].UseRequestContent = true
	executor := &supervisedExecutor{
		runtimeDir: runtimeDir, launcher: &testSourceTaskLauncher{pathSource: &testFullPathSource{}},
		identity: testSourceTaskIdentity(),
	}
	first := base
	first.Content = byteTaskContent("durable-request")
	receipt, err := executor.ApplyMutation(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	proof := MutationTerminalProof{
		Authority: root.Authority, AuthorityGeneration: base.Fence.AuthorityGeneration, Operation: base.OperationID,
		Outcome: MutationAcknowledged, Digest: receipt.Digest,
	}
	if err := executor.AcknowledgeMutation(t.Context(), root.Authority, base.Fence.AuthorityGeneration, base.OperationID, receipt.Digest); err != nil {
		t.Fatal(err)
	}
	if err := executor.ForgetMutation(t.Context(), root.Authority, proof); err != nil {
		t.Fatal(err)
	}
	journal, exists, err := loadMutationOperation(t.Context(), runtimeDir, mutationOperationIdentityForTask(base))
	if err != nil || !exists || !journal.Consumed || journal.Request == nil ||
		journal.Request.Size != int64(len("durable-request")) || journal.Request.Hash != sha256.Sum256([]byte("durable-request")) {
		t.Fatalf("consumed request digest = %+v, exists %v, err %v", journal.Request, exists, err)
	}
	for name, body := range map[string]string{"same": "durable-request", "different": "other-request"} {
		replay := base
		replay.Content = byteTaskContent(body)
		_, err := executor.ApplyMutation(t.Context(), replay)
		if name == "same" && (err == nil || !strings.Contains(err.Error(), errMutationConsumed.Error())) {
			t.Fatalf("same consumed request = %v", err)
		}
		if name == "different" && (err == nil || !strings.Contains(err.Error(), catalog.ErrMutationConflict.Error())) {
			t.Fatalf("different consumed request = %v", err)
		}
	}
	got, err := os.ReadFile(filepath.Join(root.Path, "value"))
	if err != nil || string(got) != "durable-request" {
		t.Fatalf("consumed replay changed source = %q, %v", got, err)
	}
}

func TestMutationTerminalProofsAreInvisibleAcrossAuthorities(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	launcher := &testSourceTaskLauncher{pathSource: &testFullPathSource{}}
	executor := &supervisedExecutor{runtimeDir: runtimeDir, launcher: launcher, identity: testSourceTaskIdentity()}
	rootA := mutationTestRoot(t)
	rootA.Authority = "authority-a"
	rootB := mutationTestRoot(t)
	rootB.Authority = "authority-b"
	taskA := mutationWriteTask(t, rootA, catalog.MutationID{70}, "value", []byte("a"))
	taskB := mutationWriteTask(t, rootB, catalog.MutationID{71}, "value", []byte("b"))
	receiptA, err := executor.ApplyMutation(t.Context(), taskA)
	if err != nil {
		t.Fatal(err)
	}
	receiptB, err := executor.ApplyMutation(t.Context(), taskB)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.AcknowledgeMutation(t.Context(), rootA.Authority, taskA.Fence.AuthorityGeneration, taskA.OperationID, receiptA.Digest); err != nil {
		t.Fatal(err)
	}
	if err := executor.AcknowledgeMutation(t.Context(), rootB.Authority, taskB.Fence.AuthorityGeneration, taskB.OperationID, receiptB.Digest); err != nil {
		t.Fatal(err)
	}
	proofB := MutationTerminalProof{
		Authority: rootB.Authority, AuthorityGeneration: taskB.Fence.AuthorityGeneration, Operation: taskB.OperationID,
		Outcome: MutationAcknowledged, Digest: receiptB.Digest,
	}
	if err := executor.ForgetMutation(t.Context(), rootA.Authority, proofB); err == nil {
		t.Fatal("authority A forgot authority B's terminal proof")
	}
	databasePath := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := catalog.Open(t.Context(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	runtimeA := &Runtime{
		catalog: store, authority: rootA.Authority,
		fleetGeneration: taskA.Fence.AuthorityGeneration, executor: executor,
	}
	if err := runtimeA.cleanupTerminalMutationProofs(t.Context()); err != nil {
		t.Fatal(err)
	}
	proofsA, err := mutationProofsForTest(t, executor, rootA.Authority)
	if err != nil || len(proofsA) != 0 {
		t.Fatalf("authority A proofs after GC = %+v, %v", proofsA, err)
	}
	proofsB, err := mutationProofsForTest(t, executor, rootB.Authority)
	if err != nil || len(proofsB) != 1 || proofsB[0] != proofB {
		t.Fatalf("authority B proof after authority A GC = %+v, %v", proofsB, err)
	}
}

func TestSourceTaskMutationKilledAfterDurableReceiptReplaysInNewChild(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{32}, "value", []byte("durable"))
	receiptReady := make(chan struct{})
	launcher := &testSourceTaskLauncher{
		pathSource: &testFullPathSource{},
		afterMutation: func(ctx context.Context, _ MutationReceipt) error {
			close(receiptReady)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	executor := &supervisedExecutor{runtimeDir: runtimeDir, launcher: launcher, identity: testSourceTaskIdentity()}
	failed := make(chan error, 1)
	go func() {
		_, err := executor.ApplyMutation(context.Background(), task)
		failed <- err
	}()
	select {
	case <-receiptReady:
	case <-time.After(5 * time.Second):
		t.Fatal("child did not durably journal its receipt")
	}
	launcher.mu.Lock()
	process := launcher.processes[0]
	launcher.mu.Unlock()
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := process.Stop(stopCtx); err != nil && !errors.Is(err, context.Canceled) {
		cancel()
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-failed:
		if err == nil {
			t.Fatal("killed child unexpectedly delivered its receipt")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("killed mutation child did not settle")
	}
	journal, exists, err := loadMutationJournal(t.Context(), runtimeDir, task.OperationID)
	if err != nil || !exists || journal.Receipt == nil {
		t.Fatalf("durable receipt after child kill = exists %v journal %+v err %v", exists, journal, err)
	}
	replayLauncher := &testSourceTaskLauncher{pathSource: &testFullPathSource{}}
	replayExecutor := &supervisedExecutor{
		runtimeDir: runtimeDir, launcher: replayLauncher, identity: testSourceTaskIdentity(),
	}
	receipt, err := replayExecutor.ApplyMutation(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	replayLauncher.mu.Lock()
	children := len(replayLauncher.processes)
	replayLauncher.mu.Unlock()
	if children != 1 {
		t.Fatalf("replay launched %d new children, want one", children)
	}
	if receipt.Digest != journal.Receipt.Digest {
		t.Fatal("new child did not replay the exact durable receipt")
	}
	if err := replayExecutor.AcknowledgeMutation(t.Context(), task.Fence.Authority, task.Fence.AuthorityGeneration, task.OperationID, receipt.Digest); err != nil {
		t.Fatal(err)
	}
	if err := replayExecutor.ForgetMutation(t.Context(), task.Fence.Authority, MutationTerminalProof{
		Authority: task.Fence.Authority, AuthorityGeneration: task.Fence.AuthorityGeneration,
		Operation: task.OperationID, Outcome: MutationAcknowledged, Digest: receipt.Digest,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSourceTaskMutationDeadlineReapsWorkerAndPreservesReceipt(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{44}, "value", []byte("durable"))
	mutationApplied := make(chan struct{})
	launcher := &testSourceTaskLauncher{
		pathSource: &testFullPathSource{},
		afterMutation: func(ctx context.Context, _ MutationReceipt) error {
			close(mutationApplied)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	deadlines := StandardOperationDeadlines()
	deadlines.Mutation = 2 * time.Second
	executor := &supervisedExecutor{
		runtimeDir: runtimeDir, launcher: launcher, deadlines: deadlines, identity: testSourceTaskIdentity(),
	}
	if _, err := executor.ApplyMutation(t.Context(), task); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("mutation deadline = %v", err)
	}
	select {
	case <-mutationApplied:
	default:
		t.Fatal("mutation deadline expired before the durable-receipt failpoint")
	}
	launcher.mu.Lock()
	first := launcher.processes[0]
	launcher.mu.Unlock()
	assertSourceTaskProcessStopped(t, first)
	journal, exists, err := loadMutationJournal(t.Context(), runtimeDir, task.OperationID)
	if err != nil || !exists || journal.Receipt == nil {
		t.Fatalf("deadline receipt = %+v exists=%v err=%v", journal.Receipt, exists, err)
	}
	if err := executor.AcknowledgeMutation(t.Context(), task.Fence.Authority, task.Fence.AuthorityGeneration, task.OperationID, journal.Receipt.Digest); err != nil {
		t.Fatal(err)
	}
	if err := executor.ForgetMutation(t.Context(), task.Fence.Authority, MutationTerminalProof{
		Authority: task.Fence.Authority, AuthorityGeneration: task.Fence.AuthorityGeneration, Operation: task.OperationID,
		Outcome: MutationAcknowledged, Digest: journal.Receipt.Digest,
	}); err != nil {
		t.Fatal(err)
	}
	launcher.afterMutation = nil
	declaredRoot := root
	declaredRoot.ExpectedIdentity = FileIdentity{}
	if _, err := executor.RootIdentity(t.Context(), declaredRoot); err != nil {
		t.Fatalf("operation after mutation deadline = %v", err)
	}
}

func TestMutationCrashAfterEveryActionResumesToOnePoststate(t *testing.T) {
	t.Parallel()
	for crashPoint := 0; crashPoint < 2; crashPoint++ {
		crashPoint := crashPoint
		t.Run(string(rune('0'+crashPoint)), func(t *testing.T) {
			runtimeDir := shortTaskRuntimeDir(t)
			root := mutationTestRoot(t)
			task := mutationWriteTask(t, root, catalog.MutationID{byte(crashPoint + 2)}, "a", []byte("alpha"))
			secondExpected, err := statMutationPath(t.Context(), root, "b")
			if err != nil {
				t.Fatal(err)
			}
			task.Program.Actions = append(task.Program.Actions, MutationAction{
				Kind: MutationAtomicWriteFile, Path: PathRef{Root: root.ID, Relative: "b"}, Mode: 0o600, Data: []byte("beta"),
			})
			task.Expected = append(task.Expected, ExpectedEffect{
				Path: PathRef{Root: root.ID, Relative: "b"}, Before: physicalState(secondExpected),
				Outcome: MutationPresent, Kind: PhysicalFile,
			})
			payloads := mutationTestPayloads(t, runtimeDir, task)
			defer func() { _ = payloads.Close() }()
			crashed := errors.New("simulated worker crash")
			if _, err := applyMutationTask(t.Context(), runtimeDir, task, payloads, func(index int) error {
				if index == crashPoint {
					return crashed
				}
				return nil
			}); !errors.Is(err, crashed) {
				t.Fatalf("crash result = %v", err)
			}
			executor := &supervisedExecutor{
				runtimeDir: runtimeDir, launcher: &testSourceTaskLauncher{pathSource: &testFullPathSource{}},
				identity: testSourceTaskIdentity(),
			}
			receipt, err := executor.ApplyMutation(t.Context(), task)
			if err != nil {
				t.Fatal(err)
			}
			if len(receipt.Effects) != 2 {
				t.Fatalf("receipt effects = %d", len(receipt.Effects))
			}
			for name, expected := range map[string]string{"a": "alpha", "b": "beta"} {
				got, err := os.ReadFile(filepath.Join(root.Path, name))
				if err != nil || string(got) != expected {
					t.Fatalf("%s = %q, %v", name, got, err)
				}
			}
		})
	}
}

func TestMutationUnprovenExternalIdenticalOutcomeIsRejected(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*testing.T, RootSpec)
		task   func(*testing.T, RootSpec) MutationTask
	}{
		{
			name: "write",
			task: func(t *testing.T, root RootSpec) MutationTask {
				return mutationWriteTask(t, root, catalog.MutationID{20}, "value", []byte("same"))
			},
			mutate: func(t *testing.T, root RootSpec) { writeMutationFixture(t, root, "value", "same") },
		},
		{
			name: "directory",
			task: func(t *testing.T, root RootSpec) MutationTask {
				entry := mustMutationStat(t, root, "value")
				return mutationSimpleTask(root, catalog.MutationID{21}, MutationAction{
					Kind: MutationCreateDirectory, Path: PathRef{Root: root.ID, Relative: "value"}, Mode: 0o700,
				}, entry, MutationPresent, PhysicalDirectory)
			},
			mutate: func(t *testing.T, root RootSpec) {
				if err := os.Mkdir(filepath.Join(root.Path, "value"), 0o700); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlink",
			task: func(t *testing.T, root RootSpec) MutationTask {
				entry := mustMutationStat(t, root, "value")
				return mutationSimpleTask(root, catalog.MutationID{22}, MutationAction{
					Kind: MutationCreateSymlink, Path: PathRef{Root: root.ID, Relative: "value"}, LinkTarget: "target",
				}, entry, MutationPresent, PhysicalSymlink)
			},
			mutate: func(t *testing.T, root RootSpec) {
				if err := os.Symlink("target", filepath.Join(root.Path, "value")); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			runtimeDir := shortTaskRuntimeDir(t)
			root := mutationTestRoot(t)
			task := test.task(t, root)
			payloads := mutationTestPayloads(t, runtimeDir, task)
			defer func() { _ = payloads.Close() }()
			intent, err := mutationIntentFingerprint(task, payloads)
			if err != nil {
				t.Fatal(err)
			}
			if err := storeMutationJournal(t.Context(), runtimeDir, testActiveMutationJournal(task, intent)); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, root)
			if _, err := applyMutationTask(t.Context(), runtimeDir, task, payloads, nil); !errors.Is(err, ErrSourceChanged) {
				t.Fatalf("unproven identical %s result = %v", test.name, err)
			}
		})
	}
	t.Run("remove", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		root := mutationTestRoot(t)
		writeMutationFixture(t, root, "value", "same")
		before := mustMutationStat(t, root, "value")
		task := mutationSimpleTask(root, catalog.MutationID{25}, MutationAction{
			Kind: MutationRemove, Path: PathRef{Root: root.ID, Relative: "value"},
		}, before, MutationAbsent, 0)
		payloads := mutationTestPayloads(t, runtimeDir, task)
		defer func() { _ = payloads.Close() }()
		storeInitialMutationJournal(t, runtimeDir, task, payloads)
		if err := os.Remove(filepath.Join(root.Path, "value")); err != nil {
			t.Fatal(err)
		}
		if _, err := applyMutationTask(t.Context(), runtimeDir, task, payloads, nil); !errors.Is(err, ErrSourceChanged) {
			t.Fatalf("unproven identical remove result = %v", err)
		}
	})
	t.Run("rename", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		root := mutationTestRoot(t)
		writeMutationFixture(t, root, "from", "same")
		from := mustMutationStat(t, root, "from")
		to := mustMutationStat(t, root, "to")
		fromRef := PathRef{Root: root.ID, Relative: "from"}
		toRef := PathRef{Root: root.ID, Relative: "to"}
		task := MutationTask{
			Fence: rootFenceForTest([]RootSpec{root}), Roots: []RootSpec{root}, OperationID: catalog.MutationID{26},
			ExpectationDigest: Fingerprint{1},
			Program:           MutationProgram{Actions: []MutationAction{{Kind: MutationRename, Path: toRef, From: &fromRef}}},
			Expected: []ExpectedEffect{
				{Path: fromRef, Before: physicalState(from), Outcome: MutationAbsent},
				{Path: toRef, Before: physicalState(to), Outcome: MutationPresent, Kind: PhysicalFile},
			},
		}
		payloads := mutationTestPayloads(t, runtimeDir, task)
		defer func() { _ = payloads.Close() }()
		storeInitialMutationJournal(t, runtimeDir, task, payloads)
		if err := os.Rename(filepath.Join(root.Path, "from"), filepath.Join(root.Path, "to")); err != nil {
			t.Fatal(err)
		}
		if _, err := applyMutationTask(t.Context(), runtimeDir, task, payloads, nil); !errors.Is(err, ErrSourceChanged) {
			t.Fatalf("unproven identical rename result = %v", err)
		}
	})
}

func TestMutationJournalRejectsSymlinkedRuntimeAndJournalFile(t *testing.T) {
	t.Parallel()
	parent := canonicalTemporaryDirectory(t)
	target := canonicalTemporaryDirectory(t)
	link := filepath.Join(parent, "runtime-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := validateMutationJournalDirectory(t.Context(), link); err == nil {
		t.Fatal("symlinked runtime directory was accepted")
	}
	runtimeDir := shortTaskRuntimeDir(t)
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	_ = unix.Close(directoryFD)
	operation := catalog.MutationID{23}
	if err := os.Symlink("outside", mutationJournalPath(runtimeDir, operation)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadMutationJournal(t.Context(), runtimeDir, operation); err == nil {
		t.Fatal("symlinked journal file was accepted")
	}
}

func TestChildMutationRejectsMultipleWritersForOnePath(t *testing.T) {
	t.Parallel()
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{27}, "value", []byte("one"))
	task.Program.Actions = append(task.Program.Actions, task.Program.Actions[0])
	task.Program.Actions[1].Data = []byte("two")
	request, _, sizes, err := encodeMutationRequest(task)
	if err != nil {
		t.Fatal(err)
	}
	for index := range task.Program.Actions {
		task.Program.Actions[index].Data = nil
	}
	if err := validateChildMutationTask(task, sizes, request.HasRequestContent); err == nil {
		t.Fatal("multiple writers for one PathRef were accepted")
	}
}

func TestMutationJournalEnforcesRuntimeCountAndByteBounds(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	proof := MutationTerminalProof{
		Authority: "authority", AuthorityGeneration: 1,
		Operation: catalog.MutationID{28}, Outcome: MutationAbandoned,
	}
	first := testTerminalMutationJournal(proof, Fingerprint{1})
	if err := storeMutationJournal(t.Context(), runtimeDir, first); err != nil {
		t.Fatal(err)
	}
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(directoryFD) }()
	payload, exists, err := readMutationJournalAt(
		t.Context(), directoryFD, first.Operation.String()+".json",
	)
	if err != nil || !exists {
		t.Fatalf("read first journal = exists %v err %v", exists, err)
	}
	if err := enforceMutationJournalBounds(t.Context(), directoryFD, first.Operation.String()+".json", int64(len(payload)), 1, int64(len(payload))); err != nil {
		t.Fatalf("exact replacement boundary rejected: %v", err)
	}
	secondName := catalog.MutationID{29}.String() + ".json"
	if err := enforceMutationJournalBounds(t.Context(), directoryFD, secondName, 1, 1, int64(len(payload))+1); err == nil {
		t.Fatal("count boundary admitted a second operation")
	}
	if err := enforceMutationJournalBounds(t.Context(), directoryFD, secondName, 1, 2, int64(len(payload))); err == nil {
		t.Fatal("byte boundary admitted one byte beyond capacity")
	}
	if err := enforceMutationJournalBounds(t.Context(), directoryFD, secondName, 1, 2, int64(len(payload))+1); err != nil {
		t.Fatalf("exact count and byte boundary rejected: %v", err)
	}
}

func TestMutationStartupGCKeepsUnacknowledgedTerminalAndIncompleteJournals(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	incompleteTask := mutationWriteTask(t, root, catalog.MutationID{30}, "incomplete", []byte("value"))
	terminalTask := mutationWriteTask(t, root, catalog.MutationID{31}, "terminal", []byte("value"))
	incomplete := testActiveMutationJournal(incompleteTask, Fingerprint{1})
	terminal := testActiveMutationJournal(terminalTask, Fingerprint{2})
	terminal.Next = 1
	terminal.Receipt = &MutationReceipt{
		OperationID: terminalTask.OperationID, Effects: make([]PhysicalEntry, len(terminalTask.Expected)),
		Digest: Fingerprint{3},
	}
	if err := storeMutationJournal(t.Context(), runtimeDir, incomplete); err != nil {
		t.Fatal(err)
	}
	if err := storeMutationJournal(t.Context(), runtimeDir, terminal); err != nil {
		t.Fatal(err)
	}
	temporary := filepath.Join(
		mutationJournalDirectory(runtimeDir),
		".mutation-00000000000000000000000000000000.tmp",
	)
	if err := os.WriteFile(temporary, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateMutationJournalDirectory(t.Context(), runtimeDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(temporary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("startup temp GC left residue: %v", err)
	}
	for _, operation := range []catalog.MutationID{incomplete.Operation, terminal.Operation} {
		if _, err := os.Stat(mutationJournalPath(runtimeDir, operation)); err != nil {
			t.Fatalf("startup deleted unacknowledged journal %s: %v", operation.String(), err)
		}
	}
	launcher := &testSourceTaskLauncher{pathSource: &testFullPathSource{}}
	executor := &supervisedExecutor{runtimeDir: runtimeDir, launcher: launcher, identity: testSourceTaskIdentity()}
	if err := executor.AcknowledgeMutation(t.Context(), incomplete.Authority, incomplete.AuthorityGeneration, incomplete.Operation, Fingerprint{3}); err == nil {
		t.Fatal("incomplete journal was acknowledged")
	}
	if err := executor.AbandonMutation(t.Context(), terminal.Authority, terminal.AuthorityGeneration, terminal.Operation); err == nil {
		t.Fatal("receipt-bearing journal was abandoned without its digest")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := executor.AbandonMutation(canceled, incomplete.Authority, incomplete.AuthorityGeneration, incomplete.Operation); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled abandonment = %v", err)
	}
	if _, err := os.Stat(mutationJournalPath(runtimeDir, incomplete.Operation)); err != nil {
		t.Fatalf("canceled abandonment removed incomplete journal: %v", err)
	}
	restarted := &supervisedExecutor{runtimeDir: runtimeDir, launcher: launcher, identity: testSourceTaskIdentity()}
	if err := restarted.AbandonMutation(t.Context(), incomplete.Authority, incomplete.AuthorityGeneration, incomplete.Operation); err != nil {
		t.Fatal(err)
	}
	if err := restarted.AbandonMutation(t.Context(), incomplete.Authority, incomplete.AuthorityGeneration, incomplete.Operation); err != nil {
		t.Fatalf("idempotent abandonment after restart = %v", err)
	}
	if _, err := os.Stat(mutationJournalPath(runtimeDir, incomplete.Operation)); err != nil {
		t.Fatalf("abandonment lost its retained terminal proof: %v", err)
	}
	if err := executor.AcknowledgeMutation(t.Context(), terminal.Authority, terminal.AuthorityGeneration, terminal.Operation, terminal.Receipt.Digest); err != nil {
		t.Fatal(err)
	}
	proofs, err := mutationProofsForTest(t, executor, root.Authority)
	if err != nil || len(proofs) != 2 {
		t.Fatalf("retained terminal proofs = %+v, %v", proofs, err)
	}
	for _, proof := range proofs {
		if err := executor.ForgetMutation(t.Context(), root.Authority, proof); err != nil {
			t.Fatal(err)
		}
	}
	for _, operation := range []catalog.MutationID{incomplete.Operation, terminal.Operation} {
		journal, exists, err := loadMutationOperation(t.Context(), runtimeDir, mutationOperationIdentity{
			Authority: root.Authority, AuthorityGeneration: incomplete.AuthorityGeneration, Operation: operation,
		})
		if err != nil || !exists || !journal.Consumed || journal.Terminal == nil {
			t.Fatalf("consumed terminal journal %s = %+v, exists %v, err %v", operation, journal, exists, err)
		}
	}
}

func TestMutationAbandonDeadlineDoesNotBlockOnJournalLock(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{45}, "value", []byte("value"))
	journal := testActiveMutationJournal(task, Fingerprint{1})
	if err := storeMutationJournal(t.Context(), runtimeDir, journal); err != nil {
		t.Fatal(err)
	}
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(directoryFD) }()
	lock, err := lockMutationJournalDirectory(t.Context(), directoryFD)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Close() }()
	deadlines := StandardOperationDeadlines()
	deadlines.Mutation = 50 * time.Millisecond
	launcher := &testSourceTaskLauncher{pathSource: &testFullPathSource{}}
	executor := &supervisedExecutor{
		runtimeDir: runtimeDir, deadlines: deadlines,
		launcher: launcher, identity: testSourceTaskIdentity(),
	}
	started := time.Now()
	if err := executor.AbandonMutation(t.Context(), journal.Authority, journal.AuthorityGeneration, journal.Operation); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("locked abandonment = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("locked abandonment took %s", elapsed)
	}
	if _, err := os.Stat(mutationJournalPath(runtimeDir, journal.Operation)); err != nil {
		t.Fatalf("timed-out abandonment removed journal: %v", err)
	}
	launcher.mu.Lock()
	process := launcher.processes[0]
	launcher.mu.Unlock()
	assertSourceTaskProcessStopped(t, process)
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	executor.deadlines.Mutation = 2 * time.Second
	if err := executor.AbandonMutation(t.Context(), journal.Authority, journal.AuthorityGeneration, journal.Operation); err != nil {
		t.Fatalf("operation lane was not reusable after cleanup deadline: %v", err)
	}
	proofs, err := mutationProofsForTest(t, executor, journal.Authority)
	if err != nil || len(proofs) != 1 || proofs[0].Operation != journal.Operation {
		t.Fatalf("terminal proof after lane reuse = %+v, %v", proofs, err)
	}
}

func TestMutationRenameCrashRecoveryValidatesBothRecordedIdentities(t *testing.T) {
	t.Parallel()
	t.Run("source-staged-before-phase-persist", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		root := mutationTestRoot(t)
		writeMutationFixture(t, root, "from", "new")
		writeMutationFixture(t, root, "to", "old")
		from := mustMutationStat(t, root, "from")
		to := mustMutationStat(t, root, "to")
		fromParent, fromLeaf, err := openMutationParent(root, "from")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = unix.Close(fromParent) }()
		operation := catalog.MutationID{8}
		fromRef := PathRef{Root: root.ID, Relative: "from"}
		toRef := PathRef{Root: root.ID, Relative: "to"}
		task := MutationTask{
			Fence: rootFenceForTest([]RootSpec{root}), Roots: []RootSpec{root}, OperationID: operation,
			ExpectationDigest: Fingerprint{1},
			Program:           MutationProgram{Actions: []MutationAction{{Kind: MutationRename, Path: toRef, From: &fromRef}}},
			Expected: []ExpectedEffect{
				{Path: fromRef, Before: physicalState(from), Outcome: MutationAbsent},
				{Path: toRef, Before: physicalState(to), Outcome: MutationPresent, Kind: PhysicalFile},
			},
		}
		stageName := mutationTemporaryName(fromLeaf, operation, 0)
		if err := mutationRenameNoReplace(fromParent, fromLeaf, fromParent, stageName); err != nil {
			t.Fatal(err)
		}
		journal := testActiveMutationJournal(task, Fingerprint{1})
		journal.Active = &mutationActionProof{
			Index: 0, Kind: MutationRename, Phase: mutationPhasePrepared,
			StageName: stageName, TombName: mutationTombstoneName("to", operation, 0),
			Source: physicalState(from), Target: physicalState(to),
		}
		if err := storeMutationJournal(t.Context(), runtimeDir, journal); err != nil {
			t.Fatal(err)
		}
		if err := executeProvenRename(t.Context(), runtimeDir, root, "from", root, "to",
			physicalState(from), physicalState(to), operation, 0, &journal, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(filepath.Join(root.Path, "from")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("swapped old target was not removed: %v", err)
		}
	})
	t.Run("source-absent-target-is-old", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		root := mutationTestRoot(t)
		writeMutationFixture(t, root, "from", "new")
		writeMutationFixture(t, root, "to", "old")
		from := mustMutationStat(t, root, "from")
		to := mustMutationStat(t, root, "to")
		if err := os.Remove(filepath.Join(root.Path, "from")); err != nil {
			t.Fatal(err)
		}
		journal := mutationJournal{Protocol: mutationJournalProtocol, Operation: catalog.MutationID{7}, Intent: Fingerprint{1}}
		if err := executeProvenRename(t.Context(), runtimeDir, root, "from", root, "to",
			physicalState(from), physicalState(to), journal.Operation, 0, &journal, nil); !errors.Is(err, ErrSourceChanged) {
			t.Fatalf("wrong target identity after missing source = %v", err)
		}
	})
}

func TestMutationPhaseJournalNeverLeadsDurableNamespace(t *testing.T) {
	t.Parallel()
	crash := errors.New("simulated crash after namespace fsync")
	t.Run("create", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		root := mutationTestRoot(t)
		task := mutationWriteTask(t, root, catalog.MutationID{40}, "value", []byte("new"))
		payloads := mutationTestPayloads(t, runtimeDir, task)
		defer func() { _ = payloads.Close() }()
		intent, err := mutationIntentFingerprint(task, payloads)
		if err != nil {
			t.Fatal(err)
		}
		journal := testActiveMutationJournal(task, intent)
		err = executeProvenMutationAction(t.Context(), runtimeDir, task, 0, payloads, &journal,
			func(phase mutationActionPhase) error {
				if phase == mutationPhaseInstalled {
					return crash
				}
				return nil
			})
		if !errors.Is(err, crash) {
			t.Fatalf("create failpoint = %v", err)
		}
		persisted, exists, err := loadMutationJournal(t.Context(), runtimeDir, task.OperationID)
		if err != nil || !exists || persisted.Active == nil || persisted.Active.Phase != mutationPhaseTargetStaged {
			t.Fatalf("create journal led namespace: %+v exists=%v err=%v", persisted, exists, err)
		}
		if err := executeProvenMutationAction(t.Context(), runtimeDir, task, 0, payloads, &persisted, nil); err != nil {
			t.Fatalf("create restart recovery = %v", err)
		}
	})

	t.Run("remove", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		root := mutationTestRoot(t)
		writeMutationFixture(t, root, "value", "old")
		before := mustMutationStat(t, root, "value")
		task := mutationSimpleTask(root, catalog.MutationID{41}, MutationAction{
			Kind: MutationRemove, Path: PathRef{Root: root.ID, Relative: "value"},
		}, before, MutationAbsent, 0)
		payloads := mutationTestPayloads(t, runtimeDir, task)
		defer func() { _ = payloads.Close() }()
		intent, err := mutationIntentFingerprint(task, payloads)
		if err != nil {
			t.Fatal(err)
		}
		journal := testActiveMutationJournal(task, intent)
		err = executeProvenMutationAction(t.Context(), runtimeDir, task, 0, payloads, &journal,
			func(phase mutationActionPhase) error {
				if phase == mutationPhaseTargetStaged {
					return crash
				}
				return nil
			})
		if !errors.Is(err, crash) {
			t.Fatalf("remove failpoint = %v", err)
		}
		persisted, exists, err := loadMutationJournal(t.Context(), runtimeDir, task.OperationID)
		if err != nil || !exists || persisted.Active == nil || persisted.Active.Phase != mutationPhasePrepared {
			t.Fatalf("remove journal led namespace: %+v exists=%v err=%v", persisted, exists, err)
		}
		if err := executeProvenMutationAction(t.Context(), runtimeDir, task, 0, payloads, &persisted, nil); err != nil {
			t.Fatalf("remove restart recovery = %v", err)
		}
	})

	for _, test := range []struct {
		name      string
		crashAt   mutationActionPhase
		persisted mutationActionPhase
	}{
		{"rename-source", mutationPhaseSourceStaged, mutationPhasePrepared},
		{"rename-target", mutationPhaseTargetStaged, mutationPhaseSourceStaged},
		{"rename-install", mutationPhaseInstalled, mutationPhaseTargetStaged},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			runtimeDir := shortTaskRuntimeDir(t)
			root := mutationTestRoot(t)
			writeMutationFixture(t, root, "from", "new")
			writeMutationFixture(t, root, "to", "old")
			from := mustMutationStat(t, root, "from")
			to := mustMutationStat(t, root, "to")
			fromRef := PathRef{Root: root.ID, Relative: "from"}
			toRef := PathRef{Root: root.ID, Relative: "to"}
			task := MutationTask{
				Fence: rootFenceForTest([]RootSpec{root}), Roots: []RootSpec{root}, OperationID: catalog.MutationID{42, byte(test.crashAt)},
				ExpectationDigest: Fingerprint{1},
				Program:           MutationProgram{Actions: []MutationAction{{Kind: MutationRename, Path: toRef, From: &fromRef}}},
				Expected: []ExpectedEffect{
					{Path: fromRef, Before: physicalState(from), Outcome: MutationAbsent},
					{Path: toRef, Before: physicalState(to), Outcome: MutationPresent, Kind: PhysicalFile},
				},
			}
			payloads := mutationTestPayloads(t, runtimeDir, task)
			defer func() { _ = payloads.Close() }()
			intent, err := mutationIntentFingerprint(task, payloads)
			if err != nil {
				t.Fatal(err)
			}
			journal := testActiveMutationJournal(task, intent)
			err = executeProvenMutationAction(t.Context(), runtimeDir, task, 0, payloads, &journal,
				func(phase mutationActionPhase) error {
					if phase == test.crashAt {
						return crash
					}
					return nil
				})
			if !errors.Is(err, crash) {
				t.Fatalf("rename failpoint = %v", err)
			}
			persisted, exists, err := loadMutationJournal(t.Context(), runtimeDir, task.OperationID)
			if err != nil || !exists || persisted.Active == nil || persisted.Active.Phase != test.persisted {
				t.Fatalf("rename journal led namespace: %+v exists=%v err=%v", persisted, exists, err)
			}
			if err := executeProvenMutationAction(t.Context(), runtimeDir, task, 0, payloads, &persisted, nil); err != nil {
				t.Fatalf("rename restart recovery = %v", err)
			}
		})
	}
}

func TestMutationRecoveryRejectsExternalMetadataDrift(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	writeMutationFixture(t, root, "from", "new")
	writeMutationFixture(t, root, "to", "old")
	from := mustMutationStat(t, root, "from")
	to := mustMutationStat(t, root, "to")
	parent, leaf, err := openMutationParent(root, "from")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(parent) }()
	operation := catalog.MutationID{43}
	fromRef := PathRef{Root: root.ID, Relative: "from"}
	toRef := PathRef{Root: root.ID, Relative: "to"}
	task := MutationTask{
		Fence: rootFenceForTest([]RootSpec{root}), Roots: []RootSpec{root}, OperationID: operation,
		ExpectationDigest: Fingerprint{1},
		Program:           MutationProgram{Actions: []MutationAction{{Kind: MutationRename, Path: toRef, From: &fromRef}}},
		Expected: []ExpectedEffect{
			{Path: fromRef, Before: physicalState(from), Outcome: MutationAbsent},
			{Path: toRef, Before: physicalState(to), Outcome: MutationPresent, Kind: PhysicalFile},
		},
	}
	stageName := mutationTemporaryName(leaf, operation, 0)
	if err := mutationRenameNoReplace(parent, leaf, parent, stageName); err != nil {
		t.Fatal(err)
	}
	if err := unix.Fsync(parent); err != nil {
		t.Fatal(err)
	}
	if err := unix.Fchmodat(parent, stageName, 0o640, 0); err != nil {
		t.Fatal(err)
	}
	journal := testActiveMutationJournal(task, Fingerprint{1})
	journal.Active = &mutationActionProof{
		Index: 0, Kind: MutationRename, Phase: mutationPhasePrepared,
		StageName: stageName, TombName: mutationTombstoneName("to", operation, 0),
		Source: physicalState(from), Target: physicalState(to),
	}
	if err := storeMutationJournal(t.Context(), runtimeDir, journal); err != nil {
		t.Fatal(err)
	}
	if err := executeProvenRename(t.Context(), runtimeDir, root, "from", root, "to",
		physicalState(from), physicalState(to), operation, 0, &journal, nil); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("external chmod after namespace rename = %v", err)
	}
}

func TestMutationRecoveryProofRejectsExternalOwnershipDrift(t *testing.T) {
	t.Parallel()
	entry := PhysicalEntry{
		Exists: true, Kind: PhysicalFile,
		Identity: FileIdentity{VolumeUUID: "volume", Inode: 1, BirthtimeSec: 2},
		Mode:     0o100600, UID: 501, GID: 20, Size: 7, ContentFingerprint: Fingerprint{1},
	}
	proof := physicalState(entry)
	entry.GID++
	if mutationProofMatches(entry, proof) {
		t.Fatal("recovery proof accepted external chown metadata drift")
	}
}

func TestMutationRejectsSymlinkAncestorWithoutTouchingOutside(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	outside := canonicalTemporaryDirectory(t)
	if err := os.Symlink(outside, filepath.Join(root.Path, "link")); err != nil {
		t.Fatal(err)
	}
	task := MutationTask{
		Fence: rootFenceForTest([]RootSpec{root}), Roots: []RootSpec{root}, OperationID: catalog.MutationID{9},
		ExpectationDigest: Fingerprint{1},
		Program: MutationProgram{Actions: []MutationAction{{
			Kind: MutationAtomicWriteFile, Path: PathRef{Root: root.ID, Relative: "link/value"}, Mode: 0o600, Data: []byte("escape"),
		}}},
		Expected: []ExpectedEffect{{
			Path: PathRef{Root: root.ID, Relative: "link/value"}, Outcome: MutationPresent, Kind: PhysicalFile,
		}},
	}
	payloads := mutationTestPayloads(t, runtimeDir, task)
	defer func() { _ = payloads.Close() }()
	if _, err := applyMutationTask(t.Context(), runtimeDir, task, payloads, nil); err == nil {
		t.Fatal("symlink ancestor mutation was accepted")
	}
	if _, err := os.Stat(filepath.Join(outside, "value")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outside target was touched: %v", err)
	}
}

func TestMutationRejectsReplacedDirectoryRootBeforeWritingReplacement(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{72}, "value", []byte("new"))
	detached := root.Path + "-detached"
	if err := os.Rename(root.Path, detached); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root.Path, 0o700); err != nil {
		t.Fatal(err)
	}
	payloads := mutationTestPayloads(t, runtimeDir, task)
	defer func() { _ = payloads.Close() }()
	if _, err := applyMutationTask(t.Context(), runtimeDir, task, payloads, nil); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("replaced root mutation error = %v, want ErrSourceChanged", err)
	}
	if _, err := os.Stat(filepath.Join(root.Path, "value")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement root was written: %v", err)
	}
	if _, err := os.Stat(mutationJournalPath(runtimeDir, task.OperationID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("root-fence rejection created a mutation journal: %v", err)
	}
}

func TestMutationRejectsAuthorityRootObjectsBeforeChildMutation(t *testing.T) {
	t.Parallel()
	launcher := &testSourceTaskLauncher{pathSource: &testFullPathSource{}}
	t.Run("exact-file-root", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		file := filepath.Join(canonicalTemporaryDirectory(t), "source.json")
		if err := os.WriteFile(file, []byte("before"), 0o600); err != nil {
			t.Fatal(err)
		}
		root := testPinnedRoot(t, RootSpec{
			Authority: "authority", ID: "file-root", Path: file, Kind: RootFile, Generation: 1,
		})
		entry, err := (securePathSource{}).Stat(t.Context(), root, ".")
		if err != nil {
			t.Fatal(err)
		}
		task := MutationTask{
			Fence: rootFenceForTest([]RootSpec{root}), Roots: []RootSpec{root}, OperationID: catalog.MutationID{73},
			ExpectationDigest: Fingerprint{1},
			Program: MutationProgram{Actions: []MutationAction{{
				Kind: MutationAtomicWriteFile, Path: PathRef{Root: root.ID, Relative: "."}, Mode: 0o600, Data: []byte("after"),
			}}},
			Expected: []ExpectedEffect{{
				Path: PathRef{Root: root.ID, Relative: "."}, Before: physicalState(entry),
				Outcome: MutationPresent, Kind: PhysicalFile,
			}},
		}
		executor := &supervisedExecutor{runtimeDir: runtimeDir, launcher: launcher, identity: testSourceTaskIdentity()}
		if _, err := executor.ApplyMutation(t.Context(), task); err == nil {
			t.Fatal("exact-file authority root was mutable")
		}
		got, err := os.ReadFile(file)
		if err != nil || string(got) != "before" {
			t.Fatalf("exact-file root changed to %q, %v", got, err)
		}
		identity, err := (securePathSource{}).RootIdentity(t.Context(), RootSpec{
			Authority: root.Authority, ID: root.ID, Path: root.Path, Kind: root.Kind, Generation: root.Generation,
		})
		if err != nil || identity != root.ExpectedIdentity {
			t.Fatalf("exact-file root identity = %+v, %v", identity, err)
		}
	})
	t.Run("directory-root-itself", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		root := mutationTestRoot(t)
		entry, err := (securePathSource{}).Stat(t.Context(), root, ".")
		if err != nil {
			t.Fatal(err)
		}
		task := MutationTask{
			Fence: rootFenceForTest([]RootSpec{root}), Roots: []RootSpec{root}, OperationID: catalog.MutationID{74},
			ExpectationDigest: Fingerprint{1},
			Program: MutationProgram{Actions: []MutationAction{{
				Kind: MutationRemove, Path: PathRef{Root: root.ID, Relative: "."},
			}}},
			Expected: []ExpectedEffect{{
				Path: PathRef{Root: root.ID, Relative: "."}, Before: physicalState(entry), Outcome: MutationAbsent,
			}},
		}
		executor := &supervisedExecutor{runtimeDir: runtimeDir, launcher: launcher, identity: testSourceTaskIdentity()}
		if _, err := executor.ApplyMutation(t.Context(), task); err == nil {
			t.Fatal("directory authority root itself was mutable")
		}
		if err := validateRootPathStillPinned(root); err != nil {
			t.Fatalf("directory root identity changed: %v", err)
		}
	})
}

func mutationTestRoot(t *testing.T) RootSpec {
	t.Helper()
	return testPinnedRoot(t, RootSpec{
		Authority: "authority", ID: "root", Path: canonicalTemporaryDirectory(t), Kind: RootDirectory, Generation: 1,
	})
}

func mutationOperationIdentityForTask(task MutationTask) mutationOperationIdentity {
	return mutationOperationIdentity{
		Authority: task.Fence.Authority, AuthorityGeneration: task.Fence.AuthorityGeneration, Operation: task.OperationID,
	}
}

func mutationProofsForTest(
	t *testing.T,
	executor *supervisedExecutor,
	authority causal.SourceAuthorityID,
) ([]MutationTerminalProof, error) {
	t.Helper()
	var after catalog.MutationID
	var proofs []MutationTerminalProof
	for {
		page, err := executor.MutationTerminalProofPage(
			t.Context(), authority, after, MutationTerminalProofPageLimit,
		)
		if err != nil {
			return nil, err
		}
		proofs = append(proofs, page.Proofs...)
		if !page.More {
			return proofs, nil
		}
		if page.Next == (catalog.MutationID{}) || page.Next.String() <= after.String() {
			return nil, errors.New("mutation proof test cursor did not advance")
		}
		after = page.Next
	}
}

func mutationWriteTask(t *testing.T, root RootSpec, operation catalog.MutationID, name string, data []byte) MutationTask {
	t.Helper()
	entry, err := statMutationPath(t.Context(), root, name)
	if err != nil {
		t.Fatal(err)
	}
	return MutationTask{
		Fence: rootFenceForTest([]RootSpec{root}), Roots: []RootSpec{root}, OperationID: operation,
		ExpectationDigest: Fingerprint{1},
		Program: MutationProgram{Actions: []MutationAction{{
			Kind: MutationAtomicWriteFile, Path: PathRef{Root: root.ID, Relative: name}, Mode: 0o600, Data: append([]byte(nil), data...),
		}}},
		Expected: []ExpectedEffect{{
			Path: PathRef{Root: root.ID, Relative: name}, Before: physicalState(entry), Outcome: MutationPresent, Kind: PhysicalFile,
		}},
	}
}

func mutationSimpleTask(
	root RootSpec,
	operation catalog.MutationID,
	action MutationAction,
	before PhysicalEntry,
	outcome MutationOutcome,
	kind PhysicalKind,
) MutationTask {
	return MutationTask{
		Fence: rootFenceForTest([]RootSpec{root}), Roots: []RootSpec{root}, OperationID: operation,
		ExpectationDigest: Fingerprint{1},
		Program:           MutationProgram{Actions: []MutationAction{action}},
		Expected:          []ExpectedEffect{{Path: action.Path, Before: physicalState(before), Outcome: outcome, Kind: kind}},
	}
}

func mutationTestPayloads(t *testing.T, runtimeDir string, task MutationTask) *mutationPayloadSet {
	t.Helper()
	set := &mutationPayloadSet{actions: make([]*mutationPayload, len(task.Program.Actions))}
	for index, action := range task.Program.Actions {
		if len(action.Data) == 0 {
			continue
		}
		file, err := os.CreateTemp(runtimeDir, "mutation-test-")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(file.Name()); err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(action.Data); err != nil {
			t.Fatal(err)
		}
		set.actions[index] = &mutationPayload{file: file, size: int64(len(action.Data)), hash: sha256.Sum256(action.Data)}
	}
	return set
}

func testActiveMutationJournal(task MutationTask, intent Fingerprint) mutationJournal {
	return mutationJournal{
		Protocol: mutationJournalProtocol, Authority: task.Fence.Authority,
		AuthorityGeneration: task.Fence.AuthorityGeneration,
		Operation:           task.OperationID, ExpectationDigest: task.ExpectationDigest,
		Intent: intent, Task: &task,
	}
}

func testTerminalMutationJournal(proof MutationTerminalProof, intent Fingerprint) mutationJournal {
	return mutationJournal{
		Protocol: mutationJournalProtocol, Authority: proof.Authority,
		AuthorityGeneration: proof.AuthorityGeneration,
		Operation:           proof.Operation, ExpectationDigest: Fingerprint{1},
		Intent: intent, Terminal: &proof,
	}
}

func storeInitialMutationJournal(t *testing.T, runtimeDir string, task MutationTask, payloads *mutationPayloadSet) {
	t.Helper()
	intent, err := mutationIntentFingerprint(task, payloads)
	if err != nil {
		t.Fatal(err)
	}
	if err := storeMutationJournal(t.Context(), runtimeDir, testActiveMutationJournal(task, intent)); err != nil {
		t.Fatal(err)
	}
}

func writeMutationFixture(t *testing.T, root RootSpec, name, value string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root.Path, name), []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustMutationStat(t *testing.T, root RootSpec, name string) PhysicalEntry {
	t.Helper()
	entry, err := statMutationPath(t.Context(), root, name)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}
