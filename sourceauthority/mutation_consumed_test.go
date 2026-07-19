package sourceauthority

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/catalog"
)

func TestConsumedMutationLedgerDoesNotExhaustActiveCapacity(t *testing.T) {
	runtimeDir := shortTaskRuntimeDir(t)
	for index := 0; index <= maxMutationJournals; index++ {
		operation := mutationJournalPagingOperation(index + maxMutationJournals)
		proof := MutationTerminalProof{
			Authority: "capacity-authority", AuthorityGeneration: 1, Operation: operation,
			Outcome: MutationAbandoned,
		}
		writeConsumedMutationFixture(t, runtimeDir, mutationJournal{
			Protocol: mutationJournalProtocol, Authority: proof.Authority, AuthorityGeneration: proof.AuthorityGeneration,
			Operation: operation, Intent: sha256.Sum256(operation[:]), Terminal: &proof, Consumed: true,
		})
	}
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{0xff, 0xee}, "fresh", []byte("admitted"))
	payloads := mutationTestPayloads(t, runtimeDir, task)
	defer func() { _ = payloads.Close() }()
	if _, err := applyMutationTask(t.Context(), runtimeDir, task, payloads, nil); err != nil {
		t.Fatalf("fresh active mutation after %d consumed identities = %v", maxMutationJournals+1, err)
	}
}

func TestConsumedMutationAddressRejectsCorruptAndInsecureEntriesBeforeIO(t *testing.T) {
	for _, mode := range []string{"corrupt", "insecure", "symlink"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			runtimeDir := shortTaskRuntimeDir(t)
			root := mutationTestRoot(t)
			task := mutationWriteTask(t, root, catalog.MutationID{0xc0, byte(len(mode))}, "value", []byte("blocked"))
			payloads := mutationTestPayloads(t, runtimeDir, task)
			defer func() { _ = payloads.Close() }()
			intent, err := mutationIntentFingerprint(task, payloads)
			if err != nil {
				t.Fatal(err)
			}
			proof := MutationTerminalProof{
				Authority: task.Fence.Authority, AuthorityGeneration: task.Fence.AuthorityGeneration,
				Operation: task.OperationID, Outcome: MutationAbandoned,
			}
			journal := mutationJournal{
				Protocol: mutationJournalProtocol, Authority: proof.Authority,
				AuthorityGeneration: proof.AuthorityGeneration, Operation: proof.Operation,
				Intent: intent, Terminal: &proof, Consumed: true,
			}
			path := consumedMutationFixturePath(t, runtimeDir, journal)
			payload, err := json.Marshal(journal)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "corrupt":
				if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "insecure":
				if err := os.WriteFile(path, payload, 0o644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				external := filepath.Join(t.TempDir(), "external.json")
				if err := os.WriteFile(external, payload, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(external, path); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Rename(root.Path, root.Path+"-detached"); err != nil {
				t.Fatal(err)
			}
			if _, err := applyMutationTask(t.Context(), runtimeDir, task, payloads, func(int) error {
				return errors.New("corrupt consumed operation executed")
			}); err == nil || errors.Is(err, ErrSourceChanged) {
				t.Fatalf("%s consumed address = %v", mode, err)
			}
		})
	}
}

func TestConsumedMutationOperationIDCannotReenterInNewGeneration(t *testing.T) {
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{0xd1}, "value", []byte("durable"))
	consumeMutationTask(t, runtimeDir, task)
	replayed := task
	replayed.Fence.AuthorityGeneration++
	payloads := mutationTestPayloads(t, runtimeDir, replayed)
	defer func() { _ = payloads.Close() }()
	if err := os.Rename(root.Path, root.Path+"-detached"); err != nil {
		t.Fatal(err)
	}
	_, err := applyMutationTask(t.Context(), runtimeDir, replayed, payloads, func(int) error {
		return errors.New("cross-generation consumed operation executed")
	})
	if !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("cross-generation operation reuse = %v, want mutation conflict", err)
	}
}

func TestInspectMutationIsExactAndDoesNotProbeSourceRoots(t *testing.T) {
	runtimeDir := shortTaskRuntimeDir(t)
	root := mutationTestRoot(t)
	task := mutationWriteTask(t, root, catalog.MutationID{0xd0}, "value", []byte("pending"))
	payloads := mutationTestPayloads(t, runtimeDir, task)
	defer func() { _ = payloads.Close() }()
	storeInitialMutationJournal(t, runtimeDir, task, payloads)
	if err := os.Rename(root.Path, root.Path+"-detached"); err != nil {
		t.Fatal(err)
	}
	request := MutationInspectionRequest{
		Authority: task.Fence.Authority, AuthorityGeneration: task.Fence.AuthorityGeneration,
		Operation: task.OperationID, ExpectationDigest: task.ExpectationDigest,
	}
	inspection, err := inspectMutationOperation(t.Context(), runtimeDir, request)
	if err != nil || inspection.State != MutationInspectionActiveUnapplied ||
		inspection.ExpectationDigest != task.ExpectationDigest || inspection.Intent == (Fingerprint{}) ||
		inspection.Receipt != nil || inspection.Terminal != nil {
		t.Fatalf("active mutation inspection = %+v, %v", inspection, err)
	}
	wrongDigest := request
	wrongDigest.ExpectationDigest[0] ^= 0xff
	if _, err := inspectMutationOperation(t.Context(), runtimeDir, wrongDigest); !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("expectation mismatch = %v, want mutation conflict", err)
	}
	wrongGeneration := request
	wrongGeneration.AuthorityGeneration++
	if _, err := inspectMutationOperation(t.Context(), runtimeDir, wrongGeneration); !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("generation mismatch = %v, want mutation conflict", err)
	}
}

func TestActiveMutationIdentityMismatchFailsBeforeFilesystemProbe(t *testing.T) {
	for _, mutate := range []struct {
		name string
		edit func(*MutationTask)
	}{
		{"authority", func(task *MutationTask) { task.Fence.Authority = "other-authority" }},
		{"generation", func(task *MutationTask) { task.Fence.AuthorityGeneration++ }},
	} {
		mutate := mutate
		t.Run(mutate.name, func(t *testing.T) {
			runtimeDir := shortTaskRuntimeDir(t)
			root := mutationTestRoot(t)
			task := mutationWriteTask(t, root, catalog.MutationID{0xd2, byte(len(mutate.name))}, "value", []byte("blocked"))
			initialPayloads := mutationTestPayloads(t, runtimeDir, task)
			storeInitialMutationJournal(t, runtimeDir, task, initialPayloads)
			if err := initialPayloads.Close(); err != nil {
				t.Fatal(err)
			}
			replayed := task
			mutate.edit(&replayed)
			payloads := mutationTestPayloads(t, runtimeDir, replayed)
			defer func() { _ = payloads.Close() }()
			if err := os.Rename(root.Path, root.Path+"-detached"); err != nil {
				t.Fatal(err)
			}
			_, err := applyMutationTask(t.Context(), runtimeDir, replayed, payloads, func(int) error {
				return errors.New("identity-mismatched active operation executed")
			})
			if !errors.Is(err, catalog.ErrMutationConflict) {
				t.Fatalf("active %s mismatch = %v, want mutation conflict", mutate.name, err)
			}
		})
	}
}

func TestMutationAdmissionLossReloadsAndClassifiesExactState(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		conflict := conflict
		t.Run(map[bool]string{false: "exact", true: "conflict"}[conflict], func(t *testing.T) {
			runtimeDir := shortTaskRuntimeDir(t)
			root := mutationTestRoot(t)
			task := mutationWriteTask(t, root, catalog.MutationID{0xd3, byte(1 + boolByte(conflict))}, "value", []byte("blocked"))
			payloads := mutationTestPayloads(t, runtimeDir, task)
			defer func() { _ = payloads.Close() }()
			_, err := applyMutationTaskWithAdmissionHook(
				t.Context(), runtimeDir, task, payloads,
				func(int) error { return errors.New("admission-losing operation executed") },
				func(raced mutationJournal) error {
					if conflict {
						raced.Intent[0] ^= 0xff
					}
					created, err := createMutationJournal(t.Context(), runtimeDir, raced)
					if err != nil || !created {
						return errors.Join(errors.New("install raced admission"), err)
					}
					return os.Rename(root.Path, root.Path+"-detached")
				},
			)
			if conflict && !errors.Is(err, catalog.ErrMutationConflict) {
				t.Fatalf("conflicting admission loss = %v, want mutation conflict", err)
			}
			if !conflict && !errors.Is(err, errMutationInProgress) {
				t.Fatalf("exact admission loss = %v, want in progress", err)
			}
		})
	}
}

func boolByte(value bool) byte {
	if value {
		return 1
	}
	return 0
}

func writeConsumedMutationFixture(t *testing.T, runtimeDir string, journal mutationJournal) {
	t.Helper()
	if journal.ExpectationDigest == (Fingerprint{}) {
		journal.ExpectationDigest = Fingerprint{1}
	}
	path := consumedMutationFixturePath(t, runtimeDir, journal)
	payload, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}

func consumedMutationFixturePath(t *testing.T, runtimeDir string, journal mutationJournal) string {
	t.Helper()
	identity := mutationOperationIdentity{
		Authority: journal.Authority, AuthorityGeneration: journal.AuthorityGeneration, Operation: journal.Operation,
	}
	shard, name := mutationConsumedAddress(identity)
	directory := filepath.Join(runtimeDir, "source-mutations-consumed", shard)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(directory, name)
}
