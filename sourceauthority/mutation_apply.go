package sourceauthority

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"golang.org/x/sys/unix"
)

const (
	mutationJournalProtocol       = uint16(1)
	maxMutationJournals           = 10_000
	maxMutationJournalBytes       = 1 << 30
	mutationJournalLockWait       = 30 * time.Second
	mutationJournalPageEntryLimit = 128
	mutationJournalPageByteLimit  = 4 << 10
	mutationJournalNameByteLimit  = 69
	mutationJournalReadBatchLimit = 16
)

var errMutationJournalDirectoryChanged = errors.New(
	"sourceauthority: mutation journal directory changed during bounded scan",
)

var errMutationJournalDirectoryEntryBound = errors.New(
	"sourceauthority: mutation journal directory exceeds its bounded entry scan",
)

var errMutationConsumed = errors.New("sourceauthority: mutation operation is durably consumed")

var errMutationInProgress = errors.New("sourceauthority: mutation operation is already in progress")

type mutationPayloadDigest struct {
	Size int64
	Hash [32]byte
}

type mutationIntent struct {
	Task    MutationTask
	Actions []mutationPayloadDigest
	Request *mutationPayloadDigest
}

type mutationJournal struct {
	Protocol            uint16
	Authority           causal.SourceAuthorityID
	AuthorityGeneration causal.Generation
	Operation           catalog.MutationID
	ExpectationDigest   Fingerprint
	Intent              Fingerprint
	Request             *mutationPayloadDigest
	Task                *MutationTask
	Next                int
	Active              *mutationActionProof
	Receipt             *MutationReceipt
	Terminal            *MutationTerminalProof
	Consumed            bool
}

type mutationActionPhase uint8

const (
	mutationPhasePrepared mutationActionPhase = iota + 1
	mutationPhaseSourceStaged
	mutationPhaseTargetStaged
	mutationPhaseInstalled
)

type mutationActionProof struct {
	Index     int
	Kind      MutationActionKind
	Phase     mutationActionPhase
	StageName string
	TombName  string
	Stage     ExpectedPhysicalState
	Source    ExpectedPhysicalState
	Target    ExpectedPhysicalState
}

type mutationDurabilityFailpoint func(mutationActionPhase) error

func applyMutationTask(
	ctx context.Context,
	runtimeDir string,
	task MutationTask,
	payloads *mutationPayloadSet,
	afterAction func(int) error,
) (MutationReceipt, error) {
	return applyMutationTaskWithAdmissionHook(ctx, runtimeDir, task, payloads, afterAction, nil)
}

func applyMutationTaskWithAdmissionHook(
	ctx context.Context,
	runtimeDir string,
	task MutationTask,
	payloads *mutationPayloadSet,
	afterAction func(int) error,
	beforeCreate func(mutationJournal) error,
) (MutationReceipt, error) {
	intent, err := mutationIntentFingerprint(task, payloads)
	if err != nil {
		return MutationReceipt{}, err
	}
	requestDigest := mutationRequestDigest(payloads)
	identity := mutationOperationIdentity{
		Authority: task.Fence.Authority, AuthorityGeneration: task.Fence.AuthorityGeneration,
		Operation: task.OperationID,
	}
	var journal mutationJournal
	var rootsValidated, lostAdmission bool
	for {
		var exists bool
		journal, exists, err = loadMutationOperation(ctx, runtimeDir, identity)
		if err != nil {
			return MutationReceipt{}, err
		}
		if !exists {
			if err := validateTaskRootFence(task.Fence, task.Roots); err != nil {
				return MutationReceipt{}, err
			}
			if err := validateMutationRootPaths(task.Roots); err != nil {
				return MutationReceipt{}, err
			}
			if err := validateMutationPreconditions(ctx, task); err != nil {
				return MutationReceipt{}, err
			}
			rootsValidated = true
			journalTask := task
			journalTask.Content = nil
			journal = mutationJournal{
				Protocol: mutationJournalProtocol, Authority: task.Fence.Authority,
				AuthorityGeneration: task.Fence.AuthorityGeneration,
				Operation:           task.OperationID, ExpectationDigest: task.ExpectationDigest,
				Intent: intent, Request: requestDigest, Task: &journalTask,
			}
			if beforeCreate != nil {
				if err := beforeCreate(journal); err != nil {
					return MutationReceipt{}, err
				}
				beforeCreate = nil
			}
			created, err := createMutationJournal(ctx, runtimeDir, journal)
			if err != nil {
				return MutationReceipt{}, err
			}
			if created {
				break
			}
			lostAdmission = true
			rootsValidated = false
			continue
		}
		if err := classifyMutationJournal(task, intent, requestDigest, journal); err != nil {
			return MutationReceipt{}, err
		}
		if journal.Consumed {
			return MutationReceipt{}, errMutationConsumed
		}
		if journal.Terminal != nil {
			return MutationReceipt{}, errors.New("sourceauthority: mutation operation is already terminal")
		}
		if lostAdmission && journal.Receipt == nil {
			return MutationReceipt{}, errMutationInProgress
		}
		break
	}
	if !rootsValidated {
		if err := validateTaskRootFence(task.Fence, task.Roots); err != nil {
			return MutationReceipt{}, err
		}
		if err := validateMutationRootPaths(task.Roots); err != nil {
			return MutationReceipt{}, err
		}
	}
	if journal.Receipt != nil {
		if err := validateMutationReceiptState(ctx, task, *journal.Receipt); err != nil {
			return MutationReceipt{}, err
		}
		return *journal.Receipt, nil
	}
	for index := journal.Next; index < len(task.Program.Actions); index++ {
		if err := executeProvenMutationAction(ctx, runtimeDir, task, index, payloads, &journal, nil); err != nil {
			return MutationReceipt{}, err
		}
		if afterAction != nil {
			if err := afterAction(index); err != nil {
				return MutationReceipt{}, err
			}
		}
		if err := validateMutationRootPaths(task.Roots); err != nil {
			return MutationReceipt{}, err
		}
		journal.Active = nil
		journal.Next = index + 1
		if err := storeMutationJournalDurably(ctx, runtimeDir, journal); err != nil {
			return MutationReceipt{}, err
		}
	}
	receipt, err := collectMutationReceipt(ctx, task)
	if err != nil {
		return MutationReceipt{}, err
	}
	journal.Receipt = &receipt
	if err := storeMutationJournalDurably(ctx, runtimeDir, journal); err != nil {
		return MutationReceipt{}, err
	}
	return receipt, nil
}

func classifyMutationJournal(
	task MutationTask,
	intent Fingerprint,
	request *mutationPayloadDigest,
	journal mutationJournal,
) error {
	if journal.Protocol != mutationJournalProtocol || journal.Operation != task.OperationID ||
		journal.Authority != task.Fence.Authority ||
		journal.AuthorityGeneration != task.Fence.AuthorityGeneration ||
		journal.ExpectationDigest != task.ExpectationDigest || journal.Intent != intent ||
		!sameMutationPayloadDigest(journal.Request, request) ||
		journal.Next < 0 || journal.Task != nil && journal.Next > len(task.Program.Actions) ||
		(journal.Active != nil && journal.Active.Index != journal.Next) {
		return fmt.Errorf("%w: mutation operation does not match exact request identity", catalog.ErrMutationConflict)
	}
	return nil
}

func validateTaskRootFence(fence Fence, roots []RootSpec) error {
	if fence.Authority == "" || fence.AuthorityGeneration == 0 || fence.RootDigest == (Fingerprint{}) || len(roots) == 0 {
		return errors.New("sourceauthority: source task is missing its root fence")
	}
	observed := make([]observedRoot, len(roots))
	for index, root := range roots {
		if root.Authority != fence.Authority {
			return errors.New("sourceauthority: source task root escaped its authority fence")
		}
		if err := validatePinnedTaskRoot(root); err != nil {
			return err
		}
		declared := root
		declared.ExpectedIdentity = FileIdentity{}
		observed[index] = observedRoot{Spec: declared, Identity: root.ExpectedIdentity}
	}
	slices.SortFunc(observed, func(left, right observedRoot) int {
		return compareString(string(left.Spec.ID), string(right.Spec.ID))
	})
	digest, err := digestJSON(observed)
	if err != nil {
		return err
	}
	if digest != fence.RootDigest {
		return ErrSourceChanged
	}
	return nil
}

func validateMutationRootPaths(roots []RootSpec) error {
	for _, root := range roots {
		if err := validateRootPathStillPinned(root); err != nil {
			return err
		}
	}
	return nil
}

func mutationIntentFingerprint(task MutationTask, payloads *mutationPayloadSet) (Fingerprint, error) {
	copyTask := task
	copyTask.Content = nil
	copyTask.Program.Actions = append([]MutationAction(nil), task.Program.Actions...)
	actions := make([]mutationPayloadDigest, len(copyTask.Program.Actions))
	for index := range copyTask.Program.Actions {
		copyTask.Program.Actions[index].Data = nil
		if payloads.actions[index] != nil {
			actions[index] = mutationPayloadDigest{Size: payloads.actions[index].size, Hash: payloads.actions[index].hash}
		}
	}
	var request *mutationPayloadDigest
	if payloads.request != nil {
		request = &mutationPayloadDigest{Size: payloads.request.size, Hash: payloads.request.hash}
	}
	payload, err := json.Marshal(mutationIntent{Task: copyTask, Actions: actions, Request: request})
	if err != nil {
		return Fingerprint{}, err
	}
	return sha256.Sum256(payload), nil
}

func mutationRequestDigest(payloads *mutationPayloadSet) *mutationPayloadDigest {
	if payloads.request == nil {
		return nil
	}
	return &mutationPayloadDigest{Size: payloads.request.size, Hash: payloads.request.hash}
}

func sameMutationPayloadDigest(left, right *mutationPayloadDigest) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func validateMutationPreconditions(ctx context.Context, task MutationTask) error {
	roots := mutationRoots(task.Roots)
	for _, effect := range task.Expected {
		root, ok := roots[effect.Path.Root]
		if !ok {
			return errors.New("sourceauthority: mutation effect root is missing")
		}
		entry, err := statMutationPath(ctx, root, effect.Path.Relative)
		if err != nil {
			return err
		}
		if physicalState(entry) != effect.Before {
			return ErrSourceChanged
		}
	}
	return nil
}

func executeProvenMutationAction(
	ctx context.Context,
	runtimeDir string,
	task MutationTask,
	index int,
	payloads *mutationPayloadSet,
	journal *mutationJournal,
	afterSync mutationDurabilityFailpoint,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	action := task.Program.Actions[index]
	roots := mutationRoots(task.Roots)
	effects := mutationEffects(task.Expected)
	targetExpected := effects[action.Path]
	root := roots[action.Path.Root]
	switch action.Kind {
	case MutationAtomicWriteFile:
		payload := payloads.actions[index]
		if action.UseRequestContent {
			payload = payloads.request
		}
		if payload == nil {
			return errors.New("sourceauthority: mutation write has no content")
		}
		return executeProvenCreate(ctx, runtimeDir, root, action.Path.Relative, action, targetExpected.Before,
			payload, task.OperationID, index, journal, afterSync)
	case MutationCreateDirectory:
		return executeProvenCreate(ctx, runtimeDir, root, action.Path.Relative, action, targetExpected.Before,
			nil, task.OperationID, index, journal, afterSync)
	case MutationCreateSymlink:
		return executeProvenCreate(ctx, runtimeDir, root, action.Path.Relative, action, targetExpected.Before,
			nil, task.OperationID, index, journal, afterSync)
	case MutationRemove:
		return executeProvenRemove(ctx, runtimeDir, root, action.Path.Relative, targetExpected.Before,
			task.OperationID, index, journal, afterSync)
	case MutationRename:
		if action.From == nil {
			return errors.New("sourceauthority: rename source is missing")
		}
		return executeProvenRename(ctx, runtimeDir, roots[action.From.Root], action.From.Relative, root, action.Path.Relative,
			effects[*action.From].Before, targetExpected.Before, task.OperationID, index, journal, afterSync)
	default:
		return errors.New("sourceauthority: unsupported mutation action")
	}
}

func executeProvenCreate(
	ctx context.Context,
	runtimeDir string,
	root RootSpec,
	relative string,
	action MutationAction,
	expected ExpectedPhysicalState,
	payload *mutationPayload,
	operation catalog.MutationID,
	index int,
	journal *mutationJournal,
	afterSync mutationDurabilityFailpoint,
) error {
	parent, leaf, err := openMutationParent(root, relative)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(parent) }()
	stageName := mutationTemporaryName(leaf, operation, index)
	tombName := mutationTombstoneName(leaf, operation, index)
	proof := journal.Active
	if proof == nil {
		target, err := statMutationSibling(ctx, root, relative, parent, leaf)
		if err != nil || physicalState(target) != expected {
			return errors.Join(ErrSourceChanged, err)
		}
		if action.Kind != MutationAtomicWriteFile && expected.Exists {
			return errors.New("sourceauthority: create action expected an absent target")
		}
		if err := requireMutationSiblingAbsent(ctx, root, relative, parent, stageName); err != nil {
			return err
		}
		if err := requireMutationSiblingAbsent(ctx, root, relative, parent, tombName); err != nil {
			return err
		}
		switch action.Kind {
		case MutationAtomicWriteFile:
			if payload == nil {
				return errors.New("sourceauthority: mutation write has no payload")
			}
			if err := createMutationStageFile(parent, stageName, action.Mode, payload); err != nil {
				return err
			}
		case MutationCreateDirectory:
			if err := unix.Mkdirat(parent, stageName, action.Mode&0o7777); err != nil {
				return err
			}
			stageFD, err := unix.Openat(parent, stageName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
			if err != nil {
				return err
			}
			err = errors.Join(unix.Fchmod(stageFD, action.Mode&0o7777), unix.Fsync(stageFD), unix.Close(stageFD))
			if err != nil {
				return err
			}
		case MutationCreateSymlink:
			if err := unix.Symlinkat(action.LinkTarget, parent, stageName); err != nil {
				return err
			}
		default:
			return errors.New("sourceauthority: invalid proven create action")
		}
		if err := unix.Fsync(parent); err != nil {
			return err
		}
		stage, err := statMutationSibling(ctx, root, relative, parent, stageName)
		if err != nil || !stage.Exists {
			return errors.Join(ErrSourceChanged, err)
		}
		proof = &mutationActionProof{
			Index: index, Kind: action.Kind, Phase: mutationPhasePrepared,
			StageName: stageName, TombName: tombName, Stage: physicalState(stage), Target: expected,
		}
		journal.Active = proof
		if err := storeMutationJournalDurably(ctx, runtimeDir, *journal); err != nil {
			return err
		}
	}
	if err := validateMutationProof(proof, index, action.Kind, stageName, tombName); err != nil {
		return err
	}
	if proof.Phase == mutationPhasePrepared {
		stage, err := statMutationSibling(ctx, root, relative, parent, stageName)
		if err != nil || physicalState(stage) != proof.Stage {
			return errors.Join(ErrSourceChanged, err)
		}
		target, err := statMutationSibling(ctx, root, relative, parent, leaf)
		if err != nil {
			return err
		}
		tomb, err := statMutationSibling(ctx, root, relative, parent, tombName)
		if err != nil {
			return err
		}
		if proof.Target.Exists {
			switch {
			case physicalState(target) == proof.Target && !tomb.Exists:
				if err := mutationRenameNoReplace(parent, leaf, parent, tombName); err != nil {
					return err
				}
			case !target.Exists && mutationProofMatches(tomb, proof.Target):
			default:
				return ErrSourceChanged
			}
			target, err = statMutationSibling(ctx, root, relative, parent, leaf)
			if err != nil {
				return err
			}
			tomb, err = statMutationSibling(ctx, root, relative, parent, tombName)
			if err != nil || target.Exists || !mutationProofMatches(tomb, proof.Target) {
				return errors.Join(ErrSourceChanged, err)
			}
		} else if target.Exists || tomb.Exists {
			return ErrSourceChanged
		}
		if err := unix.Fsync(parent); err != nil {
			return err
		}
		if afterSync != nil {
			if err := afterSync(mutationPhaseTargetStaged); err != nil {
				return err
			}
		}
		proof.Phase = mutationPhaseTargetStaged
		if err := storeMutationJournalDurably(ctx, runtimeDir, *journal); err != nil {
			return err
		}
	}
	if proof.Phase == mutationPhaseTargetStaged {
		stage, err := statMutationSibling(ctx, root, relative, parent, stageName)
		if err != nil {
			return err
		}
		target, err := statMutationSibling(ctx, root, relative, parent, leaf)
		if err != nil {
			return err
		}
		switch {
		case physicalState(stage) == proof.Stage && !target.Exists:
			if err := mutationRenameNoReplace(parent, stageName, parent, leaf); err != nil {
				return err
			}
		case !stage.Exists && mutationProofMatches(target, proof.Stage):
		default:
			return ErrSourceChanged
		}
		if err := unix.Fsync(parent); err != nil {
			return err
		}
		if afterSync != nil {
			if err := afterSync(mutationPhaseInstalled); err != nil {
				return err
			}
		}
		stage, err = statMutationSibling(ctx, root, relative, parent, stageName)
		if err != nil {
			return err
		}
		target, err = statMutationSibling(ctx, root, relative, parent, leaf)
		if err != nil || stage.Exists || !mutationProofMatches(target, proof.Stage) {
			return errors.Join(ErrSourceChanged, err)
		}
		proof.Phase = mutationPhaseInstalled
		if err := storeMutationJournalDurably(ctx, runtimeDir, *journal); err != nil {
			return err
		}
	}
	if proof.Phase != mutationPhaseInstalled {
		return errors.New("sourceauthority: invalid create mutation proof phase")
	}
	target, err := statMutationSibling(ctx, root, relative, parent, leaf)
	if err != nil || !mutationProofMatches(target, proof.Stage) {
		return errors.Join(ErrSourceChanged, err)
	}
	if proof.Target.Exists {
		tomb, err := statMutationSibling(ctx, root, relative, parent, tombName)
		if err != nil {
			return err
		}
		if tomb.Exists {
			if !mutationProofMatches(tomb, proof.Target) {
				return ErrSourceChanged
			}
			if err := removeMutationLeaf(parent, tombName, tomb.Kind); err != nil {
				return err
			}
		}
	}
	return unix.Fsync(parent)
}

func executeProvenRemove(
	ctx context.Context,
	runtimeDir string,
	root RootSpec,
	relative string,
	expected ExpectedPhysicalState,
	operation catalog.MutationID,
	index int,
	journal *mutationJournal,
	afterSync mutationDurabilityFailpoint,
) error {
	if !expected.Exists {
		return errors.New("sourceauthority: remove action has no attributable existing target")
	}
	parent, leaf, err := openMutationParent(root, relative)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(parent) }()
	tombName := mutationTombstoneName(leaf, operation, index)
	proof := journal.Active
	if proof == nil {
		target, err := statMutationSibling(ctx, root, relative, parent, leaf)
		if err != nil || physicalState(target) != expected {
			return errors.Join(ErrSourceChanged, err)
		}
		if err := requireMutationSiblingAbsent(ctx, root, relative, parent, tombName); err != nil {
			return err
		}
		proof = &mutationActionProof{
			Index: index, Kind: MutationRemove, Phase: mutationPhasePrepared,
			TombName: tombName, Source: expected,
		}
		journal.Active = proof
		if err := storeMutationJournalDurably(ctx, runtimeDir, *journal); err != nil {
			return err
		}
	}
	if err := validateMutationProof(proof, index, MutationRemove, "", tombName); err != nil {
		return err
	}
	if proof.Phase == mutationPhasePrepared {
		target, err := statMutationSibling(ctx, root, relative, parent, leaf)
		if err != nil {
			return err
		}
		tomb, err := statMutationSibling(ctx, root, relative, parent, tombName)
		if err != nil {
			return err
		}
		switch {
		case physicalState(target) == proof.Source && !tomb.Exists:
			if err := mutationRenameNoReplace(parent, leaf, parent, tombName); err != nil {
				return err
			}
		case !target.Exists && mutationProofMatches(tomb, proof.Source):
		default:
			return ErrSourceChanged
		}
		if err := unix.Fsync(parent); err != nil {
			return err
		}
		if afterSync != nil {
			if err := afterSync(mutationPhaseTargetStaged); err != nil {
				return err
			}
		}
		proof.Phase = mutationPhaseTargetStaged
		if err := storeMutationJournalDurably(ctx, runtimeDir, *journal); err != nil {
			return err
		}
	}
	if proof.Phase != mutationPhaseTargetStaged {
		return errors.New("sourceauthority: invalid remove mutation proof phase")
	}
	target, err := statMutationSibling(ctx, root, relative, parent, leaf)
	if err != nil || target.Exists {
		return errors.Join(ErrSourceChanged, err)
	}
	tomb, err := statMutationSibling(ctx, root, relative, parent, tombName)
	if err != nil {
		return err
	}
	if tomb.Exists {
		if !mutationProofMatches(tomb, proof.Source) {
			return ErrSourceChanged
		}
		if err := removeMutationLeaf(parent, tombName, tomb.Kind); err != nil {
			return err
		}
	}
	return unix.Fsync(parent)
}

func executeProvenRename(
	ctx context.Context,
	runtimeDir string,
	fromRoot RootSpec,
	fromRelative string,
	toRoot RootSpec,
	toRelative string,
	fromExpected ExpectedPhysicalState,
	toExpected ExpectedPhysicalState,
	operation catalog.MutationID,
	index int,
	journal *mutationJournal,
	afterSync mutationDurabilityFailpoint,
) error {
	if !fromExpected.Exists {
		return errors.New("sourceauthority: rename action has no attributable source")
	}
	fromParent, fromLeaf, err := openMutationParent(fromRoot, fromRelative)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fromParent) }()
	toParent, toLeaf, err := openMutationParent(toRoot, toRelative)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(toParent) }()
	var fromStatus, toStatus unix.Stat_t
	if err := unix.Fstat(fromParent, &fromStatus); err != nil {
		return err
	}
	if err := unix.Fstat(toParent, &toStatus); err != nil {
		return err
	}
	if fromStatus.Dev != toStatus.Dev {
		return errors.New("sourceauthority: mutation rename crossed volumes")
	}
	stageName := mutationTemporaryName(fromLeaf, operation, index)
	tombName := mutationTombstoneName(toLeaf, operation, index)
	proof := journal.Active
	if proof == nil {
		from, err := statMutationSibling(ctx, fromRoot, fromRelative, fromParent, fromLeaf)
		if err != nil || physicalState(from) != fromExpected {
			return errors.Join(ErrSourceChanged, err)
		}
		to, err := statMutationSibling(ctx, toRoot, toRelative, toParent, toLeaf)
		if err != nil || physicalState(to) != toExpected {
			return errors.Join(ErrSourceChanged, err)
		}
		if err := requireMutationSiblingAbsent(ctx, fromRoot, fromRelative, fromParent, stageName); err != nil {
			return err
		}
		if err := requireMutationSiblingAbsent(ctx, toRoot, toRelative, toParent, tombName); err != nil {
			return err
		}
		proof = &mutationActionProof{
			Index: index, Kind: MutationRename, Phase: mutationPhasePrepared,
			StageName: stageName, TombName: tombName, Source: fromExpected, Target: toExpected,
		}
		journal.Active = proof
		if err := storeMutationJournalDurably(ctx, runtimeDir, *journal); err != nil {
			return err
		}
	}
	if err := validateMutationProof(proof, index, MutationRename, stageName, tombName); err != nil {
		return err
	}
	if proof.Phase == mutationPhasePrepared {
		from, err := statMutationSibling(ctx, fromRoot, fromRelative, fromParent, fromLeaf)
		if err != nil {
			return err
		}
		stage, err := statMutationSibling(ctx, fromRoot, fromRelative, fromParent, stageName)
		if err != nil {
			return err
		}
		switch {
		case physicalState(from) == proof.Source && !stage.Exists:
			if err := mutationRenameNoReplace(fromParent, fromLeaf, fromParent, stageName); err != nil {
				return err
			}
		case !from.Exists && mutationProofMatches(stage, proof.Source):
		default:
			return ErrSourceChanged
		}
		if err := unix.Fsync(fromParent); err != nil {
			return err
		}
		if afterSync != nil {
			if err := afterSync(mutationPhaseSourceStaged); err != nil {
				return err
			}
		}
		proof.Phase = mutationPhaseSourceStaged
		if err := storeMutationJournalDurably(ctx, runtimeDir, *journal); err != nil {
			return err
		}
	}
	if proof.Phase == mutationPhaseSourceStaged {
		to, err := statMutationSibling(ctx, toRoot, toRelative, toParent, toLeaf)
		if err != nil {
			return err
		}
		tomb, err := statMutationSibling(ctx, toRoot, toRelative, toParent, tombName)
		if err != nil {
			return err
		}
		if proof.Target.Exists {
			switch {
			case physicalState(to) == proof.Target && !tomb.Exists:
				if err := mutationRenameNoReplace(toParent, toLeaf, toParent, tombName); err != nil {
					return err
				}
			case !to.Exists && mutationProofMatches(tomb, proof.Target):
			default:
				return ErrSourceChanged
			}
		} else if to.Exists || tomb.Exists {
			return ErrSourceChanged
		}
		if err := unix.Fsync(toParent); err != nil {
			return err
		}
		if afterSync != nil {
			if err := afterSync(mutationPhaseTargetStaged); err != nil {
				return err
			}
		}
		proof.Phase = mutationPhaseTargetStaged
		if err := storeMutationJournalDurably(ctx, runtimeDir, *journal); err != nil {
			return err
		}
	}
	if proof.Phase == mutationPhaseTargetStaged {
		stage, err := statMutationSibling(ctx, fromRoot, fromRelative, fromParent, stageName)
		if err != nil {
			return err
		}
		to, err := statMutationSibling(ctx, toRoot, toRelative, toParent, toLeaf)
		if err != nil {
			return err
		}
		switch {
		case mutationProofMatches(stage, proof.Source) && !to.Exists:
			if err := mutationRenameNoReplace(fromParent, stageName, toParent, toLeaf); err != nil {
				return err
			}
		case !stage.Exists && mutationProofMatches(to, proof.Source):
		default:
			return ErrSourceChanged
		}
		if err := errors.Join(unix.Fsync(fromParent), unix.Fsync(toParent)); err != nil {
			return err
		}
		if afterSync != nil {
			if err := afterSync(mutationPhaseInstalled); err != nil {
				return err
			}
		}
		proof.Phase = mutationPhaseInstalled
		if err := storeMutationJournalDurably(ctx, runtimeDir, *journal); err != nil {
			return err
		}
	}
	if proof.Phase != mutationPhaseInstalled {
		return errors.New("sourceauthority: invalid rename mutation proof phase")
	}
	to, err := statMutationSibling(ctx, toRoot, toRelative, toParent, toLeaf)
	if err != nil || !mutationProofMatches(to, proof.Source) {
		return errors.Join(ErrSourceChanged, err)
	}
	if proof.Target.Exists {
		tomb, err := statMutationSibling(ctx, toRoot, toRelative, toParent, tombName)
		if err != nil {
			return err
		}
		if tomb.Exists {
			if !mutationProofMatches(tomb, proof.Target) {
				return ErrSourceChanged
			}
			if err := removeMutationLeaf(toParent, tombName, tomb.Kind); err != nil {
				return err
			}
		}
	}
	return errors.Join(unix.Fsync(fromParent), unix.Fsync(toParent))
}

func validateMutationProof(
	proof *mutationActionProof,
	index int,
	kind MutationActionKind,
	stageName string,
	tombName string,
) error {
	if proof == nil || proof.Index != index || proof.Kind != kind || proof.StageName != stageName || proof.TombName != tombName ||
		proof.Phase < mutationPhasePrepared || proof.Phase > mutationPhaseInstalled {
		return errors.New("sourceauthority: mutation action proof does not match its exact action")
	}
	return nil
}

func mutationProofMatches(entry PhysicalEntry, proof ExpectedPhysicalState) bool {
	return entry.Exists && proof.Exists && entry.Kind == proof.Kind && entry.Identity == proof.Identity &&
		entry.Mode == proof.Mode && entry.UID == proof.UID && entry.GID == proof.GID && entry.Size == proof.Size &&
		entry.LinkTarget == proof.LinkTarget &&
		entry.ContentFingerprint == proof.ContentFingerprint
}

func createMutationStageFile(parent int, name string, mode uint32, payload *mutationPayload) error {
	fd, err := unix.Openat(parent, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), "source-mutation-stage")
	_, copyErr := io.CopyBuffer(file, io.NewSectionReader(payload.file, 0, payload.size), make([]byte, sourceTaskChunkSize))
	return errors.Join(copyErr, file.Chmod(os.FileMode(mode&0o7777)), file.Sync(), file.Close(), unix.Fsync(parent))
}

func statMutationSibling(
	ctx context.Context,
	root RootSpec,
	relative string,
	parent int,
	leaf string,
) (PhysicalEntry, error) {
	var status unix.Stat_t
	if err := unix.Fstatat(parent, leaf, &status, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return PhysicalEntry{Root: root.ID, Relative: relative}, nil
		}
		return PhysicalEntry{}, err
	}
	return statMutationAt(ctx, root, relative, parent, leaf)
}

func requireMutationSiblingAbsent(ctx context.Context, root RootSpec, relative string, parent int, leaf string) error {
	entry, err := statMutationSibling(ctx, root, relative, parent, leaf)
	if err != nil {
		return err
	}
	if entry.Exists {
		return ErrSourceChanged
	}
	return nil
}

func mutationTombstoneName(leaf string, operation catalog.MutationID, index int) string {
	return "." + leaf + ".fusekit-tomb-" + operation.String() + fmt.Sprintf("-%d", index)
}

func collectMutationReceipt(ctx context.Context, task MutationTask) (MutationReceipt, error) {
	roots := mutationRoots(task.Roots)
	effects := make([]PhysicalEntry, len(task.Expected))
	for index, expected := range task.Expected {
		entry, err := statMutationPath(ctx, roots[expected.Path.Root], expected.Path.Relative)
		if err != nil {
			return MutationReceipt{}, err
		}
		if (expected.Outcome == MutationAbsent && entry.Exists) ||
			(expected.Outcome == MutationPresent && (!entry.Exists || entry.Kind != expected.Kind)) {
			return MutationReceipt{}, ErrSourceChanged
		}
		effects[index] = entry
	}
	payload, err := json.Marshal(struct {
		Operation catalog.MutationID
		Effects   []PhysicalEntry
	}{task.OperationID, effects})
	if err != nil {
		return MutationReceipt{}, err
	}
	return MutationReceipt{OperationID: task.OperationID, Effects: effects, Digest: sha256.Sum256(payload)}, nil
}

func validateMutationReceiptState(ctx context.Context, task MutationTask, receipt MutationReceipt) error {
	if receipt.OperationID != task.OperationID || receipt.Digest == (Fingerprint{}) || len(receipt.Effects) != len(task.Expected) {
		return errors.New("sourceauthority: invalid durable mutation receipt")
	}
	roots := mutationRoots(task.Roots)
	for index, effect := range task.Expected {
		entry, err := statMutationPath(ctx, roots[effect.Path.Root], effect.Path.Relative)
		if err != nil || !samePhysical(entry, receipt.Effects[index]) {
			return errors.Join(ErrSourceChanged, err)
		}
	}
	return nil
}

func mutationRoots(values []RootSpec) map[RootID]RootSpec {
	result := make(map[RootID]RootSpec, len(values))
	for _, value := range values {
		result[value.ID] = value
	}
	return result
}

func mutationEffects(values []ExpectedEffect) map[PathRef]ExpectedEffect {
	result := make(map[PathRef]ExpectedEffect, len(values))
	for _, value := range values {
		result[value.Path] = value
	}
	return result
}

func statMutationPath(ctx context.Context, root RootSpec, relative string) (PhysicalEntry, error) {
	return securePathSource{}.Stat(ctx, root, relative)
}

func statMutationAt(ctx context.Context, root RootSpec, relative string, parent int, leaf string) (PhysicalEntry, error) {
	if err := ctx.Err(); err != nil {
		return PhysicalEntry{}, err
	}
	volume, err := rootIdentityFromFD(parent)
	if err != nil {
		return PhysicalEntry{}, err
	}
	var status unix.Stat_t
	if err := unix.Fstatat(parent, leaf, &status, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return PhysicalEntry{}, err
	}
	return physicalChildAt(ctx, PhysicalEntry{Root: root.ID, Relative: relative}, volume.VolumeUUID, parent, leaf, status)
}

func openMutationParent(root RootSpec, relative string) (int, string, error) {
	if root.Kind == RootFile {
		return -1, "", errors.New("sourceauthority: exact-file authority roots are immutable")
	}
	if relative == "." {
		return -1, "", errors.New("sourceauthority: directory root itself is not mutable")
	}
	rootFD, err := openSecureRoot(root)
	if err != nil {
		return -1, "", err
	}
	parent, leaf, err := openRelativeParent(rootFD, relative)
	_ = unix.Close(rootFD)
	return parent, leaf, err
}

func mutationTemporaryName(leaf string, operation catalog.MutationID, index int) string {
	return "." + leaf + ".fusekit-" + operation.String() + fmt.Sprintf("-%d", index)
}

func removeMutationLeaf(parent int, leaf string, kind PhysicalKind) error {
	flags := 0
	if kind == PhysicalDirectory {
		flags = unix.AT_REMOVEDIR
	}
	return unix.Unlinkat(parent, leaf, flags)
}

func mutationJournalDirectory(runtimeDir string) string {
	return filepath.Join(runtimeDir, "source-mutations")
}

func mutationJournalPath(runtimeDir string, operation catalog.MutationID) string {
	return filepath.Join(mutationJournalDirectory(runtimeDir), operation.String()+".json")
}

type mutationOperationIdentity struct {
	Authority           causal.SourceAuthorityID
	AuthorityGeneration causal.Generation
	Operation           catalog.MutationID
}

func mutationConsumedKey(identity mutationOperationIdentity) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("fusekit.sourceauthority.consumed.v1\x00"))
	var scalar [8]byte
	binary.BigEndian.PutUint64(scalar[:], uint64(len(identity.Authority)))
	_, _ = hash.Write(scalar[:])
	_, _ = hash.Write([]byte(identity.Authority))
	_, _ = hash.Write(identity.Operation[:])
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func mutationConsumedAddress(identity mutationOperationIdentity) (string, string) {
	key := mutationConsumedKey(identity)
	return hex.EncodeToString(key[:1]), hex.EncodeToString(key[1:]) + ".json"
}

func openMutationConsumedShard(
	runtimeDir string,
	identity mutationOperationIdentity,
	create bool,
) (int, bool, error) {
	if identity.Authority == "" || identity.AuthorityGeneration == 0 || identity.Operation == (catalog.MutationID{}) {
		return -1, false, errors.New("sourceauthority: invalid consumed mutation identity")
	}
	runtimeFD, err := openSecurePrivateDirectory(runtimeDir)
	if err != nil {
		return -1, false, err
	}
	defer func() { _ = unix.Close(runtimeFD) }()
	var runtimeStatus unix.Stat_t
	if err := unix.Fstat(runtimeFD, &runtimeStatus); err != nil {
		return -1, false, err
	}
	rootFD, exists, err := openMutationPrivateDirectoryAt(
		runtimeFD, "source-mutations-consumed", create, runtimeStatus,
	)
	if err != nil || !exists {
		return -1, false, err
	}
	defer func() { _ = unix.Close(rootFD) }()
	shard, _ := mutationConsumedAddress(identity)
	return openMutationPrivateDirectoryAt(rootFD, shard, create, runtimeStatus)
}

func openMutationPrivateDirectoryAt(
	parentFD int,
	name string,
	create bool,
	expectedRoot unix.Stat_t,
) (int, bool, error) {
	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) && !create {
		return -1, false, nil
	}
	if errors.Is(err, unix.ENOENT) {
		if err := unix.Mkdirat(parentFD, name, 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
			return -1, false, err
		}
		if err := unix.Fsync(parentFD); err != nil {
			return -1, false, err
		}
		fd, err = unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	}
	if err != nil {
		return -1, false, err
	}
	var status unix.Stat_t
	if err := unix.Fstat(fd, &status); err != nil || status.Mode&unix.S_IFMT != unix.S_IFDIR ||
		status.Uid != uint32(os.Geteuid()) || status.Mode&0o777 != 0o700 || status.Dev != expectedRoot.Dev {
		_ = unix.Close(fd)
		return -1, false, errors.Join(errors.New("sourceauthority: insecure consumed mutation directory"), err)
	}
	return fd, true, nil
}

func openMutationJournalDirectory(runtimeDir string) (int, error) {
	runtimeFD, err := openSecurePrivateDirectory(runtimeDir)
	if err != nil {
		return -1, err
	}
	defer func() { _ = unix.Close(runtimeFD) }()
	if err := unix.Mkdirat(runtimeFD, "source-mutations", 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
		return -1, err
	}
	if err := unix.Fsync(runtimeFD); err != nil {
		return -1, err
	}
	fd, err := unix.Openat(runtimeFD, "source-mutations", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, err
	}
	var runtimeStatus, status unix.Stat_t
	if err := unix.Fstat(runtimeFD, &runtimeStatus); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	if err := unix.Fstat(fd, &status); err != nil || status.Mode&unix.S_IFMT != unix.S_IFDIR ||
		status.Uid != uint32(os.Geteuid()) || status.Mode&0o777 != 0o700 || status.Dev != runtimeStatus.Dev {
		_ = unix.Close(fd)
		return -1, errors.Join(errors.New("sourceauthority: insecure mutation journal directory"), err)
	}
	return fd, nil
}

func openSecurePrivateDirectory(path string) (int, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) {
		return -1, errors.New("sourceauthority: private runtime path is invalid")
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for _, component := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if component == "" {
			continue
		}
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return -1, openErr
		}
		fd = next
	}
	var status unix.Stat_t
	if err := unix.Fstat(fd, &status); err != nil || status.Mode&unix.S_IFMT != unix.S_IFDIR ||
		status.Uid != uint32(os.Geteuid()) || status.Mode&0o777 != 0o700 {
		_ = unix.Close(fd)
		return -1, errors.Join(errors.New("sourceauthority: insecure private runtime directory"), err)
	}
	return fd, nil
}

func loadMutationJournal(
	ctx context.Context,
	runtimeDir string,
	operation catalog.MutationID,
) (mutationJournal, bool, error) {
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		return mutationJournal{}, false, err
	}
	defer func() { _ = unix.Close(directoryFD) }()
	return loadMutationJournalAt(ctx, directoryFD, operation)
}

func loadMutationOperation(
	ctx context.Context,
	runtimeDir string,
	identity mutationOperationIdentity,
) (mutationJournal, bool, error) {
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		return mutationJournal{}, false, err
	}
	defer func() { _ = unix.Close(directoryFD) }()
	lock, err := lockMutationJournalDirectory(ctx, directoryFD)
	if err != nil {
		return mutationJournal{}, false, err
	}
	defer func() { _ = lock.Close() }()
	return loadMutationOperationAt(ctx, runtimeDir, directoryFD, identity)
}

func inspectMutationOperation(
	ctx context.Context,
	runtimeDir string,
	request MutationInspectionRequest,
) (MutationInspection, error) {
	if request.Authority == "" || request.AuthorityGeneration == 0 ||
		request.Operation == (catalog.MutationID{}) || request.ExpectationDigest == (Fingerprint{}) {
		return MutationInspection{}, errors.New("sourceauthority: invalid mutation inspection identity")
	}
	journal, exists, err := loadMutationOperation(ctx, runtimeDir, mutationOperationIdentity{
		Authority: request.Authority, AuthorityGeneration: request.AuthorityGeneration,
		Operation: request.Operation,
	})
	if err != nil {
		return MutationInspection{}, err
	}
	if !exists {
		return MutationInspection{State: MutationInspectionNotFound}, nil
	}
	if journal.Authority != request.Authority ||
		journal.AuthorityGeneration != request.AuthorityGeneration ||
		journal.Operation != request.Operation || journal.ExpectationDigest != request.ExpectationDigest {
		return MutationInspection{}, fmt.Errorf(
			"%w: mutation inspection does not match exact request identity", catalog.ErrMutationConflict,
		)
	}
	inspection := MutationInspection{
		ExpectationDigest: journal.ExpectationDigest,
		Intent:            journal.Intent,
	}
	if journal.Request != nil {
		inspection.RequestContent = &MutationContentDigest{
			Size: journal.Request.Size, Digest: Fingerprint(journal.Request.Hash),
		}
	}
	if journal.Receipt != nil {
		receipt := *journal.Receipt
		receipt.Effects = append([]PhysicalEntry(nil), receipt.Effects...)
		inspection.State = MutationInspectionApplied
		inspection.Receipt = &receipt
		return inspection, nil
	}
	if journal.Terminal != nil {
		proof := *journal.Terminal
		inspection.Terminal = &proof
		if journal.Consumed {
			inspection.State = MutationInspectionConsumed
		} else {
			inspection.State = MutationInspectionTerminal
		}
		return inspection, nil
	}
	inspection.State = MutationInspectionActiveUnapplied
	return inspection, nil
}

func loadMutationOperationAt(
	ctx context.Context,
	runtimeDir string,
	directoryFD int,
	identity mutationOperationIdentity,
) (mutationJournal, bool, error) {
	consumed, exists, err := loadConsumedMutationAt(ctx, runtimeDir, identity)
	if err != nil || exists {
		return consumed, exists, err
	}
	return loadMutationJournalAt(ctx, directoryFD, identity.Operation)
}

func loadConsumedMutationAt(
	ctx context.Context,
	runtimeDir string,
	identity mutationOperationIdentity,
) (mutationJournal, bool, error) {
	shardFD, exists, err := openMutationConsumedShard(runtimeDir, identity, false)
	if err != nil || !exists {
		return mutationJournal{}, false, err
	}
	defer func() { _ = unix.Close(shardFD) }()
	_, name := mutationConsumedAddress(identity)
	payload, exists, err := readMutationJournalAt(ctx, shardFD, name)
	if err != nil || !exists {
		return mutationJournal{}, exists, err
	}
	var journal mutationJournal
	if len(payload) > wire.DefaultMaxFrame || json.Unmarshal(payload, &journal) != nil ||
		validateMutationJournal(journal, identity.Operation) != nil || !journal.Consumed ||
		journal.Authority != identity.Authority {
		return mutationJournal{}, false, errors.New("sourceauthority: corrupt addressed consumed mutation")
	}
	return journal, true, nil
}

func createMutationJournal(
	ctx context.Context,
	runtimeDir string,
	journal mutationJournal,
) (bool, error) {
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		return false, err
	}
	defer func() { _ = unix.Close(directoryFD) }()
	lock, err := lockMutationJournalDirectory(ctx, directoryFD)
	if err != nil {
		return false, err
	}
	defer func() { _ = lock.Close() }()
	identity := mutationOperationIdentity{
		Authority: journal.Authority, AuthorityGeneration: journal.AuthorityGeneration, Operation: journal.Operation,
	}
	if _, exists, err := loadMutationOperationAt(ctx, runtimeDir, directoryFD, identity); err != nil || exists {
		return false, err
	}
	if err := storeMutationJournalAt(ctx, directoryFD, journal); err != nil {
		return false, err
	}
	return true, nil
}

func loadMutationJournalAt(
	ctx context.Context,
	directoryFD int,
	operation catalog.MutationID,
) (mutationJournal, bool, error) {
	payload, exists, err := readMutationJournalAt(ctx, directoryFD, operation.String()+".json")
	if err != nil {
		return mutationJournal{}, false, err
	}
	if !exists {
		return mutationJournal{}, false, nil
	}
	var journal mutationJournal
	if len(payload) > wire.DefaultMaxFrame || json.Unmarshal(payload, &journal) != nil {
		return mutationJournal{}, false, errors.New("sourceauthority: invalid mutation journal")
	}
	if err := validateMutationJournal(journal, operation); err != nil {
		return mutationJournal{}, false, errors.Join(
			errors.New("sourceauthority: mutation journal does not match its operation"), err,
		)
	}
	return journal, true, nil
}

func validateMutationJournal(journal mutationJournal, operation catalog.MutationID) error {
	if journal.Protocol != mutationJournalProtocol || journal.Authority == "" || journal.AuthorityGeneration == 0 ||
		journal.Operation != operation || journal.ExpectationDigest == (Fingerprint{}) {
		return errors.New("sourceauthority: mutation journal identity mismatch")
	}
	if journal.Request != nil && (journal.Request.Size < 0 || journal.Request.Hash == ([sha256.Size]byte{})) {
		return errors.New("sourceauthority: invalid mutation request digest")
	}
	if journal.Terminal != nil {
		proof := *journal.Terminal
		if proof.Authority != journal.Authority || proof.AuthorityGeneration != journal.AuthorityGeneration ||
			proof.Operation != operation ||
			(proof.Outcome != MutationAcknowledged && proof.Outcome != MutationAbandoned) ||
			(proof.Outcome == MutationAcknowledged) != (proof.Digest != (Fingerprint{})) ||
			journal.Intent == (Fingerprint{}) || journal.Receipt != nil {
			return errors.New("sourceauthority: invalid mutation terminal proof")
		}
		if journal.Task == nil && (journal.Next != 0 || journal.Active != nil) {
			return errors.New("sourceauthority: non-compact mutation terminal proof")
		}
		if journal.Consumed && journal.Task != nil {
			return errors.New("sourceauthority: consumed mutation retains executable state")
		}
		if journal.Task != nil && validateMutationJournalTask(journal, operation) != nil {
			return errors.New("sourceauthority: invalid terminal mutation task")
		}
		return nil
	}
	if journal.Consumed {
		return errors.New("sourceauthority: consumed mutation lacks terminal identity")
	}
	if journal.Intent == (Fingerprint{}) || journal.Task == nil || journal.Task.Fence.Authority != journal.Authority ||
		validateMutationJournalTask(journal, operation) != nil ||
		(journal.Receipt != nil && (journal.Receipt.OperationID != operation || journal.Receipt.Digest == (Fingerprint{}) ||
			len(journal.Receipt.Effects) == 0 || len(journal.Receipt.Effects) > sourceTaskMutationActionLimit ||
			len(journal.Receipt.Effects) != len(journal.Task.Expected))) {
		return errors.New("sourceauthority: invalid active mutation journal")
	}
	return nil
}

func validateMutationJournalTask(journal mutationJournal, operation catalog.MutationID) error {
	if journal.Task == nil || journal.Task.Fence.Authority != journal.Authority ||
		journal.Task.Fence.AuthorityGeneration != journal.AuthorityGeneration || journal.Task.OperationID != operation ||
		journal.Task.ExpectationDigest != journal.ExpectationDigest ||
		journal.Next < 0 || journal.Next > len(journal.Task.Program.Actions) ||
		(journal.Active != nil && (journal.Active.Index != journal.Next ||
			journal.Active.Phase < mutationPhasePrepared || journal.Active.Phase > mutationPhaseInstalled)) {
		return errors.New("sourceauthority: invalid mutation journal task")
	}
	requiresRequest := false
	for _, action := range journal.Task.Program.Actions {
		requiresRequest = requiresRequest || action.UseRequestContent
	}
	if requiresRequest != (journal.Request != nil) || journal.Request != nil && journal.Request.Size < 0 {
		return errors.New("sourceauthority: mutation request digest does not match its task")
	}
	return nil
}

func storeMutationJournal(
	ctx context.Context,
	runtimeDir string,
	journal mutationJournal,
) error {
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(directoryFD) }()
	lockCtx, cancelLock := context.WithTimeout(ctx, mutationJournalLockWait)
	defer cancelLock()
	lock, err := lockMutationJournalDirectory(lockCtx, directoryFD)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	return storeMutationJournalAt(lockCtx, directoryFD, journal)
}

func storeMutationJournalDurably(
	ctx context.Context,
	runtimeDir string,
	journal mutationJournal,
) error {
	return storeMutationJournal(context.WithoutCancel(ctx), runtimeDir, journal)
}

func storeMutationJournalAt(
	ctx context.Context,
	directoryFD int,
	journal mutationJournal,
) error {
	payload, err := encodeMutationJournal(journal)
	if err != nil {
		return err
	}
	target := journal.Operation.String() + ".json"
	if err := enforceMutationJournalBounds(
		ctx, directoryFD, target, int64(len(payload)), maxMutationJournals, maxMutationJournalBytes,
	); err != nil {
		return err
	}
	return replaceMutationJournalAt(directoryFD, target, payload, nil)
}

type mutationJournalStorePhase uint8

const (
	mutationJournalStorePrepared mutationJournalStorePhase = iota + 1
	mutationJournalStoreReplaced
	mutationJournalStoreDurable
)

type mutationJournalStoreFailpoint func(mutationJournalStorePhase) error

func encodeMutationJournal(journal mutationJournal) ([]byte, error) {
	if journal.Operation == (catalog.MutationID{}) {
		return nil, errors.New("sourceauthority: mutation journal operation is required")
	}
	if err := validateMutationJournal(journal, journal.Operation); err != nil {
		return nil, errors.Join(errors.New("sourceauthority: refuse invalid mutation journal"), err)
	}
	payload, err := json.Marshal(journal)
	if err != nil {
		return nil, err
	}
	if len(payload) > wire.DefaultMaxFrame {
		return nil, errors.New("sourceauthority: mutation journal exceeds its bounded frame")
	}
	return payload, nil
}

func replaceMutationJournalAt(
	directoryFD int,
	target string,
	payload []byte,
	failpoint mutationJournalStoreFailpoint,
) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	temporaryName := ".mutation-" + hex.EncodeToString(nonce[:]) + ".tmp"
	fd, err := unix.Openat(directoryFD, temporaryName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	temporary := os.NewFile(uintptr(fd), "source-mutation-journal")
	result := errors.Join(writeFull(temporary, payload), temporary.Sync(), temporary.Close())
	if result == nil && failpoint != nil {
		result = failpoint(mutationJournalStorePrepared)
	}
	if result == nil {
		result = unix.Renameat(directoryFD, temporaryName, directoryFD, target)
	}
	if result != nil {
		_ = unix.Unlinkat(directoryFD, temporaryName, 0)
		return result
	}
	if failpoint != nil {
		if err := failpoint(mutationJournalStoreReplaced); err != nil {
			return err
		}
	}
	return unix.Fsync(directoryFD)
}

func storeConsumedMutationAt(
	ctx context.Context,
	runtimeDir string,
	journal mutationJournal,
	failpoint mutationJournalStoreFailpoint,
) error {
	if !journal.Consumed || journal.Task != nil || journal.Terminal == nil {
		return errors.New("sourceauthority: refuse non-compact consumed mutation")
	}
	payload, err := encodeMutationJournal(journal)
	if err != nil {
		return err
	}
	identity := mutationOperationIdentity{
		Authority: journal.Authority, AuthorityGeneration: journal.AuthorityGeneration, Operation: journal.Operation,
	}
	shardFD, _, err := openMutationConsumedShard(runtimeDir, identity, true)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(shardFD) }()
	_, name := mutationConsumedAddress(identity)
	if err := replaceMutationJournalAt(shardFD, name, payload, failpoint); err != nil {
		return err
	}
	stored, exists, err := loadConsumedMutationAt(ctx, runtimeDir, identity)
	if err != nil || !exists || stored.AuthorityGeneration != journal.AuthorityGeneration || stored.Intent != journal.Intent ||
		stored.ExpectationDigest != journal.ExpectationDigest ||
		!sameMutationPayloadDigest(stored.Request, journal.Request) || stored.Terminal == nil ||
		*stored.Terminal != *journal.Terminal {
		return errors.Join(errors.New("sourceauthority: consumed mutation verification failed"), err)
	}
	if failpoint != nil {
		if err := failpoint(mutationJournalStoreDurable); err != nil {
			return err
		}
	}
	return nil
}

func writeFull(file *os.File, payload []byte) error {
	for len(payload) != 0 {
		count, err := file.Write(payload)
		if err != nil {
			return err
		}
		payload = payload[count:]
	}
	return nil
}

func validateMutationJournalDirectory(ctx context.Context, runtimeDir string) error {
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(directoryFD) }()
	lockCtx, cancelLock := context.WithTimeout(ctx, mutationJournalLockWait)
	defer cancelLock()
	lock, err := lockMutationJournalDirectory(lockCtx, directoryFD)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	entryLimit, err := mutationJournalDirectoryEntryLimit(maxMutationJournals)
	if err != nil {
		return err
	}
	var total int64
	journalCount := 0
	err = scanMutationJournalDirectory(lockCtx, directoryFD, entryLimit, func(entry os.DirEntry) error {
		if entry.IsDir() {
			return errors.New("sourceauthority: mutation journal directory contains a nested directory")
		}
		if mutationJournalTemporaryName(entry.Name()) {
			return nil
		}
		if strings.HasPrefix(entry.Name(), ".mutation-") || strings.HasSuffix(entry.Name(), ".tmp") {
			return errors.New("sourceauthority: mutation journal directory contains a corrupt temporary file")
		}
		if entry.Name() == ".lock" {
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			return errors.New("sourceauthority: mutation journal directory contains an unknown file")
		}
		journalCount++
		if journalCount > maxMutationJournals {
			return errors.New("sourceauthority: mutation journal count exceeds its startup bound")
		}
		payload, exists, err := readMutationJournalAt(lockCtx, directoryFD, entry.Name())
		if err != nil {
			return err
		}
		if !exists {
			return errMutationJournalDirectoryChanged
		}
		if int64(len(payload)) > maxMutationJournalBytes-total {
			return errors.New("sourceauthority: mutation journal bytes exceed their startup bound")
		}
		total += int64(len(payload))
		var journal mutationJournal
		if json.Unmarshal(payload, &journal) != nil || journal.Operation == (catalog.MutationID{}) ||
			entry.Name() != journal.Operation.String()+".json" || validateMutationJournal(journal, journal.Operation) != nil {
			return errors.New("sourceauthority: corrupt mutation journal at startup")
		}
		return nil
	})
	if err != nil {
		return err
	}
	err = walkMutationJournalDirectory(lockCtx, directoryFD, entryLimit, func(entry os.DirEntry) error {
		if !mutationJournalTemporaryName(entry.Name()) {
			return nil
		}
		return unix.Unlinkat(directoryFD, entry.Name(), 0)
	})
	if err != nil {
		return err
	}
	return unix.Fsync(directoryFD)
}

func lockMutationJournalDirectory(ctx context.Context, directoryFD int) (*os.File, error) {
	fd, err := unix.Openat(directoryFD, ".lock", unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "source-mutation-journal-lock")
	var status unix.Stat_t
	if err := unix.Fstat(fd, &status); err != nil || status.Mode&unix.S_IFMT != unix.S_IFREG ||
		status.Uid != uint32(os.Geteuid()) || status.Mode&0o777 != 0o600 {
		_ = file.Close()
		return nil, errors.Join(errors.New("sourceauthority: insecure mutation journal lock"), err)
	}
	for {
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			_ = file.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

type mutationJournalDirectoryEntry struct {
	name   string
	info   os.FileInfo
	device uint64
	inode  uint64
	uid    uint32
	gid    uint32
	digest []byte
}

func (e mutationJournalDirectoryEntry) Name() string               { return e.name }
func (e mutationJournalDirectoryEntry) IsDir() bool                { return e.info.IsDir() }
func (e mutationJournalDirectoryEntry) Type() os.FileMode          { return e.info.Mode().Type() }
func (e mutationJournalDirectoryEntry) Info() (os.FileInfo, error) { return e.info, nil }

type mutationJournalDirectorySnapshot struct {
	count  int
	digest [32]byte
}

func mutationJournalDirectoryEntryLimit(maxCount int) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if maxCount < 0 || maxCount > (maxInt-1)/2 {
		return 0, errors.New("sourceauthority: invalid mutation journal entry bound")
	}
	return maxCount*2 + 1, nil
}

func scanMutationJournalDirectory(
	ctx context.Context,
	directoryFD int,
	maxEntries int,
	visit func(os.DirEntry) error,
) error {
	first, err := scanMutationJournalDirectoryPass(ctx, directoryFD, maxEntries, visit)
	if err != nil {
		return err
	}
	second, err := scanMutationJournalDirectoryPass(ctx, directoryFD, maxEntries, nil)
	if err != nil {
		return err
	}
	if first != second {
		return errMutationJournalDirectoryChanged
	}
	return nil
}

func scanMutationJournalDirectoryPass(
	ctx context.Context,
	directoryFD int,
	maxEntries int,
	visit func(os.DirEntry) error,
) (mutationJournalDirectorySnapshot, error) {
	hasher := sha256.New()
	count := 0
	err := walkMutationJournalDirectory(ctx, directoryFD, maxEntries, func(entry os.DirEntry) error {
		observed, ok := entry.(mutationJournalDirectoryEntry)
		if !ok {
			return errors.New("sourceauthority: invalid mutation journal directory page entry")
		}
		count++
		if _, err := hasher.Write(observed.digest); err != nil {
			return err
		}
		if visit != nil {
			return visit(observed)
		}
		return nil
	})
	if err != nil {
		return mutationJournalDirectorySnapshot{}, err
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return mutationJournalDirectorySnapshot{count: count, digest: digest}, nil
}

func walkMutationJournalDirectory(
	ctx context.Context,
	directoryFD int,
	maxEntries int,
	visit func(os.DirEntry) error,
) error {
	if maxEntries < 1 {
		return errors.New("sourceauthority: invalid mutation journal scan bound")
	}
	after := ""
	visited := 0
	for {
		page, more, err := pageMutationJournalDirectory(ctx, directoryFD, after)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			if more {
				return errMutationJournalDirectoryChanged
			}
			return nil
		}
		for _, entry := range page {
			if err := ctx.Err(); err != nil {
				return err
			}
			if visited == maxEntries {
				return errMutationJournalDirectoryEntryBound
			}
			visited++
			if visit != nil {
				if err := visit(entry); err != nil {
					return err
				}
			}
		}
		if !more {
			return nil
		}
		after = page[len(page)-1].Name()
	}
}

func pageMutationJournalDirectory(
	ctx context.Context,
	directoryFD int,
	after string,
) (_ []mutationJournalDirectoryEntry, more bool, resultErr error) {
	duplicate, err := unix.Openat(
		directoryFD, ".",
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return nil, false, err
	}
	directory := os.NewFile(uintptr(duplicate), "source-mutation-journal-page")
	defer func() {
		if closeErr := directory.Close(); resultErr == nil && closeErr != nil {
			resultErr = closeErr
		}
	}()
	names := make([]string, 0, mutationJournalPageEntryLimit+1)
	eligible := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		entries, readErr := directory.ReadDir(mutationJournalReadBatchLimit)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, false, readErr
		}
		if len(entries) == 0 && readErr == nil {
			return nil, false, errMutationJournalDirectoryChanged
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			name := entry.Name()
			if len(name) == 0 || len(name) > mutationJournalNameByteLimit ||
				!utf8.ValidString(name) || strings.IndexByte(name, 0) >= 0 {
				return nil, false, errors.New("sourceauthority: invalid mutation journal directory entry name")
			}
			if name <= after {
				continue
			}
			eligible++
			index := sort.SearchStrings(names, name)
			if index < len(names) && names[index] == name {
				return nil, false, errMutationJournalDirectoryChanged
			}
			if index > mutationJournalPageEntryLimit {
				continue
			}
			names = append(names, "")
			copy(names[index+1:], names[index:])
			names[index] = name
			if len(names) > mutationJournalPageEntryLimit+1 {
				names = names[:mutationJournalPageEntryLimit+1]
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
	}
	page := make([]mutationJournalDirectoryEntry, 0, min(len(names), mutationJournalPageEntryLimit))
	pageBytes := 0
	for _, name := range names {
		if len(page) == mutationJournalPageEntryLimit {
			break
		}
		entry, err := mutationJournalDirectoryEntryAt(directoryFD, name)
		if err != nil {
			return nil, false, errors.Join(errMutationJournalDirectoryChanged, err)
		}
		entry.digest, err = mutationJournalDirectoryEntryDigest(entry)
		if err != nil {
			return nil, false, err
		}
		if len(entry.digest) > mutationJournalPageByteLimit {
			return nil, false, errors.New("sourceauthority: mutation journal directory entry exceeds its page byte limit")
		}
		if pageBytes+len(entry.digest) > mutationJournalPageByteLimit {
			break
		}
		pageBytes += len(entry.digest)
		page = append(page, entry)
	}
	if len(names) != 0 && len(page) == 0 {
		return nil, false, errors.New("sourceauthority: mutation journal directory page made no progress")
	}
	return page, eligible > len(page), nil
}

func mutationJournalDirectoryEntryDigest(entry mutationJournalDirectoryEntry) ([]byte, error) {
	encoded, err := json.Marshal(struct {
		Name       string `json:"name"`
		Mode       uint32 `json:"mode"`
		Size       int64  `json:"size"`
		Modified   int64  `json:"modified"`
		ModifiedNS int32  `json:"modified_ns"`
		Device     uint64 `json:"device"`
		Inode      uint64 `json:"inode"`
		UID        uint32 `json:"uid"`
		GID        uint32 `json:"gid"`
	}{
		Name: entry.name, Mode: uint32(entry.info.Mode()), Size: entry.info.Size(),
		Modified: entry.info.ModTime().Unix(), ModifiedNS: int32(entry.info.ModTime().Nanosecond()),
		Device: entry.device, Inode: entry.inode, UID: entry.uid, GID: entry.gid,
	})
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func mutationJournalDirectoryEntryAt(
	directoryFD int,
	name string,
) (_ mutationJournalDirectoryEntry, resultErr error) {
	fd, err := unix.Openat(
		directoryFD, name,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return mutationJournalDirectoryEntry{}, err
	}
	file := os.NewFile(uintptr(fd), "source-mutation-journal-entry")
	defer func() {
		if closeErr := file.Close(); resultErr == nil && closeErr != nil {
			resultErr = closeErr
		}
	}()
	var status unix.Stat_t
	if err := unix.Fstat(fd, &status); err != nil {
		return mutationJournalDirectoryEntry{}, err
	}
	info, err := file.Stat()
	if err != nil {
		return mutationJournalDirectoryEntry{}, err
	}
	return mutationJournalDirectoryEntry{
		name: name, info: info, device: uint64(status.Dev), inode: uint64(status.Ino),
		uid: status.Uid, gid: status.Gid,
	}, nil
}

func mutationJournalTemporaryName(name string) bool {
	const prefix = ".mutation-"
	const suffix = ".tmp"
	if len(name) != len(prefix)+32+len(suffix) ||
		!strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	_, err := hex.DecodeString(name[len(prefix) : len(name)-len(suffix)])
	return err == nil
}

func enforceMutationJournalBounds(
	ctx context.Context,
	directoryFD int,
	target string,
	replacementBytes int64,
	maxCount int,
	maxBytes int64,
) error {
	if replacementBytes < 0 || maxCount < 0 || maxBytes < 0 {
		return errors.New("sourceauthority: invalid mutation journal capacity")
	}
	entryLimit, err := mutationJournalDirectoryEntryLimit(maxCount)
	if err != nil {
		return err
	}
	count := 0
	var total, prior int64
	found := false
	err = walkMutationJournalDirectory(ctx, directoryFD, entryLimit, func(entry os.DirEntry) error {
		name := entry.Name()
		if name == ".lock" || mutationJournalTemporaryName(name) {
			return nil
		}
		if strings.HasPrefix(name, ".mutation-") || strings.HasSuffix(name, ".tmp") {
			return errors.New("sourceauthority: mutation journal directory contains a corrupt temporary file")
		}
		if !strings.HasSuffix(name, ".json") {
			return errors.New("sourceauthority: mutation journal directory contains an unknown file")
		}
		payload, exists, err := readMutationJournalAt(ctx, directoryFD, name)
		if err != nil || !exists {
			return errors.Join(errMutationJournalDirectoryChanged, err)
		}
		count++
		if count > maxCount || int64(len(payload)) > maxBytes-total {
			return errors.New("sourceauthority: mutation journal capacity is exhausted pending durable acknowledgements")
		}
		total += int64(len(payload))
		if name == target {
			found = true
			prior = int64(len(payload))
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		count++
	}
	if replacementBytes > maxBytes-(total-prior) || count > maxCount {
		return errors.New("sourceauthority: mutation journal capacity is exhausted pending durable acknowledgements")
	}
	return nil
}

func readMutationJournalAt(
	ctx context.Context,
	directoryFD int,
	name string,
) ([]byte, bool, error) {
	fd, err := unix.Openat(
		directoryFD, name,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if errors.Is(err, unix.ENOENT) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	file := os.NewFile(uintptr(fd), "source-mutation-journal")
	var status unix.Stat_t
	if err := unix.Fstat(fd, &status); err != nil || status.Mode&unix.S_IFMT != unix.S_IFREG ||
		status.Uid != uint32(os.Geteuid()) || status.Mode&0o777 != 0o600 || status.Size > wire.DefaultMaxFrame {
		_ = file.Close()
		return nil, false, errors.Join(errors.New("sourceauthority: insecure mutation journal file"), err)
	}
	payload := make([]byte, 0, int(status.Size))
	var buffer [32 << 10]byte
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, errors.Join(err, file.Close())
		}
		count, readErr := file.Read(buffer[:])
		if count != 0 {
			if len(payload) > wire.DefaultMaxFrame-count {
				return nil, false, errors.Join(
					errors.New("sourceauthority: mutation journal grew beyond its bounded frame"),
					file.Close(),
				)
			}
			payload = append(payload, buffer[:count]...)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, false, errors.Join(readErr, file.Close())
		}
	}
	err = ctx.Err()
	return payload, true, errors.Join(err, file.Close())
}

func settleMutationJournal(
	ctx context.Context,
	runtimeDir string,
	proof MutationTerminalProof,
) error {
	if proof.Authority == "" || proof.AuthorityGeneration == 0 || proof.Operation == (catalog.MutationID{}) ||
		(proof.Outcome != MutationAcknowledged && proof.Outcome != MutationAbandoned) ||
		(proof.Outcome == MutationAcknowledged) != (proof.Digest != (Fingerprint{})) {
		return errors.New("sourceauthority: invalid mutation terminal request")
	}
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(directoryFD) }()
	lock, err := lockMutationJournalDirectory(ctx, directoryFD)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	journal, exists, err := loadMutationJournalAt(ctx, directoryFD, proof.Operation)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("sourceauthority: mutation terminal request has no durable operation")
	}
	if journal.Authority != proof.Authority || journal.AuthorityGeneration != proof.AuthorityGeneration {
		return errors.New("sourceauthority: mutation terminal request escaped its authority")
	}
	if journal.Terminal != nil {
		if *journal.Terminal != proof {
			return errors.New("sourceauthority: mutation terminal request conflicts with durable proof")
		}
	} else {
		switch proof.Outcome {
		case MutationAcknowledged:
			if journal.Receipt == nil || journal.Receipt.OperationID != proof.Operation || journal.Receipt.Digest != proof.Digest {
				return errors.New("sourceauthority: mutation acknowledgement does not match its durable receipt")
			}
		case MutationAbandoned:
			if journal.Receipt != nil {
				return errors.New("sourceauthority: receipt-bearing mutation must be acknowledged by digest")
			}
		}
		journal.Terminal = &proof
		journal.Receipt = nil
		if err := storeMutationJournalAt(ctx, directoryFD, journal); err != nil {
			return err
		}
	}
	if err := cleanupMutationJournalArtifacts(ctx, journal); err != nil {
		return err
	}
	compact := mutationJournal{
		Protocol: mutationJournalProtocol, Authority: proof.Authority, AuthorityGeneration: proof.AuthorityGeneration,
		Operation: proof.Operation, ExpectationDigest: journal.ExpectationDigest,
		Intent: journal.Intent, Request: journal.Request, Terminal: &proof,
	}
	return storeMutationJournalAt(ctx, directoryFD, compact)
}

func mutationTerminalProofPage(
	ctx context.Context,
	runtimeDir string,
	authority causal.SourceAuthorityID,
	after catalog.MutationID,
	limit int,
) (MutationTerminalProofPage, error) {
	if authority == "" || limit < 1 || limit > MutationTerminalProofPageLimit {
		return MutationTerminalProofPage{}, errors.New("sourceauthority: invalid terminal proof page request")
	}
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		return MutationTerminalProofPage{}, err
	}
	defer func() { _ = unix.Close(directoryFD) }()
	lock, err := lockMutationJournalDirectory(ctx, directoryFD)
	if err != nil {
		return MutationTerminalProofPage{}, err
	}
	defer func() { _ = lock.Close() }()
	afterName := ""
	if after != (catalog.MutationID{}) {
		afterName = after.String() + ".json"
	}
	entries, directoryMore, err := pageMutationJournalDirectory(ctx, directoryFD, afterName)
	if err != nil {
		return MutationTerminalProofPage{}, err
	}
	page := MutationTerminalProofPage{Proofs: make([]MutationTerminalProof, 0, limit)}
	for index, entry := range entries {
		name := entry.Name()
		if name == ".lock" || mutationJournalTemporaryName(name) {
			continue
		}
		if strings.HasPrefix(name, ".mutation-") || strings.HasSuffix(name, ".tmp") {
			return MutationTerminalProofPage{}, errors.New("sourceauthority: invalid mutation journal temporary name")
		}
		if !strings.HasSuffix(name, ".json") {
			return MutationTerminalProofPage{}, errors.New("sourceauthority: invalid mutation journal name")
		}
		operationText := strings.TrimSuffix(name, ".json")
		operation, err := catalog.ParseMutationID(operationText)
		if err != nil {
			return MutationTerminalProofPage{}, errors.New("sourceauthority: invalid mutation journal name")
		}
		if operation.String() <= after.String() {
			return MutationTerminalProofPage{}, errors.New("sourceauthority: mutation proof page cursor did not advance")
		}
		page.Next = operation
		journal, exists, err := loadMutationJournalAt(ctx, directoryFD, operation)
		if err != nil || !exists {
			return MutationTerminalProofPage{}, errors.Join(
				errors.New("sourceauthority: mutation journal changed during proof listing"),
				err,
			)
		}
		if journal.Terminal != nil && journal.Authority == authority {
			identity := mutationOperationIdentity{
				Authority: authority, AuthorityGeneration: journal.AuthorityGeneration, Operation: journal.Operation,
			}
			if consumedJournal, consumed, err := loadConsumedMutationAt(ctx, runtimeDir, identity); err != nil {
				return MutationTerminalProofPage{}, err
			} else if consumed {
				if consumedJournal.AuthorityGeneration != journal.AuthorityGeneration ||
					consumedJournal.Terminal == nil || *consumedJournal.Terminal != *journal.Terminal {
					return MutationTerminalProofPage{}, errors.New("sourceauthority: active mutation conflicts with consumed operation identity")
				}
				continue
			}
			page.Proofs = append(page.Proofs, *journal.Terminal)
			if len(page.Proofs) == limit {
				page.More = index+1 < len(entries) || directoryMore
				return page, nil
			}
		}
	}
	page.More = directoryMore
	if page.More && page.Next == (catalog.MutationID{}) {
		return MutationTerminalProofPage{}, errors.New("sourceauthority: mutation proof page made no cursor progress")
	}
	return page, nil
}

func forgetMutationJournal(ctx context.Context, runtimeDir string, proof MutationTerminalProof) error {
	return forgetMutationJournalWithFailpoint(ctx, runtimeDir, proof, nil)
}

func forgetMutationJournalWithFailpoint(
	ctx context.Context,
	runtimeDir string,
	proof MutationTerminalProof,
	failpoint mutationJournalStoreFailpoint,
) error {
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(directoryFD) }()
	lock, err := lockMutationJournalDirectory(ctx, directoryFD)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	identity := mutationOperationIdentity{
		Authority: proof.Authority, AuthorityGeneration: proof.AuthorityGeneration, Operation: proof.Operation,
	}
	consumedJournal, consumedExists, err := loadConsumedMutationAt(ctx, runtimeDir, identity)
	if err != nil {
		return err
	}
	journal, activeExists, err := loadMutationJournalAt(ctx, directoryFD, proof.Operation)
	if err != nil {
		return err
	}
	if consumedExists {
		if consumedJournal.Terminal == nil || *consumedJournal.Terminal != proof {
			return errors.New("sourceauthority: mutation forget conflicts with consumed terminal proof")
		}
		if !activeExists {
			return nil
		}
		if journal.Authority != proof.Authority || journal.AuthorityGeneration != proof.AuthorityGeneration ||
			journal.Terminal == nil || *journal.Terminal != proof {
			return errors.New("sourceauthority: consumed mutation shadows a conflicting active journal")
		}
		if err := unix.Unlinkat(directoryFD, proof.Operation.String()+".json", 0); err != nil && !errors.Is(err, unix.ENOENT) {
			return err
		}
		return unix.Fsync(directoryFD)
	}
	if !activeExists || journal.Terminal == nil {
		return errors.New("sourceauthority: mutation forget has no durable terminal proof")
	}
	if journal.Authority != proof.Authority || journal.AuthorityGeneration != proof.AuthorityGeneration {
		return errors.New("sourceauthority: mutation forget escaped its authority")
	}
	if *journal.Terminal != proof {
		return errors.New("sourceauthority: mutation forget conflicts with durable terminal proof")
	}
	if err := cleanupMutationJournalArtifacts(ctx, journal); err != nil {
		return err
	}
	consumed := mutationJournal{
		Protocol: mutationJournalProtocol, Authority: journal.Authority,
		AuthorityGeneration: journal.AuthorityGeneration,
		Operation:           journal.Operation, ExpectationDigest: journal.ExpectationDigest,
		Intent: journal.Intent, Request: journal.Request,
		Terminal: journal.Terminal, Consumed: true,
	}
	if err := storeConsumedMutationAt(ctx, runtimeDir, consumed, failpoint); err != nil {
		return err
	}
	if err := unix.Unlinkat(directoryFD, proof.Operation.String()+".json", 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	return unix.Fsync(directoryFD)
}

func cleanupMutationJournalArtifacts(ctx context.Context, journal mutationJournal) error {
	if journal.Task == nil || journal.Active == nil {
		return nil
	}
	if err := validateTaskRootFence(journal.Task.Fence, journal.Task.Roots); err != nil {
		return err
	}
	action := journal.Task.Program.Actions[journal.Active.Index]
	roots := mutationRoots(journal.Task.Roots)
	cleanup := func(root RootSpec, relative, name string, expected ExpectedPhysicalState) error {
		if name == "" || !expected.Exists {
			return nil
		}
		parent, _, err := openMutationParent(root, relative)
		if err != nil {
			return err
		}
		defer func() { _ = unix.Close(parent) }()
		entry, err := statMutationSibling(ctx, root, relative, parent, name)
		if err != nil {
			return err
		}
		if !entry.Exists {
			return nil
		}
		if !mutationProofMatches(entry, expected) {
			return ErrSourceChanged
		}
		if err := removeMutationLeaf(parent, name, entry.Kind); err != nil {
			return err
		}
		return unix.Fsync(parent)
	}
	proof := journal.Active
	switch action.Kind {
	case MutationAtomicWriteFile, MutationCreateDirectory, MutationCreateSymlink:
		if err := cleanup(roots[action.Path.Root], action.Path.Relative, proof.StageName, proof.Stage); err != nil {
			return err
		}
		return cleanup(roots[action.Path.Root], action.Path.Relative, proof.TombName, proof.Target)
	case MutationRemove:
		return cleanup(roots[action.Path.Root], action.Path.Relative, proof.TombName, proof.Source)
	case MutationRename:
		if action.From == nil {
			return errors.New("sourceauthority: terminal rename proof lost its source")
		}
		if err := cleanup(roots[action.From.Root], action.From.Relative, proof.StageName, proof.Source); err != nil {
			return err
		}
		return cleanup(roots[action.Path.Root], action.Path.Relative, proof.TombName, proof.Target)
	default:
		return errors.New("sourceauthority: terminal proof has an unknown mutation action")
	}
}
