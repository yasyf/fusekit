package sourceauthority

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

const snapshotScanPageSize = 256

const maxMaterializationPayloadBytes = 64 << 10

const terminalStageCleanupTimeout = 5 * time.Second

func (r *Runtime) reconcile(ctx context.Context) (resultErr error) {
	defer func() {
		if resultErr == nil || errors.Is(resultErr, ErrQuarantined) || !terminalAuthorityError(resultErr) {
			return
		}
		quarantineErr := r.catalog.QuarantineSourceObserver(context.WithoutCancel(ctx), r.authority, resultErr.Error())
		resultErr = errors.Join(fmt.Errorf("%w: %v", ErrQuarantined, resultErr), quarantineErr)
	}()
	if err := r.recoverCommittedSourceDriverReceipts(ctx); err != nil {
		return fmt.Errorf("sourceauthority: recover committed source-driver receipt: %w", err)
	}
	if err := r.recoverPendingPhysicalDriverStage(ctx); err != nil {
		return fmt.Errorf("sourceauthority: recover pending source-driver stage: %w", err)
	}
	pending, err := r.catalog.PendingSourcePublicationStage(ctx, r.authority)
	if err != nil {
		return fmt.Errorf("sourceauthority: load pending publication before observer fence: %w", err)
	}
	if pending != nil {
		if err := r.recoverPendingPublicationStage(ctx, *pending); err != nil {
			return fmt.Errorf("sourceauthority: recover pending publication before observer fence: %w", err)
		}
	}
	state, err := r.loadSourceObserverFence(ctx, r.authority)
	if err != nil {
		if errors.Is(err, catalog.ErrIntegrity) {
			_ = r.catalog.QuarantineSourceObserver(context.WithoutCancel(ctx), r.authority, err.Error())
			return fmt.Errorf("%w: load durable observer: %v", ErrQuarantined, err)
		}
		return fmt.Errorf("sourceauthority: load durable observer: %w", err)
	}
	if state.Stream.Mode == catalog.SourceObserverQuarantined {
		return fmt.Errorf("%w: %s", ErrQuarantined, state.Stream.Quarantine)
	}
	recoveredPriorGeneration, err := r.recoverPriorGenerationMutationLiabilities(ctx)
	if err != nil {
		return err
	}
	if recoveredPriorGeneration {
		state, err = r.loadSourceObserverFence(ctx, r.authority)
		if err != nil {
			return err
		}
	}
	if err := r.cleanupPublishedMutationRepairs(ctx); err != nil {
		return err
	}
	if err := r.cleanupTerminalMutationProofs(ctx); err != nil {
		return err
	}
	if state.Stream.Mode == catalog.SourceObserverStreamResetRequired {
		if err := r.refreshDiscontinuousStream(ctx); err != nil {
			return err
		}
		state, err = r.loadSourceObserverFence(ctx, r.authority)
		if err != nil {
			return err
		}
	}
	if state.Stream.Mode == catalog.SourceObserverSnapshotRequired {
		var repairErr error
		for range r.attempts {
			repairErr = r.repairSnapshot(ctx, state)
			if repairErr == nil {
				break
			}
			if !errors.Is(repairErr, ErrSourceChanged) {
				return fmt.Errorf("sourceauthority: repair source snapshot: %w", repairErr)
			}
			state, err = r.loadSourceObserverFence(ctx, r.authority)
			if err != nil {
				return err
			}
		}
		if repairErr != nil {
			return fmt.Errorf("%w: snapshot did not stabilize after %d attempts", ErrSourceChanged, r.attempts)
		}
		if err := r.cleanupPublishedMutationRepairs(ctx); err != nil {
			return err
		}
		if err := r.cleanupTerminalMutationProofs(ctx); err != nil {
			return err
		}
	}
	for {
		if err := r.cleanupPublishedMutationRepairs(ctx); err != nil {
			return err
		}
		if err := r.cleanupTerminalMutationProofs(ctx); err != nil {
			return err
		}
		state, err = r.loadSourceObserverFence(ctx, r.authority)
		if err != nil {
			return err
		}
		pending, err = r.catalog.PendingSourcePublicationStage(ctx, r.authority)
		if err != nil {
			return err
		}
		if pending != nil {
			if err := r.recoverPendingPublicationStage(ctx, *pending); err != nil {
				return err
			}
			continue
		}
		if state.Stream.Mode == catalog.SourceObserverStreamResetRequired {
			if err := r.refreshDiscontinuousStream(ctx); err != nil {
				return err
			}
			continue
		}
		if state.Stream.Mode != catalog.SourceObserverIncremental {
			return nil
		}
		next, err := r.catalog.SourceObserverNextInbox(ctx, r.authority, state.Stream.LastApplied)
		if err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		if next.Sequence != state.Stream.LastApplied+1 || next.PredecessorSequence != state.Stream.LastApplied {
			if err := r.catalog.RequireSourceObserverSnapshot(ctx, r.authority); err != nil {
				return err
			}
			return ErrSnapshotRequired
		}
		if err := r.applyInbox(ctx, state, *next); err != nil {
			if errors.Is(err, ErrSourceChanged) {
				return err
			}
			return err
		}
	}
}

func (r *Runtime) recoverPriorGenerationMutationLiabilities(ctx context.Context) (bool, error) {
	needsSnapshot := false
	err := r.eachSourceMutationExpectation(ctx, func(expectation catalog.SourceMutationExpectationRecord) error {
		envelope, err := decodeMutationEnvelope(expectation)
		if err != nil {
			return err
		}
		if envelope.Fence.AuthorityGeneration == r.fleetGeneration {
			return nil
		}
		if envelope.Fence.AuthorityGeneration > r.fleetGeneration {
			return fmt.Errorf("%w: source mutation expectation is from a future authority generation", ErrQuarantined)
		}
		if err := r.settleMutationExpectationFromInspection(ctx, expectation, envelope); err != nil {
			return err
		}
		needsSnapshot = needsSnapshot || expectation.State != catalog.SourceMutationExpectationRepairPublished
		return nil
	})
	if err != nil || !needsSnapshot {
		return false, err
	}
	if err := r.catalog.RequireSourceObserverSnapshot(ctx, r.authority); err != nil {
		return false, fmt.Errorf("sourceauthority: fence prior-generation source mutation for snapshot repair: %w", err)
	}
	return true, nil
}

func (r *Runtime) cleanupPublishedMutationRepairs(ctx context.Context) error {
	return r.eachSourceMutationExpectation(ctx, func(expectation catalog.SourceMutationExpectationRecord) error {
		if expectation.State != catalog.SourceMutationExpectationRepairPublished {
			return nil
		}
		envelope, err := decodeMutationEnvelope(expectation)
		if err != nil {
			return err
		}
		if err := r.settleMutationExpectationFromInspection(ctx, expectation, envelope); err != nil {
			return err
		}
		if err := r.catalog.CompleteSourceMutationRepair(ctx, r.authority, expectation.Operation); err != nil {
			return err
		}
		return nil
	})
}

func (r *Runtime) settleMutationExpectationFromInspection(
	ctx context.Context,
	expectation catalog.SourceMutationExpectationRecord,
	envelope mutationEnvelope,
) error {
	request := MutationInspectionRequest{
		Authority: expectation.Authority, AuthorityGeneration: envelope.Fence.AuthorityGeneration,
		Operation: expectation.Operation, ExpectationDigest: Fingerprint(expectation.Digest),
	}
	inspection, err := r.executor.InspectMutation(ctx, request)
	if err != nil {
		return fmt.Errorf("sourceauthority: inspect exact source mutation liability: %w", err)
	}
	switch inspection.State {
	case MutationInspectionNotFound:
		if len(expectation.Receipt) != 0 {
			return fmt.Errorf("%w: catalog mutation receipt has no exact worker operation", ErrQuarantined)
		}
		return nil
	case MutationInspectionActiveUnapplied:
		if len(expectation.Receipt) != 0 {
			return fmt.Errorf("%w: unapplied worker mutation conflicts with catalog receipt", ErrQuarantined)
		}
		if err := r.executor.AbandonMutation(
			ctx, request.Authority, request.AuthorityGeneration, request.Operation,
		); err != nil {
			return fmt.Errorf("sourceauthority: abandon unapplied source mutation liability: %w", err)
		}
		return nil
	case MutationInspectionApplied:
		if inspection.Receipt == nil {
			return fmt.Errorf("%w: applied mutation inspection lost its receipt", ErrQuarantined)
		}
		receipt, encoded, err := recoveredDurableMutationReceipt(expectation, envelope, *inspection.Receipt)
		if err != nil {
			return err
		}
		if len(expectation.Receipt) == 0 {
			if expectation.State != catalog.SourceMutationExpectationPlanned &&
				expectation.State != catalog.SourceMutationExpectationRepairRequired &&
				expectation.State != catalog.SourceMutationExpectationRepairPublished {
				return fmt.Errorf("%w: applied worker mutation crossed snapshot repair without a catalog receipt", ErrQuarantined)
			}
			if err := r.catalog.RecoverSourceMutationExpectationReceipt(
				ctx, expectation.Authority, expectation.Operation, encoded,
			); err != nil {
				return err
			}
		} else {
			persisted, err := decodeDurableMutationReceipt(expectation, envelope)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(persisted, receipt) {
				return fmt.Errorf("%w: worker receipt conflicts with catalog mutation receipt", ErrQuarantined)
			}
		}
		if err := r.executor.AcknowledgeMutation(
			ctx, receipt.Authority, receipt.AuthorityGeneration, receipt.Operation, receipt.Digest,
		); err != nil {
			return fmt.Errorf("sourceauthority: acknowledge recovered source mutation: %w", err)
		}
		return nil
	case MutationInspectionConsumed:
		return fmt.Errorf("%w: consumed worker mutation still has a catalog expectation", ErrQuarantined)
	case MutationInspectionTerminal:
		if inspection.Terminal == nil {
			return fmt.Errorf("%w: terminal mutation inspection lost its proof", ErrQuarantined)
		}
		proof := *inspection.Terminal
		if proof.Outcome == MutationAbandoned {
			if len(expectation.Receipt) != 0 || proof.Digest != (Fingerprint{}) {
				return fmt.Errorf("%w: abandoned worker mutation conflicts with catalog receipt", ErrQuarantined)
			}
			return nil
		}
		receipt, err := decodeDurableMutationReceipt(expectation, envelope)
		if err != nil {
			return err
		}
		if proof.Outcome != MutationAcknowledged || proof.Digest != receipt.Digest {
			return fmt.Errorf("%w: terminal worker proof conflicts with catalog receipt", ErrQuarantined)
		}
		if err := r.executor.AcknowledgeMutation(
			ctx, receipt.Authority, receipt.AuthorityGeneration, receipt.Operation, receipt.Digest,
		); err != nil {
			return fmt.Errorf("sourceauthority: replay source mutation acknowledgement: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown exact mutation inspection state", ErrQuarantined)
	}
}

func recoveredDurableMutationReceipt(
	expectation catalog.SourceMutationExpectationRecord,
	envelope mutationEnvelope,
	worker MutationReceipt,
) (durableMutationReceipt, []byte, error) {
	if worker.OperationID != expectation.Operation || worker.Digest == (Fingerprint{}) ||
		len(worker.Effects) != len(envelope.Plan.Effects) {
		return durableMutationReceipt{}, nil, fmt.Errorf("%w: recovered worker mutation receipt is incomplete", ErrQuarantined)
	}
	receipt := durableMutationReceipt{
		Authority: expectation.Authority, AuthorityGeneration: envelope.Fence.AuthorityGeneration,
		Operation: expectation.Operation, Origin: expectation.Origin,
		Start: envelope.Start, End: envelope.Start, Digest: worker.Digest,
		Effects: make([]observedEffect, len(worker.Effects)),
	}
	for index, entry := range worker.Effects {
		effect := envelope.Plan.Effects[index]
		after := physicalState(entry)
		if entry.Root != effect.Path.Root || entry.Relative != effect.Path.Relative ||
			(effect.Outcome == MutationAbsent && after.Exists) ||
			(effect.Outcome == MutationPresent && (!after.Exists || after.Kind != effect.Kind)) {
			return durableMutationReceipt{}, nil, fmt.Errorf("%w: recovered worker receipt violates its semantic plan", ErrQuarantined)
		}
		receipt.Effects[index] = observedEffect{Path: effect.Path, Before: effect.Before, After: after}
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return durableMutationReceipt{}, nil, err
	}
	return receipt, encoded, nil
}

func (r *Runtime) cleanupTerminalMutationProofs(ctx context.Context) error {
	var after catalog.MutationID
	for {
		page, err := r.executor.MutationTerminalProofPage(
			ctx, r.authority, after, MutationTerminalProofPageLimit,
		)
		if err != nil {
			return fmt.Errorf("sourceauthority: list terminal source mutations: %w", err)
		}
		for index, proof := range page.Proofs {
			if validateMutationTerminalProof(proof) != nil || proof.Authority != r.authority ||
				bytes.Compare(proof.Operation[:], after[:]) <= 0 ||
				(index > 0 && bytes.Compare(proof.Operation[:], page.Proofs[index-1].Operation[:]) <= 0) {
				return fmt.Errorf("%w: invalid or unordered mutation terminal proof", ErrQuarantined)
			}
			expectation, expectationErr := r.catalog.SourceMutationExpectation(ctx, r.authority, proof.Operation)
			if expectationErr == nil {
				envelope, err := decodeMutationEnvelope(expectation)
				if err != nil {
					return err
				}
				if proof.AuthorityGeneration != envelope.Fence.AuthorityGeneration {
					return fmt.Errorf("%w: mutation proof generation does not match its catalog expectation", ErrQuarantined)
				}
				if len(expectation.Receipt) == 0 {
					if proof.Outcome != MutationAbandoned || proof.Digest != (Fingerprint{}) {
						return fmt.Errorf("%w: mutation abandonment proof does not match its catalog expectation", ErrQuarantined)
					}
				} else {
					receipt, err := decodeDurableMutationReceipt(expectation, envelope)
					if err != nil {
						return err
					}
					if proof.Outcome != MutationAcknowledged || proof.Digest != receipt.Digest {
						return fmt.Errorf("%w: mutation acknowledgement proof does not match its catalog receipt", ErrQuarantined)
					}
				}
				continue
			}
			if !errors.Is(expectationErr, catalog.ErrNotFound) {
				return expectationErr
			}
			if err := r.executor.ForgetMutation(ctx, r.authority, proof); err != nil {
				return fmt.Errorf("sourceauthority: forget settled source mutation: %w", err)
			}
		}
		if page.Next == (catalog.MutationID{}) {
			if page.More || len(page.Proofs) != 0 {
				return fmt.Errorf("%w: mutation terminal proof page lost its cursor", ErrQuarantined)
			}
			return nil
		}
		if bytes.Compare(page.Next[:], after[:]) <= 0 ||
			(len(page.Proofs) != 0 && bytes.Compare(page.Proofs[len(page.Proofs)-1].Operation[:], page.Next[:]) > 0) {
			return fmt.Errorf("%w: non-monotonic mutation terminal proof page", ErrQuarantined)
		}
		if !page.More {
			return nil
		}
		after = page.Next
	}
}

func (r *Runtime) eachSourceMutationExpectation(
	ctx context.Context,
	visit func(catalog.SourceMutationExpectationRecord) error,
) error {
	var after catalog.MutationID
	for {
		page, err := r.catalog.SourceMutationExpectationsPage(
			ctx, r.authority, after, catalog.SourceMutationExpectationPageLimit,
		)
		if err != nil {
			return err
		}
		for _, record := range page.Records {
			if err := visit(record); err != nil {
				return err
			}
		}
		if page.Next == (catalog.MutationID{}) {
			return nil
		}
		if bytes.Compare(page.Next[:], after[:]) <= 0 {
			return fmt.Errorf("%w: non-monotonic source mutation expectation page", ErrQuarantined)
		}
		after = page.Next
	}
}

func (r *Runtime) repairSnapshot(ctx context.Context, state catalog.SourceObserverState) (resultErr error) {
	stream := r.currentStream()
	start, err := stream.Flush(ctx)
	if err != nil {
		return fmt.Errorf("sourceauthority: flush snapshot start: %w", err)
	}
	state, err = r.loadSourceObserverFence(ctx, r.authority)
	if err != nil {
		return err
	}
	if !catalogCheckpointsCover(state.Checkpoints, start) {
		return ErrSourceChanged
	}
	startReceived := state.Stream.LastReceived
	snapshot, err := newSnapshotID()
	if err != nil {
		return err
	}
	if err := r.catalog.BeginSourceSnapshotStage(ctx, r.authority, snapshot); err != nil {
		return fmt.Errorf("sourceauthority: begin snapshot stage: %w", err)
	}
	snapshotOwned := true
	defer func() {
		if snapshotOwned {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalStageCleanupTimeout)
			defer cancel()
			resultErr = errors.Join(resultErr, r.catalog.AbortSourceSnapshotStage(cleanupCtx, r.authority, snapshot))
		}
	}()
	scan, err := r.executor.BeginScan(ctx, r.currentRoots())
	if err != nil {
		return fmt.Errorf("sourceauthority: begin source scan: %w", err)
	}
	scanClosed := false
	defer func() {
		if !scanClosed {
			resultErr = errors.Join(resultErr, scan.Close())
		}
	}()
	var last indexKey
	for {
		page, err := scan.Next(ctx, snapshotScanPageSize)
		if err != nil {
			return fmt.Errorf("sourceauthority: scan source roots: %w", err)
		}
		if err := validateScanPage(page, state.Roots, last); err != nil {
			return err
		}
		if len(page.Entries) > 0 {
			records := make([]catalog.SourcePhysicalIndexRecord, len(page.Entries))
			for index, entry := range page.Entries {
				record, err := physicalRecord(r.authority, IndexedEntry{Physical: entry})
				if err != nil {
					return err
				}
				records[index] = record
			}
			tail := records[len(records)-1]
			if err := r.catalog.AppendSourceSnapshotStagePage(ctx, r.authority, snapshot, catalog.SourceSnapshotPage{
				Records: records,
				Next:    catalog.SourceIndexLocator{RootID: tail.RootID, Relative: tail.Relative},
			}); err != nil {
				return fmt.Errorf("sourceauthority: append snapshot scan page: %w", err)
			}
			last = indexKey{root: page.Entries[len(page.Entries)-1].Root, relative: page.Entries[len(page.Entries)-1].Relative}
		}
		if page.Next == "" {
			break
		}
		if len(page.Entries) == 0 {
			return fmt.Errorf("%w: snapshot scan cursor did not advance", ErrInvalidPlan)
		}
	}
	if err := scan.Close(); err != nil {
		return fmt.Errorf("sourceauthority: close source scan: %w", err)
	}
	scanClosed = true
	end, err := stream.Flush(ctx)
	if err != nil {
		return fmt.Errorf("sourceauthority: flush snapshot end: %w", err)
	}
	latest, err := r.loadSourceObserverFence(ctx, r.authority)
	if err != nil {
		return err
	}
	if !checkpointsEqual(start, end) || !catalogCheckpointsCover(latest.Checkpoints, end) ||
		latest.Stream.LastReceived != startReceived {
		return ErrSourceChanged
	}
	fence := r.fence(latest, end)
	view := snapshotView{
		fence: fence, roots: r.currentRoots(), tenants: r.currentTenants(),
		catalog: r.catalog, authority: r.authority, snapshot: snapshot,
	}
	watermark, err := r.catalog.SourceWatermark(ctx, r.authority)
	if err != nil {
		return err
	}
	change, operation, err := newCausalIDs()
	if err != nil {
		return err
	}
	fenceDigest, err := digestJSON(fence)
	if err != nil {
		return err
	}
	identity := catalog.SourceSnapshotIdentity{
		Authority: r.authority, AuthorityGeneration: r.fleetGeneration,
		Snapshot: snapshot, FenceDigest: fenceDigest,
		Change: causal.ChangeSet{
			SourceAuthority: r.authority, SourceRevision: watermark + 1,
			ChangeID: change, OperationID: operation, Cause: causal.CauseExternalUnattributed,
		},
	}
	if err := r.catalog.BeginSourceSnapshotPublication(ctx, identity); err != nil {
		return fmt.Errorf("sourceauthority: begin snapshot publication: %w", err)
	}
	visited := make(map[SnapshotPlanCursor]struct{})
	stageState := newSnapshotStageState()
	var cursor SnapshotPlanCursor
	var ref catalog.SourceSnapshotStageRef
	for {
		if _, duplicate := visited[cursor]; duplicate {
			return fmt.Errorf("%w: snapshot plan cursor cycle", ErrInvalidPlan)
		}
		visited[cursor] = struct{}{}
		page, err := r.policy.PlanSnapshot(ctx, view, cursor, SnapshotPlanPageLimit)
		if err != nil {
			return fmt.Errorf("sourceauthority: plan snapshot page: %w", err)
		}
		ref, err = r.stageSnapshotPlanPage(ctx, view, identity, cursor, page, stageState)
		if err != nil {
			return fmt.Errorf("sourceauthority: stage snapshot plan page: %w", err)
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	settlement := catalog.SourceSnapshotSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: r.authority, Stream: latest.Stream.Stream, RootEpoch: latest.Stream.RootEpoch,
			Through: latest.Stream.LastReceived, Operation: ref.Operation,
		},
		Snapshot: ref, MismatchAllActive: true,
	}
	if err := r.promoteSnapshotWithHandoff(ctx, ref, settlement, func() {
		snapshotOwned = false
	}); err != nil {
		return fmt.Errorf("sourceauthority: promote snapshot: %w", err)
	}
	return nil
}

func (r *Runtime) applyInbox(ctx context.Context, state catalog.SourceObserverState, record catalog.SourceObserverInboxRecord) error {
	var batch EventBatch
	if err := json.Unmarshal(record.Payload, &batch); err != nil {
		_ = r.catalog.QuarantineSourceObserver(context.WithoutCancel(ctx), r.authority, "corrupt event batch payload")
		return fmt.Errorf("%w: decode event batch", ErrQuarantined)
	}
	if err := validateBatch(r.currentRoots(), batch); err != nil {
		return err
	}
	view, index, deletes, refreshed, err := r.incrementalView(ctx, state, batch, InboxSequence(record.Sequence))
	if err != nil {
		return err
	}
	correlation, err := r.correlateMutationEcho(ctx, record.Sequence, batch, view)
	if err != nil {
		return err
	}
	watermark, err := r.catalog.SourceWatermark(ctx, r.authority)
	if err != nil {
		return err
	}
	var publications []sourcePublication
	var bindings []catalog.SourceAuthorityBindingRecord
	allWork := append(append([]EventBatch(nil), correlation.external...), correlation.provider...)
	if err := r.extendIncrementalView(ctx, &view, allWork, refreshed); err != nil {
		return err
	}
	externalPlan, err := r.planEventBatches(ctx, &view, correlation.external, refreshed)
	if err != nil {
		return err
	}
	providerPlan, err := r.planEventBatches(ctx, &view, correlation.provider, refreshed)
	if err != nil {
		return err
	}
	if len(correlation.provider) != 0 && len(correlation.external) != 0 {
		externalPlan, providerPlan, err = partitionCausalPlans(externalPlan, providerPlan, correlation.externalPaths, correlation.providerPaths)
		if err != nil {
			return err
		}
	}
	applyPlan := func(plan DeltaPlan, origin *catalog.CausalOrigin, operation *catalog.MutationID) error {
		if len(plan.Reads) == 0 && len(plan.Deletes) == 0 {
			return nil
		}
		materialized, err := r.materialize(ctx, view, plan.Reads)
		if err != nil {
			return err
		}
		materializedOwned := true
		defer func() {
			if materializedOwned {
				_ = closeMaterializations(materialized)
			}
		}()
		priorBindings, err := r.bindingsForMaterializations(ctx, materialized)
		if err != nil {
			return err
		}
		changed, suppressed := suppressUnchanged(materialized, priorBindings)
		if err := closeMaterializations(suppressed); err != nil {
			return err
		}
		materialized = changed
		index, err = ensureIndexRecordsForReads(r.authority, index, view, plan.Reads)
		if err != nil {
			return err
		}
		index, err = bindIndexRecords(index, plan.Reads)
		if err != nil {
			return err
		}
		if len(changed) == 0 && len(plan.Deletes) == 0 {
			materializedOwned = false
			return nil
		}
		materializedOwned = false
		publication, settled, err := r.buildPublication(ctx, catalog.SourceDelta, watermark+causal.Revision(len(publications)),
			plan.AffectedKeys, plan.Roots, changed, plan.Deletes, origin, operation)
		if err != nil {
			return err
		}
		publications = append(publications, publication)
		bindings = append(bindings, settled...)
		return nil
	}
	if err := applyPlan(externalPlan, nil, nil); err != nil {
		return err
	}
	if len(correlation.provider) != 0 {
		operation := correlation.matched[0]
		if err := applyPlan(providerPlan, correlation.origin, &operation); err != nil {
			return err
		}
	}
	_, settlementOperation, err := newCausalIDs()
	if err != nil {
		return err
	}
	if len(publications) != 0 {
		settlementOperation = publications[0].Change.OperationID
	}
	settlement := stagedObserverSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: r.authority, Stream: state.Stream.Stream, RootEpoch: state.Stream.RootEpoch,
			Through: record.Sequence, Operation: settlementOperation,
		},
		Index: index, Deletes: deletes, Bindings: bindings,
		MatchedMutations: correlation.matched, MismatchedMutations: correlation.mismatched,
	}
	return r.applyStagedPublications(ctx, publications, settlement)
}

func (r *Runtime) planEventBatches(
	ctx context.Context,
	view *authorityView,
	batches []EventBatch,
	refreshed map[indexKey]struct{},
) (DeltaPlan, error) {
	result := DeltaPlan{Fence: view.Fence()}
	inputCache := make(map[LogicalID][]PathRef)
	merger := newDeltaPlanMerger(&result)
	for _, batch := range batches {
		plan, err := r.policy.PlanDelta(ctx, view, batch)
		if err != nil {
			if errors.Is(err, ErrSnapshotRequired) {
				if transitionErr := r.catalog.RequireSourceObserverSnapshot(ctx, r.authority); transitionErr != nil {
					return DeltaPlan{}, errors.Join(err, transitionErr)
				}
				r.signal()
			}
			return DeltaPlan{}, fmt.Errorf("sourceauthority: plan delta: %w", err)
		}
		plan.Reads, err = r.expandLogicalInputs(ctx, plan.Reads, inputCache)
		if err != nil {
			return DeltaPlan{}, err
		}
		if err := r.extendIncrementalViewForReads(ctx, view, plan.Reads, refreshed); err != nil {
			return DeltaPlan{}, err
		}
		if err := validateDeltaPlan(plan, *view, batch); err != nil {
			return DeltaPlan{}, err
		}
		if err := merger.merge(plan); err != nil {
			return DeltaPlan{}, err
		}
	}
	merger.finish()
	return result, nil
}

func (r *Runtime) expandLogicalInputs(
	ctx context.Context,
	reads []MaterializationRequest,
	cache map[LogicalID][]PathRef,
) ([]MaterializationRequest, error) {
	var uncached []LogicalID
	pending := make(map[LogicalID]struct{})
	for _, read := range reads {
		if _, found := cache[read.Logical]; found {
			continue
		}
		if _, found := pending[read.Logical]; found {
			continue
		}
		pending[read.Logical] = struct{}{}
		uncached = append(uncached, read.Logical)
	}
	bindings, err := r.sourceAuthorityBindings(ctx, uncached)
	if err != nil {
		return nil, err
	}
	for _, logical := range uncached {
		binding, found := bindings[logical]
		if !found {
			cache[logical] = nil
			continue
		}
		var inputs []PathRef
		var after catalog.SourceIndexLocator
		for {
			page, err := r.catalog.SourceObserverBindingIndexPage(
				ctx, r.authority, binding.SourceKey, after, catalog.SourcePhysicalIndexPageLimit,
			)
			if err != nil {
				return nil, err
			}
			for _, record := range page.Records {
				inputs = append(inputs, PathRef{Root: RootID(record.RootID), Relative: record.Relative})
			}
			if page.Next == (catalog.SourceIndexLocator{}) {
				break
			}
			if compareSourceIndexLocator(page.Next, after) <= 0 {
				return nil, fmt.Errorf("%w: non-monotonic source binding page", ErrQuarantined)
			}
			after = page.Next
		}
		cache[logical] = inputs
	}
	result := make([]MaterializationRequest, len(reads))
	for index, read := range reads {
		result[index] = cloneMaterializationRequest(read)
		seen := make(map[indexKey]struct{}, len(result[index].Inputs)+len(cache[read.Logical]))
		for _, input := range result[index].Inputs {
			seen[indexKey{root: input.Root, relative: input.Relative}] = struct{}{}
		}
		for _, input := range cache[read.Logical] {
			key := indexKey{root: input.Root, relative: input.Relative}
			if _, found := seen[key]; found {
				continue
			}
			seen[key] = struct{}{}
			result[index].Inputs = append(result[index].Inputs, input)
		}
		slices.SortFunc(result[index].Inputs, func(left, right PathRef) int {
			if left.Root != right.Root {
				return compareString(string(left.Root), string(right.Root))
			}
			return compareString(left.Relative, right.Relative)
		})
	}
	return result, nil
}

func (r *Runtime) extendIncrementalViewForReads(
	ctx context.Context,
	view *authorityView,
	reads []MaterializationRequest,
	refreshed map[indexKey]struct{},
) error {
	var locators []catalog.SourceIndexLocator
	seen := make(map[indexKey]struct{})
	for _, read := range reads {
		for _, input := range read.Inputs {
			key := indexKey{root: input.Root, relative: input.Relative}
			if _, found := seen[key]; found {
				continue
			}
			seen[key] = struct{}{}
			locators = append(locators, catalog.SourceIndexLocator{RootID: string(input.Root), Relative: input.Relative})
		}
	}
	records, err := r.sourcePhysicalIndexRecords(ctx, locators)
	if err != nil {
		return err
	}
	for _, record := range records {
		key := indexKey{root: RootID(record.RootID), relative: record.Relative}
		if _, found := refreshed[key]; found {
			continue
		}
		if _, found := view.entries[key]; found {
			continue
		}
		var physical PhysicalEntry
		if err := json.Unmarshal(record.Payload, &physical); err != nil {
			return fmt.Errorf("%w: corrupt source physical index", ErrQuarantined)
		}
		entry := IndexedEntry{Physical: physical}
		for _, logical := range record.Logical {
			entry.Logical = append(entry.Logical, LogicalID(logical))
		}
		view.entries[key] = entry
	}
	return nil
}

type deltaPlanMerger struct {
	target   *DeltaPlan
	affected map[causal.LogicalKey]struct{}
	roots    map[catalog.TenantID]TenantRoot
	reads    map[LogicalID]MaterializationRequest
	deletes  map[LogicalID]Delete
}

func newDeltaPlanMerger(target *DeltaPlan) *deltaPlanMerger {
	merger := &deltaPlanMerger{
		target: target, affected: make(map[causal.LogicalKey]struct{}, len(target.AffectedKeys)),
		roots:   make(map[catalog.TenantID]TenantRoot, len(target.Roots)),
		reads:   make(map[LogicalID]MaterializationRequest, len(target.Reads)),
		deletes: make(map[LogicalID]Delete, len(target.Deletes)),
	}
	for _, key := range target.AffectedKeys {
		merger.affected[key] = struct{}{}
	}
	for _, root := range target.Roots {
		merger.roots[root.Tenant] = root
	}
	for _, read := range target.Reads {
		merger.reads[read.Logical] = read
	}
	for _, deletion := range target.Deletes {
		merger.deletes[deletion.Logical] = deletion
	}
	return merger
}

func (m *deltaPlanMerger) merge(next DeltaPlan) error {
	for _, key := range next.AffectedKeys {
		if _, found := m.affected[key]; found {
			continue
		}
		m.affected[key] = struct{}{}
		m.target.AffectedKeys = append(m.target.AffectedKeys, key)
	}
	for _, root := range next.Roots {
		if current, found := m.roots[root.Tenant]; found {
			if current != root {
				return fmt.Errorf("%w: conflicting tenant root across event segments", ErrInvalidPlan)
			}
			continue
		}
		m.roots[root.Tenant] = root
		m.target.Roots = append(m.target.Roots, root)
	}
	for _, read := range next.Reads {
		if current, found := m.reads[read.Logical]; found {
			if !reflect.DeepEqual(current, read) {
				return fmt.Errorf("%w: conflicting logical read across event segments", ErrInvalidPlan)
			}
			continue
		}
		copy := cloneMaterializationRequest(read)
		m.reads[read.Logical] = copy
		m.target.Reads = append(m.target.Reads, copy)
	}
	for _, deletion := range next.Deletes {
		if current, found := m.deletes[deletion.Logical]; found {
			if !reflect.DeepEqual(current, deletion) {
				return fmt.Errorf("%w: conflicting logical delete across event segments", ErrInvalidPlan)
			}
			continue
		}
		m.deletes[deletion.Logical] = deletion
		m.target.Deletes = append(m.target.Deletes, deletion)
	}
	return nil
}

func (m *deltaPlanMerger) finish() {
	slices.Sort(m.target.AffectedKeys)
}

func partitionCausalPlans(
	external DeltaPlan,
	provider DeltaPlan,
	externalPaths map[indexKey]struct{},
	providerPaths map[indexKey]struct{},
) (DeltaPlan, DeltaPlan, error) {
	externalResult := DeltaPlan{Fence: external.Fence, Roots: append([]TenantRoot(nil), external.Roots...), AffectedKeys: append([]causal.LogicalKey(nil), external.AffectedKeys...)}
	providerResult := DeltaPlan{Fence: provider.Fence, Roots: append([]TenantRoot(nil), provider.Roots...), AffectedKeys: append([]causal.LogicalKey(nil), provider.AffectedKeys...)}
	type readCandidate struct {
		request            MaterializationRequest
		external, provider bool
	}
	reads := make(map[LogicalID]readCandidate)
	for _, pair := range []struct {
		plan     DeltaPlan
		provider bool
	}{{external, false}, {provider, true}} {
		for _, read := range pair.plan.Reads {
			candidate, found := reads[read.Logical]
			if found && !reflect.DeepEqual(candidate.request, read) {
				return DeltaPlan{}, DeltaPlan{}, fmt.Errorf("%w: causal partitions disagree on one logical read", ErrInvalidPlan)
			}
			candidate.request = cloneMaterializationRequest(read)
			candidate.provider = candidate.provider || pair.provider
			candidate.external = candidate.external || !pair.provider
			reads[read.Logical] = candidate
		}
	}
	for _, candidate := range reads {
		hasExternal, hasProvider := false, false
		for _, input := range candidate.request.Inputs {
			key := indexKey{root: input.Root, relative: input.Relative}
			_, externalChanged := externalPaths[key]
			_, providerChanged := providerPaths[key]
			hasExternal = hasExternal || externalChanged
			hasProvider = hasProvider || providerChanged
		}
		if hasProvider && !hasExternal && !candidate.external {
			providerResult.Reads = append(providerResult.Reads, candidate.request)
		} else {
			externalResult.Reads = append(externalResult.Reads, candidate.request)
			if candidate.provider {
				externalResult.AffectedKeys = append(externalResult.AffectedKeys, provider.AffectedKeys...)
			}
		}
	}
	type deleteCandidate struct {
		value              Delete
		external, provider bool
	}
	deletes := make(map[LogicalID]deleteCandidate)
	for _, pair := range []struct {
		plan     DeltaPlan
		provider bool
	}{{external, false}, {provider, true}} {
		for _, deletion := range pair.plan.Deletes {
			candidate, found := deletes[deletion.Logical]
			if found && !reflect.DeepEqual(candidate.value, deletion) {
				return DeltaPlan{}, DeltaPlan{}, fmt.Errorf("%w: causal partitions disagree on one logical delete", ErrInvalidPlan)
			}
			candidate.value = deletion
			candidate.provider = candidate.provider || pair.provider
			candidate.external = candidate.external || !pair.provider
			deletes[deletion.Logical] = candidate
		}
	}
	for _, candidate := range deletes {
		if candidate.provider && !candidate.external {
			providerResult.Deletes = append(providerResult.Deletes, candidate.value)
		} else {
			externalResult.Deletes = append(externalResult.Deletes, candidate.value)
			if candidate.provider {
				externalResult.AffectedKeys = append(externalResult.AffectedKeys, provider.AffectedKeys...)
			}
		}
	}
	for _, result := range []*DeltaPlan{&externalResult, &providerResult} {
		slices.Sort(result.AffectedKeys)
		result.AffectedKeys = slices.Compact(result.AffectedKeys)
		slices.SortFunc(result.Reads, func(left, right MaterializationRequest) int {
			return compareString(string(left.Logical), string(right.Logical))
		})
		slices.SortFunc(result.Deletes, func(left, right Delete) int {
			return compareString(string(left.Logical), string(right.Logical))
		})
	}
	return externalResult, providerResult, nil
}

func (r *Runtime) extendIncrementalView(
	ctx context.Context,
	view *authorityView,
	batches []EventBatch,
	refreshed map[indexKey]struct{},
) error {
	var locators []catalog.SourceIndexLocator
	seen := make(map[indexKey]struct{})
	for _, batch := range batches {
		for _, event := range batch.Events {
			key := indexKey{root: event.Root, relative: event.Relative}
			if _, found := refreshed[key]; found {
				continue
			}
			if _, found := seen[key]; found {
				continue
			}
			seen[key] = struct{}{}
			locators = append(locators, catalog.SourceIndexLocator{RootID: string(key.root), Relative: key.relative})
		}
	}
	records, err := r.sourcePhysicalIndexRecords(ctx, locators)
	if err != nil {
		return err
	}
	for _, record := range records {
		var physical PhysicalEntry
		if err := json.Unmarshal(record.Payload, &physical); err != nil {
			return fmt.Errorf("%w: corrupt source physical index", ErrQuarantined)
		}
		entry := IndexedEntry{Physical: physical}
		for _, logical := range record.Logical {
			entry.Logical = append(entry.Logical, LogicalID(logical))
		}
		view.entries[indexKey{root: physical.Root, relative: physical.Relative}] = entry
	}
	return nil
}

func (r *Runtime) incrementalView(
	ctx context.Context,
	state catalog.SourceObserverState,
	batch EventBatch,
	sequence InboxSequence,
) (authorityView, []catalog.SourcePhysicalIndexRecord, []catalog.SourceIndexLocator, map[indexKey]struct{}, error) {
	paths := make(map[indexKey]struct{})
	for _, event := range batch.Events {
		paths[indexKey{root: event.Root, relative: event.Relative}] = struct{}{}
		if parent := parentRelative(event.Relative); parent != "" {
			paths[indexKey{root: event.Root, relative: parent}] = struct{}{}
		}
	}
	if err := r.eachSourceMutationExpectation(ctx, func(expectation catalog.SourceMutationExpectationRecord) error {
		if expectation.State != catalog.SourceMutationExpectationComplete || len(expectation.Receipt) == 0 {
			return nil
		}
		var receipt durableMutationReceipt
		if err := json.Unmarshal(expectation.Receipt, &receipt); err != nil {
			return fmt.Errorf("%w: corrupt mutation receipt", ErrQuarantined)
		}
		if sequence <= receipt.Start || sequence > receipt.End {
			return nil
		}
		for _, effect := range receipt.Effects {
			paths[indexKey{root: effect.Path.Root, relative: effect.Path.Relative}] = struct{}{}
			if parent := parentRelative(effect.Path.Relative); parent != "" {
				paths[indexKey{root: effect.Path.Root, relative: parent}] = struct{}{}
			}
		}
		return nil
	}); err != nil {
		return authorityView{}, nil, nil, nil, err
	}
	locators := make([]catalog.SourceIndexLocator, 0, len(paths))
	for key := range paths {
		locators = append(locators, catalog.SourceIndexLocator{RootID: string(key.root), Relative: key.relative})
	}
	records, err := r.sourcePhysicalIndexRecords(ctx, locators)
	if err != nil {
		return authorityView{}, nil, nil, nil, err
	}
	view, err := viewFromState(r.fence(state, checkpointsFromCatalog(state.Checkpoints)), r.currentRoots(), r.currentTenants(), records)
	if err != nil {
		return authorityView{}, nil, nil, nil, err
	}
	view.fence.Inbox = sequence
	rootMap := make(map[RootID]RootSpec)
	for _, root := range view.roots {
		rootMap[root.ID] = root
	}
	var upserts []catalog.SourcePhysicalIndexRecord
	var deletes []catalog.SourceIndexLocator
	keys := make([]indexKey, 0, len(paths))
	for key := range paths {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(left, right indexKey) int {
		if left.root != right.root {
			return compareString(string(left.root), string(right.root))
		}
		return compareString(left.relative, right.relative)
	})
	for _, key := range keys {
		entry, err := r.executor.Stat(ctx, rootMap[key.root], key.relative)
		if err != nil {
			return authorityView{}, nil, nil, nil, fmt.Errorf("sourceauthority: stat %s/%s: %w", key.root, key.relative, err)
		}
		if entry.Root != key.root || entry.Relative != key.relative {
			return authorityView{}, nil, nil, nil, fmt.Errorf("%w: stat escaped requested locator", ErrInvalidEvent)
		}
		prior, hadPrior := view.entries[key]
		if !entry.Exists {
			delete(view.entries, key)
			if hadPrior {
				deletes = append(deletes, catalog.SourceIndexLocator{RootID: string(key.root), Relative: key.relative})
			}
			continue
		}
		logical := prior.Logical
		identity, marshalErr := json.Marshal(entry.Identity)
		if marshalErr != nil {
			return authorityView{}, nil, nil, nil, marshalErr
		}
		if identityRecord, identityErr := r.catalog.SourcePhysicalIndexRecordByIdentity(ctx, r.authority, identity); identityErr == nil {
			oldKey := indexKey{root: RootID(identityRecord.RootID), relative: identityRecord.Relative}
			if oldKey != key {
				var oldEntry IndexedEntry
				if err := json.Unmarshal(identityRecord.Payload, &oldEntry.Physical); err != nil {
					return authorityView{}, nil, nil, nil, fmt.Errorf("%w: corrupt source physical index", ErrQuarantined)
				}
				for _, logical := range identityRecord.Logical {
					oldEntry.Logical = append(oldEntry.Logical, LogicalID(logical))
				}
				oldPhysical, err := r.executor.Stat(ctx, rootMap[oldKey.root], oldKey.relative)
				if err != nil {
					return authorityView{}, nil, nil, nil, err
				}
				if oldPhysical.Exists && oldPhysical.Identity == entry.Identity {
					return authorityView{}, nil, nil, nil, fmt.Errorf("%w: one physical identity has multiple live paths", ErrSnapshotRequired)
				}
				logical = append(logical, oldEntry.Logical...)
				slices.Sort(logical)
				logical = slices.Compact(logical)
				delete(view.entries, oldKey)
				deletes = append(deletes, catalog.SourceIndexLocator{RootID: string(oldKey.root), Relative: oldKey.relative})
			}
		} else if !errors.Is(identityErr, catalog.ErrNotFound) {
			return authorityView{}, nil, nil, nil, identityErr
		}
		view.entries[key] = IndexedEntry{Physical: entry, Logical: append([]LogicalID(nil), logical...)}
		record, err := physicalRecord(r.authority, view.entries[key])
		if err != nil {
			return authorityView{}, nil, nil, nil, err
		}
		upserts = append(upserts, record)
	}
	return view, upserts, deletes, paths, nil
}

func (r *Runtime) materialize(ctx context.Context, view authorityView, requests []MaterializationRequest) ([]Materialization, error) {
	return r.materializeFrom(ctx, view.fence, view.roots, view.tenants, requests, func(_ context.Context, input PathRef) (PhysicalEntry, bool, error) {
		indexed, found := view.Entry(input.Root, input.Relative)
		return indexed.Physical, found, nil
	})
}

func (r *Runtime) materializeSnapshot(ctx context.Context, view snapshotView, requests []MaterializationRequest) ([]Materialization, error) {
	var locators []catalog.SourceIndexLocator
	seen := make(map[indexKey]struct{})
	for _, request := range requests {
		for _, input := range request.Inputs {
			key := indexKey{root: input.Root, relative: input.Relative}
			if _, found := seen[key]; found {
				continue
			}
			seen[key] = struct{}{}
			locators = append(locators, catalog.SourceIndexLocator{
				RootID: string(input.Root), Relative: input.Relative,
			})
		}
	}
	records, err := sourceSnapshotStageEntries(ctx, r.catalog, r.authority, view.snapshot, locators)
	if err != nil {
		return nil, err
	}
	entries := make(map[indexKey]PhysicalEntry, len(records))
	for index, record := range records {
		if record == nil {
			continue
		}
		var entry PhysicalEntry
		if err := json.Unmarshal(record.Payload, &entry); err != nil {
			return nil, fmt.Errorf("%w: corrupt staged physical entry", ErrQuarantined)
		}
		locator := locators[index]
		entries[indexKey{root: RootID(locator.RootID), relative: locator.Relative}] = entry
	}
	return r.materializeFrom(ctx, view.fence, view.roots, view.tenants, requests, func(ctx context.Context, input PathRef) (PhysicalEntry, bool, error) {
		entry, found := entries[indexKey{root: input.Root, relative: input.Relative}]
		return entry, found, nil
	})
}

func (r *Runtime) materializeFrom(
	ctx context.Context,
	fence Fence,
	roots []RootSpec,
	tenants []tenant.TenantSpec,
	requests []MaterializationRequest,
	lookup func(context.Context, PathRef) (PhysicalEntry, bool, error),
) ([]Materialization, error) {
	owned := make([]Materialization, 0, len(requests))
	success := false
	defer func() {
		if !success {
			_ = closeMaterializations(owned)
		}
	}()
	seen := make(map[LogicalID]struct{}, len(requests))
	rootMap := make(map[RootID]RootSpec)
	for _, root := range roots {
		rootMap[root.ID] = root
	}
	for _, request := range requests {
		if request.Logical == "" || len(request.Inputs) == 0 {
			return nil, fmt.Errorf("%w: incomplete materialization request", ErrInvalidPlan)
		}
		if _, duplicate := seen[request.Logical]; duplicate {
			return nil, fmt.Errorf("%w: duplicate logical materialization %q", ErrInvalidPlan, request.Logical)
		}
		seen[request.Logical] = struct{}{}
		before := make([]PhysicalEntry, len(request.Inputs))
		for index, input := range request.Inputs {
			indexed, found, err := lookup(ctx, input)
			if err != nil {
				return nil, err
			}
			if !found || !indexed.Exists {
				return nil, fmt.Errorf("%w: materialization input is not in the fenced index", ErrInvalidPlan)
			}
			entry, err := r.executor.Stat(ctx, rootMap[input.Root], input.Relative)
			if err != nil || !samePhysical(entry, indexed) {
				return nil, ErrSourceChanged
			}
			before[index] = entry
		}
		task := MaterializationTask{
			Fence: fence,
			Roots: append([]RootSpec(nil), roots...), Tenants: append([]tenant.TenantSpec(nil), tenants...),
			Request: cloneMaterializationRequest(request), Expected: append([]PhysicalEntry(nil), before...),
		}
		materialization, err := r.executor.Materialize(ctx, task)
		if err != nil {
			return nil, fmt.Errorf("sourceauthority: materialize %q: %w", request.Logical, err)
		}
		owned = append(owned, cloneMaterialization(materialization))
		if materialization.Logical != request.Logical || len(materialization.Objects) == 0 {
			return nil, fmt.Errorf("%w: materialization changed logical identity or returned no objects", ErrInvalidPlan)
		}
		for index, input := range request.Inputs {
			after, err := r.executor.Stat(ctx, rootMap[input.Root], input.Relative)
			if err != nil || !samePhysical(after, before[index]) {
				return nil, ErrSourceChanged
			}
		}
	}
	success = true
	return owned, nil
}

type sourcePublication struct {
	Mode        catalog.SourceMode
	Predecessor causal.Revision
	Change      causal.ChangeSet
	Tenants     []catalog.SourceTenant
	Mutation    catalog.MutationID
}

func (r *Runtime) buildPublication(
	ctx context.Context,
	mode catalog.SourceMode,
	predecessor causal.Revision,
	affected []causal.LogicalKey,
	roots []TenantRoot,
	materialized []Materialization,
	deletes []Delete,
	origin *catalog.CausalOrigin,
	providerOperation *catalog.MutationID,
) (publicationResult sourcePublication, bindingResult []catalog.SourceAuthorityBindingRecord, resultErr error) {
	var staged []catalog.ContentRef
	stagedByContent := make(map[struct {
		hash catalog.ContentHash
		size int64
	}]catalog.ContentRef)
	defer func() {
		if err := closeMaterializations(materialized); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("sourceauthority: release materialized content: %w", err))
		}
		if resultErr != nil && len(staged) != 0 {
			resultErr = errors.Join(resultErr, r.releaseUnclaimedContent(ctx, staged))
		}
	}()
	if err := validateAffectedKeys(affected); err != nil {
		return sourcePublication{}, nil, err
	}
	revision := predecessor + 1
	changeID, operationID, err := newCausalIDs()
	if err != nil {
		return sourcePublication{}, nil, err
	}
	publication := sourcePublication{
		Mode: mode, Predecessor: predecessor,
		Change: causal.ChangeSet{
			SourceAuthority: r.authority, SourceRevision: revision, ChangeID: changeID,
			OperationID: operationID, Cause: causal.CauseExternalUnattributed,
			AffectedKeys: append([]causal.LogicalKey(nil), affected...),
		},
	}
	var providerPrepared *catalog.PreparedMutation
	var providerReservation *catalog.SourceDriverMutationReservation
	if providerOperation != nil {
		reservation, err := r.catalog.SourceDriverMutationReservation(ctx, *providerOperation)
		if err != nil {
			return sourcePublication{}, nil, err
		}
		prepared, err := r.catalog.PreparedMutation(ctx, reservation.Target.Tenant, *providerOperation)
		if err != nil {
			return sourcePublication{}, nil, err
		}
		if reservation.Receipt == nil || reservation.Predecessor > predecessor ||
			prepared.State != catalog.MutationApplying || prepared.Claim == nil ||
			*prepared.Claim != reservation.Claim {
			return sourcePublication{}, nil, catalog.ErrMutationConflict
		}
		publication.Mutation = *providerOperation
		publication.Change.ChangeID = reservation.ChangeID
		publication.Change.OperationID = reservation.SourceOperation
		providerPrepared = &prepared
		providerReservation = &reservation
	}
	if origin != nil {
		if (origin.Cause == causal.CauseProviderMutation && (origin.Domain == "" || origin.Generation == 0)) ||
			(origin.Cause != causal.CauseProviderMutation && origin.Cause != causal.CauseDaemonWrite) {
			return sourcePublication{}, nil, fmt.Errorf("%w: invalid correlated mutation origin", ErrInvalidPlan)
		}
		publication.Change.Cause = origin.Cause
		publication.Change.Origin = origin.Domain
		publication.Change.OriginGeneration = origin.Generation
		if providerOperation == nil {
			return sourcePublication{}, nil, fmt.Errorf("%w: correlated origin has no exact mutation operation", ErrInvalidPlan)
		}
	}
	if mode == catalog.SourceSnapshot {
		publication.Predecessor = 0
	}
	logicalSet := make(map[LogicalID]struct{})
	var logicals []LogicalID
	addLogical := func(logical LogicalID) {
		if _, found := logicalSet[logical]; found {
			return
		}
		logicalSet[logical] = struct{}{}
		logicals = append(logicals, logical)
	}
	for _, root := range roots {
		addLogical(root.Logical)
	}
	for _, value := range materialized {
		addLogical(value.Logical)
		for _, projection := range value.Objects {
			addLogical(projection.Parent)
		}
	}
	for _, deletion := range deletes {
		addLogical(deletion.Logical)
	}
	bindingMap, err := r.sourceAuthorityBindings(ctx, logicals)
	if err != nil {
		return sourcePublication{}, nil, err
	}
	reservedKeys := make(map[LogicalID]catalog.SourceObjectKey)
	ensureBinding := func(logical LogicalID) (catalog.SourceAuthorityBindingRecord, error) {
		if binding, found := bindingMap[logical]; found {
			if key := reservedKeys[logical]; key != "" && binding.SourceKey != key {
				return catalog.SourceAuthorityBindingRecord{}, catalog.ErrMutationConflict
			}
			return binding, nil
		}
		key := reservedKeys[logical]
		if key == "" {
			allocated, allocateErr := newOpaqueSourceKey()
			if allocateErr != nil {
				return catalog.SourceAuthorityBindingRecord{}, allocateErr
			}
			key = allocated
		}
		binding, err := r.catalog.ReserveSourceAuthorityBinding(ctx, r.authority, string(logical), key)
		if err != nil {
			return catalog.SourceAuthorityBindingRecord{}, err
		}
		bindingMap[logical] = binding
		return binding, nil
	}
	rootByTenant := make(map[catalog.TenantID]TenantRoot, len(roots))
	for _, root := range roots {
		rootByTenant[root.Tenant] = root
		if _, err := ensureBinding(root.Logical); err != nil {
			return sourcePublication{}, nil, err
		}
	}
	if providerPrepared != nil && providerPrepared.Kind == catalog.MutationCreate {
		if providerPrepared.Source == nil || providerPrepared.Source.Parent == nil ||
			providerPrepared.SourceResult == nil || providerReservation == nil ||
			providerPrepared.SourceResult.SourceKey != providerReservation.Receipt.Result {
			return sourcePublication{}, nil, catalog.ErrSourceLocatorMissing
		}
		var candidate LogicalID
		for _, value := range materialized {
			for _, projection := range value.Objects {
				if projection.Tenant != providerPrepared.Tenant ||
					providerPrepared.Intent.Create == nil || projection.Name != providerPrepared.Intent.Create.Spec.Name {
					continue
				}
				root := rootByTenant[projection.Tenant]
				parentKey := catalog.SourceObjectKey("")
				if projection.Parent == root.Logical {
					parent, err := ensureBinding(root.Logical)
					if err != nil {
						return sourcePublication{}, nil, err
					}
					parentKey = parent.SourceKey
				} else {
					parent, err := ensureBinding(projection.Parent)
					if err != nil {
						return sourcePublication{}, nil, err
					}
					parentKey = parent.SourceKey
				}
				if parentKey != providerPrepared.Source.Parent.SourceKey {
					continue
				}
				if candidate != "" && candidate != value.Logical {
					return sourcePublication{}, nil, catalog.ErrMutationConflict
				}
				candidate = value.Logical
			}
		}
		if candidate == "" {
			return sourcePublication{}, nil, catalog.ErrSourceLocatorMissing
		}
		reservedKeys[candidate] = providerPrepared.SourceResult.SourceKey
	}
	targets := make(map[catalog.TenantID]*catalog.SourceTenant)
	ensureTarget := func(id catalog.TenantID, generation catalog.Generation) (*catalog.SourceTenant, error) {
		root, found := rootByTenant[id]
		if !found || root.Generation != generation {
			return nil, fmt.Errorf("%w: tenant root fence is missing", ErrInvalidPlan)
		}
		rootBinding, err := ensureBinding(root.Logical)
		if err != nil {
			return nil, err
		}
		target := targets[id]
		if target == nil {
			target = &catalog.SourceTenant{Tenant: id, Generation: generation, RootKey: rootBinding.SourceKey}
			targets[id] = target
		}
		return target, nil
	}
	var settledBindings []catalog.SourceAuthorityBindingRecord
	for _, value := range materialized {
		binding, err := ensureBinding(value.Logical)
		if err != nil {
			return sourcePublication{}, nil, err
		}
		binding.Fingerprint = value.Fingerprint
		settledBindings = append(settledBindings, binding)
		for _, projection := range value.Objects {
			target, err := ensureTarget(projection.Tenant, projection.Generation)
			if err != nil {
				return sourcePublication{}, nil, err
			}
			object := catalog.SourceObject{
				Key: binding.SourceKey, Name: projection.Name, Kind: projection.Kind,
				Mode: projection.Mode, LinkTarget: projection.LinkTarget, Visibility: projection.Visibility,
			}
			root := rootByTenant[projection.Tenant]
			if projection.Parent != root.Logical {
				parent, err := ensureBinding(projection.Parent)
				if err != nil {
					return sourcePublication{}, nil, err
				}
				object.Parent = parent.SourceKey
			}
			switch projection.Kind {
			case catalog.KindFile:
				if projection.Content == nil || projection.LinkTarget != "" {
					return sourcePublication{}, nil, fmt.Errorf("%w: file projection has invalid content", ErrInvalidPlan)
				}
				reader, err := projection.Content.Open(ctx)
				if err != nil {
					return sourcePublication{}, nil, fmt.Errorf("sourceauthority: open projected content: %w", err)
				}
				if reader == nil {
					return sourcePublication{}, nil, fmt.Errorf("%w: content source returned a nil stream", ErrInvalidPlan)
				}
				ref, stageErr := r.stageContent(ctx, reader)
				if stageErr == nil {
					staged = append(staged, ref)
				}
				if stageErr != nil {
					return sourcePublication{}, nil, fmt.Errorf("sourceauthority: stream projected content: %w", stageErr)
				}
				contentKey := struct {
					hash catalog.ContentHash
					size int64
				}{hash: ref.Hash, size: ref.Size}
				if shared, found := stagedByContent[contentKey]; found {
					if err := r.releaseUnclaimedContent(ctx, []catalog.ContentRef{ref}); err != nil {
						return sourcePublication{}, nil, err
					}
					ref = shared
				} else {
					stagedByContent[contentKey] = ref
				}
				object.ContentRevision, object.Content = catalog.Revision(revision), ref
			case catalog.KindSymlink:
				if projection.Content != nil || projection.LinkTarget == "" {
					return sourcePublication{}, nil, fmt.Errorf("%w: symlink projection has invalid content", ErrInvalidPlan)
				}
				object.ContentRevision = catalog.Revision(revision)
			case catalog.KindDirectory:
				if projection.Content != nil || projection.LinkTarget != "" {
					return sourcePublication{}, nil, fmt.Errorf("%w: directory projection has invalid content", ErrInvalidPlan)
				}
			default:
				return sourcePublication{}, nil, fmt.Errorf("%w: invalid projection kind", ErrInvalidPlan)
			}
			target.Objects = append(target.Objects, object)
		}
	}
	for _, deletion := range deletes {
		binding, found := bindingMap[deletion.Logical]
		if !found {
			return sourcePublication{}, nil, fmt.Errorf("%w: delete has no retained opaque binding", ErrInvalidPlan)
		}
		if binding.SourceKey == "" {
			return sourcePublication{}, nil, fmt.Errorf("%w: delete has no retained opaque binding", ErrInvalidPlan)
		}
		for _, tenant := range deletion.Tenants {
			target, err := ensureTarget(tenant.Tenant, tenant.Generation)
			if err != nil {
				return sourcePublication{}, nil, err
			}
			target.Deletes = append(target.Deletes, binding.SourceKey)
		}
	}
	if mode == catalog.SourceSnapshot {
		for _, root := range roots {
			if _, err := ensureTarget(root.Tenant, root.Generation); err != nil {
				return sourcePublication{}, nil, err
			}
		}
	}
	ids := make([]catalog.TenantID, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		target := *targets[id]
		slices.SortFunc(target.Objects, func(left, right catalog.SourceObject) int { return compareString(string(left.Key), string(right.Key)) })
		slices.Sort(target.Deletes)
		publication.Tenants = append(publication.Tenants, target)
	}
	return publication, settledBindings, nil
}

func (r *Runtime) mergeSourcePublications(
	ctx context.Context,
	publications []sourcePublication,
) (sourcePublication, error) {
	if len(publications) == 0 {
		return sourcePublication{}, fmt.Errorf("%w: empty publication merge", ErrInvalidPlan)
	}
	if len(publications) == 1 {
		return publications[0], nil
	}
	changeID, operationID, err := newCausalIDs()
	if err != nil {
		return sourcePublication{}, err
	}
	merged := sourcePublication{
		Mode: publications[0].Mode, Predecessor: publications[0].Predecessor,
		Change: causal.ChangeSet{
			SourceAuthority: r.authority,
			SourceRevision:  publications[0].Predecessor + causal.Revision(len(publications)),
			ChangeID:        changeID, OperationID: operationID,
			Cause: publications[0].Change.Cause, Origin: publications[0].Change.Origin,
			OriginGeneration: publications[0].Change.OriginGeneration,
		},
	}
	affected := make(map[causal.LogicalKey]struct{})
	type targetState struct {
		value  catalog.SourceTenant
		values map[catalog.SourceObjectKey]*catalog.SourceObject
	}
	targets := make(map[catalog.TenantID]*targetState)
	var superseded []catalog.ContentRef
	for index, publication := range publications {
		if publication.Mode != catalog.SourceDelta ||
			publication.Predecessor != merged.Predecessor+causal.Revision(index) ||
			publication.Change.SourceRevision != publication.Predecessor+1 {
			return sourcePublication{}, fmt.Errorf("%w: publication merge chain is discontinuous", ErrInvalidPlan)
		}
		if publication.Change.Cause != merged.Change.Cause || publication.Change.Origin != merged.Change.Origin ||
			publication.Change.OriginGeneration != merged.Change.OriginGeneration {
			merged.Change.Cause = causal.CauseExternalUnattributed
			merged.Change.Origin = ""
			merged.Change.OriginGeneration = 0
		}
		if publication.Mutation != (catalog.MutationID{}) {
			if merged.Mutation != (catalog.MutationID{}) && merged.Mutation != publication.Mutation {
				return sourcePublication{}, fmt.Errorf("%w: publication merge contains multiple mutations", ErrInvalidPlan)
			}
			merged.Mutation = publication.Mutation
			reservation, err := r.catalog.SourceDriverMutationReservation(ctx, publication.Mutation)
			if err != nil {
				return sourcePublication{}, err
			}
			merged.Change.ChangeID = reservation.ChangeID
			merged.Change.OperationID = reservation.SourceOperation
		}
		for _, key := range publication.Change.AffectedKeys {
			affected[key] = struct{}{}
		}
		for _, target := range publication.Tenants {
			state := targets[target.Tenant]
			if state == nil {
				state = &targetState{
					value:  catalog.SourceTenant{Tenant: target.Tenant, Generation: target.Generation, RootKey: target.RootKey},
					values: make(map[catalog.SourceObjectKey]*catalog.SourceObject),
				}
				targets[target.Tenant] = state
			} else if state.value.Generation != target.Generation || state.value.RootKey != target.RootKey {
				return sourcePublication{}, fmt.Errorf("%w: publication merge changed a tenant fence", ErrInvalidPlan)
			}
			for _, object := range target.Objects {
				if object.Kind == catalog.KindFile || object.Kind == catalog.KindSymlink {
					object.ContentRevision = catalog.Revision(merged.Change.SourceRevision)
				}
				if prior := state.values[object.Key]; prior != nil && prior.Kind == catalog.KindFile &&
					prior.Content.Stage != object.Content.Stage {
					superseded = append(superseded, prior.Content)
				}
				copy := object
				state.values[object.Key] = &copy
			}
			for _, key := range target.Deletes {
				if prior := state.values[key]; prior != nil && prior.Kind == catalog.KindFile {
					superseded = append(superseded, prior.Content)
				}
				state.values[key] = nil
			}
		}
	}
	for key := range affected {
		merged.Change.AffectedKeys = append(merged.Change.AffectedKeys, key)
	}
	slices.Sort(merged.Change.AffectedKeys)
	ids := make([]catalog.TenantID, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		state := targets[id]
		keys := make([]catalog.SourceObjectKey, 0, len(state.values))
		for key := range state.values {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			if object := state.values[key]; object != nil {
				state.value.Objects = append(state.value.Objects, *object)
			} else {
				state.value.Deletes = append(state.value.Deletes, key)
			}
		}
		merged.Tenants = append(merged.Tenants, state.value)
	}
	if len(superseded) != 0 {
		if err := r.releaseUnclaimedContent(ctx, superseded); err != nil {
			return sourcePublication{}, err
		}
	}
	return merged, nil
}

func (r *Runtime) physicalSourceDriverIdentity(
	ctx context.Context,
	publication sourcePublication,
	settlement catalog.SourceObserverSettlement,
) (catalog.SourceDriverStageIdentity, error) {
	if settlement.Authority != r.authority || settlement.Stream == "" || settlement.RootEpoch == "" ||
		settlement.Through == 0 {
		return catalog.SourceDriverStageIdentity{}, fmt.Errorf("%w: incomplete source-driver observer settlement", ErrInvalidPlan)
	}
	targets := make([]catalog.SourceDriverTarget, 0, len(r.currentTenants()))
	for _, spec := range r.currentTenants() {
		if spec.Content.ID != string(r.authority) {
			continue
		}
		targets = append(targets, catalog.SourceDriverTarget{Tenant: spec.ID, Generation: spec.Generation})
	}
	slices.SortFunc(targets, func(left, right catalog.SourceDriverTarget) int {
		return compareString(string(left.Tenant), string(right.Tenant))
	})
	targetsDigest, err := catalog.SourceDriverTargetsDigest(targets)
	if err != nil {
		return catalog.SourceDriverStageIdentity{}, err
	}
	if publication.Mutation != (catalog.MutationID{}) {
		reservation, err := r.catalog.SourceDriverMutationReservation(ctx, publication.Mutation)
		if err != nil {
			return catalog.SourceDriverStageIdentity{}, err
		}
		prepared, err := r.catalog.PreparedMutation(ctx, reservation.Target.Tenant, publication.Mutation)
		if err != nil {
			return catalog.SourceDriverStageIdentity{}, err
		}
		if reservation.Receipt == nil || reservation.Predecessor != publication.Predecessor ||
			reservation.TargetCount != uint64(len(targets)) || reservation.TargetsDigest != targetsDigest ||
			publication.Change.OperationID != reservation.SourceOperation ||
			publication.Change.ChangeID != reservation.ChangeID || prepared.Claim == nil ||
			*prepared.Claim != reservation.Claim {
			return catalog.SourceDriverStageIdentity{}, catalog.ErrMutationConflict
		}
		return catalog.SourceDriverStageIdentity{
			Authority: r.authority, FleetOwner: r.fleetOwner,
			AuthorityGeneration: r.fleetGeneration, DeclarationDigest: r.declarationDigest,
			TargetCount: uint64(len(targets)), TargetsDigest: targetsDigest,
			Operation: reservation.Operation, SourceOperation: reservation.SourceOperation,
			ChangeID: reservation.ChangeID, Cause: prepared.Intent.Origin.Cause,
			Origin: prepared.Intent.Origin.Domain, OriginGeneration: prepared.Intent.Origin.Generation,
			Mode: catalog.SourceDriverMutation, FromToken: reservation.FromToken,
			ToToken: reservation.Receipt.ToToken, Predecessor: reservation.Predecessor,
			Mutation: reservation.Mutation, MutationTenant: reservation.Target.Tenant,
			MutationGeneration: reservation.Target.Generation, MutationResult: reservation.Receipt.Result,
			MutationRequestDigest: reservation.RequestDigest,
			MutationReceiptDigest: reservation.Receipt.Digest, Claim: reservation.Claim,
			ObserverStream: settlement.Stream, ObserverRootEpoch: settlement.RootEpoch,
			ObserverThrough: settlement.Through,
		}, nil
	}
	_, operation, err := newCausalIDs()
	if err != nil {
		return catalog.SourceDriverStageIdentity{}, err
	}
	return catalog.SourceDriverStageIdentity{
		Authority: r.authority, FleetOwner: r.fleetOwner,
		AuthorityGeneration: r.fleetGeneration, DeclarationDigest: r.declarationDigest,
		TargetCount: uint64(len(targets)), TargetsDigest: targetsDigest,
		Operation: operation, SourceOperation: publication.Change.OperationID,
		ChangeID: publication.Change.ChangeID, Cause: publication.Change.Cause,
		Origin: publication.Change.Origin, OriginGeneration: publication.Change.OriginGeneration,
		Mode:           catalog.SourceDriverDelta,
		FromToken:      strconv.FormatUint(uint64(publication.Predecessor), 10),
		ToToken:        strconv.FormatUint(uint64(publication.Change.SourceRevision), 10),
		Predecessor:    publication.Predecessor,
		ObserverStream: settlement.Stream, ObserverRootEpoch: settlement.RootEpoch,
		ObserverThrough: settlement.Through,
	}, nil
}

func (r *Runtime) appendPhysicalSourceDriverStage(
	ctx context.Context,
	publication sourcePublication,
	settlement stagedObserverSettlement,
) (result catalog.SourceDriverStageState, resultErr error) {
	identity, err := r.physicalSourceDriverIdentity(ctx, publication, settlement.Fence)
	if err != nil {
		return catalog.SourceDriverStageState{}, err
	}
	if err := r.catalog.BeginSourceDriverStage(ctx, identity); err != nil {
		return catalog.SourceDriverStageState{}, err
	}
	owned := true
	defer func() {
		if owned {
			resultErr = errors.Join(resultErr, r.catalog.AbortSourceDriverStage(context.WithoutCancel(ctx), identity))
		}
	}()
	entries := make([]catalog.SourceDriverStageEntry, 0)
	for _, target := range publication.Tenants {
		for _, object := range target.Objects {
			copy := object
			entry := catalog.SourceDriverStageEntry{
				Tenant: target.Tenant, Generation: target.Generation,
				ChangeSequence: 1, Key: object.Key, Object: &copy,
			}
			if object.Visibility == (catalog.Visibility{}) {
				entry.Object = nil
				entry.Private = &catalog.PrivateSourceObject{
					Key: object.Key, Parent: object.Parent, Name: object.Name, Kind: object.Kind,
					Mode: object.Mode, ContentRevision: object.ContentRevision,
					Content: object.Content, LinkTarget: object.LinkTarget,
				}
			}
			entries = append(entries, entry)
		}
		for _, key := range target.Deletes {
			entries = append(entries, catalog.SourceDriverStageEntry{
				Tenant: target.Tenant, Generation: target.Generation,
				ChangeSequence: 1, Key: key,
			})
		}
	}
	slices.SortFunc(entries, func(left, right catalog.SourceDriverStageEntry) int {
		if order := compareString(string(left.Tenant), string(right.Tenant)); order != 0 {
			return order
		}
		if left.Generation < right.Generation {
			return -1
		}
		if left.Generation > right.Generation {
			return 1
		}
		return compareString(string(left.Key), string(right.Key))
	})
	index := append([]catalog.SourcePhysicalIndexRecord(nil), settlement.Index...)
	slices.SortFunc(index, func(left, right catalog.SourcePhysicalIndexRecord) int {
		if order := compareString(left.RootID, right.RootID); order != 0 {
			return order
		}
		return compareString(left.Relative, right.Relative)
	})
	deletes := append([]catalog.SourceIndexLocator(nil), settlement.Deletes...)
	slices.SortFunc(deletes, func(left, right catalog.SourceIndexLocator) int {
		if order := compareString(left.RootID, right.RootID); order != 0 {
			return order
		}
		return compareString(left.Relative, right.Relative)
	})
	bindings := append([]catalog.SourceAuthorityBindingRecord(nil), settlement.Bindings...)
	slices.SortFunc(bindings, func(left, right catalog.SourceAuthorityBindingRecord) int {
		return compareString(left.LogicalID, right.LogicalID)
	})
	matched := append([]catalog.MutationID(nil), settlement.MatchedMutations...)
	mismatched := append([]catalog.MutationID(nil), settlement.MismatchedMutations...)
	slices.SortFunc(matched, compareMutationID)
	slices.SortFunc(mismatched, compareMutationID)
	pages := make([]catalog.SourceDriverStagePage, 0)
	for offset := 0; offset < len(entries); {
		limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(entries))
		pages = append(pages, catalog.SourceDriverStagePage{Entries: entries[offset:limit]})
		offset = limit
	}
	for offset := 0; offset < len(index); {
		limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(index))
		pages = append(pages, catalog.SourceDriverStagePage{Index: index[offset:limit]})
		offset = limit
	}
	for offset := 0; offset < len(deletes); {
		limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(deletes))
		pages = append(pages, catalog.SourceDriverStagePage{Deletes: deletes[offset:limit]})
		offset = limit
	}
	for offset := 0; offset < len(bindings); {
		limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(bindings))
		pages = append(pages, catalog.SourceDriverStagePage{Bindings: bindings[offset:limit]})
		offset = limit
	}
	for offset := 0; offset < len(matched); {
		limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(matched))
		pages = append(pages, catalog.SourceDriverStagePage{MatchedMutations: matched[offset:limit]})
		offset = limit
	}
	for offset := 0; offset < len(mismatched); {
		limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(mismatched))
		pages = append(pages, catalog.SourceDriverStagePage{MismatchedMutations: mismatched[offset:limit]})
		offset = limit
	}
	if len(pages) == 0 {
		return catalog.SourceDriverStageState{}, fmt.Errorf("%w: empty physical source-driver stage", ErrInvalidPlan)
	}
	state, err := r.catalog.PendingSourceDriverStage(ctx, r.authority)
	if err != nil || state == nil || state.Identity != identity {
		return catalog.SourceDriverStageState{}, errors.Join(catalog.ErrIntegrity, err)
	}
	for pageIndex := range pages {
		complete := pageIndex == len(pages)-1
		cursor := []byte(nil)
		if !complete {
			cursor = []byte(strconv.Itoa(pageIndex + 1))
		}
		page := pages[pageIndex]
		page.Sequence = state.Stage.Sequence
		page.Cursor = cursor
		page.PredecessorDigest = catalog.SourceDriverPagePredecessorDigest(state.Cursor, state.PageDigest)
		page.Complete = complete
		encoded, err := json.Marshal(struct {
			Cursor              []byte
			Entries             []catalog.SourceDriverStageEntry
			Index               []catalog.SourcePhysicalIndexRecord
			Deletes             []catalog.SourceIndexLocator
			Bindings            []catalog.SourceAuthorityBindingRecord
			MatchedMutations    []catalog.MutationID
			MismatchedMutations []catalog.MutationID
			Complete            bool
		}{page.Cursor, page.Entries, page.Index, page.Deletes, page.Bindings,
			page.MatchedMutations, page.MismatchedMutations, page.Complete})
		if err != nil {
			return catalog.SourceDriverStageState{}, err
		}
		page.Digest = sha256.Sum256(encoded)
		next, err := r.catalog.AppendSourceDriverStage(ctx, identity, page)
		if err != nil {
			return catalog.SourceDriverStageState{}, err
		}
		state = &next
	}
	owned = false
	return *state, nil
}

func (r *Runtime) finishPhysicalSourceDriverStage(
	ctx context.Context,
	state catalog.SourceDriverStageState,
) (catalog.SourceDriverStageResult, error) {
	for {
		preparation, err := r.catalog.PrepareSourceDriverPublicationBatch(ctx, state.Identity)
		if err != nil {
			return catalog.SourceDriverStageResult{}, err
		}
		if preparation.Prepared {
			break
		}
		if err := ctx.Err(); err != nil {
			return catalog.SourceDriverStageResult{}, err
		}
	}
	var result catalog.SourceDriverStageResult
	var err error
	if state.Identity.Mode == catalog.SourceDriverMutation {
		result, err = r.catalog.CommitSourceDriverMutation(ctx, state)
	} else {
		result, err = r.catalog.CommitSourceDriverStage(ctx, state)
	}
	if err != nil {
		return catalog.SourceDriverStageResult{}, err
	}
	if state.Identity.Mode == catalog.SourceDriverMutation {
		return result, nil
	}
	if err := r.catalog.AcknowledgeSourceDriverCommittedReceipt(ctx, result); err != nil {
		return catalog.SourceDriverStageResult{}, err
	}
	if err := r.catalog.ForgetSourceDriverCommittedReceipt(ctx, result); err != nil {
		return catalog.SourceDriverStageResult{}, err
	}
	return result, nil
}

func (r *Runtime) recoverPendingPhysicalDriverStage(ctx context.Context) error {
	pending, err := r.catalog.PendingSourceDriverStage(ctx, r.authority)
	if err != nil || pending == nil {
		return err
	}
	if pending.Stage.Sequence == 0 || len(pending.Cursor) != 0 {
		return r.catalog.AbortSourceDriverStage(context.WithoutCancel(ctx), pending.Identity)
	}
	if _, err := r.finishPhysicalSourceDriverStage(ctx, *pending); err != nil {
		return err
	}
	return r.recoverCommittedSourceDriverReceipts(ctx)
}

type stagedObserverSettlement struct {
	Fence               catalog.SourceObserverSettlement
	Index               []catalog.SourcePhysicalIndexRecord
	Deletes             []catalog.SourceIndexLocator
	Bindings            []catalog.SourceAuthorityBindingRecord
	MatchedMutations    []catalog.MutationID
	MismatchedMutations []catalog.MutationID
}

func (s stagedObserverSettlement) emptyDerivedState() bool {
	return len(s.Index) == 0 && len(s.Deletes) == 0 && len(s.Bindings) == 0 &&
		len(s.MatchedMutations) == 0 && len(s.MismatchedMutations) == 0
}

func (r *Runtime) applyStagedPublications(
	ctx context.Context,
	publications []sourcePublication,
	settlement stagedObserverSettlement,
) (resultErr error) {
	return r.stageApplySettleWithHandoff(ctx, publications, settlement, nil)
}

func (r *Runtime) stageApplySettleWithHandoff(
	ctx context.Context,
	publications []sourcePublication,
	settlement stagedObserverSettlement,
	onHandoff func(),
) (resultErr error) {
	var publication *sourcePublication
	if len(publications) != 0 {
		merged, err := r.mergeSourcePublications(ctx, publications)
		if err != nil {
			return err
		}
		publication = &merged
		publications = []sourcePublication{merged}
	}
	if len(publications) == 0 && settlement.emptyDerivedState() {
		return r.catalog.SettleSourceObserver(ctx, settlement.Fence)
	}
	if publication != nil {
		contentOwned := true
		defer func() {
			if contentOwned {
				resultErr = errors.Join(resultErr, r.releasePublicationsStages(ctx, publications))
			}
		}()
		staged, err := r.appendPhysicalSourceDriverStage(ctx, *publication, settlement)
		if err != nil {
			return err
		}
		contentOwned = false
		if onHandoff != nil {
			onHandoff()
		}
		if _, err := r.finishPhysicalSourceDriverStage(ctx, staged); err != nil {
			driverErr := fmt.Errorf("sourceauthority: publish bounded source driver stage: %w", err)
			if terminalAuthorityError(err) {
				quarantineErr := r.catalog.QuarantineSourceObserver(
					context.WithoutCancel(ctx), r.authority, driverErr.Error(),
				)
				return errors.Join(ErrQuarantined, driverErr, quarantineErr)
			}
			return driverErr
		}
		return nil
	}
	_, stageOperation, err := newCausalIDs()
	if err != nil {
		return err
	}
	identity := catalog.SourcePublicationStageIdentity{
		Authority: r.authority, FleetOwner: r.fleetOwner,
		FleetGeneration: r.fleetGeneration, DriverID: r.driverID, DeclarationDigest: r.declarationDigest,
		Operation: stageOperation,
		Stream:    settlement.Fence.Stream, RootEpoch: settlement.Fence.RootEpoch,
		Through: settlement.Fence.Through,
	}
	if len(publications) != 0 {
		identity.Predecessor = publications[0].Predecessor
	} else {
		identity.Predecessor, err = r.catalog.SourceWatermark(ctx, r.authority)
		if err != nil {
			return err
		}
	}
	if err := r.catalog.BeginSourcePublicationStage(ctx, identity); err != nil {
		return fmt.Errorf("sourceauthority: begin staged publication: %w", err)
	}
	owned := true
	defer func() {
		if owned {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalStageCleanupTimeout)
			abortErr := r.catalog.AbortSourcePublicationStage(cleanupCtx, r.authority, stageOperation)
			cancel()
			resultErr = errors.Join(resultErr, abortErr)
			resultErr = errors.Join(resultErr, r.releasePublicationsStages(ctx, publications))
		}
	}()
	ref, err := r.appendPublicationStage(ctx, identity, publications, settlement)
	if err != nil {
		return err
	}
	owned = false
	if onHandoff != nil {
		onHandoff()
	}
	result, err := r.catalog.CommitSourcePublicationStage(ctx, ref)
	if err != nil {
		commitErr := fmt.Errorf("sourceauthority: commit staged publication: %w", err)
		if terminalAuthorityError(err) {
			quarantineErr := r.catalog.QuarantineSourceObserver(
				context.WithoutCancel(ctx), r.authority, commitErr.Error(),
			)
			return errors.Join(ErrQuarantined, commitErr, quarantineErr)
		}
		return commitErr
	}
	if err := validateCommittedPublicationResult(ref, result); err != nil {
		quarantineErr := r.catalog.QuarantineSourceObserver(
			context.WithoutCancel(ctx), r.authority, err.Error(),
		)
		return errors.Join(ErrQuarantined, err, quarantineErr)
	}
	if result.Count == 0 {
		if err := r.catalog.AcknowledgeSourceObserverSettlement(ctx, ref); err != nil {
			return fmt.Errorf("sourceauthority: acknowledge observer settlement: %w", err)
		}
	}
	return nil
}

func (r *Runtime) appendPublicationStage(
	ctx context.Context,
	identity catalog.SourcePublicationStageIdentity,
	publications []sourcePublication,
	settlement stagedObserverSettlement,
) (catalog.SourcePublicationStageRef, error) {
	var sequence uint64
	var pending *catalog.SourcePublicationStagePage
	emit := func(page catalog.SourcePublicationStagePage) error {
		if pending != nil {
			pending.Sequence = sequence
			_, err := r.catalog.AppendSourcePublicationStage(ctx, identity, *pending)
			if err != nil {
				return err
			}
			sequence++
		}
		copy := page
		pending = &copy
		return nil
	}
	for index, publication := range publications {
		if publication.Mode != catalog.SourceDelta ||
			publication.Change.SourceAuthority != identity.Authority ||
			publication.Change.OperationID == identity.Operation ||
			publication.Change.SourceRevision != publication.Predecessor+1 ||
			(index == 0 && publication.Predecessor != identity.Predecessor) ||
			(index > 0 && publication.Predecessor != publications[index-1].Change.SourceRevision) {
			return catalog.SourcePublicationStageRef{}, fmt.Errorf("%w: invalid staged publication chain", ErrInvalidPlan)
		}
		header := catalog.SourcePublicationStageHeader{
			Mode: publication.Mode, Predecessor: publication.Predecessor,
			Change: publication.Change,
		}
		header.Change.AffectedKeys = nil
		firstCount := min(len(publication.Change.AffectedKeys), catalog.SourcePublicationStagePageItemLimit-1)
		page := catalog.SourcePublicationStagePage{Header: &header}
		for _, key := range publication.Change.AffectedKeys[:firstCount] {
			page.Affected = append(page.Affected, catalog.SourcePublicationAffected{
				Revision: publication.Change.SourceRevision, Key: key,
			})
		}
		if err := emit(page); err != nil {
			return catalog.SourcePublicationStageRef{}, err
		}
		for offset := firstCount; offset < len(publication.Change.AffectedKeys); {
			limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(publication.Change.AffectedKeys))
			page := catalog.SourcePublicationStagePage{}
			for _, key := range publication.Change.AffectedKeys[offset:limit] {
				page.Affected = append(page.Affected, catalog.SourcePublicationAffected{
					Revision: publication.Change.SourceRevision, Key: key,
				})
			}
			if err := emit(page); err != nil {
				return catalog.SourcePublicationStageRef{}, err
			}
			offset = limit
		}
	}
	index := append([]catalog.SourcePhysicalIndexRecord(nil), settlement.Index...)
	slices.SortFunc(index, func(left, right catalog.SourcePhysicalIndexRecord) int {
		if value := compareString(left.RootID, right.RootID); value != 0 {
			return value
		}
		return compareString(left.Relative, right.Relative)
	})
	for offset := 0; offset < len(index); {
		limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(index))
		if err := emit(catalog.SourcePublicationStagePage{Index: index[offset:limit]}); err != nil {
			return catalog.SourcePublicationStageRef{}, err
		}
		offset = limit
	}
	deletes := append([]catalog.SourceIndexLocator(nil), settlement.Deletes...)
	slices.SortFunc(deletes, func(left, right catalog.SourceIndexLocator) int {
		if value := compareString(left.RootID, right.RootID); value != 0 {
			return value
		}
		return compareString(left.Relative, right.Relative)
	})
	for offset := 0; offset < len(deletes); {
		limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(deletes))
		if err := emit(catalog.SourcePublicationStagePage{Deletes: deletes[offset:limit]}); err != nil {
			return catalog.SourcePublicationStageRef{}, err
		}
		offset = limit
	}
	bindings := append([]catalog.SourceAuthorityBindingRecord(nil), settlement.Bindings...)
	slices.SortFunc(bindings, func(left, right catalog.SourceAuthorityBindingRecord) int {
		return compareString(left.LogicalID, right.LogicalID)
	})
	for offset := 0; offset < len(bindings); {
		limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(bindings))
		if err := emit(catalog.SourcePublicationStagePage{Bindings: bindings[offset:limit]}); err != nil {
			return catalog.SourcePublicationStageRef{}, err
		}
		offset = limit
	}
	matched := append([]catalog.MutationID(nil), settlement.MatchedMutations...)
	mismatched := append([]catalog.MutationID(nil), settlement.MismatchedMutations...)
	slices.SortFunc(matched, compareMutationID)
	slices.SortFunc(mismatched, compareMutationID)
	for offset := 0; offset < len(matched); {
		limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(matched))
		if err := emit(catalog.SourcePublicationStagePage{MatchedMutations: matched[offset:limit]}); err != nil {
			return catalog.SourcePublicationStageRef{}, err
		}
		offset = limit
	}
	for offset := 0; offset < len(mismatched); {
		limit := min(offset+catalog.SourcePublicationStagePageItemLimit, len(mismatched))
		if err := emit(catalog.SourcePublicationStagePage{MismatchedMutations: mismatched[offset:limit]}); err != nil {
			return catalog.SourcePublicationStageRef{}, err
		}
		offset = limit
	}
	if pending == nil {
		return catalog.SourcePublicationStageRef{}, fmt.Errorf("%w: empty staged publication", ErrInvalidPlan)
	}
	pending.Sequence = sequence
	pending.Complete = true
	return r.catalog.AppendSourcePublicationStage(ctx, identity, *pending)
}

func compareMutationID(left, right catalog.MutationID) int {
	return bytes.Compare(left[:], right[:])
}

func (r *Runtime) promoteSnapshotWithHandoff(
	ctx context.Context,
	ref catalog.SourceSnapshotStageRef,
	settlement catalog.SourceSnapshotSettlement,
	onHandoff func(),
) error {
	if settlement.Snapshot != ref || settlement.Fence.Operation != ref.Operation ||
		ref.Authority != r.authority || ref.Digest == ([32]byte{}) || ref.FenceDigest == ([32]byte{}) {
		return fmt.Errorf("%w: incomplete staged snapshot handoff", ErrInvalidPlan)
	}
	if onHandoff != nil {
		onHandoff()
	}
	if _, err := r.catalog.PromoteSourceSnapshot(ctx, ref, settlement); err != nil {
		return fmt.Errorf("sourceauthority: promote and settle staged snapshot: %w", err)
	}
	return nil
}

func (r *Runtime) releasePublicationsStages(ctx context.Context, publications []sourcePublication) error {
	var result error
	for _, publication := range publications {
		result = errors.Join(result, r.releasePublicationStages(ctx, publication))
	}
	return result
}

func (r *Runtime) releasePublicationStages(ctx context.Context, publication sourcePublication) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalStageCleanupTimeout)
	defer cancel()
	refs := make([]catalog.ContentRef, 0, catalog.ReleaseUnclaimedContentLimit)
	for _, target := range publication.Tenants {
		for _, object := range target.Objects {
			if object.Kind == catalog.KindFile {
				refs = append(refs, object.Content)
				if len(refs) == catalog.ReleaseUnclaimedContentLimit {
					if err := r.catalog.ReleaseUnclaimedContent(cleanupCtx, refs); err != nil {
						return err
					}
					refs = refs[:0]
				}
			}
		}
	}
	return r.catalog.ReleaseUnclaimedContent(cleanupCtx, refs)
}

func (r *Runtime) releaseUnclaimedContent(ctx context.Context, refs []catalog.ContentRef) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalStageCleanupTimeout)
	defer cancel()
	for len(refs) > 0 {
		if err := cleanupCtx.Err(); err != nil {
			return err
		}
		count := min(len(refs), catalog.ReleaseUnclaimedContentLimit)
		if err := r.catalog.ReleaseUnclaimedContent(cleanupCtx, refs[:count]); err != nil {
			return err
		}
		refs = refs[count:]
	}
	return nil
}

func terminalAuthorityError(err error) bool {
	return errors.Is(err, ErrInvalidPlan) || errors.Is(err, catalog.ErrIntegrity) ||
		errors.Is(err, catalog.ErrSourceObserverConflict) || errors.Is(err, catalog.ErrInvalidObject) ||
		errors.Is(err, catalog.ErrInvalidTransition) || errors.Is(err, catalog.ErrMutationConflict) ||
		errors.Is(err, catalog.ErrGenerationMismatch) || errors.Is(err, catalog.ErrTenantOwnerMismatch) ||
		errors.Is(err, catalog.ErrSchemaMismatch) || errors.Is(err, catalog.ErrConflict)
}

func (r *Runtime) recoverPendingPublicationStage(ctx context.Context, ref catalog.SourcePublicationStageRef) error {
	if err := r.validatePendingPublicationStage(ref); err != nil {
		quarantineErr := r.catalog.QuarantineSourceObserver(
			context.WithoutCancel(ctx), r.authority, err.Error(),
		)
		return errors.Join(err, quarantineErr)
	}
	pendingDriver, err := r.catalog.PendingSourceDriverStage(ctx, r.authority)
	if err != nil {
		return err
	}
	if pendingDriver != nil {
		identity := pendingDriver.Identity
		if identity.FleetOwner != ref.FleetOwner || identity.AuthorityGeneration != ref.FleetGeneration ||
			identity.DeclarationDigest != ref.DeclarationDigest || identity.Predecessor+1 != ref.Revision {
			return fmt.Errorf("%w: pending source driver differs from observer recovery proof", ErrQuarantined)
		}
		if pendingDriver.Stage.Sequence == 0 || len(pendingDriver.Cursor) != 0 {
			if err := r.catalog.AbortSourceDriverStage(context.WithoutCancel(ctx), identity); err != nil {
				return err
			}
			if err := r.catalog.AbortSourcePublicationStage(ctx, ref.Authority, ref.Operation); err != nil {
				return err
			}
			return nil
		}
		if _, err := r.finishPhysicalSourceDriverStage(ctx, *pendingDriver); err != nil {
			return err
		}
	}
	watermark, err := r.catalog.SourceWatermark(ctx, r.authority)
	if err != nil {
		return err
	}
	if watermark < ref.Revision {
		checkpoint, checkpointErr := r.catalog.SourceDriverCheckpoint(ctx, r.authority)
		if checkpointErr != nil && !errors.Is(checkpointErr, catalog.ErrNotFound) {
			return checkpointErr
		}
		if checkpointErr != nil || checkpoint.SourceRevision != ref.Revision ||
			checkpoint.FleetOwner != ref.FleetOwner || checkpoint.AuthorityGeneration != ref.FleetGeneration ||
			checkpoint.DeclarationDigest != ref.DeclarationDigest {
			if err := r.catalog.AbortSourcePublicationStage(ctx, ref.Authority, ref.Operation); err != nil {
				return err
			}
			return nil
		}
	}
	if result, err := r.catalog.CommitSourcePublicationStage(ctx, ref); err == nil {
		if validationErr := validateCommittedPublicationResult(ref, result); validationErr != nil {
			quarantineErr := r.catalog.QuarantineSourceObserver(
				context.WithoutCancel(ctx), r.authority, validationErr.Error(),
			)
			return errors.Join(ErrQuarantined, validationErr, quarantineErr)
		}
		if result.Count == 0 {
			return r.catalog.AcknowledgeSourceObserverSettlement(ctx, ref)
		}
		return nil
	} else if !errors.Is(err, catalog.ErrInvalidTransition) {
		return err
	}
	if err := r.catalog.AbortSourcePublicationStage(ctx, ref.Authority, ref.Operation); err != nil {
		return err
	}
	return nil
}

func validateCommittedPublicationResult(
	ref catalog.SourcePublicationStageRef,
	result catalog.SourcePublicationStageResult,
) error {
	if result.Authority != ref.Authority || result.FleetOwner != ref.FleetOwner ||
		result.FleetGeneration != ref.FleetGeneration || result.DriverID != ref.DriverID ||
		result.DeclarationDigest != ref.DeclarationDigest || result.Operation != ref.Operation ||
		result.Last != ref.Revision || result.Digest != ref.Digest {
		return fmt.Errorf("%w: committed publication result changed its stage identity", catalog.ErrIntegrity)
	}
	if result.Count == 0 {
		if result.First != result.Last {
			return fmt.Errorf("%w: empty publication result changed its revision", catalog.ErrIntegrity)
		}
		return nil
	}
	if result.First == 0 || result.First > result.Last ||
		uint64(result.Last-result.First)+1 != result.Count {
		return fmt.Errorf("%w: publication result has a discontinuous revision range", catalog.ErrIntegrity)
	}
	return nil
}

func (r *Runtime) validatePendingPublicationStage(ref catalog.SourcePublicationStageRef) error {
	if ref.Authority != r.authority || ref.FleetOwner != r.fleetOwner ||
		ref.FleetGeneration == 0 || catalog.ValidateSourceDriverID(ref.DriverID) != nil ||
		ref.DeclarationDigest == ([32]byte{}) {
		return fmt.Errorf("%w: pending publication has an invalid fleet identity", ErrQuarantined)
	}
	if ref.FleetGeneration > r.fleetGeneration {
		return fmt.Errorf("%w: pending publication belongs to a future fleet generation", ErrQuarantined)
	}
	if ref.FleetGeneration == r.fleetGeneration &&
		(ref.DriverID != r.driverID || ref.DeclarationDigest != r.declarationDigest) {
		return fmt.Errorf("%w: pending publication declaration does not match the current generation", ErrQuarantined)
	}
	return nil
}

func (r *Runtime) fence(state catalog.SourceObserverState, checkpoints []StreamCheckpoint) Fence {
	return Fence{
		Authority: r.authority, AuthorityGeneration: r.fleetGeneration,
		Streams: cloneCheckpoints(checkpoints), Inbox: InboxSequence(state.Stream.LastReceived),
		RootDigest: Fingerprint(state.Stream.RootDigest), FleetDigest: Fingerprint(state.Stream.FleetDigest),
	}
}

func viewFromState(fence Fence, roots []RootSpec, tenants []tenant.TenantSpec, records []catalog.SourcePhysicalIndexRecord) (authorityView, error) {
	view := authorityView{fence: fence, roots: roots, tenants: tenants, entries: make(map[indexKey]IndexedEntry, len(records))}
	for _, record := range records {
		var physical PhysicalEntry
		if err := json.Unmarshal(record.Payload, &physical); err != nil {
			return authorityView{}, fmt.Errorf("%w: corrupt physical index payload", ErrQuarantined)
		}
		logical := make([]LogicalID, len(record.Logical))
		for index, value := range record.Logical {
			logical[index] = LogicalID(value)
		}
		view.entries[indexKey{root: RootID(record.RootID), relative: record.Relative}] = IndexedEntry{Physical: physical, Logical: logical}
	}
	return view, nil
}

func validateScanPage(page ScanPage, roots []catalog.SourceObserverRootRecord, prior indexKey) error {
	if len(page.Entries) > snapshotScanPageSize {
		return fmt.Errorf("%w: snapshot page exceeds the bounded limit", ErrInvalidPlan)
	}
	rootSet := make(map[RootID]catalog.SourceObserverRootRecord, len(roots))
	for _, root := range roots {
		rootSet[RootID(root.ID)] = root
	}
	for _, entry := range page.Entries {
		key := indexKey{root: entry.Root, relative: entry.Relative}
		root, ok := rootSet[entry.Root]
		validPath := validRelative(entry.Relative) || (entry.Relative == "." && RootKind(root.Kind) == RootFile)
		if !ok || !entry.Exists || !validPath || entry.Kind < PhysicalFile || entry.Kind > PhysicalSymlink ||
			validateFileIdentity(entry.Identity) != nil || entry.Identity.VolumeUUID != root.VolumeUUID {
			return fmt.Errorf("%w: snapshot scan escaped its root fence", ErrInvalidPlan)
		}
		if prior.root != "" && (key.root < prior.root || (key.root == prior.root && key.relative <= prior.relative)) {
			return fmt.Errorf("%w: snapshot scan is not globally ordered", ErrInvalidPlan)
		}
		prior = key
	}
	return nil
}

func validateDeltaPlan(plan DeltaPlan, view authorityView, batch EventBatch) error {
	if !equalFence(plan.Fence, view.fence) {
		return fmt.Errorf("%w: delta changed its fence", ErrInvalidPlan)
	}
	if err := validatePlanRoots(plan.Roots, view.tenants, false); err != nil {
		return err
	}
	allowed := make(map[indexKey]struct{})
	for _, event := range batch.Events {
		allowed[indexKey{root: event.Root, relative: event.Relative}] = struct{}{}
		for parent := parentRelative(event.Relative); parent != ""; parent = parentRelative(parent) {
			allowed[indexKey{root: event.Root, relative: parent}] = struct{}{}
		}
	}
	if err := validateRequests(plan.Reads, view, allowed); err != nil {
		return err
	}
	return nil
}

func validatePlanRoots(roots []TenantRoot, tenants []tenant.TenantSpec, complete bool) error {
	fleet := make(map[catalog.TenantID]catalog.Generation, len(tenants))
	for _, spec := range tenants {
		fleet[spec.ID] = spec.Generation
	}
	seen := make(map[catalog.TenantID]struct{}, len(roots))
	for _, root := range roots {
		if root.Logical == "" || fleet[root.Tenant] != root.Generation {
			return fmt.Errorf("%w: tenant root escaped the fleet fence", ErrInvalidPlan)
		}
		if _, duplicate := seen[root.Tenant]; duplicate {
			return fmt.Errorf("%w: duplicate tenant root", ErrInvalidPlan)
		}
		seen[root.Tenant] = struct{}{}
	}
	if complete && len(seen) != len(fleet) {
		return fmt.Errorf("%w: snapshot root fleet is incomplete", ErrInvalidPlan)
	}
	return nil
}

func validateRequests(requests []MaterializationRequest, view authorityView, allowed map[indexKey]struct{}) error {
	seen := make(map[LogicalID]struct{}, len(requests))
	for _, request := range requests {
		if request.Logical == "" || len(request.Inputs) == 0 || len(request.Payload) > maxMaterializationPayloadBytes {
			return fmt.Errorf("%w: incomplete read request", ErrInvalidPlan)
		}
		if _, duplicate := seen[request.Logical]; duplicate {
			return fmt.Errorf("%w: duplicate read logical identity", ErrInvalidPlan)
		}
		seen[request.Logical] = struct{}{}
		eventRelated := allowed == nil
		for _, input := range request.Inputs {
			key := indexKey{root: input.Root, relative: input.Relative}
			entry, found := view.entries[key]
			if !found {
				return fmt.Errorf("%w: read is outside the indexed fence", ErrSnapshotRequired)
			}
			if allowed != nil {
				if _, related := allowed[key]; related {
					eventRelated = true
				} else if !slices.Contains(entry.Logical, request.Logical) {
					return fmt.Errorf("%w: incremental read is outside named paths and parents", ErrInvalidPlan)
				}
			}
		}
		if !eventRelated {
			return fmt.Errorf("%w: incremental read has no event-related input", ErrInvalidPlan)
		}
	}
	return nil
}

func cloneMaterializationRequest(request MaterializationRequest) MaterializationRequest {
	request.Inputs = append([]PathRef(nil), request.Inputs...)
	request.Payload = append([]byte(nil), request.Payload...)
	return request
}

func validateAffectedKeys(keys []causal.LogicalKey) error {
	if len(keys) == 0 {
		return fmt.Errorf("%w: publication has no affected logical keys", ErrInvalidPlan)
	}
	for index, key := range keys {
		if key == "" || (index > 0 && keys[index-1] >= key) {
			return fmt.Errorf("%w: affected keys are not sorted and unique", ErrInvalidPlan)
		}
	}
	return nil
}

func bindIndexRecords(records []catalog.SourcePhysicalIndexRecord, reads []MaterializationRequest) ([]catalog.SourcePhysicalIndexRecord, error) {
	byKey := make(map[indexKey]int, len(records))
	for index, record := range records {
		byKey[indexKey{root: RootID(record.RootID), relative: record.Relative}] = index
	}
	for _, request := range reads {
		for _, input := range request.Inputs {
			index, found := byKey[indexKey{root: input.Root, relative: input.Relative}]
			if !found {
				continue
			}
			records[index].Logical = append(records[index].Logical, string(request.Logical))
			slices.Sort(records[index].Logical)
			records[index].Logical = slices.Compact(records[index].Logical)
		}
	}
	return records, nil
}

func ensureIndexRecordsForReads(
	authority causal.SourceAuthorityID,
	records []catalog.SourcePhysicalIndexRecord,
	view authorityView,
	reads []MaterializationRequest,
) ([]catalog.SourcePhysicalIndexRecord, error) {
	seen := make(map[indexKey]struct{}, len(records))
	for _, record := range records {
		seen[indexKey{root: RootID(record.RootID), relative: record.Relative}] = struct{}{}
	}
	for _, request := range reads {
		for _, input := range request.Inputs {
			key := indexKey{root: input.Root, relative: input.Relative}
			if _, found := seen[key]; found {
				continue
			}
			entry, found := view.entries[key]
			if !found {
				continue
			}
			record, err := physicalRecord(authority, entry)
			if err != nil {
				return nil, err
			}
			records = append(records, record)
			seen[key] = struct{}{}
		}
	}
	slices.SortFunc(records, compareCatalogSourceRecord)
	return records, nil
}

func compareCatalogSourceRecord(left, right catalog.SourcePhysicalIndexRecord) int {
	if left.RootID != right.RootID {
		return compareString(left.RootID, right.RootID)
	}
	return compareString(left.Relative, right.Relative)
}

func physicalRecord(authority causal.SourceAuthorityID, entry IndexedEntry) (catalog.SourcePhysicalIndexRecord, error) {
	identity, err := json.Marshal(entry.Physical.Identity)
	if err != nil {
		return catalog.SourcePhysicalIndexRecord{}, err
	}
	payload, err := json.Marshal(entry.Physical)
	if err != nil {
		return catalog.SourcePhysicalIndexRecord{}, err
	}
	logical := make([]string, len(entry.Logical))
	for index, value := range entry.Logical {
		logical[index] = string(value)
	}
	return catalog.SourcePhysicalIndexRecord{
		Authority: authority, RootID: string(entry.Physical.Root), Relative: entry.Physical.Relative,
		FileIdentity: identity, Kind: uint8(entry.Physical.Kind), MetadataFingerprint: entry.Physical.MetadataFingerprint,
		ContentFingerprint: entry.Physical.ContentFingerprint, Logical: logical, Payload: payload,
	}, nil
}

func suppressUnchanged(values []Materialization, bindings []catalog.SourceAuthorityBindingRecord) ([]Materialization, []Materialization) {
	prior := make(map[LogicalID]Fingerprint, len(bindings))
	for _, binding := range bindings {
		prior[LogicalID(binding.LogicalID)] = Fingerprint(binding.Fingerprint)
	}
	result := values[:0]
	var suppressed []Materialization
	for _, value := range values {
		if fingerprint, found := prior[value.Logical]; found && fingerprint == value.Fingerprint {
			suppressed = append(suppressed, value)
			continue
		}
		result = append(result, value)
	}
	return result, suppressed
}

func (r *Runtime) bindingsForMaterializations(ctx context.Context, values []Materialization) ([]catalog.SourceAuthorityBindingRecord, error) {
	logicals := make([]LogicalID, 0, len(values))
	for _, value := range values {
		logicals = append(logicals, value.Logical)
	}
	bindings, err := r.sourceAuthorityBindings(ctx, logicals)
	if err != nil {
		return nil, err
	}
	result := make([]catalog.SourceAuthorityBindingRecord, 0, len(values))
	for _, value := range values {
		if binding, found := bindings[value.Logical]; found {
			result = append(result, binding)
		}
	}
	return result, nil
}

func samePhysical(left, right PhysicalEntry) bool {
	return left.Root == right.Root && left.Relative == right.Relative && left.Exists == right.Exists && left.Kind == right.Kind &&
		left.Identity == right.Identity && left.Mode == right.Mode && left.UID == right.UID && left.GID == right.GID &&
		left.Size == right.Size && left.LinkTarget == right.LinkTarget &&
		left.MetadataFingerprint == right.MetadataFingerprint && left.ContentFingerprint == right.ContentFingerprint
}

func cloneMaterialization(value Materialization) Materialization {
	value.Objects = append([]Projection(nil), value.Objects...)
	return value
}

func closeMaterializations(values []Materialization) error {
	var result error
	for _, value := range values {
		for _, projection := range value.Objects {
			if projection.Content != nil {
				result = errors.Join(result, projection.Content.Close())
			}
		}
	}
	return result
}

func equalFence(left, right Fence) bool {
	return left.Authority == right.Authority && left.Inbox == right.Inbox && left.RootDigest == right.RootDigest &&
		left.FleetDigest == right.FleetDigest && checkpointsEqual(left.Streams, right.Streams)
}

func checkpointsEqual(left, right []StreamCheckpoint) bool {
	return slices.Equal(left, right)
}

func newCausalIDs() (causal.ChangeID, causal.OperationID, error) {
	var change causal.ChangeID
	if _, err := rand.Read(change[:]); err != nil {
		return causal.ChangeID{}, causal.OperationID{}, fmt.Errorf("sourceauthority: mint change id: %w", err)
	}
	var operation causal.OperationID
	if _, err := rand.Read(operation[:]); err != nil {
		return causal.ChangeID{}, causal.OperationID{}, fmt.Errorf("sourceauthority: mint operation id: %w", err)
	}
	return change, operation, nil
}

func newOpaqueSourceKey() (catalog.SourceObjectKey, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("sourceauthority: allocate opaque source key: %w", err)
	}
	return catalog.SourceObjectKey("opaque:" + hex.EncodeToString(raw[:])), nil
}

func newSnapshotID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("sourceauthority: allocate snapshot id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

type echoCorrelation struct {
	matched       []catalog.MutationID
	mismatched    []catalog.MutationID
	origin        *catalog.CausalOrigin
	provider      []EventBatch
	external      []EventBatch
	providerPaths map[indexKey]struct{}
	externalPaths map[indexKey]struct{}
}

func (r *Runtime) correlateMutationEcho(
	ctx context.Context,
	sequence uint64,
	batch EventBatch,
	view authorityView,
) (echoCorrelation, error) {
	result := echoCorrelation{external: []EventBatch{batch}}
	err := r.eachSourceMutationExpectation(ctx, func(record catalog.SourceMutationExpectationRecord) error {
		var envelope mutationEnvelope
		if err := json.Unmarshal(record.Payload, &envelope); err != nil {
			return fmt.Errorf("%w: corrupt mutation expectation", ErrQuarantined)
		}
		if len(record.Receipt) == 0 {
			return nil
		}
		var receipt durableMutationReceipt
		if err := json.Unmarshal(record.Receipt, &receipt); err != nil || receipt.Operation != record.Operation ||
			receipt.Start != envelope.Start || sequence <= uint64(receipt.Start) || sequence > uint64(receipt.End) {
			return nil
		}
		providerPaths := make(map[indexKey]struct{}, len(receipt.Effects)*2)
		for _, effect := range receipt.Effects {
			providerPaths[indexKey{root: effect.Path.Root, relative: effect.Path.Relative}] = struct{}{}
			if parent := parentRelative(effect.Path.Relative); parent != "" {
				providerPaths[indexKey{root: effect.Path.Root, relative: parent}] = struct{}{}
			}
		}
		if record.State != catalog.SourceMutationExpectationComplete {
			return nil
		}
		seenEffects := make(map[indexKey]struct{}, len(receipt.Effects))
		var provider, external, all []EventBatch
		providerChanged := make(map[indexKey]struct{})
		externalChanged := make(map[indexKey]struct{})
		after := uint64(receipt.Start)
		for {
			page, err := r.catalog.SourceObserverInboxPage(
				ctx, r.authority, after, sequence, catalog.SourceObserverInboxPageLimit,
			)
			if err != nil {
				return err
			}
			for _, inbox := range page.Records {
				var retained EventBatch
				if err := json.Unmarshal(inbox.Payload, &retained); err != nil {
					return fmt.Errorf("%w: corrupt retained mutation event", ErrQuarantined)
				}
				all = append(all, retained)
				providerBatch, externalBatch := retained, retained
				providerBatch.Events, externalBatch.Events = nil, nil
				for _, event := range retained.Events {
					key := indexKey{root: event.Root, relative: event.Relative}
					if _, found := providerPaths[key]; !found {
						externalBatch.Events = append(externalBatch.Events, event)
						externalChanged[key] = struct{}{}
						continue
					}
					providerBatch.Events = append(providerBatch.Events, event)
					providerChanged[key] = struct{}{}
					for _, effect := range receipt.Effects {
						if effect.Path.Root == event.Root && effect.Path.Relative == event.Relative {
							seenEffects[key] = struct{}{}
						}
					}
				}
				if len(providerBatch.Events) != 0 {
					provider = append(provider, providerBatch)
				}
				if len(externalBatch.Events) != 0 {
					external = append(external, externalBatch)
				}
			}
			if page.Next == 0 {
				break
			}
			if page.Next <= after {
				return fmt.Errorf("%w: retained mutation inbox cursor did not advance", ErrQuarantined)
			}
			after = page.Next
		}
		result.external = nil
		complete := len(receipt.Effects) > 0 && len(seenEffects) == len(receipt.Effects)
		statesMatch := true
		for _, effect := range receipt.Effects {
			key := indexKey{root: effect.Path.Root, relative: effect.Path.Relative}
			entry, found := view.entries[key]
			statesMatch = statesMatch && expectedStateMatches(effect.After, entry.Physical, found)
		}
		if sequence == uint64(receipt.End) && complete && statesMatch {
			result.matched = append(result.matched, record.Operation)
			copy := receipt.Origin
			result.origin = &copy
			result.provider, result.external = provider, external
			result.providerPaths, result.externalPaths = providerChanged, externalChanged
			return nil
		}
		if sequence == uint64(receipt.End) {
			result.mismatched = append(result.mismatched, record.Operation)
			result.external = all
			result.externalPaths = make(map[indexKey]struct{})
			for _, work := range result.external {
				for _, event := range work.Events {
					result.externalPaths[indexKey{root: event.Root, relative: event.Relative}] = struct{}{}
				}
			}
		}
		return nil
	})
	if err != nil {
		return echoCorrelation{}, err
	}
	return result, nil
}

func expectedStateMatches(expected ExpectedPhysicalState, actual PhysicalEntry, found bool) bool {
	if expected.Exists != found || expected.Exists != actual.Exists {
		return false
	}
	if !expected.Exists {
		return true
	}
	return expected.Kind == actual.Kind && expected.Identity == actual.Identity &&
		expected.MetadataFingerprint == actual.MetadataFingerprint && expected.ContentFingerprint == actual.ContentFingerprint
}
