package sourceauthority

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

type mutationEnvelope struct {
	Request   MutationRequest
	Operation tenant.SourceMutationOperation
	Plan      MutationPlan
	Fence     Fence
	Start     InboxSequence
}

type observedEffect struct {
	Path   PathRef
	Before ExpectedPhysicalState
	After  ExpectedPhysicalState
}

type durableMutationReceipt struct {
	Authority           causal.SourceAuthorityID
	AuthorityGeneration causal.Generation
	Operation           catalog.MutationID
	Origin              catalog.CausalOrigin
	Start               InboxSequence
	End                 InboxSequence
	Effects             []observedEffect
	Digest              Fingerprint
}

func decodeMutationEnvelope(record catalog.SourceMutationExpectationRecord) (mutationEnvelope, error) {
	var envelope mutationEnvelope
	if err := json.Unmarshal(record.Payload, &envelope); err != nil ||
		envelope.Fence.Authority != record.Authority || envelope.Fence.AuthorityGeneration == 0 ||
		envelope.Request.Step.OperationID != record.Operation || envelope.Operation.OperationID != record.Operation ||
		envelope.Request.Step.TenantID != record.Tenant || envelope.Request.Step.Generation != record.Generation ||
		!reflect.DeepEqual(envelope.Request.Step.Origin, record.Origin) {
		return mutationEnvelope{}, fmt.Errorf("%w: corrupt mutation plan identity", ErrQuarantined)
	}
	return envelope, nil
}

func decodeDurableMutationReceipt(
	record catalog.SourceMutationExpectationRecord,
	envelope mutationEnvelope,
) (durableMutationReceipt, error) {
	var receipt durableMutationReceipt
	if len(record.Receipt) == 0 {
		return durableMutationReceipt{}, fmt.Errorf("%w: mutation receipt is absent", ErrQuarantined)
	}
	if err := json.Unmarshal(record.Receipt, &receipt); err != nil ||
		receipt.Authority != record.Authority || receipt.Authority != envelope.Fence.Authority ||
		receipt.AuthorityGeneration != envelope.Fence.AuthorityGeneration ||
		receipt.Operation != record.Operation || receipt.Digest == (Fingerprint{}) ||
		!reflect.DeepEqual(receipt.Origin, record.Origin) {
		return durableMutationReceipt{}, fmt.Errorf("%w: corrupt mutation receipt identity", ErrQuarantined)
	}
	return receipt, nil
}

func (r *Runtime) hasUnsettledSourceMutations(ctx context.Context) (bool, error) {
	page, err := r.catalog.SourceMutationExpectationsPage(ctx, r.authority, catalog.MutationID{}, 1)
	if err != nil {
		return false, err
	}
	return len(page.Records) != 0, nil
}

func (r *Runtime) mutationPreparationBlocked(ctx context.Context, operation catalog.MutationID) (bool, error) {
	pending, err := r.catalog.SourceObserverNextInbox(ctx, r.authority, 0)
	if err != nil {
		return false, err
	}
	if pending != nil {
		return true, nil
	}
	page, err := r.catalog.SourceMutationExpectationsPage(ctx, r.authority, catalog.MutationID{}, 2)
	if err != nil {
		return false, err
	}
	var active catalog.MutationID
	for _, record := range page.Records {
		if active != (catalog.MutationID{}) && active != record.Operation {
			return false, fmt.Errorf("%w: multiple source mutations are active for one authority", ErrQuarantined)
		}
		active = record.Operation
	}
	if page.Next != (catalog.MutationID{}) {
		return false, fmt.Errorf("%w: multiple source mutations are active for one authority", ErrQuarantined)
	}
	return active != (catalog.MutationID{}) && active != operation, nil
}

func (r *Runtime) prepareSourceMutation(ctx context.Context, step tenant.SourceMutationStep) (tenant.SourceMutationOperation, error) {
	if err := r.validateMutationStep(step); err != nil {
		return tenant.SourceMutationOperation{}, err
	}
	record, err := r.catalog.SourceMutationExpectation(ctx, r.authority, step.OperationID)
	if err == nil {
		var envelope mutationEnvelope
		if err := json.Unmarshal(record.Payload, &envelope); err != nil {
			return tenant.SourceMutationOperation{}, fmt.Errorf("%w: corrupt mutation plan", ErrQuarantined)
		}
		if !reflect.DeepEqual(envelope.Request.Step, step) {
			return tenant.SourceMutationOperation{}, catalog.ErrSourceObserverConflict
		}
		return envelope.Operation, nil
	}
	if !errors.Is(err, catalog.ErrNotFound) {
		return tenant.SourceMutationOperation{}, err
	}

	request := MutationRequest{Step: step}
	if step.Source.Object != nil {
		request.Object, err = r.resolveMutationLocator(ctx, *step.Source.Object)
		if err != nil {
			return tenant.SourceMutationOperation{}, err
		}
	}
	if step.Source.Parent != nil {
		request.Parent, err = r.resolveMutationLocator(ctx, *step.Source.Parent)
		if err != nil {
			return tenant.SourceMutationOperation{}, err
		}
	}
	if step.Source.Target != nil {
		request.Target, err = r.resolveMutationLocator(ctx, *step.Source.Target)
		if err != nil {
			return tenant.SourceMutationOperation{}, err
		}
	}
	plan, err := r.policy.PlanMutation(ctx, request)
	if err != nil {
		return tenant.SourceMutationOperation{}, fmt.Errorf("sourceauthority: plan source mutation: %w", err)
	}
	state, err := r.loadSourceObserverFence(ctx, r.authority)
	if err != nil {
		return tenant.SourceMutationOperation{}, err
	}
	if err := r.validateMutationPlan(ctx, step, plan); err != nil {
		return tenant.SourceMutationOperation{}, err
	}
	operation := tenant.SourceMutationOperation{
		OperationID: step.OperationID, SourceID: step.SourceID, SourceMetadata: step.SourceMetadata,
	}
	if step.Kind == catalog.MutationCreate {
		if request.Parent == nil {
			return tenant.SourceMutationOperation{}, fmt.Errorf("%w: create has no source parent", ErrInvalidPlan)
		}
		key, err := newOpaqueSourceKey()
		if err != nil {
			return tenant.SourceMutationOperation{}, err
		}
		operation.SourceResult = &catalog.SourceLocator{
			SourceAuthority: r.authority, SourceKey: key, SourceRevision: request.Parent.Source.SourceRevision,
		}
	}
	envelope := mutationEnvelope{
		Request: request, Operation: operation, Plan: plan,
		Fence: r.fence(state, checkpointsFromCatalog(state.Checkpoints)), Start: InboxSequence(state.Stream.LastReceived),
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return tenant.SourceMutationOperation{}, err
	}
	if err := r.catalog.PutSourceMutationExpectation(ctx, catalog.SourceMutationExpectationRecord{
		Operation: step.OperationID, Authority: r.authority, Tenant: step.TenantID, Generation: step.Generation,
		Origin: step.Origin, Digest: sha256.Sum256(payload), Payload: payload,
	}); err != nil {
		return tenant.SourceMutationOperation{}, err
	}
	return operation, nil
}

func (r *Runtime) applySourceMutation(
	ctx context.Context,
	step tenant.SourceMutationStep,
	operation tenant.SourceMutationOperation,
	content tenant.SourceMutationContent,
) (resultErr error) {
	if content != nil {
		defer func() {
			resultErr = errors.Join(resultErr, content.Close())
		}()
	}
	if err := r.validateMutationStep(step); err != nil {
		return err
	}
	if committed, err := r.catalog.CommittedSourceDriverMutation(ctx, r.authority, step.OperationID); err != nil {
		return err
	} else if committed != nil {
		return r.settleCommittedPhysicalMutation(ctx, *committed)
	}
	record, err := r.catalog.SourceMutationExpectation(ctx, r.authority, step.OperationID)
	if err != nil {
		return err
	}
	envelope, err := decodeMutationEnvelope(record)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(envelope.Request.Step, step) || !reflect.DeepEqual(envelope.Operation, operation) {
		return catalog.ErrSourceObserverConflict
	}
	reservation, err := r.reservePhysicalMutation(ctx, step, record)
	if err != nil {
		return err
	}
	if len(record.Receipt) != 0 {
		if _, err := decodeDurableMutationReceipt(record, envelope); err != nil {
			return err
		}
		if reservation.Receipt == nil {
			return fmt.Errorf("%w: durable physical receipt is not bound to its source-driver reservation", catalog.ErrIntegrity)
		}
		if err := r.reconcile(ctx, nil); err != nil {
			return err
		}
		committed, err := r.catalog.CommittedSourceDriverMutation(ctx, r.authority, step.OperationID)
		if err != nil {
			return err
		}
		if committed == nil {
			return catalog.ErrMutationActive
		}
		return r.settleCommittedPhysicalMutation(ctx, *committed)
	}
	if envelope.Fence.AuthorityGeneration != r.fleetGeneration {
		return fmt.Errorf("%w: unfinished source mutation belongs to authority generation %d", catalog.ErrGenerationMismatch,
			envelope.Fence.AuthorityGeneration)
	}
	childReceipt, err := r.executor.ApplyMutation(ctx, MutationTask{
		Fence: envelope.Fence, Roots: r.currentRoots(), OperationID: step.OperationID,
		ExpectationDigest: Fingerprint(record.Digest), Program: envelope.Plan.Program,
		Expected: append([]ExpectedEffect(nil), envelope.Plan.Effects...), Content: content,
	})
	if err != nil {
		return fmt.Errorf("sourceauthority: apply semantic source mutation: %w", err)
	}
	if childReceipt.OperationID != step.OperationID || childReceipt.Digest == (Fingerprint{}) ||
		len(childReceipt.Effects) != len(envelope.Plan.Effects) {
		return fmt.Errorf("%w: invalid source mutation child receipt", ErrQuarantined)
	}
	stream := r.currentStream()
	if stream == nil {
		return ErrClosed
	}
	checkpoints, err := stream.Flush(ctx)
	if err != nil {
		return fmt.Errorf("sourceauthority: flush mutation receipt: %w", err)
	}
	state, err := r.loadSourceObserverFence(ctx, r.authority)
	if err != nil {
		return err
	}
	if !catalogCheckpointsCover(state.Checkpoints, checkpoints) {
		return ErrSourceChanged
	}
	roots := make(map[RootID]RootSpec)
	for _, root := range r.currentRoots() {
		roots[root.ID] = root
	}
	receipt := durableMutationReceipt{
		Authority: r.authority, AuthorityGeneration: envelope.Fence.AuthorityGeneration,
		Operation: step.OperationID, Origin: step.Origin, Digest: childReceipt.Digest, Start: envelope.Start,
		Effects: make([]observedEffect, len(envelope.Plan.Effects)),
	}
	for index, effect := range envelope.Plan.Effects {
		entry, err := r.executor.Stat(ctx, roots[effect.Path.Root], effect.Path.Relative)
		if err != nil {
			return fmt.Errorf("sourceauthority: stat mutation receipt %s/%s: %w", effect.Path.Root, effect.Path.Relative, err)
		}
		after := physicalState(entry)
		if !samePhysical(entry, childReceipt.Effects[index]) {
			return fmt.Errorf("%w: child receipt differs from observer-fenced source state", ErrSourceChanged)
		}
		if (effect.Outcome == MutationAbsent && after.Exists) ||
			(effect.Outcome == MutationPresent && (!after.Exists || after.Kind != effect.Kind)) {
			_ = r.catalog.RequireSourceObserverSnapshot(context.WithoutCancel(ctx), r.authority)
			return fmt.Errorf("%w: source mutation post-state violated its semantic plan", ErrSourceChanged)
		}
		receipt.Effects[index] = observedEffect{Path: effect.Path, Before: effect.Before, After: after}
	}
	endCheckpoints, err := stream.Flush(ctx)
	if err != nil {
		return fmt.Errorf("sourceauthority: flush mutation post-state fence: %w", err)
	}
	settled, err := r.loadSourceObserverFence(ctx, r.authority)
	if err != nil {
		return err
	}
	if !catalogCheckpointsCover(settled.Checkpoints, endCheckpoints) {
		return ErrSourceChanged
	}
	for index, effect := range envelope.Plan.Effects {
		entry, err := r.executor.Stat(ctx, roots[effect.Path.Root], effect.Path.Relative)
		if err != nil || physicalState(entry) != receipt.Effects[index].After {
			_ = r.catalog.RequireSourceObserverSnapshot(context.WithoutCancel(ctx), r.authority)
			return fmt.Errorf("%w: source mutation post-state moved outside its observer fence", ErrSourceChanged)
		}
	}
	receipt.End = InboxSequence(settled.Stream.LastReceived)
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if err := r.catalog.CompleteSourceMutationExpectation(ctx, r.authority, step.OperationID, payload); err != nil {
		return err
	}
	resultKey := catalog.SourceObjectKey("")
	if operation.SourceResult != nil {
		resultKey = operation.SourceResult.SourceKey
	}
	reservation, err = r.catalog.RecordSourceDriverMutationReceipt(ctx, step.OperationID, reservation.Claim,
		catalog.SourceDriverMutationReceiptProof{
			ToToken: strconv.FormatUint(uint64(reservation.Predecessor+1), 10),
			Result:  resultKey, Digest: [32]byte(receipt.Digest),
		})
	if err != nil {
		return err
	}
	if err := r.reconcile(ctx, nil); err != nil {
		return err
	}
	committed, err := r.catalog.CommittedSourceDriverMutation(ctx, r.authority, step.OperationID)
	if err != nil {
		return err
	}
	if committed == nil {
		return catalog.ErrMutationActive
	}
	return r.settleCommittedPhysicalMutation(ctx, *committed)
}

func (r *Runtime) reservePhysicalMutation(
	ctx context.Context,
	step tenant.SourceMutationStep,
	record catalog.SourceMutationExpectationRecord,
) (catalog.SourceDriverMutationReservation, error) {
	prepared, err := r.catalog.PreparedMutation(ctx, step.TenantID, step.OperationID)
	if err != nil {
		return catalog.SourceDriverMutationReservation{}, err
	}
	if prepared.State != catalog.MutationApplying || prepared.Claim == nil || prepared.Source == nil {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrInvalidTransition
	}
	targets := make([]catalog.SourceDriverTarget, 0, len(r.currentTenants()))
	for _, spec := range r.currentTenants() {
		if spec.Content.ID == string(r.authority) {
			targets = append(targets, catalog.SourceDriverTarget{Tenant: spec.ID, Generation: spec.Generation})
		}
	}
	slices.SortFunc(targets, func(left, right catalog.SourceDriverTarget) int {
		return compareString(string(left.Tenant), string(right.Tenant))
	})
	digest, err := catalog.SourceDriverTargetsDigest(targets)
	if err != nil {
		return catalog.SourceDriverMutationReservation{}, err
	}
	target := catalog.SourceDriverTarget{Tenant: step.TenantID, Generation: step.Generation}
	index, found := slices.BinarySearchFunc(targets, target, func(left, right catalog.SourceDriverTarget) int {
		return compareString(string(left.Tenant), string(right.Tenant))
	})
	if !found || targets[index] != target {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrGenerationMismatch
	}
	reservation, err := r.catalog.SourceDriverMutationReservation(ctx, step.OperationID)
	if errors.Is(err, catalog.ErrNotFound) {
		checkpoint, checkpointErr := r.catalog.SourceDriverCheckpoint(ctx, r.authority)
		if checkpointErr != nil {
			return catalog.SourceDriverMutationReservation{}, checkpointErr
		}
		if checkpoint.FleetOwner != r.fleetOwner || checkpoint.AuthorityGeneration != r.fleetGeneration ||
			checkpoint.DeclarationDigest != r.declarationDigest || checkpoint.TargetCount != uint64(len(targets)) ||
			checkpoint.TargetsDigest != digest || checkpoint.SnapshotRequired != 0 {
			return catalog.SourceDriverMutationReservation{}, catalog.ErrGenerationMismatch
		}
		targetCheckpoint, checkpointErr := r.catalog.SourceDriverTargetCheckpoint(
			ctx, r.authority, target.Tenant, target.Generation,
		)
		if checkpointErr != nil {
			return catalog.SourceDriverMutationReservation{}, checkpointErr
		}
		if targetCheckpoint.CatalogRevision != step.ExpectedHead {
			return catalog.SourceDriverMutationReservation{}, catalog.ErrSourcePredecessor
		}
		change, sourceOperation, idErr := newCausalIDs()
		if idErr != nil {
			return catalog.SourceDriverMutationReservation{}, idErr
		}
		_, stageOperation, idErr := newCausalIDs()
		if idErr != nil {
			return catalog.SourceDriverMutationReservation{}, idErr
		}
		reservation, err = r.catalog.ReserveSourceDriverMutation(ctx, catalog.SourceDriverMutationReservationRequest{
			Mutation: step.OperationID, Claim: *prepared.Claim,
			Authority: r.authority, FleetOwner: r.fleetOwner,
			AuthorityGeneration: r.fleetGeneration, DeclarationDigest: r.declarationDigest,
			TargetCount: uint64(len(targets)), TargetsDigest: digest, Target: target,
			FromToken: checkpoint.Token, Predecessor: checkpoint.SourceRevision,
			Operation: stageOperation, SourceOperation: sourceOperation, ChangeID: change,
		})
	}
	if err != nil {
		return catalog.SourceDriverMutationReservation{}, err
	}
	if reservation.Mutation != step.OperationID || reservation.Claim != *prepared.Claim ||
		reservation.Authority != r.authority || reservation.FleetOwner != r.fleetOwner ||
		reservation.AuthorityGeneration != r.fleetGeneration || reservation.DeclarationDigest != r.declarationDigest ||
		reservation.Target != target || reservation.TargetCount != uint64(len(targets)) || reservation.TargetsDigest != digest {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrMutationConflict
	}
	for !reservation.TargetsPrepared {
		reservation, err = r.catalog.PrepareSourceDriverMutationReservationBatch(
			ctx, reservation.Mutation, reservation.Claim,
		)
		if err != nil {
			return catalog.SourceDriverMutationReservation{}, err
		}
	}
	if reservation.RequestBound {
		if reservation.RequestDigest != record.Digest {
			return catalog.SourceDriverMutationReservation{}, catalog.ErrMutationConflict
		}
		return reservation, nil
	}
	return r.catalog.BindSourceDriverMutationRequest(ctx, reservation.Mutation, reservation.Claim, record.Digest)
}

func (r *Runtime) settleCommittedPhysicalMutation(
	ctx context.Context,
	receipt catalog.SourceDriverCommittedReceipt,
) error {
	result := receipt.Result
	if result.Identity.Mode != catalog.SourceDriverMutation || result.Identity.Authority != r.authority ||
		result.Identity.AuthorityGeneration != r.fleetGeneration || result.Identity.Mutation == (catalog.MutationID{}) ||
		result.Identity.MutationReceiptDigest == ([32]byte{}) {
		return catalog.ErrIntegrity
	}
	if !receipt.Acknowledged {
		if err := r.executor.AcknowledgeMutation(ctx, r.authority, r.fleetGeneration,
			result.Identity.Mutation, Fingerprint(result.Identity.MutationReceiptDigest)); err != nil {
			return err
		}
		if err := r.catalog.AcknowledgeSourceDriverCommittedReceipt(ctx, result); err != nil {
			return err
		}
	}
	if !receipt.Forgotten {
		return r.catalog.ForgetSourceDriverCommittedReceipt(ctx, result)
	}
	return nil
}

func (r *Runtime) recoverCommittedSourceDriverReceipts(ctx context.Context) error {
	for {
		receipt, err := r.catalog.PendingSourceDriverCommittedReceipt(ctx, r.authority)
		if err != nil || receipt == nil {
			return err
		}
		if receipt.Result.Identity.Mode == catalog.SourceDriverMutation {
			if err := r.settleCommittedPhysicalMutation(ctx, *receipt); err != nil {
				return err
			}
			continue
		}
		if !receipt.Acknowledged {
			if err := r.catalog.AcknowledgeSourceDriverCommittedReceipt(ctx, receipt.Result); err != nil {
				return err
			}
		}
		if !receipt.Forgotten {
			if err := r.catalog.ForgetSourceDriverCommittedReceipt(ctx, receipt.Result); err != nil {
				return err
			}
		}
	}
}

func (r *Runtime) validateMutationStep(step tenant.SourceMutationStep) error {
	if step.TenantID == "" || step.Generation == 0 || step.OperationID == (catalog.MutationID{}) ||
		step.SourceID != string(r.authority) || step.ExpectedHead == 0 {
		return fmt.Errorf("%w: incomplete source mutation step", ErrMutationLocator)
	}
	found := false
	for _, spec := range r.currentTenants() {
		if spec.ID == step.TenantID && spec.Generation == step.Generation {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("%w: mutation tenant escaped the authority fleet", ErrMutationLocator)
	}
	return nil
}

func (r *Runtime) resolveMutationLocator(ctx context.Context, locator catalog.SourceLocator) (*PhysicalLocator, error) {
	if locator.SourceAuthority != r.authority || locator.SourceKey == "" || locator.SourceRevision == 0 {
		return nil, ErrMutationLocator
	}
	watermark, err := r.sourceRevision(ctx)
	if err != nil {
		return nil, err
	}
	if locator.SourceRevision != watermark {
		return nil, catalog.ErrSourceLocatorStale
	}
	binding, err := r.catalog.SourceObserverBindingForKey(ctx, r.authority, locator.SourceKey)
	if err != nil {
		return nil, errors.Join(ErrMutationLocator, err)
	}
	result := &PhysicalLocator{Source: locator, Logical: LogicalID(binding.LogicalID)}
	var after catalog.SourceIndexLocator
	for {
		page, err := r.catalog.SourceObserverBindingIndexPage(
			ctx, r.authority, locator.SourceKey, after, catalog.SourcePhysicalIndexPageLimit,
		)
		if errors.Is(err, catalog.ErrNotFound) {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		roots := make(map[RootID]RootSpec)
		for _, root := range r.currentRoots() {
			roots[root.ID] = root
		}
		for _, record := range page.Records {
			var entry PhysicalEntry
			if err := json.Unmarshal(record.Payload, &entry); err != nil {
				return nil, fmt.Errorf("%w: corrupt mutation physical locator", ErrQuarantined)
			}
			root, found := roots[entry.Root]
			if !found {
				return nil, fmt.Errorf("%w: mutation physical locator escaped roots", ErrQuarantined)
			}
			result.Bindings = append(result.Bindings, PhysicalBinding{Physical: entry, Root: root})
		}
		if page.Next == (catalog.SourceIndexLocator{}) {
			return result, nil
		}
		if compareSourceIndexLocator(page.Next, after) <= 0 {
			return nil, fmt.Errorf("%w: non-monotonic source binding page", ErrQuarantined)
		}
		after = page.Next
	}
}

func (r *Runtime) validateMutationPlan(
	ctx context.Context,
	step tenant.SourceMutationStep,
	plan MutationPlan,
) error {
	if len(plan.Program.Actions) == 0 || len(plan.Effects) == 0 {
		return fmt.Errorf("%w: empty source mutation program", ErrInvalidPlan)
	}
	rootMap := make(map[RootID]RootSpec)
	for _, root := range r.currentRoots() {
		rootMap[root.ID] = root
	}
	locators := make([]catalog.SourceIndexLocator, len(plan.Effects))
	for index, effect := range plan.Effects {
		locators[index] = catalog.SourceIndexLocator{RootID: string(effect.Path.Root), Relative: effect.Path.Relative}
	}
	records, err := r.sourcePhysicalIndexRecords(ctx, locators)
	if err != nil {
		return err
	}
	indexed := make(map[indexKey]PhysicalEntry, len(records))
	for _, record := range records {
		var entry PhysicalEntry
		if err := json.Unmarshal(record.Payload, &entry); err != nil {
			return fmt.Errorf("%w: corrupt mutation index", ErrQuarantined)
		}
		indexed[indexKey{root: RootID(record.RootID), relative: record.Relative}] = entry
	}
	for index, effect := range plan.Effects {
		root, found := rootMap[effect.Path.Root]
		validPath := validRelative(effect.Path.Relative) && root.Kind == RootDirectory
		if !found || !validPath ||
			(index > 0 && comparePathRef(plan.Effects[index-1].Path, effect.Path) >= 0) ||
			(effect.Outcome != MutationAbsent && effect.Outcome != MutationPresent) ||
			(effect.Outcome == MutationPresent && (effect.Kind < PhysicalFile || effect.Kind > PhysicalSymlink)) {
			return fmt.Errorf("%w: invalid or unordered mutation effect", ErrInvalidPlan)
		}
		entry, err := r.executor.Stat(ctx, root, effect.Path.Relative)
		if err != nil {
			return err
		}
		if effect.Before != physicalState(entry) {
			return fmt.Errorf("%w: mutation precondition does not match actor stat", ErrInvalidPlan)
		}
		prior, found := indexed[indexKey{root: effect.Path.Root, relative: effect.Path.Relative}]
		if effect.Before.Exists != found || (found && effect.Before != physicalState(prior)) {
			return fmt.Errorf("%w: mutation precondition does not match durable index", ErrInvalidPlan)
		}
	}
	effects := make(map[indexKey]struct{}, len(plan.Effects))
	for _, effect := range plan.Effects {
		effects[indexKey{root: effect.Path.Root, relative: effect.Path.Relative}] = struct{}{}
	}
	requestContentActions := 0
	actionPaths := make(map[PathRef]struct{}, len(plan.Program.Actions)*2)
	for _, action := range plan.Program.Actions {
		root, found := rootMap[action.Path.Root]
		validPath := validRelative(action.Path.Relative) && root.Kind == RootDirectory
		if !found || !validPath {
			return fmt.Errorf("%w: mutation action escaped declared roots", ErrInvalidPlan)
		}
		if _, found := effects[indexKey{root: action.Path.Root, relative: action.Path.Relative}]; !found {
			return fmt.Errorf("%w: mutation action has no exact post-state effect", ErrInvalidPlan)
		}
		if _, duplicate := actionPaths[action.Path]; duplicate {
			return fmt.Errorf("%w: mutation path has more than one writer", ErrInvalidPlan)
		}
		actionPaths[action.Path] = struct{}{}
		switch action.Kind {
		case MutationAtomicWriteFile:
			if action.From != nil || action.LinkTarget != "" || action.Mode == 0 || action.UseRequestContent == (len(action.Data) != 0) {
				return fmt.Errorf("%w: invalid atomic file write action", ErrInvalidPlan)
			}
		case MutationCreateDirectory:
			if action.From != nil || action.LinkTarget != "" || action.Mode == 0 || action.UseRequestContent || len(action.Data) != 0 {
				return fmt.Errorf("%w: invalid directory action", ErrInvalidPlan)
			}
		case MutationCreateSymlink:
			if action.From != nil || action.LinkTarget == "" || action.Mode != 0 || action.UseRequestContent || len(action.Data) != 0 {
				return fmt.Errorf("%w: invalid symlink action", ErrInvalidPlan)
			}
		case MutationRemove:
			if action.From != nil || action.LinkTarget != "" || action.Mode != 0 || action.UseRequestContent || len(action.Data) != 0 {
				return fmt.Errorf("%w: invalid remove action", ErrInvalidPlan)
			}
		case MutationRename:
			if action.From == nil || action.LinkTarget != "" || action.Mode != 0 || action.UseRequestContent || len(action.Data) != 0 {
				return fmt.Errorf("%w: invalid rename action", ErrInvalidPlan)
			}
			fromRoot, found := rootMap[action.From.Root]
			if !found || fromRoot.Kind != RootDirectory || !validRelative(action.From.Relative) {
				return fmt.Errorf("%w: rename source escaped declared roots", ErrInvalidPlan)
			}
			if _, found := effects[indexKey{root: action.From.Root, relative: action.From.Relative}]; !found {
				return fmt.Errorf("%w: rename source has no exact post-state effect", ErrInvalidPlan)
			}
			if _, duplicate := actionPaths[*action.From]; duplicate {
				return fmt.Errorf("%w: mutation path has more than one writer", ErrInvalidPlan)
			}
			actionPaths[*action.From] = struct{}{}
		default:
			return fmt.Errorf("%w: unknown semantic mutation action", ErrInvalidPlan)
		}
		if action.UseRequestContent {
			requestContentActions++
		}
	}
	if requestContentActions > 1 || (requestContentActions == 1) != step.Source.Operation.HasContent {
		return fmt.Errorf("%w: mutation request-content ownership does not match its journal", ErrInvalidPlan)
	}
	return nil
}

func physicalState(entry PhysicalEntry) ExpectedPhysicalState {
	if !entry.Exists {
		return ExpectedPhysicalState{}
	}
	return ExpectedPhysicalState{
		Exists: true, Kind: entry.Kind, Identity: entry.Identity,
		Mode: entry.Mode, UID: entry.UID, GID: entry.GID, Size: entry.Size, LinkTarget: entry.LinkTarget,
		MetadataFingerprint: entry.MetadataFingerprint, ContentFingerprint: entry.ContentFingerprint,
	}
}

func comparePathRef(left, right PathRef) int {
	if left.Root != right.Root {
		return compareString(string(left.Root), string(right.Root))
	}
	return compareString(left.Relative, right.Relative)
}
