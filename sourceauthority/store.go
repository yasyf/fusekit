package sourceauthority

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
)

const (
	maxSourcePhysicalIndexLocators = 8192
	maxSourceAuthorityLogicals     = 8192
)

type observerConfiguration struct {
	Authority       causal.SourceAuthorityID
	FleetOwner      catalog.SourceAuthorityFleetOwnerID
	FleetGeneration causal.Generation
	Stream          string
	RootEpoch       string
	RootDigest      [32]byte
	FleetDigest     [32]byte
	Reset           bool
	Roots           []catalog.SourceObserverRootRecord
	Checkpoints     []catalog.SourceObserverCheckpointRecord
}

// Store is the durable catalog boundary required by one source authority.
type Store interface {
	SourceObserverStream(context.Context, causal.SourceAuthorityID) (catalog.SourceObserverStreamRecord, error)
	SourceObserverRootsPage(context.Context, causal.SourceAuthorityID, string, int) (catalog.SourceObserverRootPage, error)
	SourceObserverCheckpointsPage(context.Context, causal.SourceAuthorityID, string, int) (catalog.SourceObserverCheckpointPage, error)
	SourceObserverAppliedCheckpointsPage(context.Context, causal.SourceAuthorityID, string, int) (catalog.SourceObserverAppliedCheckpointPage, error)
	BeginSourceObserverConfiguration(context.Context, catalog.SourceObserverConfigurationIdentity) error
	AppendSourceObserverConfigurationRoots(context.Context, causal.SourceAuthorityID, causal.OperationID, catalog.SourceObserverRootAppendPage) (catalog.SourceObserverConfigurationRef, error)
	AppendSourceObserverConfigurationCheckpoints(context.Context, causal.SourceAuthorityID, causal.OperationID, catalog.SourceObserverCheckpointAppendPage) (catalog.SourceObserverConfigurationRef, error)
	CommitSourceObserverConfiguration(context.Context, catalog.SourceObserverConfigurationRef) (catalog.SourceObserverStreamRecord, error)
	AcknowledgeSourceObserverConfiguration(context.Context, catalog.SourceObserverConfigurationRef) error
	AbortSourceObserverConfiguration(context.Context, causal.SourceAuthorityID, causal.OperationID) error
	AppendSourceObserverInbox(context.Context, catalog.SourceObserverInboxRecord) (uint64, error)
	SourceObserverNextInbox(context.Context, causal.SourceAuthorityID, uint64) (*catalog.SourceObserverInboxRecord, error)
	SourceObserverInboxPage(context.Context, causal.SourceAuthorityID, uint64, uint64, int) (catalog.SourceObserverInboxPage, error)
	RequireSourceObserverSnapshot(context.Context, causal.SourceAuthorityID) error
	QuarantineSourceObserver(context.Context, causal.SourceAuthorityID, string) error
	SettleSourceObserver(context.Context, catalog.SourceObserverSettlement) error
	AcknowledgeSourceObserverSettlement(context.Context, catalog.SourcePublicationStageRef) error

	BeginSourceSnapshotStage(context.Context, causal.SourceAuthorityID, string) error
	AbortSourceSnapshotStage(context.Context, causal.SourceAuthorityID, string) error
	AppendSourceSnapshotStagePage(context.Context, causal.SourceAuthorityID, string, catalog.SourceSnapshotPage) error
	SourceSnapshotStagePage(context.Context, causal.SourceAuthorityID, string, catalog.SourceIndexLocator, int) (catalog.SourceSnapshotPage, error)
	SourceSnapshotStageLookup(context.Context, catalog.SourceSnapshotPhysicalLookupRequest) (catalog.SourceSnapshotPhysicalLookupPage, error)
	BeginSourceSnapshotPublication(context.Context, catalog.SourceSnapshotIdentity) error
	SourceSnapshotRootLookup(context.Context, catalog.SourceSnapshotRootLookupRequest) (catalog.SourceSnapshotRootLookupPage, error)
	AppendSourceSnapshotPublication(context.Context, catalog.SourceSnapshotIdentity, catalog.SourceSnapshotPublicationPage) (catalog.SourceSnapshotStageRef, error)
	PromoteSourceSnapshot(context.Context, catalog.SourceSnapshotStageRef, catalog.SourceSnapshotSettlement) (catalog.SourceResult, error)

	SourceWatermark(context.Context, causal.SourceAuthorityID) (causal.Revision, error)
	PendingSourcePublicationStage(context.Context, causal.SourceAuthorityID) (*catalog.SourcePublicationStageRef, error)
	BeginSourcePublicationStage(context.Context, catalog.SourcePublicationStageIdentity) error
	AppendSourcePublicationStage(context.Context, catalog.SourcePublicationStageIdentity, catalog.SourcePublicationStagePage) (catalog.SourcePublicationStageRef, error)
	CommitSourcePublicationStage(context.Context, catalog.SourcePublicationStageRef) (catalog.SourcePublicationStageResult, error)
	AbortSourcePublicationStage(context.Context, causal.SourceAuthorityID, causal.OperationID) error
	SourceDriverCheckpoint(context.Context, causal.SourceAuthorityID) (catalog.SourceDriverCheckpoint, error)
	PreparedMutation(context.Context, catalog.TenantID, catalog.MutationID) (catalog.PreparedMutation, error)
	PendingSourceDriverStage(context.Context, causal.SourceAuthorityID) (*catalog.SourceDriverStageState, error)
	BeginSourceDriverStage(context.Context, catalog.SourceDriverStageIdentity) error
	AppendSourceDriverStage(context.Context, catalog.SourceDriverStageIdentity, catalog.SourceDriverStagePage) (catalog.SourceDriverStageState, error)
	PrepareSourceDriverTargetDeclarationBatch(context.Context, catalog.SourceDriverStageIdentity) (catalog.SourceDriverTargetDeclarationState, error)
	PrepareSourceDriverPublicationBatch(context.Context, catalog.SourceDriverStageIdentity) (catalog.SourceDriverPreparationState, error)
	CommitSourceDriverStage(context.Context, catalog.SourceDriverStageState) (catalog.SourceDriverStageResult, error)
	CommitSourceDriverMutation(context.Context, catalog.SourceDriverStageState) (catalog.SourceDriverStageResult, error)
	AcknowledgeSourceDriverCommittedReceipt(context.Context, catalog.SourceDriverStageResult) error
	ForgetSourceDriverCommittedReceipt(context.Context, catalog.SourceDriverStageResult) error
	AbortSourceDriverStage(context.Context, catalog.SourceDriverStageIdentity) error
	ReserveSourceDriverMutation(context.Context, catalog.SourceDriverMutationReservationRequest) (catalog.SourceDriverMutationReservation, error)
	SourceDriverMutationReservation(context.Context, catalog.MutationID) (catalog.SourceDriverMutationReservation, error)
	PrepareSourceDriverMutationReservationBatch(context.Context, catalog.MutationID, catalog.MutationClaim) (catalog.SourceDriverMutationReservation, error)
	BindSourceDriverMutationRequest(context.Context, catalog.MutationID, catalog.MutationClaim, [32]byte) (catalog.SourceDriverMutationReservation, error)
	RecordSourceDriverMutationReceipt(context.Context, catalog.MutationID, catalog.MutationClaim, catalog.SourceDriverMutationReceiptProof) (catalog.SourceDriverMutationReservation, error)
	PendingSourceDriverCommittedReceipt(context.Context, causal.SourceAuthorityID) (*catalog.SourceDriverCommittedReceipt, error)
	CommittedSourceDriverMutation(context.Context, causal.SourceAuthorityID, catalog.MutationID) (*catalog.SourceDriverCommittedReceipt, error)

	SourceAuthorityBindingLookup(context.Context, catalog.SourceAuthorityBindingLookupRequest) (catalog.SourceAuthorityBindingLookupPage, error)
	SourceObserverBindingForKey(context.Context, causal.SourceAuthorityID, catalog.SourceObjectKey) (catalog.SourceAuthorityBindingRecord, error)
	SourceObserverBindingIndexPage(context.Context, causal.SourceAuthorityID, catalog.SourceObjectKey, catalog.SourceIndexLocator, int) (catalog.SourcePhysicalIndexPage, error)
	SourcePhysicalIndexLookup(context.Context, catalog.SourcePhysicalIndexLookupRequest) (catalog.SourcePhysicalIndexLookupPage, error)
	SourcePhysicalIndexRecordsPage(context.Context, causal.SourceAuthorityID, catalog.SourceIndexLocator, int) (catalog.SourcePhysicalIndexPage, error)
	SourcePhysicalIndexRecordByIdentity(context.Context, causal.SourceAuthorityID, []byte) (catalog.SourcePhysicalIndexRecord, error)
	ReserveSourceAuthorityBinding(context.Context, causal.SourceAuthorityID, string, catalog.SourceObjectKey) (catalog.SourceAuthorityBindingRecord, error)

	StageOwnedContent(context.Context, contentstream.Source) (catalog.ContentRef, error)
	ReleaseUnclaimedContent(context.Context, []catalog.ContentRef) error

	SourceMutationExpectation(context.Context, causal.SourceAuthorityID, catalog.MutationID) (catalog.SourceMutationExpectationRecord, error)
	SourceMutationExpectationsPage(context.Context, causal.SourceAuthorityID, catalog.MutationID, int) (catalog.SourceMutationExpectationPage, error)
	ReserveSourceMutationExpectation(context.Context, catalog.SourceMutationExpectationReservation) error
	CompleteSourceMutationExpectation(context.Context, causal.SourceAuthorityID, catalog.MutationID, []byte) error
	RecoverSourceMutationExpectationReceipt(context.Context, causal.SourceAuthorityID, catalog.MutationID, []byte) error
	CompleteSourceMutationRepair(context.Context, causal.SourceAuthorityID, catalog.MutationID) error

	SourceDriverTargetCheckpoint(context.Context, causal.SourceAuthorityID, catalog.TenantID, catalog.Generation) (catalog.SourceDriverTargetCheckpoint, error)

	SourceAuthorityFleetHead(context.Context, catalog.SourceAuthorityFleetOwnerID) (catalog.SourceAuthorityFleetStatus, error)
	SourceAuthorityFleetPage(context.Context, catalog.SourceAuthorityFleetPageRequest) (catalog.SourceAuthorityFleetPage, error)
	ReconcileSourceAuthorityFleet(context.Context, catalog.SourceAuthorityFleetReconcileRequest) (catalog.SourceAuthorityFleetReconcileState, error)
	AbortSourceAuthorityFleet(context.Context, catalog.SourceAuthorityFleetAbortRequest) (catalog.SourceAuthorityFleetAbortReceipt, error)
	RetireSourceAuthority(context.Context, catalog.SourceAuthorityRetireRequest) (catalog.SourceAuthorityRetirementReceipt, error)
	AcknowledgeSourceAuthorityFleet(context.Context, catalog.SourceAuthorityFleetAcknowledgement) (catalog.SourceAuthorityFleetState, error)
	SourceAuthorityRuntimeStatus(context.Context, catalog.SourceAuthorityRuntimeRef) (catalog.SourceAuthorityRuntimeState, error)
	TakeoverSourceAuthorityRuntime(context.Context, catalog.SourceAuthorityRuntimeTakeover) error
	RecoverReapedSourceAuthorityRuntimes(context.Context, proc.ReapReceipt) (catalog.SourceAuthorityRuntimeRecoveryResult, error)
	OpenSourceAuthorityRuntime(context.Context, catalog.SourceAuthorityRuntimeFence) error
	CloseSourceAuthorityRuntime(context.Context, catalog.SourceAuthorityRuntimeFence) error
}

func (r *Runtime) sourceRevision(ctx context.Context) (causal.Revision, error) {
	checkpoint, err := r.catalog.SourceDriverCheckpoint(ctx, r.authority)
	if errors.Is(err, catalog.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return checkpoint.SourceRevision, nil
}

func (r *Runtime) loadSourceObserverFence(
	ctx context.Context,
	authority causal.SourceAuthorityID,
) (catalog.SourceObserverState, error) {
	stream, err := r.catalog.SourceObserverStream(ctx, authority)
	if err != nil {
		return catalog.SourceObserverState{}, err
	}
	state := catalog.SourceObserverState{Stream: stream}
	var after string
	for {
		page, err := r.catalog.SourceObserverRootsPage(
			ctx, authority, after, catalog.SourceObserverConfigurationPageLimit,
		)
		if err != nil {
			return catalog.SourceObserverState{}, err
		}
		state.Roots = append(state.Roots, page.Records...)
		if page.Next == "" {
			break
		}
		if page.Next <= after {
			return catalog.SourceObserverState{}, fmt.Errorf("%w: non-monotonic source root page", ErrQuarantined)
		}
		after = page.Next
	}
	after = ""
	for {
		page, err := r.catalog.SourceObserverCheckpointsPage(
			ctx, authority, after, catalog.SourceObserverConfigurationPageLimit,
		)
		if err != nil {
			return catalog.SourceObserverState{}, err
		}
		state.Checkpoints = append(state.Checkpoints, page.Records...)
		if page.Next == "" {
			break
		}
		if page.Next <= after {
			return catalog.SourceObserverState{}, fmt.Errorf("%w: non-monotonic source checkpoint page", ErrQuarantined)
		}
		after = page.Next
	}
	after = ""
	for {
		page, err := r.catalog.SourceObserverAppliedCheckpointsPage(
			ctx, authority, after, catalog.SourceObserverConfigurationPageLimit,
		)
		if err != nil {
			return catalog.SourceObserverState{}, err
		}
		state.AppliedCheckpoints = append(state.AppliedCheckpoints, page.Records...)
		if page.Next == "" {
			return state, nil
		}
		if page.Next <= after {
			return catalog.SourceObserverState{}, fmt.Errorf("%w: non-monotonic source applied-checkpoint page", ErrQuarantined)
		}
		after = page.Next
	}
}

func (r *Runtime) configureSourceObserver(
	ctx context.Context,
	config observerConfiguration,
) (stream catalog.SourceObserverStreamRecord, resultErr error) {
	_, operation, err := newCausalIDs()
	if err != nil {
		return catalog.SourceObserverStreamRecord{}, err
	}
	rootsDigest, err := catalog.SourceObserverRootsDigest(config.Roots)
	if err != nil {
		return catalog.SourceObserverStreamRecord{}, err
	}
	checkpointsDigest, err := catalog.SourceObserverCheckpointsDigest(config.Checkpoints)
	if err != nil {
		return catalog.SourceObserverStreamRecord{}, err
	}
	identity := catalog.SourceObserverConfigurationIdentity{
		Authority: config.Authority, FleetOwner: config.FleetOwner,
		FleetGeneration: config.FleetGeneration, Operation: operation, Stream: config.Stream,
		RootEpoch: config.RootEpoch, RootDigest: config.RootDigest, FleetDigest: config.FleetDigest,
		Reset: config.Reset, RootCount: uint64(len(config.Roots)),
		CheckpointCount: uint64(len(config.Checkpoints)),
		RootsDigest:     rootsDigest, CheckpointsDigest: checkpointsDigest,
	}
	if err := r.catalog.BeginSourceObserverConfiguration(ctx, identity); err != nil {
		return catalog.SourceObserverStreamRecord{}, err
	}
	owned := true
	defer func() {
		if owned {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalStageCleanupTimeout)
			defer cancel()
			resultErr = errors.Join(
				resultErr,
				r.catalog.AbortSourceObserverConfiguration(cleanupCtx, config.Authority, operation),
			)
		}
	}()
	var ref catalog.SourceObserverConfigurationRef
	var sequence uint64
	for start := 0; start < len(config.Roots); start += catalog.SourceObserverConfigurationPageLimit {
		end := min(start+catalog.SourceObserverConfigurationPageLimit, len(config.Roots))
		ref, err = r.catalog.AppendSourceObserverConfigurationRoots(
			ctx, config.Authority, operation,
			catalog.SourceObserverRootAppendPage{
				Sequence: sequence,
				Records:  config.Roots[start:end],
			},
		)
		if err != nil {
			return catalog.SourceObserverStreamRecord{}, err
		}
		sequence++
	}
	for start := 0; start < len(config.Checkpoints); start += catalog.SourceObserverConfigurationPageLimit {
		end := min(start+catalog.SourceObserverConfigurationPageLimit, len(config.Checkpoints))
		ref, err = r.catalog.AppendSourceObserverConfigurationCheckpoints(
			ctx, config.Authority, operation,
			catalog.SourceObserverCheckpointAppendPage{
				Sequence: sequence,
				Records:  config.Checkpoints[start:end],
			},
		)
		if err != nil {
			return catalog.SourceObserverStreamRecord{}, err
		}
		sequence++
	}
	stream, err = r.catalog.CommitSourceObserverConfiguration(ctx, ref)
	if err != nil {
		return catalog.SourceObserverStreamRecord{}, err
	}
	if err := r.catalog.AcknowledgeSourceObserverConfiguration(ctx, ref); err != nil {
		return catalog.SourceObserverStreamRecord{}, err
	}
	owned = false
	return stream, nil
}

func (r *Runtime) sourcePhysicalIndexRecords(
	ctx context.Context,
	locators []catalog.SourceIndexLocator,
) ([]catalog.SourcePhysicalIndexRecord, error) {
	if len(locators) > maxSourcePhysicalIndexLocators {
		return nil, fmt.Errorf("%w: source physical lookup exceeds %d locators", ErrSnapshotRequired, maxSourcePhysicalIndexLocators)
	}
	result := make([]catalog.SourcePhysicalIndexRecord, 0, len(locators))
	for start := 0; start < len(locators); start += catalog.SourceKeyedLookupLimit {
		end := min(start+catalog.SourceKeyedLookupLimit, len(locators))
		request, err := catalog.NewSourcePhysicalIndexLookupRequest(
			r.authority, uint32(start), locators[start:end],
		)
		if err != nil {
			return nil, err
		}
		page, err := r.catalog.SourcePhysicalIndexLookup(ctx, request)
		if err != nil {
			return nil, err
		}
		if err := page.Validate(request); err != nil {
			return nil, err
		}
		for _, entry := range page.Entries {
			if entry.Record != nil {
				result = append(result, *entry.Record)
			}
		}
	}
	return result, nil
}

func (r *Runtime) sourceAuthorityBindings(
	ctx context.Context,
	logicals []LogicalID,
) (map[LogicalID]catalog.SourceAuthorityBindingRecord, error) {
	if len(logicals) > maxSourceAuthorityLogicals {
		return nil, fmt.Errorf("%w: source binding lookup exceeds %d logicals", ErrSnapshotRequired, maxSourceAuthorityLogicals)
	}
	result := make(map[LogicalID]catalog.SourceAuthorityBindingRecord, len(logicals))
	for start := 0; start < len(logicals); start += catalog.SourceKeyedLookupLimit {
		end := min(start+catalog.SourceKeyedLookupLimit, len(logicals))
		values := make([]string, end-start)
		for index, logical := range logicals[start:end] {
			values[index] = string(logical)
		}
		request, err := catalog.NewSourceAuthorityBindingLookupRequest(r.authority, uint32(start), values)
		if err != nil {
			return nil, err
		}
		page, err := r.catalog.SourceAuthorityBindingLookup(ctx, request)
		if err != nil {
			return nil, err
		}
		if err := page.Validate(request); err != nil {
			return nil, err
		}
		for _, entry := range page.Entries {
			if entry.Record != nil {
				result[LogicalID(entry.Logical)] = *entry.Record
			}
		}
	}
	return result, nil
}

func sourceSnapshotStageEntries(
	ctx context.Context,
	store Store,
	authority causal.SourceAuthorityID,
	snapshot string,
	locators []catalog.SourceIndexLocator,
) ([]*catalog.SourcePhysicalIndexRecord, error) {
	result := make([]*catalog.SourcePhysicalIndexRecord, 0, len(locators))
	for start := 0; start < len(locators); start += catalog.SourceKeyedLookupLimit {
		end := min(start+catalog.SourceKeyedLookupLimit, len(locators))
		request, err := catalog.NewSourceSnapshotPhysicalLookupRequest(
			authority, snapshot, uint32(start), locators[start:end],
		)
		if err != nil {
			return nil, err
		}
		page, err := store.SourceSnapshotStageLookup(ctx, request)
		if err != nil {
			return nil, err
		}
		if err := page.Validate(request); err != nil {
			return nil, err
		}
		for _, entry := range page.Entries {
			result = append(result, entry.Record)
		}
	}
	return result, nil
}

func compareSourceIndexLocator(left, right catalog.SourceIndexLocator) int {
	if left.RootID < right.RootID {
		return -1
	}
	if left.RootID > right.RootID {
		return 1
	}
	if left.Relative < right.Relative {
		return -1
	}
	if left.Relative > right.Relative {
		return 1
	}
	return 0
}
