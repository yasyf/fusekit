package catalogservice

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/convergence"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/tenant"
)

// MutationAdapter binds streamed daemonkit mutations to the durable catalog and tenant actor.
type MutationAdapter struct {
	Store   CatalogMutationStore
	Runtime *tenant.TenantRuntime
	Engine  *convergence.Engine
}

// StageMutation durably stages request bytes without holding tenant lifecycle admission.
func (a MutationAdapter) StageMutation(
	ctx context.Context,
	_ Identity,
	authorization Authorization,
	tenantID catalog.TenantID,
	requestID catalogproto.MutationRequestID,
	generation catalog.Generation,
	hasContent bool,
	source contentstream.Source,
) (stage MutationStage, err error) {
	if a.Store == nil || a.Runtime == nil || a.Engine == nil {
		return MutationStage{}, errors.New("catalog service: mutation adapter is incomplete")
	}
	if hasContent && source == nil {
		return MutationStage{}, errors.New("catalog service: mutation content source is required")
	}
	if !hasContent && source != nil {
		return MutationStage{}, errors.New("catalog service: contentless mutation carried a source")
	}
	transferred := false
	defer func() {
		if source != nil && !transferred {
			settleErr := source.Settle(err)
			waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationStageCleanupTimeout)
			waitErr := source.Wait(waitCtx)
			cancel()
			err = errors.Join(err, settleErr, waitErr)
		}
	}()
	if err := validateAuthorization(authorization, catalogproto.OperationCatalogMutate); err != nil {
		return MutationStage{}, err
	}
	if authorization.Route.Tenant != tenantID || authorization.Route.Generation != generation {
		return MutationStage{}, fmt.Errorf("%w: mutation route changed", catalog.ErrIntegrity)
	}
	stage = MutationStage{
		Token: string(requestID), RequestID: requestID, Tenant: tenantID,
		Generation: generation, authorization: authorization, state: &mutationStageState{},
	}
	if hasContent {
		transferred = true
		ref, err := a.Store.StageOwnedContent(ctx, source)
		if err != nil {
			return MutationStage{}, err
		}
		stage.Size = ref.Size
		stage.content = &ref
		stage.state.abort = func(abortCtx context.Context) error {
			return a.Store.ReleaseUnclaimedContent(abortCtx, []catalog.ContentRef{ref})
		}
		return stage, nil
	}
	return stage, nil
}

// SubmitMutation prepares exactly one catalog intent and waits for its tenant actor commit.
func (a MutationAdapter) SubmitMutation(
	ctx context.Context,
	_ Identity,
	authorization Authorization,
	submission MutationSubmission,
) (MutationResult, error) {
	stage := submission.Stage
	if stage.Token == "" || stage.authorization != authorization {
		return MutationResult{}, fmt.Errorf("%w: mutation stage authorization changed", catalog.ErrIntegrity)
	}
	request := submission.Request
	if request.RequestID != stage.RequestID || catalog.Generation(request.Generation) != stage.Generation {
		return MutationResult{}, fmt.Errorf("%w: mutation stage identity changed", catalog.ErrIntegrity)
	}
	intent, err := a.intent(ctx, authorization, stage.Tenant, request, stage.content)
	if err != nil {
		return MutationResult{}, err
	}
	lease, err := a.Runtime.AcquireGeneration(ctx, stage.Tenant, stage.Generation)
	if err != nil {
		return MutationResult{}, err
	}
	defer lease.Release()
	prepared, err := a.Store.BeginMutation(ctx, stage.Tenant, catalog.Revision(request.ExpectedRevision), intent)
	if err != nil {
		return MutationResult{}, err
	}
	if prepared.Tenant != stage.Tenant || prepared.OperationID == (catalog.MutationID{}) ||
		prepared.ExpectedHead != catalog.Revision(request.ExpectedRevision) {
		return MutationResult{}, fmt.Errorf("%w: prepared mutation identity changed", catalog.ErrIntegrity)
	}
	if stage.content != nil {
		preparedRef, found := mutationIntentContent(prepared.Intent)
		if !found || preparedRef.Hash != stage.content.Hash || preparedRef.Size != stage.content.Size {
			return MutationResult{}, catalog.ErrMutationConflict
		}
		if preparedRef.Stage != stage.content.Stage {
			if err := a.Store.ReleaseUnclaimedContent(context.WithoutCancel(ctx), []catalog.ContentRef{*stage.content}); err != nil {
				return MutationResult{}, err
			}
		}
	}
	stage.claim()
	state, err := lease.Prepare(ctx, prepared.ExpectedHead+1)
	if err != nil {
		return MutationResult{}, err
	}
	if !state.Prepared() {
		return MutationResult{}, fmt.Errorf("%w: tenant actor returned an unprepared state", catalog.ErrIntegrity)
	}
	if err := a.Engine.Drain(ctx); err != nil {
		return MutationResult{}, err
	}
	record, err := a.Store.Mutation(ctx, stage.Tenant, prepared.OperationID)
	if err != nil {
		return MutationResult{}, err
	}
	primary := catalog.ObjectID(record.Primary)
	result := MutationResult{
		RequestID: stage.RequestID, OperationID: prepared.OperationID,
		Revision: record.Revision, PrimaryID: &primary,
	}
	if record.Secondary != ([16]byte{}) {
		secondary := catalog.ObjectID(record.Secondary)
		result.SecondaryID = &secondary
	}
	return result, nil
}

func (a MutationAdapter) intent(
	ctx context.Context,
	authorization Authorization,
	tenantID catalog.TenantID,
	request catalogproto.MutationRequest,
	content *catalog.ContentRef,
) (catalog.MutationIntent, error) {
	sourceID, sourceMetadata, err := mutationSource(authorization)
	if err != nil {
		return catalog.MutationIntent{}, err
	}
	origin := catalog.CausalOrigin{Cause: causal.CauseDaemonWrite}
	if authorization.Role == RoleFileProvider {
		origin = catalog.CausalOrigin{
			Cause: causal.CauseProviderMutation, Domain: causal.DomainID(authorization.Route.Domain),
			Generation: causal.Generation(authorization.Route.Generation),
		}
	}
	intent := catalog.MutationIntent{SourceID: sourceID, SourceMetadata: sourceMetadata, Origin: origin}
	contentUpdate := func() (*catalog.ContentUpdate, error) {
		if !request.HasContent {
			return nil, nil
		}
		if content == nil || request.ContentRevision == nil {
			return nil, fmt.Errorf("%w: content mutation has no durable stage", catalog.ErrIntegrity)
		}
		return &catalog.ContentUpdate{Revision: catalog.Revision(*request.ContentRevision), Ref: *content}, nil
	}
	switch request.Kind {
	case catalogproto.MutationKindCreate:
		parent, err := catalogObjectID(*request.ParentID)
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		kind := catalog.KindDirectory
		var ref catalog.ContentRef
		var contentRevision catalog.Revision
		var linkTarget string
		switch *request.ObjectKind {
		case catalogproto.ObjectKindDirectory:
		case catalogproto.ObjectKindFile:
			kind = catalog.KindFile
			if content == nil || request.ContentRevision == nil {
				return catalog.MutationIntent{}, fmt.Errorf("%w: file create has no durable content", catalog.ErrIntegrity)
			}
			ref = *content
			contentRevision = catalog.Revision(*request.ContentRevision)
		case catalogproto.ObjectKindSymlink:
			kind = catalog.KindSymlink
			contentRevision = catalog.Revision(*request.ContentRevision)
			linkTarget = *request.LinkTarget
		default:
			return catalog.MutationIntent{}, fmt.Errorf("%w: unknown object kind %q", catalog.ErrInvalidObject, *request.ObjectKind)
		}
		intent.Create = &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: parent, Name: *request.Name, Kind: kind, Mode: *request.Mode,
			ContentRevision: contentRevision, Content: ref, LinkTarget: linkTarget, Visibility: visibilityForAuthorization(authorization),
		}}
	case catalogproto.MutationKindRevise:
		id, err := catalogObjectID(*request.ObjectID)
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		parent, err := catalogObjectID(*request.ParentID)
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		current, err := a.Store.Inspect(ctx, tenantID, id)
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		update, err := contentUpdate()
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		intent.Revise = &catalog.ReviseMutation{Object: id, Spec: catalog.RevisionSpec{
			Parent: parent, Name: *request.Name, Mode: *request.Mode, Content: update,
			Convergence: current.Convergence, Visibility: current.Visibility,
		}}
	case catalogproto.MutationKindDelete:
		id, err := catalogObjectID(*request.ObjectID)
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		intent.Delete = &catalog.DeleteMutation{Object: id}
	case catalogproto.MutationKindReplace:
		source, err := catalogObjectID(*request.ObjectID)
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		target, err := catalogObjectID(*request.TargetID)
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		var parent *catalog.ObjectID
		if request.ParentID != nil {
			value, err := catalogObjectID(*request.ParentID)
			if err != nil {
				return catalog.MutationIntent{}, err
			}
			parent = &value
		}
		update, err := contentUpdate()
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		intent.Replace = &catalog.ReplaceMutation{
			Source: source, Target: target, Parent: parent, Name: request.Name, Mode: request.Mode, Content: update,
		}
	default:
		return catalog.MutationIntent{}, fmt.Errorf("%w: unknown mutation kind %q", catalog.ErrInvalidObject, request.Kind)
	}
	return intent, nil
}

func visibilityForAuthorization(authorization Authorization) catalog.Visibility {
	return catalog.Visibility{
		Mount:        authorization.Presentation == catalog.PresentationMount,
		FileProvider: authorization.Presentation == catalog.PresentationFileProvider,
	}
}

func mutationSource(authorization Authorization) (string, string, error) {
	metadata := fmt.Sprintf("generation=%d", authorization.Route.Generation)
	switch authorization.Role {
	case RoleFileProvider:
		if authorization.Route.Domain == "" {
			return "", "", fmt.Errorf("%w: File Provider mutation has no bound domain", catalog.ErrIntegrity)
		}
		return "fileprovider:" + string(authorization.Route.Domain), metadata, nil
	case RoleMount:
		return "mount:" + authorization.Principal, metadata, nil
	default:
		return "", "", fmt.Errorf("%w: mutation has an unknown authenticated role", catalog.ErrIntegrity)
	}
}

func mutationIntentContent(intent catalog.MutationIntent) (catalog.ContentRef, bool) {
	switch {
	case intent.Create != nil && intent.Create.Spec.Kind == catalog.KindFile:
		return intent.Create.Spec.Content, true
	case intent.Revise != nil && intent.Revise.Spec.Content != nil:
		return intent.Revise.Spec.Content.Ref, true
	case intent.Replace != nil && intent.Replace.Content != nil:
		return intent.Replace.Content.Ref, true
	default:
		return catalog.ContentRef{}, false
	}
}

// FileProviderPresentationPreparer returns one current-activation OS observation.
type FileProviderPresentationPreparer interface {
	PrepareFileProviderPresentation(context.Context, catalog.TenantID, catalog.Generation) (catalog.FileProviderDomain, error)
}

// MountPresentationPreparer returns only after the native backend is ready.
type MountPresentationPreparer interface {
	PrepareMountPresentation(context.Context, catalog.TenantID, catalog.Generation) error
}

// TenantPreparationStore owns the durable application, presentation, and
// activation transaction for one desired tenant generation.
type TenantPreparationStore interface {
	TenantLifecycle(context.Context, string, catalog.TenantID) (catalog.TenantLifecycleState, error)
	StageApplication(context.Context, catalog.StageApplicationRequest) (catalog.StagedViewLease, catalog.TenantLifecycleState, error)
	RecordPresentation(context.Context, catalog.PresentationReceipt) (catalog.TenantLifecycleState, error)
	TenantTargetingRevision(context.Context, catalog.TenantID) (uint64, error)
	ActivateTenant(context.Context, catalog.ActivateTenantRequest) (catalog.TenantActivationResult, error)
}

// PreparationAdapter joins the tenant catalog lane, presentation activation,
// and external domain lane without collapsing revisions.
type PreparationAdapter struct {
	Store                TenantPreparationStore
	Runtime              *tenant.TenantRuntime
	Engine               *convergence.Engine
	Barrier              sourceauthority.Barrier
	Mounts               MountPresentationPreparer
	Presentations        FileProviderPresentationPreparer
	ActivationGeneration string
}

// PrepareTenant returns the catalog/source proof for one exact tenant generation.
func (a PreparationAdapter) PrepareTenant(
	ctx context.Context,
	_ Identity,
	tenantID catalog.TenantID,
	request catalogproto.PrepareTenantRequest,
) (catalogproto.TenantPreparationProof, error) {
	if a.Store == nil || a.Runtime == nil || a.Barrier == nil || a.Engine == nil {
		return catalogproto.TenantPreparationProof{}, errors.New("catalog service: tenant preparation adapter is incomplete")
	}
	if a.ActivationGeneration == "" || request.ActivationGeneration != a.ActivationGeneration {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: runtime activation generation changed", catalog.ErrGenerationMismatch)
	}
	barrier, err := a.Barrier.Barrier(ctx, tenantID, catalog.Generation(request.Generation))
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	target := barrier.Target
	source := barrier.Source
	if target.Tenant != tenantID || target.Generation != catalog.Generation(request.Generation) ||
		target.CatalogRevision == 0 || source.Authority == "" || source.SourceRevision == 0 ||
		target.SourceRevision != source.SourceRevision || source.ChangeID == (causal.ChangeID{}) ||
		source.SourceOperation == (causal.OperationID{}) || source.PublicationID == (causal.OperationID{}) ||
		source.PublicationDigest == ([sha256.Size]byte{}) {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: source authority returned an invalid preparation target", catalog.ErrIntegrity)
	}
	desired, err := desiredTenantSpec(a.Runtime.Specs(), tenantID, catalog.Generation(request.Generation))
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	lifecycle, err := a.Store.TenantLifecycle(ctx, string(desired.OwnerID), tenantID)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	if lifecycle.OwnerID == "" || lifecycle.Intent.Kind != catalog.TenantIntentPresent ||
		lifecycle.Intent.TargetGeneration != catalog.Generation(request.Generation) || lifecycle.Target == nil ||
		tenantSpec(lifecycle.Target.Definition) != desired {
		return catalogproto.TenantPreparationProof{}, catalog.ErrTenantLifecycleStale
	}
	requestedBackend, err := presentationBackend(request.Presentation)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	if !lifecycle.Target.RequiredBackends.Has(requestedBackend) {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: tenant has no requested presentation", catalog.ErrInvalidObject)
	}
	if lifecycle.Activation.ActiveGeneration != catalog.Generation(request.Generation) {
		lifecycle, err = a.activateDesiredGeneration(ctx, lifecycle, source)
		if err != nil {
			return catalogproto.TenantPreparationProof{}, err
		}
	}
	receipt, err := activationReceipt(lifecycle, catalog.Generation(request.Generation))
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	if err := a.Runtime.InstallActivatedTenant(ctx, receipt); err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	presentation, err := a.preparePresentation(ctx, request.Presentation, lifecycle.Active.Definition)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	lease, err := a.Runtime.AcquireGeneration(ctx, tenantID, catalog.Generation(request.Generation))
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	state, err := lease.Prepare(ctx, target.CatalogRevision)
	lease.Release()
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	if !state.Prepared() {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: tenant preparation did not converge", catalog.ErrIntegrity)
	}
	if err := a.Engine.Tick(ctx); err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	return catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: catalogproto.TenantID(tenantID), Generation: uint64(state.Generation),
			Requested: uint64(target.CatalogRevision), Desired: uint64(state.Desired), Observed: uint64(state.Observed),
			Verified: uint64(state.Verified), Applied: uint64(state.Applied),
		},
		Presentation:    presentation,
		SourceAuthority: catalogproto.SourceAuthorityID(source.Authority),
		SourceRevision:  uint64(source.SourceRevision),
		CatalogRevision: uint64(target.CatalogRevision),
		ChangeID:        catalogproto.ChangeID(hex.EncodeToString(source.ChangeID[:])),
		OperationID:     catalogproto.OperationID(hex.EncodeToString(source.SourceOperation[:])),
	}, nil
}

func desiredTenantSpec(specs []tenant.TenantSpec, id catalog.TenantID, generation catalog.Generation) (tenant.TenantSpec, error) {
	for _, spec := range specs {
		if spec.ID != id {
			continue
		}
		if spec.Generation != generation {
			return tenant.TenantSpec{}, tenant.ErrGenerationConflict
		}
		return spec, nil
	}
	return tenant.TenantSpec{}, tenant.ErrTenantNotFound
}

func (a PreparationAdapter) activateDesiredGeneration(
	ctx context.Context,
	state catalog.TenantLifecycleState,
	source catalog.SourceDriverCheckpoint,
) (catalog.TenantLifecycleState, error) {
	target := state.Target
	mutation := func(operation catalog.TenantOperationID) catalog.TenantMutation {
		return catalog.TenantMutation{
			OperationID: operation, HolderRuntimeGeneration: a.ActivationGeneration,
			OwnerID: state.OwnerID, ExpectedIntentRevision: state.Intent.Revision,
		}
	}
	lease, resumed, err := stagedApplicationLease(state, a.ActivationGeneration)
	if err != nil {
		return catalog.TenantLifecycleState{}, err
	}
	staged := state
	if !resumed {
		stageOperation := preparationOperationID(
			"stage", a.ActivationGeneration, state.Intent.Tenant, state.Intent.TargetGeneration,
			state.Intent.Revision, source.PublicationID, catalog.StagedViewID{}, 0,
		)
		lease, staged, err = a.Store.StageApplication(ctx, catalog.StageApplicationRequest{
			Mutation: mutation(stageOperation), Tenant: state.Intent.Tenant,
			Generation: state.Intent.TargetGeneration, Authority: source.Authority,
			Publication: source.PublicationID, PublicationDigest: source.PublicationDigest,
		})
		if err != nil {
			return catalog.TenantLifecycleState{}, err
		}
		exact, found, exactErr := stagedApplicationLease(staged, a.ActivationGeneration)
		if exactErr != nil {
			return catalog.TenantLifecycleState{}, exactErr
		}
		if !found || exact != lease {
			return catalog.TenantLifecycleState{}, catalog.ErrTenantLifecycleStale
		}
	}
	if staged.Target == nil || staged.Target.Definition.Generation != target.Definition.Generation ||
		staged.Target.SpecHash != target.SpecHash {
		return catalog.TenantLifecycleState{}, catalog.ErrTenantLifecycleStale
	}
	for _, backend := range staged.Target.RequiredBackends.Backends() {
		kind, err := presentationKind(backend)
		if err != nil {
			return catalog.TenantLifecycleState{}, err
		}
		proof, err := a.preparePresentation(ctx, kind, staged.Target.Definition)
		if err != nil {
			return catalog.TenantLifecycleState{}, err
		}
		backendGeneration, err := presentationGeneration(proof)
		if err != nil {
			return catalog.TenantLifecycleState{}, err
		}
		operation := preparationOperationID(
			"presentation", a.ActivationGeneration, lease.Tenant, lease.Generation,
			lease.IntentRevision, lease.SourcePublication, lease.ViewID, backend,
		)
		if _, err := a.Store.RecordPresentation(ctx, catalog.PresentationReceipt{
			Mutation: mutation(operation), Lease: lease, Backend: backend,
			BackendGeneration: backendGeneration, ObservedRevision: lease.CatalogHead,
		}); err != nil {
			return catalog.TenantLifecycleState{}, err
		}
	}
	targetingRevision, err := a.Store.TenantTargetingRevision(ctx, lease.Tenant)
	if err != nil {
		return catalog.TenantLifecycleState{}, err
	}
	activateOperation := preparationOperationID(
		"activate", a.ActivationGeneration, lease.Tenant, lease.Generation,
		lease.IntentRevision, lease.SourcePublication, lease.ViewID, 0,
	)
	result, err := a.Store.ActivateTenant(ctx, catalog.ActivateTenantRequest{
		Mutation: mutation(activateOperation), Tenant: lease.Tenant, Generation: lease.Generation,
		ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
		ExpectedActivationRevision: staged.Activation.Revision,
		ExpectedActiveGeneration:   staged.Activation.ActiveGeneration,
		CausePublications:          []causal.OperationID{lease.SourcePublication},
		ExpectedTargetingRevision:  targetingRevision,
	})
	if err != nil {
		return catalog.TenantLifecycleState{}, err
	}
	return result.State, nil
}

func stagedApplicationLease(
	state catalog.TenantLifecycleState,
	holderGeneration string,
) (catalog.StagedViewLease, bool, error) {
	if state.Target == nil || state.Intent.Kind != catalog.TenantIntentPresent ||
		state.Intent.Tenant == "" || state.Intent.TargetGeneration == 0 || state.Intent.Revision == 0 {
		return catalog.StagedViewLease{}, false, catalog.ErrTenantLifecycleStale
	}
	var application *catalog.TenantApplication
	for index := range state.Applications {
		candidate := &state.Applications[index]
		if candidate.Generation != state.Intent.TargetGeneration {
			continue
		}
		if application != nil {
			return catalog.StagedViewLease{}, false, catalog.ErrIntegrity
		}
		application = candidate
	}
	if application == nil || application.Phase == catalog.TenantApplicationPending {
		return catalog.StagedViewLease{}, false, nil
	}
	if application.Phase != catalog.TenantApplicationStaged || application.Tenant != state.Intent.Tenant ||
		application.IntentRevision != state.Intent.Revision || application.ContentSourceID != state.Target.Definition.ContentSourceID ||
		application.SourceAuthority != causal.SourceAuthorityID(state.Target.Definition.ContentSourceID) ||
		application.SourcePublication == (causal.OperationID{}) || application.ViewID == (catalog.StagedViewID{}) ||
		application.ViewDigest == ([sha256.Size]byte{}) || application.StagedCatalogHead == 0 ||
		application.StagedHeadDigest == ([sha256.Size]byte{}) || application.StagedSourceRevision == 0 ||
		application.PublicationDigest == ([sha256.Size]byte{}) || application.OperationID == (catalog.TenantOperationID{}) {
		return catalog.StagedViewLease{}, false, catalog.ErrTenantLifecycleStale
	}
	if application.HolderRuntimeGeneration != holderGeneration {
		return catalog.StagedViewLease{}, false, catalog.ErrGenerationMismatch
	}
	expectedOperation := preparationOperationID(
		"stage", holderGeneration, application.Tenant, application.Generation,
		application.IntentRevision, application.SourcePublication, catalog.StagedViewID{}, 0,
	)
	if application.OperationID != expectedOperation {
		return catalog.StagedViewLease{}, false, catalog.ErrTenantLifecycleStale
	}
	return catalog.StagedViewLease{
		Tenant: application.Tenant, Generation: application.Generation, IntentRevision: application.IntentRevision,
		ViewID: application.ViewID, ViewDigest: application.ViewDigest,
		CatalogHead: application.StagedCatalogHead, HeadDigest: application.StagedHeadDigest,
		SourceRevision: application.StagedSourceRevision, HolderRuntimeGeneration: application.HolderRuntimeGeneration,
		OperationID: application.OperationID, SourceAuthority: application.SourceAuthority,
		SourcePublication: application.SourcePublication,
	}, true, nil
}

func activationReceipt(state catalog.TenantLifecycleState, generation catalog.Generation) (tenant.ActivationReceipt, error) {
	if state.OwnerID == "" || state.Intent.Kind != catalog.TenantIntentPresent || state.Intent.Tenant == "" ||
		state.Intent.TargetGeneration != generation || state.Intent.Revision == 0 || state.Active == nil ||
		state.Active.Definition.OwnerID != state.OwnerID || state.Active.Definition.Tenant != state.Intent.Tenant ||
		state.Active.Definition.Generation != generation ||
		state.Activation.Tenant != state.Intent.Tenant || state.Activation.ActiveGeneration != generation ||
		state.Activation.ActiveView == (catalog.StagedViewID{}) || state.Activation.ActiveCatalogHead == 0 ||
		state.Activation.Revision == 0 {
		return tenant.ActivationReceipt{}, catalog.ErrTenantLifecycleStale
	}
	var application *catalog.TenantApplication
	for _, row := range state.Applications {
		if row.Generation != generation {
			continue
		}
		candidate := row
		if application != nil {
			return tenant.ActivationReceipt{}, catalog.ErrIntegrity
		}
		application = &candidate
	}
	if application == nil ||
		(application.Phase != catalog.TenantApplicationStaged && application.Phase != catalog.TenantApplicationRetiring) ||
		application.Tenant != state.Intent.Tenant || application.IntentRevision != state.Intent.Revision ||
		application.ViewID != state.Activation.ActiveView ||
		application.StagedCatalogHead != state.Activation.ActiveCatalogHead ||
		application.ViewDigest == ([sha256.Size]byte{}) || application.StagedHeadDigest == ([sha256.Size]byte{}) {
		return tenant.ActivationReceipt{}, catalog.ErrTenantLifecycleStale
	}
	required := state.Active.RequiredBackends.Backends()
	if len(required) == 0 ||
		state.Active.Definition.Presentations.Has(catalog.PresentationMount) != state.Active.RequiredBackends.Has(catalog.TenantBackendNative) ||
		state.Active.Definition.Presentations.Has(catalog.PresentationFileProvider) != state.Active.RequiredBackends.Has(catalog.TenantBackendBroker) {
		return tenant.ActivationReceipt{}, catalog.ErrTenantLifecycleStale
	}
	activeRows := make([]catalog.PresentationMaterialization, 0, len(required))
	for _, row := range state.Presentations {
		if row.Generation == generation {
			activeRows = append(activeRows, row)
		}
	}
	if len(activeRows) != len(required) {
		return tenant.ActivationReceipt{}, catalog.ErrTenantLifecycleStale
	}
	for _, backend := range required {
		found := false
		for _, row := range activeRows {
			if row.Backend != backend {
				continue
			}
			if found || row.Tenant != state.Intent.Tenant || row.Phase != catalog.PresentationMaterializationActive ||
				row.IntentRevision != application.IntentRevision || row.ViewID != application.ViewID ||
				row.ViewDigest != application.ViewDigest || row.ObservedRevision != application.StagedCatalogHead ||
				row.BackendGeneration == "" {
				return tenant.ActivationReceipt{}, catalog.ErrTenantLifecycleStale
			}
			found = true
		}
		if !found {
			return tenant.ActivationReceipt{}, catalog.ErrTenantLifecycleStale
		}
	}
	return tenant.ActivationReceipt{
		Owner: tenant.OwnerID(state.OwnerID), Tenant: state.Intent.Tenant, Generation: generation,
		ActivationRevision: state.Activation.Revision, CatalogHead: state.Activation.ActiveCatalogHead,
		ViewID: state.Activation.ActiveView, HeadDigest: application.StagedHeadDigest,
	}, nil
}

func tenantSpec(provision catalog.TenantProvision) tenant.TenantSpec {
	var fileProvider tenant.FileProviderSpec
	if provision.FileProvider.Enabled() {
		fileProvider = tenant.FileProviderSpec{
			Enabled: true, PresentationInstanceID: provision.FileProvider.PresentationInstanceID,
			DisplayName: provision.FileProvider.DisplayName,
		}
	}
	return tenant.TenantSpec{
		OwnerID: tenant.OwnerID(provision.OwnerID), ID: provision.Tenant,
		Mount:   tenant.MountSpec{PresentationRoot: provision.Mount.PresentationRoot},
		Backing: tenant.BackingSpec{Root: provision.BackingRoot},
		Content: tenant.ContentSource{ID: provision.ContentSourceID},
		Traits: tenant.TenantTraits{
			Access: provision.Access, CaseSensitivity: provision.CasePolicy, Presentations: provision.Presentations,
		},
		FileProvider: fileProvider, Generation: provision.Generation,
	}
}

func presentationKind(backend catalog.TenantBackend) (catalogproto.PresentationKind, error) {
	switch backend {
	case catalog.TenantBackendNative:
		return catalogproto.PresentationKindMount, nil
	case catalog.TenantBackendBroker:
		return catalogproto.PresentationKindFileProvider, nil
	default:
		return 0, fmt.Errorf("%w: unknown tenant backend", catalog.ErrIntegrity)
	}
}

func presentationBackend(kind catalogproto.PresentationKind) (catalog.TenantBackend, error) {
	switch kind {
	case catalogproto.PresentationKindMount:
		return catalog.TenantBackendNative, nil
	case catalogproto.PresentationKindFileProvider:
		return catalog.TenantBackendBroker, nil
	default:
		return 0, fmt.Errorf("%w: unknown requested presentation", catalog.ErrInvalidObject)
	}
}

func presentationGeneration(proof catalogproto.PresentationProof) (string, error) {
	switch proof.Kind {
	case catalogproto.PresentationKindMount:
		if proof.Mount != nil && proof.FileProvider == nil && proof.Mount.ActivationGeneration != "" {
			return proof.Mount.ActivationGeneration, nil
		}
	case catalogproto.PresentationKindFileProvider:
		if proof.FileProvider != nil && proof.Mount == nil && proof.FileProvider.ActivationGeneration != "" {
			return proof.FileProvider.ActivationGeneration, nil
		}
	}
	return "", fmt.Errorf("%w: presentation proof is not exact", catalog.ErrIntegrity)
}

func preparationOperationID(
	step, holder string,
	tenantID catalog.TenantID,
	generation catalog.Generation,
	intent catalog.TenantIntentRevision,
	publication causal.OperationID,
	view catalog.StagedViewID,
	backend catalog.TenantBackend,
) catalog.TenantOperationID {
	digest := sha256.New()
	_, _ = digest.Write([]byte("github.com/yasyf/fusekit/catalogservice/prepare-tenant/v1\x00"))
	for _, value := range []string{step, holder, string(tenantID)} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = digest.Write(size[:])
		_, _ = digest.Write([]byte(value))
	}
	var encoded [8]byte
	for _, value := range []uint64{uint64(generation), uint64(intent), uint64(backend)} {
		binary.BigEndian.PutUint64(encoded[:], value)
		_, _ = digest.Write(encoded[:])
	}
	_, _ = digest.Write(publication[:])
	_, _ = digest.Write(view[:])
	sum := digest.Sum(nil)
	var result catalog.TenantOperationID
	copy(result[:], sum[:len(result)])
	return result
}

func (a PreparationAdapter) preparePresentation(
	ctx context.Context,
	kind catalogproto.PresentationKind,
	provision catalog.TenantProvision,
) (catalogproto.PresentationProof, error) {
	spec := tenantSpec(provision)
	switch kind {
	case catalogproto.PresentationKindMount:
		if !spec.Traits.Presentations.Has(catalog.PresentationMount) || a.Mounts == nil {
			return catalogproto.PresentationProof{}, fmt.Errorf("%w: tenant has no mount presentation", catalog.ErrInvalidObject)
		}
		if err := a.Mounts.PrepareMountPresentation(ctx, spec.ID, spec.Generation); err != nil {
			return catalogproto.PresentationProof{}, err
		}
		mount := catalogproto.MountPresentationProof{
			TenantID: catalogproto.TenantID(spec.ID), Generation: uint64(spec.Generation), PublicPath: spec.Mount.PresentationRoot,
			ActivationGeneration: a.ActivationGeneration,
		}
		return catalogproto.PresentationProof{Kind: kind, Mount: &mount}, nil
	case catalogproto.PresentationKindFileProvider:
		if !spec.Traits.Presentations.Has(catalog.PresentationFileProvider) || a.Presentations == nil {
			return catalogproto.PresentationProof{}, fmt.Errorf("%w: tenant has no File Provider presentation", catalog.ErrInvalidObject)
		}
		presentation, err := a.Presentations.PrepareFileProviderPresentation(ctx, spec.ID, spec.Generation)
		if err != nil {
			return catalogproto.PresentationProof{}, err
		}
		expectedDomain, err := causal.DeriveDomainID(provision.OwnerID, provision.FileProvider.PresentationInstanceID)
		if err != nil {
			return catalogproto.PresentationProof{}, fmt.Errorf("%w: derive File Provider presentation identity: %v", catalog.ErrIntegrity, err)
		}
		if !presentation.Registered || presentation.DomainID != expectedDomain ||
			presentation.OwnerID != provision.OwnerID || presentation.Tenant != provision.Tenant ||
			presentation.Generation != provision.Generation || presentation.Root != provision.Root ||
			presentation.Access != provision.Access ||
			presentation.PresentationInstance != provision.FileProvider.PresentationInstanceID ||
			presentation.DisplayName != provision.FileProvider.DisplayName || !exactPresentationPath(presentation.PublicPath) {
			return catalogproto.PresentationProof{}, fmt.Errorf("%w: File Provider presentation proof is not exact", catalog.ErrIntegrity)
		}
		if presentation.ActivationGeneration != a.ActivationGeneration {
			return catalogproto.PresentationProof{}, fmt.Errorf("%w: File Provider presentation belongs to another runtime activation", catalog.ErrGenerationMismatch)
		}
		fileProvider := catalogproto.FileProviderPresentationProof{
			TenantID: catalogproto.TenantID(presentation.Tenant), DomainID: catalogproto.DomainID(presentation.DomainID),
			Generation: uint64(presentation.Generation), PublicPath: presentation.PublicPath,
			ActivationGeneration: presentation.ActivationGeneration,
		}
		return catalogproto.PresentationProof{Kind: kind, FileProvider: &fileProvider}, nil
	default:
		return catalogproto.PresentationProof{}, fmt.Errorf("%w: unknown requested presentation", catalog.ErrInvalidObject)
	}
}

func exactPresentationPath(value string) bool {
	return filepath.IsAbs(value) && filepath.Clean(value) == value && !strings.ContainsRune(value, 0)
}

// ConvergenceAdapter maps exact activation acknowledgement tuples into the engine.
type ConvergenceAdapter struct {
	Engine *convergence.Engine
}

// AckActivation acknowledges exactly the tuple proven by File Provider enumeration.
func (a ConvergenceAdapter) AckActivation(
	ctx context.Context,
	_ Identity,
	tenantID catalog.TenantID,
	request catalogproto.AckActivationRequest,
) error {
	if a.Engine == nil {
		return errors.New("catalog service: convergence adapter is incomplete")
	}
	activationID, err := activationChangeID(request.ActivationChangeID)
	if err != nil {
		return err
	}
	headDigest, err := activationHeadDigest(request.HeadDigest)
	if err != nil {
		return err
	}
	ack := causal.ActivationAck{
		ActivationChangeID:         activationID,
		TenantID:                   causal.TenantID(tenantID),
		TenantGeneration:           causal.Generation(request.Generation),
		PresentationID:             causal.PresentationID(request.DomainID),
		Backend:                    causal.BackendFileProvider,
		ObservedActivationRevision: causal.Revision(request.ActivationRevision),
		ObservedCatalogHead:        causal.CatalogRevision(request.CatalogHead),
		ObservedHeadDigest:         headDigest,
	}
	if err := a.Engine.Acknowledge(ctx, ack); err != nil {
		return err
	}
	return nil
}

func activationChangeID(id catalogproto.ActivationChangeID) (causal.ActivationChangeID, error) {
	var result causal.ActivationChangeID
	decoded, err := hex.DecodeString(string(id))
	if err != nil || len(decoded) != len(result) {
		return result, fmt.Errorf("%w: invalid activation change id", catalog.ErrInvalidObject)
	}
	copy(result[:], decoded)
	return result, nil
}

func activationHeadDigest(value string) ([32]byte, error) {
	var result [32]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(result) {
		return result, fmt.Errorf("%w: invalid activation head digest", catalog.ErrInvalidObject)
	}
	copy(result[:], decoded)
	return result, nil
}
