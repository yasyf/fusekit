package catalogservice

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

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

// PreparationAdapter joins the tenant catalog lane, presentation activation,
// and external domain lane without collapsing revisions.
type PreparationAdapter struct {
	Runtime              *tenant.TenantRuntime
	Engine               *convergence.Engine
	Barrier              sourceauthority.Barrier
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
	if a.Runtime == nil || a.Barrier == nil {
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
	if target.Tenant != tenantID || target.CatalogRevision == 0 || target.Change.SourceAuthority == "" || target.Change.SourceRevision == 0 ||
		target.Change.ChangeID == (causal.ChangeID{}) || target.Change.OperationID == (causal.OperationID{}) {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: source authority returned an invalid preparation target", catalog.ErrIntegrity)
	}
	lease, err := a.Runtime.AcquireGeneration(ctx, tenantID, catalog.Generation(request.Generation))
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	spec, err := lease.Spec()
	if err != nil {
		lease.Release()
		return catalogproto.TenantPreparationProof{}, err
	}
	state, err := lease.Prepare(ctx, target.CatalogRevision)
	lease.Release()
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	presentation, err := a.preparePresentation(ctx, request.Presentation, spec)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	return catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: catalogproto.TenantID(tenantID), Generation: uint64(state.Generation),
			Requested: uint64(target.CatalogRevision), Desired: uint64(state.Desired), Observed: uint64(state.Observed),
			Verified: uint64(state.Verified), Applied: uint64(state.Applied),
		},
		Presentation:    presentation,
		SourceAuthority: catalogproto.SourceAuthorityID(target.Change.SourceAuthority),
		SourceRevision:  uint64(target.Change.SourceRevision),
		CatalogRevision: uint64(target.CatalogRevision),
		ChangeID:        catalogproto.ChangeID(hex.EncodeToString(target.Change.ChangeID[:])),
		OperationID:     catalogproto.OperationID(hex.EncodeToString(target.Change.OperationID[:])),
	}, nil
}

func (a PreparationAdapter) preparePresentation(
	ctx context.Context,
	kind catalogproto.PresentationKind,
	spec tenant.TenantSpec,
) (catalogproto.PresentationProof, error) {
	switch kind {
	case catalogproto.PresentationKindMount:
		if !spec.Traits.Presentations.Has(catalog.PresentationMount) {
			return catalogproto.PresentationProof{}, fmt.Errorf("%w: tenant has no mount presentation", catalog.ErrInvalidObject)
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
		if !presentation.Registered || presentation.Tenant != spec.ID || presentation.Generation != spec.Generation {
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

// PrepareDomain revalidates one echoed tenant proof and prepares only its
// exact File Provider domain.
func (a PreparationAdapter) PrepareDomain(
	ctx context.Context,
	_ Identity,
	tenantID catalog.TenantID,
	request catalogproto.PrepareDomainRequest,
) (catalogproto.DomainObservation, error) {
	if a.Runtime == nil || a.Engine == nil || a.Barrier == nil {
		return catalogproto.DomainObservation{}, errors.New("catalog service: domain preparation adapter is incomplete")
	}
	barrier, err := a.Barrier.Barrier(ctx, tenantID, catalog.Generation(request.Generation))
	if err != nil {
		return catalogproto.DomainObservation{}, err
	}
	target := barrier.Target
	changeID, err := convergenceChangeID(request.ChangeID)
	if err != nil {
		return catalogproto.DomainObservation{}, err
	}
	operationID, err := convergenceOperationID(request.OperationID)
	if err != nil {
		return catalogproto.DomainObservation{}, err
	}
	if target.Tenant != tenantID || uint64(target.CatalogRevision) != request.CatalogRevision ||
		catalogproto.SourceAuthorityID(target.Change.SourceAuthority) != request.SourceAuthority ||
		uint64(target.Change.SourceRevision) != request.SourceRevision ||
		convergence.ChangeID(target.Change.ChangeID) != changeID ||
		convergence.OperationID(target.Change.OperationID) != operationID {
		return catalogproto.DomainObservation{}, fmt.Errorf("%w: domain preparation proof is stale", catalog.ErrMutationConflict)
	}
	lease, err := a.Runtime.AcquireGeneration(ctx, tenantID, catalog.Generation(request.Generation))
	if err != nil {
		return catalogproto.DomainObservation{}, err
	}
	state, err := lease.Prepare(ctx, target.CatalogRevision)
	lease.Release()
	if err != nil {
		return catalogproto.DomainObservation{}, err
	}
	if uint64(state.Generation) != request.Generation || state.Applied < target.CatalogRevision {
		return catalogproto.DomainObservation{}, fmt.Errorf("%w: tenant preparation proof is not applied", catalog.ErrIntegrity)
	}
	requirement := convergence.PreparationRequirement{
		Tenant: convergence.TenantID(tenantID), Domain: convergence.DomainID(request.DomainID),
		Generation:      convergence.Generation(request.Generation),
		SourceAuthority: convergence.SourceAuthorityID(request.SourceAuthority),
		SourceRevision:  convergence.Revision(request.SourceRevision),
		CatalogRevision: convergence.CatalogRevision(request.CatalogRevision),
		ChangeID:        changeID,
		OperationID:     operationID,
	}
	proof, err := a.Engine.PrepareTenant(ctx, requirement)
	if err != nil {
		if errors.Is(err, convergence.ErrQuarantined) {
			return catalogproto.DomainObservation{}, &CodedError{Code: catalogproto.ErrorCodeQuarantined, Cause: err}
		}
		return catalogproto.DomainObservation{}, err
	}
	if proof.Requested.Tenant != requirement.Tenant || proof.Requested.Domain != requirement.Domain ||
		proof.Requested.Generation != requirement.Generation || proof.Requested.Revision == 0 ||
		proof.Requested.CatalogRevision != requirement.CatalogRevision ||
		proof.Requested.SourceAuthority != requirement.SourceAuthority ||
		proof.Requested.SourceRevision != requirement.SourceRevision ||
		proof.Requested.ChangeID != requirement.ChangeID || proof.Requested.OperationID != requirement.OperationID {
		return catalogproto.DomainObservation{}, fmt.Errorf("%w: convergence engine returned an invalid derived preparation", catalog.ErrIntegrity)
	}
	return protocolDomainObservation(proof), nil
}

// ConvergenceAdapter generation-fences and maps exact acknowledgement tuples into the engine.
type ConvergenceAdapter struct {
	Runtime *tenant.TenantRuntime
	Engine  *convergence.Engine
}

// AckConvergence acknowledges exactly the tuple proven by File Provider enumeration.
func (a ConvergenceAdapter) AckConvergence(
	ctx context.Context,
	_ Identity,
	tenantID catalog.TenantID,
	request catalogproto.AckConvergenceRequest,
) (catalogproto.DomainObservation, error) {
	if a.Runtime == nil || a.Engine == nil {
		return catalogproto.DomainObservation{}, errors.New("catalog service: convergence adapter is incomplete")
	}
	lease, err := a.Runtime.AcquireGeneration(ctx, tenantID, catalog.Generation(request.Generation))
	if err != nil {
		return catalogproto.DomainObservation{}, err
	}
	lease.Release()
	changeID, err := convergenceChangeID(request.ChangeID)
	if err != nil {
		return catalogproto.DomainObservation{}, err
	}
	operationID, err := convergenceOperationID(request.OperationID)
	if err != nil {
		return catalogproto.DomainObservation{}, err
	}
	ack := convergence.Ack{
		Domain: convergence.DomainID(request.DomainID), Generation: convergence.Generation(request.Generation), Revision: convergence.Revision(request.Revision),
		SourceAuthority: convergence.SourceAuthorityID(request.SourceAuthority), SourceRevision: convergence.Revision(request.SourceRevision),
		CatalogRevision: convergence.CatalogRevision(request.CatalogRevision),
		ChangeID:        changeID, OperationID: operationID,
	}
	if err := a.Engine.Acknowledge(ctx, ack); err != nil {
		if errors.Is(err, convergence.ErrQuarantined) {
			return catalogproto.DomainObservation{}, &CodedError{Code: catalogproto.ErrorCodeQuarantined, Cause: err}
		}
		return catalogproto.DomainObservation{}, err
	}
	lease, err = a.Runtime.AcquireGeneration(ctx, tenantID, catalog.Generation(request.Generation))
	if err != nil {
		return catalogproto.DomainObservation{}, err
	}
	lease.Release()
	state, err := a.Engine.Snapshot(ctx)
	if err != nil {
		return catalogproto.DomainObservation{}, err
	}
	domain, ok := state.Domains[ack.Domain]
	if !ok || domain.Tenant != convergence.TenantID(tenantID) || domain.Generation != ack.Generation || domain.Observed < ack.Revision {
		return catalogproto.DomainObservation{}, fmt.Errorf("%w: acknowledged domain observation is missing", catalog.ErrIntegrity)
	}
	return catalogproto.DomainObservation{
		TenantID: catalogproto.TenantID(tenantID), DomainID: request.DomainID, Generation: request.Generation,
		RequestedRevision: request.Revision, ObservedRevision: uint64(domain.Observed),
		CatalogRevision: uint64(domain.ObservedCatalogRevision), SourceAuthority: catalogproto.SourceAuthorityID(domain.ObservedChange.SourceAuthority),
		SourceRevision: uint64(domain.ObservedChange.SourceRevision),
		ChangeID:       catalogproto.ChangeID(hex.EncodeToString(domain.ObservedChange.ChangeID[:])),
		OperationID:    catalogproto.OperationID(hex.EncodeToString(domain.ObservedChange.OperationID[:])),
	}, nil
}

func protocolDomainObservation(proof convergence.ObservationProof) catalogproto.DomainObservation {
	return catalogproto.DomainObservation{
		TenantID: catalogproto.TenantID(proof.Requested.Tenant), DomainID: catalogproto.DomainID(proof.Requested.Domain),
		Generation: uint64(proof.Requested.Generation), RequestedRevision: uint64(proof.Requested.Revision), ObservedRevision: uint64(proof.Observed.Revision),
		CatalogRevision: uint64(proof.Observed.CatalogRevision), SourceAuthority: catalogproto.SourceAuthorityID(proof.Observed.SourceAuthority),
		SourceRevision: uint64(proof.Observed.SourceRevision),
		ChangeID:       catalogproto.ChangeID(hex.EncodeToString(proof.Observed.ChangeID[:])),
		OperationID:    catalogproto.OperationID(hex.EncodeToString(proof.Observed.OperationID[:])),
	}
}

func convergenceChangeID(id catalogproto.ChangeID) (convergence.ChangeID, error) {
	var result convergence.ChangeID
	decoded, err := hex.DecodeString(string(id))
	if err != nil || len(decoded) != len(result) {
		return result, fmt.Errorf("%w: invalid convergence change id", catalog.ErrInvalidObject)
	}
	copy(result[:], decoded)
	return result, nil
}

func convergenceOperationID(id catalogproto.OperationID) (convergence.OperationID, error) {
	var result convergence.OperationID
	decoded, err := hex.DecodeString(string(id))
	if err != nil || len(decoded) != len(result) {
		return result, fmt.Errorf("%w: invalid convergence operation id", catalog.ErrInvalidObject)
	}
	copy(result[:], decoded)
	return result, nil
}
