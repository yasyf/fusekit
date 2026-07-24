package catalogservice

import (
	"context"
	"crypto/sha256"
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
	if err := a.Engine.Pump(ctx); err != nil {
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

// MountPresentationPreparer establishes the exact native route before its proof is returned.
type MountPresentationPreparer interface {
	PrepareMountPresentation(context.Context, catalog.TenantID, catalog.Generation) error
}

// PreparationAdapter joins the tenant catalog lane, presentation activation,
// and external domain lane without collapsing revisions.
type PreparationAdapter struct {
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
	source := barrier.Source
	if target.Tenant != tenantID || target.Generation != catalog.Generation(request.Generation) ||
		target.CatalogRevision == 0 || source.Authority == "" || source.SourceRevision == 0 ||
		target.SourceRevision != source.SourceRevision || source.ChangeID == (causal.ChangeID{}) ||
		source.SourceOperation == (causal.OperationID{}) || source.PublicationID == (causal.OperationID{}) ||
		source.PublicationDigest == ([sha256.Size]byte{}) {
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
		SourceAuthority: catalogproto.SourceAuthorityID(source.Authority),
		SourceRevision:  uint64(source.SourceRevision),
		CatalogRevision: uint64(target.CatalogRevision),
		ChangeID:        catalogproto.ChangeID(hex.EncodeToString(source.ChangeID[:])),
		OperationID:     catalogproto.OperationID(hex.EncodeToString(source.SourceOperation[:])),
	}, nil
}

func (a PreparationAdapter) preparePresentation(
	ctx context.Context,
	kind catalogproto.PresentationKind,
	spec tenant.TenantSpec,
) (catalogproto.PresentationProof, error) {
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

// ActivationAdapter generation-fences and maps exact acknowledgements into the engine.
type ActivationAdapter struct {
	Runtime *tenant.TenantRuntime
	Engine  *convergence.Engine
}

// AckActivation acknowledges exactly the activation observed by File Provider enumeration.
func (a ActivationAdapter) AckActivation(
	ctx context.Context,
	_ Identity,
	tenantID catalog.TenantID,
	request catalogproto.AckActivationRequest,
) error {
	if a.Runtime == nil || a.Engine == nil {
		return errors.New("catalog service: activation adapter is incomplete")
	}
	lease, err := a.Runtime.AcquireGeneration(ctx, tenantID, catalog.Generation(request.Generation))
	if err != nil {
		return err
	}
	lease.Release()
	changeID, err := activationChangeID(request.ActivationChangeID)
	if err != nil {
		return err
	}
	headDigest, err := activationHeadDigest(request.HeadDigest)
	if err != nil {
		return err
	}
	ack := causal.ActivationAck{
		ActivationChangeID:         changeID,
		TenantID:                   causal.TenantID(tenantID),
		TenantGeneration:           causal.Generation(request.Generation),
		PresentationID:             causal.PresentationID(request.DomainID),
		Backend:                    causal.BackendFileProvider,
		ObservedActivationRevision: causal.Revision(request.ActivationRevision),
		ObservedCatalogHead:        causal.CatalogRevision(request.CatalogHead),
		ObservedHeadDigest:         headDigest,
	}
	return a.Engine.Acknowledge(ctx, ack)
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
