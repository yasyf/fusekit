package catalogservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

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

// LookupPrivate resolves one unpublished object only for its authenticated origin route.
func (a MutationAdapter) LookupPrivate(
	ctx context.Context,
	_ Identity,
	authorization Authorization,
	tenantID catalog.TenantID,
	id catalog.ObjectID,
) (catalog.PrivateMutationResult, error) {
	if a.Store == nil {
		return catalog.PrivateMutationResult{}, errors.New("catalog service: mutation adapter is incomplete")
	}
	origin, err := privateMutationOrigin(authorization, tenantID)
	if err != nil {
		return catalog.PrivateMutationResult{}, err
	}
	result, err := a.Store.PrivateMutationObject(ctx, tenantID, id, origin)
	if err != nil {
		return catalog.PrivateMutationResult{}, err
	}
	if result.Tenant != tenantID || result.Generation != authorization.Route.Generation ||
		result.ObjectID != id || result.Mutation == (catalog.MutationID{}) {
		return catalog.PrivateMutationResult{}, fmt.Errorf("%w: private lookup identity changed", catalog.ErrIntegrity)
	}
	return result, nil
}

// OpenPrivate opens one unpublished object for its exact creator capability and origin route.
func (a MutationAdapter) OpenPrivate(
	ctx context.Context,
	_ Identity,
	authorization Authorization,
	tenantID catalog.TenantID,
	generation catalog.Generation,
	id catalog.ObjectID,
	creator catalog.MutationID,
) (PrivateOpenResult, error) {
	if a.Store == nil {
		return PrivateOpenResult{}, errors.New("catalog service: mutation adapter is incomplete")
	}
	origin, err := privateMutationOrigin(authorization, tenantID)
	if err != nil {
		return PrivateOpenResult{}, err
	}
	if generation != authorization.Route.Generation {
		return PrivateOpenResult{}, fmt.Errorf("%w: private open generation changed", catalog.ErrGenerationMismatch)
	}
	result, source, err := a.Store.OpenPrivateContent(ctx, tenantID, generation, id, creator, origin)
	if err != nil {
		return PrivateOpenResult{}, err
	}
	if source == nil || result.Tenant != tenantID || result.Generation != generation ||
		result.ObjectID != id || result.Mutation != creator {
		if source != nil {
			cause := fmt.Errorf("%w: private open identity changed", catalog.ErrIntegrity)
			_ = source.Settle(cause)
			waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationStageCleanupTimeout)
			_ = source.Wait(waitCtx)
			cancel()
		}
		return PrivateOpenResult{}, fmt.Errorf("%w: private open identity changed", catalog.ErrIntegrity)
	}
	return PrivateOpenResult{Result: result, Content: source}, nil
}

func privateMutationOrigin(authorization Authorization, tenantID catalog.TenantID) (catalog.CausalOrigin, error) {
	if authorization.Role != RoleFileProvider || authorization.Presentation != catalog.PresentationFileProvider ||
		authorization.Route.Tenant != tenantID || authorization.Route.Domain == "" ||
		authorization.Route.Generation == 0 || !authorization.Route.Forwarded {
		return catalog.CausalOrigin{}, fmt.Errorf("%w: private mutation lacks an authenticated File Provider origin", catalog.ErrIntegrity)
	}
	return catalog.CausalOrigin{
		Cause: causal.CauseProviderMutation, Domain: causal.DomainID(authorization.Route.Domain),
		Generation: causal.Generation(authorization.Route.Generation),
	}, nil
}

func validatePrivateMutationAuthorization(
	authorization Authorization,
	tenantID catalog.TenantID,
	request catalogproto.MutationRequest,
) error {
	private := request.Disposition == catalogproto.MutationDispositionPrivateStaging ||
		request.Kind == catalogproto.MutationKindPromote || request.PrivateCreator != nil
	if !private {
		return nil
	}
	if _, err := privateMutationOrigin(authorization, tenantID); err != nil {
		return err
	}
	if authorization.Route.Generation != catalog.Generation(request.Generation) {
		return fmt.Errorf("%w: private mutation generation changed", catalog.ErrGenerationMismatch)
	}
	return nil
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
	if err := validatePrivateMutationAuthorization(authorization, stage.Tenant, request); err != nil {
		return MutationResult{}, err
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
	private := prepared.Intent.Disposition == catalog.MutationDispositionPrivate
	if private {
		prepared, err = lease.SettleMutation(ctx, prepared.OperationID, prepared.ExpectedHead)
		if err != nil {
			return MutationResult{}, err
		}
	} else {
		state, err := lease.Prepare(ctx, prepared.ExpectedHead+1)
		if err != nil {
			return MutationResult{}, err
		}
		if !state.Prepared() {
			return MutationResult{}, fmt.Errorf("%w: tenant actor returned an unprepared state", catalog.ErrIntegrity)
		}
	}
	if err := a.Engine.Pump(ctx); err != nil {
		return MutationResult{}, err
	}
	if private {
		return a.privateMutationResult(ctx, stage, request, prepared)
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

func (a MutationAdapter) privateMutationResult(
	ctx context.Context,
	stage MutationStage,
	request catalogproto.MutationRequest,
	prepared catalog.PreparedMutation,
) (MutationResult, error) {
	authority, err := preparedMutationAuthority(prepared)
	if err != nil {
		return MutationResult{}, err
	}
	receipt, err := a.Store.CommittedSourceDriverMutation(ctx, authority, prepared.OperationID)
	if err != nil {
		return MutationResult{}, err
	}
	if receipt == nil || receipt.Result.Identity.Mode != catalog.SourceDriverMutation ||
		receipt.Result.Identity.Mutation != prepared.OperationID ||
		receipt.Result.Identity.MutationTenant != stage.Tenant ||
		receipt.Result.Identity.MutationGeneration != stage.Generation ||
		receipt.Result.MutationResult == nil ||
		receipt.Result.MutationResult.Kind != catalog.SourceDriverMutationPrivate ||
		receipt.Result.MutationResult.Private == nil {
		return MutationResult{}, fmt.Errorf("%w: private mutation receipt identity changed", catalog.ErrIntegrity)
	}
	private := *receipt.Result.MutationResult.Private
	if private.Tenant != stage.Tenant || private.Generation != stage.Generation ||
		private.ObjectID == (catalog.ObjectID{}) || private.Mutation == (catalog.MutationID{}) {
		return MutationResult{}, fmt.Errorf("%w: private mutation result identity changed", catalog.ErrIntegrity)
	}
	switch request.Kind {
	case catalogproto.MutationKindCreate:
		if private.Mutation != prepared.OperationID {
			return MutationResult{}, fmt.Errorf("%w: private create creator changed", catalog.ErrIntegrity)
		}
	case catalogproto.MutationKindDelete:
		object, err := catalogObjectID(*request.ObjectID)
		if err != nil {
			return MutationResult{}, err
		}
		creator, err := catalog.ParseMutationID(string(*request.PrivateCreator))
		if err != nil {
			return MutationResult{}, err
		}
		if private.ObjectID != object || private.Mutation != creator {
			return MutationResult{}, fmt.Errorf("%w: private discard capability changed", catalog.ErrIntegrity)
		}
	default:
		return MutationResult{}, fmt.Errorf("%w: private mutation has an invalid request kind", catalog.ErrIntegrity)
	}
	return MutationResult{
		RequestID: stage.RequestID, OperationID: prepared.OperationID,
		Revision: prepared.OperationID.TargetRevision(), Private: &private,
	}, nil
}

func preparedMutationAuthority(prepared catalog.PreparedMutation) (causal.SourceAuthorityID, error) {
	var authority causal.SourceAuthorityID
	if prepared.Source != nil {
		for _, locator := range []*catalog.SourceLocator{
			prepared.Source.Object, prepared.Source.Parent, prepared.Source.Target, prepared.SourceResult,
		} {
			if locator == nil {
				continue
			}
			if authority != "" && authority != locator.SourceAuthority {
				return "", fmt.Errorf("%w: prepared mutation crosses source authorities", catalog.ErrIntegrity)
			}
			authority = locator.SourceAuthority
		}
	}
	if authority == "" {
		return "", fmt.Errorf("%w: private mutation has no source authority", catalog.ErrIntegrity)
	}
	return authority, nil
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
	var disposition catalog.MutationDisposition
	switch request.Disposition {
	case catalogproto.MutationDispositionNamespace:
		disposition = catalog.MutationDispositionNamespace
	case catalogproto.MutationDispositionPrivateStaging:
		disposition = catalog.MutationDispositionPrivate
	default:
		return catalog.MutationIntent{}, fmt.Errorf("%w: unknown mutation disposition %q", catalog.ErrInvalidObject, request.Disposition)
	}
	intent := catalog.MutationIntent{
		SourceID: sourceID, SourceMetadata: sourceMetadata, Origin: origin, Disposition: disposition,
	}
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
		visibility := visibilityForAuthorization(authorization)
		if disposition == catalog.MutationDispositionPrivate {
			visibility = catalog.Visibility{}
		}
		intent.Create = &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: parent, Name: *request.Name, Kind: kind, Mode: *request.Mode,
			ContentRevision: contentRevision, Content: ref, LinkTarget: linkTarget, Visibility: visibility,
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
		if disposition == catalog.MutationDispositionPrivate {
			creator, err := catalog.ParseMutationID(string(*request.PrivateCreator))
			if err != nil {
				return catalog.MutationIntent{}, err
			}
			intent.DiscardPrivate = &catalog.DiscardPrivateMutation{Object: id, Creator: creator}
		} else {
			intent.Delete = &catalog.DeleteMutation{Object: id}
		}
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
		var creator *catalog.MutationID
		if request.PrivateCreator != nil {
			value, err := catalog.ParseMutationID(string(*request.PrivateCreator))
			if err != nil {
				return catalog.MutationIntent{}, err
			}
			creator = &value
		}
		intent.Replace = &catalog.ReplaceMutation{
			Source: source, Target: target, Parent: parent, Name: request.Name, Mode: request.Mode, Content: update,
			PrivateCreator: creator,
		}
	case catalogproto.MutationKindPromote:
		id, err := catalogObjectID(*request.ObjectID)
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		parent, err := catalogObjectID(*request.ParentID)
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		creator, err := catalog.ParseMutationID(string(*request.PrivateCreator))
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		update, err := contentUpdate()
		if err != nil {
			return catalog.MutationIntent{}, err
		}
		intent.PromotePrivate = &catalog.PromotePrivateMutation{
			Object: id, Creator: creator, Parent: parent, Name: *request.Name,
			Mode: request.Mode, Content: update, Visibility: visibilityForAuthorization(authorization),
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
	case intent.PromotePrivate != nil && intent.PromotePrivate.Content != nil:
		return intent.PromotePrivate.Content.Ref, true
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

// CriticalObjectResolver resolves caller-owned logical launch policy after convergence.
type CriticalObjectResolver interface {
	ResolveCriticalObjects(context.Context, catalog.CriticalObjectResolutionRequest) (catalog.CriticalObjectResolution, error)
}

// FileProviderLeaseStore owns provisional presentation policy and active demand receipts.
type FileProviderLeaseStore interface {
	PrepareFileProviderLease(context.Context, catalog.FileProviderLease) (catalog.FileProviderLease, error)
	CommitFileProviderLease(context.Context, catalog.FileProviderLease) (catalog.FileProviderLease, error)
	RenewFileProviderLease(context.Context, catalog.FileProviderLease) (catalog.FileProviderLease, error)
	ReleaseFileProviderLease(context.Context, catalog.FileProviderLease) (catalog.FileProviderLease, error)
}

// PreparationAdapter joins the tenant catalog lane, presentation activation,
// and external domain lane without collapsing revisions.
type PreparationAdapter struct {
	Runtime              *tenant.TenantRuntime
	Engine               *convergence.Engine
	Barrier              sourceauthority.Barrier
	Mounts               MountPresentationPreparer
	Presentations        FileProviderPresentationPreparer
	CriticalObjects      CriticalObjectResolver
	PresentationLeases   FileProviderLeaseStore
	CriticalReadiness    CriticalReadinessPreparer
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
	proof := catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: catalogproto.TenantID(tenantID), Generation: uint64(state.Generation),
			Requested: uint64(target.CatalogRevision), Desired: uint64(state.Desired), Observed: uint64(state.Observed),
			Verified: uint64(state.Verified), Applied: uint64(state.Applied),
		},
		Presentation:      presentation,
		SourceAuthority:   catalogproto.SourceAuthorityID(source.Authority),
		SourcePublication: catalogproto.OperationID(hex.EncodeToString(source.PublicationID[:])),
		SourceRevision:    uint64(source.SourceRevision),
		CatalogRevision:   uint64(target.CatalogRevision),
		ChangeID:          catalogproto.ChangeID(hex.EncodeToString(source.ChangeID[:])),
		OperationID:       catalogproto.OperationID(hex.EncodeToString(source.SourceOperation[:])),
	}
	if request.Presentation == catalogproto.PresentationKindFileProvider {
		critical, err := a.resolveCriticalObjects(ctx, request, spec, source, target.CatalogRevision, presentation)
		if err != nil {
			return catalogproto.TenantPreparationProof{}, err
		}
		if a.CriticalReadiness == nil {
			return catalogproto.TenantPreparationProof{}, errors.New("catalog service: critical readiness preparer is unavailable")
		}
		proof.CriticalReadiness = &critical
		proof, err = a.CriticalReadiness.PrepareCriticalReadiness(ctx, proof)
		if err != nil {
			return catalogproto.TenantPreparationProof{}, err
		}
	}
	return proof, nil
}

func (a PreparationAdapter) resolveCriticalObjects(
	ctx context.Context,
	request catalogproto.PrepareTenantRequest,
	spec tenant.TenantSpec,
	source catalog.SourceDriverCheckpoint,
	head catalog.Revision,
	presentation catalogproto.PresentationProof,
) (catalogproto.CriticalReadinessProof, error) {
	if a.CriticalObjects == nil || a.PresentationLeases == nil || presentation.FileProvider == nil {
		return catalogproto.CriticalReadinessProof{}, errors.New("catalog service: critical object resolver is unavailable")
	}
	requirements := make([]catalog.CriticalObjectRequirement, len(request.CriticalObjects))
	for index, object := range request.CriticalObjects {
		requirements[index] = catalog.CriticalObjectRequirement{LogicalID: object.LogicalID, Role: object.Role}
	}
	policyDigest, err := catalog.CriticalObjectPolicyDigest(requirements)
	if err != nil {
		return catalogproto.CriticalReadinessProof{}, err
	}
	if hex.EncodeToString(policyDigest[:]) != request.CriticalPolicyDigest {
		return catalogproto.CriticalReadinessProof{}, fmt.Errorf("%w: critical policy digest changed", catalog.ErrIntegrity)
	}
	resolution, err := a.CriticalObjects.ResolveCriticalObjects(ctx, catalog.CriticalObjectResolutionRequest{
		Authority: source.Authority, Publication: source.PublicationID,
		Tenant: spec.ID, Generation: spec.Generation, Head: head, Objects: requirements,
	})
	if err != nil {
		return catalogproto.CriticalReadinessProof{}, err
	}
	resolutionDigest, err := catalog.CriticalObjectResolutionDigest(resolution)
	if err != nil {
		return catalogproto.CriticalReadinessProof{}, err
	}
	objects := make([]catalogproto.ResolvedCriticalObjectProof, len(resolution.Objects))
	for index, object := range resolution.Objects {
		objects[index] = catalogproto.ResolvedCriticalObjectProof{
			LogicalID: object.LogicalID, Role: object.Role, ObjectID: catalogproto.ObjectID(object.ObjectID.String()),
			ObjectRevision: uint64(object.ObjectRevision), ContentRevision: uint64(object.ContentRevision),
			Size: uint64(object.Size), Hash: hex.EncodeToString(object.Hash[:]),
		}
	}
	fileProvider := presentation.FileProvider
	root, err := catalog.ParseObjectID(string(fileProvider.RootID))
	if err != nil {
		return catalogproto.CriticalReadinessProof{}, err
	}
	lease, err := a.PresentationLeases.PrepareFileProviderLease(ctx, catalog.FileProviderLease{
		ID: request.LeaseID, Tenant: spec.ID, DomainID: causal.DomainID(fileProvider.DomainID),
		Generation: spec.Generation, Root: root,
		PresentationInstance: string(fileProvider.PresentationInstanceID), State: catalog.FileProviderLeaseProvisional,
		PolicyDigest: policyDigest, ResolutionDigest: resolutionDigest, CatalogHead: head,
		SourceAuthority: source.Authority, SourcePublication: source.PublicationID, SourceRevision: source.SourceRevision,
		ActivationGeneration: fileProvider.ActivationGeneration,
		ExpiresAt:            time.Unix(0, int64(request.LeaseExpiresUnixNano)).UTC(), CriticalObjects: resolution.Objects,
	})
	if err != nil {
		return catalogproto.CriticalReadinessProof{}, err
	}
	leaseProof, err := protocolFileProviderLease(lease)
	if err != nil {
		return catalogproto.CriticalReadinessProof{}, err
	}
	return catalogproto.CriticalReadinessProof{
		PolicyDigest: request.CriticalPolicyDigest, ResolutionDigest: hex.EncodeToString(resolutionDigest[:]),
		CatalogHead: uint64(head), SourceRevision: uint64(source.SourceRevision), TenantGeneration: uint64(spec.Generation),
		DomainID: fileProvider.DomainID, PresentationInstanceID: fileProvider.PresentationInstanceID,
		RootID: fileProvider.RootID, ActivationGeneration: fileProvider.ActivationGeneration, Lease: leaseProof, Objects: objects,
	}, nil
}

func protocolFileProviderLease(lease catalog.FileProviderLease) (catalogproto.FileProviderLeaseReceipt, error) {
	var state catalogproto.FileProviderLeaseState
	switch lease.State {
	case catalog.FileProviderLeaseProvisional:
		state = catalogproto.FileProviderLeaseStateProvisional
	case catalog.FileProviderLeaseCommitted:
		state = catalogproto.FileProviderLeaseStateCommitted
	case catalog.FileProviderLeaseReleased:
		state = catalogproto.FileProviderLeaseStateReleased
	default:
		return catalogproto.FileProviderLeaseReceipt{}, fmt.Errorf("%w: unsupported File Provider lease state", catalog.ErrInvalidTransition)
	}
	if lease.ExpiresAt.UnixNano() <= 0 {
		return catalogproto.FileProviderLeaseReceipt{}, fmt.Errorf("%w: File Provider lease expiry is invalid", catalog.ErrIntegrity)
	}
	return catalogproto.FileProviderLeaseReceipt{
		LeaseID: lease.ID, TenantID: catalogproto.TenantID(lease.Tenant), DomainID: catalogproto.DomainID(lease.DomainID),
		Generation: uint64(lease.Generation), RootID: catalogproto.ObjectID(lease.Root.String()),
		PresentationInstanceID: catalogproto.PresentationInstanceID(lease.PresentationInstance), State: state,
		SessionID: lease.SessionID, ProcessIdentity: lease.ProcessIdentity,
		PolicyDigest: hex.EncodeToString(lease.PolicyDigest[:]), ResolutionDigest: hex.EncodeToString(lease.ResolutionDigest[:]),
		CatalogHead: uint64(lease.CatalogHead), SourceAuthority: catalogproto.SourceAuthorityID(lease.SourceAuthority),
		SourcePublication: catalogproto.OperationID(hex.EncodeToString(lease.SourcePublication[:])),
		SourceRevision:    uint64(lease.SourceRevision), ActivationGeneration: lease.ActivationGeneration,
		ExpiresUnixNano: uint64(lease.ExpiresAt.UnixNano()),
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
			ActivationGeneration:   presentation.ActivationGeneration,
			PresentationInstanceID: catalogproto.PresentationInstanceID(presentation.PresentationInstance),
			RootID:                 catalogproto.ObjectID(presentation.Root.String()),
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
