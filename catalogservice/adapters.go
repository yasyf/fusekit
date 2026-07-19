package catalogservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/convergence"
	"github.com/yasyf/fusekit/tenant"
)

// MutationAdapter binds streamed daemonkit mutations to the durable catalog and tenant actor.
type MutationAdapter struct {
	Catalog *catalog.Catalog
	Runtime *tenant.TenantRuntime
	Engine  *convergence.Engine
}

// StageMutation durably stages request bytes without holding tenant lifecycle admission.
func (a MutationAdapter) StageMutation(
	ctx context.Context,
	_ Identity,
	authorization Authorization,
	tenantID catalog.TenantID,
	operationID catalog.MutationID,
	generation catalog.Generation,
	hasContent bool,
	source io.Reader,
) (stage MutationStage, err error) {
	if a.Catalog == nil || a.Runtime == nil || a.Engine == nil {
		return MutationStage{}, errors.New("catalog service: mutation adapter is incomplete")
	}
	if err := validateAuthorization(authorization, catalogproto.OperationCatalogMutate); err != nil {
		return MutationStage{}, err
	}
	if authorization.Route.Tenant != tenantID || authorization.Route.Generation != generation {
		return MutationStage{}, fmt.Errorf("%w: mutation route changed", catalog.ErrIntegrity)
	}
	stage = MutationStage{
		Token: operationID.String(), OperationID: operationID, Tenant: tenantID,
		Generation: generation, authorization: authorization,
	}
	if existing, existingErr := a.Catalog.PreparedMutation(ctx, operationID); existingErr == nil {
		if existing.Tenant != tenantID {
			return MutationStage{}, catalog.ErrMutationConflict
		}
		ref, carriesContent := mutationIntentContent(existing.Intent)
		if carriesContent != hasContent {
			return MutationStage{}, catalog.ErrMutationConflict
		}
		if hasContent {
			size, hash, err := digestReader(ctx, source)
			if err != nil {
				return MutationStage{}, err
			}
			if size != ref.Size || hash != ref.Hash {
				return MutationStage{}, catalog.ErrMutationConflict
			}
			stage.Size = size
			stage.content = &ref
			return stage, nil
		}
		count, err := io.Copy(io.Discard, &contextReader{ctx: ctx, source: source})
		if err != nil {
			return MutationStage{}, err
		}
		if count != 0 {
			return MutationStage{}, fmt.Errorf("%w: contentless mutation carried bytes", catalog.ErrIntegrity)
		}
		return stage, nil
	} else if !errors.Is(existingErr, catalog.ErrNotFound) {
		return MutationStage{}, existingErr
	}
	if hasContent {
		ref, err := a.Catalog.StageContent(ctx, source)
		if err != nil {
			return MutationStage{}, err
		}
		stage.Size = ref.Size
		stage.content = &ref
		return stage, nil
	}
	count, err := io.Copy(io.Discard, &contextReader{ctx: ctx, source: source})
	if err != nil {
		return MutationStage{}, err
	}
	if count != 0 {
		return MutationStage{}, fmt.Errorf("%w: contentless mutation carried bytes", catalog.ErrIntegrity)
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
	operationID, err := catalogMutationID(request.OperationID)
	if err != nil {
		return MutationResult{}, err
	}
	if operationID != stage.OperationID || catalog.Generation(request.Generation) != stage.Generation {
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
	prepared, err := a.Catalog.BeginMutation(ctx, operationID, stage.Tenant, catalog.Revision(request.ExpectedRevision), intent)
	if err != nil {
		return MutationResult{}, err
	}
	if prepared.Tenant != stage.Tenant || prepared.OperationID != operationID || prepared.ExpectedHead != catalog.Revision(request.ExpectedRevision) {
		return MutationResult{}, fmt.Errorf("%w: prepared mutation identity changed", catalog.ErrIntegrity)
	}
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
	record, err := a.Catalog.Mutation(ctx, operationID)
	if err != nil {
		return MutationResult{}, err
	}
	primary := catalog.ObjectID(record.Primary)
	result := MutationResult{OperationID: operationID, Revision: record.Revision, PrimaryID: &primary}
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
		if *request.ObjectKind == catalogproto.ObjectKindFile {
			kind = catalog.KindFile
			if content == nil || request.ContentRevision == nil {
				return catalog.MutationIntent{}, fmt.Errorf("%w: file create has no durable content", catalog.ErrIntegrity)
			}
			ref = *content
			contentRevision = catalog.Revision(*request.ContentRevision)
		}
		intent.Create = &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: parent, Name: *request.Name, Kind: kind, Mode: *request.Mode,
			ContentRevision: contentRevision, Content: ref, Visibility: visibilityForAuthorization(authorization),
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
		current, err := a.Catalog.Inspect(ctx, tenantID, id)
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

func digestReader(ctx context.Context, source io.Reader) (int64, catalog.ContentHash, error) {
	digest := sha256.New()
	size, err := io.Copy(digest, &contextReader{ctx: ctx, source: source})
	if err != nil {
		return 0, catalog.ContentHash{}, err
	}
	var hash catalog.ContentHash
	copy(hash[:], digest.Sum(nil))
	return size, hash, nil
}

type contextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(buffer)
}

// PreparationAdapter joins the tenant catalog lane and external domain lane without collapsing revisions.
type PreparationAdapter struct {
	Runtime *tenant.TenantRuntime
	Engine  *convergence.Engine
}

// PrepareTenant returns split catalog and domain observation proof for one exact causal request.
func (a PreparationAdapter) PrepareTenant(
	ctx context.Context,
	_ Identity,
	tenantID catalog.TenantID,
	request catalogproto.PrepareTenantRequest,
) (catalogproto.PreparationProof, error) {
	if a.Runtime == nil || a.Engine == nil {
		return catalogproto.PreparationProof{}, errors.New("catalog service: preparation adapter is incomplete")
	}
	lease, err := a.Runtime.AcquireGeneration(ctx, tenantID, catalog.Generation(request.Generation))
	if err != nil {
		return catalogproto.PreparationProof{}, err
	}
	state, err := lease.Prepare(ctx, catalog.Revision(request.CatalogRevision))
	lease.Release()
	if err != nil {
		return catalogproto.PreparationProof{}, err
	}
	proof, err := a.Engine.PrepareTenant(ctx, convergence.TenantID(tenantID), convergence.Revision(request.SourceRevision), convergence.CatalogRevision(request.CatalogRevision))
	if err != nil {
		if errors.Is(err, convergence.ErrQuarantined) {
			return catalogproto.PreparationProof{}, &CodedError{Code: catalogproto.ErrorCodeQuarantined, Cause: err}
		}
		return catalogproto.PreparationProof{}, err
	}
	changeID, err := convergenceChangeID(request.ChangeID)
	if err != nil {
		return catalogproto.PreparationProof{}, err
	}
	operationID, err := convergenceOperationID(request.OperationID)
	if err != nil {
		return catalogproto.PreparationProof{}, err
	}
	want := convergence.Preparation{
		Tenant: convergence.TenantID(tenantID), Domain: convergence.DomainID(request.DomainID),
		Generation: convergence.Generation(request.Generation),
		Revision:   convergence.Revision(request.Revision), SourceRevision: convergence.Revision(request.SourceRevision),
		CatalogRevision: convergence.CatalogRevision(request.CatalogRevision), ChangeID: changeID, OperationID: operationID,
	}
	if proof.Requested != want {
		return catalogproto.PreparationProof{}, fmt.Errorf("%w: convergence engine prepared a different causal request", catalog.ErrIntegrity)
	}
	lease, err = a.Runtime.AcquireGeneration(ctx, tenantID, catalog.Generation(request.Generation))
	if err != nil {
		return catalogproto.PreparationProof{}, err
	}
	defer lease.Release()
	state, err = lease.Prepare(ctx, catalog.Revision(request.CatalogRevision))
	if err != nil {
		return catalogproto.PreparationProof{}, err
	}
	return catalogproto.PreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: catalogproto.TenantID(tenantID), Generation: uint64(state.Generation),
			Requested: request.CatalogRevision, Desired: uint64(state.Desired), Observed: uint64(state.Observed),
			Verified: uint64(state.Verified), Applied: uint64(state.Applied),
		},
		Domain: protocolDomainObservation(proof),
	}, nil
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
		SourceRevision: convergence.Revision(request.SourceRevision), CatalogRevision: convergence.CatalogRevision(request.CatalogRevision),
		ChangeID: changeID, OperationID: operationID,
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
		CatalogRevision: uint64(domain.ObservedCatalogRevision), SourceRevision: uint64(domain.ObservedChange.SourceRevision),
		ChangeID:    catalogproto.ChangeID(hex.EncodeToString(domain.ObservedChange.ChangeID[:])),
		OperationID: catalogproto.MutationID(hex.EncodeToString(domain.ObservedChange.OperationID[:])),
	}, nil
}

func protocolDomainObservation(proof convergence.ObservationProof) catalogproto.DomainObservation {
	return catalogproto.DomainObservation{
		TenantID: catalogproto.TenantID(proof.Requested.Tenant), DomainID: catalogproto.DomainID(proof.Requested.Domain),
		Generation: uint64(proof.Requested.Generation), RequestedRevision: uint64(proof.Requested.Revision), ObservedRevision: uint64(proof.Observed.Revision),
		CatalogRevision: uint64(proof.Observed.CatalogRevision), SourceRevision: uint64(proof.Observed.SourceRevision),
		ChangeID:    catalogproto.ChangeID(hex.EncodeToString(proof.Observed.ChangeID[:])),
		OperationID: catalogproto.MutationID(hex.EncodeToString(proof.Observed.OperationID[:])),
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

func convergenceOperationID(id catalogproto.MutationID) (convergence.OperationID, error) {
	parsed, err := catalogMutationID(id)
	if err != nil {
		return convergence.OperationID{}, err
	}
	return convergence.OperationID(parsed), nil
}
