package holder

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

const localDesiredFleetCASLimit = 8

// ErrLocalTenantControllerUnavailable means no ready holder publication is available.
var ErrLocalTenantControllerUnavailable = errors.New("FuseKit runtime: local tenant controller is unavailable")

// LocalRuntimeReadiness identifies one admitted holder generation.
type LocalRuntimeReadiness struct {
	RuntimeBuild         string
	ActivationGeneration string
	ProcessGeneration    proc.OwnerGeneration
}

// LocalTenantAcknowledgement proves one exact durable tenant definition.
type LocalTenantAcknowledgement struct {
	Tenant        catalog.TenantID
	Generation    catalog.Generation
	Presentations catalog.PresentationSet
}

// LocalTenantRetirementProof proves one generation and its File Provider domain are absent.
type LocalTenantRetirementProof struct {
	Tenant             catalog.TenantID
	Generation         catalog.Generation
	FileProviderAbsent bool
}

// LocalPreparationRequest is the product policy for one exact presentation preparation.
// FuseKit derives protocol, activation, and critical-policy digests internally.
type LocalPreparationRequest struct {
	Generation      catalog.Generation
	Presentation    catalog.Presentation
	CriticalObjects []catalog.CriticalObjectRequirement
	LeaseID         string
	LeaseExpiresAt  time.Time
}

// LocalProvisionRequest composes one source declaration, tenant definition,
// and exact preparation request for the controller's immutable owner.
type LocalProvisionRequest struct {
	Declaration catalog.SourceAuthorityDeclaration
	Tenant      tenant.TenantSpec
	Preparation LocalPreparationRequest
}

// LocalSourceFleetPublication replaces the immutable owner's complete desired source fleet.
type LocalSourceFleetPublication struct {
	ExpectedGeneration causal.Generation
	Generation         causal.Generation
	Declarations       []catalog.SourceAuthorityDeclaration
}

// LocalProvisionProof joins the applied desired fleet, durable tenant, and
// complete source, catalog, and presentation preparation proof.
type LocalProvisionProof struct {
	Fleet       catalog.DesiredSourceAuthorityFleetState
	Tenant      LocalTenantAcknowledgement
	Preparation catalogproto.TenantPreparationProof
}

// LocalFileProviderLeaseCommit binds one provisional proof to a live session.
type LocalFileProviderLeaseCommit struct {
	Lease           catalogproto.FileProviderLeaseReceipt
	SessionID       string
	ProcessIdentity string
	ExpiresAt       time.Time
}

// LocalFileProviderLeaseRenew extends one exact committed live session.
type LocalFileProviderLeaseRenew struct {
	Lease     catalogproto.FileProviderLeaseReceipt
	ExpiresAt time.Time
}

type tenantSpecSource interface {
	Specs() []tenant.TenantSpec
}

type tenantRetirementProver interface {
	ProveTenantRetired(context.Context, string, catalog.TenantID, catalog.Generation) (catalog.TenantRetirementProof, error)
}

// LocalTenantController is the immutable-owner in-process product lifecycle surface.
type LocalTenantController struct {
	runtime *Runtime
	owner   tenant.OwnerID
	graph   *runtimeGraph
	scope   *localControllerScope
}

// LocalTenantController returns the immutable-owner in-process controller.
func (r *Runtime) LocalTenantController() *LocalTenantController {
	if r == nil {
		return &LocalTenantController{}
	}
	owner, _ := tenantOwnerFromProductOwner(r.config.Owner)
	return &LocalTenantController{runtime: r, owner: owner}
}

// Readiness waits for and identifies the exact admitted holder generation.
func (c *LocalTenantController) Readiness(ctx context.Context) (LocalRuntimeReadiness, error) {
	if c == nil || c.runtime == nil || c.owner == "" {
		return LocalRuntimeReadiness{}, ErrLocalTenantControllerUnavailable
	}
	if c.scope == nil {
		if err := c.runtime.WaitReady(ctx); err != nil {
			return LocalRuntimeReadiness{}, err
		}
	}
	graph, release, err := c.acquireGraph()
	if err != nil {
		return LocalRuntimeReadiness{}, err
	}
	defer release()
	if graph.runtimeOwnerRecord.Generation == (proc.OwnerGeneration{}) {
		return LocalRuntimeReadiness{}, fmt.Errorf("%w: runtime process generation is zero", catalog.ErrIntegrity)
	}
	return LocalRuntimeReadiness{
		RuntimeBuild: c.runtime.config.RuntimeBuild, ActivationGeneration: graph.activationGeneration,
		ProcessGeneration: graph.runtimeOwnerRecord.Generation,
	}, nil
}

// State returns one owner-fenced durable lifecycle snapshot.
func (c *LocalTenantController) State(ctx context.Context, id catalog.TenantID) (tenant.TenantStatus, error) {
	graph, release, err := c.acquireGraph()
	if err != nil {
		return tenant.TenantStatus{}, err
	}
	defer release()
	return c.state(graph, ctx, id)
}

// Provision durably provisions one exact tenant generation without preparing content.
func (c *LocalTenantController) Provision(
	ctx context.Context,
	spec tenant.TenantSpec,
) (LocalTenantAcknowledgement, error) {
	graph, release, err := c.acquireGraph()
	if err != nil {
		return LocalTenantAcknowledgement{}, err
	}
	defer release()
	return c.provision(graph, ctx, spec)
}

// Replace generation-fences one exact successor without preparing content.
func (c *LocalTenantController) Replace(
	ctx context.Context,
	expected catalog.Generation,
	next tenant.TenantSpec,
) (LocalTenantAcknowledgement, error) {
	graph, release, err := c.acquireGraph()
	if err != nil {
		return LocalTenantAcknowledgement{}, err
	}
	defer release()
	if next.OwnerID != c.owner {
		return LocalTenantAcknowledgement{}, tenant.ErrTenantOwnerMismatch
	}
	if err := graph.tenantLifecycle.ReplaceTenant(ctx, expected, next); err != nil {
		return LocalTenantAcknowledgement{}, err
	}
	return c.acknowledge(graph, ctx, next)
}

// Retire generation-fences removal and proves File Provider domain absence.
func (c *LocalTenantController) Retire(
	ctx context.Context,
	id catalog.TenantID,
	expected catalog.Generation,
) (LocalTenantRetirementProof, error) {
	graph, release, err := c.acquireGraph()
	if err != nil {
		return LocalTenantRetirementProof{}, err
	}
	defer release()
	if proof, err := c.proveRetirement(graph, ctx, id, expected); err == nil {
		return proof, nil
	} else if !errors.Is(err, catalog.ErrNotFound) {
		return LocalTenantRetirementProof{}, err
	}
	if err := graph.tenantLifecycle.RemoveTenant(ctx, id, expected, c.owner); err != nil {
		return LocalTenantRetirementProof{}, err
	}
	return c.proveRetirement(graph, ctx, id, expected)
}

// Prepare returns the complete source, catalog, and presentation proof for one generation.
func (c *LocalTenantController) Prepare(
	ctx context.Context,
	id catalog.TenantID,
	request LocalPreparationRequest,
) (catalogproto.TenantPreparationProof, error) {
	graph, release, err := c.acquireGraph()
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	defer release()
	return c.prepare(graph, ctx, id, request)
}

// PublishSourceFleet atomically publishes the immutable owner's complete desired source fleet.
func (c *LocalTenantController) PublishSourceFleet(
	ctx context.Context,
	publication LocalSourceFleetPublication,
) (catalog.DesiredSourceAuthorityFleetState, error) {
	graph, release, err := c.acquireGraph()
	if err != nil {
		return catalog.DesiredSourceAuthorityFleetState{}, err
	}
	defer release()
	declarations := append([]catalog.SourceAuthorityDeclaration(nil), publication.Declarations...)
	state, err := graph.sourceFleets.PublishDesiredSourceFleet(ctx, catalog.PublishDesiredSourceFleetRequest{
		Owner:              catalog.SourceAuthorityFleetOwnerID(c.owner),
		ExpectedGeneration: publication.ExpectedGeneration,
		Generation:         publication.Generation,
		Declarations:       declarations,
	})
	if err != nil {
		return catalog.DesiredSourceAuthorityFleetState{}, err
	}
	if err := validateLocalFleetState(
		state, catalog.SourceAuthorityFleetOwnerID(c.owner), publication.Generation, declarations,
	); err != nil {
		return catalog.DesiredSourceAuthorityFleetState{}, err
	}
	return state, nil
}

// CommitFileProviderLease promotes one exact provisional proof to live demand.
func (c *LocalTenantController) CommitFileProviderLease(
	ctx context.Context,
	request LocalFileProviderLeaseCommit,
) (catalogproto.FileProviderLeaseReceipt, error) {
	graph, release, err := c.acquireGraph()
	if err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	defer release()
	if request.Lease.State != catalogproto.FileProviderLeaseStateProvisional || request.SessionID == "" ||
		request.ProcessIdentity == "" || request.ExpiresAt.UnixNano() <= 0 {
		return catalogproto.FileProviderLeaseReceipt{}, fmt.Errorf("%w: invalid local File Provider lease commit", catalog.ErrInvalidObject)
	}
	lease, err := catalogLeaseFromLocalReceipt(request.Lease)
	if err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	lease.State = catalog.FileProviderLeaseCommitted
	lease.SessionID = request.SessionID
	lease.ProcessIdentity = request.ProcessIdentity
	lease.ExpiresAt = request.ExpiresAt.UTC()
	committed, err := graph.presentationLeases.CommitFileProviderLease(ctx, lease)
	if err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	return validateLocalLeaseResult(lease, committed, catalog.FileProviderLeaseCommitted)
}

// RenewFileProviderLease extends one exact committed live demand receipt.
func (c *LocalTenantController) RenewFileProviderLease(
	ctx context.Context,
	request LocalFileProviderLeaseRenew,
) (catalogproto.FileProviderLeaseReceipt, error) {
	graph, release, err := c.acquireGraph()
	if err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	defer release()
	if request.Lease.State != catalogproto.FileProviderLeaseStateCommitted || request.ExpiresAt.UnixNano() <= 0 {
		return catalogproto.FileProviderLeaseReceipt{}, fmt.Errorf("%w: invalid local File Provider lease renewal", catalog.ErrInvalidObject)
	}
	lease, err := catalogLeaseFromLocalReceipt(request.Lease)
	if err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	lease.ExpiresAt = request.ExpiresAt.UTC()
	renewed, err := graph.presentationLeases.RenewFileProviderLease(ctx, lease)
	if err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	return validateLocalLeaseResult(lease, renewed, catalog.FileProviderLeaseCommitted)
}

// ReleaseFileProviderLease retires one exact provisional or committed receipt.
func (c *LocalTenantController) ReleaseFileProviderLease(
	ctx context.Context,
	receipt catalogproto.FileProviderLeaseReceipt,
) (catalogproto.FileProviderLeaseReceipt, error) {
	graph, release, err := c.acquireGraph()
	if err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	defer release()
	if receipt.State != catalogproto.FileProviderLeaseStateProvisional && receipt.State != catalogproto.FileProviderLeaseStateCommitted &&
		receipt.State != catalogproto.FileProviderLeaseStateReleased {
		return catalogproto.FileProviderLeaseReceipt{}, fmt.Errorf("%w: invalid local File Provider lease release", catalog.ErrInvalidObject)
	}
	lease, err := catalogLeaseFromLocalReceipt(receipt)
	if err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	released, err := graph.presentationLeases.ReleaseFileProviderLease(ctx, lease)
	if err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	return validateLocalLeaseResult(lease, released, catalog.FileProviderLeaseReleased)
}

// ProvisionAndPrepare publishes one declaration, provisions its tenant, and
// returns only after exact source, catalog, and presentation convergence.
func (c *LocalTenantController) ProvisionAndPrepare(
	ctx context.Context,
	request LocalProvisionRequest,
) (LocalProvisionProof, error) {
	graph, release, err := c.acquireGraph()
	if err != nil {
		return LocalProvisionProof{}, err
	}
	defer release()
	if request.Tenant.OwnerID != c.owner ||
		request.Declaration.Authority != causal.SourceAuthorityID(request.Tenant.Content.ID) {
		return LocalProvisionProof{}, tenant.ErrTenantOwnerMismatch
	}
	fleet, err := publishLocalDeclaration(
		ctx, graph.sourceFleets, catalog.SourceAuthorityFleetOwnerID(c.owner), request.Declaration,
	)
	if err != nil {
		return LocalProvisionProof{}, err
	}
	ack, err := c.provision(graph, ctx, request.Tenant)
	if err != nil {
		return LocalProvisionProof{}, err
	}
	proof, err := c.prepare(graph, ctx, request.Tenant.ID, request.Preparation)
	if err != nil {
		return LocalProvisionProof{}, err
	}
	return LocalProvisionProof{Fleet: fleet, Tenant: ack, Preparation: proof}, nil
}

// ProvisionAndPrepare composes the local controller operation for the signed host.
func (r *Runtime) ProvisionAndPrepare(
	ctx context.Context,
	request LocalProvisionRequest,
) (LocalProvisionProof, error) {
	return r.LocalTenantController().ProvisionAndPrepare(ctx, request)
}

func (c *LocalTenantController) acquireGraph() (*runtimeGraph, func(), error) {
	if c == nil || c.owner == "" {
		return nil, nil, ErrLocalTenantControllerUnavailable
	}
	if c.scope != nil {
		release, err := c.scope.acquire()
		if err != nil {
			return nil, nil, err
		}
		if err := validateLocalTenantGraph(c.graph); err != nil {
			release()
			return nil, nil, err
		}
		return c.graph, release, nil
	}
	if c.runtime == nil || c.runtime.graphs == nil {
		return nil, nil, ErrLocalTenantControllerUnavailable
	}
	graph, release, err := c.runtime.graphs.Acquire()
	if err != nil {
		if errors.Is(err, daemon.ErrPublicationUnavailable) || errors.Is(err, daemon.ErrRuntimeNotReady) ||
			errors.Is(err, daemon.ErrDraining) {
			return nil, nil, ErrLocalTenantControllerUnavailable
		}
		return nil, nil, err
	}
	if err := validateLocalTenantGraph(graph); err != nil {
		release()
		return nil, nil, err
	}
	return graph, release, nil
}

func validateLocalTenantGraph(graph *runtimeGraph) error {
	if graph == nil || graph.readiness == nil || !graph.readiness.Published() || graph.tenantLifecycle == nil ||
		graph.tenantPreparation == nil || graph.sourceFleets == nil || graph.tenantSpecs == nil ||
		graph.tenantRetirements == nil || graph.presentationLeases == nil || graph.activationGeneration == "" {
		return ErrLocalTenantControllerUnavailable
	}
	return nil
}

func (c *LocalTenantController) proveRetirement(
	graph *runtimeGraph,
	ctx context.Context,
	id catalog.TenantID,
	generation catalog.Generation,
) (LocalTenantRetirementProof, error) {
	proof, err := graph.tenantRetirements.ProveTenantRetired(ctx, string(c.owner), id, generation)
	if err != nil {
		return LocalTenantRetirementProof{}, err
	}
	if proof.Tenant != id || proof.Generation != generation || !proof.FileProviderAbsent {
		return LocalTenantRetirementProof{}, fmt.Errorf("%w: local tenant retirement proof changed", catalog.ErrIntegrity)
	}
	return LocalTenantRetirementProof{
		Tenant: proof.Tenant, Generation: proof.Generation, FileProviderAbsent: proof.FileProviderAbsent,
	}, nil
}

func (c *LocalTenantController) state(
	graph *runtimeGraph,
	ctx context.Context,
	id catalog.TenantID,
) (tenant.TenantStatus, error) {
	status, err := graph.tenantLifecycle.State(ctx, id, c.owner)
	if err != nil {
		return tenant.TenantStatus{}, err
	}
	if status.Owner != c.owner || status.State.Tenant != id || status.State.Generation == 0 {
		return tenant.TenantStatus{}, fmt.Errorf("%w: local tenant state is not exact", catalog.ErrIntegrity)
	}
	return status, nil
}

func (c *LocalTenantController) provision(
	graph *runtimeGraph,
	ctx context.Context,
	spec tenant.TenantSpec,
) (LocalTenantAcknowledgement, error) {
	if spec.OwnerID != c.owner {
		return LocalTenantAcknowledgement{}, tenant.ErrTenantOwnerMismatch
	}
	if err := graph.tenantLifecycle.ProvisionTenant(ctx, spec); err != nil {
		return LocalTenantAcknowledgement{}, err
	}
	return c.acknowledge(graph, ctx, spec)
}

func (c *LocalTenantController) acknowledge(
	graph *runtimeGraph,
	ctx context.Context,
	spec tenant.TenantSpec,
) (LocalTenantAcknowledgement, error) {
	status, err := c.state(graph, ctx, spec.ID)
	if err != nil {
		return LocalTenantAcknowledgement{}, err
	}
	stored, err := localTenantSpec(graph, spec.ID)
	if err != nil {
		return LocalTenantAcknowledgement{}, err
	}
	if status.State.Generation != spec.Generation || stored != spec {
		return LocalTenantAcknowledgement{}, fmt.Errorf("%w: local tenant acknowledgement changed", catalog.ErrIntegrity)
	}
	return localTenantAcknowledgement(spec), nil
}

func (c *LocalTenantController) prepare(
	graph *runtimeGraph,
	ctx context.Context,
	id catalog.TenantID,
	request LocalPreparationRequest,
) (catalogproto.TenantPreparationProof, error) {
	spec, err := localTenantSpec(graph, id)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	if spec.OwnerID != c.owner {
		return catalogproto.TenantPreparationProof{}, tenant.ErrTenantOwnerMismatch
	}
	protocol, err := localPreparationProtocol(graph, spec, request)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	proof, err := graph.tenantPreparation.PrepareTenant(ctx, catalogservice.Identity{}, id, protocol)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	if err := validateLocalPreparationProof(spec, request, protocol, proof); err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	return proof, nil
}

func localPreparationProtocol(
	graph *runtimeGraph,
	spec tenant.TenantSpec,
	request LocalPreparationRequest,
) (catalogproto.PrepareTenantRequest, error) {
	if request.Generation == 0 || request.Generation != spec.Generation ||
		!spec.Traits.Presentations.Has(request.Presentation) {
		return catalogproto.PrepareTenantRequest{}, fmt.Errorf("%w: local preparation does not match tenant generation", catalog.ErrGenerationMismatch)
	}
	result := catalogproto.PrepareTenantRequest{
		Protocol: catalogproto.Version, Generation: uint64(request.Generation), ActivationGeneration: graph.activationGeneration,
	}
	switch request.Presentation {
	case catalog.PresentationMount:
		result.Presentation = catalogproto.PresentationKindMount
		if len(request.CriticalObjects) != 0 || request.LeaseID != "" || !request.LeaseExpiresAt.IsZero() {
			return catalogproto.PrepareTenantRequest{}, fmt.Errorf("%w: mount preparation carries File Provider policy", catalog.ErrInvalidObject)
		}
	case catalog.PresentationFileProvider:
		result.Presentation = catalogproto.PresentationKindFileProvider
		digest, err := catalog.CriticalObjectPolicyDigest(request.CriticalObjects)
		if err != nil {
			return catalogproto.PrepareTenantRequest{}, err
		}
		if request.LeaseExpiresAt.UnixNano() <= 0 {
			return catalogproto.PrepareTenantRequest{}, fmt.Errorf("%w: File Provider lease expiry is invalid", catalog.ErrInvalidObject)
		}
		result.CriticalPolicyDigest = hex.EncodeToString(digest[:])
		result.CriticalObjects = make([]catalogproto.CriticalObjectRequirement, len(request.CriticalObjects))
		for index, object := range request.CriticalObjects {
			result.CriticalObjects[index] = catalogproto.CriticalObjectRequirement{LogicalID: object.LogicalID, Role: object.Role}
		}
		result.LeaseID = request.LeaseID
		result.LeaseExpiresUnixNano = uint64(request.LeaseExpiresAt.UnixNano())
	default:
		return catalogproto.PrepareTenantRequest{}, fmt.Errorf("%w: unknown local presentation", catalog.ErrInvalidObject)
	}
	if err := catalogproto.Validate(result); err != nil {
		return catalogproto.PrepareTenantRequest{}, err
	}
	return result, nil
}

func validateLocalPreparationProof(
	spec tenant.TenantSpec,
	request LocalPreparationRequest,
	protocol catalogproto.PrepareTenantRequest,
	proof catalogproto.TenantPreparationProof,
) error {
	if err := catalogproto.Validate(proof); err != nil {
		return err
	}
	catalogProof := proof.Catalog
	if catalog.TenantID(catalogProof.Tenant) != spec.ID || catalog.Generation(catalogProof.Generation) != spec.Generation ||
		catalogProof.Requested == 0 || catalogProof.Desired != catalogProof.Requested || catalogProof.Observed != catalogProof.Requested ||
		catalogProof.Verified != catalogProof.Requested || catalogProof.Applied != catalogProof.Requested ||
		proof.CatalogRevision != catalogProof.Requested || causal.SourceAuthorityID(proof.SourceAuthority) != causal.SourceAuthorityID(spec.Content.ID) {
		return fmt.Errorf("%w: local preparation catalog proof changed", catalog.ErrIntegrity)
	}
	switch request.Presentation {
	case catalog.PresentationMount:
		mount := proof.Presentation.Mount
		if proof.Presentation.Kind != catalogproto.PresentationKindMount || mount == nil ||
			catalog.TenantID(mount.TenantID) != spec.ID || catalog.Generation(mount.Generation) != spec.Generation ||
			mount.PublicPath != spec.Mount.PresentationRoot || mount.ActivationGeneration != protocol.ActivationGeneration {
			return fmt.Errorf("%w: local mount preparation proof changed", catalog.ErrIntegrity)
		}
	case catalog.PresentationFileProvider:
		presentation, readiness := proof.Presentation.FileProvider, proof.CriticalReadiness
		if proof.Presentation.Kind != catalogproto.PresentationKindFileProvider || presentation == nil || readiness == nil ||
			catalog.TenantID(presentation.TenantID) != spec.ID || catalog.Generation(presentation.Generation) != spec.Generation ||
			string(presentation.PresentationInstanceID) != spec.FileProvider.PresentationInstanceID ||
			presentation.ActivationGeneration != protocol.ActivationGeneration || readiness.PolicyDigest != protocol.CriticalPolicyDigest ||
			readiness.Lease.LeaseID != request.LeaseID || readiness.Lease.ExpiresUnixNano != protocol.LeaseExpiresUnixNano {
			return fmt.Errorf("%w: local File Provider preparation proof changed", catalog.ErrIntegrity)
		}
		if len(readiness.Objects) != len(request.CriticalObjects) {
			return fmt.Errorf("%w: local critical object proof count changed", catalog.ErrIntegrity)
		}
		for index, object := range request.CriticalObjects {
			if readiness.Objects[index].LogicalID != object.LogicalID || readiness.Objects[index].Role != object.Role {
				return fmt.Errorf("%w: local critical object policy changed", catalog.ErrIntegrity)
			}
		}
	default:
		return fmt.Errorf("%w: unknown local presentation", catalog.ErrInvalidObject)
	}
	return nil
}

func localTenantSpec(graph *runtimeGraph, id catalog.TenantID) (tenant.TenantSpec, error) {
	specs := graph.tenantSpecs.Specs()
	index, found := slices.BinarySearchFunc(specs, id, func(spec tenant.TenantSpec, target catalog.TenantID) int {
		return stringCompare(string(spec.ID), string(target))
	})
	if !found {
		return tenant.TenantSpec{}, tenant.ErrTenantNotFound
	}
	return specs[index], nil
}

func localTenantAcknowledgement(spec tenant.TenantSpec) LocalTenantAcknowledgement {
	return LocalTenantAcknowledgement{
		Tenant: spec.ID, Generation: spec.Generation, Presentations: spec.Traits.Presentations,
	}
}

func publishLocalDeclaration(
	ctx context.Context,
	service catalogservice.SourceFleetService,
	owner catalog.SourceAuthorityFleetOwnerID,
	declaration catalog.SourceAuthorityDeclaration,
) (catalog.DesiredSourceAuthorityFleetState, error) {
	if _, err := catalog.SourceAuthorityFleetDeclarationsDigest([]catalog.SourceAuthorityDeclaration{declaration}); err != nil {
		return catalog.DesiredSourceAuthorityFleetState{}, err
	}
	for attempt := 0; attempt < localDesiredFleetCASLimit; attempt++ {
		state, declarations, err := readLocalDesiredFleet(ctx, service, owner)
		if err != nil {
			return catalog.DesiredSourceAuthorityFleetState{}, err
		}
		merged, changed, err := mergeLocalDeclaration(declarations, declaration)
		if err != nil {
			return catalog.DesiredSourceAuthorityFleetState{}, err
		}
		if state != nil && !changed {
			return *state, nil
		}
		expected, generation := causal.Generation(0), causal.Generation(1)
		if state != nil {
			if state.Generation == causal.Generation(math.MaxUint64) {
				return catalog.DesiredSourceAuthorityFleetState{}, errors.New("FuseKit runtime: desired source fleet exhausted v1 generations")
			}
			expected, generation = state.Generation, state.Generation+1
		}
		result, err := service.PublishDesiredSourceFleet(ctx, catalog.PublishDesiredSourceFleetRequest{
			Owner: owner, ExpectedGeneration: expected, Generation: generation, Declarations: merged,
		})
		if err == nil {
			if err := validateLocalFleetState(result, owner, generation, merged); err != nil {
				return catalog.DesiredSourceAuthorityFleetState{}, err
			}
			return result, nil
		}
		if !errors.Is(err, catalog.ErrGenerationMismatch) && !errors.Is(err, catalog.ErrMutationConflict) {
			return catalog.DesiredSourceAuthorityFleetState{}, err
		}
	}
	return catalog.DesiredSourceAuthorityFleetState{}, errors.New("FuseKit runtime: desired source fleet changed during every bounded CAS attempt")
}

func readLocalDesiredFleet(
	ctx context.Context,
	service catalogservice.SourceFleetService,
	owner catalog.SourceAuthorityFleetOwnerID,
) (*catalog.DesiredSourceAuthorityFleetState, []catalog.SourceAuthorityDeclaration, error) {
	request := catalog.DesiredSourceFleetPageRequest{Owner: owner, Limit: catalog.TopologyPageLimit}
	page, err := service.DesiredSourceFleetPage(ctx, request)
	if errors.Is(err, catalog.ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if err := page.Validate(request); err != nil {
		return nil, nil, err
	}
	state := page.State
	declarations := append([]catalog.SourceAuthorityDeclaration(nil), page.Declarations...)
	for page.Next != "" {
		request.Generation = state.Generation
		request.DeclarationsDigest = state.DeclarationsDigest
		request.After = page.Next
		page, err = service.DesiredSourceFleetPage(ctx, request)
		if err != nil {
			return nil, nil, err
		}
		if err := page.Validate(request); err != nil {
			return nil, nil, err
		}
		if page.State != state {
			return nil, nil, catalog.ErrIntegrity
		}
		declarations = append(declarations, page.Declarations...)
	}
	if err := validateLocalFleetState(state, owner, state.Generation, declarations); err != nil {
		return nil, nil, err
	}
	return &state, declarations, nil
}

func validateLocalFleetState(
	state catalog.DesiredSourceAuthorityFleetState,
	owner catalog.SourceAuthorityFleetOwnerID,
	generation causal.Generation,
	declarations []catalog.SourceAuthorityDeclaration,
) error {
	if err := state.Validate(); err != nil || state.Owner != owner || state.Generation != generation ||
		state.AuthorityCount != uint64(len(declarations)) {
		return fmt.Errorf("%w: local desired fleet state changed", catalog.ErrIntegrity)
	}
	authorities := make([]causal.SourceAuthorityID, len(declarations))
	for index, declaration := range declarations {
		authorities[index] = declaration.Authority
	}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest(authorities)
	if err != nil {
		return err
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		return err
	}
	if state.AuthoritiesDigest != authoritiesDigest || state.DeclarationsDigest != declarationsDigest {
		return fmt.Errorf("%w: local desired fleet digest changed", catalog.ErrIntegrity)
	}
	return nil
}

func mergeLocalDeclaration(
	current []catalog.SourceAuthorityDeclaration,
	declaration catalog.SourceAuthorityDeclaration,
) ([]catalog.SourceAuthorityDeclaration, bool, error) {
	index, found := slices.BinarySearchFunc(current, declaration, func(left, right catalog.SourceAuthorityDeclaration) int {
		return bytes.Compare([]byte(left.Authority), []byte(right.Authority))
	})
	if found {
		if current[index].Authority == declaration.Authority && current[index].DriverID == declaration.DriverID &&
			bytes.Equal(current[index].DriverConfig, declaration.DriverConfig) &&
			current[index].DeclarationDigest == declaration.DeclarationDigest {
			return append([]catalog.SourceAuthorityDeclaration(nil), current...), false, nil
		}
		return nil, false, catalog.ErrMutationConflict
	}
	result := make([]catalog.SourceAuthorityDeclaration, len(current)+1)
	copy(result, current[:index])
	result[index] = declaration
	copy(result[index+1:], current[index:])
	return result, true, nil
}

func catalogLeaseFromLocalReceipt(receipt catalogproto.FileProviderLeaseReceipt) (catalog.FileProviderLease, error) {
	if err := catalogproto.Validate(receipt); err != nil {
		return catalog.FileProviderLease{}, err
	}
	tenantID, err := catalog.NewTenantID(string(receipt.TenantID))
	if err != nil {
		return catalog.FileProviderLease{}, err
	}
	root, err := catalog.ParseObjectID(string(receipt.RootID))
	if err != nil {
		return catalog.FileProviderLease{}, err
	}
	policy, err := decodeLocalDigest(receipt.PolicyDigest)
	if err != nil {
		return catalog.FileProviderLease{}, err
	}
	resolution, err := decodeLocalDigest(receipt.ResolutionDigest)
	if err != nil {
		return catalog.FileProviderLease{}, err
	}
	publicationBytes, err := hex.DecodeString(string(receipt.SourcePublication))
	if err != nil || len(publicationBytes) != len(causal.OperationID{}) {
		return catalog.FileProviderLease{}, fmt.Errorf("%w: File Provider lease publication is invalid", catalog.ErrInvalidObject)
	}
	var publication causal.OperationID
	copy(publication[:], publicationBytes)
	var state catalog.FileProviderLeaseState
	switch receipt.State {
	case catalogproto.FileProviderLeaseStateProvisional:
		state = catalog.FileProviderLeaseProvisional
	case catalogproto.FileProviderLeaseStateCommitted:
		state = catalog.FileProviderLeaseCommitted
	case catalogproto.FileProviderLeaseStateReleased:
		state = catalog.FileProviderLeaseReleased
	default:
		return catalog.FileProviderLease{}, fmt.Errorf("%w: File Provider lease state is invalid", catalog.ErrInvalidObject)
	}
	return catalog.FileProviderLease{
		ID: receipt.LeaseID, Tenant: tenantID, DomainID: causal.DomainID(receipt.DomainID), Generation: catalog.Generation(receipt.Generation),
		Root: root, PresentationInstance: string(receipt.PresentationInstanceID), State: state,
		SessionID: receipt.SessionID, ProcessIdentity: receipt.ProcessIdentity, PolicyDigest: policy, ResolutionDigest: resolution,
		CatalogHead: catalog.Revision(receipt.CatalogHead), SourceAuthority: causal.SourceAuthorityID(receipt.SourceAuthority),
		SourcePublication: publication, SourceRevision: causal.Revision(receipt.SourceRevision),
		ActivationGeneration: receipt.ActivationGeneration, ExpiresAt: time.Unix(0, int64(receipt.ExpiresUnixNano)).UTC(),
	}, nil
}

func validateLocalLeaseResult(
	expected catalog.FileProviderLease,
	actual catalog.FileProviderLease,
	state catalog.FileProviderLeaseState,
) (catalogproto.FileProviderLeaseReceipt, error) {
	if actual.ID != expected.ID || actual.Tenant != expected.Tenant || actual.DomainID != expected.DomainID ||
		actual.Generation != expected.Generation || actual.Root != expected.Root || actual.PresentationInstance != expected.PresentationInstance ||
		actual.SessionID != expected.SessionID || actual.ProcessIdentity != expected.ProcessIdentity || actual.PolicyDigest != expected.PolicyDigest ||
		actual.ResolutionDigest != expected.ResolutionDigest || actual.CatalogHead != expected.CatalogHead ||
		actual.SourceAuthority != expected.SourceAuthority || actual.SourcePublication != expected.SourcePublication ||
		actual.SourceRevision != expected.SourceRevision || actual.ActivationGeneration != expected.ActivationGeneration ||
		!actual.ExpiresAt.Equal(expected.ExpiresAt) || actual.State != state {
		return catalogproto.FileProviderLeaseReceipt{}, fmt.Errorf("%w: local File Provider lease result changed", catalog.ErrIntegrity)
	}
	return localLeaseReceipt(actual)
}

func localLeaseReceipt(lease catalog.FileProviderLease) (catalogproto.FileProviderLeaseReceipt, error) {
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
	result := catalogproto.FileProviderLeaseReceipt{
		LeaseID: lease.ID, TenantID: catalogproto.TenantID(lease.Tenant), DomainID: catalogproto.DomainID(lease.DomainID),
		Generation: uint64(lease.Generation), RootID: catalogproto.ObjectID(lease.Root.String()),
		PresentationInstanceID: catalogproto.PresentationInstanceID(lease.PresentationInstance), State: state,
		SessionID: lease.SessionID, ProcessIdentity: lease.ProcessIdentity,
		PolicyDigest: hex.EncodeToString(lease.PolicyDigest[:]), ResolutionDigest: hex.EncodeToString(lease.ResolutionDigest[:]),
		CatalogHead: uint64(lease.CatalogHead), SourceAuthority: catalogproto.SourceAuthorityID(lease.SourceAuthority),
		SourcePublication: catalogproto.OperationID(hex.EncodeToString(lease.SourcePublication[:])), SourceRevision: uint64(lease.SourceRevision),
		ActivationGeneration: lease.ActivationGeneration, ExpiresUnixNano: uint64(lease.ExpiresAt.UnixNano()),
	}
	if err := catalogproto.Validate(result); err != nil {
		return catalogproto.FileProviderLeaseReceipt{}, err
	}
	return result, nil
}

func decodeLocalDigest(value string) ([32]byte, error) {
	var digest [32]byte
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != len(digest) {
		return digest, fmt.Errorf("%w: invalid digest", catalog.ErrInvalidObject)
	}
	copy(digest[:], raw)
	return digest, nil
}
