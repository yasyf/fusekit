package holder

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

const localDesiredFleetCASLimit = 8

// ErrLocalTenantControllerUnavailable means the holder graph is not published.
var ErrLocalTenantControllerUnavailable = errors.New("FuseKit runtime: local tenant controller is unavailable")

// LocalRuntimeReadiness identifies one published in-process holder generation.
type LocalRuntimeReadiness struct {
	RuntimeBuild         string
	ActivationGeneration string
}

// LocalTenantAcknowledgement proves one exact durable tenant definition.
type LocalTenantAcknowledgement struct {
	Tenant        catalog.TenantID
	Generation    catalog.Generation
	Presentations catalog.PresentationSet
}

// LocalTenantRemovalProof proves one generation and its File Provider domain are absent.
type LocalTenantRemovalProof struct {
	Tenant             catalog.TenantID
	Generation         catalog.Generation
	FileProviderAbsent bool
}

// LocalProvisionRequest composes one source declaration, tenant definition,
// and exact preparation request.
type LocalProvisionRequest struct {
	Owner       catalog.SourceAuthorityFleetOwnerID
	Declaration catalog.SourceAuthorityDeclaration
	Tenant      tenant.TenantSpec
	Preparation catalogproto.PrepareTenantRequest
}

// LocalProvisionProof joins the applied desired fleet, durable tenant, and
// complete source, catalog, and presentation preparation proof.
type LocalProvisionProof struct {
	Fleet       catalog.DesiredSourceAuthorityFleetState
	Tenant      LocalTenantAcknowledgement
	Preparation catalogproto.TenantPreparationProof
}

// LocalTenantController is the in-process product lifecycle surface bound to
// one holder Runtime.
type LocalTenantController struct {
	runtime *Runtime
	owner   tenant.OwnerID
}

// LocalTenantController returns the immutable-owner in-process controller.
func (r *Runtime) LocalTenantController() *LocalTenantController {
	if r == nil {
		return &LocalTenantController{}
	}
	owner, _ := tenantOwnerFromProductOwner(r.config.Owner)
	return &LocalTenantController{runtime: r, owner: owner}
}

// Readiness waits for and identifies the exact published holder generation.
func (c *LocalTenantController) Readiness(ctx context.Context) (LocalRuntimeReadiness, error) {
	if c == nil || c.runtime == nil || c.owner == "" {
		return LocalRuntimeReadiness{}, ErrLocalTenantControllerUnavailable
	}
	if err := c.runtime.WaitReady(ctx); err != nil {
		return LocalRuntimeReadiness{}, err
	}
	graph, err := c.graph()
	if err != nil {
		return LocalRuntimeReadiness{}, err
	}
	return LocalRuntimeReadiness{
		RuntimeBuild: c.runtime.config.RuntimeBuild, ActivationGeneration: graph.activationGeneration,
	}, nil
}

// State returns one owner-fenced durable lifecycle snapshot.
func (c *LocalTenantController) State(
	ctx context.Context,
	owner tenant.OwnerID,
	id catalog.TenantID,
) (tenant.TenantStatus, error) {
	graph, err := c.ownedGraph(owner)
	if err != nil {
		return tenant.TenantStatus{}, err
	}
	return graph.tenantLifecycle.State(ctx, id, owner)
}

// Provision durably provisions one exact tenant generation without preparing content.
func (c *LocalTenantController) Provision(
	ctx context.Context,
	spec tenant.TenantSpec,
) (LocalTenantAcknowledgement, error) {
	graph, err := c.ownedGraph(spec.OwnerID)
	if err != nil {
		return LocalTenantAcknowledgement{}, err
	}
	if err := graph.tenantLifecycle.ProvisionTenant(ctx, spec); err != nil {
		return LocalTenantAcknowledgement{}, err
	}
	return localTenantAcknowledgement(spec), nil
}

// Replace generation-fences one exact successor without preparing content.
func (c *LocalTenantController) Replace(
	ctx context.Context,
	expected catalog.Generation,
	next tenant.TenantSpec,
) (LocalTenantAcknowledgement, error) {
	graph, err := c.ownedGraph(next.OwnerID)
	if err != nil {
		return LocalTenantAcknowledgement{}, err
	}
	if err := graph.tenantLifecycle.ReplaceTenant(ctx, expected, next); err != nil {
		return LocalTenantAcknowledgement{}, err
	}
	return localTenantAcknowledgement(next), nil
}

// Remove generation-fences retirement and proves File Provider domain absence.
func (c *LocalTenantController) Remove(
	ctx context.Context,
	owner tenant.OwnerID,
	id catalog.TenantID,
	expected catalog.Generation,
) (LocalTenantRemovalProof, error) {
	graph, err := c.ownedGraph(owner)
	if err != nil {
		return LocalTenantRemovalProof{}, err
	}
	if err := graph.tenantLifecycle.RemoveTenant(ctx, id, expected, owner); err != nil {
		return LocalTenantRemovalProof{}, err
	}
	return LocalTenantRemovalProof{Tenant: id, Generation: expected, FileProviderAbsent: true}, nil
}

// Prepare returns the complete source, catalog, and presentation proof for one generation.
func (c *LocalTenantController) Prepare(
	ctx context.Context,
	id catalog.TenantID,
	request catalogproto.PrepareTenantRequest,
) (catalogproto.TenantPreparationProof, error) {
	graph, err := c.graph()
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	if request.Protocol != catalogproto.Version {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: local preparation protocol is not exact", catalog.ErrInvalidObject)
	}
	return graph.tenantPreparation.PrepareTenant(ctx, catalogservice.Identity{}, id, request)
}

// ProvisionAndPrepare publishes one declaration, provisions its tenant, and
// returns only after exact source, catalog, and presentation convergence.
func (c *LocalTenantController) ProvisionAndPrepare(
	ctx context.Context,
	request LocalProvisionRequest,
) (LocalProvisionProof, error) {
	graph, err := c.graph()
	if err != nil {
		return LocalProvisionProof{}, err
	}
	if request.Owner != catalog.SourceAuthorityFleetOwnerID(c.owner) || request.Tenant.OwnerID != c.owner ||
		request.Declaration.Authority != causal.SourceAuthorityID(request.Tenant.Content.ID) {
		return LocalProvisionProof{}, tenant.ErrTenantOwnerMismatch
	}
	if request.Preparation.Presentation != catalogproto.PresentationKindMount ||
		!request.Tenant.Traits.Presentations.Has(catalog.PresentationMount) {
		return LocalProvisionProof{}, fmt.Errorf("%w: local provision composition requires mount presentation", catalog.ErrInvalidObject)
	}
	fleet, err := publishLocalDeclaration(ctx, graph.sourceFleets, request.Owner, request.Declaration)
	if err != nil {
		return LocalProvisionProof{}, err
	}
	ack, err := c.Provision(ctx, request.Tenant)
	if err != nil {
		return LocalProvisionProof{}, err
	}
	proof, err := c.Prepare(ctx, request.Tenant.ID, request.Preparation)
	if err != nil {
		return LocalProvisionProof{}, err
	}
	if err := validateLocalProvisionProof(request, ack, proof); err != nil {
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

func (c *LocalTenantController) ownedGraph(owner tenant.OwnerID) (*runtimeGraph, error) {
	if c == nil || owner == "" || owner != c.owner {
		return nil, tenant.ErrTenantOwnerMismatch
	}
	return c.graph()
}

func (c *LocalTenantController) graph() (*runtimeGraph, error) {
	if c == nil || c.runtime == nil || c.owner == "" {
		return nil, ErrLocalTenantControllerUnavailable
	}
	return c.runtime.localTenantGraph()
}

func (r *Runtime) localTenantGraph() (*runtimeGraph, error) {
	if r == nil {
		return nil, ErrLocalTenantControllerUnavailable
	}
	r.graphMu.Lock()
	graph := r.graph
	r.graphMu.Unlock()
	if graph == nil || graph.readiness == nil || !graph.readiness.Published() ||
		graph.tenantLifecycle == nil || graph.tenantPreparation == nil || graph.sourceFleets == nil ||
		graph.activationGeneration == "" {
		return nil, ErrLocalTenantControllerUnavailable
	}
	return graph, nil
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
	for attempt := 0; attempt < localDesiredFleetCASLimit; attempt++ {
		state, declarations, err := readLocalDesiredFleet(ctx, service, owner)
		if err != nil {
			return catalog.DesiredSourceAuthorityFleetState{}, err
		}
		merged, changed, err := mergeLocalDeclaration(declarations, declaration)
		if err != nil {
			return catalog.DesiredSourceAuthorityFleetState{}, err
		}
		expected, generation := causal.Generation(0), causal.Generation(1)
		if state != nil {
			expected, generation = state.Generation, state.Generation+1
			if !changed {
				expected, generation = state.Generation-1, state.Generation
			} else if state.Generation == causal.Generation(math.MaxUint64) {
				return catalog.DesiredSourceAuthorityFleetState{}, errors.New("FuseKit runtime: desired source fleet exhausted v1 generations")
			}
		}
		result, err := service.PublishDesiredSourceFleet(ctx, catalog.PublishDesiredSourceFleetRequest{
			Owner: owner, ExpectedGeneration: expected, Generation: generation, Declarations: merged,
		})
		if err == nil {
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
		if page.State != state || len(page.Declarations) == 0 {
			return nil, nil, catalog.ErrIntegrity
		}
		declarations = append(declarations, page.Declarations...)
	}
	if uint64(len(declarations)) != state.AuthorityCount {
		return nil, nil, catalog.ErrIntegrity
	}
	return &state, declarations, nil
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
			return current, false, nil
		}
		return nil, false, catalog.ErrMutationConflict
	}
	result := make([]catalog.SourceAuthorityDeclaration, len(current)+1)
	copy(result, current[:index])
	result[index] = declaration
	copy(result[index+1:], current[index:])
	return result, true, nil
}

func validateLocalProvisionProof(
	request LocalProvisionRequest,
	ack LocalTenantAcknowledgement,
	proof catalogproto.TenantPreparationProof,
) error {
	catalogProof := proof.Catalog
	if ack.Tenant != request.Tenant.ID || ack.Generation != request.Tenant.Generation ||
		catalog.TenantID(catalogProof.Tenant) != request.Tenant.ID ||
		catalog.Generation(catalogProof.Generation) != request.Tenant.Generation || catalogProof.Requested == 0 ||
		catalogProof.Desired != catalogProof.Requested || catalogProof.Observed != catalogProof.Requested ||
		catalogProof.Verified != catalogProof.Requested || catalogProof.Applied != catalogProof.Requested ||
		proof.Presentation.Mount == nil || catalog.TenantID(proof.Presentation.Mount.TenantID) != request.Tenant.ID ||
		catalog.Generation(proof.Presentation.Mount.Generation) != request.Tenant.Generation ||
		proof.Presentation.Mount.PublicPath != request.Tenant.Mount.PresentationRoot ||
		proof.Presentation.Mount.ActivationGeneration == "" ||
		causal.SourceAuthorityID(proof.SourceAuthority) != request.Declaration.Authority || proof.SourceRevision == 0 ||
		proof.CatalogRevision != catalogProof.Requested || proof.SourcePublication == "" || proof.ChangeID == "" || proof.OperationID == "" {
		return fmt.Errorf("%w: local provision proof is incomplete", catalog.ErrIntegrity)
	}
	return nil
}
