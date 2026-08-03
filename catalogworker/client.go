package catalogworker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
)

// Client is the typed remote catalog surface for one exact worker generation.
type Client struct {
	wire      sessionClient
	deadlines map[string]time.Duration
	identity  WorkerIdentity
}

type sessionClient interface {
	Call(ctx context.Context, op string, body []byte) (daemonkit.Reply, error)
	Close(ctx context.Context) error
}

// TransportError means the exact worker generation did not deliver a valid
// response and must be poisoned before another storage call is admitted.
type TransportError struct {
	Message string
	Cause   error
}

func (e *TransportError) Error() string { return "catalog worker transport: " + e.Message }
func (e *TransportError) Unwrap() error { return e.Cause }

func newOwnedClient(client sessionClient, identity WorkerIdentity) (*Client, error) {
	if client == nil {
		return nil, errors.New("catalog worker: exact owned transport session is required")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	_, deadlines := generatedDeadlines(childSessionServerDeadline, childSessionClientDeadline)
	return &Client{wire: client, deadlines: deadlines, identity: identity}, nil
}

// Close settles the session this client owns.
func (c *Client) Close(ctx context.Context) error {
	return c.wire.Close(ctx)
}

// Head returns one tenant's current catalog revision.
func (c *Client) Head(ctx context.Context, tenant catalog.TenantID) (catalog.Revision, error) {
	header, err := c.header()
	if err != nil {
		return 0, err
	}
	response, err := call[headResponse](ctx, c, OperationHead, headRequest{Header: header, Tenant: tenant})
	if err := validateResponse(header, response.Header, err); err != nil {
		return 0, err
	}
	return response.Revision, nil
}

func (c *Client) CompactionFloor(ctx context.Context, tenant catalog.TenantID) (catalog.Revision, error) {
	header, err := c.header()
	if err != nil {
		return 0, err
	}
	response, err := call[compactionFloorResponse](ctx, c, OperationCompactionFloor, compactionFloorRequest{Header: header, Tenant: tenant})
	if err := validateResponse(header, response.Header, err); err != nil {
		return 0, err
	}
	return response.Revision, nil
}

func (c *Client) Tenant(ctx context.Context, tenant catalog.TenantID) (catalog.TenantMetadata, error) {
	header, err := c.header()
	if err != nil {
		return catalog.TenantMetadata{}, err
	}
	response, err := call[tenantResponse](ctx, c, OperationTenant, tenantRequest{Header: header, Tenant: tenant})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.TenantMetadata{}, err
	}
	return response.Metadata, nil
}

func (c *Client) Root(ctx context.Context, tenant catalog.TenantID) (catalog.Object, error) {
	header, err := c.header()
	if err != nil {
		return catalog.Object{}, err
	}
	response, err := call[rootResponse](ctx, c, OperationRoot, rootRequest{Header: header, Tenant: tenant})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.Object{}, err
	}
	return response.Object, nil
}

func (c *Client) Lookup(ctx context.Context, tenant catalog.TenantID, presentation catalog.Presentation, id catalog.ObjectID) (catalog.Object, error) {
	header, err := c.header()
	if err != nil {
		return catalog.Object{}, err
	}
	response, err := call[lookupResponse](ctx, c, OperationLookup, lookupRequest{Header: header, Tenant: tenant, Presentation: presentation, ID: id})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.Object{}, err
	}
	return response.Object, nil
}

func (c *Client) LookupName(ctx context.Context, tenant catalog.TenantID, presentation catalog.Presentation, parent catalog.ObjectID, name string) (catalog.Object, error) {
	header, err := c.header()
	if err != nil {
		return catalog.Object{}, err
	}
	response, err := call[lookupNameResponse](ctx, c, OperationLookupName, lookupNameRequest{Header: header, Tenant: tenant, Presentation: presentation, Parent: parent, Name: name})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.Object{}, err
	}
	return response.Object, nil
}

func (c *Client) Snapshot(ctx context.Context, tenant catalog.TenantID, scope catalog.EnumerationScope, revision catalog.Revision, cursor catalog.SnapshotCursor, limit int) (catalog.SnapshotPage, error) {
	header, err := c.header()
	if err != nil {
		return catalog.SnapshotPage{}, err
	}
	response, err := call[snapshotResponse](ctx, c, OperationSnapshot, snapshotRequest{Header: header, Tenant: tenant, Scope: scope, Revision: revision, Cursor: cursor, Limit: limit})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.SnapshotPage{}, err
	}
	return response.Page, nil
}

func (c *Client) ChangesSince(ctx context.Context, tenant catalog.TenantID, scope catalog.EnumerationScope, cursor catalog.ChangeCursor, limit int) (catalog.ChangePage, error) {
	header, err := c.header()
	if err != nil {
		return catalog.ChangePage{}, err
	}
	response, err := call[changesSinceResponse](ctx, c, OperationChangesSince, changesSinceRequest{Header: header, Tenant: tenant, Scope: scope, Cursor: cursor, Limit: limit})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.ChangePage{}, err
	}
	return response.Page, nil
}

func (c *Client) ClaimMutation(ctx context.Context, id catalog.MutationID, owner catalog.MutationOwnerID) (catalog.PreparedMutation, error) {
	header, err := c.header()
	if err != nil {
		return catalog.PreparedMutation{}, err
	}
	response, err := call[claimMutationResponse](ctx, c, OperationClaimMutation, claimMutationRequest{Header: header, ID: id, Owner: owner})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.PreparedMutation{}, err
	}
	return response.Mutation, nil
}

func (c *Client) PrepareMutationSource(ctx context.Context, id catalog.MutationID, claim catalog.MutationClaim) (catalog.PreparedMutation, error) {
	header, err := c.header()
	if err != nil {
		return catalog.PreparedMutation{}, err
	}
	response, err := call[prepareMutationSourceResponse](ctx, c, OperationPrepareMutationSource, prepareMutationSourceRequest{Header: header, ID: id, Claim: claim})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.PreparedMutation{}, err
	}
	return response.Mutation, nil
}

func (c *Client) SetMutationSourceResult(ctx context.Context, id catalog.MutationID, claim catalog.MutationClaim, locator catalog.SourceLocator) (catalog.PreparedMutation, error) {
	header, err := c.header()
	if err != nil {
		return catalog.PreparedMutation{}, err
	}
	response, err := call[setMutationSourceResultResponse](ctx, c, OperationSetMutationSourceResult, setMutationSourceResultRequest{Header: header, ID: id, Claim: claim, Locator: locator})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.PreparedMutation{}, err
	}
	return response.Mutation, nil
}

func (c *Client) ReclaimMutation(ctx context.Context, id catalog.MutationID, stale catalog.MutationClaim, owner catalog.MutationOwnerID) (catalog.PreparedMutation, error) {
	header, err := c.header()
	if err != nil {
		return catalog.PreparedMutation{}, err
	}
	response, err := call[reclaimMutationResponse](ctx, c, OperationReclaimMutation, reclaimMutationRequest{Header: header, ID: id, Stale: stale, Owner: owner})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.PreparedMutation{}, err
	}
	return response.Mutation, nil
}

// LoadTenantState returns one CAS-protected tenant state record.
func (c *Client) LoadTenantState(ctx context.Context, tenant catalog.TenantID) (catalog.TenantStateRecord, error) {
	header, err := c.header()
	if err != nil {
		return catalog.TenantStateRecord{}, err
	}
	response, err := call[loadTenantStateResponse](ctx, c, OperationLoadTenantState, loadTenantStateRequest{Header: header, Tenant: tenant})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.TenantStateRecord{}, err
	}
	return response.State, nil
}

// ProvisionTenant atomically creates or exactly replays one desired definition.
func (c *Client) ProvisionTenant(ctx context.Context, provision catalog.TenantProvision) (catalog.TenantProvision, error) {
	header, err := c.header()
	if err != nil {
		return catalog.TenantProvision{}, err
	}
	response, err := call[provisionTenantResponse](ctx, c, OperationProvisionTenant, provisionTenantRequest{Header: header, Provision: provision})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.TenantProvision{}, err
	}
	return response.Provision, nil
}

// ReplaceTenantProvision atomically advances or exactly replays one generation.
func (c *Client) ReplaceTenantProvision(ctx context.Context, expected catalog.Generation, next catalog.TenantProvision) (catalog.TenantProvision, error) {
	header, err := c.header()
	if err != nil {
		return catalog.TenantProvision{}, err
	}
	response, err := call[replaceTenantProvisionResponse](ctx, c, OperationReplaceTenantProvision, replaceTenantProvisionRequest{
		Header: header, Expected: expected, Next: next,
	})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.TenantProvision{}, err
	}
	return response.Provision, nil
}

// RemoveTenantProvision durably removes or exactly replays one generation.
func (c *Client) RemoveTenantProvision(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[removeTenantProvisionResponse](ctx, c, OperationRemoveTenantProvision, removeTenantProvisionRequest{
		Header: header, Tenant: tenant, Generation: generation,
	})
	return validateResponse(header, response.Header, err)
}

// SaveTenantState persists or exactly replays one CAS state transition.
func (c *Client) SaveTenantState(ctx context.Context, expected catalog.StateVersion, state catalog.TenantStateRecord) (catalog.TenantStateRecord, error) {
	header, err := c.header()
	if err != nil {
		return catalog.TenantStateRecord{}, err
	}
	response, err := call[saveTenantStateResponse](ctx, c, OperationSaveTenantState, saveTenantStateRequest{
		Header: header, Expected: expected, State: state,
	})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.TenantStateRecord{}, err
	}
	return response.State, nil
}

func (c *Client) BeginFileProviderDomainRemoval(
	ctx context.Context,
	owner string,
	tenant catalog.TenantID,
	generation catalog.Generation,
) (catalog.FileProviderDomainRemoval, error) {
	header, err := c.header()
	if err != nil {
		return catalog.FileProviderDomainRemoval{}, err
	}
	response, err := call[beginFileProviderDomainRemovalResponse](
		ctx, c, OperationBeginFileProviderDomainRemoval,
		beginFileProviderDomainRemovalRequest{Header: header, Owner: owner, Tenant: tenant, Generation: generation},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.FileProviderDomainRemoval{}, err
	}
	return response.Removal, nil
}

func (c *Client) FileProviderDomainRemovalState(
	ctx context.Context,
	owner string,
	tenant catalog.TenantID,
	generation catalog.Generation,
) (catalog.FileProviderDomainRemoval, error) {
	header, err := c.header()
	if err != nil {
		return catalog.FileProviderDomainRemoval{}, err
	}
	response, err := call[fileProviderDomainRemovalStateResponse](
		ctx, c, OperationFileProviderDomainRemovalState,
		fileProviderDomainRemovalStateRequest{Header: header, Owner: owner, Tenant: tenant, Generation: generation},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.FileProviderDomainRemoval{}, err
	}
	return response.Removal, nil
}

func (c *Client) ConfirmFileProviderDomainRemoval(ctx context.Context, removal catalog.FileProviderDomainRemoval) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[confirmFileProviderDomainRemovalResponse](
		ctx, c, OperationConfirmFileProviderDomainRemoval,
		confirmFileProviderDomainRemovalRequest{Header: header, Removal: removal},
	)
	return validateResponse(header, response.Header, err)
}

func (c *Client) ConfirmFileProviderDomain(ctx context.Context, domain catalog.FileProviderDomain) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[confirmFileProviderDomainResponse](
		ctx, c, OperationConfirmFileProviderDomain,
		confirmFileProviderDomainRequest{Header: header, Domain: domain},
	)
	return validateResponse(header, response.Header, err)
}

func (c *Client) ConfirmFileProviderDomainAbsent(ctx context.Context, domain causal.DomainID) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[confirmFileProviderDomainAbsentResponse](
		ctx, c, OperationConfirmFileProviderDomainAbsent,
		confirmFileProviderDomainAbsentRequest{Header: header, Domain: domain},
	)
	return validateResponse(header, response.Header, err)
}

func (c *Client) NextBrokerCommandID(ctx context.Context) (uint64, error) {
	header, err := c.header()
	if err != nil {
		return 0, err
	}
	response, err := call[nextBrokerCommandIDResponse](
		ctx, c, OperationNextBrokerCommandID, nextBrokerCommandIDRequest{Header: header},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return 0, err
	}
	return response.ID, nil
}

func (c *Client) BeginBrokerCommandAttempt(ctx context.Context, attempt catalog.BrokerCommandAttempt) (catalog.BrokerCommandAttempt, bool, error) {
	header, err := c.header()
	if err != nil {
		return catalog.BrokerCommandAttempt{}, false, err
	}
	response, err := call[beginBrokerCommandAttemptResponse](
		ctx, c, OperationBeginBrokerCommandAttempt,
		beginBrokerCommandAttemptRequest{Header: header, Attempt: attempt},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.BrokerCommandAttempt{}, false, err
	}
	return response.Attempt, response.Created, nil
}

func (c *Client) TransitionBrokerCommandAttempt(
	ctx context.Context,
	attempt catalog.BrokerCommandAttempt,
	next catalog.BrokerCommandAttemptState,
) (catalog.BrokerCommandAttempt, error) {
	header, err := c.header()
	if err != nil {
		return catalog.BrokerCommandAttempt{}, err
	}
	response, err := call[transitionBrokerCommandAttemptResponse](
		ctx, c, OperationTransitionBrokerCommandAttempt,
		transitionBrokerCommandAttemptRequest{Header: header, Attempt: attempt, Next: next},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.BrokerCommandAttempt{}, err
	}
	return response.Attempt, nil
}

func (c *Client) AbandonBrokerCommandAttempt(ctx context.Context, attempt catalog.BrokerCommandAttempt) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[abandonBrokerCommandAttemptResponse](
		ctx, c, OperationAbandonBrokerCommandAttempt,
		abandonBrokerCommandAttemptRequest{Header: header, Attempt: attempt},
	)
	return validateResponse(header, response.Header, err)
}

func (c *Client) RecoverBrokerCommandAttempts(ctx context.Context) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[recoverBrokerCommandAttemptsResponse](
		ctx, c, OperationRecoverBrokerCommandAttempts,
		recoverBrokerCommandAttemptsRequest{Header: header},
	)
	return validateResponse(header, response.Header, err)
}

// OpenMutationContent drains the verified staged bytes of one prepared mutation.
func (c *Client) OpenMutationContent(
	ctx context.Context, tenant catalog.TenantID, id catalog.MutationID,
) (contentstream.Source, error) {
	header, err := c.header()
	if err != nil {
		return nil, err
	}
	response, err := call[openMutationContentResponse](
		ctx, c, OperationOpenMutationContent,
		openMutationContentRequest{Header: header, Tenant: tenant, ID: id},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return nil, err
	}
	return c.contentReader(ctx, response.Token), nil
}

// OpenPrivateContent drains one live private file for its exact creator origin.
func (c *Client) OpenPrivateContent(
	ctx context.Context,
	tenant catalog.TenantID,
	generation catalog.Generation,
	id catalog.ObjectID,
	creator catalog.MutationID,
	origin catalog.CausalOrigin,
) (contentstream.Source, error) {
	header, err := c.header()
	if err != nil {
		return nil, err
	}
	response, err := call[openPrivateContentResponse](
		ctx, c, OperationOpenPrivateContent,
		openPrivateContentRequest{
			Header: header, Tenant: tenant, Generation: generation,
			ID: id, Creator: creator, Origin: origin,
		},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return nil, err
	}
	return c.contentReader(ctx, response.Token), nil
}

func (c *Client) header() (requestHeader, error) {
	var operation requestID
	if _, err := rand.Read(operation[:]); err != nil {
		return requestHeader{}, err
	}
	return requestHeader{Protocol: protocolVersion, OperationID: operation, Worker: c.identity}, nil
}

func call[Response any](ctx context.Context, client *Client, operation Operation, request any) (Response, error) {
	var response Response
	payload, err := json.Marshal(request)
	if err != nil {
		return response, err
	}
	if len(payload) > maxPayloadSize {
		return response, errors.New("catalog worker: request exceeds frame limit")
	}
	deadline, declared := client.deadlines[string(operation)]
	if !declared {
		return response, fmt.Errorf("catalog worker: operation %q has no client deadline", operation)
	}
	callCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	reply, err := client.wire.Call(callCtx, string(operation), payload)
	if err != nil {
		return response, &TransportError{Message: err.Error(), Cause: err}
	}
	if len(reply.Body) == 0 || len(reply.Body) > maxPayloadSize {
		return response, &TransportError{Message: "invalid response payload size"}
	}
	if err := decodePayload(reply.Body, &response); err != nil {
		return response, &TransportError{Message: err.Error(), Cause: err}
	}
	return response, nil
}

func validateResponse(request requestHeader, response responseHeader, callErr error) error {
	if callErr != nil {
		return callErr
	}
	if response.Protocol != protocolVersion || response.OperationID != request.OperationID {
		return &TransportError{Message: "response identity mismatch"}
	}
	return decodeRemoteError(response.Error)
}
