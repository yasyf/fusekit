package catalogworker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/transportproto"
)

// Client is the typed remote catalog surface for one exact worker generation.
type Client struct {
	wire     *wire.Client
	identity WorkerIdentity
	owns     bool
}

// TransportError means the exact worker generation did not deliver a valid
// response and must be poisoned before another storage call is admitted.
type TransportError struct {
	Message string
	Cause   error
}

func (e *TransportError) Error() string { return "catalog worker transport: " + e.Message }
func (e *TransportError) Unwrap() error { return e.Cause }

// NewClient opens one exact-build session to a catalog worker generation.
func NewClient(ctx context.Context, config wire.ClientConfig, identity WorkerIdentity) (*Client, error) {
	if err := identity.validate(); err != nil {
		return nil, err
	}
	if config.WireBuild != "" && config.WireBuild != transportproto.WireBuild {
		return nil, fmt.Errorf("catalog worker: build %q does not match %q", config.WireBuild, transportproto.WireBuild)
	}
	config.WireBuild = transportproto.WireBuild
	client, err := wire.NewClient(ctx, config)
	if err != nil {
		return nil, err
	}
	return &Client{wire: client, identity: identity, owns: true}, nil
}

// NewClientOn binds typed catalog calls to an existing worker session.
func NewClientOn(client *wire.Client, identity WorkerIdentity) (*Client, error) {
	if client == nil || client.PeerWireIdentity().WireBuild != transportproto.WireBuild {
		return nil, errors.New("catalog worker: exact transport session is required")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	return &Client{wire: client, identity: identity}, nil
}

// Close closes a session opened by NewClient.
func (c *Client) Close() error {
	if !c.owns {
		return nil
	}
	return c.wire.Close()
}

// Abort tears down a session opened by NewClient without a GoAway exchange.
func (c *Client) Abort(cause error) error {
	if !c.owns {
		return nil
	}
	return c.wire.Abort(cause)
}

// Head returns one tenant's current catalog revision.
func (c *Client) Head(ctx context.Context, tenant catalog.TenantID) (catalog.Revision, error) {
	header, err := c.header()
	if err != nil {
		return 0, err
	}
	response, err := call[headResponse](ctx, c.wire, OperationHead, headRequest{Header: header, Tenant: tenant})
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
	response, err := call[compactionFloorResponse](ctx, c.wire, OperationCompactionFloor, compactionFloorRequest{Header: header, Tenant: tenant})
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
	response, err := call[tenantResponse](ctx, c.wire, OperationTenant, tenantRequest{Header: header, Tenant: tenant})
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
	response, err := call[rootResponse](ctx, c.wire, OperationRoot, rootRequest{Header: header, Tenant: tenant})
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
	response, err := call[lookupResponse](ctx, c.wire, OperationLookup, lookupRequest{Header: header, Tenant: tenant, Presentation: presentation, ID: id})
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
	response, err := call[lookupNameResponse](ctx, c.wire, OperationLookupName, lookupNameRequest{Header: header, Tenant: tenant, Presentation: presentation, Parent: parent, Name: name})
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
	response, err := call[snapshotResponse](ctx, c.wire, OperationSnapshot, snapshotRequest{Header: header, Tenant: tenant, Scope: scope, Revision: revision, Cursor: cursor, Limit: limit})
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
	response, err := call[changesSinceResponse](ctx, c.wire, OperationChangesSince, changesSinceRequest{Header: header, Tenant: tenant, Scope: scope, Cursor: cursor, Limit: limit})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.ChangePage{}, err
	}
	return response.Page, nil
}

func (c *Client) HasMaterializationDemand(ctx context.Context, tenant catalog.TenantID) (bool, error) {
	header, err := c.header()
	if err != nil {
		return false, err
	}
	response, err := call[hasMaterializationDemandResponse](ctx, c.wire, OperationHasMaterializationDemand, hasMaterializationDemandRequest{Header: header, Tenant: tenant})
	if err := validateResponse(header, response.Header, err); err != nil {
		return false, err
	}
	return response.Demand, nil
}

func (c *Client) ClaimMutation(ctx context.Context, id catalog.MutationID, owner catalog.MutationOwnerID) (catalog.PreparedMutation, error) {
	header, err := c.header()
	if err != nil {
		return catalog.PreparedMutation{}, err
	}
	response, err := call[claimMutationResponse](ctx, c.wire, OperationClaimMutation, claimMutationRequest{Header: header, ID: id, Owner: owner})
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
	response, err := call[prepareMutationSourceResponse](ctx, c.wire, OperationPrepareMutationSource, prepareMutationSourceRequest{Header: header, ID: id, Claim: claim})
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
	response, err := call[setMutationSourceResultResponse](ctx, c.wire, OperationSetMutationSourceResult, setMutationSourceResultRequest{Header: header, ID: id, Claim: claim, Locator: locator})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.PreparedMutation{}, err
	}
	return response.Mutation, nil
}

func (c *Client) MarkMutationApplied(ctx context.Context, id catalog.MutationID, claim catalog.MutationClaim) (catalog.PreparedMutation, error) {
	header, err := c.header()
	if err != nil {
		return catalog.PreparedMutation{}, err
	}
	response, err := call[markMutationAppliedResponse](ctx, c.wire, OperationMarkMutationApplied, markMutationAppliedRequest{Header: header, ID: id, Claim: claim})
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
	response, err := call[reclaimMutationResponse](ctx, c.wire, OperationReclaimMutation, reclaimMutationRequest{Header: header, ID: id, Stale: stale, Owner: owner})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.PreparedMutation{}, err
	}
	return response.Mutation, nil
}

func (c *Client) CommitMutation(ctx context.Context, tenant catalog.TenantID, id catalog.MutationID) (catalog.NamespaceMutationResult, error) {
	header, err := c.header()
	if err != nil {
		return catalog.NamespaceMutationResult{}, err
	}
	response, err := call[commitMutationResponse](ctx, c.wire, OperationCommitMutation, commitMutationRequest{Header: header, Tenant: tenant, ID: id})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.NamespaceMutationResult{}, err
	}
	return response.Result, nil
}

// LoadTenantState returns one CAS-protected tenant state record.
func (c *Client) LoadTenantState(ctx context.Context, tenant catalog.TenantID) (catalog.TenantStateRecord, error) {
	header, err := c.header()
	if err != nil {
		return catalog.TenantStateRecord{}, err
	}
	response, err := call[loadTenantStateResponse](ctx, c.wire, OperationLoadTenantState, loadTenantStateRequest{Header: header, Tenant: tenant})
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
	response, err := call[provisionTenantResponse](ctx, c.wire, OperationProvisionTenant, provisionTenantRequest{Header: header, Provision: provision})
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
	response, err := call[replaceTenantProvisionResponse](ctx, c.wire, OperationReplaceTenantProvision, replaceTenantProvisionRequest{
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
	response, err := call[removeTenantProvisionResponse](ctx, c.wire, OperationRemoveTenantProvision, removeTenantProvisionRequest{
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
	response, err := call[saveTenantStateResponse](ctx, c.wire, OperationSaveTenantState, saveTenantStateRequest{
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
		ctx, c.wire, OperationBeginFileProviderDomainRemoval,
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
		ctx, c.wire, OperationFileProviderDomainRemovalState,
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
		ctx, c.wire, OperationConfirmFileProviderDomainRemoval,
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
		ctx, c.wire, OperationConfirmFileProviderDomain,
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
		ctx, c.wire, OperationConfirmFileProviderDomainAbsent,
		confirmFileProviderDomainAbsentRequest{Header: header, Domain: domain},
	)
	return validateResponse(header, response.Header, err)
}

func (c *Client) FileProviderSignalPlan(
	ctx context.Context,
	tenant catalog.TenantID,
	domain causal.DomainID,
	generation catalog.Generation,
	revision catalog.Revision,
) (catalog.FileProviderSignalPlan, error) {
	header, err := c.header()
	if err != nil {
		return catalog.FileProviderSignalPlan{}, err
	}
	response, err := call[fileProviderSignalPlanResponse](
		ctx, c.wire, OperationFileProviderSignalPlan,
		fileProviderSignalPlanRequest{
			Header: header, Tenant: tenant, Domain: domain, Generation: generation, Revision: revision,
		},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.FileProviderSignalPlan{}, err
	}
	return response.Plan, nil
}

func (c *Client) NextBrokerCommandID(ctx context.Context) (uint64, error) {
	header, err := c.header()
	if err != nil {
		return 0, err
	}
	response, err := call[nextBrokerCommandIDResponse](
		ctx, c.wire, OperationNextBrokerCommandID, nextBrokerCommandIDRequest{Header: header},
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
		ctx, c.wire, OperationBeginBrokerCommandAttempt,
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
		ctx, c.wire, OperationTransitionBrokerCommandAttempt,
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
		ctx, c.wire, OperationAbandonBrokerCommandAttempt,
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
		ctx, c.wire, OperationRecoverBrokerCommandAttempts,
		recoverBrokerCommandAttemptsRequest{Header: header},
	)
	return validateResponse(header, response.Header, err)
}

// OpenMutationContent opens one backpressured immutable content stream.
func (c *Client) OpenMutationContent(ctx context.Context, tenant catalog.TenantID, id catalog.MutationID) (contentstream.Source, error) {
	header, err := c.header()
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(openMutationContentRequest{Header: header, Tenant: tenant, ID: id})
	if err != nil {
		return nil, err
	}
	call, err := c.wire.Open(ctx, wire.Op(OperationOpenMutationContent), "", payload, true)
	if err != nil {
		return nil, &TransportError{Message: err.Error(), Cause: err}
	}
	streamCtx, cancel := context.WithCancel(ctx)
	return &contentReader{
		ctx: streamCtx, cancel: cancel, call: call, chunks: call.Chunks(), request: header,
		done: make(chan struct{}),
	}, nil
}

type contentReader struct {
	ctx     context.Context
	cancel  context.CancelFunc
	call    *wire.ClientCall
	chunks  <-chan wire.Chunk
	request requestHeader

	readMu   sync.Mutex
	mu       sync.Mutex
	current  []byte
	ended    bool
	settled  bool
	err      error
	done     chan struct{}
	doneOnce sync.Once
}

func (r *contentReader) Read(buffer []byte) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if len(buffer) == 0 {
		return 0, nil
	}
	for {
		r.mu.Lock()
		if len(r.current) > 0 {
			count := copy(buffer, r.current)
			r.current = r.current[count:]
			r.mu.Unlock()
			return count, nil
		}
		if r.settled {
			err := r.err
			r.mu.Unlock()
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		ended := r.ended
		r.mu.Unlock()
		if ended {
			r.settle()
			continue
		}
		select {
		case <-r.ctx.Done():
			r.abort(r.ctx.Err())
		case chunk, ok := <-r.chunks:
			if !ok {
				r.abort(errors.New("catalog worker: content stream ended without terminal chunk"))
				continue
			}
			if len(chunk.Payload) > streamChunkSize ||
				(len(chunk.Payload) == 0 && !chunk.End) {
				r.abort(errors.New("catalog worker: invalid content stream chunk"))
				continue
			}
			r.mu.Lock()
			r.current = append(r.current[:0], chunk.Payload...)
			r.ended = chunk.End
			r.mu.Unlock()
		}
	}
}

func (r *contentReader) Settle(result error) error {
	if result != nil {
		r.abort(result)
	} else {
		r.mu.Lock()
		settled := r.settled
		r.mu.Unlock()
		if !settled {
			r.abort(errors.New("catalog worker: content stream settled before terminal response"))
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *contentReader) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.err
	case <-ctx.Done():
		r.abort(ctx.Err())
		<-r.done
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.err
	}
}

func (r *contentReader) settle() {
	result, err := r.call.Response(r.ctx)
	var response openMutationContentResponse
	if err == nil {
		if result.Outcome != wire.Delivered || result.Response.Rejected || result.Response.Err != "" {
			err = &TransportError{Message: "stream terminal was not delivered"}
		} else {
			err = decodePayload(result.Response.Payload, &response)
			if err != nil {
				err = &TransportError{Message: err.Error(), Cause: err}
			} else {
				err = validateResponse(r.request, response.Header, nil)
			}
		}
	}
	r.mu.Lock()
	if !r.settled {
		r.err = err
		r.settled = true
	}
	r.mu.Unlock()
	r.cancel()
	r.doneOnce.Do(func() { close(r.done) })
}

func (r *contentReader) abort(err error) {
	r.mu.Lock()
	if r.settled {
		r.mu.Unlock()
		return
	}
	r.err = err
	r.settled = true
	r.current = nil
	r.mu.Unlock()
	r.cancel()
	r.call.Cancel()
	go r.joinCanceled()
}

func (r *contentReader) joinCanceled() {
	settleCtx, settleCancel := context.WithTimeout(context.Background(), defaultStopTimeout)
	_, settleErr := r.call.Response(settleCtx)
	settleCancel()
	r.mu.Lock()
	r.err = errors.Join(r.err, settleErr)
	r.mu.Unlock()
	r.doneOnce.Do(func() { close(r.done) })
}

func (c *Client) header() (requestHeader, error) {
	var operation requestID
	if _, err := rand.Read(operation[:]); err != nil {
		return requestHeader{}, err
	}
	return requestHeader{Protocol: protocolVersion, OperationID: operation, Worker: c.identity}, nil
}

func call[Response any](ctx context.Context, client *wire.Client, operation Operation, request any) (Response, error) {
	var response Response
	payload, err := json.Marshal(request)
	if err != nil {
		return response, err
	}
	if len(payload) > maxFrameSize {
		return response, errors.New("catalog worker: request exceeds frame limit")
	}
	result, err := client.Call(ctx, wire.Op(operation), "", payload)
	if err != nil {
		return response, &TransportError{Message: err.Error(), Cause: err}
	}
	if result.Outcome != wire.Delivered || result.Response.Rejected || result.Response.Err != "" {
		message := result.Response.Reason
		if message == "" {
			message = result.Response.Err
		}
		if message == "" {
			message = "request was not delivered"
		}
		return response, &TransportError{Message: message}
	}
	if len(result.Response.Payload) == 0 || len(result.Response.Payload) > maxFrameSize {
		return response, &TransportError{Message: "invalid response payload size"}
	}
	if err := decodePayload(result.Response.Payload, &response); err != nil {
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
