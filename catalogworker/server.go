package catalogworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
)

const maxFrameSize = 2 << 20
const streamChunkSize = 64 << 10
const workerWALTimeout = 2 * time.Second

type server struct {
	store    *catalog.Catalog
	identity WorkerIdentity
	mutation sync.Mutex
	writeMu  sync.Mutex

	maintenance *maintenanceScheduler

	handleMu     sync.Mutex
	handles      map[string]*snapshotHandleRecord
	closedHandle map[string]closedSnapshotHandle
	ownerHandles map[string]int
	writeRoot    string
}

type serverHandler = wire.Handler

func (s *server) mutationHandler(next serverHandler) serverHandler {
	return func(ctx context.Context, request wire.Request) (any, error) {
		s.mutation.Lock()
		defer s.mutation.Unlock()
		if err := s.enforceWAL(ctx); err != nil {
			return nil, fmt.Errorf("catalog worker: pre-mutation WAL recovery: %w", err)
		}
		response, callErr := next(ctx, request)
		if err := s.enforceWAL(context.WithoutCancel(ctx)); err != nil {
			return nil, errors.Join(callErr, fmt.Errorf("catalog worker: post-mutation WAL recovery: %w", err))
		}
		if s.maintenance != nil {
			s.maintenance.Wake()
		}
		return response, callErr
	}
}

func (s *server) enforceWAL(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, workerWALTimeout)
	defer cancel()
	return s.store.EnforceWorkerWALBudget(ctx)
}

func (s *server) handleCompactionFloor(ctx context.Context, request wire.Request) (any, error) {
	var input compactionFloorRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(compactionFloorResponse{Header: decodeError(err)})
	}
	response := compactionFloorResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Revision, response.Header.Error = valueResult(s.store.CompactionFloor(ctx, input.Tenant))
	}
	return encodeResponse(response)
}

func (s *server) handleTenant(ctx context.Context, request wire.Request) (any, error) {
	var input tenantRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(tenantResponse{Header: decodeError(err)})
	}
	response := tenantResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Metadata, response.Header.Error = valueResult(s.store.Tenant(ctx, input.Tenant))
	}
	return encodeResponse(response)
}

func (s *server) handleRoot(ctx context.Context, request wire.Request) (any, error) {
	var input rootRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(rootResponse{Header: decodeError(err)})
	}
	response := rootResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Object, response.Header.Error = valueResult(s.store.Root(ctx, input.Tenant))
	}
	return encodeResponse(response)
}

func (s *server) handleLookup(ctx context.Context, request wire.Request) (any, error) {
	var input lookupRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(lookupResponse{Header: decodeError(err)})
	}
	response := lookupResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Object, response.Header.Error = valueResult(s.store.Lookup(ctx, input.Tenant, input.Presentation, input.ID))
	}
	return encodeResponse(response)
}

func (s *server) handleLookupName(ctx context.Context, request wire.Request) (any, error) {
	var input lookupNameRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(lookupNameResponse{Header: decodeError(err)})
	}
	response := lookupNameResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Object, response.Header.Error = valueResult(s.store.LookupName(ctx, input.Tenant, input.Presentation, input.Parent, input.Name))
	}
	return encodeResponse(response)
}

func (s *server) handleSnapshot(ctx context.Context, request wire.Request) (any, error) {
	var input snapshotRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(snapshotResponse{Header: decodeError(err)})
	}
	response := snapshotResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Page, response.Header.Error = valueResult(s.store.Snapshot(ctx, input.Tenant, input.Scope, input.Revision, input.Cursor, input.Limit))
	}
	return encodeResponse(response)
}

func (s *server) handleChangesSince(ctx context.Context, request wire.Request) (any, error) {
	var input changesSinceRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(changesSinceResponse{Header: decodeError(err)})
	}
	response := changesSinceResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Page, response.Header.Error = valueResult(s.store.ChangesSince(ctx, input.Tenant, input.Scope, input.Cursor, input.Limit))
	}
	return encodeResponse(response)
}

func (s *server) handleHasMaterializationDemand(ctx context.Context, request wire.Request) (any, error) {
	var input hasMaterializationDemandRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(hasMaterializationDemandResponse{Header: decodeError(err)})
	}
	response := hasMaterializationDemandResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Demand, response.Header.Error = valueResult(s.store.HasMaterializationDemand(ctx, input.Tenant))
	}
	return encodeResponse(response)
}

func (s *server) handleOpenMutationContent(ctx context.Context, request wire.Request) (any, error) {
	var input openMutationContentRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return emptyContentStream(openMutationContentResponse{Header: decodeError(err)})
	}
	response := openMutationContentResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return emptyContentStream(response)
	}
	content, err := s.store.OpenMutationContent(ctx, input.Tenant, input.ID)
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return emptyContentStream(response)
	}
	chunks := make(chan []byte)
	terminal := new(json.RawMessage)
	go func() {
		defer close(chunks)
		var streamErr error
		defer func() {
			settleErr := content.Settle(streamErr)
			waitCtx, waitCancel := context.WithTimeout(context.WithoutCancel(ctx), workerWALTimeout)
			waitErr := content.Wait(waitCtx)
			waitCancel()
			response.Header.Error = encodeRemoteError(errors.Join(
				decodeRemoteError(response.Header.Error), settleErr, waitErr,
			))
			*terminal = mustResponse(response)
		}()
		buffer := make([]byte, streamChunkSize)
		for {
			count, readErr := content.Read(buffer)
			if count > 0 {
				chunk := append([]byte(nil), buffer[:count]...)
				select {
				case chunks <- chunk:
				case <-ctx.Done():
					streamErr = ctx.Err()
					response.Header.Error = encodeRemoteError(ctx.Err())
					return
				}
			}
			if errors.Is(readErr, io.EOF) {
				return
			}
			if readErr != nil || count == 0 {
				if readErr == nil {
					readErr = errors.New("catalog worker: mutation content reader made no progress")
				}
				streamErr = readErr
				response.Header.Error = encodeRemoteError(readErr)
				return
			}
		}
	}()
	return wire.StreamResponse{Chunks: chunks, Value: terminal}, nil
}

func emptyContentStream(response openMutationContentResponse) (any, error) {
	chunks := make(chan []byte)
	close(chunks)
	raw := mustResponse(response)
	return wire.StreamResponse{Chunks: chunks, Value: &raw}, nil
}

func mustResponse(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return json.RawMessage(encoded)
}

func (s *server) handleClaimMutation(ctx context.Context, request wire.Request) (any, error) {
	var input claimMutationRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(claimMutationResponse{Header: decodeError(err)})
	}
	response := claimMutationResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Mutation, response.Header.Error = valueResult(s.store.ClaimMutation(ctx, input.ID, input.Owner))
	}
	return encodeResponse(response)
}

func (s *server) handlePrepareMutationSource(ctx context.Context, request wire.Request) (any, error) {
	var input prepareMutationSourceRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(prepareMutationSourceResponse{Header: decodeError(err)})
	}
	response := prepareMutationSourceResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Mutation, response.Header.Error = valueResult(s.store.PrepareMutationSource(ctx, input.ID, input.Claim))
	}
	return encodeResponse(response)
}

func (s *server) handleSetMutationSourceResult(ctx context.Context, request wire.Request) (any, error) {
	var input setMutationSourceResultRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(setMutationSourceResultResponse{Header: decodeError(err)})
	}
	response := setMutationSourceResultResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Mutation, response.Header.Error = valueResult(s.store.SetMutationSourceResult(ctx, input.ID, input.Claim, input.Locator))
	}
	return encodeResponse(response)
}

func (s *server) handleReclaimMutation(ctx context.Context, request wire.Request) (any, error) {
	var input reclaimMutationRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(reclaimMutationResponse{Header: decodeError(err)})
	}
	response := reclaimMutationResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Mutation, response.Header.Error = valueResult(s.store.ReclaimMutation(ctx, input.ID, input.Stale, input.Owner))
	}
	return encodeResponse(response)
}

func decodeError(err error) responseHeader {
	return responseHeader{Protocol: protocolVersion, Error: encodeRemoteError(err)}
}

func newServer(
	ctx context.Context,
	store *catalog.Catalog,
	identity WorkerIdentity,
	database string,
) (*server, error) {
	if store == nil {
		return nil, errors.New("catalog worker: catalog is required")
	}
	if !filepath.IsAbs(database) {
		return nil, errors.New("catalog worker: absolute database path is required")
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	service := &server{
		store: store, identity: identity,
		handles:      make(map[string]*snapshotHandleRecord),
		closedHandle: make(map[string]closedSnapshotHandle),
		ownerHandles: make(map[string]int),
		writeRoot:    database + ".native-writes",
	}
	if err := service.recoverMutationPins(ctx); err != nil {
		return nil, fmt.Errorf("catalog worker: recover mutation pins: %w", err)
	}
	for {
		retirement, err := store.RetirePriorCatalogGenerations(ctx)
		if err != nil {
			return nil, fmt.Errorf("catalog worker: retire recovered catalog generations: %w", err)
		}
		if !retirement.More {
			break
		}
	}
	for {
		retirement, err := store.RetirePriorRetentionOwners(ctx)
		if err != nil {
			return nil, fmt.Errorf("catalog worker: retire recovered retention owners: %w", err)
		}
		if !retirement.More {
			break
		}
	}
	return service, nil
}

func (s *server) handleHead(ctx context.Context, request wire.Request) (any, error) {
	var input headRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(headResponse{Header: responseHeader{Protocol: protocolVersion, Error: encodeRemoteError(err)}})
	}
	response := headResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Revision, response.Header.Error = valueResult(s.store.Head(ctx, input.Tenant))
	}
	return encodeResponse(response)
}

func (s *server) handleLoadTenantState(ctx context.Context, request wire.Request) (any, error) {
	var input loadTenantStateRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(loadTenantStateResponse{Header: responseHeader{Protocol: protocolVersion, Error: encodeRemoteError(err)}})
	}
	response := loadTenantStateResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.State, response.Header.Error = valueResult(s.store.LoadTenantState(ctx, input.Tenant))
	}
	return encodeResponse(response)
}

func (s *server) handleProvisionTenant(ctx context.Context, request wire.Request) (any, error) {
	var input provisionTenantRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(provisionTenantResponse{Header: responseHeader{Protocol: protocolVersion, Error: encodeRemoteError(err)}})
	}
	response := provisionTenantResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Provision, response.Header.Error = valueResult(s.store.ProvisionTenant(ctx, input.Provision))
	}
	return encodeResponse(response)
}

func (s *server) handleReplaceTenantProvision(ctx context.Context, request wire.Request) (any, error) {
	var input replaceTenantProvisionRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(replaceTenantProvisionResponse{Header: responseHeader{Protocol: protocolVersion, Error: encodeRemoteError(err)}})
	}
	response := replaceTenantProvisionResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Provision, response.Header.Error = valueResult(s.store.ReplaceTenantProvision(ctx, input.Expected, input.Next))
	}
	return encodeResponse(response)
}

func (s *server) handleSaveTenantState(ctx context.Context, request wire.Request) (any, error) {
	var input saveTenantStateRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(saveTenantStateResponse{Header: responseHeader{Protocol: protocolVersion, Error: encodeRemoteError(err)}})
	}
	response := saveTenantStateResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.State, response.Header.Error = valueResult(s.store.SaveTenantState(ctx, input.Expected, input.State))
	}
	return encodeResponse(response)
}

func (s *server) handleRemoveTenantProvision(ctx context.Context, request wire.Request) (any, error) {
	var input removeTenantProvisionRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(removeTenantProvisionResponse{Header: responseHeader{Protocol: protocolVersion, Error: encodeRemoteError(err)}})
	}
	response := removeTenantProvisionResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Header.Error = encodeRemoteError(s.store.RemoveTenantProvision(ctx, input.Tenant, input.Generation))
	}
	return encodeResponse(response)
}

func (s *server) handleBeginFileProviderDomainRemoval(ctx context.Context, request wire.Request) (any, error) {
	var input beginFileProviderDomainRemovalRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(beginFileProviderDomainRemovalResponse{Header: decodeError(err)})
	}
	response := beginFileProviderDomainRemovalResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Removal, response.Header.Error = valueResult(
			s.store.BeginFileProviderDomainRemoval(ctx, input.Owner, input.Tenant, input.Generation),
		)
	}
	return encodeResponse(response)
}

func (s *server) handleFileProviderDomainRemovalState(ctx context.Context, request wire.Request) (any, error) {
	var input fileProviderDomainRemovalStateRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(fileProviderDomainRemovalStateResponse{Header: decodeError(err)})
	}
	response := fileProviderDomainRemovalStateResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Removal, response.Header.Error = valueResult(
			s.store.FileProviderDomainRemovalState(ctx, input.Owner, input.Tenant, input.Generation),
		)
	}
	return encodeResponse(response)
}

func (s *server) handleConfirmFileProviderDomainRemoval(ctx context.Context, request wire.Request) (any, error) {
	var input confirmFileProviderDomainRemovalRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(confirmFileProviderDomainRemovalResponse{Header: decodeError(err)})
	}
	response := confirmFileProviderDomainRemovalResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Header.Error = encodeRemoteError(s.store.ConfirmFileProviderDomainRemoval(ctx, input.Removal))
	}
	return encodeResponse(response)
}

func (s *server) handleConfirmFileProviderDomain(ctx context.Context, request wire.Request) (any, error) {
	var input confirmFileProviderDomainRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(confirmFileProviderDomainResponse{Header: decodeError(err)})
	}
	response := confirmFileProviderDomainResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Header.Error = encodeRemoteError(s.store.ConfirmFileProviderDomain(ctx, input.Domain))
	}
	return encodeResponse(response)
}

func (s *server) handleConfirmFileProviderDomainAbsent(ctx context.Context, request wire.Request) (any, error) {
	var input confirmFileProviderDomainAbsentRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(confirmFileProviderDomainAbsentResponse{Header: decodeError(err)})
	}
	response := confirmFileProviderDomainAbsentResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Header.Error = encodeRemoteError(s.store.ConfirmFileProviderDomainAbsent(ctx, input.Domain))
	}
	return encodeResponse(response)
}

func (s *server) handleNextBrokerCommandID(ctx context.Context, request wire.Request) (any, error) {
	var input nextBrokerCommandIDRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(nextBrokerCommandIDResponse{Header: decodeError(err)})
	}
	response := nextBrokerCommandIDResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.ID, response.Header.Error = valueResult(s.store.NextBrokerCommandID(ctx))
	}
	return encodeResponse(response)
}

func (s *server) handleBeginBrokerCommandAttempt(ctx context.Context, request wire.Request) (any, error) {
	var input beginBrokerCommandAttemptRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(beginBrokerCommandAttemptResponse{Header: decodeError(err)})
	}
	response := beginBrokerCommandAttemptResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Attempt, response.Created, response.Header.Error = brokerAttemptResult(
			s.store.BeginBrokerCommandAttempt(ctx, input.Attempt),
		)
	}
	return encodeResponse(response)
}

func (s *server) handleTransitionBrokerCommandAttempt(ctx context.Context, request wire.Request) (any, error) {
	var input transitionBrokerCommandAttemptRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(transitionBrokerCommandAttemptResponse{Header: decodeError(err)})
	}
	response := transitionBrokerCommandAttemptResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Attempt, response.Header.Error = valueResult(
			s.store.TransitionBrokerCommandAttempt(ctx, input.Attempt, input.Next),
		)
	}
	return encodeResponse(response)
}

func (s *server) handleAbandonBrokerCommandAttempt(ctx context.Context, request wire.Request) (any, error) {
	var input abandonBrokerCommandAttemptRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(abandonBrokerCommandAttemptResponse{Header: decodeError(err)})
	}
	response := abandonBrokerCommandAttemptResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Header.Error = encodeRemoteError(s.store.AbandonBrokerCommandAttempt(ctx, input.Attempt))
	}
	return encodeResponse(response)
}

func (s *server) handleRecoverBrokerCommandAttempts(ctx context.Context, request wire.Request) (any, error) {
	var input recoverBrokerCommandAttemptsRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(recoverBrokerCommandAttemptsResponse{Header: decodeError(err)})
	}
	response := recoverBrokerCommandAttemptsResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		response.Header.Error = encodeRemoteError(s.store.RecoverBrokerCommandAttempts(ctx))
	}
	return encodeResponse(response)
}

func brokerAttemptResult(value catalog.BrokerCommandAttempt, created bool, err error) (catalog.BrokerCommandAttempt, bool, *remoteError) {
	return value, created, encodeRemoteError(err)
}

func (s *server) response(header requestHeader) responseHeader {
	return responseHeader{
		Protocol: protocolVersion, OperationID: header.OperationID,
		Error: encodeRemoteError(header.validate(s.identity)),
	}
}

func valueResult[T any](value T, err error) (T, *remoteError) {
	return value, encodeRemoteError(err)
}

func decodePayload(payload []byte, value any) error {
	if len(payload) == 0 || len(payload) > maxFrameSize {
		return errors.New("catalog worker: invalid request payload size")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("catalog worker: decode request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("catalog worker: request contains trailing data")
	}
	return nil
}

func encodeResponse(value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("catalog worker: encode response: %w", err)
	}
	if len(encoded) > maxFrameSize {
		return nil, errors.New("catalog worker: response exceeds frame limit")
	}
	return json.RawMessage(encoded), nil
}
