package catalogservice

import (
	"bytes"
	"context"
	"errors"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
)

func (s *Server) handleBeginMaterializationSnapshot(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.BeginMaterializationSnapshotRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.BeginMaterializationSnapshotResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest,
			Message: boundedErrorMessage(err.Error()),
		})
	}
	tenant, _, err := s.authorizeMaterializationRoute(ctx, request, catalogproto.OperationMaterializationSnapshotBegin,
		input.TenantID, input.DomainID, input.Generation)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.BeginMaterializationSnapshotResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	identity, err := catalogMaterializationIdentity(tenant, input.DomainID, input.Generation,
		input.SnapshotID, input.BackingStoreIdentity)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.BeginMaterializationSnapshotResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	epoch, err := s.fileProvider.Materialization.BeginFileProviderMaterializationSnapshot(ctx, identity)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.BeginMaterializationSnapshotResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.BeginMaterializationSnapshotResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Epoch: epoch,
	})
}

func (s *Server) handleSuspendMaterializationSnapshot(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.SuspendMaterializationSnapshotRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.SuspendMaterializationSnapshotResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest,
			Message: boundedErrorMessage(err.Error()),
		})
	}
	tenant, _, err := s.authorizeMaterializationRoute(ctx, request, catalogproto.OperationMaterializationSnapshotSuspend,
		input.TenantID, input.DomainID, input.Generation)
	if err == nil {
		err = s.fileProvider.Materialization.SuspendFileProviderMaterialization(
			ctx, tenant, causal.DomainID(input.DomainID), catalog.Generation(input.Generation),
		)
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.SuspendMaterializationSnapshotResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.SuspendMaterializationSnapshotResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk})
}

func (s *Server) handleStageMaterializationSnapshotPage(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.StageMaterializationSnapshotPageRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.StageMaterializationSnapshotPageResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest,
			Message: boundedErrorMessage(err.Error()),
		})
	}
	tenant, _, err := s.authorizeMaterializationRoute(ctx, request, catalogproto.OperationMaterializationSnapshotStagePage,
		input.TenantID, input.DomainID, input.Generation)
	var identity catalog.FileProviderMaterializationIdentity
	if err == nil {
		identity, err = catalogMaterializationIdentity(tenant, input.DomainID, input.Generation,
			input.SnapshotID, input.BackingStoreIdentity)
	}
	ids := make([]catalog.ObjectID, len(input.ContainerIDs))
	if err == nil {
		for index, encodedID := range input.ContainerIDs {
			ids[index], err = catalog.ParseObjectID(string(encodedID))
			if err != nil {
				break
			}
		}
	}
	if err == nil {
		err = s.fileProvider.Materialization.StageFileProviderMaterializationPage(ctx, catalog.FileProviderMaterializationPage{
			Identity: identity, Sequence: input.Sequence, IDs: ids,
		})
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.StageMaterializationSnapshotPageResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.StageMaterializationSnapshotPageResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk})
}

func (s *Server) handleCommitMaterializationSnapshot(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.CommitMaterializationSnapshotRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.CommitMaterializationSnapshotResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest,
			Message: boundedErrorMessage(err.Error()),
		})
	}
	tenant, _, err := s.authorizeMaterializationRoute(ctx, request, catalogproto.OperationMaterializationSnapshotCommit,
		input.TenantID, input.DomainID, input.Generation)
	var identity catalog.FileProviderMaterializationIdentity
	if err == nil {
		identity, err = catalogMaterializationIdentity(tenant, input.DomainID, input.Generation,
			input.SnapshotID, input.BackingStoreIdentity)
	}
	var result catalog.FileProviderMaterializationResult
	if err == nil {
		result, err = s.fileProvider.Materialization.CommitFileProviderMaterializationSnapshot(ctx,
			catalog.FileProviderMaterializationCommit{Identity: identity, PageCount: input.PageCount})
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.CommitMaterializationSnapshotResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.CommitMaterializationSnapshotResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		Revision: result.Revision, Added: result.Added, Removed: result.Removed,
	})
}

func (s *Server) authorizeMaterializationRoute(
	ctx context.Context,
	request wire.Request,
	operation catalogproto.Operation,
	payloadTenant catalogproto.TenantID,
	payloadDomain catalogproto.DomainID,
	payloadGeneration uint64,
) (catalog.TenantID, Authorization, error) {
	tenant, authorization, _, err := s.authorize(ctx, request, operation, catalog.Generation(payloadGeneration), true)
	if err != nil {
		return "", Authorization{}, err
	}
	if tenant != catalog.TenantID(payloadTenant) || authorization.Route.Domain != payloadDomain ||
		authorization.Route.Generation != catalog.Generation(payloadGeneration) || !authorization.Route.Forwarded {
		return "", Authorization{}, errors.New("catalog service: materialization payload does not match broker binding")
	}
	return tenant, authorization, nil
}

func catalogMaterializationIdentity(
	tenant catalog.TenantID,
	domain catalogproto.DomainID,
	generation uint64,
	snapshot catalogproto.MaterializationSnapshotID,
	backingStoreIdentity []byte,
) (catalog.FileProviderMaterializationIdentity, error) {
	parsed, err := catalog.ParseMaterializationSnapshotID(string(snapshot))
	if err != nil {
		return catalog.FileProviderMaterializationIdentity{}, err
	}
	return catalog.FileProviderMaterializationIdentity{
		Tenant: tenant, Domain: causal.DomainID(domain), Generation: catalog.Generation(generation),
		Snapshot: parsed, BackingStoreIdentity: bytes.Clone(backingStoreIdentity),
	}, nil
}
