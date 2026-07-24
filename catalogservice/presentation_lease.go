package catalogservice

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
)

func (s *Server) handleCommitFileProviderLease(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.CommitFileProviderLeaseRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.CommitFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, _, _, err := s.authorize(ctx, request, catalogproto.OperationPresentationLeaseCommit, catalog.Generation(input.Lease.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.CommitFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	lease, err := catalogFileProviderLease(tenant, input.Lease)
	if err == nil {
		lease, err = s.core.Leases.CommitFileProviderLease(ctx, lease)
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.CommitFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	receipt, err := protocolFileProviderLease(lease)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.CommitFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.CommitFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Lease: &receipt})
}

func (s *Server) handleRenewFileProviderLease(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.RenewFileProviderLeaseRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.RenewFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, _, _, err := s.authorize(ctx, request, catalogproto.OperationPresentationLeaseRenew, catalog.Generation(input.Lease.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.RenewFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	lease, err := catalogFileProviderLease(tenant, input.Lease)
	if err == nil {
		lease, err = s.core.Leases.RenewFileProviderLease(ctx, lease)
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.RenewFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	receipt, err := protocolFileProviderLease(lease)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.RenewFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.RenewFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Lease: &receipt})
}

func (s *Server) handleReleaseFileProviderLease(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.ReleaseFileProviderLeaseRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.ReleaseFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, _, _, err := s.authorize(ctx, request, catalogproto.OperationPresentationLeaseRelease, catalog.Generation(input.Lease.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ReleaseFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	lease, err := catalogFileProviderLease(tenant, input.Lease)
	if err == nil {
		lease, err = s.core.Leases.ReleaseFileProviderLease(ctx, lease)
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ReleaseFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	receipt, err := protocolFileProviderLease(lease)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ReleaseFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.ReleaseFileProviderLeaseResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Lease: &receipt})
}

func catalogFileProviderLease(routeTenant catalog.TenantID, receipt catalogproto.FileProviderLeaseReceipt) (catalog.FileProviderLease, error) {
	if err := catalogproto.Validate(receipt); err != nil {
		return catalog.FileProviderLease{}, err
	}
	tenant, err := catalog.NewTenantID(string(receipt.TenantID))
	if err != nil {
		return catalog.FileProviderLease{}, err
	}
	if tenant != routeTenant {
		return catalog.FileProviderLease{}, fmt.Errorf("%w: File Provider lease tenant does not match route", catalog.ErrGenerationMismatch)
	}
	root, err := catalog.ParseObjectID(string(receipt.RootID))
	if err != nil {
		return catalog.FileProviderLease{}, err
	}
	policy, err := decodeDigest(receipt.PolicyDigest)
	if err != nil {
		return catalog.FileProviderLease{}, err
	}
	resolution, err := decodeDigest(receipt.ResolutionDigest)
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
		ID: receipt.LeaseID, Tenant: tenant, DomainID: causal.DomainID(receipt.DomainID),
		Generation: catalog.Generation(receipt.Generation), Root: root,
		PresentationInstance: string(receipt.PresentationInstanceID), State: state,
		SessionID: receipt.SessionID, ProcessIdentity: receipt.ProcessIdentity,
		PolicyDigest: policy, ResolutionDigest: resolution, CatalogHead: catalog.Revision(receipt.CatalogHead),
		SourceAuthority: causal.SourceAuthorityID(receipt.SourceAuthority), SourcePublication: publication,
		SourceRevision: causal.Revision(receipt.SourceRevision), ActivationGeneration: receipt.ActivationGeneration,
		ExpiresAt: time.Unix(0, int64(receipt.ExpiresUnixNano)).UTC(),
	}, nil
}
