package mountservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

// NativeConfig supplies the optional authenticated native presentation surface.
type NativeConfig struct {
	Sessions NativeSessions
	Catalog  NativeCatalog
	// ProtectedPeer verifies the exact signed native child peer.
	ProtectedPeer func(context.Context, wire.Peer) error
}

// Config supplies the tenant runtime and authenticated owner policy.
type Config struct {
	Runtime    Runtime
	Authorizer Authorizer
	Native     *NativeConfig
}

// Server binds tenant lifecycle exclusively to persistent daemonkit sessions.
type Server struct {
	config Config
	native nativeSessionRegistry
}

// Register installs the exact tenant lifecycle protocol on a daemonkit server.
func Register(server *wire.Server, config Config) (*Server, error) {
	if server == nil {
		return nil, errors.New("mount service: daemonkit server is nil")
	}
	if server.WireBuild != transportproto.WireBuild {
		return nil, fmt.Errorf("mount service: daemonkit build %q does not match transport suite %q", server.WireBuild, transportproto.WireBuild)
	}
	if config.Runtime == nil || config.Authorizer == nil {
		return nil, errors.New("mount service: runtime and authorizer are required")
	}
	if config.Native != nil &&
		(config.Native.Sessions == nil || config.Native.Catalog == nil || config.Native.ProtectedPeer == nil) {
		return nil, errors.New("mount service: native sessions, catalog, and protected peer verifier are required together")
	}
	service := &Server{config: config}
	for _, handler := range []wire.HandlerSpec{
		{Op: wire.Op(mountproto.OperationTenantProvision), Handler: service.handleProvision, Concurrent: true},
		{Op: wire.Op(mountproto.OperationTenantReplace), Handler: service.handleReplace, Concurrent: true},
		{Op: wire.Op(mountproto.OperationTenantRemove), Handler: service.handleRemove, Concurrent: true},
		{Op: wire.Op(mountproto.OperationTenantState), Handler: service.handleState, Concurrent: true},
	} {
		server.Register(handler)
	}
	if config.Native != nil {
		for _, handler := range []wire.HandlerSpec{
			{Op: wire.Op(mountproto.OperationNativeBind), Handler: service.handleNativeBind},
			{Op: wire.Op(mountproto.OperationNativeMounted), Handler: service.handleNativeMounted, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeReady), Handler: service.handleNativeReady, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeUnbind), Handler: service.handleNativeUnbind},
			{Op: wire.Op(mountproto.OperationNativeRoutePage), Handler: service.handleNativeRoutePage, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativePin), Handler: service.handleNativePin, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeRelease), Handler: service.handleNativeRelease, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeSnapshotOpen), Handler: service.handleNativeSnapshotOpen, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeSnapshotRead), Handler: service.handleNativeSnapshotRead, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeSnapshotClose), Handler: service.handleNativeSnapshotClose, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeWriteOpen), Handler: service.handleNativeWriteOpen, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeWriteRead), Handler: service.handleNativeWriteRead, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeWriteWrite), Handler: service.handleNativeWrite, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeWriteTruncate), Handler: service.handleNativeWriteTruncate, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeWriteSync), Handler: service.handleNativeWriteSync, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeWriteCommit), Handler: service.handleNativeWriteCommit, Concurrent: true},
			{Op: wire.Op(mountproto.OperationNativeWriteAbort), Handler: service.handleNativeWriteAbort, Concurrent: true},
		} {
			server.Register(handler)
		}
	}
	return service, nil
}

func (s *Server) handleProvision(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.ProvisionTenantRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.ProvisionTenantResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenantID, owner, err := s.authorize(ctx, request, mountproto.OperationTenantProvision, catalog.Generation(input.Definition.Generation))
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.ProvisionTenantResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	spec, err := definitionSpec(owner, tenantID, input.Definition)
	if err != nil {
		return encoded(mountproto.ProvisionTenantResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	if err := s.config.Runtime.ProvisionTenant(ctx, spec); err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.ProvisionTenantResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	return encoded(mountproto.ProvisionTenantResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		TenantID: mountproto.TenantID(tenantID), Generation: uint64(spec.Generation),
	})
}

func (s *Server) handleReplace(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.ReplaceTenantRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.ReplaceTenantResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenantID, owner, err := s.authorize(ctx, request, mountproto.OperationTenantReplace, catalog.Generation(input.ExpectedGeneration))
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.ReplaceTenantResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	spec, err := definitionSpec(owner, tenantID, input.Definition)
	if err != nil {
		return encoded(mountproto.ReplaceTenantResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	if err := s.config.Runtime.ReplaceTenant(ctx, catalog.Generation(input.ExpectedGeneration), spec); err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.ReplaceTenantResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	return encoded(mountproto.ReplaceTenantResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		TenantID: mountproto.TenantID(tenantID), Generation: uint64(spec.Generation),
	})
}

func (s *Server) handleRemove(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.RemoveTenantRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.RemoveTenantResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenantID, owner, err := s.authorize(ctx, request, mountproto.OperationTenantRemove, catalog.Generation(input.Generation))
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.RemoveTenantResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	if err := s.config.Runtime.RemoveTenant(ctx, tenantID, catalog.Generation(input.Generation), owner); err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.RemoveTenantResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	return encoded(mountproto.RemoveTenantResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		TenantID: mountproto.TenantID(tenantID), Generation: input.Generation, FileProviderAbsent: true,
	})
}

func (s *Server) handleState(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.StateRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.StateResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenantID, owner, err := s.authorize(ctx, request, mountproto.OperationTenantState, 0)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.StateResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	status, err := s.config.Runtime.State(ctx, tenantID, owner)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.StateResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	if status.Owner != owner || status.State.Tenant != tenantID || status.State.Generation == 0 {
		return encoded(mountproto.StateResponse{
			Protocol: mountproto.Version, Code: mountproto.ErrorCodeUnavailable,
			Message: "mount service: runtime returned mismatched owner or tenant state",
		})
	}
	result := protocolState(status)
	return encoded(mountproto.StateResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, State: &result})
}

func (s *Server) authorize(ctx context.Context, request wire.Request, operation mountproto.Operation, generation catalog.Generation) (catalog.TenantID, tenant.OwnerID, error) {
	identity, err := requestIdentity(request)
	if err != nil {
		return "", "", ErrUnauthorized
	}
	tenantID, err := catalog.NewTenantID(request.Tenant)
	if err != nil {
		return "", "", fmt.Errorf("mount service: routing tenant: %w", err)
	}
	owner, err := s.config.Authorizer.Authorize(ctx, identity, operation, tenantID, generation)
	if err != nil {
		return "", "", err
	}
	if owner == "" {
		return "", "", ErrUnauthorized
	}
	return tenantID, owner, nil
}

func requestIdentity(request wire.Request) (Identity, error) {
	if request.WireBuild != transportproto.WireBuild || request.Session == nil || request.Session.WireBuild() != transportproto.WireBuild {
		return Identity{}, ErrUnauthorized
	}
	peer := request.Session.Peer()
	if peer.PID != request.Peer.PID || peer.UID != request.Peer.UID || !bytes.Equal(peer.Audit, request.Peer.Audit) {
		return Identity{}, ErrUnauthorized
	}
	return Identity{Peer: peer, WireBuild: request.Session.WireBuild(), Session: request.Session}, nil
}

func applicationError(err error) (mountproto.ErrorCode, string) {
	var coded *CodedError
	if errors.As(err, &coded) {
		return coded.Code, coded.Error()
	}
	var quarantined *tenant.QuarantinedError
	var retryable interface{ RetryAt() (time.Time, bool) }
	switch {
	case errors.Is(err, ErrUnauthorized), errors.Is(err, tenant.ErrTenantOwnerMismatch), errors.Is(err, catalog.ErrTenantOwnerMismatch):
		return mountproto.ErrorCodeUnauthorized, err.Error()
	case errors.Is(err, tenant.ErrTenantNotFound), errors.Is(err, catalog.ErrNotFound):
		return mountproto.ErrorCodeNotFound, err.Error()
	case errors.Is(err, tenant.ErrTenantConflict), errors.Is(err, tenant.ErrGenerationConflict),
		errors.Is(err, tenant.ErrTenantChanging), errors.Is(err, catalog.ErrGenerationMismatch),
		errors.Is(err, catalog.ErrStateConflict), errors.Is(err, catalog.ErrConflict):
		return mountproto.ErrorCodeConflict, err.Error()
	case errors.As(err, &quarantined):
		return mountproto.ErrorCodeQuarantined, err.Error()
	case errors.As(err, &retryable):
		if _, ok := retryable.RetryAt(); ok {
			return mountproto.ErrorCodeQuarantined, err.Error()
		}
		return mountproto.ErrorCodeUnavailable, err.Error()
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return mountproto.ErrorCodeCanceled, err.Error()
	default:
		return mountproto.ErrorCodeUnavailable, err.Error()
	}
}

func encoded(value any) (any, error) {
	raw, err := mountproto.Encode(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}
