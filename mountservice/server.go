package mountservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

// Config supplies the tenant runtime and authenticated owner policy.
type Config struct {
	Runtime        Runtime
	NativeSessions NativeSessions
	Authorizer     Authorizer
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
	if server.Build != transportproto.Build {
		return nil, fmt.Errorf("mount service: daemonkit build %q does not match transport suite %q", server.Build, transportproto.Build)
	}
	if config.Runtime == nil || config.NativeSessions == nil || config.Authorizer == nil {
		return nil, errors.New("mount service: runtime, native sessions, and authorizer are required")
	}
	service := &Server{config: config}
	server.RegisterConcurrent(wire.Op(mountproto.OperationTenantProvision), service.handleProvision)
	server.RegisterConcurrent(wire.Op(mountproto.OperationTenantReplace), service.handleReplace)
	server.RegisterConcurrent(wire.Op(mountproto.OperationTenantRemove), service.handleRemove)
	server.RegisterConcurrent(wire.Op(mountproto.OperationTenantState), service.handleState)
	server.RegisterControl(wire.Op(mountproto.OperationNativeBind), service.handleNativeBind)
	server.RegisterConcurrent(wire.Op(mountproto.OperationNativeReady), service.handleNativeReady)
	server.RegisterConcurrent(wire.Op(mountproto.OperationNativeRoutes), service.handleNativeRoutes)
	server.RegisterConcurrent(wire.Op(mountproto.OperationNativePin), service.handleNativePin)
	server.RegisterConcurrent(wire.Op(mountproto.OperationNativeRelease), service.handleNativeRelease)
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
	tenantID, _, err := s.authorize(ctx, request, mountproto.OperationTenantRemove, catalog.Generation(input.Generation))
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.RemoveTenantResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	if err := s.config.Runtime.RemoveTenant(ctx, tenantID, catalog.Generation(input.Generation)); err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.RemoveTenantResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	return encoded(mountproto.RemoveTenantResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		TenantID: mountproto.TenantID(tenantID), Generation: input.Generation,
	})
}

func (s *Server) handleState(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.StateRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.StateResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenantID, _, err := s.authorize(ctx, request, mountproto.OperationTenantState, catalog.Generation(input.Generation))
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.StateResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	state, err := s.config.Runtime.State(ctx, tenantID, catalog.Generation(input.Generation))
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.StateResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	if state.Tenant != tenantID || state.Generation != catalog.Generation(input.Generation) {
		return encoded(mountproto.StateResponse{
			Protocol: mountproto.Version, Code: mountproto.ErrorCodeUnavailable,
			Message: "mount service: runtime returned mismatched tenant state",
		})
	}
	result := protocolState(state)
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
	if request.Build != transportproto.Build || request.Session == nil || request.Session.Build() != transportproto.Build {
		return Identity{}, ErrUnauthorized
	}
	peer := request.Session.Peer()
	if peer.PID != request.Peer.PID || peer.UID != request.Peer.UID || !bytes.Equal(peer.Audit, request.Peer.Audit) {
		return Identity{}, ErrUnauthorized
	}
	return Identity{Peer: peer, Build: request.Session.Build(), Session: request.Session}, nil
}

func applicationError(err error) (mountproto.ErrorCode, string) {
	var coded *CodedError
	if errors.As(err, &coded) {
		return coded.Code, coded.Error()
	}
	var quarantined *tenant.QuarantinedError
	switch {
	case errors.Is(err, ErrUnauthorized):
		return mountproto.ErrorCodeUnauthorized, err.Error()
	case errors.Is(err, tenant.ErrTenantNotFound), errors.Is(err, catalog.ErrNotFound):
		return mountproto.ErrorCodeNotFound, err.Error()
	case errors.Is(err, tenant.ErrTenantConflict), errors.Is(err, tenant.ErrGenerationConflict),
		errors.Is(err, tenant.ErrTenantChanging), errors.Is(err, catalog.ErrGenerationMismatch),
		errors.Is(err, catalog.ErrStateConflict), errors.Is(err, catalog.ErrConflict):
		return mountproto.ErrorCodeConflict, err.Error()
	case errors.As(err, &quarantined):
		return mountproto.ErrorCodeQuarantined, err.Error()
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
