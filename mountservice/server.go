package mountservice

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit"
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
	ProtectedPeer func(context.Context, daemonkit.Caller) error
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

// Routes is the immutable protocol shape installed before daemon activation.
type Routes struct {
	Native bool
}

// Resolver binds one admitted request to its generation-pinned service.
type Resolver func(daemonkit.Request) (*Server, error)

// New validates and constructs one generation-local mount service.
func New(config Config) (*Server, error) {
	if config.Runtime == nil || config.Authorizer == nil {
		return nil, errors.New("mount service: runtime and authorizer are required")
	}
	if config.Native != nil &&
		(config.Native.Sessions == nil || config.Native.Catalog == nil || config.Native.ProtectedPeer == nil) {
		return nil, errors.New("mount service: native sessions, catalog, and protected peer verifier are required together")
	}
	return &Server{config: config}, nil
}

// Register installs the exact immutable mount route set before daemon Begin.
func Register(routes Routes, resolve Resolver) ([]transportproto.HandlerSpec, error) {
	if resolve == nil {
		return nil, errors.New("mount service: generation resolver is required")
	}
	var specs []transportproto.HandlerSpec
	register := func(operation mountproto.Operation, concurrent bool, handler func(*Server, context.Context, daemonkit.Request) ([]byte, error)) {
		specs = append(specs, transportproto.HandlerSpec{
			Op: string(operation), Concurrent: concurrent,
			Handler: func(ctx context.Context, request daemonkit.Request) ([]byte, error) {
				service, err := resolve(request)
				if err != nil {
					return nil, err
				}
				if service == nil {
					return nil, errors.New("mount service: generation resolver returned nil")
				}
				return handler(service, ctx, request)
			},
		})
	}
	register(mountproto.OperationTenantProvision, true, (*Server).handleProvision)
	register(mountproto.OperationTenantReplace, true, (*Server).handleReplace)
	register(mountproto.OperationTenantRemove, true, (*Server).handleRemove)
	register(mountproto.OperationTenantState, true, (*Server).handleState)
	if routes.Native {
		register(mountproto.OperationNativeBind, false, (*Server).handleNativeBind)
		register(mountproto.OperationNativeMounted, true, (*Server).handleNativeMounted)
		register(mountproto.OperationNativeReady, true, (*Server).handleNativeReady)
		register(mountproto.OperationNativeUnbind, false, (*Server).handleNativeUnbind)
		register(mountproto.OperationNativeRoutePage, true, (*Server).handleNativeRoutePage)
		register(mountproto.OperationNativePin, true, (*Server).handleNativePin)
		register(mountproto.OperationNativeRelease, true, (*Server).handleNativeRelease)
		register(mountproto.OperationNativeSnapshotOpen, true, (*Server).handleNativeSnapshotOpen)
		register(mountproto.OperationNativeSnapshotRead, true, (*Server).handleNativeSnapshotRead)
		register(mountproto.OperationNativeSnapshotClose, true, (*Server).handleNativeSnapshotClose)
		register(mountproto.OperationNativeWriteOpen, true, (*Server).handleNativeWriteOpen)
		register(mountproto.OperationNativeWriteRead, true, (*Server).handleNativeWriteRead)
		register(mountproto.OperationNativeWriteWrite, true, (*Server).handleNativeWrite)
		register(mountproto.OperationNativeWriteTruncate, true, (*Server).handleNativeWriteTruncate)
		register(mountproto.OperationNativeWriteSync, true, (*Server).handleNativeWriteSync)
		register(mountproto.OperationNativeWriteCommit, true, (*Server).handleNativeWriteCommit)
		register(mountproto.OperationNativeWriteAbort, true, (*Server).handleNativeWriteAbort)
	}
	return specs, nil
}

func (s *Server) handleProvision(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input mountproto.ProvisionTenantRequest
	if err := mountproto.Decode(request.Body, &input); err != nil {
		return encoded(mountproto.ProvisionTenantResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenantID, owner, err := s.authorize(ctx, request, input.Tenant, mountproto.OperationTenantProvision, catalog.Generation(input.Definition.Generation))
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

func (s *Server) handleReplace(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input mountproto.ReplaceTenantRequest
	if err := mountproto.Decode(request.Body, &input); err != nil {
		return encoded(mountproto.ReplaceTenantResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenantID, owner, err := s.authorize(ctx, request, input.Tenant, mountproto.OperationTenantReplace, catalog.Generation(input.ExpectedGeneration))
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

func (s *Server) handleRemove(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input mountproto.RemoveTenantRequest
	if err := mountproto.Decode(request.Body, &input); err != nil {
		return encoded(mountproto.RemoveTenantResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenantID, owner, err := s.authorize(ctx, request, input.Tenant, mountproto.OperationTenantRemove, catalog.Generation(input.Generation))
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

func (s *Server) handleState(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input mountproto.StateRequest
	if err := mountproto.Decode(request.Body, &input); err != nil {
		return encoded(mountproto.StateResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenantID, owner, err := s.authorize(ctx, request, input.Tenant, mountproto.OperationTenantState, 0)
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

func (s *Server) authorize(ctx context.Context, request daemonkit.Request, routing mountproto.TenantID, operation mountproto.Operation, generation catalog.Generation) (catalog.TenantID, tenant.OwnerID, error) {
	identity, err := requestIdentity(request)
	if err != nil {
		return "", "", ErrUnauthorized
	}
	tenantID, err := catalog.NewTenantID(string(routing))
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

func requestIdentity(request daemonkit.Request) (Identity, error) {
	return Identity{Caller: request.Caller, Session: request.Session}, nil
}

func applicationError(err error) (mountproto.ErrorCode, string) {
	var coded *CodedError
	if errors.As(err, &coded) {
		return coded.Code, coded.Error()
	}
	var quarantined *tenant.QuarantinedError
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
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return mountproto.ErrorCodeCanceled, err.Error()
	default:
		return mountproto.ErrorCodeUnavailable, err.Error()
	}
}

func encoded(value any) ([]byte, error) {
	return mountproto.Encode(value)
}
