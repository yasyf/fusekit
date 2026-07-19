package mountservice

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
)

var errNativeSession = errors.New("mount service: native session is not bound")

type nativeSessionRegistry struct {
	mu    sync.Mutex
	bound *wire.AcceptedSession
	state *nativeSession
}

type nativeSession struct {
	mu     sync.Mutex
	closed bool
	pins   map[string]NativePin
}

func (r *nativeSessionRegistry) bind(session *wire.AcceptedSession) (*nativeSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bound != nil {
		return nil, fmt.Errorf("%w: native session is already bound", catalog.ErrConflict)
	}
	state := &nativeSession{pins: make(map[string]NativePin)}
	r.bound = session
	r.state = state
	return state, nil
}

func (r *nativeSessionRegistry) session(session *wire.AcceptedSession) (*nativeSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bound != session || r.state == nil {
		return nil, errNativeSession
	}
	return r.state, nil
}

func (r *nativeSessionRegistry) close(session *wire.AcceptedSession, state *nativeSession) {
	r.mu.Lock()
	if r.bound == session && r.state == state {
		r.bound = nil
		r.state = nil
	}
	r.mu.Unlock()
	state.close()
}

func (s *nativeSession) add(token string, pin NativePin) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errNativeSession
	}
	s.pins[token] = pin
	return nil
}

func (s *nativeSession) release(token string) error {
	s.mu.Lock()
	pin, exists := s.pins[token]
	if exists {
		delete(s.pins, token)
	}
	s.mu.Unlock()
	if !exists {
		return catalog.ErrNotFound
	}
	return pin.Release()
}

func (s *nativeSession) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	pins := s.pins
	s.pins = nil
	s.mu.Unlock()
	for _, pin := range pins {
		_ = pin.Release()
	}
}

func (s *Server) handleNativeBind(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeBindRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.NativeBindResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	identity, err := s.authorizeNative(ctx, request, mountproto.OperationNativeBind)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeBindResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	state, err := s.native.bind(identity.Session)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeBindResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	if err := s.config.NativeSessions.Bind(ctx, identity); err != nil {
		s.native.close(identity.Session, state)
		code, message := applicationError(err)
		return encoded(mountproto.NativeBindResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	response, err := encoded(mountproto.NativeBindResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk})
	if err != nil {
		s.native.close(identity.Session, state)
		s.config.NativeSessions.Unbind(identity)
		return nil, err
	}
	go func() {
		<-identity.Session.Done()
		s.native.close(identity.Session, state)
		s.config.NativeSessions.Unbind(identity)
	}()
	return response, nil
}

func (s *Server) handleNativeReady(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeReadyRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.NativeReadyResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	if _, err := s.boundNative(ctx, request, mountproto.OperationNativeReady); err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeReadyResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	identity, err := requestIdentity(request)
	if err == nil {
		err = s.config.NativeSessions.Ready(ctx, identity)
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeReadyResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	return encoded(mountproto.NativeReadyResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk})
}

func (s *Server) handleNativeRoutes(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeRoutesRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.NativeRoutesResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	if _, err := s.boundNative(ctx, request, mountproto.OperationNativeRoutes); err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeRoutesResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	routes, err := s.config.NativeSessions.Routes(ctx)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeRoutesResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	slices.SortFunc(routes, func(left, right NativeRoute) int { return strings.Compare(left.Name, right.Name) })
	result := make([]mountproto.MountRoute, len(routes))
	for index, route := range routes {
		result[index] = protocolNativeRoute(route)
	}
	return encoded(mountproto.NativeRoutesResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Routes: result})
}

func (s *Server) handleNativePin(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativePinRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.NativePinResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	state, err := s.boundNative(ctx, request, mountproto.OperationNativePin)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativePinResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	pin, err := s.config.NativeSessions.Pin(ctx, input.Name)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativePinResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	id, err := catalog.NewMutationID()
	if err != nil {
		_ = pin.Release()
		return nil, err
	}
	token := id.String()
	if err := state.add(token, pin); err != nil {
		_ = pin.Release()
		code, message := applicationError(err)
		return encoded(mountproto.NativePinResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	route := protocolNativeRoute(pin.Route)
	definition := protocolDefinition(pin.Spec)
	return encoded(mountproto.NativePinResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Token: token,
		OwnerID: mountproto.OwnerID(pin.Spec.OwnerID),
		Route:   &route, Definition: &definition,
	})
}

func (s *Server) handleNativeRelease(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeReleaseRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.NativeReleaseResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	state, err := s.boundNative(ctx, request, mountproto.OperationNativeRelease)
	if err == nil {
		err = state.release(input.Token)
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeReleaseResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	return encoded(mountproto.NativeReleaseResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Token: input.Token})
}

func (s *Server) authorizeNative(ctx context.Context, request wire.Request, operation mountproto.Operation) (Identity, error) {
	if request.Tenant != "" {
		return Identity{}, ErrUnauthorized
	}
	identity, err := requestIdentity(request)
	if err != nil {
		return Identity{}, err
	}
	if err := s.config.Authorizer.AuthorizeNative(ctx, identity, operation); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

func (s *Server) boundNative(ctx context.Context, request wire.Request, operation mountproto.Operation) (*nativeSession, error) {
	identity, err := s.authorizeNative(ctx, request, operation)
	if err != nil {
		return nil, err
	}
	return s.native.session(identity.Session)
}

func protocolNativeRoute(route NativeRoute) mountproto.MountRoute {
	return mountproto.MountRoute{Name: route.Name, TenantID: mountproto.TenantID(route.Tenant), Generation: uint64(route.Generation)}
}
