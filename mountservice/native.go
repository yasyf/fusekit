package mountservice

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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
	mu      sync.Mutex
	settled *sync.Cond
	closed  bool
	active  int
	owner   string
	pins    map[string]NativePin
	handles map[string]nativeHandle

	closeOnce sync.Once
	closeErr  error
}

type nativeHandleKind uint8

const (
	nativeSnapshotHandle nativeHandleKind = iota + 1
	nativeWriteHandle
)

type nativeHandle struct {
	kind       nativeHandleKind
	tenant     catalog.TenantID
	generation catalog.Generation
	object     catalog.ObjectID
	revision   catalog.Revision
	operation  catalog.MutationID
}

func (r *nativeSessionRegistry) bind(session *wire.AcceptedSession) (*nativeSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bound != nil {
		return nil, fmt.Errorf("%w: native session is already bound", catalog.ErrConflict)
	}
	owner, err := newNativeToken()
	if err != nil {
		return nil, err
	}
	state := &nativeSession{
		owner: owner, pins: make(map[string]NativePin), handles: make(map[string]nativeHandle),
	}
	state.settled = sync.NewCond(&state.mu)
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

func (r *nativeSessionRegistry) settle(
	session *wire.AcceptedSession,
	state *nativeSession,
	store NativeCatalog,
) error {
	r.mu.Lock()
	matches := r.bound == session && r.state == state
	r.mu.Unlock()
	if !matches {
		return errNativeSession
	}
	return state.close(store)
}

func (r *nativeSessionRegistry) close(
	session *wire.AcceptedSession,
	state *nativeSession,
	store NativeCatalog,
) error {
	r.mu.Lock()
	if r.bound != session || r.state != state {
		r.mu.Unlock()
		return state.close(store)
	}
	r.mu.Unlock()

	result := state.close(store)

	r.mu.Lock()
	if r.bound == session && r.state == state {
		r.bound = nil
		r.state = nil
	}
	r.mu.Unlock()
	return result
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

func (s *nativeSession) begin() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errNativeSession
	}
	s.active++
	return nil
}

func (s *nativeSession) end() {
	s.mu.Lock()
	s.active--
	if s.active == 0 {
		s.settled.Broadcast()
	}
	s.mu.Unlock()
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

func (s *nativeSession) authorizeRoute(tenant catalog.TenantID, generation catalog.Generation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errNativeSession
	}
	for _, pin := range s.pins {
		if pin.Route.Tenant == tenant && pin.Route.Generation == generation {
			return nil
		}
	}
	return catalog.ErrGenerationMismatch
}

func (s *nativeSession) addHandle(token string, handle nativeHandle) error {
	if token == "" {
		return catalog.ErrIntegrity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errNativeSession
	}
	if _, exists := s.handles[token]; exists {
		return catalog.ErrConflict
	}
	s.handles[token] = handle
	return nil
}

func (s *nativeSession) handle(token string, kind nativeHandleKind) (nativeHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	handle, exists := s.handles[token]
	if !exists {
		return nativeHandle{}, catalog.ErrNotFound
	}
	if handle.kind != kind {
		return nativeHandle{}, catalog.ErrInvalidObject
	}
	return handle, nil
}

func (s *nativeSession) removeHandle(token string, kind nativeHandleKind) (nativeHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	handle, exists := s.handles[token]
	if !exists {
		return nativeHandle{}, catalog.ErrNotFound
	}
	if handle.kind != kind {
		return nativeHandle{}, catalog.ErrInvalidObject
	}
	delete(s.handles, token)
	return handle, nil
}

func (s *nativeSession) acceptWriteCommit(
	token string,
	operation catalog.MutationID,
	object catalog.Object,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	handle, exists := s.handles[token]
	if !exists {
		return catalog.ErrNotFound
	}
	if handle.kind != nativeWriteHandle {
		return catalog.ErrInvalidObject
	}
	if object.Tenant != handle.tenant || object.ID != handle.object || object.Revision == 0 {
		return catalog.ErrIntegrity
	}
	if operation == (catalog.MutationID{}) || operation.TargetRevision() != object.Revision {
		return catalog.ErrIntegrity
	}
	if handle.operation == operation {
		if object.Revision != handle.revision {
			return catalog.ErrIntegrity
		}
		return nil
	}
	if object.Revision != handle.revision+1 {
		return catalog.ErrIntegrity
	}
	handle.revision = object.Revision
	handle.operation = operation
	s.handles[token] = handle
	return nil
}

func (s *nativeSession) close(store NativeCatalog) error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		for s.active != 0 {
			s.settled.Wait()
		}
		owner := s.owner
		pins := s.pins
		s.pins = nil
		s.handles = nil
		s.mu.Unlock()

		result := store.CloseSession(context.Background(), owner)
		for _, pin := range pins {
			result = errors.Join(result, pin.Release())
		}
		s.closeErr = result
	})
	return s.closeErr
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
		s.config.NativeSessions.Unbind(identity)
		closeErr := s.native.close(identity.Session, state, s.config.NativeCatalog)
		s.config.NativeSessions.Settled(identity, closeErr)
		code, message := applicationError(err)
		return encoded(mountproto.NativeBindResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	response, err := encoded(mountproto.NativeBindResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk})
	if err != nil {
		s.config.NativeSessions.Unbind(identity)
		closeErr := s.native.close(identity.Session, state, s.config.NativeCatalog)
		s.config.NativeSessions.Settled(identity, closeErr)
		return nil, err
	}
	go func() {
		<-identity.Session.Disconnected()
		s.config.NativeSessions.Unbind(identity)
		<-identity.Session.Done()
		closeErr := s.native.close(identity.Session, state, s.config.NativeCatalog)
		s.config.NativeSessions.Settled(identity, closeErr)
	}()
	return response, nil
}

func (s *Server) handleNativeReady(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeReadyRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.NativeReadyResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	_, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeReady)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeReadyResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	defer finish()
	identity, err := requestIdentity(request)
	if err == nil {
		err = s.config.NativeSessions.Ready(ctx, identity, nativeMountProof(input.Mount))
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeReadyResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	return encoded(mountproto.NativeReadyResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk})
}

func (s *Server) handleNativeMounted(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeMountedRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.NativeMountedResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	_, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeMounted)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeMountedResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	defer finish()
	identity, err := requestIdentity(request)
	if err == nil {
		err = s.config.NativeSessions.Mounted(ctx, identity, nativeMountIdentity(input.Mount))
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeMountedResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	return encoded(mountproto.NativeMountedResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk})
}

func (s *Server) handleNativeUnbind(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeUnbindRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.NativeUnbindResponse{
			Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error(),
		})
	}
	identity, err := s.authorizeNative(ctx, request, mountproto.OperationNativeUnbind)
	if err == nil {
		var state *nativeSession
		state, err = s.native.session(identity.Session)
		if err == nil {
			err = s.native.settle(identity.Session, state, s.config.NativeCatalog)
		}
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeUnbindResponse{
			Protocol: mountproto.Version, Code: code, Message: message,
		})
	}
	return encoded(mountproto.NativeUnbindResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
	})
}

func (s *Server) handleNativeRoutePage(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeRoutePageRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.NativeRoutePageResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	_, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeRoutePage)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeRoutePageResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	defer finish()
	page, err := s.config.NativeSessions.RoutePage(ctx, input.Snapshot, input.After, int(input.Limit))
	if err == nil && (page.Snapshot == 0 ||
		(input.Snapshot != 0 && page.Snapshot != input.Snapshot) ||
		len(page.Routes) > int(input.Limit) ||
		(input.After != "" && len(page.Routes) > 0 &&
			strings.Compare(input.After, page.Routes[0].Name) >= 0)) {
		err = catalog.ErrIntegrity
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativeRoutePageResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	result := make([]mountproto.MountRoute, len(page.Routes))
	for index, route := range page.Routes {
		result[index] = protocolNativeRoute(route)
	}
	return encoded(mountproto.NativeRoutePageResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		Snapshot: page.Snapshot, Routes: result, Next: page.Next,
	})
}

func (s *Server) handleNativePin(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativePinRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.NativePinResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativePin)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativePinResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	defer finish()
	pin, err := s.config.NativeSessions.Pin(ctx, input.Name)
	if err != nil {
		code, message := applicationError(err)
		return encoded(mountproto.NativePinResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	token, err := newNativeToken()
	if err != nil {
		_ = pin.Release()
		return nil, err
	}
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

func newNativeToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("mount service: generate native token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

func (s *Server) handleNativeRelease(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeReleaseRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return encoded(mountproto.NativeReleaseResponse{Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeRelease)
	if err == nil {
		defer finish()
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
	if err := s.config.ProtectedNativePeer(ctx, identity.Peer); err != nil {
		return Identity{}, err
	}
	if err := s.config.Authorizer.AuthorizeNative(ctx, identity, operation); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

func (s *Server) boundNative(ctx context.Context, request wire.Request, operation mountproto.Operation) (*nativeSession, func(), error) {
	identity, err := s.authorizeNative(ctx, request, operation)
	if err != nil {
		return nil, nil, err
	}
	state, err := s.native.session(identity.Session)
	if err != nil {
		return nil, nil, err
	}
	if err := state.begin(); err != nil {
		return nil, nil, err
	}
	return state, state.end, nil
}

func protocolNativeRoute(route NativeRoute) mountproto.MountRoute {
	return mountproto.MountRoute{Name: route.Name, TenantID: mountproto.TenantID(route.Tenant), Generation: uint64(route.Generation)}
}
