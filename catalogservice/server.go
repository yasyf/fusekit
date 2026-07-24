package catalogservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/transportproto"
)

const (
	streamBufferSize            = 64 * 1024
	contentlessTerminalTimeout  = 5 * time.Second
	mutationStageCleanupTimeout = 5 * time.Second
	remoteErrorMessageBytes     = int(catalogproto.MaxErrorMessageBytes)
)

// CoreConfig supplies the services required by every catalog presentation.
type CoreConfig struct {
	Reader       Reader
	Mutations    MutationService
	Preparation  PreparationService
	SourceFleets SourceFleetService
	Authorizer   Authorizer
}

// FileProviderConfig supplies the services required only by File Provider.
type FileProviderConfig struct {
	Activations ActivationService
	Broker      BrokerService
	// ProtectedPeer verifies a signed File Provider broker after the product
	// authorizer has selected the closed File Provider role.
	ProtectedPeer func(context.Context, wire.Peer) error
}

// Routes fixes the product's exact catalog capabilities before the daemon runtime begins.
type Routes struct {
	FileProvider bool
}

// Resolver selects the generation-local service exclusively through the admitted
// request's PublicationSlot.LoadPinned token. Zero, stale, and current-generation
// fallback resolution must fail.
type Resolver func(wire.Request) (*Server, error)

// Server binds the catalog application protocol exclusively to daemonkit wire.
type Server struct {
	core CoreConfig

	fileProvider *FileProviderConfig

	brokerMu sync.Mutex
	brokers  map[string]*brokerSlot
}

type brokerSlot struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type forwardedRouteKey struct{}

func (s *Server) handleBrokerForward(ctx context.Context, request wire.Request) (any, error) {
	if request.Tenant != "" {
		return nil, errors.New("catalog service: broker forward forbids a routing tenant")
	}
	var envelope catalogproto.BrokerForwardRequest
	if err := catalogproto.Decode(request.Payload, &envelope); err != nil {
		return nil, err
	}
	tenant, err := catalog.NewTenantID(string(envelope.Context.TenantID))
	if err != nil {
		return nil, err
	}
	route := Route{
		Tenant: tenant, Generation: catalog.Generation(envelope.Context.Generation),
		Domain: envelope.Context.DomainID, Forwarded: true,
	}
	inner := request
	inner.Op = wire.Op(envelope.Operation)
	inner.Tenant = string(tenant)
	inner.Payload = envelope.Payload
	ctx = context.WithValue(ctx, forwardedRouteKey{}, route)
	switch envelope.Operation {
	case catalogproto.OperationCatalogHead:
		return s.handleHead(ctx, inner)
	case catalogproto.OperationCatalogSnapshot:
		return s.handleSnapshot(ctx, inner)
	case catalogproto.OperationCatalogChangesSince:
		return s.handleChangesSince(ctx, inner)
	case catalogproto.OperationCatalogLookup:
		return s.handleLookup(ctx, inner)
	case catalogproto.OperationCatalogLookupName:
		return s.handleLookupName(ctx, inner)
	case catalogproto.OperationCatalogOpenAt:
		return s.handleOpenAt(ctx, inner)
	case catalogproto.OperationCatalogMutate:
		return s.handleMutation(ctx, inner)
	case catalogproto.OperationActivationAck:
		return s.handleAckActivation(ctx, inner)
	default:
		return nil, errors.New("catalog service: operation cannot be broker-forwarded")
	}
}

// New validates and constructs one generation-local catalog service.
func New(core CoreConfig, fileProvider *FileProviderConfig) (*Server, error) {
	if core.Reader == nil || core.Mutations == nil || core.Preparation == nil || core.SourceFleets == nil || core.Authorizer == nil {
		return nil, errors.New("catalog service: every core service and the authorizer are required")
	}
	if fileProvider != nil {
		if fileProvider.Activations == nil || fileProvider.Broker == nil || fileProvider.ProtectedPeer == nil {
			return nil, errors.New("catalog service: every File Provider service and protected-peer verifier are required")
		}
		copy := *fileProvider
		fileProvider = &copy
	}
	return &Server{core: core, fileProvider: fileProvider, brokers: make(map[string]*brokerSlot)}, nil
}

type serviceHandler func(*Server, context.Context, wire.Request) (any, error)

type serviceRoute struct {
	operation    catalogproto.Operation
	handler      serviceHandler
	concurrent   bool
	fileProvider bool
}

// Register installs the fixed catalog route set before the daemon runtime begins.
func Register(server *wire.Server, routes Routes, resolve Resolver) error {
	if server == nil {
		return errors.New("catalog service: daemonkit server is nil")
	}
	if server.WireBuild != transportproto.WireBuild {
		return fmt.Errorf("catalog service: daemonkit build %q does not match transport suite %q", server.WireBuild, transportproto.WireBuild)
	}
	if resolve == nil {
		return errors.New("catalog service: resolver is required")
	}
	registered := []serviceRoute{
		{catalogproto.OperationCatalogRoot, (*Server).handleRoot, true, false},
		{catalogproto.OperationCatalogHead, (*Server).handleHead, true, false},
		{catalogproto.OperationCatalogSnapshot, (*Server).handleSnapshot, true, false},
		{catalogproto.OperationCatalogChangesSince, (*Server).handleChangesSince, true, false},
		{catalogproto.OperationCatalogLookup, (*Server).handleLookup, true, false},
		{catalogproto.OperationCatalogLookupName, (*Server).handleLookupName, true, false},
		{catalogproto.OperationCatalogOpenAt, (*Server).handleOpenAt, true, false},
		{catalogproto.OperationCatalogMutate, (*Server).handleMutation, true, false},
		{catalogproto.OperationTenantPrepare, (*Server).handlePrepareTenant, true, false},
		{catalogproto.OperationSourceAuthorityPublishDesiredFleet, (*Server).handlePublishDesiredSourceFleet, true, false},
		{catalogproto.OperationSourceAuthorityReadDesiredFleet, (*Server).handleReadDesiredSourceFleet, true, false},
	}
	if routes.FileProvider {
		registered = append(registered,
			serviceRoute{catalogproto.OperationActivationAck, (*Server).handleAckActivation, true, true},
			serviceRoute{catalogproto.OperationBrokerForward, (*Server).handleBrokerForward, true, true},
			serviceRoute{catalogproto.OperationBrokerOpen, (*Server).handleBrokerOpen, false, true},
		)
	}
	for _, route := range registered {
		server.Register(wire.HandlerSpec{
			Op: wire.Op(route.operation), Concurrent: route.concurrent,
			Handler: resolvedHandler(resolve, route.fileProvider, route.handler),
		})
	}
	return nil
}

func resolvedHandler(resolve Resolver, fileProvider bool, handler serviceHandler) wire.Handler {
	return func(ctx context.Context, request wire.Request) (any, error) {
		server, err := resolve(request)
		if err != nil {
			return nil, err
		}
		if server == nil {
			return nil, errors.New("catalog service: resolver returned nil service")
		}
		if fileProvider && server.fileProvider == nil {
			return nil, errors.New("catalog service: resolved generation has no File Provider capability")
		}
		return handler(server, ctx, request)
	}
}

func (s *Server) handleRoot(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.RootRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogRoot, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	object, err := s.core.Reader.Root(ctx, authorization, tenant)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	result, err := protocolObject(object)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Object: &result})
}

func (s *Server) handleHead(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.HeadRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.HeadResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogHead, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.HeadResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	revision, err := s.core.Reader.Head(ctx, authorization, tenant)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.HeadResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.HeadResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Revision: uint64(revision)})
}

func (s *Server) handleSnapshot(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.SnapshotRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()), Objects: []catalogproto.CatalogObject{}})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogSnapshot, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: code, Message: message, Objects: []catalogproto.CatalogObject{}})
	}
	cursor := catalog.SnapshotCursor{}
	scope, err := catalogEnumerationScope(input.Scope)
	if err != nil {
		return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()), Objects: []catalogproto.CatalogObject{}})
	}
	if input.After != nil {
		after, err := catalogObjectID(*input.After)
		if err != nil {
			return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()), Objects: []catalogproto.CatalogObject{}})
		}
		cursor.After = &after
	}
	page, err := s.core.Reader.Snapshot(ctx, authorization, tenant, scope, catalog.Revision(input.Revision), cursor, int(input.Limit))
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: code, Message: message, Objects: []catalogproto.CatalogObject{}})
	}
	objects, err := protocolObjects(page.Objects)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: code, Message: message, Objects: []catalogproto.CatalogObject{}})
	}
	var next *catalogproto.ObjectID
	if page.Next != nil && page.Next.After != nil {
		next = protocolObjectID(*page.Next.After)
	}
	return encoded(catalogproto.SnapshotResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		Revision: uint64(page.Revision), Objects: objects, Next: next,
	})
}

func (s *Server) handleChangesSince(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.ChangesSinceRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.ChangesSinceResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()), Changes: []catalogproto.Change{}})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogChangesSince, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ChangesSinceResponse{Protocol: catalogproto.Version, Code: code, Message: message, Changes: []catalogproto.Change{}})
	}
	scope, err := catalogEnumerationScope(input.Scope)
	if err != nil {
		return encoded(catalogproto.ChangesSinceResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()), Changes: []catalogproto.Change{}})
	}
	page, err := s.core.Reader.ChangesSince(ctx, authorization, tenant, scope, catalog.ChangeCursor{
		Revision: catalog.Revision(input.Cursor.Revision), Sequence: input.Cursor.Sequence,
	}, int(input.Limit))
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ChangesSinceResponse{Protocol: catalogproto.Version, Code: code, Message: message, Changes: []catalogproto.Change{}})
	}
	changes, err := protocolChanges(page.Changes)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ChangesSinceResponse{Protocol: catalogproto.Version, Code: code, Message: message, Changes: []catalogproto.Change{}})
	}
	return encoded(catalogproto.ChangesSinceResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		Floor: uint64(page.Floor), Head: uint64(page.Head),
		Next:     catalogproto.ChangeCursor{Revision: uint64(page.Next.Revision), Sequence: page.Next.Sequence},
		Complete: page.Complete, Changes: changes,
	})
}

func (s *Server) handleLookup(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.LookupRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogLookup, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	id, err := catalogObjectID(input.ObjectID)
	if err != nil {
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	object, err := s.core.Reader.Lookup(ctx, authorization, tenant, id)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	converted, err := protocolObject(object)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Object: &converted})
}

func (s *Server) handleLookupName(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.LookupNameRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogLookupName, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	parent, err := catalogObjectID(input.ParentID)
	if err != nil {
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	object, err := s.core.Reader.LookupName(ctx, authorization, tenant, parent, input.Name)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	converted, err := protocolObject(object)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Object: &converted})
}

func (s *Server) handleOpenAt(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.OpenAtRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return emptyStream(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogOpenAt, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return emptyStream(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	id, err := catalogObjectID(input.ObjectID)
	if err != nil {
		return emptyStream(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	opened, err := s.core.Reader.OpenAt(ctx, authorization, tenant, catalog.Generation(input.Generation), id, catalog.Revision(input.Revision))
	if err != nil {
		code, message := applicationError(err)
		return emptyStream(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if opened.Content == nil || opened.Object.ID != id || opened.Object.Revision != catalog.Revision(input.Revision) {
		var closeErr error
		if opened.Content != nil {
			closeErr = opened.Content.Close()
		}
		code, message := applicationError(errors.Join(
			fmt.Errorf("%w: open returned the wrong immutable object revision", catalog.ErrIntegrity),
			closeErr,
		))
		return emptyStream(catalogproto.OpenAtResponse{
			Protocol: catalogproto.Version, Code: code, Message: message,
		})
	}
	object, err := protocolObject(opened.Object)
	if err != nil {
		code, message := applicationError(errors.Join(err, opened.Content.Close()))
		return emptyStream(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	chunks := make(chan []byte)
	terminal := new(json.RawMessage)
	go streamContent(ctx, opened.Content, object, chunks, terminal)
	return wire.StreamResponse{Chunks: chunks, Value: terminal}, nil
}

func (s *Server) handleMutation(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.MutationRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationCatalogMutate, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	generation := catalog.Generation(input.Generation)
	var stream *chunkReader
	if input.HasContent {
		stream = &chunkReader{
			ctx: ctx, chunks: request.Chunks, closed: make(chan struct{}), settled: make(chan struct{}),
		}
	} else if err := validateEmptyMutationInput(ctx, request.Chunks, contentlessTerminalTimeout); err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.MutationResponse{
			Protocol: catalogproto.Version, Code: code, Message: message,
		})
	}
	stage, err := s.core.Mutations.StageMutation(ctx, identity, authorization, tenant, input.RequestID, generation, input.HasContent, stream)
	if stream != nil {
		settleErr := stream.Settle(err)
		waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationStageCleanupTimeout)
		waitErr := stream.Wait(waitCtx)
		cancel()
		err = errors.Join(err, settleErr, waitErr)
	}
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	defer func() {
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationStageCleanupTimeout)
		_ = stage.Abort(abortCtx)
		cancel()
	}()
	if input.HasContent && !stream.exhausted.Load() || stage.Token == "" ||
		stage.RequestID != input.RequestID || stage.Tenant != tenant ||
		stage.Generation != generation || stage.Size < 0 || !input.HasContent && stage.Size != 0 {
		return mutationStageFailure(ctx, stage, fmt.Errorf("%w: staged mutation identity or byte stream is inconsistent", catalog.ErrIntegrity))
	}
	result, err := s.core.Mutations.SubmitMutation(ctx, identity, authorization, MutationSubmission{Request: input, Stage: stage})
	if err != nil {
		return mutationStageFailure(ctx, stage, err)
	}
	if result.RequestID != input.RequestID || result.OperationID == (catalog.MutationID{}) || result.Revision == 0 {
		return mutationStageFailure(ctx, stage, fmt.Errorf("%w: mutation result identity is inconsistent", catalog.ErrIntegrity))
	}
	responseRequest := result.RequestID
	responseMutation := catalogproto.MutationID(result.OperationID.String())
	return encoded(catalogproto.MutationResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		RequestID: &responseRequest, MutationID: &responseMutation, Revision: uint64(result.Revision),
		PrimaryID: protocolOptionalObjectID(result.PrimaryID), SecondaryID: protocolOptionalObjectID(result.SecondaryID),
	})
}

func validateEmptyMutationInput(ctx context.Context, chunks <-chan wire.Chunk, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("catalog service: contentless mutation terminal: %w", context.DeadlineExceeded)
	case chunk, ok := <-chunks:
		if !ok {
			return fmt.Errorf("%w: contentless mutation ended without terminal framing", catalog.ErrIntegrity)
		}
		if !chunk.End || len(chunk.Payload) != 0 {
			return fmt.Errorf("%w: contentless mutation carried payload or nonterminal framing", catalog.ErrInvalidObject)
		}
		return nil
	}
}

func mutationStageFailure(ctx context.Context, stage MutationStage, cause error) (any, error) {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationStageCleanupTimeout)
	abortErr := stage.Abort(abortCtx)
	cancel()
	if abortErr != nil {
		cause = fmt.Errorf("catalog service: abandon failed mutation stage after %v: %w", cause, abortErr)
	}
	code, message := applicationError(cause)
	return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
}

func (s *Server) handlePrepareTenant(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.PrepareTenantRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.PrepareTenantResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, _, identity, err := s.authorize(ctx, request, catalogproto.OperationTenantPrepare, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.PrepareTenantResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	proof, err := s.core.Preparation.PrepareTenant(ctx, identity, tenant, input)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.PrepareTenantResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.PrepareTenantResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Proof: &proof})
}

func (s *Server) handleAckActivation(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.AckActivationRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.AckActivationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationActivationAck, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.AckActivationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if !authorization.Route.Forwarded || authorization.Route.Domain != input.DomainID {
		return encoded(catalogproto.AckActivationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: "acknowledged domain does not match broker binding"})
	}
	if err := s.fileProvider.Activations.AckActivation(ctx, identity, tenant, input); err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.AckActivationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.AckActivationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk})
}

func (s *Server) authorize(ctx context.Context, request wire.Request, operation catalogproto.Operation, generation catalog.Generation, tenantRequired bool) (catalog.TenantID, Authorization, Identity, error) {
	identity := Identity{Peer: request.Peer, WireBuild: request.WireBuild, Session: request.Session}
	if identity.Session == nil {
		return "", Authorization{}, identity, errors.New("catalog service: authenticated session is missing")
	}
	var tenant catalog.TenantID
	if tenantRequired {
		parsed, err := catalog.NewTenantID(request.Tenant)
		if err != nil {
			return "", Authorization{}, identity, err
		}
		tenant = parsed
		if generation == 0 {
			return "", Authorization{}, identity, errors.New("catalog service: generation is missing")
		}
	} else if request.Tenant != "" {
		return "", Authorization{}, identity, errors.New("catalog service: operation forbids a routing tenant")
	}
	route := Route{Tenant: tenant, Generation: generation}
	if forwarded, ok := ctx.Value(forwardedRouteKey{}).(Route); ok {
		if !tenantRequired || forwarded.Tenant != tenant || forwarded.Generation != generation {
			return "", Authorization{}, identity, errors.New("catalog service: request does not match broker binding")
		}
		route = forwarded
	}
	authorization, err := s.core.Authorizer.Authorize(ctx, identity, operation, route)
	if err != nil {
		return "", Authorization{}, identity, err
	}
	if authorization.Principal == "" {
		return "", Authorization{}, identity, errors.New("catalog service: authorizer returned an empty principal")
	}
	if authorization.Route != route {
		return "", Authorization{}, identity, errors.New("catalog service: authorizer returned a different route")
	}
	if err := validateAuthorization(authorization, operation); err != nil {
		return "", Authorization{}, identity, err
	}
	if authorization.Role == RoleFileProvider {
		fileProvider := s.fileProvider
		if fileProvider == nil {
			return "", Authorization{}, identity, errors.New("catalog service: File Provider capability is not registered")
		}
		if err := fileProvider.ProtectedPeer(ctx, identity.Peer); err != nil {
			return "", Authorization{}, identity, err
		}
	}
	return tenant, authorization, identity, nil
}

func validateAuthorization(authorization Authorization, operation catalogproto.Operation) error {
	switch authorization.Role {
	case RoleFileProvider:
		if !fileProviderOperation(operation) {
			return errors.New("catalog service: operation is not permitted for File Provider role")
		}
		if authorization.Presentation != catalog.PresentationFileProvider {
			return errors.New("catalog service: File Provider role has the wrong presentation")
		}
		if operation == catalogproto.OperationBrokerOpen {
			if authorization.Route != (Route{}) {
				return errors.New("catalog service: broker session carries a tenant route")
			}
			return nil
		}
		if !authorization.Route.Forwarded || authorization.Route.Domain == "" {
			return errors.New("catalog service: File Provider request lacks a broker-bound route")
		}
	case RoleMount:
		if !catalogPresentationOperation(operation) {
			return errors.New("catalog service: operation is not permitted for mount role")
		}
		if authorization.Presentation != catalog.PresentationMount {
			return errors.New("catalog service: mount role has the wrong presentation")
		}
		if authorization.Route.Forwarded || authorization.Route.Domain != "" {
			return errors.New("catalog service: mount request carries a broker-bound route")
		}
	case RoleTenantOwner:
		if operation != catalogproto.OperationTenantPrepare ||
			authorization.Route.Forwarded || authorization.Route.Domain != "" || authorization.Presentation != 0 {
			return errors.New("catalog service: tenant owner authorization is inconsistent")
		}
	case RoleProductAdmin:
		if operation != catalogproto.OperationSourceAuthorityPublishDesiredFleet &&
			operation != catalogproto.OperationSourceAuthorityReadDesiredFleet ||
			authorization.Route != (Route{}) || authorization.Presentation != 0 {
			return errors.New("catalog service: product admin authorization is inconsistent")
		}
	default:
		return errors.New("catalog service: authorizer returned an unknown role")
	}
	return nil
}

func fileProviderOperation(operation catalogproto.Operation) bool {
	return operation == catalogproto.OperationBrokerOpen ||
		operation == catalogproto.OperationActivationAck ||
		catalogPresentationOperation(operation)
}

func catalogPresentationOperation(operation catalogproto.Operation) bool {
	switch operation {
	case catalogproto.OperationCatalogRoot,
		catalogproto.OperationCatalogHead,
		catalogproto.OperationCatalogSnapshot,
		catalogproto.OperationCatalogChangesSince,
		catalogproto.OperationCatalogLookup,
		catalogproto.OperationCatalogLookupName,
		catalogproto.OperationCatalogOpenAt,
		catalogproto.OperationCatalogMutate:
		return true
	default:
		return false
	}
}

func streamContent(ctx context.Context, content io.ReadCloser, object catalogproto.CatalogObject, chunks chan<- []byte, terminal *json.RawMessage) {
	defer close(chunks)
	closed := false
	closeContent := func() error {
		if closed {
			return nil
		}
		closed = true
		return content.Close()
	}
	defer func() { _ = closeContent() }()
	buffer := make([]byte, streamBufferSize)
	for {
		count, err := content.Read(buffer)
		if count > 0 {
			chunk := append([]byte(nil), buffer[:count]...)
			select {
			case chunks <- chunk:
			case <-ctx.Done():
				cause := errors.Join(ctx.Err(), closeContent())
				code, message := applicationError(cause)
				*terminal = mustEncode(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: code, Message: message})
				return
			}
		}
		if errors.Is(err, io.EOF) {
			if closeErr := closeContent(); closeErr != nil {
				code, message := applicationError(closeErr)
				*terminal = mustEncode(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: code, Message: message})
				return
			}
			*terminal = mustEncode(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Object: &object})
			return
		}
		if err != nil {
			cause := errors.Join(err, closeContent())
			code, message := applicationError(cause)
			*terminal = mustEncode(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: code, Message: message})
			return
		}
		if count == 0 {
			cause := errors.Join(errors.New("content reader made no progress"), closeContent())
			*terminal = mustEncode(catalogproto.OpenAtResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity,
				Message: boundedErrorMessage(cause.Error()),
			})
			return
		}
	}
}

func emptyStream(response catalogproto.OpenAtResponse) (any, error) {
	payload, err := catalogproto.Encode(response)
	if err != nil {
		return nil, err
	}
	chunks := make(chan []byte)
	close(chunks)
	raw := json.RawMessage(payload)
	return wire.StreamResponse{Chunks: chunks, Value: &raw}, nil
}

func encoded(value any) (any, error) {
	payload, err := catalogproto.Encode(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(payload), nil
}

func mustEncode(value any) json.RawMessage {
	payload, err := catalogproto.Encode(value)
	if err != nil {
		panic(err)
	}
	return json.RawMessage(payload)
}

func applicationError(err error) (catalogproto.ErrorCode, string) {
	var coded *CodedError
	if errors.As(err, &coded) {
		switch coded.Code {
		case catalogproto.ErrorCodeInvalidRequest, catalogproto.ErrorCodeStaleAnchor, catalogproto.ErrorCodeNotFound,
			catalogproto.ErrorCodeConflict, catalogproto.ErrorCodeQuarantined, catalogproto.ErrorCodeIntegrity,
			catalogproto.ErrorCodeUnavailable:
			return coded.Code, boundedErrorMessage(coded.Error())
		default:
			return catalogproto.ErrorCodeUnavailable, boundedErrorMessage(coded.Error())
		}
	}
	message := boundedErrorMessage(err.Error())
	var stale *catalog.StaleAnchorError
	switch {
	case errors.As(err, &stale):
		return catalogproto.ErrorCodeStaleAnchor, message
	case errors.Is(err, catalog.ErrNotFound), errors.Is(err, catalog.ErrStateNotFound):
		return catalogproto.ErrorCodeNotFound, message
	case errors.Is(err, catalog.ErrInvalidObject):
		return catalogproto.ErrorCodeInvalidRequest, message
	case errors.Is(err, catalog.ErrConflict), errors.Is(err, catalog.ErrMutationConflict), errors.Is(err, catalog.ErrStateConflict),
		errors.Is(err, catalog.ErrMutationActive), errors.Is(err, catalog.ErrMutationClaimed), errors.Is(err, catalog.ErrGenerationMismatch),
		errors.Is(err, catalog.ErrSourcePredecessor), errors.Is(err, catalog.ErrSourceRequiresSnapshot):
		return catalogproto.ErrorCodeConflict, message
	case errors.Is(err, ErrQuarantined):
		return catalogproto.ErrorCodeQuarantined, message
	case errors.Is(err, catalog.ErrIntegrity):
		return catalogproto.ErrorCodeIntegrity, message
	default:
		return catalogproto.ErrorCodeUnavailable, message
	}
}

func boundedErrorMessage(message string) string {
	message = strings.ToValidUTF8(message, "\uFFFD")
	if len(message) <= remoteErrorMessageBytes {
		return message
	}
	end := remoteErrorMessageBytes - len("...")
	for end > 0 && !utf8.RuneStart(message[end]) {
		end--
	}
	return message[:end] + "..."
}

func protocolOptionalObjectID(id *catalog.ObjectID) *catalogproto.ObjectID {
	if id == nil {
		return nil
	}
	return protocolObjectID(*id)
}

type chunkReader struct {
	ctx       context.Context
	chunks    <-chan wire.Chunk
	closed    chan struct{}
	settled   chan struct{}
	settle    sync.Once
	settleErr error
	current   []byte
	ended     bool
	exhausted atomic.Bool
}

func (r *chunkReader) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	for len(r.current) == 0 {
		if r.ended {
			r.exhausted.Store(true)
			return 0, io.EOF
		}
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case <-r.closed:
			return 0, errors.New("catalog service: mutation content source closed")
		case chunk, ok := <-r.chunks:
			if !ok {
				return 0, errors.New("catalog service: mutation content ended without terminal framing")
			}
			if len(chunk.Payload) > streamBufferSize {
				return 0, fmt.Errorf("%w: mutation content chunk exceeds limit", catalog.ErrInvalidObject)
			}
			if len(chunk.Payload) == 0 && !chunk.End {
				return 0, fmt.Errorf("%w: mutation content carried an empty nonterminal chunk", catalog.ErrInvalidObject)
			}
			r.current = chunk.Payload
			r.ended = chunk.End
		}
	}
	count := copy(buffer, r.current)
	r.current = r.current[count:]
	return count, nil
}

func (r *chunkReader) Settle(cause error) error {
	r.settle.Do(func() {
		if cause == nil && !r.exhausted.Load() {
			r.settleErr = fmt.Errorf("%w: mutation content source settled before EOF", catalog.ErrIntegrity)
		}
		close(r.closed)
		close(r.settled)
	})
	return r.settleErr
}

func (r *chunkReader) Wait(ctx context.Context) error {
	var waitErr error
	select {
	case <-r.settled:
	case <-ctx.Done():
		waitErr = ctx.Err()
		_ = r.Settle(ctx.Err())
	}
	<-r.settled
	return errors.Join(waitErr, r.settleErr)
}
