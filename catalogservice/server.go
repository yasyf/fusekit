package catalogservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/contentstream"
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
	Leases       FileProviderLeaseStore
	SourceFleets SourceFleetService
	Authorizer   Authorizer
}

// FileProviderConfig supplies the services required only by File Provider.
type FileProviderConfig struct {
	Activations     ActivationService
	Broker          BrokerService
	Materialization MaterializationService
	CriticalFetches CriticalFetchService
	// ProtectedPeer verifies a signed File Provider broker after the product
	// authorizer has selected the closed File Provider role.
	ProtectedPeer func(context.Context, daemonkit.Caller) error
}

// Routes fixes the product's exact catalog capabilities before the daemon runtime begins.
type Routes struct {
	FileProvider bool
}

// Resolver selects the generation-local service exclusively through the admitted
// request's PublicationSlot.Value token. Zero, stale, and current-generation
// fallback resolution must fail.
type Resolver func(daemonkit.Request) (*Server, error)

// Server binds the catalog application protocol exclusively to daemonkit wire.
type Server struct {
	core CoreConfig

	fileProvider *FileProviderConfig

	brokerMu sync.Mutex
	brokers  map[string]*brokerInstance

	sessionMu sync.Mutex
	sessions  map[uint64]*sessionState
}

// routingTenantKey carries the envelope's routing tenant to authorize. v0.21
// reserves the wire tenant header, so the tenant rides a fusekit-owned request
// envelope instead.
type routingTenantKey struct{}

type requestEnvelope struct {
	Tenant  string          `json:"tenant,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

// New validates and constructs one generation-local catalog service.
func New(core CoreConfig, fileProvider *FileProviderConfig) (*Server, error) {
	if core.Reader == nil || core.Mutations == nil || core.Preparation == nil || core.Leases == nil || core.SourceFleets == nil || core.Authorizer == nil {
		return nil, errors.New("catalog service: every core service and the authorizer are required")
	}
	if fileProvider != nil {
		if fileProvider.Activations == nil || fileProvider.Broker == nil || fileProvider.Materialization == nil || fileProvider.CriticalFetches == nil || fileProvider.ProtectedPeer == nil {
			return nil, errors.New("catalog service: every File Provider service and protected-peer verifier are required")
		}
		copy := *fileProvider
		fileProvider = &copy
	}
	return &Server{
		core: core, fileProvider: fileProvider,
		brokers:  make(map[string]*brokerInstance),
		sessions: make(map[uint64]*sessionState),
	}, nil
}

type serviceHandler func(*Server, context.Context, daemonkit.Request) ([]byte, error)

type serviceRoute struct {
	operation    catalogproto.Operation
	handler      serviceHandler
	concurrent   bool
	fileProvider bool
}

// Register installs the fixed catalog route set before the daemon runtime begins.
func Register(routes Routes, resolve Resolver) ([]transportproto.HandlerSpec, error) {
	if resolve == nil {
		return nil, errors.New("catalog service: resolver is required")
	}
	registered := []serviceRoute{
		{catalogproto.OperationCatalogRoot, (*Server).handleRoot, true, false},
		{catalogproto.OperationCatalogHead, (*Server).handleHead, true, false},
		{catalogproto.OperationCatalogSnapshot, (*Server).handleSnapshot, true, false},
		{catalogproto.OperationCatalogChangesSince, (*Server).handleChangesSince, true, false},
		{catalogproto.OperationCatalogLookup, (*Server).handleLookup, true, false},
		{catalogproto.OperationCatalogLookupName, (*Server).handleLookupName, true, false},
		{catalogproto.OperationCatalogOpenAt, (*Server).handleOpenAt, true, false},
		{catalogproto.OperationCatalogRead, (*Server).handleRead, true, false},
		{catalogproto.OperationCatalogClose, (*Server).handleClose, true, false},
		{catalogproto.OperationCatalogMutateBegin, (*Server).handleMutation, true, false},
		{catalogproto.OperationCatalogMutateChunk, (*Server).handleMutationChunk, true, false},
		{catalogproto.OperationCatalogMutateCommit, (*Server).handleMutationCommit, true, false},
		{catalogproto.OperationTenantPrepare, (*Server).handlePrepareTenant, true, false},
		{catalogproto.OperationPresentationLeaseCommit, (*Server).handleCommitFileProviderLease, true, false},
		{catalogproto.OperationPresentationLeaseRenew, (*Server).handleRenewFileProviderLease, true, false},
		{catalogproto.OperationPresentationLeaseRelease, (*Server).handleReleaseFileProviderLease, true, false},
		{catalogproto.OperationSourceAuthorityPublishDesiredFleet, (*Server).handlePublishDesiredSourceFleet, true, false},
		{catalogproto.OperationSourceAuthorityReadDesiredFleet, (*Server).handleReadDesiredSourceFleet, true, false},
	}
	if routes.FileProvider {
		registered = append(registered,
			serviceRoute{catalogproto.OperationCatalogLookupPrivate, (*Server).handleLookupPrivate, true, true},
			serviceRoute{catalogproto.OperationCatalogOpenPrivate, (*Server).handleOpenPrivate, true, true},
			serviceRoute{catalogproto.OperationActivationAck, (*Server).handleAckActivation, true, true},
			serviceRoute{catalogproto.OperationActivationPoll, (*Server).handleActivationPoll, true, true},
			serviceRoute{catalogproto.OperationBrokerPoll, (*Server).handleBrokerPoll, true, true},
			serviceRoute{catalogproto.OperationBrokerResult, (*Server).handleBrokerResult, true, true},
			serviceRoute{catalogproto.OperationCriticalReadinessResolve, (*Server).handleResolveCriticalFetch, true, true},
			serviceRoute{catalogproto.OperationCriticalReadinessFetchAck, (*Server).handleAckCriticalFetch, true, true},
			serviceRoute{catalogproto.OperationMaterializationSnapshotBegin, (*Server).handleBeginMaterializationSnapshot, true, true},
			serviceRoute{catalogproto.OperationMaterializationSnapshotSuspend, (*Server).handleSuspendMaterializationSnapshot, true, true},
			serviceRoute{catalogproto.OperationMaterializationSnapshotStagePage, (*Server).handleStageMaterializationSnapshotPage, true, true},
			serviceRoute{catalogproto.OperationMaterializationSnapshotCommit, (*Server).handleCommitMaterializationSnapshot, true, true},
		)
	}
	specs := make([]transportproto.HandlerSpec, 0, len(registered))
	for _, route := range registered {
		specs = append(specs, transportproto.HandlerSpec{
			Op: string(route.operation), Concurrent: route.concurrent,
			Handler: resolvedHandler(resolve, route.fileProvider, route.handler),
		})
	}
	return specs, nil
}

func resolvedHandler(resolve Resolver, fileProvider bool, handler serviceHandler) transportproto.Handler {
	return func(ctx context.Context, request daemonkit.Request) ([]byte, error) {
		var envelope requestEnvelope
		if err := json.Unmarshal(request.Body, &envelope); err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, routingTenantKey{}, envelope.Tenant)
		request.Body = envelope.Payload
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

func (s *Server) handleRoot(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.RootRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
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

func (s *Server) handleHead(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.HeadRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
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

func (s *Server) handleSnapshot(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.SnapshotRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
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

func (s *Server) handleChangesSince(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.ChangesSinceRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
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

func (s *Server) handleLookup(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.LookupRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
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

func (s *Server) handleLookupPrivate(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.LookupPrivateRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
		return encoded(catalogproto.LookupPrivateResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationCatalogLookupPrivate, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupPrivateResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	id, err := catalogObjectID(input.ObjectID)
	if err != nil {
		return encoded(catalogproto.LookupPrivateResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	result, err := s.core.Mutations.LookupPrivate(ctx, identity, authorization, tenant, id)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupPrivateResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	converted, err := protocolPrivateMutationResult(result)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupPrivateResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.LookupPrivateResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Result: &converted})
}

func (s *Server) handleLookupName(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.LookupNameRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
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

func (s *Server) handleOpenAt(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.OpenAtRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
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
	handle, err := s.bindSession(request.Session).openHandle(namespaceContentHandle(opened.Content))
	if err != nil {
		code, message := applicationError(errors.Join(err, opened.Content.Close()))
		return emptyStream(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.OpenAtResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Object: &object, Handle: &handle,
	})
}

func (s *Server) handleOpenPrivate(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.OpenPrivateRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
		return emptyPrivateStream(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationCatalogOpenPrivate, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return emptyPrivateStream(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	id, err := catalogObjectID(input.ObjectID)
	if err != nil {
		return emptyPrivateStream(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	creator, err := catalog.ParseMutationID(string(input.Creator))
	if err != nil {
		return emptyPrivateStream(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	opened, err := s.core.Mutations.OpenPrivate(ctx, identity, authorization, tenant, catalog.Generation(input.Generation), id, creator)
	if err != nil {
		code, message := applicationError(err)
		return emptyPrivateStream(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if opened.Content == nil || opened.Result.ObjectID != id || opened.Result.Mutation != creator {
		cause := fmt.Errorf("%w: private open returned the wrong capability", catalog.ErrIntegrity)
		code, message := applicationError(settlePrivateOpenSource(ctx, opened.Content, cause))
		return emptyPrivateStream(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	converted, err := protocolPrivateMutationResult(opened.Result)
	if err != nil {
		code, message := applicationError(settlePrivateOpenSource(ctx, opened.Content, err))
		return emptyPrivateStream(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	handle, err := s.bindSession(request.Session).openHandle(privateContentHandle(opened.Content))
	if err != nil {
		code, message := applicationError(settlePrivateOpenSource(ctx, opened.Content, err))
		return emptyPrivateStream(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.OpenPrivateResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Result: &converted, Handle: &handle,
	})
}

func (s *Server) handleMutation(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.MutationRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationCatalogMutateBegin, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if err := validatePrivateMutationAuthorization(authorization, tenant, input); err != nil {
		return beginMutationFailure(input.HasContent, err)
	}
	if input.HasContent {
		upload, err := newMutationUpload()
		if err != nil {
			return beginMutationFailure(true, err)
		}
		pending := &pendingMutation{
			input: input, tenant: tenant, generation: catalog.Generation(input.Generation),
			identity: identity, authorization: authorization, upload: upload,
		}
		if err := s.bindSession(request.Session).beginUpload(pending); err != nil {
			_ = upload.close()
			return beginMutationFailure(true, err)
		}
		return encoded(catalogproto.BeginMutationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk})
	}
	return s.stageAndSubmitMutation(ctx, identity, authorization, tenant, input, nil, 0)
}

func beginMutationFailure(hasContent bool, err error) ([]byte, error) {
	code, message := applicationError(err)
	if hasContent {
		return encoded(catalogproto.BeginMutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
}

func (s *Server) handleRead(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.ReadRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
		return encoded(catalogproto.ReadResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	if err := forbidRoutingTenant(ctx); err != nil {
		return encoded(catalogproto.ReadResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	handle, err := s.bindSession(request.Session).handle(input.Handle)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ReadResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	data, eof, err := handle.page(ctx, input.Offset, input.Limit)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ReadResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.ReadResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Data: data, EOF: eof})
}

func (s *Server) handleClose(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.CloseRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
		return encoded(catalogproto.CloseResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	if err := forbidRoutingTenant(ctx); err != nil {
		return encoded(catalogproto.CloseResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	handle, err := s.bindSession(request.Session).takeHandle(input.Handle)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.CloseResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	handle.release(ctx)
	return encoded(catalogproto.CloseResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk})
}

func (s *Server) handleMutationChunk(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.MutationChunkRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
		return encoded(catalogproto.MutationChunkResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	if err := forbidRoutingTenant(ctx); err != nil {
		return encoded(catalogproto.MutationChunkResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	if err := s.bindSession(request.Session).stageChunk(input.RequestID, input.Sequence, input.Payload); err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.MutationChunkResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.MutationChunkResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk})
}

func (s *Server) handleMutationCommit(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.CommitMutationRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	if err := forbidRoutingTenant(ctx); err != nil {
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	pending, err := s.bindSession(request.Session).takeUpload(input.RequestID)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	source, err := pending.upload.source(input.Total, input.Digest)
	if err != nil {
		_ = pending.upload.close()
		code, message := applicationError(err)
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return s.stageAndSubmitMutation(ctx, pending.identity, pending.authorization, pending.tenant, pending.input, source, input.Total)
}

func forbidRoutingTenant(ctx context.Context) error {
	if routing, _ := ctx.Value(routingTenantKey{}).(string); routing != "" {
		return errors.New("catalog service: operation forbids a routing tenant")
	}
	return nil
}

// stageAndSubmitMutation runs the staging and submission half shared by a
// contentless mutate-begin and a chunked upload's mutate-commit.
func (s *Server) stageAndSubmitMutation(
	ctx context.Context,
	identity Identity,
	authorization Authorization,
	tenant catalog.TenantID,
	input catalogproto.MutationRequest,
	source contentstream.Source,
	total uint64,
) ([]byte, error) {
	generation := catalog.Generation(input.Generation)
	stage, err := s.core.Mutations.StageMutation(ctx, identity, authorization, tenant, input.RequestID, generation, input.HasContent, source)
	if source != nil {
		settleErr := source.Settle(err)
		waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationStageCleanupTimeout)
		waitErr := source.Wait(waitCtx)
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
	if stage.Token == "" ||
		stage.RequestID != input.RequestID || stage.Tenant != tenant ||
		stage.Generation != generation || stage.Size < 0 || !input.HasContent && stage.Size != 0 ||
		input.HasContent && stage.Size != int64(total) {
		return mutationStageFailure(ctx, stage, fmt.Errorf("%w: staged mutation identity or byte stream is inconsistent", catalog.ErrIntegrity))
	}
	result, err := s.core.Mutations.SubmitMutation(ctx, identity, authorization, MutationSubmission{Request: input, Stage: stage})
	if err != nil {
		return mutationStageFailure(ctx, stage, err)
	}
	if result.RequestID != input.RequestID || result.OperationID == (catalog.MutationID{}) || result.Revision == 0 {
		return mutationStageFailure(ctx, stage, fmt.Errorf("%w: mutation result identity is inconsistent", catalog.ErrIntegrity))
	}
	if err := validateMutationResultDisposition(input, result); err != nil {
		return mutationStageFailure(ctx, stage, err)
	}
	responseRequest := result.RequestID
	responseMutation := catalogproto.MutationID(result.OperationID.String())
	var private *catalogproto.PrivateMutationResult
	if result.Private != nil {
		if result.PrimaryID != nil || result.SecondaryID != nil {
			return mutationStageFailure(ctx, stage, fmt.Errorf("%w: private mutation returned namespace identities", catalog.ErrIntegrity))
		}
		converted, err := protocolPrivateMutationResult(*result.Private)
		if err != nil {
			return mutationStageFailure(ctx, stage, err)
		}
		if input.Kind == catalogproto.MutationKindCreate && converted.Creator != responseMutation {
			return mutationStageFailure(ctx, stage, fmt.Errorf("%w: private create returned the wrong creator", catalog.ErrIntegrity))
		}
		private = &converted
	}
	return encoded(catalogproto.MutationResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		RequestID: &responseRequest, MutationID: &responseMutation, Revision: uint64(result.Revision),
		PrimaryID: protocolOptionalObjectID(result.PrimaryID), SecondaryID: protocolOptionalObjectID(result.SecondaryID),
		Private: private,
	})
}

func validateMutationResultDisposition(request catalogproto.MutationRequest, result MutationResult) error {
	private := request.Disposition == catalogproto.MutationDispositionPrivateStaging
	if private {
		if result.Private == nil || result.PrimaryID != nil || result.SecondaryID != nil {
			return fmt.Errorf("%w: private staging mutation returned a namespace result", catalog.ErrIntegrity)
		}
		return nil
	}
	if result.Private != nil || result.PrimaryID == nil {
		return fmt.Errorf("%w: namespace mutation returned a private result", catalog.ErrIntegrity)
	}
	return nil
}

func mutationStageFailure(ctx context.Context, stage MutationStage, cause error) ([]byte, error) {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationStageCleanupTimeout)
	abortErr := stage.Abort(abortCtx)
	cancel()
	if abortErr != nil {
		cause = fmt.Errorf("catalog service: abandon failed mutation stage after %v: %w", cause, abortErr)
	}
	code, message := applicationError(cause)
	return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
}

func (s *Server) handlePrepareTenant(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.PrepareTenantRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
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

func (s *Server) handleAckActivation(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.AckActivationRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
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

func (s *Server) authorize(ctx context.Context, request daemonkit.Request, operation catalogproto.Operation, generation catalog.Generation, tenantRequired bool) (catalog.TenantID, Authorization, Identity, error) {
	identity := Identity{Caller: request.Caller, Session: request.Session}
	routing, _ := ctx.Value(routingTenantKey{}).(string)
	var tenant catalog.TenantID
	if tenantRequired {
		parsed, err := catalog.NewTenantID(routing)
		if err != nil {
			return "", Authorization{}, identity, err
		}
		tenant = parsed
		if generation == 0 {
			return "", Authorization{}, identity, errors.New("catalog service: generation is missing")
		}
	} else if routing != "" {
		return "", Authorization{}, identity, errors.New("catalog service: operation forbids a routing tenant")
	}
	route := Route{Tenant: tenant, Generation: generation}
	authorization, err := s.core.Authorizer.Authorize(ctx, identity, operation, route)
	if err != nil {
		return "", Authorization{}, identity, err
	}
	if authorization.Principal == "" {
		return "", Authorization{}, identity, errors.New("catalog service: authorizer returned an empty principal")
	}
	if authorization.Route != route {
		// The broker's forward wrap died with v0.20, so the domain binding a
		// File Provider request carries is asserted by the authorizer itself:
		// it may enrich the route with its session's broker-bound domain, but
		// never move the tenant or generation the request named.
		enriched := authorization.Role == RoleFileProvider &&
			authorization.Route.Tenant == route.Tenant &&
			authorization.Route.Generation == route.Generation
		if !enriched {
			return "", Authorization{}, identity, errors.New("catalog service: authorizer returned a different route")
		}
	}
	if err := validateAuthorization(authorization, operation); err != nil {
		return "", Authorization{}, identity, err
	}
	if authorization.Role == RoleFileProvider {
		fileProvider := s.fileProvider
		if fileProvider == nil {
			return "", Authorization{}, identity, errors.New("catalog service: File Provider capability is not registered")
		}
		if err := fileProvider.ProtectedPeer(ctx, identity.Caller); err != nil {
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
		if operation == catalogproto.OperationBrokerPoll || operation == catalogproto.OperationBrokerResult {
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
		if operation != catalogproto.OperationTenantPrepare &&
			operation != catalogproto.OperationPresentationLeaseCommit &&
			operation != catalogproto.OperationPresentationLeaseRenew &&
			operation != catalogproto.OperationPresentationLeaseRelease ||
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
	return operation == catalogproto.OperationCatalogLookupPrivate ||
		operation == catalogproto.OperationCatalogOpenPrivate ||
		operation == catalogproto.OperationActivationAck ||
		operation == catalogproto.OperationActivationPoll ||
		operation == catalogproto.OperationBrokerPoll ||
		operation == catalogproto.OperationBrokerResult ||
		operation == catalogproto.OperationCriticalReadinessResolve ||
		operation == catalogproto.OperationCriticalReadinessFetchAck ||
		operation == catalogproto.OperationMaterializationSnapshotBegin ||
		operation == catalogproto.OperationMaterializationSnapshotSuspend ||
		operation == catalogproto.OperationMaterializationSnapshotStagePage ||
		operation == catalogproto.OperationMaterializationSnapshotCommit ||
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
		catalogproto.OperationCatalogMutateBegin:
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

func streamPrivateContent(
	ctx context.Context,
	content contentstream.Source,
	result catalogproto.PrivateMutationResult,
	chunks chan<- []byte,
	terminal *json.RawMessage,
) {
	defer close(chunks)
	settled := false
	settle := func(cause error) error {
		if settled {
			return cause
		}
		settled = true
		settleErr := content.Settle(cause)
		waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationStageCleanupTimeout)
		waitErr := content.Wait(waitCtx)
		cancel()
		return errors.Join(cause, settleErr, waitErr)
	}
	defer func() { _ = settle(errors.New("catalog service: private content stream abandoned")) }()
	buffer := make([]byte, streamBufferSize)
	for {
		count, err := content.Read(buffer)
		if count > 0 {
			chunk := append([]byte(nil), buffer[:count]...)
			select {
			case chunks <- chunk:
			case <-ctx.Done():
				cause := settle(ctx.Err())
				code, message := applicationError(cause)
				*terminal = mustEncode(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: code, Message: message})
				return
			}
		}
		if errors.Is(err, io.EOF) {
			if settleErr := settle(nil); settleErr != nil {
				code, message := applicationError(settleErr)
				*terminal = mustEncode(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: code, Message: message})
				return
			}
			*terminal = mustEncode(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Result: &result})
			return
		}
		if err != nil {
			cause := settle(err)
			code, message := applicationError(cause)
			*terminal = mustEncode(catalogproto.OpenPrivateResponse{Protocol: catalogproto.Version, Code: code, Message: message})
			return
		}
		if count == 0 {
			cause := settle(errors.New("content reader made no progress"))
			*terminal = mustEncode(catalogproto.OpenPrivateResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity,
				Message: boundedErrorMessage(cause.Error()),
			})
			return
		}
	}
}

func settlePrivateOpenSource(ctx context.Context, source contentstream.Source, cause error) error {
	if source == nil {
		return cause
	}
	settleErr := source.Settle(cause)
	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationStageCleanupTimeout)
	waitErr := source.Wait(waitCtx)
	cancel()
	return errors.Join(cause, settleErr, waitErr)
}

func emptyStream(response catalogproto.OpenAtResponse) ([]byte, error) {
	return catalogproto.Encode(response)
}

func emptyPrivateStream(response catalogproto.OpenPrivateResponse) ([]byte, error) {
	return catalogproto.Encode(response)
}

func encoded(value any) ([]byte, error) {
	return catalogproto.Encode(value)
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
