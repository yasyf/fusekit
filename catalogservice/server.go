package catalogservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/transportproto"
)

const streamBufferSize = 64 * 1024

// Config supplies every required catalog application service.
type Config struct {
	Reader      Reader
	Mutations   MutationService
	Sources     SourcePublicationService
	Preparation PreparationService
	Convergence ConvergenceService
	Broker      BrokerService
	Authorizer  Authorizer
}

// Server binds the catalog application protocol exclusively to daemonkit wire.
type Server struct {
	config Config

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
	case catalogproto.OperationTenantPrepare:
		return s.handlePrepareTenant(ctx, inner)
	case catalogproto.OperationConvergenceAck:
		return s.handleAckConvergence(ctx, inner)
	default:
		return nil, errors.New("catalog service: operation cannot be broker-forwarded")
	}
}

// Register installs all client-request operations on a daemonkit server.
func Register(server *wire.Server, config Config) (*Server, error) {
	if server == nil {
		return nil, errors.New("catalog service: daemonkit server is nil")
	}
	if server.Build != transportproto.Build {
		return nil, fmt.Errorf("catalog service: daemonkit build %q does not match transport suite %q", server.Build, transportproto.Build)
	}
	if config.Reader == nil || config.Mutations == nil || config.Sources == nil || config.Preparation == nil ||
		config.Convergence == nil || config.Broker == nil || config.Authorizer == nil {
		return nil, errors.New("catalog service: every service and authorizer is required")
	}
	service := &Server{config: config, brokers: make(map[string]*brokerSlot)}
	server.RegisterConcurrent(wire.Op(catalogproto.OperationCatalogRoot), service.handleRoot)
	server.RegisterConcurrent(wire.Op(catalogproto.OperationCatalogHead), service.handleHead)
	server.RegisterConcurrent(wire.Op(catalogproto.OperationCatalogSnapshot), service.handleSnapshot)
	server.RegisterConcurrent(wire.Op(catalogproto.OperationCatalogChangesSince), service.handleChangesSince)
	server.RegisterConcurrent(wire.Op(catalogproto.OperationCatalogLookup), service.handleLookup)
	server.RegisterConcurrent(wire.Op(catalogproto.OperationCatalogLookupName), service.handleLookupName)
	server.RegisterConcurrent(wire.Op(catalogproto.OperationCatalogOpenAt), service.handleOpenAt)
	server.RegisterConcurrent(wire.Op(catalogproto.OperationCatalogMutate), service.handleMutation)
	server.RegisterConcurrent(wire.Op(catalogproto.OperationSourceReconcile), service.handleSourceReconcile)
	server.RegisterConcurrent(wire.Op(catalogproto.OperationTenantPrepare), service.handlePrepareTenant)
	server.RegisterConcurrent(wire.Op(catalogproto.OperationConvergenceAck), service.handleAckConvergence)
	server.RegisterConcurrent(wire.Op(catalogproto.OperationBrokerForward), service.handleBrokerForward)
	server.RegisterControl(wire.Op(catalogproto.OperationBrokerOpen), service.handleBrokerOpen)
	return service, nil
}

func (s *Server) handleRoot(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.RootRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogRoot, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	object, err := s.config.Reader.Root(ctx, authorization, tenant)
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
		return encoded(catalogproto.HeadResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogHead, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.HeadResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	revision, err := s.config.Reader.Head(ctx, authorization, tenant)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.HeadResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.HeadResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Revision: uint64(revision)})
}

func (s *Server) handleSnapshot(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.SnapshotRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(), Objects: []catalogproto.CatalogObject{}})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogSnapshot, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: code, Message: message, Objects: []catalogproto.CatalogObject{}})
	}
	cursor := catalog.SnapshotCursor{}
	scope, err := catalogEnumerationScope(input.Scope)
	if err != nil {
		return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(), Objects: []catalogproto.CatalogObject{}})
	}
	if input.After != nil {
		after, err := catalogObjectID(*input.After)
		if err != nil {
			return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(), Objects: []catalogproto.CatalogObject{}})
		}
		cursor.After = &after
	}
	page, err := s.config.Reader.Snapshot(ctx, authorization, tenant, scope, catalog.Revision(input.Revision), cursor, int(input.Limit))
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: code, Message: message, Objects: []catalogproto.CatalogObject{}})
	}
	objects := make([]catalogproto.CatalogObject, 0, len(page.Objects))
	for _, object := range page.Objects {
		converted, err := protocolObject(object)
		if err != nil {
			code, message := applicationError(err)
			return encoded(catalogproto.SnapshotResponse{Protocol: catalogproto.Version, Code: code, Message: message, Objects: []catalogproto.CatalogObject{}})
		}
		objects = append(objects, converted)
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
		return encoded(catalogproto.ChangesSinceResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(), Changes: []catalogproto.Change{}})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogChangesSince, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ChangesSinceResponse{Protocol: catalogproto.Version, Code: code, Message: message, Changes: []catalogproto.Change{}})
	}
	scope, err := catalogEnumerationScope(input.Scope)
	if err != nil {
		return encoded(catalogproto.ChangesSinceResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error(), Changes: []catalogproto.Change{}})
	}
	page, err := s.config.Reader.ChangesSince(ctx, authorization, tenant, scope, catalog.ChangeCursor{
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
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogLookup, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	id, err := catalogObjectID(input.ObjectID)
	if err != nil {
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	object, err := s.config.Reader.Lookup(ctx, authorization, tenant, id)
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
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogLookupName, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	parent, err := catalogObjectID(input.ParentID)
	if err != nil {
		return encoded(catalogproto.LookupResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	object, err := s.config.Reader.LookupName(ctx, authorization, tenant, parent, input.Name)
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
		return emptyStream(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenant, authorization, _, err := s.authorize(ctx, request, catalogproto.OperationCatalogOpenAt, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return emptyStream(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	id, err := catalogObjectID(input.ObjectID)
	if err != nil {
		return emptyStream(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	opened, err := s.config.Reader.OpenAt(ctx, authorization, tenant, catalog.Generation(input.Generation), id, catalog.Revision(input.Revision))
	if err != nil {
		code, message := applicationError(err)
		return emptyStream(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if opened.Content == nil || opened.Object.ID != id || opened.Object.Revision != catalog.Revision(input.Revision) {
		if opened.Content != nil {
			_ = opened.Content.Close()
		}
		return emptyStream(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: "open returned the wrong immutable object revision"})
	}
	object, err := protocolObject(opened.Object)
	if err != nil {
		_ = opened.Content.Close()
		code, message := applicationError(err)
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
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenant, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationCatalogMutate, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	operationID, err := catalogMutationID(input.OperationID)
	if err != nil {
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	generation := catalog.Generation(input.Generation)
	stream := &chunkReader{ctx: ctx, chunks: request.Chunks}
	stage, err := s.config.Mutations.StageMutation(ctx, identity, authorization, tenant, operationID, generation, input.HasContent, stream)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	defer stage.release()
	if !stream.exhausted || stage.Token == "" || stage.OperationID != operationID || stage.Tenant != tenant || stage.Generation != generation || stage.Size < 0 || !input.HasContent && stage.Size != 0 {
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: "staged mutation identity or byte stream is inconsistent"})
	}
	result, err := s.config.Mutations.SubmitMutation(ctx, identity, authorization, MutationSubmission{Request: input, Stage: stage})
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if result.OperationID != operationID || result.Revision == 0 {
		return encoded(catalogproto.MutationResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: "mutation result identity is inconsistent"})
	}
	responseOperation := catalogproto.MutationID(result.OperationID.String())
	return encoded(catalogproto.MutationResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		OperationID: &responseOperation, Revision: uint64(result.Revision),
		PrimaryID: protocolOptionalObjectID(result.PrimaryID), SecondaryID: protocolOptionalObjectID(result.SecondaryID),
	})
}

func (s *Server) handleSourceReconcile(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.SourceReconcileRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	_, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationSourceReconcile, 0, false)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if authorization.SourceAuthority != causal.SourceAuthorityID(input.SourceAuthority) {
		return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: "source authority is not authorized"})
	}
	stream := &sourceInput{ctx: ctx, chunks: request.Chunks}
	tenants := make([]catalog.SourceTenant, 0, input.TenantCount)
	handedOff := false
	defer func() {
		if !handedOff {
			_ = s.config.Sources.DiscardSource(context.WithoutCancel(ctx), identity, authorization, tenants)
		}
	}()
	for range input.TenantCount {
		var tenantRecord catalogproto.SourceTenantRecord
		if err := stream.message(&tenantRecord); err != nil {
			return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
		}
		tenantID, err := catalog.NewTenantID(string(tenantRecord.TenantID))
		if err != nil {
			return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
		}
		tenants = append(tenants, catalog.SourceTenant{Tenant: tenantID, Generation: catalog.Generation(tenantRecord.Generation)})
		target := &tenants[len(tenants)-1]
		for range tenantRecord.ObjectCount {
			var objectRecord catalogproto.SourceObjectRecord
			if err := stream.message(&objectRecord); err != nil {
				return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
			}
			content := io.Reader(strings.NewReader(""))
			var streamed *sourceContentReader
			if objectRecord.Kind == catalogproto.ObjectKindFile {
				streamed = &sourceContentReader{input: stream, remaining: objectRecord.Size}
				content = streamed
			}
			object, err := s.config.Sources.StageSourceObject(ctx, identity, authorization, input, tenantRecord, objectRecord, content)
			if err != nil {
				code, message := applicationError(err)
				return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: code, Message: message})
			}
			target.Objects = append(target.Objects, object)
			if streamed != nil && (streamed.remaining != 0 || len(stream.current) != 0) {
				return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: "source service did not consume the exact content stream"})
			}
			if !sourceObjectMatchesRecord(object, objectRecord) {
				return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: "staged source object identity changed"})
			}
		}
		for range tenantRecord.DeleteCount {
			var deleted catalogproto.SourceDeleteRecord
			if err := stream.message(&deleted); err != nil {
				return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
			}
			target.Deletes = append(target.Deletes, catalog.SourceObjectKey(deleted.SourceKey))
		}
	}
	if err := stream.finish(); err != nil {
		return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	handedOff = true
	result, err := s.config.Sources.ApplySource(ctx, identity, authorization, SourceSubmission{
		Request: input, Tenants: tenants, authorization: authorization,
	})
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.SourceReconcileResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	response := catalogproto.SourceReconcileResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		SourceAuthority: catalogproto.SourceAuthorityID(result.Authority), SourceRevision: uint64(result.Revision),
		ChangeID:    catalogproto.ChangeID(fmt.Sprintf("%x", result.ChangeID[:])),
		OperationID: catalogproto.MutationID(fmt.Sprintf("%x", result.Operation[:])),
		Commits:     make([]catalogproto.SourceCommit, len(result.Commits)),
	}
	for index, commit := range result.Commits {
		response.Commits[index] = catalogproto.SourceCommit{TenantID: catalogproto.TenantID(commit.Tenant), CatalogRevision: uint64(commit.CatalogRevision)}
	}
	return encoded(response)
}

func sourceObjectMatchesRecord(object catalog.SourceObject, record catalogproto.SourceObjectRecord) bool {
	kind := catalog.KindDirectory
	if record.Kind == catalogproto.ObjectKindFile {
		kind = catalog.KindFile
	}
	if object.Key != catalog.SourceObjectKey(record.SourceKey) || object.Parent != catalog.SourceObjectKey(record.ParentKey) ||
		object.Name != record.Name || object.Kind != kind || object.Mode != record.Mode ||
		object.ContentRevision != catalog.Revision(record.ContentRevision) ||
		object.Visibility != (catalog.Visibility{Mount: record.MountVisible, FileProvider: record.FileProviderVisible}) {
		return false
	}
	if kind == catalog.KindDirectory {
		return object.Content == (catalog.ContentRef{})
	}
	return object.Content.Stage != (catalog.StageID{}) && object.Content.Size >= 0 && uint64(object.Content.Size) == record.Size &&
		fmt.Sprintf("%x", object.Content.Hash[:]) == record.Hash
}

func (s *Server) handlePrepareTenant(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.PrepareTenantRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.PrepareTenantResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenant, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationTenantPrepare, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.PrepareTenantResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if authorization.Route.Forwarded && authorization.Route.Domain != input.DomainID {
		return encoded(catalogproto.PrepareTenantResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: "prepared domain does not match broker binding"})
	}
	proof, err := s.config.Preparation.PrepareTenant(ctx, identity, tenant, input)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.PrepareTenantResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.PrepareTenantResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Proof: &proof})
}

func (s *Server) handleAckConvergence(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.AckConvergenceRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.AckConvergenceResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: err.Error()})
	}
	tenant, authorization, identity, err := s.authorize(ctx, request, catalogproto.OperationConvergenceAck, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.AckConvergenceResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if authorization.Route.Forwarded && authorization.Route.Domain != input.DomainID {
		return encoded(catalogproto.AckConvergenceResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: "acknowledged domain does not match broker binding"})
	}
	observation, err := s.config.Convergence.AckConvergence(ctx, identity, tenant, input)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.AckConvergenceResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.AckConvergenceResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Observation: &observation})
}

func (s *Server) authorize(ctx context.Context, request wire.Request, operation catalogproto.Operation, generation catalog.Generation, tenantRequired bool) (catalog.TenantID, Authorization, Identity, error) {
	identity := Identity{Peer: request.Peer, Build: request.Build, Session: request.Session}
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
	authorization, err := s.config.Authorizer.Authorize(ctx, identity, operation, route)
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
	return tenant, authorization, identity, nil
}

func validateAuthorization(authorization Authorization, operation catalogproto.Operation) error {
	switch authorization.Role {
	case RoleFileProvider:
		if authorization.SourceAuthority != "" {
			return errors.New("catalog service: File Provider role carries a source authority")
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
		if authorization.SourceAuthority != "" {
			return errors.New("catalog service: mount role carries a source authority")
		}
		if authorization.Presentation != catalog.PresentationMount {
			return errors.New("catalog service: mount role has the wrong presentation")
		}
		if authorization.Route.Forwarded || authorization.Route.Domain != "" {
			return errors.New("catalog service: mount request carries a broker-bound route")
		}
	case RoleSourcePublisher:
		if operation != catalogproto.OperationSourceReconcile || authorization.Route != (Route{}) || authorization.Presentation != 0 || authorization.SourceAuthority == "" {
			return errors.New("catalog service: source publisher authorization is inconsistent")
		}
	default:
		return errors.New("catalog service: authorizer returned an unknown role")
	}
	return nil
}

func streamContent(ctx context.Context, content io.ReadCloser, object catalogproto.CatalogObject, chunks chan<- []byte, terminal *json.RawMessage) {
	defer close(chunks)
	defer func() { _ = content.Close() }()
	buffer := make([]byte, streamBufferSize)
	for {
		count, err := content.Read(buffer)
		if count > 0 {
			chunk := append([]byte(nil), buffer[:count]...)
			select {
			case chunks <- chunk:
			case <-ctx.Done():
				*terminal = mustEncode(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeUnavailable, Message: ctx.Err().Error()})
				return
			}
		}
		if errors.Is(err, io.EOF) {
			*terminal = mustEncode(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Object: &object})
			return
		}
		if err != nil {
			*terminal = mustEncode(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: err.Error()})
			return
		}
		if count == 0 {
			*terminal = mustEncode(catalogproto.OpenAtResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeIntegrity, Message: "content reader made no progress"})
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
			return coded.Code, coded.Error()
		default:
			return catalogproto.ErrorCodeUnavailable, coded.Error()
		}
	}
	var stale *catalog.StaleAnchorError
	switch {
	case errors.As(err, &stale):
		return catalogproto.ErrorCodeStaleAnchor, err.Error()
	case errors.Is(err, catalog.ErrNotFound), errors.Is(err, catalog.ErrStateNotFound):
		return catalogproto.ErrorCodeNotFound, err.Error()
	case errors.Is(err, catalog.ErrInvalidObject):
		return catalogproto.ErrorCodeInvalidRequest, err.Error()
	case errors.Is(err, catalog.ErrConflict), errors.Is(err, catalog.ErrMutationConflict), errors.Is(err, catalog.ErrStateConflict),
		errors.Is(err, catalog.ErrMutationActive), errors.Is(err, catalog.ErrMutationClaimed), errors.Is(err, catalog.ErrGenerationMismatch),
		errors.Is(err, catalog.ErrSourcePredecessor), errors.Is(err, catalog.ErrSourceRequiresSnapshot):
		return catalogproto.ErrorCodeConflict, err.Error()
	case errors.Is(err, ErrQuarantined):
		return catalogproto.ErrorCodeQuarantined, err.Error()
	case errors.Is(err, catalog.ErrIntegrity):
		return catalogproto.ErrorCodeIntegrity, err.Error()
	default:
		return catalogproto.ErrorCodeUnavailable, err.Error()
	}
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
	current   []byte
	ended     bool
	exhausted bool
}

type sourceInput struct {
	ctx     context.Context
	chunks  <-chan wire.Chunk
	current []byte
	ended   bool
}

func (r *sourceInput) message(destination any) error {
	if len(r.current) != 0 {
		return errors.New("catalog service: source content was not consumed")
	}
	chunk, err := r.next()
	if err != nil {
		return err
	}
	if chunk.End || len(chunk.Payload) == 0 {
		return errors.New("catalog service: source record stream ended early")
	}
	return catalogproto.Decode(chunk.Payload, destination)
}

func (r *sourceInput) finish() error {
	if len(r.current) != 0 {
		return errors.New("catalog service: source content was not consumed")
	}
	chunk, err := r.next()
	if err != nil {
		return err
	}
	if !chunk.End || len(chunk.Payload) != 0 {
		return errors.New("catalog service: source record stream has trailing input")
	}
	return nil
}

func (r *sourceInput) next() (wire.Chunk, error) {
	if r.ended {
		return wire.Chunk{}, errors.New("catalog service: source record stream already ended")
	}
	select {
	case <-r.ctx.Done():
		return wire.Chunk{}, r.ctx.Err()
	case chunk, ok := <-r.chunks:
		if !ok {
			return wire.Chunk{}, errors.New("catalog service: source record stream closed without end")
		}
		r.ended = chunk.End
		return chunk, nil
	}
}

type sourceContentReader struct {
	input     *sourceInput
	remaining uint64
}

func (r *sourceContentReader) Read(buffer []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if len(buffer) == 0 {
		return 0, nil
	}
	if len(r.input.current) == 0 {
		chunk, err := r.input.next()
		if err != nil {
			return 0, err
		}
		if chunk.End || len(chunk.Payload) == 0 || uint64(len(chunk.Payload)) > r.remaining {
			return 0, errors.New("catalog service: source content stream has the wrong size")
		}
		r.input.current = chunk.Payload
	}
	limit := len(buffer)
	if uint64(limit) > r.remaining {
		limit = int(r.remaining)
	}
	count := copy(buffer[:limit], r.input.current)
	r.input.current = r.input.current[count:]
	r.remaining -= uint64(count)
	return count, nil
}

func (r *chunkReader) Read(buffer []byte) (int, error) {
	for len(r.current) == 0 {
		if r.ended {
			r.exhausted = true
			return 0, io.EOF
		}
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case chunk, ok := <-r.chunks:
			if !ok {
				r.ended = true
				continue
			}
			r.current = chunk.Payload
			r.ended = chunk.End
		}
	}
	count := copy(buffer, r.current)
	r.current = r.current[count:]
	return count, nil
}
