package catalogservice

import (
	"context"
	"errors"
	"io"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
)

var ErrQuarantined = errors.New("catalog service: tenant quarantined")

// Identity is the exact authenticated daemonkit session identity.
type Identity struct {
	Peer    wire.Peer
	Build   string
	Session *wire.AcceptedSession
}

// Role is one authenticated FuseKit consumer role.
type Role uint8

const (
	// RoleFileProvider is the signed File Provider broker role.
	RoleFileProvider Role = iota + 1
	// RoleMount is the signed mount-holder role.
	RoleMount
	// RoleSourcePublisher is the authenticated owner of one source authority.
	RoleSourcePublisher
	// RoleTenantOwner is the product daemon that owns tenant preparation.
	RoleTenantOwner
)

// Authorization names the stable authenticated application principal.
type Authorization struct {
	Principal       string
	Role            Role
	Presentation    catalog.Presentation
	Route           Route
	SourceAuthority causal.SourceAuthorityID
}

// Route is the exact generation-fenced tenant origin authenticated for one request.
type Route struct {
	Tenant     catalog.TenantID
	Generation catalog.Generation
	Domain     catalogproto.DomainID
	Forwarded  bool
}

// Authorizer admits one operation for an exact authenticated session and routing tenant.
type Authorizer interface {
	Authorize(context.Context, Identity, catalogproto.Operation, Route) (Authorization, error)
}

// Reader exposes metadata-only catalog queries and exact-revision content opens.
type Reader interface {
	Root(context.Context, Authorization, catalog.TenantID) (catalog.Object, error)
	Head(context.Context, Authorization, catalog.TenantID) (catalog.Revision, error)
	Snapshot(context.Context, Authorization, catalog.TenantID, catalog.EnumerationScope, catalog.Revision, catalog.SnapshotCursor, int) (catalog.SnapshotPage, error)
	ChangesSince(context.Context, Authorization, catalog.TenantID, catalog.EnumerationScope, catalog.ChangeCursor, int) (catalog.ChangePage, error)
	Lookup(context.Context, Authorization, catalog.TenantID, catalog.ObjectID) (catalog.Object, error)
	LookupName(context.Context, Authorization, catalog.TenantID, catalog.ObjectID, string) (catalog.Object, error)
	OpenAt(context.Context, Authorization, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision) (OpenResult, error)
}

// OpenResult is one exact immutable object revision and its content stream.
type OpenResult struct {
	Object  catalog.Object
	Content io.ReadCloser
}

// MutationStage identifies content durably staged exactly once for one request.
type MutationStage struct {
	Token       string
	OperationID catalog.MutationID
	Tenant      catalog.TenantID
	Generation  catalog.Generation
	Size        int64

	content       *catalog.ContentRef
	authorization Authorization
}

func (s MutationStage) release() {}

// MutationSubmission is one validated closed mutation and its staged byte ownership.
type MutationSubmission struct {
	Request catalogproto.MutationRequest
	Stage   MutationStage
}

// MutationResult is the committed catalog identity outcome.
type MutationResult struct {
	OperationID catalog.MutationID
	Revision    catalog.Revision
	PrimaryID   *catalog.ObjectID
	SecondaryID *catalog.ObjectID
}

// MutationService owns durable byte staging and closed mutation submission.
type MutationService interface {
	StageMutation(context.Context, Identity, Authorization, catalog.TenantID, catalog.MutationID, catalog.Generation, bool, io.Reader) (MutationStage, error)
	SubmitMutation(context.Context, Identity, Authorization, MutationSubmission) (MutationResult, error)
}

// SourceSubmission is one completely staged authority publication.
type SourceSubmission struct {
	Request       catalogproto.SourceReconcileRequest
	Tenants       []catalog.SourceTenant
	authorization Authorization
}

// SourceObjectInput pairs one authoritative object record with its exact file bytes.
type SourceObjectInput struct {
	Record  catalogproto.SourceObjectRecord
	Content io.Reader
}

// SourceTenantInput is one ordered tenant record and its exact streamed records.
type SourceTenantInput struct {
	Record  catalogproto.SourceTenantRecord
	Objects []SourceObjectInput
	Deletes []catalogproto.SourceDeleteRecord
}

// SourcePublicationService stages source objects and commits complete publications.
type SourcePublicationService interface {
	StageSourceObject(context.Context, Identity, Authorization, catalogproto.SourceReconcileRequest, catalogproto.SourceTenantRecord, catalogproto.SourceObjectRecord, io.Reader) (catalog.SourceObject, error)
	DiscardSource(context.Context, Identity, Authorization, []catalog.SourceTenant) error
	ApplySource(context.Context, Identity, Authorization, SourceSubmission) (catalog.SourceResult, error)
}

// PreparationService prepares one exact tenant generation and revision.
type PreparationService interface {
	PrepareTenant(context.Context, Identity, catalog.TenantID, catalogproto.PrepareTenantRequest) (catalogproto.PreparationProof, error)
}

// ConvergenceService accepts exact post-enumeration convergence proofs.
type ConvergenceService interface {
	AckConvergence(context.Context, Identity, catalog.TenantID, catalogproto.AckConvergenceRequest) (catalogproto.DomainObservation, error)
}

// BrokerSession is one authenticated broker command stream.
type BrokerSession interface {
	Commands() <-chan catalogproto.BrokerCommand
	AcceptResult(context.Context, catalogproto.BrokerResult) error
	Close(error)
}

// BrokerService opens one broker session after prior sessions for its principal settle.
type BrokerService interface {
	OpenBroker(context.Context, Identity, string) (BrokerSession, error)
}

// CodedError carries a stable application error code without using daemonkit terminal text.
type CodedError struct {
	Code    catalogproto.ErrorCode
	Message string
	Cause   error
}

// Error implements error.
func (e *CodedError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Cause == nil {
		return "catalog service: coded error"
	}
	return e.Cause.Error()
}

// Unwrap returns the underlying cause.
func (e *CodedError) Unwrap() error { return e.Cause }
