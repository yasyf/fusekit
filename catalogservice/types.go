package catalogservice

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/contentstream"
)

var ErrQuarantined = errors.New("catalog service: tenant quarantined")

// ErrBrokerStreamAbsent means the holder socket is live but no authenticated
// signed-app broker stream became available before the caller's deadline.
var ErrBrokerStreamAbsent = errors.New("catalog service: signed broker stream absent")

// BrokerIdentity is the fixed signed product identity bound to every broker
// proof after the accepted peer passes the corresponding trust policy.
type BrokerIdentity struct {
	ProductBuild                string
	Executable                  string
	DesignatedRequirement       string
	EntitlementValidationDigest [32]byte
}

// Identity is the exact authenticated daemonkit session identity.
type Identity struct {
	Peer      wire.Peer
	WireBuild string
	Session   *wire.AcceptedSession
}

// Role is one authenticated FuseKit consumer role.
type Role uint8

const (
	// RoleFileProvider is the signed File Provider broker role.
	RoleFileProvider Role = iota + 1
	// RoleMount is the signed mount-holder role.
	RoleMount
	// RoleTenantOwner is the product daemon that owns tenant preparation.
	RoleTenantOwner
	// RoleProductAdmin publishes complete product-owned source authority fleets.
	RoleProductAdmin
)

// Authorization names the stable authenticated application principal.
type Authorization struct {
	Principal    string
	Role         Role
	Presentation catalog.Presentation
	Route        Route
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

// CatalogReadStore is the remote catalog surface required by presentation reads.
type CatalogReadStore interface {
	Tenant(context.Context, catalog.TenantID) (catalog.TenantMetadata, error)
	Root(context.Context, catalog.TenantID) (catalog.Object, error)
	Head(context.Context, catalog.TenantID) (catalog.Revision, error)
	Snapshot(context.Context, catalog.TenantID, catalog.EnumerationScope, catalog.Revision, catalog.SnapshotCursor, int) (catalog.SnapshotPage, error)
	ChangesSince(context.Context, catalog.TenantID, catalog.EnumerationScope, catalog.ChangeCursor, int) (catalog.ChangePage, error)
	Lookup(context.Context, catalog.TenantID, catalog.Presentation, catalog.ObjectID) (catalog.Object, error)
	LookupName(context.Context, catalog.TenantID, catalog.Presentation, catalog.ObjectID, string) (catalog.Object, error)
	OpenContentAt(context.Context, catalog.TenantID, catalog.Presentation, catalog.Generation, catalog.ObjectID, catalog.Revision) (catalog.Object, io.ReadCloser, error)
}

// CatalogMutationStore is the remote catalog surface required by presentation mutations.
type CatalogMutationStore interface {
	StageOwnedContent(context.Context, contentstream.Source) (catalog.ContentRef, error)
	ReleaseUnclaimedContent(context.Context, []catalog.ContentRef) error
	Inspect(context.Context, catalog.TenantID, catalog.ObjectID) (catalog.Object, error)
	BeginMutation(context.Context, catalog.TenantID, catalog.Revision, catalog.MutationIntent) (catalog.PreparedMutation, error)
	Mutation(context.Context, catalog.TenantID, catalog.MutationID) (catalog.MutationRecord, error)
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
	Token      string
	RequestID  catalogproto.MutationRequestID
	Tenant     catalog.TenantID
	Generation catalog.Generation
	Size       int64

	content       *catalog.ContentRef
	authorization Authorization
	state         *mutationStageState
}

type mutationStageState struct {
	mu      sync.Mutex
	claimed bool
	abort   func(context.Context) error
}

// Abort idempotently releases staged bytes unless BeginMutation claimed them.
func (s MutationStage) Abort(ctx context.Context) error {
	if s.state == nil {
		return nil
	}
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.claimed || s.state.abort == nil {
		return nil
	}
	err := s.state.abort(ctx)
	if err == nil {
		s.state.claimed = true
	}
	return err
}

func (s MutationStage) claim() {
	if s.state == nil {
		return
	}
	s.state.mu.Lock()
	s.state.claimed = true
	s.state.mu.Unlock()
}

// MutationSubmission is one validated closed mutation and its staged byte ownership.
type MutationSubmission struct {
	Request catalogproto.MutationRequest
	Stage   MutationStage
}

// MutationResult is the committed catalog identity outcome.
type MutationResult struct {
	RequestID   catalogproto.MutationRequestID
	OperationID catalog.MutationID
	Revision    catalog.Revision
	PrimaryID   *catalog.ObjectID
	SecondaryID *catalog.ObjectID
}

// MutationService owns durable byte staging and closed mutation submission.
type MutationService interface {
	StageMutation(context.Context, Identity, Authorization, catalog.TenantID, catalogproto.MutationRequestID, catalog.Generation, bool, contentstream.Source) (MutationStage, error)
	SubmitMutation(context.Context, Identity, Authorization, MutationSubmission) (MutationResult, error)
}

// PreparationService prepares one exact tenant generation from authoritative source state.
type PreparationService interface {
	PrepareTenant(context.Context, Identity, catalog.TenantID, catalogproto.PrepareTenantRequest) (catalogproto.TenantPreparationProof, error)
}

// SourceFleetService reads and atomically publishes complete product-owned source authority fleets.
type SourceFleetService interface {
	PublishDesiredSourceFleet(context.Context, catalog.PublishDesiredSourceFleetRequest) (catalog.DesiredSourceAuthorityFleetState, error)
	DesiredSourceFleetPage(context.Context, catalog.DesiredSourceFleetPageRequest) (catalog.DesiredSourceFleetPage, error)
}

// DomainPreparationService prepares one exact File Provider domain from a
// caller-supplied tenant preparation proof.
type DomainPreparationService interface {
	PrepareDomain(context.Context, Identity, catalog.TenantID, catalogproto.PrepareDomainRequest) (catalogproto.DomainObservation, error)
}

// ConvergenceService accepts exact post-enumeration convergence proofs.
type ConvergenceService interface {
	AckConvergence(context.Context, Identity, catalog.TenantID, catalogproto.AckConvergenceRequest) (catalogproto.DomainObservation, error)
}

// BrokerSession is one authenticated broker command stream.
type BrokerSession interface {
	Commands() <-chan catalogproto.BrokerCommand
	Done() <-chan struct{}
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
