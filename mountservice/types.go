// Package mountservice binds exact tenant lifecycle and native catalog operations
// to the generated mount protocol.
package mountservice

import (
	"context"
	"errors"

	"fmt"
	"github.com/yasyf/daemonkit/daemon"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/tenant"
)

var ErrUnauthorized = errors.New("mount service: unauthorized")

// Identity is the daemonkit-authenticated identity of one persistent session.
type Identity struct {
	Peer      wire.Peer
	WireBuild string
	Session   *wire.AcceptedSession
}

// ObservationIdentity is the immutable authenticated peer metadata for one read-only observation.
type ObservationIdentity struct {
	Peer      wire.Peer
	WireBuild string
}

// Authorizer derives the owning consumer from authenticated peer identity.
type Authorizer interface {
	AuthorizeObservation(context.Context, ObservationIdentity, mountproto.Operation) error
	Authorize(context.Context, Identity, mountproto.Operation, catalog.TenantID, catalog.Generation) (tenant.OwnerID, error)
	AuthorizeNative(context.Context, Identity, mountproto.Operation) error
}

// RuntimeHealth is the exact holder activation and presentation readiness state.
type RuntimeHealth struct {
	RuntimeBuild         string
	RuntimeProtocol      uint16
	RuntimePID           int64
	ProcessGeneration    string
	ActivationGeneration string
	State                mountproto.RuntimeState
	Draining             bool
	Busy                 bool
	Ready                bool
	ReadinessPhase       mountproto.ReadinessPhase
	ReadinessStep        mountproto.ReadinessStep
	NativePhase          mountproto.NativePhase
	NativeMount          *NativeMountProof
	BrokerPhase          mountproto.BrokerPhase
}

// RuntimeHealthProvider returns one atomic runtime-health observation.
type RuntimeHealthProvider interface {
	Health(context.Context, daemon.Publication) (RuntimeHealth, error)
}

// NativeMountProof proves one exact native mount identity and one causal root read.
type NativeMountProof struct {
	PresentationRoot string
	Filesystem       string
	Source           string
	RootReadEpoch    uint64
}

// NativeMountIdentity proves the exact mounted presentation before the holder
// drives the child-armed causal root probe through it.
type NativeMountIdentity struct {
	PresentationRoot string
	Filesystem       string
	Source           string
}

func protocolNativeMountIdentity(identity NativeMountIdentity) mountproto.NativeMountIdentity {
	return mountproto.NativeMountIdentity{
		PresentationRoot: identity.PresentationRoot,
		Filesystem:       identity.Filesystem,
		Source:           identity.Source,
	}
}

func nativeMountIdentity(identity mountproto.NativeMountIdentity) NativeMountIdentity {
	return NativeMountIdentity{
		PresentationRoot: identity.PresentationRoot,
		Filesystem:       identity.Filesystem,
		Source:           identity.Source,
	}
}

func protocolNativeMountProof(proof NativeMountProof) mountproto.NativeMountProof {
	return mountproto.NativeMountProof{
		PresentationRoot: proof.PresentationRoot,
		Filesystem:       proof.Filesystem,
		Source:           proof.Source,
		RootReadEpoch:    proof.RootReadEpoch,
	}
}

func nativeMountProof(proof mountproto.NativeMountProof) NativeMountProof {
	return NativeMountProof{
		PresentationRoot: proof.PresentationRoot,
		Filesystem:       proof.Filesystem,
		Source:           proof.Source,
		RootReadEpoch:    proof.RootReadEpoch,
	}
}

// Runtime is the exact-generation tenant lifecycle surface exposed over the wire.
type Runtime interface {
	ProvisionTenant(context.Context, tenant.TenantSpec) error
	ReplaceTenant(context.Context, catalog.Generation, tenant.TenantSpec) error
	RemoveTenant(context.Context, catalog.TenantID, catalog.Generation, tenant.OwnerID) error
	State(context.Context, catalog.TenantID, tenant.OwnerID) (tenant.TenantStatus, error)
}

// NativeRoute is one immutable mount-root binding exposed to the native child.
type NativeRoute struct {
	Name       string
	Tenant     catalog.TenantID
	Generation catalog.Generation
}

// NativeRoutePage is one version-fenced bounded route page.
type NativeRoutePage struct {
	Snapshot uint64
	Routes   []NativeRoute
	Next     string
}

// NativePin holds one exact tenant generation for a native callback handle.
type NativePin struct {
	Route   NativeRoute
	Spec    tenant.TenantSpec
	Release func() error
}

// NativeSessions owns authenticated child route snapshots and generation pins.
// Unbind publishes transport loss before catalog-and-pin settlement begins.
// Settled records that eventual result without retaining the process lane.
type NativeSessions interface {
	Bind(context.Context, Identity) error
	Mounted(context.Context, Identity, NativeMountIdentity, string) error
	Ready(context.Context, Identity, NativeMountProof) error
	Unbind(Identity)
	Settled(Identity, error)
	RoutePage(context.Context, uint64, string, int) (NativeRoutePage, error)
	Pin(context.Context, string) (NativePin, error)
}

// NativeCatalog owns all native snapshot and mutable staging handles inside one
// exact catalog worker generation. CloseSession must settle every owner operation
// and reap a poisoned worker before it returns, including on error.
type NativeCatalog interface {
	OpenSnapshot(context.Context, string, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision) (NativeHandle, error)
	ReadSnapshot(context.Context, string, string, int64, int) ([]byte, bool, error)
	CloseSnapshot(context.Context, string, string) error
	OpenWrite(context.Context, string, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision) (NativeHandle, error)
	ReadWrite(context.Context, string, string, int64, int) ([]byte, bool, error)
	Write(context.Context, string, string, int64, []byte) (int, error)
	Truncate(context.Context, string, string, int64) error
	Sync(context.Context, string, string) error
	CommitWrite(context.Context, string, string) (catalog.Object, catalog.MutationID, error)
	AbortWrite(context.Context, string, string) error
	CloseSession(context.Context, string) error
}

// NativeHandle identifies one worker-owned snapshot or mutable stage.
type NativeHandle struct {
	Token  string
	Object catalog.Object
}

// CodedError supplies a stable protocol error classification.
type CodedError struct {
	Code    mountproto.ErrorCode
	Message string
	Cause   error
}

// Error implements error.
func (e *CodedError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Cause.Error()
}

// Unwrap returns the underlying service failure.
func (e *CodedError) Unwrap() error { return e.Cause }

// RemoteError is one typed application failure returned by the server.
type RemoteError struct {
	Code    mountproto.ErrorCode
	Message string
}

// Error implements error.
func (e *RemoteError) Error() string { return e.Message }

// TransportError is one daemonkit delivery or terminal failure.
type TransportError struct {
	Outcome wire.Outcome
	Message string
	cause   error
}

// Error implements error.
func (e *TransportError) Error() string { return e.Message }

// Unwrap returns the typed daemonkit rejection when one is available.
func (e *TransportError) Unwrap() error { return e.cause }

func definitionSpec(owner tenant.OwnerID, id catalog.TenantID, definition mountproto.TenantDefinition) (tenant.TenantSpec, error) {
	presentations := catalog.PresentationSet(0)
	for _, presentation := range definition.Presentations {
		switch presentation {
		case mountproto.PresentationMount:
			presentations |= catalog.PresentMount
		case mountproto.PresentationFileProvider:
			presentations |= catalog.PresentFileProvider
		default:
			return tenant.TenantSpec{}, fmt.Errorf("mount service: presentation %q is invalid", presentation)
		}
	}
	var access tenant.AccessMode
	switch definition.AccessMode {
	case mountproto.AccessModeReadOnly:
		access = tenant.ReadOnly
	case mountproto.AccessModeReadWrite:
		access = tenant.ReadWrite
	default:
		return tenant.TenantSpec{}, fmt.Errorf("mount service: access mode %q is invalid", definition.AccessMode)
	}
	var casePolicy catalog.CasePolicy
	switch definition.CasePolicy {
	case mountproto.CasePolicySensitive:
		casePolicy = catalog.CaseSensitive
	case mountproto.CasePolicyInsensitive:
		casePolicy = catalog.CaseInsensitive
	default:
		return tenant.TenantSpec{}, fmt.Errorf("mount service: case policy %q is invalid", definition.CasePolicy)
	}
	var fileProvider tenant.FileProviderSpec
	if presentations.Has(catalog.PresentationFileProvider) {
		fileProvider = tenant.FileProviderSpec{
			Enabled:                true,
			PresentationInstanceID: definition.FileProviderPresentationInstanceID,
			DisplayName:            definition.FileProviderDisplayName,
		}
	}
	var mount tenant.MountSpec
	if definition.Mount != nil {
		mount = tenant.MountSpec{PresentationRoot: definition.Mount.PresentationRoot}
	}
	return tenant.TenantSpec{
		OwnerID: owner,
		ID:      id,
		Mount:   mount,
		Backing: tenant.BackingSpec{Root: definition.BackingRoot},
		Content: tenant.ContentSource{ID: definition.ContentSourceID},
		Traits: tenant.TenantTraits{
			Access: access, CaseSensitivity: casePolicy, Presentations: presentations,
		},
		FileProvider: fileProvider,
		Generation:   catalog.Generation(definition.Generation),
	}, nil
}

// DecodeTenantDefinition reconstructs one exact tenant specification from the mount protocol.
func DecodeTenantDefinition(owner tenant.OwnerID, id catalog.TenantID, definition mountproto.TenantDefinition) (tenant.TenantSpec, error) {
	return definitionSpec(owner, id, definition)
}

func protocolDefinition(spec tenant.TenantSpec) mountproto.TenantDefinition {
	presentations := make([]mountproto.Presentation, 0, 2)
	if spec.Traits.Presentations.Has(catalog.PresentationMount) {
		presentations = append(presentations, mountproto.PresentationMount)
	}
	if spec.Traits.Presentations.Has(catalog.PresentationFileProvider) {
		presentations = append(presentations, mountproto.PresentationFileProvider)
	}
	access := mountproto.AccessModeReadOnly
	if spec.Traits.Access == tenant.ReadWrite {
		access = mountproto.AccessModeReadWrite
	}
	casePolicy := mountproto.CasePolicySensitive
	if spec.Traits.CaseSensitivity == catalog.CaseInsensitive {
		casePolicy = mountproto.CasePolicyInsensitive
	}
	var mount *mountproto.MountSpec
	if spec.Traits.Presentations.Has(catalog.PresentationMount) {
		mount = &mountproto.MountSpec{PresentationRoot: spec.Mount.PresentationRoot}
	}
	return mountproto.TenantDefinition{
		Mount:           mount,
		BackingRoot:     spec.Backing.Root,
		ContentSourceID: spec.Content.ID,
		AccessMode:      access,
		CasePolicy:      casePolicy,
		Presentations:   presentations,
		FileProviderPresentationInstanceID: func() string {
			if !spec.FileProvider.Enabled {
				return ""
			}
			return spec.FileProvider.PresentationInstanceID
		}(),
		FileProviderDisplayName: func() string {
			if !spec.FileProvider.Enabled {
				return ""
			}
			return spec.FileProvider.DisplayName
		}(),
		Generation: uint64(spec.Generation),
	}
}

func protocolState(status tenant.TenantStatus) mountproto.TenantState {
	state := status.State
	result := mountproto.TenantState{
		OwnerID:             mountproto.OwnerID(status.Owner),
		TenantID:            mountproto.TenantID(state.Tenant),
		Generation:          uint64(state.Generation),
		Requested:           uint64(state.Requested),
		Desired:             uint64(state.Desired),
		Observed:            uint64(state.Observed),
		Verified:            uint64(state.Verified),
		Applied:             uint64(state.Applied),
		ActivatedGeneration: uint64(state.Activated),
		StateVersion:        uint64(state.Version),
		ReplacementEligible: status.ReplacementEligible,
	}
	if state.Quarantine != nil {
		result.Quarantine = &mountproto.Quarantine{
			Lane:          protocolQuarantineLane(state.Quarantine.Lane),
			Revision:      uint64(state.Quarantine.Revision),
			Cause:         protocolQuarantineCause(state.Quarantine.Cause),
			Detail:        state.Quarantine.Detail,
			SinceUnixNano: state.Quarantine.Since.UnixNano(),
		}
	}
	return result
}

func protocolQuarantineLane(lane catalog.QuarantineLane) mountproto.QuarantineLane {
	switch lane {
	case catalog.QuarantineLaneCatalogMutation:
		return mountproto.QuarantineLaneCatalogMutation
	case catalog.QuarantineLaneMaterialization:
		return mountproto.QuarantineLaneMaterialization
	case catalog.QuarantineLaneEnumeration:
		return mountproto.QuarantineLaneEnumeration
	case catalog.QuarantineLaneMountLifecycle:
		return mountproto.QuarantineLaneMountLifecycle
	default:
		panic(fmt.Sprintf("mount service: invalid quarantine lane %d", lane))
	}
}

func protocolQuarantineCause(cause catalog.QuarantineCause) mountproto.QuarantineCause {
	switch cause {
	case catalog.QuarantineCauseConflict:
		return mountproto.QuarantineCauseConflict
	case catalog.QuarantineCauseIntegrity:
		return mountproto.QuarantineCauseIntegrity
	case catalog.QuarantineCauseUnsettled:
		return mountproto.QuarantineCauseUnsettled
	case catalog.QuarantineCauseUnavailable:
		return mountproto.QuarantineCauseUnavailable
	default:
		panic(fmt.Sprintf("mount service: invalid quarantine cause %d", cause))
	}
}
