// Package mountservice binds exact tenant lifecycle operations to daemonkit wire v3.
package mountservice

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/tenant"
)

var ErrUnauthorized = errors.New("mount service: unauthorized")

// Identity is the daemonkit-authenticated identity of one persistent session.
type Identity struct {
	Peer    wire.Peer
	Build   string
	Session *wire.AcceptedSession
}

// Authorizer derives the owning consumer from authenticated peer identity.
type Authorizer interface {
	Authorize(context.Context, Identity, mountproto.Operation, catalog.TenantID, catalog.Generation) (tenant.OwnerID, error)
}

// Runtime is the exact-generation tenant lifecycle surface exposed over the wire.
type Runtime interface {
	RegisterTenant(context.Context, tenant.TenantSpec) error
	ReplaceTenant(context.Context, catalog.Generation, tenant.TenantSpec) error
	RemoveTenant(context.Context, catalog.TenantID, catalog.Generation) error
	PrepareTenant(context.Context, catalog.TenantID, catalog.Generation, catalog.Revision) (tenant.TenantState, error)
	State(context.Context, catalog.TenantID, catalog.Generation) (tenant.TenantState, error)
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

// TransportError is an untyped daemonkit delivery or terminal failure.
type TransportError struct {
	Outcome wire.Outcome
	Message string
}

// Error implements error.
func (e *TransportError) Error() string { return e.Message }

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
	return tenant.TenantSpec{
		OwnerID:          owner,
		ID:               id,
		PresentationRoot: definition.PresentationRoot,
		Backing:          tenant.BackingSpec{Root: definition.BackingRoot},
		Content:          tenant.ContentSource{ID: definition.ContentSourceID},
		Traits: tenant.TenantTraits{
			Access: access, CaseSensitivity: casePolicy, Presentations: presentations,
		},
		Generation: catalog.Generation(definition.Generation),
	}, nil
}

func protocolState(state tenant.TenantState) mountproto.TenantState {
	result := mountproto.TenantState{
		TenantID:            mountproto.TenantID(state.Tenant),
		Generation:          uint64(state.Generation),
		Requested:           uint64(state.Requested),
		Desired:             uint64(state.Desired),
		Observed:            uint64(state.Observed),
		Verified:            uint64(state.Verified),
		Applied:             uint64(state.Applied),
		ActivatedGeneration: uint64(state.Activated),
		StateVersion:        uint64(state.Version),
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
