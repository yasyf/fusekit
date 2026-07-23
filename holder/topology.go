package holder

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/tenant"
)

// ErrTopologyGeneration means a durable tenant transition requires a holder
// generation with a signed capability that the committed fleet needs.
var ErrTopologyGeneration = errors.New("FuseKit runtime: presentation topology requires a new generation")

type topologyFleetTransitions struct {
	next                tenant.FleetTransitionHook
	nativeCapable       bool
	fileProviderCapable bool
}

func (t topologyFleetTransitions) Prepare(ctx context.Context, transition tenant.FleetTransition) error {
	if err := t.validate(transition); err != nil {
		return err
	}
	return t.next.Prepare(ctx, transition)
}

func (t topologyFleetTransitions) Commit(ctx context.Context, transition tenant.FleetTransition) error {
	if err := t.validate(transition); err != nil {
		return err
	}
	return t.next.Commit(ctx, transition)
}

func (t topologyFleetTransitions) Abort(ctx context.Context, transition tenant.FleetTransition) error {
	return t.next.Abort(ctx, transition)
}

func (t topologyFleetTransitions) validate(transition tenant.FleetTransition) error {
	return validatePresentationCapabilities(t.nativeCapable, t.fileProviderCapable, transition.Committed)
}

func validatePresentationCapabilities(native, fileProvider bool, specs []tenant.TenantSpec) error {
	nativeRequired, fileProviderRequired := tenantSpecsRequirePresentations(specs)
	if nativeRequired && !native {
		return fmt.Errorf(
			"%w: native capable=%t, required=%t",
			ErrTopologyGeneration, native, nativeRequired,
		)
	}
	if fileProviderRequired && !fileProvider {
		return fmt.Errorf(
			"%w: File Provider capable=%t, required=%t",
			ErrTopologyGeneration, fileProvider, fileProviderRequired,
		)
	}
	return nil
}

func tenantSpecsRequirePresentations(specs []tenant.TenantSpec) (bool, bool) {
	native := false
	fileProvider := false
	for _, spec := range specs {
		if spec.Traits.Presentations.Has(catalog.PresentationMount) {
			native = true
		}
		if spec.Traits.Presentations.Has(catalog.PresentationFileProvider) {
			fileProvider = true
		}
	}
	return native, fileProvider
}
