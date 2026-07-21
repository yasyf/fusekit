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
var ErrTopologyGeneration = errors.New("holder: presentation topology requires a new generation")

type topologyFleetTransitions struct {
	next                tenant.FleetTransitionHook
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
	return validateFileProviderCapability(t.fileProviderCapable, transition.Committed)
}

func validateFileProviderCapability(capable bool, specs []tenant.TenantSpec) error {
	required := tenantSpecsRequireFileProvider(specs)
	if required && !capable {
		return fmt.Errorf(
			"%w: File Provider capable=%t, required=%t",
			ErrTopologyGeneration, capable, required,
		)
	}
	return nil
}

func tenantSpecsRequireFileProvider(specs []tenant.TenantSpec) bool {
	for _, spec := range specs {
		if spec.Traits.Presentations.Has(catalog.PresentationFileProvider) {
			return true
		}
	}
	return false
}
