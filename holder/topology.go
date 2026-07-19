package holder

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/tenant"
)

// ErrTopologyGeneration means a durable tenant transition requires a holder
// generation whose signed capabilities match the committed fleet.
var ErrTopologyGeneration = errors.New("holder: presentation topology requires a new generation")

type topologyFleetTransitions struct {
	next         tenant.FleetTransitionHook
	fileProvider bool
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
	required := tenantSpecsRequireFileProvider(transition.Committed)
	if required != t.fileProvider {
		return fmt.Errorf(
			"%w: configured=%t, committed=%t",
			ErrTopologyGeneration, t.fileProvider, required,
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
