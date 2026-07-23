package holder

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/tenant"
)

type authorityRegistryBuilder func(SourceAuthorityFleet) (*authorityRegistry, error)

type authorityFleetReplacer interface {
	replace(context.Context, *authorityRegistry, []tenant.TenantSpec) error
}

type topologyController struct {
	reconciler  topologyReconciler
	drivers     DriverFactories
	authorities authorityFleetReplacer
	build       authorityRegistryBuilder

	mu      sync.Mutex
	current desiredTopology
	wake    chan struct{}
	stopped bool

	startOnce sync.Once
	cancel    context.CancelFunc
	done      chan struct{}
	err       error
}

func newTopologyController(
	store topologyStore,
	owner catalog.SourceAuthorityFleetOwnerID,
	drivers DriverFactories,
	authorities authorityFleetReplacer,
	build authorityRegistryBuilder,
	initial desiredTopology,
) (*topologyController, error) {
	if store == nil || owner == "" || authorities == nil || build == nil {
		return nil, errors.New("holder: dynamic topology controller is incomplete")
	}
	controller := &topologyController{
		drivers: drivers, authorities: authorities, build: build,
		current: initial, wake: make(chan struct{}), done: make(chan struct{}),
	}
	controller.reconciler = topologyReconciler{store: store, owner: owner, apply: controller.apply}
	return controller, nil
}

func (c *topologyController) Start(lifetime context.Context) {
	c.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(lifetime)
		c.mu.Lock()
		c.cancel = cancel
		c.mu.Unlock()
		go func() {
			defer close(c.done)
			err := c.reconciler.run(ctx)
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				err = nil
			}
			c.mu.Lock()
			c.err = err
			c.stopped = true
			if c.wake != nil {
				close(c.wake)
			}
			c.wake = make(chan struct{})
			c.mu.Unlock()
		}()
	})
}

func (c *topologyController) apply(ctx context.Context, desired desiredTopology) error {
	c.mu.Lock()
	current := c.current
	c.mu.Unlock()
	if sameDesiredSourceFleet(current, desired) {
		c.publishApplied(desired)
		return nil
	}
	fleet, err := c.drivers.sourceFleet(ctx, desired)
	if err != nil {
		return err
	}
	var next *authorityRegistry
	if desired.Head.Fleet != nil {
		next, err = c.build(fleet)
		if err != nil {
			return err
		}
	}
	if err := c.authorities.replace(ctx, next, topologyTenantSpecs(desired.Tenants)); err != nil {
		return err
	}
	c.publishApplied(desired)
	return nil
}

func (c *topologyController) publishApplied(desired desiredTopology) {
	c.mu.Lock()
	c.current = desired
	if c.wake != nil {
		close(c.wake)
	}
	c.wake = make(chan struct{})
	c.mu.Unlock()
}

func (c *topologyController) AwaitSourceFleetApplied(
	ctx context.Context,
	desired catalog.DesiredSourceAuthorityFleetState,
) error {
	for {
		c.mu.Lock()
		current, wake, terminalErr, stopped := c.current.Head.Fleet, c.wake, c.err, c.stopped
		c.mu.Unlock()
		if current != nil {
			if current.Owner != desired.Owner || current.Generation > desired.Generation {
				return catalog.ErrGenerationMismatch
			}
			if current.Generation == desired.Generation {
				if *current != desired {
					return catalog.ErrMutationConflict
				}
				return nil
			}
		}
		if terminalErr != nil {
			return fmt.Errorf("holder: desired topology controller failed: %w", terminalErr)
		}
		if stopped {
			return errors.New("holder: desired topology controller stopped before fleet application")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

func sameDesiredSourceFleet(left, right desiredTopology) bool {
	if left.Head.Fleet == nil || right.Head.Fleet == nil {
		return left.Head.Fleet == nil && right.Head.Fleet == nil
	}
	return *left.Head.Fleet == *right.Head.Fleet && slices.EqualFunc(
		left.Authorities, right.Authorities,
		func(left, right catalog.TopologySourceAuthority) bool {
			return left.Owner == right.Owner && left.FleetGeneration == right.FleetGeneration &&
				left.Authority == right.Authority && left.DriverID == right.DriverID &&
				bytes.Equal(left.DriverConfig, right.DriverConfig) &&
				left.DeclarationDigest == right.DeclarationDigest
		},
	)
}

func topologyTenantSpecs(provisions []catalog.TenantProvision) []tenant.TenantSpec {
	result := make([]tenant.TenantSpec, len(provisions))
	for index, provision := range provisions {
		var fileProvider tenant.FileProviderSpec
		if provision.FileProvider.Enabled() {
			fileProvider = tenant.FileProviderSpec{
				Enabled: true, PresentationInstanceID: provision.FileProvider.PresentationInstanceID,
				DisplayName: provision.FileProvider.DisplayName,
			}
		}
		result[index] = tenant.TenantSpec{
			OwnerID: tenant.OwnerID(provision.OwnerID), ID: provision.Tenant,
			PresentationRoot: provision.PresentationRoot,
			Backing:          tenant.BackingSpec{Root: provision.BackingRoot},
			Content:          tenant.ContentSource{ID: provision.ContentSourceID},
			Traits: tenant.TenantTraits{
				Access: provision.Access, CaseSensitivity: provision.CasePolicy,
				Presentations: provision.Presentations,
			},
			FileProvider: fileProvider, Generation: provision.Generation,
		}
	}
	return result
}

func (c *topologyController) Close(ctx context.Context) error {
	c.startOnce.Do(func() {
		c.mu.Lock()
		c.stopped = true
		if c.wake != nil {
			close(c.wake)
		}
		close(c.done)
		c.mu.Unlock()
	})
	c.mu.Lock()
	cancel := c.cancel
	done := c.done
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	select {
	case <-done:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.err
	case <-ctx.Done():
		return fmt.Errorf("holder: wait for dynamic topology controller: %w", ctx.Err())
	}
}

func (c *topologyController) Cancel() {
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *topologyController) Failed() bool {
	select {
	case <-c.done:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.err != nil
	default:
		return false
	}
}
