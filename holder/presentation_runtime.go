package holder

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/tenant"
)

type presentationOperationFactoryFunc func() (presentationOperation, error)

func (f presentationOperationFactoryFunc) newPresentationOperation() (presentationOperation, error) {
	return f()
}

type ownedPresentationOperation struct {
	startOperation func(context.Context) error
	health         func() error
	closeOperation func() error

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func newOwnedPresentationOperation(
	start func(context.Context) error,
	health func() error,
	closeOperation func() error,
) (*ownedPresentationOperation, error) {
	if start == nil || health == nil || closeOperation == nil {
		return nil, errors.New("FuseKit runtime: presentation operation is incomplete")
	}
	return &ownedPresentationOperation{
		startOperation: start, health: health, closeOperation: closeOperation,
		closeDone: make(chan struct{}),
	}, nil
}

func (o *ownedPresentationOperation) start(ctx context.Context) error { return o.startOperation(ctx) }

func (o *ownedPresentationOperation) ready(context.Context) error { return o.health() }

func (o *ownedPresentationOperation) healthy() error { return o.health() }

func (o *ownedPresentationOperation) stop(context.Context) error {
	o.closeOnce.Do(func() {
		go func() {
			o.closeErr = o.closeOperation()
			close(o.closeDone)
		}()
	})
	return nil
}

func (o *ownedPresentationOperation) wait(ctx context.Context) error {
	select {
	case <-o.closeDone:
		return o.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func nativePresentationFactory(runtime *mountmux.Runtime, native nativeController) presentationOperationFactory {
	if runtime == nil || native == nil {
		return nil
	}
	return presentationOperationFactoryFunc(func() (presentationOperation, error) {
		return newOwnedPresentationOperation(
			runtime.Start,
			func() error {
				if state := native.HealthState(); state != daemon.StateHealthy {
					return fmt.Errorf("FuseKit runtime: native presentation health is %s", state)
				}
				return nil
			},
			func() error { return runtime.CloseContext(context.Background()) },
		)
	})
}

func brokerPresentationFactory(runtime *catalogservice.RuntimeBroker) presentationOperationFactory {
	if runtime == nil {
		return nil
	}
	return presentationOperationFactoryFunc(func() (presentationOperation, error) {
		return newOwnedPresentationOperation(
			runtime.Start,
			func() error {
				if phase := runtime.ReadinessPhase(); phase != catalogservice.RuntimeBrokerLive {
					return fmt.Errorf("FuseKit runtime: File Provider broker phase is %d", phase)
				}
				return nil
			},
			func() error { return runtime.Close(context.Background()) },
		)
	})
}

type presentationLifecycleRuntime struct {
	next          mountservice.Runtime
	presentations *presentationManager
	lookup        func(catalog.TenantID) (tenant.TenantSpec, error)
}

func (r presentationLifecycleRuntime) ProvisionTenant(ctx context.Context, spec tenant.TenantSpec) error {
	if err := r.ensure(ctx, spec); err != nil {
		return err
	}
	return r.next.ProvisionTenant(ctx, spec)
}

func (r presentationLifecycleRuntime) ReplaceTenant(
	ctx context.Context,
	expected catalog.Generation,
	spec tenant.TenantSpec,
) error {
	if err := r.ensure(ctx, spec); err != nil {
		return err
	}
	return r.next.ReplaceTenant(ctx, expected, spec)
}

func (r presentationLifecycleRuntime) RemoveTenant(
	ctx context.Context,
	id catalog.TenantID,
	generation catalog.Generation,
	owner tenant.OwnerID,
) error {
	if r.lookup == nil {
		return errors.New("FuseKit runtime: tenant presentation lookup is required")
	}
	current, err := r.lookup(id)
	if errors.Is(err, tenant.ErrTenantNotFound) {
		return r.next.RemoveTenant(ctx, id, generation, owner)
	}
	if err != nil {
		return err
	}
	if current.Traits.Presentations.Has(catalog.PresentationFileProvider) {
		if r.presentations == nil {
			return errors.New("FuseKit runtime: presentation manager is required")
		}
		if err := r.presentations.EnsureBroker(ctx); err != nil {
			return err
		}
	}
	return r.next.RemoveTenant(ctx, id, generation, owner)
}

func (r presentationLifecycleRuntime) State(
	ctx context.Context,
	id catalog.TenantID,
	owner tenant.OwnerID,
) (tenant.TenantStatus, error) {
	return r.next.State(ctx, id, owner)
}

func (r presentationLifecycleRuntime) ensure(ctx context.Context, spec tenant.TenantSpec) error {
	if r.next == nil {
		return errors.New("FuseKit runtime: tenant lifecycle runtime is required")
	}
	if r.presentations == nil {
		return errors.New("FuseKit runtime: presentation manager is required")
	}
	return r.presentations.Ensure(
		ctx,
		spec.Traits.Presentations.Has(catalog.PresentationMount),
		spec.Traits.Presentations.Has(catalog.PresentationFileProvider),
	)
}

type nativePresentationPreparer struct {
	presentations *presentationManager
	route         func(catalog.TenantID, catalog.Generation) error
}

func (p nativePresentationPreparer) PrepareMountPresentation(
	ctx context.Context,
	id catalog.TenantID,
	generation catalog.Generation,
) error {
	if p.presentations == nil || p.route == nil {
		return errors.New("FuseKit runtime: native presentation preparer is incomplete")
	}
	if err := p.presentations.EnsureNative(ctx); err != nil {
		return err
	}
	return p.route(id, generation)
}

type fileProviderPresentationPreparer struct {
	presentations *presentationManager
	next          catalogservice.FileProviderPresentationPreparer
}

func (p fileProviderPresentationPreparer) PrepareFileProviderPresentation(
	ctx context.Context,
	id catalog.TenantID,
	generation catalog.Generation,
) (catalog.FileProviderDomain, error) {
	if p.presentations == nil || p.next == nil {
		return catalog.FileProviderDomain{}, errors.New("FuseKit runtime: File Provider presentation preparer is incomplete")
	}
	if err := p.presentations.EnsureBroker(ctx); err != nil {
		return catalog.FileProviderDomain{}, err
	}
	return p.next.PrepareFileProviderPresentation(ctx, id, generation)
}

var _ catalogservice.MountPresentationPreparer = nativePresentationPreparer{}
var _ catalogservice.FileProviderPresentationPreparer = fileProviderPresentationPreparer{}
var _ mountservice.Runtime = presentationLifecycleRuntime{}
