package holder

import (
	"context"
	"errors"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/tenant"
)

type presentationLifecycleRuntime struct {
	next          mountservice.Runtime
	presentations *presentationManager
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
	if err := r.presentations.Ensure(
		ctx,
		spec.Traits.Presentations.Has(catalog.PresentationMount),
		spec.Traits.Presentations.Has(catalog.PresentationFileProvider),
	); err != nil {
		return err
	}
	return nil
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

var _ mountservice.Runtime = presentationLifecycleRuntime{}
var _ catalogservice.MountPresentationPreparer = nativePresentationPreparer{}
var _ catalogservice.FileProviderPresentationPreparer = fileProviderPresentationPreparer{}
