package catalogservice

import (
	"context"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

// CatalogReader exposes catalog reads through the service's narrow read interface.
type CatalogReader struct {
	Catalog *catalog.Catalog
}

// Head returns the current tenant revision.
func (r CatalogReader) Head(ctx context.Context, authorization Authorization, tenant catalog.TenantID) (catalog.Revision, error) {
	if err := r.requirePresentation(ctx, authorization, tenant); err != nil {
		return 0, err
	}
	return r.Catalog.Head(ctx, tenant)
}

// Snapshot returns one immutable metadata-only page.
func (r CatalogReader) Snapshot(ctx context.Context, authorization Authorization, tenant catalog.TenantID, scope catalog.EnumerationScope, revision catalog.Revision, cursor catalog.SnapshotCursor, limit int) (catalog.SnapshotPage, error) {
	if err := r.requirePresentation(ctx, authorization, tenant); err != nil {
		return catalog.SnapshotPage{}, err
	}
	scope.Presentation = authorization.Presentation
	if scope.Kind == catalog.EnumerationWorkingSet {
		scope.Domain = causal.DomainID(authorization.Route.Domain)
		scope.Generation = causal.Generation(authorization.Route.Generation)
	}
	return r.Catalog.Snapshot(ctx, tenant, scope, revision, cursor, limit)
}

// ChangesSince returns ordered metadata-only changes.
func (r CatalogReader) ChangesSince(ctx context.Context, authorization Authorization, tenant catalog.TenantID, scope catalog.EnumerationScope, cursor catalog.ChangeCursor, limit int) (catalog.ChangePage, error) {
	if err := r.requirePresentation(ctx, authorization, tenant); err != nil {
		return catalog.ChangePage{}, err
	}
	scope.Presentation = authorization.Presentation
	if scope.Kind == catalog.EnumerationWorkingSet {
		scope.Domain = causal.DomainID(authorization.Route.Domain)
		scope.Generation = causal.Generation(authorization.Route.Generation)
	}
	return r.Catalog.ChangesSince(ctx, tenant, scope, cursor, limit)
}

// Lookup returns one object by identity.
func (r CatalogReader) Lookup(ctx context.Context, authorization Authorization, tenant catalog.TenantID, id catalog.ObjectID) (catalog.Object, error) {
	if err := r.requirePresentation(ctx, authorization, tenant); err != nil {
		return catalog.Object{}, err
	}
	return r.Catalog.Lookup(ctx, tenant, authorization.Presentation, id)
}

// LookupName returns one child by exact name.
func (r CatalogReader) LookupName(ctx context.Context, authorization Authorization, tenant catalog.TenantID, parent catalog.ObjectID, name string) (catalog.Object, error) {
	if err := r.requirePresentation(ctx, authorization, tenant); err != nil {
		return catalog.Object{}, err
	}
	return r.Catalog.LookupName(ctx, tenant, authorization.Presentation, parent, name)
}

// OpenAt opens one exact immutable content revision.
func (r CatalogReader) OpenAt(ctx context.Context, authorization Authorization, tenant catalog.TenantID, generation catalog.Generation, id catalog.ObjectID, revision catalog.Revision) (OpenResult, error) {
	if err := r.requirePresentation(ctx, authorization, tenant); err != nil {
		return OpenResult{}, err
	}
	handle, err := r.Catalog.OpenAt(ctx, tenant, authorization.Presentation, generation, id, revision)
	if err != nil {
		return OpenResult{}, err
	}
	return OpenResult{Object: handle.Object, Content: handle}, nil
}

func (r CatalogReader) requirePresentation(ctx context.Context, authorization Authorization, tenant catalog.TenantID) error {
	metadata, err := r.Catalog.Tenant(ctx, tenant)
	if err != nil {
		return err
	}
	if !metadata.Presentations.Has(authorization.Presentation) {
		return fmt.Errorf("%w: tenant %q has no presentation %d", catalog.ErrInvalidObject, tenant, authorization.Presentation)
	}
	return nil
}
