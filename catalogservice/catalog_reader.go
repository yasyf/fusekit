package catalogservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	remoteObjectWireBudget      = 64 << 10
	remotePageWireBudget        = 1536 << 10
	remoteObjectFixedWireBudget = 1024
	remoteChangeFixedWireBudget = 128
)

// CatalogReader exposes catalog reads through the service's narrow read interface.
type CatalogReader struct {
	Store CatalogReadStore
}

// Root returns the tenant's stable presentation root.
func (r CatalogReader) Root(ctx context.Context, authorization Authorization, tenant catalog.TenantID) (catalog.Object, error) {
	metadata, err := r.requirePresentation(ctx, authorization, tenant)
	if err != nil {
		return catalog.Object{}, err
	}
	object, err := r.Store.Root(ctx, tenant)
	if err != nil {
		return catalog.Object{}, err
	}
	if err := validateLiveObject(object, tenant, authorization.Presentation); err != nil {
		return catalog.Object{}, err
	}
	if object.ID != metadata.Root || object.Parent != metadata.Root || object.Kind != catalog.KindDirectory {
		return catalog.Object{}, fmt.Errorf("%w: remote tenant root identity changed", catalog.ErrIntegrity)
	}
	return object, nil
}

// Head returns the current tenant revision.
func (r CatalogReader) Head(ctx context.Context, authorization Authorization, tenant catalog.TenantID) (catalog.Revision, error) {
	if _, err := r.requirePresentation(ctx, authorization, tenant); err != nil {
		return 0, err
	}
	revision, err := r.Store.Head(ctx, tenant)
	if err != nil {
		return 0, err
	}
	if revision == 0 {
		return 0, fmt.Errorf("%w: remote catalog head is zero", catalog.ErrIntegrity)
	}
	return revision, nil
}

// Snapshot returns one immutable metadata-only page.
func (r CatalogReader) Snapshot(ctx context.Context, authorization Authorization, tenant catalog.TenantID, scope catalog.EnumerationScope, revision catalog.Revision, cursor catalog.SnapshotCursor, limit int) (catalog.SnapshotPage, error) {
	if _, err := r.requirePresentation(ctx, authorization, tenant); err != nil {
		return catalog.SnapshotPage{}, err
	}
	if limit <= 0 || limit > int(catalogproto.MaxPageSize) {
		return catalog.SnapshotPage{}, fmt.Errorf("%w: snapshot limit is outside the remote page bound", catalog.ErrInvalidObject)
	}
	scope.Presentation = authorization.Presentation
	if scope.Kind == catalog.EnumerationWorkingSet {
		scope.Domain = causal.DomainID(authorization.Route.Domain)
		scope.Generation = causal.Generation(authorization.Route.Generation)
	}
	page, err := r.Store.Snapshot(ctx, tenant, scope, revision, cursor, limit)
	if err != nil {
		return catalog.SnapshotPage{}, err
	}
	if err := validateSnapshotPage(page, tenant, authorization.Presentation, scope, revision, cursor, limit); err != nil {
		return catalog.SnapshotPage{}, err
	}
	return page, nil
}

// ChangesSince returns ordered metadata-only changes.
func (r CatalogReader) ChangesSince(ctx context.Context, authorization Authorization, tenant catalog.TenantID, scope catalog.EnumerationScope, cursor catalog.ChangeCursor, limit int) (catalog.ChangePage, error) {
	if _, err := r.requirePresentation(ctx, authorization, tenant); err != nil {
		return catalog.ChangePage{}, err
	}
	if limit <= 0 || limit > int(catalogproto.MaxPageSize) {
		return catalog.ChangePage{}, fmt.Errorf("%w: change limit is outside the remote page bound", catalog.ErrInvalidObject)
	}
	scope.Presentation = authorization.Presentation
	if scope.Kind == catalog.EnumerationWorkingSet {
		scope.Domain = causal.DomainID(authorization.Route.Domain)
		scope.Generation = causal.Generation(authorization.Route.Generation)
	}
	page, err := r.Store.ChangesSince(ctx, tenant, scope, cursor, limit)
	if err != nil {
		return catalog.ChangePage{}, err
	}
	if err := validateChangePage(page, tenant, authorization.Presentation, scope, cursor, limit); err != nil {
		return catalog.ChangePage{}, err
	}
	return page, nil
}

// Lookup returns one object by identity.
func (r CatalogReader) Lookup(ctx context.Context, authorization Authorization, tenant catalog.TenantID, id catalog.ObjectID) (catalog.Object, error) {
	if _, err := r.requirePresentation(ctx, authorization, tenant); err != nil {
		return catalog.Object{}, err
	}
	object, err := r.Store.Lookup(ctx, tenant, authorization.Presentation, id)
	if err != nil {
		return catalog.Object{}, err
	}
	if err := validateLiveObject(object, tenant, authorization.Presentation); err != nil {
		return catalog.Object{}, err
	}
	if object.ID != id {
		return catalog.Object{}, fmt.Errorf("%w: remote object identity changed", catalog.ErrIntegrity)
	}
	return object, nil
}

// LookupName returns one child by exact name.
func (r CatalogReader) LookupName(ctx context.Context, authorization Authorization, tenant catalog.TenantID, parent catalog.ObjectID, name string) (catalog.Object, error) {
	metadata, err := r.requirePresentation(ctx, authorization, tenant)
	if err != nil {
		return catalog.Object{}, err
	}
	object, err := r.Store.LookupName(ctx, tenant, authorization.Presentation, parent, name)
	if err != nil {
		return catalog.Object{}, err
	}
	if err := validateLiveObject(object, tenant, authorization.Presentation); err != nil {
		return catalog.Object{}, err
	}
	if object.Parent != parent || normalizeName(metadata.CasePolicy, object.Name) != normalizeName(metadata.CasePolicy, name) {
		return catalog.Object{}, fmt.Errorf("%w: remote object binding changed", catalog.ErrIntegrity)
	}
	return object, nil
}

// OpenAt opens one exact immutable content revision.
func (r CatalogReader) OpenAt(ctx context.Context, authorization Authorization, tenant catalog.TenantID, generation catalog.Generation, id catalog.ObjectID, revision catalog.Revision) (OpenResult, error) {
	if _, err := r.requirePresentation(ctx, authorization, tenant); err != nil {
		return OpenResult{}, err
	}
	object, content, err := r.Store.OpenContentAt(ctx, tenant, authorization.Presentation, generation, id, revision)
	if err != nil {
		return OpenResult{}, err
	}
	if content == nil || validateLiveObject(object, tenant, authorization.Presentation) != nil ||
		object.ID != id || object.Revision != revision || object.Kind != catalog.KindFile {
		var closeErr error
		if content != nil {
			closeErr = content.Close()
		}
		return OpenResult{}, errors.Join(
			fmt.Errorf("%w: remote content identity changed", catalog.ErrIntegrity),
			closeErr,
		)
	}
	return OpenResult{Object: object, Content: content}, nil
}

func (r CatalogReader) requirePresentation(ctx context.Context, authorization Authorization, tenant catalog.TenantID) (catalog.TenantMetadata, error) {
	if r.Store == nil {
		return catalog.TenantMetadata{}, errors.New("catalog service: catalog reader store is required")
	}
	metadata, err := r.Store.Tenant(ctx, tenant)
	if err != nil {
		return catalog.TenantMetadata{}, err
	}
	if metadata.Tenant != tenant {
		return catalog.TenantMetadata{}, fmt.Errorf("%w: remote tenant identity changed", catalog.ErrIntegrity)
	}
	if metadata.Root == (catalog.ObjectID{}) ||
		(metadata.CasePolicy != catalog.CaseSensitive && metadata.CasePolicy != catalog.CaseInsensitive) ||
		metadata.Presentations == 0 ||
		metadata.Presentations&^(catalog.PresentMount|catalog.PresentFileProvider) != 0 {
		return catalog.TenantMetadata{}, fmt.Errorf("%w: remote tenant metadata changed", catalog.ErrIntegrity)
	}
	if !metadata.Presentations.Has(authorization.Presentation) {
		return catalog.TenantMetadata{}, fmt.Errorf("%w: tenant %q has no presentation %d", catalog.ErrInvalidObject, tenant, authorization.Presentation)
	}
	return metadata, nil
}

func validateSnapshotPage(
	page catalog.SnapshotPage,
	tenant catalog.TenantID,
	presentation catalog.Presentation,
	scope catalog.EnumerationScope,
	revision catalog.Revision,
	cursor catalog.SnapshotCursor,
	limit int,
) error {
	if page.Revision == 0 || (revision != 0 && page.Revision != revision) || len(page.Objects) > limit {
		return fmt.Errorf("%w: remote snapshot shape changed", catalog.ErrIntegrity)
	}
	var previous catalog.ObjectID
	pageBudget := 0
	if cursor.After != nil {
		previous = *cursor.After
	}
	for _, object := range page.Objects {
		if err := validateLiveObject(object, tenant, presentation); err != nil {
			return err
		}
		objectBudget, err := remoteObjectBudget(object)
		if err != nil || objectBudget > remotePageWireBudget-pageBudget {
			return fmt.Errorf("%w: remote snapshot exceeds the semantic wire budget", catalog.ErrIntegrity)
		}
		pageBudget += objectBudget
		if object.Revision > page.Revision || bytes.Compare(object.ID[:], previous[:]) <= 0 {
			return fmt.Errorf("%w: remote snapshot order changed", catalog.ErrIntegrity)
		}
		if scope.Kind == catalog.EnumerationContainer && object.Parent != scope.Parent {
			return fmt.Errorf("%w: remote snapshot scope changed", catalog.ErrIntegrity)
		}
		previous = object.ID
	}
	if page.Next == nil {
		return nil
	}
	if len(page.Objects) != limit || page.Next.After == nil || *page.Next.After != previous {
		return fmt.Errorf("%w: remote snapshot continuation changed", catalog.ErrIntegrity)
	}
	return nil
}

func validateChangePage(
	page catalog.ChangePage,
	tenant catalog.TenantID,
	presentation catalog.Presentation,
	scope catalog.EnumerationScope,
	cursor catalog.ChangeCursor,
	limit int,
) error {
	if page.Head == 0 || page.Floor > page.Head || cursor.Revision < page.Floor ||
		cursor.Revision > page.Head || len(page.Changes) > limit {
		return fmt.Errorf("%w: remote change page shape changed", catalog.ErrIntegrity)
	}
	previous := cursor
	pageBudget := 0
	for _, change := range page.Changes {
		position := catalog.ChangeCursor{Revision: change.Revision, Sequence: change.Sequence}
		if !changeCursorBefore(previous, position) || change.Revision > page.Head ||
			change.Sequence == catalog.CompleteChangeSequence || change.Object.Revision > change.Revision {
			return fmt.Errorf("%w: remote change order changed", catalog.ErrIntegrity)
		}
		if err := validateRemoteObject(change.Object, tenant); err != nil {
			return err
		}
		objectBudget, err := remoteObjectBudget(change.Object)
		if err != nil || objectBudget > remotePageWireBudget-pageBudget-remoteChangeFixedWireBudget {
			return fmt.Errorf("%w: remote changes exceed the semantic wire budget", catalog.ErrIntegrity)
		}
		pageBudget += objectBudget + remoteChangeFixedWireBudget
		switch change.Kind {
		case catalog.ChangeDelete:
		case catalog.ChangeUpsert:
			if err := validateLiveObject(change.Object, tenant, presentation); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: remote change kind changed", catalog.ErrIntegrity)
		}
		if scope.Kind == catalog.EnumerationContainer && change.Object.Parent != scope.Parent {
			return fmt.Errorf("%w: remote change scope changed", catalog.ErrIntegrity)
		}
		previous = position
	}
	if page.Complete {
		if page.Next != catalog.CompleteChangeCursor(page.Head) {
			return fmt.Errorf("%w: remote change completion changed", catalog.ErrIntegrity)
		}
		return nil
	}
	if len(page.Changes) != limit || page.Next != previous {
		return fmt.Errorf("%w: remote change continuation changed", catalog.ErrIntegrity)
	}
	return nil
}

func validateLiveObject(object catalog.Object, tenant catalog.TenantID, presentation catalog.Presentation) error {
	if err := validateRemoteObject(object, tenant); err != nil {
		return err
	}
	if object.Tombstone || !object.Visibility.Has(presentation) {
		return fmt.Errorf("%w: remote object identity changed", catalog.ErrIntegrity)
	}
	return nil
}

func validateRemoteObject(object catalog.Object, tenant catalog.TenantID) error {
	if object.Tenant != tenant || object.ID == (catalog.ObjectID{}) || object.Revision == 0 ||
		object.MetadataRevision == 0 || object.MetadataRevision > object.Revision {
		return fmt.Errorf("%w: remote object identity changed", catalog.ErrIntegrity)
	}
	if object.Kind != catalog.KindDirectory && object.Kind != catalog.KindFile && object.Kind != catalog.KindSymlink {
		return fmt.Errorf("%w: remote object kind changed", catalog.ErrIntegrity)
	}
	if object.Size < 0 || object.Convergence.Applied > object.Convergence.Verified ||
		object.Convergence.Verified > object.Convergence.Observed ||
		object.Convergence.Observed > object.Convergence.Desired {
		return fmt.Errorf("%w: remote object state changed", catalog.ErrIntegrity)
	}
	if _, err := remoteObjectBudget(object); err != nil {
		return err
	}
	return nil
}

func remoteObjectBudget(object catalog.Object) (int, error) {
	budget := remoteObjectFixedWireBudget
	for _, text := range []string{string(object.Tenant), object.Name, object.LinkTarget} {
		if len(text) > (remoteObjectWireBudget-budget)/6 {
			return 0, fmt.Errorf("%w: remote object exceeds the semantic wire budget", catalog.ErrIntegrity)
		}
		budget += len(text) * 6
	}
	return budget, nil
}

func changeCursorBefore(left, right catalog.ChangeCursor) bool {
	return left.Revision < right.Revision || left.Revision == right.Revision && left.Sequence < right.Sequence
}

func normalizeName(policy catalog.CasePolicy, name string) string {
	normalized := norm.NFC.String(name)
	if policy == catalog.CaseInsensitive {
		return cases.Fold().String(normalized)
	}
	return normalized
}
