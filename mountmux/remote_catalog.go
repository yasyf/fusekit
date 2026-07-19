//go:build darwin && cgo && fuse

package mountmux

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
)

// RemoteNativeCatalog projects the holder-owned catalog over one persistent session.
type RemoteNativeCatalog struct {
	client *catalogservice.Client
}

// NewRemoteNativeCatalog binds native callbacks to an existing exact-suite catalog client.
func NewRemoteNativeCatalog(client *catalogservice.Client) (*RemoteNativeCatalog, error) {
	if client == nil {
		return nil, errors.New("mountmux: nil remote catalog client")
	}
	return &RemoteNativeCatalog{client: client}, nil
}

func (c *RemoteNativeCatalog) Root(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation) (catalog.Object, error) {
	callContext, cancel := callbackContext(ctx)
	defer cancel()
	response, err := c.client.Root(callContext, catalogproto.TenantID(tenantID), uint64(generation))
	return responseObject(tenantID, response, err)
}

func (c *RemoteNativeCatalog) Head(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation) (catalog.Revision, error) {
	callContext, cancel := callbackContext(ctx)
	defer cancel()
	response, err := c.client.Head(callContext, catalogproto.TenantID(tenantID), uint64(generation))
	if err != nil {
		return 0, remoteCatalogError(err)
	}
	return catalog.Revision(response.Revision), nil
}

func (c *RemoteNativeCatalog) Lookup(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation, id catalog.ObjectID) (catalog.Object, error) {
	callContext, cancel := callbackContext(ctx)
	defer cancel()
	response, err := c.client.Lookup(callContext, catalogproto.TenantID(tenantID), catalogproto.LookupRequest{
		Protocol: catalogproto.Version, Generation: uint64(generation), ObjectID: protocolObjectID(id),
	})
	return responseObject(tenantID, response, err)
}

func (c *RemoteNativeCatalog) LookupName(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation, parent catalog.ObjectID, name string) (catalog.Object, error) {
	callContext, cancel := callbackContext(ctx)
	defer cancel()
	response, err := c.client.LookupName(callContext, catalogproto.TenantID(tenantID), catalogproto.LookupNameRequest{
		Protocol: catalogproto.Version, Generation: uint64(generation), ParentID: protocolObjectID(parent), Name: name,
	})
	return responseObject(tenantID, response, err)
}

func (c *RemoteNativeCatalog) Snapshot(
	ctx context.Context,
	tenantID catalog.TenantID,
	generation catalog.Generation,
	parent catalog.ObjectID,
	revision catalog.Revision,
	cursor catalog.SnapshotCursor,
	limit int,
) (catalog.SnapshotPage, error) {
	callContext, cancel := callbackContext(ctx)
	defer cancel()
	parentID := protocolObjectID(parent)
	request := catalogproto.SnapshotRequest{
		Protocol: catalogproto.Version, Generation: uint64(generation), Revision: uint64(revision),
		Scope: catalogproto.EnumerationScope{Kind: catalogproto.EnumerationScopeKindContainer, ParentID: &parentID},
		Limit: uint32(limit),
	}
	if cursor.After != nil {
		after := protocolObjectID(*cursor.After)
		request.After = &after
	}
	response, err := c.client.Snapshot(callContext, catalogproto.TenantID(tenantID), request)
	if err != nil {
		return catalog.SnapshotPage{}, remoteCatalogError(err)
	}
	objects := make([]catalog.Object, 0, len(response.Objects))
	for _, object := range response.Objects {
		converted, err := nativeCatalogObject(tenantID, object)
		if err != nil {
			return catalog.SnapshotPage{}, err
		}
		objects = append(objects, converted)
	}
	page := catalog.SnapshotPage{Revision: catalog.Revision(response.Revision), Objects: objects}
	if response.Next != nil {
		next, err := catalog.ParseObjectID(string(*response.Next))
		if err != nil {
			return catalog.SnapshotPage{}, fmt.Errorf("%w: invalid snapshot cursor: %v", catalog.ErrIntegrity, err)
		}
		page.Next = &catalog.SnapshotCursor{After: &next}
	}
	return page, nil
}

func (c *RemoteNativeCatalog) Open(
	ctx context.Context,
	tenantID catalog.TenantID,
	generation catalog.Generation,
	id catalog.ObjectID,
	revision catalog.Revision,
) (*NativeSnapshot, error) {
	callContext, cancel := callbackContext(ctx)
	defer cancel()
	reader, err := c.client.OpenAt(callContext, catalogproto.TenantID(tenantID), catalogproto.OpenAtRequest{
		Protocol: catalogproto.Version, Generation: uint64(generation),
		ObjectID: protocolObjectID(id), Revision: uint64(revision),
	})
	if err != nil {
		return nil, remoteCatalogError(err)
	}
	staging, err := privateStagingFile()
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	fail := func(err error) (*NativeSnapshot, error) {
		_ = reader.Close()
		_ = staging.Close()
		return nil, err
	}
	count, err := io.Copy(staging, reader)
	if err != nil {
		return fail(remoteCatalogError(err))
	}
	response, err := reader.Response()
	if err != nil {
		return fail(remoteCatalogError(err))
	}
	if response.Object == nil {
		return fail(fmt.Errorf("%w: open response has no object", catalog.ErrIntegrity))
	}
	object, err := nativeCatalogObject(tenantID, *response.Object)
	if err != nil {
		return fail(err)
	}
	if object.ID != id || object.Revision != revision || object.Size != count {
		return fail(fmt.Errorf("%w: streamed snapshot identity or size changed", catalog.ErrIntegrity))
	}
	if _, err := staging.Seek(0, io.SeekStart); err != nil {
		return fail(fmt.Errorf("mountmux: rewind remote snapshot: %w", err))
	}
	return &NativeSnapshot{Object: object, Source: staging}, nil
}

func (c *RemoteNativeCatalog) Mutate(
	ctx context.Context,
	tenantID catalog.TenantID,
	generation catalog.Generation,
	request catalogproto.MutationRequest,
	content io.Reader,
) (catalogproto.MutationResponse, error) {
	if request.Generation != uint64(generation) {
		return catalogproto.MutationResponse{}, fmt.Errorf("%w: mutation generation changed", catalog.ErrIntegrity)
	}
	callContext, cancel := callbackContext(ctx)
	defer cancel()
	response, err := c.client.Mutate(callContext, catalogproto.TenantID(tenantID), request, content)
	return response, remoteCatalogError(err)
}

func responseObject(tenantID catalog.TenantID, response catalogproto.LookupResponse, err error) (catalog.Object, error) {
	if err != nil {
		return catalog.Object{}, remoteCatalogError(err)
	}
	if response.Object == nil {
		return catalog.Object{}, fmt.Errorf("%w: lookup response has no object", catalog.ErrIntegrity)
	}
	return nativeCatalogObject(tenantID, *response.Object)
}

func nativeCatalogObject(tenantID catalog.TenantID, object catalogproto.CatalogObject) (catalog.Object, error) {
	id, err := catalog.ParseObjectID(string(object.ID))
	if err != nil {
		return catalog.Object{}, fmt.Errorf("%w: invalid object identity: %v", catalog.ErrIntegrity, err)
	}
	parent, err := catalog.ParseObjectID(string(object.ParentID))
	if err != nil {
		return catalog.Object{}, fmt.Errorf("%w: invalid parent identity: %v", catalog.ErrIntegrity, err)
	}
	kind := catalog.KindDirectory
	switch object.Kind {
	case catalogproto.ObjectKindDirectory:
	case catalogproto.ObjectKindFile:
		kind = catalog.KindFile
	case catalogproto.ObjectKindSymlink:
		kind = catalog.KindSymlink
	default:
		return catalog.Object{}, fmt.Errorf("%w: invalid object kind", catalog.ErrIntegrity)
	}
	var hash catalog.ContentHash
	if object.Hash != "" {
		decoded, err := hex.DecodeString(object.Hash)
		if err != nil || len(decoded) != len(hash) {
			return catalog.Object{}, fmt.Errorf("%w: invalid content hash", catalog.ErrIntegrity)
		}
		copy(hash[:], decoded)
	}
	return catalog.Object{
		Tenant: tenantID, ID: id, Parent: parent, Revision: catalog.Revision(object.Revision),
		MetadataRevision: catalog.Revision(object.MetadataRevision), ContentRevision: catalog.Revision(object.ContentRevision),
		Name: object.Name, Kind: kind, Mode: object.Mode, Size: int64(object.Size), Hash: hash, LinkTarget: object.LinkTarget,
		Convergence: catalog.Convergence{
			Desired: catalog.Revision(object.Desired), Observed: catalog.Revision(object.Observed),
			Verified: catalog.Revision(object.Verified), Applied: catalog.Revision(object.Applied),
		},
		Visibility: catalog.Visibility{Mount: true}, Tombstone: object.Tombstone,
	}, nil
}

func remoteCatalogError(err error) error {
	if err == nil {
		return nil
	}
	var remote *catalogservice.RemoteError
	if !errors.As(err, &remote) {
		return err
	}
	var cause error
	switch remote.Code {
	case catalogproto.ErrorCodeNotFound, catalogproto.ErrorCodeStaleAnchor:
		cause = catalog.ErrNotFound
	case catalogproto.ErrorCodeConflict:
		cause = catalog.ErrConflict
	case catalogproto.ErrorCodeIntegrity:
		cause = catalog.ErrIntegrity
	case catalogproto.ErrorCodeInvalidRequest:
		cause = catalog.ErrInvalidObject
	default:
		return err
	}
	return fmt.Errorf("%w: %s", cause, remote.Message)
}

var _ NativeCatalog = (*RemoteNativeCatalog)(nil)
