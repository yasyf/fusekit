//go:build darwin && cgo && fuse

package mountmux

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
)

// RemoteNativeCatalog projects the holder-owned catalog over one persistent session.
type RemoteNativeCatalog struct {
	client *catalogservice.Client
	mount  *mountservice.Client
}

// NewRemoteNativeCatalog binds native callbacks to an existing exact-suite catalog client.
func NewRemoteNativeCatalog(client *catalogservice.Client, mount *mountservice.Client) (*RemoteNativeCatalog, error) {
	if client == nil || mount == nil {
		return nil, errors.New("mountmux: nil remote catalog or mount client")
	}
	return &RemoteNativeCatalog{client: client, mount: mount}, nil
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
	response, err := c.mount.NativeSnapshotOpen(callContext, tenantID, generation, id, revision)
	if err != nil {
		return nil, remoteMountError(err)
	}
	fail := func(cause error) (*NativeSnapshot, error) {
		if response.Handle == "" {
			return nil, cause
		}
		closeCtx, closeCancel := callbackContext(context.Background())
		closeErr := c.mount.NativeSnapshotClose(closeCtx, response.Handle)
		closeCancel()
		return nil, errors.Join(cause, remoteMountError(closeErr))
	}
	if response.Object == nil {
		return fail(fmt.Errorf("%w: open response has no object", catalog.ErrIntegrity))
	}
	object, err := nativeMountObject(tenantID, *response.Object)
	if err != nil {
		return fail(err)
	}
	if object.ID != id || object.Revision != revision || response.Handle == "" {
		return fail(fmt.Errorf("%w: opened snapshot identity changed", catalog.ErrIntegrity))
	}
	return &NativeSnapshot{
		Object: object,
		Source: &remoteSnapshot{client: c.mount, handle: response.Handle},
	}, nil
}

func (c *RemoteNativeCatalog) OpenWrite(
	ctx context.Context,
	tenantID catalog.TenantID,
	generation catalog.Generation,
	id catalog.ObjectID,
	revision catalog.Revision,
) (*NativeWriteStage, error) {
	callContext, cancel := callbackContext(ctx)
	defer cancel()
	response, err := c.mount.NativeWriteOpen(callContext, tenantID, generation, id, revision)
	if err != nil {
		return nil, remoteMountError(err)
	}
	fail := func(cause error) (*NativeWriteStage, error) {
		if response.Handle == "" {
			return nil, cause
		}
		closeCtx, closeCancel := callbackContext(context.Background())
		closeErr := c.mount.NativeWriteAbort(closeCtx, response.Handle)
		closeCancel()
		return nil, errors.Join(cause, remoteMountError(closeErr))
	}
	if response.Object == nil {
		return fail(fmt.Errorf("%w: write-stage response has no object", catalog.ErrIntegrity))
	}
	object, err := nativeMountObject(tenantID, *response.Object)
	if err != nil {
		return fail(err)
	}
	if object.ID != id || object.Revision != revision || response.Handle == "" {
		return fail(fmt.Errorf("%w: opened write-stage identity changed", catalog.ErrIntegrity))
	}
	source := &remoteWriteStage{client: c.mount, handle: response.Handle, size: object.Size, tenant: tenantID}
	return &NativeWriteStage{Object: object, Source: source}, nil
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
	if object.Size > uint64(maxNativeObjectSize) {
		return catalog.Object{}, fmt.Errorf("%w: object size exceeds native range", catalog.ErrIntegrity)
	}
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

type remoteSnapshot struct {
	client nativeSnapshotClient
	handle string

	mu     sync.Mutex
	offset int64
	closed bool
}

type nativeSnapshotClient interface {
	NativeSnapshotRead(context.Context, string, int64, int) (mountproto.NativeSnapshotReadResponse, error)
	NativeSnapshotClose(context.Context, string) error
}

func (r *remoteSnapshot) Read(buffer []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count, err := r.readAt(buffer, r.offset)
	r.offset += int64(count)
	return count, err
}

func (r *remoteSnapshot) ReadAt(buffer []byte, offset int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readAt(buffer, offset)
}

func (r *remoteSnapshot) readAt(buffer []byte, offset int64) (int, error) {
	if r.closed {
		return 0, catalog.ErrHandleClosed
	}
	if offset < 0 || int64(len(buffer)) > maxNativeObjectSize-offset {
		return 0, catalog.ErrInvalidObject
	}
	total := 0
	for total < len(buffer) {
		length := min(len(buffer)-total, nativeCallbackChunkLimit)
		ctx, cancel := callbackContext(context.Background())
		response, err := r.client.NativeSnapshotRead(ctx, r.handle, offset+int64(total), length)
		cancel()
		if err != nil {
			return total, remoteMountError(err)
		}
		count := copy(buffer[total:], response.Data)
		total += count
		if count != len(response.Data) || len(response.Data) > length {
			return total, catalog.ErrIntegrity
		}
		if response.EOF {
			if total < len(buffer) {
				return total, io.EOF
			}
			return total, nil
		}
		if count == 0 {
			return total, catalog.ErrIntegrity
		}
	}
	return total, nil
}

func (r *remoteSnapshot) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	ctx, cancel := callbackContext(context.Background())
	err := r.client.NativeSnapshotClose(ctx, r.handle)
	cancel()
	if err != nil {
		return remoteMountError(err)
	}
	r.closed = true
	return nil
}

type remoteWriteStage struct {
	client nativeWriteClient
	handle string
	tenant catalog.TenantID

	mu      sync.Mutex
	size    int64
	dirty   bool
	failed  bool
	pending bool
	closed  bool
}

type nativeWriteClient interface {
	NativeWriteRead(context.Context, string, int64, int) (mountproto.NativeWriteReadResponse, error)
	NativeWrite(context.Context, string, int64, []byte) (int, error)
	NativeWriteTruncate(context.Context, string, int64) error
	NativeWriteSync(context.Context, string) error
	NativeWriteCommit(context.Context, string) (mountproto.NativeWriteCommitResponse, error)
	NativeWriteAbort(context.Context, string) error
}

func (s *remoteWriteStage) ReadAt(buffer []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, catalog.ErrHandleClosed
	}
	if offset < 0 || int64(len(buffer)) > maxNativeObjectSize-offset {
		return 0, catalog.ErrInvalidObject
	}
	total := 0
	for total < len(buffer) {
		length := min(len(buffer)-total, nativeCallbackChunkLimit)
		ctx, cancel := callbackContext(context.Background())
		response, err := s.client.NativeWriteRead(ctx, s.handle, offset+int64(total), length)
		cancel()
		if err != nil {
			return total, remoteMountError(err)
		}
		count := copy(buffer[total:], response.Data)
		total += count
		if count != len(response.Data) || len(response.Data) > length {
			return total, catalog.ErrIntegrity
		}
		if response.EOF {
			if total < len(buffer) {
				return total, io.EOF
			}
			return total, nil
		}
		if count == 0 {
			return total, catalog.ErrIntegrity
		}
	}
	return total, nil
}

func (s *remoteWriteStage) WriteAt(buffer []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, catalog.ErrHandleClosed
	}
	if s.failed {
		return 0, fmt.Errorf("%w: failed staged mutation must be aborted", catalog.ErrInvalidTransition)
	}
	if s.pending {
		return 0, fmt.Errorf("%w: write commit outcome must be recovered before modification", catalog.ErrConflict)
	}
	if offset < 0 || int64(len(buffer)) > maxNativeObjectSize-offset {
		return 0, catalog.ErrInvalidObject
	}
	total := 0
	for total < len(buffer) {
		length := min(len(buffer)-total, nativeCallbackChunkLimit)
		ctx, cancel := callbackContext(context.Background())
		written, err := s.client.NativeWrite(ctx, s.handle, offset+int64(total), buffer[total:total+length])
		cancel()
		if err != nil {
			s.failed = true
			return total, remoteMountError(err)
		}
		if written != length {
			s.failed = true
			return total, catalog.ErrIntegrity
		}
		total += written
	}
	s.size = max(s.size, offset+int64(total))
	s.dirty = true
	return total, nil
}

func (s *remoteWriteStage) Truncate(size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return catalog.ErrHandleClosed
	}
	if s.failed {
		return fmt.Errorf("%w: failed staged mutation must be aborted", catalog.ErrInvalidTransition)
	}
	if s.pending {
		return fmt.Errorf("%w: write commit outcome must be recovered before truncation", catalog.ErrConflict)
	}
	ctx, cancel := callbackContext(context.Background())
	err := s.client.NativeWriteTruncate(ctx, s.handle, size)
	cancel()
	if err != nil {
		s.failed = true
		return remoteMountError(err)
	}
	s.size = size
	s.dirty = true
	return nil
}

func (s *remoteWriteStage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return catalog.ErrHandleClosed
	}
	ctx, cancel := callbackContext(context.Background())
	err := s.client.NativeWriteSync(ctx, s.handle)
	cancel()
	return remoteMountError(err)
}

func (s *remoteWriteStage) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

func (s *remoteWriteStage) Commit(parent context.Context) (catalog.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitLocked(parent)
}

func (s *remoteWriteStage) commitLocked(parent context.Context) (catalog.Object, error) {
	if s.closed {
		return catalog.Object{}, catalog.ErrHandleClosed
	}
	if s.failed {
		return catalog.Object{}, fmt.Errorf("%w: failed staged mutation cannot be committed", catalog.ErrInvalidTransition)
	}
	if !s.pending {
		if !s.dirty {
			return catalog.Object{}, fmt.Errorf("%w: clean write stage cannot be committed", catalog.ErrConflict)
		}
		s.pending = true
	}

	for attempt := 0; attempt < 2; attempt++ {
		ctx, cancel := callbackContext(parent)
		response, err := s.client.NativeWriteCommit(ctx, s.handle)
		cancel()
		if err != nil {
			if attempt == 0 && recoverableNativeCommit(err) && (parent == nil || parent.Err() == nil) {
				continue
			}
			return catalog.Object{}, remoteMountError(err)
		}
		if response.Object == nil {
			return catalog.Object{}, catalog.ErrIntegrity
		}
		object, err := nativeMountObject(s.tenant, *response.Object)
		if err != nil {
			return catalog.Object{}, err
		}
		mutation, err := catalog.ParseMutationID(string(response.MutationID))
		if err != nil || mutation.TargetRevision() != object.Revision {
			return catalog.Object{}, fmt.Errorf("%w: native commit returned an invalid derived mutation", catalog.ErrIntegrity)
		}
		s.size = object.Size
		s.dirty = false
		s.pending = false
		return object, nil
	}
	panic("unreachable")
}

func (s *remoteWriteStage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.pending {
		if _, err := s.commitLocked(context.Background()); err != nil {
			return err
		}
	}
	ctx, cancel := callbackContext(context.Background())
	err := s.client.NativeWriteAbort(ctx, s.handle)
	cancel()
	if err != nil {
		return remoteMountError(err)
	}
	s.closed = true
	return nil
}

const (
	nativeCallbackChunkLimit = 1 << 20
	maxNativeObjectSize      = int64(1<<63 - 1)
)

func recoverableNativeCommit(err error) bool {
	var transport *mountservice.TransportError
	if errors.As(err, &transport) {
		return true
	}
	var remote *mountservice.RemoteError
	return errors.As(err, &remote) &&
		(remote.Code == mountproto.ErrorCodeCanceled || remote.Code == mountproto.ErrorCodeUnavailable)
}

func nativeMountObject(tenantID catalog.TenantID, object mountproto.NativeObject) (catalog.Object, error) {
	id, err := catalog.ParseObjectID(object.ID)
	if err != nil {
		return catalog.Object{}, fmt.Errorf("%w: invalid object identity: %v", catalog.ErrIntegrity, err)
	}
	parent, err := catalog.ParseObjectID(object.ParentID)
	if err != nil {
		return catalog.Object{}, fmt.Errorf("%w: invalid parent identity: %v", catalog.ErrIntegrity, err)
	}
	kind := catalog.KindDirectory
	switch object.Kind {
	case mountproto.ObjectKindDirectory:
	case mountproto.ObjectKindFile:
		kind = catalog.KindFile
	case mountproto.ObjectKindSymlink:
		kind = catalog.KindSymlink
	default:
		return catalog.Object{}, catalog.ErrIntegrity
	}
	var hash catalog.ContentHash
	if object.Hash != "" {
		decoded, err := hex.DecodeString(object.Hash)
		if err != nil || len(decoded) != len(hash) {
			return catalog.Object{}, catalog.ErrIntegrity
		}
		copy(hash[:], decoded)
	}
	return catalog.Object{
		Tenant: tenantID, ID: id, Parent: parent, Name: object.Name, Kind: kind, Mode: object.Mode,
		Size: object.Size, Hash: hash, LinkTarget: object.LinkTarget,
		Revision: catalog.Revision(object.Revision), MetadataRevision: catalog.Revision(object.MetadataRevision),
		ContentRevision: catalog.Revision(object.ContentRevision),
		Convergence: catalog.Convergence{
			Desired: catalog.Revision(object.Desired), Observed: catalog.Revision(object.Observed),
			Verified: catalog.Revision(object.Verified), Applied: catalog.Revision(object.Applied),
		},
		Visibility: catalog.Visibility{Mount: true},
	}, nil
}
