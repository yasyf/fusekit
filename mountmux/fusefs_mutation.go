//go:build darwin && cgo && fuse

package mountmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/tenant"
)

// Create publishes an empty file through the holder-owned catalog mutation lane.
func (fs *FuseFS) Create(value string, flags int, mode uint32) (int, uint64) {
	if appleDouble(pathBase(value)) {
		return -int(syscall.EACCES), invalidHandle
	}
	ctx := context.Background()
	pin, view, parent, name, err := fs.resolveParent(ctx, value)
	if err != nil {
		return errno(err), invalidHandle
	}
	if pin.Spec.Traits.Access != tenant.ReadWrite {
		pin.Release()
		return -int(syscall.EROFS), invalidHandle
	}
	lane := fs.mutationLane(pin.Route.Tenant)
	lane.Lock()
	defer lane.Unlock()
	defer func() {
		if pin != nil {
			pin.Release()
		}
	}()
	parent, name, err = refreshParent(ctx, view, value)
	if err != nil {
		return errno(err), invalidHandle
	}
	kind := catalogproto.ObjectKindFile
	parentID := protocolObjectID(parent.Object.ID)
	contentRevision := uint64(1)
	permissions := mode & 0o7777
	response, err := view.Mutate(ctx, catalogproto.MutationRequest{
		Kind: catalogproto.MutationKindCreate, ObjectKind: &kind, HasContent: true,
		ParentID: &parentID, Name: &name, Mode: &permissions, ContentRevision: &contentRevision,
	}, bytes.NewReader(nil))
	if err != nil {
		return errno(err), invalidHandle
	}
	created, err := mutationPrimary(response)
	if err != nil {
		return errno(err), invalidHandle
	}
	entry, err := view.Lookup(ctx, created)
	if err != nil {
		return errno(err), invalidHandle
	}
	opened, err := fs.newWriteHandle(ctx, pin, view, entry.Object)
	if err != nil {
		return errno(err), invalidHandle
	}
	if flags&syscall.O_TRUNC != 0 {
		if err := opened.staging.Truncate(0); err != nil {
			_ = opened.staging.Close()
			return errno(err), invalidHandle
		}
		opened.dirty = true
	}
	pin = nil
	return 0, fs.storeFile(opened)
}

// Write writes into the private staging file owned by an open catalog handle.
func (fs *FuseFS) Write(_ string, buffer []byte, offset int64, handle uint64) int {
	opened := fs.file(handle)
	if opened == nil || opened.staging == nil {
		return -int(syscall.EBADF)
	}
	if offset < 0 {
		return -int(syscall.EINVAL)
	}
	opened.mu.Lock()
	defer opened.mu.Unlock()
	n, err := opened.staging.WriteAt(buffer, offset)
	if err != nil {
		return errno(err)
	}
	opened.dirty = true
	return n
}

// Truncate changes a write handle's staged size and commits path-only truncation synchronously.
func (fs *FuseFS) Truncate(value string, size int64, handle uint64) int {
	if size < 0 {
		return -int(syscall.EINVAL)
	}
	if handle != invalidHandle {
		opened := fs.file(handle)
		if opened == nil || opened.staging == nil {
			return -int(syscall.EBADF)
		}
		opened.mu.Lock()
		defer opened.mu.Unlock()
		if err := opened.staging.Truncate(size); err != nil {
			return errno(err)
		}
		opened.dirty = true
		return 0
	}
	rc, temporary := fs.Open(value, syscall.O_WRONLY)
	if rc != 0 {
		return rc
	}
	if rc = fs.Truncate(value, size, temporary); rc != 0 {
		_ = fs.Release(value, temporary)
		return rc
	}
	return fs.Release(value, temporary)
}

// Flush commits dirty staged bytes through the holder-owned tenant actor.
func (fs *FuseFS) Flush(_ string, handle uint64) int {
	opened := fs.file(handle)
	if opened == nil {
		return -int(syscall.EBADF)
	}
	return errno(fs.commitWrite(context.Background(), opened))
}

// Fsync durably commits dirty bytes before returning.
func (fs *FuseFS) Fsync(_ string, _ bool, handle uint64) int {
	opened := fs.file(handle)
	if opened == nil {
		return -int(syscall.EBADF)
	}
	if opened.staging != nil {
		opened.mu.Lock()
		err := opened.staging.Sync()
		opened.mu.Unlock()
		if err != nil {
			return errno(err)
		}
	}
	return errno(fs.commitWrite(context.Background(), opened))
}

// Mkdir creates one catalog directory through the holder-owned mutation lane.
func (fs *FuseFS) Mkdir(value string, mode uint32) int {
	if appleDouble(pathBase(value)) {
		return -int(syscall.EACCES)
	}
	ctx := context.Background()
	pin, view, parent, name, err := fs.resolveParent(ctx, value)
	if err != nil {
		return errno(err)
	}
	defer pin.Release()
	if pin.Spec.Traits.Access != tenant.ReadWrite {
		return -int(syscall.EROFS)
	}
	lane := fs.mutationLane(pin.Route.Tenant)
	lane.Lock()
	defer lane.Unlock()
	parent, name, err = refreshParent(ctx, view, value)
	if err != nil {
		return errno(err)
	}
	kind := catalogproto.ObjectKindDirectory
	parentID := protocolObjectID(parent.Object.ID)
	permissions := mode & 0o7777
	_, err = view.Mutate(ctx, catalogproto.MutationRequest{
		Kind: catalogproto.MutationKindCreate, ObjectKind: &kind,
		ParentID: &parentID, Name: &name, Mode: &permissions,
	}, nil)
	return errno(err)
}

// Unlink tombstones one file through the holder-owned mutation lane.
func (fs *FuseFS) Unlink(value string) int { return fs.remove(value, catalog.KindFile) }

// Rmdir tombstones one empty directory through the holder-owned mutation lane.
func (fs *FuseFS) Rmdir(value string) int { return fs.remove(value, catalog.KindDirectory) }

func (fs *FuseFS) remove(value string, want catalog.Kind) int {
	if isTenantRoot(value) {
		return -int(syscall.EPERM)
	}
	ctx := context.Background()
	pin, view, entry, err := fs.resolve(ctx, value)
	if err != nil {
		return errno(err)
	}
	defer pin.Release()
	if pin.Spec.Traits.Access != tenant.ReadWrite {
		return -int(syscall.EROFS)
	}
	lane := fs.mutationLane(pin.Route.Tenant)
	lane.Lock()
	defer lane.Unlock()
	_, parts, err := splitTenantPath(value)
	if err != nil {
		return errno(err)
	}
	entry, err = lookupParts(ctx, view, parts)
	if err != nil {
		return errno(err)
	}
	if entry.Object.Kind != want {
		if want == catalog.KindDirectory {
			return -int(syscall.ENOTDIR)
		}
		return -int(syscall.EISDIR)
	}
	id := protocolObjectID(entry.Object.ID)
	_, err = view.Mutate(ctx, catalogproto.MutationRequest{
		Kind: catalogproto.MutationKindDelete, ObjectID: &id,
	}, nil)
	if want == catalog.KindDirectory && errors.Is(err, catalog.ErrConflict) {
		return -int(syscall.ENOTEMPTY)
	}
	return errno(err)
}

// Rename preserves the source object identity and atomically tombstones an existing target.
func (fs *FuseFS) Rename(from, to string) int {
	if from == to {
		return 0
	}
	ctx := context.Background()
	fromRoute, fromParts, err := splitTenantPath(from)
	if err != nil {
		return errno(err)
	}
	toRoute, toParts, err := splitTenantPath(to)
	if err != nil {
		if appleDouble(pathBase(to)) {
			return -int(syscall.EACCES)
		}
		return errno(err)
	}
	if fromRoute == "" || toRoute == "" || len(fromParts) == 0 || len(toParts) == 0 {
		return -int(syscall.EPERM)
	}
	if routeKey(fromRoute) != routeKey(toRoute) {
		return -int(syscall.EXDEV)
	}
	pin, view, _, err := fs.pinPath(ctx, from)
	if err != nil {
		return errno(err)
	}
	defer pin.Release()
	if pin.Spec.Traits.Access != tenant.ReadWrite {
		return -int(syscall.EROFS)
	}
	lane := fs.mutationLane(pin.Route.Tenant)
	lane.Lock()
	defer lane.Unlock()
	source, err := lookupParts(ctx, view, fromParts)
	if err != nil {
		return errno(err)
	}
	parent, err := lookupParts(ctx, view, toParts[:len(toParts)-1])
	if err != nil {
		return errno(err)
	}
	if parent.Object.Kind != catalog.KindDirectory {
		return -int(syscall.ENOTDIR)
	}
	if source.Object.Kind == catalog.KindDirectory {
		inside, err := fs.isDescendant(ctx, view, parent.Object.ID, source.Object.ID)
		if err != nil {
			return errno(err)
		}
		if inside {
			return -int(syscall.EINVAL)
		}
	}
	name := toParts[len(toParts)-1]
	if appleDouble(name) {
		return -int(syscall.EACCES)
	}
	sourceID := protocolObjectID(source.Object.ID)
	parentID := protocolObjectID(parent.Object.ID)
	permissions := source.Object.Mode
	request := catalogproto.MutationRequest{
		Kind: catalogproto.MutationKindRevise, ObjectID: &sourceID,
		ParentID: &parentID, Name: &name, Mode: &permissions,
	}
	target, lookupErr := view.LookupName(ctx, parent.Object.ID, name)
	switch {
	case lookupErr == nil:
		if source.Object.ID == target.Object.ID {
			break
		}
		if source.Object.Kind != target.Object.Kind {
			if source.Object.Kind == catalog.KindDirectory {
				return -int(syscall.ENOTDIR)
			}
			return -int(syscall.EISDIR)
		}
		if target.Object.Kind == catalog.KindDirectory {
			head, err := view.Head(ctx)
			if err != nil {
				return errno(err)
			}
			page, err := view.ReadDir(ctx, target.Object.ID, head, catalog.SnapshotCursor{}, 1)
			if err != nil {
				return errno(err)
			}
			if len(page.Entries) != 0 {
				return -int(syscall.ENOTEMPTY)
			}
		}
		targetID := protocolObjectID(target.Object.ID)
		request.Kind = catalogproto.MutationKindReplace
		request.TargetID = &targetID
	case errors.Is(lookupErr, catalog.ErrNotFound):
	default:
		return errno(lookupErr)
	}
	_, err = view.Mutate(ctx, request, nil)
	return errno(err)
}

// Chmod revises only the catalog object's permission bits.
func (fs *FuseFS) Chmod(value string, mode uint32) int {
	if isTenantRoot(value) {
		return -int(syscall.EPERM)
	}
	ctx := context.Background()
	pin, view, entry, err := fs.resolve(ctx, value)
	if err != nil {
		return errno(err)
	}
	defer pin.Release()
	if pin.Spec.Traits.Access != tenant.ReadWrite {
		return -int(syscall.EROFS)
	}
	lane := fs.mutationLane(pin.Route.Tenant)
	lane.Lock()
	defer lane.Unlock()
	_, parts, err := splitTenantPath(value)
	if err != nil {
		return errno(err)
	}
	entry, err = lookupParts(ctx, view, parts)
	if err != nil {
		return errno(err)
	}
	id := protocolObjectID(entry.Object.ID)
	parentID := protocolObjectID(entry.Object.Parent)
	name := entry.Object.Name
	permissions := mode & 0o7777
	_, err = view.Mutate(ctx, catalogproto.MutationRequest{
		Kind: catalogproto.MutationKindRevise, ObjectID: &id,
		ParentID: &parentID, Name: &name, Mode: &permissions,
	}, nil)
	return errno(err)
}

func (fs *FuseFS) newWriteHandle(ctx context.Context, pin *PinnedRoute, view *nativeView, object catalog.Object) (*fileHandle, error) {
	head, err := view.Head(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := view.Open(ctx, object.ID, head)
	if err != nil {
		return nil, err
	}
	staging, err := privateStagingFile()
	if err != nil {
		_ = snapshot.Close()
		return nil, err
	}
	if _, err := io.Copy(staging, snapshot); err != nil {
		_ = staging.Close()
		_ = snapshot.Close()
		return nil, fmt.Errorf("mountmux: seed write staging: %w", err)
	}
	return &fileHandle{pin: pin, fs: view, object: snapshot.Object, snapshot: snapshot, staging: staging}, nil
}

func privateStagingFile() (*os.File, error) {
	file, err := os.CreateTemp("", "fusekit-mount-write-")
	if err != nil {
		return nil, fmt.Errorf("mountmux: create write staging: %w", err)
	}
	name := file.Name()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("mountmux: secure write staging: %w", err)
	}
	if err := os.Remove(name); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("mountmux: unlink write staging: %w", err)
	}
	return file, nil
}

func (fs *FuseFS) commitWrite(ctx context.Context, opened *fileHandle) error {
	if opened.staging == nil {
		return nil
	}
	opened.mu.Lock()
	defer opened.mu.Unlock()
	if !opened.dirty {
		return nil
	}
	lane := fs.mutationLane(opened.pin.Route.Tenant)
	lane.Lock()
	defer lane.Unlock()
	if err := opened.staging.Sync(); err != nil {
		return fmt.Errorf("mountmux: sync write staging: %w", err)
	}
	if _, err := opened.staging.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("mountmux: rewind write staging: %w", err)
	}
	current, err := opened.fs.Lookup(ctx, opened.object.ID)
	if err != nil {
		return err
	}
	id := protocolObjectID(current.Object.ID)
	parentID := protocolObjectID(current.Object.Parent)
	name := current.Object.Name
	permissions := current.Object.Mode
	contentRevision := uint64(current.Object.ContentRevision + 1)
	response, err := opened.fs.Mutate(ctx, catalogproto.MutationRequest{
		Kind: catalogproto.MutationKindRevise, HasContent: true, ObjectID: &id,
		ParentID: &parentID, Name: &name, Mode: &permissions, ContentRevision: &contentRevision,
	}, opened.staging)
	if err != nil {
		return err
	}
	updatedID, err := mutationPrimary(response)
	if err != nil {
		return err
	}
	if updatedID != current.Object.ID {
		return fmt.Errorf("%w: write mutation changed object identity", catalog.ErrIntegrity)
	}
	updated, err := opened.fs.Lookup(ctx, updatedID)
	if err != nil {
		return err
	}
	opened.object = updated.Object
	opened.dirty = false
	return nil
}

func (fs *FuseFS) mutationLane(id catalog.TenantID) *sync.Mutex {
	fs.mutationMu.Lock()
	defer fs.mutationMu.Unlock()
	lane := fs.mutationLanes[id]
	if lane == nil {
		lane = &sync.Mutex{}
		fs.mutationLanes[id] = lane
	}
	return lane
}

func (fs *FuseFS) isDescendant(ctx context.Context, view *nativeView, object, ancestor catalog.ObjectID) (bool, error) {
	root, err := view.Root(ctx)
	if err != nil {
		return false, err
	}
	for {
		if object == ancestor {
			return true, nil
		}
		if object == root.Object.ID {
			return false, nil
		}
		entry, err := view.Lookup(ctx, object)
		if err != nil {
			return false, err
		}
		object = entry.Object.Parent
	}
}

func isTenantRoot(value string) bool {
	_, parts, err := splitTenantPath(value)
	return err == nil && len(parts) == 0
}

func refreshParent(ctx context.Context, view *nativeView, value string) (Entry, string, error) {
	_, parts, err := splitTenantPath(value)
	if err != nil {
		return Entry{}, "", err
	}
	if len(parts) == 0 {
		return Entry{}, "", syscall.EPERM
	}
	parent, err := lookupParts(ctx, view, parts[:len(parts)-1])
	if err != nil {
		return Entry{}, "", err
	}
	if parent.Object.Kind != catalog.KindDirectory {
		return Entry{}, "", syscall.ENOTDIR
	}
	return parent, parts[len(parts)-1], nil
}

func pathBase(value string) string {
	for index := len(value) - 1; index >= 0; index-- {
		if value[index] == '/' {
			return value[index+1:]
		}
	}
	return value
}

func protocolObjectID(id catalog.ObjectID) catalogproto.ObjectID {
	return catalogproto.ObjectID(id.String())
}

func mutationPrimary(response catalogproto.MutationResponse) (catalog.ObjectID, error) {
	if response.PrimaryID == nil {
		return catalog.ObjectID{}, fmt.Errorf("%w: mutation response has no primary object", catalog.ErrIntegrity)
	}
	id, err := catalog.ParseObjectID(string(*response.PrimaryID))
	if err != nil {
		return catalog.ObjectID{}, fmt.Errorf("%w: mutation response primary object: %v", catalog.ErrIntegrity, err)
	}
	return id, nil
}
