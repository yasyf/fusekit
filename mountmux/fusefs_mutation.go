//go:build darwin && cgo && fuse

package mountmux

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"syscall"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

type mutationMetadata struct {
	Operation string `json:"operation"`
	Path      string `json:"path,omitempty"`
	From      string `json:"from,omitempty"`
	To        string `json:"to,omitempty"`
}

// Create publishes an empty file through the canonical prepared-mutation lane and opens it for writing.
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
	ref, err := view.StageContent(ctx, bytes.NewReader(nil))
	if err != nil {
		return errno(err), invalidHandle
	}
	head, err := view.Head(ctx)
	if err != nil {
		return errno(err), invalidHandle
	}
	sourcePath, err := tenantRelativePath(value)
	if err != nil {
		return errno(err), invalidHandle
	}
	intent, err := fs.intent(pin, mutationMetadata{Operation: "create", Path: sourcePath})
	if err != nil {
		return errno(err), invalidHandle
	}
	intent.Create = &catalog.CreateMutation{Spec: catalog.CreateSpec{
		Parent: parent.Object.ID, Name: name, Kind: catalog.KindFile, Mode: mode & 0o7777,
		ContentRevision: 1, Content: ref, Convergence: catalog.Convergence{Desired: 1},
		Visibility: parent.Object.Visibility,
	}}
	if err := fs.applyIntent(ctx, pin, view, head, intent); err != nil {
		return errno(err), invalidHandle
	}
	entry, err := view.LookupName(ctx, parent.Object.ID, name)
	if err != nil {
		return errno(err), invalidHandle
	}
	opened, err := fs.newWriteHandle(ctx, pin, view, entry.Object)
	if err != nil {
		return errno(err), invalidHandle
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

// Flush commits dirty staged bytes through the tenant actor.
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

// Mkdir creates one catalog directory through the prepared-mutation lane.
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
	head, err := view.Head(ctx)
	if err != nil {
		return errno(err)
	}
	sourcePath, err := tenantRelativePath(value)
	if err != nil {
		return errno(err)
	}
	intent, err := fs.intent(pin, mutationMetadata{Operation: "mkdir", Path: sourcePath})
	if err != nil {
		return errno(err)
	}
	intent.Create = &catalog.CreateMutation{Spec: catalog.CreateSpec{
		Parent: parent.Object.ID, Name: name, Kind: catalog.KindDirectory,
		Mode: mode & 0o7777, Visibility: parent.Object.Visibility,
	}}
	return errno(fs.applyIntent(ctx, pin, view, head, intent))
}

// Unlink tombstones one file through the prepared-mutation lane.
func (fs *FuseFS) Unlink(value string) int { return fs.remove(value, catalog.KindFile) }

// Rmdir tombstones one empty directory through the prepared-mutation lane.
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
	head, err := view.Head(ctx)
	if err != nil {
		return errno(err)
	}
	operation := "unlink"
	if want == catalog.KindDirectory {
		operation = "rmdir"
	}
	sourcePath, err := tenantRelativePath(value)
	if err != nil {
		return errno(err)
	}
	intent, err := fs.intent(pin, mutationMetadata{Operation: operation, Path: sourcePath})
	if err != nil {
		return errno(err)
	}
	intent.Delete = &catalog.DeleteMutation{Object: entry.Object.ID}
	err = fs.applyIntent(ctx, pin, view, head, intent)
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
	head, err := view.Head(ctx)
	if err != nil {
		return errno(err)
	}
	fromSource, err := tenantRelativePath(from)
	if err != nil {
		return errno(err)
	}
	toSource, err := tenantRelativePath(to)
	if err != nil {
		return errno(err)
	}
	intent, err := fs.intent(pin, mutationMetadata{Operation: "rename", From: fromSource, To: toSource})
	if err != nil {
		return errno(err)
	}
	target, lookupErr := view.LookupName(ctx, parent.Object.ID, name)
	switch {
	case lookupErr == nil:
		if source.Object.ID == target.Object.ID {
			intent.Revise = &catalog.ReviseMutation{Object: source.Object.ID, Spec: catalog.RevisionSpec{
				Parent: parent.Object.ID, Name: name, Mode: source.Object.Mode,
				Convergence: source.Object.Convergence, Visibility: source.Object.Visibility,
			}}
			break
		}
		if source.Object.Kind != target.Object.Kind {
			if source.Object.Kind == catalog.KindDirectory {
				return -int(syscall.ENOTDIR)
			}
			return -int(syscall.EISDIR)
		}
		if target.Object.Kind == catalog.KindDirectory {
			page, err := view.ReadDir(ctx, target.Object.ID, head, catalog.SnapshotCursor{}, 1)
			if err != nil {
				return errno(err)
			}
			if len(page.Entries) != 0 {
				return -int(syscall.ENOTEMPTY)
			}
		}
		parentID := parent.Object.ID
		intent.Replace = &catalog.ReplaceMutation{
			Source: source.Object.ID, Target: target.Object.ID, Parent: &parentID, Name: &name,
		}
	case errors.Is(lookupErr, catalog.ErrNotFound):
		intent.Revise = &catalog.ReviseMutation{Object: source.Object.ID, Spec: catalog.RevisionSpec{
			Parent: parent.Object.ID, Name: name, Mode: source.Object.Mode,
			Convergence: source.Object.Convergence, Visibility: source.Object.Visibility,
		}}
	default:
		return errno(lookupErr)
	}
	return errno(fs.applyIntent(ctx, pin, view, head, intent))
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
	head, err := view.Head(ctx)
	if err != nil {
		return errno(err)
	}
	sourcePath, err := tenantRelativePath(value)
	if err != nil {
		return errno(err)
	}
	intent, err := fs.intent(pin, mutationMetadata{Operation: "chmod", Path: sourcePath})
	if err != nil {
		return errno(err)
	}
	intent.Revise = &catalog.ReviseMutation{Object: entry.Object.ID, Spec: catalog.RevisionSpec{
		Parent: entry.Object.Parent, Name: entry.Object.Name, Mode: mode & 0o7777,
		Convergence: entry.Object.Convergence, Visibility: entry.Object.Visibility,
	}}
	return errno(fs.applyIntent(ctx, pin, view, head, intent))
}

func (fs *FuseFS) newWriteHandle(ctx context.Context, pin *PinnedRoute, view *CatalogFS, object catalog.Object) (*fileHandle, error) {
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
	ref, err := opened.fs.StageContent(ctx, opened.staging)
	if err != nil {
		return err
	}
	current, err := opened.fs.Lookup(ctx, opened.object.ID)
	if err != nil {
		return err
	}
	head, err := opened.fs.Head(ctx)
	if err != nil {
		return err
	}
	nextContent := current.Object.ContentRevision + 1
	convergence := current.Object.Convergence
	convergence.Desired = nextContent
	sourcePath, err := fs.objectPath(ctx, opened.fs, current.Object)
	if err != nil {
		return err
	}
	metadata, err := mutationMetadataJSON(mutationMetadata{Operation: "write", Path: sourcePath})
	if err != nil {
		return err
	}
	intent := catalog.MutationIntent{
		SourceID: opened.pin.Spec.Content.ID, SourceMetadata: metadata,
		Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
		Revise: &catalog.ReviseMutation{Object: current.Object.ID, Spec: catalog.RevisionSpec{
			Parent: current.Object.Parent, Name: current.Object.Name, Mode: current.Object.Mode,
			Content:     &catalog.ContentUpdate{Revision: nextContent, Ref: ref},
			Convergence: convergence, Visibility: current.Object.Visibility,
		}},
	}
	if err := fs.applyIntent(ctx, opened.pin, opened.fs, head, intent); err != nil {
		return err
	}
	updated, err := opened.fs.Lookup(ctx, current.Object.ID)
	if err != nil {
		return err
	}
	opened.object = updated.Object
	opened.dirty = false
	return nil
}

func (fs *FuseFS) intent(pin *PinnedRoute, metadata mutationMetadata) (catalog.MutationIntent, error) {
	encoded, err := mutationMetadataJSON(metadata)
	if err != nil {
		return catalog.MutationIntent{}, err
	}
	return catalog.MutationIntent{
		SourceID: pin.Spec.Content.ID, SourceMetadata: encoded,
		Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
	}, nil
}

func mutationMetadataJSON(metadata mutationMetadata) (string, error) {
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("mountmux: encode source mutation metadata: %w", err)
	}
	return string(encoded), nil
}

func (fs *FuseFS) applyIntent(
	ctx context.Context,
	pin *PinnedRoute,
	view *CatalogFS,
	expected catalog.Revision,
	intent catalog.MutationIntent,
) error {
	requested := expected + 1
	if requested == 0 {
		return fmt.Errorf("%w: catalog revision exhausted", catalog.ErrInvalidTransition)
	}
	id, err := catalog.NewMutationID()
	if err != nil {
		return err
	}
	if _, err := view.BeginMutation(ctx, id, expected, intent); err != nil {
		return err
	}
	state, err := pin.lease.Prepare(ctx, requested)
	if err != nil {
		return err
	}
	if !state.Prepared() || state.Requested != requested || state.Generation != pin.Route.Generation {
		return fmt.Errorf("%w: mutation preparation did not prove revision %d", catalog.ErrIntegrity, requested)
	}
	result, err := view.CommitMutation(ctx, id)
	if err != nil {
		return err
	}
	if result.Mutation.ID != id || result.Mutation.Revision != requested {
		return fmt.Errorf("%w: mutation journal did not prove revision %d", catalog.ErrIntegrity, requested)
	}
	head, err := view.Head(ctx)
	if err != nil {
		return err
	}
	if head < requested {
		return fmt.Errorf("%w: mutation head %d is behind %d", catalog.ErrIntegrity, head, requested)
	}
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

func (fs *FuseFS) objectPath(ctx context.Context, view *CatalogFS, object catalog.Object) (string, error) {
	root, err := view.Root(ctx)
	if err != nil {
		return "", err
	}
	var names []string
	for object.ID != root.Object.ID {
		names = append(names, object.Name)
		parent, err := view.Lookup(ctx, object.Parent)
		if err != nil {
			return "", err
		}
		object = parent.Object
	}
	slices.Reverse(names)
	return "/" + strings.Join(names, "/"), nil
}

func (fs *FuseFS) isDescendant(ctx context.Context, view *CatalogFS, object, ancestor catalog.ObjectID) (bool, error) {
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

func tenantRelativePath(value string) (string, error) {
	_, parts, err := splitTenantPath(value)
	if err != nil {
		return "", err
	}
	return "/" + strings.Join(parts, "/"), nil
}

func isTenantRoot(value string) bool {
	_, parts, err := splitTenantPath(value)
	return err == nil && len(parts) == 0
}

func refreshParent(ctx context.Context, view *CatalogFS, value string) (Entry, string, error) {
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
