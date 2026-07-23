//go:build darwin && cgo && fuse

package mountmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

func TestFuseFSRootAndPagedDirectoryEnumeration(t *testing.T) {
	fixture := newCallbackFixture(t, "paged")
	fixture.callbacks.Init()
	if epoch := fixture.callbacks.rootReadEpoch.Load(); epoch != 0 {
		t.Fatal("Init alone proved catalog-backed root readiness")
	}
	for index := 0; index < directoryPage+7; index++ {
		createFile(t, fixture.view, fixture.root.Object.ID, fmt.Sprintf("file-%03d", index), "x", true)
	}

	oneRC, oneHandle := fixture.callbacks.Opendir("/")
	if oneRC != 0 {
		t.Fatalf("Opendir(one-entry root) = %d", oneRC)
	}
	filled := 0
	if rc := fixture.callbacks.Readdir("/", func(string, *fuse.Stat_t, int64) bool {
		filled++
		return false
	}, 0, oneHandle); rc != 0 {
		t.Fatalf("Readdir(one-entry root) = %d", rc)
	}
	if filled != 1 || fixture.callbacks.rootReadEpoch.Load() != 0 {
		t.Fatalf("ordinary root read changed readiness = fills %d, epoch %d", filled, fixture.callbacks.rootReadEpoch.Load())
	}
	if rc := fixture.callbacks.Releasedir("/", oneHandle); rc != 0 {
		t.Fatalf("Releasedir(one-entry root) = %d", rc)
	}

	rootBefore := fixture.resolver.releases.Load()
	rootRC, rootHandle := fixture.callbacks.Opendir("/")
	if rootRC != 0 {
		t.Fatalf("Opendir(root) = %d", rootRC)
	}
	var rootNames []string
	if rc := fixture.callbacks.Readdir("/", collectNames(&rootNames), 0, rootHandle); rc != 0 {
		t.Fatalf("Readdir(root) = %d", rc)
	}
	if fmt.Sprint(rootNames) != "[. .. acct]" {
		t.Fatalf("root names = %v", rootNames)
	}
	if fixture.resolver.releases.Load() != rootBefore {
		t.Fatal("root directory released its tenant pin before Releasedir")
	}
	if rc := fixture.callbacks.Releasedir("/", rootHandle); rc != 0 {
		t.Fatalf("Releasedir(root) = %d", rc)
	}
	if fixture.resolver.releases.Load() != rootBefore+1 {
		t.Fatal("root directory did not release its tenant pin")
	}

	before := fixture.resolver.releases.Load()
	rc, handle := fixture.callbacks.Opendir("/acct")
	if rc != 0 {
		t.Fatalf("Opendir(tenant) = %d", rc)
	}
	var names []string
	if rc := fixture.callbacks.Readdir("/acct", collectNames(&names), 0, handle); rc != 0 {
		t.Fatalf("Readdir(tenant) = %d", rc)
	}
	if len(names) != directoryPage+9 {
		t.Fatalf("directory names = %d, want %d", len(names), directoryPage+9)
	}
	directory := fixture.callbacks.directory(handle)
	if directory == nil || len(directory.entries) != directoryPage+7 || !directory.complete {
		t.Fatalf("paged directory = %+v", directory)
	}
	if fixture.resolver.releases.Load() != before {
		t.Fatal("directory generation pin released before Releasedir")
	}
	if rc := fixture.callbacks.Releasedir("/acct", handle); rc != 0 {
		t.Fatalf("Releasedir(tenant) = %d", rc)
	}
	if fixture.resolver.releases.Load() != before+1 {
		t.Fatal("directory generation pin was not released")
	}
}

func TestFuseFSNativeProbeIsCausalSingleCallbackWithoutRoutes(t *testing.T) {
	fixture := newCallbackFixture(t, "native-probe")
	var readinessLog bytes.Buffer
	fixture.callbacks.probeLog = &readinessLog
	token := strings.Repeat("a", 64)
	other := strings.Repeat("b", 64)
	if err := fixture.callbacks.beginNativeProbe(token); err != nil {
		t.Fatal(err)
	}
	if err := fixture.callbacks.beginNativeProbe(other); err == nil {
		t.Fatal("overlapping native probe succeeded")
	}
	var stat fuse.Stat_t
	if rc := fixture.callbacks.Getattr("/"+nativeProbePrefix+other, &stat, invalidHandle); rc != -int(syscall.ENOENT) {
		t.Fatalf("stale probe Getattr = %d", rc)
	}
	if epoch := fixture.callbacks.rootReadEpoch.Load(); epoch != 0 {
		t.Fatalf("stale token advanced epoch to %d", epoch)
	}
	if rc := fixture.callbacks.Getattr("/"+nativeProbePrefix+token, &stat, invalidHandle); rc != -int(syscall.ENOENT) {
		t.Fatalf("exact probe Getattr = %d", rc)
	}
	if rc := fixture.callbacks.Getattr("/"+nativeProbePrefix+token, &stat, invalidHandle); rc != -int(syscall.ENOENT) {
		t.Fatalf("repeated probe Getattr = %d", rc)
	}
	if epoch := fixture.callbacks.rootReadEpoch.Load(); epoch != 1 {
		t.Fatalf("exact token epoch = %d, want one", epoch)
	}
	if _, err := fixture.callbacks.finishNativeProbe(other); err == nil {
		t.Fatal("stale token finished active probe")
	}
	epoch, err := fixture.callbacks.finishNativeProbe(token)
	if err != nil || epoch != 1 {
		t.Fatalf("finish exact probe = %d, %v", epoch, err)
	}
	if fixture.resolver.routePageCalls.Load() != 0 || fixture.resolver.pinCalls.Load() != 0 {
		t.Fatalf("probe entered routes: pages=%d pins=%d", fixture.resolver.routePageCalls.Load(), fixture.resolver.pinCalls.Load())
	}
	if err := fixture.callbacks.beginNativeProbe(other); err != nil {
		t.Fatalf("retry probe: %v", err)
	}
	fixture.callbacks.cancelNativeProbe(token)
	if rc := fixture.callbacks.Getattr("/"+nativeProbePrefix+other, &stat, invalidHandle); rc != -int(syscall.ENOENT) {
		t.Fatalf("retry probe Getattr = %d", rc)
	}
	if epoch, err := fixture.callbacks.finishNativeProbe(other); err != nil || epoch != 2 {
		t.Fatalf("finish retry probe = %d, %v", epoch, err)
	}
	probeID, err := NativeProbeID(token)
	if err != nil {
		t.Fatal(err)
	}
	logged := readinessLog.String()
	if strings.Count(logged, "phase=root_callback_observed") != 2 ||
		!strings.Contains(logged, "probe_id="+probeID+" result=ok root_read_epoch=1") ||
		strings.Contains(logged, token) {
		t.Fatalf("readiness log = %q", logged)
	}
}

func TestFuseFSRootEnumerationNeverAdvancesReadinessEpoch(t *testing.T) {
	for _, empty := range []bool{true, false} {
		t.Run(fmt.Sprintf("empty=%t", empty), func(t *testing.T) {
			fixture := newCallbackFixture(t, "root-read")
			if empty {
				fixture.resolver.mu.Lock()
				clear(fixture.resolver.routes)
				clear(fixture.resolver.specs)
				fixture.resolver.mu.Unlock()
			} else {
				fixture.resolver.add(Route{Tenant: fixture.view.Tenant(), Generation: fixture.view.Generation(), Name: "acct-2"}, fixture.resolver.specs[fixture.view.Tenant()])
			}
			rc, handle := fixture.callbacks.Opendir("/")
			if rc != 0 {
				t.Fatalf("Opendir = %d", rc)
			}
			if rc := fixture.callbacks.Readdir("/", func(string, *fuse.Stat_t, int64) bool { return true }, 0, handle); rc != 0 {
				t.Fatalf("Readdir = %d", rc)
			}
			if epoch := fixture.callbacks.rootReadEpoch.Load(); epoch != 0 {
				t.Fatalf("root enumeration advanced readiness epoch to %d", epoch)
			}
			if rc := fixture.callbacks.Releasedir("/", handle); rc != 0 {
				t.Fatalf("Releasedir = %d", rc)
			}
		})
	}
}

func TestFuseFSReadHandlePinsReplacedBytesAndIdentity(t *testing.T) {
	fixture := newCallbackFixture(t, "snapshot")
	target := createFile(t, fixture.view, fixture.root.Object.ID, "target", "old", true)
	before := fixture.resolver.releases.Load()
	rc, handle := fixture.callbacks.Open("/acct/target", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open = %d", rc)
	}

	temporary := createFile(t, fixture.view, fixture.root.Object.ID, "temporary", "new", true)
	head := mustHead(t, fixture.view)
	name := "target"
	parent := fixture.root.Object.ID
	commitMutation(t, fixture.view, head, catalog.MutationIntent{
		SourceID: "test-source", Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
		Replace: &catalog.ReplaceMutation{Source: temporary.ID, Target: target.ID, Parent: &parent, Name: &name},
	})

	buffer := make([]byte, 8)
	n := fixture.callbacks.Read("/acct/target", buffer, 0, handle)
	if n != 3 || string(buffer[:n]) != "old" {
		t.Fatalf("Read(open target) = %d %q", n, buffer[:max(n, 0)])
	}
	if fixture.resolver.releases.Load() != before {
		t.Fatal("file generation pin released before Release")
	}
	if rc := fixture.callbacks.Release("/acct/target", handle); rc != 0 {
		t.Fatalf("Release = %d", rc)
	}
	if fixture.resolver.releases.Load() != before+1 {
		t.Fatal("file generation pin was not released")
	}
	current, err := fixture.view.LookupName(context.Background(), fixture.root.Object.ID, "target")
	if err != nil || current.Object.ID != temporary.ID {
		t.Fatalf("target identity = %s, %v; want %s", current.Object.ID, err, temporary.ID)
	}
}

func TestFuseFSAtomicCreateWriteRenameOverTarget(t *testing.T) {
	fixture := newCallbackFixture(t, "atomic")
	target := createFile(t, fixture.view, fixture.root.Object.ID, "settings.json", "old", true)
	fixture.syncState(t)
	rc, oldHandle := fixture.callbacks.Open("/acct/settings.json", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open(old target) = %d", rc)
	}

	rc, handle := fixture.callbacks.Create("/acct/.settings.tmp", syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL, 0o600)
	if rc != 0 {
		t.Fatalf("Create(temp) = %d", rc)
	}
	if n := fixture.callbacks.Write("/acct/.settings.tmp", []byte("new"), 0, handle); n != 3 {
		t.Fatalf("Write(temp) = %d", n)
	}
	if rc := fixture.callbacks.Truncate("/acct/.settings.tmp", 3, handle); rc != 0 {
		t.Fatalf("Truncate(temp) = %d", rc)
	}
	if rc := fixture.callbacks.Fsync("/acct/.settings.tmp", false, handle); rc != 0 {
		t.Fatalf("Fsync(temp) = %d", rc)
	}
	temporary, err := fixture.view.LookupName(context.Background(), fixture.root.Object.ID, ".settings.tmp")
	if err != nil {
		t.Fatalf("LookupName(temp): %v", err)
	}
	if rc := fixture.callbacks.Release("/acct/.settings.tmp", handle); rc != 0 {
		t.Fatalf("Release(temp) = %d", rc)
	}
	if rc := fixture.callbacks.Rename("/acct/.settings.tmp", "/acct/settings.json"); rc != 0 {
		t.Fatalf("Rename(over target) = %d", rc)
	}

	current, err := fixture.view.LookupName(context.Background(), fixture.root.Object.ID, "settings.json")
	if err != nil {
		t.Fatalf("LookupName(settings): %v", err)
	}
	if current.Object.ID != temporary.Object.ID || current.Object.ID == target.ID {
		t.Fatalf("replacement identity = %s, temp %s, target %s", current.Object.ID, temporary.Object.ID, target.ID)
	}
	rc, currentHandle := fixture.callbacks.Open("/acct/settings.json", syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open(current target) = %d", rc)
	}
	if body := readCallbackFile(t, fixture.callbacks, "/acct/settings.json", currentHandle); body != "new" {
		t.Fatalf("current body = %q", body)
	}
	if rc := fixture.callbacks.Release("/acct/settings.json", currentHandle); rc != 0 {
		t.Fatalf("Release(current target) = %d", rc)
	}
	if body := readCallbackFile(t, fixture.callbacks, "/acct/settings.json", oldHandle); body != "old" {
		t.Fatalf("old open body = %q", body)
	}
	if rc := fixture.callbacks.Release("/acct/settings.json", oldHandle); rc != 0 {
		t.Fatalf("Release(old target) = %d", rc)
	}
}

func TestFuseFSMkdirChmodRemoveAndGuards(t *testing.T) {
	fixture := newCallbackFixture(t, "mutations")
	fixture.syncState(t)
	if rc := fixture.callbacks.Mkdir("/acct/dir", 0o755); rc != 0 {
		t.Fatalf("Mkdir = %d", rc)
	}
	if rc := fixture.callbacks.Chmod("/acct/dir", 0o700); rc != 0 {
		t.Fatalf("Chmod = %d", rc)
	}
	var stat fuse.Stat_t
	if rc := fixture.callbacks.Getattr("/acct/dir", &stat, invalidHandle); rc != 0 {
		t.Fatalf("Getattr(dir) = %d", rc)
	}
	if stat.Mode&0o777 != 0o700 || stat.Mode&fuse.S_IFDIR == 0 {
		t.Fatalf("dir mode = %#o", stat.Mode)
	}
	if rc, handle := fixture.callbacks.Create("/acct/dir/file", syscall.O_RDWR|syscall.O_CREAT, 0o600); rc != 0 {
		t.Fatalf("Create(file) = %d", rc)
	} else if rc := fixture.callbacks.Release("/acct/dir/file", handle); rc != 0 {
		t.Fatalf("Release(file) = %d", rc)
	}
	if rc := fixture.callbacks.Rmdir("/acct/dir"); rc != -int(syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir(nonempty) = %d, want ENOTEMPTY", rc)
	}
	if rc := fixture.callbacks.Unlink("/acct/dir/file"); rc != 0 {
		t.Fatalf("Unlink(file) = %d", rc)
	}
	if rc := fixture.callbacks.Rmdir("/acct/dir"); rc != 0 {
		t.Fatalf("Rmdir(empty) = %d", rc)
	}
	if rc := fixture.callbacks.Mkdir("/acct", 0o755); rc != -int(syscall.EPERM) {
		t.Fatalf("Mkdir(route root) = %d", rc)
	}
	if rc := fixture.callbacks.Chmod("/acct", 0o700); rc != -int(syscall.EPERM) {
		t.Fatalf("Chmod(route root) = %d", rc)
	}
	if rc := fixture.callbacks.Unlink("/acct"); rc != -int(syscall.EPERM) {
		t.Fatalf("Unlink(route root) = %d", rc)
	}
	if rc, _ := fixture.callbacks.Create("/acct/._sidecar", syscall.O_CREAT|syscall.O_RDWR, 0o600); rc != -int(syscall.EACCES) {
		t.Fatalf("Create(AppleDouble) = %d", rc)
	}
	if rc := fixture.callbacks.Getattr("/acct/._sidecar", &stat, invalidHandle); rc != -int(syscall.ENOENT) {
		t.Fatalf("Getattr(AppleDouble) = %d", rc)
	}
}

func TestFuseFSSerializesSameTenantMutations(t *testing.T) {
	fixture := newCallbackFixture(t, "serialized")
	fixture.syncState(t)
	const count = 12
	results := make(chan int, count)
	var group sync.WaitGroup
	for index := 0; index < count; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			results <- fixture.callbacks.Mkdir(fmt.Sprintf("/acct/dir-%02d", index), 0o755)
		}(index)
	}
	group.Wait()
	close(results)
	for rc := range results {
		if rc != 0 {
			t.Fatalf("concurrent Mkdir = %d", rc)
		}
	}
	page, err := fixture.view.ReadDir(context.Background(), fixture.root.Object.ID, 0, catalog.SnapshotCursor{}, count+1)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(page.Entries) != count {
		t.Fatalf("committed directories = %d, want %d", len(page.Entries), count)
	}
}

func TestFuseFSRenameRejectsNonemptyTargetAndDescendant(t *testing.T) {
	fixture := newCallbackFixture(t, "rename-guards")
	fixture.syncState(t)
	for _, value := range []string{"/acct/source", "/acct/target", "/acct/source/child"} {
		if rc := fixture.callbacks.Mkdir(value, 0o755); rc != 0 {
			t.Fatalf("Mkdir(%s) = %d", value, rc)
		}
	}
	if rc, handle := fixture.callbacks.Create("/acct/target/file", syscall.O_RDWR|syscall.O_CREAT, 0o600); rc != 0 {
		t.Fatalf("Create(target child) = %d", rc)
	} else if rc := fixture.callbacks.Release("/acct/target/file", handle); rc != 0 {
		t.Fatalf("Release(target child) = %d", rc)
	}
	if rc := fixture.callbacks.Rename("/acct/source", "/acct/target"); rc != -int(syscall.ENOTEMPTY) {
		t.Fatalf("Rename(nonempty target) = %d, want ENOTEMPTY", rc)
	}
	if rc := fixture.callbacks.Rename("/acct/source", "/acct/source/child/nested"); rc != -int(syscall.EINVAL) {
		t.Fatalf("Rename(into descendant) = %d, want EINVAL", rc)
	}
	if rc := fixture.callbacks.Rename("/acct/source", "/acct/source"); rc != 0 {
		t.Fatalf("Rename(same path) = %d", rc)
	}
}

func TestFuseFSRejectsCrossTenantRename(t *testing.T) {
	fixture := newCallbackFixture(t, "first")
	createFile(t, fixture.view, fixture.root.Object.ID, "file", "body", true)
	second, err := catalog.NewTenantID("second")
	if err != nil {
		t.Fatal(err)
	}
	secondRoot, err := fixture.source.CreateTenant(context.Background(), second, catalog.CaseSensitive, catalog.PresentMount)
	if err != nil {
		t.Fatalf("CreateTenant(second): %v", err)
	}
	if _, err := fixture.source.SaveTenantState(context.Background(), 0, catalog.TenantStateRecord{
		Tenant: second, Generation: 1, ActivatedGeneration: 1,
	}); err != nil {
		t.Fatalf("SaveTenantState(second): %v", err)
	}
	fixture.resolver.add(Route{Tenant: second, Generation: 1, Name: "other"}, tenant.TenantSpec{
		OwnerID: "test", ID: second, PresentationRoot: "/mount/other",
		Backing: tenant.BackingSpec{Root: "/backing/other"}, Content: tenant.ContentSource{ID: "test-source"},
		Traits:     tenant.TenantTraits{Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: catalog.PresentMount},
		Generation: 1,
	})
	_ = secondRoot
	if rc := fixture.callbacks.Rename("/acct/file", "/other/file"); rc != -int(syscall.EXDEV) {
		t.Fatalf("Rename(cross tenant) = %d, want EXDEV", rc)
	}
}

type callbackFixture struct {
	source    *catalog.Catalog
	view      *CatalogFS
	root      Entry
	resolver  *applyingResolver
	callbacks *FuseFS
}

type callbackCatalog struct {
	source   *catalog.Catalog
	resolver *applyingResolver
}

func (c *callbackCatalog) requireGeneration(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation) error {
	state, err := c.source.LoadTenantState(ctx, tenantID)
	if err != nil {
		return err
	}
	if state.Generation != generation || state.ActivatedGeneration != generation {
		return catalog.ErrGenerationMismatch
	}
	return nil
}

func (c *callbackCatalog) Root(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation) (catalog.Object, error) {
	if err := c.requireGeneration(ctx, tenantID, generation); err != nil {
		return catalog.Object{}, err
	}
	return c.source.Root(ctx, tenantID)
}

func (c *callbackCatalog) Head(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation) (catalog.Revision, error) {
	if err := c.requireGeneration(ctx, tenantID, generation); err != nil {
		return 0, err
	}
	return c.source.Head(ctx, tenantID)
}

func (c *callbackCatalog) Lookup(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation, id catalog.ObjectID) (catalog.Object, error) {
	if err := c.requireGeneration(ctx, tenantID, generation); err != nil {
		return catalog.Object{}, err
	}
	return c.source.Lookup(ctx, tenantID, catalog.PresentationMount, id)
}

func (c *callbackCatalog) LookupName(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation, parent catalog.ObjectID, name string) (catalog.Object, error) {
	if err := c.requireGeneration(ctx, tenantID, generation); err != nil {
		return catalog.Object{}, err
	}
	return c.source.LookupName(ctx, tenantID, catalog.PresentationMount, parent, name)
}

func (c *callbackCatalog) Snapshot(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation, parent catalog.ObjectID, revision catalog.Revision, cursor catalog.SnapshotCursor, limit int) (catalog.SnapshotPage, error) {
	if err := c.requireGeneration(ctx, tenantID, generation); err != nil {
		return catalog.SnapshotPage{}, err
	}
	return c.source.Snapshot(ctx, tenantID, catalog.EnumerationScope{
		Kind: catalog.EnumerationContainer, Presentation: catalog.PresentationMount, Parent: parent,
	}, revision, cursor, limit)
}

func (c *callbackCatalog) Open(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation, id catalog.ObjectID, revision catalog.Revision) (*NativeSnapshot, error) {
	handle, err := c.source.OpenAt(
		ctx, catalog.RetentionOwner("mountmux-fuse-test"), tenantID,
		catalog.PresentationMount, generation, id, revision,
	)
	if err != nil {
		return nil, err
	}
	return &NativeSnapshot{Object: handle.Object, Source: handle}, nil
}

func (c *callbackCatalog) OpenWrite(
	ctx context.Context,
	tenantID catalog.TenantID,
	generation catalog.Generation,
	id catalog.ObjectID,
	revision catalog.Revision,
) (*NativeWriteStage, error) {
	snapshot, err := c.Open(ctx, tenantID, generation, id, revision)
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(snapshot)
	closeErr := snapshot.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	stage := &callbackWriteStage{
		catalog: c, tenant: tenantID, generation: generation,
		object: snapshot.Object, body: body,
	}
	return &NativeWriteStage{Object: snapshot.Object, Source: stage}, nil
}

func (c *callbackCatalog) Mutate(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation, request catalogproto.MutationRequest, content io.Reader) (catalogproto.MutationResponse, error) {
	if err := catalogproto.Validate(request); err != nil {
		return catalogproto.MutationResponse{}, err
	}
	if err := c.requireGeneration(ctx, tenantID, generation); err != nil {
		return catalogproto.MutationResponse{}, err
	}
	var staged *catalog.ContentRef
	if request.HasContent {
		ref, err := c.source.StageContent(ctx, content)
		if err != nil {
			return catalogproto.MutationResponse{}, err
		}
		staged = &ref
	}
	objectID := func(value *catalogproto.ObjectID) (catalog.ObjectID, error) {
		if value == nil {
			return catalog.ObjectID{}, catalog.ErrInvalidObject
		}
		return catalog.ParseObjectID(string(*value))
	}
	intent := catalog.MutationIntent{SourceID: "mount:test", Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite}}
	switch request.Kind {
	case catalogproto.MutationKindCreate:
		parent, err := objectID(request.ParentID)
		if err != nil {
			return catalogproto.MutationResponse{}, err
		}
		kind := catalog.KindDirectory
		var ref catalog.ContentRef
		var contentRevision catalog.Revision
		if *request.ObjectKind == catalogproto.ObjectKindFile {
			kind = catalog.KindFile
			ref = *staged
			contentRevision = catalog.Revision(*request.ContentRevision)
		}
		intent.Create = &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: parent, Name: *request.Name, Kind: kind, Mode: *request.Mode,
			Content: ref, ContentRevision: contentRevision,
			Convergence: catalog.Convergence{Desired: contentRevision}, Visibility: catalog.Visibility{Mount: true},
		}}
	case catalogproto.MutationKindRevise:
		id, err := objectID(request.ObjectID)
		if err != nil {
			return catalogproto.MutationResponse{}, err
		}
		parent, err := objectID(request.ParentID)
		if err != nil {
			return catalogproto.MutationResponse{}, err
		}
		current, err := c.source.Inspect(ctx, tenantID, id)
		if err != nil {
			return catalogproto.MutationResponse{}, err
		}
		var update *catalog.ContentUpdate
		if request.HasContent {
			update = &catalog.ContentUpdate{Revision: catalog.Revision(*request.ContentRevision), Ref: *staged}
		}
		intent.Revise = &catalog.ReviseMutation{Object: id, Spec: catalog.RevisionSpec{
			Parent: parent, Name: *request.Name, Mode: *request.Mode, Content: update,
			Convergence: current.Convergence, Visibility: current.Visibility,
		}}
	case catalogproto.MutationKindDelete:
		id, err := objectID(request.ObjectID)
		if err != nil {
			return catalogproto.MutationResponse{}, err
		}
		intent.Delete = &catalog.DeleteMutation{Object: id}
	case catalogproto.MutationKindReplace:
		source, err := objectID(request.ObjectID)
		if err != nil {
			return catalogproto.MutationResponse{}, err
		}
		target, err := objectID(request.TargetID)
		if err != nil {
			return catalogproto.MutationResponse{}, err
		}
		var parent *catalog.ObjectID
		if request.ParentID != nil {
			value, err := objectID(request.ParentID)
			if err != nil {
				return catalogproto.MutationResponse{}, err
			}
			parent = &value
		}
		intent.Replace = &catalog.ReplaceMutation{Source: source, Target: target, Parent: parent, Name: request.Name, Mode: request.Mode}
	default:
		return catalogproto.MutationResponse{}, catalog.ErrInvalidObject
	}
	prepared, err := c.source.BeginMutation(ctx, tenantID, catalog.Revision(request.ExpectedRevision), intent)
	if err != nil {
		return catalogproto.MutationResponse{}, err
	}
	operationID := prepared.OperationID
	lease := applyingLease{source: c.source, tenant: tenantID, generation: generation, owner: c.resolver.owner}
	if _, err := lease.Prepare(ctx, catalog.Revision(request.ExpectedRevision+1)); err != nil {
		return catalogproto.MutationResponse{}, err
	}
	record, err := c.source.Mutation(ctx, tenantID, operationID)
	if err != nil {
		return catalogproto.MutationResponse{}, err
	}
	mutation := catalogproto.MutationID(operationID.String())
	requestID := request.RequestID
	primary := catalogproto.ObjectID(catalog.ObjectID(record.Primary).String())
	response := catalogproto.MutationResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		RequestID: &requestID, MutationID: &mutation,
		Revision: uint64(record.Revision), PrimaryID: &primary,
	}
	if record.Secondary != ([16]byte{}) {
		secondary := catalogproto.ObjectID(catalog.ObjectID(record.Secondary).String())
		response.SecondaryID = &secondary
	}
	return response, nil
}

type callbackWriteStage struct {
	catalog    *callbackCatalog
	tenant     catalog.TenantID
	generation catalog.Generation

	mu     sync.Mutex
	object catalog.Object
	body   []byte
	closed bool
}

func (s *callbackWriteStage) ReadAt(buffer []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, catalog.ErrHandleClosed
	}
	return bytes.NewReader(s.body).ReadAt(buffer, offset)
}

func (s *callbackWriteStage) WriteAt(buffer []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, catalog.ErrHandleClosed
	}
	if offset < 0 || int64(len(buffer)) > int64(1<<63-1)-offset {
		return 0, catalog.ErrInvalidObject
	}
	end := int(offset) + len(buffer)
	if end > len(s.body) {
		s.body = append(s.body, make([]byte, end-len(s.body))...)
	}
	return copy(s.body[int(offset):], buffer), nil
}

func (s *callbackWriteStage) Truncate(size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return catalog.ErrHandleClosed
	}
	if size < 0 || uint64(size) > uint64(^uint(0)>>1) {
		return catalog.ErrInvalidObject
	}
	if size < int64(len(s.body)) {
		s.body = s.body[:size]
	} else {
		s.body = append(s.body, make([]byte, int(size)-len(s.body))...)
	}
	return nil
}

func (s *callbackWriteStage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return catalog.ErrHandleClosed
	}
	return nil
}

func (s *callbackWriteStage) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.body))
}

func (s *callbackWriteStage) Commit(ctx context.Context) (catalog.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return catalog.Object{}, catalog.ErrHandleClosed
	}
	requestID, err := newMutationRequestID()
	if err != nil {
		return catalog.Object{}, err
	}
	objectID := catalogproto.ObjectID(s.object.ID.String())
	parentID := catalogproto.ObjectID(s.object.Parent.String())
	name := s.object.Name
	mode := s.object.Mode
	contentRevision := uint64(s.object.ContentRevision + 1)
	if _, err := s.catalog.Mutate(ctx, s.tenant, s.generation, catalogproto.MutationRequest{
		Protocol: catalogproto.Version, RequestID: requestID,
		Generation: uint64(s.generation), ExpectedRevision: uint64(s.object.Revision),
		Kind: catalogproto.MutationKindRevise, HasContent: true,
		ObjectID: &objectID, ParentID: &parentID, Name: &name, Mode: &mode,
		ContentRevision: &contentRevision,
	}, bytes.NewReader(s.body)); err != nil {
		return catalog.Object{}, err
	}
	updated, err := s.catalog.Lookup(ctx, s.tenant, s.generation, s.object.ID)
	if err != nil {
		return catalog.Object{}, err
	}
	s.object = updated
	return updated, nil
}

func (s *callbackWriteStage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.body = nil
	return nil
}

func newCallbackFixture(t *testing.T, name string) callbackFixture {
	t.Helper()
	backend, view, root := newTestFS(t, name)
	source := backend.store
	route := Route{Tenant: view.Tenant(), Generation: view.Generation(), Name: "acct"}
	spec := tenant.TenantSpec{
		OwnerID: "test", ID: view.Tenant(), PresentationRoot: "/mount/acct",
		Backing: tenant.BackingSpec{Root: "/backing/acct"}, Content: tenant.ContentSource{ID: "test-source"},
		Traits:     tenant.TenantTraits{Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: catalog.PresentMount},
		Generation: view.Generation(),
	}
	resolver := newApplyingResolver(t, source, route, spec)
	callbacks, err := NewFuseFS(&callbackCatalog{source: source, resolver: resolver}, resolver)
	if err != nil {
		t.Fatalf("NewFuseFS: %v", err)
	}
	return callbackFixture{source: source, view: view, root: root, resolver: resolver, callbacks: callbacks}
}

func (fixture callbackFixture) syncState(t *testing.T) {
	t.Helper()
	head := mustHead(t, fixture.view)
	state, err := fixture.source.LoadTenantState(context.Background(), fixture.view.Tenant())
	if err != nil {
		t.Fatalf("LoadTenantState: %v", err)
	}
	state.Desired, state.Observed, state.Verified, state.Applied = head, head, head, head
	if _, err := fixture.source.SaveTenantState(context.Background(), state.Version, state); err != nil {
		t.Fatalf("SaveTenantState: %v", err)
	}
}

type applyingResolver struct {
	t       *testing.T
	source  *catalog.Catalog
	runtime *Runtime
	owner   catalog.MutationOwnerID

	mu             sync.Mutex
	routes         map[string]Route
	specs          map[catalog.TenantID]tenant.TenantSpec
	releases       atomic.Int64
	routePageCalls atomic.Int64
	pinCalls       atomic.Int64
}

func newApplyingResolver(t *testing.T, source *catalog.Catalog, route Route, spec tenant.TenantSpec) *applyingResolver {
	t.Helper()
	owner, err := catalog.NewMutationOwnerID()
	if err != nil {
		t.Fatalf("NewMutationOwnerID: %v", err)
	}
	resolver := &applyingResolver{
		t: t, source: source, owner: owner, routes: make(map[string]Route),
		specs:   make(map[catalog.TenantID]tenant.TenantSpec),
		runtime: &Runtime{started: true, drained: make(chan struct{})},
	}
	resolver.add(route, spec)
	return resolver
}

func (resolver *applyingResolver) add(route Route, spec tenant.TenantSpec) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	resolver.routes[routeKey(route.Name)] = route
	resolver.specs[route.Tenant] = spec
}

func (resolver *applyingResolver) RoutePage(_ context.Context, cursor RouteCursor, limit int) (RoutePage, error) {
	resolver.routePageCalls.Add(1)
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	routes := make([]Route, 0, len(resolver.routes))
	for _, route := range resolver.routes {
		routes = append(routes, route)
	}
	slices.SortFunc(routes, func(left, right Route) int {
		return strings.Compare(left.Name, right.Name)
	})
	if cursor.Snapshot != 0 && cursor.Snapshot != 1 {
		return RoutePage{}, tenant.ErrGenerationConflict
	}
	start := 0
	if cursor.After != "" {
		for index, route := range routes {
			if route.Name == cursor.After {
				start = index + 1
				break
			}
		}
	}
	end := min(start+limit, len(routes))
	page := RoutePage{Snapshot: 1, Routes: slices.Clone(routes[start:end])}
	if end < len(routes) {
		page.Next = &RouteCursor{Snapshot: 1, After: routes[end-1].Name}
	}
	return page, nil
}

func (resolver *applyingResolver) Pin(_ context.Context, name string) (*PinnedRoute, error) {
	resolver.pinCalls.Add(1)
	resolver.mu.Lock()
	route, ok := resolver.routes[routeKey(name)]
	spec := resolver.specs[route.Tenant]
	resolver.mu.Unlock()
	if !ok {
		return nil, catalog.ErrNotFound
	}
	resolver.runtime.mu.Lock()
	resolver.runtime.active++
	resolver.runtime.mu.Unlock()
	return &PinnedRoute{Route: route, Spec: spec, release: func() error {
		resolver.releases.Add(1)
		resolver.runtime.releasePin()
		return nil
	}}, nil
}

type applyingLease struct {
	source     *catalog.Catalog
	tenant     catalog.TenantID
	generation catalog.Generation
	owner      catalog.MutationOwnerID
}

func (lease *applyingLease) Prepare(ctx context.Context, revision catalog.Revision) (tenant.TenantState, error) {
	pending, err := lease.source.PendingMutation(ctx, lease.tenant)
	if err != nil {
		return tenant.TenantState{}, err
	}
	if pending != nil {
		claimed, err := lease.source.ClaimMutation(ctx, pending.OperationID, lease.owner)
		if err != nil {
			return tenant.TenantState{}, err
		}
		if _, err := lease.source.MarkMutationApplied(ctx, pending.OperationID, *claimed.Claim); err != nil {
			return tenant.TenantState{}, err
		}
		if _, err := lease.source.CommitMutation(ctx, lease.tenant, pending.OperationID); err != nil {
			return tenant.TenantState{}, err
		}
	}
	state, err := lease.source.LoadTenantState(ctx, lease.tenant)
	if err != nil {
		return tenant.TenantState{}, err
	}
	state.Desired, state.Observed, state.Verified, state.Applied = revision, revision, revision, revision
	state.ActivatedGeneration = lease.generation
	state, err = lease.source.SaveTenantState(ctx, state.Version, state)
	if err != nil {
		return tenant.TenantState{}, err
	}
	return tenant.TenantState{
		Requested: revision, Tenant: lease.tenant, Generation: lease.generation,
		Desired: state.Desired, Observed: state.Observed, Verified: state.Verified,
		Applied: state.Applied, Activated: state.ActivatedGeneration, Version: state.Version,
	}, nil
}

func collectNames(names *[]string) func(string, *fuse.Stat_t, int64) bool {
	return func(name string, _ *fuse.Stat_t, _ int64) bool {
		*names = append(*names, name)
		return true
	}
}

func readCallbackFile(t *testing.T, callbacks *FuseFS, value string, handle uint64) string {
	t.Helper()
	var body bytes.Buffer
	buffer := make([]byte, 16)
	for offset := int64(0); ; {
		n := callbacks.Read(value, buffer, offset, handle)
		if n < 0 {
			t.Fatalf("Read(%s) = %d", value, n)
		}
		if n == 0 {
			break
		}
		_, _ = body.Write(buffer[:n])
		offset += int64(n)
	}
	return body.String()
}
