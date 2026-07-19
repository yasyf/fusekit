//go:build darwin && cgo && fuse

package mountmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

func TestFuseFSRootAndPagedDirectoryEnumeration(t *testing.T) {
	fixture := newCallbackFixture(t, "paged")
	for index := 0; index < directoryPage+7; index++ {
		createFile(t, fixture.view, fixture.root.Object.ID, fmt.Sprintf("file-%03d", index), "x", true)
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
	commitMutation(t, fixture.view, mustMutationID(t), head, catalog.MutationIntent{
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
	secondRoot, err := fixture.source.CreateTenant(context.Background(), mustMutationID(t), second, catalog.CaseSensitive, catalog.PresentMount)
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

func TestNativeMountStartsLowLevelRootOnceAndClosesBounded(t *testing.T) {
	fixture := newCallbackFixture(t, "native")
	original := nativeMount
	t.Cleanup(func() { nativeMount = original })
	handle := &fakeNativeHandle{done: make(chan struct{})}
	var calls atomic.Int64
	nativeMount = func(config fusekit.Config) (nativeHandle, error) {
		calls.Add(1)
		if config.Dir != "/mount" || config.FS == nil || config.ProbePath != "/mount" {
			t.Fatalf("mount config = %+v", config)
		}
		return handle, nil
	}
	native, err := NewNativeMount(NativeMountConfig{
		Catalog: fixture.source, ServeExitWait: time.Second,
	})
	if err != nil {
		t.Fatalf("NewNativeMount: %v", err)
	}
	if err := native.Start(context.Background(), "/mount", fixture.resolver); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := native.Start(context.Background(), "/mount", fixture.resolver); !errors.Is(err, ErrStarted) {
		t.Fatalf("Start(second) = %v, want ErrStarted", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("low-level mount calls = %d, want 1", calls.Load())
	}
	if native.Filesystem() == nil {
		t.Fatal("native callback filesystem is nil")
	}
	if err := native.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if handle.unmounts.Load() != 1 {
		t.Fatalf("unmount calls = %d, want 1", handle.unmounts.Load())
	}
}

type callbackFixture struct {
	source    *catalog.Catalog
	view      *CatalogFS
	root      Entry
	resolver  *applyingResolver
	callbacks *FuseFS
}

func newCallbackFixture(t *testing.T, name string) callbackFixture {
	t.Helper()
	source, view, root := newTestFS(t, name)
	route := Route{Tenant: view.Tenant(), Generation: view.Generation(), Name: "acct"}
	spec := tenant.TenantSpec{
		OwnerID: "test", ID: view.Tenant(), PresentationRoot: "/mount/acct",
		Backing: tenant.BackingSpec{Root: "/backing/acct"}, Content: tenant.ContentSource{ID: "test-source"},
		Traits:     tenant.TenantTraits{Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: catalog.PresentMount},
		Generation: view.Generation(),
	}
	resolver := newApplyingResolver(t, source, route, spec)
	callbacks, err := NewFuseFS(source, resolver)
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

	mu       sync.Mutex
	routes   map[string]Route
	specs    map[catalog.TenantID]tenant.TenantSpec
	releases atomic.Int64
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

func (resolver *applyingResolver) Routes() []Route {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	routes := make([]Route, 0, len(resolver.routes))
	for _, route := range resolver.routes {
		routes = append(routes, route)
	}
	return routes
}

func (resolver *applyingResolver) Pin(_ context.Context, name string) (*PinnedRoute, error) {
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
	lease := &applyingLease{source: resolver.source, tenant: route.Tenant, generation: route.Generation, owner: resolver.owner, releases: &resolver.releases}
	return &PinnedRoute{Route: route, Spec: spec, runtime: resolver.runtime, lease: lease}, nil
}

type applyingLease struct {
	source     *catalog.Catalog
	tenant     catalog.TenantID
	generation catalog.Generation
	owner      catalog.MutationOwnerID
	releases   *atomic.Int64
	once       sync.Once
}

func (lease *applyingLease) Prepare(ctx context.Context, revision catalog.Revision) (tenant.TenantState, error) {
	pending, err := lease.source.PendingMutations(ctx, lease.tenant)
	if err != nil {
		return tenant.TenantState{}, err
	}
	for _, mutation := range pending {
		claimed, err := lease.source.ClaimMutation(ctx, mutation.OperationID, lease.owner)
		if err != nil {
			return tenant.TenantState{}, err
		}
		if _, err := lease.source.MarkMutationApplied(ctx, mutation.OperationID, *claimed.Claim); err != nil {
			return tenant.TenantState{}, err
		}
		if _, err := lease.source.CommitMutation(ctx, mutation.OperationID); err != nil {
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

func (lease *applyingLease) Release() { lease.once.Do(func() { lease.releases.Add(1) }) }

type fakeNativeHandle struct {
	done     chan struct{}
	unmounts atomic.Int64
}

func (handle *fakeNativeHandle) Unmount() error {
	handle.unmounts.Add(1)
	close(handle.done)
	return nil
}

func (handle *fakeNativeHandle) Done() <-chan struct{} { return handle.done }

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

var _ GenerationPin = (*applyingLease)(nil)
