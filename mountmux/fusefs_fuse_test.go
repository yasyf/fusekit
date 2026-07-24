//go:build darwin && cgo && fuse

package mountmux

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"github.com/yasyf/fusekit/tenant"
)

func TestFuseFSRootAndPagedDirectoryEnumeration(t *testing.T) {
	fixture := newCallbackFixture(t, "paged")
	fixture.callbacks.Init()
	if epoch := fixture.callbacks.rootReadEpoch.Load(); epoch != 0 {
		t.Fatal("Init alone proved catalog-backed root readiness")
	}
	fixture.seedDirectory(t, directoryPage+7)

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

type callbackFixture struct {
	source    *catalog.Catalog
	view      *CatalogFS
	root      Entry
	backend   *callbackCatalog
	resolver  *applyingResolver
	callbacks *FuseFS
}

type callbackCatalog struct {
	source         *catalog.Catalog
	resolver       *applyingResolver
	root           catalog.Object
	snapshotParent catalog.ObjectID
	snapshot       []catalog.Object
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
	return c.root, nil
}

func (c *callbackCatalog) Head(ctx context.Context, tenantID catalog.TenantID, generation catalog.Generation) (catalog.Revision, error) {
	if err := c.requireGeneration(ctx, tenantID, generation); err != nil {
		return 0, err
	}
	return c.root.Revision, nil
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
	if c.snapshot != nil && parent == c.snapshotParent {
		start := 0
		if cursor.After != nil {
			start = len(c.snapshot)
			for index := range c.snapshot {
				if bytes.Compare(c.snapshot[index].ID[:], cursor.After[:]) > 0 {
					start = index
					break
				}
			}
		}
		end := min(start+limit, len(c.snapshot))
		page := catalog.SnapshotPage{
			Revision: revision,
			Objects:  append([]catalog.Object(nil), c.snapshot[start:end]...),
		}
		if end < len(c.snapshot) {
			after := c.snapshot[end-1].ID
			page.Next = &catalog.SnapshotCursor{After: &after}
		}
		return page, nil
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

func (*callbackCatalog) OpenWrite(context.Context, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision) (*NativeWriteStage, error) {
	return nil, catalog.ErrInvalidObject
}

func (*callbackCatalog) Mutate(context.Context, catalog.TenantID, catalog.Generation, catalogproto.MutationRequest, io.Reader) (catalogproto.MutationResponse, error) {
	return catalogproto.MutationResponse{}, catalog.ErrInvalidObject
}

func newCallbackFixture(t *testing.T, name string) callbackFixture {
	t.Helper()
	backend, view, root := newTestFS(t, name)
	source := backend.store
	route := Route{Tenant: view.Tenant(), Generation: view.Generation(), Name: "acct"}
	spec := tenant.TenantSpec{
		OwnerID: "test", ID: view.Tenant(), Mount: tenant.MountSpec{PresentationRoot: "/mount/acct"},
		Backing: tenant.BackingSpec{Root: "/backing/acct"}, Content: tenant.ContentSource{ID: "test-source"},
		Traits:     tenant.TenantTraits{Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: catalog.PresentMount},
		Generation: view.Generation(),
	}
	resolver := newApplyingResolver(t, source, route, spec)
	callbackBackend := &callbackCatalog{source: source, resolver: resolver, root: root.Object}
	callbacks, err := NewFuseFS(callbackBackend, resolver)
	if err != nil {
		t.Fatalf("NewFuseFS: %v", err)
	}
	return callbackFixture{source: source, view: view, root: root, backend: callbackBackend, resolver: resolver, callbacks: callbacks}
}

func (fixture callbackFixture) seedDirectory(t *testing.T, count int) {
	t.Helper()
	revision := mustHead(t, fixture.view)
	objects := make([]catalog.Object, count)
	for index := range objects {
		var id catalog.ObjectID
		binary.BigEndian.PutUint64(id[8:], uint64(index+1))
		objects[index] = catalog.Object{
			Tenant: fixture.view.Tenant(), ID: id, Parent: fixture.root.Object.ID,
			Revision: revision, MetadataRevision: revision, ContentRevision: 1,
			Name: fmt.Sprintf("file-%03d", index), Kind: catalog.KindFile, Mode: 0o600, Size: 1,
			Convergence: catalog.Convergence{Desired: 1}, Visibility: catalog.Visibility{Mount: true},
		}
	}
	fixture.backend.snapshotParent = fixture.root.Object.ID
	fixture.backend.snapshot = objects
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
