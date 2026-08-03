package mountservice

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/paths"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

func TestPersistentTenantLifecycleUsesAuthenticatedOwnerAndExactGeneration(t *testing.T) {
	ctx := mountCtx(t)
	runtime := &fakeRuntime{}
	authorizer := &recordingAuthorizer{owner: "trusted-owner"}
	d := startMountServer(t, runtime, authorizer)
	client := newMountClient(t, d)
	id, err := catalog.NewTenantID("acct-18")
	if err != nil {
		t.Fatalf("NewTenantID: %v", err)
	}
	definition := testDefinition(1)
	provisioned, err := client.ProvisionTenant(ctx, id, definition)
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	if provisioned.TenantID != "acct-18" || provisioned.Generation != 1 {
		t.Fatalf("ProvisionTenant response = %#v", provisioned)
	}
	snapshot := runtime.snapshot()
	if snapshot.spec.OwnerID != "trusted-owner" || snapshot.spec.ID != id || snapshot.spec.Generation != 1 {
		t.Fatalf("provisioned spec = %#v", snapshot.spec)
	}
	state, err := client.State(ctx, id)
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if state.State == nil || state.State.OwnerID != "trusted-owner" || state.State.Generation != 1 ||
		!state.State.ReplacementEligible || state.State.StateVersion == 0 {
		t.Fatalf("State response = %#v", state)
	}
	next := testDefinition(7)
	replaced, err := client.ReplaceTenant(ctx, id, 1, next)
	if err != nil {
		t.Fatalf("ReplaceTenant: %v", err)
	}
	snapshot = runtime.snapshot()
	if replaced.Generation != 7 || snapshot.spec.OwnerID != "trusted-owner" || snapshot.spec.Generation != 7 {
		t.Fatalf("ReplaceTenant response/spec = %#v / %#v", replaced, snapshot.spec)
	}
	state, err = client.State(ctx, id)
	if err != nil || state.State == nil || state.State.Generation != 7 {
		t.Fatalf("State after multi-generation replacement = %#v, %v", state, err)
	}
	removed, err := client.RemoveTenant(ctx, id, 7)
	if err != nil {
		t.Fatalf("RemoveTenant: %v", err)
	}
	snapshot = runtime.snapshot()
	if removed.Generation != 7 || !removed.FileProviderAbsent || snapshot.present {
		t.Fatalf("RemoveTenant response/present = %#v / %v", removed, snapshot.present)
	}
	if _, err := client.State(ctx, id); err == nil {
		t.Fatal("removed tenant State succeeded")
	} else {
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.Code != mountproto.ErrorCodeNotFound {
			t.Fatalf("removed tenant State error = %T %v", err, err)
		}
	}
	for _, identity := range authorizer.identities() {
		if identity.Session == (daemonkit.Session{}) || identity.Caller.PID <= 0 ||
			identity.Caller.UID != uint32(os.Getuid()) {
			t.Fatalf("authorizer identity = %#v", identity)
		}
	}
}

func TestTenantStateFailsClosedOnOwnerAndTenantMismatch(t *testing.T) {
	ctx := mountCtx(t)
	for _, test := range []struct {
		name           string
		overrideOwner  tenant.OwnerID
		overrideTenant catalog.TenantID
	}{
		{name: "owner", overrideOwner: "wrong-owner"},
		{name: "tenant", overrideTenant: "wrong-tenant"},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := &fakeRuntime{}
			d := startMountServer(t, runtime, &recordingAuthorizer{owner: "trusted-owner"})
			client := newMountClient(t, d)
			id := catalog.TenantID("acct-18")
			if _, err := client.ProvisionTenant(ctx, id, testDefinition(1)); err != nil {
				t.Fatal(err)
			}
			runtime.mu.Lock()
			runtime.stateOwnerOverride = test.overrideOwner
			runtime.stateTenantOverride = test.overrideTenant
			runtime.mu.Unlock()
			if _, err := client.State(ctx, id); err == nil {
				t.Fatal("mismatched State succeeded")
			} else {
				var remote *RemoteError
				if !errors.As(err, &remote) || remote.Code != mountproto.ErrorCodeUnavailable {
					t.Fatalf("State error = %T %v", err, err)
				}
			}
		})
	}
}

func TestTenantLifecycleAllowsPrivateOwnerWhileNativeRequiresProtectedPeer(t *testing.T) {
	ctx := mountCtx(t)
	runtime := &fakeRuntime{}
	authorizer := &recordingAuthorizer{owner: "trusted-owner"}
	native := newRecordingNativeSessions()
	var protectedCalls atomic.Int64
	d, _ := startMountServerWithNativeAdmissionAndProtectedPeer(
		t, runtime, native, authorizer,
		func(context.Context, daemonkit.Caller) error {
			protectedCalls.Add(1)
			return errors.New("designated requirement mismatch")
		},
	)
	client := newMountClient(t, d)
	if _, err := client.ProvisionTenant(ctx, "acct-18", testDefinition(1)); err != nil {
		t.Fatalf("private tenant owner provision: %v", err)
	}
	if protectedCalls.Load() != 0 {
		t.Fatalf("tenant lifecycle invoked protected verifier %d times", protectedCalls.Load())
	}
	if _, err := client.BindNative(ctx); err == nil {
		t.Fatal("native bind succeeded with a mismatched signed identity")
	}
	if protectedCalls.Load() != 1 {
		t.Fatalf("native bind protected verifier calls = %d, want one", protectedCalls.Load())
	}
	native.mu.Lock()
	defer native.mu.Unlock()
	if native.identity != nil {
		t.Fatal("rejected native peer reached tracked-session admission")
	}
}

func TestMismatchedProtocolCannotMutate(t *testing.T) {
	ctx := mountCtx(t)
	if mountproto.Version != 1 || transportproto.Version != 1 {
		t.Fatalf("current protocol versions = mount %d transport %d, want exact v1 suite",
			mountproto.Version, transportproto.Version)
	}
	runtime := &fakeRuntime{}
	authorizer := &recordingAuthorizer{owner: "trusted-owner"}
	d := startMountServer(t, runtime, authorizer)
	client, err := daemonkit.Open(d)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	business := client.Business()
	t.Cleanup(func() { _ = business.Close(ctx) })
	payload := []byte(`{"protocol":2,"tenant":"acct-18","definition":{"presentation_root":"/Volumes/FuseKit/acct-18","backing_root":"/Users/test/.cc-pool/accounts/acct-18","content_source_id":"source","access_mode":"read_write","case_policy":"sensitive","presentations":["mount"],"file_provider_presentation_instance_id":"","file_provider_display_name":"","generation":1}}`)
	reply, err := business.Call(ctx, string(mountproto.OperationTenantProvision), payload)
	if err != nil {
		t.Fatalf("malformed Call: %v", err)
	}
	var response mountproto.ProvisionTenantResponse
	if err := mountproto.Decode(reply.Body, &response); err != nil {
		t.Fatalf("Decode malformed response: %v", err)
	}
	if response.Code != mountproto.ErrorCodeInvalidRequest {
		t.Fatalf("malformed response = %#v", response)
	}
	if snapshot := runtime.snapshot(); snapshot.provisionCalls != 0 {
		t.Fatalf("provision calls = %d, want zero", snapshot.provisionCalls)
	}
	if identities := authorizer.identities(); len(identities) != 0 {
		t.Fatalf("rejected requests reached authorization: %d calls", len(identities))
	}
}

func TestNativeSessionIsSingletonAndReleasesEveryPinOnLoss(t *testing.T) {
	ctx := mountCtx(t)
	runtime := &fakeRuntime{}
	authorizer := &recordingAuthorizer{owner: "owner-native"}
	native := newRecordingNativeSessions()
	d := startMountServerWithNative(t, runtime, native, authorizer)
	first := newMountClient(t, d)
	binding, err := first.BindNative(ctx)
	if err != nil {
		t.Fatalf("BindNative(first): %v", err)
	}
	if err := first.NativeMounted(ctx, testNativeMountIdentity(), testNativeProbeToken()); err != nil {
		t.Fatalf("NativeMounted: %v", err)
	}
	if err := first.NativeReady(ctx, testNativeMountProof()); err != nil {
		t.Fatalf("NativeReady: %v", err)
	}
	routes, err := first.NativeRoutePage(ctx, 0, "", mountproto.MaxNativeRoutePageSize)
	if err != nil || routes.Snapshot != 1 || len(routes.Routes) != 1 || routes.Routes[0].Name != "acct" {
		t.Fatalf("NativeRoutePage = %+v, %v", routes, err)
	}
	pin, err := first.NativePin(ctx, "acct")
	if err != nil || pin.Token == "" || pin.OwnerID != "owner-native" || pin.Route == nil || pin.Definition == nil {
		t.Fatalf("NativePin = %+v, %v", pin, err)
	}

	second := newMountClient(t, d)
	if _, err := second.BindNative(ctx); err == nil {
		t.Fatal("second native session bound while first remained live")
	}
	if _, err := first.NativeRelease(ctx, pin.Token); err != nil {
		t.Fatalf("NativeRelease: %v", err)
	}
	if native.releases.Load() != 1 {
		t.Fatalf("explicit releases = %d, want 1", native.releases.Load())
	}
	if _, err := first.NativePin(ctx, "acct"); err != nil {
		t.Fatalf("NativePin(retained): %v", err)
	}
	if err := binding.Close(); err != nil {
		t.Fatalf("binding Close: %v", err)
	}
	native.waitUnbound(t)
	if native.releases.Load() != 2 {
		t.Fatalf("session-loss releases = %d, want 2", native.releases.Load())
	}
	rebound, err := second.BindNative(ctx)
	if err != nil {
		t.Fatalf("BindNative(after loss): %v", err)
	}
	_ = rebound.Close()
}

func TestNativeBindSettlesAdmissionWhileSessionRemainsBound(t *testing.T) {
	ctx := mountCtx(t)
	runtime := &fakeRuntime{}
	native := newRecordingNativeSessions()
	d, fixture := startMountServerWithNativeAdmission(t, runtime, native, &recordingAuthorizer{owner: "owner-native"})
	client := newMountClient(t, d)
	binding, err := client.BindNative(ctx)
	if err != nil {
		t.Fatalf("BindNative: %v", err)
	}
	fixture.requireResolved(t, 1)
	if err := client.NativeMounted(ctx, testNativeMountIdentity(), testNativeProbeToken()); err != nil {
		t.Fatalf("NativeMounted after bind admission settled: %v", err)
	}
	fixture.requireResolved(t, 2)
	if err := client.NativeReady(ctx, testNativeMountProof()); err != nil {
		t.Fatalf("NativeReady after bind admission settled: %v", err)
	}
	fixture.requireResolved(t, 3)
	if err := binding.Close(); err != nil {
		t.Fatalf("binding Close: %v", err)
	}
	native.waitUnbound(t)
}

type fakeRuntime struct {
	mu sync.Mutex

	present             bool
	spec                tenant.TenantSpec
	requested           catalog.Revision
	provisionCalls      int
	stateOwnerOverride  tenant.OwnerID
	stateTenantOverride catalog.TenantID
}

type fakeRuntimeSnapshot struct {
	present        bool
	spec           tenant.TenantSpec
	provisionCalls int
}

func (r *fakeRuntime) snapshot() fakeRuntimeSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fakeRuntimeSnapshot{
		present:        r.present,
		spec:           r.spec,
		provisionCalls: r.provisionCalls,
	}
}

func testNativeMountProof() NativeMountProof {
	source, err := mountproto.NativeMountSource("/Volumes/FuseKit")
	if err != nil {
		panic(err)
	}
	return NativeMountProof{
		PresentationRoot: "/Volumes/FuseKit",
		Filesystem:       mountproto.NativeMountFilesystem,
		Source:           source,
		RootReadEpoch:    7,
	}
}

func testNativeProbeToken() string { return strings.Repeat("a", 64) }

func testNativeMountIdentity() NativeMountIdentity {
	proof := testNativeMountProof()
	return NativeMountIdentity{
		PresentationRoot: proof.PresentationRoot,
		Filesystem:       proof.Filesystem,
		Source:           proof.Source,
	}
}

func (r *fakeRuntime) ProvisionTenant(_ context.Context, spec tenant.TenantSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.provisionCalls++
	if r.present {
		return tenant.ErrTenantConflict
	}
	r.present = true
	r.spec = spec
	return nil
}

func (r *fakeRuntime) ReplaceTenant(_ context.Context, expected catalog.Generation, spec tenant.TenantSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.present {
		return tenant.ErrTenantNotFound
	}
	if r.spec.Generation != expected {
		return tenant.ErrGenerationConflict
	}
	r.spec = spec
	r.requested = 0
	return nil
}

func (r *fakeRuntime) RemoveTenant(_ context.Context, id catalog.TenantID, generation catalog.Generation, owner tenant.OwnerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.present || r.spec.ID != id {
		return tenant.ErrTenantNotFound
	}
	if r.spec.OwnerID != owner {
		return tenant.ErrTenantOwnerMismatch
	}
	if r.spec.Generation != generation {
		return tenant.ErrGenerationConflict
	}
	r.present = false
	return nil
}

func (r *fakeRuntime) State(_ context.Context, id catalog.TenantID, owner tenant.OwnerID) (tenant.TenantStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.present || r.spec.ID != id {
		return tenant.TenantStatus{}, tenant.ErrTenantNotFound
	}
	if r.spec.OwnerID != owner {
		return tenant.TenantStatus{}, tenant.ErrTenantOwnerMismatch
	}
	status := tenant.TenantStatus{Owner: owner, State: r.stateLocked(), ReplacementEligible: true}
	if r.stateOwnerOverride != "" {
		status.Owner = r.stateOwnerOverride
	}
	if r.stateTenantOverride != "" {
		status.State.Tenant = r.stateTenantOverride
	}
	return status, nil
}

func (r *fakeRuntime) stateLocked() tenant.TenantState {
	revision := r.requested
	if revision == 0 {
		revision = 1
	}
	return tenant.TenantState{
		Requested: r.requested, Tenant: r.spec.ID, Generation: r.spec.Generation,
		Desired: revision, Observed: revision, Verified: revision, Applied: revision,
		Activated: r.spec.Generation, Version: 1,
	}
}

type recordingAuthorizer struct {
	mu    sync.Mutex
	owner tenant.OwnerID
	seen  []Identity
}

func (a *recordingAuthorizer) Authorize(_ context.Context, identity Identity, _ mountproto.Operation, _ catalog.TenantID, _ catalog.Generation) (tenant.OwnerID, error) {
	a.mu.Lock()
	a.seen = append(a.seen, identity)
	a.mu.Unlock()
	return a.owner, nil
}

func (a *recordingAuthorizer) AuthorizeNative(_ context.Context, identity Identity, _ mountproto.Operation) error {
	a.mu.Lock()
	a.seen = append(a.seen, identity)
	a.mu.Unlock()
	return nil
}

func (a *recordingAuthorizer) identities() []Identity {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Identity(nil), a.seen...)
}

func testDefinition(generation uint64) mountproto.TenantDefinition {
	return mountproto.TenantDefinition{
		Mount:                              &mountproto.MountSpec{PresentationRoot: "/Volumes/FuseKit/acct-18"},
		BackingRoot:                        "/Users/test/.cc-pool/accounts/acct-18",
		ContentSourceID:                    "acct-18-source",
		AccessMode:                         mountproto.AccessModeReadWrite,
		CasePolicy:                         mountproto.CasePolicySensitive,
		Presentations:                      []mountproto.Presentation{mountproto.PresentationMount, mountproto.PresentationFileProvider},
		FileProviderPresentationInstanceID: "acct-18-instance",
		FileProviderDisplayName:            "Account 18",
		Generation:                         generation,
	}
}

func startMountServer(t *testing.T, runtime Runtime, authorizer Authorizer) daemonkit.Daemon {
	d, _ := startMountServerWithConfig(t, runtime, authorizer, nil)
	return d
}

func startMountServerWithNative(t *testing.T, runtime Runtime, native NativeSessions, authorizer Authorizer) daemonkit.Daemon {
	d, _ := startMountServerWithNativeAdmission(t, runtime, native, authorizer)
	return d
}

func startMountServerWithNativeAdmission(t *testing.T, runtime Runtime, native NativeSessions, authorizer Authorizer) (daemonkit.Daemon, *mountTestFixture) {
	return startMountServerWithNativeAdmissionAndProtectedPeer(
		t, runtime, native, authorizer, func(context.Context, daemonkit.Caller) error { return nil },
	)
}

func startMountServerWithNativeAdmissionAndProtectedPeer(
	t *testing.T,
	runtime Runtime,
	native NativeSessions,
	authorizer Authorizer,
	protectedNativePeer func(context.Context, daemonkit.Caller) error,
) (daemonkit.Daemon, *mountTestFixture) {
	return startMountServerWithNativeCatalog(
		t, runtime, native, emptyNativeCatalog{}, authorizer, protectedNativePeer,
	)
}

func startMountServerWithNativeCatalog(
	t *testing.T,
	runtime Runtime,
	native NativeSessions,
	nativeCatalog NativeCatalog,
	authorizer Authorizer,
	protectedNativePeer func(context.Context, daemonkit.Caller) error,
) (daemonkit.Daemon, *mountTestFixture) {
	return startMountServerWithConfig(t, runtime, authorizer, &NativeConfig{
		Sessions: native, Catalog: nativeCatalog, ProtectedPeer: protectedNativePeer,
	})
}

func startMountServerWithConfig(
	t *testing.T,
	runtime Runtime,
	authorizer Authorizer,
	native *NativeConfig,
) (daemonkit.Daemon, *mountTestFixture) {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "fkm-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv(daemonkitHomeEnv, home)
	service, err := New(Config{Runtime: runtime, Authorizer: authorizer, Native: native})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fixture := &mountTestFixture{service: service}
	specs, err := Register(Routes{Native: native != nil}, fixture.resolve)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	deadlines := make(map[string]time.Duration, len(specs))
	for _, spec := range specs {
		deadlines[spec.Op] = 5 * time.Second
	}
	mux, err := transportproto.NewMux(deadlines, specs...)
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	d := daemonkit.Daemon{
		Label:     "fkmount",
		Schemas:   []daemonkit.Schema{daemonkit.Schema(transportproto.WireBuild)},
		Trust:     daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		Shutdown:  daemonkit.Grace(10 * time.Second),
		Handshake: daemonkit.Grace(10 * time.Second),
	}
	serveCtx, stopServing := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		_, err := daemonkit.Serve(serveCtx, d, func(daemonkit.Ctx) (daemonkit.Product, error) {
			return &mountTestProduct{mux: mux}, nil
		})
		served <- err
	}()
	t.Cleanup(func() {
		stopServing()
		select {
		case err := <-served:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Error("daemon did not return after drain")
		}
	})
	awaitMountDaemonReady(t, d)
	return d, fixture
}

// daemonkitHomeEnv relocates every daemonkit home-derived path, so the test
// daemon's socket, lock, and owner record land under a short /tmp home rather
// than the real one.
const daemonkitHomeEnv = "DAEMONKIT_HOME"

// awaitMountDaemonReady probes the business lane, not Control: Control pins the
// serving process and refuses to pin its own, and the test daemon shares this
// process. An operation the mux does not route proves the daemon is bound,
// admitting, and dispatching, and mutates nothing on the way.
func awaitMountDaemonReady(t *testing.T, d daemonkit.Daemon) {
	t.Helper()
	client, err := daemonkit.Open(d)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	business := client.Business()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = business.Close(ctx)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, err := business.Call(ctx, "mountservice.readiness.probe", []byte(`{}`))
		cancel()
		var product *daemonkit.ProductError
		if errors.As(err, &product) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon did not begin dispatching: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// mountTestProduct is the daemon half: the mux answers business, and the drain
// stages have nothing of their own to settle.
type mountTestProduct struct {
	mux *transportproto.Mux
}

func (p *mountTestProduct) Handle(ctx context.Context, request daemonkit.Request) (daemonkit.Reply, error) {
	return p.mux.Handle(ctx, request)
}

func (p *mountTestProduct) Drain(daemonkit.Budget) error { return nil }

func (p *mountTestProduct) Close(daemonkit.Budget) error { return nil }

type mountTestFixture struct {
	service  *Server
	calls    atomic.Int64
	mu       sync.Mutex
	sessions []daemonkit.Session
}

func (f *mountTestFixture) resolve(request daemonkit.Request) (*Server, error) {
	f.mu.Lock()
	f.sessions = append(f.sessions, request.Session)
	f.mu.Unlock()
	f.calls.Add(1)
	return f.service, nil
}

func (f *mountTestFixture) requireResolved(t *testing.T, want int64) {
	t.Helper()
	if got := f.calls.Load(); got != want {
		t.Fatalf("resolver calls = %d, want %d", got, want)
	}
}

type recordingNativeSessions struct {
	mu       sync.Mutex
	identity *Identity
	unbound  chan struct{}
	settled  chan error
	releases atomic.Int64

	pinStarted  chan struct{}
	pinContinue chan struct{}
}

func newRecordingNativeSessions() *recordingNativeSessions {
	return &recordingNativeSessions{unbound: make(chan struct{}, 1), settled: make(chan error, 1)}
}

func (s *recordingNativeSessions) Bind(_ context.Context, identity Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.identity != nil {
		return catalog.ErrConflict
	}
	s.identity = &identity
	return nil
}

func (s *recordingNativeSessions) Ready(_ context.Context, identity Identity, proof NativeMountProof) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.identity == nil || s.identity.Session != identity.Session {
		return ErrUnauthorized
	}
	if proof != testNativeMountProof() {
		return catalog.ErrIntegrity
	}
	return nil
}

func (s *recordingNativeSessions) Mounted(_ context.Context, identity Identity, mount NativeMountIdentity, probeToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.identity == nil || s.identity.Session != identity.Session {
		return ErrUnauthorized
	}
	proof := testNativeMountProof()
	if mount != (NativeMountIdentity{
		PresentationRoot: proof.PresentationRoot,
		Filesystem:       proof.Filesystem,
		Source:           proof.Source,
	}) {
		return catalog.ErrIntegrity
	}
	if probeToken != testNativeProbeToken() {
		return catalog.ErrIntegrity
	}
	return nil
}

func (s *recordingNativeSessions) Unbind(identity Identity) {
	s.mu.Lock()
	if s.identity != nil && s.identity.Session == identity.Session {
		s.identity = nil
		select {
		case s.unbound <- struct{}{}:
		default:
		}
	}
	s.mu.Unlock()
}

func (s *recordingNativeSessions) Settled(_ Identity, result error) {
	select {
	case s.settled <- result:
	default:
	}
}

func (*recordingNativeSessions) RoutePage(context.Context, uint64, string, int) (NativeRoutePage, error) {
	return NativeRoutePage{
		Snapshot: 1,
		Routes:   []NativeRoute{{Name: "acct", Tenant: "tenant-native", Generation: 1}},
	}, nil
}

func (s *recordingNativeSessions) Pin(_ context.Context, name string) (NativePin, error) {
	if name != "acct" {
		return NativePin{}, catalog.ErrNotFound
	}
	if s.pinStarted != nil {
		close(s.pinStarted)
		<-s.pinContinue
	}
	return NativePin{
		Route: NativeRoute{Name: name, Tenant: "tenant-native", Generation: 1},
		Spec: tenant.TenantSpec{
			OwnerID: "owner-native", ID: "tenant-native", Mount: tenant.MountSpec{PresentationRoot: "/Volumes/FuseKit/acct"},
			Backing: tenant.BackingSpec{Root: "/Users/test/.cc-pool/accounts/acct"},
			Content: tenant.ContentSource{ID: "source-native"},
			Traits: tenant.TenantTraits{
				Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: catalog.PresentMount,
			},
			Generation: 1,
		},
		Release: func() error { s.releases.Add(1); return nil },
	}, nil
}

func (s *recordingNativeSessions) waitUnbound(t *testing.T) {
	t.Helper()
	select {
	case <-s.unbound:
	case <-time.After(5 * time.Second):
		t.Fatal("native session did not unbind")
	}
}

type emptyNativeSessions struct{}

func (emptyNativeSessions) Bind(context.Context, Identity) error { return nil }
func (emptyNativeSessions) Mounted(context.Context, Identity, NativeMountIdentity, string) error {
	return nil
}

func (emptyNativeSessions) Ready(context.Context, Identity, NativeMountProof) error {
	return nil
}
func (emptyNativeSessions) Unbind(Identity)         {}
func (emptyNativeSessions) Settled(Identity, error) {}

func (emptyNativeSessions) RoutePage(context.Context, uint64, string, int) (NativeRoutePage, error) {
	return NativeRoutePage{Snapshot: 1}, nil
}

func (emptyNativeSessions) Pin(context.Context, string) (NativePin, error) {
	return NativePin{}, catalog.ErrNotFound
}

func newMountClient(t *testing.T, d daemonkit.Daemon) *Client {
	t.Helper()
	client, err := NewClient(d)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// newMountClientOverConn hands the test the session's own transport, so a test
// that needs an abrupt loss can drop it without the graceful drain Close runs.
func newMountClientOverConn(t *testing.T, ctx context.Context, d daemonkit.Daemon) (net.Conn, *Client) {
	t.Helper()
	socket, err := paths.Socket(string(d.Label))
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	business, err := daemonkit.BusinessOverConn(ctx, conn, daemonkit.Contract{Schema: d.Schemas[0]})
	if err != nil {
		t.Fatalf("BusinessOverConn: %v", err)
	}
	client, err := NewClientOn(business)
	if err != nil {
		t.Fatalf("NewClientOn: %v", err)
	}
	return conn, client
}

// mountCtx bounds every client call in one test. daemonkit refuses a Call,
// Close, or Stop with no context deadline: the caller owns the budget.
func mountCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}
