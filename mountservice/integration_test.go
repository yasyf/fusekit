package mountservice

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
	"github.com/yasyf/fusekit/trustroles"
)

func TestPersistentTenantLifecycleUsesAuthenticatedOwnerAndExactGeneration(t *testing.T) {
	runtime := &fakeRuntime{}
	authorizer := &recordingAuthorizer{owner: "trusted-owner"}
	path := startMountServer(t, runtime, authorizer)
	client := newMountClient(t, path)
	id, err := catalog.NewTenantID("acct-18")
	if err != nil {
		t.Fatalf("NewTenantID: %v", err)
	}
	definition := testDefinition(1)
	provisioned, err := client.ProvisionTenant(context.Background(), id, definition)
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	if provisioned.TenantID != "acct-18" || provisioned.Generation != 1 {
		t.Fatalf("ProvisionTenant response = %#v", provisioned)
	}
	if runtime.spec.OwnerID != "trusted-owner" || runtime.spec.ID != id || runtime.spec.Generation != 1 {
		t.Fatalf("provisioned spec = %#v", runtime.spec)
	}
	state, err := client.State(context.Background(), id)
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if state.State == nil || state.State.OwnerID != "trusted-owner" || state.State.Generation != 1 ||
		!state.State.ReplacementEligible || state.State.StateVersion == 0 {
		t.Fatalf("State response = %#v", state)
	}
	next := testDefinition(7)
	replaced, err := client.ReplaceTenant(context.Background(), id, 1, next)
	if err != nil {
		t.Fatalf("ReplaceTenant: %v", err)
	}
	if replaced.Generation != 7 || runtime.spec.OwnerID != "trusted-owner" || runtime.spec.Generation != 7 {
		t.Fatalf("ReplaceTenant response/spec = %#v / %#v", replaced, runtime.spec)
	}
	state, err = client.State(context.Background(), id)
	if err != nil || state.State == nil || state.State.Generation != 7 {
		t.Fatalf("State after multi-generation replacement = %#v, %v", state, err)
	}
	removed, err := client.RemoveTenant(context.Background(), id, 7)
	if err != nil {
		t.Fatalf("RemoveTenant: %v", err)
	}
	if removed.Generation != 7 || !removed.FileProviderAbsent || runtime.present {
		t.Fatalf("RemoveTenant response/present = %#v / %v", removed, runtime.present)
	}
	if _, err := client.State(context.Background(), id); err == nil {
		t.Fatal("removed tenant State succeeded")
	} else {
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.Code != mountproto.ErrorCodeNotFound {
			t.Fatalf("removed tenant State error = %T %v", err, err)
		}
	}
	for _, identity := range authorizer.identities() {
		if identity.WireBuild != transportproto.WireBuild || identity.Session == nil || identity.Peer.PID <= 0 || identity.Peer.UID != os.Getuid() {
			t.Fatalf("authorizer identity = %#v", identity)
		}
	}
}

func TestRuntimeHealthReportsExactActivationAndNativeThroughProof(t *testing.T) {
	authorizer := &recordingAuthorizer{owner: "trusted-owner"}
	route, err := RuntimeHealthObservation(staticRuntimeHealth{}, authorizer)
	if err != nil {
		t.Fatalf("RuntimeHealthObservation: %v", err)
	}
	if route.Op != wire.Op(mountproto.OperationRuntimeHealth) ||
		route.MaxResponseBytes != mountproto.RuntimeHealthMaxResponseBytes {
		t.Fatalf("RuntimeHealth observation route = %#v", route)
	}
	request, err := mountproto.Encode(mountproto.RuntimeHealthRequest{Protocol: mountproto.Version})
	if err != nil {
		t.Fatalf("encode RuntimeHealth: %v", err)
	}
	response, err := route.Handler(t.Context(), wire.ObservationRequest{
		Op: route.Op, WireBuild: transportproto.WireBuild,
		Peer: wire.Peer{PID: os.Getpid(), UID: os.Geteuid()}, Payload: request,
	})
	if err != nil {
		t.Fatalf("RuntimeHealth: %v", err)
	}
	var health mountproto.RuntimeHealthResponse
	if err := mountproto.Decode(response.Payload, &health); err != nil {
		t.Fatalf("decode RuntimeHealth: %v", err)
	}
	proof := testNativeMountProof()
	if health.RuntimeBuild != "product-1.8.0" || health.RuntimeProtocol != mountproto.RuntimeProtocolVersion ||
		health.RuntimePID != 4242 || health.ProcessGeneration != "process-7" ||
		health.ActivationGeneration != "activation-7" || health.State != mountproto.RuntimeStateHealthy ||
		health.Draining || health.Busy || !health.Ready ||
		health.ReadinessPhase != mountproto.ReadinessPhaseReady || health.ReadinessStep != mountproto.ReadinessStepPublished ||
		health.NativePhase != mountproto.NativePhaseLive || health.BrokerPhase != mountproto.BrokerPhaseLive ||
		health.NativeMount == nil || *health.NativeMount != protocolNativeMountProof(proof) {
		t.Fatalf("RuntimeHealth = %#v", health)
	}
	observations := authorizer.observationIdentities()
	if len(observations) != 1 || observations[0].WireBuild != transportproto.WireBuild || observations[0].Peer.PID <= 0 {
		t.Fatalf("runtime health observation identities = %#v", observations)
	}
}

func TestTenantStateFailsClosedOnOwnerAndTenantMismatch(t *testing.T) {
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
			path := startMountServer(t, runtime, &recordingAuthorizer{owner: "trusted-owner"})
			client := newMountClient(t, path)
			id := catalog.TenantID("acct-18")
			if _, err := client.ProvisionTenant(t.Context(), id, testDefinition(1)); err != nil {
				t.Fatal(err)
			}
			runtime.mu.Lock()
			runtime.stateOwnerOverride = test.overrideOwner
			runtime.stateTenantOverride = test.overrideTenant
			runtime.mu.Unlock()
			if _, err := client.State(t.Context(), id); err == nil {
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
	runtime := &fakeRuntime{}
	authorizer := &recordingAuthorizer{owner: "trusted-owner"}
	native := newRecordingNativeSessions()
	var protectedCalls atomic.Int64
	path, _ := startMountServerWithNativeAdmissionAndProtectedPeer(
		t, runtime, native, authorizer,
		func(context.Context, wire.Peer) error {
			protectedCalls.Add(1)
			return errors.New("designated requirement mismatch")
		},
	)
	client := newMountClient(t, path)
	if _, err := client.ProvisionTenant(t.Context(), "acct-18", testDefinition(1)); err != nil {
		t.Fatalf("private tenant owner provision: %v", err)
	}
	if protectedCalls.Load() != 0 {
		t.Fatalf("tenant lifecycle invoked protected verifier %d times", protectedCalls.Load())
	}
	if _, err := client.BindNative(t.Context()); err == nil {
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

func TestMismatchedProtocolAndBuildCannotMutate(t *testing.T) {
	if mountproto.Version != 1 || transportproto.Version != 1 {
		t.Fatalf("current protocol versions = mount %d transport %d, want exact v1 suite",
			mountproto.Version, transportproto.Version)
	}
	runtime := &fakeRuntime{}
	authorizer := &recordingAuthorizer{owner: "trusted-owner"}
	path := startMountServer(t, runtime, authorizer)
	rawClient, err := wire.NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), WireBuild: transportproto.WireBuild, Role: trust.UnprotectedRole,
	})
	if err != nil {
		t.Fatalf("wire.NewClient: %v", err)
	}
	defer func() {
		if err := rawClient.Close(); err != nil {
			t.Errorf("Close raw client: %v", err)
		}
	}()
	payload := []byte(`{"protocol":2,"definition":{"presentation_root":"/Volumes/FuseKit/acct-18","backing_root":"/Users/test/.cc-pool/accounts/acct-18","content_source_id":"source","access_mode":"read_write","case_policy":"sensitive","presentations":["mount"],"file_provider_presentation_instance_id":"","file_provider_display_name":"","generation":1}}`)
	result, err := rawClient.Call(context.Background(), wire.Op(mountproto.OperationTenantProvision), "acct-18", payload)
	if err != nil {
		t.Fatalf("malformed Call: %v", err)
	}
	var response mountproto.ProvisionTenantResponse
	if err := mountproto.Decode(result.Response.Payload, &response); err != nil {
		t.Fatalf("Decode malformed response: %v", err)
	}
	if response.Code != mountproto.ErrorCodeInvalidRequest {
		t.Fatalf("malformed response = %#v", response)
	}

	oldClient, err := wire.NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), WireBuild: transportproto.WireBuildFor(
			transportproto.Version-1,
			transportproto.CatalogSchemaFingerprint,
			transportproto.CatalogWorkerSchemaFingerprint,
			transportproto.MountSchemaFingerprint,
			transportproto.SourceDriverSchemaFingerprint,
		),
		HandshakeTimeout: 5 * time.Second, Role: trust.UnprotectedRole,
	})
	if err == nil {
		_ = oldClient.Close()
		t.Fatal("old build transport handshake succeeded")
	}
	if !errors.Is(err, wire.ErrBuildMismatch) {
		t.Fatalf("old build transport handshake = %v, want %v", err, wire.ErrBuildMismatch)
	}

	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("Dial old LF client: %v", err)
	}
	_ = connection.SetDeadline(time.Now().Add(time.Second))
	if _, err := connection.Write([]byte("{\"op\":\"tenant.register\"}\n")); err != nil {
		t.Fatalf("write old LF request: %v", err)
	}
	buffer := make([]byte, 1)
	if _, err := connection.Read(buffer); err == nil {
		t.Fatal("old LF client received a protocol response")
	}
	_ = connection.Close()
	if runtime.provisionCalls != 0 {
		t.Fatalf("provision calls = %d, want zero", runtime.provisionCalls)
	}
	if identities := authorizer.identities(); len(identities) != 0 {
		t.Fatalf("rejected requests reached authorization: %d calls", len(identities))
	}
}

func TestNativeSessionIsSingletonAndReleasesEveryPinOnLoss(t *testing.T) {
	runtime := &fakeRuntime{}
	authorizer := &recordingAuthorizer{owner: "owner-native"}
	native := newRecordingNativeSessions()
	path := startMountServerWithNative(t, runtime, native, authorizer)
	first := newMountClient(t, path)
	binding, err := first.BindNative(context.Background())
	if err != nil {
		t.Fatalf("BindNative(first): %v", err)
	}
	if err := first.NativeMounted(context.Background(), testNativeMountIdentity(), testNativeProbeToken()); err != nil {
		t.Fatalf("NativeMounted: %v", err)
	}
	if err := first.NativeReady(context.Background(), testNativeMountProof()); err != nil {
		t.Fatalf("NativeReady: %v", err)
	}
	routes, err := first.NativeRoutePage(context.Background(), 0, "", mountproto.MaxNativeRoutePageSize)
	if err != nil || routes.Snapshot != 1 || len(routes.Routes) != 1 || routes.Routes[0].Name != "acct" {
		t.Fatalf("NativeRoutePage = %+v, %v", routes, err)
	}
	pin, err := first.NativePin(context.Background(), "acct")
	if err != nil || pin.Token == "" || pin.OwnerID != "owner-native" || pin.Route == nil || pin.Definition == nil {
		t.Fatalf("NativePin = %+v, %v", pin, err)
	}

	second := newMountClient(t, path)
	if _, err := second.BindNative(context.Background()); err == nil {
		t.Fatal("second native session bound while first remained live")
	}
	if _, err := first.NativeRelease(context.Background(), pin.Token); err != nil {
		t.Fatalf("NativeRelease: %v", err)
	}
	if native.releases.Load() != 1 {
		t.Fatalf("explicit releases = %d, want 1", native.releases.Load())
	}
	if _, err := first.NativePin(context.Background(), "acct"); err != nil {
		t.Fatalf("NativePin(retained): %v", err)
	}
	if err := binding.Close(); err != nil {
		t.Fatalf("binding Close: %v", err)
	}
	native.waitUnbound(t)
	if native.releases.Load() != 2 {
		t.Fatalf("session-loss releases = %d, want 2", native.releases.Load())
	}
	rebound, err := second.BindNative(context.Background())
	if err != nil {
		t.Fatalf("BindNative(after loss): %v", err)
	}
	_ = rebound.Close()
}

func TestNativeBindSettlesAdmissionWhileSessionRemainsBound(t *testing.T) {
	runtime := &fakeRuntime{}
	native := newRecordingNativeSessions()
	path, fixture := startMountServerWithNativeAdmission(t, runtime, native, &recordingAuthorizer{owner: "owner-native"})
	client := newMountClient(t, path)
	binding, err := client.BindNative(t.Context())
	if err != nil {
		t.Fatalf("BindNative: %v", err)
	}
	fixture.requireSettled(t, 1)
	if err := client.NativeMounted(t.Context(), testNativeMountIdentity(), testNativeProbeToken()); err != nil {
		t.Fatalf("NativeMounted after bind admission settled: %v", err)
	}
	fixture.requireSettled(t, 2)
	if err := client.NativeReady(t.Context(), testNativeMountProof()); err != nil {
		t.Fatalf("NativeReady after bind admission settled: %v", err)
	}
	fixture.requireSettled(t, 3)
	if err := binding.Close(); err != nil {
		t.Fatalf("binding Close: %v", err)
	}
	native.waitUnbound(t)
}

func TestMountResolverRejectsUnpublishedRequest(t *testing.T) {
	_, fixture := startMountServerWithConfig(
		t, &fakeRuntime{}, &recordingAuthorizer{owner: "trusted-owner"}, nil,
	)
	if _, err := resolvePinnedMountService(fixture.slot, wire.Request{}); !errors.Is(err, daemon.ErrPublicationStale) {
		t.Fatalf("resolve unpublished request = %v, want %v", err, daemon.ErrPublicationStale)
	}
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

type staticRuntimeHealth struct{}

func (staticRuntimeHealth) Health(context.Context) (RuntimeHealth, error) {
	proof := testNativeMountProof()
	return RuntimeHealth{
		RuntimeBuild:         "product-1.8.0",
		RuntimeProtocol:      mountproto.RuntimeProtocolVersion,
		RuntimePID:           4242,
		ProcessGeneration:    "process-7",
		ActivationGeneration: "activation-7",
		State:                mountproto.RuntimeStateHealthy,
		Ready:                true,
		ReadinessPhase:       mountproto.ReadinessPhaseReady,
		ReadinessStep:        mountproto.ReadinessStepPublished,
		NativePhase:          mountproto.NativePhaseLive,
		NativeMount:          &proof,
		BrokerPhase:          mountproto.BrokerPhaseLive,
	}, nil
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
	mu           sync.Mutex
	owner        tenant.OwnerID
	seen         []Identity
	observations []ObservationIdentity
}

func (a *recordingAuthorizer) Authorize(_ context.Context, identity Identity, _ mountproto.Operation, _ catalog.TenantID, _ catalog.Generation) (tenant.OwnerID, error) {
	a.mu.Lock()
	a.seen = append(a.seen, identity)
	a.mu.Unlock()
	return a.owner, nil
}

func (a *recordingAuthorizer) AuthorizeObservation(_ context.Context, identity ObservationIdentity, _ mountproto.Operation) error {
	a.mu.Lock()
	a.observations = append(a.observations, identity)
	a.mu.Unlock()
	return nil
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

func (a *recordingAuthorizer) observationIdentities() []ObservationIdentity {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]ObservationIdentity(nil), a.observations...)
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

func startMountServer(t *testing.T, runtime Runtime, authorizer Authorizer) string {
	path, _ := startMountServerWithConfig(t, runtime, authorizer, nil)
	return path
}

func startMountServerWithNative(t *testing.T, runtime Runtime, native NativeSessions, authorizer Authorizer) string {
	path, _ := startMountServerWithNativeAdmission(t, runtime, native, authorizer)
	return path
}

func startMountServerWithNativeAdmission(t *testing.T, runtime Runtime, native NativeSessions, authorizer Authorizer) (string, *mountTestFixture) {
	return startMountServerWithNativeAdmissionAndProtectedPeer(
		t, runtime, native, authorizer, func(context.Context, wire.Peer) error { return nil },
	)
}

func startMountServerWithNativeAdmissionAndProtectedPeer(
	t *testing.T,
	runtime Runtime,
	native NativeSessions,
	authorizer Authorizer,
	protectedNativePeer func(context.Context, wire.Peer) error,
) (string, *mountTestFixture) {
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
	protectedNativePeer func(context.Context, wire.Peer) error,
) (string, *mountTestFixture) {
	return startMountServerWithConfig(t, runtime, authorizer, &NativeConfig{
		Sessions: native, Catalog: nativeCatalog, ProtectedPeer: protectedNativePeer,
	})
}

func startMountServerWithConfig(
	t *testing.T,
	runtime Runtime,
	authorizer Authorizer,
	native *NativeConfig,
) (string, *mountTestFixture) {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "fusekit-mount-service-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "socket")
	server := &wire.Server{
		WireBuild:               transportproto.WireBuild,
		HandshakeTimeout:        100 * time.Millisecond,
		PeerVerificationTimeout: 5 * time.Second,
	}
	service, err := New(Config{Runtime: runtime, Authorizer: authorizer, Native: native})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runtimeHost := newMountTestRuntime(t, path, server)
	fixture := &mountTestFixture{slot: daemon.NewPublicationSlot[*Server](runtimeHost)}
	if err := Register(server, Routes{Native: native != nil}, fixture.resolve); err != nil {
		t.Fatalf("Register: %v", err)
	}
	activation, err := runtimeHost.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin runtime: %v", err)
	}
	publication, err := fixture.slot.Stage(activation, service)
	if err != nil {
		t.Fatalf("Stage runtime: %v", err)
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatalf("CommitReady: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtimeHost.Close(ctx); err != nil {
			t.Errorf("Close runtime: %v", err)
		}
	})
	return path, fixture
}

type mountTestFixture struct {
	slot     *daemon.PublicationSlot[*Server]
	calls    atomic.Int64
	mu       sync.Mutex
	requests []wire.Request
}

func (f *mountTestFixture) resolve(request wire.Request) (*Server, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.mu.Unlock()
	f.calls.Add(1)
	return resolvePinnedMountService(f.slot, request)
}

func (f *mountTestFixture) requireSettled(t *testing.T, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := f.calls.Load()
		if got > want {
			t.Fatalf("resolver calls = %d, want %d", got, want)
		}
		if got == want {
			f.mu.Lock()
			request := f.requests[want-1]
			f.mu.Unlock()
			if _, ok := f.slot.LoadPinned(request.Publication); !ok {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("request %d publication remained live after settlement", want)
		}
		time.Sleep(time.Millisecond)
	}
}

func resolvePinnedMountService(slot *daemon.PublicationSlot[*Server], request wire.Request) (*Server, error) {
	service, ok := slot.LoadPinned(request.Publication)
	if !ok {
		return nil, daemon.ErrPublicationStale
	}
	return service, nil
}

func newMountTestRuntime(t *testing.T, socket string, server *wire.Server) *daemon.Runtime {
	t.Helper()
	directory := filepath.Dir(socket)
	generation, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatalf("ProcessGeneration: %v", err)
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 4, QueueCapacity: 4, MaxTotalRun: 5 * time.Second,
		MaxStdinBytes: 64 << 10, MaxStdoutBytes: 64 << 10, MaxStderrBytes: 64 << 10,
	}, &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(directory, "workers.db")},
		Generation: generation, Grace: 10 * time.Millisecond, Settlement: time.Second,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	children, err := proc.NewManager(4, &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(directory, "children.db")},
		Generation: generation, Grace: 10 * time.Millisecond, Settlement: time.Second,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(),
		Roles: map[trust.PeerRole]trust.Requirement{
			trustroles.StopController:      {TeamID: "DAEMONKITTEST", SigningIdentifier: "com.yasyf.fusekit.mountservice.stop"},
			trustroles.ReceiptController:   {TeamID: "DAEMONKITTEST", SigningIdentifier: "com.yasyf.fusekit.mountservice.receipt"},
			trustroles.ReadinessController: {TeamID: "DAEMONKITTEST", SigningIdentifier: "com.yasyf.fusekit.mountservice.readiness"},
		},
		StopRoles:      []trust.PeerRole{trustroles.StopController},
		ReceiptRoles:   []trust.PeerRole{trustroles.ReceiptController},
		ReadinessRoles: []trust.PeerRole{trustroles.ReadinessController},
	})
	if err != nil {
		t.Fatalf("NewTrustPolicy: %v", err)
	}
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: socket, RuntimeBuild: "mountservice-test-v1", RuntimeProtocol: 1,
		Wire: server, TrustPolicy: policy,
		StopControlStore: &proc.FileStore{Path: filepath.Join(directory, "stop.db")},
		Workers:          workers, Children: children, ShutdownTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	return runtime
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

func newMountClient(t *testing.T, path string) *Client {
	t.Helper()
	client, err := NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), Role: trust.UnprotectedRole,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
