package mountservice

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
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
		if identity.Build != transportproto.Build || identity.Session == nil || identity.Peer.PID <= 0 || identity.Peer.UID != os.Getuid() {
			t.Fatalf("authorizer identity = %#v", identity)
		}
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
		func(wire.Peer) error {
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

func TestMalformedOwnerOldLFAndBuildMismatchCannotMutate(t *testing.T) {
	runtime := &fakeRuntime{}
	authorizer := &recordingAuthorizer{owner: "trusted-owner"}
	path := startMountServer(t, runtime, authorizer)
	rawClient, err := wire.NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), Build: transportproto.Build,
	})
	if err != nil {
		t.Fatalf("wire.NewClient: %v", err)
	}
	defer func() {
		if err := rawClient.Close(); err != nil {
			t.Errorf("Close raw client: %v", err)
		}
	}()
	payload := []byte(`{"protocol":2,"owner_id":"spoofed","definition":{"presentation_root":"/Volumes/FuseKit/acct-18","backing_root":"/Users/test/.cc-pool/accounts/acct-18","content_source_id":"source","access_mode":"read_write","case_policy":"sensitive","presentations":["mount"],"generation":1}}`)
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
		Dial: wire.UnixDialer(path), Build: transportproto.BuildFor(
			transportproto.Version,
			transportproto.CatalogSchemaFingerprint+"-drift",
			transportproto.MountSchemaFingerprint,
		),
		HandshakeTimeout: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("old build transport handshake: %v", err)
	}
	oldPayload, err := mountproto.Encode(mountproto.ProvisionTenantRequest{
		Protocol: mountproto.Version, Definition: testDefinition(1),
	})
	if err != nil {
		t.Fatalf("Encode old build request: %v", err)
	}
	oldResult, err := oldClient.Call(context.Background(), wire.Op(mountproto.OperationTenantProvision), "acct-18", oldPayload)
	if err != nil {
		t.Fatalf("old build Call: %v", err)
	}
	if oldResult.Outcome != wire.Rejected || !oldResult.Response.Rejected {
		t.Fatalf("old build result = %#v", oldResult)
	}
	_ = oldClient.Close()

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
	if err := first.NativeReady(context.Background()); err != nil {
		t.Fatalf("NativeReady: %v", err)
	}
	routes, err := first.NativeRoutes(context.Background())
	if err != nil || len(routes.Routes) != 1 || routes.Routes[0].Name != "acct" {
		t.Fatalf("NativeRoutes = %+v, %v", routes, err)
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
	path, inflight := startMountServerWithNativeAdmission(t, runtime, native, &recordingAuthorizer{owner: "owner-native"})
	client := newMountClient(t, path)
	binding, err := client.BindNative(t.Context())
	if err != nil {
		t.Fatalf("BindNative: %v", err)
	}
	waitAtomicZero(t, inflight)
	if err := client.NativeReady(t.Context()); err != nil {
		t.Fatalf("NativeReady after bind admission settled: %v", err)
	}
	waitAtomicZero(t, inflight)
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
		PresentationRoot:        "/Volumes/FuseKit/acct-18",
		BackingRoot:             "/Users/test/.cc-pool/accounts/acct-18",
		ContentSourceID:         "acct-18-source",
		AccessMode:              mountproto.AccessModeReadWrite,
		CasePolicy:              mountproto.CasePolicySensitive,
		Presentations:           []mountproto.Presentation{mountproto.PresentationMount, mountproto.PresentationFileProvider},
		FileProviderAccountID:   "acct-18-instance",
		FileProviderDisplayName: "Account 18",
		Generation:              generation,
	}
}

func startMountServer(t *testing.T, runtime Runtime, authorizer Authorizer) string {
	return startMountServerWithNative(t, runtime, emptyNativeSessions{}, authorizer)
}

func startMountServerWithNative(t *testing.T, runtime Runtime, native NativeSessions, authorizer Authorizer) string {
	path, _ := startMountServerWithNativeAdmission(t, runtime, native, authorizer)
	return path
}

func startMountServerWithNativeAdmission(t *testing.T, runtime Runtime, native NativeSessions, authorizer Authorizer) (string, *atomic.Int64) {
	return startMountServerWithNativeAdmissionAndProtectedPeer(
		t, runtime, native, authorizer, func(wire.Peer) error { return nil },
	)
}

func startMountServerWithNativeAdmissionAndProtectedPeer(
	t *testing.T,
	runtime Runtime,
	native NativeSessions,
	authorizer Authorizer,
	protectedNativePeer func(wire.Peer) error,
) (string, *atomic.Int64) {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "fusekit-mount-service-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "socket")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	server := &wire.Server{Build: transportproto.Build, HandshakeTimeout: 100 * time.Millisecond}
	if _, err := Register(server, Config{
		Runtime: runtime, NativeSessions: native, Authorizer: authorizer,
		ProtectedNativePeer: protectedNativePeer,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	inflight := &atomic.Int64{}
	go func() {
		admit := func() (func(), error) {
			inflight.Add(1)
			var once sync.Once
			return func() { once.Do(func() { inflight.Add(-1) }) }, nil
		}
		done <- server.Serve(ctx, listener, admit, admit)
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})
	return path, inflight
}

func waitAtomicZero(t *testing.T, value *atomic.Int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for value.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("inflight admissions = %d, want zero", value.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

type recordingNativeSessions struct {
	mu       sync.Mutex
	identity *Identity
	unbound  chan struct{}
	releases atomic.Int64
}

func newRecordingNativeSessions() *recordingNativeSessions {
	return &recordingNativeSessions{unbound: make(chan struct{}, 1)}
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

func (s *recordingNativeSessions) Ready(_ context.Context, identity Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.identity == nil || s.identity.Session != identity.Session {
		return ErrUnauthorized
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

func (*recordingNativeSessions) Routes(context.Context) ([]NativeRoute, error) {
	return []NativeRoute{{Name: "acct", Tenant: "tenant-native", Generation: 1}}, nil
}

func (s *recordingNativeSessions) Pin(_ context.Context, name string) (NativePin, error) {
	if name != "acct" {
		return NativePin{}, catalog.ErrNotFound
	}
	return NativePin{
		Route: NativeRoute{Name: name, Tenant: "tenant-native", Generation: 1},
		Spec: tenant.TenantSpec{
			OwnerID: "owner-native", ID: "tenant-native", PresentationRoot: "/Volumes/FuseKit/acct",
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

func (emptyNativeSessions) Bind(context.Context, Identity) error  { return nil }
func (emptyNativeSessions) Ready(context.Context, Identity) error { return nil }
func (emptyNativeSessions) Unbind(Identity)                       {}

func (emptyNativeSessions) Routes(context.Context) ([]NativeRoute, error) { return nil, nil }

func (emptyNativeSessions) Pin(context.Context, string) (NativePin, error) {
	return NativePin{}, catalog.ErrNotFound
}

func newMountClient(t *testing.T, path string) *Client {
	t.Helper()
	client, err := NewClient(context.Background(), wire.ClientConfig{Dial: wire.UnixDialer(path)})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
