package mountservice

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/tenant"
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
	registered, err := client.RegisterTenant(context.Background(), id, definition)
	if err != nil {
		t.Fatalf("RegisterTenant: %v", err)
	}
	if registered.TenantID != "acct-18" || registered.Generation != 1 {
		t.Fatalf("RegisterTenant response = %#v", registered)
	}
	if runtime.spec.OwnerID != "trusted-owner" || runtime.spec.ID != id || runtime.spec.Generation != 1 {
		t.Fatalf("registered spec = %#v", runtime.spec)
	}
	prepared, err := client.PrepareTenant(context.Background(), id, 1, 9)
	if err != nil {
		t.Fatalf("PrepareTenant: %v", err)
	}
	if prepared.State == nil || prepared.State.TenantID != "acct-18" || prepared.State.Generation != 1 || prepared.State.Requested != 9 {
		t.Fatalf("PrepareTenant response = %#v", prepared)
	}
	state, err := client.State(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if state.State == nil || state.State.StateVersion == 0 {
		t.Fatalf("State response = %#v", state)
	}
	next := testDefinition(2)
	replaced, err := client.ReplaceTenant(context.Background(), id, 1, next)
	if err != nil {
		t.Fatalf("ReplaceTenant: %v", err)
	}
	if replaced.Generation != 2 || runtime.spec.OwnerID != "trusted-owner" || runtime.spec.Generation != 2 {
		t.Fatalf("ReplaceTenant response/spec = %#v / %#v", replaced, runtime.spec)
	}
	if _, err := client.State(context.Background(), id, 1); err == nil {
		t.Fatal("stale generation State succeeded")
	} else {
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.Code != mountproto.ErrorCodeConflict {
			t.Fatalf("stale generation error = %T %v", err, err)
		}
	}
	removed, err := client.RemoveTenant(context.Background(), id, 2)
	if err != nil {
		t.Fatalf("RemoveTenant: %v", err)
	}
	if removed.Generation != 2 || runtime.present {
		t.Fatalf("RemoveTenant response/present = %#v / %v", removed, runtime.present)
	}
	for _, identity := range authorizer.identities() {
		if identity.Build != mountproto.Build || identity.Session == nil || identity.Peer.PID <= 0 || identity.Peer.UID != os.Getuid() {
			t.Fatalf("authorizer identity = %#v", identity)
		}
	}
}

func TestMalformedOwnerOldLFAndBuildMismatchCannotMutate(t *testing.T) {
	runtime := &fakeRuntime{}
	authorizer := &recordingAuthorizer{owner: "trusted-owner"}
	path := startMountServer(t, runtime, authorizer)
	rawClient, err := wire.NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), Build: mountproto.Build,
	})
	if err != nil {
		t.Fatalf("wire.NewClient: %v", err)
	}
	defer func() {
		if err := rawClient.Close(); err != nil {
			t.Errorf("Close raw client: %v", err)
		}
	}()
	payload := []byte(`{"protocol":1,"owner_id":"spoofed","definition":{"presentation_root":"/Volumes/FuseKit/acct-18","backing_root":"/Users/test/.cc-pool/accounts/acct-18","content_source_id":"source","access_mode":"read_write","case_policy":"sensitive","presentations":["mount"],"generation":1}}`)
	result, err := rawClient.Call(context.Background(), wire.Op(mountproto.OperationTenantRegister), "acct-18", payload)
	if err != nil {
		t.Fatalf("malformed Call: %v", err)
	}
	var response mountproto.RegisterTenantResponse
	if err := mountproto.Decode(result.Response.Payload, &response); err != nil {
		t.Fatalf("Decode malformed response: %v", err)
	}
	if response.Code != mountproto.ErrorCodeInvalidRequest {
		t.Fatalf("malformed response = %#v", response)
	}

	oldClient, err := wire.NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), Build: "fusekit.mount.old-build",
		HandshakeTimeout: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("old build transport handshake: %v", err)
	}
	oldPayload, err := mountproto.Encode(mountproto.RegisterTenantRequest{
		Protocol: mountproto.Version, Definition: testDefinition(1),
	})
	if err != nil {
		t.Fatalf("Encode old build request: %v", err)
	}
	oldResult, err := oldClient.Call(context.Background(), wire.Op(mountproto.OperationTenantRegister), "acct-18", oldPayload)
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
	if runtime.registerCalls != 0 {
		t.Fatalf("register calls = %d, want zero", runtime.registerCalls)
	}
	if identities := authorizer.identities(); len(identities) != 0 {
		t.Fatalf("rejected requests reached authorization: %d calls", len(identities))
	}
}

func TestPrepareCancellationReachesRuntimeAndSettles(t *testing.T) {
	runtime := &fakeRuntime{
		present:         true,
		spec:            tenant.TenantSpec{ID: "acct-18", OwnerID: "trusted-owner", Generation: 1},
		prepareStarted:  make(chan struct{}),
		prepareCanceled: make(chan struct{}),
	}
	path := startMountServer(t, runtime, &recordingAuthorizer{owner: "trusted-owner"})
	client := newMountClient(t, path)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.PrepareTenant(ctx, "acct-18", 1, 8)
		result <- err
	}()
	select {
	case <-runtime.prepareStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not receive PrepareTenant")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled PrepareTenant succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client cancellation did not settle")
	}
	select {
	case <-runtime.prepareCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime context was not canceled")
	}
}

type fakeRuntime struct {
	mu sync.Mutex

	present       bool
	spec          tenant.TenantSpec
	requested     catalog.Revision
	registerCalls int

	prepareStarted  chan struct{}
	prepareCanceled chan struct{}
	prepareOnce     sync.Once
}

func (r *fakeRuntime) RegisterTenant(_ context.Context, spec tenant.TenantSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registerCalls++
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

func (r *fakeRuntime) RemoveTenant(_ context.Context, id catalog.TenantID, generation catalog.Generation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.present || r.spec.ID != id {
		return tenant.ErrTenantNotFound
	}
	if r.spec.Generation != generation {
		return tenant.ErrGenerationConflict
	}
	r.present = false
	return nil
}

func (r *fakeRuntime) PrepareTenant(ctx context.Context, id catalog.TenantID, generation catalog.Generation, revision catalog.Revision) (tenant.TenantState, error) {
	r.mu.Lock()
	if !r.present || r.spec.ID != id {
		r.mu.Unlock()
		return tenant.TenantState{}, tenant.ErrTenantNotFound
	}
	if r.spec.Generation != generation {
		r.mu.Unlock()
		return tenant.TenantState{}, tenant.ErrGenerationConflict
	}
	started := r.prepareStarted
	canceled := r.prepareCanceled
	r.mu.Unlock()
	if started != nil {
		r.prepareOnce.Do(func() { close(started) })
		<-ctx.Done()
		close(canceled)
		return tenant.TenantState{}, ctx.Err()
	}
	r.mu.Lock()
	r.requested = revision
	state := r.stateLocked()
	r.mu.Unlock()
	return state, nil
}

func (r *fakeRuntime) State(_ context.Context, id catalog.TenantID, generation catalog.Generation) (tenant.TenantState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.present || r.spec.ID != id {
		return tenant.TenantState{}, tenant.ErrTenantNotFound
	}
	if r.spec.Generation != generation {
		return tenant.TenantState{}, tenant.ErrGenerationConflict
	}
	return r.stateLocked(), nil
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

func (a *recordingAuthorizer) identities() []Identity {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Identity(nil), a.seen...)
}

func testDefinition(generation uint64) mountproto.TenantDefinition {
	return mountproto.TenantDefinition{
		PresentationRoot: "/Volumes/FuseKit/acct-18",
		BackingRoot:      "/Users/test/.cc-pool/accounts/acct-18",
		ContentSourceID:  "acct-18-source",
		AccessMode:       mountproto.AccessModeReadWrite,
		CasePolicy:       mountproto.CasePolicySensitive,
		Presentations:    []mountproto.Presentation{mountproto.PresentationMount, mountproto.PresentationFileProvider},
		Generation:       generation,
	}
}

func startMountServer(t *testing.T, runtime Runtime, authorizer Authorizer) string {
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
	server := &wire.Server{Build: mountproto.Build, HandshakeTimeout: 100 * time.Millisecond}
	if _, err := Register(server, Config{Runtime: runtime, Authorizer: authorizer}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener, func() (func(), error) { return func() {}, nil })
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
	return path
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
