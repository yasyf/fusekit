package mountservice

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/mountproto"
)

func TestNativeMountedRemainsConcurrentAndMapsHandlerFailure(t *testing.T) {
	injected := errors.New("injected native mounted failure")
	native := newControlledMountedSessions()
	authorizer := newNativeOperationAuthorizer()
	var protectedCalls atomic.Int64
	path, _ := startMountServerWithNativeAdmissionAndProtectedPeer(
		t, &fakeRuntime{}, native, authorizer,
		func(context.Context, wire.Peer) error {
			protectedCalls.Add(1)
			return nil
		},
	)
	client := newMountClient(t, path)
	binding, err := client.BindNative(t.Context())
	if err != nil {
		t.Fatalf("BindNative: %v", err)
	}

	mounted := make(chan error, 1)
	go func() { mounted <- client.NativeMounted(t.Context(), testNativeMountIdentity()) }()
	invocation := waitMountedInvocation(t, native)
	if invocation.identity.Session == nil || invocation.mount != testNativeMountIdentity() {
		t.Fatalf("mounted invocation = %#v", invocation)
	}

	callbackContext, cancelCallback := context.WithTimeout(t.Context(), time.Second)
	defer cancelCallback()
	page, err := client.NativeRoutePage(callbackContext, 0, "", mountproto.MaxNativeRoutePageSize)
	if err != nil || page.Snapshot != 1 {
		t.Fatalf("NativeRoutePage during NativeMounted = %#v, %v", page, err)
	}
	native.mountedResults <- injected
	if err := <-mounted; err == nil {
		t.Fatal("NativeMounted succeeded after handler failure")
	} else {
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.Code != mountproto.ErrorCodeUnavailable ||
			!strings.Contains(remote.Message, injected.Error()) {
			t.Fatalf("NativeMounted failure = %T %v", err, err)
		}
	}
	if err := client.NativeReady(t.Context(), testNativeMountProof()); err != nil {
		t.Fatalf("NativeReady after mounted failure: %v", err)
	}
	mountedIdentities := authorizer.nativeIdentities(mountproto.OperationNativeMounted)
	if len(mountedIdentities) != 1 || mountedIdentities[0].Session != invocation.identity.Session {
		t.Fatalf("native mounted authorizations = %#v, want exact mounted session", mountedIdentities)
	}
	if protectedCalls.Load() != 4 {
		t.Fatalf("protected peer checks = %d, want bind, mounted, route, and ready", protectedCalls.Load())
	}

	if err := binding.Close(); err != nil {
		t.Fatalf("NativeBinding.Close: %v", err)
	}
	native.waitUnbound(t)
	second := newMountClient(t, path)
	rebound, err := second.BindNative(t.Context())
	if err != nil {
		t.Fatalf("BindNative after mounted failure: %v", err)
	}
	if err := rebound.Close(); err != nil {
		t.Fatalf("rebound Close: %v", err)
	}
}

func TestNativeMountedCancellationReleasesAdmissionBeforeUnbindSettles(t *testing.T) {
	native := newControlledMountedSessions()
	path := startMountServerWithNative(t, &fakeRuntime{}, native, newNativeOperationAuthorizer())
	client := newMountClient(t, path)
	binding, err := client.BindNative(t.Context())
	if err != nil {
		t.Fatalf("BindNative: %v", err)
	}

	mountedContext, cancelMounted := context.WithCancel(t.Context())
	mounted := make(chan error, 1)
	go func() { mounted <- client.NativeMounted(mountedContext, testNativeMountIdentity()) }()
	_ = waitMountedInvocation(t, native)

	closed := make(chan error, 1)
	go func() { closed <- binding.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := client.NativeReady(t.Context(), testNativeMountProof()); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("unbind did not close native operation admission")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closed:
		t.Fatalf("unbind settled before NativeMounted cancellation: %v", err)
	default:
	}

	cancelMounted()
	if err := <-mounted; !errors.Is(err, context.Canceled) {
		t.Fatalf("NativeMounted cancellation = %T %v, want context canceled", err, err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("NativeBinding.Close: %v", err)
	}
	native.waitUnbound(t)
	waitAtomicValue(t, &native.unbindCalls, 1, "native unbind calls")
	waitAtomicValue(t, &native.settledCalls, 1, "native settled calls")

	second := newMountClient(t, path)
	rebound, err := second.BindNative(t.Context())
	if err != nil {
		t.Fatalf("BindNative after canceled mounted call: %v", err)
	}
	if err := rebound.Close(); err != nil {
		t.Fatalf("rebound Close: %v", err)
	}
}

func TestNativeMountedRejectsUnboundForeignAndSettledSessionsBeforeHandler(t *testing.T) {
	native := newControlledMountedSessions()
	authorizer := newNativeOperationAuthorizer()
	var protectedCalls atomic.Int64
	path, _ := startMountServerWithNativeAdmissionAndProtectedPeer(
		t, &fakeRuntime{}, native, authorizer,
		func(context.Context, wire.Peer) error {
			protectedCalls.Add(1)
			return nil
		},
	)
	first := newMountClient(t, path)
	second := newMountClient(t, path)

	assertRejectedBeforeMounted(t, first, native, &protectedCalls, "unbound session")
	binding, err := first.BindNative(t.Context())
	if err != nil {
		t.Fatalf("BindNative(first): %v", err)
	}
	assertRejectedBeforeMounted(t, second, native, &protectedCalls, "foreign session")

	mounted := make(chan error, 1)
	go func() { mounted <- first.NativeMounted(t.Context(), testNativeMountIdentity()) }()
	invocation := waitMountedInvocation(t, native)
	native.mountedResults <- nil
	if err := <-mounted; err != nil {
		t.Fatalf("NativeMounted(bound session): %v", err)
	}
	if invocation.identity.Session == nil {
		t.Fatal("NativeMounted reached handler without an authenticated session")
	}

	if err := first.NativeUnbind(t.Context()); err != nil {
		t.Fatalf("NativeUnbind: %v", err)
	}
	assertRejectedBeforeMounted(t, first, native, &protectedCalls, "settled session")
	if err := binding.Close(); err != nil {
		t.Fatalf("NativeBinding.Close: %v", err)
	}
	native.waitUnbound(t)

	rebound, err := second.BindNative(t.Context())
	if err != nil {
		t.Fatalf("BindNative(second): %v", err)
	}
	mounted = make(chan error, 1)
	go func() { mounted <- second.NativeMounted(t.Context(), testNativeMountIdentity()) }()
	secondInvocation := waitMountedInvocation(t, native)
	native.mountedResults <- nil
	if err := <-mounted; err != nil {
		t.Fatalf("NativeMounted(rebound session): %v", err)
	}
	if secondInvocation.identity.Session == invocation.identity.Session {
		t.Fatal("rebound NativeMounted reused the settled session")
	}
	mountedIdentities := authorizer.nativeIdentities(mountproto.OperationNativeMounted)
	if len(mountedIdentities) != 5 ||
		mountedIdentities[2].Session != invocation.identity.Session ||
		mountedIdentities[4].Session != secondInvocation.identity.Session {
		t.Fatalf("native mounted authorizations = %#v, want five attempts bound to exact sessions", mountedIdentities)
	}
	if native.mountedCalls.Load() != 2 {
		t.Fatalf("native mounted handler calls = %d, want two bound calls", native.mountedCalls.Load())
	}
	if err := rebound.Close(); err != nil {
		t.Fatalf("rebound Close: %v", err)
	}
}

func assertRejectedBeforeMounted(
	t *testing.T,
	client *Client,
	native *controlledMountedSessions,
	protectedCalls *atomic.Int64,
	label string,
) {
	t.Helper()
	beforeHandler := native.mountedCalls.Load()
	beforeProtected := protectedCalls.Load()
	if err := client.NativeMounted(t.Context(), testNativeMountIdentity()); err == nil {
		t.Fatalf("NativeMounted(%s) succeeded", label)
	}
	if native.mountedCalls.Load() != beforeHandler {
		t.Fatalf("NativeMounted(%s) reached handler", label)
	}
	if protectedCalls.Load() != beforeProtected+1 {
		t.Fatalf("NativeMounted(%s) protected checks = %d, want one", label, protectedCalls.Load()-beforeProtected)
	}
}

type mountedInvocation struct {
	identity Identity
	mount    NativeMountIdentity
}

type controlledMountedSessions struct {
	*recordingNativeSessions
	mountedStarted chan mountedInvocation
	mountedResults chan error
	mountedCalls   atomic.Int64
	unbindCalls    atomic.Int64
	settledCalls   atomic.Int64
}

func newControlledMountedSessions() *controlledMountedSessions {
	return &controlledMountedSessions{
		recordingNativeSessions: newRecordingNativeSessions(),
		mountedStarted:          make(chan mountedInvocation, 1),
		mountedResults:          make(chan error, 1),
	}
}

func (s *controlledMountedSessions) Mounted(
	ctx context.Context,
	identity Identity,
	mount NativeMountIdentity,
) error {
	s.mountedCalls.Add(1)
	select {
	case s.mountedStarted <- mountedInvocation{identity: identity, mount: mount}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case result := <-s.mountedResults:
		return result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *controlledMountedSessions) Unbind(identity Identity) {
	s.unbindCalls.Add(1)
	s.recordingNativeSessions.Unbind(identity)
}

func (s *controlledMountedSessions) Settled(identity Identity, result error) {
	s.settledCalls.Add(1)
	s.recordingNativeSessions.Settled(identity, result)
}

func waitMountedInvocation(t *testing.T, native *controlledMountedSessions) mountedInvocation {
	t.Helper()
	select {
	case invocation := <-native.mountedStarted:
		return invocation
	case <-time.After(5 * time.Second):
		t.Fatal("NativeMounted did not reach the native session handler")
		return mountedInvocation{}
	}
}

func waitAtomicValue(t *testing.T, value *atomic.Int64, want int64, label string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for value.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s = %d, want %d", label, value.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

type nativeAuthorization struct {
	identity  Identity
	operation mountproto.Operation
}

type nativeOperationAuthorizer struct {
	*recordingAuthorizer
	mu     sync.Mutex
	native []nativeAuthorization
}

func newNativeOperationAuthorizer() *nativeOperationAuthorizer {
	return &nativeOperationAuthorizer{
		recordingAuthorizer: &recordingAuthorizer{owner: "owner-native"},
	}
}

func (a *nativeOperationAuthorizer) AuthorizeNative(
	ctx context.Context,
	identity Identity,
	operation mountproto.Operation,
) error {
	a.mu.Lock()
	a.native = append(a.native, nativeAuthorization{identity: identity, operation: operation})
	a.mu.Unlock()
	return a.recordingAuthorizer.AuthorizeNative(ctx, identity, operation)
}

func (a *nativeOperationAuthorizer) nativeIdentities(operation mountproto.Operation) []Identity {
	a.mu.Lock()
	defer a.mu.Unlock()
	var identities []Identity
	for _, call := range a.native {
		if call.operation == operation {
			identities = append(identities, call.identity)
		}
	}
	return identities
}
