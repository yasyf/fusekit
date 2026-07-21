package mountservice

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
)

func TestNativeCatalogHandlesAreBoundedRouteFencedAndSessionOwned(t *testing.T) {
	store := newRecordingNativeCatalog()
	native := newRecordingNativeSessions()
	path, _ := startMountServerWithNativeCatalog(
		t, &fakeRuntime{}, native, store, &recordingAuthorizer{owner: "owner-native"},
		func(context.Context, wire.Peer) error { return nil },
	)
	client := newMountClient(t, path)
	if _, err := client.BindNative(context.Background()); err != nil {
		t.Fatalf("BindNative: %v", err)
	}
	if _, err := client.NativePin(context.Background(), "acct"); err != nil {
		t.Fatalf("NativePin: %v", err)
	}

	opened, err := client.NativeSnapshotOpen(
		context.Background(), "tenant-native", 1, store.object.ID, store.object.Revision,
	)
	if err != nil {
		t.Fatalf("NativeSnapshotOpen: %v", err)
	}
	if opened.Object == nil || opened.Handle == "" || opened.Object.ID != store.object.ID.String() {
		t.Fatalf("opened snapshot = %+v", opened)
	}
	read, err := client.NativeSnapshotRead(context.Background(), opened.Handle, 1, 3)
	if err != nil {
		t.Fatalf("NativeSnapshotRead: %v", err)
	}
	if string(read.Data) != "eed" || read.EOF {
		t.Fatalf("snapshot read = %q eof=%v", read.Data, read.EOF)
	}
	if _, err := client.NativeSnapshotRead(context.Background(), opened.Handle, 0, nativeIOChunkLimit+1); err == nil {
		t.Fatal("oversized snapshot read succeeded")
	}
	if err := client.NativeSnapshotClose(context.Background(), opened.Handle); err != nil {
		t.Fatalf("NativeSnapshotClose: %v", err)
	}

	write, err := client.NativeWriteOpen(
		context.Background(), "tenant-native", 1, store.object.ID, store.object.Revision,
	)
	if err != nil {
		t.Fatalf("NativeWriteOpen: %v", err)
	}
	if count, err := client.NativeWrite(context.Background(), write.Handle, 0, []byte("new")); err != nil || count != 3 {
		t.Fatalf("NativeWrite = %d, %v", count, err)
	}
	if err := client.NativeWriteTruncate(context.Background(), write.Handle, 3); err != nil {
		t.Fatalf("NativeWriteTruncate: %v", err)
	}
	if err := client.NativeWriteSync(context.Background(), write.Handle); err != nil {
		t.Fatalf("NativeWriteSync: %v", err)
	}
	committed, err := client.NativeWriteCommit(context.Background(), write.Handle)
	if err != nil {
		t.Fatalf("NativeWriteCommit: %v", err)
	}
	if committed.Object == nil || committed.Object.ContentRevision != uint64(store.initialContentRevision+1) {
		t.Fatalf("committed object = %+v", committed.Object)
	}
	if count, err := client.NativeWrite(context.Background(), write.Handle, 3, []byte("!")); err != nil || count != 1 {
		t.Fatalf("post-commit NativeWrite = %d, %v", count, err)
	}
	next, err := client.NativeWriteCommit(context.Background(), write.Handle)
	if err != nil {
		t.Fatalf("NativeWriteCommit(next): %v", err)
	}
	if next.Object == nil || next.Object.ContentRevision != uint64(store.initialContentRevision+2) {
		t.Fatalf("next committed object = %+v", next.Object)
	}

	if _, err := client.NativeSnapshotOpen(
		context.Background(), "other-tenant", 1, store.object.ID, store.object.Revision,
	); err == nil {
		t.Fatal("un-pinned tenant snapshot open succeeded")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close(native session): %v", err)
	}
	select {
	case owner := <-store.closed:
		if owner == "" || owner != store.owner {
			t.Fatalf("closed owner = %q, want %q", owner, store.owner)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("native session did not close catalog handles")
	}
	native.waitUnbound(t)
	if native.releases.Load() != 1 {
		t.Fatalf("route releases = %d, want 1", native.releases.Load())
	}
}

func TestNativeUnbindReplaysLostAcknowledgementBeforeRebind(t *testing.T) {
	store := newRecordingNativeCatalog()
	native := newRecordingNativeSessions()
	path, _ := startMountServerWithNativeCatalog(
		t, &fakeRuntime{}, native, store, &recordingAuthorizer{owner: "owner-native"},
		func(context.Context, wire.Peer) error { return nil },
	)
	client := newMountClient(t, path)
	binding, err := client.BindNative(context.Background())
	if err != nil {
		t.Fatalf("BindNative: %v", err)
	}
	if _, err := client.NativePin(context.Background(), "acct"); err != nil {
		t.Fatalf("NativePin: %v", err)
	}
	if err := client.NativeUnbind(context.Background()); err != nil {
		t.Fatalf("NativeUnbind(first): %v", err)
	}
	if err := client.NativeReady(context.Background(), testNativeMountProof()); err == nil {
		t.Fatal("settled native session acknowledged readiness")
	}
	if _, err := client.NativeRoutePage(context.Background(), 0, "", 1); err == nil {
		t.Fatal("settled native session enumerated routes")
	}
	if err := client.NativeUnbind(context.Background()); err != nil {
		t.Fatalf("NativeUnbind(replay lost acknowledgement): %v", err)
	}
	if store.closeCalls.Load() != 1 || native.releases.Load() != 1 {
		t.Fatalf("physical unbind settlement = catalog %d, pins %d; want one each",
			store.closeCalls.Load(), native.releases.Load())
	}
	if _, err := client.NativePin(context.Background(), "acct"); err == nil {
		t.Fatal("closed native session admitted a new pin")
	}

	second := newMountClient(t, path)
	if _, err := second.BindNative(context.Background()); err == nil {
		t.Fatal("new native session bound before acknowledged session closed its transport")
	}
	if err := binding.Close(); err != nil {
		t.Fatalf("NativeBinding.Close: %v", err)
	}
	if err := binding.Close(); err != nil {
		t.Fatalf("NativeBinding.Close(replay): %v", err)
	}
	native.waitUnbound(t)
	rebound, err := second.BindNative(context.Background())
	if err != nil {
		t.Fatalf("BindNative(after acknowledged teardown): %v", err)
	}
	if err := rebound.Close(); err != nil {
		t.Fatalf("rebound Close: %v", err)
	}
}

func TestNativeUnbindReplaysIdenticalTerminalError(t *testing.T) {
	settlementFailure := errors.New("injected native teardown failure")
	settle := make(chan struct{})
	close(settle)
	store := &blockingCloseNativeCatalog{
		started: make(chan struct{}),
		settle:  settle,
		result:  settlementFailure,
	}
	native := newRecordingNativeSessions()
	path, _ := startMountServerWithNativeCatalog(
		t, &fakeRuntime{}, native, store, &recordingAuthorizer{owner: "owner-native"},
		func(context.Context, wire.Peer) error { return nil },
	)
	client := newMountClient(t, path)
	binding, err := client.BindNative(context.Background())
	if err != nil {
		t.Fatalf("BindNative: %v", err)
	}

	first := client.NativeUnbind(context.Background())
	second := client.NativeUnbind(context.Background())
	var firstRemote, secondRemote *RemoteError
	if !errors.As(first, &firstRemote) || !errors.As(second, &secondRemote) ||
		firstRemote.Code != mountproto.ErrorCodeUnavailable ||
		secondRemote.Code != firstRemote.Code ||
		secondRemote.Message != firstRemote.Message ||
		!strings.Contains(firstRemote.Message, settlementFailure.Error()) {
		t.Fatalf("terminal unbind results = %v and %v, want identical unavailable response", first, second)
	}
	if err := binding.Close(); err == nil || err.Error() != first.Error() {
		t.Fatalf("binding Close = %v, want cached terminal response %v", err, first)
	}
	if store.calls.Load() != 1 {
		t.Fatalf("physical catalog settlements = %d, want one", store.calls.Load())
	}
	native.waitUnbound(t)
}

func TestNativeUnbindWaitsForAdmittedOperationAndItsResourceSettlement(t *testing.T) {
	store := newRecordingNativeCatalog()
	native := newRecordingNativeSessions()
	native.pinStarted = make(chan struct{})
	native.pinContinue = make(chan struct{})
	path, _ := startMountServerWithNativeCatalog(
		t, &fakeRuntime{}, native, store, &recordingAuthorizer{owner: "owner-native"},
		func(context.Context, wire.Peer) error { return nil },
	)
	client := newMountClient(t, path)
	binding, err := client.BindNative(context.Background())
	if err != nil {
		t.Fatalf("BindNative: %v", err)
	}

	pinned := make(chan error, 1)
	go func() {
		_, err := client.NativePin(context.Background(), "acct")
		pinned <- err
	}()
	<-native.pinStarted

	closed := make(chan error, 1)
	go func() { closed <- binding.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := client.NativeReady(context.Background(), testNativeMountProof()); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("unbind did not close native operation admission")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closed:
		t.Fatalf("unbind acknowledged before admitted pin settled: %v", err)
	default:
	}
	if store.closeCalls.Load() != 0 || native.releases.Load() != 0 {
		t.Fatalf("premature settlement = catalog %d pin %d, want neither before admitted operation exits",
			store.closeCalls.Load(), native.releases.Load())
	}

	close(native.pinContinue)
	if err := <-pinned; err == nil {
		t.Fatal("pin admitted before unbind was published after session settlement")
	}
	if err := <-closed; err != nil {
		t.Fatalf("NativeBinding.Close: %v", err)
	}
	if store.closeCalls.Load() != 1 || native.releases.Load() != 1 {
		t.Fatalf("terminal settlement = catalog %d pin %d, want one each",
			store.closeCalls.Load(), native.releases.Load())
	}
	native.waitUnbound(t)
}

func TestNativeBindingCloseIsOneAcknowledgedBarrierAcrossConcurrentCallers(t *testing.T) {
	settlementFailure := errors.New("injected terminal native settlement")
	store := &blockingCloseNativeCatalog{
		started: make(chan struct{}),
		settle:  make(chan struct{}),
		result:  settlementFailure,
	}
	native := newRecordingNativeSessions()
	path, _ := startMountServerWithNativeCatalog(
		t, &fakeRuntime{}, native, store, &recordingAuthorizer{owner: "owner-native"},
		func(context.Context, wire.Peer) error { return nil },
	)
	client := newMountClient(t, path)
	binding, err := client.BindNative(context.Background())
	if err != nil {
		t.Fatalf("BindNative: %v", err)
	}

	const callers = 16
	results := make(chan error, callers)
	for index := range callers {
		if index%2 == 0 {
			go func() { results <- binding.Close() }()
		} else {
			go func() { results <- client.Close() }()
		}
	}
	<-store.started

	second := newMountClient(t, path)
	if _, err := second.BindNative(context.Background()); err == nil {
		t.Fatal("new native session bound before the acknowledged barrier settled")
	}
	select {
	case err := <-results:
		t.Fatalf("Close returned before terminal settlement: %v", err)
	default:
	}

	close(store.settle)
	first := <-results
	var remote *RemoteError
	if !errors.As(first, &remote) ||
		remote.Code != mountproto.ErrorCodeUnavailable ||
		!strings.Contains(remote.Message, settlementFailure.Error()) {
		t.Fatalf("Close result = %v, want terminal settlement failure response", first)
	}
	for range callers - 1 {
		if result := <-results; result != first {
			t.Fatalf("Close result = %v, want identical cached result %v", result, first)
		}
	}
	if store.calls.Load() != 1 {
		t.Fatalf("physical catalog settlements = %d, want one", store.calls.Load())
	}
	native.waitUnbound(t)
	rebound, err := second.BindNative(context.Background())
	if err != nil {
		t.Fatalf("BindNative(after exact settlement): %v", err)
	}
	if err := rebound.Close(); err == nil {
		t.Fatal("rebound Close lost the catalog's terminal settlement failure")
	}
}

func TestNativeWriteCommitRecoversWorkerDerivedMutationAfterLostResponse(t *testing.T) {
	store := newRecordingNativeCatalog()
	store.loseNextCommit = true
	path, _ := startMountServerWithNativeCatalog(
		t, &fakeRuntime{}, newRecordingNativeSessions(), store,
		&recordingAuthorizer{owner: "owner-native"},
		func(context.Context, wire.Peer) error { return nil },
	)
	client := newMountClient(t, path)
	if _, err := client.BindNative(context.Background()); err != nil {
		t.Fatalf("BindNative: %v", err)
	}
	if _, err := client.NativePin(context.Background(), "acct"); err != nil {
		t.Fatalf("NativePin: %v", err)
	}
	write, err := client.NativeWriteOpen(
		context.Background(), "tenant-native", 1, store.object.ID, store.object.Revision,
	)
	if err != nil {
		t.Fatalf("NativeWriteOpen: %v", err)
	}
	if _, err := client.NativeWrite(context.Background(), write.Handle, 0, []byte("changed")); err != nil {
		t.Fatalf("NativeWrite: %v", err)
	}
	if _, err := client.NativeWriteCommit(context.Background(), write.Handle); err == nil {
		t.Fatal("lost commit response reported success")
	}
	recovered, err := client.NativeWriteCommit(context.Background(), write.Handle)
	if err != nil {
		t.Fatalf("recover exact NativeWriteCommit: %v", err)
	}
	if recovered.Object == nil || recovered.Object.ContentRevision != uint64(store.initialContentRevision+1) {
		t.Fatalf("recovered object = %+v", recovered.Object)
	}
	if _, err := client.NativeWrite(context.Background(), write.Handle, recovered.Object.Size, []byte("!")); err != nil {
		t.Fatalf("NativeWrite(after rebase): %v", err)
	}
	committed, err := client.NativeWriteCommit(context.Background(), write.Handle)
	if err != nil {
		t.Fatalf("NativeWriteCommit(after rebase): %v", err)
	}
	if committed.Object == nil || committed.Object.ContentRevision != uint64(store.initialContentRevision+2) {
		t.Fatalf("rebased committed object = %+v", committed.Object)
	}
	if committed.MutationID == recovered.MutationID {
		t.Fatal("rebased dirty epoch reused its prior derived mutation")
	}
}

func TestNativeWriteCommitRejectsWrongRevision(t *testing.T) {
	store := newRecordingNativeCatalog()
	store.returnCommitRevision = store.object.Revision + 2
	path, _ := startMountServerWithNativeCatalog(
		t, &fakeRuntime{}, newRecordingNativeSessions(), store,
		&recordingAuthorizer{owner: "owner-native"},
		func(context.Context, wire.Peer) error { return nil },
	)
	client := newMountClient(t, path)
	if _, err := client.BindNative(context.Background()); err != nil {
		t.Fatalf("BindNative: %v", err)
	}
	if _, err := client.NativePin(context.Background(), "acct"); err != nil {
		t.Fatalf("NativePin: %v", err)
	}
	write, err := client.NativeWriteOpen(
		context.Background(), "tenant-native", 1, store.object.ID, store.object.Revision,
	)
	if err != nil {
		t.Fatalf("NativeWriteOpen: %v", err)
	}
	if _, err := client.NativeWrite(context.Background(), write.Handle, 0, []byte("changed")); err != nil {
		t.Fatalf("NativeWrite: %v", err)
	}
	if _, err := client.NativeWriteCommit(context.Background(), write.Handle); err == nil {
		t.Fatal("commit with wrong catalog revision succeeded")
	}
	recovered, err := client.NativeWriteCommit(context.Background(), write.Handle)
	if err != nil {
		t.Fatalf("recover commit after invalid response: %v", err)
	}
	if recovered.Object == nil || recovered.Object.Revision != uint64(store.object.Revision) {
		t.Fatalf("recovered object = %+v", recovered.Object)
	}
}

type recordingNativeCatalog struct {
	mu sync.Mutex

	object                 catalog.Object
	initialContentRevision catalog.Revision
	body                   []byte
	stage                  []byte
	owner                  string
	closed                 chan string
	dirty                  bool
	lastOperation          catalog.MutationID
	lastObject             catalog.Object
	loseNextCommit         bool
	returnCommitRevision   catalog.Revision
	closeCalls             atomic.Int64
}

func newRecordingNativeCatalog() *recordingNativeCatalog {
	objectID := catalog.ObjectID{1}
	parentID := catalog.ObjectID{2}
	object := catalog.Object{
		Tenant: "tenant-native", ID: objectID, Parent: parentID, Name: "settings.json",
		Kind: catalog.KindFile, Mode: 0o600, Size: 6, Revision: 7, MetadataRevision: 7,
		ContentRevision: 3, Visibility: catalog.Visibility{Mount: true},
	}
	return &recordingNativeCatalog{
		object: object, initialContentRevision: object.ContentRevision,
		body: []byte("seeded"), closed: make(chan string, 1),
	}
}

func (s *recordingNativeCatalog) bind(owner string, tenant catalog.TenantID, generation catalog.Generation, id catalog.ObjectID, revision catalog.Revision) error {
	if owner == "" || tenant != s.object.Tenant || generation != 1 || id != s.object.ID || revision != s.object.Revision {
		return catalog.ErrGenerationMismatch
	}
	if s.owner == "" {
		s.owner = owner
	}
	if s.owner != owner {
		return catalog.ErrConflict
	}
	return nil
}

func (s *recordingNativeCatalog) OpenSnapshot(_ context.Context, owner string, tenant catalog.TenantID, generation catalog.Generation, id catalog.ObjectID, revision catalog.Revision) (NativeHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.bind(owner, tenant, generation, id, revision); err != nil {
		return NativeHandle{}, err
	}
	return NativeHandle{Token: "snapshot", Object: s.object}, nil
}

func (s *recordingNativeCatalog) ReadSnapshot(_ context.Context, owner, token string, offset int64, length int) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner != s.owner || token != "snapshot" || offset < 0 {
		return nil, false, catalog.ErrNotFound
	}
	return readRange(s.body, offset, length)
}

func (s *recordingNativeCatalog) CloseSnapshot(_ context.Context, owner, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner != s.owner || token != "snapshot" {
		return catalog.ErrNotFound
	}
	return nil
}

func (s *recordingNativeCatalog) OpenWrite(_ context.Context, owner string, tenant catalog.TenantID, generation catalog.Generation, id catalog.ObjectID, revision catalog.Revision) (NativeHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.bind(owner, tenant, generation, id, revision); err != nil {
		return NativeHandle{}, err
	}
	s.stage = append(s.stage[:0], s.body...)
	s.dirty = false
	s.lastOperation = catalog.MutationID{}
	s.lastObject = catalog.Object{}
	return NativeHandle{Token: "write", Object: s.object}, nil
}

func (s *recordingNativeCatalog) ReadWrite(_ context.Context, owner, token string, offset int64, length int) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner != s.owner || token != "write" {
		return nil, false, catalog.ErrNotFound
	}
	return readRange(s.stage, offset, length)
}

func (s *recordingNativeCatalog) Write(_ context.Context, owner, token string, offset int64, data []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner != s.owner || token != "write" || offset < 0 {
		return 0, catalog.ErrNotFound
	}
	end := int(offset) + len(data)
	if end > len(s.stage) {
		s.stage = append(s.stage, make([]byte, end-len(s.stage))...)
	}
	written := copy(s.stage[int(offset):], data)
	s.dirty = true
	return written, nil
}

func (s *recordingNativeCatalog) Truncate(_ context.Context, owner, token string, size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner != s.owner || token != "write" || size < 0 {
		return catalog.ErrNotFound
	}
	if size <= int64(len(s.stage)) {
		s.stage = s.stage[:size]
	} else {
		s.stage = append(s.stage, make([]byte, int(size)-len(s.stage))...)
	}
	s.dirty = true
	return nil
}

func (s *recordingNativeCatalog) Sync(_ context.Context, owner, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner != s.owner || token != "write" {
		return catalog.ErrNotFound
	}
	return nil
}

func (s *recordingNativeCatalog) CommitWrite(_ context.Context, owner, token string) (catalog.Object, catalog.MutationID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner != s.owner || token != "write" {
		return catalog.Object{}, catalog.MutationID{}, catalog.ErrNotFound
	}
	if !s.dirty {
		if s.lastOperation == (catalog.MutationID{}) {
			return catalog.Object{}, catalog.MutationID{}, catalog.ErrConflict
		}
		return s.lastObject, s.lastOperation, nil
	}
	s.body = append(s.body[:0], s.stage...)
	s.object.Revision++
	s.object.ContentRevision++
	s.object.Size = int64(len(s.body))
	s.dirty = false
	s.lastOperation = nativeMutationForRevision(s.object.Revision)
	s.lastObject = s.object
	if s.loseNextCommit {
		s.loseNextCommit = false
		return catalog.Object{}, catalog.MutationID{}, errors.New("injected lost commit response")
	}
	if s.returnCommitRevision != 0 {
		changed := s.object
		changed.Revision = s.returnCommitRevision
		s.returnCommitRevision = 0
		return changed, s.lastOperation, nil
	}
	return s.object, s.lastOperation, nil
}

func (s *recordingNativeCatalog) AbortWrite(_ context.Context, owner, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner != s.owner || token != "write" {
		return catalog.ErrNotFound
	}
	s.stage = nil
	return nil
}

func (s *recordingNativeCatalog) CloseSession(_ context.Context, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalls.Add(1)
	select {
	case s.closed <- owner:
	default:
	}
	return nil
}

func readRange(body []byte, offset int64, length int) ([]byte, bool, error) {
	if offset < 0 || length <= 0 {
		return nil, false, catalog.ErrInvalidObject
	}
	if offset >= int64(len(body)) {
		return nil, true, nil
	}
	end := min(int(offset)+length, len(body))
	return bytes.Clone(body[int(offset):end]), end == len(body), nil
}

var _ NativeCatalog = (*recordingNativeCatalog)(nil)

func nativeMutationForRevision(revision catalog.Revision) catalog.MutationID {
	var operation catalog.MutationID
	binary.BigEndian.PutUint64(operation[:8], uint64(revision))
	operation[len(operation)-1] = byte(revision)
	return operation
}
