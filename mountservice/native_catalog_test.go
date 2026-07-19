package mountservice

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
)

type emptyNativeCatalog struct{}

func (emptyNativeCatalog) OpenSnapshot(context.Context, string, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision) (NativeHandle, error) {
	return NativeHandle{}, catalog.ErrNotFound
}

func (emptyNativeCatalog) ReadSnapshot(context.Context, string, string, int64, int) ([]byte, bool, error) {
	return nil, false, catalog.ErrNotFound
}

func (emptyNativeCatalog) CloseSnapshot(context.Context, string, string) error {
	return catalog.ErrNotFound
}

func (emptyNativeCatalog) OpenWrite(context.Context, string, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision) (NativeHandle, error) {
	return NativeHandle{}, catalog.ErrNotFound
}

func (emptyNativeCatalog) ReadWrite(context.Context, string, string, int64, int) ([]byte, bool, error) {
	return nil, false, catalog.ErrNotFound
}

func (emptyNativeCatalog) Write(context.Context, string, string, int64, []byte) (int, error) {
	return 0, catalog.ErrNotFound
}

func (emptyNativeCatalog) Truncate(context.Context, string, string, int64) error {
	return catalog.ErrNotFound
}

func (emptyNativeCatalog) Sync(context.Context, string, string) error {
	return catalog.ErrNotFound
}

func (emptyNativeCatalog) CommitWrite(context.Context, string, string) (catalog.Object, catalog.MutationID, error) {
	return catalog.Object{}, catalog.MutationID{}, catalog.ErrNotFound
}

func (emptyNativeCatalog) AbortWrite(context.Context, string, string) error {
	return catalog.ErrNotFound
}

func (emptyNativeCatalog) CloseSession(context.Context, string) error { return nil }

var _ NativeCatalog = emptyNativeCatalog{}

func TestNativeSessionCloseSettlesCatalogBeforePinsAndRebind(t *testing.T) {
	catalogFailure := errors.New("injected catalog session settlement")
	pinFailure := errors.New("injected pin release")
	store := &blockingCloseNativeCatalog{
		started: make(chan struct{}),
		settle:  make(chan struct{}),
		result:  catalogFailure,
	}
	var registry nativeSessionRegistry
	first := new(wire.AcceptedSession)
	state, err := registry.bind(first)
	if err != nil {
		t.Fatalf("bind first: %v", err)
	}
	var released atomic.Bool
	var releaseCalls atomic.Int64
	if err := state.add("pin", NativePin{Release: func() error {
		releaseCalls.Add(1)
		released.Store(true)
		return pinFailure
	}}); err != nil {
		t.Fatalf("add pin: %v", err)
	}

	closed := make(chan error, 2)
	go func() { closed <- registry.close(first, state, store) }()
	<-store.started
	go func() { closed <- registry.close(first, state, store) }()

	if _, err := registry.bind(new(wire.AcceptedSession)); !errors.Is(err, catalog.ErrConflict) {
		t.Fatalf("bind while catalog close is unsettled = %v, want conflict", err)
	}
	if released.Load() {
		t.Fatal("route pin released before catalog session settled")
	}
	select {
	case err := <-closed:
		t.Fatalf("concurrent close returned before exact settlement: %v", err)
	default:
	}

	close(store.settle)
	for range 2 {
		err := <-closed
		if !errors.Is(err, catalogFailure) || !errors.Is(err, pinFailure) {
			t.Fatalf("cached close result = %v, want catalog and pin failures", err)
		}
	}
	if !released.Load() {
		t.Fatal("route pin was not released after catalog session settled")
	}
	if store.calls.Load() != 1 || releaseCalls.Load() != 1 {
		t.Fatalf("physical settlements = catalog %d, pin %d; want one each", store.calls.Load(), releaseCalls.Load())
	}
	if _, err := registry.bind(new(wire.AcceptedSession)); err != nil {
		t.Fatalf("bind after catalog session settled: %v", err)
	}
}

func TestEveryNativeOperationSharesOneExactConcurrentUnbindBarrier(t *testing.T) {
	operations := []mountproto.Operation{
		mountproto.OperationNativeReady,
		mountproto.OperationNativeRoutePage,
		mountproto.OperationNativePin,
		mountproto.OperationNativeRelease,
		mountproto.OperationNativeSnapshotOpen,
		mountproto.OperationNativeSnapshotRead,
		mountproto.OperationNativeSnapshotClose,
		mountproto.OperationNativeWriteOpen,
		mountproto.OperationNativeWriteRead,
		mountproto.OperationNativeWriteWrite,
		mountproto.OperationNativeWriteTruncate,
		mountproto.OperationNativeWriteSync,
		mountproto.OperationNativeWriteCommit,
		mountproto.OperationNativeWriteAbort,
	}
	catalogFailure := errors.New("injected catalog terminal result")
	pinFailure := errors.New("injected pin terminal result")
	store := &blockingCloseNativeCatalog{
		started: make(chan struct{}),
		settle:  make(chan struct{}),
		result:  catalogFailure,
	}
	var registry nativeSessionRegistry
	session := new(wire.AcceptedSession)
	state, err := registry.bind(session)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	var pinReleases atomic.Int64
	for _, token := range []string{"pin-a", "pin-b"} {
		if err := state.add(token, NativePin{Release: func() error {
			pinReleases.Add(1)
			return pinFailure
		}}); err != nil {
			t.Fatalf("add %s: %v", token, err)
		}
	}
	finish := make([]func(), len(operations))
	for index, operation := range operations {
		if err := state.begin(); err != nil {
			t.Fatalf("begin %s: %v", operation, err)
		}
		finish[index] = state.end
	}

	const unbindCallers = 8
	results := make(chan error, unbindCallers)
	for range unbindCallers {
		go func() { results <- registry.settle(session, state, store) }()
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		state.mu.Lock()
		closed := state.closed
		state.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("unbind did not close native admission")
		}
		time.Sleep(time.Millisecond)
	}
	if err := state.begin(); !errors.Is(err, errNativeSession) {
		t.Fatalf("late operation admission = %v, want native session closed", err)
	}

	var canceled sync.WaitGroup
	canceled.Add(len(finish) / 2)
	for index := 0; index < len(finish)/2; index++ {
		go func(done func()) {
			defer canceled.Done()
			done()
		}(finish[index])
	}
	canceled.Wait()
	select {
	case <-store.started:
		t.Fatal("catalog settlement began while admitted operations remained")
	default:
	}
	for index := len(finish) / 2; index < len(finish); index++ {
		finish[index]()
	}
	<-store.started
	state.mu.Lock()
	active := state.active
	state.mu.Unlock()
	if active != 0 || pinReleases.Load() != 0 {
		t.Fatalf("pre-catalog settlement active=%d pin releases=%d, want zero and zero", active, pinReleases.Load())
	}

	close(store.settle)
	var first error
	for range unbindCallers {
		result := <-results
		if !errors.Is(result, catalogFailure) || !errors.Is(result, pinFailure) {
			t.Fatalf("unbind result = %v, want catalog and pin failures", result)
		}
		if first == nil {
			first = result
		}
	}
	if store.calls.Load() != 1 || pinReleases.Load() != 2 {
		t.Fatalf("physical settlements = catalog %d pins %d, want one and two",
			store.calls.Load(), pinReleases.Load())
	}
	if result := registry.close(session, state, store); result != first {
		t.Fatalf("wire-close replay = %v, want cached terminal result %v", result, first)
	}
	if _, err := registry.bind(new(wire.AcceptedSession)); err != nil {
		t.Fatalf("rebind after exact settlement: %v", err)
	}
}

type blockingCloseNativeCatalog struct {
	emptyNativeCatalog
	started chan struct{}
	settle  chan struct{}
	result  error
	calls   atomic.Int64

	startedOnce sync.Once
}

func (s *blockingCloseNativeCatalog) CloseSession(context.Context, string) error {
	s.calls.Add(1)
	s.startedOnce.Do(func() { close(s.started) })
	<-s.settle
	return s.result
}
