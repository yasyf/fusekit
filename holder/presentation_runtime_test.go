package holder

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
)

func TestNativePresentationPreparerWaitsForBackendBeforeRouteProof(t *testing.T) {
	operation := &presentationTestOperation{}
	manager, err := newPresentationManager(
		t.Context(), time.Second, time.Second,
		&presentationTestFactory{operations: []presentationOperation{operation}}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	var routes atomic.Int64
	preparer := nativePresentationPreparer{
		presentations: manager,
		route: func(id catalog.TenantID, generation catalog.Generation) error {
			routes.Add(1)
			if id != "tenant" || generation != 7 {
				return errors.New("wrong route identity")
			}
			if operation.starts.Load() != 1 || operation.readies.Load() != 1 {
				return errors.New("route checked before backend readiness")
			}
			return nil
		},
	}
	if err := preparer.PrepareMountPresentation(t.Context(), "tenant", 7); err != nil {
		t.Fatal(err)
	}
	if routes.Load() != 1 {
		t.Fatalf("route proof calls = %d, want one", routes.Load())
	}
}

func TestOwnedPresentationOperationProvesAsynchronousSettlement(t *testing.T) {
	closeEntered := make(chan struct{})
	release := make(chan struct{})
	operation, err := newOwnedPresentationOperation(
		func(context.Context) error { return nil },
		func() error { return nil },
		func() error {
			close(closeEntered)
			<-release
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := operation.stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-closeEntered
	deadline, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if err := operation.wait(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded wait = %v", err)
	}
	close(release)
	if err := operation.wait(t.Context()); err != nil {
		t.Fatalf("settled wait: %v", err)
	}
}

func TestPresentationManagerCloseCanFinishProofAfterBoundedWait(t *testing.T) {
	closeEntered := make(chan struct{})
	release := make(chan struct{})
	operation, err := newOwnedPresentationOperation(
		func(context.Context) error { return nil },
		func() error { return nil },
		func() error {
			close(closeEntered)
			<-release
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := newPresentationManager(
		t.Context(), time.Second, time.Millisecond,
		presentationOperationFactoryFunc(func() (presentationOperation, error) { return operation, nil }), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureNative(t.Context()); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- manager.Close(deadline) }()
	<-closeEntered
	if err := <-first; !errors.Is(err, errPresentationShutdownIncomplete) {
		t.Fatalf("bounded close = %v", err)
	}
	close(release)
	if err := manager.Close(t.Context()); err != nil {
		t.Fatalf("settled close: %v", err)
	}
}
