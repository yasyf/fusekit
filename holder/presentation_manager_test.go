package holder

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPresentationManagerStartsRequiredBackendsConcurrentlyAndJoinsInOrder(t *testing.T) {
	nativeEntered := make(chan struct{})
	brokerEntered := make(chan struct{})
	release := make(chan struct{})
	nativeErr := errors.New("native failed")
	brokerErr := errors.New("broker failed")
	native := &presentationTestOperation{readyEntered: nativeEntered, readyRelease: release, readyErr: nativeErr}
	broker := &presentationTestOperation{readyEntered: brokerEntered, readyRelease: release, readyErr: brokerErr}
	manager, err := newPresentationManager(
		t.Context(), time.Second, time.Second,
		&presentationTestFactory{operations: []presentationOperation{native}},
		&presentationTestFactory{operations: []presentationOperation{broker}},
	)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- manager.Ensure(t.Context(), true, true) }()
	for _, entered := range []chan struct{}{nativeEntered, brokerEntered} {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("both backends did not start concurrently")
		}
	}
	close(release)
	err = <-done
	if !errors.Is(err, nativeErr) || !errors.Is(err, brokerErr) {
		t.Fatalf("joined error = %v", err)
	}
	if strings.Index(err.Error(), nativeErr.Error()) > strings.Index(err.Error(), brokerErr.Error()) {
		t.Fatalf("joined error order = %q", err)
	}
}

func TestPresentationManagerCloseSettlesBothReadyBackends(t *testing.T) {
	native := &presentationTestOperation{}
	broker := &presentationTestOperation{}
	manager, err := newPresentationManager(
		t.Context(), time.Second, time.Second,
		&presentationTestFactory{operations: []presentationOperation{native}},
		&presentationTestFactory{operations: []presentationOperation{broker}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Ensure(t.Context(), true, true); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if native.stops.Load() != 1 || native.waits.Load() != 1 || broker.stops.Load() != 1 || broker.waits.Load() != 1 {
		t.Fatalf(
			"close settlement native=%d/%d broker=%d/%d",
			native.stops.Load(), native.waits.Load(), broker.stops.Load(), broker.waits.Load(),
		)
	}
}

func TestPresentationManagerCanceledLifetimeAllocatesNoBackend(t *testing.T) {
	lifetime, cancel := context.WithCancel(t.Context())
	cancel()
	native := &presentationTestFactory{operations: []presentationOperation{&presentationTestOperation{}}}
	broker := &presentationTestFactory{operations: []presentationOperation{&presentationTestOperation{}}}
	manager, err := newPresentationManager(lifetime, time.Second, time.Second, native, broker)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Ensure(t.Context(), true, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Ensure = %v", err)
	}
	if native.calls.Load() != 0 || broker.calls.Load() != 0 {
		t.Fatalf("factories called after cancellation = %d/%d", native.calls.Load(), broker.calls.Load())
	}
}
