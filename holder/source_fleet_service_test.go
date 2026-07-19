package holder

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
)

type fixedDesiredFleetStore struct {
	state     catalog.DesiredSourceAuthorityFleetState
	err       error
	published chan struct{}
}

func (s *fixedDesiredFleetStore) PublishDesiredSourceFleet(
	context.Context,
	catalog.PublishDesiredSourceFleetRequest,
) (catalog.DesiredSourceAuthorityFleetState, error) {
	close(s.published)
	return s.state, s.err
}

func (s *fixedDesiredFleetStore) DesiredSourceFleetPage(
	context.Context,
	catalog.DesiredSourceFleetPageRequest,
) (catalog.DesiredSourceFleetPage, error) {
	return catalog.DesiredSourceFleetPage{State: s.state}, s.err
}

type replayDesiredFleetStore struct {
	state catalog.DesiredSourceAuthorityFleetState
	lost  error
	calls int
}

func (s *replayDesiredFleetStore) PublishDesiredSourceFleet(
	context.Context,
	catalog.PublishDesiredSourceFleetRequest,
) (catalog.DesiredSourceAuthorityFleetState, error) {
	s.calls++
	if s.calls == 1 {
		return catalog.DesiredSourceAuthorityFleetState{}, s.lost
	}
	return s.state, nil
}

func (*replayDesiredFleetStore) DesiredSourceFleetPage(
	context.Context,
	catalog.DesiredSourceFleetPageRequest,
) (catalog.DesiredSourceFleetPage, error) {
	return catalog.DesiredSourceFleetPage{}, nil
}

func TestSourceFleetServiceLostResponseReplayReconfirmsApplication(t *testing.T) {
	topology := desiredEmptyTopology(t, "product", 1)
	desired := *topology.Head.Fleet
	controller := &topologyController{
		current: topology, wake: make(chan struct{}), done: make(chan struct{}),
	}
	lost := errors.New("lost response")
	store := &replayDesiredFleetStore{state: desired, lost: lost}
	service := sourceFleetService{store: store, topology: controller, owner: "product"}
	request := catalog.PublishDesiredSourceFleetRequest{Owner: "product"}
	if _, err := service.PublishDesiredSourceFleet(t.Context(), request); !errors.Is(err, lost) {
		t.Fatalf("first publish = %v, want lost response", err)
	}
	state, err := service.PublishDesiredSourceFleet(t.Context(), request)
	if err != nil || state != desired || store.calls != 2 {
		t.Fatalf("replayed publish = %+v, %v, calls %d", state, err, store.calls)
	}
}

func TestSourceFleetServiceRejectsForeignOwnerBeforeCatalogAccess(t *testing.T) {
	store := &fixedDesiredFleetStore{published: make(chan struct{})}
	service := sourceFleetService{
		store: store, topology: &topologyController{}, owner: "product",
	}
	if _, err := service.PublishDesiredSourceFleet(t.Context(), catalog.PublishDesiredSourceFleetRequest{
		Owner: "foreign",
	}); err == nil {
		t.Fatal("foreign publish succeeded")
	}
	if _, err := service.DesiredSourceFleetPage(t.Context(), catalog.DesiredSourceFleetPageRequest{
		Owner: "foreign",
	}); err == nil {
		t.Fatal("foreign read succeeded")
	}
	select {
	case <-store.published:
		t.Fatal("foreign request reached the catalog")
	default:
	}
}

func TestAwaitSourceFleetAppliedRejectsFailureSupersedeCancellationAndShutdown(t *testing.T) {
	desiredTopology := desiredEmptyTopology(t, "product", 1)
	desired := *desiredTopology.Head.Fleet

	failed := &topologyController{
		current: desiredTopologyForOwner("product"), wake: make(chan struct{}), done: make(chan struct{}),
		err: errors.New("controller failed"), stopped: true,
	}
	if err := failed.AwaitSourceFleetApplied(t.Context(), desired); err == nil {
		t.Fatal("controller failure was accepted")
	}

	superseding := desiredEmptyTopology(t, "product", 2)
	superseded := &topologyController{
		current: superseding, wake: make(chan struct{}), done: make(chan struct{}),
	}
	if err := superseded.AwaitSourceFleetApplied(t.Context(), desired); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("superseded application = %v, want generation mismatch", err)
	}

	canceled := &topologyController{
		current: desiredTopologyForOwner("product"), wake: make(chan struct{}), done: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := canceled.AwaitSourceFleetApplied(ctx, desired); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait = %v, want canceled", err)
	}

	stopped := &topologyController{
		current: desiredTopologyForOwner("product"), wake: make(chan struct{}), done: make(chan struct{}),
		stopped: true,
	}
	if err := stopped.AwaitSourceFleetApplied(t.Context(), desired); err == nil {
		t.Fatal("stopped controller was accepted")
	}
}

func TestSourceFleetServiceReturnsOnlyAfterExactAppliedFleet(t *testing.T) {
	desiredTopology := desiredEmptyTopology(t, "product", 1)
	desired := *desiredTopology.Head.Fleet
	controller := &topologyController{
		current: desiredTopologyForOwner("product"), wake: make(chan struct{}), done: make(chan struct{}),
	}
	store := &fixedDesiredFleetStore{state: desired, published: make(chan struct{})}
	service := sourceFleetService{store: store, topology: controller, owner: "product"}
	done := make(chan error, 1)
	go func() {
		_, err := service.PublishDesiredSourceFleet(t.Context(), catalog.PublishDesiredSourceFleetRequest{Owner: "product"})
		done <- err
	}()
	<-store.published
	select {
	case err := <-done:
		t.Fatalf("publish returned before application: %v", err)
	default:
	}
	controller.publishApplied(desiredTopology)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("publish did not return after exact application")
	}
}

func desiredTopologyForOwner(owner catalog.SourceAuthorityFleetOwnerID) desiredTopology {
	return desiredTopology{Head: catalog.TopologyHeadState{Owner: owner}}
}
