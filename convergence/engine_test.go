package convergence

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
)

func TestEngineClaimsSendsAndAcknowledgesExactActivation(t *testing.T) {
	event := activationEvent(t, 1, 1)
	store := &fakeStore{pending: []causal.ActivationEvent{event}}
	notifier := &fakeNotifier{delivery: DeliverySent}
	clock := &fakeClock{now: time.Unix(100, 0)}
	engine := newEngineForTest(t, store, notifier, clock)
	if err := engine.Pump(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(notifier.events) != 1 || notifier.events[0].Key() != event.Key() {
		t.Fatalf("notifications = %+v", notifier.events)
	}
	if len(store.results) != 1 || store.results[0].Outcome != DeliverySent ||
		!store.results[0].AckDeadline.Equal(clock.now.Add(AckTimeout)) {
		t.Fatalf("delivery result = %+v", store.results)
	}
	ack := activationAck(event)
	if err := engine.Acknowledge(t.Context(), ack); err != nil {
		t.Fatal(err)
	}
	if len(store.acks) != 1 || store.acks[0] != ack {
		t.Fatalf("acks = %+v", store.acks)
	}
}

func TestEngineNeverReplaysAmbiguousDelivery(t *testing.T) {
	event := activationEvent(t, 1, 1)
	store := &fakeStore{pending: []causal.ActivationEvent{event}}
	notifyErr := errors.New("connection lost after write")
	notifier := &fakeNotifier{delivery: DeliverySent, err: notifyErr}
	engine := newEngineForTest(t, store, notifier, &fakeClock{now: time.Unix(100, 0)})
	if err := engine.Pump(t.Context()); !errors.Is(err, notifyErr) {
		t.Fatalf("Pump = %v", err)
	}
	if len(store.results) != 1 || store.results[0].Outcome != DeliveryUnknown {
		t.Fatalf("delivery result = %+v", store.results)
	}
	if err := engine.Pump(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(notifier.events) != 1 {
		t.Fatalf("notification count = %d, want 1", len(notifier.events))
	}
}

func TestEngineDefiniteNotSentReturnsActivationToPending(t *testing.T) {
	event := activationEvent(t, 1, 1)
	store := &fakeStore{pending: []causal.ActivationEvent{event}}
	notifier := &fakeNotifier{delivery: DeliveryNotSent}
	engine := newEngineForTest(t, store, notifier, &fakeClock{now: time.Unix(100, 0)})
	if err := engine.Pump(t.Context()); !errors.Is(err, ErrNotSent) {
		t.Fatalf("Pump = %v, want ErrNotSent", err)
	}
	if len(store.results) != 1 || store.results[0].Outcome != DeliveryNotSent || len(store.pending) != 1 {
		t.Fatalf("delivery result/pending = %+v / %d", store.results, len(store.pending))
	}
}

func TestEngineRecoversBeforeClaimingAndClosesAdmission(t *testing.T) {
	store := &fakeStore{}
	engine := newEngineForTest(t, store, &fakeNotifier{delivery: DeliverySent}, &fakeClock{now: time.Unix(100, 0)})
	if store.recoveries != 1 {
		t.Fatalf("recoveries = %d, want 1", store.recoveries)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Pump(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pump after close = %v", err)
	}
}

func TestEngineInvalidNotifierOutcomeIsAmbiguousAndFailsThePump(t *testing.T) {
	store := &fakeStore{pending: []causal.ActivationEvent{activationEvent(t, 1, 2)}}
	notifier := &fakeNotifier{}
	engine := newEngineForTest(t, store, notifier, &fakeClock{now: time.Unix(50, 0)})
	err := engine.Pump(t.Context())
	if err == nil || !strings.Contains(err.Error(), "invalid delivery outcome") {
		t.Fatalf("Pump() error = %v, want invalid delivery outcome", err)
	}
	if len(store.results) != 1 || store.results[0].Outcome != DeliveryUnknown ||
		store.results[0].Failure.Code != "invalid_delivery" {
		t.Fatalf("delivery results = %+v", store.results)
	}
}

func newEngineForTest(t *testing.T, store Store, notifier Notifier, clock Clock) *Engine {
	t.Helper()
	token := causal.OperationID{9}
	engine, err := New(t.Context(), Config{
		Store: store, Notifier: notifier, Clock: clock,
		RuntimeGeneration: "runtime-1", HolderOperation: causal.OperationID{8},
		NewClaimToken: func() (causal.OperationID, error) { return token, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func activationEvent(t *testing.T, activation, publication byte) causal.ActivationEvent {
	t.Helper()
	tenant := causal.TenantID("acct-07")
	head := sha256.Sum256([]byte{activation})
	publicationID := causal.OperationID{publication}
	id, err := causal.DeriveActivationChangeID(tenant, 1, uint64(activation), head, []causal.OperationID{publicationID})
	if err != nil {
		t.Fatal(err)
	}
	return causal.ActivationEvent{
		ActivationChangeID: id, TenantID: tenant, TenantGeneration: 1,
		ActivationRevision: causal.Revision(activation), PresentationID: "fp-acct-07",
		Backend: causal.BackendFileProvider, CatalogHead: causal.CatalogRevision(activation), HeadDigest: head,
		Causes: []causal.SourceCause{{
			PublicationID: publicationID, ChangeID: causal.ChangeID{publication},
			SourceRevision: causal.Revision(publication), OperationID: causal.OperationID{publication + 10},
			Cause: causal.CauseDaemonWrite, AffectedKeysDigest: sha256.Sum256([]byte{publication, 1}),
		}},
	}
}

func activationAck(event causal.ActivationEvent) causal.ActivationAck {
	return causal.ActivationAck{
		ActivationChangeID: event.ActivationChangeID, TenantID: event.TenantID,
		TenantGeneration: event.TenantGeneration, PresentationID: event.PresentationID, Backend: event.Backend,
		ObservedActivationRevision: event.ActivationRevision, ObservedCatalogHead: event.CatalogHead,
		ObservedHeadDigest: event.HeadDigest,
	}
}

type fakeStore struct {
	mu         sync.Mutex
	pending    []causal.ActivationEvent
	delivering map[causal.ActivationKey]DeliveryClaim
	results    []DeliveryResult
	acks       []causal.ActivationAck
	recoveries int
}

func (s *fakeStore) RecoverDeliveries(context.Context, string, time.Time) error {
	s.recoveries++
	return nil
}

func (s *fakeStore) ClaimDelivery(_ context.Context, request ClaimRequest) (*DeliveryClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) == 0 || len(s.delivering) >= MaxAwaiting {
		return nil, nil
	}
	if s.delivering == nil {
		s.delivering = make(map[causal.ActivationKey]DeliveryClaim)
	}
	event := s.pending[0]
	s.pending = s.pending[1:]
	claim := DeliveryClaim{Event: event, ClaimToken: request.ClaimToken, Attempt: 1}
	s.delivering[event.Key()] = claim
	return &claim, nil
}

func (s *fakeStore) RecordDelivery(_ context.Context, result DeliveryResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, result)
	claim := s.delivering[result.Key]
	if result.Outcome == DeliveryNotSent {
		delete(s.delivering, result.Key)
		s.pending = append(s.pending, claim.Event)
	}
	return nil
}

func (s *fakeStore) AcknowledgeDelivery(_ context.Context, ack causal.ActivationAck) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acks = append(s.acks, ack)
	delete(s.delivering, causal.ActivationKey{
		ActivationChangeID: ack.ActivationChangeID,
		PresentationID:     ack.PresentationID,
	})
	return nil
}

func (*fakeStore) QuarantineExpired(context.Context, time.Time) error { return nil }

type fakeNotifier struct {
	delivery Delivery
	err      error
	events   []causal.ActivationEvent
}

func (n *fakeNotifier) Notify(_ context.Context, event causal.ActivationEvent) (Delivery, error) {
	n.events = append(n.events, event)
	return n.delivery, n.err
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }
