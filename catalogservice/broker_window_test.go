package catalogservice

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalogproto"
)

// windowBroker is the runtime half of the poll tests: it hands each bind a
// scripted command session and exposes the drain signal the tests fire.
type windowBroker struct {
	draining chan struct{}
	sessions chan *scriptedBrokerSession
}

func newWindowBroker() *windowBroker {
	return &windowBroker{draining: make(chan struct{}), sessions: make(chan *scriptedBrokerSession, 4)}
}

func (b *windowBroker) Draining() <-chan struct{} { return b.draining }

// OpenBroker refuses once the drain begins, exactly as the runtime's closed
// check does, so a rebind racing the drain terminates instead of standing up
// a lane the drain immediately tears down.
func (b *windowBroker) OpenBroker(context.Context, Identity, string) (BrokerSession, error) {
	select {
	case <-b.draining:
		return nil, &CodedError{Code: catalogproto.ErrorCodeUnavailable, Message: "catalog service: broker runtime draining"}
	default:
	}
	session := &scriptedBrokerSession{
		commands: make(chan catalogproto.BrokerCommand),
		done:     make(chan struct{}),
	}
	b.sessions <- session
	return session, nil
}

// authorizeCounter closes arrived when the target-th request has authorized:
// the positive signal that a parked poll has entered the server, replacing a
// timing sleep that flakes under load.
type authorizeCounter struct {
	inner   Authorizer
	target  int
	arrived chan struct{}

	mu    sync.Mutex
	calls int
}

func newAuthorizeCounter(inner Authorizer, target int) *authorizeCounter {
	return &authorizeCounter{inner: inner, target: target, arrived: make(chan struct{})}
}

func (a *authorizeCounter) Authorize(ctx context.Context, identity Identity, operation catalogproto.Operation, route Route) (Authorization, error) {
	authorization, err := a.inner.Authorize(ctx, identity, operation, route)
	a.mu.Lock()
	a.calls++
	if a.calls == a.target {
		close(a.arrived)
	}
	a.mu.Unlock()
	return authorization, err
}

func awaitArrival(t *testing.T, arrived <-chan struct{}) {
	t.Helper()
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("parked poll never reached the server")
	}
}

type scriptedBrokerSession struct {
	commands chan catalogproto.BrokerCommand

	mu        sync.Mutex
	accepted  []uint64
	closeErr  error
	closeOnce sync.Once
	done      chan struct{}
}

func (s *scriptedBrokerSession) Commands() <-chan catalogproto.BrokerCommand { return s.commands }

func (s *scriptedBrokerSession) Done() <-chan struct{} { return s.done }

func (s *scriptedBrokerSession) AcceptResult(_ context.Context, result catalogproto.BrokerResult) error {
	s.mu.Lock()
	s.accepted = append(s.accepted, result.CommandID)
	s.mu.Unlock()
	return nil
}

func (s *scriptedBrokerSession) Close(err error) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closeErr = err
		s.mu.Unlock()
		close(s.done)
	})
}

func (s *scriptedBrokerSession) closed() (error, bool) {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closeErr, true
	default:
		return nil, false
	}
}

func listDomainsCommand(id uint64) catalogproto.BrokerCommand {
	return catalogproto.BrokerCommand{
		Protocol: catalogproto.Version, CommandID: id, Kind: catalogproto.BrokerCommandKindListDomains,
	}
}

func listDomainsResult(id uint64) catalogproto.BrokerResult {
	return catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: id, Kind: catalogproto.BrokerCommandKindListDomains,
		Domains: observedBrokerDomainPage([]catalogproto.RegisteredDomain{}),
	}
}

func brokerPoll(t *testing.T, client *Client, instance *catalogproto.BrokerInstanceID, cursor uint64, waitMillis uint32) (catalogproto.BrokerPollResponse, error) {
	t.Helper()
	var response catalogproto.BrokerPollResponse
	err := client.unary(t.Context(), catalogproto.OperationBrokerPoll, "", catalogproto.BrokerPollRequest{
		Protocol: catalogproto.Version, Instance: instance, Cursor: cursor, WaitMillis: waitMillis,
	}, &response)
	return response, err
}

func postBrokerResult(t *testing.T, client *Client, instance catalogproto.BrokerInstanceID, result catalogproto.BrokerResult) (catalogproto.PostBrokerResultResponse, error) {
	t.Helper()
	var response catalogproto.PostBrokerResultResponse
	err := client.unary(t.Context(), catalogproto.OperationBrokerResult, "", catalogproto.PostBrokerResultRequest{
		Protocol: catalogproto.Version, Instance: instance, Result: result,
	}, &response)
	return response, err
}

func bindBrokerLane(t *testing.T, client *Client, broker *windowBroker) (catalogproto.BrokerInstanceID, *scriptedBrokerSession) {
	t.Helper()
	bind, err := brokerPoll(t, client, nil, 0, 0)
	if err != nil || bind.Instance == nil {
		t.Fatalf("bind poll = %+v, %v", bind, err)
	}
	select {
	case session := <-broker.sessions:
		return *bind.Instance, session
	case <-time.After(time.Second):
		t.Fatal("bind did not open a runtime broker session")
		return "", nil
	}
}

// collectBrokerCommands polls forward from cursor until it has gathered count
// command ids, proving delivery pages stay ordered and correlated by id.
func collectBrokerCommands(t *testing.T, client *Client, instance catalogproto.BrokerInstanceID, cursor uint64, count int) []uint64 {
	t.Helper()
	var ids []uint64
	deadline := time.Now().Add(5 * time.Second)
	for len(ids) < count {
		if time.Now().After(deadline) {
			t.Fatalf("collected %d of %d commands before timeout", len(ids), count)
		}
		// A cursor-0 poll is the bind form by protocol shape; on the bound
		// session it must return the same live instance, not a replacement.
		ref := &instance
		if cursor == 0 {
			ref = nil
		}
		response, err := brokerPoll(t, client, ref, cursor, 500)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		if cursor == 0 && (response.Instance == nil || *response.Instance != instance) {
			t.Fatalf("same-session rebind moved the instance: %+v", response.Instance)
		}
		prior := cursor
		for _, command := range response.Commands {
			if command.CommandID <= prior {
				t.Fatalf("command id %d is not past cursor %d", command.CommandID, prior)
			}
			prior = command.CommandID
			ids = append(ids, command.CommandID)
		}
		if len(response.Commands) > 0 {
			cursor = response.NextCursor
		}
	}
	return ids
}

// TestBrokerPollWindowBoundsProducerWithoutDropping holds the first window
// parity property: outstanding commands stay bounded by the protocol's
// 32-command window, the producer blocks rather than drops at the bound, and
// one matched result frees exactly one slot.
func TestBrokerPollWindowBoundsProducerWithoutDropping(t *testing.T) {
	broker := newWindowBroker()
	_, d := startConfiguredCatalogServer(t, newFakeReader(1), &fakeMutations{}, catalogServerConfig{broker: broker})
	client := newCatalogClient(t, d)
	instance, session := bindBrokerLane(t, client, broker)

	bound := int(catalogproto.MaxOutstandingBrokerCommands)
	produced := make(chan uint64, bound+2)
	go func() {
		for id := uint64(1); id <= uint64(bound)+2; id++ {
			select {
			case session.commands <- listDomainsCommand(id):
			case <-t.Context().Done():
				return
			}
			produced <- id
		}
	}()

	ids := collectBrokerCommands(t, client, instance, 0, bound)
	for index, id := range ids {
		if id != uint64(index+1) {
			t.Fatalf("delivered ids = %v", ids)
		}
	}
	for id := uint64(1); id <= uint64(bound); id++ {
		select {
		case got := <-produced:
			if got != id {
				t.Fatalf("produced command = %d, want %d", got, id)
			}
		case <-time.After(time.Second):
			t.Fatalf("producer stalled before window at %d", id)
		}
	}
	select {
	case id := <-produced:
		t.Fatalf("producer exceeded outstanding window with command %d", id)
	case <-time.After(50 * time.Millisecond):
	}
	if response, err := brokerPoll(t, client, &instance, uint64(bound), 50); err != nil || len(response.Commands) != 0 {
		t.Fatalf("window-full poll = %+v, %v", response, err)
	}

	if response, err := postBrokerResult(t, client, instance, listDomainsResult(1)); err != nil || response.Code != catalogproto.ErrorCodeOk {
		t.Fatalf("result post = %+v, %v", response, err)
	}
	select {
	case id := <-produced:
		if id != uint64(bound)+1 {
			t.Fatalf("resumed command = %d", id)
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not resume after matched result")
	}
	ids = collectBrokerCommands(t, client, instance, uint64(bound), 1)
	if ids[0] != uint64(bound)+1 {
		t.Fatalf("post-result delivery = %v", ids)
	}
	session.mu.Lock()
	accepted := append([]uint64(nil), session.accepted...)
	session.mu.Unlock()
	if len(accepted) != 1 || accepted[0] != 1 {
		t.Fatalf("accepted results = %v", accepted)
	}
}

// TestBrokerResultsCorrelateOutOfOrder holds the second parity property:
// results correlate by command id, so out-of-order completion is native.
func TestBrokerResultsCorrelateOutOfOrder(t *testing.T) {
	broker := newWindowBroker()
	_, d := startConfiguredCatalogServer(t, newFakeReader(1), &fakeMutations{}, catalogServerConfig{broker: broker})
	client := newCatalogClient(t, d)
	instance, session := bindBrokerLane(t, client, broker)
	for id := uint64(1); id <= 3; id++ {
		session.commands <- listDomainsCommand(id)
	}
	if ids := collectBrokerCommands(t, client, instance, 0, 3); ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("delivered ids = %v", ids)
	}
	for _, id := range []uint64{3, 1, 2} {
		if response, err := postBrokerResult(t, client, instance, listDomainsResult(id)); err != nil || response.Code != catalogproto.ErrorCodeOk {
			t.Fatalf("result %d = %+v, %v", id, response, err)
		}
	}
	session.mu.Lock()
	accepted := append([]uint64(nil), session.accepted...)
	session.mu.Unlock()
	if len(accepted) != 3 || accepted[0] != 3 || accepted[1] != 1 || accepted[2] != 2 {
		t.Fatalf("accepted order = %v", accepted)
	}
	if response, err := brokerPoll(t, client, &instance, 3, 50); err != nil || len(response.Commands) != 0 {
		t.Fatalf("settled lane poll = %+v, %v", response, err)
	}
	session.commands <- listDomainsCommand(4)
	if ids := collectBrokerCommands(t, client, instance, 3, 1); ids[0] != 4 {
		t.Fatalf("delivered ids = %v", ids)
	}
	redelivered, err := brokerPoll(t, client, &instance, 0, 50)
	if err != nil || len(redelivered.Commands) != 1 || redelivered.Commands[0].CommandID != 4 {
		t.Fatalf("instance-held cursor-0 redelivery = %+v, %v", redelivered, err)
	}
	if _, err := postBrokerResult(t, client, instance, listDomainsResult(9)); err == nil {
		t.Fatal("unmatched result was accepted")
	}
	closeErr, closed := session.closed()
	if !closed || closeErr == nil {
		t.Fatalf("unmatched result did not poison the lane: %v, %t", closeErr, closed)
	}
}

// TestBrokerCommandIDCannotBeReusedAfterSettlement is the successor of the
// withdrawn test of the same name: a settled command id re-sent by the runtime
// session poisons the lane with integrity, as does the reserved ^uint64(0)
// sentinel.
func TestBrokerCommandIDCannotBeReusedAfterSettlement(t *testing.T) {
	broker := newWindowBroker()
	_, d := startConfiguredCatalogServer(t, newFakeReader(1), &fakeMutations{}, catalogServerConfig{broker: broker})
	client := newCatalogClient(t, d)
	instance, session := bindBrokerLane(t, client, broker)
	session.commands <- listDomainsCommand(1)
	if ids := collectBrokerCommands(t, client, instance, 0, 1); ids[0] != 1 {
		t.Fatalf("delivered ids = %v", ids)
	}
	if response, err := postBrokerResult(t, client, instance, listDomainsResult(1)); err != nil || response.Code != catalogproto.ErrorCodeOk {
		t.Fatalf("result post = %+v, %v", response, err)
	}
	session.commands <- listDomainsCommand(1)
	awaitIntegrityClose(t, session)
	if _, err := brokerPoll(t, client, &instance, 1, 50); err == nil {
		t.Fatal("poisoned lane still served polls")
	}

	rebind, err := brokerPoll(t, client, nil, 0, 0)
	if err != nil || rebind.Instance == nil || *rebind.Instance == instance {
		t.Fatalf("post-poison rebind = %+v, %v", rebind, err)
	}
	var sentinelSession *scriptedBrokerSession
	select {
	case sentinelSession = <-broker.sessions:
	case <-time.After(time.Second):
		t.Fatal("rebind did not open a runtime broker session")
	}
	sentinelSession.commands <- listDomainsCommand(^uint64(0))
	awaitIntegrityClose(t, sentinelSession)
	if _, err := brokerPoll(t, client, rebind.Instance, 1, 50); err == nil {
		t.Fatal("sentinel-poisoned lane still served polls")
	}
}

// awaitIntegrityClose proves the runtime session settled with an integrity
// cause — the deterministic half of the poison; a racing poll may see either
// the terminal failure or the post-cleanup stale verdict.
func awaitIntegrityClose(t *testing.T, session *scriptedBrokerSession) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if closeErr, closed := session.closed(); closed {
			var coded *CodedError
			if !errors.As(closeErr, &coded) || coded.Code != catalogproto.ErrorCodeIntegrity {
				t.Fatalf("lane settled with %v, want integrity", closeErr)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("lane did not settle")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestBrokerReplacementSettlesIncumbent holds the third parity property: a new
// broker session binding the principal settles the incumbent's pending table,
// fenced by the instance binding.
func TestBrokerReplacementSettlesIncumbent(t *testing.T) {
	broker := newWindowBroker()
	_, d := startConfiguredCatalogServer(t, newFakeReader(1), &fakeMutations{}, catalogServerConfig{broker: broker})
	first := newCatalogClient(t, d)
	instance, session := bindBrokerLane(t, first, broker)
	session.commands <- listDomainsCommand(1)
	if ids := collectBrokerCommands(t, first, instance, 0, 1); ids[0] != 1 {
		t.Fatalf("delivered ids = %v", ids)
	}

	parked := make(chan error, 1)
	go func() {
		_, err := brokerPoll(t, first, &instance, 1, catalogproto.MaxPollWaitMillis)
		parked <- err
	}()
	time.Sleep(50 * time.Millisecond)

	second := newCatalogClient(t, d)
	replacement, err := brokerPoll(t, second, nil, 0, 0)
	if err != nil || replacement.Instance == nil {
		t.Fatalf("replacement bind = %+v, %v", replacement, err)
	}
	if *replacement.Instance == instance {
		t.Fatal("replacement reused the incumbent instance")
	}
	closeErr, closed := session.closed()
	if !closed || closeErr == nil {
		t.Fatalf("incumbent session did not settle with a cause: %v, %t", closeErr, closed)
	}
	select {
	case err := <-parked:
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.Code != catalogproto.ErrorCodeUnavailable {
			t.Fatalf("parked incumbent poll = %v, want unavailable", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement left the incumbent's poll parked")
	}
	if _, err := brokerPoll(t, first, &instance, 1, 0); err == nil {
		t.Fatal("stale instance poll was admitted")
	}
	if _, err := postBrokerResult(t, first, instance, listDomainsResult(1)); err == nil {
		t.Fatal("stale instance result was admitted")
	}
	select {
	case <-broker.sessions:
	case <-time.After(time.Second):
		t.Fatal("replacement did not open a runtime broker session")
	}
}

// TestBrokerDisconnectFencesUnresultedCommands holds the fourth parity
// property: the polling session's disconnect settles the lane with a cause,
// marking every un-resulted command unknown-delivery on the runtime side.
func TestBrokerDisconnectFencesUnresultedCommands(t *testing.T) {
	broker := newWindowBroker()
	_, d := startConfiguredCatalogServer(t, newFakeReader(1), &fakeMutations{}, catalogServerConfig{broker: broker})

	client, err := daemonkit.Open(d)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	business := client.Business()
	catalogClient, err := NewClientOn(business)
	if err != nil {
		t.Fatalf("NewClientOn: %v", err)
	}
	instance, session := bindBrokerLane(t, catalogClient, broker)
	session.commands <- listDomainsCommand(1)
	session.commands <- listDomainsCommand(2)
	if ids := collectBrokerCommands(t, catalogClient, instance, 0, 2); ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("delivered ids = %v", ids)
	}
	if response, err := postBrokerResult(t, catalogClient, instance, listDomainsResult(1)); err != nil || response.Code != catalogproto.ErrorCodeOk {
		t.Fatalf("result post = %+v, %v", response, err)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := business.Close(closeCtx); err != nil {
		t.Fatalf("close business lane: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if closeErr, closed := session.closed(); closed {
			if closeErr == nil {
				t.Fatal("disconnect settled the lane without a cause")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("disconnect did not settle the broker lane")
		}
		time.Sleep(5 * time.Millisecond)
	}
	session.mu.Lock()
	accepted := append([]uint64(nil), session.accepted...)
	session.mu.Unlock()
	if len(accepted) != 1 || accepted[0] != 1 {
		t.Fatalf("accepted results after disconnect = %v", accepted)
	}

	replacementClient := newCatalogClient(t, d)
	replacement, err := brokerPoll(t, replacementClient, nil, 0, 0)
	if err != nil || replacement.Instance == nil || *replacement.Instance == instance {
		t.Fatalf("post-disconnect bind = %+v, %v", replacement, err)
	}
}

// TestBrokerPollReleasesPromptlyAtDrain holds the drain-release property from
// cookiesync's production finding: daemonkit settles in-flight requests before
// the product drain, so a parked poll must release the moment the drain
// begins instead of stalling shutdown for its full wait.
func TestBrokerPollReleasesPromptlyAtDrain(t *testing.T) {
	broker := newWindowBroker()
	authorizer := newAuthorizeCounter(fakeAuthorizer{}, 2)
	_, d := startConfiguredCatalogServer(t, newFakeReader(1), &fakeMutations{}, catalogServerConfig{
		broker: broker, authorizer: authorizer,
	})
	client := newCatalogClient(t, d)
	_, _ = bindBrokerLane(t, client, broker)

	parked := make(chan error, 1)
	go func() {
		_, err := brokerPoll(t, client, nil, 0, catalogproto.MaxPollWaitMillis)
		parked <- err
	}()
	awaitArrival(t, authorizer.arrived)
	drained := time.Now()
	close(broker.draining)
	select {
	case err := <-parked:
		if elapsed := time.Since(drained); elapsed > 2*time.Second {
			t.Fatalf("parked poll released after %v, want prompt release", elapsed)
		}
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.Code != catalogproto.ErrorCodeUnavailable {
			t.Fatalf("drained poll = %v, want unavailable", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not release the parked poll")
	}
}

// TestActivationPollReleasesPromptlyAtDrain extends the drain-release
// property to the activation lane's parked polls.
func TestActivationPollReleasesPromptlyAtDrain(t *testing.T) {
	broker := newWindowBroker()
	authorizer := newAuthorizeCounter(fakeAuthorizer{fileProvider: true}, 1)
	_, d := startConfiguredCatalogServer(t, newFakeReader(1), &fakeMutations{}, catalogServerConfig{
		broker: broker, authorizer: authorizer,
	})
	client := newCatalogClient(t, d)
	parked := make(chan error, 1)
	go func() {
		_, err := client.PollActivations(t.Context(), testTenant, catalogproto.PollActivationsRequest{
			Protocol: catalogproto.Version, DomainID: testBoundDomain(), Generation: 7,
			Cursor: 0, WaitMillis: catalogproto.MaxPollWaitMillis, Limit: 16,
		})
		parked <- err
	}()
	awaitArrival(t, authorizer.arrived)
	drained := time.Now()
	close(broker.draining)
	select {
	case err := <-parked:
		if elapsed := time.Since(drained); elapsed > 2*time.Second {
			t.Fatalf("parked activation poll released after %v, want prompt release", elapsed)
		}
		if err != nil {
			t.Fatalf("drained activation poll = %v, want empty page", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not release the parked activation poll")
	}
}

// sessionCapturingAuthorizer records the authenticated session so a test can
// aim NotifyActivation at it, the way a production caller holds the session
// from its own request.
type sessionCapturingAuthorizer struct {
	mu   sync.Mutex
	last daemonkit.Session
}

func (a *sessionCapturingAuthorizer) Authorize(ctx context.Context, identity Identity, operation catalogproto.Operation, route Route) (Authorization, error) {
	a.mu.Lock()
	a.last = identity.Session
	a.mu.Unlock()
	return fakeAuthorizer{fileProvider: true}.Authorize(ctx, identity, operation, route)
}

func (a *sessionCapturingAuthorizer) session() (daemonkit.Session, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.last, a.last != (daemonkit.Session{})
}

func TestNotifyActivationWakesParkedPoll(t *testing.T) {
	authorizer := &sessionCapturingAuthorizer{}
	server, d := startConfiguredCatalogServer(t, newFakeReader(1), &fakeMutations{}, catalogServerConfig{authorizer: authorizer})
	client := newCatalogClient(t, d)

	parked := make(chan catalogproto.PollActivationsResponse, 1)
	pollErr := make(chan error, 1)
	go func() {
		response, err := client.PollActivations(t.Context(), testTenant, catalogproto.PollActivationsRequest{
			Protocol: catalogproto.Version, DomainID: testBoundDomain(), Generation: 7,
			Cursor: 0, WaitMillis: 10_000, Limit: 16,
		})
		parked <- response
		pollErr <- err
	}()

	var session daemonkit.Session
	deadline := time.Now().Add(5 * time.Second)
	for {
		if captured, ok := authorizer.session(); ok {
			session = captured
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("parked poll never authorized")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	notification := testActivationNotification(5)
	if err := server.NotifyActivation(t.Context(), session, notification); err != nil {
		t.Fatalf("NotifyActivation: %v", err)
	}
	select {
	case response := <-parked:
		if err := <-pollErr; err != nil {
			t.Fatalf("woken poll: %v", err)
		}
		if len(response.Notifications) != 1 || response.NextCursor != 5 ||
			response.Notifications[0].ActivationRevision != 5 {
			t.Fatalf("woken poll = %+v", response)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("NotifyActivation left the poll parked")
	}

	replay, err := client.PollActivations(t.Context(), testTenant, catalogproto.PollActivationsRequest{
		Protocol: catalogproto.Version, DomainID: testBoundDomain(), Generation: 7,
		Cursor: 0, WaitMillis: 0, Limit: 16,
	})
	if err != nil || len(replay.Notifications) != 1 {
		t.Fatalf("un-acknowledged replay = %+v, %v", replay, err)
	}
	drained, err := client.PollActivations(t.Context(), testTenant, catalogproto.PollActivationsRequest{
		Protocol: catalogproto.Version, DomainID: testBoundDomain(), Generation: 7,
		Cursor: 5, WaitMillis: 0, Limit: 16,
	})
	if err != nil || len(drained.Notifications) != 0 {
		t.Fatalf("advanced-cursor poll = %+v, %v", drained, err)
	}
}

func testActivationNotification(revision uint64) catalogproto.ActivationNotification {
	parent := catalogproto.ObjectID("01010101010101010101010101010101")
	return catalogproto.ActivationNotification{
		Protocol:           catalogproto.Version,
		ActivationChangeID: catalogproto.ActivationChangeID("11111111111111111111111111111111"),
		TenantID:           testTenant, DomainID: testBoundDomain(), Generation: 7,
		ActivationRevision: revision, CatalogHead: 12,
		HeadDigest:          strings.Repeat("a", 64),
		ProviderFingerprint: strings.Repeat("b", 64),
		Causes: []catalogproto.ActivationSourceCause{{
			PublicationID:      catalogproto.OperationID("22222222222222222222222222222222"),
			ChangeID:           catalogproto.ChangeID("33333333333333333333333333333333"),
			SourceRevision:     8,
			OperationID:        catalogproto.OperationID("44444444444444444444444444444444"),
			Cause:              catalogproto.ActivationCauseProviderMutation,
			AffectedKeysDigest: strings.Repeat("c", 64),
		}},
		TargetCount: 1, TargetDigest: strings.Repeat("d", 64),
		Targets: []catalogproto.SignalTarget{{Kind: catalogproto.SignalTargetKindContainer, ParentID: &parent}},
	}
}
