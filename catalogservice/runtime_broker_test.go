package catalogservice

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/convergence"
	"github.com/yasyf/fusekit/transportproto"
)

const testBrokerExecutable = "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper"

func testRuntimeBrokerIdentity() BrokerIdentity {
	return BrokerIdentity{
		ProductBuild: "test-product-build", Executable: testBrokerExecutable,
		DesignatedRequirement:       `identifier "com.example.test-broker"`,
		EntitlementValidationDigest: [32]byte{1},
	}
}

func newTestRuntimeBroker(t *testing.T, store RuntimeBrokerStore) *RuntimeBroker {
	t.Helper()
	return newTestRuntimeBrokerWithOwner(t, store, &testBrokerProcessOwner{})
}

func newTestRuntimeBrokerWithOwner(
	t *testing.T,
	store RuntimeBrokerStore,
	owner *testBrokerProcessOwner,
) *RuntimeBroker {
	t.Helper()
	broker, err := NewRuntimeBroker(t.Context(), activationTargetBrokerStore{RuntimeBrokerStore: store}, testRuntimeBrokerIdentity(), "activation-test", owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	return broker
}

type activationTargetBrokerStore struct{ RuntimeBrokerStore }

func (s activationTargetBrokerStore) ActivationPresentationTarget(
	_ context.Context,
	key causal.ActivationKey,
) (catalog.TenantPresentationTarget, error) {
	return catalog.TenantPresentationTarget{
		PresentationID: key.PresentationID, Backend: causal.BackendFileProvider,
		ProviderFingerprint: [32]byte{1},
		SignalTargets:       []catalog.FileProviderSignalTarget{{WorkingSet: true}},
		SignalTargetCount:   1, SignalTargetDigest: [32]byte{2},
	}, nil
}

func closeTestRuntimeBroker(t *testing.T, broker *RuntimeBroker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := broker.Close(ctx); err != nil {
		t.Errorf("Close RuntimeBroker: %v", err)
	}
}

type testBrokerProcessOwner struct {
	mu            sync.Mutex
	retired       []catalog.BrokerProcessIdentity
	starts        int
	startCalled   chan struct{}
	retireStarted chan struct{}
	retireRelease <-chan struct{}
	retireErr     error
	retireOnce    sync.Once
}

type blockingBrokerRecoveryStore struct {
	RuntimeBrokerStore
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

type countingRuntimeBrokerStore struct {
	*catalog.Catalog
	domainLookups atomic.Int32
	desiredPages  atomic.Int32
}

func (s *countingRuntimeBrokerStore) FileProviderDomainForTenant(
	ctx context.Context,
	tenant catalog.TenantID,
) (catalog.FileProviderDomain, bool, error) {
	s.domainLookups.Add(1)
	return s.Catalog.FileProviderDomainForTenant(ctx, tenant)
}

func (s *countingRuntimeBrokerStore) PageFileProviderDomains(
	ctx context.Context,
	after catalog.TenantID,
	limit int,
) (catalog.FileProviderDomainPage, error) {
	s.desiredPages.Add(1)
	return s.Catalog.PageFileProviderDomains(ctx, after, limit)
}

func (s *blockingBrokerRecoveryStore) RecoverReapedBrokerCommandAttempts(
	ctx context.Context,
	identity catalog.BrokerProcessIdentity,
) error {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return s.RuntimeBrokerStore.RecoverReapedBrokerCommandAttempts(ctx, identity)
}

func (*testBrokerProcessOwner) BindBroker(_ context.Context, peer wire.Peer) (catalog.BrokerProcessIdentity, error) {
	return catalog.BrokerProcessIdentity{
		PID: peer.PID, StartTime: peer.StartTime, Boot: peer.Boot,
		Generation: fmt.Sprintf("generation-%d-%s", peer.PID, peer.StartTime),
	}, nil
}

func (o *testBrokerProcessOwner) RetireBroker(_ context.Context, identity catalog.BrokerProcessIdentity) error {
	o.mu.Lock()
	o.retired = append(o.retired, identity)
	retireErr := o.retireErr
	o.mu.Unlock()
	if o.retireStarted != nil {
		o.retireOnce.Do(func() { close(o.retireStarted) })
	}
	if o.retireRelease != nil {
		<-o.retireRelease
	}
	return retireErr
}

func (o *testBrokerProcessOwner) StartBroker(context.Context) error {
	o.mu.Lock()
	o.starts++
	o.mu.Unlock()
	if o.startCalled != nil {
		select {
		case o.startCalled <- struct{}{}:
		default:
		}
	}
	return nil
}

func brokerPeerIdentity() Identity {
	return brokerPeerIdentityAt(41, "test-start")
}

func brokerPeerIdentityAt(pid int, start string) Identity {
	return Identity{WireBuild: transportproto.WireBuild, Peer: wire.Peer{
		PID: pid, UID: 501, StartTime: start, Boot: "test-boot", Comm: "TestBroker", Executable: testBrokerExecutable,
		Audit: make([]byte, 32),
	}}
}

func TestRuntimeBrokerRejectsStartAndBindBeforeExplicitRecovery(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	owner := &testBrokerProcessOwner{}
	broker, err := NewRuntimeBroker(t.Context(), store, testRuntimeBrokerIdentity(), "activation-test", owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Start(t.Context()); err == nil {
		t.Fatal("Start accepted unrecovered durable broker state")
	}
	if _, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal"); err == nil {
		t.Fatal("OpenBroker accepted process before durable recovery")
	}
	owner.mu.Lock()
	starts := owner.starts
	owner.mu.Unlock()
	if starts != 0 {
		t.Fatalf("unrecovered broker launches = %d", starts)
	}
	if err := broker.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := broker.Recover(t.Context()); err != nil {
		t.Fatalf("exact recovery replay: %v", err)
	}
	closeTestRuntimeBroker(t, broker)
}

func TestRuntimeBrokerRejectsSignedPeerAtAnotherExecutablePath(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	identity := brokerPeerIdentity()
	identity.Peer.Executable = "/tmp/CCPoolStatus.app/Contents/MacOS/CCPoolStatus"
	if _, err := broker.OpenBroker(t.Context(), identity, "principal"); err == nil {
		t.Fatal("same signed identity at a non-fixed executable path was accepted")
	}
}

func settleRegisteredBrokerList(
	t *testing.T,
	session *runtimeBrokerSession,
	registered catalogproto.RegisteredDomain,
) {
	t.Helper()
	list := nextBrokerCommand(t, session)
	domains := []catalogproto.RegisteredDomain{registered}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(domains),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBrokerSignalTargetBoundMatchesCatalogAndWire(t *testing.T) {
	if catalog.MaxFileProviderSignalTargets != int(catalogproto.MaxSignalTargets) {
		t.Fatalf(
			"signal target bound: catalog %d, protocol %d",
			catalog.MaxFileProviderSignalTargets,
			catalogproto.MaxSignalTargets,
		)
	}
}

func TestRuntimeBrokerStartLaunchesFixedBroker(t *testing.T) {
	store := emptyBrokerTestCatalog(t)
	owner := &testBrokerProcessOwner{startCalled: make(chan struct{}, 1)}
	broker := newTestRuntimeBrokerWithOwner(t, store, owner)
	started := make(chan error, 1)
	go func() { started <- broker.Start(t.Context()) }()
	select {
	case <-owner.startCalled:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-started:
		t.Fatalf("broker Start returned before reconciliation: %v", err)
	default:
	}
	session := sessionValue.(*runtimeBrokerSession)
	command := nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: command.CommandID, Kind: command.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	owner.mu.Lock()
	starts := owner.starts
	owner.mu.Unlock()
	if starts != 1 {
		t.Fatalf("broker launches = %d, want 1", starts)
	}
	closeTestRuntimeBroker(t, broker)
	if err := broker.Start(t.Context()); err == nil {
		t.Fatal("closed broker runtime restarted")
	}
}

func TestRuntimeBrokerReadinessWaitsForReconciliationFixedPoint(t *testing.T) {
	store := emptyBrokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	if phase := broker.ReadinessPhase(); phase != RuntimeBrokerStarting {
		t.Fatalf("initial readiness = %d, want starting", phase)
	}
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	if phase := broker.ReadinessPhase(); phase != RuntimeBrokerStarting {
		t.Fatalf("bound readiness = %d, want starting before reconciliation", phase)
	}
	command := nextBrokerCommand(t, session)
	if command.Kind != catalogproto.BrokerCommandKindListDomains {
		t.Fatalf("initial broker command = %q, want list domains", command.Kind)
	}
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: command.CommandID, Kind: command.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	if phase := broker.ReadinessPhase(); phase != RuntimeBrokerLive {
		t.Fatalf("fixed-point readiness = %d, want live", phase)
	}
	closeTestRuntimeBroker(t, broker)
	if phase := broker.ReadinessPhase(); phase != RuntimeBrokerFailed {
		t.Fatalf("closed readiness = %d, want failed", phase)
	}
}

func TestRuntimeBrokerFailedReconcileAdmissionDoesNotStrandReadiness(t *testing.T) {
	store := emptyBrokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	if phase := broker.ReadinessPhase(); phase != RuntimeBrokerLive {
		t.Fatalf("initial fixed-point readiness = %d, want live", phase)
	}
	epoch := session.reconcileEpoch
	for range brokerCommandBuffer {
		<-session.slots
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := broker.enqueueReconcile(ctx, session); !errors.Is(err, context.Canceled) {
		t.Fatalf("enqueue canceled reconciliation = %v, want context canceled", err)
	}
	for range brokerCommandBuffer {
		session.slots <- struct{}{}
	}
	if session.reconcileEpoch != epoch {
		t.Fatalf("failed reconciliation advanced epoch to %d, want %d", session.reconcileEpoch, epoch)
	}
	if phase := broker.ReadinessPhase(); phase != RuntimeBrokerLive {
		t.Fatalf("readiness after failed reconciliation admission = %d, want live", phase)
	}
}

func TestRuntimeBrokerRemovesMetadataFreeDomainBeforeRegisteringDesiredAndSettling(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, session)
	legacyID := brokerObservedDomainID("legacy-account-07")
	legacy := []catalogproto.ObservedDomain{{ObservedID: legacyID}}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &legacy,
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, session)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain ||
		remove.ObservedID == nil || *remove.ObservedID != legacyID {
		t.Fatalf("legacy removal = %+v", remove)
	}
	if phase := broker.ReadinessPhase(); phase != RuntimeBrokerStarting {
		t.Fatalf("readiness with unacknowledged removal = %d, want starting", phase)
	}
	absent := true
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	list = nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	register := nextBrokerCommand(t, session)
	if register.Kind != catalogproto.BrokerCommandKindRegisterDomain || register.Registration == nil {
		t.Fatalf("desired registration = %+v", register)
	}
	registered := catalogproto.RegisteredDomain{
		DomainID: register.Registration.DomainID, OwnerID: register.Registration.OwnerID,
		TenantID: register.Registration.TenantID, Generation: register.Registration.Generation,
		RootID: register.Registration.RootID, AccessMode: register.Registration.AccessMode,
		PresentationInstanceID: register.Registration.PresentationInstanceID,
		DisplayName:            register.Registration.DisplayName, PublicPath: filepath.Join(t.TempDir(), "Domain"),
	}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: register.CommandID, Kind: register.Kind, Registered: &registered,
	}); err != nil {
		t.Fatal(err)
	}
	list = nextBrokerCommand(t, session)
	managed := []catalogproto.RegisteredDomain{registered}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(managed),
	}); err != nil {
		t.Fatal(err)
	}
	if phase := broker.ReadinessPhase(); phase != RuntimeBrokerLive {
		t.Fatalf("final fixed-point readiness = %d, want live", phase)
	}
}

func TestRuntimeBrokerLostRemovalResponseCannotSettleReconciliation(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, session)
	legacy := []catalogproto.ObservedDomain{{ObservedID: brokerObservedDomainID("legacy-account-07")}}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &legacy,
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, session)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain {
		t.Fatalf("command = %q, want remove domain", remove.Kind)
	}
	session.Close(errBrokerDeliveryUnknown)
	select {
	case <-session.done:
	case <-time.After(time.Second):
		t.Fatal("broker session did not settle after lost response")
	}
	if phase := broker.ReadinessPhase(); phase == RuntimeBrokerLive {
		t.Fatal("broker reconciliation settled after a lost removal response")
	}
	replacementValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentityAt(42, "replacement-start"), "principal")
	if err != nil {
		t.Fatal(err)
	}
	replacement := replacementValue.(*runtimeBrokerSession)
	list = nextBrokerCommand(t, replacement)
	if err := replacement.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &legacy,
	}); err != nil {
		t.Fatal(err)
	}
	remove = nextBrokerCommand(t, replacement)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain || remove.ObservedID == nil ||
		*remove.ObservedID != legacy[0].ObservedID {
		t.Fatalf("replacement removal = %+v", remove)
	}
	if phase := broker.ReadinessPhase(); phase == RuntimeBrokerLive {
		t.Fatal("replacement broker settled before replayed removal acknowledgement")
	}
	absent := true
	if err := replacement.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	list = nextBrokerCommand(t, replacement)
	empty := []catalogproto.RegisteredDomain{}
	if err := replacement.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	register := nextBrokerCommand(t, replacement)
	if register.Kind != catalogproto.BrokerCommandKindRegisterDomain || register.Registration == nil {
		t.Fatalf("replacement registration = %+v", register)
	}
	registered := catalogproto.RegisteredDomain{
		DomainID: register.Registration.DomainID, OwnerID: register.Registration.OwnerID,
		TenantID: register.Registration.TenantID, Generation: register.Registration.Generation,
		RootID: register.Registration.RootID, AccessMode: register.Registration.AccessMode,
		PresentationInstanceID: register.Registration.PresentationInstanceID,
		DisplayName:            register.Registration.DisplayName, PublicPath: filepath.Join(t.TempDir(), "Recovered"),
	}
	if err := replacement.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: register.CommandID, Kind: register.Kind, Registered: &registered,
	}); err != nil {
		t.Fatal(err)
	}
	list = nextBrokerCommand(t, replacement)
	managed := []catalogproto.RegisteredDomain{registered}
	if err := replacement.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(managed),
	}); err != nil {
		t.Fatal(err)
	}
	if phase := broker.ReadinessPhase(); phase != RuntimeBrokerLive {
		t.Fatalf("recovered fixed-point readiness = %d, want live", phase)
	}
}

func TestRuntimeBrokerPreparesCurrentActivationAfterDurableInvalidation(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	registered := confirmBrokerDomain(t, store)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	settleRegisteredBrokerList(t, session, registered)

	type result struct {
		domain catalog.FileProviderDomain
		err    error
	}
	prepared := make(chan result, 1)
	go func() {
		domain, prepareErr := broker.PrepareFileProviderPresentation(
			t.Context(), provision.Tenant, provision.Generation,
		)
		prepared <- result{domain: domain, err: prepareErr}
	}()
	register := nextBrokerCommand(t, session)
	if register.Kind != catalogproto.BrokerCommandKindRegisterDomain || register.Registration == nil {
		t.Fatalf("prepare command = %+v", register)
	}
	invalidated, found, err := store.FileProviderDomainForTenant(t.Context(), provision.Tenant)
	if err != nil || !found || invalidated.Registered || invalidated.PublicPath != "" || invalidated.ActivationGeneration != "" {
		t.Fatalf("domain before broker observation = %+v, %t, %v", invalidated, found, err)
	}
	registered.PublicPath = filepath.Join(t.TempDir(), "Reobserved")
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: register.CommandID, Kind: register.Kind, Registered: &registered,
	}); err != nil {
		t.Fatal(err)
	}
	outcome := <-prepared
	if outcome.err != nil || !outcome.domain.Registered || outcome.domain.PublicPath != registered.PublicPath ||
		outcome.domain.ActivationGeneration != "activation-test" {
		t.Fatalf("prepared domain = %+v, %v", outcome.domain, outcome.err)
	}
}

func TestRuntimeBrokerReconcilesRegisterRestartAndRemove(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	if _, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal"); err == nil {
		t.Fatal("duplicate broker session was accepted")
	}
	list := nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	register := nextBrokerCommand(t, session)
	if register.Kind != catalogproto.BrokerCommandKindRegisterDomain || register.Registration == nil {
		t.Fatalf("register command = %+v", register)
	}
	registered := catalogproto.RegisteredDomain{
		DomainID: register.Registration.DomainID, OwnerID: register.Registration.OwnerID,
		TenantID: register.Registration.TenantID, Generation: register.Registration.Generation,
		RootID: register.Registration.RootID, AccessMode: register.Registration.AccessMode,
		PresentationInstanceID: register.Registration.PresentationInstanceID,
		DisplayName:            register.Registration.DisplayName, PublicPath: filepath.Join(t.TempDir(), "Domain"),
	}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: register.CommandID, Kind: register.Kind, Registered: &registered,
	}); err != nil {
		t.Fatal(err)
	}
	list = nextBrokerCommand(t, session)
	domains := []catalogproto.RegisteredDomain{registered}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(domains),
	}); err != nil {
		t.Fatal(err)
	}
	actual, err := allRuntimeBrokerDomains(t, store)
	if err != nil || len(actual) != 1 || !actual[0].Registered {
		t.Fatalf("registered domains = %+v, %v", actual, err)
	}
	session.Close(nil)

	restartedValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentityAt(42, "restart-1"), "principal")
	if err != nil {
		t.Fatal(err)
	}
	restarted := restartedValue.(*runtimeBrokerSession)
	list = nextBrokerCommand(t, restarted)
	if err := restarted.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(domains),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveTenantProvision(t.Context(), provision.Tenant, provision.Generation); err != nil {
		t.Fatal(err)
	}
	restarted.Close(nil)
	restartedValue, err = broker.OpenBroker(t.Context(), brokerPeerIdentityAt(43, "restart-2"), "principal")
	if err != nil {
		t.Fatal(err)
	}
	restarted = restartedValue.(*runtimeBrokerSession)
	list = nextBrokerCommand(t, restarted)
	if err := restarted.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(domains),
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, restarted)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain || remove.ObservedID == nil || *remove.ObservedID != brokerObservedDomainID(registered.DomainID) {
		t.Fatalf("remove command = %+v", remove)
	}
	absent := true
	if err := restarted.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	restarted.Close(nil)
	closeTestRuntimeBroker(t, broker)
}

func TestRuntimeBrokerLostListResponseRestartsFromFirstPage(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })

	firstValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	first := firstValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, first)
	if list.Kind != catalogproto.BrokerCommandKindListDomains || list.AfterObservedID != nil {
		t.Fatalf("first list command = %+v", list)
	}
	first.Close(errors.New("list response lost"))

	secondValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentityAt(42, "list-restart"), "principal")
	if err != nil {
		t.Fatal(err)
	}
	second := secondValue.(*runtimeBrokerSession)
	list = nextBrokerCommand(t, second)
	if list.Kind != catalogproto.BrokerCommandKindListDomains || list.AfterObservedID != nil {
		t.Fatalf("restarted list command = %+v, want first page", list)
	}
	empty := []catalogproto.RegisteredDomain{}
	if err := second.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	register := nextBrokerCommand(t, second)
	if register.Kind != catalogproto.BrokerCommandKindRegisterDomain {
		t.Fatalf("post-restart reconciliation command = %+v", register)
	}
}

func TestRuntimeBrokerLostRegisterResponseReconcilesObservedDomainWithoutReplay(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })

	firstValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	first := firstValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, first)
	empty := []catalogproto.RegisteredDomain{}
	if err := first.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	register := nextBrokerCommand(t, first)
	if register.Kind != catalogproto.BrokerCommandKindRegisterDomain || register.Registration == nil {
		t.Fatalf("register command = %+v", register)
	}
	registered := catalogproto.RegisteredDomain{
		DomainID: register.Registration.DomainID, OwnerID: register.Registration.OwnerID,
		TenantID: register.Registration.TenantID, Generation: register.Registration.Generation,
		RootID: register.Registration.RootID, AccessMode: register.Registration.AccessMode,
		PresentationInstanceID: register.Registration.PresentationInstanceID,
		DisplayName:            register.Registration.DisplayName, PublicPath: filepath.Join(t.TempDir(), "Domain"),
	}
	first.Close(errors.New("successful register response lost"))

	secondValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentityAt(42, "register-restart"), "principal")
	if err != nil {
		t.Fatal(err)
	}
	second := secondValue.(*runtimeBrokerSession)
	settleRegisteredBrokerList(t, second, registered)
	select {
	case command := <-second.Commands():
		t.Fatalf("observed registered domain was replayed: %+v", command)
	default:
	}
	domains, err := allRuntimeBrokerDomains(t, store)
	if err != nil || len(domains) != 1 || !domains[0].Registered || domains[0].PublicPath != registered.PublicPath {
		t.Fatalf("reconciled domains = %+v, %v", domains, err)
	}
}

func TestRuntimeBrokerProductUpgradePreservesMatchingDomainsWithoutNotification(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	registered := confirmBrokerDomain(t, store)

	oldIdentity := testRuntimeBrokerIdentity()
	oldIdentity.ProductBuild = "product-build-a"
	oldBroker, err := NewRuntimeBroker(t.Context(), store, oldIdentity, "activation-old", &testBrokerProcessOwner{})
	if err != nil {
		t.Fatal(err)
	}
	if err := oldBroker.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	oldSessionValue, err := oldBroker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	settleRegisteredBrokerList(t, oldSessionValue.(*runtimeBrokerSession), registered)
	oldSessionValue.Close(nil)
	closeTestRuntimeBroker(t, oldBroker)

	newIdentity := oldIdentity
	newIdentity.ProductBuild = "product-build-b"
	newBroker, err := NewRuntimeBroker(t.Context(), store, newIdentity, "activation-new", &testBrokerProcessOwner{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTestRuntimeBroker(t, newBroker) })
	if err := newBroker.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	newSessionValue, err := newBroker.OpenBroker(t.Context(), brokerPeerIdentityAt(42, "upgrade"), "principal")
	if err != nil {
		t.Fatal(err)
	}
	newSession := newSessionValue.(*runtimeBrokerSession)
	settleRegisteredBrokerList(t, newSession, registered)
	select {
	case command := <-newSession.Commands():
		t.Fatalf("product upgrade emitted domain or content command: %+v", command)
	default:
	}
}

func TestRuntimeBrokerStopsPagingWhenActualPageRequiresReconciliation(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	first := nextBrokerCommand(t, session)
	if first.Kind != catalogproto.BrokerCommandKindListDomains || first.AfterObservedID != nil {
		t.Fatalf("initial list command = %+v", first)
	}
	page := make([]catalogproto.RegisteredDomain, 0, catalogproto.MaxBrokerDomainPageSize)
	for index := 0; index < int(catalogproto.MaxBrokerDomainPageSize); index++ {
		account := catalogproto.PresentationInstanceID(fmt.Sprintf("page-%03d", index))
		domain, err := catalogproto.DeriveDomainID("owner-page", account)
		if err != nil {
			t.Fatal(err)
		}
		page = append(page, catalogproto.RegisteredDomain{
			DomainID: domain, OwnerID: "owner-page", TenantID: catalogproto.TenantID(fmt.Sprintf("tenant-%03d", index)),
			Generation: 1, RootID: "00000000000000000000000000000001",
			AccessMode: catalogproto.TenantAccessModeReadWrite, PresentationInstanceID: account,
			DisplayName: "Page", PublicPath: "/Users/test/Library/CloudStorage/Page",
		})
	}
	sort.Slice(page, func(i, j int) bool {
		return brokerObservedDomainID(page[i].DomainID) < brokerObservedDomainID(page[j].DomainID)
	})
	next := brokerObservedDomainID(page[len(page)-1].DomainID)
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: first.CommandID, Kind: first.Kind, Domains: observedBrokerDomainPage(page), NextAfterObservedID: &next,
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, session)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain || remove.ObservedID == nil {
		t.Fatalf("first bounded reconciliation command = %+v, want domain removal", remove)
	}
	session.Close(nil)
}

func TestRuntimeBrokerReconcilesOneHundredDomainsWithBoundedPagesAndPointLookups(t *testing.T) {
	store, firstProvision := brokerTestCatalog(t)
	provisions := []catalog.TenantProvision{firstProvision}
	for index := 1; index < 100; index++ {
		provision := firstProvision
		provision.Tenant = catalog.TenantID(fmt.Sprintf("tenant-%03d", index))
		provision.BackingRoot = filepath.Join(t.TempDir(), "backing")
		provision.FileProvider.PresentationInstanceID = fmt.Sprintf("instance-%03d", index)
		provision.FileProvider.DisplayName = fmt.Sprintf("Tenant %03d", index)
		created, err := store.ProvisionTenant(t.Context(), provision)
		if err != nil {
			t.Fatal(err)
		}
		provisions = append(provisions, created)
	}
	actual := make([]catalogproto.RegisteredDomain, 0, len(provisions))
	for _, provision := range provisions {
		domain, found, err := store.FileProviderDomainForTenant(t.Context(), provision.Tenant)
		if err != nil || !found {
			t.Fatalf("FileProviderDomainForTenant(%s) = %+v, %t, %v", provision.Tenant, domain, found, err)
		}
		domain.PublicPath = filepath.Join("/Users/test/Library/CloudStorage", string(provision.Tenant))
		actual = append(actual, protocolRegisteredDomain(t, domain))
	}
	sort.Slice(actual, func(i, j int) bool {
		return brokerObservedDomainID(actual[i].DomainID) < brokerObservedDomainID(actual[j].DomainID)
	})

	counting := &countingRuntimeBrokerStore{Catalog: store}
	broker, err := NewRuntimeBroker(t.Context(), counting, testRuntimeBrokerIdentity(), "activation-test", &testBrokerProcessOwner{})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	command := nextBrokerCommand(t, session)
	pages := 0
	for start := 0; start < len(actual); start += int(catalogproto.MaxBrokerDomainPageSize) {
		end := min(start+int(catalogproto.MaxBrokerDomainPageSize), len(actual))
		page := append([]catalogproto.RegisteredDomain(nil), actual[start:end]...)
		var next *catalogproto.ObservedDomainID
		if end < len(actual) {
			value := brokerObservedDomainID(page[len(page)-1].DomainID)
			next = &value
		}
		if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
			CommandID: command.CommandID, Kind: command.Kind, Domains: observedBrokerDomainPage(page), NextAfterObservedID: next,
		}); err != nil {
			t.Fatal(err)
		}
		pages++
		if next != nil {
			command = nextBrokerCommand(t, session)
			if command.AfterObservedID == nil || *command.AfterObservedID != *next {
				t.Fatalf("page %d continuation = %+v, want %s", pages, command, *next)
			}
		}
	}
	wantPages := (len(actual) + int(catalogproto.MaxBrokerDomainPageSize) - 1) /
		int(catalogproto.MaxBrokerDomainPageSize)
	if pages != wantPages {
		t.Fatalf("actual-domain pages = %d, want %d", pages, wantPages)
	}
	if got := counting.domainLookups.Load(); got != int32(len(actual)) {
		t.Fatalf("desired point lookups = %d, want %d", got, len(actual))
	}
	if got := counting.desiredPages.Load(); got != 1 {
		t.Fatalf("desired full pages = %d, want one bounded final page", got)
	}
	select {
	case unexpected := <-session.Commands():
		t.Fatalf("fully matched reconciliation emitted command %+v", unexpected)
	default:
	}
	session.Close(nil)
}

func TestRuntimeBrokerLiveSessionRemovalWaitsForExactAbsentResult(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	registered := confirmBrokerDomain(t, store)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	initial := nextBrokerCommand(t, session)
	actual := []catalogproto.RegisteredDomain{registered}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: initial.CommandID, Kind: initial.Kind, Domains: observedBrokerDomainPage(actual),
	}); err != nil {
		t.Fatal(err)
	}

	settled := make(chan error, 1)
	go func() {
		settled <- broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	list := nextBrokerCommand(t, session)
	if list.Kind != catalogproto.BrokerCommandKindListDomains {
		t.Fatalf("removal reconcile command = %+v", list)
	}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(actual),
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, session)
	select {
	case err := <-settled:
		t.Fatalf("removal acknowledged before OS result: %v", err)
	default:
	}
	absent := true
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-settled:
		t.Fatalf("removal acknowledged before authoritative absence list: %v", err)
	default:
	}
	list = nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
	state, err := store.FileProviderDomainRemovalState(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	if err != nil || !state.ConfirmedAbsent {
		t.Fatalf("removal state = %+v, %v", state, err)
	}
}

func TestRuntimeBrokerRemovalRecoversDisconnectAndLostResponse(t *testing.T) {
	tests := []struct {
		name       string
		firstReply bool
	}{
		{name: "disconnect before remove result", firstReply: false},
		{name: "lost successful remove response", firstReply: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, provision := brokerTestCatalog(t)
			registered := confirmBrokerDomain(t, store)
			broker := newTestRuntimeBroker(t, store)
			t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
			firstValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
			if err != nil {
				t.Fatal(err)
			}
			first := firstValue.(*runtimeBrokerSession)
			actual := []catalogproto.RegisteredDomain{registered}
			initial := nextBrokerCommand(t, first)
			if err := first.AcceptResult(t.Context(), catalogproto.BrokerResult{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				CommandID: initial.CommandID, Kind: initial.Kind, Domains: observedBrokerDomainPage(actual),
			}); err != nil {
				t.Fatal(err)
			}
			settled := make(chan error, 1)
			go func() {
				settled <- broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
			}()
			list := nextBrokerCommand(t, first)
			if err := first.AcceptResult(t.Context(), catalogproto.BrokerResult{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(actual),
			}); err != nil {
				t.Fatal(err)
			}
			_ = nextBrokerCommand(t, first)
			first.Close(nil)

			secondValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentityAt(42, "restart"), "principal")
			if err != nil {
				t.Fatal(err)
			}
			second := secondValue.(*runtimeBrokerSession)
			list = nextBrokerCommand(t, second)
			if test.firstReply {
				actual = []catalogproto.RegisteredDomain{}
			}
			if err := second.AcceptResult(t.Context(), catalogproto.BrokerResult{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(actual),
			}); err != nil {
				t.Fatal(err)
			}
			if !test.firstReply {
				remove := nextBrokerCommand(t, second)
				absent := true
				if err := second.AcceptResult(t.Context(), catalogproto.BrokerResult{
					Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
					CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
				}); err != nil {
					t.Fatal(err)
				}
				list = nextBrokerCommand(t, second)
				empty := []catalogproto.RegisteredDomain{}
				if err := second.AcceptResult(t.Context(), catalogproto.BrokerResult{
					Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
					CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(empty),
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := <-settled; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRuntimeBrokerRemovalFencesRequestAndRetiresObservedIdentityDrift(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	wrong := confirmBrokerDomain(t, store)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	if err := broker.RemoveTenantDomain(t.Context(), "wrong-owner", provision.Tenant, provision.Generation); !errors.Is(err, catalog.ErrTenantOwnerMismatch) {
		t.Fatalf("wrong owner removal = %v", err)
	}
	if err := broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation+1); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("wrong generation removal = %v", err)
	}
	if _, err := store.BeginFileProviderDomainRemoval(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation); err != nil {
		t.Fatal(err)
	}

	settled := make(chan error, 1)
	go func() {
		settled <- broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	waitForBrokerRemovalIntent(t, store, provision)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, session)
	wrong.Generation++
	actual := []catalogproto.RegisteredDomain{wrong}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(actual),
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, session)
	if remove.ObservedID == nil || *remove.ObservedID != brokerObservedDomainID(wrong.DomainID) {
		t.Fatalf("drifted domain removal = %+v", remove)
	}
	select {
	case err := <-settled:
		t.Fatalf("drifted domain was reported absent while still listed: %v", err)
	default:
	}
	absent := true
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	list = nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBrokerRemovalWaitsForEveryMatchingStrayDomain(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	expected := confirmBrokerDomain(t, store)
	stray := expected
	stray.DomainID = distinctBrokerDomainID(expected.DomainID)
	stray.Generation++

	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	settled := make(chan error, 1)
	go func() {
		settled <- broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	waitForBrokerRemovalIntent(t, store, provision)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, session)
	actual := []catalogproto.RegisteredDomain{stray}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(actual),
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, session)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain || remove.ObservedID == nil || *remove.ObservedID != brokerObservedDomainID(stray.DomainID) {
		t.Fatalf("stray-domain removal = %+v", remove)
	}
	absent := true
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-settled:
		t.Fatalf("removal acknowledged while expected domain remains: %v", err)
	default:
	}
	list = nextBrokerCommand(t, session)
	actual = []catalogproto.RegisteredDomain{expected}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(actual),
	}); err != nil {
		t.Fatal(err)
	}
	remove = nextBrokerCommand(t, session)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain || remove.ObservedID == nil || *remove.ObservedID != brokerObservedDomainID(expected.DomainID) {
		t.Fatalf("expected-domain removal = %+v", remove)
	}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-settled:
		t.Fatalf("removal acknowledged before authoritative fleet absence: %v", err)
	default:
	}
	list = nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBrokerAlreadyAbsentRemovalNeedsNoSessionRestart(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	if _, err := store.BeginFileProviderDomainRemoval(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation); err != nil {
		t.Fatal(err)
	}
	settled := make(chan error, 1)
	go func() {
		settled <- broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBrokerRemovalIntentRecoversAcrossRuntimeRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := catalog.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	provision := catalog.TenantProvision{
		OwnerID: "owner", Tenant: "restart-tenant",
		BackingRoot: filepath.Join(t.TempDir(), "backing"), ContentSourceID: "source",
		Access: catalog.TenantReadWrite, CasePolicy: catalog.CaseSensitive,
		Presentations: catalog.PresentFileProvider,
		FileProvider:  catalog.FileProviderPresentation{PresentationInstanceID: "restart-instance", DisplayName: "Restart"}, Generation: 9,
	}
	provision, err = store.ProvisionTenant(t.Context(), provision)
	if err != nil {
		t.Fatal(err)
	}
	registered := confirmBrokerDomain(t, store)
	registered.DomainID = distinctBrokerDomainID(registered.DomainID)
	registered.Generation++
	first := newTestRuntimeBroker(t, store)
	removeContext, cancel := context.WithCancel(t.Context())
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- first.RemoveTenantDomain(removeContext, provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := store.FileProviderDomainRemovalState(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("durable removal intent was not written")
		}
		time.Sleep(time.Millisecond)
	}
	firstSessionValue, err := first.OpenBroker(removeContext, brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	firstSession := firstSessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, firstSession)
	actual := []catalogproto.RegisteredDomain{registered}
	if err := firstSession.AcceptResult(removeContext, catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(actual),
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, firstSession)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain || remove.ObservedID == nil || *remove.ObservedID != brokerObservedDomainID(registered.DomainID) {
		t.Fatalf("stray removal before restart = %+v", remove)
	}
	firstSession.Close(nil)
	cancel()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("first removal = %v", err)
	}
	closeTestRuntimeBroker(t, first)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = catalog.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	second := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, second) })
	settled := make(chan error, 1)
	go func() {
		settled <- second.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	sessionValue, err := second.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list = nextBrokerCommand(t, session)
	actual = []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: observedBrokerDomainPage(actual),
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
}

func distinctBrokerDomainID(id catalogproto.DomainID) catalogproto.DomainID {
	value := []byte(id)
	if value[len(value)-1] == '0' {
		value[len(value)-1] = '1'
	} else {
		value[len(value)-1] = '0'
	}
	return catalogproto.DomainID(value)
}

func waitForBrokerRemovalIntent(t *testing.T, store *catalog.Catalog, provision catalog.TenantProvision) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := store.FileProviderDomainRemovalState(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("durable removal intent was not written")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRuntimeBrokerSessionLossMakesSentNotificationUnknownAndBoundsQueue(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	domains, err := allRuntimeBrokerDomains(t, store)
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	registration, err := protocolDomainRegistration(domains[0])
	if err != nil {
		t.Fatal(err)
	}
	for index := 1; index < brokerCommandBuffer; index++ {
		if err := broker.enqueue(t.Context(), session, catalogproto.BrokerCommand{
			Kind: catalogproto.BrokerCommandKindRegisterDomain, Registration: &registration,
		}, nil); err != nil {
			t.Fatalf("fill command %d: %v", index, err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := broker.enqueue(ctx, session, catalogproto.BrokerCommand{
		Kind: catalogproto.BrokerCommandKindRegisterDomain, Registration: &registration,
	}, nil); err == nil {
		t.Fatal("broker queue exceeded its fixed capacity")
	}
	session.Close(nil)
	closeTestRuntimeBroker(t, broker)
}

func TestRuntimeBrokerSessionLossMakesSentNotificationUnknown(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	_ = nextBrokerCommand(t, session)
	domains, err := allRuntimeBrokerDomains(t, store)
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	outcome := make(chan convergence.Delivery, 1)
	notification := brokerNotification(domains[0], 1)
	go func() {
		delivery, _ := broker.Notify(t.Context(), notification)
		outcome <- delivery
	}()
	command := nextBrokerCommand(t, session)
	if command.Kind != catalogproto.BrokerCommandKindSignalDomain {
		t.Fatalf("signal command = %+v", command)
	}
	wantFingerprint := [32]byte{1}
	wantTargetDigest := [32]byte{2}
	if command.Notification == nil ||
		command.Notification.ActivationChangeID != catalogproto.ActivationChangeID(hex.EncodeToString(notification.ActivationChangeID[:])) ||
		command.Notification.ProviderFingerprint != hex.EncodeToString(wantFingerprint[:]) ||
		command.Notification.TargetCount != 1 || command.Notification.TargetDigest != hex.EncodeToString(wantTargetDigest[:]) ||
		len(command.Notification.Targets) != 1 || command.Notification.Targets[0].Kind != catalogproto.SignalTargetKindWorkingSet {
		t.Fatalf("activation notification = %+v", command.Notification)
	}
	session.Close(nil)
	select {
	case delivery := <-outcome:
		if delivery != convergence.DeliveryUnknown {
			t.Fatalf("delivery = %v", delivery)
		}
	case <-time.After(time.Second):
		t.Fatal("Notify did not settle after session loss")
	}
	closeTestRuntimeBroker(t, broker)
}

func TestRuntimeBrokerAcceptedResponseLossNeverResendsExactRevision(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	registered := confirmBrokerDomain(t, store)
	owner := &testBrokerProcessOwner{}
	broker := newTestRuntimeBrokerWithOwner(t, store, owner)
	firstValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	first := firstValue.(*runtimeBrokerSession)
	settleRegisteredBrokerList(t, first, registered)
	domains, err := allRuntimeBrokerDomains(t, store)
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	notification := brokerNotification(domains[0], 1)
	outcome := make(chan convergence.Delivery, 1)
	go func() {
		delivery, _ := broker.Notify(t.Context(), notification)
		outcome <- delivery
	}()
	command := nextBrokerCommand(t, first)
	if command.Kind != catalogproto.BrokerCommandKindSignalDomain {
		t.Fatalf("signal command = %+v", command)
	}
	// The signed app may have accepted FileProviderManager signaling even when
	// its exact result frame is lost with the stream.
	first.Close(errors.New("accepted result frame lost"))
	select {
	case delivery := <-outcome:
		if delivery != convergence.DeliveryUnknown {
			t.Fatalf("delivery = %v, want unknown", delivery)
		}
	case <-time.After(time.Second):
		t.Fatal("lost accepted response did not settle")
	}

	secondIdentity := brokerPeerIdentity()
	secondIdentity.Peer.PID++
	secondIdentity.Peer.StartTime = "test-start-2"
	secondValue, err := broker.OpenBroker(t.Context(), secondIdentity, "principal")
	if err != nil {
		t.Fatal(err)
	}
	second := secondValue.(*runtimeBrokerSession)
	settleRegisteredBrokerList(t, second, registered)
	delivery, err := broker.Notify(t.Context(), notification)
	if err != nil || delivery != convergence.DeliveryUnknown {
		t.Fatalf("exact retry = %v, %v, want unknown no-resend fence", delivery, err)
	}
	select {
	case repeated := <-second.Commands():
		t.Fatalf("exact revision was resent: %+v", repeated)
	case <-time.After(20 * time.Millisecond):
	}
	closeTestRuntimeBroker(t, broker)
}

func TestRuntimeBrokerClosedStreamBeforePlanningIsNotSent(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	registered := confirmBrokerDomain(t, store)
	broker := newTestRuntimeBroker(t, store)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	settleRegisteredBrokerList(t, session, registered)
	session.Close(errors.New("closed before signal"))
	select {
	case <-session.done:
	case <-time.After(time.Second):
		t.Fatal("broker session did not settle")
	}
	domains, err := allRuntimeBrokerDomains(t, store)
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	delivery, err := broker.Notify(t.Context(), brokerNotification(domains[0], 1))
	if delivery != convergence.DeliveryNotSent || !errors.Is(err, errBrokerSessionLost) {
		t.Fatalf("closed stream delivery = %v, %v", delivery, err)
	}
	closeTestRuntimeBroker(t, broker)
}

func TestRuntimeBrokerDeadlineReapsGenerationBeforeReleasingCapacity(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	registered := confirmBrokerDomain(t, store)
	retireRelease := make(chan struct{})
	owner := &testBrokerProcessOwner{
		retireStarted: make(chan struct{}),
		retireRelease: retireRelease,
	}
	broker := newTestRuntimeBrokerWithOwner(t, store, owner)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	settleRegisteredBrokerList(t, session, registered)
	domains, err := allRuntimeBrokerDomains(t, store)
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	outcome := make(chan convergence.Delivery, 1)
	go func() {
		delivery, _ := broker.Notify(ctx, brokerNotification(domains[0], 1))
		outcome <- delivery
	}()
	command := nextBrokerCommand(t, session)
	if command.Kind != catalogproto.BrokerCommandKindSignalDomain {
		t.Fatalf("signal command = %+v", command)
	}
	select {
	case <-owner.retireStarted:
	case <-time.After(time.Second):
		t.Fatal("broker generation was not poisoned at the command deadline")
	}
	owner.mu.Lock()
	starts := owner.starts
	owner.mu.Unlock()
	if starts != 0 {
		t.Fatalf("replacement starts before reap = %d", starts)
	}
	select {
	case delivery := <-outcome:
		if delivery != convergence.DeliveryUnknown {
			t.Fatalf("delivery = %v, want unknown", delivery)
		}
	case <-time.After(time.Second):
		t.Fatal("Notify deadline did not settle as unknown")
	}
	select {
	case <-session.done:
		t.Fatal("broker capacity released before exact reap")
	default:
	}
	close(retireRelease)
	select {
	case <-session.done:
	case <-time.After(time.Second):
		t.Fatal("broker generation did not settle after exact reap")
	}
	deadline := time.Now().Add(time.Second)
	for {
		owner.mu.Lock()
		starts = owner.starts
		owner.mu.Unlock()
		if starts == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replacement broker was not launched after reap")
		}
		time.Sleep(time.Millisecond)
	}
	closeTestRuntimeBroker(t, broker)
}

func TestRuntimeBrokerFailedReapWithholdsCapacityUntilRecovery(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	registered := confirmBrokerDomain(t, store)
	owner := &testBrokerProcessOwner{retireErr: errors.New("reap proof unavailable")}
	broker := newTestRuntimeBrokerWithOwner(t, store, owner)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	settleRegisteredBrokerList(t, session, registered)
	domains, err := allRuntimeBrokerDomains(t, store)
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	outcome := make(chan convergence.Delivery, 1)
	go func() {
		delivery, _ := broker.Notify(ctx, brokerNotification(domains[0], 1))
		outcome <- delivery
	}()
	_ = nextBrokerCommand(t, session)
	select {
	case delivery := <-outcome:
		if delivery != convergence.DeliveryUnknown {
			t.Fatalf("delivery = %v, want unknown", delivery)
		}
	case <-time.After(time.Second):
		t.Fatal("Notify did not settle after failed reap")
	}
	owner.mu.Lock()
	starts := owner.starts
	owner.mu.Unlock()
	if starts != 0 {
		t.Fatalf("replacement starts after failed reap = %d", starts)
	}
	if _, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal"); err == nil {
		t.Fatal("failed reap released broker admission")
	}
	owner.mu.Lock()
	owner.retireErr = nil
	owner.mu.Unlock()
	select {
	case <-session.done:
	case <-time.After(time.Second):
		t.Fatal("broker retirement did not recover after transient reap failure")
	}
	closeTestRuntimeBroker(t, broker)
}

func TestRuntimeBrokerCloseNotesDeadlineButJoinsExactRetirement(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	registered := confirmBrokerDomain(t, store)
	retireRelease := make(chan struct{})
	owner := &testBrokerProcessOwner{
		retireStarted: make(chan struct{}),
		retireRelease: retireRelease,
	}
	broker := newTestRuntimeBrokerWithOwner(t, store, owner)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	settleRegisteredBrokerList(t, session, registered)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- broker.Close(ctx) }()
	select {
	case <-owner.retireStarted:
	case <-time.After(time.Second):
		t.Fatal("broker retirement did not begin")
	}
	<-ctx.Done()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before exact retirement settled: %v", err)
	default:
	}
	close(retireRelease)
	select {
	case err := <-closed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close error = %v, want recorded deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after exact retirement")
	}
	select {
	case <-session.done:
	default:
		t.Fatal("Close returned before broker session settlement")
	}
}

func TestRuntimeBrokerCloseJoinsRecoveryAfterRetirementDespiteDeadline(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	registered := confirmBrokerDomain(t, store)
	recoveryRelease := make(chan struct{})
	blockingStore := &blockingBrokerRecoveryStore{
		RuntimeBrokerStore: store,
		started:            make(chan struct{}),
		release:            recoveryRelease,
	}
	owner := &testBrokerProcessOwner{}
	broker, err := NewRuntimeBroker(t.Context(), blockingStore, testRuntimeBrokerIdentity(), "activation-test", owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	settleRegisteredBrokerList(t, session, registered)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- broker.Close(ctx) }()
	select {
	case <-blockingStore.started:
	case <-time.After(time.Second):
		t.Fatal("durable broker-attempt recovery did not begin")
	}
	owner.mu.Lock()
	retired := len(owner.retired)
	owner.mu.Unlock()
	if retired == 0 {
		t.Fatal("attempt recovery began before exact process retirement")
	}
	<-ctx.Done()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before durable recovery settled: %v", err)
	default:
	}
	close(recoveryRelease)
	select {
	case err := <-closed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close error = %v, want recorded deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after durable recovery")
	}
	select {
	case <-session.done:
	default:
		t.Fatal("Close returned before broker session settlement")
	}
}

func brokerNotification(domain catalog.FileProviderDomain, revision uint64) causal.ActivationEvent {
	var publication, operation causal.OperationID
	var change causal.ChangeID
	publication[0] = byte(revision)
	operation[0] = byte(revision + 1)
	change[0] = byte(revision + 2)
	headDigest := [32]byte{byte(revision + 3)}
	cause := causal.SourceCause{
		PublicationID: publication, ChangeID: change, SourceRevision: causal.Revision(revision),
		OperationID: operation, Cause: causal.CauseDaemonWrite, AffectedKeysDigest: [32]byte{1},
	}
	activation, err := causal.DeriveActivationChangeID(
		causal.TenantID(domain.Tenant), causal.Generation(domain.Generation), revision, headDigest,
		[]causal.OperationID{publication},
	)
	if err != nil {
		panic(err)
	}
	return causal.ActivationEvent{
		ActivationChangeID: activation, TenantID: causal.TenantID(domain.Tenant),
		TenantGeneration: causal.Generation(domain.Generation), ActivationRevision: causal.Revision(revision),
		PresentationID: causal.PresentationID(domain.DomainID), Backend: causal.BackendFileProvider,
		CatalogHead: causal.CatalogRevision(revision), HeadDigest: headDigest, Causes: []causal.SourceCause{cause},
	}
}

func nextBrokerCommand(t *testing.T, session *runtimeBrokerSession) catalogproto.BrokerCommand {
	t.Helper()
	select {
	case command := <-session.Commands():
		return command
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for broker command")
		return catalogproto.BrokerCommand{}
	}
}

func brokerTestCatalog(t *testing.T) (*catalog.Catalog, catalog.TenantProvision) {
	t.Helper()
	store := emptyBrokerTestCatalog(t)
	var err error
	provision := catalog.TenantProvision{
		OwnerID: "owner", Tenant: "tenant",
		BackingRoot: filepath.Join(t.TempDir(), "backing"), ContentSourceID: "source",
		Access: catalog.TenantReadWrite, CasePolicy: catalog.CaseSensitive,
		Presentations: catalog.PresentFileProvider,
		FileProvider:  catalog.FileProviderPresentation{PresentationInstanceID: "instance", DisplayName: "Tenant"}, Generation: 1,
	}
	provision, err = store.ProvisionTenant(t.Context(), provision)
	if err != nil {
		t.Fatal(err)
	}
	return store, provision
}

func emptyBrokerTestCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func allRuntimeBrokerDomains(t *testing.T, store *catalog.Catalog) ([]catalog.FileProviderDomain, error) {
	t.Helper()
	var domains []catalog.FileProviderDomain
	for after := catalog.TenantID(""); ; {
		page, err := store.PageFileProviderDomains(t.Context(), after, catalog.FileProviderDomainPageLimit)
		if err != nil {
			return nil, err
		}
		domains = append(domains, page.Domains...)
		if page.Next == "" {
			return domains, nil
		}
		after = page.Next
	}
}

func confirmBrokerDomain(t *testing.T, store *catalog.Catalog) catalogproto.RegisteredDomain {
	t.Helper()
	domains, err := allRuntimeBrokerDomains(t, store)
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	domain := domains[0]
	domain.PublicPath = filepath.Join(t.TempDir(), "Domain")
	domain.ActivationGeneration = "activation-test"
	domain.Registered = true
	if err := store.ConfirmFileProviderDomain(t.Context(), domain); err != nil {
		t.Fatal(err)
	}
	return protocolRegisteredDomain(t, domain)
}

func protocolRegisteredDomain(t *testing.T, domain catalog.FileProviderDomain) catalogproto.RegisteredDomain {
	t.Helper()
	access, err := protocolTenantAccess(domain.Access)
	if err != nil {
		t.Fatal(err)
	}
	return catalogproto.RegisteredDomain{
		DomainID: catalogproto.DomainID(domain.DomainID), OwnerID: catalogproto.OwnerID(domain.OwnerID),
		TenantID: catalogproto.TenantID(domain.Tenant), Generation: uint64(domain.Generation),
		RootID: catalogproto.ObjectID(domain.Root.String()), AccessMode: access,
		PresentationInstanceID: catalogproto.PresentationInstanceID(domain.PresentationInstance),
		DisplayName:            domain.DisplayName, PublicPath: domain.PublicPath,
	}
}

func observedBrokerDomainPage(
	domains []catalogproto.RegisteredDomain,
) *[]catalogproto.ObservedDomain {
	observed := make([]catalogproto.ObservedDomain, len(domains))
	for index := range domains {
		managed := domains[index]
		observed[index] = catalogproto.ObservedDomain{
			ObservedID: brokerObservedDomainID(managed.DomainID),
			Managed:    &managed,
		}
	}
	sort.Slice(observed, func(i, j int) bool { return observed[i].ObservedID < observed[j].ObservedID })
	return &observed
}

func brokerObservedDomainID[T ~string](identifier T) catalogproto.ObservedDomainID {
	id, err := catalogproto.EncodeObservedDomainID(string(identifier))
	if err != nil {
		panic(err)
	}
	return id
}

func TestProtocolDomainRegistrationRejectsUnknownAccessMode(t *testing.T) {
	t.Parallel()
	if _, err := protocolDomainRegistration(catalog.FileProviderDomain{}); err == nil {
		t.Fatal("protocolDomainRegistration accepted an unknown access mode")
	}
}
