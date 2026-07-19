package convergence

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
)

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

type fakeTimer struct {
	at time.Time
	c  chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(delay time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if delay <= 0 {
		ch <- c.now
		return ch
	}
	c.timers = append(c.timers, fakeTimer{at: c.now.Add(delay), c: ch})
	return ch
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	now := c.now
	ready := make([]fakeTimer, 0, len(c.timers))
	remaining := c.timers[:0]
	for _, timer := range c.timers {
		if timer.at.After(now) {
			remaining = append(remaining, timer)
		} else {
			ready = append(ready, timer)
		}
	}
	c.timers = remaining
	c.mu.Unlock()
	for _, timer := range ready {
		timer.c <- now
	}
}

type fakeResolver struct {
	mu          sync.Mutex
	resolutions []Resolution
	applicable  map[TenantID]map[SourceAuthorityID]ChangeSet
	affected    map[ChangeID]map[TenantID]struct{}
	changes     []ChangeSet
}

func (r *fakeResolver) ResolveAffected(_ context.Context, change ChangeSet) ([]Resolution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.changes = append(r.changes, cloneChange(change))
	resolutions := cloneResolutions(r.resolutions)
	if targets, targeted := r.affected[change.ChangeID]; targeted {
		filtered := resolutions[:0]
		for _, resolution := range resolutions {
			if _, ok := targets[resolution.Tenant]; ok {
				filtered = append(filtered, resolution)
			}
		}
		resolutions = filtered
	}
	if r.applicable == nil {
		r.applicable = make(map[TenantID]map[SourceAuthorityID]ChangeSet)
	}
	for index := range resolutions {
		resolutions[index].Applicable = cloneChange(change)
		if r.applicable[resolutions[index].Tenant] == nil {
			r.applicable[resolutions[index].Tenant] = make(map[SourceAuthorityID]ChangeSet)
		}
		r.applicable[resolutions[index].Tenant][change.SourceAuthority] = cloneChange(change)
		if resolutions[index].CatalogRevision == 0 {
			resolutions[index].CatalogRevision = CatalogRevision(change.SourceRevision)
		}
	}
	return resolutions, nil
}

func (r *fakeResolver) ResolveTenant(_ context.Context, tenant TenantID, authority SourceAuthorityID) (Resolution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, resolution := range r.resolutions {
		if resolution.Tenant == tenant {
			resolved := cloneResolution(resolution)
			resolved.Applicable = cloneChange(r.applicable[tenant][authority])
			if resolved.Applicable.SourceAuthority == "" {
				return Resolution{}, fmt.Errorf("tenant %s authority %s not found", tenant, authority)
			}
			if resolved.CatalogRevision == 0 {
				resolved.CatalogRevision = CatalogRevision(resolved.Applicable.SourceRevision)
			}
			return resolved, nil
		}
	}
	return Resolution{}, fmt.Errorf("tenant %s not found", tenant)
}

func (r *fakeResolver) setBytes(index int, value string) {
	r.mu.Lock()
	r.resolutions[index].Effective[0].Bytes = []byte(value)
	r.mu.Unlock()
}

func (r *fakeResolver) setAffected(change ChangeSet, tenants ...TenantID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.affected == nil {
		r.affected = make(map[ChangeID]map[TenantID]struct{})
	}
	targets := make(map[TenantID]struct{}, len(tenants))
	for _, tenant := range tenants {
		targets[tenant] = struct{}{}
	}
	r.affected[change.ChangeID] = targets
}

func (r *fakeResolver) appliedChanges() []ChangeSet {
	r.mu.Lock()
	defer r.mu.Unlock()
	changes := make([]ChangeSet, len(r.changes))
	for index, change := range r.changes {
		changes[index] = cloneChange(change)
	}
	return changes
}

type memoryStore struct {
	mu     sync.Mutex
	state  State
	outbox []causal.OutboxBatch
}

func (s *memoryStore) ClaimOutbox(context.Context) (*causal.OutboxBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.outbox) == 0 {
		return nil, nil
	}
	batch := s.outbox[0]
	batch.Change = cloneChange(batch.Change)
	batch.Commits = append([]causal.CatalogCommit(nil), batch.Commits...)
	return &batch, nil
}

func (s *memoryStore) SettleOutbox(_ context.Context, change causal.ChangeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.outbox) == 0 || s.outbox[0].Change.ChangeID != change {
		return errors.New("memory store: unexpected outbox settlement")
	}
	s.outbox = s.outbox[1:]
	return nil
}

func (s *memoryStore) Load(context.Context) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneState(s.state), nil
}

func (s *memoryStore) Save(_ context.Context, state State) error {
	s.mu.Lock()
	s.state = cloneState(state)
	s.mu.Unlock()
	return nil
}

type fakeNotifier struct {
	mu            sync.Mutex
	notifications []Notification
	hook          func(context.Context, Notification) (Delivery, error)
}

func (n *fakeNotifier) Notify(ctx context.Context, notification Notification) (Delivery, error) {
	n.mu.Lock()
	n.notifications = append(n.notifications, cloneNotification(notification))
	hook := n.hook
	n.mu.Unlock()
	if hook != nil {
		return hook(ctx, notification)
	}
	return DeliveryAccepted, nil
}

func (n *fakeNotifier) calls() []Notification {
	n.mu.Lock()
	defer n.mu.Unlock()
	calls := make([]Notification, len(n.notifications))
	for index, notification := range n.notifications {
		calls[index] = cloneNotification(notification)
	}
	return calls
}

type fixture struct {
	engine   *Engine
	resolver *fakeResolver
	notifier *fakeNotifier
	store    *memoryStore
	clock    *fakeClock
}

func newFixture(t *testing.T, resolutions []Resolution) *fixture {
	t.Helper()
	resolver := &fakeResolver{resolutions: cloneResolutions(resolutions)}
	notifier := &fakeNotifier{}
	store := &memoryStore{}
	clock := newFakeClock()
	engine, err := New(t.Context(), Config{Resolver: resolver, Notifier: notifier, Persistence: store, Clock: clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := engine.Close(ctx); err != nil && !errors.Is(err, ErrClosed) {
			t.Errorf("Close: %v", err)
		}
	})
	return &fixture{engine: engine, resolver: resolver, notifier: notifier, store: store, clock: clock}
}

func fleet(count, live, materialized int) []Resolution {
	resolutions := make([]Resolution, count)
	for index := range count {
		resolution := Resolution{
			Tenant:     TenantID(fmt.Sprintf("tenant-%03d", index)),
			Domain:     DomainID(fmt.Sprintf("domain-%03d", index)),
			Generation: 1,
			Registered: true,
			Effective:  []EffectiveValue{{Key: "config", Bytes: []byte("v1")}},
		}
		if index < live {
			resolution.LiveLeases = 1
		}
		if index < materialized {
			resolution.MaterializedInterests = 1
		}
		resolutions[index] = resolution
	}
	return resolutions
}

func TestSourceChangeRejectsDuplicateRegisteredDomainsForTenant(t *testing.T) {
	resolutions := fleet(2, 2, 2)
	resolutions[1].Tenant = resolutions[0].Tenant
	fixture := newFixture(t, resolutions)
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); !errors.Is(err, ErrInvalidResolution) {
		t.Fatalf("publish duplicate tenant domains = %v, want ErrInvalidResolution", err)
	}
}

func changeID(value uint64) ChangeID {
	var id ChangeID
	binary.BigEndian.PutUint64(id[8:], value)
	return id
}

func operationID(value uint64) OperationID {
	var id OperationID
	binary.BigEndian.PutUint64(id[8:], value)
	return id
}

func semanticChange(value uint64) ChangeSet {
	return ChangeSet{
		SourceAuthority: "test-source",
		SourceRevision:  Revision(value),
		ChangeID:        changeID(value),
		OperationID:     operationID(value),
		Cause:           CauseExternalUnattributed,
		AffectedKeys:    []LogicalKey{"config"},
	}
}

func pending(state State) []Ack {
	acks := make([]Ack, 0, MaxAwaiting)
	for id, domain := range state.Domains {
		if domain.Pending != nil {
			notification := domain.Pending.Notification
			acks = append(acks, Ack{
				Domain:          id,
				Generation:      notification.Generation,
				Revision:        notification.Revision,
				SourceAuthority: notification.SourceAuthority,
				SourceRevision:  notification.SourceRevision,
				CatalogRevision: notification.CatalogRevision,
				ChangeID:        notification.ChangeID,
				OperationID:     notification.OperationID,
			})
		}
	}
	slices.SortFunc(acks, func(a, b Ack) int { return compareString(string(a.Domain), string(b.Domain)) })
	return acks
}

func ackFor(notification Notification) Ack {
	return Ack{
		Domain:          notification.Domain,
		Generation:      notification.Generation,
		Revision:        notification.Revision,
		SourceAuthority: notification.SourceAuthority,
		SourceRevision:  notification.SourceRevision,
		CatalogRevision: notification.CatalogRevision,
		ChangeID:        notification.ChangeID,
		OperationID:     notification.OperationID,
	}
}

func drain(t *testing.T, engine *Engine) {
	t.Helper()
	for {
		state, err := engine.Snapshot(t.Context())
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		acks := pending(state)
		if len(acks) == 0 {
			return
		}
		if len(acks) > MaxAwaiting {
			t.Fatalf("awaiting = %d, want <= %d", len(acks), MaxAwaiting)
		}
		for _, ack := range acks {
			if err := engine.Acknowledge(t.Context(), ack); err != nil {
				t.Fatalf("Acknowledge: %v", err)
			}
		}
	}
}

func tenantRequirement(t *testing.T, engine *Engine, tenant TenantID) PreparationRequirement {
	t.Helper()
	state, err := engine.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot tenant requirements: %v", err)
	}
	for _, domain := range state.Domains {
		if domain.Tenant == tenant {
			return PreparationRequirement{
				Tenant: tenant, Domain: domain.Domain, Generation: domain.Generation,
				SourceAuthority: domain.DesiredChange.SourceAuthority, SourceRevision: domain.DesiredChange.SourceRevision,
				CatalogRevision: domain.CatalogRevision, ChangeID: domain.DesiredChange.ChangeID, OperationID: domain.DesiredChange.OperationID,
			}
		}
	}
	t.Fatalf("tenant %q has no convergence domain", tenant)
	return PreparationRequirement{}
}

func requestCurrentTenant(t *testing.T, engine *Engine, tenant TenantID) (Preparation, error) {
	t.Helper()
	return engine.RequestTenant(t.Context(), tenantRequirement(t, engine, tenant))
}

func prepareCurrentTenant(t *testing.T, engine *Engine, tenant TenantID) (ObservationProof, error) {
	t.Helper()
	return engine.PrepareTenant(t.Context(), tenantRequirement(t, engine, tenant))
}

func TestReportedFleetChangeTargetsOnlyNineActiveDomainsAtTwoPending(t *testing.T) {
	fixture := newFixture(t, fleet(14, 9, 9))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	state, err := fixture.engine.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got := len(pending(state)); got != MaxAwaiting {
		t.Fatalf("pending = %d, want %d", got, MaxAwaiting)
	}
	if got := len(state.Domains); got != 14 {
		t.Fatalf("durable domains = %d, want 14", got)
	}
	for index := 9; index < 14; index++ {
		domain := state.Domains[DomainID(fmt.Sprintf("domain-%03d", index))]
		if !domain.Stale() || domain.Notified != 0 {
			t.Fatalf("inactive domain %d = %#v", index, domain)
		}
	}
	drain(t, fixture.engine)
	if got := len(fixture.notifier.calls()); got != 9 {
		t.Fatalf("notifications = %d, want 9", got)
	}
}

func TestMaterializedLiveDemandAndOneOnDemandTenant(t *testing.T) {
	fixture := newFixture(t, fleet(100, 10, 3))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	drain(t, fixture.engine)
	if got := len(fixture.notifier.calls()); got != 3 {
		t.Fatalf("automatic notifications = %d, want 3", got)
	}
	preparation, err := requestCurrentTenant(t, fixture.engine, "tenant-050")
	if err != nil {
		t.Fatalf("RequestTenant: %v", err)
	}
	calls := fixture.notifier.calls()
	if len(calls) != 4 || calls[3].Tenant != "tenant-050" {
		t.Fatalf("notifications after prepare = %#v", calls)
	}
	if err := fixture.engine.Acknowledge(t.Context(), ackFor(calls[3])); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.engine.AwaitObserved(t.Context(), preparation); err != nil {
		t.Fatalf("AwaitObserved: %v", err)
	}
}

func TestRevisionSpacesRemainDistinctAndAckIsExact(t *testing.T) {
	resolutions := fleet(1, 1, 1)
	resolutions[0].CatalogRevision = 41
	fixture := newFixture(t, resolutions)
	change := semanticChange(7)
	if err := fixture.engine.publishForTest(t.Context(), change); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	notification := fixture.notifier.calls()[0]
	if notification.SourceRevision != 7 || notification.CatalogRevision != 41 || notification.Revision != 1 {
		t.Fatalf("notification revision spaces collapsed: %#v", notification)
	}
	wrong := ackFor(notification)
	wrong.CatalogRevision--
	if err := fixture.engine.Acknowledge(t.Context(), wrong); !errors.Is(err, ErrUnexpectedAck) {
		t.Fatalf("wrong catalog revision ack = %v, want ErrUnexpectedAck", err)
	}
	if err := fixture.engine.Acknowledge(t.Context(), ackFor(notification)); err != nil {
		t.Fatalf("exact ack: %v", err)
	}
	requirement := tenantRequirement(t, fixture.engine, notification.Tenant)
	proof, err := fixture.engine.PrepareTenant(t.Context(), requirement)
	if err != nil {
		t.Fatalf("PrepareTenant: %v", err)
	}
	if proof.Requested.SourceRevision != 7 || proof.Requested.CatalogRevision != 41 || proof.Observed.Revision != 1 {
		t.Fatalf("proof revision spaces collapsed: %#v", proof)
	}
	requirement.SourceRevision = 8
	if _, err := fixture.engine.RequestTenant(t.Context(), requirement); !errors.Is(err, ErrInvalidResolution) {
		t.Fatalf("unpublished source preparation = %v, want ErrInvalidResolution", err)
	}
}

func TestPreparationRejectsCausalMismatchBeforeNotificationWork(t *testing.T) {
	fixture := newFixture(t, fleet(1, 0, 0))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatal(err)
	}
	requirement := tenantRequirement(t, fixture.engine, "tenant-000")
	requirement.OperationID = operationID(99)
	if _, err := fixture.engine.RequestTenant(t.Context(), requirement); !errors.Is(err, ErrInvalidResolution) {
		t.Fatalf("RequestTenant(mismatch) = %v, want ErrInvalidResolution", err)
	}
	if calls := fixture.notifier.calls(); len(calls) != 0 {
		t.Fatalf("mismatched requirement notified domains: %#v", calls)
	}
}

func TestPreparationRetainsRequestedAuthorityAcrossIndependentSourceHeads(t *testing.T) {
	resolutions := fleet(1, 1, 1)
	resolutions[0].CatalogRevision = 41
	fixture := newFixture(t, resolutions)
	first := semanticChange(7)
	first.SourceAuthority = "source-a"
	if err := fixture.engine.publishForTest(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	firstNotification := fixture.notifier.calls()[0]
	if err := fixture.engine.Acknowledge(t.Context(), ackFor(firstNotification)); err != nil {
		t.Fatal(err)
	}
	firstRequirement := tenantRequirement(t, fixture.engine, firstNotification.Tenant)
	fixture.resolver.setBytes(0, "v2")
	second := semanticChange(1)
	second.SourceAuthority = "source-b"
	if err := fixture.engine.publishForTest(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	preparation, err := fixture.engine.RequestTenant(t.Context(), firstRequirement)
	if err != nil {
		t.Fatalf("RequestTenant(first authority): %v", err)
	}
	if preparation.Revision != 2 || preparation.SourceAuthority != "source-a" || preparation.SourceRevision != 7 ||
		preparation.CatalogRevision != 41 || preparation.ChangeID != first.ChangeID || preparation.OperationID != first.OperationID {
		t.Fatalf("derived preparation = %+v", preparation)
	}
}

func TestPrepareRetainsTenantApplicableCommitAfterGlobalJournalCompaction(t *testing.T) {
	fixture := newFixture(t, fleet(2, 0, 0))
	first := semanticChange(1)
	if err := fixture.engine.publishForTest(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	bRequirement := tenantRequirement(t, fixture.engine, "tenant-001")
	for revision := uint64(2); revision <= MaxAppliedChanges+5; revision++ {
		change := semanticChange(revision)
		fixture.resolver.setAffected(change, "tenant-000")
		if err := fixture.engine.publishForTest(t.Context(), change); err != nil {
			t.Fatalf("publish A-only revision %d: %v", revision, err)
		}
	}
	state, err := fixture.engine.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, retained := state.Changes[first.ChangeID]; retained {
		t.Fatal("tenant B applicable commit remained in the bounded global change journal")
	}
	if state.SourceHeads[first.SourceAuthority] != MaxAppliedChanges+5 {
		t.Fatalf("global source head = %d, want %d", state.SourceHeads[first.SourceAuthority], MaxAppliedChanges+5)
	}
	preparation, err := fixture.engine.RequestTenant(t.Context(), bRequirement)
	if err != nil {
		t.Fatalf("RequestTenant(B after compaction): %v", err)
	}
	if preparation.SourceRevision != 1 || preparation.ChangeID != first.ChangeID || preparation.OperationID != first.OperationID {
		t.Fatalf("tenant B preparation = %+v, want retained revision-1 tuple", preparation)
	}
}

func TestUnchangedEffectiveBytesProduceNoNotification(t *testing.T) {
	fixture := newFixture(t, fleet(1, 1, 1))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatal(err)
	}
	drain(t, fixture.engine)
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(2)); err != nil {
		t.Fatal(err)
	}
	if got := len(fixture.notifier.calls()); got != 1 {
		t.Fatalf("notifications = %d, want 1", got)
	}
}

func TestOriginAcknowledgedWriteSuppressesEcho(t *testing.T) {
	fixture := newFixture(t, fleet(1, 1, 1))
	change := semanticChange(1)
	change.Cause = CauseProviderMutation
	change.Origin = "domain-000"
	change.OriginGeneration = 1
	if err := fixture.engine.publishForTest(t.Context(), change); err != nil {
		t.Fatal(err)
	}
	if got := len(fixture.notifier.calls()); got != 0 {
		t.Fatalf("notifications = %d, want 0", got)
	}
	state, err := fixture.engine.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if domain := state.Domains["domain-000"]; domain.Stale() || domain.Observed != domain.Desired {
		t.Fatalf("origin state = %#v", domain)
	}
}

func TestCausalMetadataSurvivesResolveNotifyAndAcknowledge(t *testing.T) {
	fixture := newFixture(t, fleet(1, 1, 1))
	change := semanticChange(7)
	change.Cause = CauseDaemonWrite
	change.AffectedKeys = []LogicalKey{"config", "settings"}
	if err := fixture.engine.publishForTest(t.Context(), change); err != nil {
		t.Fatal(err)
	}
	resolved := fixture.resolver.appliedChanges()
	if len(resolved) != 1 || !equalChange(resolved[0], change) {
		t.Fatalf("resolver causal input = %#v", resolved)
	}
	calls := fixture.notifier.calls()
	if len(calls) != 1 {
		t.Fatalf("notifications = %d, want 1", len(calls))
	}
	notification := calls[0]
	if notification.SourceRevision != change.SourceRevision || notification.ChangeID != change.ChangeID ||
		notification.OperationID != change.OperationID || notification.Cause != change.Cause ||
		notification.Origin != change.Origin || !slices.Equal(notification.AffectedKeys, change.AffectedKeys) {
		t.Fatalf("notification lost causal metadata: %#v", notification)
	}
	ack := ackFor(notification)
	if err := fixture.engine.Acknowledge(t.Context(), ack); err != nil {
		t.Fatal(err)
	}
	state, err := fixture.engine.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	domain := state.Domains[notification.Domain]
	if domain.Observed != notification.Revision || !equalChange(domain.ObservedChange, change) {
		t.Fatalf("observed causal state = %#v", domain)
	}
}

func TestThousandWritesCollapseToLatestRevision(t *testing.T) {
	fixture := newFixture(t, fleet(1, 1, 1))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatal(err)
	}
	first := fixture.notifier.calls()[0]
	for revision := uint64(2); revision <= 1000; revision++ {
		fixture.resolver.setBytes(0, fmt.Sprintf("v%d", revision))
		if err := fixture.engine.publishForTest(t.Context(), semanticChange(revision)); err != nil {
			t.Fatalf("Apply %d: %v", revision, err)
		}
	}
	if got := len(fixture.notifier.calls()); got != 1 {
		t.Fatalf("pre-ack notifications = %d, want 1", got)
	}
	state, err := fixture.engine.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := len(state.Changes); got != MaxAppliedChanges || state.DedupFloors["test-source"] == 0 {
		t.Fatalf("dedup journal = %d entries at floors %+v", got, state.DedupFloors)
	}
	resolvedBefore := len(fixture.resolver.appliedChanges())
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatalf("compacted duplicate: %v", err)
	}
	if got := len(fixture.resolver.appliedChanges()); got != resolvedBefore {
		t.Fatalf("compacted duplicate reached resolver: %d calls, want %d", got, resolvedBefore)
	}
	if err := fixture.engine.Acknowledge(t.Context(), ackFor(first)); err != nil {
		t.Fatal(err)
	}
	calls := fixture.notifier.calls()
	if len(calls) != 2 || calls[1].Revision != 1000 {
		t.Fatalf("coalesced notifications = %#v", calls)
	}
}

func TestAcknowledgementStopsRelaunch(t *testing.T) {
	fixture := newFixture(t, fleet(1, 1, 1))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatal(err)
	}
	drain(t, fixture.engine)
	fixture.clock.Advance(2 * time.Minute)
	if err := fixture.engine.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := len(fixture.notifier.calls()); got != 1 {
		t.Fatalf("notifications = %d, want 1", got)
	}
}

func TestNoAckQuarantinesWithoutBlockingOtherDomains(t *testing.T) {
	fixture := newFixture(t, fleet(3, 3, 3))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(AckTimeout)
	if err := fixture.engine.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	calls := fixture.notifier.calls()
	if len(calls) != 3 || calls[2].Domain != "domain-002" {
		t.Fatalf("notifications = %#v", calls)
	}
	state, err := fixture.engine.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []DomainID{"domain-000", "domain-001"} {
		if state.Domains[id].Quarantine == nil {
			t.Fatalf("domain %s was not quarantined", id)
		}
	}
	if state.Domains["domain-002"].Pending == nil {
		t.Fatal("third domain did not use the released fleet slot")
	}
}

func TestTimeoutBackoffMintsNewRevisionWithoutReplayingNotificationIdentity(t *testing.T) {
	fixture := newFixture(t, fleet(1, 1, 1))
	change := semanticChange(1)
	change.Cause = CauseMigration
	if err := fixture.engine.publishForTest(t.Context(), change); err != nil {
		t.Fatal(err)
	}
	first := fixture.notifier.calls()[0]
	fixture.clock.Advance(AckTimeout)
	if err := fixture.engine.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, err := fixture.engine.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	quarantine := state.Domains[first.Domain].Quarantine
	if quarantine == nil || quarantine.Notification.ChangeID != change.ChangeID ||
		quarantine.Notification.OperationID != change.OperationID || quarantine.Notification.Cause != change.Cause ||
		!slices.Equal(quarantine.Notification.AffectedKeys, change.AffectedKeys) {
		t.Fatalf("quarantine lost causal notification: %#v", quarantine)
	}
	if _, err := prepareCurrentTenant(t, fixture.engine, first.Tenant); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("PrepareTenant during backoff = %v, want quarantine", err)
	}
	if got := len(fixture.notifier.calls()); got != 1 {
		t.Fatalf("prepare bypassed quarantine: %d notifications", got)
	}
	fixture.clock.Advance(QuarantineBackoff)
	if err := fixture.engine.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	calls := fixture.notifier.calls()
	if len(calls) != 2 {
		t.Fatalf("recovery notifications = %d, want 2", len(calls))
	}
	recovery := calls[1]
	if recovery.Revision <= first.Revision || recovery.Generation != first.Generation ||
		recovery.ChangeID != first.ChangeID || recovery.OperationID != first.OperationID || recovery.Cause != first.Cause ||
		recovery.SourceRevision != first.SourceRevision || recovery.CatalogRevision != first.CatalogRevision ||
		!slices.Equal(recovery.AffectedKeys, first.AffectedKeys) {
		t.Fatalf("recovery notification = %#v after %#v", recovery, first)
	}
	preparation, err := requestCurrentTenant(t, fixture.engine, first.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Revision != preparation.Revision {
		t.Fatalf("preparation revision = %d, recovery = %d", preparation.Revision, recovery.Revision)
	}
	if err := fixture.engine.Acknowledge(t.Context(), ackFor(recovery)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.engine.AwaitObserved(t.Context(), preparation); err != nil {
		t.Fatal(err)
	}
	state, err = fixture.engine.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	domain := state.Domains[recovery.Domain]
	if domain.Stale() || domain.ObservedChange.Cause != change.Cause || domain.ObservedChange.ChangeID != recovery.ChangeID {
		t.Fatalf("recovery acknowledgement state = %#v", domain)
	}
}

func TestCancellationPersistsUnknownDeliveryAndRestartNeverDuplicates(t *testing.T) {
	resolver := &fakeResolver{resolutions: fleet(1, 1, 1)}
	store := &memoryStore{}
	clock := newFakeClock()
	started := make(chan struct{})
	notifier := &fakeNotifier{hook: func(ctx context.Context, _ Notification) (Delivery, error) {
		close(started)
		<-ctx.Done()
		return DeliveryUnknown, ctx.Err()
	}}
	engine, err := New(t.Context(), Config{Resolver: resolver, Notifier: notifier, Persistence: store, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	applyDone := make(chan error, 1)
	go func() { applyDone <- engine.publishForTest(ctx, semanticChange(1)) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("notifier did not start")
	}
	if err := <-applyDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Apply = %v, want deadline", err)
	}
	state, err := engine.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	acks := pending(state)
	if len(acks) != 1 {
		t.Fatalf("pending after cancellation = %#v", acks)
	}
	engine.Cancel()
	if err := engine.Wait(t.Context()); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	restartedNotifier := &fakeNotifier{}
	restarted, err := New(t.Context(), Config{Resolver: resolver, Notifier: restartedNotifier, Persistence: store, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := restarted.Close(t.Context()); err != nil {
			t.Errorf("Close restarted engine: %v", err)
		}
	}()
	if got := len(restartedNotifier.calls()); got != 0 {
		t.Fatalf("restart duplicated notification: %d", got)
	}
	clock.Advance(AckTimeout)
	if err := restarted.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	restartedState, err := restarted.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if restartedState.Domains[acks[0].Domain].Quarantine == nil {
		t.Fatal("persisted unknown delivery did not quarantine after restart")
	}
	clock.Advance(QuarantineBackoff)
	if err := restarted.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	calls := restartedNotifier.calls()
	if len(calls) != 1 || calls[0].Revision <= acks[0].Revision || calls[0].Generation != acks[0].Generation {
		t.Fatalf("restart recovery notification = %#v", calls)
	}
	preparation, err := requestCurrentTenant(t, restarted, "tenant-000")
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Acknowledge(t.Context(), ackFor(calls[0])); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.AwaitObserved(t.Context(), preparation); err != nil {
		t.Fatal(err)
	}
}

func TestInactivePrepareBlocksUntilAcknowledged(t *testing.T) {
	fixture := newFixture(t, fleet(1, 0, 0))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatal(err)
	}
	notified := make(chan Notification, 1)
	fixture.notifier.hook = func(_ context.Context, notification Notification) (Delivery, error) {
		notified <- notification
		return DeliveryAccepted, nil
	}
	result := make(chan struct {
		proof ObservationProof
		err   error
	}, 1)
	go func() {
		proof, err := prepareCurrentTenant(t, fixture.engine, "tenant-000")
		result <- struct {
			proof ObservationProof
			err   error
		}{proof: proof, err: err}
	}()
	notification := <-notified
	select {
	case got := <-result:
		t.Fatalf("prepare returned before ack: %#v", got)
	default:
	}
	if err := fixture.engine.Acknowledge(t.Context(), ackFor(notification)); err != nil {
		t.Fatal(err)
	}
	got := <-result
	if got.err != nil || got.proof.Requested.Revision != notification.Revision || got.proof.Observed.Revision != notification.Revision {
		t.Fatalf("prepare result = %#v", got)
	}
}

func TestMultipleWaitersCoalesceAndCancellationIsIsolated(t *testing.T) {
	fixture := newFixture(t, fleet(1, 0, 0))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatal(err)
	}
	preparation, err := requestCurrentTenant(t, fixture.engine, "tenant-000")
	if err != nil {
		t.Fatal(err)
	}
	canceledCtx, cancel := context.WithCancel(t.Context())
	canceled := make(chan error, 1)
	surviving := make(chan error, 1)
	go func() {
		_, err := fixture.engine.AwaitObserved(canceledCtx, preparation)
		canceled <- err
	}()
	go func() {
		_, err := fixture.engine.AwaitObserved(t.Context(), preparation)
		surviving <- err
	}()
	cancel()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter = %v", err)
	}
	select {
	case err := <-surviving:
		t.Fatalf("surviving waiter returned before ack: %v", err)
	default:
	}
	calls := fixture.notifier.calls()
	if len(calls) != 1 {
		t.Fatalf("coalesced notifications = %d, want 1", len(calls))
	}
	if err := fixture.engine.Acknowledge(t.Context(), ackFor(calls[0])); err != nil {
		t.Fatal(err)
	}
	if err := <-surviving; err != nil {
		t.Fatalf("surviving waiter = %v", err)
	}
}

func TestPendingWaiterFailsWhenDeliveryIsQuarantined(t *testing.T) {
	fixture := newFixture(t, fleet(1, 0, 0))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatal(err)
	}
	preparation, err := requestCurrentTenant(t, fixture.engine, "tenant-000")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := fixture.engine.AwaitObserved(t.Context(), preparation)
		result <- err
	}()
	fixture.clock.Advance(AckTimeout)
	if err := fixture.engine.Tick(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrQuarantined) {
		t.Fatalf("waiter = %v, want quarantine", err)
	}
}

func TestNewerDesiredSatisfiesOlderWaiterOnlyWhenObserved(t *testing.T) {
	fixture := newFixture(t, fleet(3, 3, 3))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(1)); err != nil {
		t.Fatal(err)
	}
	preparation, err := requestCurrentTenant(t, fixture.engine, "tenant-002")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan ObservationProof, 1)
	go func() {
		proof, err := fixture.engine.AwaitObserved(t.Context(), preparation)
		if err != nil {
			t.Errorf("AwaitObserved: %v", err)
			return
		}
		result <- proof
	}()
	fixture.resolver.setBytes(2, "v2")
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(2)); err != nil {
		t.Fatal(err)
	}
	select {
	case proof := <-result:
		t.Fatalf("newer desired revision was treated as observed: %#v", proof)
	default:
	}
	state, err := fixture.engine.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, ack := range pending(state) {
		if err := fixture.engine.Acknowledge(t.Context(), ack); err != nil {
			t.Fatal(err)
		}
	}
	calls := fixture.notifier.calls()
	var latest Notification
	for _, notification := range calls {
		if notification.Domain == "domain-002" {
			latest = notification
		}
	}
	if latest.Revision <= preparation.Revision {
		t.Fatalf("latest notification = %#v, requested %#v", latest, preparation)
	}
	select {
	case proof := <-result:
		t.Fatalf("newer notification was treated as observed: %#v", proof)
	default:
	}
	if err := fixture.engine.Acknowledge(t.Context(), ackFor(latest)); err != nil {
		t.Fatal(err)
	}
	proof := <-result
	if proof.Requested != preparation || proof.Observed.Revision != latest.Revision || proof.Observed.Revision <= proof.Requested.Revision {
		t.Fatalf("observation proof = %#v", proof)
	}
}

func TestRejectsOutOfOrderSourceRevisionBeforeResolution(t *testing.T) {
	fixture := newFixture(t, fleet(1, 1, 1))
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(10)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(9)); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("out-of-order apply = %v", err)
	}
	if got := len(fixture.resolver.appliedChanges()); got != 1 {
		t.Fatalf("resolver calls = %d, want 1", got)
	}
	if err := fixture.engine.publishForTest(t.Context(), semanticChange(10)); err != nil {
		t.Fatalf("known duplicate: %v", err)
	}
}

func TestSourceOrderingAndDeduplicationAreAuthorityScoped(t *testing.T) {
	fixture := newFixture(t, fleet(1, 0, 0))
	first := semanticChange(10)
	first.SourceAuthority = "source-a"
	if err := fixture.engine.publishForTest(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	second := semanticChange(1)
	second.SourceAuthority = "source-b"
	second.ChangeID = changeID(101)
	second.OperationID = operationID(101)
	if err := fixture.engine.publishForTest(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	state, err := fixture.engine.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if state.SourceHeads["source-a"] != 10 || state.SourceHeads["source-b"] != 1 {
		t.Fatalf("source heads = %+v", state.SourceHeads)
	}
	stale := semanticChange(9)
	stale.SourceAuthority = "source-a"
	stale.ChangeID = changeID(109)
	stale.OperationID = operationID(109)
	if err := fixture.engine.publishForTest(t.Context(), stale); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("source-a stale revision = %v", err)
	}
	next := semanticChange(2)
	next.SourceAuthority = "source-b"
	next.ChangeID = changeID(102)
	next.OperationID = operationID(102)
	if err := fixture.engine.publishForTest(t.Context(), next); err != nil {
		t.Fatalf("source-b successor: %v", err)
	}
}

func TestEffectiveFingerprintIsOrderIndependentAndLengthDelimited(t *testing.T) {
	first, err := EffectiveFingerprint([]EffectiveValue{{Key: "a", Bytes: []byte("bc")}, {Key: "ab", Bytes: []byte("c")}})
	if err != nil {
		t.Fatal(err)
	}
	ordered, err := EffectiveFingerprint([]EffectiveValue{{Key: "ab", Bytes: []byte("c")}, {Key: "a", Bytes: []byte("bc")}})
	if err != nil {
		t.Fatal(err)
	}
	other, err := EffectiveFingerprint([]EffectiveValue{{Key: "a", Bytes: []byte("b")}, {Key: "ab", Bytes: []byte("cc")}})
	if err != nil {
		t.Fatal(err)
	}
	if first != ordered || first == other {
		t.Fatalf("fingerprints first=%x ordered=%x other=%x", first, ordered, other)
	}
}

func cloneResolutions(resolutions []Resolution) []Resolution {
	cloned := make([]Resolution, len(resolutions))
	for index, resolution := range resolutions {
		cloned[index] = cloneResolution(resolution)
	}
	return cloned
}

func cloneResolution(resolution Resolution) Resolution {
	resolution.Applicable = cloneChange(resolution.Applicable)
	resolution.Effective = append([]EffectiveValue(nil), resolution.Effective...)
	for index := range resolution.Effective {
		resolution.Effective[index].Bytes = append([]byte(nil), resolution.Effective[index].Bytes...)
	}
	return resolution
}
