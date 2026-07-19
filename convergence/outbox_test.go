package convergence

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestEngineStartupDrainsCatalogCommitOutbox(t *testing.T) {
	cat, tenant := catalogWithCommittedTenant(t)
	persistence, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatalf("NewCatalogPersistence: %v", err)
	}
	resolver := outboxResolver(tenant)
	notifier := &fakeNotifier{}
	engine, err := New(t.Context(), Config{
		Resolver: resolver, Notifier: notifier, Persistence: persistence, Clock: newFakeClock(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if calls := notifier.calls(); len(calls) != 1 || calls[0].CatalogRevision != 2 || calls[0].SourceRevision != 1 {
		t.Fatalf("startup notifications = %+v", calls)
	}
	if pending, err := cat.ClaimConvergenceOutbox(t.Context()); err != nil || pending != nil {
		t.Fatalf("settled outbox claim = %+v, %v", pending, err)
	}
	if err := engine.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestOutboxRecoveryNeverDuplicatesDispatchedNotification(t *testing.T) {
	cat, tenant := catalogWithCommittedTenant(t)
	persistence, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatalf("NewCatalogPersistence: %v", err)
	}
	resolver := outboxResolver(tenant)
	firstNotifier := &fakeNotifier{}
	settleFailure := errors.New("simulated crash after dispatch before outbox settlement")
	faulted := &failSettlePersistence{Persistence: persistence, err: settleFailure}
	if _, err := New(t.Context(), Config{
		Resolver: resolver, Notifier: firstNotifier, Persistence: faulted, Clock: newFakeClock(),
	}); !errors.Is(err, settleFailure) {
		t.Fatalf("faulted New = %v, want settle failure", err)
	}
	firstCalls := firstNotifier.calls()
	if len(firstCalls) != 1 {
		t.Fatalf("notifications before crash = %d, want 1", len(firstCalls))
	}

	restartedNotifier := &fakeNotifier{}
	restarted, err := New(t.Context(), Config{
		Resolver: resolver, Notifier: restartedNotifier, Persistence: persistence, Clock: newFakeClock(),
	})
	if err != nil {
		t.Fatalf("restart New: %v", err)
	}
	if calls := restartedNotifier.calls(); len(calls) != 0 {
		t.Fatalf("recovery duplicated notification: %+v", calls)
	}
	if pending, err := cat.ClaimConvergenceOutbox(t.Context()); err != nil || pending != nil {
		t.Fatalf("recovered outbox claim = %+v, %v", pending, err)
	}
	notification := firstCalls[0]
	if err := restarted.Acknowledge(t.Context(), Ack{
		Domain: notification.Domain, Generation: notification.Generation, Revision: notification.Revision,
		SourceAuthority: notification.SourceAuthority,
		SourceRevision:  notification.SourceRevision, CatalogRevision: notification.CatalogRevision,
		ChangeID: notification.ChangeID, OperationID: notification.OperationID,
	}); err != nil {
		t.Fatalf("Acknowledge recovered pending notification: %v", err)
	}
	state, err := restarted.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if domain := state.Domains[notification.Domain]; domain.Pending != nil || domain.Observed != notification.Revision {
		t.Fatalf("recovered acknowledgement state = %+v", domain)
	}
	if err := restarted.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestInterestOutboxTargetsOnlyExactDomainThroughAddRemoveAndAck(t *testing.T) {
	cat, tenant := catalogWithCommittedTenant(t)
	persistence, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatalf("NewCatalogPersistence: %v", err)
	}
	resolver := &fakeResolver{resolutions: []Resolution{
		{
			Tenant: TenantID(tenant), Domain: "outbox-domain", Generation: 1,
			CatalogRevision: 2, Registered: true, LiveLeases: 1, MaterializedInterests: 1,
			Effective: []EffectiveValue{{Key: "object", Bytes: []byte("interested")}},
		},
		{
			Tenant: "unrelated-tenant", Domain: "unrelated-domain", Generation: 1,
			CatalogRevision: 1, Registered: true, LiveLeases: 1, MaterializedInterests: 1,
			Effective: []EffectiveValue{{Key: "object", Bytes: []byte("unrelated")}},
		},
	}}
	notifier := &fakeNotifier{}
	engine, err := New(t.Context(), Config{
		Resolver: resolver, Notifier: notifier, Persistence: persistence, Clock: newFakeClock(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	first := requireNotification(t, notifier, 0, "outbox-domain")
	ackNotification(t, engine, first)

	root, err := cat.Root(t.Context(), tenant)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	interests, err := cat.Interests(t.Context(), tenant, root.ID)
	if err != nil || len(interests) != 1 {
		t.Fatalf("Interests = %+v, %v", interests, err)
	}
	removeOperation, err := catalog.NewMutationID()
	if err != nil {
		t.Fatalf("NewMutationID(remove): %v", err)
	}
	if _, err := cat.RemoveInterest(t.Context(), removeOperation, tenant, interests[0].ID); err != nil {
		t.Fatalf("RemoveInterest: %v", err)
	}
	resolver.mu.Lock()
	resolver.resolutions[0].CatalogRevision = 3
	resolver.resolutions[0].MaterializedInterests = 0
	resolver.resolutions[0].Effective[0].Bytes = []byte("removed")
	resolver.mu.Unlock()
	if err := engine.Drain(t.Context()); err != nil {
		t.Fatalf("Drain(remove): %v", err)
	}
	second := requireNotification(t, notifier, 1, "outbox-domain")
	if second.ChangeID == first.ChangeID || second.SourceRevision <= first.SourceRevision {
		t.Fatalf("remove notification did not advance exact cause: first=%+v second=%+v", first, second)
	}
	ackNotification(t, engine, second)
	if calls := notifier.calls(); len(calls) != 2 {
		t.Fatalf("exact-domain notifications = %+v", calls)
	}
	state, err := engine.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if _, exists := state.Domains["unrelated-domain"]; exists {
		t.Fatalf("unrelated domain entered convergence state: %+v", state.Domains["unrelated-domain"])
	}
	if pending, err := cat.ClaimConvergenceOutbox(t.Context()); err != nil || pending != nil {
		t.Fatalf("settled interest outbox = %+v, %v", pending, err)
	}
	if err := engine.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSharedSourceChangeAcrossFourteenTenantsPublishesOnceToNine(t *testing.T) {
	ctx := t.Context()
	cat, err := catalog.Open(ctx, filepath.Join(t.TempDir(), "shared-source.sqlite"))
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	const totalTenants = 14
	const changedTenants = 9
	tenantIDs := make([]catalog.TenantID, totalTenants)
	roots := make([]catalog.Object, totalTenants)
	for index := range totalTenants {
		tenantID, err := catalog.NewTenantID(fmt.Sprintf("shared-%02d", index))
		if err != nil {
			t.Fatalf("NewTenantID(%d): %v", index, err)
		}
		operation, err := catalog.NewMutationID()
		if err != nil {
			t.Fatalf("NewMutationID(create tenant %d): %v", index, err)
		}
		root, err := cat.CreateTenant(ctx, operation, tenantID, catalog.CaseSensitive, catalog.PresentFileProvider)
		if err != nil {
			t.Fatalf("CreateTenant(%d): %v", index, err)
		}
		tenantIDs[index], roots[index] = tenantID, root
	}
	change := semanticChange(77)
	change.Cause = CauseDaemonWrite
	change.AffectedKeys = []LogicalKey{"config:.claude.json"}
	targets := make([]causal.TenantID, changedTenants)
	for index := range changedTenants {
		targets[index] = causal.TenantID(tenantIDs[index])
	}
	for index := range changedTenants {
		operation, err := catalog.NewMutationID()
		if err != nil {
			t.Fatalf("NewMutationID(change %d): %v", index, err)
		}
		commitSharedDirectory(t, cat, tenantIDs[index], roots[index], operation, change, targets)
		if index < changedTenants-1 {
			batch, err := cat.ClaimConvergenceOutbox(ctx)
			if err != nil || batch != nil {
				t.Fatalf("partial shared batch after %d commits = %+v, %v", index+1, batch, err)
			}
		}
	}
	batch, err := cat.ClaimConvergenceOutbox(ctx)
	if err != nil {
		t.Fatalf("ClaimConvergenceOutbox(shared): %v", err)
	}
	if batch == nil || len(batch.Commits) != changedTenants || !equalChange(batch.Change, change) {
		t.Fatalf("shared outbox batch = %+v", batch)
	}
	resolutions := make([]Resolution, changedTenants)
	for index := range changedTenants {
		resolutions[index] = Resolution{
			Tenant: TenantID(tenantIDs[index]), Domain: DomainID(fmt.Sprintf("shared-domain-%02d", index)), Generation: 1,
			CatalogRevision: 2, Registered: true, LiveLeases: 1, MaterializedInterests: 1,
			Effective: []EffectiveValue{{Key: "config:.claude.json", Bytes: []byte("shared-value")}},
		}
	}
	persistence, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatalf("NewCatalogPersistence: %v", err)
	}
	notifier := &fakeNotifier{}
	engine, err := New(ctx, Config{
		Resolver: &fakeResolver{resolutions: resolutions}, Notifier: notifier,
		Persistence: persistence, Clock: newFakeClock(),
	})
	if err != nil {
		t.Fatalf("New(shared): %v", err)
	}
	for acknowledged := 0; acknowledged < changedTenants; acknowledged++ {
		notification := requireNotification(t, notifier, acknowledged, DomainID(fmt.Sprintf("shared-domain-%02d", acknowledged)))
		if notification.SourceRevision != change.SourceRevision || notification.ChangeID != change.ChangeID || notification.OperationID != change.OperationID {
			t.Fatalf("notification %d fragmented source identity: %+v", acknowledged, notification)
		}
		ackNotification(t, engine, notification)
	}
	if calls := notifier.calls(); len(calls) != changedTenants {
		t.Fatalf("shared source notifications = %d, want %d", len(calls), changedTenants)
	}
	if err := engine.Drain(ctx); err != nil {
		t.Fatalf("second Drain: %v", err)
	}
	if calls := notifier.calls(); len(calls) != changedTenants {
		t.Fatalf("shared source was published twice: %d notifications", len(calls))
	}
	state, err := engine.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot(shared): %v", err)
	}
	if len(state.Changes) != 1 || state.SourceHeads[change.SourceAuthority] != change.SourceRevision || len(state.Domains) != changedTenants {
		t.Fatalf("shared convergence state = %+v", state)
	}
	for index := changedTenants; index < totalTenants; index++ {
		if _, exists := state.Domains[DomainID(fmt.Sprintf("shared-domain-%02d", index))]; exists {
			t.Fatalf("unchanged tenant %d entered convergence state", index)
		}
	}
	if err := engine.Close(ctx); err != nil {
		t.Fatalf("Close(shared): %v", err)
	}
}

func commitSharedDirectory(
	t *testing.T,
	cat *catalog.Catalog,
	tenantID catalog.TenantID,
	root catalog.Object,
	operation catalog.MutationID,
	change ChangeSet,
	targets []causal.TenantID,
) {
	t.Helper()
	intent := catalog.MutationIntent{
		SourceID: "shared-source",
		Origin: catalog.CausalOrigin{
			Change: &change, Targets: append([]causal.TenantID(nil), targets...),
		},
		Create: &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: root.ID, Name: "shared", Kind: catalog.KindDirectory,
			Visibility: catalog.Visibility{FileProvider: true},
		}},
	}
	prepared, err := cat.BeginMutation(t.Context(), operation, tenantID, root.Revision, intent)
	if err != nil {
		t.Fatalf("BeginMutation(%s): %v", tenantID, err)
	}
	owner, err := catalog.NewMutationOwnerID()
	if err != nil {
		t.Fatalf("NewMutationOwnerID: %v", err)
	}
	claimed, err := cat.ClaimMutation(t.Context(), operation, owner)
	if err != nil {
		t.Fatalf("ClaimMutation(%s): %v", tenantID, err)
	}
	if _, err := cat.MarkMutationApplied(t.Context(), operation, *claimed.Claim); err != nil {
		t.Fatalf("MarkMutationApplied(%s): %v", tenantID, err)
	}
	result, err := cat.CommitMutation(t.Context(), operation)
	if err != nil {
		t.Fatalf("CommitMutation(%s): %v", tenantID, err)
	}
	if result.Mutation.Revision != prepared.ExpectedHead+1 {
		t.Fatalf("commit revision %d, want %d", result.Mutation.Revision, prepared.ExpectedHead+1)
	}
}

func requireNotification(t *testing.T, notifier *fakeNotifier, index int, domain DomainID) Notification {
	t.Helper()
	calls := notifier.calls()
	if len(calls) <= index {
		t.Fatalf("notifications = %d, need index %d", len(calls), index)
	}
	if calls[index].Domain != domain {
		t.Fatalf("notification %d domain = %q, want %q", index, calls[index].Domain, domain)
	}
	return calls[index]
}

func ackNotification(t *testing.T, engine *Engine, notification Notification) {
	t.Helper()
	if err := engine.Acknowledge(t.Context(), Ack{
		Domain: notification.Domain, Generation: notification.Generation, Revision: notification.Revision,
		SourceAuthority: notification.SourceAuthority,
		SourceRevision:  notification.SourceRevision, CatalogRevision: notification.CatalogRevision,
		ChangeID: notification.ChangeID, OperationID: notification.OperationID,
	}); err != nil {
		t.Fatalf("Acknowledge(%s): %v", notification.Domain, err)
	}
}

type failSettlePersistence struct {
	Persistence
	err error
}

func (p *failSettlePersistence) SettleOutbox(context.Context, causal.ChangeID) error {
	return p.err
}

func catalogWithCommittedTenant(t *testing.T) (*catalog.Catalog, catalog.TenantID) {
	t.Helper()
	cat, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	tenant, err := catalog.NewTenantID("outbox-tenant")
	if err != nil {
		t.Fatalf("NewTenantID: %v", err)
	}
	operation, err := catalog.NewMutationID()
	if err != nil {
		t.Fatalf("NewMutationID: %v", err)
	}
	root, err := cat.CreateTenant(t.Context(), operation, tenant, catalog.CaseSensitive, catalog.PresentFileProvider)
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	interestOperation, err := catalog.NewMutationID()
	if err != nil {
		t.Fatalf("NewMutationID(interest): %v", err)
	}
	owner := catalog.InterestOwner{
		Presentation: catalog.PresentationFileProvider,
		Domain:       "outbox-domain",
		Generation:   1,
	}
	if _, err := cat.AddInterest(t.Context(), interestOperation, tenant, root.ID, owner, root.Revision); err != nil {
		t.Fatalf("AddInterest: %v", err)
	}
	return cat, tenant
}

func outboxResolver(tenant catalog.TenantID) *fakeResolver {
	return &fakeResolver{resolutions: []Resolution{{
		Tenant: TenantID(tenant), Domain: "outbox-domain", Generation: 1,
		CatalogRevision: 2, Registered: true, LiveLeases: 1, MaterializedInterests: 1,
		Effective: []EffectiveValue{{Key: "object", Bytes: []byte("root")}},
	}}}
}
