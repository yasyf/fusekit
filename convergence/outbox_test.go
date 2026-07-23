package convergence

import (
	"context"
	"errors"
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

func TestOutboxRecoverySettlesBeforePresentingNotification(t *testing.T) {
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
	if len(firstCalls) != 0 {
		t.Fatalf("notifications before terminal settlement = %d, want 0", len(firstCalls))
	}

	restartedNotifier := &fakeNotifier{}
	restarted, err := New(t.Context(), Config{
		Resolver: resolver, Notifier: restartedNotifier, Persistence: persistence, Clock: newFakeClock(),
	})
	if err != nil {
		t.Fatalf("restart New: %v", err)
	}
	restartedCalls := restartedNotifier.calls()
	if len(restartedCalls) != 1 {
		t.Fatalf("recovery notifications = %+v, want one post-settlement notification", restartedCalls)
	}
	if pending, err := cat.ClaimConvergenceOutbox(t.Context()); err != nil || pending != nil {
		t.Fatalf("recovered outbox claim = %+v, %v", pending, err)
	}
	notification := restartedCalls[0]
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

func TestInterestOutboxAdvancesExactDomainWithoutContentNotification(t *testing.T) {
	cat, tenant := catalogWithCommittedTenant(t)
	persistence, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatalf("NewCatalogPersistence: %v", err)
	}
	resolver := &fakeResolver{resolutions: []Resolution{
		{
			Tenant: TenantID(tenant), Domain: "outbox-domain", Generation: 1,
			CatalogRevision: 2, Registered: true, LiveLeases: 1, MaterializedInterests: 1,
			Fingerprint: testFingerprint("interested"),
		},
		{
			Tenant: "unrelated-tenant", Domain: "unrelated-domain", Generation: 1,
			CatalogRevision: 1, Registered: true, LiveLeases: 1, MaterializedInterests: 1,
			Fingerprint: testFingerprint("unrelated"),
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
	head, err := cat.Head(t.Context(), tenant)
	if err != nil {
		t.Fatalf("Head(remove): %v", err)
	}
	if _, err := cat.RemoveInterest(t.Context(), tenant, head, interests[0].ID); err != nil {
		t.Fatalf("RemoveInterest: %v", err)
	}
	resolver.mu.Lock()
	resolver.resolutions[0].CatalogRevision = 3
	resolver.resolutions[0].MaterializedInterests = 0
	resolver.mu.Unlock()
	if err := engine.Drain(t.Context()); err != nil {
		t.Fatalf("Drain(remove): %v", err)
	}
	if calls := notifier.calls(); len(calls) != 1 {
		t.Fatalf("interest-only change emitted content notification: %+v", calls)
	}
	state, err := engine.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if _, exists := state.Domains["unrelated-domain"]; exists {
		t.Fatalf("unrelated domain entered convergence state: %+v", state.Domains["unrelated-domain"])
	}
	domain := state.Domains["outbox-domain"]
	if domain.Demanded || domain.Stale() || domain.CatalogRevision != 3 || domain.ObservedCatalogRevision != 3 ||
		domain.Applicable.SourceRevision <= first.SourceRevision {
		t.Fatalf("interest-only convergence state = %+v", domain)
	}
	if pending, err := cat.ClaimConvergenceOutbox(t.Context()); err != nil || pending != nil {
		t.Fatalf("settled interest outbox = %+v, %v", pending, err)
	}
	if err := engine.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
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

func (p *failSettlePersistence) SettleOutbox(context.Context, causal.OutboxSettlement) error {
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
	provision := provisionOutboxTenant(t, cat, tenant, "outbox")
	settleAllCatalogOutbox(t, cat)
	head, err := cat.Head(t.Context(), tenant)
	if err != nil {
		t.Fatalf("Head(interest): %v", err)
	}
	owner := catalog.InterestOwner{
		Presentation: catalog.PresentationFileProvider,
		Domain:       "outbox-domain",
		Generation:   1,
	}
	if _, err := cat.AddInterest(t.Context(), tenant, head, provision.Root, owner, 1); err != nil {
		t.Fatalf("AddInterest: %v", err)
	}
	return cat, tenant
}

func provisionOutboxTenant(
	t *testing.T,
	store *catalog.Catalog,
	tenant catalog.TenantID,
	instance string,
) catalog.TenantProvision {
	t.Helper()
	root := t.TempDir()
	provision, err := store.ProvisionTenant(t.Context(), catalog.TenantProvision{
		OwnerID: "owner", Tenant: tenant,
		PresentationRoot: filepath.Join(root, "presentation"),
		BackingRoot:      filepath.Join(root, "backing"),
		ContentSourceID:  "source:" + string(tenant),
		Access:           catalog.TenantReadWrite,
		CasePolicy:       catalog.CaseSensitive,
		Presentations:    catalog.PresentFileProvider,
		FileProvider: catalog.FileProviderPresentation{
			PresentationInstanceID: instance,
			DisplayName:       string(tenant),
		},
		Generation: 1,
	})
	if err != nil {
		t.Fatalf("ProvisionTenant(%s): %v", tenant, err)
	}
	return provision
}

func settleAllCatalogOutbox(t *testing.T, store *catalog.Catalog) {
	t.Helper()
	for {
		claim, err := store.ClaimConvergenceOutbox(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if claim == nil {
			return
		}
		for {
			page, err := store.PageConvergenceOutbox(t.Context(), *claim)
			if err != nil {
				t.Fatal(err)
			}
			if page.Settlement != nil {
				if err := store.SettleConvergenceOutbox(t.Context(), *page.Settlement); err != nil {
					t.Fatal(err)
				}
				break
			}
			if page.Next == nil {
				t.Fatal("catalog outbox page has no continuation")
			}
			claim.Cursor = *page.Next
		}
	}
}

func outboxResolver(tenant catalog.TenantID) *fakeResolver {
	return &fakeResolver{resolutions: []Resolution{{
		Tenant: TenantID(tenant), Domain: "outbox-domain", Generation: 1,
		CatalogRevision: 2, Registered: true, LiveLeases: 1, MaterializedInterests: 1,
		Fingerprint: testFingerprint("root"),
	}}}
}
