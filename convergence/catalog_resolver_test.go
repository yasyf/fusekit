package convergence

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestCatalogResolverSuppressesInactiveAndTargetsLiveMaterializedDomain(t *testing.T) {
	for _, test := range []struct {
		name       string
		live       bool
		calls      int
		quarantine bool
	}{
		{name: "inactive", calls: 0},
		{name: "live-materialized", live: true, calls: 1, quarantine: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := newFakeClock()
			store, domain := convergenceCatalog(t)
			if test.live {
				if err := store.RenewFileProviderLease(t.Context(), catalog.FileProviderLease{
					ID: "lease", Tenant: domain.Tenant, DomainID: domain.DomainID,
					Generation: domain.Generation, ExpiresAt: clock.Now().Add(time.Hour),
				}); err != nil {
					t.Fatal(err)
				}
			}
			persistence, err := NewCatalogPersistence(store)
			if err != nil {
				t.Fatal(err)
			}
			notifier := &fakeNotifier{}
			engine, err := New(t.Context(), Config{
				Resolver: CatalogResolver{Catalog: store, Now: clock.Now},
				Notifier: notifier, Persistence: persistence, Clock: clock,
			})
			if err != nil {
				t.Fatal(err)
			}
			if calls := len(notifier.calls()); calls != test.calls {
				t.Fatalf("notification calls = %d, want %d", calls, test.calls)
			}
			state, err := engine.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			domainState := state.Domains[domain.DomainID]
			if domainState.Demanded != test.live {
				t.Fatalf("Demanded = %t, want %t", domainState.Demanded, test.live)
			}
			if test.quarantine {
				clock.Advance(AckTimeout)
				if err := engine.Tick(t.Context()); err != nil {
					t.Fatal(err)
				}
				state, err = engine.Snapshot(t.Context())
				if err != nil || state.Domains[domain.DomainID].Quarantine == nil {
					t.Fatalf("quarantine = %+v, %v", state.Domains[domain.DomainID].Quarantine, err)
				}
			}
			if err := engine.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := engine.Wait(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func convergenceCatalog(t *testing.T) (*catalog.Catalog, catalog.FileProviderDomain) {
	t.Helper()
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	provision, err := store.ProvisionTenant(t.Context(), catalog.TenantProvision{
		OwnerID: "owner", Tenant: "tenant", PresentationRoot: filepath.Join(t.TempDir(), "presentation"),
		BackingRoot: filepath.Join(t.TempDir(), "backing"), ContentSourceID: "source",
		Access: catalog.TenantReadWrite, CasePolicy: catalog.CaseSensitive,
		Presentations: catalog.PresentFileProvider,
		FileProvider:  catalog.FileProviderPresentation{AccountInstanceID: "instance", DisplayName: "Tenant"}, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	domains, err := store.FileProviderDomains(t.Context())
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	domain := domains[0]
	domain.Registered = true
	domain.PublicPath = filepath.Join(t.TempDir(), "Domain")
	if err := store.ConfirmFileProviderDomain(t.Context(), domain); err != nil {
		t.Fatal(err)
	}
	operation, err := catalog.NewMutationID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddInterest(t.Context(), operation, provision.Tenant, provision.Root, catalog.InterestOwner{
		Presentation: catalog.PresentationFileProvider, Domain: domain.DomainID,
		Generation: causal.Generation(domain.Generation),
	}, 1); err != nil {
		t.Fatal(err)
	}
	initial, err := store.ClaimConvergenceOutbox(t.Context())
	if err != nil || initial == nil {
		t.Fatalf("ClaimConvergenceOutbox(initial) = %+v, %v", initial, err)
	}
	if err := store.SettleConvergenceOutbox(t.Context(), initial.Change.ChangeID); err != nil {
		t.Fatal(err)
	}
	ref, err := store.StageContent(t.Context(), strings.NewReader("settings"))
	if err != nil {
		t.Fatal(err)
	}
	changeID, err := NewChangeID()
	if err != nil {
		t.Fatal(err)
	}
	operationID, err := NewOperationID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplySource(t.Context(), catalog.SourcePublication{
		Mode: catalog.SourceSnapshot,
		Change: causal.ChangeSet{
			SourceAuthority: "source", SourceRevision: 1, ChangeID: changeID,
			OperationID: operationID, Cause: causal.CauseDaemonWrite,
			AffectedKeys: []causal.LogicalKey{"settings"},
		},
		Tenants: []catalog.SourceTenant{{
			Tenant: provision.Tenant, Generation: provision.Generation,
			Objects: []catalog.SourceObject{{
				Key: "settings", Name: "settings.json", Kind: catalog.KindFile, Mode: 0o600,
				ContentRevision: 1, Content: ref, Visibility: catalog.Visibility{FileProvider: true},
			}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	return store, domain
}
