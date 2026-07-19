package convergence

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestCatalogResolverUsesOnePrecomputedFileProviderProofPerTenant(t *testing.T) {
	store := &countingResolutionStore{}
	commits := make([]causal.CatalogCommit, 100)
	for index := range commits {
		tenant := causal.TenantID(fmt.Sprintf("tenant-%03d", index))
		commits[index] = causal.CatalogCommit{
			Tenant: tenant, CatalogRevision: causal.CatalogRevision(index + 1),
			FileProviderFingerprint: [32]byte{byte(index + 1)},
		}
	}
	resolver, err := NewCatalogResolver(store, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	resolutions, err := resolver.ResolveAffected(t.Context(), semanticChange(1), commits)
	if err != nil {
		t.Fatal(err)
	}
	if len(resolutions) != len(commits) || store.domainCalls != len(commits) || store.demandCalls != len(commits) ||
		store.currentCalls != 0 {
		t.Fatalf("resolutions=%d domain=%d demand=%d current=%d, want 100/100/100/0",
			len(resolutions), store.domainCalls, store.demandCalls, store.currentCalls)
	}
	for index, resolution := range resolutions {
		if resolution.Fingerprint != Fingerprint(commits[index].FileProviderFingerprint) {
			t.Fatalf("tenant %d fingerprint = %x, want %x", index, resolution.Fingerprint, commits[index].FileProviderFingerprint)
		}
	}
	store.domainCalls, store.demandCalls = 0, 0
	commits[0].FileProviderFingerprint = [32]byte{}
	if _, err := resolver.ResolveAffected(t.Context(), semanticChange(2), commits); !errors.Is(err, ErrInvalidResolution) {
		t.Fatalf("zero File Provider proof = %v, want ErrInvalidResolution", err)
	}
	if store.domainCalls != 0 || store.demandCalls != 0 {
		t.Fatalf("zero-proof resolution performed catalog work: domain=%d demand=%d", store.domainCalls, store.demandCalls)
	}
}

type countingResolutionStore struct {
	currentCalls int
	domainCalls  int
	demandCalls  int
}

func (s *countingResolutionStore) CurrentConvergenceTarget(
	context.Context,
	catalog.TenantID,
	causal.SourceAuthorityID,
) (catalog.ConvergenceTarget, error) {
	s.currentCalls++
	return catalog.ConvergenceTarget{}, catalog.ErrNotFound
}

func (s *countingResolutionStore) FileProviderDomainForTenant(
	_ context.Context,
	tenant catalog.TenantID,
) (catalog.FileProviderDomain, bool, error) {
	s.domainCalls++
	suffix := strings.TrimPrefix(string(tenant), "tenant-")
	return catalog.FileProviderDomain{
		Tenant: tenant, DomainID: causal.DomainID("domain-" + suffix), Generation: 1, Registered: true,
	}, true, nil
}

func (s *countingResolutionStore) FileProviderDemand(
	context.Context,
	catalog.TenantID,
	causal.DomainID,
	catalog.Generation,
	time.Time,
) (uint32, uint32, error) {
	s.demandCalls++
	return 1, 1, nil
}

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
			resolver, err := NewCatalogResolver(store, clock.Now)
			if err != nil {
				t.Fatal(err)
			}
			engine, err := New(t.Context(), Config{
				Resolver: resolver,
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
	domains, err := allConvergenceDomains(t, store)
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	domain := domains[0]
	domain.Registered = true
	domain.PublicPath = filepath.Join(t.TempDir(), "Domain")
	if err := store.ConfirmFileProviderDomain(t.Context(), domain); err != nil {
		t.Fatal(err)
	}
	head, err := store.Head(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddInterest(t.Context(), provision.Tenant, head, provision.Root, catalog.InterestOwner{
		Presentation: catalog.PresentationFileProvider, Domain: domain.DomainID,
		Generation: causal.Generation(domain.Generation),
	}, 1); err != nil {
		t.Fatal(err)
	}
	initial, err := store.ClaimConvergenceOutbox(t.Context())
	if err != nil || initial == nil {
		t.Fatalf("ClaimConvergenceOutbox(initial) = %+v, %v", initial, err)
	}
	page, err := store.PageConvergenceOutbox(t.Context(), *initial)
	if err != nil || page.Settlement == nil {
		t.Fatalf("PageConvergenceOutbox(initial) = %+v, %v", page, err)
	}
	if err := store.SettleConvergenceOutbox(t.Context(), *page.Settlement); err != nil {
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
	if err := applyStagedSource(t, store, stagedSourceRevision{
		mode: catalog.SourceSnapshot,
		change: causal.ChangeSet{
			SourceAuthority: "source", SourceRevision: 1, ChangeID: changeID,
			OperationID: operationID, Cause: causal.CauseDaemonWrite,
			AffectedKeys: []causal.LogicalKey{"settings"},
		},
		tenants: []catalog.SourceTenant{{
			Tenant: provision.Tenant, Generation: provision.Generation, RootKey: catalog.SourceObjectKey("root:" + string(provision.Tenant)),
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

func allConvergenceDomains(t *testing.T, store *catalog.Catalog) ([]catalog.FileProviderDomain, error) {
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
