package convergence

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestPrepareUsesLatestApplicableTenantCommitAcrossGlobalDeltasAndRestart(t *testing.T) {
	store, provisions, domains := applicableCatalog(t)
	applyApplicableSource(t, store, catalog.SourceSnapshot, 1, 0,
		applicableTenant{provision: provisions[0], name: "a-v1"},
		applicableTenant{provision: provisions[1], name: "b-v1"},
	)

	engine, notifier := startApplicableEngine(t, store)
	if calls := notifier.calls(); len(calls) != 0 {
		t.Fatalf("inactive snapshot notifications = %+v, want none", calls)
	}

	gap := applicablePublication(catalog.SourceDelta, 3, 2,
		applicableTenant{provision: provisions[0], name: "a-gap"},
	)
	if err := applyStagedSource(t, store, gap); !errors.Is(err, catalog.ErrSourcePredecessor) {
		t.Fatalf("global source gap = %v, want ErrSourcePredecessor", err)
	}
	applyApplicableSource(t, store, catalog.SourceDelta, 2, 1,
		applicableTenant{provision: provisions[0], name: "a-v2"},
	)
	if err := engine.Drain(t.Context()); err != nil {
		t.Fatalf("Drain(A-only): %v", err)
	}

	bAtOne := applicableRequirement(t, store, domains[1], "source")
	if bAtOne.SourceRevision != 1 {
		t.Fatalf("tenant B applicable source revision = %d, want 1", bAtOne.SourceRevision)
	}
	proof := prepareApplicableTenant(t, engine, notifier, bAtOne, 1)
	assertApplicableProof(t, proof, bAtOne)

	closeApplicableEngine(t, engine)
	restarted, restartedNotifier := startApplicableEngine(t, store)
	proof, err := restarted.PrepareTenant(t.Context(), bAtOne)
	if err != nil {
		t.Fatalf("PrepareTenant(B after restart): %v", err)
	}
	assertApplicableProof(t, proof, bAtOne)
	if calls := restartedNotifier.calls(); len(calls) != 0 {
		t.Fatalf("restart replay notifications = %+v, want none", calls)
	}

	applyApplicableSource(t, store, catalog.SourceDelta, 3, 2,
		applicableTenant{provision: provisions[1], name: "b-v3"},
	)
	if err := restarted.Drain(t.Context()); err != nil {
		t.Fatalf("Drain(B delta): %v", err)
	}
	bAtThree := applicableRequirement(t, store, domains[1], "source")
	if bAtThree.SourceRevision != 3 {
		t.Fatalf("tenant B later applicable source revision = %d, want 3", bAtThree.SourceRevision)
	}
	proof = prepareApplicableTenant(t, restarted, restartedNotifier, bAtThree, 1)
	assertApplicableProof(t, proof, bAtThree)

	removal, err := store.BeginFileProviderDomainRemoval(t.Context(), provisions[1].OwnerID, provisions[1].Tenant, provisions[1].Generation)
	if err != nil {
		t.Fatalf("BeginFileProviderDomainRemoval(B): %v", err)
	}
	if err := store.ConfirmFileProviderDomainRemoval(t.Context(), removal); err != nil {
		t.Fatalf("ConfirmFileProviderDomainRemoval(B): %v", err)
	}
	if err := store.RemoveTenantProvision(t.Context(), provisions[1].Tenant, provisions[1].Generation); err != nil {
		t.Fatalf("RemoveTenantProvision(B): %v", err)
	}
	applyApplicableSource(t, store, catalog.SourceSnapshot, 4, 0,
		applicableTenant{provision: provisions[0], name: "a-v4"},
	)
	if err := restarted.Drain(t.Context()); err != nil {
		t.Fatalf("Drain(fleet removal snapshot): %v", err)
	}
	if _, err := restarted.RequestTenant(t.Context(), bAtThree); !errors.Is(err, ErrInvalidResolution) {
		t.Fatalf("Prepare removed tenant B = %v, want ErrInvalidResolution", err)
	}
	state, err := restarted.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if state.SourceHeads["source"] != 4 {
		t.Fatalf("global source head = %d, want 4", state.SourceHeads["source"])
	}
	closeApplicableEngine(t, restarted)
}

type applicableTenant struct {
	provision catalog.TenantProvision
	name      string
}

func applicableCatalog(t *testing.T) (*catalog.Catalog, []catalog.TenantProvision, []catalog.FileProviderDomain) {
	t.Helper()
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	provisions := make([]catalog.TenantProvision, 2)
	for index := range provisions {
		name := fmt.Sprintf("tenant-%c", 'a'+rune(index))
		provision, err := store.ProvisionTenant(t.Context(), catalog.TenantProvision{
			OwnerID: "owner", Tenant: catalog.TenantID(name),
			BackingRoot:     filepath.Join(t.TempDir(), "backing"),
			ContentSourceID: "source", Access: catalog.TenantReadWrite,
			CasePolicy: catalog.CaseSensitive, Presentations: catalog.PresentFileProvider,
			FileProvider: catalog.FileProviderPresentation{
				PresentationInstanceID: "instance-" + name,
				DisplayName:            name,
			},
			Generation: 1,
		})
		if err != nil {
			t.Fatalf("ProvisionTenant(%s): %v", name, err)
		}
		provisions[index] = provision
	}
	domains, err := allConvergenceDomains(t, store)
	if err != nil || len(domains) != len(provisions) {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	for index := range domains {
		domains[index].Registered = true
		domains[index].PublicPath = filepath.Join(t.TempDir(), "Domain")
		domains[index].ActivationGeneration = "activation-1"
		if err := store.ConfirmFileProviderDomain(t.Context(), domains[index]); err != nil {
			t.Fatalf("ConfirmFileProviderDomain(%s): %v", domains[index].Tenant, err)
		}
	}
	return store, provisions, domains
}

func applicablePublication(mode catalog.SourceMode, revision, predecessor uint64, tenants ...applicableTenant) stagedSourceRevision {
	publication := stagedSourceRevision{
		mode: mode, predecessor: causal.Revision(predecessor),
		change: ChangeSet{
			SourceAuthority: "source", SourceRevision: Revision(revision),
			ChangeID: changeID(10_000 + revision), OperationID: operationID(10_000 + revision),
			Cause: CauseDaemonWrite, AffectedKeys: []LogicalKey{"config"},
		},
		tenants: make([]catalog.SourceTenant, len(tenants)),
	}
	for index, tenant := range tenants {
		publication.tenants[index] = catalog.SourceTenant{
			Tenant: tenant.provision.Tenant, Generation: tenant.provision.Generation,
			RootKey: catalog.SourceObjectKey("root:" + string(tenant.provision.Tenant)),
			Objects: []catalog.SourceObject{{
				Key: "config", Name: tenant.name, Kind: catalog.KindDirectory, Mode: 0o700,
				Visibility: catalog.Visibility{FileProvider: true},
			}},
		}
	}
	return publication
}

func applyApplicableSource(
	t *testing.T,
	store *catalog.Catalog,
	mode catalog.SourceMode,
	revision, predecessor uint64,
	tenants ...applicableTenant,
) {
	t.Helper()
	if err := applyStagedSource(t, store, applicablePublication(mode, revision, predecessor, tenants...)); err != nil {
		t.Fatalf("applyStagedSource(revision %d): %v", revision, err)
	}
}

func startApplicableEngine(t *testing.T, store *catalog.Catalog) (*Engine, *fakeNotifier) {
	t.Helper()
	persistence, err := NewCatalogPersistence(store)
	if err != nil {
		t.Fatalf("NewCatalogPersistence: %v", err)
	}
	notifier := &fakeNotifier{}
	resolver, err := NewCatalogResolver(store, newFakeClock().Now)
	if err != nil {
		t.Fatalf("NewCatalogResolver: %v", err)
	}
	engine, err := New(t.Context(), Config{
		Resolver: resolver,
		Notifier: notifier, Persistence: persistence, Clock: newFakeClock(),
	})
	if err != nil {
		t.Fatalf("New convergence engine: %v", err)
	}
	return engine, notifier
}

func closeApplicableEngine(t *testing.T, engine *Engine) {
	t.Helper()
	if err := engine.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := engine.Wait(t.Context()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func applicableRequirement(
	t *testing.T,
	store *catalog.Catalog,
	domain catalog.FileProviderDomain,
	authority SourceAuthorityID,
) PreparationRequirement {
	t.Helper()
	target, err := store.CurrentConvergenceTarget(t.Context(), domain.Tenant, authority)
	if err != nil {
		t.Fatalf("CurrentConvergenceTarget(%s): %v", domain.Tenant, err)
	}
	return PreparationRequirement{
		Tenant: TenantID(domain.Tenant), Domain: DomainID(domain.DomainID), Generation: Generation(domain.Generation),
		SourceAuthority: target.Change.SourceAuthority, SourceRevision: target.Change.SourceRevision,
		CatalogRevision: CatalogRevision(target.CatalogRevision), ChangeID: target.Change.ChangeID, OperationID: target.Change.OperationID,
	}
}

func prepareApplicableTenant(
	t *testing.T,
	engine *Engine,
	notifier *fakeNotifier,
	requirement PreparationRequirement,
	wantNewNotifications int,
) ObservationProof {
	t.Helper()
	before := len(notifier.calls())
	preparation, err := engine.RequestTenant(t.Context(), requirement)
	if err != nil {
		t.Fatalf("RequestTenant(%s): %v", requirement.Tenant, err)
	}
	calls := notifier.calls()
	if got := len(calls) - before; got != wantNewNotifications {
		t.Fatalf("new preparation notifications = %d, want %d: %+v", got, wantNewNotifications, calls[before:])
	}
	for _, notification := range calls[before:] {
		if err := engine.Acknowledge(t.Context(), ackFor(notification)); err != nil {
			t.Fatalf("Acknowledge(%s): %v", requirement.Tenant, err)
		}
	}
	proof, err := engine.AwaitObserved(t.Context(), preparation)
	if err != nil {
		t.Fatalf("AwaitObserved(%s): %v", requirement.Tenant, err)
	}
	return proof
}

func assertApplicableProof(t *testing.T, proof ObservationProof, requirement PreparationRequirement) {
	t.Helper()
	if proof.Requested.Tenant != requirement.Tenant || proof.Requested.Domain != requirement.Domain ||
		proof.Requested.Generation != requirement.Generation || proof.Requested.SourceAuthority != requirement.SourceAuthority ||
		proof.Requested.SourceRevision != requirement.SourceRevision || proof.Requested.CatalogRevision != requirement.CatalogRevision ||
		proof.Requested.ChangeID != requirement.ChangeID || proof.Requested.OperationID != requirement.OperationID {
		t.Fatalf("requested proof = %+v, want causal requirement %+v", proof.Requested, requirement)
	}
	if proof.Observed.SourceAuthority != requirement.SourceAuthority || proof.Observed.SourceRevision != requirement.SourceRevision ||
		proof.Observed.CatalogRevision < requirement.CatalogRevision || proof.Observed.ChangeID != requirement.ChangeID ||
		proof.Observed.OperationID != requirement.OperationID {
		t.Fatalf("observed proof = %+v, want exact applicable tuple %+v", proof.Observed, requirement)
	}
}
