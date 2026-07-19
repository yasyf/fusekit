package convergence

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestCatalogPersistenceRoundTripUsesCatalogOwner(t *testing.T) {
	cat, err := catalog.Open(context.Background(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	store, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatalf("NewCatalogPersistence: %v", err)
	}
	change := semanticChange(7)
	affectedCount, affectedDigest, err := summarizeAffected(change.AffectedKeys)
	if err != nil {
		t.Fatal(err)
	}
	notification := Notification{
		SourceAuthority: change.SourceAuthority,
		SourceRevision:  change.SourceRevision, CatalogRevision: 3,
		ChangeID: change.ChangeID, OperationID: change.OperationID, Cause: change.Cause,
		AffectedCount: affectedCount, AffectedDigest: affectedDigest,
		Tenant: "tenant-b", Domain: "domain-b", Generation: 2, Revision: 4, Fingerprint: Fingerprint{1, 2, 3},
	}
	state := State{
		Revision: 4, SourceHeads: map[SourceAuthorityID]Revision{change.SourceAuthority: 7},
		DedupFloors: map[SourceAuthorityID]Revision{change.SourceAuthority: 2},
		Domains: map[DomainID]DomainState{
			"domain-b": {
				Tenant: "tenant-b", Domain: "domain-b", Generation: 2, Fingerprint: notification.Fingerprint,
				Applicable: change, CatalogRevision: 3, NotifiedCatalogRevision: 3,
				Desired: 4, Notified: 4, DesiredChange: change, NotifiedChange: change,
				Demanded: true, Pending: &Pending{Notification: notification, SentAt: time.Unix(100, 5).UTC()},
			},
		},
		Changes: map[ChangeID]AppliedChange{change.ChangeID: {
			Change: changeHeader(change), EngineRevision: 4,
			AffectedCount: affectedCount, AffectedDigest: affectedDigest,
		}},
	}
	if _, err := store.Load(context.Background()); err != nil {
		t.Fatalf("initial Load: %v", err)
	}
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := validateState(loaded); err != nil {
		t.Fatalf("validateState: %v", err)
	}
	if loaded.Domains["domain-b"].Pending == nil || loaded.Domains["domain-b"].Generation != 2 {
		t.Fatalf("loaded state = %#v", loaded)
	}
}

func TestNormalizedPersistenceLostPublishResponseRecoversExactState(t *testing.T) {
	cat, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	lost := errors.New("lost publish response")
	wrapped := &lostPublishStore{Catalog: cat, err: lost}
	persistence, err := NewCatalogPersistence(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := persistence.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	change := semanticChange(11)
	affectedCount, affectedDigest, err := summarizeAffected(change.AffectedKeys)
	if err != nil {
		t.Fatal(err)
	}
	state := emptyState()
	state.Revision = 1
	state.SourceHeads[change.SourceAuthority] = change.SourceRevision
	state.Changes[change.ChangeID] = AppliedChange{
		Change: changeHeader(change), EngineRevision: 1,
		AffectedCount: affectedCount, AffectedDigest: affectedDigest,
	}
	if err := persistence.Save(t.Context(), state); err != nil {
		t.Fatalf("Save after exact lost-response retry: %v", err)
	}
	recovered, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := recovered.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Revision != state.Revision || loaded.SourceHeads[change.SourceAuthority] != change.SourceRevision {
		t.Fatalf("recovered state = %+v, want %+v", loaded, state)
	}
}

func TestNormalizedPersistenceWritesOnlyChangedDomainAcrossHundred(t *testing.T) {
	cat, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	recording := &recordingStateStore{Catalog: cat}
	persistence, err := NewCatalogPersistence(recording)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := persistence.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	change := semanticChange(13)
	state := emptyState()
	state.Revision = 1
	state.SourceHeads[change.SourceAuthority] = change.SourceRevision
	for index := range 100 {
		domain := DomainID(fmt.Sprintf("domain-%03d", index))
		state.Domains[domain] = DomainState{
			Tenant: TenantID(fmt.Sprintf("tenant-%03d", index)), Domain: domain, Generation: 1,
			Fingerprint: Fingerprint{byte(index + 1)}, Applicable: changeHeader(change),
			CatalogRevision: 1,
		}
	}
	if err := persistence.Save(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	recording.reset()
	changed := cloneState(state)
	domain := changed.Domains["domain-042"]
	domain.Demanded = true
	changed.Domains[domain.Domain] = domain
	if err := persistence.Save(t.Context(), changed); err != nil {
		t.Fatal(err)
	}
	if recording.records != 1 || recording.domains != 1 || recording.pages != 1 {
		t.Fatalf("second save pages=%d records=%d domains=%d, want 1/1/1", recording.pages, recording.records, recording.domains)
	}
}

func TestNormalizedPersistencePinsOutstandingProofsAndCompactsTerminalRows(t *testing.T) {
	cat, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	persistence, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := persistence.Load(t.Context()); err != nil {
		t.Fatal(err)
	}

	state := emptyState()
	const total = MaxAppliedChanges + 4
	state.Revision = total
	state.SourceHeads["test-source"] = total
	for revision := uint64(1); revision <= total; revision++ {
		change := semanticChange(revision)
		count, digest, err := summarizeAffected(change.AffectedKeys)
		if err != nil {
			t.Fatal(err)
		}
		state.Changes[change.ChangeID] = AppliedChange{
			Change: changeHeader(change), EngineRevision: Revision(revision),
			AffectedCount: count, AffectedDigest: digest,
		}
	}
	first := semanticChange(1)
	second := semanticChange(2)
	pending := DomainState{
		Tenant: "tenant-pending", Domain: "domain-pending", Generation: 1, Fingerprint: Fingerprint{1},
		Applicable: changeHeader(first), CatalogRevision: 1, NotifiedCatalogRevision: 1,
		Desired: 1, Notified: 1, DesiredChange: changeHeader(first), NotifiedChange: changeHeader(first),
		Demanded: true, Pending: &Pending{SentAt: time.Unix(100, 0).UTC()},
	}
	quarantined := DomainState{
		Tenant: "tenant-quarantined", Domain: "domain-quarantined", Generation: 1, Fingerprint: Fingerprint{2},
		Applicable: changeHeader(second), CatalogRevision: 2, NotifiedCatalogRevision: 2,
		Desired: 2, Notified: 2, DesiredChange: changeHeader(second), NotifiedChange: changeHeader(second),
		Demanded: true, Quarantine: &Quarantine{
			Since: time.Unix(200, 0).UTC(),
			Until: time.Unix(200, 0).UTC().Add(QuarantineBackoff),
		},
	}
	state.Domains[pending.Domain] = pending
	state.Domains[quarantined.Domain] = quarantined
	compactChanges(&state)
	if len(state.Changes) != MaxAppliedChanges {
		t.Fatalf("pinned change journal = %d, want %d", len(state.Changes), MaxAppliedChanges)
	}
	pending.Pending.Notification = notificationFor(state, pending)
	quarantined.Quarantine.Notification = notificationFor(state, quarantined)
	state.Domains[pending.Domain] = pending
	state.Domains[quarantined.Domain] = quarantined
	if err := validateState(state); err != nil {
		t.Fatalf("validate pinned state: %v", err)
	}
	if err := persistence.Save(t.Context(), state); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := validateState(loaded); err != nil {
		t.Fatalf("validate reopened pinned state: %v", err)
	}
	for _, domain := range []DomainID{pending.Domain, quarantined.Domain} {
		record := loaded.Domains[domain]
		var notification Notification
		if record.Pending != nil {
			notification = record.Pending.Notification
		} else if record.Quarantine != nil {
			notification = record.Quarantine.Notification
		} else {
			t.Fatalf("domain %q lost outstanding delivery", domain)
		}
		applied := loaded.Changes[notification.ChangeID]
		if notification.AffectedCount != applied.AffectedCount ||
			notification.AffectedDigest != applied.AffectedDigest {
			t.Fatalf("domain %q replayed proof = %+v, want %+v", domain, notification, applied)
		}
	}

	for _, domain := range []DomainID{pending.Domain, quarantined.Domain} {
		record := state.Domains[domain]
		record.Pending = nil
		record.Quarantine = nil
		record.Observed = record.Desired
		record.ObservedCatalogRevision = record.CatalogRevision
		record.ObservedChange = cloneChange(record.DesiredChange)
		state.Domains[domain] = record
	}
	compactChanges(&state)
	if err := persistence.Save(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	terminal, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatal(err)
	}
	compacted, err := terminal.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(compacted.Changes) != MaxAppliedChanges-2 {
		t.Fatalf("terminal persisted change rows = %d, want %d", len(compacted.Changes), MaxAppliedChanges-2)
	}
	if _, retained := compacted.Changes[first.ChangeID]; retained {
		t.Fatal("terminal pending proof remained persisted")
	}
	if _, retained := compacted.Changes[second.ChangeID]; retained {
		t.Fatal("terminal quarantined proof remained persisted")
	}
}

func TestNormalizedPersistenceRetainsBoundedOnDemandProof(t *testing.T) {
	cat, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	persistence, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := persistence.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	applicable := semanticChange(1)
	onDemand := ChangeSet{
		SourceAuthority: applicable.SourceAuthority, SourceRevision: applicable.SourceRevision,
		ChangeID: changeID(101), OperationID: operationID(101), Cause: CauseOnDemand,
		Origin: "domain", OriginGeneration: 1,
		AffectedKeys: []LogicalKey{"tenant:tenant"},
	}
	state := emptyState()
	state.Revision = 1
	state.SourceHeads[applicable.SourceAuthority] = applicable.SourceRevision
	state.Domains["domain"] = DomainState{
		Tenant: "tenant", Domain: "domain", Generation: 1, Fingerprint: Fingerprint{1},
		Applicable: changeHeader(applicable), CatalogRevision: 1,
		Desired: 1, DesiredChange: onDemand, Demanded: true, Forced: true,
	}
	count, digest, err := summarizeAffected(onDemand.AffectedKeys)
	if err != nil {
		t.Fatal(err)
	}
	state.Changes[onDemand.ChangeID] = AppliedChange{
		Change: changeHeader(onDemand), EngineRevision: 1,
		AffectedCount: count, AffectedDigest: digest,
	}
	if err := persistence.Save(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewCatalogPersistence(cat)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := validateState(loaded); err != nil {
		t.Fatalf("validate replayed on-demand state: %v", err)
	}
	domain := loaded.Domains["domain"]
	if !equalChangeHeader(domain.DesiredChange, onDemand) {
		t.Fatalf("replayed on-demand change = %+v, want %+v", domain.DesiredChange, onDemand)
	}
	notification := notificationFor(loaded, domain)
	if notification.AffectedCount != count || notification.AffectedDigest != digest {
		t.Fatalf("replayed on-demand proof = %+v, want count=%d digest=%x", notification, count, digest)
	}
}

func TestNormalizedStateStageSurvivesReopenAndPublishIsExact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	cat, err := catalog.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := catalog.NewConvergenceEngineOperation()
	if err != nil {
		t.Fatal(err)
	}
	change := semanticChange(17)
	first := catalog.ConvergenceEngineStage{
		Operation: operation, ExpectedVersion: 0, TargetRevision: 1, Sequence: 0, PageCount: 2,
		Page: catalog.ConvergenceEngineDeltaPage{UpsertHeads: []catalog.ConvergenceEngineHead{{
			Authority: change.SourceAuthority, Head: change.SourceRevision,
		}}},
	}
	if err := cat.StageConvergenceEngineMutation(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := cat.Close(); err != nil {
		t.Fatal(err)
	}
	cat, err = catalog.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	second := catalog.ConvergenceEngineStage{
		Operation: operation, ExpectedVersion: 0, TargetRevision: 1, Sequence: 1, PageCount: 2,
		Page: catalog.ConvergenceEngineDeltaPage{UpsertDomains: []catalog.ConvergenceEngineDomain{{
			Tenant: "tenant", Domain: "domain", Generation: 1, Fingerprint: [32]byte{1},
			CatalogRevision: 1, Applicable: causal.ChangeSet(changeHeader(change)),
		}}},
	}
	if err := cat.StageConvergenceEngineMutation(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	published, err := cat.PublishConvergenceEngineMutation(t.Context(), operation)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := cat.PublishConvergenceEngineMutation(t.Context(), operation)
	if err != nil || replayed != published {
		t.Fatalf("replayed publish = %+v, %v, want %+v", replayed, err, published)
	}
}

type lostPublishStore struct {
	*catalog.Catalog
	err  error
	lost bool
}

func (s *lostPublishStore) PublishConvergenceEngineMutation(
	ctx context.Context,
	operation catalog.ConvergenceEngineOperation,
) (catalog.ConvergenceEngineHeader, error) {
	header, err := s.Catalog.PublishConvergenceEngineMutation(ctx, operation)
	if err == nil && !s.lost {
		s.lost = true
		return catalog.ConvergenceEngineHeader{}, s.err
	}
	return header, err
}

type recordingStateStore struct {
	*catalog.Catalog
	pages   int
	records int
	domains int
}

func (s *recordingStateStore) StageConvergenceEngineMutation(
	ctx context.Context,
	stage catalog.ConvergenceEngineStage,
) error {
	s.pages++
	s.records += convergenceStageRecordCount(stage.Page)
	s.domains += len(stage.Page.UpsertDomains)
	return s.Catalog.StageConvergenceEngineMutation(ctx, stage)
}

func (s *recordingStateStore) reset() {
	s.pages, s.records, s.domains = 0, 0, 0
}

func convergenceStageRecordCount(page catalog.ConvergenceEngineDeltaPage) int {
	return len(page.UpsertHeads) + len(page.DeleteHeads) + len(page.UpsertDomains) + len(page.DeleteDomains) +
		len(page.UpsertChanges) + len(page.DeleteChanges)
}
