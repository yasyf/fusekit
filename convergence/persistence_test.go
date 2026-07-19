package convergence

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
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
	notification := Notification{
		SourceAuthority: change.SourceAuthority,
		SourceRevision:  change.SourceRevision, CatalogRevision: 3,
		ChangeID: change.ChangeID, OperationID: change.OperationID, Cause: change.Cause,
		AffectedKeys: append([]LogicalKey(nil), change.AffectedKeys...),
		Tenant:       "tenant-b", Domain: "domain-b", Generation: 2, Revision: 4, Fingerprint: Fingerprint{1, 2, 3},
	}
	state := State{
		Revision: 4, SourceHeads: map[SourceAuthorityID]Revision{change.SourceAuthority: 7},
		DedupFloors: map[SourceAuthorityID]Revision{change.SourceAuthority: 2},
		Domains: map[DomainID]DomainState{
			"domain-b": {
				Tenant: "tenant-b", Domain: "domain-b", Generation: 2, Fingerprint: notification.Fingerprint,
				ResolvedSourceRevision: 7, CatalogRevision: 3, NotifiedCatalogRevision: 3,
				Desired: 4, Notified: 4, DesiredChange: change, NotifiedChange: change,
				Demanded: true, Pending: &Pending{Notification: notification, SentAt: time.Unix(100, 5).UTC()},
			},
		},
		Changes: map[ChangeID]AppliedChange{change.ChangeID: {Change: change, EngineRevision: 4}},
	}
	first, err := encodeDurableState(state)
	if err != nil {
		t.Fatalf("encodeDurableState: %v", err)
	}
	reversed := cloneState(state)
	second, err := encodeDurableState(reversed)
	if err != nil {
		t.Fatalf("encodeDurableState(reversed): %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("durable encoding is not deterministic")
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
