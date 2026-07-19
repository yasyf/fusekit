package catalog

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceSnapshotDeltaReplayAndStableIdentity(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	provision := testTenantProvision(t, "source-reconcile", 1)
	created, err := c.ProvisionTenant(ctx, provision)
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}

	first := sourcePublication(t, c, created, SourceSnapshot, 1, 0, "stable", "config.json", "one")
	result, err := c.ApplySource(ctx, first)
	if err != nil {
		t.Fatalf("ApplySource(snapshot): %v", err)
	}
	if len(result.Commits) != 1 || result.Commits[0].CatalogRevision != 2 {
		t.Fatalf("snapshot result = %+v", result)
	}
	initial, err := c.LookupName(ctx, created.Tenant, PresentationFileProvider, created.Root, "config.json")
	if err != nil {
		t.Fatalf("LookupName(snapshot): %v", err)
	}
	batch, err := c.ClaimConvergenceOutbox(ctx)
	if err != nil || batch == nil || len(batch.Commits) != 1 || batch.Change.SourceAuthority != "test-source" {
		t.Fatalf("snapshot outbox = %+v, %v", batch, err)
	}
	if err := c.SettleConvergenceOutbox(ctx, batch.Change.ChangeID); err != nil {
		t.Fatalf("SettleConvergenceOutbox: %v", err)
	}

	replay := sourcePublication(t, c, created, SourceSnapshot, 1, 0, "stable", "config.json", "one")
	replayed, err := c.ApplySource(ctx, replay)
	if err != nil || !sourceResultsEqual(replayed, result) {
		t.Fatalf("ApplySource(replay) = %+v, %v; want %+v", replayed, err, result)
	}
	if pending, err := c.ClaimConvergenceOutbox(ctx); err != nil || pending != nil {
		t.Fatalf("replay outbox = %+v, %v", pending, err)
	}

	delta := sourcePublication(t, c, created, SourceDelta, 2, 1, "stable", "renamed.json", "two")
	if _, err := c.ApplySource(ctx, delta); err != nil {
		t.Fatalf("ApplySource(delta): %v", err)
	}
	renamed, err := c.LookupName(ctx, created.Tenant, PresentationFileProvider, created.Root, "renamed.json")
	if err != nil || renamed.ID != initial.ID {
		t.Fatalf("renamed object = %+v, %v; want id %s", renamed, err, initial.ID)
	}
	if _, err := c.LookupName(ctx, created.Tenant, PresentationFileProvider, created.Root, "config.json"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old name error = %v, want ErrNotFound", err)
	}

	conflict := sourcePublication(t, c, created, SourceDelta, 2, 1, "stable", "different.json", "two")
	if _, err := c.ApplySource(ctx, conflict); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("conflicting replay = %v, want ErrMutationConflict", err)
	}
	gap := sourcePublication(t, c, created, SourceDelta, 4, 2, "stable", "renamed.json", "four")
	if _, err := c.ApplySource(ctx, gap); !errors.Is(err, ErrSourceRequiresSnapshot) {
		t.Fatalf("gap delta = %v, want ErrSourceRequiresSnapshot", err)
	}

	deleted := sourcePublication(t, c, created, SourceSnapshot, 4, 0, "other", "other.json", "other")
	if _, err := c.ApplySource(ctx, deleted); err != nil {
		t.Fatalf("ApplySource(snapshot repair): %v", err)
	}
	if _, err := c.Lookup(ctx, created.Tenant, PresentationFileProvider, initial.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("omitted source object = %v, want tombstoned", err)
	}
	reappeared := sourcePublication(t, c, created, SourceSnapshot, 5, 0, "stable", "back.json", "back")
	if _, err := c.ApplySource(ctx, reappeared); err != nil {
		t.Fatalf("ApplySource(reappearance): %v", err)
	}
	back, err := c.LookupName(ctx, created.Tenant, PresentationFileProvider, created.Root, "back.json")
	if err != nil || back.ID != initial.ID {
		t.Fatalf("reappeared object = %+v, %v; want id %s", back, err, initial.ID)
	}
}

func TestSourceRejectsWrongPredecessorGenerationAndAuthorityFleet(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	first := testTenantProvision(t, "source-fence-a", 1)
	second := testTenantProvision(t, "source-fence-b", 1)
	createdA, err := c.ProvisionTenant(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	createdB, err := c.ProvisionTenant(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	incomplete := sourcePublication(t, c, createdA, SourceSnapshot, 1, 0, "a", "a", "a")
	if _, err := c.ApplySource(ctx, incomplete); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("incomplete snapshot = %v, want ErrInvalidObject", err)
	}
	assertNoSourceStages(t, c)
	delta := sourcePublication(t, c, createdA, SourceDelta, 1, 0, "a", "a", "a")
	if _, err := c.ApplySource(ctx, delta); !errors.Is(err, ErrSourceRequiresSnapshot) {
		t.Fatalf("first delta = %v, want ErrSourceRequiresSnapshot", err)
	}
	assertNoSourceStages(t, c)
	complete := sourceFleetPublication(t, c, []TenantProvision{createdA, createdB}, SourceSnapshot, 1, 0, "a", "a", "a")
	if _, err := c.ApplySource(ctx, complete); err != nil {
		t.Fatalf("complete snapshot: %v", err)
	}
	wrongGeneration := sourcePublication(t, c, createdA, SourceDelta, 2, 1, "a", "a", "a")
	wrongGeneration.Tenants[0].Generation = 2
	if _, err := c.ApplySource(ctx, wrongGeneration); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("wrong generation = %v, want ErrGenerationMismatch", err)
	}
	assertNoSourceStages(t, c)
}

func TestSourceObjectIdentityIsStableAcrossAuthorityFleet(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	provisions := []TenantProvision{
		testTenantProvision(t, "source-identity-a", 1),
		testTenantProvision(t, "source-identity-b", 1),
	}
	for index := range provisions {
		created, err := c.ProvisionTenant(ctx, provisions[index])
		if err != nil {
			t.Fatal(err)
		}
		provisions[index] = created
	}
	publication := sourceFleetPublication(t, c, provisions, SourceSnapshot, 1, 0, "settings", "settings.json", "same")
	if _, err := c.ApplySource(ctx, publication); err != nil {
		t.Fatalf("ApplySource: %v", err)
	}
	first, err := c.LookupName(ctx, provisions[0].Tenant, PresentationFileProvider, provisions[0].Root, "settings.json")
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.LookupName(ctx, provisions[1].Tenant, PresentationFileProvider, provisions[1].Root, "settings.json")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("authority source key ids = %s and %s", first.ID, second.ID)
	}
}

func TestSourcePublicationFailpointsAreFleetAtomic(t *testing.T) {
	points := []string{
		sourceAfterBegin, sourceAfterRevisions, sourceAfterApply, sourceAfterJournal,
		sourceAfterWatermark, sourceAfterOutbox, sourceBeforeCommit, sourceAfterCommit,
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "catalog.sqlite")
			base, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			provisions := []TenantProvision{
				testTenantProvision(t, "source-failpoint-a", 1),
				testTenantProvision(t, "source-failpoint-b", 1),
			}
			for index := range provisions {
				created, err := base.ProvisionTenant(ctx, provisions[index])
				if err != nil {
					t.Fatal(err)
				}
				provisions[index] = created
			}
			if err := base.Close(); err != nil {
				t.Fatal(err)
			}
			boom := errors.New("simulated source crash")
			fired := false
			faulted, err := open(ctx, path, func(candidate string) error {
				if !fired && candidate == point {
					fired = true
					return boom
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			publication := sourceFleetPublication(t, faulted, provisions, SourceSnapshot, 1, 0, "stable", "config", "value")
			if _, err := faulted.ApplySource(ctx, publication); !errors.Is(err, boom) {
				t.Fatalf("ApplySource = %v, want failpoint", err)
			}
			if err := faulted.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = recovered.Close() })
			heads := make([]Revision, len(provisions))
			lookupErrors := make([]error, len(provisions))
			for index, provision := range provisions {
				heads[index], err = recovered.Head(ctx, provision.Tenant)
				if err != nil {
					t.Fatal(err)
				}
				_, lookupErrors[index] = recovered.LookupName(ctx, provision.Tenant, PresentationFileProvider, provision.Root, "config")
			}
			batch, outboxErr := recovered.ClaimConvergenceOutbox(ctx)
			if point == sourceAfterCommit {
				if heads[0] != 2 || heads[1] != 2 || lookupErrors[0] != nil || lookupErrors[1] != nil ||
					outboxErr != nil || batch == nil || len(batch.Commits) != 2 {
					t.Fatalf("post-commit recovery heads=%v lookup=%v outbox=%+v/%v", heads, lookupErrors, batch, outboxErr)
				}
				return
			}
			if heads[0] != 1 || heads[1] != 1 || !errors.Is(lookupErrors[0], ErrNotFound) || !errors.Is(lookupErrors[1], ErrNotFound) ||
				outboxErr != nil || batch != nil {
				t.Fatalf("pre-commit recovery heads=%v lookup=%v outbox=%+v/%v", heads, lookupErrors, batch, outboxErr)
			}
		})
	}
}

func sourcePublication(t *testing.T, c *Catalog, provision TenantProvision, mode SourceMode, revision, predecessor uint64, key SourceObjectKey, name, content string) SourcePublication {
	return sourceFleetPublication(t, c, []TenantProvision{provision}, mode, revision, predecessor, key, name, content)
}

func sourceFleetPublication(t *testing.T, c *Catalog, provisions []TenantProvision, mode SourceMode, revision, predecessor uint64, key SourceObjectKey, name, content string) SourcePublication {
	t.Helper()
	publication := SourcePublication{
		Mode: mode, Predecessor: causal.Revision(predecessor), Change: sourceChange(revision),
		Tenants: make([]SourceTenant, len(provisions)),
	}
	for index, provision := range provisions {
		publication.Tenants[index] = SourceTenant{
			Tenant: provision.Tenant, Generation: provision.Generation,
			Objects: []SourceObject{{
				Key: key, Name: name, Kind: KindFile, Mode: 0o600, ContentRevision: Revision(revision), Content: stageTestContent(t, c, content),
				Visibility: Visibility{Mount: provision.Presentations.Has(PresentationMount), FileProvider: provision.Presentations.Has(PresentationFileProvider)},
			}},
		}
	}
	return publication
}

func sourceChange(revision uint64) causal.ChangeSet {
	var change causal.ChangeID
	var operation causal.OperationID
	binary.BigEndian.PutUint64(change[8:], revision)
	binary.BigEndian.PutUint64(operation[8:], revision)
	return causal.ChangeSet{
		SourceAuthority: "test-source", SourceRevision: causal.Revision(revision),
		ChangeID: change, OperationID: operation, Cause: causal.CauseExternalUnattributed,
		AffectedKeys: []causal.LogicalKey{"config"},
	}
}

func sourceResultsEqual(left, right SourceResult) bool {
	if left.Authority != right.Authority || left.Revision != right.Revision || left.ChangeID != right.ChangeID || left.Operation != right.Operation {
		return false
	}
	if len(left.Commits) != len(right.Commits) {
		return false
	}
	for index := range left.Commits {
		if left.Commits[index] != right.Commits[index] {
			return false
		}
	}
	return true
}

func assertNoSourceStages(t *testing.T, c *Catalog) {
	t.Helper()
	var count int
	if err := c.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM content_stages").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("unclaimed source stages = %d", count)
	}
}
