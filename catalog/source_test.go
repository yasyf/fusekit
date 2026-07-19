package catalog

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
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

func TestSourceAuthoritativeEmptySnapshotPersistsExactProofAcrossRestartAndLostResponse(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	base, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	provision, err := base.ProvisionTenant(ctx, testTenantProvision(t, "source-empty", 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("simulated lost source acknowledgement")
	faulted, err := open(ctx, path, func(point string) error {
		if point == sourceAfterCommit {
			return boom
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	publication := SourcePublication{Mode: SourceSnapshot, Change: sourceChange(1), Tenants: []SourceTenant{}}
	_, err = faulted.ApplySource(ctx, publication)
	if !errors.Is(err, boom) {
		t.Fatalf("ApplySource(authoritative empty) = %v", err)
	}
	expected := SourceResult{
		Authority: publication.Change.SourceAuthority, Revision: publication.Change.SourceRevision,
		ChangeID: publication.Change.ChangeID, Operation: publication.Change.OperationID,
	}
	if err := faulted.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	replayed, err := recovered.ApplySource(ctx, publication)
	if err != nil || !sourceResultsEqual(replayed, expected) {
		t.Fatalf("replayed authoritative empty = %+v, %v; want %+v", replayed, err, expected)
	}
	if batch, err := recovered.ClaimConvergenceOutbox(ctx); err != nil || batch != nil {
		t.Fatalf("authoritative empty outbox = %+v, %v", batch, err)
	}
	delta := sourcePublication(t, recovered, provision, SourceDelta, 2, 1, "stable", "config.json", "restored")
	if _, err := recovered.ApplySource(ctx, delta); err != nil {
		t.Fatalf("delta after authoritative empty watermark: %v", err)
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

func TestSourceSymlinkReplayRenameTombstoneAndGapPreserveIdentity(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	provision, err := c.ProvisionTenant(ctx, testTenantProvision(t, "source-symlink", 1))
	if err != nil {
		t.Fatal(err)
	}
	publication := sourceSymlinkPublication(provision, SourceSnapshot, 1, 0, "link", "current", "../one")
	result, err := c.ApplySource(ctx, publication)
	if err != nil {
		t.Fatalf("ApplySource(snapshot): %v", err)
	}
	initial, err := c.LookupName(ctx, provision.Tenant, PresentationMount, provision.Root, "current")
	if err != nil || initial.Kind != KindSymlink || initial.LinkTarget != "../one" {
		t.Fatalf("initial symlink = %+v, %v", initial, err)
	}
	replayed, err := c.ApplySource(ctx, sourceSymlinkPublication(provision, SourceSnapshot, 1, 0, "link", "current", "../one"))
	if err != nil || !sourceResultsEqual(replayed, result) {
		t.Fatalf("replay = %+v, %v; want %+v", replayed, err, result)
	}
	if _, err := c.ApplySource(ctx, sourceSymlinkPublication(provision, SourceDelta, 2, 1, "link", "renamed", "../two")); err != nil {
		t.Fatalf("ApplySource(delta): %v", err)
	}
	renamed, err := c.LookupName(ctx, provision.Tenant, PresentationMount, provision.Root, "renamed")
	if err != nil || renamed.ID != initial.ID || renamed.LinkTarget != "../two" || renamed.ContentRevision != 2 {
		t.Fatalf("renamed symlink = %+v, %v; want id %s", renamed, err, initial.ID)
	}
	if _, err := c.ApplySource(ctx, sourceSymlinkPublication(provision, SourceDelta, 4, 2, "link", "gap", "../gap")); !errors.Is(err, ErrSourceRequiresSnapshot) {
		t.Fatalf("gap delta = %v, want ErrSourceRequiresSnapshot", err)
	}
	empty := SourcePublication{
		Mode: SourceSnapshot, Change: sourceChange(4),
		Tenants: []SourceTenant{{Tenant: provision.Tenant, Generation: provision.Generation, RootKey: sourceRootKey(provision)}},
	}
	if _, err := c.ApplySource(ctx, empty); err != nil {
		t.Fatalf("tombstone snapshot: %v", err)
	}
	if _, err := c.Lookup(ctx, provision.Tenant, PresentationMount, initial.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tombstoned symlink lookup = %v, want ErrNotFound", err)
	}
	if _, err := c.ApplySource(ctx, sourceSymlinkPublication(provision, SourceSnapshot, 5, 0, "link", "back", "/absolute")); err != nil {
		t.Fatalf("reappearance snapshot: %v", err)
	}
	back, err := c.LookupName(ctx, provision.Tenant, PresentationMount, provision.Root, "back")
	if err != nil || back.ID != initial.ID || back.LinkTarget != "/absolute" {
		t.Fatalf("reappeared symlink = %+v, %v; want id %s", back, err, initial.ID)
	}
}

func TestSourceDeltaReplacesDifferentKeyAtSameNameAtomically(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	provision, err := c.ProvisionTenant(ctx, testTenantProvision(t, "source-same-name-delta", 1))
	if err != nil {
		t.Fatal(err)
	}
	initial := sourceObjectsPublication(t, c, provision, SourceSnapshot, 1, 0,
		[]sourceObjectInput{{key: "old", name: "settings.json", content: "old"}}, nil)
	if _, err := c.ApplySource(ctx, initial); err != nil {
		t.Fatalf("ApplySource(initial): %v", err)
	}
	old, err := c.LookupName(ctx, provision.Tenant, PresentationFileProvider, provision.Root, "settings.json")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := c.OpenAt(ctx, provision.Tenant, PresentationFileProvider, provision.Generation, old.ID, old.Revision)
	if err != nil {
		t.Fatalf("OpenAt(old): %v", err)
	}
	defer func() { _ = handle.Close() }()

	replacement := sourceObjectsPublication(t, c, provision, SourceDelta, 2, 1,
		[]sourceObjectInput{{key: "new", name: "settings.json", content: "new"}}, []SourceObjectKey{"old"})
	result, err := c.ApplySource(ctx, replacement)
	if err != nil {
		t.Fatalf("ApplySource(replacement): %v", err)
	}
	current, err := c.LookupName(ctx, provision.Tenant, PresentationFileProvider, provision.Root, "settings.json")
	if err != nil || current.ID == old.ID {
		t.Fatalf("replacement = %+v, %v; old id %s", current, err, old.ID)
	}
	if _, err := c.Lookup(ctx, provision.Tenant, PresentationFileProvider, old.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old object lookup = %v, want ErrNotFound", err)
	}
	bytes, err := io.ReadAll(handle)
	if err != nil || string(bytes) != "old" {
		t.Fatalf("old handle = %q, %v", bytes, err)
	}

	page, err := c.ChangesSince(ctx, provision.Tenant, EnumerationScope{
		Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: provision.Root,
	}, CompleteChangeCursor(2), 10)
	if err != nil {
		t.Fatalf("ChangesSince: %v", err)
	}
	if len(page.Changes) != 2 || page.Changes[0].Kind != ChangeDelete || page.Changes[0].Object.ID != old.ID ||
		page.Changes[1].Kind != ChangeUpsert || page.Changes[1].Object.ID != current.ID {
		t.Fatalf("replacement changes = %+v", page.Changes)
	}
	var tombstones int
	if err := c.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM object_versions
WHERE tenant = ? AND object_id = ? AND revision = ? AND tombstone = 1`,
		string(provision.Tenant), old.ID[:], uint64(result.Commits[0].CatalogRevision)).Scan(&tombstones); err != nil {
		t.Fatal(err)
	}
	if tombstones != 1 {
		t.Fatalf("old tombstone versions = %d, want 1", tombstones)
	}
	replay := sourceObjectsPublication(t, c, provision, SourceDelta, 2, 1,
		[]sourceObjectInput{{key: "new", name: "settings.json", content: "new"}}, []SourceObjectKey{"old"})
	replayed, err := c.ApplySource(ctx, replay)
	if err != nil || !sourceResultsEqual(replayed, result) {
		t.Fatalf("replacement replay = %+v, %v; want %+v", replayed, err, result)
	}
	var replayTombstones int
	if err := c.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM object_versions
WHERE tenant = ? AND object_id = ? AND revision = ? AND tombstone = 1`,
		string(provision.Tenant), old.ID[:], uint64(result.Commits[0].CatalogRevision)).Scan(&replayTombstones); err != nil {
		t.Fatal(err)
	}
	if replayTombstones != tombstones {
		t.Fatalf("replay tombstone versions = %d, want %d", replayTombstones, tombstones)
	}
}

func TestSourceSnapshotReplacesDifferentKeyAtSameNameAtomically(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	provision, err := c.ProvisionTenant(ctx, testTenantProvision(t, "source-same-name-snapshot", 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplySource(ctx, sourceObjectsPublication(t, c, provision, SourceSnapshot, 1, 0,
		[]sourceObjectInput{{key: "old", name: "settings.json", content: "old"}}, nil)); err != nil {
		t.Fatalf("ApplySource(initial): %v", err)
	}
	old, err := c.LookupName(ctx, provision.Tenant, PresentationFileProvider, provision.Root, "settings.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplySource(ctx, sourceObjectsPublication(t, c, provision, SourceSnapshot, 2, 0,
		[]sourceObjectInput{{key: "new", name: "settings.json", content: "new"}}, nil)); err != nil {
		t.Fatalf("ApplySource(replacement): %v", err)
	}
	current, err := c.LookupName(ctx, provision.Tenant, PresentationFileProvider, provision.Root, "settings.json")
	if err != nil || current.ID == old.ID {
		t.Fatalf("replacement = %+v, %v; old id %s", current, err, old.ID)
	}
}

func TestSourceNamespaceRenameCyclesPreserveObjectIdentity(t *testing.T) {
	for _, test := range []struct {
		name     string
		initial  []sourceObjectInput
		renames  []sourceObjectInput
		expected map[SourceObjectKey]string
	}{
		{
			name: "swap",
			initial: []sourceObjectInput{
				{key: "a", name: "a", content: "a"}, {key: "b", name: "b", content: "b"},
			},
			renames: []sourceObjectInput{
				{key: "a", name: "b", content: "a"}, {key: "b", name: "a", content: "b"},
			},
			expected: map[SourceObjectKey]string{"a": "b", "b": "a"},
		},
		{
			name: "three-way-cycle",
			initial: []sourceObjectInput{
				{key: "a", name: "a", content: "a"}, {key: "b", name: "b", content: "b"}, {key: "c", name: "c", content: "c"},
			},
			renames: []sourceObjectInput{
				{key: "a", name: "b", content: "a"}, {key: "b", name: "c", content: "b"}, {key: "c", name: "a", content: "c"},
			},
			expected: map[SourceObjectKey]string{"a": "b", "b": "c", "c": "a"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			c := newTestCatalog(t)
			provision, err := c.ProvisionTenant(ctx, testTenantProvision(t, "source-rename-"+test.name, 1))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.ApplySource(ctx, sourceObjectsPublication(t, c, provision, SourceSnapshot, 1, 0, test.initial, nil)); err != nil {
				t.Fatalf("ApplySource(initial): %v", err)
			}
			ids := make(map[SourceObjectKey]ObjectID, len(test.initial))
			for _, object := range test.initial {
				current, err := c.LookupName(ctx, provision.Tenant, PresentationFileProvider, provision.Root, object.name)
				if err != nil {
					t.Fatal(err)
				}
				ids[object.key] = current.ID
			}
			if _, err := c.ApplySource(ctx, sourceObjectsPublication(t, c, provision, SourceDelta, 2, 1, test.renames, nil)); err != nil {
				t.Fatalf("ApplySource(rename cycle): %v", err)
			}
			for key, name := range test.expected {
				current, err := c.LookupName(ctx, provision.Tenant, PresentationFileProvider, provision.Root, name)
				if err != nil || current.ID != ids[key] {
					t.Fatalf("%s at %q = %+v, %v; want id %s", key, name, current, err, ids[key])
				}
			}
		})
	}
}

func TestSourceRejectsParentCycleBeforeNamespaceMutation(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	provision, err := c.ProvisionTenant(ctx, testTenantProvision(t, "source-parent-cycle", 1))
	if err != nil {
		t.Fatal(err)
	}
	initial := SourcePublication{
		Mode: SourceSnapshot, Change: sourceChange(1),
		Tenants: []SourceTenant{{Tenant: provision.Tenant, Generation: provision.Generation, RootKey: sourceRootKey(provision), Objects: []SourceObject{
			{Key: "a", Name: "a", Kind: KindDirectory, Mode: 0o700, Visibility: sourceVisibility(provision)},
			{Key: "b", Parent: "a", Name: "b", Kind: KindDirectory, Mode: 0o700, Visibility: sourceVisibility(provision)},
		}}},
	}
	if _, err := c.ApplySource(ctx, initial); err != nil {
		t.Fatalf("ApplySource(initial): %v", err)
	}
	cycle := SourcePublication{
		Mode: SourceDelta, Predecessor: 1, Change: sourceChange(2),
		Tenants: []SourceTenant{{Tenant: provision.Tenant, Generation: provision.Generation, RootKey: sourceRootKey(provision), Objects: []SourceObject{
			{Key: "a", Parent: "b", Name: "a", Kind: KindDirectory, Mode: 0o700, Visibility: sourceVisibility(provision)},
			{Key: "b", Parent: "a", Name: "b", Kind: KindDirectory, Mode: 0o700, Visibility: sourceVisibility(provision)},
		}}},
	}
	if _, err := c.ApplySource(ctx, cycle); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("ApplySource(parent cycle) = %v, want ErrInvalidObject", err)
	}
	a, err := c.LookupName(ctx, provision.Tenant, PresentationFileProvider, provision.Root, "a")
	if err != nil {
		t.Fatalf("lookup original a: %v", err)
	}
	if _, err := c.LookupName(ctx, provision.Tenant, PresentationFileProvider, a.ID, "b"); err != nil {
		t.Fatalf("lookup original b: %v", err)
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

func TestSourceSameNameReplacementFailpointsAreAtomic(t *testing.T) {
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
			provision, err := base.ProvisionTenant(ctx, testTenantProvision(t, "source-replacement-failpoint", 1))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := base.ApplySource(ctx, sourceObjectsPublication(t, base, provision, SourceSnapshot, 1, 0,
				[]sourceObjectInput{{key: "old", name: "settings.json", content: "old"}}, nil)); err != nil {
				t.Fatal(err)
			}
			old, err := base.LookupName(ctx, provision.Tenant, PresentationFileProvider, provision.Root, "settings.json")
			if err != nil {
				t.Fatal(err)
			}
			batch, err := base.ClaimConvergenceOutbox(ctx)
			if err != nil || batch == nil {
				t.Fatalf("initial outbox = %+v, %v", batch, err)
			}
			if err := base.SettleConvergenceOutbox(ctx, batch.Change.ChangeID); err != nil {
				t.Fatal(err)
			}
			if err := base.Close(); err != nil {
				t.Fatal(err)
			}

			boom := errors.New("simulated replacement crash")
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
			replacement := sourceObjectsPublication(t, faulted, provision, SourceDelta, 2, 1,
				[]sourceObjectInput{{key: "new", name: "settings.json", content: "new"}}, []SourceObjectKey{"old"})
			if _, err := faulted.ApplySource(ctx, replacement); !errors.Is(err, boom) {
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
			head, err := recovered.Head(ctx, provision.Tenant)
			if err != nil {
				t.Fatal(err)
			}
			current, lookupErr := recovered.LookupName(ctx, provision.Tenant, PresentationFileProvider, provision.Root, "settings.json")
			if point == sourceAfterCommit {
				if head != 3 || lookupErr != nil || current.ID == old.ID {
					t.Fatalf("post-commit head=%d object=%+v lookup=%v; old id %s", head, current, lookupErr, old.ID)
				}
				if _, err := recovered.Lookup(ctx, provision.Tenant, PresentationFileProvider, old.ID); !errors.Is(err, ErrNotFound) {
					t.Fatalf("post-commit old lookup = %v, want ErrNotFound", err)
				}
				return
			}
			if head != 2 || lookupErr != nil || current.ID != old.ID {
				t.Fatalf("pre-commit head=%d object=%+v lookup=%v; want old id %s", head, current, lookupErr, old.ID)
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
			Tenant: provision.Tenant, Generation: provision.Generation, RootKey: sourceRootKey(provision),
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

func sourceSymlinkPublication(provision TenantProvision, mode SourceMode, revision, predecessor uint64, key SourceObjectKey, name, target string) SourcePublication {
	return SourcePublication{
		Mode: mode, Predecessor: causal.Revision(predecessor), Change: sourceChange(revision),
		Tenants: []SourceTenant{{
			Tenant: provision.Tenant, Generation: provision.Generation, RootKey: sourceRootKey(provision),
			Objects: []SourceObject{{
				Key: key, Name: name, Kind: KindSymlink, Mode: 0o777,
				ContentRevision: Revision(revision), LinkTarget: target,
				Visibility: Visibility{Mount: provision.Presentations.Has(PresentationMount), FileProvider: provision.Presentations.Has(PresentationFileProvider)},
			}},
		}},
	}
}

type sourceObjectInput struct {
	key     SourceObjectKey
	parent  SourceObjectKey
	name    string
	content string
}

func sourceObjectsPublication(t *testing.T, c *Catalog, provision TenantProvision, mode SourceMode, revision, predecessor uint64, objects []sourceObjectInput, deletes []SourceObjectKey) SourcePublication {
	t.Helper()
	publication := SourcePublication{
		Mode: mode, Predecessor: causal.Revision(predecessor), Change: sourceChange(revision),
		Tenants: []SourceTenant{{Tenant: provision.Tenant, Generation: provision.Generation, RootKey: sourceRootKey(provision), Deletes: deletes}},
	}
	for _, object := range objects {
		publication.Tenants[0].Objects = append(publication.Tenants[0].Objects, SourceObject{
			Key: object.key, Parent: object.parent, Name: object.name, Kind: KindFile, Mode: 0o600,
			ContentRevision: Revision(revision), Content: stageTestContent(t, c, object.content), Visibility: sourceVisibility(provision),
		})
	}
	return publication
}

func sourceRootKey(provision TenantProvision) SourceObjectKey {
	return SourceObjectKey("root:" + string(provision.Tenant))
}

func sourceVisibility(provision TenantProvision) Visibility {
	return Visibility{
		Mount:        provision.Presentations.Has(PresentationMount),
		FileProvider: provision.Presentations.Has(PresentationFileProvider),
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
