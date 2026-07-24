package catalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestUnpresentedObjectPublishesOnlyThroughAtomicReplace(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "hidden-replace", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "settings.json", "old")
	anchor, err := c.Head(ctx, tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	ref := stageTestContent(t, c, "new")
	spec := fileSpec(root.ID, ".settings.json.tmp", ref, 1)
	spec.Visibility = Visibility{}
	source, err := c.Create(ctx, tenant, spec)
	if err != nil {
		t.Fatalf("Create(hidden): %v", err)
	}
	if source.Visibility.FileProvider {
		t.Fatal("hidden source is presented")
	}
	if _, err := c.Lookup(ctx, tenant, PresentationFileProvider, source.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(hidden by ID) err = %v, want ErrNotFound", err)
	}
	if _, err := c.lookupAnyObject(ctx, tenant, source.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("internal lookup(hidden by ID) err = %v, want ErrNotFound", err)
	}
	private, found, err := readPrivatePromotionSource(ctx, c.readDB, tenant, source.ID, "test")
	if err != nil || !found || private.ObjectID != source.ID {
		t.Fatalf("private source = %+v, found %t, err %v", private, found, err)
	}
	if _, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, spec.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LookupName(hidden) err = %v, want ErrNotFound", err)
	}
	scope := EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}
	page, err := c.Snapshot(ctx, tenant, scope, 0, SnapshotCursor{}, 20)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if containsObject(page.Objects, source.ID) {
		t.Fatal("snapshot contains hidden source")
	}
	changes, err := c.ChangesSince(ctx, tenant, scope, CompleteChangeCursor(anchor), 20)
	if err != nil {
		t.Fatalf("ChangesSince(hidden create): %v", err)
	}
	if len(changes.Changes) != 0 || changes.Next != CompleteChangeCursor(anchor) {
		t.Fatalf("hidden changes = %+v, want no deltas through %d", changes, anchor)
	}

	result, err := c.Replace(ctx, tenant, source.ID, target.ID)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if result.Source.ID != source.ID || !result.Source.Visibility.FileProvider || !result.Target.Tombstone {
		t.Fatalf("replace result = %+v", result)
	}
	bound, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, target.Name)
	if err != nil {
		t.Fatalf("LookupName(replacement): %v", err)
	}
	if bound.ID != source.ID {
		t.Fatalf("replacement ID = %s, want source %s", bound.ID, source.ID)
	}
	if _, found, err := readPrivatePromotionSource(ctx, c.readDB, tenant, source.ID, "test"); err != nil || found {
		t.Fatalf("promoted private source found = %t, err %v", found, err)
	}
	changes, err = c.ChangesSince(ctx, tenant, scope, CompleteChangeCursor(anchor), 20)
	if err != nil {
		t.Fatalf("ChangesSince(replace): %v", err)
	}
	if len(changes.Changes) != 2 ||
		changes.Changes[0].Kind != ChangeDelete || changes.Changes[0].Object.ID != target.ID ||
		changes.Changes[1].Kind != ChangeUpsert || changes.Changes[1].Object.ID != source.ID ||
		changes.Changes[0].Revision != result.Revision || changes.Changes[1].Revision != result.Revision {
		t.Fatalf("replace changes = %+v", changes.Changes)
	}
}

func TestRestartLeavesHiddenObjectAndCanonicalBindingSeparated(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	tenant, root := createTestTenant(t, c, "hidden-restart", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "settings.json", "old")
	ref := stageTestContent(t, c, "new")
	spec := fileSpec(root.ID, ".settings.json.tmp", ref, 1)
	spec.Visibility = Visibility{}
	source, err := c.Create(ctx, tenant, spec)
	if err != nil {
		t.Fatalf("Create(hidden): %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(restart): %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	bound, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, target.Name)
	if err != nil {
		t.Fatalf("LookupName(canonical): %v", err)
	}
	if bound.ID != target.ID {
		t.Fatalf("canonical binding = %s, want %s", bound.ID, target.ID)
	}
	if _, err := c.lookupAnyObject(ctx, tenant, source.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(hidden) err = %v, want ErrNotFound", err)
	}
	private, found, err := readPrivatePromotionSource(ctx, c.readDB, tenant, source.ID, "test")
	if err != nil || !found || private.ObjectID != source.ID {
		t.Fatalf("private source after restart = %+v, found %t, err %v", private, found, err)
	}
}

func TestReplacePublishesFinalMetadataAndContentInOneRevision(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "streamed-replace", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "settings.json", "old")
	staged := stageTestContent(t, c, "placeholder")
	sourceSpec := fileSpec(root.ID, ".settings.json.tmp", staged, 1)
	sourceSpec.Visibility = Visibility{}
	source, err := c.Create(ctx, tenant, sourceSpec)
	if err != nil {
		t.Fatalf("Create(hidden source): %v", err)
	}
	private, found, err := readPrivatePromotionSource(ctx, c.readDB, tenant, source.ID, "test")
	if err != nil || !found {
		t.Fatalf("private source = %+v, found %t, err %v", private, found, err)
	}
	head, err := c.Head(ctx, tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	content := stageTestContent(t, c, "final")
	name := "renamed.json"
	mode := uint32(0o600)
	presented := true
	result, err := c.testNamespaceMutation(ctx, tenant, MutationIntent{
		SourceID: "test", Disposition: MutationDispositionNamespace,
		Replace: &ReplaceMutation{
			Source: source.ID, Target: target.ID, PrivateCreator: &private.Mutation,
			Parent: &root.ID, Name: &name, Mode: &mode, Visibility: &Visibility{FileProvider: presented},
			Content: &ContentUpdate{Revision: source.ContentRevision + 1, Ref: content},
		},
	})
	if err != nil {
		t.Fatalf("Replace(streamed): %v", err)
	}
	if result.Mutation.Revision != head+1 || result.Primary.Revision != head+1 || result.Secondary.Revision != head+1 {
		t.Fatalf("replace revisions = mutation %d, source %d, target %d, want %d",
			result.Mutation.Revision, result.Primary.Revision, result.Secondary.Revision, head+1)
	}
	if result.Primary.ID != source.ID || result.Primary.Name != name || result.Primary.Mode != mode ||
		!result.Primary.Visibility.FileProvider || result.Primary.ContentRevision != source.ContentRevision+1 ||
		result.Primary.Hash != content.Hash || result.Primary.Size != content.Size {
		t.Fatalf("published source = %+v", result.Primary)
	}
	if result.Secondary.ID != target.ID || !result.Secondary.Tombstone {
		t.Fatalf("replaced target = %+v", result.Secondary)
	}
	if _, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, target.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old binding err = %v, want ErrNotFound", err)
	}
	bound, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, name)
	if err != nil || bound.ID != source.ID {
		t.Fatalf("final binding = %+v, %v", bound, err)
	}
	handle, err := c.OpenAt(ctx, testRetentionOwner, tenant, PresentationFileProvider, 1, source.ID, result.Primary.Revision)
	if err != nil {
		t.Fatalf("OpenAt(final): %v", err)
	}
	defer func() {
		if err := handle.Close(); err != nil {
			t.Errorf("Close handle: %v", err)
		}
	}()
	payload, err := io.ReadAll(handle)
	if err != nil {
		t.Fatalf("ReadAll(final): %v", err)
	}
	if string(payload) != "final" {
		t.Fatalf("final content = %q", payload)
	}
	scope := EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}
	changes, err := c.ChangesSince(ctx, tenant, scope, CompleteChangeCursor(head), 10)
	if err != nil {
		t.Fatalf("ChangesSince(replace): %v", err)
	}
	if changes.Next != CompleteChangeCursor(head+1) || len(changes.Changes) != 2 ||
		changes.Changes[0].Revision != head+1 || changes.Changes[0].Kind != ChangeDelete || changes.Changes[0].Object.ID != target.ID ||
		changes.Changes[1].Revision != head+1 || changes.Changes[1].Kind != ChangeUpsert || changes.Changes[1].Object.ID != source.ID {
		t.Fatalf("atomic replace changes = %+v", changes)
	}
}

func TestPrivateAtomicReplaceIsOldOrNewAcrossCommitFailpoints(t *testing.T) {
	for _, test := range privateMutationCommitFailpoints() {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAtomicPrivateReplaceFixture(t)
			injected := errors.New("private replace visibility failpoint")
			occurrence := 0
			fixture.store.failpoint = func(point string) error {
				if point == test.point {
					occurrence++
					if occurrence == test.occurrence {
						return injected
					}
				}
				return nil
			}
			if _, err := fixture.store.CommitSourceDriverMutation(t.Context(), fixture.stage); !errors.Is(err, injected) {
				t.Fatalf("CommitSourceDriverMutation at %s/%d = %v, want injected error",
					test.point, test.occurrence, err)
			}
			fixture.store.failpoint = nil
			if err := fixture.store.Close(); err != nil {
				t.Fatal(err)
			}

			store, err := Open(t.Context(), fixture.path)
			if err != nil {
				t.Fatalf("Open after %s/%d: %v", test.point, test.occurrence, err)
			}
			defer func() { _ = store.Close() }()
			pending, err := store.PendingSourceDriverCommittedReceipt(t.Context(), fixture.stage.Identity.Authority)
			if err != nil {
				t.Fatal(err)
			}
			if test.committed {
				if pending == nil || pending.Result.Identity.Operation != fixture.stage.Identity.Operation {
					t.Fatalf("committed receipt = %+v, want operation %x", pending, fixture.stage.Identity.Operation)
				}
				if err := store.activateTestSourcePublication(t.Context(), fixture.provision, pending.Result); err != nil {
					t.Fatalf("activate lost-response publication: %v", err)
				}
				assertAtomicPrivateReplacement(t, store, fixture, pending.Result)
			} else {
				if pending != nil {
					t.Fatalf("rolled-back receipt = %+v, want nil", pending)
				}
				assertAtomicPrivateReplaceOldState(t, store, fixture)
			}

			replayed, err := store.CommitSourceDriverMutation(t.Context(), fixture.stage)
			if err != nil {
				t.Fatalf("replay after %s/%d: %v", test.point, test.occurrence, err)
			}
			if test.lostResponse && replayed.ReceiptDigest != pending.Result.ReceiptDigest {
				t.Fatalf("lost-response replay digest = %x, want %x",
					replayed.ReceiptDigest, pending.Result.ReceiptDigest)
			}
			if err := store.activateTestSourcePublication(t.Context(), fixture.provision, replayed); err != nil {
				t.Fatalf("activate replayed publication: %v", err)
			}
			assertAtomicPrivateReplacement(t, store, fixture, replayed)

			again, err := store.CommitSourceDriverMutation(t.Context(), fixture.stage)
			if err != nil || again.ReceiptDigest != replayed.ReceiptDigest {
				t.Fatalf("exact commit replay = %+v, %v, want digest %x", again, err, replayed.ReceiptDigest)
			}
		})
	}
}

func TestPrivateCreateIsExactAcrossCommitFailpoints(t *testing.T) {
	for _, test := range privateMutationCommitFailpoints() {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAtomicPrivateCreateFixture(t)
			injected := errors.New("private create visibility failpoint")
			occurrence := 0
			fixture.store.failpoint = func(point string) error {
				if point == test.point {
					occurrence++
					if occurrence == test.occurrence {
						return injected
					}
				}
				return nil
			}
			if _, err := fixture.store.CommitSourceDriverMutation(t.Context(), fixture.stage); !errors.Is(err, injected) {
				t.Fatalf("CommitSourceDriverMutation at %s/%d = %v, want injected error",
					test.point, test.occurrence, err)
			}
			fixture.store.failpoint = nil
			if err := fixture.store.Close(); err != nil {
				t.Fatal(err)
			}

			store, err := Open(t.Context(), fixture.path)
			if err != nil {
				t.Fatalf("Open after %s/%d: %v", test.point, test.occurrence, err)
			}
			defer func() { _ = store.Close() }()
			pending, err := store.PendingSourceDriverCommittedReceipt(t.Context(), fixture.stage.Identity.Authority)
			if err != nil {
				t.Fatal(err)
			}
			if test.committed {
				if pending == nil || pending.Result.Identity.Operation != fixture.stage.Identity.Operation {
					t.Fatalf("committed receipt = %+v, want operation %x", pending, fixture.stage.Identity.Operation)
				}
				assertExactPrivateCreate(
					t, store, fixture, pending.Result, pending.Result.MutationResult.Private.ObjectID,
				)
			} else {
				if pending != nil {
					t.Fatalf("rolled-back receipt = %+v, want nil", pending)
				}
				var privateCount int
				if err := store.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM private_mutation_objects WHERE mutation_id = ?`,
					fixture.stage.Identity.Mutation[:]).Scan(&privateCount); err != nil {
					t.Fatal(err)
				}
				if privateCount != 0 {
					t.Fatalf("rolled-back private rows = %d, want 0", privateCount)
				}
				var identityCount int
				if err := store.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_object_ids WHERE source_authority = ? AND source_key = ?`,
					string(fixture.stage.Identity.Authority), string(fixture.stage.Identity.MutationResult),
				).Scan(&identityCount); err != nil {
					t.Fatal(err)
				}
				if identityCount != 0 {
					t.Fatalf("rolled-back private identities = %d, want 0", identityCount)
				}
			}

			replayed, err := store.CommitSourceDriverMutation(t.Context(), fixture.stage)
			if err != nil {
				t.Fatalf("replay after %s/%d: %v", test.point, test.occurrence, err)
			}
			if test.lostResponse && replayed.ReceiptDigest != pending.Result.ReceiptDigest {
				t.Fatalf("lost-response replay digest = %x, want %x",
					replayed.ReceiptDigest, pending.Result.ReceiptDigest)
			}
			if err := store.activateTestSourcePublication(t.Context(), fixture.provision, replayed); err != nil {
				t.Fatalf("activate private publication: %v", err)
			}
			replayID := replayed.MutationResult.Private.ObjectID
			if pending != nil && replayID != pending.Result.MutationResult.Private.ObjectID {
				t.Fatalf("lost-response private ID = %s, want %s",
					replayID, pending.Result.MutationResult.Private.ObjectID)
			}
			assertExactPrivateCreate(t, store, fixture, replayed, replayID)
			again, err := store.CommitSourceDriverMutation(t.Context(), fixture.stage)
			if err != nil || again.ReceiptDigest != replayed.ReceiptDigest {
				t.Fatalf("exact private replay = %+v, %v, want digest %x", again, err, replayed.ReceiptDigest)
			}
			if again.MutationResult.Private.ObjectID != replayID {
				t.Fatalf("exact private replay ID = %s, want %s", again.MutationResult.Private.ObjectID, replayID)
			}
		})
	}
}

func TestPrivatePromotionRejectsMismatchedOriginWithoutConsumption(t *testing.T) {
	store, provision, _ := newAtomicPrivateCatalog(t, "private-origin-mismatch")
	target := createTestFile(t, store, provision.Tenant, provision.Root, "settings.json", "old")
	ref := stageTestContent(t, store, "new")
	spec := fileSpec(provision.Root, ".settings.json.tmp", ref, 1)
	spec.Visibility = Visibility{}
	privateObject, err := store.Create(t.Context(), provision.Tenant, spec)
	if err != nil {
		t.Fatalf("Create(private): %v", err)
	}
	private, found, err := readPrivatePromotionSource(
		t.Context(), store.readDB, provision.Tenant, privateObject.ID, "test",
	)
	if err != nil || !found {
		t.Fatalf("private capability = %+v, found %t, err %v", private, found, err)
	}
	head, err := store.Head(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.BeginMutation(t.Context(), provision.Tenant, head, MutationIntent{
		SourceID: "test", Origin: CausalOrigin{Cause: causal.CauseExternalUnattributed},
		Disposition: MutationDispositionNamespace,
		Replace: &ReplaceMutation{
			Source: privateObject.ID, Target: target.ID, PrivateCreator: &private.Mutation,
		},
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("BeginMutation = %v, want ErrNotFound", err)
	}
	durable, found, err := readPrivatePromotionSource(
		t.Context(), store.readDB, provision.Tenant, privateObject.ID, "test",
	)
	if err != nil || !found || durable.Mutation != private.Mutation {
		t.Fatalf("private capability after rejection = %+v, found %t, err %v", durable, found, err)
	}
	bound, err := store.LookupName(
		t.Context(), provision.Tenant, PresentationFileProvider, provision.Root, target.Name,
	)
	if err != nil || bound.ID != target.ID {
		t.Fatalf("target after rejection = %+v, %v", bound, err)
	}
}

type privateMutationCommitFailpoint struct {
	name         string
	point        string
	occurrence   int
	committed    bool
	lostResponse bool
}

func privateMutationCommitFailpoints() []privateMutationCommitFailpoint {
	return []privateMutationCommitFailpoint{
		{name: "before pointer CAS", point: sourceDriverBeforeVisibilityCASPoint, occurrence: 1},
		{name: "first final statement fence", point: sourceDriverFinalCommitStatementPoint, occurrence: 1},
		{name: "after pointer CAS before commit", point: sourceDriverAfterVisibilityCASPoint, occurrence: 1},
		{name: "second final statement fence", point: sourceDriverFinalCommitStatementPoint, occurrence: 2},
		{name: "receipt fence before commit", point: sourceDriverFinalCommitStatementPoint, occurrence: 3},
		{
			name: "after commit before response", point: sourceDriverAfterVisibilityCommitPoint,
			occurrence: 1, committed: true, lostResponse: true,
		},
	}
}

type atomicPrivateReplaceFixture struct {
	store     *Catalog
	path      string
	provision TenantProvision
	stage     SourceDriverStageState
	head      Revision
	target    Object
	private   privateMutationObjectRecord
}

type atomicPrivateCreateFixture struct {
	store     *Catalog
	path      string
	provision TenantProvision
	stage     SourceDriverStageState
	head      Revision
}

func newAtomicPrivateCreateFixture(t *testing.T) atomicPrivateCreateFixture {
	t.Helper()
	store, provision, path := newAtomicPrivateCatalog(t, "private-create-failpoint")
	head, err := store.Head(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	ref := stageTestContent(t, store, "private-content")
	spec := fileSpec(provision.Root, ".settings.json.tmp", ref, 1)
	spec.Visibility = Visibility{}
	preparedProvision, stage := prepareAtomicAuthoritativeMutation(t, store, provision.Tenant, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(), Disposition: MutationDispositionPrivate,
		Create: &CreateMutation{Spec: spec},
	})
	return atomicPrivateCreateFixture{
		store: store, path: path, provision: preparedProvision, stage: stage, head: head,
	}
}

func newAtomicPrivateReplaceFixture(t *testing.T) atomicPrivateReplaceFixture {
	t.Helper()
	store, provision, path := newAtomicPrivateCatalog(t, "private-atomic-replace")

	target := createTestFile(t, store, provision.Tenant, provision.Root, "settings.json", "old")
	forgetAtomicPrivateReplaceReceipts(t, store, provision)
	privateRef := stageTestContent(t, store, "new")
	privateSpec := fileSpec(provision.Root, ".settings.json.tmp", privateRef, 1)
	privateSpec.Visibility = Visibility{}
	privateObject, err := store.Create(t.Context(), provision.Tenant, privateSpec)
	if err != nil {
		t.Fatalf("Create(private): %v", err)
	}
	forgetAtomicPrivateReplaceReceipts(t, store, provision)
	private, found, err := readPrivatePromotionSource(
		t.Context(), store.readDB, provision.Tenant, privateObject.ID, "test",
	)
	if err != nil || !found || private.ObjectID != privateObject.ID {
		t.Fatalf("private object = %+v, found %t, err %v", private, found, err)
	}
	head, err := store.Head(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	preparedProvision, stage := prepareAtomicAuthoritativeMutation(t, store, provision.Tenant, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(), Disposition: MutationDispositionNamespace,
		Replace: &ReplaceMutation{Source: privateObject.ID, Target: target.ID, PrivateCreator: &private.Mutation},
	})
	return atomicPrivateReplaceFixture{
		store: store, path: path, provision: preparedProvision, stage: stage,
		head: head, target: target, private: private,
	}
}

func newAtomicPrivateCatalog(t *testing.T, name string) (*Catalog, TenantProvision, string) {
	t.Helper()
	store, provisions, declaration, targets := newSourceDriverCatalog(t, name)
	provision := provisions[0]
	reset := sourceDriverIdentityAtHeadForTest(
		t, store, declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotReset,
		"", "private-reset-token", 201,
	)
	if err := store.BeginSourceDriverStage(t.Context(), reset); err != nil {
		t.Fatal(err)
	}
	resetStage, err := store.AppendSourceDriverStage(t.Context(), reset, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{
			Digest: sha256.Sum256([]byte("private-reset-page")), Complete: true,
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	prepareSourceDriverPublicationForTest(t, store, reset)
	resetResult, err := store.CommitSourceDriverStage(t.Context(), resetStage)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.activateTestSourcePublication(t.Context(), provision, resetResult); err != nil {
		t.Fatalf("activate reset publication: %v", err)
	}
	forgetAtomicPrivateReplaceReceipts(t, store, provision)
	var sequence int
	var databaseName, path string
	if err := store.readDB.QueryRowContext(t.Context(), `PRAGMA database_list`).Scan(&sequence, &databaseName, &path); err != nil {
		t.Fatalf("catalog path: %v", err)
	}
	return store, provision, path
}

func forgetAtomicPrivateReplaceReceipts(t *testing.T, store *Catalog, provision TenantProvision) {
	t.Helper()
	authority := causal.SourceAuthorityID(provision.ContentSourceID)
	for {
		receipt, err := store.PendingSourceDriverCommittedReceipt(t.Context(), authority)
		if err != nil {
			t.Fatal(err)
		}
		if receipt == nil {
			return
		}
		if err := store.AcknowledgeSourceDriverCommittedReceipt(t.Context(), receipt.Result); err != nil {
			t.Fatal(err)
		}
		if err := store.ForgetSourceDriverCommittedReceipt(t.Context(), receipt.Result); err != nil {
			t.Fatal(err)
		}
	}
}

func prepareAtomicAuthoritativeMutation(
	t *testing.T,
	store *Catalog,
	tenant TenantID,
	intent MutationIntent,
) (TenantProvision, SourceDriverStageState) {
	t.Helper()
	provision, found, err := appliedTenantProvision(t.Context(), store.readDB, tenant)
	if err != nil || !found {
		t.Fatalf("applied tenant provision = %+v, found %t, err %v", provision, found, err)
	}
	checkpoint, err := store.SourceDriverCheckpoint(t.Context(), causal.SourceAuthorityID(provision.ContentSourceID))
	if err != nil {
		t.Fatal(err)
	}
	entries, resultKey, err := store.testSourceEntries(t.Context(), provision, intent)
	if err != nil {
		t.Fatal(err)
	}
	prepared := beginClaimedSourceMutation(t, store, tenant, intent)
	prepared, err = store.PrepareMutationSource(t.Context(), prepared.OperationID, *prepared.Claim)
	if err != nil {
		t.Fatal(err)
	}
	mutationResult := SourceObjectKey("")
	if intent.Create != nil {
		mutationResult = resultKey
		prepared, err = store.SetMutationSourceResult(t.Context(), prepared.OperationID, *prepared.Claim, SourceLocator{
			SourceAuthority: causal.SourceAuthorityID(provision.ContentSourceID),
			SourceRevision:  prepared.Source.Parent.SourceRevision,
			SourceKey:       mutationResult,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	targets := []SourceDriverTarget{{Tenant: tenant, Generation: provision.Generation}}
	targetsDigest, err := SourceDriverTargetsDigest(targets)
	if err != nil {
		t.Fatal(err)
	}
	identity := testSourceDriverMutationIdentity(provision, checkpoint, targetsDigest, prepared, mutationResult, intent)
	reservation, err := store.ReserveSourceDriverMutation(
		t.Context(), sourceDriverMutationReservationRequestForIdentity(identity),
	)
	if err != nil {
		t.Fatal(err)
	}
	for !reservation.TargetsPrepared {
		reservation, err = store.PrepareSourceDriverMutationReservationBatch(
			t.Context(), identity.Mutation, identity.Claim,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	reservation, err = store.BindSourceDriverMutationRequest(
		t.Context(), identity.Mutation, identity.Claim, identity.MutationRequestDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err = store.RecordSourceDriverMutationReceipt(t.Context(), identity.Mutation, identity.Claim,
		SourceDriverMutationReceiptProof{
			ToToken: identity.ToToken, Result: identity.MutationResult, Digest: identity.MutationReceiptDigest,
		})
	if err != nil {
		t.Fatal(err)
	}
	identity.TargetCount = 1
	identity.TargetsDigest = targetsDigest
	identity.DeclarationDigest = sha256.Sum256([]byte("declaration:" + provision.ContentSourceID))
	identity.AuthorityGeneration = 1
	identity.Claim = reservation.Claim
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	pageDigest := sha256.Sum256(append([]byte("atomic-private-page:"), identity.Operation[:]...))
	stage, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{
			Digest: pageDigest, Complete: true, Entries: entries,
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	prepareSourceDriverPublicationForTest(t, store, identity)
	return provision, stage
}

func assertAtomicPrivateReplaceOldState(t *testing.T, store *Catalog, fixture atomicPrivateReplaceFixture) {
	t.Helper()
	head, err := store.Head(t.Context(), fixture.provision.Tenant)
	if err != nil || head != fixture.head {
		t.Fatalf("old head = %d, %v, want %d", head, err, fixture.head)
	}
	bound, err := store.LookupName(
		t.Context(), fixture.provision.Tenant, PresentationFileProvider, fixture.target.Parent, fixture.target.Name,
	)
	if err != nil || bound.ID != fixture.target.ID {
		t.Fatalf("old canonical binding = %+v, %v, want %s", bound, err, fixture.target.ID)
	}
	private, found, err := readPrivatePromotionSource(
		t.Context(), store.readDB, fixture.provision.Tenant, fixture.private.ObjectID, "test",
	)
	if err != nil || !found || private.ObjectID != fixture.private.ObjectID || private.SourceKey != fixture.private.SourceKey {
		t.Fatalf("rolled-back private = %+v, found %t, err %v", private, found, err)
	}
	if _, err := store.Lookup(
		t.Context(), fixture.provision.Tenant, PresentationFileProvider, fixture.private.ObjectID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("private public lookup err = %v, want ErrNotFound", err)
	}
}

func assertExactPrivateCreate(
	t *testing.T,
	store *Catalog,
	fixture atomicPrivateCreateFixture,
	result SourceDriverStageResult,
	expectedID ObjectID,
) {
	t.Helper()
	if result.MutationResult == nil || result.MutationResult.Kind != SourceDriverMutationPrivate ||
		result.MutationResult.Private == nil || result.MutationResult.Namespace != nil {
		t.Fatalf("private create result = %+v, want private arm", result.MutationResult)
	}
	privateResult := *result.MutationResult.Private
	if privateResult.ObjectID != expectedID || privateResult.Tenant != fixture.provision.Tenant ||
		privateResult.SourceKey != fixture.stage.Identity.MutationResult {
		t.Fatalf("private create identity = %+v, want object %s", privateResult, expectedID)
	}
	private, found, err := readPrivatePromotionSource(
		t.Context(), store.readDB, fixture.provision.Tenant, expectedID, "test",
	)
	if err != nil || !found || private.PrivateMutationResult != privateResult {
		t.Fatalf("durable private = %+v, found %t, err %v, want %+v", private, found, err, privateResult)
	}
	var rawID []byte
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT object_id FROM source_object_ids WHERE source_authority = ? AND source_key = ?`,
		string(privateResult.SourceAuthority), string(privateResult.SourceKey)).Scan(&rawID); err != nil {
		t.Fatalf("private identity: %v", err)
	}
	identity, err := objectID(rawID)
	if err != nil || identity != expectedID {
		t.Fatalf("private identity = %s, %v, want %s", identity, err, expectedID)
	}
	head, err := store.Head(t.Context(), fixture.provision.Tenant)
	if err != nil || head != fixture.head {
		t.Fatalf("private create head = %d, %v, want %d", head, err, fixture.head)
	}
	if result.Checkpoint.SourceRevision != fixture.stage.Identity.Predecessor+1 {
		t.Fatalf("private source revision = %d, want %d",
			result.Checkpoint.SourceRevision, fixture.stage.Identity.Predecessor+1)
	}
	if _, err := store.Lookup(
		t.Context(), fixture.provision.Tenant, PresentationFileProvider, expectedID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("private public lookup err = %v, want ErrNotFound", err)
	}
}

func assertAtomicPrivateReplacement(
	t *testing.T,
	store *Catalog,
	fixture atomicPrivateReplaceFixture,
	result SourceDriverStageResult,
) {
	t.Helper()
	if result.MutationResult == nil || result.MutationResult.Kind != SourceDriverMutationNamespace ||
		result.MutationResult.Namespace == nil || result.MutationResult.Private != nil {
		t.Fatalf("replace result = %+v, want namespace arm", result.MutationResult)
	}
	mutation := *result.MutationResult.Namespace
	if mutation.Primary.ID != fixture.private.ObjectID || mutation.Secondary == nil ||
		mutation.Secondary.ID != fixture.target.ID || !mutation.Secondary.Tombstone {
		t.Fatalf("atomic replace mutation = %+v", mutation)
	}
	bound, err := store.LookupName(
		t.Context(), fixture.provision.Tenant, PresentationFileProvider, fixture.target.Parent, fixture.target.Name,
	)
	if err != nil || bound.ID != fixture.private.ObjectID {
		t.Fatalf("new canonical binding = %+v, %v, want %s", bound, err, fixture.private.ObjectID)
	}
	if _, found, err := readPrivatePromotionSource(
		t.Context(), store.readDB, fixture.provision.Tenant, fixture.private.ObjectID, "test",
	); err != nil || found {
		t.Fatalf("promoted private found = %t, err %v", found, err)
	}
	var tombstones int
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_driver_publication_versions
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND object_id = ? AND tombstone = 1`, string(result.Identity.Authority), result.Identity.Operation[:],
		string(fixture.provision.Tenant), fixture.target.ID[:]).Scan(&tombstones); err != nil {
		t.Fatal(err)
	}
	if tombstones != 1 {
		t.Fatalf("target tombstones = %d, want 1", tombstones)
	}
}

func containsObject(objects []Object, id ObjectID) bool {
	for _, object := range objects {
		if object.ID == id {
			return true
		}
	}
	return false
}
