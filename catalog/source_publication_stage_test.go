package catalog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourcePublicationStageDriverBackedCommitIsObserverOnly(t *testing.T) {
	c := newTestCatalog(t)
	fixture := stageDriverBackedObserverPublication(t, c, "observer-only", 2, false)
	before, err := c.Head(t.Context(), fixture.Provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.CommitSourcePublicationStage(t.Context(), fixture.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if result.First != 1 || result.Last != 1 || result.Count != 1 {
		t.Fatalf("stage result = %+v", result)
	}
	after, err := c.Head(t.Context(), fixture.Provision.Tenant)
	if err != nil || after != before {
		t.Fatalf("observer commit advanced namespace head from %d to %d: %v", before, after, err)
	}
	watermark, err := c.SourceWatermark(t.Context(), fixture.Identity.Authority)
	if err != nil || watermark != 1 {
		t.Fatalf("watermark = %d, %v; want 1", watermark, err)
	}
	var indexed int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_physical_index WHERE source_authority = ?`,
		string(fixture.Identity.Authority)).Scan(&indexed); err != nil {
		t.Fatal(err)
	}
	if indexed != 2 {
		t.Fatalf("physical index rows = %d, want 2", indexed)
	}
	replayed, err := c.CommitSourcePublicationStage(t.Context(), fixture.Ref)
	if err != nil || replayed != result {
		t.Fatalf("lost-response replay = %+v, %v; want %+v", replayed, err, result)
	}
	forged := fixture.Ref
	forged.Items++
	if _, err := c.CommitSourcePublicationStage(t.Context(), forged); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("forged receipt proof = %v, want mutation conflict", err)
	}
}

func TestSourcePublicationStageReceiptSurvivesReopen(t *testing.T) {
	database := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	fixture := stageDriverBackedObserverPublication(t, c, "observer-reopen", 1, false)
	want, err := c.CommitSourcePublicationStage(t.Context(), fixture.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	got, err := c.CommitSourcePublicationStage(t.Context(), fixture.Ref)
	if err != nil || got != want {
		t.Fatalf("reopened receipt = %+v, %v; want %+v", got, err, want)
	}
}

func TestSourcePublicationStageRejectsCrossPageIndexReordering(t *testing.T) {
	c := newTestCatalog(t)
	fixture := beginDriverBackedObserverPublication(t, c, "observer-order", false)
	first := observerPublicationHeaderPage(fixture.Driver, 0, false)
	first.Index = []SourcePhysicalIndexRecord{observerIndexRecord("z")}
	if _, err := c.AppendSourcePublicationStage(t.Context(), fixture.Identity, first); err != nil {
		t.Fatal(err)
	}
	reordered := SourcePublicationStagePage{
		Sequence: 1,
		Index:    []SourcePhysicalIndexRecord{observerIndexRecord("a")},
		Complete: true,
	}
	if _, err := c.AppendSourcePublicationStage(t.Context(), fixture.Identity, reordered); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("cross-page reorder = %v, want invalid transition", err)
	}
}

func TestSourcePublicationStageRejectsDuplicateCausalIdentityWithinStage(t *testing.T) {
	c := newTestCatalog(t)
	fixture := beginDriverBackedObserverPublication(t, c, "observer-causal-scope", false)
	first := observerPublicationHeaderPage(fixture.Driver, 0, false)
	if _, err := c.AppendSourcePublicationStage(t.Context(), fixture.Identity, first); err != nil {
		t.Fatal(err)
	}
	duplicate := observerPublicationHeaderPage(fixture.Driver, 1, false)
	duplicate.Header.Predecessor = 1
	duplicate.Header.Change.SourceRevision = 2
	duplicate.Affected[0].Revision = 2
	if _, err := c.AppendSourcePublicationStage(t.Context(), fixture.Identity, duplicate); !errors.Is(err, ErrConflict) {
		t.Fatalf("same-stage causal identity reuse = %v, want conflict", err)
	}
}

func TestSourcePublicationStageRejectsDriverHeaderMismatch(t *testing.T) {
	c := newTestCatalog(t)
	fixture := stageDriverBackedObserverPublication(t, c, "observer-mismatch", 1, true)
	if _, err := c.CommitSourcePublicationStage(t.Context(), fixture.Ref); !errors.Is(err, ErrSourcePredecessor) {
		t.Fatalf("mismatched driver header = %v, want source predecessor", err)
	}
}

func TestSourcePublicationFinalStatementCountIsPhysicalObjectCountIndependent(t *testing.T) {
	one := sourcePublicationFinalStatementCount(t, 1)
	thousand := sourcePublicationFinalStatementCount(t, 1_000)
	if one == 0 || thousand != one {
		t.Fatalf("final statement count one=%d thousand=%d, want equal nonzero", one, thousand)
	}
}

func TestSourcePublicationFinalStatementsAreAtomicAcrossReopen(t *testing.T) {
	statementCount := sourcePublicationFinalStatementCount(t, 1)
	for ordinal := 1; ordinal <= statementCount; ordinal++ {
		t.Run(fmt.Sprintf("statement-%02d", ordinal), func(t *testing.T) {
			database := filepath.Join(t.TempDir(), "catalog.sqlite")
			c, err := Open(t.Context(), database)
			if err != nil {
				t.Fatal(err)
			}
			fixture := stageDriverBackedObserverPublication(t, c, "observer-atomic", 8, false)
			injected := errors.New("observer final statement failpoint")
			seen := 0
			c.failpoint = func(point string) error {
				if point != sourceObserverSettlementStatementPoint {
					return nil
				}
				seen++
				if seen == ordinal {
					return injected
				}
				return nil
			}
			if _, err := c.CommitSourcePublicationStage(t.Context(), fixture.Ref); !errors.Is(err, injected) {
				t.Fatalf("statement %d error = %v, want injected", ordinal, err)
			}
			c.failpoint = nil
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			c, err = Open(t.Context(), database)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			result, err := c.CommitSourcePublicationStage(t.Context(), fixture.Ref)
			if err != nil {
				t.Fatalf("retry statement %d: %v", ordinal, err)
			}
			replayed, err := c.CommitSourcePublicationStage(t.Context(), fixture.Ref)
			if err != nil || replayed != result {
				t.Fatalf("replay statement %d = %+v, %v; want %+v", ordinal, replayed, err, result)
			}
		})
	}
}

func sourcePublicationFinalStatementCount(t *testing.T, indexCount int) int {
	t.Helper()
	c := newTestCatalog(t)
	fixture := stageDriverBackedObserverPublication(t, c, fmt.Sprintf("observer-count-%d", indexCount), indexCount, false)
	statements := 0
	c.failpoint = func(point string) error {
		if point == sourceObserverSettlementStatementPoint {
			statements++
		}
		return nil
	}
	if _, err := c.CommitSourcePublicationStage(t.Context(), fixture.Ref); err != nil {
		t.Fatal(err)
	}
	c.failpoint = nil
	return statements
}

type driverBackedObserverFixture struct {
	Provision      TenantProvision
	Driver         SourceDriverStageIdentity
	Identity       SourcePublicationStageIdentity
	Ref            SourcePublicationStageRef
	HeaderMismatch bool
}

func stageDriverBackedObserverPublication(
	t *testing.T,
	c *Catalog,
	name string,
	indexCount int,
	headerMismatch bool,
) driverBackedObserverFixture {
	t.Helper()
	fixture := beginDriverBackedObserverPublication(t, c, name, headerMismatch)
	fixture.Ref = appendObserverPublicationPages(t, c, fixture, indexCount)
	state := appendAtomicVisibilityObject(t, c, fixture.Driver, fixture.Provision, "published")
	prepareAtomicVisibilityPublication(t, c, fixture.Driver)
	if _, err := c.CommitSourceDriverStage(t.Context(), state); err != nil {
		t.Fatalf("commit source driver stage: %v", err)
	}
	return fixture
}

func beginDriverBackedObserverPublication(
	t *testing.T,
	c *Catalog,
	name string,
	headerMismatch bool,
) driverBackedObserverFixture {
	t.Helper()
	provision := testTenantProvision(t, name, 1)
	provision.ContentSourceID = "driver-authority"
	var err error
	provision, err = provisionTenantForTest(t, c, t.Context(), provision)
	if err != nil {
		t.Fatal(err)
	}
	fleet := reconcileSourceAuthorityFleetForTest(t, c, "driver-owner", 0, 1, "driver-authority")
	acknowledgeSourceAuthorityFleetForTest(t, c, fleet)
	declaration := sourceAuthorityDeclarationsForTest("driver-authority")[0].DeclarationDigest
	targets := sourceDriverTargetsForProvisions(t, provision)
	driver := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "observer-token", 0, 0x41,
	)
	roots := []SourceObserverRootRecord{{
		ID: "root", Generation: 1, Path: "/root", VolumeUUID: "volume", Inode: 1, Kind: 1,
	}}
	checkpoints := []SourceObserverCheckpointRecord{{Stream: "stream", RootEpoch: "epoch", EventID: 1}}
	configuration := sourceObserverConfigurationIdentityForTest(
		t, driver.Authority, causal.OperationID{0x31}, "stream", "epoch", roots, checkpoints,
	)
	configuration.FleetOwner = driver.FleetOwner
	configuration.FleetGeneration = driver.AuthorityGeneration
	stageSourceObserverConfigurationForTest(t, c, configuration, roots, checkpoints)
	identity := SourcePublicationStageIdentity{
		Authority: driver.Authority, FleetOwner: driver.FleetOwner,
		FleetGeneration: driver.AuthorityGeneration, DriverID: "test-driver",
		DeclarationDigest: driver.DeclarationDigest, Operation: causal.OperationID{0x32},
		Stream: "stream", RootEpoch: "epoch", Through: 0, Predecessor: 0,
	}
	if err := c.BeginSourcePublicationStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	return driverBackedObserverFixture{
		Provision: provision, Driver: driver, Identity: identity, HeaderMismatch: headerMismatch,
	}
}

func appendObserverPublicationPages(
	t *testing.T,
	c *Catalog,
	fixture driverBackedObserverFixture,
	indexCount int,
) SourcePublicationStageRef {
	t.Helper()
	if indexCount < 0 {
		t.Fatal("negative observer index count")
	}
	indices := make([]SourcePhysicalIndexRecord, indexCount)
	for index := range indices {
		indices[index] = observerIndexRecord(fmt.Sprintf("item-%06d", index))
	}
	var ref SourcePublicationStageRef
	for sequence, offset := uint64(0), 0; ; sequence++ {
		page := SourcePublicationStagePage{Sequence: sequence}
		capacity := SourcePublicationStagePageItemLimit
		if sequence == 0 {
			page = observerPublicationHeaderPage(fixture.Driver, sequence, fixture.HeaderMismatch)
			capacity -= 2
		}
		end := min(offset+capacity, len(indices))
		page.Index = indices[offset:end]
		offset = end
		page.Complete = offset == len(indices)
		var err error
		ref, err = c.AppendSourcePublicationStage(t.Context(), fixture.Identity, page)
		if err != nil {
			t.Fatalf("append observer page %d: %v", sequence, err)
		}
		if page.Complete {
			return ref
		}
	}
}

func observerPublicationHeaderPage(
	driver SourceDriverStageIdentity,
	sequence uint64,
	mismatch bool,
) SourcePublicationStagePage {
	operation := driver.SourceOperation
	if mismatch {
		operation[15] ^= 0xff
	}
	return SourcePublicationStagePage{
		Sequence: sequence,
		Header: &SourcePublicationStageHeader{
			Mode: SourceDelta, Predecessor: driver.Predecessor,
			Change: causal.ChangeSet{
				SourceAuthority: driver.Authority, SourceRevision: driver.Predecessor + 1,
				ChangeID: driver.ChangeID, OperationID: operation, Cause: driver.Cause,
				Origin: driver.Origin, OriginGeneration: driver.OriginGeneration,
			},
		},
		Affected: []SourcePublicationAffected{{Revision: driver.Predecessor + 1, Key: "config"}},
	}
}

func observerIndexRecord(relative string) SourcePhysicalIndexRecord {
	return SourcePhysicalIndexRecord{
		Authority: "driver-authority", RootID: "root", Relative: relative,
		FileIdentity: []byte("identity:" + relative), Kind: uint8(KindFile), Payload: []byte("payload:" + relative),
		MetadataFingerprint: sha256.Sum256([]byte("metadata:" + relative)),
		ContentFingerprint:  sha256.Sum256([]byte("content:" + relative)),
	}
}
