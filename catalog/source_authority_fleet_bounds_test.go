package catalog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceAuthorityFleetReconcileAndCatalogPagingCrossHardPageBoundary(t *testing.T) {
	c := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("fleet-page-boundary")
	authorities := sourceAuthorityFleetIDsForBoundsTest(SourceAuthorityFleetPageLimit + 1)
	declarations := sourceAuthorityFleetDeclarationsForBoundsTest(authorities)
	digest, err := SourceAuthorityFleetDigest(authorities)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := SourceAuthorityFleetReconcileRequest{
		Owner: owner, Generation: 1, Sequence: 0,
		Declarations:   declarations[:SourceAuthorityFleetPageLimit],
		AuthorityCount: uint64(len(authorities)), AuthoritiesDigest: digest,
		DeclarationsDigest: declarationsDigest,
	}
	first, err := c.ReconcileSourceAuthorityFleet(t.Context(), firstRequest)
	if err != nil {
		t.Fatalf("ReconcileSourceAuthorityFleet(first): %v", err)
	}
	if first.NextSequence != 1 || first.ReceivedCount != SourceAuthorityFleetPageLimit ||
		first.Complete {
		t.Fatalf("first reconciliation state = %+v", first)
	}
	replayed, err := c.ReconcileSourceAuthorityFleet(t.Context(), firstRequest)
	if err != nil {
		t.Fatalf("ReconcileSourceAuthorityFleet(first replay): %v", err)
	}
	if replayed != first {
		t.Fatalf("first page replay = %+v, want %+v", replayed, first)
	}

	secondRequest := SourceAuthorityFleetReconcileRequest{
		Owner: owner, Generation: 1, Sequence: first.NextSequence,
		Declarations: declarations[SourceAuthorityFleetPageLimit:], Complete: true,
		AuthorityCount: uint64(len(authorities)), AuthoritiesDigest: digest,
		DeclarationsDigest: declarationsDigest,
	}
	second, err := c.ReconcileSourceAuthorityFleet(t.Context(), secondRequest)
	if err != nil {
		t.Fatalf("ReconcileSourceAuthorityFleet(second): %v", err)
	}
	if second.NextSequence != 2 || second.ReceivedCount != uint64(len(authorities)) ||
		!second.Complete {
		t.Fatalf("terminal reconciliation state = %+v", second)
	}
	acknowledged, err := c.AcknowledgeSourceAuthorityFleet(
		t.Context(),
		SourceAuthorityFleetAcknowledgement{
			Owner: owner, Generation: 1, AuthorityCount: uint64(len(authorities)),
			AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
			StageDigest: second.StageDigest,
		},
	)
	if err != nil {
		t.Fatalf("AcknowledgeSourceAuthorityFleet: %v", err)
	}

	var paged []causal.SourceAuthorityID
	after := causal.SourceAuthorityID("")
	for {
		page, err := c.SourceAuthorityFleetPage(
			t.Context(),
			SourceAuthorityFleetPageRequest{
				Owner: owner, Generation: acknowledged.Generation,
				After: after, Limit: SourceAuthorityFleetPageLimit,
			},
		)
		if err != nil {
			t.Fatalf("SourceAuthorityFleetPage(after %q): %v", after, err)
		}
		for _, declaration := range page.Declarations {
			paged = append(paged, declaration.Authority)
		}
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	if !slices.Equal(paged, authorities) {
		t.Fatalf("paged authorities = %v, want %v", paged, authorities)
	}
}

func TestValidateSourceAuthorityFleetOwnerIDUsesExactUTF8ByteBound(t *testing.T) {
	valid := []SourceAuthorityFleetOwnerID{
		"owner",
		SourceAuthorityFleetOwnerID(strings.Repeat("a", causal.SourceAuthorityIDMaxBytes)),
	}
	for _, owner := range valid {
		if err := ValidateSourceAuthorityFleetOwnerID(owner); err != nil {
			t.Fatalf("valid owner %q: %v", owner, err)
		}
	}
	invalid := []SourceAuthorityFleetOwnerID{
		"",
		"nul\x00owner",
		SourceAuthorityFleetOwnerID(string([]byte{0xff})),
		SourceAuthorityFleetOwnerID(strings.Repeat("a", causal.SourceAuthorityIDMaxBytes+1)),
	}
	for _, owner := range invalid {
		if err := ValidateSourceAuthorityFleetOwnerID(owner); !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("invalid owner %q = %v, want ErrInvalidObject", owner, err)
		}
	}
}

func TestSourceAuthorityFleetReconcileBoundsDoNotPoisonPendingState(t *testing.T) {
	t.Run("oversized single page", func(t *testing.T) {
		c := newTestCatalog(t)
		owner := SourceAuthorityFleetOwnerID("fleet-oversized-page")
		authorities := sourceAuthorityFleetIDsForBoundsTest(SourceAuthorityFleetPageLimit + 1)
		declarations := sourceAuthorityFleetDeclarationsForBoundsTest(authorities)
		digest, err := SourceAuthorityFleetDigest(authorities)
		if err != nil {
			t.Fatal(err)
		}
		declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(declarations)
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.ReconcileSourceAuthorityFleet(
			t.Context(),
			SourceAuthorityFleetReconcileRequest{
				Owner: owner, Generation: 1, Declarations: declarations, Complete: true,
				AuthorityCount: uint64(len(authorities)), AuthoritiesDigest: digest,
				DeclarationsDigest: declarationsDigest,
			},
		)
		if !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("oversized fleet page = %v, want ErrInvalidObject", err)
		}
		if status, err := c.SourceAuthorityFleetHead(t.Context(), owner); !errors.Is(err, ErrNotFound) {
			t.Fatalf("fleet after oversized page = %+v, %v, want ErrNotFound", status, err)
		}
	})

	t.Run("cumulative count", func(t *testing.T) {
		c := newTestCatalog(t)
		owner := SourceAuthorityFleetOwnerID("fleet-cumulative-count")
		authorities := sourceAuthorityFleetIDsForBoundsTest(SourceAuthorityFleetPageLimit)
		declarations := sourceAuthorityFleetDeclarationsForBoundsTest(authorities)
		digest, err := SourceAuthorityFleetDigest(authorities)
		if err != nil {
			t.Fatal(err)
		}
		declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(declarations)
		if err != nil {
			t.Fatal(err)
		}
		firstRequest := SourceAuthorityFleetReconcileRequest{
			Owner: owner, Generation: 1, Declarations: declarations[:len(declarations)-1],
			AuthorityCount: uint64(len(authorities)), AuthoritiesDigest: digest,
			DeclarationsDigest: declarationsDigest,
		}
		first, err := c.ReconcileSourceAuthorityFleet(t.Context(), firstRequest)
		if err != nil {
			t.Fatal(err)
		}
		badDeclarations := sourceAuthorityFleetDeclarationsForBoundsTest(
			[]causal.SourceAuthorityID{authorities[len(authorities)-1], "z-extra"},
		)
		badPage := SourceAuthorityFleetReconcileRequest{
			Owner: owner, Generation: 1, Sequence: first.NextSequence,
			Declarations: badDeclarations,
			Complete:     true, AuthorityCount: uint64(len(authorities)), AuthoritiesDigest: digest,
			DeclarationsDigest: declarationsDigest,
		}
		if _, err := c.ReconcileSourceAuthorityFleet(
			t.Context(), badPage,
		); !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("cumulative count overflow = %v, want ErrInvalidObject", err)
		}
		assertSourceAuthorityFleetPendingForBoundsTest(
			t, c, owner, first, causal.SourceAuthorityID("z-extra"),
		)

		terminal := SourceAuthorityFleetReconcileRequest{
			Owner: owner, Generation: 1, Sequence: first.NextSequence,
			Declarations: declarations[len(declarations)-1:],
			Complete:     true, AuthorityCount: uint64(len(authorities)), AuthoritiesDigest: digest,
			DeclarationsDigest: declarationsDigest,
		}
		if _, err := c.ReconcileSourceAuthorityFleet(t.Context(), terminal); err != nil {
			t.Fatalf("terminal page after rejected count overflow: %v", err)
		}
	})

	t.Run("cumulative bytes", func(t *testing.T) {
		c := newTestCatalog(t)
		owner := SourceAuthorityFleetOwnerID("fleet-cumulative-bytes")
		authorities := []causal.SourceAuthorityID{"authority-a", "authority-b"}
		declarations := sourceAuthorityFleetDeclarationsForBoundsTest(authorities)
		digest, err := SourceAuthorityFleetDigest(authorities)
		if err != nil {
			t.Fatal(err)
		}
		declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(declarations)
		if err != nil {
			t.Fatal(err)
		}
		firstRequest := SourceAuthorityFleetReconcileRequest{
			Owner: owner, Generation: 1, Declarations: declarations[:1],
			AuthorityCount: uint64(len(authorities)), AuthoritiesDigest: digest,
			DeclarationsDigest: declarationsDigest,
		}
		first, err := c.ReconcileSourceAuthorityFleet(t.Context(), firstRequest)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_authority_fleet_stages
SET byte_count = ?
WHERE owner_id = ? AND generation = 1`,
			SourceAuthorityFleetByteLimit, string(owner)); err != nil {
			t.Fatal(err)
		}
		terminal := SourceAuthorityFleetReconcileRequest{
			Owner: owner, Generation: 1, Sequence: first.NextSequence,
			Declarations: declarations[1:], Complete: true,
			AuthorityCount: uint64(len(authorities)), AuthoritiesDigest: digest,
			DeclarationsDigest: declarationsDigest,
		}
		if _, err := c.ReconcileSourceAuthorityFleet(
			t.Context(), terminal,
		); !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("cumulative byte overflow = %v, want ErrInvalidObject", err)
		}
		assertSourceAuthorityFleetPendingForBoundsTest(
			t, c, owner, first, authorities[1],
		)

		if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_authority_fleet_stages
SET byte_count = ?
WHERE owner_id = ? AND generation = 1`,
			sourceAuthorityDeclarationsBytes(declarations[:1]), string(owner)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.ReconcileSourceAuthorityFleet(t.Context(), terminal); err != nil {
			t.Fatalf("terminal page after rejected byte overflow: %v", err)
		}
	})
}

func sourceAuthorityFleetIDsForBoundsTest(count int) []causal.SourceAuthorityID {
	authorities := make([]causal.SourceAuthorityID, count)
	for index := range authorities {
		authorities[index] = causal.SourceAuthorityID(fmt.Sprintf("authority-%04d", index))
	}
	return authorities
}

func sourceAuthorityFleetDeclarationsForBoundsTest(
	authorities []causal.SourceAuthorityID,
) []SourceAuthorityDeclaration {
	declarations := make([]SourceAuthorityDeclaration, len(authorities))
	for index, authority := range authorities {
		declarations[index] = SourceAuthorityDeclaration{
			Authority: authority,
			DriverID:  "test-driver",
			DeclarationDigest: sha256.Sum256(
				[]byte("fusekit.test.fleet-bounds\x00" + string(authority)),
			),
		}
	}
	return declarations
}

func assertSourceAuthorityFleetPendingForBoundsTest(
	t *testing.T,
	c *Catalog,
	owner SourceAuthorityFleetOwnerID,
	want SourceAuthorityFleetReconcileState,
	rejectedAuthority causal.SourceAuthorityID,
) {
	t.Helper()
	status, err := c.SourceAuthorityFleetHead(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if status.Current != nil || status.Pending == nil || *status.Pending != want {
		t.Fatalf("fleet after rejected page = %+v, want pending %+v", status, want)
	}
	var rejectedMembers int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_authority_fleet_stage_members
WHERE owner_id = ? AND generation = ? AND source_authority = ?`,
		string(owner), uint64(want.Generation), string(rejectedAuthority),
	).Scan(&rejectedMembers); err != nil {
		t.Fatal(err)
	}
	if rejectedMembers != 0 {
		t.Fatalf("rejected authority %q persisted %d times", rejectedAuthority, rejectedMembers)
	}
}
