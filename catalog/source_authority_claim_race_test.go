package catalog

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceAuthorityFleetConcurrentOwnersCannotSharePendingClaim(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	first := openSourceAuthorityFleetAtomicityCatalog(t, path)
	defer func() {
		if err := first.Close(); err != nil {
			t.Errorf("close first catalog: %v", err)
		}
	}()
	second := openSourceAuthorityFleetAtomicityCatalog(t, path)
	defer func() {
		if err := second.Close(); err != nil {
			t.Errorf("close second catalog: %v", err)
		}
	}()

	authorities := []causal.SourceAuthorityID{"shared", "zz-unpublished-sentinel"}
	declarations := sourceAuthorityFleetDeclarationsForAtomicityTest(authorities)
	authoritiesDigest, err := SourceAuthorityFleetDigest(
		authorities,
	)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatal(err)
	}
	requests := []SourceAuthorityFleetReconcileRequest{
		{
			Owner: "claim-race-first", Generation: 1,
			Declarations: declarations[:1], AuthorityCount: 2,
			AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
		},
		{
			Owner: "claim-race-second", Generation: 1,
			Declarations: declarations[:1], AuthorityCount: 2,
			AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
		},
	}
	catalogs := []*Catalog{first, second}
	type result struct {
		state SourceAuthorityFleetReconcileState
		err   error
	}
	results := make([]result, len(catalogs))
	ready := make(chan struct{})
	var wait sync.WaitGroup
	for index := range catalogs {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-ready
			results[index].state, results[index].err = catalogs[index].ReconcileSourceAuthorityFleet(
				t.Context(), requests[index],
			)
		}(index)
	}
	close(ready)
	wait.Wait()

	winner, loser := -1, -1
	for index, result := range results {
		switch {
		case result.err == nil:
			if winner != -1 {
				t.Fatalf("both owners acquired the same pending claim: %+v", results)
			}
			winner = index
		case errors.Is(result.err, ErrMutationConflict):
			loser = index
		default:
			t.Fatalf("owner %d stage = %v, want success or ErrMutationConflict", index, result.err)
		}
	}
	if winner == -1 || loser == -1 {
		t.Fatalf("claim race results = %+v, want one winner and one conflict", results)
	}
	if _, err := first.AbortSourceAuthorityFleet(
		t.Context(),
		SourceAuthorityFleetAbortRequest{
			Owner: requests[winner].Owner, Generation: 1,
			StageDigest: results[winner].state.StageDigest,
		},
	); err != nil {
		t.Fatalf("abort winning pending claim: %v", err)
	}
	if _, err := second.ReconcileSourceAuthorityFleet(
		t.Context(), requests[loser],
	); err != nil {
		t.Fatalf("losing owner did not acquire released claim: %v", err)
	}
}
