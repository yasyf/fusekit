package catalog

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

func TestSourceAuthorityFleetEmptyRemovalAndHigherGenerationReintroduction(t *testing.T) {
	store := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("product")

	first := reconcileSourceAuthorityFleetForTest(t, store, owner, 0, 1, "alpha", "beta")
	replayed, err := store.ReconcileSourceAuthorityFleet(t.Context(), first.request)
	if err != nil {
		t.Fatalf("ReconcileSourceAuthorityFleet(replay): %v", err)
	}
	if replayed != first.state {
		t.Fatalf("reconciled replay = %+v, want %+v", replayed, first.state)
	}
	current := acknowledgeSourceAuthorityFleetForTest(t, store, first)
	ackReplay, err := store.AcknowledgeSourceAuthorityFleet(t.Context(), first.ack)
	if err != nil {
		t.Fatalf("AcknowledgeSourceAuthorityFleet(replay): %v", err)
	}
	if ackReplay != current {
		t.Fatalf("ack replay = %+v, want %+v", ackReplay, current)
	}
	page, err := store.SourceAuthorityFleetPage(t.Context(), SourceAuthorityFleetPageRequest{
		Owner: owner, Generation: 1, Limit: 1,
	})
	if err != nil {
		t.Fatalf("SourceAuthorityFleetPage(first): %v", err)
	}
	if len(page.Declarations) != 1 || page.Declarations[0].Authority != "alpha" ||
		string(page.Declarations[0].DriverConfig) != "config:alpha" || page.Next != "alpha" {
		t.Fatalf("first page = %+v, want alpha with continuation", page)
	}
	secondPage, err := store.SourceAuthorityFleetPage(t.Context(), SourceAuthorityFleetPageRequest{
		Owner: owner, Generation: 1, After: page.Next, Limit: 1,
	})
	if err != nil {
		t.Fatalf("SourceAuthorityFleetPage(second): %v", err)
	}
	if len(secondPage.Declarations) != 1 ||
		secondPage.Declarations[0].Authority != "beta" || secondPage.Next != "" {
		t.Fatalf("second page = %+v, want terminal beta", secondPage)
	}

	epoch := [16]byte{1}
	fence := SourceAuthorityRuntimeFence{
		Owner: owner, Generation: 1, Authority: "alpha", Epoch: epoch,
	}
	if err := store.TakeoverSourceAuthorityRuntime(t.Context(), SourceAuthorityRuntimeTakeover{
		Ref: SourceAuthorityRuntimeRef{
			Owner: owner, Generation: 1, Authority: "alpha",
		},
		Epoch: epoch, Process: sourceAuthorityRuntimeProcessForTest("fleet-test"),
	}); err != nil {
		t.Fatalf("TakeoverSourceAuthorityRuntime: %v", err)
	}
	if err := store.OpenSourceAuthorityRuntime(t.Context(), fence); err != nil {
		t.Fatalf("OpenSourceAuthorityRuntime: %v", err)
	}
	second := reconcileSourceAuthorityFleetForTest(t, store, owner, 1, 2, "beta")
	alphaRetire := SourceAuthorityRetireRequest{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		Authority: "alpha", StageDigest: second.state.StageDigest,
	}
	if _, err := store.RetireSourceAuthority(t.Context(), alphaRetire); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("RetireSourceAuthority(open) = %v, want ErrMutationConflict", err)
	}
	if err := store.CloseSourceAuthorityRuntime(t.Context(), fence); err != nil {
		t.Fatalf("CloseSourceAuthorityRuntime: %v", err)
	}
	retired, err := store.RetireSourceAuthority(t.Context(), alphaRetire)
	if err != nil {
		t.Fatalf("RetireSourceAuthority: %v", err)
	}
	retiredReplay, err := store.RetireSourceAuthority(t.Context(), alphaRetire)
	if err != nil {
		t.Fatalf("RetireSourceAuthority(replay): %v", err)
	}
	if retiredReplay != retired {
		t.Fatalf("retirement replay = %+v, want %+v", retiredReplay, retired)
	}
	acknowledgeSourceAuthorityFleetForTest(t, store, second)

	staleOwnerDigest, err := SourceAuthorityFleetDigest([]causal.SourceAuthorityID{"beta"})
	if err != nil {
		t.Fatal(err)
	}
	staleDeclarations := sourceAuthorityDeclarationsForTest("beta")
	staleDeclarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(staleDeclarations)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileSourceAuthorityFleet(t.Context(), SourceAuthorityFleetReconcileRequest{
		Owner: "foreign", Generation: 1, Sequence: 0, Declarations: staleDeclarations,
		Complete: true, AuthorityCount: 1, AuthoritiesDigest: staleOwnerDigest,
		DeclarationsDigest: staleDeclarationsDigest,
	}); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("ReconcileSourceAuthorityFleet(stale owner) = %v, want ErrMutationConflict", err)
	}

	third := reconcileSourceAuthorityFleetForTest(t, store, owner, 2, 3, "alpha", "beta")
	acknowledgeSourceAuthorityFleetForTest(t, store, third)

	empty := reconcileSourceAuthorityFleetForTest(t, store, owner, 3, 4)
	for _, authority := range []causal.SourceAuthorityID{"alpha", "beta"} {
		request := SourceAuthorityRetireRequest{
			Owner: owner, ExpectedGeneration: 3, Generation: 4,
			Authority: authority, StageDigest: empty.state.StageDigest,
		}
		if _, err := store.RetireSourceAuthority(t.Context(), request); err != nil {
			t.Fatalf("RetireSourceAuthority(%s): %v", authority, err)
		}
	}
	emptyState := acknowledgeSourceAuthorityFleetForTest(t, store, empty)
	if emptyState.AuthorityCount != 0 {
		t.Fatalf("empty fleet count = %d, want 0", emptyState.AuthorityCount)
	}
	status, err := store.SourceAuthorityFleetHead(t.Context(), owner)
	if err != nil {
		t.Fatalf("SourceAuthorityFleetHead(empty): %v", err)
	}
	if status.Current == nil || *status.Current != emptyState || status.Pending != nil {
		t.Fatalf("empty fleet status = %+v, want current %+v", status, emptyState)
	}
}

func sourceAuthorityRuntimeProcessForTest(generation string) proc.Record {
	digest := sha256.Sum256([]byte(generation))
	var ownerGeneration proc.OwnerGeneration
	copy(ownerGeneration[:], digest[:len(ownerGeneration)])
	return proc.Record{
		RecoveryID: recoveryid.SourceOwner,
		PID:        4242,
		StartTime:  "source-authority-start",
		Boot:       "source-authority-boot",
		Comm:       "holder",
		Generation: ownerGeneration,
	}
}

func TestSourceAuthorityFleetPendingStageSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	firstStore, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open(first): %v", err)
	}
	owner := SourceAuthorityFleetOwnerID("product")
	authorities := []causal.SourceAuthorityID{"alpha", "beta"}
	declarations := sourceAuthorityDeclarationsForTest(authorities...)
	digest, err := SourceAuthorityFleetDigest(authorities)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := SourceAuthorityFleetReconcileRequest{
		Owner: owner, Generation: 1, Sequence: 0, Declarations: declarations[:1],
		AuthorityCount: 2, AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
	}
	firstState, err := firstStore.ReconcileSourceAuthorityFleet(t.Context(), firstRequest)
	if err != nil {
		t.Fatalf("ReconcileSourceAuthorityFleet(first): %v", err)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}
	restarted, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open(restarted): %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	status, err := restarted.SourceAuthorityFleetHead(t.Context(), owner)
	if err != nil {
		t.Fatalf("SourceAuthorityFleetHead(restarted): %v", err)
	}
	if status.Current != nil || status.Pending == nil || *status.Pending != firstState {
		t.Fatalf("restarted status = %+v, want pending %+v", status, firstState)
	}
	secondRequest := SourceAuthorityFleetReconcileRequest{
		Owner: owner, Generation: 1, Sequence: 1, Declarations: declarations[1:],
		Complete: true, AuthorityCount: 2, AuthoritiesDigest: digest,
		DeclarationsDigest: declarationsDigest,
	}
	secondState, err := restarted.ReconcileSourceAuthorityFleet(t.Context(), secondRequest)
	if err != nil {
		t.Fatalf("ReconcileSourceAuthorityFleet(second): %v", err)
	}
	ack := SourceAuthorityFleetAcknowledgement{
		Owner: owner, Generation: 1, AuthorityCount: 2,
		AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
		StageDigest: secondState.StageDigest,
	}
	if _, err := restarted.AcknowledgeSourceAuthorityFleet(t.Context(), ack); err != nil {
		t.Fatalf("AcknowledgeSourceAuthorityFleet: %v", err)
	}
}

func TestSourceAuthorityFleetRejectsNonterminalCompleteCountWithoutPoisoningStage(t *testing.T) {
	store := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("product")
	authorities := []causal.SourceAuthorityID{"alpha"}
	declarations := sourceAuthorityDeclarationsForTest(authorities...)
	digest, err := SourceAuthorityFleetDigest(authorities)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatal(err)
	}
	request := SourceAuthorityFleetReconcileRequest{
		Owner: owner, Generation: 1, Sequence: 0, Declarations: declarations,
		AuthorityCount: 1, AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
	}
	if _, err := store.ReconcileSourceAuthorityFleet(t.Context(), request); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("nonterminal complete-count page = %v, want ErrInvalidObject", err)
	}
	status, err := store.SourceAuthorityFleetHead(t.Context(), owner)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("SourceAuthorityFleetHead = %+v, %v, want ErrNotFound", status, err)
	}
	request.Complete = true
	state, err := store.ReconcileSourceAuthorityFleet(t.Context(), request)
	if err != nil {
		t.Fatalf("terminal retry: %v", err)
	}
	if !state.Complete || state.ReceivedCount != 1 {
		t.Fatalf("terminal state = %+v", state)
	}
}

type sourceAuthorityFleetTestStage struct {
	request SourceAuthorityFleetReconcileRequest
	state   SourceAuthorityFleetReconcileState
	ack     SourceAuthorityFleetAcknowledgement
}

func reconcileSourceAuthorityFleetForTest(
	t *testing.T,
	store *Catalog,
	owner SourceAuthorityFleetOwnerID,
	expected, generation causal.Generation,
	authorities ...causal.SourceAuthorityID,
) sourceAuthorityFleetTestStage {
	t.Helper()
	digest, err := SourceAuthorityFleetDigest(authorities)
	if err != nil {
		t.Fatalf("SourceAuthorityFleetDigest: %v", err)
	}
	declarations := sourceAuthorityDeclarationsForTest(authorities...)
	declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatalf("SourceAuthorityFleetDeclarationsDigest: %v", err)
	}
	request := SourceAuthorityFleetReconcileRequest{
		Owner: owner, ExpectedGeneration: expected, Generation: generation,
		Declarations: declarations, Complete: true, AuthorityCount: uint64(len(authorities)),
		AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
	}
	state, err := store.ReconcileSourceAuthorityFleet(t.Context(), request)
	if err != nil {
		t.Fatalf("ReconcileSourceAuthorityFleet: %v", err)
	}
	return sourceAuthorityFleetTestStage{
		request: request,
		state:   state,
		ack: SourceAuthorityFleetAcknowledgement{
			Owner: owner, ExpectedGeneration: expected, Generation: generation,
			AuthorityCount: uint64(len(authorities)), AuthoritiesDigest: digest,
			DeclarationsDigest: declarationsDigest,
			StageDigest:        state.StageDigest,
		},
	}
}

func sourceAuthorityDeclarationsForTest(
	authorities ...causal.SourceAuthorityID,
) []SourceAuthorityDeclaration {
	declarations := make([]SourceAuthorityDeclaration, len(authorities))
	for index, authority := range authorities {
		declarations[index] = SourceAuthorityDeclaration{
			Authority: authority, DriverID: "test-driver",
			DriverConfig:      []byte("config:" + authority),
			DeclarationDigest: sha256.Sum256([]byte("declaration:" + authority)),
		}
	}
	return declarations
}

func acknowledgeSourceAuthorityFleetForTest(
	t *testing.T,
	store *Catalog,
	stage sourceAuthorityFleetTestStage,
) SourceAuthorityFleetState {
	t.Helper()
	state, err := store.AcknowledgeSourceAuthorityFleet(t.Context(), stage.ack)
	if err != nil {
		t.Fatalf("AcknowledgeSourceAuthorityFleet: %v", err)
	}
	return state
}
