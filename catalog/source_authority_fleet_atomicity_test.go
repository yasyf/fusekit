package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceAuthorityFleetCrashRecoveryKeepsCurrentUntilAtomicAcknowledgement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c := openSourceAuthorityFleetAtomicityCatalog(t, path)
	t.Cleanup(func() { _ = c.Close() })
	owner := SourceAuthorityFleetOwnerID("fleet-crash-atomicity")
	first := reconcileSourceAuthorityFleetForTest(t, c, owner, 0, 1, "alpha", "beta", "gamma")
	acknowledgeSourceAuthorityFleetForTest(t, c, first)
	lastGood := sourceAuthorityFleetCurrentBytesForAtomicityTest(t, c, owner)

	desired := []causal.SourceAuthorityID{"delta", "gamma"}
	desiredDeclarations := sourceAuthorityFleetDeclarationsForAtomicityTest(desired)
	digest, err := SourceAuthorityFleetDigest(desired)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(desiredDeclarations)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := c.ReconcileSourceAuthorityFleet(
		t.Context(),
		SourceAuthorityFleetReconcileRequest{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			Declarations: desiredDeclarations[:1], AuthorityCount: 2,
			AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
		},
	)
	if err != nil {
		t.Fatalf("ReconcileSourceAuthorityFleet(partial): %v", err)
	}
	c = reopenSourceAuthorityFleetAtomicityCatalog(t, c, path)
	assertSourceAuthorityFleetCurrentBytesForAtomicityTest(t, c, owner, lastGood)
	status, err := c.SourceAuthorityFleetHead(t.Context(), owner)
	if err != nil || status.Pending == nil || *status.Pending != partial {
		t.Fatalf("partial stage after reopen = %+v, %v; want %+v", status, err, partial)
	}

	complete, err := c.ReconcileSourceAuthorityFleet(
		t.Context(),
		SourceAuthorityFleetReconcileRequest{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			Sequence: partial.NextSequence, Declarations: desiredDeclarations[1:], Complete: true,
			AuthorityCount: 2, AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
		},
	)
	if err != nil {
		t.Fatalf("ReconcileSourceAuthorityFleet(complete): %v", err)
	}
	assertSourceAuthorityFleetCurrentBytesForAtomicityTest(t, c, owner, lastGood)
	c = reopenSourceAuthorityFleetAtomicityCatalog(t, c, path)
	assertSourceAuthorityFleetCurrentBytesForAtomicityTest(t, c, owner, lastGood)

	for _, authority := range []causal.SourceAuthorityID{"alpha", "beta"} {
		request := SourceAuthorityRetireRequest{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			Authority: authority, StageDigest: complete.StageDigest,
		}
		retired, err := c.RetireSourceAuthority(t.Context(), request)
		if err != nil {
			t.Fatalf("RetireSourceAuthority(%s): %v", authority, err)
		}
		assertSourceAuthorityFleetCurrentBytesForAtomicityTest(t, c, owner, lastGood)
		c = reopenSourceAuthorityFleetAtomicityCatalog(t, c, path)
		replayed, err := c.RetireSourceAuthority(t.Context(), request)
		if err != nil {
			t.Fatalf("RetireSourceAuthority(%s replay): %v", authority, err)
		}
		if replayed != retired {
			t.Fatalf("retirement %s replay = %+v, want %+v", authority, replayed, retired)
		}
		assertSourceAuthorityFleetCurrentBytesForAtomicityTest(t, c, owner, lastGood)
	}

	c = reopenSourceAuthorityFleetAtomicityCatalog(t, c, path)
	assertSourceAuthorityFleetCurrentBytesForAtomicityTest(t, c, owner, lastGood)
	ack := SourceAuthorityFleetAcknowledgement{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		AuthorityCount: 2, AuthoritiesDigest: digest,
		DeclarationsDigest: declarationsDigest, StageDigest: complete.StageDigest,
	}
	acknowledged, err := c.AcknowledgeSourceAuthorityFleet(t.Context(), ack)
	if err != nil {
		t.Fatalf("AcknowledgeSourceAuthorityFleet: %v", err)
	}
	if acknowledged.Generation != 2 || acknowledged.AuthorityCount != 2 {
		t.Fatalf("acknowledged fleet = %+v", acknowledged)
	}
	assertSourceAuthorityFleetMembersForAtomicityTest(t, c, owner, 2, desired)
	if _, err := c.SourceAuthorityFleetPage(
		t.Context(),
		SourceAuthorityFleetPageRequest{Owner: owner, Generation: 1, Limit: 1},
	); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("generation-1 page after acknowledgement = %v, want generation mismatch", err)
	}

	c = reopenSourceAuthorityFleetAtomicityCatalog(t, c, path)
	replayedAck, err := c.AcknowledgeSourceAuthorityFleet(t.Context(), ack)
	if err != nil {
		t.Fatalf("AcknowledgeSourceAuthorityFleet(replay after reopen): %v", err)
	}
	if replayedAck != acknowledged {
		t.Fatalf("acknowledgement replay = %+v, want %+v", replayedAck, acknowledged)
	}
	assertSourceAuthorityFleetMembersForAtomicityTest(t, c, owner, 2, desired)
}

func TestSourceAuthorityFleetAbortIsExactAndRestoresLastGoodGeneration(t *testing.T) {
	t.Run("incomplete stage and higher generation", func(t *testing.T) {
		c := newTestCatalog(t)
		owner := SourceAuthorityFleetOwnerID("fleet-abort-incomplete")
		first := reconcileSourceAuthorityFleetForTest(t, c, owner, 0, 1, "last-good")
		acknowledgeSourceAuthorityFleetForTest(t, c, first)
		lastGood := sourceAuthorityFleetCurrentBytesForAtomicityTest(t, c, owner)
		desired := []causal.SourceAuthorityID{"next-a", "next-b"}
		declarations := sourceAuthorityFleetDeclarationsForAtomicityTest(desired)
		digest, err := SourceAuthorityFleetDigest(desired)
		if err != nil {
			t.Fatal(err)
		}
		declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(declarations)
		if err != nil {
			t.Fatal(err)
		}
		pending, err := c.ReconcileSourceAuthorityFleet(
			t.Context(),
			SourceAuthorityFleetReconcileRequest{
				Owner: owner, ExpectedGeneration: 1, Generation: 2,
				Declarations: declarations[:1], AuthorityCount: 2,
				AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		abort := SourceAuthorityFleetAbortRequest{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			StageDigest: pending.StageDigest,
		}
		wrong := abort
		wrong.StageDigest[0] ^= 0xff
		if _, err := c.AbortSourceAuthorityFleet(
			t.Context(), wrong,
		); !errors.Is(err, ErrMutationConflict) {
			t.Fatalf("wrong abort fence = %v, want ErrMutationConflict", err)
		}
		status, err := c.SourceAuthorityFleetHead(t.Context(), owner)
		if err != nil || status.Pending == nil || *status.Pending != pending {
			t.Fatalf("pending after wrong abort = %+v, %v", status, err)
		}
		aborted, err := c.AbortSourceAuthorityFleet(t.Context(), abort)
		if err != nil {
			t.Fatalf("AbortSourceAuthorityFleet: %v", err)
		}
		replayed, err := c.AbortSourceAuthorityFleet(t.Context(), abort)
		if err != nil || replayed != aborted {
			t.Fatalf("AbortSourceAuthorityFleet(replay) = %+v, %v; want %+v", replayed, err, aborted)
		}
		assertSourceAuthorityFleetCurrentBytesForAtomicityTest(t, c, owner, lastGood)
		status, err = c.SourceAuthorityFleetHead(t.Context(), owner)
		if err != nil || status.Pending != nil {
			t.Fatalf("status after abort = %+v, %v", status, err)
		}
		ref := SourceAuthorityRuntimeRef{
			Owner: owner, Generation: 1, Authority: "last-good",
		}
		epoch := [16]byte{1}
		if err := c.TakeoverSourceAuthorityRuntime(
			t.Context(),
			SourceAuthorityRuntimeTakeover{
				Ref: ref, Epoch: epoch,
				Process: sourceAuthorityRuntimeProcessForTest("abort-incomplete"),
			},
		); err != nil {
			t.Fatalf("take over last-good runtime after abort: %v", err)
		}
		fence := SourceAuthorityRuntimeFence{
			Owner: ref.Owner, Generation: ref.Generation, Authority: ref.Authority, Epoch: epoch,
		}
		if err := c.OpenSourceAuthorityRuntime(t.Context(), fence); err != nil {
			t.Fatalf("restart last-good runtime after abort: %v", err)
		}
		if err := c.CloseSourceAuthorityRuntime(t.Context(), fence); err != nil {
			t.Fatalf("close restarted last-good runtime: %v", err)
		}

		higher := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 3, "next")
		if _, err := c.RetireSourceAuthority(
			t.Context(),
			SourceAuthorityRetireRequest{
				Owner: owner, ExpectedGeneration: 1, Generation: 3,
				Authority: "last-good", StageDigest: higher.state.StageDigest,
			},
		); err != nil {
			t.Fatalf("RetireSourceAuthority(higher generation): %v", err)
		}
		acknowledgeSourceAuthorityFleetForTest(t, c, higher)
		assertSourceAuthorityFleetMembersForAtomicityTest(
			t, c, owner, 3, []causal.SourceAuthorityID{"next"},
		)
	})

	t.Run("published desired stage cannot be aborted", func(t *testing.T) {
		c := newTestCatalog(t)
		owner := SourceAuthorityFleetOwnerID("fleet-abort-complete")
		first := reconcileSourceAuthorityFleetForTest(t, c, owner, 0, 1, "keep", "remove")
		acknowledgeSourceAuthorityFleetForTest(t, c, first)
		pending := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2, "keep")
		if _, err := c.RetireSourceAuthority(
			t.Context(),
			SourceAuthorityRetireRequest{
				Owner: owner, ExpectedGeneration: 1, Generation: 2,
				Authority: "remove", StageDigest: pending.state.StageDigest,
			},
		); err != nil {
			t.Fatalf("RetireSourceAuthority: %v", err)
		}
		abort := SourceAuthorityFleetAbortRequest{
			Owner: owner, ExpectedGeneration: 1, Generation: 2,
			StageDigest: pending.state.StageDigest,
		}
		if _, err := c.AbortSourceAuthorityFleet(t.Context(), abort); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("AbortSourceAuthorityFleet = %v, want invalid transition", err)
		}
		status, err := c.SourceAuthorityFleetHead(t.Context(), owner)
		if err != nil || status.Pending == nil || *status.Pending != pending.state {
			t.Fatalf("status after rejected abort = %+v, %v; want pending %+v", status, err, pending.state)
		}
		var proofs int
		if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_authority_retirement_receipts
WHERE owner_id = ? AND generation = 2`, string(owner)).Scan(&proofs); err != nil {
			t.Fatal(err)
		}
		if proofs != 1 {
			t.Fatalf("retirement proofs after rejected abort = %d, want 1", proofs)
		}
		desired, err := c.DesiredSourceAuthorityFleet(t.Context(), owner)
		if err != nil || desired.Generation != 2 {
			t.Fatalf("desired fleet after rejected abort = %+v, %v", desired, err)
		}
		acknowledgeSourceAuthorityFleetForTest(t, c, pending)
		assertSourceAuthorityFleetMembersForAtomicityTest(t, c, owner, 2, []causal.SourceAuthorityID{"keep"})
	})

	t.Run("pending-only claim releases to competing owner", func(t *testing.T) {
		c := newTestCatalog(t)
		firstOwner := SourceAuthorityFleetOwnerID("fleet-abort-claim-first")
		secondOwner := SourceAuthorityFleetOwnerID("fleet-abort-claim-second")
		firstRequest, first := stageIncompleteSourceAuthorityFleetForTest(t, c, firstOwner, 0, 1, "shared")
		competing := firstRequest
		competing.Owner = secondOwner
		if _, err := c.ReconcileSourceAuthorityFleet(
			t.Context(), competing,
		); !errors.Is(err, ErrMutationConflict) {
			t.Fatalf("competing owner before abort = %v, want ErrMutationConflict", err)
		}
		if _, err := c.AbortSourceAuthorityFleet(
			t.Context(),
			SourceAuthorityFleetAbortRequest{
				Owner: firstOwner, Generation: 1, StageDigest: first.StageDigest,
			},
		); err != nil {
			t.Fatalf("AbortSourceAuthorityFleet: %v", err)
		}
		second := reconcileSourceAuthorityFleetForTest(t, c, secondOwner, 0, 1, "shared")
		acknowledgeSourceAuthorityFleetForTest(t, c, second)
		assertSourceAuthorityFleetMembersForAtomicityTest(
			t, c, secondOwner, 1, []causal.SourceAuthorityID{"shared"},
		)
	})
}

func stageIncompleteSourceAuthorityFleetForTest(
	t *testing.T,
	c *Catalog,
	owner SourceAuthorityFleetOwnerID,
	expected, generation causal.Generation,
	authorities ...causal.SourceAuthorityID,
) (SourceAuthorityFleetReconcileRequest, SourceAuthorityFleetReconcileState) {
	t.Helper()
	all := append(append([]causal.SourceAuthorityID(nil), authorities...), "zz-unpublished-sentinel")
	declarations := sourceAuthorityDeclarationsForTest(all...)
	authoritiesDigest, err := SourceAuthorityFleetDigest(all)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatal(err)
	}
	request := SourceAuthorityFleetReconcileRequest{
		Owner: owner, ExpectedGeneration: expected, Generation: generation,
		Declarations: declarations[:len(authorities)], AuthorityCount: uint64(len(all)),
		AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
	}
	state, err := c.ReconcileSourceAuthorityFleet(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	return request, state
}

func TestSourceAuthorityRetirementRejectsResidualMutationStateUntilSettled(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("fleet-retire-residual-mutation")
	owner := SourceAuthorityFleetOwnerID("test:" + authority)
	configureSourceObserverForIndexTest(t, c, authority)
	empty := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2)
	request := SourceAuthorityRetireRequest{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		Authority: authority, StageDigest: empty.state.StageDigest,
	}

	operation := MutationID{1}
	payload := []byte("residual mutation expectation")
	if err := reserveSourceMutationExpectationForTest(
		t, c,
		SourceMutationExpectationRecord{
			Operation: operation, Authority: authority, Tenant: "detached-tenant", Generation: 1,
			Origin: CausalOrigin{Cause: causal.CauseDaemonWrite},
			Digest: sha256.Sum256(payload), Payload: payload,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RetireSourceAuthority(
		t.Context(), request,
	); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("retirement with mutation expectation = %v, want ErrMutationConflict", err)
	}
	if err := c.CompleteSourceMutationExpectation(
		t.Context(), authority, operation, []byte("receipt"),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_mutation_expectations
SET state = ?
WHERE operation_id = ?`, SourceMutationExpectationRepairPublished, operation[:]); err != nil {
		t.Fatal(err)
	}
	if err := c.CompleteSourceMutationRepair(t.Context(), authority, operation); err != nil {
		t.Fatalf("CompleteSourceMutationRepair: %v", err)
	}

	reservation := MutationID{2}
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_key_reservations(source_authority, source_key, mutation_id)
VALUES (?, 'reserved', ?)`, string(authority), reservation[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RetireSourceAuthority(
		t.Context(), request,
	); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("retirement with key reservation = %v, want ErrMutationConflict", err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
DELETE FROM source_key_reservations
WHERE source_authority = ? AND mutation_id = ?`, string(authority), reservation[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RetireSourceAuthority(t.Context(), request); err != nil {
		t.Fatalf("retirement after durable mutation settlement: %v", err)
	}
}

func TestSourceAuthorityRetirementRejectsUnforgottenDriverReceipt(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("fleet-retire-driver-receipt")
	owner := SourceAuthorityFleetOwnerID("test:" + authority)
	configureSourceObserverForIndexTest(t, c, authority)
	empty := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2)
	request := SourceAuthorityRetireRequest{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		Authority: authority, StageDigest: empty.state.StageDigest,
	}
	operation := causal.OperationID{1}
	digest := sha256.Sum256([]byte("driver receipt"))
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_driver_stage_receipts(
    source_authority, stage_operation_id, mode, from_token, to_token, source_revision,
    target_count, targets_digest, stage_sequence, stage_item_count, stage_byte_count,
    stage_digest, identity_digest, result_json, result_digest,
    acknowledged, forgotten
) VALUES (?, ?, 1, '', 'head-1', 1, 1, ?, 1, 1, 1, ?, ?, X'7b7d', ?, 0, 0)`,
		string(authority), operation[:], digest[:], digest[:], digest[:], digest[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RetireSourceAuthority(t.Context(), request); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("retirement with pending driver receipt = %v, want ErrMutationConflict", err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_driver_stage_receipts SET acknowledged = 1, forgotten = 1
WHERE source_authority = ? AND stage_operation_id = ?`, string(authority), operation[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RetireSourceAuthority(t.Context(), request); err != nil {
		t.Fatalf("retirement after driver receipt settlement: %v", err)
	}
}

func TestSourceAuthorityFleetAcknowledgementRejectsRetainedMutationLiability(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("fleet-retained-mutation")
	owner := SourceAuthorityFleetOwnerID("test:" + authority)
	configureSourceObserverForIndexTest(t, c, authority)
	next := reconcileSourceAuthorityFleetForTest(t, c, owner, 1, 2, authority)
	operation := MutationID{19}
	payload := []byte("retained mutation liability")
	if err := reserveSourceMutationExpectationForTest(t, c, SourceMutationExpectationRecord{
		Operation: operation, Authority: authority, Tenant: "tenant", Generation: 1,
		Origin: CausalOrigin{Cause: causal.CauseDaemonWrite}, Digest: sha256.Sum256(payload), Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AcknowledgeSourceAuthorityFleet(t.Context(), next.ack); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("fleet acknowledgement with retained mutation liability = %v, want ErrMutationConflict", err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_mutation_expectations SET state = ?
WHERE source_authority = ? AND operation_id = ?`,
		SourceMutationExpectationRepairPublished, string(authority), operation[:]); err != nil {
		t.Fatal(err)
	}
	if err := c.CompleteSourceMutationRepair(t.Context(), authority, operation); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AcknowledgeSourceAuthorityFleet(t.Context(), next.ack); err != nil {
		t.Fatalf("fleet acknowledgement after mutation settlement: %v", err)
	}
}

func openSourceAuthorityFleetAtomicityCatalog(t *testing.T, path string) *Catalog {
	t.Helper()
	c, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func reopenSourceAuthorityFleetAtomicityCatalog(
	t *testing.T,
	c *Catalog,
	path string,
) *Catalog {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	return openSourceAuthorityFleetAtomicityCatalog(t, path)
}

func sourceAuthorityFleetCurrentBytesForAtomicityTest(
	t *testing.T,
	c *Catalog,
	owner SourceAuthorityFleetOwnerID,
) []byte {
	t.Helper()
	status, err := c.SourceAuthorityFleetHead(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if status.Current == nil {
		t.Fatal("source authority fleet has no current generation")
	}
	page, err := c.SourceAuthorityFleetPage(
		t.Context(),
		SourceAuthorityFleetPageRequest{
			Owner: owner, Generation: status.Current.Generation,
			Limit: SourceAuthorityFleetPageLimit,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(struct {
		Current SourceAuthorityFleetState
		Page    SourceAuthorityFleetPage
	}{Current: *status.Current, Page: page})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func assertSourceAuthorityFleetCurrentBytesForAtomicityTest(
	t *testing.T,
	c *Catalog,
	owner SourceAuthorityFleetOwnerID,
	want []byte,
) {
	t.Helper()
	got := sourceAuthorityFleetCurrentBytesForAtomicityTest(t, c, owner)
	if !bytes.Equal(got, want) {
		t.Fatalf("current fleet changed before acknowledgement:\n got %s\nwant %s", got, want)
	}
}

func assertSourceAuthorityFleetMembersForAtomicityTest(
	t *testing.T,
	c *Catalog,
	owner SourceAuthorityFleetOwnerID,
	generation causal.Generation,
	want []causal.SourceAuthorityID,
) {
	t.Helper()
	page, err := c.SourceAuthorityFleetPage(
		t.Context(),
		SourceAuthorityFleetPageRequest{
			Owner: owner, Generation: generation, Limit: SourceAuthorityFleetPageLimit,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]causal.SourceAuthorityID, len(page.Declarations))
	for index, declaration := range page.Declarations {
		got[index] = declaration.Authority
	}
	if page.Next != "" || !slices.Equal(got, want) {
		t.Fatalf("fleet members = %+v, want %v", page, want)
	}
	status, err := c.SourceAuthorityFleetHead(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if status.Current == nil || status.Current.Generation != generation || status.Pending != nil {
		t.Fatalf("fleet status = %+v, want current generation %d only", status, generation)
	}
}

func sourceAuthorityFleetDeclarationsForAtomicityTest(
	authorities []causal.SourceAuthorityID,
) []SourceAuthorityDeclaration {
	declarations := make([]SourceAuthorityDeclaration, len(authorities))
	for index, authority := range authorities {
		declarations[index] = SourceAuthorityDeclaration{
			Authority:         authority,
			DriverID:          "test-driver",
			DriverConfig:      []byte("config:" + authority),
			DeclarationDigest: sha256.Sum256([]byte("fusekit.test.fleet-atomicity\x00" + string(authority))),
		}
	}
	return declarations
}
