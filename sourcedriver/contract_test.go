package sourcedriver

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestPageDigestsAreDeterministicAndBindCursorChain(t *testing.T) {
	object := testProjection("a")
	targetSet := testTargetSetRef(t, 1, 1, [32]byte{1},
		TargetDeclaration{Tenant: object.Tenant, Generation: object.Generation})
	nilDigest, err := SnapshotPageDigest("head-1", []Projection{object})
	if err != nil {
		t.Fatalf("SnapshotPageDigest: %v", err)
	}
	again, err := SnapshotPageDigest("head-1", []Projection{object})
	if err != nil || again != nilDigest {
		t.Fatalf("deterministic digest = %x, %v; want %x", again, err, nilDigest)
	}
	emptyNil, err := SnapshotPageDigest("head-1", nil)
	if err != nil {
		t.Fatalf("nil page digest: %v", err)
	}
	emptySlice, err := SnapshotPageDigest("head-1", []Projection{})
	if err != nil || emptySlice != emptyNil {
		t.Fatalf("nil and empty page digests differ: %x != %x (%v)", emptyNil, emptySlice, err)
	}
	cursor, err := NewPageCursor(
		targetSet, PageSnapshot, "", "head-1", 1, 10,
		PagePosition{Tenant: object.Tenant, Generation: object.Generation, ID: object.ID}, nil, nilDigest,
	)
	if err != nil {
		t.Fatalf("NewPageCursor: %v", err)
	}
	request := SnapshotRequest{TargetSet: targetSet, Revision: "head-1", Limit: 10}
	page := SnapshotPage{Revision: "head-1", Objects: []Projection{object}, Next: &cursor, Digest: nilDigest}
	if err := ValidateSnapshotPage(request, page); err != nil {
		t.Fatalf("ValidateSnapshotPage: %v", err)
	}
	page.Next.After = "forged"
	if err := ValidateSnapshotPage(request, page); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("tampered cursor error = %v, want integrity", err)
	}
}

func TestPagesFailClosedOnOrderingCountAndByteBounds(t *testing.T) {
	first := testProjection("b")
	second := testProjection("a")
	targetSet := testTargetSetRef(t, 1, 1, [32]byte{1},
		TargetDeclaration{Tenant: first.Tenant, Generation: first.Generation})
	digest, err := SnapshotPageDigest("head-1", []Projection{first, second})
	if err != nil {
		t.Fatalf("SnapshotPageDigest: %v", err)
	}
	err = ValidateSnapshotPage(
		SnapshotRequest{TargetSet: targetSet, Revision: "head-1", Limit: 2},
		SnapshotPage{Revision: "head-1", Objects: []Projection{first, second}, Digest: digest},
	)
	if !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("unordered page error = %v, want invalid value", err)
	}

	objects := make([]Projection, MaxPageItems)
	for index := range objects {
		id := LogicalID(fmt.Sprintf("%04d-%s", index, strings.Repeat("x", LogicalIDMaxBytes-5)))
		objects[index] = testProjection(id)
		objects[index].Parent = LogicalID(strings.Repeat("p", LogicalIDMaxBytes))
		objects[index].Content.Object = id
	}
	if _, err := SnapshotPageDigest("head-1", objects); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("oversized page digest error = %v, want invalid value", err)
	}
}

func TestMutationReceiptIsExactAndStaleErrorsStayTyped(t *testing.T) {
	id := catalog.MutationID{1}
	receipt := MutationReceipt{
		OperationID: id, State: MutationApplied, RequestDigest: [32]byte{2},
		Expected: "head-1", Committed: "head-2", Result: "created",
	}
	digest, err := MutationReceiptDigest(receipt)
	if err != nil {
		t.Fatalf("MutationReceiptDigest: %v", err)
	}
	receipt.Digest = digest
	if err := ValidateMutationReceipt(receipt); err != nil {
		t.Fatalf("ValidateMutationReceipt: %v", err)
	}
	receipt.Committed = "head-3"
	if err := ValidateMutationReceipt(receipt); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("tampered receipt error = %v, want integrity", err)
	}
	var stale *StaleRevisionError
	if !errors.As(&StaleRevisionError{Expected: "head-1", Actual: "head-2"}, &stale) || stale.Actual != "head-2" {
		t.Fatal("stale revision error lost its exact actual revision")
	}
}

func TestMutationSettlementRequiresExactAuthorityAndReceiptProof(t *testing.T) {
	targetSet := testTargetSetRef(t, 7, 1, [32]byte{1},
		TargetDeclaration{Tenant: "tenant", Generation: 1})
	settlement := MutationSettlement{
		TargetSet:     targetSet,
		OperationID:   catalog.MutationID{2},
		RequestDigest: [sha256.Size]byte{3},
		ReceiptDigest: [sha256.Size]byte{4},
		Kind:          MutationSettlementAcknowledge,
	}
	for _, kind := range []MutationSettlementKind{
		MutationSettlementAcknowledge,
		MutationSettlementAbandon,
		MutationSettlementForget,
	} {
		settlement.Kind = kind
		if err := ValidateMutationSettlement(settlement); err != nil {
			t.Fatalf("ValidateMutationSettlement(%d): %v", kind, err)
		}
	}
	for name, mutate := range map[string]func(*MutationSettlement){
		"target set":     func(value *MutationSettlement) { value.TargetSet = TargetSetRef{} },
		"operation id":   func(value *MutationSettlement) { value.OperationID = catalog.MutationID{} },
		"request digest": func(value *MutationSettlement) { value.RequestDigest = [32]byte{} },
		"receipt digest": func(value *MutationSettlement) { value.ReceiptDigest = [32]byte{} },
		"kind":           func(value *MutationSettlement) { value.Kind = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := settlement
			mutate(&invalid)
			if err := ValidateMutationSettlement(invalid); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("ValidateMutationSettlement = %v, want invalid value", err)
			}
		})
	}
}

func TestLogicalIDsAreExactCatalogKeys(t *testing.T) {
	for _, id := range []LogicalID{"a/b", `a\b`, "a\x1fb", LogicalID(strings.Repeat("x", LogicalIDMaxBytes+1))} {
		projection := testProjection("valid")
		projection.ID = id
		if err := ValidateProjection(projection); !errors.Is(err, ErrInvalidValue) {
			t.Fatalf("ValidateProjection(%q) = %v, want invalid value", id, err)
		}
	}
}

func TestAuthorityWideTupleOrderingAndTargetFences(t *testing.T) {
	left := testProjection("same")
	right := testProjection("same")
	left.Tenant, right.Tenant = "tenant-a", "tenant-b"
	left.Content.Tenant, right.Content.Tenant = left.Tenant, right.Tenant
	targetSet := testTargetSetRef(t, 7, 1, [32]byte{9},
		TargetDeclaration{Tenant: left.Tenant, Generation: left.Generation},
		TargetDeclaration{Tenant: right.Tenant, Generation: right.Generation},
	)
	digest, err := SnapshotPageDigest("head-1", []Projection{left, right})
	if err != nil {
		t.Fatal(err)
	}
	request := SnapshotRequest{TargetSet: targetSet, Revision: "head-1", Limit: 2}
	page := SnapshotPage{Revision: "head-1", Objects: []Projection{left, right}, Digest: digest}
	if err := ValidateSnapshotPage(request, page); err != nil {
		t.Fatalf("same logical ID in ordered targets: %v", err)
	}
	duplicate := []Projection{left, left}
	duplicateDigest, err := SnapshotPageDigest("head-1", duplicate)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateSnapshotPage(request, SnapshotPage{
		Revision: "head-1", Objects: duplicate, Digest: duplicateDigest,
	}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("duplicate projection tuple = %v, want invalid value", err)
	}
	reordered := []Projection{right, left}
	reorderedDigest, err := SnapshotPageDigest("head-1", reordered)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateSnapshotPage(request, SnapshotPage{
		Revision: "head-1", Objects: reordered, Digest: reorderedDigest,
	}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("reordered projection tuple = %v, want invalid value", err)
	}
	page.Objects[1].Content.Tenant = left.Tenant
	page.Digest, err = SnapshotPageDigest("head-1", page.Objects)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateSnapshotPage(request, page); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("cross-target content ref = %v, want invalid value", err)
	}

	changes := []Change{
		{Kind: ChangeDelete, Tenant: "tenant-a", Generation: 1, Sequence: 4, ID: "z"},
		{Kind: ChangeDelete, Tenant: "tenant-a", Generation: 1, Sequence: 5, ID: "a"},
	}
	changeDigest, err := ChangePageDigest("head-1", "head-2", changes)
	if err != nil {
		t.Fatal(err)
	}
	changeRequest := ChangesRequest{TargetSet: targetSet, From: "head-1", To: "head-2", Limit: 2}
	if err := ValidateChangePage(changeRequest, ChangePage{
		From: "head-1", To: "head-2", Changes: changes, Digest: changeDigest,
	}); err != nil {
		t.Fatalf("sequence-before-ID ordering: %v", err)
	}
	changes[1].Sequence = 3
	changeDigest, err = ChangePageDigest("head-1", "head-2", changes)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateChangePage(changeRequest, ChangePage{
		From: "head-1", To: "head-2", Changes: changes, Digest: changeDigest,
	}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("decreasing change sequence = %v, want invalid value", err)
	}
}

func TestCursorRejectsCompositeReplayAndOversizedContinuation(t *testing.T) {
	declaration := [32]byte{1}
	previous := sha256.Sum256([]byte("prior"))
	targets := []TargetDeclaration{{Tenant: "tenant", Generation: 2}}
	targetSet := testTargetSetRef(t, 4, 1, declaration, targets...)
	cursor, err := NewPageCursor(
		targetSet, PageSnapshot, "", "head-1", 1, 1,
		PagePosition{Tenant: "tenant", Generation: 2, ID: "item"}, []byte("opaque"), previous,
	)
	if err != nil {
		t.Fatal(err)
	}
	request := SnapshotRequest{TargetSet: testTargetSetRef(t, 5, 1, declaration, targets...),
		Revision: "head-1", Cursor: &cursor, Limit: 1}
	if err := ValidateSnapshotRequest(request); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("cross-generation cursor = %v, want invalid value", err)
	}
	request.TargetSet = testTargetSetRef(t, 4, 1, [32]byte{2}, targets...)
	if err := ValidateSnapshotRequest(request); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("cross-declaration cursor = %v, want invalid value", err)
	}
	request.TargetSet = targetSet
	request.Limit = 2
	if err := ValidateSnapshotRequest(request); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("cross-limit cursor = %v, want invalid value", err)
	}
	_, err = NewPageCursor(
		targetSet, PageSnapshot, "", "head-1", 1, 1,
		PagePosition{Tenant: "tenant", Generation: 2, ID: "item"},
		make([]byte, MaxContinuationBytes+1), previous,
	)
	if !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("oversized continuation = %v, want invalid value", err)
	}
}

func TestPagesRejectOverlapAcrossCursorBoundaries(t *testing.T) {
	declaration := [32]byte{1}
	object := testProjection("item")
	targetSet := testTargetSetRef(t, 1, 1, declaration,
		TargetDeclaration{Tenant: object.Tenant, Generation: object.Generation})
	firstDigest, err := SnapshotPageDigest("head-1", []Projection{object})
	if err != nil {
		t.Fatal(err)
	}
	snapshotCursor, err := NewPageCursor(
		targetSet, PageSnapshot, "", "head-1", 1, 1,
		PagePosition{Tenant: object.Tenant, Generation: object.Generation, ID: object.ID}, nil, firstDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateSnapshotPage(SnapshotRequest{
		TargetSet: targetSet, Revision: "head-1", Cursor: &snapshotCursor, Limit: 1,
	}, SnapshotPage{Revision: "head-1", Objects: []Projection{object}, Digest: firstDigest}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("overlapping snapshot page = %v, want invalid value", err)
	}

	change := Change{Kind: ChangeDelete, Tenant: object.Tenant, Generation: object.Generation, Sequence: 2, ID: object.ID}
	changeDigest, err := ChangePageDigest("head-1", "head-2", []Change{change})
	if err != nil {
		t.Fatal(err)
	}
	changeCursor, err := NewPageCursor(
		targetSet, PageChanges, "head-1", "head-2", 1, 1,
		PagePosition{Tenant: change.Tenant, Generation: change.Generation, Sequence: change.Sequence, ID: change.ID}, nil, changeDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateChangePage(ChangesRequest{
		TargetSet: targetSet, From: "head-1", To: "head-2", Cursor: &changeCursor, Limit: 1,
	}, ChangePage{From: "head-1", To: "head-2", Changes: []Change{change}, Digest: changeDigest}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("overlapping change page = %v, want invalid value", err)
	}
}

func TestPageRequestsCarryOnlyTargetSetReference(t *testing.T) {
	targetSetType := reflect.TypeFor[TargetSetRef]()
	for _, value := range []any{SnapshotRequest{}, ChangesRequest{}, PageCursor{}} {
		typeOf := reflect.TypeOf(value)
		field, found := typeOf.FieldByName("TargetSet")
		if !found || field.Type != targetSetType {
			t.Fatalf("%s TargetSet field = %v, found=%t", typeOf, field.Type, found)
		}
		for index := 0; index < typeOf.NumField(); index++ {
			candidate := typeOf.Field(index)
			if candidate.Name == "Targets" || (candidate.Type.Kind() == reflect.Slice && candidate.Type.Elem() == reflect.TypeFor[TargetDeclaration]()) {
				t.Fatalf("%s embeds target declarations in field %s", typeOf, candidate.Name)
			}
		}
	}
}

func TestTargetSetPageReplayIsExactAndEpochFenced(t *testing.T) {
	authority := causal.SourceAuthorityID("test-authority")
	declaration := [32]byte{4}
	targetsA := []TargetDeclaration{{Tenant: "tenant-a", Generation: 1}, {Tenant: "tenant-b", Generation: 2}}
	refA1 := testTargetSetRef(t, 3, 1, declaration, targetsA...)
	state, err := NewTargetSetState(authority, refA1)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewTargetSetPage(state, targetsA[:1])
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := ApplyTargetSetPage(state, first)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := ApplyTargetSetPage(advanced, first)
	if err != nil || replayed != advanced {
		t.Fatalf("exact replay = %+v, %v, want %+v", replayed, err, advanced)
	}
	forged := first
	forged.Targets = append([]TargetDeclaration(nil), first.Targets...)
	forged.Targets[0].Generation++
	if _, err := ApplyTargetSetPage(advanced, forged); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("same page digest with different body = %v, want integrity", err)
	}
	last, err := NewTargetSetPage(advanced, targetsA[1:])
	if err != nil {
		t.Fatal(err)
	}
	complete, err := ApplyTargetSetPage(advanced, last)
	if err != nil || !complete.Complete || complete.DeclaredCount != uint64(len(targetsA)) {
		t.Fatalf("complete target set = %+v, %v", complete, err)
	}

	targetsB := []TargetDeclaration{{Tenant: "tenant-c", Generation: 1}}
	refB2 := testTargetSetRef(t, 3, 2, declaration, targetsB...)
	refA3 := testTargetSetRef(t, 3, 3, declaration, targetsA...)
	if refA1.ID == refB2.ID || refA1.ID == refA3.ID || refB2.ID == refA3.ID {
		t.Fatal("target-set identity did not bind monotonically changing target epoch")
	}
	forgedEpoch := refA1
	forgedEpoch.TargetEpoch = refA3.TargetEpoch
	if err := ValidateTargetSetRef(authority, forgedEpoch); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("old identity with current epoch = %v, want integrity", err)
	}
	previous := sha256.Sum256([]byte("previous"))
	cursor, err := NewPageCursor(refA1, PageSnapshot, "", "head", 1, 1,
		PagePosition{Tenant: "tenant-a", Generation: 1, ID: "item"}, nil, previous)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateSnapshotRequest(SnapshotRequest{
		TargetSet: refA3, Revision: "head", Cursor: &cursor, Limit: 1,
	}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("ABA cursor reuse = %v, want invalid value", err)
	}
}

func TestMaximumTargetSetStreamsInBoundedPages(t *testing.T) {
	targets := make([]TargetDeclaration, MaxTargets)
	for index := range targets {
		targets[index] = TargetDeclaration{Tenant: catalog.TenantID(fmt.Sprintf("tenant-%05d", index)), Generation: 1}
	}
	ref := testTargetSetRef(t, 1, 1, [32]byte{1}, targets...)
	state, err := NewTargetSetState("test-authority", ref)
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(targets); offset += MaxTargetPageItems {
		end := min(offset+MaxTargetPageItems, len(targets))
		page, err := NewTargetSetPage(state, targets[offset:end])
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Targets) > MaxTargetPageItems || len(encoded) > MaxPageBytes {
			t.Fatalf("target page items=%d bytes=%d exceed per-call bounds", len(page.Targets), len(encoded))
		}
		state, err = ApplyTargetSetPage(state, page)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !state.Complete || state.DeclaredCount != MaxTargets || state.RollingDigest != ref.TargetsDigest {
		t.Fatalf("maximum target declaration = %+v", state)
	}
}

func testTargetSetRef(
	t *testing.T,
	authorityGeneration causal.Generation,
	targetEpoch uint64,
	declarationDigest [32]byte,
	targets ...TargetDeclaration,
) TargetSetRef {
	t.Helper()
	ref, err := NewTargetSetRef(
		"test-authority", authorityGeneration, targetEpoch, declarationDigest, targets,
	)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func testProjection(id LogicalID) Projection {
	hash := catalog.ContentHash(sha256.Sum256([]byte(id)))
	content := ContentRef{
		Revision: "head-1", Tenant: "tenant-1", Generation: 1,
		Object: id, Size: int64(len(id)), Hash: hash,
	}
	return Projection{
		Tenant: "tenant-1", Generation: causal.Generation(1), ID: id, Parent: "root", Name: string(id),
		Kind: catalog.KindFile, Mode: 0o600, Visibility: catalog.Visibility{Mount: true},
		Size: content.Size, Hash: hash, Content: &content,
	}
}
