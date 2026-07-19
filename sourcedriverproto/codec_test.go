package sourcedriverproto

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourcedriver"
)

func TestDecodeRejectsOldUnknownDuplicateAndOversizedMessages(t *testing.T) {
	tests := [][]byte{
		[]byte(`{"protocol":0}`),
		[]byte(`{"protocol":2,"unknown":true}`),
		[]byte(`{"protocol":2,"protocol":2}`),
		bytes.Repeat([]byte(" "), int(MaxPageBytes)+1),
	}
	for _, input := range tests {
		var request RefreshRequest
		if err := Decode(input, &request); err == nil {
			t.Fatalf("Decode(%d bytes) unexpectedly succeeded", len(input))
		}
	}
	var old RefreshRequest
	if err := Decode(tests[0], &old); !errors.Is(err, ErrProtocol) {
		t.Fatalf("old protocol error = %v, want ErrProtocol", err)
	}
}

func TestMutationSettlementWireProofIsExact(t *testing.T) {
	_, targetSet := protocolTargetSetForTest(t, 4, 7, []sourcedriver.TargetDeclaration{{Tenant: "tenant", Generation: 1}})
	message := MutationSettlement{
		TargetSet:     targetSet,
		OperationID:   strings.Repeat("2", 64),
		RequestDigest: strings.Repeat("3", 64),
		ReceiptDigest: strings.Repeat("4", 64),
		Kind:          MutationSettlementAcknowledge,
	}
	for _, kind := range []MutationSettlementKind{
		MutationSettlementAcknowledge,
		MutationSettlementAbandon,
		MutationSettlementForget,
	} {
		message.Kind = kind
		if err := Validate(SettleMutationRequest{Protocol: Version, Settlement: message}); err != nil {
			t.Fatalf("Validate settlement %q: %v", kind, err)
		}
	}
	for name, mutate := range map[string]func(*MutationSettlement){
		"target set": func(value *MutationSettlement) { value.TargetSet.ID = "" },
		"operation":  func(value *MutationSettlement) { value.OperationID = "" },
		"request":    func(value *MutationSettlement) { value.RequestDigest = "" },
		"receipt":    func(value *MutationSettlement) { value.ReceiptDigest = "" },
		"kind":       func(value *MutationSettlement) { value.Kind = "invalid" },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := message
			mutate(&invalid)
			if err := Validate(SettleMutationRequest{Protocol: Version, Settlement: invalid}); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("Validate forged settlement = %v", err)
			}
		})
	}
}

func TestCursorDigestIsBoundToRevisionPagePositionAndPreviousDigest(t *testing.T) {
	previous := sha256.Sum256([]byte("page-one"))
	targetSet, protocolTargetSet := protocolTargetSetForTest(
		t, 9, 7, []sourcedriver.TargetDeclaration{{Tenant: "tenant", Generation: 3}},
	)
	cursor, err := sourcedriver.NewPageCursor(
		targetSet, sourcedriver.PageChanges, "from", "to", 2, 16,
		sourcedriver.PagePosition{Tenant: "tenant", Generation: 3, Sequence: 7, ID: "object-9"},
		[]byte("opaque"), previous,
	)
	if err != nil {
		t.Fatalf("NewPageCursor: %v", err)
	}
	message := PageCursor{
		TargetSet: protocolTargetSet,
		Kind:      PageKindChanges, From: "from", To: "to", Page: 2, Limit: 16,
		AfterTenant: "tenant", AfterGeneration: 3, AfterSequence: 7, After: "object-9",
		Continuation:   "b3BhcXVl",
		PreviousDigest: stringHex(previous[:]), Digest: stringHex(cursor.Digest[:]),
	}
	if err := Validate(message); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	for name, mutate := range map[string]func(*PageCursor){
		"target_set": func(value *PageCursor) { value.TargetSet.ID = strings.Repeat("0", 32) },
		"kind":       func(value *PageCursor) { value.Kind = PageKindSnapshot },
		"from":       func(value *PageCursor) { value.From = "other" },
		"to":         func(value *PageCursor) { value.To = "other" },
		"page":       func(value *PageCursor) { value.Page++ },
		"limit":      func(value *PageCursor) { value.Limit++ },
		"tenant":     func(value *PageCursor) { value.AfterTenant = "other" },
		"generation": func(value *PageCursor) { value.AfterGeneration++ },
		"sequence":   func(value *PageCursor) { value.AfterSequence++ },
		"continue":   func(value *PageCursor) { value.Continuation = "Zm9yZ2Vk" },
		"after":      func(value *PageCursor) { value.After = "other" },
		"previous":   func(value *PageCursor) { value.PreviousDigest = strings.Repeat("0", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			forged := message
			mutate(&forged)
			if err := Validate(forged); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("Validate forged cursor = %v", err)
			}
		})
	}
}

func TestEncodeEnforcesExactMessageByteBudget(t *testing.T) {
	objects := make([]Projection, MaxPageItems)
	for index := range objects {
		objects[index] = Projection{
			Tenant: "tenant", Generation: uint64(index + 1), ID: strings.Repeat("i", 255),
			Parent: strings.Repeat("p", 255), Name: strings.Repeat("n", 255), Kind: ObjectKindSymlink,
			LinkTarget: strings.Repeat("t", 4096), MountVisible: true,
		}
	}
	_, err := Encode(SnapshotResponse{
		Protocol: Version, Code: ErrorCodeOK, Revision: "head", Objects: objects, Digest: strings.Repeat("0", 64),
	})
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Encode oversized page = %v, want invalid message", err)
	}
}

func TestTupleOrderAllowsSameLogicalIDAcrossTenants(t *testing.T) {
	left := Projection{Tenant: "a", Generation: 1, ID: "same", Name: "left", Kind: ObjectKindDirectory, MountVisible: true}
	right := Projection{Tenant: "b", Generation: 1, ID: "same", Name: "right", Kind: ObjectKindDirectory, MountVisible: true}
	domainLeft := sourcedriver.Projection{Tenant: catalog.TenantID(left.Tenant), Generation: causal.Generation(left.Generation), ID: "same", Name: left.Name, Kind: catalog.KindDirectory, Visibility: catalog.Visibility{Mount: true}}
	domainRight := sourcedriver.Projection{Tenant: catalog.TenantID(right.Tenant), Generation: causal.Generation(right.Generation), ID: "same", Name: right.Name, Kind: catalog.KindDirectory, Visibility: catalog.Visibility{Mount: true}}
	digest, err := sourcedriver.SnapshotPageDigest("head", []sourcedriver.Projection{domainLeft, domainRight})
	if err != nil {
		t.Fatal(err)
	}
	message := SnapshotResponse{Protocol: Version, Code: ErrorCodeOK, Revision: "head", Objects: []Projection{left, right}, Digest: stringHex(digest[:])}
	if err := Validate(message); err != nil {
		t.Fatalf("tuple-ordered duplicate logical IDs: %v", err)
	}
}

func TestRequestRejectsForgedTargetDeclaration(t *testing.T) {
	domainTargets := []sourcedriver.TargetDeclaration{
		{Tenant: "tenant-a", Generation: 1},
		{Tenant: "tenant-b", Generation: 2},
	}
	_, targetSet := protocolTargetSetForTest(t, 3, 5, domainTargets)
	request := SnapshotRequest{
		Protocol: Version, TargetSet: targetSet,
		Revision: "head", Limit: 2,
	}
	if err := Validate(request); err != nil {
		t.Fatalf("valid target declaration: %v", err)
	}
	request.TargetSet.ID = ""
	if err := Validate(request); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("forged target declaration = %v, want invalid message", err)
	}
}

func TestTenThousandTargetsSerializeOnceAndSourceRequestsStayConstant(t *testing.T) {
	targets := make([]sourcedriver.TargetDeclaration, sourcedriver.MaxTargets)
	for index := range targets {
		targets[index] = sourcedriver.TargetDeclaration{
			Tenant: catalog.TenantID(fmt.Sprintf("tenant-%05d", index)), Generation: causal.Generation(index + 1),
		}
	}
	ref, protocolRef := protocolTargetSetForTest(t, 3, 9, targets)
	state, err := sourcedriver.NewTargetSetState("source-authority", ref)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	serializedTargets := 0
	for !state.Complete {
		end := min(int(state.DeclaredCount)+sourcedriver.MaxTargetPageItems, len(targets))
		page, err := sourcedriver.NewTargetSetPage(state, targets[state.DeclaredCount:end])
		if err != nil {
			t.Fatal(err)
		}
		message := DeclareTargetSetRequest{Protocol: Version, Page: protocolTargetSetPageForTest(page)}
		payload, err := Encode(message)
		if err != nil {
			t.Fatal(err)
		}
		calls++
		serializedTargets += bytes.Count(payload, []byte(`"tenant":`))
		state, err = sourcedriver.ApplyTargetSetPage(state, page)
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls != 79 || serializedTargets != len(targets) {
		t.Fatalf("target declaration calls=%d serialized=%d, want 79/%d", calls, serializedTargets, len(targets))
	}

	_, oneRef := protocolTargetSetForTest(
		t, 3, 9, []sourcedriver.TargetDeclaration{{Tenant: "tenant-00000", Generation: 1}},
	)
	one, err := Encode(SnapshotRequest{Protocol: Version, TargetSet: oneRef, Revision: "head", Limit: 128})
	if err != nil {
		t.Fatal(err)
	}
	maximum, err := Encode(SnapshotRequest{Protocol: Version, TargetSet: protocolRef, Revision: "head", Limit: 128})
	if err != nil {
		t.Fatal(err)
	}
	delta := len(maximum) - len(one)
	if delta < 0 {
		delta = -delta
	}
	if len(maximum) > 1024 || delta > 8 || bytes.Contains(maximum, []byte("tenant-")) ||
		bytes.Contains(maximum, []byte(`"targets"`)) {
		t.Fatalf("source request leaked target cardinality: one=%d maximum=%d payload=%s", len(one), len(maximum), maximum)
	}
}

func protocolTargetSetPageForTest(page sourcedriver.TargetSetPage) TargetSetPage {
	targets := make([]TargetDeclaration, len(page.Targets))
	for index, target := range page.Targets {
		targets[index] = TargetDeclaration{Tenant: string(target.Tenant), Generation: uint64(target.Generation)}
	}
	_, ref := protocolTargetSetForTestNoFail(page.Ref)
	return TargetSetPage{
		Ref: ref, Sequence: page.Sequence, Targets: targets, Complete: page.Complete,
		PreviousDigest: stringHex(page.PreviousDigest[:]), Digest: stringHex(page.Digest[:]),
		PageDigest: stringHex(page.PageDigest[:]),
	}
}

func protocolTargetSetForTestNoFail(ref sourcedriver.TargetSetRef) (sourcedriver.TargetSetRef, TargetSetRef) {
	return ref, TargetSetRef{
		ID: stringHex(ref.ID[:]), AuthorityGeneration: uint64(ref.AuthorityGeneration),
		TargetEpoch: ref.TargetEpoch, DeclarationDigest: stringHex(ref.DeclarationDigest[:]),
		TargetCount: ref.TargetCount, TargetsDigest: stringHex(ref.TargetsDigest[:]),
	}
}

func protocolTargetSetForTest(
	t *testing.T,
	authorityGeneration causal.Generation,
	targetEpoch uint64,
	targets []sourcedriver.TargetDeclaration,
) (sourcedriver.TargetSetRef, TargetSetRef) {
	t.Helper()
	declaration := sha256.Sum256([]byte("declaration"))
	ref, err := sourcedriver.NewTargetSetRef(
		"source-authority", authorityGeneration, targetEpoch, declaration, targets,
	)
	if err != nil {
		t.Fatal(err)
	}
	return protocolTargetSetForTestNoFail(ref)
}

func TestGeneratorOutputIsCurrent(t *testing.T) {
	// The repository gate runs `go run ./sourcedriverproto/gen -check`; this
	// assertion keeps the generated identity visibly distinct from other wires.
	if Version != 1 || !strings.HasPrefix(SchemaFingerprint, "fusekit.sourcedriver.") || Build != SchemaFingerprint {
		t.Fatalf("generated identity = v%d %q %q", Version, SchemaFingerprint, Build)
	}
}

func stringHex(value []byte) string {
	const digits = "0123456789abcdef"
	encoded := make([]byte, len(value)*2)
	for index, item := range value {
		encoded[index*2] = digits[item>>4]
		encoded[index*2+1] = digits[item&15]
	}
	return string(encoded)
}
