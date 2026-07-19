package catalogproto

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
)

const (
	requestOne   MutationRequestID = "10000000000000000000000000000001"
	mutationOne  MutationID        = "0000000000000002100000000000000000000000000000000000000000000001"
	operationOne OperationID       = "30000000000000000000000000000001"
	changeOne    ChangeID          = "20000000000000000000000000000001"
)

var (
	objectOne ObjectID = "00000000000000000000000000000001"
	objectTwo ObjectID = "00000000000000000000000000000002"
	domainOne          = mustTestDomainID("owner-1", "account-1")
)

func mustTestDomainID(owner OwnerID, account AccountInstanceID) DomainID {
	domain, err := DeriveDomainID(owner, account)
	if err != nil {
		panic(err)
	}
	return domain
}

func TestDecodeRejectsUnknownDuplicateAndOldProtocol(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		payload string
		target  error
	}{
		{name: "unknown", payload: `{"protocol":1,"unknown":true}`, target: ErrInvalidMessage},
		{name: "duplicate", payload: `{"protocol":1,"protocol":1}`, target: ErrInvalidMessage},
		{name: "old protocol", payload: `{"protocol":0}`, target: ErrProtocol},
		{name: "trailing", payload: `{"protocol":1}{"protocol":1}`, target: ErrInvalidMessage},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var request HeadRequest
			err := Decode([]byte(test.payload), &request)
			if !errors.Is(err, test.target) {
				t.Fatalf("Decode() error = %v, want %v", err, test.target)
			}
		})
	}
}

func TestDecodeRejectsGroupContainerPathAnywhere(t *testing.T) {
	t.Parallel()
	payload := `{"protocol":1,"object_id":"00000000000000000000000000000001","parent_id":"00000000000000000000000000000002","name":"/Users/a/Library/Group Containers/group.example/private"}`
	var request LookupNameRequest
	err := Decode([]byte(payload), &request)
	if !errors.Is(err, ErrForbiddenPath) {
		t.Fatalf("Decode() error = %v, want ErrForbiddenPath", err)
	}
}

func TestMutationContentIntentIsUnambiguous(t *testing.T) {
	t.Parallel()
	mode := uint32(0o644)
	name := "empty"
	revision := uint64(2)
	kind := ObjectKindFile
	valid := MutationRequest{
		Protocol: Version, RequestID: requestOne, Generation: 3, ExpectedRevision: 1,
		Kind: MutationKindCreate, ObjectKind: &kind, HasContent: true,
		ParentID: &objectOne, Name: &name, Mode: &mode, ContentRevision: &revision,
	}
	if err := Validate(valid); err != nil {
		t.Fatalf("Validate(valid empty file intent): %v", err)
	}
	invalid := valid
	invalid.HasContent = false
	if err := Validate(invalid); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(ambiguous create) error = %v, want ErrInvalidMessage", err)
	}
	metadataOnly := MutationRequest{
		Protocol: Version, RequestID: requestOne, Generation: 3, ExpectedRevision: 1,
		Kind: MutationKindRevise, ObjectID: &objectTwo, ParentID: &objectOne,
		Name: &name, Mode: &mode,
	}
	if err := Validate(metadataOnly); err != nil {
		t.Fatalf("Validate(metadata-only revise): %v", err)
	}
}

func TestMutationRequestCommitAndCausalIdentitiesAreDistinct(t *testing.T) {
	t.Parallel()
	requestID := requestOne
	mutationID := mutationOne
	response := MutationResponse{
		Protocol: Version, Code: ErrorCodeOk, RequestID: &requestID, MutationID: &mutationID,
		Revision: 2, PrimaryID: &objectOne,
	}
	if err := Validate(response); err != nil {
		t.Fatalf("Validate(distinct mutation identities): %v", err)
	}
	wrongRevision := MutationID(
		"0000000000000003100000000000000000000000000000000000000000000001",
	)
	response.MutationID = &wrongRevision
	if err := Validate(response); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(wrong mutation target revision) = %v, want ErrInvalidMessage", err)
	}
	shortMutation := MutationID(requestOne)
	response.MutationID = &shortMutation
	if err := Validate(response); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(short committed mutation id) = %v, want ErrInvalidMessage", err)
	}
	longRequest := MutationRequestID(mutationOne)
	response.MutationID = &mutationID
	response.RequestID = &longRequest
	if err := Validate(response); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(long request id) = %v, want ErrInvalidMessage", err)
	}
	payload := fmt.Sprintf(
		`{"protocol":%d,"operation_id":%q,"generation":3,"expected_revision":1,"kind":"delete","has_content":false,"object_id":%q}`,
		Version,
		requestOne,
		objectOne,
	)
	var request MutationRequest
	if err := Decode([]byte(payload), &request); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Decode(old mutation request identity) = %v, want ErrInvalidMessage", err)
	}
}

func TestNameHasExactPortableUTF8Bound(t *testing.T) {
	t.Parallel()
	if err := validateName(strings.Repeat("a", int(MaxNameBytes))); err != nil {
		t.Fatalf("validateName(exact max): %v", err)
	}
	for name, value := range map[string]string{
		"over max":     strings.Repeat("a", int(MaxNameBytes)+1),
		"invalid utf8": string([]byte{0xff}),
		"control":      "bad\u0001name",
		"slash":        "bad/name",
		"NUL":          "bad\x00name",
		"dot":          ".",
		"dot dot":      "..",
	} {
		if err := validateName(value); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("validateName(%s) = %v, want ErrInvalidMessage", name, err)
		}
	}
}

func TestSymlinkProtocolCarriesInlineTargetWithoutBody(t *testing.T) {
	t.Parallel()
	target := "../settings.json"
	digest := sha256.Sum256([]byte(target))
	hash := fmt.Sprintf("%x", digest[:])
	revision := uint64(2)
	mode := uint32(0o777)
	name := "settings"
	kind := ObjectKindSymlink
	request := MutationRequest{
		Protocol: Version, RequestID: requestOne, Generation: 3, ExpectedRevision: 1,
		Kind: MutationKindCreate, ObjectKind: &kind, ParentID: &objectOne, Name: &name, Mode: &mode,
		ContentRevision: &revision, LinkTarget: &target,
	}
	if err := Validate(request); err != nil {
		t.Fatalf("Validate(symlink create): %v", err)
	}
	object := CatalogObject{
		ID: objectTwo, ParentID: objectOne, Revision: 2, MetadataRevision: 2, ContentRevision: 2,
		Name: name, Kind: ObjectKindSymlink, Mode: mode, Size: uint64(len(target)), Hash: hash, LinkTarget: target,
	}
	if err := Validate(object); err != nil {
		t.Fatalf("Validate(symlink object): %v", err)
	}
	request.HasContent = true
	if err := Validate(request); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(symlink body) = %v, want ErrInvalidMessage", err)
	}
	object.LinkTarget = "bad\x00target"
	if err := Validate(object); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(malformed target) = %v, want ErrInvalidMessage", err)
	}
}

func TestCatalogObjectSizeFitsSignedPresentationRange(t *testing.T) {
	t.Parallel()
	object := CatalogObject{
		ID: objectTwo, ParentID: objectOne, Revision: 2, MetadataRevision: 2, ContentRevision: 2,
		Name: "large", Kind: ObjectKindFile, Mode: 0o600, Size: uint64(math.MaxInt64) + 1,
		Hash: strings.Repeat("0", sha256.Size*2),
	}
	if err := Validate(object); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(oversized object) = %v, want ErrInvalidMessage", err)
	}
}

func TestChangesResponseRejectsRowsOutsideFloorAndHead(t *testing.T) {
	t.Parallel()
	object := CatalogObject{
		ID: objectTwo, ParentID: objectOne, Revision: 2, MetadataRevision: 2,
		Name: "directory", Kind: ObjectKindDirectory,
	}
	response := ChangesSinceResponse{
		Protocol: Version, Code: ErrorCodeOk, Floor: 2, Head: 3,
		Next: ChangeCursor{Revision: 3, Sequence: ChangeCursorCompleteSequence}, Complete: true,
		Changes: []Change{{Revision: 2, Sequence: 1, Kind: ChangeKindUpsert, Object: object}},
	}
	if err := Validate(response); err != nil {
		t.Fatalf("Validate(in-range changes): %v", err)
	}
	for name, revision := range map[string]uint64{"below_floor": 1, "above_head": 4} {
		t.Run(name, func(t *testing.T) {
			forged := response
			forged.Changes = append([]Change(nil), response.Changes...)
			forged.Changes[0].Revision = revision
			forged.Changes[0].Object.Revision = revision
			forged.Changes[0].Object.MetadataRevision = revision
			if err := Validate(forged); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("Validate(out-of-range changes) = %v, want ErrInvalidMessage", err)
			}
		})
	}
}

func TestDomainRegistrationRequiresExactRootIdentity(t *testing.T) {
	t.Parallel()
	registration := DomainRegistration{
		DomainID: domainOne, OwnerID: "owner-1", TenantID: "tenant-1", Generation: 1,
		RootID: objectOne, AccessMode: TenantAccessModeReadWrite,
		AccountInstanceID: "account-1", DisplayName: "Account 1",
	}
	if err := Validate(registration); err != nil {
		t.Fatalf("Validate(registration): %v", err)
	}
	registration.RootID = ""
	if err := Validate(registration); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(missing root) = %v, want ErrInvalidMessage", err)
	}
	registration.RootID = objectOne
	registration.AccessMode = ""
	if err := Validate(registration); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(missing access mode) = %v, want ErrInvalidMessage", err)
	}
}

func TestBrokerPayloadsHaveExactStructuralBounds(t *testing.T) {
	t.Parallel()
	registration := DomainRegistration{
		DomainID: domainOne, OwnerID: "owner-1", TenantID: "tenant-1", Generation: 1,
		RootID: objectOne, AccessMode: TenantAccessModeReadWrite,
		AccountInstanceID: "account-1", DisplayName: strings.Repeat("d", int(MaxDisplayNameBytes)),
	}
	if err := Validate(registration); err != nil {
		t.Fatalf("Validate(exact display name): %v", err)
	}
	registration.DisplayName += "x"
	if err := Validate(registration); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(overlong display name) = %v, want ErrInvalidMessage", err)
	}

	context := BrokerForwardContext{DomainID: domainOne, TenantID: "tenant-1", Generation: 1}
	forward := BrokerForwardRequest{
		Protocol: Version, Context: context, Operation: OperationCatalogHead,
		Payload: bytes.Repeat([]byte{'x'}, int(MaxBrokerForwardPayloadBytes)),
	}
	encoded, err := Encode(forward)
	if err != nil {
		t.Fatalf("Encode(exact forward payload): %v", err)
	}
	if len(encoded) >= 2<<20 {
		t.Fatalf("encoded forward payload = %d bytes, want below 2 MiB", len(encoded))
	}
	forward.Payload = append(forward.Payload, 'x')
	if err := Validate(forward); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(overlong forward payload) = %v, want ErrInvalidMessage", err)
	}

	message := BrokerOpenResponse{
		Protocol: Version, Code: ErrorCodeUnavailable,
		Message: strings.Repeat("e", int(MaxErrorMessageBytes)),
	}
	if err := Validate(message); err != nil {
		t.Fatalf("Validate(exact error message): %v", err)
	}
	message.Message += "x"
	if err := Validate(message); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(overlong error message) = %v, want ErrInvalidMessage", err)
	}
}

func TestBrokerDomainPagesAreBoundedSortedAndBelowFrameLimit(t *testing.T) {
	t.Parallel()
	domains := make([]RegisteredDomain, 0, MaxBrokerDomainPageSize+1)
	for index := 0; index < int(MaxBrokerDomainPageSize)+1; index++ {
		account := AccountInstanceID(fmt.Sprintf("account-%03d", index))
		domain := mustTestDomainID("owner-1", account)
		prefix := "/Users/test/Library/CloudStorage/"
		publicPath := prefix + strings.Repeat("p", int(MaxPublicPathBytes)-len(prefix))
		domains = append(domains, RegisteredDomain{
			DomainID: domain, OwnerID: "owner-1", TenantID: TenantID(fmt.Sprintf("tenant-%03d", index)),
			Generation: 1, RootID: objectOne, AccessMode: TenantAccessModeReadWrite,
			AccountInstanceID: account, DisplayName: "Account", PublicPath: publicPath,
		})
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i].DomainID < domains[j].DomainID })
	overlongPath := domains[0]
	overlongPath.PublicPath += "x"
	if err := Validate(overlongPath); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(overlong public path) = %v, want ErrInvalidMessage", err)
	}
	exact := append([]RegisteredDomain(nil), domains[:MaxBrokerDomainPageSize]...)
	next := exact[len(exact)-1].DomainID
	result := BrokerResult{
		Protocol: Version, Code: ErrorCodeOk, CommandID: 1, Kind: BrokerCommandKindListDomains,
		Domains: &exact, NextAfterDomainID: &next,
	}
	encoded, err := Encode(result)
	if err != nil {
		t.Fatalf("Encode(exact broker domain page): %v", err)
	}
	if len(encoded) >= 2<<20 {
		t.Fatalf("encoded broker domain page = %d bytes, want below 2 MiB", len(encoded))
	}
	over := domains
	result.Domains = &over
	result.NextAfterDomainID = nil
	if err := Validate(result); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(overlong broker domain page) = %v, want ErrInvalidMessage", err)
	}
	unsorted := append([]RegisteredDomain(nil), exact...)
	unsorted[0], unsorted[1] = unsorted[1], unsorted[0]
	result.Domains = &unsorted
	if err := Validate(result); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(unsorted broker domain page) = %v, want ErrInvalidMessage", err)
	}
}

func TestReplaceMutationCarriesOptionalFinalStateAndContent(t *testing.T) {
	t.Parallel()
	name := "settings.json"
	mode := uint32(0o600)
	revision := uint64(3)
	request := MutationRequest{
		Protocol: Version, RequestID: requestOne, Generation: 3, ExpectedRevision: 2,
		Kind:     MutationKindReplace,
		ObjectID: &objectOne, TargetID: &objectTwo, ParentID: &objectTwo,
		Name: &name, Mode: &mode,
		HasContent: true, ContentRevision: &revision,
	}
	if err := Validate(request); err != nil {
		t.Fatalf("Validate(streamed replace): %v", err)
	}
	request.HasContent = false
	if err := Validate(request); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(ambiguous replace) error = %v, want ErrInvalidMessage", err)
	}
}

func TestDesiredSourceFleetPublicationIsExactBoundedAndOrdered(t *testing.T) {
	t.Parallel()
	declaration := SourceAuthorityDeclaration{
		Authority: "authority-a", DriverID: "driver.v1",
		DriverConfig:      bytes.Repeat([]byte{0xa5}, int(MaxSourceDriverConfigBytes)),
		DeclarationDigest: strings.Repeat("a", 64),
	}
	request := PublishDesiredSourceFleetRequest{
		Protocol: Version, Owner: "owner", ExpectedGeneration: 0, Generation: 1,
		Declarations: []SourceAuthorityDeclaration{declaration},
	}
	if err := Validate(request); err != nil {
		t.Fatalf("Validate(exact publication): %v", err)
	}
	over := request
	over.Declarations = append([]SourceAuthorityDeclaration(nil), request.Declarations...)
	over.Declarations[0].DriverConfig = append(over.Declarations[0].DriverConfig, 0)
	if err := Validate(over); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(overlong driver config) = %v, want ErrInvalidMessage", err)
	}
	unsorted := request
	unsorted.Declarations = []SourceAuthorityDeclaration{
		{Authority: "authority-b", DriverID: "driver.v1", DeclarationDigest: strings.Repeat("b", 64)},
		{Authority: "authority-a", DriverID: "driver.v1", DeclarationDigest: strings.Repeat("a", 64)},
	}
	if err := Validate(unsorted); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(unsorted declarations) = %v, want ErrInvalidMessage", err)
	}
	replayedGeneration := request
	replayedGeneration.ExpectedGeneration = 1
	replayedGeneration.Generation = 1
	if err := Validate(replayedGeneration); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(non-advancing generation) = %v, want ErrInvalidMessage", err)
	}
}

func TestDesiredSourceFleetReadPinsSnapshotAndCursor(t *testing.T) {
	t.Parallel()
	head := ReadDesiredSourceFleetRequest{Protocol: Version, Owner: "owner", Limit: 16}
	if err := Validate(head); err != nil {
		t.Fatalf("Validate(head read): %v", err)
	}
	digest := strings.Repeat("d", 64)
	after := SourceAuthorityID("authority-a")
	pinned := ReadDesiredSourceFleetRequest{
		Protocol: Version, Owner: "owner", Generation: 3,
		SnapshotDigest: &digest, After: &after, Limit: 16,
	}
	if err := Validate(pinned); err != nil {
		t.Fatalf("Validate(pinned read): %v", err)
	}
	drift := pinned
	drift.SnapshotDigest = nil
	if err := Validate(drift); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(unpinned continuation) = %v, want ErrInvalidMessage", err)
	}
	declaration := SourceAuthorityDeclaration{
		Authority: after, DriverID: "driver.v1", DeclarationDigest: strings.Repeat("a", 64),
	}
	state := DesiredSourceFleetState{
		Owner: "owner", Generation: 3, AuthorityCount: 1,
		AuthoritiesDigest: strings.Repeat("b", 64), DeclarationsDigest: digest,
	}
	response := ReadDesiredSourceFleetResponse{
		Protocol: Version, Code: ErrorCodeOk, State: &state,
		Declarations: []SourceAuthorityDeclaration{declaration}, Next: &after,
	}
	if err := Validate(response); err != nil {
		t.Fatalf("Validate(pinned page): %v", err)
	}
	wrongNext := SourceAuthorityID("authority-z")
	response.Next = &wrongNext
	if err := Validate(response); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(wrong cursor) = %v, want ErrInvalidMessage", err)
	}
}

func TestNotificationRequiresCanonicalCausalTuple(t *testing.T) {
	t.Parallel()
	workingSet := SignalTarget{Kind: SignalTargetKindWorkingSet}
	container := SignalTarget{Kind: SignalTargetKindContainer, ParentID: &objectTwo}
	notification := ConvergenceNotification{
		Protocol: Version, TenantID: "acct-18", DomainID: domainOne, Generation: 4, Revision: 9, CatalogRevision: 8,
		SourceAuthority: "source-main", SourceRevision: 4,
		ChangeID: changeOne, OperationID: operationOne, Cause: ConvergenceCauseExternalUnattributed,
		Fingerprint:   strings.Repeat("c", 64),
		AffectedCount: 1, AffectedDigest: strings.Repeat("a", 64),
		TargetCount: 2, TargetDigest: strings.Repeat("b", 64),
		Targets: []SignalTarget{container, workingSet},
	}
	if err := Validate(notification); err != nil {
		t.Fatalf("Validate(notification): %v", err)
	}
	notification.Cause = ConvergenceCauseProviderMutation
	if err := Validate(notification); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(provider notification without origin) error = %v, want ErrInvalidMessage", err)
	}
	origin := domainOne
	notification.OriginDomain = &origin
	notification.OriginGeneration = 4
	if err := Validate(notification); err != nil {
		t.Fatalf("Validate(provider notification with origin): %v", err)
	}
	notification.Cause = ConvergenceCauseExternalUnattributed
	notification.OriginDomain = nil
	notification.OriginGeneration = 0
	notification.TargetCount = 3
	if err := Validate(notification); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(mismatched target count) error = %v, want ErrInvalidMessage", err)
	}
	notification.TargetCount = uint64(MaxSignalTargets) + 1
	notification.TargetsCoalesced = true
	notification.Targets = []SignalTarget{workingSet}
	if err := Validate(notification); err != nil {
		t.Fatalf("Validate(coalesced notification): %v", err)
	}
	notification.Targets = []SignalTarget{container}
	if err := Validate(notification); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(non-coarse coalesced notification) error = %v, want ErrInvalidMessage", err)
	}
}

func TestLargeConvergenceSummaryHasBoundedBrokerCommand(t *testing.T) {
	t.Parallel()
	notification := ConvergenceNotification{
		Protocol: Version, TenantID: "acct-18", DomainID: domainOne, Generation: 4,
		Revision: 9, CatalogRevision: 8, SourceAuthority: "source-main", SourceRevision: 4,
		ChangeID: changeOne, OperationID: operationOne, Cause: ConvergenceCauseExternalUnattributed,
		Fingerprint:   strings.Repeat("c", 64),
		AffectedCount: 10_000, AffectedDigest: strings.Repeat("a", 64),
		TargetCount: 10_000, TargetDigest: strings.Repeat("b", 64), TargetsCoalesced: true,
		Targets: []SignalTarget{{Kind: SignalTargetKindWorkingSet}},
	}
	encoded, err := Encode(BrokerCommand{
		Protocol: Version, CommandID: 1, Kind: BrokerCommandKindSignalDomain,
		Notification: &notification,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > 1_024 {
		t.Fatalf("summarized notification encoded size = %d, want <= 1024", len(encoded))
	}
	if bytes.Contains(encoded, []byte("affected_keys")) {
		t.Fatal("summarized notification embeds affected key bodies")
	}
}

func TestPrepareTenantCarriesOnlyGeneration(t *testing.T) {
	t.Parallel()
	request := PrepareTenantRequest{Protocol: Version, Generation: 4}
	if err := Validate(request); err != nil {
		t.Fatalf("Validate(prepare): %v", err)
	}
	request.Generation = 0
	if err := Validate(request); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(zero generation) = %v, want ErrInvalidMessage", err)
	}
	payload := fmt.Sprintf(`{"protocol":%d,"domain_id":%q,"generation":4,"catalog_revision":9}`, Version, domainOne)
	if err := Decode([]byte(payload), &request); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Decode(old prepare shape) = %v, want ErrInvalidMessage", err)
	}
}

func TestPrepareDomainCarriesExactTenantProofIdentity(t *testing.T) {
	t.Parallel()
	request := PrepareDomainRequest{
		Protocol: Version, DomainID: domainOne, Generation: 4,
		SourceAuthority: "source-main", SourceRevision: 8, CatalogRevision: 12,
		ChangeID: changeOne, OperationID: operationOne,
	}
	if err := Validate(request); err != nil {
		t.Fatalf("Validate(domain prepare): %v", err)
	}
	request.SourceRevision = 0
	if err := Validate(request); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(zero source revision) = %v, want ErrInvalidMessage", err)
	}
}

func TestEncodeIsCanonical(t *testing.T) {
	t.Parallel()
	payload, err := Encode(HeadResponse{Protocol: Version, Code: ErrorCodeOk, Revision: 7})
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	want := `{"code":"ok","message":"","protocol":1,"revision":7}`
	if string(payload) != want {
		t.Fatalf("Encode() = %s, want %s", payload, want)
	}
	if strings.Contains(string(payload), " ") {
		t.Fatalf("Encode() is not compact: %s", payload)
	}
}

func TestCrossLanguageGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("testdata/golden.json")
	if err != nil {
		t.Fatalf("ReadFile(golden): %v", err)
	}
	var golden map[string]json.RawMessage
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("Unmarshal(golden): %v", err)
	}
	mode := uint32(0o755)
	name := "folder"
	directory := ObjectKindDirectory
	container := SignalTarget{Kind: SignalTargetKindContainer, ParentID: &objectTwo}
	values := map[string]any{
		"head_response": HeadResponse{Protocol: Version, Code: ErrorCodeOk, Revision: 7},
		"mutation_request": MutationRequest{
			Protocol: Version, RequestID: requestOne, Generation: 4, ExpectedRevision: 1,
			Kind:       MutationKindCreate,
			ObjectKind: &directory, ParentID: &objectOne, Name: &name, Mode: &mode,
		},
		"broker_bind_request": BrokerBindDomainRequest{
			Protocol: Version, DomainID: domainOne, TenantID: "acct-18", Generation: 4,
		},
		"broker_command": BrokerCommand{
			Protocol: Version, CommandID: 7, Kind: BrokerCommandKindSignalDomain,
			Notification: &ConvergenceNotification{
				Protocol: Version, TenantID: "acct-18", DomainID: domainOne, Generation: 4, Revision: 9, CatalogRevision: 8,
				SourceAuthority: "source-main", SourceRevision: 4,
				ChangeID: changeOne, OperationID: operationOne, Cause: ConvergenceCauseExternalUnattributed,
				Fingerprint:   strings.Repeat("c", 64),
				AffectedCount: 1, AffectedDigest: strings.Repeat("a", 64),
				TargetCount: 2, TargetDigest: strings.Repeat("b", 64),
				Targets: []SignalTarget{container, {Kind: SignalTargetKindWorkingSet}},
			},
		},
	}
	for name, value := range values {
		t.Run(name, func(t *testing.T) {
			encoded, err := Encode(value)
			if err != nil {
				t.Fatalf("Encode(): %v", err)
			}
			canonical, err := canonicalJSON(golden[name])
			if err != nil {
				t.Fatalf("canonicalJSON(golden): %v", err)
			}
			if string(encoded) != string(canonical) {
				t.Fatalf("Encode() = %s\ngolden = %s", encoded, canonical)
			}
		})
	}
}
