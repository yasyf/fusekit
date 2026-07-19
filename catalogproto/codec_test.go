package catalogproto

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

const (
	mutationOne MutationID = "10000000000000000000000000000001"
	changeOne   ChangeID   = "20000000000000000000000000000001"
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
		Protocol: Version, OperationID: mutationOne, Generation: 3, ExpectedRevision: 1,
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
		Protocol: Version, OperationID: mutationOne, Generation: 3, ExpectedRevision: 1,
		Kind: MutationKindRevise, ObjectID: &objectTwo, ParentID: &objectOne,
		Name: &name, Mode: &mode,
	}
	if err := Validate(metadataOnly); err != nil {
		t.Fatalf("Validate(metadata-only revise): %v", err)
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
		Protocol: Version, OperationID: mutationOne, Generation: 3, ExpectedRevision: 1,
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

func TestDomainRegistrationRequiresExactRootIdentity(t *testing.T) {
	t.Parallel()
	registration := DomainRegistration{
		DomainID: domainOne, OwnerID: "owner-1", TenantID: "tenant-1", Generation: 1,
		RootID: objectOne, AccountInstanceID: "account-1", DisplayName: "Account 1",
	}
	if err := Validate(registration); err != nil {
		t.Fatalf("Validate(registration): %v", err)
	}
	registration.RootID = ""
	if err := Validate(registration); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(missing root) = %v, want ErrInvalidMessage", err)
	}
}

func TestReplaceMutationCarriesOptionalFinalStateAndContent(t *testing.T) {
	t.Parallel()
	name := "settings.json"
	mode := uint32(0o600)
	revision := uint64(3)
	request := MutationRequest{
		Protocol: Version, OperationID: mutationOne, Generation: 3, ExpectedRevision: 2,
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

func TestNotificationRequiresCanonicalCausalTuple(t *testing.T) {
	t.Parallel()
	workingSet := SignalTarget{Kind: SignalTargetKindWorkingSet}
	container := SignalTarget{Kind: SignalTargetKindContainer, ParentID: &objectTwo}
	notification := ConvergenceNotification{
		Protocol: Version, TenantID: "acct-18", DomainID: domainOne, Generation: 4, Revision: 9, CatalogRevision: 8,
		SourceAuthority: "source-main", SourceRevision: 4,
		ChangeID: changeOne, OperationID: mutationOne, Cause: ConvergenceCauseExternalUnattributed,
		AffectedKeys: []string{"settings.json"}, Targets: []SignalTarget{container, workingSet},
	}
	if err := Validate(notification); err != nil {
		t.Fatalf("Validate(notification): %v", err)
	}
	notification.AffectedKeys = []string{"z", "a"}
	if err := Validate(notification); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(unsorted keys) error = %v, want ErrInvalidMessage", err)
	}
}

func TestSourceReconcileRequiresAuthorityFencedCanonicalShape(t *testing.T) {
	t.Parallel()
	request := SourceReconcileRequest{
		Protocol: Version, Mode: SourceModeSnapshot, SourceAuthority: "source-main", SourceRevision: 4,
		ChangeID: changeOne, OperationID: mutationOne, Cause: ConvergenceCauseDaemonWrite,
		AffectedKeys: []string{"settings.json"}, TenantCount: 1,
	}
	if err := Validate(request); err != nil {
		t.Fatalf("Validate(snapshot): %v", err)
	}
	request.AffectedKeys = []string{"z", "a"}
	if err := Validate(request); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(unsorted source keys) = %v, want ErrInvalidMessage", err)
	}
	request.AffectedKeys = []string{"settings.json"}
	request.Mode = SourceModeDelta
	if err := Validate(request); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(unfenced delta) = %v, want ErrInvalidMessage", err)
	}
	file := SourceObjectRecord{
		SourceKey: "settings", Name: "settings.json", Kind: ObjectKindFile, Mode: 0o600,
		ContentRevision: 4, Hash: strings.Repeat("a", sha256.Size*2), MountVisible: true,
	}
	if err := Validate(file); err != nil {
		t.Fatalf("Validate(source file): %v", err)
	}
	response := SourceReconcileResponse{
		Protocol: Version, Code: ErrorCodeOk, SourceAuthority: "source-main", SourceRevision: 4,
		ChangeID: changeOne, OperationID: mutationOne,
		Commits: []SourceCommit{{TenantID: "acct-18", CatalogRevision: 7}},
	}
	if err := Validate(response); err != nil {
		t.Fatalf("Validate(source response): %v", err)
	}
	tenant := SourceTenantRecord{TenantID: "acct-18", Generation: 4}
	if err := Validate(tenant); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(source tenant without root key) = %v, want ErrInvalidMessage", err)
	}
	tenant.RootKey = "root:acct-18"
	if err := Validate(tenant); err != nil {
		t.Fatalf("Validate(source tenant): %v", err)
	}
}

func TestEncodeIsCanonical(t *testing.T) {
	t.Parallel()
	payload, err := Encode(HeadResponse{Protocol: Version, Code: ErrorCodeOk, Revision: 7})
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	want := `{"code":"ok","message":"","protocol":4,"revision":7}`
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
			Protocol: Version, OperationID: mutationOne, Generation: 4, ExpectedRevision: 1,
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
				ChangeID: changeOne, OperationID: mutationOne, Cause: ConvergenceCauseExternalUnattributed,
				AffectedKeys: []string{"settings.json"}, Targets: []SignalTarget{container, {Kind: SignalTargetKindWorkingSet}},
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
