package catalogproto

import (
	"encoding/json"
	"errors"
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
		Protocol: Version, TenantID: "acct-18", DomainID: domainOne, Generation: 4, Revision: 9, CatalogRevision: 8, SourceRevision: 4,
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
				Protocol: Version, TenantID: "acct-18", DomainID: domainOne, Generation: 4, Revision: 9, CatalogRevision: 8, SourceRevision: 4,
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
