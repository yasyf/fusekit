package mountproto

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestEncodeDecodeExactV1(t *testing.T) {
	request := RegisterTenantRequest{
		Protocol: Version,
		Definition: TenantDefinition{
			PresentationRoot: "/Volumes/FuseKit/acct-18",
			BackingRoot:      "/Users/test/.cc-pool/accounts/acct-18",
			ContentSourceID:  "acct-18-source",
			AccessMode:       AccessModeReadWrite,
			CasePolicy:       CasePolicySensitive,
			Presentations:    []Presentation{PresentationMount, PresentationFileProvider},
			Generation:       7,
		},
	}
	raw, err := Encode(request)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var decoded RegisterTenantRequest
	if err := Decode(raw, &decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	reencoded, err := Encode(decoded)
	if err != nil {
		t.Fatalf("re-Encode: %v", err)
	}
	if !bytes.Equal(raw, reencoded) {
		t.Fatalf("canonical bytes changed:\n%s\n%s", raw, reencoded)
	}
}

func TestDecodeRejectsNonSchemaInputs(t *testing.T) {
	valid := `{"protocol":1,"definition":{"presentation_root":"/Volumes/FuseKit/acct-18","backing_root":"/Users/test/.cc-pool/accounts/acct-18","content_source_id":"source","access_mode":"read_write","case_policy":"sensitive","presentations":["mount"],"generation":1}}`
	tests := map[string]string{
		"unknown owner":           strings.Replace(valid, `"generation":1`, `"owner_id":"spoofed","generation":1`, 1),
		"duplicate generation":    strings.Replace(valid, `"generation":1`, `"generation":1,"generation":2`, 1),
		"old protocol":            strings.Replace(valid, `"protocol":1`, `"protocol":0`, 1),
		"trailing value":          valid + `{}`,
		"unordered presentations": strings.Replace(valid, `["mount"]`, `["file_provider","mount"]`, 1),
		"unclean root":            strings.Replace(valid, `/Volumes/FuseKit/acct-18`, `/Volumes/FuseKit/../acct-18`, 1),
		"group container":         strings.Replace(valid, `/Users/test/.cc-pool/accounts/acct-18`, `/Users/test/Library/Group Containers/group.example`, 1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			var request RegisterTenantRequest
			if err := Decode([]byte(raw), &request); err == nil {
				t.Fatal("Decode succeeded")
			}
		})
	}
}

func TestDecodeReportsExactProtocolAndForbiddenPath(t *testing.T) {
	var request StateRequest
	err := Decode([]byte(`{"protocol":2,"generation":1}`), &request)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("Decode protocol error = %v", err)
	}
	err = Decode([]byte(`{"protocol":1,"definition":{"presentation_root":"/tmp/root","backing_root":"/Users/test/Library/Group Containers/group.example","content_source_id":"source","access_mode":"read_only","case_policy":"insensitive","presentations":["file_provider"],"generation":1}}`), &RegisterTenantRequest{})
	if !errors.Is(err, ErrForbiddenPath) {
		t.Fatalf("Decode path error = %v", err)
	}
}

func TestGeneratedMessagesAreCurrent(t *testing.T) {
	if SchemaFingerprint == "" || !strings.HasPrefix(SchemaFingerprint, "fusekit.mount.") {
		t.Fatalf("SchemaFingerprint = %q", SchemaFingerprint)
	}
}
