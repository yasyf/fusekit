package holder

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/deploy"
	"howett.net/plist"
)

const testTeamID = "ABCDE12345"

func testProcessRequirement(suffix string) daemonkit.Requirement {
	return daemonkit.Requirement{TeamID: testTeamID, SigningIdentifier: "com.example." + suffix}
}

func testRuntimeRequirement() daemonkit.Requirement {
	return daemonkit.Requirement{
		TeamID: testTeamID, SigningIdentifier: "com.example.product.runtime",
		RequiredAppGroup: "ABCDE12345.example",
		RequiredEntitlements: map[string]daemonkit.EntitlementRequirement{
			"com.example.filesystem-runtime": {Match: daemonkit.EntitlementBoolean, Boolean: true},
		},
	}
}

func testBrokerRequirement() daemonkit.Requirement {
	return daemonkit.Requirement{
		TeamID: testTeamID, SigningIdentifier: "com.example.product.broker",
		RequiredAppGroup: "ABCDE12345.example",
		RequiredEntitlements: map[string]daemonkit.EntitlementRequirement{
			"com.example.file-provider-broker": {Match: daemonkit.EntitlementBoolean, Boolean: true},
		},
	}
}

func testExtensionRequirement() daemonkit.Requirement {
	return daemonkit.Requirement{
		TeamID: testTeamID, SigningIdentifier: "com.example.product.fileprovider",
		RequiredAppGroup: "ABCDE12345.example",
		RequiredEntitlements: map[string]daemonkit.EntitlementRequirement{
			"com.example.file-provider-extension": {Match: daemonkit.EntitlementBoolean, Boolean: true},
		},
	}
}

func testControllerRequirement() daemonkit.Requirement {
	return daemonkit.Requirement{
		TeamID: testTeamID, SigningIdentifier: "com.example.product.control",
		RequiredEntitlements: map[string]daemonkit.EntitlementRequirement{
			"com.example.filesystem-control": {Match: daemonkit.EntitlementBoolean, Boolean: true},
		},
	}
}

func testNativePeers() runtimePeers {
	return runtimePeers{
		runtime: testRuntimeRequirement(), native: true, controller: testControllerRequirement(),
	}
}

func testFileProviderPeers() runtimePeers {
	return runtimePeers{
		runtime: testRuntimeRequirement(), controller: testControllerRequirement(),
		fileProvider: &fileProviderPeers{
			broker: testBrokerRequirement(), extension: testExtensionRequirement(),
		},
	}
}

func TestFuseKitTrustBusinessFollowsEnabledPresentations(t *testing.T) {
	both := testFileProviderPeers()
	both.native = true
	tests := []struct {
		name  string
		peers runtimePeers
		want  daemonkit.Requirements
	}{
		{"native only", testNativePeers(), daemonkit.Requirements{testRuntimeRequirement()}},
		{
			"file provider only", testFileProviderPeers(),
			daemonkit.Requirements{testExtensionRequirement(), testBrokerRequirement()},
		},
		{
			"native and file provider", both,
			daemonkit.Requirements{
				testRuntimeRequirement(), testExtensionRequirement(), testBrokerRequirement(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trust, err := fuseKitTrust(tt.peers)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(trust.Business, tt.want) {
				t.Fatalf("business = %+v, want %+v", trust.Business, tt.want)
			}
			if trust.Control == nil || !reflect.DeepEqual(*trust.Control, testControllerRequirement()) {
				t.Fatalf("control = %+v, want %+v", trust.Control, testControllerRequirement())
			}
			if !reflect.DeepEqual(trust.Serving, daemonkit.ServingSigned(testRuntimeRequirement())) {
				t.Fatalf("serving = %+v, want the signed runtime requirement", trust.Serving)
			}
			shared := daemonkit.Daemon{Label: "com.example.product.fusekit", Trust: trust}
			if err := shared.ValidateForServe(); err != nil {
				t.Fatalf("validate collapsed trust for serve: %v", err)
			}
			if err := shared.ValidateForClient(); err != nil {
				t.Fatalf("validate collapsed trust for a Go client: %v", err)
			}
		})
	}
}

func TestFuseKitTrustRefusesIncompletePeers(t *testing.T) {
	missingExtension := testFileProviderPeers()
	missingExtension.fileProvider.extension = daemonkit.Requirement{}
	missingBroker := testFileProviderPeers()
	missingBroker.fileProvider.broker = daemonkit.Requirement{}
	missingController := testNativePeers()
	missingController.controller = daemonkit.Requirement{}
	missingRuntime := testNativePeers()
	missingRuntime.runtime = daemonkit.Requirement{}
	noPresentation := testNativePeers()
	noPresentation.native = false
	tests := []struct {
		name  string
		peers runtimePeers
	}{
		{"missing extension", missingExtension},
		{"missing broker", missingBroker},
		{"missing controller", missingController},
		{"missing runtime", missingRuntime},
		{"no presentation", noPresentation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := fuseKitTrust(tt.peers); err == nil {
				t.Fatal("incomplete peer set was accepted")
			}
		})
	}
}

func TestBusinessAndControlRequirementsAreSatisfiedByPeerEntitlements(t *testing.T) {
	peers := testFileProviderPeers()
	peers.native = true
	trust, err := fuseKitTrust(peers)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		requirement daemonkit.Requirement
		fixture     string
	}{
		{"business runtime", trust.Business[0], "runtime.plist"},
		{"business file provider extension", trust.Business[1], "file-provider-extension.plist"},
		{"business broker", trust.Business[2], "broker.plist"},
		{"control", *trust.Control, "controller.plist"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entitlements := readEntitlementFixture(t, tt.fixture)
			if err := satisfiesEntitlements(tt.requirement, entitlements); err != nil {
				t.Fatalf("%s entitlements do not satisfy the stated requirement: %v", tt.fixture, err)
			}
			for _, foreign := range []string{"runtime.plist", "broker.plist", "controller.plist"} {
				if foreign == tt.fixture {
					continue
				}
				if err := satisfiesEntitlements(tt.requirement, readEntitlementFixture(t, foreign)); err == nil {
					t.Fatalf("%s entitlements satisfied the %s requirement", foreign, tt.name)
				}
			}
		})
	}
}

func TestPeerEntitlementFixturesArePinnedComplete(t *testing.T) {
	tests := []struct {
		fixture string
		want    string
	}{
		{"runtime.plist", "abdfc138af52e6af2bea8f4756d5f8e76329d7213ba24f99ea21575c4af182aa"},
		{"broker.plist", "494af4db62cb96307a96ce44d3dbdadf75718e192826610c48ae4df9392ab036"},
		{"file-provider-extension.plist", "abf5b526d065ca853699913e2c0e637921a123c18bb65680e0ceee6ce3a7a724"},
		{"controller.plist", "2ac9585c236b99bb9575ff93e1028313a734db9a531b9293559201c5dc5b55d2"},
	}
	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			digest, err := deploy.DigestEntitlements(readEntitlementFixtureBytes(t, tt.fixture))
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(digest[:]); got != tt.want {
				t.Fatalf("complete entitlement digest = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCollapsedTrustDigestsArePinned(t *testing.T) {
	peers := testFileProviderPeers()
	peers.native = true
	trust, err := fuseKitTrust(peers)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		got  daemonkit.PolicyDigest
		want daemonkit.PolicyDigest
	}{
		{
			"business runtime", trust.Business[0].Digest(),
			"8d0c9049a9c6a7f55f88fdb40ac0e8d3e6d10a408eb344216be5a48addfd8c82",
		},
		{
			"business file provider extension", trust.Business[1].Digest(),
			"3225db386592a32a0979fcf04fa9d95d8c6024e6ff4e4647af338fb7f125a964",
		},
		{
			"business broker", trust.Business[2].Digest(),
			"7c329237728b4a30cb8b03e3b59f2db14da828bb5b8fd3d17523fdf681d97e5a",
		},
		{
			"business set", trust.Business.Digest(),
			"0ad920fa82a53c4c122ab198e2ff7148eff24245f26de6091689ff0be8d1c937",
		},
		{
			"control", trust.Control.Digest(),
			"422e6c84c3ae15ccf0e04cc938f7765314e890ae3e1264fdbca528ee151455bf",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("policy digest = %q, want %q", tt.got, tt.want)
			}
		})
	}
	if trust.Business.Digest() == trust.Business[0].Digest() {
		t.Fatal("the business set shares a digest with one of its members")
	}
}

func readEntitlementFixtureBytes(t *testing.T, name string) []byte {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join("testdata", "entitlements", name))
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func readEntitlementFixture(t *testing.T, name string) map[string]any {
	t.Helper()
	var entitlements map[string]any
	if _, err := plist.Unmarshal(readEntitlementFixtureBytes(t, name), &entitlements); err != nil {
		t.Fatal(err)
	}
	return entitlements
}

func satisfiesEntitlements(requirement daemonkit.Requirement, entitlements map[string]any) error {
	predicates := map[string]daemonkit.EntitlementRequirement{}
	for key, predicate := range requirement.RequiredEntitlements {
		predicates[key] = predicate
	}
	if requirement.RequiredAppGroup != "" {
		predicates["com.apple.security.application-groups"] = daemonkit.EntitlementRequirement{
			Match: daemonkit.EntitlementStringArrayContains, String: requirement.RequiredAppGroup,
		}
	}
	for key, predicate := range predicates {
		value, present := entitlements[key]
		if !present {
			return fmt.Errorf("entitlement %s is absent", key)
		}
		if !matchesEntitlement(value, predicate) {
			return fmt.Errorf("entitlement %s = %v does not satisfy %+v", key, value, predicate)
		}
	}
	return nil
}

func matchesEntitlement(value any, predicate daemonkit.EntitlementRequirement) bool {
	switch predicate.Match {
	case daemonkit.EntitlementBoolean:
		boolean, ok := value.(bool)
		return ok && boolean == predicate.Boolean
	case daemonkit.EntitlementString:
		text, ok := value.(string)
		return ok && text == predicate.String
	case daemonkit.EntitlementStringArrayContains:
		elements, ok := value.([]any)
		if !ok {
			return false
		}
		for _, element := range elements {
			if text, ok := element.(string); ok && text == predicate.String {
				return true
			}
		}
		return false
	default:
		return false
	}
}
