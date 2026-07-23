package mountproto

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestEncodeDecodeExactV1(t *testing.T) {
	request := ProvisionTenantRequest{
		Protocol: Version,
		Definition: TenantDefinition{
			PresentationRoot:        "/Volumes/FuseKit/acct-18",
			BackingRoot:             "/Users/test/.cc-pool/accounts/acct-18",
			ContentSourceID:         "acct-18-source",
			AccessMode:              AccessModeReadWrite,
			CasePolicy:              CasePolicySensitive,
			Presentations:           []Presentation{PresentationMount, PresentationFileProvider},
			FileProviderAccountID:   "acct-18-instance",
			FileProviderDisplayName: "Account 18",
			Generation:              7,
		},
	}
	raw, err := Encode(request)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var decoded ProvisionTenantRequest
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

func TestRemovalResponseRequiresExactFileProviderAbsenceProof(t *testing.T) {
	base := RemoveTenantResponse{
		Protocol: Version, Code: ErrorCodeOk, TenantID: "acct-18", Generation: 7,
	}
	if _, err := Encode(base); err == nil {
		t.Fatal("successful removal encoded without File Provider absence proof")
	}
	base.FileProviderAbsent = true
	if _, err := Encode(base); err != nil {
		t.Fatalf("exact removal proof rejected: %v", err)
	}
	base.Code = ErrorCodeUnavailable
	base.Message = "not settled"
	base.TenantID = ""
	base.Generation = 0
	if _, err := Encode(base); err == nil {
		t.Fatal("failed removal encoded with File Provider absence proof")
	}
}

func TestDecodeRejectsNonSchemaInputs(t *testing.T) {
	valid := `{"protocol":1,"definition":{"presentation_root":"/Volumes/FuseKit/acct-18","backing_root":"/Users/test/.cc-pool/accounts/acct-18","content_source_id":"source","access_mode":"read_write","case_policy":"sensitive","presentations":["mount"],"file_provider_account_id":"","file_provider_display_name":"","generation":1}}`
	tests := map[string]string{
		"unknown owner":           strings.Replace(valid, `"generation":1`, `"owner_id":"spoofed","generation":1`, 1),
		"duplicate generation":    strings.Replace(valid, `"generation":1`, `"generation":1,"generation":2`, 1),
		"wrong protocol":          strings.Replace(valid, `"protocol":1`, `"protocol":2`, 1),
		"trailing value":          valid + `{}`,
		"unordered presentations": strings.Replace(valid, `["mount"]`, `["file_provider","mount"]`, 1),
		"unclean root":            strings.Replace(valid, `/Volumes/FuseKit/acct-18`, `/Volumes/FuseKit/../acct-18`, 1),
		"group container":         strings.Replace(valid, `/Users/test/.cc-pool/accounts/acct-18`, `/Users/test/Library/Group Containers/group.example`, 1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			var request ProvisionTenantRequest
			if err := Decode([]byte(raw), &request); err == nil {
				t.Fatal("Decode succeeded")
			}
		})
	}
}

func TestDecodeReportsExactProtocolAndForbiddenPath(t *testing.T) {
	var request StateRequest
	err := Decode([]byte(`{"protocol":2}`), &request)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("Decode protocol error = %v", err)
	}
	if err := Decode([]byte(`{"protocol":1,"generation":1}`), &request); err == nil {
		t.Fatal("generation-bearing StateRequest decoded")
	}
	err = Decode([]byte(`{"protocol":1,"definition":{"presentation_root":"/tmp/root","backing_root":"/Users/test/Library/Group Containers/group.example","content_source_id":"source","access_mode":"read_only","case_policy":"insensitive","presentations":["file_provider"],"file_provider_account_id":"instance","file_provider_display_name":"Account","generation":1}}`), &ProvisionTenantRequest{})
	if !errors.Is(err, ErrForbiddenPath) {
		t.Fatalf("Decode path error = %v", err)
	}
}

func TestNativeHandleMessagesEnforceExactIdentityAndBoundedIO(t *testing.T) {
	object := NativeObject{
		ID: "01000000000000000000000000000000", ParentID: "02000000000000000000000000000000",
		Name: "settings.json", Kind: ObjectKindFile, Mode: 0o600, Size: 4,
		Hash: strings.Repeat("0", 64), Revision: 7, MetadataRevision: 7, ContentRevision: 3,
	}
	response := NativeSnapshotOpenResponse{
		Protocol: Version, Code: ErrorCodeOk, Handle: "snapshot-handle", Object: &object,
	}
	raw, err := Encode(response)
	if err != nil {
		t.Fatalf("Encode(snapshot open): %v", err)
	}
	var decoded NativeSnapshotOpenResponse
	if err := Decode(raw, &decoded); err != nil {
		t.Fatalf("Decode(snapshot open): %v", err)
	}
	if decoded.Object == nil || *decoded.Object != object {
		t.Fatalf("decoded snapshot object = %+v", decoded.Object)
	}
	if _, err := Encode(NativeSnapshotReadRequest{
		Protocol: Version, Handle: response.Handle, Length: maxNativeChunk + 1,
	}); err == nil {
		t.Fatal("oversized native read encoded")
	}
	if _, err := Encode(NativeWriteWriteRequest{
		Protocol: Version, Handle: "write-handle", Data: make([]byte, maxNativeChunk+1),
	}); err == nil {
		t.Fatal("oversized native write encoded")
	}
	if _, err := Encode(NativeWriteCommitRequest{
		Protocol: Version, Handle: "write-handle",
	}); err != nil {
		t.Fatalf("native write commit request: %v", err)
	}
	commit := NativeWriteCommitResponse{
		Protocol: Version, Code: ErrorCodeOk, Handle: "write-handle",
		MutationID: MutationID(strings.Repeat("0", 64)), Object: &object,
	}
	if _, err := Encode(commit); err != nil {
		t.Fatalf("native write commit response with a derived mutation id: %v", err)
	}
	commit.MutationID = MutationID(strings.Repeat("0", 32))
	if _, err := Encode(commit); err == nil {
		t.Fatal("native write commit response with a legacy-size mutation id encoded")
	}
	failed := response
	failed.Code = ErrorCodeUnavailable
	failed.Message = "worker retired"
	if _, err := Encode(failed); err == nil {
		t.Fatal("failed snapshot response encoded with a live handle")
	}
}

func TestNativeRoutePagesAreStrictlyBoundedAndCursorFenced(t *testing.T) {
	routes := make([]MountRoute, MaxNativeRoutePageSize)
	for index := range routes {
		routes[index] = MountRoute{
			Name:       fmt.Sprintf("acct-%03d", index),
			TenantID:   TenantID(fmt.Sprintf("tenant-%03d", index)),
			Generation: 1,
		}
	}
	response := NativeRoutePageResponse{
		Protocol: Version, Code: ErrorCodeOk, Snapshot: 7, Routes: routes,
		Next: routes[len(routes)-1].Name,
	}
	raw, err := Encode(response)
	if err != nil {
		t.Fatalf("Encode(max route page): %v", err)
	}
	if len(raw) > maxNativeRoutePageBytes {
		t.Fatalf("encoded route page = %d bytes, budget %d", len(raw), maxNativeRoutePageBytes)
	}
	overflow := response
	overflow.Routes = append(append([]MountRoute(nil), routes...), MountRoute{
		Name: "acct-overflow", TenantID: "tenant-overflow", Generation: 1,
	})
	overflow.Next = overflow.Routes[len(overflow.Routes)-1].Name
	if _, err := Encode(overflow); err == nil {
		t.Fatal("oversized route page encoded")
	}
	unordered := response
	unordered.Routes = append([]MountRoute(nil), routes...)
	unordered.Routes[0], unordered.Routes[1] = unordered.Routes[1], unordered.Routes[0]
	if _, err := Encode(unordered); err == nil {
		t.Fatal("unordered route page encoded")
	}
	if _, err := Encode(NativeRoutePageRequest{
		Protocol: Version, After: "acct-001", Limit: 1,
	}); err == nil {
		t.Fatal("initial route page encoded with a cursor")
	}
}

func TestNativeReadyAndRuntimeHealthRequireExactThroughProof(t *testing.T) {
	source, err := NativeMountSource("/Volumes/FuseKit")
	if err != nil {
		t.Fatalf("native mount source: %v", err)
	}
	proof := NativeMountProof{
		PresentationRoot: "/Volumes/FuseKit",
		Filesystem:       NativeMountFilesystem,
		Source:           source,
		RootReadEpoch:    7,
	}
	identity := NativeMountIdentity{
		PresentationRoot: proof.PresentationRoot,
		Filesystem:       proof.Filesystem,
		Source:           proof.Source,
	}
	if err := Validate(identity); err != nil {
		t.Fatalf("exact native mount identity: %v", err)
	}
	probeToken := strings.Repeat("a", 64)
	if _, err := Encode(NativeMountedRequest{Protocol: Version, Mount: identity, ProbeToken: probeToken}); err != nil {
		t.Fatalf("exact native mounted identity: %v", err)
	}
	if _, err := Encode(NativeMountedResponse{Protocol: Version, Code: ErrorCodeOk, ProbeToken: probeToken}); err != nil {
		t.Fatalf("exact native mounted response: %v", err)
	}
	if _, err := Encode(NativeReadyRequest{Protocol: Version, Mount: proof}); err != nil {
		t.Fatalf("exact native ready proof: %v", err)
	}
	for name, mutate := range map[string]func(*NativeMountProof){
		"root":       func(value *NativeMountProof) { value.PresentationRoot = "relative" },
		"filesystem": func(value *NativeMountProof) { value.Filesystem = "fusefs" },
		"source":     func(value *NativeMountProof) { value.Source = "legacy" },
		"epoch":      func(value *NativeMountProof) { value.RootReadEpoch = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := proof
			mutate(&invalid)
			if _, err := Encode(NativeReadyRequest{Protocol: Version, Mount: invalid}); err == nil {
				t.Fatal("inexact native ready proof encoded")
			}
		})
	}
	for name, mutate := range map[string]func(*NativeMountedRequest){
		"protocol":   func(value *NativeMountedRequest) { value.Protocol++ },
		"root":       func(value *NativeMountedRequest) { value.Mount.PresentationRoot = "relative" },
		"filesystem": func(value *NativeMountedRequest) { value.Mount.Filesystem = "fusefs" },
		"source":     func(value *NativeMountedRequest) { value.Mount.Source = "legacy" },
		"token":      func(value *NativeMountedRequest) { value.ProbeToken = "short" },
	} {
		t.Run("mounted "+name, func(t *testing.T) {
			invalid := NativeMountedRequest{Protocol: Version, Mount: identity, ProbeToken: probeToken}
			mutate(&invalid)
			if _, err := Encode(invalid); err == nil {
				t.Fatal("inexact native mounted identity encoded")
			}
		})
	}
	if _, err := Encode(NativeMountedResponse{Protocol: Version, Code: ErrorCodeUnavailable, Message: "failed", ProbeToken: probeToken}); err == nil {
		t.Fatal("failed native mounted response encoded with probe token")
	}
	validHealth := func() RuntimeHealthResponse {
		return RuntimeHealthResponse{
			Protocol: Version, Code: ErrorCodeOk,
			RuntimeBuild: "product-1.8.0", RuntimeProtocol: RuntimeProtocolVersion, RuntimePID: 4242,
			ProcessGeneration: "process-7", ActivationGeneration: "activation-7", State: RuntimeStateHealthy,
			Ready:          true,
			ReadinessPhase: ReadinessPhaseReady, ReadinessStep: ReadinessStepPublished,
			NativePhase: NativePhaseLive, NativeMount: &proof, BrokerPhase: BrokerPhaseDisabled,
		}
	}
	health := validHealth()
	if _, err := Encode(health); err != nil {
		t.Fatalf("exact runtime health: %v", err)
	}
	health.NativeMount = nil
	if _, err := Encode(health); err == nil {
		t.Fatal("live runtime health encoded without native mount proof")
	}
	health = validHealth()
	health.State = RuntimeStateDegraded
	health.ReadinessPhase = ReadinessPhaseStarting
	health.ReadinessStep = ReadinessStepNative
	health.NativePhase = NativePhaseStarting
	health.Ready = false
	if _, err := Encode(health); err == nil {
		t.Fatal("non-live runtime health encoded with stale native mount proof")
	}
	health = validHealth()
	health.Code = ErrorCodeUnavailable
	health.Message = "not ready"
	health.RuntimeBuild = ""
	health.RuntimeProtocol = 0
	health.RuntimePID = 0
	health.ProcessGeneration = ""
	health.ActivationGeneration = ""
	health.State = ""
	health.Draining = false
	health.Busy = false
	health.Ready = false
	health.ReadinessPhase = ""
	health.ReadinessStep = ""
	health.NativePhase = ""
	health.NativeMount = nil
	health.BrokerPhase = ""
	if _, err := Encode(health); err != nil {
		t.Fatalf("unavailable runtime health: %v", err)
	}
	health.ActivationGeneration = "stale"
	if _, err := Encode(health); err == nil {
		t.Fatal("failed runtime health encoded with stale health state")
	}

	for name, mutate := range map[string]func(*RuntimeHealthResponse){
		"build":              func(value *RuntimeHealthResponse) { value.RuntimeBuild = "" },
		"runtime protocol":   func(value *RuntimeHealthResponse) { value.RuntimeProtocol++ },
		"runtime pid":        func(value *RuntimeHealthResponse) { value.RuntimePID = 0 },
		"process generation": func(value *RuntimeHealthResponse) { value.ProcessGeneration = "" },
		"activation generation": func(value *RuntimeHealthResponse) {
			value.ActivationGeneration = ""
		},
		"runtime state":  func(value *RuntimeHealthResponse) { value.State = "unknown" },
		"ready":          func(value *RuntimeHealthResponse) { value.Ready = false },
		"failed state":   func(value *RuntimeHealthResponse) { value.State = RuntimeStateFailed },
		"phase":          func(value *RuntimeHealthResponse) { value.ReadinessPhase = ReadinessPhaseStarting },
		"step":           func(value *RuntimeHealthResponse) { value.ReadinessStep = ReadinessStepBroker },
		"native":         func(value *RuntimeHealthResponse) { value.NativePhase = NativePhaseStarting },
		"broker":         func(value *RuntimeHealthResponse) { value.BrokerPhase = BrokerPhaseStarting },
		"unknown broker": func(value *RuntimeHealthResponse) { value.BrokerPhase = "unknown" },
	} {
		t.Run("ready "+name, func(t *testing.T) {
			invalid := validHealth()
			invalid.BrokerPhase = BrokerPhaseLive
			mutate(&invalid)
			if _, err := Encode(invalid); err == nil {
				t.Fatal("inexact ready runtime health encoded")
			}
		})
	}
	starting := validHealth()
	starting.State = RuntimeStateDegraded
	starting.Busy = true
	starting.ReadinessPhase = ReadinessPhaseStarting
	starting.ReadinessStep = ReadinessStepBroker
	starting.BrokerPhase = BrokerPhaseStarting
	starting.Ready = false
	if _, err := Encode(starting); err != nil {
		t.Fatalf("exact starting runtime health: %v", err)
	}
	for name, mutate := range map[string]func(*RuntimeHealthResponse){
		"degraded": func(value *RuntimeHealthResponse) { value.State = RuntimeStateDegraded },
		"busy":     func(value *RuntimeHealthResponse) { value.Busy = true },
	} {
		t.Run("published "+name, func(t *testing.T) {
			orthogonal := validHealth()
			mutate(&orthogonal)
			if _, err := Encode(orthogonal); err != nil {
				t.Fatalf("orthogonal runtime lifecycle state: %v", err)
			}
		})
	}
	draining := validHealth()
	draining.State = RuntimeStateDraining
	draining.Draining = true
	draining.Ready = false
	draining.ReadinessPhase = ReadinessPhaseDraining
	if _, err := Encode(draining); err != nil {
		t.Fatalf("exact draining runtime health: %v", err)
	}
	for name, mutate := range map[string]func(*RuntimeHealthResponse){
		"state": func(value *RuntimeHealthResponse) { value.State = RuntimeStateHealthy },
		"flag":  func(value *RuntimeHealthResponse) { value.Draining = false },
		"phase": func(value *RuntimeHealthResponse) { value.ReadinessPhase = ReadinessPhaseReady },
		"step":  func(value *RuntimeHealthResponse) { value.ReadinessStep = ReadinessStepReceipts },
	} {
		t.Run("draining "+name, func(t *testing.T) {
			invalid := draining
			mutate(&invalid)
			if _, err := Encode(invalid); err == nil {
				t.Fatal("inexact draining runtime health encoded")
			}
		})
	}
	failed := validHealth()
	failed.State = RuntimeStateFailed
	failed.Ready = false
	failed.ReadinessPhase = ReadinessPhaseFailed
	if _, err := Encode(failed); err != nil {
		t.Fatalf("published runtime failure: %v", err)
	}
}

func TestRuntimeHealthOperationUsesSuiteQualifiedNamespace(t *testing.T) {
	if OperationRuntimeHealth != "fusekit.runtime.health" {
		t.Fatalf("RuntimeHealth operation = %q", OperationRuntimeHealth)
	}
	if RuntimeHealthMaxResponseBytes != 16<<10 {
		t.Fatalf("RuntimeHealth response bound = %d", RuntimeHealthMaxResponseBytes)
	}
}

func TestResponseMessageHasExactByteBound(t *testing.T) {
	boundary := RuntimeHealthResponse{
		Protocol: Version, Code: ErrorCodeUnavailable, Message: strings.Repeat("x", 4096),
	}
	if _, err := Encode(boundary); err != nil {
		t.Fatalf("4096-byte response message: %v", err)
	}
	boundary.Message += "x"
	if _, err := Encode(boundary); err == nil {
		t.Fatal("4097-byte response message encoded")
	}
}

func TestNativeMountSourceFollowsExactPresentationRootLeaf(t *testing.T) {
	tests := map[string]string{
		"/Users/yasyf/.cc-pool/accounts": "fuse-t:/accounts",
		"/private/tmp/mount":             "fuse-t:/mount",
		"/Volumes/other":                 "fuse-t:/other",
	}
	for root, want := range tests {
		got, err := NativeMountSource(root)
		if err != nil || got != want {
			t.Fatalf("NativeMountSource(%q) = %q, %v; want %q", root, got, err, want)
		}
	}
	for _, root := range []string{"", "relative", "/", "/tmp/accounts/../mount", "/tmp/accounts\x00"} {
		if _, err := NativeMountSource(root); err == nil {
			t.Fatalf("NativeMountSource(%q) succeeded", root)
		}
	}
}

func TestGeneratedMessagesAreCurrent(t *testing.T) {
	if SchemaFingerprint == "" || !strings.HasPrefix(SchemaFingerprint, "fusekit.mount.") {
		t.Fatalf("SchemaFingerprint = %q", SchemaFingerprint)
	}
}
