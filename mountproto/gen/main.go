package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/yasyf/daemonkit/wire"
)

type enum struct {
	Name    string
	Values  []string
	GoNames map[string]string `json:"-"`
}

type field struct {
	JSON     string
	Go       string
	Type     string
	Optional bool
	Array    bool
}

type message struct {
	Name   string
	Fields []field
}

var enums = []enum{
	{Name: "Operation", GoNames: map[string]string{"fusekit.runtime.health": "RuntimeHealth"}, Values: []string{
		"fusekit.runtime.health",
		"tenant.provision", "tenant.replace", "tenant.remove", "tenant.state",
		"native.bind", "native.mounted", "native.ready", "native.unbind", "native.route.page", "native.pin", "native.release",
		"native.snapshot.open", "native.snapshot.read", "native.snapshot.close",
		"native.write.open", "native.write.read", "native.write.write",
		"native.write.truncate", "native.write.sync", "native.write.commit", "native.write.abort",
	}},
	{Name: "ErrorCode", Values: []string{"ok", "invalid_request", "unauthorized", "not_found", "conflict", "quarantined", "canceled", "unavailable"}},
	{Name: "AccessMode", Values: []string{"read_only", "read_write"}},
	{Name: "CasePolicy", Values: []string{"sensitive", "insensitive"}},
	{Name: "Presentation", Values: []string{"mount", "file_provider"}},
	{Name: "QuarantineLane", Values: []string{"catalog_mutation", "materialization", "enumeration", "mount_lifecycle"}},
	{Name: "QuarantineCause", Values: []string{"conflict", "integrity", "unsettled", "unavailable"}},
	{Name: "ObjectKind", Values: []string{"directory", "file", "symlink"}},
	{Name: "RuntimeState", Values: []string{"healthy", "degraded", "draining", "failed"}},
	{Name: "ReadinessPhase", Values: []string{"starting", "ready", "draining", "failed"}},
	{Name: "ReadinessStep", Values: []string{"listener", "native", "broker", "receipts", "published"}},
	{Name: "NativePhase", Values: []string{"disabled", "idle", "starting", "live", "failed", "closing", "closed"}},
	{Name: "BrokerPhase", Values: []string{"disabled", "starting", "live", "failed"}},
}

var protocol = field{JSON: "protocol", Go: "Protocol", Type: "uint16"}
var code = field{JSON: "code", Go: "Code", Type: "ErrorCode"}
var responseMessage = field{JSON: "message", Go: "Message", Type: "string"}

func request(name string, fields ...field) message {
	return message{Name: name, Fields: append([]field{protocol}, fields...)}
}

func response(name string, fields ...field) message {
	return message{Name: name, Fields: append([]field{protocol, code, responseMessage}, fields...)}
}

var messages = []message{
	{Name: "MountSpec", Fields: []field{
		{JSON: "presentation_root", Go: "PresentationRoot", Type: "string"},
	}},
	{Name: "TenantDefinition", Fields: []field{
		{JSON: "mount", Go: "Mount", Type: "MountSpec", Optional: true},
		{JSON: "backing_root", Go: "BackingRoot", Type: "string"},
		{JSON: "content_source_id", Go: "ContentSourceID", Type: "string"},
		{JSON: "access_mode", Go: "AccessMode", Type: "AccessMode"},
		{JSON: "case_policy", Go: "CasePolicy", Type: "CasePolicy"},
		{JSON: "presentations", Go: "Presentations", Type: "Presentation", Array: true},
		{JSON: "file_provider_presentation_instance_id", Go: "FileProviderPresentationInstanceID", Type: "string"},
		{JSON: "file_provider_display_name", Go: "FileProviderDisplayName", Type: "string"},
		{JSON: "generation", Go: "Generation", Type: "uint64"},
	}},
	{Name: "MountRoute", Fields: []field{
		{JSON: "name", Go: "Name", Type: "string"},
		{JSON: "tenant_id", Go: "TenantID", Type: "TenantID"},
		{JSON: "generation", Go: "Generation", Type: "uint64"},
	}},
	{Name: "Quarantine", Fields: []field{
		{JSON: "lane", Go: "Lane", Type: "QuarantineLane"},
		{JSON: "revision", Go: "Revision", Type: "uint64"},
		{JSON: "cause", Go: "Cause", Type: "QuarantineCause"},
		{JSON: "detail", Go: "Detail", Type: "string"},
		{JSON: "since_unix_nano", Go: "SinceUnixNano", Type: "int64"},
	}},
	{Name: "TenantState", Fields: []field{
		{JSON: "owner_id", Go: "OwnerID", Type: "OwnerID"},
		{JSON: "tenant_id", Go: "TenantID", Type: "TenantID"},
		{JSON: "generation", Go: "Generation", Type: "uint64"},
		{JSON: "requested", Go: "Requested", Type: "uint64"},
		{JSON: "desired", Go: "Desired", Type: "uint64"},
		{JSON: "observed", Go: "Observed", Type: "uint64"},
		{JSON: "verified", Go: "Verified", Type: "uint64"},
		{JSON: "applied", Go: "Applied", Type: "uint64"},
		{JSON: "activated_generation", Go: "ActivatedGeneration", Type: "uint64"},
		{JSON: "state_version", Go: "StateVersion", Type: "uint64"},
		{JSON: "replacement_eligible", Go: "ReplacementEligible", Type: "bool"},
		{JSON: "quarantine", Go: "Quarantine", Type: "Quarantine", Optional: true},
	}},
	{Name: "NativeObject", Fields: []field{
		{JSON: "id", Go: "ID", Type: "string"},
		{JSON: "parent_id", Go: "ParentID", Type: "string"},
		{JSON: "name", Go: "Name", Type: "string"},
		{JSON: "kind", Go: "Kind", Type: "ObjectKind"},
		{JSON: "mode", Go: "Mode", Type: "uint32"},
		{JSON: "size", Go: "Size", Type: "int64"},
		{JSON: "hash", Go: "Hash", Type: "string"},
		{JSON: "link_target", Go: "LinkTarget", Type: "string"},
		{JSON: "revision", Go: "Revision", Type: "uint64"},
		{JSON: "metadata_revision", Go: "MetadataRevision", Type: "uint64"},
		{JSON: "content_revision", Go: "ContentRevision", Type: "uint64"},
		{JSON: "desired", Go: "Desired", Type: "uint64"},
		{JSON: "observed", Go: "Observed", Type: "uint64"},
		{JSON: "verified", Go: "Verified", Type: "uint64"},
		{JSON: "applied", Go: "Applied", Type: "uint64"},
	}},
	{Name: "NativeMountProof", Fields: []field{
		{JSON: "presentation_root", Go: "PresentationRoot", Type: "string"},
		{JSON: "filesystem", Go: "Filesystem", Type: "string"},
		{JSON: "source", Go: "Source", Type: "string"},
		{JSON: "root_read_epoch", Go: "RootReadEpoch", Type: "uint64"},
	}},
	{Name: "NativeMountIdentity", Fields: []field{
		{JSON: "presentation_root", Go: "PresentationRoot", Type: "string"},
		{JSON: "filesystem", Go: "Filesystem", Type: "string"},
		{JSON: "source", Go: "Source", Type: "string"},
	}},
	request("RuntimeHealthRequest"),
	response("RuntimeHealthResponse",
		field{JSON: "runtime_build", Go: "RuntimeBuild", Type: "string"},
		field{JSON: "runtime_protocol", Go: "RuntimeProtocol", Type: "uint16"},
		field{JSON: "runtime_pid", Go: "RuntimePID", Type: "int64"},
		field{JSON: "process_generation", Go: "ProcessGeneration", Type: "string"},
		field{JSON: "activation_generation", Go: "ActivationGeneration", Type: "string"},
		field{JSON: "state", Go: "State", Type: "RuntimeState"},
		field{JSON: "draining", Go: "Draining", Type: "bool"},
		field{JSON: "busy", Go: "Busy", Type: "bool"},
		field{JSON: "ready", Go: "Ready", Type: "bool"},
		field{JSON: "readiness_phase", Go: "ReadinessPhase", Type: "ReadinessPhase"},
		field{JSON: "readiness_step", Go: "ReadinessStep", Type: "ReadinessStep"},
		field{JSON: "native_phase", Go: "NativePhase", Type: "NativePhase"},
		field{JSON: "native_mount", Go: "NativeMount", Type: "NativeMountProof", Optional: true},
		field{JSON: "broker_phase", Go: "BrokerPhase", Type: "BrokerPhase"},
	),
	request("ProvisionTenantRequest", field{JSON: "definition", Go: "Definition", Type: "TenantDefinition"}),
	response("ProvisionTenantResponse", field{JSON: "tenant_id", Go: "TenantID", Type: "TenantID"}, field{JSON: "generation", Go: "Generation", Type: "uint64"}),
	request("ReplaceTenantRequest", field{JSON: "expected_generation", Go: "ExpectedGeneration", Type: "uint64"}, field{JSON: "definition", Go: "Definition", Type: "TenantDefinition"}),
	response("ReplaceTenantResponse", field{JSON: "tenant_id", Go: "TenantID", Type: "TenantID"}, field{JSON: "generation", Go: "Generation", Type: "uint64"}),
	request("RemoveTenantRequest", field{JSON: "generation", Go: "Generation", Type: "uint64"}),
	response("RemoveTenantResponse",
		field{JSON: "tenant_id", Go: "TenantID", Type: "TenantID"},
		field{JSON: "generation", Go: "Generation", Type: "uint64"},
		field{JSON: "file_provider_absent", Go: "FileProviderAbsent", Type: "bool"},
	),
	request("StateRequest"),
	response("StateResponse", field{JSON: "state", Go: "State", Type: "TenantState", Optional: true}),
	request("NativeBindRequest"),
	response("NativeBindResponse"),
	request("NativeMountedRequest",
		field{JSON: "mount", Go: "Mount", Type: "NativeMountIdentity"},
		field{JSON: "probe_token", Go: "ProbeToken", Type: "string"},
	),
	response("NativeMountedResponse", field{JSON: "probe_token", Go: "ProbeToken", Type: "string"}),
	request("NativeReadyRequest", field{JSON: "mount", Go: "Mount", Type: "NativeMountProof"}),
	response("NativeReadyResponse"),
	request("NativeUnbindRequest"),
	response("NativeUnbindResponse"),
	request("NativeRoutePageRequest",
		field{JSON: "snapshot", Go: "Snapshot", Type: "uint64"},
		field{JSON: "after", Go: "After", Type: "string"},
		field{JSON: "limit", Go: "Limit", Type: "uint16"},
	),
	response("NativeRoutePageResponse",
		field{JSON: "snapshot", Go: "Snapshot", Type: "uint64"},
		field{JSON: "routes", Go: "Routes", Type: "MountRoute", Array: true},
		field{JSON: "next", Go: "Next", Type: "string"},
	),
	request("NativePinRequest", field{JSON: "name", Go: "Name", Type: "string"}),
	response("NativePinResponse",
		field{JSON: "token", Go: "Token", Type: "string"},
		field{JSON: "owner_id", Go: "OwnerID", Type: "OwnerID"},
		field{JSON: "route", Go: "Route", Type: "MountRoute", Optional: true},
		field{JSON: "definition", Go: "Definition", Type: "TenantDefinition", Optional: true},
	),
	request("NativeReleaseRequest", field{JSON: "token", Go: "Token", Type: "string"}),
	response("NativeReleaseResponse", field{JSON: "token", Go: "Token", Type: "string"}),
	request("NativeSnapshotOpenRequest",
		field{JSON: "tenant_id", Go: "TenantID", Type: "TenantID"},
		field{JSON: "generation", Go: "Generation", Type: "uint64"},
		field{JSON: "object_id", Go: "ObjectID", Type: "string"},
		field{JSON: "revision", Go: "Revision", Type: "uint64"},
	),
	response("NativeSnapshotOpenResponse",
		field{JSON: "handle", Go: "Handle", Type: "string"},
		field{JSON: "object", Go: "Object", Type: "NativeObject", Optional: true},
	),
	request("NativeSnapshotReadRequest",
		field{JSON: "handle", Go: "Handle", Type: "string"},
		field{JSON: "offset", Go: "Offset", Type: "int64"},
		field{JSON: "length", Go: "Length", Type: "uint32"},
	),
	response("NativeSnapshotReadResponse",
		field{JSON: "data", Go: "Data", Type: "byte", Array: true},
		field{JSON: "eof", Go: "EOF", Type: "bool"},
	),
	request("NativeSnapshotCloseRequest", field{JSON: "handle", Go: "Handle", Type: "string"}),
	response("NativeSnapshotCloseResponse", field{JSON: "handle", Go: "Handle", Type: "string"}),
	request("NativeWriteOpenRequest",
		field{JSON: "tenant_id", Go: "TenantID", Type: "TenantID"},
		field{JSON: "generation", Go: "Generation", Type: "uint64"},
		field{JSON: "object_id", Go: "ObjectID", Type: "string"},
		field{JSON: "revision", Go: "Revision", Type: "uint64"},
	),
	response("NativeWriteOpenResponse",
		field{JSON: "handle", Go: "Handle", Type: "string"},
		field{JSON: "object", Go: "Object", Type: "NativeObject", Optional: true},
	),
	request("NativeWriteReadRequest",
		field{JSON: "handle", Go: "Handle", Type: "string"},
		field{JSON: "offset", Go: "Offset", Type: "int64"},
		field{JSON: "length", Go: "Length", Type: "uint32"},
	),
	response("NativeWriteReadResponse",
		field{JSON: "data", Go: "Data", Type: "byte", Array: true},
		field{JSON: "eof", Go: "EOF", Type: "bool"},
	),
	request("NativeWriteWriteRequest",
		field{JSON: "handle", Go: "Handle", Type: "string"},
		field{JSON: "offset", Go: "Offset", Type: "int64"},
		field{JSON: "data", Go: "Data", Type: "byte", Array: true},
	),
	response("NativeWriteWriteResponse", field{JSON: "written", Go: "Written", Type: "uint32"}),
	request("NativeWriteTruncateRequest",
		field{JSON: "handle", Go: "Handle", Type: "string"},
		field{JSON: "size", Go: "Size", Type: "int64"},
	),
	response("NativeWriteTruncateResponse", field{JSON: "size", Go: "Size", Type: "int64"}),
	request("NativeWriteSyncRequest", field{JSON: "handle", Go: "Handle", Type: "string"}),
	response("NativeWriteSyncResponse", field{JSON: "handle", Go: "Handle", Type: "string"}),
	request("NativeWriteCommitRequest", field{JSON: "handle", Go: "Handle", Type: "string"}),
	response("NativeWriteCommitResponse",
		field{JSON: "handle", Go: "Handle", Type: "string"},
		field{JSON: "mutation_id", Go: "MutationID", Type: "MutationID"},
		field{JSON: "object", Go: "Object", Type: "NativeObject", Optional: true},
	),
	request("NativeWriteAbortRequest", field{JSON: "handle", Go: "Handle", Type: "string"}),
	response("NativeWriteAbortResponse", field{JSON: "handle", Go: "Handle", Type: "string"}),
}

func main() {
	check := flag.Bool("check", false, "fail if generated output differs")
	flag.Parse()
	goSource, err := format.Source([]byte(render()))
	if err != nil {
		panic(err)
	}
	swiftSource, err := formatSwift([]byte(renderSwift()))
	if err != nil {
		panic(err)
	}
	outputs := map[string][]byte{
		filepath.Join(moduleRoot(), "mountproto", "messages_gen.go"):                                    goSource,
		filepath.Join(moduleRoot(), "Sources", "FuseKit", "Generated", "MountProtocol.generated.swift"): swiftSource,
	}
	for path, source := range outputs {
		if *check {
			existing, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(existing, source) {
				fmt.Fprintf(os.Stderr, "%s is stale\n", path)
				os.Exit(1)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			panic(err)
		}
		if err := os.WriteFile(path, source, 0o644); err != nil {
			panic(err)
		}
	}
}

func formatSwift(source []byte) ([]byte, error) {
	command := exec.Command("swift", "format", "-")
	command.Stdin = bytes.NewReader(source)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("mountproto/gen: swift format: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func moduleRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("mountproto/gen: caller path unavailable")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func render() string {
	var b strings.Builder
	b.WriteString("// Code generated by mountproto/gen; DO NOT EDIT.\n\npackage mountproto\n\n")
	fmt.Fprintf(
		&b,
		"const Version uint16 = 1\nconst RuntimeProtocolVersion uint16 = %d\nconst RuntimeHealthMaxResponseBytes = 16 << 10\nconst SchemaFingerprint = %q\n\n",
		wire.ProtocolVersion,
		schemaBuild(),
	)
	for _, enum := range enums {
		fmt.Fprintf(&b, "type %s string\n\nconst (\n", enum.Name)
		for _, value := range enum.Values {
			name := enum.GoNames[value]
			if name == "" {
				name = exported(value)
			}
			fmt.Fprintf(&b, "%s%s %s = %q\n", enum.Name, name, enum.Name, value)
		}
		b.WriteString(")\n\n")
	}
	b.WriteString("type TenantID string\ntype OwnerID string\ntype MutationID string\n\n")
	for _, message := range messages {
		fmt.Fprintf(&b, "type %s struct {\n", message.Name)
		for _, field := range message.Fields {
			typeName := field.Type
			if field.Array {
				typeName = "[]" + typeName
			}
			if field.Optional {
				typeName = "*" + typeName
			}
			tag := field.JSON
			if field.Optional {
				tag += ",omitempty"
			}
			fmt.Fprintf(&b, "%s %s `json:%q`\n", field.Go, typeName, tag)
		}
		b.WriteString("}\n\n")
	}
	return b.String()
}

func schemaBuild() string {
	raw, err := json.Marshal(struct {
		Version                       uint16
		RuntimeProtocol               uint16
		RuntimeHealthMaxResponseBytes int
		Enums                         []enum
		Messages                      []message
	}{
		Version: 1, RuntimeProtocol: wire.ProtocolVersion, RuntimeHealthMaxResponseBytes: 16 << 10,
		Enums: enums, Messages: messages,
	})
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(raw)
	return "fusekit.mount." + hex.EncodeToString(digest[:])
}

func exported(value string) string {
	var b strings.Builder
	upper := true
	for _, character := range value {
		if character == '_' || character == '.' {
			upper = true
			continue
		}
		if upper {
			b.WriteString(strings.ToUpper(string(character)))
			upper = false
			continue
		}
		b.WriteRune(character)
	}
	return b.String()
}
