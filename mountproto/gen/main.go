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
	"path/filepath"
	"runtime"
	"strings"
)

type enum struct {
	Name   string
	Values []string
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
	{Name: "Operation", Values: []string{"tenant.register", "tenant.replace", "tenant.remove", "tenant.prepare", "tenant.state"}},
	{Name: "ErrorCode", Values: []string{"ok", "invalid_request", "unauthorized", "not_found", "conflict", "quarantined", "canceled", "unavailable"}},
	{Name: "AccessMode", Values: []string{"read_only", "read_write"}},
	{Name: "CasePolicy", Values: []string{"sensitive", "insensitive"}},
	{Name: "Presentation", Values: []string{"mount", "file_provider"}},
	{Name: "QuarantineLane", Values: []string{"catalog_mutation", "materialization", "enumeration", "mount_lifecycle"}},
	{Name: "QuarantineCause", Values: []string{"conflict", "integrity", "unsettled", "unavailable"}},
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
	{Name: "TenantDefinition", Fields: []field{
		{JSON: "presentation_root", Go: "PresentationRoot", Type: "string"},
		{JSON: "backing_root", Go: "BackingRoot", Type: "string"},
		{JSON: "content_source_id", Go: "ContentSourceID", Type: "string"},
		{JSON: "access_mode", Go: "AccessMode", Type: "AccessMode"},
		{JSON: "case_policy", Go: "CasePolicy", Type: "CasePolicy"},
		{JSON: "presentations", Go: "Presentations", Type: "Presentation", Array: true},
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
		{JSON: "tenant_id", Go: "TenantID", Type: "TenantID"},
		{JSON: "generation", Go: "Generation", Type: "uint64"},
		{JSON: "requested", Go: "Requested", Type: "uint64"},
		{JSON: "desired", Go: "Desired", Type: "uint64"},
		{JSON: "observed", Go: "Observed", Type: "uint64"},
		{JSON: "verified", Go: "Verified", Type: "uint64"},
		{JSON: "applied", Go: "Applied", Type: "uint64"},
		{JSON: "activated_generation", Go: "ActivatedGeneration", Type: "uint64"},
		{JSON: "state_version", Go: "StateVersion", Type: "uint64"},
		{JSON: "quarantine", Go: "Quarantine", Type: "Quarantine", Optional: true},
	}},
	request("RegisterTenantRequest", field{JSON: "definition", Go: "Definition", Type: "TenantDefinition"}),
	response("RegisterTenantResponse", field{JSON: "tenant_id", Go: "TenantID", Type: "TenantID"}, field{JSON: "generation", Go: "Generation", Type: "uint64"}),
	request("ReplaceTenantRequest", field{JSON: "expected_generation", Go: "ExpectedGeneration", Type: "uint64"}, field{JSON: "definition", Go: "Definition", Type: "TenantDefinition"}),
	response("ReplaceTenantResponse", field{JSON: "tenant_id", Go: "TenantID", Type: "TenantID"}, field{JSON: "generation", Go: "Generation", Type: "uint64"}),
	request("RemoveTenantRequest", field{JSON: "generation", Go: "Generation", Type: "uint64"}),
	response("RemoveTenantResponse", field{JSON: "tenant_id", Go: "TenantID", Type: "TenantID"}, field{JSON: "generation", Go: "Generation", Type: "uint64"}),
	request("PrepareTenantRequest", field{JSON: "generation", Go: "Generation", Type: "uint64"}, field{JSON: "revision", Go: "Revision", Type: "uint64"}),
	response("PrepareTenantResponse", field{JSON: "state", Go: "State", Type: "TenantState", Optional: true}),
	request("StateRequest", field{JSON: "generation", Go: "Generation", Type: "uint64"}),
	response("StateResponse", field{JSON: "state", Go: "State", Type: "TenantState", Optional: true}),
}

func main() {
	check := flag.Bool("check", false, "fail if generated output differs")
	flag.Parse()
	source, err := format.Source([]byte(render()))
	if err != nil {
		panic(err)
	}
	path := filepath.Join(moduleRoot(), "mountproto", "messages_gen.go")
	if *check {
		existing, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(existing, source) {
			fmt.Fprintf(os.Stderr, "%s is stale\n", path)
			os.Exit(1)
		}
		return
	}
	if err := os.WriteFile(path, source, 0o644); err != nil {
		panic(err)
	}
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
	fmt.Fprintf(&b, "const Version uint16 = 1\nconst Build = %q\n\n", schemaBuild())
	for _, enum := range enums {
		fmt.Fprintf(&b, "type %s string\n\nconst (\n", enum.Name)
		for _, value := range enum.Values {
			fmt.Fprintf(&b, "%s%s %s = %q\n", enum.Name, exported(value), enum.Name, value)
		}
		b.WriteString(")\n\n")
	}
	b.WriteString("type TenantID string\n\n")
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
		Version  uint16
		Enums    []enum
		Messages []message
	}{Version: 1, Enums: enums, Messages: messages})
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
