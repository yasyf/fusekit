package trustroles

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSwiftSessionRolesMatchGo(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve trustroles test path")
	}
	source, err := os.ReadFile(filepath.Join(
		filepath.Dir(file), "..", "Sources", "FuseKit", "SessionPeerRole.swift",
	))
	if err != nil {
		t.Fatal(err)
	}
	for name, role := range map[string]string{
		"broker":                string(Broker),
		"fileProviderExtension": string(FileProviderExtension),
	} {
		declaration := fmt.Sprintf("static let %s = %q", name, role)
		if !strings.Contains(string(source), declaration) {
			t.Fatalf("Swift role declaration missing %q", declaration)
		}
	}
}
