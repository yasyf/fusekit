package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWireDriftChangesFingerprintAndFailsCheck(t *testing.T) {
	baseline, baselineFingerprint, err := generate(wireSpec)
	if err != nil {
		t.Fatalf("generate baseline: %v", err)
	}
	path := filepath.Join(t.TempDir(), "messages_gen.go")
	if err := os.WriteFile(path, baseline, 0o600); err != nil {
		t.Fatalf("write baseline: %v", err)
	}
	if err := run(true, wireSpec, path); err != nil {
		t.Fatalf("check baseline: %v", err)
	}

	mutations := map[string]string{
		"operation":  strings.Replace(wireSpec, `OperationRefresh Operation = "source.refresh"`, `OperationRefresh Operation = "source.refresh.changed"`, 1),
		"field type": strings.Replace(wireSpec, "Generation uint64", "Generation uint32", 1),
	}
	for name, definition := range mutations {
		t.Run(name, func(t *testing.T) {
			if definition == wireSpec {
				t.Fatal("mutation did not change canonical definition")
			}
			_, fingerprint, err := generate(definition)
			if err != nil {
				t.Fatalf("generate mutation: %v", err)
			}
			if fingerprint == baselineFingerprint {
				t.Fatalf("fingerprint remained %q after wire drift", fingerprint)
			}
			if err := run(true, definition, path); !errors.Is(err, errGeneratedOutputStale) {
				t.Fatalf("check error = %v, want stale output", err)
			}
		})
	}
}
