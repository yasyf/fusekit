package holder

import (
	"slices"
	"testing"
)

func TestWorkerChildEnvironmentReservesDaemonkitKeys(t *testing.T) {
	input := []string{
		"PATH=/custom/bin",
		"LANG=fr_FR.UTF-8",
		"CGOFUSE_LIBFUSE_PATH=/tmp/libfuse.dylib",
		"FUSEKIT_SENTINEL=present",
		"malformed",
	}
	want := []string{"FUSEKIT_SENTINEL=present", "malformed"}
	if got := workerChildEnvironment(input); !slices.Equal(got, want) {
		t.Fatalf("workerChildEnvironment() = %q, want %q", got, want)
	}
	if input[0] != "PATH=/custom/bin" {
		t.Fatalf("workerChildEnvironment mutated input: %q", input)
	}
}
