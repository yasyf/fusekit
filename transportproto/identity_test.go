package transportproto

import (
	"os/exec"
	"strings"
	"testing"
)

func TestSuiteIdentityIncludesEverySchemaAndVersion(t *testing.T) {
	if BuildFor(Version, CatalogSchemaFingerprint, MountSchemaFingerprint) != Build {
		t.Fatal("generated build does not match its canonical schema inputs")
	}
	for name, got := range map[string]string{
		"version": BuildFor(Version+1, CatalogSchemaFingerprint, MountSchemaFingerprint),
		"catalog": BuildFor(Version, CatalogSchemaFingerprint+"-drift", MountSchemaFingerprint),
		"mount":   BuildFor(Version, CatalogSchemaFingerprint, MountSchemaFingerprint+"-drift"),
	} {
		if got == Build {
			t.Fatalf("%s drift did not change suite build", name)
		}
	}
}

func TestGeneratedSuiteIdentityIsCurrent(t *testing.T) {
	command := exec.Command("go", "run", "./gen", "-check")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated suite identity is stale: %v\n%s", err, output)
	}
}

func TestBuildHasOpaqueSuiteShape(t *testing.T) {
	if !strings.HasPrefix(Build, "fusekit.transport.") || len(Build) != len("fusekit.transport.")+64 {
		t.Fatalf("Build = %q", Build)
	}
}
