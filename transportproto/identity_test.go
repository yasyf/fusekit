package transportproto

import (
	"os/exec"
	"strings"
	"testing"
)

func TestSuiteIdentityIncludesEverySchemaAndVersion(t *testing.T) {
	if BuildFor(Version, CatalogSchemaFingerprint, CatalogWorkerSchemaFingerprint, MountSchemaFingerprint, SourceDriverSchemaFingerprint) != Build {
		t.Fatal("generated build does not match its canonical schema inputs")
	}
	for name, got := range map[string]string{
		"version":        BuildFor(Version+1, CatalogSchemaFingerprint, CatalogWorkerSchemaFingerprint, MountSchemaFingerprint, SourceDriverSchemaFingerprint),
		"catalog":        BuildFor(Version, CatalogSchemaFingerprint+"-drift", CatalogWorkerSchemaFingerprint, MountSchemaFingerprint, SourceDriverSchemaFingerprint),
		"catalog worker": BuildFor(Version, CatalogSchemaFingerprint, CatalogWorkerSchemaFingerprint+"-drift", MountSchemaFingerprint, SourceDriverSchemaFingerprint),
		"mount":          BuildFor(Version, CatalogSchemaFingerprint, CatalogWorkerSchemaFingerprint, MountSchemaFingerprint+"-drift", SourceDriverSchemaFingerprint),
		"source driver":  BuildFor(Version, CatalogSchemaFingerprint, CatalogWorkerSchemaFingerprint, MountSchemaFingerprint, SourceDriverSchemaFingerprint+"-drift"),
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
