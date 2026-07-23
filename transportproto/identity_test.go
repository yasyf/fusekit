package transportproto

import (
	"os/exec"
	"strings"
	"testing"
)

func TestSuiteIdentityIncludesEverySchemaAndVersion(t *testing.T) {
	if WireBuildFor(Version, CatalogSchemaFingerprint, CatalogWorkerSchemaFingerprint, MountSchemaFingerprint, SourceDriverSchemaFingerprint) != WireBuild {
		t.Fatal("generated wire build does not match its canonical schema inputs")
	}
	for name, got := range map[string]string{
		"version":        WireBuildFor(Version+1, CatalogSchemaFingerprint, CatalogWorkerSchemaFingerprint, MountSchemaFingerprint, SourceDriverSchemaFingerprint),
		"catalog":        WireBuildFor(Version, CatalogSchemaFingerprint+"-drift", CatalogWorkerSchemaFingerprint, MountSchemaFingerprint, SourceDriverSchemaFingerprint),
		"catalog worker": WireBuildFor(Version, CatalogSchemaFingerprint, CatalogWorkerSchemaFingerprint+"-drift", MountSchemaFingerprint, SourceDriverSchemaFingerprint),
		"mount":          WireBuildFor(Version, CatalogSchemaFingerprint, CatalogWorkerSchemaFingerprint, MountSchemaFingerprint+"-drift", SourceDriverSchemaFingerprint),
		"source driver":  WireBuildFor(Version, CatalogSchemaFingerprint, CatalogWorkerSchemaFingerprint, MountSchemaFingerprint, SourceDriverSchemaFingerprint+"-drift"),
	} {
		if got == WireBuild {
			t.Fatalf("%s drift did not change suite wire build", name)
		}
	}
}

func TestGeneratedSuiteIdentityIsCurrent(t *testing.T) {
	command := exec.Command("go", "run", "./gen", "-check")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated suite identity is stale: %v\n%s", err, output)
	}
}

func TestWireBuildHasExactV1SuiteShape(t *testing.T) {
	prefix := "com.yasyf.fusekit.transport/"
	if !strings.HasPrefix(WireBuild, prefix) || !strings.HasSuffix(WireBuild, "/v1") ||
		len(WireBuild) != len(prefix)+64+len("/v1") {
		t.Fatalf("WireBuild = %q", WireBuild)
	}
}
