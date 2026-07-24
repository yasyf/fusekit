package holder

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDaemonFacingDeploymentBinaryContainsNoConcreteAppGroupPolicy(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "deployment-cli")
	command := exec.Command("go", "build", "-o", binary, "./testdata/deploymentcli")
	command.Env = append(os.Environ(), "GOFLAGS=-mod=readonly")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build daemon-facing deployment fixture: %v\n%s", err, output)
	}
	body, err := exec.Command("strings", "-a", binary).CombinedOutput()
	if err != nil {
		t.Fatalf("inspect daemon-facing deployment fixture: %v\n%s", err, body)
	}
	for _, forbidden := range [][]byte{
		[]byte("Library/Group Containers"),
		[]byte("ABCDE12345.example"),
	} {
		if bytes.Contains(body, forbidden) {
			t.Fatalf("daemon-facing deployment binary contains concrete App Group policy %q", forbidden)
		}
	}
}
