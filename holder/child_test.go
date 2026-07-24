package holder

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/sourceauthority"
)

func TestRunChildResolvesPhysicalDriverOnlyForExactSourceTaskChild(t *testing.T) {
	calls := 0
	resolutionStop := errors.New("physical driver resolution proved")
	drivers, err := NewDriverFactories(map[string]DriverFactory{
		"physical": {Physical: func(context.Context, sourceauthority.SourceTaskIdentity) (sourceauthority.AuthorityPolicy, error) {
			calls++
			return nil, resolutionStop
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	config := ChildConfig{Drivers: drivers}
	cases := [][]string{
		{"consumer-mode"},
		{"--daemonkit-trust-verifier-v1"},
		{"--fusekit-catalog-worker-v1"},
		{"fusekit-native-v1"},
		{"--fusekit-source-observer-child"},
		{"--fusekit-source-task-child"},
	}
	for _, arguments := range cases {
		_, _ = RunChild(t.Context(), arguments, config)
		if calls != 0 {
			t.Fatalf("fleet loader calls after %q = %d, want 0", arguments, calls)
		}
	}
	arguments := testSourceTaskArguments(t)
	handled, err := RunChild(t.Context(), arguments, config)
	if !handled || !errors.Is(err, resolutionStop) {
		t.Fatalf("source child = %t, %v, want exact resolution stop", handled, err)
	}
	if calls != 1 {
		t.Fatalf("physical factory calls = %d, want 1", calls)
	}
}

func TestRunChildRejectsUnknownPhysicalDriverBeforeSourceIO(t *testing.T) {
	arguments := testSourceTaskArguments(t)
	handled, err := RunChild(t.Context(), arguments, ChildConfig{})
	if !handled || err == nil {
		t.Fatalf("unknown physical DriverID = %t, %v", handled, err)
	}
}

func testSourceTaskArguments(t *testing.T) []string {
	t.Helper()
	journalRoot := filepath.Join("/tmp", filepath.Base(t.TempDir()))
	taskRoot := filepath.Join(journalRoot, "source-task-missing")
	arguments, err := sourceauthority.SourceTaskChildArguments(taskRoot, journalRoot, sourceauthority.SourceTaskIdentity{
		Owner: "product", FleetGeneration: 1, Authority: "source", AuthorityGeneration: 1,
		DriverID: "physical", DeclarationDigest: sha256.Sum256([]byte("declaration")),
	})
	if err != nil {
		t.Fatalf("SourceTaskChildArguments: %v", err)
	}
	return arguments
}
