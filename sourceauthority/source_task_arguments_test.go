package sourceauthority

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yasyf/fusekit/catalog"
)

func TestSourceTaskChildArgumentsRoundTripExactContract(t *testing.T) {
	journalRoot := shortSourceTaskJournalRoot(t)
	taskRoot := filepath.Join(journalRoot, "source-task-123")
	identity := testSourceTaskIdentity()
	arguments, err := SourceTaskChildArguments(taskRoot, journalRoot, identity)
	if err != nil {
		t.Fatalf("SourceTaskChildArguments: %v", err)
	}
	config, recognized, err := ParseSourceTaskChildArguments(arguments)
	if err != nil || !recognized {
		t.Fatalf("ParseSourceTaskChildArguments = %#v, %t, %v", config, recognized, err)
	}
	if config.TaskRoot != taskRoot || config.JournalRoot != journalRoot || !reflect.DeepEqual(config.Identity, identity) ||
		config.InvocationDigest == ([32]byte{}) {
		t.Fatalf("config = %#v, want task root/journal root/identity %#v", config, identity)
	}
}

func TestParseSourceTaskChildArgumentsRejectsUnknownAndMalformedInvocations(t *testing.T) {
	if config, recognized, err := ParseSourceTaskChildArguments([]string{"consumer-mode"}); err != nil || recognized {
		t.Fatalf("consumer invocation = %#v, %t, %v", config, recognized, err)
	}
	journalRoot := shortSourceTaskJournalRoot(t)
	taskRoot := filepath.Join(journalRoot, "source-task-123")
	cases := [][]string{
		{sourceTaskChildArg},
		{sourceTaskChildArg, taskRoot, journalRoot, "extra"},
		{sourceTaskChildArg, "relative", journalRoot},
		{sourceTaskChildArg, filepath.Join(journalRoot, "worker-123"), journalRoot},
		{sourceTaskChildArg, taskRoot, filepath.Join(journalRoot, "other")},
	}
	for _, arguments := range cases {
		if config, recognized, err := ParseSourceTaskChildArguments(arguments); err == nil || !recognized {
			t.Errorf("ParseSourceTaskChildArguments(%q) = %#v, %t, %v", arguments, config, recognized, err)
		}
	}
}

func TestSourceTaskChildArgumentsRejectsOversizedDriverConfig(t *testing.T) {
	identity := testSourceTaskIdentity()
	identity.DriverConfig = make([]byte, catalog.SourceDriverConfigMaxBytes+1)
	journalRoot := shortSourceTaskJournalRoot(t)
	if _, err := SourceTaskChildArguments(
		filepath.Join(journalRoot, "source-task-123"), journalRoot, identity,
	); err == nil {
		t.Fatal("oversized DriverConfig was accepted")
	}
}

func testSourceTaskIdentity() SourceTaskIdentity {
	return SourceTaskIdentity{
		Owner: "product", FleetGeneration: 7, Authority: "source", AuthorityGeneration: 7,
		DriverID: "physical", DriverConfig: []byte("root=/tmp/example"),
		DeclarationDigest: sha256.Sum256([]byte("declaration")),
	}
}

func shortSourceTaskJournalRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "fk-source-task-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("RemoveAll(%q): %v", root, err)
		}
	})
	return root
}
