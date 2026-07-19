package sourceauthority

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/catalog"
)

func TestSourceTaskChildArgumentsRoundTripExactContract(t *testing.T) {
	journalRoot := shortSourceTaskJournalRoot(t)
	socket := filepath.Join(journalRoot, "source-task-123", "source.sock")
	identity := testSourceTaskIdentity()
	arguments, err := SourceTaskChildArguments(socket, journalRoot, identity)
	if err != nil {
		t.Fatalf("SourceTaskChildArguments: %v", err)
	}
	config, recognized, err := ParseSourceTaskChildArguments(arguments)
	if err != nil || !recognized {
		t.Fatalf("ParseSourceTaskChildArguments = %#v, %t, %v", config, recognized, err)
	}
	if config.Socket != socket || config.JournalRoot != journalRoot || !reflect.DeepEqual(config.Identity, identity) ||
		config.InvocationDigest == ([32]byte{}) {
		t.Fatalf("config = %#v, want socket/root/identity %#v", config, identity)
	}
}

func TestParseSourceTaskChildArgumentsRejectsUnknownAndMalformedInvocations(t *testing.T) {
	if config, recognized, err := ParseSourceTaskChildArguments([]string{"consumer-mode"}); err != nil || recognized {
		t.Fatalf("consumer invocation = %#v, %t, %v", config, recognized, err)
	}
	journalRoot := shortSourceTaskJournalRoot(t)
	socket := filepath.Join(journalRoot, "source-task-123", "source.sock")
	cases := [][]string{
		{sourceTaskChildArg},
		{sourceTaskChildArg, socket, journalRoot, "extra"},
		{sourceTaskChildArg, "relative.sock", journalRoot},
		{sourceTaskChildArg, filepath.Join(journalRoot, "worker-123", "source.sock"), journalRoot},
		{sourceTaskChildArg, socket, filepath.Join(journalRoot, "other")},
	}
	for _, arguments := range cases {
		if config, recognized, err := ParseSourceTaskChildArguments(arguments); err == nil || !recognized {
			t.Errorf("ParseSourceTaskChildArguments(%q) = %#v, %t, %v", arguments, config, recognized, err)
		}
	}
}

func TestSourceTaskChildArgumentsSocketPathBoundary(t *testing.T) {
	for _, test := range []struct {
		name    string
		length  int
		wantErr bool
	}{
		{name: "99 bytes accepted", length: 99},
		{name: "100 bytes rejected", length: 100, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			suffix := filepath.Join("source-task-boundary", "source.sock")
			prefix := "/tmp/"
			journalRoot := prefix + strings.Repeat("r", test.length-len(prefix)-len(suffix)-1)
			socket := filepath.Join(journalRoot, suffix)
			if got := len(socket); got != test.length {
				t.Fatalf("socket length = %d, want %d", got, test.length)
			}

			arguments, err := SourceTaskChildArguments(socket, journalRoot, testSourceTaskIdentity())
			if (err != nil) != test.wantErr {
				t.Fatalf("SourceTaskChildArguments error = %v, wantErr %t", err, test.wantErr)
			}
			if test.wantErr {
				config, recognized, parseErr := ParseSourceTaskChildArguments(
					[]string{sourceTaskChildArg, socket, journalRoot},
				)
				if parseErr == nil || !recognized {
					t.Fatalf("ParseSourceTaskChildArguments = %#v, %t, %v", config, recognized, parseErr)
				}
				return
			}
			if config, recognized, parseErr := ParseSourceTaskChildArguments(arguments); parseErr != nil || !recognized {
				t.Fatalf("ParseSourceTaskChildArguments = %#v, %t, %v", config, recognized, parseErr)
			}
		})
	}
}

func TestSourceTaskChildArgumentsRejectsOversizedDriverConfig(t *testing.T) {
	identity := testSourceTaskIdentity()
	identity.DriverConfig = make([]byte, catalog.SourceDriverConfigMaxBytes+1)
	journalRoot := shortSourceTaskJournalRoot(t)
	if _, err := SourceTaskChildArguments(
		filepath.Join(journalRoot, "source-task-123", "source.sock"), journalRoot, identity,
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
