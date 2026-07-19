package catalog

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceMutationExpectationRejectsUnknownPersistedState(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("corrupt-expectation-state")
	configureSourceObserverForIndexTest(t, c, authority)
	prepared := beginSourceExpectationMutation(t, c, authority, "corrupt-expectation-state")
	payload := []byte("expectation")
	if err := c.PutSourceMutationExpectation(t.Context(), SourceMutationExpectationRecord{
		Operation: prepared.OperationID, Authority: authority,
		Tenant: prepared.Tenant, Generation: 1, Origin: prepared.Intent.Origin,
		Digest: sha256.Sum256(payload), Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	corruptCheckedSourceRow(
		t, c,
		`UPDATE source_mutation_expectations SET state = 255 WHERE operation_id = ?`,
		prepared.OperationID[:],
	)
	if _, err := c.SourceMutationExpectation(
		t.Context(), authority, prepared.OperationID,
	); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("unknown persisted expectation state = %v, want ErrIntegrity", err)
	}
}

func TestSourcePhysicalIndexRejectsUnknownPersistedKind(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("corrupt-physical-kind")
	configureSourceObserverForIndexTest(t, c, authority)
	zero := [32]byte{}
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_physical_index(
    source_authority, root_id, relative_path, file_identity, physical_kind,
    metadata_fingerprint, content_fingerprint, payload
) VALUES (?, 'root', 'file', X'01', 1, ?, ?, X'01')`,
		string(authority), zero[:], zero[:]); err != nil {
		t.Fatal(err)
	}
	corruptCheckedSourceRow(
		t, c,
		`UPDATE source_physical_index SET physical_kind = 255
		 WHERE source_authority = ? AND root_id = 'root' AND relative_path = 'file'`,
		string(authority),
	)
	if _, err := c.SourcePhysicalIndexRecordsPage(
		t.Context(), authority, SourceIndexLocator{}, 1,
	); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("unknown persisted physical kind = %v, want ErrIntegrity", err)
	}
}

func TestSourceObserverConfigurationReceiptRejectsUnknownPersistedMode(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("corrupt-configuration-mode")
	roots := []SourceObserverRootRecord{{
		ID: "root", Generation: 1, Path: "/root",
		VolumeUUID: "volume", Inode: 1, Kind: 1,
	}}
	checkpoints := []SourceObserverCheckpointRecord{{
		Stream: "stream", RootEpoch: "epoch",
	}}
	identity := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{1}, "stream", "epoch", roots, checkpoints,
	)
	ensureSourceObserverFleetForTest(t, c, identity)
	if err := c.BeginSourceObserverConfiguration(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	ref, err := c.AppendSourceObserverConfigurationRoots(
		t.Context(), authority, identity.Operation,
		SourceObserverRootAppendPage{Records: roots},
	)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = c.AppendSourceObserverConfigurationCheckpoints(
		t.Context(), authority, identity.Operation,
		SourceObserverCheckpointAppendPage{
			Sequence: ref.Sequence, Records: checkpoints,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CommitSourceObserverConfiguration(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	corruptCheckedSourceRow(
		t, c,
		`UPDATE source_observer_configuration_receipts SET state = 255
		 WHERE source_authority = ? AND operation_id = ?`,
		string(authority), identity.Operation[:],
	)
	if _, err := c.CommitSourceObserverConfiguration(
		t.Context(), ref,
	); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("unknown persisted observer mode = %v, want ErrIntegrity", err)
	}
}

func corruptCheckedSourceRow(t *testing.T, c *Catalog, query string, args ...any) {
	t.Helper()
	connection, err := c.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close corruption connection: %v", err)
		}
	}()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatal(err)
	}
}
