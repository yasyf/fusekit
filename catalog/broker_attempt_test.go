package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestBrokerCommandSequenceSurvivesProcessRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	first, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := first.NextBrokerCommandID(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	secondID, err := second.NextBrokerCommandID(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if firstID != 1 || secondID != 2 {
		t.Fatalf("command IDs across restart = %d, %d, want 1, 2", firstID, secondID)
	}
}

func TestBrokerSignalAttemptIsDurableNoResendFence(t *testing.T) {
	store := newTestCatalog(t)
	first := testBrokerAttempt(1, "domain-1", 7)
	planned, created, err := store.BeginBrokerCommandAttempt(t.Context(), first)
	if err != nil || !created || planned.State != BrokerCommandPlanned {
		t.Fatalf("BeginBrokerCommandAttempt = %+v, %t, %v", planned, created, err)
	}
	sent, err := store.TransitionBrokerCommandAttempt(t.Context(), planned, BrokerCommandSent)
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := store.TransitionBrokerCommandAttempt(t.Context(), sent, BrokerCommandDeliveryUnknown)
	if err != nil || unknown.State != BrokerCommandDeliveryUnknown || unknown.SettledAt.IsZero() {
		t.Fatalf("unknown attempt = %+v, %v", unknown, err)
	}

	replay := first
	replay.CommandID = 2
	replay.AttemptID[0] = 2
	existing, created, err := store.BeginBrokerCommandAttempt(t.Context(), replay)
	if err != nil || created || existing.CommandID != first.CommandID ||
		existing.State != BrokerCommandDeliveryUnknown {
		t.Fatalf("replay = %+v, %t, %v", existing, created, err)
	}
	drift := replay
	drift.PayloadDigest = sha256.Sum256([]byte("different"))
	if _, _, err := store.BeginBrokerCommandAttempt(t.Context(), drift); !errors.Is(err, ErrBrokerAttemptConflict) {
		t.Fatalf("drift = %v, want ErrBrokerAttemptConflict", err)
	}

	next := testBrokerAttempt(3, "domain-1", 8)
	if _, created, err := store.BeginBrokerCommandAttempt(t.Context(), next); err != nil || !created {
		t.Fatalf("new revision = %t, %v", created, err)
	}
	var prior int
	if err := store.db.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM broker_command_attempts
WHERE domain_id = 'domain-1' AND revision = 7`).Scan(&prior); err != nil || prior != 0 {
		t.Fatalf("prior signal attempts = %d, %v", prior, err)
	}
	stale := testBrokerAttempt(4, "domain-1", 7)
	if _, _, err := store.BeginBrokerCommandAttempt(t.Context(), stale); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("stale revision = %v, want ErrInvalidTransition", err)
	}
}

func TestBrokerSignalSupersessionWaitsForUnsettledAttempt(t *testing.T) {
	store := newTestCatalog(t)
	first, _, err := store.BeginBrokerCommandAttempt(t.Context(), testBrokerAttempt(1, "domain-1", 7))
	if err != nil {
		t.Fatal(err)
	}
	sent, err := store.TransitionBrokerCommandAttempt(t.Context(), first, BrokerCommandSent)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.BeginBrokerCommandAttempt(
		t.Context(), testBrokerAttempt(2, "domain-1", 8),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("new revision while sent = %v, want ErrConflict", err)
	}
	accepted, err := store.TransitionBrokerCommandAttempt(t.Context(), sent, BrokerCommandAccepted)
	if err != nil || accepted.State != BrokerCommandAccepted {
		t.Fatalf("settle first attempt = %+v, %v", accepted, err)
	}
	next, created, err := store.BeginBrokerCommandAttempt(t.Context(), testBrokerAttempt(2, "domain-1", 8))
	if err != nil || !created || next.Revision != 8 {
		t.Fatalf("new revision after settlement = %+v, %t, %v", next, created, err)
	}
}

func TestBrokerSignalRecoveryPreservesSentFenceBeforeSupersession(t *testing.T) {
	store := newTestCatalog(t)
	first, _, err := store.BeginBrokerCommandAttempt(t.Context(), testBrokerAttempt(1, "domain-1", 7))
	if err != nil {
		t.Fatal(err)
	}
	sent, err := store.TransitionBrokerCommandAttempt(t.Context(), first, BrokerCommandSent)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.BeginBrokerCommandAttempt(
		t.Context(), testBrokerAttempt(2, "domain-1", 8),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("new revision while sent = %v, want ErrConflict", err)
	}
	if err := store.RecoverBrokerCommandAttempts(t.Context()); err != nil {
		t.Fatal(err)
	}
	replay := testBrokerAttempt(3, "domain-1", 7)
	recovered, created, err := store.BeginBrokerCommandAttempt(t.Context(), replay)
	if err != nil || created || recovered.AttemptID != sent.AttemptID ||
		recovered.State != BrokerCommandDeliveryUnknown {
		t.Fatalf("recovered sent fence = %+v, %t, %v", recovered, created, err)
	}
	next, created, err := store.BeginBrokerCommandAttempt(t.Context(), testBrokerAttempt(2, "domain-1", 8))
	if err != nil || !created || next.Revision != 8 {
		t.Fatalf("new revision after recovery = %+v, %t, %v", next, created, err)
	}
}

func TestBrokerAttemptAbandonAndReapedCleanupAreExact(t *testing.T) {
	store := newTestCatalog(t)
	attempt := testBrokerAttempt(1, "", 0)
	attempt.Kind = "list_domains"
	planned, _, err := store.BeginBrokerCommandAttempt(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AbandonBrokerCommandAttempt(t.Context(), planned); err != nil {
		t.Fatal(err)
	}
	if err := store.AbandonBrokerCommandAttempt(t.Context(), planned); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second abandon = %v, want ErrNotFound", err)
	}

	attempt.CommandID = 2
	attempt.AttemptID[0] = 2
	planned, _, err = store.BeginBrokerCommandAttempt(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	sent, err := store.TransitionBrokerCommandAttempt(t.Context(), planned, BrokerCommandSent)
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := store.TransitionBrokerCommandAttempt(t.Context(), sent, BrokerCommandDeliveryUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverReapedBrokerCommandAttempts(t.Context(), unknown.Process); err != nil {
		t.Fatal(err)
	}
	if _, err := readBrokerCommandAttempt(t.Context(), mustBeginBrokerAttemptTx(t, store), unknown.CommandID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reaped attempt = %v, want ErrNotFound", err)
	}
}

func TestReapedBrokerRecoverySettlesOnlyExactProcess(t *testing.T) {
	store := newTestCatalog(t)
	planned, _, err := store.BeginBrokerCommandAttempt(t.Context(), testBrokerAttempt(1, "planned", 1))
	if err != nil {
		t.Fatal(err)
	}
	sentPlan, _, err := store.BeginBrokerCommandAttempt(t.Context(), testBrokerAttempt(2, "sent", 1))
	if err != nil {
		t.Fatal(err)
	}
	sent, err := store.TransitionBrokerCommandAttempt(t.Context(), sentPlan, BrokerCommandSent)
	if err != nil {
		t.Fatal(err)
	}
	other := testBrokerAttempt(3, "other", 1)
	other.Process = BrokerProcessIdentity{
		PID: 43, StartTime: "start-2", Boot: "boot-1", Generation: "generation-2",
	}
	otherPlanned, _, err := store.BeginBrokerCommandAttempt(t.Context(), other)
	if err != nil {
		t.Fatal(err)
	}
	otherSent, err := store.TransitionBrokerCommandAttempt(t.Context(), otherPlanned, BrokerCommandSent)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverReapedBrokerCommandAttempts(t.Context(), planned.Process); err != nil {
		t.Fatal(err)
	}
	replanned := testBrokerAttempt(4, "planned", 1)
	if value, created, err := store.BeginBrokerCommandAttempt(t.Context(), replanned); err != nil ||
		!created || value.State != BrokerCommandPlanned {
		t.Fatalf("replanned command = %+v, %t, %v", value, created, err)
	}
	replay := testBrokerAttempt(5, "sent", 1)
	value, created, err := store.BeginBrokerCommandAttempt(t.Context(), replay)
	if err != nil || created || value.AttemptID != sent.AttemptID ||
		value.State != BrokerCommandDeliveryUnknown {
		t.Fatalf("sent recovery = %+v, %t, %v", value, created, err)
	}
	otherReplay := other
	otherReplay.AttemptID[0] = 6
	otherReplay.CommandID = 6
	value, created, err = store.BeginBrokerCommandAttempt(t.Context(), otherReplay)
	if err != nil || created || value.AttemptID != otherSent.AttemptID ||
		value.State != BrokerCommandSent {
		t.Fatalf("foreign process attempt = %+v, %t, %v", value, created, err)
	}
}

func TestBrokerAcceptedAttemptCannotRegress(t *testing.T) {
	store := newTestCatalog(t)
	attempt := testBrokerAttempt(1, "domain-1", 1)
	planned, _, err := store.BeginBrokerCommandAttempt(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionBrokerCommandAttempt(t.Context(), planned, BrokerCommandAccepted); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("planned to accepted = %v, want ErrInvalidTransition", err)
	}
	sent, err := store.TransitionBrokerCommandAttempt(t.Context(), planned, BrokerCommandSent)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := store.TransitionBrokerCommandAttempt(t.Context(), sent, BrokerCommandAccepted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionBrokerCommandAttempt(t.Context(), accepted, BrokerCommandDeliveryUnknown); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("accepted to unknown = %v, want ErrInvalidTransition", err)
	}
	if err := store.RecoverBrokerCommandAttempts(t.Context()); err != nil {
		t.Fatal(err)
	}
	replay := attempt
	replay.CommandID = 2
	replay.AttemptID[0] = 2
	recovered, created, err := store.BeginBrokerCommandAttempt(t.Context(), replay)
	if err != nil || created || recovered.AttemptID != accepted.AttemptID ||
		recovered.State != BrokerCommandAccepted {
		t.Fatalf("accepted recovery = %+v, %t, %v", recovered, created, err)
	}
}

func TestBrokerAttemptRecoveryDistinguishesPlannedFromSent(t *testing.T) {
	store := newTestCatalog(t)
	planned, _, err := store.BeginBrokerCommandAttempt(t.Context(), testBrokerAttempt(1, "planned", 1))
	if err != nil {
		t.Fatal(err)
	}
	sentPlan, _, err := store.BeginBrokerCommandAttempt(t.Context(), testBrokerAttempt(2, "sent", 1))
	if err != nil {
		t.Fatal(err)
	}
	sent, err := store.TransitionBrokerCommandAttempt(t.Context(), sentPlan, BrokerCommandSent)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverBrokerCommandAttempts(t.Context()); err != nil {
		t.Fatal(err)
	}
	replanned, created, err := store.BeginBrokerCommandAttempt(t.Context(), testBrokerAttempt(3, "planned", 1))
	if err != nil || !created || replanned.State != BrokerCommandPlanned {
		t.Fatalf("planned recovery = %+v, %t, %v", replanned, created, err)
	}
	recovered, created, err := store.BeginBrokerCommandAttempt(t.Context(), testBrokerAttempt(4, "sent", 1))
	if err != nil || created || recovered.AttemptID != sent.AttemptID ||
		recovered.State != BrokerCommandDeliveryUnknown {
		t.Fatalf("sent recovery = %+v, %t, %v", recovered, created, err)
	}
	if err := store.AbandonBrokerCommandAttempt(context.Background(), planned); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old planned attempt = %v, want ErrNotFound", err)
	}
}

func testBrokerAttempt(command uint64, domain string, revision uint64) BrokerCommandAttempt {
	return BrokerCommandAttempt{
		AttemptID: BrokerCommandAttemptID{byte(command)},
		CommandID: command,
		Process: BrokerProcessIdentity{
			PID: 42, StartTime: "start-1", Boot: "boot-1", Generation: "generation-1",
		},
		Kind: "signal_domain", PayloadDigest: sha256.Sum256([]byte("payload")),
		DomainID: domain, Revision: revision,
	}
}

func mustBeginBrokerAttemptTx(t *testing.T, store *Catalog) *sql.Tx {
	t.Helper()
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}
