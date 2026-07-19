package catalog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBrokerAttemptCompactionBoundsHealthyGenerationAndRetainsReplayFence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	var lastSent BrokerCommandAttempt
	const total = 200
	for command := uint64(1); command <= total; command++ {
		attempt := retentionBrokerAttempt(command)
		planned, created, err := store.BeginBrokerCommandAttempt(t.Context(), attempt)
		if err != nil || !created {
			t.Fatalf("begin command %d = %+v, %t, %v", command, planned, created, err)
		}
		sent, err := store.TransitionBrokerCommandAttempt(t.Context(), planned, BrokerCommandSent)
		if err != nil {
			t.Fatalf("send command %d: %v", command, err)
		}
		if _, err := store.TransitionBrokerCommandAttempt(t.Context(), sent, BrokerCommandAccepted); err != nil {
			t.Fatalf("accept command %d: %v", command, err)
		}
		lastSent = sent
	}
	var compacted int
	for {
		result, err := store.CompactBrokerCommandAttempts(t.Context(), 17)
		if err != nil {
			t.Fatal(err)
		}
		compacted += result.Attempts
		if !result.More {
			break
		}
	}
	if compacted != total-1 {
		t.Fatalf("compacted attempts = %d, want %d", compacted, total-1)
	}
	assertBrokerAttemptRows(t, store, 1, total)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	replayed, err := store.TransitionBrokerCommandAttempt(t.Context(), lastSent, BrokerCommandAccepted)
	if err != nil || replayed.CommandID != total || replayed.State != BrokerCommandAccepted {
		t.Fatalf("accepted lost-response replay = %+v, %v", replayed, err)
	}
	forged := lastSent
	forged.PayloadDigest = sha256.Sum256([]byte("forged"))
	if _, err := store.TransitionBrokerCommandAttempt(
		t.Context(), forged, BrokerCommandAccepted,
	); !errors.Is(err, ErrBrokerAttemptConflict) {
		t.Fatalf("forged replay = %v, want ErrBrokerAttemptConflict", err)
	}
	result, err := store.CompactBrokerCommandAttempts(t.Context(), BrokerAttemptCompactionPageLimit)
	if err != nil || result != (BrokerAttemptCompactionResult{}) {
		t.Fatalf("replayed compaction = %+v, %v", result, err)
	}
	assertBrokerAttemptRows(t, store, 1, total)
}

func TestBrokerAttemptCompactionRequiresAcceptedProofAndExactGenerationSuccessor(t *testing.T) {
	store := newTestCatalog(t)
	accepted := retentionBrokerAttempt(1)
	planned, _, err := store.BeginBrokerCommandAttempt(t.Context(), accepted)
	if err != nil {
		t.Fatal(err)
	}
	sent, err := store.TransitionBrokerCommandAttempt(t.Context(), planned, BrokerCommandSent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionBrokerCommandAttempt(t.Context(), sent, BrokerCommandAccepted); err != nil {
		t.Fatal(err)
	}
	pending, _, err := store.BeginBrokerCommandAttempt(t.Context(), retentionBrokerAttempt(2))
	if err != nil {
		t.Fatal(err)
	}
	inflight, _, err := store.BeginBrokerCommandAttempt(t.Context(), retentionBrokerAttempt(3))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionBrokerCommandAttempt(t.Context(), inflight, BrokerCommandSent); err != nil {
		t.Fatal(err)
	}
	other := retentionBrokerAttempt(4)
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
	if _, err := store.TransitionBrokerCommandAttempt(t.Context(), otherSent, BrokerCommandAccepted); err != nil {
		t.Fatal(err)
	}
	result, err := store.CompactBrokerCommandAttempts(t.Context(), 8)
	if err != nil || result != (BrokerAttemptCompactionResult{}) {
		t.Fatalf("compaction without a same-generation accepted successor = %+v, %v", result, err)
	}
	if err := store.AbandonBrokerCommandAttempt(t.Context(), pending); err != nil {
		t.Fatal(err)
	}
	var attempts int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM broker_command_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("retained attempts = %d, want accepted fences plus sent attempt", attempts)
	}
}

func TestBrokerAttemptRetentionIsBoundedAcrossReapedProcessGenerationsAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	const generations = 200
	for generation := 1; generation <= generations; generation++ {
		attempt := retentionBrokerAttempt(uint64(generation))
		attempt.Process = BrokerProcessIdentity{
			PID: generation + 10, StartTime: fmt.Sprintf("start-%d", generation),
			Boot: "boot", Generation: fmt.Sprintf("generation-%d", generation),
		}
		planned, _, err := store.BeginBrokerCommandAttempt(t.Context(), attempt)
		if err != nil {
			t.Fatalf("begin generation %d: %v", generation, err)
		}
		sent, err := store.TransitionBrokerCommandAttempt(t.Context(), planned, BrokerCommandSent)
		if err != nil {
			t.Fatalf("send generation %d: %v", generation, err)
		}
		if _, err := store.TransitionBrokerCommandAttempt(
			t.Context(), sent, BrokerCommandAccepted,
		); err != nil {
			t.Fatalf("accept generation %d: %v", generation, err)
		}
		if result, err := store.CompactBrokerCommandAttempts(
			t.Context(), BrokerAttemptCompactionPageLimit,
		); err != nil || result != (BrokerAttemptCompactionResult{}) {
			t.Fatalf("generation %d replay fence = %+v, %v", generation, result, err)
		}
		var rows int
		if err := store.db.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM broker_command_attempts`).Scan(&rows); err != nil || rows != 1 {
			t.Fatalf("generation %d rows before reap = %d, %v; want exact fence", generation, rows, err)
		}
		wrong := attempt.Process
		wrong.Generation += "-wrong"
		if err := store.RecoverReapedBrokerCommandAttempts(t.Context(), wrong); err != nil {
			t.Fatalf("recover substituted generation %d: %v", generation, err)
		}
		if err := store.db.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM broker_command_attempts`).Scan(&rows); err != nil || rows != 1 {
			t.Fatalf("generation %d substituted reap rows = %d, %v; want 1", generation, rows, err)
		}
		if err := store.RecoverReapedBrokerCommandAttempts(t.Context(), attempt.Process); err != nil {
			t.Fatalf("recover generation %d: %v", generation, err)
		}
		if err := store.db.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM broker_command_attempts`).Scan(&rows); err != nil || rows != 0 {
			t.Fatalf("generation %d rows after exact reap = %d, %v", generation, rows, err)
		}
		if generation%25 == 0 {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(t.Context(), path)
			if err != nil {
				t.Fatalf("reopen after generation %d: %v", generation, err)
			}
		}
	}
	current := retentionBrokerAttempt(generations + 1)
	current.Process = BrokerProcessIdentity{
		PID: 1000, StartTime: "current-start", Boot: "boot", Generation: "current-generation",
	}
	planned, _, err := store.BeginBrokerCommandAttempt(t.Context(), current)
	if err != nil {
		t.Fatal(err)
	}
	sent, err := store.TransitionBrokerCommandAttempt(t.Context(), planned, BrokerCommandSent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionBrokerCommandAttempt(t.Context(), sent, BrokerCommandAccepted); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	replayed, err := store.TransitionBrokerCommandAttempt(t.Context(), sent, BrokerCommandAccepted)
	if err != nil || replayed.AttemptID != sent.AttemptID {
		t.Fatalf("current generation replay fence = %+v, %v", replayed, err)
	}
	assertBrokerAttemptRows(t, store, 1, generations+1)
}

func TestExpiredFileProviderLeaseCompactionIsBoundedReplayAndGenerationSafe(t *testing.T) {
	store := newTestCatalog(t)
	provision, domain := registerRetentionDomain(t, store, "lease-retention", 7)
	now := time.Unix(10_000, 0).UTC()
	const expired = 513
	for index := 0; index < expired; index++ {
		lease := FileProviderLease{
			ID: fmt.Sprintf("expired-%04d", index), Tenant: provision.Tenant,
			DomainID: domain.DomainID, Generation: provision.Generation,
			ExpiresAt: now.Add(-time.Duration(index+1) * time.Second),
		}
		if err := store.RenewFileProviderLease(t.Context(), lease); err != nil {
			t.Fatalf("renew expired lease %d: %v", index, err)
		}
	}
	live := FileProviderLease{
		ID: "live", Tenant: provision.Tenant, DomainID: domain.DomainID,
		Generation: provision.Generation, ExpiresAt: now.Add(time.Hour),
	}
	if err := store.RenewFileProviderLease(t.Context(), live); err != nil {
		t.Fatal(err)
	}
	if err := store.RenewFileProviderLease(t.Context(), live); err != nil {
		t.Fatalf("exact renewal replay: %v", err)
	}
	regressed := live
	regressed.ExpiresAt = live.ExpiresAt.Add(-time.Second)
	if err := store.RenewFileProviderLease(t.Context(), regressed); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("lease expiry regression = %v, want ErrInvalidTransition", err)
	}
	otherProvision, otherDomain := registerRetentionDomain(t, store, "lease-other", 3)
	substituted := live
	substituted.Tenant = otherProvision.Tenant
	substituted.DomainID = otherDomain.DomainID
	substituted.Generation = otherProvision.Generation
	if err := store.RenewFileProviderLease(t.Context(), substituted); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("lease identity substitution = %v, want ErrInvalidTransition", err)
	}

	var retired int
	for {
		result, err := store.CompactExpiredFileProviderLeases(t.Context(), now, 31)
		if err != nil {
			t.Fatal(err)
		}
		retired += result.Leases
		if !result.More {
			break
		}
	}
	if retired != expired {
		t.Fatalf("retired leases = %d, want %d", retired, expired)
	}
	replayed, err := store.CompactExpiredFileProviderLeases(
		t.Context(), now, FileProviderLeaseCompactionPageLimit,
	)
	if err != nil || replayed != (FileProviderLeaseCompactionResult{}) {
		t.Fatalf("lease compaction replay = %+v, %v", replayed, err)
	}
	leases, _, err := store.FileProviderDemand(
		t.Context(), provision.Tenant, domain.DomainID, provision.Generation, now,
	)
	if err != nil || leases != 1 {
		t.Fatalf("live successor demand = %d, %v; want 1", leases, err)
	}
	var rows int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM file_provider_leases`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("lease rows after churn = %d, want 1", rows)
	}
}

func TestRetentionQueriesUseBoundedCleanupIndexes(t *testing.T) {
	store := newTestCatalog(t)
	brokerPlan := queryPlanForTest(t, store, `
SELECT old.attempt_id
FROM broker_command_attempts old
WHERE old.command_kind <> 'signal_domain' AND old.state = 3
  AND EXISTS (
      SELECT 1 FROM broker_command_attempts fence
      WHERE fence.process_pid = old.process_pid
        AND fence.process_start_time = old.process_start_time
        AND fence.process_boot = old.process_boot
        AND fence.process_generation = old.process_generation
        AND fence.command_kind <> 'signal_domain' AND fence.state = 3
        AND fence.command_id > old.command_id
  )
ORDER BY old.process_pid, old.process_start_time, old.process_boot,
         old.process_generation, old.command_id, old.attempt_id
LIMIT ?`, 257)
	if !strings.Contains(brokerPlan, "broker_command_attempts_terminal_cleanup") {
		t.Fatalf("broker cleanup plan = %q, want terminal cleanup index", brokerPlan)
	}
	leasePlan := queryPlanForTest(t, store, `
SELECT lease_id, tenant, domain_id, generation, expires_unix_nano
FROM file_provider_leases
WHERE expires_unix_nano <= ?
ORDER BY expires_unix_nano, lease_id
LIMIT ?`, time.Now().UnixNano(), 257)
	if !strings.Contains(leasePlan, "file_provider_leases_expired") {
		t.Fatalf("lease cleanup plan = %q, want expiry index", leasePlan)
	}
}

func TestRetentionCompactionRejectsInvalidBoundsWithoutMutation(t *testing.T) {
	store := newTestCatalog(t)
	for _, limit := range []int{0, -1, BrokerAttemptCompactionPageLimit + 1} {
		if _, err := store.CompactBrokerCommandAttempts(
			t.Context(), limit,
		); !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("broker limit %d = %v, want ErrInvalidObject", limit, err)
		}
	}
	for _, test := range []struct {
		name  string
		now   time.Time
		limit int
	}{
		{name: "zero time", limit: 1},
		{name: "pre-epoch", now: time.Unix(-1, 0), limit: 1},
		{name: "zero limit", now: time.Unix(1, 0)},
		{name: "negative limit", now: time.Unix(1, 0), limit: -1},
		{
			name: "over limit", now: time.Unix(1, 0),
			limit: FileProviderLeaseCompactionPageLimit + 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.CompactExpiredFileProviderLeases(
				t.Context(), test.now, test.limit,
			); !errors.Is(err, ErrInvalidObject) {
				t.Fatalf("lease compaction = %v, want ErrInvalidObject", err)
			}
		})
	}
}

func retentionBrokerAttempt(command uint64) BrokerCommandAttempt {
	attempt := testBrokerAttempt(command, "", 0)
	attempt.AttemptID = BrokerCommandAttemptID{
		byte(command >> 56), byte(command >> 48), byte(command >> 40), byte(command >> 32),
		byte(command >> 24), byte(command >> 16), byte(command >> 8), byte(command),
		0x72, 0x65, 0x74, 0x65, 0x6e, 0x74, 0x69, 0x6f,
	}
	attempt.Kind = "list_domains"
	attempt.PayloadDigest = sha256.Sum256([]byte(fmt.Sprintf("payload-%d", command)))
	return attempt
}

func assertBrokerAttemptRows(t *testing.T, store *Catalog, wantRows int, wantCommand uint64) {
	t.Helper()
	var rows int
	var command uint64
	if err := store.db.QueryRowContext(t.Context(), `
SELECT COUNT(*), COALESCE(MAX(command_id), 0) FROM broker_command_attempts
WHERE command_kind <> 'signal_domain'`).Scan(&rows, &command); err != nil {
		t.Fatal(err)
	}
	if rows != wantRows || command != wantCommand {
		t.Fatalf("broker attempt rows/max = %d/%d, want %d/%d", rows, command, wantRows, wantCommand)
	}
}

func registerRetentionDomain(
	t *testing.T,
	store *Catalog,
	name string,
	generation Generation,
) (TenantProvision, FileProviderDomain) {
	t.Helper()
	provision, err := store.ProvisionTenant(t.Context(), testTenantProvision(t, name, generation))
	if err != nil {
		t.Fatal(err)
	}
	domain, found, err := store.FileProviderDomainForTenant(t.Context(), provision.Tenant)
	if err != nil || !found {
		t.Fatalf("FileProviderDomainForTenant = %+v, %t, %v", domain, found, err)
	}
	domain.Registered = true
	domain.PublicPath = filepath.Join(t.TempDir(), "Domain")
	if err := store.ConfirmFileProviderDomain(t.Context(), domain); err != nil {
		t.Fatal(err)
	}
	return provision, domain
}

func queryPlanForTest(t *testing.T, store *Catalog, statement string, args ...any) string {
	t.Helper()
	rows, err := store.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+statement, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan.String()
}
