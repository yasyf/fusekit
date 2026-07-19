package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yasyf/fusekit/causal"
)

// FileProviderDomain is one desired tenant domain plus its last proven system state.
type FileProviderDomain struct {
	DomainID        causal.DomainID
	OwnerID         string
	Tenant          TenantID
	Generation      Generation
	Root            ObjectID
	AccountInstance string
	DisplayName     string
	PublicPath      string
	Registered      bool
}

// FileProviderDomainRemoval is one durable exact-domain retirement intent.
type FileProviderDomainRemoval struct {
	Domain          FileProviderDomain
	ConfirmedAbsent bool
}

// FileProviderCutover is the durable one-shot fence that disables domain
// reconciliation before the signed broker's first authoritative enumeration.
type FileProviderCutover struct {
	OperationID MutationID
	PlanHash    [32]byte
	StartedAt   time.Time
	State       FileProviderCutoverState
	ProofHash   [32]byte
	ProofJSON   []byte
	ProofBoot   string
	ProofUptime time.Duration
	ClaimedAt   time.Time
}

// FileProviderCutoverState is the durable one-way proof-consumption state.
type FileProviderCutoverState uint8

const (
	FileProviderCutoverFenced FileProviderCutoverState = iota + 1
	FileProviderCutoverProved
	FileProviderCutoverClaimed
	FileProviderCutoverExpired
)

// BeginFileProviderCutover durably enters the one-shot domain cutover mode.
// A replay of the same account plan adopts the original random operation ID;
// a different plan cannot replace the active fence.
func (c *Catalog) BeginFileProviderCutover(
	ctx context.Context,
	operation MutationID,
	planHash [32]byte,
) (FileProviderCutover, bool, error) {
	if operation == (MutationID{}) || planHash == ([32]byte{}) {
		return FileProviderCutover{}, false, fmt.Errorf("%w: File Provider cutover identity is incomplete", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return FileProviderCutover{}, false, fmt.Errorf("catalog: begin File Provider cutover: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	mode, found, err := fileProviderCutover(ctx, tx)
	if err != nil {
		return FileProviderCutover{}, false, err
	}
	if found {
		if !bytes.Equal(mode.PlanHash[:], planHash[:]) {
			return FileProviderCutover{}, false, fmt.Errorf("%w: another File Provider cutover plan is already fenced", ErrConflict)
		}
		if mode.State == FileProviderCutoverExpired {
			now := time.Now().UTC()
			result, err := tx.ExecContext(ctx, `
UPDATE file_provider_cutover
SET operation_id = ?, started_unix_nano = ?, state = ?, proof_hash = X'', proof_json = X'',
    proof_boot = '', proof_uptime_nano = 0, claimed_unix_nano = 0
WHERE singleton = 1 AND state = ? AND plan_hash = ?`,
				operation[:], now.UnixNano(), FileProviderCutoverFenced, FileProviderCutoverExpired, planHash[:])
			if err != nil {
				return FileProviderCutover{}, false, mapConstraint(err)
			}
			if changed, err := result.RowsAffected(); err != nil || changed != 1 {
				return FileProviderCutover{}, false, fmt.Errorf("%w: expired File Provider cutover rotation raced", ErrConflict)
			}
			if err := tx.Commit(); err != nil {
				return FileProviderCutover{}, false, fmt.Errorf("catalog: commit File Provider cutover rotation: %w", err)
			}
			return FileProviderCutover{
				OperationID: operation, PlanHash: planHash, StartedAt: now, State: FileProviderCutoverFenced,
			}, true, nil
		}
		if err := tx.Commit(); err != nil {
			return FileProviderCutover{}, false, fmt.Errorf("catalog: finish File Provider cutover replay: %w", err)
		}
		return mode, false, nil
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO file_provider_cutover(
    singleton, operation_id, plan_hash, started_unix_nano, state, proof_hash, proof_json,
    proof_boot, proof_uptime_nano, claimed_unix_nano
) VALUES (1, ?, ?, ?, ?, X'', X'', '', 0, 0)`, operation[:], planHash[:], now.UnixNano(), FileProviderCutoverFenced); err != nil {
		return FileProviderCutover{}, false, mapConstraint(err)
	}
	if err := tx.Commit(); err != nil {
		return FileProviderCutover{}, false, fmt.Errorf("catalog: commit File Provider cutover: %w", err)
	}
	return FileProviderCutover{
		OperationID: operation, PlanHash: planHash, StartedAt: now, State: FileProviderCutoverFenced,
	}, true, nil
}

// RecordFileProviderCutoverProof atomically binds the one authoritative proof
// to a fenced operation before it can leave the holder.
func (c *Catalog) RecordFileProviderCutoverProof(
	ctx context.Context,
	operation MutationID,
	planHash [32]byte,
	proofHash [32]byte,
	proofJSON []byte,
	proofBoot string,
	proofUptime time.Duration,
) error {
	if operation == (MutationID{}) || planHash == ([32]byte{}) || proofHash == ([32]byte{}) || len(proofJSON) == 0 ||
		proofBoot == "" || proofUptime < 0 {
		return fmt.Errorf("%w: File Provider cutover proof identity is incomplete", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin File Provider cutover proof: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	mode, found, err := fileProviderCutover(ctx, tx)
	if err != nil {
		return err
	}
	if !found || mode.OperationID != operation || mode.PlanHash != planHash {
		return fmt.Errorf("%w: File Provider cutover proof does not match the durable fence", ErrConflict)
	}
	switch mode.State {
	case FileProviderCutoverFenced:
		result, err := tx.ExecContext(ctx, `
UPDATE file_provider_cutover
SET state = ?, proof_hash = ?, proof_json = ?, proof_boot = ?, proof_uptime_nano = ?
WHERE singleton = 1 AND state = ?`,
			FileProviderCutoverProved, proofHash[:], proofJSON, proofBoot, int64(proofUptime), FileProviderCutoverFenced)
		if err != nil {
			return mapConstraint(err)
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return fmt.Errorf("%w: File Provider cutover proof raced another caller", ErrConflict)
		}
	case FileProviderCutoverProved:
		if mode.ProofHash != proofHash || !bytes.Equal(mode.ProofJSON, proofJSON) ||
			mode.ProofBoot != proofBoot || mode.ProofUptime != proofUptime {
			return fmt.Errorf("%w: another File Provider cutover proof is already recorded", ErrConflict)
		}
	case FileProviderCutoverClaimed:
		return fmt.Errorf("%w: File Provider cutover proof is already claimed", ErrInvalidTransition)
	case FileProviderCutoverExpired:
		return fmt.Errorf("%w: File Provider cutover proof requires fresh enumeration", ErrCutoverProofExpired)
	default:
		return fmt.Errorf("%w: invalid File Provider cutover state", ErrIntegrity)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit File Provider cutover proof: %w", err)
	}
	return nil
}

// ClaimFileProviderCutoverProof atomically consumes one recorded proof. A
// concurrent or replaying caller cannot claim the same nonce again.
func (c *Catalog) ClaimFileProviderCutoverProof(
	ctx context.Context,
	operation MutationID,
	proofHash [32]byte,
	boot string,
	uptime time.Duration,
	ttl time.Duration,
) (FileProviderCutover, error) {
	if operation == (MutationID{}) || proofHash == ([32]byte{}) || boot == "" || uptime < 0 || ttl <= 0 {
		return FileProviderCutover{}, fmt.Errorf("%w: File Provider cutover claim identity is incomplete", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return FileProviderCutover{}, fmt.Errorf("catalog: begin File Provider cutover claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	mode, found, err := fileProviderCutover(ctx, tx)
	if err != nil {
		return FileProviderCutover{}, err
	}
	if !found || mode.OperationID != operation || mode.ProofHash != proofHash {
		return FileProviderCutover{}, fmt.Errorf("%w: File Provider cutover claim does not match the recorded proof", ErrConflict)
	}
	if mode.State == FileProviderCutoverExpired {
		return FileProviderCutover{}, fmt.Errorf("%w: File Provider cutover proof requires fresh enumeration", ErrCutoverProofExpired)
	}
	if mode.State != FileProviderCutoverProved {
		return FileProviderCutover{}, fmt.Errorf("%w: File Provider cutover proof is not claimable", ErrInvalidTransition)
	}
	if cutoverProofExpired(mode, boot, uptime, ttl) {
		result, err := tx.ExecContext(ctx, `
UPDATE file_provider_cutover SET state = ?
WHERE singleton = 1 AND state = ? AND operation_id = ? AND proof_hash = ?`,
			FileProviderCutoverExpired, FileProviderCutoverProved, operation[:], proofHash[:])
		if err != nil {
			return FileProviderCutover{}, mapConstraint(err)
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			return FileProviderCutover{}, fmt.Errorf("%w: File Provider cutover expiry raced another claim", ErrConflict)
		}
		if err := tx.Commit(); err != nil {
			return FileProviderCutover{}, fmt.Errorf("catalog: commit File Provider cutover expiry: %w", err)
		}
		return FileProviderCutover{}, fmt.Errorf("%w: File Provider cutover proof requires fresh enumeration", ErrCutoverProofExpired)
	}
	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `
UPDATE file_provider_cutover
SET state = ?, claimed_unix_nano = ?
WHERE singleton = 1 AND state = ? AND operation_id = ? AND proof_hash = ?`,
		FileProviderCutoverClaimed, now.UnixNano(), FileProviderCutoverProved, operation[:], proofHash[:])
	if err != nil {
		return FileProviderCutover{}, mapConstraint(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return FileProviderCutover{}, fmt.Errorf("%w: File Provider cutover proof raced another claim", ErrConflict)
	}
	if err := tx.Commit(); err != nil {
		return FileProviderCutover{}, fmt.Errorf("catalog: commit File Provider cutover claim: %w", err)
	}
	mode.State = FileProviderCutoverClaimed
	mode.ClaimedAt = now
	return mode, nil
}

// ExpireFileProviderCutoverProof atomically invalidates an unclaimed proof
// whose boot changed or whose kernel-monotonic claim deadline elapsed.
func (c *Catalog) ExpireFileProviderCutoverProof(
	ctx context.Context,
	boot string,
	uptime time.Duration,
	ttl time.Duration,
) (bool, error) {
	if boot == "" || uptime < 0 || ttl <= 0 {
		return false, fmt.Errorf("%w: File Provider cutover expiry identity is incomplete", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("catalog: begin File Provider cutover expiry: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	mode, found, err := fileProviderCutover(ctx, tx)
	if err != nil || !found || mode.State != FileProviderCutoverProved || !cutoverProofExpired(mode, boot, uptime, ttl) {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `
UPDATE file_provider_cutover SET state = ?
WHERE singleton = 1 AND state = ? AND operation_id = ? AND proof_hash = ?`,
		FileProviderCutoverExpired, FileProviderCutoverProved, mode.OperationID[:], mode.ProofHash[:])
	if err != nil {
		return false, mapConstraint(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return false, fmt.Errorf("%w: File Provider cutover expiry raced", ErrConflict)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("catalog: commit File Provider cutover expiry: %w", err)
	}
	return true, nil
}

func cutoverProofExpired(mode FileProviderCutover, boot string, uptime, ttl time.Duration) bool {
	if mode.ProofBoot != boot || uptime < mode.ProofUptime {
		return true
	}
	return uptime-mode.ProofUptime >= ttl
}

// RecoverClaimedFileProviderCutoverProof reads an already-committed claim
// without creating or re-claiming it.
func (c *Catalog) RecoverClaimedFileProviderCutoverProof(
	ctx context.Context,
	operation MutationID,
	proofHash [32]byte,
) (FileProviderCutover, error) {
	mode, found, err := c.FileProviderCutoverMode(ctx)
	if err != nil {
		return FileProviderCutover{}, err
	}
	if !found || mode.State != FileProviderCutoverClaimed || mode.OperationID != operation ||
		mode.ProofHash != proofHash || len(mode.ProofJSON) == 0 {
		return FileProviderCutover{}, fmt.Errorf("%w: claimed File Provider cutover proof does not match", ErrInvalidTransition)
	}
	return mode, nil
}

// RecoverClaimedFileProviderCutoverByPlan reads the singleton terminal claim
// by its canonical account-set hash when the caller has no local receipt.
func (c *Catalog) RecoverClaimedFileProviderCutoverByPlan(
	ctx context.Context,
	planHash [32]byte,
) (FileProviderCutover, error) {
	if planHash == ([32]byte{}) {
		return FileProviderCutover{}, fmt.Errorf("%w: File Provider cutover plan hash is empty", ErrInvalidObject)
	}
	mode, found, err := c.FileProviderCutoverMode(ctx)
	if err != nil {
		return FileProviderCutover{}, err
	}
	if !found {
		return FileProviderCutover{}, fmt.Errorf("%w: no File Provider cutover receipt exists", ErrNotFound)
	}
	if mode.PlanHash != planHash {
		return FileProviderCutover{}, fmt.Errorf("%w: claimed File Provider cutover account set does not match", ErrConflict)
	}
	if mode.State != FileProviderCutoverClaimed {
		return FileProviderCutover{}, fmt.Errorf("%w: File Provider cutover receipt is not terminal", ErrNotFound)
	}
	if len(mode.ProofJSON) == 0 {
		return FileProviderCutover{}, fmt.Errorf("%w: claimed File Provider cutover proof is missing", ErrIntegrity)
	}
	return mode, nil
}

// FileProviderCutoverMode returns the active durable one-shot fence.
func (c *Catalog) FileProviderCutoverMode(ctx context.Context) (FileProviderCutover, bool, error) {
	return fileProviderCutover(ctx, c.readDB)
}

// FileProviderLease is one expiring demand claim for an exact domain generation.
type FileProviderLease struct {
	ID         string
	Tenant     TenantID
	DomainID   causal.DomainID
	Generation Generation
	ExpiresAt  time.Time
}

// FileProviderSignalTarget is one generic File Provider enumerator invalidation.
type FileProviderSignalTarget struct {
	WorkingSet bool
	Parent     ObjectID
}

// FileProviderDomains returns every desired File Provider domain in tenant order.
func (c *Catalog) FileProviderDomains(ctx context.Context) ([]FileProviderDomain, error) {
	rows, err := c.readDB.QueryContext(ctx, `
SELECT d.owner_id, d.tenant, d.generation, t.root_id,
       d.file_provider_account_id, d.file_provider_display_name,
       COALESCE(f.domain_id, ''), COALESCE(f.public_path, ''), COALESCE(f.registered, 0),
       COALESCE(f.owner_id, ''), COALESCE(f.generation, 0), COALESCE(f.root_id, X''),
       COALESCE(f.account_instance_id, ''), COALESCE(f.display_name, '')
FROM desired_tenants d
JOIN tenants t ON t.tenant = d.tenant
LEFT JOIN file_provider_domains f ON f.tenant = d.tenant
LEFT JOIN file_provider_domain_removals r ON r.tenant = d.tenant
WHERE d.file_provider_account_id <> '' AND r.tenant IS NULL
ORDER BY d.tenant`)
	if err != nil {
		return nil, fmt.Errorf("catalog: query File Provider domains: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var domains []FileProviderDomain
	for rows.Next() {
		var desired FileProviderDomain
		var rawRoot, actualRoot []byte
		var actualDomain, actualOwner, actualAccount, actualDisplay string
		var desiredGeneration, actualGeneration uint64
		var registered bool
		if err := rows.Scan(
			&desired.OwnerID, &desired.Tenant, &desiredGeneration, &rawRoot,
			&desired.AccountInstance, &desired.DisplayName,
			&actualDomain, &desired.PublicPath, &registered,
			&actualOwner, &actualGeneration, &actualRoot, &actualAccount, &actualDisplay,
		); err != nil {
			return nil, fmt.Errorf("catalog: scan File Provider domain: %w", err)
		}
		desired.Generation = Generation(desiredGeneration)
		root, err := objectID(rawRoot)
		if err != nil {
			return nil, err
		}
		desired.Root = root
		derived, err := causal.DeriveDomainID(desired.OwnerID, desired.AccountInstance)
		if err != nil {
			return nil, fmt.Errorf("catalog: derive File Provider domain: %w", err)
		}
		desired.DomainID = derived
		if registered {
			actualRootID, rootErr := objectID(actualRoot)
			desired.Registered = rootErr == nil && actualDomain == string(derived) && actualOwner == desired.OwnerID &&
				Generation(actualGeneration) == desired.Generation && actualRootID == desired.Root &&
				actualAccount == desired.AccountInstance && actualDisplay == desired.DisplayName && exactAbsolutePath(desired.PublicPath)
		}
		domains = append(domains, desired)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: read File Provider domains: %w", err)
	}
	return domains, nil
}

// BeginFileProviderDomainRemoval durably fences one exact tenant domain before
// any external removal is attempted. Replays return the same intent.
func (c *Catalog) BeginFileProviderDomainRemoval(
	ctx context.Context,
	owner string,
	tenant TenantID,
	generation Generation,
) (FileProviderDomainRemoval, error) {
	if owner == "" || tenant == "" || generation == 0 {
		return FileProviderDomainRemoval{}, fmt.Errorf("%w: domain removal identity is incomplete", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return FileProviderDomainRemoval{}, fmt.Errorf("catalog: begin File Provider domain removal: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	existing, found, err := fileProviderDomainRemoval(ctx, tx, tenant)
	if err != nil {
		return FileProviderDomainRemoval{}, err
	}
	if found {
		if existing.Domain.OwnerID != owner {
			return FileProviderDomainRemoval{}, ErrTenantOwnerMismatch
		}
		if existing.Domain.Generation != generation {
			return FileProviderDomainRemoval{}, ErrGenerationMismatch
		}
		if err := tx.Commit(); err != nil {
			return FileProviderDomainRemoval{}, fmt.Errorf("catalog: finish File Provider domain removal replay: %w", err)
		}
		return existing, nil
	}
	provision, found, err := tenantProvision(ctx, tx, tenant)
	if err != nil {
		return FileProviderDomainRemoval{}, err
	}
	if !found {
		return FileProviderDomainRemoval{}, ErrNotFound
	}
	if provision.OwnerID != owner {
		return FileProviderDomainRemoval{}, ErrTenantOwnerMismatch
	}
	if provision.Generation != generation {
		return FileProviderDomainRemoval{}, ErrGenerationMismatch
	}
	if !provision.FileProvider.Enabled() {
		return FileProviderDomainRemoval{}, fmt.Errorf("%w: tenant has no File Provider domain", ErrInvalidTransition)
	}
	domain, err := domainFromProvision(provision)
	if err != nil {
		return FileProviderDomainRemoval{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO file_provider_domain_removals(
    domain_id, tenant, owner_id, generation, root_id, account_instance_id,
    display_name, confirmed_absent
) VALUES (?, ?, ?, ?, ?, ?, ?, 0)`,
		string(domain.DomainID), string(domain.Tenant), domain.OwnerID, uint64(domain.Generation),
		domain.Root[:], domain.AccountInstance, domain.DisplayName); err != nil {
		return FileProviderDomainRemoval{}, mapConstraint(err)
	}
	if err := tx.Commit(); err != nil {
		return FileProviderDomainRemoval{}, fmt.Errorf("catalog: commit File Provider domain removal: %w", err)
	}
	return FileProviderDomainRemoval{Domain: domain}, nil
}

// FileProviderDomainRemovalState returns one exact durable retirement intent.
func (c *Catalog) FileProviderDomainRemovalState(
	ctx context.Context,
	owner string,
	tenant TenantID,
	generation Generation,
) (FileProviderDomainRemoval, error) {
	removal, found, err := fileProviderDomainRemoval(ctx, c.readDB, tenant)
	if err != nil {
		return FileProviderDomainRemoval{}, err
	}
	if !found {
		return FileProviderDomainRemoval{}, ErrNotFound
	}
	if removal.Domain.OwnerID != owner {
		return FileProviderDomainRemoval{}, ErrTenantOwnerMismatch
	}
	if removal.Domain.Generation != generation {
		return FileProviderDomainRemoval{}, ErrGenerationMismatch
	}
	return removal, nil
}

// FileProviderDomainRemovals returns every durable retirement intent.
func (c *Catalog) FileProviderDomainRemovals(ctx context.Context) ([]FileProviderDomainRemoval, error) {
	rows, err := c.readDB.QueryContext(ctx, `
SELECT domain_id, tenant, owner_id, generation, root_id, account_instance_id,
       display_name, confirmed_absent
FROM file_provider_domain_removals ORDER BY tenant`)
	if err != nil {
		return nil, fmt.Errorf("catalog: list File Provider domain removals: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var removals []FileProviderDomainRemoval
	for rows.Next() {
		removal, err := scanFileProviderDomainRemoval(rows)
		if err != nil {
			return nil, err
		}
		removals = append(removals, removal)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: read File Provider domain removals: %w", err)
	}
	return removals, nil
}

// ConfirmFileProviderDomainRemoval records one exact broker-proven absence.
func (c *Catalog) ConfirmFileProviderDomainRemoval(ctx context.Context, removal FileProviderDomainRemoval) error {
	if err := validateFileProviderDomainRemoval(removal); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin exact File Provider domain retirement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, found, err := fileProviderDomainRemoval(ctx, tx, removal.Domain.Tenant)
	if err != nil {
		return err
	}
	if !found || !equalFileProviderDomainIdentity(current.Domain, removal.Domain) {
		return fmt.Errorf("%w: File Provider domain removal identity changed", ErrInvalidTransition)
	}
	var actual, exact int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(SUM(CASE WHEN domain_id = ? AND tenant = ? AND owner_id = ?
    AND generation = ? AND root_id = ? AND account_instance_id = ? AND display_name = ? THEN 1 ELSE 0 END), 0)
FROM file_provider_domains WHERE domain_id = ? OR tenant = ?`,
		string(removal.Domain.DomainID), string(removal.Domain.Tenant), removal.Domain.OwnerID,
		uint64(removal.Domain.Generation), removal.Domain.Root[:], removal.Domain.AccountInstance, removal.Domain.DisplayName,
		string(removal.Domain.DomainID), string(removal.Domain.Tenant)).Scan(&actual, &exact); err != nil {
		return fmt.Errorf("catalog: inspect exact File Provider domain retirement: %w", err)
	}
	if actual != exact {
		return fmt.Errorf("%w: registered File Provider domain identity changed", ErrInvalidTransition)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_provider_leases WHERE domain_id = ?`, string(removal.Domain.DomainID)); err != nil {
		return fmt.Errorf("catalog: retire File Provider leases: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_provider_domains WHERE domain_id = ?`, string(removal.Domain.DomainID)); err != nil {
		return fmt.Errorf("catalog: retire File Provider domain: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE file_provider_domain_removals SET confirmed_absent = 1
WHERE domain_id = ? AND tenant = ? AND owner_id = ? AND generation = ?`,
		string(removal.Domain.DomainID), string(removal.Domain.Tenant), removal.Domain.OwnerID, uint64(removal.Domain.Generation))
	if err != nil {
		return fmt.Errorf("catalog: confirm File Provider domain absence: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return fmt.Errorf("%w: File Provider domain removal identity changed", ErrInvalidTransition)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit exact File Provider domain retirement: %w", err)
	}
	return nil
}

// ConfirmFileProviderDomain records one exact domain result returned by the signed broker.
func (c *Catalog) ConfirmFileProviderDomain(ctx context.Context, domain FileProviderDomain) error {
	if !domain.Registered || !exactAbsolutePath(domain.PublicPath) {
		return fmt.Errorf("%w: confirmed File Provider domain is incomplete", ErrInvalidObject)
	}
	desired, err := c.desiredFileProviderDomain(ctx, domain.Tenant)
	if err != nil {
		return err
	}
	if !equalFileProviderDomainIdentity(desired, domain) {
		return fmt.Errorf("%w: confirmed File Provider domain does not match desired identity", ErrInvalidTransition)
	}
	_, err = c.db.ExecContext(ctx, `
INSERT INTO file_provider_domains(
    domain_id, tenant, owner_id, generation, root_id, account_instance_id,
    display_name, public_path, registered
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
ON CONFLICT(tenant) DO UPDATE SET
    domain_id = excluded.domain_id, owner_id = excluded.owner_id,
    generation = excluded.generation, root_id = excluded.root_id,
    account_instance_id = excluded.account_instance_id, display_name = excluded.display_name,
    public_path = excluded.public_path, registered = 1`,
		string(domain.DomainID), string(domain.Tenant), domain.OwnerID, uint64(domain.Generation), domain.Root[:],
		domain.AccountInstance, domain.DisplayName, domain.PublicPath)
	if err != nil {
		return mapConstraint(err)
	}
	return nil
}

// ConfirmFileProviderDomainAbsent forgets one broker-proven absent domain and its leases.
func (c *Catalog) ConfirmFileProviderDomainAbsent(ctx context.Context, domain causal.DomainID) error {
	if domain == "" {
		return fmt.Errorf("%w: File Provider domain id is empty", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin File Provider domain retirement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_provider_leases WHERE domain_id = ?`, string(domain)); err != nil {
		return fmt.Errorf("catalog: retire File Provider leases: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_provider_domains WHERE domain_id = ?`, string(domain)); err != nil {
		return fmt.Errorf("catalog: retire File Provider domain: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit File Provider domain retirement: %w", err)
	}
	return nil
}

// RenewFileProviderLease creates or extends one exact domain-generation demand claim.
func (c *Catalog) RenewFileProviderLease(ctx context.Context, lease FileProviderLease) error {
	if lease.ID == "" || strings.ContainsRune(lease.ID, 0) || lease.Tenant == "" || lease.DomainID == "" ||
		lease.Generation == 0 || lease.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: File Provider lease is incomplete", ErrInvalidObject)
	}
	desired, err := c.desiredFileProviderDomain(ctx, lease.Tenant)
	if err != nil {
		return err
	}
	if !desired.Registered || desired.DomainID != lease.DomainID || desired.Generation != lease.Generation {
		return fmt.Errorf("%w: File Provider lease does not match a registered domain generation", ErrInvalidTransition)
	}
	_, err = c.db.ExecContext(ctx, `
INSERT INTO file_provider_leases(lease_id, tenant, domain_id, generation, expires_unix_nano)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(lease_id) DO UPDATE SET
    tenant = excluded.tenant, domain_id = excluded.domain_id,
    generation = excluded.generation, expires_unix_nano = excluded.expires_unix_nano`,
		lease.ID, string(lease.Tenant), string(lease.DomainID), uint64(lease.Generation), lease.ExpiresAt.UnixNano())
	if err != nil {
		return mapConstraint(err)
	}
	return nil
}

// ReleaseFileProviderLease idempotently retires one demand claim.
func (c *Catalog) ReleaseFileProviderLease(ctx context.Context, id string) error {
	if id == "" || strings.ContainsRune(id, 0) {
		return fmt.Errorf("%w: File Provider lease id is invalid", ErrInvalidObject)
	}
	if _, err := c.db.ExecContext(ctx, `DELETE FROM file_provider_leases WHERE lease_id = ?`, id); err != nil {
		return fmt.Errorf("catalog: release File Provider lease: %w", err)
	}
	return nil
}

// FileProviderDemand returns live lease and materialization-interest counts for one domain generation.
func (c *Catalog) FileProviderDemand(
	ctx context.Context,
	tenant TenantID,
	domain causal.DomainID,
	generation Generation,
	now time.Time,
) (uint32, uint32, error) {
	if tenant == "" || domain == "" || generation == 0 || now.IsZero() {
		return 0, 0, fmt.Errorf("%w: File Provider demand identity is incomplete", ErrInvalidObject)
	}
	var leases, interests uint64
	if err := c.readDB.QueryRowContext(ctx, `
SELECT
    (SELECT COUNT(*) FROM file_provider_leases
     WHERE tenant = ? AND domain_id = ? AND generation = ? AND expires_unix_nano > ?),
    (SELECT COUNT(*) FROM materialization_interests
     WHERE tenant = ? AND owner_presentation = ? AND owner_domain = ? AND owner_generation = ?
       AND removed_revision IS NULL)`,
		string(tenant), string(domain), uint64(generation), now.UnixNano(),
		string(tenant), uint8(PresentationFileProvider), string(domain), uint64(generation)).Scan(&leases, &interests); err != nil {
		return 0, 0, fmt.Errorf("catalog: inspect File Provider demand: %w", err)
	}
	if leases > uint64(^uint32(0)) || interests > uint64(^uint32(0)) {
		return 0, 0, fmt.Errorf("%w: File Provider demand count overflow", ErrIntegrity)
	}
	return uint32(leases), uint32(interests), nil
}

// FileProviderSignalTargets returns the exact coalesced invalidations for one catalog revision.
func (c *Catalog) FileProviderSignalTargets(
	ctx context.Context,
	tenant TenantID,
	domain causal.DomainID,
	generation Generation,
	revision Revision,
) ([]FileProviderSignalTarget, error) {
	if tenant == "" || domain == "" || generation == 0 || revision == 0 {
		return nil, fmt.Errorf("%w: File Provider signal identity is incomplete", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT DISTINCT scope_kind, scope_parent
FROM changes
WHERE tenant = ? AND revision = ? AND presentation = ?
  AND ((scope_kind = ? AND scope_domain = ? AND scope_generation = ?)
    OR (scope_kind = ? AND scope_domain = '' AND scope_generation = 0))
ORDER BY scope_kind, scope_parent`, string(tenant), uint64(revision), uint8(PresentationFileProvider),
		uint8(EnumerationWorkingSet), string(domain), uint64(generation), uint8(EnumerationContainer))
	if err != nil {
		return nil, fmt.Errorf("catalog: query File Provider signal targets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var targets []FileProviderSignalTarget
	for rows.Next() {
		var kind uint8
		var raw []byte
		if err := rows.Scan(&kind, &raw); err != nil {
			return nil, fmt.Errorf("catalog: scan File Provider signal target: %w", err)
		}
		switch EnumerationScopeKind(kind) {
		case EnumerationWorkingSet:
			targets = append(targets, FileProviderSignalTarget{WorkingSet: true})
		case EnumerationContainer:
			parent, err := objectID(raw)
			if err != nil {
				return nil, err
			}
			targets = append(targets, FileProviderSignalTarget{Parent: parent})
		default:
			return nil, fmt.Errorf("%w: unknown File Provider signal target kind", ErrIntegrity)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: read File Provider signal targets: %w", err)
	}
	if len(targets) == 0 {
		targets = append(targets, FileProviderSignalTarget{WorkingSet: true})
	}
	return targets, nil
}

// NextBrokerCommandID durably allocates a process-independent broker sequence value.
func (c *Catalog) NextBrokerCommandID(ctx context.Context) (uint64, error) {
	var id uint64
	if err := c.db.QueryRowContext(ctx, `
UPDATE broker_sequence SET last_command_id = last_command_id + 1
WHERE singleton = 1 AND last_command_id < 9223372036854775807
RETURNING last_command_id`).Scan(&id); err != nil {
		return 0, fmt.Errorf("catalog: allocate broker command id: %w", err)
	}
	if id == 0 {
		return 0, fmt.Errorf("%w: broker command id wrapped", ErrIntegrity)
	}
	return id, nil
}

func (c *Catalog) desiredFileProviderDomain(ctx context.Context, tenant TenantID) (FileProviderDomain, error) {
	domains, err := c.FileProviderDomains(ctx)
	if err != nil {
		return FileProviderDomain{}, err
	}
	for _, domain := range domains {
		if domain.Tenant == tenant {
			return domain, nil
		}
	}
	return FileProviderDomain{}, ErrNotFound
}

func domainFromProvision(provision TenantProvision) (FileProviderDomain, error) {
	domainID, err := causal.DeriveDomainID(provision.OwnerID, provision.FileProvider.AccountInstanceID)
	if err != nil {
		return FileProviderDomain{}, fmt.Errorf("catalog: derive File Provider domain: %w", err)
	}
	return FileProviderDomain{
		DomainID: domainID, OwnerID: provision.OwnerID, Tenant: provision.Tenant,
		Generation: provision.Generation, Root: provision.Root,
		AccountInstance: provision.FileProvider.AccountInstanceID,
		DisplayName:     provision.FileProvider.DisplayName,
	}, nil
}

func fileProviderCutover(
	ctx context.Context,
	query domainRemovalQuery,
) (FileProviderCutover, bool, error) {
	var mode FileProviderCutover
	var operation, planHash, proofHash, proofJSON []byte
	var started, proofUptime, claimed int64
	err := query.QueryRowContext(ctx, `
SELECT operation_id, plan_hash, started_unix_nano, state, proof_hash, proof_json,
       proof_boot, proof_uptime_nano, claimed_unix_nano
FROM file_provider_cutover WHERE singleton = 1`).Scan(
		&operation, &planHash, &started, &mode.State, &proofHash, &proofJSON,
		&mode.ProofBoot, &proofUptime, &claimed,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return FileProviderCutover{}, false, nil
	}
	if err != nil {
		return FileProviderCutover{}, false, fmt.Errorf("catalog: read File Provider cutover: %w", err)
	}
	if len(operation) != len(mode.OperationID) || len(planHash) != len(mode.PlanHash) || started <= 0 ||
		mode.State < FileProviderCutoverFenced || mode.State > FileProviderCutoverExpired || proofUptime < 0 ||
		(mode.State == FileProviderCutoverFenced && (len(proofHash) != 0 || len(proofJSON) != 0 || mode.ProofBoot != "" || proofUptime != 0 || claimed != 0)) ||
		(mode.State == FileProviderCutoverProved && (len(proofHash) != len(mode.ProofHash) || len(proofJSON) == 0 || mode.ProofBoot == "" || claimed != 0)) ||
		(mode.State == FileProviderCutoverClaimed && (len(proofHash) != len(mode.ProofHash) || len(proofJSON) == 0 || mode.ProofBoot == "" || claimed <= 0)) ||
		(mode.State == FileProviderCutoverExpired && (len(proofHash) != len(mode.ProofHash) || len(proofJSON) == 0 || mode.ProofBoot == "" || claimed != 0)) {
		return FileProviderCutover{}, false, fmt.Errorf("%w: corrupt File Provider cutover fence", ErrIntegrity)
	}
	copy(mode.OperationID[:], operation)
	copy(mode.PlanHash[:], planHash)
	copy(mode.ProofHash[:], proofHash)
	mode.ProofJSON = append([]byte(nil), proofJSON...)
	mode.ProofUptime = time.Duration(proofUptime)
	mode.StartedAt = time.Unix(0, started).UTC()
	if claimed > 0 {
		mode.ClaimedAt = time.Unix(0, claimed).UTC()
	}
	return mode, true, nil
}

type domainRemovalQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func fileProviderDomainRemoval(
	ctx context.Context,
	query domainRemovalQuery,
	tenant TenantID,
) (FileProviderDomainRemoval, bool, error) {
	row := query.QueryRowContext(ctx, `
SELECT domain_id, tenant, owner_id, generation, root_id, account_instance_id,
       display_name, confirmed_absent
FROM file_provider_domain_removals WHERE tenant = ?`, string(tenant))
	removal, err := scanFileProviderDomainRemoval(row)
	if errors.Is(err, sql.ErrNoRows) {
		return FileProviderDomainRemoval{}, false, nil
	}
	if err != nil {
		return FileProviderDomainRemoval{}, false, err
	}
	return removal, true, nil
}

func scanFileProviderDomainRemoval(scanner provisionScanner) (FileProviderDomainRemoval, error) {
	var removal FileProviderDomainRemoval
	var domainID, tenant string
	var generation uint64
	var root []byte
	if err := scanner.Scan(
		&domainID, &tenant, &removal.Domain.OwnerID, &generation, &root,
		&removal.Domain.AccountInstance, &removal.Domain.DisplayName, &removal.ConfirmedAbsent,
	); err != nil {
		return FileProviderDomainRemoval{}, err
	}
	parsedRoot, err := objectID(root)
	if err != nil {
		return FileProviderDomainRemoval{}, err
	}
	removal.Domain.DomainID = causal.DomainID(domainID)
	removal.Domain.Tenant = TenantID(tenant)
	removal.Domain.Generation = Generation(generation)
	removal.Domain.Root = parsedRoot
	if err := validateFileProviderDomainRemoval(removal); err != nil {
		return FileProviderDomainRemoval{}, fmt.Errorf("catalog: corrupt File Provider domain removal: %w", err)
	}
	return removal, nil
}

func validateFileProviderDomainRemoval(removal FileProviderDomainRemoval) error {
	domain := removal.Domain
	if domain.DomainID == "" || domain.OwnerID == "" || domain.Tenant == "" || domain.Generation == 0 ||
		domain.Root == (ObjectID{}) || domain.AccountInstance == "" || domain.DisplayName == "" {
		return fmt.Errorf("%w: File Provider domain removal identity is incomplete", ErrInvalidObject)
	}
	derived, err := causal.DeriveDomainID(domain.OwnerID, domain.AccountInstance)
	if err != nil {
		return fmt.Errorf("%w: derive File Provider domain removal identity: %v", ErrInvalidObject, err)
	}
	if derived != domain.DomainID {
		return fmt.Errorf("%w: File Provider domain removal id is not derived from owner and account", ErrInvalidObject)
	}
	return nil
}

func equalFileProviderDomainIdentity(left, right FileProviderDomain) bool {
	return left.DomainID == right.DomainID && left.OwnerID == right.OwnerID && left.Tenant == right.Tenant &&
		left.Generation == right.Generation && left.Root == right.Root && left.AccountInstance == right.AccountInstance &&
		left.DisplayName == right.DisplayName
}
