package catalog

import (
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
	DomainID             causal.DomainID
	OwnerID              string
	Tenant               TenantID
	Generation           Generation
	ActivationGeneration string
	Root                 ObjectID
	Access               TenantAccessMode
	PresentationInstance string
	DisplayName          string
	PublicPath           string
	Registered           bool
}

// FileProviderDomainRemoval is one durable exact-domain retirement intent.
type FileProviderDomainRemoval struct {
	Domain          FileProviderDomain
	ConfirmedAbsent bool
}

const (
	// FileProviderDomainPageLimit is the hard maximum domain-state page size.
	FileProviderDomainPageLimit = 256
	// FileProviderDomainRecordMaxBytes bounds one domain-state wire record.
	FileProviderDomainRecordMaxBytes = 4 << 10
	// FileProviderDomainPageMaxBytes bounds raw record bytes before wire encoding.
	FileProviderDomainPageMaxBytes = 256 << 10
)

// FileProviderDomainPage is one bounded desired-domain page.
type FileProviderDomainPage struct {
	Domains []FileProviderDomain
	Next    TenantID
}

// FileProviderDomainRemovalPage is one bounded retirement-intent page.
type FileProviderDomainRemovalPage struct {
	Removals []FileProviderDomainRemoval
	Next     TenantID
}

// Validate rejects malformed or oversized desired-domain pages.
func (p FileProviderDomainPage) Validate(after TenantID, limit int) error {
	if limit < 1 || limit > FileProviderDomainPageLimit || len(p.Domains) > limit {
		return fmt.Errorf("%w: invalid File Provider domain page limit", ErrInvalidObject)
	}
	rawBytes := 0
	previous := after
	for _, domain := range p.Domains {
		if domain.Tenant <= previous {
			return fmt.Errorf("%w: File Provider domain page is not strictly ordered", ErrIntegrity)
		}
		if err := validateFileProviderDomainRecord(domain); err != nil {
			return err
		}
		rawBytes += fileProviderDomainRecordBytes(domain)
		if rawBytes > FileProviderDomainPageMaxBytes {
			return fmt.Errorf("%w: File Provider domain page exceeds raw byte limit", ErrInvalidObject)
		}
		previous = domain.Tenant
	}
	if p.Next != "" && (len(p.Domains) == 0 || p.Next != previous) {
		return fmt.Errorf("%w: File Provider domain page cursor is invalid", ErrIntegrity)
	}
	return nil
}

// Validate rejects malformed or oversized domain-removal pages.
func (p FileProviderDomainRemovalPage) Validate(after TenantID, limit int) error {
	if limit < 1 || limit > FileProviderDomainPageLimit || len(p.Removals) > limit {
		return fmt.Errorf("%w: invalid File Provider removal page limit", ErrInvalidObject)
	}
	rawBytes := 0
	previous := after
	for _, removal := range p.Removals {
		if removal.Domain.Tenant <= previous {
			return fmt.Errorf("%w: File Provider removal page is not strictly ordered", ErrIntegrity)
		}
		if err := validateFileProviderDomainRemoval(removal); err != nil {
			return err
		}
		rawBytes += fileProviderDomainRecordBytes(removal.Domain)
		if rawBytes > FileProviderDomainPageMaxBytes {
			return fmt.Errorf("%w: File Provider removal page exceeds raw byte limit", ErrInvalidObject)
		}
		previous = removal.Domain.Tenant
	}
	if p.Next != "" && (len(p.Removals) == 0 || p.Next != previous) {
		return fmt.Errorf("%w: File Provider removal page cursor is invalid", ErrIntegrity)
	}
	return nil
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

// MaxFileProviderSignalTargets bounds one broker command independently of tree size.
const MaxFileProviderSignalTargets = 64

// FileProviderSignalPlan is one bounded invalidation plus a digest of its exact target set.
type FileProviderSignalPlan struct {
	Targets     []FileProviderSignalTarget
	ExactCount  uint64
	ExactDigest [32]byte
	Coalesced   bool
}

// PageFileProviderDomains returns desired domains after one exclusive tenant cursor.
func (c *Catalog) PageFileProviderDomains(
	ctx context.Context,
	after TenantID,
	limit int,
) (FileProviderDomainPage, error) {
	if limit < 1 || limit > FileProviderDomainPageLimit {
		return FileProviderDomainPage{}, fmt.Errorf("%w: invalid File Provider domain page limit", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT generation.owner_id, intent.tenant_id, intent.target_generation, t.root_id, generation.access_mode,
       generation.file_provider_presentation_instance_id, generation.file_provider_display_name,
       COALESCE(f.domain_id, ''), COALESCE(f.public_path, ''), COALESCE(f.registered, 0),
       COALESCE(f.owner_id, ''), COALESCE(f.generation, 0), COALESCE(f.root_id, X''),
       COALESCE(f.access_mode, 0), COALESCE(f.presentation_instance_id, ''), COALESCE(f.display_name, ''),
       COALESCE(f.activation_generation, '')
FROM tenant_intents intent
JOIN tenant_generations generation
  ON generation.tenant_id = intent.tenant_id AND generation.generation = intent.target_generation
JOIN tenants t ON t.tenant = intent.tenant_id
LEFT JOIN file_provider_domains f ON f.tenant = intent.tenant_id
LEFT JOIN file_provider_domain_removals r ON r.tenant = intent.tenant_id
WHERE intent.state = ? AND generation.file_provider_presentation_instance_id <> ''
  AND r.tenant IS NULL AND intent.tenant_id > ?
ORDER BY intent.tenant_id LIMIT ?`, uint8(TenantIntentPresent), string(after), limit+1)
	if err != nil {
		return FileProviderDomainPage{}, fmt.Errorf("catalog: page File Provider domains: %w", err)
	}
	defer func() { _ = rows.Close() }()
	page := FileProviderDomainPage{Domains: make([]FileProviderDomain, 0, limit)}
	rawBytes := 0
	for rows.Next() {
		desired, err := scanDesiredFileProviderDomain(rows)
		if err != nil {
			return FileProviderDomainPage{}, fmt.Errorf("catalog: scan File Provider domain: %w", err)
		}
		if len(page.Domains) == limit {
			page.Next = page.Domains[len(page.Domains)-1].Tenant
			break
		}
		recordBytes := fileProviderDomainRecordBytes(desired)
		if len(page.Domains) > 0 && rawBytes+recordBytes > FileProviderDomainPageMaxBytes {
			page.Next = page.Domains[len(page.Domains)-1].Tenant
			break
		}
		page.Domains = append(page.Domains, desired)
		rawBytes += recordBytes
	}
	if err := rows.Err(); err != nil {
		return FileProviderDomainPage{}, fmt.Errorf("catalog: read File Provider domains: %w", err)
	}
	if err := page.Validate(after, limit); err != nil {
		return FileProviderDomainPage{}, err
	}
	return page, nil
}

// FileProviderDomainForTenant returns one keyed desired domain without scanning the fleet.
func (c *Catalog) FileProviderDomainForTenant(
	ctx context.Context,
	tenant TenantID,
) (FileProviderDomain, bool, error) {
	if tenant == "" {
		return FileProviderDomain{}, false, fmt.Errorf("%w: tenant is empty", ErrInvalidObject)
	}
	return fileProviderDomainForTenant(ctx, c.readDB, tenant)
}

func fileProviderDomainForTenant(
	ctx context.Context,
	query domainRemovalQuery,
	tenant TenantID,
) (FileProviderDomain, bool, error) {
	row := query.QueryRowContext(ctx, `
SELECT generation.owner_id, intent.tenant_id, intent.target_generation, t.root_id, generation.access_mode,
       generation.file_provider_presentation_instance_id, generation.file_provider_display_name,
       COALESCE(f.domain_id, ''), COALESCE(f.public_path, ''), COALESCE(f.registered, 0),
       COALESCE(f.owner_id, ''), COALESCE(f.generation, 0), COALESCE(f.root_id, X''),
       COALESCE(f.access_mode, 0), COALESCE(f.presentation_instance_id, ''), COALESCE(f.display_name, ''),
       COALESCE(f.activation_generation, '')
FROM tenant_intents intent
JOIN tenant_generations generation
  ON generation.tenant_id = intent.tenant_id AND generation.generation = intent.target_generation
JOIN tenants t ON t.tenant = intent.tenant_id
LEFT JOIN file_provider_domains f ON f.tenant = intent.tenant_id
LEFT JOIN file_provider_domain_removals r ON r.tenant = intent.tenant_id
WHERE intent.tenant_id = ? AND intent.state = ?
  AND generation.file_provider_presentation_instance_id <> '' AND r.tenant IS NULL`,
		string(tenant), uint8(TenantIntentPresent))
	domain, err := scanDesiredFileProviderDomain(row)
	if errors.Is(err, sql.ErrNoRows) {
		return FileProviderDomain{}, false, nil
	}
	if err != nil {
		return FileProviderDomain{}, false, fmt.Errorf("catalog: scan keyed File Provider domain: %w", err)
	}
	return domain, true, nil
}

func scanDesiredFileProviderDomain(scanner provisionScanner) (FileProviderDomain, error) {
	var desired FileProviderDomain
	var rawRoot, actualRoot []byte
	var actualDomain, actualOwner, actualPresentation, actualDisplay, actualActivation string
	var desiredGeneration, actualGeneration uint64
	var desiredAccess, actualAccess uint8
	var registered bool
	if err := scanner.Scan(
		&desired.OwnerID, &desired.Tenant, &desiredGeneration, &rawRoot, &desiredAccess,
		&desired.PresentationInstance, &desired.DisplayName,
		&actualDomain, &desired.PublicPath, &registered,
		&actualOwner, &actualGeneration, &actualRoot, &actualAccess, &actualPresentation, &actualDisplay,
		&actualActivation,
	); err != nil {
		return FileProviderDomain{}, err
	}
	desired.Generation = Generation(desiredGeneration)
	desired.Access = TenantAccessMode(desiredAccess)
	if desired.Access != TenantReadOnly && desired.Access != TenantReadWrite {
		return FileProviderDomain{}, fmt.Errorf("%w: invalid File Provider access mode", ErrIntegrity)
	}
	root, err := objectID(rawRoot)
	if err != nil {
		return FileProviderDomain{}, err
	}
	desired.Root = root
	derived, err := causal.DeriveDomainID(desired.OwnerID, desired.PresentationInstance)
	if err != nil {
		return FileProviderDomain{}, fmt.Errorf("catalog: derive File Provider domain: %w", err)
	}
	desired.DomainID = derived
	if registered {
		actualRootID, rootErr := objectID(actualRoot)
		desired.Registered = rootErr == nil && actualDomain == string(derived) && actualOwner == desired.OwnerID &&
			Generation(actualGeneration) == desired.Generation && actualRootID == desired.Root &&
			TenantAccessMode(actualAccess) == desired.Access &&
			actualPresentation == desired.PresentationInstance && actualDisplay == desired.DisplayName &&
			exactAbsolutePath(desired.PublicPath) && actualActivation != "" && !strings.ContainsRune(actualActivation, 0)
		if desired.Registered {
			desired.ActivationGeneration = actualActivation
		}
	}
	if err := validateFileProviderDomainRecord(desired); err != nil {
		return FileProviderDomain{}, fmt.Errorf("catalog: corrupt File Provider domain: %w", err)
	}
	return desired, nil
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
	provision, found, err := appliedTenantProvision(ctx, tx, tenant)
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
    domain_id, tenant, owner_id, generation, root_id, access_mode,
    presentation_instance_id, display_name, confirmed_absent
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)`,
		string(domain.DomainID), string(domain.Tenant), domain.OwnerID, uint64(domain.Generation),
		domain.Root[:], uint8(domain.Access), domain.PresentationInstance, domain.DisplayName); err != nil {
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

// PageFileProviderDomainRemovals returns retirement intents after one exclusive tenant cursor.
func (c *Catalog) PageFileProviderDomainRemovals(
	ctx context.Context,
	after TenantID,
	limit int,
) (FileProviderDomainRemovalPage, error) {
	if limit < 1 || limit > FileProviderDomainPageLimit {
		return FileProviderDomainRemovalPage{}, fmt.Errorf("%w: invalid File Provider removal page limit", ErrInvalidObject)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT domain_id, tenant, owner_id, generation, root_id, access_mode,
       presentation_instance_id, display_name, confirmed_absent
FROM file_provider_domain_removals
WHERE tenant > ?
ORDER BY tenant LIMIT ?`, string(after), limit+1)
	if err != nil {
		return FileProviderDomainRemovalPage{}, fmt.Errorf("catalog: page File Provider domain removals: %w", err)
	}
	defer func() { _ = rows.Close() }()
	page := FileProviderDomainRemovalPage{Removals: make([]FileProviderDomainRemoval, 0, limit)}
	rawBytes := 0
	for rows.Next() {
		removal, err := scanFileProviderDomainRemoval(rows)
		if err != nil {
			return FileProviderDomainRemovalPage{}, err
		}
		if len(page.Removals) == limit {
			page.Next = page.Removals[len(page.Removals)-1].Domain.Tenant
			break
		}
		recordBytes := fileProviderDomainRecordBytes(removal.Domain)
		if len(page.Removals) > 0 && rawBytes+recordBytes > FileProviderDomainPageMaxBytes {
			page.Next = page.Removals[len(page.Removals)-1].Domain.Tenant
			break
		}
		page.Removals = append(page.Removals, removal)
		rawBytes += recordBytes
	}
	if err := rows.Err(); err != nil {
		return FileProviderDomainRemovalPage{}, fmt.Errorf("catalog: read File Provider domain removals: %w", err)
	}
	if err := page.Validate(after, limit); err != nil {
		return FileProviderDomainRemovalPage{}, err
	}
	return page, nil
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
    AND generation = ? AND root_id = ? AND access_mode = ?
    AND presentation_instance_id = ? AND display_name = ? THEN 1 ELSE 0 END), 0)
FROM file_provider_domains WHERE domain_id = ? OR tenant = ?`,
		string(removal.Domain.DomainID), string(removal.Domain.Tenant), removal.Domain.OwnerID,
		uint64(removal.Domain.Generation), removal.Domain.Root[:], uint8(removal.Domain.Access),
		removal.Domain.PresentationInstance, removal.Domain.DisplayName,
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
	if !domain.Registered || !exactAbsolutePath(domain.PublicPath) || domain.ActivationGeneration == "" ||
		strings.ContainsRune(domain.ActivationGeneration, 0) {
		return fmt.Errorf("%w: confirmed File Provider domain is incomplete", ErrInvalidObject)
	}
	if err := validateFileProviderDomainRecord(domain); err != nil {
		return err
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
    domain_id, tenant, owner_id, generation, root_id, access_mode,
    presentation_instance_id, display_name, public_path, activation_generation, registered
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
ON CONFLICT(tenant) DO UPDATE SET
    domain_id = excluded.domain_id, owner_id = excluded.owner_id,
    generation = excluded.generation, root_id = excluded.root_id,
    access_mode = excluded.access_mode,
    presentation_instance_id = excluded.presentation_instance_id, display_name = excluded.display_name,
    public_path = excluded.public_path, activation_generation = excluded.activation_generation, registered = 1`,
		string(domain.DomainID), string(domain.Tenant), domain.OwnerID, uint64(domain.Generation), domain.Root[:],
		uint8(domain.Access), domain.PresentationInstance, domain.DisplayName, domain.PublicPath, domain.ActivationGeneration)
	if err != nil {
		return mapConstraint(err)
	}
	return nil
}

// InvalidateFileProviderDomain removes a prior OS observation before a fresh
// activation attempts to prove the exact domain again.
func (c *Catalog) InvalidateFileProviderDomain(
	ctx context.Context,
	tenant TenantID,
	generation Generation,
) error {
	if tenant == "" || generation == 0 {
		return fmt.Errorf("%w: File Provider invalidation identity is incomplete", ErrInvalidObject)
	}
	_, err := c.db.ExecContext(ctx, `
DELETE FROM file_provider_domains WHERE tenant = ? AND generation = ?`, string(tenant), uint64(generation))
	if err != nil {
		return fmt.Errorf("catalog: invalidate File Provider domain: %w", err)
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
		lease.Generation == 0 || lease.ExpiresAt.IsZero() || lease.ExpiresAt.UnixNano() <= 0 {
		return fmt.Errorf("%w: File Provider lease is incomplete", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin File Provider lease renewal: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	desired, found, err := fileProviderDomainForTenant(ctx, tx, lease.Tenant)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	if !desired.Registered || desired.DomainID != lease.DomainID || desired.Generation != lease.Generation {
		return fmt.Errorf("%w: File Provider lease does not match a registered domain generation", ErrInvalidTransition)
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO file_provider_leases(lease_id, tenant, domain_id, generation, expires_unix_nano)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(lease_id) DO UPDATE SET
    expires_unix_nano = excluded.expires_unix_nano
WHERE file_provider_leases.tenant = excluded.tenant
  AND file_provider_leases.domain_id = excluded.domain_id
  AND file_provider_leases.generation = excluded.generation
  AND excluded.expires_unix_nano >= file_provider_leases.expires_unix_nano`,
		lease.ID, string(lease.Tenant), string(lease.DomainID), uint64(lease.Generation), lease.ExpiresAt.UnixNano())
	if err != nil {
		return mapConstraint(err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect File Provider lease renewal: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("%w: File Provider lease identity or expiry regressed", ErrInvalidTransition)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit File Provider lease renewal: %w", err)
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
	domain, found, err := c.FileProviderDomainForTenant(ctx, tenant)
	if err != nil {
		return FileProviderDomain{}, err
	}
	if !found {
		return FileProviderDomain{}, ErrNotFound
	}
	return domain, nil
}

func domainFromProvision(provision TenantProvision) (FileProviderDomain, error) {
	domainID, err := causal.DeriveDomainID(provision.OwnerID, provision.FileProvider.PresentationInstanceID)
	if err != nil {
		return FileProviderDomain{}, fmt.Errorf("catalog: derive File Provider domain: %w", err)
	}
	return FileProviderDomain{
		DomainID: domainID, OwnerID: provision.OwnerID, Tenant: provision.Tenant,
		Generation: provision.Generation, Root: provision.Root, Access: provision.Access,
		PresentationInstance: provision.FileProvider.PresentationInstanceID,
		DisplayName:          provision.FileProvider.DisplayName,
	}, nil
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
SELECT domain_id, tenant, owner_id, generation, root_id, access_mode,
       presentation_instance_id, display_name, confirmed_absent
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
	var access uint8
	var root []byte
	if err := scanner.Scan(
		&domainID, &tenant, &removal.Domain.OwnerID, &generation, &root, &access,
		&removal.Domain.PresentationInstance, &removal.Domain.DisplayName, &removal.ConfirmedAbsent,
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
	removal.Domain.Access = TenantAccessMode(access)
	removal.Domain.Root = parsedRoot
	if err := validateFileProviderDomainRemoval(removal); err != nil {
		return FileProviderDomainRemoval{}, fmt.Errorf("catalog: corrupt File Provider domain removal: %w", err)
	}
	return removal, nil
}

func validateFileProviderDomainRemoval(removal FileProviderDomainRemoval) error {
	domain := removal.Domain
	if domain.DomainID == "" || domain.OwnerID == "" || domain.Tenant == "" || domain.Generation == 0 ||
		domain.Root == (ObjectID{}) || (domain.Access != TenantReadOnly && domain.Access != TenantReadWrite) ||
		domain.PresentationInstance == "" || domain.DisplayName == "" {
		return fmt.Errorf("%w: File Provider domain removal identity is incomplete", ErrInvalidObject)
	}
	derived, err := causal.DeriveDomainID(domain.OwnerID, domain.PresentationInstance)
	if err != nil {
		return fmt.Errorf("%w: derive File Provider domain removal identity: %v", ErrInvalidObject, err)
	}
	if derived != domain.DomainID {
		return fmt.Errorf("%w: File Provider domain removal id is not derived from owner and account", ErrInvalidObject)
	}
	if fileProviderDomainRecordBytes(domain) > FileProviderDomainRecordMaxBytes {
		return fmt.Errorf("%w: File Provider domain removal exceeds raw byte limit", ErrInvalidObject)
	}
	return nil
}

func validateFileProviderDomainRecord(domain FileProviderDomain) error {
	if domain.DomainID == "" || domain.OwnerID == "" || domain.Tenant == "" || domain.Generation == 0 ||
		domain.Root == (ObjectID{}) || (domain.Access != TenantReadOnly && domain.Access != TenantReadWrite) ||
		domain.PresentationInstance == "" || domain.DisplayName == "" {
		return fmt.Errorf("%w: File Provider domain identity is incomplete", ErrInvalidObject)
	}
	if domain.Registered != (exactAbsolutePath(domain.PublicPath) && domain.ActivationGeneration != "" &&
		!strings.ContainsRune(domain.ActivationGeneration, 0)) {
		return fmt.Errorf("%w: File Provider domain activation proof is incomplete", ErrInvalidObject)
	}
	if fileProviderDomainRecordBytes(domain) > FileProviderDomainRecordMaxBytes {
		return fmt.Errorf("%w: File Provider domain exceeds raw byte limit", ErrInvalidObject)
	}
	return nil
}

func fileProviderDomainRecordBytes(domain FileProviderDomain) int {
	return len(domain.DomainID) + len(domain.OwnerID) + len(domain.Tenant) +
		len(domain.PresentationInstance) + len(domain.DisplayName) + len(domain.PublicPath) +
		len(domain.ActivationGeneration) + 64
}

func equalFileProviderDomainIdentity(left, right FileProviderDomain) bool {
	return left.DomainID == right.DomainID && left.OwnerID == right.OwnerID && left.Tenant == right.Tenant &&
		left.Generation == right.Generation && left.Root == right.Root && left.Access == right.Access &&
		left.PresentationInstance == right.PresentationInstance &&
		left.DisplayName == right.DisplayName
}
