package catalog

import (
	"context"
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
WHERE d.file_provider_account_id <> ''
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

func equalFileProviderDomainIdentity(left, right FileProviderDomain) bool {
	return left.DomainID == right.DomainID && left.OwnerID == right.OwnerID && left.Tenant == right.Tenant &&
		left.Generation == right.Generation && left.Root == right.Root && left.AccountInstance == right.AccountInstance &&
		left.DisplayName == right.DisplayName
}
