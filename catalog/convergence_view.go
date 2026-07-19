package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

// ConvergenceTarget is one tenant-local catalog commit of a causal source change.
type ConvergenceTarget struct {
	Change                  causal.ChangeSet
	Tenant                  TenantID
	CatalogRevision         Revision
	FileProviderFingerprint [32]byte
}

func readConvergenceChangeMetadata(
	ctx context.Context,
	query rowQuerier,
	change causal.ChangeID,
) (causal.ChangeSet, error) {
	var operation []byte
	var source, generation uint64
	var authority, cause, origin string
	if err := query.QueryRowContext(ctx, `
SELECT source_operation_id, source_authority, source_revision, cause, origin_domain, origin_generation
FROM convergence_changes WHERE change_id = ?`, change[:]).Scan(
		&operation, &authority, &source, &cause, &origin, &generation,
	); err != nil {
		return causal.ChangeSet{}, err
	}
	if len(operation) != len(causal.OperationID{}) {
		return causal.ChangeSet{}, fmt.Errorf("%w: corrupt convergence operation identity", ErrIntegrity)
	}
	var operationID causal.OperationID
	copy(operationID[:], operation)
	result := causal.ChangeSet{
		SourceAuthority:  causal.SourceAuthorityID(authority),
		SourceRevision:   causal.Revision(source),
		ChangeID:         change,
		OperationID:      operationID,
		Cause:            causal.Cause(cause),
		Origin:           causal.DomainID(origin),
		OriginGeneration: causal.Generation(generation),
	}
	if err := validateConvergenceChangeMetadata(result); err != nil {
		return causal.ChangeSet{}, err
	}
	return result, nil
}

func validateConvergenceChangeMetadata(change causal.ChangeSet) error {
	if change.SourceAuthority == "" || change.SourceRevision == 0 ||
		change.ChangeID == (causal.ChangeID{}) || change.OperationID == (causal.OperationID{}) {
		return fmt.Errorf("%w: incomplete convergence source identity", ErrIntegrity)
	}
	if err := validateCausalOrigin(CausalOrigin{
		Cause: change.Cause, Domain: change.Origin, Generation: change.OriginGeneration,
	}); err != nil {
		return fmt.Errorf("%w: corrupt convergence source metadata", ErrIntegrity)
	}
	return nil
}

// CurrentConvergenceTarget returns the newest causal catalog commit for one tenant and authority.
func (c *Catalog) CurrentConvergenceTarget(
	ctx context.Context,
	tenant TenantID,
	authority causal.SourceAuthorityID,
) (ConvergenceTarget, error) {
	if tenant == "" || authority == "" {
		return ConvergenceTarget{}, fmt.Errorf("%w: tenant source identity is incomplete", ErrInvalidObject)
	}
	var rawChange, rawOperation, rawFingerprint []byte
	var sourceRevision, originGeneration, catalogRevision uint64
	var sourceAuthority, cause, origin string
	if err := c.readDB.QueryRowContext(ctx, `
SELECT target.change_id, c.source_operation_id, c.source_authority, c.source_revision,
       c.cause, c.origin_domain, c.origin_generation, target.catalog_revision,
       target.file_provider_fingerprint
FROM source_tenant_targets target
JOIN convergence_changes c ON c.change_id = target.change_id
WHERE target.tenant = ? AND target.source_authority = ?`, string(tenant), string(authority)).Scan(
		&rawChange, &rawOperation, &sourceAuthority, &sourceRevision,
		&cause, &origin, &originGeneration, &catalogRevision, &rawFingerprint,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ConvergenceTarget{}, ErrNotFound
		}
		return ConvergenceTarget{}, fmt.Errorf("catalog: read current convergence target: %w", err)
	}
	if len(rawChange) != len(causal.ChangeID{}) || len(rawOperation) != len(causal.OperationID{}) ||
		len(rawFingerprint) != sha256.Size || sourceAuthority != string(authority) ||
		sourceRevision == 0 || catalogRevision == 0 {
		return ConvergenceTarget{}, fmt.Errorf("%w: corrupt current convergence target", ErrIntegrity)
	}
	var changeID causal.ChangeID
	copy(changeID[:], rawChange)
	var operationID causal.OperationID
	copy(operationID[:], rawOperation)
	change := causal.ChangeSet{
		SourceAuthority: authority, SourceRevision: causal.Revision(sourceRevision),
		ChangeID: changeID, OperationID: operationID, Cause: causal.Cause(cause),
		Origin: causal.DomainID(origin), OriginGeneration: causal.Generation(originGeneration),
	}
	if err := validateConvergenceChangeMetadata(change); err != nil {
		return ConvergenceTarget{}, err
	}
	return ConvergenceTarget{
		Change: change, Tenant: tenant, CatalogRevision: Revision(catalogRevision),
		FileProviderFingerprint: bytesToDigest(rawFingerprint),
	}, nil
}
