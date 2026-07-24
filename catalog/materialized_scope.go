package catalog

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

type materializedWorkingSetOwner struct {
	domain     causal.DomainID
	generation causal.Generation
}

func workingSetScope(owner materializedWorkingSetOwner) EnumerationScope {
	return EnumerationScope{
		Kind:         EnumerationWorkingSet,
		Presentation: PresentationFileProvider,
		Domain:       owner.domain,
		Generation:   owner.generation,
	}
}

func materializedWorkingSetOwners(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	container ObjectID,
) ([]materializedWorkingSetOwner, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT head.domain_id, head.generation
FROM file_provider_materialization_heads head
JOIN file_provider_materialized_containers container
  ON container.tenant = head.tenant AND container.domain_id = head.domain_id
 AND container.generation = head.generation
 AND container.backing_store_identity = head.backing_store_identity
WHERE head.tenant = ? AND head.eligible = 1 AND container.container_id = ?
ORDER BY head.domain_id, head.generation`, string(tenant), container[:])
	if err != nil {
		return nil, fmt.Errorf("catalog: query materialized container owners: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var owners []materializedWorkingSetOwner
	for rows.Next() {
		var domain string
		var generation uint64
		if err := rows.Scan(&domain, &generation); err != nil {
			return nil, fmt.Errorf("catalog: scan materialized container owner: %w", err)
		}
		owners = append(owners, materializedWorkingSetOwner{
			domain: causal.DomainID(domain), generation: causal.Generation(generation),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: read materialized container owners: %w", err)
	}
	return owners, nil
}
