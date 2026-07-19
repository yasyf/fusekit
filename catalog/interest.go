package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

// AddInterest durably records one owner's demand for an exact content revision.
func (c *Catalog) AddInterest(
	ctx context.Context,
	mutation MutationID,
	tenant TenantID,
	object ObjectID,
	owner InterestOwner,
	desired Revision,
) (MaterializationInterest, error) {
	if err := validateInterestOwner(owner); err != nil || desired == 0 {
		return MaterializationInterest{}, fmt.Errorf("%w: invalid materialization interest", ErrInvalidObject)
	}
	id := interestFromMutation(mutation)
	request := struct {
		Object  ObjectID
		Owner   InterestOwner
		Desired Revision
	}{object, owner, desired}
	record, err := c.applyPreparedMutation(ctx, mutation, tenant, MutationAddInterest, request, 0,
		CausalOrigin{Cause: causal.CauseOnDemand, Domain: owner.Domain, Generation: owner.Generation}, nil,
		func(ctx context.Context, tx *sql.Tx, revision Revision) (ObjectID, ObjectID, error) {
			current, err := currentObject(ctx, tx, tenant, object, false)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO materialization_interests(
    interest_id, tenant, object_id, owner_presentation, owner_domain, owner_generation,
    desired_revision, created_revision, removed_revision
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
				id[:], string(tenant), object[:], uint8(owner.Presentation), string(owner.Domain), uint64(owner.Generation), uint64(desired), uint64(revision)); err != nil {
				return ObjectID{}, ObjectID{}, mapConstraint(err)
			}
			if owner.Presentation == PresentationFileProvider && current.Visibility.FileProvider {
				if err := writeChange(ctx, tx, tenant, revision, workingSetScope(owner), 0, ChangeUpsert, object, current.Revision); err != nil {
					return ObjectID{}, ObjectID{}, err
				}
			}
			return ObjectID(id), object, nil
		})
	if err != nil {
		return MaterializationInterest{}, err
	}
	return c.interest(ctx, tenant, InterestID(record.Primary))
}

// RemoveInterest durably retires one materialization interest.
func (c *Catalog) RemoveInterest(ctx context.Context, mutation MutationID, tenant TenantID, id InterestID) (MaterializationInterest, error) {
	existing, err := c.interest(ctx, tenant, id)
	if err != nil {
		return MaterializationInterest{}, err
	}
	request := struct {
		ID    InterestID
		Owner InterestOwner
	}{id, existing.Owner}
	record, err := c.applyPreparedMutation(ctx, mutation, tenant, MutationRemoveInterest, request, 0,
		CausalOrigin{Cause: causal.CauseOnDemand, Domain: existing.Owner.Domain, Generation: existing.Owner.Generation}, nil,
		func(ctx context.Context, tx *sql.Tx, revision Revision) (ObjectID, ObjectID, error) {
			interest, err := readInterest(ctx, tx, tenant, id, false)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			result, err := tx.ExecContext(ctx, `
UPDATE materialization_interests SET removed_revision = ?
WHERE tenant = ? AND interest_id = ? AND removed_revision IS NULL`,
				uint64(revision), string(tenant), id[:])
			if err != nil {
				return ObjectID{}, ObjectID{}, fmt.Errorf("catalog: remove materialization interest: %w", err)
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return ObjectID{}, ObjectID{}, fmt.Errorf("catalog: inspect interest removal: %w", err)
			}
			if rows != 1 {
				return ObjectID{}, ObjectID{}, ErrNotFound
			}
			if interest.Owner != existing.Owner {
				return ObjectID{}, ObjectID{}, fmt.Errorf("%w: materialization interest owner changed", ErrIntegrity)
			}
			if interest.Owner.Presentation == PresentationFileProvider {
				current, err := currentObject(ctx, tx, tenant, interest.ObjectID, false)
				if err != nil {
					return ObjectID{}, ObjectID{}, err
				}
				if current.Visibility.FileProvider {
					if err := writeChange(ctx, tx, tenant, revision, workingSetScope(interest.Owner), 0, ChangeDelete, interest.ObjectID, current.Revision); err != nil {
						return ObjectID{}, ObjectID{}, err
					}
				}
			}
			return ObjectID(id), interest.ObjectID, nil
		})
	if err != nil {
		return MaterializationInterest{}, err
	}
	return c.interest(ctx, tenant, InterestID(record.Primary))
}

// Interests returns every live materialization interest for an object.
func (c *Catalog) Interests(ctx context.Context, tenant TenantID, object ObjectID) ([]MaterializationInterest, error) {
	rows, err := c.readDB.QueryContext(ctx, `
SELECT interest_id, tenant, object_id, owner_presentation, owner_domain, owner_generation,
       desired_revision, created_revision, removed_revision
FROM materialization_interests
WHERE tenant = ? AND object_id = ? AND removed_revision IS NULL
ORDER BY interest_id`, string(tenant), object[:])
	if err != nil {
		return nil, fmt.Errorf("catalog: query materialization interests: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var interests []MaterializationInterest
	for rows.Next() {
		interest, err := scanInterest(rows)
		if err != nil {
			return nil, fmt.Errorf("catalog: scan materialization interest: %w", err)
		}
		interests = append(interests, interest)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: read materialization interests: %w", err)
	}
	return interests, nil
}

// HasMaterializationDemand reports whether any live interest requires tenant content.
func (c *Catalog) HasMaterializationDemand(ctx context.Context, tenant TenantID) (bool, error) {
	var live int
	if err := c.readDB.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM materialization_interests
    WHERE tenant = ? AND removed_revision IS NULL
)`, string(tenant)).Scan(&live); err != nil {
		return false, fmt.Errorf("catalog: inspect materialization demand: %w", err)
	}
	return live != 0, nil
}

func (c *Catalog) interest(ctx context.Context, tenant TenantID, id InterestID) (MaterializationInterest, error) {
	return readInterest(ctx, c.readDB, tenant, id, true)
}

type interestQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readInterest(ctx context.Context, q interestQuerier, tenant TenantID, id InterestID, includeRemoved bool) (MaterializationInterest, error) {
	query := `
SELECT interest_id, tenant, object_id, owner_presentation, owner_domain, owner_generation,
       desired_revision, created_revision, removed_revision
FROM materialization_interests WHERE tenant = ? AND interest_id = ?`
	if !includeRemoved {
		query += " AND removed_revision IS NULL"
	}
	interest, err := scanInterest(q.QueryRowContext(ctx, query, string(tenant), id[:]))
	if errors.Is(err, sql.ErrNoRows) {
		return MaterializationInterest{}, ErrNotFound
	}
	if err != nil {
		return MaterializationInterest{}, fmt.Errorf("catalog: read materialization interest: %w", err)
	}
	return interest, nil
}

func scanInterest(s rowScanner) (MaterializationInterest, error) {
	var rawID, rawObject []byte
	var tenant, domain string
	var presentation uint8
	var generation, desired, created uint64
	var removed sql.NullInt64
	if err := s.Scan(&rawID, &tenant, &rawObject, &presentation, &domain, &generation, &desired, &created, &removed); err != nil {
		return MaterializationInterest{}, err
	}
	if len(rawID) != len(InterestID{}) {
		return MaterializationInterest{}, fmt.Errorf("catalog: corrupt interest id length %d", len(rawID))
	}
	var id InterestID
	copy(id[:], rawID)
	object, err := objectID(rawObject)
	if err != nil {
		return MaterializationInterest{}, err
	}
	owner := InterestOwner{Presentation: Presentation(presentation), Domain: causal.DomainID(domain), Generation: causal.Generation(generation)}
	if err := validateInterestOwner(owner); err != nil {
		return MaterializationInterest{}, fmt.Errorf("catalog: corrupt materialization interest owner: %w", err)
	}
	interest := MaterializationInterest{
		ID: id, Tenant: TenantID(tenant), ObjectID: object, Owner: owner,
		DesiredRevision: Revision(desired), CreatedRevision: Revision(created),
	}
	if removed.Valid {
		interest.RemovedRevision = Revision(removed.Int64)
	}
	return interest, nil
}

func validateInterestOwner(owner InterestOwner) error {
	if owner.Presentation != PresentationMount && owner.Presentation != PresentationFileProvider {
		return fmt.Errorf("%w: unknown interest presentation %d", ErrInvalidObject, owner.Presentation)
	}
	if owner.Domain == "" || owner.Generation == 0 {
		return fmt.Errorf("%w: interest owner has no exact domain generation", ErrInvalidObject)
	}
	return nil
}

func workingSetScope(owner InterestOwner) EnumerationScope {
	return EnumerationScope{
		Kind: EnumerationWorkingSet, Presentation: PresentationFileProvider,
		Domain: owner.Domain, Generation: owner.Generation,
	}
}

func interestFromMutation(id MutationID) InterestID {
	digest := sha256.Sum256(append([]byte("fusekit.catalog.interest\x00"), id[:]...))
	var interest InterestID
	copy(interest[:], digest[:len(interest)])
	return interest
}

func liveInterestOwners(ctx context.Context, tx *sql.Tx, tenant TenantID, object ObjectID) ([]InterestOwner, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT owner_presentation, owner_domain, owner_generation
FROM materialization_interests
WHERE tenant = ? AND object_id = ? AND removed_revision IS NULL
ORDER BY owner_presentation, owner_domain, owner_generation`, string(tenant), object[:])
	if err != nil {
		return nil, fmt.Errorf("catalog: query materialization interest owners: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var owners []InterestOwner
	for rows.Next() {
		var presentation uint8
		var domain string
		var generation uint64
		if err := rows.Scan(&presentation, &domain, &generation); err != nil {
			return nil, fmt.Errorf("catalog: scan materialization interest owner: %w", err)
		}
		owner := InterestOwner{Presentation: Presentation(presentation), Domain: causal.DomainID(domain), Generation: causal.Generation(generation)}
		if err := validateInterestOwner(owner); err != nil {
			return nil, fmt.Errorf("catalog: corrupt materialization interest owner: %w", err)
		}
		owners = append(owners, owner)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: read materialization interest owners: %w", err)
	}
	return owners, nil
}

func transferInterests(
	ctx context.Context,
	tx *sql.Tx,
	mutation MutationID,
	tenant TenantID,
	source ObjectID,
	target ObjectID,
	revision Revision,
) error {
	rows, err := tx.QueryContext(ctx, `
SELECT interest_id, owner_presentation, owner_domain, owner_generation, desired_revision
FROM materialization_interests
WHERE tenant = ? AND object_id = ? AND removed_revision IS NULL
ORDER BY interest_id`, string(tenant), target[:])
	if err != nil {
		return fmt.Errorf("catalog: query replacement interests: %w", err)
	}
	type transfer struct {
		id      InterestID
		owner   InterestOwner
		desired Revision
	}
	var transfers []transfer
	for rows.Next() {
		var rawID []byte
		var domain string
		var presentation uint8
		var generation, desired uint64
		if err := rows.Scan(&rawID, &presentation, &domain, &generation, &desired); err != nil {
			_ = rows.Close()
			return fmt.Errorf("catalog: scan replacement interest: %w", err)
		}
		if len(rawID) != len(InterestID{}) {
			_ = rows.Close()
			return fmt.Errorf("catalog: corrupt replacement interest id length %d", len(rawID))
		}
		var id InterestID
		copy(id[:], rawID)
		owner := InterestOwner{Presentation: Presentation(presentation), Domain: causal.DomainID(domain), Generation: causal.Generation(generation)}
		if err := validateInterestOwner(owner); err != nil {
			_ = rows.Close()
			return fmt.Errorf("catalog: corrupt replacement interest owner: %w", err)
		}
		transfers = append(transfers, transfer{id: id, owner: owner, desired: Revision(desired)})
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("catalog: close replacement interests: %w", err)
	}
	if len(transfers) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE materialization_interests SET removed_revision = ?
WHERE tenant = ? AND object_id = ? AND removed_revision IS NULL`,
		uint64(revision), string(tenant), target[:]); err != nil {
		return fmt.Errorf("catalog: close replacement interests: %w", err)
	}
	for _, interest := range transfers {
		result, err := tx.ExecContext(ctx, `
UPDATE materialization_interests
SET desired_revision = MAX(desired_revision, ?)
WHERE tenant = ? AND object_id = ? AND owner_presentation = ? AND owner_domain = ?
  AND owner_generation = ? AND removed_revision IS NULL`,
			uint64(interest.desired), string(tenant), source[:], uint8(interest.owner.Presentation), string(interest.owner.Domain), uint64(interest.owner.Generation))
		if err != nil {
			return fmt.Errorf("catalog: merge replacement interest: %w", err)
		}
		merged, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("catalog: inspect replacement interest merge: %w", err)
		}
		if merged != 0 {
			continue
		}
		id := transferredInterestID(mutation, interest.id)
		if _, err := tx.ExecContext(ctx, `
INSERT INTO materialization_interests(
    interest_id, tenant, object_id, owner_presentation, owner_domain, owner_generation,
    desired_revision, created_revision, removed_revision
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
			id[:], string(tenant), source[:], uint8(interest.owner.Presentation), string(interest.owner.Domain), uint64(interest.owner.Generation), uint64(interest.desired), uint64(revision)); err != nil {
			return mapConstraint(err)
		}
	}
	return nil
}

func transferredInterestID(mutation MutationID, interest InterestID) InterestID {
	material := make([]byte, 0, len("fusekit.catalog.interest-transfer\x00")+len(mutation)+len(interest))
	material = append(material, "fusekit.catalog.interest-transfer\x00"...)
	material = append(material, mutation[:]...)
	material = append(material, interest[:]...)
	digest := sha256.Sum256(material)
	var id InterestID
	copy(id[:], digest[:len(id)])
	return id
}
