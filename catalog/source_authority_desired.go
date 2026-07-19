package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

// DesiredSourceAuthorityFleetState is one immutable product-published fleet.
type DesiredSourceAuthorityFleetState struct {
	Owner              SourceAuthorityFleetOwnerID
	Generation         causal.Generation
	AuthorityCount     uint64
	AuthoritiesDigest  [32]byte
	DeclarationsDigest [32]byte
}

// PublishDesiredSourceFleetRequest replaces one owner's complete desired fleet.
type PublishDesiredSourceFleetRequest struct {
	Owner              SourceAuthorityFleetOwnerID
	ExpectedGeneration causal.Generation
	Generation         causal.Generation
	Declarations       []SourceAuthorityDeclaration
}

// DesiredSourceFleetPageRequest addresses one immutable desired-fleet snapshot page.
type DesiredSourceFleetPageRequest struct {
	Owner              SourceAuthorityFleetOwnerID
	Generation         causal.Generation
	DeclarationsDigest [32]byte
	After              causal.SourceAuthorityID
	Limit              int
}

// DesiredSourceFleetPage is one authority-ordered desired-fleet snapshot page.
type DesiredSourceFleetPage struct {
	State        DesiredSourceAuthorityFleetState
	Declarations []SourceAuthorityDeclaration
	Next         causal.SourceAuthorityID
}

// Validate verifies one exact desired-fleet snapshot page.
func (p DesiredSourceFleetPage) Validate(request DesiredSourceFleetPageRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := p.State.Validate(); err != nil || p.State.Owner != request.Owner {
		return fmt.Errorf("%w: invalid desired source fleet page state", ErrIntegrity)
	}
	if request.Generation != 0 &&
		(p.State.Generation != request.Generation || p.State.DeclarationsDigest != request.DeclarationsDigest) {
		return fmt.Errorf("%w: desired source fleet page changed snapshot", ErrIntegrity)
	}
	if len(p.Declarations) > request.Limit ||
		sourceAuthorityDeclarationsBytes(p.Declarations) > TopologyPageByteLimit {
		return fmt.Errorf("%w: desired source fleet page exceeds bounds", ErrIntegrity)
	}
	previous := request.After
	for _, declaration := range p.Declarations {
		if declaration.Authority <= previous {
			return fmt.Errorf("%w: desired source fleet page is not ordered", ErrIntegrity)
		}
		previous = declaration.Authority
	}
	if err := validateSourceAuthorityDeclarations(p.Declarations); err != nil {
		return fmt.Errorf("%w: invalid desired source fleet declaration", ErrIntegrity)
	}
	if p.Next != "" && (len(p.Declarations) == 0 || p.Next != previous) {
		return fmt.Errorf("%w: desired source fleet continuation is unbound", ErrIntegrity)
	}
	return nil
}

// Validate verifies one immutable desired fleet state.
func (s DesiredSourceAuthorityFleetState) Validate() error {
	if err := ValidateSourceAuthorityFleetOwnerID(s.Owner); err != nil {
		return err
	}
	if s.Generation == 0 || s.AuthorityCount > SourceAuthorityFleetAuthorityLimit ||
		s.AuthoritiesDigest == ([32]byte{}) || s.DeclarationsDigest == ([32]byte{}) {
		return fmt.Errorf("%w: invalid desired source authority fleet", ErrInvalidObject)
	}
	return nil
}

// Validate rejects an unbounded, unordered, or stale desired fleet request.
func (r PublishDesiredSourceFleetRequest) Validate() error {
	if err := ValidateSourceAuthorityFleetOwnerID(r.Owner); err != nil {
		return err
	}
	if r.Generation == 0 || r.Generation <= r.ExpectedGeneration ||
		len(r.Declarations) > TopologyPageLimit ||
		sourceAuthorityDeclarationsBytes(r.Declarations) > TopologyPageByteLimit {
		return fmt.Errorf("%w: invalid desired source authority fleet request", ErrInvalidObject)
	}
	return validateSourceAuthorityDeclarations(r.Declarations)
}

// Validate rejects an unbound or oversized desired-fleet page request.
func (r DesiredSourceFleetPageRequest) Validate() error {
	if err := ValidateSourceAuthorityFleetOwnerID(r.Owner); err != nil {
		return err
	}
	if r.Limit <= 0 || r.Limit > TopologyPageLimit {
		return fmt.Errorf("%w: invalid desired source fleet page limit", ErrInvalidObject)
	}
	if r.Generation == 0 {
		if r.DeclarationsDigest != ([32]byte{}) || r.After != "" {
			return fmt.Errorf("%w: initial desired source fleet page is cursor-bound", ErrInvalidObject)
		}
		return nil
	}
	if r.DeclarationsDigest == ([32]byte{}) {
		return fmt.Errorf("%w: desired source fleet continuation lacks snapshot digest", ErrInvalidObject)
	}
	if r.After != "" {
		if err := validateSourceAuthorityID(r.After); err != nil {
			return err
		}
	}
	return nil
}

// PublishDesiredSourceFleet atomically publishes one complete desired fleet.
func (c *Catalog) PublishDesiredSourceFleet(
	ctx context.Context,
	request PublishDesiredSourceFleetRequest,
) (DesiredSourceAuthorityFleetState, error) {
	if err := request.Validate(); err != nil {
		return DesiredSourceAuthorityFleetState{}, err
	}
	authorities := sourceAuthorityDeclarationIDs(request.Declarations)
	authoritiesDigest, err := SourceAuthorityFleetDigest(authorities)
	if err != nil {
		return DesiredSourceAuthorityFleetState{}, err
	}
	declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(request.Declarations)
	if err != nil {
		return DesiredSourceAuthorityFleetState{}, err
	}
	stage, err := c.ReconcileSourceAuthorityFleet(ctx, SourceAuthorityFleetReconcileRequest{
		Owner: request.Owner, ExpectedGeneration: request.ExpectedGeneration,
		Generation: request.Generation, Declarations: request.Declarations, Complete: true,
		AuthorityCount: uint64(len(request.Declarations)), AuthoritiesDigest: authoritiesDigest,
		DeclarationsDigest: declarationsDigest,
	})
	if err != nil {
		return DesiredSourceAuthorityFleetState{}, err
	}
	if !stage.Complete {
		return DesiredSourceAuthorityFleetState{}, ErrIntegrity
	}
	return c.DesiredSourceAuthorityFleet(ctx, request.Owner)
}

// DesiredSourceAuthorityFleet returns the current product-published fleet.
func (c *Catalog) DesiredSourceAuthorityFleet(
	ctx context.Context,
	owner SourceAuthorityFleetOwnerID,
) (DesiredSourceAuthorityFleetState, error) {
	if err := ValidateSourceAuthorityFleetOwnerID(owner); err != nil {
		return DesiredSourceAuthorityFleetState{}, err
	}
	state, found, err := readDesiredSourceAuthorityFleet(ctx, c.readDB, owner)
	if err != nil {
		return DesiredSourceAuthorityFleetState{}, err
	}
	if !found {
		return DesiredSourceAuthorityFleetState{}, ErrNotFound
	}
	return state, nil
}

// DesiredSourceFleetPage reads one generation- and digest-fenced immutable desired fleet page.
func (c *Catalog) DesiredSourceFleetPage(
	ctx context.Context,
	request DesiredSourceFleetPageRequest,
) (DesiredSourceFleetPage, error) {
	if err := request.Validate(); err != nil {
		return DesiredSourceFleetPage{}, err
	}
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return DesiredSourceFleetPage{}, err
	}
	defer func() { _ = tx.Rollback() }()
	state, found, err := readDesiredSourceAuthorityFleet(ctx, tx, request.Owner)
	if err != nil {
		return DesiredSourceFleetPage{}, err
	}
	if !found {
		return DesiredSourceFleetPage{}, ErrNotFound
	}
	if request.Generation != 0 &&
		(state.Generation != request.Generation || state.DeclarationsDigest != request.DeclarationsDigest) {
		return DesiredSourceFleetPage{}, ErrGenerationMismatch
	}
	rows, err := tx.QueryContext(ctx, `
SELECT source_authority, driver_id, driver_config, declaration_digest
FROM source_authority_desired_fleet_members
WHERE owner_id = ? AND generation = ? AND source_authority > ?
ORDER BY source_authority LIMIT ?`, string(request.Owner), uint64(state.Generation),
		string(request.After), request.Limit+1)
	if err != nil {
		return DesiredSourceFleetPage{}, err
	}
	defer func() { _ = rows.Close() }()
	page := DesiredSourceFleetPage{
		State: state, Declarations: make([]SourceAuthorityDeclaration, 0, request.Limit),
	}
	bytesRead := 0
	for rows.Next() {
		var declaration SourceAuthorityDeclaration
		var digest []byte
		if err := rows.Scan(
			&declaration.Authority, &declaration.DriverID, &declaration.DriverConfig, &digest,
		); err != nil {
			return DesiredSourceFleetPage{}, err
		}
		if len(digest) != len(declaration.DeclarationDigest) {
			return DesiredSourceFleetPage{}, ErrIntegrity
		}
		copy(declaration.DeclarationDigest[:], digest)
		if len(page.Declarations) == request.Limit {
			page.Next = page.Declarations[len(page.Declarations)-1].Authority
			break
		}
		bytesRead += sourceAuthorityDeclarationsBytes([]SourceAuthorityDeclaration{declaration})
		if bytesRead > TopologyPageByteLimit {
			if len(page.Declarations) == 0 {
				return DesiredSourceFleetPage{}, fmt.Errorf("%w: desired source fleet declaration exceeds page limit", ErrIntegrity)
			}
			page.Next = page.Declarations[len(page.Declarations)-1].Authority
			break
		}
		page.Declarations = append(page.Declarations, declaration)
	}
	if err := rows.Err(); err != nil {
		return DesiredSourceFleetPage{}, err
	}
	if err := page.Validate(request); err != nil {
		return DesiredSourceFleetPage{}, err
	}
	if err := tx.Commit(); err != nil {
		return DesiredSourceFleetPage{}, err
	}
	return page, nil
}

func publishDesiredSourceAuthorityFleetTx(
	ctx context.Context,
	tx *sql.Tx,
	state SourceAuthorityFleetReconcileState,
) error {
	current, found, err := readDesiredSourceAuthorityFleet(ctx, tx, state.Owner)
	if err != nil {
		return err
	}
	if found {
		if current.Generation == state.Generation && current.AuthorityCount == state.AuthorityCount &&
			current.AuthoritiesDigest == state.AuthoritiesDigest &&
			current.DeclarationsDigest == state.DeclarationsDigest {
			return nil
		}
		if current.Generation != state.ExpectedGeneration {
			return ErrGenerationMismatch
		}
	} else if state.ExpectedGeneration != 0 {
		return ErrGenerationMismatch
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_desired_fleets(
    owner_id, generation, authority_count, authorities_digest, declarations_digest
) VALUES (?, ?, ?, ?, ?)`, string(state.Owner), uint64(state.Generation), state.AuthorityCount,
		state.AuthoritiesDigest[:], state.DeclarationsDigest[:]); err != nil {
		return mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_desired_fleet_members(
    owner_id, generation, source_authority, driver_id, driver_config, declaration_digest
)
SELECT owner_id, generation, source_authority, driver_id, driver_config, declaration_digest
FROM source_authority_fleet_stage_members
WHERE owner_id = ? AND generation = ?`, string(state.Owner), uint64(state.Generation)); err != nil {
		return mapConstraint(err)
	}
	var result sql.Result
	if found {
		result, err = tx.ExecContext(ctx, `
UPDATE source_authority_desired_fleet_heads SET generation = ?
WHERE owner_id = ? AND generation = ?`, uint64(state.Generation), string(state.Owner), uint64(state.ExpectedGeneration))
	} else {
		result, err = tx.ExecContext(ctx, `
INSERT INTO source_authority_desired_fleet_heads(owner_id, generation) VALUES (?, ?)`,
			string(state.Owner), uint64(state.Generation))
	}
	if err != nil {
		return mapConstraint(err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrGenerationMismatch
	}
	_, err = advanceTopologyTx(ctx, tx, state.Owner, TopologyChangeSourceAuthorityFleet, "", state.Generation, 0)
	return err
}

func readDesiredSourceAuthorityFleet(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, owner SourceAuthorityFleetOwnerID) (DesiredSourceAuthorityFleetState, bool, error) {
	var generation, count uint64
	var authorities, declarations []byte
	err := query.QueryRowContext(ctx, `
SELECT fleet.generation, fleet.authority_count, fleet.authorities_digest, fleet.declarations_digest
FROM source_authority_desired_fleet_heads head
JOIN source_authority_desired_fleets fleet
  ON fleet.owner_id = head.owner_id AND fleet.generation = head.generation
WHERE head.owner_id = ?`, string(owner)).Scan(&generation, &count, &authorities, &declarations)
	if errors.Is(err, sql.ErrNoRows) {
		return DesiredSourceAuthorityFleetState{}, false, nil
	}
	if err != nil {
		return DesiredSourceAuthorityFleetState{}, false, err
	}
	if len(authorities) != 32 || len(declarations) != 32 {
		return DesiredSourceAuthorityFleetState{}, false, ErrIntegrity
	}
	state := DesiredSourceAuthorityFleetState{Owner: owner, Generation: causal.Generation(generation), AuthorityCount: count}
	copy(state.AuthoritiesDigest[:], authorities)
	copy(state.DeclarationsDigest[:], declarations)
	if err := state.Validate(); err != nil {
		return DesiredSourceAuthorityFleetState{}, false, ErrIntegrity
	}
	return state, true, nil
}
