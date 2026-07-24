package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/yasyf/fusekit/causal"
)

const (
	// TopologyPageLimit bounds one durable topology page.
	TopologyPageLimit = 256
	// TopologyPageByteLimit bounds the raw topology records in one page.
	TopologyPageByteLimit = 1 << 20
	// TopologyTenantLimit bounds desired tenants owned by one product.
	TopologyTenantLimit     = 1_000_000
	topologyChangeRetention = 4096
)

// ErrTopologyRevisionStale means a topology change cursor requires resnapshot.
var ErrTopologyRevisionStale = errors.New("catalog: topology revision is stale")

// TopologyRevision identifies one atomic desired-topology commit for an owner.
type TopologyRevision uint64

// TopologyHeadState identifies the durable head and retained change floor.
type TopologyHeadState struct {
	Owner       SourceAuthorityFleetOwnerID
	Revision    TopologyRevision
	Floor       TopologyRevision
	TenantCount uint64
	Fleet       *DesiredSourceAuthorityFleetState
}

// TopologySection identifies the ordered record class of a snapshot cursor.
type TopologySection uint8

const (
	TopologySectionInitial TopologySection = iota
	TopologySectionTenants
	TopologySectionAuthorities
)

// TopologyCursor binds pagination to one owner and immutable topology revision.
type TopologyCursor struct {
	Owner          SourceAuthorityFleetOwnerID
	Revision       TopologyRevision
	Section        TopologySection
	AfterTenant    TenantID
	AfterAuthority causal.SourceAuthorityID
	TenantOffset   uint64
}

// TopologySnapshotRequest addresses one immutable owner topology page.
type TopologySnapshotRequest struct {
	Owner    SourceAuthorityFleetOwnerID
	Revision TopologyRevision
	Cursor   TopologyCursor
	Limit    int
}

// TopologySourceAuthority is one desired source runtime declaration.
type TopologySourceAuthority struct {
	Owner             SourceAuthorityFleetOwnerID
	FleetGeneration   causal.Generation
	Authority         causal.SourceAuthorityID
	DriverID          string
	DriverConfig      []byte
	DeclarationDigest [32]byte
}

// TopologySnapshotPage contains one atomic slice of tenants and authorities.
type TopologySnapshotPage struct {
	Head        TopologyHeadState
	Tenants     []TenantProvision
	Authorities []TopologySourceAuthority
	Next        TopologyCursor
}

// TopologyChangeKind identifies which owner topology class changed.
type TopologyChangeKind uint8

const (
	TopologyChangeTenant TopologyChangeKind = iota + 1
	TopologyChangeSourceAuthorityFleet
)

// TopologyChange identifies one committed topology transition.
type TopologyChange struct {
	Revision        TopologyRevision
	Kind            TopologyChangeKind
	Tenant          TenantID
	FleetGeneration causal.Generation
}

// TopologyChangesRequest addresses changes strictly after one revision.
type TopologyChangesRequest struct {
	Owner SourceAuthorityFleetOwnerID
	After TopologyRevision
	Limit int
}

// TopologyChangePage contains one bounded durable change page.
type TopologyChangePage struct {
	Head    TopologyHeadState
	Changes []TopologyChange
	Next    TopologyRevision
}

// StaleTopologyRevisionError reports a change cursor below the retained floor.
type StaleTopologyRevisionError struct {
	Revision TopologyRevision
	Floor    TopologyRevision
}

// Error implements error.
func (e *StaleTopologyRevisionError) Error() string {
	return fmt.Sprintf("catalog: stale topology revision %d; compaction floor is %d", e.Revision, e.Floor)
}

// Unwrap exposes the stable cross-process resnapshot classification.
func (e *StaleTopologyRevisionError) Unwrap() error { return ErrTopologyRevisionStale }

type topologyNotifier struct {
	mu     sync.Mutex
	wake   chan struct{}
	closed bool
}

func newTopologyNotifier() *topologyNotifier {
	return &topologyNotifier{wake: make(chan struct{})}
}

func (n *topologyNotifier) snapshot() (<-chan struct{}, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.wake, n.closed
}

func (n *topologyNotifier) signal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	close(n.wake)
	n.wake = make(chan struct{})
}

func (n *topologyNotifier) close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.closed {
		n.closed = true
		close(n.wake)
	}
}

func validateTopologyOwner(owner SourceAuthorityFleetOwnerID) error {
	return ValidateSourceAuthorityFleetOwnerID(owner)
}

// Validate rejects an unbound or unbounded snapshot request.
func (r TopologySnapshotRequest) Validate() error {
	if err := validateTopologyOwner(r.Owner); err != nil {
		return err
	}
	if r.Limit < 1 || r.Limit > TopologyPageLimit {
		return fmt.Errorf("%w: invalid topology snapshot request", ErrInvalidObject)
	}
	if r.Cursor == (TopologyCursor{}) {
		return nil
	}
	if r.Revision == 0 {
		return fmt.Errorf("%w: empty topology cannot be paged", ErrInvalidObject)
	}
	if r.Cursor.Owner != r.Owner || r.Cursor.Revision != r.Revision ||
		(r.Cursor.Section != TopologySectionTenants && r.Cursor.Section != TopologySectionAuthorities) {
		return fmt.Errorf("%w: topology cursor is not bound to request", ErrInvalidObject)
	}
	if r.Cursor.Section == TopologySectionTenants && r.Cursor.AfterAuthority != "" {
		return fmt.Errorf("%w: tenant topology cursor contains authority state", ErrInvalidObject)
	}
	if r.Cursor.Section == TopologySectionAuthorities && r.Cursor.AfterTenant != "" {
		return fmt.Errorf("%w: authority topology cursor contains tenant state", ErrInvalidObject)
	}
	return nil
}

// Validate rejects an unbound or unbounded change request.
func (r TopologyChangesRequest) Validate() error {
	if err := validateTopologyOwner(r.Owner); err != nil {
		return err
	}
	if r.Limit < 1 || r.Limit > TopologyPageLimit {
		return fmt.Errorf("%w: invalid topology change limit", ErrInvalidObject)
	}
	return nil
}

// Validate verifies one topology head for owner.
func (h TopologyHeadState) Validate(owner SourceAuthorityFleetOwnerID) error {
	if err := validateTopologyOwner(owner); err != nil {
		return err
	}
	if h.Owner != owner {
		return fmt.Errorf("%w: invalid topology head", ErrIntegrity)
	}
	if h.Revision == 0 || h.Floor == 0 {
		if h.Revision != 0 || h.Floor != 0 || h.TenantCount != 0 || h.Fleet != nil {
			return fmt.Errorf("%w: invalid empty topology head", ErrIntegrity)
		}
		return nil
	}
	if h.Floor > h.Revision {
		return fmt.Errorf("%w: invalid topology head", ErrIntegrity)
	}
	if h.TenantCount > TopologyTenantLimit {
		return fmt.Errorf("%w: topology tenant count exceeds limit", ErrIntegrity)
	}
	if h.Fleet != nil {
		if h.Fleet.Owner != owner || h.Fleet.Generation == 0 {
			return fmt.Errorf("%w: topology fleet does not match owner", ErrIntegrity)
		}
		if err := h.Fleet.Validate(); err != nil {
			return fmt.Errorf("%w: invalid topology fleet", ErrIntegrity)
		}
	}
	return nil
}

// Validate verifies a bounded revision-fenced topology snapshot page.
func (p TopologySnapshotPage) Validate(request TopologySnapshotRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := p.Head.Validate(request.Owner); err != nil {
		return err
	}
	if p.Head.Revision != request.Revision || len(p.Tenants)+len(p.Authorities) > request.Limit {
		return fmt.Errorf("%w: topology snapshot does not match request", ErrIntegrity)
	}
	if request.Revision == 0 {
		if p.Head.Fleet != nil || len(p.Tenants) != 0 || len(p.Authorities) != 0 || p.Next != (TopologyCursor{}) {
			return fmt.Errorf("%w: empty topology snapshot contains records", ErrIntegrity)
		}
		return nil
	}
	if request.Cursor.Section == TopologySectionAuthorities && len(p.Tenants) != 0 {
		return fmt.Errorf("%w: authority cursor returned tenant topology", ErrIntegrity)
	}
	rawBytes := 0
	previousTenant := request.Cursor.AfterTenant
	for _, provision := range p.Tenants {
		if provision.OwnerID != string(request.Owner) || provision.Tenant <= previousTenant {
			return fmt.Errorf("%w: topology tenants are not ordered for owner", ErrIntegrity)
		}
		if err := validateTenantProvision(provision); err != nil {
			return err
		}
		rawBytes += tenantProvisionRecordBytes(provision)
		previousTenant = provision.Tenant
	}
	tenantOffset := request.Cursor.TenantOffset + uint64(len(p.Tenants))
	if tenantOffset > p.Head.TenantCount {
		return fmt.Errorf("%w: topology tenant page exceeds declared count", ErrIntegrity)
	}
	if len(p.Authorities) != 0 && tenantOffset != p.Head.TenantCount {
		return fmt.Errorf("%w: topology authorities preceded remaining tenants", ErrIntegrity)
	}
	previousAuthority := request.Cursor.AfterAuthority
	if request.Cursor.Section != TopologySectionAuthorities {
		previousAuthority = ""
	}
	for _, authority := range p.Authorities {
		if p.Head.Fleet == nil || authority.Owner != request.Owner ||
			authority.FleetGeneration != p.Head.Fleet.Generation || authority.Authority <= previousAuthority {
			return fmt.Errorf("%w: topology authorities are not ordered for fleet", ErrIntegrity)
		}
		if err := validateSourceAuthorityDeclarations([]SourceAuthorityDeclaration{{
			Authority: authority.Authority, DriverID: authority.DriverID,
			DriverConfig:      authority.DriverConfig,
			DeclarationDigest: authority.DeclarationDigest,
		}}); err != nil {
			return err
		}
		rawBytes += len(authority.Authority) + len(authority.DriverID) +
			len(authority.DriverConfig) + len(authority.DeclarationDigest) + 32
		previousAuthority = authority.Authority
	}
	if rawBytes > TopologyPageByteLimit {
		return fmt.Errorf("%w: topology snapshot exceeds byte limit", ErrInvalidObject)
	}
	if p.Next != (TopologyCursor{}) {
		if p.Next.Owner != request.Owner || p.Next.Revision != request.Revision {
			return fmt.Errorf("%w: topology next cursor is unbound", ErrIntegrity)
		}
		switch p.Next.Section {
		case TopologySectionTenants:
			if len(p.Tenants) == 0 || p.Next.AfterTenant != previousTenant ||
				p.Next.AfterAuthority != "" || p.Next.TenantOffset != tenantOffset ||
				tenantOffset >= p.Head.TenantCount || len(p.Authorities) != 0 {
				return fmt.Errorf("%w: invalid tenant topology next cursor", ErrIntegrity)
			}
		case TopologySectionAuthorities:
			if p.Next.AfterTenant != "" || p.Next.AfterAuthority != previousAuthority ||
				p.Next.TenantOffset != tenantOffset || tenantOffset != p.Head.TenantCount {
				return fmt.Errorf("%w: invalid authority topology next cursor", ErrIntegrity)
			}
		default:
			return fmt.Errorf("%w: invalid topology next section", ErrIntegrity)
		}
	} else if tenantOffset != p.Head.TenantCount {
		return fmt.Errorf("%w: topology snapshot ended before tenant count", ErrIntegrity)
	}
	return nil
}

// Validate verifies one bounded ordered topology change page.
func (p TopologyChangePage) Validate(request TopologyChangesRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := p.Head.Validate(request.Owner); err != nil {
		return err
	}
	if request.After > p.Head.Revision {
		return fmt.Errorf("%w: topology change cursor is ahead of head", ErrIntegrity)
	}
	if request.After+1 < p.Head.Floor {
		return fmt.Errorf("%w: topology change cursor is below floor", ErrIntegrity)
	}
	if len(p.Changes) > request.Limit {
		return fmt.Errorf("%w: topology change page exceeds limit", ErrIntegrity)
	}
	previous := request.After
	for _, change := range p.Changes {
		if change.Revision != previous+1 || change.Revision > p.Head.Revision {
			return fmt.Errorf("%w: topology changes are not contiguous", ErrIntegrity)
		}
		switch change.Kind {
		case TopologyChangeTenant:
			if change.Tenant == "" || change.FleetGeneration != 0 {
				return fmt.Errorf("%w: invalid tenant topology change", ErrIntegrity)
			}
		case TopologyChangeSourceAuthorityFleet:
			if change.Tenant != "" || change.FleetGeneration == 0 {
				return fmt.Errorf("%w: invalid fleet topology change", ErrIntegrity)
			}
		default:
			return fmt.Errorf("%w: invalid topology change kind", ErrIntegrity)
		}
		previous = change.Revision
	}
	if p.Head.Revision > request.After && len(p.Changes) == 0 {
		return fmt.Errorf("%w: topology change page omitted committed changes", ErrIntegrity)
	}
	if p.Next != 0 && (len(p.Changes) == 0 || p.Next != previous || previous >= p.Head.Revision) {
		return fmt.Errorf("%w: invalid topology change cursor", ErrIntegrity)
	}
	if p.Next == 0 && previous < p.Head.Revision {
		return fmt.Errorf("%w: topology change page ended before head", ErrIntegrity)
	}
	return nil
}

// TopologyHead returns the current atomic desired-topology revision for owner.
func (c *Catalog) TopologyHead(ctx context.Context, owner SourceAuthorityFleetOwnerID) (TopologyHeadState, error) {
	if err := validateTopologyOwner(owner); err != nil {
		return TopologyHeadState{}, err
	}
	return readTopologyHead(ctx, c.readDB, owner)
}

func readTopologyHead(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, owner SourceAuthorityFleetOwnerID) (TopologyHeadState, error) {
	var revision, floor, tenantCount uint64
	var fleetGeneration, authorityCount sql.NullInt64
	var authoritiesDigest, declarationsDigest []byte
	err := query.QueryRowContext(ctx, `
SELECT topology.revision, topology.floor, topology.tenant_count,
       fleet.generation, fleet.authority_count, fleet.authorities_digest,
       fleet.declarations_digest
FROM desired_topology_heads topology
LEFT JOIN source_authority_desired_fleet_heads head ON head.owner_id = topology.owner_id
LEFT JOIN source_authority_desired_fleets fleet
	  ON fleet.owner_id = head.owner_id AND fleet.generation = head.generation
WHERE topology.owner_id = ?`, string(owner)).Scan(
		&revision, &floor, &tenantCount, &fleetGeneration, &authorityCount, &authoritiesDigest,
		&declarationsDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return TopologyHeadState{Owner: owner}, nil
	}
	if err != nil {
		return TopologyHeadState{}, fmt.Errorf("catalog: read topology head: %w", err)
	}
	if revision == 0 || floor == 0 || floor > revision {
		return TopologyHeadState{}, ErrIntegrity
	}
	head := TopologyHeadState{
		Owner: owner, Revision: TopologyRevision(revision), Floor: TopologyRevision(floor),
		TenantCount: tenantCount,
	}
	if fleetGeneration.Valid {
		if !authorityCount.Valid || len(authoritiesDigest) != 32 || len(declarationsDigest) != 32 {
			return TopologyHeadState{}, ErrIntegrity
		}
		fleet := DesiredSourceAuthorityFleetState{
			Owner: owner, Generation: causal.Generation(fleetGeneration.Int64),
			AuthorityCount: uint64(authorityCount.Int64),
		}
		copy(fleet.AuthoritiesDigest[:], authoritiesDigest)
		copy(fleet.DeclarationsDigest[:], declarationsDigest)
		if err := fleet.Validate(); err != nil {
			return TopologyHeadState{}, ErrIntegrity
		}
		head.Fleet = &fleet
	}
	return head, nil
}

// TopologySnapshot returns one revision-fenced page from a single read transaction.
func (c *Catalog) TopologySnapshot(ctx context.Context, request TopologySnapshotRequest) (TopologySnapshotPage, error) {
	if err := request.Validate(); err != nil {
		return TopologySnapshotPage{}, err
	}
	tx, err := c.readDB.BeginTx(ctx, nil)
	if err != nil {
		return TopologySnapshotPage{}, fmt.Errorf("catalog: begin topology snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	head, err := readTopologyHead(ctx, tx, request.Owner)
	if err != nil {
		return TopologySnapshotPage{}, err
	}
	if head.Revision != request.Revision {
		return TopologySnapshotPage{}, ErrGenerationMismatch
	}
	page := TopologySnapshotPage{Head: head}
	if head.Revision == 0 {
		return page, nil
	}
	section := request.Cursor.Section
	if section == TopologySectionInitial {
		section = TopologySectionTenants
	}
	if section == TopologySectionTenants {
		more, err := appendTopologyTenants(ctx, tx, request, &page)
		if err != nil {
			return TopologySnapshotPage{}, err
		}
		if more || len(page.Tenants) == request.Limit {
			return page, nil
		}
		section = TopologySectionAuthorities
	}
	if section == TopologySectionAuthorities {
		if err := appendTopologyAuthorities(ctx, tx, request, &page); err != nil {
			return TopologySnapshotPage{}, err
		}
	}
	return page, nil
}

func appendTopologyTenants(
	ctx context.Context,
	tx *sql.Tx,
	request TopologySnapshotRequest,
	page *TopologySnapshotPage,
) (more bool, err error) {
	rows, err := tx.QueryContext(ctx, `
SELECT generation.tenant_id, tenant.root_id, generation.owner_id,
       generation.mount_presentation_root, generation.backing_root,
       generation.content_source_id, generation.file_provider_presentation_instance_id,
       generation.file_provider_display_name, generation.access_mode,
       generation.case_policy, generation.presentation_set, generation.generation
FROM tenant_intents intent
JOIN tenant_generations generation
  ON generation.tenant_id = intent.tenant_id
 AND generation.generation = intent.target_generation
JOIN tenant_activations activation
  ON activation.tenant_id = intent.tenant_id
 AND activation.active_generation = intent.target_generation
JOIN tenant_applications application
  ON application.tenant_id = intent.tenant_id
 AND application.generation = intent.target_generation
 AND application.staged_view_id = activation.active_view_id
JOIN tenants tenant ON tenant.tenant = intent.tenant_id
WHERE generation.owner_id = ? AND generation.tenant_id > ?
  AND intent.state = 1
  AND application.intent_revision = intent.intent_revision
  AND application.phase = 3
  AND application.staged_catalog_head = activation.active_catalog_head
  AND (
      SELECT COALESCE(SUM(CASE presentation.backend WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 0 END), 0)
      FROM presentation_materializations presentation
      WHERE presentation.tenant_id = intent.tenant_id
        AND presentation.generation = intent.target_generation
        AND presentation.intent_revision = intent.intent_revision
        AND presentation.phase = 4
        AND presentation.staged_view_id = activation.active_view_id
        AND presentation.staged_view_digest = application.staged_view_digest
        AND presentation.observed_revision = activation.active_catalog_head
  ) = generation.required_backends
  AND (
      SELECT COUNT(*) FROM presentation_materializations presentation
      WHERE presentation.tenant_id = intent.tenant_id
        AND presentation.generation = intent.target_generation
        AND presentation.intent_revision = intent.intent_revision
        AND presentation.phase = 4
        AND presentation.staged_view_id = activation.active_view_id
        AND presentation.staged_view_digest = application.staged_view_digest
        AND presentation.observed_revision = activation.active_catalog_head
  ) = CASE generation.required_backends WHEN 3 THEN 2 ELSE 1 END
  AND NOT EXISTS (
      SELECT 1 FROM presentation_materializations presentation
      WHERE presentation.tenant_id = intent.tenant_id
        AND presentation.generation = intent.target_generation
        AND (presentation.intent_revision <> intent.intent_revision
          OR presentation.phase <> 4
          OR presentation.staged_view_id <> activation.active_view_id
          OR presentation.staged_view_digest <> application.staged_view_digest
          OR presentation.observed_revision <> activation.active_catalog_head)
  )
ORDER BY generation.tenant_id LIMIT ?`, string(request.Owner), string(request.Cursor.AfterTenant), request.Limit+1)
	if err != nil {
		return false, fmt.Errorf("catalog: snapshot topology tenants: %w", err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	bytesUsed := 0
	for rows.Next() {
		provision, err := scanTenantProvision(rows)
		if err != nil {
			return false, err
		}
		if len(page.Tenants) == request.Limit {
			last := page.Tenants[len(page.Tenants)-1].Tenant
			page.Next = TopologyCursor{
				Owner: request.Owner, Revision: request.Revision,
				Section: TopologySectionTenants, AfterTenant: last,
				TenantOffset: request.Cursor.TenantOffset + uint64(len(page.Tenants)),
			}
			return true, nil
		}
		recordBytes := tenantProvisionRecordBytes(provision)
		if bytesUsed+recordBytes > TopologyPageByteLimit {
			if len(page.Tenants) == 0 {
				return false, fmt.Errorf("%w: tenant topology record exceeds byte limit", ErrInvalidObject)
			}
			last := page.Tenants[len(page.Tenants)-1].Tenant
			page.Next = TopologyCursor{
				Owner: request.Owner, Revision: request.Revision,
				Section: TopologySectionTenants, AfterTenant: last,
				TenantOffset: request.Cursor.TenantOffset + uint64(len(page.Tenants)),
			}
			return true, nil
		}
		page.Tenants = append(page.Tenants, provision)
		bytesUsed += recordBytes
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(page.Tenants) == request.Limit {
		var authorityExists int
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM source_authority_desired_fleet_heads head
    JOIN source_authority_desired_fleet_members member
      ON member.owner_id = head.owner_id AND member.generation = head.generation
    WHERE head.owner_id = ?
)`, string(request.Owner)).Scan(&authorityExists); err != nil {
			return false, err
		}
		if authorityExists != 0 {
			page.Next = TopologyCursor{
				Owner: request.Owner, Revision: request.Revision,
				Section:      TopologySectionAuthorities,
				TenantOffset: request.Cursor.TenantOffset + uint64(len(page.Tenants)),
			}
			return true, nil
		}
	}
	return false, nil
}

func appendTopologyAuthorities(
	ctx context.Context,
	tx *sql.Tx,
	request TopologySnapshotRequest,
	page *TopologySnapshotPage,
) (err error) {
	remaining := request.Limit - len(page.Tenants)
	bytesUsed := 0
	for _, provision := range page.Tenants {
		bytesUsed += tenantProvisionRecordBytes(provision)
	}
	after := request.Cursor.AfterAuthority
	if request.Cursor.Section != TopologySectionAuthorities {
		after = ""
	}
	rows, err := tx.QueryContext(ctx, `
SELECT member.generation, member.source_authority, member.driver_id,
       member.driver_config, member.declaration_digest
FROM source_authority_desired_fleet_heads head
JOIN source_authority_desired_fleet_members member
  ON member.owner_id = head.owner_id AND member.generation = head.generation
WHERE head.owner_id = ? AND member.source_authority > ?
ORDER BY member.source_authority LIMIT ?`, string(request.Owner), string(after), remaining+1)
	if err != nil {
		return fmt.Errorf("catalog: snapshot topology authorities: %w", err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	for rows.Next() {
		var generation uint64
		var authority, driverID string
		var driverConfig, digest []byte
		if err := rows.Scan(&generation, &authority, &driverID, &driverConfig, &digest); err != nil {
			return err
		}
		if len(page.Authorities) == remaining {
			last := page.Authorities[len(page.Authorities)-1].Authority
			page.Next = TopologyCursor{
				Owner: request.Owner, Revision: request.Revision,
				Section: TopologySectionAuthorities, AfterAuthority: last,
				TenantOffset: request.Cursor.TenantOffset + uint64(len(page.Tenants)),
			}
			return nil
		}
		if len(digest) != 32 {
			return ErrIntegrity
		}
		entry := TopologySourceAuthority{
			Owner: request.Owner, FleetGeneration: causal.Generation(generation),
			Authority: causal.SourceAuthorityID(authority), DriverID: driverID,
			DriverConfig: driverConfig,
		}
		copy(entry.DeclarationDigest[:], digest)
		recordBytes := len(entry.Authority) + len(entry.DriverID) +
			len(entry.DriverConfig) + len(entry.DeclarationDigest) + 32
		if bytesUsed+recordBytes > TopologyPageByteLimit {
			if bytesUsed == 0 {
				return fmt.Errorf("%w: authority topology record exceeds byte limit", ErrInvalidObject)
			}
			last := after
			if len(page.Authorities) != 0 {
				last = page.Authorities[len(page.Authorities)-1].Authority
			}
			page.Next = TopologyCursor{
				Owner: request.Owner, Revision: request.Revision,
				Section: TopologySectionAuthorities, AfterAuthority: last,
				TenantOffset: request.Cursor.TenantOffset + uint64(len(page.Tenants)),
			}
			return nil
		}
		page.Authorities = append(page.Authorities, entry)
		bytesUsed += recordBytes
	}
	return rows.Err()
}

// TopologyChangesSince returns durable owner topology changes after one revision.
func (c *Catalog) TopologyChangesSince(
	ctx context.Context,
	request TopologyChangesRequest,
) (page TopologyChangePage, err error) {
	if err := request.Validate(); err != nil {
		return TopologyChangePage{}, err
	}
	tx, err := c.readDB.BeginTx(ctx, nil)
	if err != nil {
		return TopologyChangePage{}, err
	}
	defer func() { _ = tx.Rollback() }()
	head, err := readTopologyHead(ctx, tx, request.Owner)
	if err != nil {
		return TopologyChangePage{}, err
	}
	if request.After > head.Revision {
		return TopologyChangePage{}, ErrGenerationMismatch
	}
	if request.After+1 < head.Floor {
		return TopologyChangePage{}, &StaleTopologyRevisionError{Revision: request.After, Floor: head.Floor}
	}
	rows, err := tx.QueryContext(ctx, `
SELECT revision, kind, tenant, fleet_generation
FROM desired_topology_changes
WHERE owner_id = ? AND revision > ?
ORDER BY revision LIMIT ?`, string(request.Owner), uint64(request.After), request.Limit+1)
	if err != nil {
		return TopologyChangePage{}, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	page = TopologyChangePage{Head: head, Changes: make([]TopologyChange, 0, request.Limit)}
	for rows.Next() {
		var revision, fleetGeneration uint64
		var kind uint8
		var tenant string
		if err := rows.Scan(&revision, &kind, &tenant, &fleetGeneration); err != nil {
			return TopologyChangePage{}, err
		}
		if len(page.Changes) == request.Limit {
			page.Next = page.Changes[len(page.Changes)-1].Revision
			break
		}
		page.Changes = append(page.Changes, TopologyChange{Revision: TopologyRevision(revision), Kind: TopologyChangeKind(kind), Tenant: TenantID(tenant), FleetGeneration: causal.Generation(fleetGeneration)})
	}
	if err := rows.Err(); err != nil {
		return TopologyChangePage{}, err
	}
	return page, nil
}

// WaitTopologyChanges waits without polling until a change is visible or ctx settles.
func (c *Catalog) WaitTopologyChanges(ctx context.Context, request TopologyChangesRequest) (TopologyChangePage, error) {
	for {
		page, err := c.TopologyChangesSince(ctx, request)
		if err != nil || len(page.Changes) != 0 || page.Head.Revision > request.After {
			return page, err
		}
		wake, closed := c.topology.snapshot()
		if closed {
			return TopologyChangePage{}, sql.ErrConnDone
		}
		page, err = c.TopologyChangesSince(ctx, request)
		if err != nil || len(page.Changes) != 0 || page.Head.Revision > request.After {
			return page, err
		}
		select {
		case <-ctx.Done():
			return TopologyChangePage{}, ctx.Err()
		case <-wake:
		}
	}
}

func advanceTopologyTx(
	ctx context.Context,
	tx *sql.Tx,
	owner SourceAuthorityFleetOwnerID,
	kind TopologyChangeKind,
	tenant TenantID,
	generation causal.Generation,
	tenantDelta int64,
) (TopologyRevision, error) {
	if err := validateTopologyOwner(owner); err != nil {
		return 0, err
	}
	if tenantDelta < -1 || tenantDelta > 1 ||
		(kind == TopologyChangeSourceAuthorityFleet && tenantDelta != 0) {
		return 0, fmt.Errorf("%w: invalid topology tenant delta", ErrInvalidObject)
	}
	var revision, tenantCount uint64
	err := tx.QueryRowContext(ctx, `SELECT revision, tenant_count FROM desired_topology_heads WHERE owner_id = ?`, string(owner)).Scan(&revision, &tenantCount)
	if errors.Is(err, sql.ErrNoRows) {
		revision = 1
		if tenantDelta < 0 {
			return 0, ErrIntegrity
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO desired_topology_heads(owner_id, revision, floor, tenant_count)
VALUES (?, 1, 1, ?)`, string(owner), tenantDelta); err != nil {
			return 0, mapConstraint(err)
		}
	} else if err != nil {
		return 0, err
	} else {
		revision++
		floor := uint64(1)
		if revision > topologyChangeRetention {
			floor = revision - topologyChangeRetention + 1
		}
		if tenantDelta < 0 && tenantCount == 0 {
			return 0, ErrIntegrity
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE desired_topology_heads
SET revision = ?, floor = ?, tenant_count = tenant_count + ?
WHERE owner_id = ?`, revision, floor, tenantDelta, string(owner)); err != nil {
			return 0, mapConstraint(err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM desired_topology_changes WHERE owner_id = ? AND revision < ?`, string(owner), floor); err != nil {
			return 0, err
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO desired_topology_changes(owner_id, revision, kind, tenant, fleet_generation)
VALUES (?, ?, ?, ?, ?)`, string(owner), revision, uint8(kind), string(tenant), uint64(generation)); err != nil {
		return 0, mapConstraint(err)
	}
	return TopologyRevision(revision), nil
}
