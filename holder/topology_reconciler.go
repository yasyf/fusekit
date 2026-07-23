package holder

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

type topologyStore interface {
	TopologyHead(context.Context, catalog.SourceAuthorityFleetOwnerID) (catalog.TopologyHeadState, error)
	TopologySnapshot(context.Context, catalog.TopologySnapshotRequest) (catalog.TopologySnapshotPage, error)
	TopologyChangesSince(context.Context, catalog.TopologyChangesRequest) (catalog.TopologyChangePage, error)
	WaitTopologyChanges(context.Context, catalog.TopologyChangesRequest) (catalog.TopologyChangePage, error)
}

type desiredTopology struct {
	Head        catalog.TopologyHeadState
	Tenants     []catalog.TenantProvision
	Authorities []catalog.TopologySourceAuthority
}

type topologyReconciler struct {
	store topologyStore
	owner catalog.SourceAuthorityFleetOwnerID
	apply func(context.Context, desiredTopology) error
}

func (r topologyReconciler) run(ctx context.Context) error {
	if r.store == nil || r.owner == "" || r.apply == nil {
		return errors.New("FuseKit runtime: desired topology reconciler is incomplete")
	}
	current, err := r.resnapshot(ctx)
	if err != nil {
		return err
	}
	if err := r.apply(ctx, current); err != nil {
		return fmt.Errorf("FuseKit runtime: apply initial desired topology: %w", err)
	}
	revision := current.Head.Revision
	for {
		next, stale, err := r.consumeChanges(ctx, revision, true)
		if err != nil {
			return err
		}
		if stale {
			current, err = r.resnapshot(ctx)
		} else {
			current, err = r.snapshot(ctx, next)
			if errors.Is(err, catalog.ErrGenerationMismatch) {
				continue
			}
		}
		if err != nil {
			return err
		}
		if err := r.apply(ctx, current); err != nil {
			return fmt.Errorf("FuseKit runtime: apply desired topology revision %d: %w", current.Head.Revision, err)
		}
		revision = current.Head.Revision
	}
}

func (r topologyReconciler) consumeChanges(
	ctx context.Context,
	after catalog.TopologyRevision,
	wait bool,
) (catalog.TopologyRevision, bool, error) {
	request := catalog.TopologyChangesRequest{Owner: r.owner, After: after, Limit: catalog.TopologyPageLimit}
	page, err := r.store.TopologyChangesSince(ctx, request)
	if err == nil && len(page.Changes) == 0 && page.Head.Revision == after && wait {
		page, err = r.store.WaitTopologyChanges(ctx, request)
	}
	if errors.Is(err, catalog.ErrTopologyRevisionStale) {
		return 0, true, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("FuseKit runtime: read desired topology changes: %w", err)
	}
	current := after
	for {
		for _, change := range page.Changes {
			if change.Revision != current+1 {
				return 0, false, errors.New("FuseKit runtime: desired topology change feed is not contiguous")
			}
			current = change.Revision
		}
		if page.Next == 0 {
			if current != page.Head.Revision {
				request.After = current
				page, err = r.store.TopologyChangesSince(ctx, request)
				if errors.Is(err, catalog.ErrTopologyRevisionStale) {
					return 0, true, nil
				}
				if err != nil {
					return 0, false, fmt.Errorf("FuseKit runtime: continue desired topology changes: %w", err)
				}
				continue
			}
			return current, false, nil
		}
		if page.Next != current {
			return 0, false, errors.New("FuseKit runtime: desired topology change cursor did not advance exactly")
		}
		request.After = page.Next
		page, err = r.store.TopologyChangesSince(ctx, request)
		if errors.Is(err, catalog.ErrTopologyRevisionStale) {
			return 0, true, nil
		}
		if err != nil {
			return 0, false, fmt.Errorf("FuseKit runtime: page desired topology changes: %w", err)
		}
	}
}

func (r topologyReconciler) resnapshot(ctx context.Context) (desiredTopology, error) {
	for {
		head, err := r.store.TopologyHead(ctx, r.owner)
		if err != nil {
			return desiredTopology{}, fmt.Errorf("FuseKit runtime: read desired topology head: %w", err)
		}
		topology, err := r.snapshot(ctx, head.Revision)
		if errors.Is(err, catalog.ErrGenerationMismatch) {
			continue
		}
		return topology, err
	}
}

func (r topologyReconciler) snapshot(
	ctx context.Context,
	revision catalog.TopologyRevision,
) (desiredTopology, error) {
	topology := desiredTopology{Head: catalog.TopologyHeadState{Owner: r.owner, Revision: revision}}
	var cursor catalog.TopologyCursor
	for {
		page, err := r.store.TopologySnapshot(ctx, catalog.TopologySnapshotRequest{
			Owner: r.owner, Revision: revision, Cursor: cursor, Limit: catalog.TopologyPageLimit,
		})
		if err != nil {
			return desiredTopology{}, err
		}
		if page.Head.Owner != r.owner || page.Head.Revision != revision {
			return desiredTopology{}, errors.New("FuseKit runtime: desired topology snapshot fence mismatch")
		}
		topology.Head = page.Head
		topology.Tenants = append(topology.Tenants, page.Tenants...)
		topology.Authorities = append(topology.Authorities, page.Authorities...)
		if page.Next == (catalog.TopologyCursor{}) {
			if err := validateDesiredTopology(topology); err != nil {
				return desiredTopology{}, err
			}
			return topology, nil
		}
		if page.Next.Owner != r.owner || page.Next.Revision != revision || page.Next == cursor {
			return desiredTopology{}, errors.New("FuseKit runtime: desired topology snapshot cursor is invalid")
		}
		cursor = page.Next
	}
}

func validateDesiredTopology(topology desiredTopology) error {
	if uint64(len(topology.Tenants)) != topology.Head.TenantCount {
		return errors.New("FuseKit runtime: desired topology tenant count mismatch")
	}
	var previousTenant catalog.TenantID
	for index, provision := range topology.Tenants {
		if provision.OwnerID != string(topology.Head.Owner) || (index > 0 && provision.Tenant <= previousTenant) {
			return errors.New("FuseKit runtime: desired topology tenant page is not exact and ordered")
		}
		previousTenant = provision.Tenant
	}
	if topology.Head.Fleet == nil {
		if len(topology.Authorities) != 0 {
			return errors.New("FuseKit runtime: desired topology has authority rows without an acknowledged fleet")
		}
		return nil
	}
	fleet := topology.Head.Fleet
	if fleet.Owner != topology.Head.Owner || uint64(len(topology.Authorities)) != fleet.AuthorityCount {
		return errors.New("FuseKit runtime: desired topology authority count mismatch")
	}
	authorities := make([]causal.SourceAuthorityID, len(topology.Authorities))
	declarations := make([]catalog.SourceAuthorityDeclaration, len(topology.Authorities))
	for index, authority := range topology.Authorities {
		if authority.Owner != topology.Head.Owner || authority.FleetGeneration != fleet.Generation ||
			(index > 0 && authority.Authority <= topology.Authorities[index-1].Authority) {
			return errors.New("FuseKit runtime: desired topology authority page is not exact and ordered")
		}
		authorities[index] = authority.Authority
		declarations[index] = catalog.SourceAuthorityDeclaration{
			Authority: authority.Authority, DriverID: authority.DriverID,
			DriverConfig:      append([]byte(nil), authority.DriverConfig...),
			DeclarationDigest: authority.DeclarationDigest,
		}
	}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest(authorities)
	if err != nil {
		return err
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		return err
	}
	if authoritiesDigest != fleet.AuthoritiesDigest || declarationsDigest != fleet.DeclarationsDigest {
		return errors.New("FuseKit runtime: desired topology fleet digest mismatch")
	}
	return nil
}
