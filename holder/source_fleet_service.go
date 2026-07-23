package holder

import (
	"context"
	"errors"

	"github.com/yasyf/fusekit/catalog"
)

type desiredSourceFleetStore interface {
	PublishDesiredSourceFleet(
		context.Context,
		catalog.PublishDesiredSourceFleetRequest,
	) (catalog.DesiredSourceAuthorityFleetState, error)
	DesiredSourceFleetPage(
		context.Context,
		catalog.DesiredSourceFleetPageRequest,
	) (catalog.DesiredSourceFleetPage, error)
}

func (s sourceFleetService) DesiredSourceFleetPage(
	ctx context.Context,
	request catalog.DesiredSourceFleetPageRequest,
) (catalog.DesiredSourceFleetPage, error) {
	if s.store == nil || s.owner == "" {
		return catalog.DesiredSourceFleetPage{}, errors.New("FuseKit runtime: source fleet service is incomplete")
	}
	if request.Owner != s.owner {
		return catalog.DesiredSourceFleetPage{}, errors.New("FuseKit runtime: source fleet request is not for the immutable product owner")
	}
	return s.store.DesiredSourceFleetPage(ctx, request)
}

type sourceFleetService struct {
	store    desiredSourceFleetStore
	topology *topologyController
	owner    catalog.SourceAuthorityFleetOwnerID
}

func (s sourceFleetService) PublishDesiredSourceFleet(
	ctx context.Context,
	request catalog.PublishDesiredSourceFleetRequest,
) (catalog.DesiredSourceAuthorityFleetState, error) {
	if s.store == nil || s.topology == nil || s.owner == "" {
		return catalog.DesiredSourceAuthorityFleetState{}, errors.New("FuseKit runtime: source fleet service is incomplete")
	}
	if request.Owner != s.owner {
		return catalog.DesiredSourceAuthorityFleetState{}, errors.New("FuseKit runtime: source fleet request is not for the immutable product owner")
	}
	desired, err := s.store.PublishDesiredSourceFleet(ctx, request)
	if err != nil {
		return catalog.DesiredSourceAuthorityFleetState{}, err
	}
	if err := s.topology.AwaitSourceFleetApplied(ctx, desired); err != nil {
		return catalog.DesiredSourceAuthorityFleetState{}, err
	}
	return desired, nil
}
