package catalogservice

import (
	"context"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
)

type fakeCriticalFetches struct{}

func (fakeCriticalFetches) ResolveCriticalFetch(
	context.Context,
	Identity,
	Authorization,
	catalog.TenantID,
	catalogproto.ResolveCriticalFetchRequest,
) (*catalogproto.CriticalFetchContext, error) {
	return nil, nil
}

func (fakeCriticalFetches) AckCriticalFetch(
	context.Context,
	Identity,
	Authorization,
	catalog.TenantID,
	catalogproto.AckCriticalFetchRequest,
) error {
	return nil
}
