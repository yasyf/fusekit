//go:build darwin && cgo && fuse

package mountmux

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/tenant"
)

const nativeCallbackTimeout = 30 * time.Second

// RemoteResolver retains holder-owned generation pins on the native child's session.
type RemoteResolver struct {
	client *mountservice.Client
}

// NewRemoteResolver binds route resolution to an existing exact-suite mount client.
func NewRemoteResolver(client *mountservice.Client) (*RemoteResolver, error) {
	if client == nil {
		return nil, errors.New("mountmux: nil remote mount client")
	}
	return &RemoteResolver{client: client}, nil
}

// RoutePage returns one holder-owned version-fenced route page.
func (r *RemoteResolver) RoutePage(ctx context.Context, cursor RouteCursor, limit int) (RoutePage, error) {
	if limit <= 0 || limit > MaxRoutePageSize {
		return RoutePage{}, fmt.Errorf("%w: invalid route page limit", ErrInvalidRoute)
	}
	callContext, cancel := callbackContext(ctx)
	defer cancel()
	response, err := r.client.NativeRoutePage(callContext, cursor.Snapshot, cursor.After, uint16(limit))
	if err != nil {
		return RoutePage{}, remoteMountError(err)
	}
	if cursor.Snapshot != 0 && response.Snapshot != cursor.Snapshot {
		return RoutePage{}, fmt.Errorf("%w: native route snapshot changed", catalog.ErrIntegrity)
	}
	routes := make([]Route, len(response.Routes))
	for index, route := range response.Routes {
		tenantID, err := catalog.NewTenantID(string(route.TenantID))
		if err != nil {
			return RoutePage{}, fmt.Errorf("%w: native route tenant: %v", catalog.ErrIntegrity, err)
		}
		routes[index] = Route{Tenant: tenantID, Generation: catalog.Generation(route.Generation), Name: route.Name}
	}
	page := RoutePage{Snapshot: response.Snapshot, Routes: routes}
	if response.Next != "" {
		page.Next = &RouteCursor{Snapshot: response.Snapshot, After: response.Next}
	}
	return page, nil
}

// Pin retains one exact holder-side tenant generation until Release.
func (r *RemoteResolver) Pin(ctx context.Context, name string) (*PinnedRoute, error) {
	callContext, cancel := callbackContext(ctx)
	response, err := r.client.NativePin(callContext, name)
	cancel()
	if err != nil {
		return nil, remoteMountError(err)
	}
	if response.Route == nil || response.Definition == nil || response.OwnerID == "" || response.Token == "" {
		return nil, fmt.Errorf("%w: native pin response is incomplete", catalog.ErrIntegrity)
	}
	tenantID, err := catalog.NewTenantID(string(response.Route.TenantID))
	if err != nil {
		return nil, fmt.Errorf("%w: native pin tenant: %v", catalog.ErrIntegrity, err)
	}
	spec, err := mountservice.DecodeTenantDefinition(tenant.OwnerID(response.OwnerID), tenantID, *response.Definition)
	if err != nil {
		return nil, fmt.Errorf("%w: native pin definition: %v", catalog.ErrIntegrity, err)
	}
	route := Route{Tenant: tenantID, Generation: catalog.Generation(response.Route.Generation), Name: response.Route.Name}
	if route.Tenant != spec.ID || route.Generation != spec.Generation {
		return nil, fmt.Errorf("%w: native pin route and definition differ", catalog.ErrIntegrity)
	}
	token := response.Token
	return &PinnedRoute{Route: route, Spec: spec, release: func() error {
		releaseContext, releaseCancel := context.WithTimeout(context.Background(), nativeCallbackTimeout)
		defer releaseCancel()
		response, err := r.client.NativeRelease(releaseContext, token)
		if err != nil {
			return remoteMountError(err)
		}
		if response.Token != token {
			return fmt.Errorf("%w: native release acknowledged another token", catalog.ErrIntegrity)
		}
		return nil
	}}, nil
}

func callbackContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, nativeCallbackTimeout)
}

func remoteMountError(err error) error {
	if err == nil {
		return nil
	}
	var remote *mountservice.RemoteError
	if !errors.As(err, &remote) {
		return err
	}
	var cause error
	switch remote.Code {
	case mountproto.ErrorCodeNotFound:
		cause = catalog.ErrNotFound
	case mountproto.ErrorCodeConflict:
		cause = tenant.ErrGenerationConflict
	case mountproto.ErrorCodeQuarantined:
		return err
	case mountproto.ErrorCodeCanceled:
		cause = context.Canceled
	case mountproto.ErrorCodeInvalidRequest:
		cause = catalog.ErrInvalidObject
	default:
		return err
	}
	return fmt.Errorf("%w: %s", cause, remote.Message)
}

var _ routeSource = (*RemoteResolver)(nil)
