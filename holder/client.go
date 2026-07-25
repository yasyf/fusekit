package holder

import (
	"fmt"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/transportproto"
)

// ServiceClient owns one generation-aware transport shared by FuseKit's
// tenant lifecycle and catalog services.
type ServiceClient struct {
	transport *wire.ServiceClient
	mount     *mountservice.ServiceClient
	catalog   *catalogservice.ServiceClient
}

// NewServiceClient constructs one lazy exact-suite client across holder generations.
func NewServiceClient(config wire.RuntimeClientConfig) (*ServiceClient, error) {
	if config.Client.WireBuild != "" && config.Client.WireBuild != transportproto.WireBuild {
		return nil, fmt.Errorf(
			"FuseKit service client: daemonkit build %q does not match transport suite %q",
			config.Client.WireBuild,
			transportproto.WireBuild,
		)
	}
	config.Client.WireBuild = transportproto.WireBuild
	transport, err := wire.NewServiceClient(config)
	if err != nil {
		return nil, err
	}
	mount, err := mountservice.NewServiceClientOn(transport)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	catalog, err := catalogservice.NewServiceClientOn(transport)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	return &ServiceClient{transport: transport, mount: mount, catalog: catalog}, nil
}

// Mount returns the shared generation-aware tenant lifecycle client.
func (c *ServiceClient) Mount() *mountservice.ServiceClient { return c.mount }

// Catalog returns the shared generation-aware unary catalog client.
func (c *ServiceClient) Catalog() *catalogservice.ServiceClient { return c.catalog }

// Close permanently closes every current or retiring holder session.
func (c *ServiceClient) Close() error { return c.transport.Close() }
