package mountservice

import (
	"context"
	"fmt"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
)

// Client owns one persistent daemonkit session for tenant lifecycle operations.
type Client struct {
	wire *wire.Client
}

// NewClient opens one exact-build persistent daemonkit session.
func NewClient(ctx context.Context, config wire.ClientConfig) (*Client, error) {
	if config.Build != "" && config.Build != mountproto.Build {
		return nil, fmt.Errorf("mount service: daemonkit build %q does not match schema build %q", config.Build, mountproto.Build)
	}
	config.Build = mountproto.Build
	client, err := wire.NewClient(ctx, config)
	if err != nil {
		return nil, err
	}
	return &Client{wire: client}, nil
}

// Close closes the persistent daemonkit session.
func (c *Client) Close() error { return c.wire.Close() }

// RegisterTenant registers one tenant definition under authenticated server ownership.
func (c *Client) RegisterTenant(ctx context.Context, id catalog.TenantID, definition mountproto.TenantDefinition) (mountproto.RegisterTenantResponse, error) {
	var response mountproto.RegisterTenantResponse
	err := c.unary(ctx, mountproto.OperationTenantRegister, id, mountproto.RegisterTenantRequest{
		Protocol: mountproto.Version, Definition: definition,
	}, &response)
	return response, err
}

// ReplaceTenant replaces one exact generation under authenticated server ownership.
func (c *Client) ReplaceTenant(ctx context.Context, id catalog.TenantID, expected catalog.Generation, definition mountproto.TenantDefinition) (mountproto.ReplaceTenantResponse, error) {
	var response mountproto.ReplaceTenantResponse
	err := c.unary(ctx, mountproto.OperationTenantReplace, id, mountproto.ReplaceTenantRequest{
		Protocol: mountproto.Version, ExpectedGeneration: uint64(expected), Definition: definition,
	}, &response)
	return response, err
}

// RemoveTenant removes one exact tenant generation.
func (c *Client) RemoveTenant(ctx context.Context, id catalog.TenantID, generation catalog.Generation) (mountproto.RemoveTenantResponse, error) {
	var response mountproto.RemoveTenantResponse
	err := c.unary(ctx, mountproto.OperationTenantRemove, id, mountproto.RemoveTenantRequest{
		Protocol: mountproto.Version, Generation: uint64(generation),
	}, &response)
	return response, err
}

// PrepareTenant converges one exact tenant generation to a revision.
func (c *Client) PrepareTenant(ctx context.Context, id catalog.TenantID, generation catalog.Generation, revision catalog.Revision) (mountproto.PrepareTenantResponse, error) {
	var response mountproto.PrepareTenantResponse
	err := c.unary(ctx, mountproto.OperationTenantPrepare, id, mountproto.PrepareTenantRequest{
		Protocol: mountproto.Version, Generation: uint64(generation), Revision: uint64(revision),
	}, &response)
	return response, err
}

// State returns the durable state of one exact tenant generation.
func (c *Client) State(ctx context.Context, id catalog.TenantID, generation catalog.Generation) (mountproto.StateResponse, error) {
	var response mountproto.StateResponse
	err := c.unary(ctx, mountproto.OperationTenantState, id, mountproto.StateRequest{
		Protocol: mountproto.Version, Generation: uint64(generation),
	}, &response)
	return response, err
}

func (c *Client) unary(ctx context.Context, operation mountproto.Operation, tenantID catalog.TenantID, request, response any) error {
	validatedTenant, err := catalog.NewTenantID(string(tenantID))
	if err != nil {
		return fmt.Errorf("mount service: tenant id: %w", err)
	}
	payload, err := mountproto.Encode(request)
	if err != nil {
		return err
	}
	result, err := c.wire.Call(ctx, wire.Op(operation), string(validatedTenant), payload)
	if err != nil {
		return err
	}
	if err := decodeWireResult(result, response); err != nil {
		return err
	}
	code, message, err := responseHeader(response)
	if err != nil {
		return err
	}
	if code == mountproto.ErrorCodeOk {
		return nil
	}
	return &RemoteError{Code: code, Message: message}
}

func decodeWireResult(result wire.Result, response any) error {
	if result.Outcome != wire.Delivered || result.Response.Rejected {
		message := result.Response.Reason
		if message == "" {
			message = "mount service: daemonkit request was not delivered"
		}
		return &TransportError{Outcome: result.Outcome, Message: message}
	}
	if result.Response.Err != "" {
		return &TransportError{Outcome: result.Outcome, Message: result.Response.Err}
	}
	if len(result.Response.Payload) == 0 {
		return &TransportError{Outcome: result.Outcome, Message: "mount service: daemonkit response has no payload"}
	}
	return mountproto.Decode(result.Response.Payload, response)
}

func responseHeader(response any) (mountproto.ErrorCode, string, error) {
	switch typed := response.(type) {
	case *mountproto.RegisterTenantResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.ReplaceTenantResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.RemoveTenantResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.PrepareTenantResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.StateResponse:
		return typed.Code, typed.Message, nil
	default:
		return "", "", fmt.Errorf("mount service: unsupported response type %T", response)
	}
}
