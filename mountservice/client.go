package mountservice

import (
	"context"
	"fmt"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/transportproto"
)

// Client owns one persistent daemonkit session for tenant lifecycle operations.
type Client struct {
	wire *wire.Client
	owns bool
}

// NewClient opens one exact-build persistent daemonkit session.
func NewClient(ctx context.Context, config wire.ClientConfig) (*Client, error) {
	if config.Build != "" && config.Build != transportproto.Build {
		return nil, fmt.Errorf("mount service: daemonkit build %q does not match transport suite %q", config.Build, transportproto.Build)
	}
	config.Build = transportproto.Build
	client, err := wire.NewClient(ctx, config)
	if err != nil {
		return nil, err
	}
	return &Client{wire: client, owns: true}, nil
}

// Close closes the persistent daemonkit session.
func (c *Client) Close() error {
	if !c.owns {
		return nil
	}
	return c.wire.Close()
}

// NewClientOn binds mount operations to an existing exact-suite session.
func NewClientOn(client *wire.Client) (*Client, error) {
	if client == nil || client.PeerBuild().Build != transportproto.Build {
		return nil, fmt.Errorf("mount service: exact transport session is required")
	}
	return &Client{wire: client}, nil
}

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

// State returns the durable state of one exact tenant generation.
func (c *Client) State(ctx context.Context, id catalog.TenantID, generation catalog.Generation) (mountproto.StateResponse, error) {
	var response mountproto.StateResponse
	err := c.unary(ctx, mountproto.OperationTenantState, id, mountproto.StateRequest{
		Protocol: mountproto.Version, Generation: uint64(generation),
	}, &response)
	return response, err
}

// NativeBinding owns the authenticated native-child session until Close.
type NativeBinding struct{ client *wire.Client }

// BindNative authenticates this persistent session as the sole native mount child.
func (c *Client) BindNative(ctx context.Context) (*NativeBinding, error) {
	payload, err := mountproto.Encode(mountproto.NativeBindRequest{Protocol: mountproto.Version})
	if err != nil {
		return nil, err
	}
	result, err := c.wire.Call(ctx, wire.Op(mountproto.OperationNativeBind), "", payload)
	if err != nil {
		return nil, err
	}
	var response mountproto.NativeBindResponse
	if err := decodeWireResult(result, &response); err != nil {
		return nil, err
	}
	if response.Code != mountproto.ErrorCodeOk {
		return nil, &RemoteError{Code: response.Code, Message: response.Message}
	}
	return &NativeBinding{client: c.wire}, nil
}

// Close tears down the exact session so server-side pins settle before reuse.
func (b *NativeBinding) Close() error {
	if b == nil || b.client == nil {
		return nil
	}
	return b.client.Close()
}

// NativeReady proves that the bound child established the native root.
func (c *Client) NativeReady(ctx context.Context) error {
	var response mountproto.NativeReadyResponse
	return c.unaryNative(ctx, mountproto.OperationNativeReady, mountproto.NativeReadyRequest{Protocol: mountproto.Version}, &response)
}

// NativeRoutes returns one immutable snapshot of mounted tenant routes.
func (c *Client) NativeRoutes(ctx context.Context) (mountproto.NativeRoutesResponse, error) {
	var response mountproto.NativeRoutesResponse
	err := c.unaryNative(ctx, mountproto.OperationNativeRoutes, mountproto.NativeRoutesRequest{Protocol: mountproto.Version}, &response)
	return response, err
}

// NativePin retains one exact routed tenant generation on this session.
func (c *Client) NativePin(ctx context.Context, name string) (mountproto.NativePinResponse, error) {
	var response mountproto.NativePinResponse
	err := c.unaryNative(ctx, mountproto.OperationNativePin, mountproto.NativePinRequest{Protocol: mountproto.Version, Name: name}, &response)
	return response, err
}

// NativeRelease releases one exact session-owned generation pin.
func (c *Client) NativeRelease(ctx context.Context, token string) (mountproto.NativeReleaseResponse, error) {
	var response mountproto.NativeReleaseResponse
	err := c.unaryNative(ctx, mountproto.OperationNativeRelease, mountproto.NativeReleaseRequest{Protocol: mountproto.Version, Token: token}, &response)
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

func (c *Client) unaryNative(ctx context.Context, operation mountproto.Operation, request, response any) error {
	payload, err := mountproto.Encode(request)
	if err != nil {
		return err
	}
	result, err := c.wire.Call(ctx, wire.Op(operation), "", payload)
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
	case *mountproto.StateResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeBindResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeReadyResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeRoutesResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativePinResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeReleaseResponse:
		return typed.Code, typed.Message, nil
	default:
		return "", "", fmt.Errorf("mount service: unsupported response type %T", response)
	}
}
