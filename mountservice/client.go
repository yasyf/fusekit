package mountservice

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/transportproto"
)

// Client owns one persistent daemonkit session for tenant lifecycle operations.
type Client struct {
	wire *wire.Client
	owns bool

	nativeMu sync.Mutex
	native   *NativeBinding
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
	c.nativeMu.Lock()
	native := c.native
	c.nativeMu.Unlock()
	if native != nil {
		return native.Close()
	}
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

// ProvisionTenant durably provisions one tenant under authenticated server ownership.
func (c *Client) ProvisionTenant(ctx context.Context, id catalog.TenantID, definition mountproto.TenantDefinition) (mountproto.ProvisionTenantResponse, error) {
	var response mountproto.ProvisionTenantResponse
	err := c.unary(ctx, mountproto.OperationTenantProvision, id, mountproto.ProvisionTenantRequest{
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

// State returns the authenticated owner's exact durable tenant state.
func (c *Client) State(ctx context.Context, id catalog.TenantID) (mountproto.StateResponse, error) {
	var response mountproto.StateResponse
	err := c.unary(ctx, mountproto.OperationTenantState, id, mountproto.StateRequest{
		Protocol: mountproto.Version,
	}, &response)
	return response, err
}

// RuntimeHealth returns exact holder activation and native mount readiness.
func (c *Client) RuntimeHealth(ctx context.Context) (mountproto.RuntimeHealthResponse, error) {
	var response mountproto.RuntimeHealthResponse
	err := c.unaryRuntime(ctx, mountproto.OperationRuntimeHealth, mountproto.RuntimeHealthRequest{
		Protocol: mountproto.Version,
	}, &response)
	return response, err
}

// NativeBinding owns the authenticated native-child session until Close.
type NativeBinding struct {
	client *Client

	closeOnce sync.Once
	closeErr  error
}

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
	binding := &NativeBinding{client: c}
	c.nativeMu.Lock()
	c.native = binding
	c.nativeMu.Unlock()
	return binding, nil
}

// Close acknowledges exact server-side settlement before closing the session.
func (b *NativeBinding) Close() error {
	if b == nil || b.client == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		unbindErr := b.client.NativeUnbind(context.Background())
		b.closeErr = errors.Join(unbindErr, b.client.wire.Close())
	})
	return b.closeErr
}

// NativeMounted asks the holder to drive an external traversal through the
// bound child's exact mounted root.
func (c *Client) NativeMounted(ctx context.Context, identity NativeMountIdentity) error {
	var response mountproto.NativeMountedResponse
	return c.unaryNative(ctx, mountproto.OperationNativeMounted, mountproto.NativeMountedRequest{
		Protocol: mountproto.Version, Mount: protocolNativeMountIdentity(identity),
	}, &response)
}

// NativeReady proves that the holder-driven traversal reached the child's catalog callbacks.
func (c *Client) NativeReady(ctx context.Context, proof NativeMountProof) error {
	var response mountproto.NativeReadyResponse
	return c.unaryNative(ctx, mountproto.OperationNativeReady, mountproto.NativeReadyRequest{
		Protocol: mountproto.Version, Mount: protocolNativeMountProof(proof),
	}, &response)
}

// NativeUnbind settles the bound native session without closing its transport.
func (c *Client) NativeUnbind(ctx context.Context) error {
	var response mountproto.NativeUnbindResponse
	return c.unaryNative(
		ctx,
		mountproto.OperationNativeUnbind,
		mountproto.NativeUnbindRequest{Protocol: mountproto.Version},
		&response,
	)
}

// NativeRoutePage returns one version-fenced bounded native route page.
func (c *Client) NativeRoutePage(
	ctx context.Context,
	snapshot uint64,
	after string,
	limit uint16,
) (mountproto.NativeRoutePageResponse, error) {
	var response mountproto.NativeRoutePageResponse
	err := c.unaryNative(ctx, mountproto.OperationNativeRoutePage, mountproto.NativeRoutePageRequest{
		Protocol: mountproto.Version, Snapshot: snapshot, After: after, Limit: limit,
	}, &response)
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

// NativeSnapshotOpen opens one worker-owned exact-revision snapshot handle.
func (c *Client) NativeSnapshotOpen(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, id catalog.ObjectID, revision catalog.Revision) (mountproto.NativeSnapshotOpenResponse, error) {
	var response mountproto.NativeSnapshotOpenResponse
	err := c.unaryNative(ctx, mountproto.OperationNativeSnapshotOpen, mountproto.NativeSnapshotOpenRequest{
		Protocol: mountproto.Version, TenantID: mountproto.TenantID(tenant), Generation: uint64(generation),
		ObjectID: id.String(), Revision: uint64(revision),
	}, &response)
	return response, err
}

// NativeSnapshotRead reads one bounded range from a worker-owned snapshot.
func (c *Client) NativeSnapshotRead(ctx context.Context, handle string, offset int64, length int) (mountproto.NativeSnapshotReadResponse, error) {
	var response mountproto.NativeSnapshotReadResponse
	if length <= 0 || length > nativeIOChunkLimit {
		return response, fmt.Errorf("mount service: native snapshot read length %d is invalid", length)
	}
	err := c.unaryNative(ctx, mountproto.OperationNativeSnapshotRead, mountproto.NativeSnapshotReadRequest{
		Protocol: mountproto.Version, Handle: handle, Offset: offset, Length: uint32(length),
	}, &response)
	return response, err
}

// NativeSnapshotClose releases one worker-owned snapshot.
func (c *Client) NativeSnapshotClose(ctx context.Context, handle string) error {
	var response mountproto.NativeSnapshotCloseResponse
	return c.unaryNative(ctx, mountproto.OperationNativeSnapshotClose, mountproto.NativeSnapshotCloseRequest{
		Protocol: mountproto.Version, Handle: handle,
	}, &response)
}

// NativeWriteOpen opens one worker-owned mutable stage seeded from an exact revision.
func (c *Client) NativeWriteOpen(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, id catalog.ObjectID, revision catalog.Revision) (mountproto.NativeWriteOpenResponse, error) {
	var response mountproto.NativeWriteOpenResponse
	err := c.unaryNative(ctx, mountproto.OperationNativeWriteOpen, mountproto.NativeWriteOpenRequest{
		Protocol: mountproto.Version, TenantID: mountproto.TenantID(tenant), Generation: uint64(generation),
		ObjectID: id.String(), Revision: uint64(revision),
	}, &response)
	return response, err
}

// NativeWriteRead reads one bounded range from a mutable stage.
func (c *Client) NativeWriteRead(ctx context.Context, handle string, offset int64, length int) (mountproto.NativeWriteReadResponse, error) {
	var response mountproto.NativeWriteReadResponse
	if length <= 0 || length > nativeIOChunkLimit {
		return response, fmt.Errorf("mount service: native write read length %d is invalid", length)
	}
	err := c.unaryNative(ctx, mountproto.OperationNativeWriteRead, mountproto.NativeWriteReadRequest{
		Protocol: mountproto.Version, Handle: handle, Offset: offset, Length: uint32(length),
	}, &response)
	return response, err
}

// NativeWrite writes one bounded range to a mutable stage.
func (c *Client) NativeWrite(ctx context.Context, handle string, offset int64, data []byte) (int, error) {
	if len(data) == 0 || len(data) > nativeIOChunkLimit {
		return 0, fmt.Errorf("mount service: native write length %d is invalid", len(data))
	}
	var response mountproto.NativeWriteWriteResponse
	err := c.unaryNative(ctx, mountproto.OperationNativeWriteWrite, mountproto.NativeWriteWriteRequest{
		Protocol: mountproto.Version, Handle: handle, Offset: offset, Data: data,
	}, &response)
	return int(response.Written), err
}

// NativeWriteTruncate changes a mutable stage's exact size.
func (c *Client) NativeWriteTruncate(ctx context.Context, handle string, size int64) error {
	var response mountproto.NativeWriteTruncateResponse
	return c.unaryNative(ctx, mountproto.OperationNativeWriteTruncate, mountproto.NativeWriteTruncateRequest{
		Protocol: mountproto.Version, Handle: handle, Size: size,
	}, &response)
}

// NativeWriteSync durably syncs a mutable stage.
func (c *Client) NativeWriteSync(ctx context.Context, handle string) error {
	var response mountproto.NativeWriteSyncResponse
	return c.unaryNative(ctx, mountproto.OperationNativeWriteSync, mountproto.NativeWriteSyncRequest{
		Protocol: mountproto.Version, Handle: handle,
	}, &response)
}

// NativeWriteCommit consumes one dirty stage epoch into a worker-derived catalog mutation.
func (c *Client) NativeWriteCommit(ctx context.Context, handle string) (mountproto.NativeWriteCommitResponse, error) {
	var response mountproto.NativeWriteCommitResponse
	err := c.unaryNative(ctx, mountproto.OperationNativeWriteCommit, mountproto.NativeWriteCommitRequest{
		Protocol: mountproto.Version, Handle: handle,
	}, &response)
	return response, err
}

// NativeWriteAbort discards one mutable stage.
func (c *Client) NativeWriteAbort(ctx context.Context, handle string) error {
	var response mountproto.NativeWriteAbortResponse
	return c.unaryNative(ctx, mountproto.OperationNativeWriteAbort, mountproto.NativeWriteAbortRequest{
		Protocol: mountproto.Version, Handle: handle,
	}, &response)
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
	return c.unaryRuntime(ctx, operation, request, response)
}

func (c *Client) unaryRuntime(ctx context.Context, operation mountproto.Operation, request, response any) error {
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
	case *mountproto.ProvisionTenantResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.ReplaceTenantResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.RemoveTenantResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.StateResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.RuntimeHealthResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeBindResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeMountedResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeReadyResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeUnbindResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeRoutePageResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativePinResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeReleaseResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeSnapshotOpenResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeSnapshotReadResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeSnapshotCloseResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeWriteOpenResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeWriteReadResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeWriteWriteResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeWriteTruncateResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeWriteSyncResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeWriteCommitResponse:
		return typed.Code, typed.Message, nil
	case *mountproto.NativeWriteAbortResponse:
		return typed.Code, typed.Message, nil
	default:
		return "", "", fmt.Errorf("mount service: unsupported response type %T", response)
	}
}
