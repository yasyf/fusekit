package mountservice

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/transportproto"
)

// RuntimeHealthObservation returns the immutable read-only runtime-health route.
func RuntimeHealthObservation(provider RuntimeHealthProvider, authorizer Authorizer) (wire.ObservationRoute, error) {
	if provider == nil || authorizer == nil {
		return wire.ObservationRoute{}, errors.New("mount service: runtime health provider and authorizer are required")
	}
	return wire.ObservationRoute{
		Op:               wire.Op(mountproto.OperationRuntimeHealth),
		MaxResponseBytes: mountproto.RuntimeHealthMaxResponseBytes,
		Handler: func(ctx context.Context, request wire.ObservationRequest) (wire.ObservationResponse, error) {
			return handleRuntimeHealthObservation(ctx, request, provider, authorizer)
		},
	}, nil
}

func handleRuntimeHealthObservation(
	ctx context.Context,
	request wire.ObservationRequest,
	provider RuntimeHealthProvider,
	authorizer Authorizer,
) (wire.ObservationResponse, error) {
	respond := func(value mountproto.RuntimeHealthResponse) (wire.ObservationResponse, error) {
		raw, err := mountproto.Encode(value)
		return wire.ObservationResponse{Payload: json.RawMessage(raw)}, err
	}
	var input mountproto.RuntimeHealthRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return respond(mountproto.RuntimeHealthResponse{
			Protocol: mountproto.Version, Code: mountproto.ErrorCodeInvalidRequest, Message: err.Error(),
		})
	}
	if request.Op != wire.Op(mountproto.OperationRuntimeHealth) || request.Tenant != "" || request.WireBuild != transportproto.WireBuild {
		return respond(mountproto.RuntimeHealthResponse{
			Protocol: mountproto.Version, Code: mountproto.ErrorCodeUnauthorized, Message: ErrUnauthorized.Error(),
		})
	}
	identity := ObservationIdentity{Peer: request.Peer, WireBuild: request.WireBuild}
	if err := authorizer.AuthorizeObservation(ctx, identity, mountproto.OperationRuntimeHealth); err != nil {
		code, message := applicationError(err)
		return respond(mountproto.RuntimeHealthResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	health, err := provider.Health(ctx)
	if err != nil {
		code, message := applicationError(err)
		return respond(mountproto.RuntimeHealthResponse{Protocol: mountproto.Version, Code: code, Message: message})
	}
	response := mountproto.RuntimeHealthResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		RuntimeBuild: health.RuntimeBuild, RuntimeProtocol: health.RuntimeProtocol,
		RuntimePID: health.RuntimePID, ProcessGeneration: health.ProcessGeneration,
		ActivationGeneration: health.ActivationGeneration,
		State:                health.State, Draining: health.Draining, Busy: health.Busy, Ready: health.Ready,
		ReadinessPhase: health.ReadinessPhase, ReadinessStep: health.ReadinessStep,
		NativePhase: health.NativePhase, BrokerPhase: health.BrokerPhase,
	}
	if health.NativeMount != nil {
		proof := protocolNativeMountProof(*health.NativeMount)
		response.NativeMount = &proof
	}
	return respond(response)
}
