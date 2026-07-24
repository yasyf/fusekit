package catalogservice

import (
	"context"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
)

func (s *Server) handleResolveCriticalFetch(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.ResolveCriticalFetchRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.ResolveCriticalFetchResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, identity, err := s.authorize(
		ctx, request, catalogproto.OperationCriticalReadinessResolve,
		catalog.Generation(input.Generation), true,
	)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ResolveCriticalFetchResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if !authorization.Route.Forwarded || authorization.Route.Domain == "" {
		return encoded(catalogproto.ResolveCriticalFetchResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: "critical fetch resolution has no broker-bound domain"})
	}
	resolved, err := s.fileProvider.CriticalFetches.ResolveCriticalFetch(ctx, identity, authorization, tenant, input)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ResolveCriticalFetchResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.ResolveCriticalFetchResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Context: resolved})
}

func (s *Server) handleAckCriticalFetch(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.AckCriticalFetchRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.AckCriticalFetchResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error())})
	}
	tenant, authorization, identity, err := s.authorize(
		ctx, request, catalogproto.OperationCriticalReadinessFetchAck,
		catalog.Generation(input.Generation), true,
	)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.AckCriticalFetchResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	if !authorization.Route.Forwarded || authorization.Route.Domain == "" {
		return encoded(catalogproto.AckCriticalFetchResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: "critical fetch acknowledgement has no broker-bound domain"})
	}
	if err := s.fileProvider.CriticalFetches.AckCriticalFetch(ctx, identity, authorization, tenant, input); err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.AckCriticalFetchResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.AckCriticalFetchResponse{Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk})
}
