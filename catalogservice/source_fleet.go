package catalogservice

import (
	"context"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogproto"
)

func (s *Server) handlePublishDesiredSourceFleet(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.PublishDesiredSourceFleetRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.PublishDesiredSourceFleetResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()),
		})
	}
	if _, _, _, err := s.authorize(ctx, request, catalogproto.OperationSourceAuthorityPublishDesiredFleet, 0, false); err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.PublishDesiredSourceFleetResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	publish, err := catalogPublishDesiredSourceFleetRequest(input)
	if err != nil {
		return encoded(catalogproto.PublishDesiredSourceFleetResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()),
		})
	}
	state, err := s.core.SourceFleets.PublishDesiredSourceFleet(ctx, publish)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.PublishDesiredSourceFleetResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	encodedState, err := protocolDesiredSourceFleetState(state)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.PublishDesiredSourceFleetResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(catalogproto.PublishDesiredSourceFleetResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, State: &encodedState,
	})
}

func (s *Server) handleReadDesiredSourceFleet(ctx context.Context, request wire.Request) (any, error) {
	var input catalogproto.ReadDesiredSourceFleetRequest
	if err := catalogproto.Decode(request.Payload, &input); err != nil {
		return encoded(catalogproto.ReadDesiredSourceFleetResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()),
		})
	}
	if _, _, _, err := s.authorize(ctx, request, catalogproto.OperationSourceAuthorityReadDesiredFleet, 0, false); err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ReadDesiredSourceFleetResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	pageRequest, err := catalogDesiredSourceFleetPageRequest(input)
	if err != nil {
		return encoded(catalogproto.ReadDesiredSourceFleetResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()),
		})
	}
	page, err := s.core.SourceFleets.DesiredSourceFleetPage(ctx, pageRequest)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ReadDesiredSourceFleetResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	response, err := protocolDesiredSourceFleetPage(page)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.ReadDesiredSourceFleetResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	return encoded(response)
}
