package catalogservice

import (
	"context"
	"errors"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogproto"
)

// NotifyConvergence pushes one exact event-only convergence tuple to an authenticated session.
func NotifyConvergence(ctx context.Context, session *wire.AcceptedSession, notification catalogproto.ConvergenceNotification) error {
	if session == nil {
		return errors.New("catalog service: convergence session is nil")
	}
	payload, err := catalogproto.Encode(notification)
	if err != nil {
		return err
	}
	return session.PushEvent(ctx, wire.Event{Topic: string(catalogproto.OperationConvergenceNotify), Payload: payload})
}
