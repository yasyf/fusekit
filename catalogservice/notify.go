package catalogservice

import (
	"context"
	"errors"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogproto"
)

// NotifyActivation pushes one exact event-only activation tuple to an authenticated session.
func NotifyActivation(ctx context.Context, session *wire.AcceptedSession, notification catalogproto.ActivationNotification) error {
	if session == nil {
		return errors.New("catalog service: activation session is nil")
	}
	payload, err := catalogproto.Encode(notification)
	if err != nil {
		return err
	}
	return session.PushEvent(ctx, wire.Event{Topic: string(catalogproto.OperationActivationNotify), Payload: payload})
}
