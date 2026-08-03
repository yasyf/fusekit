package catalogservice

import (
	"context"
	"sort"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
)

// activationInboxBound caps one session's buffered notifications per domain
// and generation; the durable outbox redelivers anything an overflow drops.
const activationInboxBound = 256

type activationKey struct {
	domain     catalogproto.DomainID
	generation uint64
}

// NotifyActivation enqueues one exact activation tuple into the session's
// inbox and wakes any activation.poll parked on it. v0.21 has no server push:
// delivery is the poll's own read, settled by the existing activation.ack, so
// enqueueing never proves delivery.
func (s *Server) NotifyActivation(ctx context.Context, session daemonkit.Session, notification catalogproto.ActivationNotification) error {
	if err := catalogproto.Validate(notification); err != nil {
		return err
	}
	state := s.bindSession(session)
	key := activationKey{domain: notification.DomainID, generation: notification.Generation}
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return errSessionReleased
	}
	entries := state.inbox[key]
	replaced := false
	for index, entry := range entries {
		if entry.ActivationRevision == notification.ActivationRevision {
			entries[index] = notification
			replaced = true
			break
		}
	}
	if !replaced {
		entries = append(entries, notification)
		sort.Slice(entries, func(left, right int) bool {
			return entries[left].ActivationRevision < entries[right].ActivationRevision
		})
		if len(entries) > activationInboxBound {
			entries = entries[len(entries)-activationInboxBound:]
		}
	}
	state.inbox[key] = entries
	wake := state.wake
	state.wake = make(chan struct{})
	state.mu.Unlock()
	close(wake)
	return nil
}

// pageActivations trims entries the cursor already acknowledged and returns
// the next page, with the wake channel to park on when the page is empty.
func (state *sessionState) pageActivations(key activationKey, cursor uint64, limit uint32) ([]catalogproto.ActivationNotification, uint64, <-chan struct{}) {
	state.mu.Lock()
	defer state.mu.Unlock()
	entries := state.inbox[key]
	kept := entries[:0]
	for _, entry := range entries {
		if entry.ActivationRevision > cursor {
			kept = append(kept, entry)
		}
	}
	state.inbox[key] = kept
	if len(kept) == 0 {
		return nil, 0, state.wake
	}
	page := kept
	if len(page) > int(limit) {
		page = page[:limit]
	}
	copied := append([]catalogproto.ActivationNotification(nil), page...)
	return copied, copied[len(copied)-1].ActivationRevision, nil
}

func (s *Server) handleActivationPoll(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input catalogproto.PollActivationsRequest
	if err := catalogproto.Decode(request.Body, &input); err != nil {
		return encoded(catalogproto.PollActivationsResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeInvalidRequest, Message: boundedErrorMessage(err.Error()),
		})
	}
	_, _, identity, err := s.authorize(ctx, request, catalogproto.OperationActivationPoll, catalog.Generation(input.Generation), true)
	if err != nil {
		code, message := applicationError(err)
		return encoded(catalogproto.PollActivationsResponse{Protocol: catalogproto.Version, Code: code, Message: message})
	}
	state := s.bindSession(identity.Session)
	key := activationKey{domain: input.DomainID, generation: input.Generation}
	timer := time.NewTimer(pollWait(ctx, input.WaitMillis))
	defer timer.Stop()
	for {
		notifications, next, wake := state.pageActivations(key, input.Cursor, input.Limit)
		if len(notifications) > 0 {
			return encoded(catalogproto.PollActivationsResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				Notifications: notifications, NextCursor: next,
			})
		}
		select {
		case <-wake:
		case <-timer.C:
			return encoded(catalogproto.PollActivationsResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				Notifications: []catalogproto.ActivationNotification{},
			})
		case <-s.draining():
			return encoded(catalogproto.PollActivationsResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				Notifications: []catalogproto.ActivationNotification{},
			})
		case <-identity.Session.Disconnected():
			return encoded(catalogproto.PollActivationsResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				Notifications: []catalogproto.ActivationNotification{},
			})
		case <-ctx.Done():
			return encoded(catalogproto.PollActivationsResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				Notifications: []catalogproto.ActivationNotification{},
			})
		}
	}
}
