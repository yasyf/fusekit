// Package convergence coordinates exact tenant activation delivery.
package convergence

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit/causal"
)

const (
	// MaxAwaiting bounds deliveries whose acknowledgements are outstanding.
	MaxAwaiting = 2
	// AckTimeout quarantines a delivery that is not acknowledged.
	AckTimeout = 30 * time.Second
)

var (
	// ErrClosed means the engine no longer accepts work.
	ErrClosed = errors.New("convergence: engine closed")
	// ErrNotSent means the notifier proved no side effect and the same activation remains pending.
	ErrNotSent = errors.New("convergence: activation was not sent")
)

// Delivery is the exact side-effect outcome reported by a notifier.
type Delivery uint8

const (
	// DeliveryNotSent proves no notification side effect occurred.
	DeliveryNotSent Delivery = iota + 1
	// DeliverySent proves the notification was accepted for delivery.
	DeliverySent
	// DeliveryUnknown means the side effect may have occurred and must never be replayed.
	DeliveryUnknown
)

// DeliveryFailure is a bounded typed failure persisted with the outbox row.
type DeliveryFailure struct {
	Code   string
	Detail string
}

// ClaimRequest fences one Pending-to-Delivering transition to the current holder runtime.
type ClaimRequest struct {
	RuntimeGeneration string
	HolderOperation   causal.OperationID
	ClaimToken        causal.OperationID
	ClaimedAt         time.Time
}

// DeliveryClaim is the exact durable receipt returned by a successful claim transaction.
type DeliveryClaim struct {
	Event      causal.ActivationEvent
	ClaimToken causal.OperationID
	Attempt    uint64
}

// DeliveryResult settles the synchronous notification call for one claimed activation.
type DeliveryResult struct {
	Key         causal.ActivationKey
	ClaimToken  causal.OperationID
	Outcome     Delivery
	AckDeadline time.Time
	Failure     DeliveryFailure
}

// Store is the catalog-owned durable activation scheduler.
type Store interface {
	RecoverDeliveries(context.Context, string, time.Time) error
	ClaimDelivery(context.Context, ClaimRequest) (*DeliveryClaim, error)
	RecordDelivery(context.Context, DeliveryResult) error
	AcknowledgeDelivery(context.Context, causal.ActivationAck) error
	QuarantineExpired(context.Context, time.Time) error
}

// Notifier signals one exact presentation activation.
type Notifier interface {
	Notify(context.Context, causal.ActivationEvent) (Delivery, error)
}

// Clock supplies scheduler timestamps.
type Clock interface {
	Now() time.Time
}

// TokenSource mints one unpredictable claim token.
type TokenSource func() (causal.OperationID, error)

// Config contains the exact external seams required by Engine.
type Config struct {
	Store             Store
	Notifier          Notifier
	Clock             Clock
	RuntimeGeneration string
	HolderOperation   causal.OperationID
	NewClaimToken     TokenSource
}

func randomToken() (causal.OperationID, error) {
	var token causal.OperationID
	if _, err := rand.Read(token[:]); err != nil {
		return causal.OperationID{}, fmt.Errorf("read randomness: %w", err)
	}
	return token, nil
}
