package convergence

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/fusekit/causal"
)

// Engine coordinates bounded activation delivery over the catalog's durable outbox.
type Engine struct {
	store     Store
	notifier  Notifier
	clock     Clock
	runtime   string
	operation causal.OperationID
	newToken  TokenSource

	mu     sync.Mutex
	closed bool
}

// New constructs an activation scheduler and fences deliveries owned by dead runtimes.
func New(ctx context.Context, config Config) (*Engine, error) {
	if config.Store == nil || config.Notifier == nil || config.RuntimeGeneration == "" ||
		config.HolderOperation == (causal.OperationID{}) {
		return nil, errors.New("convergence: incomplete config")
	}
	if config.Clock == nil {
		config.Clock = realClock{}
	}
	if config.NewClaimToken == nil {
		config.NewClaimToken = randomToken
	}
	engine := &Engine{
		store: config.Store, notifier: config.Notifier, clock: config.Clock,
		runtime: config.RuntimeGeneration, operation: config.HolderOperation,
		newToken: config.NewClaimToken,
	}
	if err := engine.store.RecoverDeliveries(ctx, engine.runtime, engine.clock.Now()); err != nil {
		return nil, fmt.Errorf("convergence: recover deliveries: %w", err)
	}
	return engine, nil
}

// Pump fills the globally bounded delivery window from the durable outbox.
func (e *Engine) Pump(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	if err := e.store.QuarantineExpired(ctx, e.clock.Now()); err != nil {
		return fmt.Errorf("convergence: quarantine expired deliveries: %w", err)
	}
	for {
		token, err := e.newToken()
		if err != nil {
			return fmt.Errorf("convergence: mint claim token: %w", err)
		}
		claim, err := e.store.ClaimDelivery(ctx, ClaimRequest{
			RuntimeGeneration: e.runtime,
			HolderOperation:   e.operation,
			ClaimToken:        token,
			ClaimedAt:         e.clock.Now(),
		})
		if err != nil {
			return fmt.Errorf("convergence: claim activation delivery: %w", err)
		}
		if claim == nil {
			return nil
		}
		if err := causal.ValidateActivationEvent(claim.Event); err != nil {
			return fmt.Errorf("convergence: invalid claimed event: %w", err)
		}
		if claim.ClaimToken != token || claim.Attempt == 0 {
			return errors.New("convergence: invalid claim receipt")
		}
		delivery, notifyErr := e.notifier.Notify(ctx, claim.Event)
		if delivery != DeliveryNotSent && delivery != DeliverySent && delivery != DeliveryUnknown && notifyErr == nil {
			notifyErr = errors.New("convergence: notifier returned an invalid delivery outcome")
		}
		outcome, failure := normalizeDelivery(delivery, notifyErr)
		result := DeliveryResult{
			Key: claim.Event.Key(), ClaimToken: claim.ClaimToken,
			Outcome: outcome, Failure: failure,
		}
		if outcome == DeliverySent || outcome == DeliveryUnknown {
			result.AckDeadline = e.clock.Now().Add(AckTimeout)
		}
		if err := e.store.RecordDelivery(ctx, result); err != nil {
			return fmt.Errorf("convergence: record activation delivery: %w", err)
		}
		if notifyErr != nil {
			return fmt.Errorf("convergence: notify activation: %w", notifyErr)
		}
		if outcome == DeliveryNotSent {
			return ErrNotSent
		}
	}
}

// Acknowledge settles an exact observed activation and pumps the next pending work.
func (e *Engine) Acknowledge(ctx context.Context, ack causal.ActivationAck) error {
	if err := causal.ValidateActivationAck(ack); err != nil {
		return fmt.Errorf("convergence: invalid acknowledgement: %w", err)
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	if err := e.store.AcknowledgeDelivery(ctx, ack); err != nil {
		e.mu.Unlock()
		return fmt.Errorf("convergence: acknowledge activation: %w", err)
	}
	e.mu.Unlock()
	return e.Pump(ctx)
}

// Tick quarantines expired acknowledgements and pumps newly eligible work.
func (e *Engine) Tick(ctx context.Context) error {
	return e.Pump(ctx)
}

// Close stops admission. Durable deliveries remain owned by the catalog.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	e.closed = true
	return nil
}

func normalizeDelivery(delivery Delivery, err error) (Delivery, DeliveryFailure) {
	switch delivery {
	case DeliveryNotSent:
		return DeliveryNotSent, classifyFailure(err)
	case DeliverySent:
		if err == nil {
			return DeliverySent, DeliveryFailure{}
		}
		return DeliveryUnknown, classifyFailure(err)
	case DeliveryUnknown:
		return DeliveryUnknown, classifyFailure(err)
	default:
		return DeliveryUnknown, DeliveryFailure{Code: "invalid_delivery", Detail: "notifier returned an invalid delivery outcome"}
	}
}

func classifyFailure(err error) DeliveryFailure {
	if err == nil {
		return DeliveryFailure{}
	}
	var typed interface{ DeliveryErrorCode() string }
	code := "notify_failed"
	if errors.As(err, &typed) && typed.DeliveryErrorCode() != "" {
		code = typed.DeliveryErrorCode()
	}
	return DeliveryFailure{Code: code, Detail: err.Error()}
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
