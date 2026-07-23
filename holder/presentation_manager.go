package holder

import (
	"context"
	"errors"
	"sync"
	"time"
)

type presentationManager struct {
	lifetime context.Context
	cancel   context.CancelFunc
	native   *presentationStart
	broker   *presentationStart

	closeOnce sync.Once
	closeErr  error
}

func newPresentationManager(
	parent context.Context,
	startTimeout time.Duration,
	settlementTimeout time.Duration,
	native presentationOperationFactory,
	broker presentationOperationFactory,
) (*presentationManager, error) {
	if parent == nil {
		return nil, errors.New("FuseKit runtime: presentation manager lifetime is required")
	}
	lifetime, cancel := context.WithCancel(parent)
	manager := &presentationManager{lifetime: lifetime, cancel: cancel}
	var err error
	if native != nil {
		manager.native, err = newPresentationStart(lifetime, startTimeout, settlementTimeout, "native", native)
	}
	if err == nil && broker != nil {
		manager.broker, err = newPresentationStart(lifetime, startTimeout, settlementTimeout, "File Provider broker", broker)
	}
	if err != nil {
		cancel()
		return nil, err
	}
	return manager, nil
}

func (m *presentationManager) EnsureNative(ctx context.Context) error {
	if m == nil || m.native == nil {
		return errors.New("FuseKit runtime: native presentation is unavailable")
	}
	return m.native.Ensure(ctx)
}

func (m *presentationManager) EnsureBroker(ctx context.Context) error {
	if m == nil || m.broker == nil {
		return errors.New("FuseKit runtime: File Provider presentation is unavailable")
	}
	return m.broker.Ensure(ctx)
}

func (m *presentationManager) Ensure(ctx context.Context, native, broker bool) error {
	switch {
	case native && broker:
		nativeResult := make(chan error, 1)
		brokerResult := make(chan error, 1)
		go func() { nativeResult <- m.EnsureNative(ctx) }()
		go func() { brokerResult <- m.EnsureBroker(ctx) }()
		return errors.Join(<-nativeResult, <-brokerResult)
	case native:
		return m.EnsureNative(ctx)
	case broker:
		return m.EnsureBroker(ctx)
	default:
		return errors.New("FuseKit runtime: presentation selection is empty")
	}
}

func (m *presentationManager) Close(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.closeOnce.Do(func() {
		m.cancel()
		var nativeErr error
		if m.native != nil {
			nativeErr = m.native.Close(ctx)
		}
		var brokerErr error
		if m.broker != nil {
			brokerErr = m.broker.Close(ctx)
		}
		m.closeErr = errors.Join(nativeErr, brokerErr)
	})
	return m.closeErr
}
