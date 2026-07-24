package holder

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountmux"
)

type presentationOperationFactoryFunc func() (presentationOperation, error)

func (f presentationOperationFactoryFunc) newPresentationOperation() (presentationOperation, error) {
	return f()
}

type ownedPresentationOperation struct {
	startOperation func(context.Context) error
	health         func() error
	closeOperation func() error

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func newOwnedPresentationOperation(
	start func(context.Context) error,
	health func() error,
	closeOperation func() error,
) (*ownedPresentationOperation, error) {
	if start == nil || health == nil || closeOperation == nil {
		return nil, errors.New("FuseKit runtime: presentation operation is incomplete")
	}
	return &ownedPresentationOperation{
		startOperation: start, health: health, closeOperation: closeOperation,
		closeDone: make(chan struct{}),
	}, nil
}

func (o *ownedPresentationOperation) start(ctx context.Context) error { return o.startOperation(ctx) }

func (o *ownedPresentationOperation) ready(context.Context) error { return o.health() }

func (o *ownedPresentationOperation) healthy() error { return o.health() }

func (o *ownedPresentationOperation) stop(context.Context) error {
	o.closeOnce.Do(func() {
		go func() {
			o.closeErr = o.closeOperation()
			close(o.closeDone)
		}()
	})
	return nil
}

func (o *ownedPresentationOperation) wait(ctx context.Context) error {
	select {
	case <-o.closeDone:
		return o.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func nativePresentationFactory(runtime *mountmux.Runtime, native nativeController) presentationOperationFactory {
	if runtime == nil || native == nil {
		return nil
	}
	return presentationOperationFactoryFunc(func() (presentationOperation, error) {
		return newOwnedPresentationOperation(
			runtime.Start,
			func() error {
				if state := native.HealthState(); state != daemon.StateHealthy {
					return fmt.Errorf("FuseKit runtime: native presentation health is %s", state)
				}
				return nil
			},
			func() error { return runtime.CloseContext(context.Background()) },
		)
	})
}

func brokerPresentationFactory(runtime *catalogservice.RuntimeBroker) presentationOperationFactory {
	if runtime == nil {
		return nil
	}
	return presentationOperationFactoryFunc(func() (presentationOperation, error) {
		return newOwnedPresentationOperation(
			runtime.Start,
			func() error {
				if phase := runtime.ReadinessPhase(); phase != catalogservice.RuntimeBrokerLive {
					return fmt.Errorf("FuseKit runtime: File Provider broker phase is %d", phase)
				}
				return nil
			},
			func() error { return runtime.Close(context.Background()) },
		)
	})
}

type nativePresentationPreparer struct {
	presentations *presentationManager
	route         func(catalog.TenantID, catalog.Generation) error
}

func (p nativePresentationPreparer) PrepareMountPresentation(
	ctx context.Context,
	id catalog.TenantID,
	generation catalog.Generation,
) error {
	if p.presentations == nil || p.route == nil {
		return errors.New("FuseKit runtime: native presentation preparer is incomplete")
	}
	if err := p.presentations.EnsureNative(ctx); err != nil {
		return err
	}
	return p.route(id, generation)
}

type fileProviderPresentationPreparer struct {
	presentations *presentationManager
	next          catalogservice.FileProviderPresentationPreparer
}

func (p fileProviderPresentationPreparer) PrepareFileProviderPresentation(
	ctx context.Context,
	id catalog.TenantID,
	generation catalog.Generation,
) (catalog.FileProviderDomain, error) {
	if p.presentations == nil || p.next == nil {
		return catalog.FileProviderDomain{}, errors.New("FuseKit runtime: File Provider presentation preparer is incomplete")
	}
	if err := p.presentations.EnsureBroker(ctx); err != nil {
		return catalog.FileProviderDomain{}, err
	}
	return p.next.PrepareFileProviderPresentation(ctx, id, generation)
}

var _ catalogservice.MountPresentationPreparer = nativePresentationPreparer{}
var _ catalogservice.FileProviderPresentationPreparer = fileProviderPresentationPreparer{}
