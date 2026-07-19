package holder

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestTerminalSettlementRunsOnceAndReplaysExactResult(t *testing.T) {
	var settlement terminalSettlement
	var calls atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})
	injected := errors.New("injected settlement failure")
	cancelSettlement := func() {}
	settle := func() error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return injected
	}

	const waiters = 16
	results := make(chan error, waiters)
	var ready sync.WaitGroup
	ready.Add(waiters)
	for range waiters {
		go func() {
			ready.Done()
			results <- settlement.run(context.Background(), settle, cancelSettlement)
		}()
	}
	ready.Wait()
	<-started
	if calls.Load() != 1 {
		t.Fatalf("physical settlement calls before release = %d, want one", calls.Load())
	}
	close(release)
	for range waiters {
		if err := <-results; !errors.Is(err, injected) {
			t.Fatalf("concurrent settlement result = %v, want injected failure", err)
		}
	}

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	err := settlement.run(expired, func() error {
		t.Fatal("cached settlement ran again")
		return nil
	}, cancelSettlement)
	if !errors.Is(err, injected) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cached expired result = %v, want failure and caller cancellation", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("physical settlement calls = %d, want one", calls.Load())
	}
}

func TestTerminalSettlementCancellationCannotAbandonPhysicalJoin(t *testing.T) {
	var settlement terminalSettlement
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	terminalErr := errors.New("terminal settlement failure")
	result := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		result <- settlement.run(ctx, func() error {
			close(started)
			<-release
			return terminalErr
		}, func() { close(canceled) })
	}()
	<-started
	cancel()
	<-canceled
	select {
	case err := <-result:
		t.Fatalf("Wait returned before physical settlement: %v", err)
	default:
	}
	close(release)
	if err := <-result; !errors.Is(err, context.Canceled) || !errors.Is(err, terminalErr) {
		t.Fatalf("canceled Wait = %v, want caller cancellation and terminal failure", err)
	}
	if err := settlement.run(context.Background(), func() error {
		t.Fatal("cached settlement ran again")
		return nil
	}, func() {
		t.Fatal("cached settled result canceled again")
	}); !errors.Is(err, terminalErr) {
		t.Fatalf("replayed terminal result = %v, want terminal failure", err)
	}
}
