package holder

import (
	"errors"
	"io"
	"sync/atomic"
	"testing"
)

type recordingProcessCloser struct {
	calls atomic.Int32
	err   error
}

func (c *recordingProcessCloser) Close() error {
	c.calls.Add(1)
	return c.err
}

func TestPreparedManagedProcessSettlementOwnsOutputLifetime(t *testing.T) {
	processDone := make(chan struct{})
	pipeDone := make(chan error, 1)
	closeErr := errors.New("close output")
	closer := &recordingProcessCloser{err: closeErr}
	process := &preparedManagedProcess{
		done:       processDone,
		pipes:      []<-chan error{pipeDone},
		outputs:    []*ownedProcessWriter{{Writer: io.Discard, closer: closer}},
		settlement: make(chan struct{}),
	}
	go process.settleOutputs()

	close(processDone)
	select {
	case <-process.Done():
		t.Fatal("managed process settled before output copier")
	default:
	}
	if got := closer.calls.Load(); got != 0 {
		t.Fatalf("output closed before copier settlement: %d", got)
	}

	pipeDone <- nil
	close(pipeDone)
	<-process.Done()
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("output closes = %d, want 1", got)
	}
	process.mu.Lock()
	err := process.settlementErr
	process.mu.Unlock()
	if !errors.Is(err, closeErr) {
		t.Fatalf("settlement error = %v, want output close error", err)
	}
}
