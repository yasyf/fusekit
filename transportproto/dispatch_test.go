package transportproto

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

func echoHandler(_ context.Context, request daemonkit.Request) ([]byte, error) {
	return request.Body, nil
}

func TestNewMuxRequiresExactDeadlinePairing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		deadlines map[string]time.Duration
		specs     []HandlerSpec
		wantErr   bool
	}{
		{
			name:      "paired",
			deadlines: map[string]time.Duration{"catalog.head": time.Second},
			specs:     []HandlerSpec{{Op: "catalog.head", Handler: echoHandler, Concurrent: true}},
		},
		{
			name:      "handler without deadline",
			deadlines: map[string]time.Duration{},
			specs:     []HandlerSpec{{Op: "catalog.head", Handler: echoHandler}},
			wantErr:   true,
		},
		{
			name:      "deadline without handler",
			deadlines: map[string]time.Duration{"catalog.head": time.Second, "catalog.read": time.Second},
			specs:     []HandlerSpec{{Op: "catalog.head", Handler: echoHandler}},
			wantErr:   true,
		},
		{
			name:      "non-positive deadline",
			deadlines: map[string]time.Duration{"catalog.head": 0},
			specs:     []HandlerSpec{{Op: "catalog.head", Handler: echoHandler}},
			wantErr:   true,
		},
		{
			name:      "duplicate operation",
			deadlines: map[string]time.Duration{"catalog.head": time.Second},
			specs: []HandlerSpec{
				{Op: "catalog.head", Handler: echoHandler},
				{Op: "catalog.head", Handler: echoHandler},
			},
			wantErr: true,
		},
		{
			name:      "missing operation",
			deadlines: map[string]time.Duration{"": time.Second},
			specs:     []HandlerSpec{{Handler: echoHandler}},
			wantErr:   true,
		},
		{
			name:      "missing handler",
			deadlines: map[string]time.Duration{"catalog.head": time.Second},
			specs:     []HandlerSpec{{Op: "catalog.head"}},
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mux, err := NewMux(tt.deadlines, tt.specs...)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewMux error = %v, want error %v", err, tt.wantErr)
			}
			if !tt.wantErr && mux == nil {
				t.Fatal("NewMux returned no mux")
			}
		})
	}
}

func TestMuxHandleRoutesAndRefuses(t *testing.T) {
	t.Parallel()
	mux, err := NewMux(
		map[string]time.Duration{"catalog.head": time.Second},
		HandlerSpec{Op: "catalog.head", Handler: echoHandler, Concurrent: true},
	)
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	reply, err := mux.Handle(t.Context(), daemonkit.Request{Op: "catalog.head", Body: []byte("head")})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if string(reply.Body) != "head" {
		t.Fatalf("reply body = %q, want %q", reply.Body, "head")
	}
	if _, err := mux.Handle(t.Context(), daemonkit.Request{Op: "catalog.absent"}); !errors.Is(err, ErrUnknownOperation) {
		t.Fatalf("Handle unknown operation = %v, want ErrUnknownOperation", err)
	}
}

func TestMuxHandleAppliesTheOperationDeadline(t *testing.T) {
	t.Parallel()
	mux, err := NewMux(
		map[string]time.Duration{"catalog.head": time.Millisecond},
		HandlerSpec{Op: "catalog.head", Concurrent: true, Handler: func(ctx context.Context, _ daemonkit.Request) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}},
	)
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	if _, err := mux.Handle(t.Context(), daemonkit.Request{Op: "catalog.head"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Handle = %v, want context.DeadlineExceeded", err)
	}
}

// A parked long-poll must not block unrelated traffic: v0.20 ran a
// non-concurrent handler inline on the request's own goroutine, and v0.21 bounds
// every handle through Daemon.Concurrency, so the mux itself never serializes.
func TestMuxHandleDoesNotSerializeAcrossOperations(t *testing.T) {
	t.Parallel()
	parked := make(chan struct{})
	release := make(chan struct{})
	mux, err := NewMux(
		map[string]time.Duration{"broker.poll": time.Minute, "catalog.head": time.Minute},
		HandlerSpec{Op: "broker.poll", Handler: func(_ context.Context, _ daemonkit.Request) ([]byte, error) {
			close(parked)
			<-release
			return nil, nil
		}},
		HandlerSpec{Op: "catalog.head", Handler: echoHandler},
	)
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	poll := make(chan error, 1)
	go func() {
		_, err := mux.Handle(context.Background(), daemonkit.Request{Op: "broker.poll"})
		poll <- err
	}()
	<-parked
	if _, err := mux.Handle(t.Context(), daemonkit.Request{Op: "catalog.head", Body: []byte("head")}); err != nil {
		t.Fatalf("Handle behind a parked long-poll: %v", err)
	}
	close(release)
	if err := <-poll; err != nil {
		t.Fatalf("parked Handle: %v", err)
	}
}

func TestMuxHandleRunsOneOperationConcurrentlyWithItself(t *testing.T) {
	t.Parallel()
	var group sync.WaitGroup
	entered := make(chan struct{}, 2)
	both := make(chan struct{})
	mux, err := NewMux(
		map[string]time.Duration{"catalog.read": time.Minute},
		HandlerSpec{Op: "catalog.read", Handler: func(ctx context.Context, _ daemonkit.Request) ([]byte, error) {
			entered <- struct{}{}
			select {
			case <-both:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}},
	)
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := mux.Handle(context.Background(), daemonkit.Request{Op: "catalog.read"}); err != nil {
				t.Errorf("Handle: %v", err)
			}
		}()
	}
	for range 2 {
		<-entered
	}
	close(both)
	group.Wait()
}
