//go:build darwin && cgo

package sourceauthority

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestDarwinFSEventsDeliveryCopiesAndOrdersCallbacks(t *testing.T) {
	t.Parallel()
	var delivered []EventBatch
	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &darwinFSEventsRootStream{
		fence: testFSEventsFence(), ctx: streamCtx,
		sink: func(_ context.Context, batch EventBatch) error {
			delivered = append(delivered, batch)
			return nil
		},
		cursor: 7,
	}
	first := []nativeFSEvent{{path: "Users/test/source/first", flags: fseventItemCreated, id: 9}}
	if err := stream.deliver(first); err != nil {
		t.Fatal(err)
	}
	first[0] = nativeFSEvent{path: "Users/test/source/mutated", flags: fseventItemRemoved, id: 99}
	if err := stream.deliver([]nativeFSEvent{{path: "Users/test/source/second", flags: fseventItemModified, id: 14}}); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 2 || delivered[0].Predecessor != 7 || delivered[0].Cursor != 9 ||
		delivered[0].Events[0].Relative != "first" || delivered[1].Predecessor != 9 ||
		delivered[1].Cursor != 14 || delivered[1].Events[0].Relative != "second" {
		t.Fatalf("delivered = %+v", delivered)
	}
}

func TestDarwinFSEventsDeliveryAcceptsDurableSnapshotSignal(t *testing.T) {
	t.Parallel()
	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	stream := &darwinFSEventsRootStream{
		fence: testFSEventsFence(), ctx: streamCtx,
		sink: func(context.Context, EventBatch) error {
			calls++
			return ErrSnapshotRequired
		},
		cursor: 15,
	}
	if err := stream.deliver([]nativeFSEvent{{
		path: "Users/test/source", flags: fseventRootChanged, id: 0,
	}}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !stream.halted || stream.sinkErr != nil || stream.cursor != 15 {
		t.Fatalf("calls=%d halted=%t sinkErr=%v cursor=%d", calls, stream.halted, stream.sinkErr, stream.cursor)
	}
}

func TestDarwinFSEventsEngineRejectsSymlinkedRoot(t *testing.T) {
	t.Parallel()
	directory, err := os.MkdirTemp("/private/tmp", "fk-fsevents-root-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	target := filepath.Join(directory, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	linked := filepath.Join(directory, "linked")
	if err := os.Symlink(target, linked); err != nil {
		t.Fatal(err)
	}
	root := RootSpec{
		Authority: "authority", ID: "root", Path: linked, Kind: RootDirectory, Generation: 1,
	}
	stream, err := newPlatformFSEventsEngine().Open(context.Background(), []RootSpec{root}, nil, func(context.Context, EventBatch) error { return nil })
	if err == nil {
		_ = stream.Close()
		t.Fatal("symlinked root was accepted")
	}
}

func TestDarwinFSEventsCloseJoinsOnceAndReplaysTerminalResult(t *testing.T) {
	t.Parallel()
	terminalErr := errors.New("root close failed")
	firstRoot := &darwinFSEventsRootStream{}
	secondRoot := &darwinFSEventsRootStream{}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	calls := make(map[*darwinFSEventsRootStream]int)
	stream := &darwinFSEventsStream{
		streams:   []*darwinFSEventsRootStream{firstRoot, secondRoot},
		closeDone: make(chan struct{}),
		closeRoot: func(root *darwinFSEventsRootStream) error {
			mu.Lock()
			calls[root]++
			mu.Unlock()
			if root == firstRoot {
				once.Do(func() { close(entered) })
				<-release
				return terminalErr
			}
			return nil
		},
	}
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- stream.Close() }()
	go func() { second <- stream.Close() }()
	<-entered
	select {
	case err := <-first:
		t.Fatalf("first Close returned before root settlement: %v", err)
	case err := <-second:
		t.Fatalf("second Close returned before root settlement: %v", err)
	default:
	}
	close(release)
	for index, result := range []<-chan error{first, second} {
		if err := <-result; !errors.Is(err, terminalErr) {
			t.Fatalf("Close[%d] = %v, want terminal result", index, err)
		}
	}
	if err := stream.Close(); !errors.Is(err, terminalErr) {
		t.Fatalf("repeated Close = %v, want cached terminal result", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls[firstRoot] != 1 || calls[secondRoot] != 1 {
		t.Fatalf("root close calls = first %d second %d, want one each", calls[firstRoot], calls[secondRoot])
	}
}
