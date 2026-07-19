//go:build darwin && cgo

package sourceauthority

/*
#cgo CFLAGS: -Werror -Wall -Wextra
#cgo LDFLAGS: -framework CoreServices
#include "fsevents_darwin.h"
*/
import "C"

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"runtime/cgo"
	"slices"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

type platformFSEventsEngine struct{}

type darwinFSEventsStream struct {
	streams   []*darwinFSEventsRootStream
	closeRoot func(*darwinFSEventsRootStream) error
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

type darwinFSEventsRootStream struct {
	opMu sync.Mutex
	mu   sync.Mutex

	fence   fseventsRootFence
	sink    DurableEventSink
	ctx     context.Context
	cancel  context.CancelFunc
	handle  cgo.Handle
	native  *C.fk_fsevents_stream
	cursor  EventID
	active  bool
	closed  bool
	halted  bool
	sinkErr error
}

func (platformFSEventsEngine) Open(
	ctx context.Context,
	roots []RootSpec,
	resume []StreamCheckpoint,
	sink DurableEventSink,
) (EventStream, error) {
	if sink == nil || len(roots) == 0 {
		return nil, fmt.Errorf("%w: roots and durable sink are required", ErrInvalidEvent)
	}
	resumeByIdentity, err := validateFSEventsOpen(roots, resume)
	if err != nil {
		return nil, err
	}

	ordered := append([]RootSpec(nil), roots...)
	slices.SortFunc(ordered, func(left, right RootSpec) int {
		if left.ID < right.ID {
			return -1
		}
		if left.ID > right.ID {
			return 1
		}
		return 0
	})
	result := &darwinFSEventsStream{closeDone: make(chan struct{})}
	for _, root := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, result.Close())
		}
		stream, err := openDarwinFSEventsRoot(ctx, root, resumeByIdentity, sink)
		if err != nil {
			return nil, errors.Join(err, result.Close())
		}
		result.streams = append(result.streams, stream)
	}
	return result, nil
}

func openDarwinFSEventsRoot(
	ctx context.Context,
	root RootSpec,
	resume map[StreamIdentity]StreamCheckpoint,
	sink DurableEventSink,
) (*darwinFSEventsRootStream, error) {
	cPath := C.CString(root.Path)
	defer C.free(unsafe.Pointer(cPath))
	var device C.dev_t
	var pinnedRoot, pinnedWatch C.int
	var watchDevicePath, eventDevicePath, volumeUUID, message *C.char
	var inode, current C.uint64_t
	var birthSec, birthNsec C.int64_t
	if C.fk_fsevents_root_info(
		cPath, C.int(root.Kind), &device, &pinnedRoot, &pinnedWatch,
		&watchDevicePath, &eventDevicePath, &volumeUUID,
		&inode, &birthSec, &birthNsec, &current, &message,
	) == 0 {
		detail := "unknown CoreServices failure"
		if message != nil {
			detail = C.GoString(message)
			C.fk_fsevents_free(unsafe.Pointer(message))
		}
		return nil, fmt.Errorf("sourceauthority: inspect FSEvents root %q: %s", root.ID, detail)
	}
	defer C.fk_fsevents_free(unsafe.Pointer(watchDevicePath))
	defer C.fk_fsevents_free(unsafe.Pointer(eventDevicePath))
	defer C.fk_fsevents_free(unsafe.Pointer(volumeUUID))
	observedIdentity := FileIdentity{
		VolumeUUID: C.GoString(volumeUUID), Inode: uint64(inode),
		BirthtimeSec: int64(birthSec), BirthtimeNsec: int64(birthNsec),
	}
	if observedIdentity != root.ExpectedIdentity {
		_ = unix.Close(int(pinnedRoot))
		_ = unix.Close(int(pinnedWatch))
		return nil, ErrSourceChanged
	}

	identity := fseventsStreamIdentity(root)
	epoch := fseventsRootEpoch(C.GoString(volumeUUID), uint64(inode), int64(birthSec), int64(birthNsec))
	cursor := EventID(current)
	since := uint64(cursor)
	if checkpoint, ok := resume[identity]; ok && checkpoint.RootEpoch == epoch {
		cursor = checkpoint.Cursor
		since = uint64(checkpoint.Cursor)
	} else if cursor == 0 {
		since = math.MaxUint64
	}

	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stream := &darwinFSEventsRootStream{
		fence: fseventsRootFence{
			root: root, stream: identity, epoch: epoch, devicePath: C.GoString(eventDevicePath),
		},
		sink: sink, ctx: streamCtx, cancel: cancel, cursor: cursor,
	}
	stream.handle = cgo.NewHandle(stream)
	stream.native = C.fk_fsevents_open(
		device, pinnedRoot, pinnedWatch, watchDevicePath, C.uint64_t(since), C.uintptr_t(stream.handle),
	)
	if stream.native == nil {
		stream.handle.Delete()
		cancel()
		return nil, fmt.Errorf("sourceauthority: create FSEvents stream for root %q", root.ID)
	}
	return stream, nil
}

func (s *darwinFSEventsStream) Checkpoints() []StreamCheckpoint {
	s.mu.Lock()
	streams := append([]*darwinFSEventsRootStream(nil), s.streams...)
	s.mu.Unlock()
	checkpoints := make([]StreamCheckpoint, len(streams))
	for index, stream := range streams {
		checkpoints[index] = stream.checkpoint()
	}
	slices.SortFunc(checkpoints, func(left, right StreamCheckpoint) int {
		if left.Identity < right.Identity {
			return -1
		}
		if left.Identity > right.Identity {
			return 1
		}
		return 0
	})
	return checkpoints
}

func (s *darwinFSEventsStream) Activate(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	streams := append([]*darwinFSEventsRootStream(nil), s.streams...)
	s.mu.Unlock()
	for _, stream := range streams {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, s.Close())
		}
		if err := stream.activate(); err != nil {
			return errors.Join(err, s.Close())
		}
	}
	return nil
}

func (s *darwinFSEventsStream) Flush(ctx context.Context) ([]StreamCheckpoint, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	streams := append([]*darwinFSEventsRootStream(nil), s.streams...)
	s.mu.Unlock()
	for _, stream := range streams {
		if err := stream.flush(ctx); err != nil {
			return nil, err
		}
	}
	return s.Checkpoints(), nil
}

func (s *darwinFSEventsStream) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		streams := append([]*darwinFSEventsRootStream(nil), s.streams...)
		closeRoot := s.closeRoot
		s.mu.Unlock()
		var errs []error
		for _, stream := range streams {
			var err error
			if closeRoot == nil {
				err = stream.close()
			} else {
				err = closeRoot(stream)
			}
			if err != nil {
				errs = append(errs, err)
			}
		}
		s.mu.Lock()
		s.closeErr = errors.Join(errs...)
		s.mu.Unlock()
		close(s.closeDone)
	})
	<-s.closeDone
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

func (s *darwinFSEventsRootStream) checkpoint() StreamCheckpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StreamCheckpoint{Identity: s.fence.stream, Cursor: s.cursor, RootEpoch: s.fence.epoch}
}

func (s *darwinFSEventsRootStream) activate() error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.active {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	if C.fk_fsevents_start(s.native) == 0 {
		return fmt.Errorf("sourceauthority: start FSEvents stream %q", s.fence.stream)
	}
	s.mu.Lock()
	s.active = true
	s.mu.Unlock()
	return nil
}

func (s *darwinFSEventsRootStream) flush(ctx context.Context) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	closed, active := s.closed, s.active
	s.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if !active {
		return fmt.Errorf("%w: FSEvents stream is not active", ErrInvalidEvent)
	}
	C.fk_fsevents_flush(s.native)
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sinkErr
}

func (s *darwinFSEventsRootStream) close() error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	if s.closed {
		err := s.sinkErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	native := s.native
	s.mu.Unlock()
	C.fk_fsevents_close(native)
	s.cancel()
	s.handle.Delete()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sinkErr
}

func (s *darwinFSEventsRootStream) deliver(native []nativeFSEvent) error {
	s.mu.Lock()
	if s.closed || s.halted || s.sinkErr != nil {
		err := s.sinkErr
		s.mu.Unlock()
		return err
	}
	predecessor := s.cursor
	s.mu.Unlock()

	batches, cursor, halt, err := translateFSEvents(s.fence, predecessor, native)
	if err != nil {
		s.fail(err)
		return err
	}
	for _, batch := range batches {
		err = s.sink(s.ctx, batch)
		if err != nil && !errors.Is(err, ErrSnapshotRequired) {
			s.fail(err)
			return err
		}
	}
	s.mu.Lock()
	if cursor > s.cursor {
		s.cursor = cursor
	}
	s.halted = halt
	s.mu.Unlock()
	return nil
}

func (s *darwinFSEventsRootStream) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sinkErr == nil {
		s.sinkErr = err
	}
}

//export goFuseKitFSEventsCallback
func goFuseKitFSEventsCallback(
	handle C.uintptr_t,
	count C.size_t,
	paths **C.char,
	flags *C.uint32_t,
	ids *C.uint64_t,
) C.int {
	stream, ok := cgo.Handle(handle).Value().(*darwinFSEventsRootStream)
	if !ok {
		return 1
	}
	if uint64(count) > uint64(^uint(0)>>1) {
		return 1
	}
	if uint64(count) > uint64(maxEventBatchEvents) {
		length := int(count)
		flagValues := unsafe.Slice(flags, length)
		idValues := unsafe.Slice(ids, length)
		var maximum EventID
		var discontinuity uint32 = fseventMustScanSubDirs
		for index := 0; index < length; index++ {
			if id := EventID(idValues[index]); id > maximum {
				maximum = id
			}
			discontinuity |= uint32(flagValues[index]) & (fseventRootChanged | fseventIDsWrapped |
				fseventUserDropped | fseventKernelDropped | fseventMount | fseventUnmount)
		}
		if err := stream.deliver([]nativeFSEvent{{
			path: stream.fence.devicePath, flags: discontinuity, id: maximum,
		}}); err != nil {
			return 1
		}
		return 0
	}
	length := int(count)
	pathValues := unsafe.Slice(paths, length)
	flagValues := unsafe.Slice(flags, length)
	idValues := unsafe.Slice(ids, length)
	native := make([]nativeFSEvent, length)
	for index := range native {
		native[index] = nativeFSEvent{
			path:  C.GoString(pathValues[index]),
			flags: uint32(flagValues[index]),
			id:    EventID(idValues[index]),
		}
	}
	if err := stream.deliver(native); err != nil {
		return 1
	}
	return 0
}

func fseventsRootEpoch(volumeUUID string, inode uint64, birthSec, birthNsec int64) RootEpoch {
	digest := sha256.Sum256([]byte(fmt.Sprintf("fsevents-root-v1\x00%s\x00%d\x00%d\x00%d", volumeUUID, inode, birthSec, birthNsec)))
	return RootEpoch(fmt.Sprintf("fsevents:%x", digest[:]))
}

func newPlatformFSEventsEngine() EventBackend { return platformFSEventsEngine{} }

var _ EventBackend = platformFSEventsEngine{}
var _ EventStream = (*darwinFSEventsStream)(nil)
