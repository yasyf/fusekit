package sourceauthority

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

type testObserverLauncher struct {
	backend EventBackend

	mu      sync.Mutex
	process *testObserverProcess
	dialErr error
}

type testObserverProcess struct {
	session *testSourceSession
	cancel  context.CancelFunc
	dialErr error

	mu          sync.Mutex
	claimed     bool
	stopCalls   int
	stopBounded bool
}

func (l *testObserverLauncher) LaunchSourceObserver(
	ctx context.Context,
	spec ObserverProcessSpec,
) (ObserverProcess, error) {
	if !reflect.DeepEqual(spec.Arguments, FSEventsObserverChildArguments()) {
		return nil, errors.New("invalid observer process spec")
	}
	_, cancel := context.WithCancel(context.Background())
	child := &fseventsObserverChild{backend: l.backend, cancel: cancel}
	session, err := startTestSourceSession(context.Background(), fseventsObserverBuild, observerHandlerSpecs(child))
	if err != nil {
		cancel()
		return nil, err
	}
	process := &testObserverProcess{
		session: session, cancel: cancel, dialErr: l.dialErr,
	}
	l.mu.Lock()
	l.process = process
	l.mu.Unlock()
	return process, nil
}

func (p *testObserverProcess) SessionEndpoint(context.Context) (proc.SpawnedSessionEndpoint, error) {
	return proc.SpawnedSessionEndpoint{}, errors.New("test observer uses an internal session")
}

func (p *testObserverProcess) openSourceSession(ctx context.Context) (sourceSessionClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dialErr != nil {
		return nil, p.dialErr
	}
	if p.claimed {
		return nil, errors.New("test observer session already claimed")
	}
	p.claimed = true
	return testSourceSessionClient{Client: p.session.client, closeSession: p.session.Close}, nil
}

func (p *testObserverProcess) Stop(ctx context.Context) error {
	p.mu.Lock()
	p.stopCalls++
	_, p.stopBounded = ctx.Deadline()
	p.mu.Unlock()
	p.cancel()
	return errors.Join(p.session.client.Abort(ErrClosed), p.session.Close())
}

type testObserverBackend struct {
	stream *testObserverStream
}

type testObserverStream struct {
	mu          sync.Mutex
	checkpoint  StreamCheckpoint
	sink        DurableEventSink
	emitOnFlush bool
	emitOnClose bool
	closed      bool
}

func (b *testObserverBackend) Open(
	_ context.Context,
	_ []RootSpec,
	_ []StreamCheckpoint,
	sink DurableEventSink,
) (EventStream, error) {
	b.stream.sink = sink
	return b.stream, nil
}

func (s *testObserverStream) Checkpoints() []StreamCheckpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []StreamCheckpoint{s.checkpoint}
}

func (s *testObserverStream) Activate(context.Context) error { return nil }

func (s *testObserverStream) Flush(ctx context.Context) ([]StreamCheckpoint, error) {
	if s.emitOnFlush {
		if err := s.emit(ctx); err != nil {
			return nil, err
		}
	}
	return s.Checkpoints(), nil
}

func (s *testObserverStream) Close() error {
	if s.emitOnClose {
		if err := s.emit(context.Background()); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *testObserverStream) emit(ctx context.Context) error {
	s.mu.Lock()
	checkpoint := s.checkpoint
	s.mu.Unlock()
	batch := EventBatch{
		Stream: checkpoint.Identity, Predecessor: checkpoint.Cursor,
		Cursor: checkpoint.Cursor + 1, RootEpoch: checkpoint.RootEpoch,
		Events: []PathEvent{{Root: "root", Relative: "value", Kind: EventModified, ID: checkpoint.Cursor + 1, Ordinal: 1}},
	}
	if err := s.sink(ctx, batch); err != nil {
		return err
	}
	s.mu.Lock()
	s.checkpoint.Cursor = batch.Cursor
	s.mu.Unlock()
	return nil
}

func TestFSEventsProxyFlushWaitsForDurableAck(t *testing.T) {
	t.Parallel()
	stream, _, cleanup := openTestObserverProxy(t, true, false, nil)
	defer cleanup()

	result := make(chan error, 1)
	go func() {
		_, err := stream.Flush(context.Background())
		result <- err
	}()
	fixture := stream.(*testProxyFixture)
	<-fixture.entered
	select {
	case err := <-result:
		t.Fatalf("Flush returned before durable sink acknowledgement: %v", err)
	default:
	}
	close(fixture.release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if checkpoints := stream.Checkpoints(); len(checkpoints) != 1 || checkpoints[0].Cursor != 8 {
		t.Fatalf("checkpoints = %+v, want durable cursor 8", checkpoints)
	}
}

func TestFSEventsProxyCloseJoinsCallbackAndProcess(t *testing.T) {
	t.Parallel()
	stream, launcher, cleanup := openTestObserverProxy(t, false, true, nil)
	defer cleanup()
	fixture := stream.(*testProxyFixture)
	result := make(chan error, 1)
	go func() { result <- stream.Close() }()
	<-fixture.entered
	select {
	case err := <-result:
		t.Fatalf("Close returned before callback acknowledgement: %v", err)
	default:
	}
	launcher.mu.Lock()
	process := launcher.process
	launcher.mu.Unlock()
	process.mu.Lock()
	stopsBeforeAck := process.stopCalls
	process.mu.Unlock()
	if stopsBeforeAck != 0 {
		t.Fatalf("process stopped before callback settled: %d", stopsBeforeAck)
	}
	close(fixture.release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	process.mu.Lock()
	stops := process.stopCalls
	process.mu.Unlock()
	if stops != 1 {
		t.Fatalf("process stop calls = %d, want 1", stops)
	}
}

type gatedObserverProcess struct {
	entered  chan struct{}
	deadline chan struct{}
	release  chan struct{}
}

func (gatedObserverProcess) SessionEndpoint(context.Context) (proc.SpawnedSessionEndpoint, error) {
	return proc.SpawnedSessionEndpoint{}, errors.New("not used")
}

func (p gatedObserverProcess) Stop(ctx context.Context) error {
	close(p.entered)
	<-ctx.Done()
	close(p.deadline)
	<-p.release
	return ctx.Err()
}

func TestObserverStopReportsDeadlineOnlyAfterExactSettlement(t *testing.T) {
	t.Parallel()
	process := gatedObserverProcess{
		entered: make(chan struct{}), deadline: make(chan struct{}), release: make(chan struct{}),
	}
	result := make(chan error, 1)
	go func() { result <- stopObserverProcessWithin(process, 20*time.Millisecond) }()
	<-process.entered
	<-process.deadline
	select {
	case err := <-result:
		t.Fatalf("stop returned before exact process settlement: %v", err)
	default:
	}
	close(process.release)
	err := <-result
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop error = %v, want deadline", err)
	}
}

type blockingObserverLauncher struct {
	entered  chan struct{}
	deadline chan struct{}
	release  chan struct{}
	process  *lateObserverProcess
}

func (l blockingObserverLauncher) LaunchSourceObserver(ctx context.Context, _ ObserverProcessSpec) (ObserverProcess, error) {
	close(l.entered)
	<-ctx.Done()
	close(l.deadline)
	<-l.release
	return l.process, nil
}

type lateObserverProcess struct {
	mu          sync.Mutex
	stopCalls   int
	stopBounded bool
}

func (*lateObserverProcess) SessionEndpoint(context.Context) (proc.SpawnedSessionEndpoint, error) {
	return proc.SpawnedSessionEndpoint{}, errors.New("not used")
}

func (p *lateObserverProcess) Stop(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopCalls++
	_, p.stopBounded = ctx.Deadline()
	return nil
}

func TestFSEventsProxyOpenJoinsContextIgnoringLauncher(t *testing.T) {
	t.Parallel()
	process := &lateObserverProcess{}
	launcher := blockingObserverLauncher{
		entered: make(chan struct{}), deadline: make(chan struct{}), release: make(chan struct{}), process: process,
	}
	deadlines := StandardOperationDeadlines()
	deadlines.ObserverControl = 30 * time.Millisecond
	backend, err := NewFSEventsBackend(launcher, deadlines)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, openErr := backend.Open(t.Context(), testProxyRoots(), nil, func(context.Context, EventBatch) error { return nil })
		result <- openErr
	}()
	<-launcher.entered
	<-launcher.deadline
	select {
	case err := <-result:
		t.Fatalf("Open returned before launcher settlement: %v", err)
	default:
	}
	close(launcher.release)
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open error = %v, want deadline", err)
	}
	process.mu.Lock()
	stops, bounded := process.stopCalls, process.stopBounded
	process.mu.Unlock()
	if stops != 1 || !bounded {
		t.Fatalf("late observer stops=%d bounded=%v", stops, bounded)
	}
}

func TestFSEventsProxySinkFailureDoesNotAdvanceAndStopsChild(t *testing.T) {
	t.Parallel()
	sinkErr := errors.New("durable append failed")
	stream, launcher, cleanup := openTestObserverProxy(t, true, false, sinkErr)
	defer cleanup()
	if _, err := stream.Flush(context.Background()); err == nil {
		t.Fatal("Flush succeeded after durable sink failure")
	}
	if checkpoints := stream.Checkpoints(); len(checkpoints) != 1 || checkpoints[0].Cursor != 7 {
		t.Fatalf("checkpoints = %+v, want unchanged durable cursor 7", checkpoints)
	}
	launcher.mu.Lock()
	process := launcher.process
	launcher.mu.Unlock()
	process.mu.Lock()
	stops := process.stopCalls
	process.mu.Unlock()
	if stops != 1 {
		t.Fatalf("process stop calls = %d, want 1", stops)
	}
}

func TestFSEventsProxyRejectsUnauthenticatedDialAndStopsChild(t *testing.T) {
	t.Parallel()
	backend := &testObserverBackend{stream: newTestObserverStream(false, false)}
	launcher := &testObserverLauncher{backend: backend, dialErr: errors.New("observer process identity mismatch")}
	proxy, err := NewFSEventsBackend(launcher, StandardOperationDeadlines())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.Open(context.Background(), testProxyRoots(), nil, func(context.Context, EventBatch) error { return nil }); err == nil {
		t.Fatal("Open accepted an unauthenticated observer connection")
	}
	launcher.mu.Lock()
	process := launcher.process
	launcher.mu.Unlock()
	if process == nil {
		t.Fatal("observer process was not launched")
	}
	process.mu.Lock()
	stops := process.stopCalls
	process.mu.Unlock()
	if stops != 1 {
		t.Fatalf("process stop calls = %d, want 1", stops)
	}
}

type testProxyFixture struct {
	*fseventsProxyStream
	entered chan struct{}
	release chan struct{}
}

func openTestObserverProxy(
	t *testing.T,
	emitOnFlush bool,
	emitOnClose bool,
	sinkErr error,
) (EventStream, *testObserverLauncher, func()) {
	t.Helper()
	native := newTestObserverStream(emitOnFlush, emitOnClose)
	launcher := &testObserverLauncher{backend: &testObserverBackend{stream: native}}
	backend, err := NewFSEventsBackend(launcher, StandardOperationDeadlines())
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	block := sinkErr == nil
	stream, err := backend.Open(context.Background(), testProxyRoots(), nil, func(context.Context, EventBatch) error {
		if block {
			close(entered)
			<-release
		}
		return sinkErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture := &testProxyFixture{fseventsProxyStream: stream.(*fseventsProxyStream), entered: entered, release: release}
	cleanup := func() {
		if block {
			select {
			case <-release:
			default:
				close(release)
			}
		}
		_ = fixture.Close()
	}
	return fixture, launcher, cleanup
}

func newTestObserverStream(flush, close bool) *testObserverStream {
	return &testObserverStream{
		checkpoint:  StreamCheckpoint{Identity: "stream", Cursor: 7, RootEpoch: "epoch"},
		emitOnFlush: flush, emitOnClose: close,
	}
}

func testProxyRoots() []RootSpec {
	return []RootSpec{{
		Authority: "proxy-test", ID: "root", Path: "/source", Kind: RootDirectory, Generation: 1,
		ExpectedIdentity: FileIdentity{VolumeUUID: "volume", Inode: 1},
	}}
}

func TestFSEventsProxyCloseHasBoundedControlDeadline(t *testing.T) {
	if fseventsCloseTimeout <= 0 || fseventsCloseTimeout > 10*time.Second {
		t.Fatalf("close timeout = %v", fseventsCloseTimeout)
	}
}

func TestFSEventsProxyFlushDeadlineStopsObserver(t *testing.T) {
	t.Parallel()
	native := newTestObserverStream(true, false)
	launcher := &testObserverLauncher{backend: &testObserverBackend{stream: native}}
	deadlines := StandardOperationDeadlines()
	backend, err := NewFSEventsBackend(launcher, deadlines)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := backend.Open(t.Context(), testProxyRoots(), nil, func(ctx context.Context, _ EventBatch) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	stream.(*fseventsProxyStream).controlTimeout = 50 * time.Millisecond
	if err := stream.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := stream.Flush(t.Context()); err == nil {
		t.Fatal("flush succeeded after its durable sink deadline")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded observer flush took %s", elapsed)
	}
	launcher.mu.Lock()
	process := launcher.process
	launcher.mu.Unlock()
	process.mu.Lock()
	stops, bounded := process.stopCalls, process.stopBounded
	process.mu.Unlock()
	if stops != 1 || !bounded {
		t.Fatalf("flush deadline stops=%d bounded=%v", stops, bounded)
	}
}

func TestFSEventsProxyTimeoutJoinsContextIgnoringSinkAndTermination(t *testing.T) {
	t.Parallel()
	native := newTestObserverStream(true, false)
	launcher := &testObserverLauncher{backend: &testObserverBackend{stream: native}}
	deadlines := StandardOperationDeadlines()
	backend, err := NewFSEventsBackend(launcher, deadlines)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	deadline := make(chan struct{})
	release := make(chan struct{})
	stream, err := backend.Open(t.Context(), testProxyRoots(), nil, func(ctx context.Context, _ EventBatch) error {
		close(entered)
		<-ctx.Done()
		close(deadline)
		<-release
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	stream.(*fseventsProxyStream).controlTimeout = 30 * time.Millisecond
	if err := stream.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, flushErr := stream.Flush(t.Context())
		result <- flushErr
	}()
	<-entered
	<-deadline
	select {
	case err := <-result:
		t.Fatalf("Flush returned before sink and event-loop settlement: %v", err)
	default:
	}
	close(release)
	if err := <-result; err == nil {
		t.Fatal("Flush succeeded after a context-ignoring sink exceeded its deadline")
	}
	closeErr := stream.Close()
	if closeErr == nil {
		t.Fatal("Close discarded the terminal sink failure")
	}
	if !errors.Is(closeErr, wire.ErrClientAbort) {
		t.Fatalf("Close after sink timeout = %v, want typed client abort", closeErr)
	}
	if strings.Contains(closeErr.Error(), "await go-away acknowledgement") || strings.Contains(closeErr.Error(), "read: EOF") {
		t.Fatalf("Close attempted graceful GoAway after sink timeout: %v", closeErr)
	}
	launcher.mu.Lock()
	process := launcher.process
	launcher.mu.Unlock()
	process.mu.Lock()
	stops, bounded := process.stopCalls, process.stopBounded
	process.mu.Unlock()
	if stops != 1 || !bounded {
		t.Fatalf("context-ignoring sink stops=%d bounded=%v", stops, bounded)
	}
}

func TestObserverAcknowledgementErrorIsSanitizedAndExactlyBounded(t *testing.T) {
	t.Parallel()
	exact := strings.Repeat("x", maxObserverTerminalErrorBytes)
	if got := boundedObserverErrorMessage(exact); got != exact {
		t.Fatalf("exact terminal error changed: got %d bytes", len(got))
	}
	plusOne := strings.Repeat("x", maxObserverTerminalErrorBytes+1)
	if got := boundedObserverErrorMessage(plusOne); len(got) != maxObserverTerminalErrorBytes {
		t.Fatalf("max+1 terminal error = %d bytes, want %d", len(got), maxObserverTerminalErrorBytes)
	}
	if err := validateObserverAckError(plusOne); err == nil {
		t.Fatal("max+1 terminal error passed validation")
	}
	invalid := strings.Repeat("x", maxObserverTerminalErrorBytes-1) + string([]byte{0xff})
	sanitized := boundedObserverErrorMessage(invalid)
	if len(sanitized) > maxObserverTerminalErrorBytes || !utf8.ValidString(sanitized) {
		t.Fatalf("sanitized terminal error is invalid: bytes=%d utf8=%v", len(sanitized), utf8.ValidString(sanitized))
	}
	if got := boundedObserverErrorMessage("before\x00after"); strings.ContainsRune(got, 0) {
		t.Fatal("sanitized terminal error retained NUL")
	}
}

func TestObserverControlEnvelopesHaveCountAndEncodedBounds(t *testing.T) {
	t.Parallel()
	scaleManifest, err := planObserverOpenPages(sourceTaskScaleRoots(maxObserverRoots), nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := uint32((maxObserverRoots + observerOpenPageItems - 1) / observerOpenPageItems); scaleManifest.Pages != want {
		t.Fatalf("4k-root observer open uses %d pages, want %d", scaleManifest.Pages, want)
	}
	byteSplitRoots := sourceTaskScaleRoots(observerOpenPageItems)
	for index := range byteSplitRoots {
		byteSplitRoots[index].Path = "/" + strings.Repeat("x", sourceTaskStringByteLimit-1)
	}
	byteSplitManifest, err := planObserverOpenPages(byteSplitRoots, nil)
	if err != nil {
		t.Fatal(err)
	}
	if byteSplitManifest.Pages <= 1 {
		t.Fatal("encoded-byte bound did not split a 128-item observer page")
	}
	pageForPathLength := func(length int) observerOpenPage {
		roots := sourceTaskScaleRoots(observerOpenPageItems)
		for index := range roots {
			roots[index].Path = "/" + strings.Repeat("x", length-1)
		}
		return observerOpenPage{Protocol: fseventsObserverProtocol, Roots: roots}
	}
	low, high := 1, sourceTaskStringByteLimit
	for low < high {
		middle := low + (high-low+1)/2
		page := pageForPathLength(middle)
		if _, err := encodeObserverOpenPage(&page); err == nil {
			low = middle
		} else {
			high = middle - 1
		}
	}
	exactPage := pageForPathLength(low)
	if _, err := encodeObserverOpenPage(&exactPage); err != nil {
		t.Fatalf("largest encoded observer page rejected: %v", err)
	}
	if low == sourceTaskStringByteLimit {
		t.Fatal("observer page did not reach its encoded byte limit before its string limit")
	}
	plusOnePage := pageForPathLength(low + 1)
	if _, err := encodeObserverOpenPage(&plusOnePage); err == nil {
		t.Fatal("encoded observer page max+1 was accepted")
	}
	if err := validateObserverOpenManifest(observerOpenManifest{
		Pages:        observerOpenPageLimit + 1,
		EncodedBytes: 1,
		Digest:       Fingerprint{1},
		Roots:        1,
	}); err == nil {
		t.Fatal("observer open manifest exceeded the page count")
	}
	if _, err := planObserverOpenPages(make([]RootSpec, maxObserverRoots+1), nil); err == nil {
		t.Fatal("observer open request exceeded the root count")
	}
	if _, err := planObserverOpenPages([]RootSpec{{Path: "/root"}}, make([]StreamCheckpoint, maxObserverCheckpoints+1)); err == nil {
		t.Fatal("observer open request exceeded the resume count")
	}
	if _, err := planObserverOpenPages([]RootSpec{{Path: strings.Repeat("x", sourceTaskStringByteLimit+1)}}, nil); err == nil {
		t.Fatal("observer open request exceeded the encoded budget")
	}
	if _, err := marshalObserverControl(observerCheckpointResponse{
		Protocol:    fseventsObserverProtocol,
		Checkpoints: make([]StreamCheckpoint, maxObserverCheckpoints+1),
	}); err == nil {
		t.Fatal("observer checkpoint response exceeded the item count")
	}
	if _, err := marshalObserverControl(observerCheckpointResponse{
		Protocol: fseventsObserverProtocol,
		Checkpoints: []StreamCheckpoint{{
			Identity: StreamIdentity(strings.Repeat("x", maxObserverCheckpointBytes)),
		}},
	}); err == nil {
		t.Fatal("observer checkpoint response exceeded the encoded budget")
	}
}

func TestObserverOpenPagesRejectDuplicateSkipReorderTerminalMismatchCancelAndChildDeath(t *testing.T) {
	t.Parallel()
	roots := sourceTaskScaleRoots(observerOpenPageItems*2 + 1)
	resume := make([]StreamCheckpoint, observerOpenPageItems+1)
	for index := range resume {
		resume[index] = StreamCheckpoint{
			Identity:  StreamIdentity(fmt.Sprintf("stream-%04d", index)),
			RootEpoch: RootEpoch(fmt.Sprintf("epoch-%04d", index)),
		}
	}
	manifest, err := planObserverOpenPages(roots, resume)
	if err != nil {
		t.Fatal(err)
	}
	var payloads [][]byte
	var cursor uint32
	var previous Fingerprint
	if err := emitObserverOpenBodies(roots, resume, func(page observerOpenPage) error {
		page.Cursor, page.Previous = cursor, previous
		encoded, err := encodeObserverOpenPage(&page)
		if err != nil {
			return err
		}
		payloads = append(payloads, encodeStreamChunk(observerOpenChunk, cursor, encoded))
		cursor++
		previous = page.Digest
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 5 {
		t.Fatalf("encoded %d observer pages, want 5", len(payloads))
	}
	replay := func(ctx context.Context, selected [][]byte, proof observerOpenManifest) error {
		chunks := make(chan wire.Chunk, len(selected)+1)
		for index, payload := range selected {
			chunks <- wire.Chunk{Sequence: uint32(index + 1), Payload: payload}
		}
		chunks <- wire.Chunk{Sequence: uint32(len(selected) + 1), End: true}
		close(chunks)
		gotRoots, gotResume, err := receiveObserverOpenPages(ctx, chunks, proof)
		if err == nil && (len(gotRoots) != len(roots) || len(gotResume) != len(resume)) {
			t.Fatalf("replay counts = roots %d resume %d", len(gotRoots), len(gotResume))
		}
		return err
	}
	if err := replay(context.Background(), payloads, manifest); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if err := replay(context.Background(), payloads, manifest); err != nil {
		t.Fatalf("second exact replay: %v", err)
	}
	for name, selected := range map[string][][]byte{
		"duplicate": {payloads[0], payloads[0], payloads[1], payloads[2], payloads[3], payloads[4]},
		"skip":      {payloads[0], payloads[2], payloads[3], payloads[4]},
		"reorder":   {payloads[1], payloads[0], payloads[2], payloads[3], payloads[4]},
	} {
		if err := replay(context.Background(), selected, manifest); err == nil {
			t.Fatalf("%s observer page sequence was accepted", name)
		}
	}
	countMismatch := manifest
	countMismatch.Roots++
	if err := replay(context.Background(), payloads, countMismatch); err == nil {
		t.Fatal("observer terminal count mismatch was accepted")
	}
	digestMismatch := manifest
	digestMismatch.Digest[0] ^= 1
	if err := replay(context.Background(), payloads, digestMismatch); err == nil {
		t.Fatal("observer terminal digest mismatch was accepted")
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelChunks := make(chan wire.Chunk)
	cancelSent := make(chan struct{})
	go func() {
		cancelChunks <- wire.Chunk{Sequence: 1, Payload: payloads[0]}
		cancel()
		close(cancelSent)
	}()
	if _, _, err := receiveObserverOpenPages(cancelCtx, cancelChunks, manifest); !errors.Is(err, context.Canceled) {
		t.Fatalf("observer cancellation = %v, want context.Canceled", err)
	}
	<-cancelSent

	deathChunks := make(chan wire.Chunk, 1)
	deathChunks <- wire.Chunk{Sequence: 1, Payload: payloads[0]}
	close(deathChunks)
	if _, _, err := receiveObserverOpenPages(context.Background(), deathChunks, manifest); err == nil {
		t.Fatal("observer child death between pages was accepted")
	}
}
