package sourceauthority

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/tenant"
)

type testSourceTaskLauncher struct {
	pathSource           PathSource
	materializers        SourceTaskMaterializers
	afterMutation        func(context.Context, MutationReceipt) error
	afterMaterialization func(context.Context) error

	mu        sync.Mutex
	processes []*testSourceTaskProcess
}

type testSourceTaskProcess struct {
	conn   net.Conn
	cancel context.CancelFunc
	done   chan struct{}

	mu          sync.Mutex
	err         error
	stopCalls   int
	waitCalls   int
	stopBounded bool
}

func (l *testSourceTaskLauncher) LaunchSourceTask(
	ctx context.Context,
	spec SourceTaskProcessSpec,
) (SourceTaskProcess, error) {
	config, recognized, err := ParseSourceTaskChildArguments(spec.Arguments)
	if err != nil || !recognized ||
		!reflect.DeepEqual(config.Identity, spec.Identity) {
		return nil, errors.New("invalid source task process spec")
	}
	parentConn, childConn, err := testManagedSessionPair()
	parent, err := wire.SpawnedParentSessionIdentity()
	if err != nil {
		_ = parentConn.Close()
		_ = childConn.Close()
		return nil, err
	}
	serveCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	process := &testSourceTaskProcess{conn: parentConn, cancel: cancel, done: make(chan struct{})}
	go func() {
		err := serveSourceTaskChildWithHooks(
			serveCtx, childConn, parent, l.pathSource, l.materializers, config.TaskRoot, config.JournalRoot,
			l.afterMutation, l.afterMaterialization,
		)
		process.mu.Lock()
		process.err = err
		process.mu.Unlock()
		close(process.done)
	}()
	l.mu.Lock()
	l.processes = append(l.processes, process)
	l.mu.Unlock()
	return process, nil
}

func (p *testSourceTaskProcess) Dial(ctx context.Context) (net.Conn, error) {
	return p.conn, nil
}

func (p *testSourceTaskProcess) Wait(ctx context.Context) error {
	p.mu.Lock()
	p.waitCalls++
	p.mu.Unlock()
	select {
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *testSourceTaskProcess) Stop(ctx context.Context) error {
	p.mu.Lock()
	p.stopCalls++
	_, p.stopBounded = ctx.Deadline()
	p.mu.Unlock()
	p.cancel()
	err := p.Wait(ctx)
	if errors.Is(err, os.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
		return nil
	}
	return err
}

type testFullPathSource struct {
	entries   []PhysicalEntry
	scanCalls atomic.Int32
}

type deadlineTaskPathSource struct {
	*testFullPathSource
	blockRoot   bool
	blockScan   bool
	scanStarted chan struct{}
}

func (s *deadlineTaskPathSource) RootIdentity(ctx context.Context, root RootSpec) (FileIdentity, error) {
	if s.blockRoot {
		<-ctx.Done()
		return FileIdentity{}, ctx.Err()
	}
	return s.testFullPathSource.RootIdentity(ctx, root)
}

func (s *deadlineTaskPathSource) scanAll(
	ctx context.Context,
	roots []RootSpec,
	emit func(PhysicalEntry) error,
) error {
	if s.scanStarted != nil {
		close(s.scanStarted)
	}
	if s.blockScan {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.testFullPathSource.scanAll(ctx, roots, emit)
}

func (s *testFullPathSource) RootIdentity(context.Context, RootSpec) (FileIdentity, error) {
	return FileIdentity{VolumeUUID: "volume", Inode: 7, BirthtimeSec: 11}, nil
}

func (s *testFullPathSource) Stat(_ context.Context, root RootSpec, relative string) (PhysicalEntry, error) {
	return PhysicalEntry{
		Root: root.ID, Relative: relative, Exists: true, Kind: PhysicalFile,
		Identity: FileIdentity{VolumeUUID: "volume", Inode: 7, BirthtimeSec: 11},
		Mode:     0o100644, Size: 5,
	}, nil
}

func (*testFullPathSource) BeginScan(context.Context, []RootSpec) (ScanSession, error) {
	return nil, errors.New("in-process child scan must not be called")
}

func (s *testFullPathSource) scanAll(ctx context.Context, _ []RootSpec, emit func(PhysicalEntry) error) error {
	s.scanCalls.Add(1)
	for _, entry := range s.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := emit(entry); err != nil {
			return err
		}
	}
	return nil
}

type testMaterializer struct {
	body    []byte
	block   bool
	calls   atomic.Int32
	payload []byte
	input   []byte
	mu      sync.Mutex
}

func (m *testMaterializer) Materialize(ctx context.Context, task MaterializerTask) (Materialization, error) {
	m.calls.Add(1)
	m.mu.Lock()
	m.payload = append([]byte(nil), task.Payload...)
	m.mu.Unlock()
	if len(task.Inputs) != 1 || task.Inputs[0].Content == nil {
		return Materialization{}, errors.New("materializer did not receive one immutable file input")
	}
	reader, err := task.Inputs[0].Content.Open(ctx)
	if err != nil {
		return Materialization{}, err
	}
	input, err := io.ReadAll(reader)
	err = errors.Join(err, reader.Close())
	if err != nil {
		return Materialization{}, err
	}
	m.mu.Lock()
	m.input = input
	m.mu.Unlock()
	var content ContentSource = byteTaskContent(append([]byte(nil), m.body...))
	if m.block {
		content = blockingTaskContent{}
	}
	return Materialization{
		Logical:     task.Logical,
		Fingerprint: shaFingerprint("materialized"),
		Objects: []Projection{{
			Tenant: "tenant", Generation: 1, Name: "value", Kind: catalog.KindFile,
			Mode: 0o644, Visibility: catalog.Visibility{Mount: true}, Content: content,
		}},
	}, nil
}

type byteTaskContent []byte

func (c byteTaskContent) Open(context.Context) (contentstream.Source, error) {
	return ownedTestContent(io.NopCloser(bytes.NewReader(c))), nil
}
func (byteTaskContent) Close() error { return nil }

type blockingTaskContent struct{}

func (blockingTaskContent) Open(ctx context.Context) (contentstream.Source, error) {
	return ownedTestContent(&contextTaskReader{ctx: ctx}), nil
}
func (blockingTaskContent) Close() error { return nil }

type contextTaskReader struct{ ctx context.Context }

func (r *contextTaskReader) Read([]byte) (int, error) {
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}
func (*contextTaskReader) Close() error { return nil }

func TestSourceTaskScanStreamsOneChildAcrossEveryPage(t *testing.T) {
	t.Parallel()
	entries := make([]PhysicalEntry, 10_000)
	for index := range entries {
		entries[index] = PhysicalEntry{
			Root: "root", Relative: fmt.Sprintf("%05d-%s", len(entries)-index, strings.Repeat("x", index%31)),
			Exists: true, Kind: PhysicalFile,
			Identity: FileIdentity{VolumeUUID: "volume", Inode: uint64(index + 1), BirthtimeSec: 1},
		}
	}
	pathSource := &testFullPathSource{entries: entries}
	launcher := &testSourceTaskLauncher{pathSource: pathSource}
	executor := &supervisedExecutor{runtimeDir: shortTaskRuntimeDir(t), launcher: launcher, identity: testSourceTaskIdentity()}
	var received []PhysicalEntry
	scan, err := executor.BeginScan(t.Context(), testSourceTaskRoots())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scan.Close() }()
	_, ok := scan.(*streamedScanSession)
	if !ok {
		t.Fatalf("scan type = %T, want streamed scan", scan)
	}
	for {
		page, err := scan.Next(t.Context(), 1_000)
		if err != nil {
			t.Fatal(err)
		}
		received = append(received, page.Entries...)
		if page.Next == "" {
			break
		}
	}
	if len(received) != len(entries) {
		t.Fatalf("received %d entries, want %d", len(received), len(entries))
	}
	for index := 1; index < len(received); index++ {
		if comparePathRef(PathRef{Root: received[index-1].Root, Relative: received[index-1].Relative},
			PathRef{Root: received[index].Root, Relative: received[index].Relative}) >= 0 {
			t.Fatalf("snapshot was not deterministically ordered at %d", index)
		}
	}
	if calls := pathSource.scanCalls.Load(); calls != 1 {
		t.Fatalf("full scan calls = %d, want one", calls)
	}
	launcher.mu.Lock()
	processes := append([]*testSourceTaskProcess(nil), launcher.processes...)
	launcher.mu.Unlock()
	if len(processes) != 1 {
		t.Fatalf("source children = %d, want one", len(processes))
	}
	processes[0].mu.Lock()
	stops, waits := processes[0].stopCalls, processes[0].waitCalls
	processes[0].mu.Unlock()
	if stops != 0 || waits == 0 {
		t.Fatalf("natural settlement stops=%d waits=%d", stops, waits)
	}
	staged, err := filepath.Glob(filepath.Join(executor.runtimeDir, "source-snapshot-*"))
	if err != nil || len(staged) != 0 {
		t.Fatalf("completed scan residue = %v, %v", staged, err)
	}
}

func TestSourceTaskScanStreamsTenThousandRootsWithoutOneShotJSON(t *testing.T) {
	t.Parallel()
	roots := sourceTaskScaleRoots(sourceTaskRootLimit)
	for index := range roots {
		roots[index].ExpectedIdentity = FileIdentity{VolumeUUID: "volume", Inode: uint64(index + 1)}
	}
	pathSource := &testFullPathSource{}
	launcher := &testSourceTaskLauncher{pathSource: pathSource}
	executor := &supervisedExecutor{runtimeDir: shortTaskRuntimeDir(t), launcher: launcher, identity: testSourceTaskIdentity()}
	scan, err := executor.BeginScan(t.Context(), roots)
	if err != nil {
		t.Fatal(err)
	}
	page, err := scan.Next(t.Context(), maxScanPageSize)
	if err != nil {
		t.Fatal(err)
	}
	if page.Next != "" || len(page.Entries) != 0 {
		t.Fatalf("empty 10k-root scan = %#v", page)
	}
	if err := scan.Close(); err != nil {
		t.Fatal(err)
	}
	if calls := pathSource.scanCalls.Load(); calls != 1 {
		t.Fatalf("10k-root scan calls = %d, want one", calls)
	}
}

func TestSourceTaskScanRejectsRootMaxPlusOneBeforeLaunchingChild(t *testing.T) {
	t.Parallel()
	launcher := &testSourceTaskLauncher{pathSource: &testFullPathSource{}}
	executor := &supervisedExecutor{runtimeDir: shortTaskRuntimeDir(t), launcher: launcher, identity: testSourceTaskIdentity()}
	if _, err := executor.BeginScan(t.Context(), sourceTaskScaleRoots(sourceTaskRootLimit+1)); err == nil {
		t.Fatal("root max+1 was accepted")
	}
	launcher.mu.Lock()
	defer launcher.mu.Unlock()
	if len(launcher.processes) != 0 {
		t.Fatalf("root max+1 launched %d children", len(launcher.processes))
	}
}

func TestSourceTaskProxyOwnsNoScratchDatabaseOrContentFiles(t *testing.T) {
	content, err := os.ReadFile("source_task_proxy.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		`"database/sql"`,
		`"modernc.org/sqlite"`,
		"os.CreateTemp(",
		"os.OpenFile(",
		"os.Open(",
	} {
		if bytes.Contains(content, []byte(forbidden)) {
			t.Errorf("source task proxy retained parent scratch ownership through %q", forbidden)
		}
	}
}

func TestStreamedScanCloseCancelsBlockedNextAndReapsChild(t *testing.T) {
	t.Parallel()
	pathSource := &deadlineTaskPathSource{
		testFullPathSource: &testFullPathSource{},
		blockScan:          true,
		scanStarted:        make(chan struct{}),
	}
	launcher := &testSourceTaskLauncher{pathSource: pathSource}
	executor := &supervisedExecutor{
		runtimeDir: shortTaskRuntimeDir(t), launcher: launcher,
		deadlines: StandardOperationDeadlines(), identity: testSourceTaskIdentity(),
	}
	session, err := executor.BeginScan(t.Context(), testSourceTaskRoots())
	if err != nil {
		t.Fatal(err)
	}
	nextDone := make(chan error, 1)
	go func() {
		_, err := session.Next(context.Background(), 1)
		nextDone <- err
	}()
	<-pathSource.scanStarted
	closed := make(chan error, 1)
	go func() { closed <- session.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel and join a blocked Next")
	}
	if err := <-nextDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked Next error = %v, want cancellation", err)
	}
	launcher.mu.Lock()
	process := launcher.processes[0]
	launcher.mu.Unlock()
	process.mu.Lock()
	stops, waits := process.stopCalls, process.waitCalls
	process.mu.Unlock()
	if stops == 0 || waits == 0 {
		t.Fatalf("blocked scan child stops=%d waits=%d, want both", stops, waits)
	}
}

func TestSourceTaskScanSessionsAreIndependentAndCloseCleansAbandonment(t *testing.T) {
	t.Parallel()
	pathSource := &testFullPathSource{entries: []PhysicalEntry{
		{Root: "root", Relative: "a", Exists: true, Kind: PhysicalFile},
		{Root: "root", Relative: "b", Exists: true, Kind: PhysicalFile},
	}}
	executor := &supervisedExecutor{
		runtimeDir: shortTaskRuntimeDir(t), launcher: &testSourceTaskLauncher{pathSource: pathSource},
		identity: testSourceTaskIdentity(),
	}
	first, err := executor.BeginScan(t.Context(), testSourceTaskRoots())
	if err != nil {
		t.Fatal(err)
	}
	second, err := executor.BeginScan(t.Context(), testSourceTaskRoots())
	if err != nil {
		t.Fatal(err)
	}
	firstPage, firstErr := first.Next(t.Context(), 1)
	secondPage, secondErr := second.Next(t.Context(), 1)
	if firstErr != nil || secondErr != nil || firstPage.Next == "" || secondPage.Next == "" || firstPage.Next == secondPage.Next {
		t.Fatalf("independent pages = %+v/%v and %+v/%v", firstPage, firstErr, secondPage, secondErr)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}
	staged, _ := filepath.Glob(filepath.Join(executor.runtimeDir, "source-snapshot-*"))
	if len(staged) != 0 {
		t.Fatalf("abandoned scan residue = %v", staged)
	}
}

func TestSourceTaskMaterializationStreamsRawPayloadAndSettlesNaturally(t *testing.T) {
	t.Parallel()
	requestPayload := bytes.Repeat([]byte("payload"), 20_000)
	body := bytes.Repeat([]byte("content"), 30_000)
	materializer := &testMaterializer{body: body}
	launcher := &testSourceTaskLauncher{
		pathSource:    &testFullPathSource{},
		materializers: SourceTaskMaterializers{"authority": materializer},
	}
	executor := &supervisedExecutor{runtimeDir: shortTaskRuntimeDir(t), launcher: launcher, identity: testSourceTaskIdentity()}
	task := testSourceMaterializationTask(t, requestPayload)
	materialization, err := executor.Materialize(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := materialization.Objects[0].Content.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(reader.Settle(nil), reader.Wait(t.Context())); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("content length = %d, want %d", len(got), len(body))
	}
	if err := materialization.Objects[0].Content.Close(); err != nil {
		t.Fatal(err)
	}
	materializer.mu.Lock()
	gotPayload := append([]byte(nil), materializer.payload...)
	gotInput := append([]byte(nil), materializer.input...)
	materializer.mu.Unlock()
	if !bytes.Equal(gotPayload, requestPayload) {
		t.Fatal("materializer payload did not survive raw request streaming")
	}
	if !bytes.Equal(gotInput, []byte("input")) {
		t.Fatalf("materializer immutable input = %q, want input", gotInput)
	}
	launcher.mu.Lock()
	process := launcher.processes[0]
	launcher.mu.Unlock()
	process.mu.Lock()
	stops, waits := process.stopCalls, process.waitCalls
	process.mu.Unlock()
	if stops != 0 || waits == 0 {
		t.Fatalf("natural materializer settlement stops=%d waits=%d", stops, waits)
	}
}

func TestSourceTaskUnreadContentCloseCancelsStopsAndWaits(t *testing.T) {
	t.Parallel()
	materializer := &testMaterializer{block: true}
	launcher := &testSourceTaskLauncher{
		pathSource:    &testFullPathSource{},
		materializers: SourceTaskMaterializers{"authority": materializer},
	}
	executor := &supervisedExecutor{runtimeDir: shortTaskRuntimeDir(t), launcher: launcher, identity: testSourceTaskIdentity()}
	materialization, err := executor.Materialize(t.Context(), testSourceMaterializationTask(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := materialization.Objects[0].Content.Close(); err == nil {
		t.Fatal("unread canceled content unexpectedly reported success")
	}
	launcher.mu.Lock()
	process := launcher.processes[0]
	launcher.mu.Unlock()
	process.mu.Lock()
	stops, waits, bounded := process.stopCalls, process.waitCalls, process.stopBounded
	process.mu.Unlock()
	if stops != 1 || waits == 0 || !bounded {
		t.Fatalf("unread settlement stops=%d waits=%d bounded=%v", stops, waits, bounded)
	}
}

func TestSourceTaskReadyContentWaitsForDelayedTerminalWithoutStopping(t *testing.T) {
	t.Parallel()
	launcher := &testSourceTaskLauncher{
		pathSource:    &testFullPathSource{},
		materializers: SourceTaskMaterializers{"authority": &testMaterializer{body: []byte("content")}},
		afterMaterialization: func(ctx context.Context) error {
			select {
			case <-time.After(100 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	executor := &supervisedExecutor{runtimeDir: shortTaskRuntimeDir(t), launcher: launcher, identity: testSourceTaskIdentity()}
	materialization, err := executor.Materialize(t.Context(), testSourceMaterializationTask(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := materialization.Objects[0].Content.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatal(err)
	}
	_ = errors.Join(reader.Settle(nil), reader.Wait(t.Context()))
	if err := materialization.Objects[0].Content.Close(); err != nil {
		t.Fatal(err)
	}
	launcher.mu.Lock()
	process := launcher.processes[0]
	launcher.mu.Unlock()
	process.mu.Lock()
	stops := process.stopCalls
	process.mu.Unlock()
	if stops != 0 {
		t.Fatalf("delayed natural terminal was stopped %d times", stops)
	}
}

func TestSourceTaskReadyContentHungTerminalStopsAfterBoundedGrace(t *testing.T) {
	t.Parallel()
	hung := make(chan struct{})
	launcher := &testSourceTaskLauncher{
		pathSource:    &testFullPathSource{},
		materializers: SourceTaskMaterializers{"authority": &testMaterializer{body: []byte("content")}},
		afterMaterialization: func(ctx context.Context) error {
			close(hung)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	executor := &supervisedExecutor{runtimeDir: shortTaskRuntimeDir(t), launcher: launcher, identity: testSourceTaskIdentity()}
	materialization, err := executor.Materialize(t.Context(), testSourceMaterializationTask(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := materialization.Objects[0].Content.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatal(err)
	}
	<-hung
	started := time.Now()
	if err := reader.Settle(nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= sourceTaskTerminalGrace {
		t.Fatalf("Settle blocked for %v", elapsed)
	}
	if err := reader.Wait(t.Context()); err == nil {
		t.Fatal("hung terminal wait unexpectedly reported success")
	}
	if elapsed := time.Since(started); elapsed < sourceTaskTerminalGrace || elapsed > 3*time.Second {
		t.Fatalf("hung terminal settlement took %v", elapsed)
	}
	if err := materialization.Objects[0].Content.Close(); err == nil {
		t.Fatal("hung terminal content close unexpectedly reported success")
	}
	launcher.mu.Lock()
	process := launcher.processes[0]
	launcher.mu.Unlock()
	process.mu.Lock()
	stops, waits, bounded := process.stopCalls, process.waitCalls, process.stopBounded
	process.mu.Unlock()
	if stops != 1 || waits == 0 || !bounded {
		t.Fatalf("hung settlement stops=%d waits=%d bounded=%v", stops, waits, bounded)
	}
}

func TestSourceTaskMaterializationDeadlineStopsChildAndAllowsNextOperation(t *testing.T) {
	t.Parallel()
	launcher := &testSourceTaskLauncher{
		pathSource: &testFullPathSource{}, materializers: SourceTaskMaterializers{
			"authority": &testMaterializer{block: true},
		},
	}
	deadlines := StandardOperationDeadlines()
	deadlines.Materialize = 200 * time.Millisecond
	executor := &supervisedExecutor{
		runtimeDir: shortTaskRuntimeDir(t), launcher: launcher, deadlines: deadlines,
		identity: testSourceTaskIdentity(),
	}
	materialization, err := executor.Materialize(t.Context(), testSourceMaterializationTask(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	source := materialization.Objects[0].Content
	reader, err := source.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	readErr := error(nil)
	if _, err := io.ReadAll(reader); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline materialization stream = %v", err)
	} else {
		readErr = err
	}
	_ = errors.Join(reader.Settle(readErr), reader.Wait(context.Background()))
	if err := source.Close(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline materialization close = %v", err)
	}
	launcher.mu.Lock()
	first := launcher.processes[0]
	launcher.mu.Unlock()
	first.mu.Lock()
	stops, waits, bounded := first.stopCalls, first.waitCalls, first.stopBounded
	first.mu.Unlock()
	if stops != 1 || waits == 0 || !bounded {
		t.Fatalf("deadline settlement stops=%d waits=%d bounded=%v", stops, waits, bounded)
	}
	if _, err := executor.RootIdentity(t.Context(), testSourceTaskRoots()[0]); err != nil {
		t.Fatalf("operation after deadline did not reuse the executor: %v", err)
	}
}

func TestSourceTaskUnaryAndScanDeadlinesStopWorkersAndAllowReuse(t *testing.T) {
	t.Parallel()
	t.Run("unary", func(t *testing.T) {
		pathSource := &deadlineTaskPathSource{testFullPathSource: &testFullPathSource{}, blockRoot: true}
		launcher := &testSourceTaskLauncher{pathSource: pathSource}
		deadlines := StandardOperationDeadlines()
		deadlines.Unary = 200 * time.Millisecond
		executor := &supervisedExecutor{
			runtimeDir: shortTaskRuntimeDir(t), launcher: launcher, deadlines: deadlines,
			identity: testSourceTaskIdentity(),
		}
		if _, err := executor.RootIdentity(t.Context(), testSourceTaskRoots()[0]); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unary deadline = %v", err)
		}
		assertSourceTaskProcessStopped(t, launcher.processes[0])
		if _, err := executor.Stat(t.Context(), testSourceTaskRoots()[0], "value"); err != nil {
			t.Fatalf("operation after unary deadline = %v", err)
		}
	})
	t.Run("scan", func(t *testing.T) {
		pathSource := &deadlineTaskPathSource{testFullPathSource: &testFullPathSource{}, blockScan: true}
		launcher := &testSourceTaskLauncher{pathSource: pathSource}
		deadlines := StandardOperationDeadlines()
		deadlines.Scan = 200 * time.Millisecond
		executor := &supervisedExecutor{
			runtimeDir: shortTaskRuntimeDir(t), launcher: launcher, deadlines: deadlines,
			identity: testSourceTaskIdentity(),
		}
		scan, err := executor.BeginScan(t.Context(), testSourceTaskRoots())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := scan.Next(t.Context(), maxScanPageSize); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("scan deadline = %v", err)
		}
		assertSourceTaskProcessStopped(t, launcher.processes[0])
		if _, err := executor.RootIdentity(t.Context(), testSourceTaskRoots()[0]); err != nil {
			t.Fatalf("operation after scan deadline = %v", err)
		}
	})
}

func assertSourceTaskProcessStopped(t *testing.T, process *testSourceTaskProcess) {
	t.Helper()
	process.mu.Lock()
	stops, waits, bounded := process.stopCalls, process.waitCalls, process.stopBounded
	process.mu.Unlock()
	if stops != 1 || waits == 0 || !bounded {
		t.Fatalf("worker settlement stops=%d waits=%d bounded=%v", stops, waits, bounded)
	}
}

func TestSourceTaskMaterializationRejectsBoundedAggregateOutput(t *testing.T) {
	t.Parallel()
	launcher := &testSourceTaskLauncher{
		pathSource: &testFullPathSource{}, materializers: SourceTaskMaterializers{
			"authority": &testMaterializer{body: []byte("output-too-large")},
		},
	}
	executor := &supervisedExecutor{
		runtimeDir: shortTaskRuntimeDir(t), launcher: launcher,
		deadlines: StandardOperationDeadlines(), materializerOutputLimit: 5,
		identity: testSourceTaskIdentity(),
	}
	materialization, err := executor.Materialize(t.Context(), testSourceMaterializationTask(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	source := materialization.Objects[0].Content
	reader, err := source.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	readErr := error(nil)
	if _, err := io.ReadAll(reader); err == nil || !strings.Contains(err.Error(), "bounded size") {
		t.Fatalf("oversized materialization stream = %v", err)
	} else {
		readErr = err
	}
	_ = errors.Join(reader.Settle(readErr), reader.Wait(context.Background()))
	if err := source.Close(); err == nil || !strings.Contains(err.Error(), "bounded size") {
		t.Fatalf("oversized materialization close = %v", err)
	}
}

func TestSecurePathSourceRejectsSymlinkAncestorAndFIFOFileRoot(t *testing.T) {
	t.Parallel()
	temporary := t.TempDir()
	real := filepath.Join(temporary, "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(temporary, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	source := securePathSource{}
	root := RootSpec{Authority: "authority", ID: "root", Path: filepath.Join(link, "child"), Kind: RootDirectory, Generation: 1}
	if _, err := source.RootIdentity(t.Context(), root); err == nil {
		t.Fatal("symlinked ancestor was accepted")
	}
	fifo := filepath.Join(temporary, "fifo")
	if err := mkfifo(fifo); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}
	root = RootSpec{Authority: "authority", ID: "root", Path: fifo, Kind: RootFile, Generation: 1}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := source.RootIdentity(ctx, root); err == nil {
		t.Fatal("FIFO exact-file root was accepted")
	}
}

func testSourceTaskRoots() []RootSpec {
	return []RootSpec{{
		Authority: "authority", ID: "root", Path: "/source", Kind: RootDirectory, Generation: 1,
		ExpectedIdentity: FileIdentity{VolumeUUID: "volume", Inode: 1},
	}}
}

func testSourceMaterializationTask(t *testing.T, payload []byte) MaterializationTask {
	t.Helper()
	directory := t.TempDir()
	directory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "value"), []byte("input"), 0o600); err != nil {
		t.Fatal(err)
	}
	root := testPinnedRoot(t, RootSpec{Authority: "authority", ID: "root", Path: directory, Kind: RootDirectory, Generation: 1})
	expected, err := (securePathSource{}).Stat(t.Context(), root, "value")
	if err != nil {
		t.Fatal(err)
	}
	return MaterializationTask{
		Fence: testRootFence(t, []RootSpec{root}), Roots: []RootSpec{root},
		Tenants: []tenant.TenantSpec{{
			OwnerID: "owner", ID: "tenant", Mount: tenant.MountSpec{PresentationRoot: "/present"},
			Backing: tenant.BackingSpec{Root: "/backing"},
			Content: tenant.ContentSource{ID: "authority"}, Generation: 1,
			Traits: tenant.TenantTraits{Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: catalog.PresentMount},
		}},
		Request:  MaterializationRequest{Logical: "logical", Inputs: []PathRef{{Root: root.ID, Relative: "value"}}, Payload: payload},
		Expected: []PhysicalEntry{expected},
	}
}

func shaFingerprint(value string) Fingerprint {
	var result Fingerprint
	copy(result[:], []byte(value))
	return result
}

func shortTaskRuntimeDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "fkt-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return canonical
}

func mkfifo(path string) error {
	return syscallMkfifo(path)
}
