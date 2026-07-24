package sourceauthority

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
)

const (
	sourceTaskBuild         = "fusekit-source-task-v1"
	sourceTaskProtocol      = uint16(1)
	sourceTaskChildArg      = "--fusekit-source-task-child"
	sourceTaskCloseTimeout  = 5 * time.Second
	sourceTaskTerminalGrace = 500 * time.Millisecond
	sourceTaskChunkSize     = 64 << 10
	maxScanPageSize         = 1_000
	maxScanSnapshotEntries  = 10_000_000
	maxScanSnapshotBytes    = 1 << 30
	maxMaterializerPayload  = 1 << 20
	maxMaterializerObjects  = 100_000
	maxMaterializerOutput   = 1 << 30
	maxMutationPayload      = 1 << 30

	sourceTaskChunkMetadata    byte = 1
	sourceTaskChunkContent     byte = 2
	sourceTaskChunkEnd         byte = 3
	sourceTaskChunkAction      byte = 4
	sourceTaskChunkRequest     byte = 5
	sourceTaskChunkMutationEnd byte = 6

	sourceTaskOpRootIdentity wire.Op = "source.root_identity"
	sourceTaskOpStat         wire.Op = "source.stat"
	sourceTaskOpScan         wire.Op = "source.scan"
	sourceTaskOpMaterialize  wire.Op = "source.materialize"
	sourceTaskOpMutation     wire.Op = "source.mutate"
	sourceTaskOpMutationGet  wire.Op = "source.mutation_inspect"
	sourceTaskOpMutationAck  wire.Op = "source.mutation_ack"
	sourceTaskOpMutationDrop wire.Op = "source.mutation_abandon"
	sourceTaskOpMutationList wire.Op = "source.mutation_proofs"
	sourceTaskOpMutationGC   wire.Op = "source.mutation_forget"
)

// SourceTaskProcessSpec identifies one private, one-request source child.
type SourceTaskProcessSpec struct {
	Arguments []string
	Identity  SourceTaskIdentity
}

// SourceTaskProcess is one fixed-signed supervised one-request child.
type SourceTaskProcess interface {
	// SessionEndpoint transfers the daemonkit-managed session bound to the exact process.
	SessionEndpoint(context.Context) (proc.SpawnedSessionEndpoint, error)
	Wait(context.Context) error
	Stop(context.Context) error
}

// SourceTaskProcessLauncher starts the fixed signed executable in source-task mode.
type SourceTaskProcessLauncher interface {
	LaunchSourceTask(context.Context, SourceTaskProcessSpec) (SourceTaskProcess, error)
}

type supervisedExecutor struct {
	EventBackend
	runtimeDir              string
	launcher                SourceTaskProcessLauncher
	deadlines               OperationDeadlines
	materializerOutputLimit int64
	identity                SourceTaskIdentity

	mu     sync.Mutex
	scans  map[*streamedScanSession]struct{}
	closed bool
}

var scanSessionSequence atomic.Uint64

type streamedScanSession struct {
	owner     *supervisedExecutor
	process   SourceTaskProcess
	client    *wire.SpawnedClient
	call      *wire.ClientCall
	temporary string
	chunks    <-chan wire.Chunk
	roots     [32]byte
	token     uint64

	ctx        context.Context
	cancel     context.CancelFunc
	stopCaller func() bool

	nextMu    sync.Mutex
	stateMu   sync.Mutex
	settle    sync.Once
	count     uint64
	delivered uint64
	bytes     uint64
	last      indexKey
	pending   *PhysicalEntry
	closed    bool
	settleErr error
}

type sourceTaskRootRequest struct {
	Protocol uint16   `json:"protocol"`
	Root     RootSpec `json:"root"`
}

type sourceTaskStatRequest struct {
	Protocol uint16   `json:"protocol"`
	Root     RootSpec `json:"root"`
	Relative string   `json:"relative"`
}

type sourceTaskScanRequest struct {
	Protocol uint16                   `json:"protocol"`
	Limit    int                      `json:"limit"`
	Config   sourceTaskConfigManifest `json:"config"`
}

type sourceTaskMaterializeRequest struct {
	Protocol    uint16                   `json:"protocol"`
	Fence       Fence                    `json:"fence"`
	Logical     LogicalID                `json:"logical"`
	PayloadSize int                      `json:"payload_size"`
	Config      sourceTaskConfigManifest `json:"config"`
}

type sourceTaskMutationRequest struct {
	Protocol          uint16                   `json:"protocol"`
	Fence             Fence                    `json:"fence"`
	OperationID       catalog.MutationID       `json:"operation_id"`
	ExpectationDigest Fingerprint              `json:"expectation_digest"`
	HasRequestContent bool                     `json:"has_request_content"`
	Config            sourceTaskConfigManifest `json:"config"`
}

type sourceTaskMutationInspectionRequest struct {
	Protocol uint16                    `json:"protocol"`
	Request  MutationInspectionRequest `json:"request"`
}

type sourceTaskMutationTerminalRequest struct {
	Protocol uint16                `json:"protocol"`
	Proof    MutationTerminalProof `json:"proof"`
}

type sourceTaskMutationProofsRequest struct {
	Protocol  uint16                   `json:"protocol"`
	Authority causal.SourceAuthorityID `json:"authority"`
	After     catalog.MutationID       `json:"after"`
	Limit     uint16                   `json:"limit"`
}

type sourceTaskIdentityResponse struct {
	Protocol uint16       `json:"protocol"`
	Identity FileIdentity `json:"identity"`
}

type sourceTaskStatResponse struct {
	Protocol uint16        `json:"protocol"`
	Entry    PhysicalEntry `json:"entry"`
}

type sourceTaskScanResponse struct {
	Protocol uint16 `json:"protocol"`
	Count    uint64 `json:"count"`
	Error    string `json:"error,omitempty"`
}

type sourceTaskMaterializeResponse struct {
	Protocol    uint16      `json:"protocol"`
	Logical     LogicalID   `json:"logical"`
	Fingerprint Fingerprint `json:"fingerprint"`
	Objects     int         `json:"objects"`
	Error       string      `json:"error,omitempty"`
}

type sourceTaskMutationResponse struct {
	Protocol uint16          `json:"protocol"`
	Receipt  MutationReceipt `json:"receipt"`
}

type sourceTaskMutationInspectionResponse struct {
	Protocol   uint16             `json:"protocol"`
	Inspection MutationInspection `json:"inspection"`
}

type sourceTaskMutationTerminalResponse struct {
	Protocol uint16 `json:"protocol"`
}

type sourceTaskMutationProofsResponse struct {
	Protocol uint16             `json:"protocol"`
	Count    uint32             `json:"count"`
	Digest   Fingerprint        `json:"digest"`
	Next     catalog.MutationID `json:"next"`
	More     bool               `json:"more"`
	Error    string             `json:"error,omitempty"`
}

type sourceTaskMaterializationMetadata struct {
	Protocol    uint16                 `json:"protocol"`
	Logical     LogicalID              `json:"logical"`
	Fingerprint Fingerprint            `json:"fingerprint"`
	Objects     []sourceTaskProjection `json:"objects"`
}

type sourceTaskProjection struct {
	Tenant              string    `json:"tenant"`
	Generation          uint64    `json:"generation"`
	Parent              LogicalID `json:"parent"`
	Name                string    `json:"name"`
	Kind                uint8     `json:"kind"`
	Mode                uint32    `json:"mode"`
	LinkTarget          string    `json:"link_target"`
	MountVisible        bool      `json:"mount_visible"`
	FileProviderVisible bool      `json:"file_provider_visible"`
	HasContent          bool      `json:"has_content"`
}

type materializationProducer struct {
	process   SourceTaskProcess
	client    *wire.SpawnedClient
	call      *wire.ClientCall
	ctx       context.Context
	cancel    context.CancelFunc
	temporary string

	mu                  sync.Mutex
	err                 error
	closed              int
	total               int
	done                chan struct{}
	remove              sync.Once
	files               []*streamedContentSource
	expectedLogical     LogicalID
	expectedFingerprint Fingerprint
	expectedObjects     int
	maxOutput           int64
	stopOnce            sync.Once
	stopErr             error
	terminateOnce       sync.Once
	terminateErr        error
	readerSettled       int
}

type streamedContentSource struct {
	owner      *materializationProducer
	reader     *io.PipeReader
	writer     *io.PipeWriter
	ready      chan struct{}
	writerOnce sync.Once
	closeOnce  sync.Once
	openMu     sync.Mutex
	opened     bool
	index      uint32
}

type contextPipeReader struct {
	reader     *io.PipeReader
	stop       func() bool
	source     *streamedContentSource
	settleOnce sync.Once
	settleMu   sync.Mutex
	settleErr  error
	cause      error
}

func (r *contextPipeReader) Read(buffer []byte) (int, error) { return r.reader.Read(buffer) }

func (r *contextPipeReader) Settle(cause error) error {
	r.settleOnce.Do(func() {
		r.stop()
		r.settleMu.Lock()
		r.cause = cause
		if cause == nil {
			r.settleErr = r.reader.Close()
		} else {
			r.settleErr = r.reader.CloseWithError(cause)
		}
		r.settleMu.Unlock()
		r.source.owner.readerSettledOne()
		if cause != nil {
			r.source.owner.call.Cancel()
		}
	})
	r.settleMu.Lock()
	defer r.settleMu.Unlock()
	return r.settleErr
}

func (r *contextPipeReader) Wait(ctx context.Context) error {
	r.settleMu.Lock()
	cause := r.cause
	r.settleMu.Unlock()
	if cause != nil {
		return errors.Join(cause, r.source.owner.terminateAndJoin())
	}
	select {
	case <-r.source.ready:
		if !r.source.owner.allReadersSettled() {
			return nil
		}
		return r.source.owner.awaitTerminal(ctx)
	case <-ctx.Done():
		_ = r.Settle(ctx.Err())
		return errors.Join(ctx.Err(), r.source.owner.terminateAndJoin())
	}
}

// NewExecutor composes the persistent FSEvents observer with disposable,
// one-request filesystem and materialization children.
func NewExecutor(
	runtimeDir string,
	observerLauncher ObserverProcessLauncher,
	taskLauncher SourceTaskProcessLauncher,
	deadlines OperationDeadlines,
	identity SourceTaskIdentity,
) (Executor, error) {
	if err := deadlines.validate(); err != nil {
		return nil, err
	}
	if err := validateMutationJournalDirectory(context.Background(), runtimeDir); err != nil {
		return nil, err
	}
	backend, err := NewFSEventsBackend(observerLauncher, deadlines)
	if err != nil {
		return nil, err
	}
	if taskLauncher == nil {
		return nil, errors.New("sourceauthority: source task process launcher is required")
	}
	if err := validateSourceTaskChildConfig(SourceTaskChildConfig{
		TaskRoot: filepath.Join(runtimeDir, "source-task-validation"), JournalRoot: runtimeDir,
		Identity: identity,
	}, false); err != nil {
		return nil, fmt.Errorf("sourceauthority: source task identity: %w", err)
	}
	return &supervisedExecutor{
		EventBackend: backend, runtimeDir: runtimeDir, launcher: taskLauncher,
		deadlines: deadlines, materializerOutputLimit: maxMaterializerOutput,
		scans: make(map[*streamedScanSession]struct{}), identity: identity,
	}, nil
}

func (e *supervisedExecutor) RootIdentity(ctx context.Context, root RootSpec) (FileIdentity, error) {
	var response sourceTaskIdentityResponse
	err := e.runUnary(ctx, sourceTaskOpRootIdentity, sourceTaskRootRequest{Protocol: sourceTaskProtocol, Root: root}, &response)
	if err != nil {
		return FileIdentity{}, err
	}
	if response.Protocol != sourceTaskProtocol || response.Identity.VolumeUUID == "" || response.Identity.Inode == 0 {
		return FileIdentity{}, errors.New("sourceauthority: invalid source identity response")
	}
	return response.Identity, nil
}

func (e *supervisedExecutor) Stat(ctx context.Context, root RootSpec, relative string) (PhysicalEntry, error) {
	var response sourceTaskStatResponse
	err := e.runUnary(ctx, sourceTaskOpStat, sourceTaskStatRequest{
		Protocol: sourceTaskProtocol, Root: root, Relative: relative,
	}, &response)
	if err != nil {
		return PhysicalEntry{}, err
	}
	if response.Protocol != sourceTaskProtocol || response.Entry.Root != root.ID || response.Entry.Relative != relative {
		return PhysicalEntry{}, errors.New("sourceauthority: invalid source stat response")
	}
	return response.Entry, nil
}

func (e *supervisedExecutor) BeginScan(ctx context.Context, roots []RootSpec) (ScanSession, error) {
	emit := sourceTaskPageEmitterForScan(roots)
	manifest, err := planSourceTaskPages(emit)
	if err != nil {
		return nil, err
	}
	if manifest.Roots != uint32(len(roots)) || manifest.Roots == 0 {
		return nil, errors.New("sourceauthority: source scan roots exceed the protocol limit")
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, ErrClosed
	}
	e.mu.Unlock()
	scanCtx, cancel := context.WithTimeout(context.Background(), e.operationDeadlines().Scan)
	stopCaller := context.AfterFunc(ctx, cancel)
	process, client, temporary, err := e.start(scanCtx)
	if err != nil {
		stopCaller()
		cancel()
		return nil, err
	}
	request := sourceTaskScanRequest{
		Protocol: sourceTaskProtocol, Limit: maxScanPageSize, Config: manifest,
	}
	payload, err := encodeSourceTaskRequest(request)
	if err != nil {
		stopCaller()
		cancel()
		return nil, e.failTask(process, client, temporary, err)
	}
	call, err := client.OpenStream(scanCtx, sourceTaskOpScan, "", payload, false)
	if err != nil {
		stopCaller()
		cancel()
		return nil, e.failTask(process, client, temporary, err)
	}
	if err := sendSourceTaskPages(scanCtx, call, manifest, emit); err != nil {
		stopCaller()
		cancel()
		return nil, e.failCall(process, client, call, temporary, err)
	}
	if err := call.CloseSend(scanCtx); err != nil {
		stopCaller()
		cancel()
		return nil, e.failCall(process, client, call, temporary, err)
	}
	session := &streamedScanSession{
		owner: e, process: process, client: client, call: call, temporary: temporary,
		chunks: call.Chunks(), roots: scanRootsDigest(roots), token: scanSessionSequence.Add(1),
		ctx: scanCtx, cancel: cancel, stopCaller: stopCaller,
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		_ = session.Close()
		return nil, ErrClosed
	}
	if e.scans == nil {
		e.scans = make(map[*streamedScanSession]struct{})
	}
	e.scans[session] = struct{}{}
	e.mu.Unlock()
	return session, nil
}

func (s *streamedScanSession) Next(ctx context.Context, limit int) (ScanPage, error) {
	if limit <= 0 || limit > maxScanPageSize {
		return ScanPage{}, errors.New("sourceauthority: source scan limit is invalid")
	}
	s.nextMu.Lock()
	defer s.nextMu.Unlock()
	if s.isClosed() {
		return ScanPage{}, ErrClosed
	}
	result := ScanPage{Entries: make([]PhysicalEntry, 0, limit)}
	for len(result.Entries) < limit {
		entry, ok, err := s.nextEntry(ctx)
		if err != nil {
			return ScanPage{}, err
		}
		if !ok {
			return result, nil
		}
		result.Entries = append(result.Entries, entry)
		s.delivered++
	}
	entry, ok, err := s.nextEntry(ctx)
	if err != nil {
		return ScanPage{}, err
	}
	if !ok {
		return result, nil
	}
	s.pending = &entry
	result.Next = ScanCursor(fmt.Sprintf("%x:%d:%d", s.roots[:8], s.token, s.delivered))
	return result, nil
}

func (s *streamedScanSession) nextEntry(ctx context.Context) (PhysicalEntry, bool, error) {
	if s.pending != nil {
		entry := *s.pending
		s.pending = nil
		return entry, true, nil
	}
	for {
		select {
		case <-s.ctx.Done():
			return PhysicalEntry{}, false, s.finish(false, s.ctx.Err())
		case <-ctx.Done():
			return PhysicalEntry{}, false, s.finish(false, ctx.Err())
		case chunk, ok := <-s.chunks:
			if !ok {
				return PhysicalEntry{}, false, s.complete(ctx)
			}
			if chunk.End {
				continue
			}
			if s.count == maxScanSnapshotEntries || len(chunk.Payload) == 0 ||
				len(chunk.Payload) > sourceTaskPageByteLimit {
				return PhysicalEntry{}, false, s.finish(false,
					errors.New("sourceauthority: source snapshot exceeds its bounded entry or frame limit"))
			}
			written := s.bytes + uint64(len(chunk.Payload))
			if written < s.bytes || written > maxScanSnapshotBytes {
				return PhysicalEntry{}, false, s.finish(false,
					errors.New("sourceauthority: source snapshot exceeds its bounded streaming limit"))
			}
			var entry PhysicalEntry
			if err := decodeSourceTaskBounded(chunk.Payload, &entry, sourceTaskPageByteLimit); err != nil {
				return PhysicalEntry{}, false, s.finish(false, err)
			}
			current := indexKey{root: entry.Root, relative: entry.Relative}
			if current.root == "" || current.relative == "" || !entry.Exists ||
				(s.last.root != "" && (current.root < s.last.root ||
					(current.root == s.last.root && current.relative <= s.last.relative))) {
				return PhysicalEntry{}, false, s.finish(false,
					errors.New("sourceauthority: source snapshot entry is invalid or unordered"))
			}
			s.count++
			s.bytes = written
			s.last = current
			return entry, true, nil
		}
	}
}

func (s *streamedScanSession) complete(ctx context.Context) error {
	result, err := s.call.Response(ctx)
	if err != nil {
		return s.finish(false, err)
	}
	if err := ctx.Err(); err != nil {
		return s.finish(false, err)
	}
	var response sourceTaskScanResponse
	if err := decodeSourceTaskResult(result, &response); err != nil {
		return s.finish(false, err)
	}
	if response.Protocol != sourceTaskProtocol || response.Count != s.count ||
		len(response.Error) > sourceTaskErrorByteLimit {
		return s.finish(false, errors.New("sourceauthority: source snapshot terminal did not match its stream"))
	}
	if response.Error != "" {
		return s.finish(false, errors.New(response.Error))
	}
	return s.finish(true, nil)
}

func (s *streamedScanSession) Close() error {
	s.cancel()
	s.nextMu.Lock()
	defer s.nextMu.Unlock()
	return s.finish(false, nil)
}

func (s *streamedScanSession) isClosed() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.closed
}

func (s *streamedScanSession) finish(natural bool, cause error) error {
	s.settle.Do(func() {
		s.cancel()
		s.stopCaller()
		if natural {
			s.settleErr = s.owner.finishTask(s.process, s.client, s.temporary)
		} else {
			s.call.Cancel()
			s.settleErr = s.owner.failTask(s.process, s.client, s.temporary, nil)
		}
		s.stateMu.Lock()
		s.closed = true
		s.stateMu.Unlock()
		s.owner.mu.Lock()
		delete(s.owner.scans, s)
		s.owner.mu.Unlock()
	})
	return errors.Join(cause, s.settleErr)
}

// Close reaps any live scan child and rejects further finite work.
func (e *supervisedExecutor) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	scans := make([]*streamedScanSession, 0, len(e.scans))
	for scan := range e.scans {
		scans = append(scans, scan)
	}
	e.mu.Unlock()
	var result error
	for _, scan := range scans {
		result = errors.Join(result, scan.Close())
	}
	return result
}

func (e *supervisedExecutor) Materialize(ctx context.Context, task MaterializationTask) (Materialization, error) {
	if len(task.Request.Payload) > maxMaterializerPayload {
		return Materialization{}, errors.New("sourceauthority: materializer payload exceeds the protocol limit")
	}
	if task.Fence.Authority == "" || task.Fence.AuthorityGeneration == 0 ||
		len(task.Roots) == 0 || len(task.Tenants) == 0 || len(task.Request.Inputs) == 0 ||
		len(task.Expected) != len(task.Request.Inputs) {
		return Materialization{}, errors.New("sourceauthority: incomplete materialization configuration")
	}
	emit := sourceTaskPageEmitterForMaterialization(task)
	manifest, err := planSourceTaskPages(emit)
	if err != nil {
		return Materialization{}, err
	}
	if manifest.Roots != uint32(len(task.Roots)) || manifest.Roots == 0 ||
		manifest.Checkpoints != uint32(len(task.Fence.Streams)) ||
		manifest.Tenants != uint32(len(task.Tenants)) || manifest.Tenants == 0 ||
		manifest.Inputs != uint32(len(task.Request.Inputs)) || manifest.Inputs == 0 ||
		manifest.ExpectedEntries != manifest.Inputs {
		return Materialization{}, errors.New("sourceauthority: materialization configuration exceeds the protocol limit")
	}
	ctx, cancel := context.WithTimeout(ctx, e.operationDeadlines().Materialize)
	producerOwnsContext := false
	defer func() {
		if !producerOwnsContext {
			cancel()
		}
	}()
	process, client, temporary, err := e.start(ctx)
	if err != nil {
		return Materialization{}, err
	}
	payloadBytes := append([]byte(nil), task.Request.Payload...)
	fence := task.Fence
	fence.Streams = nil
	request := sourceTaskMaterializeRequest{
		Protocol: sourceTaskProtocol, Fence: fence, Logical: task.Request.Logical,
		PayloadSize: len(payloadBytes), Config: manifest,
	}
	payload, err := encodeSourceTaskRequest(request)
	if err != nil {
		return Materialization{}, e.failTask(process, client, temporary, err)
	}
	call, err := client.OpenStream(ctx, sourceTaskOpMaterialize, "", payload, false)
	if err != nil {
		return Materialization{}, e.failTask(process, client, temporary, err)
	}
	if err := sendSourceTaskPages(ctx, call, manifest, emit); err != nil {
		return Materialization{}, e.failCall(process, client, call, temporary, err)
	}
	if len(payloadBytes) != 0 {
		for len(payloadBytes) != 0 {
			length := min(len(payloadBytes), sourceTaskChunkSize)
			if err := call.SendChunk(ctx, encodeStreamChunk(sourceTaskChunkRequest, 0, payloadBytes[:length])); err != nil {
				return Materialization{}, e.failCall(process, client, call, temporary, err)
			}
			payloadBytes = payloadBytes[length:]
		}
	}
	if err := call.CloseSend(ctx); err != nil {
		return Materialization{}, e.failCall(process, client, call, temporary, err)
	}
	var first wire.Chunk
	var ok bool
	select {
	case first, ok = <-call.Chunks():
	case <-ctx.Done():
		return Materialization{}, e.failCall(process, client, call, temporary, ctx.Err())
	}
	if !ok || first.End || len(first.Payload) < 2 || len(first.Payload) > sourceTaskPageByteLimit+1 ||
		first.Payload[0] != sourceTaskChunkMetadata {
		return Materialization{}, e.failCall(process, client, call, temporary, errors.New("sourceauthority: materializer omitted projection metadata"))
	}
	var metadata sourceTaskMaterializationMetadata
	if err := decodeSourceTaskBounded(first.Payload[1:], &metadata, sourceTaskPageByteLimit); err != nil {
		return Materialization{}, e.failCall(process, client, call, temporary, err)
	}
	if metadata.Protocol != sourceTaskProtocol || metadata.Logical != task.Request.Logical ||
		len(metadata.Objects) == 0 || len(metadata.Objects) > maxMaterializerObjects {
		return Materialization{}, e.failCall(process, client, call, temporary, errors.New("sourceauthority: invalid materializer projection metadata"))
	}
	producer, materialization, err := newMaterializationProducer(
		ctx, cancel, process, client, call, temporary, metadata, e.materializationOutputLimit(),
	)
	if err != nil {
		return Materialization{}, e.failCall(process, client, call, temporary, err)
	}
	producerOwnsContext = true
	go producer.run()
	if producer.total == 0 {
		<-producer.done
		if err := producer.result(); err != nil {
			return Materialization{}, err
		}
	}
	return materialization, nil
}

func (e *supervisedExecutor) runUnary(ctx context.Context, op wire.Op, request, response any) error {
	return e.runUnaryWithin(ctx, e.operationDeadlines().Unary, op, request, response)
}

func (e *supervisedExecutor) runUnaryWithin(
	ctx context.Context,
	deadline time.Duration,
	op wire.Op,
	request, response any,
) error {
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	process, client, temporary, err := e.start(ctx)
	if err != nil {
		return err
	}
	payload, err := encodeSourceTaskRequest(request)
	if err != nil {
		return e.failTask(process, client, temporary, err)
	}
	result, err := client.Call(ctx, op, "", payload)
	if err != nil {
		return e.failTask(process, client, temporary, err)
	}
	if err := ctx.Err(); err != nil {
		return e.failTask(process, client, temporary, err)
	}
	if err := decodeSourceTaskResult(result, response); err != nil {
		return e.failTask(process, client, temporary, err)
	}
	return e.finishTask(process, client, temporary)
}

func (e *supervisedExecutor) operationDeadlines() OperationDeadlines {
	if e.deadlines.validate() == nil {
		return e.deadlines
	}
	return StandardOperationDeadlines()
}

func (e *supervisedExecutor) materializationOutputLimit() int64 {
	if e.materializerOutputLimit > 0 && e.materializerOutputLimit <= maxMaterializerOutput {
		return e.materializerOutputLimit
	}
	return maxMaterializerOutput
}

func (e *supervisedExecutor) start(ctx context.Context) (SourceTaskProcess, *wire.SpawnedClient, string, error) {
	temporary, err := os.MkdirTemp(e.runtimeDir, "source-task-")
	if err != nil {
		return nil, nil, "", fmt.Errorf("sourceauthority: create source task directory: %w", err)
	}
	if err := os.Chmod(temporary, 0o700); err != nil {
		_ = os.RemoveAll(temporary)
		return nil, nil, "", err
	}
	arguments, err := SourceTaskChildArguments(temporary, e.runtimeDir, e.identity)
	if err != nil {
		_ = os.RemoveAll(temporary)
		return nil, nil, "", err
	}
	process, err := e.launcher.LaunchSourceTask(ctx, SourceTaskProcessSpec{
		Arguments: arguments, Identity: e.identity,
	})
	if err != nil {
		_ = os.RemoveAll(temporary)
		return nil, nil, "", fmt.Errorf("sourceauthority: launch source task child: %w", err)
	}
	endpoint, err := process.SessionEndpoint(ctx)
	if err != nil {
		return nil, nil, "", errors.Join(err, stopSourceTask(process), os.RemoveAll(temporary))
	}
	client, err := newSourceTaskSpawnedClient(ctx, endpoint)
	if err != nil {
		return nil, nil, "", errors.Join(err, stopSourceTask(process), os.RemoveAll(temporary))
	}
	return process, client, temporary, nil
}

func (e *supervisedExecutor) finishTask(process SourceTaskProcess, client *wire.SpawnedClient, temporary string) error {
	clientErr := client.Close()
	waitCtx, cancel := context.WithTimeout(context.Background(), sourceTaskCloseTimeout)
	defer cancel()
	waitErr := process.Wait(waitCtx)
	if waitErr != nil {
		waitErr = errors.Join(waitErr, stopSourceTask(process))
	}
	return errors.Join(clientErr, waitErr, os.RemoveAll(temporary))
}

func (e *supervisedExecutor) failCall(process SourceTaskProcess, client *wire.SpawnedClient, call *wire.ClientCall, temporary string, cause error) error {
	call.Cancel()
	return errors.Join(cause, e.failTask(process, client, temporary, nil))
}

func (e *supervisedExecutor) failTask(process SourceTaskProcess, client *wire.SpawnedClient, temporary string, cause error) error {
	if client != nil {
		cause = errors.Join(cause, client.Abort(cause))
	}
	return errors.Join(cause, stopSourceTask(process), os.RemoveAll(temporary))
}

func stopSourceTask(process SourceTaskProcess) error {
	if process == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), sourceTaskCloseTimeout)
	defer cancel()
	return process.Stop(ctx)
}

func decodeSourceTaskResult(result wire.Result, target any) error {
	if result.Outcome != wire.Delivered || result.Response.Rejected || result.Response.Err != "" {
		detail := result.Response.Err
		if detail == "" {
			detail = result.Response.Reason
		}
		detail = boundedSourceTaskError(errors.New(detail))
		switch detail {
		case context.Canceled.Error():
			return fmt.Errorf("sourceauthority: source task was not delivered: %w", context.Canceled)
		case context.DeadlineExceeded.Error():
			return fmt.Errorf("sourceauthority: source task was not delivered: %w", context.DeadlineExceeded)
		default:
			return fmt.Errorf("sourceauthority: source task was not delivered: %s", detail)
		}
	}
	return decodeSourceTaskBounded(result.Response.Payload, target, sourceTaskJSONByteLimit)
}

func decodeSourceTask(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("sourceauthority: trailing source task payload")
	}
	return nil
}

func newMaterializationProducer(
	ctx context.Context,
	cancel context.CancelFunc,
	process SourceTaskProcess,
	client *wire.SpawnedClient,
	call *wire.ClientCall,
	temporary string,
	metadata sourceTaskMaterializationMetadata,
	maxOutput int64,
) (*materializationProducer, Materialization, error) {
	producer := &materializationProducer{
		process: process, client: client, call: call, ctx: ctx, cancel: cancel,
		temporary: temporary, done: make(chan struct{}),
		maxOutput:       maxOutput,
		expectedLogical: metadata.Logical, expectedFingerprint: metadata.Fingerprint,
		expectedObjects: len(metadata.Objects),
	}
	materialization := Materialization{
		Logical: metadata.Logical, Fingerprint: metadata.Fingerprint,
		Objects: make([]Projection, len(metadata.Objects)),
	}
	for index, encoded := range metadata.Objects {
		projection, err := decodeProjection(encoded)
		if err != nil {
			return nil, Materialization{}, err
		}
		if encoded.HasContent {
			reader, writer := io.Pipe()
			source := &streamedContentSource{
				owner: producer, reader: reader, writer: writer,
				ready: make(chan struct{}), index: uint32(index),
			}
			producer.files = append(producer.files, source)
			producer.total++
			projection.Content = source
		}
		materialization.Objects[index] = projection
	}
	return producer, materialization, nil
}

func (p *materializationProducer) run() {
	defer p.cancel()
	defer close(p.done)
	ready := make(map[uint32]*streamedContentSource, len(p.files))
	for _, source := range p.files {
		ready[source.index] = source
	}
	var runErr error
	var written int64
	chunks := p.call.Chunks()
	for runErr == nil {
		var chunk wire.Chunk
		var ok bool
		select {
		case chunk, ok = <-chunks:
		case <-p.ctx.Done():
			runErr = p.ctx.Err()
			continue
		}
		if !ok {
			break
		}
		if chunk.End {
			continue
		}
		if len(chunk.Payload) < 5 || len(chunk.Payload) > sourceTaskChunkSize+5 {
			runErr = errors.New("sourceauthority: invalid materializer stream chunk")
			break
		}
		kind := chunk.Payload[0]
		index := binary.BigEndian.Uint32(chunk.Payload[1:5])
		source, exists := ready[index]
		if !exists {
			runErr = errors.New("sourceauthority: materializer stream index is invalid")
			break
		}
		switch kind {
		case sourceTaskChunkContent:
			if len(chunk.Payload) == 5 {
				runErr = errors.New("sourceauthority: empty materializer content chunk")
				break
			}
			written += int64(len(chunk.Payload) - 5)
			if written > p.maxOutput {
				runErr = errors.New("sourceauthority: materializer output exceeds its bounded size")
				break
			}
			_, runErr = source.writer.Write(chunk.Payload[5:])
		case sourceTaskChunkEnd:
			if len(chunk.Payload) != 5 {
				runErr = errors.New("sourceauthority: invalid materializer end chunk")
				break
			}
			source.finishWriter(nil)
			delete(ready, index)
		default:
			runErr = errors.New("sourceauthority: invalid materializer stream chunk kind")
		}
		if runErr != nil {
			break
		}
	}
	if runErr == nil {
		runErr = p.ctx.Err()
	}
	var response sourceTaskMaterializeResponse
	if runErr == nil {
		result, err := p.call.Response(p.ctx)
		if err != nil {
			runErr = err
		} else {
			runErr = decodeSourceTaskResult(result, &response)
		}
	}
	if runErr == nil && len(ready) != 0 {
		runErr = errors.New("sourceauthority: materializer stream ended before every file")
	}
	if runErr == nil && (response.Protocol != sourceTaskProtocol || response.Logical != p.expectedLogical ||
		response.Fingerprint != p.expectedFingerprint || response.Objects != p.expectedObjects ||
		len(response.Error) > sourceTaskErrorByteLimit) {
		runErr = errors.New("sourceauthority: invalid materializer terminal response")
	}
	if runErr == nil && response.Error != "" {
		runErr = errors.New(response.Error)
	}
	if runErr == nil {
		clientErr := p.client.Close()
		waitCtx, cancel := context.WithTimeout(context.Background(), sourceTaskCloseTimeout)
		waitErr := p.process.Wait(waitCtx)
		cancel()
		if waitErr != nil {
			waitErr = errors.Join(waitErr, p.stop())
		}
		runErr = errors.Join(clientErr, waitErr)
	} else {
		p.call.Cancel()
		runErr = errors.Join(runErr, p.client.Abort(runErr), p.stop())
	}
	p.mu.Lock()
	p.err = runErr
	p.mu.Unlock()
	for _, source := range ready {
		source.finishWriter(runErr)
	}
	p.cleanupIfReleased()
}

func (p *materializationProducer) result() error {
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *materializationProducer) readerSettledOne() {
	p.mu.Lock()
	p.readerSettled++
	p.mu.Unlock()
}

func (p *materializationProducer) allReadersSettled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readerSettled == p.total
}

func (p *materializationProducer) awaitTerminal(ctx context.Context) error {
	timer := time.NewTimer(sourceTaskTerminalGrace)
	defer timer.Stop()
	select {
	case <-p.done:
		return p.result()
	case <-ctx.Done():
		return errors.Join(ctx.Err(), p.terminateAndJoin())
	case <-timer.C:
		return p.terminateAndJoin()
	}
}

func (p *materializationProducer) terminateAndJoin() error {
	p.terminateOnce.Do(func() {
		p.call.Cancel()
		p.terminateErr = errors.Join(p.client.Abort(ErrClosed), p.stop())
	})
	return errors.Join(p.terminateErr, p.result())
}

func (p *materializationProducer) release() error {
	p.mu.Lock()
	p.closed++
	closed := p.closed
	total := p.total
	p.mu.Unlock()
	if closed == total {
		settled := false
		if p.allFilesReady() {
			timer := time.NewTimer(sourceTaskTerminalGrace)
			select {
			case <-p.done:
				settled = true
			case <-timer.C:
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		if !settled {
			select {
			case <-p.done:
			default:
				_ = p.terminateAndJoin()
			}
		}
		p.cleanupIfReleased()
		return p.result()
	}
	return nil
}

func (p *materializationProducer) allFilesReady() bool {
	for _, file := range p.files {
		select {
		case <-file.ready:
		default:
			return false
		}
	}
	return true
}

func (p *materializationProducer) cleanupIfReleased() {
	p.mu.Lock()
	released := p.closed == p.total
	p.mu.Unlock()
	if released {
		p.remove.Do(func() { _ = os.RemoveAll(p.temporary) })
	}
}

func (s *streamedContentSource) Open(ctx context.Context) (contentstream.Source, error) {
	s.openMu.Lock()
	defer s.openMu.Unlock()
	if s.opened {
		return nil, errors.New("sourceauthority: materialized content stream was already opened")
	}
	s.opened = true
	stop := context.AfterFunc(ctx, func() { _ = s.reader.CloseWithError(ctx.Err()) })
	return &contextPipeReader{reader: s.reader, stop: stop, source: s}, nil
}

func (s *streamedContentSource) Close() error {
	var err error
	s.closeOnce.Do(func() {
		err = errors.Join(s.reader.CloseWithError(ErrClosed), s.owner.release())
	})
	return err
}

func (s *streamedContentSource) finishWriter(err error) {
	s.writerOnce.Do(func() {
		if err == nil {
			_ = s.writer.Close()
		} else {
			_ = s.writer.CloseWithError(err)
		}
		close(s.ready)
	})
}

func (p *materializationProducer) stop() error {
	p.stopOnce.Do(func() { p.stopErr = stopSourceTask(p.process) })
	return p.stopErr
}

func encodeStreamChunk(kind byte, index uint32, payload []byte) []byte {
	result := make([]byte, 5+len(payload))
	result[0] = kind
	binary.BigEndian.PutUint32(result[1:5], index)
	copy(result[5:], payload)
	return result
}

func decodeProjection(encoded sourceTaskProjection) (Projection, error) {
	projection := Projection{
		Tenant: catalog.TenantID(encoded.Tenant), Generation: catalog.Generation(encoded.Generation),
		Parent: encoded.Parent, Name: encoded.Name, Kind: catalog.Kind(encoded.Kind),
		Mode: encoded.Mode, LinkTarget: encoded.LinkTarget,
		Visibility: catalog.Visibility{
			Mount: encoded.MountVisible, FileProvider: encoded.FileProviderVisible,
		},
	}
	if projection.Tenant == "" || projection.Generation == 0 || projection.Name == "" {
		return Projection{}, errors.New("sourceauthority: invalid materializer projection identity")
	}
	if projection.Kind != catalog.KindFile && projection.Kind != catalog.KindDirectory && projection.Kind != catalog.KindSymlink {
		return Projection{}, errors.New("sourceauthority: invalid materializer projection kind")
	}
	return projection, nil
}

var _ Executor = (*supervisedExecutor)(nil)
var _ ContentSource = (*streamedContentSource)(nil)
