package sourceauthority

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/daemonkit"
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
	maxScanReadBytes        = 1 << 20
	maxScanSnapshotEntries  = 10_000_000
	maxScanSnapshotBytes    = 1 << 30
	maxMaterializerPayload  = 1 << 20
	maxMaterializerObjects  = 100_000
	maxMaterializerOutput   = 1 << 30
	maxMutationPayload      = 1 << 30

	sourceTaskUndelivered     = "was not delivered"
	sourceTaskDeliveryUnknown = "delivery is unknown"

	sourceTaskUploadAction  byte = 4
	sourceTaskUploadRequest byte = 5

	sourceTaskOpStage           = "source.stage"
	sourceTaskOpUpload          = "source.upload"
	sourceTaskOpRootIdentity    = "source.root_identity"
	sourceTaskOpStat            = "source.stat"
	sourceTaskOpScan            = "source.scan"
	sourceTaskOpScanRead        = "source.scan_read"
	sourceTaskOpMaterialize     = "source.materialize"
	sourceTaskOpMaterializeRead = "source.materialize_read"
	sourceTaskOpMaterializeDone = "source.materialize_done"
	sourceTaskOpMutation        = "source.mutate"
	sourceTaskOpMutationGet     = "source.mutation_inspect"
	sourceTaskOpMutationAck     = "source.mutation_ack"
	sourceTaskOpMutationDrop    = "source.mutation_abandon"
	sourceTaskOpMutationList    = "source.mutation_proofs"
	sourceTaskOpMutationGC      = "source.mutation_forget"
)

// SourceTaskProcessSpec identifies one private, one-task source child.
type SourceTaskProcessSpec struct {
	Arguments []string
	Identity  SourceTaskIdentity
}

// SourceTaskProcess is one fixed-signed supervised one-task child.
type SourceTaskProcess interface {
	// Business opens the unary lane the exact spawned child serves.
	Business(context.Context, daemonkit.Contract) (*daemonkit.Business, error)
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
	client    sourceSessionClient
	caller    spawnedCaller
	temporary string
	roots     [32]byte
	token     uint64
	total     uint64

	ctx        context.Context
	cancel     context.CancelFunc
	stopCaller func() bool

	nextMu    sync.Mutex
	stateMu   sync.Mutex
	settle    sync.Once
	buffer    []json.RawMessage
	eof       bool
	count     uint64
	delivered uint64
	bytes     uint64
	last      indexKey
	pending   *PhysicalEntry
	closed    bool
	settleErr error
}

type sourceTaskRequest struct {
	Protocol uint16 `json:"protocol"`
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

type sourceTaskScanReadRequest struct {
	Protocol uint16 `json:"protocol"`
	Cursor   uint64 `json:"cursor"`
	Limit    int    `json:"limit"`
}

type sourceTaskScanReadResponse struct {
	Protocol uint16            `json:"protocol"`
	Cursor   uint64            `json:"cursor"`
	Entries  []json.RawMessage `json:"entries,omitempty"`
	EOF      bool              `json:"eof"`
}

type sourceTaskUploadRequestBody struct {
	Protocol uint16 `json:"protocol"`
	Kind     byte   `json:"kind"`
	Index    uint32 `json:"index"`
	Cursor   uint32 `json:"cursor"`
	Payload  []byte `json:"payload,omitempty"`
	End      bool   `json:"end"`
}

type sourceTaskUploadResponse struct {
	Protocol uint16 `json:"protocol"`
	Cursor   uint32 `json:"cursor"`
}

type sourceTaskMaterializeRequest struct {
	Protocol    uint16                   `json:"protocol"`
	Fence       Fence                    `json:"fence"`
	Logical     LogicalID                `json:"logical"`
	PayloadSize int                      `json:"payload_size"`
	Config      sourceTaskConfigManifest `json:"config"`
}

type sourceTaskMaterializeReadRequest struct {
	Protocol uint16 `json:"protocol"`
	Index    uint32 `json:"index"`
	Offset   int64  `json:"offset"`
	Limit    int    `json:"limit"`
}

type sourceTaskMaterializeReadResponse struct {
	Protocol uint16 `json:"protocol"`
	Index    uint32 `json:"index"`
	Offset   int64  `json:"offset"`
	Payload  []byte `json:"payload,omitempty"`
	EOF      bool   `json:"eof"`
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
}

type sourceTaskMaterializeResponse struct {
	Protocol uint16                            `json:"protocol"`
	Metadata sourceTaskMaterializationMetadata `json:"metadata"`
}

type sourceTaskMaterializeTerminal struct {
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
	Page     []byte             `json:"page,omitempty"`
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

// materializationProducer pulls the child's staged projection content one
// bounded page at a time and settles the child once every reader releases.
type materializationProducer struct {
	owner     *supervisedExecutor
	process   SourceTaskProcess
	client    sourceSessionClient
	caller    spawnedCaller
	ctx       context.Context
	cancel    context.CancelFunc
	temporary string

	mu                  sync.Mutex
	err                 error
	closed              int
	settledReaders      int
	total               int
	written             int64
	settleOnce          sync.Once
	done                chan struct{}
	files               []*streamedContentSource
	expectedLogical     LogicalID
	expectedFingerprint Fingerprint
	expectedObjects     int
	maxOutput           int64
}

type streamedContentSource struct {
	owner     *materializationProducer
	index     uint32
	openMu    sync.Mutex
	opened    bool
	closeOnce sync.Once
}

// pagedContentReader is one projection's content, read forward through bounded
// unary pages rather than a server-pushed stream.
type pagedContentReader struct {
	source *streamedContentSource
	ctx    context.Context
	stop   func() bool

	offset int64
	buffer []byte
	eof    bool

	settleOnce sync.Once
	settleMu   sync.Mutex
	settleErr  error
	cause      error
}

func (r *pagedContentReader) Read(buffer []byte) (int, error) {
	if len(r.buffer) == 0 {
		if r.eof {
			return 0, io.EOF
		}
		if err := r.fetch(); err != nil {
			return 0, err
		}
		if len(r.buffer) == 0 {
			return 0, io.EOF
		}
	}
	count := copy(buffer, r.buffer)
	r.buffer = r.buffer[count:]
	return count, nil
}

func (r *pagedContentReader) fetch() error {
	r.settleMu.Lock()
	cause := r.cause
	r.settleMu.Unlock()
	if cause != nil {
		return cause
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}
	response, err := r.source.owner.readContent(r.ctx, r.source.index, r.offset)
	if err != nil {
		return err
	}
	r.offset += int64(len(response.Payload))
	r.buffer = response.Payload
	r.eof = response.EOF
	if len(response.Payload) == 0 && !response.EOF {
		return errors.New("sourceauthority: materializer content page made no progress")
	}
	return nil
}

func (r *pagedContentReader) Settle(cause error) error {
	r.settleOnce.Do(func() {
		r.stop()
		r.settleMu.Lock()
		r.cause = cause
		r.settleMu.Unlock()
		r.source.owner.readerSettledOne()
		if cause != nil {
			r.source.owner.fail(cause)
		}
	})
	r.settleMu.Lock()
	defer r.settleMu.Unlock()
	return r.settleErr
}

func (r *pagedContentReader) Wait(ctx context.Context) error {
	r.settleMu.Lock()
	cause := r.cause
	r.settleMu.Unlock()
	if cause != nil {
		return errors.Join(cause, r.source.owner.result())
	}
	if err := ctx.Err(); err != nil {
		_ = r.Settle(err)
		return errors.Join(err, r.source.owner.result())
	}
	if !r.source.owner.allReadersSettled() {
		return nil
	}
	return r.source.owner.awaitTerminal(ctx)
}

// NewExecutor composes the persistent FSEvents observer with disposable,
// one-task filesystem and materialization children.
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
	process, client, caller, temporary, err := e.start(scanCtx)
	if err != nil {
		stopCaller()
		cancel()
		return nil, err
	}
	fail := func(cause error) (ScanSession, error) {
		stopCaller()
		cancel()
		return nil, e.failTask(process, client, temporary, cause)
	}
	if err := sendSourceTaskPages(scanCtx, caller, manifest, emit); err != nil {
		return fail(err)
	}
	var response sourceTaskScanResponse
	if err := e.callTask(scanCtx, caller, sourceTaskOpScan, sourceTaskScanRequest{
		Protocol: sourceTaskProtocol, Limit: maxScanPageSize, Config: manifest,
	}, &response); err != nil {
		return fail(err)
	}
	if response.Protocol != sourceTaskProtocol || response.Count > maxScanSnapshotEntries {
		return fail(errors.New("sourceauthority: invalid source scan response"))
	}
	session := &streamedScanSession{
		owner: e, process: process, client: client, caller: caller, temporary: temporary,
		roots: scanRootsDigest(roots), token: scanSessionSequence.Add(1), total: response.Count,
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
	for len(s.buffer) == 0 {
		if s.eof {
			return PhysicalEntry{}, false, s.complete()
		}
		if err := s.ctx.Err(); err != nil {
			return PhysicalEntry{}, false, s.finish(false, err)
		}
		if err := ctx.Err(); err != nil {
			return PhysicalEntry{}, false, s.finish(false, ctx.Err())
		}
		if err := s.fetch(ctx); err != nil {
			return PhysicalEntry{}, false, s.finish(false, err)
		}
	}
	payload := s.buffer[0]
	s.buffer = s.buffer[1:]
	if s.count == maxScanSnapshotEntries || len(payload) == 0 || len(payload) > sourceTaskPageByteLimit {
		return PhysicalEntry{}, false, s.finish(false,
			errors.New("sourceauthority: source snapshot exceeds its bounded entry or frame limit"))
	}
	written := s.bytes + uint64(len(payload))
	if written < s.bytes || written > maxScanSnapshotBytes {
		return PhysicalEntry{}, false, s.finish(false,
			errors.New("sourceauthority: source snapshot exceeds its bounded streaming limit"))
	}
	var entry PhysicalEntry
	if err := decodeSourceTaskBounded(payload, &entry, sourceTaskPageByteLimit); err != nil {
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

func (s *streamedScanSession) fetch(ctx context.Context) error {
	var response sourceTaskScanReadResponse
	if err := s.owner.callTask(ctx, s.caller, sourceTaskOpScanRead, sourceTaskScanReadRequest{
		Protocol: sourceTaskProtocol, Cursor: s.count, Limit: maxScanPageSize,
	}, &response); err != nil {
		return err
	}
	if response.Protocol != sourceTaskProtocol || response.Cursor != s.count ||
		len(response.Entries) > maxScanPageSize ||
		(len(response.Entries) == 0 && !response.EOF) {
		return errors.New("sourceauthority: source snapshot page escaped its cursor")
	}
	s.buffer = response.Entries
	s.eof = response.EOF
	return nil
}

func (s *streamedScanSession) complete() error {
	if s.count != s.total {
		return s.finish(false, errors.New("sourceauthority: source snapshot terminal did not match its stream"))
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
	process, client, caller, temporary, err := e.start(ctx)
	if err != nil {
		return Materialization{}, err
	}
	fail := func(cause error) (Materialization, error) {
		return Materialization{}, e.failTask(process, client, temporary, cause)
	}
	if err := sendSourceTaskPages(ctx, caller, manifest, emit); err != nil {
		return fail(err)
	}
	payloadBytes := append([]byte(nil), task.Request.Payload...)
	if len(payloadBytes) != 0 {
		if err := uploadSourceTaskBytes(ctx, caller, sourceTaskUploadRequest, 0, payloadBytes); err != nil {
			return fail(err)
		}
	}
	fence := task.Fence
	fence.Streams = nil
	var response sourceTaskMaterializeResponse
	if err := e.callTask(ctx, caller, sourceTaskOpMaterialize, sourceTaskMaterializeRequest{
		Protocol: sourceTaskProtocol, Fence: fence, Logical: task.Request.Logical,
		PayloadSize: len(payloadBytes), Config: manifest,
	}, &response); err != nil {
		return fail(err)
	}
	metadata := response.Metadata
	if response.Protocol != sourceTaskProtocol || metadata.Protocol != sourceTaskProtocol ||
		metadata.Logical != task.Request.Logical ||
		len(metadata.Objects) == 0 || len(metadata.Objects) > maxMaterializerObjects {
		return fail(errors.New("sourceauthority: invalid materializer projection metadata"))
	}
	producer, materialization, err := newMaterializationProducer(
		e, ctx, cancel, process, client, caller, temporary, metadata, e.materializationOutputLimit(),
	)
	if err != nil {
		return fail(err)
	}
	producerOwnsContext = true
	if producer.total == 0 {
		producer.settleTerminal()
		if err := producer.result(); err != nil {
			return Materialization{}, err
		}
	}
	return materialization, nil
}

func (e *supervisedExecutor) runUnary(ctx context.Context, op string, request, response any) error {
	return e.runUnaryWithin(ctx, e.operationDeadlines().Unary, op, request, response)
}

func (e *supervisedExecutor) runUnaryWithin(
	ctx context.Context,
	deadline time.Duration,
	op string,
	request, response any,
) error {
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	process, client, caller, temporary, err := e.start(ctx)
	if err != nil {
		return err
	}
	if err := e.callTask(ctx, caller, op, request, response); err != nil {
		return e.failTask(process, client, temporary, err)
	}
	if err := ctx.Err(); err != nil {
		return e.failTask(process, client, temporary, err)
	}
	return e.finishTask(process, client, temporary)
}

func (e *supervisedExecutor) callTask(
	ctx context.Context,
	caller spawnedCaller,
	op string,
	request, response any,
) error {
	payload, err := encodeSourceTaskRequest(request)
	if err != nil {
		return err
	}
	body, err := caller.call(ctx, op, payload)
	if err != nil {
		return classifySourceTaskCallError(err)
	}
	return decodeSourceTaskBounded(body, response, sourceTaskResponseByteLimit)
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

func (e *supervisedExecutor) start(
	ctx context.Context,
) (SourceTaskProcess, sourceSessionClient, spawnedCaller, string, error) {
	deadlines, err := sourceTaskSpawnedDeadlines(e.operationDeadlines())
	if err != nil {
		return nil, nil, spawnedCaller{}, "", err
	}
	temporary, err := os.MkdirTemp(e.runtimeDir, "source-task-")
	if err != nil {
		return nil, nil, spawnedCaller{}, "", fmt.Errorf("sourceauthority: create source task directory: %w", err)
	}
	if err := os.Chmod(temporary, 0o700); err != nil {
		_ = os.RemoveAll(temporary)
		return nil, nil, spawnedCaller{}, "", err
	}
	arguments, err := SourceTaskChildArguments(temporary, e.runtimeDir, e.identity)
	if err != nil {
		_ = os.RemoveAll(temporary)
		return nil, nil, spawnedCaller{}, "", err
	}
	process, err := e.launcher.LaunchSourceTask(ctx, SourceTaskProcessSpec{
		Arguments: arguments, Identity: e.identity,
	})
	if err != nil {
		_ = os.RemoveAll(temporary)
		return nil, nil, spawnedCaller{}, "", fmt.Errorf("sourceauthority: launch source task child: %w", err)
	}
	client, err := openSourceTaskProcessSession(ctx, process)
	if err != nil {
		return nil, nil, spawnedCaller{}, "",
			errors.Join(err, stopSourceTask(process), os.RemoveAll(temporary))
	}
	return process, client, spawnedCaller{client: client, deadlines: deadlines}, temporary, nil
}

func (e *supervisedExecutor) finishTask(process SourceTaskProcess, client sourceSessionClient, temporary string) error {
	clientErr := closeSourceSession(client, sourceTaskCloseTimeout)
	waitCtx, cancel := context.WithTimeout(context.Background(), sourceTaskCloseTimeout)
	defer cancel()
	waitErr := process.Wait(waitCtx)
	if waitErr != nil {
		waitErr = errors.Join(waitErr, stopSourceTask(process))
	}
	return errors.Join(clientErr, waitErr, os.RemoveAll(temporary))
}

func (e *supervisedExecutor) failTask(process SourceTaskProcess, client sourceSessionClient, temporary string, cause error) error {
	return errors.Join(
		cause, closeSourceSession(client, sourceTaskCloseTimeout),
		stopSourceTask(process), os.RemoveAll(temporary),
	)
}

func stopSourceTask(process SourceTaskProcess) error {
	if process == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), sourceTaskCloseTimeout)
	defer cancel()
	return errors.Join(process.Stop(ctx), process.Wait(ctx))
}

// classifySourceTaskCallError restores the cancellation and deadline identities
// a product failure crossing the lane as text would otherwise lose, and states
// delivery only as far as daemonkit proves it: non-delivery is claimed on
// Undispatched alone, every other failure reports an unknown outcome.
func classifySourceTaskCallError(err error) error {
	detail := boundedSourceTaskError(err)
	delivery := sourceTaskDeliveryUnknown
	if daemonkit.Undispatched(err) {
		delivery = sourceTaskUndelivered
	}
	switch {
	case errors.Is(err, context.Canceled) || strings.HasSuffix(detail, context.Canceled.Error()):
		return fmt.Errorf("sourceauthority: source task %s: %w", delivery, context.Canceled)
	case errors.Is(err, context.DeadlineExceeded) || strings.HasSuffix(detail, context.DeadlineExceeded.Error()):
		return fmt.Errorf("sourceauthority: source task %s: %w", delivery, context.DeadlineExceeded)
	default:
		return fmt.Errorf("sourceauthority: source task %s: %s", delivery, detail)
	}
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

// uploadSourceTaskBytes stages one payload as bounded unary chunks, closed by
// the exact end marker the streamed carrier used as its boundary chunk.
func uploadSourceTaskBytes(
	ctx context.Context,
	caller spawnedCaller,
	kind byte,
	index uint32,
	value []byte,
) error {
	return uploadSourceTaskReader(ctx, caller, kind, index, bytes.NewReader(value))
}

func uploadSourceTaskReader(
	ctx context.Context,
	caller spawnedCaller,
	kind byte,
	index uint32,
	reader io.Reader,
) error {
	buffer := make([]byte, sourceTaskChunkSize)
	var cursor uint32
	var total int64
	for {
		count, readErr := reader.Read(buffer)
		if count != 0 {
			total += int64(count)
			if total > maxMutationPayload {
				return errors.New("sourceauthority: mutation request content exceeds its bounded size")
			}
			if err := sendSourceTaskUpload(ctx, caller, sourceTaskUploadRequestBody{
				Protocol: sourceTaskProtocol, Kind: kind, Index: index,
				Cursor: cursor, Payload: buffer[:count],
			}); err != nil {
				return err
			}
			cursor++
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return readErr
			}
			break
		}
	}
	return sendSourceTaskUpload(ctx, caller, sourceTaskUploadRequestBody{
		Protocol: sourceTaskProtocol, Kind: kind, Index: index, Cursor: cursor, End: true,
	})
}

func sendSourceTaskUpload(ctx context.Context, caller spawnedCaller, request sourceTaskUploadRequestBody) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	body, err := caller.call(ctx, sourceTaskOpUpload, payload)
	if err != nil {
		return classifySourceTaskCallError(err)
	}
	var response sourceTaskUploadResponse
	if err := decodeSourceTaskBounded(body, &response, sourceTaskResponseByteLimit); err != nil {
		return err
	}
	if response.Protocol != sourceTaskProtocol || response.Cursor != request.Cursor {
		return errors.New("sourceauthority: source task upload acknowledgement is invalid")
	}
	return nil
}

func newMaterializationProducer(
	owner *supervisedExecutor,
	ctx context.Context,
	cancel context.CancelFunc,
	process SourceTaskProcess,
	client sourceSessionClient,
	caller spawnedCaller,
	temporary string,
	metadata sourceTaskMaterializationMetadata,
	maxOutput int64,
) (*materializationProducer, Materialization, error) {
	producer := &materializationProducer{
		owner: owner, process: process, client: client, caller: caller,
		ctx: ctx, cancel: cancel, temporary: temporary, done: make(chan struct{}),
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
			source := &streamedContentSource{owner: producer, index: uint32(index)}
			producer.files = append(producer.files, source)
			producer.total++
			projection.Content = source
		}
		materialization.Objects[index] = projection
	}
	// A caller that abandons the materialization without releasing its content
	// still settles the child: the operation's own deadline retires it.
	context.AfterFunc(ctx, func() { producer.settleWith(ctx.Err()) })
	return producer, materialization, nil
}

func (p *materializationProducer) readContent(
	ctx context.Context,
	index uint32,
	offset int64,
) (sourceTaskMaterializeReadResponse, error) {
	var response sourceTaskMaterializeReadResponse
	if err := p.owner.callTask(ctx, p.caller, sourceTaskOpMaterializeRead, sourceTaskMaterializeReadRequest{
		Protocol: sourceTaskProtocol, Index: index, Offset: offset, Limit: sourceTaskChunkSize,
	}, &response); err != nil {
		p.fail(err)
		return sourceTaskMaterializeReadResponse{}, err
	}
	if response.Protocol != sourceTaskProtocol || response.Index != index || response.Offset != offset ||
		len(response.Payload) > sourceTaskChunkSize {
		err := errors.New("sourceauthority: invalid materializer content page")
		p.fail(err)
		return sourceTaskMaterializeReadResponse{}, err
	}
	p.mu.Lock()
	p.written += int64(len(response.Payload))
	over := p.written > p.maxOutput
	p.mu.Unlock()
	if over {
		err := errors.New("sourceauthority: materializer output exceeds its bounded size")
		p.fail(err)
		return sourceTaskMaterializeReadResponse{}, err
	}
	return response, nil
}

func (p *materializationProducer) fail(cause error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = cause
	}
	p.mu.Unlock()
}

// settleTerminal posts the materialization terminal, verifies its proof against
// the metadata the child already committed to, and retires the child.
func (p *materializationProducer) settleTerminal() { p.settleWith(nil) }

// settleWith starts the one terminal this materialization gets, recording cause
// only when it is the reason the terminal runs at all.
func (p *materializationProducer) settleWith(cause error) {
	p.settleOnce.Do(func() {
		if cause != nil {
			p.fail(cause)
		}
		go p.runTerminal()
	})
}

func (p *materializationProducer) runTerminal() {
	defer close(p.done)
	defer p.cancel()
	{
		p.mu.Lock()
		runErr := p.err
		p.mu.Unlock()
		if runErr == nil {
			var terminal sourceTaskMaterializeTerminal
			runErr = p.owner.callTask(p.ctx, p.caller, sourceTaskOpMaterializeDone, sourceTaskRequest{
				Protocol: sourceTaskProtocol,
			}, &terminal)
			if runErr == nil && (terminal.Protocol != sourceTaskProtocol ||
				terminal.Logical != p.expectedLogical || terminal.Fingerprint != p.expectedFingerprint ||
				terminal.Objects != p.expectedObjects || len(terminal.Error) > sourceTaskErrorByteLimit) {
				runErr = errors.New("sourceauthority: invalid materializer terminal response")
			}
			if runErr == nil && terminal.Error != "" {
				runErr = errors.New(terminal.Error)
			}
		}
		if runErr == nil {
			runErr = p.owner.finishTask(p.process, p.client, p.temporary)
		} else {
			runErr = errors.Join(runErr, p.owner.failTask(p.process, p.client, p.temporary, nil))
		}
		p.mu.Lock()
		p.err = runErr
		p.mu.Unlock()
	}
}

func (p *materializationProducer) result() error {
	p.settleTerminal()
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// awaitTerminal posts the terminal and waits out its bounded grace; a terminal
// that outlives the grace is a hung child, retired rather than waited on.
func (p *materializationProducer) awaitTerminal(ctx context.Context) error {
	p.settleTerminal()
	timer := time.NewTimer(sourceTaskTerminalGrace)
	defer timer.Stop()
	select {
	case <-p.done:
	case <-ctx.Done():
		p.cancel()
	case <-timer.C:
		p.cancel()
	}
	return p.result()
}

func (p *materializationProducer) allReadersSettled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.settledReaders == p.total
}

func (p *materializationProducer) readerSettledOne() {
	p.mu.Lock()
	p.settledReaders++
	p.mu.Unlock()
}

func (p *materializationProducer) release() error {
	p.mu.Lock()
	p.closed++
	released := p.closed == p.total
	p.mu.Unlock()
	if released {
		return p.awaitTerminal(context.Background())
	}
	return nil
}

func (s *streamedContentSource) Open(ctx context.Context) (contentstream.Source, error) {
	s.openMu.Lock()
	defer s.openMu.Unlock()
	if s.opened {
		return nil, errors.New("sourceauthority: materialized content stream was already opened")
	}
	s.opened = true
	readCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(ctx, cancel)
	return &pagedContentReader{
		source: s, ctx: readCtx,
		stop: func() bool { defer cancel(); return stop() },
	}, nil
}

func (s *streamedContentSource) Close() error {
	var err error
	s.closeOnce.Do(func() { err = s.owner.release() })
	return err
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
