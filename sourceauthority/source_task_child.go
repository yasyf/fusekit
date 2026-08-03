package sourceauthority

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"reflect"
	"sync"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
	_ "modernc.org/sqlite"
)

const (
	scanStagePageSize = 4 << 10
	maxScanStageBytes = 2 << 30
)

// SourceTaskMaterializers is the fixed signed child's exact authority registry.
// Unknown authorities are rejected before request payloads are consumed.
type SourceTaskMaterializers map[causal.SourceAuthorityID]Materializer

type sourceTaskChild struct {
	mu                   sync.Mutex
	pathSource           PathSource
	materializers        SourceTaskMaterializers
	runtimeDir           string
	journalRoot          string
	afterMutation        func(context.Context, MutationReceipt) error
	afterMaterialization func(context.Context) error

	bound     bool
	sessionID uint64
	task      string
	stage     sourceTaskConfigStage
	uploads   map[sourceTaskUploadKey]*sourceTaskUpload

	scan     *childScanStage
	staged   *childMaterialization
	settled  bool
	terminal sourceTaskMaterializeTerminal

	cancel context.CancelFunc
}

type sourceTaskUploadKey struct {
	kind  byte
	index uint32
}

type sourceTaskUpload struct {
	file   *os.File
	hash   hash.Hash
	size   int64
	cursor uint32
	ended  bool
	digest [32]byte
}

// childMaterialization is the projection content the child staged on disk so
// bounded unary reads can serve it at any offset, in any order.
type childMaterialization struct {
	files map[uint32]*os.File
	sizes map[uint32]int64
}

type fullPathScanner interface {
	scanAll(context.Context, []RootSpec, func(PhysicalEntry) error) error
}

// RunSourceTaskChild recognizes and serves one exact, one-task source child invocation.
func RunSourceTaskChild(
	ctx context.Context,
	arguments []string,
	materializers SourceTaskMaterializers,
) (bool, error) {
	if len(arguments) == 0 || arguments[0] != sourceTaskChildArg {
		return false, nil
	}
	config, _, err := ParseSourceTaskChildArguments(arguments)
	if err != nil {
		return true, err
	}
	if err := validateMutationJournalDirectory(ctx, config.JournalRoot); err != nil {
		return true, err
	}
	return true, serveSourceTaskChild(
		ctx, newSecurePathSource(), materializers, config.TaskRoot, config.JournalRoot,
	)
}

func serveSourceTaskChild(
	ctx context.Context,
	pathSource PathSource,
	materializers SourceTaskMaterializers,
	runtimeDir string,
	journalRoot string,
) error {
	return serveSourceTaskChildWithHook(ctx, pathSource, materializers, runtimeDir, journalRoot, nil)
}

func serveSourceTaskChildWithHook(
	ctx context.Context,
	pathSource PathSource,
	materializers SourceTaskMaterializers,
	runtimeDir string,
	journalRoot string,
	afterMutation func(context.Context, MutationReceipt) error,
) error {
	return serveSourceTaskChildWithHooks(ctx, pathSource, materializers, runtimeDir, journalRoot, afterMutation, nil)
}

func serveSourceTaskChildWithHooks(
	ctx context.Context,
	pathSource PathSource,
	materializers SourceTaskMaterializers,
	runtimeDir string,
	journalRoot string,
	afterMutation func(context.Context, MutationReceipt) error,
	afterMaterialization func(context.Context) error,
) error {
	child, serveCtx, cancel, err := newSourceTaskChild(
		ctx, pathSource, materializers, runtimeDir, journalRoot, afterMutation, afterMaterialization,
	)
	if err != nil {
		return err
	}
	defer cancel()
	serveErr := daemonkit.ServeSpawned(serveCtx, sourceTaskSpawnedContract(), child.handle)
	return errors.Join(serveErr, child.releaseStaged())
}

func newSourceTaskChild(
	ctx context.Context,
	pathSource PathSource,
	materializers SourceTaskMaterializers,
	runtimeDir string,
	journalRoot string,
	afterMutation func(context.Context, MutationReceipt) error,
	afterMaterialization func(context.Context) error,
) (*sourceTaskChild, context.Context, context.CancelFunc, error) {
	if pathSource == nil {
		return nil, nil, nil, errors.New("sourceauthority: source task path source is required")
	}
	registered := make(SourceTaskMaterializers, len(materializers))
	for authority, materializer := range materializers {
		if authority == "" || materializer == nil {
			return nil, nil, nil, errors.New("sourceauthority: invalid source task materializer registration")
		}
		registered[authority] = materializer
	}
	serveCtx, cancel := context.WithCancel(ctx)
	child := &sourceTaskChild{
		pathSource: pathSource, materializers: registered,
		runtimeDir: runtimeDir, journalRoot: journalRoot,
		afterMutation: afterMutation, afterMaterialization: afterMaterialization,
		uploads: make(map[sourceTaskUploadKey]*sourceTaskUpload), cancel: cancel,
	}
	return child, serveCtx, cancel, nil
}

func (c *sourceTaskChild) handle(ctx context.Context, request daemonkit.Request) (daemonkit.Reply, error) {
	handler, registered := sourceTaskHandlers(c)[request.Op]
	if !registered {
		return daemonkit.Reply{}, fmt.Errorf("sourceauthority: unknown source task operation %q", request.Op)
	}
	if err := c.bind(request); err != nil {
		return daemonkit.Reply{}, errors.New(boundedSourceTaskError(err))
	}
	value, err := handler(ctx, request)
	if err != nil {
		return daemonkit.Reply{}, errors.New(boundedSourceTaskError(err))
	}
	body, err := encodeSourceTaskResponse(value)
	if err != nil {
		return daemonkit.Reply{}, errors.New(boundedSourceTaskError(err))
	}
	return daemonkit.Reply{Body: body}, nil
}

func sourceTaskHandlers(
	child *sourceTaskChild,
) map[string]func(context.Context, daemonkit.Request) (any, error) {
	return map[string]func(context.Context, daemonkit.Request) (any, error){
		sourceTaskOpStage:           child.handleStage,
		sourceTaskOpUpload:          child.handleUpload,
		sourceTaskOpRootIdentity:    child.handleRootIdentity,
		sourceTaskOpStat:            child.handleStat,
		sourceTaskOpScan:            child.handleScan,
		sourceTaskOpScanRead:        child.handleScanRead,
		sourceTaskOpMaterialize:     child.handleMaterialize,
		sourceTaskOpMaterializeRead: child.handleMaterializeRead,
		sourceTaskOpMaterializeDone: child.handleMaterializeDone,
		sourceTaskOpMutation:        child.handleMutation,
		sourceTaskOpMutationGet:     child.handleMutationInspect,
		sourceTaskOpMutationAck:     child.handleMutationAcknowledge,
		sourceTaskOpMutationDrop:    child.handleMutationAbandon,
		sourceTaskOpMutationList:    child.handleMutationProofs,
		sourceTaskOpMutationGC:      child.handleMutationForget,
	}
}

// bind fences every request to the one session ServeSpawned admits.
func (c *sourceTaskChild) bind(request daemonkit.Request) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.bound {
		c.bound, c.sessionID = true, request.Session.ID()
		if done := request.Session.Done(); done != nil {
			go func() {
				<-done
				c.cancel()
			}()
		}
		return nil
	}
	if request.Session.ID() != c.sessionID {
		return errors.New("sourceauthority: source task request escaped its session")
	}
	return nil
}

// claim admits exactly one task per child; its staged inputs precede it and its
// bounded reads follow it, all on the one session.
func (c *sourceTaskChild) claim(op string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.task != "" {
		return errors.New("sourceauthority: source task accepts exactly one request")
	}
	c.task = op
	return nil
}

func (c *sourceTaskChild) claimed(op string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.task != op {
		return errors.New("sourceauthority: source task continuation escaped its request")
	}
	return nil
}

func (c *sourceTaskChild) staging() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.task != "" {
		return errors.New("sourceauthority: source task input followed its request")
	}
	return nil
}

func (c *sourceTaskChild) handleStage(_ context.Context, request daemonkit.Request) (any, error) {
	if err := c.staging(); err != nil {
		return nil, err
	}
	return c.stage.accept(request.Body)
}

func (c *sourceTaskChild) handleUpload(_ context.Context, request daemonkit.Request) (any, error) {
	if err := c.staging(); err != nil {
		return nil, err
	}
	var input sourceTaskUploadRequestBody
	if err := decodeSourceTaskBounded(request.Body, &input, sourceTaskJSONByteLimit+sourceTaskChunkSize*2); err != nil ||
		input.Protocol != sourceTaskProtocol ||
		(input.Kind != sourceTaskUploadAction && input.Kind != sourceTaskUploadRequest) ||
		len(input.Payload) > sourceTaskChunkSize || (input.End && len(input.Payload) != 0) ||
		(!input.End && len(input.Payload) == 0) {
		return nil, errors.New("sourceauthority: invalid source task upload")
	}
	key := sourceTaskUploadKey{kind: input.Kind, index: input.Index}
	c.mu.Lock()
	defer c.mu.Unlock()
	upload, exists := c.uploads[key]
	if !exists {
		if input.Cursor != 0 {
			return nil, errors.New("sourceauthority: source task upload sequence mismatch")
		}
		file, err := createSourceTaskScratch(c.runtimeDir, "source-mutation-input-")
		if err != nil {
			return nil, err
		}
		upload = &sourceTaskUpload{file: file, hash: sha256.New()}
		c.uploads[key] = upload
	}
	if upload.ended || upload.cursor != input.Cursor {
		return nil, errors.New("sourceauthority: source task upload sequence mismatch")
	}
	if input.End {
		if err := upload.file.Sync(); err != nil {
			return nil, err
		}
		copy(upload.digest[:], upload.hash.Sum(nil))
		upload.ended = true
	} else {
		upload.size += int64(len(input.Payload))
		if upload.size < 0 || upload.size > maxMutationPayload {
			return nil, errors.New("sourceauthority: mutation payload exceeds its bounded size")
		}
		if _, err := upload.file.Write(input.Payload); err != nil {
			return nil, err
		}
		_, _ = upload.hash.Write(input.Payload)
	}
	upload.cursor++
	return sourceTaskUploadResponse{Protocol: sourceTaskProtocol, Cursor: input.Cursor}, nil
}

func (c *sourceTaskChild) takeUpload(kind byte, index uint32, expected int64) (*mutationPayload, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := sourceTaskUploadKey{kind: kind, index: index}
	upload, exists := c.uploads[key]
	if !exists || !upload.ended || (expected >= 0 && upload.size != expected) {
		return nil, errors.New("sourceauthority: staged payload did not match its declared size")
	}
	delete(c.uploads, key)
	return &mutationPayload{file: upload.file, size: upload.size, hash: upload.digest}, nil
}

func (c *sourceTaskChild) discardUploads() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result error
	for key, upload := range c.uploads {
		result = errors.Join(result, upload.file.Close())
		delete(c.uploads, key)
	}
	return result
}

func (c *sourceTaskChild) handleRootIdentity(ctx context.Context, request daemonkit.Request) (any, error) {
	if err := c.claim(request.Op); err != nil {
		return nil, err
	}
	var input sourceTaskRootRequest
	if err := decodeSourceTaskBounded(request.Body, &input, sourceTaskJSONByteLimit); err != nil || input.Protocol != sourceTaskProtocol {
		return nil, errors.New("sourceauthority: invalid root identity request")
	}
	identity, err := c.pathSource.RootIdentity(ctx, input.Root)
	if err != nil {
		return nil, err
	}
	return sourceTaskIdentityResponse{Protocol: sourceTaskProtocol, Identity: identity}, nil
}

func (c *sourceTaskChild) handleStat(ctx context.Context, request daemonkit.Request) (any, error) {
	if err := c.claim(request.Op); err != nil {
		return nil, err
	}
	var input sourceTaskStatRequest
	if err := decodeSourceTaskBounded(request.Body, &input, sourceTaskJSONByteLimit); err != nil || input.Protocol != sourceTaskProtocol {
		return nil, errors.New("sourceauthority: invalid source stat request")
	}
	entry, err := c.pathSource.Stat(ctx, input.Root, input.Relative)
	if err != nil {
		return nil, err
	}
	return sourceTaskStatResponse{Protocol: sourceTaskProtocol, Entry: entry}, nil
}

func (c *sourceTaskChild) handleScan(ctx context.Context, request daemonkit.Request) (any, error) {
	if err := c.claim(request.Op); err != nil {
		return nil, err
	}
	var input sourceTaskScanRequest
	if err := decodeSourceTaskBounded(request.Body, &input, sourceTaskJSONByteLimit); err != nil || input.Protocol != sourceTaskProtocol ||
		input.Limit <= 0 || input.Limit > maxScanPageSize ||
		input.Config.Roots == 0 || input.Config.Roots > sourceTaskRootLimit ||
		input.Config.Checkpoints != 0 || input.Config.Tenants != 0 ||
		input.Config.Inputs != 0 || input.Config.ExpectedEntries != 0 ||
		input.Config.Actions != 0 || input.Config.ExpectedEffects != 0 {
		return nil, errors.New("sourceauthority: invalid source scan request")
	}
	roots := make([]RootSpec, 0, input.Config.Roots)
	if err := c.stage.settle(input.Config, func(page sourceTaskConfigPageBody) error {
		if len(page.Roots) == 0 {
			return errors.New("sourceauthority: source scan received a non-root configuration page")
		}
		roots = append(roots, page.Roots...)
		return nil
	}); err != nil {
		return nil, err
	}
	if len(roots) != int(input.Config.Roots) {
		return nil, errors.New("sourceauthority: source scan root count changed")
	}
	scanner, ok := c.pathSource.(fullPathScanner)
	if !ok {
		return nil, errors.New("sourceauthority: path source does not support streamed snapshots")
	}
	stage, count, err := buildChildScanStage(ctx, c.runtimeDir, scanner, roots)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.scan = &stage
	c.mu.Unlock()
	return sourceTaskScanResponse{Protocol: sourceTaskProtocol, Count: count}, nil
}

func (c *sourceTaskChild) handleScanRead(ctx context.Context, request daemonkit.Request) (any, error) {
	if err := c.claimed(sourceTaskOpScan); err != nil {
		return nil, err
	}
	var input sourceTaskScanReadRequest
	if err := decodeSourceTaskBounded(request.Body, &input, sourceTaskJSONByteLimit); err != nil ||
		input.Protocol != sourceTaskProtocol || input.Limit <= 0 || input.Limit > maxScanPageSize {
		return nil, errors.New("sourceauthority: invalid source scan read request")
	}
	c.mu.Lock()
	stage := c.scan
	c.mu.Unlock()
	if stage == nil {
		return nil, errors.New("sourceauthority: source scan stage is absent")
	}
	rows, err := stage.db.QueryContext(ctx, `
SELECT payload FROM entries ORDER BY root COLLATE BINARY, relative COLLATE BINARY LIMIT ? OFFSET ?`,
		input.Limit, int64(input.Cursor))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	response := sourceTaskScanReadResponse{Protocol: sourceTaskProtocol, Cursor: input.Cursor}
	var bytesRead int
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		if len(payload) == 0 || len(payload) > sourceTaskPageByteLimit {
			return nil, errors.New("sourceauthority: staged scan entry exceeds its byte limit")
		}
		if bytesRead != 0 && bytesRead+len(payload) > maxScanReadBytes {
			break
		}
		bytesRead += len(payload)
		response.Entries = append(response.Entries, json.RawMessage(payload))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	response.EOF = len(response.Entries) == 0
	return response, nil
}

type childScanStage struct {
	db   *sql.DB
	path string
}

func (s *childScanStage) close() error {
	if s == nil {
		return nil
	}
	return errors.Join(s.db.Close(), os.Remove(s.path))
}

func buildChildScanStage(
	ctx context.Context,
	runtimeDir string,
	scanner fullPathScanner,
	roots []RootSpec,
) (_ childScanStage, count uint64, resultErr error) {
	stageFile, err := os.CreateTemp(runtimeDir, "source-snapshot-")
	if err != nil {
		return childScanStage{}, 0, err
	}
	stagePath := stageFile.Name()
	if err := stageFile.Close(); err != nil {
		return childScanStage{}, 0, errors.Join(err, os.Remove(stagePath))
	}
	if err := os.Remove(stagePath); err != nil {
		return childScanStage{}, 0, err
	}
	db, err := sql.Open("sqlite", stagePath)
	if err != nil {
		return childScanStage{}, 0, err
	}
	defer func() {
		if resultErr != nil {
			_ = db.Close()
			_ = os.Remove(stagePath)
		}
	}()
	statements := []string{
		fmt.Sprintf("PRAGMA page_size=%d", scanStagePageSize),
		"PRAGMA journal_mode=DELETE",
		"PRAGMA synchronous=FULL",
		fmt.Sprintf("PRAGMA max_page_count=%d", maxScanStageBytes/scanStagePageSize),
		`CREATE TABLE entries (
    root TEXT NOT NULL,
    relative TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (root, relative)
) WITHOUT ROWID`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return childScanStage{}, 0, err
		}
	}
	if err := os.Chmod(stagePath, 0o600); err != nil {
		return childScanStage{}, 0, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return childScanStage{}, 0, err
	}
	insert, err := tx.PrepareContext(ctx, `INSERT INTO entries(root, relative, payload) VALUES (?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return childScanStage{}, 0, err
	}
	defer func() { _ = insert.Close() }()
	var written uint64
	err = scanner.scanAll(ctx, roots, func(entry PhysicalEntry) error {
		if count == maxScanSnapshotEntries || entry.Root == "" || entry.Relative == "" || !entry.Exists {
			return errors.New("sourceauthority: source snapshot exceeds its entry limit or contains an invalid entry")
		}
		if err := validateSourceTaskStrings(reflect.ValueOf(entry)); err != nil {
			return err
		}
		payload, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		size := uint64(len(payload))
		if size == 0 || size > sourceTaskPageByteLimit ||
			written+size < written || written+size > maxScanSnapshotBytes {
			return errors.New("sourceauthority: source snapshot exceeds its bounded byte or frame limit")
		}
		if _, err := insert.ExecContext(ctx, string(entry.Root), entry.Relative, payload); err != nil {
			return err
		}
		count++
		written += size
		return nil
	})
	if err != nil {
		_ = tx.Rollback()
		return childScanStage{}, 0, err
	}
	if err := tx.Commit(); err != nil {
		return childScanStage{}, 0, err
	}
	info, err := os.Stat(stagePath)
	if err != nil {
		return childScanStage{}, 0, err
	}
	if info.Size() > maxScanStageBytes {
		return childScanStage{}, 0, errors.New("sourceauthority: source snapshot exceeds its bounded child stage")
	}
	return childScanStage{db: db, path: stagePath}, count, nil
}

func (c *sourceTaskChild) handleMaterialize(ctx context.Context, request daemonkit.Request) (any, error) {
	if err := c.claim(request.Op); err != nil {
		return nil, err
	}
	var input sourceTaskMaterializeRequest
	if err := decodeSourceTaskBounded(request.Body, &input, sourceTaskJSONByteLimit); err != nil || input.Protocol != sourceTaskProtocol ||
		input.PayloadSize < 0 || input.PayloadSize > maxMaterializerPayload || input.Fence.Authority == "" ||
		input.Fence.AuthorityGeneration == 0 ||
		input.Logical == "" || len(input.Fence.Streams) != 0 ||
		input.Config.Roots == 0 || input.Config.Tenants == 0 ||
		input.Config.Inputs == 0 || input.Config.ExpectedEntries != input.Config.Inputs ||
		input.Config.Actions != 0 || input.Config.ExpectedEffects != 0 {
		return nil, errors.New("sourceauthority: invalid materialization request")
	}
	task := MaterializationTask{
		Fence:   input.Fence,
		Roots:   make([]RootSpec, 0, input.Config.Roots),
		Tenants: make([]tenant.TenantSpec, 0, input.Config.Tenants),
		Request: MaterializationRequest{
			Logical: input.Logical, Inputs: make([]PathRef, 0, input.Config.Inputs),
		},
		Expected: make([]PhysicalEntry, 0, input.Config.ExpectedEntries),
	}
	phase := 0
	if err := c.stage.settle(input.Config, func(page sourceTaskConfigPageBody) error {
		switch {
		case len(page.Roots) != 0:
			if phase != 0 {
				return errors.New("sourceauthority: materialization root page is out of order")
			}
			task.Roots = append(task.Roots, page.Roots...)
		case len(page.Checkpoints) != 0:
			if phase > 1 {
				return errors.New("sourceauthority: materialization checkpoint page is out of order")
			}
			phase = 1
			task.Fence.Streams = append(task.Fence.Streams, page.Checkpoints...)
		case len(page.Tenants) != 0:
			if phase > 2 {
				return errors.New("sourceauthority: materialization tenant page is out of order")
			}
			phase = 2
			task.Tenants = append(task.Tenants, page.Tenants...)
		case len(page.Inputs) != 0:
			phase = 3
			task.Request.Inputs = append(task.Request.Inputs, page.Inputs...)
			task.Expected = append(task.Expected, page.ExpectedEntries...)
		default:
			return errors.New("sourceauthority: materialization received an invalid configuration page")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	materializer, exists := c.materializers[task.Fence.Authority]
	if !exists {
		return nil, fmt.Errorf("sourceauthority: undeclared materializer %q", task.Fence.Authority)
	}
	payload, err := c.takeMaterializerPayload(input.PayloadSize)
	if err != nil {
		return nil, err
	}
	task.Request.Payload = payload
	if err := validateChildMaterializationTask(task); err != nil {
		return nil, err
	}
	materializerTask, inputs, err := prepareMaterializerTask(ctx, c.runtimeDir, task)
	if err != nil {
		return nil, err
	}
	materialization, err := materializer.Materialize(ctx, materializerTask)
	if err != nil {
		_ = inputs.Close()
		return nil, err
	}
	metadata, err := encodeMaterializationMetadata(task.Request.Logical, materialization)
	if err != nil {
		_ = closeMaterializations([]Materialization{materialization})
		_ = inputs.Close()
		return nil, err
	}
	staged, err := stageMaterializationContent(ctx, c.runtimeDir, materialization)
	cleanup := errors.Join(closeMaterializations([]Materialization{materialization}), inputs.Close())
	if err != nil {
		return nil, errors.Join(err, staged.close(), cleanup)
	}
	c.mu.Lock()
	c.staged = staged
	c.terminal = sourceTaskMaterializeTerminal{
		Protocol: sourceTaskProtocol, Logical: materialization.Logical,
		Fingerprint: materialization.Fingerprint, Objects: len(materialization.Objects),
		Error: boundedSourceTaskError(cleanup),
	}
	c.mu.Unlock()
	return sourceTaskMaterializeResponse{Protocol: sourceTaskProtocol, Metadata: metadata}, nil
}

func (c *sourceTaskChild) takeMaterializerPayload(size int) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	staged, err := c.takeUpload(sourceTaskUploadRequest, 0, int64(size))
	if err != nil {
		return nil, err
	}
	defer func() { _ = staged.file.Close() }()
	payload := make([]byte, size)
	if _, err := io.ReadFull(io.NewSectionReader(staged.file, 0, staged.size), payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (c *sourceTaskChild) handleMaterializeRead(_ context.Context, request daemonkit.Request) (any, error) {
	if err := c.claimed(sourceTaskOpMaterialize); err != nil {
		return nil, err
	}
	var input sourceTaskMaterializeReadRequest
	if err := decodeSourceTaskBounded(request.Body, &input, sourceTaskJSONByteLimit); err != nil ||
		input.Protocol != sourceTaskProtocol || input.Offset < 0 ||
		input.Limit <= 0 || input.Limit > sourceTaskChunkSize {
		return nil, errors.New("sourceauthority: invalid materializer read request")
	}
	c.mu.Lock()
	staged := c.staged
	c.mu.Unlock()
	if staged == nil {
		return nil, errors.New("sourceauthority: materialized content is absent")
	}
	file, exists := staged.files[input.Index]
	if !exists {
		return nil, errors.New("sourceauthority: materializer stream index is invalid")
	}
	size := staged.sizes[input.Index]
	if input.Offset > size {
		return nil, errors.New("sourceauthority: materializer read escaped its staged content")
	}
	length := min(int64(input.Limit), size-input.Offset)
	payload := make([]byte, length)
	if length != 0 {
		if _, err := file.ReadAt(payload, input.Offset); err != nil {
			return nil, err
		}
	}
	return sourceTaskMaterializeReadResponse{
		Protocol: sourceTaskProtocol, Index: input.Index, Offset: input.Offset,
		Payload: payload, EOF: input.Offset+length == size,
	}, nil
}

func (c *sourceTaskChild) handleMaterializeDone(ctx context.Context, request daemonkit.Request) (any, error) {
	if err := c.claimed(sourceTaskOpMaterialize); err != nil {
		return nil, err
	}
	var input sourceTaskRequest
	if err := decodeSourceTaskBounded(request.Body, &input, sourceTaskJSONByteLimit); err != nil ||
		input.Protocol != sourceTaskProtocol {
		return nil, errors.New("sourceauthority: invalid materializer terminal request")
	}
	c.mu.Lock()
	if c.settled {
		c.mu.Unlock()
		return nil, errors.New("sourceauthority: materialization terminal was already settled")
	}
	c.settled = true
	terminal := c.terminal
	c.mu.Unlock()
	if terminal.Protocol != sourceTaskProtocol {
		return nil, errors.New("sourceauthority: materialization has no terminal")
	}
	if c.afterMaterialization != nil {
		if err := c.afterMaterialization(ctx); err != nil {
			terminal.Error = boundedSourceTaskError(errors.Join(errors.New(terminal.Error), err))
		}
	}
	if err := c.releaseStaged(); err != nil {
		terminal.Error = boundedSourceTaskError(errors.Join(errors.New(terminal.Error), err))
	}
	return terminal, nil
}

func (c *sourceTaskChild) releaseStaged() error {
	c.mu.Lock()
	staged, scan := c.staged, c.scan
	c.staged, c.scan = nil, nil
	c.mu.Unlock()
	return errors.Join(staged.close(), scan.close(), c.discardUploads())
}

func stageMaterializationContent(
	ctx context.Context,
	runtimeDir string,
	materialization Materialization,
) (_ *childMaterialization, resultErr error) {
	staged := &childMaterialization{
		files: make(map[uint32]*os.File), sizes: make(map[uint32]int64),
	}
	defer func() {
		if resultErr != nil {
			_ = staged.close()
		}
	}()
	var written int64
	buffer := make([]byte, sourceTaskChunkSize)
	for index, projection := range materialization.Objects {
		if projection.Content == nil {
			continue
		}
		reader, err := projection.Content.Open(ctx)
		if err != nil {
			return nil, err
		}
		file, err := createSourceTaskScratch(runtimeDir, "source-materialization-")
		if err != nil {
			_ = reader.Settle(err)
			_ = reader.Wait(context.WithoutCancel(ctx))
			return nil, err
		}
		budget := maxMaterializerOutput - written
		size, copyErr := io.CopyBuffer(writerOnly{file}, io.LimitReader(reader, budget+1), buffer)
		written += size
		if copyErr == nil && written > maxMaterializerOutput {
			copyErr = errors.New("sourceauthority: materializer output exceeds its bounded size")
		}
		settleErr := reader.Settle(copyErr)
		waitErr := reader.Wait(context.WithoutCancel(ctx))
		if err := errors.Join(copyErr, settleErr, waitErr); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return nil, err
		}
		staged.files[uint32(index)] = file
		staged.sizes[uint32(index)] = size
	}
	return staged, nil
}

// writerOnly hides ReadFrom so CopyBuffer honors the caller's bounded buffer.
type writerOnly struct{ io.Writer }

func (m *childMaterialization) close() error {
	if m == nil {
		return nil
	}
	var result error
	for _, file := range m.files {
		result = errors.Join(result, file.Close())
	}
	m.files = nil
	return result
}

func createSourceTaskScratch(runtimeDir, prefix string) (*os.File, error) {
	file, err := os.CreateTemp(runtimeDir, prefix)
	if err != nil {
		return nil, err
	}
	path := file.Name()
	if err := os.Chmod(path, 0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := os.Remove(path); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func encodeMaterializationMetadata(
	expected LogicalID,
	materialization Materialization,
) (sourceTaskMaterializationMetadata, error) {
	if materialization.Logical == "" || materialization.Logical != expected || len(materialization.Objects) == 0 {
		return sourceTaskMaterializationMetadata{},
			errors.New("sourceauthority: materializer returned invalid logical metadata")
	}
	metadata := sourceTaskMaterializationMetadata{
		Protocol: sourceTaskProtocol, Logical: materialization.Logical,
		Fingerprint: materialization.Fingerprint,
		Objects:     make([]sourceTaskProjection, len(materialization.Objects)),
	}
	for index, projection := range materialization.Objects {
		hasContent := projection.Content != nil
		switch projection.Kind {
		case catalog.KindFile:
			if !hasContent || projection.LinkTarget != "" {
				return sourceTaskMaterializationMetadata{},
					errors.New("sourceauthority: materializer returned invalid file content")
			}
		case catalog.KindDirectory:
			if hasContent || projection.LinkTarget != "" {
				return sourceTaskMaterializationMetadata{},
					errors.New("sourceauthority: materializer returned invalid directory content")
			}
		case catalog.KindSymlink:
			if hasContent || projection.LinkTarget == "" {
				return sourceTaskMaterializationMetadata{},
					errors.New("sourceauthority: materializer returned invalid symlink content")
			}
		default:
			return sourceTaskMaterializationMetadata{},
				errors.New("sourceauthority: materializer returned invalid object kind")
		}
		metadata.Objects[index] = sourceTaskProjection{
			Tenant: string(projection.Tenant), Generation: uint64(projection.Generation),
			Parent: projection.Parent, Name: projection.Name, Kind: uint8(projection.Kind),
			Mode: projection.Mode, LinkTarget: projection.LinkTarget,
			MountVisible:        projection.Visibility.Mount,
			FileProviderVisible: projection.Visibility.FileProvider,
			HasContent:          hasContent,
		}
	}
	payload, err := json.Marshal(metadata)
	if err != nil {
		return sourceTaskMaterializationMetadata{}, err
	}
	if err := validateSourceTaskStrings(reflect.ValueOf(metadata)); err != nil {
		return sourceTaskMaterializationMetadata{}, err
	}
	if len(payload)+1 > sourceTaskPageByteLimit || len(metadata.Objects) > maxScanPageSize {
		return sourceTaskMaterializationMetadata{},
			errors.New("sourceauthority: materializer projection metadata exceeds the protocol limit")
	}
	return metadata, nil
}

func validateChildMaterializationTask(task MaterializationTask) error {
	if task.Fence.Authority == "" || task.Fence.AuthorityGeneration == 0 ||
		task.Request.Logical == "" || len(task.Request.Inputs) == 0 ||
		len(task.Expected) != len(task.Request.Inputs) || len(task.Roots) == 0 || len(task.Tenants) == 0 {
		return errors.New("sourceauthority: incomplete materialization task")
	}
	if err := validateTaskRootFence(task.Fence, task.Roots); err != nil {
		return err
	}
	roots := make(map[RootID]RootSpec, len(task.Roots))
	for _, root := range task.Roots {
		if root.Authority != task.Fence.Authority {
			return errors.New("sourceauthority: materialization root escaped its authority")
		}
		roots[root.ID] = root
	}
	for index, input := range task.Request.Inputs {
		root, found := roots[input.Root]
		if !found || validateTaskRelative(root, input.Relative) != nil ||
			task.Expected[index].Root != input.Root || task.Expected[index].Relative != input.Relative ||
			!task.Expected[index].Exists {
			return errors.New("sourceauthority: materialization input escaped its physical fence")
		}
	}
	return nil
}
