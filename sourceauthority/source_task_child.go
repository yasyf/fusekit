package sourceauthority

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/yasyf/daemonkit/wire"
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
	used                 bool
	cancel               context.CancelFunc
}

type fullPathScanner interface {
	scanAll(context.Context, []RootSpec, func(PhysicalEntry) error) error
}

// RunSourceTaskChild recognizes and serves one exact, one-request source child invocation.
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
	listener, err := net.Listen("unix", config.Socket)
	if err != nil {
		return true, fmt.Errorf("sourceauthority: listen for source task parent: %w", err)
	}
	defer func() { _ = listener.Close() }()
	if err := os.Chmod(config.Socket, 0o600); err != nil {
		return true, fmt.Errorf("sourceauthority: secure source task listener: %w", err)
	}
	return true, serveSourceTaskChild(ctx, listener, newSecurePathSource(), materializers, os.Getppid(), config.JournalRoot)
}

func serveSourceTaskChild(
	ctx context.Context,
	listener net.Listener,
	pathSource PathSource,
	materializers SourceTaskMaterializers,
	parentPID int,
	journalRoot string,
) error {
	return serveSourceTaskChildWithHook(ctx, listener, pathSource, materializers, parentPID, journalRoot, nil)
}

func serveSourceTaskChildWithHook(
	ctx context.Context,
	listener net.Listener,
	pathSource PathSource,
	materializers SourceTaskMaterializers,
	parentPID int,
	journalRoot string,
	afterMutation func(context.Context, MutationReceipt) error,
) error {
	return serveSourceTaskChildWithHooks(ctx, listener, pathSource, materializers, parentPID, journalRoot, afterMutation, nil)
}

func serveSourceTaskChildWithHooks(
	ctx context.Context,
	listener net.Listener,
	pathSource PathSource,
	materializers SourceTaskMaterializers,
	parentPID int,
	journalRoot string,
	afterMutation func(context.Context, MutationReceipt) error,
	afterMaterialization func(context.Context) error,
) error {
	if pathSource == nil {
		return errors.New("sourceauthority: source task path source is required")
	}
	registered := make(SourceTaskMaterializers, len(materializers))
	for authority, materializer := range materializers {
		if authority == "" || materializer == nil {
			return errors.New("sourceauthority: invalid source task materializer registration")
		}
		registered[authority] = materializer
	}
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	child := &sourceTaskChild{
		pathSource: pathSource, materializers: registered,
		runtimeDir: filepath.Dir(listener.Addr().String()), journalRoot: journalRoot,
		afterMutation: afterMutation, afterMaterialization: afterMaterialization, cancel: cancel,
	}
	server := &wire.Server{
		WireBuild: sourceTaskBuild, Workers: 1, Backlog: 1, MaxSessions: 1,
		InboundQueue: 4, OutboundQueue: 4, StreamQueue: 2,
		Trust: observerParentTrust(parentPID),
	}
	server.RegisterConcurrent(sourceTaskOpRootIdentity, boundedSourceTaskHandler(child.handleRootIdentity))
	server.RegisterConcurrent(sourceTaskOpStat, boundedSourceTaskHandler(child.handleStat))
	server.RegisterConcurrent(sourceTaskOpScan, boundedSourceTaskHandler(child.handleScan))
	server.RegisterConcurrent(sourceTaskOpMaterialize, boundedSourceTaskHandler(child.handleMaterialize))
	server.RegisterConcurrent(sourceTaskOpMutation, boundedSourceTaskHandler(child.handleMutation))
	server.RegisterConcurrent(sourceTaskOpMutationGet, boundedSourceTaskHandler(child.handleMutationInspect))
	server.RegisterConcurrent(sourceTaskOpMutationAck, boundedSourceTaskHandler(child.handleMutationAcknowledge))
	server.RegisterConcurrent(sourceTaskOpMutationDrop, boundedSourceTaskHandler(child.handleMutationAbandon))
	server.RegisterConcurrent(sourceTaskOpMutationList, boundedSourceTaskHandler(child.handleMutationProofs))
	server.RegisterConcurrent(sourceTaskOpMutationGC, boundedSourceTaskHandler(child.handleMutationForget))
	admit := func() (func(), error) { return func() {}, nil }
	ready := func() error { return nil }
	return server.Serve(serveCtx, listener, ready, admit, admit)
}

func boundedSourceTaskHandler(
	handler func(context.Context, wire.Request) (any, error),
) func(context.Context, wire.Request) (any, error) {
	return func(ctx context.Context, request wire.Request) (any, error) {
		value, err := handler(ctx, request)
		if err != nil {
			return nil, errors.New(boundedSourceTaskError(err))
		}
		return value, nil
	}
}

func (c *sourceTaskChild) claim(request wire.Request) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.used || request.ID == 0 || request.Session == nil {
		return errors.New("sourceauthority: source task accepts exactly one request")
	}
	c.used = true
	go func(session *wire.AcceptedSession) {
		<-session.Done()
		c.cancel()
	}(request.Session)
	return nil
}

func (c *sourceTaskChild) handleRootIdentity(ctx context.Context, request wire.Request) (any, error) {
	if err := c.claim(request); err != nil {
		return nil, err
	}
	var input sourceTaskRootRequest
	if err := decodeSourceTaskBounded(request.Payload, &input, sourceTaskJSONByteLimit); err != nil || input.Protocol != sourceTaskProtocol {
		return nil, errors.New("sourceauthority: invalid root identity request")
	}
	identity, err := c.pathSource.RootIdentity(ctx, input.Root)
	if err != nil {
		return nil, err
	}
	response := sourceTaskIdentityResponse{Protocol: sourceTaskProtocol, Identity: identity}
	if _, err := encodeSourceTaskRequest(response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *sourceTaskChild) handleStat(ctx context.Context, request wire.Request) (any, error) {
	if err := c.claim(request); err != nil {
		return nil, err
	}
	var input sourceTaskStatRequest
	if err := decodeSourceTaskBounded(request.Payload, &input, sourceTaskJSONByteLimit); err != nil || input.Protocol != sourceTaskProtocol {
		return nil, errors.New("sourceauthority: invalid source stat request")
	}
	entry, err := c.pathSource.Stat(ctx, input.Root, input.Relative)
	if err != nil {
		return nil, err
	}
	response := sourceTaskStatResponse{Protocol: sourceTaskProtocol, Entry: entry}
	if _, err := encodeSourceTaskRequest(response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *sourceTaskChild) handleScan(ctx context.Context, request wire.Request) (any, error) {
	if err := c.claim(request); err != nil {
		return nil, err
	}
	var input sourceTaskScanRequest
	if err := decodeSourceTaskBounded(request.Payload, &input, sourceTaskJSONByteLimit); err != nil || input.Protocol != sourceTaskProtocol ||
		input.Limit <= 0 || input.Limit > maxScanPageSize ||
		input.Config.Roots == 0 || input.Config.Roots > sourceTaskRootLimit ||
		input.Config.Checkpoints != 0 || input.Config.Tenants != 0 ||
		input.Config.Inputs != 0 || input.Config.ExpectedEntries != 0 ||
		input.Config.Actions != 0 || input.Config.ExpectedEffects != 0 {
		return nil, errors.New("sourceauthority: invalid source scan request")
	}
	roots := make([]RootSpec, 0, input.Config.Roots)
	if err := receiveSourceTaskPages(ctx, request.Chunks, input.Config, func(page sourceTaskConfigPageBody) error {
		if len(page.Roots) == 0 {
			return errors.New("sourceauthority: source scan received a non-root configuration page")
		}
		roots = append(roots, page.Roots...)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := finishSourceTaskInput(request.Chunks); err != nil {
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
	terminal := &sourceTaskScanResponse{Protocol: sourceTaskProtocol, Count: count}
	chunks := make(chan []byte)
	go streamChildScanStage(ctx, stage, terminal, chunks)
	return wire.StreamResponse{Chunks: chunks, Value: terminal}, nil
}

type childScanStage struct {
	db   *sql.DB
	path string
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

func streamChildScanStage(
	ctx context.Context,
	stage childScanStage,
	terminal *sourceTaskScanResponse,
	chunks chan<- []byte,
) {
	defer close(chunks)
	defer func() {
		if cleanupErr := errors.Join(stage.db.Close(), os.Remove(stage.path)); cleanupErr != nil {
			if terminal.Error == "" {
				terminal.Error = boundedSourceTaskError(cleanupErr)
			} else {
				terminal.Error = boundedSourceTaskError(errors.Join(errors.New(terminal.Error), cleanupErr))
			}
		}
	}()
	rows, err := stage.db.QueryContext(ctx, `
SELECT payload FROM entries ORDER BY root COLLATE BINARY, relative COLLATE BINARY`)
	if err != nil {
		terminal.Error = boundedSourceTaskError(err)
		return
	}
	defer func() { _ = rows.Close() }()
	var streamed uint64
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			terminal.Error = boundedSourceTaskError(err)
			return
		}
		if !sendSourceTaskChunk(ctx, chunks, payload) {
			return
		}
		streamed++
	}
	if err := rows.Err(); err != nil {
		terminal.Error = boundedSourceTaskError(err)
		return
	}
	if streamed != terminal.Count {
		terminal.Error = "sourceauthority: child scan stage count changed"
	}
}

func (c *sourceTaskChild) handleMaterialize(ctx context.Context, request wire.Request) (any, error) {
	if err := c.claim(request); err != nil {
		return nil, err
	}
	var input sourceTaskMaterializeRequest
	if err := decodeSourceTaskBounded(request.Payload, &input, sourceTaskJSONByteLimit); err != nil || input.Protocol != sourceTaskProtocol ||
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
	if err := receiveSourceTaskPages(ctx, request.Chunks, input.Config, func(page sourceTaskConfigPageBody) error {
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
	payload := make([]byte, 0, input.PayloadSize)
	for chunk := range request.Chunks {
		if chunk.End {
			continue
		}
		if len(chunk.Payload) < 6 || len(chunk.Payload) > sourceTaskChunkSize+5 ||
			chunk.Payload[0] != sourceTaskChunkRequest || binary.BigEndian.Uint32(chunk.Payload[1:5]) != 0 ||
			len(payload)+len(chunk.Payload[5:]) > input.PayloadSize {
			return nil, errors.New("sourceauthority: materializer payload exceeded its declared size")
		}
		payload = append(payload, chunk.Payload[5:]...)
	}
	if len(payload) != input.PayloadSize {
		return nil, errors.New("sourceauthority: materializer payload did not match its declared size")
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
	terminal := &sourceTaskMaterializeResponse{
		Protocol: sourceTaskProtocol, Logical: materialization.Logical,
		Fingerprint: materialization.Fingerprint, Objects: len(materialization.Objects),
	}
	chunks := make(chan []byte)
	go streamMaterialization(ctx, materialization, metadata, terminal, chunks, inputs.Close, c.afterMaterialization)
	return wire.StreamResponse{Chunks: chunks, Value: terminal}, nil
}

func encodeMaterializationMetadata(expected LogicalID, materialization Materialization) ([]byte, error) {
	if materialization.Logical == "" || materialization.Logical != expected || len(materialization.Objects) == 0 {
		return nil, errors.New("sourceauthority: materializer returned invalid logical metadata")
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
				return nil, errors.New("sourceauthority: materializer returned invalid file content")
			}
		case catalog.KindDirectory:
			if hasContent || projection.LinkTarget != "" {
				return nil, errors.New("sourceauthority: materializer returned invalid directory content")
			}
		case catalog.KindSymlink:
			if hasContent || projection.LinkTarget == "" {
				return nil, errors.New("sourceauthority: materializer returned invalid symlink content")
			}
		default:
			return nil, errors.New("sourceauthority: materializer returned invalid object kind")
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
		return nil, err
	}
	if err := validateSourceTaskStrings(reflect.ValueOf(metadata)); err != nil {
		return nil, err
	}
	if len(payload)+1 > sourceTaskPageByteLimit || len(metadata.Objects) > maxScanPageSize {
		return nil, errors.New("sourceauthority: materializer projection metadata exceeds the protocol limit")
	}
	return append([]byte{sourceTaskChunkMetadata}, payload...), nil
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

func streamMaterialization(
	ctx context.Context,
	materialization Materialization,
	metadata []byte,
	terminal *sourceTaskMaterializeResponse,
	chunks chan<- []byte,
	cleanup func() error,
	afterMaterialization func(context.Context) error,
) {
	defer close(chunks)
	defer func() {
		if err := errors.Join(closeMaterializations([]Materialization{materialization}), cleanup()); err != nil {
			terminal.Error = boundedSourceTaskError(errors.Join(errors.New(terminal.Error), err))
		}
	}()
	if !sendSourceTaskChunk(ctx, chunks, metadata) {
		return
	}
	buffer := make([]byte, sourceTaskChunkSize)
	for index, projection := range materialization.Objects {
		if projection.Content == nil {
			continue
		}
		reader, err := projection.Content.Open(ctx)
		if err != nil {
			terminal.Error = boundedSourceTaskError(err)
			return
		}
		for {
			count, readErr := reader.Read(buffer)
			if count != 0 && !sendSourceTaskChunk(ctx, chunks, encodeStreamChunk(sourceTaskChunkContent, uint32(index), buffer[:count])) {
				cause := ctx.Err()
				if cause == nil {
					cause = ErrClosed
				}
				_ = reader.Settle(cause)
				_ = reader.Wait(context.WithoutCancel(ctx))
				return
			}
			if readErr != nil {
				var cause error
				if !errors.Is(readErr, io.EOF) {
					cause = readErr
				}
				settleErr := reader.Settle(cause)
				waitErr := reader.Wait(context.WithoutCancel(ctx))
				if cause != nil || settleErr != nil || waitErr != nil {
					terminal.Error = boundedSourceTaskError(errors.Join(cause, settleErr, waitErr))
					return
				}
				break
			}
		}
		if !sendSourceTaskChunk(ctx, chunks, encodeStreamChunk(sourceTaskChunkEnd, uint32(index), nil)) {
			return
		}
	}
	if afterMaterialization != nil {
		if err := afterMaterialization(ctx); err != nil {
			terminal.Error = boundedSourceTaskError(err)
		}
	}
}

func sendSourceTaskChunk(ctx context.Context, chunks chan<- []byte, payload []byte) bool {
	if len(payload) == 0 || len(payload) > sourceTaskPageByteLimit+5 {
		return false
	}
	select {
	case chunks <- append([]byte(nil), payload...):
		return true
	case <-ctx.Done():
		return false
	}
}
