package catalogworker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

const defaultStopTimeout = 5 * time.Second
const childDiagnosticLimit = 64 << 10

// ManagerConfig defines one sealed catalog storage process family.
type ManagerConfig struct {
	Processes         *proc.Manager
	ExpectedSignature proc.SignatureDigest
	Executable        string
	Database          string
	Stderr            io.Writer

	ReadinessTimeout time.Duration
	OperationTimeout time.Duration
	StopTimeout      time.Duration

	launcher processLauncher
}

// Manager owns the exact current catalog process generation. Calls remain
// concurrent; only generation creation and poisoning are serialized.
type Manager struct {
	config      ManagerConfig
	launcher    processLauncher
	lifecycle   context.Context
	runtime     string
	environment []string

	mu       sync.Mutex
	next     uint64
	current  *workerGeneration
	retiring *workerGeneration
	starting *workerStart
	closed   bool

	preparerMu sync.Mutex
	preparer   PrepareTenantFunc

	nativeOwnerMu sync.Mutex
	nativeOwners  map[string]*nativeOwnerLane

	nativeTokenMu sync.Mutex
	nativeTokens  map[string]*nativeTokenLane

	closeOnce   sync.Once
	closeDone   chan struct{}
	closeResult error
}

var _ tenant.Store = (*Manager)(nil)

// RecoverReapedSourceAuthorityRuntimes assembles and verifies the complete
// bounded close-all result across exact durable worker pages.
func (m *Manager) RecoverReapedSourceAuthorityRuntimes(
	ctx context.Context,
	receipt proc.ReapReceipt,
) (catalog.SourceAuthorityRuntimeRecoveryResult, error) {
	summary, err := m.BeginRecoverReapedSourceAuthorityRuntimes(ctx, receipt)
	if err != nil {
		return catalog.SourceAuthorityRuntimeRecoveryResult{}, err
	}
	result := catalog.SourceAuthorityRuntimeRecoveryResult{
		Summary: summary,
		Closed:  make([]catalog.SourceAuthorityRuntimeFence, 0, int(summary.ClosedCount)),
	}
	for after := uint64(0); summary.ClosedCount > 0; {
		page, err := m.SourceAuthorityRuntimeRecoveryPage(
			ctx,
			catalog.SourceAuthorityRuntimeRecoveryPageRequest{
				Receipt: receipt,
				After:   after,
				Limit:   catalog.SourceAuthorityRuntimeRecoveryPageLimit,
			},
		)
		if err != nil {
			return catalog.SourceAuthorityRuntimeRecoveryResult{}, err
		}
		if page.Summary != summary {
			return catalog.SourceAuthorityRuntimeRecoveryResult{}, fmt.Errorf(
				"%w: source authority runtime recovery summary changed between pages",
				catalog.ErrIntegrity,
			)
		}
		result.Closed = append(result.Closed, page.Closed...)
		if page.Next == 0 {
			break
		}
		after = page.Next
	}
	if err := result.Validate(receipt); err != nil {
		return catalog.SourceAuthorityRuntimeRecoveryResult{}, err
	}
	return result, nil
}

type workerStart struct {
	cancel         context.CancelFunc
	done           chan struct{}
	err            error
	closeRequested bool
}

type workerGeneration struct {
	number      uint64
	process     managedProcess
	client      *Client
	closeClient func() error
	abortClient func(error) error
	diagnostics *childDiagnostics

	settled  chan struct{}
	stopErr  error
	poisoned bool
}

type childDiagnostics struct {
	mu   sync.Mutex
	data []byte
}

func (d *childDiagnostics) Write(value []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(value) >= childDiagnosticLimit {
		d.data = append(d.data[:0], value[len(value)-childDiagnosticLimit:]...)
		return len(value), nil
	}
	overflow := len(d.data) + len(value) - childDiagnosticLimit
	if overflow > 0 {
		copy(d.data, d.data[overflow:])
		d.data = d.data[:len(d.data)-overflow]
	}
	d.data = append(d.data, value...)
	return len(value), nil
}

func (d *childDiagnostics) err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	value := strings.TrimSpace(string(d.data))
	if value == "" {
		return nil
	}
	return fmt.Errorf("catalog worker child stderr: %s", value)
}

type managedProcess interface {
	Identity() proc.Identity
	Stop(context.Context) error
}

type sessionProcessSpec struct {
	Spawn       proc.SpawnConfig
	Client      wire.SpawnedClientConfig
	Stderr      io.Writer
	StopTimeout time.Duration
}

type processLauncher interface {
	StartSession(context.Context, sessionProcessSpec) (managedProcess, sessionClient, error)
}

type spawnedSessionLauncher struct{ manager *proc.Manager }

type spawnedSessionProcess struct {
	child      *proc.PreparedChild
	identity   proc.Identity
	stderr     *os.File
	stderrDone <-chan error

	stopOnce sync.Once
	stopped  chan struct{}
	stopErr  error
}

func (l spawnedSessionLauncher) StartSession(
	ctx context.Context,
	spec sessionProcessSpec,
) (managedProcess, sessionClient, error) {
	if l.manager == nil {
		return nil, nil, errors.New("catalog worker: process manager is required")
	}
	request, err := proc.NewSpawnRequest(spec.Spawn)
	if err != nil {
		return nil, nil, fmt.Errorf("catalog worker: construct spawn request: %w", err)
	}
	child, receipt, err := l.manager.Prepare(ctx, request)
	if err != nil {
		return nil, nil, fmt.Errorf("catalog worker: prepare spawned child: %w", err)
	}
	stderr, err := child.TakeStderr()
	if err != nil {
		return nil, nil, errors.Join(err, stopPreparedChild(ctx, child, spec.StopTimeout))
	}
	diagnosticsDone := make(chan error, 1)
	writer := spec.Stderr
	if writer == nil {
		writer = io.Discard
	}
	go func() {
		_, copyErr := io.Copy(writer, stderr)
		diagnosticsDone <- copyErr
	}()
	process := &spawnedSessionProcess{
		child: child, identity: receipt.ProcessIdentity(), stderr: stderr,
		stderrDone: diagnosticsDone, stopped: make(chan struct{}),
	}
	if err := child.Start(ctx); err != nil {
		return nil, nil, errors.Join(err, stopManagedProcess(ctx, process, spec.StopTimeout))
	}
	endpoint, err := child.ClaimSpawnedSession(ctx, receipt)
	if err != nil {
		return nil, nil, errors.Join(err, stopManagedProcess(ctx, process, spec.StopTimeout))
	}
	spec.Client.Endpoint = endpoint
	client, err := wire.NewSpawnedClient(ctx, spec.Client)
	if err != nil {
		return nil, nil, errors.Join(err, stopManagedProcess(ctx, process, spec.StopTimeout))
	}
	return process, spawnedSessionClient{client}, nil
}

func (p *spawnedSessionProcess) Identity() proc.Identity { return p.identity }

func (p *spawnedSessionProcess) Stop(ctx context.Context) error {
	p.stopOnce.Do(func() {
		p.stopErr = p.child.Stop(ctx)
		select {
		case <-p.child.Done():
		case <-ctx.Done():
			p.stopErr = errors.Join(p.stopErr, p.stderr.Close())
		}
		copyErr := <-p.stderrDone
		if errors.Is(copyErr, os.ErrClosed) {
			copyErr = nil
		}
		closeErr := p.stderr.Close()
		if errors.Is(closeErr, os.ErrClosed) {
			closeErr = nil
		}
		p.stopErr = errors.Join(p.stopErr, copyErr, closeErr)
		close(p.stopped)
	})
	<-p.stopped
	return p.stopErr
}

func stopPreparedChild(parent context.Context, child *proc.PreparedChild, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultStopTimeout
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()
	return child.Stop(ctx)
}

func stopManagedProcess(parent context.Context, process managedProcess, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultStopTimeout
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()
	return process.Stop(ctx)
}

// NewManager creates a lazy catalog worker manager without opening SQLite.
func NewManager(lifecycle context.Context, config ManagerConfig) (*Manager, error) {
	if lifecycle == nil {
		return nil, errors.New("catalog worker: lifecycle context is required")
	}
	if config.Executable == "" || !filepath.IsAbs(config.Executable) || !filepath.IsAbs(config.Database) {
		return nil, errors.New("catalog worker: absolute executable and database are required")
	}
	if config.ReadinessTimeout <= 0 || config.OperationTimeout <= 0 {
		return nil, errors.New("catalog worker: positive readiness and hard operation timeouts are required")
	}
	if config.ExpectedSignature == (proc.SignatureDigest{}) {
		return nil, errors.New("catalog worker: expected child signature is required")
	}
	launcher := config.launcher
	if launcher == nil {
		if config.Processes == nil {
			return nil, errors.New("catalog worker: process manager is required")
		}
		launcher = spawnedSessionLauncher{manager: config.Processes}
	}
	runtime, err := newNativeWriteToken()
	if err != nil {
		return nil, fmt.Errorf("catalog worker: create manager runtime identity: %w", err)
	}
	return &Manager{
		config: config, launcher: launcher, lifecycle: lifecycle, runtime: runtime,
		environment: catalogWorkerEnvironment(os.Environ()),
		closeDone:   make(chan struct{}),
	}, nil
}

// Close poisons and settles the current generation and rejects future calls.
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.closeResult = m.close()
		close(m.closeDone)
	})
	<-m.closeDone
	return m.closeResult
}

func (m *Manager) close() error {
	var result error
	for {
		m.mu.Lock()
		m.closed = true
		if starting := m.starting; starting != nil {
			starting.closeRequested = true
			starting.cancel()
			done := starting.done
			m.mu.Unlock()
			<-done
			startErr := starting.err
			if starting.closeRequested && cancellationOnly(startErr) {
				startErr = nil
			}
			result = errors.Join(result, startErr)
			continue
		}
		generation := m.retiring
		owner := false
		if generation == nil && m.current != nil {
			generation = m.current
			m.current = nil
			generation.poisoned = true
			m.retiring = generation
			owner = true
		}
		m.mu.Unlock()
		if generation == nil {
			return result
		}
		if !owner {
			<-generation.settled
			return errors.Join(result, generation.stopErr)
		}
		return errors.Join(result, m.settle(generation))
	}
}

func cancellationOnly(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		found := false
		for _, part := range joined.Unwrap() {
			if part == nil {
				continue
			}
			found = true
			if !cancellationOnly(part) {
				return false
			}
		}
		return found
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return cancellationOnly(wrapped.Unwrap())
	}
	return err == context.Canceled
}

// Head returns one tenant's current catalog revision.
func (m *Manager) Head(ctx context.Context, tenant catalog.TenantID) (catalog.Revision, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.Revision, error) { return client.Head(ctx, tenant) })
}

func (m *Manager) CompactionFloor(ctx context.Context, tenant catalog.TenantID) (catalog.Revision, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.Revision, error) { return client.CompactionFloor(ctx, tenant) })
}

func (m *Manager) Tenant(ctx context.Context, tenant catalog.TenantID) (catalog.TenantMetadata, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.TenantMetadata, error) { return client.Tenant(ctx, tenant) })
}

func (m *Manager) Root(ctx context.Context, tenant catalog.TenantID) (catalog.Object, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.Object, error) { return client.Root(ctx, tenant) })
}

func (m *Manager) Lookup(ctx context.Context, tenant catalog.TenantID, presentation catalog.Presentation, id catalog.ObjectID) (catalog.Object, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.Object, error) { return client.Lookup(ctx, tenant, presentation, id) })
}

func (m *Manager) LookupName(ctx context.Context, tenant catalog.TenantID, presentation catalog.Presentation, parent catalog.ObjectID, name string) (catalog.Object, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.Object, error) {
		return client.LookupName(ctx, tenant, presentation, parent, name)
	})
}

func (m *Manager) Snapshot(ctx context.Context, tenant catalog.TenantID, scope catalog.EnumerationScope, revision catalog.Revision, cursor catalog.SnapshotCursor, limit int) (catalog.SnapshotPage, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.SnapshotPage, error) {
		return client.Snapshot(ctx, tenant, scope, revision, cursor, limit)
	})
}

func (m *Manager) ChangesSince(ctx context.Context, tenant catalog.TenantID, scope catalog.EnumerationScope, cursor catalog.ChangeCursor, limit int) (catalog.ChangePage, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.ChangePage, error) {
		return client.ChangesSince(ctx, tenant, scope, cursor, limit)
	})
}

func (m *Manager) HasMaterializationDemand(ctx context.Context, tenant catalog.TenantID) (bool, error) {
	return managerCall(m, ctx, func(client *Client) (bool, error) { return client.HasMaterializationDemand(ctx, tenant) })
}

func (m *Manager) ClaimMutation(ctx context.Context, id catalog.MutationID, owner catalog.MutationOwnerID) (catalog.PreparedMutation, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.PreparedMutation, error) { return client.ClaimMutation(ctx, id, owner) })
}

func (m *Manager) PrepareMutationSource(ctx context.Context, id catalog.MutationID, claim catalog.MutationClaim) (catalog.PreparedMutation, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.PreparedMutation, error) {
		return client.PrepareMutationSource(ctx, id, claim)
	})
}

func (m *Manager) SetMutationSourceResult(ctx context.Context, id catalog.MutationID, claim catalog.MutationClaim, locator catalog.SourceLocator) (catalog.PreparedMutation, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.PreparedMutation, error) {
		return client.SetMutationSourceResult(ctx, id, claim, locator)
	})
}

func (m *Manager) MarkMutationApplied(ctx context.Context, id catalog.MutationID, claim catalog.MutationClaim) (catalog.PreparedMutation, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.PreparedMutation, error) {
		return client.MarkMutationApplied(ctx, id, claim)
	})
}

func (m *Manager) ReclaimMutation(ctx context.Context, id catalog.MutationID, stale catalog.MutationClaim, owner catalog.MutationOwnerID) (catalog.PreparedMutation, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.PreparedMutation, error) {
		return client.ReclaimMutation(ctx, id, stale, owner)
	})
}

func (m *Manager) CommitMutation(ctx context.Context, tenant catalog.TenantID, id catalog.MutationID) (catalog.NamespaceMutationResult, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.NamespaceMutationResult, error) {
		return client.CommitMutation(ctx, tenant, id)
	})
}

func (m *Manager) OpenMutationContent(ctx context.Context, tenant catalog.TenantID, id catalog.MutationID) (contentstream.Source, error) {
	prepared, err := m.PreparedMutation(ctx, tenant, id)
	if err != nil {
		return nil, err
	}
	ref, found := mutationIntentContentRef(prepared.Intent)
	if !found {
		return nil, fmt.Errorf("%w: prepared mutation has no file content", catalog.ErrInvalidObject)
	}
	reader, generation, err := managerGenerationCall(m, ctx, func(client *Client) (contentstream.Source, error) {
		return client.OpenMutationContent(ctx, tenant, id)
	})
	if err != nil {
		return nil, err
	}
	managed := newManagedReader(ctx, reader, nil, m, generation, ref.Size, ref.Hash)
	return newManagedMutationContent(managed, reader), nil
}

type managedContentReader struct {
	reader      io.Reader
	closeReader func() error
	manager     *Manager
	generation  *workerGeneration

	readMu     sync.Mutex
	poisonOnce sync.Once
	finishOnce sync.Once
	closeOnce  sync.Once
	closeErr   error
	clean      atomic.Bool
	digest     hash.Hash
	expected   catalog.ContentHash
	size       int64
	read       int64

	deadline context.Context
	cancel   context.CancelFunc
	done     chan struct{}

	stateMu       sync.Mutex
	finished      bool
	poisonStarted bool
	poisonErr     error
}

func (r *managedContentReader) Read(buffer []byte) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	type readResult struct {
		count int
		err   error
	}
	result := make(chan readResult, 1)
	go func() {
		count, err := r.reader.Read(buffer)
		result <- readResult{count: count, err: err}
	}()
	var value readResult
	select {
	case value = <-result:
	case <-r.deadline.Done():
		r.poison()
		value = <-result
		value.err = errors.Join(r.deadline.Err(), value.err, r.poisonError())
	}
	count, err := value.count, value.err
	if count > 0 {
		r.read += int64(count)
		_, _ = r.digest.Write(buffer[:count])
		if r.read > r.size {
			r.poison()
			return count, errors.Join(catalog.ErrIntegrity, errors.New("catalog worker: content stream exceeded declared size"), r.poisonError())
		}
	}
	if errors.Is(err, io.EOF) {
		var actual catalog.ContentHash
		copy(actual[:], r.digest.Sum(nil))
		if r.read != r.size || actual != r.expected {
			r.poison()
			return count, errors.Join(catalog.ErrIntegrity, errors.New("catalog worker: content stream did not match declared size and digest"), r.poisonError())
		}
		if deadlineErr := r.deadline.Err(); deadlineErr != nil {
			r.poison()
			return count, errors.Join(deadlineErr, r.poisonError())
		}
		if !r.finishClean() {
			r.poison()
			return count, errors.Join(r.deadline.Err(), r.poisonError())
		}
		r.clean.Store(true)
	} else if err != nil {
		r.poison()
		err = errors.Join(err, r.poisonError())
	}
	return count, err
}

func (r *managedContentReader) Close() error {
	r.closeOnce.Do(func() {
		if r.closeReader != nil {
			r.closeErr = r.closeReader()
		}
		r.readMu.Lock()
		if r.closeErr != nil || !r.clean.Load() {
			r.poison()
			r.closeErr = errors.Join(r.closeErr, r.poisonError())
		}
		r.readMu.Unlock()
		r.finish()
	})
	return r.closeErr
}

type managedMutationContent struct {
	reader *managedContentReader
	source contentstream.Source

	settleOnce sync.Once
	mu         sync.Mutex
	settleErr  error
	done       chan struct{}
}

func newManagedMutationContent(
	reader *managedContentReader,
	source contentstream.Source,
) *managedMutationContent {
	return &managedMutationContent{reader: reader, source: source, done: make(chan struct{})}
}

func (s *managedMutationContent) Read(buffer []byte) (int, error) {
	return s.reader.Read(buffer)
}

func (s *managedMutationContent) Settle(result error) error {
	s.settleOnce.Do(func() {
		settleErr := s.source.Settle(result)
		s.mu.Lock()
		s.settleErr = settleErr
		s.mu.Unlock()
		go s.cleanup(result)
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settleErr
}

func (s *managedMutationContent) Wait(ctx context.Context) error {
	select {
	case <-s.done:
	case <-ctx.Done():
		_ = s.Settle(ctx.Err())
		<-s.done
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settleErr
}

func (s *managedMutationContent) cleanup(result error) {
	timeout := s.reader.manager.config.StopTimeout
	if timeout <= 0 {
		timeout = defaultStopTimeout
	}
	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(s.reader.manager.lifecycle), timeout)
	poisoned := make(chan struct{})
	s.reader.readMu.Lock()
	if result != nil || !s.reader.clean.Load() {
		go func() {
			s.reader.poison()
			close(poisoned)
		}()
	} else {
		close(poisoned)
	}
	s.reader.readMu.Unlock()
	waitErr := s.source.Wait(waitCtx)
	cancel()
	if waitErr != nil {
		s.reader.poison()
	}
	<-poisoned
	s.reader.finish()
	s.mu.Lock()
	s.settleErr = errors.Join(s.settleErr, waitErr, s.reader.poisonError())
	s.mu.Unlock()
	close(s.done)
}

func (r *managedContentReader) poison() {
	r.stateMu.Lock()
	if r.finished {
		r.stateMu.Unlock()
		return
	}
	r.poisonStarted = true
	r.stateMu.Unlock()
	r.poisonOnce.Do(func() {
		err := r.manager.poison(r.generation)
		r.stateMu.Lock()
		r.poisonErr = err
		r.stateMu.Unlock()
		r.finish()
	})
}

func (r *managedContentReader) poisonError() error {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	return r.poisonErr
}

func (r *managedContentReader) finishClean() bool {
	r.stateMu.Lock()
	if r.poisonStarted {
		r.stateMu.Unlock()
		return false
	}
	r.finished = true
	r.stateMu.Unlock()
	r.finish()
	return true
}

func (r *managedContentReader) finish() {
	r.stateMu.Lock()
	if !r.finished {
		r.finished = true
	}
	r.stateMu.Unlock()
	r.finishOnce.Do(func() {
		close(r.done)
		r.cancel()
	})
}

func newManagedContentReader(
	ctx context.Context,
	reader io.ReadCloser,
	manager *Manager,
	generation *workerGeneration,
	size int64,
	expected catalog.ContentHash,
) *managedContentReader {
	return newManagedReader(ctx, reader, reader.Close, manager, generation, size, expected)
}

func newManagedReader(
	ctx context.Context,
	reader io.Reader,
	closeReader func() error,
	manager *Manager,
	generation *workerGeneration,
	size int64,
	expected catalog.ContentHash,
) *managedContentReader {
	deadline, cancel := context.WithTimeout(ctx, manager.config.OperationTimeout)
	result := &managedContentReader{
		reader: reader, closeReader: closeReader, manager: manager, generation: generation,
		digest: sha256.New(), expected: expected, size: size,
		deadline: deadline, cancel: cancel, done: make(chan struct{}),
	}
	go func() {
		select {
		case <-deadline.Done():
			result.poison()
		case <-result.done:
		}
	}()
	return result
}

func mutationIntentContentRef(intent catalog.MutationIntent) (catalog.ContentRef, bool) {
	switch {
	case intent.Create != nil && intent.Create.Spec.Kind == catalog.KindFile:
		return intent.Create.Spec.Content, true
	case intent.Revise != nil && intent.Revise.Spec.Content != nil:
		return intent.Revise.Spec.Content.Ref, true
	case intent.Replace != nil && intent.Replace.Content != nil:
		return intent.Replace.Content.Ref, true
	default:
		return catalog.ContentRef{}, false
	}
}

// LoadTenantState returns one CAS-protected tenant state record.
func (m *Manager) LoadTenantState(ctx context.Context, tenant catalog.TenantID) (catalog.TenantStateRecord, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.TenantStateRecord, error) { return client.LoadTenantState(ctx, tenant) })
}

// ProvisionTenant atomically creates or exactly replays one desired definition.
func (m *Manager) ProvisionTenant(ctx context.Context, provision catalog.TenantProvision) (catalog.TenantProvision, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.TenantProvision, error) { return client.ProvisionTenant(ctx, provision) })
}

// ReplaceTenantProvision atomically advances or exactly replays one generation.
func (m *Manager) ReplaceTenantProvision(ctx context.Context, expected catalog.Generation, next catalog.TenantProvision) (catalog.TenantProvision, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.TenantProvision, error) {
		return client.ReplaceTenantProvision(ctx, expected, next)
	})
}

// RemoveTenantProvision removes one exact generation with same-transaction replay.
func (m *Manager) RemoveTenantProvision(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation) error {
	_, err := managerCall(m, ctx, func(client *Client) (struct{}, error) {
		return struct{}{}, client.RemoveTenantProvision(ctx, tenant, generation)
	})
	return err
}

// SaveTenantState persists one CAS transition with same-transaction replay.
func (m *Manager) SaveTenantState(ctx context.Context, expected catalog.StateVersion, state catalog.TenantStateRecord) (catalog.TenantStateRecord, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.TenantStateRecord, error) {
		return client.SaveTenantState(ctx, expected, state)
	})
}

func (m *Manager) FileProviderDomainForTenant(
	ctx context.Context,
	tenant catalog.TenantID,
) (catalog.FileProviderDomain, bool, error) {
	type result struct {
		domain catalog.FileProviderDomain
		found  bool
	}
	value, err := managerCall(m, ctx, func(client *Client) (result, error) {
		domain, found, callErr := client.FileProviderDomainForTenant(ctx, tenant)
		return result{domain: domain, found: found}, callErr
	})
	return value.domain, value.found, err
}

// FileProviderDemand returns exact live lease and materialization-interest counts.
func (m *Manager) FileProviderDemand(
	ctx context.Context,
	tenant catalog.TenantID,
	domain causal.DomainID,
	generation catalog.Generation,
	now time.Time,
) (uint32, uint32, error) {
	type result struct {
		leases    uint32
		interests uint32
	}
	value, err := managerCall(m, ctx, func(client *Client) (result, error) {
		leases, interests, callErr := client.FileProviderDemand(ctx, tenant, domain, generation, now)
		return result{leases: leases, interests: interests}, callErr
	})
	return value.leases, value.interests, err
}

func (m *Manager) BeginFileProviderDomainRemoval(
	ctx context.Context,
	owner string,
	tenant catalog.TenantID,
	generation catalog.Generation,
) (catalog.FileProviderDomainRemoval, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.FileProviderDomainRemoval, error) {
		return client.BeginFileProviderDomainRemoval(ctx, owner, tenant, generation)
	})
}

func (m *Manager) FileProviderDomainRemovalState(
	ctx context.Context,
	owner string,
	tenant catalog.TenantID,
	generation catalog.Generation,
) (catalog.FileProviderDomainRemoval, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.FileProviderDomainRemoval, error) {
		return client.FileProviderDomainRemovalState(ctx, owner, tenant, generation)
	})
}

func (m *Manager) ConfirmFileProviderDomainRemoval(ctx context.Context, removal catalog.FileProviderDomainRemoval) error {
	_, err := managerCall(m, ctx, func(client *Client) (struct{}, error) {
		return struct{}{}, client.ConfirmFileProviderDomainRemoval(ctx, removal)
	})
	return err
}

func (m *Manager) ConfirmFileProviderDomain(ctx context.Context, domain catalog.FileProviderDomain) error {
	_, err := managerCall(m, ctx, func(client *Client) (struct{}, error) {
		return struct{}{}, client.ConfirmFileProviderDomain(ctx, domain)
	})
	return err
}

func (m *Manager) ConfirmFileProviderDomainAbsent(ctx context.Context, domain causal.DomainID) error {
	_, err := managerCall(m, ctx, func(client *Client) (struct{}, error) {
		return struct{}{}, client.ConfirmFileProviderDomainAbsent(ctx, domain)
	})
	return err
}

func (m *Manager) FileProviderSignalPlan(
	ctx context.Context,
	tenant catalog.TenantID,
	domain causal.DomainID,
	generation catalog.Generation,
	revision catalog.Revision,
) (catalog.FileProviderSignalPlan, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.FileProviderSignalPlan, error) {
		return client.FileProviderSignalPlan(ctx, tenant, domain, generation, revision)
	})
}

func (m *Manager) NextBrokerCommandID(ctx context.Context) (uint64, error) {
	return managerCall(m, ctx, func(client *Client) (uint64, error) {
		return client.NextBrokerCommandID(ctx)
	})
}

func (m *Manager) BeginBrokerCommandAttempt(ctx context.Context, attempt catalog.BrokerCommandAttempt) (catalog.BrokerCommandAttempt, bool, error) {
	type attemptResult struct {
		value   catalog.BrokerCommandAttempt
		created bool
	}
	result, err := managerCall(m, ctx, func(client *Client) (attemptResult, error) {
		value, created, callErr := client.BeginBrokerCommandAttempt(ctx, attempt)
		return attemptResult{value: value, created: created}, callErr
	})
	return result.value, result.created, err
}

func (m *Manager) TransitionBrokerCommandAttempt(
	ctx context.Context,
	attempt catalog.BrokerCommandAttempt,
	next catalog.BrokerCommandAttemptState,
) (catalog.BrokerCommandAttempt, error) {
	return managerCall(m, ctx, func(client *Client) (catalog.BrokerCommandAttempt, error) {
		return client.TransitionBrokerCommandAttempt(ctx, attempt, next)
	})
}

func (m *Manager) AbandonBrokerCommandAttempt(ctx context.Context, attempt catalog.BrokerCommandAttempt) error {
	_, err := managerCall(m, ctx, func(client *Client) (struct{}, error) {
		return struct{}{}, client.AbandonBrokerCommandAttempt(ctx, attempt)
	})
	return err
}

func (m *Manager) RecoverBrokerCommandAttempts(ctx context.Context) error {
	_, err := managerCall(m, ctx, func(client *Client) (struct{}, error) {
		return struct{}{}, client.RecoverBrokerCommandAttempts(ctx)
	})
	return err
}

func managerCall[T any](m *Manager, ctx context.Context, call func(*Client) (T, error)) (T, error) {
	value, _, err := managerGenerationCall(m, ctx, call)
	return value, err
}

func managerGenerationCall[T any](
	m *Manager,
	ctx context.Context,
	call func(*Client) (T, error),
) (T, *workerGeneration, error) {
	var zero T
	readinessCtx, cancelReadiness := context.WithTimeout(ctx, m.config.ReadinessTimeout)
	generation, err := m.acquire(readinessCtx)
	cancelReadiness()
	if err != nil {
		return zero, nil, err
	}
	operationCtx, cancel := context.WithTimeout(ctx, m.config.OperationTimeout)
	defer cancel()
	type callResult struct {
		value T
		err   error
	}
	result := make(chan callResult, 1)
	go func() {
		value, callErr := call(generation.client)
		result <- callResult{value: value, err: callErr}
	}()
	var settled callResult
	select {
	case settled = <-result:
	case <-operationCtx.Done():
		stopErr := m.poison(generation)
		settled = <-result
		return zero, generation, errors.Join(
			operationCtx.Err(), settled.err, stopErr, generation.diagnostics.err(),
		)
	}
	value, err := settled.value, settled.err
	if err == nil {
		return value, generation, nil
	}
	var transport *TransportError
	if operationCtx.Err() == nil && !errors.As(err, &transport) {
		return zero, generation, err
	}
	stopErr := m.poison(generation)
	return zero, generation, errors.Join(err, stopErr, generation.diagnostics.err())
}

// managerWaitCall runs a context-aware long poll without imposing the ordinary
// operation deadline. Caller cancellation is normal settlement, not evidence
// that the worker generation is wedged.
func managerWaitCall[T any](m *Manager, ctx context.Context, call func(*Client) (T, error)) (T, error) {
	var zero T
	readinessCtx, cancelReadiness := context.WithTimeout(ctx, m.config.ReadinessTimeout)
	generation, err := m.acquire(readinessCtx)
	cancelReadiness()
	if err != nil {
		return zero, err
	}
	value, err := call(generation.client)
	if err == nil {
		return value, nil
	}
	if ctx.Err() != nil {
		return zero, err
	}
	var transport *TransportError
	if !errors.As(err, &transport) {
		return zero, err
	}
	stopErr := m.poison(generation)
	return zero, errors.Join(err, stopErr, generation.diagnostics.err())
}

func managerUploadCall(
	m *Manager,
	ctx context.Context,
	source contentstream.Source,
	call func(*Client) (catalog.ContentRef, error),
) (catalog.ContentRef, error) {
	if source == nil {
		return catalog.ContentRef{}, errors.New("catalog worker: owned content source is required")
	}
	readinessCtx, cancelReadiness := context.WithTimeout(ctx, m.config.ReadinessTimeout)
	generation, err := m.acquire(readinessCtx)
	cancelReadiness()
	if err != nil {
		settleErr, waitErr := m.settleUploadSource(source, err)
		return catalog.ContentRef{}, errors.Join(err, settleErr, waitErr)
	}
	operationCtx, cancel := context.WithTimeout(ctx, m.config.OperationTimeout)
	defer cancel()
	type uploadResult struct {
		ref catalog.ContentRef
		err error
	}
	result := make(chan uploadResult, 1)
	go func() {
		ref, callErr := call(generation.client)
		result <- uploadResult{ref: ref, err: callErr}
	}()
	select {
	case settled := <-result:
		settleErr, waitErr := m.settleUploadSource(source, settled.err)
		if settleErr == nil && waitErr == nil {
			if settled.err == nil {
				return settled.ref, nil
			}
			var transport *TransportError
			if operationCtx.Err() == nil && !errors.As(settled.err, &transport) {
				return catalog.ContentRef{}, settled.err
			}
		}
		return catalog.ContentRef{}, errors.Join(
			settled.err, settleErr, waitErr, m.poison(generation),
		)
	case <-operationCtx.Done():
		settleResult := make(chan error, 1)
		stopResult := make(chan error, 1)
		go func() { settleResult <- source.Settle(operationCtx.Err()) }()
		go func() { stopResult <- m.poison(generation) }()
		timeout := m.config.StopTimeout
		if timeout <= 0 {
			timeout = defaultStopTimeout
		}
		waitCtx, waitCancel := context.WithTimeout(context.WithoutCancel(m.lifecycle), timeout)
		waitErr := source.Wait(waitCtx)
		waitCancel()
		settleErr := <-settleResult
		stopErr := <-stopResult
		settled := <-result
		return catalog.ContentRef{}, errors.Join(
			operationCtx.Err(), settled.err, settleErr, waitErr, stopErr,
		)
	}
}

func (m *Manager) settleUploadSource(source contentstream.Source, result error) (error, error) {
	settleErr := source.Settle(result)
	timeout := m.config.StopTimeout
	if timeout <= 0 {
		timeout = defaultStopTimeout
	}
	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(m.lifecycle), timeout)
	waitErr := source.Wait(waitCtx)
	cancel()
	return settleErr, waitErr
}

func (m *Manager) acquire(ctx context.Context) (*workerGeneration, error) {
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, errors.New("catalog worker: manager closed")
		}
		if retiring := m.retiring; retiring != nil {
			settled := retiring.settled
			m.mu.Unlock()
			select {
			case <-settled:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if m.current != nil && !m.current.poisoned {
			generation := m.current
			m.mu.Unlock()
			return generation, nil
		}
		if starting := m.starting; starting != nil {
			done := starting.done
			m.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		m.next++
		number := m.next
		startCtx, cancel := context.WithCancel(ctx)
		starting := &workerStart{cancel: cancel, done: make(chan struct{})}
		m.starting = starting
		m.mu.Unlock()

		generation, err := m.start(startCtx, number)
		cancel()
		m.mu.Lock()
		closed := m.closed
		if err == nil && !closed {
			m.current = generation
		}
		if err == nil && closed {
			generation.poisoned = true
			m.retiring = generation
			m.mu.Unlock()
			settleErr := m.abort(generation, errors.New("catalog worker: manager closed during start"))
			m.mu.Lock()
			err = errors.Join(errors.New("catalog worker: manager closed during start"), settleErr)
		}
		if m.starting == starting {
			starting.err = err
			m.starting = nil
		}
		close(starting.done)
		m.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return generation, nil
	}
}

func (m *Manager) start(ctx context.Context, number uint64) (*workerGeneration, error) {
	generationName := strconv.FormatUint(number, 16)
	arguments, err := ChildArguments(m.config.Database, generationName, m.runtime)
	if err != nil {
		return nil, err
	}
	var client *Client
	diagnostics := &childDiagnostics{}
	stderr := io.Writer(diagnostics)
	if m.config.Stderr != nil {
		stderr = io.MultiWriter(m.config.Stderr, diagnostics)
	}
	ladder, err := generatedLadder(childSessionServerDeadline, childSessionClientDeadline)
	if err != nil {
		return nil, fmt.Errorf("catalog worker: construct deadline ladder: %w", err)
	}
	readinessCtx, cancelReadiness := context.WithTimeout(ctx, m.config.ReadinessTimeout)
	process, session, err := m.launcher.StartSession(readinessCtx, sessionProcessSpec{
		Spawn: proc.SpawnConfig{
			RecoveryClass:     proc.RecoveryCatalogWorker,
			Executable:        m.config.Executable,
			Args:              arguments,
			Env:               append([]string(nil), m.environment...),
			Stdin:             proc.StdioNull,
			Stdout:            proc.StdioNull,
			Stderr:            proc.StdioPipe,
			SpawnedSession:    true,
			ExpectedSignature: &m.config.ExpectedSignature,
		},
		Client: wire.SpawnedClientConfig{
			WireBuild: transportproto.WireBuild,
			Ladder:    ladder,
			Limits:    childSessionLimits(m.config.ReadinessTimeout),
		},
		Stderr: stderr, StopTimeout: m.config.StopTimeout,
	})
	cancelReadiness()
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("catalog worker: start generation: %w", err), diagnostics.err(),
		)
	}
	if process == nil || session == nil {
		var cleanupErr error
		if session != nil {
			cleanupErr = session.Abort(errors.New("catalog worker: process launcher returned an incomplete session"))
		}
		if process != nil {
			cleanupErr = errors.Join(cleanupErr, stopManagedProcess(m.lifecycle, process, m.config.StopTimeout))
		}
		return nil, errors.Join(
			errors.New("catalog worker: process launcher returned an incomplete session"),
			cleanupErr,
			diagnostics.err(),
		)
	}
	processIdentity := process.Identity()
	identity := WorkerIdentity{
		PID: processIdentity.PID, StartTime: processIdentity.StartTime, Boot: processIdentity.Boot, Generation: generationName,
	}
	client, err = newOwnedClient(session, identity)
	if err != nil {
		return nil, errors.Join(err, m.stopProcess(process), diagnostics.err())
	}
	if client == nil {
		return nil, errors.Join(
			errors.New("catalog worker: process became ready without a client"),
			m.stopProcess(process),
		)
	}
	return &workerGeneration{
		number: number, process: process, client: client,
		closeClient: client.Close, abortClient: client.Abort,
		diagnostics: diagnostics, settled: make(chan struct{}),
	}, nil
}

func catalogWorkerEnvironment(environment []string) []string {
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "PATH" || key == "LANG" || key == "CGOFUSE_LIBFUSE_PATH" {
			continue
		}
		result = append(result, entry)
	}
	return result
}

func (m *Manager) stopProcess(process managedProcess) error {
	timeout := m.config.StopTimeout
	if timeout <= 0 {
		timeout = defaultStopTimeout
	}
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(m.lifecycle), timeout)
	stopErr := process.Stop(stopCtx)
	cancel()
	return stopErr
}

func (m *Manager) poison(generation *workerGeneration) error {
	m.mu.Lock()
	if generation.poisoned {
		settled := generation.settled
		m.mu.Unlock()
		<-settled
		return generation.stopErr
	}
	generation.poisoned = true
	if m.current == generation {
		m.current = nil
	}
	m.retiring = generation
	m.mu.Unlock()
	return m.abort(generation, errors.New("catalog worker: generation poisoned"))
}

func (m *Manager) settle(generation *workerGeneration) error {
	closeClient := generation.closeClient
	if closeClient == nil {
		closeClient = generation.client.Close
	}
	return m.finishSettlement(generation, closeClient())
}

func (m *Manager) abort(generation *workerGeneration, cause error) error {
	abortClient := generation.abortClient
	if abortClient == nil {
		abortClient = generation.client.Abort
	}
	return m.finishSettlement(generation, abortClient(cause))
}

func (m *Manager) finishSettlement(generation *workerGeneration, clientErr error) error {
	generation.stopErr = errors.Join(clientErr, m.stopProcess(generation.process))
	close(generation.settled)
	m.mu.Lock()
	if m.retiring == generation {
		m.retiring = nil
	}
	m.mu.Unlock()
	return generation.stopErr
}
