package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
)

const testTimeout = 5 * time.Second
const runtimeFixtureSyntheticHead catalog.Revision = 1 << 20

type taskKey struct {
	lane     Lane
	revision catalog.Revision
}

type fakeWorkers struct {
	mu          sync.Mutex
	runHook     func(context.Context, taskKey) error
	proofHook   func(supervise.Task, taskKey) error
	recoverHook func(context.Context) error
	calls       []taskKey
	inputs      [][]byte
	active      int
	closed      bool
	canceled    bool
	closeCalls  int
	cancelCalls int
	recoveries  int
	events      chan taskKey
}

type fakeFleetTransitions struct {
	mu      sync.Mutex
	prepare func(context.Context, FleetTransition) error
	commit  func(context.Context, FleetTransition) error
	abort   func(context.Context, FleetTransition) error
	events  []string
	changes []FleetTransition
}

func newFakeFleetTransitions() *fakeFleetTransitions { return &fakeFleetTransitions{} }

func (h *fakeFleetTransitions) Prepare(ctx context.Context, change FleetTransition) error {
	h.mu.Lock()
	h.events = append(h.events, "prepare")
	h.changes = append(h.changes, change)
	hook := h.prepare
	h.mu.Unlock()
	if hook != nil {
		return hook(ctx, change)
	}
	return nil
}

func (h *fakeFleetTransitions) Commit(ctx context.Context, change FleetTransition) error {
	h.mu.Lock()
	h.events = append(h.events, "commit")
	h.changes = append(h.changes, change)
	hook := h.commit
	h.mu.Unlock()
	if hook != nil {
		return hook(ctx, change)
	}
	return nil
}

func (h *fakeFleetTransitions) Abort(ctx context.Context, change FleetTransition) error {
	h.mu.Lock()
	h.events = append(h.events, "abort")
	h.changes = append(h.changes, change)
	hook := h.abort
	h.mu.Unlock()
	if hook != nil {
		return hook(ctx, change)
	}
	return nil
}

func newFakeWorkers() *fakeWorkers {
	return &fakeWorkers{events: make(chan taskKey, 4096)}
}

func (w *fakeWorkers) Run(ctx context.Context, task supervise.Task) error {
	if task.RecoveryClass != proc.RecoveryTask {
		return fmt.Errorf("worker recovery class = %d, want task", task.RecoveryClass)
	}
	var input []byte
	if task.Stdin != nil {
		var err error
		input, err = io.ReadAll(task.Stdin)
		closeErr := task.Stdin.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	key, err := parseTask(task)
	if err != nil {
		return err
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return supervise.ErrClosed
	}
	w.calls = append(w.calls, key)
	w.inputs = append(w.inputs, input)
	w.active++
	hook := w.runHook
	proofHook := w.proofHook
	w.mu.Unlock()
	w.events <- key
	defer func() {
		w.mu.Lock()
		w.active--
		w.mu.Unlock()
	}()
	if proofHook != nil {
		if err := proofHook(task, key); err != nil {
			return err
		}
	} else if err := writeTaskProof(task, key); err != nil {
		return err
	}
	if hook != nil {
		return hook(ctx, key)
	}
	return nil
}

func (w *fakeWorkers) inputSnapshot() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	inputs := make([][]byte, len(w.inputs))
	for index := range w.inputs {
		inputs[index] = append([]byte(nil), w.inputs[index]...)
	}
	return inputs
}

func (w *fakeWorkers) Recover(ctx context.Context) error {
	w.mu.Lock()
	w.recoveries++
	hook := w.recoverHook
	w.mu.Unlock()
	if hook != nil {
		return hook(ctx)
	}
	return nil
}

func (w *fakeWorkers) Close() {
	w.mu.Lock()
	w.closed = true
	w.closeCalls++
	w.mu.Unlock()
}

func (w *fakeWorkers) Cancel() {
	w.mu.Lock()
	w.canceled = true
	w.cancelCalls++
	w.mu.Unlock()
}

func (w *fakeWorkers) Wait(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active != 0 {
		return fmt.Errorf("fake workers: %d operations remain active", w.active)
	}
	if !w.closed {
		return errors.New("fake workers: Wait before Close")
	}
	return nil
}

func (w *fakeWorkers) snapshot() (calls []taskKey, active, closeCalls, cancelCalls int, closed, canceled bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]taskKey(nil), w.calls...), w.active, w.closeCalls, w.cancelCalls, w.closed, w.canceled
}

type fakePlanner struct{}

func (fakePlanner) PrepareSourceMutation(_ context.Context, step SourceMutationStep) (SourceMutationOperation, error) {
	var result *catalog.SourceLocator
	if step.Kind == catalog.MutationCreate {
		value := catalog.SourceLocator{
			SourceAuthority: step.Source.Parent.SourceAuthority,
			SourceKey:       catalog.SourceObjectKey("created:" + step.OperationID.String()),
			SourceRevision:  step.Source.Parent.SourceRevision,
		}
		result = &value
	}
	return SourceMutationOperation{
		OperationID: step.OperationID, SourceID: step.SourceID, SourceMetadata: step.SourceMetadata,
		SourceResult: result, ExpectedSettlement: SourceMutationExternalApplied,
	}, nil
}

func (fakePlanner) ApplySourceMutation(ctx context.Context, _ SourceMutationStep, _ SourceMutationOperation, content SourceMutationContent) (result SourceMutationApplyResult, resultErr error) {
	result.Settlement = SourceMutationExternalApplied
	if content == nil {
		return result, nil
	}
	defer func() { resultErr = errors.Join(resultErr, content.Close()) }()
	reader, err := content.Open(ctx)
	if err != nil {
		return SourceMutationApplyResult{}, err
	}
	defer func() {
		settleErr := reader.Settle(resultErr)
		waitErr := reader.Wait(ctx)
		resultErr = errors.Join(resultErr, settleErr, waitErr)
	}()
	_, err = io.Copy(io.Discard, reader)
	return result, err
}

func (fakePlanner) SourceMutationCommitted(context.Context, SourceMutationCommit) error { return nil }

type recordingSourcePlanner struct {
	fakePlanner
	steps chan SourceMutationStep
}

type atomicSourcePlanner struct {
	store     *catalog.Catalog
	committed atomic.Int64
}

func (p *atomicSourcePlanner) PrepareSourceMutation(_ context.Context, step SourceMutationStep) (SourceMutationOperation, error) {
	return SourceMutationOperation{
		OperationID: step.OperationID, SourceID: step.SourceID, SourceMetadata: step.SourceMetadata,
		ExpectedSettlement: SourceMutationCatalogCommitted,
	}, nil
}

func (p *atomicSourcePlanner) ApplySourceMutation(
	ctx context.Context,
	step SourceMutationStep,
	_ SourceMutationOperation,
	_ SourceMutationContent,
) (SourceMutationApplyResult, error) {
	prepared, err := p.store.PreparedMutation(ctx, step.TenantID, step.OperationID)
	if err != nil {
		return SourceMutationApplyResult{}, err
	}
	if prepared.Claim == nil {
		return SourceMutationApplyResult{}, catalog.ErrIntegrity
	}
	if _, err := p.store.MarkMutationApplied(ctx, step.OperationID, *prepared.Claim); err != nil {
		return SourceMutationApplyResult{}, err
	}
	if _, err := p.store.CommitMutation(ctx, step.TenantID, step.OperationID); err != nil {
		return SourceMutationApplyResult{}, err
	}
	return SourceMutationApplyResult{Settlement: SourceMutationCatalogCommitted}, nil
}

func (p *atomicSourcePlanner) SourceMutationCommitted(context.Context, SourceMutationCommit) error {
	p.committed.Add(1)
	return nil
}

func (*atomicSourcePlanner) PrepareMountLifecycle(context.Context, Catalog, MountLifecycleStep) (*WorkerSpec, error) {
	return nil, nil
}

type commitCountingStore struct {
	Store
	commits atomic.Int64
}

func (s *commitCountingStore) CommitMutation(
	ctx context.Context,
	tenant catalog.TenantID,
	id catalog.MutationID,
) (catalog.NamespaceMutationResult, error) {
	s.commits.Add(1)
	return s.Store.CommitMutation(ctx, tenant, id)
}

func (p recordingSourcePlanner) PrepareSourceMutation(ctx context.Context, step SourceMutationStep) (SourceMutationOperation, error) {
	p.steps <- step
	return p.fakePlanner.PrepareSourceMutation(ctx, step)
}

func (fakePlanner) PrepareMountLifecycle(context.Context, Catalog, MountLifecycleStep) (*WorkerSpec, error) {
	return nil, nil
}

type mountPlanner struct{ fakePlanner }

func (mountPlanner) PrepareMountLifecycle(_ context.Context, _ Catalog, step MountLifecycleStep) (*WorkerSpec, error) {
	spec := workerSpecFor(step.Tenant, LaneMountLifecycle, step.Revision)
	return &spec, nil
}

type mismatchedSourcePlanner struct{ fakePlanner }

func (mismatchedSourcePlanner) PrepareSourceMutation(_ context.Context, step SourceMutationStep) (SourceMutationOperation, error) {
	return SourceMutationOperation{
		OperationID: step.OperationID, SourceID: step.SourceID, SourceMetadata: "different-operation",
	}, nil
}

func workerSpecFor(tenant TenantSpec, lane Lane, revision catalog.Revision) WorkerSpec {
	return WorkerSpec{Path: "/usr/bin/true", Args: []string{
		strconv.Itoa(int(lane)),
		strconv.FormatUint(uint64(revision), 10),
		string(tenant.ID),
		strconv.FormatUint(uint64(tenant.Generation), 10),
	}}
}

func parseTask(task supervise.Task) (taskKey, error) {
	if len(task.Args) != 4 {
		return taskKey{}, fmt.Errorf("fake workers: want four arguments, got %d", len(task.Args))
	}
	lane, err := strconv.ParseUint(task.Args[0], 10, 8)
	if err != nil {
		return taskKey{}, err
	}
	revision, err := strconv.ParseUint(task.Args[1], 10, 64)
	if err != nil {
		return taskKey{}, err
	}
	return taskKey{lane: Lane(lane), revision: catalog.Revision(revision)}, nil
}

func writeTaskProof(task supervise.Task, key taskKey) error {
	if task.Stdout == nil {
		return errors.New("fake workers: proof sink is required")
	}
	generation, err := strconv.ParseUint(task.Args[3], 10, 64)
	if err != nil {
		return err
	}
	return json.NewEncoder(task.Stdout).Encode(workerProof{
		Tenant: catalog.TenantID(task.Args[2]), Generation: catalog.Generation(generation),
		Revision: key.revision, Lane: key.lane,
	})
}

func newFixture(t *testing.T, workers WorkerPool, generation catalog.Generation) (*catalog.Catalog, TenantSpec, *TenantRuntime) {
	t.Helper()
	store, spec := newStoreAndSpec(t, generation)
	runtime := newProvisionedRuntime(t, store, workers, fakePlanner{}, spec)
	return store, spec, runtime
}

func newStoreAndSpec(t *testing.T, generation catalog.Generation) (*catalog.Catalog, TenantSpec) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	dir := t.TempDir()
	store, err := catalog.Open(ctx, filepath.Join(dir, "catalog.db"))
	if err != nil {
		t.Fatalf("Open catalog: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close catalog: %v", err)
		}
	})
	tenantID, err := catalog.NewTenantID("tenant")
	if err != nil {
		t.Fatalf("NewTenantID: %v", err)
	}
	presentations := catalog.PresentMount | catalog.PresentFileProvider
	spec := TenantSpec{
		OwnerID:          "test-owner",
		ID:               tenantID,
		PresentationRoot: filepath.Join(dir, "presentation"),
		Backing:          BackingSpec{Root: filepath.Join(dir, "backing")},
		Content:          ContentSource{ID: "test-content"},
		Traits: TenantTraits{
			Access: ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: presentations,
		},
		FileProvider: FileProviderSpec{Enabled: true, AccountInstanceID: "tenant-instance", DisplayName: "Tenant"},
		Generation:   generation,
	}
	return store, spec
}

type runtimeTestStore struct {
	Store
	head catalog.Revision
}

type failingFleetStore struct {
	Store
	replaceErr error
	removeErr  error
}

type blockingReplaceStore struct {
	Store
	started chan struct{}
	release chan struct{}
}

func (s blockingReplaceStore) ReplaceTenantProvision(ctx context.Context, generation catalog.Generation, provision catalog.TenantProvision) (catalog.TenantProvision, error) {
	close(s.started)
	<-s.release
	return s.Store.ReplaceTenantProvision(ctx, generation, provision)
}

func (s failingFleetStore) ReplaceTenantProvision(context.Context, catalog.Generation, catalog.TenantProvision) (catalog.TenantProvision, error) {
	return catalog.TenantProvision{}, s.replaceErr
}

func (s failingFleetStore) RemoveTenantProvision(context.Context, catalog.TenantID, catalog.Generation) error {
	return s.removeErr
}

func (s *runtimeTestStore) PrepareMutationSource(_ context.Context, id catalog.MutationID, claim catalog.MutationClaim) (catalog.PreparedMutation, error) {
	pending, err := s.PendingMutation(context.Background(), "tenant")
	if err != nil {
		return catalog.PreparedMutation{}, err
	}
	if pending != nil && pending.OperationID == id && pending.Claim != nil && *pending.Claim == claim {
		mutation := *pending
		if mutation.Intent.Create == nil {
			return catalog.PreparedMutation{}, errors.New("runtime fixture only resolves create mutations")
		}
		locator := catalog.SourceLocator{SourceAuthority: "test-content", SourceKey: "root", SourceRevision: 1}
		mutation.Source = &catalog.SourceMutationContext{
			Operation: catalog.SourceMutationOperation{
				Kind: catalog.MutationCreate, Name: mutation.Intent.Create.Spec.Name,
				ObjectKind: mutation.Intent.Create.Spec.Kind, Mode: mutation.Intent.Create.Spec.Mode,
				LinkTarget: mutation.Intent.Create.Spec.LinkTarget, HasContent: mutation.Intent.Create.Spec.Kind == catalog.KindFile,
			},
			Parent: &locator,
		}
		return mutation, nil
	}
	return catalog.PreparedMutation{}, catalog.ErrNotFound
}

func (s *runtimeTestStore) SetMutationSourceResult(_ context.Context, id catalog.MutationID, claim catalog.MutationClaim, locator catalog.SourceLocator) (catalog.PreparedMutation, error) {
	mutation, err := s.PrepareMutationSource(context.Background(), id, claim)
	if err != nil {
		return catalog.PreparedMutation{}, err
	}
	mutation.SourceResult = &locator
	return mutation, nil
}

func (s *runtimeTestStore) Head(context.Context, catalog.TenantID) (catalog.Revision, error) {
	return s.head, nil
}

func (s *runtimeTestStore) VerifyMaterialization(
	ctx context.Context,
	tenant catalog.TenantID,
	generation catalog.Generation,
	revision catalog.Revision,
) error {
	head, err := s.Store.Head(ctx, tenant)
	if err != nil {
		return err
	}
	if err := s.Store.VerifyMaterialization(ctx, tenant, generation, head); err != nil {
		return err
	}
	if revision == 0 || revision > s.head {
		return catalog.ErrInvalidTransition
	}
	return nil
}

func newProvisionedRuntime(t *testing.T, store Store, workers WorkerPool, planner Planner, spec TenantSpec) *TenantRuntime {
	t.Helper()
	testStore := &runtimeTestStore{Store: store, head: runtimeFixtureSyntheticHead}
	if _, err := testStore.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
		t.Fatalf("ProvisionTenant fixture: %v", err)
	}
	runtime, err := NewRuntime(t.Context(), testStore, workers, planner, newFakeFleetTransitions(), []catalog.TenantProvision{tenantProvision(spec)})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	return runtime
}

func TestRuntimeFixtureSyntheticHeadPreservesRealMaterializationChecks(t *testing.T) {
	store, spec := newStoreAndSpec(t, 7)
	if _, err := store.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
		t.Fatal(err)
	}
	fixture := &runtimeTestStore{Store: store, head: runtimeFixtureSyntheticHead}
	if head, err := fixture.Head(t.Context(), spec.ID); err != nil || head != runtimeFixtureSyntheticHead {
		t.Fatalf("synthetic Head = %d, %v", head, err)
	}
	if err := fixture.VerifyMaterialization(t.Context(), spec.ID, spec.Generation, 3); err != nil {
		t.Fatalf("synthetic revision after real proof: %v", err)
	}
	if err := fixture.VerifyMaterialization(
		t.Context(), spec.ID, spec.Generation+1, 3,
	); !errors.Is(err, catalog.ErrInvalidTransition) {
		t.Fatalf("generation mismatch = %v, want ErrInvalidTransition", err)
	}
	if err := fixture.VerifyMaterialization(
		t.Context(), spec.ID, spec.Generation, runtimeFixtureSyntheticHead+1,
	); !errors.Is(err, catalog.ErrInvalidTransition) {
		t.Fatalf("revision beyond synthetic head = %v, want ErrInvalidTransition", err)
	}
}

func closeRuntime(t *testing.T, runtime *TenantRuntime) {
	t.Helper()
	runtime.Close()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := runtime.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func waitEvent(t *testing.T, workers *fakeWorkers) taskKey {
	t.Helper()
	select {
	case event := <-workers.events:
		return event
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for worker event")
		return taskKey{}
	}
}

func waitTenantTransition(t *testing.T, runtime *TenantRuntime, tenant catalog.TenantID) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		runtime.mu.Lock()
		slot := runtime.tenants[tenant]
		transitioning := slot != nil && slot.transitioning
		runtime.mu.Unlock()
		if transitioning {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for tenant lifecycle transition")
}

func beginDirectoryMutation(t *testing.T, store *catalog.Catalog, tenant catalog.TenantID, name string) catalog.PreparedMutation {
	t.Helper()
	root, err := store.Root(context.Background(), tenant)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	head, err := store.Head(context.Background(), tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	prepared, err := store.BeginMutation(context.Background(), tenant, head, catalog.MutationIntent{
		SourceID: "test-source", SourceMetadata: "operation-metadata",
		Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
		Create: &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: root.ID, Name: name, Kind: catalog.KindDirectory, Mode: 0o755, Visibility: catalog.Visibility{Mount: true, FileProvider: true},
		}},
	})
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	return prepared
}

func beginFileMutation(t *testing.T, store *catalog.Catalog, tenant catalog.TenantID, name, body string) catalog.PreparedMutation {
	t.Helper()
	root, err := store.Root(context.Background(), tenant)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	content, err := store.StageContent(context.Background(), strings.NewReader(body))
	if err != nil {
		t.Fatalf("StageContent: %v", err)
	}
	head, err := store.Head(context.Background(), tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	prepared, err := store.BeginMutation(context.Background(), tenant, head, catalog.MutationIntent{
		SourceID: "test-source", SourceMetadata: "operation-metadata",
		Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
		Create: &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: root.ID, Name: name, Kind: catalog.KindFile, Mode: 0o644,
			ContentRevision: 1, Content: content, Visibility: catalog.Visibility{Mount: true, FileProvider: true},
		}},
	})
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	return prepared
}

type contentOpenStore struct {
	*catalog.Catalog
	mu     sync.Mutex
	opens  int
	states []catalog.PreparedMutationState
	last   contentstream.Source
}

func (s *contentOpenStore) OpenMutationContent(
	ctx context.Context, tenant catalog.TenantID, id catalog.MutationID,
) (contentstream.Source, error) {
	mutation, err := s.PreparedMutation(ctx, tenant, id)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.opens++
	s.states = append(s.states, mutation.State)
	s.mu.Unlock()
	if mutation.State != catalog.MutationApplying {
		return nil, fmt.Errorf("content opened outside applying claim: %d", mutation.State)
	}
	content, err := s.Catalog.OpenMutationContent(ctx, tenant, id)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.last = content
	s.mu.Unlock()
	return content, nil
}

func (s *contentOpenStore) snapshot() (int, []catalog.PreparedMutationState, contentstream.Source) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens, append([]catalog.PreparedMutationState(nil), s.states...), s.last
}

type pendingOverrideStore struct {
	*catalog.Catalog
	pending *catalog.PreparedMutation
}

func (s *pendingOverrideStore) PendingMutation(context.Context, catalog.TenantID) (*catalog.PreparedMutation, error) {
	if s.pending == nil {
		return nil, nil
	}
	value := *s.pending
	return &value, nil
}

type countingVerificationStore struct {
	Store
	calls atomic.Int32
}

type controlledVerificationStore struct {
	Store
	verify func(context.Context, catalog.TenantID, catalog.Generation, catalog.Revision) error
}

func (s *controlledVerificationStore) VerifyMaterialization(
	ctx context.Context,
	tenant catalog.TenantID,
	generation catalog.Generation,
	revision catalog.Revision,
) error {
	if s.verify != nil {
		return s.verify(ctx, tenant, generation, revision)
	}
	return s.Store.VerifyMaterialization(ctx, tenant, generation, revision)
}

func (s *countingVerificationStore) VerifyMaterialization(
	ctx context.Context,
	tenant catalog.TenantID,
	generation catalog.Generation,
	revision catalog.Revision,
) error {
	s.calls.Add(1)
	return s.Store.VerifyMaterialization(ctx, tenant, generation, revision)
}

func TestPrepareTenantVerifiesLogicallyAppliedSameRevisionOnDemand(t *testing.T) {
	store, spec, bootstrap := newFixture(t, newFakeWorkers(), 1)
	closeRuntime(t, bootstrap)
	record, err := store.LoadTenantState(t.Context(), spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	record.Verified = 0
	if _, err := store.SaveTenantState(t.Context(), record.Version, record); err != nil {
		t.Fatal(err)
	}
	observed := &countingVerificationStore{Store: store}
	runtime, err := NewRuntime(t.Context(), observed, newFakeWorkers(), fakePlanner{}, newFakeFleetTransitions(), []catalog.TenantProvision{tenantProvision(spec)})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		state, err := runtime.PrepareTenant(t.Context(), spec.ID, 1)
		if err != nil || !state.Prepared() {
			t.Fatalf("same-revision PrepareTenant: state=%+v err=%v", state, err)
		}
	}
	if calls := observed.calls.Load(); calls != 1 {
		t.Fatalf("materialization verification calls = %d, want one on-demand catch-up", calls)
	}
	closeRuntime(t, runtime)
}

func TestPrepareTenantRunsCatalogAndMaterializationProofsWithoutAuxiliaryWorker(t *testing.T) {
	workers := newFakeWorkers()
	_, spec, runtime := newFixture(t, workers, 7)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil {
		t.Fatalf("PrepareTenant: %v", err)
	}
	if !state.Prepared() || state.Requested != 3 || state.Generation != 7 || state.Desired != 3 || state.Observed != 3 || state.Verified != 3 || state.Applied != 3 {
		t.Fatalf("unexpected proof: %+v", state)
	}
	calls, _, _, _, _, _ := workers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("catalog-backed materialization launched an auxiliary worker: %v", calls)
	}
	closeRuntime(t, runtime)
}

func TestAlreadyPreparedAndNoDemandRevisionsLaunchNoWorkers(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	workers := newFakeWorkers()
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{}, newFakeFleetTransitions(), []catalog.TenantProvision{tenantProvision(spec)})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 1)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant initial no-demand revision: state=%+v err=%v", state, err)
	}
	state, err = runtime.PrepareTenant(context.Background(), spec.ID, 1)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant already prepared revision: state=%+v err=%v", state, err)
	}
	prepared := beginDirectoryMutation(t, store, spec.ID, "content-only")
	owner, err := catalog.NewMutationOwnerID()
	if err != nil {
		t.Fatalf("NewMutationOwnerID: %v", err)
	}
	claimed, err := store.ClaimMutation(context.Background(), prepared.OperationID, owner)
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	if _, err := store.MarkMutationApplied(context.Background(), prepared.OperationID, *claimed.Claim); err != nil {
		t.Fatalf("MarkMutationApplied: %v", err)
	}
	committed, err := store.CommitMutation(context.Background(), prepared.Tenant, prepared.OperationID)
	if err != nil {
		t.Fatalf("CommitMutation: %v", err)
	}
	state, err = runtime.PrepareTenant(context.Background(), spec.ID, committed.Mutation.Revision)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant content-only no-demand revision: state=%+v err=%v", state, err)
	}
	calls, _, _, _, _, _ := workers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("no-demand preparation launched workers: %v", calls)
	}
	closeRuntime(t, runtime)
}

func TestMountLifecycleRunsOncePerGenerationActivation(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	spec.Generation = 2
	workers := newFakeWorkers()
	if _, err := store.ReplaceTenantProvision(t.Context(), 1, tenantProvision(spec)); err != nil {
		t.Fatalf("ReplaceTenantProvision generation 2: %v", err)
	}
	runtime, err := NewRuntime(t.Context(), store, workers, mountPlanner{}, newFakeFleetTransitions(), nil)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := runtime.ProvisionTenant(context.Background(), spec); err != nil {
		t.Fatalf("duplicate ProvisionTenant generation 2: %v", err)
	}
	next := spec
	next.Generation = 3
	if err := runtime.ReplaceTenant(context.Background(), 2, next); err != nil {
		t.Fatalf("ReplaceTenant generation 3: %v", err)
	}
	calls, _, _, _, _, _ := workers.snapshot()
	want := []taskKey{{LaneMountLifecycle, 1}, {LaneMountLifecycle, 1}}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("mount activation calls=%v, want once per generation %v", calls, want)
	}
	closeRuntime(t, runtime)

	restartWorkers := newFakeWorkers()
	restarted, err := NewRuntime(t.Context(), store, restartWorkers, mountPlanner{}, newFakeFleetTransitions(), mustRuntimeTenantProvisions(t, store))
	if err != nil {
		t.Fatalf("NewRuntime restart: %v", err)
	}
	calls, _, _, _, _, _ = restartWorkers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("persisted activation relaunched mount lifecycle: %v", calls)
	}
	closeRuntime(t, restarted)
}

func TestPrepareTenantRejectsRevisionAheadOfCatalog(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	workers := newFakeWorkers()
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{}, newFakeFleetTransitions(), []catalog.TenantProvision{tenantProvision(spec)})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 2)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) || state.Quarantine == nil || state.Quarantine.Lane != LaneCatalogMutation || state.Quarantine.Cause != catalog.QuarantineCauseIntegrity {
		t.Fatalf("ahead-of-catalog state=%+v err=%v", state, err)
	}
	calls, _, _, _, _, _ := workers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("ahead-of-catalog preparation launched workers: %v", calls)
	}
	closeRuntime(t, runtime)
}

func TestWorkerProofFailuresNeverAdvanceTheirLane(t *testing.T) {
	tests := []struct {
		name  string
		write func(supervise.Task, taskKey) error
	}{
		{name: "missing", write: func(supervise.Task, taskKey) error { return nil }},
		{name: "oversized", write: func(task supervise.Task, _ taskKey) error {
			_, err := task.Stdout.Write(bytes.Repeat([]byte("x"), maxWorkerProofBytes+1))
			return err
		}},
		{name: "wrong generation", write: func(task supervise.Task, key taskKey) error {
			generation, err := strconv.ParseUint(task.Args[3], 10, 64)
			if err != nil {
				return err
			}
			return json.NewEncoder(task.Stdout).Encode(workerProof{
				Tenant: catalog.TenantID(task.Args[2]), Generation: catalog.Generation(generation + 1),
				Revision: key.revision, Lane: key.lane,
			})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, spec := newStoreAndSpec(t, 7)
			if _, err := store.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
				t.Fatal(err)
			}
			workers := newFakeWorkers()
			workers.proofHook = test.write
			if runtime, err := NewRuntime(
				t.Context(), store, workers, mountPlanner{}, newFakeFleetTransitions(),
				[]catalog.TenantProvision{tenantProvision(spec)},
			); err == nil {
				closeRuntime(t, runtime)
				t.Fatal("invalid mount-lifecycle proof was accepted")
			}
			record, err := store.LoadTenantState(t.Context(), spec.ID)
			if err != nil {
				t.Fatal(err)
			}
			if record.ActivatedGeneration != 0 {
				t.Fatalf("invalid proof activated generation: %+v", record)
			}
			workers.mu.Lock()
			workers.proofHook = nil
			workers.mu.Unlock()
			runtime, err := NewRuntime(
				t.Context(), store, workers, mountPlanner{}, newFakeFleetTransitions(),
				[]catalog.TenantProvision{tenantProvision(spec)},
			)
			if err != nil {
				t.Fatalf("retry with exact proof: %v", err)
			}
			record, err = store.LoadTenantState(t.Context(), spec.ID)
			if err != nil || record.ActivatedGeneration != spec.Generation {
				t.Fatalf("exact proof activation = %+v, %v", record, err)
			}
			closeRuntime(t, runtime)
		})
	}
}

func TestWorkerProofDoesNotAdvanceBeforeProcessReap(t *testing.T) {
	store, spec := newStoreAndSpec(t, 9)
	if _, err := store.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
		t.Fatal(err)
	}
	workers := newFakeWorkers()
	release := make(chan struct{})
	workers.runHook = func(ctx context.Context, key taskKey) error {
		if key.lane != LaneMountLifecycle {
			return nil
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	type runtimeResult struct {
		runtime *TenantRuntime
		err     error
	}
	result := make(chan runtimeResult, 1)
	go func() {
		runtime, err := NewRuntime(
			context.Background(), store, workers, mountPlanner{}, newFakeFleetTransitions(),
			[]catalog.TenantProvision{tenantProvision(spec)},
		)
		result <- runtimeResult{runtime: runtime, err: err}
	}()
	waitEvent(t, workers)
	record, err := store.LoadTenantState(context.Background(), spec.ID)
	if err != nil {
		t.Fatalf("LoadTenantState while worker remains unreaped: %v", err)
	}
	if record.ActivatedGeneration != 0 {
		t.Fatalf("proof advanced before worker reap: %+v", record)
	}
	close(release)
	settled := <-result
	if settled.err != nil {
		t.Fatalf("NewRuntime after reap: %v", settled.err)
	}
	record, err = store.LoadTenantState(context.Background(), spec.ID)
	if err != nil || record.ActivatedGeneration != spec.Generation {
		t.Fatalf("activation after reap = %+v, %v", record, err)
	}
	closeRuntime(t, settled.runtime)
}

func TestProofWorkerRunsThroughRealDaemonkitPoolWithRecoveryClass(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	if _, err := store.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
		t.Fatal(err)
	}
	reaper := &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(t.TempDir(), "processes.db")},
		Generation: "tenant-proof-test",
	}
	pool, err := supervise.NewPool(1, reaper)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if err := pool.Wait(context.Background()); err != nil {
			t.Errorf("wait for proof pool: %v", err)
		}
	})
	payload, err := json.Marshal(workerProof{
		Tenant: spec.ID, Generation: spec.Generation, Revision: 0, Lane: LaneMountLifecycle,
	})
	if err != nil {
		t.Fatal(err)
	}
	actor := &tenantActor{store: store, workers: pool, spec: spec}
	if err := actor.runProofWorker(t.Context(), LaneMountLifecycle, 0, WorkerSpec{
		Path: "/bin/echo", Args: []string{string(payload)},
	}); err != nil {
		t.Fatalf("real daemonkit proof worker: %v", err)
	}
}

func TestRuntimeStartsEmptyAndProvisioningIsExact(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	workers := newFakeWorkers()
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{}, newFakeFleetTransitions(), nil)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if specs := runtime.Specs(); len(specs) != 0 {
		t.Fatalf("zero-spec runtime = %v", specs)
	}
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 1); !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("PrepareTenant before provisioning = %v, want ErrTenantNotFound", err)
	}
	if err := runtime.ProvisionTenant(context.Background(), spec); err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	if err := runtime.ProvisionTenant(context.Background(), spec); err != nil {
		t.Fatalf("exact duplicate ProvisionTenant: %v", err)
	}
	mismatch := spec
	mismatch.Content.ID = "different-content"
	if err := runtime.ProvisionTenant(context.Background(), mismatch); !errors.Is(err, ErrTenantConflict) {
		t.Fatalf("mismatched duplicate = %v, want ErrTenantConflict", err)
	}
	if specs := runtime.Specs(); len(specs) != 1 || specs[0] != spec {
		t.Fatalf("Specs = %v, want exact provisioned spec", specs)
	}
	closeRuntime(t, runtime)
}

func TestTenantStateIsOwnerFencedAndDurableAcrossRestart(t *testing.T) {
	store, spec, runtime := newFixture(t, newFakeWorkers(), 5)
	status, err := runtime.State(t.Context(), spec.OwnerID, spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Owner != spec.OwnerID || status.State.Tenant != spec.ID || status.State.Generation != 5 || !status.ReplacementEligible {
		t.Fatalf("status = %+v", status)
	}
	if _, err := runtime.State(t.Context(), "other-owner", spec.ID); !errors.Is(err, ErrTenantOwnerMismatch) {
		t.Fatalf("owner mismatch = %v", err)
	}
	if _, err := runtime.State(t.Context(), spec.OwnerID, "absent"); !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("absent tenant = %v", err)
	}
	closeRuntime(t, runtime)

	restarted, err := NewRuntime(t.Context(), store, newFakeWorkers(), fakePlanner{}, newFakeFleetTransitions(), mustRuntimeTenantProvisions(t, store))
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := restarted.State(t.Context(), spec.OwnerID, spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Owner != status.Owner || recovered.State.Tenant != status.State.Tenant ||
		recovered.State.Generation != status.State.Generation || recovered.State.Desired != status.State.Desired ||
		recovered.State.Applied != status.State.Applied || !recovered.ReplacementEligible {
		t.Fatalf("recovered status = %+v, want durable fields from %+v", recovered, status)
	}
	closeRuntime(t, restarted)
}

func TestProvisionTenantLinearizesAgainstPrepare(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	workers := newFakeWorkers()
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{}, newFakeFleetTransitions(), nil)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	start := make(chan struct{})
	provisioned := make(chan error, 1)
	prepared := make(chan error, 1)
	go func() {
		<-start
		provisioned <- runtime.ProvisionTenant(context.Background(), spec)
	}()
	go func() {
		<-start
		_, err := runtime.PrepareTenant(context.Background(), spec.ID, 1)
		prepared <- err
	}()
	close(start)
	if err := <-provisioned; err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	if err := <-prepared; err != nil && !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("racing PrepareTenant = %v", err)
	}
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 1); err != nil {
		t.Fatalf("PrepareTenant after provisioning: %v", err)
	}
	closeRuntime(t, runtime)
}

func TestRuntimeRecoversReplacedAndRemovedDesiredTenants(t *testing.T) {
	store, first := newStoreAndSpec(t, 1)
	workers := newFakeWorkers()
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{}, newFakeFleetTransitions(), nil)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	second := first
	second.ID = catalog.TenantID("tenant-other")
	second.PresentationRoot += "-other"
	second.Backing.Root += "-other"
	second.Content.ID += "-other"
	if err := runtime.ProvisionTenant(t.Context(), first); err != nil {
		t.Fatalf("ProvisionTenant first: %v", err)
	}
	if err := runtime.ProvisionTenant(t.Context(), second); err != nil {
		t.Fatalf("ProvisionTenant second: %v", err)
	}
	next := first
	next.Generation = 2
	if err := runtime.ReplaceTenant(t.Context(), 1, next); err != nil {
		t.Fatalf("ReplaceTenant first: %v", err)
	}
	if err := runtime.RemoveTenant(t.Context(), second.ID, 1); err != nil {
		t.Fatalf("RemoveTenant second: %v", err)
	}
	runtime.Cancel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := runtime.Wait(ctx); err != nil {
		t.Fatalf("Wait canceled runtime: %v", err)
	}

	restarted := newFakeWorkers()
	recovered, err := NewRuntime(t.Context(), store, restarted, fakePlanner{}, newFakeFleetTransitions(), mustRuntimeTenantProvisions(t, store))
	if err != nil {
		t.Fatalf("NewRuntime recovery: %v", err)
	}
	if specs := recovered.Specs(); len(specs) != 1 || specs[0] != next {
		t.Fatalf("recovered Specs = %+v, want [%+v]", specs, next)
	}
	if _, err := recovered.State(t.Context(), second.OwnerID, second.ID); !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("removed tenant recovered: %v", err)
	}
	provisions, err := allRuntimeTenantProvisions(t, store)
	if err != nil || len(provisions) != 1 || provisionSpec(provisions[0]) != next {
		t.Fatalf("durable desired tenants = %+v, %v; want [%+v]", provisions, err, next)
	}
	closeRuntime(t, recovered)
}

func TestReplaceTenantDrainsOldWaiterBeforeGenerationSwap(t *testing.T) {
	workers := newFakeWorkers()
	started := make(chan struct{})
	release := make(chan struct{})
	store, spec := newStoreAndSpec(t, 1)
	var startOnce sync.Once
	observed := &controlledVerificationStore{Store: store}
	observed.verify = func(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, revision catalog.Revision) error {
		startOnce.Do(func() { close(started) })
		select {
		case <-release:
			return store.VerifyMaterialization(ctx, tenant, generation, revision)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	runtime := newProvisionedRuntime(t, observed, workers, fakePlanner{}, spec)
	oldResult := make(chan prepareResult, 1)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 2)
		oldResult <- prepareResult{state: state, err: err}
	}()
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("old generation verification did not start")
	}
	next := spec
	next.Generation = 2
	replaced := make(chan error, 1)
	go func() { replaced <- runtime.ReplaceTenant(context.Background(), 1, next) }()
	waitTenantTransition(t, runtime, spec.ID)
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 3); !errors.Is(err, ErrTenantChanging) {
		t.Fatalf("PrepareTenant during replacement = %v, want ErrTenantChanging", err)
	}
	close(release)
	old := <-oldResult
	if old.err != nil || old.state.Generation != 1 || !old.state.Prepared() {
		t.Fatalf("old waiter = %+v, %v", old.state, old.err)
	}
	if err := <-replaced; err != nil {
		t.Fatalf("ReplaceTenant: %v", err)
	}
	if specs := runtime.Specs(); len(specs) != 1 || specs[0] != next {
		t.Fatalf("Specs after replacement = %v", specs)
	}
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil || state.Generation != 2 || !state.Prepared() {
		t.Fatalf("new generation prepare = %+v, %v", state, err)
	}
	_, _, _, _, closed, _ := workers.snapshot()
	if closed {
		t.Fatal("replacement closed the shared worker pool")
	}
	closeRuntime(t, runtime)
}

func TestRemoveTenantDrainsActiveWorkerWithoutDeletingData(t *testing.T) {
	workers := newFakeWorkers()
	started := make(chan struct{})
	release := make(chan struct{})
	store, spec := newStoreAndSpec(t, 1)
	var first sync.Once
	observed := &controlledVerificationStore{Store: store}
	observed.verify = func(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, revision catalog.Revision) error {
		first.Do(func() { close(started) })
		select {
		case <-release:
			return store.VerifyMaterialization(ctx, tenant, generation, revision)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	runtime := newProvisionedRuntime(t, observed, workers, fakePlanner{}, spec)
	active := make(chan prepareResult, 1)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 2)
		active <- prepareResult{state: state, err: err}
	}()
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("active verification did not start")
	}
	removed := make(chan error, 1)
	go func() { removed <- runtime.RemoveTenant(context.Background(), spec.ID, 1) }()
	waitTenantTransition(t, runtime, spec.ID)
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 3); !errors.Is(err, ErrTenantChanging) {
		t.Fatalf("PrepareTenant during removal = %v, want ErrTenantChanging", err)
	}
	close(release)
	result := <-active
	if result.err != nil || !result.state.Prepared() {
		t.Fatalf("active waiter = %+v, %v", result.state, result.err)
	}
	if err := <-removed; err != nil {
		t.Fatalf("RemoveTenant: %v", err)
	}
	if specs := runtime.Specs(); len(specs) != 0 {
		t.Fatalf("Specs after removal = %v", specs)
	}
	if _, err := store.Root(context.Background(), spec.ID); err != nil {
		t.Fatalf("removal deleted catalog data: %v", err)
	}
	_, _, _, _, closed, _ := workers.snapshot()
	if closed {
		t.Fatal("removal closed the shared worker pool")
	}
	closeRuntime(t, runtime)
}

func TestTenantGenerationFencesAndAddRemoveLoop(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	workers := newFakeWorkers()
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{}, newFakeFleetTransitions(), []catalog.TenantProvision{tenantProvision(spec)})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	next := spec
	next.Generation = 2
	if err := runtime.ReplaceTenant(context.Background(), 2, next); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("stale ReplaceTenant = %v, want ErrGenerationConflict", err)
	}
	if err := runtime.RemoveTenant(context.Background(), spec.ID, 2); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("stale RemoveTenant = %v, want ErrGenerationConflict", err)
	}
	if err := runtime.ReplaceTenant(context.Background(), 1, next); err != nil {
		t.Fatalf("ReplaceTenant: %v", err)
	}
	if err := runtime.RemoveTenant(context.Background(), spec.ID, 1); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("old-generation RemoveTenant = %v, want ErrGenerationConflict", err)
	}
	if err := runtime.RemoveTenant(context.Background(), spec.ID, 2); err != nil {
		t.Fatalf("RemoveTenant generation 2: %v", err)
	}
	for range 50 {
		next.Generation++
		if err := runtime.ProvisionTenant(context.Background(), next); err != nil {
			t.Fatalf("ProvisionTenant loop: %v", err)
		}
		if err := runtime.RemoveTenant(context.Background(), next.ID, next.Generation); err != nil {
			t.Fatalf("RemoveTenant loop: %v", err)
		}
	}
	if len(runtime.Specs()) != 0 {
		t.Fatalf("fleet not empty after add/remove loop: %v", runtime.Specs())
	}
	_, active, _, _, closed, _ := workers.snapshot()
	if active != 0 || closed {
		t.Fatalf("worker pool after add/remove loop active=%d closed=%v", active, closed)
	}
	closeRuntime(t, runtime)
}

func TestPreparedMutationReplaysBeforeOrdinaryLanes(t *testing.T) {
	workers := newFakeWorkers()
	store, spec, runtime := newFixture(t, workers, 1)
	beginDirectoryMutation(t, store, spec.ID, "replayed")
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant: state=%+v err=%v", state, err)
	}
	calls, _, _, _, _, _ := workers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("prepared mutation replay used auxiliary tenant worker: %v", calls)
	}
	root, err := store.Root(context.Background(), spec.ID)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	if _, err := store.LookupName(context.Background(), spec.ID, catalog.PresentationFileProvider, root.ID, "replayed"); err != nil {
		t.Fatalf("LookupName(replayed): %v", err)
	}
	pending, err := store.PendingMutation(context.Background(), spec.ID)
	if err != nil || pending != nil {
		t.Fatalf("PendingMutations after replay = %v, %v", pending, err)
	}
	closeRuntime(t, runtime)
}

func TestSourcePlannerReceivesOnlyAuthorityLocatorsAndPathFreeTenantIdentity(t *testing.T) {
	workers := newFakeWorkers()
	store, spec := newStoreAndSpec(t, 1)
	planner := recordingSourcePlanner{steps: make(chan SourceMutationStep, 1)}
	runtime := newProvisionedRuntime(t, store, workers, planner, spec)
	prepared := beginDirectoryMutation(t, store, spec.ID, "located")
	state, err := runtime.PrepareTenant(t.Context(), spec.ID, 3)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant: state=%+v err=%v", state, err)
	}
	step := <-planner.steps
	if step.TenantID != spec.ID || step.Generation != spec.Generation || step.OperationID != prepared.OperationID || step.ExpectedHead != prepared.ExpectedHead {
		t.Fatalf("source step identity = %+v", step)
	}
	if step.Source.Parent == nil || step.Source.Parent.SourceAuthority != "test-content" || step.Source.Parent.SourceKey != "root" || step.Source.Parent.SourceRevision != 1 {
		t.Fatalf("source step locators = %+v", step.Source)
	}
	stepType := reflect.TypeOf(SourceMutationStep{})
	for _, forbidden := range []string{"Tenant", "Intent", "Catalog", "BackingRoot", "CatalogPath"} {
		if _, present := stepType.FieldByName(forbidden); present {
			t.Fatalf("source step exposes forbidden field %q", forbidden)
		}
	}
	closeRuntime(t, runtime)
}

func TestSourceMutationStreamsClaimedFileBytesFromCatalog(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	body := strings.Repeat("streamed-content-", 1024)
	beginFileMutation(t, store, spec.ID, "streamed", body)
	observed := &contentOpenStore{Catalog: store}
	workers := newFakeWorkers()
	runtime := newProvisionedRuntime(t, observed, workers, fakePlanner{}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant: state=%+v err=%v", state, err)
	}
	opens, states, content := observed.snapshot()
	if opens != 1 || fmt.Sprint(string(states)) != fmt.Sprint(string([]catalog.PreparedMutationState{catalog.MutationApplying})) {
		t.Fatalf("content opens=%d states=%v, want one applying claim", opens, states)
	}
	inputs := workers.inputSnapshot()
	if len(inputs) != 0 {
		t.Fatalf("tenant workers received source bytes = %q", inputs)
	}
	if content == nil {
		t.Fatal("catalog content file was not captured")
	}
	if _, err := content.Read(make([]byte, 1)); err == nil {
		t.Fatal("catalog content source remained readable after worker completion")
	}
	closeRuntime(t, runtime)
}

func TestMetadataSourceMutationNeverOpensContent(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	beginDirectoryMutation(t, store, spec.ID, "metadata-only")
	observed := &contentOpenStore{Catalog: store}
	workers := newFakeWorkers()
	runtime := newProvisionedRuntime(t, observed, workers, fakePlanner{}, spec)
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 3); err != nil {
		t.Fatalf("PrepareTenant: %v", err)
	}
	opens, states, _ := observed.snapshot()
	if opens != 0 || len(states) != 0 {
		t.Fatalf("metadata mutation opened content %d times in states %v", opens, states)
	}
	inputs := workers.inputSnapshot()
	if len(inputs) != 0 {
		t.Fatalf("metadata source reached tenant worker stdin = %v", inputs)
	}
	closeRuntime(t, runtime)
}

func TestSourceOperationCannotSupplyExecutableOrPath(t *testing.T) {
	typeOfOperation := reflect.TypeOf(SourceMutationOperation{})
	for _, forbidden := range []string{"Spec", "Path", "Args", "Dir", "Env", "Input"} {
		if _, found := typeOfOperation.FieldByName(forbidden); found {
			t.Fatalf("source operation exposes forbidden field %q", forbidden)
		}
	}
}

func TestAppliedMutationCommitsWithoutRepeatingSourceWorker(t *testing.T) {
	workers := newFakeWorkers()
	store, spec, runtime := newFixture(t, workers, 1)
	prepared := beginDirectoryMutation(t, store, spec.ID, "already-applied")
	owner, err := catalog.NewMutationOwnerID()
	if err != nil {
		t.Fatalf("NewMutationOwnerID: %v", err)
	}
	claimed, err := store.ClaimMutation(context.Background(), prepared.OperationID, owner)
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	if _, err := store.MarkMutationApplied(context.Background(), prepared.OperationID, *claimed.Claim); err != nil {
		t.Fatalf("MarkMutationApplied: %v", err)
	}
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant: state=%+v err=%v", state, err)
	}
	calls, _, _, _, _, _ := workers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("applied mutation replay used auxiliary tenant worker: %v", calls)
	}
	closeRuntime(t, runtime)
}

func TestAtomicallyCommittedSourceMutationSkipsTenantDoubleCommit(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	observed := &commitCountingStore{Store: store}
	planner := &atomicSourcePlanner{store: store}
	runtime := newProvisionedRuntime(t, observed, newFakeWorkers(), planner, spec)
	prepared := beginDirectoryMutation(t, store, spec.ID, "atomic-source")
	if _, err := runtime.PrepareTenant(t.Context(), spec.ID, prepared.ExpectedHead+1); err != nil {
		t.Fatalf("PrepareTenant: %v", err)
	}
	if calls := observed.commits.Load(); calls != 0 {
		t.Fatalf("tenant CommitMutation calls after atomic source commit = %d, want 0", calls)
	}
	if calls := planner.committed.Load(); calls != 1 {
		t.Fatalf("SourceMutationCommitted calls = %d, want 1", calls)
	}
	mutation, err := store.PreparedMutation(t.Context(), spec.ID, prepared.OperationID)
	if err != nil || mutation.State != catalog.MutationCommitted {
		t.Fatalf("committed mutation = %+v, %v", mutation, err)
	}
	closeRuntime(t, runtime)
}

func TestApplyingMutationRequiresWorkerRecoveryBeforeReplay(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	prepared := beginDirectoryMutation(t, store, spec.ID, "recovered-claim")
	staleOwner, err := catalog.NewMutationOwnerID()
	if err != nil {
		t.Fatalf("NewMutationOwnerID: %v", err)
	}
	claimed, err := store.ClaimMutation(context.Background(), prepared.OperationID, staleOwner)
	if err != nil || claimed.Claim == nil {
		t.Fatalf("ClaimMutation: %+v, %v", claimed, err)
	}

	workers := newFakeWorkers()
	runtime := newProvisionedRuntime(t, store, workers, fakePlanner{}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) {
		t.Fatalf("PrepareTenant before recovery = %v, want QuarantinedError", err)
	}
	if state.Quarantine == nil || state.Quarantine.Lane != LaneCatalogMutation || state.Quarantine.Cause != catalog.QuarantineCauseUnsettled {
		t.Fatalf("unexpected applying quarantine: %+v", state.Quarantine)
	}
	calls, _, _, _, _, _ := workers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("worker admitted before claim recovery: %v", calls)
	}
	if err := runtime.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	state, err = runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant after recovery: state=%+v err=%v", state, err)
	}
	calls, _, _, _, _, _ = workers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("recovered mutation replay used auxiliary tenant worker: %v", calls)
	}
	pending, err := store.PendingMutation(context.Background(), spec.ID)
	if err != nil || pending != nil {
		t.Fatalf("PendingMutations after recovered replay = %v, %v", pending, err)
	}
	closeRuntime(t, runtime)
}

func TestSourceWorkerMustPreservePersistedOperationIdentity(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	prepared := beginDirectoryMutation(t, store, spec.ID, "identity-mismatch")
	workers := newFakeWorkers()
	runtime := newProvisionedRuntime(t, store, workers, mismatchedSourcePlanner{}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) {
		t.Fatalf("PrepareTenant error = %v, want QuarantinedError", err)
	}
	if state.Quarantine == nil || state.Quarantine.Lane != LaneCatalogMutation || state.Quarantine.Cause != catalog.QuarantineCauseIntegrity {
		t.Fatalf("unexpected quarantine: %+v", state.Quarantine)
	}
	calls, _, _, _, _, _ := workers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("mismatched source worker executed: %v", calls)
	}
	pending, err := store.PreparedMutation(context.Background(), prepared.Tenant, prepared.OperationID)
	if err != nil || pending.State != catalog.MutationApplying {
		t.Fatalf("prepared mutation after rejected worker = %+v, %v", pending, err)
	}
	closeRuntime(t, runtime)
}

func TestRecoveryRequiredMutationQuarantinesBeforeWorkerAdmission(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	prepared := beginDirectoryMutation(t, store, spec.ID, "recovery-required")
	prepared.State = catalog.MutationRecoveryRequired
	workers := newFakeWorkers()
	override := &pendingOverrideStore{Catalog: store, pending: &prepared}
	runtime := newProvisionedRuntime(t, override, workers, fakePlanner{}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 4)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) {
		t.Fatalf("PrepareTenant error = %v, want QuarantinedError", err)
	}
	if state.Quarantine == nil || state.Quarantine.Lane != LaneCatalogMutation || state.Quarantine.Cause != catalog.QuarantineCauseConflict {
		t.Fatalf("unexpected quarantine: %+v", state.Quarantine)
	}
	calls, _, _, _, _, _ := workers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("workers admitted behind recovery barrier: %v", calls)
	}
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 4); !errors.As(err, &quarantined) {
		t.Fatalf("recovery-required retry error = %v, want durable QuarantinedError", err)
	}
	calls, _, _, _, _, _ = workers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("retry admitted workers behind recovery barrier: %v", calls)
	}
	persisted, err := store.LoadTenantState(context.Background(), spec.ID)
	if err != nil || persisted.Quarantine == nil || persisted.Quarantine.Cause != catalog.QuarantineCauseConflict {
		t.Fatalf("durable quarantine = %+v, %v", persisted.Quarantine, err)
	}
	closeRuntime(t, runtime)
}

func TestQuarantineCauseMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want catalog.QuarantineCause
	}{
		{"catalog conflict", catalog.ErrConflict, catalog.QuarantineCauseConflict},
		{"prepared recovery", catalog.ErrMutationRecoveryRequired, catalog.QuarantineCauseConflict},
		{"integrity", catalog.ErrIntegrity, catalog.QuarantineCauseIntegrity},
		{"invalid transition", catalog.ErrInvalidTransition, catalog.QuarantineCauseIntegrity},
		{"unsettled worker", supervise.ErrUnsettledGroup, catalog.QuarantineCauseUnsettled},
		{"unavailable", errors.New("offline"), catalog.QuarantineCauseUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := quarantineCause(test.err); got != test.want {
				t.Fatalf("quarantineCause(%v) = %d, want %d", test.err, got, test.want)
			}
		})
	}
}

func TestPrepareTenantLatestRevisionWinsAndCoalesces(t *testing.T) {
	workers := newFakeWorkers()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var first sync.Once
	store, spec := newStoreAndSpec(t, 1)
	observed := &controlledVerificationStore{Store: store}
	var calls atomic.Int32
	observed.verify = func(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, revision catalog.Revision) error {
		calls.Add(1)
		blocked := false
		first.Do(func() {
			blocked = true
			close(firstStarted)
		})
		if blocked {
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return store.VerifyMaterialization(ctx, tenant, generation, revision)
	}
	runtime := newProvisionedRuntime(t, observed, workers, fakePlanner{}, spec)

	const callers = 1000
	results := make(chan prepareResult, callers)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 1)
		results <- prepareResult{state: state, err: err}
	}()
	select {
	case <-firstStarted:
	case <-time.After(testTimeout):
		t.Fatal("first revision did not start")
	}
	for revision := 2; revision <= callers; revision++ {
		revision := catalog.Revision(revision)
		go func() {
			state, err := runtime.PrepareTenant(context.Background(), spec.ID, revision)
			results <- prepareResult{state: state, err: err}
		}()
	}
	deadline := time.Now().Add(testTimeout)
	for {
		status, err := runtime.State(context.Background(), spec.OwnerID, spec.ID)
		if err != nil {
			t.Fatalf("State: %v", err)
		}
		state := status.State
		if state.Desired == callers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("desired revision stopped at %d", state.Desired)
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseFirst)
	for range callers {
		result := <-results
		if result.err != nil {
			t.Fatalf("PrepareTenant: %v", result.err)
		}
		if !result.state.Prepared() || result.state.Applied != callers {
			t.Fatalf("unexpected coalesced proof: %+v", result.state)
		}
	}
	if got := calls.Load(); got < 2 || got > 3 {
		t.Fatalf("materialization verification count = %d, want bounded initial/superseded/latest work", got)
	}
	workerCalls, _, _, _, _, _ := workers.snapshot()
	if len(workerCalls) != 0 {
		t.Fatalf("coalesced verification used auxiliary tenant workers: %v", workerCalls)
	}
	closeRuntime(t, runtime)
}

func TestCancelOneOfTwoWaitersKeepsSharedWork(t *testing.T) {
	workers := newFakeWorkers()
	started := make(chan struct{})
	release := make(chan struct{})
	store, spec := newStoreAndSpec(t, 1)
	var startOnce sync.Once
	observed := &controlledVerificationStore{Store: store}
	observed.verify = func(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, revision catalog.Revision) error {
		startOnce.Do(func() { close(started) })
		select {
		case <-release:
			return store.VerifyMaterialization(ctx, tenant, generation, revision)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	runtime := newProvisionedRuntime(t, observed, workers, fakePlanner{}, spec)
	actor := runtime.tenants[spec.ID].actor
	first := make(chan prepareResult, 1)
	second := make(chan prepareResult, 1)
	if err := actor.send(context.Background(), prepareRequest{id: 1, revision: 2, response: first}); err != nil {
		t.Fatalf("send first prepare: %v", err)
	}
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("shared verification did not start")
	}
	if err := actor.send(context.Background(), prepareRequest{id: 2, revision: 2, response: second}); err != nil {
		t.Fatalf("send second prepare: %v", err)
	}
	actor.cancelWaiter(1)
	close(release)
	select {
	case result := <-second:
		if result.err != nil || !result.state.Prepared() {
			t.Fatalf("second waiter result: %+v", result)
		}
	case <-time.After(testTimeout):
		t.Fatal("shared work did not complete")
	}
	select {
	case result := <-first:
		t.Fatalf("canceled waiter received result: %+v", result)
	default:
	}
	closeRuntime(t, runtime)
}

func TestCancelAllWaitersStopsWorkWithoutQuarantine(t *testing.T) {
	workers := newFakeWorkers()
	started := make(chan struct{})
	canceled := make(chan struct{})
	var first sync.Once
	store, spec := newStoreAndSpec(t, 1)
	observed := &controlledVerificationStore{Store: store}
	observed.verify = func(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, revision catalog.Revision) error {
		blocked := false
		first.Do(func() {
			blocked = true
			close(started)
		})
		if !blocked {
			return store.VerifyMaterialization(ctx, tenant, generation, revision)
		}
		<-ctx.Done()
		close(canceled)
		return ctx.Err()
	}
	runtime := newProvisionedRuntime(t, observed, workers, fakePlanner{}, spec)
	actor := runtime.tenants[spec.ID].actor
	response := make(chan prepareResult, 1)
	if err := actor.send(context.Background(), prepareRequest{id: 1, revision: 4, response: response}); err != nil {
		t.Fatalf("send prepare: %v", err)
	}
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("materialization verification did not start")
	}
	actor.cancelWaiter(1)
	select {
	case <-canceled:
	case <-time.After(testTimeout):
		t.Fatal("last waiter cancellation did not cancel work")
	}
	deadline := time.Now().Add(testTimeout)
	for {
		status, err := runtime.State(context.Background(), spec.OwnerID, spec.ID)
		if err != nil {
			t.Fatalf("State: %v", err)
		}
		state := status.State
		calls, active, _, _, _, _ := workers.snapshot()
		if active == 0 && len(calls) == 0 {
			if state.Quarantine != nil {
				t.Fatalf("waiter cancellation quarantined lane: %+v", state.Quarantine)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("canceled verification did not settle: state=%+v calls=%v active=%d", state, calls, active)
		}
		time.Sleep(time.Millisecond)
	}
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 4)
	if err != nil || !state.Prepared() {
		t.Fatalf("retry after all waiters canceled: state=%+v err=%v", state, err)
	}
	closeRuntime(t, runtime)
}

func TestUnavailableLaneQuarantinesAndExplicitRetryClears(t *testing.T) {
	workers := newFakeWorkers()
	store, spec := newStoreAndSpec(t, 1)
	var fail sync.Once
	observed := &controlledVerificationStore{Store: store}
	observed.verify = func(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, revision catalog.Revision) error {
		var err error
		fail.Do(func() { err = errors.New("materializer unavailable") })
		if err != nil {
			return err
		}
		return store.VerifyMaterialization(ctx, tenant, generation, revision)
	}
	runtime := newProvisionedRuntime(t, observed, workers, fakePlanner{}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 5)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) {
		t.Fatalf("PrepareTenant error = %v, want QuarantinedError", err)
	}
	if state.Quarantine == nil || state.Quarantine.Lane != LaneMaterialization || state.Quarantine.Cause != catalog.QuarantineCauseUnavailable {
		t.Fatalf("unexpected quarantine: %+v", state.Quarantine)
	}
	state, err = runtime.PrepareTenant(context.Background(), spec.ID, 5)
	if err != nil || !state.Prepared() || state.Quarantine != nil {
		t.Fatalf("explicit retry: state=%+v err=%v", state, err)
	}
	closeRuntime(t, runtime)
}

func TestUnsettledLaneRequiresWorkerRecoveryAcrossRestart(t *testing.T) {
	workers := newFakeWorkers()
	store, spec := newStoreAndSpec(t, 1)
	observed := &controlledVerificationStore{Store: store}
	observed.verify = func(context.Context, catalog.TenantID, catalog.Generation, catalog.Revision) error {
		return supervise.ErrUnsettledGroup
	}
	runtime := newProvisionedRuntime(t, observed, workers, fakePlanner{}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 6)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) {
		t.Fatalf("PrepareTenant error = %v, want QuarantinedError", err)
	}
	if state.Quarantine == nil || state.Quarantine.Lane != LaneMaterialization || state.Quarantine.Cause != catalog.QuarantineCauseUnsettled {
		t.Fatalf("unexpected quarantine: %+v", state.Quarantine)
	}
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 6); !errors.As(err, &quarantined) {
		t.Fatalf("unsettled retry error = %v, want durable QuarantinedError", err)
	}
	closeRuntime(t, runtime)

	recoveredWorkers := newFakeWorkers()
	restarted := newProvisionedRuntime(t, store, recoveredWorkers, fakePlanner{}, spec)
	if _, err := restarted.PrepareTenant(context.Background(), spec.ID, 6); !errors.As(err, &quarantined) {
		t.Fatalf("restart erased unsettled quarantine: %v", err)
	}
	if err := restarted.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	state, err = restarted.PrepareTenant(context.Background(), spec.ID, 6)
	if err != nil || !state.Prepared() || state.Quarantine != nil {
		t.Fatalf("PrepareTenant after recovery: state=%+v err=%v", state, err)
	}
	closeRuntime(t, restarted)
}

func TestNewGenerationResetsSettledConvergence(t *testing.T) {
	workers := newFakeWorkers()
	store, spec, runtime := newFixture(t, workers, 1)
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 8); err != nil {
		t.Fatalf("PrepareTenant generation 1: %v", err)
	}
	closeRuntime(t, runtime)

	nextWorkers := newFakeWorkers()
	spec.Generation = 2
	if _, err := store.ReplaceTenantProvision(t.Context(), 1, tenantProvision(spec)); err != nil {
		t.Fatalf("ReplaceTenantProvision generation 2: %v", err)
	}
	testStore := &runtimeTestStore{Store: store, head: runtimeFixtureSyntheticHead}
	restarted, err := NewRuntime(t.Context(), testStore, nextWorkers, fakePlanner{}, newFakeFleetTransitions(), mustRuntimeTenantProvisions(t, store))
	if err != nil {
		t.Fatalf("NewRuntime generation 2: %v", err)
	}
	state, err := restarted.PrepareTenant(context.Background(), spec.ID, 2)
	if err != nil || !state.Prepared() || state.Generation != 2 || state.Desired != 2 {
		t.Fatalf("generation reset proof: state=%+v err=%v", state, err)
	}
	closeRuntime(t, restarted)
}

func TestCloseDrainsAdmittedWorkBeforeClosingWorkers(t *testing.T) {
	workers := newFakeWorkers()
	started := make(chan struct{})
	release := make(chan struct{})
	canceled := make(chan struct{}, 1)
	var first sync.Once
	store, spec := newStoreAndSpec(t, 1)
	observed := &controlledVerificationStore{Store: store}
	observed.verify = func(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, revision catalog.Revision) error {
		blocked := false
		first.Do(func() {
			blocked = true
			close(started)
		})
		if !blocked {
			return store.VerifyMaterialization(ctx, tenant, generation, revision)
		}
		select {
		case <-release:
			return store.VerifyMaterialization(ctx, tenant, generation, revision)
		case <-ctx.Done():
			canceled <- struct{}{}
			return ctx.Err()
		}
	}
	runtime := newProvisionedRuntime(t, observed, workers, fakePlanner{}, spec)
	result := make(chan prepareResult, 1)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 9)
		result <- prepareResult{state: state, err: err}
	}()
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("admitted verification did not start")
	}
	runtime.Close()
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 10); !errors.Is(err, ErrClosed) {
		t.Fatalf("PrepareTenant after Close error = %v, want ErrClosed", err)
	}
	_, _, closeCalls, cancelCalls, closed, _ := workers.snapshot()
	if closed || closeCalls != 0 || cancelCalls != 0 {
		t.Fatalf("Close touched workers before admitted work settled: closed=%t close=%d cancel=%d", closed, closeCalls, cancelCalls)
	}
	select {
	case <-canceled:
		t.Fatal("Close canceled admitted work")
	default:
	}
	close(release)
	prepared := <-result
	if prepared.err != nil || !prepared.state.Prepared() {
		t.Fatalf("admitted work result: %+v", prepared)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := runtime.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	calls, active, closeCalls, cancelCalls, closed, _ := workers.snapshot()
	if len(calls) != 0 || active != 0 || closed || closeCalls != 0 || cancelCalls != 0 {
		t.Fatalf("graceful close state: calls=%v active=%d closed=%t close=%d cancel=%d", calls, active, closed, closeCalls, cancelCalls)
	}
}

func TestCancelStopsActiveActorWorkWithoutTouchingSharedWorkers(t *testing.T) {
	workers := newFakeWorkers()
	started := make(chan struct{})
	store, spec := newStoreAndSpec(t, 1)
	var first sync.Once
	observed := &controlledVerificationStore{Store: store}
	observed.verify = func(ctx context.Context, _ catalog.TenantID, _ catalog.Generation, _ catalog.Revision) error {
		first.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	}
	runtime := newProvisionedRuntime(t, observed, workers, fakePlanner{}, spec)
	result := make(chan error, 1)
	go func() {
		_, err := runtime.PrepareTenant(context.Background(), spec.ID, 10)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("active verification did not start")
	}
	runtime.Cancel()
	if err := <-result; !errors.Is(err, ErrCanceled) {
		t.Fatalf("PrepareTenant error = %v, want ErrCanceled", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := runtime.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	_, active, closeCalls, cancelCalls, closed, canceled := workers.snapshot()
	if active != 0 || closed || canceled || closeCalls != 0 || cancelCalls != 0 {
		t.Fatalf("cancel state: active=%d closed=%t canceled=%t close=%d cancel=%d", active, closed, canceled, closeCalls, cancelCalls)
	}
}

func TestRepeatedRuntimeLifecycleLeavesNoOwnedWork(t *testing.T) {
	for cycle := range 25 {
		workers := newFakeWorkers()
		_, spec, runtime := newFixture(t, workers, 1)
		if _, err := runtime.PrepareTenant(context.Background(), spec.ID, catalog.Revision(cycle+1)); err != nil {
			t.Fatalf("cycle %d PrepareTenant: %v", cycle, err)
		}
		closeRuntime(t, runtime)
		_, active, closeCalls, cancelCalls, closed, _ := workers.snapshot()
		if active != 0 || closed || closeCalls != 0 || cancelCalls != 0 {
			t.Fatalf("cycle %d leaked ownership: active=%d closed=%t close=%d cancel=%d", cycle, active, closed, closeCalls, cancelCalls)
		}
	}
}

func TestReplaceAndRemoveFenceAuthorityFleetAroundCatalogCommit(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name := "replace"
		if remove {
			name = "remove"
		}
		t.Run(name, func(t *testing.T) {
			store, spec := newStoreAndSpec(t, 1)
			if _, err := store.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
				t.Fatal(err)
			}
			hook := newFakeFleetTransitions()
			next := spec
			next.Generation = 2
			hook.prepare = func(_ context.Context, change FleetTransition) error {
				provisions, err := allRuntimeTenantProvisions(t, store)
				if err != nil || len(provisions) != 1 || provisions[0].Generation != 1 || len(change.Drained) != 0 {
					t.Fatalf("pre-persist fleet=%+v provisions=%+v err=%v", change, provisions, err)
				}
				return nil
			}
			hook.commit = func(_ context.Context, change FleetTransition) error {
				provisions, err := allRuntimeTenantProvisions(t, store)
				want := 0
				if !remove {
					want = 1
				}
				if err != nil || len(provisions) != want || len(change.Committed) != want {
					t.Fatalf("post-persist fleet=%+v provisions=%+v err=%v", change, provisions, err)
				}
				if !remove && provisions[0].Generation != 2 {
					t.Fatalf("replacement generation = %d", provisions[0].Generation)
				}
				return nil
			}
			runtime, err := NewRuntime(t.Context(), store, newFakeWorkers(), fakePlanner{}, hook, mustRuntimeTenantProvisions(t, store))
			if err != nil {
				t.Fatal(err)
			}
			if remove {
				err = runtime.RemoveTenant(t.Context(), spec.ID, 1)
			} else {
				err = runtime.ReplaceTenant(t.Context(), 1, next)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(hook.events) != 2 || hook.events[0] != "prepare" || hook.events[1] != "commit" {
				t.Fatalf("hook events = %v", hook.events)
			}
			closeRuntime(t, runtime)
		})
	}
}

func TestFleetCommitFailureResumesExactDurableTransition(t *testing.T) {
	for _, kind := range []FleetTransitionKind{FleetProvision, FleetReplace, FleetRemove} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			store, spec := newStoreAndSpec(t, 1)
			if kind != FleetProvision {
				if _, err := store.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
					t.Fatal(err)
				}
			}
			hook := newFakeFleetTransitions()
			boom := errors.New("transient fleet commit")
			commitCalls := 0
			hook.commit = func(_ context.Context, change FleetTransition) error {
				commitCalls++
				if change.Kind != kind {
					t.Fatalf("commit kind = %v, want %v", change.Kind, kind)
				}
				if commitCalls == 1 {
					return boom
				}
				return nil
			}
			runtime, err := NewRuntime(t.Context(), store, newFakeWorkers(), fakePlanner{}, hook, mustRuntimeTenantProvisions(t, store))
			if err != nil {
				t.Fatal(err)
			}
			next := spec
			next.Generation = 2
			invoke := func() error {
				switch kind {
				case FleetProvision:
					return runtime.ProvisionTenant(t.Context(), spec)
				case FleetReplace:
					return runtime.ReplaceTenant(t.Context(), spec.Generation, next)
				case FleetRemove:
					return runtime.RemoveTenant(t.Context(), spec.ID, spec.Generation)
				default:
					panic("unknown transition")
				}
			}
			if err := invoke(); !errors.Is(err, boom) {
				t.Fatalf("first transition = %v, want %v", err, boom)
			}
			if _, err := runtime.AcquireGeneration(t.Context(), spec.ID, spec.Generation); !errors.Is(err, ErrTenantChanging) {
				t.Fatalf("admission while durable transition is unsettled = %v, want ErrTenantChanging", err)
			}
			if err := invoke(); err != nil {
				t.Fatalf("exact retry: %v", err)
			}
			if commitCalls != 2 {
				t.Fatalf("commit calls = %d, want 2", commitCalls)
			}
			switch kind {
			case FleetProvision:
				lease, err := runtime.AcquireGeneration(t.Context(), spec.ID, spec.Generation)
				if err != nil {
					t.Fatalf("provisioned generation unavailable: %v", err)
				}
				lease.Release()
			case FleetReplace:
				lease, err := runtime.AcquireGeneration(t.Context(), spec.ID, next.Generation)
				if err != nil {
					t.Fatalf("replacement generation unavailable: %v", err)
				}
				lease.Release()
			case FleetRemove:
				if _, err := runtime.AcquireGeneration(t.Context(), spec.ID, spec.Generation); !errors.Is(err, ErrTenantNotFound) {
					t.Fatalf("removed generation admission = %v, want ErrTenantNotFound", err)
				}
			}
			closeRuntime(t, runtime)
		})
	}
}

func TestCloseRejectsImmediatelyAndCancelInterruptsFleetCommit(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	if _, err := store.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	hook := newFakeFleetTransitions()
	hook.commit = func(ctx context.Context, _ FleetTransition) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	runtime, err := NewRuntime(t.Context(), store, newFakeWorkers(), fakePlanner{}, hook, mustRuntimeTenantProvisions(t, store))
	if err != nil {
		t.Fatal(err)
	}
	next := spec
	next.Generation = 2
	result := make(chan error, 1)
	go func() { result <- runtime.ReplaceTenant(context.Background(), spec.Generation, next) }()
	select {
	case <-entered:
	case <-time.After(testTimeout):
		t.Fatal("fleet commit did not start")
	}
	closed := make(chan struct{})
	go func() {
		runtime.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close blocked behind fleet transition")
	}
	if _, err := runtime.AcquireGeneration(t.Context(), spec.ID, spec.Generation); !errors.Is(err, ErrClosed) {
		t.Fatalf("admission after Close = %v, want ErrClosed", err)
	}
	runtime.Cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReplaceTenant after Cancel = %v, want context cancellation", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Cancel did not interrupt fleet commit")
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := runtime.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCallerDeadlineOnlyCancelsFleetTransitionBeforeDurablePersistence(t *testing.T) {
	t.Run("prepare", func(t *testing.T) {
		store, spec := newStoreAndSpec(t, 1)
		if _, err := store.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
			t.Fatal(err)
		}
		hook := newFakeFleetTransitions()
		hook.prepare = func(ctx context.Context, _ FleetTransition) error {
			<-ctx.Done()
			return ctx.Err()
		}
		runtime, err := NewRuntime(t.Context(), store, newFakeWorkers(), fakePlanner{}, hook, mustRuntimeTenantProvisions(t, store))
		if err != nil {
			t.Fatal(err)
		}
		next := spec
		next.Generation = 2
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := runtime.ReplaceTenant(ctx, spec.Generation, next); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ReplaceTenant = %v, want deadline", err)
		}
		lease, err := runtime.AcquireGeneration(t.Context(), spec.ID, spec.Generation)
		if err != nil {
			t.Fatalf("old generation did not reopen: %v", err)
		}
		lease.Release()
		closeRuntime(t, runtime)
	})

	t.Run("persist", func(t *testing.T) {
		store, spec := newStoreAndSpec(t, 1)
		if _, err := store.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
			t.Fatal(err)
		}
		started := make(chan struct{})
		release := make(chan struct{})
		blocking := blockingReplaceStore{Store: store, started: started, release: release}
		runtime, err := NewRuntime(t.Context(), blocking, newFakeWorkers(), fakePlanner{}, newFakeFleetTransitions(), mustRuntimeTenantProvisions(t, store))
		if err != nil {
			t.Fatal(err)
		}
		next := spec
		next.Generation = 2
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- runtime.ReplaceTenant(ctx, spec.Generation, next) }()
		<-started
		cancel()
		close(release)
		if err := <-result; err != nil {
			t.Fatalf("durable replacement after caller cancellation: %v", err)
		}
		closeRuntime(t, runtime)
	})

	t.Run("commit", func(t *testing.T) {
		store, spec := newStoreAndSpec(t, 1)
		if _, err := store.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
			t.Fatal(err)
		}
		started := make(chan struct{})
		release := make(chan struct{})
		hook := newFakeFleetTransitions()
		hook.commit = func(context.Context, FleetTransition) error {
			close(started)
			<-release
			return nil
		}
		runtime, err := NewRuntime(t.Context(), store, newFakeWorkers(), fakePlanner{}, hook, mustRuntimeTenantProvisions(t, store))
		if err != nil {
			t.Fatal(err)
		}
		next := spec
		next.Generation = 2
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- runtime.ReplaceTenant(ctx, spec.Generation, next) }()
		<-started
		cancel()
		close(release)
		if err := <-result; err != nil {
			t.Fatalf("durable fleet commit after caller cancellation: %v", err)
		}
		closeRuntime(t, runtime)
	})
}

func TestReplaceStoreFailureRestoresFleetBeforeAdmissionsReopen(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	if _, err := store.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("replace store failed")
	failing := failingFleetStore{Store: store, replaceErr: boom}
	hook := newFakeFleetTransitions()
	hook.abort = func(_ context.Context, change FleetTransition) error {
		if len(change.Before) != 1 || change.Before[0] != spec {
			t.Fatalf("abort fleet = %+v", change)
		}
		return nil
	}
	runtime, err := NewRuntime(t.Context(), failing, newFakeWorkers(), fakePlanner{}, hook, mustRuntimeTenantProvisions(t, failing.Store))
	if err != nil {
		t.Fatal(err)
	}
	next := spec
	next.Generation = 2
	if err := runtime.ReplaceTenant(t.Context(), 1, next); !errors.Is(err, boom) {
		t.Fatalf("ReplaceTenant = %v, want store failure", err)
	}
	if len(hook.events) != 2 || hook.events[0] != "prepare" || hook.events[1] != "abort" {
		t.Fatalf("hook events = %v", hook.events)
	}
	lease, err := runtime.AcquireGeneration(t.Context(), spec.ID, 1)
	if err != nil {
		t.Fatalf("old admissions did not reopen after exact restore: %v", err)
	}
	lease.Release()
	closeRuntime(t, runtime)
}

func allRuntimeTenantProvisions(t *testing.T, store Store) ([]catalog.TenantProvision, error) {
	t.Helper()
	topology, ok := store.(interface {
		TopologyHead(context.Context, catalog.SourceAuthorityFleetOwnerID) (catalog.TopologyHeadState, error)
		TopologySnapshot(context.Context, catalog.TopologySnapshotRequest) (catalog.TopologySnapshotPage, error)
	})
	if !ok {
		return nil, errors.New("tenant test store has no topology snapshot")
	}
	owner := catalog.SourceAuthorityFleetOwnerID("test-owner")
	head, err := topology.TopologyHead(t.Context(), owner)
	if err != nil {
		return nil, err
	}
	var provisions []catalog.TenantProvision
	var cursor catalog.TopologyCursor
	for {
		page, err := topology.TopologySnapshot(t.Context(), catalog.TopologySnapshotRequest{
			Owner: owner, Revision: head.Revision, Cursor: cursor, Limit: catalog.TopologyPageLimit,
		})
		if err != nil {
			return nil, err
		}
		provisions = append(provisions, page.Tenants...)
		if page.Next == (catalog.TopologyCursor{}) {
			return provisions, nil
		}
		cursor = page.Next
	}
}

func mustRuntimeTenantProvisions(t *testing.T, store Store) []catalog.TenantProvision {
	t.Helper()
	provisions, err := allRuntimeTenantProvisions(t, store)
	if err != nil {
		t.Fatal(err)
	}
	return provisions
}
