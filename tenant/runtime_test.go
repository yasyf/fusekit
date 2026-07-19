package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

const testTimeout = 5 * time.Second

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

func newFakeWorkers() *fakeWorkers {
	return &fakeWorkers{events: make(chan taskKey, 4096)}
}

func (w *fakeWorkers) Run(ctx context.Context, task supervise.Task) error {
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

func (fakePlanner) PrepareSourceMutation(_ context.Context, step SourceMutationStep) (SourceMutationWorker, error) {
	return SourceMutationWorker{
		OperationID: step.OperationID, SourceID: step.SourceID, SourceMetadata: step.SourceMetadata,
		Spec: workerSpecFor(step.Tenant, LaneCatalogMutation, step.ExpectedHead),
	}, nil
}

func (fakePlanner) PrepareMaterialization(_ context.Context, _ Catalog, step MaterializationStep) (WorkerSpec, error) {
	return workerSpecFor(step.Tenant, LaneMaterialization, step.Revision), nil
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

func (mismatchedSourcePlanner) PrepareSourceMutation(_ context.Context, step SourceMutationStep) (SourceMutationWorker, error) {
	return SourceMutationWorker{
		OperationID: step.OperationID, SourceID: step.SourceID, SourceMetadata: "different-operation",
		Spec: workerSpecFor(step.Tenant, LaneCatalogMutation, step.ExpectedHead),
	}, nil
}

type inputSourcePlanner struct {
	fakePlanner
}

func (inputSourcePlanner) PrepareSourceMutation(_ context.Context, step SourceMutationStep) (SourceMutationWorker, error) {
	spec := workerSpecFor(step.Tenant, LaneCatalogMutation, step.ExpectedHead)
	spec.Input = []byte("planner-owned-input")
	return SourceMutationWorker{
		OperationID: step.OperationID, SourceID: step.SourceID, SourceMetadata: step.SourceMetadata,
		Spec: spec,
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
	head   catalog.Revision
	demand bool
}

func (s *runtimeTestStore) Head(context.Context, catalog.TenantID) (catalog.Revision, error) {
	return s.head, nil
}

func (s *runtimeTestStore) HasMaterializationDemand(context.Context, catalog.TenantID) (bool, error) {
	return s.demand, nil
}

func newProvisionedRuntime(t *testing.T, store Store, workers WorkerPool, planner Planner, spec TenantSpec) *TenantRuntime {
	t.Helper()
	testStore := &runtimeTestStore{Store: store, head: catalog.Revision(^uint64(0)), demand: true}
	if _, err := testStore.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
		t.Fatalf("ProvisionTenant fixture: %v", err)
	}
	runtime, err := NewRuntime(t.Context(), testStore, workers, planner)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	return runtime
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
	id, err := catalog.NewMutationID()
	if err != nil {
		t.Fatalf("NewMutationID: %v", err)
	}
	head, err := store.Head(context.Background(), tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	prepared, err := store.BeginMutation(context.Background(), id, tenant, head, catalog.MutationIntent{
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
	id, err := catalog.NewMutationID()
	if err != nil {
		t.Fatalf("NewMutationID: %v", err)
	}
	head, err := store.Head(context.Background(), tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	prepared, err := store.BeginMutation(context.Background(), id, tenant, head, catalog.MutationIntent{
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
	last   *os.File
}

func (s *contentOpenStore) OpenMutationContent(ctx context.Context, id catalog.MutationID) (*os.File, error) {
	mutation, err := s.PreparedMutation(ctx, id)
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
	content, err := s.Catalog.OpenMutationContent(ctx, id)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.last = content
	s.mu.Unlock()
	return content, nil
}

func (s *contentOpenStore) snapshot() (int, []catalog.PreparedMutationState, *os.File) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens, append([]catalog.PreparedMutationState(nil), s.states...), s.last
}

type pendingOverrideStore struct {
	*catalog.Catalog
	pending []catalog.PreparedMutation
}

func (s *pendingOverrideStore) PendingMutations(context.Context, catalog.TenantID) ([]catalog.PreparedMutation, error) {
	return append([]catalog.PreparedMutation(nil), s.pending...), nil
}

func TestPrepareTenantRunsFixedLanesAndReturnsProof(t *testing.T) {
	workers := newFakeWorkers()
	_, spec, runtime := newFixture(t, workers, 7)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil {
		t.Fatalf("PrepareTenant: %v", err)
	}
	if !state.Prepared() || state.Requested != 3 || state.Generation != 7 || state.Desired != 3 || state.Observed != 3 || state.Verified != 3 || state.Applied != 3 {
		t.Fatalf("unexpected proof: %+v", state)
	}
	want := []taskKey{
		{LaneMaterialization, 3},
	}
	calls, _, _, _, _, _ := workers.snapshot()
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("worker calls = %v, want %v", calls, want)
	}
	closeRuntime(t, runtime)
}

func TestAlreadyPreparedAndNoDemandRevisionsLaunchNoWorkers(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	workers := newFakeWorkers()
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{})
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
	committed, err := store.CommitMutation(context.Background(), prepared.OperationID)
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
	runtime, err := NewRuntime(t.Context(), store, workers, mountPlanner{})
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
	restarted, err := NewRuntime(t.Context(), store, restartWorkers, mountPlanner{})
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
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{})
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
			workers := newFakeWorkers()
			workers.proofHook = test.write
			_, spec, runtime := newFixture(t, workers, 7)
			state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
			var quarantined *QuarantinedError
			if !errors.As(err, &quarantined) || state.Quarantine == nil || state.Quarantine.Lane != LaneMaterialization || state.Quarantine.Cause != catalog.QuarantineCauseIntegrity {
				t.Fatalf("invalid proof state=%+v err=%v", state, err)
			}
			if state.Verified >= 3 || state.Applied >= 3 {
				t.Fatalf("invalid proof advanced lane: %+v", state)
			}
			workers.mu.Lock()
			workers.proofHook = nil
			workers.mu.Unlock()
			state, err = runtime.PrepareTenant(context.Background(), spec.ID, 3)
			if err != nil || !state.Prepared() {
				t.Fatalf("retry with exact proof: state=%+v err=%v", state, err)
			}
			closeRuntime(t, runtime)
		})
	}
}

func TestWorkerProofDoesNotAdvanceBeforeProcessReap(t *testing.T) {
	workers := newFakeWorkers()
	release := make(chan struct{})
	workers.runHook = func(ctx context.Context, key taskKey) error {
		if key.lane != LaneMaterialization {
			return nil
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	_, spec, runtime := newFixture(t, workers, 9)
	result := make(chan prepareResult, 1)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
		result <- prepareResult{state: state, err: err}
	}()
	waitEvent(t, workers)
	state, err := runtime.State(context.Background(), spec.ID)
	if err != nil {
		t.Fatalf("State while worker remains unreaped: %v", err)
	}
	if state.Verified >= 3 || state.Applied >= 3 {
		t.Fatalf("proof advanced before worker reap: %+v", state)
	}
	close(release)
	prepared := <-result
	if prepared.err != nil || !prepared.state.Prepared() {
		t.Fatalf("PrepareTenant after reap: %+v", prepared)
	}
	closeRuntime(t, runtime)
}

func TestRuntimeStartsEmptyAndProvisioningIsExact(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	workers := newFakeWorkers()
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{})
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

func TestProvisionTenantLinearizesAgainstPrepare(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	workers := newFakeWorkers()
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{})
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
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{})
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
	recovered, err := NewRuntime(t.Context(), store, restarted, fakePlanner{})
	if err != nil {
		t.Fatalf("NewRuntime recovery: %v", err)
	}
	if specs := recovered.Specs(); len(specs) != 1 || specs[0] != next {
		t.Fatalf("recovered Specs = %+v, want [%+v]", specs, next)
	}
	if _, err := recovered.State(t.Context(), second.ID); !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("removed tenant recovered: %v", err)
	}
	provisions, err := store.TenantProvisions(t.Context())
	if err != nil || len(provisions) != 1 || provisionSpec(provisions[0]) != next {
		t.Fatalf("durable desired tenants = %+v, %v; want [%+v]", provisions, err, next)
	}
	closeRuntime(t, recovered)
}

func TestReplaceTenantDrainsOldWaiterBeforeGenerationSwap(t *testing.T) {
	workers := newFakeWorkers()
	release := make(chan struct{})
	workers.runHook = func(ctx context.Context, key taskKey) error {
		if key.lane != LaneMaterialization {
			return nil
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	_, spec, runtime := newFixture(t, workers, 1)
	oldResult := make(chan prepareResult, 1)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 2)
		oldResult <- prepareResult{state: state, err: err}
	}()
	waitEvent(t, workers)
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
	release := make(chan struct{})
	workers.runHook = func(ctx context.Context, key taskKey) error {
		if key.lane != LaneMaterialization {
			return nil
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	store, spec, runtime := newFixture(t, workers, 1)
	active := make(chan prepareResult, 1)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 2)
		active <- prepareResult{state: state, err: err}
	}()
	waitEvent(t, workers)
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
	runtime, err := NewRuntime(t.Context(), store, workers, fakePlanner{})
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

func TestCloseCancelsConcurrentRecoveryBeforeWorkerShutdown(t *testing.T) {
	workers := newFakeWorkers()
	started := make(chan struct{})
	workers.recoverHook = func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	_, spec, runtime := newFixture(t, workers, 1)
	recovered := make(chan error, 1)
	go func() { recovered <- runtime.Recover(context.Background()) }()
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("Recover did not reach worker pool")
	}
	if err := runtime.ProvisionTenant(context.Background(), spec); !errors.Is(err, ErrRecovering) {
		t.Fatalf("ProvisionTenant during recovery = %v, want ErrRecovering", err)
	}
	runtime.Close()
	if err := <-recovered; !errors.Is(err, context.Canceled) {
		t.Fatalf("Recover after Close = %v, want context cancellation", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := runtime.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	_, active, closeCalls, _, closed, _ := workers.snapshot()
	if active != 0 || !closed || closeCalls != 1 {
		t.Fatalf("worker shutdown active=%d closed=%v closeCalls=%d", active, closed, closeCalls)
	}
}

func TestPreparedMutationReplaysBeforeOrdinaryLanes(t *testing.T) {
	workers := newFakeWorkers()
	store, spec, runtime := newFixture(t, workers, 1)
	prepared := beginDirectoryMutation(t, store, spec.ID, "replayed")
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant: state=%+v err=%v", state, err)
	}
	calls, _, _, _, _, _ := workers.snapshot()
	want := []taskKey{
		{LaneCatalogMutation, prepared.ExpectedHead},
		{LaneMaterialization, 3},
	}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("worker calls = %v, want prepared mutation recovery before ordinary fragments %v", calls, want)
	}
	root, err := store.Root(context.Background(), spec.ID)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	if _, err := store.LookupName(context.Background(), spec.ID, catalog.PresentationFileProvider, root.ID, "replayed"); err != nil {
		t.Fatalf("LookupName(replayed): %v", err)
	}
	pending, err := store.PendingMutations(context.Background(), spec.ID)
	if err != nil || len(pending) != 0 {
		t.Fatalf("PendingMutations after replay = %v, %v", pending, err)
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
	if len(inputs) != 2 || string(inputs[0]) != body || inputs[1] != nil {
		t.Fatalf("worker stdin = %q, want exact source bytes then EOF lanes", inputs)
	}
	if content == nil {
		t.Fatal("catalog content file was not captured")
	}
	if _, err := content.Stat(); err == nil {
		t.Fatal("catalog content file remained open after worker completion")
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
	if len(inputs) != 2 || inputs[0] != nil {
		t.Fatalf("metadata source stdin = %v, want EOF", inputs)
	}
	closeRuntime(t, runtime)
}

func TestSourcePlannerCannotSupplyWorkerStdin(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	beginDirectoryMutation(t, store, spec.ID, "planner-stdin")
	workers := newFakeWorkers()
	runtime := newProvisionedRuntime(t, store, workers, inputSourcePlanner{}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) || state.Quarantine == nil || state.Quarantine.Cause != catalog.QuarantineCauseIntegrity {
		t.Fatalf("PrepareTenant planner stdin: state=%+v err=%v", state, err)
	}
	calls, _, _, _, _, _ := workers.snapshot()
	if len(calls) != 0 {
		t.Fatalf("worker ran with planner-owned stdin: %v", calls)
	}
	closeRuntime(t, runtime)
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
	want := []taskKey{{LaneMaterialization, 3}}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("worker calls = %v, want no repeated source apply %v", calls, want)
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
	want := []taskKey{
		{LaneCatalogMutation, prepared.ExpectedHead},
		{LaneMaterialization, 3},
	}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("worker calls = %v, want recovered source replay first %v", calls, want)
	}
	pending, err := store.PendingMutations(context.Background(), spec.ID)
	if err != nil || len(pending) != 0 {
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
	pending, err := store.PreparedMutation(context.Background(), prepared.OperationID)
	if err != nil || pending.State != catalog.MutationApplying {
		t.Fatalf("prepared mutation after rejected worker = %+v, %v", pending, err)
	}
	closeRuntime(t, runtime)
}

func TestRecoveryRequiredMutationQuarantinesBeforeWorkerAdmission(t *testing.T) {
	bootstrapWorkers := newFakeWorkers()
	store, spec, bootstrap := newFixture(t, bootstrapWorkers, 1)
	closeRuntime(t, bootstrap)
	id, err := catalog.NewMutationID()
	if err != nil {
		t.Fatalf("NewMutationID: %v", err)
	}
	workers := newFakeWorkers()
	override := &pendingOverrideStore{Catalog: store, pending: []catalog.PreparedMutation{{
		OperationID: id, Tenant: spec.ID, Kind: catalog.MutationCreate,
		State: catalog.MutationRecoveryRequired, ExpectedHead: 1,
	}}}
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
	workers.runHook = func(_ context.Context, _ taskKey) error {
		blocked := false
		first.Do(func() {
			blocked = true
			close(firstStarted)
		})
		if blocked {
			<-releaseFirst
		}
		return nil
	}
	_, spec, runtime := newFixture(t, workers, 1)

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
		state, err := runtime.State(context.Background(), spec.ID)
		if err != nil {
			t.Fatalf("State: %v", err)
		}
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
	calls, _, _, _, _, _ := workers.snapshot()
	if len(calls) != 2 {
		t.Fatalf("worker call count = %d, want one canceled fragment plus the latest materialization; calls=%v", len(calls), calls)
	}
	for _, call := range calls[1:] {
		if call.revision != callers {
			t.Fatalf("non-latest call after supersession: %v", calls)
		}
	}
	closeRuntime(t, runtime)
}

func TestCancelOneOfTwoWaitersKeepsSharedWork(t *testing.T) {
	workers := newFakeWorkers()
	release := make(chan struct{})
	workers.runHook = func(ctx context.Context, key taskKey) error {
		if key.lane != LaneMaterialization {
			return nil
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	_, spec, runtime := newFixture(t, workers, 1)
	actor := runtime.tenants[spec.ID].actor
	first := make(chan prepareResult, 1)
	second := make(chan prepareResult, 1)
	if err := actor.send(context.Background(), prepareRequest{id: 1, revision: 2, response: first}); err != nil {
		t.Fatalf("send first prepare: %v", err)
	}
	waitEvent(t, workers)
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
	canceled := make(chan struct{})
	var first sync.Once
	workers.runHook = func(ctx context.Context, key taskKey) error {
		blocked := false
		first.Do(func() { blocked = true })
		if !blocked {
			return nil
		}
		<-ctx.Done()
		close(canceled)
		return ctx.Err()
	}
	_, spec, runtime := newFixture(t, workers, 1)
	actor := runtime.tenants[spec.ID].actor
	response := make(chan prepareResult, 1)
	if err := actor.send(context.Background(), prepareRequest{id: 1, revision: 4, response: response}); err != nil {
		t.Fatalf("send prepare: %v", err)
	}
	waitEvent(t, workers)
	actor.cancelWaiter(1)
	select {
	case <-canceled:
	case <-time.After(testTimeout):
		t.Fatal("last waiter cancellation did not cancel work")
	}
	deadline := time.Now().Add(testTimeout)
	for {
		state, err := runtime.State(context.Background(), spec.ID)
		if err != nil {
			t.Fatalf("State: %v", err)
		}
		calls, active, _, _, _, _ := workers.snapshot()
		if active == 0 && len(calls) == 1 {
			if state.Quarantine != nil {
				t.Fatalf("waiter cancellation quarantined lane: %+v", state.Quarantine)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("canceled work did not settle: state=%+v calls=%v active=%d", state, calls, active)
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
	var fail sync.Once
	workers.runHook = func(_ context.Context, key taskKey) error {
		if key.lane != LaneMaterialization {
			return nil
		}
		var err error
		fail.Do(func() { err = errors.New("materializer unavailable") })
		return err
	}
	_, spec, runtime := newFixture(t, workers, 1)
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
	workers.runHook = func(_ context.Context, key taskKey) error {
		if key.lane == LaneMaterialization {
			return supervise.ErrUnsettledGroup
		}
		return nil
	}
	store, spec, runtime := newFixture(t, workers, 1)
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
	testStore := &runtimeTestStore{Store: store, head: catalog.Revision(^uint64(0)), demand: true}
	restarted, err := NewRuntime(t.Context(), testStore, nextWorkers, fakePlanner{})
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
	release := make(chan struct{})
	canceled := make(chan struct{}, 1)
	var first sync.Once
	workers.runHook = func(ctx context.Context, _ taskKey) error {
		blocked := false
		first.Do(func() { blocked = true })
		if !blocked {
			return nil
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			canceled <- struct{}{}
			return ctx.Err()
		}
	}
	_, spec, runtime := newFixture(t, workers, 1)
	result := make(chan prepareResult, 1)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 9)
		result <- prepareResult{state: state, err: err}
	}()
	waitEvent(t, workers)
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
	if len(calls) != 1 || active != 0 || !closed || closeCalls != 1 || cancelCalls != 0 {
		t.Fatalf("graceful close state: calls=%v active=%d closed=%t close=%d cancel=%d", calls, active, closed, closeCalls, cancelCalls)
	}
}

func TestCancelStopsActiveWorkAndJoinsWorkers(t *testing.T) {
	workers := newFakeWorkers()
	workers.runHook = func(ctx context.Context, _ taskKey) error {
		<-ctx.Done()
		return ctx.Err()
	}
	_, spec, runtime := newFixture(t, workers, 1)
	result := make(chan error, 1)
	go func() {
		_, err := runtime.PrepareTenant(context.Background(), spec.ID, 10)
		result <- err
	}()
	waitEvent(t, workers)
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
	if active != 0 || !closed || !canceled || closeCalls != 1 || cancelCalls != 1 {
		t.Fatalf("cancel state: active=%d closed=%t canceled=%t close=%d cancel=%d", active, closed, canceled, closeCalls, cancelCalls)
	}
}

func TestRecoverPausesAdmittedRequestsUntilRecoveryCompletes(t *testing.T) {
	workers := newFakeWorkers()
	recoverStarted := make(chan struct{})
	releaseRecover := make(chan struct{})
	workers.recoverHook = func(ctx context.Context) error {
		close(recoverStarted)
		select {
		case <-releaseRecover:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	_, spec, runtime := newFixture(t, workers, 1)
	recovered := make(chan error, 1)
	go func() { recovered <- runtime.Recover(context.Background()) }()
	select {
	case <-recoverStarted:
	case <-time.After(testTimeout):
		t.Fatal("worker recovery did not start")
	}
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 11); !errors.Is(err, ErrRecovering) {
		t.Fatalf("new PrepareTenant during recovery error = %v, want ErrRecovering", err)
	}

	actor := runtime.tenants[spec.ID].actor
	response := make(chan prepareResult, 1)
	if err := actor.send(context.Background(), prepareRequest{id: 1, revision: 11, response: response}); err != nil {
		t.Fatalf("send admitted prepare: %v", err)
	}
	select {
	case event := <-workers.events:
		t.Fatalf("admitted request started inside recovery barrier: %v", event)
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseRecover)
	if err := <-recovered; err != nil {
		t.Fatalf("Recover: %v", err)
	}
	select {
	case result := <-response:
		if result.err != nil || !result.state.Prepared() {
			t.Fatalf("admitted prepare after recovery: %+v", result)
		}
	case <-time.After(testTimeout):
		t.Fatal("admitted prepare did not resume")
	}
	closeRuntime(t, runtime)
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
		if active != 0 || !closed || closeCalls != 1 || cancelCalls != 0 {
			t.Fatalf("cycle %d leaked ownership: active=%d closed=%t close=%d cancel=%d", cycle, active, closed, closeCalls, cancelCalls)
		}
	}
}
