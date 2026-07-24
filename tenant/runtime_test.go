package tenant

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
)

const testTimeout = 5 * time.Second
const runtimeFixtureSyntheticHead catalog.Revision = 1 << 20

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

type fakePlanner struct {
	terminal *atomicMutationStore
}

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
		SourceResult: result,
	}, nil
}

func (p fakePlanner) ApplySourceMutation(ctx context.Context, _ SourceMutationStep, operation SourceMutationOperation, content SourceMutationContent) error {
	if content != nil {
		reader, err := content.Open(ctx)
		if err != nil {
			return errors.Join(err, content.Close())
		}
		_, copyErr := io.Copy(io.Discard, reader)
		settleErr := reader.Settle(copyErr)
		waitErr := reader.Wait(ctx)
		if err := errors.Join(copyErr, settleErr, waitErr, content.Close()); err != nil {
			return err
		}
	}
	if p.terminal != nil {
		p.terminal.commit(operation.OperationID)
	}
	return nil
}

func (fakePlanner) SourceMutationCommitted(context.Context, SourceMutationCommit) error { return nil }

type recordingSourcePlanner struct {
	fakePlanner
	steps chan SourceMutationStep
}

type atomicSourcePlanner struct {
	fakePlanner
	committed atomic.Int64
}

func (p *atomicSourcePlanner) SourceMutationCommitted(context.Context, SourceMutationCommit) error {
	p.committed.Add(1)
	return nil
}

type atomicMutationStore struct {
	Store
	committed sync.Map
}

func (s *atomicMutationStore) commit(id catalog.MutationID) {
	s.committed.Store(id, struct{}{})
}

func (s *atomicMutationStore) PreparedMutation(ctx context.Context, tenant catalog.TenantID, id catalog.MutationID) (catalog.PreparedMutation, error) {
	prepared, err := s.Store.PreparedMutation(ctx, tenant, id)
	if err == nil {
		if _, committed := s.committed.Load(id); committed {
			prepared.State = catalog.MutationCommitted
		}
	}
	return prepared, err
}

func (s *atomicMutationStore) PendingMutation(ctx context.Context, tenant catalog.TenantID) (*catalog.PreparedMutation, error) {
	prepared, err := s.Store.PendingMutation(ctx, tenant)
	if err != nil || prepared == nil {
		return prepared, err
	}
	if _, committed := s.committed.Load(prepared.OperationID); committed {
		return nil, nil
	}
	return prepared, nil
}

func (p recordingSourcePlanner) PrepareSourceMutation(ctx context.Context, step SourceMutationStep) (SourceMutationOperation, error) {
	p.steps <- step
	return p.fakePlanner.PrepareSourceMutation(ctx, step)
}

type mismatchedSourcePlanner struct{ fakePlanner }

func (mismatchedSourcePlanner) PrepareSourceMutation(_ context.Context, step SourceMutationStep) (SourceMutationOperation, error) {
	return SourceMutationOperation{
		OperationID: step.OperationID, SourceID: step.SourceID, SourceMetadata: "different-operation",
	}, nil
}

func newFixture(t *testing.T, generation catalog.Generation) (*catalog.Catalog, TenantSpec, *TenantRuntime) {
	t.Helper()
	store, spec := newStoreAndSpec(t, generation)
	runtime := newProvisionedRuntime(t, store, fakePlanner{}, spec)
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
		OwnerID: "test-owner",
		ID:      tenantID,
		Mount:   MountSpec{PresentationRoot: filepath.Join(dir, "presentation")},
		Backing: BackingSpec{Root: filepath.Join(dir, "backing")},
		Content: ContentSource{ID: "test-content"},
		Traits: TenantTraits{
			Access: ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: presentations,
		},
		FileProvider: FileProviderSpec{Enabled: true, PresentationInstanceID: "tenant-instance", DisplayName: "Tenant"},
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

func (s *runtimeTestStore) Head(ctx context.Context, tenant catalog.TenantID) (catalog.Revision, error) {
	if controlled, ok := s.Store.(interface {
		runtimeHead(context.Context, catalog.TenantID, catalog.Revision) (catalog.Revision, error)
	}); ok {
		return controlled.runtimeHead(ctx, tenant, s.head)
	}
	return s.head, nil
}

func newProvisionedRuntime(t *testing.T, store Store, planner Planner, spec TenantSpec) *TenantRuntime {
	t.Helper()
	testStore := &runtimeTestStore{Store: store, head: runtimeFixtureSyntheticHead}
	if _, err := testStore.ProvisionTenant(t.Context(), tenantProvision(spec)); err != nil {
		t.Fatalf("ProvisionTenant fixture: %v", err)
	}
	if catalogStore := runtimeFixtureCatalog(store); catalogStore != nil {
		activateRuntimeFixturePublication(t, catalogStore, spec)
	}
	runtime, err := NewRuntime(t.Context(), testStore, planner, newFakeFleetTransitions(), []catalog.TenantProvision{tenantProvision(spec)})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	return runtime
}

func runtimeFixtureCatalog(store Store) *catalog.Catalog {
	switch value := store.(type) {
	case *catalog.Catalog:
		return value
	case *atomicMutationStore:
		return runtimeFixtureCatalog(value.Store)
	case *contentOpenStore:
		return runtimeFixtureCatalog(value.Store)
	case *controlledHeadStore:
		return runtimeFixtureCatalog(value.Store)
	case *pendingOverrideStore:
		return value.Catalog
	default:
		return nil
	}
}

type runtimeFixtureFenceCheckpoint struct {
	Identity  string
	Cursor    uint64
	RootEpoch string
}

type runtimeFixtureFenceProof struct {
	Authority           causal.SourceAuthorityID
	AuthorityGeneration causal.Generation
	Streams             []runtimeFixtureFenceCheckpoint
	Inbox               uint64
	RootDigest          [sha256.Size]byte
	FleetDigest         [sha256.Size]byte
}

func activateRuntimeFixturePublication(t *testing.T, store *catalog.Catalog, spec TenantSpec) {
	t.Helper()
	state, err := store.TenantLifecycle(t.Context(), string(spec.OwnerID), spec.ID)
	if err != nil {
		t.Fatalf("TenantLifecycle source fixture: %v", err)
	}
	if state.Ready() && state.Activation.ActiveGeneration == spec.Generation {
		return
	}
	if _, err := store.EnsureTenantNamespace(t.Context(), catalog.EnsureTenantNamespaceRequest{
		OwnerID: string(spec.OwnerID), Tenant: spec.ID, Generation: spec.Generation,
		IntentRevision: state.Intent.Revision,
	}); err != nil {
		t.Fatalf("EnsureTenantNamespace source fixture: %v", err)
	}
	authority := causal.SourceAuthorityID(spec.Content.ID)
	owner := catalog.SourceAuthorityFleetOwnerID(spec.OwnerID)
	declaration := catalog.SourceAuthorityDeclaration{
		Authority: authority, DriverID: "tenant-fixture", DriverConfig: []byte("fixture"),
		DeclarationDigest: sha256.Sum256([]byte("tenant-fixture:" + authority)),
	}
	ensureRuntimeFixtureFleet(t, store, owner, declaration)
	publication, publicationDigest := publishRuntimeFixtureSnapshot(t, store, spec, authority, owner)
	mutation := func() catalog.TenantMutation {
		operation, err := catalog.NewTenantOperationID()
		if err != nil {
			t.Fatal(err)
		}
		return catalog.TenantMutation{
			OperationID: operation, HolderRuntimeGeneration: "tenant-runtime-fixture",
			OwnerID: string(spec.OwnerID), ExpectedIntentRevision: state.Intent.Revision,
		}
	}
	lease, state, err := store.StageApplication(t.Context(), catalog.StageApplicationRequest{
		Mutation: mutation(), Tenant: spec.ID, Generation: spec.Generation,
		Authority: authority, Publication: publication, PublicationDigest: publicationDigest,
	})
	if err != nil {
		t.Fatalf("StageApplication source fixture: %v", err)
	}
	for _, backend := range state.Target.RequiredBackends.Backends() {
		state, err = store.RecordPresentation(t.Context(), catalog.PresentationReceipt{
			Mutation: mutation(), Lease: lease, Backend: backend,
			BackendGeneration: fmt.Sprintf("tenant-fixture-%d", backend), ObservedRevision: lease.CatalogHead,
		})
		if err != nil {
			t.Fatalf("RecordPresentation source fixture: %v", err)
		}
	}
	targeting, err := store.TenantTargetingRevision(t.Context(), spec.ID)
	if err != nil {
		t.Fatalf("TenantTargetingRevision source fixture: %v", err)
	}
	activated, err := store.ActivateTenant(t.Context(), catalog.ActivateTenantRequest{
		Mutation: mutation(), Tenant: spec.ID, Generation: spec.Generation,
		ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
		ExpectedActivationRevision: state.Activation.Revision,
		ExpectedActiveGeneration:   state.Activation.ActiveGeneration,
		ExpectedTargetingRevision:  targeting,
		CausePublications:          []causal.OperationID{publication},
	})
	if err != nil {
		t.Fatalf("ActivateTenant source fixture: %v", err)
	}
	if !activated.State.Ready() {
		t.Fatalf("source fixture activation is not ready: %+v", activated.State)
	}
}

func ensureRuntimeFixtureFleet(
	t *testing.T,
	store *catalog.Catalog,
	owner catalog.SourceAuthorityFleetOwnerID,
	declaration catalog.SourceAuthorityDeclaration,
) {
	t.Helper()
	status, err := store.SourceAuthorityFleetHead(t.Context(), owner)
	if err == nil {
		if status.Current == nil || status.Current.Generation != 1 {
			t.Fatalf("source fixture fleet = %+v", status)
		}
		return
	}
	if !errors.Is(err, catalog.ErrNotFound) {
		t.Fatal(err)
	}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest([]causal.SourceAuthorityID{declaration.Authority})
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest([]catalog.SourceAuthorityDeclaration{declaration})
	if err != nil {
		t.Fatal(err)
	}
	stage, err := store.ReconcileSourceAuthorityFleet(t.Context(), catalog.SourceAuthorityFleetReconcileRequest{
		Owner: owner, Generation: 1, Declarations: []catalog.SourceAuthorityDeclaration{declaration},
		Complete: true, AuthorityCount: 1, AuthoritiesDigest: authoritiesDigest,
		DeclarationsDigest: declarationsDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcknowledgeSourceAuthorityFleet(t.Context(), catalog.SourceAuthorityFleetAcknowledgement{
		Owner: owner, Generation: 1, AuthorityCount: 1, AuthoritiesDigest: authoritiesDigest,
		DeclarationsDigest: declarationsDigest, StageDigest: stage.StageDigest,
	}); err != nil {
		t.Fatal(err)
	}
}

func publishRuntimeFixtureSnapshot(
	t *testing.T,
	store *catalog.Catalog,
	spec TenantSpec,
	authority causal.SourceAuthorityID,
	owner catalog.SourceAuthorityFleetOwnerID,
) (causal.OperationID, [sha256.Size]byte) {
	t.Helper()
	root := catalog.SourceObserverRootRecord{
		ID: "root", Generation: 1, Path: spec.Backing.Root, VolumeUUID: "tenant-fixture",
		Inode: 1, Kind: 2,
	}
	checkpoint := catalog.SourceObserverCheckpointRecord{Stream: "stream", RootEpoch: "epoch"}
	rootsDigest, err := catalog.SourceObserverRootsDigest([]catalog.SourceObserverRootRecord{root})
	if err != nil {
		t.Fatal(err)
	}
	checkpointsDigest, err := catalog.SourceObserverCheckpointsDigest([]catalog.SourceObserverCheckpointRecord{checkpoint})
	if err != nil {
		t.Fatal(err)
	}
	configurationOperation := runtimeFixtureOperation(t)
	configuration := catalog.SourceObserverConfigurationIdentity{
		Authority: authority, FleetOwner: owner, FleetGeneration: 1,
		Operation: configurationOperation, Stream: checkpoint.Stream, RootEpoch: checkpoint.RootEpoch,
		RootDigest: [32]byte{1}, FleetDigest: [32]byte{2}, RootCount: 1, CheckpointCount: 1,
		RootsDigest: rootsDigest, CheckpointsDigest: checkpointsDigest,
	}
	if err := store.BeginSourceObserverConfiguration(t.Context(), configuration); err != nil {
		t.Fatal(err)
	}
	ref, err := store.AppendSourceObserverConfigurationRoots(t.Context(), authority, configurationOperation,
		catalog.SourceObserverRootAppendPage{Records: []catalog.SourceObserverRootRecord{root}})
	if err != nil {
		t.Fatal(err)
	}
	ref, err = store.AppendSourceObserverConfigurationCheckpoints(t.Context(), authority, configurationOperation,
		catalog.SourceObserverCheckpointAppendPage{Sequence: ref.Sequence, Records: []catalog.SourceObserverCheckpointRecord{checkpoint}})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := store.CommitSourceObserverConfiguration(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	operation := runtimeFixtureOperation(t)
	snapshot := fmt.Sprintf("tenant-fixture-%x", operation)
	if err := store.BeginSourceSnapshotStage(t.Context(), authority, snapshot); err != nil {
		t.Fatal(err)
	}
	proof := runtimeFixtureFenceProof{
		Authority: authority, AuthorityGeneration: 1,
		Streams:    []runtimeFixtureFenceCheckpoint{{Identity: checkpoint.Stream, RootEpoch: checkpoint.RootEpoch}},
		RootDigest: stream.RootDigest, FleetDigest: stream.FleetDigest,
	}
	encoded, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	identity := catalog.SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1, Snapshot: snapshot,
		FenceDigest: sha256.Sum256(encoded),
		Change: causal.ChangeSet{
			SourceAuthority: authority, SourceRevision: 1,
			ChangeID: causal.ChangeID(runtimeFixtureOperation(t)), OperationID: operation,
			Cause: causal.CauseBootstrap,
		},
	}
	if err := store.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	rootKey, err := catalog.DeriveSourceDriverRootKey(authority, spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshotRef, err := store.AppendSourceSnapshotPublication(t.Context(), identity, catalog.SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"tenant-fixture"},
		Roots:        []catalog.SourceSnapshotRoot{{Tenant: spec.ID, Generation: spec.Generation, LogicalID: "root", RootKey: rootKey}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PromoteSourceSnapshot(t.Context(), snapshotRef, catalog.SourceSnapshotSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: authority, Stream: checkpoint.Stream, RootEpoch: checkpoint.RootEpoch,
			Operation: snapshotRef.Operation,
		},
		Snapshot: snapshotRef,
	}); err != nil {
		t.Fatal(err)
	}
	return snapshotRef.Operation, snapshotRef.Digest
}

func runtimeFixtureOperation(t *testing.T) causal.OperationID {
	t.Helper()
	operation, err := catalog.NewTenantOperationID()
	if err != nil {
		t.Fatal(err)
	}
	return causal.OperationID(operation)
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
	Store
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
	content, err := s.Store.OpenMutationContent(ctx, tenant, id)
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

type countingHeadStore struct {
	Store
	calls atomic.Int32
	head  catalog.Revision
}

type controlledHeadStore struct {
	Store
	head func(context.Context, catalog.TenantID, catalog.Revision) (catalog.Revision, error)
}

type blockingProvisionStore struct {
	Store
	persisted chan catalog.TenantProvision
	release   chan struct{}
}

func (s *blockingProvisionStore) ProvisionTenant(ctx context.Context, provision catalog.TenantProvision) (catalog.TenantProvision, error) {
	persisted, err := s.Store.ProvisionTenant(ctx, provision)
	if err != nil {
		return catalog.TenantProvision{}, err
	}
	s.persisted <- persisted
	select {
	case <-s.release:
		return persisted, nil
	case <-ctx.Done():
		return catalog.TenantProvision{}, ctx.Err()
	}
}

func (s *controlledHeadStore) runtimeHead(
	ctx context.Context,
	tenant catalog.TenantID,
	fallback catalog.Revision,
) (catalog.Revision, error) {
	if s.head != nil {
		return s.head(ctx, tenant, fallback)
	}
	return fallback, nil
}

func (s *countingHeadStore) Head(
	ctx context.Context,
	tenant catalog.TenantID,
) (catalog.Revision, error) {
	s.calls.Add(1)
	return s.head, nil
}

func TestPrepareTenantRevalidatesLogicallyAppliedSameRevisionOnDemand(t *testing.T) {
	store, spec, bootstrap := newFixture(t, 1)
	closeRuntime(t, bootstrap)
	record, err := store.LoadTenantState(t.Context(), spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	record.Verified = 0
	if _, err := store.SaveTenantState(t.Context(), record.Version, record); err != nil {
		t.Fatal(err)
	}
	observed := &countingHeadStore{Store: store, head: runtimeFixtureSyntheticHead}
	runtime, err := NewRuntime(t.Context(), observed, fakePlanner{}, newFakeFleetTransitions(), []catalog.TenantProvision{tenantProvision(spec)})
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
		t.Fatalf("catalog head calls = %d, want one on-demand catch-up", calls)
	}
	closeRuntime(t, runtime)
}

func TestPrepareTenantReturnsExactConvergenceProof(t *testing.T) {
	_, spec, runtime := newFixture(t, 7)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil {
		t.Fatalf("PrepareTenant: %v", err)
	}
	if !state.Prepared() || state.Requested != 3 || state.Generation != 7 || state.Desired != 3 || state.Observed != 3 || state.Verified != 3 || state.Applied != 3 {
		t.Fatalf("unexpected proof: %+v", state)
	}
	closeRuntime(t, runtime)
}

func TestAlreadyPreparedAndNoDemandRevisionsRemainPrepared(t *testing.T) {
	store, spec, bootstrap := newFixture(t, 1)
	closeRuntime(t, bootstrap)
	runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, newFakeFleetTransitions(), []catalog.TenantProvision{tenantProvision(spec)})
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
	closeRuntime(t, runtime)
}

func TestPresentationActivationGenerationIsDurable(t *testing.T) {
	store, spec, bootstrap := newFixture(t, 1)
	closeRuntime(t, bootstrap)
	spec.Generation = 2
	if _, err := store.ReplaceTenantProvision(t.Context(), 1, tenantProvision(spec)); err != nil {
		t.Fatalf("ReplaceTenantProvision generation 2: %v", err)
	}
	runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, newFakeFleetTransitions(), nil)
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
	record, err := store.LoadTenantState(t.Context(), spec.ID)
	if err != nil || record.ActivatedGeneration != next.Generation {
		t.Fatalf("activation after replacement = %+v, %v", record, err)
	}
	closeRuntime(t, runtime)

	restarted, err := NewRuntime(t.Context(), store, fakePlanner{}, newFakeFleetTransitions(), mustRuntimeTenantProvisions(t, store))
	if err != nil {
		t.Fatalf("NewRuntime restart: %v", err)
	}
	record, err = store.LoadTenantState(t.Context(), spec.ID)
	if err != nil || record.ActivatedGeneration != next.Generation {
		t.Fatalf("durable activation after restart = %+v, %v", record, err)
	}
	closeRuntime(t, restarted)
}

func TestPrepareTenantRejectsRevisionAheadOfCatalog(t *testing.T) {
	store, spec, bootstrap := newFixture(t, 1)
	closeRuntime(t, bootstrap)
	runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, newFakeFleetTransitions(), []catalog.TenantProvision{tenantProvision(spec)})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, runtimeFixtureSyntheticHead+1)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) || state.Quarantine == nil || state.Quarantine.Lane != LaneCatalogMutation || state.Quarantine.Cause != catalog.QuarantineCauseIntegrity {
		t.Fatalf("ahead-of-catalog state=%+v err=%v", state, err)
	}
	closeRuntime(t, runtime)
}

func TestRuntimeStartsEmptyAndProvisioningIsExact(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, newFakeFleetTransitions(), nil)
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

func TestPrepareTenantWithoutAuthoritativePublicationFailsClosed(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, newFakeFleetTransitions(), nil)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := runtime.ProvisionTenant(t.Context(), spec); err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	state, err := runtime.PrepareTenant(t.Context(), spec.ID, 1)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) || state.Quarantine == nil || state.Quarantine.Lane != LaneCatalogMutation || state.Quarantine.Cause != catalog.QuarantineCauseUnavailable {
		t.Fatalf("PrepareTenant without publication state=%+v err=%v", state, err)
	}
	closeRuntime(t, runtime)
}

func TestTenantStateIsOwnerFencedAndDurableAcrossRestart(t *testing.T) {
	store, spec, runtime := newFixture(t, 5)
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

	restarted, err := NewRuntime(t.Context(), store, fakePlanner{}, newFakeFleetTransitions(), mustRuntimeTenantProvisions(t, store))
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
	blockedStore := &blockingProvisionStore{
		Store: store, persisted: make(chan catalog.TenantProvision, 1), release: make(chan struct{}),
	}
	runtime, err := NewRuntime(t.Context(), blockedStore, fakePlanner{}, newFakeFleetTransitions(), nil)
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
	<-blockedStore.persisted
	activateRuntimeFixturePublication(t, store, spec)
	close(blockedStore.release)
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
	runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, newFakeFleetTransitions(), nil)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	second := first
	second.ID = catalog.TenantID("tenant-other")
	second.Mount.PresentationRoot += "-other"
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

	recovered, err := NewRuntime(t.Context(), store, fakePlanner{}, newFakeFleetTransitions(), mustRuntimeTenantProvisions(t, store))
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
	started := make(chan struct{})
	release := make(chan struct{})
	store, spec := newStoreAndSpec(t, 1)
	var startOnce sync.Once
	observed := &controlledHeadStore{Store: store}
	observed.head = func(ctx context.Context, _ catalog.TenantID, fallback catalog.Revision) (catalog.Revision, error) {
		startOnce.Do(func() { close(started) })
		select {
		case <-release:
			return fallback, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	runtime := newProvisionedRuntime(t, observed, fakePlanner{}, spec)
	oldResult := make(chan prepareResult, 1)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 2)
		oldResult <- prepareResult{state: state, err: err}
	}()
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("old generation catalog proof did not start")
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
	closeRuntime(t, runtime)
}

func TestRemoveTenantDrainsActivePreparationWithoutDeletingData(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	store, spec := newStoreAndSpec(t, 1)
	var first sync.Once
	observed := &controlledHeadStore{Store: store}
	observed.head = func(ctx context.Context, _ catalog.TenantID, fallback catalog.Revision) (catalog.Revision, error) {
		first.Do(func() { close(started) })
		select {
		case <-release:
			return fallback, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	runtime := newProvisionedRuntime(t, observed, fakePlanner{}, spec)
	active := make(chan prepareResult, 1)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 2)
		active <- prepareResult{state: state, err: err}
	}()
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("active catalog proof did not start")
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
	closeRuntime(t, runtime)
}

func TestTenantGenerationFencesAndAddRemoveLoop(t *testing.T) {
	store, spec, bootstrap := newFixture(t, 1)
	closeRuntime(t, bootstrap)
	runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, newFakeFleetTransitions(), []catalog.TenantProvision{tenantProvision(spec)})
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
	closeRuntime(t, runtime)
}

func TestPreparedMutationReplaysBeforeOrdinaryLanes(t *testing.T) {
	store, spec, bootstrap := newFixture(t, 1)
	closeRuntime(t, bootstrap)
	atomicStore := &atomicMutationStore{Store: store}
	runtime := newProvisionedRuntime(t, atomicStore, fakePlanner{terminal: atomicStore}, spec)
	beginDirectoryMutation(t, store, spec.ID, "replayed")
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant: state=%+v err=%v", state, err)
	}
	pending, err := atomicStore.PendingMutation(context.Background(), spec.ID)
	if err != nil || pending != nil {
		t.Fatalf("PendingMutations after replay = %v, %v", pending, err)
	}
	closeRuntime(t, runtime)
}

func TestSourcePlannerReceivesOnlyAuthorityLocatorsAndPathFreeTenantIdentity(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	atomicStore := &atomicMutationStore{Store: store}
	planner := recordingSourcePlanner{fakePlanner: fakePlanner{terminal: atomicStore}, steps: make(chan SourceMutationStep, 1)}
	runtime := newProvisionedRuntime(t, atomicStore, planner, spec)
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
	store, spec, bootstrap := newFixture(t, 1)
	closeRuntime(t, bootstrap)
	body := strings.Repeat("streamed-content-", 1024)
	beginFileMutation(t, store, spec.ID, "streamed", body)
	atomicStore := &atomicMutationStore{Store: store}
	observed := &contentOpenStore{Store: atomicStore}
	runtime := newProvisionedRuntime(t, observed, fakePlanner{terminal: atomicStore}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant: state=%+v err=%v", state, err)
	}
	opens, states, content := observed.snapshot()
	if opens != 1 || fmt.Sprint(string(states)) != fmt.Sprint(string([]catalog.PreparedMutationState{catalog.MutationApplying})) {
		t.Fatalf("content opens=%d states=%v, want one applying claim", opens, states)
	}
	if content == nil {
		t.Fatal("catalog content file was not captured")
	}
	if _, err := content.Read(make([]byte, 1)); err == nil {
		t.Fatal("catalog content source remained readable after operation completion")
	}
	closeRuntime(t, runtime)
}

func TestMetadataSourceMutationNeverOpensContent(t *testing.T) {
	store, spec, bootstrap := newFixture(t, 1)
	closeRuntime(t, bootstrap)
	beginDirectoryMutation(t, store, spec.ID, "metadata-only")
	atomicStore := &atomicMutationStore{Store: store}
	observed := &contentOpenStore{Store: atomicStore}
	runtime := newProvisionedRuntime(t, observed, fakePlanner{terminal: atomicStore}, spec)
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 3); err != nil {
		t.Fatalf("PrepareTenant: %v", err)
	}
	opens, states, _ := observed.snapshot()
	if opens != 0 || len(states) != 0 {
		t.Fatalf("metadata mutation opened content %d times in states %v", opens, states)
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

func TestSourcePlannerMustMakeMutationTerminal(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	atomicStore := &atomicMutationStore{Store: store}
	planner := &atomicSourcePlanner{fakePlanner: fakePlanner{terminal: atomicStore}}
	runtime := newProvisionedRuntime(t, atomicStore, planner, spec)
	prepared := beginDirectoryMutation(t, store, spec.ID, "atomic-source")
	if _, err := runtime.PrepareTenant(t.Context(), spec.ID, prepared.ExpectedHead+1); err != nil {
		t.Fatalf("PrepareTenant: %v", err)
	}
	if calls := planner.committed.Load(); calls != 1 {
		t.Fatalf("SourceMutationCommitted calls = %d, want 1", calls)
	}
	mutation, err := atomicStore.PreparedMutation(t.Context(), spec.ID, prepared.OperationID)
	if err != nil || mutation.State != catalog.MutationCommitted {
		t.Fatalf("committed mutation = %+v, %v", mutation, err)
	}
	closeRuntime(t, runtime)
}

func TestApplyingMutationRequiresRecoveryBeforeReplay(t *testing.T) {
	store, spec, bootstrap := newFixture(t, 1)
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

	atomicStore := &atomicMutationStore{Store: store}
	runtime := newProvisionedRuntime(t, atomicStore, fakePlanner{terminal: atomicStore}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) {
		t.Fatalf("PrepareTenant before recovery = %v, want QuarantinedError", err)
	}
	if state.Quarantine == nil || state.Quarantine.Lane != LaneCatalogMutation || state.Quarantine.Cause != catalog.QuarantineCauseUnsettled {
		t.Fatalf("unexpected applying quarantine: %+v", state.Quarantine)
	}
	if err := runtime.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	state, err = runtime.PrepareTenant(context.Background(), spec.ID, 3)
	if err != nil || !state.Prepared() {
		t.Fatalf("PrepareTenant after recovery: state=%+v err=%v", state, err)
	}
	pending, err := atomicStore.PendingMutation(context.Background(), spec.ID)
	if err != nil || pending != nil {
		t.Fatalf("PendingMutations after recovered replay = %v, %v", pending, err)
	}
	closeRuntime(t, runtime)
}

func TestSourceOperationMustPreservePersistedOperationIdentity(t *testing.T) {
	store, spec, bootstrap := newFixture(t, 1)
	closeRuntime(t, bootstrap)
	prepared := beginDirectoryMutation(t, store, spec.ID, "identity-mismatch")
	runtime := newProvisionedRuntime(t, store, mismatchedSourcePlanner{}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 3)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) {
		t.Fatalf("PrepareTenant error = %v, want QuarantinedError", err)
	}
	if state.Quarantine == nil || state.Quarantine.Lane != LaneCatalogMutation || state.Quarantine.Cause != catalog.QuarantineCauseIntegrity {
		t.Fatalf("unexpected quarantine: %+v", state.Quarantine)
	}
	pending, err := store.PreparedMutation(context.Background(), prepared.Tenant, prepared.OperationID)
	if err != nil || pending.State != catalog.MutationApplying {
		t.Fatalf("prepared mutation after rejected operation = %+v, %v", pending, err)
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
		{"integrity", catalog.ErrIntegrity, catalog.QuarantineCauseIntegrity},
		{"invalid transition", catalog.ErrInvalidTransition, catalog.QuarantineCauseIntegrity},
		{"claimed mutation", catalog.ErrMutationClaimed, catalog.QuarantineCauseUnsettled},
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
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var first sync.Once
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(releaseFirst) }) })
	store, spec := newStoreAndSpec(t, 1)
	observed := &controlledHeadStore{Store: store}
	var calls atomic.Int32
	observed.head = func(ctx context.Context, _ catalog.TenantID, fallback catalog.Revision) (catalog.Revision, error) {
		calls.Add(1)
		blocked := false
		first.Do(func() {
			blocked = true
			close(firstStarted)
		})
		if blocked {
			<-releaseFirst
			if err := ctx.Err(); err != nil {
				return 0, ctx.Err()
			}
		}
		return fallback, nil
	}
	runtime := newProvisionedRuntime(t, observed, fakePlanner{}, spec)

	const callers = 1000
	type callerResult struct {
		requested catalog.Revision
		prepareResult
	}
	results := make(chan callerResult, callers)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 1)
		results <- callerResult{requested: 1, prepareResult: prepareResult{state: state, err: err}}
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
			results <- callerResult{requested: revision, prepareResult: prepareResult{state: state, err: err}}
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
	release.Do(func() { close(releaseFirst) })
	for range callers {
		result := <-results
		if result.err != nil {
			t.Fatalf("PrepareTenant: %v", result.err)
		}
		if result.state.Requested != result.requested || !result.state.Prepared() || result.state.Applied != callers {
			t.Fatalf("unexpected coalesced proof: %+v", result.state)
		}
	}
	final, err := runtime.State(context.Background(), spec.OwnerID, spec.ID)
	if err != nil {
		t.Fatalf("final State: %v", err)
	}
	if state := final.State; state.Desired != callers || state.Observed != callers ||
		state.Verified != callers || state.Applied != callers || state.Quarantine != nil {
		t.Fatalf("final latest revision = %+v", state)
	}
	if got := calls.Load(); got < 2 || got > 3 {
		t.Fatalf("catalog proof count = %d, want bounded initial/superseded/latest work", got)
	}
	closeRuntime(t, runtime)
}

func TestCancelOneOfTwoWaitersKeepsSharedWork(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	store, spec := newStoreAndSpec(t, 1)
	var startOnce sync.Once
	observed := &controlledHeadStore{Store: store}
	observed.head = func(ctx context.Context, _ catalog.TenantID, fallback catalog.Revision) (catalog.Revision, error) {
		startOnce.Do(func() { close(started) })
		select {
		case <-release:
			return fallback, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	runtime := newProvisionedRuntime(t, observed, fakePlanner{}, spec)
	actor := runtime.tenants[spec.ID].actor
	first := make(chan prepareResult, 1)
	second := make(chan prepareResult, 1)
	if err := actor.send(context.Background(), prepareRequest{id: 1, revision: 2, response: first}); err != nil {
		t.Fatalf("send first prepare: %v", err)
	}
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("shared catalog proof did not start")
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
	started := make(chan struct{})
	canceled := make(chan struct{})
	var first sync.Once
	store, spec := newStoreAndSpec(t, 1)
	observed := &controlledHeadStore{Store: store}
	observed.head = func(ctx context.Context, _ catalog.TenantID, fallback catalog.Revision) (catalog.Revision, error) {
		blocked := false
		first.Do(func() {
			blocked = true
			close(started)
		})
		if !blocked {
			return fallback, nil
		}
		<-ctx.Done()
		close(canceled)
		return 0, ctx.Err()
	}
	runtime := newProvisionedRuntime(t, observed, fakePlanner{}, spec)
	actor := runtime.tenants[spec.ID].actor
	response := make(chan prepareResult, 1)
	if err := actor.send(context.Background(), prepareRequest{id: 1, revision: 4, response: response}); err != nil {
		t.Fatalf("send prepare: %v", err)
	}
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("catalog proof did not start")
	}
	actor.cancelWaiter(1)
	select {
	case <-canceled:
	case <-time.After(testTimeout):
		t.Fatal("last waiter cancellation did not cancel work")
	}
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 4)
	if err != nil || !state.Prepared() {
		t.Fatalf("retry after all waiters canceled: state=%+v err=%v", state, err)
	}
	closeRuntime(t, runtime)
}

func TestUnavailableLaneQuarantinesAndExplicitRetryClears(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	var fail sync.Once
	observed := &controlledHeadStore{Store: store}
	observed.head = func(_ context.Context, _ catalog.TenantID, fallback catalog.Revision) (catalog.Revision, error) {
		var err error
		fail.Do(func() { err = errors.New("catalog unavailable") })
		if err != nil {
			return 0, err
		}
		return fallback, nil
	}
	runtime := newProvisionedRuntime(t, observed, fakePlanner{}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 5)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) {
		t.Fatalf("PrepareTenant error = %v, want QuarantinedError", err)
	}
	if state.Quarantine == nil || state.Quarantine.Lane != LaneCatalogMutation || state.Quarantine.Cause != catalog.QuarantineCauseUnavailable {
		t.Fatalf("unexpected quarantine: %+v", state.Quarantine)
	}
	state, err = runtime.PrepareTenant(context.Background(), spec.ID, 5)
	if err != nil || !state.Prepared() || state.Quarantine != nil {
		t.Fatalf("explicit retry: state=%+v err=%v", state, err)
	}
	closeRuntime(t, runtime)
}

func TestClaimedLaneRequiresRecoveryAcrossRestart(t *testing.T) {
	store, spec := newStoreAndSpec(t, 1)
	observed := &controlledHeadStore{Store: store}
	observed.head = func(context.Context, catalog.TenantID, catalog.Revision) (catalog.Revision, error) {
		return 0, catalog.ErrMutationClaimed
	}
	runtime := newProvisionedRuntime(t, observed, fakePlanner{}, spec)
	state, err := runtime.PrepareTenant(context.Background(), spec.ID, 6)
	var quarantined *QuarantinedError
	if !errors.As(err, &quarantined) {
		t.Fatalf("PrepareTenant error = %v, want QuarantinedError", err)
	}
	if state.Quarantine == nil || state.Quarantine.Lane != LaneCatalogMutation || state.Quarantine.Cause != catalog.QuarantineCauseUnsettled {
		t.Fatalf("unexpected quarantine: %+v", state.Quarantine)
	}
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 6); !errors.As(err, &quarantined) {
		t.Fatalf("unsettled retry error = %v, want durable QuarantinedError", err)
	}
	closeRuntime(t, runtime)

	restarted := newProvisionedRuntime(t, store, fakePlanner{}, spec)
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
	store, spec, runtime := newFixture(t, 1)
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 8); err != nil {
		t.Fatalf("PrepareTenant generation 1: %v", err)
	}
	closeRuntime(t, runtime)

	spec.Generation = 2
	if _, err := store.ReplaceTenantProvision(t.Context(), 1, tenantProvision(spec)); err != nil {
		t.Fatalf("ReplaceTenantProvision generation 2: %v", err)
	}
	testStore := &runtimeTestStore{Store: store, head: runtimeFixtureSyntheticHead}
	restarted, err := NewRuntime(t.Context(), testStore, fakePlanner{}, newFakeFleetTransitions(), mustRuntimeTenantProvisions(t, store))
	if err != nil {
		t.Fatalf("NewRuntime generation 2: %v", err)
	}
	state, err := restarted.PrepareTenant(context.Background(), spec.ID, 2)
	if err != nil || !state.Prepared() || state.Generation != 2 || state.Desired != 2 {
		t.Fatalf("generation reset proof: state=%+v err=%v", state, err)
	}
	closeRuntime(t, restarted)
}

func TestCloseDrainsAdmittedWork(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	canceled := make(chan struct{}, 1)
	var first sync.Once
	store, spec := newStoreAndSpec(t, 1)
	observed := &controlledHeadStore{Store: store}
	observed.head = func(ctx context.Context, _ catalog.TenantID, fallback catalog.Revision) (catalog.Revision, error) {
		blocked := false
		first.Do(func() {
			blocked = true
			close(started)
		})
		if !blocked {
			return fallback, nil
		}
		select {
		case <-release:
			return fallback, nil
		case <-ctx.Done():
			canceled <- struct{}{}
			return 0, ctx.Err()
		}
	}
	runtime := newProvisionedRuntime(t, observed, fakePlanner{}, spec)
	result := make(chan prepareResult, 1)
	go func() {
		state, err := runtime.PrepareTenant(context.Background(), spec.ID, 9)
		result <- prepareResult{state: state, err: err}
	}()
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("admitted catalog proof did not start")
	}
	runtime.Close()
	if _, err := runtime.PrepareTenant(context.Background(), spec.ID, 10); !errors.Is(err, ErrClosed) {
		t.Fatalf("PrepareTenant after Close error = %v, want ErrClosed", err)
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
}

func TestCancelStopsActiveActorWork(t *testing.T) {
	started := make(chan struct{})
	store, spec := newStoreAndSpec(t, 1)
	var first sync.Once
	observed := &controlledHeadStore{Store: store}
	observed.head = func(ctx context.Context, _ catalog.TenantID, _ catalog.Revision) (catalog.Revision, error) {
		first.Do(func() { close(started) })
		<-ctx.Done()
		return 0, ctx.Err()
	}
	runtime := newProvisionedRuntime(t, observed, fakePlanner{}, spec)
	result := make(chan error, 1)
	go func() {
		_, err := runtime.PrepareTenant(context.Background(), spec.ID, 10)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("active catalog proof did not start")
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
}

func TestRepeatedRuntimeLifecycleSettlesActors(t *testing.T) {
	for cycle := range 25 {
		_, spec, runtime := newFixture(t, 1)
		if _, err := runtime.PrepareTenant(context.Background(), spec.ID, catalog.Revision(cycle+1)); err != nil {
			t.Fatalf("cycle %d PrepareTenant: %v", cycle, err)
		}
		closeRuntime(t, runtime)
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
			runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, hook, mustRuntimeTenantProvisions(t, store))
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
			runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, hook, mustRuntimeTenantProvisions(t, store))
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
	runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, hook, mustRuntimeTenantProvisions(t, store))
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
		runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, hook, mustRuntimeTenantProvisions(t, store))
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
		runtime, err := NewRuntime(t.Context(), blocking, fakePlanner{}, newFakeFleetTransitions(), mustRuntimeTenantProvisions(t, store))
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
		runtime, err := NewRuntime(t.Context(), store, fakePlanner{}, hook, mustRuntimeTenantProvisions(t, store))
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
	runtime, err := NewRuntime(t.Context(), failing, fakePlanner{}, hook, mustRuntimeTenantProvisions(t, failing.Store))
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
