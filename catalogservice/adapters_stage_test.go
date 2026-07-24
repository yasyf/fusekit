package catalogservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/convergence"
	"github.com/yasyf/fusekit/tenant"
)

const stageRequestID catalogproto.MutationRequestID = "10000000000000000000000000000001"

func TestMutationStageAbortIsExactIdempotent(t *testing.T) {
	store, tenantID, root := mutationStageCatalog(t)
	adapter := mutationStageAdapter(store)
	authorization := mutationStageAuthorization(tenantID)

	stage, err := adapter.StageMutation(
		t.Context(), Identity{}, authorization, tenantID, stageRequestID, 1, true,
		newMutationSource("aborted"),
	)
	if err != nil {
		t.Fatal(err)
	}
	ref := *stage.content
	if stage.Token != string(stageRequestID) || stage.RequestID != stageRequestID {
		t.Fatalf("stage identity = %q, %q", stage.Token, stage.RequestID)
	}
	if err := stage.Abort(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := stage.Abort(t.Context()); err != nil {
		t.Fatalf("second Abort: %v", err)
	}
	if _, err := store.BeginMutation(
		t.Context(), tenantID, 1, createStageIntent(root.ID, "aborted", ref),
	); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("BeginMutation(released stage) = %v, want ErrNotFound", err)
	}
}

func TestMutationStageReplayKeepsDerivedMutationIdentity(t *testing.T) {
	store, tenantID, root := mutationStageCatalog(t)
	adapter := mutationStageAdapter(store)
	authorization := mutationStageAuthorization(tenantID)

	first, err := adapter.StageMutation(
		t.Context(), Identity{}, authorization, tenantID, stageRequestID, 1, true,
		newMutationSource("replay"),
	)
	if err != nil {
		t.Fatal(err)
	}
	intent := createStageIntent(root.ID, "settings.json", *first.content)
	prepared, err := store.BeginMutation(t.Context(), tenantID, 1, intent)
	if err != nil {
		t.Fatal(err)
	}
	first.claim()
	if err := first.Abort(t.Context()); err != nil {
		t.Fatalf("Abort claimed stage: %v", err)
	}

	replay, err := adapter.StageMutation(
		t.Context(), Identity{}, authorization, tenantID, stageRequestID, 1, true,
		newMutationSource("replay"),
	)
	if err != nil {
		t.Fatal(err)
	}
	replayedIntent := createStageIntent(root.ID, "settings.json", *replay.content)
	replayed, err := store.BeginMutation(t.Context(), tenantID, 1, replayedIntent)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.OperationID != prepared.OperationID {
		t.Fatalf("derived replay identity = %s, want %s", replayed.OperationID, prepared.OperationID)
	}
	if err := replay.Abort(t.Context()); err != nil {
		t.Fatal(err)
	}
	stored, err := store.PreparedMutation(t.Context(), tenantID, prepared.OperationID)
	if err != nil || stored.Intent.Create == nil || stored.Intent.Create.Spec.Content != *first.content {
		t.Fatalf("prepared replay = %+v, %v", stored, err)
	}
}

func TestMutationStageAbortAfterBeginRejectionReleasesContent(t *testing.T) {
	store, tenantID, root := mutationStageCatalog(t)
	stage, err := mutationStageAdapter(store).StageMutation(
		t.Context(), Identity{}, mutationStageAuthorization(tenantID), tenantID,
		stageRequestID, 1, true, newMutationSource("conflict"),
	)
	if err != nil {
		t.Fatal(err)
	}
	ref := *stage.content
	if _, err := store.BeginMutation(
		t.Context(), tenantID, 99, createStageIntent(root.ID, "conflict", ref),
	); err == nil {
		t.Fatal("BeginMutation unexpectedly succeeded")
	}
	if err := stage.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginMutation(
		t.Context(), tenantID, 1, createStageIntent(root.ID, "reuse", ref),
	); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("BeginMutation(released stage) = %v, want ErrNotFound", err)
	}
}

func TestMutationStageTransfersAndReleasesOwnedContent(t *testing.T) {
	store := &recordingMutationStore{
		ref: catalog.ContentRef{Stage: catalog.StageID{1}, Hash: catalog.ContentHash{2}, Size: 5},
	}
	tenantID := catalog.TenantID("tenant")
	source := newMutationSource("bytes")
	stage, err := (MutationAdapter{
		Store: store, Runtime: &tenant.TenantRuntime{}, Engine: &convergence.Engine{},
	}).StageMutation(
		t.Context(), Identity{}, mutationStageAuthorization(tenantID), tenantID,
		stageRequestID, 1, true, source,
	)
	if err != nil {
		t.Fatalf("StageMutation: %v", err)
	}
	if source.count != 1 || string(store.content) != "bytes" {
		t.Fatalf("owned transfer = settle %d, content %q", source.count, store.content)
	}
	if err := stage.Abort(t.Context()); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if len(store.released) != 1 || store.released[0] != store.ref {
		t.Fatalf("released refs = %#v, want %#v", store.released, store.ref)
	}
}

func TestMutationStageFailureSurfacesRemoteReleaseFailure(t *testing.T) {
	stage := MutationStage{state: &mutationStageState{abort: func(context.Context) error {
		return context.DeadlineExceeded
	}}}
	value, err := mutationStageFailure(t.Context(), stage, catalog.ErrIntegrity)
	if err != nil {
		t.Fatalf("mutationStageFailure: %v", err)
	}
	payload, ok := value.(json.RawMessage)
	if !ok {
		t.Fatalf("mutationStageFailure type = %T", value)
	}
	var response catalogproto.MutationResponse
	if err := catalogproto.Decode(payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Code != catalogproto.ErrorCodeUnavailable {
		t.Fatalf("response code = %q, want unavailable", response.Code)
	}
}

func TestMutationStageNeverReadsOwnedSourcesInHolder(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	ref := catalog.ContentRef{Stage: catalog.StageID{1}, Hash: catalog.ContentHash{2}, Size: 5}
	store := &ownedSourceStore{ref: ref}
	source := newUnreadableMutationSource()
	stage, err := (MutationAdapter{
		Store: store, Runtime: &tenant.TenantRuntime{}, Engine: &convergence.Engine{},
	}).StageMutation(
		t.Context(), Identity{}, mutationStageAuthorization(tenantID), tenantID,
		stageRequestID, 1, true, source,
	)
	if err != nil {
		t.Fatal(err)
	}
	if store.stageCalls != 1 || source.count != 1 || stage.content == nil || *stage.content != ref {
		t.Fatal("content source was not transferred exactly once")
	}
	if _, err := (MutationAdapter{
		Store: store, Runtime: &tenant.TenantRuntime{}, Engine: &convergence.Engine{},
	}).StageMutation(
		t.Context(), Identity{}, mutationStageAuthorization(tenantID), tenantID,
		stageRequestID, 1, false, nil,
	); err != nil {
		t.Fatalf("contentless stage: %v", err)
	}
	if store.stageCalls != 1 {
		t.Fatalf("contentless stage called content owner %d times", store.stageCalls)
	}
}

func mutationStageAdapter(store *catalog.Catalog) MutationAdapter {
	return MutationAdapter{
		Store:   localMutationStore{Catalog: store},
		Runtime: &tenant.TenantRuntime{},
		Engine:  &convergence.Engine{},
	}
}

func mutationStageAuthorization(tenantID catalog.TenantID) Authorization {
	return Authorization{
		Principal: "mount", Role: RoleMount, Presentation: catalog.PresentationMount,
		Route: Route{Tenant: tenantID, Generation: 1},
	}
}

func mutationStageCatalog(t *testing.T) (*catalog.Catalog, catalog.TenantID, catalog.Object) {
	t.Helper()
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	tenantID, err := catalog.NewTenantID("stage-tenant")
	if err != nil {
		t.Fatal(err)
	}
	provision := provisionCatalogServiceTenant(t, store, catalog.TenantProvision{
		OwnerID: "stage-owner", Tenant: tenantID,
		Mount:       catalog.MountPresentation{PresentationRoot: filepath.Join(t.TempDir(), "presentation")},
		BackingRoot: filepath.Join(t.TempDir(), "backing"), ContentSourceID: "stage-source",
		Access: catalog.TenantReadWrite, CasePolicy: catalog.CaseSensitive,
		Presentations: catalog.PresentMount, Generation: 1,
	})
	root, err := store.Root(t.Context(), tenantID)
	if err != nil || root.ID != provision.Root {
		t.Fatalf("active root = %+v, %v; provision root = %s", root, err, provision.Root)
	}
	return store, tenantID, root
}

type localMutationStore struct{ *catalog.Catalog }

type recordingMutationStore struct {
	CatalogMutationStore
	ref      catalog.ContentRef
	content  []byte
	released []catalog.ContentRef
}

func (s *recordingMutationStore) StageOwnedContent(
	ctx context.Context,
	source contentstream.Source,
) (ref catalog.ContentRef, err error) {
	defer func() {
		err = errors.Join(err, source.Settle(err), source.Wait(ctx))
	}()
	s.content, err = io.ReadAll(source)
	return s.ref, err
}

func (s *recordingMutationStore) ReleaseUnclaimedContent(
	_ context.Context,
	refs []catalog.ContentRef,
) error {
	s.released = append(s.released, refs...)
	return nil
}

type testMutationSource struct {
	io.Reader
	settled chan struct{}
	once    sync.Once
	err     error
	count   int
}

func newMutationSource(content string) *testMutationSource {
	return &testMutationSource{Reader: bytes.NewBufferString(content), settled: make(chan struct{})}
}

func newUnreadableMutationSource() *testMutationSource {
	return &testMutationSource{Reader: panicReader{}, settled: make(chan struct{})}
}

func (s *testMutationSource) Settle(err error) error {
	s.once.Do(func() {
		s.err = err
		s.count++
		close(s.settled)
	})
	return nil
}

func (s *testMutationSource) Wait(ctx context.Context) error {
	select {
	case <-s.settled:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) {
	panic("holder read an owned source")
}

type ownedSourceStore struct {
	CatalogMutationStore
	ref        catalog.ContentRef
	stageCalls int
}

func (s *ownedSourceStore) StageOwnedContent(
	ctx context.Context,
	source contentstream.Source,
) (catalog.ContentRef, error) {
	s.stageCalls++
	return s.ref, errors.Join(source.Settle(nil), source.Wait(ctx))
}

func createStageIntent(
	parent catalog.ObjectID,
	name string,
	ref catalog.ContentRef,
) catalog.MutationIntent {
	return catalog.MutationIntent{
		SourceID: "stage-test", Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
		Create: &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: parent, Name: name, Kind: catalog.KindFile, Mode: 0o600,
			ContentRevision: 1, Content: ref, Visibility: catalog.Visibility{Mount: true},
		}},
	}
}
