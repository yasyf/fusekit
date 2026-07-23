package catalogworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestNativeWriteRecoveryPreservesWorkerGenerationAndClearsManagerEpoch(t *testing.T) {
	root := filepath.Join(t.TempDir(), "catalog.sqlite.native-writes")
	firstRuntime := writeTokenForTest(1)
	if err := recoverNativeWrites(root, firstRuntime); err != nil {
		t.Fatal(err)
	}
	service := &server{writeRoot: root}
	manifest := writeManifestForTest(2, "owner-a")
	manifest.Dirty = true
	persistWriteForTest(t, service, manifest, 7)

	if err := recoverNativeWrites(root, firstRuntime); err != nil {
		t.Fatalf("same manager epoch recovery: %v", err)
	}
	if _, err := service.loadWrite(manifest.Token); err != nil {
		t.Fatalf("worker generation lost durable stage: %v", err)
	}

	if err := recoverNativeWrites(root, writeTokenForTest(4)); err != nil {
		t.Fatalf("new manager epoch recovery: %v", err)
	}
	if _, err := service.loadWrite(manifest.Token); !errors.Is(err, catalog.ErrHandleClosed) {
		t.Fatalf("manager restart retained orphan handle: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != nativeWriteRuntime {
		t.Fatalf("recovered files = %v, want runtime marker only", entries)
	}
}

func TestNativeWriteAuditRejectsOversizedDirectoryBeforeUnboundedCollection(t *testing.T) {
	root := filepath.Join(t.TempDir(), "catalog.sqlite.native-writes")
	if err := recoverNativeWrites(root, writeTokenForTest(1)); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= maxNativeStageFiles; index++ {
		path := filepath.Join(root, writeTokenForTest(index+1)+".data")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := auditNativeWrites(root); !errors.Is(err, catalog.ErrStorageQuota) {
		t.Fatalf("oversized native stage audit = %v, want storage quota", err)
	}
}

func TestNativeWriteAggregateByteBudgetsIncludeSparseGrowth(t *testing.T) {
	t.Run("owner", func(t *testing.T) {
		service := newWriteServerForTest(t)
		persistWriteForTest(t, service, writeManifestForTest(1, "owner-a"), 600<<20)
		if err := service.writeCapacity("owner-a", 500<<20, false); !errors.Is(err, catalog.ErrStorageQuota) {
			t.Fatalf("owner sparse growth = %v, want storage quota", err)
		}
	})

	t.Run("runtime", func(t *testing.T) {
		service := newWriteServerForTest(t)
		for index := 1; index <= 4; index++ {
			persistWriteForTest(
				t, service, writeManifestForTest(index, fmt.Sprintf("owner-%d", index)),
				maxWriteStageSize,
			)
		}
		if err := service.writeCapacity("new-owner", 1, false); !errors.Is(err, catalog.ErrStorageQuota) {
			t.Fatalf("runtime sparse growth = %v, want storage quota", err)
		}
	})
}

func TestSeedNativeWriteJoinsSourceCloseWhenOpenFails(t *testing.T) {
	source := &trackingReadCloser{Reader: strings.NewReader("content")}
	err := seedNativeWrite(
		filepath.Join(t.TempDir(), "stage"), int64(len("content")), source,
		func(string, int, os.FileMode) (*os.File, error) {
			return nil, syscall.ENOSPC
		},
	)
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("seed error = %v, want ENOSPC", err)
	}
	if !source.closed {
		t.Fatal("seed open failure did not close source")
	}
}

func TestMutableWriteReceiptIsBoundedAndReplaysLatestAcknowledgement(t *testing.T) {
	manager, provision, object, revision := newMutableWriteManagerForTest(t)
	bindMutationCommitterForTest(t, manager, nil)
	token, opened, err := manager.OpenWriteAt(
		t.Context(), "owner-a", provision.Tenant, catalog.PresentationMount,
		provision.Generation, object.ID, revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if opened != object {
		t.Fatalf("opened object = %+v, want %+v", opened, object)
	}

	for index := byte(1); index <= 8; index++ {
		payload := []byte(fmt.Sprintf("value-%02d", index))
		if _, err := manager.WriteAt(t.Context(), "owner-a", token, 0, payload); err != nil {
			t.Fatal(err)
		}
		if err := manager.TruncateWrite(t.Context(), "owner-a", token, int64(len(payload))); err != nil {
			t.Fatal(err)
		}
		committed, err := manager.CommitWriteAt(t.Context(), "owner-a", token)
		if err != nil {
			t.Fatalf("commit %d: %v", index, err)
		}
		if committed.Object.Size != int64(len(payload)) {
			t.Fatalf("commit %d size = %d", index, committed.Object.Size)
		}
		service := &server{writeRoot: manager.config.Database + ".native-writes"}
		manifest, err := service.loadWrite(token)
		if err != nil {
			t.Fatal(err)
		}
		if manifest.Last == nil || manifest.Last.Operation != committed.OperationID || manifest.Prepared != nil {
			t.Fatalf("commit %d manifest = %+v", index, manifest)
		}
		encoded, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) > 4096 {
			t.Fatalf("bounded receipt manifest grew to %d bytes", len(encoded))
		}
		replayed, err := manager.CommitWriteAt(t.Context(), "owner-a", token)
		if err != nil || replayed != committed {
			t.Fatalf("last receipt replay = %+v, %v; want %+v", replayed, err, committed)
		}
	}
}

func TestCommitWriteAtSerializesSameTokenButNotDifferentTokens(t *testing.T) {
	manager, provision, object, revision := newMutableWriteManagerForTest(t)
	started := make(chan struct{})
	release := make(chan struct{})
	bindMutationCommitterForTest(t, manager, func(ctx context.Context) {
		select {
		case <-started:
		default:
			close(started)
			<-release
		}
	})
	token, _, err := manager.OpenWriteAt(
		t.Context(), "owner-a", provision.Tenant, catalog.PresentationMount,
		provision.Generation, object.ID, revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.WriteAt(t.Context(), "owner-a", token, 0, []byte("next")); err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() {
		_, err := manager.CommitWriteAt(context.Background(), "owner-a", token)
		first <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first same-token commit did not reach preparer")
	}
	go func() {
		_, err := manager.CommitWriteAt(context.Background(), "owner-a", token)
		second <- err
	}()
	select {
	case err := <-second:
		t.Fatalf("second same-token commit bypassed first response: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first same-token commit: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second acknowledged commit replay = %v", err)
	}

	otherA := writeTokenForTest(90)
	otherB := writeTokenForTest(91)
	unlockA, err := manager.lockNativeToken(t.Context(), otherA)
	if err != nil {
		t.Fatal(err)
	}
	acquiredB := make(chan func(), 1)
	go func() {
		unlockB, _ := manager.lockNativeToken(context.Background(), otherB)
		acquiredB <- unlockB
	}()
	select {
	case unlockB := <-acquiredB:
		unlockB()
	case <-time.After(5 * time.Second):
		t.Fatal("different token was globally serialized")
	}
	unlockA()
}

func TestCloseNativeSessionWaitsForBlockedCommitBeforeCleanup(t *testing.T) {
	manager, provision, object, revision := newMutableWriteManagerForTest(t)
	started := make(chan struct{})
	release := make(chan struct{})
	bindMutationCommitterForTest(t, manager, func(context.Context) {
		close(started)
		<-release
	})
	token, _, err := manager.OpenWriteAt(
		t.Context(), "owner-a", provision.Tenant, catalog.PresentationMount,
		provision.Generation, object.ID, revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.WriteAt(t.Context(), "owner-a", token, 0, []byte("next")); err != nil {
		t.Fatal(err)
	}
	commitResult := make(chan error, 1)
	go func() {
		_, err := manager.CommitWriteAt(context.Background(), "owner-a", token)
		commitResult <- err
	}()
	<-started
	closeResult := make(chan error, 1)
	go func() { closeResult <- manager.CloseNativeSession(context.Background(), "owner-a") }()
	select {
	case err := <-closeResult:
		t.Fatalf("session close bypassed admitted commit: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-commitResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled commit = %v, want context canceled", err)
	}
	if err := <-closeResult; err != nil {
		t.Fatalf("session cleanup: %v", err)
	}
	if _, _, err := manager.ReadWriteAt(t.Context(), "owner-a", token, 0, 1); !errors.Is(err, catalog.ErrHandleClosed) {
		t.Fatalf("closed owner admission = %v, want handle closed", err)
	}
}

func TestMutableWriteLostChildResponsesReplayExactlyAcrossWorkerGeneration(t *testing.T) {
	t.Run("seal", func(t *testing.T) {
		manager, provision, object, revision := newMutableWriteManagerForTest(t)
		bindMutationCommitterForTest(t, manager, nil)
		token, _, err := manager.OpenWriteAt(
			t.Context(), "owner-a", provision.Tenant, catalog.PresentationMount,
			provision.Generation, object.ID, revision,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := manager.WriteAt(t.Context(), "owner-a", token, 0, []byte("sealed")); err != nil {
			t.Fatal(err)
		}
		if err := manager.TruncateWrite(t.Context(), "owner-a", token, int64(len("sealed"))); err != nil {
			t.Fatal(err)
		}
		generation := manager.current
		sealed, err := generation.client.SealAndBeginWrite(t.Context(), "owner-a", token)
		if err != nil || sealed.Prepared == nil {
			t.Fatalf("discarded seal response = %+v, %v", sealed, err)
		}
		service := &server{writeRoot: manager.config.Database + ".native-writes"}
		beforeRestart, err := service.loadWrite(token)
		if err != nil || beforeRestart.Prepared == nil || beforeRestart.Prepared.Pin.ID == (catalog.MutationPinID{}) {
			t.Fatalf("prepared mutation pin before restart = %+v, %v", beforeRestart.Prepared, err)
		}
		firstPin := beforeRestart.Prepared.Pin.ID
		if err := manager.poison(generation); err != nil {
			t.Fatal(err)
		}
		committed, err := manager.CommitWriteAt(t.Context(), "owner-a", token)
		if err != nil || committed.Object.Size != int64(len("sealed")) {
			t.Fatalf("seal replay = %+v, %v", committed, err)
		}
		afterRestart, err := service.loadWrite(token)
		if err != nil || afterRestart.Last == nil || afterRestart.Last.Pin.ID == (catalog.MutationPinID{}) {
			t.Fatalf("committed mutation pin after restart = %+v, %v", afterRestart.Last, err)
		}
		if afterRestart.Last.Pin.ID == firstPin {
			t.Fatalf("worker restart reused prior catalog-owner mutation pin %x", firstPin)
		}
	})

	t.Run("resolve", func(t *testing.T) {
		manager, provision, object, revision := newMutableWriteManagerForTest(t)
		bindMutationCommitterForTest(t, manager, nil)
		token, _, err := manager.OpenWriteAt(
			t.Context(), "owner-a", provision.Tenant, catalog.PresentationMount,
			provision.Generation, object.ID, revision,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := manager.WriteAt(t.Context(), "owner-a", token, 0, []byte("resolved")); err != nil {
			t.Fatal(err)
		}
		if err := manager.TruncateWrite(t.Context(), "owner-a", token, int64(len("resolved"))); err != nil {
			t.Fatal(err)
		}
		generation := manager.current
		sealed, err := generation.client.SealAndBeginWrite(t.Context(), "owner-a", token)
		if err != nil || sealed.Prepared == nil {
			t.Fatalf("seal = %+v, %v", sealed, err)
		}
		preparer, err := manager.tenantPreparer()
		if err != nil {
			t.Fatal(err)
		}
		if err := preparer(
			t.Context(), sealed.Prepared.Tenant, sealed.Generation,
			sealed.Prepared.ExpectedHead+1,
		); err != nil {
			t.Fatal(err)
		}
		discarded, err := generation.client.ResolveCommittedWrite(
			t.Context(), "owner-a", token,
		)
		if err != nil {
			t.Fatal(err)
		}
		service := &server{writeRoot: manager.config.Database + ".native-writes"}
		beforeRestart, err := service.loadWrite(token)
		if err != nil || beforeRestart.Last == nil || beforeRestart.Last.Pin.ID == (catalog.MutationPinID{}) {
			t.Fatalf("resolved mutation pin before restart = %+v, %v", beforeRestart.Last, err)
		}
		firstPin := beforeRestart.Last.Pin.ID
		if err := manager.poison(generation); err != nil {
			t.Fatal(err)
		}
		replayed, err := manager.CommitWriteAt(t.Context(), "owner-a", token)
		if err != nil || replayed != discarded {
			t.Fatalf("resolve replay = %+v, %v; want %+v", replayed, err, discarded)
		}
		afterRestart, err := service.loadWrite(token)
		if err != nil || afterRestart.Last == nil || afterRestart.Last.Pin.ID == (catalog.MutationPinID{}) {
			t.Fatalf("recovered mutation pin after restart = %+v, %v", afterRestart.Last, err)
		}
		if afterRestart.Last.Pin.ID == firstPin {
			t.Fatalf("worker restart reused prior catalog-owner mutation pin %x", firstPin)
		}
	})
}

type trackingReadCloser struct {
	io.Reader
	closed bool
}

func (r *trackingReadCloser) Close() error {
	r.closed = true
	return nil
}

func newWriteServerForTest(t *testing.T) *server {
	t.Helper()
	root := filepath.Join(t.TempDir(), "catalog.sqlite.native-writes")
	if err := recoverNativeWrites(root, writeTokenForTest(1)); err != nil {
		t.Fatal(err)
	}
	return &server{writeRoot: root}
}

func persistWriteForTest(
	t *testing.T, service *server, manifest nativeWriteManifest, size int64,
) {
	t.Helper()
	_, dataPath, err := service.writePaths(manifest.Token)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(dataPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := service.saveWrite(manifest); err != nil {
		t.Fatal(err)
	}
}

func writeManifestForTest(index int, owner string) nativeWriteManifest {
	tenant, err := catalog.NewTenantID(fmt.Sprintf("write-test-%d", index))
	if err != nil {
		panic(err)
	}
	var object catalog.ObjectID
	object[15] = byte(index)
	return nativeWriteManifest{
		Token: writeTokenForTest(index), Owner: owner, Tenant: tenant,
		Presentation: catalog.PresentationMount, Generation: 1,
		Object: catalog.Object{
			ID: object, Tenant: tenant, Kind: catalog.KindFile, Revision: 1,
			ContentRevision: 1, Size: 0, Mode: 0o600,
			Visibility: catalog.Visibility{Mount: true},
		},
		ExpectedHead: 1,
	}
}

func writeTokenForTest(index int) string {
	return fmt.Sprintf("%032x", index)
}

func newMutableWriteManagerForTest(
	t *testing.T,
) (*Manager, catalog.TenantProvision, catalog.Object, catalog.Revision) {
	t.Helper()
	manager, _ := newTestManager(t)
	store, err := catalog.Open(t.Context(), manager.config.Database)
	if err != nil {
		t.Fatal(err)
	}
	provision := testTenantProvision(t, "mutable-write")
	if _, err := store.ProvisionTenant(t.Context(), provision); err != nil {
		t.Fatal(err)
	}
	ref, err := store.StageContent(t.Context(), strings.NewReader("initial"))
	if err != nil {
		t.Fatal(err)
	}
	seedMutableWriteSource(t, store, provision, ref)
	revision, err := store.Head(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.Root(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.Snapshot(t.Context(), provision.Tenant, catalog.EnumerationScope{
		Kind: catalog.EnumerationContainer, Presentation: catalog.PresentationMount, Parent: root.ID,
	}, revision, catalog.SnapshotCursor{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 {
		t.Fatalf("seed snapshot objects = %+v", page.Objects)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return manager, provision, page.Objects[0], revision
}

func seedMutableWriteSource(
	t *testing.T,
	store *catalog.Catalog,
	provision catalog.TenantProvision,
	content catalog.ContentRef,
) {
	t.Helper()
	root, err := store.Root(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	head, err := store.Head(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.BeginMutation(t.Context(), provision.Tenant, head, catalog.MutationIntent{
		SourceID: "mutable-write-fixture",
		Origin:   catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
		Create: &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: root.ID, Name: "settings.json", Kind: catalog.KindFile, Mode: 0o600,
			ContentRevision: 1, Content: content, Visibility: catalog.Visibility{Mount: true},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := catalog.NewMutationOwnerID()
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimMutation(t.Context(), prepared.OperationID, owner)
	if err != nil || claimed.Claim == nil {
		t.Fatalf("claim seed mutation = %+v, %v", claimed, err)
	}
	if _, err := store.MarkMutationApplied(t.Context(), prepared.OperationID, *claimed.Claim); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitMutation(t.Context(), provision.Tenant, prepared.OperationID); err != nil {
		t.Fatal(err)
	}
}

func bindMutationCommitterForTest(
	t *testing.T,
	manager *Manager,
	before func(context.Context),
) {
	t.Helper()
	err := manager.BindTenantPreparer(func(
		ctx context.Context,
		tenantID catalog.TenantID,
		_ catalog.Generation,
		_ catalog.Revision,
	) error {
		if before != nil {
			before(ctx)
		}
		pending, err := manager.PendingMutation(ctx, tenantID)
		if err != nil {
			return err
		}
		if pending == nil {
			return nil
		}
		owner, err := catalog.NewMutationOwnerID()
		if err != nil {
			return err
		}
		claimed, err := manager.ClaimMutation(ctx, pending.OperationID, owner)
		if err != nil {
			return err
		}
		if claimed.Claim == nil {
			return catalog.ErrIntegrity
		}
		if _, err := manager.MarkMutationApplied(ctx, pending.OperationID, *claimed.Claim); err != nil {
			return err
		}
		_, err = manager.CommitMutation(ctx, pending.Tenant, pending.OperationID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}
