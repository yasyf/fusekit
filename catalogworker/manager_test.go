package catalogworker

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/internal/recoveryid"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

func testChildSignature() proc.SignatureDigest {
	var signature proc.SignatureDigest
	signature[0] = 1
	return signature
}

func catalogWorkerOwnerGeneration(label string) proc.OwnerGeneration {
	digest := sha256.Sum256([]byte(label))
	var generation proc.OwnerGeneration
	copy(generation[:], digest[:len(generation)])
	return generation
}

func TestManagerRequiresExactSpawnAuthority(t *testing.T) {
	config := ManagerConfig{
		Executable: "/test/product-helper", Database: "/tmp/catalogworker-test.sqlite",
		ReadinessTimeout: time.Second, OperationTimeout: time.Second,
	}
	if manager, err := NewManager(t.Context(), config); manager != nil || err == nil ||
		!strings.Contains(err.Error(), "expected child signature") {
		t.Fatalf("NewManager without signature = %v, %v", manager, err)
	}
	config.ExpectedSignature = testChildSignature()
	if manager, err := NewManager(t.Context(), config); manager != nil || err == nil ||
		!strings.Contains(err.Error(), "process manager") {
		t.Fatalf("NewManager without process manager = %v, %v", manager, err)
	}
}

func TestManagerUsesOnlyDaemonkitManagedSession(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "fcw-session-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	launcher := newTestProcessLauncher(t)
	manager, err := NewManager(t.Context(), ManagerConfig{
		Executable: "/test/product-helper", Database: filepath.Join(directory, "catalog.sqlite"),
		ExpectedSignature: testChildSignature(),
		launcher:          launcher, ReadinessTimeout: 30 * time.Second,
		OperationTimeout: 10 * time.Second, StopTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.TopologyHead(t.Context(), "test-owner"); err != nil {
		t.Fatal(err)
	}
	spec := launcher.spec(t, 0)
	if len(spec.Spawn.Args) != 4 || spec.Spawn.Args[0] != childMode || spec.Spawn.Args[1] != filepath.Join(directory, "catalog.sqlite") {
		t.Fatalf("catalog worker arguments = %q", spec.Spawn.Args)
	}
	if spec.Spawn.RecoveryID != recoveryid.CatalogWorker || spec.Spawn.Executable != "/test/product-helper" ||
		spec.Spawn.Stdin != proc.StdioNull || spec.Spawn.Stdout != proc.StdioNull || spec.Spawn.Stderr != proc.StdioPipe ||
		!spec.Spawn.SpawnedSession || spec.Spawn.RequiresPeerFence || spec.Spawn.ExpectedSignature == nil ||
		*spec.Spawn.ExpectedSignature != testChildSignature() {
		t.Fatalf("catalog worker spawn is not exact: %+v", spec.Spawn)
	}
	serverDeadline, clientDeadline, ok := spec.Client.Ladder.Deadlines(wire.Op(OperationHead))
	if spec.Client.WireBuild != transportproto.WireBuild || !ok ||
		serverDeadline != childSessionServerDeadline || clientDeadline != childSessionClientDeadline {
		t.Fatalf("catalog worker spawned client config is not exact: %+v", spec.Client)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSocket != 0 {
			t.Fatalf("catalog worker left synthetic socket %q", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestManagerTopologyWaitIgnoresOperationDeadlineAndKeepsGeneration(t *testing.T) {
	manager, launcher := newTestManager(t)
	owner := catalog.SourceAuthorityFleetOwnerID("topology-long-poll")
	head, err := manager.TopologyHead(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	manager.config.OperationTimeout = 25 * time.Millisecond
	waitCtx, cancelWait := context.WithCancel(t.Context())
	waiting := make(chan error, 1)
	go func() {
		_, waitErr := manager.WaitTopologyChanges(waitCtx, catalog.TopologyChangesRequest{
			Owner: owner, After: head.Revision, Limit: catalog.TopologyPageLimit,
		})
		waiting <- waitErr
	}()
	select {
	case err := <-waiting:
		t.Fatalf("topology long poll settled at operation deadline: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancelWait()
	if err := <-waiting; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel topology long poll = %v, want context canceled", err)
	}
	manager.config.OperationTimeout = 10 * time.Second
	if _, err := manager.TopologyHead(t.Context(), owner); err != nil {
		t.Fatalf("generation after cancelled topology long poll: %v", err)
	}
	if got := launcher.count(); got != 1 {
		t.Fatalf("catalog worker generations = %d, want 1", got)
	}
}

func TestManagerDeadlineSettlesExactGenerationBeforeReturn(t *testing.T) {
	t.Setenv("FUSEKIT_CHILD_ENV_SENTINEL", "preserved")
	t.Setenv("CGOFUSE_LIBFUSE_PATH", "/tmp/foreign-libfuse.dylib")
	directory, err := os.MkdirTemp("/tmp", "fcw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	launcher := newTestProcessLauncher(t)
	manager, err := NewManager(t.Context(), ManagerConfig{
		Executable: "/test/product-helper", Database: filepath.Join(directory, "catalog.sqlite"),
		ExpectedSignature: testChildSignature(),
		launcher:          launcher,
		ReadinessTimeout:  5 * time.Second, OperationTimeout: 10 * time.Second, StopTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	t.Setenv("FUSEKIT_CHILD_ENV_SENTINEL", "changed-after-manager-construction")

	if _, err := manager.TopologyHead(t.Context(), "test-owner"); err != nil {
		t.Fatalf("first generation: %v", err)
	}
	if got := launcher.spec(t, 0).Spawn.Env; !slices.Contains(got, "FUSEKIT_CHILD_ENV_SENTINEL=preserved") ||
		slices.Contains(got, "CGOFUSE_LIBFUSE_PATH=/tmp/foreign-libfuse.dylib") {
		t.Fatalf("catalog worker environment was not isolated from native-only state: %v", got)
	}
	first := launcher.process(t, 0)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := manager.TopologyHead(ctx, "test-owner"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call = %v, want context.Canceled", err)
	}
	select {
	case <-first.stopped:
	default:
		t.Fatal("canceled call returned before exact generation settled")
	}
	if !first.reaped() {
		t.Fatal("canceled call returned before worker reap")
	}

	if _, err := manager.TopologyHead(t.Context(), "test-owner"); err != nil {
		t.Fatalf("replacement generation: %v", err)
	}
	second := launcher.process(t, 1)
	if second == first {
		t.Fatal("poisoned generation was reused")
	}
}

func TestManagerHardDeadlineReapsWedgedWorkerBeforeReturnAndReplacement(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "fcw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	launcher := newTestProcessLauncher(t)
	manager, err := NewManager(t.Context(), ManagerConfig{
		Executable: "/test/product-helper", Database: filepath.Join(directory, "catalog.sqlite"),
		ExpectedSignature: testChildSignature(),
		launcher:          launcher,
		ReadinessTimeout:  5 * time.Second, OperationTimeout: 5 * time.Second, StopTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if _, err := manager.TopologyHead(t.Context(), "test-owner"); err != nil {
		t.Fatal(err)
	}
	manager.config.OperationTimeout = 250 * time.Millisecond
	first := launcher.process(t, 0)
	aborted := make(chan error, 1)
	client := manager.current.client
	manager.current.abortClient = func(cause error) error {
		aborted <- cause
		return client.Abort(cause)
	}
	gracefulCloseErr := errors.New("graceful close used for poisoned generation")
	manager.current.closeClient = func() error { return gracefulCloseErr }
	_, err = managerCall(manager, context.Background(), func(*Client) (struct{}, error) {
		<-first.stopped
		return struct{}{}, errors.New("wedged generation stopped")
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wedged call = %v, want hard deadline", err)
	}
	if !first.reaped() {
		t.Fatal("hard deadline returned before exact worker reap")
	}
	if errors.Is(err, gracefulCloseErr) {
		t.Fatalf("deadline used graceful close: %v", err)
	}
	select {
	case cause := <-aborted:
		if cause == nil {
			t.Fatal("deadline aborted generation without a cause")
		}
	default:
		t.Fatal("deadline stopped generation without typed client abort")
	}
	manager.config.OperationTimeout = 5 * time.Second
	if _, err := manager.TopologyHead(context.Background(), "test-owner"); err != nil {
		t.Fatalf("replacement after hard deadline: %v", err)
	}
	if launcher.count() != 2 {
		t.Fatalf("generation count = %d, want 2", launcher.count())
	}
}

func TestManagerUploadDeadlineSettlesAndJoinsProducerBeforeReturn(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "fcw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	launcher := newTestProcessLauncher(t)
	manager, err := NewManager(t.Context(), ManagerConfig{
		Executable: "/test/product-helper", Database: filepath.Join(directory, "catalog.sqlite"),
		ExpectedSignature: testChildSignature(),
		launcher:          launcher,
		ReadinessTimeout:  5 * time.Second, OperationTimeout: 5 * time.Second, StopTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if _, err := manager.TopologyHead(t.Context(), "test-owner"); err != nil {
		t.Fatal(err)
	}
	manager.config.OperationTimeout = 250 * time.Millisecond
	source := newOwnedBlockingSource()
	_, err = manager.StageOwnedContent(context.Background(), source)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StageOwnedContent = %v, want hard deadline", err)
	}
	if !source.waited() {
		t.Fatal("upload returned before producer joined")
	}
	if !launcher.process(t, 0).reaped() {
		t.Fatal("upload returned before consumer worker reaped")
	}
	manager.config.OperationTimeout = 5 * time.Second
	if _, err := manager.TopologyHead(context.Background(), "test-owner"); err != nil {
		t.Fatalf("replacement after upload deadline: %v", err)
	}
}

func TestManagerBlocksReplacementAndCloseUntilRetiringGenerationReaped(t *testing.T) {
	t.Run("replacement", func(t *testing.T) {
		manager, launcher := newTestManager(t)
		if _, err := manager.TopologyHead(t.Context(), "test-owner"); err != nil {
			t.Fatal(err)
		}
		first := launcher.process(t, 0)
		gate := make(chan struct{})
		first.stopGate = gate
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		failed := make(chan error, 1)
		go func() {
			_, err := manager.TopologyHead(ctx, "test-owner")
			failed <- err
		}()
		<-first.stopStarted
		replacement := make(chan error, 1)
		go func() {
			_, err := manager.TopologyHead(t.Context(), "test-owner")
			replacement <- err
		}()
		assertNoResult(t, replacement, "replacement admitted before reap")
		if count := launcher.count(); count != 1 {
			t.Fatalf("launched %d generations before reap, want 1", count)
		}
		close(gate)
		if err := <-failed; !errors.Is(err, context.Canceled) {
			t.Fatalf("failed call = %v", err)
		}
		if err := <-replacement; err != nil {
			t.Fatalf("replacement call: %v", err)
		}
		if count := launcher.count(); count != 2 {
			t.Fatalf("launched %d generations after reap, want 2", count)
		}
	})

	t.Run("close", func(t *testing.T) {
		manager, launcher := newTestManager(t)
		if _, err := manager.TopologyHead(t.Context(), "test-owner"); err != nil {
			t.Fatal(err)
		}
		first := launcher.process(t, 0)
		gate := make(chan struct{})
		first.stopGate = gate
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		failed := make(chan error, 1)
		go func() {
			_, err := manager.TopologyHead(ctx, "test-owner")
			failed <- err
		}()
		<-first.stopStarted
		closed := make(chan error, 1)
		go func() { closed <- manager.Close() }()
		assertNoResult(t, closed, "Close returned before reap")
		close(gate)
		if err := <-failed; !errors.Is(err, context.Canceled) {
			t.Fatalf("failed call = %v", err)
		}
		if err := <-closed; err != nil {
			t.Fatalf("Close: %v", err)
		}
		if count := launcher.count(); count != 1 {
			t.Fatalf("Close launched %d generations, want 1", count)
		}
	})
}

func TestManagerCloseReplaysExactJoinedSettlementError(t *testing.T) {
	manager, launcher := newTestManager(t)
	if _, err := manager.TopologyHead(t.Context(), "test-owner"); err != nil {
		t.Fatal(err)
	}
	stopErr := errors.New("stop failed")
	clientErr := errors.New("client close failed")
	launcher.process(t, 0).stopErr = stopErr
	manager.current.closeClient = func() error { return clientErr }

	const callers = 16
	results := make(chan error, callers)
	for range callers {
		go func() { results <- manager.Close() }()
	}
	for range callers {
		err := <-results
		if !errors.Is(err, stopErr) || !errors.Is(err, clientErr) {
			t.Fatalf("Close = %v, want joined stop/client errors", err)
		}
	}
	if err := manager.Close(); !errors.Is(err, stopErr) || !errors.Is(err, clientErr) {
		t.Fatalf("repeated Close = %v, want exact joined result", err)
	}
}

func TestManagerCloseDuringStartCachesFinalStartSettlementError(t *testing.T) {
	directory := t.TempDir()
	ready := make(chan struct{})
	started := make(chan *testManagedProcess, 1)
	launcher := newTestProcessLauncher(t)
	launcher.readyGate = ready
	launcher.started = started
	manager, err := NewManager(t.Context(), ManagerConfig{
		Executable: "/test/product-helper", Database: filepath.Join(directory, "catalog.sqlite"),
		ExpectedSignature: testChildSignature(),
		launcher:          launcher,
		ReadinessTimeout:  5 * time.Second, OperationTimeout: time.Second, StopTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	startResult := make(chan error, 1)
	go func() {
		_, err := manager.TopologyHead(context.Background(), "test-owner")
		startResult <- err
	}()
	process := <-started
	stopErr := errors.New("start stop failed")
	process.stopErr = stopErr
	closeErr := manager.Close()
	if !errors.Is(closeErr, stopErr) || !errors.Is(closeErr, context.Canceled) {
		t.Fatalf("Close during start = %v, want canceled plus stop error", closeErr)
	}
	if err := <-startResult; !errors.Is(err, stopErr) || !errors.Is(err, context.Canceled) {
		t.Fatalf("starting call = %v, want canceled plus stop error", err)
	}
	if err := manager.Close(); !errors.Is(err, stopErr) || !errors.Is(err, context.Canceled) {
		t.Fatalf("repeated Close during start = %v, want canceled plus stop error", err)
	}
}

func TestManagerCloseCancelsAndJoinsBlockedReadiness(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "fcw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	readyGate := make(chan struct{})
	launcher := newTestProcessLauncher(t)
	launcher.readyGate = readyGate
	launcher.started = make(chan *testManagedProcess, 1)
	manager, err := NewManager(t.Context(), ManagerConfig{
		Executable: "/test/product-helper", Database: filepath.Join(directory, "catalog.sqlite"),
		ExpectedSignature: testChildSignature(),
		launcher:          launcher,
		ReadinessTimeout:  5 * time.Second, OperationTimeout: time.Second, StopTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	callDone := make(chan error, 1)
	go func() {
		_, callErr := manager.TopologyHead(t.Context(), "test-owner")
		callDone <- callErr
	}()
	process := <-launcher.started
	stopGate := make(chan struct{})
	process.stopGate = stopGate
	closed := make(chan error, 1)
	go func() { closed <- manager.Close() }()
	<-process.stopStarted
	assertNoResult(t, closed, "Close returned while readiness child was unreaped")
	assertNoResult(t, callDone, "starting call returned while readiness child was unreaped")
	close(stopGate)
	if err := <-closed; err != nil {
		t.Fatalf("Close = %v, want clean manager-canceled start settlement", err)
	}
	if err := <-callDone; err == nil {
		t.Fatal("starting call succeeded after Close")
	}
	if !process.reaped() {
		t.Fatal("Close returned before blocked-readiness child reaped")
	}
}

func TestTenantRuntimeRecoversDesiredFleetThroughWorkerOnly(t *testing.T) {
	manager, _ := newTestManager(t)
	runtime, err := tenant.NewRuntime(t.Context(), manager, noopPlanner{}, noopFleet{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tenantID, err := catalog.NewTenantID("remote-tenant-store")
	if err != nil {
		t.Fatal(err)
	}
	spec := tenant.TenantSpec{
		OwnerID: "test", ID: tenantID, Mount: tenant.MountSpec{PresentationRoot: "/tmp/remote-tenant-store"},
		Backing: tenant.BackingSpec{Root: "/tmp/remote-tenant-backing"},
		Content: tenant.ContentSource{ID: "test"},
		Traits: tenant.TenantTraits{
			Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: catalog.PresentMount,
		},
		Generation: 1,
	}
	if err := runtime.ProvisionTenant(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	runtime.Close()
	if err := runtime.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	recovered, err := tenant.NewRuntime(t.Context(), manager, noopPlanner{}, noopFleet{}, []catalog.TenantProvision{{
		OwnerID: string(spec.OwnerID), Tenant: spec.ID,
		Mount: catalog.MountPresentation{PresentationRoot: spec.Mount.PresentationRoot}, BackingRoot: spec.Backing.Root,
		ContentSourceID: spec.Content.ID, Access: spec.Traits.Access,
		CasePolicy: spec.Traits.CaseSensitivity, Presentations: spec.Traits.Presentations,
		Generation: spec.Generation,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		recovered.Close()
		_ = recovered.Wait(t.Context())
	}()
	if specs := recovered.Specs(); len(specs) != 1 || specs[0] != spec {
		t.Fatalf("recovered specs = %+v, want %+v", specs, spec)
	}
}

func TestManagerReplaysCommittedTenantWritesAfterGenerationLoss(t *testing.T) {
	t.Run("state CAS", func(t *testing.T) {
		manager, launcher := newTestManager(t)
		provision := testTenantProvision(t, "remote-state-replay")
		if _, err := manager.ProvisionTenant(t.Context(), provision); err != nil {
			t.Fatal(err)
		}
		current, err := manager.LoadTenantState(t.Context(), provision.Tenant)
		if err != nil {
			t.Fatal(err)
		}
		requested := current
		requested.ActivatedGeneration = requested.Generation
		committed, err := manager.SaveTenantState(t.Context(), current.Version, requested)
		if err != nil {
			t.Fatal(err)
		}
		first := launcher.process(t, 0)
		if err := manager.poison(manager.current); err != nil {
			t.Fatal(err)
		}
		if !first.reaped() {
			t.Fatal("lost generation was not reaped")
		}
		replayed, err := manager.SaveTenantState(t.Context(), current.Version, requested)
		if err != nil {
			t.Fatalf("replay after generation loss: %v", err)
		}
		if replayed != committed {
			t.Fatalf("replayed state = %+v, want %+v", replayed, committed)
		}
	})

	t.Run("provision removal", func(t *testing.T) {
		manager, launcher := newTestManager(t)
		provision := testTenantProvision(t, "remote-remove-replay")
		if _, err := manager.ProvisionTenant(t.Context(), provision); err != nil {
			t.Fatal(err)
		}
		generation := manager.current
		if err := generation.client.RemoveTenantProvision(t.Context(), provision.Tenant, provision.Generation); err != nil {
			t.Fatal(err)
		}
		if err := manager.poison(generation); err != nil {
			t.Fatal(err)
		}
		if !launcher.process(t, 0).reaped() {
			t.Fatal("lost generation was not reaped")
		}
		if err := manager.RemoveTenantProvision(t.Context(), provision.Tenant, provision.Generation); err != nil {
			t.Fatalf("replay after generation loss: %v", err)
		}
		head, err := manager.TopologyHead(t.Context(), "test-owner")
		if err != nil {
			t.Fatal(err)
		}
		if head.TenantCount != 0 {
			t.Fatalf("topology after replay = %+v", head)
		}
	})
}

func TestManagerReplaysSourceAuthorityFleetTransitionsAfterGenerationLoss(t *testing.T) {
	manager, _ := newTestManager(t)
	owner := catalog.SourceAuthorityFleetOwnerID("owner")
	authority := causal.SourceAuthorityID("authority")
	declarations, digest, declarationsDigest := testSourceAuthorityFleet(
		t, []causal.SourceAuthorityID{authority},
	)
	reconcile := catalog.SourceAuthorityFleetReconcileRequest{
		Owner: owner, Generation: 1, Declarations: declarations,
		Complete: true, AuthorityCount: 1, AuthoritiesDigest: digest,
		DeclarationsDigest: declarationsDigest,
	}
	generation, err := manager.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	committedReconcile, err := generation.client.ReconcileSourceAuthorityFleet(t.Context(), reconcile)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	replayedReconcile, err := manager.ReconcileSourceAuthorityFleet(t.Context(), reconcile)
	if err != nil {
		t.Fatalf("reconcile replay after generation loss: %v", err)
	}
	if replayedReconcile != committedReconcile {
		t.Fatalf("replayed reconcile = %+v, want %+v", replayedReconcile, committedReconcile)
	}

	acknowledgement := catalog.SourceAuthorityFleetAcknowledgement{
		Owner: owner, Generation: 1, AuthorityCount: 1,
		AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
		StageDigest: committedReconcile.StageDigest,
	}
	generation = manager.current
	committedFleet, err := generation.client.AcknowledgeSourceAuthorityFleet(t.Context(), acknowledgement)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	replayedFleet, err := manager.AcknowledgeSourceAuthorityFleet(t.Context(), acknowledgement)
	if err != nil {
		t.Fatalf("acknowledgement replay after generation loss: %v", err)
	}
	if replayedFleet != committedFleet {
		t.Fatalf("replayed fleet = %+v, want %+v", replayedFleet, committedFleet)
	}

	ref := catalog.SourceAuthorityRuntimeRef{
		Owner: owner, Generation: 1, Authority: authority,
	}
	fence := catalog.SourceAuthorityRuntimeFence{
		Owner: owner, Generation: 1, Authority: authority, Epoch: [16]byte{1},
	}
	generation = manager.current
	if err := generation.client.TakeoverSourceAuthorityRuntime(
		t.Context(),
		catalog.SourceAuthorityRuntimeTakeover{
			Ref: ref, Epoch: fence.Epoch, Process: testSourceRuntimeProcessRecord(t),
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := generation.client.OpenSourceAuthorityRuntime(t.Context(), fence); err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	if err := manager.OpenSourceAuthorityRuntime(t.Context(), fence); err != nil {
		t.Fatalf("runtime-open replay after generation loss: %v", err)
	}
	generation = manager.current
	if err := generation.client.CloseSourceAuthorityRuntime(t.Context(), fence); err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	if err := manager.CloseSourceAuthorityRuntime(t.Context(), fence); err != nil {
		t.Fatalf("runtime-close replay after generation loss: %v", err)
	}

	emptyDigest, err := catalog.SourceAuthorityFleetDigest(nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyDeclarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyReconcile := catalog.SourceAuthorityFleetReconcileRequest{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		Complete: true, AuthoritiesDigest: emptyDigest,
		DeclarationsDigest: emptyDeclarationsDigest,
	}
	generation = manager.current
	committedEmpty, err := generation.client.ReconcileSourceAuthorityFleet(t.Context(), emptyReconcile)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	replayedEmpty, err := manager.ReconcileSourceAuthorityFleet(t.Context(), emptyReconcile)
	if err != nil {
		t.Fatalf("empty reconcile replay after generation loss: %v", err)
	}
	if replayedEmpty != committedEmpty {
		t.Fatalf("replayed empty reconcile = %+v, want %+v", replayedEmpty, committedEmpty)
	}

	retire := catalog.SourceAuthorityRetireRequest{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		Authority: authority, StageDigest: committedEmpty.StageDigest,
	}
	generation = manager.current
	committedRetirement, err := generation.client.RetireSourceAuthority(t.Context(), retire)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	replayedRetirement, err := manager.RetireSourceAuthority(t.Context(), retire)
	if err != nil {
		t.Fatalf("retirement replay after generation loss: %v", err)
	}
	if replayedRetirement != committedRetirement {
		t.Fatalf("replayed retirement = %+v, want %+v", replayedRetirement, committedRetirement)
	}

	emptyAcknowledgement := catalog.SourceAuthorityFleetAcknowledgement{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		AuthoritiesDigest: emptyDigest, DeclarationsDigest: emptyDeclarationsDigest,
		StageDigest: committedEmpty.StageDigest,
	}
	generation = manager.current
	committedEmptyFleet, err := generation.client.AcknowledgeSourceAuthorityFleet(
		t.Context(), emptyAcknowledgement,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	replayedEmptyFleet, err := manager.AcknowledgeSourceAuthorityFleet(t.Context(), emptyAcknowledgement)
	if err != nil {
		t.Fatalf("empty acknowledgement replay after generation loss: %v", err)
	}
	if replayedEmptyFleet != committedEmptyFleet {
		t.Fatalf("replayed empty fleet = %+v, want %+v", replayedEmptyFleet, committedEmptyFleet)
	}
	page, err := manager.SourceAuthorityFleetPage(t.Context(), catalog.SourceAuthorityFleetPageRequest{
		Owner: owner, Generation: 2, Limit: catalog.SourceAuthorityFleetPageLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Declarations) != 0 || page.Next != "" || page.Fleet != committedEmptyFleet {
		t.Fatalf("empty acknowledged fleet page = %+v", page)
	}
	if _, err := manager.ReconcileSourceAuthorityFleet(t.Context(), reconcile); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("stale fleet reconciliation = %v, want ErrGenerationMismatch", err)
	}
	if err := manager.OpenSourceAuthorityRuntime(t.Context(), fence); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("stale runtime fence = %v, want ErrGenerationMismatch", err)
	}
}

func TestManagerReplaysSourceObserverReceiptAcknowledgementsAfterGenerationLoss(t *testing.T) {
	manager, _ := newTestManager(t)
	configuration, settlement := seedSourceObserverReceipts(t, manager.config.Database)

	generation, err := manager.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := generation.client.AcknowledgeSourceObserverConfiguration(t.Context(), configuration); err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	if err := manager.AcknowledgeSourceObserverConfiguration(t.Context(), configuration); err != nil {
		t.Fatalf("configuration acknowledgement replay after generation loss: %v", err)
	}

	generation = manager.current
	if err := generation.client.AcknowledgeSourceObserverSettlement(t.Context(), settlement); err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	if err := manager.AcknowledgeSourceObserverSettlement(t.Context(), settlement); err != nil {
		t.Fatalf("settlement acknowledgement replay after generation loss: %v", err)
	}

	forged := settlement
	forged.Digest[0] ^= 0xff
	if err := manager.AcknowledgeSourceObserverSettlement(t.Context(), forged); !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("forged settlement acknowledgement = %v, want ErrMutationConflict", err)
	}
}

func TestManagerReplaysStorageQuarantineResolutionAndAcknowledgement(t *testing.T) {
	manager, _ := newTestManager(t)
	hash := seedStorageQuarantine(t, manager.config.Database)
	generation, err := manager.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	page, err := generation.client.InspectStorageQuarantine(
		t.Context(), catalog.StorageTransitionID{}, catalog.MaintenancePageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.More {
		t.Fatalf("storage quarantine page = %+v, want one entry", page)
	}
	entry := page.Entries[0]
	committed, err := generation.client.ResolveStorageQuarantine(
		t.Context(), entry.ID, entry.Token, catalog.StorageQuarantineDiscard,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	replayed, err := manager.ResolveStorageQuarantine(
		t.Context(), entry.ID, entry.Token, catalog.StorageQuarantineDiscard,
	)
	if err != nil {
		t.Fatalf("resolution replay after generation loss: %v", err)
	}
	if replayed != committed {
		t.Fatalf("replayed resolution = %+v, want %+v", replayed, committed)
	}
	generation = manager.current
	if err := generation.client.AcknowledgeStorageQuarantineResolution(
		t.Context(), committed,
	); err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	if err := manager.AcknowledgeStorageQuarantineResolution(
		t.Context(), committed,
	); err != nil {
		t.Fatalf("acknowledgement replay after generation loss: %v", err)
	}
	if _, err := os.Stat(
		filepath.Join(manager.config.Database+".blobs", hex.EncodeToString(hash[:])),
	); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discarded quarantined blob still exists: %v", err)
	}
}

func TestManagedContentReaderEarlyCloseReapsGenerationBeforeReturn(t *testing.T) {
	manager, launcher := newTestManager(t)
	generation, err := manager.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	process := launcher.process(t, 0)
	stopGate := make(chan struct{})
	process.stopGate = stopGate
	blocked := newBlockingReadCloser(errors.New("blocked content read canceled"))
	reader := newManagedContentReader(t.Context(), blocked, manager, generation, 0, sha256.Sum256(nil))
	readDone := make(chan error, 1)
	go func() {
		_, readErr := reader.Read(make([]byte, 1))
		readDone <- readErr
	}()
	<-blocked.started
	closeDone := make(chan error, 1)
	go func() { closeDone <- reader.Close() }()
	<-process.stopStarted
	assertNoResult(t, closeDone, "early Close returned before exact worker reap")
	assertNoResult(t, readDone, "blocked Read returned before exact worker reap")
	replacement := make(chan error, 1)
	go func() {
		_, callErr := manager.TopologyHead(t.Context(), "test-owner")
		replacement <- callErr
	}()
	assertNoResult(t, replacement, "replacement generation admitted before exact worker reap")
	if count := launcher.count(); count != 1 {
		t.Fatalf("launched %d generations before reap, want 1", count)
	}
	close(stopGate)
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-readDone; err == nil {
		t.Fatal("blocked Read succeeded after early Close")
	}
	if err := <-replacement; err != nil {
		t.Fatalf("replacement generation: %v", err)
	}
	if !process.reaped() {
		t.Fatal("early Close returned before exact worker reap")
	}
}

func TestManagedContentReaderErrorReapsGenerationBeforeReadReturns(t *testing.T) {
	manager, launcher := newTestManager(t)
	generation, err := manager.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	process := launcher.process(t, 0)
	stopGate := make(chan struct{})
	process.stopGate = stopGate
	expired := make(chan struct{})
	reader := newManagedContentReader(
		t.Context(),
		&errorReadCloser{ready: expired, err: context.DeadlineExceeded},
		manager, generation, 0, sha256.Sum256(nil),
	)
	readDone := make(chan error, 1)
	go func() {
		_, readErr := reader.Read(make([]byte, 1))
		readDone <- readErr
	}()
	close(expired)
	<-process.stopStarted
	assertNoResult(t, readDone, "expired Read returned before exact worker reap")
	replacement := make(chan error, 1)
	go func() {
		_, callErr := manager.TopologyHead(t.Context(), "test-owner")
		replacement <- callErr
	}()
	assertNoResult(t, replacement, "replacement generation admitted before expired Read reap")
	close(stopGate)
	if err := <-readDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Read = %v, want context deadline exceeded", err)
	}
	if err := <-replacement; err != nil {
		t.Fatalf("replacement generation: %v", err)
	}
	if !process.reaped() {
		t.Fatal("expired Read returned before exact worker reap")
	}
}

func TestManagedContentReaderValidatedTerminalKeepsGeneration(t *testing.T) {
	manager, launcher := newTestManager(t)
	generation, err := manager.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	reader := newManagedContentReader(
		t.Context(),
		io.NopCloser(&terminalReader{}), manager, generation, 0, sha256.Sum256(nil),
	)
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("Read = %v, want EOF", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-launcher.process(t, 0).stopStarted:
		t.Fatal("validated terminal poisoned a healthy worker generation")
	default:
	}
}

func TestManagedContentReaderRejectsTruncatedAndCorruptSuccessfulStreams(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		expected string
	}{
		{name: "truncated", payload: "payloa", expected: "payload"},
		{name: "corrupt", payload: "payloae", expected: "payload"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager, launcher := newTestManager(t)
			generation, err := manager.acquire(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			expected := sha256.Sum256([]byte(test.expected))
			reader := newManagedContentReader(
				t.Context(),
				io.NopCloser(strings.NewReader(test.payload)),
				manager, generation, int64(len(test.expected)), expected,
			)
			if _, err := io.ReadAll(reader); !errors.Is(err, catalog.ErrIntegrity) {
				t.Fatalf("ReadAll = %v, want ErrIntegrity", err)
			}
			if !launcher.process(t, 0).reaped() {
				t.Fatal("integrity failure returned before exact worker reap")
			}
		})
	}
}

func TestManagedContentReaderCloseIsExactIdempotentAcrossConcurrentCallers(t *testing.T) {
	gate := make(chan struct{})
	closer := &countingCloser{gate: gate, started: make(chan struct{})}
	_, cancel := context.WithCancel(t.Context())
	reader := &managedContentReader{
		reader: strings.NewReader(""), closeReader: closer.Close,
		cancel: cancel, done: make(chan struct{}),
	}
	reader.clean.Store(true)

	const callers = 16
	results := make(chan error, callers)
	for range callers {
		go func() { results <- reader.Close() }()
	}
	select {
	case <-closer.started:
	case <-time.After(time.Second):
		t.Fatal("underlying close did not start")
	}
	time.Sleep(10 * time.Millisecond)
	if count := closer.count(); count != 1 {
		t.Fatalf("underlying close count while blocked = %d, want 1", count)
	}
	close(gate)
	for range callers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if count := closer.count(); count != 1 {
		t.Fatalf("underlying close count = %d, want 1", count)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if count := closer.count(); count != 1 {
		t.Fatalf("repeated underlying close count = %d, want 1", count)
	}
}

func TestManagedMutationContentSettleRemainsNonblockingAndWaitJoinsCleanup(t *testing.T) {
	manager, _ := newTestManager(t)
	generation, err := manager.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, cancel := context.WithCancel(t.Context())
	reader := &managedContentReader{
		reader: strings.NewReader(""), manager: manager, generation: generation,
		cancel: cancel, done: make(chan struct{}),
	}
	reader.clean.Store(true)
	waitErr := errors.New("wait failed")
	source := &delayedWaitSource{release: make(chan struct{}), waitErr: waitErr}
	content := newManagedMutationContent(reader, source)

	if err := content.Settle(nil); err != nil {
		t.Fatal(err)
	}
	repeated := make(chan error, 1)
	go func() { repeated <- content.Settle(nil) }()
	select {
	case err := <-repeated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("repeated Settle blocked on cleanup")
	}
	close(source.release)
	if err := content.Wait(t.Context()); !errors.Is(err, waitErr) {
		t.Fatalf("Wait = %v, want joined cleanup error", err)
	}
	if err := content.Settle(nil); !errors.Is(err, waitErr) {
		t.Fatalf("settled replay = %v, want joined cleanup error", err)
	}
}

func TestManagedMutationContentConcurrentSettleCachesExactTriggerError(t *testing.T) {
	manager := &Manager{config: ManagerConfig{StopTimeout: time.Second}, lifecycle: t.Context()}
	_, cancel := context.WithCancel(t.Context())
	reader := &managedContentReader{
		reader: strings.NewReader(""), manager: manager,
		cancel: cancel, done: make(chan struct{}),
	}
	reader.clean.Store(true)
	settleErr := errors.New("settle failed")
	source := &delayedWaitSource{
		release: make(chan struct{}), settleErr: settleErr,
	}
	content := newManagedMutationContent(reader, source)

	const callers = 16
	results := make(chan error, callers)
	for range callers {
		go func() { results <- content.Settle(nil) }()
	}
	for range callers {
		if err := <-results; !errors.Is(err, settleErr) {
			t.Fatalf("Settle = %v, want cached trigger error", err)
		}
	}
	if count := source.settleCount(); count != 1 {
		t.Fatalf("source settle count = %d, want 1", count)
	}
	close(source.release)
	if err := content.Wait(t.Context()); !errors.Is(err, settleErr) {
		t.Fatalf("Wait = %v, want cached trigger error", err)
	}
}

type countingCloser struct {
	gate    <-chan struct{}
	started chan struct{}
	once    sync.Once
	mu      sync.Mutex
	calls   int
}

func (c *countingCloser) Close() error {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	c.once.Do(func() {
		close(c.started)
	})
	<-c.gate
	return nil
}

func (c *countingCloser) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

type delayedWaitSource struct {
	release   chan struct{}
	waitErr   error
	settleErr error
	mu        sync.Mutex
	settles   int
}

func (*delayedWaitSource) Read([]byte) (int, error) { return 0, io.EOF }
func (s *delayedWaitSource) Settle(error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settles++
	return s.settleErr
}
func (s *delayedWaitSource) Wait(context.Context) error {
	<-s.release
	return s.waitErr
}
func (s *delayedWaitSource) settleCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settles
}

type blockingReadCloser struct {
	started   chan struct{}
	closed    chan struct{}
	readDone  chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
	err       error
}

func newBlockingReadCloser(err error) *blockingReadCloser {
	return &blockingReadCloser{
		started: make(chan struct{}), closed: make(chan struct{}), readDone: make(chan struct{}), err: err,
	}
}

func (r *blockingReadCloser) Read([]byte) (int, error) {
	r.startOnce.Do(func() { close(r.started) })
	<-r.closed
	close(r.readDone)
	return 0, r.err
}

func (r *blockingReadCloser) Settle(error) error {
	r.closeOnce.Do(func() { close(r.closed) })
	return nil
}

func (r *blockingReadCloser) Close() error { return r.Settle(nil) }

func (r *blockingReadCloser) Wait(ctx context.Context) error {
	select {
	case <-r.readDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type errorReadCloser struct {
	ready <-chan struct{}
	err   error
}

func (r *errorReadCloser) Read([]byte) (int, error) {
	<-r.ready
	return 0, r.err
}

func (*errorReadCloser) Settle(error) error         { return nil }
func (*errorReadCloser) Wait(context.Context) error { return nil }
func (*errorReadCloser) Close() error               { return nil }

type terminalReader struct{}

func (*terminalReader) Read([]byte) (int, error) { return 0, io.EOF }
func (*terminalReader) Settle(error) error       { return nil }
func (*terminalReader) Wait(context.Context) error {
	return nil
}

type ownedBlockingSource struct {
	started  chan struct{}
	settled  chan struct{}
	readDone chan struct{}

	startOnce  sync.Once
	settleOnce sync.Once
	mu         sync.Mutex
	settleErr  error
	didWait    bool
}

func newOwnedBlockingSource() *ownedBlockingSource {
	return &ownedBlockingSource{
		started: make(chan struct{}), settled: make(chan struct{}), readDone: make(chan struct{}),
	}
}

func (s *ownedBlockingSource) Read([]byte) (int, error) {
	s.startOnce.Do(func() { close(s.started) })
	<-s.settled
	s.mu.Lock()
	err := s.settleErr
	s.mu.Unlock()
	close(s.readDone)
	return 0, err
}

func (s *ownedBlockingSource) Settle(err error) error {
	s.settleOnce.Do(func() {
		s.mu.Lock()
		s.settleErr = err
		s.mu.Unlock()
		close(s.settled)
	})
	return nil
}

func (s *ownedBlockingSource) Wait(ctx context.Context) error {
	select {
	case <-s.readDone:
		s.mu.Lock()
		s.didWait = true
		s.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ownedBlockingSource) waited() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.didWait
}

func testTenantProvision(t *testing.T, name string) catalog.TenantProvision {
	t.Helper()
	tenantID, err := catalog.NewTenantID(name)
	if err != nil {
		t.Fatal(err)
	}
	return catalog.TenantProvision{
		Tenant: tenantID, OwnerID: "test",
		Mount: catalog.MountPresentation{PresentationRoot: "/tmp/" + name}, BackingRoot: "/tmp/" + name + "-backing",
		ContentSourceID: "test", Access: catalog.TenantReadWrite,
		CasePolicy: catalog.CaseSensitive, Presentations: catalog.PresentMount,
		Generation: 1,
	}
}

func testSourceRuntimeProcessRecord(t *testing.T) proc.Record {
	t.Helper()
	identity, err := proc.Probe(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	return proc.Record{
		RecoveryID: recoveryid.SourceOwner,
		PID:        identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Executable: identity.Executable, AuditToken: identity.AuditToken,
		Generation: catalogWorkerOwnerGeneration("source-runtime-test"), ProcessGroup: true, SessionID: identity.PID,
	}
}

func newTestManager(t *testing.T) (*Manager, *testProcessLauncher) {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "fcw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	launcher := newTestProcessLauncher(t)
	manager, err := NewManager(t.Context(), ManagerConfig{
		Executable: "/test/product-helper", Database: filepath.Join(directory, "catalog.sqlite"),
		ExpectedSignature: testChildSignature(),
		launcher:          launcher,
		ReadinessTimeout:  5 * time.Second, OperationTimeout: 10 * time.Second, StopTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager, launcher
}

func seedSourceObserverReceipts(
	t *testing.T,
	database string,
) (catalog.SourceObserverConfigurationRef, catalog.SourcePublicationStageRef) {
	t.Helper()
	store, err := catalog.Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	owner := catalog.SourceAuthorityFleetOwnerID("receipt-owner")
	authority := causal.SourceAuthorityID("receipt-authority")
	declarations, fleetDigest, declarationsDigest := testSourceAuthorityFleet(
		t, []causal.SourceAuthorityID{authority},
	)
	fleetStage, err := store.ReconcileSourceAuthorityFleet(
		t.Context(),
		catalog.SourceAuthorityFleetReconcileRequest{
			Owner: owner, Generation: 1, Declarations: declarations,
			Complete: true, AuthorityCount: 1, AuthoritiesDigest: fleetDigest,
			DeclarationsDigest: declarationsDigest,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcknowledgeSourceAuthorityFleet(
		t.Context(),
		catalog.SourceAuthorityFleetAcknowledgement{
			Owner: owner, Generation: 1, AuthorityCount: 1,
			AuthoritiesDigest: fleetDigest, DeclarationsDigest: declarationsDigest,
			StageDigest: fleetStage.StageDigest,
		},
	); err != nil {
		t.Fatal(err)
	}
	roots := []catalog.SourceObserverRootRecord{{
		ID: "root", Generation: 1, Path: "/root", VolumeUUID: "volume",
		Inode: 1, Kind: 1,
	}}
	checkpoints := []catalog.SourceObserverCheckpointRecord{{
		Stream: "stream", RootEpoch: "epoch",
	}}
	rootsDigest, err := catalog.SourceObserverRootsDigest(roots)
	if err != nil {
		t.Fatal(err)
	}
	checkpointsDigest, err := catalog.SourceObserverCheckpointsDigest(checkpoints)
	if err != nil {
		t.Fatal(err)
	}
	configurationOperation := causal.OperationID{1}
	identity := catalog.SourceObserverConfigurationIdentity{
		Authority: authority, FleetOwner: owner, FleetGeneration: 1,
		Operation: configurationOperation, Stream: "stream", RootEpoch: "epoch",
		RootDigest: [32]byte{1}, FleetDigest: fleetDigest,
		RootCount: uint64(len(roots)), CheckpointCount: uint64(len(checkpoints)),
		RootsDigest: rootsDigest, CheckpointsDigest: checkpointsDigest,
	}
	if err := store.BeginSourceObserverConfiguration(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	configuration, err := store.AppendSourceObserverConfigurationRoots(
		t.Context(), authority, configurationOperation,
		catalog.SourceObserverRootAppendPage{Records: roots},
	)
	if err != nil {
		t.Fatal(err)
	}
	configuration, err = store.AppendSourceObserverConfigurationCheckpoints(
		t.Context(), authority, configurationOperation,
		catalog.SourceObserverCheckpointAppendPage{
			Sequence: configuration.Sequence, Records: checkpoints,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := store.CommitSourceObserverConfiguration(t.Context(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	watermark, err := store.SourceWatermark(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	settlement := catalog.SourcePublicationStageRef{
		Authority: authority, FleetOwner: owner, FleetGeneration: 1,
		DriverID: declarations[0].DriverID, DeclarationDigest: declarations[0].DeclarationDigest,
		Operation: causal.OperationID{2},
		Through:   stream.LastApplied, Revision: watermark,
		Sequence: 1, Items: 1, Bytes: 1, Digest: [32]byte{3},
	}
	receiptDB, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := receiptDB.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if _, err := receiptDB.ExecContext(t.Context(), `
INSERT INTO source_observer_settlement_receipts(
    source_authority, fleet_owner_id, authority_generation, driver_id,
    declaration_digest, stage_operation_id, through_sequence, source_revision,
    stage_sequence, stage_item_count, stage_byte_count, stage_digest, receipt_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(settlement.Authority), string(settlement.FleetOwner),
		uint64(settlement.FleetGeneration), string(settlement.DriverID),
		settlement.DeclarationDigest[:], settlement.Operation[:], settlement.Through,
		uint64(settlement.Revision), settlement.Sequence, settlement.Items,
		settlement.Bytes, settlement.Digest[:], make([]byte, sha256.Size)); err != nil {
		t.Fatalf("seed source observer settlement receipt: %v", err)
	}
	return configuration, settlement
}

func seedStorageQuarantine(t *testing.T, database string) catalog.ContentHash {
	t.Helper()
	store, err := catalog.Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var owner []byte
	if err := db.QueryRowContext(t.Context(), `
SELECT owner_id
FROM catalog_generations
ORDER BY owner_id
LIMIT 1`).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	body := []byte("worker-storage-quarantine")
	hash := sha256.Sum256(body)
	name := hex.EncodeToString(hash[:])
	if err := os.WriteFile(
		filepath.Join(database+".blobs", name), body, 0o600,
	); err != nil {
		t.Fatal(err)
	}
	transition := catalog.StorageTransitionID{1}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO storage_entries(name, kind, state, size, hash)
VALUES (?, 2, 2, ?, ?)`,
		name, len(body), hash[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
UPDATE storage_accounting
SET published_bytes = ?, version = version + 1
WHERE singleton = 1`,
		len(body)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO storage_transitions(
    transition_id, kind, owner_id, source_name, hash, size,
    new_blob, quarantined, reason
) VALUES (?, 4, ?, ?, ?, ?, 0, 1, ?)`,
		transition[:], owner, name, hash[:], len(body),
		"operator repair required",
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return hash
}

func assertNoResult(t *testing.T, result <-chan error, failure string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("%s: %v", failure, err)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestCatalogWorkerSpawnedHelperProcess(t *testing.T) {
	if os.Getenv("CATALOG_WORKER_SPAWNED_HELPER") != "1" {
		t.Skip("helper body; runs only re-exec'd")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 || separator == len(os.Args)-1 {
		t.Fatal("catalog worker helper arguments are missing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	recognized, err := RunChild(ctx, os.Args[separator+1:])
	if !recognized {
		err = errors.New("catalog worker child mode was not recognized")
	}
	if err != nil {
		_ = os.WriteFile(os.Getenv("CATALOG_WORKER_SPAWNED_DIAGNOSTIC"), []byte(err.Error()), 0o600)
		t.Fatal(err)
	}
}

type testProcessLauncher struct {
	mu        sync.Mutex
	launcher  spawnedSessionLauncher
	processes []*testManagedProcess
	specs     []sessionProcessSpec
	readyGate <-chan struct{}
	started   chan *testManagedProcess
}

func newTestProcessLauncher(t *testing.T) *testProcessLauncher {
	t.Helper()
	manager, err := proc.NewManager(4, &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(t.TempDir(), "processes.db")},
		Generation: catalogWorkerOwnerGeneration("catalogworker-test"), Grace: 10 * time.Millisecond, Settlement: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ClaimRuntime(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Shutdown(ctx); err != nil {
			t.Errorf("shutdown process manager: %v", err)
		}
	})
	return &testProcessLauncher{launcher: spawnedSessionLauncher{manager: manager}}
}

func (l *testProcessLauncher) StartSession(
	ctx context.Context,
	spec sessionProcessSpec,
) (managedProcess, sessionClient, error) {
	executable, err := proc.ExecutablePath(os.Getpid())
	if err != nil {
		return nil, nil, err
	}
	forwarded := append([]string(nil), spec.Spawn.Args...)
	testSpec := spec
	testSpec.Spawn.Executable = executable
	testSpec.Spawn.Args = append(
		[]string{"-test.run=^TestCatalogWorkerSpawnedHelperProcess$", "-test.v", "--"},
		forwarded...,
	)
	diagnostic, err := os.CreateTemp("", "catalogworker-spawned-diagnostic-")
	if err != nil {
		return nil, nil, err
	}
	diagnosticPath := diagnostic.Name()
	if err := diagnostic.Close(); err != nil {
		return nil, nil, err
	}
	defer func() { _ = os.Remove(diagnosticPath) }()
	testSpec.Spawn.Env = append(
		append([]string(nil), spec.Spawn.Env...),
		"CATALOG_WORKER_SPAWNED_HELPER=1",
		"CATALOG_WORKER_SPAWNED_DIAGNOSTIC="+diagnosticPath,
	)
	signature := testChildSignature()
	testSpec.Spawn.ExpectedSignature = &signature
	process, client, err := l.launcher.StartSession(ctx, testSpec)
	if err != nil {
		diagnosticPayload, _ := os.ReadFile(diagnosticPath)
		return nil, nil, errors.Join(err, fmt.Errorf("catalog worker helper: %s", diagnosticPayload))
	}
	testProcess := &testManagedProcess{
		managedProcess: process, stopped: make(chan struct{}), stopStarted: make(chan struct{}),
	}
	l.mu.Lock()
	l.processes = append(l.processes, testProcess)
	l.specs = append(l.specs, spec)
	l.mu.Unlock()
	if l.started != nil {
		l.started <- testProcess
	}
	if l.readyGate != nil {
		select {
		case <-l.readyGate:
		case <-ctx.Done():
			return nil, nil, errors.Join(
				ctx.Err(), client.Abort(ctx.Err()), testProcess.Stop(context.WithoutCancel(ctx)),
			)
		}
	}
	return testProcess, client, nil
}

func (l *testProcessLauncher) spec(t *testing.T, index int) sessionProcessSpec {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if index >= len(l.specs) {
		t.Fatalf("process spec %d not started", index)
	}
	return l.specs[index]
}

func (l *testProcessLauncher) process(t *testing.T, index int) *testManagedProcess {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.processes) <= index {
		t.Fatalf("process %d was not launched", index)
	}
	return l.processes[index]
}

func (l *testProcessLauncher) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.processes)
}

type testManagedProcess struct {
	managedProcess
	stopped     chan struct{}
	stopStarted chan struct{}
	stopGate    <-chan struct{}
	stopErr     error

	mu       sync.Mutex
	didReap  bool
	stopOnce sync.Once
}

func (p *testManagedProcess) Stop(ctx context.Context) error {
	p.stopOnce.Do(func() {
		close(p.stopStarted)
		if p.stopGate != nil {
			<-p.stopGate
		}
		p.stopErr = errors.Join(p.stopErr, p.managedProcess.Stop(ctx))
		p.mu.Lock()
		p.didReap = true
		p.mu.Unlock()
		close(p.stopped)
	})
	return p.stopErr
}

func (p *testManagedProcess) reaped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.didReap
}

type noopFleet struct{}

func (noopFleet) Prepare(context.Context, tenant.FleetTransition) error { return nil }
func (noopFleet) Commit(context.Context, tenant.FleetTransition) error  { return nil }
func (noopFleet) Abort(context.Context, tenant.FleetTransition) error   { return nil }

type noopPlanner struct{}

func (noopPlanner) PrepareSourceMutation(context.Context, tenant.SourceMutationStep) (tenant.SourceMutationOperation, error) {
	return tenant.SourceMutationOperation{}, nil
}
func (noopPlanner) ApplySourceMutation(context.Context, tenant.SourceMutationStep, tenant.SourceMutationOperation, tenant.SourceMutationContent) (tenant.SourceMutationApplyResult, error) {
	return tenant.SourceMutationApplyResult{}, nil
}
func (noopPlanner) SourceMutationCommitted(context.Context, tenant.SourceMutationCommit) error {
	return nil
}
