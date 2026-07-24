package holder

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
)

const (
	criticalWorkerVMOptIn       = "FUSEKIT_VM_PROCESS_KILL"
	criticalWorkerIgnoreTERM    = "FUSEKIT_TEST_CRITICAL_IGNORE_TERM"
	criticalWorkerStartedMarker = "FUSEKIT_TEST_CRITICAL_STARTED_MARKER"
)

func TestCriticalReadinessRejectsStaleChallengeAndRequiresFreshLeaseWork(t *testing.T) {
	scheduled := make(chan catalogproto.CriticalReadinessProof, 2)
	scheduler := criticalSchedulerFunc(func(
		_ context.Context,
		readiness catalogproto.CriticalReadinessProof,
	) ([]catalogproto.CriticalMaterializationPath, error) {
		scheduled <- readiness
		return adversarialCriticalPaths(readiness), nil
	})
	coordinator, err := newCriticalReadinessCoordinator(
		t.Context(), scheduler, successfulCriticalRunner(), "/Applications/Test.app/Contents/MacOS/Test",
	)
	if err != nil {
		t.Fatal(err)
	}
	authorization, proof := adversarialCriticalPreparationProof("lease-one", time.Now().Add(time.Minute))
	first := prepareCriticalAsync(t, coordinator, proof)
	firstScheduled := awaitScheduledCriticalReadiness(t, scheduled)
	firstContext := resolveCriticalFetch(t, coordinator, authorization, firstScheduled)

	stale := criticalAckRequest(firstScheduled, firstContext)
	stale.ReadChallenge = strings.Repeat("f", sha256.Size*2)
	if stale.ReadChallenge == firstContext.ReadChallenge {
		stale.ReadChallenge = strings.Repeat("e", sha256.Size*2)
	}
	if err := coordinator.AckCriticalFetch(
		t.Context(), catalogservice.Identity{}, authorization,
		catalog.TenantID(firstScheduled.Lease.TenantID), stale,
	); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("stale challenge acknowledgement = %v, want integrity error", err)
	}
	select {
	case result := <-first:
		t.Fatalf("stale challenge settled preparation: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}
	ackCriticalFetch(t, coordinator, authorization, firstScheduled, firstContext)
	firstResult := awaitCriticalPreparation(t, first)
	if firstResult.err != nil {
		t.Fatalf("first PrepareCriticalReadiness: %v", firstResult.err)
	}
	if firstResult.proof.CriticalReadiness == nil ||
		firstResult.proof.CriticalReadiness.ReadChallenge != firstContext.ReadChallenge ||
		firstResult.proof.CriticalReadiness.ReadProofDigest == nil {
		t.Fatalf("first critical readiness proof = %+v", firstResult.proof.CriticalReadiness)
	}
	if err := coordinator.AckCriticalFetch(
		t.Context(), catalogservice.Identity{}, authorization,
		catalog.TenantID(firstScheduled.Lease.TenantID), criticalAckRequest(firstScheduled, firstContext),
	); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("completed challenge replay = %v, want not found", err)
	}

	_, secondProof := adversarialCriticalPreparationProof("lease-two", time.Now().Add(time.Minute))
	second := prepareCriticalAsync(t, coordinator, secondProof)
	secondScheduled := awaitScheduledCriticalReadiness(t, scheduled)
	if secondScheduled.ReadChallenge == firstScheduled.ReadChallenge {
		t.Fatalf("new lease reused stale challenge %q", secondScheduled.ReadChallenge)
	}
	secondContext := resolveCriticalFetch(t, coordinator, authorization, secondScheduled)
	staleForSecond := criticalAckRequest(secondScheduled, secondContext)
	staleForSecond.LeaseID = firstContext.LeaseID
	staleForSecond.ReadChallenge = firstContext.ReadChallenge
	if err := coordinator.AckCriticalFetch(
		t.Context(), catalogservice.Identity{}, authorization,
		catalog.TenantID(secondScheduled.Lease.TenantID), staleForSecond,
	); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("prior lease challenge on fresh work = %v, want integrity error", err)
	}
	ackCriticalFetch(t, coordinator, authorization, secondScheduled, secondContext)
	if result := awaitCriticalPreparation(t, second); result.err != nil {
		t.Fatalf("second PrepareCriticalReadiness: %v", result.err)
	}
}

func TestCriticalReadinessDoesNotReleaseWorkBeforeWorkerSettlement(t *testing.T) {
	workerStarted := make(chan struct{})
	workerCanceled := make(chan struct{})
	workerReaped := make(chan struct{})
	secondRun := errors.New("second worker run")
	var runs atomic.Int32
	runner := criticalWorkerRunnerFunc(func(ctx context.Context, _ worker.CommandRequest) (worker.CommandResult, error) {
		if runs.Add(1) != 1 {
			return worker.CommandResult{}, secondRun
		}
		close(workerStarted)
		<-ctx.Done()
		close(workerCanceled)
		<-workerReaped
		return worker.CommandResult{}, errors.Join(worker.ErrTimedOut, ctx.Err())
	})
	scheduler := criticalSchedulerFunc(func(
		_ context.Context,
		readiness catalogproto.CriticalReadinessProof,
	) ([]catalogproto.CriticalMaterializationPath, error) {
		return adversarialCriticalPaths(readiness), nil
	})
	coordinator, err := newCriticalReadinessCoordinator(
		context.Background(), scheduler, runner, "/Applications/Test.app/Contents/MacOS/Test",
	)
	if err != nil {
		t.Fatal(err)
	}
	_, proof := adversarialCriticalPreparationProof("settlement-one", time.Now().Add(250*time.Millisecond))
	first := prepareCriticalAsync(t, coordinator, proof)
	awaitSignal(t, workerStarted, "critical worker start")
	awaitSignal(t, workerCanceled, "critical worker cancellation")
	select {
	case result := <-first:
		t.Fatalf("preparation returned before reap settlement: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}
	coordinator.mu.Lock()
	activeWorks := len(coordinator.works)
	activeWaiters := len(coordinator.waiters)
	coordinator.mu.Unlock()
	if activeWorks != 1 || activeWaiters != 1 {
		t.Fatalf("unsettled worker state: works=%d waiters=%d", activeWorks, activeWaiters)
	}

	close(workerReaped)
	settled := awaitCriticalPreparation(t, first)
	if !errors.Is(settled.err, worker.ErrTimedOut) || !errors.Is(settled.err, context.DeadlineExceeded) {
		t.Fatalf("settled worker error = %v", settled.err)
	}
	coordinator.mu.Lock()
	activeWorks = len(coordinator.works)
	activeWaiters = len(coordinator.waiters)
	coordinator.mu.Unlock()
	if activeWorks != 0 || activeWaiters != 0 {
		t.Fatalf("settled worker retained state: works=%d waiters=%d", activeWorks, activeWaiters)
	}

	_, retryProof := adversarialCriticalPreparationProof("settlement-two", time.Now().Add(time.Minute))
	retry := awaitCriticalPreparation(t, prepareCriticalAsync(t, coordinator, retryProof))
	if !errors.Is(retry.err, secondRun) || runs.Load() != 2 {
		t.Fatalf("retry after settlement = %v runs=%d", retry.err, runs.Load())
	}
}

func TestCriticalReadWorkerTimeoutKillsReapsAndReleasesClaim(t *testing.T) {
	if os.Getenv(criticalWorkerVMOptIn) != "1" {
		t.Skip("live process termination and reap proof is VM-only")
	}
	dir := t.TempDir()
	marker := filepath.Join(dir, "critical-child-started")
	t.Setenv(criticalWorkerIgnoreTERM, "1")
	t.Setenv(criticalWorkerStartedMarker, marker)
	fifo := filepath.Join(dir, "blocked-critical-read")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	pool, claim := newAdversarialCriticalWorkerPool(t)
	claimClosed := false
	t.Cleanup(func() {
		if claimClosed {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := claim.Close(ctx); err != nil {
			t.Errorf("close worker claim: %v", err)
		}
	})
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	readiness := adversarialCriticalPreparationProofReadiness("vm-worker", time.Now().Add(time.Minute))
	paths := []catalogproto.CriticalMaterializationPath{{ObjectID: readiness.Objects[0].ObjectID, Path: fifo}}
	type workerOutcome struct {
		result worker.CommandResult
		err    error
	}
	workerResult := make(chan workerOutcome, 1)
	runner := criticalWorkerRunnerFunc(func(ctx context.Context, request worker.CommandRequest) (worker.CommandResult, error) {
		result, runErr := pool.Run(ctx, request)
		workerResult <- workerOutcome{result: result, err: runErr}
		return result, runErr
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2300*time.Millisecond)
	defer cancel()
	_, err = runCriticalPathReads(ctx, runner, executable, readiness, paths)
	if !errors.Is(err, worker.ErrTimedOut) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("critical read timeout = %v", err)
	}
	result := <-workerResult
	if !errors.Is(result.err, worker.ErrTimedOut) || result.result.Receipt.ProcessIdentity().PID == 0 {
		t.Fatalf("worker timeout outcome = %+v, %v", result.result, result.err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("same-app critical child did not enter TERM-ignore mode: %v", err)
	}
	identity := result.result.Receipt.ProcessIdentity()
	current, probeErr := proc.Probe(identity.PID)
	if probeErr == nil && current.StartTime == identity.StartTime && current.Boot == identity.Boot {
		t.Fatalf("worker returned while exact process remained live: %+v", current)
	}
	if probeErr != nil && !errors.Is(probeErr, proc.ErrNoProcess) {
		t.Fatalf("probe reaped worker pid %d: %v", identity.PID, probeErr)
	}
	if active := pool.Active(); active != 0 {
		t.Fatalf("timed-out critical worker retained capacity: %d", active)
	}

	content := []byte("ready")
	readyPath := filepath.Join(dir, "ready-critical-read")
	if err := os.WriteFile(readyPath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(content)
	readiness.Objects[0].Size = uint64(len(content))
	readiness.Objects[0].Hash = fmt.Sprintf("%x", hash)
	paths[0].Path = readyPath
	if _, err := runCriticalPathReads(context.Background(), pool, executable, readiness, paths); err != nil {
		t.Fatalf("worker lane was not reusable after exact reap: %v", err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer closeCancel()
	if err := claim.Close(closeCtx); err != nil {
		t.Fatalf("worker claim remained retained: %v", err)
	}
	claimClosed = true
}

type criticalSchedulerFunc func(
	context.Context,
	catalogproto.CriticalReadinessProof,
) ([]catalogproto.CriticalMaterializationPath, error)

func (f criticalSchedulerFunc) ScheduleCriticalMaterialization(
	ctx context.Context,
	readiness catalogproto.CriticalReadinessProof,
) ([]catalogproto.CriticalMaterializationPath, error) {
	return f(ctx, readiness)
}

type criticalWorkerRunnerFunc func(context.Context, worker.CommandRequest) (worker.CommandResult, error)

func (f criticalWorkerRunnerFunc) Run(
	ctx context.Context,
	request worker.CommandRequest,
) (worker.CommandResult, error) {
	return f(ctx, request)
}

type criticalPreparationResult struct {
	proof catalogproto.TenantPreparationProof
	err   error
}

func prepareCriticalAsync(
	t *testing.T,
	coordinator *criticalReadinessCoordinator,
	proof catalogproto.TenantPreparationProof,
) <-chan criticalPreparationResult {
	t.Helper()
	done := make(chan criticalPreparationResult, 1)
	go func() {
		prepared, err := coordinator.PrepareCriticalReadiness(t.Context(), proof)
		done <- criticalPreparationResult{proof: prepared, err: err}
	}()
	return done
}

func awaitCriticalPreparation(
	t *testing.T,
	done <-chan criticalPreparationResult,
) criticalPreparationResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("critical readiness preparation did not settle")
		return criticalPreparationResult{}
	}
}

func awaitScheduledCriticalReadiness(
	t *testing.T,
	scheduled <-chan catalogproto.CriticalReadinessProof,
) catalogproto.CriticalReadinessProof {
	t.Helper()
	select {
	case readiness := <-scheduled:
		if len(readiness.ReadChallenge) != sha256.Size*2 || readiness.ReadProofDigest != nil {
			t.Fatalf("scheduled critical readiness = %+v", readiness)
		}
		return readiness
	case <-time.After(time.Second):
		t.Fatal("critical materialization was not scheduled")
		return catalogproto.CriticalReadinessProof{}
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func successfulCriticalRunner() workerRunner {
	return criticalWorkerRunnerFunc(func(_ context.Context, request worker.CommandRequest) (worker.CommandResult, error) {
		var task criticalReadTask
		if err := json.Unmarshal(request.Stdin, &task); err != nil {
			return worker.CommandResult{}, err
		}
		result := criticalReadResult{Objects: make([]criticalReadObservation, len(task.Objects))}
		for index, object := range task.Objects {
			result.Objects[index] = criticalReadObservation{ObjectID: object.ObjectID, Size: object.Size, Hash: object.Hash}
		}
		output, err := json.Marshal(result)
		if err != nil {
			return worker.CommandResult{}, err
		}
		return worker.CommandResult{Stdout: append(output, '\n')}, nil
	})
}

func adversarialCriticalPreparationProof(
	leaseID string,
	expires time.Time,
) (catalogservice.Authorization, catalogproto.TenantPreparationProof) {
	readiness := adversarialCriticalPreparationProofReadiness(leaseID, expires)
	proof := catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: readiness.Lease.TenantID, Generation: readiness.TenantGeneration,
			Requested: readiness.CatalogHead, Desired: readiness.CatalogHead,
			Observed: readiness.CatalogHead, Verified: readiness.CatalogHead, Applied: readiness.CatalogHead,
		},
		Presentation: catalogproto.PresentationProof{
			Kind: catalogproto.PresentationKindFileProvider,
			FileProvider: &catalogproto.FileProviderPresentationProof{
				TenantID: readiness.Lease.TenantID, DomainID: readiness.DomainID,
				Generation: readiness.TenantGeneration, PublicPath: "/File Provider/Test",
				ActivationGeneration:   readiness.ActivationGeneration,
				PresentationInstanceID: readiness.PresentationInstanceID, RootID: readiness.RootID,
			},
		},
		SourceAuthority:   readiness.Lease.SourceAuthority,
		SourcePublication: readiness.Lease.SourcePublication, SourceRevision: readiness.SourceRevision,
		CatalogRevision: readiness.CatalogHead,
		ChangeID:        "11111111111111111111111111111111", OperationID: "22222222222222222222222222222222",
		CriticalReadiness: &readiness,
	}
	authorization := catalogservice.Authorization{
		Principal: "file-provider", Role: catalogservice.RoleFileProvider,
		Presentation: catalog.PresentationFileProvider,
		Route: catalogservice.Route{
			Tenant: catalog.TenantID(readiness.Lease.TenantID), Generation: catalog.Generation(readiness.TenantGeneration),
			Domain: readiness.DomainID, Forwarded: true,
		},
	}
	return authorization, proof
}

func adversarialCriticalPreparationProofReadiness(
	leaseID string,
	expires time.Time,
) catalogproto.CriticalReadinessProof {
	domain, err := catalogproto.DeriveDomainID("owner", "presentation")
	if err != nil {
		panic(err)
	}
	policyDigest := strings.Repeat("a", sha256.Size*2)
	resolutionDigest := strings.Repeat("b", sha256.Size*2)
	return catalogproto.CriticalReadinessProof{
		PolicyDigest: policyDigest, ResolutionDigest: resolutionDigest,
		CatalogHead: 12, SourceRevision: 8, TenantGeneration: 1,
		DomainID: domain, PresentationInstanceID: "presentation",
		RootID: catalogproto.ObjectID(strings.Repeat("1", 32)), ActivationGeneration: "activation-test",
		Lease: catalogproto.FileProviderLeaseReceipt{
			LeaseID: leaseID, TenantID: "tenant", DomainID: domain, Generation: 1,
			RootID: catalogproto.ObjectID(strings.Repeat("1", 32)), PresentationInstanceID: "presentation",
			State:        catalogproto.FileProviderLeaseStateProvisional,
			PolicyDigest: policyDigest, ResolutionDigest: resolutionDigest,
			CatalogHead: 12, SourceAuthority: "source-main",
			SourcePublication: "33333333333333333333333333333333", SourceRevision: 8,
			ActivationGeneration: "activation-test", ExpiresUnixNano: uint64(expires.UnixNano()),
		},
		Objects: []catalogproto.ResolvedCriticalObjectProof{{
			LogicalID: "settings", Role: "settings", ObjectID: catalogproto.ObjectID(strings.Repeat("2", 32)),
			ObjectRevision: 11, ContentRevision: 10, Size: 8, Hash: strings.Repeat("c", sha256.Size*2),
		}},
	}
}

func adversarialCriticalPaths(
	readiness catalogproto.CriticalReadinessProof,
) []catalogproto.CriticalMaterializationPath {
	return []catalogproto.CriticalMaterializationPath{{
		ObjectID: readiness.Objects[0].ObjectID, Path: "/File Provider/Test/settings.json",
	}}
}

func resolveCriticalFetch(
	t *testing.T,
	coordinator *criticalReadinessCoordinator,
	authorization catalogservice.Authorization,
	readiness catalogproto.CriticalReadinessProof,
) catalogproto.CriticalFetchContext {
	t.Helper()
	object := readiness.Objects[0]
	resolved, err := coordinator.ResolveCriticalFetch(
		t.Context(), catalogservice.Identity{}, authorization, catalog.TenantID(readiness.Lease.TenantID),
		catalogproto.ResolveCriticalFetchRequest{
			Protocol: catalogproto.Version, Generation: readiness.TenantGeneration,
			ObjectID: object.ObjectID, ObjectRevision: object.ObjectRevision,
			ContentRevision: object.ContentRevision, Size: object.Size, Hash: object.Hash,
		},
	)
	if err != nil {
		t.Fatalf("ResolveCriticalFetch: %v", err)
	}
	if resolved == nil {
		t.Fatal("ResolveCriticalFetch returned no pending context")
	}
	return *resolved
}

func criticalAckRequest(
	readiness catalogproto.CriticalReadinessProof,
	context catalogproto.CriticalFetchContext,
) catalogproto.AckCriticalFetchRequest {
	object := readiness.Objects[0]
	return catalogproto.AckCriticalFetchRequest{
		Protocol: catalogproto.Version, Generation: readiness.TenantGeneration,
		ObjectID: object.ObjectID, ObjectRevision: object.ObjectRevision,
		ContentRevision: object.ContentRevision, Size: object.Size,
		Hash: object.Hash, ReadHash: object.Hash,
		LeaseID: context.LeaseID, ResolutionDigest: context.ResolutionDigest, ReadChallenge: context.ReadChallenge,
	}
}

func ackCriticalFetch(
	t *testing.T,
	coordinator *criticalReadinessCoordinator,
	authorization catalogservice.Authorization,
	readiness catalogproto.CriticalReadinessProof,
	context catalogproto.CriticalFetchContext,
) {
	t.Helper()
	if err := coordinator.AckCriticalFetch(
		t.Context(), catalogservice.Identity{}, authorization,
		catalog.TenantID(readiness.Lease.TenantID), criticalAckRequest(readiness, context),
	); err != nil {
		t.Fatalf("AckCriticalFetch: %v", err)
	}
}

func newAdversarialCriticalWorkerPool(t *testing.T) (*worker.Pool, *worker.RuntimeClaim) {
	t.Helper()
	reaper := &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(t.TempDir(), "critical-workers.db")},
		Generation: holderOwnerGeneration("critical-worker-" + strings.ReplaceAll(t.Name(), "/", "-")),
		Grace:      10 * time.Millisecond, Settlement: 2 * time.Second,
	}
	pool, err := worker.NewPool(worker.Config{
		Capacity: 1, QueueCapacity: 1, MaxTotalRun: criticalReadTimeout,
		MaxStdinBytes: criticalReadInputLimit, MaxStdoutBytes: 1 << 20, MaxStderrBytes: 1 << 20,
	}, reaper)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	claim, err := pool.ClaimRuntime()
	if err != nil {
		t.Fatalf("ClaimRuntime: %v", err)
	}
	if err := claim.Recover(t.Context()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if err := claim.Activate(); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	return pool, claim
}

func configureCriticalReadWorkerTestChild(arguments []string) error {
	if len(arguments) != 1 || arguments[0] != criticalReadChildArgument || os.Getenv(criticalWorkerIgnoreTERM) != "1" {
		return nil
	}
	signal.Ignore(syscall.SIGTERM)
	marker := os.Getenv(criticalWorkerStartedMarker)
	if marker == "" {
		return errors.New("critical read worker test marker is empty")
	}
	if err := os.WriteFile(marker, []byte("started"), 0o600); err != nil {
		return fmt.Errorf("write critical read worker test marker: %w", err)
	}
	return nil
}

var (
	_ criticalMaterializationScheduler = criticalSchedulerFunc(nil)
	_ workerRunner                     = criticalWorkerRunnerFunc(nil)
)
