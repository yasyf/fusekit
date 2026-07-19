package catalogworker

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
)

func TestMaintenanceSchedulerRecoversPersistedDirtyClaimsAndRotatesFairly(t *testing.T) {
	first := catalog.MaintenanceTask{Tenant: "first", DirtyRevision: 3}
	second := catalog.MaintenanceTask{Tenant: "second", DirtyRevision: 4}
	store := newSchedulerTestStore(first, second)
	ctx, cancel := context.WithCancelCause(t.Context())
	var mutation sync.Mutex
	ticks := make(chan time.Time)
	scheduler, err := startMaintenanceScheduler(ctx, store, &mutation, ticks, cancel)
	if err != nil {
		t.Fatalf("startMaintenanceScheduler: %v", err)
	}
	select {
	case <-store.idle:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance scheduler did not drain")
	}
	cancel(context.Canceled)
	if err := scheduler.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.recovered {
		t.Fatal("startup claim recovery did not run")
	}
	want := []catalog.TenantID{"first", "second", "first"}
	if len(store.maintained) != len(want) {
		t.Fatalf("maintained = %v, want %v", store.maintained, want)
	}
	for index := range want {
		if store.maintained[index] != want[index] {
			t.Fatalf("maintained = %v, want %v", store.maintained, want)
		}
	}
	if store.globalCalls != 1 {
		t.Fatalf("global calls = %d, want 1 bounded sweep after tenant drain", store.globalCalls)
	}
}

func TestMaintenanceSchedulerFailureCancelsChild(t *testing.T) {
	boom := errors.New("maintenance failed")
	store := newSchedulerTestStore(catalog.MaintenanceTask{Tenant: "tenant", DirtyRevision: 2})
	store.maintainErr = boom
	ctx, cancel := context.WithCancelCause(t.Context())
	var mutation sync.Mutex
	scheduler, err := startMaintenanceScheduler(ctx, store, &mutation, make(chan time.Time), cancel)
	if err != nil {
		t.Fatalf("startMaintenanceScheduler: %v", err)
	}
	if err := scheduler.Wait(); !errors.Is(err, boom) {
		t.Fatalf("Wait = %v, want %v", err, boom)
	}
	if cause := context.Cause(ctx); !errors.Is(cause, boom) {
		t.Fatalf("child cancellation cause = %v, want %v", cause, boom)
	}
}

func TestMaintenanceSchedulerDrainsProductionCatalog(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := catalog.Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	provision := testTenantProvision(t, "maintenance-production")
	if _, err := store.ProvisionTenant(ctx, provision); err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	state, err := store.LoadTenantState(ctx, provision.Tenant)
	if err != nil {
		t.Fatalf("LoadTenantState: %v", err)
	}
	state.ActivatedGeneration = state.Generation
	state.Desired = 1
	state.Observed = 1
	state.Verified = 1
	state.Applied = 1
	if _, err := store.SaveTenantState(ctx, state.Version, state); err != nil {
		t.Fatalf("SaveTenantState: %v", err)
	}
	var mutation sync.Mutex
	scheduler, err := startMaintenanceScheduler(
		ctx, store, &mutation, make(chan time.Time), cancel,
	)
	if err != nil {
		t.Fatalf("startMaintenanceScheduler: %v", err)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		floor, err := store.CompactionFloor(ctx, provision.Tenant)
		if err != nil {
			t.Fatalf("CompactionFloor: %v", err)
		}
		if floor == 1 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("compaction floor = %d, want 1", floor)
		case <-poll.C:
		}
	}
	cancel(context.Canceled)
	if err := scheduler.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestMaintenanceSchedulerDoesNotScanCleanTenThousandTenantFleetAtStartup(t *testing.T) {
	store := &cleanFleetMaintenanceStore{tenants: 10_000, idle: make(chan struct{})}
	ctx, cancel := context.WithCancelCause(t.Context())
	var mutation sync.Mutex
	scheduler, err := startMaintenanceScheduler(
		ctx, store, &mutation, make(chan time.Time), cancel,
	)
	if err != nil {
		t.Fatalf("startMaintenanceScheduler: %v", err)
	}
	select {
	case <-store.idle:
	case <-time.After(5 * time.Second):
		t.Fatal("clean-fleet startup did not settle")
	}
	cancel(context.Canceled)
	if err := scheduler.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if store.tenantCalls != 0 || store.globalCalls != 1 {
		t.Fatalf("clean fleet calls tenant/global = %d/%d, want 0/1", store.tenantCalls, store.globalCalls)
	}
}

func TestMaintenanceSchedulerRunsExpiredLeaseGlobalWorkAtStartup(t *testing.T) {
	store := &cleanFleetMaintenanceStore{globalWork: 1, idle: make(chan struct{})}
	ctx, cancel := context.WithCancelCause(t.Context())
	var mutation sync.Mutex
	scheduler, err := startMaintenanceScheduler(
		ctx, store, &mutation, make(chan time.Time), cancel,
	)
	if err != nil {
		t.Fatalf("startMaintenanceScheduler: %v", err)
	}
	select {
	case <-store.idle:
	case <-time.After(5 * time.Second):
		t.Fatal("startup global lease maintenance did not settle")
	}
	cancel(context.Canceled)
	if err := scheduler.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if store.tenantCalls != 0 || store.globalCalls != 2 {
		t.Fatalf("lease startup calls tenant/global = %d/%d, want 0/2", store.tenantCalls, store.globalCalls)
	}
}

type cleanFleetMaintenanceStore struct {
	tenants     int
	globalWork  int
	tenantCalls int
	globalCalls int
	idle        chan struct{}
	idleOnce    sync.Once
}

func (*cleanFleetMaintenanceStore) RecoverMaintenanceClaims(context.Context) error { return nil }

func (*cleanFleetMaintenanceStore) ClaimMaintenance(
	context.Context,
) (catalog.MaintenanceTask, bool, error) {
	return catalog.MaintenanceTask{}, false, nil
}

func (s *cleanFleetMaintenanceStore) MaintainTenant(
	context.Context,
	catalog.TenantID,
	time.Time,
) (catalog.MaintenanceResult, error) {
	s.tenantCalls++
	return catalog.MaintenanceResult{}, errors.New("clean fleet tenant was scanned")
}

func (*cleanFleetMaintenanceStore) FinishMaintenance(
	context.Context,
	catalog.MaintenanceTask,
	bool,
) error {
	return errors.New("clean fleet maintenance claim was settled")
}

func (s *cleanFleetMaintenanceStore) MaintainGlobal(
	context.Context,
	time.Time,
) (catalog.GlobalMaintenanceResult, error) {
	s.globalCalls++
	if s.globalWork > 0 {
		s.globalWork--
		return catalog.GlobalMaintenanceResult{
			Phase: catalog.MaintenanceFileProviderLeases, Retired: 1, More: true,
		}, nil
	}
	s.idleOnce.Do(func() { close(s.idle) })
	return catalog.GlobalMaintenanceResult{}, nil
}

func (*cleanFleetMaintenanceStore) EnforceWorkerWALBudget(context.Context) error { return nil }

type schedulerTestStore struct {
	mu sync.Mutex

	queue       []catalog.MaintenanceTask
	maintained  []catalog.TenantID
	calls       map[catalog.TenantID]int
	globalCalls int
	recovered   bool
	maintainErr error
	emptyClaim  bool
	idle        chan struct{}
	idleOnce    sync.Once
}

func newSchedulerTestStore(tasks ...catalog.MaintenanceTask) *schedulerTestStore {
	return &schedulerTestStore{
		queue: append([]catalog.MaintenanceTask(nil), tasks...),
		calls: make(map[catalog.TenantID]int),
		idle:  make(chan struct{}),
	}
}

func (s *schedulerTestStore) RecoverMaintenanceClaims(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recovered = true
	return nil
}

func (s *schedulerTestStore) ClaimMaintenance(context.Context) (catalog.MaintenanceTask, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		s.emptyClaim = true
		return catalog.MaintenanceTask{}, false, nil
	}
	s.emptyClaim = false
	task := s.queue[0]
	s.queue = s.queue[1:]
	return task, true, nil
}

func (s *schedulerTestStore) MaintainTenant(
	_ context.Context,
	tenant catalog.TenantID,
	_ time.Time,
) (catalog.MaintenanceResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maintainErr != nil {
		return catalog.MaintenanceResult{}, s.maintainErr
	}
	s.maintained = append(s.maintained, tenant)
	s.calls[tenant]++
	return catalog.MaintenanceResult{More: tenant == "first" && s.calls[tenant] == 1}, nil
}

func (s *schedulerTestStore) FinishMaintenance(
	_ context.Context,
	task catalog.MaintenanceTask,
	more bool,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if more {
		s.queue = append(s.queue, task)
	}
	return nil
}

func (s *schedulerTestStore) MaintainGlobal(
	context.Context,
	time.Time,
) (catalog.GlobalMaintenanceResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.globalCalls++
	if s.emptyClaim {
		s.idleOnce.Do(func() { close(s.idle) })
	}
	return catalog.GlobalMaintenanceResult{}, nil
}

func (*schedulerTestStore) EnforceWorkerWALBudget(context.Context) error { return nil }
