package catalogworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/fusekit/catalog"
)

const (
	catalogMaintenanceInterval = time.Minute
	tenantTurnsBeforeGlobal    = 16
)

type maintenanceStore interface {
	RecoverMaintenanceClaims(context.Context) error
	ClaimMaintenance(context.Context) (catalog.MaintenanceTask, bool, error)
	MaintainTenant(context.Context, catalog.TenantID, time.Time) (catalog.MaintenanceResult, error)
	FinishMaintenance(context.Context, catalog.MaintenanceTask, bool) error
	MaintainGlobal(context.Context, time.Time) (catalog.GlobalMaintenanceResult, error)
	EnforceWorkerWALBudget(context.Context) error
}

type maintenanceScheduler struct {
	ctx      context.Context
	store    maintenanceStore
	mutation *sync.Mutex
	ticks    <-chan time.Time
	fatal    context.CancelCauseFunc

	wake chan struct{}
	done chan struct{}
	err  error
}

func startMaintenanceScheduler(
	ctx context.Context,
	store maintenanceStore,
	mutation *sync.Mutex,
	ticks <-chan time.Time,
	fatal context.CancelCauseFunc,
) (*maintenanceScheduler, error) {
	if ctx == nil || store == nil || mutation == nil || ticks == nil || fatal == nil {
		return nil, errors.New("catalog worker: complete maintenance scheduler dependencies are required")
	}
	if err := store.RecoverMaintenanceClaims(ctx); err != nil {
		return nil, fmt.Errorf("catalog worker: recover maintenance claims: %w", err)
	}
	scheduler := &maintenanceScheduler{
		ctx: ctx, store: store, mutation: mutation, ticks: ticks, fatal: fatal,
		wake: make(chan struct{}, 1), done: make(chan struct{}),
	}
	go scheduler.run()
	scheduler.Wake()
	return scheduler, nil
}

// Wake schedules a maintenance drain without blocking a catalog request.
func (s *maintenanceScheduler) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *maintenanceScheduler) Wait() error {
	<-s.done
	return s.err
}

func (s *maintenanceScheduler) run() {
	defer close(s.done)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
		case <-s.ticks:
		}
		if err := s.drain(); err != nil {
			s.err = err
			s.fatal(err)
			return
		}
	}
}

func (s *maintenanceScheduler) drain() error {
	tenantTurns := 0
	for {
		if err := s.ctx.Err(); err != nil {
			return nil
		}
		found, err := s.maintainTenant()
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			return err
		}
		if s.ctx.Err() != nil {
			return nil
		}
		tenantTurns++
		if found && tenantTurns < tenantTurnsBeforeGlobal {
			continue
		}
		tenantTurns = 0
		global, err := s.maintainGlobal()
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			return err
		}
		if !found && !global {
			return nil
		}
	}
}

func (s *maintenanceScheduler) maintainTenant() (bool, error) {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	task, found, err := s.store.ClaimMaintenance(s.ctx)
	if err != nil {
		return false, fmt.Errorf("catalog worker: claim tenant maintenance: %w", err)
	}
	if !found {
		return false, nil
	}
	if err := s.enforceWAL(); err != nil {
		return false, err
	}
	result, err := s.store.MaintainTenant(s.ctx, task.Tenant, time.Now().UTC())
	if err != nil {
		return false, fmt.Errorf("catalog worker: maintain tenant %q: %w", task.Tenant, err)
	}
	if err := s.store.FinishMaintenance(s.ctx, task, result.More); err != nil {
		return false, fmt.Errorf("catalog worker: finish tenant %q maintenance: %w", task.Tenant, err)
	}
	if err := s.enforceWAL(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *maintenanceScheduler) maintainGlobal() (bool, error) {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	if err := s.enforceWAL(); err != nil {
		return false, err
	}
	result, err := s.store.MaintainGlobal(s.ctx, time.Now().UTC())
	if err != nil {
		return false, fmt.Errorf("catalog worker: maintain global catalog state: %w", err)
	}
	if err := s.enforceWAL(); err != nil {
		return false, err
	}
	return result.More, nil
}

func (s *maintenanceScheduler) enforceWAL() error {
	ctx, cancel := context.WithTimeout(s.ctx, workerWALTimeout)
	defer cancel()
	if err := s.store.EnforceWorkerWALBudget(ctx); err != nil {
		return fmt.Errorf("catalog worker: maintenance WAL recovery: %w", err)
	}
	return nil
}
