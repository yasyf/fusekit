package catalog

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTenantStateCASGenerationAndQuarantine(t *testing.T) {
	c := newTestCatalog(t)
	tenant, _ := createTestTenant(t, c, "state", CaseSensitive)
	if _, err := c.LoadTenantState(context.Background(), tenant); !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("LoadTenantState before save err = %v, want ErrStateNotFound", err)
	}
	record := TenantStateRecord{
		Tenant: tenant, Generation: 1, Desired: 4, Observed: 3,
		Verified: 2, Applied: 1, Quarantine: testQuarantine(4),
	}
	saved, err := c.SaveTenantState(context.Background(), 0, record)
	if err != nil {
		t.Fatalf("SaveTenantState(insert): %v", err)
	}
	if saved.Version != 1 {
		t.Fatalf("inserted version = %d, want 1", saved.Version)
	}
	if replayed, err := c.SaveTenantState(context.Background(), 0, record); err != nil ||
		replayed.Version != saved.Version || !equalTenantStateValue(replayed, saved) {
		t.Fatalf("duplicate insert replay = %+v, %v; want %+v", replayed, err, saved)
	}

	next := saved
	next.Desired = 5
	next.Observed = 4
	next.Quarantine = nil
	next, err = c.SaveTenantState(context.Background(), saved.Version, next)
	if err != nil {
		t.Fatalf("SaveTenantState(update): %v", err)
	}
	if next.Version != 2 {
		t.Fatalf("updated version = %d, want 2", next.Version)
	}
	stale := saved
	if _, err := c.SaveTenantState(context.Background(), saved.Version, stale); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("stale CAS err = %v, want ErrStateConflict", err)
	}
	regressed := next
	regressed.Applied = 0
	if _, err := c.SaveTenantState(context.Background(), next.Version, regressed); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("same-generation regression err = %v, want ErrInvalidTransition", err)
	}
	newGeneration := next
	newGeneration.Generation = 2
	newGeneration.Desired = 1
	newGeneration.Observed = 0
	newGeneration.Verified = 0
	newGeneration.Applied = 0
	if _, err := c.SaveTenantState(context.Background(), next.Version, newGeneration); err != nil {
		t.Fatalf("new-generation reset: %v", err)
	}
	loaded, err := c.LoadTenantState(context.Background(), tenant)
	if err != nil {
		t.Fatalf("LoadTenantState: %v", err)
	}
	if loaded.Generation != 2 || loaded.Desired != 1 || loaded.Version != 3 {
		t.Fatalf("loaded state = %+v", loaded)
	}
}

func TestTenantStateRejectsIncompleteQuarantine(t *testing.T) {
	c := newTestCatalog(t)
	tenant, _ := createTestTenant(t, c, "bad-state", CaseSensitive)
	tests := []Quarantine{
		{Lane: 99, Revision: 1, Cause: QuarantineCauseConflict, Detail: "x", Since: time.Now()},
		{Lane: QuarantineLaneEnumeration, Revision: 1, Cause: 99, Detail: "x", Since: time.Now()},
		{Lane: QuarantineLaneEnumeration, Revision: 0, Cause: QuarantineCauseIntegrity, Detail: "x", Since: time.Now()},
		{Lane: QuarantineLaneEnumeration, Revision: 1, Cause: QuarantineCauseIntegrity, Detail: "", Since: time.Now()},
	}
	for i := range tests {
		record := TenantStateRecord{Tenant: tenant, Generation: 1, Desired: 1, Quarantine: &tests[i]}
		if _, err := c.SaveTenantState(context.Background(), 0, record); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("case %d err = %v, want ErrInvalidTransition", i, err)
		}
	}
}
