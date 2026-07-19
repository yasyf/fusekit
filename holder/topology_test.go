package holder

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/tenant"
)

func TestTopologyFleetTransitionFencesCapabilityChangesByGeneration(t *testing.T) {
	mount := topologyTenantSpec("mount", catalog.PresentMount)
	fileProvider := topologyTenantSpec("fp", catalog.PresentMount|catalog.PresentFileProvider)
	for _, test := range []struct {
		name       string
		configured bool
		committed  []tenant.TenantSpec
		wantErr    bool
	}{
		{name: "mount stays mount", committed: []tenant.TenantSpec{mount}},
		{name: "mount requires fp restart", committed: []tenant.TenantSpec{fileProvider}, wantErr: true},
		{name: "fp stays fp", configured: true, committed: []tenant.TenantSpec{fileProvider}},
		{name: "fp removal requires mount restart", configured: true, committed: []tenant.TenantSpec{mount}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			next := &topologyFleetRecorder{}
			hook := topologyFleetTransitions{next: next, fileProvider: test.configured}
			err := hook.Prepare(t.Context(), tenant.FleetTransition{Committed: test.committed})
			if test.wantErr {
				if !errors.Is(err, ErrTopologyGeneration) || next.prepares != 0 {
					t.Fatalf("Prepare = %v, downstream calls %d", err, next.prepares)
				}
				return
			}
			if err != nil || next.prepares != 1 {
				t.Fatalf("Prepare = %v, downstream calls %d", err, next.prepares)
			}
		})
	}
}

func TestRuntimeOwnerClassIsImmutableAcrossTenantProvisioning(t *testing.T) {
	mountOnly := testConfig(shortTempDir(t), "mount-only", newTestNative(nil))
	if got := runtimeOwnerRecoveryClass(mountOnly.Plan); got != proc.RecoveryHolder {
		t.Fatalf("mount-only recovery class = %d, want holder", got)
	}
	sourceCapable := testConfig(shortTempDir(t), "source-capable", newTestNative(nil))
	configureTestSourceFleet(&sourceCapable, testSourceAuthoritySpec("source"))
	if got := runtimeOwnerRecoveryClass(sourceCapable.Plan); got != proc.RecoverySourceOwner {
		t.Fatalf("empty source-capable recovery class = %d, want source owner", got)
	}
	// Provisioning changes desired tenants, not this generation's kill authority.
	if got := runtimeOwnerRecoveryClass(sourceCapable.Plan); got != proc.RecoverySourceOwner {
		t.Fatalf("later-provisionable recovery class = %d, want source owner", got)
	}
}

func TestSourceFleetRequiresImmutablePlanCapability(t *testing.T) {
	config := testConfig(shortTempDir(t), "source-capability", newTestNative(nil))
	if err := validateConfig(config); err != nil {
		t.Fatalf("mount-only immutable plan rejected: %v", err)
	}
	configureTestSourceFleet(&config, testSourceAuthoritySpec("source"))
	if err := validateConfig(config); err != nil {
		t.Fatalf("empty source-capable generation rejected: %v", err)
	}
}

func topologyTenantSpec(id string, presentations catalog.PresentationSet) tenant.TenantSpec {
	return tenant.TenantSpec{
		ID:     catalog.TenantID(id),
		Traits: tenant.TenantTraits{Presentations: presentations},
	}
}

type topologyFleetRecorder struct{ prepares int }

func (r *topologyFleetRecorder) Prepare(context.Context, tenant.FleetTransition) error {
	r.prepares++
	return nil
}
func (*topologyFleetRecorder) Commit(context.Context, tenant.FleetTransition) error { return nil }
func (*topologyFleetRecorder) Abort(context.Context, tenant.FleetTransition) error  { return nil }
