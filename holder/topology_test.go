package holder

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/internal/recoveryid"
	"github.com/yasyf/fusekit/tenant"
)

func TestTopologyFleetTransitionRequiresSufficientGenerationCapabilities(t *testing.T) {
	mount := topologyTenantSpec("mount", catalog.PresentMount)
	fileProviderOnly := topologyTenantSpec("fp-only", catalog.PresentFileProvider)
	fileProvider := topologyTenantSpec("fp", catalog.PresentMount|catalog.PresentFileProvider)
	for _, test := range []struct {
		name         string
		native       bool
		fileProvider bool
		committed    []tenant.TenantSpec
		wantErr      bool
	}{
		{name: "native stays native", native: true, committed: []tenant.TenantSpec{mount}},
		{name: "native requires fp restart", native: true, committed: []tenant.TenantSpec{fileProvider}, wantErr: true},
		{name: "fp-only rejects native", fileProvider: true, committed: []tenant.TenantSpec{mount}, wantErr: true},
		{name: "fp-only stays fp-only", fileProvider: true, committed: []tenant.TenantSpec{fileProviderOnly}},
		{name: "combined generation starts empty", native: true, fileProvider: true},
		{name: "combined generation serves mount only", native: true, fileProvider: true, committed: []tenant.TenantSpec{mount}},
		{name: "combined stays combined", native: true, fileProvider: true, committed: []tenant.TenantSpec{fileProvider}},
		{name: "combined removes final fp", native: true, fileProvider: true, committed: []tenant.TenantSpec{mount}},
	} {
		t.Run(test.name, func(t *testing.T) {
			next := &topologyFleetRecorder{}
			hook := topologyFleetTransitions{
				next: next, nativeCapable: test.native, fileProviderCapable: test.fileProvider,
			}
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

func TestRuntimeOwnerRecoveryIDIsImmutableAcrossTenantProvisioning(t *testing.T) {
	mountOnly := testConfig(shortTempDir(t), "mount-only", newTestNative(nil))
	if got := runtimeOwnerRecoveryID(mountOnly.Plan); got != recoveryid.Holder {
		t.Fatalf("mount-only recovery ID = %q, want holder", got)
	}
	sourceCapable := testConfig(shortTempDir(t), "source-capable", newTestNative(nil))
	configureTestSourceFleet(&sourceCapable, testSourceAuthoritySpec("source"))
	if got := runtimeOwnerRecoveryID(sourceCapable.Plan); got != recoveryid.SourceOwner {
		t.Fatalf("empty source-capable recovery ID = %q, want source owner", got)
	}
	// Provisioning changes desired tenants, not this generation's kill authority.
	if got := runtimeOwnerRecoveryID(sourceCapable.Plan); got != recoveryid.SourceOwner {
		t.Fatalf("later-provisionable recovery ID = %q, want source owner", got)
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
