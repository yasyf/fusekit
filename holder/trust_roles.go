package holder

import (
	"fmt"
	"os"

	"github.com/yasyf/daemonkit/trust"
)

const (
	RoleStopController        trust.PeerRole = "fusekit.stop-controller.v1"
	RoleReceiptController     trust.PeerRole = "fusekit.receipt-controller.v1"
	RoleReadinessController   trust.PeerRole = "fusekit.readiness-controller.v1"
	RoleNativeChild           trust.PeerRole = "native-child.v1"
	RoleBroker                trust.PeerRole = "broker.v1"
	RoleFileProviderExtension trust.PeerRole = "file-provider-extension.v1"
)

// RuntimeTrustRequirements is the signed consumer's complete peer contract.
type RuntimeTrustRequirements struct {
	StopController        trust.Requirement
	ReceiptController     trust.Requirement
	ReadinessController   trust.Requirement
	FileProviderExtension trust.Requirement
}

func runtimeTrustPolicy(config Config) (trust.TrustPolicy, error) {
	roles := map[trust.PeerRole]trust.Requirement{
		RoleStopController:      config.TrustRequirements.StopController,
		RoleReceiptController:   config.TrustRequirements.ReceiptController,
		RoleReadinessController: config.TrustRequirements.ReadinessController,
	}
	if _, native := config.Plan.NativePresentation(); native {
		roles[RoleNativeChild] = config.Plan.RuntimeRequirement()
	}
	var handoff []trust.PeerRole
	if broker, enabled := config.Plan.Broker(); enabled {
		roles[RoleBroker] = broker.Requirement
		roles[RoleFileProviderExtension] = config.TrustRequirements.FileProviderExtension
		handoff = []trust.PeerRole{RoleBroker}
	} else if !emptyRequirement(config.TrustRequirements.FileProviderExtension) {
		return trust.TrustPolicy{}, fmt.Errorf("FuseKit runtime: File Provider extension requirement requires a broker presentation")
	}
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID:    os.Geteuid(),
		Roles:          roles,
		StopRoles:      []trust.PeerRole{RoleStopController},
		ReceiptRoles:   []trust.PeerRole{RoleReceiptController},
		ReadinessRoles: []trust.PeerRole{RoleReadinessController},
		HandoffRoles:   handoff,
	})
	if err != nil {
		return trust.TrustPolicy{}, fmt.Errorf("FuseKit runtime: compile trust policy: %w", err)
	}
	return policy, nil
}
