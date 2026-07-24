package holder

import (
	"errors"
	"fmt"
	"os"

	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/fusekit/trustroles"
)

// RuntimeTrustRequirements is the signed consumer's complete peer contract.
type RuntimeTrustRequirements struct {
	StopController        trust.Requirement
	ReceiptController     trust.Requirement
	ReadinessController   trust.Requirement
	FileProviderExtension trust.Requirement
}

type fuseKitProcessRequirements struct {
	nativeChild           *trust.Requirement
	broker                *trust.Requirement
	brokerLifecycle       *trust.Requirement
	fileProviderExtension *trust.Requirement
	stopController        *trust.Requirement
	receiptController     *trust.Requirement
	readinessController   *trust.Requirement
}

func runtimeTrustPolicy(config Config) (trust.TrustPolicy, error) {
	requirements := fuseKitProcessRequirements{
		stopController:      &config.TrustRequirements.StopController,
		receiptController:   &config.TrustRequirements.ReceiptController,
		readinessController: &config.TrustRequirements.ReadinessController,
	}
	if _, native := config.Plan.NativePresentation(); native {
		requirement := config.Plan.RuntimeRequirement()
		requirements.nativeChild = &requirement
	}
	if broker, enabled := config.Plan.Broker(); enabled {
		requirements.broker = &broker.Requirement
		requirements.brokerLifecycle = &broker.Requirement
		requirements.fileProviderExtension = &config.TrustRequirements.FileProviderExtension
	} else if !emptyRequirement(config.TrustRequirements.FileProviderExtension) {
		return trust.TrustPolicy{}, errors.New("FuseKit runtime: File Provider extension requirement requires a broker presentation")
	}
	policyConfig, err := applyFuseKitProcessTrustRoles(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(),
	}, requirements)
	if err != nil {
		return trust.TrustPolicy{}, err
	}
	policy, err := trust.NewTrustPolicy(policyConfig)
	if err != nil {
		return trust.TrustPolicy{}, fmt.Errorf("FuseKit runtime: compile trust policy: %w", err)
	}
	return policy, nil
}

func applyFuseKitProcessTrustRoles(
	config trust.TrustPolicyConfig,
	requirements fuseKitProcessRequirements,
) (trust.TrustPolicyConfig, error) {
	roles := make(map[trust.PeerRole]trust.Requirement, len(config.Roles)+6)
	for role, requirement := range config.Roles {
		roles[role] = requirement
	}
	for _, role := range []trust.PeerRole{
		trustroles.NativeChild, trustroles.Broker, trustroles.BrokerLifecycle,
		trustroles.FileProviderExtension,
		trustroles.StopController, trustroles.ReceiptController, trustroles.ReadinessController,
	} {
		if _, exists := roles[role]; exists {
			return trust.TrustPolicyConfig{}, errors.New("FuseKit runtime: process trust role is already declared")
		}
	}
	if len(config.StopRoles) != 0 || len(config.ReceiptRoles) != 0 ||
		len(config.ReadinessRoles) != 0 || len(config.HandoffRoles) != 0 {
		return trust.TrustPolicyConfig{}, errors.New("FuseKit runtime: controller and handoff roles are fixed")
	}
	if requirements.nativeChild != nil {
		roles[trustroles.NativeChild] = *requirements.nativeChild
	}
	if requirements.broker != nil {
		roles[trustroles.Broker] = *requirements.broker
		config.HandoffRoles = []trust.PeerRole{trustroles.Broker}
	}
	if requirements.brokerLifecycle != nil {
		roles[trustroles.BrokerLifecycle] = *requirements.brokerLifecycle
	}
	if requirements.fileProviderExtension != nil {
		roles[trustroles.FileProviderExtension] = *requirements.fileProviderExtension
	}
	if requirements.stopController != nil {
		roles[trustroles.StopController] = *requirements.stopController
		config.StopRoles = []trust.PeerRole{trustroles.StopController}
	}
	if requirements.receiptController != nil {
		roles[trustroles.ReceiptController] = *requirements.receiptController
		config.ReceiptRoles = []trust.PeerRole{trustroles.ReceiptController}
	}
	if requirements.brokerLifecycle != nil {
		config.ReceiptRoles = append(config.ReceiptRoles, trustroles.BrokerLifecycle)
	}
	if requirements.readinessController != nil {
		roles[trustroles.ReadinessController] = *requirements.readinessController
		config.ReadinessRoles = []trust.PeerRole{trustroles.ReadinessController}
	}
	if requirements.brokerLifecycle != nil {
		config.ReadinessRoles = append(config.ReadinessRoles, trustroles.BrokerLifecycle)
	}
	config.Roles = roles
	return config, nil
}
