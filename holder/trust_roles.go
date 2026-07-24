package holder

import (
	"errors"
	"fmt"
	"os"

	"github.com/yasyf/daemonkit/trust"
)

const (
	// NativeChildRole authenticates the signed native mount child.
	NativeChildRole trust.PeerRole = "fusekit.native-child.v1"
	// BrokerRole authenticates the signed File Provider broker.
	BrokerRole trust.PeerRole = "fusekit.broker.v1"
	// FileProviderExtensionRole authenticates the signed File Provider extension.
	FileProviderExtensionRole trust.PeerRole = "fusekit.file-provider-extension.v1"
	// StopControllerRole authenticates the one stop controller.
	StopControllerRole trust.PeerRole = "fusekit.stop-controller.v1"
	// ReceiptControllerRole authenticates the one receipt controller.
	ReceiptControllerRole trust.PeerRole = "fusekit.receipt-controller.v1"
	// ReadinessControllerRole authenticates the one readiness controller.
	ReadinessControllerRole trust.PeerRole = "fusekit.readiness-controller.v1"
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
		NativeChildRole, BrokerRole, FileProviderExtensionRole,
		StopControllerRole, ReceiptControllerRole, ReadinessControllerRole,
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
		roles[NativeChildRole] = *requirements.nativeChild
	}
	if requirements.broker != nil {
		roles[BrokerRole] = *requirements.broker
		config.HandoffRoles = []trust.PeerRole{BrokerRole}
	}
	if requirements.fileProviderExtension != nil {
		roles[FileProviderExtensionRole] = *requirements.fileProviderExtension
	}
	if requirements.stopController != nil {
		roles[StopControllerRole] = *requirements.stopController
		config.StopRoles = []trust.PeerRole{StopControllerRole}
	}
	if requirements.receiptController != nil {
		roles[ReceiptControllerRole] = *requirements.receiptController
		config.ReceiptRoles = []trust.PeerRole{ReceiptControllerRole}
	}
	if requirements.readinessController != nil {
		roles[ReadinessControllerRole] = *requirements.readinessController
		config.ReadinessRoles = []trust.PeerRole{ReadinessControllerRole}
	}
	config.Roles = roles
	return config, nil
}
