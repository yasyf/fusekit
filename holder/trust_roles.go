package holder

import (
	"errors"

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

type fuseKitProcessRequirements struct {
	nativeChild           *trust.Requirement
	broker                *trust.Requirement
	fileProviderExtension *trust.Requirement
	stopController        *trust.Requirement
	receiptController     *trust.Requirement
	readinessController   *trust.Requirement
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
