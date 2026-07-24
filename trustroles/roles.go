// Package trustroles defines FuseKit's fixed daemon session roles.
package trustroles

import "github.com/yasyf/daemonkit/trust"

const (
	// NativeChild authenticates the signed native mount child.
	NativeChild trust.PeerRole = "fusekit.native-child.v1"
	// Broker authenticates the signed File Provider broker.
	Broker trust.PeerRole = "fusekit.broker.v1"
	// FileProviderExtension authenticates the signed File Provider extension.
	FileProviderExtension trust.PeerRole = "fusekit.file-provider-extension.v1"
	// StopController authenticates the one stop controller.
	StopController trust.PeerRole = "fusekit.stop-controller.v1"
	// ReceiptController authenticates the one receipt controller.
	ReceiptController trust.PeerRole = "fusekit.receipt-controller.v1"
	// ReadinessController authenticates the one readiness controller.
	ReadinessController trust.PeerRole = "fusekit.readiness-controller.v1"
)
