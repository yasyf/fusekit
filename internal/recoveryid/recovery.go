// Package recoveryid defines FuseKit-owned daemonkit recovery barriers.
package recoveryid

import "github.com/yasyf/daemonkit/proc"

const (
	// SourceOwner settles retired source-authority runtime owners.
	SourceOwner proc.RecoveryID = "fusekit.source-owner.v1"
	// SourceDriver settles retired semantic source-driver processes.
	SourceDriver proc.RecoveryID = "fusekit.source-driver.v1"
	// Broker settles retired File Provider broker processes.
	Broker proc.RecoveryID = "fusekit.broker.v1"
	// NativeMount settles retired native mount processes.
	NativeMount proc.RecoveryID = "fusekit.native-mount.v1"
	// CatalogWorker settles retired catalog worker processes.
	CatalogWorker proc.RecoveryID = "fusekit.catalog-worker.v1"
	// SourceObserver settles retired physical source-observer processes.
	SourceObserver proc.RecoveryID = "fusekit.source-observer.v1"
	// Holder settles retired FuseKit runtime owners.
	Holder proc.RecoveryID = "fusekit.holder.v1"
)
