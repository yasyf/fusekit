package holder

import (
	"errors"

	"github.com/yasyf/daemonkit"
)

// RuntimeTrust is the signed consumer's contract for the peers the plan does
// not name. The plan carries its own executables' requirements: the runtime
// and, when a broker presentation is configured, the File Provider broker.
type RuntimeTrust struct {
	// Controller admits the one control session the daemon serves at a time —
	// stop and drain — and replaces the stop, receipt, and readiness controllers.
	Controller daemonkit.Requirement
	// FileProviderExtension admits the extension's handoff-adopted sessions,
	// which walk full trust admission against the extension's own identity. It
	// is stated only by a plan that configures a broker presentation.
	FileProviderExtension daemonkit.Requirement
}

type fileProviderPeers struct {
	broker    daemonkit.Requirement
	extension daemonkit.Requirement
}

type runtimePeers struct {
	runtime      daemonkit.Requirement
	native       bool
	fileProvider *fileProviderPeers
	controller   daemonkit.Requirement
}

func runtimeTrust(config Config) (daemonkit.Trust, error) {
	_, native := config.Plan.NativePresentation()
	peers := runtimePeers{
		runtime:    config.Plan.RuntimeRequirement(),
		native:     native,
		controller: config.Trust.Controller,
	}
	if broker, enabled := config.Plan.Broker(); enabled {
		peers.fileProvider = &fileProviderPeers{
			broker: broker.Requirement, extension: config.Trust.FileProviderExtension,
		}
	} else if !emptyRequirement(config.Trust.FileProviderExtension) {
		return daemonkit.Trust{}, errors.New("FuseKit runtime: File Provider extension requirement requires a broker presentation")
	}
	return fuseKitTrust(peers)
}

func fuseKitTrust(peers runtimePeers) (daemonkit.Trust, error) {
	if emptyRequirement(peers.runtime) {
		return daemonkit.Trust{}, errors.New("FuseKit runtime: signed runtime requirement is required")
	}
	if emptyRequirement(peers.controller) {
		return daemonkit.Trust{}, errors.New("FuseKit runtime: control requirement is required")
	}
	var business daemonkit.Requirements
	if peers.native {
		business = append(business, peers.runtime)
	}
	if peers.fileProvider != nil {
		if emptyRequirement(peers.fileProvider.broker) {
			return daemonkit.Trust{}, errors.New("FuseKit runtime: broker presentation requires a broker requirement")
		}
		if emptyRequirement(peers.fileProvider.extension) {
			return daemonkit.Trust{}, errors.New("FuseKit runtime: broker presentation requires a File Provider extension requirement")
		}
		business = append(business, peers.fileProvider.extension, peers.fileProvider.broker)
	}
	if len(business) == 0 {
		return daemonkit.Trust{}, errors.New("FuseKit runtime: runtime trust requires a native or File Provider presentation")
	}
	controller := peers.controller
	return daemonkit.Trust{
		Control:  &controller,
		Business: business,
		Serving:  daemonkit.ServingSigned(peers.runtime),
	}, nil
}
