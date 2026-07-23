package holder

import (
	"context"
	"errors"

	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/transportproto"
)

const stopControlChildMode = "--fusekit-stop-control-v1"

var runDaemonStopControlChild = service.RunStopControlChild

// StopControlChildConfig identifies the exact holder runtime endpoint to stop.
type StopControlChildConfig struct {
	Socket string
}

// StopControlChildArguments returns the private signed stop-control child mode.
func StopControlChildArguments() []string {
	return []string{stopControlChildMode}
}

// RunStopControlChild recognizes and runs the exact signed stop-control child mode.
func RunStopControlChild(
	ctx context.Context,
	arguments []string,
	config StopControlChildConfig,
) (bool, error) {
	if len(arguments) == 0 || arguments[0] != stopControlChildMode {
		return false, nil
	}
	if len(arguments) != 1 {
		return true, errors.New("FuseKit runtime: malformed stop-control child arguments")
	}
	if !exactAbsolutePath(config.Socket) || len([]byte(config.Socket)) > maxUnixSocketPath {
		return true, errors.New("FuseKit runtime: stop-control child socket is not an exact absolute path")
	}
	_, err := runDaemonStopControlChild(ctx, service.StopControlClientConfig{
		Dial:            wire.UnixDialer(config.Socket),
		WireBuild:       transportproto.WireBuild,
		RuntimeProtocol: int(mountproto.RuntimeProtocolVersion),
	})
	return true, err
}
