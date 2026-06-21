//go:build !darwin

package mountd

import (
	"context"
	"errors"
)

// OpenNetworkVolumesSettings is darwin-only: the Network Volumes TCC grant and
// the System Settings deep link it points at exist only on macOS.
func OpenNetworkVolumesSettings(context.Context) error {
	return errors.New("opening Network Volumes settings is only supported on macOS")
}
