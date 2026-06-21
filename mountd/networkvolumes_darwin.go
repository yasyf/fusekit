//go:build darwin

package mountd

import (
	"context"
	"fmt"
)

// OpenNetworkVolumesSettings opens the macOS System Settings pane that holds the
// one-time Network Volumes TCC grant, trying networkVolumesSettingsURLs in order
// and returning nil on the first success. If every URL fails it wraps the last
// failure with %w.
func OpenNetworkVolumesSettings(ctx context.Context) error {
	var lastErr error
	for _, url := range networkVolumesSettingsURLs {
		if err := openRunner(ctx, url); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("could not open any Network Volumes settings pane: %w", lastErr)
}
