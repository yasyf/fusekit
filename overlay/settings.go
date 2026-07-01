package overlay

import "context"

// openRunner is the seam Backend.OpenSettings drives to run a System Settings
// deep link; the default is openSettingsURL (macOS "open"), and tests override
// it to observe which URLs are tried.
var openRunner = func(ctx context.Context, url string) error {
	return openSettingsURL(ctx, url)
}
