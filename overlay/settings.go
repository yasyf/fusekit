package overlay

import "context"

// openRunner runs a System Settings deep link, returning nil on success. It is
// the seam Backend.OpenSettings drives; the default is the platform's
// openSettingsURL (the macOS "open" command on darwin), and tests override it to
// observe which URLs are tried without launching System Settings.
var openRunner = func(ctx context.Context, url string) error {
	return openSettingsURL(ctx, url)
}
