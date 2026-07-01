//go:build !darwin

package fileproviderd

import "context"

func launchAppPlatform(context.Context, string) error {
	return ErrAppLaunchUnsupported
}
