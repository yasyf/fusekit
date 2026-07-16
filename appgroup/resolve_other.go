//go:build !darwin

package appgroup

// App Groups are macOS-only; excluding purego from this file confines it to darwin.
func platformResolveContainer(string) (string, error) {
	return "", ErrNoGroupContainer
}
