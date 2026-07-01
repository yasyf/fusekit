//go:build !(fuse && cgo && darwin)

// Stub holder for any build that cannot host mounts (macOS-only); it refuses.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "fusekit-holder: built without the 'fuse' tag (needs cgo + fuse-t); install the fusekit-holder cask")
	os.Exit(1)
}
