//go:build !(fuse && cgo && darwin)

// Stub for any build that cannot host mounts (no fuse tag, no cgo, or non-darwin
// — the holder is macOS-only): it just refuses.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "fusekit-holder: built without the 'fuse' tag (needs cgo + fuse-t); install the fusekit-holder cask")
	os.Exit(1)
}
