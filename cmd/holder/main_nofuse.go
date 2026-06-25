//go:build !(fuse && cgo)

// Pure-build stub of the holder: it serves fuse-t mounts, so without the fuse
// build it just refuses at runtime. Exists so `go build ./...` stays green.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "fusekit-holder: built without the 'fuse' tag (needs cgo + fuse-t); install the fusekit-holder cask")
	os.Exit(1)
}
