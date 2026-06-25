//go:build !(fuse && cgo)

// Pure-build stub: the holder needs the fuse build, so this just refuses.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "fusekit-holder: built without the 'fuse' tag (needs cgo + fuse-t); install the fusekit-holder cask")
	os.Exit(1)
}
