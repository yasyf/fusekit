//go:build !darwin

// Stub vmstress for non-darwin builds: the harness drives macOS guests only,
// and the binary must stay inert everywhere else.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "vmstress: macOS-guest workload driver only; this build refuses to run")
	os.Exit(86)
}
