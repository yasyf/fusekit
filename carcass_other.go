//go:build !darwin

package fusekit

import "os/exec"

// forceReap best-effort unmounts a carcass; caller verifies with a retried stat.
func forceReap(dir string) {
	_ = exec.Command("fusermount3", "-uz", dir).Run()
}
