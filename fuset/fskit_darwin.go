//go:build darwin

package fuset

import (
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func fskitAvailable() bool {
	if !Installed() {
		return false
	}
	if macOSMajor() < 26 {
		return false
	}
	_, err := os.Stat(FSKitModuleBundle)
	return err == nil
}

// macOSMajor returns the major component of the macOS product version (26 for
// "26.5"), or 0 if unreadable. kern.osproductversion is the product version
// ("26.5"), not the Darwin kernel version ("25.5.0"), so no Darwin->macOS
// offset is needed.
func macOSMajor() int {
	v, err := unix.Sysctl("kern.osproductversion")
	if err != nil {
		return 0
	}
	major, _, _ := strings.Cut(v, ".")
	n, err := strconv.Atoi(major)
	if err != nil {
		return 0
	}
	return n
}
