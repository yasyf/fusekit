//go:build linux

package sourceauthority

import "golang.org/x/sys/unix"

func mutationRenameNoReplace(fromFD int, from string, toFD int, to string) error {
	return unix.Renameat2(fromFD, from, toFD, to, unix.RENAME_NOREPLACE)
}
