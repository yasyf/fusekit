//go:build darwin

package sourceauthority

import "golang.org/x/sys/unix"

func mutationRenameNoReplace(fromFD int, from string, toFD int, to string) error {
	return unix.RenameatxNp(fromFD, from, toFD, to, unix.RENAME_EXCL)
}
