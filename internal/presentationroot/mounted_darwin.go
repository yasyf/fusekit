//go:build darwin

package presentationroot

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func platformMounted(path string) (bool, error) {
	count, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return false, fmt.Errorf("getfsstat: %w", err)
	}
	table := make([]unix.Statfs_t, count+8)
	count, err = unix.Getfsstat(table, unix.MNT_NOWAIT)
	if err != nil {
		return false, fmt.Errorf("getfsstat: %w", err)
	}
	if count > len(table) {
		return false, fmt.Errorf("getfsstat: mount table grew from %d to %d entries", len(table)-8, count)
	}
	for index := 0; index < count; index++ {
		if unix.ByteSliceToString(table[index].Mntonname[:]) == path {
			return true, nil
		}
	}
	return false, nil
}
