//go:build !darwin

package sourceauthority

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func platformRootIdentity(_ int, status unix.Stat_t) (FileIdentity, error) {
	return identityFromStat(fmt.Sprintf("device:%x", uint64(status.Dev)), status), nil
}

func identityFromStat(volume string, status unix.Stat_t) FileIdentity {
	return FileIdentity{
		VolumeUUID: volume, Inode: status.Ino,
		BirthtimeSec: status.Ctim.Sec, BirthtimeNsec: status.Ctim.Nsec,
	}
}
