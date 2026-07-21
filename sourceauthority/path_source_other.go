//go:build !darwin && !linux

package sourceauthority

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func platformRootIdentity(_ int, status unix.Stat_t) (FileIdentity, error) {
	return platformFileIdentity(0, "", 0, fmt.Sprintf("device:%x", uint64(status.Dev)), status)
}

func platformFileIdentity(_ int, _ string, _ int, volume string, status unix.Stat_t) (FileIdentity, error) {
	return FileIdentity{
		VolumeUUID: volume, Inode: status.Ino,
		BirthtimeSec: status.Ctim.Sec, BirthtimeNsec: status.Ctim.Nsec,
	}, nil
}
