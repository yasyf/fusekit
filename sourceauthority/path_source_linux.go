//go:build linux

package sourceauthority

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

func platformRootIdentity(fd int, status unix.Stat_t) (FileIdentity, error) {
	return platformFileIdentity(fd, "", 0, fmt.Sprintf("device:%x", uint64(status.Dev)), status)
}

func platformFileIdentity(
	dirfd int,
	path string,
	flags int,
	volume string,
	status unix.Stat_t,
) (FileIdentity, error) {
	if path == "" {
		flags |= unix.AT_EMPTY_PATH
	}
	var stable unix.Statx_t
	if err := unix.Statx(
		dirfd,
		path,
		flags|unix.AT_STATX_SYNC_AS_STAT,
		unix.STATX_INO|unix.STATX_BTIME,
		&stable,
	); err != nil {
		return FileIdentity{}, errors.Join(errors.New("sourceauthority: resolve stable Linux file identity"), err)
	}
	if stable.Mask&(unix.STATX_INO|unix.STATX_BTIME) != unix.STATX_INO|unix.STATX_BTIME ||
		stable.Ino != status.Ino || stable.Dev_major != unix.Major(uint64(status.Dev)) ||
		stable.Dev_minor != unix.Minor(uint64(status.Dev)) || stable.Btime.Nsec >= 1_000_000_000 {
		return FileIdentity{}, errors.New("sourceauthority: stable Linux file identity is unavailable")
	}
	return FileIdentity{
		VolumeUUID: volume, Inode: stable.Ino,
		BirthtimeSec: stable.Btime.Sec, BirthtimeNsec: int64(stable.Btime.Nsec),
	}, nil
}
