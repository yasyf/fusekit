//go:build darwin && cgo

package sourceauthority

/*
#cgo LDFLAGS: -framework CoreServices
#include <stdlib.h>
#include "fsevents_darwin.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

func platformRootIdentity(fd int, status unix.Stat_t) (FileIdentity, error) {
	var volume, message *C.char
	if C.fk_fsevents_fd_volume_uuid(C.int(fd), &volume, &message) == 0 {
		defer C.fk_fsevents_free(unsafe.Pointer(message))
		return FileIdentity{}, fmt.Errorf("sourceauthority: resolve source root identity: %s", C.GoString(message))
	}
	defer C.fk_fsevents_free(unsafe.Pointer(volume))
	return identityFromStat(C.GoString(volume), status), nil
}

func identityFromStat(volume string, status unix.Stat_t) FileIdentity {
	return FileIdentity{
		VolumeUUID: volume, Inode: status.Ino,
		BirthtimeSec: status.Btim.Sec, BirthtimeNsec: status.Btim.Nsec,
	}
}
