//go:build !darwin

package holderfs

import (
	"syscall"
	"time"
)

// statCtime reads a file's real on-disk change time from a raw stat. Linux and
// the other unixes expose it as Ctim (macOS calls the same field Ctimespec).
func statCtime(st *syscall.Stat_t) time.Time {
	return time.Unix(st.Ctim.Sec, st.Ctim.Nsec)
}
