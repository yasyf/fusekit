package holderfs

import (
	"syscall"
	"time"
)

// statCtime reads a file's real on-disk change time from a raw stat. macOS
// exposes it as Ctimespec (the field name differs per platform, hence the
// split — keep this test compiling on the shared Linux CI leg).
func statCtime(st *syscall.Stat_t) time.Time {
	return time.Unix(st.Ctimespec.Sec, st.Ctimespec.Nsec)
}
