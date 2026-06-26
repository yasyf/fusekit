//go:build fuse && cgo && darwin

package holderfs

import (
	"crypto/rand"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

// probeFhBase is the first probe read handle. The probe range is [1<<61, 1<<62):
// disjoint from real kernel fds below and synthetic handles above. Both
// allocators only increment, so crossing ranges would take 2^61 concurrent opens.
const probeFhBase = uint64(1) << 61

// probeSize is the virtual probe file's fixed length: 2 MiB. It is LOAD-BEARING
// for wedge detection, not arbitrary. The wedge signature is multi-READ-RPC
// readahead — a wedged fuse-t mirror serves small stats and reads instantly while
// a large sequential read hangs forever — so the probe must span MANY NFS READ
// RPCs (with the mount's rwsize=1 MiB, 2 MiB is 2 RPCs); a small probe provably
// succeeds on a wedged mirror and would report it healthy. The consumer's
// deep-probe reader reads the whole file and verifies it got exactly this many
// bytes, so this size is a wire contract: it must match the reader's expectation
// (cc-pool's overlay.ProbeFileSize). The first 8 bytes are a per-open nonce (the
// reader rejects a repeat as a cache replay); random bytes satisfy that.
const probeSize = 2 << 20

// probeFh reports whether fh is a probe read handle.
func probeFh(fh uint64) bool { return fh >= probeFhBase && fh < synthFhBase }

// probeView serves a virtual read-only wedge-probe file. Every open mints fresh
// random bytes so two opens observe different content (a page cache replaying a
// prior open's data is caught, including via the random first-8-byte nonce the
// consumer's reader checks), and Getattr advances the reported mtime on every
// call so the NFS client invalidates its data pages and re-issues a READ.
type probeView struct {
	mu       sync.Mutex
	nextFh   uint64
	bufs     map[uint64][]byte
	lastAttr time.Time
}

func newProbeView() *probeView {
	return &probeView{nextFh: probeFhBase, bufs: map[uint64][]byte{}}
}

func (v *probeView) getattr(stat *fuse.Stat_t) int {
	v.mu.Lock()
	now := time.Now()
	if !now.After(v.lastAttr) {
		now = v.lastAttr.Add(time.Nanosecond)
	}
	v.lastAttr = now
	v.mu.Unlock()
	ts := fuse.Timespec{Sec: now.Unix(), Nsec: int64(now.Nanosecond())}
	*stat = fuse.Stat_t{
		Mode:     fuse.S_IFREG | 0o444,
		Nlink:    1,
		Uid:      uint32(os.Getuid()),
		Gid:      uint32(os.Getgid()),
		Size:     probeSize,
		Blksize:  4096,
		Blocks:   (probeSize + 511) / 512,
		Atim:     ts,
		Mtim:     ts,
		Ctim:     ts,
		Birthtim: ts,
	}
	return 0
}

func (v *probeView) open(flags int) (int, uint64) {
	if flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		return -int(syscall.EACCES), ^uint64(0)
	}
	buf := make([]byte, probeSize)
	if _, err := rand.Read(buf); err != nil {
		return -int(syscall.EIO), ^uint64(0)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	fh := v.nextFh
	v.nextFh++
	v.bufs[fh] = buf
	return 0, fh
}

func (v *probeView) read(fh uint64, buff []byte, ofst int64) int {
	v.mu.Lock()
	buf, ok := v.bufs[fh]
	v.mu.Unlock()
	if !ok {
		return -int(syscall.EBADF)
	}
	if ofst < 0 {
		return -int(syscall.EINVAL)
	}
	if ofst >= int64(len(buf)) {
		return 0
	}
	return copy(buff, buf[ofst:])
}

func (v *probeView) release(fh uint64) {
	v.mu.Lock()
	delete(v.bufs, fh)
	v.mu.Unlock()
}
