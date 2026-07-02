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

// probeFhBase is the first probe read handle. The range [1<<61, 1<<62) is disjoint
// from real kernel fds (below) and synth handles (above); the increment-only
// allocators would need 2^61 concurrent opens to cross.
const probeFhBase = uint64(1) << 61

// probeSize is the virtual probe file's fixed length, 2 MiB — LOAD-BEARING for wedge
// detection: a wedged fuse-t mirror serves small reads instantly but hangs large
// sequential reads, so the probe must span many NFS READ RPCs (rwsize=1 MiB → 2 RPCs).
// Wire contract: it must match the consumer's deep-probe reader (cc-pool's
// overlay.ProbeFileSize). The first 8 bytes are a per-open nonce; the reader rejects
// a repeat as a cache replay.
const probeSize = 2 << 20

func probeFh(fh uint64) bool { return fh >= probeFhBase && fh < synthFhBase }

// probeView serves a virtual read-only wedge-probe file. Every open mints
// fresh random bytes so a page-cache replay of a prior open is caught, and
// advances the reported mtime so the NFS client's open-time revalidation drops
// its cached data pages and re-issues READs. Getattr never advances the mtime:
// a per-Getattr bump deliberately invalidated pages under open files — exactly
// the churn implicated in the macOS nfs_vinvalbuf2 kernel panics — and the
// per-open nonce already defeats cache replay for the probe's reader.
type probeView struct {
	mu     sync.Mutex
	nextFh uint64
	bufs   map[uint64][]byte
	mtime  time.Time
}

func newProbeView() *probeView {
	return &probeView{nextFh: probeFhBase, bufs: map[uint64][]byte{}, mtime: time.Now()}
}

func (v *probeView) getattr(stat *fuse.Stat_t) int {
	v.mu.Lock()
	ts := tsOf(v.mtime)
	v.mu.Unlock()
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
	now := time.Now()
	if !now.After(v.mtime) {
		now = v.mtime.Add(time.Nanosecond)
	}
	v.mtime = now
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
