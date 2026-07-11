//go:build fuse && cgo

// cgofuse dlopens libfuse-t.dylib (macOS) / libfuse3 (Linux); the library pin
// (CGOFUSE_LIBFUSE_PATH) is each consumer's to set before its first mount.

package fusekit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit/fuset"
	"golang.org/x/sys/unix"
)

// unmountGrace bounds teardown; a var so tests can shrink it.
var unmountGrace = 3 * time.Second

// everMountedLive is sticky and process-global: once any mount comes live, the
// one-time macOS volume-access grant is proven for every later mount-up wait.
var (
	provenMu        sync.Mutex
	everMountedLive bool
)

func mountProven() bool {
	provenMu.Lock()
	defer provenMu.Unlock()
	return everMountedLive
}

func markMountProven() {
	provenMu.Lock()
	everMountedLive = true
	provenMu.Unlock()
}

// Config describes a mount for Mount and Serve.
type Config struct {
	// Base is the backing dir, fed to the default MountAlive readiness check
	// when Ready is nil.
	Base string

	// Dir is the mountpoint.
	Dir string

	// FS is the filesystem served at Dir.
	FS fuse.FileSystemInterface

	// Options is the flat ["-o", "k=v", ...] slice passed to host.Mount.
	Options []string

	// Ready reports whether the mount has come live; nil defaults to
	// MountAlive(Base, Dir), which a synthetic tree (Base never shows through)
	// must override.
	Ready func() bool

	// Wait bounds the mount-up wait once the TCC grant is proven.
	Wait time.Duration

	// FirstWait bounds the mount-up wait while the grant is unproven (the first
	// mount may block on the one-time TCC prompt); zero falls back to Wait.
	FirstWait time.Duration

	// CacheDefeat, when non-nil, wraps FS in the cache-defeat decorator.
	CacheDefeat *CacheDefeat
}

// unmounter seams *fuse.FileSystemHost. Handle.Unmount NEVER calls its
// Unmount: cgofuse's Darwin hostUnmount is unconditionally
// unmount(mountpoint, MNT_FORCE) (host_cgo.go) — tests pin that it is never
// issued.
type unmounter interface {
	Unmount() bool
}

// unmountFn seams Handle.Unmount's kernel unmount(2)/umount2(2) call. flags
// is ALWAYS 0 — never MNT_FORCE or MNT_DETACH; the fleet's only force is the
// holder's fenced, proof-gated carcass clear.
var unmountFn = func(dir string, flags int) error { return unix.Unmount(dir, flags) }

// Handle is a live mount; Unmount tears it down bounded.
type Handle struct {
	host unmounter
	dir  string
	// done closes when the serving goroutine returns — unmount or hard
	// mount(2) failure.
	done chan struct{}

	// callMu guards call, the most recent Unmount invocation's per-call
	// resolution channel (UnmountDone).
	callMu sync.Mutex
	call   chan struct{}
}

// Mount starts serving cfg.FS at cfg.Dir and blocks only until the mount comes
// live (cfg.Ready) or the wait elapses; the returned Handle owns teardown. On
// failure the mount is torn down before returning — never a leaked serving
// goroutine or a half-up mountpoint. A Dir that is already a mountpoint fails
// loud: Mount never stacks and never clears — carcass clearing is the mountd
// server's fenced, proof-gated pre-mount clear, and a mount that appeared
// between that clear and Setup must surface, not be forced.
func Mount(cfg Config) (*Handle, error) {
	if Mounted(cfg.Dir) {
		return nil, fmt.Errorf("%w: %s is already a mountpoint; refusing to stack (only the holder's fenced pre-mount clear removes carcasses)", ErrMountFailed, cfg.Dir)
	}

	fsys := cfg.FS
	if cfg.CacheDefeat != nil {
		fsys = &cacheDefeatFS{FileSystemInterface: fsys, cd: *cfg.CacheDefeat}
	}
	host := fuse.NewFileSystemHost(fsys)
	host.SetCapReaddirPlus(true)

	// fuse-t's FSKit backend is snappier but ignores libfuse fi->fh synthetic
	// reads, so only a pure-passthrough FS may opt in. Asserted on cfg.FS, not
	// the wrapped fsys, so the cache-defeat decorator never hides the marker.
	opts := cfg.Options
	if passthroughEligible(cfg.FS) && fuset.FSKitAvailable() {
		opts = append(append([]string(nil), cfg.Options...), "-o", "backend=fskit")
	}

	done := make(chan struct{})
	// Buffered so the serving goroutine never blocks delivering a panic after
	// Mount has returned.
	panicked := make(chan string, 1)
	go func() {
		defer close(done)
		defer func() {
			// cgofuse panics when libfuse cannot be dlopen'd.
			if r := recover(); r != nil {
				panicked <- fmt.Sprint(r)
			}
		}()
		// Blocks until unmounted; mount failure is observed via done + the
		// readiness probe, not the bool.
		_ = host.Mount(cfg.Dir, opts)
	}()

	ready := cfg.Ready
	if ready == nil {
		ready = func() bool { return MountAlive(cfg.Base, cfg.Dir) }
	}
	wait := cfg.Wait
	if !mountProven() && cfg.FirstWait > 0 {
		wait = cfg.FirstWait
	}

	start := time.Now()
	live, serveExited := waitReady(ready, wait, done)
	if !live {
		host.Unmount()
		// Bounded: a mount stuck on the one-time TCC grant must not hang the caller.
		select {
		case <-done:
		case <-time.After(unmountGrace):
		}
		// A recovered cgofuse-load panic outranks the timeout: it is a hard
		// "no fuse runtime" verdict, not a slow grant.
		select {
		case msg := <-panicked:
			return nil, fmt.Errorf("%w: %s (macOS: brew install fuse-t; Linux: apt install fuse3)", ErrFuseUnavailable, msg)
		default:
		}
		// mountProven is re-read at failure time: a sibling mount coming live
		// mid-wait proves the grant, reclassifying the timeout from missing-grant
		// to transient slowness.
		return nil, mountFailureErr(cfg.Dir, time.Since(start), serveExited, mountProven())
	}
	markMountProven()
	return &Handle{host: host, dir: cfg.Dir, done: done}, nil
}

// Serve is the foreground variant of Mount: it blocks until ctx is canceled
// (bounded teardown) or the mount is removed externally (umount(8) — returns
// nil). A mount-up failure returns the same error Mount would.
func Serve(ctx context.Context, cfg Config) error {
	h, err := Mount(cfg)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return h.Unmount()
	case <-h.done:
		return nil // unmounted externally (umount(8))
	}
}

// Unmount tears the mount down GRACEFULLY ONLY, via fusekit's OWN
// unmount(2)/umount2(2) with flags=0 — NEVER cgofuse's FileSystemHost.Unmount,
// whose Darwin implementation is unconditionally MNT_FORCE (see unmounter).
// The external unmount ends the serve loop, so the serving goroutine exits on
// its own. The call can wedge on a fuse-t fault, so it runs behind
// unmountGrace; there is no force escalation — the fleet's only force is the
// holder's fenced, proof-gated carcass clear. Safe to call more than once.
//
// The verdict keys on the UNMOUNT CALL's own outcome — never the serve
// loop's channel: the call returned with the mountpoint gone is clean (nil);
// returned with it still mounted — a prompt EBUSY refusal included — is a
// FINAL wedge (ErrUnmountWedged, no park); only a call still IN FLIGHT past
// the grace with the mountpoint still up is ErrTeardownPending (wrapping
// ErrUnmountWedged) — the parked call may land at any later moment, so the
// caller must keep the dir fenced until UnmountDone() closes.
func (h *Handle) Unmount() error {
	returned := make(chan struct{})
	h.callMu.Lock()
	h.call = returned
	h.callMu.Unlock()
	go func() {
		// Outcome is read from the kernel (mountedFn), not the error: EINVAL
		// on an already-gone mount is as clean as nil.
		_ = unmountFn(h.dir, 0)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(unmountGrace):
	}
	select {
	case <-returned:
		if mountedFn(h.dir) {
			return fmt.Errorf("%w: unmount of %s returned with it still mounted; refusing to treat it as torn down", ErrUnmountWedged, h.dir)
		}
		reapServers(h.dir)
		return nil
	default:
		if !mountedFn(h.dir) {
			// The kernel mount is gone; the parked call is returning.
			reapServers(h.dir)
			return nil
		}
		return fmt.Errorf("%w: %w: graceful unmount of %s still in flight past %s", ErrUnmountWedged, ErrTeardownPending, h.dir, unmountGrace)
	}
}

// UnmountDone returns the most recent Unmount CALL's resolution channel,
// closed when that unmount(2) call returned — the signal an
// ErrTeardownPending verdict resolves on (the serve loop's Done is a
// different, later event). nil before any Unmount.
func (h *Handle) UnmountDone() <-chan struct{} {
	h.callMu.Lock()
	defer h.callMu.Unlock()
	return h.call
}

// Done returns the channel that closes when the serving goroutine exits —
// external-unmount detection for Serve, never a teardown resolution signal
// (that is UnmountDone's).
func (h *Handle) Done() <-chan struct{} { return h.done }

// mountedFn seams the post-teardown mountpoint check so tests can fake
// Unmount's wedged-vs-clean verdict without a real mount.
var mountedFn = Mounted

// waitReady polls ready until the mount is live, the timeout elapses, or the
// serving goroutine exits (host.Mount returned — a hard mount(2) rejection
// that will never come live). Probe-first ordering guarantees one final probe
// at or after the deadline, so a mount that lands while the last sleep
// straddles it is kept. exited is true only when the serve goroutine returned
// AND the final probe failed; a plain timeout returns false, false — the
// mount call may still be blocked on the one-time OS volume-access grant.
func waitReady(ready func() bool, timeout time.Duration, serveExited <-chan struct{}) (live, exited bool) {
	deadline := time.Now().Add(timeout)
	for {
		if ready() {
			return true, false
		}
		if !time.Now().Before(deadline) {
			return false, false
		}
		select {
		case <-serveExited:
			// One last probe keeps a mount that landed while the loop slept.
			if ready() {
				return true, false
			}
			return false, true
		case <-time.After(mountPollInterval):
		}
	}
}
