//go:build fuse && cgo

// cgofuse dlopens libfuse-t.dylib (macOS) / libfuse3 (Linux); the library pin
// (CGOFUSE_LIBFUSE_PATH) is each consumer's to set before its first mount.

package fusekit

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"sync"
	"syscall"
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

	// ProbePath, when set, is the path Mount's post-ready through-mount
	// confirm op stats instead of the mountpoint root Dir (see
	// confirmMounted).
	ProbePath string

	// Wait bounds the mount-up wait once the TCC grant is proven.
	Wait time.Duration

	// FirstWait bounds the mount-up wait while the grant is unproven (the first
	// mount may block on the one-time TCC prompt); zero falls back to Wait.
	FirstWait time.Duration

	// CacheDefeat, when non-nil, wraps FS in the cache-defeat decorator.
	CacheDefeat *CacheDefeat

	// ReArmSignals, when non-nil, is invoked synchronously right after Mount
	// defuses cgofuse's signal handler with signal.Reset(SIGINT, SIGTERM) —
	// which unsubscribes EVERY channel process-wide — so the embedding app
	// re-registers its OWN signal.Notify here. nil means the app handles
	// signals itself (or needs no re-arm). See defuseCgofuseSignals.
	ReArmSignals func()
}

// Handle is a live mount; Unmount tears it down bounded. Nothing in this
// package can name cgofuse's FileSystemHost.Unmount — its Darwin
// implementation is unconditionally unmount(mountpoint, MNT_FORCE)
// (host_cgo.go), and the fleet's only force is the holder's fenced,
// proof-gated carcass clear.
type Handle struct {
	dir string
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
		// Graceful-only cleanup — NEVER cgofuse's Unmount (Darwin MNT_FORCE).
		// A mount that appeared but failed the readiness wait comes down via
		// fusekit's own unmount(2); one that never appeared has nothing to
		// unmount (undetermined attempts anyway — EINVAL is harmless).
		if mounted, merr := mountedCheckFn(cfg.Dir); mounted || merr != nil {
			go func() { _ = unmountFn(cfg.Dir, 0) }()
		}
		// Bounded: a mount stuck on the one-time TCC grant must not hang the caller.
		select {
		case <-done:
		case <-time.After(unmountGrace):
		}
		// Unconditional: a half-up mount may have run init (cgofuse subscribed);
		// if it never did, the Reset is harmless — no Notify happened.
		defuseCgofuseSignals(cfg.ReArmSignals)
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
	if cerr := confirmMounted(cfg); cerr != nil {
		// No defuse: without a proven through-mount op there is no proof
		// cgofuse's Notify already ran, so a Reset could land before it and
		// defuse nothing. The suspect mount comes down gracefully; cgofuse
		// staying subscribed can at worst force this one suspect mount.
		go func() { _ = unmountFn(cfg.Dir, 0) }()
		select {
		case <-done:
		case <-time.After(unmountGrace):
		}
		return nil, fmt.Errorf("%w: %s reported ready but the through-mount confirm failed (%v); torn down as suspect", ErrMountTimeout, cfg.Dir, cerr)
	}
	markMountProven()
	defuseCgofuseSignals(cfg.ReArmSignals)
	return &Handle{dir: cfg.Dir, done: done}, nil
}

// confirmBound bounds the post-ready through-mount confirm op; a var so tests
// can shrink it.
var confirmBound = 2 * time.Second

// confirmMounted gates the cgofuse signal defuse (see defuseCgofuseSignals):
// mount-table check first — a bare-directory stat proves nothing — then a
// stat that therefore resolves through the covering mount, proving FUSE init
// (and cgofuse's Notify) already ran. Anything else: skip the Reset, surface.
func confirmMounted(cfg Config) error {
	p := cfg.Dir
	if cfg.ProbePath != "" {
		p = cfg.ProbePath
	}
	errc := make(chan error, 1)
	go func() {
		mounted, merr := mountedCheckFn(cfg.Dir)
		if merr != nil {
			errc <- fmt.Errorf("mount-table read: %w", merr)
			return
		}
		if !mounted {
			errc <- fmt.Errorf("%s is not a mountpoint — a bare-directory stat proves nothing", cfg.Dir)
			return
		}
		var st unix.Stat_t
		if err := unix.Stat(p, &st); err != nil {
			errc <- fmt.Errorf("through-mount stat %s: %w", p, err)
			return
		}
		errc <- nil
	}()
	select {
	case err := <-errc:
		if err != nil {
			return fmt.Errorf("confirm %s: %w", cfg.Dir, err)
		}
		return nil
	case <-time.After(confirmBound):
		return fmt.Errorf("confirm %s: no answer within %s", cfg.Dir, confirmBound)
	}
}

// defuseMu serializes the whole defusal: concurrent Mounts must never
// interleave one call's Reset with another's re-Notify, which would widen
// the unsubscribed window.
var defuseMu sync.Mutex

// defuseCgofuseSignals globally unsubscribes cgofuse's per-host signal
// channel: hostInit runs signal.Notify(sigc, SIGINT, SIGTERM) and a delivered
// signal makes cgofuse's goroutine call host.Unmount() — Darwin MNT_FORCE on
// every live mount at logout/shutdown/bootout SIGTERM. signal.Reset drops
// every subscriber process-wide, so reArm (the embedding app's own re-Notify)
// runs synchronously right after, under defuseMu.
//
// ORDERING: the Reset runs only after confirmMounted proved a through-mount
// operation served, ordering it after cgofuse's Notify by protocol
// construction, not by timing (TestCgofusePinnedSignalRegistration pins the
// pinned source's shape).
//
// Residual, accepted: a TERM in the pre-Reset window at mount creation can
// force that single fresh mount (empty, no dirty pages), and one landing in
// the in-lock instants between Reset and re-Notify hits the default
// disposition — microseconds per mount creation; at steady state cgofuse is
// fully defused.
func defuseCgofuseSignals(reArm func()) {
	defuseMu.Lock()
	defer defuseMu.Unlock()
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)
	if reArm != nil {
		reArm()
	}
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
// unmount(2)/umount2(2) with flags=0 — never any force, never a cgofuse call
// (see Handle). The external unmount ends the serve loop, so the serving
// goroutine exits on its own. The call can wedge on a fuse-t fault, so it
// runs behind unmountGrace; there is no force escalation — the fleet's only
// force is the holder's fenced, proof-gated carcass clear. Safe to call more
// than once.
//
// The verdict keys on the UNMOUNT CALL's own outcome — never the serve
// loop's channel: the call returned with the mountpoint gone is clean (nil);
// returned with it still mounted is a FINAL wedge (ErrUnmountWedged, no
// park), except a prompt EBUSY refusal, which is retryable ErrMountBusy
// (still wrapping ErrUnmountWedged — the dir is still a mountpoint); a
// failed mounted check is UNDETERMINED and fails closed as a wedge, never
// clean. A call still IN FLIGHT past the grace is ErrTeardownPending
// (wrapping ErrUnmountWedged) even when the mountpoint already reads gone:
// the pending/final verdict is ADVISORY — the park watcher re-reading kernel
// truth when the call returns (UnmountDone) is the single source of truth
// for the final release, which makes any grace-boundary misclassification
// harmless by construction.
func (h *Handle) Unmount() error {
	returned := make(chan struct{})
	h.callMu.Lock()
	h.call = returned
	h.callMu.Unlock()
	var callErr error // written before close(returned); read only after it
	go func() {
		// Outcome is read from the kernel, not the error alone: EINVAL on an
		// already-gone mount is as clean as nil.
		callErr = unmountFn(h.dir, 0)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(unmountGrace):
	}
	select {
	case <-returned:
		mounted, merr := mountedCheckFn(h.dir)
		// %w, not %v: the call's errno must survive errors.Is on the verdict.
		wrapped := callErr
		if wrapped == nil {
			wrapped = errors.New("nil")
		}
		if merr != nil {
			return fmt.Errorf("%w: unmount of %s returned (%w) but the mounted check failed (%v): undetermined; refusing to treat it as torn down", ErrUnmountWedged, h.dir, wrapped, merr)
		}
		if mounted {
			if errors.Is(callErr, unix.EBUSY) {
				return fmt.Errorf("%w: %w: unmount of %s answered EBUSY; retry once the dir is idle", ErrUnmountWedged, ErrMountBusy, h.dir)
			}
			return fmt.Errorf("%w: unmount of %s returned (%w) with it still mounted; refusing to treat it as torn down", ErrUnmountWedged, h.dir, wrapped)
		}
		reapServers(h.dir)
		return nil
	default:
		// In flight: unknown even if the mountpoint already reads gone — the
		// caller parks on UnmountDone and releases only at call-return.
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

// mountedFn seams the liveness-direction mountpoint check (error collapsed to
// not-mounted); mountedCheckFn seams Unmount's teardown verification, where an
// error is UNDETERMINED and fails closed as a wedge.
var (
	mountedFn      = Mounted
	mountedCheckFn = MountedCheck
)

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
