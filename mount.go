//go:build fuse && cgo

// This file holds the in-process fuse mount lifecycle: the unified Config every
// consumer fills, Mount (start serving in a goroutine, block until live), Serve
// (the foreground/blocking variant with ctx-cancel teardown), and Handle's
// bounded graceful-then-forced teardown. It is the verbatim cc-pool mount
// machinery (FuseProvider.Setup/Teardown) with cc-notes' robustness folded in:
// cgofuse-load panic recovery converted to ErrFuseUnavailable, pre-mount
// carcass cleanup (ClearCarcass, in the pure half), the optional cache-defeat
// decorator (cachedefeat.go), and the bail-the-mount-wait-on-serve-exit fix
// (cc-pool d5f358a).
//
// cgofuse drives fuse-t natively on macOS (it dlopens libfuse-t.dylib) and
// libfuse3 on Linux. The RUNTIME library pin (CGOFUSE_LIBFUSE_PATH) stays
// app-side: each consumer pins its own platform's library before the first
// mount. The macOS install-time facts — the libfuse-t path and the Homebrew
// cask that installs it — live in package fuset (fuset.Dylib / fuset.Cask).
// Build with: CGO_ENABLED=1 go build -tags fuse ./...

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

const (
	// unmountGrace lets cgofuse's graceful Unmount complete before teardown
	// escalates to a forced kernel unmount.
	unmountGrace = 3 * time.Second
	// forceGrace bounds the wait for the serving goroutine to exit after a
	// forced unmount, so a wedged fuse-t fault can't hold shutdown open.
	forceGrace = 2 * time.Second
)

// everMountedLive is the sticky, process-global OS-grant deduction, lifted
// verbatim from cc-pool's package-global of the same intent: once ANY mount in
// this process comes live, the one-time macOS volume-access grant is proven for
// the whole process, and later mount-up timeouts are transient
// (ErrMountTimeout), not a missing grant (ErrMountNotLive). It lives here, not
// on MountSet, because the grant is per-process, not per-registry — a process
// hosting N MountSets proves the grant once. Guarded by provenMu.
var (
	provenMu        sync.Mutex
	everMountedLive bool
)

// mountProven reports whether this process has ever hosted a live mount — the
// proof that the macOS one-time volume-access grant is held.
func mountProven() bool {
	provenMu.Lock()
	defer provenMu.Unlock()
	return everMountedLive
}

// markMountProven records that a mount in this process came live, proving the
// OS volume-access grant for every later mount-up wait.
func markMountProven() {
	provenMu.Lock()
	everMountedLive = true
	provenMu.Unlock()
}

// Config is the unified mount description every consumer fills. Mount and Serve
// both consume it: they run ClearCarcass(Dir) pre-mount (if set), wrap FS in
// the cache-defeat decorator (if CacheDefeat is non-nil), serve under
// panic-recovery, and wait for the mount to come live up to FirstWait/Wait.
type Config struct {
	// Base is the dir whose contents the mount mirrors (cc-pool) or the repo
	// root the synthetic tree renders over (cc-notes). It is passed to the
	// default readiness check (MountAlive) when Ready is nil.
	Base string

	// Dir is the mountpoint. The mount is served here; teardown unmounts it.
	Dir string

	// FS is the cgofuse filesystem served at Dir. It is wrapped in the
	// cache-defeat decorator when CacheDefeat is non-nil.
	FS fuse.FileSystemInterface

	// Options is the flat ["-o", k=v, ...] slice passed to cgofuse's
	// host.Mount, typically built with MountOptions.Build.
	Options []string

	// Ready reports whether the mount has come live. Mount polls it until it
	// returns true or the wait elapses. When nil it defaults to
	// MountAlive(Base, Dir) — the right check for a passthrough mirror, but
	// wrong for a synthetic tree (whose Base contents never show through), so
	// such consumers set it (e.g. cc-notes' hasMountRoot).
	Ready func() bool

	// Wait bounds the mount-up wait once the TCC grant is proven (this process
	// has already hosted a live mount).
	Wait time.Duration

	// FirstWait bounds the mount-up wait for the first mount in the process,
	// when the one-time macOS volume-access grant is still unproven: a genuine
	// denial fails fast regardless, while a slow-but-granted fuse-t deserves the
	// extra patience before the consumer surfaces the grant walkthrough. When
	// zero, Wait is used for the first mount too.
	FirstWait time.Duration

	// ClearCarcass, when true, force-unmounts any dead-mount carcass a killed
	// holder left at Dir before mounting over it (see ClearCarcass).
	ClearCarcass bool

	// CacheDefeat, when non-nil, wraps FS in the cache-defeat decorator: a
	// per-version mtime-nanosecond override on Getattr and a commit hook on
	// Flush and Fsync. Nil leaves FS mounted verbatim.
	CacheDefeat *CacheDefeat
}

// Handle is a live mount: the cgofuse host serving it, its mountpoint, and the
// channel that closes when the serving goroutine returns. Unmount tears it down
// bounded.
type Handle struct {
	host *fuse.FileSystemHost
	dir  string
	// done closes when the serving goroutine returns — a graceful or forced
	// unmount, or a hard mount(2) failure.
	done chan struct{}
}

// Mount starts serving cfg.FS at cfg.Dir and blocks only until the mount comes
// live (cfg.Ready) or the wait elapses. The serving loop runs in a goroutine;
// the returned Handle owns its teardown. It folds the two source lifecycles:
//
//   - pre-mount carcass cleanup (cc-notes): ClearCarcass(Dir) when set;
//   - cache-defeat decoration (cc-notes): FS wrapped when CacheDefeat is set;
//   - cgofuse-load panic recovery (cc-notes): a panic from the first fuse call
//     (libfuse failed to dlopen) is recovered and surfaced as ErrFuseUnavailable
//     rather than crashing the process;
//   - bail-the-wait-on-serve-exit (cc-pool d5f358a): if the serving goroutine
//     returns before the mount comes live (a hard mount(2) failure), the wait
//     bails after one final probe instead of burning the full timeout;
//   - the proven/unproven grant split (cc-pool): a timed-out unproven mount
//     wraps ErrMountNotLive (the OS grant is presumed still missing), a proven
//     one wraps ErrMountTimeout as transient slowness.
//
// On any failure the mount is torn down before returning, so a stuck mount
// never leaks a serving goroutine or a half-up mountpoint.
func Mount(cfg Config) (*Handle, error) {
	if cfg.ClearCarcass {
		if err := ClearCarcass(cfg.Dir); err != nil {
			return nil, err
		}
		// ClearCarcass clears a dead MOUNTPOINT but not a go-nfsv4 left bound to
		// Dir by a prior mount (e.g. a forced unmount whose server outlived it).
		// If Dir is provably not a live mountpoint now, reap that orphan so the
		// fresh host.Mount below cannot stack a second server on it. Guarded on
		// !Mounted so a genuinely live mount's server is never killed.
		if !Mounted(cfg.Dir) {
			reapServers(cfg.Dir)
		}
	}

	fsys := cfg.FS
	if cfg.CacheDefeat != nil {
		fsys = &cacheDefeatFS{FileSystemInterface: fsys, cd: *cfg.CacheDefeat}
	}
	host := fuse.NewFileSystemHost(fsys)
	host.SetCapReaddirPlus(true)

	// Backend selection: a pure-passthrough FS (PassthroughOnly) is safe on
	// fuse-t's FSKit backend — snappier than the NFS default, but it does NOT
	// honor libfuse fi->fh synthetic reads. Select it only when the FS opts in
	// AND fuse-t's FSKit module is available; otherwise fuse-t's default NFS
	// backend stays (the option is simply absent). Asserted on cfg.FS, not the
	// possibly cache-defeat-wrapped fsys, so the marker is never hidden by the
	// decorator.
	opts := cfg.Options
	if passthroughEligible(cfg.FS) && fuset.FSKitAvailable() {
		opts = append(append([]string(nil), cfg.Options...), "-o", "backend=fskit")
	}

	done := make(chan struct{})
	// panicked is buffered so the serving goroutine never blocks delivering a
	// recovered cgofuse-load panic, even if Mount has already returned.
	panicked := make(chan string, 1)
	go func() {
		defer close(done)
		defer func() {
			// cgofuse panics when libfuse cannot be dlopen'd; turn that into
			// ErrFuseUnavailable instead of crashing the process.
			if r := recover(); r != nil {
				panicked <- fmt.Sprint(r)
			}
		}()
		// host.Mount blocks until unmounted; its bool result (mount failed) is
		// observed through done + the readiness probe, not its return value.
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
		// Bounded: a mount stuck on the one-time TCC grant must not hang the
		// caller; the failure is surfaced and the holder/caller retries.
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
		// Classify by HOW the wait ended: a serve-exit is a hard mount(2)
		// rejection (ErrMountFailed); a timeout with the serve goroutine still
		// alive is the proven/unproven grant split (mountWaitErr). Re-read
		// mountProven at failure time — a sibling mount coming live mid-wait
		// proves the grant and turns a timeout from missing-grant into transient
		// slowness.
		return nil, mountFailureErr(cfg.Dir, time.Since(start), serveExited, mountProven())
	}
	markMountProven()
	return &Handle{host: host, dir: cfg.Dir, done: done}, nil
}

// Serve mounts cfg and blocks in the foreground until ctx is canceled or the
// mount is removed externally (umount(8)), then tears it down. It is the
// blocking variant for a CLI that owns the mount for its own lifetime (cc-notes
// `mount --foreground`): ctx cancellation (SIGINT/SIGTERM) triggers a bounded
// teardown; an external unmount closes the serving goroutine and Serve returns
// nil. A mount-up failure returns the same error Mount would.
func Serve(ctx context.Context, cfg Config) error {
	h, err := Mount(cfg)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return h.Unmount()
	case <-h.done:
		return nil // unmounted externally (umount(8)) — clean exit
	}
}

// Unmount tears the mount down bounded: cgofuse's host.Unmount is a blocking
// cgo call that can wedge on a fuse-t fault, so it runs in a goroutine behind a
// grace timer (unmountGrace) and escalates to a forced kernel unmount
// (MNT_FORCE), then waits forceGrace for the serving goroutine to exit. Honest
// teardown: it confirms the path is no longer a mountpoint with the
// non-blocking Mounted read and returns ErrUnmountWedged when it still is — so
// a caller never treats a live mount as torn down (and RemoveAll through it
// into the backing dir). Safe to call more than once.
func (h *Handle) Unmount() error {
	go h.host.Unmount()
	select {
	case <-h.done:
	case <-time.After(unmountGrace):
		_ = unix.Unmount(h.dir, unix.MNT_FORCE)
		select {
		case <-h.done:
		case <-time.After(forceGrace):
		}
	}
	if Mounted(h.dir) {
		return fmt.Errorf("%w: %s; refusing to treat it as torn down", ErrUnmountWedged, h.dir)
	}
	// The mountpoint is gone, but fuse-t does not guarantee the go-nfsv4 server
	// behind it exited (a forced unmount can outlive its server). Reap any orphan
	// still bound to this dir so a later mount cannot stack a second server.
	reapServers(h.dir)
	return nil
}

// waitReady polls ready until it reports the mount live, the timeout elapses,
// or the serving goroutine exits first. Probe-first, then deadline, then an
// interruptible sleep: the ordering guarantees one final probe at/after the
// deadline, so a mount that lands while the last sleep straddles the deadline
// is kept rather than reported dead. serveExited closing means host.Mount
// returned — a hard mount(2) failure that will never come live — so the loop
// bails after one final probe instead of burning the rest of the timeout
// (cc-pool d5f358a). It mirrors the pure waitMounted, but polls the consumer's
// Ready seam rather than the hardwired MountAlive: a synthetic-tree mount
// (cc-notes) is live without Base showing through, so MountAlive would never
// see it. It returns (live, exited): exited is true only when the serving
// goroutine returned AND the final probe found no live mount — a hard mount(2)
// rejection the caller classifies as ErrMountFailed, distinct from a plain
// timeout (both false) where the mount call is still blocked, possibly on the
// one-time OS volume-access grant.
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
			// The serving goroutine returned, so no mount will come live. One
			// last probe keeps a mount that landed in the instant the loop
			// slept; an empty/torn-down dir fails it — and that empty case is
			// the hard mount(2) rejection, reported via exited.
			if ready() {
				return true, false
			}
			return false, true
		case <-time.After(mountPollInterval):
		}
	}
}
