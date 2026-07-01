package fusekit

import (
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Untagged deliberately: non-fuse binaries need the mount-alive checks.

// StatProbes runs kernel stats behind a timeout, joining same-key callers onto
// one in-flight probe: fuse-t's NFS backend has no soft/timeout mount option,
// so a wedged-mirror stat can block forever and its probe goroutine is
// deliberately left untracked.
type StatProbes[V any] struct {
	mu       sync.Mutex
	inflight map[string]*statProbe[V]
}

// v is valid once done closes.
type statProbe[V any] struct {
	done chan struct{}
	v    V
}

// Do runs stat keyed by key, returning its verdict, or the zero V and ok=false
// when it does not answer within timeout; the caller chooses a timed-out
// probe's fail direction.
func (p *StatProbes[V]) Do(key string, timeout time.Duration, stat func() V) (V, bool) {
	p.mu.Lock()
	pr, ok := p.inflight[key]
	if !ok {
		if p.inflight == nil {
			p.inflight = map[string]*statProbe[V]{}
		}
		pr = &statProbe[V]{done: make(chan struct{})}
		p.inflight[key] = pr
		go func() {
			v := stat()
			p.mu.Lock()
			pr.v = v
			delete(p.inflight, key)
			p.mu.Unlock()
			close(pr.done)
		}()
	}
	p.mu.Unlock()
	select {
	case <-pr.done:
		return pr.v, true
	case <-time.After(timeout):
		var zero V
		return zero, false
	}
}

// Inflight reports the probes currently running; tests drain wedged probes
// against it.
func (p *StatProbes[V]) Inflight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.inflight)
}

// statProbeTimeout is a var so tests can shrink it.
var statProbeTimeout = 2 * time.Second

// MountAlive reports whether accountDir currently mirrors base.
func MountAlive(base, accountDir string) bool {
	fi, err := os.Stat(accountDir)
	if err != nil || !fi.IsDir() {
		return false
	}
	// ANY visible base entry means live: a content holder may redirect base's
	// first dotfile to a per-account root the account lacks, so probing only
	// that entry would read a live mount dead and churn remounts.
	entries, err := os.ReadDir(base)
	if err != nil || len(entries) == 0 {
		return err == nil
	}
	for _, e := range entries {
		if _, err := os.Lstat(filepath.Join(accountDir, e.Name())); err == nil {
			return true
		}
	}
	return false
}

// mountAliveFn seams MountAlive for tests.
var mountAliveFn = MountAlive

// aliveProbes is never shared with the deep probes: a shallow verdict must
// never join a deep read of the same dir.
var aliveProbes StatProbes[bool]

// MountAliveWithin is MountAlive bounded by statProbeTimeout. A timed-out
// probe reads NOT alive — fail-closed, so a wedged mirror can never pass for
// healthy.
func MountAliveWithin(base, accountDir string) bool {
	alive, ok := aliveProbes.Do(accountDir, statProbeTimeout, func() bool {
		return mountAliveFn(base, accountDir)
	})
	return ok && alive
}

// mountPollInterval is a var so tests can shrink it.
var mountPollInterval = 100 * time.Millisecond

// waitMounted polls until base is visible through accountDir. Probe-first
// ordering guarantees one final probe at or after the deadline; a zero
// timeout probes exactly once. serveExited closing means host.Mount returned
// and the mount will never come live, so the loop bails; nil disables that.
func waitMounted(base, accountDir string, timeout time.Duration, serveExited <-chan struct{}) bool {
	deadline := time.Now().Add(timeout)
	for {
		if mountAliveFn(base, accountDir) {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		select {
		case <-serveExited:
			// One last probe keeps a mount that landed while the loop slept.
			return mountAliveFn(base, accountDir)
		case <-time.After(mountPollInterval):
		}
	}
}
