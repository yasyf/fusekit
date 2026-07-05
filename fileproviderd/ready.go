package fileproviderd

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// ErrDomainNotServing means a registered File Provider domain did not serve an
// enumeration within the readiness bound: NSFileProviderManager.add returned but
// the appex has not materialized the domain enough to answer a directory read.
// Not a permanent verdict — Setup returns it rather than cutting an account dir
// over to a domain that cannot yet serve reads, the exact pre-readiness cutover
// that crushed the File Provider host under a fleet migrate.
var ErrDomainNotServing = errors.New("file provider domain did not serve an enumeration in time")

// DefaultReadyTimeout bounds WaitDomainServes when the caller passes a
// non-positive timeout: an appex materialization budget, generous because domain
// add/remove "can take seconds to materialize".
const DefaultReadyTimeout = 30 * time.Second

// readyPollInterval spaces readiness probes; a var so tests can shrink it.
var readyPollInterval = 100 * time.Millisecond

// readDirFn probes whether a domain root serves an enumeration. A var so tests
// stand in a hung or scripted reader; production reads the real dir.
var readDirFn = os.ReadDir

// WaitDomainServes blocks until an enumeration of root succeeds and returns nil,
// or returns ErrDomainNotServing at the deadline. A non-positive timeout uses
// DefaultReadyTimeout.
//
// NSFileProviderManager.add returns before the appex has materialized the domain,
// so a through-domain read is unbounded while it materializes. The probe runs in
// a worker goroutine so a read parked inside a materializing appex never blocks
// the caller: it holds at most one goroutine and one fd and exits when the kernel
// finally answers — the accepted parked-read profile of StatProbes. Probe-first
// ordering (mirroring waitMounted) makes a domain that is already serving return
// on the first probe; the stop channel is checked between attempts so the worker
// stops looping once the caller has given up.
func WaitDomainServes(root string, timeout time.Duration) error {
	if root == "" {
		return fmt.Errorf("WaitDomainServes: root is required")
	}
	if timeout <= 0 {
		timeout = DefaultReadyTimeout
	}
	// Buffered so a probe that answers after the caller has already timed out
	// never blocks the worker on its way to exiting.
	served := make(chan struct{}, 1)
	stop := make(chan struct{})
	// Resolve the tunables once, in the caller, so the worker's loop reads no
	// package var: a worker still draining after the deadline must never race a
	// test restoring these seams (production never reassigns them mid-call).
	probe, interval := readDirFn, readyPollInterval
	go func() {
		for {
			if _, err := probe(root); err == nil {
				served <- struct{}{}
				return
			}
			select {
			case <-stop:
				return
			case <-time.After(interval):
			}
		}
	}()
	select {
	case <-served:
		return nil
	case <-time.After(timeout):
		close(stop)
		return fmt.Errorf("%w: %s", ErrDomainNotServing, root)
	}
}
