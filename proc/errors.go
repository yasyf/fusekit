// Package proc holds the consumer-agnostic process primitives the mountd
// subsystem (and its consumers' own daemons) lean on: a flock single-entrant
// socket bind, a detached-child spawn, and an exponential backoff. It depends
// on nothing but the standard library — never on the root fusekit package or
// cgofuse — so it compiles on every platform and build tag.
package proc

import "errors"

// ErrChildUnavailable means a child process could not be reached or started:
// the socket would not come up, or the spawn could not even be assembled. It is
// a process-availability condition, never a domain verdict — drivers retry on
// it rather than taking an irreversible action. It lives here (not in any
// consumer) so the spawn primitive and a consumer's domain re-export (e.g.
// mountd.ErrHolderUnavailable) share one identity.
var ErrChildUnavailable = errors.New("child process not running")

// ErrSkipSpawn is the benign spawn refusal: a Spawn.Override (or a consumer's
// CanHost) returns it to mean "there is intentionally nothing for a child to
// serve right now" — an empty desired state, NOT a failure. The Supervisor
// treats it as a no-op: no backoff is armed, the crash-loop breaker is not
// advanced, and OnSpawnError is not called. The consumer wraps it in its own
// sentinel (e.g. cc-pool's errNothingToServe) so the reason is legible.
var ErrSkipSpawn = errors.New("nothing for the child to serve; spawn skipped")

// ErrPeerStarting means the ".lock" file is held but its owner does not answer
// the Evict probe yet — it may be mid-start (post-flock, pre-bind). The bind is
// refused; the caller (e.g. launchd) retries against whatever it becomes.
var ErrPeerStarting = errors.New("a peer owns the socket lock but is not answering yet")

// ErrLockStillHeld means Evict made way (returned true) but the contended lock
// was not released within the post-evict poll deadline.
var ErrLockStillHeld = errors.New("socket lock still held after eviction")
