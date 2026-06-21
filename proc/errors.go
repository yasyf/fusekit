// Package proc holds the consumer-agnostic process primitives the mountd
// subsystem (and its consumers' own daemons) lean on: a flock single-entrant
// socket bind, a detached-child spawn, and an exponential backoff. It depends
// on nothing but the standard library — never on the root fusekit package or
// cgofuse — so it compiles on every platform and build tag.
package proc

import "errors"

// ErrHolderUnavailable means a child process could not be reached or started:
// the socket would not come up, or the spawn could not even be assembled. It is
// a process-availability condition, never a domain verdict — drivers retry on
// it rather than taking an irreversible action. It lives here (not in mountd) so
// both the spawn primitive and mountd's re-export share one identity.
var ErrHolderUnavailable = errors.New("mount holder not running")

// ErrPeerStarting means the ".lock" file is held but its owner does not answer
// the Evict probe yet — it may be mid-start (post-flock, pre-bind). The bind is
// refused; the caller (e.g. launchd) retries against whatever it becomes.
var ErrPeerStarting = errors.New("a peer owns the socket lock but is not answering yet")

// ErrLockStillHeld means Evict made way (returned true) but the contended lock
// was not released within the post-evict poll deadline.
var ErrLockStillHeld = errors.New("socket lock still held after eviction")
