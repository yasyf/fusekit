// Package proc holds consumer-agnostic process primitives: a flock
// single-entrant socket bind, a detached-child spawn, an exponential backoff,
// and sliding-window strike/ladder breakers. It never imports the root
// fusekit package or cgofuse, so it compiles on every platform and build tag.
package proc

import "errors"

// ErrChildUnavailable means a child process could not be reached or started —
// an availability condition, never a domain verdict; drivers retry rather than
// act irreversibly.
var ErrChildUnavailable = errors.New("child process not running")

// ErrSkipSpawn is the benign spawn refusal a Spawn Override or CanHost returns
// to mean there is intentionally nothing for a child to serve; callers treat
// it as a no-op, never a failure.
var ErrSkipSpawn = errors.New("nothing for the child to serve; spawn skipped")

// ErrPeerStarting means the ".lock" file is held but its owner does not answer
// the Evict probe yet (mid-start: post-flock, pre-bind); callers retry.
var ErrPeerStarting = errors.New("a peer owns the socket lock but is not answering yet")

// ErrLockStillHeld means Evict made way but the contended lock was not
// released within the post-evict poll deadline.
var ErrLockStillHeld = errors.New("socket lock still held after eviction")
