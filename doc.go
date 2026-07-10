// Package fusekit is a library for hosting FUSE-T mirror mounts that survive the
// process restarts of the daemon driving them, plus the high-level overlay
// abstraction most consumers actually program against.
//
// Most consumers enter through subpackage overlay: a three-backend overlay over
// a base directory — symlink (in-process, no holder), nfs, and fskit (the two
// fuse-t backends, hosted out of process) — chosen at runtime by what the build
// can host and what a reachable mount-holder will mount. overlay.Select picks a
// Provider and reports why; overlay.Spec describes which entries are private,
// shared, excluded, or skipped, and points at the holder.
//
// Beneath overlay sit the two layers it composes:
//
//   - The mount core (this root package, under the fuse build tag): the
//     in-process fuse mount lifecycle — Config, Mount/Serve, Handle's bounded
//     teardown, MountSet's N-mount registry, and the host/cache-defeat
//     decorators.
//   - The detached mount-holder (subpackage mountd): a tiny standalone process
//     that owns the kernel mounts behind a 0600 unix socket and its frozen wire
//     protocol, so daemon restarts and upgrades never disturb live mounts. It
//     builds pure (no cgofuse). RemoteHost drives it from any build, and the
//     shared Retire helper — behind RemoteHost.Converge (one-shot) — retires a
//     version-skewed holder and remounts everything it served; a journaling
//     holder also self-retires on version skew (Server.RetireSkew).
//
// Supporting subpackages: fuset (macOS fuse-t install facts — libfuse-t path,
// Homebrew cask, availability), proc (stdlib-only process primitives — a
// single-entrant socket bind, detached spawn, exponential backoff, and
// strike/ladder breakers), state
// (a consumer's ~/.<App> private state directory and atomic status mirror),
// service (macOS LaunchAgent install/manage with Homebrew reconciliation,
// including the KeepAlive relauncher for the cask holder .app), and
// version (build metadata).
package fusekit
