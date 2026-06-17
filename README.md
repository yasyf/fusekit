# fusekit

![fusekit banner](docs/assets/readme-banner.webp)

[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/fusekit/ci.yml?branch=main&label=CI)](https://github.com/yasyf/fusekit/actions/workflows/ci.yml)
[![License: PolyForm-Noncommercial-1.0.0](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](https://github.com/yasyf/fusekit/blob/main/LICENSE)

Detached FUSE-T mount-holder and mount-lifecycle primitives for Go.

fusekit is the mount machinery behind [cc-pool](https://github.com/yasyf/cc-pool) and
[cc-notes](https://github.com/yasyf/cc-notes), lifted into one library. It gives you a
**detached mount-holder** — a long-lived process that owns FUSE-T mounts over a frozen
unix-socket protocol, so your app's restarts and upgrades never disturb a live mount —
plus the lifecycle primitives around it: bounded mount/teardown, cgofuse-load panic
recovery, wedged-carcass cleanup, and an optional NFS data-cache-defeat decorator.

## Install

```sh
go get github.com/yasyf/fusekit@latest
```

The root package and `fusekit/mountd` build pure (`CGO_ENABLED=0`) on every platform; the
in-process FUSE host compiles under `-tags fuse` with cgo (FUSE-T on macOS, libfuse3 on
Linux).

## Quickstart

Serve a `fuse.FileSystemInterface` at a mountpoint, blocking until unmounted:

```go
h, err := fusekit.Mount(fusekit.Config{
    Base: repoRoot, Dir: mountpoint,
    FS:   myFS,
    Options: fusekit.MountOptions{Volname: "myapp-" + name, NoBrowse: true}.Build(),
    Ready:   func() bool { return mountReady(mountpoint) },
})
if err != nil { /* errors.Is(err, fusekit.ErrFuseUnavailable | ErrMountNotLive) */ }
defer h.Unmount()
```

To run mounts out-of-process, host a `mountd.Server` in a detached holder and drive it
from your CLI/daemon with a `mountd.Client` — see the [holder model](#what-problems-does-this-solve).

## What problems does this solve?

- **Mounts that outlive your process.** The detached holder owns the FUSE-T mount; your
  daemon can restart, upgrade, or crash without unmounting a live session.
- **Frozen, skew-safe wire protocol.** `mountd`'s newline-JSON protocol is versioned and
  additive-only, so a newer client and an older holder (or vice-versa) interoperate; the
  holder's reported version is injected by the consumer, never fusekit's own.
- **Bounded, never-wedging teardown.** Mount/unmount run on timeout ladders with forced
  fallback and post-verify, and `ClearCarcass` reaps the dead NFS mounts that otherwise
  freeze the host.
- **NFS cache defeat, opt-in.** A flag-gated FS decorator bumps mtime nanoseconds and
  commits on both Flush and Fsync, so same-second edits stay visible and a bad save fails
  loudly at `close(2)` — the tricks FUSE-T-over-NFS needs, off by default.

## License

PolyForm-Noncommercial-1.0.0. See [LICENSE](https://github.com/yasyf/fusekit/blob/main/LICENSE).
