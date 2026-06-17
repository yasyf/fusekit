# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

[Unreleased]: https://github.com/yasyf/fusekit/compare/v0.1.1...HEAD

## [0.1.1] - 2026-06-17

Test-only release surfaced by the cc-pool migration. No public API or behavior change, so it is safe to bump from 0.1.0.

### Added
- Deadline-edge `waitMounted` coverage, so a mount that lands while the final poll straddles the deadline is kept, not reported dead.
- darwin `Mounted` coverage for the `mountpointIn` and `mountCandidates` paths.

[0.1.1]: https://github.com/yasyf/fusekit/compare/v0.1.0...v0.1.1

## [0.1.0] - 2026-06-17

Initial release — cc-pool's FUSE-T mount machinery extracted into a reusable library, with cc-notes' robustness folded in.

### Added
- **Mount-core primitives** (pure, `CGO_ENABLED=0`): `MountOptions` (with the macOS-forced `noattrcache` rule), `VersionNsec`, `StatProbes`, `Mounted`/`MountAlive`/`MountAliveWithin`, `ForceUnmount`, `ClearCarcass`, and the root sentinels (`ErrMountNotLive`, `ErrMountTimeout`, `ErrUnmountWedged`, `ErrForceUnmountTimeout`, `ErrFuseUnavailable`).
- **In-process fuse host** (`-tags fuse`): `Mount`/`Serve`/`Config`/`Handle` with cgofuse-load panic recovery, bail-the-readiness-wait-on-serve-exit, and bounded teardown; the `CacheDefeat` NFS-cache-defeat decorator (mtime-nanosecond bump + commit-on-Flush-and-Fsync); `MountSet`, `HostProbe`.
- **`fusekit/mountd`** (pure): the detached mount-holder — a frozen newline-JSON wire protocol (`MountProtoVersion=1`), `Client`, the narrow `Host` seam, a `Version`-injected `Server`, `Spawn` (with the `ErrCannotHost` gate), and `RemoteHost`.
- Cross-platform: darwin (FUSE-T) and Linux (libfuse3) fuse builds; darwin-only syscalls split into `_darwin`/`_other` files.

[0.1.0]: https://github.com/yasyf/fusekit/releases/tag/v0.1.0
