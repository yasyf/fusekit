# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

[Unreleased]: https://github.com/yasyf/fusekit/compare/v0.4.0...HEAD

## [0.4.0] - 2026-06-21

The daemon-lifecycle layer cc-pool had kept private — the single-entrant socket bind, the detached spawn, and the holder supervisor — is lifted into a new pure `proc` package, so a second consumer (cc-squash's Go control plane) can supervise a detached, versioned child without re-porting it. Additive: `mountd`'s public API and behavior are unchanged (it now consumes `proc` internally), safe to bump from 0.3.x.

### Added
- **`proc` package** (pure, `CGO_ENABLED=0`, imports only the standard library — never the root `fusekit` or `mountd`):
  - `SingleEntrant` — the generic flock single-entrant socket bind (never-unlinked lock, stale-socket rebind, 0600) behind one `Evict` policy seam, so a holder (never evicts a live peer) and a daemon (evicts a version-skewed peer, then polls the lock to rebind) share one primitive over their different wire protocols.
  - `Spawn`/`EnsureRunning` — detached `setsid` spawn + socket-wait with `Available`/`CanHost` hooks and the stable-exec-copy machinery (TCC-grant persistence). `mountd.Spawn` is now a thin fuse-gated wrapper over it; `ErrHolderUnavailable` moves here (re-exported from `mountd` for `errors.Is` compatibility).
  - `Backoff` — the doubling-to-cap respawn backoff.
  - `Supervisor` + `Policy` — the generic respawn / crash-loop-breaker / version-skew-replace / peer-gated-kill state machine for any detached, versioned child reached over a socket (the mount holder, or a non-mount child such as a proxy). Child-control (`Probe`/`PeerAlive`/`Shutdown`/`WaitGone`/`Kill`) and the desired-state/retreat policy are consumer hooks. Three safety contracts are enforced and pinned by tests: `PeerAlive` gates every destructive arm (an alive-but-wedged child is spared, never force-revived/killed on a normal tick), the crash-loop breaker resets **only** on a healthy settle at `MyVersion` (a child looping at a skewed version still trips the retreat), and the `Replace` finalizer fires exactly once on every exit path including context cancel.

### Changed
- `mountd` `listen()` and `Spawn.EnsureRunning()` now delegate to `proc`; the duplicated flock-bind and spawn machinery is gone. The `mountd` exported surface (`Server`, `Spawn`, `RemoteHost`, `Client`) is unchanged.

[0.4.0]: https://github.com/yasyf/fusekit/compare/v0.3.0...v0.4.0

## [0.3.0] - 2026-06-20

### Added
- **`Spawn.StableExecDir`** — the holder binary is materialized as a stable copy and spawned from there, so the macOS "Network Volumes" TCC grant survives version upgrades (the embedded Developer-ID designated requirement survives the copy), plus the Network Volumes deep-link helpers.

[0.3.0]: https://github.com/yasyf/fusekit/compare/v0.2.0...v0.3.0

## [0.2.0] - 2026-06-20

Supervised-holder primitives lifted out of cc-pool, so any consumer running a long-lived mount holder gets the same meltdown-safe peer-gated kill and a single poll verdict instead of reimplementing them. Additive — no existing API or behavior changes, safe to bump from 0.1.x.

### Added
- **Peer-gated kill on `Client`** (darwin; a non-darwin stub reports unsupported): `PeerPID`, `PeerAlive`, `Kill`, and the identity-gated `KillPeer(wantPID)`, plus the `ErrUnreachable` sentinel. They resolve the socket's exact peer via `LOCAL_PEERPID` and signal it in one dial — never by name, and never pid≤1 or the caller itself.
- **`Client.Poll`** returning a `PollResult{Reachable, Degraded, Version, Mounts}` verdict that distinguishes an unreachable holder (Health failed) from a degraded one (Health OK but List failed — alive at a known version, mounts unreadable) from a healthy one, so a supervising consumer never re-derives that state from raw call outcomes.

[0.2.0]: https://github.com/yasyf/fusekit/compare/v0.1.1...v0.2.0

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
