# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.22.1] - 2026-06-27

### Fixed
- **`Select` now threads `ExecPath` into its holder-probe `Spawn`.** A pure-Go build with the cask installed (so `holderCanSpawn` passed on the cask `ExecPath`) hit `canHost`'s `fusekit.Built()` branch and refused with `ErrCannotHost` ("this binary cannot host fuse mounts") instead of launching the `.app` via `open -g`. Masked while the holder was already serving (`EnsureRunning` short-circuits on `Available` before `canHost`), so it surfaced only when the holder was down — e.g. a `DetectOverlayBackend`/`migrate` probe right after the holder was stopped for a cask upgrade. The `AddMount` path (`newRemoteFuse`) already threaded `ExecPath`; only the `Select` probe dropped it.

## [0.22.0] - 2026-06-27

### Changed
- **Mount teardown is graceful-only by default (`Config.ForceOnWedge`).** A macOS kernel panic (`nfs_vinvalbuf2: ubc_msync failed!`, error 22) traced to `MNT_FORCE` on a busy fuse-t/NFS mount: a graceful unmount only stalls because a live client still holds the mount busy, and forcing past its mapped pages panics the kernel. `Handle.Unmount` now escalates to a forced kernel unmount ONLY when the new `Config.ForceOnWedge` is set; the false zero value (the correct default for an in-process self-teardown) leaves a busy mount in place and returns `ErrUnmountWedged`. The shared `cmd/holder` is graceful-only for every tenant — its death-sweep (logout, reboot, SIGTERM) no longer `MNT_FORCE`-es a busy mount. When escalation IS enabled, the force now runs through the bounded `ForceUnmount` in its own goroutine raced against `forceGrace`, so a wedged `MNT_FORCE` can no longer park `Handle.Unmount` past its grace (a latent bug in the old synchronous force). Consumers that have proven a mount idle by other means and still want the old behavior set `Config.ForceOnWedge = true`.

[Unreleased]: https://github.com/yasyf/fusekit/compare/v0.22.1...HEAD
[0.22.1]: https://github.com/yasyf/fusekit/compare/v0.22.0...v0.22.1
[0.22.0]: https://github.com/yasyf/fusekit/compare/v0.21.1...v0.22.0

## [0.13.0] - 2026-06-23

### Added
- **`overlay` package** — fusekit now owns the overlay abstraction, so a consumer declares what it is and asks for an overlay without naming a backend. `Backend` (`symlink`|`nfs`|`fskit`) with `Parse`/`IsFuse`/`Available`/`Enablement`/`OpenSettings` plus `FuseBackend`; a `Provider` interface (`Backend`/`Setup`/`Sync`/`Health`/`Teardown`/`PrivateRoot`) with an in-process `SymlinkProvider` and a holder-backed `RemoteFuseProvider`; a `Spec` the consumer injects its classification into (`IsPrivate`/`Excluded`/`Shared`/`Skip`/`PassthroughOnly` plus a `HolderSpec`); `ProviderFor` (the pure resolver) and `Select` (probe-and-construct, hiding the holder/in-process asymmetry); and the migration primitives `MovePrivateEntries`/`MoveSharedOrphans`/`HasPrivateEntries`/`FusePrivateRoot` plus the `ResolvedConflictLogf` seam. A consumer declares its classification and content and asks for an overlay; the backend stays the library's to choose.

### Changed
- **Mount-up timeout errors are backend-neutral.** `mountWaitErr`, `ErrMountNotLive`, and `ErrMountTimeout` (their text and godocs) no longer name a concrete System Settings pane — the timeout is stated as a factual condition, and surfacing the pane to enable is the consumer's job via `overlay.Backend.Enablement()`, so the sentinel does not bake in one backend's enablement copy.
- **Root `backend.go` renamed to `passthrough.go`.** The file holds the `PassthroughOnly` marker; `Backend` now names the overlay type in the `overlay` package, so the root file is named for what it actually contains.

### Removed
- **BREAKING: `mountd.NetworkVolumesSettingsURL`, `mountd.NetworkVolumesTCCService`, `mountd.OpenNetworkVolumesSettings`** and the `mountd/networkvolumes*.go` files. The macOS enablement copy and the settings opener now live in `overlay.Backend.Enablement()` / `OpenSettings()`, sourced per backend (NFS → Network Volumes, FSKit → Login Items), so the right pane follows the chosen backend instead of being hard-coded to one in `mountd`.

[0.13.0]: https://github.com/yasyf/fusekit/compare/v0.12.0...v0.13.0

## [0.12.0] - 2026-06-23

### Added
- **`RemoteHost.Version` + `RemoteHost.Converge(ctx)`** — a version-skew replace so a consumer upgrade takes effect on the shared multi-mount holder without a manual restart. `Version` is the consumer's wire version (the value the holder reports through `OpHealth`); empty disables converge. `Converge` polls the holder once: a settled holder already at `Version` is the cheap no-op that runs on every session-start mount, and an unreachable or degraded holder is left for the caller's subsequent `Setup` (a degraded holder is spared — its live-mount set is unreadable, so retiring it would lose the pairs to remount). On confirmed skew it retires the stale holder (`Shutdown`, then an identity-gated peer kill (`KillPeer`) of the captured pid if the socket lingers — bounds mirror cc-notes' 5s/2s, and a successor that rebound the socket is refused, not shot), respawns the consumer's binary, and remounts every `(base, dir)` the shared holder served so the OTHER repos it hosted come back; a single failed remount heals on that dir's own next `Setup` and never fails the whole converge. The `spawnHolder` package-var seam makes the respawn leg testable without exec'ing a real holder.

[0.12.0]: https://github.com/yasyf/fusekit/compare/v0.11.0...v0.12.0

## [0.11.0] - 2026-06-23

### Added
- **Opt-in FSKit backend for pure-passthrough mounts (macOS 26+)** — a new `PassthroughOnly` marker interface a `Config.FS` may implement to declare it serves only real backing files. When such an FS is mounted and fuse-t's FSKit backend is available (`fuset.FSKitAvailable`: macOS 26+, fuse-t installed, FSKit module present), `Mount` selects `-o backend=fskit` over fuse-t's default NFS backend. Opt-in and safe-by-default: an FS that serves synthetic, `fi->fh`-keyed content (merged/injected/virtual files) must not implement the marker — the FSKit backend does not preserve `fi->fh` read semantics. `probeFS` stays unmarked so `HostProbe` keeps proving the NFS-backend "Network Volumes" TCC grant.

### Changed
- **Hooks now consume released capt-hook packs** instead of hand-copied ones, so the repo's guardrails track upstream.

[0.11.0]: https://github.com/yasyf/fusekit/compare/v0.10.0...v0.11.0

## [0.10.0] - 2026-06-23

### Added
- **`ErrLivenessTimeout`** — `RemoteHost.Health` now wraps a timed-out liveness probe with this sentinel: the mirror is unresponsive but not proven dead (fuse-t's NFS backend stalls stats under load), distinct from a definitive dead reading (not a mountpoint / base invisible), which stays a plain error. A caller can debounce a saturated-holder blip instead of tearing down and remounting a live mirror.

### Fixed
- **Orphaned go-nfsv4 servers are reaped (darwin).** A forced fuse-t unmount can leave the NFS server backing a mount alive after the mountpoint is gone, so a later mount stacks a second server on the same dir (observed: duplicate go-nfsv4 per account). `Handle.Unmount` (after the mountpoint is confirmed gone) and a guarded pre-mount sweep now SIGKILL any go-nfsv4 child of the holder still bound to the dir, found via `kern.proc.ppid` + `kern.procargs2` — bounded, and never opening the wedged mount the way lsof would.

[0.10.0]: https://github.com/yasyf/fusekit/compare/v0.9.0...v0.10.0

## [0.9.0] - 2026-06-23

cc-pool and cc-squash both render a `service status` management block from the same `service.Agent` brew/launchctl primitives, and each hand-rolled the composition. Lifting it into the library removes the divergence. Additive — one new `service` method — so it is safe to bump from 0.8.x.

### Added
- **`service.Agent.StatusLines()`** — the management block a consumer's `service status` prints: whether the daemon is Homebrew- or self-managed, plus the matching detail (the `brew services info` body, or whether the LaunchAgent is loaded). The brew/launchctl primitives (`IsBrewManaged`/`BrewInfo`/`Loaded`) were already here; this composes them once so cc-pool and cc-squash stop re-deriving the same branch, with consumers appending their own daemon-health and socket lines.

[0.9.0]: https://github.com/yasyf/fusekit/compare/v0.8.0...v0.9.0

## [0.8.0] - 2026-06-23

cc-pool needs a one-command "set up the live-mirror overlay" flow (`ccp fuse enable`), and the parts that are not cc-pool-specific belong in the library named after fuse-t. Additive — a new `fuset` package plus two `service` helpers — so it is safe to bump from 0.7.x.

### Added
- **`fuset` package** (pure) — the install-time facts about FUSE-T shared by every consumer that offers to set it up: `Cask` (the `macos-fuse-t/homebrew-cask/fuse-t` reference; fuse-t ships only as a cask, which is why a consuming formula cannot depend on it), `Dylib` (the `/usr/local/lib/libfuse-t.dylib` cgofuse dlopens), `Installed()` (a cheap stat — no dlopen, no probe mount), and `Install(out, errOut)` (installs the cask via Homebrew, streamed). Distinct from the per-platform RUNTIME pin (`CGOFUSE_LIBFUSE_PATH`), which stays consumer-side.
- **`service.InstallCask(ref, out, errOut)`** — `brew install -y --cask <ref>`, streamed; auto-taps a tap-qualified ref. `fuset.Install` wraps it.
- **`service.Agent.BrewReinstall(out, errOut)`** — `brew reinstall <formula>`, streamed; for a consumer that ships install-time-selected build variants and must re-run its formula after a dependency lands.

[0.8.0]: https://github.com/yasyf/fusekit/compare/v0.7.0...v0.8.0

## [0.7.0] - 2026-06-23

### Added
- **`ErrMountFailed`** — `fusekit.Mount` now distinguishes a serve-exit (a hard `mount(2)` rejection: fuse-t missing/unloadable, the kernel refusing the mount) from a mount-up timeout. The former returns this sentinel; the latter keeps the proven/unproven `ErrMountTimeout`/`ErrMountNotLive` split, so a hard failure no longer masquerades as the one-time Network Volumes TCC grant still being pending. The mountd probe op ships the error class over the wire (`RemoteHost` dual-wraps `ClassMountFailed` back to `fusekit.ErrMountFailed`), so a consumer can tell "fuse cannot mount on this machine" from "the grant is still pending".

### Changed
- **BREAKING: `HostProbe()` is now `(bool, error)` and `mountd.Server.Probe` is now `func() (bool, error)`** — carrying the hard-failure-vs-pending-grant classification through the probe path instead of collapsing it to a bare bool.

[0.7.0]: https://github.com/yasyf/fusekit/compare/v0.6.0...v0.7.0

## [0.6.0] - 2026-06-22

A second consumer (cc-squash's Go control plane) needs the same daemon plumbing cc-pool already had, so three more pure primitives are lifted out of cc-pool, siblings to `proc`/`mountd`. Additive — no existing package changes — so it is safe to bump from 0.5.x.

### Added
- **`version` package** (pure) — the consuming binary's build metadata: `Version`/`Commit` ldflags vars plus `String()` (with the `go build` BuildInfo fallback). It holds the CONSUMER's version, never fusekit's own — fusekit's module version stays off every wire, per the mountd protocol freeze — so every consumer injects `-X github.com/yasyf/fusekit/version.Version=…` once instead of re-deriving the version string the `Supervisor` / `mountd.Server` consume.
- **`state` package** (pure) — a consumer's private per-user state directory: `Dir{App}` → `Root()` / `Path(leaf)` / `Ensure()` for the `~/.<App>` layout, plus `AtomicWrite` (temp + rename) for the out-of-process status mirror a status command or menu-bar widget reads. App-agnostic, so consumers share one home-resolution-and-temp-rename primitive instead of each re-deriving it.
- **`service` package** (pure; the launchctl/brew calls are macOS-only at runtime) — installs a consumer's long-lived daemon as a user LaunchAgent: `Agent{Label, Formula, Program, Args, LogPath, Env}` with `Install` / `Uninstall` / `Loaded` (the bootout → bootstrap → enable → kickstart choreography) and the Homebrew reconciliation (`IsBrewManaged`, `BrewStart` / `BrewStop` / `BrewKickstart` / `BrewInfo`). The launchctl dance and brew detection are generic; only the `Agent` fields vary per consumer.

[0.6.0]: https://github.com/yasyf/fusekit/compare/v0.5.1...v0.6.0

## [0.5.0] - 2026-06-21

The first real `Supervisor` consumer surfaced three additive hooks the generic state machine was missing: a way to render booked spawn failures, a way to force an immediate retry, and a way to drive the supervisor through a consumer's own spawn seam. All three are additive and backward-compatible — nil hooks preserve v0.4.0 behavior exactly.

### Added
- **`Supervisor.OnSpawnError`** — called with each booked `Spawn.EnsureRunning` / verify failure so the consumer can surface it (a status field, a once-per-text log); it takes no irreversible action — the Supervisor still owns the retry policy (backoff, breaker). A clean bring-up never calls it; nil discards the failures (v0.4.0 behavior).
- **`Supervisor.ClearBackoff()`** — drops the spawn-backoff floor so the next `Tick` retries the spawn immediately (e.g. a consumer that observed the host become available again), without touching the crash-loop breaker count — a child that keeps looping still trips `Retreat` at the same threshold.
- **`Spawn.Override`** — when non-nil, fully replaces `EnsureRunning`'s detached-spawn body after the `Available` short-circuit (its error is returned verbatim, `CanHost` is never consulted, no child is exec'd), so a consumer that already owns a spawn seam drives the `Supervisor` through `proc.Spawn` without proc exec'ing `os.Executable()`. nil preserves the real detached spawn (v0.4.0 behavior).

[0.5.0]: https://github.com/yasyf/fusekit/compare/v0.4.0...v0.5.0

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
