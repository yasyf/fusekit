# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Consumer deployment uses the product's fixed signed app.** Public guidance
  now embeds `holder.Runtime` in an existing consumer app, or in a dedicated
  `$HOME/Applications/<Product>Helper.app` when no such app exists. FuseKit
  ships no generic standalone application and exposes no user-facing holder
  product.

## [1.10.1] - 2026-07-23

### Fixed

- **Release gates exercise schema mismatch and deadline settlement without
  teardown races.** The catalog mismatch fixture now supplies a structurally
  valid foreign digest, and deadline-canceled holder tests join retained worker
  settlement before removing their runtime directory.

## [1.10.0] - 2026-07-23

### Changed

- **Catalog storage declares one explicit fresh-v1 schema identity.** Every
  catalog records `github.com/yasyf/fusekit/catalog`, numeric version `1`, and
  the exact DDL digest in a single strictly shaped metadata row. Open rejects
  missing, extra, legacy, or mismatched metadata without migration or repair.

## [1.9.1] - 2026-07-23

### Fixed

- **Convergence fixtures carry the exact File Provider activation proof.** The
  FUSE-tag release gate now confirms test domains with the required holder
  activation generation instead of constructing an incomplete broker result.

## [1.9.0] - 2026-07-23

### Changed

- **Tenant preparation now returns one exact typed presentation proof.** Every
  request names `mount` or `file_provider` and carries the holder activation
  generation observed immediately before the call. Mount proofs return the
  exact tenant path; File Provider proofs return the domain identity and the
  public path observed from `NSFileProviderManager.getUserVisibleURL`. A holder
  takeover, stale tenant generation, lost domain, ambiguous proof shape, or
  synthesized CloudStorage path is rejected.
- **File Provider identity is presentation-owned.** The hard API and schema cut
  renames `AccountInstanceID` to opaque `PresentationInstanceID` throughout
  Go, Swift, catalog storage, and both generated protocols. Consumer account
  vocabulary no longer crosses the FuseKit boundary, and no compatibility
  alias or old field reader remains.
- **Native presentation is optional in the embedded holder plan.** File
  Provider-only products omit FUSE verification, native mount startup, route
  services, and native worker capacity. Runtime health reports native disabled
  and requires the signed broker to be live; mixed and native-only products
  retain their exact native proof. Tenant declarations now carry native path
  state only in the typed `MountSpec`; File Provider-only tenants have no
  caller-supplied presentation root.

### Fixed

- **File Provider readiness is activation-fenced and loss-aware.** Preparation
  durably invalidates the prior observation before asking the current signed
  broker to re-observe or recreate the exact OS domain. Only the resulting
  absolute public path, tenant generation, domain ID, and holder activation can
  satisfy the caller; restart never trusts a persisted path without a fresh OS
  observation.
- **Catalog-worker identity includes nested domain wire shape.** Changes to the
  persisted File Provider domain proof now change the catalog-worker and suite
  fingerprints instead of hashing only the outer Go type name.

## [1.8.1] - 2026-07-23

### Fixed

- **The latest-wins tenant test now preserves its explicit actor barrier.** The
  canceled initial verification stays resident until all concurrent requests
  are admitted, and every result proves its caller's original requested
  revision before the final revision is asserted exactly. Valid intermediate
  superseding proofs no longer fail the release gate based on goroutine
  scheduling.

## [1.8.0] - 2026-07-23

### Changed

- **DaemonKit is pinned exactly at 0.8.1 across Go and Swift.** The release
  checker derives the one required version from the module and package files
  and rejects any Go/Swift/resolution drift.
- **Holder readiness is one explicit plan-owned contract.** Signed startup,
  failure settlement, and the outer service observation have one exact budget;
  the observer is structurally unable to preempt startup plus cleanup.
- **Cross-process stop authority is consumer-owned and receipt-bound.** Every
  holder config supplies an exact stop role and the consumer controller's
  process store. The holder composes daemonkit's receipt verifier with its
  private signed-runtime classifier and reserves stop capacity alongside the
  native and source-capable broker sessions; FuseKit does not infer authority
  from tenant ownership or its own process registry. Consumers launch the one
  Fuse-owned private stop-child marker; FuseKit supplies the generated wire
  identity and exact runtime protocol while daemonkit owns hidden transport
  framing and timing.
- **Runtime health is one immutable suite-qualified observation.** The bounded
  `fusekit.runtime.health` response carries the runtime build and protocol,
  canonical daemon PID and process generation, distinct FuseKit activation
  generation, lifecycle state, draining, busy, and ready flags, readiness
  phase and last step, native mount proof, and broker phase. Orderly drain is
  an explicit `draining` state and phase with `ready=false`; broker readiness
  is reached at authenticated bind and never waits for File Provider domain
  reconciliation.

### Fixed

- **Ordinary admission opens only after daemon readiness is published.** An
  internal publishing phase satisfies daemonkit's health callback while
  remaining externally `starting` and rejecting ordinary work; callback
  success alone atomically publishes `ready`.
- **Startup failures retain their exact terminal phase.** Listener, native,
  broker, recovery-receipt, and publication progress is emitted with the
  runtime build and activation generation, so an outer installer can diagnose
  the actual boundary from the one public observation.

## [1.7.7] - 2026-07-23

### Changed

- **DaemonKit is pinned exactly at 0.7.0 across Go and Swift.** The Swift
  broker and catalog transports use the hard async session API, including
  asynchronous connection, request admission, cancellation, and exact server
  startup and shutdown settlement.
- **File Provider construction performs no socket I/O.** The synchronous
  extension initializer validates configuration only; the first asynchronous
  catalog operation lazily establishes one coalesced authenticated session,
  and a failed connection can be retried without retaining stale session state.

### Fixed

- **Native readiness is proven by one causally bound external callback.** The
  native child publishes its exact mount identity and a random single-use
  token; the holder launches the fixed signed runtime as a disposable probe,
  and the child accepts only that token's reserved hidden lookup before
  fencing `RootReadEpoch` and publishing final readiness. The proof performs
  no tenant enumeration or catalog access, rejects stale and overlapping
  tokens, and emits phase telemetry using only a non-secret derived probe ID.
- **Settled native shutdown failures replay deterministically.** A canceled
  caller can no longer win a ready/ready select and hide the already-settled
  native failure while the runtime is draining.

## [1.7.6] - 2026-07-21

### Changed
- **Daemon lifecycle dependencies are exact at daemonkit 0.4.1.** Go and Swift
  now share the released durable process-identity, canonical executable, and
  runtime defense fixes used by the rest of the hard-cut fleet, including
  SIGPIPE-safe Swift socket writes and exact child-session teardown.

### Fixed
- **Native child loss leaves the process lane before resource settlement.** The
  disposable child aborts its transport instead of issuing an unbounded unbind,
  and the holder publishes transport loss before exact catalog-and-pin cleanup,
  so a wedged cleanup cannot keep a reaped child falsely live.
- **The source release chain now publishes the complete native hard cut.** The
  exact presentation root, causal runtime health proof, and bounded Darwin
  unmount contracts from 1.7.5 are carried forward without moving that
  immutable tag, with complete comparison metadata for the release gate.

## [1.7.5] - 2026-07-21

### Changed
- **Holder plans require the exact native presentation root.** Runtime and
  deployment plan specs now carry a required, disjoint presentation root below
  the user's home. FuseKit no longer derives that product-owned path below its
  private runtime directory.

### Fixed
- **Runtime readiness proves the exact live native presentation.** The exact v1
  mount transport reports the holder activation generation, native phase, and
  the mount identity plus causal catalog through-proof; a connected socket is
  no longer sufficient readiness evidence.
- **Darwin native unmount settlement is bounded.** The disposable native child
  issues one regular unmount and returns on a fixed deadline without joining a
  wedged syscall or mount host, leaving daemonkit TERM/KILL/reap authoritative.

## [1.7.4] - 2026-07-21

### Fixed
- **Native presentation startup is exact and crash-contained.** The holder now
  creates the private presentation root before launch, the native child proves
  its ownership, mode, emptiness, and unmounted state before entering FUSE-T,
  and readiness requires the exact FUSE-T mount plus a causal through-mount
  catalog read. Teardown displaces cgofuse's forced-unmount signal handler,
  performs one regular unmount, and retains the session until all FUSE workers
  settle or daemonkit kills and reaps the disposable child.
- **Tenant lifecycle authorization delegates to the consumer policy.**
  Provision, replace, and remove no longer require the caller to be the signed
  runtime executable; the configured mount authorizer admits the ordinary
  product daemon while FuseKit still pins its immutable product owner.

## [1.7.3] - 2026-07-21

### Fixed
- **Broker authentication can complete before runtime presentations start.**
  Only `broker.open` bypasses the starting-state admission gate, and it still
  traverses the normal role, route, signature, and protected-peer checks. All
  other catalog operations remain rejected until the holder is ready.

## [1.7.2] - 2026-07-21

### Fixed
- **Broker capability is no longer mistaken for current File Provider demand.**
  A broker-capable holder may start from an empty or mount-only catalog and
  later provision its first File Provider tenant without changing generations.
  Brokerless generations still reject File Provider topology. Pre-start
  topology-controller cleanup is terminal and idempotent, so an earlier
  activation failure is never masked by a secondary cleanup error.

## [1.7.1] - 2026-07-21

### Changed
- **DaemonKit v0.3.1 is the exact runtime dependency.** Go and Swift now share
  the same published pin required by downstream SwiftPM graphs.

## [1.7.0] - 2026-07-21

### Changed
- **BREAKING: convergence bootstrap causality is exact.** Fresh tenant creation
  and full-authority snapshot publication now use the `bootstrap` cause. The
  misleading `migration` enum and wire value are deleted without an alias or
  reader; the exact-v1 catalog schema and generated Go/Swift protocol reject it.
- **DaemonKit v0.3.0 is the exact runtime dependency.** Go and Swift now pin the
  hard-cut release that removes the unused Go App Group surface.

## [1.6.2] - 2026-07-20

### Fixed
- **Linux source identity now uses stable filesystem birth time.** FuseKit reads
  `STATX_BTIME` from the pinned file descriptor and fails closed when the
  filesystem cannot provide it, instead of treating mutable inode `ctime` as
  identity and rejecting its own valid namespace mutations as source changes.

## [1.6.1] - 2026-07-20

### Fixed
- **Signed holder requirement verification now evaluates the signature instead
  of comparing `codesign` display text.** Developer ID output may reorder or
  normalize an equivalent designated requirement; FuseKit now verifies the
  canonical same-Team requirement semantically for both the outer consumer app
  and nested FUSE library, and records that canonical policy in the manifest.
- **Hard-cut CI fixtures now construct exact v1 state.** Catalog WAL cleanup,
  catalog-worker mutation setup, observer receipt identity, and tenant desired
  state tests no longer double-release transactions, mix source and direct
  mutations, omit required fleet identity, or wait on an unprovisioned tenant.

## [1.6.0] - 2026-07-20

### Added
- **One revisioned catalog and tenant runtime.** Opaque object IDs,
  transactional namespace mutations, immutable snapshot handles, paged
  snapshots and deltas, generation-fenced tenant actors, coalesced
  `PrepareTenant`, demand-aware convergence, and generic Go/Swift mount and File
  Provider presentations now share one state model.
- **One authority-owned source runtime.** Bounded source snapshots and deltas,
  opaque target sets, fenced mutation reservations, durable receipts,
  authoritative watermarks, and exact recovery replace consumer-owned watcher,
  manifest, registry, and spool machinery.
- **A fixed signed-child runtime.** `holder.Runtime` composes daemonkit
  lifecycle, exact persistent sessions, peer trust, disposable workers, and one
  native mount root. `holder.NewRuntimePlan` derives the exact `RuntimePlan` and
  daemon-facing `DeploymentPlan` from the consumer-owned signed app identity.
  Readiness is accepted only from the exact authenticated session and
  PID/start-time/boot identity recorded before execution.
- **A bundled FUSE-T runtime boundary.** Packaging copies the exact
  versioned FUSE-T 1.2.7 regular-file input into the consumer app, verifies its
  bytes and Mach-O dependencies, signs it inside-out with the consumer Team ID,
  and pins the bundled path and digest while rejecting the complete
  code-injection entitlement set.

### Changed
- **BREAKING: role-aware private transport and exact protocol suite v1.** Unsigned
  same-UID product daemons can publish sources and prepare tenants only after
  product authorization; signed broker, mount-presentation, and native-child
  roles retain exact runtime-plan verification. Catalog, catalog-worker, mount,
  source-driver, source-task, observer, manifest, and deployment epochs all
  start at v1 with exact equality and no compatibility reader. Source
  publications require authority-owned tenant root keys, and source-mutation
  planning receives only durable opaque locators at an exact causal revision.

### Removed
- **BREAKING: every pre-v1.6 filesystem surface.** `mountd`, `MountSet`,
  `MountSpec`, in-process mount/live APIs, content bridge/tree APIs, `holderfs`,
  overlay selection, legacy File Provider control, per-directory lease and
  strike state, holder retirement journals/breakers, frozen newline-JSON wire,
  feature negotiation, and compatibility errors are deleted without adapters.
- **The standalone FuseKit holder app, cask, VM stress driver, and historical
  holder harnesses.** Each consumer now owns its fixed signed application,
  bundle identity, entitlements, and isolated-VM acceptance suite.

## [1.5.0] - 2026-07-19

### Removed
- **BREAKING: the `proc`, `service`, `appgroup`, and `version` packages** — the fleet's daemon plumbing moved to [daemonkit](https://github.com/yasyf/daemonkit), and fusekit now consumes `daemonkit/proc`, `daemonkit/service`, `daemonkit/bundle`, and `daemonkit/version` directly. Importers switch to the daemonkit paths; the fusekit sentinels that moved (`proc.ErrChildUnavailable` and friends) keep `errors.Is` identity through daemonkit. Consumers that stamped `-X github.com/yasyf/fusekit/version.Version` now own their build stamp (a `main`-package var) and render it via `daemonkit/version.Running` — the 0.6.0 injection instruction below is superseded.

### Changed
- **`mountd.SkewCheck` retires only on a strictly newer installed build.** The check compares via `daemonkit/version.Newer` against the installed bundle's short version, so reinstalling the same build — or a downgrade — no longer retires a live holder.

## [1.2.0] - 2026-07-14

### Added
- **`content.BridgeClient.SelfTest` — a manifest-plus-read bridge round-trip
  health check.** `SelfTest(ctx, domain, name)` lists the domain's manifest,
  confirms `name` is present, then `Read`s it — so a consumer's doctor can
  exercise the whole bridge path (connect, manifest, fetch computed bytes) in
  one call instead of inferring liveness from a bare dial. Transport sentinels
  are preserved: a bound-but-dead server surfaces as `ErrBridgeUnavailable`
  without `ErrBridgeDialRefused`, while a served manifest that lacks the entry
  is a plain content error, never a transport sentinel. SelfTest proves only
  the server answering the socket — a caching relay (`content.RelaySource`)
  can answer from warm cache after its origin dies.
- **`fileproviderd.ErrAppDialRefused` — `ErrAppUnavailable`'s dial-refusal
  subset.** An `AppClient` dial that fails with `ECONNREFUSED` or `ENOENT` on the
  socket path (the companion app is not up) now surfaces `ErrAppDialRefused`
  rather than a bare `ErrAppUnavailable`; a connection that succeeds and then
  fails mid-RPC stays plain `ErrAppUnavailable`. It wraps `ErrAppUnavailable`
  (itself an alias of `proc.ErrChildUnavailable`), so existing `errors.Is`
  classification is unaffected while callers that must tell "app is down" from
  "app is up but the op failed" now can. Mirrors the existing
  `content.ErrBridgeDialRefused` split on the File Provider control socket.
- **`proc.Flock` — a ctx-aware polling cross-process advisory lock.**
  `Flock(ctx, path)` takes an exclusive `flock(2)` lock, creating the lock file
  and its parent dir first, and returns a `*FlockHandle` whose `Release` drops
  the lock (the lock file is left on disk on purpose — unlinking under `flock`
  races other holders). It polls `LOCK_EX|LOCK_NB` on a 25ms interval instead of
  blocking in the syscall, so context cancellation is observed promptly and no
  goroutine leaks on a stuck holder.

## [1.1.0] - 2026-07-14

### Added
- **Shallow `probe-domain` arm — a non-materializing readiness verdict.** The
  frozen proto-1 request gains an optional `shallow` flag and the response an
  optional `listed` pointer; a shallow probe does a domain lookup +
  `getUserVisibleURL` + a readdir of the domain root only (no byte read, no
  materialization) and reports whether `.claude.json` appears in the listing.
  `AppClient.ProbeDomainShallow`, `RemoteDomainHost.ProbeDomainShallow`, and
  `overlay.FileProviderProvider.ProbeDomainShallow` forward it. An app predating
  the flag ignores it and answers a deep `probe-domain`; the client detects the
  absent `listed` and derives the verdict from the deep byte-count shape (the
  designed skew). Error-class mapping is identical to `ProbeDomain`, unknown-op
  default arm included (`ErrOpUnsupported`).
- **`prepare-domain` control op — force-materialize a domain's computed
  settings.json.** New `OpPrepareDomain` with an optional `deadline_ms` request
  field (0 = the app's ~30s default); the app resolves the domain, confirms the
  settings.json item enumerates, `requestDownloadForItem`s it, and waits bounded
  by the deadline, so a live session's first read never blocks on a cold File
  Provider fetch. `AppClient.PrepareDomain`, `RemoteDomainHost.PrepareDomain`, and
  `overlay.FileProviderProvider.PrepareDomain` forward it; a not-enumerated item or
  a timed-out download is `ErrDomainNotServing`, an app too old to know the op is
  `ErrOpUnsupported` (the provider prefixes its upgrade hint).
- **`content.Fingerprint` and `content.FreshnessVersion` — deterministic
  content-change keys.** `FreshnessVersion` hashes each path's `(path, mtime_ns,
  size)` in the given order (ENOENT contributes a stable absent marker, any other
  lstat errno fails loud); `Fingerprint` hashes a manifest's entries sorted by
  name over their identity fields plus each Freshness path's live lstat.
- **`overlay.FileProviderSpec.Source` — fingerprint-gated enumerator signals.**
  With a `content.Source` wired, `Sync` and `Health` route their nudge through one
  `signalIfChanged`: they compute `Fingerprint(Manifest(domain))` and `Signal`
  only when it moves, recording the new fingerprint solely on a successful signal
  (a failed signal retries next `Sync`). A `Manifest`/`Fingerprint` error fails
  loud with no signal-anyway fallback. A nil `Source` preserves the unconditional
  signal-every-`Sync` (a documented opt-in, like `HolderSpec.AttrCache`). The new
  exported `FileProviderProvider.Signal` is the unconditional nudge that bypasses
  the cache, for recovery ladders that must not be neutered by it.

### Changed
- **`overlay.FileProviderProvider.Sync` and `Health` no longer swallow
  `ErrAppUnavailable` from a `Signal`.** A signal against a momentarily-down app now
  surfaces the transient error wrapped, so callers `errors.Is`-classify and
  debounce on it rather than treating a dropped signal as success.

## [1.0.1] - 2026-07-12

### Fixed
- **Journal replay skips the teardown seize on a bare mountpoint.** After a
  non-quiesced holder death, a row whose dir had no mount at all (the mount
  died with the old holder) still ran the pre-mount clear through the lease
  ladder: the EX seize bounced off the live session's SH lease and the row
  deferred indefinitely, while the equivalent consumer mount of the same bare
  dir succeeded (VM finding F-2). Replay now consults mount-table truth —
  `MountedCheck`, the same getfsstat primitive `confirmMounted` trusts; never
  a stat, which proves nothing on a bare dir and can hang on a wedged one —
  and when nothing is mounted it skips the seize and mounts straight over the
  held lease, mirroring the consumer-mount path. A mounted dir keeps the
  existing ladder byte-for-byte, and an undetermined mount-table read falls
  through to the ladder, fail-closed; both branches and the error branch are
  regression-pinned.
- **`vmctl archive` calls `cc-notes log append --entry`** (the `-m` flag was
  renamed in cc-notes v0.27).

### Added
- **Phase-5 VM validation scenarios** (`scripts/vm/scenarios/p5-*.sh` with
  `p5lib.sh`/`p5reset.sh` and the `p5util` guest driver): the nine holder-v2
  scenarios that gated the fleet migration — lease-gated retire, no-force-on-
  hang, TERM displacement and replay, kill-9 carcass reap, journal v2 decode
  of a live snapshot, lease-agent shell-death release, contentd-before-mounts
  replay ordering, `ClassBusy` provenance, and owner isolation — updated to
  the v1.0.1 replay contract, with a reset that refuses to wipe holder
  bookkeeping while a wedged mount survives.

## [1.0.0] - 2026-07-12

Holder v2: one shared multi-tenant holder whose every safety decision derives
from kernel ground truth at action time — flock leases and bounded stat
verdicts — never journaled or pushed consumer intent. Breaking across the wire
(proto 2), the journal (v2), and the Go API.

### Fixed
- **Journal replay defers on a refused content socket instead of striking.**
  A replay racing the consumer daemon's own login-time start (launchd offers
  no inter-agent start ordering) dialed a not-yet-listening content socket,
  burned the row's bounded strikes, and dropped it for good. The dial-refusal
  class — ECONNREFUSED, or ENOENT on the socket path
  (`content.ErrBridgeDialRefused`, wire `ClassContentDialRefused`) — now keeps
  the row journaled and retries it on a capped backoff for the holder's
  lifetime; health surfaces the waiting rows as `ContentDeferred`
  (`FeatureContentDeferred`). Every other mount failure strikes exactly as
  before, and a row that never becomes serviceable stays visible as deferred
  forever — loud, never dropped.

### Added
- **`lease` package — per-dir flock session leases.** Deterministic lease
  files on local APFS (`~/.fusekit/leases/<sha256(dir)[:16]>.lease`, dir
  hashed byte-identical). `Acquire` opens non-CLOEXEC via raw `syscall.Open`
  and takes `LOCK_SH`, binding the lease to the open-file-description
  refcount across the whole session tree: it releases when the last
  inheritor's descriptor closes, and only then. `Seize` is the teardown-side
  `LOCK_EX|LOCK_NB` fence, held across the entire action and unlinked under
  EX on release; busy surfaces `ErrBusy` plus the acquirer's advisory
  provenance. `Probe`/`List` are read-only diagnostics. No TTL, no revoke,
  no daemon in the loop.
- **Proto 2 `hello`/features negotiation.** `Client.Hello` returns the
  holder's version and feature set (`mux`, `bridge`, `tree`, `lease-gate`);
  `HelloInfo.Require` replaces every `MinHolderVersion` comparison and
  `IsUnknownOp` probe. Proto-1 requests are refused with an error naming the
  fix per direction; `ErrProtoMismatch` classifies the reply.
- **`leases` op** — the holder's read-only lease-file diagnostic — and a
  lease summary (`leases_total`/`leases_held`) in `health`.
- **Idempotent-mount journal rewrite.** A mount OK on an already-live pair
  rewrites the journal row when ANY spec field differs, so a successor never
  replays a stale spec.
- **`persist-warning` feature + `Response.Warning`.** A drop
  (unmount/removebridge/reclaim/addbridge) whose journal write fails still
  acks OK — the kernel state already changed — carrying an explicit warning;
  memory keeps kernel truth (no rollback on drops) and the next full-snapshot
  save heals the file.

### Changed
- **The lease ladder is the one busy/liveness decision procedure** (retire
  gate, retire sweep, journal replay, unmount/reclaim,
  remount-of-dead-mirror): a live lease defers with provenance
  (`ClassBusy`); a free lease is seized exclusively across the whole
  graceful teardown (an in-kernel TOCTOU fence); anything unproven defers.
  Owner is required and `validOwner`-checked on every op except
  hello/health/probe; `list`/`bridges` take `all:true` for a read-only
  cross-tenant view.
- **Carcass proof v2 gates the fleet's ONLY force path.** `ClearCarcass`
  forces iff: the dir's stat answers a dead errno (ENOTCONN/EIO/EPERM/
  EACCES) within an explicit immediacy bound — a HANGING stat is NEVER
  proof of death (`ErrCarcassUndetermined`); the dir is a current kernel
  mountpoint (errno provenance via getfsstat, no server I/O); death is
  revalidated immediately before the force; and the caller holds the seized
  lease fence. Exactly two holder-internal sites reach it: the pre-mount
  clear and the journal-replay clear. The go-nfsv4 reap's kill-time
  reconfirm now re-checks the scan-time process start second, so a reused
  pid is never shot.
- **`MountSet.Teardown(base, dir)` is graceful-only** — the handle-less
  MNT_FORCE branch and the `carcassPolicy` parameter are gone; a leftover
  carcass surfaces as `ErrUnmountWedged` for the pre-mount/replay clear.
- **Journal v2 is re-serve identity only**; legacy journals'
  `idle_policy`/`carcass_policy` fields decode away via Go's unknown-field
  ignoring — no migration. A lease-deferred root keeps its entries (and
  skips their replay) for the next generation.
- **Self-retire is lease-gated**: the attest gate is now a lease probe over
  the journaled mounts, and the sweep's own `Seize` is the mid-sweep busy
  re-check. `SkewCheck` is plist-only.
- **holderfs tree mode keys "content changed" on `Entry.Version`.** A
  version change whose reported mtime does not advance past the monotonic
  high-water mark bumps it +1ns, so the served mtime always changes when
  the content does (the consumer's nanosecond cache defeat survives the
  shared holder) while never regressing; a post-write canonical render is
  never swallowed by the wall-clock write bump.
- **The force gate fails CLOSED on process-enumeration failure.** A sysctl
  error, or an unreadable argv on a go-nfsv4-shaped pid, is
  `ErrCarcassUndetermined` — never an empty server list; zero candidates
  prove death only off a full scan. The lease-dir subtree scan re-runs
  immediately before the force syscall (the residual non-atomicity is
  documented with the lease package's probe-after-Acquire consumer
  contract), and a bounded force that times out PARKS the fence and claims
  on the umount process's actual exit instead of releasing them under a
  late-landing unmount.
- **Ordinary teardown is genuinely graceful for the first time —
  `Handle.Unmount` never calls cgofuse's `FileSystemHost.Unmount`.** Round-4
  discovery, confirmed against the pinned dependency source: cgofuse's
  Darwin `hostUnmount` is unconditionally `unmount(mountpoint, MNT_FORCE)`
  (its Linux path `MNT_DETACH`/`fusermount -z`), so every fleet release to
  date has FORCE-unmounted at the syscall on every "graceful" teardown —
  unmount, reclaim, retire, and exit sweep included. v1.0.0 issues fusekit's
  OWN `unmount(2)`/`umount2(2)` with flags=0, always: the external unmount
  ends the serve loop, a prompt `EBUSY` surfaces as retryable
  `ErrMountBusy` (`ClassBusy` on the wire, per the frozen protocol contract;
  still wrapping `ErrUnmountWedged` — the dir is still a mountpoint), and
  the forcing cgofuse call is structurally unreachable — `Handle` stores no
  reference to the cgofuse host at all, and the readiness-failure cleanup
  uses the same graceful call. The two proof-gated carcass clears remain the
  fleet's only force.
- **cgofuse's SIGTERM→MNT_FORCE handler is defused in process.** cgofuse's
  `Mount` creates a signal channel and `hostInit` subscribes it
  (`signal.Notify(sigc, SIGINT, SIGTERM)`); a delivered signal made its
  goroutine `host.Unmount()` — Darwin `MNT_FORCE` on EVERY live mount at
  logout/shutdown/`bootout` SIGTERM. Once a mount proves ready,
  `fusekit.Mount` now calls `signal.Reset(SIGINT, SIGTERM)` — gated on
  fusekit's OWN through-the-mount confirm op (a bounded stat of the
  mountpoint root, or `Config.ProbePath`), which orders the Reset after
  cgofuse's `Notify` by FUSE protocol construction (init serves before any
  served operation) independently of the arbitrary `Config.Ready` callback;
  a failed or overdue confirm SKIPS the Reset and tears the suspect mount
  down gracefully. `Mount` then invokes the new `Config.ReArmSignals` hook
  so the embedding app re-registers its own handler; the whole defusal is
  serialized under one mutex so concurrent Mounts never interleave a Reset
  with another's re-Notify, and a readiness FAILURE runs Reset+ReArm
  unconditionally on its cleanup path (a half-up mount may have subscribed;
  with no subscription the Reset is harmless). The holder rides its re-arm
  hook on every `MountSpec` (`ReArmSignals`, process-local, never
  serialized) through one `Host.Setup` gateway, and `HostProbe` now takes
  the same hook — OpProbe's throwaway mount was the one holder mount that
  defused WITHOUT re-arming, leaving the next SIGTERM to default-terminate
  the holder under live mounts. A source-pin test fails the suite loudly if
  a future cgofuse pin moves the registration shape. Residual, accepted: a
  TERM in the pre-Reset window at mount creation can force that single
  fresh mount (empty, no dirty pages), and one landing in the in-lock
  instants between Reset and re-Notify hits the default disposition; at
  steady state cgofuse is fully defused.
- **Linux non-root teardown works again.** `umount2(2)` of a FUSE mount
  answers EPERM for ordinary users; the graceful unmount now falls back to
  `fusermount3 -u` (then `fusermount -u`) — graceful `-u` only, NEVER `-z`
  — with a busy refusal surfacing as EBUSY. The helper runs under
  `LC_ALL=C`/`LANG=C` so the busy match never misses on localized output,
  and any other non-zero exit with the dir still mounted reads as a wedge
  with the errno unknown. Darwin is unchanged.
- **Teardown verification fails closed on an unanswered mount check.** The
  unmount verdict's mounted re-read (`MountedCheck`) now propagates
  Getfsstat/stat failures: an errored check is UNDETERMINED — classified a
  wedge, never clean — and the unmount syscall's own errno rides the
  verdict, so an EBUSY/EPERM followed by a stat failure can no longer read
  as clean teardown and trigger a reap of a live mount.
- **A resolved-to-wedge park durably retains its lease fence.** The fence
  now transfers into server state (`wedged_dirs` in `health`,
  `FeatureWedgedDirs`) — a strong reference for the process lifetime, so
  Go's `os.File` finalizer can never silently close the fence fd (dropping
  `LOCK_EX`) while the in-memory claim remains. A wedge-clear watcher
  re-probes (fresh, non-coalesced, single-flight — a permanently wedged
  `State` costs ONE stuck goroutine, never one per tick) and releases
  fence, claims, and the stale registry row once the mount is observed
  gone; otherwise the fence dies with the process fd. A host CONTRACT
  VIOLATION (`ErrTeardownPending` with no resolution channel) is PERMANENT:
  with no proof the original call ever returned, an observed-gone mount
  could still take that stale call under a replacement, so no watcher
  auto-releases it — `wedged_dirs` marks the entry with
  `mountd.WedgeContractViolation` and only a holder restart clears it.
- **Park resolutions re-read kernel truth FRESH.** The at-resolution probe
  no longer joins the per-dir single-flight — a wedged pre-resolution probe
  could re-serve a stale mounted=true sampled before the unmount call
  returned, manufacturing a final wedge after the mount was gone.
- **The pending/final unmount verdict is formally ADVISORY.** The park
  watcher re-evaluating kernel truth at call-return is the single source of
  truth for the final release: an in-flight call now reports
  `ErrTeardownPending` even when the mountpoint already reads gone (the
  resolution channel still reaches the park), `reapDeadRoot` registers its
  in-flight unmount with the same machinery, and any grace-boundary
  misclassification is harmless by construction.
- **A pending retire that lands clean drops its stale registry row.** The
  watcher's clean resolution always drops the REGISTRY row (the mount is
  gone; the row would lie) while `dropJournal` keeps deciding the JOURNAL
  row alone — a retire/force park's replay intent survives for the
  successor.
- **Persist-warnings survive error replies and parked resolutions.** A
  wedged/pending unmount's error reply now carries the journal
  persist-warning it computed, and a parked bridge removal whose late
  journal flush fails records the failure into `health`'s `Warning` until a
  later flush heals it. A reclaim sub-unmount's FAILED reply and a park
  resolution's late journal drop retain their warnings the same way — no
  persist-failure path is log-only.
- **Pending teardowns have exactly ONE release owner, resolved on the
  unmount CALL's own completion.** `Handle.UnmountDone` is the per-call
  resolution channel (the serve loop's `Done` is a different, later event);
  the provider's `TeardownDone` closes only after the parked CALL returned
  AND its registry reconciliation ran — a fence can neither release while
  the call is still blocked nor park forever behind a final refusal. At
  resolution the holder's park watcher re-reads kernel truth: mountpoint
  gone releases fence and claims (dropping the row where the op's intent
  was removal); still mounted is a FINAL wedge — the dir stays fenced and
  claimed until the holder exits, loudly, and the park itself still
  resolves. A failed first-child mux setup routes its in-flight empty-root
  unwind through the same parking; `mountd.Host` must implement
  `TeardownPender` (`Validate` enforces it structurally); a pending verdict
  without a resolution channel fails CLOSED (claims kept).
- **Shutdown waits for park watchers.** `Run` returns only after every
  pending-teardown and pending-force watcher resolved — unbounded and loud;
  process exit must never drop an EX-fence fd while an unmount or
  `umount -f` is still in flight (the cask relauncher supervises). SIGKILL
  residual, accepted: an operator kill drops the fence fds; the journaled
  rows and the successor's carcass proof cover it.
- **The retire abort is root-park-aware.** With multiple tenants on one mux
  root, the abort awaits every park its remount can collide with — the
  tenant dir AND the shared root (parks key under both) — before
  re-attaching, bounded with the journaled-loud fallback.
- **Reclaim honors the lease ladder for rowless journal entries.** A
  journal-only row (the lease-deferred-replay shape) seizes the dir's — and
  its journaled mux root's — session lease BEFORE any journal drop: a held
  lease answers busy and the row survives for the deferred replay.
- **Persist-warnings reach consumers.** `Reclaim` aggregates every journal
  persist-warning (rowful sweep drops, rowless drops, the bridge drop) into
  its OK reply, and the public client surfaces `Response.Warning`
  first-class: `Client.Unmount`, `Reclaim`, `AddBridge`, and `RemoveBridge`
  return a `warning string` alongside their data (empty when clean).
- **Owners are a strict lowercase charset** — `^[a-z0-9][a-z0-9._-]{0,63}$`.
  The owner keys registry maps byte-wise but names an on-disk spool dir on
  case-insensitive, Unicode-normalizing APFS; lowercase ASCII kills the
  "tenant"/"TENANT" (and NFC/NFD) alias classes that would run two live
  relays over one spool. `cc-pool`/`cc-notes` unaffected.
- **`Client.AddMount` refuses a `MountSpec.Owner` that disagrees with
  `Client.Owner`** — crisply, before any wire I/O — instead of silently
  sending `Client.Owner`.
- **The force gate's process enumeration is stricter still.** A truncated
  or unterminated `kern.procargs2` buffer is an enumeration error (never a
  usable mountpoint), and a pid whose procargs AND comm lookups both fail
  is `ErrCarcassUndetermined` — only a positive ESRCH-class gone signal (or
  a readable non-go-nfsv4 comm) drops a pid from the scan.
- **`health` reports `replay_done`** (`FeatureReplayDone`): the socket
  binds before the journal replays, so this is the deterministic
  "serving from a settled registry" signal for doctors and tests.
- **`RemoteHost.Setup`/`AddMount` always send the Mount RPC.** The
  local-liveness zero-RPC adopt is gone: the RPC is idempotent and local,
  and its holder-side refresh is what heals a stale journal row after a
  failed write.
- **Journal and bridge hygiene.** `Reclaim` also sweeps the owner's rowless
  journal entries (same owner guard, same lease ladder); journal rows
  canonicalize at load exactly like the wire ingress (non-absolute rows drop
  loudly); a bridge row is removed only after its runner AND replay
  goroutines exit — a timed-out stop parks the removal, keeping one live
  relay per spool dir.
- **holderfs tree-mode write coherence.** The handle mtime merges with
  max() under concurrent writers (a stale lower HWM never overwrites a later
  merge), and the post-close refresh is single-flight-with-rerun per path —
  still fresh for the last commit, now bounded to one detached fetch.
- **Teardown asks before it destroys the bridge.** The mux
  `RemoteFuseProvider.Teardown` and `FileProviderProvider.Teardown` detach
  (or deregister) via the authority FIRST and retract the account dir's
  bridge symlink only on success — a lease-ladder `ErrBusy` (or any failure)
  leaves a live session's canonical path resolving instead of ENOENT. A
  real-dir/regular-file shape still refuses fail-closed up front.
- **`RemoteHost.Teardown`/`RemoveMount` and `overlay.Provider.Teardown`
  return the holder's journal persist-warning** (breaking signature:
  `(warning string, err error)`): a kernel detach whose journal save failed
  no longer reads clean to the consumer — a successor could replay the
  reclaimed row.
- **`HealthStatus` carries `WedgedDirs` and `Warning`**, so a doctor built
  on `Client.Status` sees a permanent contract-violation wedge
  (`WedgeContractViolation`-suffixed) and unresolved persist-warnings, not
  just the raw wire response.

### Removed
- Ops `shutdown`, `attestidle`, `revokeidle`, `listdomains` — handlers,
  opcodes, client methods, and the whole attestation surface
  (`MaxAttestTTL`, TTLs, revokes). `DomainSource`/`DomainInfo` (FP stays
  daemon-bound in consumers). `IdlePolicy*`/`CarcassPolicy*` and every
  `MountSpec`/`Request`/`HolderSpec`/journal carrier, `carcassPolicyFor`,
  `forcedRoots`. `RemoteHost.Converge`, `Retire`/`RetirePlan`, and the
  consumer-driven retire/converge surface. `proc.Spawn.StableExecDir`,
  `materializeStableExe`, `proc/reexec.go` (`ReexecStable`),
  `RefreshStable`/`RemoteHost.RefreshStableExe`. `exeHashSkew`.
  `IsUnknownOp`. No wire field named `force` exists.

## [0.38.4] - 2026-07-10

### Added
- **`proc.Spawn.RefreshStable` / `mountd.RemoteHost.RefreshStableExe` —
  re-materialize the stable holder copy without spawning.** A private
  self-owning holder runs from a `StableExecDir` copy that was only refreshed
  during spawn, so after an upgrade the running holder kept executing the old
  bytes until the next spawn. Consumers now call the refresh explicitly
  post-upgrade: it shares the spawn path's hash-compare + atomic-rename core
  (the running holder's inode survives; its exe-hash skew check then retires
  it), and reports whether the bytes changed.

## [0.38.3] - 2026-07-10

### Added
- **The fusekit-holder cask owns the KeepAlive relauncher (`cmd/holder`,
  cask).** The v0.38.0 holder self-retires on version skew and relied on a
  LaunchAgent to relaunch it, but `service.AppKeepAlive` — built and
  golden-tested — was wired to nothing. The holder binary now takes
  `--install-launchagent` / `--uninstall-launchagent` (install or remove the
  `com.yasyf.fusekit-holder` agent targeting the stable cask bundle at
  `mountd.HolderApp`, then exit), and the cask drives them: postflight
  installs the agent, uninstall preflight boots it out before `quit` so
  launchd cannot relaunch the holder mid-uninstall, and `zap` trashes the
  plist.

### Fixed
- **`AppKeepAlive.Uninstall` no longer orphans a loaded agent (`service`).**
  A `launchctl bootout` failure was discarded and the plist removed anyway,
  leaving a loaded, plist-less KeepAlive job throttle-looping `open` against
  the trashed bundle. Only bootout exit 3 ("No such process" — label not
  loaded) counts as success now; any other failure propagates wrapped and
  leaves the plist in place.
- **`AppKeepAlive.Install` enables before bootstrap (`service`).** A user- or
  MDM-disabled label failed at `bootstrap` before the discarded `enable`
  could self-heal it, and with the cask postflight `must_succeed`, every
  subsequent `brew install`/`upgrade` of the cask failed with it. `enable` —
  which works regardless of load state — now runs first and its error is
  checked.

## [0.38.2] - 2026-07-10

### Added
- **`overlay.HolderSpec.CarcassPolicy` / `IdlePolicy` — the v0.38.0 consumer
  seam gap.** The mountd wire gained per-mount `CarcassPolicy`/`IdlePolicy` in
  v0.38.0, but the overlay `HolderSpec` never carried them, so a fuse-overlay
  consumer could not set `"defer"` and its mounts — including the
  kernel-panic-critical mux root — defaulted to `"force"`, letting the
  self-owning holder force-unmount a live mount (the Apple-NFS-kext panic
  class). Both fields now ride the `HolderSpec` into every `MountSpec` the
  provider registers, plain and mux alike; empty keeps today's
  `"force"`/`"attest"` defaults.

### Fixed
- **A holder refusing a contended socket lock keeps the
  `proc.ErrPeerStarting` identity (`mountd`).** `Server.listen` re-worded the
  refusal without `%w`, so a spawner that lost the retire→respawn flock race
  could not `errors.Is` the benign starting-peer case apart from a real
  startup failure. The refusal now wraps the sentinel.
- **`Retire` retries a transiently failing successor spawn (`mountd`).** A
  retiring holder's socket dies at shutdown-trigger time, but it frees
  `Socket+".lock"` only at process exit — after its handler drain, unmount
  sweep, and journal drain — so a successor spawned the moment the socket dies
  can lose the flock race for real wall-clock time: it refuses with
  `proc.ErrPeerStarting`, which a forked spawn surfaces as
  `proc.ErrChildUnavailable` (documented "transient; drivers retry" — but no
  driver did). A `Converge` losing that race failed the whole holder upgrade
  outright. `Retire` now retries both transient sentinels within a
  `RetirePlan.SpawnWait` budget (default 30s, ctx-bounded); permanent refusals
  still abort on the first attempt. The converge tests' shared successor
  helper absorbs the same race in-process for deterministic spawn counts.

## [0.38.1] - 2026-07-10

### Fixed
- **Tree mode: a rename no longer strands the moved node with a stale
  not-found (`holderfs`).** `treeView.rename` runs its consumer-side `Rename`
  and the node's generation bump under the view lock, but a fire-and-forget
  post-`release` `fetchStat` could still land its RPC in the window where the
  source path had already been deleted and cache `notFound` on the node before
  the gen bump could fence it — so the moved node served a spurious `ENOENT` at
  its new path (a ~1-in-400 flake). The move loop now clears each moved node's
  `notFound` under the lock: a successful rename proves the object exists, and
  the concurrent gen bump discards any earlier fetch, so the clear is the last
  word.

## [0.38.0] - 2026-07-10

### Added
- **Durable holder spec journal + fresh-start replay (`mountd`).** A holder
  with `Server.JournalPath` set (the cask holder uses
  `mountd.DefaultJournalPath` → `~/.fusekit/holder-specs.json`) mirrors every
  active mount (full `MountSpec`) and hosted bridge to an atomically-written
  journal, co-updated with the in-memory registries on every mount, unmount,
  sweep, AddBridge adopt, RemoveBridge, and Reclaim. On start — after the
  socket bind, before the accept loop — the holder replays the journal:
  carcass-clear plus a cross-generation orphaned-go-nfsv4 reap over the
  journaled kernel roots (mux tenants collapse to their shared root), then
  re-`Setup` of each mount and re-establishment of each bridge with per-entry
  capped backoff. Replay never fails startup: a persistently failing entry is
  dropped loudly and the holder serves whatever succeeded; a corrupt journal
  starts empty. A clean shutdown drains the journal — consumers re-establish —
  except entries whose final teardown failed: a mount still up at exit stays
  journaled so the successor force-clears or surfaces its carcass per its
  `CarcassPolicy`. The on-disk format is a frozen, additive-only artifact
  (golden test), like the wire protocol.
- **Self-retire on version skew (`mountd`).** A journaling holder with
  `Server.RetireSkew` set (the cask holder wires `mountd.SkewCheck`: its
  compiled-in version vs the installed bundle's Info.plist; a cask-less holder
  keys on its executable's hash) polls every 60s. On skew it enters a retiring
  state — new mount/bridge requests bounce retryable `ClassBusy` — and drains
  only once every journaled mount is provably idle per its `IdlePolicy`, then
  exits 0 with the journal intact so the LaunchAgent's relaunch replays it.
  The drain is graceful-only: any busy claim, expired attestation, or
  EBUSY/wedged teardown aborts the sweep, remounts the swept prefix (the
  kernel-panic invariant — never a force-unmount), and returns to serving
  normally until the next attempt, and a persisted strike
  breaker (3 retire attempts in 10 minutes, across generations) parks the
  holder loudly on an escalating ladder instead of thrashing or kill-cycling.
- **`OpAttestIdle` + `OpRevokeIdle` + per-mount `IdlePolicy` (additive proto-1
  surface).** Consumers attest idleness per dir with a TTL (`Client.AttestIdle` /
  `RemoteHost.AttestIdle`; owner-validated, capped at `MaxAttestTTL`, a dir
  owned by another consumer refused), and `Client.RevokeIdle` synchronously
  withdraws an owner's own attestations before a mount goes back into use.
  `MountSpec.IdlePolicy` selects the retire gate per mount: `"attest"` — the
  meaning of ABSENT, fail-closed — requires a fresh attestation from the
  mount's owner; `"probe"` lets the holder attempt a graceful unmount and
  treat EBUSY as busy.
- **Per-mount `CarcassPolicy` (additive proto-1 surface).**
  `MountSpec.CarcassPolicy` / `Request.CarcassPolicy` declares how the holder
  treats a dead-mount carcass at the mount's kernel root: `"force"` — the
  meaning of ABSENT, every spec's prior behavior — force-clears it (the
  pre-mount `ClearCarcass`, the replay's carcass-clear, the handle-less forced
  teardown); `"defer"` forbids every autonomous force-unmount — the carcass
  stays in place and is surfaced (`ErrUnmountWedged`, a loud replay log) for
  the consumer, who holds the live-session knowledge the holder lacks. One
  deferring tenant defers its whole mux root. The new `MountInfo.CarcassPolicy`
  reports each row's policy through `list`, so driver-side retirement honors
  it too.
- **`OpListDomains` (additive proto-1 surface).** Returns the File Provider
  domains the holder's `Server.DomainSource` enumerates — the platform's
  registered-domain truth, orphans included — so a consumer whose FP bridge
  the holder hosts reconciles domains without its own fileproviderd path
  (`Client.ListDomains`). A holder wired without a source fails loudly, never
  an empty list; a holder predating the op answers unknown-op (`IsUnknownOp`).
- **`OpHealth` status snapshot (additive fields).** `health` now carries the
  self-owning holder's observable state — `retiring`, the storm-park deadline,
  the journal entry counts, the persisted strike history, and the idle-gate
  deferral (`retire_deferred_dir` / `retire_deferred_reason`, so "skewed but
  deferred by a busy mount" is distinguishable from "no skew at all") — and
  the new `Client.Status` decodes it into `HealthStatus`. A holder predating
  the fields decodes as all-zeros; an old client ignores them.
- **`service.AppKeepAlive` — the holder's LaunchAgent relauncher.** A per-user
  KeepAlive LaunchAgent whose program is `/usr/bin/open -g -W <app>`: `-W`
  attaches to an already-running instance (never a second copy) and blocks
  until it exits, so launchd relaunches only on a real exit — a self-retired
  holder's exit 0 is exactly what brings up the installed build — and never
  spins against a live holder. `Install` is idempotent (bootout → bootstrap →
  enable → kickstart) and kills only the blocked waiter, never the app;
  `Uninstall` likewise. The plist is XML-escaped and golden-tested.
- **`proc.Strikes` + `proc.Ladder`.** A minimal sliding-window strike breaker
  (persistable via `Times`/`Load`) and an escalating-duration ladder, backing
  the retire-storm breaker.

### Changed
- **`mountd.Retire` clears confirmed-dead carcasses only, honoring
  `CarcassPolicy`.** The legacy version-skew replace (`RemoteHost.Converge`)
  force-unmounted every root the retired holder served, unconditionally — a
  busy mount that survived the graceful shutdown keeps serving through its
  orphaned go-nfsv4 server, so the blind `MNT_FORCE` could hit a live mount
  (the Apple-NFS-kext panic class). The carcass phase now runs
  `fusekit.ClearCarcass` — a root that still answers stats is left alone, and
  the remount surfaces it as `ErrForeignMount` — and skips any root a row
  declares `CarcassPolicy` `"defer"` for, exactly like the holder's own
  journal replay. `RetirePlan.ForceUnmount` is renamed
  `RetirePlan.ClearCarcass` to carry the confirmed-dead contract.

### Removed
- **`proc.Supervisor` (+ `proc.Policy`) and `mountd.RetirePolicy`.** The
  daemon-driven holder-supervision state machine is superseded by the
  self-owning holder: the spec journal plus `RetireSkew` self-retire owns skew
  replacement, and the `service.AppKeepAlive` LaunchAgent owns respawn. CLI
  consumers keep `RemoteHost.Converge` for single-consumer holders.
- The `--reap-root` holder flag: the journal-driven replay reap supersedes the
  one-shot startup reap, and no spawner passed the flag (the cask holder is
  launched argv-less via `open -g`).

## [0.37.0] - 2026-07-09

### Added
- **Holder-hosted File Provider content bridge (`content.RelaySource` +
  `mountd` bridge ops).** A consumer daemon can now ask the shared holder to
  host its File-Provider-facing content bridge, so the appex keeps enumerating
  and saving across daemon restarts. `content.RelaySource` is a caching,
  write-spooling `content.Source` that proxies the consumer daemon's bridge over
  a `BridgeClient`: `Manifest`/`ReadSynth` serve the last-good cache on an
  unreachable upstream (a cold cache propagates `ErrBridgeUnavailable`),
  `Classify` answers offline from cached-manifest entries plus the private
  prefixes (the fuse holder's fidelity; a cold cache returns the new
  `ErrClassifyUnavailable` rather than guess), and `WriteThrough` always accepts
  — persisting to a durable disk spool (`~/.fusekit/spool/<owner>`, latest-wins,
  survives a crash or holder handoff) and replaying it upstream asynchronously
  with capped backoff. The optional `content.Classifier` superset lets a Source
  signal classification unavailability, which the `BridgeServer` prefers over
  `Classify`.
- **Additive `mountd` bridge protocol (proto-1 preserved).** New ops
  `OpAddBridge`/`OpRemoveBridge`/`OpBridges`, a new `Request.BridgeSocket` (the
  appex-facing socket the holder binds; `Request.ContentSocket` is the upstream
  the relay dials), a new `Response.Bridges []BridgeInfo` listing (owner, socket,
  state `starting|serving|consent-pending|bind-failed`, pending-write depth), and
  the `ClassForeignBridge` error class for a foreign owner colliding on a bind
  socket. The bridge registry is kept separate from the mount registry, so no
  mount sweep, converge, or carcass path touches it; Reclaim and Shutdown
  owner-accounting count bridge owners. New `Client.AddBridge`/`RemoveBridge`/
  `Bridges` and `RemoteHost.AddBridge`/`RemoveBridge`, plus `mountd.IsUnknownOp`
  (the mirror of `content.IsUnsupported`) for a defensive version-skew check
  behind the consumer's client-side version pre-flight.

## [0.33.1] - 2026-07-07

### Fixed
- **Stale pluginkit duplicates no longer mask a live File Provider election.**
  `pluginkit -m` lists every registered copy of an extension (an old app copy in
  the Trash included); the enablement check read only the first line's status
  flag, so a stale disabled duplicate listed first made a genuinely elected
  extension report disabled — `FileProviderAvailable` skipped a working backend
  and `TryEnableFileProvider` returned a spurious
  `ErrFileProviderElectionIneffective`. The parse now scans every line and an
  enabled `+` flag anywhere wins.

## [0.33.0] - 2026-07-07

### Added
- **File Provider extension election helper.** `TryEnableFileProvider(bundleID string) error`
  runs `pluginkit -e use -i <bundleID>` (under detection's `pluginkitTimeout`, since the
  election path can wedge the same way a query can), then re-checks enablement through the same
  pluginkit detection seam `FileProviderAvailable` consults. It returns nil once the extension
  reports enabled, wraps the pluginkit failure loud if the election command itself failed, or
  returns the new `errors.Is`-able sentinel `ErrFileProviderElectionIneffective` when
  `pluginkit -e use` succeeds yet the extension stays disabled — the case on macOS versions where
  the System Settings File Providers pane owns election and silently ignores pluginkit — so
  consumers can fall back to their Settings-guidance path (`BackendFileProvider.OpenSettings`).

## [0.30.0] - 2026-07-04

### Added
- **Cross-generation go-nfsv4 orphan reaping.** A holder that dies without an exit path (the
  2026-07-03 incident: SIGTRAP inside libc `exit()`) leaves its spawned go-nfsv4 servers alive
  and bound to their sockets, answering EPERM to every op on every mount — and no successor
  could reap them: the existing reaper is direct-children-only by design. New exported
  `ReapOrphanedServers(roots []string) []int` finds go-nfsv4 processes of ANY generation whose
  argv mountpoint lies under one of the caller's consumer roots and SIGKILLs each only after
  the mountpoint is independently confirmed a carcass (never a live mount, never a server
  outside the roots). Every kill is re-confirmed at signal time with a FRESH per-candidate stat
  and an argv-mountpoint re-read (guards a mount that comes back live mid-sweep and PID reuse to
  a server on a different path — `comm` is identical across all go-nfsv4). Wired into
  `ClearCarcass`'s force-reap (which previously only `umount -f`'d, leaving the orphan to stack a
  duplicate under the remount) and into holder startup via the new repeatable `--reap-root` flag.
- **Probe-denied verdict sentinels.** `ErrProbeDenied` and `ErrProbeMissing` with
  `ProbeOpenVerdict(err) error`: EPERM/EACCES from an existing mount's wedge-probe file is the
  orphaned-dead-server signature and now classifies as a distinct, `errors.Is`-able dead
  verdict instead of folding into the "probe file missing (old holder)" no-verdict class that
  let every broken mount read healthy through the incident.
- **Ship-gated holder process-group isolation.** With `FUSEKIT_HOLDER_KILL_GROUP=1` the holder
  calls `setpgid` at startup and SIGKILLs its own process group on the abnormal serve-error
  exit path, so spawned servers die with it. Default OFF pending the VM gate (see the new
  scenario); a TODO in `cmd/holder` cites it.
- VM scenario `scripts/vm/scenarios/repro-holder-crash-orphan.sh`: SIGKILL/SIGTRAP the holder
  under load, assert orphans are detected + reaped + remounted by the respawned holder with
  zero post-recovery EIO; a `VMCTL_KILL_GROUP=1` arm gates the setpgid feature on 10 kill
  cycles with the KeepAlive-respawn analog intact.

### Fixed
- **EPERM/EACCES on a carcass stat now reads as a carcass.** `ClearCarcass` treated any stat
  outcome except ENOTCONN/EIO as alive, so a mount backed by an orphaned dead-holder server —
  which answers EPERM to everything — read healthy and was never cleared (and the pre-mount
  sweep never ran because the orphan kept the mount-table entry).
- **The holder's log file no longer leaks into spawned servers.** With `--log`, the holder
  routes its own output (Go crash traces included, via `debug.SetCrashOutput`) to the
  O_CLOEXEC log fd and re-points stdout/stderr at /dev/null, so go-nfsv4 children inherit a
  harmless sink instead of writing into the holder log after the holder dies.

## [0.29.2] - 2026-07-04

### Fixed
- Overlay file-conflict resolution no longer destroys the losing copy: the newer mtime still wins at dst, but the loser is quarantined next to it as `<dst>.conflict-<mtime unixnano>` instead of being unlinked. Quarantine placement is an atomic hardlink (EEXIST fails loud on differing bytes; identical bytes make an interrupted pass re-runnable). Destroyed real Claude session transcripts on 2026-06-15 and 2026-07-04 when freshly-written stubs outdated multi-MB originals.

## [0.29.0] - 2026-07-03

### Added
- **Single-mount multiplexing (`MountSpec.MuxRoot`).** Specs sharing a MuxRoot are served as
  subtrees of ONE native fuse-t mount — one go-nfsv4 process for N tenants instead of one per
  mount. `holderfs/mux.go`'s muxFS routes by first path component under an RWMutex, slot-remaps
  child-minted synthetic inos into disjoint per-tenant ranges (slots never reused for the native
  mount's lifetime), forces minted tenant-root inos (the shared base dir's real ino must never
  alias across tenants), refuses root-level mutations (EPERM) and cross-tenant rename/link
  (EXDEV), and attaches/detaches tenants live with no kernel unmount — per-tenant teardown is a
  map operation, shrinking the forced-unmount surface. `MountSet` grows the tree lifecycle in
  the one registry (find-or-mount the native root, `ErrMuxMismatch` on per-root option
  disagreement, unmount-on-last-detach, never MNT_FORCE a subtree); `mountd` carries `mux_root`
  as an additive proto-1 field with `ClassMuxMismatch`, per-MuxRoot claims, and subtree-aware
  idempotency/reclaim/retire/converge; the overlay `RemoteFuseProvider` gains a mux mode that
  bridge-symlinks each account dir onto its subtree (`ErrAccountDirOccupied` refuses non-empty
  dirs). Plain per-dir mounts are unchanged. Release-gated by the new `validate-mux.sh` VM
  scenario (one mount/one go-nfsv4, per-tenant synth+carve-out isolation, fileid identity,
  re-attach content coherence, detach-under-load, force-unmount reassembly, full-window clean).

### Fixed
- **Synthetic documents now stay coherent across tenant re-attach.** fuse-t's go-nfsv4 mints
  its own path-keyed client fileids (the handler's st_ino never reaches the NFS client) and the
  macOS NFSv4 client validates its data cache on the `change` attribute alone, which go-nfsv4
  derives from the served ctime. holderfs served writePath's resting ctime verbatim, so a
  re-attached tenant's fresh incarnation repeated the old `change` value and clients kept stale
  pages under the reused fileid (VM-proven: same-size rewrite across detach/re-attach served the
  old bytes indefinitely). Served mtime/ctime are now floored by a per-writePath registry
  chained one nanosecond past everything the previous incarnation actually served — clock-tie
  and backward-step safe by construction; first incarnations serve genuine on-disk timestamps,
  so plain-mount attr semantics are unchanged.
- **Synth refresh installs are TOCTOU-safe.** A freshness-file rewrite racing the bridge read
  can no longer install torn/empty bytes under a signature that claims freshness: `refreshOnce`
  brackets the read with pre/post signature checks plus an in-window mtime test, retries with
  backoff (bounded), and keeps last-good loudly on exhaustion.

## [0.28.0] - 2026-07-03

### Fixed
- **Silly-rename placeholders divert into PrivateRoot, never shared Base.** go-nfsv4 renames an
  in-use unlink/rename-over victim to a top-level `.fuse_hidden*`/`.nfs.*` placeholder and
  defers the real unlink; holderfs mapped that placeholder into the SHARED base — for a synth
  victim (whose `real()` is its private writePath) the account-private merged document
  physically landed in base, listed and readable through every other tenant's mount, stranded
  whenever the deferred unlink never arrived. The silly-rename class (a predicate apart from
  isPrivate) now routes to PrivateRoot in `real()`, Readdir suppresses the class at root (also
  hiding pre-fix legacy litter), and Build sweeps stale placeholders from PrivateRoot each new
  generation. Release-gated by the new `validate-sillyrename.sh` VM scenario (guest gate:
  unlink-while-open keeps base clean, readdir silent, held handles serving).

## [0.27.0] - 2026-07-03

### Fixed
- **The holder runs at `nice 5`, no longer in the Darwin background band.** v0.23.0's
  `PRIO_DARWIN_BG` demotion (CPU throttle + lowest disk I/O tier, inherited by the per-mount NFS
  servers) starved the mounts' data plane exactly when demand peaked — measured live on a loaded
  10-mount host: 1.4–3.4 ms open/close vs a ~214 µs normal-priority floor, "serves metadata but
  hangs reads" wedges, and holder-saturation liveness failures. A self-set band also cannot be
  cleared from outside the process (`taskpolicy -B` and `setpriority(…, 0)` return success
  without effect), so affected holders can only be cured by restart. `proc.SetBackgroundPriority`
  is replaced by `proc.Nice(n)`: classic nice keeps the politeness intent as a soft scheduling
  weight, leaves I/O untouched, and has no starvation cliff. Note `Nice` is one-way for
  unprivileged processes — the startup value is the value.

## [0.26.0] - 2026-07-03

### Added
- **Per-mount AttrCache opt-in** (`MountOptions.AttrCache`/`AttrCacheTimeout`, plumbed
  MountSpec → mountd → holderfs → overlay.HolderSpec) — drops the darwin-forced `noattrcache`.
  Ships with a **proven contraindication**: synth-document mounts tear at ANY TTL (VM gate
  failed 2/2 within seconds of churn) and must never opt in — see `ccn doc show 130274e`.
- **The former attrcache VM release gate** combined an envelope torn-read detector with a
  writer staleness-bound phase and the AppleDouble/panic scenarios. The v1.6 hard cut retires
  that holder-specific harness; future cache work must add a new isolated-VM acceptance gate.
- VM runs archive their evidence to the cc-notes chronology.

## [0.25.0] - 2026-07-02

### Added
- **Prefix-aware overlay skip: `Spec.SkipPrefixes` + `Spec.Skipped`.** `SkipPrefixes` lists top-level name prefixes ignored exactly like `Skip` entries — never linked, mirrored, or moved (the motivating consumer case: AppleDouble `._` litter, whose per-file names no fixed `Skip` set can enumerate). `Skipped(name)` is the one membership predicate — true for a `Skip` hit or a `SkipPrefixes` match — and every `Skip` consumer (`SymlinkProvider.Sync`/`Health`, `MoveSharedOrphans`, `mergeDir`) now classifies through it. Prefixes must be non-empty; an empty prefix would match every name.

## [0.24.0] - 2026-07-01

Tree-mode release: a fully-remote tenant — one with NO local base directory — is now servable through the shared holder. v0.18.0 shipped the read side (`content.Tree`); this release adds the write side, per-open handle tokens, the tree-mode holder filesystem, and the nominal-Base mount path, with v0.23.0's attr-stability rules applying from day one.

### Added
- **Write-side bridge ops for a fully-remote (tree-mode) tenant.** `content.WritableTree` extends `Tree` with the mutations a no-local-base consumer's editors need — `Create`, `WriteAt`, `Truncate`, `Unlink`, `Rename`, `Mkdir` — carried by the new additive bridge ops `create`/`writeat`/`truncate`/`unlink`/`rename`/`mkdir` (`BridgeProtoVersion` stays 1). A write op against a read-only `Tree` answers the new `ClassUnsupported` error class — a clean capability verdict, never a panic — and the new `content.IsUnsupported` reads both that reply and the class-less unknown-op reply an older server gives, so one predicate covers both vintages. `BridgeClient` grows the matching methods.
- **Per-open snapshot handle-tokens over the bridge.** `content.HandleTree` extends `Tree` with `OpenHandle`/`ReadAtHandle`/`WriteAtHandle`/`TruncateHandle`/`FlushHandle`/`ReleaseHandle`/`ReleaseAllHandles`, wired as the new `open`/`flush`/`release`/`releaseall` ops plus a `token` field on `readat`/`writeat`/`truncate` — the holder opens one token per fuse handle so the consumer keys its snapshot cache and edit buffer by it (chunked NFS reads stop re-rendering per `ReadAt`; buffered edits commit once). `FlushHandle` exists because a fuse Release status is kernel-discarded: it is the op that returns the commit verdict (rejected save → `ClassInvalid`/`ClassPerm`) at the writer's fsync/close boundary. Crash safety is release-all, not TTL — the holder sweeps `ReleaseAllHandles` per domain when it starts and stops serving it, so a crashed holder's leaked tokens die on the next generation's first sweep, and an idle-but-dirty edit buffer can never expire out from under a still-open editor. Token support is optional: a plain `Tree` consumer keeps working exactly as v0.18.0 shipped (the holder detects `IsUnsupported` and stays stateless). The open reply also carries the snapshot's `Entry` (reusing the response's `item` field; `OpenHandle` returns it alongside the token) — its `Size` MUST be the exact length of the bytes `ReadAtHandle` serves for the token, with `Mtime`/`Birth`/`Ino` meaning what they do in `Stat` — so the holder sizes every open from the snapshot it will actually read, never from a stat cache that may straddle a consumer commit (a stale cached size would cap kernel reads at the wrong length: a grown snapshot read torn to the old length, a shrunk one zero-padded). `BridgeClient.OpenHandle` returns a `content.Handle` carrying the token and snapshot through `ReadAt`/`WriteAt`/`Truncate`/`Flush`/`Release`.
- **`content.Entry` carries Tree attrs.** Additive `Mtime`/`Birth` (Unix nanoseconds) and `Ino` (a stable consumer-side identity key the holder mints its own inode from, never serves raw) plus the `EntryDir` kind, so a Tree consumer's `Stat`/`List` can express directories and times — required for the tree-mode holder to serve consumer times under the attr-stability rules instead of inventing its own.
- **Tree-mode holder filesystem.** A `MountSpec` with the new `fusekit.ContentModeTree` now builds a fully-remote holder fs (`holderfs/tree.go` + `treeview.go`) instead of failing: EVERY op is answered from the consumer's `content.Tree` over the bridge — Getattr from Stat, Readdir from List, reads from ReadAt (token-keyed per fuse open against a `HandleTree` consumer; a plain-Tree consumer gets a tokenless open-time snapshot, selected only by the `IsUnsupported` capability verdict, never a silent fallback), Readlink from Readlink, and mutations through the `WritableTree` ops (a read-only tenant answers every mutation EROFS) — with NO local Base syscalls anywhere. Serving follows synth mode's off-handler discipline: warm entries answer from cache and refresh off the handler; only the first touch of a path waits on the bridge, bounded and joined per path, so a hung consumer can never park a fuse-t worker. The nfs_vinvalbuf2 rules apply from day one: minted mount-lifetime-stable inodes keyed by the consumer's `Entry.Ino` identity (path-keyed otherwise, transferred across renames — a rename never re-mints a fileid; the registry re-keys to the identity the moment the consumer first reports one and clears a replaced rename destination's path key, so the editor atomic-save loop can never hand a new file a live file's fileid), a monotonic served-mtime high-water mark, attrs pinned while any handle is open, no `namedattr`, and macFUSE-style AppleDouble `._` blocking on every vnop including consumer-supplied names. Flush/Fsync forward a dirty handle's commit verdict (a rejected save reaches the writer as EINVAL/EPERM); Build fails loud on an unreachable bridge or Tree-less consumer and sweeps `ReleaseAllHandles` so a crashed holder's leaked tokens die with the new generation. Rmdir/Link/Symlink/Chown answer EPERM (no bridge op exists); Chmod/Utimens are acknowledged and ignored — the consumer owns modes and times.

### Changed
- **A tree-mode mount's `Base` is nominal.** A tree tenant has no local backing tree: its `MountSpec.Base` is an identity key — consumers pass their repo root — that the served filesystem never reads. The wire rules do not loosen for it: `mountd` keeps requiring a non-empty `Base` and keeps refusing `dir == base` in EVERY mode, because Base still keys the registry, teardown, and `unmount` — a tree mount over its own base would shadow the consumer's backing tree from the consumer itself and could never come down through `OpUnmount`. Tree tenants mount at a dedicated mountpoint. New `fusekit.ContentModeSource`/`ContentModeTree` constants name the modes.
- **Bridge capability misses now carry `ClassUnsupported`.** A Tree/WritableTree/HandleTree op against a `Source` that does not implement the surface answers `<op>: source does not implement <surface>` with `ErrClass: "unsupported"` instead of the class-less `unknown op` reply, which now exclusively signals version skew (an op the server has never heard of). No deployed client matched the old text for these ops.

## [0.23.0] - 2026-07-01

Panic-mitigation release. Three macOS kernel panics (`nfs_vinvalbuf2: ubc_msync failed!, error 22` in xnu's nfs_bio.c — the NFS kext panics unconditionally when `ubc_msync` returns EINVAL during vnode invalidation) traced to what holderfs serves: attribute churn under open files plus the NFSv4 named-attribute vnode path. This release removes every known churn source; the retained safety conclusions are recorded in the [v1.6 rewrite ledger](docs/reports/v1.6-rewrite-ledger.md).

### Added
- **The holder runs at Darwin background priority.** `cmd/holder` demotes itself via the new `proc.SetBackgroundPriority` — `setpriority(PRIO_DARWIN_PROCESS, 0, PRIO_DARWIN_BG)`, the process-wide equivalent of `QOS_CLASS_BACKGROUND` — right after flag parsing, so the shared holder (and the per-mount NFS servers it spawns, which inherit the band) never competes with foreground work for CPU. In-process is the only lever: the holder launches via `open -g`, not a LaunchAgent, so there is no plist to carry launchd's `Nice`/`ProcessType` keys.

### Changed
- **holderfs mounts no longer pass `-o namedattr`.** The NFSv4 named-attribute vnode path appears in every `nfs_vinvalbuf2` panic backtrace (Quarantine.kext provenance-xattr frames). Without the option the macOS NFS client defaults to `nonamedattr` and fails xattr ops ENOTSUP client-side; holderfs's xattr handlers stay as-is for direct-Serve and Linux consumers, and `MountOptions.NamedAttr` remains for consumers that accept the risk.
- **AppleDouble `._` names are blocked outright (macFUSE `noappledouble` semantics).** Dropping namedattr revives xnu's fallback of writing `._*` sidecars through regular creates — the litter namedattr was originally enabled to prevent. holderfs now refuses the whole namespace instead: creating ops (Create, `O_CREAT` opens, Mkdir, and Rename/Link/Symlink destinations) on a `._` basename answer EACCES; resolution, removal, and metadata ops answer ENOENT even when litter already sits on the backing store; and Readdir never lists a `._` name from Base, the PrivateRoot merge, or a synth entry. `.foo`, `..data`, and `x._y` are not AppleDouble names and pass untouched. No `._` vnode ever exists on the mount, so sidecars can no longer generate invalidation churn.

### Fixed
- **Synth attribute stability** — the suspected panic root cause. A synth entry now serves a minted, mount-lifetime-stable inode from path Getattr, handle Getattr, writable-fd Getattr, and Readdir — never writePath's real fileid, which every atomic-rename write-through re-mints under a file the client holds open. Build seeds a cold cache from writePath (the durable last-committed bytes), so the served size never flaps cold→warm while the consumer is slow or unreachable. The served mtime is a per-view high-water mark that never regresses when a freshness file vanishes or is backdated. And while any read handle is open, path attrs pin to the newest open's snapshot — a background refresh can no longer land an invalidation on an open or mapped file; changes surface on the first Getattr after the last close.
- **The wedge probe no longer bumps its mtime on every Getattr.** The bump moved to open-time only (monotonic), which still makes the NFS client's open-time revalidation drop cached pages, and the per-open random nonce already defeats cache replay — so attribute polls against the probe stop generating deliberate invalidation churn.

## [0.22.1] - 2026-06-27

### Fixed
- **`Select` now threads `ExecPath` into its holder-probe `Spawn`.** A pure-Go build with the cask installed (so `holderCanSpawn` passed on the cask `ExecPath`) hit `canHost`'s `fusekit.Built()` branch and refused with `ErrCannotHost` ("this binary cannot host fuse mounts") instead of launching the `.app` via `open -g`. Masked while the holder was already serving (`EnsureRunning` short-circuits on `Available` before `canHost`), so it surfaced only when the holder was down — e.g. a `DetectOverlayBackend`/`migrate` probe right after the holder was stopped for a cask upgrade. The `AddMount` path (`newRemoteFuse`) already threaded `ExecPath`; only the `Select` probe dropped it.

## [0.22.0] - 2026-06-27

### Changed
- **Mount teardown is graceful-only by default (`Config.ForceOnWedge`).** A macOS kernel panic (`nfs_vinvalbuf2: ubc_msync failed!`, error 22) traced to `MNT_FORCE` on a busy fuse-t/NFS mount: a graceful unmount only stalls because a live client still holds the mount busy, and forcing past its mapped pages panics the kernel. `Handle.Unmount` now escalates to a forced kernel unmount ONLY when the new `Config.ForceOnWedge` is set; the false zero value (the correct default for an in-process self-teardown) leaves a busy mount in place and returns `ErrUnmountWedged`. The shared `cmd/holder` is graceful-only for every tenant — its death-sweep (logout, reboot, SIGTERM) no longer `MNT_FORCE`-es a busy mount. When escalation IS enabled, the force now runs through the bounded `ForceUnmount` in its own goroutine raced against `forceGrace`, so a wedged `MNT_FORCE` can no longer park `Handle.Unmount` past its grace (a latent bug in the old synchronous force). Consumers that have proven a mount idle by other means and still want the old behavior set `Config.ForceOnWedge = true`.

[Unreleased]: https://github.com/yasyf/fusekit/compare/v1.10.1...HEAD
[1.10.1]: https://github.com/yasyf/fusekit/compare/v1.10.0...v1.10.1
[1.10.0]: https://github.com/yasyf/fusekit/compare/v1.9.1...v1.10.0
[1.9.1]: https://github.com/yasyf/fusekit/compare/v1.9.0...v1.9.1
[1.9.0]: https://github.com/yasyf/fusekit/compare/v1.8.1...v1.9.0
[1.8.1]: https://github.com/yasyf/fusekit/compare/v1.8.0...v1.8.1
[1.8.0]: https://github.com/yasyf/fusekit/compare/v1.7.7...v1.8.0
[1.7.7]: https://github.com/yasyf/fusekit/compare/v1.7.6...v1.7.7
[1.7.6]: https://github.com/yasyf/fusekit/compare/v1.7.5...v1.7.6
[1.7.5]: https://github.com/yasyf/fusekit/compare/v1.7.4...v1.7.5
[1.7.4]: https://github.com/yasyf/fusekit/compare/v1.7.3...v1.7.4
[1.7.3]: https://github.com/yasyf/fusekit/compare/v1.7.2...v1.7.3
[1.7.2]: https://github.com/yasyf/fusekit/compare/v1.7.1...v1.7.2
[1.7.1]: https://github.com/yasyf/fusekit/compare/v1.7.0...v1.7.1
[1.7.0]: https://github.com/yasyf/fusekit/compare/v1.6.2...v1.7.0
[1.6.2]: https://github.com/yasyf/fusekit/compare/v1.6.1...v1.6.2
[1.6.1]: https://github.com/yasyf/fusekit/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/yasyf/fusekit/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/yasyf/fusekit/compare/v1.4.0...v1.5.0
[0.25.0]: https://github.com/yasyf/fusekit/compare/v0.24.0...v0.25.0
[0.24.0]: https://github.com/yasyf/fusekit/compare/v0.23.0...v0.24.0
[0.23.0]: https://github.com/yasyf/fusekit/compare/v0.22.2...v0.23.0
[0.22.2]: https://github.com/yasyf/fusekit/compare/v0.22.1...v0.22.2
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
